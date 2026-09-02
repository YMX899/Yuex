package workers

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"huahuoai/backend/source/internal/persistence"
	"huahuoai/backend/source/internal/queue"
	runtimepkg "huahuoai/backend/source/internal/runtime"
	"huahuoai/backend/source/internal/services"

	"github.com/jackc/pgx/v5"
)

type RuntimeRecoveryWorker struct {
	Repos                      *persistence.Repositories
	Hosts                      *runtimepkg.RuntimeHostRepository
	Locks                      *queue.DistributedLockManager
	Scheduler                  *runtimepkg.RuntimeScheduler
	Client                     runtimepkg.AsyncOpenClawClient
	TicketSecret               string
	SessionKeyEncryptionSecret string
	OwnerInstanceID            string
	Now                        func() time.Time
	Interval                   time.Duration
	UnhealthyAfter             time.Duration
	OfflineAfter               time.Duration
	// terminalProjector is an internal test seam for the Product projection
	// boundary. Production workers always use AgentRunProductProjector.
	terminalProjector func(persistence.AgentRunRecord, string, map[string]any, map[string]any, map[string]any) error
	// reconcileLeaseAcquire is an internal test seam. Production workers always
	// use the configured Redis/Tair manager for the global ownership lease.
	reconcileLeaseAcquire func(context.Context, string, time.Duration) (queue.DistributedLease, error)
	// dispatchRecoveryLeaseAcquire is an internal test seam. Production workers
	// acquire the per-dispatch recovery lease from the same Redis/Tair manager.
	dispatchRecoveryLeaseAcquire func(context.Context, string, string, time.Duration) (queue.DistributedLease, error)
	// The following recovery seams isolate candidate handling in focused tests.
	// Production always reads durable convergence/dispatch facts directly.
	listIncompleteConvergences func(context.Context, int) ([]runtimepkg.TerminalConvergenceRecoveryCandidate, error)
	getRecoveryDispatch        func(context.Context, string) (runtimepkg.RuntimeDispatch, error)
	enqueueTerminalRecovery    func(context.Context, runtimepkg.RuntimeDispatch, string) (bool, bool, error)
}

const runtimeDispatchRecoveryMaxDeferral = 10 * time.Minute

func NewRuntimeRecoveryWorker(repos *persistence.Repositories, hosts *runtimepkg.RuntimeHostRepository, locks *queue.DistributedLockManager, sessionKeyEncryptionSecret, ownerInstanceID string) RuntimeRecoveryWorker {
	return RuntimeRecoveryWorker{
		Repos: repos, Hosts: hosts, Locks: locks, Scheduler: runtimepkg.NewRuntimeScheduler(hosts, locks),
		Client: runtimepkg.HTTPTransportOpenClawClient{}, TicketSecret: strings.TrimSpace(os.Getenv("HUAHUO_RUNTIME_RUN_TICKET_SECRET")),
		SessionKeyEncryptionSecret: strings.TrimSpace(sessionKeyEncryptionSecret), OwnerInstanceID: strings.TrimSpace(ownerInstanceID),
		Now: func() time.Time { return time.Now().UTC() }, Interval: 10 * time.Second,
		UnhealthyAfter: 30 * time.Second, OfflineAfter: 90 * time.Second,
	}
}

func (w RuntimeRecoveryWorker) Run(ctx context.Context, workerID string) error {
	if err := w.validateRuntimeRecoveryStartup(ctx); err != nil {
		return err
	}
	interval := w.Interval
	if interval <= 0 {
		interval = 10 * time.Second
	}
	for {
		w.OwnerInstanceID = firstWorkerString(w.OwnerInstanceID, workerID)
		if _, err := w.Reconcile(ctx); err != nil {
			if !runtimeRecoveryRetryable(err) {
				return err
			}
			log.Printf("runtime_reconcile cycle failed; retrying: %v", err)
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// Reconcile is a periodic control plane. Empty scans and transient control
// plane failures must not permanently stop the singleton consumer.
func runtimeRecoveryRetryable(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return true
	}
	if strings.Contains(strings.ToLower(err.Error()), "no rows in result set") || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	switch strings.ToUpper(strings.TrimSpace(err.Error())) {
	case "RUNTIME_RECOVERY_UNAVAILABLE", "INVALID_ARGUMENT":
		return false
	default:
		// PostgreSQL, Redis/Tair and Runtime control-plane failures can occur
		// after a healthy process has started. Keep the periodic controller
		// alive so it can retry the fenced ownership lease next interval.
		return true
	}
}

func (w RuntimeRecoveryWorker) validateRuntimeRecoveryStartup(ctx context.Context) error {
	if w.Repos == nil || w.Repos.AgentRuns == nil || w.Repos.Queue == nil || w.Hosts == nil || w.Locks == nil || len(w.SessionKeyEncryptionSecret) < 16 || strings.TrimSpace(w.OwnerInstanceID) == "" {
		return fmt.Errorf("RUNTIME_RECOVERY_UNAVAILABLE")
	}
	checkCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	health, err := w.Locks.Health(checkCtx)
	if err != nil || !health.OK || health.Backend != "redis_tair" {
		return fmt.Errorf("RUNTIME_RECOVERY_UNAVAILABLE")
	}
	return nil
}

func (w RuntimeRecoveryWorker) Reconcile(ctx context.Context) (map[string]any, error) {
	if w.Repos == nil || w.Repos.AgentRuns == nil || w.Repos.Queue == nil || w.Hosts == nil || w.Locks == nil || w.Scheduler == nil || w.Scheduler.Sessions == nil || w.Scheduler.Capacity == nil || len(w.SessionKeyEncryptionSecret) < 16 || strings.TrimSpace(w.OwnerInstanceID) == "" {
		return nil, fmt.Errorf("RUNTIME_RECOVERY_UNAVAILABLE")
	}
	owner := w.OwnerInstanceID
	const leaseTTL = 60 * time.Second
	lease, err := w.acquireReconcileLease(ctx, owner, leaseTTL)
	if err != nil {
		if runtimeRecoveryLeaseContended(err) {
			return map[string]any{"status": "skipped", "reason": "reconcile_owner_active"}, nil
		}
		return nil, err
	}
	reconcileCtx, cancel := context.WithCancel(ctx)
	renewErrors := make(chan error, 1)
	stopRenew := make(chan struct{})
	go func() {
		ticker := time.NewTicker(leaseTTL / 3)
		defer ticker.Stop()
		current := lease
		for {
			select {
			case <-stopRenew:
				return
			case <-reconcileCtx.Done():
				return
			case <-ticker.C:
				renewed, renewErr := w.Locks.Renew(reconcileCtx, current, leaseTTL)
				if renewErr != nil {
					select {
					case renewErrors <- renewErr:
					default:
					}
					cancel()
					return
				}
				current = renewed
			}
		}
	}()
	defer cancel()
	defer close(stopRenew)
	defer func() { _ = w.Locks.Release(context.Background(), lease) }()
	now := time.Now().UTC()
	if w.Now != nil {
		now = w.Now().UTC()
	}
	report, err := w.Hosts.ReconcileHostHealth(reconcileCtx, now, w.UnhealthyAfter, w.OfflineAfter)
	if err != nil {
		return nil, err
	}
	drainDispatches, err := w.Hosts.ListActiveDispatchesByHost(reconcileCtx, report.DrainDeadlineHostIDs)
	if err != nil {
		return nil, err
	}
	abortQueued := 0
	for _, dispatch := range drainDispatches {
		current, currentErr := w.Repos.AgentRuns.GetRunInternal(reconcileCtx, dispatch.RunID)
		if currentErr != nil {
			return nil, currentErr
		}
		if current.Status == "aborting" {
			if err := w.appendDrainAbortEvent(reconcileCtx, dispatch); err != nil {
				return nil, err
			}
			continue
		}
		decision, err := w.Repos.AgentRuns.RequestCancelAndEnqueue(reconcileCtx, dispatch.RunID, "LEASE_LOST", runtimeRecoveryReasonHash("drain_deadline:"+dispatch.RuntimeHostID), w.Repos.Queue)
		if err != nil {
			// Cancellation is a durable status-plus-queue transaction. A queue or
			// database failure is not recovery-owner contention, and must remain
			// visible so a later reconciler retries the entire atomic operation.
			return nil, fmt.Errorf("RUNTIME_ABORT_ENQUEUE_FAILED: %w", err)
		}
		if decision.Status != "aborting" {
			continue
		}
		if decision.AbortEnqueued {
			abortQueued++
		}
		if err := w.appendDrainAbortEvent(reconcileCtx, dispatch); err != nil {
			return nil, err
		}
	}

	offlineDispatches, err := w.Hosts.ListActiveDispatchesByHost(reconcileCtx, report.OfflineHostIDs)
	if err != nil {
		return nil, err
	}
	orphaned := 0
	for _, dispatch := range offlineDispatches {
		if err := w.Repos.AgentRuns.UpdateStatusVersioned(reconcileCtx, dispatch.RunID, []string{"reserving", "dispatched", "accepted", "materializing", "running", "finalizing", "aborting"}, "orphaned", map[string]any{"errorSummary": map[string]any{"code": "RUNTIME_HOST_OFFLINE"}}); err == nil {
			orphaned++
			_ = w.Repos.AgentRuns.AppendPublicEventIdempotent(reconcileCtx, persistence.AgentRunEvent{
				AgentRunID: dispatch.RunID, EventType: "orphaned", Status: "orphaned",
				SafeData: map[string]any{"status": "orphaned", "code": "RUNTIME_HOST_OFFLINE"},
			}, "runtime-host-orphaned:"+dispatch.DispatchID)
		} else if current, getErr := w.Repos.AgentRuns.GetRunInternal(reconcileCtx, dispatch.RunID); getErr != nil || current.Status != "orphaned" {
			return nil, err
		}
		reservation, getErr := w.Hosts.GetReservation(reconcileCtx, dispatch.ReservationID)
		if getErr != nil {
			return nil, getErr
		}
		if err := w.Hosts.FinalizeDispatchAndReleaseSlot(reconcileCtx, runtimepkg.DispatchTerminalCommand{
			Fence:      runtimepkg.ReservationFence{ReservationID: reservation.ReservationID, RuntimeHostID: reservation.RuntimeHostID, OwnerInstanceID: reservation.OwnerInstanceID, LeaseTokenHash: reservation.LeaseTokenHash, FencingToken: reservation.FencingToken},
			DispatchID: dispatch.DispatchID, TerminalStatus: "orphaned", ErrorCode: "RUNTIME_HOST_OFFLINE",
		}); err != nil && err.Error() != "STALE_FENCING_TOKEN" {
			return nil, err
		}
		// Accepted capacity cannot expire on its own: a live provider call may
		// still exist. Once this exact dispatch has durably converged to
		// orphaned, release its capacity and active session admission by their
		// fenced Run identity so Host loss cannot permanently consume admission.
		if _, err := w.releaseOrphanedRuntimeAdmissions(reconcileCtx, dispatch); err != nil {
			return nil, err
		}
	}
	orphanedAdmissionRecoveries, err := w.reconcileOrphanedRuntimeAdmissions(reconcileCtx)
	if err != nil {
		return nil, err
	}
	rotated := 0
	for _, hostID := range report.OfflineHostIDs {
		items, rotateErr := w.Hosts.RotateProductSessionsForHost(reconcileCtx, hostID, w.SessionKeyEncryptionSecret, "host_offline", "medium")
		if rotateErr != nil {
			return nil, rotateErr
		}
		rotated += len(items)
	}
	if err := w.Hosts.RecalculateHostCounters(reconcileCtx); err != nil {
		return nil, err
	}
	staleDispatches, statusRecovered, recoveryDeferred, err := w.reconcileStaleDispatches(reconcileCtx, now, report.OfflineHostIDs)
	if err != nil {
		return nil, err
	}
	// Stale-dispatch convergence can release a reservation after the first
	// accounting pass. Rebuild counters before the next heartbeat can admit work.
	if err := w.Hosts.RecalculateHostCounters(reconcileCtx); err != nil {
		return nil, err
	}
	incompleteConvergences, convergenceRequeued, convergenceBlocked, err := w.reconcileIncompleteConvergences(reconcileCtx)
	if err != nil {
		return nil, err
	}
	sessionScanned, sessionRecovering, sessionExpired, sessionAdmissionCleanupBackfilled := 0, 0, 0, 0
	sessionCleanupClaimed, sessionCleanupCompleted, sessionCleanupRetried, sessionCleanupStale := 0, 0, 0, 0
	sessionAdmissionCleanupClaimed, sessionAdmissionCleanupCompleted, sessionAdmissionCleanupRetried, sessionAdmissionCleanupStale := 0, 0, 0, 0
	capacityScanned, capacityExpired := 0, 0
	if w.Scheduler != nil {
		if w.Scheduler.Sessions != nil {
			sessionReport, recoverErr := w.Scheduler.Sessions.Recover(reconcileCtx, now, 200)
			if recoverErr != nil {
				return nil, recoverErr
			}
			sessionScanned, sessionRecovering, sessionExpired = sessionReport.Scanned, sessionReport.Recovering, sessionReport.Expired
			backfilled, cleanupErr := w.Scheduler.Sessions.BackfillAdmissionCleanup(reconcileCtx, 200)
			if cleanupErr != nil {
				return nil, cleanupErr
			}
			sessionAdmissionCleanupBackfilled = backfilled
			cleanupReport, cleanupErr := w.Scheduler.Sessions.DrainTerminalLeaseCleanup(reconcileCtx, "runtime-recovery:"+owner, 200)
			if cleanupErr != nil {
				return nil, cleanupErr
			}
			sessionCleanupClaimed, sessionCleanupCompleted = cleanupReport.Claimed, cleanupReport.Completed
			sessionCleanupRetried, sessionCleanupStale = cleanupReport.Retried, cleanupReport.Stale
			admissionCleanupReport, cleanupErr := w.Scheduler.Sessions.DrainAdmissionCleanup(reconcileCtx, "runtime-recovery:"+owner, 200)
			if cleanupErr != nil {
				return nil, cleanupErr
			}
			sessionAdmissionCleanupClaimed, sessionAdmissionCleanupCompleted = admissionCleanupReport.Claimed, admissionCleanupReport.Completed
			sessionAdmissionCleanupRetried, sessionAdmissionCleanupStale = admissionCleanupReport.Retried, admissionCleanupReport.Stale
		}
		if w.Scheduler.Capacity != nil {
			capacityReport, recoverErr := w.Scheduler.Capacity.Recover(reconcileCtx, now, 200)
			if recoverErr != nil {
				return nil, recoverErr
			}
			capacityScanned, capacityExpired = capacityReport.Scanned, capacityReport.Expired
		}
	}
	select {
	case renewErr := <-renewErrors:
		return nil, renewErr
	default:
	}
	return map[string]any{
		"unhealthyHosts": len(report.UnhealthyHostIDs), "offlineHosts": len(report.OfflineHostIDs),
		"drainDeadlineHosts": len(report.DrainDeadlineHostIDs), "expiredReservations": report.ExpiredReservations,
		"abortQueued": abortQueued, "orphanedRuns": orphaned, "rotatedSessions": rotated,
		"orphanedAdmissionRecoveries": orphanedAdmissionRecoveries,
		"staleDispatches":             staleDispatches, "statusRecovered": statusRecovered, "recoveryDeferred": recoveryDeferred,
		"incompleteConvergences": incompleteConvergences, "convergenceRequeued": convergenceRequeued, "convergenceBlocked": convergenceBlocked,
		"sessionAdmissionsScanned": sessionScanned, "sessionAdmissionsRecovering": sessionRecovering, "sessionAdmissionsExpired": sessionExpired,
		"sessionAdmissionCleanupBackfilled": sessionAdmissionCleanupBackfilled,
		"sessionTerminalCleanupClaimed":     sessionCleanupClaimed, "sessionTerminalCleanupCompleted": sessionCleanupCompleted,
		"sessionTerminalCleanupRetried": sessionCleanupRetried, "sessionTerminalCleanupStale": sessionCleanupStale,
		"sessionAdmissionCleanupClaimed": sessionAdmissionCleanupClaimed, "sessionAdmissionCleanupCompleted": sessionAdmissionCleanupCompleted,
		"sessionAdmissionCleanupRetried": sessionAdmissionCleanupRetried, "sessionAdmissionCleanupStale": sessionAdmissionCleanupStale,
		"capacityReservationsScanned": capacityScanned, "capacityReservationsExpired": capacityExpired,
	}, nil
}

func (w RuntimeRecoveryWorker) acquireReconcileLease(ctx context.Context, owner string, ttl time.Duration) (queue.DistributedLease, error) {
	if w.reconcileLeaseAcquire != nil {
		return w.reconcileLeaseAcquire(ctx, owner, ttl)
	}
	return w.Locks.Acquire(ctx, "runtime-reconcile:global", owner, "runtime-reconcile", ttl)
}

// runtimeRecoveryLeaseContended recognizes only an explicit lock-ownership
// conflict. It deliberately does not treat Redis/Tair transport, timeout, or
// generic dependency errors as contention: those must remain observable to
// the retry loop and readiness/alerting paths.
func runtimeRecoveryLeaseContended(err error) bool {
	if err == nil {
		return false
	}
	code := strings.ToUpper(strings.TrimSpace(err.Error()))
	return code == "SERVICE_BUSY" || strings.HasPrefix(code, "SERVICE_BUSY:")
}

func (w RuntimeRecoveryWorker) acquireDispatchRecoveryLease(ctx context.Context, dispatch runtimepkg.RuntimeDispatch, ttl time.Duration) (queue.DistributedLease, error) {
	owner := firstWorkerString(w.OwnerInstanceID, "runtime-reconcile")
	if w.dispatchRecoveryLeaseAcquire != nil {
		return w.dispatchRecoveryLeaseAcquire(ctx, owner, dispatch.RunID, ttl)
	}
	return w.Locks.Acquire(ctx, "runtime:dispatch-recovery:"+dispatch.DispatchID, owner, dispatch.RunID, ttl)
}

func (w RuntimeRecoveryWorker) appendDrainAbortEvent(ctx context.Context, dispatch runtimepkg.RuntimeDispatch) error {
	if err := w.Repos.AgentRuns.AppendPublicEventIdempotent(ctx, persistence.AgentRunEvent{
		AgentRunID: dispatch.RunID, EventType: "aborting", Status: "aborting",
		SafeData: map[string]any{"status": "aborting", "code": "RUNTIME_HOST_DRAIN_DEADLINE"},
	}, "runtime-drain-abort:"+dispatch.DispatchID); err != nil {
		return fmt.Errorf("RUNTIME_ABORT_EVENT_WRITE_FAILED: %w", err)
	}
	return nil
}

func (w RuntimeRecoveryWorker) reconcileIncompleteConvergences(ctx context.Context) (int, int, int, error) {
	if w.listIncompleteConvergences == nil && (w.Repos == nil || w.Repos.DB == nil || w.Scheduler == nil) {
		return 0, 0, 0, nil
	}
	var candidates []runtimepkg.TerminalConvergenceRecoveryCandidate
	var err error
	if w.listIncompleteConvergences != nil {
		candidates, err = w.listIncompleteConvergences(ctx, 200)
	} else {
		converger := runtimepkg.NewRuntimeTerminalConverger(w.Repos.DB, w.Hosts, w.Scheduler.Sessions, w.Scheduler.Capacity, w.Repos.Queue)
		candidates, err = converger.ListIncomplete(ctx, 200)
	}
	if err != nil {
		return 0, 0, 0, err
	}
	requeued, blocked := 0, 0
	for _, candidate := range candidates {
		var dispatch runtimepkg.RuntimeDispatch
		var dispatchErr error
		if w.getRecoveryDispatch != nil {
			dispatch, dispatchErr = w.getRecoveryDispatch(ctx, candidate.DispatchID)
		} else {
			dispatch, dispatchErr = w.Hosts.GetDispatch(ctx, candidate.DispatchID)
		}
		if dispatchErr != nil || dispatch.RunID != candidate.RunID {
			continue
		}
		var enqueued, candidateBlocked bool
		if w.enqueueTerminalRecovery != nil {
			enqueued, candidateBlocked, err = w.enqueueTerminalRecovery(ctx, dispatch, candidate.ConvergenceID)
		} else {
			enqueued, candidateBlocked, err = w.enqueueRuntimeTerminalConvergenceRecovery(ctx, dispatch, candidate.ConvergenceID)
		}
		if err != nil {
			return len(candidates), requeued, blocked, err
		}
		if candidateBlocked {
			blocked++
			continue
		}
		if enqueued {
			requeued++
		}
	}
	return len(candidates), requeued, blocked, nil
}

// enqueueRuntimeTerminalConvergenceRecovery creates a new durable event queue
// record for a terminal convergence. Its original runtime_events record may be
// dead-lettered or timed out and is therefore intentionally never reused.
func (w RuntimeRecoveryWorker) enqueueRuntimeTerminalConvergenceRecovery(ctx context.Context, dispatch runtimepkg.RuntimeDispatch, convergenceID string) (bool, bool, error) {
	if w.Repos == nil || w.Repos.Queue == nil {
		return false, false, fmt.Errorf("RUNTIME_RECOVERY_UNAVAILABLE")
	}
	result, err := w.Repos.Queue.EnqueueRuntimeTerminalRecovery(ctx, persistence.RuntimeTerminalRecoveryQueueCommand{
		ConvergenceID: convergenceID,
		DispatchID:    dispatch.DispatchID,
		RunID:         dispatch.RunID,
		RuntimeHostID: dispatch.RuntimeHostID,
	})
	if err != nil {
		return false, false, err
	}
	if result.Skipped || result.Deferred {
		return false, false, nil
	}
	if result.Blocked {
		return false, true, nil
	}
	if !runtimeRecoveryEventQueueEnqueued(result.Record, dispatch, result.QueueID) {
		return false, false, fmt.Errorf("RUNTIME_EVENT_RECOVERY_QUEUE_INVALID")
	}
	return true, false, nil
}

func (w RuntimeRecoveryWorker) reconcileStaleDispatches(ctx context.Context, now time.Time, offlineHostIDs []string) (int, int, int, error) {
	candidates, err := w.Hosts.ListDispatchRecoveryCandidates(ctx, now, 200)
	if err != nil {
		return 0, 0, 0, err
	}
	offline := map[string]bool{}
	for _, hostID := range offlineHostIDs {
		offline[hostID] = true
	}
	scanned, recovered, deferred := 0, 0, 0
	for _, dispatch := range candidates {
		if offline[dispatch.RuntimeHostID] {
			continue
		}
		host, hostErr := w.Hosts.GetHost(ctx, dispatch.RuntimeHostID)
		if hostErr != nil || !stringInWorker(host.Status, []string{"ready", "unhealthy", "draining"}) {
			continue
		}
		scanned++
		claimLease, claimErr := w.acquireDispatchRecoveryLease(ctx, dispatch, 30*time.Second)
		if claimErr != nil {
			if runtimeRecoveryLeaseContended(claimErr) {
				deferred++
				continue
			}
			return scanned, recovered, deferred, claimErr
		}
		claim := runtimepkg.DispatchRecoveryClaim{
			DispatchID: dispatch.DispatchID, OwnerInstanceID: claimLease.OwnerInstanceID,
			FencingToken: claimLease.FencingToken, ExpiresAt: now.Add(30 * time.Second),
			ExpectedState: dispatch.State, ExpectedVersion: dispatch.Version,
		}
		if claimErr = w.Hosts.ClaimDispatchRecovery(ctx, claim); claimErr != nil {
			_ = w.Locks.Release(ctx, claimLease)
			if claimErr.Error() == "STALE_FENCING_TOKEN" {
				deferred++
				continue
			}
			return scanned, recovered, deferred, claimErr
		}
		nextState, nextCheck := "recovering", now.Add(10*time.Second)
		if w.Client == nil || strings.TrimSpace(w.TicketSecret) == "" {
			_ = w.Hosts.AdvanceDispatchRecovery(ctx, claim, nextState, nextCheck)
			_ = w.Locks.Release(ctx, claimLease)
			deferred++
			continue
		}
		frozen, frozenErr := w.Repos.AgentRuns.GetWorkspaceContextByRunID(ctx, dispatch.RunID)
		if frozenErr != nil || frozen.CapabilityHash == "" {
			if err := w.orphanStalledDispatchWithRecoveryClaim(ctx, dispatch, "AGENT_PLAN_EXPIRED", claim); err != nil {
				_ = w.Hosts.AdvanceDispatchRecovery(ctx, claim, nextState, nextCheck)
				_ = w.Locks.Release(ctx, claimLease)
				if err.Error() == "STALE_FENCING_TOKEN" {
					deferred++
					continue
				}
				return scanned, recovered, deferred, err
			}
			_ = w.Locks.Release(ctx, claimLease)
			recovered++
			continue
		}
		run, runErr := w.Repos.AgentRuns.GetRunInternal(ctx, dispatch.RunID)
		plan, planErr := frozenDispatchPlanForRun(ctx, w.Repos.AgentRuns, run, dispatch.PlanVersion)
		planHash, planHashErr := runtimepkg.ComputeAgentRunPlanHash(plan)
		if runErr != nil || planErr != nil || planHashErr != nil || frozen.TenantID == "" || frozen.TenantID != run.TenantID || frozen.CapabilityHash != plan.CapabilityHash {
			if err := w.orphanStalledDispatchWithRecoveryClaim(ctx, dispatch, "AGENT_PLAN_EXPIRED", claim); err != nil {
				_ = w.Hosts.AdvanceDispatchRecovery(ctx, claim, nextState, nextCheck)
				_ = w.Locks.Release(ctx, claimLease)
				if err.Error() == "STALE_FENCING_TOKEN" {
					deferred++
					continue
				}
				return scanned, recovered, deferred, err
			}
			_ = w.Locks.Release(ctx, claimLease)
			recovered++
			continue
		}
		ticket, ticketErr := runtimepkg.SignRunTicket(runtimepkg.RunTicketClaims{
			RunID: dispatch.RunID, TenantID: frozen.TenantID, ReservationID: dispatch.ReservationID, RuntimeHostID: dispatch.RuntimeHostID,
			CapabilityHash: frozen.CapabilityHash, WorkspaceID: frozen.WorkspaceID, WorkspaceVersion: frozen.WorkspaceVersion,
			ContextGeneration: frozen.ContextGeneration, InputManifestHash: dispatch.InputManifestHash, PlanHash: planHash,
			FencingToken: dispatch.FencingToken, JTI: fmt.Sprintf("recovery-status-%s-%d", dispatch.DispatchID, now.UnixNano()),
			IssuedAt: now.Unix(), ExpiresAt: now.Add(5 * time.Minute).Unix(),
		}, w.TicketSecret)
		if ticketErr != nil {
			_ = w.Hosts.AdvanceDispatchRecovery(ctx, claim, nextState, nextCheck)
			_ = w.Locks.Release(ctx, claimLease)
			deferred++
			continue
		}
		terminalRecovered := false
		status, statusErr := w.Client.GetStatus(ctx, host, dispatch.RunID, ticket)
		if statusErr == nil && status.RunID == dispatch.RunID {
			if runtimeDispatchRecoveryTerminal(nil, dispatch, now) {
				if err := w.orphanStalledDispatchWithRecoveryClaim(ctx, dispatch, "RUNTIME_RUN_STALLED", claim); err != nil {
					_ = w.Hosts.AdvanceDispatchRecovery(ctx, claim, nextState, nextCheck)
					_ = w.Locks.Release(ctx, claimLease)
					if err.Error() == "STALE_FENCING_TOKEN" {
						deferred++
						continue
					}
					return scanned, recovered, deferred, err
				}
				recovered++
				terminalRecovered = true
			} else {
				nextState, nextCheck = "accepted", now.Add(5*time.Second)
				if enqueueErr := w.enqueueRuntimeEventRecovery(dispatch); enqueueErr != nil {
					_ = w.Hosts.AdvanceDispatchRecovery(ctx, claim, "recovering", now.Add(10*time.Second))
					_ = w.Locks.Release(ctx, claimLease)
					return scanned, recovered, deferred, enqueueErr
				}
				recovered++
			}
		} else if runtimeDispatchRecoveryTerminal(statusErr, dispatch, now) {
			if err := w.orphanStalledDispatchWithRecoveryClaim(ctx, dispatch, runtimeDispatchRecoveryErrorCode(statusErr), claim); err != nil {
				_ = w.Hosts.AdvanceDispatchRecovery(ctx, claim, nextState, nextCheck)
				_ = w.Locks.Release(ctx, claimLease)
				if err.Error() == "STALE_FENCING_TOKEN" {
					deferred++
					continue
				}
				return scanned, recovered, deferred, err
			}
			recovered++
			terminalRecovered = true
		} else {
			deferred++
		}
		if !terminalRecovered {
			if advanceErr := w.Hosts.AdvanceDispatchRecovery(ctx, claim, nextState, nextCheck); advanceErr != nil && advanceErr.Error() != "STALE_FENCING_TOKEN" {
				_ = w.Locks.Release(ctx, claimLease)
				return scanned, recovered, deferred, advanceErr
			}
		}
		_ = w.Locks.Release(ctx, claimLease)
	}
	return scanned, recovered, deferred, nil
}

func runtimeDispatchRecoveryTerminal(statusErr error, dispatch runtimepkg.RuntimeDispatch, now time.Time) bool {
	if runtimeDispatchStatusNotFound(statusErr) {
		return true
	}
	return !dispatch.CreatedAt.IsZero() && !dispatch.CreatedAt.Add(runtimeDispatchRecoveryMaxDeferral).After(now)
}

func runtimeDispatchStatusNotFound(err error) bool {
	if err == nil {
		return false
	}
	code := strings.ToUpper(strings.TrimSpace(err.Error()))
	return code == "NOT_FOUND" || code == "RUNTIME_RUN_NOT_FOUND" || strings.Contains(code, "RUNTIME_RUN_NOT_FOUND")
}

func runtimeDispatchRecoveryErrorCode(err error) string {
	if runtimeDispatchStatusNotFound(err) {
		return "RUNTIME_RUN_NOT_FOUND"
	}
	return "RUNTIME_RUN_STALLED"
}

func (w RuntimeRecoveryWorker) orphanStalledDispatch(ctx context.Context, dispatch runtimepkg.RuntimeDispatch, code string) error {
	return w.orphanStalledDispatchWithOptionalRecoveryClaim(ctx, dispatch, code, nil)
}

func (w RuntimeRecoveryWorker) orphanStalledDispatchWithRecoveryClaim(ctx context.Context, dispatch runtimepkg.RuntimeDispatch, code string, claim runtimepkg.DispatchRecoveryClaim) error {
	return w.orphanStalledDispatchWithOptionalRecoveryClaim(ctx, dispatch, code, &claim)
}

func (w RuntimeRecoveryWorker) orphanStalledDispatchWithOptionalRecoveryClaim(ctx context.Context, dispatch runtimepkg.RuntimeDispatch, code string, recoveryClaim *runtimepkg.DispatchRecoveryClaim) error {
	if w.Repos == nil || w.Repos.AgentRuns == nil || w.Scheduler == nil || w.Scheduler.Capacity == nil {
		return fmt.Errorf("RUNTIME_RECOVERY_UNAVAILABLE")
	}
	// A recovery-owned terminal transition must close the Host Slot while its
	// exact claim is still valid. Projection is deliberately downstream: if it
	// fails, the orphaned dispatch retains capacity/session admission and the
	// next reconcile cycle retries projection through the orphaned-admission
	// recovery path.
	if recoveryClaim != nil {
		if err := w.Hosts.AssertDispatchRecoveryClaim(ctx, *recoveryClaim); err != nil {
			return err
		}
		if err := w.finalizeOrphanedDispatch(ctx, dispatch, code, recoveryClaim); err != nil {
			return err
		}
		if err := w.projectOrphanedAgentRun(ctx, dispatch, code); err != nil {
			return err
		}
		_, err := w.releaseOrphanedRuntimeAdmissions(ctx, dispatch)
		return err
	}
	if err := w.projectOrphanedAgentRun(ctx, dispatch, code); err != nil {
		return err
	}
	if err := w.finalizeOrphanedDispatch(ctx, dispatch, code, nil); err != nil && err.Error() != "STALE_FENCING_TOKEN" {
		return err
	}
	_, err := w.releaseOrphanedRuntimeAdmissions(ctx, dispatch)
	return err
}

func (w RuntimeRecoveryWorker) projectOrphanedAgentRun(ctx context.Context, dispatch runtimepkg.RuntimeDispatch, code string) error {
	run, err := w.Repos.AgentRuns.GetRunInternal(ctx, dispatch.RunID)
	if err != nil {
		return err
	}
	errorSummary := map[string]any{"code": code, "retryable": true, "stage": "runtime_recovery"}
	if err := w.projectTerminal(run, "orphaned", map[string]any{}, errorSummary, map[string]any{}); err != nil && !w.terminalProductProjectionAlreadyOrphaned(run, err) {
		return err
	}
	if run.Status != "orphaned" {
		if err := w.Repos.AgentRuns.UpdateStatusVersioned(ctx, dispatch.RunID, []string{"planning", "queued", "reserving", "dispatched", "accepted", "materializing", "running", "finalizing", "aborting"}, "orphaned", map[string]any{"errorSummary": errorSummary}); err != nil && err.Error() != "AGENT_PLAN_EXPIRED" {
			return err
		}
	}
	if err := w.Repos.AgentRuns.AppendPublicEventIdempotent(ctx, persistence.AgentRunEvent{
		AgentRunID: dispatch.RunID, EventType: "orphaned", Status: "orphaned",
		SafeData: map[string]any{"status": "orphaned", "code": code},
	}, "runtime-recovery-orphaned:"+dispatch.DispatchID); err != nil {
		return err
	}
	return nil
}

func (w RuntimeRecoveryWorker) finalizeOrphanedDispatch(ctx context.Context, dispatch runtimepkg.RuntimeDispatch, code string, recoveryClaim *runtimepkg.DispatchRecoveryClaim) error {
	reservation, err := w.Hosts.GetReservation(ctx, dispatch.ReservationID)
	if err != nil {
		return err
	}
	fence := runtimepkg.ReservationFence{
		ReservationID: reservation.ReservationID, RuntimeHostID: reservation.RuntimeHostID,
		OwnerInstanceID: reservation.OwnerInstanceID, LeaseTokenHash: reservation.LeaseTokenHash,
		FencingToken: reservation.FencingToken,
	}
	if err := w.Hosts.FinalizeDispatchAndReleaseSlot(ctx, runtimepkg.DispatchTerminalCommand{
		Fence: fence, DispatchID: dispatch.DispatchID, TerminalStatus: "orphaned", ErrorCode: code,
		RecoveryClaim: recoveryClaim,
	}); err != nil {
		return err
	}
	return nil
}

func (w RuntimeRecoveryWorker) reconcileOrphanedRuntimeAdmissions(ctx context.Context) (int, error) {
	if w.Hosts == nil {
		return 0, fmt.Errorf("RUNTIME_RECOVERY_UNAVAILABLE")
	}
	dispatches, err := w.Hosts.ListOrphanedDispatchesNeedingAdmissionRecovery(ctx, 200)
	if err != nil {
		return 0, err
	}
	recovered := 0
	for _, dispatch := range dispatches {
		needsRecovery, needsErr := w.orphanedDispatchNeedsAdmissionRecovery(ctx, dispatch)
		if needsErr != nil {
			return recovered, needsErr
		}
		if !needsRecovery {
			continue
		}
		// A previous recovery process may have completed the Host Slot boundary
		// before its AgentRun/Product projection. Retry projection before any
		// admission release so a crash cannot create a terminal-slot success path
		// with a missing public terminal state.
		if err := w.projectOrphanedAgentRun(ctx, dispatch, "RUNTIME_RUN_STALLED"); err != nil {
			return recovered, err
		}
		changed, releaseErr := w.releaseOrphanedRuntimeAdmissions(ctx, dispatch)
		if releaseErr != nil {
			return recovered, releaseErr
		}
		if changed {
			recovered++
		}
	}
	return recovered, nil
}

func (w RuntimeRecoveryWorker) orphanedDispatchNeedsAdmissionRecovery(ctx context.Context, dispatch runtimepkg.RuntimeDispatch) (bool, error) {
	if w.Scheduler == nil || w.Scheduler.Capacity == nil {
		return false, fmt.Errorf("RUNTIME_RECOVERY_UNAVAILABLE")
	}
	if dispatch.HasCapacityReservationBinding() {
		capacity, err := w.Scheduler.Capacity.GetActiveByRunID(ctx, dispatch.RunID)
		if err == nil {
			if !dispatch.MatchesCapacityReservation(capacity) {
				return false, fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
			}
			return true, nil
		}
		if err.Error() != "NOT_FOUND" {
			return false, err
		}
	}
	if w.Scheduler.Sessions == nil {
		return false, nil
	}
	_, err := w.Scheduler.Sessions.ActiveHandleByRunID(ctx, dispatch.RunID)
	if err == nil {
		return true, nil
	}
	if runtimeSessionHandleAbsentAfterRestart(err) {
		return false, nil
	}
	return false, err
}

// releaseOrphanedRuntimeAdmissions closes the admission resources only after
// the Slot/dispatch terminal boundary has been durably fenced. Accepted
// capacity deliberately does not expire by TTL, so both offline-Host and
// status-backed orphan recovery must converge it explicitly.
func (w RuntimeRecoveryWorker) releaseOrphanedRuntimeAdmissions(ctx context.Context, dispatch runtimepkg.RuntimeDispatch) (bool, error) {
	if w.Scheduler == nil || w.Scheduler.Capacity == nil {
		return false, fmt.Errorf("RUNTIME_RECOVERY_UNAVAILABLE")
	}
	// Dispatches created before the capacity-binding migration own no capacity
	// admission to release. Treat that durable legacy fact as an idempotent no-op.
	if !dispatch.HasCapacityReservationBinding() {
		return false, nil
	}
	changed := false
	if capacity, err := w.Scheduler.Capacity.GetActiveByRunID(ctx, dispatch.RunID); err == nil {
		if !dispatch.MatchesCapacityReservation(capacity) {
			return false, fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
		}
		released, releaseErr := w.Scheduler.Capacity.Release(ctx, capacity, nil)
		if releaseErr != nil {
			return false, releaseErr
		}
		changed = changed || released
	} else if err.Error() != "NOT_FOUND" {
		return false, err
	}
	if w.Scheduler.Sessions == nil {
		return changed, nil
	}
	if admission, err := w.Scheduler.Sessions.ActiveHandleByRunID(ctx, dispatch.RunID); err == nil {
		released, releaseErr := w.Scheduler.Sessions.Release(ctx, admission, "orphaned")
		if releaseErr != nil && releaseErr.Error() != "STALE_FENCING_TOKEN" {
			return false, releaseErr
		}
		changed = changed || released
	} else if !runtimeSessionHandleAbsentAfterRestart(err) {
		return false, err
	}
	return changed, nil
}

func (w RuntimeRecoveryWorker) projectTerminal(run persistence.AgentRunRecord, status string, result, errorSummary, usage map[string]any) error {
	if w.terminalProjector != nil {
		return w.terminalProjector(run, status, result, errorSummary, usage)
	}
	return services.NewAgentRunProductProjector(w.Repos, time.Now).ProjectTerminal(run, status, result, errorSummary, usage)
}

// terminalProductProjectionAlreadyOrphaned accepts one historical split-brain
// state only: the same scoped Product task was already orphaned and its quota
// was settled. In that state AGENT_PLAN_EXPIRED means there is no Product work
// left to retry, so Runtime resources must still converge.
func (w RuntimeRecoveryWorker) terminalProductProjectionAlreadyOrphaned(run persistence.AgentRunRecord, projectionErr error) bool {
	if projectionErr == nil || strings.TrimSpace(projectionErr.Error()) != "AGENT_PLAN_EXPIRED" || run.TaskID == "" || w.Repos == nil || w.Repos.ChatTasks == nil || w.Repos.Usage == nil {
		return false
	}
	task, err := w.Repos.ChatTasks.GetAiTask(run.TaskID)
	if err != nil || strings.TrimSpace(stringValue(task["status"])) != "orphaned" {
		return false
	}
	if !runtimeRecoveryTaskMatchesRun(task, run) {
		return false
	}
	reservationID := runtimeRecoveryTaskQuotaReservationID(task)
	if reservationID == "" {
		return false
	}
	quotaReservation, err := w.Repos.Usage.GetQuotaReservation(reservationID)
	if err != nil || strings.TrimSpace(stringValue(quotaReservation["status"])) != "settled" {
		return false
	}
	if strings.TrimSpace(stringValue(quotaReservation["taskId"])) != run.TaskID {
		return false
	}
	if run.UserID != "" && strings.TrimSpace(stringValue(quotaReservation["userId"])) != run.UserID {
		return false
	}
	return run.WorkspaceID == "" || strings.TrimSpace(stringValue(quotaReservation["workspaceId"])) == run.WorkspaceID
}

func runtimeRecoveryTaskMatchesRun(task map[string]any, run persistence.AgentRunRecord) bool {
	if strings.TrimSpace(stringValue(task["taskId"])) != run.TaskID {
		return false
	}
	if run.UserID != "" && strings.TrimSpace(stringValue(task["userId"])) != run.UserID {
		return false
	}
	if run.WorkspaceID != "" && strings.TrimSpace(stringValue(task["workspaceId"])) != run.WorkspaceID {
		return false
	}
	refs := runtimeRecoveryTaskReferences(task)
	if linkedRunID := runtimeRecoveryReferenceString(refs, "agentRunId"); linkedRunID != "" && linkedRunID != run.AgentRunID {
		return false
	}
	if run.ThreadID == "" {
		return true
	}
	threadID := strings.TrimSpace(stringValue(task["threadId"]))
	if threadID == "" {
		threadID = runtimeRecoveryReferenceString(refs, "threadId")
	}
	return threadID == run.ThreadID
}

func runtimeRecoveryTaskQuotaReservationID(task map[string]any) string {
	refs := runtimeRecoveryTaskReferences(task)
	if reservationID := runtimeRecoveryReferenceString(refs, "reservationId"); reservationID != "" {
		return reservationID
	}
	for _, ref := range refs {
		quotaReservation, ok := ref["quotaReservation"].(map[string]any)
		if !ok {
			continue
		}
		if reservationID := strings.TrimSpace(stringValue(quotaReservation["reservationId"])); reservationID != "" {
			return reservationID
		}
	}
	return ""
}

func runtimeRecoveryTaskReferences(task map[string]any) []map[string]any {
	refs := make([]map[string]any, 0, 2)
	for _, value := range []any{task["inputSnapshot"], task["refs"]} {
		if ref, ok := value.(map[string]any); ok {
			refs = append(refs, ref)
		}
	}
	return refs
}

func runtimeRecoveryReferenceString(refs []map[string]any, key string) string {
	for _, ref := range refs {
		if value := strings.TrimSpace(stringValue(ref[key])); value != "" {
			return value
		}
	}
	return ""
}

// Admission handles intentionally live only in memory. After a worker restart
// an expired durable admission has no handle to release; the later Recover
// pass expires it once this dispatch has been terminalized.
func runtimeSessionHandleAbsentAfterRestart(err error) bool {
	if err == nil {
		return false
	}
	code := strings.ToUpper(strings.TrimSpace(err.Error()))
	return code == "NOT_FOUND" || code == "RUNTIME_SESSION_ADMISSION_UNAVAILABLE"
}

func (w RuntimeRecoveryWorker) enqueueRuntimeEventRecovery(dispatch runtimepkg.RuntimeDispatch, queueID ...string) error {
	if w.Repos == nil || w.Repos.Queue == nil {
		return fmt.Errorf("RUNTIME_RECOVERY_UNAVAILABLE")
	}
	targetQueueID := queue.QueueRuntimeEvents + ":" + dispatch.DispatchID
	if len(queueID) > 0 && strings.TrimSpace(queueID[0]) != "" {
		targetQueueID = strings.TrimSpace(queueID[0])
	}
	record := w.Repos.Queue.Enqueue(map[string]any{
		"queueId": targetQueueID, "queueName": queue.QueueRuntimeEvents,
		"taskType": "runtime_event_ingest", "taskId": dispatch.RunID, "dedupeKey": "runtime_event_ingest:" + dispatch.DispatchID,
		"priority": 150, "maxAttempts": 7200,
		"payload": map[string]any{"runId": dispatch.RunID, "dispatchId": dispatch.DispatchID, "runtimeHostId": dispatch.RuntimeHostID},
	})
	if runtimeRecoveryEventQueueEnqueued(record, dispatch, targetQueueID) {
		return nil
	}
	if workerMapString(record, "errorCode") == "QUEUE_DURABLE_BACKEND_UNAVAILABLE" {
		return fmt.Errorf("QUEUE_DURABLE_BACKEND_UNAVAILABLE")
	}
	return fmt.Errorf("RUNTIME_EVENT_RECOVERY_QUEUE_INVALID")
}

// runtimeRecoveryEventQueueEnqueued accepts only the canonical durable
// runtime-event record. A queue failure is a dependency failure, not a global
// recovery-owner contention, so it must never be collapsed into SERVICE_BUSY.
func runtimeRecoveryEventQueueEnqueued(record map[string]any, dispatch runtimepkg.RuntimeDispatch, queueID string) bool {
	if record == nil || dispatch.DispatchID == "" || dispatch.RunID == "" || queueID == "" ||
		workerMapString(record, "queueId") != queueID ||
		workerMapString(record, "queueName") != queue.QueueRuntimeEvents ||
		workerMapString(record, "taskType") != "runtime_event_ingest" ||
		workerMapString(record, "taskId") != dispatch.RunID ||
		workerMapString(record, "errorCode") == "QUEUE_DURABLE_BACKEND_UNAVAILABLE" {
		return false
	}
	payload := aiWorkerMap(record["payload"])
	isTerminalRecovery := workerMapString(payload, "terminalConvergenceId") != ""
	if isTerminalRecovery {
		if !strings.HasPrefix(workerMapString(record, "dedupeKey"), "runtime_terminal_recovery:") {
			return false
		}
	} else if queueID != queue.QueueRuntimeEvents+":"+dispatch.DispatchID || workerMapString(record, "dedupeKey") != "runtime_event_ingest:"+dispatch.DispatchID {
		return false
	}
	return stringInWorker(workerMapString(record, "status"), []string{"pending", "retry_wait", "leased", "running"}) &&
		workerMapString(payload, "runId") == dispatch.RunID &&
		workerMapString(payload, "dispatchId") == dispatch.DispatchID &&
		workerMapString(payload, "runtimeHostId") == dispatch.RuntimeHostID
}

func runtimeRecoveryReasonHash(value string) string {
	sum := sha256.Sum256([]byte(value))
	return "sha256:" + hex.EncodeToString(sum[:])
}
