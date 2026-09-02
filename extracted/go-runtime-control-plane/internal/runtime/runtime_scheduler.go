package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"huahuoai/backend/source/internal/domain"
	"huahuoai/backend/source/internal/persistence"
	"huahuoai/backend/source/internal/queue"
)

type ScheduleCommand struct {
	RunID               string
	OwnerInstanceID     string
	ExecutionScope      string
	CapabilityHash      string
	RuntimeVersion      string
	AdapterVersion      string
	RequiredTools       []string
	SessionBinding      ProductSessionHostBinding
	SessionAdmission    ProductSessionAdmissionCommand
	CapacityReservation RuntimeCapacityReservation
	ReservationTTL      time.Duration
}

type RuntimeReservationLease struct {
	Reservation      RuntimeSlotReservation
	Host             RuntimeHost
	Lease            queue.DistributedLease
	SessionAdmission *RuntimeSessionAdmissionLease
	Capacity         RuntimeCapacityReservation
}

type RuntimeDispatchRecoveryAction string

const (
	RuntimeRecoveryResumeEvents        RuntimeDispatchRecoveryAction = "resume_events"
	RuntimeRecoveryReleaseBeforeAccept RuntimeDispatchRecoveryAction = "release_before_accept"
	RuntimeRecoveryRetrySameHost       RuntimeDispatchRecoveryAction = "retry_same_host"
	RuntimeRecoveryOrphaned            RuntimeDispatchRecoveryAction = "orphaned"
	RuntimeRecoveryDeferred            RuntimeDispatchRecoveryAction = "deferred"
)

type RuntimeDispatchRecoveryDecision struct {
	Action           RuntimeDispatchRecoveryAction
	Dispatch         RuntimeDispatch
	OriginalState    string
	ReservationState string
	RuntimeStatus    AsyncRuntimeStatus
	ErrorCode        string
}

type RuntimeDispatchRecoveryOptions struct {
	OwnerInstanceID string
	LeaseTTL        time.Duration
	NextCheckDelay  time.Duration
	StatusTimeout   time.Duration
	SkipHostIDs     map[string]bool
	ResolveStatus   func(context.Context, RuntimeHost, RuntimeDispatch) (AsyncRuntimeStatus, error)
	OnDecision      func(context.Context, RuntimeDispatchRecoveryDecision) error
}

type RuntimeDispatchRecoveryReport struct {
	Scanned              int
	Claimed              int
	Resumed              int
	ReleasedBeforeAccept int
	RetrySameHost        int
	Orphaned             int
	Deferred             int
	Skipped              int
}

func (l RuntimeReservationLease) Fence() ReservationFence {
	return ReservationFence{
		ReservationID: l.Reservation.ReservationID, RuntimeHostID: l.Reservation.RuntimeHostID,
		OwnerInstanceID: l.Reservation.OwnerInstanceID, LeaseTokenHash: l.Reservation.LeaseTokenHash,
		FencingToken: l.Reservation.FencingToken,
	}
}

type RuntimeScheduler struct {
	Hosts           *RuntimeHostRepository
	Locks           *queue.DistributedLockManager
	Sessions        *RuntimeSessionAdmissionService
	Capacity        *RuntimeCapacityAdmissionService
	LeaseSupervisor *RuntimeLeaseSupervisor
	Recovery        RuntimeDispatchRecoveryOptions
	Now             func() time.Time
}

func NewRuntimeScheduler(hosts *RuntimeHostRepository, locks *queue.DistributedLockManager) *RuntimeScheduler {
	var db = hostsDatabase(hosts)
	return NewRuntimeSchedulerWithAdmissions(hosts, locks, NewRuntimeSessionAdmissionService(db, locks), NewRuntimeCapacityAdmissionService(db))
}

func NewRuntimeSchedulerWithAdmissions(hosts *RuntimeHostRepository, locks *queue.DistributedLockManager, sessions *RuntimeSessionAdmissionService, capacity *RuntimeCapacityAdmissionService) *RuntimeScheduler {
	scheduler := &RuntimeScheduler{Hosts: hosts, Locks: locks, Sessions: sessions, Capacity: capacity, Now: func() time.Time { return time.Now().UTC() }}
	scheduler.LeaseSupervisor = NewRuntimeLeaseSupervisor(scheduler)
	return scheduler
}

func (s *RuntimeScheduler) Reserve(ctx context.Context, command ScheduleCommand) (RuntimeReservationLease, error) {
	if s == nil || s.Hosts == nil || s.Locks == nil || s.Capacity == nil || command.RunID == "" || command.OwnerInstanceID == "" || command.CapabilityHash == "" || !stringInRuntime(command.ExecutionScope, []string{"product_thread", "detached_task"}) {
		return RuntimeReservationLease{}, domain.ErrorCode("RUNTIME_CAPACITY_UNAVAILABLE")
	}
	if command.CapacityReservation.RunID != command.RunID || s.Capacity.AssertActive(ctx, command.CapacityReservation) != nil {
		return RuntimeReservationLease{}, domain.ErrorCode("RUNTIME_CAPACITY_UNAVAILABLE")
	}
	ttl := command.ReservationTTL
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	var sessionLease *RuntimeSessionAdmissionLease
	if command.ExecutionScope == "product_thread" {
		if s.Sessions == nil || command.SessionBinding.TenantID == "" || command.SessionAdmission.Key.TenantID != command.SessionBinding.TenantID || command.SessionAdmission.RunID != command.RunID {
			return RuntimeReservationLease{}, domain.ErrorCode("RUNTIME_CAPACITY_UNAVAILABLE")
		}
		acquired, err := s.Sessions.Acquire(ctx, command.SessionAdmission)
		if err != nil {
			return RuntimeReservationLease{}, mapRuntimeScheduleError(err)
		}
		sessionLease = &acquired
	}
	runLease, err := s.Locks.Acquire(ctx, "runtime:run:"+command.RunID, command.OwnerInstanceID, command.RunID, ttl)
	if err != nil {
		if sessionLease != nil {
			_, _ = s.Sessions.Release(ctx, *sessionLease, "reservation_failed")
		}
		return RuntimeReservationLease{}, mapRuntimeScheduleError(err)
	}
	affinityHostID := ""
	if command.ExecutionScope == "product_thread" {
		if hostID, affinityErr := s.Hosts.GetProductSessionHost(ctx, command.SessionBinding); affinityErr == nil {
			affinityHostID = hostID
		}
	}
	reservationID := "reservation_" + stableRuntimeSchedulerID(command.RunID+":"+fmt.Sprint(runLease.FencingToken))
	reservation, host, err := s.Hosts.TryReserveSlot(ctx, AtomicReservationCommand{
		ReservationID: reservationID, RunID: command.RunID, OwnerInstanceID: command.OwnerInstanceID,
		ExecutionScope: command.ExecutionScope, CapabilityHash: command.CapabilityHash,
		RuntimeVersion: command.RuntimeVersion, AdapterVersion: command.AdapterVersion,
		RequiredTools: command.RequiredTools, AffinityRuntimeHostID: affinityHostID,
		LeaseTokenHash: runLease.TokenHash, FencingToken: runLease.FencingToken,
		ExpiresAt: runLease.ExpiresAt, HeartbeatAfter: s.now().Add(-30 * time.Second),
	})
	if err != nil {
		_ = s.Locks.Release(ctx, runLease)
		if sessionLease != nil {
			_, _ = s.Sessions.Release(ctx, *sessionLease, "reservation_failed")
		}
		return RuntimeReservationLease{}, mapRuntimeScheduleError(err)
	}
	handle := RuntimeReservationLease{Reservation: reservation, Host: host, Lease: runLease, SessionAdmission: sessionLease, Capacity: command.CapacityReservation}
	if sessionLease != nil {
		if err := s.Sessions.BindReservation(ctx, *sessionLease, reservation.ReservationID); err != nil {
			_ = s.ReleaseBeforeAccept(ctx, handle, "reservation_failed")
			return RuntimeReservationLease{}, err
		}
		binding := command.SessionBinding
		binding.RuntimeHostID = host.RuntimeHostID
		binding.SessionStoreID = host.SessionStoreID
		if err := s.Hosts.BindProductSessionHost(ctx, binding); err != nil {
			_ = s.ReleaseBeforeAccept(ctx, handle, "reservation_failed")
			return RuntimeReservationLease{}, err
		}
	}
	return handle, nil
}

func (s *RuntimeScheduler) Renew(ctx context.Context, handle RuntimeReservationLease, ttl time.Duration) (RuntimeReservationLease, error) {
	if ttl <= 0 || handle.Reservation.ReservationID == "" {
		return RuntimeReservationLease{}, fmt.Errorf("INVALID_ARGUMENT")
	}
	if err := s.Capacity.AssertActive(ctx, handle.Capacity); err != nil {
		return RuntimeReservationLease{}, err
	}
	lease, err := s.Locks.Renew(ctx, handle.Lease, ttl)
	if err != nil {
		return RuntimeReservationLease{}, mapRuntimeScheduleError(err)
	}
	handle.Lease = lease
	handle.Reservation.ExpiresAt = lease.ExpiresAt
	if err := s.Hosts.RenewReservation(ctx, handle.Fence(), lease.ExpiresAt); err != nil {
		return RuntimeReservationLease{}, mapRuntimeScheduleError(err)
	}
	if handle.SessionAdmission != nil {
		renewed, err := s.Sessions.Renew(ctx, *handle.SessionAdmission, ttl)
		if err != nil {
			return RuntimeReservationLease{}, mapRuntimeScheduleError(err)
		}
		handle.SessionAdmission = &renewed
	}
	return handle, nil
}

func (s *RuntimeScheduler) BindDispatch(ctx context.Context, handle RuntimeReservationLease, dispatchID string) error {
	if err := s.assertActive(ctx, handle); err != nil {
		return err
	}
	if handle.SessionAdmission != nil {
		return s.Sessions.BindDispatch(ctx, *handle.SessionAdmission, dispatchID)
	}
	return nil
}

func (s *RuntimeScheduler) Accept(ctx context.Context, handle RuntimeReservationLease, dispatchID, runtimeRequestID string) error {
	if s == nil || s.Hosts == nil || s.Capacity == nil {
		return domain.ErrorCode("RUNTIME_CAPACITY_UNAVAILABLE")
	}
	if err := s.assertActive(ctx, handle); err != nil {
		return err
	}
	command := DispatchAcceptedCommand{Fence: handle.Fence(), DispatchID: dispatchID, RuntimeRequestID: runtimeRequestID}
	if s.Hosts.postgresReady() || s.Capacity.postgresReady() {
		return s.acceptPostgres(ctx, handle.Capacity, command)
	}
	return s.acceptMemory(handle.Capacity, command)
}

// acceptPostgres makes the Capacity generation and the Host/Slot/Dispatch
// acceptance one durable fact. Submit already crossed the external Runtime
// boundary, so a database error rolls both writes back and leaves the original
// dispatch for submit_unknown recovery; sequential fallbacks are forbidden.
func (s *RuntimeScheduler) acceptPostgres(ctx context.Context, capacity RuntimeCapacityReservation, command DispatchAcceptedCommand) error {
	hostDB := hostsDatabase(s.Hosts)
	capacityDB := s.Capacity.DB
	if hostDB == nil || capacityDB == nil || hostDB.Disabled || capacityDB.Disabled || hostDB.Pool == nil || capacityDB.Pool == nil || hostDB.Pool != capacityDB.Pool {
		return domain.ErrorCode("RUNTIME_CAPACITY_UNAVAILABLE")
	}
	err := hostDB.WithSerializableRetry(ctx, "runtime_scheduler_accept", 3, func(tx *persistence.Tx) error {
		if err := s.Capacity.CommitAcceptedTx(ctx, tx, capacity); err != nil {
			return err
		}
		return s.Hosts.confirmDispatchAcceptedTx(ctx, tx, command)
	})
	return mapRuntimeAcceptError(err)
}

// acceptMemory holds both repositories while it validates Host acceptance,
// commits capacity, and applies the already-validated Host change. The Host
// mutation has no remaining failure point after Capacity succeeds.
func (s *RuntimeScheduler) acceptMemory(capacity RuntimeCapacityReservation, command DispatchAcceptedCommand) error {
	s.Capacity.mu.Lock()
	defer s.Capacity.mu.Unlock()
	s.Hosts.mu.Lock()
	defer s.Hosts.mu.Unlock()
	if err := s.Hosts.validateConfirmDispatchAcceptedMemoryLocked(command); err != nil {
		return mapRuntimeScheduleError(err)
	}
	if err := s.Capacity.commitAcceptedMemory(capacity); err != nil {
		return mapRuntimeScheduleError(err)
	}
	s.Hosts.applyConfirmDispatchAcceptedMemoryLocked(command)
	return nil
}

func (s *RuntimeScheduler) ReleaseBeforeAccept(ctx context.Context, handle RuntimeReservationLease, reason string) error {
	if handle.Reservation.ReservationID == "" {
		return fmt.Errorf("INVALID_ARGUMENT")
	}
	if s.LeaseSupervisor != nil {
		s.LeaseSupervisor.Stop(handle.Reservation.RunID)
	}
	var firstErr error
	if _, err := s.Hosts.ReleaseReservation(ctx, ReservationReleaseCommand{Fence: handle.Fence(), Reason: reason}); err != nil && err.Error() != "STALE_FENCING_TOKEN" {
		firstErr = err
	}
	if handle.SessionAdmission != nil {
		if _, err := s.Sessions.Release(ctx, *handle.SessionAdmission, normalizeAdmissionReleaseReason(reason)); err != nil && firstErr == nil && err.Error() != "STALE_FENCING_TOKEN" {
			firstErr = err
		}
	}
	if _, err := s.Capacity.Release(ctx, handle.Capacity, nil); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := s.Locks.Release(ctx, handle.Lease); err != nil && firstErr == nil && err.Error() != "STALE_FENCING_TOKEN" {
		firstErr = err
	}
	return firstErr
}

func (s *RuntimeScheduler) ReleaseAfterTerminal(ctx context.Context, handle RuntimeReservationLease) error {
	if handle.Reservation.ReservationID == "" {
		return fmt.Errorf("INVALID_ARGUMENT")
	}
	var firstErr error
	if handle.SessionAdmission != nil {
		if _, err := s.Sessions.Release(ctx, *handle.SessionAdmission, "recovered"); err != nil && err.Error() != "STALE_FENCING_TOKEN" {
			firstErr = err
		}
	}
	if err := s.Locks.Release(ctx, handle.Lease); err != nil && err.Error() != "STALE_FENCING_TOKEN" && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

func (s *RuntimeScheduler) DrainHost(ctx context.Context, hostID string, deadline time.Time) error {
	return s.Hosts.SetHostStatus(ctx, hostID, "draining", deadline)
}

func (s *RuntimeScheduler) RecoverStaleDispatches(ctx context.Context, now time.Time) error {
	_, err := s.RecoverStaleDispatchesWithOptions(ctx, now, s.Recovery)
	return err
}

func (s *RuntimeScheduler) RecoverStaleDispatchesWithOptions(ctx context.Context, now time.Time, options RuntimeDispatchRecoveryOptions) (RuntimeDispatchRecoveryReport, error) {
	report := RuntimeDispatchRecoveryReport{}
	if s == nil || s.Hosts == nil || s.Locks == nil || strings.TrimSpace(options.OwnerInstanceID) == "" || options.ResolveStatus == nil {
		return report, fmt.Errorf("RUNTIME_RECOVERY_UNAVAILABLE")
	}
	if _, err := s.Hosts.ExpireReservations(ctx, now); err != nil {
		return report, err
	}
	candidates, err := s.Hosts.ListDispatchRecoveryCandidates(ctx, now, 100)
	if err != nil {
		return report, err
	}
	for _, candidate := range candidates {
		if options.SkipHostIDs[candidate.RuntimeHostID] {
			report.Skipped++
			continue
		}
		report.Scanned++
		decision, claimed, recoverErr := s.recoverDispatchCandidate(ctx, now, candidate, options)
		if claimed {
			report.Claimed++
		}
		if recoverErr != nil {
			if stringInRuntime(recoverErr.Error(), []string{"SERVICE_BUSY", "STALE_FENCING_TOKEN"}) {
				report.Deferred++
				continue
			}
			return report, recoverErr
		}
		switch decision.Action {
		case RuntimeRecoveryResumeEvents:
			report.Resumed++
		case RuntimeRecoveryReleaseBeforeAccept:
			report.ReleasedBeforeAccept++
		case RuntimeRecoveryRetrySameHost:
			report.RetrySameHost++
		case RuntimeRecoveryOrphaned:
			report.Orphaned++
		default:
			report.Deferred++
		}
	}
	return report, nil
}

func (s *RuntimeScheduler) recoverDispatchCandidate(ctx context.Context, now time.Time, candidate RuntimeDispatch, options RuntimeDispatchRecoveryOptions) (RuntimeDispatchRecoveryDecision, bool, error) {
	ttl := options.LeaseTTL
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	nextDelay := options.NextCheckDelay
	if nextDelay <= 0 {
		nextDelay = 10 * time.Second
	}
	lease, err := s.Locks.Acquire(ctx, "runtime:dispatch-recovery:"+candidate.DispatchID, options.OwnerInstanceID, candidate.RunID, ttl)
	if err != nil {
		return RuntimeDispatchRecoveryDecision{Action: RuntimeRecoveryDeferred, Dispatch: candidate, OriginalState: candidate.State, ErrorCode: err.Error()}, false, err
	}
	defer func() { _ = s.Locks.Release(context.Background(), lease) }()
	claim := DispatchRecoveryClaim{
		DispatchID: candidate.DispatchID, OwnerInstanceID: lease.OwnerInstanceID, FencingToken: lease.FencingToken,
		ExpiresAt: lease.ExpiresAt, ExpectedState: candidate.State, ExpectedVersion: candidate.Version,
	}
	if err := s.Hosts.ClaimDispatchRecovery(ctx, claim); err != nil {
		return RuntimeDispatchRecoveryDecision{Action: RuntimeRecoveryDeferred, Dispatch: candidate, OriginalState: candidate.State, ErrorCode: err.Error()}, false, err
	}
	host, hostErr := s.Hosts.GetHost(ctx, candidate.RuntimeHostID)
	reservation, reservationErr := s.Hosts.GetReservation(ctx, candidate.ReservationID)
	if hostErr != nil || reservationErr != nil {
		decision := RuntimeDispatchRecoveryDecision{Action: RuntimeRecoveryDeferred, Dispatch: candidate, OriginalState: candidate.State, ErrorCode: "RUNTIME_CAPACITY_UNAVAILABLE"}
		_ = s.Hosts.AdvanceDispatchRecovery(ctx, claim, "recovering", now.Add(nextDelay))
		return decision, true, nil
	}
	timeout := options.StatusTimeout
	if timeout <= 0 || timeout >= ttl {
		timeout = ttl * 2 / 3
	}
	statusCtx, cancel := context.WithTimeout(ctx, timeout)
	status, statusErr := options.ResolveStatus(statusCtx, host, candidate)
	cancel()
	decision := classifyRuntimeDispatchRecovery(candidate, reservation, status, statusErr)
	if options.OnDecision != nil {
		if err := options.OnDecision(ctx, decision); err != nil {
			_ = s.Hosts.AdvanceDispatchRecovery(ctx, claim, "recovering", now.Add(nextDelay))
			return decision, true, err
		}
	}
	fence := ReservationFence{
		ReservationID: reservation.ReservationID, RuntimeHostID: reservation.RuntimeHostID,
		OwnerInstanceID: reservation.OwnerInstanceID, LeaseTokenHash: reservation.LeaseTokenHash,
		FencingToken: reservation.FencingToken,
	}
	switch decision.Action {
	case RuntimeRecoveryResumeEvents:
		err = s.Hosts.RecoverDispatchAccepted(ctx, DispatchRecoveryAcceptedCommand{
			Claim: claim, Fence: fence, RuntimeRequestID: status.RuntimeRequestID, NextCheckAt: now.Add(nextDelay),
		})
	case RuntimeRecoveryRetrySameHost:
		err = s.Hosts.AdvanceDispatchRecovery(ctx, claim, "retry_same_host", now.Add(nextDelay))
	case RuntimeRecoveryReleaseBeforeAccept:
		err = s.finalizeRecoveredDispatch(ctx, claim, candidate, reservation, "rejected", "RUNTIME_NOT_ACCEPTED")
	case RuntimeRecoveryOrphaned:
		err = s.finalizeRecoveredDispatch(ctx, claim, candidate, reservation, "orphaned", firstRuntimeString(decision.ErrorCode, "RUNTIME_RUN_STALLED"))
	default:
		err = s.Hosts.AdvanceDispatchRecovery(ctx, claim, "recovering", now.Add(nextDelay))
	}
	return decision, true, err
}

func (s *RuntimeScheduler) finalizeRecoveredDispatch(ctx context.Context, claim DispatchRecoveryClaim, dispatch RuntimeDispatch, reservation RuntimeSlotReservation, terminalStatus, errorCode string) error {
	fence := ReservationFence{
		ReservationID: reservation.ReservationID, RuntimeHostID: reservation.RuntimeHostID,
		OwnerInstanceID: reservation.OwnerInstanceID, LeaseTokenHash: reservation.LeaseTokenHash,
		FencingToken: reservation.FencingToken,
	}
	if err := s.Hosts.FinalizeDispatchAndReleaseSlot(ctx, DispatchTerminalCommand{Fence: fence, DispatchID: dispatch.DispatchID, TerminalStatus: terminalStatus, ErrorCode: errorCode, RecoveryClaim: &claim}); err != nil {
		return err
	}
	if s.Capacity != nil {
		capacity, err := s.Capacity.GetActiveByRunID(ctx, dispatch.RunID)
		if err == nil {
			if _, err = s.Capacity.Release(ctx, capacity, nil); err != nil {
				return err
			}
		} else if err.Error() != "NOT_FOUND" {
			return err
		}
	}
	if s.Sessions != nil {
		handle, err := s.Sessions.ActiveHandleByRunID(ctx, dispatch.RunID)
		if err == nil {
			if _, err = s.Sessions.Release(ctx, handle, normalizeAdmissionReleaseReason(terminalStatus)); err != nil && err.Error() != "STALE_FENCING_TOKEN" {
				return err
			}
		}
	}
	return nil
}

func classifyRuntimeDispatchRecovery(dispatch RuntimeDispatch, reservation RuntimeSlotReservation, status AsyncRuntimeStatus, statusErr error) RuntimeDispatchRecoveryDecision {
	decision := RuntimeDispatchRecoveryDecision{
		Action: RuntimeRecoveryDeferred, Dispatch: dispatch, OriginalState: dispatch.State,
		ReservationState: reservation.State, RuntimeStatus: status,
	}
	missing := statusErr != nil && runtimeRecoveryStatusNotFound(statusErr) || statusErr == nil && stringInRuntime(strings.ToLower(strings.TrimSpace(status.Status)), []string{"not_found", "missing"})
	postAccept := stringInRuntime(reservation.State, []string{"accepted", "running"}) || stringInRuntime(dispatch.State, []string{"accepted", "materializing", "running", "finalizing"})
	if statusErr != nil && !missing {
		decision.ErrorCode = statusErr.Error()
		return decision
	}
	if statusErr == nil && status.RunID != "" && status.RunID != dispatch.RunID {
		decision.ErrorCode = "RUNTIME_RUN_ID_MISMATCH"
		return decision
	}
	runtimeStatus := strings.ToLower(strings.TrimSpace(status.Status))
	if statusErr == nil && stringInRuntime(runtimeStatus, []string{"accepted", "materializing", "running", "finalizing", "aborting", "succeeded", "failed", "timeout", "aborted"}) {
		decision.Action = RuntimeRecoveryResumeEvents
		return decision
	}
	if statusErr == nil && runtimeStatus == "rejected" {
		if postAccept {
			decision.Action, decision.ErrorCode = RuntimeRecoveryOrphaned, "RUNTIME_STATUS_CONFLICT"
		} else {
			decision.Action, decision.ErrorCode = RuntimeRecoveryReleaseBeforeAccept, "RUNTIME_NOT_ACCEPTED"
		}
		return decision
	}
	if missing {
		decision.ErrorCode = "RUNTIME_RUN_NOT_FOUND"
		if postAccept {
			decision.Action = RuntimeRecoveryOrphaned
		} else if dispatch.State == "created" {
			decision.Action = RuntimeRecoveryReleaseBeforeAccept
		} else {
			decision.Action = RuntimeRecoveryRetrySameHost
		}
	}
	return decision
}

func runtimeRecoveryStatusNotFound(err error) bool {
	if err == nil {
		return false
	}
	code := strings.ToUpper(strings.TrimSpace(err.Error()))
	return code == "NOT_FOUND" || code == "RUNTIME_RUN_NOT_FOUND" || strings.Contains(code, "RUNTIME_RUN_NOT_FOUND")
}

func firstRuntimeString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func (s *RuntimeScheduler) assertActive(ctx context.Context, handle RuntimeReservationLease) error {
	if err := s.Locks.AssertActiveLease(ctx, handle.Lease, 0); err != nil {
		return mapRuntimeScheduleError(err)
	}
	if err := s.Capacity.AssertActive(ctx, handle.Capacity); err != nil {
		return err
	}
	if handle.SessionAdmission != nil {
		if err := s.Sessions.AssertActive(ctx, *handle.SessionAdmission); err != nil {
			return mapRuntimeScheduleError(err)
		}
	}
	return nil
}

func (s *RuntimeScheduler) now() time.Time {
	if s.Now != nil {
		return s.Now().UTC()
	}
	return time.Now().UTC()
}

func hostsDatabase(hosts *RuntimeHostRepository) *persistence.Database {
	if hosts == nil {
		return nil
	}
	return hosts.db
}

func stableRuntimeSchedulerID(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])[:24]
}

func mapRuntimeScheduleError(err error) error {
	if err == nil {
		return nil
	}
	switch err.Error() {
	case "STALE_FENCING_TOKEN":
		return domain.ErrorCode("STALE_FENCING_TOKEN")
	case "SERVICE_BUSY", "RUNTIME_SESSION_BUSY", "RUNTIME_CAPACITY_UNAVAILABLE", "DISTRIBUTED_LOCK_UNAVAILABLE":
		return domain.ErrorCode("RUNTIME_CAPACITY_UNAVAILABLE")
	default:
		return err
	}
}

func mapRuntimeAcceptError(err error) error {
	if err == nil {
		return nil
	}
	message := strings.ToUpper(strings.TrimSpace(err.Error()))
	if strings.Contains(message, "STALE_FENCING_TOKEN") {
		return domain.ErrorCode("STALE_FENCING_TOKEN")
	}
	if strings.Contains(message, "RUNTIME_CAPACITY_UNAVAILABLE") || strings.Contains(message, "RUNTIME_HOST_COUNTER_DRIFT") {
		return domain.ErrorCode("RUNTIME_CAPACITY_UNAVAILABLE")
	}
	return err
}

func normalizeAdmissionReleaseReason(reason string) string {
	switch reason {
	case "succeeded", "failed", "timeout", "aborted", "orphaned", "reservation_failed", "dispatch_failed", "lease_expired", "recovered":
		return reason
	default:
		return "dispatch_failed"
	}
}
