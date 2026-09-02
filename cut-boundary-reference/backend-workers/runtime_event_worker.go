package workers

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"huahuoai/backend/source/internal/persistence"
	"huahuoai/backend/source/internal/queue"
	runtimepkg "huahuoai/backend/source/internal/runtime"
	"huahuoai/backend/source/internal/services"
)

type RuntimeEventWorker struct {
	Repos        *persistence.Repositories
	Hosts        *runtimepkg.RuntimeHostRepository
	Scheduler    *runtimepkg.RuntimeScheduler
	Client       runtimepkg.AsyncOpenClawClient
	TicketSecret string
	Material     *services.MaterialService
	LeaseTTL     time.Duration
	IdleSleep    time.Duration
	PollDelay    time.Duration
	newConverger func() *runtimepkg.RuntimeTerminalConverger
}

func NewRuntimeEventWorker(repos *persistence.Repositories, hosts *runtimepkg.RuntimeHostRepository, scheduler *runtimepkg.RuntimeScheduler, client runtimepkg.AsyncOpenClawClient, ticketSecret string, material ...services.MaterialService) RuntimeEventWorker {
	worker := RuntimeEventWorker{
		Repos: repos, Hosts: hosts, Scheduler: scheduler, Client: client, TicketSecret: strings.TrimSpace(ticketSecret),
		LeaseTTL: 60 * time.Second, IdleSleep: 500 * time.Millisecond, PollDelay: time.Second,
	}
	if len(material) > 0 {
		worker.Material = &material[0]
	}
	worker.newConverger = func() *runtimepkg.RuntimeTerminalConverger {
		return runtimepkg.NewRuntimeTerminalConverger(repos.DB, hosts, runtimeSchedulerSessions(scheduler), runtimeSchedulerCapacity(scheduler), repos.Queue, repos.AgentRuns)
	}
	return worker
}

func (w RuntimeEventWorker) Run(ctx context.Context, workerID string) error {
	if w.Repos == nil || w.Repos.Queue == nil || w.Repos.AgentRuns == nil || w.Hosts == nil || w.Client == nil || w.TicketSecret == "" {
		return fmt.Errorf("RUNTIME_EVENT_INGESTOR_UNAVAILABLE")
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		record, proof, err := w.Repos.Queue.Lease(ctx, queue.QueueRuntimeEvents, workerID, w.LeaseTTL, "runtime_event_ingest")
		if errors.Is(err, persistence.ErrNoQueueWork) {
			if !sleepContext(ctx, w.IdleSleep) {
				return ctx.Err()
			}
			continue
		}
		if err != nil {
			return err
		}
		w.Process(ctx, record, proof)
	}
}

func (w RuntimeEventWorker) Process(ctx context.Context, queueRecord map[string]any, proof persistence.QueueLeaseProof) map[string]any {
	queueID := workerMapString(queueRecord, "queueId")
	payload := aiWorkerMap(queueRecord["payload"])
	runID := firstWorkerString(workerMapString(payload, "runId"), workerMapString(queueRecord, "taskId"))
	dispatchID := workerMapString(payload, "dispatchId")
	if queueID == "" || runID == "" || dispatchID == "" {
		return map[string]any{"status": "failed", "errorCode": "RUNTIME_INPUT_INVALID"}
	}
	if queueID != proof.QueueID {
		return map[string]any{"status": "failed", "errorCode": "STALE_QUEUE_LEASE"}
	}
	if _, err := w.Repos.Queue.MarkRunning(ctx, proof); err != nil {
		return map[string]any{"queueId": queueID, "agentRunId": runID, "status": "aborted", "errorCode": "STALE_QUEUE_LEASE"}
	}
	leaseCtx, heartbeat := startQueueRepositoryHeartbeat(ctx, w.Repos.Queue, proof, w.LeaseTTL)
	converger := w.terminalConverger()
	if recovery, found, recoveryErr := converger.FindIncompleteByQueueID(leaseCtx, queueID); recoveryErr != nil {
		return w.retry(heartbeat, runID, planningErrorCode(recoveryErr))
	} else if found {
		return w.resumeIncompleteTerminalConvergence(leaseCtx, heartbeat, converger, recovery, runID, dispatchID)
	}
	dispatch, err := w.Hosts.GetDispatch(leaseCtx, dispatchID)
	if err != nil || dispatch.RunID != runID {
		return w.retry(heartbeat, runID, "RUNTIME_RUN_STALLED")
	}
	// A terminal dispatch without an incomplete convergence has already crossed
	// the Runtime boundary. Re-reading an evicted Run only turns an obsolete
	// queue record into an unbounded RUNTIME_RUN_NOT_FOUND polling loop.
	if runtimeTerminalDispatchState(dispatch.State) {
		return w.completeTerminalDispatchQueue(ctx, heartbeat, queueID, runID, dispatch.State)
	}
	reservation, err := w.Hosts.GetReservation(leaseCtx, dispatch.ReservationID)
	if err != nil || reservation.FencingToken != dispatch.FencingToken || reservation.RuntimeHostID != dispatch.RuntimeHostID {
		return w.retry(heartbeat, runID, "STALE_FENCING_TOKEN")
	}
	host, err := w.Hosts.GetHost(leaseCtx, dispatch.RuntimeHostID)
	if err != nil {
		return w.retry(heartbeat, runID, "RUNTIME_CAPACITY_UNAVAILABLE")
	}
	frozen, err := w.Repos.AgentRuns.GetWorkspaceContextByRunID(leaseCtx, runID)
	if err != nil {
		return w.retry(heartbeat, runID, "AGENT_PLAN_EXPIRED")
	}
	run, err := w.Repos.AgentRuns.GetRunInternal(leaseCtx, runID)
	if err != nil {
		return w.retry(heartbeat, runID, "NOT_FOUND")
	}
	plan, err := w.frozenDispatchPlan(leaseCtx, run, dispatch.PlanVersion)
	if err != nil || frozen.TenantID == "" || frozen.TenantID != run.TenantID || frozen.CapabilityHash == "" || frozen.CapabilityHash != plan.CapabilityHash {
		return w.retry(heartbeat, runID, "AGENT_PLAN_INVALID")
	}
	planHash, err := runtimepkg.ComputeAgentRunPlanHash(plan)
	if err != nil {
		return w.retry(heartbeat, runID, "AGENT_PLAN_INVALID")
	}
	now := time.Now().UTC()
	ticket, err := runtimepkg.SignRunTicket(runtimepkg.RunTicketClaims{
		RunID: runID, TenantID: frozen.TenantID, ReservationID: dispatch.ReservationID, RuntimeHostID: dispatch.RuntimeHostID,
		CapabilityHash: frozen.CapabilityHash, WorkspaceID: frozen.WorkspaceID, WorkspaceVersion: frozen.WorkspaceVersion,
		ContextGeneration: frozen.ContextGeneration, InputManifestHash: dispatch.InputManifestHash, PlanHash: planHash,
		FencingToken: dispatch.FencingToken, JTI: fmt.Sprintf("event-read-%s-%d", dispatchID, now.UnixNano()),
		IssuedAt: now.Unix(), ExpiresAt: now.Add(5 * time.Minute).Unix(),
	}, w.TicketSecret)
	if err != nil {
		return w.retry(heartbeat, runID, "RUNTIME_PERMISSION_DENIED")
	}
	cursor, err := w.Hosts.GetDispatchEventCursor(leaseCtx, dispatchID)
	if err != nil {
		return w.retry(heartbeat, runID, "RUNTIME_RUN_STALLED")
	}
	after, err := w.ingestEventPages(leaseCtx, host, runID, dispatchID, ticket, cursor.LastSequence)
	if err != nil {
		return w.retry(heartbeat, runID, planningErrorCode(err))
	}
	status, err := w.Client.GetStatus(leaseCtx, host, runID, ticket)
	if err != nil {
		return w.retry(heartbeat, runID, planningErrorCode(err))
	}
	status = normalizeUserCancellationTimeout(run, status)
	if !runtimeTerminalStatus(status.Status) {
		return w.retry(heartbeat, runID, "")
	}
	if status.LastEventSequence > after {
		return w.retry(heartbeat, runID, "RUNTIME_EVENT_GAP")
	}
	terminalSequence := status.LastEventSequence
	if terminalSequence <= 0 {
		terminalSequence = after
	}
	if after > terminalSequence {
		terminalSequence = after
	}
	if terminalSequence <= 0 {
		return w.retry(heartbeat, runID, "RUNTIME_EVENT_GAP")
	}
	projector := services.NewAgentRunProductProjector(w.Repos, time.Now)
	if w.Material != nil {
		projector = projector.WithMaterialService(*w.Material)
	}
	effectiveStatus, effectiveResult, effectiveError, normalizeErr := projector.NormalizeTerminalForPlan(run, plan, status.Status, status.Result, status.Error)
	if normalizeErr != nil {
		return w.retry(heartbeat, runID, planningErrorCode(normalizeErr))
	}
	next := runtimeTerminalAgentRunStatus(effectiveStatus)
	publicResult := runtimeTerminalPublicResult(run, plan, effectiveResult)
	if w.Scheduler == nil || w.Scheduler.Capacity == nil {
		return w.retry(heartbeat, runID, "RUNTIME_CAPACITY_UNAVAILABLE")
	}
	capacityReservation, err := w.Scheduler.Capacity.GetActiveByRunID(leaseCtx, runID)
	if err != nil {
		capacityReservation, err = w.Scheduler.Capacity.GetLatestByRunID(leaseCtx, runID)
		if err != nil {
			return w.retry(heartbeat, runID, "RUNTIME_CAPACITY_UNAVAILABLE")
		}
	}
	if !dispatch.MatchesCapacityReservation(capacityReservation) {
		return w.retry(heartbeat, runID, "RUNTIME_CAPACITY_UNAVAILABLE")
	}
	// The active row advances once on acceptance. Terminal convergence snapshots
	// the dispatch's original reserved generation, while Release accepts that
	// generation or its immediate accepted revision.
	terminalCapacityReservation := capacityReservation
	terminalCapacityReservation.Version = dispatch.CapacityReservedVersion
	sessionRequired, sessionScopeErr := runtimeSessionRequiredForExecutionScope(plan.ExecutionScope)
	if sessionScopeErr != nil {
		return w.retry(heartbeat, runID, "AGENT_PLAN_INVALID")
	}
	var sessionAdmission *runtimepkg.RuntimeSessionAdmissionLease
	if sessionRequired && w.Scheduler.Sessions != nil {
		handle, handleErr := w.Scheduler.Sessions.ActiveHandleByRunID(leaseCtx, runID)
		if handleErr == nil {
			sessionAdmission = &handle
		}
	}
	converger.ProjectProduct = func(stepCtx context.Context, _ runtimepkg.TerminalConvergenceCommand, convergenceID string) error {
		if err := stepCtx.Err(); err != nil {
			return err
		}
		return projector.ProjectTerminalWithPlanAndConvergence(stepCtx, run, plan, next, effectiveResult, effectiveError, status.Usage, convergenceID)
	}
	converger.ConvergeAgentRun = func(stepCtx context.Context, _ runtimepkg.TerminalConvergenceCommand, _ string) error {
		current, getErr := w.Repos.AgentRuns.GetRunInternal(stepCtx, runID)
		if getErr != nil {
			return getErr
		}
		if current.Status != next {
			patch := map[string]any{}
			if next == "succeeded" {
				patch["publicResult"] = publicResult
			} else {
				patch["errorSummary"] = effectiveError
			}
			if err := w.Repos.AgentRuns.UpdateStatusVersioned(stepCtx, runID, []string{"reserving", "dispatched", "accepted", "materializing", "running", "finalizing", "aborting", "orphaned"}, next, patch); err != nil {
				return err
			}
		}
		plan, getPlanErr := w.Repos.AgentRuns.GetPlan(stepCtx, runID, dispatch.PlanVersion)
		if getPlanErr != nil {
			return getPlanErr
		}
		terminalPlanStatus := runtimePlanTerminalStatus(next)
		if workerMapString(plan, "status") == terminalPlanStatus {
			return nil
		}
		return w.Repos.AgentRuns.MarkPlanStatus(stepCtx, runID, dispatch.PlanVersion, "executing", terminalPlanStatus)
	}
	converger.AppendPublicEvent = func(stepCtx context.Context, _ runtimepkg.TerminalConvergenceCommand, idempotencyKey string) error {
		return w.Repos.AgentRuns.AppendPublicEventIdempotent(stepCtx, persistence.AgentRunEvent{AgentRunID: runID, EventType: next, Status: next, SafeData: map[string]any{"status": next, "result": publicResult, "error": effectiveError}}, idempotencyKey)
	}
	converger.FinalQueueProof = w.terminalQueueProof(heartbeat)
	converger.CompleteQueue = w.terminalQueueCompleter(heartbeat)
	_, err = converger.Converge(leaseCtx, runtimepkg.TerminalConvergenceCommand{
		DispatchID: dispatchID, RunID: runID, TerminalSourceSequence: terminalSequence, TerminalStatus: status.Status,
		SafeResult: status.Result, SafeError: status.Error, ActualUsage: status.Usage, QueueProof: heartbeat.Proof(),
		DispatchTerminal: runtimepkg.DispatchTerminalCommand{Fence: runtimepkg.ReservationFence{ReservationID: reservation.ReservationID, RuntimeHostID: reservation.RuntimeHostID, OwnerInstanceID: reservation.OwnerInstanceID, LeaseTokenHash: reservation.LeaseTokenHash, FencingToken: reservation.FencingToken}, DispatchID: dispatchID, TerminalStatus: status.Status, ErrorCode: workerMapString(status.Error, "code")},
		SessionAdmission: sessionAdmission, SessionRequired: sessionRequired, CapacityReservation: terminalCapacityReservation,
		AgentRunTerminal: &runtimepkg.TerminalAgentRunProjection{
			AgentRunStatus: next, PlanVersion: dispatch.PlanVersion, PlanStatus: runtimePlanTerminalStatus(next),
			PublicResult: publicResult, ErrorSummary: effectiveError,
			PublicEvent: persistence.AgentRunEvent{AgentRunID: runID, EventType: next, Status: next, SafeData: map[string]any{"status": next, "result": publicResult, "error": effectiveError}},
		},
	})
	if err != nil {
		terminalProof, heartbeatErr := heartbeat.Stop()
		if heartbeatErr != nil {
			return map[string]any{"queueId": queueID, "agentRunId": runID, "status": "aborted", "errorCode": "STALE_QUEUE_LEASE"}
		}
		if _, retryErr := w.Repos.Queue.ScheduleRetry(context.Background(), terminalProof, w.PollDelay, planningErrorCode(err)); retryErr != nil {
			return map[string]any{"queueId": queueID, "agentRunId": runID, "status": "aborted", "errorCode": "STALE_QUEUE_LEASE"}
		}
		return map[string]any{"queueId": queueID, "agentRunId": runID, "status": "polling", "errorCode": planningErrorCode(err), "retryable": true}
	}
	if w.Scheduler.LeaseSupervisor != nil {
		w.Scheduler.LeaseSupervisor.Stop(runID)
	}
	return map[string]any{"queueId": queueID, "agentRunId": runID, "status": next, "lastSourceSequence": status.LastEventSequence}
}

func (w RuntimeEventWorker) resumeIncompleteTerminalConvergence(ctx context.Context, heartbeat *queueRepositoryHeartbeat, converger *runtimepkg.RuntimeTerminalConverger, recovery runtimepkg.TerminalConvergenceRecovery, expectedRunID, expectedDispatchID string) map[string]any {
	if recovery.RunID != expectedRunID || recovery.DispatchID != expectedDispatchID || recovery.QueueID == "" || converger == nil || w.Scheduler == nil {
		return w.retry(heartbeat, expectedRunID, "RUNTIME_EVENT_GAP")
	}
	dispatch, err := w.Hosts.GetDispatch(ctx, recovery.DispatchID)
	if err != nil || dispatch.RunID != recovery.RunID {
		return w.retry(heartbeat, expectedRunID, "RUNTIME_RUN_STALLED")
	}
	if legacyTerminalRecoveryHasUnsafeCapacityContract(recovery, dispatch) {
		return w.blockLegacyTerminalRecoveryContract(heartbeat, recovery)
	}
	run, err := w.Repos.AgentRuns.GetRunInternal(ctx, recovery.RunID)
	if err != nil {
		return w.retry(heartbeat, expectedRunID, "NOT_FOUND")
	}
	plan, err := w.frozenDispatchPlan(ctx, run, dispatch.PlanVersion)
	if err != nil {
		return w.retry(heartbeat, expectedRunID, planningErrorCode(err))
	}
	sessionRequired, sessionScopeErr := runtimeSessionRequiredForExecutionScope(plan.ExecutionScope)
	if sessionScopeErr != nil || recovery.SessionRequired != sessionRequired {
		return w.retry(heartbeat, expectedRunID, "AGENT_PLAN_INVALID")
	}
	var sessionAdmission *runtimepkg.RuntimeSessionAdmissionLease
	if sessionRequired && w.Scheduler.Sessions != nil {
		if handle, handleErr := w.Scheduler.Sessions.ActiveHandleByRunID(ctx, recovery.RunID); handleErr == nil {
			sessionAdmission = &handle
		}
	}
	legacyTailOnly := false
	if legacyTerminalRecoveryMayCompleteWithoutCapacity(recovery, dispatch, run) {
		slot, slotErr := w.Hosts.GetReservation(ctx, dispatch.ReservationID)
		sessionTerminal := !sessionRequired
		if sessionRequired && w.Scheduler.Sessions != nil {
			released, releasedErr := w.Scheduler.Sessions.ReleasedOrExpiredByRunID(ctx, recovery.RunID)
			sessionTerminal = releasedErr == nil && released
		}
		legacyTailOnly = legacyTerminalTailOnlyRecoveryEligible(recovery, dispatch, run, slot, slotErr == nil, sessionTerminal)
	}
	terminalCapacityReservation := runtimepkg.RuntimeCapacityReservation{}
	if !legacyTailOnly {
		if w.Scheduler.Capacity == nil {
			return w.retry(heartbeat, expectedRunID, "RUNTIME_CAPACITY_UNAVAILABLE")
		}
		capacityReservation, capacityErr := w.Scheduler.Capacity.GetLatestByRunID(ctx, recovery.RunID)
		if capacityErr != nil {
			if !recovery.UsageSettled || recovery.CapacityReservationID == "" || recovery.CapacitySnapshotVersion < 1 || recovery.CapacityReservedVersion < 1 {
				return w.retry(heartbeat, expectedRunID, "RUNTIME_CAPACITY_UNAVAILABLE")
			}
			capacityReservation = runtimepkg.RuntimeCapacityReservation{
				ReservationID: recovery.CapacityReservationID, RunID: recovery.RunID, SnapshotVersion: recovery.CapacitySnapshotVersion, Version: recovery.CapacityReservedVersion,
			}
		}
		if !dispatch.HasCapacityReservationBinding() || recovery.CapacityReservationID != dispatch.CapacityReservationID || recovery.CapacityReservedVersion != dispatch.CapacityReservedVersion || capacityReservation.ReservationID != recovery.CapacityReservationID || capacityReservation.SnapshotVersion != recovery.CapacitySnapshotVersion {
			return w.retry(heartbeat, expectedRunID, "RUNTIME_CAPACITY_UNAVAILABLE")
		}
		if !dispatch.MatchesCapacityReservation(capacityReservation) {
			return w.retry(heartbeat, expectedRunID, "RUNTIME_CAPACITY_UNAVAILABLE")
		}
		// Reconstruct the original generation from the immutable snapshot. The
		// live reservation may be the one-step accepted revision, which must not
		// change the persisted terminal snapshot hash on a retry.
		terminalCapacityReservation = capacityReservation
		terminalCapacityReservation.Version = recovery.CapacityReservedVersion
	}
	projector := services.NewAgentRunProductProjector(w.Repos, time.Now)
	if w.Material != nil {
		projector = projector.WithMaterialService(*w.Material)
	}
	effectiveStatus, effectiveResult, effectiveError, normalizeErr := projector.NormalizeTerminalForPlan(run, plan, recovery.TerminalStatus, recovery.SafeResult, recovery.SafeError)
	if normalizeErr != nil {
		return w.retry(heartbeat, expectedRunID, planningErrorCode(normalizeErr))
	}
	next := runtimeTerminalAgentRunStatus(effectiveStatus)
	publicResult := runtimeTerminalPublicResult(run, plan, effectiveResult)
	converger.ProjectProduct = func(stepCtx context.Context, _ runtimepkg.TerminalConvergenceCommand, convergenceID string) error {
		if err := stepCtx.Err(); err != nil {
			return err
		}
		return projector.ProjectTerminalWithPlanAndConvergence(stepCtx, run, plan, next, effectiveResult, effectiveError, recovery.ActualUsage, convergenceID)
	}
	converger.ConvergeAgentRun = func(stepCtx context.Context, _ runtimepkg.TerminalConvergenceCommand, _ string) error {
		current, getErr := w.Repos.AgentRuns.GetRunInternal(stepCtx, recovery.RunID)
		if getErr != nil {
			return getErr
		}
		if current.Status != next {
			patch := map[string]any{}
			if next == "succeeded" {
				patch["publicResult"] = publicResult
			} else {
				patch["errorSummary"] = effectiveError
			}
			if err := w.Repos.AgentRuns.UpdateStatusVersioned(stepCtx, recovery.RunID, []string{"reserving", "dispatched", "accepted", "materializing", "running", "finalizing", "aborting", "orphaned"}, next, patch); err != nil {
				return err
			}
		}
		plan, getPlanErr := w.Repos.AgentRuns.GetPlan(stepCtx, recovery.RunID, dispatch.PlanVersion)
		if getPlanErr != nil {
			return getPlanErr
		}
		terminalPlanStatus := runtimePlanTerminalStatus(next)
		if workerMapString(plan, "status") == terminalPlanStatus {
			return nil
		}
		return w.Repos.AgentRuns.MarkPlanStatus(stepCtx, recovery.RunID, dispatch.PlanVersion, "executing", terminalPlanStatus)
	}
	converger.AppendPublicEvent = func(stepCtx context.Context, _ runtimepkg.TerminalConvergenceCommand, idempotencyKey string) error {
		return w.Repos.AgentRuns.AppendPublicEventIdempotent(stepCtx, persistence.AgentRunEvent{AgentRunID: recovery.RunID, EventType: next, Status: next, SafeData: map[string]any{"status": next, "result": publicResult, "error": effectiveError}}, idempotencyKey)
	}
	converger.FinalQueueProof = w.terminalQueueProof(heartbeat)
	converger.CompleteQueue = w.terminalQueueCompleter(heartbeat)
	_, err = converger.Converge(ctx, runtimepkg.TerminalConvergenceCommand{
		DispatchID: recovery.DispatchID, RunID: recovery.RunID, TerminalSourceSequence: recovery.TerminalSourceSequence, TerminalStatus: recovery.TerminalStatus,
		SafeResult: recovery.SafeResult, SafeError: recovery.SafeError, ActualUsage: recovery.ActualUsage, QueueProof: heartbeat.Proof(), OriginalQueueID: recovery.QueueID,
		DispatchTerminal: runtimepkg.DispatchTerminalCommand{Fence: runtimepkg.ReservationFence{ReservationID: dispatch.ReservationID, RuntimeHostID: dispatch.RuntimeHostID, OwnerInstanceID: dispatch.OwnerInstanceID, LeaseTokenHash: dispatch.LeaseTokenHash, FencingToken: dispatch.FencingToken}, DispatchID: dispatch.DispatchID, TerminalStatus: recovery.TerminalStatus, ErrorCode: workerMapString(recovery.SafeError, "code")},
		SessionAdmission: sessionAdmission, SessionRequired: sessionRequired, CapacityReservation: terminalCapacityReservation,
		LegacyTailOnlyRecovery: legacyTailOnly,
		AgentRunTerminal: &runtimepkg.TerminalAgentRunProjection{
			AgentRunStatus: next, PlanVersion: dispatch.PlanVersion, PlanStatus: runtimePlanTerminalStatus(next),
			PublicResult: publicResult, ErrorSummary: effectiveError,
			PublicEvent: persistence.AgentRunEvent{AgentRunID: recovery.RunID, EventType: next, Status: next, SafeData: map[string]any{"status": next, "result": publicResult, "error": effectiveError}},
		},
	})
	if err != nil {
		terminalProof, heartbeatErr := heartbeat.Stop()
		if heartbeatErr != nil {
			return map[string]any{"queueId": recovery.QueueID, "agentRunId": recovery.RunID, "status": "aborted", "errorCode": "STALE_QUEUE_LEASE"}
		}
		if _, retryErr := w.Repos.Queue.ScheduleRetry(context.Background(), terminalProof, w.PollDelay, planningErrorCode(err)); retryErr != nil {
			return map[string]any{"queueId": recovery.QueueID, "agentRunId": recovery.RunID, "status": "aborted", "errorCode": "STALE_QUEUE_LEASE"}
		}
		return map[string]any{"queueId": recovery.QueueID, "agentRunId": recovery.RunID, "status": "polling", "errorCode": planningErrorCode(err), "retryable": true}
	}
	if w.Scheduler.LeaseSupervisor != nil {
		w.Scheduler.LeaseSupervisor.Stop(recovery.RunID)
	}
	return map[string]any{"queueId": recovery.QueueID, "agentRunId": recovery.RunID, "status": next, "lastSourceSequence": recovery.TerminalSourceSequence, "recoveredTerminalConvergence": true}
}

func (w RuntimeEventWorker) ingestEventPages(ctx context.Context, host runtimepkg.RuntimeHost, runID, dispatchID, ticket string, after int64) (int64, error) {
	for pageNumber := 0; pageNumber < 100; pageNumber++ {
		pageStart := after
		page, err := w.Client.ListEvents(ctx, host, runID, ticket, after, 500, 20000)
		if err != nil {
			return after, err
		}
		if page.Gap || page.OldestAvailableSequence > 0 && after+1 < page.OldestAvailableSequence {
			return after, fmt.Errorf("RUNTIME_EVENT_GAP")
		}
		if page.LatestSequence > 0 && page.LatestSequence < after ||
			page.TerminalSequence > 0 && page.LatestSequence > 0 && page.TerminalSequence > page.LatestSequence {
			return after, fmt.Errorf("RUNTIME_EVENT_GAP")
		}
		for _, source := range page.Items {
			if source.Sequence != after+1 {
				return after, fmt.Errorf("RUNTIME_EVENT_GAP")
			}
			occurredAt := source.Timestamp
			if occurredAt.IsZero() {
				occurredAt = source.OccurredAt
			}
			eventType, visibility := runtimeEventStorageRoute(source.EventType)
			safe, usage, payloadErr := runtimeEventPayloadForStorage(source)
			if payloadErr != nil {
				return after, payloadErr
			}
			if err := w.Hosts.AppendRunEventAndAdvanceCursor(ctx, runtimepkg.RuntimeHostRunEvent{
				EventID: fmt.Sprintf("runtime_event_%s_%d", dispatchID, source.Sequence), RunID: runID,
				DispatchID: dispatchID, RuntimeHostID: host.RuntimeHostID, SourceSequence: source.Sequence,
				EventType: eventType, Visibility: visibility, SafePayload: safe,
				UsageDelta: usage, OccurredAt: occurredAt,
			}, after); err != nil {
				return after, err
			}
			if visibility == "app_safe" && w.Repos != nil && w.Repos.AgentRuns != nil {
				w.Repos.AgentRuns.NotifyPublicEvent(runID)
			}
			after = source.Sequence
		}
		if page.NextAfterSequence != 0 && page.NextAfterSequence != after {
			return after, fmt.Errorf("RUNTIME_EVENT_GAP")
		}
		if !page.HasMore {
			if page.LatestSequence > after {
				return after, fmt.Errorf("RUNTIME_EVENT_GAP")
			}
			return after, nil
		}
		if after == pageStart {
			return after, fmt.Errorf("RUNTIME_EVENT_GAP")
		}
	}
	return after, fmt.Errorf("RUNTIME_EVENT_PAGE_LIMIT")
}

// runtimeEventPayloadForStorage keeps generic lifecycle events on their
// existing compatibility path while making tool receipts a separate strict
// hash-only boundary. Tool usage is represented by the receipt's bytes and
// duration fields, so an auxiliary usage payload is not accepted there.
func runtimeEventPayloadForStorage(source runtimepkg.AsyncRuntimeEvent) (map[string]any, map[string]any, error) {
	if source.EventType == "assistant.delta" {
		deltaText, _ := source.Data["deltaText"].(string)
		if deltaText == "" {
			deltaText, _ = source.SafePayload["deltaText"].(string)
		}
		replace, _ := source.Data["replace"].(bool)
		if value, ok := source.SafePayload["replace"].(bool); ok {
			replace = value
		}
		return map[string]any{"deltaText": deltaText, "replace": replace, "status": "running"}, map[string]any{}, nil
	}
	if runtimepkg.IsRuntimeToolAuditEventType(source.EventType) {
		if len(source.UsageDelta) != 0 {
			return nil, nil, fmt.Errorf("RUNTIME_EVENT_GAP")
		}
		safe, err := runtimepkg.NormalizeRuntimeToolAuditEventPayload(source.EventType, source.Data, source.SafePayload)
		if err != nil {
			return nil, nil, err
		}
		return safe, map[string]any{}, nil
	}
	safe := map[string]any{}
	for key, value := range source.SafePayload {
		safe[key] = value
	}
	for key, value := range source.Data {
		safe[key] = value
	}
	safe["status"] = source.Status
	return safe, source.UsageDelta, nil
}

func runtimeEventStorageRoute(eventType string) (string, string) {
	if eventType == "assistant.delta" {
		return "draft_delta", "app_safe"
	}
	return eventType, "admin_safe"
}

func (w RuntimeEventWorker) retry(heartbeat *queueRepositoryHeartbeat, runID, code string) map[string]any {
	proof, heartbeatErr := heartbeat.Stop()
	if heartbeatErr != nil {
		return map[string]any{"queueId": proof.QueueID, "agentRunId": runID, "status": "aborted", "errorCode": "STALE_QUEUE_LEASE"}
	}
	if _, err := w.Repos.Queue.ScheduleRetry(context.Background(), proof, w.PollDelay, code); err != nil {
		return map[string]any{"queueId": proof.QueueID, "agentRunId": runID, "status": "aborted", "errorCode": "STALE_QUEUE_LEASE"}
	}
	return map[string]any{"queueId": proof.QueueID, "agentRunId": runID, "status": "polling", "errorCode": code, "retryable": true}
}

// completeTerminalDispatchQueue acknowledges an obsolete event-ingest job
// only after the queue-bound incomplete-convergence lookup has found nothing.
// A stale proof is never retried or treated as a successful acknowledgement.
func (w RuntimeEventWorker) completeTerminalDispatchQueue(ctx context.Context, heartbeat *queueRepositoryHeartbeat, queueID, runID, state string) map[string]any {
	proof, heartbeatErr := heartbeat.Stop()
	if heartbeatErr != nil {
		return map[string]any{"queueId": queueID, "agentRunId": runID, "status": "aborted", "errorCode": "STALE_QUEUE_LEASE"}
	}
	if _, err := w.Repos.Queue.Complete(ctx, proof); err != nil {
		return map[string]any{"queueId": queueID, "agentRunId": runID, "status": "aborted", "errorCode": "STALE_QUEUE_LEASE"}
	}
	return map[string]any{"queueId": queueID, "agentRunId": runID, "status": state, "terminalDispatch": true}
}

// frozenDispatchPlan reads the exact persisted PlanVersion that was signed
// into the Runtime dispatch. Neither the mutable Run snapshot nor AiTask may
// substitute another parser/writeback identity at terminal time.
func (w RuntimeEventWorker) frozenDispatchPlan(ctx context.Context, run persistence.AgentRunRecord, planVersion int) (runtimepkg.AgentRunPlan, error) {
	if w.Repos == nil {
		return runtimepkg.AgentRunPlan{}, fmt.Errorf("AGENT_PLAN_INVALID")
	}
	return frozenDispatchPlanForRun(ctx, w.Repos.AgentRuns, run, planVersion)
}

// frozenDispatchPlanForRun returns the persisted plan that was bound to the
// dispatch. Status, abort, and recovery tickets all derive their plan hash
// from this same immutable snapshot.
func frozenDispatchPlanForRun(ctx context.Context, runs *persistence.AgentRunRepository, run persistence.AgentRunRecord, planVersion int) (runtimepkg.AgentRunPlan, error) {
	if runs == nil || strings.TrimSpace(run.AgentRunID) == "" || planVersion < 1 {
		return runtimepkg.AgentRunPlan{}, fmt.Errorf("AGENT_PLAN_INVALID")
	}
	snapshot, err := runs.GetPlan(ctx, run.AgentRunID, planVersion)
	if err != nil {
		return runtimepkg.AgentRunPlan{}, err
	}
	plan, err := runtimepkg.AgentRunPlanFromSnapshot(snapshot)
	if err != nil || plan.AgentRunID != run.AgentRunID || plan.PlanVersion != planVersion {
		return runtimepkg.AgentRunPlan{}, fmt.Errorf("AGENT_PLAN_INVALID")
	}
	return plan, nil
}

func runtimeTerminalPublicResult(run persistence.AgentRunRecord, plan runtimepkg.AgentRunPlan, result map[string]any) map[string]any {
	if plan.TaskType != huokeTopicTaskType {
		return result
	}
	parsed, err := runtimepkg.NewOutputParser().ParseAgentRunResultForPlan(result, plan, run.AgentRunID, run.TaskID)
	if err != nil {
		return map[string]any{}
	}
	public := map[string]any{}
	for _, key := range []string{"runId", "status", "threadId", "queue", "resolvedConfigSnapshotId", "session", "usage"} {
		if value, ok := result[key]; ok {
			public[key] = value
		}
	}
	public["finalAnswer"] = workerMapString(aiWorkerMap(parsed["data"]), "reply")
	return public
}

func runtimeTerminalStatus(status string) bool {
	return stringInWorker(status, []string{"succeeded", "failed", "timeout", "aborted"})
}

func runtimeTerminalDispatchState(state string) bool {
	return stringInWorker(state, []string{"succeeded", "failed", "timeout", "aborted", "rejected", "orphaned"})
}

// legacyTerminalRecoveryMayCompleteWithoutCapacity is only the first filter for
// historical tail-only recovery. It does not grant a bypass by itself; the
// Worker also proves every durable checkpoint plus terminal Run/Slot/Session
// facts in legacyTerminalTailOnlyRecoveryEligible, and the PostgreSQL
// finalizer locks and repeats those checks before it mutates anything.
func legacyTerminalRecoveryMayCompleteWithoutCapacity(recovery runtimepkg.TerminalConvergenceRecovery, dispatch runtimepkg.RuntimeDispatch, run persistence.AgentRunRecord) bool {
	if !recovery.UsageSettled || !dispatch.IsLegacyCapacityUnbound() ||
		recovery.CapacityReservationID != "" || recovery.CapacitySnapshotVersion != 0 || recovery.CapacityReservedVersion != 0 ||
		run.AgentRunID != recovery.RunID || !legacyTerminalAgentRunStatus(run.Status) {
		return false
	}
	return stringInWorker(dispatch.State, []string{"succeeded", "failed", "timeout", "aborted", "rejected", "orphaned"}) &&
		(recovery.TerminalStatus == "" || (dispatch.State == recovery.TerminalStatus && runtimeTerminalAgentRunStatus(recovery.TerminalStatus) == run.Status))
}

func legacyTerminalTailOnlyRecoveryEligible(recovery runtimepkg.TerminalConvergenceRecovery, dispatch runtimepkg.RuntimeDispatch, run persistence.AgentRunRecord, slot runtimepkg.RuntimeSlotReservation, slotFound, sessionTerminal bool) bool {
	if !legacyTerminalRecoveryMayCompleteWithoutCapacity(recovery, dispatch, run) || recovery.TerminalStatus == "" || dispatch.State != recovery.TerminalStatus || runtimeTerminalAgentRunStatus(recovery.TerminalStatus) != run.Status ||
		!recovery.EventsVerified || !recovery.ProductProjected || !recovery.UsageSettled || !recovery.AgentRunConverged || !recovery.DispatchFinalized || !recovery.SessionReleased ||
		!slotFound || slot.ReservationID != dispatch.ReservationID || slot.RunID != recovery.RunID || slot.DispatchID != recovery.DispatchID ||
		slot.RuntimeHostID != dispatch.RuntimeHostID || slot.FencingToken != dispatch.FencingToken || !stringInWorker(slot.State, []string{"released", "expired"}) {
		return false
	}
	return !recovery.SessionRequired || sessionTerminal
}

// legacyTerminalRecoveryHasUnsafeCapacityContract detects a historical row that
// cannot take tail-only recovery because its immutable snapshot claims capacity
// facts while the legacy dispatch has no binding. This is not a temporary
// capacity miss: the immutable contract itself is contradictory, so retrying it
// would only generate another recovery queue generation.
func legacyTerminalRecoveryHasUnsafeCapacityContract(recovery runtimepkg.TerminalConvergenceRecovery, dispatch runtimepkg.RuntimeDispatch) bool {
	return dispatch.IsLegacyCapacityUnbound() &&
		(recovery.CapacityReservationID != "" || recovery.CapacitySnapshotVersion != 0 || recovery.CapacityReservedVersion != 0)
}

func (w RuntimeEventWorker) blockLegacyTerminalRecoveryContract(heartbeat *queueRepositoryHeartbeat, recovery runtimepkg.TerminalConvergenceRecovery) map[string]any {
	if w.Repos == nil || w.Repos.Queue == nil || heartbeat == nil || recovery.ConvergenceID == "" {
		return map[string]any{"queueId": recovery.QueueID, "agentRunId": recovery.RunID, "status": "aborted", "errorCode": "STALE_QUEUE_LEASE"}
	}
	proof, err := heartbeat.Freeze()
	if err != nil {
		return map[string]any{"queueId": recovery.QueueID, "agentRunId": recovery.RunID, "status": "aborted", "errorCode": "STALE_QUEUE_LEASE"}
	}
	if _, err := w.Repos.Queue.BlockRuntimeTerminalRecoveryLegacyContract(context.Background(), proof, recovery.ConvergenceID); err != nil {
		return w.retry(heartbeat, recovery.RunID, planningErrorCode(err))
	}
	return map[string]any{
		"queueId": recovery.QueueID, "agentRunId": recovery.RunID, "status": "blocked",
		"errorCode": "RUNTIME_TERMINAL_LEGACY_CONTRACT_BLOCKED", "retryable": false,
	}
}

func legacyTerminalAgentRunStatus(status string) bool {
	return stringInWorker(status, []string{"succeeded", "failed", "timeout", "cancelled", "orphaned"})
}

func runtimeTerminalAgentRunStatus(status string) string {
	if status == "aborted" {
		return "cancelled"
	}
	return status
}

// normalizeUserCancellationTimeout preserves an explicit Runtime cancellation
// terminal signal when a user cancellation is already durable. Abort transport
// acknowledgement is deliberately not a prerequisite: the Runtime can honor
// a cancellation while a racing abort request reports a transport failure.
// A timeout without both the durable intent and explicit signal stays a
// timeout.
func normalizeUserCancellationTimeout(run persistence.AgentRunRecord, status runtimepkg.AsyncRuntimeStatus) runtimepkg.AsyncRuntimeStatus {
	if status.Status != "timeout" || run.Status != "aborting" || run.CancelRequestedAt == nil ||
		run.CancelReasonCode != "USER_CANCELLED" {
		return status
	}
	code := strings.TrimSpace(workerMapString(status.Error, "code"))
	message := strings.TrimSpace(workerMapString(status.Error, "message"))
	if code != "USER_CANCELLED" && code != "RUNTIME_ABORTED" && !strings.EqualFold(message, "user_cancelled") {
		return status
	}
	status.Status = "aborted"
	status.Result = nil
	status.Error = map[string]any{"code": "RUNTIME_ABORTED"}
	return status
}

// runtimeSessionRequiredForExecutionScope deliberately derives session
// ownership from the frozen plan, not from a convenient ThreadID field. A
// detached task can retain a product-thread reference for audit/UI linkage but
// must not acquire or release a Product-session admission as a side effect.
func runtimeSessionRequiredForExecutionScope(scope string) (bool, error) {
	switch strings.TrimSpace(scope) {
	case string(runtimepkg.ScopeProductThread):
		return true, nil
	case string(runtimepkg.ScopeDetachedTask):
		return false, nil
	default:
		return false, fmt.Errorf("AGENT_PLAN_INVALID")
	}
}

func runtimeSchedulerSessions(scheduler *runtimepkg.RuntimeScheduler) *runtimepkg.RuntimeSessionAdmissionService {
	if scheduler == nil {
		return nil
	}
	return scheduler.Sessions
}

func runtimeSchedulerCapacity(scheduler *runtimepkg.RuntimeScheduler) *runtimepkg.RuntimeCapacityAdmissionService {
	if scheduler == nil {
		return nil
	}
	return scheduler.Capacity
}

func (w RuntimeEventWorker) terminalConverger() *runtimepkg.RuntimeTerminalConverger {
	if w.newConverger != nil {
		return w.newConverger()
	}
	return runtimepkg.NewRuntimeTerminalConverger(w.Repos.DB, w.Hosts, runtimeSchedulerSessions(w.Scheduler), runtimeSchedulerCapacity(w.Scheduler), w.Repos.Queue, w.Repos.AgentRuns)
}

// terminalQueueCompleter freezes the current heartbeat only after every
// durable terminal side effect has converged. The queue repository then checks
// that exact latest proof while atomically acknowledging the queue record and
// marking the terminal convergence complete.
func (w RuntimeEventWorker) terminalQueueCompleter(heartbeat *queueRepositoryHeartbeat) runtimepkg.TerminalQueueCompletionFunc {
	return func(_ context.Context, _ runtimepkg.TerminalConvergenceCommand, convergenceID string) error {
		if w.Repos == nil || w.Repos.Queue == nil || heartbeat == nil {
			return fmt.Errorf("QUEUE_REPOSITORY_UNAVAILABLE")
		}
		proof, err := heartbeat.Freeze()
		if err != nil {
			return err
		}
		_, err = w.Repos.Queue.CompleteTerminalConvergence(context.Background(), proof, convergenceID)
		return err
	}
}

func (w RuntimeEventWorker) terminalQueueProof(heartbeat *queueRepositoryHeartbeat) runtimepkg.TerminalQueueProofFunc {
	return func(_ context.Context, _ runtimepkg.TerminalConvergenceCommand) (persistence.QueueLeaseProof, error) {
		if heartbeat == nil {
			return persistence.QueueLeaseProof{}, fmt.Errorf("QUEUE_REPOSITORY_UNAVAILABLE")
		}
		return heartbeat.Freeze()
	}
}

func runtimePlanTerminalStatus(runStatus string) string {
	if runStatus == "cancelled" {
		return "cancelled"
	}
	if runStatus == "succeeded" {
		return "succeeded"
	}
	return "failed"
}
