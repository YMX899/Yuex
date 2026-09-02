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
)

type RuntimeAbortWorker struct {
	Repos        *persistence.Repositories
	Hosts        *runtimepkg.RuntimeHostRepository
	Client       runtimepkg.AsyncOpenClawClient
	TicketSecret string
	// Sessions supplies durable, hash-bound Product-session assertions. It is
	// required to abort product-thread Runs but is intentionally not used to
	// reconstruct a mutable plaintext lease handle.
	Sessions   *runtimepkg.RuntimeSessionAdmissionService
	LeaseTTL   time.Duration
	IdleSleep  time.Duration
	RetryDelay time.Duration
}

func NewRuntimeAbortWorker(repos *persistence.Repositories, hosts *runtimepkg.RuntimeHostRepository, client runtimepkg.AsyncOpenClawClient, ticketSecret string) RuntimeAbortWorker {
	return RuntimeAbortWorker{
		Repos: repos, Hosts: hosts, Client: client, TicketSecret: strings.TrimSpace(ticketSecret),
		LeaseTTL: 60 * time.Second, IdleSleep: 500 * time.Millisecond, RetryDelay: time.Second,
	}
}

func (w RuntimeAbortWorker) Run(ctx context.Context, workerID string) error {
	if w.Repos == nil || w.Repos.Queue == nil || w.Repos.AgentRuns == nil || w.Hosts == nil || w.Client == nil || w.TicketSecret == "" || w.Sessions == nil {
		return fmt.Errorf("RUNTIME_ABORT_WORKER_UNAVAILABLE")
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		record, proof, err := w.Repos.Queue.Lease(ctx, queue.QueueRuntimeAbort, workerID, w.LeaseTTL, "runtime_abort")
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

func (w RuntimeAbortWorker) Process(ctx context.Context, queueRecord map[string]any, proof persistence.QueueLeaseProof) map[string]any {
	// Process is also invoked by focused consumers/tests, not only Run. Keep a
	// partial or stale dependency injection from turning an abort queue record
	// into a worker panic or a false terminal result.
	if w.Repos == nil || w.Repos.Queue == nil || w.Repos.AgentRuns == nil || w.Hosts == nil || w.Client == nil || w.TicketSecret == "" {
		return map[string]any{"status": "failed", "errorCode": "RUNTIME_ABORT_WORKER_UNAVAILABLE"}
	}
	queueID := workerMapString(queueRecord, "queueId")
	payload := aiWorkerMap(queueRecord["payload"])
	runID := firstWorkerString(workerMapString(payload, "agentRunId"), workerMapString(queueRecord, "taskId"))
	if queueID == "" || runID == "" {
		return map[string]any{"status": "failed", "errorCode": "RUNTIME_INPUT_INVALID"}
	}
	if _, err := w.Repos.Queue.MarkRunning(ctx, proof); err != nil {
		return map[string]any{"queueId": queueID, "agentRunId": runID, "status": "aborted", "errorCode": "STALE_QUEUE_LEASE"}
	}
	leaseCtx, heartbeat := startQueueRepositoryHeartbeat(ctx, w.Repos.Queue, proof, w.LeaseTTL)
	run, err := w.Repos.AgentRuns.GetRunInternal(leaseCtx, runID)
	if err != nil {
		return w.fail(leaseCtx, heartbeat, queueRecord, queueID, runID, "NOT_FOUND")
	}
	if stringInWorker(run.Status, []string{"succeeded", "failed", "cancelled", "timeout"}) {
		terminalProof, heartbeatErr := heartbeat.Stop()
		if heartbeatErr != nil {
			return map[string]any{"queueId": queueID, "agentRunId": runID, "status": "aborted", "errorCode": "STALE_QUEUE_LEASE"}
		}
		if _, err := w.Repos.Queue.Complete(context.Background(), terminalProof); err != nil {
			return map[string]any{"queueId": queueID, "agentRunId": runID, "status": "aborted", "errorCode": "STALE_QUEUE_LEASE"}
		}
		return map[string]any{"queueId": queueID, "agentRunId": runID, "status": run.Status, "replayed": true}
	}
	if run.Status != "aborting" && run.Status != "orphaned" {
		return w.fail(leaseCtx, heartbeat, queueRecord, queueID, runID, "AGENT_PLAN_EXPIRED")
	}
	dispatch, err := w.Hosts.GetActiveDispatchByRunID(leaseCtx, runID)
	if err != nil {
		return w.fail(leaseCtx, heartbeat, queueRecord, queueID, runID, "RUNTIME_RUN_STALLED")
	}
	reservation, err := w.Hosts.GetReservation(leaseCtx, dispatch.ReservationID)
	if err != nil || !runtimeAbortReservationFenceMatches(runID, dispatch, reservation) {
		return w.fail(leaseCtx, heartbeat, queueRecord, queueID, runID, "STALE_FENCING_TOKEN")
	}
	if reservation.ExecutionScope == "product_thread" {
		if w.Sessions == nil {
			return w.fail(leaseCtx, heartbeat, queueRecord, queueID, runID, "RUNTIME_SESSION_ADMISSION_UNAVAILABLE")
		}
		if err := w.Sessions.AssertActiveDispatchFence(leaseCtx, runID, reservation.ReservationID, dispatch.DispatchID); err != nil {
			return w.fail(leaseCtx, heartbeat, queueRecord, queueID, runID, planningErrorCode(err))
		}
	} else if reservation.ExecutionScope != "detached_task" {
		return w.fail(leaseCtx, heartbeat, queueRecord, queueID, runID, "STALE_FENCING_TOKEN")
	}
	host, err := w.Hosts.GetHost(leaseCtx, dispatch.RuntimeHostID)
	if err != nil {
		return w.fail(leaseCtx, heartbeat, queueRecord, queueID, runID, "RUNTIME_CAPACITY_UNAVAILABLE")
	}
	frozen, err := w.Repos.AgentRuns.GetWorkspaceContextByRunID(leaseCtx, runID)
	if err != nil || frozen.CapabilityHash == "" || frozen.WorkspaceID != run.WorkspaceID {
		return w.fail(leaseCtx, heartbeat, queueRecord, queueID, runID, "AGENT_PLAN_EXPIRED")
	}
	plan, err := frozenDispatchPlanForRun(leaseCtx, w.Repos.AgentRuns, run, dispatch.PlanVersion)
	if err != nil || frozen.TenantID == "" || frozen.TenantID != run.TenantID || plan.CapabilityHash != frozen.CapabilityHash {
		return w.fail(leaseCtx, heartbeat, queueRecord, queueID, runID, "AGENT_PLAN_INVALID")
	}
	planHash, err := runtimepkg.ComputeAgentRunPlanHash(plan)
	if err != nil {
		return w.fail(leaseCtx, heartbeat, queueRecord, queueID, runID, "AGENT_PLAN_INVALID")
	}
	if err := w.Hosts.MarkDispatchAbortStatus(leaseCtx, dispatch.DispatchID, host.RuntimeHostID, dispatch.FencingToken, "requested"); err != nil {
		return w.fail(leaseCtx, heartbeat, queueRecord, queueID, runID, planningErrorCode(err))
	}
	now := time.Now().UTC()
	ticket, err := runtimepkg.SignRunTicket(runtimepkg.RunTicketClaims{
		RunID: runID, TenantID: frozen.TenantID, ReservationID: dispatch.ReservationID, RuntimeHostID: dispatch.RuntimeHostID,
		CapabilityHash: frozen.CapabilityHash, WorkspaceID: frozen.WorkspaceID, WorkspaceVersion: frozen.WorkspaceVersion,
		ContextGeneration: frozen.ContextGeneration, InputManifestHash: dispatch.InputManifestHash, PlanHash: planHash,
		FencingToken: dispatch.FencingToken, JTI: fmt.Sprintf("abort-%s-%d", dispatch.DispatchID, now.UnixNano()),
		IssuedAt: now.Unix(), ExpiresAt: now.Add(5 * time.Minute).Unix(),
	}, w.TicketSecret)
	if err != nil {
		return w.fail(leaseCtx, heartbeat, queueRecord, queueID, runID, "RUNTIME_PERMISSION_DENIED")
	}
	result, err := w.Client.AbortAsync(leaseCtx, host, runtimepkg.AsyncRuntimeAbortRequest{
		RunID: runID, ReservationID: dispatch.ReservationID, FencingToken: dispatch.FencingToken,
		Reason: "user_cancelled", RunTicket: ticket,
	})
	// A transport implementation is an untrusted Runtime boundary. Do not
	// treat a response for another Run, or an unsupported acknowledgement,
	// as evidence that this fenced dispatch has been aborted.
	if err != nil || result.RunID != runID || !stringInWorker(result.Status, []string{"aborting", "aborted"}) {
		_ = w.Hosts.MarkDispatchAbortStatus(leaseCtx, dispatch.DispatchID, host.RuntimeHostID, dispatch.FencingToken, "failed")
		return w.fail(leaseCtx, heartbeat, queueRecord, queueID, runID, "RUNTIME_ABORT_FAILED")
	}
	if err := w.Hosts.MarkDispatchAbortStatus(leaseCtx, dispatch.DispatchID, host.RuntimeHostID, dispatch.FencingToken, "accepted"); err != nil {
		return w.fail(leaseCtx, heartbeat, queueRecord, queueID, runID, planningErrorCode(err))
	}
	_ = w.Repos.AgentRuns.AppendPublicEventIdempotent(leaseCtx, persistence.AgentRunEvent{
		AgentRunID: runID, EventType: "aborting", Status: "aborting",
		// Runtime acknowledgement, including an immediate `aborted` response,
		// is not the Product terminal projection. RuntimeEventWorker owns the
		// terminal event, AgentRun, Slot, session and usage convergence.
		SafeData: map[string]any{"status": "aborting"}, CreatedAt: now,
	}, "runtime-abort-accepted:"+dispatch.DispatchID)
	eventQueue := w.Repos.Queue.Enqueue(map[string]any{
		"queueId": queue.QueueRuntimeEvents + ":" + dispatch.DispatchID, "queueName": queue.QueueRuntimeEvents,
		"taskType": "runtime_event_ingest", "taskId": runID, "dedupeKey": "runtime_event_ingest:" + dispatch.DispatchID,
		"priority": 200, "maxAttempts": 7200,
		"payload": map[string]any{"runId": runID, "dispatchId": dispatch.DispatchID, "runtimeHostId": host.RuntimeHostID},
	})
	if !runtimeAbortEventQueueEnqueued(eventQueue, dispatch, runID) {
		// Gateway acknowledgement is not a terminal result. Keep the abort job
		// retryable until the durable event consumer has a record from which it
		// can observe the terminal abort and converge the Slot/session state.
		return w.fail(leaseCtx, heartbeat, queueRecord, queueID, runID, "RUNTIME_ABORT_FAILED")
	}
	terminalProof, heartbeatErr := heartbeat.Stop()
	if heartbeatErr != nil {
		return map[string]any{"queueId": queueID, "agentRunId": runID, "status": "aborted", "errorCode": "STALE_QUEUE_LEASE"}
	}
	if _, err := w.Repos.Queue.Complete(context.Background(), terminalProof); err != nil {
		return map[string]any{"queueId": queueID, "agentRunId": runID, "status": "aborted", "errorCode": "STALE_QUEUE_LEASE"}
	}
	return map[string]any{"queueId": queueID, "agentRunId": runID, "dispatchId": dispatch.DispatchID, "status": "aborting"}
}

func runtimeAbortReservationFenceMatches(runID string, dispatch runtimepkg.RuntimeDispatch, reservation runtimepkg.RuntimeSlotReservation) bool {
	return dispatch.DispatchID != "" && dispatch.RunID == runID && dispatch.ReservationID == reservation.ReservationID &&
		dispatch.RuntimeHostID == reservation.RuntimeHostID && dispatch.FencingToken == reservation.FencingToken &&
		reservation.RunID == runID && stringInWorker(reservation.State, []string{"reserved", "accepted", "running"})
}

func (w RuntimeAbortWorker) fail(ctx context.Context, heartbeat *queueRepositoryHeartbeat, queueRecord map[string]any, queueID, runID, code string) map[string]any {
	attempt, maxAttempts := aiWorkerInt(queueRecord["attempt"]), aiWorkerInt(queueRecord["maxAttempts"])
	if maxAttempts < 1 {
		maxAttempts = 8
	}
	proof, heartbeatErr := heartbeat.Stop()
	if heartbeatErr != nil {
		return map[string]any{"queueId": queueID, "agentRunId": runID, "status": "aborted", "errorCode": "STALE_QUEUE_LEASE"}
	}
	if attempt >= maxAttempts {
		if _, err := w.Repos.Queue.Fail(context.Background(), proof, code, false); err != nil {
			return map[string]any{"queueId": queueID, "agentRunId": runID, "status": "aborted", "errorCode": "STALE_QUEUE_LEASE"}
		}
		_ = w.Repos.AgentRuns.UpdateStatusVersioned(ctx, runID, []string{"aborting", "orphaned"}, "orphaned", map[string]any{"errorSummary": map[string]any{"code": "RUNTIME_ABORT_FAILED"}})
		_ = w.Repos.AgentRuns.AppendPublicEventIdempotent(ctx, persistence.AgentRunEvent{
			AgentRunID: runID, EventType: "orphaned", Status: "orphaned",
			SafeData: map[string]any{"status": "orphaned", "code": "RUNTIME_ABORT_FAILED"},
		}, "runtime-abort-orphaned:"+queueID)
		return map[string]any{"queueId": queueID, "agentRunId": runID, "status": "orphaned", "errorCode": code}
	}
	if _, err := w.Repos.Queue.ScheduleRetry(context.Background(), proof, w.RetryDelay, code); err != nil {
		return map[string]any{"queueId": queueID, "agentRunId": runID, "status": "aborted", "errorCode": "STALE_QUEUE_LEASE"}
	}
	return map[string]any{"queueId": queueID, "agentRunId": runID, "status": "retrying", "errorCode": code, "retryable": true}
}

// runtimeAbortEventQueueEnqueued accepts a durable new or already-pending
// runtime-event record for this exact dispatch. A generic success-shaped
// enqueue result is not enough: without the canonical record EventWorker has
// no authority to observe abort terminal state and converge capacity/session
// admission.
func runtimeAbortEventQueueEnqueued(record map[string]any, dispatch runtimepkg.RuntimeDispatch, runID string) bool {
	if record == nil || dispatch.DispatchID == "" || runID == "" ||
		workerMapString(record, "queueId") != queue.QueueRuntimeEvents+":"+dispatch.DispatchID ||
		workerMapString(record, "queueName") != queue.QueueRuntimeEvents ||
		workerMapString(record, "taskType") != "runtime_event_ingest" ||
		workerMapString(record, "taskId") != runID ||
		workerMapString(record, "dedupeKey") != "runtime_event_ingest:"+dispatch.DispatchID ||
		workerMapString(record, "errorCode") == "QUEUE_DURABLE_BACKEND_UNAVAILABLE" {
		return false
	}
	payload := aiWorkerMap(record["payload"])
	return stringInWorker(workerMapString(record, "status"), []string{"pending", "retry_wait", "running"}) &&
		workerMapString(payload, "runId") == runID &&
		workerMapString(payload, "dispatchId") == dispatch.DispatchID &&
		workerMapString(payload, "runtimeHostId") == dispatch.RuntimeHostID
}
