package workers

import (
	"context"
	"errors"
	"testing"
	"time"

	"huahuoai/backend/source/internal/domain"
	"huahuoai/backend/source/internal/persistence"
	"huahuoai/backend/source/internal/queue"
	runtimepkg "huahuoai/backend/source/internal/runtime"
	"huahuoai/backend/source/internal/services"
)

func TestRuntimeAbortWorkerConvergesCancelAndReleasesSlot(t *testing.T) {
	ctx := context.Background()
	repos := persistence.NewRepositories(nil)
	hosts := runtimepkg.NewRuntimeHostRepository(nil)
	runID := "agent_run_abort_1"
	hostID := "runtime_host_abort_1"
	reservationID := "reservation_abort_1"
	dispatchID := "dispatch_abort_1"
	secret := "runtime-abort-test-secret"
	auth := domain.AuthContext{TenantID: "tenant_abort", UserID: "user_abort", WorkspaceID: "workspace_abort"}

	_, _, err := repos.AgentRuns.CreateRun(ctx, persistence.CreateAgentRunCommand{Record: persistence.AgentRunRecord{
		AgentRunID: runID, TenantID: auth.TenantID, UserID: auth.UserID, WorkspaceID: auth.WorkspaceID,
		IdempotencyKey: "abort-create", RequestHash: "abort-request", Status: "planning",
		WorkspaceVersion: 1, BindingVersion: 1, ContextGeneration: 1,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := repos.AgentRuns.SaveWorkspaceContext(ctx, domain.RunWorkspaceContextRecord{
		RunID: runID, AgentRunID: runID, TenantID: auth.TenantID, UserID: auth.UserID,
		WorkspaceID: auth.WorkspaceID, WorkspaceVersion: 1, ContextGeneration: 1,
		L1AgentProfile: "work_ai_agent", ManifestVersion: "manifest-v1", CapabilityHash: "capability-v1",
		ManifestHash: "workspace-context-hash", Status: "frozen",
	}); err != nil {
		t.Fatal(err)
	}
	if err := repos.AgentRuns.SavePlan(ctx, persistence.AgentRunPlanRecord{AgentRunID: runID, PlanVersion: 1, PlanStatus: "executing", AgentRunStatus: "running", Plan: valueMap(runtimeTicketTestPlan(runID, "capability-v1", "detached_task"))}); err != nil {
		t.Fatal(err)
	}
	if _, err := hosts.RegisterHost(ctx, runtimepkg.RuntimeHostIdentity{RuntimeHostID: hostID, InstanceID: "instance-abort", Environment: "test"}, runtimepkg.RuntimeHostRegistration{
		Endpoint: "http://runtime-host.test", RuntimeVersion: "2026.6.2", AdapterVersion: "v0.5",
		Capabilities: runtimepkg.RuntimeCapabilitySnapshot{CapabilityHash: "capability-v1"}, MaxActiveRuns: 8,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := hosts.HeartbeatHost(ctx, runtimepkg.RuntimeHostIdentity{RuntimeHostID: hostID, InstanceID: "instance-abort", Environment: "test"}, runtimepkg.RuntimeHostHeartbeat{
		Sequence: 1, ObservedAt: time.Now().UTC(), CapabilityHash: "capability-v1", SignatureKeyID: "service-token",
	}); err != nil {
		t.Fatal(err)
	}
	scheduler := runtimepkg.NewRuntimeScheduler(hosts, queue.NewMemoryDistributedLockManager())
	dimension := func(key string) runtimepkg.RuntimeCapacityDimension {
		return runtimepkg.RuntimeCapacityDimension{Key: key, Limit: 10, Requested: 1, Version: 1}
	}
	capacityReservation, err := scheduler.Capacity.Reserve(ctx, runtimepkg.RuntimeCapacityCommand{RunID: runID, SnapshotVersion: 1, TTL: 10 * time.Minute, Dimensions: runtimepkg.RuntimeCapacityDimensions{Model: dimension("model"), AuthPool: dimension("auth"), Tool: dimension("tool"), Tenant: dimension("tenant"), User: dimension("user")}})
	if err != nil {
		t.Fatal(err)
	}
	if err := scheduler.Capacity.CommitAccepted(ctx, capacityReservation); err != nil {
		t.Fatal(err)
	}
	_, _, err = hosts.TryReserveSlot(ctx, runtimepkg.AtomicReservationCommand{
		ReservationID: reservationID, RunID: runID, OwnerInstanceID: "worker-abort", ExecutionScope: "detached_task",
		FencingToken: 7, LeaseTokenHash: "sha256:abort", CapabilityHash: "capability-v1", ExpiresAt: time.Now().UTC().Add(10 * time.Minute), HeartbeatAfter: time.Now().Add(-time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := hosts.CreateDispatch(ctx, runtimepkg.RuntimeDispatch{
		DispatchID: dispatchID, RunID: runID, ReservationID: reservationID, CapacityReservationID: capacityReservation.ReservationID, CapacityReservedVersion: capacityReservation.Version, RuntimeHostID: hostID,
		DispatchAttempt: 1, PlanVersion: 1, FencingToken: 7, RunTicketJTIHash: "sha256:dispatch-abort",
		TicketExpiresAt: time.Now().UTC().Add(10 * time.Minute), InputManifestHash: runtimepkg.RunTicketJTIHash("input-manifest-hash"), OwnerInstanceID: "worker-abort", LeaseTokenHash: "sha256:abort", LeaseExpiresAt: time.Now().Add(10 * time.Minute),
	}); err != nil {
		t.Fatal(err)
	}
	if err := hosts.ConfirmDispatchAccepted(ctx, runtimepkg.DispatchAcceptedCommand{Fence: runtimepkg.ReservationFence{ReservationID: reservationID, RuntimeHostID: hostID, OwnerInstanceID: "worker-abort", LeaseTokenHash: "sha256:abort", FencingToken: 7}, DispatchID: dispatchID, RuntimeRequestID: "request-abort"}); err != nil {
		t.Fatal(err)
	}

	service := services.NewAgentRunService(repos, nil)
	record, err := service.Cancel(ctx, auth, runID, "USER_CANCELLED", "abort-key")
	if err != nil || record.Status != "aborting" {
		t.Fatalf("cancel record=%#v err=%v", record, err)
	}
	abortJob, abortProof, err := repos.Queue.Lease(ctx, queue.QueueRuntimeAbort, "abort-worker", time.Minute)
	if err != nil {
		t.Fatal("runtime_abort job was not enqueued")
	}
	client := &fakeAsyncRuntimeClient{abortResult: runtimepkg.AsyncRuntimeAbortResult{RunID: runID, Status: "aborting"}}
	result := NewRuntimeAbortWorker(repos, hosts, client, secret).Process(ctx, abortJob, abortProof)
	if result["status"] != "aborting" || client.aborted.RunID != runID || client.aborted.ReservationID != reservationID || client.aborted.FencingToken != 7 || client.aborted.RunTicket == "" {
		t.Fatalf("abort result=%#v request=%#v", result, client.aborted)
	}
	dispatch, err := hosts.GetDispatch(ctx, dispatchID)
	if err != nil || dispatch.AbortStatus != "accepted" || dispatch.AbortRequestedAt == nil {
		t.Fatalf("dispatch=%#v err=%v", dispatch, err)
	}

	eventJob, eventProof, err := repos.Queue.Lease(ctx, queue.QueueRuntimeEvents, "event-worker", time.Minute)
	if err != nil {
		t.Fatal("runtime_events job was not available after abort")
	}
	client.events = runtimepkg.AsyncRuntimeEventPage{Items: []runtimepkg.AsyncRuntimeEvent{{Sequence: 1, EventType: "run.aborting", Status: "aborting", Timestamp: time.Now()}, {Sequence: 2, EventType: "run.aborted", Status: "aborted", Timestamp: time.Now()}}, NextAfterSequence: 2}
	client.status = runtimepkg.AsyncRuntimeStatus{RunID: runID, Status: "aborted", LastEventSequence: 2}
	eventResult := NewRuntimeEventWorker(repos, hosts, scheduler, client, secret).Process(ctx, eventJob, eventProof)
	if eventResult["status"] != "cancelled" {
		t.Fatalf("event result=%#v", eventResult)
	}
	finalRun, err := repos.AgentRuns.GetRun(ctx, auth.TenantID, auth.UserID, runID)
	if err != nil || finalRun.Status != "cancelled" {
		t.Fatalf("final run=%#v err=%v", finalRun, err)
	}
	finalReservation, err := hosts.GetReservation(ctx, reservationID)
	if err != nil || finalReservation.State != "released" {
		t.Fatalf("reservation=%#v err=%v", finalReservation, err)
	}
	finalDispatch, err := hosts.GetDispatch(ctx, dispatchID)
	if err != nil || finalDispatch.State != "aborted" || finalDispatch.AbortStatus != "terminal" {
		t.Fatalf("final dispatch=%#v err=%v", finalDispatch, err)
	}
}

func TestRuntimeAbortWorkerRunRejectsMissingSessionAdmissionService(t *testing.T) {
	worker := NewRuntimeAbortWorker(
		persistence.NewRepositories(nil), runtimepkg.NewRuntimeHostRepository(nil), &fakeAsyncRuntimeClient{}, "runtime-abort-missing-session-secret",
	)
	if err := worker.Run(context.Background(), "abort-missing-session-worker"); err == nil || err.Error() != "RUNTIME_ABORT_WORKER_UNAVAILABLE" {
		t.Fatalf("missing Session Admission dependency started abort worker err=%v", err)
	}
}

func TestRuntimeAbortWorkerProcessRejectsMissingDependencies(t *testing.T) {
	result := RuntimeAbortWorker{}.Process(context.Background(), map[string]any{
		"queueId": "runtime_abort:run_missing_dependencies", "taskId": "run_missing_dependencies",
	}, persistence.QueueLeaseProof{})
	if result["status"] != "failed" || result["errorCode"] != "RUNTIME_ABORT_WORKER_UNAVAILABLE" {
		t.Fatalf("missing worker dependencies must fail closed without panic: %#v", result)
	}
}

func TestRuntimeAbortWorkerRequiresExactProductSessionAdmissionFence(t *testing.T) {
	ctx := context.Background()
	worker, client, record, proof := runtimeAbortProductSessionFixture(t, false)
	result := worker.Process(ctx, record, proof)
	if result["status"] != "retrying" || result["errorCode"] != "STALE_FENCING_TOKEN" {
		t.Fatalf("missing session admission must retry without abort: %#v", result)
	}
	if client.aborted.RunID != "" {
		t.Fatalf("Runtime abort bypassed missing session fence: %#v", client.aborted)
	}
}

func TestRuntimeAbortWorkerAcceptsExactProductSessionAdmissionFence(t *testing.T) {
	ctx := context.Background()
	worker, client, record, proof := runtimeAbortProductSessionFixture(t, true)
	result := worker.Process(ctx, record, proof)
	if result["status"] != "aborting" || client.aborted.RunID == "" || client.aborted.ReservationID == "" || client.aborted.FencingToken < 1 {
		t.Fatalf("exact session admission fence did not reach Runtime abort: result=%#v request=%#v", result, client.aborted)
	}
}

func TestRuntimeAbortWorkerRejectsMismatchedRuntimeAbortAcknowledgement(t *testing.T) {
	ctx := context.Background()
	worker, client, record, proof := runtimeAbortProductSessionFixture(t, true)
	client.abortResult = runtimepkg.AsyncRuntimeAbortResult{RunID: "another_run", Status: "aborting"}

	result := worker.Process(ctx, record, proof)
	if result["status"] != "retrying" || result["errorCode"] != "RUNTIME_ABORT_FAILED" {
		t.Fatalf("mismatched Runtime acknowledgement must retry: %#v", result)
	}
	if _, _, err := worker.Repos.Queue.Lease(ctx, queue.QueueRuntimeEvents, "mismatched-ack-event-worker", time.Minute); !errors.Is(err, persistence.ErrNoQueueWork) {
		t.Fatalf("mismatched Runtime acknowledgement queued event work: %v", err)
	}
	dispatch, err := worker.Hosts.GetDispatch(ctx, "dispatch_abort_product_session")
	if err != nil || dispatch.AbortStatus != "failed" {
		t.Fatalf("mismatched Runtime acknowledgement must persist failed dispatch status=%#v err=%v", dispatch, err)
	}
}

func TestRuntimeAbortWorkerDoesNotPublishTerminalStatusFromAcknowledgement(t *testing.T) {
	ctx := context.Background()
	worker, client, record, proof := runtimeAbortProductSessionFixture(t, true)
	client.abortResult = runtimepkg.AsyncRuntimeAbortResult{RunID: "agent_run_abort_product_session", Status: "aborted"}

	result := worker.Process(ctx, record, proof)
	if result["status"] != "aborting" {
		t.Fatalf("abort acknowledgement result=%#v", result)
	}
	events, err := worker.Repos.AgentRuns.ListPublicEvents(ctx, "agent_run_abort_product_session", 0, 10)
	if err != nil || len(events.Items) != 1 || events.Items[0].EventType != "aborting" || events.Items[0].Status != "aborting" || events.TerminalSequence != nil {
		t.Fatalf("acknowledgement must not publish terminal event page=%#v err=%v", events, err)
	}
}

func TestRuntimeAbortEventQueueEnqueuedRequiresExactDurableDispatchRecord(t *testing.T) {
	dispatch := runtimepkg.RuntimeDispatch{DispatchID: "dispatch_abort_event_queue", RuntimeHostID: "host_abort_event_queue"}
	exact := func(status string) map[string]any {
		return map[string]any{
			"queueId": queue.QueueRuntimeEvents + ":dispatch_abort_event_queue", "queueName": queue.QueueRuntimeEvents,
			"taskType": "runtime_event_ingest", "taskId": "agent_run_abort_event_queue",
			"dedupeKey": "runtime_event_ingest:dispatch_abort_event_queue", "status": status,
			"payload": map[string]any{"runId": "agent_run_abort_event_queue", "dispatchId": "dispatch_abort_event_queue", "runtimeHostId": "host_abort_event_queue"},
		}
	}
	for name, fixture := range map[string]struct {
		record map[string]any
		want   bool
	}{
		"accepted_pending_record": {
			record: exact("pending"),
			want:   true,
		},
		"accepted_existing_running_record": {
			record: exact("running"),
			want:   true,
		},
		"accepted_existing_retry_record": {
			record: exact("retry_wait"),
			want:   true,
		},
		"durable_backend_failure": {
			record: func() map[string]any {
				record := exact("failed")
				record["errorCode"] = "QUEUE_DURABLE_BACKEND_UNAVAILABLE"
				return record
			}(),
			want: false,
		},
		"wrong_dispatch_record": {
			record: func() map[string]any {
				record := exact("pending")
				record["queueId"] = queue.QueueRuntimeEvents + ":another_dispatch"
				return record
			}(),
			want: false,
		},
		"terminal_event_record": {
			record: exact("dead_letter"),
			want:   false,
		},
		"incomplete_record_shape": {
			record: map[string]any{"queueId": queue.QueueRuntimeEvents + ":dispatch_abort_event_queue"},
			want:   false,
		},
		"wrong_task_identity": {
			record: func() map[string]any { record := exact("pending"); record["taskId"] = "another_run"; return record }(),
			want:   false,
		},
		"wrong_payload_host": {
			record: func() map[string]any {
				record := exact("pending")
				record["payload"] = map[string]any{"runId": "agent_run_abort_event_queue", "dispatchId": "dispatch_abort_event_queue", "runtimeHostId": "another_host"}
				return record
			}(),
			want: false,
		},
	} {
		t.Run(name, func(t *testing.T) {
			if got := runtimeAbortEventQueueEnqueued(fixture.record, dispatch, "agent_run_abort_event_queue"); got != fixture.want {
				t.Fatalf("runtime abort event queue accepted=%t want=%t record=%#v", got, fixture.want, fixture.record)
			}
		})
	}
}

func runtimeAbortProductSessionFixture(t *testing.T, bindSession bool) (RuntimeAbortWorker, *fakeAsyncRuntimeClient, map[string]any, persistence.QueueLeaseProof) {
	t.Helper()
	ctx := context.Background()
	repos := persistence.NewRepositories(nil)
	hosts := runtimepkg.NewRuntimeHostRepository(nil)
	locks := queue.NewMemoryDistributedLockManager()
	sessions := runtimepkg.NewRuntimeSessionAdmissionService(nil, locks)
	const (
		runID         = "agent_run_abort_product_session"
		hostID        = "runtime_host_abort_product_session"
		reservationID = "reservation_abort_product_session"
		dispatchID    = "dispatch_abort_product_session"
		capability    = "capability_abort_product_session"
	)
	auth := domain.AuthContext{TenantID: "tenant_abort_product_session", UserID: "user_abort_product_session", WorkspaceID: "workspace_abort_product_session"}
	if _, _, err := repos.AgentRuns.CreateRun(ctx, persistence.CreateAgentRunCommand{Record: persistence.AgentRunRecord{
		AgentRunID: runID, TenantID: auth.TenantID, UserID: auth.UserID, WorkspaceID: auth.WorkspaceID,
		ThreadID: "thread_abort_product_session", IdempotencyKey: "abort-product-session-create", RequestHash: "abort-product-session-request", Status: "planning",
		WorkspaceVersion: 1, BindingVersion: 1, ContextGeneration: 1,
	}}); err != nil {
		t.Fatalf("create aborting run: %v", err)
	}
	if err := repos.AgentRuns.SaveWorkspaceContext(ctx, domain.RunWorkspaceContextRecord{
		RunID: runID, AgentRunID: runID, TenantID: auth.TenantID, UserID: auth.UserID, WorkspaceID: auth.WorkspaceID,
		WorkspaceVersion: 1, ContextGeneration: 1, L1AgentProfile: "work_ai_agent", ManifestVersion: "manifest-product-session", ManifestHash: "workspace-context-product-session", CapabilityHash: capability, Status: "frozen",
	}); err != nil {
		t.Fatalf("save workspace context: %v", err)
	}
	if err := repos.AgentRuns.SavePlan(ctx, persistence.AgentRunPlanRecord{AgentRunID: runID, PlanVersion: 1, PlanStatus: "executing", AgentRunStatus: "aborting", Plan: valueMap(runtimeTicketTestPlan(runID, capability, "product_thread"))}); err != nil {
		t.Fatalf("save plan: %v", err)
	}
	identity := runtimepkg.RuntimeHostIdentity{RuntimeHostID: hostID, InstanceID: "instance-abort-product-session", Environment: "test"}
	if _, err := hosts.RegisterHost(ctx, identity, runtimepkg.RuntimeHostRegistration{
		Endpoint: "http://runtime-host-product-session.test", RuntimeVersion: "2026.6.2", AdapterVersion: "v0.5",
		Capabilities: runtimepkg.RuntimeCapabilitySnapshot{CapabilityHash: capability}, MaxActiveRuns: 8,
	}); err != nil {
		t.Fatalf("register Runtime host: %v", err)
	}
	if _, err := hosts.HeartbeatHost(ctx, identity, runtimepkg.RuntimeHostHeartbeat{
		Sequence: 1, ObservedAt: time.Now().UTC(), CapabilityHash: capability, SignatureKeyID: "service-token",
	}); err != nil {
		t.Fatalf("heartbeat Runtime host: %v", err)
	}
	reservation, _, err := hosts.TryReserveSlot(ctx, runtimepkg.AtomicReservationCommand{
		ReservationID: reservationID, RunID: runID, OwnerInstanceID: "worker-abort-product-session", ExecutionScope: "product_thread",
		FencingToken: 17, LeaseTokenHash: "sha256:abort-product-session", CapabilityHash: capability,
		ExpiresAt: time.Now().UTC().Add(time.Minute), HeartbeatAfter: time.Now().UTC().Add(-time.Second), AffinityRuntimeHostID: hostID,
	})
	if err != nil {
		t.Fatalf("reserve Runtime slot: %v", err)
	}
	if _, err := hosts.CreateDispatch(ctx, runtimepkg.RuntimeDispatch{
		DispatchID: dispatchID, RunID: runID, ReservationID: reservationID, RuntimeHostID: hostID,
		DispatchAttempt: 1, PlanVersion: 1, FencingToken: reservation.FencingToken, RunTicketJTIHash: "sha256:dispatch-product-session",
		TicketExpiresAt: time.Now().UTC().Add(time.Minute), InputManifestHash: runtimepkg.RunTicketJTIHash("input-manifest-product-session"),
		OwnerInstanceID: reservation.OwnerInstanceID, LeaseTokenHash: reservation.LeaseTokenHash, LeaseExpiresAt: reservation.ExpiresAt,
	}); err != nil {
		t.Fatalf("create dispatch: %v", err)
	}
	if err := hosts.ConfirmDispatchAccepted(ctx, runtimepkg.DispatchAcceptedCommand{Fence: runtimepkg.ReservationFence{
		ReservationID: reservationID, RuntimeHostID: hostID, OwnerInstanceID: reservation.OwnerInstanceID, LeaseTokenHash: reservation.LeaseTokenHash, FencingToken: reservation.FencingToken,
	}, DispatchID: dispatchID, RuntimeRequestID: "request-product-session"}); err != nil {
		t.Fatalf("accept dispatch: %v", err)
	}
	if bindSession {
		handle, admissionErr := sessions.Acquire(ctx, runtimepkg.ProductSessionAdmissionCommand{
			Key:       runtimepkg.ProductSessionAdmissionKey{TenantID: auth.TenantID, ThreadID: "thread_abort_product_session", AgentProfile: "work_ai_agent", ContextGeneration: 1, SessionGeneration: 1},
			BindingID: "binding_abort_product_session", RunID: runID, OwnerInstanceID: reservation.OwnerInstanceID, TTL: time.Minute,
		})
		if admissionErr != nil {
			t.Fatalf("acquire session admission: %v", admissionErr)
		}
		if admissionErr := sessions.BindReservation(ctx, handle, reservationID); admissionErr != nil {
			t.Fatalf("bind session reservation: %v", admissionErr)
		}
		if admissionErr := sessions.BindDispatch(ctx, handle, dispatchID); admissionErr != nil {
			t.Fatalf("bind session dispatch: %v", admissionErr)
		}
	}
	enqueued := repos.Queue.Enqueue(map[string]any{
		"queueId": queue.QueueRuntimeAbort + ":" + runID, "queueName": queue.QueueRuntimeAbort, "taskType": "runtime_abort", "taskId": runID,
		"dedupeKey": "runtime-abort-product-session:" + runID, "priority": 200, "maxAttempts": 8, "payload": map[string]any{"agentRunId": runID},
	})
	if workerMapString(enqueued, "status") != "pending" {
		t.Fatal("enqueue runtime abort")
	}
	record, proof, err := repos.Queue.Lease(ctx, queue.QueueRuntimeAbort, "abort-product-session-worker", time.Minute, "runtime_abort")
	if err != nil {
		t.Fatalf("lease runtime abort: %v", err)
	}
	client := &fakeAsyncRuntimeClient{abortResult: runtimepkg.AsyncRuntimeAbortResult{RunID: runID, Status: "aborting"}}
	worker := NewRuntimeAbortWorker(repos, hosts, client, "runtime-abort-product-session-secret")
	worker.Sessions = sessions
	return worker, client, record, proof
}

func runtimeTicketTestPlan(runID, capabilityHash, executionScope string) runtimepkg.AgentRunPlan {
	return runtimepkg.AgentRunPlan{
		SchemaVersion: "agent_run_plan.v1", AgentRunID: runID, PlanVersion: 1,
		RoutingMode: "dynamic", TaskType: "work_ai_general_chat", ExecutionScope: executionScope,
		L1AgentProfile: "work_ai_agent", RuntimeConfigID: "runtime-test", AgentHash: runtimepkg.RunTicketJTIHash("test-agent"),
		ManifestVersion: "manifest-test", RequiredTools: []string{"read"}, WriteMode: "none",
		ToolBudget: runtimepkg.RuntimeToolBudget{
			MaxToolCalls: 200, SoftToolCallLimit: 160, FinalizationReserve: 10,
			MaxRepeatedCalls: 2, MaxNoProgressCalls: 4, MaxSearchCalls: 60, MaxWriteCalls: 20,
			MaxReadBytes: 1024, MaxWallTimeSeconds: 60,
		},
		WorkspaceVersion: 1, CapabilityHash: capabilityHash,
	}
}
