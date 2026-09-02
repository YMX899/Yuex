package workers

import (
	"context"
	"errors"
	"testing"
	"time"

	"huahuoai/backend/source/internal/persistence"
	"huahuoai/backend/source/internal/queue"
	runtimepkg "huahuoai/backend/source/internal/runtime"
	"huahuoai/backend/source/internal/services"

	"github.com/jackc/pgx/v5"
)

func TestRuntimeRecoveryWorkerOrphansOfflineRunAndRotatesSession(t *testing.T) {
	ctx := context.Background()
	base := time.Date(2026, 7, 12, 10, 0, 0, 0, time.UTC)
	repos := persistence.NewRepositories(nil)
	hosts := runtimepkg.NewRuntimeHostRepository(nil)
	registerRecoveryHost(t, hosts, base, "host_offline")
	binding, err := hosts.EnsureProductSessionBinding(ctx, runtimepkg.ProductSessionBindingCommand{
		ThreadID: "thread_recovery", TenantID: "tenant_1", UserID: "user_1", WorkspaceID: "workspace_1",
		AgentProfile: "work_ai_agent", ContextGeneration: 1, ManifestVersion: "manifest-v1",
		AgentHash: "sha256:agent", SessionKeyEncryptionSecret: "runtime-session-secret-test",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := hosts.BindProductSessionHost(ctx, runtimepkg.ProductSessionHostBinding{
		TenantID: binding.TenantID, ThreadID: binding.ThreadID, AgentProfile: binding.AgentProfile, ContextGeneration: binding.ContextGeneration,
		SessionGeneration: binding.SessionGeneration, RuntimeHostID: "host_offline", SessionStoreID: "store-host-offline",
	}); err != nil {
		t.Fatal(err)
	}
	createRecoveryRun(t, repos, "run_offline", "thread_recovery")
	capacity := createRecoveryDispatch(t, hosts, base, "run_offline", "host_offline", "reservation_offline", "dispatch_offline")

	worker := NewRuntimeRecoveryWorker(repos, hosts, queue.NewMemoryDistributedLockManager(), "runtime-session-secret-test", "recovery-test")
	worker.Scheduler.Capacity = capacity
	sessionAdmission, err := worker.Scheduler.Sessions.Acquire(ctx, runtimepkg.ProductSessionAdmissionCommand{
		Key: runtimepkg.ProductSessionAdmissionKey{
			TenantID: "tenant_1", ThreadID: "thread_recovery", AgentProfile: "work_ai_agent", ContextGeneration: 1, SessionGeneration: 1,
		},
		BindingID: "binding_offline", RunID: "run_offline", OwnerInstanceID: "worker_1", TTL: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	worker.Now = func() time.Time { return base.Add(2 * time.Minute) }
	result, err := worker.Reconcile(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result["orphanedRuns"] != 1 || result["rotatedSessions"] != 1 {
		t.Fatalf("reconcile result=%#v", result)
	}
	run, err := repos.AgentRuns.GetRunInternal(ctx, "run_offline")
	if err != nil || run.Status != "orphaned" {
		t.Fatalf("run=%+v err=%v", run, err)
	}
	dispatch, err := hosts.GetDispatch(ctx, "dispatch_offline")
	if err != nil || dispatch.State != "orphaned" {
		t.Fatalf("dispatch=%+v err=%v", dispatch, err)
	}
	reservation, err := hosts.GetReservation(ctx, "reservation_offline")
	if err != nil || reservation.State != "released" {
		t.Fatalf("reservation=%+v err=%v", reservation, err)
	}
	latestCapacity, err := worker.Scheduler.Capacity.GetLatestByRunID(ctx, "run_offline")
	if err != nil || latestCapacity.State != "released" {
		t.Fatalf("capacity=%+v err=%v", latestCapacity, err)
	}
	if released, err := worker.Scheduler.Sessions.ReleasedOrExpiredByRunID(ctx, "run_offline"); err != nil || !released {
		t.Fatalf("session released=%v err=%v", released, err)
	}
	dimension := func(key string) runtimepkg.RuntimeCapacityDimension {
		return runtimepkg.RuntimeCapacityDimension{Key: key, Limit: 1, Requested: 1, Version: 1}
	}
	if replacement, err := worker.Scheduler.Capacity.Reserve(ctx, runtimepkg.RuntimeCapacityCommand{
		RunID: "run_offline_replacement", SnapshotVersion: 1, TTL: time.Hour,
		Dimensions: runtimepkg.RuntimeCapacityDimensions{
			Model: dimension("model-offline"), AuthPool: dimension("auth-offline"), Tool: dimension("tool-offline"),
			Tenant: dimension("tenant-offline"), User: dimension("user-offline"),
		},
	}); err != nil {
		t.Fatalf("released capacity remained unavailable: %v", err)
	} else if _, err := worker.Scheduler.Capacity.Release(ctx, replacement, nil); err != nil {
		t.Fatal(err)
	}
	if replacement, err := worker.Scheduler.Sessions.Acquire(ctx, runtimepkg.ProductSessionAdmissionCommand{
		Key: runtimepkg.ProductSessionAdmissionKey{
			TenantID: "tenant_1", ThreadID: "thread_recovery", AgentProfile: "work_ai_agent", ContextGeneration: 1, SessionGeneration: 1,
		},
		BindingID: "binding_offline_replacement", RunID: "run_offline_replacement", OwnerInstanceID: "worker_2", TTL: time.Hour,
	}); err != nil {
		t.Fatalf("released session admission remained unavailable: %v", err)
	} else {
		if replacement.Admission.FencingToken <= sessionAdmission.Admission.FencingToken {
			t.Fatalf("session fencing did not advance: first=%d replacement=%d", sessionAdmission.Admission.FencingToken, replacement.Admission.FencingToken)
		}
		if _, err := worker.Scheduler.Sessions.Release(ctx, replacement, "succeeded"); err != nil {
			t.Fatal(err)
		}
	}
	next, err := hosts.EnsureProductSessionBinding(ctx, runtimepkg.ProductSessionBindingCommand{
		ThreadID: "thread_recovery", TenantID: "tenant_1", UserID: "user_1", WorkspaceID: "workspace_1",
		AgentProfile: "work_ai_agent", ContextGeneration: 1, ManifestVersion: "manifest-v1",
		AgentHash: "sha256:agent", SessionKeyEncryptionSecret: "runtime-session-secret-test",
	})
	if err != nil || next.SessionGeneration != 2 || next.RuntimeHostID != "" || next.RecoveredFromGeneration != 1 {
		t.Fatalf("next binding=%+v err=%v", next, err)
	}
	replayed, err := worker.Reconcile(ctx)
	if err != nil || replayed["orphanedRuns"] != 0 || replayed["rotatedSessions"] != 0 {
		t.Fatalf("idempotent reconcile=%#v err=%v", replayed, err)
	}
}

func TestRuntimeRecoveryWorkerRecoversAdmissionsAfterPriorOrphanFinalization(t *testing.T) {
	ctx := context.Background()
	base := time.Now().UTC()
	repos := persistence.NewRepositories(nil)
	hosts := runtimepkg.NewRuntimeHostRepository(nil)
	registerRecoveryHost(t, hosts, base, "host_orphan_admission_recovery")
	createRecoveryRun(t, repos, "run_orphan_admission_recovery", "thread_orphan_admission_recovery")
	capacity := createRecoveryDispatch(t, hosts, base, "run_orphan_admission_recovery", "host_orphan_admission_recovery", "reservation_orphan_admission_recovery", "dispatch_orphan_admission_recovery", "product_thread")

	worker := NewRuntimeRecoveryWorker(repos, hosts, queue.NewMemoryDistributedLockManager(), "runtime-session-secret-test", "recovery-test")
	worker.Scheduler.Capacity = capacity
	if _, err := worker.Scheduler.Sessions.Acquire(ctx, runtimepkg.ProductSessionAdmissionCommand{
		Key: runtimepkg.ProductSessionAdmissionKey{
			TenantID: "tenant_1", ThreadID: "thread_orphan_admission_recovery", AgentProfile: "work_ai_agent", ContextGeneration: 1, SessionGeneration: 1,
		},
		BindingID: "binding_orphan_admission_recovery", RunID: "run_orphan_admission_recovery", OwnerInstanceID: "worker_1", TTL: time.Hour,
	}); err != nil {
		t.Fatal(err)
	}
	reservation, err := hosts.GetReservation(ctx, "reservation_orphan_admission_recovery")
	if err != nil {
		t.Fatal(err)
	}
	if err := hosts.FinalizeDispatchAndReleaseSlot(ctx, runtimepkg.DispatchTerminalCommand{
		Fence: runtimepkg.ReservationFence{
			ReservationID: reservation.ReservationID, RuntimeHostID: reservation.RuntimeHostID, OwnerInstanceID: reservation.OwnerInstanceID,
			LeaseTokenHash: reservation.LeaseTokenHash, FencingToken: reservation.FencingToken,
		},
		DispatchID: "dispatch_orphan_admission_recovery", TerminalStatus: "orphaned", ErrorCode: "RUNTIME_HOST_OFFLINE",
	}); err != nil {
		t.Fatal(err)
	}
	if err := repos.AgentRuns.UpdateStatusVersioned(ctx, "run_orphan_admission_recovery", []string{"running"}, "orphaned", map[string]any{"errorSummary": map[string]any{"code": "RUNTIME_HOST_OFFLINE"}}); err != nil {
		t.Fatal(err)
	}
	worker.Now = func() time.Time { return base.Add(2 * time.Minute) }
	result, err := worker.Reconcile(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result["orphanedRuns"] != 0 || result["orphanedAdmissionRecoveries"] != 1 {
		t.Fatalf("reconcile result=%#v", result)
	}
	latestCapacity, err := worker.Scheduler.Capacity.GetLatestByRunID(ctx, "run_orphan_admission_recovery")
	if err != nil || latestCapacity.State != "released" {
		t.Fatalf("capacity=%+v err=%v", latestCapacity, err)
	}
	if released, err := worker.Scheduler.Sessions.ReleasedOrExpiredByRunID(ctx, "run_orphan_admission_recovery"); err != nil || !released {
		t.Fatalf("session released=%v err=%v", released, err)
	}
}

func TestRuntimeRecoveryWorkerLegacyOrphanWithoutCapacityBindingIsNoop(t *testing.T) {
	worker := NewRuntimeRecoveryWorker(nil, nil, nil, "runtime-session-secret-test", "recovery-test")

	changed, err := worker.releaseOrphanedRuntimeAdmissions(context.Background(), runtimepkg.RuntimeDispatch{
		RunID:      "run_pre_capacity_binding",
		DispatchID: "dispatch_pre_capacity_binding",
	})
	if err != nil {
		t.Fatalf("legacy dispatch without a capacity binding must be a no-op, err=%v", err)
	}
	if changed {
		t.Fatalf("legacy dispatch without a capacity binding must not report a release")
	}
}

func TestRuntimeRecoveryWorkerDrainDeadlineQueuesAbort(t *testing.T) {
	ctx := context.Background()
	base := time.Date(2026, 7, 12, 11, 0, 0, 0, time.UTC)
	repos := persistence.NewRepositories(nil)
	hosts := runtimepkg.NewRuntimeHostRepository(nil)
	registerRecoveryHost(t, hosts, base, "host_drain")
	createRecoveryRun(t, repos, "run_drain", "")
	createRecoveryDispatch(t, hosts, base, "run_drain", "host_drain", "reservation_drain", "dispatch_drain")
	if err := hosts.SetHostStatus(ctx, "host_drain", "draining", base.Add(10*time.Second)); err != nil {
		t.Fatal(err)
	}

	worker := NewRuntimeRecoveryWorker(repos, hosts, queue.NewMemoryDistributedLockManager(), "runtime-session-secret-test", "recovery-test")
	worker.Now = func() time.Time { return base.Add(20 * time.Second) }
	result, err := worker.Reconcile(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result["abortQueued"] != 1 || result["orphanedRuns"] != 0 {
		t.Fatalf("reconcile result=%#v", result)
	}
	run, err := repos.AgentRuns.GetRunInternal(ctx, "run_drain")
	if err != nil || run.Status != "aborting" {
		t.Fatalf("run=%+v err=%v", run, err)
	}
	record, _, leaseErr := repos.Queue.Lease(ctx, queue.QueueRuntimeAbort, "abort-worker", time.Minute, "runtime_abort")
	if leaseErr != nil || record["taskId"] != "run_drain" {
		t.Fatalf("abort record=%#v err=%v", record, leaseErr)
	}
	dispatch, err := hosts.GetDispatch(ctx, "dispatch_drain")
	if err != nil || dispatch.State == "orphaned" {
		t.Fatalf("drain dispatch must remain active for authorized abort: %+v err=%v", dispatch, err)
	}
	replayed, err := worker.Reconcile(ctx)
	if err != nil || replayed["abortQueued"] != 0 {
		t.Fatalf("idempotent drain reconcile=%#v err=%v", replayed, err)
	}
}

func TestRuntimeRecoveryWorkerClaimsHealthyStaleDispatchAndResumesEvents(t *testing.T) {
	ctx := context.Background()
	base := time.Now().UTC()
	repos := persistence.NewRepositories(nil)
	hosts := runtimepkg.NewRuntimeHostRepository(nil)
	registerRecoveryHost(t, hosts, base, "host_healthy")
	createRecoveryRun(t, repos, "run_healthy_recovery", "")
	createRecoveryDispatch(t, hosts, base, "run_healthy_recovery", "host_healthy", "reservation_healthy", "dispatch_healthy")
	client := &fakeAsyncRuntimeClient{status: runtimepkg.AsyncRuntimeStatus{RunID: "run_healthy_recovery", Status: "running", LastEventSequence: 1}}
	worker := NewRuntimeRecoveryWorker(repos, hosts, queue.NewMemoryDistributedLockManager(), "runtime-session-secret-test", "recovery-test")
	worker.Client = client
	worker.TicketSecret = "runtime-ticket-secret"
	accepted, err := hosts.GetDispatch(ctx, "dispatch_healthy")
	if err != nil || accepted.NextRecoveryCheckAt.IsZero() {
		t.Fatalf("accepted dispatch=%+v err=%v", accepted, err)
	}

	// A newly accepted Run must not be claimed immediately. Advance the worker's
	// logical clock to its persisted due time only after proving that boundary.
	worker.Now = func() time.Time { return accepted.NextRecoveryCheckAt.Add(-time.Nanosecond) }
	beforeDue, err := worker.Reconcile(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if beforeDue["staleDispatches"] != 0 {
		t.Fatalf("accepted dispatch was recovered before its due time: %#v", beforeDue)
	}
	worker.Now = func() time.Time { return accepted.NextRecoveryCheckAt }

	result, err := worker.Reconcile(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result["staleDispatches"] != 1 || result["statusRecovered"] != 1 || result["recoveryDeferred"] != 0 {
		t.Fatalf("reconcile result=%#v", result)
	}
	dispatch, err := hosts.GetDispatch(ctx, "dispatch_healthy")
	if err != nil || dispatch.State != "accepted" || dispatch.NextRecoveryCheckAt.IsZero() {
		t.Fatalf("dispatch=%+v err=%v", dispatch, err)
	}
	eventJob, _, err := repos.Queue.Lease(ctx, queue.QueueRuntimeEvents, "runtime-events-recovery", time.Minute, "runtime_event_ingest")
	if err != nil || eventJob["taskId"] != "run_healthy_recovery" {
		t.Fatalf("event job=%#v err=%v", eventJob, err)
	}
}

func TestRuntimeRecoveryWorkerOrphansMissingRunAndReleasesHealthyHostSlot(t *testing.T) {
	ctx := context.Background()
	base := time.Now().UTC()
	repos := persistence.NewRepositories(nil)
	hosts := runtimepkg.NewRuntimeHostRepository(nil)
	registerRecoveryHost(t, hosts, base, "host_missing")
	createRecoveryRun(t, repos, "run_missing_recovery", "")
	capacity := createRecoveryDispatch(t, hosts, base, "run_missing_recovery", "host_missing", "reservation_missing", "dispatch_missing")
	dispatch, err := hosts.GetDispatch(ctx, "dispatch_missing")
	if err != nil {
		t.Fatal(err)
	}
	worker := NewRuntimeRecoveryWorker(repos, hosts, queue.NewMemoryDistributedLockManager(), "runtime-session-secret-test", "recovery-test")
	worker.Scheduler.Capacity = capacity
	worker.Client = &fakeAsyncRuntimeClient{statusErr: errors.New("RUNTIME_RUN_NOT_FOUND")}
	worker.TicketSecret = "runtime-ticket-secret"
	worker.Now = func() time.Time { return dispatch.NextRecoveryCheckAt }

	result, err := worker.Reconcile(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result["statusRecovered"] != 1 || result["recoveryDeferred"] != 0 {
		t.Fatalf("reconcile result=%#v", result)
	}
	run, err := repos.AgentRuns.GetRunInternal(ctx, "run_missing_recovery")
	if err != nil || run.Status != "orphaned" {
		t.Fatalf("run=%+v err=%v", run, err)
	}
	updatedDispatch, err := hosts.GetDispatch(ctx, "dispatch_missing")
	if err != nil || updatedDispatch.State != "orphaned" {
		t.Fatalf("dispatch=%+v err=%v", updatedDispatch, err)
	}
	reservation, err := hosts.GetReservation(ctx, "reservation_missing")
	if err != nil || reservation.State != "released" {
		t.Fatalf("reservation=%+v err=%v", reservation, err)
	}
}

func TestRuntimeRecoveryWorkerOrphanAllowsMissingInMemorySessionHandle(t *testing.T) {
	ctx := context.Background()
	base := time.Now().UTC()
	repos := persistence.NewRepositories(nil)
	hosts := runtimepkg.NewRuntimeHostRepository(nil)
	registerRecoveryHost(t, hosts, base, "host_session_restart")
	createRecoveryRun(t, repos, "run_session_restart", "thread_session_restart")
	capacity := createRecoveryDispatch(t, hosts, base, "run_session_restart", "host_session_restart", "reservation_session_restart", "dispatch_session_restart")
	dispatch, err := hosts.GetDispatch(ctx, "dispatch_session_restart")
	if err != nil {
		t.Fatal(err)
	}
	worker := NewRuntimeRecoveryWorker(repos, hosts, queue.NewMemoryDistributedLockManager(), "runtime-session-secret-test", "recovery-test")
	worker.Scheduler.Capacity = capacity
	worker.Client = &fakeAsyncRuntimeClient{statusErr: errors.New("RUNTIME_RUN_NOT_FOUND")}
	worker.TicketSecret = "runtime-ticket-secret"
	worker.Now = func() time.Time { return dispatch.NextRecoveryCheckAt }

	result, err := worker.Reconcile(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result["statusRecovered"] != 1 {
		t.Fatalf("reconcile result=%#v", result)
	}
	run, err := repos.AgentRuns.GetRunInternal(ctx, "run_session_restart")
	if err != nil || run.Status != "orphaned" {
		t.Fatalf("run=%+v err=%v", run, err)
	}
}

func TestRuntimeRecoveryWorkerConvergesAlreadyOrphanedProductProjectionAfterPlanExpiry(t *testing.T) {
	ctx := context.Background()
	base := time.Now().UTC()
	const (
		hostID        = "host_orphaned_product_projection"
		runID         = "run_orphaned_product_projection"
		taskID        = "task_orphaned_product_projection"
		threadID      = "thread_orphaned_product_projection"
		userID        = "user_orphaned_product_projection"
		workspaceID   = "workspace_orphaned_product_projection"
		reservationID = "reservation_orphaned_product_projection"
		dispatchID    = "dispatch_orphaned_product_projection"
	)
	repos := persistence.NewRepositories(nil)
	hosts := runtimepkg.NewRuntimeHostRepository(nil)
	registerRecoveryHost(t, hosts, base, hostID)
	repos.ChatTasks.CreateChatThread(threadID, userID, workspaceID, "faya_germination", "Faya recovery")
	usage := services.NewPermissionUsageService(repos)
	admission := usage.CheckAdmission(userID, workspaceID, "work_ai_faya_germination", map[string]any{"generation": 1})
	quota := usage.ReserveQuota(stringValue(admission["permissionCheckId"]), taskID, "work_ai_faya_germination", map[string]int{"generation": 1})
	quotaReservationID := stringValue(quota["reservationId"])
	if quotaReservationID == "" {
		t.Fatalf("quota reservation=%#v", quota)
	}
	repos.ChatTasks.CreateAiTask(taskID, "work_ai_faya_germination", userID, workspaceID, map[string]any{
		"threadId": threadID, "reservationId": quotaReservationID, "agentRunId": runID,
	})
	repos.ChatTasks.UpdateAiTaskStatus(taskID, "", "orphaned", map[string]any{"code": "AGENT_PLAN_EXPIRED"}, map[string]any{})
	settled := usage.SettleUsage(taskID, quotaReservationID, map[string]any{
		"usageKey": "usage_" + runID, "userId": userID, "workspaceId": workspaceID,
		"meterType": "generation", "amount": 1.0,
	})
	if stringValue(settled["status"]) != "settled" {
		t.Fatalf("quota settlement=%#v", settled)
	}
	if _, _, err := repos.AgentRuns.CreateRun(ctx, persistence.CreateAgentRunCommand{Record: persistence.AgentRunRecord{
		AgentRunID: runID, TenantID: "tenant_1", UserID: userID, WorkspaceID: workspaceID, ThreadID: threadID, TaskID: taskID,
		IdempotencyKey: "idem_" + runID, RequestHash: "hash_" + runID, Status: "queued",
		WorkspaceVersion: 1, BindingVersion: 1, ContextGeneration: 1,
	}}); err != nil {
		t.Fatal(err)
	}
	recoveryRun, err := repos.AgentRuns.GetRunInternal(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	contextPlan := runtimeTicketTestPlan(runID, "cap-v1", "product_thread")
	contextPlan.ManifestVersion = "manifest-v1"
	if err := repos.AgentRuns.SaveWorkspaceContext(ctx, testFrozenWorkspaceContext(t, recoveryRun, contextPlan, 0, 0)); err != nil {
		t.Fatal(err)
	}
	capacity := createRecoveryDispatch(t, hosts, base, runID, hostID, reservationID, dispatchID, "product_thread")
	dispatch, err := hosts.GetDispatch(ctx, dispatchID)
	if err != nil {
		t.Fatal(err)
	}
	worker := NewRuntimeRecoveryWorker(repos, hosts, queue.NewMemoryDistributedLockManager(), "runtime-session-secret-test", "recovery-test")
	worker.Scheduler.Capacity = capacity
	worker.Client = &fakeAsyncRuntimeClient{statusErr: errors.New("RUNTIME_RUN_NOT_FOUND")}
	worker.TicketSecret = "runtime-ticket-secret"
	worker.Now = func() time.Time { return dispatch.NextRecoveryCheckAt }
	worker.terminalProjector = func(persistence.AgentRunRecord, string, map[string]any, map[string]any, map[string]any) error {
		return errors.New("AGENT_PLAN_EXPIRED")
	}

	result, err := worker.Reconcile(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result["statusRecovered"] != 1 || result["recoveryDeferred"] != 0 {
		t.Fatalf("reconcile result=%#v", result)
	}
	run, err := repos.AgentRuns.GetRunInternal(ctx, runID)
	if err != nil || run.Status != "orphaned" {
		t.Fatalf("run=%+v err=%v", run, err)
	}
	updatedDispatch, err := hosts.GetDispatch(ctx, dispatchID)
	if err != nil || updatedDispatch.State != "orphaned" {
		t.Fatalf("dispatch=%+v err=%v", updatedDispatch, err)
	}
	reservation, err := hosts.GetReservation(ctx, reservationID)
	if err != nil || reservation.State != "released" {
		t.Fatalf("reservation=%+v err=%v", reservation, err)
	}
	host, err := hosts.GetHost(ctx, hostID)
	if err != nil || host.ActiveRuns != 0 || host.ReservedRuns != 0 || host.RecoveryState != "reconciled" {
		t.Fatalf("host=%+v err=%v", host, err)
	}
	replayed, err := worker.Reconcile(ctx)
	if err != nil || replayed["statusRecovered"] != 0 {
		t.Fatalf("idempotent recovery=%#v err=%v", replayed, err)
	}
}

func TestRuntimeRecoveryWorkerConvergesRealProjectorAfterSettledQuotaReplay(t *testing.T) {
	ctx := context.Background()
	base := time.Now().UTC()
	const (
		hostID        = "host_settled_quota_projection"
		runID         = "run_settled_quota_projection"
		taskID        = "task_settled_quota_projection"
		threadID      = "thread_settled_quota_projection"
		userID        = "user_settled_quota_projection"
		workspaceID   = "workspace_settled_quota_projection"
		reservationID = "reservation_settled_quota_projection"
		dispatchID    = "dispatch_settled_quota_projection"
	)
	repos := persistence.NewRepositories(nil)
	hosts := runtimepkg.NewRuntimeHostRepository(nil)
	registerRecoveryHost(t, hosts, base, hostID)
	repos.ChatTasks.CreateChatThread(threadID, userID, workspaceID, "faya_germination", "Faya recovery")
	usage := services.NewPermissionUsageService(repos)
	admission := usage.CheckAdmission(userID, workspaceID, "work_ai_faya_germination", map[string]any{"generation": 1})
	quota := usage.ReserveQuota(stringValue(admission["permissionCheckId"]), taskID, "work_ai_faya_germination", map[string]int{"generation": 1})
	quotaReservationID := stringValue(quota["reservationId"])
	if quotaReservationID == "" {
		t.Fatalf("quota reservation=%#v", quota)
	}
	repos.ChatTasks.CreateAiTask(taskID, "work_ai_faya_germination", userID, workspaceID, map[string]any{
		"threadId": threadID, "reservationId": quotaReservationID, "agentRunId": runID,
	})
	repos.ChatTasks.UpdateAiTaskStatus(taskID, "", "orphaned", map[string]any{"code": "AGENT_PLAN_EXPIRED"}, map[string]any{})
	settled := usage.SettleUsage(taskID, quotaReservationID, map[string]any{
		"usageKey": "usage_" + runID, "userId": userID, "workspaceId": workspaceID,
		"meterType": "generation", "amount": 1.0,
	})
	if stringValue(settled["status"]) != "settled" {
		t.Fatalf("quota settlement=%#v", settled)
	}
	if _, _, err := repos.AgentRuns.CreateRun(ctx, persistence.CreateAgentRunCommand{Record: persistence.AgentRunRecord{
		AgentRunID: runID, TenantID: "tenant_1", UserID: userID, WorkspaceID: workspaceID, ThreadID: threadID, TaskID: taskID,
		IdempotencyKey: "idem_" + runID, RequestHash: "hash_" + runID, Status: "queued",
		WorkspaceVersion: 1, BindingVersion: 1, ContextGeneration: 1,
	}}); err != nil {
		t.Fatal(err)
	}
	capacity := createRecoveryDispatch(t, hosts, base, runID, hostID, reservationID, dispatchID, "product_thread")
	dispatch, err := hosts.GetDispatch(ctx, dispatchID)
	if err != nil {
		t.Fatal(err)
	}
	worker := NewRuntimeRecoveryWorker(repos, hosts, queue.NewMemoryDistributedLockManager(), "runtime-session-secret-test", "recovery-test")
	worker.Scheduler.Capacity = capacity
	if err := worker.orphanStalledDispatch(ctx, dispatch, "RUNTIME_RUN_NOT_FOUND"); err != nil {
		t.Fatal(err)
	}
	updatedRun, err := repos.AgentRuns.GetRunInternal(ctx, runID)
	if err != nil || updatedRun.Status != "orphaned" {
		t.Fatalf("run=%+v err=%v", updatedRun, err)
	}
	updatedDispatch, err := hosts.GetDispatch(ctx, dispatchID)
	if err != nil || updatedDispatch.State != "orphaned" {
		t.Fatalf("dispatch=%+v err=%v", updatedDispatch, err)
	}
	reservation, err := hosts.GetReservation(ctx, reservationID)
	if err != nil || reservation.State != "released" {
		t.Fatalf("reservation=%+v err=%v", reservation, err)
	}
	host, err := hosts.GetHost(ctx, hostID)
	if err != nil || host.ActiveRuns != 0 || host.ReservedRuns != 0 || host.RecoveryState != "reconciled" {
		t.Fatalf("host=%+v err=%v", host, err)
	}
}

func TestRuntimeRecoveryWorkerRejectsPlanExpiryWithoutSettledTerminalProductProjection(t *testing.T) {
	ctx := context.Background()
	base := time.Now().UTC()
	repos := persistence.NewRepositories(nil)
	hosts := runtimepkg.NewRuntimeHostRepository(nil)
	registerRecoveryHost(t, hosts, base, "host_nonterminal_product_projection")
	usage := services.NewPermissionUsageService(repos)
	admission := usage.CheckAdmission("user_1", "workspace_1", "work_ai_faya_germination", map[string]any{"generation": 1})
	quota := usage.ReserveQuota(stringValue(admission["permissionCheckId"]), "task_nonterminal_product_projection", "work_ai_faya_germination", map[string]int{"generation": 1})
	quotaReservationID := stringValue(quota["reservationId"])
	if quotaReservationID == "" {
		t.Fatalf("quota reservation=%#v", quota)
	}
	repos.ChatTasks.CreateAiTask("task_nonterminal_product_projection", "work_ai_faya_germination", "user_1", "workspace_1", map[string]any{
		"reservationId": quotaReservationID, "agentRunId": "run_nonterminal_product_projection",
	})
	repos.ChatTasks.UpdateAiTaskStatus("task_nonterminal_product_projection", "", "orphaned", map[string]any{"code": "AGENT_PLAN_EXPIRED"}, map[string]any{})
	if _, _, err := repos.AgentRuns.CreateRun(ctx, persistence.CreateAgentRunCommand{Record: persistence.AgentRunRecord{
		AgentRunID: "run_nonterminal_product_projection", TenantID: "tenant_1", UserID: "user_1", WorkspaceID: "workspace_1", TaskID: "task_nonterminal_product_projection",
		IdempotencyKey: "idem_nonterminal_product_projection", RequestHash: "hash_nonterminal_product_projection", Status: "queued",
		WorkspaceVersion: 1, BindingVersion: 1, ContextGeneration: 1,
	}}); err != nil {
		t.Fatal(err)
	}
	createRecoveryDispatch(t, hosts, base, "run_nonterminal_product_projection", "host_nonterminal_product_projection", "reservation_nonterminal_product_projection", "dispatch_nonterminal_product_projection", "product_thread")
	dispatch, err := hosts.GetDispatch(ctx, "dispatch_nonterminal_product_projection")
	if err != nil {
		t.Fatal(err)
	}
	worker := NewRuntimeRecoveryWorker(repos, hosts, queue.NewMemoryDistributedLockManager(), "runtime-session-secret-test", "recovery-test")
	worker.terminalProjector = func(persistence.AgentRunRecord, string, map[string]any, map[string]any, map[string]any) error {
		return errors.New("AGENT_PLAN_EXPIRED")
	}
	if err := worker.orphanStalledDispatch(ctx, dispatch, "RUNTIME_RUN_NOT_FOUND"); err == nil || err.Error() != "AGENT_PLAN_EXPIRED" {
		t.Fatalf("orphan err=%v", err)
	}
	reservation, err := hosts.GetReservation(ctx, "reservation_nonterminal_product_projection")
	if err != nil || reservation.State != "accepted" {
		t.Fatalf("reservation=%+v err=%v", reservation, err)
	}
}

func TestRuntimeRecoveryWorkerExpiredClaimDoesNotProjectOrRelease(t *testing.T) {
	ctx := context.Background()
	base := time.Now().UTC()
	repos := persistence.NewRepositories(nil)
	hosts := runtimepkg.NewRuntimeHostRepository(nil)
	registerRecoveryHost(t, hosts, base, "host_expired_recovery_projection")
	createRecoveryRun(t, repos, "run_expired_recovery_projection", "")
	capacity := createRecoveryDispatch(t, hosts, base, "run_expired_recovery_projection", "host_expired_recovery_projection", "reservation_expired_recovery_projection", "dispatch_expired_recovery_projection")
	dispatch, err := hosts.GetDispatch(ctx, "dispatch_expired_recovery_projection")
	if err != nil {
		t.Fatal(err)
	}
	staleClaim := runtimepkg.DispatchRecoveryClaim{
		DispatchID: dispatch.DispatchID, OwnerInstanceID: "stale-recovery-owner", FencingToken: 71,
		ExpiresAt: time.Now().UTC().Add(-time.Second), ExpectedState: dispatch.State, ExpectedVersion: dispatch.Version,
	}
	if err := hosts.ClaimDispatchRecovery(ctx, staleClaim); err != nil {
		t.Fatal(err)
	}
	beforeEvents, err := repos.AgentRuns.ListPublicEvents(ctx, dispatch.RunID, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	worker := NewRuntimeRecoveryWorker(repos, hosts, queue.NewMemoryDistributedLockManager(), "runtime-session-secret-test", "recovery-test")
	worker.Scheduler.Capacity = capacity
	projected := false
	worker.terminalProjector = func(persistence.AgentRunRecord, string, map[string]any, map[string]any, map[string]any) error {
		projected = true
		return nil
	}
	if err := worker.orphanStalledDispatchWithRecoveryClaim(ctx, dispatch, "RUNTIME_RUN_STALLED", staleClaim); err == nil || err.Error() != "STALE_FENCING_TOKEN" {
		t.Fatalf("expired recovery claim error=%v", err)
	}
	if projected {
		t.Fatal("expired recovery claimant invoked terminal Product projection")
	}
	run, err := repos.AgentRuns.GetRunInternal(ctx, dispatch.RunID)
	if err != nil || run.Status != "running" {
		t.Fatalf("expired recovery claimant projected AgentRun terminal state: run=%+v err=%v", run, err)
	}
	afterEvents, err := repos.AgentRuns.ListPublicEvents(ctx, dispatch.RunID, 0, 100)
	if err != nil || len(afterEvents.Items) != len(beforeEvents.Items) || afterEvents.LatestSequence != beforeEvents.LatestSequence {
		t.Fatalf("expired recovery claimant appended public event: before=%+v after=%+v err=%v", beforeEvents, afterEvents, err)
	}
	reservation, err := hosts.GetReservation(ctx, dispatch.ReservationID)
	if err != nil || reservation.State != "accepted" {
		t.Fatalf("expired recovery claimant released reservation: reservation=%+v err=%v", reservation, err)
	}
	storedDispatch, err := hosts.GetDispatch(ctx, dispatch.DispatchID)
	if err != nil || storedDispatch.State != "recovering" {
		t.Fatalf("expired recovery claimant changed dispatch terminal state: dispatch=%+v err=%v", storedDispatch, err)
	}
	storedCapacity, err := capacity.GetLatestByRunID(ctx, dispatch.RunID)
	if err != nil || storedCapacity.State != "accepted" {
		t.Fatalf("expired recovery claimant released capacity: capacity=%+v err=%v", storedCapacity, err)
	}
}

func TestRuntimeRecoveryWorkerClaimedOrphanFinalizesSlotBeforeProjection(t *testing.T) {
	ctx := context.Background()
	base := time.Now().UTC()
	repos := persistence.NewRepositories(nil)
	hosts := runtimepkg.NewRuntimeHostRepository(nil)
	registerRecoveryHost(t, hosts, base, "host_claimed_orphan_order")
	createRecoveryRun(t, repos, "run_claimed_orphan_order", "")
	capacity := createRecoveryDispatch(t, hosts, base, "run_claimed_orphan_order", "host_claimed_orphan_order", "reservation_claimed_orphan_order", "dispatch_claimed_orphan_order")
	dispatch, err := hosts.GetDispatch(ctx, "dispatch_claimed_orphan_order")
	if err != nil {
		t.Fatal(err)
	}
	claim := runtimepkg.DispatchRecoveryClaim{
		DispatchID: dispatch.DispatchID, OwnerInstanceID: "current-recovery-owner", FencingToken: 72,
		ExpiresAt: time.Now().UTC().Add(time.Minute), ExpectedState: dispatch.State, ExpectedVersion: dispatch.Version,
	}
	if err := hosts.ClaimDispatchRecovery(ctx, claim); err != nil {
		t.Fatal(err)
	}
	worker := NewRuntimeRecoveryWorker(repos, hosts, queue.NewMemoryDistributedLockManager(), "runtime-session-secret-test", "recovery-test")
	worker.Scheduler.Capacity = capacity
	projectedAfterSlotFinalized := false
	worker.terminalProjector = func(persistence.AgentRunRecord, string, map[string]any, map[string]any, map[string]any) error {
		storedDispatch, dispatchErr := hosts.GetDispatch(ctx, dispatch.DispatchID)
		reservation, reservationErr := hosts.GetReservation(ctx, dispatch.ReservationID)
		storedCapacity, capacityErr := capacity.GetLatestByRunID(ctx, dispatch.RunID)
		if dispatchErr != nil || reservationErr != nil || capacityErr != nil || storedDispatch.State != "orphaned" || reservation.State != "released" || storedCapacity.State != "accepted" {
			return errors.New("projection_before_claimed_slot_finalization")
		}
		projectedAfterSlotFinalized = true
		return nil
	}
	if err := worker.orphanStalledDispatchWithRecoveryClaim(ctx, dispatch, "RUNTIME_RUN_STALLED", claim); err != nil {
		t.Fatal(err)
	}
	if !projectedAfterSlotFinalized {
		t.Fatal("claimed orphan did not project after slot finalization")
	}
	finalCapacity, err := capacity.GetLatestByRunID(ctx, dispatch.RunID)
	if err != nil || finalCapacity.State != "released" {
		t.Fatalf("claimed orphan did not release capacity after projection: capacity=%+v err=%v", finalCapacity, err)
	}
}

func TestRuntimeDispatchRecoveryTerminalAfterBoundedDeferral(t *testing.T) {
	now := time.Now().UTC()
	dispatch := runtimepkg.RuntimeDispatch{CreatedAt: now.Add(-runtimeDispatchRecoveryMaxDeferral - time.Second)}
	if !runtimeDispatchRecoveryTerminal(nil, dispatch, now) {
		t.Fatal("active runtime status beyond the recovery SLA must be terminal")
	}
	dispatch.CreatedAt = now.Add(-runtimeDispatchRecoveryMaxDeferral + time.Second)
	if runtimeDispatchRecoveryTerminal(nil, dispatch, now) {
		t.Fatal("recent runtime status must remain recoverable")
	}
}

func TestRuntimeRecoveryRetryableEmptyResult(t *testing.T) {
	if !runtimeRecoveryRetryable(pgx.ErrNoRows) {
		t.Fatal("empty postgres result must keep the recovery loop alive")
	}
	if !runtimeRecoveryRetryable(errors.New("no rows in result set")) {
		t.Fatal("driver empty-result wording must keep the recovery loop alive")
	}
	if runtimeRecoveryRetryable(errors.New("RUNTIME_RECOVERY_UNAVAILABLE")) {
		t.Fatal("dependency failures must remain terminal")
	}
	if !runtimeRecoveryRetryable(errors.New("DISTRIBUTED_LOCK_UNAVAILABLE")) {
		t.Fatal("runtime Redis outage after startup must retry the control loop")
	}
	if !runtimeRecoveryRetryable(context.DeadlineExceeded) {
		t.Fatal("runtime control-plane timeout must retry the control loop")
	}
}

func TestRuntimeRecoveryWorkerDoesNotHideGlobalLeaseOutageAsContention(t *testing.T) {
	worker := NewRuntimeRecoveryWorker(persistence.NewRepositories(nil), runtimepkg.NewRuntimeHostRepository(nil), queue.NewMemoryDistributedLockManager(), "runtime-session-secret-test", "recovery-test")
	worker.reconcileLeaseAcquire = func(context.Context, string, time.Duration) (queue.DistributedLease, error) {
		return queue.DistributedLease{}, errors.New("DISTRIBUTED_LOCK_UNAVAILABLE")
	}
	if result, err := worker.Reconcile(context.Background()); err == nil || result != nil || err.Error() != "DISTRIBUTED_LOCK_UNAVAILABLE" {
		t.Fatalf("lease outage result=%#v err=%v", result, err)
	}
	worker.reconcileLeaseAcquire = func(context.Context, string, time.Duration) (queue.DistributedLease, error) {
		return queue.DistributedLease{}, errors.New("SERVICE_BUSY")
	}
	result, err := worker.Reconcile(context.Background())
	if err != nil || result["status"] != "skipped" || result["reason"] != "reconcile_owner_active" {
		t.Fatalf("lease contention result=%#v err=%v", result, err)
	}
	worker.reconcileLeaseAcquire = func(context.Context, string, time.Duration) (queue.DistributedLease, error) {
		return queue.DistributedLease{}, errors.New("SERVICE_BUSY: runtime-reconcile global lease held")
	}
	result, err = worker.Reconcile(context.Background())
	if err != nil || result["status"] != "skipped" || result["reason"] != "reconcile_owner_active" {
		t.Fatalf("decorated lease contention result=%#v err=%v", result, err)
	}
}

func TestRuntimeRecoveryWorkerDispatchRecoveryOnlyDefersExplicitContention(t *testing.T) {
	ctx := context.Background()
	repos := persistence.NewRepositories(nil)
	hosts := runtimepkg.NewRuntimeHostRepository(nil)
	base := time.Now().UTC()
	registerRecoveryHost(t, hosts, base, "host_dispatch_recovery_claim")
	createRecoveryRun(t, repos, "run_dispatch_recovery_claim", "")
	createRecoveryDispatch(t, hosts, base, "run_dispatch_recovery_claim", "host_dispatch_recovery_claim", "reservation_dispatch_recovery_claim", "dispatch_recovery_claim")
	dispatch, err := hosts.GetDispatch(ctx, "dispatch_recovery_claim")
	if err != nil {
		t.Fatal(err)
	}
	worker := NewRuntimeRecoveryWorker(repos, hosts, queue.NewMemoryDistributedLockManager(), "runtime-session-secret-test", "recovery-test")
	worker.Now = func() time.Time { return dispatch.NextRecoveryCheckAt }
	worker.dispatchRecoveryLeaseAcquire = func(context.Context, string, string, time.Duration) (queue.DistributedLease, error) {
		return queue.DistributedLease{}, errors.New("SERVICE_BUSY: dispatch recovery held")
	}
	result, err := worker.Reconcile(ctx)
	if err != nil || result["staleDispatches"] != 1 || result["recoveryDeferred"] != 1 {
		t.Fatalf("contention must defer only this dispatch: result=%#v err=%v", result, err)
	}
	worker.dispatchRecoveryLeaseAcquire = func(context.Context, string, string, time.Duration) (queue.DistributedLease, error) {
		return queue.DistributedLease{}, errors.New("DISTRIBUTED_LOCK_UNAVAILABLE")
	}
	if result, err := worker.Reconcile(ctx); err == nil || result != nil || err.Error() != "DISTRIBUTED_LOCK_UNAVAILABLE" {
		t.Fatalf("lock backend outage must remain visible: result=%#v err=%v", result, err)
	}
}

func TestRuntimeRecoveryWorkerReconcileRequiresStableOwnerIdentity(t *testing.T) {
	worker := NewRuntimeRecoveryWorker(persistence.NewRepositories(nil), runtimepkg.NewRuntimeHostRepository(nil), queue.NewMemoryDistributedLockManager(), "runtime-session-secret-test", "")
	if result, err := worker.Reconcile(context.Background()); err == nil || result != nil || err.Error() != "RUNTIME_RECOVERY_UNAVAILABLE" {
		t.Fatalf("missing recovery owner result=%#v err=%v", result, err)
	}
}

func TestRuntimeRecoveryWorkerReconcileFailsClosedWithoutRequiredDependencies(t *testing.T) {
	worker := RuntimeRecoveryWorker{
		Locks:                      queue.NewMemoryDistributedLockManager(),
		OwnerInstanceID:            "recovery-test",
		SessionKeyEncryptionSecret: "runtime-session-secret-test",
	}
	if result, err := worker.Reconcile(context.Background()); err == nil || result != nil || err.Error() != "RUNTIME_RECOVERY_UNAVAILABLE" {
		t.Fatalf("missing dependency result=%#v err=%v", result, err)
	}
}

func TestRuntimeRecoveryWorkerRunFailsClosedWithoutRedisTairLeaseBackend(t *testing.T) {
	worker := NewRuntimeRecoveryWorker(persistence.NewRepositories(nil), runtimepkg.NewRuntimeHostRepository(nil), queue.NewMemoryDistributedLockManager(), "runtime-session-secret-test", "recovery-test")
	if err := worker.Run(context.Background(), "recovery-consumer"); err == nil || err.Error() != "RUNTIME_RECOVERY_UNAVAILABLE" {
		t.Fatalf("memory lock worker startup err=%v", err)
	}
}

func TestRuntimeRecoveryWorkerFailsClosedWhenEventRecoveryEnqueueIsNotDurable(t *testing.T) {
	repos := persistence.NewRepositories(&persistence.Database{Disabled: false})
	worker := NewRuntimeRecoveryWorker(repos, runtimepkg.NewRuntimeHostRepository(nil), queue.NewMemoryDistributedLockManager(), "runtime-session-secret-test", "recovery-test")
	err := worker.enqueueRuntimeEventRecovery(runtimepkg.RuntimeDispatch{
		DispatchID:    "dispatch_recovery_queue_unavailable",
		RunID:         "run_recovery_queue_unavailable",
		RuntimeHostID: "host_recovery_queue_unavailable",
	})
	if err == nil || err.Error() != "QUEUE_DURABLE_BACKEND_UNAVAILABLE" {
		t.Fatalf("enqueue err=%v, want QUEUE_DURABLE_BACKEND_UNAVAILABLE", err)
	}
}

func TestRuntimeRecoveryEventQueueEnqueuedRejectsSuccessShapedWrongRecord(t *testing.T) {
	dispatch := runtimepkg.RuntimeDispatch{DispatchID: "dispatch_queue_contract", RunID: "run_queue_contract", RuntimeHostID: "host_queue_contract"}
	queueID := queue.QueueRuntimeEvents + ":" + dispatch.DispatchID
	valid := map[string]any{
		"queueId": queueID, "queueName": queue.QueueRuntimeEvents, "taskType": "runtime_event_ingest", "taskId": dispatch.RunID,
		"dedupeKey": "runtime_event_ingest:" + dispatch.DispatchID, "status": "pending",
		"payload": map[string]any{"runId": dispatch.RunID, "dispatchId": dispatch.DispatchID, "runtimeHostId": dispatch.RuntimeHostID},
	}
	if !runtimeRecoveryEventQueueEnqueued(valid, dispatch, queueID) {
		t.Fatal("canonical durable queue record must be accepted")
	}
	valid["queueName"] = "other"
	if runtimeRecoveryEventQueueEnqueued(valid, dispatch, queueID) {
		t.Fatal("success-shaped record for another queue must not hide an enqueue failure")
	}
}

func TestRuntimeRecoveryEventQueueEnqueuedAcceptsLineagedTerminalRecoveryQueue(t *testing.T) {
	dispatch := runtimepkg.RuntimeDispatch{DispatchID: "dispatch_terminal_lineage", RunID: "run_terminal_lineage", RuntimeHostID: "host_terminal_lineage"}
	recovery := map[string]any{
		"queueId": "runtime_events:recovery:7c1d0c9f92a4:1", "queueName": queue.QueueRuntimeEvents,
		"taskType": "runtime_event_ingest", "taskId": dispatch.RunID,
		"dedupeKey": "runtime_terminal_recovery:7c1d0c9f92a4:1", "status": "leased",
		"payload": map[string]any{
			"runId": dispatch.RunID, "dispatchId": dispatch.DispatchID, "runtimeHostId": dispatch.RuntimeHostID,
			"terminalConvergenceId": "terminal:" + dispatch.DispatchID + ":1",
		},
	}
	if !runtimeRecoveryEventQueueEnqueued(recovery, dispatch, workerMapString(recovery, "queueId")) {
		t.Fatal("lineaged terminal recovery queue must be accepted while leased")
	}
	recovery["dedupeKey"] = "runtime_event_ingest:" + dispatch.DispatchID
	if runtimeRecoveryEventQueueEnqueued(recovery, dispatch, workerMapString(recovery, "queueId")) {
		t.Fatal("lineaged recovery queue without its dedicated dedupe contract must be rejected")
	}
}

func TestRuntimeRecoveryWorkerSkipsBlockedTerminalCandidateAndContinues(t *testing.T) {
	blocked := runtimepkg.TerminalConvergenceRecoveryCandidate{
		ConvergenceID: "terminal:dispatch_blocked:1", DispatchID: "dispatch_blocked", RunID: "run_blocked",
	}
	valid := runtimepkg.TerminalConvergenceRecoveryCandidate{
		ConvergenceID: "terminal:dispatch_valid:1", DispatchID: "dispatch_valid", RunID: "run_valid",
	}
	worker := RuntimeRecoveryWorker{
		listIncompleteConvergences: func(context.Context, int) ([]runtimepkg.TerminalConvergenceRecoveryCandidate, error) {
			return []runtimepkg.TerminalConvergenceRecoveryCandidate{blocked, valid}, nil
		},
		getRecoveryDispatch: func(_ context.Context, dispatchID string) (runtimepkg.RuntimeDispatch, error) {
			switch dispatchID {
			case blocked.DispatchID:
				return runtimepkg.RuntimeDispatch{DispatchID: blocked.DispatchID, RunID: blocked.RunID}, nil
			case valid.DispatchID:
				return runtimepkg.RuntimeDispatch{DispatchID: valid.DispatchID, RunID: valid.RunID}, nil
			default:
				return runtimepkg.RuntimeDispatch{}, errors.New("unexpected dispatch")
			}
		},
		enqueueTerminalRecovery: func(_ context.Context, dispatch runtimepkg.RuntimeDispatch, convergenceID string) (bool, bool, error) {
			if dispatch.DispatchID == blocked.DispatchID && convergenceID == blocked.ConvergenceID {
				// A durable invalid-legacy blocker is a candidate-local skip.
				return false, true, nil
			}
			if dispatch.DispatchID == valid.DispatchID && convergenceID == valid.ConvergenceID {
				return true, false, nil
			}
			return false, false, errors.New("unexpected recovery enqueue")
		},
	}

	scanned, requeued, blockedCount, err := worker.reconcileIncompleteConvergences(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if scanned != 2 || requeued != 1 || blockedCount != 1 {
		t.Fatalf("reconcile must continue after one blocked legacy candidate: scanned=%d requeued=%d blocked=%d", scanned, requeued, blockedCount)
	}
}

func registerRecoveryHost(t *testing.T, hosts *runtimepkg.RuntimeHostRepository, observedAt time.Time, hostID string) {
	t.Helper()
	identity := runtimepkg.RuntimeHostIdentity{RuntimeHostID: hostID, InstanceID: "instance_" + hostID, Environment: "test"}
	if _, err := hosts.RegisterHost(context.Background(), identity, runtimepkg.RuntimeHostRegistration{
		Endpoint: "http://" + hostID, RuntimeVersion: "runtime-v1", AdapterVersion: "adapter-v1",
		Capabilities: runtimepkg.RuntimeCapabilitySnapshot{CapabilityHash: "cap-v1"}, SessionStoreID: "store-" + hostID, MaxActiveRuns: 4,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := hosts.HeartbeatHost(context.Background(), identity, runtimepkg.RuntimeHostHeartbeat{
		Sequence: 1, ObservedAt: observedAt, CapabilityHash: "cap-v1", SignatureKeyID: "test-key",
	}); err != nil {
		t.Fatal(err)
	}
}

func createRecoveryRun(t *testing.T, repos *persistence.Repositories, runID, threadID string) {
	t.Helper()
	scope := "detached_task"
	if threadID != "" {
		scope = "product_thread"
	}
	_, _, err := repos.AgentRuns.CreateRun(context.Background(), persistence.CreateAgentRunCommand{Record: persistence.AgentRunRecord{
		AgentRunID: runID, TenantID: "tenant_1", UserID: "user_1", WorkspaceID: "workspace_1", ThreadID: threadID,
		IdempotencyKey: "idem_" + runID, RequestHash: "hash_" + runID, Status: "planning",
		WorkspaceVersion: 1, BindingVersion: 1, ContextGeneration: 1,
		RequestSnapshot: map[string]any{"input": map[string]any{"type": "text", "text": "test"}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	run, err := repos.AgentRuns.GetRunInternal(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	plan := runtimeTicketTestPlan(runID, "cap-v1", scope)
	frozen := testFrozenWorkspaceContext(t, run, plan, 0, 0)
	plan.WorkspaceContextManifestHash = frozen.ManifestHash
	if err := repos.AgentRuns.SaveWorkspaceContext(context.Background(), frozen); err != nil {
		t.Fatal(err)
	}
	if err := repos.AgentRuns.SavePlan(context.Background(), persistence.AgentRunPlanRecord{
		AgentRunID: runID, PlanVersion: 1, PlanStatus: "executing", AgentRunStatus: "running",
		Plan: valueMap(plan),
	}); err != nil {
		t.Fatal(err)
	}
}

func createRecoveryDispatch(t *testing.T, hosts *runtimepkg.RuntimeHostRepository, base time.Time, runID, hostID, reservationID, dispatchID string, executionScope ...string) *runtimepkg.RuntimeCapacityAdmissionService {
	t.Helper()
	scope := "detached_task"
	if len(executionScope) > 0 && executionScope[0] != "" {
		scope = executionScope[0]
	}
	expiresAt := time.Now().UTC().Add(time.Hour)
	reservation, _, err := hosts.TryReserveSlot(context.Background(), runtimepkg.AtomicReservationCommand{
		ReservationID: reservationID, RunID: runID, OwnerInstanceID: "worker_1", ExecutionScope: scope,
		FencingToken: 1, LeaseTokenHash: "sha256:lease", CapabilityHash: "cap-v1", ExpiresAt: expiresAt, HeartbeatAfter: base.Add(-time.Minute), AffinityRuntimeHostID: hostID,
	})
	if err != nil {
		t.Fatal(err)
	}
	capacity := runtimepkg.NewRuntimeCapacityAdmissionService(nil)
	dimension := func(key string) runtimepkg.RuntimeCapacityDimension {
		return runtimepkg.RuntimeCapacityDimension{Key: key + ":" + runID, Limit: 1, Requested: 1, Version: 1}
	}
	capacityReservation, err := capacity.Reserve(context.Background(), runtimepkg.RuntimeCapacityCommand{
		RunID: runID, SnapshotVersion: 1, TTL: time.Hour,
		Dimensions: runtimepkg.RuntimeCapacityDimensions{
			Model: dimension("model"), AuthPool: dimension("auth"), Tool: dimension("tool"), Tenant: dimension("tenant"), User: dimension("user"),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := capacity.CommitAccepted(context.Background(), capacityReservation); err != nil {
		t.Fatal(err)
	}
	if _, err := hosts.CreateDispatch(context.Background(), runtimepkg.RuntimeDispatch{
		DispatchID: dispatchID, RunID: runID, ReservationID: reservationID, CapacityReservationID: capacityReservation.ReservationID, CapacityReservedVersion: capacityReservation.Version, RuntimeHostID: hostID,
		DispatchAttempt: 1, PlanVersion: 1, FencingToken: reservation.FencingToken,
		RunTicketJTIHash: runtimepkg.RunTicketJTIHash(dispatchID), TicketExpiresAt: expiresAt, InputManifestHash: runtimepkg.RunTicketJTIHash("manifest-" + dispatchID), OwnerInstanceID: "worker_1", LeaseTokenHash: "sha256:lease", LeaseExpiresAt: expiresAt,
	}); err != nil {
		t.Fatal(err)
	}
	if err := hosts.ConfirmDispatchAccepted(context.Background(), runtimepkg.DispatchAcceptedCommand{Fence: runtimepkg.ReservationFence{ReservationID: reservationID, RuntimeHostID: hostID, OwnerInstanceID: "worker_1", LeaseTokenHash: "sha256:lease", FencingToken: reservation.FencingToken}, DispatchID: dispatchID, RuntimeRequestID: "request_" + runID}); err != nil {
		t.Fatal(err)
	}
	return capacity
}
