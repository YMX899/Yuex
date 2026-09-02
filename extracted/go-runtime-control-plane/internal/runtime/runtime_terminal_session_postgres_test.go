package runtime

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"huahuoai/backend/source/internal/config"
	"huahuoai/backend/source/internal/persistence"
	"huahuoai/backend/source/internal/queue"
)

// TestRuntimeTerminalProductSessionPostgresRedisConvergesWhenConfigured is an
// isolated live proof for the product-thread lifecycle. It intentionally uses
// a real PostgreSQL database plus the configured Redis/Tair lease authority;
// the broad integration package has unrelated compile drift and cannot serve
// as this release-gate evidence.
func TestRuntimeTerminalProductSessionPostgresRedisConvergesWhenConfigured(t *testing.T) {
	if strings.TrimSpace(os.Getenv("HUAHUO_RUNTIME_V1_LIVE_POSTGRES")) != "1" {
		t.Skip("HUAHUO_RUNTIME_V1_LIVE_POSTGRES=1 is required")
	}
	dsn := strings.TrimSpace(os.Getenv("HUAHUO_TEST_DATABASE_DSN"))
	if dsn == "" {
		t.Skip("HUAHUO_TEST_DATABASE_DSN is required")
	}
	redisAddr, redisPassword, redisDB := runtimeV1TerminalRedisSettings(t)

	database, err := persistence.NewDatabase(config.Settings{
		DatabaseDSN: dsn,
		Database: config.DatabaseSettings{
			PoolMinConns: 1, PoolMaxConns: 8, ConnectTimeoutSeconds: 5, StatementTimeoutSeconds: 10,
		},
	})
	if err != nil || database.Disabled || database.Pool == nil {
		if database != nil {
			database.Close()
		}
		t.Fatalf("open live PostgreSQL: %v", err)
	}
	t.Cleanup(func() { database.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	locks := queue.NewRedisDistributedLockManager(redisAddr, redisPassword, redisDB)
	t.Cleanup(func() { _ = locks.Close() })
	if _, err := locks.Health(ctx); err != nil {
		t.Fatalf("check live Redis/Tair lease authority: %v", err)
	}

	suffix := strconv.FormatInt(time.Now().UTC().UnixNano(), 36)
	fixture := runtimeV1TerminalSessionFixture{
		tenantID:       "runtime_v1_terminal_tenant_" + suffix,
		userID:         "runtime_v1_terminal_user_" + suffix,
		workspaceID:    "runtime_v1_terminal_workspace_" + suffix,
		threadID:       "runtime_v1_terminal_thread_" + suffix,
		bindingID:      "runtime_v1_terminal_binding_" + suffix,
		runID:          "runtime_v1_terminal_run_" + suffix,
		hostID:         "runtime_v1_terminal_host_" + suffix,
		dispatchID:     "runtime_v1_terminal_dispatch_" + suffix,
		queueID:        "runtime_events:runtime_v1_terminal_" + suffix,
		capabilityHash: "runtime_v1_terminal_capability_" + suffix,
	}
	var runLease, sessionLease queue.DistributedLease
	t.Cleanup(func() {
		if runLease.Key != "" {
			_ = locks.Release(context.Background(), runLease)
		}
		if sessionLease.Key != "" {
			_ = locks.Release(context.Background(), sessionLease)
		}
		fixture.cleanup(t, database)
	})

	agentRuns := persistence.NewAgentRunRepository(database)
	fixture.seedProductThread(t, ctx, database, agentRuns)

	hosts := NewRuntimeHostRepository(database)
	identity := RuntimeHostIdentity{
		RuntimeHostID: fixture.hostID,
		InstanceID:    "runtime_v1_terminal_instance_" + suffix,
		Environment:   "integration",
	}
	capabilities := runtimeTestCapabilities(fixture.capabilityHash, CanonicalAgentFacingToolCapability("read", "ready"))
	registration := RuntimeHostRegistration{
		Endpoint:             "http://runtime-v1-terminal.invalid",
		RuntimeVersion:       "runtime-v1-live",
		AdapterVersion:       "adapter-v1-live",
		Capabilities:         capabilities,
		MaxActiveRuns:        1,
		MaxProductThreadRuns: 1,
		MaxDetachedTaskRuns:  1,
		SessionStoreID:       "runtime-v1-terminal-session-store-" + suffix,
	}
	if _, err := hosts.RegisterHost(ctx, identity, registration); err != nil {
		t.Fatalf("register live RuntimeHost: %v", err)
	}
	if _, err := hosts.HeartbeatHost(ctx, identity, RuntimeHostHeartbeat{
		Sequence: 1, ObservedAt: time.Now().UTC(), CapabilityHash: capabilities.CapabilityHash, SignatureKeyID: "runtime-v1-terminal-live",
	}); err != nil {
		t.Fatalf("heartbeat live RuntimeHost: %v", err)
	}

	capacity := NewRuntimeCapacityAdmissionService(database)
	capacityReservation, err := capacity.Reserve(ctx, RuntimeCapacityCommand{
		RunID: fixture.runID, SnapshotVersion: 1, TTL: 2 * time.Minute,
		Dimensions: runtimeV1TerminalCapacityDimensions(fixture, suffix),
	})
	if err != nil {
		t.Fatalf("reserve live capacity: %v", err)
	}

	sessions := NewRuntimeSessionAdmissionService(database, locks)
	scheduler := NewRuntimeSchedulerWithAdmissions(hosts, locks, sessions, capacity)
	sessionKey := ProductSessionAdmissionKey{
		TenantID: fixture.tenantID, ThreadID: fixture.threadID, AgentProfile: "runtime_v1_terminal_agent",
		ContextGeneration: 1, SessionGeneration: 1,
	}
	sessionBinding := ProductSessionHostBinding{
		TenantID: fixture.tenantID, ThreadID: fixture.threadID, AgentProfile: sessionKey.AgentProfile,
		ContextGeneration: sessionKey.ContextGeneration, SessionGeneration: sessionKey.SessionGeneration,
	}
	handle, err := scheduler.Reserve(ctx, ScheduleCommand{
		RunID: fixture.runID, OwnerInstanceID: "runtime-v1-terminal-dispatch-owner", ExecutionScope: "product_thread",
		CapabilityHash: capabilities.CapabilityHash, RuntimeVersion: registration.RuntimeVersion, AdapterVersion: registration.AdapterVersion,
		RequiredTools: []string{"read"}, SessionBinding: sessionBinding,
		SessionAdmission: ProductSessionAdmissionCommand{
			Key: sessionKey, BindingID: fixture.bindingID, RunID: fixture.runID,
			OwnerInstanceID: "runtime-v1-terminal-dispatch-owner", TTL: 2 * time.Minute,
		},
		CapacityReservation: capacityReservation, ReservationTTL: 2 * time.Minute,
	})
	if err != nil {
		t.Fatalf("reserve product-thread Host Slot and Session Admission: %v", err)
	}
	runLease = handle.Lease
	if handle.SessionAdmission == nil {
		t.Fatal("product-thread reservation did not create a Session Admission")
	}
	sessionLease = handle.SessionAdmission.Lease
	fixture.assertReserved(t, ctx, database, handle)

	if _, err := hosts.CreateDispatchWithRuntimeRunRecord(ctx, RuntimeDispatch{
		DispatchID: fixture.dispatchID, RunID: fixture.runID, ReservationID: handle.Reservation.ReservationID,
		CapacityReservationID: capacityReservation.ReservationID, CapacityReservedVersion: capacityReservation.Version,
		RuntimeHostID: fixture.hostID, DispatchAttempt: 1, PlanVersion: 1, FencingToken: handle.Reservation.FencingToken,
		RunTicketJTIHash: "sha256:runtime-v1-terminal", TicketExpiresAt: time.Now().UTC().Add(time.Minute),
		InputManifestHash: "sha256:runtime-v1-terminal", OwnerInstanceID: handle.Reservation.OwnerInstanceID,
		LeaseTokenHash: handle.Reservation.LeaseTokenHash, LeaseExpiresAt: handle.Reservation.ExpiresAt,
	}, fixture.runtimeRunRecord()); err != nil {
		t.Fatalf("create dispatch: %v", err)
	}
	createdRecord, err := hosts.GetRuntimeRunRecordV1(ctx, fixture.runID)
	if err != nil || createdRecord.Status != "created" || createdRecord.AgentRunID != fixture.runID ||
		createdRecord.RuntimeHostID != fixture.hostID || createdRecord.ReservationID != handle.Reservation.ReservationID ||
		createdRecord.FencingToken != handle.Reservation.FencingToken || createdRecord.DispatchID != fixture.dispatchID {
		t.Fatalf("created V1 runtime run record=%+v err=%v", createdRecord, err)
	}
	if err := scheduler.BindDispatch(ctx, handle, fixture.dispatchID); err != nil {
		t.Fatalf("bind Session Admission to dispatch: %v", err)
	}
	if err := scheduler.Accept(ctx, handle, fixture.dispatchID, "runtime-v1-terminal-request-"+suffix); err != nil {
		t.Fatalf("accept dispatch: %v", err)
	}
	fixture.assertAccepted(t, ctx, database, handle, capacityReservation)

	if err := hosts.AppendRunEventAndAdvanceCursor(ctx, RuntimeHostRunEvent{
		EventID: "runtime-v1-terminal-event-" + suffix, RunID: fixture.runID, DispatchID: fixture.dispatchID,
		RuntimeHostID: fixture.hostID, SourceSequence: 1, EventType: "run.succeeded", Visibility: "internal",
		SafePayload: map[string]any{"status": "succeeded"},
	}, 0); err != nil {
		t.Fatalf("append terminal Runtime event: %v", err)
	}

	queueRepository := persistence.NewQueueRepository(database)
	queueRepository.Enqueue(map[string]any{
		"queueId": fixture.queueID, "queueName": "runtime_events", "taskType": "runtime_event_ingest", "taskId": fixture.runID,
		"dedupeKey": "runtime-v1-terminal:" + fixture.runID,
	})
	if status, err := fixture.queueStatus(ctx, database); err != nil || status != "pending" {
		t.Fatalf("enqueue terminal queue status=%q err=%v", status, err)
	}
	if _, proof, err := queueRepository.LeaseByID(ctx, fixture.queueID, "runtime-v1-terminal-event-worker", 2*time.Minute); err != nil {
		t.Fatalf("lease terminal queue: %v", err)
	} else {
		fixture.queueProof = proof
	}
	// RuntimeEventWorker normally marks a leased Queue attempt running before
	// convergence. The durable finalizer deliberately accepts either exact
	// active state so it can close the crash boundary before MarkRunning.
	if status, err := fixture.queueStatus(ctx, database); err != nil || status != "leased" {
		t.Fatalf("lease terminal queue status=%q err=%v", status, err)
	}

	converger := NewRuntimeTerminalConverger(database, hosts, sessions, capacity, queueRepository, agentRuns)
	productProjectionCalls := 0
	// Product projection is outside B5's all-effects transaction. Its durable
	// checkpoint is exercised here so this test can prove the B5 transaction
	// that follows it without claiming product-workflow coverage.
	converger.ProjectProduct = func(context.Context, TerminalConvergenceCommand, string) error {
		productProjectionCalls++
		return nil
	}
	command := fixture.terminalCommand(handle, capacityReservation)
	badProof := command.QueueProof
	badProof.LeaseExpiresAt = badProof.LeaseExpiresAt.Add(time.Millisecond)
	command.QueueProof = badProof
	if result, err := converger.Converge(ctx, command); err == nil || result.Complete {
		t.Fatalf("terminal queue-proof rollback result=%+v err=%v", result, err)
	}
	fixture.assertTerminalRolledBack(t, ctx, database, handle, capacityReservation)
	if productProjectionCalls != 1 {
		t.Fatalf("product projection calls after rollback=%d, want 1", productProjectionCalls)
	}

	command.QueueProof = fixture.queueProof
	if result, err := converger.Converge(ctx, command); err != nil || !result.Complete {
		t.Fatalf("terminal convergence result=%+v err=%v", result, err)
	}
	fixture.assertTerminalConverged(t, ctx, database, handle, capacityReservation)
	if err := locks.AssertActiveLease(ctx, sessionLease, 0); err == nil {
		t.Fatal("terminal Session Admission cleanup left the Redis/Tair lease active")
	}
	if productProjectionCalls != 1 {
		t.Fatalf("terminal replayed completed product projection calls=%d", productProjectionCalls)
	}
	if result, err := converger.Converge(ctx, command); err != nil || !result.Complete {
		t.Fatalf("terminal convergence replay result=%+v err=%v", result, err)
	}
	fixture.assertTerminalConverged(t, ctx, database, handle, capacityReservation)
}

type runtimeV1TerminalSessionFixture struct {
	tenantID       string
	userID         string
	workspaceID    string
	threadID       string
	bindingID      string
	runID          string
	hostID         string
	dispatchID     string
	queueID        string
	capabilityHash string
	queueProof     persistence.QueueLeaseProof
}

func runtimeV1TerminalRedisSettings(t *testing.T) (string, string, int) {
	t.Helper()
	addr := strings.TrimSpace(os.Getenv("HUAHUO_RUNTIME_V1_LIVE_REDIS_ADDR"))
	if addr == "" {
		addr = strings.TrimSpace(os.Getenv("HUAHUO_REDIS_ADDR"))
	}
	if addr == "" {
		t.Skip("HUAHUO_RUNTIME_V1_LIVE_REDIS_ADDR or HUAHUO_REDIS_ADDR is required")
	}
	password := os.Getenv("HUAHUO_RUNTIME_V1_LIVE_REDIS_PASSWORD")
	if password == "" {
		password = os.Getenv("HUAHUO_REDIS_PASSWORD")
	}
	dbValue := strings.TrimSpace(os.Getenv("HUAHUO_RUNTIME_V1_LIVE_REDIS_DB"))
	if dbValue == "" {
		dbValue = strings.TrimSpace(os.Getenv("HUAHUO_REDIS_DB"))
	}
	if dbValue == "" {
		return addr, password, 0
	}
	db, err := strconv.Atoi(dbValue)
	if err != nil || db < 0 {
		t.Fatalf("invalid live Redis/Tair database index")
	}
	return addr, password, db
}

func runtimeV1TerminalCapacityDimensions(fixture runtimeV1TerminalSessionFixture, suffix string) RuntimeCapacityDimensions {
	dimension := func(name string) RuntimeCapacityDimension {
		return RuntimeCapacityDimension{Key: "runtime-v1-terminal-" + suffix + "-" + fixture.runID + "-" + name, Limit: 1, Requested: 1, Version: 1}
	}
	return RuntimeCapacityDimensions{Model: dimension("model"), AuthPool: dimension("auth"), Tool: dimension("tool"), Tenant: dimension("tenant"), User: dimension("user")}
}

func (f runtimeV1TerminalSessionFixture) seedProductThread(t *testing.T, ctx context.Context, database *persistence.Database, agentRuns *persistence.AgentRunRepository) {
	t.Helper()
	if _, err := database.Pool.Exec(ctx, `insert into users(user_id,phone_hash,phone_ciphertext,phone_masked,status) values($1,$2,$3,'138****0000','normal')`, f.userID, "runtime_v1_terminal_hash_"+f.userID, "runtime_v1_terminal_cipher_"+f.userID); err != nil {
		t.Fatalf("seed terminal user: %v", err)
	}
	if _, err := database.Pool.Exec(ctx, `insert into workspaces(workspace_id,tenant_id,user_id,status,meta_version) values($1,$2,$3,'ready','runtime-v1-terminal')`, f.workspaceID, f.tenantID, f.userID); err != nil {
		t.Fatalf("seed terminal Workspace: %v", err)
	}
	if _, err := database.Pool.Exec(ctx, `
insert into chat_threads(thread_id,user_id,workspace_id,active_workspace_id,workspace_binding_version,context_generation,scene,title,status)
values($1,$2,$3,$3,1,1,'work_ai','runtime v1 terminal session','active')`, f.threadID, f.userID, f.workspaceID); err != nil {
		t.Fatalf("seed terminal Thread: %v", err)
	}
	if _, err := database.Pool.Exec(ctx, `
insert into thread_agent_runtime_bindings(binding_id,thread_id,tenant_id,user_id,agent_profile,session_generation,context_generation,openclaw_session_key_ciphertext,openclaw_session_key_hash,workspace_id,manifest_version,agent_hash,status)
values($1,$2,$3,$4,'runtime_v1_terminal_agent',1,1,$5,$6,$7,'runtime-v1-terminal','runtime-v1-terminal','active')`,
		f.bindingID, f.threadID, f.tenantID, f.userID, "runtime_v1_terminal_cipher_"+f.bindingID, "runtime_v1_terminal_session_hash_"+f.bindingID, f.workspaceID); err != nil {
		t.Fatalf("seed terminal runtime binding: %v", err)
	}
	if _, _, err := agentRuns.CreateRun(ctx, persistence.CreateAgentRunCommand{Record: persistence.AgentRunRecord{
		AgentRunID: f.runID, TenantID: f.tenantID, UserID: f.userID, WorkspaceID: f.workspaceID, ThreadID: f.threadID,
		IdempotencyKey: "runtime-v1-terminal:" + f.runID, RequestHash: "runtime-v1-terminal-request:" + f.runID,
		Status: "planning", RoutingMode: "dynamic", WorkspaceVersion: 1, BindingVersion: 1, ContextGeneration: 1,
	}}); err != nil {
		t.Fatalf("create terminal AgentRun: %v", err)
	}
	if err := agentRuns.SavePlan(ctx, persistence.AgentRunPlanRecord{
		AgentRunID: f.runID, PlanVersion: 1, PlanStatus: "executing", AgentRunStatus: "running",
		Plan: map[string]any{
			"taskType": "work_ai_general_chat", "l1AgentProfile": "runtime_v1_terminal_agent",
			"selectedSkillProfiles": []any{}, "selectedKnowledgeRefs": []any{}, "requiredTools": []any{"read"}, "outputContract": map[string]any{},
			"workspaceVersion": 1, "indexVersion": 0, "manifestVersion": "runtime-v1-terminal", "capabilityHash": f.capabilityHash,
		},
	}); err != nil {
		t.Fatalf("save terminal AgentRun plan: %v", err)
	}
}

func (f runtimeV1TerminalSessionFixture) terminalCommand(handle RuntimeReservationLease, capacity RuntimeCapacityReservation) TerminalConvergenceCommand {
	return TerminalConvergenceCommand{
		DispatchID: f.dispatchID, RunID: f.runID, TerminalSourceSequence: 1, TerminalStatus: "succeeded",
		SafeResult: map[string]any{"finalAnswer": "runtime v1 terminal fixture"}, ActualUsage: map[string]any{"modelTokens": 3}, QueueProof: f.queueProof,
		DispatchTerminal: DispatchTerminalCommand{Fence: handle.Fence(), DispatchID: f.dispatchID, TerminalStatus: "succeeded"},
		SessionAdmission: handle.SessionAdmission, SessionRequired: true, CapacityReservation: capacity,
		AgentRunTerminal: &TerminalAgentRunProjection{
			AgentRunStatus: "succeeded", PlanVersion: 1, PlanStatus: "succeeded", PublicResult: map[string]any{"finalAnswer": "runtime v1 terminal fixture"},
			PublicEvent: persistence.AgentRunEvent{AgentRunID: f.runID, EventType: "succeeded", Status: "succeeded", SafeData: map[string]any{"status": "succeeded"}},
		},
	}
}

func (f runtimeV1TerminalSessionFixture) runtimeRunRecord() RuntimeRunRecordV1 {
	return RuntimeRunRecordV1{
		RunID:                         f.runID,
		AgentRunID:                    f.runID,
		TenantID:                      f.tenantID,
		UserID:                        f.userID,
		ThreadID:                      f.threadID,
		WorkspaceID:                   f.workspaceID,
		WorkspaceVersion:              1,
		IndexVersion:                  0,
		ThreadWorkspaceBindingVersion: 1,
		ContextGeneration:             1,
		SessionGeneration:             1,
		ExecutionScope:                string(ScopeProductThread),
		PlanVersion:                   1,
		RuntimeConfigID:               "runtime-v1-terminal",
		RuntimeConfigVersion:          "v1",
		CapabilityHash:                f.capabilityHash,
		Status:                        "created",
	}
}

func (f runtimeV1TerminalSessionFixture) assertReserved(t *testing.T, ctx context.Context, database *persistence.Database, handle RuntimeReservationLease) {
	t.Helper()
	var reservationState, admissionState string
	if err := database.Pool.QueryRow(ctx, `select state from runtime_slot_reservations where reservation_id=$1`, handle.Reservation.ReservationID).Scan(&reservationState); err != nil || reservationState != "reserved" {
		t.Fatalf("reserved Slot state=%q err=%v", reservationState, err)
	}
	if err := database.Pool.QueryRow(ctx, `select state from runtime_session_admissions where run_id=$1`, f.runID).Scan(&admissionState); err != nil || admissionState != "reservation_bound" {
		t.Fatalf("reserved Session Admission state=%q err=%v", admissionState, err)
	}
	f.assertHostCounters(t, ctx, database, 1, 1, 0, 0)
}

func (f runtimeV1TerminalSessionFixture) assertAccepted(t *testing.T, ctx context.Context, database *persistence.Database, handle RuntimeReservationLease, capacity RuntimeCapacityReservation) {
	t.Helper()
	var reservationState, capacityState, admissionState string
	if err := database.Pool.QueryRow(ctx, `select state from runtime_slot_reservations where reservation_id=$1`, handle.Reservation.ReservationID).Scan(&reservationState); err != nil || reservationState != "accepted" {
		t.Fatalf("accepted Slot state=%q err=%v", reservationState, err)
	}
	if err := database.Pool.QueryRow(ctx, `select state from runtime_capacity_reservations where capacity_reservation_id=$1`, capacity.ReservationID).Scan(&capacityState); err != nil || capacityState != "accepted" {
		t.Fatalf("accepted capacity state=%q err=%v", capacityState, err)
	}
	if err := database.Pool.QueryRow(ctx, `select state from runtime_session_admissions where run_id=$1`, f.runID).Scan(&admissionState); err != nil || admissionState != "dispatch_bound" {
		t.Fatalf("accepted Session Admission state=%q err=%v", admissionState, err)
	}
	f.assertHostCounters(t, ctx, database, 0, 0, 1, 1)
	var runtimeStatus, requestID string
	if err := database.Pool.QueryRow(ctx, `select status,coalesce(runtime_request_id,'') from runtime_run_records where run_id=$1 and agent_run_id=$1`, f.runID).Scan(&runtimeStatus, &requestID); err != nil || runtimeStatus != "running" || requestID == "" {
		t.Fatalf("accepted runtime run record status=%q request=%q err=%v", runtimeStatus, requestID, err)
	}
}

func (f runtimeV1TerminalSessionFixture) assertTerminalRolledBack(t *testing.T, ctx context.Context, database *persistence.Database, handle RuntimeReservationLease, capacity RuntimeCapacityReservation) {
	t.Helper()
	for _, check := range []struct {
		query string
		arg   string
		want  string
	}{
		{`select status from agent_runs where agent_run_id=$1`, f.runID, "running"},
		{`select status from agent_run_plans where agent_run_id=$1 and plan_version=1`, f.runID, "executing"},
		{`select state from runtime_slot_reservations where reservation_id=$1`, handle.Reservation.ReservationID, "accepted"},
		{`select state from runtime_capacity_reservations where capacity_reservation_id=$1`, capacity.ReservationID, "accepted"},
		{`select state from runtime_run_dispatches where dispatch_id=$1`, f.dispatchID, "accepted"},
		{`select state from runtime_session_admissions where run_id=$1`, f.runID, "dispatch_bound"},
		{`select status from task_queue_records where queue_id=$1`, f.queueID, "leased"},
	} {
		var got string
		if err := database.Pool.QueryRow(ctx, check.query, check.arg).Scan(&got); err != nil || got != check.want {
			t.Fatalf("terminal rollback query=%q got=%q want=%q err=%v", check.query, got, check.want, err)
		}
	}
	f.assertHostCounters(t, ctx, database, 0, 0, 1, 1)
}

func (f runtimeV1TerminalSessionFixture) assertTerminalConverged(t *testing.T, ctx context.Context, database *persistence.Database, handle RuntimeReservationLease, capacity RuntimeCapacityReservation) {
	t.Helper()
	for _, check := range []struct {
		query string
		arg   string
		want  string
	}{
		{`select status from agent_runs where agent_run_id=$1`, f.runID, "succeeded"},
		{`select status from agent_run_plans where agent_run_id=$1 and plan_version=1`, f.runID, "succeeded"},
		{`select state from runtime_slot_reservations where reservation_id=$1`, handle.Reservation.ReservationID, "released"},
		{`select state from runtime_capacity_reservations where capacity_reservation_id=$1`, capacity.ReservationID, "released"},
		{`select state from runtime_run_dispatches where dispatch_id=$1`, f.dispatchID, "succeeded"},
		{`select state from runtime_session_admissions where run_id=$1`, f.runID, "released"},
		{`select status from runtime_session_terminal_cleanup_outbox where convergence_id=$1`, "terminal:" + f.dispatchID + ":1", "succeeded"},
		{`select status from task_queue_records where queue_id=$1`, f.queueID, "succeeded"},
	} {
		var got string
		if err := database.Pool.QueryRow(ctx, check.query, check.arg).Scan(&got); err != nil || got != check.want {
			t.Fatalf("terminal convergence query=%q got=%q want=%q err=%v", check.query, got, check.want, err)
		}
	}
	var events int
	if err := database.Pool.QueryRow(ctx, `select count(*) from runtime_run_events where run_id=$1`, f.runID).Scan(&events); err != nil || events != 2 {
		t.Fatalf("terminal Runtime event count=%d err=%v", events, err)
	}
	f.assertHostCounters(t, ctx, database, 0, 0, 0, 0)
	var runtimeStatus string
	var lastEventSequence int64
	if err := database.Pool.QueryRow(ctx, `select status,last_event_sequence from runtime_run_records where run_id=$1 and agent_run_id=$1`, f.runID).Scan(&runtimeStatus, &lastEventSequence); err != nil || runtimeStatus != "succeeded" || lastEventSequence != 1 {
		t.Fatalf("terminal runtime run record status=%q sequence=%d err=%v", runtimeStatus, lastEventSequence, err)
	}
}

func (f runtimeV1TerminalSessionFixture) assertHostCounters(t *testing.T, ctx context.Context, database *persistence.Database, reserved, reservedProduct, active, activeProduct int) {
	t.Helper()
	var gotReserved, gotReservedProduct, gotActive, gotActiveProduct int
	err := database.Pool.QueryRow(ctx, `select reserved_runs,reserved_product_thread_runs,active_runs,active_product_thread_runs from runtime_hosts where runtime_host_id=$1`, f.hostID).Scan(&gotReserved, &gotReservedProduct, &gotActive, &gotActiveProduct)
	if err != nil || gotReserved != reserved || gotReservedProduct != reservedProduct || gotActive != active || gotActiveProduct != activeProduct {
		t.Fatalf("Host counters reserved=%d/%d active=%d/%d want=%d/%d/%d/%d err=%v", gotReserved, gotReservedProduct, gotActive, gotActiveProduct, reserved, reservedProduct, active, activeProduct, err)
	}
}

func (f runtimeV1TerminalSessionFixture) queueStatus(ctx context.Context, database *persistence.Database) (string, error) {
	var status string
	err := database.Pool.QueryRow(ctx, `select status from task_queue_records where queue_id=$1`, f.queueID).Scan(&status)
	return status, err
}

func (f runtimeV1TerminalSessionFixture) cleanup(t *testing.T, database *persistence.Database) {
	t.Helper()
	if database == nil || database.Pool == nil {
		t.Error("runtime_v1 terminal fixture cleanup PostgreSQL is unavailable")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	convergenceID := "terminal:" + f.dispatchID + ":1"
	for _, statement := range []struct {
		name  string
		query string
		args  []any
	}{
		{"terminal cleanup outbox", `delete from runtime_session_terminal_cleanup_outbox where convergence_id=$1`, []any{convergenceID}},
		{"admission cleanup outbox", `delete from runtime_session_admission_cleanup_outbox where run_id=$1`, []any{f.runID}},
		{"terminal projection ledger", `delete from runtime_terminal_product_projections where run_id=$1`, []any{f.runID}},
		{"terminal recovery lineage", `delete from runtime_terminal_convergence_recovery_queue_lineage where convergence_id=$1`, []any{convergenceID}},
		{"terminal convergence", `delete from runtime_terminal_convergences where convergence_id=$1`, []any{convergenceID}},
		{"runtime events", `delete from runtime_run_events where run_id=$1`, []any{f.runID}},
		{"runtime run record", `delete from runtime_run_records where run_id=$1`, []any{f.runID}},
		{"session admission", `delete from runtime_session_admissions where run_id=$1`, []any{f.runID}},
		{"dispatch", `delete from runtime_run_dispatches where dispatch_id=$1`, []any{f.dispatchID}},
		{"Slot reservation", `delete from runtime_slot_reservations where run_id=$1`, []any{f.runID}},
		{"capacity reservation", `delete from runtime_capacity_reservations where run_id=$1`, []any{f.runID}},
		{"queue", `delete from task_queue_records where queue_id=$1 or task_id=$2`, []any{f.queueID, f.runID}},
		{"AgentRun plan", `delete from agent_run_plans where agent_run_id=$1`, []any{f.runID}},
		{"AgentRun", `delete from agent_runs where agent_run_id=$1`, []any{f.runID}},
		{"runtime binding", `delete from thread_agent_runtime_bindings where binding_id=$1`, []any{f.bindingID}},
		{"thread", `delete from chat_threads where thread_id=$1`, []any{f.threadID}},
		{"Host heartbeats", `delete from runtime_host_heartbeats where runtime_host_id=$1`, []any{f.hostID}},
		{"RuntimeHost", `delete from runtime_hosts where runtime_host_id=$1`, []any{f.hostID}},
		{"Workspace", `delete from workspaces where workspace_id=$1`, []any{f.workspaceID}},
		{"user", `delete from users where user_id=$1`, []any{f.userID}},
	} {
		if _, err := database.Pool.Exec(ctx, statement.query, statement.args...); err != nil {
			t.Errorf("runtime_v1 terminal fixture cleanup %s: %v", statement.name, err)
		}
	}
}
