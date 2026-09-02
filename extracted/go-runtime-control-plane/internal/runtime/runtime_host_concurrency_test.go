package runtime

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"huahuoai/backend/source/internal/config"
	"huahuoai/backend/source/internal/persistence"
	"huahuoai/backend/source/internal/queue"
)

func TestRuntimeHostFromMapDecodesStructuredCapabilitySnapshot(t *testing.T) {
	host, err := runtimeHostFromMap(map[string]any{
		"runtime_host_id": "host-structured-capabilities",
		"capability_snapshot": map[string]any{
			"capabilityHash": "cap-v1",
			"tools": []any{
				map[string]any{
					"name": "read", "status": "ready", "source": RuntimeToolSourceOpenClawCore,
					"pluginId": RuntimeCoreToolsPluginID, "pluginVersion": RuntimeCoreToolsPluginVersion,
					"schemaId": "openclaw.core.read.v2026.6.2", "schemaHash": "sha256:134f19bcabe3e29d63c5cebb38f1d2556759fd08adad6bc90a4b4d3cd1fb8441",
				},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !runtimeHostHasTools(host, []string{"read"}) {
		t.Fatalf("structured capability snapshot did not expose required tools: %+v", host.Capabilities)
	}
}

func TestRuntimeHostWithinTargetUsesSixRunSchedulerWaterlineForEightSlotHost(t *testing.T) {
	host := RuntimeHost{
		MaxActiveRuns:             8,
		MaxProductThreadRuns:      8,
		MaxDetachedTaskRuns:       8,
		ActiveRuns:                5,
		ActiveProductThreadRuns:   5,
		ActiveDetachedTaskRuns:    0,
		ReservedRuns:              0,
		ReservedProductThreadRuns: 0,
		ReservedDetachedTaskRuns:  0,
	}
	if !runtimeHostWithinTarget(host, "product_thread") {
		t.Fatal("five active-plus-reserved runs must remain below the eight-slot Host scheduler waterline")
	}

	host.ReservedRuns = 1
	host.ReservedProductThreadRuns = 1
	if runtimeHostWithinTarget(host, "product_thread") {
		t.Fatal("six active-plus-reserved runs must reach the eight-slot Host scheduler waterline")
	}
}

func TestAppendRunEventRejectsDispatchRunAndHostBindingMismatch(t *testing.T) {
	ctx := context.Background()
	const dispatchID = "dispatch-event-binding"
	for name, event := range map[string]RuntimeHostRunEvent{
		"run": {
			EventID: "event-run-binding-mismatch", RunID: "run-other", DispatchID: dispatchID, RuntimeHostID: "host-bound",
			SourceSequence: 1, EventType: "run.started", Visibility: "internal", SafePayload: map[string]any{"status": "running"},
		},
		"host": {
			EventID: "event-host-binding-mismatch", RunID: "run-bound", DispatchID: dispatchID, RuntimeHostID: "host-other",
			SourceSequence: 1, EventType: "run.started", Visibility: "internal", SafePayload: map[string]any{"status": "running"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			repository := NewRuntimeHostRepository(nil)
			repository.dispatches[dispatchID] = RuntimeDispatch{DispatchID: dispatchID, RunID: "run-bound", RuntimeHostID: "host-bound", State: "accepted"}
			if err := repository.AppendRunEventAndAdvanceCursor(ctx, event, 0); err == nil || err.Error() != "RUNTIME_EVENT_GAP" {
				t.Fatalf("binding mismatch error=%v, want RUNTIME_EVENT_GAP", err)
			}
			cursor, err := repository.GetDispatchEventCursor(ctx, dispatchID)
			if err != nil || cursor.LastSequence != 0 {
				t.Fatalf("binding mismatch advanced cursor=%+v err=%v", cursor, err)
			}
			for _, runID := range []string{"run-bound", event.RunID} {
				events, err := repository.ListRunEvents(ctx, runID, 0, 10)
				if err != nil || len(events) != 0 {
					t.Fatalf("binding mismatch persisted run=%q events=%+v err=%v", runID, events, err)
				}
			}
		})
	}
}

func TestAppendRunEventRejectsLateToolStartAfterDispatchOrRunTerminal(t *testing.T) {
	ctx := context.Background()
	const (
		dispatchID = "dispatch-terminal-tool-start"
		runID      = "run-terminal-tool-start"
		hostID     = "host-terminal-tool-start"
	)
	for name, terminal := range map[string]struct {
		dispatchState string
		runState      string
	}{
		"dispatch": {dispatchState: "succeeded", runState: "running"},
		"run":      {dispatchState: "accepted", runState: "aborted"},
	} {
		t.Run(name, func(t *testing.T) {
			repository := NewRuntimeHostRepository(nil)
			repository.dispatches[dispatchID] = RuntimeDispatch{DispatchID: dispatchID, RunID: runID, RuntimeHostID: hostID, State: terminal.dispatchState}
			repository.runtimeRunRecords[runID] = RuntimeRunRecordV1{RunID: runID, WorkspaceVersion: 1, Status: terminal.runState}
			event := RuntimeHostRunEvent{
				EventID: "event-terminal-tool-start-" + name, RunID: runID, DispatchID: dispatchID, RuntimeHostID: hostID,
				SourceSequence: 1, EventType: "tool.call.started", Visibility: "admin_safe", SafePayload: runtimeToolAuditTestPayload("started"),
			}
			if err := repository.AppendRunEventAndAdvanceCursor(ctx, event, 0); err == nil || err.Error() != "RUNTIME_EVENT_GAP" {
				t.Fatalf("late tool start error=%v, want RUNTIME_EVENT_GAP", err)
			}
			cursor, err := repository.GetDispatchEventCursor(ctx, dispatchID)
			if err != nil || cursor.LastSequence != 0 {
				t.Fatalf("late tool start advanced cursor=%+v err=%v", cursor, err)
			}
			if events := repository.events[runID]; len(events) != 0 || len(repository.toolInvocations) != 0 {
				t.Fatalf("late tool start mutated state events=%+v invocations=%+v", events, repository.toolInvocations)
			}
		})
	}
}

func TestAppendRunEventConcurrentDuplicateToolReceiptIsIdempotent(t *testing.T) {
	ctx := context.Background()
	const (
		dispatchID = "dispatch-concurrent-tool-receipt"
		runID      = "run-concurrent-tool-receipt"
		hostID     = "host-concurrent-tool-receipt"
	)
	repository := NewRuntimeHostRepository(nil)
	repository.dispatches[dispatchID] = RuntimeDispatch{DispatchID: dispatchID, RunID: runID, RuntimeHostID: hostID, State: "accepted"}
	repository.runtimeRunRecords[runID] = RuntimeRunRecordV1{RunID: runID, WorkspaceVersion: 7, Status: "running"}
	event := RuntimeHostRunEvent{
		EventID: "event-concurrent-tool-receipt", RunID: runID, DispatchID: dispatchID, RuntimeHostID: hostID,
		SourceSequence: 1, EventType: "tool.call.started", Visibility: "admin_safe", SafePayload: runtimeToolAuditTestPayload("started"),
	}

	const callers = 16
	start := make(chan struct{})
	errors := make(chan error, callers)
	var wait sync.WaitGroup
	for range callers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			errors <- repository.AppendRunEventAndAdvanceCursor(ctx, event, 0)
		}()
	}
	close(start)
	wait.Wait()
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatalf("concurrent duplicate error=%v", err)
		}
	}

	cursor, err := repository.GetDispatchEventCursor(ctx, dispatchID)
	if err != nil || cursor.LastSequence != 1 {
		t.Fatalf("concurrent duplicate cursor=%+v err=%v", cursor, err)
	}
	events, err := repository.ListRunEvents(ctx, runID, 0, 10)
	if err != nil || len(events) != 1 {
		t.Fatalf("concurrent duplicate events=%+v err=%v", events, err)
	}
	invocationID := runtimeToolInvocationID(runID, runtimeToolAuditTestHash("a"))
	stored, ok := repository.toolInvocations[invocationID]
	if !ok || stored.Status != "started" || stored.WorkspaceVersion != 7 || stored.ToolName != "workspace_search" {
		t.Fatalf("concurrent duplicate invocation=%+v found=%t", stored, ok)
	}
}

// TestRuntimeHostPostgresLockOrderIsCanonical is a source-level regression for
// the durable lock protocol. The affected rows are held across the transaction,
// so changing one path back to Dispatch -> Slot would reintroduce a deadlock
// against accept/renew even when the memory repository tests still pass.
func TestRuntimeHostPostgresLockOrderIsCanonical(t *testing.T) {
	testCases := []struct {
		name      string
		file      string
		function  string
		lockSteps []string
	}{
		{
			name:     "renew",
			file:     "runtime_host_concurrency.go",
			function: "func (r *RuntimeHostRepository) RenewReservation",
			lockSteps: []string{
				"from runtime_slot_reservations",
				"update runtime_run_dispatches",
			},
		},
		{
			name:     "accept",
			file:     "runtime_host_concurrency.go",
			function: "func (r *RuntimeHostRepository) confirmDispatchAcceptedTx",
			lockSteps: []string{
				"from runtime_slot_reservations",
				"from runtime_run_dispatches",
				"from runtime_hosts",
			},
		},
		{
			name:     "direct terminal",
			file:     "runtime_host_concurrency.go",
			function: "func (r *RuntimeHostRepository) FinalizeDispatchAndReleaseSlot",
			lockSteps: []string{
				"from runtime_slot_reservations",
				"from runtime_run_dispatches",
				"releaseruntimehostcountertx",
			},
		},
		{
			name:     "recovery acceptance",
			file:     "runtime_host_concurrency.go",
			function: "func (r *RuntimeHostRepository) RecoverDispatchAccepted",
			lockSteps: []string{
				"from runtime_slot_reservations",
				"from runtime_run_dispatches",
				"update runtime_hosts",
			},
		},
		{
			name:     "durable terminal converger",
			file:     "runtime_terminal_converger.go",
			function: "func (c *RuntimeTerminalConverger) finalizeDispatchAndSlotTx",
			lockSteps: []string{
				"from runtime_slot_reservations",
				"from runtime_run_dispatches",
				"releaseruntimehostcountertx",
			},
		},
		{
			name:     "legacy tail recovery",
			file:     "runtime_terminal_converger.go",
			function: "func (c *RuntimeTerminalConverger) assertLegacyTailOnlyTerminalFactsTx",
			lockSteps: []string{
				"from runtime_slot_reservations",
				"from runtime_run_dispatches",
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			source, err := os.ReadFile(testCase.file)
			if err != nil {
				t.Fatalf("read source: %v", err)
			}
			body := strings.ToLower(runtimeHostFunctionBody(t, string(source), testCase.function))
			last := -1
			for _, step := range testCase.lockSteps {
				index := strings.Index(body, step)
				if index < 0 {
					t.Fatalf("missing lock step %q", step)
				}
				if index <= last {
					t.Fatalf("non-canonical lock order: %q appears before prior step in %s", step, testCase.function)
				}
				last = index
			}
		})
	}
}

func runtimeHostFunctionBody(t *testing.T, source, signature string) string {
	t.Helper()
	start := strings.Index(source, signature)
	if start < 0 {
		t.Fatalf("missing function %q", signature)
	}
	rest := source[start+len(signature):]
	if end := strings.Index(rest, "\nfunc "); end >= 0 {
		return rest[:end]
	}
	return rest
}

func TestFinalizeDispatchRejectsExpiredRecoveryOwnerWithoutReleasingSlot(t *testing.T) {
	ctx := context.Background()
	repository := NewRuntimeHostRepository(nil)
	identity := RuntimeHostIdentity{RuntimeHostID: "host-recovery-terminal-fence", InstanceID: "instance-recovery-terminal-fence", Environment: "test"}
	capabilities := runtimeTestCapabilities("cap-recovery-terminal-fence", CanonicalAgentFacingToolCapability("read", "ready"))
	if _, err := repository.RegisterHost(ctx, identity, RuntimeHostRegistration{
		Endpoint: "http://host-recovery-terminal-fence", Capabilities: capabilities, MaxActiveRuns: 2,
	}); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if _, err := repository.HeartbeatHost(ctx, identity, RuntimeHostHeartbeat{
		Sequence: 1, ObservedAt: now, CapabilityHash: capabilities.CapabilityHash, SignatureKeyID: "test-key",
	}); err != nil {
		t.Fatal(err)
	}
	reservation, _, err := repository.TryReserveSlot(ctx, AtomicReservationCommand{
		ReservationID: "reservation-recovery-terminal-fence", RunID: "run-recovery-terminal-fence",
		AffinityRuntimeHostID: identity.RuntimeHostID, OwnerInstanceID: "worker-dispatch-owner",
		ExecutionScope: "detached_task", CapabilityHash: capabilities.CapabilityHash, RequiredTools: []string{"read"},
		LeaseTokenHash: "sha256:dispatch-owner", FencingToken: 7, ExpiresAt: now.Add(time.Minute), HeartbeatAfter: now.Add(-time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	dispatch, err := repository.CreateDispatch(ctx, RuntimeDispatch{
		DispatchID: "dispatch-recovery-terminal-fence", RunID: reservation.RunID, ReservationID: reservation.ReservationID,
		RuntimeHostID: reservation.RuntimeHostID, CapacityReservationID: "capacity-recovery-terminal-fence", CapacityReservedVersion: 1,
		DispatchAttempt: 1, PlanVersion: 1, FencingToken: reservation.FencingToken, RunTicketJTIHash: "sha256:recovery-terminal-fence",
		TicketExpiresAt: now.Add(time.Minute), InputManifestHash: "sha256:recovery-terminal-fence", OwnerInstanceID: reservation.OwnerInstanceID,
		LeaseTokenHash: reservation.LeaseTokenHash, LeaseExpiresAt: reservation.ExpiresAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	staleClaim := DispatchRecoveryClaim{
		DispatchID: dispatch.DispatchID, OwnerInstanceID: "worker-recovery-stale", FencingToken: 41,
		ExpiresAt: now.Add(-time.Second), ExpectedState: dispatch.State, ExpectedVersion: dispatch.Version,
	}
	if err := repository.ClaimDispatchRecovery(ctx, staleClaim); err != nil {
		t.Fatal(err)
	}
	fence := ReservationFence{
		ReservationID: reservation.ReservationID, RuntimeHostID: reservation.RuntimeHostID, OwnerInstanceID: reservation.OwnerInstanceID,
		LeaseTokenHash: reservation.LeaseTokenHash, FencingToken: reservation.FencingToken,
	}
	if err := repository.FinalizeDispatchAndReleaseSlot(ctx, DispatchTerminalCommand{
		Fence: fence, DispatchID: dispatch.DispatchID, TerminalStatus: "orphaned", ErrorCode: "RUNTIME_RUN_STALLED", RecoveryClaim: &staleClaim,
	}); err == nil || err.Error() != "STALE_FENCING_TOKEN" {
		t.Fatalf("expired recovery owner finalized dispatch: %v", err)
	}
	stillReserved, err := repository.GetReservation(ctx, reservation.ReservationID)
	if err != nil || stillReserved.State != "reserved" {
		t.Fatalf("expired recovery owner released reservation: reservation=%+v err=%v", stillReserved, err)
	}
	stillRecovering, err := repository.GetDispatch(ctx, dispatch.DispatchID)
	if err != nil || stillRecovering.State != "recovering" {
		t.Fatalf("expired recovery owner changed dispatch: dispatch=%+v err=%v", stillRecovering, err)
	}
	host, err := repository.GetHost(ctx, identity.RuntimeHostID)
	if err != nil || host.ReservedRuns != 1 || host.ReservedDetachedTaskRuns != 1 {
		t.Fatalf("expired recovery owner changed counters: host=%+v err=%v", host, err)
	}

	currentClaim := DispatchRecoveryClaim{
		DispatchID: dispatch.DispatchID, OwnerInstanceID: "worker-recovery-current", FencingToken: 42,
		ExpiresAt: time.Now().UTC().Add(time.Minute), ExpectedState: stillRecovering.State, ExpectedVersion: stillRecovering.Version,
	}
	if err := repository.ClaimDispatchRecovery(ctx, currentClaim); err != nil {
		t.Fatal(err)
	}
	if err := repository.FinalizeDispatchAndReleaseSlot(ctx, DispatchTerminalCommand{
		Fence: fence, DispatchID: dispatch.DispatchID, TerminalStatus: "orphaned", ErrorCode: "RUNTIME_RUN_STALLED", RecoveryClaim: &currentClaim,
	}); err != nil {
		t.Fatal(err)
	}
	finalReservation, err := repository.GetReservation(ctx, reservation.ReservationID)
	if err != nil || finalReservation.State != "released" {
		t.Fatalf("current recovery owner did not release reservation: reservation=%+v err=%v", finalReservation, err)
	}
	finalDispatch, err := repository.GetDispatch(ctx, dispatch.DispatchID)
	if err != nil || finalDispatch.State != "orphaned" {
		t.Fatalf("current recovery owner did not terminalize dispatch: dispatch=%+v err=%v", finalDispatch, err)
	}
	host, err = repository.GetHost(ctx, identity.RuntimeHostID)
	if err != nil || host.ReservedRuns != 0 || host.ReservedDetachedTaskRuns != 0 || host.ActiveRuns != 0 || host.ActiveDetachedTaskRuns != 0 {
		t.Fatalf("current recovery owner did not release counters: host=%+v err=%v", host, err)
	}
}

func TestFinalizeDispatchPostgresFencesExpiredRecoveryOwnerWhenConfigured(t *testing.T) {
	if strings.TrimSpace(os.Getenv("HUAHUO_RUNTIME_V1_LIVE_POSTGRES")) != "1" {
		t.Skip("HUAHUO_RUNTIME_V1_LIVE_POSTGRES=1 is required")
	}
	dsn := strings.TrimSpace(os.Getenv("HUAHUO_TEST_DATABASE_DSN"))
	if dsn == "" {
		t.Skip("HUAHUO_TEST_DATABASE_DSN is required")
	}
	database, err := persistence.NewDatabase(config.Settings{
		DatabaseDSN: dsn,
		Database: config.DatabaseSettings{
			PoolMinConns: 1, PoolMaxConns: 4, ConnectTimeoutSeconds: 5, StatementTimeoutSeconds: 10,
		},
	})
	if err != nil || database.Disabled || database.Pool == nil {
		if database != nil {
			database.Close()
		}
		t.Fatalf("open live PostgreSQL: %v", err)
	}
	defer database.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	suffix := fmt.Sprintf("%d", time.Now().UTC().UnixNano())
	hostID := "runtime-v1-recovery-fence-host-" + suffix
	runID := "runtime-v1-recovery-fence-run-" + suffix
	reservationID := "runtime-v1-recovery-fence-reservation-" + suffix
	dispatchID := "runtime-v1-recovery-fence-dispatch-" + suffix
	defer func() {
		_, _ = database.Pool.Exec(context.Background(), "delete from runtime_run_dispatches where dispatch_id=$1", dispatchID)
		_, _ = database.Pool.Exec(context.Background(), "delete from runtime_slot_reservations where reservation_id=$1", reservationID)
		_, _ = database.Pool.Exec(context.Background(), "delete from runtime_hosts where runtime_host_id=$1", hostID)
		_, _ = database.Pool.Exec(context.Background(), "delete from runtime_capacity_reservations where run_id=$1", runID)
	}()

	repository := NewRuntimeHostRepository(database)
	identity := RuntimeHostIdentity{RuntimeHostID: hostID, InstanceID: "runtime-v1-recovery-fence-instance-" + suffix, Environment: "integration"}
	capabilities := runtimeTestCapabilities("runtime-v1-recovery-fence-capability-"+suffix, CanonicalAgentFacingToolCapability("read", "ready"))
	if _, err := repository.RegisterHost(ctx, identity, RuntimeHostRegistration{
		Endpoint: "http://runtime-v1-recovery-fence.invalid", Capabilities: capabilities, MaxActiveRuns: 2,
	}); err != nil {
		t.Fatalf("register Host: %v", err)
	}
	if _, err := repository.HeartbeatHost(ctx, identity, RuntimeHostHeartbeat{
		Sequence: 1, ObservedAt: time.Now().UTC(), CapabilityHash: capabilities.CapabilityHash, SignatureKeyID: "runtime-v1-live",
	}); err != nil {
		t.Fatalf("heartbeat Host: %v", err)
	}
	dimension := func(name string) RuntimeCapacityDimension {
		return RuntimeCapacityDimension{Key: "runtime-v1-recovery-fence-" + suffix + "-" + name, Limit: 1, Requested: 1, Version: 1}
	}
	capacity := NewRuntimeCapacityAdmissionService(database)
	capacityReservation, err := capacity.Reserve(ctx, RuntimeCapacityCommand{
		RunID: runID, SnapshotVersion: 1, TTL: time.Minute,
		Dimensions: RuntimeCapacityDimensions{Model: dimension("model"), AuthPool: dimension("auth"), Tool: dimension("tool"), Tenant: dimension("tenant"), User: dimension("user")},
	})
	if err != nil {
		t.Fatalf("reserve Runtime capacity: %v", err)
	}
	reservation, _, err := repository.TryReserveSlot(ctx, AtomicReservationCommand{
		ReservationID: reservationID, RunID: runID, AffinityRuntimeHostID: hostID, OwnerInstanceID: "runtime-v1-dispatch-owner",
		ExecutionScope: "detached_task", CapabilityHash: capabilities.CapabilityHash, RequiredTools: []string{"read"},
		LeaseTokenHash: "sha256:runtime-v1-recovery-fence", FencingToken: 1,
		ExpiresAt: time.Now().UTC().Add(time.Minute), HeartbeatAfter: time.Now().UTC().Add(-time.Minute),
	})
	if err != nil {
		t.Fatalf("reserve Host Slot: %v", err)
	}
	dispatch, err := repository.CreateDispatch(ctx, RuntimeDispatch{
		DispatchID: dispatchID, RunID: runID, ReservationID: reservation.ReservationID, CapacityReservationID: capacityReservation.ReservationID, CapacityReservedVersion: capacityReservation.Version, RuntimeHostID: hostID,
		DispatchAttempt: 1, PlanVersion: 1, FencingToken: reservation.FencingToken, RunTicketJTIHash: "sha256:runtime-v1-recovery-fence",
		TicketExpiresAt: time.Now().UTC().Add(time.Minute), InputManifestHash: "sha256:runtime-v1-recovery-fence",
		OwnerInstanceID: reservation.OwnerInstanceID, LeaseTokenHash: reservation.LeaseTokenHash, LeaseExpiresAt: reservation.ExpiresAt,
	})
	if err != nil {
		t.Fatalf("create dispatch: %v", err)
	}
	fence := ReservationFence{ReservationID: reservation.ReservationID, RuntimeHostID: reservation.RuntimeHostID, OwnerInstanceID: reservation.OwnerInstanceID, LeaseTokenHash: reservation.LeaseTokenHash, FencingToken: reservation.FencingToken}
	stale := DispatchRecoveryClaim{DispatchID: dispatchID, OwnerInstanceID: "runtime-v1-stale-owner", FencingToken: 11, ExpiresAt: time.Now().UTC().Add(-time.Second), ExpectedState: dispatch.State, ExpectedVersion: dispatch.Version}
	if err := repository.ClaimDispatchRecovery(ctx, stale); err != nil {
		t.Fatalf("claim expired recovery owner: %v", err)
	}
	if err := repository.FinalizeDispatchAndReleaseSlot(ctx, DispatchTerminalCommand{Fence: fence, DispatchID: dispatchID, TerminalStatus: "orphaned", ErrorCode: "RUNTIME_RUN_STALLED", RecoveryClaim: &stale}); err == nil || err.Error() != "STALE_FENCING_TOKEN" {
		t.Fatalf("expired owner finalized PostgreSQL dispatch: %v", err)
	}
	storedReservation, err := repository.GetReservation(ctx, reservationID)
	if err != nil || storedReservation.State != "reserved" {
		t.Fatalf("stale owner released PostgreSQL reservation: reservation=%+v err=%v", storedReservation, err)
	}
	storedDispatch, err := repository.GetDispatch(ctx, dispatchID)
	if err != nil || storedDispatch.State != "recovering" {
		t.Fatalf("stale owner changed PostgreSQL dispatch: dispatch=%+v err=%v", storedDispatch, err)
	}
	host, err := repository.GetHost(ctx, hostID)
	if err != nil || host.ReservedRuns != 1 || host.ReservedDetachedTaskRuns != 1 {
		t.Fatalf("stale owner changed PostgreSQL counters: host=%+v err=%v", host, err)
	}

	current := DispatchRecoveryClaim{DispatchID: dispatchID, OwnerInstanceID: "runtime-v1-current-owner", FencingToken: 12, ExpiresAt: time.Now().UTC().Add(time.Minute), ExpectedState: storedDispatch.State, ExpectedVersion: storedDispatch.Version}
	if err := repository.ClaimDispatchRecovery(ctx, current); err != nil {
		t.Fatalf("claim current recovery owner: %v", err)
	}
	start := make(chan struct{})
	var wait sync.WaitGroup
	var finalizeErr, reconcileErr error
	wait.Add(2)
	go func() {
		defer wait.Done()
		<-start
		finalizeErr = repository.FinalizeDispatchAndReleaseSlot(ctx, DispatchTerminalCommand{Fence: fence, DispatchID: dispatchID, TerminalStatus: "orphaned", ErrorCode: "RUNTIME_RUN_STALLED", RecoveryClaim: &current})
	}()
	go func() {
		defer wait.Done()
		<-start
		reconcileErr = repository.RecalculateHostCounters(ctx)
	}()
	close(start)
	wait.Wait()
	if finalizeErr != nil || reconcileErr != nil {
		t.Fatalf("terminal/reconcile race errors finalize=%v reconcile=%v", finalizeErr, reconcileErr)
	}
	finalReservation, reservationErr := repository.GetReservation(ctx, reservationID)
	finalDispatch, dispatchErr := repository.GetDispatch(ctx, dispatchID)
	finalHost, err := repository.GetHost(ctx, hostID)
	if err != nil || finalHost.ReservedRuns != 0 || finalHost.ReservedDetachedTaskRuns != 0 {
		t.Fatalf("current owner did not release PostgreSQL counters: reservation=%+v reservationErr=%v dispatch=%+v dispatchErr=%v host=%+v err=%v", finalReservation, reservationErr, finalDispatch, dispatchErr, finalHost, err)
	}
}

func TestRuntimeHostPostgresFinalSlotRaceAcrossSchedulersWhenConfigured(t *testing.T) {
	if strings.TrimSpace(os.Getenv("HUAHUO_RUNTIME_V1_LIVE_POSTGRES")) != "1" {
		t.Skip("HUAHUO_RUNTIME_V1_LIVE_POSTGRES=1 is required")
	}
	dsn := strings.TrimSpace(os.Getenv("HUAHUO_TEST_DATABASE_DSN"))
	if dsn == "" {
		t.Skip("HUAHUO_TEST_DATABASE_DSN is required")
	}
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
	defer database.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	suffix := fmt.Sprintf("%d", time.Now().UTC().UnixNano())
	hostID := "runtime-v1-final-slot-host-" + suffix
	runA, runB := "runtime-v1-final-slot-run-a-"+suffix, "runtime-v1-final-slot-run-b-"+suffix
	defer func() {
		_, _ = database.Pool.Exec(context.Background(), "delete from runtime_run_dispatches where runtime_host_id=$1", hostID)
		_, _ = database.Pool.Exec(context.Background(), "delete from runtime_slot_reservations where runtime_host_id=$1", hostID)
		_, _ = database.Pool.Exec(context.Background(), "delete from runtime_hosts where runtime_host_id=$1", hostID)
		_, _ = database.Pool.Exec(context.Background(), "delete from runtime_capacity_reservations where run_id in ($1,$2)", runA, runB)
	}()

	capabilities := runtimeTestCapabilities("runtime-v1-final-slot-capability-"+suffix, CanonicalAgentFacingToolCapability("read", "ready"))
	identity := RuntimeHostIdentity{RuntimeHostID: hostID, InstanceID: "runtime-v1-final-slot-instance-" + suffix, Environment: "integration"}
	registration := RuntimeHostRegistration{
		Endpoint: "http://runtime-v1-final-slot.invalid", RuntimeVersion: "runtime-v1-live", AdapterVersion: "adapter-v1-live",
		Capabilities: capabilities, MaxActiveRuns: 1, MaxProductThreadRuns: 1, MaxDetachedTaskRuns: 1,
	}
	registrationRepository := NewRuntimeHostRepository(database)
	if _, err := registrationRepository.RegisterHost(ctx, identity, registration); err != nil {
		t.Fatalf("register Host: %v", err)
	}
	if _, err := registrationRepository.HeartbeatHost(ctx, identity, RuntimeHostHeartbeat{
		Sequence: 1, ObservedAt: time.Now().UTC(), CapabilityHash: capabilities.CapabilityHash, SignatureKeyID: "runtime-v1-live",
	}); err != nil {
		t.Fatalf("heartbeat Host: %v", err)
	}

	capacityService := NewRuntimeCapacityAdmissionService(database)
	newCapacity := func(runID, label string) RuntimeCapacityReservation {
		dimension := func(name string) RuntimeCapacityDimension {
			return RuntimeCapacityDimension{Key: "runtime-v1-final-slot-" + suffix + "-" + label + "-" + name, Limit: 1, Requested: 1, Version: 1}
		}
		reservation, reserveErr := capacityService.Reserve(ctx, RuntimeCapacityCommand{
			RunID: runID, SnapshotVersion: 1, TTL: time.Minute,
			Dimensions: RuntimeCapacityDimensions{Model: dimension("model"), AuthPool: dimension("auth"), Tool: dimension("tool"), Tenant: dimension("tenant"), User: dimension("user")},
		})
		if reserveErr != nil {
			t.Fatalf("reserve capacity %s: %v", label, reserveErr)
		}
		return reservation
	}
	capacityA, capacityB := newCapacity(runA, "a"), newCapacity(runB, "b")
	first := NewRuntimeScheduler(NewRuntimeHostRepository(database), queue.NewMemoryDistributedLockManager())
	second := NewRuntimeScheduler(NewRuntimeHostRepository(database), queue.NewMemoryDistributedLockManager())
	type result struct {
		scheduler *RuntimeScheduler
		capacity  RuntimeCapacityReservation
		handle    RuntimeReservationLease
		err       error
	}
	start := make(chan struct{})
	results := make(chan result, 2)
	var wait sync.WaitGroup
	reserve := func(scheduler *RuntimeScheduler, runID, owner string, capacity RuntimeCapacityReservation) {
		defer wait.Done()
		<-start
		handle, reserveErr := scheduler.Reserve(ctx, ScheduleCommand{
			RunID: runID, OwnerInstanceID: owner, ExecutionScope: "detached_task", CapabilityHash: capabilities.CapabilityHash,
			RuntimeVersion: registration.RuntimeVersion, AdapterVersion: registration.AdapterVersion, RequiredTools: []string{"read"},
			CapacityReservation: capacity, ReservationTTL: time.Minute,
		})
		results <- result{scheduler: scheduler, capacity: capacity, handle: handle, err: reserveErr}
	}
	wait.Add(2)
	go reserve(first, runA, "runtime-v1-final-slot-owner-a", capacityA)
	go reserve(second, runB, "runtime-v1-final-slot-owner-b", capacityB)
	close(start)
	wait.Wait()
	close(results)

	var winner result
	winners := 0
	for candidate := range results {
		if candidate.err == nil {
			winner, winners = candidate, winners+1
			continue
		}
		if !strings.Contains(candidate.err.Error(), "RUNTIME_CAPACITY_UNAVAILABLE") {
			t.Fatalf("final Slot loser error=%v", candidate.err)
		}
		if changed, releaseErr := capacityService.Release(ctx, candidate.capacity, nil); releaseErr != nil || !changed {
			t.Fatalf("release losing capacity: changed=%t err=%v", changed, releaseErr)
		}
	}
	if winners != 1 {
		t.Fatalf("final Slot admitted %d schedulers, want one", winners)
	}
	host, err := registrationRepository.GetHost(ctx, hostID)
	if err != nil || host.ReservedRuns != 1 || host.ReservedDetachedTaskRuns != 1 || host.ActiveRuns != 0 {
		t.Fatalf("final Slot Host counters=%+v err=%v", host, err)
	}
	if err := winner.scheduler.ReleaseBeforeAccept(ctx, winner.handle, "runtime_v1_live_final_slot_cleanup"); err != nil {
		t.Fatalf("release winning Slot: %v", err)
	}
	released, err := registrationRepository.GetHost(ctx, hostID)
	if err != nil || released.ReservedRuns != 0 || released.ReservedDetachedTaskRuns != 0 || released.ActiveRuns != 0 {
		t.Fatalf("released final Slot Host counters=%+v err=%v", released, err)
	}
}

func TestRuntimeHostPostgresTwoRecoveryOwnersConvergeAcceptedCapacityWhenConfigured(t *testing.T) {
	if strings.TrimSpace(os.Getenv("HUAHUO_RUNTIME_V1_LIVE_POSTGRES")) != "1" {
		t.Skip("HUAHUO_RUNTIME_V1_LIVE_POSTGRES=1 is required")
	}
	dsn := strings.TrimSpace(os.Getenv("HUAHUO_TEST_DATABASE_DSN"))
	if dsn == "" {
		t.Skip("HUAHUO_TEST_DATABASE_DSN is required")
	}
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
	defer database.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	suffix := fmt.Sprintf("%d", time.Now().UTC().UnixNano())
	hostID := "runtime-v1-recovery-race-host-" + suffix
	runID := "runtime-v1-recovery-race-run-" + suffix
	defer func() {
		_, _ = database.Pool.Exec(context.Background(), "delete from runtime_run_dispatches where run_id=$1", runID)
		_, _ = database.Pool.Exec(context.Background(), "delete from runtime_slot_reservations where run_id=$1", runID)
		_, _ = database.Pool.Exec(context.Background(), "delete from runtime_capacity_reservations where run_id=$1", runID)
		_, _ = database.Pool.Exec(context.Background(), "delete from runtime_hosts where runtime_host_id=$1", hostID)
	}()

	capabilities := runtimeTestCapabilities("runtime-v1-recovery-race-capability-"+suffix, CanonicalAgentFacingToolCapability("read", "ready"))
	identity := RuntimeHostIdentity{RuntimeHostID: hostID, InstanceID: "runtime-v1-recovery-race-instance-" + suffix, Environment: "integration"}
	primaryHosts := NewRuntimeHostRepository(database)
	registration := RuntimeHostRegistration{
		Endpoint: "http://runtime-v1-recovery-race.invalid", RuntimeVersion: "runtime-v1-live", AdapterVersion: "adapter-v1-live",
		Capabilities: capabilities, MaxActiveRuns: 4, MaxProductThreadRuns: 4, MaxDetachedTaskRuns: 4,
	}
	if _, err := primaryHosts.RegisterHost(ctx, identity, registration); err != nil {
		t.Fatalf("register Host: %v", err)
	}
	if _, err := primaryHosts.HeartbeatHost(ctx, identity, RuntimeHostHeartbeat{
		Sequence: 1, ObservedAt: time.Now().UTC(), CapabilityHash: capabilities.CapabilityHash, SignatureKeyID: "runtime-v1-live",
	}); err != nil {
		t.Fatalf("heartbeat Host: %v", err)
	}

	capacity := NewRuntimeCapacityAdmissionService(database)
	dimension := func(name string) RuntimeCapacityDimension {
		return RuntimeCapacityDimension{Key: "runtime-v1-recovery-race-" + suffix + "-" + name, Limit: 1, Requested: 1, Version: 1}
	}
	capacityReservation, err := capacity.Reserve(ctx, RuntimeCapacityCommand{
		RunID: runID, SnapshotVersion: 1, TTL: time.Minute,
		Dimensions: RuntimeCapacityDimensions{Model: dimension("model"), AuthPool: dimension("auth"), Tool: dimension("tool"), Tenant: dimension("tenant"), User: dimension("user")},
	})
	if err != nil {
		t.Fatalf("reserve capacity: %v", err)
	}
	dispatchScheduler := NewRuntimeSchedulerWithAdmissions(primaryHosts, queue.NewMemoryDistributedLockManager(), nil, capacity)
	handle, err := dispatchScheduler.Reserve(ctx, ScheduleCommand{
		RunID: runID, OwnerInstanceID: "runtime-v1-dispatch-owner", ExecutionScope: "detached_task",
		CapabilityHash: capabilities.CapabilityHash, RuntimeVersion: registration.RuntimeVersion, AdapterVersion: registration.AdapterVersion,
		RequiredTools: []string{"read"}, CapacityReservation: capacityReservation, ReservationTTL: time.Minute,
	})
	if err != nil {
		t.Fatalf("reserve Host Slot: %v", err)
	}
	dispatchID := "runtime-v1-recovery-race-dispatch-" + suffix
	if _, err := primaryHosts.CreateDispatch(ctx, RuntimeDispatch{
		DispatchID: dispatchID, RunID: runID, ReservationID: handle.Reservation.ReservationID,
		CapacityReservationID: capacityReservation.ReservationID, CapacityReservedVersion: capacityReservation.Version,
		RuntimeHostID: hostID, DispatchAttempt: 1, PlanVersion: 1, FencingToken: handle.Reservation.FencingToken,
		RunTicketJTIHash: "sha256:runtime-v1-recovery-race", TicketExpiresAt: time.Now().UTC().Add(time.Minute),
		InputManifestHash: "sha256:runtime-v1-recovery-race", OwnerInstanceID: handle.Reservation.OwnerInstanceID,
		LeaseTokenHash: handle.Reservation.LeaseTokenHash, LeaseExpiresAt: handle.Reservation.ExpiresAt,
	}); err != nil {
		t.Fatalf("create dispatch: %v", err)
	}
	if err := dispatchScheduler.Accept(ctx, handle, dispatchID, "runtime-v1-recovery-race-request"); err != nil {
		t.Fatalf("accept dispatch: %v", err)
	}
	acceptedDispatch, err := primaryHosts.GetDispatch(ctx, dispatchID)
	if err != nil || acceptedDispatch.State != "accepted" {
		t.Fatalf("accepted dispatch=%+v err=%v", acceptedDispatch, err)
	}
	acceptedCapacity, err := capacity.GetActiveByRunID(ctx, runID)
	if err != nil || acceptedCapacity.State != "accepted" || acceptedCapacity.ReservationID != capacityReservation.ReservationID {
		t.Fatalf("accepted capacity=%+v err=%v", acceptedCapacity, err)
	}
	acceptedHost, err := primaryHosts.GetHost(ctx, hostID)
	if err != nil || acceptedHost.ActiveRuns != 1 || acceptedHost.ActiveDetachedTaskRuns != 1 || acceptedHost.ReservedRuns != 0 {
		t.Fatalf("accepted Host=%+v err=%v", acceptedHost, err)
	}

	type recoveryResult struct {
		claim     DispatchRecoveryClaim
		scheduler *RuntimeScheduler
		err       error
	}
	firstScheduler := NewRuntimeSchedulerWithAdmissions(NewRuntimeHostRepository(database), queue.NewMemoryDistributedLockManager(), nil, capacity)
	secondScheduler := NewRuntimeSchedulerWithAdmissions(NewRuntimeHostRepository(database), queue.NewMemoryDistributedLockManager(), nil, capacity)
	start := make(chan struct{})
	results := make(chan recoveryResult, 2)
	var wait sync.WaitGroup
	claim := func(scheduler *RuntimeScheduler, owner string, fence int64) {
		defer wait.Done()
		<-start
		candidate := DispatchRecoveryClaim{
			DispatchID: dispatchID, OwnerInstanceID: owner, FencingToken: fence, ExpiresAt: time.Now().UTC().Add(time.Minute),
			ExpectedState: acceptedDispatch.State, ExpectedVersion: acceptedDispatch.Version,
		}
		results <- recoveryResult{claim: candidate, scheduler: scheduler, err: scheduler.Hosts.ClaimDispatchRecovery(ctx, candidate)}
	}
	wait.Add(2)
	go claim(firstScheduler, "runtime-v1-recovery-owner-a", 101)
	go claim(secondScheduler, "runtime-v1-recovery-owner-b", 202)
	close(start)
	wait.Wait()
	close(results)

	var winner recoveryResult
	winners := 0
	for candidate := range results {
		if candidate.err == nil {
			winner, winners = candidate, winners+1
			continue
		}
		if candidate.err.Error() != "STALE_FENCING_TOKEN" {
			t.Fatalf("recovery claimant error=%v", candidate.err)
		}
	}
	if winners != 1 {
		t.Fatalf("recovery claim winners=%d, want one", winners)
	}
	recoveringDispatch, err := primaryHosts.GetDispatch(ctx, dispatchID)
	if err != nil || recoveringDispatch.State != "recovering" || recoveringDispatch.RecoveryOwnerInstanceID != winner.claim.OwnerInstanceID || recoveringDispatch.RecoveryFencingToken != winner.claim.FencingToken {
		t.Fatalf("recovery winner not durable dispatch=%+v err=%v", recoveringDispatch, err)
	}
	reservation, err := primaryHosts.GetReservation(ctx, handle.Reservation.ReservationID)
	if err != nil {
		t.Fatalf("read accepted reservation: %v", err)
	}
	if err := winner.scheduler.finalizeRecoveredDispatch(ctx, winner.claim, recoveringDispatch, reservation, "orphaned", "RUNTIME_RUN_STALLED"); err != nil {
		t.Fatalf("winner terminal convergence: %v", err)
	}
	if err := winner.scheduler.finalizeRecoveredDispatch(ctx, winner.claim, recoveringDispatch, reservation, "orphaned", "RUNTIME_RUN_STALLED"); err == nil || err.Error() != "STALE_FENCING_TOKEN" {
		t.Fatalf("replayed recovery terminal result=%v", err)
	}

	finalDispatch, dispatchErr := primaryHosts.GetDispatch(ctx, dispatchID)
	finalReservation, reservationErr := primaryHosts.GetReservation(ctx, handle.Reservation.ReservationID)
	finalHost, hostErr := primaryHosts.GetHost(ctx, hostID)
	finalCapacity, capacityErr := capacity.GetLatestByRunID(ctx, runID)
	if dispatchErr != nil || finalDispatch.State != "orphaned" || reservationErr != nil || finalReservation.State != "released" || hostErr != nil || finalHost.ActiveRuns != 0 || finalHost.ActiveDetachedTaskRuns != 0 || finalHost.ReservedRuns != 0 || capacityErr != nil || finalCapacity.State != "released" || finalCapacity.ReservationID != capacityReservation.ReservationID {
		t.Fatalf("terminal convergence dispatch=%+v dispatchErr=%v reservation=%+v reservationErr=%v host=%+v hostErr=%v capacity=%+v capacityErr=%v", finalDispatch, dispatchErr, finalReservation, reservationErr, finalHost, hostErr, finalCapacity, capacityErr)
	}
}

func TestFinalizeRecoveredDispatchDoesNotReleaseCapacityAfterStaleClaim(t *testing.T) {
	ctx := context.Background()
	hosts := NewRuntimeHostRepository(nil)
	identity := RuntimeHostIdentity{RuntimeHostID: "host-stale-recovery-capacity", InstanceID: "instance-stale-recovery-capacity", Environment: "test"}
	capabilities := runtimeTestCapabilities("cap-stale-recovery-capacity", CanonicalAgentFacingToolCapability("read", "ready"))
	if _, err := hosts.RegisterHost(ctx, identity, RuntimeHostRegistration{
		Endpoint: "http://host-stale-recovery-capacity", Capabilities: capabilities, MaxActiveRuns: 2,
	}); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if _, err := hosts.HeartbeatHost(ctx, identity, RuntimeHostHeartbeat{
		Sequence: 1, ObservedAt: now, CapabilityHash: capabilities.CapabilityHash, SignatureKeyID: "test-key",
	}); err != nil {
		t.Fatal(err)
	}
	capacity := NewRuntimeCapacityAdmissionService(nil)
	capacityReservation, err := capacity.Reserve(ctx, testCapacityCommand("run-stale-recovery-capacity", 1))
	if err != nil {
		t.Fatal(err)
	}
	reservation, _, err := hosts.TryReserveSlot(ctx, AtomicReservationCommand{
		ReservationID: "reservation-stale-recovery-capacity", RunID: capacityReservation.RunID, AffinityRuntimeHostID: identity.RuntimeHostID,
		OwnerInstanceID: "worker-dispatch-owner", ExecutionScope: "detached_task", CapabilityHash: capabilities.CapabilityHash, RequiredTools: []string{"read"},
		LeaseTokenHash: "sha256:stale-recovery-capacity", FencingToken: 8, ExpiresAt: now.Add(time.Minute), HeartbeatAfter: now.Add(-time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	dispatch, err := hosts.CreateDispatch(ctx, RuntimeDispatch{
		DispatchID: "dispatch-stale-recovery-capacity", RunID: reservation.RunID, ReservationID: reservation.ReservationID,
		RuntimeHostID: reservation.RuntimeHostID, CapacityReservationID: capacityReservation.ReservationID, CapacityReservedVersion: capacityReservation.Version,
		DispatchAttempt: 1, PlanVersion: 1, FencingToken: reservation.FencingToken, RunTicketJTIHash: "sha256:stale-recovery-capacity",
		TicketExpiresAt: now.Add(time.Minute), InputManifestHash: "sha256:stale-recovery-capacity", OwnerInstanceID: reservation.OwnerInstanceID,
		LeaseTokenHash: reservation.LeaseTokenHash, LeaseExpiresAt: reservation.ExpiresAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	staleClaim := DispatchRecoveryClaim{
		DispatchID: dispatch.DispatchID, OwnerInstanceID: "worker-recovery-stale", FencingToken: 43,
		ExpiresAt: now.Add(-time.Second), ExpectedState: dispatch.State, ExpectedVersion: dispatch.Version,
	}
	if err := hosts.ClaimDispatchRecovery(ctx, staleClaim); err != nil {
		t.Fatal(err)
	}
	scheduler := NewRuntimeSchedulerWithAdmissions(hosts, queue.NewMemoryDistributedLockManager(), NewRuntimeSessionAdmissionService(nil, queue.NewMemoryDistributedLockManager()), capacity)
	if err := scheduler.finalizeRecoveredDispatch(ctx, staleClaim, dispatch, reservation, "orphaned", "RUNTIME_RUN_STALLED"); err == nil || err.Error() != "STALE_FENCING_TOKEN" {
		t.Fatalf("stale recovery terminal error=%v", err)
	}
	stillActive, err := capacity.GetActiveByRunID(ctx, capacityReservation.RunID)
	if err != nil || stillActive.ReservationID != capacityReservation.ReservationID || stillActive.State != "reserved" {
		t.Fatalf("stale recovery released capacity: capacity=%+v err=%v", stillActive, err)
	}
}
