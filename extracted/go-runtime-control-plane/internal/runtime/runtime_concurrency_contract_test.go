package runtime

import (
	"context"
	"fmt"
	"testing"
	"time"

	"huahuoai/backend/source/internal/persistence"
	"huahuoai/backend/source/internal/queue"
)

func TestProductSessionAdmissionIsTenantScopedAndFenced(t *testing.T) {
	ctx := context.Background()
	locks := queue.NewMemoryDistributedLockManager()
	service := NewRuntimeSessionAdmissionService(nil, locks)
	key := ProductSessionAdmissionKey{TenantID: "tenant-a", ThreadID: "thread", AgentProfile: "agent", ContextGeneration: 1, SessionGeneration: 1}
	if got := key.RedisKey(); got != "runtime:session:tenant-a:thread:agent:1:1" {
		t.Fatalf("redis key=%q", got)
	}
	first, err := service.Acquire(ctx, ProductSessionAdmissionCommand{Key: key, BindingID: "binding-a", RunID: "run-a", OwnerInstanceID: "worker-a", TTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Acquire(ctx, ProductSessionAdmissionCommand{Key: key, BindingID: "binding-a", RunID: "run-b", OwnerInstanceID: "worker-b", TTL: time.Minute}); err == nil {
		t.Fatal("same tenant-scoped binding admitted twice")
	}
	other := key
	other.TenantID = "tenant-b"
	if _, err := service.Acquire(ctx, ProductSessionAdmissionCommand{Key: other, BindingID: "binding-b", RunID: "run-b", OwnerInstanceID: "worker-b", TTL: time.Minute}); err != nil {
		t.Fatalf("different tenant contended: %v", err)
	}
	if err := service.BindReservation(ctx, first, "reservation-a"); err != nil {
		t.Fatal(err)
	}
	if err := service.BindReservation(ctx, first, "reservation-other"); err == nil {
		t.Fatal("different reservation rebound admission")
	}
	if changed, err := service.Release(ctx, first, "succeeded"); err != nil || !changed {
		t.Fatalf("release changed=%v err=%v", changed, err)
	}
	if changed, err := service.Release(ctx, first, "succeeded"); err != nil || changed {
		t.Fatalf("replayed release changed=%v err=%v", changed, err)
	}
}

func TestRuntimeCapacityReservationEnforcesEveryDimension(t *testing.T) {
	ctx := context.Background()
	service := NewRuntimeCapacityAdmissionService(nil)
	command := testCapacityCommand("run-a", 1)
	first, err := service.Reserve(ctx, command)
	if err != nil {
		t.Fatal(err)
	}
	second := testCapacityCommand("run-b", 1)
	if _, err := service.Reserve(ctx, second); err == nil {
		t.Fatal("second reservation exceeded final dimension token")
	}
	if err := service.CommitAccepted(ctx, first); err != nil {
		t.Fatal(err)
	}
	if changed, err := service.Release(ctx, first, map[string]any{"modelTokens": 12, "secret": "drop"}); err != nil || !changed {
		t.Fatalf("release changed=%v err=%v", changed, err)
	}
	latest, err := service.GetLatestByRunID(ctx, first.RunID)
	if err != nil || latest.State != "released" || latest.ReservationID != first.ReservationID {
		t.Fatalf("released replay reservation=%+v err=%v", latest, err)
	}
	secondReservation, err := service.Reserve(ctx, second)
	if err != nil {
		t.Fatalf("capacity not returned after release: %v", err)
	}
	if changed, err := service.Release(ctx, secondReservation, nil); err != nil || !changed {
		t.Fatalf("second release changed=%v err=%v", changed, err)
	}
	replayed, err := service.Reserve(ctx, command)
	if err != nil || replayed.Version <= first.Version {
		t.Fatalf("released run could not reserve a new fenced version: first=%+v replay=%+v err=%v", first, replayed, err)
	}
	zero := testCapacityCommand("run-zero", 1)
	zero.Dimensions.Tool.Limit = 0
	if _, err := service.Reserve(ctx, zero); err == nil {
		t.Fatal("zero tool capacity accepted")
	}
}

func TestRuntimeHostReservationCountersMoveByPriorState(t *testing.T) {
	ctx := context.Background()
	repo := NewRuntimeHostRepository(nil)
	capabilities := runtimeTestCapabilities("cap", CanonicalAgentFacingToolCapability("read", "ready"))
	identity := RuntimeHostIdentity{RuntimeHostID: "host", InstanceID: "instance", Environment: "test"}
	if _, err := repo.RegisterHost(ctx, identity, RuntimeHostRegistration{Endpoint: "http://host", Capabilities: capabilities, MaxActiveRuns: 4, MaxProductThreadRuns: 4, MaxDetachedTaskRuns: 4}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.HeartbeatHost(ctx, identity, RuntimeHostHeartbeat{Sequence: 1, ObservedAt: time.Now(), CapabilityHash: "cap", SignatureKeyID: "key", ActiveRuns: 99, ReservedRuns: 99}); err != nil {
		t.Fatal(err)
	}
	reservation, host, err := repo.TryReserveSlot(ctx, AtomicReservationCommand{ReservationID: "reservation", RunID: "run", OwnerInstanceID: "worker", ExecutionScope: "detached_task", CapabilityHash: "cap", RequiredTools: []string{"read"}, LeaseTokenHash: "sha256:token", FencingToken: 1, ExpiresAt: time.Now().Add(time.Minute), HeartbeatAfter: time.Now().Add(-time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	if host.ReservedRuns != 1 || host.ActiveRuns != 0 {
		t.Fatalf("reserved host=%+v", host)
	}
	if _, err := repo.CreateDispatch(ctx, RuntimeDispatch{DispatchID: "dispatch", RunID: "run", ReservationID: reservation.ReservationID, CapacityReservationID: "capacity-reservation-counters", CapacityReservedVersion: 1, RuntimeHostID: "host", DispatchAttempt: 1, PlanVersion: 1, FencingToken: 1, RunTicketJTIHash: "hash", TicketExpiresAt: time.Now().Add(time.Minute), InputManifestHash: "manifest", OwnerInstanceID: "worker", LeaseTokenHash: "sha256:token", LeaseExpiresAt: time.Now().Add(time.Minute)}); err != nil {
		t.Fatal(err)
	}
	if err := repo.ConfirmDispatchAccepted(ctx, DispatchAcceptedCommand{Fence: ReservationFence{ReservationID: "reservation", RuntimeHostID: "host", OwnerInstanceID: "worker", LeaseTokenHash: "sha256:token", FencingToken: 1}, DispatchID: "dispatch", RuntimeRequestID: "request"}); err != nil {
		t.Fatal(err)
	}
	accepted, _ := repo.GetHost(ctx, "host")
	if accepted.ReservedRuns != 0 || accepted.ActiveRuns != 1 || accepted.ReportedActiveRuns != 99 {
		t.Fatalf("accepted host=%+v", accepted)
	}
	fence := ReservationFence{ReservationID: "reservation", RuntimeHostID: "host", OwnerInstanceID: "worker", LeaseTokenHash: "sha256:token", FencingToken: 1}
	if changed, err := repo.ReleaseReservation(ctx, ReservationReleaseCommand{Fence: fence, Reason: "succeeded"}); err != nil || !changed {
		t.Fatalf("release changed=%v err=%v", changed, err)
	}
	if changed, err := repo.ReleaseReservation(ctx, ReservationReleaseCommand{Fence: fence, Reason: "succeeded"}); err != nil || changed {
		t.Fatalf("duplicate release changed=%v err=%v", changed, err)
	}
	released, _ := repo.GetHost(ctx, "host")
	if released.ActiveRuns != 0 || released.ReservedRuns != 0 {
		t.Fatalf("released host=%+v", released)
	}
}

func TestRuntimeHostReservationRejectsRawFilesystemRequiredTool(t *testing.T) {
	_, _, err := NewRuntimeHostRepository(nil).TryReserveSlot(context.Background(), AtomicReservationCommand{
		ReservationID: "reservation-raw-tool", RunID: "run-raw-tool", OwnerInstanceID: "worker-raw-tool",
		ExecutionScope: "detached_task", CapabilityHash: "cap-raw-tool", RequiredTools: []string{"find"},
		LeaseTokenHash: "sha256:raw-tool", FencingToken: 1, ExpiresAt: time.Now().UTC().Add(time.Minute),
	})
	if err == nil || err.Error() != "RUNTIME_TOOL_UNAVAILABLE" {
		t.Fatalf("raw required tool error=%v, want RUNTIME_TOOL_UNAVAILABLE", err)
	}
}

func TestAppendRunEventRejectsGapAndConflictingDuplicate(t *testing.T) {
	ctx := context.Background()
	repo := NewRuntimeHostRepository(nil)
	repo.dispatches["dispatch"] = RuntimeDispatch{DispatchID: "dispatch", RunID: "run"}
	event := RuntimeHostRunEvent{EventID: "event-1", RunID: "run", DispatchID: "dispatch", SourceSequence: 1, EventType: "run.started", Visibility: "internal", SafePayload: map[string]any{"status": "running"}}
	if err := repo.AppendRunEventAndAdvanceCursor(ctx, event, 0); err != nil {
		t.Fatal(err)
	}
	if err := repo.AppendRunEventAndAdvanceCursor(ctx, event, 0); err != nil {
		t.Fatalf("same duplicate not idempotent: %v", err)
	}
	conflict := event
	conflict.EventType = "run.failed"
	if err := repo.AppendRunEventAndAdvanceCursor(ctx, conflict, 0); err == nil {
		t.Fatal("conflicting duplicate accepted")
	}
	gap := event
	gap.EventID = "event-3"
	gap.SourceSequence = 3
	if err := repo.AppendRunEventAndAdvanceCursor(ctx, gap, 1); err == nil {
		t.Fatal("event gap accepted")
	}
}

func TestTerminalConvergerResumesRemainingStepsAfterFailure(t *testing.T) {
	ctx := context.Background()
	hosts := NewRuntimeHostRepository(nil)
	identity := RuntimeHostIdentity{RuntimeHostID: "host-terminal", InstanceID: "instance-terminal", Environment: "test"}
	capabilities := runtimeTestCapabilities("cap-terminal")
	if _, err := hosts.RegisterHost(ctx, identity, RuntimeHostRegistration{Endpoint: "http://host", Capabilities: capabilities, MaxActiveRuns: 4}); err != nil {
		t.Fatal(err)
	}
	if _, err := hosts.HeartbeatHost(ctx, identity, RuntimeHostHeartbeat{Sequence: 1, ObservedAt: time.Now(), CapabilityHash: "cap-terminal", SignatureKeyID: "key"}); err != nil {
		t.Fatal(err)
	}
	reservation, _, err := hosts.TryReserveSlot(ctx, AtomicReservationCommand{ReservationID: "reservation-terminal", RunID: "run-terminal", OwnerInstanceID: "worker", ExecutionScope: "detached_task", CapabilityHash: "cap-terminal", LeaseTokenHash: "sha256:terminal", FencingToken: 1, ExpiresAt: time.Now().Add(time.Minute), HeartbeatAfter: time.Now().Add(-time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := hosts.CreateDispatch(ctx, RuntimeDispatch{DispatchID: "dispatch-terminal", RunID: "run-terminal", ReservationID: reservation.ReservationID, CapacityReservationID: "capacity-terminal", CapacityReservedVersion: 1, RuntimeHostID: reservation.RuntimeHostID, DispatchAttempt: 1, PlanVersion: 1, FencingToken: 1, RunTicketJTIHash: "jti", TicketExpiresAt: time.Now().Add(time.Minute), InputManifestHash: "manifest", OwnerInstanceID: "worker", LeaseTokenHash: "sha256:terminal", LeaseExpiresAt: time.Now().Add(time.Minute)}); err != nil {
		t.Fatal(err)
	}
	fence := ReservationFence{ReservationID: reservation.ReservationID, RuntimeHostID: reservation.RuntimeHostID, OwnerInstanceID: "worker", LeaseTokenHash: "sha256:terminal", FencingToken: 1}
	if err := hosts.ConfirmDispatchAccepted(ctx, DispatchAcceptedCommand{Fence: fence, DispatchID: "dispatch-terminal", RuntimeRequestID: "request"}); err != nil {
		t.Fatal(err)
	}
	if err := hosts.AppendRunEventAndAdvanceCursor(ctx, RuntimeHostRunEvent{EventID: "event-terminal", RunID: "run-terminal", DispatchID: "dispatch-terminal", RuntimeHostID: "host-terminal", SourceSequence: 1, EventType: "run.succeeded", Visibility: "internal", SafePayload: map[string]any{"status": "succeeded"}}, 0); err != nil {
		t.Fatal(err)
	}
	capacity := NewRuntimeCapacityAdmissionService(nil)
	capacityReservation, err := capacity.Reserve(ctx, testCapacityCommand("run-terminal", 10))
	if err != nil {
		t.Fatal(err)
	}
	if err := capacity.CommitAccepted(ctx, capacityReservation); err != nil {
		t.Fatal(err)
	}
	queueRepository := persistence.NewQueueRepository()
	queueRepository.Enqueue(map[string]any{"queueId": "runtime_events:terminal", "queueName": "runtime_events", "taskType": "runtime_event_ingest", "taskId": "run-terminal"})
	_, proof, err := queueRepository.Lease(ctx, "runtime_events", "event-worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := queueRepository.MarkRunning(ctx, proof); err != nil {
		t.Fatal(err)
	}
	converger := NewRuntimeTerminalConverger(nil, hosts, nil, capacity, queueRepository)
	productCalls := 0
	productProjectionKey := ""
	agentCalls := 0
	eventCalls := 0
	converger.ProjectProduct = func(_ context.Context, _ TerminalConvergenceCommand, convergenceID string) error {
		productCalls++
		productProjectionKey = convergenceID
		if productCalls == 1 {
			return fmt.Errorf("injected")
		}
		return nil
	}
	converger.ConvergeAgentRun = func(context.Context, TerminalConvergenceCommand, string) error { agentCalls++; return nil }
	converger.AppendPublicEvent = func(context.Context, TerminalConvergenceCommand, string) error { eventCalls++; return nil }
	command := TerminalConvergenceCommand{
		DispatchID: "dispatch-terminal", RunID: "run-terminal", TerminalSourceSequence: 1, TerminalStatus: "succeeded",
		SafeResult: map[string]any{"finalAnswer": "durable terminal answer"}, SafeError: map[string]any{}, ActualUsage: map[string]any{"modelTokens": 9},
		QueueProof: proof, CapacityReservation: capacityReservation,
		DispatchTerminal: DispatchTerminalCommand{Fence: fence, DispatchID: "dispatch-terminal", TerminalStatus: "succeeded"},
	}
	if result, err := converger.Converge(ctx, command); err == nil || result.Complete {
		t.Fatalf("injected convergence result=%+v err=%v", result, err)
	}
	incomplete, err := converger.ListIncomplete(ctx, 10)
	if err != nil || len(incomplete) != 1 || incomplete[0].RunID != command.RunID || incomplete[0].SnapshotHash == "" {
		t.Fatalf("incomplete=%+v err=%v", incomplete, err)
	}
	recovery, found, err := converger.FindIncompleteByQueueID(ctx, proof.QueueID)
	if err != nil || !found || recovery.ConvergenceID != incomplete[0].ConvergenceID || recovery.SafeResult["finalAnswer"] != "durable terminal answer" || runtimeHostInt64(recovery.ActualUsage["modelTokens"]) != 9 || recovery.CapacityReservationID != capacityReservation.ReservationID || recovery.CapacitySnapshotVersion != capacityReservation.SnapshotVersion {
		t.Fatalf("recovery=%+v found=%v err=%v", recovery, found, err)
	}
	conflict := command
	conflict.SafeResult = map[string]any{"finalAnswer": "changed"}
	if _, err := converger.Converge(ctx, conflict); err == nil || err.Error() != "RUNTIME_EVENT_GAP" {
		t.Fatalf("conflicting terminal snapshot err=%v", err)
	}
	result, err := converger.Converge(ctx, command)
	if err != nil || !result.Complete {
		t.Fatalf("resumed convergence result=%+v err=%v", result, err)
	}
	if productCalls != 2 || agentCalls != 1 || eventCalls != 1 {
		t.Fatalf("step calls product=%d agent=%d event=%d", productCalls, agentCalls, eventCalls)
	}
	if productProjectionKey != "terminal:dispatch-terminal:1" {
		t.Fatalf("product projection key=%q", productProjectionKey)
	}
	if replay, err := converger.Converge(ctx, command); err != nil || !replay.Complete || agentCalls != 1 || eventCalls != 1 {
		t.Fatalf("replay=%+v err=%v", replay, err)
	}
}

func TestTerminalConvergerDurableFinalizationFailureReplaysWithoutStepwiseEffects(t *testing.T) {
	ctx := context.Background()
	hosts := NewRuntimeHostRepository(nil)
	identity := RuntimeHostIdentity{RuntimeHostID: "host-durable-terminal", InstanceID: "instance-durable-terminal", Environment: "test"}
	if _, err := hosts.RegisterHost(ctx, identity, RuntimeHostRegistration{Endpoint: "http://host", Capabilities: runtimeTestCapabilities("cap-durable-terminal"), MaxActiveRuns: 4}); err != nil {
		t.Fatal(err)
	}
	if _, err := hosts.HeartbeatHost(ctx, identity, RuntimeHostHeartbeat{Sequence: 1, ObservedAt: time.Now(), CapabilityHash: "cap-durable-terminal", SignatureKeyID: "key"}); err != nil {
		t.Fatal(err)
	}
	reservation, _, err := hosts.TryReserveSlot(ctx, AtomicReservationCommand{ReservationID: "reservation-durable-terminal", RunID: "run-durable-terminal", OwnerInstanceID: "worker", ExecutionScope: "detached_task", CapabilityHash: "cap-durable-terminal", LeaseTokenHash: "sha256:durable-terminal", FencingToken: 1, ExpiresAt: time.Now().Add(time.Minute), HeartbeatAfter: time.Now().Add(-time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	fence := ReservationFence{ReservationID: reservation.ReservationID, RuntimeHostID: reservation.RuntimeHostID, OwnerInstanceID: "worker", LeaseTokenHash: "sha256:durable-terminal", FencingToken: 1}
	if _, err := hosts.CreateDispatch(ctx, RuntimeDispatch{DispatchID: "dispatch-durable-terminal", RunID: "run-durable-terminal", ReservationID: reservation.ReservationID, CapacityReservationID: "capacity-durable-terminal", CapacityReservedVersion: 1, RuntimeHostID: reservation.RuntimeHostID, DispatchAttempt: 1, PlanVersion: 1, FencingToken: 1, RunTicketJTIHash: "jti", TicketExpiresAt: time.Now().Add(time.Minute), InputManifestHash: "manifest", OwnerInstanceID: "worker", LeaseTokenHash: "sha256:durable-terminal", LeaseExpiresAt: time.Now().Add(time.Minute)}); err != nil {
		t.Fatal(err)
	}
	if err := hosts.ConfirmDispatchAccepted(ctx, DispatchAcceptedCommand{Fence: fence, DispatchID: "dispatch-durable-terminal", RuntimeRequestID: "request"}); err != nil {
		t.Fatal(err)
	}
	if err := hosts.AppendRunEventAndAdvanceCursor(ctx, RuntimeHostRunEvent{EventID: "event-durable-terminal", RunID: "run-durable-terminal", DispatchID: "dispatch-durable-terminal", RuntimeHostID: identity.RuntimeHostID, SourceSequence: 1, EventType: "run.succeeded", Visibility: "internal", SafePayload: map[string]any{"status": "succeeded"}}, 0); err != nil {
		t.Fatal(err)
	}
	capacity := NewRuntimeCapacityAdmissionService(nil)
	capacityReservation, err := capacity.Reserve(ctx, testCapacityCommand("run-durable-terminal", 10))
	if err != nil {
		t.Fatal(err)
	}
	if err := capacity.CommitAccepted(ctx, capacityReservation); err != nil {
		t.Fatal(err)
	}
	queueRepository := persistence.NewQueueRepository()
	queueRepository.Enqueue(map[string]any{"queueId": "runtime_events:durable-terminal", "queueName": "runtime_events", "taskType": "runtime_event_ingest", "taskId": "run-durable-terminal"})
	_, proof, err := queueRepository.Lease(ctx, "runtime_events", "event-worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := queueRepository.MarkRunning(ctx, proof); err != nil {
		t.Fatal(err)
	}
	converger := NewRuntimeTerminalConverger(nil, hosts, nil, capacity, queueRepository)
	productCalls, agentCalls, eventCalls, finalizerCalls := 0, 0, 0, 0
	converger.ProjectProduct = func(context.Context, TerminalConvergenceCommand, string) error { productCalls++; return nil }
	converger.ConvergeAgentRun = func(context.Context, TerminalConvergenceCommand, string) error { agentCalls++; return nil }
	converger.AppendPublicEvent = func(context.Context, TerminalConvergenceCommand, string) error { eventCalls++; return nil }
	converger.durableFinalizer = func(_ context.Context, _ TerminalConvergenceCommand, convergenceID string, frozenProof persistence.QueueLeaseProof) error {
		finalizerCalls++
		if finalizerCalls == 1 {
			return fmt.Errorf("injected before durable commit")
		}
		if _, err := queueRepository.CompleteTerminalConvergence(ctx, frozenProof, convergenceID); err != nil {
			return err
		}
		converger.mu.Lock()
		progress := converger.progress[convergenceID]
		progress.UsageSettled = true
		progress.AgentRunConverged = true
		progress.DispatchFinalized = true
		progress.SessionReleased = true
		progress.PublicEventAppended = true
		progress.QueueCompleted = true
		converger.progress[convergenceID] = progress
		converger.mu.Unlock()
		return nil
	}
	command := TerminalConvergenceCommand{
		DispatchID: "dispatch-durable-terminal", RunID: "run-durable-terminal", TerminalSourceSequence: 1, TerminalStatus: "succeeded",
		SafeResult: map[string]any{"finalAnswer": "done"}, ActualUsage: map[string]any{"modelTokens": 7}, QueueProof: proof,
		CapacityReservation: capacityReservation, DispatchTerminal: DispatchTerminalCommand{Fence: fence, DispatchID: "dispatch-durable-terminal", TerminalStatus: "succeeded"},
		AgentRunTerminal: &TerminalAgentRunProjection{
			AgentRunStatus: "succeeded", PlanVersion: 1, PlanStatus: "succeeded", PublicResult: map[string]any{"finalAnswer": "done"},
			PublicEvent: persistence.AgentRunEvent{AgentRunID: "run-durable-terminal", EventType: "succeeded", Status: "succeeded", SafeData: map[string]any{"status": "succeeded"}},
		},
	}
	if result, err := converger.Converge(ctx, command); err == nil || result.Complete {
		t.Fatalf("first durable finalization result=%+v err=%v", result, err)
	}
	if records := queueRepository.ListQueueRecords(map[string]any{"queueId": proof.QueueID}); len(records) != 1 || fmt.Sprint(records[0]["status"]) != "running" {
		t.Fatalf("failed durable transaction changed queue=%+v", records)
	}
	if productCalls != 1 || agentCalls != 0 || eventCalls != 0 {
		t.Fatalf("failure called split steps product=%d agent=%d event=%d", productCalls, agentCalls, eventCalls)
	}
	result, err := converger.Converge(ctx, command)
	if err != nil || !result.Complete {
		t.Fatalf("durable replay result=%+v err=%v", result, err)
	}
	if productCalls != 1 || agentCalls != 0 || eventCalls != 0 || finalizerCalls != 2 {
		t.Fatalf("replay calls product=%d agent=%d event=%d finalizer=%d", productCalls, agentCalls, eventCalls, finalizerCalls)
	}
	if records := queueRepository.ListQueueRecords(map[string]any{"queueId": proof.QueueID}); len(records) != 1 || fmt.Sprint(records[0]["status"]) != "succeeded" {
		t.Fatalf("durable replay queue=%+v", records)
	}
}

func TestTerminalConvergerRejectsCrossRunSessionAdmission(t *testing.T) {
	converger := NewRuntimeTerminalConverger(nil, nil, nil, nil, nil)
	foreignAdmission := RuntimeSessionAdmissionLease{Admission: RuntimeSessionAdmission{RunID: "run-other"}}
	_, err := converger.Converge(context.Background(), TerminalConvergenceCommand{
		DispatchID:             "dispatch-target",
		RunID:                  "run-target",
		TerminalSourceSequence: 1,
		TerminalStatus:         "succeeded",
		QueueProof:             persistence.QueueLeaseProof{QueueID: "runtime_events:target"},
		DispatchTerminal:       DispatchTerminalCommand{DispatchID: "dispatch-target"},
		SessionRequired:        true,
		SessionAdmission:       &foreignAdmission,
	})
	if err == nil || err.Error() != "INVALID_ARGUMENT" {
		t.Fatalf("cross-run session admission err=%v", err)
	}
}

func TestTerminalConvergerRejectsForeignDispatchBindingBeforeProjectionOrProgress(t *testing.T) {
	ctx := context.Background()
	hosts := NewRuntimeHostRepository(nil)
	hosts.dispatches["dispatch-a"] = RuntimeDispatch{
		DispatchID:    "dispatch-a",
		RunID:         "run-a",
		RuntimeHostID: "host-a",
	}

	for _, testCase := range []struct {
		name          string
		runID         string
		runtimeHostID string
	}{
		{name: "run belongs to another dispatch", runID: "run-b", runtimeHostID: "host-a"},
		{name: "runtime host belongs to another dispatch", runID: "run-a", runtimeHostID: "host-b"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			converger := NewRuntimeTerminalConverger(nil, hosts, nil, nil, nil)
			productCalls, agentCalls, publicEventCalls, queueCalls := 0, 0, 0, 0
			converger.ProjectProduct = func(context.Context, TerminalConvergenceCommand, string) error {
				productCalls++
				return nil
			}
			converger.ConvergeAgentRun = func(context.Context, TerminalConvergenceCommand, string) error {
				agentCalls++
				return nil
			}
			converger.AppendPublicEvent = func(context.Context, TerminalConvergenceCommand, string) error {
				publicEventCalls++
				return nil
			}
			converger.CompleteQueue = func(context.Context, TerminalConvergenceCommand, string) error {
				queueCalls++
				return nil
			}

			result, err := converger.Converge(ctx, TerminalConvergenceCommand{
				DispatchID:             "dispatch-a",
				RunID:                  testCase.runID,
				TerminalSourceSequence: 1,
				TerminalStatus:         "succeeded",
				QueueProof:             persistence.QueueLeaseProof{QueueID: "runtime_events:dispatch-a"},
				DispatchTerminal: DispatchTerminalCommand{
					DispatchID: "dispatch-a",
					Fence:      ReservationFence{RuntimeHostID: testCase.runtimeHostID},
				},
			})
			if err == nil || err.Error() != "RUNTIME_TERMINAL_CONVERGENCE_INCONSISTENT" {
				t.Fatalf("foreign dispatch binding result=%+v err=%v", result, err)
			}
			if result.ConvergenceID != "" || len(result.Completed) != 0 {
				t.Fatalf("foreign binding created convergence result=%+v", result)
			}
			if productCalls != 0 || agentCalls != 0 || publicEventCalls != 0 || queueCalls != 0 {
				t.Fatalf("foreign binding invoked terminal effects product=%d agent=%d event=%d queue=%d", productCalls, agentCalls, publicEventCalls, queueCalls)
			}
			if len(converger.progress) != 0 {
				t.Fatalf("foreign binding persisted in-memory convergence progress=%+v", converger.progress)
			}
			incomplete, listErr := converger.ListIncomplete(ctx, 10)
			if listErr != nil || len(incomplete) != 0 {
				t.Fatalf("foreign binding retained incomplete convergence=%+v err=%v", incomplete, listErr)
			}
		})
	}
}

func TestSessionAdmissionDirectReleaseReplaysCleanupAfterPostCommitCrashPoint(t *testing.T) {
	ctx := context.Background()
	locks := queue.NewMemoryDistributedLockManager()
	service := NewRuntimeSessionAdmissionService(nil, locks)
	lease, err := service.Acquire(ctx, ProductSessionAdmissionCommand{
		Key:             ProductSessionAdmissionKey{TenantID: "tenant-cleanup", ThreadID: "thread-cleanup", AgentProfile: "agent", ContextGeneration: 1, SessionGeneration: 1},
		BindingID:       "binding-cleanup",
		RunID:           "run-cleanup",
		OwnerInstanceID: "worker-cleanup",
		TTL:             time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	service.afterAdmissionCleanupEnqueue = func() error { return fmt.Errorf("simulated post-commit interruption") }
	changed, err := service.Release(ctx, lease, "reservation_failed")
	if !changed || err == nil || err.Error() != "simulated post-commit interruption" {
		t.Fatalf("post-commit interruption changed=%v err=%v", changed, err)
	}
	if err := locks.AssertActiveLease(ctx, lease.Lease, 0); err != nil {
		t.Fatalf("crash point must leave external lease for durable replay: %v", err)
	}
	service.afterAdmissionCleanupEnqueue = nil
	report, err := service.DrainAdmissionCleanup(ctx, "recovery-cleanup", 10)
	if err != nil || report.Claimed != 1 || report.Completed != 1 || report.Retried != 0 {
		t.Fatalf("cleanup replay report=%+v err=%v", report, err)
	}
	if err := locks.AssertActiveLease(ctx, lease.Lease, 0); err == nil {
		t.Fatal("durable cleanup replay left the original external lease active")
	}
	service.mu.Lock()
	record := service.cleanup[lease.Admission.AdmissionID]
	service.mu.Unlock()
	if record.Status != "succeeded" || record.Completed.IsZero() || record.Admission.RunID != lease.Admission.RunID {
		t.Fatalf("cleanup durable record=%+v", record)
	}
}

func TestSessionAdmissionRecoveryExpiryEnqueuesMemoryCleanup(t *testing.T) {
	ctx := context.Background()
	locks := queue.NewMemoryDistributedLockManager()
	service := NewRuntimeSessionAdmissionService(nil, locks)
	lease, err := service.Acquire(ctx, ProductSessionAdmissionCommand{
		Key:             ProductSessionAdmissionKey{TenantID: "tenant-expiry", ThreadID: "thread-expiry", AgentProfile: "agent", ContextGeneration: 1, SessionGeneration: 1},
		BindingID:       "binding-expiry",
		RunID:           "run-expiry",
		OwnerInstanceID: "worker-expiry",
		TTL:             time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	service.Now = func() time.Time { return now }
	service.mu.Lock()
	stored := service.items[lease.Admission.AdmissionID]
	stored.ExpiresAt = now.Add(-time.Second)
	service.items[stored.AdmissionID] = stored
	service.mu.Unlock()
	report, err := service.Recover(ctx, now, 10)
	if err != nil || report.Expired != 1 {
		t.Fatalf("expiry recovery report=%+v err=%v", report, err)
	}
	cleanup, err := service.DrainAdmissionCleanup(ctx, "recovery-expiry", 10)
	if err != nil || cleanup.Completed != 1 {
		t.Fatalf("expiry cleanup report=%+v err=%v", cleanup, err)
	}
	if err := locks.AssertActiveLease(ctx, lease.Lease, 0); err == nil {
		t.Fatal("expired admission cleanup left lease active")
	}
}

func testCapacityCommand(runID string, limit int) RuntimeCapacityCommand {
	dimension := func(key string) RuntimeCapacityDimension {
		return RuntimeCapacityDimension{Key: key, Limit: limit, Requested: 1, Version: 1}
	}
	return RuntimeCapacityCommand{RunID: runID, SnapshotVersion: 1, TTL: time.Minute, Dimensions: RuntimeCapacityDimensions{Model: dimension("model"), AuthPool: dimension("auth"), Tool: dimension("tool"), Tenant: dimension("tenant"), User: dimension("user")}}
}
