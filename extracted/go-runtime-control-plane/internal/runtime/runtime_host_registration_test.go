package runtime

import (
	"context"
	"testing"
	"time"
)

func TestReregisterHostPreservesHeartbeatSequenceWhenUnoccupied(t *testing.T) {
	ctx := context.Background()
	repository := NewRuntimeHostRepository(nil)
	identity := RuntimeHostIdentity{RuntimeHostID: "host-reregister-empty", InstanceID: "static-instance", Environment: "test"}
	registration := RuntimeHostRegistration{
		Endpoint: "http://host-reregister-empty", Capabilities: runtimeTestCapabilities("cap-v1"), MaxActiveRuns: 2,
	}

	first, err := repository.RegisterHost(ctx, identity, registration)
	if err != nil {
		t.Fatal(err)
	}
	observed := time.Now().UTC().Add(-time.Minute)
	if _, err := repository.HeartbeatHost(ctx, identity, RuntimeHostHeartbeat{
		Sequence: 9, ObservedAt: observed, CapabilityHash: "cap-v1", SignatureKeyID: "test-key",
	}); err != nil {
		t.Fatal(err)
	}

	reregistered, err := repository.RegisterHost(ctx, identity, registration)
	if err != nil {
		t.Fatal(err)
	}
	if reregistered.Status != "registering" || reregistered.RecoveryState != "reconciled" {
		t.Fatalf("unexpected registration state: %+v", reregistered)
	}
	if reregistered.HeartbeatSequence != 9 || !reregistered.LastHeartbeatAt.IsZero() {
		t.Fatalf("heartbeat continuation state is invalid: %+v", reregistered)
	}
	if reregistered.InstanceGeneration != first.InstanceGeneration {
		t.Fatalf("same instance changed instance generation: first=%d current=%d", first.InstanceGeneration, reregistered.InstanceGeneration)
	}

	ready, err := repository.HeartbeatHost(ctx, identity, RuntimeHostHeartbeat{
		Sequence: 10, ObservedAt: time.Now().UTC(), CapabilityHash: "cap-v1", SignatureKeyID: "test-key",
	})
	if err != nil {
		t.Fatalf("continued heartbeat after registration was rejected: %v", err)
	}
	if ready.Status != "ready" || ready.HeartbeatSequence != 10 {
		t.Fatalf("host did not become ready with continued sequence: %+v", ready)
	}
}

func TestReregisterHostPreservesOccupancyAndRequiresRecovery(t *testing.T) {
	ctx := context.Background()
	repository := NewRuntimeHostRepository(nil)
	identity := RuntimeHostIdentity{RuntimeHostID: "host-reregister-busy", InstanceID: "static-instance", Environment: "test"}
	registration := RuntimeHostRegistration{
		Endpoint: "http://host-reregister-busy", Capabilities: runtimeTestCapabilities("cap-v1"),
		MaxActiveRuns: 4, MaxProductThreadRuns: 4, MaxDetachedTaskRuns: 4,
	}
	if _, err := repository.RegisterHost(ctx, identity, registration); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.HeartbeatHost(ctx, identity, RuntimeHostHeartbeat{
		Sequence: 12, ObservedAt: time.Now().UTC(), ActiveRuns: 3, ReservedRuns: 1,
		CapabilityHash: "cap-v1", SignatureKeyID: "test-key",
	}); err != nil {
		t.Fatal(err)
	}

	productReservation, _, err := repository.TryReserveSlot(ctx, AtomicReservationCommand{
		ReservationID: "reservation-product", RunID: "run-product", OwnerInstanceID: "worker",
		ExecutionScope: "product_thread", FencingToken: 1, LeaseTokenHash: "sha256:product", CapabilityHash: "cap-v1",
		ExpiresAt: time.Now().UTC().Add(time.Minute), HeartbeatAfter: time.Now().UTC().Add(-time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	detachedReservation, _, err := repository.TryReserveSlot(ctx, AtomicReservationCommand{
		ReservationID: "reservation-detached", RunID: "run-detached", OwnerInstanceID: "worker",
		ExecutionScope: "detached_task", FencingToken: 2, LeaseTokenHash: "sha256:detached", CapabilityHash: "cap-v1",
		ExpiresAt: time.Now().UTC().Add(time.Minute), HeartbeatAfter: time.Now().UTC().Add(-time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repository.CreateDispatch(ctx, RuntimeDispatch{
		DispatchID: "dispatch-detached", RunID: detachedReservation.RunID, ReservationID: detachedReservation.ReservationID,
		RuntimeHostID: identity.RuntimeHostID, DispatchAttempt: 1, PlanVersion: 1, FencingToken: detachedReservation.FencingToken,
		RunTicketJTIHash: "sha256:jti", TicketExpiresAt: time.Now().UTC().Add(time.Minute), InputManifestHash: "sha256:manifest",
		OwnerInstanceID: "worker", LeaseTokenHash: "sha256:detached", LeaseExpiresAt: time.Now().UTC().Add(time.Minute),
	}); err != nil {
		t.Fatal(err)
	}
	if err := repository.ConfirmDispatchAccepted(ctx, DispatchAcceptedCommand{
		Fence: ReservationFence{
			ReservationID: detachedReservation.ReservationID, RuntimeHostID: identity.RuntimeHostID,
			OwnerInstanceID: "worker", LeaseTokenHash: "sha256:detached", FencingToken: detachedReservation.FencingToken,
		},
		DispatchID: "dispatch-detached", RuntimeRequestID: "request-detached",
	}); err != nil {
		t.Fatal(err)
	}

	before, err := repository.GetHost(ctx, identity.RuntimeHostID)
	if err != nil {
		t.Fatal(err)
	}
	if before.ActiveRuns != 1 || before.ReservedRuns != 1 || before.ActiveDetachedTaskRuns != 1 || before.ReservedProductThreadRuns != 1 {
		t.Fatalf("test occupancy was not established: host=%+v product=%+v", before, productReservation)
	}

	reregistered, err := repository.RegisterHost(ctx, identity, registration)
	if err != nil {
		t.Fatal(err)
	}
	if reregistered.Status != "registering" || reregistered.RecoveryState != "pending" {
		t.Fatalf("occupied host bypassed recovery: %+v", reregistered)
	}
	if reregistered.HeartbeatSequence != 12 || !reregistered.LastHeartbeatAt.IsZero() {
		t.Fatalf("heartbeat continuation state is invalid: %+v", reregistered)
	}
	if reregistered.ActiveRuns != before.ActiveRuns || reregistered.ReservedRuns != before.ReservedRuns ||
		reregistered.ActiveDetachedTaskRuns != before.ActiveDetachedTaskRuns || reregistered.ReservedProductThreadRuns != before.ReservedProductThreadRuns ||
		reregistered.ReportedActiveRuns != before.ReportedActiveRuns || reregistered.ReportedReservedRuns != before.ReportedReservedRuns {
		t.Fatalf("registration changed occupancy counters: before=%+v after=%+v", before, reregistered)
	}

	pending, err := repository.HeartbeatHost(ctx, identity, RuntimeHostHeartbeat{
		Sequence: 13, ObservedAt: time.Now().UTC(), ActiveRuns: 1, ReservedRuns: 1,
		CapabilityHash: "cap-v1", SignatureKeyID: "test-key",
	})
	if err != nil {
		t.Fatalf("continued heartbeat for occupied host was rejected: %v", err)
	}
	if pending.Status != "registering" {
		t.Fatalf("occupied host became ready before reconciliation: %+v", pending)
	}
}

func TestProductionLikeHostNeverBecomesReadyWithoutRecoveryAttestation(t *testing.T) {
	ctx := context.Background()
	repository := NewRuntimeHostRepository(nil)
	identity := RuntimeHostIdentity{RuntimeHostID: "host-prelaunch-recovery", InstanceID: "instance-prelaunch-recovery", Environment: "prelaunch"}
	registration := RuntimeHostRegistration{
		Endpoint: "https://host-prelaunch-recovery.internal:18790", Capabilities: runtimeTestCapabilities("cap-prelaunch"),
		MaxActiveRuns: 2, MaxProductThreadRuns: 2, MaxDetachedTaskRuns: 2,
	}

	registered, err := repository.RegisterHost(ctx, identity, registration)
	if err != nil {
		t.Fatal(err)
	}
	if registered.Status != "registering" || registered.RecoveryState != "pending" {
		t.Fatalf("production-like registration bypassed recovery: %+v", registered)
	}
	heartbeat, err := repository.HeartbeatHost(ctx, identity, RuntimeHostHeartbeat{
		Sequence: 1, ObservedAt: time.Now().UTC(), CapabilityHash: "cap-prelaunch", SignatureKeyID: "test-key",
	})
	if err != nil {
		t.Fatal(err)
	}
	if heartbeat.Status != "registering" || heartbeat.RecoveryState != "pending" {
		t.Fatalf("heartbeat bypassed recovery attestation: %+v", heartbeat)
	}
	if ready, err := repository.ListReadyHosts(ctx, time.Now().UTC().Add(-time.Minute)); err != nil || len(ready) != 0 {
		t.Fatalf("pending production-like Host became schedulable: hosts=%+v err=%v", ready, err)
	}
	if err := repository.RecalculateHostCounters(ctx); err != nil {
		t.Fatal(err)
	}
	afterReconcile, err := repository.GetHost(ctx, identity.RuntimeHostID)
	if err != nil {
		t.Fatal(err)
	}
	if afterReconcile.Status != "registering" || afterReconcile.RecoveryState != "pending" {
		t.Fatalf("counter reconciliation bypassed recovery attestation: %+v", afterReconcile)
	}
	if err := repository.SetHostStatus(ctx, identity.RuntimeHostID, "draining", time.Now().UTC().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := repository.RecalculateHostCounters(ctx); err != nil {
		t.Fatal(err)
	}
	draining, err := repository.GetHost(ctx, identity.RuntimeHostID)
	if err != nil {
		t.Fatal(err)
	}
	if draining.Status != "draining" || draining.RecoveryState != "pending" {
		t.Fatalf("counter reconciliation erased non-admittable Host state: %+v", draining)
	}
}

func TestProductionHostRejectsMissingToolBudgetExecutionContract(t *testing.T) {
	_, err := NewRuntimeHostRepository(nil).RegisterHost(context.Background(), RuntimeHostIdentity{
		RuntimeHostID: "host-missing-tool-budget-contract", InstanceID: "instance-missing-tool-budget-contract", Environment: "prelaunch",
	}, RuntimeHostRegistration{
		Endpoint: "https://host-missing-tool-budget-contract.internal:18790",
		Capabilities: RuntimeCapabilitySnapshot{
			CapabilityHash:        "cap-missing-tool-budget-contract",
			Tools:                 []ToolCapability{CanonicalAgentFacingToolCapability("read", "ready")},
			SubmitBinding:         RuntimeSubmitBindingCapability{Version: RuntimeSubmitBindingV2, ProductSessionHash: true},
			MaxToolCallsSupported: 200, SupportsPerRunBudget: true, SupportsBudgetWarning: true, SupportsForcedAbort: true,
		},
		MaxActiveRuns: 1,
	})
	if err == nil || err.Error() != "RUNTIME_TOOL_BUDGET_UNSUPPORTED" {
		t.Fatalf("missing execution contract error=%v", err)
	}
}

func TestProductionHostRejectsInvalidSubmitBindingBeforeRegistration(t *testing.T) {
	for name, mutate := range map[string]func(*RuntimeCapabilitySnapshot){
		"missing": func(snapshot *RuntimeCapabilitySnapshot) {
			snapshot.SubmitBinding = RuntimeSubmitBindingCapability{}
		},
		"legacy": func(snapshot *RuntimeCapabilitySnapshot) {
			snapshot.SubmitBinding.Version = "runtime_submit_binding.v1"
		},
		"missing_product_session_hash": func(snapshot *RuntimeCapabilitySnapshot) {
			snapshot.SubmitBinding.ProductSessionHash = false
		},
	} {
		t.Run(name, func(t *testing.T) {
			repository := NewRuntimeHostRepository(nil)
			snapshot := runtimeTestCapabilities("cap-submit-binding-" + name)
			mutate(&snapshot)
			identity := RuntimeHostIdentity{RuntimeHostID: "host-submit-binding-" + name, InstanceID: "instance-submit-binding", Environment: "prelaunch"}
			_, err := repository.RegisterHost(context.Background(), identity, RuntimeHostRegistration{
				Endpoint: "https://host-submit-binding.internal:18790", Capabilities: snapshot, MaxActiveRuns: 1,
			})
			if err == nil || err.Error() != "RUNTIME_TOOL_UNAVAILABLE" {
				t.Fatalf("registration error=%v", err)
			}
			if _, err := repository.GetHost(context.Background(), identity.RuntimeHostID); err == nil {
				t.Fatal("rejected Host mutated repository state")
			}
		})
	}
}

func runtimeTestCapabilities(capabilityHash string, tools ...ToolCapability) RuntimeCapabilitySnapshot {
	if len(tools) == 0 {
		tools = []ToolCapability{CanonicalAgentFacingToolCapability("read", "ready")}
	}
	return RuntimeCapabilitySnapshot{
		CapabilityHash: capabilityHash, Tools: tools, MaxToolCallsSupported: 200,
		SubmitBinding:        RuntimeSubmitBindingCapability{Version: RuntimeSubmitBindingV2, ProductSessionHash: true},
		SupportsPerRunBudget: true, SupportsBudgetWarning: true, SupportsForcedAbort: true,
		BudgetExecution: DefaultRuntimeToolBudgetExecutionContract(),
	}
}

func TestRegisterHostRejectsInternalFilesystemCapabilities(t *testing.T) {
	for _, rawTool := range []string{"ls", "find", "grep", "workspace_material_search"} {
		t.Run(rawTool, func(t *testing.T) {
			ctx := context.Background()
			repository := NewRuntimeHostRepository(nil)
			_, err := repository.RegisterHost(ctx, RuntimeHostIdentity{
				RuntimeHostID: "host-raw-tool", InstanceID: "instance-raw-tool", Environment: "test",
			}, RuntimeHostRegistration{
				Endpoint: "http://host-raw-tool", MaxActiveRuns: 1,
				Capabilities: RuntimeCapabilitySnapshot{
					CapabilityHash: "cap-raw-tool",
					Tools:          []ToolCapability{{Name: rawTool, Status: "ready", SchemaHash: rawTool + "_v1"}},
				},
			})
			if err == nil || err.Error() != "RUNTIME_TOOL_UNAVAILABLE" {
				t.Fatalf("raw filesystem capability error=%v, want RUNTIME_TOOL_UNAVAILABLE", err)
			}
			if _, err := repository.GetHost(ctx, "host-raw-tool"); err == nil {
				t.Fatal("rejected Host capability mutated repository state")
			}
		})
	}
}
