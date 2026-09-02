package runtime

import (
	"context"
	"testing"
	"time"
)

func TestRecalculateHostCountersPreservesProductionRecoveryState(t *testing.T) {
	ctx := context.Background()
	repository := NewRuntimeHostRepository(nil)
	identity := RuntimeHostIdentity{RuntimeHostID: "host_recovery_fence", InstanceID: "instance_recovery_fence", Environment: "prelaunch"}
	if _, err := repository.RegisterHost(ctx, identity, recoveryTestRegistration("cap-recovery-fence")); err != nil {
		t.Fatal(err)
	}

	// Counter repair owns only counter fields. A completed recovery must remain
	// schedulable until a new registration or recovery boundary changes it.
	repository.mu.Lock()
	host := repository.hosts[identity.RuntimeHostID]
	host.Status, host.RecoveryState = "ready", "reconciled"
	repository.hosts[identity.RuntimeHostID] = host
	repository.mu.Unlock()
	if err := repository.RecalculateHostCounters(ctx); err != nil {
		t.Fatal(err)
	}
	ready, err := repository.GetHost(ctx, identity.RuntimeHostID)
	if err != nil || ready.Status != "ready" || ready.RecoveryState != "reconciled" {
		t.Fatalf("counter repair reopened recovery fence: host=%+v err=%v", ready, err)
	}

	// A pending recovery likewise stays pending; recalculation cannot complete
	// or replace it based on a derived empty occupancy view.
	repository.mu.Lock()
	host = repository.hosts[identity.RuntimeHostID]
	host.Status, host.RecoveryState = "registering", "pending"
	repository.hosts[identity.RuntimeHostID] = host
	repository.mu.Unlock()
	if err := repository.RecalculateHostCounters(ctx); err != nil {
		t.Fatal(err)
	}
	pending, err := repository.GetHost(ctx, identity.RuntimeHostID)
	if err != nil || pending.Status != "registering" || pending.RecoveryState != "pending" {
		t.Fatalf("counter repair changed pending recovery: host=%+v err=%v", pending, err)
	}
}

func TestRecalculateHostCountersKeepsLegacyUnclassifiedReservationOutOfAdmission(t *testing.T) {
	ctx := context.Background()
	repository := NewRuntimeHostRepository(nil)
	identity := RuntimeHostIdentity{RuntimeHostID: "host_legacy_unclassified", InstanceID: "instance_legacy_unclassified", Environment: "test"}
	if _, err := repository.RegisterHost(ctx, identity, RuntimeHostRegistration{
		Endpoint: "http://host_legacy_unclassified", Capabilities: runtimeTestCapabilities("cap-legacy-unclassified"), MaxActiveRuns: 2,
	}); err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC()
	if _, err := repository.HeartbeatHost(ctx, identity, RuntimeHostHeartbeat{Sequence: 1, ObservedAt: base, CapabilityHash: "cap-legacy-unclassified", SignatureKeyID: "test-key"}); err != nil {
		t.Fatal(err)
	}
	reservation, _, err := repository.TryReserveSlot(ctx, AtomicReservationCommand{
		ReservationID: "reservation_legacy_unclassified", RunID: "run_legacy_unclassified", AffinityRuntimeHostID: identity.RuntimeHostID,
		OwnerInstanceID: "worker", ExecutionScope: "detached_task", FencingToken: 1, LeaseTokenHash: "sha256:lease",
		CapabilityHash: "cap-legacy-unclassified", ExpiresAt: base.Add(time.Minute), HeartbeatAfter: base.Add(-time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	if reservation.ExecutionScopeSource != runtimeExecutionScopeSourceExplicit {
		t.Fatalf("new reservation source=%q", reservation.ExecutionScopeSource)
	}

	repository.mu.Lock()
	legacy := repository.reservations[reservation.ReservationID]
	legacy.ExecutionScopeSource = runtimeExecutionScopeSourceLegacyUnclassified
	repository.reservations[reservation.ReservationID] = legacy
	repository.mu.Unlock()
	if err := repository.RecalculateHostCounters(ctx); err != nil {
		t.Fatal(err)
	}
	pending, err := repository.GetHost(ctx, identity.RuntimeHostID)
	if err != nil || pending.RecoveryState != "pending" || pending.ReservedRuns != 1 || pending.ReservedDetachedTaskRuns != 1 {
		t.Fatalf("legacy reservation did not preserve pending occupancy: host=%+v err=%v", pending, err)
	}
	if _, _, err := repository.TryReserveSlot(ctx, AtomicReservationCommand{
		ReservationID: "reservation_should_not_admit", RunID: "run_should_not_admit", AffinityRuntimeHostID: identity.RuntimeHostID,
		OwnerInstanceID: "worker", ExecutionScope: "detached_task", FencingToken: 2, LeaseTokenHash: "sha256:lease-2",
		CapabilityHash: "cap-legacy-unclassified", ExpiresAt: base.Add(time.Minute), HeartbeatAfter: base.Add(-time.Minute),
	}); err == nil || err.Error() != "RUNTIME_CAPACITY_UNAVAILABLE" {
		t.Fatalf("legacy reservation admitted a second run: %v", err)
	}

	repository.mu.Lock()
	classified := repository.reservations[reservation.ReservationID]
	classified.ExecutionScopeSource = "agent_run_backfill"
	repository.reservations[reservation.ReservationID] = classified
	repository.mu.Unlock()
	if err := repository.RecalculateHostCounters(ctx); err != nil {
		t.Fatal(err)
	}
	reconciled, err := repository.GetHost(ctx, identity.RuntimeHostID)
	if err != nil || reconciled.RecoveryState != "reconciled" {
		t.Fatalf("classified reservation did not restore reconciliation: host=%+v err=%v", reconciled, err)
	}
	if _, _, err := repository.TryReserveSlot(ctx, AtomicReservationCommand{
		ReservationID: "reservation_after_classification", RunID: "run_after_classification", AffinityRuntimeHostID: identity.RuntimeHostID,
		OwnerInstanceID: "worker", ExecutionScope: "detached_task", FencingToken: 2, LeaseTokenHash: "sha256:lease-2",
		CapabilityHash: "cap-legacy-unclassified", ExpiresAt: base.Add(time.Minute), HeartbeatAfter: base.Add(-time.Minute),
	}); err != nil {
		t.Fatalf("classified reservation remained excluded from admission: %v", err)
	}
}

func TestRuntimeReservationFromMapPreservesExecutionScopeSource(t *testing.T) {
	reservation, err := runtimeReservationFromMap(map[string]any{
		"reservation_id": "reservation_pg_source", "run_id": "run_pg_source", "runtime_host_id": "host_pg_source",
		"owner_instance_id": "worker", "state": "reserved", "fencing_token": int64(1), "lease_token_hash": "sha256:lease",
		"capability_hash": "cap-pg-source", "execution_scope": "detached_task", "execution_scope_source": runtimeExecutionScopeSourceLegacyUnclassified,
		"dispatch_id": "", "version": int64(1),
	})
	if err != nil {
		t.Fatal(err)
	}
	if reservation.ExecutionScopeSource != runtimeExecutionScopeSourceLegacyUnclassified {
		t.Fatalf("execution scope source=%q", reservation.ExecutionScopeSource)
	}
}

func TestAcceptedReservationDoesNotExpireFromAdmissionTTL(t *testing.T) {
	repository := NewRuntimeHostRepository(nil)
	identity := RuntimeHostIdentity{RuntimeHostID: "host_active", InstanceID: "instance_active", Environment: "test"}
	if _, err := repository.RegisterHost(context.Background(), identity, RuntimeHostRegistration{
		Endpoint: "http://host_active", Capabilities: runtimeTestCapabilities("cap-v1"), MaxActiveRuns: 2,
	}); err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC()
	if _, err := repository.HeartbeatHost(context.Background(), identity, RuntimeHostHeartbeat{Sequence: 1, ObservedAt: base, CapabilityHash: "cap-v1", SignatureKeyID: "test-key"}); err != nil {
		t.Fatal(err)
	}
	reservation, _, err := repository.TryReserveSlot(context.Background(), AtomicReservationCommand{
		ReservationID: "reservation_active", RunID: "run_active", AffinityRuntimeHostID: "host_active", OwnerInstanceID: "worker",
		ExecutionScope: "detached_task", FencingToken: 1, LeaseTokenHash: "sha256:lease", CapabilityHash: "cap-v1",
		ExpiresAt: base.Add(time.Second), HeartbeatAfter: base.Add(-time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repository.CreateDispatch(context.Background(), RuntimeDispatch{
		DispatchID: "dispatch_active", RunID: "run_active", ReservationID: reservation.ReservationID, RuntimeHostID: "host_active",
		CapacityReservationID: "capacity_active", CapacityReservedVersion: 1,
		DispatchAttempt: 1, PlanVersion: 1, FencingToken: reservation.FencingToken, RunTicketJTIHash: "sha256:jti",
		TicketExpiresAt: base.Add(time.Minute), InputManifestHash: "sha256:manifest", OwnerInstanceID: "worker",
		LeaseTokenHash: "sha256:lease", LeaseExpiresAt: base.Add(time.Minute),
	}); err != nil {
		t.Fatal(err)
	}
	if err := repository.ConfirmDispatchAccepted(context.Background(), DispatchAcceptedCommand{
		Fence:      ReservationFence{ReservationID: reservation.ReservationID, RuntimeHostID: "host_active", OwnerInstanceID: "worker", LeaseTokenHash: "sha256:lease", FencingToken: reservation.FencingToken},
		DispatchID: "dispatch_active", RuntimeRequestID: "request_active",
	}); err != nil {
		t.Fatal(err)
	}
	count, err := repository.ExpireReservations(context.Background(), base.Add(time.Minute))
	if err != nil || count != 0 {
		t.Fatalf("accepted reservation expired count=%d err=%v", count, err)
	}
	stored, err := repository.GetReservation(context.Background(), reservation.ReservationID)
	if err != nil || stored.State != "accepted" {
		t.Fatalf("reservation=%+v err=%v", stored, err)
	}
}

func TestSubmitUnknownReservationWaitsForSameHostRecovery(t *testing.T) {
	ctx := context.Background()
	repository := NewRuntimeHostRepository(nil)
	identity := RuntimeHostIdentity{RuntimeHostID: "host_submit_unknown", InstanceID: "instance_submit_unknown", Environment: "test"}
	if _, err := repository.RegisterHost(ctx, identity, RuntimeHostRegistration{
		Endpoint: "http://host_submit_unknown", Capabilities: runtimeTestCapabilities("cap-v1"), MaxActiveRuns: 2,
	}); err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC()
	if _, err := repository.HeartbeatHost(ctx, identity, RuntimeHostHeartbeat{Sequence: 1, ObservedAt: base, CapabilityHash: "cap-v1", SignatureKeyID: "test-key"}); err != nil {
		t.Fatal(err)
	}
	reservation, _, err := repository.TryReserveSlot(ctx, AtomicReservationCommand{
		ReservationID: "reservation_submit_unknown", RunID: "run_submit_unknown", AffinityRuntimeHostID: identity.RuntimeHostID,
		OwnerInstanceID: "worker", ExecutionScope: "detached_task", FencingToken: 1, LeaseTokenHash: "sha256:lease",
		CapabilityHash: "cap-v1", ExpiresAt: base.Add(time.Second), HeartbeatAfter: base.Add(-time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	dispatchID := "dispatch_submit_unknown"
	if _, err := repository.CreateDispatch(ctx, RuntimeDispatch{
		DispatchID: dispatchID, RunID: reservation.RunID, ReservationID: reservation.ReservationID, RuntimeHostID: identity.RuntimeHostID,
		CapacityReservationID: "capacity_submit_unknown", CapacityReservedVersion: 1,
		DispatchAttempt: 1, PlanVersion: 1, FencingToken: reservation.FencingToken, RunTicketJTIHash: "sha256:jti",
		TicketExpiresAt: base.Add(time.Minute), InputManifestHash: "sha256:manifest", OwnerInstanceID: "worker",
		LeaseTokenHash: "sha256:lease", LeaseExpiresAt: base.Add(time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	fence := ReservationFence{ReservationID: reservation.ReservationID, RuntimeHostID: identity.RuntimeHostID, OwnerInstanceID: "worker", LeaseTokenHash: "sha256:lease", FencingToken: reservation.FencingToken}
	if err := repository.MarkDispatchSent(ctx, dispatchID, fence); err != nil {
		t.Fatal(err)
	}
	if err := repository.MarkDispatchSubmitUnknown(ctx, dispatchID, fence, base.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	count, err := repository.ExpireReservations(ctx, base.Add(time.Minute))
	if err != nil || count != 0 {
		t.Fatalf("submit_unknown reservation expired count=%d err=%v", count, err)
	}
	stored, err := repository.GetReservation(ctx, reservation.ReservationID)
	if err != nil || stored.State != "reserved" {
		t.Fatalf("reservation=%+v err=%v", stored, err)
	}
}
