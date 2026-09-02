package runtime

import (
	"context"
	"testing"
	"time"

	"huahuoai/backend/source/internal/queue"
)

func TestRuntimeSchedulerAcceptDoesNotLeaveHostAcceptedWhenCapacityExpiresAtCommit(t *testing.T) {
	ctx := context.Background()
	hosts := NewRuntimeHostRepository(nil)
	identity := RuntimeHostIdentity{RuntimeHostID: "host-capacity-accept-race", InstanceID: "instance-capacity-accept-race", Environment: "test"}
	capabilities := runtimeTestCapabilities("cap-capacity-accept-race", CanonicalAgentFacingToolCapability("read", "ready"))
	if _, err := hosts.RegisterHost(ctx, identity, RuntimeHostRegistration{
		Endpoint: "http://host-capacity-accept-race", Capabilities: capabilities, MaxActiveRuns: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := hosts.HeartbeatHost(ctx, identity, RuntimeHostHeartbeat{
		Sequence: 1, ObservedAt: time.Now().UTC(), CapabilityHash: capabilities.CapabilityHash, SignatureKeyID: "test-key",
	}); err != nil {
		t.Fatal(err)
	}

	locks := queue.NewMemoryDistributedLockManager()
	capacity := NewRuntimeCapacityAdmissionService(nil)
	scheduler := NewRuntimeSchedulerWithAdmissions(hosts, locks, NewRuntimeSessionAdmissionService(nil, locks), capacity)
	base := time.Date(2026, time.July, 17, 9, 0, 0, 0, time.UTC)
	capacity.Now = func() time.Time { return base }
	capacityReservation, err := capacity.Reserve(ctx, testCapacityCommand("run-capacity-accept-race", 1))
	if err != nil {
		t.Fatal(err)
	}

	handle, err := scheduler.Reserve(ctx, ScheduleCommand{
		RunID: capacityReservation.RunID, OwnerInstanceID: "worker-capacity-accept-race", ExecutionScope: "detached_task",
		CapabilityHash: capabilities.CapabilityHash, RequiredTools: []string{"read"}, CapacityReservation: capacityReservation, ReservationTTL: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	dispatchID := "dispatch-capacity-accept-race"
	if _, err := hosts.CreateDispatch(ctx, RuntimeDispatch{
		DispatchID: dispatchID, RunID: handle.Reservation.RunID, ReservationID: handle.Reservation.ReservationID,
		RuntimeHostID: handle.Reservation.RuntimeHostID, DispatchAttempt: 1, PlanVersion: 1,
		FencingToken: handle.Reservation.FencingToken, RunTicketJTIHash: "sha256:capacity-accept-race",
		TicketExpiresAt: time.Now().UTC().Add(time.Minute), InputManifestHash: "sha256:capacity-accept-race",
		OwnerInstanceID: handle.Reservation.OwnerInstanceID, LeaseTokenHash: handle.Reservation.LeaseTokenHash,
		LeaseExpiresAt: handle.Reservation.ExpiresAt,
	}); err != nil {
		t.Fatal(err)
	}

	capacityChecks := 0
	capacity.Now = func() time.Time {
		capacityChecks++
		if capacityChecks == 1 {
			return base.Add(time.Second)
		}
		return base.Add(time.Minute)
	}
	if err := scheduler.Accept(ctx, handle, dispatchID, "runtime-request-capacity-accept-race"); err == nil || err.Error() != "RUNTIME_CAPACITY_UNAVAILABLE" {
		t.Fatalf("accept error=%v", err)
	}

	dispatch, err := hosts.GetDispatch(ctx, dispatchID)
	if err != nil {
		t.Fatal(err)
	}
	if dispatch.State == "accepted" {
		t.Fatalf("capacity rejection left dispatch accepted: %+v", dispatch)
	}
	reservation, err := hosts.GetReservation(ctx, handle.Reservation.ReservationID)
	if err != nil {
		t.Fatal(err)
	}
	if reservation.State == "accepted" {
		t.Fatalf("capacity rejection left host reservation accepted: %+v", reservation)
	}
}
