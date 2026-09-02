package runtime

import (
	"context"
	"testing"
	"time"
)

func TestRuntimeDispatchAcceptMemoryRequiresExactRuntimeRequestReplay(t *testing.T) {
	ctx := context.Background()
	repository := NewRuntimeHostRepository(nil)
	identity := RuntimeHostIdentity{RuntimeHostID: "host_accept_replay", InstanceID: "instance_accept_replay", Environment: "test"}
	capabilities := runtimeTestCapabilities("cap_accept_replay", CanonicalAgentFacingToolCapability("read", "ready"))
	if _, err := repository.RegisterHost(ctx, identity, RuntimeHostRegistration{
		Endpoint: "http://host-accept-replay", Capabilities: capabilities,
		MaxActiveRuns: 2, MaxProductThreadRuns: 2, MaxDetachedTaskRuns: 2,
	}); err != nil {
		t.Fatalf("register host: %v", err)
	}
	if _, err := repository.HeartbeatHost(ctx, identity, RuntimeHostHeartbeat{
		Sequence: 1, ObservedAt: time.Now().UTC(), CapabilityHash: capabilities.CapabilityHash, SignatureKeyID: "test-key",
	}); err != nil {
		t.Fatalf("heartbeat host: %v", err)
	}

	reservation, _, err := repository.TryReserveSlot(ctx, AtomicReservationCommand{
		ReservationID: "reservation_accept_replay", RunID: "run_accept_replay", OwnerInstanceID: "worker_accept_replay",
		ExecutionScope: "detached_task", CapabilityHash: capabilities.CapabilityHash, RequiredTools: []string{"read"},
		LeaseTokenHash: "sha256:lease_accept_replay", FencingToken: 1, ExpiresAt: time.Now().UTC().Add(time.Minute),
		HeartbeatAfter: time.Now().UTC().Add(-time.Minute),
	})
	if err != nil {
		t.Fatalf("reserve host slot: %v", err)
	}
	if _, err := repository.CreateDispatch(ctx, RuntimeDispatch{
		DispatchID: "dispatch_accept_replay", RunID: reservation.RunID, ReservationID: reservation.ReservationID,
		CapacityReservationID: "capacity_accept_replay", CapacityReservedVersion: 1,
		RuntimeHostID: reservation.RuntimeHostID, DispatchAttempt: 1, PlanVersion: 1, FencingToken: reservation.FencingToken,
		RunTicketJTIHash: "sha256:jti_accept_replay", TicketExpiresAt: time.Now().UTC().Add(time.Minute),
		InputManifestHash: "sha256:manifest_accept_replay", OwnerInstanceID: reservation.OwnerInstanceID,
		LeaseTokenHash: reservation.LeaseTokenHash, LeaseExpiresAt: reservation.ExpiresAt,
	}); err != nil {
		t.Fatalf("create dispatch: %v", err)
	}
	fence := ReservationFence{
		ReservationID: reservation.ReservationID, RuntimeHostID: reservation.RuntimeHostID,
		OwnerInstanceID: reservation.OwnerInstanceID, LeaseTokenHash: reservation.LeaseTokenHash, FencingToken: reservation.FencingToken,
	}
	first := DispatchAcceptedCommand{Fence: fence, DispatchID: "dispatch_accept_replay", RuntimeRequestID: "runtime_request_first"}
	if err := repository.ConfirmDispatchAccepted(ctx, first); err != nil {
		t.Fatalf("first accept: %v", err)
	}
	if err := repository.ConfirmDispatchAccepted(ctx, first); err != nil {
		t.Fatalf("exact accept replay: %v", err)
	}
	dispatch, err := repository.GetDispatch(ctx, first.DispatchID)
	if err != nil || dispatch.RuntimeRequestID != first.RuntimeRequestID {
		t.Fatalf("accepted dispatch runtime request id=%q err=%v", dispatch.RuntimeRequestID, err)
	}
	conflicting := first
	conflicting.RuntimeRequestID = "runtime_request_conflict"
	if err := repository.ConfirmDispatchAccepted(ctx, conflicting); err == nil || err.Error() != "STALE_FENCING_TOKEN" {
		t.Fatalf("conflicting accept replay error=%v, want STALE_FENCING_TOKEN", err)
	}
	stored, err := repository.GetDispatch(ctx, first.DispatchID)
	if err != nil || stored.RuntimeRequestID != first.RuntimeRequestID || stored.State != "accepted" {
		t.Fatalf("conflicting replay changed accepted dispatch=%+v err=%v", stored, err)
	}
}

func TestRuntimeDispatchScannersRetainRuntimeRequestID(t *testing.T) {
	full, err := scanRuntimeDispatchFull(runtimeRequestIDScanner{expectedValues: 35, requestIDIndex: 16, requestID: "runtime_request_full"})
	if err != nil || full.RuntimeRequestID != "runtime_request_full" {
		t.Fatalf("full dispatch scanner runtime request id=%q err=%v", full.RuntimeRequestID, err)
	}
	compact, err := scanRuntimeDispatch(runtimeRequestIDScanner{expectedValues: 18, requestIDIndex: 13, requestID: "runtime_request_compact"})
	if err != nil || compact.RuntimeRequestID != "runtime_request_compact" {
		t.Fatalf("compact dispatch scanner runtime request id=%q err=%v", compact.RuntimeRequestID, err)
	}
}

type runtimeRequestIDScanner struct {
	expectedValues int
	requestIDIndex int
	requestID      string
}

func (s runtimeRequestIDScanner) Scan(values ...any) error {
	if len(values) != s.expectedValues {
		return &runtimeRequestIDScannerCountError{got: len(values), want: s.expectedValues}
	}
	target, ok := values[s.requestIDIndex].(*string)
	if !ok {
		return &runtimeRequestIDScannerTypeError{}
	}
	*target = s.requestID
	return nil
}

type runtimeRequestIDScannerCountError struct{ got, want int }

func (e *runtimeRequestIDScannerCountError) Error() string {
	return "runtime dispatch scanner value count mismatch"
}

type runtimeRequestIDScannerTypeError struct{}

func (*runtimeRequestIDScannerTypeError) Error() string {
	return "runtime dispatch scanner runtime request id target mismatch"
}
