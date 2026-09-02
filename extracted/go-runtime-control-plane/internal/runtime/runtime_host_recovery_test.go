package runtime

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestRuntimeHostRecoveryCanonicalEmptyFactsUseJSONArray(t *testing.T) {
	facts, hash, err := canonicalRuntimeHostRecoveryFacts(nil)
	if err != nil {
		t.Fatal(err)
	}
	if facts == nil || len(facts) != 0 {
		t.Fatalf("empty recovery facts must be a non-nil JSON array: %#v", facts)
	}
	if hash != "sha256:7510f339e14d3ad3892a53b628c62491a82cddd447a1437c227e10df88559f5a" {
		t.Fatalf("empty recovery fact hash=%s", hash)
	}
	payload, err := json.Marshal(struct {
		Version string                    `json:"version"`
		Facts   []RuntimeHostRecoveryFact `json:"facts"`
	}{Version: "runtime-host-recovery.v1", Facts: facts})
	if err != nil {
		t.Fatal(err)
	}
	if string(payload) != `{"version":"runtime-host-recovery.v1","facts":[]}` {
		t.Fatalf("empty recovery payload=%s", payload)
	}
}

func TestRuntimeHostRecoveryCanonicalFactsAreOrderIndependent(t *testing.T) {
	first := RuntimeHostRecoveryFact{
		RunID: "run-a", RuntimeHostID: "host-order", AssignedRuntimeHostInstanceID: "instance-order",
		AssignedRuntimeHostInstanceGeneration: 1, ReservationID: "reservation-a", FencingToken: 1,
		ExecutionScope: "detached_task", CapabilityHash: "cap-order", Status: "reserved",
	}
	second := RuntimeHostRecoveryFact{
		RunID: "run-b", RuntimeHostID: "host-order", AssignedRuntimeHostInstanceID: "instance-order",
		AssignedRuntimeHostInstanceGeneration: 1, ReservationID: "reservation-b", FencingToken: 2,
		ExecutionScope: "detached_task", CapabilityHash: "cap-order", Status: "running",
	}

	canonical, canonicalHash, err := canonicalRuntimeHostRecoveryFacts([]RuntimeHostRecoveryFact{first, second})
	if err != nil {
		t.Fatalf("canonical facts: %v", err)
	}
	reversed, reversedHash, err := canonicalRuntimeHostRecoveryFacts([]RuntimeHostRecoveryFact{second, first})
	if err != nil {
		t.Fatalf("reversed facts: %v", err)
	}
	if canonicalHash != reversedHash || !runtimeHostRecoveryFactsEqual(canonical, reversed) {
		t.Fatalf("canonicalization drift: canonical=%+v reversed=%+v hashes=%s/%s", canonical, reversed, canonicalHash, reversedHash)
	}
	if canonical[0].RunID != "run-a" || canonical[1].RunID != "run-b" {
		t.Fatalf("facts are not in canonical order: %+v", canonical)
	}
	normalized, err := NormalizeRuntimeHostRecoverySnapshot(RuntimeHostPrincipal{
		RuntimeHostID: "host-order", InstanceID: "instance-order", Environment: "test",
	}, RuntimeHostRecoverySnapshot{
		RuntimeHostID: "host-order", InstanceID: "instance-order", Environment: "test",
		InstanceGeneration: 1, RecoveryRevision: 1, RecoveryState: "pending",
		Facts: []RuntimeHostRecoveryFact{second, first}, FactSetHash: canonicalHash,
	})
	if err != nil || !runtimeHostRecoveryFactsEqual(normalized.Facts, canonical) {
		t.Fatalf("external snapshot was not normalized: snapshot=%+v err=%v", normalized, err)
	}
}

func TestRuntimeHostRecoveryAttestationCASAndIdempotency(t *testing.T) {
	ctx := context.Background()
	repository := NewRuntimeHostRepository(nil)
	identity := RuntimeHostIdentity{RuntimeHostID: "host-recovery", InstanceID: "instance-recovery", Environment: "test"}
	registration := recoveryTestRegistration("cap-recovery")
	if _, err := repository.RegisterHost(ctx, identity, registration); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.HeartbeatHost(ctx, identity, RuntimeHostHeartbeat{
		Sequence: 1, ObservedAt: time.Now().UTC(), CapabilityHash: registration.Capabilities.CapabilityHash, SignatureKeyID: "test-key",
	}); err != nil {
		t.Fatal(err)
	}
	reservation, _, err := repository.TryReserveSlot(ctx, AtomicReservationCommand{
		ReservationID: "reservation-recovery", RunID: "run-recovery", OwnerInstanceID: "worker-recovery",
		ExecutionScope: "detached_task", CapabilityHash: registration.Capabilities.CapabilityHash, RequiredTools: []string{"read"},
		LeaseTokenHash: "sha256:lease-recovery", FencingToken: 9, ExpiresAt: time.Now().UTC().Add(time.Minute), HeartbeatAfter: time.Now().UTC().Add(-time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	dispatch, err := repository.CreateDispatch(ctx, RuntimeDispatch{
		DispatchID: "dispatch-recovery", RunID: reservation.RunID, ReservationID: reservation.ReservationID, RuntimeHostID: identity.RuntimeHostID,
		DispatchAttempt: 1, PlanVersion: 1, FencingToken: reservation.FencingToken, RunTicketJTIHash: recoveryTestJTIHash,
		TicketExpiresAt: time.Now().UTC().Add(time.Minute), InputManifestHash: recoveryTestManifestHash,
		OwnerInstanceID: reservation.OwnerInstanceID, LeaseTokenHash: reservation.LeaseTokenHash, LeaseExpiresAt: reservation.ExpiresAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	if reservation.AssignedRuntimeHostInstanceID != identity.InstanceID || reservation.AssignedRuntimeHostInstanceGeneration < 1 ||
		dispatch.AssignedRuntimeHostInstanceID != reservation.AssignedRuntimeHostInstanceID ||
		dispatch.AssignedRuntimeHostInstanceGeneration != reservation.AssignedRuntimeHostInstanceGeneration || dispatch.DispatchIdentity == "" {
		t.Fatalf("dispatch was not bound to the reservation Host process: reservation=%+v dispatch=%+v", reservation, dispatch)
	}

	// Re-registration with occupied Backend facts is the test/local equivalent
	// of a restarted Adapter: it closes admission before recovery starts.
	if _, err := repository.RegisterHost(ctx, identity, registration); err != nil {
		t.Fatal(err)
	}
	snapshot, err := repository.GetHostRecoverySnapshot(ctx, identity)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.RecoveryState != "pending" || snapshot.FactSetHash == "" || len(snapshot.Facts) != 1 {
		t.Fatalf("unexpected recovery snapshot: %+v", snapshot)
	}
	fact := snapshot.Facts[0]
	if fact.DispatchID != dispatch.DispatchID || fact.RunTicketJTIHash != recoveryTestJTIHash || fact.ManifestHash != recoveryTestManifestHash ||
		strings.Contains(fact.DispatchIdentity, "lease") || strings.Contains(fact.DispatchIdentity, "ticket") {
		t.Fatalf("snapshot leaked or omitted recovery binding: %+v", fact)
	}

	first, err := repository.BeginHostRecoveryAttestation(ctx, identity, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := repository.BeginHostRecoveryAttestation(ctx, identity, snapshot)
	if err != nil || replayed.AttestationID != first.AttestationID || replayed.State != runtimeHostRecoveryPrepared {
		t.Fatalf("begin idempotency result=%+v err=%v", replayed, err)
	}

	if err := repository.MarkDispatchSent(ctx, dispatch.DispatchID, ReservationFence{
		ReservationID: reservation.ReservationID, RuntimeHostID: identity.RuntimeHostID, OwnerInstanceID: reservation.OwnerInstanceID,
		LeaseTokenHash: reservation.LeaseTokenHash, FencingToken: reservation.FencingToken,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.CompleteHostRecoveryAttestation(ctx, identity, first.AttestationID); err == nil || !strings.Contains(err.Error(), "RUNTIME_CAPACITY_UNAVAILABLE") {
		t.Fatalf("fact mutation completed stale attestation: %v", err)
	}
	pending, err := repository.GetHost(ctx, identity.RuntimeHostID)
	if err != nil || pending.Status != "registering" || pending.RecoveryState != "pending" {
		t.Fatalf("stale completion opened Host: host=%+v err=%v", pending, err)
	}

	updatedSnapshot, err := repository.GetHostRecoverySnapshot(ctx, identity)
	if err != nil {
		t.Fatal(err)
	}
	second, err := repository.BeginHostRecoveryAttestation(ctx, identity, updatedSnapshot)
	if err != nil || second.AttestationID == first.AttestationID {
		t.Fatalf("changed fact set did not create a new prepared attestation: second=%+v err=%v", second, err)
	}
	completed, err := repository.CompleteHostRecoveryAttestation(ctx, identity, second.AttestationID)
	if err != nil || completed.State != runtimeHostRecoveryCompleted {
		t.Fatalf("complete result=%+v err=%v", completed, err)
	}
	replayedComplete, err := repository.CompleteHostRecoveryAttestation(ctx, identity, second.AttestationID)
	if err != nil || replayedComplete.State != runtimeHostRecoveryCompleted {
		t.Fatalf("complete idempotency result=%+v err=%v", replayedComplete, err)
	}
	ready, err := repository.GetHost(ctx, identity.RuntimeHostID)
	if err != nil || ready.Status != "ready" || ready.RecoveryState != "reconciled" {
		t.Fatalf("completed recovery did not atomically open Host: host=%+v err=%v", ready, err)
	}
}

func TestRuntimeHostRecoveryRejectsEventGapAndProductionMemoryFallback(t *testing.T) {
	ctx := context.Background()
	repository := NewRuntimeHostRepository(nil)
	identity := RuntimeHostIdentity{RuntimeHostID: "host-recovery-gap", InstanceID: "instance-recovery-gap", Environment: "test"}
	registration := recoveryTestRegistration("cap-recovery-gap")
	if _, err := repository.RegisterHost(ctx, identity, registration); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.HeartbeatHost(ctx, identity, RuntimeHostHeartbeat{Sequence: 1, ObservedAt: time.Now().UTC(), CapabilityHash: registration.Capabilities.CapabilityHash, SignatureKeyID: "test-key"}); err != nil {
		t.Fatal(err)
	}
	reservation, _, err := repository.TryReserveSlot(ctx, AtomicReservationCommand{
		ReservationID: "reservation-recovery-gap", RunID: "run-recovery-gap", OwnerInstanceID: "worker-recovery-gap",
		ExecutionScope: "detached_task", CapabilityHash: registration.Capabilities.CapabilityHash, RequiredTools: []string{"read"},
		LeaseTokenHash: "sha256:lease-recovery-gap", FencingToken: 10, ExpiresAt: time.Now().UTC().Add(time.Minute), HeartbeatAfter: time.Now().UTC().Add(-time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	dispatch, err := repository.CreateDispatch(ctx, RuntimeDispatch{
		DispatchID: "dispatch-recovery-gap", RunID: reservation.RunID, ReservationID: reservation.ReservationID, RuntimeHostID: identity.RuntimeHostID,
		DispatchAttempt: 1, PlanVersion: 1, FencingToken: reservation.FencingToken, RunTicketJTIHash: recoveryTestJTIHash,
		TicketExpiresAt: time.Now().UTC().Add(time.Minute), InputManifestHash: recoveryTestManifestHash,
		OwnerInstanceID: reservation.OwnerInstanceID, LeaseTokenHash: reservation.LeaseTokenHash, LeaseExpiresAt: reservation.ExpiresAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repository.RegisterHost(ctx, identity, registration); err != nil {
		t.Fatal(err)
	}
	repository.mu.Lock()
	item := repository.dispatches[dispatch.DispatchID]
	item.EventUpperBound = 1
	repository.dispatches[dispatch.DispatchID] = item
	repository.mu.Unlock()
	if _, err := repository.GetHostRecoverySnapshot(ctx, identity); err == nil || !strings.Contains(err.Error(), "RUNTIME_EVENT_GAP") {
		t.Fatalf("event gap was treated as recoverable snapshot: %v", err)
	}

	productionRepository := NewRuntimeHostRepository(nil)
	productionIdentity := RuntimeHostIdentity{RuntimeHostID: "host-recovery-production", InstanceID: "instance-recovery-production", Environment: "prelaunch"}
	if _, err := productionRepository.RegisterHost(ctx, productionIdentity, recoveryTestRegistration("cap-recovery-production")); err != nil {
		t.Fatal(err)
	}
	if _, err := productionRepository.GetHostRecoverySnapshot(ctx, productionIdentity); err == nil || !strings.Contains(err.Error(), "RUNTIME_STORAGE_UNAVAILABLE") {
		t.Fatalf("production-like recovery used memory fallback: %v", err)
	}
}

func recoveryTestRegistration(capabilityHash string) RuntimeHostRegistration {
	return RuntimeHostRegistration{
		Endpoint: "http://runtime-recovery.test", RuntimeVersion: "test", AdapterVersion: "test", SessionStoreID: "store-test",
		Capabilities:  runtimeTestCapabilities(capabilityHash, CanonicalAgentFacingToolCapability("read", "ready")),
		MaxActiveRuns: 4, MaxProductThreadRuns: 4, MaxDetachedTaskRuns: 4,
	}
}

const (
	recoveryTestJTIHash      = "sha256:1111111111111111111111111111111111111111111111111111111111111111"
	recoveryTestManifestHash = "sha256:2222222222222222222222222222222222222222222222222222222222222222"
)
