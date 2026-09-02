package runtime

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestRuntimeCapacityLookupNormalizesNoRows(t *testing.T) {
	if got := runtimeCapacityLookupError(pgx.ErrNoRows); got == nil || got.Error() != "NOT_FOUND" {
		t.Fatalf("pgx no rows must map to NOT_FOUND, got %v", got)
	}
	input := errors.New("database unavailable")
	if got := runtimeCapacityLookupError(input); !errors.Is(got, input) {
		t.Fatalf("non-empty lookup error must be preserved, got %v", got)
	}
}

func TestRuntimeCapacityRecoverExpiresRecoveringButNotAccepted(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.July, 17, 9, 0, 0, 0, time.UTC)
	recoveringService := NewRuntimeCapacityAdmissionService(nil)
	recoveringService.Now = func() time.Time { return now }
	reserved, err := recoveringService.Reserve(ctx, testCapacityCommand("run-reserved", 2))
	if err != nil {
		t.Fatal(err)
	}
	recovering, err := recoveringService.Reserve(ctx, testCapacityCommand("run-recovering", 2))
	if err != nil {
		t.Fatal(err)
	}
	recoveringService.mu.Lock()
	reservedItem := recoveringService.items[reserved.ReservationID]
	reservedItem.ExpiresAt = now.Add(-time.Second)
	recoveringService.items[reserved.ReservationID] = reservedItem
	recoveringItem := recoveringService.items[recovering.ReservationID]
	recoveringItem.State = "recovering"
	recoveringItem.ExpiresAt = now.Add(-time.Second)
	recoveringService.items[recovering.ReservationID] = recoveringItem
	recoveringService.mu.Unlock()
	recovered, err := recoveringService.Recover(ctx, now, 10)
	if err != nil || recovered.Scanned != 2 || recovered.Expired != 2 {
		t.Fatalf("recovering expiry report=%+v err=%v", recovered, err)
	}
	latestReserved, err := recoveringService.GetLatestByRunID(ctx, reserved.RunID)
	if err != nil || latestReserved.State != "expired" {
		t.Fatalf("reserved lease state=%q err=%v", latestReserved.State, err)
	}
	latestRecovering, err := recoveringService.GetLatestByRunID(ctx, recovering.RunID)
	if err != nil || latestRecovering.State != "expired" {
		t.Fatalf("recovering lease state=%q err=%v", latestRecovering.State, err)
	}

	acceptedService := NewRuntimeCapacityAdmissionService(nil)
	acceptedService.Now = func() time.Time { return now }
	accepted, err := acceptedService.Reserve(ctx, testCapacityCommand("run-accepted", 1))
	if err != nil {
		t.Fatal(err)
	}
	if err := acceptedService.CommitAccepted(ctx, accepted); err != nil {
		t.Fatal(err)
	}
	acceptedService.mu.Lock()
	acceptedItem := acceptedService.items[accepted.ReservationID]
	acceptedItem.ExpiresAt = now.Add(-time.Second)
	acceptedService.items[accepted.ReservationID] = acceptedItem
	acceptedService.mu.Unlock()
	recovered, err = acceptedService.Recover(ctx, now, 10)
	if err != nil || recovered.Expired != 0 {
		t.Fatalf("accepted lease must not expire by clock: report=%+v err=%v", recovered, err)
	}
	latestAccepted, err := acceptedService.GetLatestByRunID(ctx, accepted.RunID)
	if err != nil || latestAccepted.State != "accepted" {
		t.Fatalf("accepted lease state=%q err=%v", latestAccepted.State, err)
	}
}

func TestRuntimeCapacityCommitAcceptedDoesNotReviveReleasedMemoryReservation(t *testing.T) {
	ctx := context.Background()
	service := NewRuntimeCapacityAdmissionService(nil)
	command := testCapacityCommand("run-release-race", 1)
	reservation, err := service.Reserve(ctx, command)
	if err != nil {
		t.Fatal(err)
	}
	changed, err := service.Release(ctx, reservation, nil)
	if err != nil || !changed {
		t.Fatalf("release changed=%v err=%v", changed, err)
	}
	if err := service.CommitAccepted(ctx, reservation); err == nil || err.Error() != "RUNTIME_CAPACITY_UNAVAILABLE" {
		t.Fatalf("released reservation was accepted again: %v", err)
	}
	latest, err := service.GetLatestByRunID(ctx, reservation.RunID)
	if err != nil || latest.State != "released" {
		t.Fatalf("released reservation was revived: state=%q err=%v", latest.State, err)
	}
	renewed, err := service.Reserve(ctx, command)
	if err != nil || renewed.Version <= reservation.Version {
		t.Fatalf("renewed reservation=%+v err=%v", renewed, err)
	}
	if err := service.CommitAccepted(ctx, reservation); err == nil || err.Error() != "RUNTIME_CAPACITY_UNAVAILABLE" {
		t.Fatalf("old reservation accepted renewed generation: %v", err)
	}
	latest, err = service.GetLatestByRunID(ctx, renewed.RunID)
	if err != nil || latest.State != "reserved" || latest.Version != renewed.Version {
		t.Fatalf("old generation mutated renewal: latest=%+v err=%v", latest, err)
	}
	if err := service.CommitAccepted(ctx, renewed); err != nil {
		t.Fatal(err)
	}
	accepted, err := service.GetLatestByRunID(ctx, renewed.RunID)
	if err != nil || accepted.State != "accepted" || accepted.Version != renewed.Version+1 {
		t.Fatalf("accepted reservation=%+v err=%v", accepted, err)
	}
	if err := service.CommitAccepted(ctx, renewed); err != nil {
		t.Fatalf("accepted replay was not idempotent: %v", err)
	}
	replayed, err := service.GetLatestByRunID(ctx, renewed.RunID)
	if err != nil || replayed.Version != accepted.Version {
		t.Fatalf("accepted replay changed reservation=%+v err=%v", replayed, err)
	}

	recoveringService := NewRuntimeCapacityAdmissionService(nil)
	recoveringReservation, err := recoveringService.Reserve(ctx, testCapacityCommand("run-accept-recovering", 1))
	if err != nil {
		t.Fatal(err)
	}
	recoveringService.mu.Lock()
	recoveringItem := recoveringService.items[recoveringReservation.ReservationID]
	recoveringItem.State = "recovering"
	recoveringService.items[recoveringReservation.ReservationID] = recoveringItem
	recoveringService.mu.Unlock()
	if err := recoveringService.CommitAccepted(ctx, recoveringReservation); err == nil || err.Error() != "RUNTIME_CAPACITY_UNAVAILABLE" {
		t.Fatalf("recovering reservation was accepted: %v", err)
	}
	recoveringLatest, err := recoveringService.GetLatestByRunID(ctx, recoveringReservation.RunID)
	if err != nil || recoveringLatest.State != "recovering" {
		t.Fatalf("recovering reservation was mutated: latest=%+v err=%v", recoveringLatest, err)
	}
}

func TestRuntimeCapacityReleaseRejectsOldGenerationAfterRereserveMemory(t *testing.T) {
	ctx := context.Background()
	service := NewRuntimeCapacityAdmissionService(nil)
	command := testCapacityCommand("run-release-generation-fence", 1)

	first, err := service.Reserve(ctx, command)
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := service.Release(ctx, first, nil); err != nil || !changed {
		t.Fatalf("first release changed=%v err=%v", changed, err)
	}

	replacement, err := service.Reserve(ctx, command)
	if err != nil {
		t.Fatal(err)
	}
	if replacement.ReservationID == first.ReservationID || replacement.Version <= first.Version {
		t.Fatalf("replacement=%+v first=%+v", replacement, first)
	}

	changed, err := service.Release(ctx, first, nil)
	if err != nil || changed {
		t.Fatalf("old generation release changed=%v err=%v", changed, err)
	}
	latest, err := service.GetLatestByRunID(ctx, command.RunID)
	if err != nil || latest.State != "reserved" || latest.Version != replacement.Version {
		t.Fatalf("old generation released replacement: latest=%+v err=%v", latest, err)
	}
}
