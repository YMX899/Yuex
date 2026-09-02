package runtime

import (
	"context"
	"testing"

	"huahuoai/backend/source/internal/persistence"
)

func TestRuntimeDispatchCapacityBindingRejectsReReservedSameIDGeneration(t *testing.T) {
	dispatch := RuntimeDispatch{
		DispatchID:              "dispatch-capacity-generation-fence",
		CapacityReservationID:   "capacity-reused-id",
		CapacityReservedVersion: 1,
	}

	if !dispatch.MatchesCapacityReservation(RuntimeCapacityReservation{
		ReservationID: "capacity-reused-id", SnapshotVersion: 1, Version: 1, State: "reserved",
	}) {
		t.Fatal("dispatch did not match its original reserved capacity generation")
	}
	if !dispatch.MatchesCapacityReservation(RuntimeCapacityReservation{
		ReservationID: "capacity-reused-id", SnapshotVersion: 1, Version: 2, State: "accepted",
	}) {
		t.Fatal("dispatch did not match its accepted capacity generation")
	}
	if !dispatch.MatchesCapacityReservation(RuntimeCapacityReservation{
		ReservationID: "capacity-reused-id", SnapshotVersion: 1, Version: 3, State: "released",
	}) {
		t.Fatal("dispatch did not match its released capacity generation")
	}

	if dispatch.MatchesCapacityReservation(RuntimeCapacityReservation{
		ReservationID: "capacity-reused-id", SnapshotVersion: 1, Version: 4, State: "reserved",
	}) {
		t.Fatal("delayed dispatch event matched a same-ID re-reserved capacity generation")
	}
}

func TestTerminalConvergenceSnapshotRejectsCapacityVersionMismatch(t *testing.T) {
	ctx := context.Background()
	converger := NewRuntimeTerminalConverger(nil, nil, nil, nil, nil)
	command := TerminalConvergenceCommand{
		DispatchID:             "dispatch-terminal-capacity-version",
		RunID:                  "run-terminal-capacity-version",
		TerminalSourceSequence: 1,
		TerminalStatus:         "succeeded",
		QueueProof:             persistence.QueueLeaseProof{QueueID: "runtime_events:terminal-capacity-version"},
		CapacityReservation: RuntimeCapacityReservation{
			ReservationID: "capacity-terminal-version", RunID: "run-terminal-capacity-version",
			SnapshotVersion: 7, Version: 11,
		},
	}
	convergenceID := "terminal:dispatch-terminal-capacity-version:1"
	if err := converger.ensureProgress(ctx, convergenceID, command); err != nil {
		t.Fatalf("record terminal snapshot: %v", err)
	}

	for _, changed := range []TerminalConvergenceCommand{
		func() TerminalConvergenceCommand {
			changed := command
			changed.CapacityReservation.Version++
			return changed
		}(),
		func() TerminalConvergenceCommand {
			changed := command
			changed.CapacityReservation.SnapshotVersion++
			return changed
		}(),
	} {
		if err := converger.ensureProgress(ctx, convergenceID, changed); err == nil || err.Error() != "RUNTIME_EVENT_GAP" {
			t.Fatalf("capacity version mismatch error=%v, want RUNTIME_EVENT_GAP", err)
		}
	}
}
