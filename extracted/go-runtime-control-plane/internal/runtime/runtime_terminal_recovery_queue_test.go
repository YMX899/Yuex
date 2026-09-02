package runtime

import (
	"context"
	"testing"
	"time"

	"huahuoai/backend/source/internal/persistence"
)

func TestRuntimeTerminalConvergencePreservesOriginalQueueIdentityAcrossRecoveryQueue(t *testing.T) {
	originalQueueID := "runtime_events:dispatch_terminal_original"
	recoveryQueueID := "runtime_events:recovery:terminal:1"
	command := TerminalConvergenceCommand{
		DispatchID:             "dispatch_terminal_original",
		RunID:                  "run_terminal_original",
		TerminalSourceSequence: 1,
		TerminalStatus:         "succeeded",
		QueueProof:             persistence.QueueLeaseProof{QueueID: recoveryQueueID},
		OriginalQueueID:        originalQueueID,
		DispatchTerminal:       DispatchTerminalCommand{DispatchID: "dispatch_terminal_original"},
	}
	snapshot, _ := terminalConvergenceSnapshot(command)
	if terminalSnapshotString(snapshot, "queueId") != originalQueueID {
		t.Fatalf("immutable terminal snapshot queue id=%q, want original %q", terminalSnapshotString(snapshot, "queueId"), originalQueueID)
	}

	converger := NewRuntimeTerminalConverger(nil, nil, nil, nil, nil)
	convergenceID := "terminal:" + command.DispatchID + ":1"
	if err := converger.ensureProgress(context.Background(), convergenceID, command); err != nil {
		t.Fatalf("store recovery progress: %v", err)
	}
	command.QueueProof.QueueID = "runtime_events:recovery:terminal:2"
	if err := converger.ensureProgress(context.Background(), convergenceID, command); err != nil {
		t.Fatalf("retry through a later recovery queue must retain immutable identity: %v", err)
	}
}

func TestRuntimeTerminalConvergerKeepsNormalDurableFinalizationAfterEveryHistoricalEffectIsSettled(t *testing.T) {
	converger := NewRuntimeTerminalConverger(nil, nil, nil, nil, nil)
	converger.durableFinalizer = func(context.Context, TerminalConvergenceCommand, string, persistence.QueueLeaseProof) error {
		return nil
	}
	command := TerminalConvergenceCommand{AgentRunTerminal: &TerminalAgentRunProjection{}}
	settled := runtimeTerminalProgress{
		EventsVerified: true, ProductProjected: true, UsageSettled: true,
		AgentRunConverged: true, DispatchFinalized: true, SessionReleased: true,
	}
	if !converger.shouldUseDurableTerminalFinalization(settled, command) {
		t.Fatal("normal convergence must retain the exact capacity-generation finalizer even after every checkpoint is settled")
	}
}

func TestRuntimeTerminalConvergerUsesTailOnlyForExplicitLegacyCommand(t *testing.T) {
	hosts := NewRuntimeHostRepository(nil)
	hosts.dispatches["dispatch_legacy_tail"] = RuntimeDispatch{
		DispatchID: "dispatch_legacy_tail", RunID: "run_legacy_tail", RuntimeHostID: "host_legacy_tail",
	}
	converger := NewRuntimeTerminalConverger(nil, hosts, nil, nil, nil)
	command := TerminalConvergenceCommand{
		DispatchID: "dispatch_legacy_tail", RunID: "run_legacy_tail", TerminalSourceSequence: 1, TerminalStatus: "succeeded",
		QueueProof:             persistence.QueueLeaseProof{QueueID: "runtime_events:legacy_tail", WorkerID: "worker_legacy_tail", Attempt: 1, TokenHash: "sha256:legacy-tail", FencingToken: 1, LeaseExpiresAt: time.Now().Add(time.Minute)},
		DispatchTerminal:       DispatchTerminalCommand{DispatchID: "dispatch_legacy_tail", Fence: ReservationFence{RuntimeHostID: "host_legacy_tail"}},
		LegacyTailOnlyRecovery: true,
		AgentRunTerminal: &TerminalAgentRunProjection{
			AgentRunStatus: "succeeded", PlanVersion: 1, PlanStatus: "succeeded",
			PublicEvent: persistence.AgentRunEvent{AgentRunID: "run_legacy_tail", EventType: "succeeded", Status: "succeeded"},
		},
	}
	convergenceID := "terminal:" + command.DispatchID + ":1"
	if err := converger.ensureProgress(context.Background(), convergenceID, command); err != nil {
		t.Fatalf("ensure progress: %v", err)
	}
	converger.mu.Lock()
	progress := converger.progress[convergenceID]
	progress.EventsVerified, progress.ProductProjected, progress.UsageSettled = true, true, true
	progress.AgentRunConverged, progress.DispatchFinalized, progress.SessionReleased = true, true, true
	converger.progress[convergenceID] = progress
	converger.mu.Unlock()
	appended, completed := 0, 0
	converger.AppendPublicEvent = func(context.Context, TerminalConvergenceCommand, string) error { appended++; return nil }
	converger.CompleteQueue = func(context.Context, TerminalConvergenceCommand, string) error { completed++; return nil }
	result, err := converger.Converge(context.Background(), command)
	if err != nil || !result.Complete {
		t.Fatalf("explicit tail-only convergence=%+v err=%v", result, err)
	}
	if appended != 1 || completed != 1 {
		t.Fatalf("tail-only writes appended=%d completed=%d, want one each", appended, completed)
	}
}
