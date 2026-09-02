package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"huahuoai/backend/source/internal/persistence"

	"github.com/jackc/pgx/v5"
)

type TerminalProjectionFunc func(context.Context, TerminalConvergenceCommand, string) error

// TerminalQueueCompletionFunc is invoked as the final convergence step. Runtime
// event workers use it to stop their renewable queue heartbeat and atomically
// acknowledge the latest fenced queue lease with the convergence checkpoint.
// Implementations must make the queue acknowledgement idempotent for
// convergenceID.
type TerminalQueueCompletionFunc func(context.Context, TerminalConvergenceCommand, string) error

// TerminalQueueProofFunc freezes the worker's latest renewable queue lease
// immediately before the durable terminal transaction begins. The transaction
// consumes that exact proof when it acknowledges the queue record.
type TerminalQueueProofFunc func(context.Context, TerminalConvergenceCommand) (persistence.QueueLeaseProof, error)

// TerminalAgentRunProjection is the app-safe terminal projection that joins
// the Runtime's immutable terminal snapshot to the public AgentRun state. It
// is intentionally carried by the convergence command so the PostgreSQL
// terminal transaction can write AgentRun, Plan and public event state with
// capacity, dispatch, session and queue state.
type TerminalAgentRunProjection struct {
	AgentRunStatus string
	PlanVersion    int
	PlanStatus     string
	PublicResult   map[string]any
	ErrorSummary   map[string]any
	PublicEvent    persistence.AgentRunEvent
}

type TerminalConvergenceCommand struct {
	DispatchID             string
	RunID                  string
	TerminalSourceSequence int64
	TerminalStatus         string
	SafeResult             map[string]any
	SafeError              map[string]any
	ActualUsage            map[string]any
	QueueProof             persistence.QueueLeaseProof
	// OriginalQueueID preserves the queue identity captured by the immutable
	// terminal snapshot when a recovery worker consumes a separate queue.
	// Empty means QueueProof.QueueID, which is the normal event path.
	OriginalQueueID     string
	DispatchTerminal    DispatchTerminalCommand
	SessionAdmission    *RuntimeSessionAdmissionLease
	SessionRequired     bool
	CapacityReservation RuntimeCapacityReservation
	AgentRunTerminal    *TerminalAgentRunProjection
	// LegacyTailOnlyRecovery is an explicit, fail-closed recovery mode for a
	// pre-capacity-binding terminal record. It is never a general capacity
	// bypass: the converger rechecks that the first six durable checkpoints are
	// complete before it may append the public event and acknowledge the exact
	// leased recovery queue.
	LegacyTailOnlyRecovery bool
}

type TerminalConvergenceResult struct {
	ConvergenceID string
	Complete      bool
	Completed     []string
}

type TerminalConvergenceRecoveryCandidate struct {
	ConvergenceID          string
	DispatchID             string
	RunID                  string
	RuntimeHostID          string
	QueueID                string
	TerminalSourceSequence int64
	TerminalStatus         string
	SnapshotHash           string
	EventsVerified         bool
	ProductProjected       bool
	UsageSettled           bool
	AgentRunConverged      bool
	DispatchFinalized      bool
	SessionReleased        bool
	UpdatedAt              time.Time
}

// TerminalConvergenceRecovery is the durable, safe-to-replay form of an
// incomplete terminal convergence. It deliberately contains no Runtime ticket,
// session lease token, or queue lease token; the recovery worker obtains fresh
// fenced capabilities from their authoritative stores.
type TerminalConvergenceRecovery struct {
	ConvergenceID           string
	DispatchID              string
	RunID                   string
	RuntimeHostID           string
	QueueID                 string
	TerminalSourceSequence  int64
	TerminalStatus          string
	SnapshotHash            string
	SafeResult              map[string]any
	SafeError               map[string]any
	ActualUsage             map[string]any
	SessionRequired         bool
	CapacityReservationID   string
	CapacitySnapshotVersion int64
	CapacityReservedVersion int64
	EventsVerified          bool
	ProductProjected        bool
	UsageSettled            bool
	AgentRunConverged       bool
	DispatchFinalized       bool
	SessionReleased         bool
	UpdatedAt               time.Time
}

type runtimeTerminalProgress struct {
	ConvergenceID          string
	DispatchID             string
	RunID                  string
	QueueID                string
	TerminalSourceSequence int64
	TerminalStatus         string
	SnapshotHash           string
	UpdatedAt              time.Time
	EventsVerified         bool
	ProductProjected       bool
	UsageSettled           bool
	AgentRunConverged      bool
	DispatchFinalized      bool
	SessionReleased        bool
	PublicEventAppended    bool
	QueueCompleted         bool
	Snapshot               map[string]any
}

type RuntimeTerminalConverger struct {
	DB                *persistence.Database
	Hosts             *RuntimeHostRepository
	Sessions          *RuntimeSessionAdmissionService
	Capacity          *RuntimeCapacityAdmissionService
	Queue             *persistence.QueueRepository
	AgentRuns         *persistence.AgentRunRepository
	ProjectProduct    TerminalProjectionFunc
	ConvergeAgentRun  TerminalProjectionFunc
	AppendPublicEvent TerminalProjectionFunc
	CompleteQueue     TerminalQueueCompletionFunc
	FinalQueueProof   TerminalQueueProofFunc
	// durableFinalizer exists only for focused local control-flow tests. The
	// production path always uses finalizeTerminalEffectsPostgres.
	durableFinalizer func(context.Context, TerminalConvergenceCommand, string, persistence.QueueLeaseProof) error
	mu               sync.Mutex
	progress         map[string]runtimeTerminalProgress
}

func NewRuntimeTerminalConverger(db *persistence.Database, hosts *RuntimeHostRepository, sessions *RuntimeSessionAdmissionService, capacity *RuntimeCapacityAdmissionService, queueRepository *persistence.QueueRepository, agentRuns ...*persistence.AgentRunRepository) *RuntimeTerminalConverger {
	converger := &RuntimeTerminalConverger{DB: db, Hosts: hosts, Sessions: sessions, Capacity: capacity, Queue: queueRepository, progress: map[string]runtimeTerminalProgress{}}
	if len(agentRuns) > 0 {
		converger.AgentRuns = agentRuns[0]
	}
	return converger
}

func (c *RuntimeTerminalConverger) Converge(ctx context.Context, command TerminalConvergenceCommand) (TerminalConvergenceResult, error) {
	if err := validateTerminalConvergenceCommand(command); err != nil {
		return TerminalConvergenceResult{}, err
	}
	// A terminal command is never allowed to create a convergence checkpoint or
	// project Product state for a different dispatch. The Run and Runtime Host
	// identity are immutable dispatch bindings, so verify them before the first
	// persisted convergence write and before the Product projection callback.
	if err := c.validateTerminalDispatchBinding(ctx, command); err != nil {
		return TerminalConvergenceResult{}, err
	}
	convergenceID := fmt.Sprintf("terminal:%s:%d", command.DispatchID, command.TerminalSourceSequence)
	if err := c.ensureProgress(ctx, convergenceID, command); err != nil {
		return TerminalConvergenceResult{}, err
	}
	progress, err := c.getProgress(ctx, convergenceID)
	if err != nil {
		return TerminalConvergenceResult{}, err
	}
	if command.LegacyTailOnlyRecovery {
		return c.convergeLegacyTailOnlyRecovery(ctx, command, convergenceID, progress)
	}
	steps := []struct {
		name string
		done func(runtimeTerminalProgress) bool
		run  func() error
	}{
		{"events_verified", func(p runtimeTerminalProgress) bool { return p.EventsVerified }, func() error {
			return c.Hosts.VerifyDispatchEventContinuity(ctx, command.DispatchID, command.TerminalSourceSequence)
		}},
		{"product_projected", func(p runtimeTerminalProgress) bool { return p.ProductProjected }, func() error {
			if c.ProjectProduct == nil {
				return fmt.Errorf("TERMINAL_PRODUCT_PROJECTOR_UNAVAILABLE")
			}
			return c.ProjectProduct(ctx, command, convergenceID)
		}},
		{"usage_settled", func(p runtimeTerminalProgress) bool { return p.UsageSettled }, func() error {
			if c.Capacity == nil {
				return fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
			}
			_, err := c.Capacity.Release(ctx, command.CapacityReservation, command.ActualUsage)
			return err
		}},
		{"agent_run_converged", func(p runtimeTerminalProgress) bool { return p.AgentRunConverged }, func() error {
			if c.ConvergeAgentRun == nil {
				return fmt.Errorf("TERMINAL_AGENT_RUN_CONVERGER_UNAVAILABLE")
			}
			return c.ConvergeAgentRun(ctx, command, convergenceID+":agent_run")
		}},
		{"dispatch_finalized", func(p runtimeTerminalProgress) bool { return p.DispatchFinalized }, func() error {
			if err := c.Hosts.FinalizeDispatchAndReleaseSlot(ctx, command.DispatchTerminal); err != nil {
				return err
			}
			if !c.postgresReady() {
				c.Hosts.finalizeRuntimeRunRecordV1Memory(command)
				c.Hosts.abortOutstandingRuntimeToolInvocationsMemory(command.RunID)
			}
			return nil
		}},
		{"session_released", func(p runtimeTerminalProgress) bool { return p.SessionReleased }, func() error {
			if !command.SessionRequired {
				return nil
			}
			if c.Sessions == nil {
				return fmt.Errorf("RUNTIME_SESSION_ADMISSION_UNAVAILABLE")
			}
			if command.SessionAdmission == nil {
				released, err := c.Sessions.ReleasedOrExpiredByRunID(ctx, command.RunID)
				if err != nil || !released {
					return fmt.Errorf("RUNTIME_SESSION_ADMISSION_UNAVAILABLE")
				}
				return nil
			}
			_, err := c.Sessions.Release(ctx, *command.SessionAdmission, normalizeAdmissionReleaseReason(command.TerminalStatus))
			return err
		}},
		{"public_event_appended", func(p runtimeTerminalProgress) bool { return p.PublicEventAppended }, func() error {
			if c.AppendPublicEvent == nil {
				return fmt.Errorf("TERMINAL_EVENT_APPENDER_UNAVAILABLE")
			}
			return c.AppendPublicEvent(ctx, command, convergenceID+":public_event")
		}},
		{"queue_completed", func(p runtimeTerminalProgress) bool { return p.QueueCompleted }, func() error {
			if c.CompleteQueue != nil {
				return c.CompleteQueue(ctx, command, convergenceID)
			}
			if c.Queue == nil {
				return fmt.Errorf("QUEUE_REPOSITORY_UNAVAILABLE")
			}
			_, err := c.Queue.CompleteTerminalConvergence(ctx, command.QueueProof, convergenceID)
			return err
		}},
	}
	for _, step := range steps {
		progress, err := c.getProgress(ctx, convergenceID)
		if err != nil {
			return TerminalConvergenceResult{}, err
		}
		// Product projection has its own durable ledger. Once the Runtime
		// source events and that ledger are complete, all remaining relational
		// terminal effects must cross one PostgreSQL commit boundary. Keep the
		// stepwise path for the in-memory unit-test backend and legacy callers
		// that do not provide the app-safe AgentRun projection.
		if step.name == "usage_settled" && c.shouldUseDurableTerminalFinalization(progress, command) {
			if err := c.convergeDurableTerminalEffects(ctx, command, convergenceID); err != nil {
				_ = c.recordProgressError(ctx, convergenceID, terminalErrorCode(err))
				return c.result(progress), err
			}
			progress, err = c.getProgress(ctx, convergenceID)
			if err != nil {
				return TerminalConvergenceResult{}, err
			}
			return c.result(progress), nil
		}
		if step.done(progress) {
			continue
		}
		if err := step.run(); err != nil {
			_ = c.recordProgressError(ctx, convergenceID, terminalErrorCode(err))
			return c.result(progress), err
		}
		if step.name == "queue_completed" && c.postgresReady() {
			continue
		}
		if err := c.markStep(ctx, convergenceID, step.name); err != nil {
			return TerminalConvergenceResult{}, err
		}
	}
	progress, err = c.getProgress(ctx, convergenceID)
	if err != nil {
		return TerminalConvergenceResult{}, err
	}
	return c.result(progress), nil
}

// convergeLegacyTailOnlyRecovery is deliberately separate from normal terminal
// finalization. A legacy row may reach this path only after every stateful
// effect (events, product, usage, AgentRun, dispatch/slot and session) is
// already durable. This function must never repair, release, or synthesize any
// of those effects; its only mutable facts are the idempotent public event, the
// exact current queue lease, and their two convergence timestamps.
func (c *RuntimeTerminalConverger) convergeLegacyTailOnlyRecovery(ctx context.Context, command TerminalConvergenceCommand, convergenceID string, progress runtimeTerminalProgress) (TerminalConvergenceResult, error) {
	if err := validateLegacyTailOnlyRecovery(command, progress); err != nil {
		_ = c.recordProgressError(ctx, convergenceID, terminalErrorCode(err))
		return c.result(progress), err
	}
	if progress.QueueCompleted {
		return c.result(progress), nil
	}
	proof := command.QueueProof
	if c.FinalQueueProof != nil {
		var err error
		proof, err = c.FinalQueueProof(ctx, command)
		if err != nil {
			_ = c.recordProgressError(ctx, convergenceID, terminalErrorCode(err))
			return c.result(progress), err
		}
	}
	if err := validateTerminalQueueProof(proof); err != nil {
		_ = c.recordProgressError(ctx, convergenceID, terminalErrorCode(err))
		return c.result(progress), err
	}
	if c.postgresReady() {
		if c.AgentRuns == nil {
			err := fmt.Errorf("RUNTIME_TERMINAL_FINALIZATION_UNAVAILABLE")
			_ = c.recordProgressError(ctx, convergenceID, terminalErrorCode(err))
			return c.result(progress), err
		}
		if err := c.DB.WithSerializableRetry(ctx, "runtime_terminal_legacy_tail_only", 3, func(tx *persistence.Tx) error {
			return c.finalizeLegacyTailOnlyRecoveryPostgres(ctx, tx, command, convergenceID, proof)
		}); err != nil {
			_ = c.recordProgressError(ctx, convergenceID, terminalErrorCode(err))
			return c.result(progress), err
		}
		c.AgentRuns.NotifyPublicEvent(command.RunID)
		updated, err := c.getProgress(ctx, convergenceID)
		if err != nil {
			return TerminalConvergenceResult{}, err
		}
		return c.result(updated), nil
	}

	// The memory backend has no transaction primitive. Keep the same narrow
	// write set for focused tests and legacy callers.
	if !progress.PublicEventAppended {
		if c.AppendPublicEvent == nil {
			err := fmt.Errorf("TERMINAL_EVENT_APPENDER_UNAVAILABLE")
			_ = c.recordProgressError(ctx, convergenceID, terminalErrorCode(err))
			return c.result(progress), err
		}
		if err := c.AppendPublicEvent(ctx, command, convergenceID+":public_event"); err != nil {
			_ = c.recordProgressError(ctx, convergenceID, terminalErrorCode(err))
			return c.result(progress), err
		}
		if err := c.markStep(ctx, convergenceID, "public_event_appended"); err != nil {
			return TerminalConvergenceResult{}, err
		}
	}
	if c.CompleteQueue != nil {
		if err := c.CompleteQueue(ctx, command, convergenceID); err != nil {
			_ = c.recordProgressError(ctx, convergenceID, terminalErrorCode(err))
			return c.result(progress), err
		}
	} else if c.Queue == nil {
		err := fmt.Errorf("QUEUE_REPOSITORY_UNAVAILABLE")
		_ = c.recordProgressError(ctx, convergenceID, terminalErrorCode(err))
		return c.result(progress), err
	} else if _, err := c.Queue.CompleteTerminalConvergence(ctx, proof, convergenceID); err != nil {
		_ = c.recordProgressError(ctx, convergenceID, terminalErrorCode(err))
		return c.result(progress), err
	}
	if err := c.markStep(ctx, convergenceID, "queue_completed"); err != nil {
		return TerminalConvergenceResult{}, err
	}
	updated, err := c.getProgress(ctx, convergenceID)
	if err != nil {
		return TerminalConvergenceResult{}, err
	}
	return c.result(updated), nil
}

func validateLegacyTailOnlyRecovery(command TerminalConvergenceCommand, progress runtimeTerminalProgress) error {
	if !command.LegacyTailOnlyRecovery || command.CapacityReservation.ReservationID != "" || command.CapacityReservation.RunID != "" ||
		command.CapacityReservation.SnapshotVersion != 0 || command.CapacityReservation.Version != 0 || command.SessionAdmission != nil ||
		!progress.EventsVerified || !progress.ProductProjected || !progress.UsageSettled || !progress.AgentRunConverged || !progress.DispatchFinalized || !progress.SessionReleased {
		return fmt.Errorf("RUNTIME_TERMINAL_CONVERGENCE_PREREQUISITE_MISSING")
	}
	if err := validateTerminalAgentRunProjection(command); err != nil {
		return err
	}
	projection := command.AgentRunTerminal
	if projection.AgentRunStatus != terminalConvergenceAgentRunStatus(command.TerminalStatus) || projection.PlanStatus != terminalConvergencePlanStatus(projection.AgentRunStatus) {
		return fmt.Errorf("RUNTIME_TERMINAL_CONVERGENCE_INCONSISTENT")
	}
	return nil
}

func terminalConvergenceAgentRunStatus(status string) string {
	if status == "aborted" {
		return "cancelled"
	}
	return status
}

func terminalConvergencePlanStatus(runStatus string) string {
	if runStatus == "cancelled" {
		return "cancelled"
	}
	if runStatus == "succeeded" {
		return "succeeded"
	}
	return "failed"
}

func (c *RuntimeTerminalConverger) useDurableTerminalFinalization(command TerminalConvergenceCommand) bool {
	return command.AgentRunTerminal != nil && (c.postgresReady() || c.durableFinalizer != nil)
}

// shouldUseDurableTerminalFinalization retains the exact capacity-generation
// fence for every normal command, including one that replays after all six
// stateful effects were marked. The only capacity-free route is the explicit
// LegacyTailOnlyRecovery branch handled before this method is reached.
func (c *RuntimeTerminalConverger) shouldUseDurableTerminalFinalization(progress runtimeTerminalProgress, command TerminalConvergenceCommand) bool {
	return !command.LegacyTailOnlyRecovery && c.useDurableTerminalFinalization(command)
}

// convergeDurableTerminalEffects commits every relational terminal projection
// after Product projection in one serializable transaction. The Redis session
// lease remains external, so the same transaction enqueues its restricted
// cleanup record beside the authoritative terminal admission row. The drain
// runs only after commit and recovery retains any unavailable Redis/Tair work.
func (c *RuntimeTerminalConverger) convergeDurableTerminalEffects(ctx context.Context, command TerminalConvergenceCommand, convergenceID string) error {
	if err := validateTerminalAgentRunProjection(command); err != nil {
		return err
	}
	if c.Hosts == nil || c.Capacity == nil || c.Queue == nil || (c.AgentRuns == nil && c.durableFinalizer == nil) {
		return fmt.Errorf("RUNTIME_TERMINAL_FINALIZATION_UNAVAILABLE")
	}
	if command.SessionRequired && c.Sessions == nil {
		return fmt.Errorf("RUNTIME_SESSION_ADMISSION_UNAVAILABLE")
	}
	if command.SessionAdmission != nil && command.SessionRequired {
		if err := c.Sessions.AssertActive(ctx, *command.SessionAdmission); err != nil {
			released, releaseErr := c.Sessions.ReleasedOrExpiredByRunID(ctx, command.RunID)
			if releaseErr != nil || !released {
				return err
			}
		}
	}
	proof := command.QueueProof
	if c.FinalQueueProof != nil {
		var err error
		proof, err = c.FinalQueueProof(ctx, command)
		if err != nil {
			return err
		}
	}
	if err := validateTerminalQueueProof(proof); err != nil {
		return err
	}
	if c.durableFinalizer != nil {
		return c.durableFinalizer(ctx, command, convergenceID, proof)
	}
	if !c.postgresReady() {
		return fmt.Errorf("RUNTIME_TERMINAL_FINALIZATION_UNAVAILABLE")
	}
	if err := c.DB.WithSerializableRetry(ctx, "runtime_terminal_finalization", 3, func(tx *persistence.Tx) error {
		return c.finalizeTerminalEffectsPostgres(ctx, tx, command, convergenceID, proof)
	}); err != nil {
		return err
	}
	if command.SessionRequired && c.Sessions != nil {
		// The terminal transaction has already made DB state authoritative and
		// durably enqueued a restricted Redis/Tair cleanup proof. Drain once for
		// low latency; recovery retains the outbox when Redis is unavailable.
		_, _ = c.Sessions.DrainTerminalLeaseCleanup(ctx, "runtime-terminal:"+command.RunID, 1)
	}
	if c.AgentRuns != nil {
		c.AgentRuns.NotifyPublicEvent(command.RunID)
	}
	return nil
}

func (c *RuntimeTerminalConverger) finalizeTerminalEffectsPostgres(ctx context.Context, tx *persistence.Tx, command TerminalConvergenceCommand, convergenceID string, proof persistence.QueueLeaseProof) error {
	rows, err := tx.Query(ctx, `
select c.events_verified_at is not null as events_verified,
       c.product_projected_at is not null as product_projected,
       c.queue_completed_at is not null as queue_completed,
       c.snapshot_hash,
       q.status as queue_status,
       q.lease_owner,
       q.attempt as queue_attempt,
       coalesce(q.lease_token_hash,'') as lease_token_hash,
       q.lease_fencing_token,
       q.lease_expires_at
from runtime_terminal_convergences c
join task_queue_records q on q.queue_id=@queue
where c.convergence_id=@convergence
  and (
    c.queue_id=@queue
    or exists(
      select 1
      from runtime_terminal_convergence_recovery_queue_lineage lineage
      where lineage.convergence_id=c.convergence_id
        and lineage.recovery_queue_id=@queue
    )
  )
for update of c,q`, map[string]any{"convergence": convergenceID, "queue": proof.QueueID})
	if err != nil || len(rows) != 1 {
		if err != nil {
			return err
		}
		return fmt.Errorf("NOT_FOUND")
	}
	checkpoint := rows[0]
	if runtimeTerminalBool(checkpoint["queue_completed"]) {
		if fmt.Sprint(checkpoint["queue_status"]) != "succeeded" {
			return fmt.Errorf("RUNTIME_TERMINAL_CONVERGENCE_INCONSISTENT")
		}
		return nil
	}
	if !runtimeTerminalBool(checkpoint["events_verified"]) || !runtimeTerminalBool(checkpoint["product_projected"]) {
		return fmt.Errorf("RUNTIME_TERMINAL_CONVERGENCE_PREREQUISITE_MISSING")
	}
	if fmt.Sprint(checkpoint["queue_status"]) != "leased" && fmt.Sprint(checkpoint["queue_status"]) != "running" ||
		fmt.Sprint(checkpoint["lease_owner"]) != proof.WorkerID || runtimeHostInt64(checkpoint["queue_attempt"]) != int64(proof.Attempt) ||
		fmt.Sprint(checkpoint["lease_token_hash"]) != proof.TokenHash || runtimeHostInt64(checkpoint["lease_fencing_token"]) != proof.FencingToken {
		return fmt.Errorf("STALE_QUEUE_LEASE")
	}

	if err := c.finalizeAgentRunProjectionTx(ctx, tx, command, convergenceID); err != nil {
		return err
	}
	if err := c.finalizeCapacityReservationTx(ctx, tx, command); err != nil {
		return err
	}
	if err := c.finalizeDispatchAndSlotTx(ctx, tx, command); err != nil {
		return err
	}
	if err := c.Hosts.finalizeRuntimeRunRecordV1Tx(ctx, tx, command); err != nil {
		return err
	}
	if err := abortOutstandingRuntimeToolInvocationsTx(ctx, tx, command.RunID); err != nil {
		return err
	}
	if err := c.finalizeSessionAdmissionTx(ctx, tx, command, convergenceID); err != nil {
		return err
	}

	tag, err := tx.ExecRaw(ctx, `
update task_queue_records
set status='succeeded',lease_owner=null,lease_token_hash=null,lease_expires_at=null,
    error_summary='{}'::jsonb,updated_at=now()
where queue_id=$1 and status in ('leased','running')
  and lease_owner=$2 and attempt=$3 and lease_token_hash=$4
  and lease_fencing_token=$5 and lease_expires_at=$6 and lease_expires_at>now()`,
		proof.QueueID, proof.WorkerID, proof.Attempt, proof.TokenHash, proof.FencingToken, proof.LeaseExpiresAt)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("STALE_QUEUE_LEASE")
	}

	_, snapshotHash := terminalConvergenceSnapshot(command)
	tag, err = tx.ExecRaw(ctx, `
update runtime_terminal_convergences
set usage_settled_at=coalesce(usage_settled_at,now()),
    agent_run_converged_at=coalesce(agent_run_converged_at,now()),
    dispatch_finalized_at=coalesce(dispatch_finalized_at,now()),
    session_released_at=coalesce(session_released_at,now()),
    public_event_appended_at=coalesce(public_event_appended_at,now()),
    queue_completed_at=coalesce(queue_completed_at,now()),
    last_error_code=null,attempt_count=attempt_count+1,updated_at=now()
where convergence_id=$1 and snapshot_hash=$2`, convergenceID, snapshotHash)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("RUNTIME_EVENT_GAP")
	}
	return nil
}

// finalizeLegacyTailOnlyRecoveryPostgres intentionally has a much smaller
// mutation set than finalizeTerminalEffectsPostgres. It is the only historical
// recovery path allowed to run without a capacity reservation, so every
// stateful prerequisite is locked and checked before the public event and queue
// acknowledgement are committed together.
func (c *RuntimeTerminalConverger) finalizeLegacyTailOnlyRecoveryPostgres(ctx context.Context, tx *persistence.Tx, command TerminalConvergenceCommand, convergenceID string, proof persistence.QueueLeaseProof) error {
	rows, err := tx.Query(ctx, `
select c.events_verified_at is not null as events_verified,
       c.product_projected_at is not null as product_projected,
       c.usage_settled_at is not null as usage_settled,
       c.agent_run_converged_at is not null as agent_run_converged,
       c.dispatch_finalized_at is not null as dispatch_finalized,
       c.session_released_at is not null as session_released,
       c.public_event_appended_at is not null as public_event_appended,
       c.queue_completed_at is not null as queue_completed,
       c.snapshot_hash,
       q.status as queue_status,
       q.lease_owner,
       q.attempt as queue_attempt,
       coalesce(q.lease_token_hash,'') as lease_token_hash,
       q.lease_fencing_token,
       q.lease_expires_at
from runtime_terminal_convergences c
join task_queue_records q on q.queue_id=@queue
where c.convergence_id=@convergence
  and (
    c.queue_id=@queue
    or exists(
      select 1
      from runtime_terminal_convergence_recovery_queue_lineage lineage
      where lineage.convergence_id=c.convergence_id
        and lineage.recovery_queue_id=@queue
    )
  )
for update of c,q`, map[string]any{"convergence": convergenceID, "queue": proof.QueueID})
	if err != nil || len(rows) != 1 {
		if err != nil {
			return err
		}
		return fmt.Errorf("NOT_FOUND")
	}
	checkpoint := rows[0]
	if runtimeTerminalBool(checkpoint["queue_completed"]) {
		if fmt.Sprint(checkpoint["queue_status"]) != "succeeded" {
			return fmt.Errorf("RUNTIME_TERMINAL_CONVERGENCE_INCONSISTENT")
		}
		return nil
	}
	if !runtimeTerminalBool(checkpoint["events_verified"]) || !runtimeTerminalBool(checkpoint["product_projected"]) ||
		!runtimeTerminalBool(checkpoint["usage_settled"]) || !runtimeTerminalBool(checkpoint["agent_run_converged"]) ||
		!runtimeTerminalBool(checkpoint["dispatch_finalized"]) || !runtimeTerminalBool(checkpoint["session_released"]) {
		return fmt.Errorf("RUNTIME_TERMINAL_CONVERGENCE_PREREQUISITE_MISSING")
	}
	_, expectedSnapshotHash := terminalConvergenceSnapshot(command)
	if fmt.Sprint(checkpoint["snapshot_hash"]) != expectedSnapshotHash {
		return fmt.Errorf("RUNTIME_EVENT_GAP")
	}
	if (fmt.Sprint(checkpoint["queue_status"]) != "leased" && fmt.Sprint(checkpoint["queue_status"]) != "running") ||
		fmt.Sprint(checkpoint["lease_owner"]) != proof.WorkerID || runtimeHostInt64(checkpoint["queue_attempt"]) != int64(proof.Attempt) ||
		fmt.Sprint(checkpoint["lease_token_hash"]) != proof.TokenHash || runtimeHostInt64(checkpoint["lease_fencing_token"]) != proof.FencingToken {
		return fmt.Errorf("STALE_QUEUE_LEASE")
	}
	if err := c.assertLegacyTailOnlyTerminalFactsTx(ctx, tx, command); err != nil {
		return err
	}
	if _, err := c.AgentRuns.AppendPublicEventIdempotentInTx(ctx, tx, command.AgentRunTerminal.PublicEvent, convergenceID+":public_event"); err != nil {
		return err
	}
	tag, err := tx.ExecRaw(ctx, `
update task_queue_records
set status='succeeded',lease_owner=null,lease_token_hash=null,lease_expires_at=null,
    error_summary='{}'::jsonb,updated_at=now()
where queue_id=$1 and status in ('leased','running')
  and lease_owner=$2 and attempt=$3 and lease_token_hash=$4
  and lease_fencing_token=$5 and lease_expires_at=$6 and lease_expires_at>now()`,
		proof.QueueID, proof.WorkerID, proof.Attempt, proof.TokenHash, proof.FencingToken, proof.LeaseExpiresAt)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("STALE_QUEUE_LEASE")
	}
	tag, err = tx.ExecRaw(ctx, `
update runtime_terminal_convergences
set public_event_appended_at=coalesce(public_event_appended_at,now()),
    queue_completed_at=coalesce(queue_completed_at,now()),
    last_error_code=null,attempt_count=attempt_count+1,updated_at=now()
where convergence_id=$1 and snapshot_hash=$2`, convergenceID, expectedSnapshotHash)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("RUNTIME_EVENT_GAP")
	}
	return nil
}

// assertLegacyTailOnlyTerminalFactsTx rechecks the terminal state under the
// same transaction that appends the public event. It prevents a stale Worker
// preflight from treating an active Run, Slot, Session, or capacity-bound
// dispatch as a historical tail-only record.
func (c *RuntimeTerminalConverger) assertLegacyTailOnlyTerminalFactsTx(ctx context.Context, tx *persistence.Tx, command TerminalConvergenceCommand) error {
	projection := command.AgentRunTerminal
	runRows, err := tx.Query(ctx, `select status from agent_runs where agent_run_id=@run for update`, map[string]any{"run": command.RunID})
	if err != nil || len(runRows) != 1 {
		if err != nil {
			return err
		}
		return fmt.Errorf("RUNTIME_TERMINAL_CONVERGENCE_INCONSISTENT")
	}
	if fmt.Sprint(runRows[0]["status"]) != projection.AgentRunStatus {
		return fmt.Errorf("RUNTIME_TERMINAL_CONVERGENCE_INCONSISTENT")
	}
	planRows, err := tx.Query(ctx, `select status from agent_run_plans where agent_run_id=@run and plan_version=@version for update`, map[string]any{"run": command.RunID, "version": projection.PlanVersion})
	if err != nil || len(planRows) != 1 {
		if err != nil {
			return err
		}
		return fmt.Errorf("RUNTIME_TERMINAL_CONVERGENCE_INCONSISTENT")
	}
	if fmt.Sprint(planRows[0]["status"]) != projection.PlanStatus {
		return fmt.Errorf("RUNTIME_TERMINAL_CONVERGENCE_INCONSISTENT")
	}
	fence := command.DispatchTerminal.Fence
	// This historical recovery is read-only, but its row locks still need the
	// canonical Slot -> Dispatch order used by active acceptance and terminal
	// transitions.
	slotRows, err := tx.Query(ctx, `select state,run_id,dispatch_id,runtime_host_id,fencing_token from runtime_slot_reservations where reservation_id=@reservation for update`, map[string]any{"reservation": fence.ReservationID})
	if err != nil || len(slotRows) != 1 {
		if err != nil {
			return err
		}
		return fmt.Errorf("RUNTIME_TERMINAL_CONVERGENCE_INCONSISTENT")
	}
	slot := slotRows[0]
	dispatchRows, err := tx.Query(ctx, `
select state,run_id,reservation_id,runtime_host_id,fencing_token,
       coalesce(capacity_reservation_id,'') as capacity_reservation_id,
       coalesce(capacity_reserved_version,0) as capacity_reserved_version
from runtime_run_dispatches where dispatch_id=@dispatch for update`, map[string]any{"dispatch": command.DispatchID})
	if err != nil || len(dispatchRows) != 1 {
		if err != nil {
			return err
		}
		return fmt.Errorf("RUNTIME_TERMINAL_CONVERGENCE_INCONSISTENT")
	}
	dispatch := dispatchRows[0]
	if fmt.Sprint(dispatch["state"]) != command.TerminalStatus || fmt.Sprint(dispatch["run_id"]) != command.RunID ||
		fmt.Sprint(dispatch["reservation_id"]) != fence.ReservationID || fmt.Sprint(dispatch["runtime_host_id"]) != fence.RuntimeHostID ||
		runtimeHostInt64(dispatch["fencing_token"]) != fence.FencingToken || fmt.Sprint(dispatch["capacity_reservation_id"]) != "" || runtimeHostInt64(dispatch["capacity_reserved_version"]) != 0 {
		return fmt.Errorf("RUNTIME_TERMINAL_CONVERGENCE_INCONSISTENT")
	}
	if !stringInRuntime(fmt.Sprint(slot["state"]), []string{"released", "expired"}) || fmt.Sprint(slot["run_id"]) != command.RunID ||
		fmt.Sprint(slot["dispatch_id"]) != command.DispatchID || fmt.Sprint(slot["runtime_host_id"]) != fence.RuntimeHostID || runtimeHostInt64(slot["fencing_token"]) != fence.FencingToken {
		return fmt.Errorf("RUNTIME_TERMINAL_CONVERGENCE_INCONSISTENT")
	}
	activeAdmissionRows, err := tx.Query(ctx, `select count(*) as count from runtime_session_admissions where run_id=@run and state not in ('released','expired')`, map[string]any{"run": command.RunID})
	if err != nil || len(activeAdmissionRows) != 1 {
		if err != nil {
			return err
		}
		return fmt.Errorf("RUNTIME_TERMINAL_CONVERGENCE_INCONSISTENT")
	}
	if runtimeHostInt64(activeAdmissionRows[0]["count"]) != 0 {
		return fmt.Errorf("RUNTIME_TERMINAL_CONVERGENCE_INCONSISTENT")
	}
	if command.SessionRequired {
		terminalAdmissionRows, err := tx.Query(ctx, `select count(*) as count from runtime_session_admissions where run_id=@run and state in ('released','expired')`, map[string]any{"run": command.RunID})
		if err != nil || len(terminalAdmissionRows) != 1 {
			if err != nil {
				return err
			}
			return fmt.Errorf("RUNTIME_TERMINAL_CONVERGENCE_INCONSISTENT")
		}
		if runtimeHostInt64(terminalAdmissionRows[0]["count"]) == 0 {
			return fmt.Errorf("RUNTIME_TERMINAL_CONVERGENCE_INCONSISTENT")
		}
	}
	return nil
}

func (c *RuntimeTerminalConverger) finalizeAgentRunProjectionTx(ctx context.Context, tx *persistence.Tx, command TerminalConvergenceCommand, convergenceID string) error {
	projection := command.AgentRunTerminal
	rows, err := tx.Query(ctx, `select status from agent_runs where agent_run_id=@run for update`, map[string]any{"run": command.RunID})
	if err != nil || len(rows) != 1 {
		if err != nil {
			return err
		}
		return fmt.Errorf("AGENT_PLAN_EXPIRED")
	}
	currentStatus := fmt.Sprint(rows[0]["status"])
	if currentStatus != projection.AgentRunStatus {
		if !terminalAgentRunTransitionAllowed(currentStatus) {
			return fmt.Errorf("AGENT_PLAN_EXPIRED")
		}
		publicResult, _ := json.Marshal(cloneTerminalSnapshot(projection.PublicResult))
		errorSummary, _ := json.Marshal(cloneTerminalSnapshot(projection.ErrorSummary))
		if err := tx.Exec(ctx, `
update agent_runs
set status=@status,
    public_result=case when @publicResult::jsonb='{}'::jsonb then public_result else @publicResult::jsonb end,
    error_summary=case when @errorSummary::jsonb='{}'::jsonb then error_summary else @errorSummary::jsonb end,
    updated_at=now()
where agent_run_id=@run`, map[string]any{
			"run": command.RunID, "status": projection.AgentRunStatus,
			"publicResult": string(publicResult), "errorSummary": string(errorSummary),
		}); err != nil {
			return err
		}
	}

	rows, err = tx.Query(ctx, `select status from agent_run_plans where agent_run_id=@run and plan_version=@version for update`, map[string]any{"run": command.RunID, "version": projection.PlanVersion})
	if err != nil || len(rows) != 1 {
		if err != nil {
			return err
		}
		return fmt.Errorf("AGENT_PLAN_EXPIRED")
	}
	if currentPlanStatus := fmt.Sprint(rows[0]["status"]); currentPlanStatus != projection.PlanStatus {
		if currentPlanStatus != "executing" {
			return fmt.Errorf("AGENT_PLAN_EXPIRED")
		}
		if err := tx.Exec(ctx, `update agent_run_plans set status=@status,updated_at=now() where agent_run_id=@run and plan_version=@version`, map[string]any{"run": command.RunID, "version": projection.PlanVersion, "status": projection.PlanStatus}); err != nil {
			return err
		}
	}
	_, err = c.AgentRuns.AppendPublicEventIdempotentInTx(ctx, tx, projection.PublicEvent, convergenceID+":public_event")
	return err
}

func (c *RuntimeTerminalConverger) finalizeCapacityReservationTx(ctx context.Context, tx *persistence.Tx, command TerminalConvergenceCommand) error {
	reservation := command.CapacityReservation
	rows, err := tx.Query(ctx, `
select state,version from runtime_capacity_reservations
where capacity_reservation_id=@id and run_id=@run and snapshot_version=@snapshot
for update`, map[string]any{"id": reservation.ReservationID, "run": reservation.RunID, "snapshot": reservation.SnapshotVersion})
	if err != nil || len(rows) != 1 {
		if err != nil {
			return err
		}
		return fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
	}
	state := fmt.Sprint(rows[0]["state"])
	version := runtimeHostInt64(rows[0]["version"])
	if (state == "reserved" || state == "accepted" || state == "recovering") && version != reservation.Version && version != reservation.Version+1 {
		return fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
	}
	switch state {
	case "reserved", "accepted", "recovering":
		usage, _ := json.Marshal(sanitizeRuntimeCapacityUsage(command.ActualUsage))
		if err := tx.Exec(ctx, `
update runtime_capacity_reservations
set state='released',actual_usage=@usage::jsonb,released_at=coalesce(released_at,now()),
    release_reason='terminal',version=version+1,updated_at=now()
where capacity_reservation_id=@id and run_id=@run and snapshot_version=@snapshot`, map[string]any{"id": reservation.ReservationID, "run": reservation.RunID, "snapshot": reservation.SnapshotVersion, "usage": string(usage)}); err != nil {
			return err
		}
	case "released", "expired":
		return nil
	default:
		return fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
	}
	return nil
}

func (c *RuntimeTerminalConverger) finalizeDispatchAndSlotTx(ctx context.Context, tx *persistence.Tx, command TerminalConvergenceCommand) error {
	fence := command.DispatchTerminal.Fence
	// Keep every active capacity transition in Slot -> Dispatch -> Host order.
	// The terminal path previously locked Dispatch first, which could deadlock
	// with acceptance or renewal after either side held its first row lock.
	rows, err := tx.Query(ctx, `
select reservation_id,run_id,runtime_host_id,owner_instance_id,state,fencing_token,lease_token_hash,capability_hash,execution_scope,execution_scope_source,coalesce(dispatch_id,''),expires_at,last_renewed_at,version,created_at,updated_at
from runtime_slot_reservations where reservation_id=@id for update`, map[string]any{"id": fence.ReservationID})
	if err != nil || len(rows) != 1 {
		if err != nil {
			return err
		}
		return fmt.Errorf("STALE_FENCING_TOKEN")
	}
	reservation, err := runtimeReservationFromMap(rows[0])
	if err != nil || !reservationFenceMatches(reservation, fence) {
		return fmt.Errorf("STALE_FENCING_TOKEN")
	}
	dispatchRows, err := tx.Query(ctx, `select state,run_id,runtime_host_id,reservation_id,fencing_token,coalesce(capacity_reservation_id,'') as capacity_reservation_id,coalesce(capacity_reserved_version,0) as capacity_reserved_version from runtime_run_dispatches where dispatch_id=@dispatch for update`, map[string]any{"dispatch": command.DispatchID})
	if err != nil || len(dispatchRows) != 1 {
		if err != nil {
			return err
		}
		return fmt.Errorf("STALE_FENCING_TOKEN")
	}
	dispatch := dispatchRows[0]
	if fmt.Sprint(dispatch["runtime_host_id"]) != fence.RuntimeHostID || fmt.Sprint(dispatch["reservation_id"]) != fence.ReservationID || runtimeHostInt64(dispatch["fencing_token"]) != fence.FencingToken {
		return fmt.Errorf("STALE_FENCING_TOKEN")
	}
	capacity := command.CapacityReservation
	if capacity.ReservationID == "" || capacity.RunID != command.RunID || capacity.SnapshotVersion < 1 || capacity.Version < 1 ||
		fmt.Sprint(dispatch["run_id"]) != command.RunID || fmt.Sprint(dispatch["capacity_reservation_id"]) != capacity.ReservationID || runtimeHostInt64(dispatch["capacity_reserved_version"]) != capacity.Version {
		return fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
	}
	if reservation.State != "released" && reservation.State != "expired" {
		if err := releaseRuntimeHostCounterTx(ctx, tx, reservation); err != nil {
			return err
		}
		if err := tx.Exec(ctx, `
update runtime_slot_reservations
set state='released',released_at=coalesce(released_at,now()),release_reason=@reason,
    version=version+1,updated_at=now()
where reservation_id=@id`, map[string]any{"id": reservation.ReservationID, "reason": "dispatch_" + command.TerminalStatus}); err != nil {
			return err
		}
	}
	currentState := fmt.Sprint(dispatch["state"])
	if currentState != command.TerminalStatus {
		if runtimeTerminalDispatchState(currentState) {
			return fmt.Errorf("STALE_FENCING_TOKEN")
		}
		if err := tx.Exec(ctx, `
update runtime_run_dispatches
set state=@state,terminal_at=coalesce(terminal_at,now()),error_code=nullif(@error,''),
    abort_status=case when @state='aborted' then 'terminal' else abort_status end,
    version=version+1,updated_at=now()
where dispatch_id=@dispatch`, map[string]any{"dispatch": command.DispatchID, "state": command.TerminalStatus, "error": command.DispatchTerminal.ErrorCode}); err != nil {
			return err
		}
	}
	return nil
}

func (c *RuntimeTerminalConverger) finalizeSessionAdmissionTx(ctx context.Context, tx *persistence.Tx, command TerminalConvergenceCommand, convergenceID string) error {
	if !command.SessionRequired {
		return nil
	}
	if command.SessionAdmission == nil {
		rows, err := tx.Query(ctx, `select admission_id,state from runtime_session_admissions where run_id=@run order by created_at desc limit 1 for update`, map[string]any{"run": command.RunID})
		if err != nil || len(rows) != 1 {
			if err != nil {
				return err
			}
			return fmt.Errorf("RUNTIME_SESSION_ADMISSION_UNAVAILABLE")
		}
		if state := fmt.Sprint(rows[0]["state"]); state != "released" && state != "expired" {
			return fmt.Errorf("RUNTIME_SESSION_ADMISSION_UNAVAILABLE")
		}
		return c.Sessions.EnqueueTerminalLeaseCleanupInTx(ctx, tx, convergenceID, fmt.Sprint(rows[0]["admission_id"]))
	}
	handle := *command.SessionAdmission
	if handle.Admission.RunID != command.RunID {
		return fmt.Errorf("STALE_FENCING_TOKEN")
	}
	rows, err := tx.Query(ctx, `
select state from runtime_session_admissions
where admission_id=@id and owner_instance_id=@owner and run_id=@run
  and lease_token_hash=@hash and fencing_token=@fencing
for update`, map[string]any{
		"id": handle.Admission.AdmissionID, "owner": handle.Admission.OwnerInstanceID,
		"run": handle.Admission.RunID, "hash": handle.Admission.LeaseTokenHash,
		"fencing": handle.Admission.FencingToken,
	})
	if err != nil || len(rows) != 1 {
		if err != nil {
			return err
		}
		return fmt.Errorf("STALE_FENCING_TOKEN")
	}
	state := fmt.Sprint(rows[0]["state"])
	if state == "released" || state == "expired" {
		return c.Sessions.EnqueueTerminalLeaseCleanupInTx(ctx, tx, convergenceID, handle.Admission.AdmissionID)
	}
	if !stringInRuntime(state, []string{"acquired", "reservation_bound", "dispatch_bound", "recovering"}) {
		return fmt.Errorf("STALE_FENCING_TOKEN")
	}
	if err := tx.Exec(ctx, `
update runtime_session_admissions
set state='released',release_reason=@reason,version=version+1,updated_at=now()
where admission_id=@id`, map[string]any{"id": handle.Admission.AdmissionID, "reason": normalizeAdmissionReleaseReason(command.TerminalStatus)}); err != nil {
		return err
	}
	return c.Sessions.EnqueueTerminalLeaseCleanupInTx(ctx, tx, convergenceID, handle.Admission.AdmissionID)
}

func validateTerminalAgentRunProjection(command TerminalConvergenceCommand) error {
	projection := command.AgentRunTerminal
	if projection == nil || projection.PlanVersion < 1 || projection.PublicEvent.AgentRunID != command.RunID ||
		projection.PublicEvent.EventType == "" || projection.PublicEvent.Status != projection.AgentRunStatus ||
		projection.PublicEvent.EventType != projection.AgentRunStatus ||
		!stringInRuntime(projection.AgentRunStatus, []string{"succeeded", "failed", "cancelled", "timeout", "orphaned"}) ||
		!stringInRuntime(projection.PlanStatus, []string{"succeeded", "failed", "cancelled"}) {
		return fmt.Errorf("INVALID_ARGUMENT")
	}
	return nil
}

func validateTerminalQueueProof(proof persistence.QueueLeaseProof) error {
	if proof.QueueID == "" || proof.WorkerID == "" || proof.Attempt < 1 || proof.TokenHash == "" || proof.FencingToken < 1 || proof.LeaseExpiresAt.IsZero() {
		return fmt.Errorf("STALE_QUEUE_LEASE")
	}
	return nil
}

func terminalAgentRunTransitionAllowed(status string) bool {
	return stringInRuntime(status, []string{"reserving", "dispatched", "accepted", "materializing", "running", "finalizing", "aborting", "orphaned"})
}

func runtimeTerminalDispatchState(state string) bool {
	return stringInRuntime(state, []string{"succeeded", "failed", "timeout", "aborted", "rejected", "orphaned"})
}

func runtimeTerminalBool(value any) bool {
	if typed, ok := value.(bool); ok {
		return typed
	}
	return fmt.Sprint(value) == "true"
}

func (c *RuntimeTerminalConverger) ensureProgress(ctx context.Context, id string, command TerminalConvergenceCommand) error {
	snapshot, snapshotHash := terminalConvergenceSnapshot(command)
	originalQueueID := terminalConvergenceOriginalQueueID(command)
	if c.postgresReady() {
		payload, _ := json.Marshal(snapshot)
		if _, err := c.DB.Pool.Exec(ctx, `insert into runtime_terminal_convergences(convergence_id,dispatch_id,run_id,queue_id,terminal_source_sequence,terminal_status,terminal_snapshot,snapshot_hash)
values($1,$2,$3,$4,$5,$6,$7::jsonb,$8) on conflict(dispatch_id,terminal_source_sequence) do nothing`,
			id, command.DispatchID, command.RunID, originalQueueID, command.TerminalSourceSequence, command.TerminalStatus, string(payload), snapshotHash); err != nil {
			return err
		}
		var storedID, runID, queueID, terminalStatus, storedHash string
		err := c.DB.Pool.QueryRow(ctx, `select convergence_id,run_id,queue_id,terminal_status,snapshot_hash from runtime_terminal_convergences where dispatch_id=$1 and terminal_source_sequence=$2`, command.DispatchID, command.TerminalSourceSequence).Scan(&storedID, &runID, &queueID, &terminalStatus, &storedHash)
		if err != nil {
			return err
		}
		if storedID != id || runID != command.RunID || queueID != originalQueueID || terminalStatus != command.TerminalStatus || storedHash != snapshotHash {
			return fmt.Errorf("RUNTIME_EVENT_GAP")
		}
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if existing, ok := c.progress[id]; ok {
		if existing.DispatchID != command.DispatchID || existing.RunID != command.RunID || existing.QueueID != originalQueueID || existing.TerminalSourceSequence != command.TerminalSourceSequence || existing.TerminalStatus != command.TerminalStatus || existing.SnapshotHash != snapshotHash {
			return fmt.Errorf("RUNTIME_EVENT_GAP")
		}
		return nil
	}
	c.progress[id] = runtimeTerminalProgress{
		ConvergenceID: id, DispatchID: command.DispatchID, RunID: command.RunID, QueueID: originalQueueID,
		TerminalSourceSequence: command.TerminalSourceSequence, TerminalStatus: command.TerminalStatus,
		SnapshotHash: snapshotHash, Snapshot: cloneTerminalSnapshot(snapshot), UpdatedAt: time.Now().UTC(),
	}
	return nil
}

func (c *RuntimeTerminalConverger) ListIncomplete(ctx context.Context, limit int) ([]TerminalConvergenceRecoveryCandidate, error) {
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}
	if c.postgresReady() {
		rows, err := c.DB.Pool.Query(ctx, `select c.convergence_id,c.dispatch_id,c.run_id,d.runtime_host_id,c.queue_id,c.terminal_source_sequence,c.terminal_status,c.snapshot_hash,c.updated_at
from runtime_terminal_convergences c
join runtime_run_dispatches d on d.dispatch_id=c.dispatch_id
where c.queue_completed_at is null
order by c.updated_at limit $1`, limit)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		out := []TerminalConvergenceRecoveryCandidate{}
		for rows.Next() {
			var item TerminalConvergenceRecoveryCandidate
			if err := rows.Scan(&item.ConvergenceID, &item.DispatchID, &item.RunID, &item.RuntimeHostID, &item.QueueID, &item.TerminalSourceSequence, &item.TerminalStatus, &item.SnapshotHash, &item.UpdatedAt); err != nil {
				return nil, err
			}
			out = append(out, item)
		}
		return out, rows.Err()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	out := []TerminalConvergenceRecoveryCandidate{}
	for _, progress := range c.progress {
		if c.result(progress).Complete {
			continue
		}
		out = append(out, TerminalConvergenceRecoveryCandidate{
			ConvergenceID: progress.ConvergenceID, DispatchID: progress.DispatchID, RunID: progress.RunID,
			QueueID: progress.QueueID, TerminalSourceSequence: progress.TerminalSourceSequence,
			TerminalStatus: progress.TerminalStatus, SnapshotHash: progress.SnapshotHash, UpdatedAt: progress.UpdatedAt,
		})
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

// FindIncompleteByQueueID reconstructs an incomplete convergence from its
// immutable terminal snapshot. It is used before any new Runtime polling so a
// completed Runtime can still converge after its event/status retention window
// has elapsed.
func (c *RuntimeTerminalConverger) FindIncompleteByQueueID(ctx context.Context, queueID string) (TerminalConvergenceRecovery, bool, error) {
	if queueID == "" {
		return TerminalConvergenceRecovery{}, false, fmt.Errorf("INVALID_ARGUMENT")
	}
	if c.postgresReady() {
		var candidate TerminalConvergenceRecoveryCandidate
		var snapshotRaw []byte
		err := c.DB.Pool.QueryRow(ctx, `select c.convergence_id,c.dispatch_id,c.run_id,d.runtime_host_id,c.queue_id,c.terminal_source_sequence,c.terminal_status,c.snapshot_hash,c.terminal_snapshot,
       c.events_verified_at is not null,c.product_projected_at is not null,c.usage_settled_at is not null,c.agent_run_converged_at is not null,c.dispatch_finalized_at is not null,c.session_released_at is not null,c.updated_at
from runtime_terminal_convergences c
join runtime_run_dispatches d on d.dispatch_id=c.dispatch_id
where c.queue_completed_at is null
  and (
    c.queue_id=$1
    or exists(
      select 1
      from runtime_terminal_convergence_recovery_queue_lineage lineage
      where lineage.convergence_id=c.convergence_id
        and lineage.recovery_queue_id=$1
    )
  )
order by c.updated_at limit 1`, queueID).Scan(
			&candidate.ConvergenceID, &candidate.DispatchID, &candidate.RunID, &candidate.RuntimeHostID, &candidate.QueueID,
			&candidate.TerminalSourceSequence, &candidate.TerminalStatus, &candidate.SnapshotHash, &snapshotRaw,
			&candidate.EventsVerified, &candidate.ProductProjected, &candidate.UsageSettled, &candidate.AgentRunConverged, &candidate.DispatchFinalized, &candidate.SessionReleased, &candidate.UpdatedAt,
		)
		if errors.Is(err, pgx.ErrNoRows) {
			return TerminalConvergenceRecovery{}, false, nil
		}
		if err != nil {
			return TerminalConvergenceRecovery{}, false, err
		}
		snapshot := map[string]any{}
		if err := json.Unmarshal(snapshotRaw, &snapshot); err != nil {
			return TerminalConvergenceRecovery{}, false, fmt.Errorf("RUNTIME_EVENT_GAP")
		}
		recovery, err := terminalConvergenceRecoveryFromSnapshot(candidate, snapshot)
		if err != nil {
			return TerminalConvergenceRecovery{}, false, err
		}
		return recovery, true, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, progress := range c.progress {
		if progress.QueueID != queueID || c.result(progress).Complete {
			continue
		}
		candidate := TerminalConvergenceRecoveryCandidate{
			ConvergenceID: progress.ConvergenceID, DispatchID: progress.DispatchID, RunID: progress.RunID,
			QueueID: progress.QueueID, TerminalSourceSequence: progress.TerminalSourceSequence,
			TerminalStatus: progress.TerminalStatus, SnapshotHash: progress.SnapshotHash,
			EventsVerified: progress.EventsVerified, ProductProjected: progress.ProductProjected, UsageSettled: progress.UsageSettled,
			AgentRunConverged: progress.AgentRunConverged, DispatchFinalized: progress.DispatchFinalized, SessionReleased: progress.SessionReleased,
			UpdatedAt: progress.UpdatedAt,
		}
		recovery, err := terminalConvergenceRecoveryFromSnapshot(candidate, cloneTerminalSnapshot(progress.Snapshot))
		if err != nil {
			return TerminalConvergenceRecovery{}, false, err
		}
		return recovery, true, nil
	}
	return TerminalConvergenceRecovery{}, false, nil
}

func (c *RuntimeTerminalConverger) getProgress(ctx context.Context, id string) (runtimeTerminalProgress, error) {
	if c.postgresReady() {
		var p runtimeTerminalProgress
		p.ConvergenceID = id
		err := c.DB.Pool.QueryRow(ctx, `select events_verified_at is not null,product_projected_at is not null,usage_settled_at is not null,agent_run_converged_at is not null,dispatch_finalized_at is not null,session_released_at is not null,public_event_appended_at is not null,queue_completed_at is not null from runtime_terminal_convergences where convergence_id=$1`, id).Scan(&p.EventsVerified, &p.ProductProjected, &p.UsageSettled, &p.AgentRunConverged, &p.DispatchFinalized, &p.SessionReleased, &p.PublicEventAppended, &p.QueueCompleted)
		return p, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	p, ok := c.progress[id]
	if !ok {
		return runtimeTerminalProgress{}, fmt.Errorf("NOT_FOUND")
	}
	return p, nil
}

func (c *RuntimeTerminalConverger) markStep(ctx context.Context, id, step string) error {
	columns := map[string]string{"events_verified": "events_verified_at", "product_projected": "product_projected_at", "usage_settled": "usage_settled_at", "agent_run_converged": "agent_run_converged_at", "dispatch_finalized": "dispatch_finalized_at", "session_released": "session_released_at", "public_event_appended": "public_event_appended_at", "queue_completed": "queue_completed_at"}
	column := columns[step]
	if column == "" {
		return fmt.Errorf("INVALID_ARGUMENT")
	}
	if c.postgresReady() {
		_, err := c.DB.Pool.Exec(ctx, `update runtime_terminal_convergences set `+column+`=coalesce(`+column+`,now()),last_error_code=null,attempt_count=attempt_count+1,updated_at=now() where convergence_id=$1`, id)
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	p := c.progress[id]
	p.UpdatedAt = time.Now().UTC()
	switch step {
	case "events_verified":
		p.EventsVerified = true
	case "product_projected":
		p.ProductProjected = true
	case "usage_settled":
		p.UsageSettled = true
	case "agent_run_converged":
		p.AgentRunConverged = true
	case "dispatch_finalized":
		p.DispatchFinalized = true
	case "session_released":
		p.SessionReleased = true
	case "public_event_appended":
		p.PublicEventAppended = true
	case "queue_completed":
		p.QueueCompleted = true
	}
	c.progress[id] = p
	return nil
}

func terminalConvergenceSnapshot(command TerminalConvergenceCommand) (map[string]any, string) {
	snapshot := map[string]any{
		"runId": command.RunID, "dispatchId": command.DispatchID,
		"terminalSourceSequence": command.TerminalSourceSequence, "terminalStatus": command.TerminalStatus,
		"safeResult": sanitizeRuntimeHostEvent(command.SafeResult), "safeError": sanitizeRuntimeHostEvent(command.SafeError),
		"actualUsage": sanitizeRuntimeCapacityUsage(command.ActualUsage), "queueId": terminalConvergenceOriginalQueueID(command),
		"sessionRequired":         command.SessionRequired,
		"capacityReservationId":   command.CapacityReservation.ReservationID,
		"capacitySnapshotVersion": command.CapacityReservation.SnapshotVersion,
		"capacityReservedVersion": command.CapacityReservation.Version,
	}
	return snapshot, terminalConvergenceSnapshotHash(snapshot)
}

func terminalConvergenceOriginalQueueID(command TerminalConvergenceCommand) string {
	if value := strings.TrimSpace(command.OriginalQueueID); value != "" {
		return value
	}
	return command.QueueProof.QueueID
}

func terminalConvergenceRecoveryFromSnapshot(candidate TerminalConvergenceRecoveryCandidate, snapshot map[string]any) (TerminalConvergenceRecovery, error) {
	if candidate.ConvergenceID == "" || candidate.DispatchID == "" || candidate.RunID == "" || candidate.QueueID == "" || candidate.TerminalSourceSequence < 1 || candidate.SnapshotHash == "" ||
		terminalSnapshotString(snapshot, "dispatchId") != candidate.DispatchID || terminalSnapshotString(snapshot, "runId") != candidate.RunID || terminalSnapshotString(snapshot, "queueId") != candidate.QueueID ||
		terminalSnapshotString(snapshot, "terminalStatus") != candidate.TerminalStatus || terminalSnapshotInt64(snapshot["terminalSourceSequence"]) != candidate.TerminalSourceSequence ||
		terminalConvergenceSnapshotHash(snapshot) != candidate.SnapshotHash {
		return TerminalConvergenceRecovery{}, fmt.Errorf("RUNTIME_EVENT_GAP")
	}
	return TerminalConvergenceRecovery{
		ConvergenceID: candidate.ConvergenceID, DispatchID: candidate.DispatchID, RunID: candidate.RunID,
		RuntimeHostID: candidate.RuntimeHostID, QueueID: candidate.QueueID, TerminalSourceSequence: candidate.TerminalSourceSequence,
		TerminalStatus: candidate.TerminalStatus, SnapshotHash: candidate.SnapshotHash,
		SafeResult: terminalSnapshotMap(snapshot, "safeResult"), SafeError: terminalSnapshotMap(snapshot, "safeError"),
		ActualUsage: terminalSnapshotMap(snapshot, "actualUsage"), SessionRequired: terminalSnapshotBool(snapshot["sessionRequired"]),
		CapacityReservationID: terminalSnapshotString(snapshot, "capacityReservationId"), CapacitySnapshotVersion: terminalSnapshotInt64(snapshot["capacitySnapshotVersion"]), CapacityReservedVersion: terminalSnapshotInt64(snapshot["capacityReservedVersion"]),
		EventsVerified: candidate.EventsVerified, ProductProjected: candidate.ProductProjected, UsageSettled: candidate.UsageSettled,
		AgentRunConverged: candidate.AgentRunConverged, DispatchFinalized: candidate.DispatchFinalized, SessionReleased: candidate.SessionReleased,
		UpdatedAt: candidate.UpdatedAt,
	}, nil
}

func terminalConvergenceSnapshotHash(snapshot map[string]any) string {
	raw, _ := json.Marshal(snapshot)
	hash := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(hash[:])
}

func cloneTerminalSnapshot(snapshot map[string]any) map[string]any {
	if len(snapshot) == 0 {
		return map[string]any{}
	}
	raw, err := json.Marshal(snapshot)
	if err != nil {
		return map[string]any{}
	}
	clone := map[string]any{}
	if json.Unmarshal(raw, &clone) != nil {
		return map[string]any{}
	}
	return clone
}

func terminalSnapshotMap(snapshot map[string]any, key string) map[string]any {
	value, _ := snapshot[key].(map[string]any)
	return cloneTerminalSnapshot(value)
}

func terminalSnapshotString(snapshot map[string]any, key string) string {
	return fmt.Sprint(snapshot[key])
}

func terminalSnapshotBool(value any) bool {
	typed, _ := value.(bool)
	return typed
}

func terminalSnapshotInt64(value any) int64 {
	switch typed := value.(type) {
	case int64:
		return typed
	case int:
		return int64(typed)
	case float64:
		return int64(typed)
	case json.Number:
		parsed, _ := typed.Int64()
		return parsed
	default:
		var out int64
		_, _ = fmt.Sscan(fmt.Sprint(value), &out)
		return out
	}
}

func (c *RuntimeTerminalConverger) recordProgressError(ctx context.Context, id, code string) error {
	if c.postgresReady() {
		_, err := c.DB.Pool.Exec(ctx, `update runtime_terminal_convergences set last_error_code=$2,attempt_count=attempt_count+1,updated_at=now() where convergence_id=$1`, id, code)
		return err
	}
	return nil
}
func (c *RuntimeTerminalConverger) postgresReady() bool {
	return c != nil && c.DB != nil && !c.DB.Disabled && c.DB.Pool != nil
}
func (c *RuntimeTerminalConverger) result(p runtimeTerminalProgress) TerminalConvergenceResult {
	completed := []string{}
	for _, item := range []struct {
		name string
		done bool
	}{{"events_verified", p.EventsVerified}, {"product_projected", p.ProductProjected}, {"usage_settled", p.UsageSettled}, {"agent_run_converged", p.AgentRunConverged}, {"dispatch_finalized", p.DispatchFinalized}, {"session_released", p.SessionReleased}, {"public_event_appended", p.PublicEventAppended}, {"queue_completed", p.QueueCompleted}} {
		if item.done {
			completed = append(completed, item.name)
		}
	}
	return TerminalConvergenceResult{ConvergenceID: p.ConvergenceID, Complete: len(completed) == 8, Completed: completed}
}
func validateTerminalConvergenceCommand(command TerminalConvergenceCommand) error {
	if command.DispatchID == "" || command.RunID == "" || command.TerminalSourceSequence < 1 || !stringInRuntime(command.TerminalStatus, []string{"succeeded", "failed", "timeout", "aborted", "rejected", "orphaned"}) || command.DispatchTerminal.DispatchID != command.DispatchID || command.QueueProof.QueueID == "" {
		return fmt.Errorf("INVALID_ARGUMENT")
	}
	if command.SessionAdmission != nil && (!command.SessionRequired || command.SessionAdmission.Admission.RunID != command.RunID) {
		return fmt.Errorf("INVALID_ARGUMENT")
	}
	return nil
}

// validateTerminalDispatchBinding is intentionally a read-only preflight. A
// terminal command has no independent RuntimeHostID field: the Host assertion
// is carried by DispatchTerminal.Fence and must match the dispatch selected for
// the command Run. This check has to precede ensureProgress because that method
// persists the immutable terminal snapshot on the PostgreSQL path.
func (c *RuntimeTerminalConverger) validateTerminalDispatchBinding(ctx context.Context, command TerminalConvergenceCommand) error {
	if c == nil || c.Hosts == nil {
		return fmt.Errorf("RUNTIME_TERMINAL_CONVERGENCE_INCONSISTENT")
	}
	dispatch, err := c.Hosts.GetDispatch(ctx, command.DispatchID)
	if err != nil {
		return err
	}
	if dispatch.DispatchID != command.DispatchID || dispatch.RunID != command.RunID || dispatch.RuntimeHostID != command.DispatchTerminal.Fence.RuntimeHostID {
		return fmt.Errorf("RUNTIME_TERMINAL_CONVERGENCE_INCONSISTENT")
	}
	return nil
}

func terminalErrorCode(err error) string {
	if err == nil {
		return ""
	}
	value := err.Error()
	if len(value) > 80 {
		value = value[:80]
	}
	return value
}

func (r *RuntimeHostRepository) VerifyDispatchEventContinuity(ctx context.Context, dispatchID string, through int64) error {
	if dispatchID == "" || through < 1 {
		return fmt.Errorf("INVALID_ARGUMENT")
	}
	if r.postgresReady() {
		var cursor, lower int64
		var gapAt *time.Time
		err := r.db.Pool.QueryRow(ctx, `select event_cursor,event_lower_bound,event_gap_detected_at from runtime_run_dispatches where dispatch_id=$1`, dispatchID).Scan(&cursor, &lower, &gapAt)
		if err != nil {
			return err
		}
		if gapAt != nil || cursor < through {
			return fmt.Errorf("RUNTIME_EVENT_GAP")
		}
		var count, min, max int64
		err = r.db.Pool.QueryRow(ctx, `select count(*),coalesce(min(source_sequence),0),coalesce(max(source_sequence),0) from runtime_run_events where dispatch_id=$1 and source_sequence between $2 and $3`, dispatchID, lower, through).Scan(&count, &min, &max)
		if err != nil {
			return err
		}
		if min != lower || max != through || count != through-lower+1 {
			return fmt.Errorf("RUNTIME_EVENT_GAP")
		}
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.dispatchEventCursors[dispatchID] < through {
		return fmt.Errorf("RUNTIME_EVENT_GAP")
	}
	seen := map[int64]bool{}
	for _, events := range r.events {
		for _, event := range events {
			if event.DispatchID == dispatchID && event.SourceSequence <= through {
				seen[event.SourceSequence] = true
			}
		}
	}
	for seq := int64(1); seq <= through; seq++ {
		if !seen[seq] {
			return fmt.Errorf("RUNTIME_EVENT_GAP")
		}
	}
	return nil
}
