package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"huahuoai/backend/source/internal/persistence"
)

type AtomicReservationCommand struct {
	ReservationID         string
	RunID                 string
	OwnerInstanceID       string
	ExecutionScope        string
	CapabilityHash        string
	RuntimeVersion        string
	AdapterVersion        string
	RequiredTools         []string
	AffinityRuntimeHostID string
	LeaseTokenHash        string
	FencingToken          int64
	ExpiresAt             time.Time
	HeartbeatAfter        time.Time
}

type ReservationFence struct {
	ReservationID   string
	RuntimeHostID   string
	OwnerInstanceID string
	LeaseTokenHash  string
	FencingToken    int64
}

type DispatchAcceptedCommand struct {
	Fence            ReservationFence
	DispatchID       string
	RuntimeRequestID string
}

type ReservationReleaseCommand struct {
	Fence  ReservationFence
	Reason string
}

type DispatchTerminalCommand struct {
	Fence          ReservationFence
	DispatchID     string
	TerminalStatus string
	ErrorCode      string
	// RecoveryClaim is required only for a recovery-owner terminal transition.
	// It prevents a worker whose recovery lease expired while querying the
	// original Host from releasing a slot claimed by its successor.
	RecoveryClaim *DispatchRecoveryClaim
}

type DispatchRecoveryClaim struct {
	DispatchID      string
	OwnerInstanceID string
	FencingToken    int64
	ExpiresAt       time.Time
	ExpectedState   string
	ExpectedVersion int64
}

type DispatchRecoveryAcceptedCommand struct {
	Claim            DispatchRecoveryClaim
	Fence            ReservationFence
	RuntimeRequestID string
	NextCheckAt      time.Time
}

type RuntimeDispatchEventCursor struct {
	DispatchID   string
	LastSequence int64
	LowerBound   int64
	UpperBound   int64
	GapExpected  int64
	GapObserved  int64
}

// A dispatched Run is given a short observation window before the recovery
// worker probes its original Host. This keeps a newly accepted Run off the
// recovery path while still giving an accepted Run a durable first check.
const runtimeInitialDispatchRecoveryDelay = 5 * time.Second

func (r *RuntimeHostRepository) TryReserveSlot(ctx context.Context, command AtomicReservationCommand) (RuntimeSlotReservation, RuntimeHost, error) {
	if err := validateAtomicReservationCommand(command); err != nil {
		return RuntimeSlotReservation{}, RuntimeHost{}, err
	}
	if r.postgresReady() {
		return r.tryReserveSlotPostgres(ctx, command)
	}
	return r.tryReserveSlotMemory(command)
}

func (r *RuntimeHostRepository) tryReserveSlotPostgres(ctx context.Context, command AtomicReservationCommand) (RuntimeSlotReservation, RuntimeHost, error) {
	var reservation RuntimeSlotReservation
	var selected RuntimeHost
	err := r.db.WithSerializableRetry(ctx, "runtime_try_reserve_slot", 3, func(tx *persistence.Tx) error {
		// Candidate discovery is deliberately unlocked. The conditional UPDATE below
		// is the one-row reservation boundary; locking a ranked candidate set would
		// serialize unrelated Hosts behind a busy least-loaded Host.
		rows, err := tx.Query(ctx, runtimeHostSelect+`
where status='ready'
  and recovery_state='reconciled'
  and capability_hash=@capability
  and (@runtimeVersion='' or runtime_version=@runtimeVersion)
  and (@adapterVersion='' or adapter_version=@adapterVersion)
  and (@heartbeatAfter::timestamptz is null or last_heartbeat_at>=@heartbeatAfter)
  and (@affinity='' or runtime_host_id=@affinity)
  and active_runs + reserved_runs < greatest(1,(max_active_runs*3+3)/4)
  and case when @scope='product_thread'
      then active_product_thread_runs + reserved_product_thread_runs < greatest(1,(max_product_thread_runs*3+3)/4)
      else active_detached_task_runs + reserved_detached_task_runs < greatest(1,(max_detached_task_runs*3+3)/4)
      end
order by ((active_runs+reserved_runs)::numeric / greatest(max_active_runs,1)), runtime_host_id`, map[string]any{
			"capability": command.CapabilityHash, "runtimeVersion": command.RuntimeVersion,
			"adapterVersion": command.AdapterVersion, "heartbeatAfter": nullableRuntimeTime(command.HeartbeatAfter),
			"affinity": command.AffinityRuntimeHostID, "scope": command.ExecutionScope,
		})
		if err != nil {
			return err
		}
		budgetUnsupported := false
		for _, row := range rows {
			candidate, err := runtimeHostFromMap(row)
			if err != nil {
				return err
			}
			if runtimeHostRecoveryAttestationRequired(candidate.Environment) {
				if err := ValidateRuntimeCapabilitySnapshot(candidate.Capabilities); err != nil {
					if strings.Contains(err.Error(), "RUNTIME_TOOL_BUDGET_UNSUPPORTED") {
						budgetUnsupported = true
					}
					continue
				}
			}
			if !runtimeHostHasTools(candidate, command.RequiredTools) {
				continue
			}

			claimedRows, err := tx.Query(ctx, `update runtime_hosts set
reserved_runs=reserved_runs+1,
reserved_product_thread_runs=reserved_product_thread_runs+case when @scope='product_thread' then 1 else 0 end,
reserved_detached_task_runs=reserved_detached_task_runs+case when @scope='detached_task' then 1 else 0 end,
updated_at=now()
where runtime_host_id=@host
  and status='ready'
  and recovery_state='reconciled'
  and capability_hash=@capability
  and (@runtimeVersion='' or runtime_version=@runtimeVersion)
  and (@adapterVersion='' or adapter_version=@adapterVersion)
  and (@heartbeatAfter::timestamptz is null or last_heartbeat_at>=@heartbeatAfter)
  and (@affinity='' or runtime_host_id=@affinity)
  and active_runs + reserved_runs < greatest(1,(max_active_runs*3+3)/4)
  and case when @scope='product_thread'
      then active_product_thread_runs + reserved_product_thread_runs < greatest(1,(max_product_thread_runs*3+3)/4)
      else active_detached_task_runs + reserved_detached_task_runs < greatest(1,(max_detached_task_runs*3+3)/4)
      end
returning `+runtimeHostColumns, map[string]any{
				"host": candidate.RuntimeHostID, "capability": command.CapabilityHash,
				"runtimeVersion": command.RuntimeVersion, "adapterVersion": command.AdapterVersion,
				"heartbeatAfter": nullableRuntimeTime(command.HeartbeatAfter),
				"affinity":       command.AffinityRuntimeHostID, "scope": command.ExecutionScope,
			})
			if err != nil {
				return err
			}
			if len(claimedRows) != 1 {
				continue
			}
			selected, err = runtimeHostFromMap(claimedRows[0])
			if err != nil {
				return err
			}
			if runtimeHostRecoveryAttestationRequired(selected.Environment) {
				if err := ValidateRuntimeCapabilitySnapshot(selected.Capabilities); err != nil {
					return err
				}
			}
			if !runtimeHostHasTools(selected, command.RequiredTools) {
				return fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
			}
			break
		}
		if selected.RuntimeHostID == "" {
			if budgetUnsupported {
				return fmt.Errorf("RUNTIME_TOOL_BUDGET_UNSUPPORTED")
			}
			return fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
		}
		now := time.Now().UTC()
		reservation = RuntimeSlotReservation{
			ReservationID: command.ReservationID, RunID: command.RunID, RuntimeHostID: selected.RuntimeHostID,
			AssignedRuntimeHostInstanceID:         selected.InstanceID,
			AssignedRuntimeHostInstanceGeneration: selected.InstanceGeneration,
			OwnerInstanceID:                       command.OwnerInstanceID, State: "reserved", FencingToken: command.FencingToken,
			LeaseTokenHash: command.LeaseTokenHash, CapabilityHash: command.CapabilityHash,
			ExecutionScope: command.ExecutionScope, ExecutionScopeSource: runtimeExecutionScopeSourceExplicit, Version: 1, LastRenewedAt: now,
			ExpiresAt: command.ExpiresAt, CreatedAt: now, UpdatedAt: now,
		}
		if err := tx.Exec(ctx, `insert into runtime_slot_reservations(
reservation_id,run_id,runtime_host_id,owner_instance_id,state,fencing_token,lease_token_hash,
 capability_hash,execution_scope,execution_scope_source,assigned_runtime_host_instance_id,assigned_runtime_host_instance_generation,expires_at,last_renewed_at,version)
 values(@id,@run,@host,@owner,'reserved',@fencing,@tokenHash,@capability,@scope,@scopeSource,@assignedInstanceID,@assignedGeneration,@expires,@renewed,1)`, map[string]any{
			"id": reservation.ReservationID, "run": reservation.RunID, "host": reservation.RuntimeHostID,
			"owner": reservation.OwnerInstanceID, "fencing": reservation.FencingToken,
			"tokenHash": reservation.LeaseTokenHash, "capability": reservation.CapabilityHash,
			"scope": reservation.ExecutionScope, "scopeSource": reservation.ExecutionScopeSource, "assignedInstanceID": reservation.AssignedRuntimeHostInstanceID,
			"assignedGeneration": reservation.AssignedRuntimeHostInstanceGeneration,
			"expires":            reservation.ExpiresAt, "renewed": reservation.LastRenewedAt,
		}); err != nil {
			if runtimeUniqueViolation(err) {
				return fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
			}
			return err
		}
		return nil
	})
	if err != nil {
		return RuntimeSlotReservation{}, RuntimeHost{}, err
	}
	return reservation, selected, nil
}

func (r *RuntimeHostRepository) tryReserveSlotMemory(command AtomicReservationCommand) (RuntimeSlotReservation, RuntimeHost, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.activeReservationByRun[command.RunID] != "" {
		return RuntimeSlotReservation{}, RuntimeHost{}, fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
	}
	hosts := []RuntimeHost{}
	budgetUnsupported := false
	for _, host := range r.hosts {
		if host.Status != "ready" || host.RecoveryState != "reconciled" || host.CapabilityHash != command.CapabilityHash ||
			(command.RuntimeVersion != "" && host.RuntimeVersion != command.RuntimeVersion) ||
			(command.AdapterVersion != "" && host.AdapterVersion != command.AdapterVersion) ||
			(!command.HeartbeatAfter.IsZero() && host.LastHeartbeatAt.Before(command.HeartbeatAfter)) ||
			(command.AffinityRuntimeHostID != "" && host.RuntimeHostID != command.AffinityRuntimeHostID) ||
			!runtimeHostWithinTarget(host, command.ExecutionScope) || !runtimeHostHasTools(host, command.RequiredTools) {
			continue
		}
		if runtimeHostRecoveryAttestationRequired(host.Environment) {
			if err := ValidateRuntimeCapabilitySnapshot(host.Capabilities); err != nil {
				if strings.Contains(err.Error(), "RUNTIME_TOOL_BUDGET_UNSUPPORTED") {
					budgetUnsupported = true
				}
				continue
			}
		}
		hosts = append(hosts, host)
	}
	if len(hosts) == 0 {
		if budgetUnsupported {
			return RuntimeSlotReservation{}, RuntimeHost{}, fmt.Errorf("RUNTIME_TOOL_BUDGET_UNSUPPORTED")
		}
		return RuntimeSlotReservation{}, RuntimeHost{}, fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
	}
	sort.Slice(hosts, func(i, j int) bool {
		left := float64(hosts[i].ActiveRuns+hosts[i].ReservedRuns) / float64(hosts[i].MaxActiveRuns)
		right := float64(hosts[j].ActiveRuns+hosts[j].ReservedRuns) / float64(hosts[j].MaxActiveRuns)
		if left == right {
			return hosts[i].RuntimeHostID < hosts[j].RuntimeHostID
		}
		return left < right
	})
	selected := hosts[0]
	now := time.Now().UTC()
	reservation := RuntimeSlotReservation{
		ReservationID: command.ReservationID, RunID: command.RunID, RuntimeHostID: selected.RuntimeHostID,
		AssignedRuntimeHostInstanceID:         selected.InstanceID,
		AssignedRuntimeHostInstanceGeneration: selected.InstanceGeneration,
		OwnerInstanceID:                       command.OwnerInstanceID, State: "reserved", FencingToken: command.FencingToken,
		LeaseTokenHash: command.LeaseTokenHash, CapabilityHash: command.CapabilityHash,
		ExecutionScope: command.ExecutionScope, ExecutionScopeSource: runtimeExecutionScopeSourceExplicit, Version: 1, LastRenewedAt: now,
		ExpiresAt: command.ExpiresAt, CreatedAt: now, UpdatedAt: now,
	}
	incrementRuntimeHostReserved(&selected, command.ExecutionScope)
	r.hosts[selected.RuntimeHostID] = selected
	r.reservations[reservation.ReservationID] = reservation
	r.activeReservationByRun[reservation.RunID] = reservation.ReservationID
	return reservation, selected, nil
}

func (r *RuntimeHostRepository) RenewReservation(ctx context.Context, fence ReservationFence, expiresAt time.Time) error {
	if err := validateReservationFence(fence); err != nil || expiresAt.IsZero() {
		if err != nil {
			return err
		}
		return fmt.Errorf("INVALID_ARGUMENT")
	}
	if r.postgresReady() {
		return r.db.WithTx(ctx, func(tx *persistence.Tx) error {
			rows, err := tx.Query(ctx, `select state,coalesce(dispatch_id,'') dispatch_id from runtime_slot_reservations
where reservation_id=@reservation and runtime_host_id=@host and owner_instance_id=@owner and lease_token_hash=@token and fencing_token=@fencing
and state in('reserved','accepted','running') and expires_at>now() for update`, map[string]any{
				"reservation": fence.ReservationID, "host": fence.RuntimeHostID, "owner": fence.OwnerInstanceID,
				"token": fence.LeaseTokenHash, "fencing": fence.FencingToken,
			})
			if err != nil || len(rows) != 1 {
				return fmt.Errorf("STALE_FENCING_TOKEN")
			}
			dispatchID := fmt.Sprint(rows[0]["dispatch_id"])
			if err := tx.Exec(ctx, `update runtime_slot_reservations set expires_at=@expires,last_renewed_at=now(),version=version+1,updated_at=now() where reservation_id=@reservation`, map[string]any{"reservation": fence.ReservationID, "expires": expiresAt}); err != nil {
				return err
			}
			if dispatchID == "" {
				return nil
			}
			tag, err := tx.ExecRaw(ctx, `update runtime_run_dispatches set lease_expires_at=$7,version=version+1,updated_at=now()
where dispatch_id=$1 and reservation_id=$2 and runtime_host_id=$3 and owner_instance_id=$4
and lease_token_hash=$5 and fencing_token=$6 and state in('created','sent','submit_unknown','retry_same_host','accepted','materializing','running','finalizing','recovering')`,
				dispatchID, fence.ReservationID, fence.RuntimeHostID, fence.OwnerInstanceID, fence.LeaseTokenHash, fence.FencingToken, expiresAt)
			if err != nil {
				return err
			}
			if tag.RowsAffected() != 1 {
				return fmt.Errorf("STALE_FENCING_TOKEN")
			}
			return nil
		})
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	item, ok := r.reservations[fence.ReservationID]
	if !ok || !reservationFenceMatches(item, fence) || !stringInRuntime(item.State, []string{"reserved", "accepted", "running"}) || !item.ExpiresAt.After(time.Now().UTC()) {
		return fmt.Errorf("STALE_FENCING_TOKEN")
	}
	var dispatch RuntimeDispatch
	if item.DispatchID != "" {
		dispatch, ok = r.dispatches[item.DispatchID]
		if !ok || dispatch.ReservationID != item.ReservationID || dispatch.RuntimeHostID != item.RuntimeHostID || dispatch.OwnerInstanceID != item.OwnerInstanceID || dispatch.LeaseTokenHash != item.LeaseTokenHash || dispatch.FencingToken != item.FencingToken || !activeRuntimeDispatchState(dispatch.State) {
			return fmt.Errorf("STALE_FENCING_TOKEN")
		}
	}
	item.ExpiresAt, item.LastRenewedAt, item.UpdatedAt = expiresAt, time.Now().UTC(), time.Now().UTC()
	item.Version++
	r.reservations[item.ReservationID] = item
	if item.DispatchID != "" {
		dispatch.LeaseExpiresAt = expiresAt
		dispatch.Version++
		dispatch.UpdatedAt = time.Now().UTC()
		r.dispatches[dispatch.DispatchID] = dispatch
	}
	return nil
}

func (r *RuntimeHostRepository) MarkDispatchLeaseLost(ctx context.Context, dispatchID string, fence ReservationFence, nextCheck time.Time) error {
	if dispatchID == "" || validateReservationFence(fence) != nil {
		return fmt.Errorf("INVALID_ARGUMENT")
	}
	if nextCheck.IsZero() {
		nextCheck = time.Now().UTC()
	}
	if r.postgresReady() {
		result, err := r.db.Pool.Exec(ctx, `update runtime_run_dispatches set state='recovering',lease_expires_at=least(lease_expires_at,now()),next_recovery_check_at=$6,version=version+1,updated_at=now()
where dispatch_id=$1 and reservation_id=$2 and runtime_host_id=$3 and owner_instance_id=$4 and fencing_token=$5
and lease_token_hash=$7 and state in('created','sent','submit_unknown','retry_same_host','accepted','materializing','running','finalizing','recovering')`,
			dispatchID, fence.ReservationID, fence.RuntimeHostID, fence.OwnerInstanceID, fence.FencingToken, nextCheck, fence.LeaseTokenHash)
		if err != nil {
			return err
		}
		if result.RowsAffected() != 1 {
			return fmt.Errorf("STALE_FENCING_TOKEN")
		}
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	dispatch, ok := r.dispatches[dispatchID]
	if !ok || dispatch.ReservationID != fence.ReservationID || dispatch.RuntimeHostID != fence.RuntimeHostID || dispatch.OwnerInstanceID != fence.OwnerInstanceID || dispatch.LeaseTokenHash != fence.LeaseTokenHash || dispatch.FencingToken != fence.FencingToken || !activeRuntimeDispatchState(dispatch.State) {
		return fmt.Errorf("STALE_FENCING_TOKEN")
	}
	dispatch.State = "recovering"
	dispatch.LeaseExpiresAt = time.Now().UTC()
	dispatch.NextRecoveryCheckAt = nextCheck
	dispatch.Version++
	dispatch.UpdatedAt = time.Now().UTC()
	r.dispatches[dispatchID] = dispatch
	return nil
}

func (r *RuntimeHostRepository) ConfirmDispatchAccepted(ctx context.Context, command DispatchAcceptedCommand) error {
	if err := validateDispatchAcceptedCommand(command); err != nil {
		return err
	}
	if r.postgresReady() {
		return r.confirmDispatchAcceptedPostgres(ctx, command)
	}
	return r.confirmDispatchAcceptedMemory(command)
}

func (r *RuntimeHostRepository) confirmDispatchAcceptedPostgres(ctx context.Context, command DispatchAcceptedCommand) error {
	return r.db.WithTx(ctx, func(tx *persistence.Tx) error {
		return r.confirmDispatchAcceptedTx(ctx, tx, command)
	})
}

// confirmDispatchAcceptedTx is the Host/Slot half of durable acceptance. The
// Scheduler composes it with the capacity generation CAS in one caller-owned
// serializable transaction; direct repository callers retain the WithTx wrapper
// above. No external Runtime call may occur inside this transaction.
func (r *RuntimeHostRepository) confirmDispatchAcceptedTx(ctx context.Context, tx *persistence.Tx, command DispatchAcceptedCommand) error {
	if tx == nil {
		return fmt.Errorf("INVALID_ARGUMENT")
	}
	if err := validateDispatchAcceptedCommand(command); err != nil {
		return err
	}
	// All mutations spanning these rows use Slot -> Dispatch -> Host. Renewal
	// follows the same sequence (it locks the Slot then updates its Dispatch),
	// so an acceptance, terminalization, or recovery transaction cannot wait on
	// the same pair in the opposite direction.
	nextRecoveryCheckAt := time.Now().UTC().Add(runtimeInitialDispatchRecoveryDelay)
	rows, err := tx.Query(ctx, `select reservation_id,run_id,runtime_host_id,owner_instance_id,state,fencing_token,lease_token_hash,capability_hash,execution_scope,execution_scope_source,coalesce(dispatch_id,''),expires_at,last_renewed_at,version,created_at,updated_at
from runtime_slot_reservations where reservation_id=@id for update`, map[string]any{"id": command.Fence.ReservationID})
	if err != nil || len(rows) != 1 {
		return fmt.Errorf("STALE_FENCING_TOKEN")
	}
	reservation, err := runtimeReservationFromMap(rows[0])
	if err != nil || !reservationFenceMatches(reservation, command.Fence) {
		return fmt.Errorf("STALE_FENCING_TOKEN")
	}
	dispatchRows, err := tx.Query(ctx, `select state,runtime_host_id,reservation_id,fencing_token,coalesce(runtime_request_id,'') runtime_request_id from runtime_run_dispatches where dispatch_id=@dispatch for update`, map[string]any{"dispatch": command.DispatchID})
	if err != nil || len(dispatchRows) != 1 {
		return fmt.Errorf("STALE_FENCING_TOKEN")
	}
	dispatch := dispatchRows[0]
	if fmt.Sprint(dispatch["runtime_host_id"]) != command.Fence.RuntimeHostID || fmt.Sprint(dispatch["reservation_id"]) != command.Fence.ReservationID || runtimeHostInt64(dispatch["fencing_token"]) != command.Fence.FencingToken {
		return fmt.Errorf("STALE_FENCING_TOKEN")
	}
	if reservation.State == "accepted" && reservation.DispatchID == command.DispatchID && fmt.Sprint(dispatch["state"]) == "accepted" && fmt.Sprint(dispatch["runtime_request_id"]) == command.RuntimeRequestID {
		// Backfill the durable probe schedule for an idempotent retry against a
		// dispatch created before this field was populated on acceptance.
		_, err := tx.ExecRaw(ctx, `update runtime_run_dispatches set next_recovery_check_at=$2,version=version+1,updated_at=now()
where dispatch_id=$1 and next_recovery_check_at is null`, command.DispatchID, nextRecoveryCheckAt)
		if err != nil {
			return err
		}
		return r.markRuntimeRunRecordAcceptedTx(ctx, tx, command)
	}
	if reservation.State != "reserved" || !reservation.ExpiresAt.After(time.Now().UTC()) || !stringInRuntime(fmt.Sprint(dispatch["state"]), []string{"created", "sent", "submit_unknown"}) {
		return fmt.Errorf("STALE_FENCING_TOKEN")
	}
	hostRows, err := tx.Query(ctx, `select runtime_host_id from runtime_hosts where runtime_host_id=@host for update`, map[string]any{"host": command.Fence.RuntimeHostID})
	if err != nil || len(hostRows) != 1 {
		return fmt.Errorf("RUNTIME_HOST_COUNTER_DRIFT")
	}
	tag, err := tx.ExecRaw(ctx, `update runtime_hosts set
reserved_runs=reserved_runs-1,active_runs=active_runs+1,
reserved_product_thread_runs=reserved_product_thread_runs-case when $2='product_thread' then 1 else 0 end,
active_product_thread_runs=active_product_thread_runs+case when $2='product_thread' then 1 else 0 end,
reserved_detached_task_runs=reserved_detached_task_runs-case when $2='detached_task' then 1 else 0 end,
active_detached_task_runs=active_detached_task_runs+case when $2='detached_task' then 1 else 0 end,
updated_at=now() where runtime_host_id=$1 and reserved_runs>0
and case when $2='product_thread' then reserved_product_thread_runs>0 else reserved_detached_task_runs>0 end`, command.Fence.RuntimeHostID, reservation.ExecutionScope)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("RUNTIME_HOST_COUNTER_DRIFT")
	}
	if err := tx.Exec(ctx, `update runtime_slot_reservations set state='accepted',dispatch_id=@dispatch,accepted_at=coalesce(accepted_at,now()),version=version+1,updated_at=now() where reservation_id=@id`, map[string]any{"id": reservation.ReservationID, "dispatch": command.DispatchID}); err != nil {
		return err
	}
	if err := tx.Exec(ctx, `update runtime_run_dispatches set state='accepted',runtime_request_id=@request,accepted_at=coalesce(accepted_at,now()),next_recovery_check_at=@next,version=version+1,updated_at=now() where dispatch_id=@dispatch`, map[string]any{"dispatch": command.DispatchID, "request": command.RuntimeRequestID, "next": nextRecoveryCheckAt}); err != nil {
		return err
	}
	return r.markRuntimeRunRecordAcceptedTx(ctx, tx, command)
}

func (r *RuntimeHostRepository) confirmDispatchAcceptedMemory(command DispatchAcceptedCommand) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.validateConfirmDispatchAcceptedMemoryLocked(command); err != nil {
		return err
	}
	r.applyConfirmDispatchAcceptedMemoryLocked(command)
	return nil
}

func (r *RuntimeHostRepository) validateConfirmDispatchAcceptedMemoryLocked(command DispatchAcceptedCommand) error {
	if err := validateDispatchAcceptedCommand(command); err != nil {
		return err
	}
	reservation, ok := r.reservations[command.Fence.ReservationID]
	dispatch, dispatchOK := r.dispatches[command.DispatchID]
	if !ok || !dispatchOK || !reservationFenceMatches(reservation, command.Fence) || dispatch.RuntimeHostID != command.Fence.RuntimeHostID || dispatch.ReservationID != reservation.ReservationID || dispatch.FencingToken != reservation.FencingToken {
		return fmt.Errorf("STALE_FENCING_TOKEN")
	}
	if reservation.State == "accepted" && reservation.DispatchID == command.DispatchID && dispatch.State == "accepted" {
		if dispatch.RuntimeRequestID != command.RuntimeRequestID {
			return fmt.Errorf("STALE_FENCING_TOKEN")
		}
		if dispatch.NextRecoveryCheckAt.IsZero() {
			dispatch.NextRecoveryCheckAt = time.Now().UTC().Add(runtimeInitialDispatchRecoveryDelay)
			dispatch.Version++
			dispatch.UpdatedAt = time.Now().UTC()
			r.dispatches[dispatch.DispatchID] = dispatch
		}
		return nil
	}
	if reservation.State != "reserved" || !reservation.ExpiresAt.After(time.Now().UTC()) || !stringInRuntime(dispatch.State, []string{"created", "sent", "submit_unknown"}) {
		return fmt.Errorf("STALE_FENCING_TOKEN")
	}
	return nil
}

// applyConfirmDispatchAcceptedMemoryLocked applies a transition that was
// already validated while the repository mutex is held. Keeping validation and
// mutation separate lets RuntimeScheduler atomically compose this in-memory
// Host change with the capacity generation transition under both mutexes.
func (r *RuntimeHostRepository) applyConfirmDispatchAcceptedMemoryLocked(command DispatchAcceptedCommand) {
	reservation := r.reservations[command.Fence.ReservationID]
	dispatch := r.dispatches[command.DispatchID]
	if reservation.State == "accepted" {
		if dispatch.NextRecoveryCheckAt.IsZero() {
			dispatch.NextRecoveryCheckAt = time.Now().UTC().Add(runtimeInitialDispatchRecoveryDelay)
			dispatch.Version++
			dispatch.UpdatedAt = time.Now().UTC()
			r.dispatches[dispatch.DispatchID] = dispatch
		}
		return
	}
	host := r.hosts[reservation.RuntimeHostID]
	decrementRuntimeHostReserved(&host, reservation.ExecutionScope)
	incrementRuntimeHostActive(&host, reservation.ExecutionScope)
	r.hosts[host.RuntimeHostID] = host
	reservation.State, reservation.DispatchID, reservation.UpdatedAt = "accepted", command.DispatchID, time.Now().UTC()
	reservation.Version++
	r.reservations[reservation.ReservationID] = reservation
	dispatch.State, dispatch.RuntimeRequestID, dispatch.NextRecoveryCheckAt, dispatch.UpdatedAt = "accepted", command.RuntimeRequestID, time.Now().UTC().Add(runtimeInitialDispatchRecoveryDelay), time.Now().UTC()
	dispatch.Version++
	r.dispatches[dispatch.DispatchID] = dispatch
	r.markRuntimeRunRecordAcceptedMemoryLocked(command)
}

func validateDispatchAcceptedCommand(command DispatchAcceptedCommand) error {
	if err := validateReservationFence(command.Fence); err != nil {
		return err
	}
	if command.DispatchID == "" || command.RuntimeRequestID == "" {
		return fmt.Errorf("INVALID_ARGUMENT")
	}
	return nil
}

func (r *RuntimeHostRepository) ReleaseReservation(ctx context.Context, command ReservationReleaseCommand) (bool, error) {
	return r.releaseReservationFenced(ctx, command, "released")
}

func (r *RuntimeHostRepository) releaseReservationFenced(ctx context.Context, command ReservationReleaseCommand, terminalState string) (bool, error) {
	if err := validateReservationFence(command.Fence); err != nil || command.Reason == "" || !stringInRuntime(terminalState, []string{"released", "expired"}) {
		if err != nil {
			return false, err
		}
		return false, fmt.Errorf("INVALID_ARGUMENT")
	}
	if r.postgresReady() {
		changed := false
		err := r.db.WithTx(ctx, func(tx *persistence.Tx) error {
			rows, err := tx.Query(ctx, `select reservation_id,run_id,runtime_host_id,owner_instance_id,state,fencing_token,lease_token_hash,capability_hash,execution_scope,execution_scope_source,coalesce(dispatch_id,''),expires_at,last_renewed_at,version,created_at,updated_at from runtime_slot_reservations where reservation_id=@id for update`, map[string]any{"id": command.Fence.ReservationID})
			if err != nil || len(rows) != 1 {
				return fmt.Errorf("STALE_FENCING_TOKEN")
			}
			reservation, err := runtimeReservationFromMap(rows[0])
			if err != nil || !reservationFenceMatches(reservation, command.Fence) {
				return fmt.Errorf("STALE_FENCING_TOKEN")
			}
			if reservation.State == "released" || reservation.State == "expired" {
				return nil
			}
			if !stringInRuntime(reservation.State, []string{"reserved", "accepted", "running"}) {
				return fmt.Errorf("STALE_FENCING_TOKEN")
			}
			if err := releaseRuntimeHostCounterTx(ctx, tx, reservation); err != nil {
				return err
			}
			if err := tx.Exec(ctx, `update runtime_slot_reservations set state=@state,released_at=coalesce(released_at,now()),release_reason=@reason,version=version+1,updated_at=now() where reservation_id=@id`, map[string]any{"id": reservation.ReservationID, "state": terminalState, "reason": boundedRuntimeReason(command.Reason)}); err != nil {
				return err
			}
			changed = true
			return nil
		})
		return changed, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	reservation, ok := r.reservations[command.Fence.ReservationID]
	if !ok || !reservationFenceMatches(reservation, command.Fence) {
		return false, fmt.Errorf("STALE_FENCING_TOKEN")
	}
	if reservation.State == "released" || reservation.State == "expired" {
		return false, nil
	}
	host := r.hosts[reservation.RuntimeHostID]
	if reservation.State == "reserved" {
		decrementRuntimeHostReserved(&host, reservation.ExecutionScope)
	} else {
		decrementRuntimeHostActive(&host, reservation.ExecutionScope)
	}
	r.hosts[host.RuntimeHostID] = host
	delete(r.activeReservationByRun, reservation.RunID)
	reservation.State, reservation.UpdatedAt = terminalState, time.Now().UTC()
	reservation.Version++
	r.reservations[reservation.ReservationID] = reservation
	return true, nil
}

func (r *RuntimeHostRepository) FinalizeDispatchAndReleaseSlot(ctx context.Context, command DispatchTerminalCommand) error {
	if command.DispatchID == "" || !stringInRuntime(command.TerminalStatus, []string{"succeeded", "failed", "timeout", "aborted", "rejected", "orphaned"}) {
		return fmt.Errorf("INVALID_ARGUMENT")
	}
	if command.RecoveryClaim != nil && (command.RecoveryClaim.DispatchID != command.DispatchID || command.RecoveryClaim.OwnerInstanceID == "" || command.RecoveryClaim.FencingToken < 1 || command.RecoveryClaim.ExpiresAt.IsZero()) {
		return fmt.Errorf("INVALID_ARGUMENT")
	}
	if r.postgresReady() {
		return r.db.WithTx(ctx, func(tx *persistence.Tx) error {
			// Keep the shared capacity-row order Slot -> Dispatch -> Host. The
			// reservation fence is independently sufficient to lock the Slot before
			// checking the recovery claim that lives on the Dispatch.
			rows, err := tx.Query(ctx, `select reservation_id,run_id,runtime_host_id,owner_instance_id,state,fencing_token,lease_token_hash,capability_hash,execution_scope,execution_scope_source,coalesce(dispatch_id,''),expires_at,last_renewed_at,version,created_at,updated_at from runtime_slot_reservations where reservation_id=@id for update`, map[string]any{"id": command.Fence.ReservationID})
			if err != nil || len(rows) != 1 {
				return fmt.Errorf("STALE_FENCING_TOKEN")
			}
			reservation, err := runtimeReservationFromMap(rows[0])
			if err != nil || !reservationFenceMatches(reservation, command.Fence) {
				return fmt.Errorf("STALE_FENCING_TOKEN")
			}
			dispatchParams := map[string]any{
				"dispatch": command.DispatchID, "requireRecoveryClaim": command.RecoveryClaim != nil,
				"recoveryOwner": "", "recoveryFencing": int64(0),
			}
			if command.RecoveryClaim != nil {
				dispatchParams["recoveryOwner"] = command.RecoveryClaim.OwnerInstanceID
				dispatchParams["recoveryFencing"] = command.RecoveryClaim.FencingToken
			}
			dispatchRows, err := tx.Query(ctx, `select state,runtime_host_id,reservation_id,fencing_token from runtime_run_dispatches
where dispatch_id=@dispatch
  and (not @requireRecoveryClaim or (
    state='recovering'
    and recovery_owner_instance_id=@recoveryOwner
    and recovery_fencing_token=@recoveryFencing
    and recovery_expires_at>now()
  )) for update`, dispatchParams)
			if err != nil || len(dispatchRows) != 1 {
				return fmt.Errorf("STALE_FENCING_TOKEN")
			}
			dispatch := dispatchRows[0]
			if fmt.Sprint(dispatch["runtime_host_id"]) != command.Fence.RuntimeHostID || fmt.Sprint(dispatch["reservation_id"]) != command.Fence.ReservationID || runtimeHostInt64(dispatch["fencing_token"]) != command.Fence.FencingToken {
				return fmt.Errorf("STALE_FENCING_TOKEN")
			}
			if reservation.State != "released" && reservation.State != "expired" {
				if err := releaseRuntimeHostCounterTx(ctx, tx, reservation); err != nil {
					return err
				}
				if err := tx.Exec(ctx, `update runtime_slot_reservations set state='released',released_at=coalesce(released_at,now()),release_reason=@reason,version=version+1,updated_at=now() where reservation_id=@id`, map[string]any{"id": reservation.ReservationID, "reason": "dispatch_" + command.TerminalStatus}); err != nil {
					return err
				}
			}
			return tx.Exec(ctx, `update runtime_run_dispatches set state=@state,terminal_at=coalesce(terminal_at,now()),error_code=nullif(@error,''),abort_status=case when @state='aborted' then 'terminal' else abort_status end,recovery_expires_at=null,version=version+1,updated_at=now() where dispatch_id=@dispatch and state not in ('succeeded','failed','timeout','aborted','rejected','orphaned')`, map[string]any{"dispatch": command.DispatchID, "state": command.TerminalStatus, "error": command.ErrorCode})
		})
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	dispatch, ok := r.dispatches[command.DispatchID]
	if !ok || dispatch.RuntimeHostID != command.Fence.RuntimeHostID || dispatch.ReservationID != command.Fence.ReservationID || dispatch.FencingToken != command.Fence.FencingToken {
		return fmt.Errorf("STALE_FENCING_TOKEN")
	}
	if command.RecoveryClaim != nil && !dispatchRecoveryClaimActive(dispatch, *command.RecoveryClaim, time.Now().UTC()) {
		return fmt.Errorf("STALE_FENCING_TOKEN")
	}
	reservation, ok := r.reservations[command.Fence.ReservationID]
	if !ok || !reservationFenceMatches(reservation, command.Fence) {
		return fmt.Errorf("STALE_FENCING_TOKEN")
	}
	if reservation.State != "released" && reservation.State != "expired" {
		if !stringInRuntime(reservation.State, []string{"reserved", "accepted", "running"}) {
			return fmt.Errorf("STALE_FENCING_TOKEN")
		}
		host := r.hosts[reservation.RuntimeHostID]
		if reservation.State == "reserved" {
			decrementRuntimeHostReserved(&host, reservation.ExecutionScope)
		} else {
			decrementRuntimeHostActive(&host, reservation.ExecutionScope)
		}
		r.hosts[host.RuntimeHostID] = host
		delete(r.activeReservationByRun, reservation.RunID)
		reservation.State, reservation.UpdatedAt = "released", time.Now().UTC()
		reservation.Version++
		r.reservations[reservation.ReservationID] = reservation
	}
	if !stringInRuntime(dispatch.State, []string{"succeeded", "failed", "timeout", "aborted", "rejected", "orphaned"}) {
		dispatch.State, dispatch.UpdatedAt = command.TerminalStatus, time.Now().UTC()
		if command.TerminalStatus == "aborted" {
			dispatch.AbortStatus = "terminal"
		}
		dispatch.RecoveryExpiresAt = time.Time{}
		dispatch.Version++
		r.dispatches[dispatch.DispatchID] = dispatch
	}
	return nil
}

func dispatchRecoveryClaimActive(dispatch RuntimeDispatch, claim DispatchRecoveryClaim, now time.Time) bool {
	return claim.DispatchID == dispatch.DispatchID && claim.OwnerInstanceID != "" && claim.FencingToken > 0 &&
		dispatch.State == "recovering" && dispatch.RecoveryOwnerInstanceID == claim.OwnerInstanceID &&
		dispatch.RecoveryFencingToken == claim.FencingToken && dispatch.RecoveryExpiresAt.After(now)
}

func (r *RuntimeHostRepository) MarkDispatchSent(ctx context.Context, dispatchID string, fence ReservationFence) error {
	return r.markDispatchBoundary(ctx, dispatchID, fence, []string{"created", "sent"}, "sent", time.Now().UTC().Add(runtimeInitialDispatchRecoveryDelay))
}

func (r *RuntimeHostRepository) MarkDispatchSubmitUnknown(ctx context.Context, dispatchID string, fence ReservationFence, nextCheck time.Time) error {
	return r.markDispatchBoundary(ctx, dispatchID, fence, []string{"created", "sent", "submit_unknown"}, "submit_unknown", nextCheck)
}

func (r *RuntimeHostRepository) markDispatchBoundary(ctx context.Context, dispatchID string, fence ReservationFence, from []string, to string, nextCheck time.Time) error {
	if dispatchID == "" || validateReservationFence(fence) != nil {
		return fmt.Errorf("INVALID_ARGUMENT")
	}
	if r.postgresReady() {
		result, err := r.db.Pool.Exec(ctx, `update runtime_run_dispatches set state=$6,next_recovery_check_at=$7,version=version+1,updated_at=now() where dispatch_id=$1 and reservation_id=$2 and runtime_host_id=$3 and owner_instance_id=$4 and fencing_token=$5 and state=any($8)`, dispatchID, fence.ReservationID, fence.RuntimeHostID, fence.OwnerInstanceID, fence.FencingToken, to, nullableRuntimeTime(nextCheck), from)
		if err != nil {
			return err
		}
		if result.RowsAffected() != 1 {
			return fmt.Errorf("STALE_FENCING_TOKEN")
		}
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	dispatch, ok := r.dispatches[dispatchID]
	if !ok || dispatch.ReservationID != fence.ReservationID || dispatch.RuntimeHostID != fence.RuntimeHostID || dispatch.OwnerInstanceID != fence.OwnerInstanceID || dispatch.FencingToken != fence.FencingToken || !stringInRuntime(dispatch.State, from) {
		return fmt.Errorf("STALE_FENCING_TOKEN")
	}
	dispatch.State, dispatch.NextRecoveryCheckAt, dispatch.UpdatedAt = to, nextCheck, time.Now().UTC()
	dispatch.Version++
	r.dispatches[dispatchID] = dispatch
	return nil
}

func (r *RuntimeHostRepository) ListDispatchRecoveryCandidates(ctx context.Context, now time.Time, limit int) ([]RuntimeDispatch, error) {
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}
	if r.postgresReady() {
		rows, err := r.db.Pool.Query(ctx, runtimeDispatchSelect+` where state in('created','sent','submit_unknown','retry_same_host','accepted','materializing','running','finalizing','recovering')
and (
  (next_recovery_check_at is not null and next_recovery_check_at<=$1)
  or (lease_expires_at is not null and lease_expires_at<=$1)
  or (state='created' and next_recovery_check_at is null and updated_at<=$1)
  or (state<>'created' and next_recovery_check_at is null and updated_at<=$3)
)
order by updated_at limit $2`, now, limit, now.Add(-runtimeInitialDispatchRecoveryDelay))
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		out := []RuntimeDispatch{}
		for rows.Next() {
			item, err := scanRuntimeDispatchFull(rows)
			if err != nil {
				return nil, err
			}
			out = append(out, item)
		}
		return out, rows.Err()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	out := []RuntimeDispatch{}
	for _, item := range r.dispatches {
		due := !item.NextRecoveryCheckAt.IsZero() && !item.NextRecoveryCheckAt.After(now)
		due = due || !item.LeaseExpiresAt.IsZero() && !item.LeaseExpiresAt.After(now)
		due = due || item.State == "created" && item.NextRecoveryCheckAt.IsZero() && !item.UpdatedAt.After(now)
		due = due || item.State != "created" && item.NextRecoveryCheckAt.IsZero() && !item.UpdatedAt.Add(runtimeInitialDispatchRecoveryDelay).After(now)
		if stringInRuntime(item.State, []string{"created", "sent", "submit_unknown", "retry_same_host", "accepted", "materializing", "running", "finalizing", "recovering"}) && due {
			out = append(out, item)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].UpdatedAt.Before(out[j].UpdatedAt) })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (r *RuntimeHostRepository) ClaimDispatchRecovery(ctx context.Context, claim DispatchRecoveryClaim) error {
	if claim.DispatchID == "" || claim.OwnerInstanceID == "" || claim.FencingToken < 1 || claim.ExpiresAt.IsZero() || claim.ExpectedState == "" || claim.ExpectedVersion < 1 {
		return fmt.Errorf("INVALID_ARGUMENT")
	}
	if r.postgresReady() {
		result, err := r.db.Pool.Exec(ctx, `update runtime_run_dispatches set state='recovering',recovery_owner_instance_id=$2,recovery_fencing_token=$3,recovery_expires_at=$4,recovery_attempt=recovery_attempt+1,version=version+1,updated_at=now()
where dispatch_id=$1 and state=$5 and version=$6
and state in('created','sent','submit_unknown','retry_same_host','accepted','materializing','running','finalizing','recovering')
and (recovery_expires_at is null or recovery_expires_at<=now() or recovery_owner_instance_id=$2)`, claim.DispatchID, claim.OwnerInstanceID, claim.FencingToken, claim.ExpiresAt, claim.ExpectedState, claim.ExpectedVersion)
		if err != nil {
			return err
		}
		if result.RowsAffected() != 1 {
			return fmt.Errorf("STALE_FENCING_TOKEN")
		}
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	item, ok := r.dispatches[claim.DispatchID]
	if !ok || item.State != claim.ExpectedState || item.Version != claim.ExpectedVersion || !stringInRuntime(item.State, []string{"created", "sent", "submit_unknown", "retry_same_host", "accepted", "materializing", "running", "finalizing", "recovering"}) || (!item.RecoveryExpiresAt.IsZero() && item.RecoveryExpiresAt.After(time.Now().UTC()) && item.RecoveryOwnerInstanceID != claim.OwnerInstanceID) {
		return fmt.Errorf("STALE_FENCING_TOKEN")
	}
	item.State = "recovering"
	item.RecoveryOwnerInstanceID = claim.OwnerInstanceID
	item.RecoveryFencingToken = claim.FencingToken
	item.RecoveryExpiresAt = claim.ExpiresAt
	item.RecoveryAttempt++
	item.Version++
	item.UpdatedAt = time.Now().UTC()
	r.dispatches[item.DispatchID] = item
	return nil
}

// AssertDispatchRecoveryClaim is the pre-projection fence for recovery work
// that cannot share the Host Slot transaction, such as AgentRun/Product event
// projection. FinalizeDispatchAndReleaseSlot repeats this proof atomically
// before it changes durable dispatch or capacity-adjacent Slot state.
func (r *RuntimeHostRepository) AssertDispatchRecoveryClaim(ctx context.Context, claim DispatchRecoveryClaim) error {
	if claim.DispatchID == "" || claim.OwnerInstanceID == "" || claim.FencingToken < 1 || claim.ExpiresAt.IsZero() {
		return fmt.Errorf("INVALID_ARGUMENT")
	}
	if r.postgresReady() {
		var exists bool
		err := r.db.Pool.QueryRow(ctx, `select true from runtime_run_dispatches
where dispatch_id=$1 and state='recovering' and recovery_owner_instance_id=$2
  and recovery_fencing_token=$3 and recovery_expires_at>now()`, claim.DispatchID, claim.OwnerInstanceID, claim.FencingToken).Scan(&exists)
		if err != nil || !exists {
			return fmt.Errorf("STALE_FENCING_TOKEN")
		}
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	dispatch, ok := r.dispatches[claim.DispatchID]
	if !ok || !dispatchRecoveryClaimActive(dispatch, claim, time.Now().UTC()) {
		return fmt.Errorf("STALE_FENCING_TOKEN")
	}
	return nil
}

func (r *RuntimeHostRepository) AdvanceDispatchRecovery(ctx context.Context, claim DispatchRecoveryClaim, state string, nextCheck time.Time) error {
	if !stringInRuntime(state, []string{"created", "sent", "submit_unknown", "retry_same_host", "accepted", "materializing", "running", "finalizing", "recovering", "orphaned", "retry_new_attempt"}) {
		return fmt.Errorf("INVALID_ARGUMENT")
	}
	if r.postgresReady() {
		result, err := r.db.Pool.Exec(ctx, `update runtime_run_dispatches set state=$4,next_recovery_check_at=$5,recovery_expires_at=null,version=version+1,updated_at=now() where dispatch_id=$1 and recovery_owner_instance_id=$2 and recovery_fencing_token=$3 and recovery_expires_at>now()`, claim.DispatchID, claim.OwnerInstanceID, claim.FencingToken, state, nullableRuntimeTime(nextCheck))
		if err != nil {
			return err
		}
		if result.RowsAffected() != 1 {
			return fmt.Errorf("STALE_FENCING_TOKEN")
		}
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	item, ok := r.dispatches[claim.DispatchID]
	if !ok || item.RecoveryOwnerInstanceID != claim.OwnerInstanceID || item.RecoveryFencingToken != claim.FencingToken || !item.RecoveryExpiresAt.After(time.Now().UTC()) {
		return fmt.Errorf("STALE_FENCING_TOKEN")
	}
	item.State = state
	item.NextRecoveryCheckAt = nextCheck
	item.RecoveryExpiresAt = time.Time{}
	item.Version++
	item.UpdatedAt = time.Now().UTC()
	r.dispatches[item.DispatchID] = item
	return nil
}

func (r *RuntimeHostRepository) RecoverDispatchAccepted(ctx context.Context, command DispatchRecoveryAcceptedCommand) error {
	if command.Claim.DispatchID == "" || validateReservationFence(command.Fence) != nil || command.Claim.OwnerInstanceID == "" || command.Claim.FencingToken < 1 {
		return fmt.Errorf("INVALID_ARGUMENT")
	}
	if r.postgresReady() {
		return r.db.WithTx(ctx, func(tx *persistence.Tx) error {
			// Recovery shares the acceptance and terminal mutation order: Slot,
			// then Dispatch, then the Host counter update below.
			rows, err := tx.Query(ctx, `select reservation_id,run_id,runtime_host_id,owner_instance_id,state,fencing_token,lease_token_hash,capability_hash,execution_scope,execution_scope_source,coalesce(dispatch_id,''),expires_at,last_renewed_at,version,created_at,updated_at from runtime_slot_reservations where reservation_id=@id for update`, map[string]any{"id": command.Fence.ReservationID})
			if err != nil || len(rows) != 1 {
				return fmt.Errorf("STALE_FENCING_TOKEN")
			}
			reservation, err := runtimeReservationFromMap(rows[0])
			if err != nil || !reservationFenceMatches(reservation, command.Fence) || !stringInRuntime(reservation.State, []string{"reserved", "accepted", "running"}) {
				return fmt.Errorf("STALE_FENCING_TOKEN")
			}
			dispatchRows, err := tx.Query(ctx, `select runtime_host_id,reservation_id,fencing_token,state,recovery_owner_instance_id,recovery_fencing_token,recovery_expires_at from runtime_run_dispatches where dispatch_id=@dispatch for update`, map[string]any{"dispatch": command.Claim.DispatchID})
			if err != nil || len(dispatchRows) != 1 {
				return fmt.Errorf("STALE_FENCING_TOKEN")
			}
			dispatch := dispatchRows[0]
			recoveryExpires, _ := dispatch["recovery_expires_at"].(time.Time)
			if fmt.Sprint(dispatch["runtime_host_id"]) != command.Fence.RuntimeHostID || fmt.Sprint(dispatch["reservation_id"]) != command.Fence.ReservationID || runtimeHostInt64(dispatch["fencing_token"]) != command.Fence.FencingToken || fmt.Sprint(dispatch["state"]) != "recovering" || fmt.Sprint(dispatch["recovery_owner_instance_id"]) != command.Claim.OwnerInstanceID || runtimeHostInt64(dispatch["recovery_fencing_token"]) != command.Claim.FencingToken || !recoveryExpires.After(time.Now().UTC()) {
				return fmt.Errorf("STALE_FENCING_TOKEN")
			}
			if reservation.State == "reserved" {
				tag, err := tx.ExecRaw(ctx, `update runtime_hosts set reserved_runs=reserved_runs-1,active_runs=active_runs+1,
reserved_product_thread_runs=reserved_product_thread_runs-case when $2='product_thread' then 1 else 0 end,
active_product_thread_runs=active_product_thread_runs+case when $2='product_thread' then 1 else 0 end,
reserved_detached_task_runs=reserved_detached_task_runs-case when $2='detached_task' then 1 else 0 end,
active_detached_task_runs=active_detached_task_runs+case when $2='detached_task' then 1 else 0 end,
updated_at=now() where runtime_host_id=$1 and reserved_runs>0
and case when $2='product_thread' then reserved_product_thread_runs>0 else reserved_detached_task_runs>0 end`, reservation.RuntimeHostID, reservation.ExecutionScope)
				if err != nil {
					return err
				}
				if tag.RowsAffected() != 1 {
					return fmt.Errorf("RUNTIME_HOST_COUNTER_DRIFT")
				}
				if err := tx.Exec(ctx, `update runtime_slot_reservations set state='accepted',dispatch_id=@dispatch,accepted_at=coalesce(accepted_at,now()),version=version+1,updated_at=now() where reservation_id=@id`, map[string]any{"id": reservation.ReservationID, "dispatch": command.Claim.DispatchID}); err != nil {
					return err
				}
			}
			return tx.Exec(ctx, `update runtime_run_dispatches set state='accepted',runtime_request_id=coalesce(nullif(@request,''),runtime_request_id),next_recovery_check_at=@next,recovery_expires_at=null,version=version+1,updated_at=now() where dispatch_id=@dispatch`, map[string]any{"dispatch": command.Claim.DispatchID, "request": command.RuntimeRequestID, "next": nullableRuntimeTime(command.NextCheckAt)})
		})
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	dispatch, ok := r.dispatches[command.Claim.DispatchID]
	reservation, reservationOK := r.reservations[command.Fence.ReservationID]
	if !ok || !reservationOK || dispatch.State != "recovering" || dispatch.RecoveryOwnerInstanceID != command.Claim.OwnerInstanceID || dispatch.RecoveryFencingToken != command.Claim.FencingToken || !dispatch.RecoveryExpiresAt.After(time.Now().UTC()) || dispatch.RuntimeHostID != command.Fence.RuntimeHostID || dispatch.ReservationID != command.Fence.ReservationID || dispatch.FencingToken != command.Fence.FencingToken || !reservationFenceMatches(reservation, command.Fence) || !stringInRuntime(reservation.State, []string{"reserved", "accepted", "running"}) {
		return fmt.Errorf("STALE_FENCING_TOKEN")
	}
	if reservation.State == "reserved" {
		host := r.hosts[reservation.RuntimeHostID]
		decrementRuntimeHostReserved(&host, reservation.ExecutionScope)
		incrementRuntimeHostActive(&host, reservation.ExecutionScope)
		r.hosts[host.RuntimeHostID] = host
		reservation.State = "accepted"
		reservation.DispatchID = dispatch.DispatchID
		reservation.Version++
		reservation.UpdatedAt = time.Now().UTC()
		r.reservations[reservation.ReservationID] = reservation
	}
	dispatch.State = "accepted"
	if dispatch.RuntimeRequestID == "" && command.RuntimeRequestID != "" {
		dispatch.RuntimeRequestID = command.RuntimeRequestID
	}
	dispatch.NextRecoveryCheckAt = command.NextCheckAt
	dispatch.RecoveryExpiresAt = time.Time{}
	dispatch.Version++
	dispatch.UpdatedAt = time.Now().UTC()
	r.dispatches[dispatch.DispatchID] = dispatch
	return nil
}

func (r *RuntimeHostRepository) GetDispatchEventCursor(ctx context.Context, dispatchID string) (RuntimeDispatchEventCursor, error) {
	if dispatchID == "" {
		return RuntimeDispatchEventCursor{}, fmt.Errorf("INVALID_ARGUMENT")
	}
	if r.postgresReady() {
		var cursor RuntimeDispatchEventCursor
		cursor.DispatchID = dispatchID
		err := r.db.Pool.QueryRow(ctx, `select event_cursor,event_lower_bound,coalesce(event_upper_bound,0),coalesce(event_gap_expected_sequence,0),coalesce(event_gap_observed_sequence,0) from runtime_run_dispatches where dispatch_id=$1`, dispatchID).Scan(&cursor.LastSequence, &cursor.LowerBound, &cursor.UpperBound, &cursor.GapExpected, &cursor.GapObserved)
		return cursor, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.dispatches[dispatchID]; !ok {
		return RuntimeDispatchEventCursor{}, fmt.Errorf("NOT_FOUND")
	}
	return RuntimeDispatchEventCursor{DispatchID: dispatchID, LastSequence: r.dispatchEventCursors[dispatchID], LowerBound: 1}, nil
}

func (r *RuntimeHostRepository) AppendRunEventAndAdvanceCursor(ctx context.Context, event RuntimeHostRunEvent, expectedPreviousSequence int64) error {
	if event.EventID == "" || event.RunID == "" || event.DispatchID == "" || event.SourceSequence < 1 || expectedPreviousSequence < 0 || event.SourceSequence != expectedPreviousSequence+1 {
		return fmt.Errorf("RUNTIME_EVENT_GAP")
	}
	var toolAudit *runtimeToolAuditEvent
	if IsRuntimeToolAuditEventType(event.EventType) {
		parsed, err := parseRuntimeToolAuditEvent(event.EventType, event.SafePayload)
		if err != nil {
			return err
		}
		toolAudit = &parsed
		event.SafePayload = parsed.safePayload()
	} else {
		event.SafePayload = sanitizeRuntimeHostEvent(event.SafePayload)
	}
	if event.OccurredAt.IsZero() {
		event.OccurredAt = time.Now().UTC()
	}
	payload, _ := json.Marshal(event.SafePayload)
	usage, _ := json.Marshal(event.UsageDelta)
	payloadHash := runtimeEventPayloadHash(event.EventType, payload, usage)
	if r.postgresReady() {
		gap := false
		err := r.db.WithTx(ctx, func(tx *persistence.Tx) error {
			// The dispatch lock is the event authority boundary. Validate the
			// Runtime-supplied identity before inspecting a replay or advancing its
			// cursor, so a dispatch cannot be used to write another Run's events or
			// tool audit rows.
			dispatchRows, err := tx.Query(ctx, `select run_id,runtime_host_id,state,event_cursor from runtime_run_dispatches where dispatch_id=@dispatch for update`, map[string]any{"dispatch": event.DispatchID})
			if err != nil || len(dispatchRows) != 1 {
				return fmt.Errorf("NOT_FOUND")
			}
			dispatch := dispatchRows[0]
			if event.RunID != fmt.Sprint(dispatch["run_id"]) || event.RuntimeHostID != fmt.Sprint(dispatch["runtime_host_id"]) {
				return fmt.Errorf("RUNTIME_EVENT_GAP")
			}
			duplicates, err := tx.Query(ctx, `select event_type,payload_hash,safe_payload,usage_delta from runtime_run_events where dispatch_id=@dispatch and source_sequence=@source`, map[string]any{"dispatch": event.DispatchID, "source": event.SourceSequence})
			if err != nil {
				return err
			}
			if len(duplicates) > 0 {
				existingHash := fmt.Sprint(duplicates[0]["payload_hash"])
				canonicalHash := runtimeEventPayloadHash(event.EventType, canonicalRuntimeEventJSON(duplicates[0]["safe_payload"]), canonicalRuntimeEventJSON(duplicates[0]["usage_delta"]))
				if fmt.Sprint(duplicates[0]["event_type"]) == event.EventType && (existingHash == payloadHash || canonicalHash == payloadHash) {
					if existingHash != payloadHash {
						if err := tx.Exec(ctx, `update runtime_run_events set payload_hash=@hash where dispatch_id=@dispatch and source_sequence=@source`, map[string]any{"hash": payloadHash, "dispatch": event.DispatchID, "source": event.SourceSequence}); err != nil {
							return err
						}
					}
					if toolAudit != nil {
						return projectRuntimeToolInvocationTx(ctx, tx, event.RunID, *toolAudit)
					}
					return nil
				}
				gap = true
				return nil
			}
			if toolAudit != nil && toolAudit.Outcome == "started" {
				if runtimeDispatchTerminal(fmt.Sprint(dispatch["state"])) {
					return fmt.Errorf("RUNTIME_EVENT_GAP")
				}
				runRows, err := tx.Query(ctx, `select status from runtime_run_records where run_id=@run for update`, map[string]any{"run": event.RunID})
				if err != nil {
					return err
				}
				if len(runRows) != 1 {
					return fmt.Errorf("RUNTIME_RUN_RECORD_UNAVAILABLE")
				}
				if runtimeRunRecordV1TerminalStatus(fmt.Sprint(runRows[0]["status"])) {
					return fmt.Errorf("RUNTIME_EVENT_GAP")
				}
			}
			current := runtimeHostInt64(dispatch["event_cursor"])
			if current != expectedPreviousSequence {
				if err := tx.Exec(ctx, `update runtime_run_dispatches set event_gap_detected_at=now(),event_gap_expected_sequence=@expected,event_gap_observed_sequence=@observed,state=case when state in('succeeded','failed','timeout','aborted','rejected','orphaned') then state else 'recovering' end,updated_at=now() where dispatch_id=@dispatch`, map[string]any{"dispatch": event.DispatchID, "expected": current + 1, "observed": event.SourceSequence}); err != nil {
					return err
				}
				gap = true
				return nil
			}
			if err := tx.Exec(ctx, `select pg_advisory_xact_lock(hashtextextended(@run,0))`, map[string]any{"run": event.RunID}); err != nil {
				return err
			}
			seqRows, err := tx.Query(ctx, `select coalesce(max(sequence),0) sequence from runtime_run_events where run_id=@run`, map[string]any{"run": event.RunID})
			if err != nil || len(seqRows) != 1 {
				return err
			}
			event.Sequence = runtimeHostInt64(seqRows[0]["sequence"]) + 1
			if err := tx.Exec(ctx, `insert into runtime_run_events(runtime_run_event_id,run_id,dispatch_id,runtime_host_id,sequence,source_sequence,event_type,visibility,safe_payload,usage_delta,payload_hash,occurred_at) values(@id,@run,@dispatch,nullif(@host,''),@sequence,@source,@type,@visibility,@payload::jsonb,@usage::jsonb,@hash,@occurred)`, map[string]any{"id": event.EventID, "run": event.RunID, "dispatch": event.DispatchID, "host": event.RuntimeHostID, "sequence": event.Sequence, "source": event.SourceSequence, "type": event.EventType, "visibility": event.Visibility, "payload": string(payload), "usage": string(usage), "hash": payloadHash, "occurred": event.OccurredAt}); err != nil {
				return err
			}
			if toolAudit != nil {
				if err := projectRuntimeToolInvocationTx(ctx, tx, event.RunID, *toolAudit); err != nil {
					return err
				}
			}
			return tx.Exec(ctx, `update runtime_run_dispatches set event_cursor=@source,event_gap_detected_at=null,event_gap_expected_sequence=null,event_gap_observed_sequence=null,version=version+1,updated_at=now() where dispatch_id=@dispatch and event_cursor=@expected`, map[string]any{"dispatch": event.DispatchID, "source": event.SourceSequence, "expected": expectedPreviousSequence})
		})
		if err != nil {
			return err
		}
		if gap {
			return fmt.Errorf("RUNTIME_EVENT_GAP")
		}
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	dispatch, ok := r.dispatches[event.DispatchID]
	if !ok {
		return fmt.Errorf("NOT_FOUND")
	}
	if event.RunID != dispatch.RunID || event.RuntimeHostID != dispatch.RuntimeHostID {
		return fmt.Errorf("RUNTIME_EVENT_GAP")
	}
	for _, existing := range r.events[event.RunID] {
		if existing.DispatchID == event.DispatchID && existing.SourceSequence == event.SourceSequence {
			ep, _ := json.Marshal(existing.SafePayload)
			eu, _ := json.Marshal(existing.UsageDelta)
			if existing.EventType == event.EventType && runtimeEventPayloadHash(existing.EventType, ep, eu) == payloadHash {
				return nil
			}
			return fmt.Errorf("RUNTIME_EVENT_GAP")
		}
	}
	if toolAudit != nil && toolAudit.Outcome == "started" {
		if runtimeDispatchTerminal(dispatch.State) {
			return fmt.Errorf("RUNTIME_EVENT_GAP")
		}
		if record, exists := r.runtimeRunRecords[event.RunID]; exists && runtimeRunRecordV1TerminalStatus(record.Status) {
			return fmt.Errorf("RUNTIME_EVENT_GAP")
		}
	}
	current := r.dispatchEventCursors[event.DispatchID]
	if current != expectedPreviousSequence {
		return fmt.Errorf("RUNTIME_EVENT_GAP")
	}
	if toolAudit != nil {
		if err := r.projectRuntimeToolInvocationMemoryLocked(event.RunID, *toolAudit); err != nil {
			return err
		}
	}
	event.Sequence = int64(len(r.events[event.RunID]) + 1)
	r.events[event.RunID] = append(r.events[event.RunID], event)
	r.dispatchEventCursors[event.DispatchID] = event.SourceSequence
	return nil
}

func releaseRuntimeHostCounterTx(ctx context.Context, tx *persistence.Tx, reservation RuntimeSlotReservation) error {
	columnCondition := "reserved_runs>0 and reserved_product_thread_runs>0"
	updates := `reserved_runs=reserved_runs-1,reserved_product_thread_runs=reserved_product_thread_runs-1`
	if reservation.State != "reserved" {
		columnCondition = "active_runs>0 and active_product_thread_runs>0"
		updates = `active_runs=active_runs-1,active_product_thread_runs=active_product_thread_runs-1`
	}
	if reservation.ExecutionScope == "detached_task" {
		if reservation.State == "reserved" {
			columnCondition = "reserved_runs>0 and reserved_detached_task_runs>0"
			updates = `reserved_runs=reserved_runs-1,reserved_detached_task_runs=reserved_detached_task_runs-1`
		} else {
			columnCondition = "active_runs>0 and active_detached_task_runs>0"
			updates = `active_runs=active_runs-1,active_detached_task_runs=active_detached_task_runs-1`
		}
	}
	tag, err := tx.ExecRaw(ctx, `update runtime_hosts set `+updates+`,updated_at=now() where runtime_host_id=$1 and `+columnCondition, reservation.RuntimeHostID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("RUNTIME_HOST_COUNTER_DRIFT")
	}
	return nil
}

func validateAtomicReservationCommand(command AtomicReservationCommand) error {
	if command.ReservationID == "" || command.RunID == "" || command.OwnerInstanceID == "" || command.CapabilityHash == "" || command.LeaseTokenHash == "" || command.FencingToken < 1 || command.ExpiresAt.IsZero() || !stringInRuntime(command.ExecutionScope, []string{"product_thread", "detached_task"}) {
		return fmt.Errorf("INVALID_ARGUMENT")
	}
	for _, tool := range command.RequiredTools {
		if !IsAgentFacingRuntimeTool(tool) {
			return fmt.Errorf("RUNTIME_TOOL_UNAVAILABLE")
		}
	}
	return nil
}
func validateReservationFence(fence ReservationFence) error {
	if fence.ReservationID == "" || fence.RuntimeHostID == "" || fence.OwnerInstanceID == "" || fence.LeaseTokenHash == "" || fence.FencingToken < 1 {
		return fmt.Errorf("INVALID_ARGUMENT")
	}
	return nil
}
func reservationFenceMatches(item RuntimeSlotReservation, fence ReservationFence) bool {
	return item.ReservationID == fence.ReservationID && item.RuntimeHostID == fence.RuntimeHostID && item.OwnerInstanceID == fence.OwnerInstanceID && item.LeaseTokenHash == fence.LeaseTokenHash && item.FencingToken == fence.FencingToken
}
func runtimeHostWithinTarget(host RuntimeHost, scope string) bool {
	if host.MaxActiveRuns <= 0 || host.ActiveRuns+host.ReservedRuns >= maxRuntimeAdmission(1, (host.MaxActiveRuns*3+3)/4) {
		return false
	}
	if scope == "product_thread" {
		return host.MaxProductThreadRuns > 0 && host.ActiveProductThreadRuns+host.ReservedProductThreadRuns < maxRuntimeAdmission(1, (host.MaxProductThreadRuns*3+3)/4)
	}
	return host.MaxDetachedTaskRuns > 0 && host.ActiveDetachedTaskRuns+host.ReservedDetachedTaskRuns < maxRuntimeAdmission(1, (host.MaxDetachedTaskRuns*3+3)/4)
}
func maxRuntimeAdmission(a, b int) int {
	if a > b {
		return a
	}
	return b
}
func runtimeHostHasTools(host RuntimeHost, required []string) bool {
	ready := map[string]bool{}
	for _, tool := range host.Capabilities.Tools {
		ready[tool.Name] = runtimeToolCapabilityReady(tool)
	}
	for _, name := range required {
		if !IsAgentFacingRuntimeTool(name) || !ready[name] {
			return false
		}
	}
	return true
}
func incrementRuntimeHostReserved(host *RuntimeHost, scope string) {
	host.ReservedRuns++
	if scope == "product_thread" {
		host.ReservedProductThreadRuns++
	} else {
		host.ReservedDetachedTaskRuns++
	}
}
func decrementRuntimeHostReserved(host *RuntimeHost, scope string) {
	if host.ReservedRuns > 0 {
		host.ReservedRuns--
	}
	if scope == "product_thread" {
		if host.ReservedProductThreadRuns > 0 {
			host.ReservedProductThreadRuns--
		}
	} else if host.ReservedDetachedTaskRuns > 0 {
		host.ReservedDetachedTaskRuns--
	}
}
func incrementRuntimeHostActive(host *RuntimeHost, scope string) {
	host.ActiveRuns++
	if scope == "product_thread" {
		host.ActiveProductThreadRuns++
	} else {
		host.ActiveDetachedTaskRuns++
	}
}
func decrementRuntimeHostActive(host *RuntimeHost, scope string) {
	if host.ActiveRuns > 0 {
		host.ActiveRuns--
	}
	if scope == "product_thread" {
		if host.ActiveProductThreadRuns > 0 {
			host.ActiveProductThreadRuns--
		}
	} else if host.ActiveDetachedTaskRuns > 0 {
		host.ActiveDetachedTaskRuns--
	}
}
func boundedRuntimeReason(value string) string {
	value = strings.TrimSpace(value)
	if len(value) > 80 {
		value = value[:80]
	}
	return value
}
func runtimeEventPayloadHash(eventType string, payload, usage []byte) string {
	sum := sha256.Sum256(append(append([]byte(eventType+"\x00"), payload...), append([]byte("\x00"), usage...)...))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func canonicalRuntimeEventJSON(value any) []byte {
	var raw []byte
	switch typed := value.(type) {
	case []byte:
		raw = typed
	case string:
		raw = []byte(typed)
	default:
		raw, _ = json.Marshal(typed)
	}
	var decoded any
	if len(raw) > 0 && json.Unmarshal(raw, &decoded) == nil {
		canonical, _ := json.Marshal(decoded)
		return canonical
	}
	return []byte("null")
}

func runtimeHostFromMap(row map[string]any) (RuntimeHost, error) {
	var host RuntimeHost
	host.RuntimeHostID = fmt.Sprint(row["runtime_host_id"])
	host.InstanceID = fmt.Sprint(row["instance_id"])
	host.Environment = fmt.Sprint(row["environment"])
	host.Endpoint = fmt.Sprint(row["endpoint"])
	host.Zone = fmt.Sprint(row["zone"])
	host.Status = fmt.Sprint(row["status"])
	host.RuntimeVersion = fmt.Sprint(row["runtime_version"])
	host.AdapterVersion = fmt.Sprint(row["adapter_version"])
	host.CapabilityHash = fmt.Sprint(row["capability_hash"])
	host.SessionStoreID = fmt.Sprint(row["session_store_id"])
	host.MaxActiveRuns = int(runtimeHostInt64(row["max_active_runs"]))
	host.ActiveRuns = int(runtimeHostInt64(row["active_runs"]))
	host.ReservedRuns = int(runtimeHostInt64(row["reserved_runs"]))
	host.ReportedActiveRuns = int(runtimeHostInt64(row["reported_active_runs"]))
	host.ReportedReservedRuns = int(runtimeHostInt64(row["reported_reserved_runs"]))
	host.MaxProductThreadRuns = int(runtimeHostInt64(row["max_product_thread_runs"]))
	host.ActiveProductThreadRuns = int(runtimeHostInt64(row["active_product_thread_runs"]))
	host.ReservedProductThreadRuns = int(runtimeHostInt64(row["reserved_product_thread_runs"]))
	host.MaxDetachedTaskRuns = int(runtimeHostInt64(row["max_detached_task_runs"]))
	host.ActiveDetachedTaskRuns = int(runtimeHostInt64(row["active_detached_task_runs"]))
	host.ReservedDetachedTaskRuns = int(runtimeHostInt64(row["reserved_detached_task_runs"]))
	host.InstanceGeneration = runtimeHostInt64(row["instance_generation"])
	host.RecoveryRevision = runtimeHostInt64(row["recovery_revision"])
	host.RecoveryState = fmt.Sprint(row["recovery_state"])
	var rawCapabilities []byte
	switch value := row["capability_snapshot"].(type) {
	case []byte:
		rawCapabilities = value
	case string:
		rawCapabilities = []byte(value)
	default:
		var err error
		rawCapabilities, err = json.Marshal(value)
		if err != nil {
			return RuntimeHost{}, err
		}
	}
	if len(rawCapabilities) == 0 || json.Unmarshal(rawCapabilities, &host.Capabilities) != nil {
		return RuntimeHost{}, fmt.Errorf("RUNTIME_HOST_CAPABILITY_INVALID")
	}
	if value, ok := row["last_heartbeat_at"].(time.Time); ok {
		host.LastHeartbeatAt = value
	}
	if value, ok := row["drain_deadline_at"].(time.Time); ok {
		host.DrainDeadlineAt = value
	}
	if value, ok := row["updated_at"].(time.Time); ok {
		host.UpdatedAt = value
	}
	return host, nil
}
func runtimeReservationFromMap(row map[string]any) (RuntimeSlotReservation, error) {
	item := RuntimeSlotReservation{ReservationID: fmt.Sprint(row["reservation_id"]), RunID: fmt.Sprint(row["run_id"]), RuntimeHostID: fmt.Sprint(row["runtime_host_id"]), AssignedRuntimeHostInstanceID: fmt.Sprint(row["assigned_runtime_host_instance_id"]), AssignedRuntimeHostInstanceGeneration: runtimeHostInt64(row["assigned_runtime_host_instance_generation"]), OwnerInstanceID: fmt.Sprint(row["owner_instance_id"]), State: fmt.Sprint(row["state"]), FencingToken: runtimeHostInt64(row["fencing_token"]), LeaseTokenHash: fmt.Sprint(row["lease_token_hash"]), CapabilityHash: fmt.Sprint(row["capability_hash"]), ExecutionScope: fmt.Sprint(row["execution_scope"]), ExecutionScopeSource: fmt.Sprint(row["execution_scope_source"]), DispatchID: fmt.Sprint(row["dispatch_id"]), Version: runtimeHostInt64(row["version"])}
	if value, ok := row["expires_at"].(time.Time); ok {
		item.ExpiresAt = value
	}
	if value, ok := row["last_renewed_at"].(time.Time); ok {
		item.LastRenewedAt = value
	}
	if value, ok := row["created_at"].(time.Time); ok {
		item.CreatedAt = value
	}
	if value, ok := row["updated_at"].(time.Time); ok {
		item.UpdatedAt = value
	}
	return item, nil
}

const runtimeDispatchSelect = `select dispatch_id,run_id,reservation_id,coalesce(capacity_reservation_id,''),coalesce(capacity_reserved_version,0),runtime_host_id,coalesce(assigned_runtime_host_instance_id,''),coalesce(assigned_runtime_host_instance_generation,0),coalesce(dispatch_identity,''),dispatch_attempt,plan_version,state,fencing_token,run_ticket_jti_hash,run_ticket_expires_at,input_manifest_hash,coalesce(runtime_request_id,''),abort_requested_at,coalesce(abort_status,''),coalesce(owner_instance_id,''),coalesce(lease_token_hash,''),lease_expires_at,coalesce(recovery_owner_instance_id,''),coalesce(recovery_fencing_token,0),recovery_expires_at,recovery_attempt,next_recovery_check_at,event_cursor,event_lower_bound,coalesce(event_upper_bound,0),coalesce(event_gap_expected_sequence,0),coalesce(event_gap_observed_sequence,0),version,created_at,updated_at from runtime_run_dispatches`

func scanRuntimeDispatchFull(row runtimeHostScanner) (RuntimeDispatch, error) {
	var item RuntimeDispatch
	var leaseExpires, recoveryExpires, nextCheck *time.Time
	err := row.Scan(
		&item.DispatchID, &item.RunID, &item.ReservationID, &item.CapacityReservationID, &item.CapacityReservedVersion, &item.RuntimeHostID, &item.AssignedRuntimeHostInstanceID, &item.AssignedRuntimeHostInstanceGeneration, &item.DispatchIdentity, &item.DispatchAttempt, &item.PlanVersion,
		&item.State, &item.FencingToken, &item.RunTicketJTIHash, &item.TicketExpiresAt, &item.InputManifestHash, &item.RuntimeRequestID,
		&item.AbortRequestedAt, &item.AbortStatus, &item.OwnerInstanceID, &item.LeaseTokenHash, &leaseExpires,
		&item.RecoveryOwnerInstanceID, &item.RecoveryFencingToken, &recoveryExpires, &item.RecoveryAttempt, &nextCheck,
		&item.EventCursor, &item.EventLowerBound, &item.EventUpperBound, &item.EventGapExpectedSequence,
		&item.EventGapObservedSequence, &item.Version, &item.CreatedAt, &item.UpdatedAt,
	)
	if leaseExpires != nil {
		item.LeaseExpiresAt = *leaseExpires
	}
	if recoveryExpires != nil {
		item.RecoveryExpiresAt = *recoveryExpires
	}
	if nextCheck != nil {
		item.NextRecoveryCheckAt = *nextCheck
	}
	return item, err
}
