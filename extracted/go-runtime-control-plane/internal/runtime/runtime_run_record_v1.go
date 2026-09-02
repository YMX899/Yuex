package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"huahuoai/backend/source/internal/domain"
	"huahuoai/backend/source/internal/persistence"
)

// RuntimeRunRecordV1 is the durable, non-secret execution trace owned by the
// Runtime Workspace V1 dispatch lifecycle. It deliberately mirrors immutable
// AgentRun, Plan, frozen Workspace, Host, reservation, and dispatch facts. It
// is not the legacy in-memory RuntimeRunRecord used by old synchronous flows.
type RuntimeRunRecordV1 struct {
	RunID                         string         `json:"runId"`
	AgentRunID                    string         `json:"agentRunId"`
	TaskID                        string         `json:"taskId,omitempty"`
	TenantID                      string         `json:"tenantId"`
	UserID                        string         `json:"userId"`
	ThreadID                      string         `json:"threadId,omitempty"`
	WorkspaceID                   string         `json:"workspaceId"`
	WorkspaceVersion              int64          `json:"workspaceVersion"`
	IndexVersion                  int64          `json:"indexVersion"`
	ThreadWorkspaceBindingVersion int64          `json:"threadWorkspaceBindingVersion"`
	ContextGeneration             int64          `json:"contextGeneration"`
	SessionGeneration             int            `json:"sessionGeneration"`
	ExecutionScope                string         `json:"executionScope"`
	PlanVersion                   int            `json:"planVersion"`
	RuntimeConfigID               string         `json:"runtimeConfigId"`
	RuntimeConfigVersion          string         `json:"runtimeConfigVersion"`
	CapabilityHash                string         `json:"capabilityHash"`
	InputManifestHash             string         `json:"inputManifestHash"`
	RuntimeHostID                 string         `json:"runtimeHostId"`
	ReservationID                 string         `json:"reservationId"`
	DispatchID                    string         `json:"dispatchId"`
	DispatchAttempt               int            `json:"dispatchAttempt"`
	FencingToken                  int64          `json:"fencingToken"`
	RuntimeRequestID              string         `json:"runtimeRequestId,omitempty"`
	Status                        string         `json:"status"`
	ConfigSnapshot                map[string]any `json:"configSnapshot"`
	ResultSnapshot                map[string]any `json:"resultSnapshot,omitempty"`
	UsageSummary                  map[string]any `json:"usageSummary,omitempty"`
	ErrorSummary                  map[string]any `json:"errorSummary,omitempty"`
	LastEventSequence             int64          `json:"lastEventSequence"`
}

const runtimeRunRecordV1SchemaVersion = "runtime_run_record.v1"

func (r *RuntimeHostRepository) postgresConfigured() bool {
	return r != nil && r.db != nil && !r.db.Disabled
}

// NewRuntimeRunRecordV1 constructs the immutable V1 record identity before a
// dispatch is created. Host/reservation/dispatch facts are filled by
// CreateDispatchWithRuntimeRunRecord after the reservation fence is validated.
func NewRuntimeRunRecordV1(run persistence.AgentRunRecord, plan AgentRunPlan, frozen domain.RunWorkspaceContextRecord, runtimeConfigID, runtimeConfigVersion string) (RuntimeRunRecordV1, error) {
	record := RuntimeRunRecordV1{
		RunID:                         strings.TrimSpace(run.AgentRunID),
		AgentRunID:                    strings.TrimSpace(run.AgentRunID),
		TaskID:                        strings.TrimSpace(run.TaskID),
		TenantID:                      strings.TrimSpace(run.TenantID),
		UserID:                        strings.TrimSpace(run.UserID),
		ThreadID:                      strings.TrimSpace(run.ThreadID),
		WorkspaceID:                   strings.TrimSpace(run.WorkspaceID),
		WorkspaceVersion:              run.WorkspaceVersion,
		IndexVersion:                  frozen.IndexVersion,
		ThreadWorkspaceBindingVersion: run.BindingVersion,
		ContextGeneration:             run.ContextGeneration,
		SessionGeneration:             frozen.SessionGeneration,
		ExecutionScope:                strings.TrimSpace(plan.ExecutionScope),
		PlanVersion:                   plan.PlanVersion,
		RuntimeConfigID:               strings.TrimSpace(runtimeConfigID),
		RuntimeConfigVersion:          strings.TrimSpace(runtimeConfigVersion),
		CapabilityHash:                strings.TrimSpace(plan.CapabilityHash),
		InputManifestHash:             "",
		Status:                        "created",
		ConfigSnapshot:                map[string]any{},
		ResultSnapshot:                map[string]any{},
		UsageSummary:                  map[string]any{},
		ErrorSummary:                  map[string]any{},
	}
	if strings.TrimSpace(plan.SchemaVersion) != "agent_run_plan.v1" || strings.TrimSpace(plan.AgentRunID) != record.RunID ||
		plan.WorkspaceVersion != record.WorkspaceVersion || plan.IndexVersion != record.IndexVersion ||
		strings.TrimSpace(plan.WorkspaceContextManifestHash) == "" || strings.TrimSpace(plan.WorkspaceContextManifestHash) != strings.TrimSpace(frozen.ManifestHash) ||
		strings.TrimSpace(frozen.RunID) != record.RunID || strings.TrimSpace(frozen.AgentRunID) != record.AgentRunID ||
		strings.TrimSpace(frozen.TenantID) != record.TenantID || strings.TrimSpace(frozen.UserID) != record.UserID ||
		strings.TrimSpace(frozen.WorkspaceID) != record.WorkspaceID || strings.TrimSpace(frozen.ThreadID) != record.ThreadID ||
		frozen.WorkspaceVersion != record.WorkspaceVersion || frozen.IndexVersion != record.IndexVersion ||
		frozen.ThreadWorkspaceBindingVersion != record.ThreadWorkspaceBindingVersion || frozen.ContextGeneration != record.ContextGeneration ||
		strings.TrimSpace(frozen.CapabilityHash) != record.CapabilityHash || strings.TrimSpace(frozen.Status) != "frozen" {
		return RuntimeRunRecordV1{}, fmt.Errorf("AGENT_PLAN_EXPIRED")
	}
	if err := validateRuntimeRunRecordV1Identity(record); err != nil {
		return RuntimeRunRecordV1{}, err
	}
	return record, nil
}

// CreateDispatchWithRuntimeRunRecord writes the V1 dispatch and its initial
// RuntimeRunRecord in one durable transaction. When a Product database is
// configured but unavailable it fails closed; it never substitutes the local
// in-memory mirror for the production execution trace.
func (r *RuntimeHostRepository) CreateDispatchWithRuntimeRunRecord(ctx context.Context, dispatch RuntimeDispatch, record RuntimeRunRecordV1) (RuntimeDispatch, error) {
	return r.createDispatch(ctx, dispatch, &record)
}

// GetRuntimeRunRecordV1 exposes only the local test mirror. Production reads
// the same facts through PostgreSQL/admin repositories; this method is not an
// execution fallback.
func (r *RuntimeHostRepository) GetRuntimeRunRecordV1(ctx context.Context, runID string) (RuntimeRunRecordV1, error) {
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return RuntimeRunRecordV1{}, fmt.Errorf("NOT_FOUND")
	}
	if r == nil {
		return RuntimeRunRecordV1{}, fmt.Errorf("RUNTIME_RUN_RECORD_UNAVAILABLE")
	}
	if r.postgresConfigured() {
		if !r.postgresReady() {
			return RuntimeRunRecordV1{}, fmt.Errorf("RUNTIME_RUN_RECORD_UNAVAILABLE")
		}
		return r.getRuntimeRunRecordV1Postgres(ctx, runID)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	record, ok := r.runtimeRunRecords[runID]
	if !ok {
		return RuntimeRunRecordV1{}, fmt.Errorf("NOT_FOUND")
	}
	return cloneRuntimeRunRecordV1(record), nil
}

func (r *RuntimeHostRepository) createDispatch(ctx context.Context, dispatch RuntimeDispatch, record *RuntimeRunRecordV1) (RuntimeDispatch, error) {
	if dispatch.DispatchID == "" || dispatch.RunID == "" || dispatch.ReservationID == "" || dispatch.RuntimeHostID == "" || (!dispatch.HasCapacityReservationBinding() && !dispatch.IsLegacyCapacityUnbound()) || dispatch.DispatchAttempt < 1 || dispatch.PlanVersion < 1 || dispatch.FencingToken < 1 || dispatch.RunTicketJTIHash == "" {
		return RuntimeDispatch{}, fmt.Errorf("INVALID_ARGUMENT")
	}
	if record != nil {
		if err := validateRuntimeRunRecordV1Identity(*record); err != nil {
			return RuntimeDispatch{}, err
		}
		if record.RunID != dispatch.RunID || record.AgentRunID != dispatch.RunID || record.PlanVersion != dispatch.PlanVersion {
			return RuntimeDispatch{}, fmt.Errorf("AGENT_PLAN_INVALID")
		}
	}
	dispatch.State = "created"
	dispatch.Version = 1
	dispatch.EventLowerBound = 1
	dispatch.CreatedAt = time.Now().UTC()
	dispatch.UpdatedAt = dispatch.CreatedAt
	if r.postgresConfigured() {
		if !r.postgresReady() {
			return RuntimeDispatch{}, fmt.Errorf("RUNTIME_RUN_RECORD_UNAVAILABLE")
		}
		if dispatch.OwnerInstanceID == "" || dispatch.LeaseTokenHash == "" || dispatch.LeaseExpiresAt.IsZero() || !dispatch.HasCapacityReservationBinding() {
			return RuntimeDispatch{}, fmt.Errorf("INVALID_ARGUMENT")
		}
		err := r.db.WithTx(ctx, func(tx *persistence.Tx) error {
			rows, err := tx.Query(ctx, `select reservation_id,run_id,runtime_host_id,coalesce(assigned_runtime_host_instance_id,'') assigned_runtime_host_instance_id,coalesce(assigned_runtime_host_instance_generation,0) assigned_runtime_host_instance_generation,owner_instance_id,state,fencing_token,lease_token_hash,capability_hash,execution_scope,execution_scope_source,coalesce(dispatch_id,'') dispatch_id,expires_at,last_renewed_at,version,created_at,updated_at from runtime_slot_reservations where reservation_id=@id for update`, map[string]any{"id": dispatch.ReservationID})
			if err != nil {
				return err
			}
			if len(rows) != 1 {
				return fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
			}
			reservation, err := runtimeReservationFromMap(rows[0])
			if err != nil {
				return err
			}
			if err := bindDispatchToReservation(&dispatch, reservation); err != nil {
				return err
			}
			if record != nil {
				if err := r.createRuntimeRunRecordV1Tx(ctx, tx, *record, dispatch); err != nil {
					return err
				}
			}
			return tx.Exec(ctx, `insert into runtime_run_dispatches(dispatch_id,run_id,reservation_id,capacity_reservation_id,capacity_reserved_version,runtime_host_id,assigned_runtime_host_instance_id,assigned_runtime_host_instance_generation,dispatch_identity,dispatch_attempt,plan_version,state,fencing_token,run_ticket_jti_hash,run_ticket_expires_at,input_manifest_hash,owner_instance_id,lease_token_hash,lease_expires_at,event_lower_bound,version) values(@dispatch,@run,@reservation,@capacityReservation,@capacityVersion,@host,@assignedInstanceID,@assignedGeneration,@dispatchIdentity,@attempt,@plan,'created',@fencing,@jti,@ticketExpires,@manifest,@owner,@leaseToken,@leaseExpires,1,1)`, map[string]any{
				"dispatch": dispatch.DispatchID, "run": dispatch.RunID, "reservation": dispatch.ReservationID, "capacityReservation": dispatch.CapacityReservationID, "capacityVersion": dispatch.CapacityReservedVersion, "host": dispatch.RuntimeHostID,
				"assignedInstanceID": dispatch.AssignedRuntimeHostInstanceID, "assignedGeneration": dispatch.AssignedRuntimeHostInstanceGeneration,
				"dispatchIdentity": dispatch.DispatchIdentity, "attempt": dispatch.DispatchAttempt, "plan": dispatch.PlanVersion,
				"fencing": dispatch.FencingToken, "jti": dispatch.RunTicketJTIHash, "ticketExpires": dispatch.TicketExpiresAt,
				"manifest": dispatch.InputManifestHash, "owner": dispatch.OwnerInstanceID, "leaseToken": dispatch.LeaseTokenHash,
				"leaseExpires": dispatch.LeaseExpiresAt,
			})
		})
		return dispatch, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	reservation, ok := r.reservations[dispatch.ReservationID]
	if !ok {
		return RuntimeDispatch{}, fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
	}
	if err := bindDispatchToReservation(&dispatch, reservation); err != nil {
		return RuntimeDispatch{}, err
	}
	for _, current := range r.dispatches {
		if current.RunID == dispatch.RunID && activeRuntimeDispatchState(current.State) {
			return RuntimeDispatch{}, fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
		}
	}
	if record != nil {
		prepared, err := runtimeRunRecordV1ForDispatch(*record, dispatch)
		if err != nil {
			return RuntimeDispatch{}, err
		}
		r.runtimeRunRecords[prepared.RunID] = prepared
	}
	r.dispatches[dispatch.DispatchID] = dispatch
	return dispatch, nil
}

func (r *RuntimeHostRepository) createRuntimeRunRecordV1Tx(ctx context.Context, tx *persistence.Tx, record RuntimeRunRecordV1, dispatch RuntimeDispatch) error {
	prepared, err := runtimeRunRecordV1ForDispatch(record, dispatch)
	if err != nil {
		return err
	}
	configSnapshot, err := runtimeRunRecordV1JSON(prepared.ConfigSnapshot)
	if err != nil {
		return err
	}
	rows, err := tx.Query(ctx, `
select run_id,coalesce(agent_run_id,''),coalesce(task_id,''),coalesce(user_id,''),coalesce(thread_id,''),
       coalesce(workspace_id,''),coalesce(execution_scope,''),coalesce(workspace_version,0),
       coalesce(index_version,0),coalesce(thread_workspace_binding_version,0),coalesce(context_generation,0),
	       coalesce(session_generation,0),status,
	       coalesce(config_snapshot->>'schemaVersion','') schema_version,
       coalesce(config_snapshot->>'tenantId','') tenant_id,
       coalesce(config_snapshot->>'planVersion','') plan_version,
       coalesce(config_snapshot->>'runtimeConfigId','') runtime_config_id,
       coalesce(config_snapshot->>'runtimeConfigVersion','') runtime_config_version,
       coalesce(config_snapshot->>'capabilityHash','') capability_hash,
       coalesce(config_snapshot->>'status','') config_status
from runtime_run_records
where run_id=@run
for update`, map[string]any{"run": prepared.RunID})
	if err != nil {
		return err
	}
	if len(rows) > 1 {
		return fmt.Errorf("RUNTIME_RUN_RECORD_CONFLICT")
	}
	if len(rows) == 1 {
		existing := rows[0]
		if fmt.Sprint(existing["agent_run_id"]) != prepared.AgentRunID || fmt.Sprint(existing["task_id"]) != prepared.TaskID ||
			fmt.Sprint(existing["user_id"]) != prepared.UserID || fmt.Sprint(existing["thread_id"]) != prepared.ThreadID ||
			fmt.Sprint(existing["workspace_id"]) != prepared.WorkspaceID || fmt.Sprint(existing["execution_scope"]) != prepared.ExecutionScope ||
			runtimeRunRecordV1Int64(existing["workspace_version"]) != prepared.WorkspaceVersion ||
			runtimeRunRecordV1Int64(existing["index_version"]) != prepared.IndexVersion ||
			runtimeRunRecordV1Int64(existing["thread_workspace_binding_version"]) != prepared.ThreadWorkspaceBindingVersion ||
			runtimeRunRecordV1Int64(existing["context_generation"]) != prepared.ContextGeneration ||
			int(runtimeRunRecordV1Int64(existing["session_generation"])) != prepared.SessionGeneration ||
			fmt.Sprint(existing["schema_version"]) != runtimeRunRecordV1SchemaVersion || fmt.Sprint(existing["tenant_id"]) != prepared.TenantID ||
			int(runtimeRunRecordV1Int64(existing["plan_version"])) != prepared.PlanVersion ||
			fmt.Sprint(existing["runtime_config_id"]) != prepared.RuntimeConfigID || fmt.Sprint(existing["runtime_config_version"]) != prepared.RuntimeConfigVersion ||
			fmt.Sprint(existing["capability_hash"]) != prepared.CapabilityHash || fmt.Sprint(existing["config_status"]) != fmt.Sprint(existing["status"]) {
			return fmt.Errorf("RUNTIME_RUN_RECORD_CONFLICT")
		}
		if runtimeRunRecordV1TerminalStatus(fmt.Sprint(existing["status"])) {
			return fmt.Errorf("RUNTIME_RUN_RECORD_CONFLICT")
		}
		_, err := tx.ExecRaw(ctx, `
update runtime_run_records
set config_snapshot=config_snapshot || ($2::jsonb - 'status'),
    runtime_host_id=$3,reservation_id=$4,dispatch_attempt=$5,fencing_token=$6,
    session_generation=$7,updated_at=now()
where run_id=$1 and status in ('created','running')`, prepared.RunID, configSnapshot, prepared.RuntimeHostID, prepared.ReservationID, prepared.DispatchAttempt, prepared.FencingToken, prepared.SessionGeneration)
		return err
	}
	return tx.Exec(ctx, `
insert into runtime_run_records(
  run_id,task_id,thread_id,workspace_id,execution_scope,attempt,status,config_snapshot,
  result_snapshot,usage_summary,error_summary,user_id,workspace_version,index_version,
  thread_workspace_binding_version,context_generation,agent_run_id,runtime_host_id,
  reservation_id,dispatch_attempt,last_event_sequence,fencing_token,session_generation
) values(
  @run,nullif(@task,''),nullif(@thread,''),@workspace,@scope,@attempt,'created',@config::jsonb,
  '{}'::jsonb,'{}'::jsonb,'{}'::jsonb,@user,@workspaceVersion,@indexVersion,
  @bindingVersion,@contextGeneration,@agentRun,@host,@reservation,@dispatchAttempt,0,@fencing,@sessionGeneration
	)`, map[string]any{
		"run": prepared.RunID, "task": prepared.TaskID, "thread": prepared.ThreadID, "workspace": prepared.WorkspaceID,
		"scope": prepared.ExecutionScope, "attempt": prepared.DispatchAttempt, "config": configSnapshot,
		"user": prepared.UserID, "workspaceVersion": prepared.WorkspaceVersion, "indexVersion": prepared.IndexVersion,
		"bindingVersion": prepared.ThreadWorkspaceBindingVersion, "contextGeneration": prepared.ContextGeneration,
		"agentRun": prepared.AgentRunID, "host": prepared.RuntimeHostID, "reservation": prepared.ReservationID,
		"dispatchAttempt": prepared.DispatchAttempt, "fencing": prepared.FencingToken, "sessionGeneration": prepared.SessionGeneration,
	})
}

func (r *RuntimeHostRepository) markRuntimeRunRecordAcceptedTx(ctx context.Context, tx *persistence.Tx, command DispatchAcceptedCommand) error {
	rows, err := tx.Query(ctx, `
select d.run_id,d.runtime_host_id,d.reservation_id,d.fencing_token,
       exists(select 1 from agent_runs ar where ar.agent_run_id=d.run_id) as is_agent_run
from runtime_run_dispatches d
where d.dispatch_id=@dispatch
for update`, map[string]any{"dispatch": command.DispatchID})
	if err != nil || len(rows) != 1 {
		return fmt.Errorf("STALE_FENCING_TOKEN")
	}
	dispatch := rows[0]
	if !runtimeRunRecordV1Bool(dispatch["is_agent_run"]) {
		// Direct legacy/test dispatches are not V1 AgentRuns. They do not get an
		// implicit runtime record through this path.
		return nil
	}
	result, err := tx.ExecRaw(ctx, `
update runtime_run_records
set status='running',runtime_request_id=$2,
    config_snapshot=config_snapshot || jsonb_build_object('runtimeRequestId',$2,'status','running'),
    updated_at=now()
where run_id=$1 and agent_run_id=$1 and runtime_host_id=$3 and reservation_id=$4
  and fencing_token=$5 and status in ('created','running')`, fmt.Sprint(dispatch["run_id"]), command.RuntimeRequestID,
		fmt.Sprint(dispatch["runtime_host_id"]), fmt.Sprint(dispatch["reservation_id"]), runtimeRunRecordV1Int64(dispatch["fencing_token"]))
	if err != nil {
		return err
	}
	if result.RowsAffected() != 1 {
		return fmt.Errorf("RUNTIME_RUN_RECORD_UNAVAILABLE")
	}
	return nil
}

func (r *RuntimeHostRepository) markRuntimeRunRecordAcceptedMemoryLocked(command DispatchAcceptedCommand) {
	dispatch, ok := r.dispatches[command.DispatchID]
	if !ok {
		return
	}
	record, ok := r.runtimeRunRecords[dispatch.RunID]
	if !ok {
		return
	}
	record.Status = "running"
	record.RuntimeRequestID = command.RuntimeRequestID
	record.ConfigSnapshot["runtimeRequestId"] = command.RuntimeRequestID
	record.ConfigSnapshot["status"] = "running"
	r.runtimeRunRecords[record.RunID] = record
}

func (r *RuntimeHostRepository) finalizeRuntimeRunRecordV1Tx(ctx context.Context, tx *persistence.Tx, command TerminalConvergenceCommand) error {
	rows, err := tx.Query(ctx, `
select d.run_id,d.runtime_host_id,d.reservation_id,d.fencing_token,
       exists(select 1 from agent_runs ar where ar.agent_run_id=d.run_id) as is_agent_run
from runtime_run_dispatches d
where d.dispatch_id=@dispatch and d.run_id=@run
for update`, map[string]any{"dispatch": command.DispatchID, "run": command.RunID})
	if err != nil || len(rows) != 1 {
		return fmt.Errorf("STALE_FENCING_TOKEN")
	}
	dispatch := rows[0]
	if !runtimeRunRecordV1Bool(dispatch["is_agent_run"]) {
		return nil
	}
	status := runtimeRunRecordV1TerminalStatusForRuntime(command.TerminalStatus)
	resultSnapshot, err := runtimeRunRecordV1JSON(command.SafeResult)
	if err != nil {
		return err
	}
	usageSummary, err := runtimeRunRecordV1JSON(command.ActualUsage)
	if err != nil {
		return err
	}
	errorSummary, err := runtimeRunRecordV1JSON(command.SafeError)
	if err != nil {
		return err
	}
	result, err := tx.ExecRaw(ctx, `
update runtime_run_records
set status=$2,result_snapshot=$3::jsonb,usage_summary=$4::jsonb,error_summary=$5::jsonb,
    last_event_sequence=greatest(last_event_sequence,$6),
    orphaned_downstream=case when $2='orphaned' then true else orphaned_downstream end,
    abort_status=case when $2='aborted' then 'terminal' else abort_status end,
    config_snapshot=config_snapshot || jsonb_build_object('status',$2,'terminalStatus',$2,'terminalSourceSequence',greatest(last_event_sequence,$6)),
    updated_at=now()
where run_id=$1 and agent_run_id=$1 and runtime_host_id=$7 and reservation_id=$8
  and fencing_token=$9 and status in ('created','running',$2)`, command.RunID, status,
		resultSnapshot, usageSummary, errorSummary, command.TerminalSourceSequence,
		fmt.Sprint(dispatch["runtime_host_id"]), fmt.Sprint(dispatch["reservation_id"]), runtimeRunRecordV1Int64(dispatch["fencing_token"]))
	if err != nil {
		return err
	}
	if result.RowsAffected() != 1 {
		return fmt.Errorf("RUNTIME_RUN_RECORD_UNAVAILABLE")
	}
	return nil
}

func (r *RuntimeHostRepository) finalizeRuntimeRunRecordV1Memory(command TerminalConvergenceCommand) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	record, ok := r.runtimeRunRecords[command.RunID]
	if !ok {
		return
	}
	record.Status = runtimeRunRecordV1TerminalStatusForRuntime(command.TerminalStatus)
	record.ResultSnapshot = cloneRuntimeRunRecordV1Map(command.SafeResult)
	record.UsageSummary = cloneRuntimeRunRecordV1Map(command.ActualUsage)
	record.ErrorSummary = cloneRuntimeRunRecordV1Map(command.SafeError)
	if command.TerminalSourceSequence > record.LastEventSequence {
		record.LastEventSequence = command.TerminalSourceSequence
	}
	record.ConfigSnapshot["terminalStatus"] = record.Status
	record.ConfigSnapshot["status"] = record.Status
	record.ConfigSnapshot["terminalSourceSequence"] = record.LastEventSequence
	r.runtimeRunRecords[record.RunID] = record
}

func (r *RuntimeHostRepository) getRuntimeRunRecordV1Postgres(ctx context.Context, runID string) (RuntimeRunRecordV1, error) {
	var record RuntimeRunRecordV1
	var configRaw, resultRaw, usageRaw, errorRaw []byte
	err := r.db.Pool.QueryRow(ctx, `
select run_id,coalesce(agent_run_id,''),coalesce(task_id,''),coalesce(user_id,''),coalesce(thread_id,''),
       coalesce(workspace_id,''),coalesce(workspace_version,0),coalesce(index_version,0),
       coalesce(thread_workspace_binding_version,0),coalesce(context_generation,0),coalesce(session_generation,0),
       execution_scope,dispatch_attempt,coalesce(runtime_host_id,''),coalesce(reservation_id,''),
       coalesce(fencing_token,0),coalesce(runtime_request_id,''),status,last_event_sequence,
       config_snapshot,result_snapshot,usage_summary,error_summary
from runtime_run_records where run_id=$1`, runID).Scan(
		&record.RunID, &record.AgentRunID, &record.TaskID, &record.UserID, &record.ThreadID,
		&record.WorkspaceID, &record.WorkspaceVersion, &record.IndexVersion, &record.ThreadWorkspaceBindingVersion,
		&record.ContextGeneration, &record.SessionGeneration, &record.ExecutionScope, &record.DispatchAttempt,
		&record.RuntimeHostID, &record.ReservationID, &record.FencingToken, &record.RuntimeRequestID,
		&record.Status, &record.LastEventSequence, &configRaw, &resultRaw, &usageRaw, &errorRaw,
	)
	if err != nil {
		return RuntimeRunRecordV1{}, err
	}
	record.ConfigSnapshot = runtimeRunRecordV1JSONMap(configRaw)
	record.ResultSnapshot = runtimeRunRecordV1JSONMap(resultRaw)
	record.UsageSummary = runtimeRunRecordV1JSONMap(usageRaw)
	record.ErrorSummary = runtimeRunRecordV1JSONMap(errorRaw)
	if err := hydrateRuntimeRunRecordV1Config(&record); err != nil {
		return RuntimeRunRecordV1{}, err
	}
	return record, nil
}

func runtimeRunRecordV1ForDispatch(record RuntimeRunRecordV1, dispatch RuntimeDispatch) (RuntimeRunRecordV1, error) {
	if err := validateRuntimeRunRecordV1Identity(record); err != nil {
		return RuntimeRunRecordV1{}, err
	}
	if record.RunID != dispatch.RunID || record.AgentRunID != dispatch.RunID || record.PlanVersion != dispatch.PlanVersion ||
		dispatch.RuntimeHostID == "" || dispatch.ReservationID == "" || dispatch.FencingToken < 1 || dispatch.DispatchAttempt < 1 {
		return RuntimeRunRecordV1{}, fmt.Errorf("AGENT_PLAN_INVALID")
	}
	record.RuntimeHostID = dispatch.RuntimeHostID
	record.ReservationID = dispatch.ReservationID
	record.DispatchID = dispatch.DispatchID
	record.DispatchAttempt = dispatch.DispatchAttempt
	record.FencingToken = dispatch.FencingToken
	record.InputManifestHash = dispatch.InputManifestHash
	record.Status = "created"
	record.ConfigSnapshot = runtimeRunRecordV1ConfigSnapshot(record)
	return record, nil
}

func validateRuntimeRunRecordV1Identity(record RuntimeRunRecordV1) error {
	if record.RunID == "" || record.RunID != record.AgentRunID || record.TenantID == "" || record.UserID == "" || record.WorkspaceID == "" ||
		record.WorkspaceVersion < 1 || record.IndexVersion < 0 || record.ThreadWorkspaceBindingVersion < 1 || record.ContextGeneration < 1 ||
		record.SessionGeneration < 0 || record.PlanVersion < 1 || record.RuntimeConfigID == "" || record.RuntimeConfigVersion == "" || record.CapabilityHash == "" {
		return fmt.Errorf("AGENT_PLAN_INVALID")
	}
	switch record.ExecutionScope {
	case string(ScopeProductThread):
		if record.ThreadID == "" || record.SessionGeneration < 1 {
			return fmt.Errorf("AGENT_PLAN_INVALID")
		}
	case string(ScopeDetachedTask):
		if record.SessionGeneration != 0 {
			return fmt.Errorf("AGENT_PLAN_INVALID")
		}
	default:
		return fmt.Errorf("AGENT_PLAN_INVALID")
	}
	return nil
}

func runtimeRunRecordV1ConfigSnapshot(record RuntimeRunRecordV1) map[string]any {
	return map[string]any{
		"schemaVersion":                 runtimeRunRecordV1SchemaVersion,
		"agentRunId":                    record.AgentRunID,
		"taskId":                        record.TaskID,
		"tenantId":                      record.TenantID,
		"userId":                        record.UserID,
		"threadId":                      record.ThreadID,
		"workspaceId":                   record.WorkspaceID,
		"workspaceVersion":              record.WorkspaceVersion,
		"indexVersion":                  record.IndexVersion,
		"threadWorkspaceBindingVersion": record.ThreadWorkspaceBindingVersion,
		"contextGeneration":             record.ContextGeneration,
		"sessionGeneration":             record.SessionGeneration,
		"executionScope":                record.ExecutionScope,
		"planVersion":                   record.PlanVersion,
		"runtimeConfigId":               record.RuntimeConfigID,
		"runtimeConfigVersion":          record.RuntimeConfigVersion,
		"capabilityHash":                record.CapabilityHash,
		"inputManifestHash":             record.InputManifestHash,
		"runtimeHostId":                 record.RuntimeHostID,
		"reservationId":                 record.ReservationID,
		"dispatchId":                    record.DispatchID,
		"dispatchAttempt":               record.DispatchAttempt,
		"fencingToken":                  record.FencingToken,
		"status":                        record.Status,
	}
}

func runtimeRunRecordV1TerminalStatusForRuntime(status string) string {
	switch strings.TrimSpace(status) {
	case "succeeded", "failed", "timeout", "aborted", "rejected", "orphaned":
		return strings.TrimSpace(status)
	default:
		return "failed"
	}
}

func runtimeRunRecordV1TerminalStatus(status string) bool {
	switch strings.TrimSpace(status) {
	case "succeeded", "failed", "timeout", "aborted", "rejected", "orphaned", "forbidden", "cancelled":
		return true
	default:
		return false
	}
}

func runtimeRunRecordV1JSON(value map[string]any) (string, error) {
	if value == nil {
		value = map[string]any{}
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("RUNTIME_RUN_RECORD_INVALID")
	}
	return string(raw), nil
}

func runtimeRunRecordV1JSONMap(raw []byte) map[string]any {
	value := map[string]any{}
	_ = json.Unmarshal(raw, &value)
	return value
}

func hydrateRuntimeRunRecordV1Config(record *RuntimeRunRecordV1) error {
	if record == nil {
		return fmt.Errorf("RUNTIME_RUN_RECORD_UNAVAILABLE")
	}
	config := record.ConfigSnapshot
	tenantID, tenantOK := runtimeRunRecordV1SnapshotString(config, "tenantId")
	runtimeConfigID, runtimeConfigIDOK := runtimeRunRecordV1SnapshotString(config, "runtimeConfigId")
	runtimeConfigVersion, runtimeConfigVersionOK := runtimeRunRecordV1SnapshotString(config, "runtimeConfigVersion")
	capabilityHash, capabilityHashOK := runtimeRunRecordV1SnapshotString(config, "capabilityHash")
	inputManifestHash, inputManifestHashOK := runtimeRunRecordV1SnapshotString(config, "inputManifestHash")
	dispatchID, dispatchIDOK := runtimeRunRecordV1SnapshotString(config, "dispatchId")
	planVersion, planVersionOK := runtimeRunRecordV1SnapshotInt64(config, "planVersion")
	workspaceVersion, workspaceVersionOK := runtimeRunRecordV1SnapshotInt64(config, "workspaceVersion")
	indexVersion, indexVersionOK := runtimeRunRecordV1SnapshotInt64(config, "indexVersion")
	bindingVersion, bindingVersionOK := runtimeRunRecordV1SnapshotInt64(config, "threadWorkspaceBindingVersion")
	contextGeneration, contextGenerationOK := runtimeRunRecordV1SnapshotInt64(config, "contextGeneration")
	sessionGeneration, sessionGenerationOK := runtimeRunRecordV1SnapshotInt64(config, "sessionGeneration")
	dispatchAttempt, dispatchAttemptOK := runtimeRunRecordV1SnapshotInt64(config, "dispatchAttempt")
	fencingToken, fencingTokenOK := runtimeRunRecordV1SnapshotInt64(config, "fencingToken")

	if runtimeRunRecordV1SnapshotValue(config, "schemaVersion") != runtimeRunRecordV1SchemaVersion || !tenantOK || !runtimeConfigIDOK ||
		!runtimeConfigVersionOK || !capabilityHashOK || !inputManifestHashOK || !dispatchIDOK || !planVersionOK ||
		!workspaceVersionOK || !indexVersionOK || !bindingVersionOK || !contextGenerationOK || !sessionGenerationOK ||
		!dispatchAttemptOK || !fencingTokenOK ||
		runtimeRunRecordV1SnapshotValue(config, "agentRunId") != record.AgentRunID ||
		runtimeRunRecordV1SnapshotValue(config, "taskId") != record.TaskID ||
		runtimeRunRecordV1SnapshotValue(config, "userId") != record.UserID ||
		runtimeRunRecordV1SnapshotValue(config, "threadId") != record.ThreadID ||
		runtimeRunRecordV1SnapshotValue(config, "workspaceId") != record.WorkspaceID ||
		runtimeRunRecordV1SnapshotValue(config, "executionScope") != record.ExecutionScope ||
		workspaceVersion != record.WorkspaceVersion || indexVersion != record.IndexVersion ||
		bindingVersion != record.ThreadWorkspaceBindingVersion || contextGeneration != record.ContextGeneration ||
		int(sessionGeneration) != record.SessionGeneration || dispatchAttempt != int64(record.DispatchAttempt) ||
		fencingToken != record.FencingToken || runtimeRunRecordV1SnapshotValue(config, "runtimeHostId") != record.RuntimeHostID ||
		runtimeRunRecordV1SnapshotValue(config, "reservationId") != record.ReservationID ||
		runtimeRunRecordV1SnapshotValue(config, "status") != record.Status {
		return fmt.Errorf("RUNTIME_RUN_RECORD_UNAVAILABLE")
	}
	if value, ok := runtimeRunRecordV1SnapshotString(config, "runtimeRequestId"); ok && value != record.RuntimeRequestID {
		return fmt.Errorf("RUNTIME_RUN_RECORD_UNAVAILABLE")
	}
	if value, exists := config["terminalSourceSequence"]; exists && runtimeRunRecordV1Int64(value) != record.LastEventSequence {
		return fmt.Errorf("RUNTIME_RUN_RECORD_UNAVAILABLE")
	}
	record.TenantID = tenantID
	record.RuntimeConfigID = runtimeConfigID
	record.RuntimeConfigVersion = runtimeConfigVersion
	record.CapabilityHash = capabilityHash
	record.InputManifestHash = inputManifestHash
	record.PlanVersion = int(planVersion)
	record.DispatchID = dispatchID
	if err := validateRuntimeRunRecordV1Identity(*record); err != nil {
		return fmt.Errorf("RUNTIME_RUN_RECORD_UNAVAILABLE")
	}
	return nil
}

func runtimeRunRecordV1SnapshotString(snapshot map[string]any, key string) (string, bool) {
	value, ok := snapshot[key]
	if !ok {
		return "", false
	}
	text, ok := value.(string)
	if !ok {
		return "", false
	}
	return strings.TrimSpace(text), true
}

func runtimeRunRecordV1SnapshotValue(snapshot map[string]any, key string) string {
	value, ok := runtimeRunRecordV1SnapshotString(snapshot, key)
	if !ok {
		return ""
	}
	return value
}

func runtimeRunRecordV1SnapshotInt64(snapshot map[string]any, key string) (int64, bool) {
	value, ok := snapshot[key]
	if !ok {
		return 0, false
	}
	switch typed := value.(type) {
	case int:
		return int64(typed), true
	case int32:
		return int64(typed), true
	case int64:
		return typed, true
	case float64:
		if float64(int64(typed)) != typed {
			return 0, false
		}
		return int64(typed), true
	default:
		return 0, false
	}
}

func cloneRuntimeRunRecordV1(record RuntimeRunRecordV1) RuntimeRunRecordV1 {
	record.ConfigSnapshot = cloneRuntimeRunRecordV1Map(record.ConfigSnapshot)
	record.ResultSnapshot = cloneRuntimeRunRecordV1Map(record.ResultSnapshot)
	record.UsageSummary = cloneRuntimeRunRecordV1Map(record.UsageSummary)
	record.ErrorSummary = cloneRuntimeRunRecordV1Map(record.ErrorSummary)
	return record
}

func cloneRuntimeRunRecordV1Map(value map[string]any) map[string]any {
	if value == nil {
		return map[string]any{}
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return map[string]any{}
	}
	copy := map[string]any{}
	_ = json.Unmarshal(raw, &copy)
	return copy
}

func runtimeRunRecordV1Int64(value any) int64 {
	switch typed := value.(type) {
	case int:
		return int64(typed)
	case int32:
		return int64(typed)
	case int64:
		return typed
	case float64:
		return int64(typed)
	default:
		var parsed int64
		_, _ = fmt.Sscan(fmt.Sprint(value), &parsed)
		return parsed
	}
}

func runtimeRunRecordV1Bool(value any) bool {
	parsed, ok := value.(bool)
	return ok && parsed
}
