package persistence

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// AgentRunCreatePlanningCommand is the immutable first durable graph for an
// AgentRun. The repository derives the planning queue and public events; a
// caller cannot omit one of those facts after the Run has been created.
type AgentRunCreatePlanningCommand struct {
	Record    AgentRunRecord
	CreatedAt time.Time
}

// AgentRunPlanningSuccessCommand is the final fenced mutation for one
// agent_planning attempt. It is deliberately separate from SavePlan: a
// successful non-confirming plan must also own its Runtime handoff, planning
// queue acknowledgement, and public event in the same transaction.
type AgentRunPlanningSuccessCommand struct {
	Plan       AgentRunPlanRecord
	QueueProof QueueLeaseProof
}

// Runtime capacity is intentionally a queueing concern: a temporary lack of a
// Host slot must not turn a successfully planned Run into a terminal failure.
const agentRunRuntimeCapacityQueueMaxAttempts = 120

func (r *AgentRunRepository) CreateRunAndEnqueuePlanning(ctx context.Context, command AgentRunCreatePlanningCommand, queueRepo *QueueRepository) (AgentRunRecord, bool, error) {
	command, err := normalizeAgentRunCreatePlanningCommand(command)
	if err != nil {
		return AgentRunRecord{}, false, err
	}
	if !r.agentRunPlanningPostgresReady(queueRepo) {
		if agentRunOrQueueDurableConfigured(r, queueRepo) {
			return AgentRunRecord{}, false, fmt.Errorf("AGENT_RUN_PLANNING_UNAVAILABLE")
		}
		return r.createRunAndEnqueuePlanningMemory(command, queueRepo)
	}
	return r.createRunAndEnqueuePlanningPostgres(ctx, command, queueRepo)
}

func (r *AgentRunRepository) FinalizePlanningSuccess(ctx context.Context, command AgentRunPlanningSuccessCommand, queueRepo *QueueRepository) error {
	command, err := normalizeAgentRunPlanningSuccessCommand(command)
	if err != nil {
		return err
	}
	if !r.agentRunPlanningPostgresReady(queueRepo) {
		if agentRunOrQueueDurableConfigured(r, queueRepo) {
			return fmt.Errorf("AGENT_RUN_PLANNING_UNAVAILABLE")
		}
		return r.finalizePlanningSuccessMemory(command, queueRepo)
	}
	return r.finalizePlanningSuccessPostgres(ctx, command)
}

func (r *AgentRunRepository) agentRunPlanningPostgresReady(queueRepo *QueueRepository) bool {
	return r != nil && r.db != nil && !r.db.Disabled && r.db.Pool != nil &&
		queueRepo != nil && queueRepo.db != nil && !queueRepo.db.Disabled && queueRepo.db.Pool != nil &&
		sameDatabaseTransactionBoundary(r.db, queueRepo.db)
}

func agentRunOrQueueDurableConfigured(r *AgentRunRepository, queueRepo *QueueRepository) bool {
	return (r != nil && r.db != nil && !r.db.Disabled) ||
		(queueRepo != nil && queueRepo.db != nil && !queueRepo.db.Disabled)
}

func normalizeAgentRunCreatePlanningCommand(command AgentRunCreatePlanningCommand) (AgentRunCreatePlanningCommand, error) {
	record := command.Record
	record.AgentRunID = strings.TrimSpace(record.AgentRunID)
	record.TenantID = strings.TrimSpace(record.TenantID)
	record.UserID = strings.TrimSpace(record.UserID)
	record.WorkspaceID = strings.TrimSpace(record.WorkspaceID)
	record.ThreadID = strings.TrimSpace(record.ThreadID)
	record.TaskID = strings.TrimSpace(record.TaskID)
	record.IdempotencyKey = strings.TrimSpace(record.IdempotencyKey)
	record.RequestHash = strings.TrimSpace(record.RequestHash)
	record.Status = strings.TrimSpace(record.Status)
	record.RoutingMode = strings.TrimSpace(record.RoutingMode)
	if record.AgentRunID == "" || record.TenantID == "" || record.UserID == "" || record.WorkspaceID == "" ||
		record.IdempotencyKey == "" || record.RequestHash == "" || record.Status != "planning" ||
		record.RoutingMode != "dynamic" || record.WorkspaceVersion < 1 || record.BindingVersion < 1 || record.ContextGeneration < 1 ||
		len(record.RequestSnapshot) == 0 || len(record.IntentSnapshot) == 0 {
		return AgentRunCreatePlanningCommand{}, fmt.Errorf("AGENT_PLAN_INVALID")
	}
	if command.CreatedAt.IsZero() {
		command.CreatedAt = time.Now().UTC()
	} else {
		command.CreatedAt = command.CreatedAt.UTC()
	}
	record.RequestSnapshot = copyMap(record.RequestSnapshot)
	record.IntentSnapshot = copyMap(record.IntentSnapshot)
	command.Record = record
	return command, nil
}

func normalizeAgentRunPlanningSuccessCommand(command AgentRunPlanningSuccessCommand) (AgentRunPlanningSuccessCommand, error) {
	command.Plan.AgentRunID = strings.TrimSpace(command.Plan.AgentRunID)
	command.Plan.PlanStatus = strings.TrimSpace(command.Plan.PlanStatus)
	command.Plan.AgentRunStatus = strings.TrimSpace(command.Plan.AgentRunStatus)
	command.Plan.Plan = copyMap(command.Plan.Plan)
	if command.Plan.AgentRunID == "" || command.Plan.PlanVersion < 1 || len(command.Plan.Plan) == 0 ||
		(command.Plan.PlanStatus != "validated" || command.Plan.AgentRunStatus != "queued") &&
			(command.Plan.PlanStatus != "awaiting_confirmation" || command.Plan.AgentRunStatus != "awaiting_confirmation") {
		return AgentRunPlanningSuccessCommand{}, fmt.Errorf("AGENT_PLAN_INVALID")
	}
	if planRunID := strings.TrimSpace(stringOr(command.Plan.Plan["agentRunId"], "")); planRunID != "" && planRunID != command.Plan.AgentRunID {
		return AgentRunPlanningSuccessCommand{}, fmt.Errorf("AGENT_PLAN_INVALID")
	}
	if planVersion := intValue(command.Plan.Plan["planVersion"]); planVersion != 0 && planVersion != command.Plan.PlanVersion {
		return AgentRunPlanningSuccessCommand{}, fmt.Errorf("AGENT_PLAN_INVALID")
	}
	if err := validateQueueLeaseProof(command.QueueProof); err != nil {
		return AgentRunPlanningSuccessCommand{}, err
	}
	return command, nil
}

func agentRunPlanningQueueCommand(record AgentRunRecord) map[string]any {
	return map[string]any{
		"queueId": "queue_plan_" + record.AgentRunID, "queueName": "agent_planning",
		"taskType": "agent_planning", "taskId": record.AgentRunID,
		"dedupeKey": "agent_planning:" + record.AgentRunID, "priority": 50,
		"payload": map[string]any{
			"agentRunId": record.AgentRunID, "tenantId": record.TenantID,
			"userId": record.UserID, "workspaceId": record.WorkspaceID,
		},
	}
}

func agentRunPlanningQueueMatches(record map[string]any, run AgentRunRecord) bool {
	if stringOr(record["queueId"], "") != "queue_plan_"+run.AgentRunID ||
		stringOr(record["queueName"], "") != "agent_planning" ||
		stringOr(record["taskType"], "") != "agent_planning" ||
		stringOr(record["taskId"], "") != run.AgentRunID ||
		stringOr(record["dedupeKey"], "") != "agent_planning:"+run.AgentRunID {
		return false
	}
	payload := mapValue(record["payload"])
	return stringOr(payload["agentRunId"], "") == run.AgentRunID &&
		stringOr(payload["tenantId"], "") == run.TenantID &&
		stringOr(payload["userId"], "") == run.UserID &&
		stringOr(payload["workspaceId"], "") == run.WorkspaceID
}

func agentRunCreatePublicEvents(runID string, at time.Time) []AgentRunEvent {
	return []AgentRunEvent{
		{AgentRunID: runID, EventType: "resolving", Status: "resolving_intent", SafeData: map[string]any{"status": "resolving_intent"}, CreatedAt: at},
		{AgentRunID: runID, EventType: "planning", Status: "planning", SafeData: map[string]any{"status": "planning"}, CreatedAt: at},
	}
}

func agentRunCreateEventKey(runID, eventType string) string {
	return "agent-run-create:" + runID + ":" + eventType
}

func agentRunPlanningSuccessEvent(runID, status string, planVersion int) AgentRunEvent {
	return AgentRunEvent{
		AgentRunID: runID, EventType: status, Status: status,
		SafeData:  map[string]any{"status": status, "planVersion": planVersion},
		CreatedAt: time.Now().UTC(),
	}
}

func agentRunPlanningSuccessEventKey(runID, status string, planVersion int) string {
	return "agent-run-planning:" + runID + ":" + status + ":" + fmt.Sprint(planVersion)
}

func (r *AgentRunRepository) createRunAndEnqueuePlanningPostgres(ctx context.Context, command AgentRunCreatePlanningCommand, _ *QueueRepository) (AgentRunRecord, bool, error) {
	record := command.Record
	requestSnapshot, err := json.Marshal(record.RequestSnapshot)
	if err != nil {
		return AgentRunRecord{}, false, fmt.Errorf("AGENT_PLAN_INVALID")
	}
	intentSnapshot, err := json.Marshal(record.IntentSnapshot)
	if err != nil {
		return AgentRunRecord{}, false, fmt.Errorf("AGENT_PLAN_INVALID")
	}
	queueCommand := agentRunPlanningQueueCommand(record)
	queuePayload, err := json.Marshal(queueCommand["payload"])
	if err != nil {
		return AgentRunRecord{}, false, fmt.Errorf("AGENT_PLAN_INVALID")
	}
	planningAttemptSeriesID, err := newQueueAttemptSeriesID()
	if err != nil {
		return AgentRunRecord{}, false, err
	}

	var existing AgentRunRecord
	created := false
	err = r.db.WithSerializableRetry(ctx, "agent run planning create", 8, func(tx *Tx) error {
		// Preserve an existing idempotent Run even after a later Workspace write
		// or thread switch. It is already an immutable historical snapshot and
		// must not be revalidated against the current binding.
		stored, findErr := r.scanRunRow(tx.QueryRowRaw(ctx, agentRunSelect+` where user_id=$1 and idempotency_key=$2 for update`, record.UserID, record.IdempotencyKey))
		if findErr == nil {
			if !CompareRequestHash(stored.RequestHash, record.RequestHash) {
				return fmt.Errorf("IDEMPOTENCY_KEY_CONFLICT")
			}
			if err := r.agentRunPlanningCreateCompleteInTx(ctx, tx, stored); err != nil {
				return err
			}
			existing = stored
			return nil
		}
		if !errors.Is(findErr, pgx.ErrNoRows) {
			return findErr
		}
		if err := validateAgentRunWorkspaceSnapshotInTx(ctx, tx, record); err != nil {
			return err
		}

		tag, insertErr := tx.ExecRaw(ctx, `
insert into agent_runs(agent_run_id,tenant_id,user_id,workspace_id,workspace_version,workspace_binding_version,context_generation,thread_id,task_id,idempotency_key,request_hash,request_snapshot,intent_snapshot,status,routing_mode,source_surface)
values($1,$2,$3,$4,$5,$6,$7,nullif($8,''),nullif($9,''),$10,$11,$12::jsonb,$13::jsonb,$14,$15,nullif($16,''))
on conflict(user_id,idempotency_key) do nothing`,
			record.AgentRunID, record.TenantID, record.UserID, record.WorkspaceID, record.WorkspaceVersion,
			record.BindingVersion, record.ContextGeneration, record.ThreadID, record.TaskID, record.IdempotencyKey,
			record.RequestHash, string(requestSnapshot), string(intentSnapshot), record.Status, record.RoutingMode, record.SourceSurface)
		if insertErr != nil {
			return insertErr
		}
		if tag.RowsAffected() == 0 {
			stored, replayErr := r.scanRunRow(tx.QueryRowRaw(ctx, agentRunSelect+` where user_id=$1 and idempotency_key=$2 for update`, record.UserID, record.IdempotencyKey))
			if replayErr != nil {
				if errors.Is(replayErr, pgx.ErrNoRows) {
					return fmt.Errorf("AGENT_RUN_CREATE_INCOMPLETE")
				}
				return replayErr
			}
			if !CompareRequestHash(stored.RequestHash, record.RequestHash) {
				return fmt.Errorf("IDEMPOTENCY_KEY_CONFLICT")
			}
			if err := r.agentRunPlanningCreateCompleteInTx(ctx, tx, stored); err != nil {
				return err
			}
			existing = stored
			return nil
		}

		queueTag, queueErr := tx.ExecRaw(ctx, `
insert into task_queue_records(queue_id,queue_name,task_type,task_id,dedupe_key,status,priority,payload,attempt_series_id)
values($1,'agent_planning','agent_planning',$2,$3,'pending',50,$4::jsonb,$5)
on conflict(queue_id) do nothing`, "queue_plan_"+record.AgentRunID, record.AgentRunID, "agent_planning:"+record.AgentRunID, string(queuePayload), planningAttemptSeriesID)
		if queueErr != nil {
			return queueErr
		}
		if queueTag.RowsAffected() != 1 {
			return fmt.Errorf("AGENT_RUN_CREATE_INCOMPLETE")
		}
		for _, event := range agentRunCreatePublicEvents(record.AgentRunID, command.CreatedAt) {
			inserted, eventErr := r.AppendPublicEventIdempotentInTx(ctx, tx, event, agentRunCreateEventKey(record.AgentRunID, event.EventType))
			if eventErr != nil {
				return eventErr
			}
			if !inserted {
				return fmt.Errorf("AGENT_RUN_CREATE_INCOMPLETE")
			}
		}
		created = true
		return nil
	})
	if err != nil {
		return AgentRunRecord{}, false, err
	}
	if !created {
		return existing, true, nil
	}
	createdRecord, err := r.getRunPostgres(ctx, record.TenantID, record.UserID, record.AgentRunID)
	if err != nil {
		return AgentRunRecord{}, false, err
	}
	r.NotifyPublicEvent(record.AgentRunID)
	return createdRecord, false, nil
}

// validateAgentRunWorkspaceSnapshotInTx establishes the durable snapshot
// boundary immediately before Run creation and final plan publication. FOR
// SHARE prevents a concurrent Workspace write from advancing the version
// until this serializable transaction has either committed or rolled back; a
// thread switch is checked against the exact binding version.
func validateAgentRunWorkspaceSnapshotInTx(ctx context.Context, tx *Tx, record AgentRunRecord) error {
	if record.ThreadID != "" {
		var threadUserID, activeWorkspaceID string
		var bindingVersion, contextGeneration int64
		err := tx.QueryRowRaw(ctx, `
select user_id,coalesce(active_workspace_id,workspace_id),workspace_binding_version,context_generation
from chat_threads
where thread_id=$1
for share`, record.ThreadID).Scan(&threadUserID, &activeWorkspaceID, &bindingVersion, &contextGeneration)
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("NOT_FOUND")
		}
		if err != nil {
			return err
		}
		if threadUserID != record.UserID || activeWorkspaceID != record.WorkspaceID ||
			bindingVersion != record.BindingVersion || contextGeneration != record.ContextGeneration {
			return fmt.Errorf("THREAD_WORKSPACE_VERSION_CONFLICT")
		}
	}

	var tenantID, userID, status string
	var workspaceVersion int64
	err := tx.QueryRowRaw(ctx, `
select tenant_id,user_id,status,workspace_version
from workspaces
where workspace_id=$1
for share`, record.WorkspaceID).Scan(&tenantID, &userID, &status, &workspaceVersion)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("NOT_FOUND")
	}
	if err != nil {
		return err
	}
	if tenantID != record.TenantID || userID != record.UserID {
		return fmt.Errorf("NOT_FOUND")
	}
	if status != "ready" {
		return fmt.Errorf("WORKSPACE_NOT_READY")
	}
	if workspaceVersion != record.WorkspaceVersion {
		return fmt.Errorf("WORKSPACE_VERSION_CONFLICT")
	}
	return nil
}

func (r *AgentRunRepository) agentRunPlanningCreateCompleteInTx(ctx context.Context, tx *Tx, record AgentRunRecord) error {
	var queueName, taskType, taskID, dedupeKey string
	var payloadRaw []byte
	err := tx.QueryRowRaw(ctx, `
select queue_name,task_type,task_id,coalesce(dedupe_key,''),payload
from task_queue_records
where queue_id=$1
for update`, "queue_plan_"+record.AgentRunID).Scan(&queueName, &taskType, &taskID, &dedupeKey, &payloadRaw)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("AGENT_RUN_CREATE_INCOMPLETE")
	}
	if err != nil {
		return err
	}
	if !agentRunPlanningQueueMatches(map[string]any{
		"queueId": "queue_plan_" + record.AgentRunID, "queueName": queueName, "taskType": taskType,
		"taskId": taskID, "dedupeKey": dedupeKey, "payload": jsonMap(payloadRaw),
	}, record) {
		return fmt.Errorf("AGENT_RUN_CREATE_INCOMPLETE")
	}
	for _, expected := range agentRunCreatePublicEvents(record.AgentRunID, time.Time{}) {
		var eventType string
		var safePayload []byte
		eventErr := tx.QueryRowRaw(ctx, `
select event_type,safe_payload
from runtime_run_events
where runtime_run_event_id=$1`, stableAgentRunEventID(record.AgentRunID, agentRunCreateEventKey(record.AgentRunID, expected.EventType))).Scan(&eventType, &safePayload)
		if errors.Is(eventErr, pgx.ErrNoRows) {
			return fmt.Errorf("AGENT_RUN_CREATE_INCOMPLETE")
		}
		if eventErr != nil || eventType != expected.EventType || stringOr(jsonMap(safePayload)["status"], "") != expected.Status {
			return fmt.Errorf("AGENT_RUN_CREATE_INCOMPLETE")
		}
	}
	return nil
}

func (r *AgentRunRepository) createRunAndEnqueuePlanningMemory(command AgentRunCreatePlanningCommand, queueRepo *QueueRepository) (AgentRunRecord, bool, error) {
	if r == nil || queueRepo == nil || !queueRepo.queueMemoryAllowed() {
		return AgentRunRecord{}, false, fmt.Errorf("AGENT_RUN_PLANNING_UNAVAILABLE")
	}
	seriesID, err := newQueueAttemptSeriesID()
	if err != nil {
		return AgentRunRecord{}, false, err
	}
	record := command.Record
	queueCommand := agentRunPlanningQueueCommand(record)
	r.mu.Lock()
	queueRepo.mu.Lock()
	var result AgentRunRecord
	var existing bool
	var notify bool
	func() {
		defer queueRepo.mu.Unlock()
		defer r.mu.Unlock()
		key := record.UserID + "\x00" + record.IdempotencyKey
		if runID := r.idempotency[key]; runID != "" {
			stored := r.runs[runID]
			if !CompareRequestHash(stored.RequestHash, record.RequestHash) {
				err = fmt.Errorf("IDEMPOTENCY_KEY_CONFLICT")
				return
			}
			if !agentRunPlanningQueueMatches(queueRepo.records["queue_plan_"+stored.AgentRunID], stored) || !agentRunCreateEventsPresentMemory(r, stored.AgentRunID) {
				err = fmt.Errorf("AGENT_RUN_CREATE_INCOMPLETE")
				return
			}
			result, existing = copyAgentRun(stored), true
			return
		}
		queueID := stringOr(queueCommand["queueId"], "")
		if queueID == "" || queueRepo.records[queueID] != nil || len(r.events[record.AgentRunID]) != 0 {
			err = fmt.Errorf("AGENT_RUN_CREATE_INCOMPLETE")
			return
		}
		now := command.CreatedAt
		record.CreatedAt, record.UpdatedAt = now, now
		record.RequestSnapshot = copyMap(record.RequestSnapshot)
		record.IntentSnapshot = copyMap(record.IntentSnapshot)
		record.PlanSnapshot = map[string]any{}
		record.PublicResult = map[string]any{}
		record.ErrorSummary = map[string]any{}
		r.runs[record.AgentRunID] = copyAgentRun(record)
		r.idempotency[key] = record.AgentRunID
		queueRepo.records[queueID] = agentRunMemoryQueueRecord(queueCommand, seriesID, now)
		for index, event := range agentRunCreatePublicEvents(record.AgentRunID, now) {
			event.Sequence = int64(index + 1)
			event.SafeData = sanitizeAgentEventMap(event.SafeData, 0)
			r.events[record.AgentRunID] = append(r.events[record.AgentRunID], event)
			r.eventIdempotency[record.AgentRunID+"\x00"+agentRunCreateEventKey(record.AgentRunID, event.EventType)] = event
		}
		result, notify = copyAgentRun(record), true
	}()
	if err != nil {
		return AgentRunRecord{}, false, err
	}
	if notify {
		r.NotifyPublicEvent(record.AgentRunID)
	}
	return result, existing, nil
}

func agentRunCreateEventsPresentMemory(r *AgentRunRepository, runID string) bool {
	for _, expected := range agentRunCreatePublicEvents(runID, time.Time{}) {
		stored, ok := r.eventIdempotency[runID+"\x00"+agentRunCreateEventKey(runID, expected.EventType)]
		if !ok || stored.EventType != expected.EventType || stored.Status != expected.Status || stringOr(stored.SafeData["status"], "") != expected.Status {
			return false
		}
	}
	return true
}

func agentRunMemoryQueueRecord(command map[string]any, attemptSeriesID string, now time.Time) QueueRecord {
	payload := copyMap(mapValue(command["payload"]))
	return QueueRecord{
		"queueId": stringOr(command["queueId"], ""), "queueName": stringOr(command["queueName"], ""),
		"taskType": stringOr(command["taskType"], ""), "taskId": stringOr(command["taskId"], ""),
		"dedupeKey": stringOr(command["dedupeKey"], ""), "status": "pending", "priority": defaultInt(command["priority"], 100),
		"attempt": 0, "maxAttempts": defaultInt(command["maxAttempts"], 3), "attemptSeriesId": attemptSeriesID,
		"leaseFencingToken": int64(0), "availableAt": now.Format(time.RFC3339Nano), "payload": payload,
		"errorSummary": map[string]any{}, "createdAt": now.Format(time.RFC3339Nano), "updatedAt": now.Format(time.RFC3339Nano), "storage": "memory",
	}
}

func (r *AgentRunRepository) finalizePlanningSuccessPostgres(ctx context.Context, command AgentRunPlanningSuccessCommand) error {
	plan := copyMap(command.Plan.Plan)
	plan["status"] = command.Plan.PlanStatus
	rawPlan, err := json.Marshal(plan)
	if err != nil {
		return fmt.Errorf("AGENT_PLAN_INVALID")
	}
	selectedSkills, _ := json.Marshal(plan["selectedSkillProfiles"])
	knowledge, _ := json.Marshal(plan["selectedKnowledgeRefs"])
	tools, _ := json.Marshal(plan["requiredTools"])
	output, _ := json.Marshal(plan["outputContract"])
	lane := ""
	if command.Plan.AgentRunStatus == "queued" {
		lane, err = agentRunPlanningRuntimeLane(plan)
		if err != nil {
			return err
		}
	}

	err = r.db.WithSerializableRetry(ctx, "agent planning success", 3, func(tx *Tx) error {
		if err := lockAgentRunPlanningQueueLeaseInTx(ctx, tx, command.QueueProof, command.Plan.AgentRunID); err != nil {
			return err
		}
		run, runErr := r.scanRunRow(tx.QueryRowRaw(ctx, agentRunSelect+` where agent_run_id=$1 for update`, command.Plan.AgentRunID))
		if errors.Is(runErr, pgx.ErrNoRows) {
			return fmt.Errorf("NOT_FOUND")
		}
		if runErr != nil {
			return runErr
		}
		if run.Status != "planning" {
			return fmt.Errorf("AGENT_PLAN_EXPIRED")
		}
		frozen, frozenErr := scanRunWorkspaceContextRow(tx.QueryRowRaw(ctx, runWorkspaceContextSelect+` where run_id=$1 for update`, command.Plan.AgentRunID))
		if errors.Is(frozenErr, pgx.ErrNoRows) {
			return fmt.Errorf("WORKSPACE_VERSION_CONFLICT")
		}
		if frozenErr != nil {
			return frozenErr
		}
		if err := validateFrozenWorkspaceContextForPlan(run, frozen, plan); err != nil {
			return fmt.Errorf("WORKSPACE_VERSION_CONFLICT")
		}
		// Freeze and plan publication are one immutable boundary. The frozen
		// context proves the manifest contents; these locks prove that the
		// Thread binding and Workspace version have not moved before the plan,
		// Runtime queue, event, and Run status become visible together.
		if err := validateAgentRunWorkspaceSnapshotInTx(ctx, tx, run); err != nil {
			return err
		}
		planTag, planErr := tx.ExecRaw(ctx, `
insert into agent_run_plans(agent_run_plan_id,agent_run_id,plan_version,status,task_type,l1_agent_profile,selected_skills,selected_knowledge_refs,required_tools,output_contract,workspace_version,index_version,manifest_version,capability_hash,safe_plan_summary)
values($1,$2,$3,$4,$5,$6,$7::jsonb,$8::jsonb,$9::jsonb,$10::jsonb,$11,$12,$13,$14,$15)
on conflict(agent_run_id,plan_version) do nothing`,
			fmt.Sprintf("agent_plan_%s_%d", command.Plan.AgentRunID, command.Plan.PlanVersion), command.Plan.AgentRunID,
			command.Plan.PlanVersion, command.Plan.PlanStatus, fmt.Sprint(plan["taskType"]), fmt.Sprint(plan["l1AgentProfile"]),
			string(selectedSkills), string(knowledge), string(tools), string(output), int64Value(plan["workspaceVersion"]),
			int64Value(plan["indexVersion"]), fmt.Sprint(plan["manifestVersion"]), fmt.Sprint(plan["capabilityHash"]), fmt.Sprint(plan["safePlanSummary"]),
		)
		if planErr != nil {
			return planErr
		}
		if planTag.RowsAffected() != 1 {
			return fmt.Errorf("AGENT_PLANNING_COMMIT_INCOMPLETE")
		}
		runTag, updateErr := tx.ExecRaw(ctx, `
update agent_runs
set plan_snapshot=$1::jsonb,status=$2,updated_at=now()
where agent_run_id=$3 and status='planning'`, string(rawPlan), command.Plan.AgentRunStatus, command.Plan.AgentRunID)
		if updateErr != nil {
			return updateErr
		}
		if runTag.RowsAffected() != 1 {
			return fmt.Errorf("AGENT_PLAN_EXPIRED")
		}
		if lane != "" {
			if err := insertAgentRunPlanningRuntimeQueueInTx(ctx, tx, command.Plan.AgentRunID, command.Plan.PlanVersion, lane); err != nil {
				return err
			}
		}
		event := agentRunPlanningSuccessEvent(command.Plan.AgentRunID, command.Plan.AgentRunStatus, command.Plan.PlanVersion)
		inserted, eventErr := r.AppendPublicEventIdempotentInTx(ctx, tx, event, agentRunPlanningSuccessEventKey(command.Plan.AgentRunID, command.Plan.AgentRunStatus, command.Plan.PlanVersion))
		if eventErr != nil {
			return eventErr
		}
		if !inserted {
			return fmt.Errorf("AGENT_PLANNING_COMMIT_INCOMPLETE")
		}
		completeTag, completeErr := tx.ExecRaw(ctx, `
update task_queue_records
set status='succeeded',lease_owner=null,lease_token_hash=null,lease_expires_at=null,error_summary='{}'::jsonb,updated_at=now()
where queue_id=$1 and queue_name='agent_planning' and task_type='agent_planning' and task_id=$2
  and status in ('leased','running') and lease_owner=$3 and attempt=$4 and lease_token_hash=$5
  and lease_fencing_token=$6 and lease_expires_at=$7 and lease_expires_at > now()`,
			command.QueueProof.QueueID, command.Plan.AgentRunID, command.QueueProof.WorkerID, command.QueueProof.Attempt,
			command.QueueProof.TokenHash, command.QueueProof.FencingToken, command.QueueProof.LeaseExpiresAt)
		if completeErr != nil {
			return completeErr
		}
		if completeTag.RowsAffected() != 1 {
			return ErrStaleQueueLease
		}
		return nil
	})
	if err == nil {
		r.NotifyPublicEvent(command.Plan.AgentRunID)
	}
	return err
}

func lockAgentRunPlanningQueueLeaseInTx(ctx context.Context, tx *Tx, proof QueueLeaseProof, runID string) error {
	var taskID, queueName, taskType, status, owner, token string
	var attempt int
	var fencingToken int64
	var leaseExpiresAt time.Time
	err := tx.QueryRowRaw(ctx, `
select task_id,queue_name,task_type,status,coalesce(lease_owner,''),attempt,
       coalesce(lease_token_hash,''),lease_fencing_token,lease_expires_at
from task_queue_records
where queue_id=$1
for update`, proof.QueueID).Scan(&taskID, &queueName, &taskType, &status, &owner, &attempt, &token, &fencingToken, &leaseExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrStaleQueueLease
	}
	if err != nil {
		return err
	}
	if queueName != "agent_planning" || taskType != "agent_planning" || taskID != runID ||
		(status != "leased" && status != "running") || owner != proof.WorkerID || attempt != proof.Attempt ||
		token != proof.TokenHash || fencingToken != proof.FencingToken || !leaseExpiresAt.Equal(proof.LeaseExpiresAt) ||
		!leaseExpiresAt.After(time.Now().UTC()) {
		return ErrStaleQueueLease
	}
	return nil
}

func agentRunPlanningRuntimeLane(plan map[string]any) (string, error) {
	taskType := strings.TrimSpace(fmt.Sprint(plan["taskType"]))
	executionScope := strings.TrimSpace(fmt.Sprint(plan["executionScope"]))
	if taskType == "" || executionScope == "" {
		return "", fmt.Errorf("AGENT_PLAN_INVALID")
	}
	if taskType == "minutes_generation" || taskType == "summary_generation" || taskType == "material_deposit_generation" {
		return "ai_runtime_recording", nil
	}
	if executionScope != "product_thread" {
		return "ai_runtime_background", nil
	}
	return "ai_runtime_interactive", nil
}

func insertAgentRunPlanningRuntimeQueueInTx(ctx context.Context, tx *Tx, runID string, planVersion int, lane string) error {
	payload, err := json.Marshal(map[string]any{"agentRunId": runID, "planVersion": planVersion, "lane": lane})
	if err != nil {
		return fmt.Errorf("AGENT_PLAN_INVALID")
	}
	attemptSeriesID, err := newQueueAttemptSeriesID()
	if err != nil {
		return err
	}
	tag, err := tx.ExecRaw(ctx, `
insert into task_queue_records(queue_id,queue_name,task_type,task_id,dedupe_key,status,priority,max_attempts,payload,attempt_series_id)
values($1,$2,'runtime_dispatch',$3,$4,'pending',100,$5,$6::jsonb,$7)
on conflict(queue_id) do nothing`, lane+":"+runID, lane, runID, fmt.Sprintf("runtime_dispatch:%s:%d", runID, planVersion), agentRunRuntimeCapacityQueueMaxAttempts, string(payload), attemptSeriesID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("AGENT_PLANNING_COMMIT_INCOMPLETE")
	}
	return nil
}

func (r *AgentRunRepository) finalizePlanningSuccessMemory(command AgentRunPlanningSuccessCommand, queueRepo *QueueRepository) error {
	if r == nil || queueRepo == nil || !queueRepo.queueMemoryAllowed() {
		return fmt.Errorf("AGENT_RUN_PLANNING_UNAVAILABLE")
	}
	plan := copyMap(command.Plan.Plan)
	plan["status"] = command.Plan.PlanStatus
	lane := ""
	var err error
	if command.Plan.AgentRunStatus == "queued" {
		lane, err = agentRunPlanningRuntimeLane(plan)
		if err != nil {
			return err
		}
	}
	seriesID := ""
	if lane != "" {
		seriesID, err = newQueueAttemptSeriesID()
		if err != nil {
			return err
		}
	}

	r.mu.Lock()
	queueRepo.mu.Lock()
	var notify bool
	func() {
		defer queueRepo.mu.Unlock()
		defer r.mu.Unlock()
		run := r.runs[command.Plan.AgentRunID]
		if run.AgentRunID == "" {
			err = fmt.Errorf("NOT_FOUND")
			return
		}
		if run.Status != "planning" {
			err = fmt.Errorf("AGENT_PLAN_EXPIRED")
			return
		}
		planningQueue := queueRepo.records[command.QueueProof.QueueID]
		if !queueMemoryProofMatches(planningQueue, command.QueueProof, time.Now().UTC(), "leased", "running") ||
			stringOr(planningQueue["queueName"], "") != "agent_planning" || stringOr(planningQueue["taskType"], "") != "agent_planning" ||
			stringOr(planningQueue["taskId"], "") != command.Plan.AgentRunID {
			err = ErrStaleQueueLease
			return
		}
		frozen, found := r.workspaceContexts[command.Plan.AgentRunID]
		if !found || validateFrozenWorkspaceContextForPlan(run, frozen, plan) != nil {
			err = fmt.Errorf("WORKSPACE_VERSION_CONFLICT")
			return
		}
		if r.plans[command.Plan.AgentRunID] != nil && r.plans[command.Plan.AgentRunID][command.Plan.PlanVersion] != nil {
			err = fmt.Errorf("AGENT_PLANNING_COMMIT_INCOMPLETE")
			return
		}
		if lane != "" && queueRepo.records[lane+":"+command.Plan.AgentRunID] != nil {
			err = fmt.Errorf("AGENT_PLANNING_COMMIT_INCOMPLETE")
			return
		}
		eventKey := command.Plan.AgentRunID + "\x00" + agentRunPlanningSuccessEventKey(command.Plan.AgentRunID, command.Plan.AgentRunStatus, command.Plan.PlanVersion)
		if _, exists := r.eventIdempotency[eventKey]; exists {
			err = fmt.Errorf("AGENT_PLANNING_COMMIT_INCOMPLETE")
			return
		}
		if r.plans[command.Plan.AgentRunID] == nil {
			r.plans[command.Plan.AgentRunID] = map[int]map[string]any{}
		}
		r.plans[command.Plan.AgentRunID][command.Plan.PlanVersion] = copyMap(plan)
		run.PlanSnapshot = copyMap(plan)
		run.Status = command.Plan.AgentRunStatus
		run.UpdatedAt = time.Now().UTC()
		r.runs[command.Plan.AgentRunID] = run
		if lane != "" {
			queueRepo.records[lane+":"+command.Plan.AgentRunID] = agentRunMemoryQueueRecord(map[string]any{
				"queueId": lane + ":" + command.Plan.AgentRunID, "queueName": lane, "taskType": "runtime_dispatch",
				"taskId": command.Plan.AgentRunID, "dedupeKey": fmt.Sprintf("runtime_dispatch:%s:%d", command.Plan.AgentRunID, command.Plan.PlanVersion),
				"priority": 100, "maxAttempts": agentRunRuntimeCapacityQueueMaxAttempts, "payload": map[string]any{"agentRunId": command.Plan.AgentRunID, "planVersion": command.Plan.PlanVersion, "lane": lane},
			}, seriesID, time.Now().UTC())
		}
		planningQueue["status"] = "succeeded"
		planningQueue["errorSummary"] = map[string]any{}
		planningQueue["updatedAt"] = time.Now().UTC().Format(time.RFC3339Nano)
		delete(planningQueue, "leaseOwner")
		delete(planningQueue, "leaseTokenHash")
		delete(planningQueue, "leaseExpiresAt")
		event := agentRunPlanningSuccessEvent(command.Plan.AgentRunID, command.Plan.AgentRunStatus, command.Plan.PlanVersion)
		event.Sequence = int64(len(r.events[command.Plan.AgentRunID]) + 1)
		event.SafeData = sanitizeAgentEventMap(event.SafeData, 0)
		r.events[command.Plan.AgentRunID] = append(r.events[command.Plan.AgentRunID], event)
		r.eventIdempotency[eventKey] = event
		notify = true
	}()
	if err != nil {
		return err
	}
	if notify {
		r.NotifyPublicEvent(command.Plan.AgentRunID)
	}
	return nil
}
