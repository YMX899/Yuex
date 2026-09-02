package persistence

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// ConfirmPlanAndEnqueueWithAdmission is the durable confirmation boundary.
// A successful return means that the owner-locked Run/Plan, AiTask,
// permission evidence, quota reservation, runtime queue and queued event
// committed together. It deliberately refuses a configured datastore that
// cannot prove a single PostgreSQL transaction boundary.
func (r *AgentRunRepository) ConfirmPlanAndEnqueueWithAdmission(ctx context.Context, command AgentRunConfirmationCommand, usage *UsageRepository, tasks *ChatTaskRepository, queueRepo *QueueRepository) (AgentRunConfirmationResult, error) {
	command.AgentRunID = strings.TrimSpace(command.AgentRunID)
	command.TenantID = strings.TrimSpace(command.TenantID)
	command.UserID = strings.TrimSpace(command.UserID)
	command.IdempotencyKey = strings.TrimSpace(command.IdempotencyKey)
	command.RuntimeLane = strings.TrimSpace(command.RuntimeLane)
	if command.AgentRunID == "" || command.TenantID == "" || command.UserID == "" || command.PlanVersion < 1 || command.IdempotencyKey == "" || usage == nil || tasks == nil || queueRepo == nil {
		return AgentRunConfirmationResult{}, fmt.Errorf("AGENT_PLAN_INVALID")
	}
	if command.ConfirmedAt.IsZero() {
		command.ConfirmedAt = time.Now().UTC()
	}
	if r.confirmAdmissionPostgresReady(usage, tasks, queueRepo) {
		return r.confirmPlanAndEnqueueWithAdmissionPostgres(ctx, command, usage)
	}
	if r != nil && r.db != nil && !r.db.Disabled && r.db.Pool != nil {
		return AgentRunConfirmationResult{}, fmt.Errorf("AGENT_RUN_ADMISSION_UNAVAILABLE")
	}
	return r.confirmPlanAndEnqueueWithAdmissionMemory(ctx, command, usage, tasks, queueRepo)
}

func (r *AgentRunRepository) confirmAdmissionPostgresReady(usage *UsageRepository, tasks *ChatTaskRepository, queueRepo *QueueRepository) bool {
	if r == nil || r.db == nil || r.db.Disabled || r.db.Pool == nil || usage == nil || tasks == nil || queueRepo == nil ||
		usage.db == nil || usage.db.Disabled || usage.db.Pool == nil || tasks.db == nil || tasks.db.Disabled || tasks.db.Pool == nil ||
		queueRepo.db == nil || queueRepo.db.Disabled || queueRepo.db.Pool == nil {
		return false
	}
	return sameDatabaseTransactionBoundary(r.db, usage.db) && sameDatabaseTransactionBoundary(r.db, tasks.db) && sameDatabaseTransactionBoundary(r.db, queueRepo.db)
}

func sameDatabaseTransactionBoundary(left, right *Database) bool {
	return left != nil && right != nil && left.DSN != "" && left.DSN == right.DSN
}

func agentRunConfirmationTaskMatches(command AgentRunConfirmationCommand, userID, workspaceID, threadID, storedTaskType, status string, input map[string]any, expectedWorkspaceID, expectedThreadID, expectedTaskType string, requireAgentRunLink bool) bool {
	if userID != command.UserID || workspaceID != expectedWorkspaceID || storedTaskType != expectedTaskType || storedTaskType == "" || threadID != expectedThreadID {
		return false
	}
	if requireAgentRunLink {
		if !agentRunConfirmedTaskStatus(status) {
			return false
		}
	} else if !agentRunConfirmableTaskStatus(status) {
		return false
	}
	if declaredThreadID := strings.TrimSpace(stringOr(input["threadId"], "")); declaredThreadID != "" && declaredThreadID != expectedThreadID {
		return false
	}
	if declaredTaskType := strings.TrimSpace(stringOr(input["taskType"], "")); declaredTaskType != "" && declaredTaskType != expectedTaskType {
		return false
	}
	linkedRunID := strings.TrimSpace(stringOr(input["agentRunId"], ""))
	if linkedRunID != "" && linkedRunID != command.AgentRunID {
		return false
	}
	return !requireAgentRunLink || linkedRunID == command.AgentRunID
}

func agentRunConfirmationQueuePayload(command AgentRunConfirmationCommand, lane string) map[string]any {
	return map[string]any{"agentRunId": command.AgentRunID, "planVersion": command.PlanVersion, "lane": lane}
}

func agentRunConfirmationQueueMatches(command AgentRunConfirmationCommand, lane string, record map[string]any) bool {
	if stringOr(record["queueId"], "") != lane+":"+command.AgentRunID ||
		stringOr(record["queueName"], "") != lane ||
		stringOr(record["taskType"], "") != "runtime_dispatch" ||
		stringOr(record["taskId"], "") != command.AgentRunID ||
		stringOr(record["dedupeKey"], "") != fmt.Sprintf("runtime_dispatch:%s:%d", command.AgentRunID, command.PlanVersion) ||
		!agentRunConfirmationLiveQueueStatus(stringOr(record["status"], "")) {
		return false
	}
	payload := mapValue(record["payload"])
	return stringOr(payload["agentRunId"], "") == command.AgentRunID &&
		intValue(payload["planVersion"]) == command.PlanVersion &&
		stringOr(payload["lane"], "") == lane
}

func agentRunConfirmationLiveQueueStatus(status string) bool {
	return stringIn(status, []string{"pending", "retry_wait", "leased", "running"})
}

func agentRunConfirmationEventKey(command AgentRunConfirmationCommand) string {
	return "agent-run-confirm:" + command.AgentRunID + ":" + fmt.Sprint(command.PlanVersion)
}

func agentRunConfirmationPublicEventMatches(command AgentRunConfirmationCommand, event AgentRunEvent) bool {
	return event.AgentRunID == command.AgentRunID &&
		event.EventType == "queued" &&
		event.Status == "queued" &&
		stringOr(event.SafeData["status"], "") == "queued" &&
		intValue(event.SafeData["planVersion"]) == command.PlanVersion
}

func (r *AgentRunRepository) confirmPlanAndEnqueueWithAdmissionPostgres(ctx context.Context, command AgentRunConfirmationCommand, usage *UsageRepository) (AgentRunConfirmationResult, error) {
	var result AgentRunConfirmationResult
	var denied bool
	var denialCode string
	confirmationKeyHash := agentRunConfirmationKeyHash(command.IdempotencyKey)
	err := r.db.WithSerializableRetry(ctx, "agent_run_confirm_admission", 3, func(tx *Tx) error {
		result = AgentRunConfirmationResult{}
		denied = false
		denialCode = ""
		rows, err := tx.Query(ctx, `
select ar.status as run_status,
       coalesce(ar.task_id, '') as task_id,
       ar.workspace_id,
	   coalesce(ar.thread_id, '') as thread_id,
       p.status as plan_status,
       p.task_type,
	   coalesce(p.confirmation_key_hash, '') as confirmation_key_hash,
       ar.plan_snapshot
from agent_runs ar
join agent_run_plans p on p.agent_run_id = ar.agent_run_id and p.plan_version = @version
where ar.agent_run_id = @run and ar.tenant_id = @tenant and ar.user_id = @user
for update of ar, p`, map[string]any{"run": command.AgentRunID, "tenant": command.TenantID, "user": command.UserID, "version": command.PlanVersion})
		if err != nil {
			return err
		}
		if len(rows) != 1 {
			return fmt.Errorf("NOT_FOUND")
		}
		runStatus := stringOr(rows[0]["run_status"], "")
		planStatus := stringOr(rows[0]["plan_status"], "")
		taskID := stringOr(rows[0]["task_id"], "")
		workspaceID := stringOr(rows[0]["workspace_id"], "")
		threadID := stringOr(rows[0]["thread_id"], "")
		taskType := stringOr(rows[0]["task_type"], "")
		storedConfirmationKeyHash := stringOr(rows[0]["confirmation_key_hash"], "")
		planSnapshot := mapFromAgentRunValue(rows[0]["plan_snapshot"])
		if taskType == "" || stringOr(planSnapshot["taskType"], "") != taskType {
			return fmt.Errorf("AGENT_PLAN_INVALID")
		}
		expectedLane := agentRunRuntimeLane(taskType, stringOr(planSnapshot["executionScope"], ""))
		if command.RuntimeLane != "" && command.RuntimeLane != expectedLane {
			return fmt.Errorf("AGENT_PLAN_INVALID")
		}
		command.RuntimeLane = expectedLane
		if runStatus == "queued" && planStatus == "confirmed" {
			if storedConfirmationKeyHash == "" {
				return fmt.Errorf("AGENT_RUN_ADMISSION_UNAVAILABLE")
			}
			if !agentRunConfirmationKeyEqual(storedConfirmationKeyHash, confirmationKeyHash) {
				return fmt.Errorf("IDEMPOTENCY_KEY_CONFLICT")
			}
			if err := r.validateConfirmedAdmissionInTx(ctx, tx, usage, command, taskID, workspaceID, threadID, taskType, command.RuntimeLane); err != nil {
				return err
			}
			result = AgentRunConfirmationResult{TaskID: taskID, Replayed: true}
			return nil
		}
		if runStatus != "awaiting_confirmation" || planStatus != "awaiting_confirmation" {
			return fmt.Errorf("AGENT_PLAN_EXPIRED")
		}

		inputSnapshot := map[string]any{}
		createdTask := taskID == ""
		if createdTask {
			taskID = agentRunConfirmationTaskID(command.AgentRunID)
		} else {
			var taskUserID, taskWorkspaceID, taskThreadID, storedTaskType, taskStatus string
			var inputRaw []byte
			if err := tx.QueryRowRaw(ctx, `
select user_id, workspace_id, coalesce(thread_id, ''), task_type, status, input_snapshot
from ai_tasks
where task_id = $1
for update`, taskID).Scan(&taskUserID, &taskWorkspaceID, &taskThreadID, &storedTaskType, &taskStatus, &inputRaw); err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					return fmt.Errorf("NOT_FOUND")
				}
				return err
			}
			inputSnapshot = jsonMap(inputRaw)
			if !agentRunConfirmationTaskMatches(command, taskUserID, taskWorkspaceID, taskThreadID, storedTaskType, taskStatus, inputSnapshot, workspaceID, threadID, taskType, false) {
				return fmt.Errorf("AGENT_PLAN_INVALID")
			}
		}

		admission, admitted, admissionErr := r.confirmationAdmissionInTx(ctx, tx, usage, command, taskID, workspaceID, taskType, inputSnapshot)
		if admissionErr != nil {
			return admissionErr
		}
		if !admitted {
			denied = true
			denialCode = admission.DenyReason
			result.PermissionCheckID = admission.PermissionCheckID
			return nil
		}

		inputSnapshot["agentRunId"] = command.AgentRunID
		inputSnapshot["permissionCheckId"] = admission.PermissionCheckID
		inputSnapshot["reservationId"] = admission.ReservationID
		inputSnapshot["quotaReservation"] = map[string]any{"reservationId": admission.ReservationID, "status": "reserved"}
		inputSnapshot["taskType"] = taskType
		inputSnapshot["threadId"] = threadID
		if createdTask {
			createdRows, err := tx.Query(ctx, `
insert into ai_tasks(task_id, task_type, user_id, workspace_id, thread_id, status, attempt, input_snapshot)
values (@task, @taskType, @user, @workspace, nullif(@thread, ''), 'queued', 1, @input::jsonb)
on conflict (task_id) do nothing
returning task_id`, map[string]any{"task": taskID, "taskType": taskType, "user": command.UserID, "workspace": workspaceID, "thread": threadID, "input": jsonString(inputSnapshot)})
			if err != nil {
				return err
			}
			if len(createdRows) > 1 {
				return fmt.Errorf("AGENT_RUN_ADMISSION_UNAVAILABLE")
			}
			var createdUserID, createdWorkspaceID, createdThreadID, createdTaskType, createdTaskStatus string
			var createdInputRaw []byte
			if err := tx.QueryRowRaw(ctx, `select user_id, workspace_id, coalesce(thread_id, ''), task_type, status, input_snapshot from ai_tasks where task_id=$1 for update`, taskID).Scan(&createdUserID, &createdWorkspaceID, &createdThreadID, &createdTaskType, &createdTaskStatus, &createdInputRaw); err != nil {
				return err
			}
			if !agentRunConfirmationTaskMatches(command, createdUserID, createdWorkspaceID, createdThreadID, createdTaskType, createdTaskStatus, jsonMap(createdInputRaw), workspaceID, threadID, taskType, len(createdRows) == 0) {
				return fmt.Errorf("AGENT_PLAN_INVALID")
			}
			if err := tx.Exec(ctx, `update ai_tasks set input_snapshot=coalesce(input_snapshot,'{}'::jsonb)||@input::jsonb,updated_at=now() where task_id=@task`, map[string]any{"task": taskID, "input": jsonString(inputSnapshot)}); err != nil {
				return err
			}
			if err := r.appendConfirmationTaskQueuedEventsInTx(ctx, tx, command, taskID, threadID); err != nil {
				return err
			}
			if err := tx.Exec(ctx, `update agent_runs set task_id=@task,updated_at=now() where agent_run_id=@run and status='awaiting_confirmation' and coalesce(task_id,'')=''`, map[string]any{"task": taskID, "run": command.AgentRunID}); err != nil {
				return err
			}
		} else if err := tx.Exec(ctx, `update ai_tasks set input_snapshot=coalesce(input_snapshot,'{}'::jsonb)||@input::jsonb,updated_at=now() where task_id=@task`, map[string]any{"task": taskID, "input": jsonString(inputSnapshot)}); err != nil {
			return err
		}

		if err := tx.Exec(ctx, `update agent_run_plans set status='confirmed',confirmation_key_hash=@keyHash,confirmed_at=@confirmedAt,updated_at=now() where agent_run_id=@run and plan_version=@version and status='awaiting_confirmation'`, map[string]any{"run": command.AgentRunID, "version": command.PlanVersion, "keyHash": confirmationKeyHash, "confirmedAt": command.ConfirmedAt}); err != nil {
			return err
		}
		if err := tx.Exec(ctx, `update agent_runs set status='queued',updated_at=now() where agent_run_id=@run and status='awaiting_confirmation'`, map[string]any{"run": command.AgentRunID}); err != nil {
			return err
		}
		payload, _ := json.Marshal(agentRunConfirmationQueuePayload(command, command.RuntimeLane))
		queuedRows, err := tx.Query(ctx, `
insert into task_queue_records(queue_id,queue_name,task_type,task_id,dedupe_key,status,priority,max_attempts,available_at,payload,attempt_series_id)
values(@queueId,@lane,'runtime_dispatch',@run,@dedupe,'pending',100,5,now(),@payload::jsonb,default)
on conflict(queue_id) do nothing
returning queue_id`, map[string]any{
			"queueId": command.RuntimeLane + ":" + command.AgentRunID,
			"lane":    command.RuntimeLane,
			"run":     command.AgentRunID,
			"dedupe":  fmt.Sprintf("runtime_dispatch:%s:%d", command.AgentRunID, command.PlanVersion),
			"payload": string(payload),
		})
		if err != nil {
			return err
		}
		if len(queuedRows) != 1 {
			return fmt.Errorf("AGENT_RUN_ADMISSION_UNAVAILABLE")
		}
		inserted, err := r.AppendPublicEventIdempotentInTx(ctx, tx, AgentRunEvent{
			AgentRunID: command.AgentRunID,
			EventType:  "queued",
			Status:     "queued",
			SafeData:   map[string]any{"status": "queued", "planVersion": command.PlanVersion},
			CreatedAt:  command.ConfirmedAt,
		}, agentRunConfirmationEventKey(command))
		if err != nil {
			return err
		}
		if !inserted {
			return fmt.Errorf("AGENT_RUN_ADMISSION_UNAVAILABLE")
		}
		if err := r.validateConfirmedAdmissionInTx(ctx, tx, usage, command, taskID, workspaceID, threadID, taskType, command.RuntimeLane); err != nil {
			return err
		}
		result = AgentRunConfirmationResult{TaskID: taskID, PermissionCheckID: admission.PermissionCheckID, ReservationID: admission.ReservationID, Replayed: !inserted}
		return nil
	})
	if err != nil {
		return AgentRunConfirmationResult{}, err
	}
	if denied {
		if denialCode == "" {
			denialCode = "QUOTA_INSUFFICIENT"
		}
		return AgentRunConfirmationResult{}, fmt.Errorf("%s", denialCode)
	}
	if !result.Replayed {
		r.NotifyPublicEvent(command.AgentRunID)
	}
	return result, nil
}

func (r *AgentRunRepository) confirmationAdmissionInTx(ctx context.Context, tx *Tx, usage *UsageRepository, command AgentRunConfirmationCommand, taskID, workspaceID, taskType string, input map[string]any) (QuotaAdmissionReservation, bool, error) {
	if reservationID := agentRunConfirmationReservationID(input); reservationID != "" {
		admission, err := r.existingConfirmationAdmissionInTx(ctx, tx, usage, command, taskID, workspaceID, taskType, reservationID, stringOr(input["permissionCheckId"], ""))
		return admission, err == nil, err
	}
	permissionCheckID := agentRunConfirmationPermissionCheckIDForKey(command.AgentRunID, command.PlanVersion, agentRunConfirmationKeyHash(command.IdempotencyKey))
	admission, err := usage.AdmitAndReserveInTx(ctx, tx, QuotaAdmissionReservationCommand{
		PermissionCheckID: permissionCheckID,
		TraceID:           "agent-run-confirm:" + command.AgentRunID + ":" + fmt.Sprint(command.PlanVersion),
		UserID:            command.UserID,
		WorkspaceID:       workspaceID,
		TaskID:            taskID,
		TaskType:          taskType,
		Estimate:          map[string]any{"agentRunId": command.AgentRunID, "planVersion": command.PlanVersion, "generation": 1},
		Meters:            map[string]int{"generation": 1},
		ExpiresAt:         command.ConfirmedAt.Add(90 * time.Minute),
	})
	if err != nil {
		return QuotaAdmissionReservation{}, false, err
	}
	return admission, admission.Allowed && admission.ReservationID != "", nil
}

func (r *AgentRunRepository) existingConfirmationAdmissionInTx(ctx context.Context, tx *Tx, usage *UsageRepository, command AgentRunConfirmationCommand, taskID, workspaceID, taskType, reservationID, claimedPermissionCheckID string) (QuotaAdmissionReservation, error) {
	var storedReservationID, permissionCheckID, reservationUserID, reservationWorkspaceID, reservationTaskID, reservationTaskType, reservationStatus string
	var permissionUserID, permissionWorkspaceID, permissionTaskType, permissionStatus string
	var expiresAt time.Time
	if err := tx.QueryRowRaw(ctx, `
select r.reservation_id, coalesce(r.permission_check_id, ''), r.user_id, coalesce(r.workspace_id, ''), coalesce(r.task_id, ''), r.task_type, r.status, r.expires_at,
       pc.user_id, coalesce(pc.workspace_id, ''), pc.task_type, pc.status
from quota_reservations r
join permission_checks pc on pc.permission_check_id = r.permission_check_id
where r.reservation_id=$1
for update of r, pc`, reservationID).Scan(
		&storedReservationID, &permissionCheckID, &reservationUserID, &reservationWorkspaceID, &reservationTaskID, &reservationTaskType, &reservationStatus, &expiresAt,
		&permissionUserID, &permissionWorkspaceID, &permissionTaskType, &permissionStatus,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return QuotaAdmissionReservation{}, fmt.Errorf("QUOTA_RESERVATION_FAILED")
		}
		return QuotaAdmissionReservation{}, err
	}
	if permissionCheckID == "" || claimedPermissionCheckID != "" && claimedPermissionCheckID != permissionCheckID ||
		reservationUserID != command.UserID || reservationWorkspaceID != workspaceID || reservationTaskID != taskID || reservationTaskType != taskType || reservationStatus != "reserved" || !expiresAt.After(command.ConfirmedAt) ||
		permissionUserID != command.UserID || permissionWorkspaceID != workspaceID || permissionTaskType != taskType || permissionStatus != "allowed" {
		return QuotaAdmissionReservation{}, fmt.Errorf("QUOTA_RESERVATION_FAILED")
	}
	snapshot, err := usage.AdminQuotaSnapshotInTx(ctx, tx, command.UserID, map[string]any{})
	if err != nil {
		return QuotaAdmissionReservation{}, err
	}
	return QuotaAdmissionReservation{PermissionCheckID: permissionCheckID, ReservationID: storedReservationID, Allowed: true, QuotaSnapshot: snapshot}, nil
}

func (r *AgentRunRepository) validateConfirmedAdmissionInTx(ctx context.Context, tx *Tx, usage *UsageRepository, command AgentRunConfirmationCommand, taskID, workspaceID, threadID, taskType, lane string) error {
	if taskID == "" {
		return fmt.Errorf("AGENT_RUN_ADMISSION_UNAVAILABLE")
	}
	var taskUserID, taskWorkspaceID, taskThreadID, storedTaskType, taskStatus string
	var inputRaw []byte
	if err := tx.QueryRowRaw(ctx, `
select user_id, workspace_id, coalesce(thread_id, ''), task_type, status, input_snapshot
from ai_tasks
where task_id=$1
for update`, taskID).Scan(&taskUserID, &taskWorkspaceID, &taskThreadID, &storedTaskType, &taskStatus, &inputRaw); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("AGENT_RUN_ADMISSION_UNAVAILABLE")
		}
		return err
	}
	input := jsonMap(inputRaw)
	if !agentRunConfirmationTaskMatches(command, taskUserID, taskWorkspaceID, taskThreadID, storedTaskType, taskStatus, input, workspaceID, threadID, taskType, true) {
		return fmt.Errorf("AGENT_RUN_ADMISSION_UNAVAILABLE")
	}
	reservationID := agentRunConfirmationReservationID(input)
	if reservationID == "" {
		return fmt.Errorf("AGENT_RUN_ADMISSION_UNAVAILABLE")
	}
	if _, err := r.existingConfirmationAdmissionInTx(ctx, tx, usage, command, taskID, workspaceID, taskType, reservationID, stringOr(input["permissionCheckId"], "")); err != nil {
		return err
	}
	var queueName, queueTaskType, queueTaskID, queueDedupeKey, queueStatus string
	var queuePayloadRaw []byte
	if err := tx.QueryRowRaw(ctx, `
select queue_name, task_type, task_id, coalesce(dedupe_key, ''), status, payload
from task_queue_records
where queue_id=$1
for update`, lane+":"+command.AgentRunID).Scan(&queueName, &queueTaskType, &queueTaskID, &queueDedupeKey, &queueStatus, &queuePayloadRaw); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("AGENT_RUN_ADMISSION_UNAVAILABLE")
		}
		return err
	}
	if !agentRunConfirmationQueueMatches(command, lane, map[string]any{
		"queueId":   lane + ":" + command.AgentRunID,
		"queueName": queueName,
		"taskType":  queueTaskType,
		"taskId":    queueTaskID,
		"dedupeKey": queueDedupeKey,
		"status":    queueStatus,
		"payload":   jsonMap(queuePayloadRaw),
	}) {
		return fmt.Errorf("AGENT_RUN_ADMISSION_UNAVAILABLE")
	}
	var eventRunID, eventType, eventVisibility string
	var eventPayloadRaw []byte
	eventID := stableAgentRunEventID(command.AgentRunID, agentRunConfirmationEventKey(command))
	if err := tx.QueryRowRaw(ctx, `
select run_id, event_type, visibility, safe_payload
from runtime_run_events
where runtime_run_event_id=$1
for update`, eventID).Scan(&eventRunID, &eventType, &eventVisibility, &eventPayloadRaw); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("AGENT_RUN_ADMISSION_UNAVAILABLE")
		}
		return err
	}
	if !agentRunConfirmationPublicEventMatches(command, AgentRunEvent{
		AgentRunID: eventRunID,
		EventType:  eventType,
		Status:     stringOr(jsonMap(eventPayloadRaw)["status"], ""),
		SafeData:   jsonMap(eventPayloadRaw),
	}) {
		return fmt.Errorf("AGENT_RUN_ADMISSION_UNAVAILABLE")
	}
	return nil
}

func (r *AgentRunRepository) confirmPlanAndEnqueueWithAdmissionMemory(ctx context.Context, command AgentRunConfirmationCommand, usage *UsageRepository, tasks *ChatTaskRepository, queueRepo *QueueRepository) (AgentRunConfirmationResult, error) {
	agentRunConfirmationMemoryMu.Lock()
	defer agentRunConfirmationMemoryMu.Unlock()
	snapshot := captureAgentRunConfirmationMemorySnapshot(r, usage, tasks, queueRepo)
	rollback := false
	defer func() {
		if rollback {
			snapshot.restore(r, usage, tasks, queueRepo)
		}
	}()
	run, err := r.GetRun(ctx, command.TenantID, command.UserID, command.AgentRunID)
	if err != nil {
		return AgentRunConfirmationResult{}, fmt.Errorf("NOT_FOUND")
	}
	plan, err := r.GetPlan(ctx, command.AgentRunID, command.PlanVersion)
	if err != nil {
		return AgentRunConfirmationResult{}, fmt.Errorf("AGENT_PLAN_EXPIRED")
	}
	taskType := stringOr(plan["taskType"], "")
	if taskType == "" {
		return AgentRunConfirmationResult{}, fmt.Errorf("AGENT_PLAN_INVALID")
	}
	expectedLane := agentRunRuntimeLane(taskType, stringOr(plan["executionScope"], ""))
	if command.RuntimeLane != "" && command.RuntimeLane != expectedLane {
		return AgentRunConfirmationResult{}, fmt.Errorf("AGENT_PLAN_INVALID")
	}
	command.RuntimeLane = expectedLane
	if run.Status == "queued" && stringOr(plan["status"], "") == "confirmed" {
		storedHash := stringOr(plan["confirmationKeyHash"], "")
		if storedHash == "" {
			return AgentRunConfirmationResult{}, fmt.Errorf("AGENT_RUN_ADMISSION_UNAVAILABLE")
		}
		if !agentRunConfirmationKeyEqual(storedHash, agentRunConfirmationKeyHash(command.IdempotencyKey)) {
			return AgentRunConfirmationResult{}, fmt.Errorf("IDEMPOTENCY_KEY_CONFLICT")
		}
		if err := memoryValidateConfirmedAdmission(r, usage, tasks, queueRepo, command, run, taskType, command.RuntimeLane); err != nil {
			return AgentRunConfirmationResult{}, err
		}
		rollback = false
		return AgentRunConfirmationResult{TaskID: run.TaskID, Replayed: true}, nil
	}
	if run.Status != "awaiting_confirmation" || stringOr(plan["status"], "") != "awaiting_confirmation" {
		return AgentRunConfirmationResult{}, fmt.Errorf("AGENT_PLAN_EXPIRED")
	}
	taskID := run.TaskID
	createdTask := taskID == ""
	input := map[string]any{}
	if createdTask {
		taskID = agentRunConfirmationTaskID(command.AgentRunID)
		if _, existingErr := tasks.GetAiTask(taskID); existingErr == nil {
			return AgentRunConfirmationResult{}, fmt.Errorf("AGENT_RUN_ADMISSION_UNAVAILABLE")
		}
	} else {
		task, taskErr := tasks.GetAiTaskForUser(command.UserID, taskID)
		input = mapValue(task["refs"])
		if taskErr != nil || !agentRunConfirmationTaskMatches(command, stringOr(task["userId"], ""), stringOr(task["workspaceId"], ""), stringOr(task["threadId"], ""), stringOr(task["taskType"], ""), stringOr(task["status"], ""), input, run.WorkspaceID, run.ThreadID, taskType, false) {
			return AgentRunConfirmationResult{}, fmt.Errorf("AGENT_PLAN_INVALID")
		}
	}
	admission, admitted, admissionErr := memoryAgentRunConfirmationAdmission(usage, command, taskID, run.WorkspaceID, taskType, input)
	if admissionErr != nil {
		return AgentRunConfirmationResult{}, admissionErr
	}
	if !admitted {
		denyReason := admission.DenyReason
		if denyReason == "" {
			denyReason = "QUOTA_INSUFFICIENT"
		}
		return AgentRunConfirmationResult{}, fmt.Errorf("%s", denyReason)
	}
	rollback = true
	input["agentRunId"] = command.AgentRunID
	input["permissionCheckId"] = admission.PermissionCheckID
	input["reservationId"] = admission.ReservationID
	input["quotaReservation"] = map[string]any{"reservationId": admission.ReservationID, "status": "reserved"}
	input["threadId"] = run.ThreadID
	if createdTask {
		tasks.CreateAiTask(taskID, taskType, command.UserID, run.WorkspaceID, input)
		r.mu.Lock()
		stored := r.runs[command.AgentRunID]
		if stored.Status != "awaiting_confirmation" || stored.TaskID != "" {
			r.mu.Unlock()
			return AgentRunConfirmationResult{}, fmt.Errorf("AGENT_PLAN_EXPIRED")
		}
		stored.TaskID = taskID
		stored.UpdatedAt = command.ConfirmedAt
		r.runs[command.AgentRunID] = stored
		r.mu.Unlock()
	} else {
		tasks.CreateAiTask(taskID, taskType, command.UserID, run.WorkspaceID, input)
	}
	r.mu.Lock()
	storedPlan := r.plans[command.AgentRunID][command.PlanVersion]
	storedRun := r.runs[command.AgentRunID]
	if storedPlan == nil || storedRun.Status != "awaiting_confirmation" || stringOr(storedPlan["status"], "") != "awaiting_confirmation" {
		r.mu.Unlock()
		return AgentRunConfirmationResult{}, fmt.Errorf("AGENT_PLAN_EXPIRED")
	}
	storedPlan = copyMap(storedPlan)
	storedPlan["status"] = "confirmed"
	storedPlan["confirmationKeyHash"] = agentRunConfirmationKeyHash(command.IdempotencyKey)
	storedPlan["confirmedAt"] = command.ConfirmedAt.UTC().Format(time.RFC3339)
	r.plans[command.AgentRunID][command.PlanVersion] = storedPlan
	storedRun.Status = "queued"
	storedRun.UpdatedAt = command.ConfirmedAt
	r.runs[command.AgentRunID] = storedRun
	r.mu.Unlock()
	queued := queueRepo.Enqueue(map[string]any{
		"queueId":     command.RuntimeLane + ":" + command.AgentRunID,
		"queueName":   command.RuntimeLane,
		"taskType":    "runtime_dispatch",
		"taskId":      command.AgentRunID,
		"dedupeKey":   fmt.Sprintf("runtime_dispatch:%s:%d", command.AgentRunID, command.PlanVersion),
		"priority":    100,
		"maxAttempts": 5,
		"payload":     agentRunConfirmationQueuePayload(command, command.RuntimeLane),
	})
	if !agentRunConfirmationQueueMatches(command, command.RuntimeLane, queued) {
		return AgentRunConfirmationResult{}, fmt.Errorf("SERVICE_BUSY")
	}
	if err := r.AppendPublicEventIdempotent(ctx, AgentRunEvent{AgentRunID: command.AgentRunID, EventType: "queued", Status: "queued", SafeData: map[string]any{"status": "queued", "planVersion": command.PlanVersion}, CreatedAt: command.ConfirmedAt}, agentRunConfirmationEventKey(command)); err != nil {
		return AgentRunConfirmationResult{}, err
	}
	if err := memoryValidateConfirmedAdmission(r, usage, tasks, queueRepo, command, AgentRunRecord{TaskID: taskID, WorkspaceID: run.WorkspaceID, ThreadID: run.ThreadID}, taskType, command.RuntimeLane); err != nil {
		return AgentRunConfirmationResult{}, err
	}
	rollback = false
	return AgentRunConfirmationResult{TaskID: taskID, PermissionCheckID: admission.PermissionCheckID, ReservationID: admission.ReservationID}, nil
}

func memoryAgentRunConfirmationAdmission(usage *UsageRepository, command AgentRunConfirmationCommand, taskID, workspaceID, taskType string, input map[string]any) (QuotaAdmissionReservation, bool, error) {
	if reservationID := agentRunConfirmationReservationID(input); reservationID != "" {
		reservation, err := usage.GetQuotaReservation(reservationID)
		permissionCheckID := stringOr(reservation["permissionCheckId"], "")
		if err != nil || permissionCheckID == "" || stringOr(reservation["userId"], "") != command.UserID || stringOr(reservation["workspaceId"], "") != workspaceID || stringOr(reservation["taskId"], "") != taskID || stringOr(reservation["taskType"], "") != taskType || stringOr(reservation["status"], "") != "reserved" || !timeValue(reservation["expiresAt"], time.Time{}).After(command.ConfirmedAt) {
			return QuotaAdmissionReservation{}, false, fmt.Errorf("QUOTA_RESERVATION_FAILED")
		}
		usage.mu.Lock()
		check := copyMap(usage.checks[permissionCheckID])
		usage.mu.Unlock()
		if stringOr(check["permissionCheckId"], "") != permissionCheckID || stringOr(check["userId"], "") != command.UserID || stringOr(check["workspaceId"], "") != workspaceID || stringOr(check["taskType"], "") != taskType || stringOr(check["status"], "") != "allowed" {
			return QuotaAdmissionReservation{}, false, fmt.Errorf("QUOTA_RESERVATION_FAILED")
		}
		return QuotaAdmissionReservation{PermissionCheckID: permissionCheckID, ReservationID: reservationID, Allowed: true}, true, nil
	}
	snapshot := usage.QuotaSummary(command.UserID)
	checkID := agentRunConfirmationPermissionCheckIDForKey(command.AgentRunID, command.PlanVersion, agentRunConfirmationKeyHash(command.IdempotencyKey))
	usage.mu.Lock()
	existingCheck := copyMap(usage.checks[checkID])
	usage.mu.Unlock()
	if stringOr(existingCheck["status"], "") == "denied" {
		return QuotaAdmissionReservation{PermissionCheckID: checkID, Allowed: false, DenyReason: firstUsageAdmissionDenyReason(stringOr(existingCheck["denyReason"], "")), QuotaSnapshot: mapValue(existingCheck["quotaSnapshot"])}, false, nil
	}
	checkID = usage.CreatePermissionCheck("agent-run-confirm:"+command.AgentRunID+":"+fmt.Sprint(command.PlanVersion), command.UserID, workspaceID, taskType, map[string]any{"permissionCheckId": checkID, "generation": 1})
	if denyReason, featureSubject := usage.memoryMembershipAdmissionSubject(command.UserID, taskType, command.ConfirmedAt); denyReason != "" {
		usage.MarkPermissionDenied(checkID, denyReason, snapshot)
		return QuotaAdmissionReservation{PermissionCheckID: checkID, Allowed: false, DenyReason: denyReason, QuotaSnapshot: snapshot}, false, nil
	} else if globalDenyReason := usage.memoryGlobalRuntimeFeatureAdmissionDenyReason(taskType, featureSubject); globalDenyReason != "" {
		usage.MarkPermissionDenied(checkID, globalDenyReason, snapshot)
		return QuotaAdmissionReservation{PermissionCheckID: checkID, Allowed: false, DenyReason: globalDenyReason, QuotaSnapshot: snapshot}, false, nil
	}
	if !agentRunQuotaAllows(snapshot, map[string]int{"generation": 1}) {
		usage.MarkPermissionDenied(checkID, "QUOTA_INSUFFICIENT", snapshot)
		return QuotaAdmissionReservation{PermissionCheckID: checkID, Allowed: false, DenyReason: "QUOTA_INSUFFICIENT", QuotaSnapshot: snapshot}, false, nil
	}
	usage.MarkPermissionAllowed(checkID, snapshot, "")
	reservationID, err := usage.CreateReservation(checkID, taskID, taskType, map[string]int{"generation": 1}, command.ConfirmedAt.Add(90*time.Minute).Format(time.RFC3339))
	if err != nil {
		if errors.Is(err, ErrQuotaInsufficient) {
			usage.MarkPermissionDenied(checkID, "QUOTA_INSUFFICIENT", snapshot)
			return QuotaAdmissionReservation{PermissionCheckID: checkID, Allowed: false, QuotaSnapshot: snapshot}, false, nil
		}
		return QuotaAdmissionReservation{}, false, fmt.Errorf("QUOTA_RESERVATION_FAILED")
	}
	usage.MarkPermissionAllowed(checkID, snapshot, reservationID)
	return QuotaAdmissionReservation{PermissionCheckID: checkID, ReservationID: reservationID, Allowed: true, QuotaSnapshot: snapshot}, true, nil
}

func memoryValidateConfirmedAdmission(r *AgentRunRepository, usage *UsageRepository, tasks *ChatTaskRepository, queueRepo *QueueRepository, command AgentRunConfirmationCommand, run AgentRunRecord, taskType, lane string) error {
	if run.TaskID == "" {
		return fmt.Errorf("AGENT_RUN_ADMISSION_UNAVAILABLE")
	}
	task, err := tasks.GetAiTaskForUser(command.UserID, run.TaskID)
	refs := mapValue(task["refs"])
	if err != nil || !agentRunConfirmationTaskMatches(command, stringOr(task["userId"], ""), stringOr(task["workspaceId"], ""), stringOr(task["threadId"], ""), stringOr(task["taskType"], ""), stringOr(task["status"], ""), refs, run.WorkspaceID, run.ThreadID, taskType, true) {
		return fmt.Errorf("AGENT_RUN_ADMISSION_UNAVAILABLE")
	}
	reservationID := agentRunConfirmationReservationID(refs)
	if reservationID == "" {
		return fmt.Errorf("AGENT_RUN_ADMISSION_UNAVAILABLE")
	}
	if _, admitted, err := memoryAgentRunConfirmationAdmission(usage, command, run.TaskID, run.WorkspaceID, taskType, refs); err != nil || !admitted {
		if err != nil {
			return err
		}
		return fmt.Errorf("AGENT_RUN_ADMISSION_UNAVAILABLE")
	}
	queueRecords := queueRepo.ListQueueRecords(map[string]any{"queueId": lane + ":" + command.AgentRunID})
	if len(queueRecords) != 1 || !agentRunConfirmationQueueMatches(command, lane, queueRecords[0]) {
		return fmt.Errorf("AGENT_RUN_ADMISSION_UNAVAILABLE")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	event, ok := r.eventIdempotency[command.AgentRunID+"\x00"+agentRunConfirmationEventKey(command)]
	if !ok || !agentRunConfirmationPublicEventMatches(command, event) {
		return fmt.Errorf("AGENT_RUN_ADMISSION_UNAVAILABLE")
	}
	for _, candidate := range r.events[command.AgentRunID] {
		if agentRunConfirmationPublicEventMatches(command, candidate) {
			return nil
		}
	}
	return fmt.Errorf("AGENT_RUN_ADMISSION_UNAVAILABLE")
}

// agentRunConfirmationMemorySnapshot gives the explicit in-memory test backend
// the same all-or-nothing confirmation boundary as the PostgreSQL path. It is
// deliberately private: configured datastores never use this path.
type agentRunConfirmationMemorySnapshot struct {
	runs             map[string]AgentRunRecord
	plans            map[string]map[int]map[string]any
	events           map[string][]AgentRunEvent
	eventIdempotency map[string]AgentRunEvent
	tasks            map[string]map[string]any
	taskEvents       map[string]map[string]any
	taskProgress     map[string]map[string]any
	usage            map[string]map[string]any
	checks           map[string]map[string]any
	reserves         map[string]map[string]any
	queueRecords     map[string]map[string]any
}

func captureAgentRunConfirmationMemorySnapshot(r *AgentRunRepository, usage *UsageRepository, tasks *ChatTaskRepository, queueRepo *QueueRepository) agentRunConfirmationMemorySnapshot {
	r.mu.Lock()
	tasks.mu.Lock()
	usage.mu.Lock()
	queueRepo.mu.Lock()
	snapshot := agentRunConfirmationMemorySnapshot{
		runs:             cloneAgentRunConfirmationRecords(r.runs),
		plans:            cloneAgentRunConfirmationPlans(r.plans),
		events:           cloneAgentRunConfirmationEvents(r.events),
		eventIdempotency: cloneAgentRunConfirmationEventMap(r.eventIdempotency),
		tasks:            cloneAgentRunConfirmationMapMap(tasks.tasks),
		taskEvents:       cloneAgentRunConfirmationMapMap(tasks.events),
		taskProgress:     cloneAgentRunConfirmationMapMap(tasks.progress),
		usage:            cloneAgentRunConfirmationMapMap(usage.usage),
		checks:           cloneAgentRunConfirmationMapMap(usage.checks),
		reserves:         cloneAgentRunConfirmationMapMap(usage.reserves),
		queueRecords:     cloneAgentRunConfirmationMapMap(queueRepo.records),
	}
	queueRepo.mu.Unlock()
	usage.mu.Unlock()
	tasks.mu.Unlock()
	r.mu.Unlock()
	return snapshot
}

func (s agentRunConfirmationMemorySnapshot) restore(r *AgentRunRepository, usage *UsageRepository, tasks *ChatTaskRepository, queueRepo *QueueRepository) {
	r.mu.Lock()
	tasks.mu.Lock()
	usage.mu.Lock()
	queueRepo.mu.Lock()
	r.runs = cloneAgentRunConfirmationRecords(s.runs)
	r.plans = cloneAgentRunConfirmationPlans(s.plans)
	r.events = cloneAgentRunConfirmationEvents(s.events)
	r.eventIdempotency = cloneAgentRunConfirmationEventMap(s.eventIdempotency)
	tasks.tasks = cloneAgentRunConfirmationMapMap(s.tasks)
	tasks.events = cloneAgentRunConfirmationMapMap(s.taskEvents)
	tasks.progress = cloneAgentRunConfirmationMapMap(s.taskProgress)
	usage.usage = cloneAgentRunConfirmationMapMap(s.usage)
	usage.checks = cloneAgentRunConfirmationMapMap(s.checks)
	usage.reserves = cloneAgentRunConfirmationMapMap(s.reserves)
	queueRepo.records = cloneAgentRunConfirmationMapMap(s.queueRecords)
	queueRepo.mu.Unlock()
	usage.mu.Unlock()
	tasks.mu.Unlock()
	r.mu.Unlock()
}

func cloneAgentRunConfirmationMapMap(source map[string]map[string]any) map[string]map[string]any {
	out := make(map[string]map[string]any, len(source))
	for key, value := range source {
		out[key] = cloneAgentRunConfirmationMap(value)
	}
	return out
}

func cloneAgentRunConfirmationMap(source map[string]any) map[string]any {
	if source == nil {
		return nil
	}
	raw, err := json.Marshal(source)
	if err != nil {
		return copyMap(source)
	}
	out := map[string]any{}
	if err := json.Unmarshal(raw, &out); err != nil {
		return copyMap(source)
	}
	return out
}

func cloneAgentRunConfirmationRecords(source map[string]AgentRunRecord) map[string]AgentRunRecord {
	out := make(map[string]AgentRunRecord, len(source))
	for key, value := range source {
		value = copyAgentRun(value)
		value.RequestSnapshot = cloneAgentRunConfirmationMap(value.RequestSnapshot)
		value.IntentSnapshot = cloneAgentRunConfirmationMap(value.IntentSnapshot)
		value.PlanSnapshot = cloneAgentRunConfirmationMap(value.PlanSnapshot)
		value.ExecutionIdentity = cloneAgentRunConfirmationMap(value.ExecutionIdentity)
		value.PublicResult = cloneAgentRunConfirmationMap(value.PublicResult)
		value.ErrorSummary = cloneAgentRunConfirmationMap(value.ErrorSummary)
		out[key] = value
	}
	return out
}

func cloneAgentRunConfirmationPlans(source map[string]map[int]map[string]any) map[string]map[int]map[string]any {
	out := make(map[string]map[int]map[string]any, len(source))
	for runID, plans := range source {
		out[runID] = make(map[int]map[string]any, len(plans))
		for version, plan := range plans {
			out[runID][version] = cloneAgentRunConfirmationMap(plan)
		}
	}
	return out
}

func cloneAgentRunConfirmationEvents(source map[string][]AgentRunEvent) map[string][]AgentRunEvent {
	out := make(map[string][]AgentRunEvent, len(source))
	for runID, events := range source {
		cloned := make([]AgentRunEvent, len(events))
		for index, event := range events {
			event.SafeData = cloneAgentRunConfirmationMap(event.SafeData)
			cloned[index] = event
		}
		out[runID] = cloned
	}
	return out
}

func cloneAgentRunConfirmationEventMap(source map[string]AgentRunEvent) map[string]AgentRunEvent {
	out := make(map[string]AgentRunEvent, len(source))
	for key, event := range source {
		event.SafeData = cloneAgentRunConfirmationMap(event.SafeData)
		out[key] = event
	}
	return out
}

func agentRunQuotaAllows(snapshot map[string]any, meters map[string]int) bool {
	remaining := map[string]float64{}
	for _, raw := range anySlice(snapshot["balances"]) {
		balance := mapValue(raw)
		remaining[stringOr(balance["quotaType"], "generation")] = floatValue(balance["remainingAmount"], 0)
	}
	for quotaType, amount := range meters {
		if amount > 0 && remaining[quotaType] < float64(amount) {
			return false
		}
	}
	return true
}

func agentRunConfirmationTaskID(agentRunID string) string {
	sum := sha256.Sum256([]byte("agent-run-confirm-task\x00" + agentRunID))
	return "agent_task_" + hex.EncodeToString(sum[:16])
}

func (r *AgentRunRepository) appendConfirmationTaskQueuedEventsInTx(ctx context.Context, tx *Tx, command AgentRunConfirmationCommand, taskID, threadID string) error {
	traceID := "agent-run-confirm:" + command.AgentRunID + ":" + fmt.Sprint(command.PlanVersion)
	if _, err := tx.ExecRaw(ctx, `
insert into task_status_events(task_status_event_id,target_type,target_id,user_id,status,stage,trace_id,error_summary)
values($1,'ai_task',$2,$3,'queued','agent_run_confirm',$4,'{}'::jsonb)
on conflict(task_status_event_id) do nothing`, agentRunConfirmationEventID(command.AgentRunID, "task_status"), taskID, command.UserID, traceID); err != nil {
		return err
	}
	_, err := tx.ExecRaw(ctx, `
insert into ai_task_events(event_id,task_id,thread_id,run_id,event_type,visibility,title,summary,redaction_version)
values($1,$2,nullif($3,''),$4,'task_queued','app_safe','任务已排队',null,'app_safe_v1')
on conflict(event_id) do nothing`, agentRunConfirmationEventID(command.AgentRunID, "task_queued"), taskID, threadID, command.AgentRunID)
	return err
}

func agentRunConfirmationEventID(agentRunID, kind string) string {
	sum := sha256.Sum256([]byte("agent-run-confirm-event\x00" + agentRunID + "\x00" + kind))
	return "agent_confirm_event_" + hex.EncodeToString(sum[:16])
}

func agentRunConfirmationPermissionCheckIDForKey(agentRunID string, planVersion int, keyHash string) string {
	sum := sha256.Sum256([]byte("agent-run-confirm-permission\x00" + agentRunID + "\x00" + fmt.Sprint(planVersion) + "\x00" + keyHash))
	return "perm_check_" + hex.EncodeToString(sum[:16])
}

func agentRunConfirmationKeyHash(key string) string {
	sum := sha256.Sum256([]byte("agent-run-confirm-key\x00" + key))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func agentRunConfirmationKeyEqual(left, right string) bool {
	if len(left) != len(right) || left == "" || right == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(left), []byte(right)) == 1
}

func agentRunConfirmationReservationID(input map[string]any) string {
	if reservationID := stringOr(input["reservationId"], ""); reservationID != "" {
		return reservationID
	}
	return stringOr(mapValue(input["quotaReservation"])["reservationId"], "")
}

func agentRunConfirmableTaskStatus(status string) bool {
	return status == "queued" || status == "admission_pending"
}

func agentRunConfirmedTaskStatus(status string) bool {
	return status == "queued" || status == "admission_pending" || status == "running"
}

func agentRunRuntimeLane(taskType, executionScope string) string {
	if taskType == "minutes_generation" || taskType == "summary_generation" || taskType == "material_deposit_generation" {
		return "ai_runtime_recording"
	}
	if executionScope != "product_thread" {
		return "ai_runtime_background"
	}
	return "ai_runtime_interactive"
}
