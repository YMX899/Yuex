package services

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"huahuoai/backend/source/internal/domain"
	"huahuoai/backend/source/internal/persistence"
	"huahuoai/backend/source/internal/queue"
	runtimepkg "huahuoai/backend/source/internal/runtime"
)

type AgentRunService struct {
	Repos                  *persistence.Repositories
	Now                    func() time.Time
	Resolver               TaskIntentResolver
	MetaWorkspaceAdmission MetaWorkspaceAdmission
}

func NewAgentRunService(repos *persistence.Repositories, now func() time.Time) AgentRunService {
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	return AgentRunService{Repos: repos, Now: now, Resolver: NewTaskIntentResolver()}
}

func NewAgentRunServiceWithMetaWorkspaceAdmission(repos *persistence.Repositories, now func() time.Time, admission MetaWorkspaceAdmission) AgentRunService {
	service := NewAgentRunService(repos, now)
	service.MetaWorkspaceAdmission = admission
	return service
}

func (s AgentRunService) Create(ctx context.Context, auth domain.AuthContext, request domain.AgentRunRequest, idempotencyKey string) (persistence.AgentRunRecord, error) {
	record, err := s.create(ctx, auth, request, idempotencyKey, "")
	if err != nil {
		return persistence.AgentRunRecord{}, err
	}
	// POST /agent/runs can replay a Run after Workers have advanced its
	// internal lifecycle. Keep that replay on the same App-visible status
	// contract as the detail endpoint without changing the durable state.
	return projectPublicAgentRunBase(record), nil
}

func (s AgentRunService) CreateProductTask(ctx context.Context, auth domain.AuthContext, request domain.AgentRunRequest, idempotencyKey, taskID string) (persistence.AgentRunRecord, error) {
	if taskID == "" || (request.IntentHint.TaskType == "" && !compatibilityTaskProjectionRequest(request)) {
		return persistence.AgentRunRecord{}, domain.ErrorCode("INVALID_ARGUMENT")
	}
	record, err := s.create(ctx, auth, request, idempotencyKey, taskID)
	if err != nil {
		return persistence.AgentRunRecord{}, err
	}
	return projectPublicAgentRunBase(record), nil
}

// compatibilityTaskProjectionRequest is an internal bridge marker. It allows
// an existing AiTask.taskType to stay available for APP projection while the
// corresponding AgentRun resolves its intent dynamically. It is not accepted
// by public AgentRun routes as a task-routing input.
func compatibilityTaskProjectionRequest(request domain.AgentRunRequest) bool {
	return request.IntentHint.TaskType == "" && request.ClientContext["compatibilityTaskProjection"] == "true"
}

func (s AgentRunService) create(ctx context.Context, auth domain.AuthContext, request domain.AgentRunRequest, idempotencyKey, taskID string) (persistence.AgentRunRecord, error) {
	if s.Repos == nil || s.Repos.AgentRuns == nil || idempotencyKey == "" {
		return persistence.AgentRunRecord{}, domain.ErrorCode("IDEMPOTENCY_KEY_REQUIRED")
	}
	// The logical App key is part of the immutable Run identity. Normalize it
	// before both idempotency hashing and persistence so whitespace cannot make
	// two spellings of the same selected Meta Workspace diverge later in
	// planning.
	request.ExpectedMetaWorkspaceKey = strings.TrimSpace(request.ExpectedMetaWorkspaceKey)
	if request.ExpectedMetaWorkspaceKey != "" {
		if s.MetaWorkspaceAdmission == nil {
			return persistence.AgentRunRecord{}, domain.ErrorCode("META_WORKSPACE_UNAVAILABLE")
		}
		if err := s.MetaWorkspaceAdmission.AdmitMetaWorkspace(ctx, auth.UserID, request.ExpectedMetaWorkspaceKey); err != nil {
			return persistence.AgentRunRecord{}, err
		}
	}
	if err := normalizeAgentRunRequestAttachments(&request); err != nil {
		return persistence.AgentRunRecord{}, err
	}
	if err := rejectV1AgentRunAttachments(request.Attachments); err != nil {
		return persistence.AgentRunRecord{}, err
	}
	requestedWorkspaceID := request.WorkspaceID
	for attempt := 0; attempt < 2; attempt++ {
		binding, err := NewThreadWorkspaceService(s.Repos).ResolveForRun(ctx, auth, request.ThreadID, requestedWorkspaceID)
		if err != nil {
			return persistence.AgentRunRecord{}, err
		}
		// For a product thread the binding, not the token/default Workspace, is the
		// authoritative execution snapshot. Keep tenant/user identity unchanged and
		// give the resolver the same effective Workspace that will be persisted.
		effectiveAuth := auth
		effectiveAuth.WorkspaceID = binding.ActiveWorkspaceID
		workspaceVersion, snapshotErr := currentRunWorkspaceVersion(s.Repos, effectiveAuth, binding.ActiveWorkspaceID)
		if snapshotErr != nil {
			return persistence.AgentRunRecord{}, snapshotErr
		}
		runID := idFromKey("agent_run", auth.UserID+"_"+idempotencyKey)
		request.AgentRunID = runID
		request.WorkspaceID = binding.ActiveWorkspaceID
		// Resolve before CreateRun: the repository publishes the planning queue in
		// the same transaction as the Run, which revalidates this exact Workspace
		// version and binding before anything becomes runnable.
		intent, resolveErr := s.Resolver.Resolve(ctx, effectiveAuth, request)
		if resolveErr != nil {
			return persistence.AgentRunRecord{}, resolveErr
		}
		// A product-thread Run has a durable session binding keyed by Thread. Do
		// not enqueue an unbound Run that Planning would later fail after creating
		// durable state.
		if intent.ExecutionScope == string(runtimepkg.ScopeProductThread) && strings.TrimSpace(request.ThreadID) == "" {
			return persistence.AgentRunRecord{}, domain.ErrorCode("INVALID_ARGUMENT")
		}
		intent.AgentRunID = runID
		intentMap := mapFromValue(intent)
		record := persistence.AgentRunRecord{AgentRunID: runID, TenantID: auth.TenantID, UserID: auth.UserID, WorkspaceID: binding.ActiveWorkspaceID, ThreadID: request.ThreadID, TaskID: taskID, IdempotencyKey: idempotencyKey, RequestHash: agentRunRequestHash(effectiveAuth, request), RequestSnapshot: agentRunRequestSnapshot(request), IntentSnapshot: intentMap, Status: "planning", RoutingMode: "dynamic", SourceSurface: request.IntentHint.SourceSurface, WorkspaceVersion: workspaceVersion, BindingVersion: binding.BindingVersion, ContextGeneration: binding.ContextGeneration}
		record, existing, createErr := s.Repos.AgentRuns.CreateRunAndEnqueuePlanning(ctx, persistence.AgentRunCreatePlanningCommand{
			Record: record, CreatedAt: s.Now(),
		}, s.Repos.Queue)
		if createErr != nil {
			if attempt == 0 && agentRunWorkspaceSnapshotConflict(createErr) {
				continue
			}
			return persistence.AgentRunRecord{}, mapAgentRunError(createErr)
		}
		if existing {
			return record, nil
		}
		return s.Repos.AgentRuns.GetRun(ctx, auth.TenantID, auth.UserID, runID)
	}
	return persistence.AgentRunRecord{}, domain.ServiceBusy()
}

func normalizeAgentRunRequestAttachments(request *domain.AgentRunRequest) error {
	if request == nil || len(request.Attachments) > 16 {
		return domain.ErrorCode("ATTACHMENT_INVALID")
	}
	seen := make(map[string]struct{}, len(request.Attachments))
	for index := range request.Attachments {
		attachment := &request.Attachments[index]
		attachment.ResourceID = strings.TrimSpace(attachment.ResourceID)
		attachment.Usage = strings.TrimSpace(attachment.Usage)
		legacyUsage := strings.TrimSpace(attachment.UsageHint)
		if attachment.Usage == "" {
			attachment.Usage = legacyUsage
		} else if legacyUsage != "" && legacyUsage != attachment.Usage {
			return domain.ErrorCode("ATTACHMENT_INVALID")
		}
		attachment.UsageHint = ""
		if attachment.ResourceID == "" || (attachment.Usage != "primary_input" && attachment.Usage != "reference") {
			return domain.ErrorCode("ATTACHMENT_INVALID")
		}
		if _, duplicate := seen[attachment.ResourceID]; duplicate {
			return domain.ErrorCode("ATTACHMENT_INVALID")
		}
		seen[attachment.ResourceID] = struct{}{}
	}
	return nil
}

// V1 publishes no Meta Workspace input policy that accepts attachments. Keep
// the rejection before any Run, planning queue, or chat-task persistence. A
// later Visual/document capability transaction must replace this rule together
// with its owned resource/MIME/size/freeze contract, not bypass it per route.
func rejectV1AgentRunAttachments(attachments []domain.AgentRunAttachment) error {
	if len(attachments) != 0 {
		return domain.ErrorCode("META_WORKSPACE_INPUT_UNSUPPORTED")
	}
	return nil
}

// currentRunWorkspaceVersion reads the creation-time Workspace version from
// the owned formal Workspace. The subsequent repository transaction locks and
// compares the same fact, so this preflight cannot turn a concurrent write or
// thread switch into a mixed-version Run.
func currentRunWorkspaceVersion(repos *persistence.Repositories, auth domain.AuthContext, workspaceID string) (int64, error) {
	if repos == nil || repos.Workspace == nil || strings.TrimSpace(auth.TenantID) == "" || strings.TrimSpace(auth.UserID) == "" || strings.TrimSpace(workspaceID) == "" {
		return 0, domain.ErrorCode("WORKSPACE_NOT_READY")
	}
	workspace, ok := repos.Workspace.GetWorkspace(workspaceID)
	if !ok || strings.TrimSpace(fmt.Sprint(workspace["tenantId"])) != auth.TenantID || strings.TrimSpace(fmt.Sprint(workspace["userId"])) != auth.UserID {
		return 0, domain.ErrorCode("NOT_FOUND")
	}
	if strings.TrimSpace(fmt.Sprint(workspace["status"])) != "ready" {
		return 0, domain.ErrorCode("WORKSPACE_NOT_READY")
	}
	version := agentRunWorkspaceVersion(workspace["workspaceVersion"])
	if version < 1 {
		return 0, domain.ErrorCode("WORKSPACE_VERSION_CONFLICT")
	}
	return version, nil
}

func agentRunWorkspaceVersion(value any) int64 {
	switch typed := value.(type) {
	case int64:
		return typed
	case int:
		return int64(typed)
	case int32:
		return int64(typed)
	case float64:
		if typed > 0 && typed == float64(int64(typed)) {
			return int64(typed)
		}
	}
	parsed, err := strconv.ParseInt(strings.TrimSpace(fmt.Sprint(value)), 10, 64)
	if err != nil {
		return 0
	}
	return parsed
}

func agentRunWorkspaceSnapshotConflict(err error) bool {
	if err == nil {
		return false
	}
	message := err.Error()
	return strings.Contains(message, "WORKSPACE_VERSION_CONFLICT") || strings.Contains(message, "THREAD_WORKSPACE_VERSION_CONFLICT")
}

func (s AgentRunService) Get(ctx context.Context, auth domain.AuthContext, agentRunID string) (persistence.AgentRunRecord, error) {
	// WorkspaceID is the immutable execution snapshot of a Run. It is not the
	// authorization boundary for historical Run operations because a thread may
	// later switch its active Workspace. Tenant and user ownership remain the
	// fail-closed boundary, enforced again by the repository lookup.
	if strings.TrimSpace(auth.TenantID) == "" || strings.TrimSpace(auth.UserID) == "" {
		return persistence.AgentRunRecord{}, domain.ErrorCode("NOT_FOUND")
	}
	record, err := s.Repos.AgentRuns.GetRun(ctx, auth.TenantID, auth.UserID, agentRunID)
	if err != nil {
		return persistence.AgentRunRecord{}, domain.ErrorCode("NOT_FOUND")
	}
	record = projectPublicAgentRunBase(record)
	record.Routing, record.Clarification = publicAgentRunRouting(record)
	return record, nil
}

// publicAgentRunStatus is the only lifecycle projection allowed to cross the
// owner-facing Run and event APIs. Persistence and Workers retain their more
// granular states for fencing, recovery, queue ownership, and audit; callers
// must never use this projection to drive an internal transition.
func publicAgentRunStatus(value string) string {
	switch strings.TrimSpace(value) {
	case "created", "resolving_intent", "resolving":
		return "resolving"
	case "planning":
		return "planning"
	case "awaiting_confirmation":
		return "awaiting_confirmation"
	case "admission_pending", "queued":
		return "queued"
	case "reserving", "dispatched", "accepted", "materializing", "running", "finalizing":
		return "running"
	case "aborting":
		return "aborting"
	case "succeeded":
		return "succeeded"
	case "cancelled", "aborted":
		return "cancelled"
	case "timeout":
		return "timeout"
	case "failed", "orphaned":
		return "failed"
	default:
		// A malformed or future internal value must not become a new App
		// lifecycle contract. Expose a safe terminal failure while operators
		// retain the original durable fact for diagnosis.
		return "failed"
	}
}

func projectPublicAgentRunBase(record persistence.AgentRunRecord) persistence.AgentRunRecord {
	internalStatus := strings.TrimSpace(record.Status)
	record.Status = publicAgentRunStatus(internalStatus)
	record.ErrorSummary = publicAgentRunErrorSummary(record.ErrorSummary)
	if internalStatus == "orphaned" && len(record.ErrorSummary) == 0 {
		// Recovery has retained the detailed cause internally, but an owner
		// must not be left polling an indefinite or opaque state. This stable
		// marker deliberately reveals neither Host nor recovery implementation.
		record.ErrorSummary = map[string]any{"code": "RUNTIME_FAILED", "retryable": true}
	}
	return record
}

// ProjectPublicAgentRunEventPage applies the same lifecycle contract to
// polling and SSE reads. It is intentionally read-only: event rows preserve
// their original internal lifecycle facts for recovery and audit.
func ProjectPublicAgentRunEventPage(page persistence.AgentRunEventPage) persistence.AgentRunEventPage {
	projected := page
	projected.Items = make([]persistence.AgentRunEvent, len(page.Items))
	var terminalSequence int64
	for index, event := range page.Items {
		projected.Items[index] = projectPublicAgentRunEvent(event)
		if publicAgentRunTerminalStatus(projected.Items[index].Status) {
			terminalSequence = projected.Items[index].Sequence
		}
	}
	if terminalSequence > 0 {
		projected.TerminalSequence = &terminalSequence
	} else if page.TerminalSequence != nil {
		sequence := *page.TerminalSequence
		projected.TerminalSequence = &sequence
	}
	return projected
}

func projectPublicAgentRunEvent(event persistence.AgentRunEvent) persistence.AgentRunEvent {
	projected := event
	projected.Status = publicAgentRunStatus(event.Status)
	projected.EventType = publicAgentRunLifecycleEventType(event.EventType)
	if event.SafeData == nil {
		return projected
	}
	projected.SafeData = make(map[string]any, len(event.SafeData))
	for key, value := range event.SafeData {
		projected.SafeData[key] = value
	}
	if _, hasStatus := event.SafeData["status"]; hasStatus {
		projected.SafeData["status"] = projected.Status
	}
	return projected
}

// Progress labels such as tool_running and finalizing remain documented
// public event types. Only raw internal lifecycle labels are normalized.
func publicAgentRunLifecycleEventType(value string) string {
	switch strings.TrimSpace(value) {
	case "created", "resolving_intent", "admission_pending", "reserving", "dispatched", "accepted", "materializing", "orphaned", "aborted":
		return publicAgentRunStatus(value)
	default:
		return value
	}
}

func publicAgentRunTerminalStatus(value string) bool {
	switch value {
	case "succeeded", "failed", "timeout", "cancelled":
		return true
	default:
		return false
	}
}

// publicAgentRunErrorSummary is the App projection of an internal terminal
// error. The durable summary may include worker stage, provider diagnostics or
// attachment facts, none of which can cross the owner-facing Run API.
func publicAgentRunErrorSummary(summary map[string]any) map[string]any {
	code := strings.TrimSpace(stringValue(summary["code"]))
	if code == "" {
		code = strings.TrimSpace(stringValue(summary["errorCode"]))
	}
	if !safeAgentRunIdentityIdentifier(code) {
		return nil
	}
	result := map[string]any{"code": code}
	if retryable, ok := summary["retryable"].(bool); ok {
		result["retryable"] = retryable
	}
	return result
}

func publicAgentRunRouting(record persistence.AgentRunRecord) (map[string]any, map[string]any) {
	plan, err := runtimepkg.AgentRunPlanFromSnapshot(record.PlanSnapshot)
	if err == nil && safeFrozenAgentRunExecutionIdentity(record.AgentRunID, plan) {
		routing := map[string]any{"state": "selected"}
		if strings.TrimSpace(plan.MetaWorkspaceKey) != "" {
			routing["selectedMetaWorkspace"] = map[string]any{
				"metaWorkspaceKey": plan.MetaWorkspaceKey,
				"version":          plan.MetaWorkspaceVersion,
				"inputPolicyHash":  plan.InputPolicyHash,
			}
		}
		return routing, nil
	}
	code := strings.TrimSpace(fmt.Sprint(record.ErrorSummary["code"]))
	clarification := map[string]any{}
	switch code {
	case "META_WORKSPACE_SELECTION_REQUIRED":
		clarification = map[string]any{"kind": "select_meta_workspace", "userMessage": "请选择本次要使用的智能体空间后重新发送。"}
	case "META_WORKSPACE_INPUT_REQUIRED":
		clarification = map[string]any{"kind": "provide_required_input", "userMessage": "请补充该智能体空间要求的输入后重新发送。"}
	case "AGENT_INTENT_UNRESOLVED":
		clarification = map[string]any{"kind": "clarify_intent", "userMessage": "请补充说明本次希望完成的任务。"}
	}
	if len(clarification) > 0 {
		return map[string]any{"state": "clarification_required"}, clarification
	}
	return map[string]any{"state": "planning"}, nil
}

// safeFrozenAgentRunExecutionIdentity validates the stored Plan before its
// narrow public routing projection is emitted. It intentionally does not call
// AgentProfileResolver or derive anything from a task type: Plan selection is
// immutable once persisted, and static mappings would rewrite that identity.
func safeFrozenAgentRunExecutionIdentity(expectedRunID string, plan runtimepkg.AgentRunPlan) bool {
	if strings.TrimSpace(expectedRunID) == "" || plan.AgentRunID != expectedRunID ||
		plan.SchemaVersion != "agent_run_plan.v1" || plan.PlanVersion < 1 ||
		(plan.RoutingMode != "dynamic" && plan.RoutingMode != "deterministic") ||
		(plan.ExecutionScope != "product_thread" && plan.ExecutionScope != "detached_task") ||
		!safeAgentRunIdentityIdentifier(plan.TaskType) ||
		!safeAgentRunIdentityIdentifier(plan.L1AgentProfile) ||
		!safeAgentRunIdentityIdentifier(plan.RuntimeConfigID) ||
		(plan.MetaWorkspaceKey != "" && !safeAgentRunIdentityIdentifier(plan.MetaWorkspaceKey)) ||
		(plan.MetaWorkspaceKey != "" && (!safeAgentRunIdentityIdentifier(plan.MetaWorkspaceVersion) || !strings.HasPrefix(plan.InputPolicyHash, "sha256:"))) ||
		strings.TrimSpace(plan.AgentHash) == "" || strings.TrimSpace(plan.CapabilityHash) == "" ||
		plan.WorkspaceVersion < 1 {
		return false
	}
	return len(safeAgentRunIdentitySkillProfiles(plan.SelectedSkillProfiles)) > 0
}

func safeAgentRunIdentitySkillProfiles(items []string) []string {
	if len(items) == 0 || len(items) > 8 {
		return nil
	}
	seen := make(map[string]struct{}, len(items))
	out := make([]string, 0, len(items))
	for _, item := range items {
		if !safeAgentRunIdentityIdentifier(item) {
			return nil
		}
		if _, duplicate := seen[item]; duplicate {
			return nil
		}
		seen[item] = struct{}{}
		out = append(out, item)
	}
	return out
}

func safeAgentRunIdentityIdentifier(value string) bool {
	if value == "" || value != strings.TrimSpace(value) || len(value) > 128 {
		return false
	}
	for _, char := range value {
		if (char < 'a' || char > 'z') && (char < 'A' || char > 'Z') &&
			(char < '0' || char > '9') && char != '_' && char != '-' && char != '.' {
			return false
		}
	}
	return true
}
func (s AgentRunService) ListEvents(ctx context.Context, auth domain.AuthContext, agentRunID string, after int64, limit int) (persistence.AgentRunEventPage, error) {
	if after < 0 || limit <= 0 || limit > 500 {
		return persistence.AgentRunEventPage{}, domain.ErrorCode("INVALID_ARGUMENT")
	}
	if _, err := s.Get(ctx, auth, agentRunID); err != nil {
		return persistence.AgentRunEventPage{}, err
	}
	page, err := s.Repos.AgentRuns.ListPublicEvents(ctx, agentRunID, after, limit)
	if err != nil {
		return persistence.AgentRunEventPage{}, mapAgentRunError(err)
	}
	if page.Gap {
		apiErr := domain.ErrorCode("RUNTIME_EVENT_GAP")
		apiErr.Details = map[string]any{
			"oldestAvailableSequence": page.OldestAvailableSequence,
			"latestSequence":          page.LatestSequence,
			"resumeAfterSequence":     page.OldestAvailableSequence - 1,
		}
		return persistence.AgentRunEventPage{}, apiErr
	}
	return ProjectPublicAgentRunEventPage(page), nil
}

func (s AgentRunService) Confirm(ctx context.Context, auth domain.AuthContext, agentRunID string, expectedPlanVersion int, key string) (persistence.AgentRunRecord, error) {
	if key == "" {
		return persistence.AgentRunRecord{}, domain.ErrorCode("IDEMPOTENCY_KEY_REQUIRED")
	}
	if s.Repos == nil || s.Repos.AgentRuns == nil || s.Repos.Usage == nil || s.Repos.ChatTasks == nil || s.Repos.Queue == nil {
		return persistence.AgentRunRecord{}, domain.ErrorCode("SERVICE_BUSY")
	}
	if _, err := s.Repos.AgentRuns.ConfirmPlanAndEnqueueWithAdmission(ctx, persistence.AgentRunConfirmationCommand{
		AgentRunID: agentRunID, TenantID: auth.TenantID, UserID: auth.UserID,
		PlanVersion: expectedPlanVersion, IdempotencyKey: key, ConfirmedAt: s.Now(),
	}, s.Repos.Usage, s.Repos.ChatTasks, s.Repos.Queue); err != nil {
		return persistence.AgentRunRecord{}, mapAgentRunError(err)
	}
	return s.Get(ctx, auth, agentRunID)
}

func runtimeLaneForPlan(plan map[string]any) string {
	taskType := fmt.Sprint(plan["taskType"])
	if taskType == "minutes_generation" || taskType == "summary_generation" || taskType == "material_deposit_generation" {
		return queue.QueueAIRuntimeRecording
	}
	if fmt.Sprint(plan["executionScope"]) != "product_thread" {
		return queue.QueueAIRuntimeBackground
	}
	return queue.QueueAIRuntimeInteractive
}

func (s AgentRunService) Cancel(ctx context.Context, auth domain.AuthContext, agentRunID, reasonCode, key string) (persistence.AgentRunRecord, error) {
	if key == "" {
		return persistence.AgentRunRecord{}, domain.ErrorCode("IDEMPOTENCY_KEY_REQUIRED")
	}
	if _, err := s.Get(ctx, auth, agentRunID); err != nil {
		return persistence.AgentRunRecord{}, err
	}
	reasonCode, err := normalizeAgentRunCancelReasonCode(reasonCode)
	if err != nil {
		return persistence.AgentRunRecord{}, domain.ErrorCode("INVALID_ARGUMENT")
	}
	decision, err := s.Repos.AgentRuns.RequestCancelAndEnqueue(ctx, agentRunID, reasonCode, abortReasonHash(reasonCode), s.Repos.Queue)
	if err != nil {
		return persistence.AgentRunRecord{}, mapAgentRunError(err)
	}
	if decision.StateChanged {
		_ = s.Repos.AgentRuns.AppendPublicEventIdempotent(ctx, persistence.AgentRunEvent{
			AgentRunID: agentRunID, EventType: decision.Status, Status: decision.Status,
			SafeData: map[string]any{"status": decision.Status}, CreatedAt: s.Now(),
		}, "agent-run-cancel:"+agentRunID+":"+decision.Status)
	}
	record, err := s.Get(ctx, auth, agentRunID)
	if err != nil {
		return persistence.AgentRunRecord{}, err
	}
	if record.Status == "cancelled" && record.TaskID != "" {
		if s.Repos == nil || s.Repos.ChatTasks == nil {
			return persistence.AgentRunRecord{}, domain.ErrorCode("SERVICE_BUSY")
		}
		cancelCode := record.CancelReasonCode
		if cancelCode == "" {
			cancelCode = reasonCode
		}
		if err := s.Repos.ChatTasks.UpdateAiTaskTerminalProjection(ctx, record.TaskID, "agent-run-cancel:"+record.AgentRunID, "failed", map[string]any{"code": cancelCode}, map[string]any{"stage": "agent_run_cancel", "runId": record.AgentRunID}); err != nil {
			return persistence.AgentRunRecord{}, mapAgentRunError(err)
		}
	}
	return record, nil
}

func normalizeAgentRunCancelReasonCode(value string) (string, error) {
	switch strings.TrimSpace(value) {
	case "USER_CANCELLED", "user_cancelled":
		return "USER_CANCELLED", nil
	case "TIMEOUT", "timeout":
		return "TIMEOUT", nil
	case "BUDGET_EXCEEDED", "budget_exceeded":
		return "BUDGET_EXCEEDED", nil
	case "LEASE_LOST", "lease_lost":
		return "LEASE_LOST", nil
	default:
		return "", fmt.Errorf("INVALID_ARGUMENT")
	}
}

func abortReasonHash(reason string) string {
	sum := sha256.Sum256([]byte(reason))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func agentRunRequestHash(auth domain.AuthContext, request domain.AgentRunRequest) string {
	raw, _ := json.Marshal(map[string]any{"tenantId": auth.TenantID, "userId": auth.UserID, "workspaceId": request.WorkspaceID, "threadId": request.ThreadID, "expectedMetaWorkspaceKey": request.ExpectedMetaWorkspaceKey, "input": request.Input, "attachments": request.Attachments, "intentHint": request.IntentHint, "businessRefs": request.BusinessRefs})
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
func mapFromValue(value any) map[string]any {
	raw, _ := json.Marshal(value)
	out := map[string]any{}
	_ = json.Unmarshal(raw, &out)
	return out
}
func agentRunRequestSnapshot(request domain.AgentRunRequest) map[string]any {
	return mapFromValue(map[string]any{
		"input": request.Input, "threadId": request.ThreadID, "workspaceId": request.WorkspaceID,
		"expectedMetaWorkspaceKey": request.ExpectedMetaWorkspaceKey,
		"attachments":              request.Attachments, "intentHint": request.IntentHint,
		"businessRefs": request.BusinessRefs, "clientContext": request.ClientContext,
	})
}
func mapAgentRunError(err error) error {
	if errors.Is(err, persistence.ErrQuotaInsufficient) {
		return domain.ErrorCode("QUOTA_INSUFFICIENT")
	}
	// Serializable owner transactions annotate their retry boundary while
	// retaining the classified cause. Keep those internal prefixes out of the
	// public contract just as the direct repository errors are kept out.
	for _, code := range []string{
		"IDEMPOTENCY_KEY_CONFLICT", "AGENT_PLAN_EXPIRED", "NOT_FOUND", "SERVICE_BUSY",
		"AGENT_RUN_PLANNING_UNAVAILABLE", "AGENT_RUN_CREATE_INCOMPLETE", "AGENT_PLANNING_COMMIT_INCOMPLETE",
		"AGENT_PLAN_INVALID", "INVALID_ARGUMENT", "WORKSPACE_NOT_READY", "THREAD_WORKSPACE_VERSION_CONFLICT", "WORKSPACE_VERSION_CONFLICT",
	} {
		if strings.Contains(err.Error(), code) {
			switch code {
			case "AGENT_RUN_PLANNING_UNAVAILABLE", "AGENT_RUN_CREATE_INCOMPLETE", "AGENT_PLANNING_COMMIT_INCOMPLETE":
				return domain.ErrorCode("SERVICE_BUSY")
			default:
				return domain.ErrorCode(code)
			}
		}
	}
	switch err.Error() {
	case "QUOTA_INSUFFICIENT":
		return domain.ErrorCode("QUOTA_INSUFFICIENT")
	case "IDEMPOTENCY_KEY_CONFLICT":
		return domain.ErrorCode("IDEMPOTENCY_KEY_CONFLICT")
	case "AGENT_PLAN_EXPIRED":
		return domain.ErrorCode("AGENT_PLAN_EXPIRED")
	case "NOT_FOUND":
		return domain.ErrorCode("NOT_FOUND")
	case "SERVICE_BUSY":
		return domain.ErrorCode("SERVICE_BUSY")
	case "AGENT_RUN_ADMISSION_UNAVAILABLE":
		return domain.ErrorCode("SERVICE_BUSY")
	case "AGENT_RUN_PLANNING_UNAVAILABLE", "AGENT_RUN_CREATE_INCOMPLETE", "AGENT_PLANNING_COMMIT_INCOMPLETE":
		return domain.ErrorCode("SERVICE_BUSY")
	case "QUOTA_RESERVATION_FAILED":
		return domain.ErrorCode("QUOTA_RESERVATION_FAILED")
	case "AGENT_PLAN_INVALID":
		return domain.ErrorCode("AGENT_PLAN_INVALID")
	case "MEMBER_EXPIRED":
		return domain.ErrorCode("MEMBER_EXPIRED")
	case "MEMBER_BLOCKED":
		return domain.ErrorCode("MEMBER_BLOCKED")
	case "FEATURE_NOT_ALLOWED":
		return domain.ErrorCode("FEATURE_NOT_ALLOWED")
	case "WORK_AI_DISABLED":
		return domain.ErrorCode("WORK_AI_DISABLED")
	case "FEED_AI_DISABLED":
		return domain.ErrorCode("FEED_AI_DISABLED")
	case "APP_CONFIG_UNAVAILABLE":
		return domain.ErrorCode("APP_CONFIG_UNAVAILABLE")
	case "INVALID_ARGUMENT":
		return domain.ErrorCode("INVALID_ARGUMENT")
	case "EVENT_IDEMPOTENCY_CONFLICT", "EVENT_SEQUENCE_CONFLICT":
		return domain.ErrorCode("SERVICE_BUSY")
	default:
		if strings.HasPrefix(err.Error(), "quota admission ") || strings.HasPrefix(err.Error(), "quota permission status conflict") || strings.HasPrefix(err.Error(), "quota reservation idempotency conflict") {
			return domain.ErrorCode("QUOTA_RESERVATION_FAILED")
		}
		return err
	}
}
func errorCodeOf(err error) string {
	if apiErr, ok := err.(*domain.APIError); ok {
		return apiErr.Code
	}
	return "INTERNAL_ERROR"
}
func stringInService(value string, values []string) bool {
	for _, item := range values {
		if value == item {
			return true
		}
	}
	return false
}
func _agentRunFmt(v any) string { return fmt.Sprint(v) }
