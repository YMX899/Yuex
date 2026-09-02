package services

import (
	"context"
	"fmt"
	"strings"
	"time"

	"huahuoai/backend/source/internal/domain"
	"huahuoai/backend/source/internal/persistence"
)

type ProductAgentRunCommand struct {
	// TenantID is an optional internal ownership assertion. The bridge never
	// trusts it as the source of truth: it must match the tenant stored on the
	// command Workspace before it can reach AgentRun or a queue payload.
	TenantID                 string
	UserID                   string
	WorkspaceID              string
	ThreadID                 string
	TaskID                   string
	TaskType                 string
	InputText                string
	SourceSurface            string
	ExpectedMetaWorkspaceKey string
	Attachments              []domain.AgentRunAttachment
	// CompatibilityTaskProjection keeps a legacy product task type for APP
	// projection and retry history, but deliberately excludes it from
	// TaskIntent resolution. The Planner must derive L1 Agent and Skill from
	// the authenticated Workspace plus the user input, never from a legacy
	// scene/feed task label.
	CompatibilityTaskProjection bool
	IdempotencyKey              string
	BusinessRefs                map[string]string
}

func CreateProductAgentRun(repos *persistence.Repositories, now func() time.Time, command ProductAgentRunCommand) (persistence.AgentRunRecord, error) {
	return createProductAgentRunWithMetaWorkspaceAdmission(repos, now, nil, command)
}

func createProductAgentRunWithMetaWorkspaceAdmission(repos *persistence.Repositories, now func() time.Time, admission MetaWorkspaceAdmission, command ProductAgentRunCommand) (persistence.AgentRunRecord, error) {
	if repos == nil || repos.AgentRuns == nil || repos.Queue == nil || repos.ChatTasks == nil || repos.Workspace == nil || strings.TrimSpace(command.UserID) == "" || strings.TrimSpace(command.WorkspaceID) == "" || strings.TrimSpace(command.TaskID) == "" || strings.TrimSpace(command.TaskType) == "" || strings.TrimSpace(command.InputText) == "" {
		return persistence.AgentRunRecord{}, domain.ErrorCode("INVALID_ARGUMENT")
	}
	task, err := repos.ChatTasks.GetAiTaskForUser(command.UserID, command.TaskID)
	if err != nil || strings.TrimSpace(fmt.Sprint(task["workspaceId"])) != command.WorkspaceID {
		return persistence.AgentRunRecord{}, domain.ErrorCode("NOT_FOUND")
	}
	refs := taskRuntimeSnapshot(task)
	storedThreadID := firstNonEmpty(stringValue(task["threadId"]), stringValue(refs["threadId"]))
	storedTaskType := firstNonEmpty(stringValue(task["taskType"]), stringValue(refs["taskType"]))
	commandThreadID := strings.TrimSpace(command.ThreadID)
	if storedTaskType != command.TaskType || storedThreadID != commandThreadID {
		return persistence.AgentRunRecord{}, domain.ErrorCode("INVALID_ARGUMENT")
	}
	tenantID, err := productAgentRunTenant(repos, command)
	if err != nil {
		return persistence.AgentRunRecord{}, err
	}
	intentHint := domain.AgentRunIntentHint{SourceSurface: command.SourceSurface}
	clientContext := map[string]string{}
	if !command.CompatibilityTaskProjection {
		intentHint.TaskType = command.TaskType
	} else {
		clientContext["compatibilityTaskProjection"] = "true"
	}
	request := domain.AgentRunRequest{
		Input: domain.AgentRunInput{Type: "text", Text: command.InputText}, ThreadID: command.ThreadID, WorkspaceID: command.WorkspaceID,
		ExpectedMetaWorkspaceKey: command.ExpectedMetaWorkspaceKey, Attachments: append([]domain.AgentRunAttachment(nil), command.Attachments...),
		IntentHint: intentHint, BusinessRefs: command.BusinessRefs, ClientContext: clientContext,
	}
	key := strings.TrimSpace(command.IdempotencyKey)
	if key == "" {
		key = "product_task:" + command.TaskID
	}
	run, err := NewAgentRunServiceWithMetaWorkspaceAdmission(repos, now, admission).CreateProductTask(context.Background(), domain.AuthContext{TenantID: tenantID, UserID: command.UserID, WorkspaceID: command.WorkspaceID}, request, key, command.TaskID)
	if err != nil {
		return persistence.AgentRunRecord{}, err
	}
	if err := repos.ChatTasks.LinkAiTaskAgentRun(command.TaskID, run.AgentRunID); err != nil {
		return persistence.AgentRunRecord{}, err
	}
	return run, nil
}

// productAgentRunTenant binds an internal product task to its durable
// Workspace owner. Product task rows do not carry a tenant column, so a
// caller-supplied claim is only an equality assertion and can never select a
// tenant or fall back to a process-wide default.
func productAgentRunTenant(repos *persistence.Repositories, command ProductAgentRunCommand) (string, error) {
	if repos == nil || repos.Workspace == nil || strings.TrimSpace(command.UserID) == "" || strings.TrimSpace(command.WorkspaceID) == "" {
		return "", domain.ErrorCode("INVALID_ARGUMENT")
	}
	workspace, ok := repos.Workspace.GetWorkspace(command.WorkspaceID)
	if !ok || strings.TrimSpace(stringValue(workspace["userId"])) != strings.TrimSpace(command.UserID) {
		return "", domain.ErrorCode("NOT_FOUND")
	}
	tenantID := strings.TrimSpace(stringValue(workspace["tenantId"]))
	if tenantID == "" {
		return "", domain.ErrorCode("NOT_FOUND")
	}
	if assertedTenantID := strings.TrimSpace(command.TenantID); assertedTenantID != "" && assertedTenantID != tenantID {
		return "", domain.ErrorCode("NOT_FOUND")
	}
	return tenantID, nil
}

func AttachAgentRunToTaskRefs(refs map[string]any, run persistence.AgentRunRecord) map[string]any {
	if refs == nil {
		refs = map[string]any{}
	}
	refs["agentRunId"] = run.AgentRunID
	refs["routingMode"] = run.RoutingMode
	refs["agentRunStatus"] = run.Status
	return refs
}

func ProductAgentRunInput(taskType string, refs map[string]any) string {
	for _, key := range []string{"message", "text", "transcript", "userRequest", "supplement"} {
		if value := strings.TrimSpace(fmt.Sprint(refs[key])); value != "" && value != "<nil>" {
			return value
		}
	}
	return "Execute product task " + taskType + " using the authorized input and business references."
}

func CreateMaterialAgentRun(repos *persistence.Repositories, now func() time.Time, pkg domain.MaterialPackage, job domain.MaterialProcessingJob) (persistence.AgentRunRecord, string, error) {
	if repos == nil || repos.ChatTasks == nil || pkg.MaterialID == "" || strings.TrimSpace(pkg.TenantID) == "" || pkg.UserID == "" || pkg.WorkspaceID == "" || job.JobID == "" {
		return persistence.AgentRunRecord{}, "", domain.ErrorCode("INVALID_ARGUMENT")
	}
	taskType := MaterialAgentTaskType(job.VariantKind)
	if taskType == "" {
		return persistence.AgentRunRecord{}, "", domain.ErrorCode("INVALID_ARGUMENT")
	}
	if _, err := productAgentRunTenant(repos, ProductAgentRunCommand{TenantID: pkg.TenantID, UserID: pkg.UserID, WorkspaceID: pkg.WorkspaceID}); err != nil {
		return persistence.AgentRunRecord{}, "", err
	}
	taskID := "material_task_" + safeProductAgentRunID(job.JobID)
	refs, businessRefs, refsErr := materialAgentRunReferences(pkg, job)
	if refsErr != nil {
		return persistence.AgentRunRecord{}, "", refsErr
	}
	repos.ChatTasks.CreateAiTask(taskID, taskType, pkg.UserID, pkg.WorkspaceID, refs)
	run, err := CreateProductAgentRun(repos, now, ProductAgentRunCommand{
		TenantID: pkg.TenantID, UserID: pkg.UserID, WorkspaceID: pkg.WorkspaceID, TaskID: taskID, TaskType: taskType,
		InputText: materialAgentInstruction(job.VariantKind), SourceSurface: "material_processing",
		IdempotencyKey: "material_agent_run:" + job.JobID, BusinessRefs: businessRefs,
	})
	return run, taskID, err
}

// HandoffMaterialAgentRunWithProof builds the same detached Material AgentRun
// contract as CreateMaterialAgentRun, but persists it only through the
// MaterialRepository proof-scoped handoff transaction. Calling the older
// create-then-mark sequence from a Material worker can leave an orphan run
// when its lease expires between those two calls.
func HandoffMaterialAgentRunWithProof(ctx context.Context, repos *persistence.Repositories, pkg domain.MaterialPackage, job domain.MaterialProcessingJob, proof domain.MaterialJobLeaseProof) (persistence.AgentRunRecord, string, error) {
	if repos == nil || repos.Materials == nil || repos.AgentRuns == nil || repos.Queue == nil || repos.ChatTasks == nil || pkg.MaterialID == "" || strings.TrimSpace(pkg.TenantID) == "" || pkg.UserID == "" || pkg.WorkspaceID == "" || job.JobID == "" {
		return persistence.AgentRunRecord{}, "", domain.ErrorCode("INVALID_ARGUMENT")
	}
	taskType := MaterialAgentTaskType(job.VariantKind)
	if taskType == "" {
		return persistence.AgentRunRecord{}, "", domain.ErrorCode("INVALID_ARGUMENT")
	}
	tenantID, err := productAgentRunTenant(repos, ProductAgentRunCommand{TenantID: pkg.TenantID, UserID: pkg.UserID, WorkspaceID: pkg.WorkspaceID})
	if err != nil {
		return persistence.AgentRunRecord{}, "", err
	}
	taskID := "material_task_" + safeProductAgentRunID(job.JobID)
	refs, businessRefs, refsErr := materialAgentRunReferences(pkg, job)
	if refsErr != nil {
		return persistence.AgentRunRecord{}, "", refsErr
	}
	request := domain.AgentRunRequest{
		Input: domain.AgentRunInput{Type: "text", Text: materialAgentInstruction(job.VariantKind)}, WorkspaceID: pkg.WorkspaceID,
		IntentHint: domain.AgentRunIntentHint{SourceSurface: "material_processing", TaskType: taskType}, BusinessRefs: businessRefs,
	}
	auth := domain.AuthContext{TenantID: tenantID, UserID: pkg.UserID, WorkspaceID: pkg.WorkspaceID}
	binding, bindingErr := NewThreadWorkspaceService(repos).ResolveForRun(ctx, auth, request.ThreadID, request.WorkspaceID)
	if bindingErr != nil {
		return persistence.AgentRunRecord{}, "", bindingErr
	}
	auth.WorkspaceID = binding.ActiveWorkspaceID
	workspaceVersion, snapshotErr := currentRunWorkspaceVersion(repos, auth, binding.ActiveWorkspaceID)
	if snapshotErr != nil {
		return persistence.AgentRunRecord{}, "", snapshotErr
	}
	runID := idFromKey("agent_run", auth.UserID+"_material_agent_run:"+job.JobID)
	request.AgentRunID = runID
	request.WorkspaceID = binding.ActiveWorkspaceID
	intent, intentErr := NewTaskIntentResolver().Resolve(ctx, auth, request)
	if intentErr != nil {
		return persistence.AgentRunRecord{}, "", intentErr
	}
	intent.AgentRunID = runID
	refs["agentRunId"] = runID
	run := persistence.AgentRunRecord{
		AgentRunID: runID, TenantID: tenantID, UserID: auth.UserID, WorkspaceID: binding.ActiveWorkspaceID, ThreadID: request.ThreadID,
		TaskID: taskID, IdempotencyKey: "material_agent_run:" + job.JobID, RequestHash: agentRunRequestHash(auth, request),
		RequestSnapshot: agentRunRequestSnapshot(request), IntentSnapshot: mapFromValue(intent), Status: "planning", RoutingMode: "dynamic",
		SourceSurface: "material_processing", WorkspaceVersion: workspaceVersion, BindingVersion: binding.BindingVersion, ContextGeneration: binding.ContextGeneration,
	}
	created, handoffErr := repos.Materials.HandoffProcessingJobToRuntimeWithProof(ctx, proof, persistence.MaterialRuntimeHandoffCommand{
		TaskID: taskID, TaskType: taskType, TaskUserID: auth.UserID, TaskWorkspaceID: binding.ActiveWorkspaceID, TaskRefs: refs, AgentRun: run,
	})
	if handoffErr != nil {
		return persistence.AgentRunRecord{}, "", mapAgentRunError(handoffErr)
	}
	return created, taskID, nil
}

func materialAgentRunReferences(pkg domain.MaterialPackage, job domain.MaterialProcessingJob) (map[string]any, map[string]string, error) {
	refs := map[string]any{
		"materialId": pkg.MaterialID, "materialProcessingJobId": job.JobID,
		"materialVariant": string(job.VariantKind), "sourceVersion": pkg.SourceVersion,
		"processingMode": pkg.ProcessingMode, "jobType": string(job.JobType),
	}
	businessRefs := map[string]string{
		"materialId": pkg.MaterialID, "materialProcessingJobId": job.JobID,
		"materialVariant": string(job.VariantKind),
	}
	proposalID := materialAgentProposalID(job.InputSnapshot)
	proposalRequested := job.JobType == domain.MaterialJobProposalGenerate || strings.TrimSpace(fmt.Sprint(job.InputSnapshot["mode"])) == "propose"
	if proposalRequested {
		if job.JobType != domain.MaterialJobProposalGenerate || proposalID == "" {
			return nil, nil, domain.ErrorCode("INVALID_ARGUMENT")
		}
		refs["materialProposalId"] = proposalID
		businessRefs["materialProposalId"] = proposalID
	}
	if value := job.InputSnapshot["mode"]; value != nil {
		refs["mode"] = value
	}
	if value := job.InputSnapshot["traceId"]; value != nil {
		refs["traceId"] = value
	}
	if minutes, ok := pkg.Variants[domain.MaterialVariantMinutes]; ok && minutes.CurrentVersion > 0 {
		refs["baseMinutesVersion"] = minutes.CurrentVersion
	}
	return refs, businessRefs, nil
}

func materialAgentProposalID(input map[string]any) string {
	if input == nil {
		return ""
	}
	value := strings.TrimSpace(fmt.Sprint(input["proposalId"]))
	if value == "" || value == "<nil>" {
		return ""
	}
	return value
}

func MaterialAgentTaskType(kind domain.MaterialVariantKind) string {
	switch kind {
	case domain.MaterialVariantMinutes:
		return "minutes_generation"
	case domain.MaterialVariantSummary:
		return "summary_generation"
	case domain.MaterialVariantDeposit:
		return "material_deposit_generation"
	default:
		return ""
	}
}

func materialAgentInstruction(kind domain.MaterialVariantKind) string {
	switch kind {
	case domain.MaterialVariantMinutes:
		return "Generate factual meeting minutes from input/transcript.md using the selected output contract."
	case domain.MaterialVariantSummary:
		return "Generate a concise reusable summary from input/transcript.md and input/minutes.md when present."
	case domain.MaterialVariantDeposit:
		return "Generate a reusable structured deposit from input/source.md and input/minutes.md when present."
	default:
		return "Process the authorized material input using the selected output contract."
	}
}

func safeProductAgentRunID(value string) string {
	var out strings.Builder
	for _, ch := range value {
		if ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' || ch == '_' || ch == '-' {
			out.WriteRune(ch)
		} else {
			out.WriteByte('_')
		}
	}
	if out.Len() == 0 {
		return "job"
	}
	return out.String()
}

func productAgentRunAPIError(err error) *domain.APIError {
	if apiErr, ok := err.(*domain.APIError); ok {
		return apiErr
	}
	return domain.ErrorCode("INTERNAL_ERROR")
}

func HandoffRecordingSuccessor(repos *persistence.Repositories, processed map[string]any) (persistence.AgentRunRecord, error) {
	if repos == nil || repos.ChatTasks == nil || repos.Queue == nil || processed == nil {
		return persistence.AgentRunRecord{}, nil
	}
	next := mapValueFromService(processed["next"])
	taskID := stringValue(next["aiTaskId"])
	if taskID == "" {
		return persistence.AgentRunRecord{}, nil
	}
	task, err := repos.ChatTasks.GetAiTask(taskID)
	if err != nil {
		return persistence.AgentRunRecord{}, fmt.Errorf("recording successor task missing")
	}
	taskType := stringValue(task["taskType"])
	if taskType != "minutes_generation" && taskType != "summary_generation" {
		return persistence.AgentRunRecord{}, nil
	}
	refs := taskRuntimeSnapshot(task)
	recordingID := firstNonEmpty(stringValue(task["recordingId"]), stringValue(refs["recordingId"]), stringValue(processed["recordingId"]))
	run, err := CreateProductAgentRun(repos, nil, ProductAgentRunCommand{
		UserID: stringValue(task["userId"]), WorkspaceID: stringValue(task["workspaceId"]),
		TaskID: taskID, TaskType: taskType, InputText: ProductAgentRunInput(taskType, refs),
		SourceSurface: "recording_postprocess", IdempotencyKey: "recording:" + taskType + ":" + recordingID,
		BusinessRefs: map[string]string{"recordingId": recordingID},
	})
	if err != nil {
		return persistence.AgentRunRecord{}, err
	}
	if legacyQueueID := stringValue(next["aiRuntimeQueueId"]); legacyQueueID != "" {
		repos.Queue.MarkIgnored(legacyQueueID, "superseded_by_agent_run", "recording_handoff")
	}
	next["agentRunId"] = run.AgentRunID
	next["agentPlanningQueueId"] = "queue_plan_" + run.AgentRunID
	delete(next, "aiRuntimeQueueId")
	processed["next"] = next
	return run, nil
}
