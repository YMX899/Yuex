package services

import (
	"context"
	"fmt"
	"strings"
	"time"

	"huahuoai/backend/source/internal/domain"
	"huahuoai/backend/source/internal/persistence"
	runtimepkg "huahuoai/backend/source/internal/runtime"
)

// materialGeneratedProposalApplier is deliberately narrower than
// MaterialService. It lets terminal projection fail closed until the Proposal
// implementation is linked, without routing a proposal candidate through the
// formal-variant write path.
type materialGeneratedProposalApplier interface {
	ApplyGeneratedProposal(context.Context, string, string, string, string, map[string]any) (domain.MaterialProposalView, error)
}

// materialGeneratedVariantApplier mirrors the proposal seam for the formal
// variant terminal path. It keeps Runtime usage propagation testable without
// weakening the authoritative MaterialService boundary.
type materialGeneratedVariantApplier interface {
	ApplyGeneratedVariantWithRuntimeUsage(context.Context, string, string, string, string, map[string]any) (domain.MaterialRevision, domain.MaterialProcessingJob, error)
	FailGeneratedVariantWithRuntimeUsage(context.Context, string, string, string, string, bool, map[string]any) error
}

type AgentRunProductProjector struct {
	Repos           *persistence.Repositories
	Now             func() time.Time
	Material        *MaterialService
	proposalApplier materialGeneratedProposalApplier
	variantApplier  materialGeneratedVariantApplier
	// afterTerminalProjection is a test-only fault point between a committed
	// Product side effect and durable ledger completion.
	afterTerminalProjection func() error
}

func (p AgentRunProductProjector) WithMaterialService(service MaterialService) AgentRunProductProjector {
	p.Material = &service
	if proposalApplier, ok := any(&service).(materialGeneratedProposalApplier); ok {
		p.proposalApplier = proposalApplier
	}
	if variantApplier, ok := any(&service).(materialGeneratedVariantApplier); ok {
		p.variantApplier = variantApplier
	}
	return p
}

func NewAgentRunProductProjector(repos *persistence.Repositories, now func() time.Time) AgentRunProductProjector {
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	return AgentRunProductProjector{Repos: repos, Now: now}
}

func (p AgentRunProductProjector) ProjectRunning(run persistence.AgentRunRecord) error {
	if run.TaskID == "" {
		return nil
	}
	if p.Repos == nil || p.Repos.ChatTasks == nil {
		return fmt.Errorf("PRODUCT_TASK_PROJECTION_UNAVAILABLE")
	}
	p.Repos.ChatTasks.UpdateAiTaskStatus(run.TaskID, "", "running", map[string]any{}, map[string]any{"stage": "agent_runtime", "runId": run.AgentRunID})
	return nil
}

func (p AgentRunProductProjector) ProjectTerminal(run persistence.AgentRunRecord, status string, result, errorSummary, usage map[string]any) error {
	plan, err := terminalProjectionPlanForRun(run)
	if err != nil {
		// Planning can fail before a Plan exists. Such a failure may project the
		// compatibility task state, but it must not choose a product route from
		// AiTask.taskType.
		return p.projectTerminal(context.Background(), run, nil, status, result, errorSummary, usage, "")
	}
	return p.ProjectTerminalForPlan(run, plan, status, result, errorSummary, usage)
}

// ProjectTerminalForPlan projects a terminal Run using the exact frozen plan.
// It exists for internal Runtime paths and focused tests; normal Runtime event
// processing uses the convergence-keyed variant below.
func (p AgentRunProductProjector) ProjectTerminalForPlan(run persistence.AgentRunRecord, plan runtimepkg.AgentRunPlan, status string, result, errorSummary, usage map[string]any) error {
	return p.projectTerminal(context.Background(), run, &plan, status, result, errorSummary, usage, "")
}

// ProjectTerminalWithConvergence binds Product writeback to the durable Runtime
// convergence ID. A replay first claims the Product projection ledger; a
// completed claim returns without reapplying Product effects.
func (p AgentRunProductProjector) ProjectTerminalWithConvergence(ctx context.Context, run persistence.AgentRunRecord, status string, result, errorSummary, usage map[string]any, convergenceID string) error {
	plan, err := terminalProjectionPlanForRun(run)
	if err != nil {
		return p.projectTerminalWithConvergence(ctx, run, nil, status, result, errorSummary, usage, convergenceID)
	}
	return p.ProjectTerminalWithPlanAndConvergence(ctx, run, plan, status, result, errorSummary, usage, convergenceID)
}

// ProjectTerminalWithPlanAndConvergence is the Runtime-event-worker path. The
// caller supplies the exact persisted dispatch PlanVersion, rather than
// letting the projector re-resolve an AiTask compatibility task type.
func (p AgentRunProductProjector) ProjectTerminalWithPlanAndConvergence(ctx context.Context, run persistence.AgentRunRecord, plan runtimepkg.AgentRunPlan, status string, result, errorSummary, usage map[string]any, convergenceID string) error {
	return p.projectTerminalWithConvergence(ctx, run, &plan, status, result, errorSummary, usage, convergenceID)
}

func (p AgentRunProductProjector) projectTerminalWithConvergence(ctx context.Context, run persistence.AgentRunRecord, plan *runtimepkg.AgentRunPlan, status string, result, errorSummary, usage map[string]any, convergenceID string) error {
	if run.TaskID == "" {
		return nil
	}
	convergenceID = strings.TrimSpace(convergenceID)
	if convergenceID == "" || p.Repos == nil || p.Repos.TerminalProductProjections == nil {
		return fmt.Errorf("PRODUCT_PROJECTION_UNAVAILABLE")
	}
	command := persistence.TerminalProductProjectionCommand{
		ConvergenceID: convergenceID, ProjectionKey: convergenceID, RunID: run.AgentRunID, TaskID: run.TaskID,
		Snapshot: map[string]any{
			"runId": run.AgentRunID, "taskId": run.TaskID, "status": status,
			"result": result, "error": errorSummary, "usage": usage,
		},
	}
	if plan != nil {
		command.Snapshot["planVersion"] = plan.PlanVersion
		command.Snapshot["terminalOutput"] = plan.TerminalOutput
	}
	claim, err := p.Repos.TerminalProductProjections.Acquire(ctx, command)
	if err != nil {
		return err
	}
	if claim.AlreadyCompleted {
		return nil
	}
	if err := p.projectTerminal(ctx, run, plan, status, result, errorSummary, usage, convergenceID); err != nil {
		_ = p.Repos.TerminalProductProjections.Fail(ctx, claim, productProjectionErrorCode(err))
		return err
	}
	if p.afterTerminalProjection != nil {
		if err := p.afterTerminalProjection(); err != nil {
			_ = p.Repos.TerminalProductProjections.Fail(ctx, claim, productProjectionErrorCode(err))
			return err
		}
	}
	if err := p.Repos.TerminalProductProjections.Complete(ctx, claim); err != nil {
		_ = p.Repos.TerminalProductProjections.Fail(ctx, claim, productProjectionErrorCode(err))
		return err
	}
	return nil
}

func (p AgentRunProductProjector) projectTerminal(ctx context.Context, run persistence.AgentRunRecord, plan *runtimepkg.AgentRunPlan, status string, result, errorSummary, usage map[string]any, projectionKey string) error {
	if run.TaskID == "" {
		return nil
	}
	projectionPlan := plan
	if projectionPlan == nil {
		projectionPlan = nil
	} else if _, identityErr := projectionPlan.TerminalOutputProfile(run.AgentRunID); identityErr != nil {
		projectionPlan = nil
	}
	status, result, errorSummary, err := p.normalizeTerminalWithPlan(run, projectionPlan, status, result, errorSummary)
	if err != nil {
		return err
	}
	task, err := p.productTaskForRun(run)
	if err != nil {
		return err
	}
	taskWasAlreadyOrphaned := strings.TrimSpace(stringValue(task["status"])) == "orphaned"
	taskType := ""
	if projectionPlan != nil {
		taskType = projectionPlan.TaskType
	}
	if status == "succeeded" {
		if projectionPlan == nil {
			return fmt.Errorf("PRODUCT_RESULT_PARSE_FAILED")
		}
		parsed, parseErr := runtimepkg.NewOutputParser().ParseAgentRunResultForPlan(result, *projectionPlan, run.AgentRunID, run.TaskID)
		if parseErr != nil {
			return fmt.Errorf("PRODUCT_RESULT_PARSE_FAILED")
		}
		result = copyProductResultWithParsed(result, parsed)
		if err := p.projectSuccess(ctx, taskForTerminalPlan(task, taskType), taskType, run, result, usage, projectionKey); err != nil {
			return err
		}
		if err := p.updateTerminalTask(ctx, run.TaskID, projectionKey, "succeeded", map[string]any{}, map[string]any{"stage": "agent_runtime", "runId": run.AgentRunID, "runtimeResult": result, "usage": usage}); err != nil {
			return err
		}
	} else {
		taskStatus := status
		if taskStatus == "cancelled" {
			taskStatus = "failed"
		}
		refs := taskRuntimeSnapshot(task)
		materialJobID := stringValue(refs["materialProcessingJobId"])
		if projectionPlan != nil && materialJobID == "" && recordingProductTask(taskType) && p.Repos.Media != nil {
			p.Repos.Media.WithRuntimeTaskRepositories(p.Repos.Queue, p.Repos.ChatTasks)
			if _, applyErr := p.Repos.Media.ApplyRecordingRuntimeFailure(run.TaskID, taskType, run.AgentRunID, errorSummary); applyErr != nil {
				return applyErr
			}
		}
		if projectionPlan != nil && materialJobID != "" && p.Repos.Materials != nil {
			code := firstNonEmpty(stringValue(errorSummary["code"]), "MATERIAL_GENERATION_FAILED")
			job, jobErr := p.Repos.Materials.GetProcessingJob(ctx, materialJobID)
			if jobErr != nil {
				return fmt.Errorf("AI_RESULT_WRITEBACK_FAILED")
			}
			if scopeErr := p.validateMaterialRunScope(ctx, run, job); scopeErr != nil {
				return scopeErr
			}
			if _, proposalJob, proposalErr := materialProposalTerminalIdentity(job); proposalErr != nil {
				return fmt.Errorf("AI_RESULT_WRITEBACK_FAILED")
			} else if proposalJob {
				if p.Material == nil {
					return fmt.Errorf("AI_RESULT_WRITEBACK_FAILED")
				}
				if _, failErr := p.Material.FailGeneratedProposalWithRuntimeUsage(ctx, materialJobID, run.AgentRunID, run.TaskID, code, copyProductProjectionMap(errorSummary), false, copyProductProjectionMap(usage)); failErr != nil {
					return fmt.Errorf("AI_RESULT_WRITEBACK_FAILED")
				}
			} else {
				variantApplier := p.variantApplier
				if variantApplier == nil && p.Material != nil {
					if linked, ok := any(p.Material).(materialGeneratedVariantApplier); ok {
						variantApplier = linked
					}
				}
				if variantApplier == nil {
					proof, proofErr := materialRuntimeGenerationProofForJob(job, run.AgentRunID, run.TaskID)
					if proofErr != nil || p.Repos.Materials.FailProcessingJobFromRunWithRuntimeUsage(ctx, proof, code, false, copyProductProjectionMap(usage)) != nil {
						return fmt.Errorf("AI_RESULT_WRITEBACK_FAILED")
					}
				} else if variantApplier.FailGeneratedVariantWithRuntimeUsage(ctx, materialJobID, run.AgentRunID, run.TaskID, code, false, copyProductProjectionMap(usage)) != nil {
					return fmt.Errorf("AI_RESULT_WRITEBACK_FAILED")
				}
			}
		}
		if err := p.updateTerminalTask(ctx, run.TaskID, projectionKey, taskStatus, errorSummary, map[string]any{"stage": "agent_runtime", "runId": run.AgentRunID, "usage": usage}); err != nil {
			return err
		}
	}
	return p.settleUsage(ctx, task, run, status, usage, projectionKey, taskWasAlreadyOrphaned)
}

func (p AgentRunProductProjector) updateTerminalTask(ctx context.Context, taskID, projectionKey, status string, errorSummary, result map[string]any) error {
	if p.Repos == nil || p.Repos.ChatTasks == nil {
		return fmt.Errorf("PRODUCT_TASK_PROJECTION_UNAVAILABLE")
	}
	if projectionKey == "" {
		p.Repos.ChatTasks.UpdateAiTaskStatus(taskID, "", status, errorSummary, result)
		return nil
	}
	return p.Repos.ChatTasks.UpdateAiTaskTerminalProjection(ctx, taskID, projectionKey, status, errorSummary, result)
}

// NormalizeTerminal keeps the direct-call compatibility surface while making
// successful output validation depend on the persisted Run plan. A Run that
// failed before planning may still project a failed AiTask without a plan.
func (p AgentRunProductProjector) NormalizeTerminal(run persistence.AgentRunRecord, status string, result, errorSummary map[string]any) (string, map[string]any, map[string]any, error) {
	plan, err := terminalProjectionPlanForRun(run)
	if err != nil {
		return p.normalizeTerminalWithPlan(run, nil, status, result, errorSummary)
	}
	return p.NormalizeTerminalForPlan(run, plan, status, result, errorSummary)
}

// NormalizeTerminalForPlan turns an immutable Runtime success with unusable
// output or an invalid frozen parser identity into a Product failure before
// convergence. It does not read AiTask.taskType.
func (p AgentRunProductProjector) NormalizeTerminalForPlan(run persistence.AgentRunRecord, plan runtimepkg.AgentRunPlan, status string, result, errorSummary map[string]any) (string, map[string]any, map[string]any, error) {
	return p.normalizeTerminalWithPlan(run, &plan, status, result, errorSummary)
}

func (p AgentRunProductProjector) normalizeTerminalWithPlan(run persistence.AgentRunRecord, plan *runtimepkg.AgentRunPlan, status string, result, errorSummary map[string]any) (string, map[string]any, map[string]any, error) {
	if status != "succeeded" || run.TaskID == "" {
		return status, result, errorSummary, nil
	}
	if _, err := p.productTaskForRun(run); err != nil {
		return status, result, errorSummary, err
	}
	if plan != nil {
		if _, parseErr := runtimepkg.NewOutputParser().ParseAgentRunResultForPlan(result, *plan, run.AgentRunID, run.TaskID); parseErr == nil {
			return status, result, errorSummary, nil
		} else {
			return "failed", map[string]any{}, terminalProjectionFailureSummary(status, errorSummary, parseErr), nil
		}
	}
	planErr := fmt.Errorf("AGENT_PLAN_INVALID")
	return "failed", map[string]any{}, terminalProjectionFailureSummary(status, errorSummary, planErr), nil
}

func terminalProjectionFailureSummary(status string, errorSummary map[string]any, cause error) map[string]any {
	if cause == nil {
		return copyProductProjectionMap(errorSummary)
	}
	failure := map[string]any{}
	for key, value := range errorSummary {
		failure[key] = value
	}
	code := strings.TrimSpace(cause.Error())
	if code == "" || code == "AI_RESULT_PARSE_FAILED" || code == "RUNTIME_INPUT_INVALID" || code == "SKILL_TASK_MISMATCH" {
		code = "AI_RESULT_PARSE_FAILED"
	}
	failure["code"] = code
	if code == "AGENT_PLAN_INVALID" {
		failure["stage"] = "agent_runtime_plan"
	} else {
		failure["stage"] = "agent_runtime_output"
	}
	failure["retryable"] = false
	failure["sourceStatus"] = status
	return failure
}

func (p AgentRunProductProjector) productTaskForRun(run persistence.AgentRunRecord) (map[string]any, error) {
	if p.Repos == nil || p.Repos.ChatTasks == nil {
		return nil, fmt.Errorf("PRODUCT_TASK_PROJECTION_UNAVAILABLE")
	}
	task, err := p.Repos.ChatTasks.GetAiTask(run.TaskID)
	if err != nil {
		return nil, fmt.Errorf("PRODUCT_TASK_NOT_FOUND")
	}
	refs := taskRuntimeSnapshot(task)
	if run.UserID != "" && stringValue(task["userId"]) != run.UserID ||
		run.WorkspaceID != "" && stringValue(task["workspaceId"]) != run.WorkspaceID ||
		run.ThreadID != "" && firstNonEmpty(stringValue(task["threadId"]), stringValue(refs["threadId"])) != run.ThreadID {
		return nil, fmt.Errorf("PRODUCT_TASK_SCOPE_MISMATCH")
	}
	return task, nil
}

func terminalProjectionPlanForRun(run persistence.AgentRunRecord) (runtimepkg.AgentRunPlan, error) {
	return runtimepkg.AgentRunPlanFromSnapshot(run.PlanSnapshot)
}

// taskForTerminalPlan preserves the durable AiTask record for thread/message
// linkage while presenting the frozen Plan task type to Product writeback.
// It never writes that derived type back into the compatibility projection.
func taskForTerminalPlan(task map[string]any, taskType string) map[string]any {
	projection := copyProductProjectionMap(task)
	projection["taskType"] = taskType
	return projection
}

func copyProductResultWithParsed(result, parsed map[string]any) map[string]any {
	out := map[string]any{}
	for key, value := range result {
		out[key] = value
	}
	out["parsedResult"] = parsed
	return out
}

func (p AgentRunProductProjector) projectSuccess(ctx context.Context, task map[string]any, taskType string, run persistence.AgentRunRecord, result, usage map[string]any, projectionKey string) error {
	if result == nil {
		result = map[string]any{}
	}
	refs := taskRuntimeSnapshot(task)
	materialJobID := stringValue(refs["materialProcessingJobId"])
	parsed := productParsedResult(result)
	if materialJobID != "" {
		if p.Material == nil || p.Repos == nil || p.Repos.Materials == nil {
			return fmt.Errorf("AI_RESULT_WRITEBACK_FAILED")
		}
		content := materialGeneratedMarkdown(taskType, parsed)
		if content == "" {
			return fmt.Errorf("PRODUCT_RESULT_PARSE_FAILED")
		}
		job, err := p.Repos.Materials.GetProcessingJob(ctx, materialJobID)
		if err != nil {
			return fmt.Errorf("AI_RESULT_WRITEBACK_FAILED")
		}
		if err := p.validateMaterialRunScope(ctx, run, job); err != nil {
			return err
		}
		if err := p.applyGeneratedMaterial(ctx, task, run, job, content, usage); err != nil {
			return fmt.Errorf("AI_RESULT_WRITEBACK_FAILED")
		}
		return nil
	}
	if recordingProductTask(taskType) {
		if p.Repos.Media == nil {
			return fmt.Errorf("AI_RESULT_WRITEBACK_FAILED")
		}
		p.Repos.Media.WithRuntimeTaskRepositories(p.Repos.Queue, p.Repos.ChatTasks)
		applied, err := p.Repos.Media.ApplyRecordingRuntimeResult(run.TaskID, taskType, run.AgentRunID, result)
		if err != nil {
			return fmt.Errorf("AI_RESULT_WRITEBACK_FAILED")
		}
		if _, err := HandoffRecordingSuccessor(p.Repos, applied); err != nil {
			return fmt.Errorf("AI_RESULT_WRITEBACK_FAILED")
		}
		return nil
	}
	switch taskType {
	case "feed_ai_chat", "profile_understanding":
		applied, applyErr := NewFeedAIService(p.Repos, p.Now).ApplyRuntimeReplyForTerminalProjection(task, parsed, projectionKey)
		if applyErr != nil {
			return applyErr
		}
		if len(applied) == 0 {
			return fmt.Errorf("AI_RESULT_WRITEBACK_FAILED")
		}
	case "profile_deposit":
		intent := mapValueFromService(parsed["assetWriteIntent"])
		if len(intent) > 0 {
			if _, err := NewFeedAIService(p.Repos, p.Now).SubmitProfileDepositIntent(task, intent); err != nil {
				return err
			}
		}
	case "work_ai_general_chat", "work_ai_topic_generation", "work_ai_renshe_content", "work_ai_huoke_content", "work_ai_huoke_topic_strategy", "work_ai_self_media_creation", "work_ai_faya_germination", "work_ai_visual_chat", "work_ai_content_creation", "workspace_lookup":
		if applied := NewWorkAIService(p.Repos, p.Now).ApplyRuntimeReplyForTerminalProjection(task, result, projectionKey); len(applied) == 0 {
			return fmt.Errorf("AI_RESULT_WRITEBACK_FAILED")
		}
	default:
		// A persisted terminal output identity without an explicit Product
		// projector must not silently mark the compatibility task succeeded.
		return fmt.Errorf("PRODUCT_RESULT_PARSE_FAILED")
	}
	return nil
}

// validateMaterialRunScope closes the terminal writeback boundary around the
// immutable AgentRun owner. Material jobs contain only a Material ID, so the
// package fact must agree with the Run before a success or failure can mutate
// the Material lifecycle. This also rejects historical Runs created under a
// wrong/default tenant rather than allowing them to write a different tenant's
// package during convergence.
func (p AgentRunProductProjector) validateMaterialRunScope(ctx context.Context, run persistence.AgentRunRecord, job domain.MaterialProcessingJob) error {
	if p.Repos == nil || p.Repos.Materials == nil || strings.TrimSpace(job.MaterialID) == "" || strings.TrimSpace(run.TenantID) == "" || strings.TrimSpace(run.UserID) == "" || strings.TrimSpace(run.WorkspaceID) == "" {
		return fmt.Errorf("PRODUCT_TASK_SCOPE_MISMATCH")
	}
	pkg, err := p.Repos.Materials.GetOwned(ctx, run.TenantID, run.UserID, run.WorkspaceID, job.MaterialID, true)
	if err != nil || pkg.TenantID != run.TenantID || pkg.UserID != run.UserID || pkg.WorkspaceID != run.WorkspaceID {
		return fmt.Errorf("PRODUCT_TASK_SCOPE_MISMATCH")
	}
	return nil
}

func (p AgentRunProductProjector) applyGeneratedMaterial(ctx context.Context, task map[string]any, run persistence.AgentRunRecord, job domain.MaterialProcessingJob, content string, usage map[string]any) error {
	proposalID, proposalJob, err := materialProposalTerminalIdentity(job)
	if err != nil {
		return err
	}
	if proposalJob {
		refs := taskRuntimeSnapshot(task)
		if strings.TrimSpace(stringValue(refs["materialProposalId"])) != proposalID {
			return fmt.Errorf("PRODUCT_TASK_SCOPE_MISMATCH")
		}
		proposalApplier := p.proposalApplier
		if proposalApplier == nil && p.Material != nil {
			if linked, ok := any(p.Material).(materialGeneratedProposalApplier); ok {
				proposalApplier = linked
			}
		}
		if proposalApplier == nil {
			return fmt.Errorf("MATERIAL_PROPOSAL_WRITEBACK_UNAVAILABLE")
		}
		_, err := proposalApplier.ApplyGeneratedProposal(ctx, job.JobID, run.AgentRunID, run.TaskID, content, copyProductProjectionMap(usage))
		return err
	}
	variantApplier := p.variantApplier
	if variantApplier == nil && p.Material != nil {
		if linked, ok := any(p.Material).(materialGeneratedVariantApplier); ok {
			variantApplier = linked
		}
	}
	if variantApplier == nil {
		return fmt.Errorf("AI_RESULT_WRITEBACK_FAILED")
	}
	_, _, err = variantApplier.ApplyGeneratedVariantWithRuntimeUsage(ctx, job.JobID, run.AgentRunID, run.TaskID, content, copyProductProjectionMap(usage))
	return err
}

func materialProposalTerminalIdentity(job domain.MaterialProcessingJob) (string, bool, error) {
	proposalID := strings.TrimSpace(stringValue(job.InputSnapshot["proposalId"]))
	mode := strings.TrimSpace(stringValue(job.InputSnapshot["mode"]))
	isProposal := job.JobType == domain.MaterialJobProposalGenerate || proposalID != "" || mode == "propose"
	if !isProposal {
		return "", false, nil
	}
	if job.JobType != domain.MaterialJobProposalGenerate || proposalID == "" || proposalID == "<nil>" {
		return "", true, fmt.Errorf("MATERIAL_PROPOSAL_INTEGRITY_FAILED")
	}
	return proposalID, true, nil
}

func copyProductProjectionMap(value map[string]any) map[string]any {
	if len(value) == 0 {
		return map[string]any{}
	}
	copy := make(map[string]any, len(value))
	for key, item := range value {
		copy[key] = item
	}
	return copy
}

func (p AgentRunProductProjector) settleUsage(ctx context.Context, task map[string]any, run persistence.AgentRunRecord, status string, usage map[string]any, projectionKey string, taskWasAlreadyOrphaned bool) error {
	if p.Repos == nil || p.Repos.Usage == nil {
		return nil
	}
	refs := taskRuntimeSnapshot(task)
	reservationID := firstNonEmpty(stringValue(refs["reservationId"]), stringValue(mapValueFromService(refs["quotaReservation"])["reservationId"]))
	if reservationID == "" {
		return nil
	}
	if len(usage) == 0 && status != "succeeded" {
		released := NewPermissionUsageService(p.Repos, p.Now).ReleaseReservation(reservationID, "agent_run_"+status)
		if stringValue(released["status"]) == "failed" {
			// A recovery replay can reach this point after the Product task was
			// already orphaned and usage settled. Releasing that reservation again
			// is a no-op, not a new quota failure.
			if p.alreadySettledOrphanedQuota(run, reservationID, status, taskWasAlreadyOrphaned) {
				return nil
			}
			return fmt.Errorf("QUOTA_RESERVATION_FAILED")
		}
		return nil
	}
	amount := 1.0
	if value, ok := usage["amount"].(float64); ok && value > 0 {
		amount = value
	}
	settled := NewPermissionUsageService(p.Repos, p.Now).SettleUsage(run.TaskID, reservationID, map[string]any{
		"usageKey": "usage_" + run.AgentRunID, "userId": run.UserID, "workspaceId": run.WorkspaceID,
		"taskId": run.TaskID, "meterType": "generation", "amount": amount, "provider": "openclaw",
		"runId": run.AgentRunID, "payload": map[string]any{"usage": usage, "status": status, "terminalProjectionKey": projectionKey},
	})
	if stringValue(settled["status"]) == "failed" {
		return fmt.Errorf("USAGE_RECORD_FAILED")
	}
	return nil
}

func (p AgentRunProductProjector) alreadySettledOrphanedQuota(run persistence.AgentRunRecord, reservationID, status string, taskWasAlreadyOrphaned bool) bool {
	if status != "orphaned" || !taskWasAlreadyOrphaned || p.Repos == nil || p.Repos.Usage == nil {
		return false
	}
	quota, err := p.Repos.Usage.GetQuotaReservation(reservationID)
	if err != nil || strings.TrimSpace(stringValue(quota["status"])) != "settled" {
		return false
	}
	if strings.TrimSpace(stringValue(quota["taskId"])) != run.TaskID {
		return false
	}
	if run.UserID != "" && strings.TrimSpace(stringValue(quota["userId"])) != run.UserID {
		return false
	}
	return run.WorkspaceID == "" || strings.TrimSpace(stringValue(quota["workspaceId"])) == run.WorkspaceID
}

func productProjectionErrorCode(err error) string {
	if err == nil {
		return ""
	}
	code := strings.TrimSpace(err.Error())
	if code == "" {
		return "AI_RESULT_WRITEBACK_FAILED"
	}
	return code
}

func productParsedResult(result map[string]any) map[string]any {
	if parsed := mapValueFromService(result["parsedResult"]); len(parsed) > 0 {
		return parsed
	}
	if runRecord := mapValueFromService(result["runRecord"]); len(runRecord) > 0 {
		if parsed := mapValueFromService(runRecord["parsedResult"]); len(parsed) > 0 {
			return parsed
		}
	}
	return result
}

func recordingProductTask(taskType string) bool {
	return taskType == "minutes_generation" || taskType == "summary_generation"
}

func materialProductTask(taskType string) bool {
	return taskType == "material_deposit_generation"
}

func materialGeneratedMarkdown(taskType string, parsed map[string]any) string {
	data := mapValueFromService(parsed["data"])
	switch taskType {
	case "minutes_generation":
		return strings.TrimSpace(firstNonEmpty(stringValue(data["minutesMarkdown"]), stringValue(data["summary"])))
	case "summary_generation":
		return strings.TrimSpace(firstNonEmpty(stringValue(data["summaryMarkdown"]), stringValue(data["summary"])))
	case "material_deposit_generation":
		return strings.TrimSpace(stringValue(data["depositMarkdown"]))
	default:
		return ""
	}
}
