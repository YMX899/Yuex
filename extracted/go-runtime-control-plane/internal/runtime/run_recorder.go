package runtime

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

type RuntimeRunRecord struct {
	RunID                  string         `json:"runId"`
	TaskID                 string         `json:"taskId"`
	Attempt                int            `json:"attempt"`
	TraceID                string         `json:"traceId,omitempty"`
	Status                 string         `json:"status"`
	ExecutionScope         string         `json:"executionScope,omitempty"`
	AgentProfile           string         `json:"agentProfile"`
	SkillProfile           string         `json:"skillProfile"`
	SkillMergedHash        string         `json:"skillMergedHash,omitempty"`
	MetaManifestVersion    string         `json:"metaManifestVersion,omitempty"`
	OpenClawSessionKeyHash string         `json:"openclawSessionKeyHash,omitempty"`
	PromptID               string         `json:"promptId"`
	PromptTemplateID       string         `json:"promptTemplateId,omitempty"`
	PromptTemplateVersion  string         `json:"promptTemplateVersion,omitempty"`
	PromptHash             string         `json:"promptHash,omitempty"`
	InputMessageHash       string         `json:"inputMessageHash,omitempty"`
	RuntimeConfigID        string         `json:"runtimeConfigId,omitempty"`
	MessageMode            string         `json:"messageMode,omitempty"`
	WorkspaceMode          string         `json:"workspaceMode,omitempty"`
	RuntimeWorkspaceRef    map[string]any `json:"runtimeWorkspaceRef,omitempty"`
	InputFiles             []string       `json:"inputFiles,omitempty"`
	OutputContract         map[string]any `json:"outputContract,omitempty"`
	DownstreamRequestID    string         `json:"downstreamRequestId,omitempty"`
	Result                 map[string]any `json:"result,omitempty"`
	ErrorCode              string         `json:"errorCode,omitempty"`
	ErrorSummary           map[string]any `json:"errorSummary,omitempty"`
	LogIndex               []string       `json:"logIndex,omitempty"`
	UsageSummary           map[string]any `json:"usageSummary,omitempty"`
	RenderManifest         map[string]any `json:"renderManifest,omitempty"`
	CreatedAt              time.Time      `json:"createdAt"`
	StartedAt              time.Time      `json:"startedAt,omitempty"`
	FinishedAt             time.Time      `json:"finishedAt,omitempty"`
}

type RunRecorder struct {
	mu           sync.Mutex
	records      map[string]RuntimeRunRecord
	taskAttempts map[string]string
	now          func() time.Time
}

// RunRecorder enforces unique in-process runId records and redacted log/usage
// summaries. It is the local transaction boundary for runtime idempotency:
// the first caller creates and starts a run, while duplicate task/attempt calls
// receive the existing record without creating another downstream request.
// Durable transactions, usage compensation, and queue side effects are owned by
// orchestrator callers and repository-backed services.
func NewRunRecorder() *RunRecorder {
	return &RunRecorder{records: map[string]RuntimeRunRecord{}, taskAttempts: map[string]string{}, now: func() time.Time { return time.Now().UTC() }}
}

func (r *RunRecorder) StartRun(command RuntimeRunCommand, plan ProfilePlan) RuntimeRunRecord {
	record, _ := r.StartRunOnce(command, plan)
	return record
}

func (r *RunRecorder) StartRunOnce(command RuntimeRunCommand, plan ProfilePlan) (RuntimeRunRecord, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ensureStateLocked()
	runID := command.RunID
	if runID == "" {
		runID = fmt.Sprintf("run_%s_%d", command.TaskID, command.Attempt)
	}
	attemptKey := taskAttemptKey(command.TaskID, command.Attempt)
	if existingRunID := r.taskAttempts[attemptKey]; existingRunID != "" {
		if existing, ok := r.records[existingRunID]; ok {
			return existing, false
		}
	}
	if existing, ok := r.records[runID]; ok {
		if attemptKey != "" {
			r.taskAttempts[attemptKey] = runID
		}
		return existing, false
	}
	now := r.now()
	record := RuntimeRunRecord{
		RunID:                 runID,
		TaskID:                command.TaskID,
		Attempt:               command.Attempt,
		TraceID:               command.TraceID,
		Status:                "running",
		ExecutionScope:        string(plan.ExecutionScope),
		AgentProfile:          plan.AgentProfile,
		SkillProfile:          plan.SkillProfile,
		PromptID:              plan.PromptTemplateID,
		PromptTemplateID:      plan.PromptTemplateID,
		PromptTemplateVersion: plan.PromptTemplateVersion,
		RuntimeConfigID:       plan.RuntimeConfigID,
		MessageMode:           string(plan.MessageMode),
		WorkspaceMode:         string(plan.WorkspaceMode),
		OutputContract:        cloneRuntimeMap(runtimeOutputContract(plan)),
		DownstreamRequestID:   NewRuntimeRequestID(runID),
		CreatedAt:             now,
		StartedAt:             now,
	}
	r.records[runID] = record
	if attemptKey != "" {
		r.taskAttempts[attemptKey] = runID
	}
	return record, true
}

func (r *RunRecorder) CreateRunRecord(task RuntimeRunCommand, runID string, attempt int, profilePlan ProfilePlan, renderManifest map[string]any) RuntimeRunRecord {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ensureStateLocked()
	if runID == "" {
		runID = fmt.Sprintf("run_%s_%d", task.TaskID, attempt)
	}
	attemptKey := taskAttemptKey(task.TaskID, attempt)
	if existingRunID := r.taskAttempts[attemptKey]; existingRunID != "" {
		if existing, ok := r.records[existingRunID]; ok {
			return existing
		}
	}
	if existing, ok := r.records[runID]; ok {
		if attemptKey != "" {
			r.taskAttempts[attemptKey] = runID
		}
		return existing
	}
	now := r.now()
	record := RuntimeRunRecord{
		RunID:                 runID,
		TaskID:                task.TaskID,
		Attempt:               attempt,
		TraceID:               task.TraceID,
		Status:                "created",
		ExecutionScope:        string(profilePlan.ExecutionScope),
		AgentProfile:          profilePlan.AgentProfile,
		SkillProfile:          profilePlan.SkillProfile,
		PromptID:              profilePlan.PromptTemplateID,
		PromptTemplateID:      profilePlan.PromptTemplateID,
		PromptTemplateVersion: profilePlan.PromptTemplateVersion,
		RuntimeConfigID:       profilePlan.RuntimeConfigID,
		MessageMode:           string(profilePlan.MessageMode),
		WorkspaceMode:         string(profilePlan.WorkspaceMode),
		OutputContract:        cloneRuntimeMap(runtimeOutputContract(profilePlan)),
		RenderManifest:        cloneRuntimeMap(renderManifest),
		CreatedAt:             now,
	}
	r.records[runID] = record
	if attemptKey != "" {
		r.taskAttempts[attemptKey] = runID
	}
	return record
}

func (r *RunRecorder) FinishRun(runID, status string, result map[string]any, errorCode string) RuntimeRunRecord {
	if status == "succeeded" {
		return r.MarkRunCompleted(runID, RuntimeRunResult{RunID: runID, Status: status, Usage: mapValue(result["usage"])}, result)
	}
	return r.MarkRunFailed(runID, map[string]any{"code": errorCode, "status": status}, nil)
}

func (r *RunRecorder) MarkRunStarted(runID, downstreamRequestID string) RuntimeRunRecord {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ensureStateLocked()
	record := r.records[runID]
	record.RunID = runID
	record.Status = "running"
	record.DownstreamRequestID = downstreamRequestID
	record.StartedAt = r.now()
	r.records[runID] = record
	return record
}

func (r *RunRecorder) MarkRunCompleted(runID string, runtimeResult RuntimeRunResult, parsedSummary map[string]any) RuntimeRunRecord {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ensureStateLocked()
	record := r.records[runID]
	record.RunID = runID
	record.Status = "succeeded"
	record.Result = cloneRuntimeMap(parsedSummary)
	record.UsageSummary = cloneRuntimeMap(runtimeResult.Usage)
	record.LogIndex = redactLogIndex(runtimeResult.Logs)
	record.FinishedAt = r.now()
	r.records[runID] = record
	return record
}

func (r *RunRecorder) MarkRunFailed(runID string, errorSummary map[string]any, logs []string) RuntimeRunRecord {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ensureStateLocked()
	record := r.records[runID]
	record.RunID = runID
	record.Status = failureStatusFromSummary(errorSummary)
	record.ErrorSummary = cloneRuntimeMap(errorSummary)
	record.ErrorCode = stringFromRuntimeMap(errorSummary, "code")
	if downstreamRequestID := stringFromRuntimeMap(errorSummary, "downstreamRequestId"); downstreamRequestID != "" {
		record.DownstreamRequestID = downstreamRequestID
	}
	record.LogIndex = redactLogIndex(logs)
	record.FinishedAt = r.now()
	r.records[runID] = record
	return record
}

func (r *RunRecorder) RecordRuntimeLogs(runID string, logIndex []string) RuntimeRunRecord {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ensureStateLocked()
	record := r.records[runID]
	record.RunID = runID
	record.LogIndex = redactLogIndex(logIndex)
	r.records[runID] = record
	return record
}

func (r *RunRecorder) RecordUsageSummary(runID string, usage map[string]any) map[string]any {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ensureStateLocked()
	record := r.records[runID]
	record.RunID = runID
	record.UsageSummary = cloneRuntimeMap(usage)
	r.records[runID] = record
	return map[string]any{"runId": runID, "status": "usage_summary_recorded", "usage": cloneRuntimeMap(usage)}
}

func (r *RunRecorder) RecordRenderManifest(runID string, manifest map[string]any) RuntimeRunRecord {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ensureStateLocked()
	record := r.records[runID]
	record.RunID = runID
	record.RenderManifest = cloneRuntimeMap(manifest)
	record.InputFiles = runtimeStringSliceFromAny(manifest["inputFiles"])
	r.records[runID] = record
	return record
}

func (r *RunRecorder) RecordPromptSnapshot(runID string, prompt PromptSnapshot, inputMessage string, plan ProfilePlan) RuntimeRunRecord {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ensureStateLocked()
	record := r.records[runID]
	record.RunID = runID
	record.PromptID = firstRuntimeNonEmpty(record.PromptID, prompt.TemplateID, plan.PromptTemplateID)
	record.PromptTemplateID = firstRuntimeNonEmpty(prompt.TemplateID, plan.PromptTemplateID)
	record.PromptTemplateVersion = firstRuntimeNonEmpty(prompt.TemplateVersion, plan.PromptTemplateVersion)
	record.PromptHash = prompt.Hash
	if strings.TrimSpace(inputMessage) != "" {
		record.InputMessageHash = simpleHash(inputMessage)
	}
	record.OutputContract = cloneRuntimeMap(runtimeOutputContract(plan))
	r.records[runID] = record
	return record
}

func (r *RunRecorder) RecordRuntimeWorkspaceRef(runID string, workspaceRef map[string]any) RuntimeRunRecord {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ensureStateLocked()
	record := r.records[runID]
	record.RunID = runID
	record.RuntimeWorkspaceRef = cloneRuntimeMap(workspaceRef)
	r.records[runID] = record
	return record
}

func (r *RunRecorder) RecordRuntimeAudit(runID string, skill SkillProfile, openclawSessionKey string) RuntimeRunRecord {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ensureStateLocked()
	record := r.records[runID]
	record.RunID = runID
	record.SkillProfile = firstRuntimeNonEmpty(record.SkillProfile, skill.SkillProfile)
	record.SkillMergedHash = firstRuntimeNonEmpty(record.SkillMergedHash, skill.Hash)
	record.MetaManifestVersion = firstRuntimeNonEmpty(record.MetaManifestVersion, skill.ManifestVersion)
	record.OpenClawSessionKeyHash = firstRuntimeNonEmpty(record.OpenClawSessionKeyHash, runtimeSessionKeyHash(openclawSessionKey))
	r.records[runID] = record
	return record
}

func (r *RunRecorder) Get(runID string) (RuntimeRunRecord, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ensureStateLocked()
	record, ok := r.records[runID]
	return record, ok
}

func (r *RunRecorder) FindByTaskAttempt(taskID string, attempt int) (RuntimeRunRecord, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ensureStateLocked()
	runID := r.taskAttempts[taskAttemptKey(taskID, attempt)]
	if runID == "" {
		return RuntimeRunRecord{}, false
	}
	record, ok := r.records[runID]
	return record, ok
}

func (r *RunRecorder) ensureStateLocked() {
	if r.records == nil {
		r.records = map[string]RuntimeRunRecord{}
	}
	if r.taskAttempts == nil {
		r.taskAttempts = map[string]string{}
	}
	if r.now == nil {
		r.now = func() time.Time { return time.Now().UTC() }
	}
}

func taskAttemptKey(taskID string, attempt int) string {
	if taskID == "" {
		return ""
	}
	return fmt.Sprintf("%s:%d", taskID, attempt)
}

func failureStatusFromSummary(summary map[string]any) string {
	switch stringFromRuntimeMap(summary, "code") {
	case "RUNTIME_TIMEOUT":
		return "timeout"
	case "WORKSPACE_FORBIDDEN":
		return "forbidden"
	default:
		return "failed"
	}
}

func redactLogIndex(logs []string) []string {
	redacted := []string{}
	for _, item := range logs {
		redacted = append(redacted, redactRuntimeText(item))
	}
	return redacted
}

func cloneRuntimeMap(value map[string]any) map[string]any {
	if value == nil {
		return nil
	}
	cloned := map[string]any{}
	for key, item := range value {
		cloned[key] = item
	}
	return cloned
}

func runtimeStringSliceFromAny(value any) []string {
	switch typed := value.(type) {
	case []string:
		out := append([]string(nil), typed...)
		return out
	case []any:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			text := fmt.Sprint(item)
			if text != "" {
				out = append(out, text)
			}
		}
		return out
	default:
		return nil
	}
}

func mapValue(value any) map[string]any {
	if typed, ok := value.(map[string]any); ok {
		return cloneRuntimeMap(typed)
	}
	return map[string]any{}
}

func stringFromRuntimeMap(value map[string]any, key string) string {
	if value == nil || value[key] == nil {
		return ""
	}
	if text, ok := value[key].(string); ok {
		return text
	}
	return fmt.Sprint(value[key])
}
