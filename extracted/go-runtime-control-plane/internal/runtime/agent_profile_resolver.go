package runtime

import (
	"fmt"
	"strings"
)

type ExecutionScope string

const (
	ScopeProductThread ExecutionScope = "product_thread"
	ScopeDetachedTask  ExecutionScope = "detached_task"
)

type MessageMode string

const (
	MessageModeUserTurn                MessageMode = "user_turn"
	MessageModeProductTaskTurn         MessageMode = "product_task_turn"
	MessageModeDetachedTaskInstruction MessageMode = "detached_task_instruction"
)

type WorkspaceMode string

const (
	WorkspaceModeFormalUserWorkspace              WorkspaceMode = "formal_user_workspace"
	WorkspaceModeFormalUserWorkspaceInputSnapshot WorkspaceMode = "formal_user_workspace_with_input_snapshot"
	WorkspaceModeDetachedRuntimeWorkspace         WorkspaceMode = "detached_runtime_workspace"
)

type ProfilePlan struct {
	TaskType              string         `json:"taskType"`
	ExecutionScope        ExecutionScope `json:"executionScope"`
	AgentProfile          string         `json:"agentProfile"`
	SkillProfile          string         `json:"skillProfile"`
	PromptTemplateID      string         `json:"promptTemplateId"`
	PromptTemplateVersion string         `json:"promptTemplateVersion"`
	OutputSchemaVersion   string         `json:"outputSchemaVersion"`
	RuntimeConfigID       string         `json:"runtimeConfigId"`
	RuntimeAccessMode     string         `json:"runtimeAccessMode"`
	MessageMode           MessageMode    `json:"messageMode"`
	WorkspaceMode         WorkspaceMode  `json:"workspaceMode"`
	TimeoutBudget         TimeoutBudget  `json:"timeoutBudget"`
}

type TimeoutBudget struct {
	BusinessTimeoutSec int `json:"businessTimeoutSec"`
	MaxToolCalls       int `json:"maxToolCalls"`
	MaxOutputChars     int `json:"maxOutputChars"`
}

const (
	runtimeProtectionWindowSec    = 3600
	legacyProjectionOnlyErrorCode = "LEGACY_PROJECTION_ONLY"
)

type AgentProfileResolver struct {
	plans                 map[string]ProfilePlan
	legacyProjectionPlans map[string]ProfilePlan
}

// IsLegacyProjectionTaskType identifies task labels that may appear on
// historical task/message records but must not select a new AgentRun plan.
func IsLegacyProjectionTaskType(taskType string) bool {
	return strings.TrimSpace(taskType) == "feed_ai_chat"
}

// AgentProfileResolver is a pure mapping layer. It does not change task status
// or other state machines; callers persist the resolved profile, execution
// scope, runtime protection budget, and runtime config on tasks/run records before
// queueing or retrying work; idempotency is enforced by those caller records.
func NewAgentProfileResolver() AgentProfileResolver {
	plans := map[string]ProfilePlan{
		"work_ai_topic_generation":     plan("work_ai_topic_generation", ScopeProductThread, "work_ai_agent", "topic_generation", "work_ai.topic_generation.v1"),
		"work_ai_general_chat":         plan("work_ai_general_chat", ScopeProductThread, "work_ai_agent", "general_chat", "work_ai.general_chat.v1"),
		"work_ai_renshe_content":       plan("work_ai_renshe_content", ScopeProductThread, "renshe_neirong_agent", "renshe_content_creation", "work_ai.renshe_content.v1"),
		"work_ai_huoke_content":        plan("work_ai_huoke_content", ScopeProductThread, "huoke_neirong_agent", "huoke_content_creation", "work_ai.huoke_content.v1"),
		"work_ai_huoke_topic_strategy": plan("work_ai_huoke_topic_strategy", ScopeProductThread, "huoke_neirong_agent", "huoke_topic_strategy", "work_ai.huoke_topic_strategy.v1"),
		"work_ai_self_media_creation":  plan("work_ai_self_media_creation", ScopeProductThread, "self_media_creation_agent", "self_media_creation_advisor", "work_ai.self_media_creation.v1"),
		"work_ai_faya_germination":     plan("work_ai_faya_germination", ScopeProductThread, "faya_agent", "viewpoint_germination", "work_ai.faya_germination.v2"),
		"work_ai_visual_chat":          plan("work_ai_visual_chat", ScopeProductThread, "visual_chat_agent", "visual_chat_assistant", "work_ai.visual_chat.v1"),
		"profile_deposit":              plan("profile_deposit", ScopeProductThread, "work_ai_agent", "profile_maintenance", "feed_ai.profile_maintenance.v1"),
		"minutes_generation":           plan("minutes_generation", ScopeDetachedTask, "recording_postprocess_agent", "meeting_minutes", "recording.meeting_minutes.v1"),
		"summary_generation":           plan("summary_generation", ScopeDetachedTask, "recording_postprocess_agent", "asset_summary", "recording.asset_summary.v1"),
		"material_deposit_generation":  plan("material_deposit_generation", ScopeDetachedTask, "recording_postprocess_agent", "material_deposit", "material.deposit.v1"),
		"hotspot_home_suggestion":      plan("hotspot_home_suggestion", ScopeDetachedTask, "hotspot_agent", "hotspot_suggestion", "hotspot.suggestion.v1"),
	}
	renshe := plans["work_ai_renshe_content"]
	renshe.RuntimeAccessMode = "read"
	plans["work_ai_renshe_content"] = renshe
	huoke := plans["work_ai_huoke_content"]
	huoke.RuntimeAccessMode = "read"
	plans["work_ai_huoke_content"] = huoke
	huokeTopic := plans["work_ai_huoke_topic_strategy"]
	huokeTopic.RuntimeAccessMode = "read"
	plans["work_ai_huoke_topic_strategy"] = huokeTopic
	selfMedia := plans["work_ai_self_media_creation"]
	selfMedia.RuntimeAccessMode = "read"
	plans["work_ai_self_media_creation"] = selfMedia
	faya := plans["work_ai_faya_germination"]
	faya.PromptTemplateVersion = "v2.0.0"
	faya.OutputSchemaVersion = "viewpoint_germination.result.v2"
	faya.RuntimeAccessMode = "read"
	plans["work_ai_faya_germination"] = faya
	visual := plans["work_ai_visual_chat"]
	visual.RuntimeAccessMode = "read"
	plans["work_ai_visual_chat"] = visual
	legacyProjectionPlans := map[string]ProfilePlan{
		"feed_ai_chat": legacyProjectionPlan("feed_ai_chat", ScopeProductThread, "feed_ai_agent", "positioning_profile_builder", "feed_ai.positioning_profile_builder.v1"),
	}
	return AgentProfileResolver{plans: plans, legacyProjectionPlans: legacyProjectionPlans}
}

func (r AgentProfileResolver) Resolve(taskType string) (ProfilePlan, error) {
	return r.ResolveWithContext(taskType, map[string]any{})
}

func (r AgentProfileResolver) ResolveWithContext(taskType string, sourceContext map[string]any) (ProfilePlan, error) {
	if IsLegacyProjectionTaskType(taskType) {
		return ProfilePlan{}, fmt.Errorf(legacyProjectionOnlyErrorCode)
	}
	plan, ok := r.plans[taskType]
	if !ok {
		return ProfilePlan{}, fmt.Errorf("SKILL_UNAVAILABLE")
	}
	plan.RuntimeConfigID = r.RuntimeConfigFor(plan.SkillProfile, taskType)
	plan.TimeoutBudget = r.TimeoutBudgetForContext(taskType, sourceContext)
	if sourceContext != nil && sourceContext["runtimeAccessMode"] != nil {
		plan.RuntimeAccessMode = fmt.Sprint(sourceContext["runtimeAccessMode"])
	}
	return plan, nil
}

// ResolveLegacyProjectionProfile is deliberately limited to rendering or
// projecting an already persisted historical task. New AgentRun planning must
// use TaskIntent, L1 routing, and CapabilityPlanner instead.
func (r AgentProfileResolver) ResolveLegacyProjectionProfile(taskType string) (ProfilePlan, error) {
	taskType = strings.TrimSpace(taskType)
	if !IsLegacyProjectionTaskType(taskType) {
		return ProfilePlan{}, fmt.Errorf(legacyProjectionOnlyErrorCode)
	}
	plan, ok := r.legacyProjectionPlans[taskType]
	if !ok {
		return ProfilePlan{}, fmt.Errorf("SKILL_UNAVAILABLE")
	}
	return plan, nil
}

func (r AgentProfileResolver) MustMatchTaskType(taskType, skillProfile string) error {
	return r.AssertTaskSkillMatch(taskType, skillProfile)
}

func (r AgentProfileResolver) AssertTaskSkillMatch(taskType, skillProfile string) error {
	plan, err := r.Resolve(taskType)
	if err != nil {
		return err
	}
	if plan.SkillProfile != skillProfile {
		return fmt.Errorf("SKILL_TASK_MISMATCH")
	}
	return nil
}

func (r AgentProfileResolver) RuntimeConfigFor(skillProfile, taskType string) string {
	if IsLegacyProjectionTaskType(taskType) {
		return ""
	}
	switch taskType {
	case "work_ai_topic_generation":
		return "huahuo-topic-generation"
	case "work_ai_renshe_content":
		return "huahuo-renshe-content"
	case "work_ai_huoke_content":
		return "huahuo-huoke-content"
	case "work_ai_huoke_topic_strategy":
		return "huahuo-huoke-topic"
	case "work_ai_self_media_creation":
		return "huahuo-self-media-creation"
	case "work_ai_faya_germination":
		return "huahuo-faya-germination"
	case "work_ai_visual_chat":
		return "huahuo-visual-chat"
	case "minutes_generation", "summary_generation", "material_deposit_generation":
		return "huahuo-recording-postprocess"
	case "hotspot_home_suggestion":
		return "huahuo-high-thinking"
	default:
		return "huahuo-default"
	}
}

func (r AgentProfileResolver) TimeoutBudgetFor(taskType string) TimeoutBudget {
	if IsLegacyProjectionTaskType(taskType) {
		return TimeoutBudget{}
	}
	switch taskType {
	case "work_ai_topic_generation":
		return TimeoutBudget{BusinessTimeoutSec: runtimeProtectionWindowSec, MaxToolCalls: 16, MaxOutputChars: 6000}
	case "work_ai_renshe_content":
		return TimeoutBudget{BusinessTimeoutSec: runtimeProtectionWindowSec, MaxToolCalls: 80, MaxOutputChars: 12000}
	case "work_ai_huoke_content":
		return TimeoutBudget{BusinessTimeoutSec: runtimeProtectionWindowSec, MaxToolCalls: 12, MaxOutputChars: 12000}
	case "work_ai_huoke_topic_strategy":
		return TimeoutBudget{BusinessTimeoutSec: runtimeProtectionWindowSec, MaxToolCalls: 80, MaxOutputChars: 65536}
	case "work_ai_self_media_creation":
		return TimeoutBudget{BusinessTimeoutSec: runtimeProtectionWindowSec, MaxToolCalls: 80, MaxOutputChars: 16000}
	case "work_ai_faya_germination":
		return TimeoutBudget{BusinessTimeoutSec: runtimeProtectionWindowSec, MaxToolCalls: 80, MaxOutputChars: 16000}
	case "work_ai_visual_chat":
		return TimeoutBudget{BusinessTimeoutSec: runtimeProtectionWindowSec, MaxToolCalls: 8, MaxOutputChars: 12000}
	case "work_ai_general_chat":
		return TimeoutBudget{BusinessTimeoutSec: runtimeProtectionWindowSec, MaxToolCalls: 50, MaxOutputChars: 6000}
	case "profile_deposit":
		return TimeoutBudget{BusinessTimeoutSec: runtimeProtectionWindowSec, MaxToolCalls: 64, MaxOutputChars: 6000}
	case "minutes_generation":
		return TimeoutBudget{BusinessTimeoutSec: runtimeProtectionWindowSec, MaxToolCalls: 12, MaxOutputChars: 12000}
	case "summary_generation":
		return TimeoutBudget{BusinessTimeoutSec: runtimeProtectionWindowSec, MaxToolCalls: 12, MaxOutputChars: 12000}
	case "material_deposit_generation":
		return TimeoutBudget{BusinessTimeoutSec: runtimeProtectionWindowSec, MaxToolCalls: 12, MaxOutputChars: 16000}
	case "hotspot_home_suggestion":
		return TimeoutBudget{BusinessTimeoutSec: runtimeProtectionWindowSec, MaxToolCalls: 8, MaxOutputChars: 8000}
	default:
		return TimeoutBudget{BusinessTimeoutSec: runtimeProtectionWindowSec, MaxToolCalls: 8, MaxOutputChars: 6000}
	}
}

func (r AgentProfileResolver) TimeoutBudgetForContext(taskType string, sourceContext map[string]any) TimeoutBudget {
	budget := r.TimeoutBudgetFor(taskType)
	transcriptLength := resolverContextInt(sourceContext, "transcriptLength", "transcriptChars", "finalTranscriptLength")
	switch taskType {
	case "minutes_generation":
		if transcriptLength >= 120000 {
			return TimeoutBudget{BusinessTimeoutSec: runtimeProtectionWindowSec, MaxToolCalls: 40, MaxOutputChars: 32000}
		}
		if transcriptLength >= 60000 {
			return TimeoutBudget{BusinessTimeoutSec: runtimeProtectionWindowSec, MaxToolCalls: 28, MaxOutputChars: 28000}
		}
		if transcriptLength >= 12000 {
			return TimeoutBudget{BusinessTimeoutSec: runtimeProtectionWindowSec, MaxToolCalls: 20, MaxOutputChars: 20000}
		}
	case "summary_generation", "material_deposit_generation":
		if transcriptLength >= 60000 {
			return TimeoutBudget{BusinessTimeoutSec: runtimeProtectionWindowSec, MaxToolCalls: 12, MaxOutputChars: 16000}
		}
		if transcriptLength >= 12000 {
			return TimeoutBudget{BusinessTimeoutSec: runtimeProtectionWindowSec, MaxToolCalls: 12, MaxOutputChars: 14000}
		}
	}
	return budget
}

func resolverContextInt(values map[string]any, keys ...string) int {
	for _, key := range keys {
		if n := runtimeIntValue(values[key]); n > 0 {
			return n
		}
	}
	return 0
}

func plan(taskType string, scope ExecutionScope, agent, skill, prompt string) ProfilePlan {
	resolver := AgentProfileResolver{}
	return ProfilePlan{
		TaskType:              taskType,
		ExecutionScope:        scope,
		AgentProfile:          agent,
		SkillProfile:          skill,
		PromptTemplateID:      prompt,
		PromptTemplateVersion: "v0.1.0",
		OutputSchemaVersion:   skill + ".result.v1",
		RuntimeConfigID:       resolver.RuntimeConfigFor(skill, taskType),
		RuntimeAccessMode:     "write",
		MessageMode:           messageModeFor(taskType, scope),
		WorkspaceMode:         workspaceModeFor(taskType, scope),
		TimeoutBudget:         resolver.TimeoutBudgetFor(taskType),
	}
}

func legacyProjectionPlan(taskType string, scope ExecutionScope, agent, skill, prompt string) ProfilePlan {
	profile := plan(taskType, scope, agent, skill, prompt)
	profile.RuntimeConfigID = "huahuo-default"
	profile.TimeoutBudget = TimeoutBudget{BusinessTimeoutSec: runtimeProtectionWindowSec, MaxToolCalls: 500, MaxOutputChars: 12000}
	return profile
}

func messageModeFor(taskType string, scope ExecutionScope) MessageMode {
	switch {
	case scope == ScopeDetachedTask:
		return MessageModeDetachedTaskInstruction
	case taskType == "work_ai_topic_generation":
		return MessageModeProductTaskTurn
	default:
		return MessageModeUserTurn
	}
}

func workspaceModeFor(taskType string, scope ExecutionScope) WorkspaceMode {
	switch {
	case scope == ScopeDetachedTask:
		return WorkspaceModeDetachedRuntimeWorkspace
	case taskType == "work_ai_topic_generation":
		return WorkspaceModeFormalUserWorkspaceInputSnapshot
	default:
		return WorkspaceModeFormalUserWorkspace
	}
}
