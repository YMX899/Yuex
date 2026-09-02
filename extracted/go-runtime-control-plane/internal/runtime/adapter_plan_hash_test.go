package runtime

import (
	"strings"
	"testing"
)

func TestComputeAgentRunPlanHashBindsFullPlanIdentity(t *testing.T) {
	plan := AgentRunPlan{
		SchemaVersion: "agent_run_plan.v1", AgentRunID: "run_full_plan_hash", PlanVersion: 1,
		RoutingMode: "dynamic", TaskType: "work_ai_visual_chat", ExecutionScope: "product_thread",
		L1AgentProfile: "visual_chat_agent", AgentRelativeRoot: "agents/visual_chat_agent",
		RuntimeConfigID: "huahuo-visual-chat", AgentHash: "sha256:" + strings.Repeat("a", 64),
		ManifestVersion: "l1-agent-manifest.v2", ToolPolicyProfile: "workspace_read_only",
		SelectedSkillProfiles: []string{"visual_chat_assistant"}, SelectedKnowledgeRefs: []string{},
		RequiredTools: []string{"read"}, OutputContract: map[string]any{"format": "text"},
		TerminalOutput: AgentRunTerminalOutputIdentity{TaskType: "work_ai_visual_chat", L1AgentProfile: "visual_chat_agent", SkillProfile: "visual_chat_assistant", PromptTemplateID: "work_ai.visual_chat.v1", PromptTemplateVersion: "v0.1.0", OutputSchemaVersion: "visual_chat.result.v1", Format: "text"},
		WriteMode:      "none", ToolBudget: defaultRuntimeToolBudget(), SafePlanSummary: "visual chat",
		WorkspaceVersion: 1, IndexVersion: 0, WorkspaceContextManifestHash: "sha256:" + strings.Repeat("b", 64),
		CapabilityHash: "capability_v05", MetaWorkspaceKey: "visual_chat", MetaWorkspaceVersion: "v1",
		InputPolicyHash: "sha256:" + strings.Repeat("c", 64),
		InputAttachments: []AgentRunInputAttachmentIdentity{
			{ResourceID: "attachment_1", Usage: "primary_input", MIMEType: "image/png", SHA256: "sha256:" + strings.Repeat("d", 64)},
			{ResourceID: "attachment_2", Usage: "reference", MIMEType: "image/jpeg", SHA256: "sha256:" + strings.Repeat("e", 64)},
		},
	}

	before, err := ComputeAgentRunPlanHash(plan)
	if err != nil {
		t.Fatal(err)
	}
	mutations := []struct {
		name  string
		apply func(*AgentRunPlan)
	}{
		{
			name: "meta workspace key",
			apply: func(candidate *AgentRunPlan) {
				candidate.MetaWorkspaceKey = "general_chat"
			},
		},
		{
			name: "meta workspace version",
			apply: func(candidate *AgentRunPlan) {
				candidate.MetaWorkspaceVersion = "v2"
			},
		},
		{
			name: "input policy hash",
			apply: func(candidate *AgentRunPlan) {
				candidate.InputPolicyHash = "sha256:" + strings.Repeat("d", 64)
			},
		},
		{
			name: "input attachment identity",
			apply: func(candidate *AgentRunPlan) {
				candidate.InputAttachments = append([]AgentRunInputAttachmentIdentity(nil), candidate.InputAttachments...)
				candidate.InputAttachments[0].ResourceID = "attachment_changed"
			},
		},
		{
			name: "input attachment order",
			apply: func(candidate *AgentRunPlan) {
				candidate.InputAttachments = append([]AgentRunInputAttachmentIdentity(nil), candidate.InputAttachments...)
				candidate.InputAttachments[0], candidate.InputAttachments[1] = candidate.InputAttachments[1], candidate.InputAttachments[0]
			},
		},
		{
			name: "agent relative root",
			apply: func(candidate *AgentRunPlan) {
				candidate.AgentRelativeRoot = "agents/other_visual_agent"
			},
		},
		{
			name: "terminal output identity",
			apply: func(candidate *AgentRunPlan) {
				candidate.TerminalOutput.Format = "markdown"
			},
		},
		{
			name: "workspace context manifest hash",
			apply: func(candidate *AgentRunPlan) {
				candidate.WorkspaceContextManifestHash = "sha256:" + strings.Repeat("f", 64)
			},
		},
	}
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			candidate := plan
			mutation.apply(&candidate)
			got, err := ComputeAgentRunPlanHash(candidate)
			if err != nil {
				t.Fatal(err)
			}
			if got == before {
				t.Fatalf("full plan hash did not bind %s: %s", mutation.name, got)
			}
		})
	}
}

func TestComputeAgentRunPlanHashForStableV05AdapterExcludesUnsupportedFields(t *testing.T) {
	plan := AgentRunPlan{
		SchemaVersion: "agent_run_plan.v1", AgentRunID: "run_adapter_v05", PlanVersion: 1,
		RoutingMode: "dynamic", TaskType: "work_ai_visual_chat", ExecutionScope: "product_thread",
		L1AgentProfile: "visual_chat_agent", AgentRelativeRoot: "agents/visual_chat_agent",
		RuntimeConfigID: "huahuo-visual-chat", AgentHash: "sha256:" + strings.Repeat("a", 64),
		ManifestVersion: "l1-agent-manifest.v2", ToolPolicyProfile: "workspace_read_only",
		SelectedSkillProfiles: []string{"visual_chat_assistant"}, SelectedKnowledgeRefs: []string{},
		RequiredTools: []string{"read"}, OutputContract: map[string]any{"format": "text"},
		TerminalOutput: AgentRunTerminalOutputIdentity{TaskType: "work_ai_visual_chat", L1AgentProfile: "visual_chat_agent", SkillProfile: "visual_chat_assistant", PromptTemplateID: "work_ai.visual_chat.v1", PromptTemplateVersion: "v0.1.0", OutputSchemaVersion: "visual_chat.result.v1", Format: "text"},
		WriteMode:      "none", ToolBudget: defaultRuntimeToolBudget(), SafePlanSummary: "visual chat",
		WorkspaceVersion: 1, IndexVersion: 0, WorkspaceContextManifestHash: "sha256:" + strings.Repeat("b", 64),
		CapabilityHash: "capability_v05", MetaWorkspaceKey: "visual_chat", MetaWorkspaceVersion: "v1",
		InputPolicyHash: "sha256:" + strings.Repeat("c", 64),
	}

	compatBefore, err := ComputeAgentRunPlanHashForStableV05Adapter(plan)
	if err != nil {
		t.Fatal(err)
	}
	fullBefore, err := ComputeAgentRunPlanHash(plan)
	if err != nil {
		t.Fatal(err)
	}

	plan.MetaWorkspaceVersion = "v2"
	plan.InputPolicyHash = "sha256:" + strings.Repeat("d", 64)
	plan.InputAttachments = []AgentRunInputAttachmentIdentity{{ResourceID: "attachment_1", Usage: "primary_input", MIMEType: "image/png", SHA256: "sha256:" + strings.Repeat("e", 64)}}
	plan.AgentRelativeRoot = "agents/other_visual_agent"
	plan.TerminalOutput.Format = "markdown"
	plan.WorkspaceContextManifestHash = "sha256:" + strings.Repeat("f", 64)
	compatAfter, err := ComputeAgentRunPlanHashForStableV05Adapter(plan)
	if err != nil {
		t.Fatal(err)
	}
	fullAfter, err := ComputeAgentRunPlanHash(plan)
	if err != nil {
		t.Fatal(err)
	}
	if compatBefore != compatAfter {
		t.Fatalf("stable v0.5 hash changed for fields its Adapter cannot decode: before=%s after=%s", compatBefore, compatAfter)
	}
	if fullBefore == fullAfter {
		t.Fatal("full plan hash must bind server-side Meta and attachment fields")
	}
}
