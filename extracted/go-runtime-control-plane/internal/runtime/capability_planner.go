package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path"
	"sort"
	"strings"

	"huahuoai/backend/source/internal/domain"
)

type RuntimeToolBudget struct {
	MaxToolCalls        int `json:"maxToolCalls"`
	SoftToolCallLimit   int `json:"softToolCallLimit"`
	FinalizationReserve int `json:"finalizationReserve"`
	MaxRepeatedCalls    int `json:"maxRepeatedCalls"`
	MaxNoProgressCalls  int `json:"maxNoProgressCalls"`
	MaxSearchCalls      int `json:"maxSearchCalls"`
	MaxWriteCalls       int `json:"maxWriteCalls"`
	MaxReadBytes        int `json:"maxReadBytes"`
	MaxWallTimeSeconds  int `json:"maxWallTimeSeconds"`
}

type AgentRunPlan struct {
	SchemaVersion         string                            `json:"schemaVersion"`
	AgentRunID            string                            `json:"agentRunId"`
	PlanVersion           int                               `json:"planVersion"`
	RoutingMode           string                            `json:"routingMode"`
	TaskType              string                            `json:"taskType"`
	ExecutionScope        string                            `json:"executionScope"`
	L1AgentProfile        string                            `json:"l1AgentProfile"`
	MetaWorkspaceKey      string                            `json:"metaWorkspaceKey,omitempty"`
	MetaWorkspaceVersion  string                            `json:"metaWorkspaceVersion,omitempty"`
	InputPolicyHash       string                            `json:"inputPolicyHash,omitempty"`
	InputAttachments      []AgentRunInputAttachmentIdentity `json:"inputAttachments,omitempty"`
	AgentRelativeRoot     string                            `json:"agentRelativeRoot,omitempty"`
	RuntimeConfigID       string                            `json:"runtimeConfigId"`
	AgentHash             string                            `json:"agentHash"`
	ManifestVersion       string                            `json:"manifestVersion"`
	ToolPolicyProfile     string                            `json:"toolPolicyProfile"`
	SelectedSkillProfiles []string                          `json:"selectedSkillProfiles"`
	SelectedKnowledgeRefs []string                          `json:"selectedKnowledgeRefs"`
	RequiredTools         []string                          `json:"requiredTools"`
	OutputContract        map[string]any                    `json:"outputContract"`
	TerminalOutput        AgentRunTerminalOutputIdentity    `json:"terminalOutput"`
	WriteMode             string                            `json:"writeMode"`
	ToolBudget            RuntimeToolBudget                 `json:"toolBudget"`
	RequiresConfirmation  bool                              `json:"requiresConfirmation"`
	SafePlanSummary       string                            `json:"safePlanSummary"`
	WorkspaceVersion      int64                             `json:"workspaceVersion"`
	IndexVersion          int64                             `json:"indexVersion"`
	// WorkspaceContextManifestHash binds this Plan to the immutable
	// RunWorkspaceContext frozen before planning succeeds. It is assigned by
	// the planning worker, not by a caller or the model.
	WorkspaceContextManifestHash string `json:"workspaceContextManifestHash"`
	CapabilityHash               string `json:"capabilityHash"`
	ModelProfileID               string `json:"-"`
	AuthPoolID                   string `json:"-"`
	RootPath                     string `json:"-"`
}

// AgentRunTerminalOutputIdentity is the frozen parser/writeback identity for
// one Run. AiTask.taskType is an APP projection and is deliberately absent.
// A plan can materialize more than one Skill, but it must name the one Skill
// that produced the terminal output so a terminal event cannot guess a parser.
type AgentRunTerminalOutputIdentity struct {
	TaskType              string `json:"taskType"`
	L1AgentProfile        string `json:"l1AgentProfile"`
	SkillProfile          string `json:"skillProfile"`
	PromptTemplateID      string `json:"promptTemplateId"`
	PromptTemplateVersion string `json:"promptTemplateVersion"`
	OutputSchemaVersion   string `json:"outputSchemaVersion"`
	Format                string `json:"format"`
}

// AgentRunPlanFromSnapshot decodes the immutable persisted plan selected by a
// Runtime dispatch. It does not infer a replacement from an AiTask, scene or
// source surface.
func AgentRunPlanFromSnapshot(snapshot map[string]any) (AgentRunPlan, error) {
	if len(snapshot) == 0 {
		return AgentRunPlan{}, domain.ErrorCode("AGENT_PLAN_INVALID")
	}
	raw, err := json.Marshal(snapshot)
	if err != nil {
		return AgentRunPlan{}, domain.ErrorCode("AGENT_PLAN_INVALID")
	}
	var plan AgentRunPlan
	if err := json.Unmarshal(raw, &plan); err != nil {
		return AgentRunPlan{}, domain.ErrorCode("AGENT_PLAN_INVALID")
	}
	return plan, nil
}

// TerminalOutputProfile returns the exact parser identity recorded during
// planning. It intentionally does not call AgentProfileResolver: resolving a
// task type at terminal time would reintroduce a mutable routing decision.
func (p AgentRunPlan) TerminalOutputProfile(expectedRunID string) (ProfilePlan, error) {
	if strings.TrimSpace(expectedRunID) == "" || strings.TrimSpace(p.AgentRunID) != strings.TrimSpace(expectedRunID) ||
		p.SchemaVersion != "agent_run_plan.v1" || p.PlanVersion < 1 ||
		p.ExecutionScope != string(ScopeProductThread) && p.ExecutionScope != string(ScopeDetachedTask) {
		return ProfilePlan{}, domain.ErrorCode("AGENT_PLAN_INVALID")
	}
	if err := ValidateAgentRunTerminalOutputIdentity(p); err != nil {
		return ProfilePlan{}, err
	}
	identity := p.TerminalOutput
	scope := ExecutionScope(p.ExecutionScope)
	return ProfilePlan{
		TaskType:              identity.TaskType,
		ExecutionScope:        scope,
		AgentProfile:          identity.L1AgentProfile,
		SkillProfile:          identity.SkillProfile,
		PromptTemplateID:      identity.PromptTemplateID,
		PromptTemplateVersion: identity.PromptTemplateVersion,
		OutputSchemaVersion:   identity.OutputSchemaVersion,
		RuntimeConfigID:       p.RuntimeConfigID,
		MessageMode:           messageModeFor(identity.TaskType, scope),
		WorkspaceMode:         workspaceModeFor(identity.TaskType, scope),
	}, nil
}

type WorkspaceIndexSummary struct {
	WorkspaceVersion int64  `json:"workspaceVersion"`
	IndexVersion     int64  `json:"indexVersion"`
	Status           string `json:"status"`
}

type PlannedSkill struct {
	SkillProfile         string   `json:"skillProfile"`
	Status               string   `json:"status"`
	Hash                 string   `json:"hash"`
	TaskTypes            []string `json:"taskTypes"`
	IntentCategories     []string `json:"intentCategories"`
	AllowedAgentProfiles []string `json:"allowedAgentProfiles"`
	RequiredCapabilities []string `json:"requiredCapabilities"`
	KnowledgeRefs        []string `json:"knowledgeRefs"`
	Priority             int      `json:"priority"`
}

type ToolCapability struct {
	Name          string `json:"name"`
	Status        string `json:"status"`
	Source        string `json:"source"`
	PluginID      string `json:"pluginId"`
	PluginVersion string `json:"pluginVersion"`
	SchemaID      string `json:"schemaId"`
	SchemaHash    string `json:"schemaHash"`
}

const (
	RuntimeToolSourceOpenClawCore = "openclaw_core"
	RuntimeToolSourcePlugin       = "plugin"

	RuntimeCoreToolsPluginID      = "openclaw-core"
	RuntimeCoreToolsPluginVersion = "2026.6.2"
	RuntimeCoreReadSchemaID       = "openclaw.core.read.v2026.6.2"
	RuntimeCoreReadSchemaHash     = "sha256:134f19bcabe3e29d63c5cebb38f1d2556759fd08adad6bc90a4b4d3cd1fb8441"
	RuntimeCoreWriteSchemaID      = "openclaw.core.write.v2026.6.2"
	RuntimeCoreWriteSchemaHash    = "sha256:e98a2484f667cf7c22d76ca103bf2022bf9113dc63fe38b899e71c328cb1e833"

	RuntimeWorkspaceSearchPluginID      = "huahuo-context-tools"
	RuntimeWorkspaceSearchPluginVersion = "0.5.0"
	RuntimeWorkspaceSearchSchemaID      = "huahuo.workspace_search.v1"
	RuntimeWorkspaceSearchSchemaHash    = "sha256:0cb790780b9b8d1538d54dc309e4377e30c885aff21c2027748b9efefdb20d80"
)

type runtimeToolContract struct {
	Source        string
	PluginID      string
	PluginVersion string
	SchemaID      string
	SchemaHash    string
}

func runtimeToolContractFor(name string) (runtimeToolContract, bool) {
	switch strings.TrimSpace(name) {
	case "read":
		return runtimeToolContract{
			Source: RuntimeToolSourceOpenClawCore, PluginID: RuntimeCoreToolsPluginID, PluginVersion: RuntimeCoreToolsPluginVersion,
			SchemaID: RuntimeCoreReadSchemaID, SchemaHash: RuntimeCoreReadSchemaHash,
		}, true
	case "write":
		return runtimeToolContract{
			Source: RuntimeToolSourceOpenClawCore, PluginID: RuntimeCoreToolsPluginID, PluginVersion: RuntimeCoreToolsPluginVersion,
			SchemaID: RuntimeCoreWriteSchemaID, SchemaHash: RuntimeCoreWriteSchemaHash,
		}, true
	case "workspace_search":
		return runtimeToolContract{
			Source: RuntimeToolSourcePlugin, PluginID: RuntimeWorkspaceSearchPluginID, PluginVersion: RuntimeWorkspaceSearchPluginVersion,
			SchemaID: RuntimeWorkspaceSearchSchemaID, SchemaHash: RuntimeWorkspaceSearchSchemaHash,
		}, true
	default:
		return runtimeToolContract{}, false
	}
}

// CanonicalAgentFacingToolCapability returns the immutable semantic-tool
// identity expected from a RuntimeHost. It is useful for local Host fixtures;
// production Hosts must still obtain the record from their live handshake.
func CanonicalAgentFacingToolCapability(name, status string) ToolCapability {
	contract, ok := runtimeToolContractFor(name)
	if !ok {
		return ToolCapability{Name: name, Status: status}
	}
	capability := ToolCapability{
		Name: name, Status: status, Source: contract.Source, PluginID: contract.PluginID,
		PluginVersion: contract.PluginVersion, SchemaID: contract.SchemaID,
	}
	if status == "ready" {
		capability.SchemaHash = contract.SchemaHash
	}
	return capability
}

// IsAgentFacingRuntimeTool defines the complete Runtime tool vocabulary that
// may enter a signed AgentRun plan or a RuntimeHost capability snapshot. Raw
// filesystem commands remain Host/Search-Service implementation details.
func IsAgentFacingRuntimeTool(name string) bool {
	switch strings.TrimSpace(name) {
	case "read", "write", "workspace_search":
		return true
	default:
		return false
	}
}

// ValidateAgentFacingRuntimeTools validates the untrusted capability snapshot
// received from a RuntimeHost before Backend can use it for planning.
func ValidateAgentFacingRuntimeTools(tools []ToolCapability) error {
	seen := map[string]bool{}
	for _, tool := range tools {
		name := strings.TrimSpace(tool.Name)
		if !IsAgentFacingRuntimeTool(name) || seen[name] {
			return domain.ErrorCode("RUNTIME_TOOL_UNAVAILABLE")
		}
		seen[name] = true
		contract, ok := runtimeToolContractFor(name)
		if !ok || strings.TrimSpace(tool.Source) != contract.Source || strings.TrimSpace(tool.PluginID) != contract.PluginID ||
			strings.TrimSpace(tool.PluginVersion) != contract.PluginVersion || strings.TrimSpace(tool.SchemaID) != contract.SchemaID {
			return domain.ErrorCode("RUNTIME_TOOL_UNAVAILABLE")
		}
		switch strings.TrimSpace(tool.Status) {
		case "ready":
			if strings.TrimSpace(tool.SchemaHash) != contract.SchemaHash {
				return domain.ErrorCode("RUNTIME_TOOL_UNAVAILABLE")
			}
		case "degraded":
			// Degraded optional tools stay visible so planning can fail closed for
			// requests that require them. A static schema hash cannot turn a
			// degraded capability into a registered runtime tool.
			if strings.TrimSpace(tool.SchemaHash) != "" && strings.TrimSpace(tool.SchemaHash) != contract.SchemaHash {
				return domain.ErrorCode("RUNTIME_TOOL_UNAVAILABLE")
			}
		default:
			return domain.ErrorCode("RUNTIME_TOOL_UNAVAILABLE")
		}
	}
	return nil
}

func runtimeToolCapabilityReady(tool ToolCapability) bool {
	contract, ok := runtimeToolContractFor(tool.Name)
	return ok && tool.Status == "ready" && strings.TrimSpace(tool.Source) == contract.Source &&
		strings.TrimSpace(tool.PluginID) == contract.PluginID && strings.TrimSpace(tool.PluginVersion) == contract.PluginVersion &&
		strings.TrimSpace(tool.SchemaID) == contract.SchemaID && strings.TrimSpace(tool.SchemaHash) == contract.SchemaHash
}

type RuntimeCapabilitySnapshot struct {
	CapabilityHash        string                             `json:"capabilityHash"`
	Tools                 []ToolCapability                   `json:"tools"`
	SubmitBinding         RuntimeSubmitBindingCapability     `json:"submitBinding"`
	MaxToolCallsSupported int                                `json:"maxToolCallsSupported"`
	SupportsPerRunBudget  bool                               `json:"supportsPerRunBudget"`
	SupportsBudgetWarning bool                               `json:"supportsBudgetWarning"`
	SupportsForcedAbort   bool                               `json:"supportsForcedAbort"`
	BudgetExecution       RuntimeToolBudgetExecutionContract `json:"budgetExecution"`
}

type PlannerPermissionSnapshot struct {
	AllowedAgents    map[string]bool `json:"allowedAgents"`
	AllowedSkills    map[string]bool `json:"allowedSkills"`
	AllowedKnowledge map[string]bool `json:"allowedKnowledge"`
	AllowFormalWrite bool            `json:"allowFormalWrite"`
}

type CapabilityPlanner struct {
	skills      []PlannedSkill
	permissions PlannerPermissionSnapshot
}

func NewCapabilityPlanner() CapabilityPlanner { return CapabilityPlanner{} }

// NewCapabilityPlannerWithCatalog constructs the normal planning path. The
// catalog is server-owned: neither App nor model output can supply a Skill,
// permission, knowledge path, provider, credential, or tool allow-list.
func NewCapabilityPlannerWithCatalog(skills []PlannedSkill, permissions PlannerPermissionSnapshot) CapabilityPlanner {
	return CapabilityPlanner{
		skills:      clonePlannedSkills(skills),
		permissions: clonePlannerPermissions(permissions),
	}
}

func (p CapabilityPlanner) Plan(ctx context.Context, intent domain.TaskIntent, route L1AgentRouteResult, index WorkspaceIndexSummary) (AgentRunPlan, error) {
	if err := ctx.Err(); err != nil {
		return AgentRunPlan{}, err
	}
	return p.buildDynamicPlan(intent, route, index)
}

func (p CapabilityPlanner) buildDynamicPlan(intent domain.TaskIntent, route L1AgentRouteResult, index WorkspaceIndexSummary) (AgentRunPlan, error) {
	if isLegacyFeedAIPlanningIdentity(intent.ResolvedTaskType, route.AgentProfile) {
		return AgentRunPlan{}, domain.ErrorCode("AGENT_PLAN_INVALID")
	}
	if err := ValidateL1AgentRouteMetaWorkspaceIdentity(route); err != nil {
		return AgentRunPlan{}, err
	}
	if intent.ResolvedTaskType == "" || route.AgentProfile == "" || route.AgentHash == "" || route.ManifestVersion == "" ||
		!safeAgentPackageRelativeRoot(route.RelativeRoot) || route.MaxCandidateSkills < 1 || route.MaxCandidateSkills > 8 ||
		!safeCatalogIdentifier(route.ToolPolicyProfile) {
		return AgentRunPlan{}, domain.ErrorCode("AGENT_PLAN_INVALID")
	}
	if len(p.skills) == 0 {
		return AgentRunPlan{}, domain.ErrorCode("SKILL_UNAVAILABLE")
	}
	if !p.permissions.AllowedAgents[route.AgentProfile] {
		return AgentRunPlan{}, domain.ErrorCode("AGENT_PROFILE_UNAVAILABLE")
	}
	selectedSkills, err := p.selectDynamicSkills(intent, route)
	if err != nil {
		return AgentRunPlan{}, err
	}
	selectedKnowledge, err := p.selectKnowledgeRefs(route, selectedSkills)
	if err != nil {
		return AgentRunPlan{}, err
	}
	return newAgentRunPlan(intent, route, "dynamic", selectedSkillProfiles(selectedSkills), selectedKnowledge, index)
}

func (CapabilityPlanner) BuildDeterministicPlan(intent domain.TaskIntent, route L1AgentRouteResult) (AgentRunPlan, error) {
	if isLegacyFeedAIPlanningIdentity(intent.ResolvedTaskType, route.AgentProfile) {
		return AgentRunPlan{}, domain.ErrorCode("AGENT_PLAN_INVALID")
	}
	if err := ValidateL1AgentRouteMetaWorkspaceIdentity(route); err != nil {
		return AgentRunPlan{}, err
	}
	if intent.ResolvedTaskType == "" || route.AgentProfile == "" || route.AgentHash == "" {
		return AgentRunPlan{}, domain.ErrorCode("AGENT_PLAN_INVALID")
	}
	skills := skillsForIntent(intent)
	if len(skills) == 0 {
		return AgentRunPlan{}, domain.ErrorCode("SKILL_UNAVAILABLE")
	}
	return newAgentRunPlan(intent, route, "deterministic", skills, []string{}, WorkspaceIndexSummary{})
}

func newAgentRunPlan(intent domain.TaskIntent, route L1AgentRouteResult, routingMode string, skills, knowledge []string, index WorkspaceIndexSummary) (AgentRunPlan, error) {
	if isLegacyFeedAIPlanningIdentity(intent.ResolvedTaskType, route.AgentProfile) {
		return AgentRunPlan{}, domain.ErrorCode("AGENT_PLAN_INVALID")
	}
	if err := ValidateL1AgentRouteMetaWorkspaceIdentity(route); err != nil {
		return AgentRunPlan{}, err
	}
	if len(skills) == 0 {
		return AgentRunPlan{}, domain.ErrorCode("SKILL_UNAVAILABLE")
	}
	outputContract := outputContractForIntent(intent)
	terminalOutput, err := newAgentRunTerminalOutputIdentity(intent.ResolvedTaskType, route.AgentProfile, skills[0], strings.TrimSpace(fmt.Sprint(outputContract["format"])))
	if err != nil {
		return AgentRunPlan{}, err
	}
	runtimeConfigID := NewAgentProfileResolver().RuntimeConfigFor("", intent.ResolvedTaskType)
	if strings.TrimSpace(runtimeConfigID) == "" {
		return AgentRunPlan{}, domain.ErrorCode("AGENT_PLAN_INVALID")
	}
	plan := AgentRunPlan{
		SchemaVersion: "agent_run_plan.v1", AgentRunID: intent.AgentRunID, PlanVersion: 1,
		RoutingMode: routingMode, TaskType: intent.ResolvedTaskType,
		ExecutionScope: intent.ExecutionScope,
		L1AgentProfile: route.AgentProfile, MetaWorkspaceKey: route.MetaWorkspaceKey,
		MetaWorkspaceVersion: route.MetaWorkspaceVersion, InputPolicyHash: route.InputPolicyHash,
		InputAttachments: []AgentRunInputAttachmentIdentity{}, AgentRelativeRoot: route.RelativeRoot,
		RuntimeConfigID: runtimeConfigID, AgentHash: route.AgentHash, ManifestVersion: route.ManifestVersion,
		ToolPolicyProfile:     route.ToolPolicyProfile,
		SelectedSkillProfiles: append([]string(nil), skills...), SelectedKnowledgeRefs: append([]string(nil), knowledge...),
		RequiredTools: toolsForIntent(intent), OutputContract: outputContract, TerminalOutput: terminalOutput,
		WriteMode: writeModeForIntent(intent), ToolBudget: runtimeToolBudgetForIntent(intent),
		RequiresConfirmation: intent.RequiresConfirmation,
		SafePlanSummary:      fmt.Sprintf("%s via %s", intent.Category, route.AgentProfile),
		WorkspaceVersion:     index.WorkspaceVersion, IndexVersion: index.IndexVersion,
	}
	sort.Strings(plan.RequiredTools)
	return plan, nil
}

func (p CapabilityPlanner) selectDynamicSkills(intent domain.TaskIntent, route L1AgentRouteResult) ([]PlannedSkill, error) {
	registry := NewSkillRegistryWithCandidateCatalog(SkillCandidateCatalog{
		Skills: p.skills, Permissions: p.permissions,
		Policies: []SkillCandidatePolicy{{
			AgentProfile: route.AgentProfile, CandidateSkillProfiles: route.CandidateSkillProfiles,
			MaxCandidateSkills: route.MaxCandidateSkills,
		}},
	})
	candidates, err := registry.ListCandidates(route.AgentProfile, intent)
	if err != nil {
		return nil, err
	}
	return registry.ValidateSelection(route.AgentProfile, intent, selectedSkillProfiles(candidates))
}

func (p CapabilityPlanner) selectKnowledgeRefs(route L1AgentRouteResult, skills []PlannedSkill) ([]string, error) {
	for _, root := range route.KnowledgeRoots {
		if !safeManifestRelativePath(root) {
			return nil, domain.ErrorCode("AGENT_PLAN_INVALID")
		}
	}
	refs := []string{}
	for _, skill := range skills {
		for _, ref := range skill.KnowledgeRefs {
			if !knowledgeReferenceAllowed(route.KnowledgeRoots, ref) || !p.permissions.AllowedKnowledge[ref] {
				return nil, domain.ErrorCode("AGENT_PLAN_INVALID")
			}
			refs = appendUnique(refs, ref)
		}
	}
	// KnowledgeRoots are an authorization boundary, not an instruction to copy
	// an entire methodology tree into every Run. The catalog must name each
	// bounded navigation entry (normally INDEX.md or OVERVIEW.md) that a Skill
	// actually needs. A Skill with no declared reference receives no implicit
	// root-directory fallback.
	sort.Strings(refs)
	return refs, nil
}

func (CapabilityPlanner) Validate(plan AgentRunPlan, manifest L1AgentManifest, skills []PlannedSkill, capabilities RuntimeCapabilitySnapshot, permissions PlannerPermissionSnapshot) error {
	if isLegacyFeedAIPlanningIdentity(plan.TaskType, plan.L1AgentProfile) {
		return domain.ErrorCode("AGENT_PLAN_INVALID")
	}
	if err := ValidateMetaWorkspaceIdentityTriple(plan.MetaWorkspaceKey, plan.MetaWorkspaceVersion, plan.InputPolicyHash); err != nil {
		return err
	}
	if plan.MetaWorkspaceKey == "" && len(plan.InputAttachments) != 0 {
		return domain.ErrorCode("AGENT_PLAN_INVALID")
	}
	if plan.SchemaVersion != "agent_run_plan.v1" || plan.PlanVersion < 1 || plan.TaskType == "" || plan.ExecutionScope == "" || plan.L1AgentProfile == "" || plan.RuntimeConfigID == "" || plan.AgentHash == "" ||
		plan.WorkspaceVersion < 1 || plan.CapabilityHash == "" || plan.CapabilityHash != capabilities.CapabilityHash {
		return domain.ErrorCode("AGENT_PLAN_INVALID")
	}
	if plan.RoutingMode != "dynamic" && plan.RoutingMode != "deterministic" {
		return domain.ErrorCode("AGENT_PLAN_INVALID")
	}
	if plan.RoutingMode == "dynamic" && !safeAgentPackageRelativeRoot(plan.AgentRelativeRoot) {
		return domain.ErrorCode("AGENT_PLAN_INVALID")
	}
	if plan.ModelProfileID != "" || plan.AuthPoolID != "" || plan.RootPath != "" {
		return domain.ErrorCode("AGENT_PLAN_INVALID")
	}
	agent, ok := activeAgentEntry(manifest, plan)
	if !permissions.AllowedAgents[plan.L1AgentProfile] || !ok {
		return domain.ErrorCode("AGENT_PROFILE_UNAVAILABLE")
	}
	maxSkills := 8
	if plan.RoutingMode == "dynamic" {
		if !safeAgentPackageRelativeRoot(agent.RelativeRoot) || plan.AgentRelativeRoot != agent.RelativeRoot || !safeCatalogIdentifier(plan.ToolPolicyProfile) ||
			agent.MaxCandidateSkills < 1 || agent.MaxCandidateSkills > 8 ||
			!safeCatalogIdentifier(agent.ToolPolicyProfile) || plan.ToolPolicyProfile != agent.ToolPolicyProfile ||
			((agent.MetaWorkspaceKey != "" || plan.MetaWorkspaceKey != "") &&
				(!safeCatalogIdentifier(agent.MetaWorkspaceKey) || plan.MetaWorkspaceKey != agent.MetaWorkspaceKey ||
					plan.MetaWorkspaceVersion != agent.Version || plan.InputPolicyHash != agent.InputPolicyHash ||
					ValidateMetaWorkspaceInputPolicy(agent.InputPolicy, plan.InputPolicyHash) != nil)) {
			return domain.ErrorCode("AGENT_PLAN_INVALID")
		}
		maxSkills = agent.MaxCandidateSkills
		if err := ValidateAgentRunInputAttachments(plan.InputAttachments, agent.InputPolicy); err != nil {
			return err
		}
	} else if len(plan.InputAttachments) != 0 {
		return domain.ErrorCode("AGENT_PLAN_INVALID")
	}
	if len(plan.SelectedSkillProfiles) == 0 || len(plan.SelectedSkillProfiles) > maxSkills {
		return domain.ErrorCode("AGENT_PLAN_INVALID")
	}
	seenSkills := map[string]bool{}
	for _, profile := range plan.SelectedSkillProfiles {
		if seenSkills[profile] || !permissions.AllowedSkills[profile] || !activeSkillMatches(skills, profile, plan.TaskType, plan.L1AgentProfile) ||
			(plan.RoutingMode == "dynamic" && !containsExact(agent.CandidateSkillProfiles, profile)) {
			return domain.ErrorCode("SKILL_UNAVAILABLE")
		}
		seenSkills[profile] = true
	}
	seenKnowledge := map[string]bool{}
	for _, ref := range plan.SelectedKnowledgeRefs {
		if seenKnowledge[ref] || !safeManifestRelativePath(ref) || !permissions.AllowedKnowledge[ref] ||
			(plan.RoutingMode == "dynamic" && !knowledgeReferenceAllowed(agent.KnowledgeRoots, ref)) {
			return domain.ErrorCode("AGENT_PLAN_INVALID")
		}
		seenKnowledge[ref] = true
	}
	if plan.WriteMode == "asset_write_intent" && !permissions.AllowFormalWrite {
		return domain.ErrorCode("RUNTIME_PERMISSION_DENIED")
	}
	if err := ValidateAgentRunToolPolicy(plan); err != nil {
		return err
	}
	if err := ValidateRuntimePlanAvailability(capabilities, plan.RequiredTools, plan.ToolBudget); err != nil {
		return err
	}
	if !registeredOutputContract(plan.OutputContract) || plan.WriteMode == "" {
		return domain.ErrorCode("AGENT_PLAN_INVALID")
	}
	if err := ValidateAgentRunTerminalOutputIdentity(plan); err != nil {
		return err
	}
	return nil
}

func safeAgentPackageRelativeRoot(value string) bool {
	value = strings.TrimSpace(value)
	return safeManifestRelativePath(value) && strings.HasPrefix(value, "agents/") && strings.TrimPrefix(value, "agents/") != ""
}

// ValidateRuntimePlanAvailability only accepts tools that a live RuntimeHost
// reported ready with a schema hash. Static planning catalogs describe a
// contract; they cannot establish this runtime availability fact.
func ValidateRuntimePlanAvailability(capabilities RuntimeCapabilitySnapshot, requiredTools []string, budget RuntimeToolBudget) error {
	if budget.MaxToolCalls <= 0 {
		return domain.ErrorCode("RUNTIME_TOOL_BUDGET_UNSUPPORTED")
	}
	if err := ValidateRuntimeSubmitBindingCapability(capabilities.SubmitBinding); err != nil {
		return err
	}
	if err := validateRequiredRuntimeTools(requiredTools); err != nil {
		return err
	}
	if err := ValidateAgentFacingRuntimeTools(capabilities.Tools); err != nil {
		return err
	}
	readyTools := map[string]bool{}
	for _, tool := range capabilities.Tools {
		readyTools[tool.Name] = runtimeToolCapabilityReady(tool)
	}
	for _, required := range requiredTools {
		if !IsAgentFacingRuntimeTool(required) || !readyTools[required] {
			return domain.ErrorCode("RUNTIME_TOOL_UNAVAILABLE")
		}
	}
	if !capabilities.SupportsPerRunBudget || !capabilities.SupportsBudgetWarning || !capabilities.SupportsForcedAbort ||
		budget.MaxToolCalls > capabilities.MaxToolCallsSupported ||
		budget.MaxToolCalls > capabilities.BudgetExecution.HardMaxToolCalls {
		return domain.ErrorCode("RUNTIME_TOOL_BUDGET_UNSUPPORTED")
	}
	if err := ValidateRuntimeToolBudgetExecutionContract(capabilities.BudgetExecution); err != nil {
		return err
	}
	return nil
}

func defaultRuntimeToolBudget() RuntimeToolBudget {
	return RuntimeToolBudget{
		MaxToolCalls: 200, SoftToolCallLimit: 160, FinalizationReserve: 10,
		MaxRepeatedCalls: 2, MaxNoProgressCalls: 4,
		MaxSearchCalls: 60, MaxWriteCalls: 20,
		MaxReadBytes: 64 * 1024 * 1024, MaxWallTimeSeconds: 1800,
	}
}

func runtimeToolBudgetForIntent(intent domain.TaskIntent) RuntimeToolBudget {
	budget := defaultRuntimeToolBudget()
	if intent.ResolvedTaskType != "work_ai_visual_chat" {
		return budget
	}

	// The visual Runtime profile has a hard limit of eight tool calls. Keep the
	// signed per-Run budget inside that ceiling so Gateway can admit the Run.
	budget.MaxToolCalls = 8
	budget.SoftToolCallLimit = 6
	budget.FinalizationReserve = 1
	budget.MaxSearchCalls = 6
	budget.MaxWriteCalls = 0
	return budget
}

func ValidateRuntimeToolBudget(b RuntimeToolBudget) error {
	if b.MaxToolCalls <= 0 || b.SoftToolCallLimit <= 0 || b.SoftToolCallLimit >= b.MaxToolCalls ||
		b.FinalizationReserve < 1 || b.FinalizationReserve >= b.MaxToolCalls ||
		b.MaxRepeatedCalls != 2 || b.MaxNoProgressCalls != 4 || b.MaxSearchCalls < 0 || b.MaxWriteCalls < 0 ||
		b.MaxReadBytes <= 0 || b.MaxWallTimeSeconds <= 0 || b.MaxToolCalls > DefaultRuntimeToolBudgetExecutionContract().HardMaxToolCalls ||
		b.MaxSearchCalls > b.MaxToolCalls || b.MaxWriteCalls > b.MaxToolCalls {
		return domain.ErrorCode("AGENT_PLAN_INVALID")
	}
	return nil
}

func ComputeAgentRunPlanHash(plan AgentRunPlan) (string, error) {
	if err := ValidateMetaWorkspaceIdentityTriple(plan.MetaWorkspaceKey, plan.MetaWorkspaceVersion, plan.InputPolicyHash); err != nil {
		return "", err
	}
	if err := ValidateAgentRunToolPolicy(plan); err != nil {
		return "", err
	}
	raw, err := json.Marshal(plan)
	if err != nil {
		return "", domain.ErrorCode("AGENT_PLAN_INVALID")
	}
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

// ComputeAgentRunPlanHashForStableV05Adapter matches the exact plan shape of
// the pinned test Adapter. Callers must gate it to that Host explicitly: this
// projection intentionally omits fields that a v0.5 Adapter cannot retain
// while round-tripping a plan before it verifies the ticket hash.
func ComputeAgentRunPlanHashForStableV05Adapter(plan AgentRunPlan) (string, error) {
	if err := ValidateMetaWorkspaceIdentityTriple(plan.MetaWorkspaceKey, plan.MetaWorkspaceVersion, plan.InputPolicyHash); err != nil {
		return "", err
	}
	if err := ValidateAgentRunToolPolicy(plan); err != nil {
		return "", err
	}
	raw, err := json.Marshal(agentRunPlanStableV05Adapter{
		SchemaVersion: plan.SchemaVersion, AgentRunID: plan.AgentRunID, PlanVersion: plan.PlanVersion,
		RoutingMode: plan.RoutingMode, TaskType: plan.TaskType, ExecutionScope: plan.ExecutionScope,
		L1AgentProfile:  plan.L1AgentProfile,
		RuntimeConfigID: plan.RuntimeConfigID, AgentHash: plan.AgentHash, ManifestVersion: plan.ManifestVersion,
		ToolPolicyProfile: plan.ToolPolicyProfile, SelectedSkillProfiles: plan.SelectedSkillProfiles,
		SelectedKnowledgeRefs: plan.SelectedKnowledgeRefs, RequiredTools: plan.RequiredTools,
		OutputContract: plan.OutputContract, WriteMode: plan.WriteMode,
		ToolBudget: plan.ToolBudget, RequiresConfirmation: plan.RequiresConfirmation,
		SafePlanSummary: plan.SafePlanSummary, WorkspaceVersion: plan.WorkspaceVersion,
		IndexVersion: plan.IndexVersion, CapabilityHash: plan.CapabilityHash,
	})
	if err != nil {
		return "", domain.ErrorCode("AGENT_PLAN_INVALID")
	}
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

// agentRunPlanStableV05Adapter preserves the JSON field order understood by
// the test Adapter's v0.5 plan decoder.
type agentRunPlanStableV05Adapter struct {
	SchemaVersion         string            `json:"schemaVersion"`
	AgentRunID            string            `json:"agentRunId"`
	PlanVersion           int               `json:"planVersion"`
	RoutingMode           string            `json:"routingMode"`
	TaskType              string            `json:"taskType"`
	ExecutionScope        string            `json:"executionScope"`
	L1AgentProfile        string            `json:"l1AgentProfile"`
	RuntimeConfigID       string            `json:"runtimeConfigId"`
	AgentHash             string            `json:"agentHash"`
	ManifestVersion       string            `json:"manifestVersion"`
	ToolPolicyProfile     string            `json:"toolPolicyProfile"`
	SelectedSkillProfiles []string          `json:"selectedSkillProfiles"`
	SelectedKnowledgeRefs []string          `json:"selectedKnowledgeRefs"`
	RequiredTools         []string          `json:"requiredTools"`
	OutputContract        map[string]any    `json:"outputContract"`
	WriteMode             string            `json:"writeMode"`
	ToolBudget            RuntimeToolBudget `json:"toolBudget"`
	RequiresConfirmation  bool              `json:"requiresConfirmation"`
	SafePlanSummary       string            `json:"safePlanSummary"`
	WorkspaceVersion      int64             `json:"workspaceVersion"`
	IndexVersion          int64             `json:"indexVersion"`
	CapabilityHash        string            `json:"capabilityHash"`
}

// ValidateMetaWorkspaceIdentityTriple keeps the public Meta Workspace
// selection atomic. Internal Agent plans intentionally carry no public Meta
// identity, while a public Meta plan must freeze all three server-selected
// identity values. A partial identity could otherwise be mistaken for an
// internal plan at one boundary and a public plan at another.
func ValidateMetaWorkspaceIdentityTriple(metaWorkspaceKey, metaWorkspaceVersion, inputPolicyHash string) error {
	if metaWorkspaceKey == "" && metaWorkspaceVersion == "" && inputPolicyHash == "" {
		return nil
	}
	if !safeCatalogIdentifier(metaWorkspaceKey) || !safeCatalogIdentifier(metaWorkspaceVersion) || !validSHA256(inputPolicyHash) {
		return domain.ErrorCode("AGENT_PLAN_INVALID")
	}
	return nil
}

func ValidateAgentRunToolPolicy(plan AgentRunPlan) error {
	if err := ValidateRuntimeToolBudget(plan.ToolBudget); err != nil {
		return err
	}
	seen := map[string]bool{}
	previous := ""
	for _, tool := range plan.RequiredTools {
		if !IsAgentFacingRuntimeTool(tool) || seen[tool] {
			return domain.ErrorCode("AGENT_PLAN_INVALID")
		}
		if previous != "" && previous >= tool {
			return domain.ErrorCode("AGENT_PLAN_INVALID")
		}
		seen[tool] = true
		previous = tool
	}
	if _, err := RuntimeWorkspaceMountForPlan(plan); err != nil {
		return err
	}
	return nil
}

// skillsForIntent remains limited to explicit deterministic/specialized
// callers. CapabilityPlanner.Plan must never call it as a fallback.
func skillsForIntent(intent domain.TaskIntent) []string {
	switch intent.ResolvedTaskType {
	case "minutes_generation":
		return []string{"meeting_minutes"}
	case "summary_generation":
		return []string{"asset_summary"}
	case "material_deposit_generation":
		return []string{"material_deposit"}
	case "work_ai_topic_generation":
		return []string{"topic_generation"}
	case "work_ai_general_chat":
		return []string{"general_chat"}
	case "work_ai_renshe_content":
		return []string{"renshe_content_creation"}
	case "work_ai_huoke_content":
		return []string{"huoke_content_creation"}
	case "work_ai_huoke_topic_strategy":
		return []string{"huoke_topic_strategy"}
	case "work_ai_self_media_creation":
		return []string{"self_media_creation_advisor"}
	case "work_ai_faya_germination":
		return []string{"viewpoint_germination"}
	case "work_ai_visual_chat":
		return []string{"visual_chat_assistant"}
	case "profile_deposit":
		return []string{"profile_maintenance"}
	case "hotspot_home_suggestion":
		return []string{"hotspot_suggestion"}
	case "workspace_lookup":
		return []string{"general_chat"}
	case "workspace_asset_edit":
		return []string{"profile_maintenance"}
	case "profile_understanding":
		return []string{"positioning_profile_builder"}
	case "work_ai_content_creation":
		return []string{"renshe_content_creation", "huoke_content_creation"}
	default:
		return nil
	}
}

func toolsForIntent(intent domain.TaskIntent) []string {
	if intent.ResolvedTaskType == "work_ai_huoke_topic_strategy" {
		// The dedicated Huoke strategy contract is intentionally narrower than
		// general Workspace discovery, even when a malformed intent carries a
		// search capability.
		return []string{"read", "write"}
	}
	// Every Agent Run receives bounded Run-local write so multi-step work can
	// maintain phase memory. RuntimeWorkspaceMountForPlan still limits native
	// writes to output/ and staging/; this is not formal Workspace authority.
	tools := []string{"write"}
	for _, capability := range intent.RequiredCapabilities {
		switch capability {
		case "workspace_search":
			// Search returns paths only. Core read inspects the selected path; raw
			// filesystem commands remain Backend/Search-Service implementation details.
			for _, tool := range []string{"workspace_search", "read"} {
				tools = appendUnique(tools, tool)
			}
		case "workspace_read", "recording_read":
			tools = appendUnique(tools, "read")
		case "workspace_staging_write":
			tools = appendUnique(tools, "write")
		}
	}
	sort.Strings(tools)
	return tools
}

func outputContractForIntent(intent domain.TaskIntent) map[string]any {
	return map[string]any{"schemaVersion": "agent_output.v1", "format": intent.ExpectedOutput}
}

func writeModeForIntent(intent domain.TaskIntent) string {
	if intent.ResolvedTaskType == "work_ai_huoke_topic_strategy" {
		return "runtime_staging"
	}
	switch intent.RiskClass {
	case "formal_write":
		return "asset_write_intent"
	default:
		return "runtime_staging"
	}
}

func activeAgentMatches(manifest L1AgentManifest, plan AgentRunPlan) bool {
	_, ok := activeAgentEntry(manifest, plan)
	return ok
}

func activeAgentEntry(manifest L1AgentManifest, plan AgentRunPlan) (L1AgentManifestEntry, bool) {
	if manifest.Version != plan.ManifestVersion {
		return L1AgentManifestEntry{}, false
	}
	for _, entry := range manifest.Agents {
		if entry.AgentProfile == plan.L1AgentProfile && entry.Status == "active" && entry.Hash == plan.AgentHash &&
			matchesOne(entry.TaskTypes, plan.TaskType) && matchesOne(entry.ExecutionScopes, plan.ExecutionScope) {
			return entry, true
		}
	}
	return L1AgentManifestEntry{}, false
}

func activeSkillMatches(skills []PlannedSkill, profile, taskType, agentProfile string) bool {
	for _, skill := range skills {
		if skill.SkillProfile == profile && skill.Status == "active" && skill.Hash != "" && matchesOne(skill.TaskTypes, taskType) &&
			(len(skill.AllowedAgentProfiles) == 0 || matchesOne(skill.AllowedAgentProfiles, agentProfile)) {
			return true
		}
	}
	return false
}

// ValidateCapabilityPlanningCatalog is a startup-time structural check. It
// cannot prove live Runtime tool readiness; that is intentionally checked
// later against the selected RuntimeHost handshake.
func ValidateCapabilityPlanningCatalog(manifest L1AgentManifest, skills []PlannedSkill, permissions PlannerPermissionSnapshot) error {
	if err := ValidateL1AgentManifestForDynamicPlanning(manifest); err != nil {
		return err
	}
	activeAgents := 0
	for _, entry := range manifest.Agents {
		if entry.Status != "active" {
			continue
		}
		if isLegacyFeedAIPlanningIdentity("", entry.AgentProfile) || containsExact(entry.TaskTypes, "feed_ai_chat") {
			return domain.ErrorCode("AGENT_PLAN_INVALID")
		}
		activeAgents++
		if !permissions.AllowedAgents[entry.AgentProfile] {
			return domain.ErrorCode("AGENT_PROFILE_UNAVAILABLE")
		}
		if !candidateSkillsCoverAgent(entry, skills, permissions) {
			return domain.ErrorCode("SKILL_UNAVAILABLE")
		}
		for _, root := range entry.KnowledgeRoots {
			if !permissions.AllowedKnowledge[root] {
				return domain.ErrorCode("AGENT_PLAN_INVALID")
			}
		}
	}
	if activeAgents == 0 {
		return domain.ErrorCode("AGENT_PROFILE_UNAVAILABLE")
	}
	return nil
}

// isLegacyFeedAIPlanningIdentity prevents an old product projection from
// returning through a hand-authored or stale dynamic catalog. Historical
// terminal projection remains isolated in AgentProfileResolver.
func isLegacyFeedAIPlanningIdentity(taskType, agentProfile string) bool {
	return IsLegacyProjectionTaskType(taskType) || strings.TrimSpace(agentProfile) == "feed_ai_agent"
}

func candidateSkillsCoverAgent(entry L1AgentManifestEntry, skills []PlannedSkill, permissions PlannerPermissionSnapshot) bool {
	for _, candidate := range entry.CandidateSkillProfiles {
		for _, skill := range skills {
			if skill.SkillProfile != candidate || !permissions.AllowedSkills[candidate] || skill.Status != "active" ||
				strings.TrimSpace(skill.Hash) == "" || len(skill.TaskTypes) == 0 ||
				(len(skill.AllowedAgentProfiles) > 0 && !matchesOne(skill.AllowedAgentProfiles, entry.AgentProfile)) {
				continue
			}
			return true
		}
	}
	return false
}

func requiredCapabilitiesSatisfied(required, available []string) bool {
	for _, capability := range required {
		if !containsExact(available, capability) {
			return false
		}
	}
	return true
}

func selectedSkillProfiles(skills []PlannedSkill) []string {
	profiles := make([]string, 0, len(skills))
	for _, skill := range skills {
		profiles = append(profiles, skill.SkillProfile)
	}
	return profiles
}

func knowledgeReferenceAllowed(roots []string, ref string) bool {
	if !safeManifestRelativePath(ref) || path.Ext(path.Base(ref)) == "" {
		return false
	}
	for _, root := range roots {
		if !safeManifestRelativePath(root) {
			return false
		}
		if strings.HasPrefix(ref, root+"/") {
			return true
		}
	}
	return false
}

func containsExact(values []string, expected string) bool {
	for _, value := range values {
		if strings.TrimSpace(value) == expected {
			return true
		}
	}
	return false
}

func registeredOutputContract(contract map[string]any) bool {
	if len(contract) == 0 || strings.TrimSpace(fmt.Sprint(contract["schemaVersion"])) == "" {
		return false
	}
	format, ok := contract["format"].(string)
	return ok && registeredOutputFormat(format)
}

func registeredOutputFormat(format string) bool {
	switch strings.TrimSpace(format) {
	case "text", "markdown", "json", "structured_json", "artifact", "markdown_or_json":
		return true
	default:
		return false
	}
}

type registeredTerminalOutputTemplate struct {
	PromptTemplateID      string
	PromptTemplateVersion string
	OutputSchemaVersion   string
}

// newAgentRunTerminalOutputIdentity turns the selected primary Skill into an
// immutable output contract while planning. The registered table handles
// specialized output protocols; other catalog Skills use the bounded generic
// reply protocol. Neither branch re-routes the selected L1 or Skill.
func newAgentRunTerminalOutputIdentity(taskType, l1AgentProfile, skillProfile, format string) (AgentRunTerminalOutputIdentity, error) {
	taskType = strings.TrimSpace(taskType)
	l1AgentProfile = strings.TrimSpace(l1AgentProfile)
	skillProfile = strings.TrimSpace(skillProfile)
	format = strings.TrimSpace(format)
	template, ok := registeredTerminalOutputTemplateFor(taskType, skillProfile)
	if !ok || l1AgentProfile == "" || !registeredOutputFormat(format) {
		return AgentRunTerminalOutputIdentity{}, domain.ErrorCode("AGENT_PLAN_INVALID")
	}
	return AgentRunTerminalOutputIdentity{
		TaskType: taskType, L1AgentProfile: l1AgentProfile, SkillProfile: skillProfile,
		PromptTemplateID: template.PromptTemplateID, PromptTemplateVersion: template.PromptTemplateVersion,
		OutputSchemaVersion: template.OutputSchemaVersion, Format: format,
	}, nil
}

// ValidateAgentRunTerminalOutputIdentity proves that the persisted terminal
// parser/writeback identity belongs to this exact frozen plan. It deliberately
// has no AiTask input; a compatibility task cannot override a dynamic run.
func ValidateAgentRunTerminalOutputIdentity(plan AgentRunPlan) error {
	identity := plan.TerminalOutput
	if strings.TrimSpace(identity.TaskType) == "" || identity.TaskType != plan.TaskType ||
		strings.TrimSpace(identity.L1AgentProfile) == "" || identity.L1AgentProfile != plan.L1AgentProfile ||
		strings.TrimSpace(identity.SkillProfile) == "" || !containsExact(plan.SelectedSkillProfiles, identity.SkillProfile) ||
		!registeredOutputContract(plan.OutputContract) || !registeredOutputFormat(identity.Format) ||
		strings.TrimSpace(fmt.Sprint(plan.OutputContract["format"])) != identity.Format {
		return domain.ErrorCode("AGENT_PLAN_INVALID")
	}
	template, ok := registeredTerminalOutputTemplateFor(identity.TaskType, identity.SkillProfile)
	if !ok || identity.PromptTemplateID != template.PromptTemplateID ||
		identity.PromptTemplateVersion != template.PromptTemplateVersion ||
		identity.OutputSchemaVersion != template.OutputSchemaVersion {
		return domain.ErrorCode("AGENT_PLAN_INVALID")
	}
	return nil
}

func registeredTerminalOutputTemplateFor(taskType, skillProfile string) (registeredTerminalOutputTemplate, bool) {
	base := func(skill, prompt, schema string) (registeredTerminalOutputTemplate, bool) {
		if skillProfile != skill {
			return registeredTerminalOutputTemplate{}, false
		}
		version := "v0.1.0"
		if taskType == "work_ai_faya_germination" {
			version = "v2.0.0"
		}
		return registeredTerminalOutputTemplate{PromptTemplateID: prompt, PromptTemplateVersion: version, OutputSchemaVersion: schema}, true
	}
	switch taskType {
	case "work_ai_topic_generation":
		return base("topic_generation", "work_ai.topic_generation.v1", "topic_generation.result.v1")
	case "work_ai_general_chat", "workspace_lookup":
		return base("general_chat", "work_ai.general_chat.v1", "general_chat.result.v1")
	case "work_ai_renshe_content":
		return base("renshe_content_creation", "work_ai.renshe_content.v1", "renshe_content_creation.result.v1")
	case "work_ai_huoke_content":
		return base("huoke_content_creation", "work_ai.huoke_content.v1", "huoke_content_creation.result.v1")
	case "work_ai_huoke_topic_strategy":
		return base("huoke_topic_strategy", "work_ai.huoke_topic_strategy.v1", "huoke_topic_strategy.result.v1")
	case "work_ai_self_media_creation":
		return base("self_media_creation_advisor", "work_ai.self_media_creation.v1", "self_media_creation_advisor.result.v1")
	case "work_ai_faya_germination":
		return base("viewpoint_germination", "work_ai.faya_germination.v2", "viewpoint_germination.result.v2")
	case "work_ai_visual_chat":
		return base("visual_chat_assistant", "work_ai.visual_chat.v1", "visual_chat_assistant.result.v1")
	case "feed_ai_chat", "profile_understanding":
		// feed_ai_chat remains parseable only for an already persisted terminal
		// identity. It is rejected before any new plan can be constructed.
		return base("positioning_profile_builder", "feed_ai.positioning_profile_builder.v1", "positioning_profile_builder.result.v1")
	case "profile_deposit", "workspace_asset_edit":
		return base("profile_maintenance", "feed_ai.profile_maintenance.v1", "profile_maintenance.result.v1")
	case "minutes_generation":
		return base("meeting_minutes", "recording.meeting_minutes.v1", "meeting_minutes.result.v1")
	case "summary_generation":
		return base("asset_summary", "recording.asset_summary.v1", "asset_summary.result.v1")
	case "material_deposit_generation":
		return base("material_deposit", "material.deposit.v1", "material_deposit.result.v1")
	case "hotspot_home_suggestion":
		return base("hotspot_suggestion", "hotspot.suggestion.v1", "hotspot_suggestion.result.v1")
	case "work_ai_content_creation":
		switch skillProfile {
		case "renshe_content_creation":
			return registeredTerminalOutputTemplate{PromptTemplateID: "work_ai.renshe_content.v1", PromptTemplateVersion: "v0.1.0", OutputSchemaVersion: "renshe_content_creation.result.v1"}, true
		case "huoke_content_creation":
			return registeredTerminalOutputTemplate{PromptTemplateID: "work_ai.huoke_content.v1", PromptTemplateVersion: "v0.1.0", OutputSchemaVersion: "huoke_content_creation.result.v1"}, true
		}
	}
	if safeCatalogIdentifier(taskType) && safeCatalogIdentifier(skillProfile) {
		return registeredTerminalOutputTemplate{
			PromptTemplateID: "dynamic." + skillProfile + ".v1", PromptTemplateVersion: "v1.0.0",
			OutputSchemaVersion: skillProfile + ".result.v1",
		}, true
	}
	return registeredTerminalOutputTemplate{}, false
}

func clonePlannedSkills(skills []PlannedSkill) []PlannedSkill {
	cloned := make([]PlannedSkill, 0, len(skills))
	for _, skill := range skills {
		cloned = append(cloned, clonePlannedSkill(skill))
	}
	return cloned
}

func clonePlannedSkill(skill PlannedSkill) PlannedSkill {
	skill.TaskTypes = append([]string(nil), skill.TaskTypes...)
	skill.IntentCategories = append([]string(nil), skill.IntentCategories...)
	skill.AllowedAgentProfiles = append([]string(nil), skill.AllowedAgentProfiles...)
	skill.RequiredCapabilities = append([]string(nil), skill.RequiredCapabilities...)
	skill.KnowledgeRefs = append([]string(nil), skill.KnowledgeRefs...)
	return skill
}

func clonePlannerPermissions(permissions PlannerPermissionSnapshot) PlannerPermissionSnapshot {
	return PlannerPermissionSnapshot{
		AllowedAgents:    clonePermissionMap(permissions.AllowedAgents),
		AllowedSkills:    clonePermissionMap(permissions.AllowedSkills),
		AllowedKnowledge: clonePermissionMap(permissions.AllowedKnowledge),
		AllowFormalWrite: permissions.AllowFormalWrite,
	}
}

func clonePermissionMap(values map[string]bool) map[string]bool {
	cloned := make(map[string]bool, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

func appendUnique(values []string, value string) []string {
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}
