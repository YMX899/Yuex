package workers

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"huahuoai/backend/source/internal/domain"
	"huahuoai/backend/source/internal/persistence"
	runtimepkg "huahuoai/backend/source/internal/runtime"
	workspacepkg "huahuoai/backend/source/internal/workspace"
)

const (
	huokeTopicTaskType                       = "work_ai_huoke_topic_strategy"
	huokeTopicAgentProfile                   = "huoke_neirong_agent"
	huokeTopicSkillProfile                   = "huoke_topic_strategy"
	huokeTopicMaxMaterialFiles               = 24
	huokeTopicMaxWorkspaceFileBytes          = 512 * 1024
	selfMediaCreationTaskType                = "work_ai_self_media_creation"
	selfMediaCreationAgentProfile            = "self_media_creation_agent"
	selfMediaCreationSkillProfile            = "self_media_creation_advisor"
	selfMediaCreationKnowledgeRoot           = "knowledge/self-media-creation"
	selfMediaCreationMaxCards                = 64
	selfMediaCreationMaxReferences           = 256
	selfMediaCreationMaxKnowledgeFiles       = 2 + selfMediaCreationMaxCards + selfMediaCreationMaxReferences
	selfMediaCreationMaxKnowledgeBytes       = 8 * 1024 * 1024
	positioningSkillProfile                  = "positioning_profile_builder"
	rensheContentTaskType                    = "work_ai_renshe_content"
	rensheContentAgentProfile                = "renshe_neirong_agent"
	rensheContentSkillProfile                = "renshe_content_creation"
	rensheContentMaxMaterialFiles            = 4
	rensheContentMaxMaterialBytes            = 1 * 1024 * 1024
	fayaGerminationTaskType                  = "work_ai_faya_germination"
	fayaGerminationAgentProfile              = "faya_agent"
	fayaGerminationSkillProfile              = "viewpoint_germination"
	dynamicKnowledgeMaxFiles                 = 64
	dynamicKnowledgeMaxBytes           int64 = 8 * 1024 * 1024
	formalWorkspacePackageMaxFiles           = 512
	formalWorkspacePackageMaxBytes     int64 = 32 * 1024 * 1024
)

var (
	huokeTopicAgentFiles = []string{
		"AGENTS.md",
		"SOUL.md",
		"TOOLS.md",
		"MEMORY.md",
		"capability-catalog.json",
		"knowledge/huoke-full-funnel/INDEX.md",
		"protocols/INDEX.md",
		"protocols/topic-analysis-state.v1.md",
		"protocols/topic-guidance-result.v1.md",
	}
	huokeTopicSkillFiles = []string{
		"SKILL.md",
		"references/coverage-matrix.md",
		"references/evidence-profile-state-revision.md",
		"references/strategy-fit-contracts.md",
		"references/subject-contracts.md",
		"references/topic-guidance-contract.md",
		"references/workflow-module-contracts.md",
	}
	selfMediaCreationAgentFiles = []string{
		"AGENTS.md",
		"SOUL.md",
		"TOOLS.md",
		"MEMORY.md",
		"capability-catalog.json",
		selfMediaCreationKnowledgeRoot + "/OVERVIEW.md",
		selfMediaCreationKnowledgeRoot + "/cards-manifest.json",
	}
	selfMediaCreationSkillFiles = []string{"SKILL.md"}
	rensheContentAgentFiles     = []string{
		"AGENTS.md",
		"SOUL.md",
		"TOOLS.md",
		"MEMORY.md",
		"capability-catalog.json",
		"knowledge/renshe-content/INDEX.md",
		"protocols/INDEX.md",
		"protocols/consultation-state.v1.md",
		"protocols/result-receipt.v3.md",
	}
	rensheContentSkillFiles = []string{
		"SKILL.md",
		"references/content-subject-contracts.md",
		"references/content-value-judgment.md",
		"references/evidence-state-revision.md",
		"references/positioning-domain-knowledge.md",
		"references/positioning-subject-contracts.md",
		"references/production-craft.md",
	}
	fayaGerminationAgentFiles = []string{
		"AGENTS.md",
		"SOUL.md",
		"TOOLS.md",
		"MEMORY.md",
		"capability-catalog.json",
		"knowledge/viewpoint-germination/INDEX.md",
		"knowledge/viewpoint-germination/core-principles.md",
		"knowledge/viewpoint-germination/interpretive-fingerprint.md",
		"knowledge/viewpoint-germination/material-modes.md",
		"knowledge/viewpoint-germination/lens-routing.md",
		"knowledge/viewpoint-germination/depth-moves.md",
		"knowledge/viewpoint-germination/content-value-check.md",
		"knowledge/viewpoint-germination/material-grounding.md",
		"knowledge/viewpoint-germination/operator-library.md",
		"knowledge/viewpoint-germination/critique-and-selection.md",
		"knowledge/viewpoint-germination/output-contract.md",
	}
	fayaGerminationSkillFiles = []string{"SKILL.md"}
	positioningSkillFiles     = []string{
		"SKILL.md",
		"references/positioning-subject-contracts.md",
	}
	dynamicAgentCoreFiles = []string{
		"AGENTS.md",
		"SOUL.md",
		"TOOLS.md",
		"capability-catalog.json",
	}
	huokeTopicMaterialPath         = regexp.MustCompile(`materials/(?:raw|processed)/[^\s\x60|)\]}>"']+`)
	huokeTopicSafeMaterialRef      = regexp.MustCompile(`^materials/(?:raw|processed)/[A-Za-z0-9][A-Za-z0-9_.-]*\.md$`)
	huokeTopicUnsafeIndexPath      = regexp.MustCompile(`(?:\.\.[/\\]|workspaces[/\\]tenants[/\\])`)
	selfMediaCreationSHA256Pattern = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)
)

const runtimeAgentCapabilityCatalogSchemaVersion = "huahuo.agent_capability_catalog.v1"

type runtimeAgentCapabilityCatalog struct {
	SchemaVersion string                          `json:"schemaVersion"`
	AgentProfile  string                          `json:"agentProfile"`
	Capabilities  []runtimeAgentCapabilityBinding `json:"capabilities"`
}

type runtimeAgentCapabilityBinding struct {
	Scene               string `json:"scene"`
	TaskType            string `json:"taskType"`
	SkillProfile        string `json:"skillProfile"`
	OutputSchemaVersion string `json:"outputSchemaVersion"`
	VisibleOwner        bool   `json:"visibleOwner"`
	Status              string `json:"status"`
}

// ValidateRuntimeCatalogPackageClosure proves that every active dynamic L1
// profile can be materialized from the release's read-only runtime package.
// Planning catalog validation alone cannot make that filesystem fact true.
func ValidateRuntimeCatalogPackageClosure(configRoot string, catalog AgentPlanningCatalog) error {
	if strings.TrimSpace(configRoot) == "" {
		return fmt.Errorf("AGENT_PROFILE_UNAVAILABLE")
	}

	skillsByProfile := make(map[string]runtimepkg.PlannedSkill, len(catalog.Skills))
	for _, skill := range catalog.Skills {
		if skill.Status != "active" {
			continue
		}
		if _, exists := skillsByProfile[skill.SkillProfile]; exists || !safeRuntimePackageSegment(skill.SkillProfile) {
			return fmt.Errorf("SKILL_UNAVAILABLE")
		}
		if err := validateRuntimeCatalogSkillPackage(configRoot, skill); err != nil {
			return err
		}
		skillsByProfile[skill.SkillProfile] = skill
	}

	for _, agent := range catalog.Manifest.Agents {
		if agent.Status != "active" {
			continue
		}
		if err := validateRuntimeCatalogAgentPackage(configRoot, agent, skillsByProfile); err != nil {
			return err
		}
	}
	return nil
}

func validateRuntimeCatalogAgentPackage(configRoot string, agent runtimepkg.L1AgentManifestEntry, skillsByProfile map[string]runtimepkg.PlannedSkill) error {
	root, err := activeRuntimeAgentPackageRoot(configRoot, agent.RelativeRoot)
	if err != nil {
		return fmt.Errorf("AGENT_PROFILE_UNAVAILABLE")
	}

	var capabilityCatalogRaw []byte
	for _, relative := range dynamicAgentCoreFiles {
		body, readErr := readRequiredPackageFile(root, relative)
		if readErr != nil {
			return fmt.Errorf("AGENT_PROFILE_UNAVAILABLE")
		}
		if relative == "AGENTS.md" && dispatcherSHA256(body) != normalizeDispatcherHash(agent.Hash) {
			return fmt.Errorf("AGENT_PROFILE_UNAVAILABLE")
		}
		if relative == "capability-catalog.json" {
			capabilityCatalogRaw = body
		}
	}

	if err := validateRuntimeAgentCapabilityCatalog(capabilityCatalogRaw, agent, skillsByProfile); err != nil {
		return err
	}
	return nil
}

func validateRuntimeCatalogSkillPackage(configRoot string, skill runtimepkg.PlannedSkill) error {
	if strings.TrimSpace(configRoot) == "" || !safeRuntimePackageSegment(skill.SkillProfile) {
		return fmt.Errorf("SKILL_UNAVAILABLE")
	}
	root := filepath.Join(configRoot, "runtime-skills", skill.SkillProfile)
	body, err := readRequiredPackageFile(root, "SKILL.md")
	if err != nil || dispatcherSHA256(body) != normalizeDispatcherHash(skill.Hash) {
		return fmt.Errorf("SKILL_UNAVAILABLE")
	}
	return nil
}

func activeRuntimeAgentPackageRoot(configRoot, logicalRoot string) (string, error) {
	if strings.TrimSpace(configRoot) == "" || !safeDynamicAgentRelativeRoot(logicalRoot) {
		return "", fmt.Errorf("runtime package unavailable")
	}
	root := filepath.Join(configRoot, "runtime-agents", filepath.FromSlash(strings.TrimPrefix(logicalRoot, "agents/")))
	if _, err := readRequiredPackageFile(root, "AGENTS.md"); err != nil {
		return "", err
	}
	return root, nil
}

func validateRuntimeAgentCapabilityCatalog(raw []byte, agent runtimepkg.L1AgentManifestEntry, skillsByProfile map[string]runtimepkg.PlannedSkill) error {
	var capabilityCatalog runtimeAgentCapabilityCatalog
	if len(raw) == 0 || json.Unmarshal(raw, &capabilityCatalog) != nil ||
		capabilityCatalog.SchemaVersion != runtimeAgentCapabilityCatalogSchemaVersion ||
		capabilityCatalog.AgentProfile != agent.AgentProfile || len(capabilityCatalog.Capabilities) == 0 {
		return fmt.Errorf("AGENT_PROFILE_UNAVAILABLE")
	}

	coveredTasks := map[string]bool{}
	seenBindings := map[string]bool{}
	for _, binding := range capabilityCatalog.Capabilities {
		taskType := strings.TrimSpace(binding.TaskType)
		skillProfile := strings.TrimSpace(binding.SkillProfile)
		bindingKey := taskType + "\x00" + skillProfile
		if strings.TrimSpace(binding.Scene) == "" || !runtimeCatalogIdentifier(taskType) || !safeRuntimePackageSegment(skillProfile) ||
			strings.TrimSpace(binding.OutputSchemaVersion) == "" || !binding.VisibleOwner || binding.Status != "active" ||
			seenBindings[bindingKey] || !containsRuntimeProfile(agent.TaskTypes, taskType) ||
			!containsRuntimeProfile(agent.CandidateSkillProfiles, skillProfile) {
			return fmt.Errorf("AGENT_PROFILE_UNAVAILABLE")
		}
		skill, exists := skillsByProfile[skillProfile]
		if !exists || !containsRuntimeProfile(skill.TaskTypes, taskType) || !containsRuntimeProfile(skill.AllowedAgentProfiles, agent.AgentProfile) {
			return fmt.Errorf("SKILL_UNAVAILABLE")
		}
		seenBindings[bindingKey] = true
		coveredTasks[taskType] = true
	}
	for _, taskType := range agent.TaskTypes {
		if !coveredTasks[taskType] {
			return fmt.Errorf("AGENT_PROFILE_UNAVAILABLE")
		}
	}
	return nil
}

func runtimeCatalogIdentifier(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" {
		return false
	}
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' || r == '-' || r == '.' {
			continue
		}
		return false
	}
	return true
}

func (p FilesystemRuntimeManifestProvider) agentManifestEntries(plan runtimepkg.AgentRunPlan, frozen domain.RunWorkspaceContextRecord) ([]runtimepkg.RuntimeManifestEntry, error) {
	if plan.TaskType == huokeTopicTaskType && plan.L1AgentProfile != huokeTopicAgentProfile {
		return nil, fmt.Errorf("AGENT_PROFILE_UNAVAILABLE")
	}
	if plan.TaskType == selfMediaCreationTaskType && plan.L1AgentProfile != selfMediaCreationAgentProfile {
		return nil, fmt.Errorf("AGENT_PROFILE_UNAVAILABLE")
	}
	if plan.TaskType == rensheContentTaskType && plan.L1AgentProfile != rensheContentAgentProfile {
		return nil, fmt.Errorf("AGENT_PROFILE_UNAVAILABLE")
	}
	if plan.TaskType == fayaGerminationTaskType && plan.L1AgentProfile != fayaGerminationAgentProfile {
		return nil, fmt.Errorf("AGENT_PROFILE_UNAVAILABLE")
	}
	root, err := p.agentPackageRoot(plan, frozen)
	if err != nil {
		return nil, err
	}
	required := []string{"AGENTS.md"}
	requiredHashes := map[string]string{}
	if plan.RoutingMode == "dynamic" {
		required = dynamicAgentCoreFiles
	} else if plan.TaskType == huokeTopicTaskType {
		required = huokeTopicAgentFiles
	} else if plan.TaskType == selfMediaCreationTaskType {
		var requiredErr error
		required, requiredHashes, requiredErr = selfMediaCreationAgentPackageFiles(root)
		if requiredErr != nil {
			return nil, fmt.Errorf("AGENT_PROFILE_UNAVAILABLE")
		}
	} else if plan.TaskType == rensheContentTaskType {
		required = rensheContentAgentFiles
	} else if plan.TaskType == fayaGerminationTaskType {
		required = fayaGerminationAgentFiles
	}
	required = runtimeMaterializedAgentFiles(required)
	entries := make([]runtimepkg.RuntimeManifestEntry, 0, len(required))
	for _, relative := range required {
		body, readErr := readRequiredPackageFile(root, relative)
		if readErr != nil {
			return nil, fmt.Errorf("AGENT_PROFILE_UNAVAILABLE")
		}
		if relative == "AGENTS.md" && dispatcherSHA256(body) != normalizeDispatcherHash(plan.AgentHash) {
			return nil, fmt.Errorf("AGENT_PROFILE_UNAVAILABLE")
		}
		if expectedHash := requiredHashes[relative]; expectedHash != "" && dispatcherSHA256(body) != expectedHash {
			return nil, fmt.Errorf("AGENT_PROFILE_UNAVAILABLE")
		}
		entries = append(entries, runtimepkg.NewInlineRuntimeEntry(relative, body))
	}
	return entries, nil
}

// capability-catalog.json is a server-side planning/validation input. It is
// intentionally not materialized into a model-visible Run workspace: the
// selected Plan already freezes the permitted Agent/Skill/Tool identity.
func runtimeMaterializedAgentFiles(files []string) []string {
	filtered := make([]string, 0, len(files))
	for _, relative := range files {
		if relative != "capability-catalog.json" {
			filtered = append(filtered, relative)
		}
	}
	return filtered
}

func (p FilesystemRuntimeManifestProvider) agentPackageRoot(plan runtimepkg.AgentRunPlan, frozen domain.RunWorkspaceContextRecord) (string, error) {
	if plan.RoutingMode != "dynamic" {
		root, err := runtimePackageRoot(p.Root, "runtime-agents", "agents", plan.L1AgentProfile, "AGENTS.md")
		if err != nil {
			return "", fmt.Errorf("AGENT_PROFILE_UNAVAILABLE")
		}
		return root, nil
	}
	if !safeDynamicAgentRelativeRoot(plan.AgentRelativeRoot) || plan.AgentRelativeRoot != frozen.AgentRelativeRoot ||
		plan.L1AgentProfile == "" || frozen.L1AgentProfile != plan.L1AgentProfile {
		return "", fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
	}
	root, err := runtimeAgentPackageRoot(p.Root, plan.AgentRelativeRoot, "AGENTS.md")
	if err != nil {
		return "", fmt.Errorf("AGENT_PROFILE_UNAVAILABLE")
	}
	return root, nil
}

func (p FilesystemRuntimeManifestProvider) selectedKnowledgeManifestEntries(plan runtimepkg.AgentRunPlan, frozen domain.RunWorkspaceContextRecord) ([]runtimepkg.RuntimeManifestEntry, error) {
	if plan.RoutingMode != "dynamic" {
		return nil, nil
	}
	if !sameRuntimeStringSet(plan.SelectedKnowledgeRefs, frozen.SelectedKnowledgeRefs) {
		return nil, fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
	}
	root, err := p.agentPackageRoot(plan, frozen)
	if err != nil {
		return nil, err
	}
	refs := append([]string(nil), frozen.SelectedKnowledgeRefs...)
	sort.Strings(refs)
	entries := make([]runtimepkg.RuntimeManifestEntry, 0, len(refs))
	seen := map[string]int{}
	var totalBytes int64
	for _, ref := range refs {
		if !safeSelectedKnowledgeReference(ref) {
			return nil, fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
		}
		target, targetErr := safeRelativeFile(root, ref)
		if targetErr != nil {
			return nil, fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
		}
		info, statErr := os.Lstat(target)
		if statErr != nil || info.Mode()&os.ModeSymlink != 0 {
			return nil, fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
		}
		if !info.Mode().IsRegular() {
			return nil, fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
		}
		if err := appendSelectedKnowledgeFile(root, ref, &entries, seen, &totalBytes); err != nil {
			return nil, err
		}
	}
	if plan.TaskType == selfMediaCreationTaskType && plan.L1AgentProfile == selfMediaCreationAgentProfile {
		manifestFiles, expectedHashes, manifestErr := selfMediaCreationKnowledgeManifestFiles(root)
		if manifestErr != nil {
			return nil, fmt.Errorf("AGENT_PROFILE_UNAVAILABLE")
		}
		for _, relative := range manifestFiles {
			if index, exists := seen[relative]; exists {
				if expectedHash := expectedHashes[relative]; expectedHash != "" && entries[index].SHA256 != expectedHash {
					return nil, fmt.Errorf("AGENT_PROFILE_UNAVAILABLE")
				}
				continue
			}
			if len(entries) >= selfMediaCreationMaxKnowledgeFiles {
				return nil, fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
			}
			content, readErr := readRequiredPackageFile(root, relative)
			if readErr != nil || totalBytes+int64(len(content)) > selfMediaCreationMaxKnowledgeBytes {
				return nil, fmt.Errorf("AGENT_PROFILE_UNAVAILABLE")
			}
			if expectedHash := expectedHashes[relative]; expectedHash != "" && dispatcherSHA256(content) != expectedHash {
				return nil, fmt.Errorf("AGENT_PROFILE_UNAVAILABLE")
			}
			seen[relative] = len(entries)
			totalBytes += int64(len(content))
			entries = append(entries, runtimepkg.NewInlineRuntimeEntry(relative, content))
		}
	}
	return entries, nil
}

func appendSelectedKnowledgeFile(root, relative string, entries *[]runtimepkg.RuntimeManifestEntry, seen map[string]int, totalBytes *int64) error {
	if _, exists := seen[relative]; !safeSelectedKnowledgeReference(relative) || exists || len(*entries) >= dynamicKnowledgeMaxFiles {
		return fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
	}
	content, err := readRequiredPackageFile(root, relative)
	if err != nil || *totalBytes+int64(len(content)) > dynamicKnowledgeMaxBytes {
		return fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
	}
	seen[relative] = len(*entries)
	*totalBytes += int64(len(content))
	*entries = append(*entries, runtimepkg.NewInlineRuntimeEntry(relative, content))
	return nil
}

func safeDynamicAgentRelativeRoot(value string) bool {
	value = strings.TrimSpace(value)
	return strings.HasPrefix(value, "agents/") && strings.TrimPrefix(value, "agents/") != "" && safeRuntimeRelativePath(value)
}

func safeSelectedKnowledgeReference(value string) bool {
	value = strings.TrimSpace(value)
	return strings.HasPrefix(value, "knowledge/") && strings.TrimPrefix(value, "knowledge/") != "" && safeRuntimeRelativePath(value)
}

func safeRuntimeRelativePath(value string) bool {
	if value == "" || filepath.IsAbs(value) || strings.Contains(value, "\\") || strings.Contains(value, ":") || strings.Contains(value, "..") {
		return false
	}
	for _, segment := range strings.Split(value, "/") {
		if !safeRuntimePackageSegment(segment) {
			return false
		}
	}
	return true
}

func sameRuntimeStringSet(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	counts := map[string]int{}
	for _, value := range left {
		counts[value]++
	}
	for _, value := range right {
		counts[value]--
	}
	for _, count := range counts {
		if count != 0 {
			return false
		}
	}
	return true
}

func (p FilesystemRuntimeManifestProvider) skillManifestEntries(plan runtimepkg.AgentRunPlan) (map[string]string, []runtimepkg.RuntimeManifestEntry, error) {
	if plan.TaskType == huokeTopicTaskType && !containsRuntimeProfile(plan.SelectedSkillProfiles, huokeTopicSkillProfile) {
		return nil, nil, fmt.Errorf("SKILL_UNAVAILABLE")
	}
	if plan.TaskType == selfMediaCreationTaskType && !containsRuntimeProfile(plan.SelectedSkillProfiles, selfMediaCreationSkillProfile) {
		return nil, nil, fmt.Errorf("SKILL_UNAVAILABLE")
	}
	if plan.TaskType == rensheContentTaskType && !containsRuntimeProfile(plan.SelectedSkillProfiles, rensheContentSkillProfile) {
		return nil, nil, fmt.Errorf("SKILL_UNAVAILABLE")
	}
	if plan.TaskType == fayaGerminationTaskType && !containsRuntimeProfile(plan.SelectedSkillProfiles, fayaGerminationSkillProfile) {
		return nil, nil, fmt.Errorf("SKILL_UNAVAILABLE")
	}
	hashes := map[string]string{}
	entries := []runtimepkg.RuntimeManifestEntry{}
	for _, skill := range plan.SelectedSkillProfiles {
		var root string
		var err error
		if plan.RoutingMode == "dynamic" {
			root, err = activeRuntimeSkillPackageRoot(p.Root, skill)
		} else {
			root, err = runtimePackageRoot(p.Root, "runtime-skills", "skills", skill, "SKILL.md")
		}
		if err != nil {
			return nil, nil, fmt.Errorf("SKILL_UNAVAILABLE")
		}
		required := []string{"SKILL.md"}
		if plan.TaskType == huokeTopicTaskType && skill == huokeTopicSkillProfile {
			required = huokeTopicSkillFiles
		} else if plan.TaskType == selfMediaCreationTaskType && skill == selfMediaCreationSkillProfile {
			required = selfMediaCreationSkillFiles
		} else if plan.TaskType == rensheContentTaskType && skill == rensheContentSkillProfile {
			required = rensheContentSkillFiles
		} else if plan.TaskType == fayaGerminationTaskType && skill == fayaGerminationSkillProfile {
			required = fayaGerminationSkillFiles
		} else if skill == positioningSkillProfile {
			required = positioningSkillFiles
		}
		expectedHash := normalizeDispatcherHash(p.SkillHashes[skill])
		if expectedHash == "" {
			return nil, nil, fmt.Errorf("SKILL_UNAVAILABLE")
		}
		for _, relative := range required {
			body, readErr := readRequiredPackageFile(root, relative)
			if readErr != nil {
				return nil, nil, fmt.Errorf("SKILL_UNAVAILABLE")
			}
			if relative == "SKILL.md" && dispatcherSHA256(body) != expectedHash {
				return nil, nil, fmt.Errorf("SKILL_UNAVAILABLE")
			}
			logicalPath := "skills/" + skill + "/" + filepath.ToSlash(relative)
			entries = append(entries, runtimepkg.NewInlineRuntimeEntry(logicalPath, body))
		}
		hashes[skill] = expectedHash
	}
	return hashes, entries, nil
}

// Product-thread workspaces may contain account-specific Agent files and
// selected Skill overrides. Runtime Meta remains the validated package source,
// while the formal Workspace supplies the effective files for this exact
// tenant/user/workspace snapshot.
func (p FilesystemRuntimeManifestProvider) applyFormalWorkspacePackage(run persistence.AgentRunRecord, plan runtimepkg.AgentRunPlan, frozen domain.RunWorkspaceContextRecord, base []runtimepkg.RuntimeManifestEntry, skillHashes map[string]string) ([]runtimepkg.RuntimeManifestEntry, map[string]string, error) {
	if plan.ExecutionScope != string(runtimepkg.ScopeProductThread) {
		return base, skillHashes, nil
	}
	if run.AgentRunID == "" || run.TenantID == "" || run.UserID == "" || run.WorkspaceID == "" ||
		plan.AgentRunID != run.AgentRunID || frozen.RunID != run.AgentRunID || frozen.AgentRunID != run.AgentRunID ||
		frozen.TenantID != run.TenantID || frozen.UserID != run.UserID || frozen.WorkspaceID != run.WorkspaceID {
		return nil, nil, fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
	}

	resolver := workspacepkg.NewPathResolver(p.DataRoot, "")
	root, err := resolver.ResolveFormalWorkspace(run.TenantID, run.UserID, run.WorkspaceID)
	if err != nil || validateWorkspaceSnapshotRoot(resolver.DataRoot, root) != nil {
		return nil, nil, fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
	}

	entries := append([]runtimepkg.RuntimeManifestEntry(nil), base...)
	seen := make(map[string]int, len(entries))
	var totalBytes int64
	for index, entry := range entries {
		if _, duplicate := seen[entry.LogicalPath]; duplicate {
			return nil, nil, fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
		}
		seen[entry.LogicalPath] = index
		totalBytes += entry.SizeBytes
	}

	// Replace only paths already authorized by the selected Meta Agent/Skills.
	for logicalPath, index := range seen {
		entry, _, found, entryErr := optionalFormalWorkspaceEntry(resolver, root, logicalPath)
		if entryErr != nil {
			return nil, nil, entryErr
		}
		if !found {
			continue
		}
		totalBytes += entry.SizeBytes - entries[index].SizeBytes
		entries[index] = entry
	}

	// Protocols belong to the selected account Agent. Skill trees are bounded to
	// profiles already frozen in the plan; unrelated Workspace Skills stay out.
	extraRoots := []string{"protocols"}
	for _, profile := range plan.SelectedSkillProfiles {
		if !safeRuntimePackageSegment(profile) {
			return nil, nil, fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
		}
		extraRoots = append(extraRoots, "skills/"+profile)
	}
	for _, relativeRoot := range extraRoots {
		paths, pathsErr := formalWorkspacePackageTree(resolver, root, relativeRoot)
		if pathsErr != nil {
			return nil, nil, pathsErr
		}
		for _, logicalPath := range paths {
			if _, exists := seen[logicalPath]; exists {
				continue
			}
			entry, _, found, entryErr := optionalFormalWorkspaceEntry(resolver, root, logicalPath)
			if entryErr != nil {
				return nil, nil, entryErr
			}
			if !found {
				continue
			}
			if len(entries) >= formalWorkspacePackageMaxFiles || totalBytes+entry.SizeBytes > formalWorkspacePackageMaxBytes {
				return nil, nil, fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
			}
			seen[logicalPath] = len(entries)
			entries = append(entries, entry)
			totalBytes += entry.SizeBytes
		}
	}

	effectiveSkillHashes := make(map[string]string, len(skillHashes))
	for profile, hash := range skillHashes {
		effectiveSkillHashes[profile] = hash
		logicalPath := "skills/" + profile + "/SKILL.md"
		if index, exists := seen[logicalPath]; exists {
			effectiveSkillHashes[profile] = entries[index].SHA256
		}
	}
	return entries, effectiveSkillHashes, nil
}

// sealDynamicFormalWorkspaceEntries bridges the dynamic Workspace release to
// the stable test Adapter. The Adapter can consume inline bytes but cannot
// reliably resolve formal_workspace_ref entries. This retains the formal
// Workspace validation and digest check, then seals the exact bytes into the
// Run manifest. Static compatibility paths keep their reference behavior.
func (p FilesystemRuntimeManifestProvider) sealDynamicFormalWorkspaceEntries(run persistence.AgentRunRecord, plan runtimepkg.AgentRunPlan, frozen domain.RunWorkspaceContextRecord, entries []runtimepkg.RuntimeManifestEntry) ([]runtimepkg.RuntimeManifestEntry, error) {
	if plan.RoutingMode != "dynamic" || plan.ExecutionScope != string(runtimepkg.ScopeProductThread) {
		return entries, nil
	}
	if run.AgentRunID == "" || run.TenantID == "" || run.UserID == "" || run.WorkspaceID == "" ||
		plan.AgentRunID != run.AgentRunID || frozen.RunID != run.AgentRunID || frozen.AgentRunID != run.AgentRunID ||
		frozen.TenantID != run.TenantID || frozen.UserID != run.UserID || frozen.WorkspaceID != run.WorkspaceID {
		return nil, fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
	}

	resolver := workspacepkg.NewPathResolver(p.DataRoot, "")
	root, err := resolver.ResolveFormalWorkspace(run.TenantID, run.UserID, run.WorkspaceID)
	if err != nil || validateWorkspaceSnapshotRoot(resolver.DataRoot, root) != nil {
		return nil, fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
	}

	sealed := append([]runtimepkg.RuntimeManifestEntry(nil), entries...)
	for index, entry := range sealed {
		if entry.SourceType != "formal_workspace_ref" {
			continue
		}
		_, content, found, entryErr := optionalFormalWorkspaceEntry(resolver, root, entry.SourceRef)
		if entryErr != nil || !found || entry.SizeBytes != int64(len(content)) || entry.SHA256 != dispatcherSHA256(content) {
			return nil, fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
		}
		sealed[index] = runtimepkg.NewInlineRuntimeEntry(entry.LogicalPath, content)
	}
	return sealed, nil
}

// verifyFormalWorkspaceManifestEntries makes the filesystem-side build a
// stable byte snapshot. Dispatcher separately checks the durable Workspace
// version before and after Build; this catches a file replacement that occurs
// between an earlier formal-reference read and the returned manifest.
func (p FilesystemRuntimeManifestProvider) verifyFormalWorkspaceManifestEntries(run persistence.AgentRunRecord, entries []runtimepkg.RuntimeManifestEntry) error {
	formalEntries := make([]runtimepkg.RuntimeManifestEntry, 0)
	for _, entry := range entries {
		if entry.SourceType == "formal_workspace_ref" {
			formalEntries = append(formalEntries, entry)
		}
	}
	if len(formalEntries) == 0 {
		return nil
	}
	if run.TenantID == "" || run.UserID == "" || run.WorkspaceID == "" {
		return fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
	}
	resolver := workspacepkg.NewPathResolver(p.DataRoot, "")
	root, err := resolver.ResolveFormalWorkspace(run.TenantID, run.UserID, run.WorkspaceID)
	if err != nil || validateWorkspaceSnapshotRoot(resolver.DataRoot, root) != nil {
		return fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
	}
	for _, entry := range formalEntries {
		if entry.SourceRef == "" || entry.SizeBytes < 0 {
			return fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
		}
		target, targetErr := safeFormalWorkspaceReferenceTarget(root, entry.SourceRef)
		if targetErr != nil {
			return fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
		}
		info, statErr := os.Lstat(target)
		if statErr != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || rejectWorkspaceSymlink(root, target) != nil {
			return fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
		}
		content, readErr := os.ReadFile(target)
		if readErr != nil || int64(len(content)) != entry.SizeBytes || normalizeDispatcherHash(entry.SHA256) != dispatcherSHA256(content) {
			return fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
		}
	}
	return nil
}

// safeFormalWorkspaceReferenceTarget accepts the canonical logical references
// emitted by the provider, including valid Unicode Renshe material names, but
// retains the same root, traversal, separator, and link rejection guarantees
// as the individual materialization readers.
func safeFormalWorkspaceReferenceTarget(root, sourceRef string) (string, error) {
	if sourceRef == "" || sourceRef != strings.TrimSpace(sourceRef) || filepath.IsAbs(sourceRef) || strings.ContainsAny(sourceRef, "\x00\r\n\\:") {
		return "", fmt.Errorf("unsafe formal Workspace reference")
	}
	normalized := filepath.ToSlash(filepath.Clean(filepath.FromSlash(sourceRef)))
	if normalized != sourceRef || normalized == "." || normalized == ".." || strings.HasPrefix(normalized, "../") || strings.Contains(normalized, "/../") {
		return "", fmt.Errorf("unsafe formal Workspace reference")
	}
	for _, segment := range strings.Split(normalized, "/") {
		if segment == "" || segment == "." || segment == ".." {
			return "", fmt.Errorf("unsafe formal Workspace reference")
		}
	}
	target := filepath.Join(root, filepath.FromSlash(normalized))
	relative, err := filepath.Rel(root, target)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("unsafe formal Workspace reference")
	}
	return target, nil
}

func formalWorkspacePackageTree(resolver workspacepkg.PathResolver, workspaceRoot, relativeRoot string) ([]string, error) {
	directory, err := resolver.SafeJoin(workspaceRoot, relativeRoot)
	if err != nil {
		return nil, fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
	}
	info, err := os.Lstat(directory)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() || rejectWorkspaceSymlink(workspaceRoot, directory) != nil {
		return nil, fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
	}

	paths := []string{}
	err = filepath.WalkDir(directory, func(current string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if current == directory {
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("workspace package symlink")
		}
		if entry.IsDir() {
			return nil
		}
		if !entry.Type().IsRegular() || !formalWorkspacePackageExtension(entry.Name()) {
			return nil
		}
		relative, relErr := filepath.Rel(workspaceRoot, current)
		if relErr != nil {
			return relErr
		}
		logicalPath := filepath.ToSlash(filepath.Clean(relative))
		if logicalPath == "." || filepath.IsAbs(logicalPath) || strings.HasPrefix(logicalPath, "../") || strings.Contains(logicalPath, "/../") {
			return fmt.Errorf("workspace package path escape")
		}
		paths = append(paths, logicalPath)
		if len(paths) > formalWorkspacePackageMaxFiles {
			return fmt.Errorf("workspace package file limit")
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
	}
	sort.Strings(paths)
	return paths, nil
}

func formalWorkspacePackageExtension(name string) bool {
	switch strings.ToLower(filepath.Ext(name)) {
	case ".md", ".json", ".yaml", ".yml":
		return true
	default:
		return false
	}
}

type selfMediaCreationCardsManifest struct {
	SchemaVersion string `json:"schemaVersion"`
	CardCount     int    `json:"cardCount"`
	Cards         []struct {
		ID         string `json:"id"`
		Path       string `json:"path"`
		References []struct {
			Path   string `json:"path"`
			SHA256 string `json:"sha256"`
		} `json:"references"`
	} `json:"cards"`
}

func selfMediaCreationAgentPackageFiles(root string) ([]string, map[string]string, error) {
	manifestFiles, expectedHashes, err := selfMediaCreationKnowledgeManifestFiles(root)
	if err != nil {
		return nil, nil, err
	}
	required := append([]string{}, selfMediaCreationAgentFiles...)
	seen := make(map[string]bool, len(required)+len(manifestFiles))
	for _, relative := range required {
		seen[relative] = true
	}
	for _, relative := range manifestFiles {
		if !seen[relative] {
			required = append(required, relative)
			seen[relative] = true
		}
	}
	return required, expectedHashes, nil
}

func selfMediaCreationKnowledgeManifestFiles(root string) ([]string, map[string]string, error) {
	manifestPath := selfMediaCreationKnowledgeRoot + "/cards-manifest.json"
	raw, err := readRequiredPackageFile(root, manifestPath)
	if err != nil {
		return nil, nil, err
	}
	var manifest selfMediaCreationCardsManifest
	if json.Unmarshal(raw, &manifest) != nil ||
		manifest.SchemaVersion != "self_media_creation.knowledge_cards.v1" ||
		manifest.CardCount < 1 || manifest.CardCount > selfMediaCreationMaxCards ||
		len(manifest.Cards) != manifest.CardCount {
		return nil, nil, fmt.Errorf("invalid self-media knowledge manifest")
	}
	required := []string{manifestPath}
	expectedHashes := map[string]string{}
	seen := map[string]bool{manifestPath: true}
	seenCardIDs := map[string]bool{}
	referenceCount := 0
	for _, card := range manifest.Cards {
		cardID := strings.TrimSpace(card.ID)
		cardPath := strings.TrimSpace(card.Path)
		if cardID == "" || cardID != card.ID || !strings.HasPrefix(cardID, "SM") || seenCardIDs[cardID] ||
			cardPath == "" || cardPath != card.Path || !strings.HasSuffix(cardPath, ".md") || !safeRuntimeRelativePath(cardPath) {
			return nil, nil, fmt.Errorf("invalid self-media knowledge card")
		}
		relative := selfMediaCreationKnowledgeRoot + "/" + cardPath
		if seen[relative] {
			return nil, nil, fmt.Errorf("duplicate self-media knowledge card")
		}
		if _, err := safeRelativeFile(root, relative); err != nil {
			return nil, nil, err
		}
		seenCardIDs[cardID] = true
		seen[relative] = true
		required = append(required, relative)

		referenceRoot := strings.TrimSuffix(cardPath, ".md") + "-references/"
		for _, reference := range card.References {
			referencePath := strings.TrimSpace(reference.Path)
			expectedHash := strings.ToLower(strings.TrimSpace(reference.SHA256))
			if referenceCount >= selfMediaCreationMaxReferences || referencePath == "" || referencePath != reference.Path ||
				!strings.HasSuffix(referencePath, ".md") || !safeRuntimeRelativePath(referencePath) ||
				!strings.HasPrefix(referencePath, referenceRoot) || strings.TrimPrefix(referencePath, referenceRoot) == "" ||
				expectedHash != reference.SHA256 || !selfMediaCreationSHA256Pattern.MatchString(expectedHash) {
				return nil, nil, fmt.Errorf("invalid self-media knowledge reference")
			}
			referenceRelative := selfMediaCreationKnowledgeRoot + "/" + referencePath
			if seen[referenceRelative] {
				return nil, nil, fmt.Errorf("duplicate self-media knowledge reference")
			}
			if _, err := safeRelativeFile(root, referenceRelative); err != nil {
				return nil, nil, err
			}
			seen[referenceRelative] = true
			referenceCount++
			required = append(required, referenceRelative)
			expectedHashes[referenceRelative] = expectedHash
		}
	}
	return required, expectedHashes, nil
}

func runtimePackageRoot(configRoot, preferredDir, fallbackDir, profile, entryFile string) (string, error) {
	if strings.TrimSpace(configRoot) == "" || !safeRuntimePackageSegment(profile) {
		return "", fmt.Errorf("runtime package unavailable")
	}
	for _, directory := range []string{preferredDir, fallbackDir} {
		root := filepath.Join(configRoot, directory, profile)
		entry := filepath.Join(root, entryFile)
		info, err := os.Lstat(entry)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			return "", fmt.Errorf("runtime package unavailable")
		}
		return root, nil
	}
	return "", fmt.Errorf("runtime package unavailable")
}

// runtimeAgentPackageRoot consumes the catalog's frozen logical agents/... path.
// A dynamic release must materialize only from its read-only runtime-agents
// mirror. The older agents/... tree is reserved for explicitly non-dynamic
// compatibility bundles and cannot become a dynamic Run fallback.
func runtimeAgentPackageRoot(configRoot, logicalRoot, entryFile string) (string, error) {
	if strings.TrimSpace(configRoot) == "" || !safeDynamicAgentRelativeRoot(logicalRoot) {
		return "", fmt.Errorf("runtime package unavailable")
	}
	suffix := strings.TrimPrefix(logicalRoot, "agents/")
	root := filepath.Join(configRoot, "runtime-agents", filepath.FromSlash(suffix))
	entry := filepath.Join(root, entryFile)
	info, err := os.Lstat(entry)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return "", fmt.Errorf("runtime package unavailable")
	}
	return root, nil
}

// activeRuntimeSkillPackageRoot is the dynamic counterpart to
// runtimePackageRoot. Dynamic plans are backed only by the signed release
// mirror; legacy skills/... files must not satisfy a frozen Plan.
func activeRuntimeSkillPackageRoot(configRoot, profile string) (string, error) {
	if strings.TrimSpace(configRoot) == "" || !safeRuntimePackageSegment(profile) {
		return "", fmt.Errorf("runtime package unavailable")
	}
	root := filepath.Join(configRoot, "runtime-skills", profile)
	if _, err := readRequiredPackageFile(root, "SKILL.md"); err != nil {
		return "", fmt.Errorf("runtime package unavailable")
	}
	return root, nil
}

func readRequiredPackageFile(root, relative string) ([]byte, error) {
	target, err := safeRelativeFile(root, relative)
	if err != nil {
		return nil, err
	}
	content, err := os.ReadFile(target)
	if err != nil || len(content) > 256*1024 {
		return nil, fmt.Errorf("runtime package unavailable")
	}
	return content, nil
}

func (p FilesystemRuntimeManifestProvider) huokeTopicWorkspaceEntries(run persistence.AgentRunRecord, plan runtimepkg.AgentRunPlan, frozen domain.RunWorkspaceContextRecord) ([]runtimepkg.RuntimeManifestEntry, error) {
	if plan.TaskType != huokeTopicTaskType {
		return nil, nil
	}
	if run.AgentRunID == "" || run.TenantID == "" || run.UserID == "" || run.WorkspaceID == "" ||
		plan.AgentRunID != run.AgentRunID || frozen.RunID != run.AgentRunID || frozen.AgentRunID != run.AgentRunID ||
		frozen.TenantID != run.TenantID || frozen.UserID != run.UserID || frozen.WorkspaceID != run.WorkspaceID {
		return nil, fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
	}
	if plan.WorkspaceVersion != frozen.WorkspaceVersion || plan.IndexVersion != frozen.IndexVersion {
		return nil, fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
	}
	resolver := workspacepkg.NewPathResolver(p.DataRoot, "")
	root, err := resolver.ResolveFormalWorkspace(run.TenantID, run.UserID, run.WorkspaceID)
	if err != nil || validateWorkspaceSnapshotRoot(resolver.DataRoot, root) != nil {
		return nil, fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
	}

	entries := []runtimepkg.RuntimeManifestEntry{}
	optional := []string{
		"resources/overview.md",
		"resources/profile.md",
		"resources/materials.md",
		"profile/user-positioning/positioning-profile.md",
		"profile/preference-boundaries.md",
	}
	var materialsIndex []byte
	for _, relative := range optional {
		entry, content, found, entryErr := optionalFormalWorkspaceEntry(resolver, root, relative)
		if entryErr != nil {
			return nil, entryErr
		}
		if !found {
			continue
		}
		entries = append(entries, entry)
		if relative == "resources/materials.md" {
			materialsIndex = content
		}
	}

	materialEntries, err := huokeTopicMaterialEntries(resolver, root, materialsIndex)
	if err != nil {
		return nil, err
	}
	entries = append(entries, materialEntries...)
	if run.ThreadID == "" || run.TaskID == "" {
		return nil, fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
	}
	state := map[string]any{}
	if p.HuokeTopicStateLoader != nil {
		state = p.HuokeTopicStateLoader(run.UserID, run.WorkspaceID, run.ThreadID, run.TaskID)
	}
	if len(state) == 0 {
		var bootstrapErr error
		state, bootstrapErr = runtimepkg.NewHuokeTopicInitialConsultationState(fmt.Sprintf("wv:%d|iv:%d|cg:%d", frozen.WorkspaceVersion, frozen.IndexVersion, frozen.ContextGeneration))
		if bootstrapErr != nil {
			return nil, fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
		}
	}
	if runtimepkg.ValidateHuokeTopicConsultationState(state) != nil {
		return nil, fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
	}
	raw, marshalErr := json.Marshal(state)
	if marshalErr != nil || len(raw) > 256*1024 {
		return nil, fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
	}
	entries = append(entries, runtimepkg.NewInlineRuntimeEntry("input/consultation_state.json", raw))
	return entries, nil
}

func (p FilesystemRuntimeManifestProvider) selfMediaCreationWorkspaceEntries(run persistence.AgentRunRecord, plan runtimepkg.AgentRunPlan, frozen domain.RunWorkspaceContextRecord) ([]runtimepkg.RuntimeManifestEntry, error) {
	if plan.TaskType != selfMediaCreationTaskType {
		return nil, nil
	}
	if run.AgentRunID == "" || run.TenantID == "" || run.UserID == "" || run.WorkspaceID == "" ||
		plan.AgentRunID != run.AgentRunID || frozen.RunID != run.AgentRunID || frozen.AgentRunID != run.AgentRunID ||
		frozen.TenantID != run.TenantID || frozen.UserID != run.UserID || frozen.WorkspaceID != run.WorkspaceID {
		return nil, fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
	}
	resolver := workspacepkg.NewPathResolver(p.DataRoot, "")
	root, err := resolver.ResolveFormalWorkspace(run.TenantID, run.UserID, run.WorkspaceID)
	if err != nil || validateWorkspaceSnapshotRoot(resolver.DataRoot, root) != nil {
		return nil, fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
	}

	entries := []runtimepkg.RuntimeManifestEntry{}
	optional := []string{
		"resources/overview.md",
		"resources/profile.md",
		"resources/materials.md",
		"profile/user-positioning/positioning-profile.md",
		"profile/preference-boundaries.md",
	}
	var materialsIndex []byte
	for _, relative := range optional {
		entry, content, found, entryErr := optionalFormalWorkspaceEntry(resolver, root, relative)
		if entryErr != nil {
			return nil, entryErr
		}
		if !found {
			continue
		}
		entries = append(entries, entry)
		if relative == "resources/materials.md" {
			materialsIndex = content
		}
	}
	materialEntries, err := huokeTopicMaterialEntries(resolver, root, materialsIndex)
	if err != nil {
		return nil, err
	}
	return append(entries, materialEntries...), nil
}

func (p FilesystemRuntimeManifestProvider) rensheContentWorkspaceEntries(run persistence.AgentRunRecord, plan runtimepkg.AgentRunPlan, frozen domain.RunWorkspaceContextRecord) ([]runtimepkg.RuntimeManifestEntry, error) {
	if plan.TaskType != rensheContentTaskType {
		return nil, nil
	}
	if run.AgentRunID == "" || run.TenantID == "" || run.UserID == "" || run.WorkspaceID == "" ||
		plan.AgentRunID != run.AgentRunID || frozen.RunID != run.AgentRunID || frozen.AgentRunID != run.AgentRunID ||
		frozen.TenantID != run.TenantID || frozen.UserID != run.UserID || frozen.WorkspaceID != run.WorkspaceID {
		return nil, fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
	}
	resolver := workspacepkg.NewPathResolver(p.DataRoot, "")
	root, err := resolver.ResolveFormalWorkspace(run.TenantID, run.UserID, run.WorkspaceID)
	if err != nil || validateWorkspaceSnapshotRoot(resolver.DataRoot, root) != nil {
		return nil, fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
	}

	optional := []string{
		"resources/overview.md",
		"resources/profile.md",
		"resources/materials.md",
		"resources/creative.md",
		"resources/files.md",
		"profile/user-positioning/positioning-profile.md",
		"profile/preference-boundaries.md",
		"内容.md",
	}
	entries := []runtimepkg.RuntimeManifestEntry{}
	materialIndexes := [][]byte{}
	for _, relative := range optional {
		entry, content, found, entryErr := optionalFormalWorkspaceEntry(resolver, root, relative)
		if entryErr != nil {
			return nil, entryErr
		}
		if !found {
			continue
		}
		entries = append(entries, entry)
		if relative == "resources/materials.md" {
			materialIndexes = append(materialIndexes, content)
		}
	}
	materialEntries, err := rensheContentMaterialEntries(root, run, materialIndexes)
	if err != nil {
		return nil, err
	}
	entries = append(entries, materialEntries...)
	route := rensheContentEvidenceRoute(entries)
	entries = append(entries, runtimepkg.NewInlineRuntimeEntry("resources/renshe-evidence-route.md", []byte(route)))
	return entries, nil
}

type rensheIndexedMaterial struct {
	Paths []string
	Keys  []string
}

func rensheContentMaterialEntries(root string, run persistence.AgentRunRecord, indexes [][]byte) ([]runtimepkg.RuntimeManifestEntry, error) {
	query := rensheContentMaterialQuery(run)
	candidates := []rensheIndexedMaterial{}
	for _, index := range indexes {
		candidates = append(candidates, rensheIndexedMaterials(string(index))...)
	}
	for _, direct := range rensheBacktickedMaterialRefs(query) {
		candidates = append(candidates, rensheIndexedMaterial{Paths: []string{direct}, Keys: []string{direct}})
	}
	entries := []runtimepkg.RuntimeManifestEntry{}
	seen := map[string]bool{}
	var totalBytes int64
	for _, candidate := range candidates {
		if !rensheMaterialCandidateRequested(query, candidate) {
			continue
		}
		for _, path := range renshePreferredMaterialPaths(candidate.Paths, query) {
			if len(entries) >= rensheContentMaxMaterialFiles {
				return entries, nil
			}
			if seen[path] {
				continue
			}
			seen[path] = true
			entry, found, err := optionalRensheMaterialEntry(root, path)
			if err != nil {
				return nil, err
			}
			if !found || totalBytes+entry.SizeBytes > rensheContentMaxMaterialBytes {
				continue
			}
			entries = append(entries, entry)
			totalBytes += entry.SizeBytes
			break
		}
	}
	return entries, nil
}

func optionalRensheMaterialEntry(root, relative string) (runtimepkg.RuntimeManifestEntry, bool, error) {
	normalized, ok := normalizeRensheMaterialRef(relative)
	if !ok {
		return runtimepkg.RuntimeManifestEntry{}, false, fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
	}
	cleanRoot := filepath.Clean(root)
	target := filepath.Join(cleanRoot, filepath.FromSlash(normalized))
	relativeToRoot, err := filepath.Rel(cleanRoot, target)
	if err != nil || relativeToRoot == ".." || strings.HasPrefix(relativeToRoot, ".."+string(filepath.Separator)) {
		return runtimepkg.RuntimeManifestEntry{}, false, fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
	}
	info, err := os.Lstat(target)
	if os.IsNotExist(err) {
		return runtimepkg.RuntimeManifestEntry{}, false, nil
	}
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Size() < 1 || info.Size() > huokeTopicMaxWorkspaceFileBytes || rejectWorkspaceSymlink(cleanRoot, target) != nil {
		return runtimepkg.RuntimeManifestEntry{}, false, fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
	}
	content, err := os.ReadFile(target)
	if err != nil || int64(len(content)) != info.Size() {
		return runtimepkg.RuntimeManifestEntry{}, false, fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
	}
	return runtimepkg.RuntimeManifestEntry{
		LogicalPath: normalized,
		SourceType:  "formal_workspace_ref",
		SourceRef:   normalized,
		SizeBytes:   int64(len(content)),
		SHA256:      dispatcherSHA256(content),
	}, true, nil
}

func rensheContentMaterialQuery(run persistence.AgentRunRecord) string {
	parts := []string{}
	input := aiWorkerMap(run.RequestSnapshot["input"])
	if text := strings.TrimSpace(workerMapString(input, "text")); text != "" {
		parts = append(parts, text)
	}
	for _, value := range aiWorkerMap(run.RequestSnapshot["businessRefs"]) {
		if text := strings.TrimSpace(fmt.Sprint(value)); text != "" && text != "<nil>" {
			parts = append(parts, text)
		}
	}
	return strings.Join(parts, "\n")
}

func rensheIndexedMaterials(markdown string) []rensheIndexedMaterial {
	out := []rensheIndexedMaterial{}
	for _, line := range strings.Split(markdown, "\n") {
		paths := rensheBacktickedMaterialRefs(line)
		if len(paths) == 0 {
			continue
		}
		keys := []string{}
		columns := strings.Split(line, "|")
		if len(columns) > 2 {
			keys = append(keys, strings.Trim(strings.TrimSpace(columns[1]), "`"), strings.TrimSpace(columns[2]))
		}
		for _, path := range paths {
			name := filepath.Base(filepath.FromSlash(path))
			keys = append(keys, path, name, strings.TrimSuffix(name, filepath.Ext(name)))
		}
		out = append(out, rensheIndexedMaterial{Paths: paths, Keys: keys})
	}
	return out
}

func rensheBacktickedMaterialRefs(text string) []string {
	out := []string{}
	remaining := text
	for {
		start := strings.Index(remaining, "`")
		if start < 0 {
			return out
		}
		remaining = remaining[start+1:]
		end := strings.Index(remaining, "`")
		if end < 0 {
			return out
		}
		if normalized, ok := normalizeRensheMaterialRef(remaining[:end]); ok {
			out = append(out, normalized)
		}
		remaining = remaining[end+1:]
	}
}

func normalizeRensheMaterialRef(value string) (string, bool) {
	value = strings.TrimSpace(value)
	if value == "" || filepath.IsAbs(value) || strings.ContainsAny(value, "\x00\r\n:\\") {
		return "", false
	}
	normalized := filepath.ToSlash(filepath.Clean(filepath.FromSlash(value)))
	if normalized == "." || strings.HasPrefix(normalized, "../") || strings.Contains(normalized, "/../") ||
		!strings.HasSuffix(strings.ToLower(normalized), ".md") ||
		!strings.HasPrefix(normalized, "materials/raw/") && !strings.HasPrefix(normalized, "materials/processed/") {
		return "", false
	}
	return normalized, true
}

func rensheMaterialCandidateRequested(query string, candidate rensheIndexedMaterial) bool {
	normalizedQuery := strings.ToLower(strings.TrimSpace(query))
	if normalizedQuery == "" {
		return false
	}
	for _, key := range candidate.Keys {
		normalizedKey := strings.ToLower(strings.Trim(strings.TrimSpace(key), "`"))
		if !rensheMaterialKeySpecific(normalizedKey) {
			continue
		}
		if strings.Contains(normalizedQuery, normalizedKey) {
			return true
		}
	}
	return false
}

func renshePreferredMaterialPaths(paths []string, query string) []string {
	preferRaw := false
	normalizedQuery := strings.ToLower(query)
	for _, marker := range []string{"原文", "原始", "转写", "文字记录", "raw", "transcript"} {
		if strings.Contains(normalizedQuery, marker) {
			preferRaw = true
			break
		}
	}
	processed := []string{}
	raw := []string{}
	for _, path := range paths {
		if strings.HasPrefix(path, "materials/processed/") {
			processed = append(processed, path)
		} else if strings.HasPrefix(path, "materials/raw/") {
			raw = append(raw, path)
		}
	}
	if preferRaw {
		return append(raw, processed...)
	}
	return append(processed, raw...)
}

func rensheMaterialKeySpecific(value string) bool {
	if value == "" {
		return false
	}
	switch value {
	case "周会", "会议", "纪要", "材料", "文字记录":
		return false
	}
	return len([]rune(value)) >= 2
}

func rensheContentEvidenceRoute(entries []runtimepkg.RuntimeManifestEntry) string {
	lines := []string{
		"# Renshe Evidence Route",
		"",
		"The current user request is the primary material. Judge its four value routes before using profile or personal evidence.",
		"Profile and personal history are optional supporting evidence. They never decide whether the current material is worth watching.",
		"Historical material files are included only when the current request or business reference identifies them. Do not search for or infer unavailable files.",
		"If a requested historical material is not listed, use the material index to offer precise choices instead of scanning the workspace.",
		"",
		"Available fixed workspace files:",
	}
	fixed := []string{}
	materials := []string{}
	for _, entry := range entries {
		if entry.SourceType == "formal_workspace_ref" {
			if strings.HasPrefix(entry.LogicalPath, "materials/") {
				materials = append(materials, entry.LogicalPath)
			} else {
				fixed = append(fixed, entry.LogicalPath)
			}
		}
	}
	sort.Strings(fixed)
	sort.Strings(materials)
	if len(fixed) == 0 {
		lines = append(lines, "- none")
	} else {
		for _, path := range fixed {
			lines = append(lines, "- `"+path+"`")
		}
	}
	lines = append(lines, "", "Selected historical material files:")
	if len(materials) == 0 {
		lines = append(lines, "- none")
	} else {
		for _, path := range materials {
			lines = append(lines, "- `"+path+"`")
		}
	}
	return strings.Join(lines, "\n") + "\n"
}

func (p FilesystemRuntimeManifestProvider) fayaGerminationWorkspaceEntries(run persistence.AgentRunRecord, plan runtimepkg.AgentRunPlan, frozen domain.RunWorkspaceContextRecord) ([]runtimepkg.RuntimeManifestEntry, error) {
	if plan.TaskType != fayaGerminationTaskType {
		return nil, nil
	}
	if run.AgentRunID == "" || run.TenantID == "" || run.UserID == "" || run.WorkspaceID == "" ||
		plan.AgentRunID != run.AgentRunID || frozen.RunID != run.AgentRunID || frozen.AgentRunID != run.AgentRunID ||
		frozen.TenantID != run.TenantID || frozen.UserID != run.UserID || frozen.WorkspaceID != run.WorkspaceID {
		return nil, fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
	}
	resolver := workspacepkg.NewPathResolver(p.DataRoot, "")
	root, err := resolver.ResolveFormalWorkspace(run.TenantID, run.UserID, run.WorkspaceID)
	if err != nil || validateWorkspaceSnapshotRoot(resolver.DataRoot, root) != nil {
		return nil, fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
	}

	optional := []string{
		"resources/overview.md",
		"resources/profile.md",
		"resources/materials.md",
		"profile/user-positioning/positioning-profile.md",
		"profile/preference-boundaries.md",
	}
	entries := make([]runtimepkg.RuntimeManifestEntry, 0, len(optional))
	for _, relative := range optional {
		entry, _, found, entryErr := optionalFormalWorkspaceEntry(resolver, root, relative)
		if entryErr != nil {
			return nil, entryErr
		}
		if found {
			entries = append(entries, entry)
		}
	}
	return entries, nil
}

func huokeTopicMaterialEntries(resolver workspacepkg.PathResolver, root string, index []byte) ([]runtimepkg.RuntimeManifestEntry, error) {
	if len(index) == 0 {
		return nil, nil
	}
	if huokeTopicUnsafeIndexPath.Match(index) {
		return nil, fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
	}
	matches := huokeTopicMaterialPath.FindAllString(string(index), -1)
	seen := map[string]bool{}
	references := make([]string, 0, len(matches))
	for _, match := range matches {
		reference := strings.TrimRight(match, ".,;:")
		// Workspace navigation may document a template such as
		// materials/raw/<materialId>.md. It is not a concrete file reference.
		if strings.Contains(reference, "<") {
			continue
		}
		if !huokeTopicSafeMaterialRef.MatchString(reference) || strings.Contains(reference, "..") || strings.ContainsAny(reference, `:\`) {
			return nil, fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
		}
		if !seen[reference] {
			seen[reference] = true
			references = append(references, reference)
		}
	}
	entries := []runtimepkg.RuntimeManifestEntry{}
	for _, reference := range references {
		if len(entries) >= huokeTopicMaxMaterialFiles {
			break
		}
		entry, _, found, err := optionalFormalWorkspaceEntry(resolver, root, reference)
		if err != nil {
			return nil, err
		}
		if found {
			entries = append(entries, entry)
		}
	}
	return entries, nil
}

func optionalFormalWorkspaceEntry(resolver workspacepkg.PathResolver, root, relative string) (runtimepkg.RuntimeManifestEntry, []byte, bool, error) {
	target, err := resolver.SafeJoin(root, relative)
	if err != nil {
		return runtimepkg.RuntimeManifestEntry{}, nil, false, fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
	}
	info, err := os.Lstat(target)
	if os.IsNotExist(err) {
		return runtimepkg.RuntimeManifestEntry{}, nil, false, nil
	}
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Size() > huokeTopicMaxWorkspaceFileBytes || rejectWorkspaceSymlink(root, target) != nil {
		return runtimepkg.RuntimeManifestEntry{}, nil, false, fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
	}
	content, err := os.ReadFile(target)
	if err != nil || int64(len(content)) != info.Size() {
		return runtimepkg.RuntimeManifestEntry{}, nil, false, fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
	}
	entry := runtimepkg.RuntimeManifestEntry{
		LogicalPath: relative,
		SourceType:  "formal_workspace_ref",
		SourceRef:   relative,
		SizeBytes:   int64(len(content)),
		SHA256:      dispatcherSHA256(content),
	}
	return entry, content, true, nil
}

func validateWorkspaceSnapshotRoot(dataRoot, root string) error {
	base := filepath.Join(filepath.Clean(dataRoot), "workspaces")
	baseInfo, err := os.Lstat(base)
	if err != nil || baseInfo.Mode()&os.ModeSymlink != 0 || !baseInfo.IsDir() {
		return fmt.Errorf("unsafe workspace root")
	}
	info, err := os.Lstat(root)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("unsafe workspace root")
	}
	return rejectWorkspaceSymlink(base, root)
}

func rejectWorkspaceSymlink(root, target string) error {
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return err
	}
	absTarget, err := filepath.Abs(target)
	if err != nil {
		return err
	}
	relative, err := filepath.Rel(absRoot, absTarget)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return fmt.Errorf("workspace path escape")
	}
	current := absRoot
	for _, part := range strings.Split(relative, string(filepath.Separator)) {
		if part == "" || part == "." {
			continue
		}
		current = filepath.Join(current, part)
		info, statErr := os.Lstat(current)
		if statErr != nil {
			return statErr
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("workspace symlink")
		}
	}
	return nil
}

func safeRelativeFile(root, relative string) (string, error) {
	if strings.TrimSpace(root) == "" || strings.TrimSpace(relative) == "" || filepath.IsAbs(relative) || strings.Contains(relative, "..") || strings.ContainsAny(relative, `:\`) {
		return "", fmt.Errorf("unsafe runtime package path")
	}
	rootInfo, err := os.Lstat(root)
	if err != nil || rootInfo.Mode()&os.ModeSymlink != 0 || !rootInfo.IsDir() {
		return "", fmt.Errorf("unsafe runtime package path")
	}
	target := filepath.Join(root, filepath.FromSlash(relative))
	rel, err := filepath.Rel(root, target)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("unsafe runtime package path")
	}
	current := root
	for _, part := range strings.Split(filepath.Clean(rel), string(filepath.Separator)) {
		if !safeRuntimePackageSegment(part) {
			return "", fmt.Errorf("unsafe runtime package path")
		}
		current = filepath.Join(current, part)
		info, statErr := os.Lstat(current)
		if statErr != nil || info.Mode()&os.ModeSymlink != 0 {
			return "", fmt.Errorf("unsafe runtime package path")
		}
	}
	return target, nil
}

func safeRuntimePackageSegment(value string) bool {
	if value == "" || value == "." || value == ".." {
		return false
	}
	for _, r := range value {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '-' || r == '.' {
			continue
		}
		return false
	}
	return true
}

func containsRuntimeProfile(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
