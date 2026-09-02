package runtime

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"sort"
	"strings"

	"huahuoai/backend/source/internal/domain"
)

type SkillProfile struct {
	SkillProfile    string   `json:"skillProfile"`
	Status          string   `json:"status"`
	TaskTypes       []string `json:"taskTypes"`
	Version         string   `json:"version"`
	Hash            string   `json:"hash,omitempty"`
	ManifestVersion string   `json:"manifestVersion,omitempty"`
	MetaFileID      string   `json:"metaFileId,omitempty"`
	RelativePath    string   `json:"relativePath,omitempty"`
	Content         string   `json:"-"`
}

type SkillHashSummary struct {
	SkillProfile    string `json:"skillProfile"`
	Hash            string `json:"hash"`
	Version         string `json:"version"`
	ManifestVersion string `json:"manifestVersion,omitempty"`
}

// SkillCandidatePolicy is the server-owned subset of an L1 Agent manifest
// needed to select Skills for one dynamic Run. It deliberately contains no
// model, client, or filesystem input.
type SkillCandidatePolicy struct {
	AgentProfile           string   `json:"agentProfile"`
	CandidateSkillProfiles []string `json:"candidateSkillProfiles"`
	MaxCandidateSkills     int      `json:"maxCandidateSkills"`
}

// SkillCandidateCatalog is the server-owned planning catalog consumed by the
// dynamic selection APIs. MetaMirror remains the source for rendering actual
// SKILL.md files; this catalog binds the selected profiles to the frozen
// AgentRun planning facts.
type SkillCandidateCatalog struct {
	Skills      []PlannedSkill            `json:"skills"`
	Permissions PlannerPermissionSnapshot `json:"permissions"`
	Policies    []SkillCandidatePolicy    `json:"policies"`
}

type SkillRegistry struct {
	Mirror               MetaMirror
	candidateSkills      []PlannedSkill
	candidatePermissions PlannerPermissionSnapshot
	candidatePolicies    []SkillCandidatePolicy
}

func NewSkillRegistry(mirror MetaMirror) SkillRegistry {
	if strings.TrimSpace(mirror.ConfigRoot) == "" {
		mirror = NewMetaMirror("")
	}
	return SkillRegistry{Mirror: mirror}
}

// NewSkillRegistryWithCandidateCatalog creates the dynamic planning view of
// the registry. Callers supply only the Backend-owned catalog and the selected
// L1 route; this constructor never accepts candidate data from an App request
// or model output.
func NewSkillRegistryWithCandidateCatalog(catalog SkillCandidateCatalog) SkillRegistry {
	policies := make([]SkillCandidatePolicy, 0, len(catalog.Policies))
	for _, policy := range catalog.Policies {
		policies = append(policies, SkillCandidatePolicy{
			AgentProfile:           strings.TrimSpace(policy.AgentProfile),
			CandidateSkillProfiles: append([]string(nil), policy.CandidateSkillProfiles...),
			MaxCandidateSkills:     policy.MaxCandidateSkills,
		})
	}
	return SkillRegistry{
		candidateSkills:      clonePlannedSkills(catalog.Skills),
		candidatePermissions: clonePlannerPermissions(catalog.Permissions),
		candidatePolicies:    policies,
	}
}

// ListCandidates resolves the active, authorized Skills eligible for the
// supplied Agent and TaskIntent. Results are ordered by explicit priority then
// stable skillProfile and bounded by the L1 manifest's maxCandidateSkills.
// It never introduces a general_chat fallback.
func (r SkillRegistry) ListCandidates(agentProfile string, intent domain.TaskIntent) ([]PlannedSkill, error) {
	policy, err := r.candidatePolicy(agentProfile)
	if err != nil {
		return nil, err
	}
	candidates := r.eligibleCandidates(policy, intent)
	if len(candidates) == 0 {
		return nil, domain.ErrorCode("SKILL_UNAVAILABLE")
	}
	if len(candidates) > policy.MaxCandidateSkills {
		candidates = candidates[:policy.MaxCandidateSkills]
	}
	return clonePlannedSkills(candidates), nil
}

// ValidateSelection accepts only an explicit, unique selection from the
// server-owned candidate set. It is used after selection as a separate
// fail-closed guard so a caller cannot inject an unregistered, disabled, or
// unauthorized Skill before the AgentRun plan is frozen.
func (r SkillRegistry) ValidateSelection(agentProfile string, intent domain.TaskIntent, selected []string) ([]PlannedSkill, error) {
	policy, err := r.candidatePolicy(agentProfile)
	if err != nil {
		return nil, err
	}
	if len(selected) == 0 || len(selected) > policy.MaxCandidateSkills {
		return nil, domain.ErrorCode("AGENT_PLAN_INVALID")
	}
	candidates := r.eligibleCandidates(policy, intent)
	if len(candidates) == 0 {
		return nil, domain.ErrorCode("SKILL_UNAVAILABLE")
	}
	if len(candidates) > policy.MaxCandidateSkills {
		candidates = candidates[:policy.MaxCandidateSkills]
	}
	requested := make(map[string]bool, len(selected))
	for _, profile := range selected {
		profile = strings.TrimSpace(profile)
		if profile == "" || requested[profile] {
			return nil, domain.ErrorCode("AGENT_PLAN_INVALID")
		}
		requested[profile] = true
	}
	validated := make([]PlannedSkill, 0, len(selected))
	for _, candidate := range candidates {
		if requested[candidate.SkillProfile] {
			validated = append(validated, candidate)
		}
	}
	if len(validated) != len(selected) {
		return nil, domain.ErrorCode("SKILL_UNAVAILABLE")
	}
	return clonePlannedSkills(validated), nil
}

func (r SkillRegistry) candidatePolicy(agentProfile string) (SkillCandidatePolicy, error) {
	agentProfile = strings.TrimSpace(agentProfile)
	if agentProfile == "" || !safeCatalogIdentifier(agentProfile) {
		return SkillCandidatePolicy{}, domain.ErrorCode("AGENT_PROFILE_UNAVAILABLE")
	}
	var policy SkillCandidatePolicy
	for _, candidate := range r.candidatePolicies {
		if candidate.AgentProfile != agentProfile {
			continue
		}
		if policy.AgentProfile != "" {
			return SkillCandidatePolicy{}, domain.ErrorCode("AGENT_PLAN_INVALID")
		}
		policy = candidate
	}
	if policy.AgentProfile == "" {
		return SkillCandidatePolicy{}, domain.ErrorCode("AGENT_PROFILE_UNAVAILABLE")
	}
	if !r.candidatePermissions.AllowedAgents[agentProfile] {
		return SkillCandidatePolicy{}, domain.ErrorCode("AGENT_PROFILE_UNAVAILABLE")
	}
	if policy.MaxCandidateSkills < 1 || policy.MaxCandidateSkills > 8 || len(policy.CandidateSkillProfiles) == 0 {
		return SkillCandidatePolicy{}, domain.ErrorCode("AGENT_PLAN_INVALID")
	}
	seen := map[string]bool{}
	for _, rawProfile := range policy.CandidateSkillProfiles {
		profile := strings.TrimSpace(rawProfile)
		if rawProfile != profile || !safeCatalogIdentifier(profile) || seen[profile] {
			return SkillCandidatePolicy{}, domain.ErrorCode("AGENT_PLAN_INVALID")
		}
		seen[profile] = true
	}
	return policy, nil
}

func (r SkillRegistry) eligibleCandidates(policy SkillCandidatePolicy, intent domain.TaskIntent) []PlannedSkill {
	if strings.TrimSpace(intent.ResolvedTaskType) == "" {
		return nil
	}
	seen := map[string]bool{}
	candidates := make([]PlannedSkill, 0, len(policy.CandidateSkillProfiles))
	for _, skill := range r.candidateSkills {
		profile := strings.TrimSpace(skill.SkillProfile)
		if profile == "" || profile != skill.SkillProfile || !safeCatalogIdentifier(profile) || seen[profile] || !containsExact(policy.CandidateSkillProfiles, profile) || !r.candidatePermissions.AllowedSkills[profile] {
			continue
		}
		if skill.Status != "active" || strings.TrimSpace(skill.Hash) == "" || !matchesRequiredOne(skill.TaskTypes, intent.ResolvedTaskType) ||
			(len(skill.IntentCategories) > 0 && !matchesOne(skill.IntentCategories, intent.Category)) ||
			(len(skill.AllowedAgentProfiles) > 0 && !matchesOne(skill.AllowedAgentProfiles, policy.AgentProfile)) ||
			!requiredCapabilitiesSatisfied(skill.RequiredCapabilities, intent.RequiredCapabilities) {
			continue
		}
		seen[profile] = true
		candidates = append(candidates, clonePlannedSkill(skill))
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].Priority != candidates[j].Priority {
			return candidates[i].Priority > candidates[j].Priority
		}
		return candidates[i].SkillProfile < candidates[j].SkillProfile
	})
	return candidates
}

func (r SkillRegistry) Load(skillProfile string) (SkillProfile, error) {
	mirror := r.Mirror
	if strings.TrimSpace(mirror.ConfigRoot) == "" {
		mirror = NewMetaMirror("")
	}
	entry, err := mirror.ResolveRuntimeSkillFile(skillProfile)
	if err != nil {
		return SkillProfile{}, fmt.Errorf("SKILL_UNAVAILABLE")
	}
	data, err := os.ReadFile(entry.Path)
	if err != nil {
		return SkillProfile{}, fmt.Errorf("SKILL_UNAVAILABLE")
	}
	content := string(data)
	requestedProfile := valueOrDefault(fieldFromSkill(content, "skillProfile"), skillProfile)
	if requestedProfile != skillProfile || entry.SkillProfile != skillProfile {
		return SkillProfile{}, fmt.Errorf("SKILL_UNAVAILABLE")
	}
	status := valueOrDefault(fieldFromSkill(content, "status"), entry.Status)
	if entry.Status != "active" {
		status = entry.Status
	}
	hash := skillContentHash(content)
	manifestHash := normalizeSkillHash(entry.Hash)
	if manifestHash != "" && manifestHash != hash {
		return SkillProfile{}, fmt.Errorf("SKILL_UNAVAILABLE")
	}
	frontmatterHash := normalizeSkillHash(fieldFromSkill(content, "hash"))
	if frontmatterHash != "" && frontmatterHash != hash {
		return SkillProfile{}, fmt.Errorf("SKILL_UNAVAILABLE")
	}
	return SkillProfile{
		SkillProfile:    skillProfile,
		Status:          status,
		TaskTypes:       taskTypesFromSkill(content),
		Version:         valueOrDefault(fieldFromSkill(content, "version"), valueOrDefault(entry.Version, "v0.1.0")),
		Hash:            hash,
		ManifestVersion: entry.ManifestVersion,
		MetaFileID:      entry.MetaFileID,
		RelativePath:    entry.RelativePath,
		Content:         content,
	}, nil
}

func (r SkillRegistry) ValidateForTask(taskType, skillProfile string) error {
	skill, err := r.Load(skillProfile)
	if err != nil {
		return err
	}
	if skill.Status != "active" {
		return fmt.Errorf("SKILL_UNAVAILABLE")
	}
	if len(skill.TaskTypes) == 0 {
		return nil
	}
	for _, allowed := range skill.TaskTypes {
		if allowed == taskType {
			return nil
		}
	}
	return fmt.Errorf("SKILL_TASK_MISMATCH")
}

func (r SkillRegistry) ComputeSkillHash(skillProfile string) (SkillHashSummary, error) {
	skill, err := r.Load(skillProfile)
	if err != nil {
		return SkillHashSummary{}, err
	}
	return SkillHashSummary{
		SkillProfile:    skill.SkillProfile,
		Hash:            skill.Hash,
		Version:         skill.Version,
		ManifestVersion: skill.ManifestVersion,
	}, nil
}

func (r SkillRegistry) ListRuntimeSkills(status string) []map[string]any {
	mirror := r.Mirror
	if strings.TrimSpace(mirror.ConfigRoot) == "" {
		mirror = NewMetaMirror("")
	}
	status = strings.TrimSpace(status)
	results := []map[string]any{}
	for _, entry := range mirror.RuntimeSkillEntries() {
		if status != "" && entry.Status != status {
			continue
		}
		item := map[string]any{
			"skillProfile":    entry.SkillProfile,
			"status":          entry.Status,
			"version":         entry.Version,
			"manifestVersion": entry.ManifestVersion,
			"metaFileId":      entry.MetaFileID,
			"relativePath":    entry.RelativePath,
		}
		if skill, err := r.Load(entry.SkillProfile); err == nil {
			item["hash"] = skill.Hash
			item["taskTypes"] = skill.TaskTypes
		}
		results = append(results, item)
	}
	return results
}

func (r SkillRegistry) ValidateRequiredSkills(required map[string]string) []map[string]any {
	results := []map[string]any{}
	for taskType, skillProfile := range required {
		item := map[string]any{"taskType": taskType, "skillProfile": skillProfile, "ok": true}
		if skill, err := r.Load(skillProfile); err != nil {
			item["ok"] = false
			item["status"] = "missing"
			item["errorCode"] = "SKILL_UNAVAILABLE"
		} else {
			item["status"] = skill.Status
			if err := r.ValidateForTask(taskType, skillProfile); err != nil {
				item["ok"] = false
				item["errorCode"] = err.Error()
			}
		}
		results = append(results, item)
	}
	return results
}

func fieldFromSkill(content, key string) string {
	for _, value := range skillMetadata(content)[key] {
		value = strings.TrimSpace(value)
		if value != "" {
			return value
		}
	}
	return ""
}

func taskTypesFromSkill(content string) []string {
	out := []string{}
	for _, part := range skillMetadata(content)["taskTypes"] {
		for _, value := range splitSkillMetadataValue(part) {
			if value != "" {
				out = append(out, value)
			}
		}
	}
	return out
}

func skillMetadata(content string) map[string][]string {
	lines := strings.Split(content, "\n")
	start, end := 0, len(lines)
	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		if trimmed == "---" {
			start = i + 1
			for j := i + 1; j < len(lines); j++ {
				if strings.TrimSpace(lines[j]) == "---" {
					end = j
					break
				}
			}
		}
		break
	}
	metadata := map[string][]string{}
	currentKey := ""
	for i := start; i < end; i++ {
		line := strings.TrimRight(lines[i], "\r")
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		if strings.HasPrefix(trimmed, "-") && currentKey != "" {
			value := cleanSkillMetadataValue(strings.TrimSpace(strings.TrimPrefix(trimmed, "-")))
			if value != "" {
				metadata[currentKey] = append(metadata[currentKey], value)
			}
			continue
		}
		index := strings.Index(trimmed, ":")
		if index <= 0 {
			currentKey = ""
			continue
		}
		key := strings.TrimSpace(trimmed[:index])
		value := strings.TrimSpace(trimmed[index+1:])
		if key == "" {
			currentKey = ""
			continue
		}
		currentKey = key
		if value == "" {
			if _, exists := metadata[key]; !exists {
				metadata[key] = nil
			}
			continue
		}
		metadata[key] = append(metadata[key], splitSkillMetadataValue(value)...)
	}
	return metadata
}

func splitSkillMetadataValue(value string) []string {
	value = strings.TrimSpace(value)
	if strings.HasPrefix(value, "[") && strings.HasSuffix(value, "]") {
		value = strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(value, "["), "]"))
		if value == "" {
			return nil
		}
		parts := strings.Split(value, ",")
		out := []string{}
		for _, part := range parts {
			if cleaned := cleanSkillMetadataValue(part); cleaned != "" {
				out = append(out, cleaned)
			}
		}
		return out
	}
	if cleaned := cleanSkillMetadataValue(value); cleaned != "" {
		return []string{cleaned}
	}
	return nil
}

func cleanSkillMetadataValue(value string) string {
	value = strings.TrimSpace(value)
	value = strings.TrimSuffix(value, ",")
	return strings.Trim(strings.TrimSpace(value), "`\"' ")
}

func valueOrDefault(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func skillContentHash(content string) string {
	sum := sha256.Sum256([]byte(content))
	return hex.EncodeToString(sum[:])
}

func normalizeSkillHash(value string) string {
	value = strings.TrimSpace(value)
	value = strings.TrimPrefix(value, "sha256:")
	return strings.ToLower(value)
}
