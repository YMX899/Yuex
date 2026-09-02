package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"path"
	"sort"
	"strings"

	"huahuoai/backend/source/internal/domain"
)

type L1AgentManifest struct {
	Version string                 `json:"version"`
	Agents  []L1AgentManifestEntry `json:"agents"`
}

type L1AgentManifestEntry struct {
	AgentProfile           string                   `json:"agentProfile"`
	DisplayName            string                   `json:"displayName"`
	MetaWorkspaceKey       string                   `json:"metaWorkspaceKey,omitempty"`
	PublicSelectable       bool                     `json:"publicSelectable"`
	DefaultTaskType        string                   `json:"defaultTaskType,omitempty"`
	InputPolicy            MetaWorkspaceInputPolicy `json:"inputPolicy"`
	InputPolicyHash        string                   `json:"inputPolicyHash"`
	Status                 string                   `json:"status"`
	Version                string                   `json:"version"`
	Hash                   string                   `json:"hash"`
	RelativeRoot           string                   `json:"relativeRoot"`
	IntentCategories       []string                 `json:"intentCategories"`
	Categories             []string                 `json:"categories,omitempty"`
	TaskTypes              []string                 `json:"taskTypes"`
	CandidateSkillProfiles []string                 `json:"candidateSkillProfiles"`
	KnowledgeRoots         []string                 `json:"knowledgeRoots"`
	ToolPolicyProfile      string                   `json:"toolPolicyProfile"`
	ExecutionScopes        []string                 `json:"executionScopes"`
	MaxCandidateSkills     int                      `json:"maxCandidateSkills"`
	RequiredFeatures       []string                 `json:"requiredFeatures"`
	MinimumMembership      int                      `json:"minimumMembership"`
	Priority               int                      `json:"priority"`
}

type AgentPermissionSnapshot struct {
	Features        map[string]bool `json:"features"`
	MembershipLevel int             `json:"membershipLevel"`
}

type L1AgentRouteCommand struct {
	Intent      domain.TaskIntent       `json:"intent"`
	Manifest    L1AgentManifest         `json:"manifest"`
	Permissions AgentPermissionSnapshot `json:"permissions"`
	// LastThreadAgent is retained for wire compatibility with already-created
	// planning requests. It is deliberately not a route selector: a previous
	// thread result cannot resolve a current catalog ambiguity.
	LastThreadAgent          string `json:"lastThreadAgent,omitempty"`
	ExpectedMetaWorkspaceKey string `json:"expectedMetaWorkspaceKey,omitempty"`
}

type L1AgentRouteResult struct {
	SelectedAgentProfile   string                   `json:"selectedAgentProfile"`
	AgentProfile           string                   `json:"agentProfile"`
	MetaWorkspaceKey       string                   `json:"metaWorkspaceKey"`
	MetaWorkspaceVersion   string                   `json:"metaWorkspaceVersion"`
	InputPolicy            MetaWorkspaceInputPolicy `json:"inputPolicy"`
	InputPolicyHash        string                   `json:"inputPolicyHash"`
	CandidateSkillProfiles []string                 `json:"candidateSkillProfiles"`
	KnowledgeRoots         []string                 `json:"knowledgeRoots"`
	ToolPolicyProfile      string                   `json:"toolPolicyProfile"`
	RelativeRoot           string                   `json:"relativeRoot"`
	MaxCandidateSkills     int                      `json:"maxCandidateSkills"`
	ManifestVersion        string                   `json:"manifestVersion"`
	AgentHash              string                   `json:"agentHash"`
	Priority               int                      `json:"priority"`
	RouteReason            string                   `json:"routeReason"`
	SafeRouteReason        string                   `json:"safeRouteReason"`
}

// PublicMetaWorkspace is the only L1 routing catalog shape intended for an
// App-facing selection surface. It deliberately excludes Agent package and
// Runtime implementation identities.
type PublicMetaWorkspace struct {
	MetaWorkspaceKey string                   `json:"metaWorkspaceKey"`
	DisplayName      string                   `json:"displayName"`
	Version          string                   `json:"version"`
	InputPolicy      MetaWorkspaceInputPolicy `json:"inputPolicy"`
	InputPolicyHash  string                   `json:"inputPolicyHash"`
}

type MetaWorkspaceInputPolicy struct {
	Usage                  string   `json:"usage"`
	AcceptsText            bool     `json:"acceptsText"`
	AcceptedImageMIMETypes []string `json:"acceptedImageMimeTypes"`
	ImageRequired          bool     `json:"imageRequired"`
	MaxFiles               int      `json:"maxFiles"`
	MaxBytes               int64    `json:"maxBytes"`
	MaxBytesPerFile        int64    `json:"maxBytesPerFile"`
	MaxWidth               int      `json:"maxWidth"`
	MaxHeight              int      `json:"maxHeight"`
	MaxPixels              int64    `json:"maxPixels"`
}

type L1AgentRouter struct{}

func NewL1AgentRouter() L1AgentRouter { return L1AgentRouter{} }

func (L1AgentRouter) Route(ctx context.Context, command L1AgentRouteCommand) (L1AgentRouteResult, error) {
	if err := ctx.Err(); err != nil {
		return L1AgentRouteResult{}, err
	}
	eligible := L1AgentRouter{}.ListEligible(command.Intent, command.Manifest, command.Permissions)
	expected := strings.TrimSpace(command.ExpectedMetaWorkspaceKey)
	if len(eligible) == 0 {
		if expected != "" {
			return L1AgentRouteResult{}, domain.ErrorCode("META_WORKSPACE_UNAVAILABLE")
		}
		return L1AgentRouteResult{}, domain.ErrorCode("AGENT_PROFILE_UNAVAILABLE")
	}

	if expected != "" {
		if !safeCatalogIdentifier(expected) {
			return L1AgentRouteResult{}, domain.ErrorCode("META_WORKSPACE_UNAVAILABLE")
		}
		matches := publicMetaWorkspaceMatches(eligible, expected)
		if len(matches) != 1 {
			return L1AgentRouteResult{}, domain.ErrorCode("META_WORKSPACE_UNAVAILABLE")
		}
		return l1AgentRouteResult(matches[0], command.Manifest.Version, "expected_meta_workspace_key")
	}

	// Priority and prior-thread history are intentionally not ambiguity
	// resolvers. They made catalog alternatives permanently unreachable: a
	// higher-priority profile silently won even when the current input matched
	// several L1 packages. A public key is the only App-facing discriminator.
	publicGroups := publicMetaWorkspaceGroups(eligible)
	switch len(publicGroups) {
	case 0:
		if len(eligible) != 1 {
			// There is no public, user-selectable way to disambiguate internal
			// alternatives. Do not revive a scene, phone, task label, or prior
			// thread profile as a fallback.
			return L1AgentRouteResult{}, domain.ErrorCode("AGENT_PROFILE_UNAVAILABLE")
		}
		return l1AgentRouteResult(eligible[0], command.Manifest.Version, "single_eligible_agent")
	case 1:
		var candidates []L1AgentManifestEntry
		for _, items := range publicGroups {
			candidates = items
		}
		if len(candidates) != 1 {
			// Multiple entries behind one public key are not an App choice and
			// cannot be repaired by priority. The manifest must be corrected.
			return L1AgentRouteResult{}, domain.ErrorCode("AGENT_PROFILE_UNAVAILABLE")
		}
		return l1AgentRouteResult(candidates[0], command.Manifest.Version, "single_public_meta_workspace")
	default:
		return L1AgentRouteResult{}, domain.ErrorCode("META_WORKSPACE_SELECTION_REQUIRED")
	}
}

// ListPublicMetaWorkspaces returns a narrow, stable App catalog. Legacy and
// internal-only entries remain intentionally absent so clients cannot use a
// metadata key to select an internal Agent profile.
func ListPublicMetaWorkspaces(manifest L1AgentManifest) []PublicMetaWorkspace {
	type orderedMetaWorkspace struct {
		workspace PublicMetaWorkspace
		order     int
	}
	byKey := make(map[string]orderedMetaWorkspace)
	invalidKeys := map[string]bool{}
	for order, entry := range manifest.Agents {
		if !validPublicMetaWorkspaceEntry(entry) {
			continue
		}
		if invalidKeys[entry.MetaWorkspaceKey] {
			continue
		}
		candidate := PublicMetaWorkspace{
			MetaWorkspaceKey: entry.MetaWorkspaceKey,
			DisplayName:      entry.DisplayName,
			Version:          entry.Version,
			InputPolicy:      cloneMetaWorkspaceInputPolicy(entry.InputPolicy),
			InputPolicyHash:  entry.InputPolicyHash,
		}
		if _, found := byKey[entry.MetaWorkspaceKey]; found {
			// A public key must identify exactly one backing Agent. Route and
			// ResolvePublicMetaWorkspace both require that uniqueness, so never
			// publish a collapsed duplicate that the App cannot actually select.
			delete(byKey, entry.MetaWorkspaceKey)
			invalidKeys[entry.MetaWorkspaceKey] = true
			continue
		}
		byKey[entry.MetaWorkspaceKey] = orderedMetaWorkspace{workspace: candidate, order: order}
	}
	items := make([]orderedMetaWorkspace, 0, len(byKey))
	for _, item := range byKey {
		items = append(items, item)
	}
	sort.SliceStable(items, func(i, j int) bool {
		if items[i].workspace.DisplayName != items[j].workspace.DisplayName {
			return items[i].workspace.DisplayName < items[j].workspace.DisplayName
		}
		return items[i].order < items[j].order
	})
	result := make([]PublicMetaWorkspace, 0, len(items))
	for _, item := range items {
		result = append(result, item.workspace)
	}
	return result
}

// ResolvePublicMetaWorkspace resolves only the App-facing logical key. It does
// not accept an Agent profile, package path, task type, or Skill from the
// caller. The returned entry remains server-internal and is subsequently used
// to constrain intent resolution and planning.
func ResolvePublicMetaWorkspace(manifest L1AgentManifest, permissions AgentPermissionSnapshot, expected string) (L1AgentManifestEntry, error) {
	expected = strings.TrimSpace(expected)
	if !safeCatalogIdentifier(expected) {
		return L1AgentManifestEntry{}, domain.ErrorCode("META_WORKSPACE_UNAVAILABLE")
	}
	matches := make([]L1AgentManifestEntry, 0, 1)
	for _, entry := range manifest.Agents {
		if !validPublicMetaWorkspaceEntry(entry) || entry.MetaWorkspaceKey != expected ||
			permissions.MembershipLevel < entry.MinimumMembership || !featuresAllowed(entry.RequiredFeatures, permissions.Features) {
			continue
		}
		matches = append(matches, entry)
	}
	if len(matches) != 1 {
		return L1AgentManifestEntry{}, domain.ErrorCode("META_WORKSPACE_UNAVAILABLE")
	}
	return matches[0], nil
}

func publicMetaWorkspaceMatches(eligible []L1AgentManifestEntry, expected string) []L1AgentManifestEntry {
	matches := make([]L1AgentManifestEntry, 0, 1)
	for _, entry := range eligible {
		if validPublicMetaWorkspaceEntry(entry) && entry.MetaWorkspaceKey == expected {
			matches = append(matches, entry)
		}
	}
	return matches
}

func publicMetaWorkspaceGroups(eligible []L1AgentManifestEntry) map[string][]L1AgentManifestEntry {
	groups := map[string][]L1AgentManifestEntry{}
	for _, entry := range eligible {
		if !validPublicMetaWorkspaceEntry(entry) {
			continue
		}
		groups[entry.MetaWorkspaceKey] = append(groups[entry.MetaWorkspaceKey], entry)
	}
	return groups
}

func l1AgentRouteResult(selected L1AgentManifestEntry, manifestVersion, reason string) (L1AgentRouteResult, error) {
	result := L1AgentRouteResult{
		SelectedAgentProfile: selected.AgentProfile, AgentProfile: selected.AgentProfile,
		CandidateSkillProfiles: append([]string(nil), selected.CandidateSkillProfiles...),
		KnowledgeRoots:         append([]string(nil), selected.KnowledgeRoots...),
		ToolPolicyProfile:      selected.ToolPolicyProfile, RelativeRoot: selected.RelativeRoot,
		MaxCandidateSkills: selected.MaxCandidateSkills, ManifestVersion: manifestVersion,
		AgentHash: selected.Hash, Priority: selected.Priority,
		RouteReason: reason, SafeRouteReason: reason,
	}
	if selected.MetaWorkspaceKey == "" {
		// A manifest version belongs to every L1 package, but it must never be
		// mistaken for a public Meta Workspace version on an internal route.
		return result, nil
	}
	result.MetaWorkspaceKey = selected.MetaWorkspaceKey
	result.MetaWorkspaceVersion = selected.Version
	result.InputPolicy = cloneMetaWorkspaceInputPolicy(selected.InputPolicy)
	result.InputPolicyHash = selected.InputPolicyHash
	if err := ValidateL1AgentRouteMetaWorkspaceIdentity(result); err != nil {
		return L1AgentRouteResult{}, err
	}
	return result, nil
}

func validPublicMetaWorkspaceEntry(entry L1AgentManifestEntry) bool {
	return entry.Status == "active" && entry.PublicSelectable && safeCatalogIdentifier(entry.MetaWorkspaceKey) &&
		strings.TrimSpace(entry.DisplayName) != "" && safeCatalogIdentifier(entry.Version) &&
		ValidateMetaWorkspaceInputPolicy(entry.InputPolicy, entry.InputPolicyHash) == nil
}

// ValidateL1AgentRouteMetaWorkspaceIdentity keeps the full route fact atomic
// before a planner can freeze it into an AgentRunPlan. Internal routes expose
// no public Meta fields or input policy. Meta routes must bind their complete
// identity to the exact server-owned input policy hash.
func ValidateL1AgentRouteMetaWorkspaceIdentity(route L1AgentRouteResult) error {
	if err := ValidateMetaWorkspaceIdentityTriple(route.MetaWorkspaceKey, route.MetaWorkspaceVersion, route.InputPolicyHash); err != nil {
		return err
	}
	if route.MetaWorkspaceKey == "" {
		if !emptyMetaWorkspaceInputPolicy(route.InputPolicy) {
			return domain.ErrorCode("AGENT_PLAN_INVALID")
		}
		return nil
	}
	return ValidateMetaWorkspaceInputPolicy(route.InputPolicy, route.InputPolicyHash)
}

func emptyMetaWorkspaceInputPolicy(policy MetaWorkspaceInputPolicy) bool {
	return policy.Usage == "" && !policy.AcceptsText && len(policy.AcceptedImageMIMETypes) == 0 && !policy.ImageRequired &&
		policy.MaxFiles == 0 && policy.MaxBytes == 0 && policy.MaxBytesPerFile == 0 && policy.MaxWidth == 0 && policy.MaxHeight == 0 && policy.MaxPixels == 0
}

func (L1AgentRouter) ListEligible(intent domain.TaskIntent, manifest L1AgentManifest, permissions AgentPermissionSnapshot) []L1AgentManifestEntry {
	eligible := make([]L1AgentManifestEntry, 0)
	for _, entry := range manifest.Agents {
		if entry.Status != "active" || entry.AgentProfile == "" || entry.Hash == "" {
			continue
		}
		if !matchesRequiredOne(intentCategoriesFor(entry), intent.Category) || !matchesRequiredOne(entry.TaskTypes, intent.ResolvedTaskType) || !matchesRequiredOne(entry.ExecutionScopes, intent.ExecutionScope) {
			continue
		}
		if permissions.MembershipLevel < entry.MinimumMembership || !featuresAllowed(entry.RequiredFeatures, permissions.Features) {
			continue
		}
		if len(intent.CandidateL1Agents) > 0 && !matchesOne(intent.CandidateL1Agents, entry.AgentProfile) {
			continue
		}
		eligible = append(eligible, entry)
	}
	// ListEligible is an inspection helper. Stable lexical ordering prevents a
	// caller from treating priority order as an implicit route decision.
	sort.SliceStable(eligible, func(i, j int) bool { return eligible[i].AgentProfile < eligible[j].AgentProfile })
	return eligible
}

// ValidateL1AgentManifestForDynamicPlanning verifies the fields needed by the
// normal catalog-backed route. Legacy callers may still exercise Route with a
// minimal fixture, but they cannot enter CapabilityPlanner.Plan without these
// facts.
func ValidateL1AgentManifestForDynamicPlanning(manifest L1AgentManifest) error {
	if strings.TrimSpace(manifest.Version) == "" {
		return domain.ErrorCode("AGENT_PROFILE_UNAVAILABLE")
	}
	seen := map[string]bool{}
	publicMeta := map[string]PublicMetaWorkspace{}
	for _, entry := range manifest.Agents {
		if entry.Status != "active" {
			continue
		}
		if !safeCatalogIdentifier(entry.AgentProfile) || strings.TrimSpace(entry.DisplayName) == "" || strings.TrimSpace(entry.Hash) == "" ||
			strings.TrimSpace(entry.Version) == "" || !safeAgentPackageRelativeRoot(entry.RelativeRoot) ||
			len(entry.IntentCategories) == 0 || len(entry.TaskTypes) == 0 || len(entry.CandidateSkillProfiles) == 0 ||
			strings.TrimSpace(entry.ToolPolicyProfile) == "" || len(entry.ExecutionScopes) == 0 ||
			entry.MaxCandidateSkills < 1 || entry.MaxCandidateSkills > 8 || seen[entry.AgentProfile] {
			return domain.ErrorCode("AGENT_PLAN_INVALID")
		}
		for _, profile := range entry.CandidateSkillProfiles {
			if !safeCatalogIdentifier(profile) {
				return domain.ErrorCode("AGENT_PLAN_INVALID")
			}
		}
		for _, root := range entry.KnowledgeRoots {
			if !safeManifestRelativePath(root) {
				return domain.ErrorCode("AGENT_PLAN_INVALID")
			}
		}
		if !safeCatalogIdentifier(entry.ToolPolicyProfile) {
			return domain.ErrorCode("AGENT_PLAN_INVALID")
		}
		if entry.MetaWorkspaceKey != "" && !safeCatalogIdentifier(entry.MetaWorkspaceKey) {
			return domain.ErrorCode("AGENT_PLAN_INVALID")
		}
		if entry.PublicSelectable {
			if !safeCatalogIdentifier(entry.MetaWorkspaceKey) || !safeCatalogIdentifier(entry.DefaultTaskType) ||
				!containsExact(entry.TaskTypes, entry.DefaultTaskType) || ValidateMetaWorkspaceInputPolicy(entry.InputPolicy, entry.InputPolicyHash) != nil {
				return domain.ErrorCode("AGENT_PLAN_INVALID")
			}
			candidate := PublicMetaWorkspace{MetaWorkspaceKey: entry.MetaWorkspaceKey, DisplayName: entry.DisplayName, Version: entry.Version, InputPolicy: cloneMetaWorkspaceInputPolicy(entry.InputPolicy), InputPolicyHash: entry.InputPolicyHash}
			if _, duplicate := publicMeta[entry.MetaWorkspaceKey]; duplicate {
				return domain.ErrorCode("AGENT_PLAN_INVALID")
			}
			publicMeta[entry.MetaWorkspaceKey] = candidate
		} else if (entry.InputPolicyHash != "" || entry.InputPolicy.Usage != "") && ValidateMetaWorkspaceInputPolicy(entry.InputPolicy, entry.InputPolicyHash) != nil {
			return domain.ErrorCode("AGENT_PLAN_INVALID")
		}
		seen[entry.AgentProfile] = true
	}
	return nil
}

func ValidateMetaWorkspaceInputPolicy(policy MetaWorkspaceInputPolicy, expectedHash string) error {
	if !safeCatalogIdentifier(policy.Usage) || !policy.AcceptsText || policy.MaxFiles < 0 || policy.MaxFiles > 16 ||
		policy.MaxBytes < 0 || policy.MaxBytes > 100*1024*1024 || policy.MaxBytesPerFile < 0 || policy.MaxBytesPerFile > 100*1024*1024 ||
		policy.MaxWidth < 0 || policy.MaxWidth > 32768 || policy.MaxHeight < 0 || policy.MaxHeight > 32768 ||
		policy.MaxPixels < 0 || policy.MaxPixels > 250000000 {
		return domain.ErrorCode("AGENT_PLAN_INVALID")
	}
	allowedMIME := map[string]bool{"image/jpeg": true, "image/png": true, "image/webp": true}
	previous := ""
	for _, mimeType := range policy.AcceptedImageMIMETypes {
		if !allowedMIME[mimeType] || (previous != "" && previous >= mimeType) {
			return domain.ErrorCode("AGENT_PLAN_INVALID")
		}
		previous = mimeType
	}
	if len(policy.AcceptedImageMIMETypes) == 0 {
		if policy.Usage != "none" || policy.ImageRequired || policy.MaxFiles != 0 || policy.MaxBytes != 0 || policy.MaxBytesPerFile != 0 ||
			policy.MaxWidth != 0 || policy.MaxHeight != 0 || policy.MaxPixels != 0 {
			return domain.ErrorCode("AGENT_PLAN_INVALID")
		}
	} else if policy.Usage != "primary_input" || policy.MaxFiles < 1 || policy.MaxBytes < 1 || policy.MaxBytesPerFile < 1 || policy.MaxBytesPerFile > policy.MaxBytes ||
		policy.MaxWidth < 1 || policy.MaxHeight < 1 || policy.MaxPixels < 1 {
		return domain.ErrorCode("AGENT_PLAN_INVALID")
	}
	if !validSHA256(expectedHash) || metaWorkspaceInputPolicyHash(policy) != normalizeSHA256(expectedHash) {
		return domain.ErrorCode("AGENT_PLAN_INVALID")
	}
	return nil
}

func cloneMetaWorkspaceInputPolicy(policy MetaWorkspaceInputPolicy) MetaWorkspaceInputPolicy {
	policy.AcceptedImageMIMETypes = append([]string{}, policy.AcceptedImageMIMETypes...)
	return policy
}

func metaWorkspaceInputPolicyHash(policy MetaWorkspaceInputPolicy) string {
	raw, _ := json.Marshal(map[string]any{
		"acceptedImageMimeTypes": policy.AcceptedImageMIMETypes,
		"acceptsText":            policy.AcceptsText,
		"imageRequired":          policy.ImageRequired,
		"maxBytes":               policy.MaxBytes,
		"maxBytesPerFile":        policy.MaxBytesPerFile,
		"maxFiles":               policy.MaxFiles,
		"maxHeight":              policy.MaxHeight,
		"maxPixels":              policy.MaxPixels,
		"maxWidth":               policy.MaxWidth,
		"usage":                  policy.Usage,
	})
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func intentCategoriesFor(entry L1AgentManifestEntry) []string {
	if len(entry.IntentCategories) > 0 {
		return entry.IntentCategories
	}
	return entry.Categories
}

func matchesRequiredOne(values []string, expected string) bool {
	return len(values) > 0 && strings.TrimSpace(expected) != "" && matchesOne(values, expected)
}

func safeManifestRelativePath(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" || strings.HasPrefix(value, "/") || strings.Contains(value, "\\") || strings.Contains(value, ":") {
		return false
	}
	clean := path.Clean(value)
	return clean == value && clean != "." && clean != ".." && !strings.HasPrefix(clean, "../")
}

func safeCatalogIdentifier(value string) bool {
	if value == "" || value != strings.TrimSpace(value) {
		return false
	}
	for _, runeValue := range value {
		if (runeValue < 'a' || runeValue > 'z') && (runeValue < 'A' || runeValue > 'Z') &&
			(runeValue < '0' || runeValue > '9') && runeValue != '_' && runeValue != '-' && runeValue != '.' {
			return false
		}
	}
	return true
}

func matchesOne(values []string, expected string) bool {
	if len(values) == 0 {
		return true
	}
	for _, value := range values {
		if strings.TrimSpace(value) == expected || strings.TrimSpace(value) == "*" {
			return true
		}
	}
	return false
}

func featuresAllowed(required []string, available map[string]bool) bool {
	for _, feature := range required {
		if !available[feature] {
			return false
		}
	}
	return true
}
