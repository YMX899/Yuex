package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"path"
	"regexp"
	"sort"
	"strings"
	"time"

	"huahuoai/backend/source/internal/domain"
)

const (
	runtimeManifestInlineLimit = 256 * 1024
	runtimeManifestMaxFiles    = 1024
	runtimeManifestMaxBytes    = 64 * 1024 * 1024
)

// Reject credential-shaped values, not ordinary security guidance such as
// "do not reveal tokens" that is legitimate Agent instruction text.
var runtimeManifestSensitiveValuePattern = regexp.MustCompile(`(?i)(?:authorization\s*:\s*(?:bearer\s+)?\S+|bearer\s+[a-z0-9._~+/=-]{12,}|["']?(?:api[_-]?key|token|session[_-]?key|openclawsessionkey|secret|private[_-]?key)["']?\s*[:=]\s*(?:"[^"]+"|'[^']+'|[^\s,;}{\]]{8,}))`)

type RuntimeManifestBuildInputs struct {
	RuntimeHostID string
	MetaRelease   string
	AgentHash     string
	SkillHashes   map[string]string
	Files         []RuntimeManifestEntry
	ExpiresAt     time.Time
}

type WorkspaceComposer struct {
	Now func() time.Time
}

func NewWorkspaceComposer() WorkspaceComposer {
	return WorkspaceComposer{Now: time.Now}
}

func (c WorkspaceComposer) BuildInputManifest(ctx context.Context, plan AgentRunPlan, runContext domain.RunWorkspaceContextRecord, inputs RuntimeManifestBuildInputs) (RuntimeInputManifest, error) {
	if err := ctx.Err(); err != nil {
		return RuntimeInputManifest{}, err
	}
	now := time.Now().UTC()
	if c.Now != nil {
		now = c.Now().UTC()
	}
	if runContext.Status != "frozen" || runContext.RunID == "" || runContext.AgentRunID == "" || runContext.TenantID == "" || runContext.UserID == "" || runContext.WorkspaceID == "" || runContext.WorkspaceVersion < 1 || runContext.ContextGeneration < 1 ||
		plan.AgentRunID != runContext.AgentRunID || plan.L1AgentProfile != runContext.L1AgentProfile || plan.ManifestVersion != runContext.ManifestVersion ||
		plan.WorkspaceVersion != runContext.WorkspaceVersion || plan.IndexVersion != runContext.IndexVersion ||
		plan.WorkspaceContextManifestHash != runContext.ManifestHash || plan.CapabilityHash != runContext.CapabilityHash ||
		!validFrozenRuntimeContextManifest(runContext) ||
		inputs.RuntimeHostID == "" || inputs.MetaRelease == "" || !validSHA256(inputs.AgentHash) || inputs.AgentHash != plan.AgentHash || runContext.CapabilityHash == "" {
		return RuntimeInputManifest{}, domain.ErrorCode("AGENT_PLAN_INVALID")
	}
	if !sameUniqueStringSet(plan.SelectedSkillProfiles, runContext.SelectedSkills) {
		return RuntimeInputManifest{}, domain.ErrorCode("AGENT_PLAN_INVALID")
	}
	if !sameUniqueStringSet(plan.SelectedKnowledgeRefs, runContext.SelectedKnowledgeRefs) {
		return RuntimeInputManifest{}, domain.ErrorCode("AGENT_PLAN_INVALID")
	}
	if plan.RoutingMode == "dynamic" && (!safeAgentPackageRelativeRoot(plan.AgentRelativeRoot) || plan.AgentRelativeRoot != runContext.AgentRelativeRoot) {
		return RuntimeInputManifest{}, domain.ErrorCode("AGENT_PLAN_INVALID")
	}
	if err := ValidateMetaWorkspaceIdentityTriple(plan.MetaWorkspaceKey, plan.MetaWorkspaceVersion, plan.InputPolicyHash); err != nil {
		return RuntimeInputManifest{}, err
	}
	if plan.MetaWorkspaceKey == "" && len(plan.InputAttachments) != 0 {
		return RuntimeInputManifest{}, domain.ErrorCode("AGENT_PLAN_INVALID")
	}
	skills := make([]RuntimeSkillProfile, 0, len(plan.SelectedSkillProfiles))
	for _, profile := range plan.SelectedSkillProfiles {
		hash := strings.TrimSpace(inputs.SkillHashes[profile])
		if !validSHA256(hash) {
			return RuntimeInputManifest{}, domain.ErrorCode("SKILL_UNAVAILABLE")
		}
		skills = append(skills, RuntimeSkillProfile{Profile: profile, Hash: hash})
	}
	sort.Slice(skills, func(i, j int) bool { return skills[i].Profile < skills[j].Profile })
	files := append([]RuntimeManifestEntry(nil), inputs.Files...)
	sort.Slice(files, func(i, j int) bool { return files[i].LogicalPath < files[j].LogicalPath })
	expiresAt := inputs.ExpiresAt.UTC()
	if expiresAt.IsZero() {
		expiresAt = now.Add(10 * time.Minute)
	}
	if !expiresAt.After(now) || expiresAt.After(now.Add(30*time.Minute)) {
		return RuntimeInputManifest{}, domain.ErrorCode("AGENT_PLAN_INVALID")
	}
	manifest := RuntimeInputManifest{
		SchemaVersion: "runtime_input_manifest.v1", RunID: runContext.RunID, RuntimeHostID: inputs.RuntimeHostID,
		TenantID: runContext.TenantID, UserID: runContext.UserID, WorkspaceID: runContext.WorkspaceID,
		WorkspaceVersion: runContext.WorkspaceVersion, ThreadWorkspaceBindingVersion: runContext.ThreadWorkspaceBindingVersion,
		ContextGeneration: runContext.ContextGeneration, MetaRelease: inputs.MetaRelease,
		AgentProfile: plan.L1AgentProfile, MetaWorkspaceKey: plan.MetaWorkspaceKey,
		MetaWorkspaceVersion: plan.MetaWorkspaceVersion, InputPolicyHash: plan.InputPolicyHash,
		Attachments: append([]AgentRunInputAttachmentIdentity{}, plan.InputAttachments...),
		AgentHash:   plan.AgentHash, SkillProfiles: skills,
		CapabilityHash: runContext.CapabilityHash, Files: files, ExpiresAt: expiresAt,
	}
	if err := c.ValidateLogicalFiles(manifest); err != nil {
		return RuntimeInputManifest{}, err
	}
	manifest.ManifestHash = c.ComputeManifestHash(manifest)
	return manifest, nil
}

// validFrozenRuntimeContextManifest proves the persisted context manifest is
// still the exact immutable context paired with this Plan. The plan fields
// above bind the plan to the record; these checks also prevent a stale or
// hand-assembled record from carrying a valid-looking hash for different
// Workspace, index, or capability facts.
func validFrozenRuntimeContextManifest(record domain.RunWorkspaceContextRecord) bool {
	if record.ManifestHash == "" || len(record.ContextManifest) == 0 {
		return false
	}
	raw, err := json.Marshal(record.ContextManifest)
	if err != nil || sha256String(raw) != record.ManifestHash {
		return false
	}
	var identity struct {
		WorkspaceID      *string `json:"workspaceId"`
		WorkspaceVersion *int64  `json:"workspaceVersion"`
		IndexVersion     *int64  `json:"indexVersion"`
		CapabilityHash   *string `json:"capabilityHash"`
	}
	if err := json.Unmarshal(raw, &identity); err != nil || identity.WorkspaceID == nil || identity.WorkspaceVersion == nil ||
		identity.IndexVersion == nil || identity.CapabilityHash == nil {
		return false
	}
	return *identity.WorkspaceID == record.WorkspaceID && *identity.WorkspaceVersion == record.WorkspaceVersion &&
		*identity.IndexVersion == record.IndexVersion && *identity.CapabilityHash == record.CapabilityHash
}

func (WorkspaceComposer) ValidateLogicalFiles(manifest RuntimeInputManifest) error {
	if manifest.SchemaVersion != "runtime_input_manifest.v1" || manifest.RunID == "" || manifest.RuntimeHostID == "" || manifest.TenantID == "" || manifest.UserID == "" || manifest.WorkspaceID == "" ||
		manifest.WorkspaceVersion < 1 || manifest.ContextGeneration < 1 || manifest.MetaRelease == "" || manifest.AgentProfile == "" || manifest.AgentHash == "" || manifest.CapabilityHash == "" || len(manifest.SkillProfiles) == 0 ||
		len(manifest.Files) == 0 || len(manifest.Files) > runtimeManifestMaxFiles {
		return runtimeWorkspaceMaterializationError()
	}
	if !safeCatalogIdentifier(manifest.AgentProfile) || !validSHA256(manifest.AgentHash) || !validRuntimeSkillProfiles(manifest.SkillProfiles) {
		return runtimeWorkspaceMaterializationError()
	}
	if ValidateMetaWorkspaceIdentityTriple(manifest.MetaWorkspaceKey, manifest.MetaWorkspaceVersion, manifest.InputPolicyHash) != nil {
		return runtimeWorkspaceMaterializationError()
	}
	if manifest.MetaWorkspaceKey == "" && len(manifest.Attachments) != 0 {
		return runtimeWorkspaceMaterializationError()
	}
	seen := map[string]bool{}
	var total int64
	for _, file := range manifest.Files {
		logicalPath, err := normalizeRuntimeLogicalPath(file.LogicalPath)
		if err != nil || logicalPath != file.LogicalPath || reservedRuntimeMaterializationPath(logicalPath) || seen[logicalPath] {
			return runtimeWorkspaceMaterializationError()
		}
		seen[logicalPath] = true
		if file.SizeBytes < 0 || file.SizeBytes > runtimeManifestMaxBytes || !validSHA256(file.SHA256) {
			return runtimeWorkspaceMaterializationError()
		}
		total += file.SizeBytes
		if total > runtimeManifestMaxBytes {
			return runtimeWorkspaceMaterializationError()
		}
		switch file.SourceType {
		case "inline":
			if file.SourceRef != "" || int64(len([]byte(file.InlineContent))) != file.SizeBytes || file.SizeBytes > runtimeManifestInlineLimit || sha256String([]byte(file.InlineContent)) != normalizeSHA256(file.SHA256) {
				return runtimeWorkspaceMaterializationError()
			}
		case "object_ref", "formal_workspace_ref", "meta_release_ref":
			if file.InlineContent != "" || !validRuntimeManifestSourceRef(file.SourceType, file.LogicalPath, file.SourceRef) {
				return runtimeWorkspaceMaterializationError()
			}
			if file.SourceType != "object_ref" && file.ObjectRead != nil {
				return runtimeWorkspaceMaterializationError()
			}
			if file.ObjectRead != nil && !validRuntimeObjectReadReference(*file.ObjectRead) {
				return runtimeWorkspaceMaterializationError()
			}
		default:
			return runtimeWorkspaceMaterializationError()
		}
		if containsForbiddenManifestValue(file.LogicalPath) || containsForbiddenManifestValue(file.InlineContent) {
			return runtimeWorkspaceMaterializationError()
		}
	}
	if err := validateRuntimeManifestAttachments(manifest.Attachments, manifest.Files); err != nil {
		return runtimeWorkspaceMaterializationError()
	}
	return nil
}

// output, staging and the marker belong to the Host materializer. A manifest
// may supply immutable input only; allowing an entry at one of these paths
// could replace the writable boundary or the cache identity marker.
func reservedRuntimeMaterializationPath(logicalPath string) bool {
	return logicalPath == ".materialization.json" || strings.HasPrefix(logicalPath, ".materialization.json/") ||
		logicalPath == "output" || strings.HasPrefix(logicalPath, "output/") ||
		logicalPath == "staging" || strings.HasPrefix(logicalPath, "staging/")
}

func validateRuntimeManifestAttachments(attachments []AgentRunInputAttachmentIdentity, files []RuntimeManifestEntry) error {
	byPath := make(map[string]RuntimeManifestEntry, len(files))
	attachmentPaths := make(map[string]bool, len(attachments))
	for _, file := range files {
		byPath[file.LogicalPath] = file
	}
	seenResources := map[string]bool{}
	for index, attachment := range attachments {
		expectedPath, err := RuntimeInputAttachmentLogicalPath(index, attachment.MIMEType)
		file, exists := byPath[attachment.LogicalPath]
		if err != nil || attachment.LogicalPath != expectedPath || !runtimeInputResourceIDPattern.MatchString(attachment.ResourceID) ||
			!safeCatalogIdentifier(attachment.Usage) || seenResources[attachment.ResourceID] || attachment.SizeBytes < 1 ||
			!validSHA256(attachment.SHA256) || attachment.Width < 1 || attachment.Height < 1 || !exists ||
			file.SourceType != "object_ref" || file.SizeBytes != attachment.SizeBytes || normalizeSHA256(file.SHA256) != normalizeSHA256(attachment.SHA256) {
			return runtimeWorkspaceMaterializationError()
		}
		if file.ObjectRead != nil && canonicalRuntimeObjectReadMIME(file.ObjectRead.MIMEType) != attachment.MIMEType {
			return runtimeWorkspaceMaterializationError()
		}
		seenResources[attachment.ResourceID] = true
		attachmentPaths[attachment.LogicalPath] = true
	}
	for _, file := range files {
		if file.ObjectRead == nil {
			continue
		}
		if !attachmentPaths[file.LogicalPath] {
			return runtimeWorkspaceMaterializationError()
		}
	}
	return nil
}

func validRuntimeObjectReadReference(reference RuntimeObjectReadReference) bool {
	if reference.URL == "" || reference.URL != strings.TrimSpace(reference.URL) || len(reference.URL) > 8192 || reference.ExpiresAt.IsZero() ||
		canonicalRuntimeObjectReadMIME(reference.MIMEType) == "" {
		return false
	}
	parsed, err := url.Parse(reference.URL)
	if err != nil || !strings.EqualFold(parsed.Scheme, "https") || parsed.Host == "" || parsed.User != nil || parsed.Opaque != "" ||
		parsed.Fragment != "" || parsed.RawQuery == "" || parsed.Path == "" {
		return false
	}
	return true
}

func canonicalRuntimeObjectReadMIME(value string) string {
	value = strings.ToLower(strings.TrimSpace(strings.Split(value, ";")[0]))
	switch value {
	case "image/jpeg", "image/png", "image/webp":
		return value
	default:
		return ""
	}
}

func (WorkspaceComposer) ComputeManifestHash(manifest RuntimeInputManifest) string {
	manifest.ManifestHash = ""
	raw, _ := json.Marshal(manifest)
	return sha256String(raw)
}

func NewInlineRuntimeEntry(logicalPath string, content []byte) RuntimeManifestEntry {
	return RuntimeManifestEntry{LogicalPath: logicalPath, SourceType: "inline", InlineContent: string(content), SizeBytes: int64(len(content)), SHA256: sha256String(content)}
}

// validRuntimeManifestSourceRef keeps references transport-safe. A Host may
// resolve a trusted reference, but it must never receive a Dispatcher-local
// path, file URL, or noncanonical name. Formal Workspace sources may be
// projected into task-specific logical input paths, so sourceRef and
// logicalPath intentionally remain distinct values.
func validRuntimeManifestSourceRef(_ string, _ string, sourceRef string) bool {
	if sourceRef == "" || sourceRef != strings.TrimSpace(sourceRef) ||
		containsForbiddenManifestValue(sourceRef) || strings.Contains(sourceRef, "\\") ||
		strings.Contains(sourceRef, "..") || strings.Contains(sourceRef, "://") ||
		strings.HasPrefix(sourceRef, "/") || strings.HasPrefix(sourceRef, "~") ||
		strings.HasPrefix(strings.ToLower(sourceRef), "file:") {
		return false
	}
	if len(sourceRef) >= 2 && sourceRef[1] == ':' && ((sourceRef[0] >= 'a' && sourceRef[0] <= 'z') || (sourceRef[0] >= 'A' && sourceRef[0] <= 'Z')) {
		return false
	}
	clean := path.Clean(sourceRef)
	if clean == "." || clean != sourceRef {
		return false
	}
	return true
}

func runtimeWorkspaceMaterializationError() error {
	return fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
}

func normalizeRuntimeLogicalPath(value string) (string, error) {
	raw := strings.TrimSpace(value)
	if raw == "" || strings.HasPrefix(raw, "/") || strings.Contains(raw, "\\") || strings.Contains(raw, ":") {
		return "", fmt.Errorf("invalid logical path")
	}
	clean := path.Clean(raw)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, "../") {
		return "", fmt.Errorf("invalid logical path")
	}
	return clean, nil
}

func sameUniqueStringSet(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	values := make(map[string]bool, len(left))
	for _, item := range left {
		if item == "" || values[item] {
			return false
		}
		values[item] = true
	}
	for _, item := range right {
		if item == "" || !values[item] {
			return false
		}
		delete(values, item)
	}
	return len(values) == 0
}

// sameStringSet remains for package-local compatibility tests. Production
// manifest binding uses sameUniqueStringSet so duplicate frozen selections
// cannot survive equality checks.
func sameStringSet(left, right []string) bool {
	return sameUniqueStringSet(left, right)
}

func validRuntimeSkillProfiles(profiles []RuntimeSkillProfile) bool {
	previous := ""
	for _, profile := range profiles {
		if !safeCatalogIdentifier(profile.Profile) || !validSHA256(profile.Hash) ||
			(previous != "" && previous >= profile.Profile) {
			return false
		}
		previous = profile.Profile
	}
	return true
}

func validSHA256(value string) bool {
	normalized := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(value)), "sha256:")
	if len(normalized) != 64 {
		return false
	}
	_, err := hex.DecodeString(normalized)
	return err == nil
}

func normalizeSHA256(value string) string {
	return "sha256:" + strings.TrimPrefix(strings.ToLower(strings.TrimSpace(value)), "sha256:")
}

func sha256String(value []byte) string {
	sum := sha256.Sum256(value)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func containsForbiddenManifestValue(value string) bool {
	return runtimeManifestSensitiveValuePattern.MatchString(value)
}
