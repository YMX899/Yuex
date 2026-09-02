package runtime

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"huahuoai/backend/source/internal/domain"
)

func TestContainsForbiddenManifestValueDistinguishesGuidanceFromCredentials(t *testing.T) {
	tests := []struct {
		name      string
		value     string
		forbidden bool
	}{
		{
			name:      "security guidance may mention tokens and secrets",
			value:     "Do not reveal tokens, secrets, or private credentials in a user-visible reply.",
			forbidden: false,
		},
		{
			name:      "agent memory may mention private credentials",
			value:     "Do not store private credentials, raw tokens, or unsupported user claims.",
			forbidden: false,
		},
		{
			name:      "authorization header is forbidden",
			value:     "Authorization: Bearer abcdefghijklmnopqrstuvwxyz",
			forbidden: true,
		},
		{
			name:      "json token value is forbidden",
			value:     `{"token":"abcdefghijklmnop"}`,
			forbidden: true,
		},
		{
			name:      "api key assignment is forbidden",
			value:     "api_key = abcdefghijklmnop",
			forbidden: true,
		},
		{
			name:      "private key assignment is forbidden",
			value:     "private_key: abcdefghijklmnop",
			forbidden: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := containsForbiddenManifestValue(test.value); got != test.forbidden {
				t.Fatalf("containsForbiddenManifestValue(%q) = %t, want %t", test.value, got, test.forbidden)
			}
		})
	}
}

func TestWorkspaceComposerRejectsDuplicateFrozenSelections(t *testing.T) {
	now := time.Date(2026, time.July, 17, 12, 0, 0, 0, time.UTC)
	composer := NewWorkspaceComposer()
	composer.Now = func() time.Time { return now }
	plan := composerTestPlan([]string{"general_chat", "general_chat"})
	contextRecord := composerTestFrozenContext(plan)
	contextRecord.SelectedSkills = []string{"general_chat", "general_chat"}

	_, err := composer.BuildInputManifest(context.Background(), plan, contextRecord, composerTestInputs(plan, now))
	if err == nil || err.Error() != "AGENT_PLAN_INVALID" {
		t.Fatalf("duplicate frozen Skill selections err=%v, want AGENT_PLAN_INVALID", err)
	}
}

func TestWorkspaceComposerRejectsInconsistentFrozenContextIdentity(t *testing.T) {
	now := time.Date(2026, time.July, 21, 12, 0, 0, 0, time.UTC)
	composer := NewWorkspaceComposer()
	composer.Now = func() time.Time { return now }

	tests := []struct {
		name   string
		mutate func(*testing.T, *AgentRunPlan, *domain.RunWorkspaceContextRecord)
	}{
		{name: "plan context manifest hash", mutate: func(_ *testing.T, plan *AgentRunPlan, _ *domain.RunWorkspaceContextRecord) {
			plan.WorkspaceContextManifestHash = sha256String([]byte("other context"))
		}},
		{name: "plan workspace version", mutate: func(_ *testing.T, plan *AgentRunPlan, _ *domain.RunWorkspaceContextRecord) { plan.WorkspaceVersion++ }},
		{name: "plan index version", mutate: func(_ *testing.T, plan *AgentRunPlan, _ *domain.RunWorkspaceContextRecord) { plan.IndexVersion++ }},
		{name: "plan capability hash", mutate: func(_ *testing.T, plan *AgentRunPlan, _ *domain.RunWorkspaceContextRecord) {
			plan.CapabilityHash = "capability_other"
		}},
		{name: "context manifest hash", mutate: func(_ *testing.T, _ *AgentRunPlan, context *domain.RunWorkspaceContextRecord) {
			context.ManifestHash = sha256String([]byte("other manifest"))
		}},
		{name: "context workspace ID", mutate: func(t *testing.T, plan *AgentRunPlan, context *domain.RunWorkspaceContextRecord) {
			context.ContextManifest["workspaceId"] = "workspace_other"
			rebindComposerTestContextManifest(t, plan, context)
		}},
		{name: "context workspace fact", mutate: func(t *testing.T, plan *AgentRunPlan, context *domain.RunWorkspaceContextRecord) {
			context.ContextManifest["workspaceVersion"] = context.WorkspaceVersion + 1
			rebindComposerTestContextManifest(t, plan, context)
		}},
		{name: "context index fact", mutate: func(t *testing.T, plan *AgentRunPlan, context *domain.RunWorkspaceContextRecord) {
			context.ContextManifest["indexVersion"] = context.IndexVersion + 1
			rebindComposerTestContextManifest(t, plan, context)
		}},
		{name: "context capability fact", mutate: func(t *testing.T, plan *AgentRunPlan, context *domain.RunWorkspaceContextRecord) {
			context.ContextManifest["capabilityHash"] = "capability_other"
			rebindComposerTestContextManifest(t, plan, context)
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plan := composerTestPlan([]string{"general_chat"})
			contextRecord := composerTestFrozenContext(plan)
			test.mutate(t, &plan, &contextRecord)
			if _, err := composer.BuildInputManifest(context.Background(), plan, contextRecord, composerTestInputs(plan, now)); err == nil || err.Error() != "AGENT_PLAN_INVALID" {
				t.Fatalf("BuildInputManifest error=%v, want AGENT_PLAN_INVALID", err)
			}
		})
	}
}

func TestWorkspaceComposerRejectsMaterializerOwnedLogicalPaths(t *testing.T) {
	now := time.Date(2026, time.July, 21, 12, 0, 0, 0, time.UTC)
	composer := NewWorkspaceComposer()
	composer.Now = func() time.Time { return now }
	plan := composerTestPlan([]string{"general_chat"})
	base, err := composer.BuildInputManifest(context.Background(), plan, composerTestFrozenContext(plan), composerTestInputs(plan, now))
	if err != nil {
		t.Fatalf("build valid manifest: %v", err)
	}
	for _, logicalPath := range []string{"output", "output/result.md", "staging", "staging/request.json", ".materialization.json", ".materialization.json/cache"} {
		t.Run(logicalPath, func(t *testing.T) {
			manifest := base
			manifest.Files = []RuntimeManifestEntry{NewInlineRuntimeEntry(logicalPath, []byte("reserved"))}
			if err := composer.ValidateLogicalFiles(manifest); err == nil || err.Error() != "RUNTIME_WORKSPACE_MATERIALIZATION_FAILED" {
				t.Fatalf("ValidateLogicalFiles error=%v, want RUNTIME_WORKSPACE_MATERIALIZATION_FAILED", err)
			}
		})
	}
}

func TestWorkspaceComposerRequiresAtomicMetaWorkspaceIdentityTriple(t *testing.T) {
	now := time.Date(2026, time.July, 18, 12, 0, 0, 0, time.UTC)
	composer := NewWorkspaceComposer()
	composer.Now = func() time.Time { return now }
	validHash := "sha256:" + strings.Repeat("c", 64)
	basePlan := composerTestPlan([]string{"general_chat"})

	internalManifest, err := composer.BuildInputManifest(context.Background(), basePlan, composerTestFrozenContext(basePlan), composerTestInputs(basePlan, now))
	if err != nil {
		t.Fatalf("internal non-Meta manifest rejected: %v", err)
	}
	if internalManifest.MetaWorkspaceKey != "" || internalManifest.MetaWorkspaceVersion != "" || internalManifest.InputPolicyHash != "" {
		t.Fatalf("internal manifest unexpectedly carries Meta identity: %+v", internalManifest)
	}

	publicPlan := basePlan
	publicPlan.MetaWorkspaceKey = "visual_chat"
	publicPlan.MetaWorkspaceVersion = "v1"
	publicPlan.InputPolicyHash = validHash
	publicManifest, err := composer.BuildInputManifest(context.Background(), publicPlan, composerTestFrozenContext(publicPlan), composerTestInputs(publicPlan, now))
	if err != nil {
		t.Fatalf("complete public Meta identity rejected: %v", err)
	}
	if publicManifest.MetaWorkspaceKey != publicPlan.MetaWorkspaceKey || publicManifest.MetaWorkspaceVersion != publicPlan.MetaWorkspaceVersion || publicManifest.InputPolicyHash != publicPlan.InputPolicyHash {
		t.Fatalf("public Meta identity was not copied to runtime manifest: %+v", publicManifest)
	}

	attachmentBody := []byte("attachment bytes")
	attachment := AgentRunInputAttachmentIdentity{
		ResourceID: "resource_internal_attachment", Usage: "primary_input", MIMEType: "image/png", SizeBytes: int64(len(attachmentBody)),
		SHA256: sha256String(attachmentBody), Width: 1, Height: 1, LogicalPath: "input/attachments/01.png",
	}
	planWithInternalAttachment := basePlan
	planWithInternalAttachment.InputAttachments = []AgentRunInputAttachmentIdentity{attachment}
	inputsWithAttachment := composerTestInputs(planWithInternalAttachment, now)
	inputsWithAttachment.Files = append(inputsWithAttachment.Files, RuntimeManifestEntry{
		LogicalPath: attachment.LogicalPath, SourceType: "object_ref", SourceRef: "runtime-attachments/" + attachment.ResourceID,
		SizeBytes: attachment.SizeBytes, SHA256: attachment.SHA256,
	})
	if _, err := composer.BuildInputManifest(context.Background(), planWithInternalAttachment, composerTestFrozenContext(planWithInternalAttachment), inputsWithAttachment); err == nil || err.Error() != "AGENT_PLAN_INVALID" {
		t.Fatalf("internal attachment BuildInputManifest err=%v, want AGENT_PLAN_INVALID", err)
	}
	manifestWithInternalAttachment := internalManifest
	manifestWithInternalAttachment.Attachments = []AgentRunInputAttachmentIdentity{attachment}
	manifestWithInternalAttachment.Files = append(manifestWithInternalAttachment.Files, RuntimeManifestEntry{
		LogicalPath: attachment.LogicalPath, SourceType: "object_ref", SourceRef: "runtime-attachments/" + attachment.ResourceID,
		SizeBytes: attachment.SizeBytes, SHA256: attachment.SHA256,
	})
	if err := composer.ValidateLogicalFiles(manifestWithInternalAttachment); err == nil || err.Error() != "RUNTIME_WORKSPACE_MATERIALIZATION_FAILED" {
		t.Fatalf("internal attachment ValidateLogicalFiles err=%v, want RUNTIME_WORKSPACE_MATERIALIZATION_FAILED", err)
	}

	for _, test := range []struct {
		name    string
		key     string
		version string
		hash    string
	}{
		{name: "key only", key: "visual_chat"},
		{name: "version only", version: "v1"},
		{name: "hash only", hash: validHash},
		{name: "key and version", key: "visual_chat", version: "v1"},
		{name: "key and hash", key: "visual_chat", hash: validHash},
		{name: "version and hash", version: "v1", hash: validHash},
		{name: "forged key", key: "visual/chat", version: "v1", hash: validHash},
		{name: "forged version", key: "visual_chat", version: "v 1", hash: validHash},
		{name: "forged hash", key: "visual_chat", version: "v1", hash: "sha256:not-a-real-hash"},
	} {
		t.Run(test.name, func(t *testing.T) {
			plan := basePlan
			plan.MetaWorkspaceKey = test.key
			plan.MetaWorkspaceVersion = test.version
			plan.InputPolicyHash = test.hash
			if _, err := composer.BuildInputManifest(context.Background(), plan, composerTestFrozenContext(plan), composerTestInputs(plan, now)); err == nil || err.Error() != "AGENT_PLAN_INVALID" {
				t.Fatalf("BuildInputManifest partial/forged Meta identity err=%v, want AGENT_PLAN_INVALID", err)
			}

			manifest := internalManifest
			manifest.MetaWorkspaceKey = test.key
			manifest.MetaWorkspaceVersion = test.version
			manifest.InputPolicyHash = test.hash
			if err := composer.ValidateLogicalFiles(manifest); err == nil || err.Error() != "RUNTIME_WORKSPACE_MATERIALIZATION_FAILED" {
				t.Fatalf("ValidateLogicalFiles partial/forged Meta identity err=%v, want RUNTIME_WORKSPACE_MATERIALIZATION_FAILED", err)
			}
		})
	}
}

func TestWorkspaceComposerManifestHashAlwaysBindsMetaWorkspaceIdentityAndAttachments(t *testing.T) {
	manifest := RuntimeInputManifest{
		SchemaVersion: "runtime_input_manifest.v1", RunID: "run_manifest_hash", RuntimeHostID: "runtime-host-test-1",
		TenantID: "tenant_default", UserID: "user_manifest_hash", WorkspaceID: "workspace_manifest_hash",
		WorkspaceVersion: 1, ThreadWorkspaceBindingVersion: 1, ContextGeneration: 1,
		MetaRelease: "l1-agent-manifest.v2", AgentProfile: "visual_chat_agent",
		AgentHash:      "sha256:" + strings.Repeat("a", 64),
		SkillProfiles:  []RuntimeSkillProfile{{Profile: "visual_chat_assistant", Hash: "sha256:" + strings.Repeat("b", 64)}},
		CapabilityHash: "sha256:" + strings.Repeat("c", 64),
		ExpiresAt:      time.Date(2026, time.July, 19, 12, 0, 0, 0, time.UTC),
	}
	composer := WorkspaceComposer{}
	before := composer.ComputeManifestHash(manifest)

	manifest.MetaWorkspaceKey = "visual_chat"
	manifest.MetaWorkspaceVersion = "v1"
	manifest.InputPolicyHash = "sha256:" + strings.Repeat("d", 64)
	withMetaIdentity := composer.ComputeManifestHash(manifest)
	if before == withMetaIdentity {
		t.Fatal("manifest hash must bind the Meta Workspace identity")
	}

	manifest.Attachments = []AgentRunInputAttachmentIdentity{{ResourceID: "resource_manifest_hash", Usage: "primary_input", MIMEType: "image/png", SHA256: "sha256:" + strings.Repeat("e", 64)}}
	withAttachment := composer.ComputeManifestHash(manifest)
	if withMetaIdentity == withAttachment {
		t.Fatal("manifest hash must bind frozen attachment identity")
	}
}

func TestWorkspaceComposerRejectsNonCanonicalRuntimeSkillProfiles(t *testing.T) {
	now := time.Date(2026, time.July, 17, 12, 0, 0, 0, time.UTC)
	manifest := RuntimeInputManifest{
		SchemaVersion: "runtime_input_manifest.v1", RunID: "run_composer", RuntimeHostID: "host_composer",
		TenantID: "tenant_composer", UserID: "user_composer", WorkspaceID: "workspace_composer",
		WorkspaceVersion: 1, ContextGeneration: 1, MetaRelease: "release_composer",
		AgentProfile: "workspace_agent", AgentHash: "sha256:" + strings.Repeat("a", 64),
		SkillProfiles: []RuntimeSkillProfile{
			{Profile: "general_chat", Hash: "sha256:" + strings.Repeat("b", 64)},
			{Profile: "general_chat", Hash: "sha256:" + strings.Repeat("c", 64)},
		},
		CapabilityHash: "capability_composer", Files: []RuntimeManifestEntry{
			NewInlineRuntimeEntry("AGENTS.md", []byte("# Agent\n")),
		},
		ExpiresAt: now.Add(time.Minute),
	}
	if err := (WorkspaceComposer{}).ValidateLogicalFiles(manifest); err == nil || err.Error() != "RUNTIME_WORKSPACE_MATERIALIZATION_FAILED" {
		t.Fatalf("duplicate runtime Skill profiles err=%v, want RUNTIME_WORKSPACE_MATERIALIZATION_FAILED", err)
	}

	manifest = RuntimeInputManifest{
		SchemaVersion: "runtime_input_manifest.v1", RunID: "run_composer", RuntimeHostID: "host_composer",
		TenantID: "tenant_composer", UserID: "user_composer", WorkspaceID: "workspace_composer",
		WorkspaceVersion: 1, ContextGeneration: 1, MetaRelease: "release_composer",
		AgentProfile: "workspace agent", AgentHash: "sha256:" + strings.Repeat("a", 64),
		SkillProfiles:  []RuntimeSkillProfile{{Profile: "general_chat", Hash: "sha256:" + strings.Repeat("b", 64)}},
		CapabilityHash: "capability_composer", Files: []RuntimeManifestEntry{
			NewInlineRuntimeEntry("AGENTS.md", []byte("# Agent\n")),
		},
		ExpiresAt: now.Add(time.Minute),
	}
	if err := (WorkspaceComposer{}).ValidateLogicalFiles(manifest); err == nil || err.Error() != "RUNTIME_WORKSPACE_MATERIALIZATION_FAILED" {
		t.Fatalf("unsafe Agent profile err=%v, want RUNTIME_WORKSPACE_MATERIALIZATION_FAILED", err)
	}
}

func TestWorkspaceComposerRejectsUnsafeOrRemappedSourceReferences(t *testing.T) {
	now := time.Date(2026, time.July, 18, 12, 0, 0, 0, time.UTC)
	validHash := "sha256:" + strings.Repeat("a", 64)
	referenceBody := []byte("resolved by a trusted source")
	validEntryHash := sha256String(referenceBody)
	unsafeRefs := []struct {
		name       string
		sourceType string
		sourceRef  string
	}{
		{name: "unix absolute object reference", sourceType: "object_ref", sourceRef: "/var/lib/huahuo/private.md"},
		{name: "windows volume reference", sourceType: "meta_release_ref", sourceRef: "C:/runtime/AGENTS.md"},
		{name: "file URL reference", sourceType: "object_ref", sourceRef: "file:/var/lib/huahuo/private.md"},
		{name: "remote URL reference", sourceType: "object_ref", sourceRef: "https://storage.example/object"},
		{name: "noncanonical reference", sourceType: "meta_release_ref", sourceRef: "releases/./v1/AGENTS.md"},
		{name: "formal workspace URL", sourceType: "formal_workspace_ref", sourceRef: "file:/var/lib/huahuo/private.md"},
	}

	for _, tc := range unsafeRefs {
		t.Run(tc.name, func(t *testing.T) {
			manifest := RuntimeInputManifest{
				SchemaVersion: "runtime_input_manifest.v1", RunID: "run_composer", RuntimeHostID: "host_composer",
				TenantID: "tenant_composer", UserID: "user_composer", WorkspaceID: "workspace_composer",
				WorkspaceVersion: 1, ContextGeneration: 1, MetaRelease: "release_composer",
				AgentProfile: "workspace_agent", AgentHash: validHash,
				SkillProfiles:  []RuntimeSkillProfile{{Profile: "general_chat", Hash: validHash}},
				CapabilityHash: "capability_composer", ExpiresAt: now.Add(time.Minute),
				Files: []RuntimeManifestEntry{{
					LogicalPath: "profile/readme.md", SourceType: tc.sourceType, SourceRef: tc.sourceRef,
					SizeBytes: int64(len(referenceBody)), SHA256: validEntryHash,
				}},
			}
			if err := (WorkspaceComposer{}).ValidateLogicalFiles(manifest); err == nil || err.Error() != "RUNTIME_WORKSPACE_MATERIALIZATION_FAILED" {
				t.Fatalf("ValidateLogicalFiles() error=%v, want RUNTIME_WORKSPACE_MATERIALIZATION_FAILED", err)
			}
		})
	}
}

func TestWorkspaceComposerAcceptsCanonicalTrustedSourceReferences(t *testing.T) {
	now := time.Date(2026, time.July, 18, 12, 0, 0, 0, time.UTC)
	validHash := "sha256:" + strings.Repeat("a", 64)
	referenceBody := []byte("resolved by a trusted source")
	validEntryHash := sha256String(referenceBody)
	entries := []RuntimeManifestEntry{
		{LogicalPath: "input/request.md", SourceType: "object_ref", SourceRef: "objects/run-inputs/request-001", SizeBytes: int64(len(referenceBody)), SHA256: validEntryHash},
		{LogicalPath: "AGENTS.md", SourceType: "meta_release_ref", SourceRef: "releases/v1/agents/workspace/AGENTS.md", SizeBytes: int64(len(referenceBody)), SHA256: validEntryHash},
		{LogicalPath: "input/transcript.md", SourceType: "formal_workspace_ref", SourceRef: "materials/raw/recording.md", SizeBytes: int64(len(referenceBody)), SHA256: validEntryHash},
	}
	manifest := RuntimeInputManifest{
		SchemaVersion: "runtime_input_manifest.v1", RunID: "run_composer", RuntimeHostID: "host_composer",
		TenantID: "tenant_composer", UserID: "user_composer", WorkspaceID: "workspace_composer",
		WorkspaceVersion: 1, ContextGeneration: 1, MetaRelease: "release_composer",
		AgentProfile: "workspace_agent", AgentHash: validHash,
		SkillProfiles:  []RuntimeSkillProfile{{Profile: "general_chat", Hash: validHash}},
		CapabilityHash: "capability_composer", ExpiresAt: now.Add(time.Minute), Files: entries,
	}
	if err := (WorkspaceComposer{}).ValidateLogicalFiles(manifest); err != nil {
		t.Fatalf("canonical trusted source references rejected: %v", err)
	}
}

func TestWorkspaceComposerRejectsUnsafeRemoteObjectReadReference(t *testing.T) {
	now := time.Date(2026, time.July, 18, 12, 0, 0, 0, time.UTC)
	body := []byte("image bytes")
	entry := RuntimeManifestEntry{
		LogicalPath: "input/attachments/01.png", SourceType: "object_ref", SourceRef: "runtime-attachments/resource_remote",
		SizeBytes: int64(len(body)), SHA256: sha256String(body),
		ObjectRead: &RuntimeObjectReadReference{URL: "https://operator@example.test/object.png?sig=secret#fragment", ExpiresAt: now.Add(time.Minute), MIMEType: "image/png"},
	}
	manifest := RuntimeInputManifest{
		SchemaVersion: "runtime_input_manifest.v1", RunID: "run_composer_remote", RuntimeHostID: "host_composer",
		TenantID: "tenant_composer", UserID: "user_composer", WorkspaceID: "workspace_composer",
		WorkspaceVersion: 1, ContextGeneration: 1, MetaRelease: "release_composer", AgentProfile: "workspace_agent",
		MetaWorkspaceKey: "visual_chat", MetaWorkspaceVersion: "v1", InputPolicyHash: "sha256:" + strings.Repeat("c", 64),
		AgentHash: "sha256:" + strings.Repeat("a", 64), SkillProfiles: []RuntimeSkillProfile{{Profile: "general_chat", Hash: "sha256:" + strings.Repeat("b", 64)}},
		CapabilityHash: "capability_composer", Files: []RuntimeManifestEntry{entry}, ExpiresAt: now.Add(2 * time.Minute),
		Attachments: []AgentRunInputAttachmentIdentity{{
			ResourceID: "resource_remote", Usage: "primary_input", MIMEType: "image/png", SizeBytes: int64(len(body)), SHA256: entry.SHA256,
			Width: 1, Height: 1, LogicalPath: entry.LogicalPath,
		}},
	}
	if err := (WorkspaceComposer{}).ValidateLogicalFiles(manifest); err == nil || err.Error() != "RUNTIME_WORKSPACE_MATERIALIZATION_FAILED" {
		t.Fatalf("unsafe remote object reference err=%v", err)
	}

	manifest.Files[0].ObjectRead = &RuntimeObjectReadReference{URL: "https://oss.example.test/object.png?sig=opaque", ExpiresAt: now.Add(time.Minute), MIMEType: "image/png"}
	if err := (WorkspaceComposer{}).ValidateLogicalFiles(manifest); err != nil {
		t.Fatalf("canonical remote object reference err=%v", err)
	}
}

func composerTestPlan(skills []string) AgentRunPlan {
	plan := AgentRunPlan{
		AgentRunID: "run_composer", L1AgentProfile: "workspace_agent", ManifestVersion: "manifest_composer",
		AgentRelativeRoot: "agents/workspace_agent", AgentHash: "sha256:" + strings.Repeat("a", 64),
		RoutingMode: "dynamic", SelectedSkillProfiles: skills, WorkspaceVersion: 1, IndexVersion: 1, CapabilityHash: "capability_composer",
	}
	plan.WorkspaceContextManifestHash = composerTestContextManifestHash(plan)
	return plan
}

func composerTestFrozenContext(plan AgentRunPlan) domain.RunWorkspaceContextRecord {
	contextManifest := composerTestContextManifest(plan)
	return domain.RunWorkspaceContextRecord{
		RunID: plan.AgentRunID, AgentRunID: plan.AgentRunID, TenantID: "tenant_composer", UserID: "user_composer", WorkspaceID: "workspace_composer",
		WorkspaceVersion: plan.WorkspaceVersion, IndexVersion: plan.IndexVersion, ContextGeneration: 1, L1AgentProfile: plan.L1AgentProfile, AgentRelativeRoot: plan.AgentRelativeRoot,
		ManifestVersion: plan.ManifestVersion, CapabilityHash: plan.CapabilityHash, SelectedSkills: append([]string(nil), plan.SelectedSkillProfiles...),
		ContextManifest: contextManifest, ManifestHash: composerTestContextManifestHash(plan), Status: "frozen",
	}
}

func composerTestContextManifest(plan AgentRunPlan) map[string]any {
	return map[string]any{
		"workspaceId": "workspace_composer", "workspaceVersion": plan.WorkspaceVersion,
		"indexVersion": plan.IndexVersion, "capabilityHash": plan.CapabilityHash,
	}
}

func composerTestContextManifestHash(plan AgentRunPlan) string {
	raw, err := json.Marshal(composerTestContextManifest(plan))
	if err != nil {
		panic(err)
	}
	return sha256String(raw)
}

func rebindComposerTestContextManifest(t *testing.T, plan *AgentRunPlan, contextRecord *domain.RunWorkspaceContextRecord) {
	t.Helper()
	raw, err := json.Marshal(contextRecord.ContextManifest)
	if err != nil {
		t.Fatal(err)
	}
	contextRecord.ManifestHash = sha256String(raw)
	plan.WorkspaceContextManifestHash = contextRecord.ManifestHash
}

func composerTestInputs(plan AgentRunPlan, now time.Time) RuntimeManifestBuildInputs {
	return RuntimeManifestBuildInputs{
		RuntimeHostID: "host_composer", MetaRelease: "release_composer", AgentHash: plan.AgentHash,
		SkillHashes: map[string]string{"general_chat": "sha256:" + strings.Repeat("b", 64)},
		Files:       []RuntimeManifestEntry{NewInlineRuntimeEntry("AGENTS.md", []byte("# Agent\n"))},
		ExpiresAt:   now.Add(time.Minute),
	}
}
