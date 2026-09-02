package workers

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"huahuoai/backend/source/internal/domain"
	"huahuoai/backend/source/internal/persistence"
	runtimepkg "huahuoai/backend/source/internal/runtime"
)

func TestFilesystemRuntimeManifestProviderBuildsCompleteHuokeTopicPackageAndBoundedSnapshot(t *testing.T) {
	fixture := newHuokeManifestFixture(t)

	for relative, body := range map[string]string{
		"resources/overview.md":                           "# Overview\n",
		"resources/profile.md":                            "# Profile\n",
		"profile/user-positioning/positioning-profile.md": "# Positioning\n",
		"profile/preference-boundaries.md":                "# Boundaries\n",
		"profile/assets/products/legacy-product.md":       "must not materialize\n",
	} {
		writeManifestFixtureFile(t, fixture.workspaceRoot, relative, body)
	}
	var materialsIndex strings.Builder
	for index := 0; index < huokeTopicMaxMaterialFiles+2; index++ {
		reference := fmt.Sprintf("materials/raw/material-%02d.md", index)
		materialsIndex.WriteString("| `" + reference + "` |\n")
		writeManifestFixtureFile(t, fixture.workspaceRoot, reference, fmt.Sprintf("material %d\n", index))
	}
	materialsIndex.WriteString("- template: `materials/raw/<materialId>.md`\n")
	writeManifestFixtureFile(t, fixture.workspaceRoot, "resources/materials.md", materialsIndex.String())

	inputs, err := fixture.provider.Build(context.Background(), fixture.run, fixture.plan, fixture.frozen, runtimepkg.RuntimeHost{})
	if err != nil {
		t.Fatal(err)
	}
	entries := manifestEntriesByLogicalPath(t, inputs.Files)
	for _, relative := range huokeTopicAgentFiles {
		if relative == "capability-catalog.json" {
			continue
		}
		entry, ok := entries[relative]
		if !ok || entry.SourceType != "inline" {
			t.Fatalf("missing complete Agent file %s: %+v", relative, entry)
		}
	}
	if _, exists := entries["capability-catalog.json"]; exists {
		t.Fatal("product-thread Huoke manifest exposed the capability catalog")
	}
	if entries["AGENTS.md"].InlineContent != fixture.agentBody {
		t.Fatalf("runtime-agents package was not preferred: %q", entries["AGENTS.md"].InlineContent)
	}
	for _, relative := range huokeTopicSkillFiles {
		logical := "skills/" + huokeTopicSkillProfile + "/" + relative
		if entry, ok := entries[logical]; !ok || entry.SourceType != "inline" {
			t.Fatalf("missing complete Skill file %s: %+v", logical, entry)
		}
	}

	formalCount := 0
	materialCount := 0
	for _, entry := range inputs.Files {
		if entry.SourceType != "formal_workspace_ref" {
			continue
		}
		formalCount++
		if entry.SourceRef != entry.LogicalPath || entry.InlineContent != "" || entry.SHA256 == "" || entry.SizeBytes < 1 {
			t.Fatalf("invalid formal Workspace entry: %+v", entry)
		}
		if strings.HasPrefix(entry.LogicalPath, "profile/assets/") {
			t.Fatalf("V1 Runtime manifest must not materialize retired Profile assets: %+v", entry)
		}
		if strings.HasPrefix(entry.LogicalPath, "materials/") {
			materialCount++
		}
	}
	if materialCount != huokeTopicMaxMaterialFiles {
		t.Fatalf("material snapshot count=%d want=%d", materialCount, huokeTopicMaxMaterialFiles)
	}
	if formalCount != 5+huokeTopicMaxMaterialFiles {
		t.Fatalf("formal snapshot count=%d", formalCount)
	}
	for _, relative := range []string{
		"resources/overview.md",
		"resources/profile.md",
		"resources/materials.md",
		"profile/user-positioning/positioning-profile.md",
		"profile/preference-boundaries.md",
	} {
		if entry, ok := entries[relative]; !ok || entry.SourceType != "formal_workspace_ref" {
			t.Fatalf("missing profile snapshot %s: %+v", relative, entry)
		}
	}
	if _, err := os.Stat(filepath.Join(fixture.workspaceRoot, "input")); !os.IsNotExist(err) {
		t.Fatalf("provider wrote into formal Workspace: %v", err)
	}
	assertHuokeManifestMaterializes(t, fixture, inputs)
}

func TestFilesystemRuntimeManifestProviderHuokeTopicFailsClosedOnRequiredPackageFiles(t *testing.T) {
	tests := []struct {
		name      string
		relative  string
		errorCode string
	}{
		{name: "agent protocol", relative: "runtime-agents/huoke_neirong_agent/protocols/topic-guidance-result.v1.md", errorCode: "AGENT_PROFILE_UNAVAILABLE"},
		{name: "skill reference", relative: "runtime-skills/huoke_topic_strategy/references/strategy-fit-contracts.md", errorCode: "SKILL_UNAVAILABLE"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newHuokeManifestFixture(t)
			if err := os.Remove(filepath.Join(fixture.metaRoot, filepath.FromSlash(test.relative))); err != nil {
				t.Fatal(err)
			}
			_, err := fixture.provider.Build(context.Background(), fixture.run, fixture.plan, fixture.frozen, runtimepkg.RuntimeHost{})
			if err == nil || err.Error() != test.errorCode {
				t.Fatalf("err=%v want=%s", err, test.errorCode)
			}
		})
	}
}

func TestFilesystemRuntimeManifestProviderHuokeTopicAllowsMissingOptionalWorkspaceFiles(t *testing.T) {
	fixture := newHuokeManifestFixture(t)
	inputs, err := fixture.provider.Build(context.Background(), fixture.run, fixture.plan, fixture.frozen, runtimepkg.RuntimeHost{})
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range inputs.Files {
		if entry.SourceType == "formal_workspace_ref" {
			t.Fatalf("unexpected optional formal Workspace entry: %+v", entry)
		}
	}
}

func TestFilesystemRuntimeManifestProviderPrefersFormalWorkspaceAgentAndSelectedSkillPackage(t *testing.T) {
	fixture := newHuokeManifestFixture(t)
	formalAgent := "# Formal Workspace Huoke Agent\n"
	formalSoul := "# Formal Workspace Soul\n"
	formalSkill := "# Formal Workspace Huoke Topic Skill\n"
	formalProtocol := "# Formal Workspace Additional Protocol\n"
	formalReference := "# Formal Workspace Method Traceability\n"

	writeManifestFixtureFile(t, fixture.workspaceRoot, "AGENTS.md", formalAgent)
	writeManifestFixtureFile(t, fixture.workspaceRoot, "SOUL.md", formalSoul)
	writeManifestFixtureFile(t, fixture.workspaceRoot, "skills/"+huokeTopicSkillProfile+"/SKILL.md", formalSkill)
	writeManifestFixtureFile(t, fixture.workspaceRoot, "skills/"+huokeTopicSkillProfile+"/references/method-traceability.md", formalReference)
	writeManifestFixtureFile(t, fixture.workspaceRoot, "protocols/additional-result.v2.md", formalProtocol)
	writeManifestFixtureFile(t, fixture.workspaceRoot, "skills/unselected_skill/SKILL.md", "must not be loaded\n")

	inputs, err := fixture.provider.Build(context.Background(), fixture.run, fixture.plan, fixture.frozen, runtimepkg.RuntimeHost{})
	if err != nil {
		t.Fatal(err)
	}
	entries := manifestEntriesByLogicalPath(t, inputs.Files)
	for logicalPath, expected := range map[string]string{
		"AGENTS.md": formalAgent,
		"SOUL.md":   formalSoul,
		"skills/" + huokeTopicSkillProfile + "/SKILL.md":                          formalSkill,
		"skills/" + huokeTopicSkillProfile + "/references/method-traceability.md": formalReference,
		"protocols/additional-result.v2.md":                                       formalProtocol,
	} {
		entry, ok := entries[logicalPath]
		if !ok || entry.SourceType != "formal_workspace_ref" || entry.SourceRef != logicalPath || entry.SHA256 != dispatcherSHA256([]byte(expected)) {
			t.Fatalf("formal Workspace package file did not win %s: %+v", logicalPath, entry)
		}
	}
	if entry := entries["TOOLS.md"]; entry.SourceType != "inline" {
		t.Fatalf("missing formal file must retain Meta fallback: %+v", entry)
	}
	if _, exists := entries["skills/unselected_skill/SKILL.md"]; exists {
		t.Fatal("unselected Workspace Skill leaked into runtime manifest")
	}
	if inputs.SkillHashes[huokeTopicSkillProfile] != dispatcherSHA256([]byte(formalSkill)) {
		t.Fatalf("effective Skill hash does not describe the loaded Workspace Skill: %#v", inputs.SkillHashes)
	}
	assertHuokeManifestMaterializes(t, fixture, inputs)
}

func TestFilesystemRuntimeManifestProviderInjectsOnlyValidatedThreadScopedHuokeState(t *testing.T) {
	fixture := newHuokeManifestFixture(t)
	fixture.run.TaskID = "task_huoke_manifest_state"
	fixture.run.ThreadID = "thread_huoke_manifest_state"
	called := []string{}
	fixture.provider.HuokeTopicStateLoader = func(userID, workspaceID, threadID, excludeTaskID string) map[string]any {
		called = []string{userID, workspaceID, threadID, excludeTaskID}
		return huokeManifestConsultationState()
	}
	inputs, err := fixture.provider.Build(context.Background(), fixture.run, fixture.plan, fixture.frozen, runtimepkg.RuntimeHost{})
	if err != nil {
		t.Fatal(err)
	}
	if len(called) != 4 || called[0] != fixture.run.UserID || called[1] != fixture.run.WorkspaceID || called[2] != fixture.run.ThreadID || called[3] != fixture.run.TaskID {
		t.Fatalf("state loader was not bound to exact run identity: %#v", called)
	}
	entry, ok := manifestEntriesByLogicalPath(t, inputs.Files)["input/consultation_state.json"]
	if !ok || entry.SourceType != "inline" || entry.SourceRef != "" {
		t.Fatalf("validated consultation state was not an inline runtime-only input: %+v", entry)
	}
	decoded := map[string]any{}
	if err := json.Unmarshal([]byte(entry.InlineContent), &decoded); err != nil || runtimepkg.HuokeTopicConsultationStateVersion(decoded) != 1 {
		t.Fatalf("injected consultation state is invalid: state=%#v err=%v", decoded, err)
	}

	fixture.provider.HuokeTopicStateLoader = func(_, _, _, _ string) map[string]any { return map[string]any{} }
	inputs, err = fixture.provider.Build(context.Background(), fixture.run, fixture.plan, fixture.frozen, runtimepkg.RuntimeHost{})
	if err != nil {
		t.Fatal(err)
	}
	entry = manifestEntriesByLogicalPath(t, inputs.Files)["input/consultation_state.json"]
	decoded = map[string]any{}
	if err := json.Unmarshal([]byte(entry.InlineContent), &decoded); err != nil ||
		decoded["profileContextVersion"] != "wv:1|iv:0|cg:1" || runtimepkg.HuokeTopicConsultationStateVersion(decoded) != 1 {
		t.Fatalf("backend baseline was not injected: state=%#v err=%v", decoded, err)
	}

	fixture.provider.HuokeTopicStateLoader = func(_, _, _, _ string) map[string]any {
		return map[string]any{"schemaVersion": runtimepkg.HuokeTopicStateSchemaVersion, "stateVersion": 1}
	}
	if _, err := fixture.provider.Build(context.Background(), fixture.run, fixture.plan, fixture.frozen, runtimepkg.RuntimeHost{}); err == nil || err.Error() != "RUNTIME_WORKSPACE_MATERIALIZATION_FAILED" {
		t.Fatalf("invalid backend state reached the runtime manifest: %v", err)
	}
}

func TestFilesystemRuntimeManifestProviderRejectsFormalWorkspaceFileChangedDuringBuild(t *testing.T) {
	fixture := newHuokeManifestFixture(t)
	writeManifestFixtureFile(t, fixture.workspaceRoot, "resources/overview.md", "# Workspace W1\n")
	fixture.provider.HuokeTopicStateLoader = func(_, _, _, _ string) map[string]any {
		// huokeTopicWorkspaceEntries has already read the formal Workspace files
		// when it asks for backend state. Simulate a concurrent W2 replacement
		// before Build returns so the final reference verification must reject it.
		writeManifestFixtureFile(t, fixture.workspaceRoot, "resources/overview.md", "# Workspace W2\n")
		return huokeManifestConsultationState()
	}

	_, err := fixture.provider.Build(context.Background(), fixture.run, fixture.plan, fixture.frozen, runtimepkg.RuntimeHost{})
	if err == nil || err.Error() != "RUNTIME_WORKSPACE_MATERIALIZATION_FAILED" {
		t.Fatalf("formal Workspace replacement during manifest build err=%v", err)
	}
}

func TestFilesystemRuntimeManifestProviderRejectsFormalWorkspaceFileChangedAfterBuildAtHost(t *testing.T) {
	fixture := newHuokeManifestFixture(t)
	writeManifestFixtureFile(t, fixture.workspaceRoot, "resources/overview.md", "# Workspace W1\n")
	inputs, err := fixture.provider.Build(context.Background(), fixture.run, fixture.plan, fixture.frozen, runtimepkg.RuntimeHost{})
	if err != nil {
		t.Fatalf("build W1 manifest: %v", err)
	}

	now := time.Now().UTC().Truncate(time.Second)
	inputs.RuntimeHostID = "host-huoke-version-fence"
	inputs.ExpiresAt = now.Add(10 * time.Minute)
	composer := runtimepkg.NewWorkspaceComposer()
	composer.Now = func() time.Time { return now }
	manifest, err := composer.BuildInputManifest(context.Background(), fixture.plan, fixture.frozen, inputs)
	if err != nil {
		t.Fatalf("compose W1 manifest: %v", err)
	}
	writeManifestFixtureFile(t, fixture.workspaceRoot, "resources/overview.md", "# Workspace W2\n")

	secret := "huoke-version-fence-ticket-secret"
	ticket, err := runtimepkg.SignRunTicket(runtimepkg.RunTicketClaims{
		RunID: manifest.RunID, TenantID: manifest.TenantID, ReservationID: "reservation-huoke-version-fence",
		RuntimeHostID: manifest.RuntimeHostID, CapabilityHash: manifest.CapabilityHash,
		WorkspaceID: manifest.WorkspaceID, WorkspaceVersion: manifest.WorkspaceVersion,
		ContextGeneration: manifest.ContextGeneration, InputManifestHash: manifest.ManifestHash,
		PlanHash: runtimepkg.RunTicketJTIHash("huoke-version-fence-plan"), FencingToken: 1,
		JTI: "jti-huoke-version-fence", IssuedAt: now.Unix(), ExpiresAt: manifest.ExpiresAt.Unix(),
	}, secret)
	if err != nil {
		t.Fatalf("sign W1 ticket: %v", err)
	}
	materializer := runtimepkg.NewRuntimeWorkspaceMaterializer(
		manifest.RuntimeHostID, t.TempDir(), secret, huokeFixtureSourceResolver{root: fixture.workspaceRoot}, runtimepkg.NewMemoryRunTicketJTIStore(),
	)
	materializer.Now = func() time.Time { return now }
	if _, err := materializer.Materialize(context.Background(), ticket, manifest); err == nil || err.Error() != "RUNTIME_WORKSPACE_MATERIALIZATION_FAILED" {
		t.Fatalf("Host accepted W2 content under W1 manifest err=%v", err)
	}
}

func TestFilesystemRuntimeManifestProviderBuildsFayaPackageAndProfileSnapshot(t *testing.T) {
	metaRoot, dataRoot := t.TempDir(), t.TempDir()
	agentBody := "# Faya Agent\n"
	skillBody := "# Faya Skill\n"
	for _, relative := range fayaGerminationAgentFiles {
		body := "faya package " + relative + "\n"
		if relative == "AGENTS.md" {
			body = agentBody
		}
		writeManifestFixtureFile(t, filepath.Join(metaRoot, "runtime-agents", fayaGerminationAgentProfile), relative, body)
	}
	writeManifestFixtureFile(t, filepath.Join(metaRoot, "runtime-skills", fayaGerminationSkillProfile), "SKILL.md", skillBody)

	const (
		runID       = "run_faya_manifest"
		tenantID    = "tenant_faya_manifest"
		userID      = "user_faya_manifest"
		workspaceID = "workspace_faya_manifest"
	)
	workspaceRoot := filepath.Join(dataRoot, "workspaces", "tenants", tenantID, "users", userID, "workspaces", workspaceID)
	for relative, body := range map[string]string{
		"resources/overview.md":                           "# Overview\n",
		"resources/profile.md":                            "# Profile navigation\n",
		"profile/user-positioning/positioning-profile.md": "# Confirmed positioning\n",
		"profile/preference-boundaries.md":                "# Boundaries\n",
		"profile/assets/viewpoints/overview.md":           "# Legacy generated overview\n",
	} {
		writeManifestFixtureFile(t, workspaceRoot, relative, body)
	}
	run := persistence.AgentRunRecord{AgentRunID: runID, TenantID: tenantID, UserID: userID, WorkspaceID: workspaceID}
	plan := runtimepkg.AgentRunPlan{
		AgentRunID: runID, TaskType: fayaGerminationTaskType, L1AgentProfile: fayaGerminationAgentProfile,
		AgentHash: dispatcherSHA256([]byte(agentBody)), SelectedSkillProfiles: []string{fayaGerminationSkillProfile},
	}
	frozen := domain.RunWorkspaceContextRecord{RunID: runID, AgentRunID: runID, TenantID: tenantID, UserID: userID, WorkspaceID: workspaceID}
	provider := FilesystemRuntimeManifestProvider{
		Root: metaRoot, DataRoot: dataRoot, MetaRelease: "faya-release-test",
		SkillHashes: map[string]string{fayaGerminationSkillProfile: dispatcherSHA256([]byte(skillBody))},
	}
	inputs, err := provider.Build(context.Background(), run, plan, frozen, runtimepkg.RuntimeHost{})
	if err != nil {
		t.Fatal(err)
	}
	entries := manifestEntriesByLogicalPath(t, inputs.Files)
	for _, relative := range fayaGerminationAgentFiles {
		if relative == "capability-catalog.json" {
			continue
		}
		if entry, ok := entries[relative]; !ok || entry.SourceType != "inline" {
			t.Fatalf("missing Faya package file %s: %+v", relative, entry)
		}
	}
	if _, exists := entries["capability-catalog.json"]; exists {
		t.Fatal("product-thread Faya manifest exposed the capability catalog")
	}
	if entry, ok := entries["skills/viewpoint_germination/SKILL.md"]; !ok || entry.SourceType != "inline" {
		t.Fatalf("missing Faya Skill: %+v", entry)
	}
	for _, relative := range []string{
		"resources/overview.md",
		"resources/profile.md",
		"profile/user-positioning/positioning-profile.md",
		"profile/preference-boundaries.md",
	} {
		if entry, ok := entries[relative]; !ok || entry.SourceType != "formal_workspace_ref" || entry.SourceRef != relative {
			t.Fatalf("missing Faya formal Workspace file %s: %+v", relative, entry)
		}
	}
	if _, ok := entries["profile/assets/viewpoints/overview.md"]; ok {
		t.Fatal("legacy fact-directory overview must not enter the Faya runtime manifest")
	}
	if _, err := os.Stat(filepath.Join(workspaceRoot, "input")); !os.IsNotExist(err) {
		t.Fatalf("provider wrote into formal Workspace: %v", err)
	}
}

func TestFilesystemRuntimeManifestProviderFayaFailsClosedOnIncompletePackage(t *testing.T) {
	metaRoot := t.TempDir()
	agentBody := "# Faya Agent\n"
	skillBody := "# Faya Skill\n"
	writeManifestFixtureFile(t, filepath.Join(metaRoot, "runtime-agents", fayaGerminationAgentProfile), "AGENTS.md", agentBody)
	writeManifestFixtureFile(t, filepath.Join(metaRoot, "runtime-skills", fayaGerminationSkillProfile), "SKILL.md", skillBody)
	provider := FilesystemRuntimeManifestProvider{
		Root: metaRoot, DataRoot: t.TempDir(), MetaRelease: "faya-release-test",
		SkillHashes: map[string]string{fayaGerminationSkillProfile: dispatcherSHA256([]byte(skillBody))},
	}
	_, err := provider.Build(context.Background(), persistence.AgentRunRecord{}, runtimepkg.AgentRunPlan{
		TaskType: fayaGerminationTaskType, L1AgentProfile: fayaGerminationAgentProfile,
		AgentHash: dispatcherSHA256([]byte(agentBody)), SelectedSkillProfiles: []string{fayaGerminationSkillProfile},
	}, domain.RunWorkspaceContextRecord{}, runtimepkg.RuntimeHost{})
	if err == nil || err.Error() != "AGENT_PROFILE_UNAVAILABLE" {
		t.Fatalf("err=%v want AGENT_PROFILE_UNAVAILABLE", err)
	}
}

func TestFilesystemRuntimeManifestProviderIncludesPositioningContractBundle(t *testing.T) {
	metaRoot := t.TempDir()
	skillBody := "# Positioning Skill\n"
	contractBody := "# Positioning Subject Contracts\n"
	skillRoot := filepath.Join(metaRoot, "runtime-skills", positioningSkillProfile)
	writeManifestFixtureFile(t, skillRoot, "SKILL.md", skillBody)
	writeManifestFixtureFile(t, skillRoot, "references/positioning-subject-contracts.md", contractBody)

	provider := FilesystemRuntimeManifestProvider{
		Root:        metaRoot,
		SkillHashes: map[string]string{positioningSkillProfile: dispatcherSHA256([]byte(skillBody))},
	}
	plan := runtimepkg.AgentRunPlan{
		TaskType:              "feed_ai_chat",
		SelectedSkillProfiles: []string{positioningSkillProfile},
	}

	hashes, entries, err := provider.skillManifestEntries(plan)
	if err != nil {
		t.Fatal(err)
	}
	if hashes[positioningSkillProfile] != dispatcherSHA256([]byte(skillBody)) {
		t.Fatalf("unexpected positioning skill hash: %q", hashes[positioningSkillProfile])
	}
	byPath := manifestEntriesByLogicalPath(t, entries)
	for _, relative := range positioningSkillFiles {
		logical := "skills/" + positioningSkillProfile + "/" + relative
		entry, ok := byPath[logical]
		if !ok || entry.SourceType != "inline" || entry.InlineContent == "" {
			t.Fatalf("missing positioning package file %s: %+v", logical, entry)
		}
	}
}

func TestFilesystemRuntimeManifestProviderRejectsPositioningSkillWithoutContractBundle(t *testing.T) {
	metaRoot := t.TempDir()
	skillBody := "# Positioning Skill\n"
	writeManifestFixtureFile(t, filepath.Join(metaRoot, "runtime-skills", positioningSkillProfile), "SKILL.md", skillBody)
	provider := FilesystemRuntimeManifestProvider{
		Root:        metaRoot,
		SkillHashes: map[string]string{positioningSkillProfile: dispatcherSHA256([]byte(skillBody))},
	}
	plan := runtimepkg.AgentRunPlan{TaskType: "feed_ai_chat", SelectedSkillProfiles: []string{positioningSkillProfile}}

	_, _, err := provider.skillManifestEntries(plan)
	if err == nil || err.Error() != "SKILL_UNAVAILABLE" {
		t.Fatalf("err=%v want=SKILL_UNAVAILABLE", err)
	}
}

func TestFilesystemRuntimeManifestProviderHuokeTopicRejectsTraversalAndCrossWorkspaceContext(t *testing.T) {
	t.Run("material index traversal", func(t *testing.T) {
		fixture := newHuokeManifestFixture(t)
		writeManifestFixtureFile(t, fixture.workspaceRoot, "resources/materials.md", "| `materials/raw/../../workspace_other/secret.md` |\n")
		_, err := fixture.provider.Build(context.Background(), fixture.run, fixture.plan, fixture.frozen, runtimepkg.RuntimeHost{})
		if err == nil || err.Error() != "RUNTIME_WORKSPACE_MATERIALIZATION_FAILED" {
			t.Fatalf("err=%v", err)
		}
	})

	t.Run("frozen identity mismatch", func(t *testing.T) {
		fixture := newHuokeManifestFixture(t)
		fixture.frozen.WorkspaceID = "workspace_other"
		_, err := fixture.provider.Build(context.Background(), fixture.run, fixture.plan, fixture.frozen, runtimepkg.RuntimeHost{})
		if err == nil || err.Error() != "RUNTIME_WORKSPACE_MATERIALIZATION_FAILED" {
			t.Fatalf("err=%v", err)
		}
	})
}

func TestFilesystemRuntimeManifestProviderHuokeTopicRejectsWorkspaceSymlink(t *testing.T) {
	fixture := newHuokeManifestFixture(t)
	materialRoot := filepath.Join(fixture.workspaceRoot, "materials", "raw")
	if err := os.MkdirAll(materialRoot, 0o750); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(t.TempDir(), "outside.md")
	if err := os.WriteFile(outside, []byte("outside\n"), 0o640); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(materialRoot, "linked.md")); err != nil {
		t.Skipf("symlink unavailable on this platform: %v", err)
	}
	writeManifestFixtureFile(t, fixture.workspaceRoot, "resources/materials.md", "| `materials/raw/linked.md` |\n")
	_, err := fixture.provider.Build(context.Background(), fixture.run, fixture.plan, fixture.frozen, runtimepkg.RuntimeHost{})
	if err == nil || err.Error() != "RUNTIME_WORKSPACE_MATERIALIZATION_FAILED" {
		t.Fatalf("err=%v", err)
	}
}

func TestFilesystemRuntimeManifestProviderDynamicCatalogUsesFrozenRootAndSelectedKnowledge(t *testing.T) {
	metaRoot := t.TempDir()
	dataRoot := t.TempDir()
	const (
		agentProfile = "catalog_dynamic_agent"
		skillProfile = "catalog_dynamic_skill"
	)
	agentBody := "# Catalog Dynamic Agent\n"
	skillBody := "# Catalog Dynamic Skill\n"
	agentRoot := filepath.Join(metaRoot, "runtime-agents", agentProfile)
	for _, relative := range dynamicAgentCoreFiles {
		body := "agent file " + relative + "\n"
		if relative == "AGENTS.md" {
			body = agentBody
		}
		writeManifestFixtureFile(t, agentRoot, relative, body)
	}
	writeManifestFixtureFile(t, agentRoot, "knowledge/selected/overview.md", "# Selected knowledge\n")
	writeManifestFixtureFile(t, agentRoot, "knowledge/selected/detail.md", "selected detail\n")
	writeManifestFixtureFile(t, agentRoot, "knowledge/unselected/hidden.md", "must not materialize\n")
	writeManifestFixtureFile(t, filepath.Join(metaRoot, "agents", agentProfile), "AGENTS.md", "# Legacy package must not win\n")
	writeManifestFixtureFile(t, filepath.Join(metaRoot, "runtime-skills", skillProfile), "SKILL.md", skillBody)

	run := persistence.AgentRunRecord{
		AgentRunID: "run_dynamic_catalog", TenantID: "tenant_dynamic", UserID: "user_dynamic", WorkspaceID: "workspace_dynamic",
		RequestSnapshot: map[string]any{"input": map[string]any{"type": "text", "text": "catalog request"}},
		IntentSnapshot:  map[string]any{"resolvedTaskType": "catalog_dynamic_task"},
	}
	workspaceRoot := filepath.Join(dataRoot, "workspaces", "tenants", run.TenantID, "users", run.UserID, "workspaces", run.WorkspaceID)
	if err := os.MkdirAll(workspaceRoot, 0o750); err != nil {
		t.Fatal(err)
	}
	formalAgent := "# Formal Workspace Dynamic Agent\n"
	formalSkill := "# Formal Workspace Dynamic Skill\n"
	formalProtocol := "# Formal Workspace Dynamic Protocol\n"
	writeManifestFixtureFile(t, workspaceRoot, "AGENTS.md", formalAgent)
	writeManifestFixtureFile(t, workspaceRoot, "skills/"+skillProfile+"/SKILL.md", formalSkill)
	writeManifestFixtureFile(t, workspaceRoot, "protocols/dynamic-result.v1.md", formalProtocol)
	plan := runtimepkg.AgentRunPlan{
		SchemaVersion: "agent_run_plan.v1", AgentRunID: run.AgentRunID, PlanVersion: 1, RoutingMode: "dynamic",
		TaskType: "catalog_dynamic_task", ExecutionScope: "product_thread", L1AgentProfile: agentProfile,
		AgentRelativeRoot: "agents/" + agentProfile, AgentHash: dispatcherSHA256([]byte(agentBody)), ManifestVersion: "catalog-release-v1",
		SelectedSkillProfiles: []string{skillProfile}, SelectedKnowledgeRefs: []string{"knowledge/selected/detail.md", "knowledge/selected/overview.md"}, CapabilityHash: "capability-dynamic",
	}
	frozen := domain.RunWorkspaceContextRecord{
		RunID: run.AgentRunID, AgentRunID: run.AgentRunID, TenantID: run.TenantID, UserID: run.UserID, WorkspaceID: run.WorkspaceID,
		WorkspaceVersion: 1, ContextGeneration: 1, L1AgentProfile: agentProfile, AgentRelativeRoot: plan.AgentRelativeRoot,
		ManifestVersion: plan.ManifestVersion, CapabilityHash: plan.CapabilityHash, SelectedSkills: []string{skillProfile},
		SelectedKnowledgeRefs: []string{"knowledge/selected/detail.md", "knowledge/selected/overview.md"}, Status: "frozen",
	}
	provider := FilesystemRuntimeManifestProvider{
		Root: metaRoot, DataRoot: dataRoot, MetaRelease: "catalog-release-v1",
		SkillHashes: map[string]string{skillProfile: dispatcherSHA256([]byte(skillBody))},
	}
	inputs, err := provider.Build(context.Background(), run, plan, frozen, runtimepkg.RuntimeHost{})
	if err != nil {
		t.Fatal(err)
	}
	entries := manifestEntriesByLogicalPath(t, inputs.Files)
	if entries["AGENTS.md"].InlineContent != formalAgent {
		t.Fatalf("dynamic root did not seal the formal Workspace override: %+v", entries["AGENTS.md"])
	}
	for _, relative := range dynamicAgentCoreFiles {
		if relative == "capability-catalog.json" {
			continue
		}
		if entry, ok := entries[relative]; !ok || entry.SourceType != "inline" || entry.SHA256 == "" {
			t.Fatalf("missing dynamic Agent core entry %s: %+v", relative, entry)
		}
	}
	if _, exists := entries["capability-catalog.json"]; exists {
		t.Fatal("dynamic product-thread manifest exposed the capability catalog")
	}
	for _, relative := range []string{"knowledge/selected/overview.md", "knowledge/selected/detail.md"} {
		entry, ok := entries[relative]
		if !ok || entry.SourceType != "inline" || entry.SHA256 != dispatcherSHA256([]byte(entry.InlineContent)) {
			t.Fatalf("selected knowledge entry is not hash verified %s: %+v", relative, entry)
		}
	}
	if _, exists := entries["knowledge/unselected/hidden.md"]; exists {
		t.Fatal("unselected knowledge entered the dynamic manifest")
	}
	if entry := entries["skills/"+skillProfile+"/SKILL.md"]; entry.SourceType != "inline" || entry.InlineContent != formalSkill {
		t.Fatalf("dynamic formal Skill was not sealed inline: %+v", entry)
	}
	if entry := entries["protocols/dynamic-result.v1.md"]; entry.SourceType != "inline" || entry.InlineContent != formalProtocol {
		t.Fatalf("dynamic formal protocol was not sealed inline: %+v", entry)
	}
	for _, entry := range entries {
		if entry.SourceType == "formal_workspace_ref" {
			t.Fatalf("dynamic manifest leaked a formal Workspace reference: %+v", entry)
		}
	}
	directoryPlan := plan
	directoryPlan.SelectedKnowledgeRefs = []string{"knowledge/selected"}
	directoryFrozen := frozen
	directoryFrozen.SelectedKnowledgeRefs = []string{"knowledge/selected"}
	if _, err := provider.Build(context.Background(), run, directoryPlan, directoryFrozen, runtimepkg.RuntimeHost{}); err == nil || err.Error() != "RUNTIME_WORKSPACE_MATERIALIZATION_FAILED" {
		t.Fatalf("directory knowledge reference err=%v", err)
	}

	forged := frozen
	forged.AgentRelativeRoot = "agents/other_agent"
	if _, err := provider.Build(context.Background(), run, plan, forged, runtimepkg.RuntimeHost{}); err == nil || err.Error() != "RUNTIME_WORKSPACE_MATERIALIZATION_FAILED" {
		t.Fatalf("forged frozen Agent root err=%v", err)
	}

	tooManyRefs := make([]string, 0, dynamicKnowledgeMaxFiles+1)
	for index := 0; index <= dynamicKnowledgeMaxFiles; index++ {
		ref := fmt.Sprintf("knowledge/too-many/file-%02d.md", index)
		writeManifestFixtureFile(t, agentRoot, fmt.Sprintf("knowledge/too-many/file-%02d.md", index), "bounded\n")
		tooManyRefs = append(tooManyRefs, ref)
	}
	tooManyPlan := plan
	tooManyPlan.SelectedKnowledgeRefs = tooManyRefs
	tooManyFrozen := frozen
	tooManyFrozen.SelectedKnowledgeRefs = tooManyRefs
	if _, err := provider.Build(context.Background(), run, tooManyPlan, tooManyFrozen, runtimepkg.RuntimeHost{}); err == nil || err.Error() != "RUNTIME_WORKSPACE_MATERIALIZATION_FAILED" {
		t.Fatalf("over-limit selected knowledge err=%v", err)
	}
}

func TestFilesystemRuntimeManifestProviderDynamicCatalogRejectsLegacyPackageFallback(t *testing.T) {
	type dynamicFixture struct {
		provider         FilesystemRuntimeManifestProvider
		run              persistence.AgentRunRecord
		plan             runtimepkg.AgentRunPlan
		frozen           domain.RunWorkspaceContextRecord
		runtimeAgentRoot string
		runtimeSkillRoot string
	}
	newFixture := func(t *testing.T) dynamicFixture {
		t.Helper()
		const (
			agentProfile = "dynamic_no_fallback_agent"
			skillProfile = "dynamic_no_fallback_skill"
		)
		metaRoot := t.TempDir()
		dataRoot := t.TempDir()
		agentBody := "# Dynamic release Agent\n"
		skillBody := "# Dynamic release Skill\n"
		runtimeAgentRoot := filepath.Join(metaRoot, "runtime-agents", agentProfile)
		legacyAgentRoot := filepath.Join(metaRoot, "agents", agentProfile)
		for _, root := range []string{runtimeAgentRoot, legacyAgentRoot} {
			for _, relative := range dynamicAgentCoreFiles {
				body := "agent core " + relative + "\n"
				if relative == "AGENTS.md" {
					body = agentBody
				}
				writeManifestFixtureFile(t, root, relative, body)
			}
			writeManifestFixtureFile(t, root, "knowledge/selected/overview.md", "# Selected knowledge\n")
		}
		runtimeSkillRoot := filepath.Join(metaRoot, "runtime-skills", skillProfile)
		writeManifestFixtureFile(t, runtimeSkillRoot, "SKILL.md", skillBody)
		writeManifestFixtureFile(t, filepath.Join(metaRoot, "skills", skillProfile), "SKILL.md", skillBody)

		run := persistence.AgentRunRecord{AgentRunID: "run_dynamic_no_fallback", TenantID: "tenant_dynamic", UserID: "user_dynamic", WorkspaceID: "workspace_dynamic"}
		workspaceRoot := filepath.Join(dataRoot, "workspaces", "tenants", run.TenantID, "users", run.UserID, "workspaces", run.WorkspaceID)
		if err := os.MkdirAll(workspaceRoot, 0o750); err != nil {
			t.Fatal(err)
		}
		plan := runtimepkg.AgentRunPlan{
			SchemaVersion: "agent_run_plan.v1", AgentRunID: run.AgentRunID, PlanVersion: 1, RoutingMode: "dynamic",
			TaskType: "dynamic_no_fallback_task", ExecutionScope: "product_thread", L1AgentProfile: agentProfile,
			AgentRelativeRoot: "agents/" + agentProfile, AgentHash: dispatcherSHA256([]byte(agentBody)), ManifestVersion: "release-dynamic-no-fallback",
			SelectedSkillProfiles: []string{skillProfile}, SelectedKnowledgeRefs: []string{"knowledge/selected/overview.md"}, CapabilityHash: "capability-dynamic-no-fallback",
		}
		frozen := domain.RunWorkspaceContextRecord{
			RunID: run.AgentRunID, AgentRunID: run.AgentRunID, TenantID: run.TenantID, UserID: run.UserID, WorkspaceID: run.WorkspaceID,
			WorkspaceVersion: 1, ContextGeneration: 1, L1AgentProfile: agentProfile, AgentRelativeRoot: plan.AgentRelativeRoot,
			ManifestVersion: plan.ManifestVersion, CapabilityHash: plan.CapabilityHash, SelectedSkills: []string{skillProfile},
			SelectedKnowledgeRefs: []string{"knowledge/selected/overview.md"}, Status: "frozen",
		}
		return dynamicFixture{
			provider: FilesystemRuntimeManifestProvider{Root: metaRoot, DataRoot: dataRoot, MetaRelease: plan.ManifestVersion, SkillHashes: map[string]string{skillProfile: dispatcherSHA256([]byte(skillBody))}},
			run:      run, plan: plan, frozen: frozen, runtimeAgentRoot: runtimeAgentRoot, runtimeSkillRoot: runtimeSkillRoot,
		}
	}

	t.Run("agent package", func(t *testing.T) {
		fixture := newFixture(t)
		if err := os.RemoveAll(fixture.runtimeAgentRoot); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.provider.Build(context.Background(), fixture.run, fixture.plan, fixture.frozen, runtimepkg.RuntimeHost{}); err == nil || err.Error() != "AGENT_PROFILE_UNAVAILABLE" {
			t.Fatalf("dynamic Agent legacy fallback err=%v", err)
		}
	})

	t.Run("skill package", func(t *testing.T) {
		fixture := newFixture(t)
		if err := os.RemoveAll(fixture.runtimeSkillRoot); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.provider.Build(context.Background(), fixture.run, fixture.plan, fixture.frozen, runtimepkg.RuntimeHost{}); err == nil || err.Error() != "SKILL_UNAVAILABLE" {
			t.Fatalf("dynamic Skill legacy fallback err=%v", err)
		}
	})
}

func TestValidateRuntimeCatalogPackageClosureRejectsMissingOrMismatchedActivePackages(t *testing.T) {
	tests := []struct {
		name      string
		mutate    func(t *testing.T, root string, catalog *AgentPlanningCatalog)
		errorCode string
	}{
		{
			name: "complete package",
			mutate: func(_ *testing.T, _ string, _ *AgentPlanningCatalog) {
			},
		},
		{
			name: "missing runtime agent core file does not use legacy fallback",
			mutate: func(t *testing.T, root string, _ *AgentPlanningCatalog) {
				if err := os.Remove(filepath.Join(root, "runtime-agents", "catalog_dynamic_agent", "TOOLS.md")); err != nil {
					t.Fatal(err)
				}
				writeManifestFixtureFile(t, filepath.Join(root, "agents", "catalog_dynamic_agent"), "AGENTS.md", "# legacy fallback\n")
			},
			errorCode: "AGENT_PROFILE_UNAVAILABLE",
		},
		{
			name: "agent hash mismatch",
			mutate: func(t *testing.T, root string, _ *AgentPlanningCatalog) {
				writeManifestFixtureFile(t, filepath.Join(root, "runtime-agents", "catalog_dynamic_agent"), "AGENTS.md", "# changed agent\n")
			},
			errorCode: "AGENT_PROFILE_UNAVAILABLE",
		},
		{
			name: "capability catalog does not cover active task",
			mutate: func(t *testing.T, root string, _ *AgentPlanningCatalog) {
				writeManifestFixtureFile(t, filepath.Join(root, "runtime-agents", "catalog_dynamic_agent"), "capability-catalog.json", `{"schemaVersion":"huahuo.agent_capability_catalog.v1","agentProfile":"catalog_dynamic_agent","capabilities":[{"scene":"catalog","taskType":"other_task","skillProfile":"catalog_dynamic_skill","outputSchemaVersion":"catalog.result.v1","visibleOwner":true,"status":"active"}]}`)
			},
			errorCode: "AGENT_PROFILE_UNAVAILABLE",
		},
		{
			name: "active skill package missing",
			mutate: func(t *testing.T, root string, _ *AgentPlanningCatalog) {
				if err := os.Remove(filepath.Join(root, "runtime-skills", "catalog_dynamic_skill", "SKILL.md")); err != nil {
					t.Fatal(err)
				}
			},
			errorCode: "SKILL_UNAVAILABLE",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root, catalog := newRuntimeCatalogPackageClosureFixture(t)
			test.mutate(t, root, &catalog)
			err := ValidateRuntimeCatalogPackageClosure(root, catalog)
			if test.errorCode == "" {
				if err != nil {
					t.Fatalf("closure error = %v", err)
				}
				return
			}
			if err == nil || err.Error() != test.errorCode {
				t.Fatalf("closure error = %v, want %s", err, test.errorCode)
			}
		})
	}
}

func newRuntimeCatalogPackageClosureFixture(t *testing.T) (string, AgentPlanningCatalog) {
	t.Helper()
	const (
		agentProfile = "catalog_dynamic_agent"
		skillProfile = "catalog_dynamic_skill"
		taskType     = "catalog_dynamic_task"
	)
	root := t.TempDir()
	agentBody := "# Catalog Dynamic Agent\n"
	skillBody := "# Catalog Dynamic Skill\n"
	agentRoot := filepath.Join(root, "runtime-agents", agentProfile)
	for _, relative := range dynamicAgentCoreFiles {
		body := "runtime core " + relative + "\n"
		if relative == "AGENTS.md" {
			body = agentBody
		}
		writeManifestFixtureFile(t, agentRoot, relative, body)
	}
	writeManifestFixtureFile(t, agentRoot, "capability-catalog.json", `{"schemaVersion":"huahuo.agent_capability_catalog.v1","agentProfile":"catalog_dynamic_agent","capabilities":[{"scene":"catalog","taskType":"catalog_dynamic_task","skillProfile":"catalog_dynamic_skill","outputSchemaVersion":"catalog.result.v1","visibleOwner":true,"status":"active"}]}`)
	writeManifestFixtureFile(t, filepath.Join(root, "runtime-skills", skillProfile), "SKILL.md", skillBody)

	return root, AgentPlanningCatalog{
		Manifest: runtimepkg.L1AgentManifest{Version: "catalog-release-v1", Agents: []runtimepkg.L1AgentManifestEntry{{
			AgentProfile: agentProfile, DisplayName: "Catalog Dynamic", Status: "active", Version: "v1",
			Hash: dispatcherSHA256([]byte(agentBody)), RelativeRoot: "agents/" + agentProfile,
			IntentCategories: []string{"catalog"}, TaskTypes: []string{taskType}, CandidateSkillProfiles: []string{skillProfile},
			ToolPolicyProfile: "workspace_read_only", ExecutionScopes: []string{"product_thread"}, MaxCandidateSkills: 1,
		}}},
		Skills: []runtimepkg.PlannedSkill{{
			SkillProfile: skillProfile, Status: "active", Hash: dispatcherSHA256([]byte(skillBody)), TaskTypes: []string{taskType},
			AllowedAgentProfiles: []string{agentProfile},
		}},
	}
}

type huokeManifestFixture struct {
	metaRoot      string
	workspaceRoot string
	agentBody     string
	provider      FilesystemRuntimeManifestProvider
	run           persistence.AgentRunRecord
	plan          runtimepkg.AgentRunPlan
	frozen        domain.RunWorkspaceContextRecord
}

func newHuokeManifestFixture(t *testing.T) huokeManifestFixture {
	t.Helper()
	metaRoot := t.TempDir()
	dataRoot := t.TempDir()
	agentBody := "# Preferred Huoke Agent\n"
	skillBody := "# Preferred Huoke Topic Skill\n"
	for _, relative := range huokeTopicAgentFiles {
		body := "agent file " + relative + "\n"
		if relative == "AGENTS.md" {
			body = agentBody
		}
		writeManifestFixtureFile(t, filepath.Join(metaRoot, "runtime-agents", huokeTopicAgentProfile), relative, body)
	}
	for _, relative := range huokeTopicSkillFiles {
		body := "skill file " + relative + "\n"
		if relative == "SKILL.md" {
			body = skillBody
		}
		writeManifestFixtureFile(t, filepath.Join(metaRoot, "runtime-skills", huokeTopicSkillProfile), relative, body)
	}
	writeManifestFixtureFile(t, filepath.Join(metaRoot, "agents", huokeTopicAgentProfile), "AGENTS.md", "# Legacy Agent Must Not Win\n")
	writeManifestFixtureFile(t, filepath.Join(metaRoot, "skills", huokeTopicSkillProfile), "SKILL.md", "# Legacy Skill Must Not Win\n")

	run := persistence.AgentRunRecord{
		AgentRunID:        "run_huoke_manifest",
		TenantID:          "tenant_huoke_manifest",
		UserID:            "user_huoke_manifest",
		WorkspaceID:       "workspace_huoke_manifest",
		WorkspaceVersion:  1,
		ContextGeneration: 1,
		ThreadID:          "thread_huoke_manifest",
		TaskID:            "task_huoke_manifest",
		RequestSnapshot:   map[string]any{"input": map[string]any{"type": "text", "text": "choose a topic"}},
		IntentSnapshot:    map[string]any{"resolvedTaskType": huokeTopicTaskType},
	}
	workspaceRoot := filepath.Join(dataRoot, "workspaces", "tenants", run.TenantID, "users", run.UserID, "workspaces", run.WorkspaceID)
	if err := os.MkdirAll(workspaceRoot, 0o750); err != nil {
		t.Fatal(err)
	}
	plan := runtimepkg.AgentRunPlan{
		SchemaVersion:         "agent_run_plan.v1",
		AgentRunID:            run.AgentRunID,
		PlanVersion:           1,
		TaskType:              huokeTopicTaskType,
		ExecutionScope:        "product_thread",
		L1AgentProfile:        huokeTopicAgentProfile,
		AgentHash:             dispatcherSHA256([]byte(agentBody)),
		ManifestVersion:       "release-huoke-test",
		SelectedSkillProfiles: []string{huokeTopicSkillProfile},
		WorkspaceVersion:      1,
		IndexVersion:          0,
		CapabilityHash:        "capability-huoke-test",
	}
	frozen := testFrozenWorkspaceContext(t, run, plan, 0, 0)
	plan.WorkspaceContextManifestHash = frozen.ManifestHash
	provider := FilesystemRuntimeManifestProvider{
		Root:        metaRoot,
		DataRoot:    dataRoot,
		MetaRelease: "release-huoke-test",
		SkillHashes: map[string]string{huokeTopicSkillProfile: dispatcherSHA256([]byte(skillBody))},
	}
	return huokeManifestFixture{metaRoot: metaRoot, workspaceRoot: workspaceRoot, agentBody: agentBody, provider: provider, run: run, plan: plan, frozen: frozen}
}

func writeManifestFixtureFile(t *testing.T, root, relative, body string) {
	t.Helper()
	target := filepath.Join(root, filepath.FromSlash(relative))
	if err := os.MkdirAll(filepath.Dir(target), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(target, []byte(body), 0o640); err != nil {
		t.Fatal(err)
	}
}

func huokeManifestConsultationState() map[string]any {
	modules := map[string]any{}
	for index := 1; index <= 9; index++ {
		modules[fmt.Sprintf("WF-%02d", index)] = map[string]any{}
	}
	strategies := map[string]any{}
	for _, key := range []string{
		"extreme_test", "advantage_comparison", "sensory_desire", "offer_benefit", "selling_point_list",
		"demand_creation", "authentic_persona", "value_reframing", "element_replacement", "buyer_voice", "relationship_care",
	} {
		strategies[key] = map[string]any{}
	}
	return map[string]any{
		"schemaVersion":            runtimepkg.HuokeTopicStateSchemaVersion,
		"stateVersion":             1,
		"profileContextVersion":    "wv:1|iv:1|cg:1",
		"executionMode":            "auto_select",
		"delegationScope":          "topic_guidance_end_to_end",
		"currentSubjectId":         "strategy_fit_selection",
		"subjectStatus":            "exploring",
		"explicitUserDecision":     "delegated_end_to_end",
		"recommendedNextSubjectId": nil,
		"currentSubject":           map[string]any{"roundCount": 1, "dryReplyCount": 0, "artifactVersion": 1, "acceptedVersion": nil},
		"acquisitionContext":       map[string]any{},
		"moduleLedger":             modules,
		"strategyOverride":         nil,
		"strategyAssessmentMode":   "all_11",
		"strategyAssessments":      strategies,
		"primaryStrategy":          nil,
		"secondaryStrategy":        nil,
		"selectionReason":          nil,
		"strategyBrief":            map[string]any{},
		"evidenceValidation":       map[string]any{},
		"topicCandidates":          map[string]any{},
		"leadingTopicCandidateId":  nil,
		"candidateComparison":      map[string]any{},
		"finalRecommendation":      nil,
		"claims":                   map[string]any{},
		"sideClues":                map[string]any{},
		"invalidations":            map[string]any{},
		"resumePoint":              nil,
	}
}

func manifestEntriesByLogicalPath(t *testing.T, entries []runtimepkg.RuntimeManifestEntry) map[string]runtimepkg.RuntimeManifestEntry {
	t.Helper()
	result := map[string]runtimepkg.RuntimeManifestEntry{}
	for _, entry := range entries {
		if _, exists := result[entry.LogicalPath]; exists {
			t.Fatalf("duplicate logical path %s", entry.LogicalPath)
		}
		result[entry.LogicalPath] = entry
	}
	return result
}

type huokeFixtureSourceResolver struct {
	root string
}

func (r huokeFixtureSourceResolver) Resolve(_ context.Context, _ runtimepkg.RuntimeInputManifest, entry runtimepkg.RuntimeManifestEntry) ([]byte, error) {
	if entry.SourceType != "formal_workspace_ref" {
		return nil, fmt.Errorf("unexpected source type %s", entry.SourceType)
	}
	return os.ReadFile(filepath.Join(r.root, filepath.FromSlash(entry.SourceRef)))
}

func assertHuokeManifestMaterializes(t *testing.T, fixture huokeManifestFixture, inputs runtimepkg.RuntimeManifestBuildInputs) {
	t.Helper()
	now := time.Now().UTC().Truncate(time.Second)
	inputs.RuntimeHostID = "host-huoke-test"
	inputs.ExpiresAt = now.Add(10 * time.Minute)
	composer := runtimepkg.NewWorkspaceComposer()
	composer.Now = func() time.Time { return now }
	manifest, err := composer.BuildInputManifest(context.Background(), fixture.plan, fixture.frozen, inputs)
	if err != nil {
		t.Fatalf("compose Huoke manifest: %v", err)
	}
	secret := "huoke-manifest-test-ticket-secret"
	ticket, err := runtimepkg.SignRunTicket(runtimepkg.RunTicketClaims{
		RunID:             manifest.RunID,
		TenantID:          manifest.TenantID,
		ReservationID:     "reservation-huoke-test",
		RuntimeHostID:     manifest.RuntimeHostID,
		CapabilityHash:    manifest.CapabilityHash,
		WorkspaceID:       manifest.WorkspaceID,
		WorkspaceVersion:  manifest.WorkspaceVersion,
		ContextGeneration: manifest.ContextGeneration,
		InputManifestHash: manifest.ManifestHash,
		PlanHash:          runtimepkg.RunTicketJTIHash("huoke-manifest-plan"),
		FencingToken:      1,
		JTI:               "jti-huoke-test",
		IssuedAt:          now.Unix(),
		ExpiresAt:         manifest.ExpiresAt.Unix(),
	}, secret)
	if err != nil {
		t.Fatal(err)
	}
	materializer := runtimepkg.NewRuntimeWorkspaceMaterializer(
		manifest.RuntimeHostID,
		t.TempDir(),
		secret,
		huokeFixtureSourceResolver{root: fixture.workspaceRoot},
		runtimepkg.NewMemoryRunTicketJTIStore(),
	)
	materializer.Now = func() time.Time { return now }
	workspace, err := materializer.Materialize(context.Background(), ticket, manifest)
	if err != nil {
		t.Fatalf("materialize Huoke manifest: %v", err)
	}
	declared := manifestEntriesByLogicalPath(t, manifest.Files)
	for _, relative := range []string{
		"AGENTS.md",
		"SOUL.md",
		"skills/huoke_topic_strategy/references/strategy-fit-contracts.md",
		"profile/user-positioning/positioning-profile.md",
		"materials/raw/material-00.md",
	} {
		if _, ok := declared[relative]; !ok {
			continue
		}
		info, statErr := os.Stat(filepath.Join(workspace.Root, filepath.FromSlash(relative)))
		if statErr != nil || !info.Mode().IsRegular() {
			t.Fatalf("materialized file %s: info=%v err=%v", relative, info, statErr)
		}
	}
	if _, err := os.Stat(filepath.Join(fixture.workspaceRoot, "output")); !os.IsNotExist(err) {
		t.Fatalf("materialization wrote into formal Workspace: %v", err)
	}
}
