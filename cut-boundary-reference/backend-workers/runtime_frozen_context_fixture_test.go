package workers

import (
	"encoding/json"
	"sort"
	"testing"

	"huahuoai/backend/source/internal/domain"
	"huahuoai/backend/source/internal/persistence"
	runtimepkg "huahuoai/backend/source/internal/runtime"
	workspacepkg "huahuoai/backend/source/internal/workspace"
)

// testFrozenWorkspaceContext mirrors ViewService.FreezeForRun. Test fixtures
// must use the same canonical manifest/hash as a durable production freeze.
func testFrozenWorkspaceContext(t *testing.T, run persistence.AgentRunRecord, plan runtimepkg.AgentRunPlan, indexVersion int64, sessionGeneration int) domain.RunWorkspaceContextRecord {
	t.Helper()
	if plan.AgentRunID != run.AgentRunID {
		t.Fatalf("plan/run mismatch: plan=%q run=%q", plan.AgentRunID, run.AgentRunID)
	}
	readRoots := workspacepkg.DefaultRunWorkspaceReadRoots()
	mount, err := runtimepkg.RuntimeWorkspaceMountForPlan(plan)
	if err != nil {
		t.Fatalf("runtime workspace mount: %v", err)
	}
	writeRoots := mount.AllowedWriteRoots
	requiredTools := append([]string{}, plan.RequiredTools...)
	sort.Strings(requiredTools)
	selectedSkills := append([]string(nil), plan.SelectedSkillProfiles...)
	normalizedKnowledge := append([]string(nil), plan.SelectedKnowledgeRefs...)
	sort.Strings(normalizedKnowledge)
	selectedKnowledge := append([]string(nil), normalizedKnowledge...)
	manifest := map[string]any{
		"workspaceId":                   run.WorkspaceID,
		"workspaceVersion":              run.WorkspaceVersion,
		"indexVersion":                  indexVersion,
		"threadId":                      run.ThreadID,
		"threadWorkspaceBindingVersion": run.BindingVersion,
		"contextGeneration":             run.ContextGeneration,
		"sessionGeneration":             sessionGeneration,
		"l1AgentProfile":                plan.L1AgentProfile,
		"agentRelativeRoot":             plan.AgentRelativeRoot,
		"manifestVersion":               plan.ManifestVersion,
		"capabilityHash":                plan.CapabilityHash,
		"allowedReadRoots":              readRoots,
		"allowedWriteRoots":             writeRoots,
		"requiredTools":                 requiredTools,
		"selectedSkills":                selectedSkills,
		"selectedKnowledgeRefs":         selectedKnowledge,
	}
	raw, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	return domain.RunWorkspaceContextRecord{
		RunID: run.AgentRunID, AgentRunID: run.AgentRunID,
		TenantID: run.TenantID, UserID: run.UserID, WorkspaceID: run.WorkspaceID,
		WorkspaceVersion: run.WorkspaceVersion, IndexVersion: indexVersion,
		ThreadID: run.ThreadID, ThreadWorkspaceBindingVersion: run.BindingVersion,
		ContextGeneration: run.ContextGeneration, SessionGeneration: sessionGeneration,
		L1AgentProfile: plan.L1AgentProfile, AgentRelativeRoot: plan.AgentRelativeRoot,
		ManifestVersion: plan.ManifestVersion, CapabilityHash: plan.CapabilityHash,
		AllowedReadRoots: readRoots, AllowedWriteRoots: writeRoots,
		SelectedSkills: selectedSkills, SelectedKnowledgeRefs: selectedKnowledge,
		ContextManifest: manifest, ManifestHash: runtimepkg.RunTicketJTIHash(string(raw)), Status: "frozen",
	}
}
