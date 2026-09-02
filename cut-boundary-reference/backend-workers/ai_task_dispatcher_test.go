package workers

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"huahuoai/backend/source/internal/domain"
	"huahuoai/backend/source/internal/persistence"
	storageprovider "huahuoai/backend/source/internal/providers/storage"
	"huahuoai/backend/source/internal/queue"
	runtimepkg "huahuoai/backend/source/internal/runtime"
)

func TestNewAITaskDispatcherKeepsRuntimeConfigVersionDiagnosticOnly(t *testing.T) {
	dispatcher := NewAITaskDispatcherWithCapabilityReader(nil, nil, nil, nil, nil, "", "", "v1", "worker-test", nil)
	if dispatcher.RuntimeConfigVersion != "v1" {
		t.Fatalf("runtime config version = %q, want v1", dispatcher.RuntimeConfigVersion)
	}
	if _, err := dispatcher.runtimeConfigVersionForPlan("huahuo-default"); err == nil || err.Error() != "RUNTIME_CONFIG_VERSION_INVALID" {
		t.Fatalf("dispatcher must not use the diagnostic version as a dispatch fallback, err=%v", err)
	}
}

func TestAITaskDispatcherUsesFrozenRuntimeConfigIDToResolveVersion(t *testing.T) {
	dispatcher := NewAITaskDispatcherWithCapabilityReader(nil, nil, nil, nil, nil, "", "", "v1", "worker-test", nil)
	dispatcher.RuntimeConfigVersions = runtimepkg.RuntimeConfigVersions{
		"huahuo-default":          "v1",
		"huahuo-faya-germination": "v3",
	}
	if got, err := dispatcher.runtimeConfigVersionForPlan("huahuo-faya-germination"); err != nil || got != "v3" {
		t.Fatalf("faya config version got=%q err=%v", got, err)
	}
	if _, err := dispatcher.runtimeConfigVersionForPlan("huahuo-missing"); err == nil || err.Error() != "RUNTIME_CONFIG_VERSION_INVALID" {
		t.Fatalf("missing config version error=%v", err)
	}
}

func TestFinalizeCapturedSubmitPlanFailsFrozenPlan(t *testing.T) {
	ctx := context.Background()
	repos := persistence.NewRepositories(nil)
	run := persistence.AgentRunRecord{
		AgentRunID: "run_dispatch_capture_plan", TenantID: "tenant_dispatch_capture_plan", UserID: "user_dispatch_capture_plan", WorkspaceID: "workspace_dispatch_capture_plan",
		IdempotencyKey: "idem_dispatch_capture_plan", RequestHash: "request_hash", Status: "planning", RoutingMode: "dynamic", SourceSurface: "runtime_v1_e2e_submit_binding_fixture",
	}
	if _, _, err := repos.AgentRuns.CreateRun(ctx, persistence.CreateAgentRunCommand{Record: run}); err != nil {
		t.Fatal(err)
	}
	if err := repos.AgentRuns.SavePlan(ctx, persistence.AgentRunPlanRecord{
		AgentRunID: run.AgentRunID, PlanVersion: 1, PlanStatus: "validated", AgentRunStatus: "queued", Plan: map[string]any{"status": "validated"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := finalizeCapturedSubmitPlan(ctx, repos.AgentRuns, run.AgentRunID, 1, map[string]any{"status": "validated"}); err != nil {
		t.Fatalf("finalize captured plan: %v", err)
	}
	plan, err := repos.AgentRuns.GetPlan(ctx, run.AgentRunID, 1)
	if err != nil || plan["status"] != "failed" {
		t.Fatalf("plan=%#v err=%v", plan, err)
	}
	if err := finalizeCapturedSubmitPlan(ctx, repos.AgentRuns, run.AgentRunID, 1, map[string]any{"status": "validated"}); err == nil {
		t.Fatal("captured plan finalization must reject an already-terminal plan")
	}
}

func TestFilesystemRuntimeManifestProviderAddsMaterialFilesByTrustedReference(t *testing.T) {
	metaRoot, dataRoot := t.TempDir(), t.TempDir()
	agentBody := []byte("# Recording Agent\n")
	skillBody := []byte("# Summary Skill\n")
	for path, body := range map[string][]byte{
		filepath.Join(metaRoot, "agents", "recording_postprocess_agent", "AGENTS.md"): agentBody,
		filepath.Join(metaRoot, "skills", "asset_summary", "SKILL.md"):                skillBody,
	} {
		if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, body, 0o640); err != nil {
			t.Fatal(err)
		}
	}
	materialID := "mat_manual_manifest"
	workspaceRoot := filepath.Join(dataRoot, "workspaces", "tenants", "tenant_manifest", "users", "user_manifest", "workspaces", "workspace_manifest")
	for rel, body := range map[string]string{
		"materials/raw/" + materialID + ".md":               "# Source\n\nFact.",
		"materials/processed/" + materialID + ".minutes.md": "# Minutes\n\nDecision.",
	} {
		path := filepath.Join(workspaceRoot, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0o640); err != nil {
			t.Fatal(err)
		}
	}
	plan := runtimepkg.AgentRunPlan{TaskType: "summary_generation", L1AgentProfile: "recording_postprocess_agent", AgentHash: dispatcherSHA256(agentBody), SelectedSkillProfiles: []string{"asset_summary"}}
	provider := FilesystemRuntimeManifestProvider{Root: metaRoot, DataRoot: dataRoot, MetaRelease: "release-test", SkillHashes: map[string]string{"asset_summary": dispatcherSHA256(skillBody)}}
	inputs, err := provider.Build(context.Background(), persistence.AgentRunRecord{
		AgentRunID: "run_material_manifest", TenantID: "tenant_manifest", UserID: "user_manifest", WorkspaceID: "workspace_manifest",
		RequestSnapshot: map[string]any{"businessRefs": map[string]any{"materialId": materialID}},
	}, plan, domain.RunWorkspaceContextRecord{}, runtimepkg.RuntimeHost{})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"input/transcript.md": "materials/raw/" + materialID + ".md", "input/minutes.md": "materials/processed/" + materialID + ".minutes.md"}
	for _, entry := range inputs.Files {
		if expected, ok := want[entry.LogicalPath]; ok {
			if entry.SourceType != "formal_workspace_ref" || entry.SourceRef != expected || entry.InlineContent != "" || entry.SizeBytes < 1 || entry.SHA256 == "" {
				t.Fatalf("material manifest entry=%+v", entry)
			}
			delete(want, entry.LogicalPath)
		}
	}
	if len(want) != 0 {
		t.Fatalf("missing material manifest entries: %#v", want)
	}
}

func TestFilesystemRuntimeManifestProviderRevalidatesVisualAttachmentByServerMetaWorkspaceKey(t *testing.T) {
	repos := persistence.NewRepositories(nil)
	const (
		tenantID    = "tenant_manifest_attachment"
		userID      = "user_manifest_attachment"
		workspaceID = "workspace_manifest_attachment"
		resourceID  = "resource_manifest_attachment"
		policyHash  = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		hash        = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	)
	metadata := map[string]any{
		"tenantId": tenantID, "mimeType": "image/png", "sizeBytes": int64(128), "sha256": hash,
		"width": 4, "height": 3, "metaWorkspaceKey": "visual_chat", "metaWorkspaceVersion": "v1", "inputPolicyHash": policyHash, "inputUsage": "primary_input",
	}
	repos.Media.CreateResourceIndex(resourceID, userID, workspaceID, "oss://local/media/resources/attachment.png", metadata)
	provider := FilesystemRuntimeManifestProvider{Resources: repos.Media}
	run := persistence.AgentRunRecord{TenantID: tenantID, UserID: userID, WorkspaceID: workspaceID}
	plan := runtimepkg.AgentRunPlan{
		MetaWorkspaceKey: "visual_chat", MetaWorkspaceVersion: "v1", InputPolicyHash: policyHash,
		InputAttachments: []runtimepkg.AgentRunInputAttachmentIdentity{{
			ResourceID: resourceID, Usage: "primary_input", MIMEType: "image/png", SizeBytes: 128, SHA256: hash,
			Width: 4, Height: 3, LogicalPath: "input/attachments/01.png",
		}},
	}
	entries, err := provider.attachmentInputEntries(run, plan)
	if err != nil || len(entries) != 2 {
		t.Fatalf("attachment entries=%#v err=%v", entries, err)
	}
	if entry := entries[0]; entry.LogicalPath != "input/attachments/01.png" || entry.SourceType != "object_ref" || entry.SourceRef != "media/resources/attachment.png" || entry.SHA256 != hash || entry.ObjectRead != nil {
		t.Fatalf("attachment object reference=%+v", entry)
	}
	if entry := entries[1]; entry.LogicalPath != "input/attachments.json" || entry.SourceType != "inline" || strings.Contains(entry.InlineContent, "oss://") {
		t.Fatalf("public attachment manifest=%+v", entry)
	}

	delete(metadata, "metaWorkspaceKey")
	metadata["expectedMetaWorkspaceKey"] = "visual_chat"
	repos.Media.CreateResourceIndex(resourceID, userID, workspaceID, "oss://local/media/resources/attachment.png", metadata)
	if _, err := provider.attachmentInputEntries(run, plan); err == nil || err.Error() != "RUNTIME_WORKSPACE_MATERIALIZATION_FAILED" {
		t.Fatalf("legacy request key must not satisfy dispatch attachment identity, err=%v", err)
	}

	metadata["metaWorkspaceKey"] = "visual_chat"
	metadata["metaWorkspaceVersion"] = "v0"
	delete(metadata, "expectedMetaWorkspaceKey")
	repos.Media.CreateResourceIndex(resourceID, userID, workspaceID, "oss://local/media/resources/attachment.png", metadata)
	if _, err := provider.attachmentInputEntries(run, plan); err == nil || err.Error() != "RUNTIME_WORKSPACE_MATERIALIZATION_FAILED" {
		t.Fatalf("stale Meta Workspace version must not satisfy dispatch attachment identity, err=%v", err)
	}
}

func TestFilesystemRuntimeManifestProviderRejectsVisualAttachmentFrozenFieldDrift(t *testing.T) {
	repos := persistence.NewRepositories(nil)
	const (
		tenantID    = "tenant_manifest_attachment_drift"
		userID      = "user_manifest_attachment_drift"
		workspaceID = "workspace_manifest_attachment_drift"
		resourceID  = "resource_manifest_attachment_drift"
		policyHash  = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		hash        = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	)
	provider := FilesystemRuntimeManifestProvider{Resources: repos.Media}
	run := persistence.AgentRunRecord{TenantID: tenantID, UserID: userID, WorkspaceID: workspaceID}
	plan := runtimepkg.AgentRunPlan{
		MetaWorkspaceKey: "visual_chat", MetaWorkspaceVersion: "v1", InputPolicyHash: policyHash,
		InputAttachments: []runtimepkg.AgentRunInputAttachmentIdentity{{
			ResourceID: resourceID, Usage: "primary_input", MIMEType: "image/png", SizeBytes: 128, SHA256: hash,
			Width: 4, Height: 3, LogicalPath: "input/attachments/01.png",
		}},
	}
	baseMetadata := func() map[string]any {
		return map[string]any{
			"tenantId": tenantID, "mimeType": "image/png", "sizeBytes": int64(128), "sha256": hash,
			"width": 4, "height": 3, "metaWorkspaceKey": "visual_chat", "metaWorkspaceVersion": "v1", "inputPolicyHash": policyHash, "inputUsage": "primary_input",
		}
	}
	for name, mutate := range map[string]func(map[string]any){
		"input_policy_hash": func(metadata map[string]any) {
			metadata["inputPolicyHash"] = "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
		},
		"input_usage": func(metadata map[string]any) { metadata["inputUsage"] = "secondary_input" },
		"mime_type":   func(metadata map[string]any) { metadata["mimeType"] = "IMAGE/JPEG" },
		"sha256": func(metadata map[string]any) {
			metadata["sha256"] = "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
		},
		"size_bytes": func(metadata map[string]any) { metadata["sizeBytes"] = int64(129) },
		"width":      func(metadata map[string]any) { metadata["width"] = 5 },
		"height":     func(metadata map[string]any) { metadata["height"] = 4 },
	} {
		t.Run(name, func(t *testing.T) {
			metadata := baseMetadata()
			mutate(metadata)
			repos.Media.CreateResourceIndex(resourceID, userID, workspaceID, "oss://local/media/resources/attachment.png", metadata)
			if _, err := provider.attachmentInputEntries(run, plan); err == nil || err.Error() != "RUNTIME_WORKSPACE_MATERIALIZATION_FAILED" {
				t.Fatalf("frozen visual attachment drift=%s error=%v", name, err)
			}
		})
	}
}

type signedAttachmentReadIssuer struct {
	url string
	ref storageprovider.StorageRef
	ttl time.Duration
	err error
}

func (i *signedAttachmentReadIssuer) CreateRuntimeAttachmentReadURL(ref storageprovider.StorageRef, ttl time.Duration) (string, error) {
	i.ref = ref
	i.ttl = ttl
	return i.url, i.err
}

func TestFilesystemRuntimeManifestProviderIssuesPrivateRemoteReadReferenceForAliyunAttachment(t *testing.T) {
	repos := persistence.NewRepositories(nil)
	const (
		tenantID    = "tenant_remote_attachment"
		userID      = "user_remote_attachment"
		workspaceID = "workspace_remote_attachment"
		resourceID  = "resource_remote_attachment"
		policyHash  = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		hash        = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	)
	repos.Media.CreateResourceIndex(resourceID, userID, workspaceID, "oss://userworkspace/media/resources/remote.png", map[string]any{
		"tenantId": tenantID, "mimeType": "image/png", "sizeBytes": int64(128), "sha256": hash,
		"width": 4, "height": 3, "metaWorkspaceKey": "visual_chat", "metaWorkspaceVersion": "v1", "inputPolicyHash": policyHash, "inputUsage": "primary_input",
	})
	now := time.Date(2026, 7, 18, 10, 0, 0, 0, time.UTC)
	issuer := &signedAttachmentReadIssuer{url: "https://oss.example.test/media/resources/remote.png?Expires=1&Signature=opaque"}
	provider := FilesystemRuntimeManifestProvider{Resources: repos.Media, AttachmentReadIssuer: issuer, Now: func() time.Time { return now }}
	plan := runtimepkg.AgentRunPlan{
		MetaWorkspaceKey: "visual_chat", MetaWorkspaceVersion: "v1", InputPolicyHash: policyHash,
		InputAttachments: []runtimepkg.AgentRunInputAttachmentIdentity{{
			ResourceID: resourceID, Usage: "primary_input", MIMEType: "image/png", SizeBytes: 128, SHA256: hash,
			Width: 4, Height: 3, LogicalPath: "input/attachments/01.png",
		}},
	}
	entries, err := provider.attachmentInputEntriesWithExpiry(persistence.AgentRunRecord{TenantID: tenantID, UserID: userID, WorkspaceID: workspaceID}, plan, now.Add(10*time.Minute))
	if err != nil || len(entries) != 2 {
		t.Fatalf("remote attachment entries=%#v err=%v", entries, err)
	}
	entry := entries[0]
	if issuer.ref != (storageprovider.StorageRef{Bucket: "userworkspace", Key: "media/resources/remote.png"}) || issuer.ttl != 9*time.Minute+55*time.Second {
		t.Fatalf("signed issuer binding ref=%+v ttl=%s", issuer.ref, issuer.ttl)
	}
	if entry.SourceRef != "runtime-attachments/"+strings.TrimPrefix(hash, "sha256:") || entry.ObjectRead == nil || entry.ObjectRead.URL != issuer.url ||
		!entry.ObjectRead.ExpiresAt.Equal(now.Add(9*time.Minute+55*time.Second)) || entry.ObjectRead.MIMEType != "image/png" {
		t.Fatalf("private object reference=%+v", entry)
	}
	// The model-visible attachment descriptor retains identity/metadata only;
	// neither the signed read URL nor provider object path may enter it.
	if strings.Contains(entries[1].InlineContent, issuer.url) || strings.Contains(entries[1].InlineContent, "media/resources/remote.png") {
		t.Fatalf("public attachment manifest leaked read capability: %s", entries[1].InlineContent)
	}
}

type fixedDispatcherManifestProvider struct{}

func (fixedDispatcherManifestProvider) Build(_ context.Context, _ persistence.AgentRunRecord, plan runtimepkg.AgentRunPlan, _ domain.RunWorkspaceContextRecord, _ runtimepkg.RuntimeHost) (runtimepkg.RuntimeManifestBuildInputs, error) {
	return runtimepkg.RuntimeManifestBuildInputs{
		MetaRelease: "release-v5", AgentHash: plan.AgentHash,
		SkillHashes: map[string]string{"general_chat": "sha256:" + strings.Repeat("b", 64)},
		Files:       []runtimepkg.RuntimeManifestEntry{runtimepkg.NewInlineRuntimeEntry("input/request.json", []byte(`{"input":"hello"}`))},
		ExpiresAt:   time.Now().UTC().Add(5 * time.Minute),
	}, nil
}

type workspaceVersionAdvancingManifestProvider struct {
	advance    func() error
	buildCalls int
}

func (p *workspaceVersionAdvancingManifestProvider) Build(ctx context.Context, run persistence.AgentRunRecord, plan runtimepkg.AgentRunPlan, frozen domain.RunWorkspaceContextRecord, host runtimepkg.RuntimeHost) (runtimepkg.RuntimeManifestBuildInputs, error) {
	p.buildCalls++
	inputs, err := fixedDispatcherManifestProvider{}.Build(ctx, run, plan, frozen, host)
	if err != nil {
		return runtimepkg.RuntimeManifestBuildInputs{}, err
	}
	// This is the point at which the filesystem provider has observed W1. A
	// concurrent Workspace write commits W2 before Dispatcher may sign/submit.
	if p.advance == nil {
		return runtimepkg.RuntimeManifestBuildInputs{}, errors.New("workspace version advance hook missing")
	}
	if err := p.advance(); err != nil {
		return runtimepkg.RuntimeManifestBuildInputs{}, err
	}
	return inputs, nil
}

type countedDispatcherManifestProvider struct {
	buildCalls int
}

func (p *countedDispatcherManifestProvider) Build(_ context.Context, _ persistence.AgentRunRecord, _ runtimepkg.AgentRunPlan, _ domain.RunWorkspaceContextRecord, _ runtimepkg.RuntimeHost) (runtimepkg.RuntimeManifestBuildInputs, error) {
	p.buildCalls++
	return runtimepkg.RuntimeManifestBuildInputs{}, errors.New("manifest build must not run after submit-binding rejection")
}

type fakeAsyncRuntimeClient struct {
	submitted   runtimepkg.AsyncRuntimeSubmitRequest
	submitCalls int
	submitCheck func() error
	aborted     runtimepkg.AsyncRuntimeAbortRequest
	abortResult runtimepkg.AsyncRuntimeAbortResult
	abortErr    error
	events      runtimepkg.AsyncRuntimeEventPage
	status      runtimepkg.AsyncRuntimeStatus
	statusErr   error
}

func (f *fakeAsyncRuntimeClient) Submit(_ context.Context, _ runtimepkg.RuntimeHost, request runtimepkg.AsyncRuntimeSubmitRequest) (runtimepkg.AsyncRuntimeSubmitResult, error) {
	f.submitCalls++
	if f.submitCheck != nil {
		if err := f.submitCheck(); err != nil {
			return runtimepkg.AsyncRuntimeSubmitResult{}, err
		}
	}
	f.submitted = request
	return runtimepkg.AsyncRuntimeSubmitResult{RunID: request.RunID, Status: "accepted", RuntimeRequestID: "runtime_request_1", AcceptedSequence: 1}, nil
}

type dispatcherCapabilityReaderFunc func(context.Context, runtimepkg.RuntimeHost) (runtimepkg.RuntimeCapabilities, error)

func (f dispatcherCapabilityReaderFunc) GetCapabilities(ctx context.Context, host runtimepkg.RuntimeHost) (runtimepkg.RuntimeCapabilities, error) {
	return f(ctx, host)
}

func (f dispatcherCapabilityReaderFunc) GetFreshCapabilities(ctx context.Context, host runtimepkg.RuntimeHost) (runtimepkg.RuntimeCapabilities, error) {
	return f(ctx, host)
}

type legacyDispatcherCapabilityReaderFunc func(context.Context, runtimepkg.RuntimeHost) (runtimepkg.RuntimeCapabilities, error)

func (f legacyDispatcherCapabilityReaderFunc) GetCapabilities(ctx context.Context, host runtimepkg.RuntimeHost) (runtimepkg.RuntimeCapabilities, error) {
	return f(ctx, host)
}

func dispatcherRuntimeCapabilities(snapshot runtimepkg.RuntimeCapabilitySnapshot) runtimepkg.RuntimeCapabilities {
	if snapshot.BudgetExecution.EnforcementVersion == "" {
		snapshot.BudgetExecution = runtimepkg.DefaultRuntimeToolBudgetExecutionContract()
	}
	return runtimepkg.RuntimeCapabilities{
		RuntimeVersion: "runtime-v1", AdapterVersion: "adapter-v1", CapabilityHash: snapshot.CapabilityHash,
		Tools: append([]runtimepkg.ToolCapability(nil), snapshot.Tools...),
		FilesystemPolicy: runtimepkg.RuntimeFilesystemPolicy{
			WorkspaceOnlyReady: true, AbsolutePathRejected: true, SymlinkEscapeRejected: true,
		},
		Abort:         runtimepkg.RuntimeAbortCapability{Supported: true, AuthorizationReady: true},
		SubmitBinding: snapshot.SubmitBinding,
		BudgetCapabilities: runtimepkg.RuntimeBudgetCapabilities{
			MaxToolCallsSupported: snapshot.MaxToolCallsSupported, DefaultMaxToolCalls: 200,
			SupportsPerRunBudget: snapshot.SupportsPerRunBudget, SupportsBudgetWarning: snapshot.SupportsBudgetWarning,
			SupportsForcedAbort: snapshot.SupportsForcedAbort, ExecutionContract: snapshot.BudgetExecution,
		},
	}
}
func (f *fakeAsyncRuntimeClient) GetStatus(context.Context, runtimepkg.RuntimeHost, string, string) (runtimepkg.AsyncRuntimeStatus, error) {
	return f.status, f.statusErr
}
func (f *fakeAsyncRuntimeClient) ListEvents(context.Context, runtimepkg.RuntimeHost, string, string, int64, int, int) (runtimepkg.AsyncRuntimeEventPage, error) {
	return f.events, nil
}
func (f *fakeAsyncRuntimeClient) AbortAsync(_ context.Context, _ runtimepkg.RuntimeHost, request runtimepkg.AsyncRuntimeAbortRequest) (runtimepkg.AsyncRuntimeAbortResult, error) {
	f.aborted = request
	return f.abortResult, f.abortErr
}

func TestRuntimeInputMessageForPlanUsesWorkspaceOwnedRensheContentPrompt(t *testing.T) {
	request := "有根据我的人设写吗？还是自由发挥的？然后你建议哪个方向更适合我？"
	got := runtimeInputMessageForPlan(runtimepkg.AgentRunPlan{TaskType: "work_ai_renshe_content"}, runtimepkg.RuntimeInputManifest{}, request)
	want := request + "\n\nUse the selected Workspace skill and rendered Workspace context."
	if got != want {
		t.Fatalf("renshe runtime message = %q, want %q", got, want)
	}

	general := runtimeInputMessageForPlan(runtimepkg.AgentRunPlan{TaskType: "work_ai_general_chat"}, runtimepkg.RuntimeInputManifest{}, request)
	if general != request {
		t.Fatalf("non-renshe runtime message changed: %s", general)
	}
}

func TestRuntimeInputMessageForPlanUsesWorkspaceOwnedFayaGerminationPrompt(t *testing.T) {
	request := "请把这份会议纪要和我的已确认画像碰撞，长出新的短视频观点原料。"
	got := runtimeInputMessageForPlan(runtimepkg.AgentRunPlan{TaskType: "work_ai_faya_germination"}, runtimepkg.RuntimeInputManifest{}, request)
	want := request + "\n\nUse the selected Workspace skill and rendered Workspace context."
	if got != want {
		t.Fatalf("faya runtime message = %q, want %q", got, want)
	}
}

func TestRuntimeInputMessageForPlanUsesWorkspaceOwnedHuokeContentPrompt(t *testing.T) {
	request := "帮我把健身体验营做成一条可拍的获客视频。"
	got := runtimeInputMessageForPlan(runtimepkg.AgentRunPlan{TaskType: "work_ai_huoke_content"}, runtimepkg.RuntimeInputManifest{}, request)
	want := request + "\n\nUse the selected Workspace skill and rendered Workspace context."
	if got != want {
		t.Fatalf("huoke content runtime message = %q, want %q", got, want)
	}
	for _, retired := range []string{"Huoke content identity:", "Product discovery boundary:", "Huoke quoted evidence draft."} {
		if strings.Contains(got, retired) {
			t.Fatalf("huoke content runtime message retained retired inline workflow %q: %s", retired, got)
		}
	}
}

func TestRuntimeInputMessageForPlanUsesWorkspaceOwnedHuokeStateInputPrompt(t *testing.T) {
	request := "这是一个全新的业务，我还没有说明卖什么。"
	plan := runtimepkg.AgentRunPlan{TaskType: "work_ai_huoke_topic_strategy", WorkspaceVersion: 7, IndexVersion: 3}
	manifest := runtimepkg.RuntimeInputManifest{ContextGeneration: 4}
	got := runtimeInputMessageForPlan(plan, manifest, request)
	want := request + "\n\nUse the selected Workspace skill and input/consultation_state.json."
	if got != want {
		t.Fatalf("huoke runtime message = %q, want %q", got, want)
	}
}

func TestRuntimeInputMessageForPlanKeepsHuokePromptIndependentOfStateMetadata(t *testing.T) {
	plan := runtimepkg.AgentRunPlan{TaskType: "work_ai_huoke_topic_strategy", WorkspaceVersion: 9, IndexVersion: 5}
	manifest := runtimepkg.RuntimeInputManifest{
		ContextGeneration: 6,
		Files:             []runtimepkg.RuntimeManifestEntry{{LogicalPath: "input/consultation_state.json"}},
	}
	request := "继续完成选题判断"
	got := runtimeInputMessageForPlan(plan, manifest, request)
	want := request + "\n\nUse the selected Workspace skill and input/consultation_state.json."
	if got != want {
		t.Fatalf("huoke runtime message with state = %q, want %q", got, want)
	}
}

func TestRuntimeConfigIDForPlanRequiresFrozenPlanRuntimeConfig(t *testing.T) {
	plan := runtimepkg.AgentRunPlan{
		TaskType: "work_ai_self_media_creation", RuntimeConfigID: "huahuo-self-media-creation",
		L1AgentProfile: "self_media_creation_agent", SelectedSkillProfiles: []string{"self_media_creation_advisor"},
	}
	got, err := runtimeConfigIDForPlan(plan)
	if err != nil || got != "huahuo-self-media-creation" {
		t.Fatalf("dedicated runtime config=%q err=%v", got, err)
	}
	for _, invalid := range []runtimepkg.AgentRunPlan{
		{TaskType: "work_ai_self_media_creation"},
		{TaskType: "work_ai_self_media_creation", RuntimeConfigID: " huahuo-self-media-creation"},
	} {
		if _, err := runtimeConfigIDForPlan(invalid); err == nil || !strings.Contains(err.Error(), "AGENT_PLAN_INVALID") {
			t.Fatalf("missing or noncanonical frozen runtime config must fail closed: plan=%+v err=%v", invalid, err)
		}
	}
	identity := dispatchExecutionIdentity(plan, got)
	if identity["taskType"] != "work_ai_self_media_creation" || identity["agentProfile"] != "self_media_creation_agent" || identity["runtimeConfigId"] != "huahuo-self-media-creation" {
		t.Fatalf("execution identity=%#v", identity)
	}
}

func TestAITaskDispatcherTerminalFailureProjectsProductTask(t *testing.T) {
	ctx := context.Background()
	repos := persistence.NewRepositories(nil)
	const (
		runID  = "run_dispatch_terminal_failure"
		taskID = "task_dispatch_terminal_failure"
	)
	repos.ChatTasks.CreateAiTask(taskID, "work_ai_faya_germination", "user_dispatch_failure", "workspace_dispatch_failure", map[string]any{})
	if _, _, err := repos.AgentRuns.CreateRun(ctx, persistence.CreateAgentRunCommand{Record: persistence.AgentRunRecord{
		AgentRunID: runID, TaskID: taskID, TenantID: "tenant_dispatch_failure", UserID: "user_dispatch_failure",
		WorkspaceID: "workspace_dispatch_failure", WorkspaceVersion: 1, BindingVersion: 1, ContextGeneration: 1,
		IdempotencyKey: "idem_dispatch_terminal_failure", RequestHash: "request_hash", Status: "queued",
	}}); err != nil {
		t.Fatal(err)
	}
	repos.Queue.Enqueue(map[string]any{
		"queueId": "queue_dispatch_terminal_failure", "queueName": queue.QueueAIRuntimeInteractive,
		"taskType": "runtime_dispatch", "taskId": runID,
	})
	_, proof, err := repos.Queue.Lease(ctx, queue.QueueAIRuntimeInteractive, "dispatcher-terminal-failure", time.Minute, "runtime_dispatch")
	if err != nil {
		t.Fatal(err)
	}
	dispatcher := AITaskDispatcher{Repos: repos}
	result := dispatcher.fail(ctx, proof, runID, "AGENT_PROFILE_UNAVAILABLE", false)
	if result["status"] != "failed" || result["retryable"] != false {
		t.Fatalf("failure result=%#v", result)
	}
	task, err := repos.ChatTasks.GetAiTask(taskID)
	if err != nil || task["status"] != "failed" {
		t.Fatalf("product task=%#v err=%v", task, err)
	}
	run, err := repos.AgentRuns.GetRunInternal(ctx, runID)
	if err != nil || run.Status != "failed" {
		t.Fatalf("agent run=%+v err=%v", run, err)
	}
	events, err := repos.AgentRuns.ListPublicEvents(ctx, runID, 0, 20)
	if err != nil || len(events.Items) == 0 || events.Items[len(events.Items)-1].EventType != "failed" {
		t.Fatalf("agent run events=%#v err=%v", events, err)
	}
}

func TestAITaskDispatcherDefersRuntimeCapacityUntilBoundedRetryBudget(t *testing.T) {
	ctx := context.Background()
	repos := persistence.NewRepositories(nil)
	const runID = "run_dispatch_capacity_wait"
	repos.Queue.Enqueue(map[string]any{
		"queueId": "queue_dispatch_capacity_wait", "queueName": queue.QueueAIRuntimeInteractive,
		"taskType": "runtime_dispatch", "taskId": runID, "maxAttempts": 120,
	})
	_, proof, err := repos.Queue.Lease(ctx, queue.QueueAIRuntimeInteractive, "dispatcher-capacity-wait", time.Minute, "runtime_dispatch")
	if err != nil {
		t.Fatal(err)
	}
	before := time.Now().UTC()
	result := (AITaskDispatcher{Repos: repos}).fail(ctx, proof, runID, "RUNTIME_CAPACITY_UNAVAILABLE", true)
	if result["retryable"] != true {
		t.Fatalf("capacity result=%#v", result)
	}
	records := repos.Queue.ListQueueRecords(map[string]any{"queueId": proof.QueueID})
	if len(records) != 1 || records[0]["status"] != "retry_wait" || records[0]["attempt"] != 1 || records[0]["maxAttempts"] != 120 {
		t.Fatalf("capacity retry queue=%#v", records)
	}
	availableAt, err := time.Parse(time.RFC3339Nano, records[0]["availableAt"].(string))
	if err != nil || availableAt.Before(before.Add(runtimeCapacityRetryInitialDelay-time.Second)) {
		t.Fatalf("capacity retry delay availableAt=%s err=%v", availableAt, err)
	}
}

func TestRuntimeDispatchRetryDelayUsesBoundedCapacityBackoff(t *testing.T) {
	for _, test := range []struct {
		attempt int
		want    time.Duration
	}{
		{attempt: 1, want: 2 * time.Second},
		{attempt: 2, want: 4 * time.Second},
		{attempt: 4, want: 16 * time.Second},
		{attempt: 5, want: 30 * time.Second},
		{attempt: 200, want: 30 * time.Second},
	} {
		if got := runtimeDispatchRetryDelay(persistence.QueueLeaseProof{Attempt: test.attempt}, "RUNTIME_CAPACITY_UNAVAILABLE"); got != test.want {
			t.Fatalf("attempt=%d delay=%s want=%s", test.attempt, got, test.want)
		}
	}
	if got := runtimeDispatchRetryDelay(persistence.QueueLeaseProof{Attempt: 200}, "RUNTIME_INPUT_INVALID"); got != time.Second {
		t.Fatalf("non-capacity retry delay=%s", got)
	}
}

func TestAITaskDispatcherReservesHostAndReturnsWithoutWaitingForTerminal(t *testing.T) {
	ctx := context.Background()
	repos := persistence.NewRepositories(nil)
	run := persistence.AgentRunRecord{
		AgentRunID: "run_dispatch_1", TenantID: "tenant_1", UserID: "user_1", WorkspaceID: "workspace_1",
		IdempotencyKey: "idem_dispatch_1", RequestHash: "request_hash", Status: "resolving_intent",
		WorkspaceVersion: 1, BindingVersion: 1, ContextGeneration: 1,
		RequestSnapshot: map[string]any{"input": map[string]any{"type": "text", "text": "hello dispatcher"}},
	}
	if _, _, err := repos.AgentRuns.CreateRun(ctx, persistence.CreateAgentRunCommand{Record: run}); err != nil {
		t.Fatal(err)
	}
	repos.Workspace.CreateWorkspace(run.WorkspaceID, run.TenantID, run.UserID, "ready", "v1")
	if err := repos.AgentRuns.SaveIntent(ctx, run.AgentRunID, map[string]any{"resolvedTaskType": "work_ai_general_chat"}, "v1"); err != nil {
		t.Fatal(err)
	}
	plan := runtimepkg.AgentRunPlan{
		SchemaVersion: "agent_run_plan.v1", AgentRunID: run.AgentRunID, PlanVersion: 1,
		RoutingMode: "deterministic", TaskType: "work_ai_general_chat", ExecutionScope: "detached_task",
		L1AgentProfile: "work_ai_agent", RuntimeConfigID: "runtime-frozen-dispatch", AgentHash: "sha256:" + strings.Repeat("a", 64), ManifestVersion: "manifest-v1",
		SelectedSkillProfiles: []string{"general_chat"}, RequiredTools: []string{},
		OutputContract: map[string]any{"schemaVersion": "agent_output.v1", "format": "text"},
		TerminalOutput: runtimepkg.AgentRunTerminalOutputIdentity{
			TaskType: "work_ai_general_chat", L1AgentProfile: "work_ai_agent", SkillProfile: "general_chat",
			PromptTemplateID: "work_ai.general_chat.v1", PromptTemplateVersion: "v0.1.0", OutputSchemaVersion: "general_chat.result.v1", Format: "text",
		},
		WriteMode:        "none",
		ToolBudget:       runtimepkg.RuntimeToolBudget{MaxToolCalls: 200, SoftToolCallLimit: 160, FinalizationReserve: 10, MaxRepeatedCalls: 2, MaxNoProgressCalls: 4, MaxReadBytes: 1024, MaxWallTimeSeconds: 60},
		WorkspaceVersion: 1, CapabilityHash: "cap-v1",
	}
	frozen := testFrozenWorkspaceContext(t, run, plan, 0, 0)
	plan.WorkspaceContextManifestHash = frozen.ManifestHash
	if err := repos.AgentRuns.SaveWorkspaceContext(ctx, frozen); err != nil {
		t.Fatal(err)
	}
	if err := repos.AgentRuns.SavePlan(ctx, persistence.AgentRunPlanRecord{AgentRunID: run.AgentRunID, PlanVersion: 1, PlanStatus: "validated", AgentRunStatus: "queued", Plan: valueMap(plan)}); err != nil {
		t.Fatal(err)
	}
	repos.Queue.Enqueue(map[string]any{
		"queueId": "dispatch_queue_1", "queueName": queue.QueueAIRuntimeBackground,
		"taskType": "runtime_dispatch", "taskId": run.AgentRunID,
		"payload": map[string]any{"agentRunId": run.AgentRunID, "planVersion": 1},
	})
	record, proof, err := repos.Queue.Lease(ctx, queue.QueueAIRuntimeBackground, "dispatcher-1", time.Minute, "runtime_dispatch")
	if err != nil {
		t.Fatalf("dispatch record not leased: %v", err)
	}
	hosts := runtimepkg.NewRuntimeHostRepository(nil)
	capabilities := runtimepkg.RuntimeCapabilitySnapshot{
		CapabilityHash: "cap-v1", MaxToolCallsSupported: 400, SupportsPerRunBudget: true, SupportsBudgetWarning: true, SupportsForcedAbort: true,
		SubmitBinding:   runtimepkg.RuntimeSubmitBindingCapability{Version: runtimepkg.RuntimeSubmitBindingV2, ProductSessionHash: true},
		BudgetExecution: runtimepkg.DefaultRuntimeToolBudgetExecutionContract(),
	}
	if _, err := hosts.RegisterHost(ctx, runtimepkg.RuntimeHostIdentity{RuntimeHostID: "host-1", InstanceID: "instance-1", Environment: "test"}, runtimepkg.RuntimeHostRegistration{
		Endpoint: "http://runtime-host.test", RuntimeVersion: "2026.6.2", AdapterVersion: "v0.5", Capabilities: capabilities, MaxActiveRuns: 4,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := hosts.HeartbeatHost(ctx, runtimepkg.RuntimeHostIdentity{RuntimeHostID: "host-1", InstanceID: "instance-1", Environment: "test"}, runtimepkg.RuntimeHostHeartbeat{
		Sequence: 1, ObservedAt: time.Now().UTC(), CapabilityHash: "cap-v1", SignatureKeyID: "test-key",
	}); err != nil {
		t.Fatal(err)
	}
	scheduler := runtimepkg.NewRuntimeScheduler(hosts, queue.NewMemoryDistributedLockManager())
	handshakeComplete := false
	client := &fakeAsyncRuntimeClient{submitCheck: func() error {
		if !handshakeComplete {
			return errors.New("submit ran before capability handshake")
		}
		return nil
	}}
	dispatcher := NewAITaskDispatcherWithCapabilityReader(repos, hosts, scheduler, client, fixedDispatcherManifestProvider{}, "ticket-secret", "dispatcher-session-secret", "v1", "worker-instance-1",
		dispatcherCapabilityReaderFunc(func(_ context.Context, host runtimepkg.RuntimeHost) (runtimepkg.RuntimeCapabilities, error) {
			if host.RuntimeHostID != "host-1" || host.CapabilityHash != capabilities.CapabilityHash {
				return runtimepkg.RuntimeCapabilities{}, errors.New("unexpected reserved host")
			}
			handshakeComplete = true
			return dispatcherRuntimeCapabilities(capabilities), nil
		}),
	)
	dispatcher.RuntimeConfigVersions = runtimepkg.RuntimeConfigVersions{"runtime-frozen-dispatch": "v1"}
	dispatcher.CapacityProvider = RuntimeCapacityCommandProviderFunc(func(_ context.Context, run persistence.AgentRunRecord, _ runtimepkg.AgentRunPlan) (runtimepkg.RuntimeCapacityCommand, error) {
		dimension := func(key string) runtimepkg.RuntimeCapacityDimension {
			return runtimepkg.RuntimeCapacityDimension{Key: key, Limit: 10, Requested: 1, Version: 1}
		}
		return runtimepkg.RuntimeCapacityCommand{RunID: run.AgentRunID, SnapshotVersion: 1, TTL: time.Minute, Dimensions: runtimepkg.RuntimeCapacityDimensions{Model: dimension("model:test"), AuthPool: dimension("auth:test"), Tool: dimension("tool:test"), Tenant: dimension("tenant:" + run.TenantID), User: dimension("user:" + run.UserID)}}, nil
	})
	result := dispatcher.ProcessWithProof(ctx, record, proof, queue.QueueAIRuntimeBackground)
	if result["status"] != "running" {
		t.Fatalf("dispatch result=%#v", result)
	}
	if !scheduler.LeaseSupervisor.IsTracking(run.AgentRunID) {
		t.Fatal("dispatcher did not transfer the accepted lease to the supervisor")
	}
	if client.submitted.RunID != run.AgentRunID || client.submitted.InputMessage != "hello dispatcher" || client.submitted.RunTicket == "" || client.submitted.RuntimeConfigID != "runtime-frozen-dispatch" {
		t.Fatalf("submit request=%+v", client.submitted)
	}
	ticketClaims, err := runtimepkg.VerifyRunTicket(client.submitted.RunTicket, "ticket-secret", time.Now().UTC())
	threadID, threadOK := client.submitted.ProductSessionRef["threadId"].(string)
	sessionKey, sessionOK := client.submitted.ProductSessionRef["openclawSessionKey"].(string)
	if err != nil || !threadOK || !sessionOK || ticketClaims.SubmitBinding == nil || ticketClaims.SubmitBinding.Version != runtimepkg.RuntimeSubmitBindingV2 ||
		ticketClaims.SubmitBinding.InputMessageHash != runtimepkg.RunTicketInputMessageHash(client.submitted.InputMessage) ||
		ticketClaims.SubmitBinding.RuntimeConfigID != client.submitted.RuntimeConfigID ||
		ticketClaims.SubmitBinding.RuntimeConfigVersion != client.submitted.RuntimeConfigVersion ||
		ticketClaims.SubmitBinding.ProductSessionHash != runtimepkg.RunTicketProductSessionHash(threadID, sessionKey) {
		t.Fatalf("submit ticket binding=%#v err=%v request=%+v", ticketClaims.SubmitBinding, err, client.submitted)
	}
	stored, err := repos.AgentRuns.GetRunInternal(ctx, run.AgentRunID)
	if err != nil || stored.Status != "running" {
		t.Fatalf("run=%+v err=%v", stored, err)
	}
	eventJob, eventProof, err := repos.Queue.Lease(ctx, queue.QueueRuntimeEvents, "events-1", time.Minute)
	if err != nil || eventJob["taskId"] != run.AgentRunID {
		t.Fatalf("runtime event job=%#v err=%v", eventJob, err)
	}
	client.events = runtimepkg.AsyncRuntimeEventPage{Items: []runtimepkg.AsyncRuntimeEvent{{
		Sequence: 1, EventType: "run.succeeded", Status: "succeeded", Timestamp: time.Now().UTC(),
		Data: map[string]any{"toolCalls": 3},
	}}, NextAfterSequence: 1}
	client.status = runtimepkg.AsyncRuntimeStatus{RunID: run.AgentRunID, Status: "succeeded", LastEventSequence: 1, Result: map[string]any{"finalAnswer": "done"}}
	eventResult := NewRuntimeEventWorker(repos, hosts, scheduler, client, "ticket-secret").Process(ctx, eventJob, eventProof)
	if eventResult["status"] != "succeeded" {
		t.Fatalf("event result=%#v", eventResult)
	}
	if scheduler.LeaseSupervisor.IsTracking(run.AgentRunID) {
		t.Fatal("terminal convergence did not stop lease ownership")
	}
	stored, err = repos.AgentRuns.GetRunInternal(ctx, run.AgentRunID)
	if err != nil || stored.Status != "succeeded" || stored.PublicResult["finalAnswer"] != "done" {
		t.Fatalf("terminal run=%+v err=%v", stored, err)
	}
	events, err := hosts.ListRunEvents(ctx, run.AgentRunID, 0, 100)
	if err != nil || len(events) == 0 || events[len(events)-1].SourceSequence != 1 {
		t.Fatalf("persisted runtime events=%#v err=%v", events, err)
	}
}

func TestAITaskDispatcherRejectsWorkspaceVersionAdvanceDuringManifestBuild(t *testing.T) {
	ctx := context.Background()
	repos := persistence.NewRepositories(nil)
	run := persistence.AgentRunRecord{
		AgentRunID: "run_dispatch_workspace_race", TenantID: "tenant_dispatch_workspace_race", UserID: "user_dispatch_workspace_race", WorkspaceID: "workspace_dispatch_workspace_race",
		IdempotencyKey: "idem_dispatch_workspace_race", RequestHash: "request_hash", Status: "resolving_intent",
		WorkspaceVersion: 1, BindingVersion: 1, ContextGeneration: 1,
		RequestSnapshot: map[string]any{"input": map[string]any{"type": "text", "text": "do not mix workspace versions"}},
	}
	if _, _, err := repos.AgentRuns.CreateRun(ctx, persistence.CreateAgentRunCommand{Record: run}); err != nil {
		t.Fatal(err)
	}
	repos.Workspace.CreateWorkspace(run.WorkspaceID, run.TenantID, run.UserID, "ready", "v1")
	if err := repos.AgentRuns.SaveIntent(ctx, run.AgentRunID, map[string]any{"resolvedTaskType": "work_ai_general_chat"}, "v1"); err != nil {
		t.Fatal(err)
	}
	capabilities := runtimepkg.RuntimeCapabilitySnapshot{
		CapabilityHash: "capability-dispatch-workspace-race", MaxToolCallsSupported: 200,
		SubmitBinding:        runtimepkg.RuntimeSubmitBindingCapability{Version: runtimepkg.RuntimeSubmitBindingV2, ProductSessionHash: true},
		SupportsPerRunBudget: true, SupportsBudgetWarning: true, SupportsForcedAbort: true,
		BudgetExecution: runtimepkg.DefaultRuntimeToolBudgetExecutionContract(),
	}
	plan := runtimepkg.AgentRunPlan{
		SchemaVersion: "agent_run_plan.v1", AgentRunID: run.AgentRunID, PlanVersion: 1,
		RoutingMode: "deterministic", TaskType: "work_ai_general_chat", ExecutionScope: "detached_task",
		L1AgentProfile: "work_ai_agent", RuntimeConfigID: "runtime-workspace-race", AgentHash: "sha256:" + strings.Repeat("a", 64), ManifestVersion: "manifest-v1",
		SelectedSkillProfiles: []string{"general_chat"}, RequiredTools: []string{},
		OutputContract: map[string]any{"schemaVersion": "agent_output.v1", "format": "text"},
		TerminalOutput: runtimepkg.AgentRunTerminalOutputIdentity{
			TaskType: "work_ai_general_chat", L1AgentProfile: "work_ai_agent", SkillProfile: "general_chat",
			PromptTemplateID: "work_ai.general_chat.v1", PromptTemplateVersion: "v0.1.0", OutputSchemaVersion: "general_chat.result.v1", Format: "text",
		},
		WriteMode: "none", ToolBudget: runtimepkg.RuntimeToolBudget{MaxToolCalls: 200, SoftToolCallLimit: 160, FinalizationReserve: 10, MaxRepeatedCalls: 2, MaxNoProgressCalls: 4, MaxReadBytes: 1024, MaxWallTimeSeconds: 60},
		WorkspaceVersion: 1, CapabilityHash: capabilities.CapabilityHash,
	}
	frozen := testFrozenWorkspaceContext(t, run, plan, 0, 0)
	plan.WorkspaceContextManifestHash = frozen.ManifestHash
	if err := repos.AgentRuns.SaveWorkspaceContext(ctx, frozen); err != nil {
		t.Fatal(err)
	}
	if err := repos.AgentRuns.SavePlan(ctx, persistence.AgentRunPlanRecord{AgentRunID: run.AgentRunID, PlanVersion: 1, PlanStatus: "validated", AgentRunStatus: "queued", Plan: valueMap(plan)}); err != nil {
		t.Fatal(err)
	}
	repos.Queue.Enqueue(map[string]any{
		"queueId": "dispatch_queue_workspace_race", "queueName": queue.QueueAIRuntimeBackground,
		"taskType": "runtime_dispatch", "taskId": run.AgentRunID,
		"payload": map[string]any{"agentRunId": run.AgentRunID, "planVersion": 1},
	})
	record, proof, err := repos.Queue.Lease(ctx, queue.QueueAIRuntimeBackground, "dispatcher-workspace-race", time.Minute, "runtime_dispatch")
	if err != nil {
		t.Fatalf("lease dispatch: %v", err)
	}
	hosts := runtimepkg.NewRuntimeHostRepository(nil)
	identity := runtimepkg.RuntimeHostIdentity{RuntimeHostID: "host-workspace-race", InstanceID: "instance-workspace-race", Environment: "test"}
	if _, err := hosts.RegisterHost(ctx, identity, runtimepkg.RuntimeHostRegistration{
		Endpoint: "http://runtime-host.test", RuntimeVersion: "runtime-v1", AdapterVersion: "adapter-v1", Capabilities: capabilities, MaxActiveRuns: 1,
	}); err != nil {
		t.Fatalf("register host: %v", err)
	}
	if _, err := hosts.HeartbeatHost(ctx, identity, runtimepkg.RuntimeHostHeartbeat{
		Sequence: 1, ObservedAt: time.Now().UTC(), CapabilityHash: capabilities.CapabilityHash, SignatureKeyID: "test-key",
	}); err != nil {
		t.Fatalf("heartbeat host: %v", err)
	}
	scheduler := runtimepkg.NewRuntimeScheduler(hosts, queue.NewMemoryDistributedLockManager())
	provider := &workspaceVersionAdvancingManifestProvider{advance: func() error {
		_, err := repos.WorkspaceIndex.RecordWorkspaceChanges(ctx, run.WorkspaceID, []string{"resources/overview.md"}, "workspace_version_race_test")
		return err
	}}
	client := &fakeAsyncRuntimeClient{}
	dispatcher := NewAITaskDispatcherWithCapabilityReader(repos, hosts, scheduler, client, provider, "ticket-secret", "dispatcher-session-secret", "v1", "worker-workspace-race",
		dispatcherCapabilityReaderFunc(func(_ context.Context, host runtimepkg.RuntimeHost) (runtimepkg.RuntimeCapabilities, error) {
			if host.RuntimeHostID != identity.RuntimeHostID {
				return runtimepkg.RuntimeCapabilities{}, errors.New("unexpected host")
			}
			return dispatcherRuntimeCapabilities(capabilities), nil
		}),
	)
	dispatcher.RuntimeConfigVersions = runtimepkg.RuntimeConfigVersions{"runtime-workspace-race": "v1"}
	dispatcher.CapacityProvider = RuntimeCapacityCommandProviderFunc(func(_ context.Context, loadedRun persistence.AgentRunRecord, _ runtimepkg.AgentRunPlan) (runtimepkg.RuntimeCapacityCommand, error) {
		dimension := func(key string) runtimepkg.RuntimeCapacityDimension {
			return runtimepkg.RuntimeCapacityDimension{Key: key, Limit: 1, Requested: 1, Version: 1}
		}
		return runtimepkg.RuntimeCapacityCommand{RunID: loadedRun.AgentRunID, SnapshotVersion: 1, TTL: time.Minute, Dimensions: runtimepkg.RuntimeCapacityDimensions{
			Model: dimension("model:test"), AuthPool: dimension("auth:test"), Tool: dimension("tool:test"), Tenant: dimension("tenant:" + loadedRun.TenantID), User: dimension("user:" + loadedRun.UserID),
		}}, nil
	})

	result := dispatcher.ProcessWithProof(ctx, record, proof, queue.QueueAIRuntimeBackground)
	if result["errorCode"] != "AGENT_PLAN_EXPIRED" || result["status"] != "failed" {
		t.Fatalf("Workspace version race result=%#v", result)
	}
	if provider.buildCalls != 1 || client.submitCalls != 0 {
		t.Fatalf("W2 manifest must stop before submit: builds=%d submits=%d", provider.buildCalls, client.submitCalls)
	}
	dispatches, err := hosts.ListDispatchesAdmin(ctx, run.AgentRunID, "", "", 10)
	if err != nil || len(dispatches) != 0 {
		t.Fatalf("W2 manifest must stop before dispatch/ticket persistence: dispatches=%#v err=%v", dispatches, err)
	}
	reservations, err := hosts.ListReservationsAdmin(ctx, run.AgentRunID, "", "", 10)
	if err != nil || len(reservations) != 1 || reservations[0].State != "released" {
		t.Fatalf("W2 manifest must release the unaccepted Host reservation: reservations=%#v err=%v", reservations, err)
	}
	if _, err := scheduler.Capacity.GetActiveByRunID(ctx, run.AgentRunID); err == nil || err.Error() != "NOT_FOUND" {
		t.Fatalf("W2 manifest must not retain active capacity: err=%v", err)
	}
	capacity, err := scheduler.Capacity.GetLatestByRunID(ctx, run.AgentRunID)
	if err != nil || capacity.State != "released" {
		t.Fatalf("W2 manifest must release capacity: capacity=%+v err=%v", capacity, err)
	}
}

func TestAITaskDispatcherRejectsLiveCapabilityDriftBeforeSubmit(t *testing.T) {
	snapshot := runtimepkg.RuntimeCapabilitySnapshot{
		CapabilityHash: "capability-dispatch-drift", MaxToolCallsSupported: 200,
		SubmitBinding:        runtimepkg.RuntimeSubmitBindingCapability{Version: runtimepkg.RuntimeSubmitBindingV2, ProductSessionHash: true},
		SupportsPerRunBudget: true, SupportsBudgetWarning: true, SupportsForcedAbort: true,
		BudgetExecution: runtimepkg.DefaultRuntimeToolBudgetExecutionContract(),
		Tools:           []runtimepkg.ToolCapability{runtimepkg.CanonicalAgentFacingToolCapability("read", "ready")},
	}
	host := runtimepkg.RuntimeHost{
		RuntimeHostID: "host-dispatch-drift", CapabilityHash: snapshot.CapabilityHash, Capabilities: snapshot,
	}
	plan := runtimepkg.AgentRunPlan{
		CapabilityHash: snapshot.CapabilityHash, RequiredTools: []string{"read"},
		ToolBudget: runtimepkg.RuntimeToolBudget{MaxToolCalls: 200, SoftToolCallLimit: 160, FinalizationReserve: 10, MaxRepeatedCalls: 2, MaxNoProgressCalls: 4, MaxReadBytes: 1024, MaxWallTimeSeconds: 60},
	}
	dispatcher := AITaskDispatcher{CapabilityReader: dispatcherCapabilityReaderFunc(func(context.Context, runtimepkg.RuntimeHost) (runtimepkg.RuntimeCapabilities, error) {
		live := dispatcherRuntimeCapabilities(snapshot)
		live.Tools = []runtimepkg.ToolCapability{runtimepkg.CanonicalAgentFacingToolCapability("read", "degraded")}
		return live, nil
	})}
	if err := dispatcher.validateReservedHostCapabilities(context.Background(), host, plan); err == nil || err.Error() != "RUNTIME_TOOL_UNAVAILABLE" {
		t.Fatalf("degraded live capability must reject dispatch before submit, err=%v", err)
	}

	dispatcher.CapabilityReader = dispatcherCapabilityReaderFunc(func(context.Context, runtimepkg.RuntimeHost) (runtimepkg.RuntimeCapabilities, error) {
		live := dispatcherRuntimeCapabilities(snapshot)
		live.CapabilityHash = "capability-other"
		return live, nil
	})
	if err := dispatcher.validateReservedHostCapabilities(context.Background(), host, plan); err == nil || err.Error() != "RUNTIME_TOOL_UNAVAILABLE" {
		t.Fatalf("live hash mismatch must reject dispatch before submit, err=%v", err)
	}

	for name, mutate := range map[string]func(*runtimepkg.RuntimeCapabilities){
		"missing_submit_binding": func(live *runtimepkg.RuntimeCapabilities) {
			live.SubmitBinding = runtimepkg.RuntimeSubmitBindingCapability{}
		},
		"legacy_submit_binding": func(live *runtimepkg.RuntimeCapabilities) {
			live.SubmitBinding.Version = "runtime_submit_binding.v1"
		},
		"malformed_v2_submit_binding": func(live *runtimepkg.RuntimeCapabilities) {
			live.SubmitBinding.ProductSessionHash = false
		},
	} {
		t.Run(name, func(t *testing.T) {
			dispatcher.CapabilityReader = dispatcherCapabilityReaderFunc(func(context.Context, runtimepkg.RuntimeHost) (runtimepkg.RuntimeCapabilities, error) {
				live := dispatcherRuntimeCapabilities(snapshot)
				mutate(&live)
				return live, nil
			})
			if err := dispatcher.validateReservedHostCapabilities(context.Background(), host, plan); err == nil || err.Error() != "RUNTIME_TOOL_UNAVAILABLE" {
				t.Fatalf("%s must reject before ticket issuance or submit, err=%v", name, err)
			}
		})
	}

	t.Run("pinned_test_loopback_v05_host_without_projection_is_rejected", func(t *testing.T) {
		legacyFixtureHost := host
		legacyFixtureHost.Environment = "test"
		legacyFixtureHost.Endpoint = "http://127.0.0.1:18790"
		legacyFixtureHost.AdapterVersion = "v0.5"
		legacyFixtureHost.RuntimeHostID = "runtime-host-test-1"
		dispatcher.CapabilityReader = dispatcherCapabilityReaderFunc(func(context.Context, runtimepkg.RuntimeHost) (runtimepkg.RuntimeCapabilities, error) {
			live := dispatcherRuntimeCapabilities(snapshot)
			live.AdapterVersion = "v0.5"
			live.SubmitBinding = runtimepkg.RuntimeSubmitBindingCapability{}
			return live, nil
		})
		if err := dispatcher.validateReservedHostCapabilities(context.Background(), legacyFixtureHost, plan); err == nil || err.Error() != "RUNTIME_TOOL_UNAVAILABLE" {
			t.Fatalf("pinned test loopback v0.5 Host must reject missing submit binding before ticket issuance or submit, err=%v", err)
		}
	})

	t.Run("pinned_test_loopback_v05_host_without_projection_is_accepted_when_explicitly_enabled", func(t *testing.T) {
		t.Setenv("HUAHUO_RUNTIME_LEGACY_RUN_TICKET_COMPAT", "1")
		legacyCapabilities := snapshot
		legacyCapabilities.SubmitBinding = runtimepkg.RuntimeSubmitBindingCapability{}
		legacyFixtureHost := host
		legacyFixtureHost.Environment = "test"
		legacyFixtureHost.Endpoint = "http://127.0.0.1:18790"
		legacyFixtureHost.AdapterVersion = "v0.5"
		legacyFixtureHost.RuntimeHostID = "runtime-host-test-1"
		legacyFixtureHost.Capabilities = legacyCapabilities
		dispatcher.CapabilityReader = dispatcherCapabilityReaderFunc(func(context.Context, runtimepkg.RuntimeHost) (runtimepkg.RuntimeCapabilities, error) {
			live := dispatcherRuntimeCapabilities(legacyCapabilities)
			live.AdapterVersion = "v0.5"
			return live, nil
		})
		if err := dispatcher.validateReservedHostCapabilities(context.Background(), legacyFixtureHost, plan); err != nil {
			t.Fatalf("explicit pinned v0.5 compatibility must preserve all non-binding validation, err=%v", err)
		}
	})
}

func TestAITaskDispatcherRejectsReaderWithoutFreshCapabilityProbe(t *testing.T) {
	snapshot := runtimepkg.RuntimeCapabilitySnapshot{
		CapabilityHash: "capability-dispatch-no-fresh-reader", MaxToolCallsSupported: 200,
		SubmitBinding:        runtimepkg.RuntimeSubmitBindingCapability{Version: runtimepkg.RuntimeSubmitBindingV2, ProductSessionHash: true},
		SupportsPerRunBudget: true, SupportsBudgetWarning: true, SupportsForcedAbort: true,
		BudgetExecution: runtimepkg.DefaultRuntimeToolBudgetExecutionContract(),
		Tools:           []runtimepkg.ToolCapability{runtimepkg.CanonicalAgentFacingToolCapability("read", "ready")},
	}
	host := runtimepkg.RuntimeHost{
		RuntimeHostID: "host-dispatch-no-fresh-reader", CapabilityHash: snapshot.CapabilityHash, Capabilities: snapshot,
	}
	plan := runtimepkg.AgentRunPlan{
		CapabilityHash: snapshot.CapabilityHash, RequiredTools: []string{"read"},
		ToolBudget: runtimepkg.RuntimeToolBudget{MaxToolCalls: 200, SoftToolCallLimit: 160, FinalizationReserve: 10, MaxRepeatedCalls: 2, MaxNoProgressCalls: 4, MaxReadBytes: 1024, MaxWallTimeSeconds: 60},
	}
	readCalls := 0
	dispatcher := AITaskDispatcher{CapabilityReader: legacyDispatcherCapabilityReaderFunc(func(context.Context, runtimepkg.RuntimeHost) (runtimepkg.RuntimeCapabilities, error) {
		readCalls++
		return dispatcherRuntimeCapabilities(snapshot), nil
	})}
	if err := dispatcher.validateReservedHostCapabilities(context.Background(), host, plan); err == nil || err.Error() != "RUNTIME_TOOL_UNAVAILABLE" {
		t.Fatalf("reader without final fresh probe must fail closed, err=%v", err)
	}
	if readCalls != 0 {
		t.Fatalf("legacy reader unexpectedly reached capability discovery %d times", readCalls)
	}
}

func TestAITaskDispatcherRejectsPinnedV05BeforeMaterializationTicketOrRuntimeSubmit(t *testing.T) {
	ctx := context.Background()
	repos := persistence.NewRepositories(nil)
	run := persistence.AgentRunRecord{
		AgentRunID: "run_dispatch_pinned_v05_rejected", TenantID: "tenant_dispatch_pinned_v05", UserID: "user_dispatch_pinned_v05", WorkspaceID: "workspace_dispatch_pinned_v05",
		IdempotencyKey: "idem_dispatch_pinned_v05", RequestHash: "request_hash", Status: "resolving_intent",
		WorkspaceVersion: 1, BindingVersion: 1, ContextGeneration: 1,
		RequestSnapshot: map[string]any{"input": map[string]any{"type": "text", "text": "reject legacy adapter before dispatch"}},
	}
	if _, _, err := repos.AgentRuns.CreateRun(ctx, persistence.CreateAgentRunCommand{Record: run}); err != nil {
		t.Fatal(err)
	}
	repos.Workspace.CreateWorkspace(run.WorkspaceID, run.TenantID, run.UserID, "ready", "v1")
	if err := repos.AgentRuns.SaveIntent(ctx, run.AgentRunID, map[string]any{"resolvedTaskType": "work_ai_general_chat"}, "v1"); err != nil {
		t.Fatal(err)
	}
	capabilities := runtimepkg.RuntimeCapabilitySnapshot{
		CapabilityHash: "capability-pinned-v05-rejected", MaxToolCallsSupported: 200,
		SubmitBinding:        runtimepkg.RuntimeSubmitBindingCapability{Version: runtimepkg.RuntimeSubmitBindingV2, ProductSessionHash: true},
		SupportsPerRunBudget: true, SupportsBudgetWarning: true, SupportsForcedAbort: true,
		BudgetExecution: runtimepkg.DefaultRuntimeToolBudgetExecutionContract(),
		Tools:           []runtimepkg.ToolCapability{runtimepkg.CanonicalAgentFacingToolCapability("read", "ready")},
	}
	plan := runtimepkg.AgentRunPlan{
		SchemaVersion: "agent_run_plan.v1", AgentRunID: run.AgentRunID, PlanVersion: 1,
		RoutingMode: "deterministic", TaskType: "work_ai_general_chat", ExecutionScope: "detached_task",
		L1AgentProfile: "work_ai_agent", RuntimeConfigID: "runtime-frozen-pinned-v05", AgentHash: "sha256:" + strings.Repeat("a", 64), ManifestVersion: "manifest-v1",
		SelectedSkillProfiles: []string{"general_chat"}, RequiredTools: []string{"read"},
		OutputContract: map[string]any{"schemaVersion": "agent_output.v1", "format": "text"},
		TerminalOutput: runtimepkg.AgentRunTerminalOutputIdentity{
			TaskType: "work_ai_general_chat", L1AgentProfile: "work_ai_agent", SkillProfile: "general_chat",
			PromptTemplateID: "work_ai.general_chat.v1", PromptTemplateVersion: "v0.1.0", OutputSchemaVersion: "general_chat.result.v1", Format: "text",
		},
		WriteMode:        "none",
		ToolBudget:       runtimepkg.RuntimeToolBudget{MaxToolCalls: 200, SoftToolCallLimit: 160, FinalizationReserve: 10, MaxRepeatedCalls: 2, MaxNoProgressCalls: 4, MaxReadBytes: 1024, MaxWallTimeSeconds: 60},
		WorkspaceVersion: 1, CapabilityHash: capabilities.CapabilityHash,
	}
	frozen := testFrozenWorkspaceContext(t, run, plan, 0, 0)
	plan.WorkspaceContextManifestHash = frozen.ManifestHash
	if err := repos.AgentRuns.SaveWorkspaceContext(ctx, frozen); err != nil {
		t.Fatal(err)
	}
	if err := repos.AgentRuns.SavePlan(ctx, persistence.AgentRunPlanRecord{AgentRunID: run.AgentRunID, PlanVersion: 1, PlanStatus: "validated", AgentRunStatus: "queued", Plan: valueMap(plan)}); err != nil {
		t.Fatal(err)
	}
	repos.Queue.Enqueue(map[string]any{
		"queueId": "dispatch_queue_pinned_v05_rejected", "queueName": queue.QueueAIRuntimeBackground,
		"taskType": "runtime_dispatch", "taskId": run.AgentRunID,
		"payload": map[string]any{"agentRunId": run.AgentRunID, "planVersion": 1},
	})
	record, proof, err := repos.Queue.Lease(ctx, queue.QueueAIRuntimeBackground, "dispatcher-pinned-v05", time.Minute, "runtime_dispatch")
	if err != nil {
		t.Fatalf("lease dispatch: %v", err)
	}
	hosts := runtimepkg.NewRuntimeHostRepository(nil)
	identity := runtimepkg.RuntimeHostIdentity{RuntimeHostID: "runtime-host-test-1", InstanceID: "instance-pinned-v05", Environment: "test"}
	if _, err := hosts.RegisterHost(ctx, identity, runtimepkg.RuntimeHostRegistration{
		Endpoint: "http://127.0.0.1:18790", RuntimeVersion: "2026.6.2", AdapterVersion: "v0.5", Capabilities: capabilities, MaxActiveRuns: 1,
	}); err != nil {
		t.Fatalf("register pinned v0.5 host: %v", err)
	}
	if _, err := hosts.HeartbeatHost(ctx, identity, runtimepkg.RuntimeHostHeartbeat{
		Sequence: 1, ObservedAt: time.Now().UTC(), CapabilityHash: capabilities.CapabilityHash, SignatureKeyID: "test-key",
	}); err != nil {
		t.Fatalf("heartbeat pinned v0.5 host: %v", err)
	}
	scheduler := runtimepkg.NewRuntimeScheduler(hosts, queue.NewMemoryDistributedLockManager())
	manifestProvider := &countedDispatcherManifestProvider{}
	client := &fakeAsyncRuntimeClient{}
	dispatcher := NewAITaskDispatcherWithCapabilityReader(repos, hosts, scheduler, client, manifestProvider, "", "dispatcher-session-secret", "v1", "worker-pinned-v05",
		dispatcherCapabilityReaderFunc(func(_ context.Context, host runtimepkg.RuntimeHost) (runtimepkg.RuntimeCapabilities, error) {
			if host.RuntimeHostID != identity.RuntimeHostID || host.Endpoint != "http://127.0.0.1:18790" || host.AdapterVersion != "v0.5" {
				return runtimepkg.RuntimeCapabilities{}, errors.New("unexpected host for pinned v0.5 regression")
			}
			live := dispatcherRuntimeCapabilities(capabilities)
			live.AdapterVersion = "v0.5"
			live.SubmitBinding = runtimepkg.RuntimeSubmitBindingCapability{}
			return live, nil
		}),
	)
	dispatcher.RuntimeConfigVersions = runtimepkg.RuntimeConfigVersions{"runtime-frozen-pinned-v05": "v1"}
	dispatcher.CapacityProvider = RuntimeCapacityCommandProviderFunc(func(_ context.Context, loadedRun persistence.AgentRunRecord, _ runtimepkg.AgentRunPlan) (runtimepkg.RuntimeCapacityCommand, error) {
		dimension := func(key string) runtimepkg.RuntimeCapacityDimension {
			return runtimepkg.RuntimeCapacityDimension{Key: key, Limit: 1, Requested: 1, Version: 1}
		}
		return runtimepkg.RuntimeCapacityCommand{RunID: loadedRun.AgentRunID, SnapshotVersion: 1, TTL: time.Minute, Dimensions: runtimepkg.RuntimeCapacityDimensions{
			Model: dimension("model:test"), AuthPool: dimension("auth:test"), Tool: dimension("tool:test"), Tenant: dimension("tenant:" + loadedRun.TenantID), User: dimension("user:" + loadedRun.UserID),
		}}, nil
	})

	result := dispatcher.ProcessWithProof(ctx, record, proof, queue.QueueAIRuntimeBackground)
	if result["errorCode"] != "RUNTIME_TOOL_UNAVAILABLE" || result["retryable"] != true {
		t.Fatalf("pinned v0.5 result=%#v; missing v2 binding must fail closed before dispatch", result)
	}
	if manifestProvider.buildCalls != 0 {
		t.Fatalf("pinned v0.5 host reached manifest materialization %d times", manifestProvider.buildCalls)
	}
	if client.submitCalls != 0 {
		t.Fatalf("pinned v0.5 host reached Runtime submit %d times", client.submitCalls)
	}
	dispatches, err := hosts.ListDispatchesAdmin(ctx, run.AgentRunID, "", "", 10)
	if err != nil || len(dispatches) != 0 {
		t.Fatalf("pinned v0.5 host reached ticket/dispatch creation: dispatches=%#v err=%v", dispatches, err)
	}
}

func TestAITaskDispatcherFreshCapabilityProbeRejectsSameHashDowngradeBeforeMaterialization(t *testing.T) {
	ctx := context.Background()
	repos := persistence.NewRepositories(nil)
	run := persistence.AgentRunRecord{
		AgentRunID: "run_dispatch_fresh_submit_binding", TenantID: "tenant_dispatch_fresh", UserID: "user_dispatch_fresh", WorkspaceID: "workspace_dispatch_fresh",
		IdempotencyKey: "idem_dispatch_fresh", RequestHash: "request_hash", Status: "resolving_intent",
		WorkspaceVersion: 1, BindingVersion: 1, ContextGeneration: 1,
		RequestSnapshot: map[string]any{"input": map[string]any{"type": "text", "text": "reject cached capability downgrade before dispatch"}},
	}
	if _, _, err := repos.AgentRuns.CreateRun(ctx, persistence.CreateAgentRunCommand{Record: run}); err != nil {
		t.Fatal(err)
	}
	repos.Workspace.CreateWorkspace(run.WorkspaceID, run.TenantID, run.UserID, "ready", "v1")
	if err := repos.AgentRuns.SaveIntent(ctx, run.AgentRunID, map[string]any{"resolvedTaskType": "work_ai_general_chat"}, "v1"); err != nil {
		t.Fatal(err)
	}
	capabilities := runtimepkg.RuntimeCapabilitySnapshot{
		CapabilityHash: "capability-fresh-submit-binding", MaxToolCallsSupported: 200,
		SubmitBinding:        runtimepkg.RuntimeSubmitBindingCapability{Version: runtimepkg.RuntimeSubmitBindingV2, ProductSessionHash: true},
		SupportsPerRunBudget: true, SupportsBudgetWarning: true, SupportsForcedAbort: true,
		BudgetExecution: runtimepkg.DefaultRuntimeToolBudgetExecutionContract(),
		Tools:           []runtimepkg.ToolCapability{runtimepkg.CanonicalAgentFacingToolCapability("read", "ready")},
	}
	plan := runtimepkg.AgentRunPlan{
		SchemaVersion: "agent_run_plan.v1", AgentRunID: run.AgentRunID, PlanVersion: 1,
		RoutingMode: "deterministic", TaskType: "work_ai_general_chat", ExecutionScope: "detached_task",
		L1AgentProfile: "work_ai_agent", RuntimeConfigID: "runtime-fresh-submit-binding", AgentHash: "sha256:" + strings.Repeat("a", 64), ManifestVersion: "manifest-v1",
		SelectedSkillProfiles: []string{"general_chat"}, RequiredTools: []string{"read"},
		OutputContract: map[string]any{"schemaVersion": "agent_output.v1", "format": "text"},
		TerminalOutput: runtimepkg.AgentRunTerminalOutputIdentity{
			TaskType: "work_ai_general_chat", L1AgentProfile: "work_ai_agent", SkillProfile: "general_chat",
			PromptTemplateID: "work_ai.general_chat.v1", PromptTemplateVersion: "v0.1.0", OutputSchemaVersion: "general_chat.result.v1", Format: "text",
		},
		WriteMode:        "none",
		ToolBudget:       runtimepkg.RuntimeToolBudget{MaxToolCalls: 200, SoftToolCallLimit: 160, FinalizationReserve: 10, MaxRepeatedCalls: 2, MaxNoProgressCalls: 4, MaxReadBytes: 1024, MaxWallTimeSeconds: 60},
		WorkspaceVersion: 1, CapabilityHash: capabilities.CapabilityHash,
	}
	frozen := testFrozenWorkspaceContext(t, run, plan, 0, 0)
	plan.WorkspaceContextManifestHash = frozen.ManifestHash
	if err := repos.AgentRuns.SaveWorkspaceContext(ctx, frozen); err != nil {
		t.Fatal(err)
	}
	if err := repos.AgentRuns.SavePlan(ctx, persistence.AgentRunPlanRecord{AgentRunID: run.AgentRunID, PlanVersion: 1, PlanStatus: "validated", AgentRunStatus: "queued", Plan: valueMap(plan)}); err != nil {
		t.Fatal(err)
	}
	repos.Queue.Enqueue(map[string]any{
		"queueId": "dispatch_queue_fresh_submit_binding", "queueName": queue.QueueAIRuntimeBackground,
		"taskType": "runtime_dispatch", "taskId": run.AgentRunID,
		"payload": map[string]any{"agentRunId": run.AgentRunID, "planVersion": 1},
	})
	record, proof, err := repos.Queue.Lease(ctx, queue.QueueAIRuntimeBackground, "dispatcher-fresh-submit-binding", time.Minute, "runtime_dispatch")
	if err != nil {
		t.Fatalf("lease dispatch: %v", err)
	}
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet || request.URL.Path != "/enterprise.runtime/capabilities" {
			t.Fatalf("capability request=%s %s", request.Method, request.URL.Path)
		}
		requests++
		live := dispatcherRuntimeCapabilities(capabilities)
		if requests > 1 {
			// The Adapter process changed while preserving its registration hash.
			live.SubmitBinding = runtimepkg.RuntimeSubmitBindingCapability{}
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(live); err != nil {
			t.Fatal(err)
		}
	}))
	defer server.Close()
	hosts := runtimepkg.NewRuntimeHostRepository(nil)
	identity := runtimepkg.RuntimeHostIdentity{RuntimeHostID: "host-fresh-submit-binding", InstanceID: "instance-fresh-submit-binding", Environment: "test"}
	if _, err := hosts.RegisterHost(ctx, identity, runtimepkg.RuntimeHostRegistration{
		Endpoint: server.URL, RuntimeVersion: "runtime-v1", AdapterVersion: "adapter-v2", Capabilities: capabilities, MaxActiveRuns: 1,
	}); err != nil {
		t.Fatalf("register Host: %v", err)
	}
	if _, err := hosts.HeartbeatHost(ctx, identity, runtimepkg.RuntimeHostHeartbeat{
		Sequence: 1, ObservedAt: time.Now().UTC(), CapabilityHash: capabilities.CapabilityHash, SignatureKeyID: "test-key",
	}); err != nil {
		t.Fatalf("heartbeat Host: %v", err)
	}
	host, err := hosts.GetHost(ctx, identity.RuntimeHostID)
	if err != nil {
		t.Fatal(err)
	}
	capabilityClient := runtimepkg.NewRuntimeCapabilityClient(runtimepkg.HTTPTransportOpenClawClient{HTTPClient: server.Client()})
	if _, err := capabilityClient.GetCapabilities(ctx, host); err != nil {
		t.Fatalf("seed planning capability cache: %v", err)
	}
	scheduler := runtimepkg.NewRuntimeScheduler(hosts, queue.NewMemoryDistributedLockManager())
	manifestProvider := &countedDispatcherManifestProvider{}
	client := &fakeAsyncRuntimeClient{}
	dispatcher := NewAITaskDispatcherWithCapabilityReader(repos, hosts, scheduler, client, manifestProvider, "ticket-secret", "dispatcher-session-secret", "v1", "worker-fresh-submit-binding", capabilityClient)
	dispatcher.RuntimeConfigVersions = runtimepkg.RuntimeConfigVersions{"runtime-fresh-submit-binding": "v1"}
	dispatcher.CapacityProvider = RuntimeCapacityCommandProviderFunc(func(_ context.Context, loadedRun persistence.AgentRunRecord, _ runtimepkg.AgentRunPlan) (runtimepkg.RuntimeCapacityCommand, error) {
		dimension := func(key string) runtimepkg.RuntimeCapacityDimension {
			return runtimepkg.RuntimeCapacityDimension{Key: key, Limit: 1, Requested: 1, Version: 1}
		}
		return runtimepkg.RuntimeCapacityCommand{RunID: loadedRun.AgentRunID, SnapshotVersion: 1, TTL: time.Minute, Dimensions: runtimepkg.RuntimeCapacityDimensions{
			Model: dimension("model:test"), AuthPool: dimension("auth:test"), Tool: dimension("tool:test"), Tenant: dimension("tenant:" + loadedRun.TenantID), User: dimension("user:" + loadedRun.UserID),
		}}, nil
	})

	result := dispatcher.ProcessWithProof(ctx, record, proof, queue.QueueAIRuntimeBackground)
	if result["errorCode"] != "RUNTIME_TOOL_UNAVAILABLE" || result["retryable"] != true {
		t.Fatalf("fresh downgrade result=%#v", result)
	}
	if requests != 2 {
		t.Fatalf("capability requests=%d, want cached planning probe plus uncached final probe", requests)
	}
	if manifestProvider.buildCalls != 0 || client.submitCalls != 0 {
		t.Fatalf("downgraded Host reached materialization=%d submit=%d", manifestProvider.buildCalls, client.submitCalls)
	}
	dispatches, err := hosts.ListDispatchesAdmin(ctx, run.AgentRunID, "", "", 10)
	if err != nil || len(dispatches) != 0 {
		t.Fatalf("downgraded Host reached ticket/dispatch creation: dispatches=%#v err=%v", dispatches, err)
	}
}
