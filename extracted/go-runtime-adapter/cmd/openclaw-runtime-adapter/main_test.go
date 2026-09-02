package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	runtimepkg "huahuoai/backend/source/internal/runtime"
)

func TestAdapterOptionsIsSideEffectFree(t *testing.T) {
	a := &adapter{}
	req := httptest.NewRequest(http.MethodOptions, "/enterprise.runtime.run", nil)
	rec := httptest.NewRecorder()

	a.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("OPTIONS status=%d want 204", rec.Code)
	}
}

func TestRuntimeMethodFromRequestForwardsEventWait(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/enterprise.runtime/runs/run-stream/events?afterSequence=7&limit=40&waitMs=20000", nil)
	method, allowedMethod, params := runtimeMethodFromRequest(req)

	if method != "enterprise.runtime.events" || allowedMethod != http.MethodGet {
		t.Fatalf("route method=%q allowed=%q", method, allowedMethod)
	}
	if params["runId"] != "run-stream" || params["afterSequence"] != int64(7) || params["limit"] != 40 || params["waitMs"] != 20000 {
		t.Fatalf("event params=%#v", params)
	}
}

func TestAdapterCapabilityResponseProjectsSubmitBindingV2(t *testing.T) {
	capabilities := runtimepkg.RuntimeCapabilities{
		CapabilityHash: "capability_submit_binding_v2",
		Tools:          []runtimepkg.ToolCapability{runtimepkg.CanonicalAgentFacingToolCapability("read", "ready")},
		FilesystemPolicy: runtimepkg.RuntimeFilesystemPolicy{
			WorkspaceOnlyReady: true, AbsolutePathRejected: true, SymlinkEscapeRejected: true,
		},
		Abort: runtimepkg.RuntimeAbortCapability{Supported: true, AuthorizationReady: true},
		BudgetCapabilities: runtimepkg.RuntimeBudgetCapabilities{
			MaxToolCallsSupported: 200, DefaultMaxToolCalls: 200,
			SupportsPerRunBudget: true, SupportsBudgetWarning: true, SupportsForcedAbort: true,
			ExecutionContract: runtimepkg.DefaultRuntimeToolBudgetExecutionContract(),
		},
	}
	raw, err := json.Marshal(capabilities)
	if err != nil {
		t.Fatal(err)
	}
	a := &adapter{timeout: time.Second, invoke: func(_ context.Context, method string, _ map[string]any, _ time.Duration) ([]byte, []byte, error) {
		if method != "enterprise.runtime.capabilities" {
			t.Fatalf("method=%q", method)
		}
		return raw, nil, nil
	}}
	req := httptest.NewRequest(http.MethodGet, "/enterprise.runtime/capabilities", nil)
	rec := httptest.NewRecorder()
	a.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	var got runtimepkg.RuntimeCapabilities
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if err := runtimepkg.ValidateRuntimeSubmitBindingCapability(got.SubmitBinding); err != nil {
		t.Fatalf("projected submit binding=%+v error=%v", got.SubmitBinding, err)
	}
}

func TestAdapterPreservesDurableStoreUnavailable(t *testing.T) {
	code := adapterRuntimeErrorCode(fmt.Errorf("gateway rejected: RUNTIME_STORAGE_UNAVAILABLE"), nil)
	if code != "RUNTIME_STORAGE_UNAVAILABLE" {
		t.Fatalf("code=%s", code)
	}
	if status := adapterRuntimeHTTPStatus(code); status != http.StatusServiceUnavailable {
		t.Fatalf("status=%d", status)
	}
}

func TestAdapterPreservesRuntimeHostIdentityRejection(t *testing.T) {
	code := adapterRuntimeErrorCode(fmt.Errorf("local admission rejected: RUNTIME_HOST_UNAUTHORIZED"), nil)
	if code != "RUNTIME_HOST_UNAUTHORIZED" {
		t.Fatalf("code=%s", code)
	}
	if status := adapterRuntimeHTTPStatus(code); status != http.StatusForbidden {
		t.Fatalf("status=%d", status)
	}
}

func TestProductionAdapterAdmissionStaysClosedUntilExplicitRecovery(t *testing.T) {
	for _, name := range []string{
		"HUAHUO_RUNTIME_HOST_HEARTBEAT_SIGNING_KEY_FILE",
		"HUAHUO_RUNTIME_HOST_HEARTBEAT_SIGNING_KEY_REF",
		"HUAHUO_RUNTIME_HOST_HEARTBEAT_KEY_ID",
		"HUAHUO_RUNTIME_HOST_BACKEND_TRUST_FILE",
		"HUAHUO_RUNTIME_HOST_BACKEND_CLIENT_MTLS_CERT_FILE",
		"HUAHUO_RUNTIME_HOST_BACKEND_CLIENT_MTLS_KEY_FILE",
		"HUAHUO_RUNTIME_HOST_MTLS_TRUST_REF",
		"HUAHUO_RUNTIME_HOST_MTLS_CERT_REF",
		"HUAHUO_RUNTIME_HOST_MTLS_KEY_REF",
		"HUAHUO_RUNTIME_RUN_TICKET_JTI_STORE_DIR",
	} {
		t.Setenv(name, "")
	}
	t.Setenv("HUAHUO_ENV", "production")
	adapter := newAdapterFromEnv()
	if _, err := adapter.acquireRunPermit("run-before-recovery", "product_thread"); err == nil || err.Error() != "RUNTIME_CAPACITY_UNAVAILABLE" {
		t.Fatalf("production admission before recovery error=%v", err)
	}
	adapter.admission.MarkReadyAfterRecovery()
	permit, err := adapter.acquireRunPermit("run-after-recovery", "product_thread")
	if err != nil || !permit.Acquired {
		t.Fatalf("explicit recovery did not reopen admission permit=%+v err=%v", permit, err)
	}
	adapter.releaseRunPermit(permit.RunID)
	adapter.blockRuntimeHostDispatch()
	adapter.admission.MarkReadyAfterRecovery()
	if _, err := adapter.acquireRunPermit("run-after-identity-block", "product_thread"); err == nil || err.Error() != "RUNTIME_HOST_UNAUTHORIZED" {
		t.Fatalf("identity block was reopened by recovery marker error=%v", err)
	}
}

func TestAdapterProductionSourceHasNoPerRunCLIOrSessionJSONLPath(t *testing.T) {
	for _, name := range []string{"main.go", "gateway_client.go", "runtime_host.go", "host_registration.go"} {
		raw, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		text := string(raw)
		for _, forbidden := range []string{"exec.CommandContext", "HUAHUO_OPENCLAW_CLI_DIR", "HUAHUO_OPENCLAW_CLI_NODE", "HUAHUO_OPENCLAW_CLI_ENTRY", "sessions.json", "chat.abort"} {
			if strings.Contains(text, forbidden) {
				t.Fatalf("%s contains forbidden legacy runtime token %q", name, forbidden)
			}
		}
	}
}

func TestAdapterAsyncSubmitMaterializesOnceAndForwardsImmutableSpec(t *testing.T) {
	tmpRoot := t.TempDir()
	secret := strings.Repeat("r", 32)
	expiresAt := time.Now().UTC().Add(5 * time.Minute).Truncate(time.Second)
	manifest := runtimepkg.RuntimeInputManifest{
		SchemaVersion: "runtime_input_manifest.v1", RunID: "run_async_adapter", RuntimeHostID: "host_test",
		TenantID: "tenant_test", UserID: "user_test", WorkspaceID: "workspace_test",
		WorkspaceVersion: 3, ThreadWorkspaceBindingVersion: 2, ContextGeneration: 4,
		MetaRelease: "release-v5", AgentProfile: "general_agent",
		AgentHash:      strings.Repeat("a", 64),
		SkillProfiles:  []runtimepkg.RuntimeSkillProfile{{Profile: "general_chat", Hash: strings.Repeat("b", 64)}},
		CapabilityHash: "capability-v5", Files: []runtimepkg.RuntimeManifestEntry{runtimepkg.NewInlineRuntimeEntry("input/request.md", []byte("hello"))},
		ExpiresAt: expiresAt,
	}
	manifest.ManifestHash = (runtimepkg.WorkspaceComposer{}).ComputeManifestHash(manifest)
	plan := runtimepkg.AgentRunPlan{
		AgentRunID: manifest.RunID, TaskType: "general_chat", ExecutionScope: "product_thread", L1AgentProfile: "general_agent",
		RuntimeConfigID: "huahuo-default",
		RequiredTools:   []string{"read", "workspace_search"}, OutputContract: map[string]any{"format": "markdown"}, ToolBudget: testRuntimeToolBudget(),
	}
	planHash, err := runtimepkg.ComputeAgentRunPlanHash(plan)
	if err != nil {
		t.Fatalf("ComputeAgentRunPlanHash: %v", err)
	}
	ticket, err := runtimepkg.SignRunTicket(runtimepkg.RunTicketClaims{
		RunID: manifest.RunID, TenantID: manifest.TenantID, ReservationID: "reservation_1", RuntimeHostID: manifest.RuntimeHostID,
		CapabilityHash: manifest.CapabilityHash, WorkspaceID: manifest.WorkspaceID,
		WorkspaceVersion: manifest.WorkspaceVersion, ContextGeneration: manifest.ContextGeneration,
		InputManifestHash: manifest.ManifestHash, PlanHash: planHash,
		SubmitBinding: &runtimepkg.RunTicketSubmitBinding{Version: runtimepkg.RuntimeSubmitBindingV2, InputMessageHash: runtimepkg.RunTicketInputMessageHash("hello from app"), RuntimeConfigID: "huahuo-default", RuntimeConfigVersion: "v1", ProductSessionHash: runtimepkg.RunTicketProductSessionHash("thread_test", "oc:session:test")},
		FencingToken:  7, JTI: "ticket_jti_1",
		IssuedAt: time.Now().UTC().Add(-time.Second).Unix(), ExpiresAt: expiresAt.Unix(),
	}, secret)
	if err != nil {
		t.Fatalf("SignRunTicket: %v", err)
	}
	var gotParams map[string]any
	callCount := 0
	a := &adapter{
		timeout: time.Second, runtimeHostID: manifest.RuntimeHostID, adapterVersion: "v0.5", runTicketSecret: secret, runtimePolicyKeyID: "runtime-policy-v1",
		runtimeTmpRoot: tmpRoot, defaultRuntimeConfig: "huahuo-default",
		maxActiveRuns: 2, maxProductThreadRuns: 1, maxDetachedTaskRuns: 1,
		invoke: func(_ context.Context, method string, params map[string]any, _ time.Duration) ([]byte, []byte, error) {
			if method != "enterprise.runtime.submit" {
				t.Fatalf("method=%s", method)
			}
			callCount++
			gotParams = params
			return []byte(`{"runId":"run_async_adapter","status":"accepted","runtimeRequestId":"request_1","acceptedSequence":1}`), nil, nil
		},
	}
	a.admission = NewHostAdmissionController(a.maxActiveRuns, a.maxProductThreadRuns, a.maxDetachedTaskRuns)
	a.materializer = runtimepkg.NewRuntimeWorkspaceMaterializer(manifest.RuntimeHostID, tmpRoot, secret, newHostManifestSourceResolver(), runtimepkg.NewMemoryRunTicketJTIStore())
	request := runtimepkg.AsyncRuntimeSubmitRequest{
		RunID: manifest.RunID, ReservationID: "reservation_1", FencingToken: 7,
		CapabilityHash: manifest.CapabilityHash, InputMessage: "hello from app",
		RuntimeConfigID: "huahuo-default", RuntimeConfigVersion: "v1", InputManifest: manifest,
		Plan: plan,
		ProductSessionRef: map[string]any{
			"threadId": "thread_test", "openclawSessionKey": "oc:session:test",
			"metadata": map[string]any{
				"promptKey": "must_not_cross_adapter", "agentKey": "must_not_cross_adapter",
				"workspaceRef": "workspace:must_not_cross_adapter", "outputContract": map[string]any{"format": "hidden"},
			},
		},
	}
	raw, _ := json.Marshal(request)
	lastResponseBody := ""
	for attempt := 0; attempt < 2; attempt++ {
		req := httptest.NewRequest(http.MethodPost, "/enterprise.runtime/runs", strings.NewReader(string(raw)))
		req.Header.Set("Authorization", "RunTicket "+ticket)
		rec := httptest.NewRecorder()
		a.ServeHTTP(rec, req)
		lastResponseBody = rec.Body.String()
		if rec.Code != http.StatusOK {
			t.Fatalf("attempt=%d status=%d body=%s", attempt, rec.Code, rec.Body.String())
		}
	}
	if callCount != 2 {
		t.Fatalf("gateway calls=%d want 2 idempotent submits", callCount)
	}
	spec := mapValue(gotParams["spec"])
	workspace := mapValue(spec["workspace"])
	materializedRoot := stringValue(workspace["realPath"])
	wantRoot := filepath.Join(tmpRoot, "runtime-workspaces", manifest.RunID)
	if materializedRoot != wantRoot {
		t.Fatalf("materialized root=%s want=%s", materializedRoot, wantRoot)
	}
	if workspace["accessMode"] != runtimepkg.RuntimeWorkspaceAccessRead || workspace["writeLease"] != nil {
		t.Fatalf("read-only plan must not expose native write: %#v", workspace)
	}
	if _, err := os.Stat(filepath.Join(wantRoot, "input", "request.md")); err != nil {
		t.Fatalf("materialized input missing: %v", err)
	}
	if spec["tenantId"] != manifest.TenantID || spec["userId"] != manifest.UserID || spec["runtimeConfigId"] != "huahuo-default" || spec["runtimeConfigVersion"] != "v1" || mapValue(spec["input"])["message"] != "hello from app" {
		t.Fatalf("immutable runtime spec mismatch: %#v", spec)
	}
	productSession := mapValue(spec["productSession"])
	if len(productSession) != 2 || productSession["threadId"] != "thread_test" || productSession["openclawSessionKey"] != "oc:session:test" {
		t.Fatalf("Gateway product session must retain only the stable Backend-owned identity: %#v", productSession)
	}
	for _, forbidden := range []string{"metadata", "promptKey", "agentKey", "workspaceRef", "outputContract", "must_not_cross_adapter"} {
		if strings.Contains(fmt.Sprint(productSession), forbidden) {
			t.Fatalf("Gateway product session leaked internal metadata %q: %#v", forbidden, productSession)
		}
	}
	tools := mapValue(spec["tools"])
	if !runtimeStringSliceEqual(tools["allow"], plan.RequiredTools) {
		t.Fatalf("Runtime tool allow-list mismatch: %#v", tools)
	}
	policy := mapValue(spec["runtimePolicy"])
	if len(policy) == 0 {
		t.Fatalf("Gateway request must retain the signed runtime policy: %#v", spec)
	}
	if !runtimeStringSliceEqual(policy["requiredTools"], plan.RequiredTools) || !runtimeStringSliceEqual(policy["allowedTools"], plan.RequiredTools) {
		t.Fatalf("Gateway runtime policy tools mismatch: %#v", policy)
	}
	if policy["runId"] != manifest.RunID || policy["idempotencyKey"] != manifest.RunID || policy["workspaceManifestHash"] != manifest.ManifestHash ||
		policy["capabilityHash"] != manifest.CapabilityHash || policy["planHash"] != planHash || stringValue(policy["signature"]) == "" {
		t.Fatalf("Gateway runtime policy bindings/signature mismatch: %#v", policy)
	}
	writeLease, writeLeaseOK := policy["writeLease"].(*runtimepkg.RuntimeWorkspaceWriteLease)
	if policy["workspaceAccessMode"] != runtimepkg.RuntimeWorkspaceAccessRead || !writeLeaseOK || writeLease != nil {
		t.Fatalf("signed policy must bind read-only Workspace access: %#v", policy)
	}
	if gotParams["runTicketJtiHash"] != adapterStableSHA256("ticket_jti_1") {
		t.Fatalf("run ticket jti hash mismatch: %#v", gotParams["runTicketJtiHash"])
	}
	wantDispatchIdentity := adapterStableSHA256(fmt.Sprintf("%s\x00%s\x00%d\x00%s\x00%s", manifest.RunID, "reservation_1", 7, manifest.RuntimeHostID, manifest.CapabilityHash))
	if gotParams["dispatchIdentity"] != wantDispatchIdentity {
		t.Fatalf("dispatch identity mismatch: %#v", gotParams["dispatchIdentity"])
	}
	if policy["dispatchIdentity"] != wantDispatchIdentity {
		t.Fatalf("Gateway runtime policy dispatch identity mismatch: %#v", policy)
	}
	privateContext := mapValue(gotParams["privateExecutionContext"])
	if privateContext["version"] != privateExecutionContextVersion || privateContext["runTicket"] != ticket || len(privateContext) != 2 {
		t.Fatalf("Gateway private execution context mismatch: %#v", privateContext)
	}
	specJSON, _ := json.Marshal(spec)
	policyJSON, _ := json.Marshal(policy)
	forwardedJSON, _ := json.Marshal(gotParams)
	if strings.Contains(string(specJSON), ticket) || strings.Contains(string(policyJSON), ticket) || strings.Contains(lastResponseBody, ticket) ||
		strings.Count(string(forwardedJSON), ticket) != 1 || strings.Contains(string(specJSON), "privateExecutionContext") || strings.Contains(string(policyJSON), "runTicket") {
		t.Fatalf("raw RunTicket escaped private Gateway envelope: spec=%s policy=%s response=%s params=%s", specJSON, policyJSON, lastResponseBody, forwardedJSON)
	}
	if got := a.activeRunCount(); got != 1 {
		t.Fatalf("active run permits=%d want 1", got)
	}
	for name, mutate := range map[string]func(*runtimepkg.AsyncRuntimeSubmitRequest){
		"input message": func(candidate *runtimepkg.AsyncRuntimeSubmitRequest) {
			candidate.InputMessage = "changed after ticket signing"
		},
		"runtime config id":      func(candidate *runtimepkg.AsyncRuntimeSubmitRequest) { candidate.RuntimeConfigID = "runtime-other" },
		"runtime config version": func(candidate *runtimepkg.AsyncRuntimeSubmitRequest) { candidate.RuntimeConfigVersion = "v2" },
		"product session thread": func(candidate *runtimepkg.AsyncRuntimeSubmitRequest) {
			candidate.ProductSessionRef["threadId"] = "thread_other"
		},
		"product session key": func(candidate *runtimepkg.AsyncRuntimeSubmitRequest) {
			candidate.ProductSessionRef["openclawSessionKey"] = "oc:session:other"
		},
	} {
		t.Run("rejects unsigned "+name+" override", func(t *testing.T) {
			candidate := request
			candidate.ProductSessionRef = map[string]any{}
			for key, value := range request.ProductSessionRef {
				candidate.ProductSessionRef[key] = value
			}
			mutate(&candidate)
			candidateRaw, _ := json.Marshal(candidate)
			candidateRequest := httptest.NewRequest(http.MethodPost, "/enterprise.runtime/runs", strings.NewReader(string(candidateRaw)))
			candidateRequest.Header.Set("Authorization", "RunTicket "+ticket)
			candidateResponse := httptest.NewRecorder()
			a.ServeHTTP(candidateResponse, candidateRequest)
			if candidateResponse.Code != http.StatusForbidden || callCount != 2 {
				t.Fatalf("%s status=%d gateway calls=%d body=%s", name, candidateResponse.Code, callCount, candidateResponse.Body.String())
			}
		})
	}
	for name, mutate := range map[string]func(*runtimepkg.AsyncRuntimeSubmitRequest){
		"missing runtime config version": func(candidate *runtimepkg.AsyncRuntimeSubmitRequest) { candidate.RuntimeConfigVersion = "" },
		"oversized signed input": func(candidate *runtimepkg.AsyncRuntimeSubmitRequest) {
			candidate.InputMessage = strings.Repeat("x", adapterInputMaxRunes()+1)
		},
	} {
		t.Run("rejects invalid "+name, func(t *testing.T) {
			candidate := request
			mutate(&candidate)
			candidateRaw, _ := json.Marshal(candidate)
			candidateRequest := httptest.NewRequest(http.MethodPost, "/enterprise.runtime/runs", strings.NewReader(string(candidateRaw)))
			candidateRequest.Header.Set("Authorization", "RunTicket "+ticket)
			candidateResponse := httptest.NewRecorder()
			a.ServeHTTP(candidateResponse, candidateRequest)
			if candidateResponse.Code != http.StatusBadRequest || callCount != 2 {
				t.Fatalf("%s status=%d gateway calls=%d body=%s", name, candidateResponse.Code, callCount, candidateResponse.Body.String())
			}
		})
	}
	legacyTicket, err := runtimepkg.SignRunTicket(runtimepkg.RunTicketClaims{
		RunID: manifest.RunID, TenantID: manifest.TenantID, ReservationID: "reservation_1", RuntimeHostID: manifest.RuntimeHostID,
		CapabilityHash: manifest.CapabilityHash, WorkspaceID: manifest.WorkspaceID, WorkspaceVersion: manifest.WorkspaceVersion,
		ContextGeneration: manifest.ContextGeneration, InputManifestHash: manifest.ManifestHash, PlanHash: planHash,
		FencingToken: 7, JTI: "ticket_jti_legacy_submit", IssuedAt: time.Now().UTC().Add(-time.Second).Unix(), ExpiresAt: expiresAt.Unix(),
	}, secret)
	if err != nil {
		t.Fatal(err)
	}
	legacyRequest := httptest.NewRequest(http.MethodPost, "/enterprise.runtime/runs", strings.NewReader(string(raw)))
	legacyRequest.Header.Set("Authorization", "RunTicket "+legacyTicket)
	legacyResponse := httptest.NewRecorder()
	a.ServeHTTP(legacyResponse, legacyRequest)
	if legacyResponse.Code != http.StatusForbidden || callCount != 2 {
		t.Fatalf("legacy submit status=%d gateway calls=%d body=%s", legacyResponse.Code, callCount, legacyResponse.Body.String())
	}
	rejectedGatewayCalls := 0
	a.invoke = func(_ context.Context, method string, params map[string]any, _ time.Duration) ([]byte, []byte, error) {
		rejectedGatewayCalls++
		if method != "enterprise.runtime.submit" || len(mapValue(mapValue(params["spec"])["runtimePolicy"])) == 0 {
			t.Fatalf("Gateway rejection path lost signed policy: method=%s params=%#v", method, params)
		}
		return nil, []byte("gateway schema does not support runtimePolicy"), fmt.Errorf("gateway schema does not support runtimePolicy")
	}
	rejectedRequest := httptest.NewRequest(http.MethodPost, "/enterprise.runtime/runs", strings.NewReader(string(raw)))
	rejectedRequest.Header.Set("Authorization", "RunTicket "+ticket)
	rejectedResponse := httptest.NewRecorder()
	a.ServeHTTP(rejectedResponse, rejectedRequest)
	if rejectedResponse.Code != http.StatusServiceUnavailable || rejectedGatewayCalls != 1 {
		t.Fatalf("policy-rejected gateway status=%d calls=%d body=%s", rejectedResponse.Code, rejectedGatewayCalls, rejectedResponse.Body.String())
	}
	var rejectedBody map[string]any
	if err := json.Unmarshal(rejectedResponse.Body.Bytes(), &rejectedBody); err != nil || rejectedBody["errorCode"] != "RUNTIME_TOOL_BUDGET_UNSUPPORTED" {
		t.Fatalf("policy-rejected gateway response=%#v err=%v", rejectedBody, err)
	}
	for name, mutate := range map[string]func(*runtimepkg.AsyncRuntimeSubmitRequest){
		"required tools": func(candidate *runtimepkg.AsyncRuntimeSubmitRequest) {
			candidate.Plan.RequiredTools = append([]string(nil), candidate.Plan.RequiredTools...)
			candidate.Plan.RequiredTools = append(candidate.Plan.RequiredTools, "write")
		},
		"agent relative root": func(candidate *runtimepkg.AsyncRuntimeSubmitRequest) {
			candidate.Plan.AgentRelativeRoot = "agents/tampered"
		},
		"terminal output identity": func(candidate *runtimepkg.AsyncRuntimeSubmitRequest) {
			candidate.Plan.TerminalOutput.Format = "markdown"
		},
		"workspace context manifest hash": func(candidate *runtimepkg.AsyncRuntimeSubmitRequest) {
			candidate.Plan.WorkspaceContextManifestHash = "sha256:" + strings.Repeat("c", 64)
		},
	} {
		t.Run("rejects full plan hash tamper "+name, func(t *testing.T) {
			tampered := request
			mutate(&tampered)
			tamperedRaw, _ := json.Marshal(tampered)
			tamperedRequest := httptest.NewRequest(http.MethodPost, "/enterprise.runtime/runs", strings.NewReader(string(tamperedRaw)))
			tamperedRequest.Header.Set("Authorization", "RunTicket "+ticket)
			tamperedResponse := httptest.NewRecorder()
			a.ServeHTTP(tamperedResponse, tamperedRequest)
			if tamperedResponse.Code != http.StatusForbidden || callCount != 2 || rejectedGatewayCalls != 1 {
				t.Fatalf("tampered plan %s status=%d materialized gateway calls=%d rejected gateway calls=%d body=%s", name, tamperedResponse.Code, callCount, rejectedGatewayCalls, tamperedResponse.Body.String())
			}
		})
	}

	storageUnavailable := &adapter{
		timeout:              a.timeout,
		runtimeHostID:        a.runtimeHostID,
		adapterVersion:       a.adapterVersion,
		runTicketSecret:      a.runTicketSecret,
		runtimePolicyKeyID:   a.runtimePolicyKeyID,
		runtimeTmpRoot:       a.runtimeTmpRoot,
		defaultRuntimeConfig: a.defaultRuntimeConfig,
		maxActiveRuns:        a.maxActiveRuns,
		maxProductThreadRuns: a.maxProductThreadRuns,
		maxDetachedTaskRuns:  a.maxDetachedTaskRuns,
		admission:            NewHostAdmissionController(2, 1, 1),
	}
	storageUnavailable.materializer = runtimepkg.NewRuntimeWorkspaceMaterializer(
		manifest.RuntimeHostID, tmpRoot, secret, newHostManifestSourceResolver(), runtimepkg.NewUnavailableRunTicketJTIStore(),
	)
	storageUnavailable.invoke = func(context.Context, string, map[string]any, time.Duration) ([]byte, []byte, error) {
		t.Fatal("Gateway must not be invoked when the durable JTI store is unavailable")
		return nil, nil, nil
	}
	storageRequest := httptest.NewRequest(http.MethodPost, "/enterprise.runtime/runs", strings.NewReader(string(raw)))
	storageRequest.Header.Set("Authorization", "RunTicket "+ticket)
	storageResponse := httptest.NewRecorder()
	storageUnavailable.ServeHTTP(storageResponse, storageRequest)
	if storageResponse.Code != http.StatusServiceUnavailable {
		t.Fatalf("durable-store status=%d body=%s", storageResponse.Code, storageResponse.Body.String())
	}
	var storageBody map[string]any
	if err := json.Unmarshal(storageResponse.Body.Bytes(), &storageBody); err != nil || storageBody["errorCode"] != "RUNTIME_STORAGE_UNAVAILABLE" {
		t.Fatalf("durable-store response=%#v err=%v", storageBody, err)
	}

	emptyPlan := plan
	emptyPlan.RequiredTools = []string{}
	emptyPlanHash, err := runtimepkg.ComputeAgentRunPlanHash(emptyPlan)
	if err != nil {
		t.Fatalf("empty-plan hash: %v", err)
	}
	emptyTicket, err := runtimepkg.SignRunTicket(runtimepkg.RunTicketClaims{
		RunID: manifest.RunID, TenantID: manifest.TenantID, ReservationID: "reservation_1", RuntimeHostID: manifest.RuntimeHostID,
		CapabilityHash: manifest.CapabilityHash, WorkspaceID: manifest.WorkspaceID,
		WorkspaceVersion: manifest.WorkspaceVersion, ContextGeneration: manifest.ContextGeneration,
		InputManifestHash: manifest.ManifestHash, PlanHash: emptyPlanHash,
		SubmitBinding: &runtimepkg.RunTicketSubmitBinding{Version: runtimepkg.RuntimeSubmitBindingV2, InputMessageHash: runtimepkg.RunTicketInputMessageHash(request.InputMessage), RuntimeConfigID: request.RuntimeConfigID, RuntimeConfigVersion: request.RuntimeConfigVersion, ProductSessionHash: runtimepkg.RunTicketProductSessionHash("thread_test", "oc:session:test")},
		FencingToken:  7, JTI: "ticket_jti_empty_allow",
		IssuedAt: time.Now().UTC().Add(-time.Second).Unix(), ExpiresAt: expiresAt.Unix(),
	}, secret)
	if err != nil {
		t.Fatalf("empty-plan ticket: %v", err)
	}
	emptyRequest := request
	emptyRequest.Plan = emptyPlan
	emptyRaw, _ := json.Marshal(emptyRequest)
	emptyParams := map[string]any{}
	if err := json.Unmarshal(emptyRaw, &emptyParams); err != nil {
		t.Fatal(err)
	}
	emptySubmit, _, err := a.prepareAsyncSubmit(context.Background(), "RunTicket "+emptyTicket, emptyParams)
	if err != nil {
		t.Fatalf("empty allow-list submit: %v", err)
	}
	emptySpec := mapValue(emptySubmit["spec"])
	emptyAllow, ok := mapValue(emptySpec["tools"])["allow"].([]string)
	if !ok || emptyAllow == nil || len(emptyAllow) != 0 {
		t.Fatalf("spec.tools.allow must be explicit empty list: %#v", emptySpec["tools"])
	}
	emptyPolicy := mapValue(emptySpec["runtimePolicy"])
	if !runtimeStringSliceEqual(emptyPolicy["requiredTools"], []string{}) || !runtimeStringSliceEqual(emptyPolicy["allowedTools"], []string{}) {
		t.Fatalf("empty Runtime policy tool lists must be explicit: %#v", emptyPolicy)
	}
	if budget, ok := emptyPolicy["toolBudget"].(runtimepkg.RuntimeToolBudget); !ok || budget != emptyPlan.ToolBudget {
		t.Fatalf("empty Runtime policy budget mismatch: %#v", emptyPolicy["toolBudget"])
	}
}

func testRuntimeToolBudget() runtimepkg.RuntimeToolBudget {
	return runtimepkg.RuntimeToolBudget{
		MaxToolCalls: 200, SoftToolCallLimit: 160, FinalizationReserve: 10,
		MaxRepeatedCalls: 2, MaxNoProgressCalls: 4,
		MaxSearchCalls: 60, MaxWriteCalls: 20, MaxReadBytes: 64 * 1024 * 1024, MaxWallTimeSeconds: 1800,
	}
}

func runtimeStringSliceEqual(value any, want []string) bool {
	items, ok := value.([]string)
	if !ok || len(items) != len(want) {
		return false
	}
	for index := range want {
		if items[index] != want[index] {
			return false
		}
	}
	return true
}

func TestAdapterLegacyRuntimeSpecDropsCallerToolPolicy(t *testing.T) {
	normalized, err := (&adapter{}).normalizeFullRuntimeSpec(map[string]any{
		"runId": "run_legacy_tools", "tenantId": "tenant_test", "userId": "user_test", "workspaceId": "workspace_test",
		"threadId": "thread_test", "runtimeConfigId": "huahuo-default",
		"workspace":      map[string]any{"realPath": "/workspace", "accessMode": "read"},
		"productSession": map[string]any{"threadId": "thread_test", "openclawSessionKey": "session_test"},
		"input":          map[string]any{"message": "hello"},
		"tools":          map[string]any{"allow": []string{"read", "ls", "find", "grep"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, exists := normalized["tools"]; exists {
		t.Fatalf("legacy caller tool policy leaked to Runtime: %#v", normalized["tools"])
	}
}

func TestAdapterLegacyRuntimeSpecRejectsUnsignedWrite(t *testing.T) {
	_, err := (&adapter{}).normalizeFullRuntimeSpec(map[string]any{
		"runId": "run_legacy_write", "tenantId": "tenant_test", "userId": "user_test", "workspaceId": "workspace_test",
		"threadId": "thread_test", "runtimeConfigId": "huahuo-default",
		"workspace":      map[string]any{"realPath": "/workspace", "accessMode": "write"},
		"productSession": map[string]any{"threadId": "thread_test", "openclawSessionKey": "session_test"},
		"input":          map[string]any{"message": "hello"},
	})
	if err == nil || !strings.Contains(err.Error(), "unsigned legacy runtime write is forbidden") {
		t.Fatalf("unsigned compatibility write error=%v", err)
	}
}

func TestAdapterAsyncStatusRejectsTicketForAnotherRun(t *testing.T) {
	secret := "runtime-ticket-test-secret"
	ticket, err := runtimepkg.SignRunTicket(runtimepkg.RunTicketClaims{
		RunID: "run_allowed", TenantID: "tenant_test", ReservationID: "reservation_1", RuntimeHostID: "host_test",
		CapabilityHash: "cap", WorkspaceID: "workspace_test", WorkspaceVersion: 1, ContextGeneration: 1,
		InputManifestHash: "sha256:" + strings.Repeat("a", 64), PlanHash: "sha256:" + strings.Repeat("b", 64), FencingToken: 1, JTI: "ticket_jti_status",
		IssuedAt: time.Now().Add(-time.Second).Unix(), ExpiresAt: time.Now().Add(time.Minute).Unix(),
	}, secret)
	if err != nil {
		t.Fatalf("SignRunTicket: %v", err)
	}
	called := false
	a := &adapter{runtimeHostID: "host_test", runTicketSecret: secret, invoke: func(context.Context, string, map[string]any, time.Duration) ([]byte, []byte, error) {
		called = true
		return []byte(`{}`), nil, nil
	}}
	req := httptest.NewRequest(http.MethodGet, "/enterprise.runtime/runs/run_other", nil)
	req.Header.Set("Authorization", "RunTicket "+ticket)
	rec := httptest.NewRecorder()
	a.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden || called {
		t.Fatalf("status=%d called=%v body=%s", rec.Code, called, rec.Body.String())
	}
}

func TestAdapterAsyncAbortBindsReservationAndFencingToRunTicket(t *testing.T) {
	secret := "runtime-ticket-test-secret"
	ticket, err := runtimepkg.SignRunTicket(runtimepkg.RunTicketClaims{
		RunID: "run_abort", TenantID: "tenant_test", ReservationID: "reservation_allowed", RuntimeHostID: "host_test",
		CapabilityHash: "cap", WorkspaceID: "workspace_test", WorkspaceVersion: 1, ContextGeneration: 1,
		InputManifestHash: "sha256:" + strings.Repeat("a", 64), PlanHash: "sha256:" + strings.Repeat("b", 64), FencingToken: 17, JTI: "ticket_jti_abort",
		IssuedAt: time.Now().Add(-time.Second).Unix(), ExpiresAt: time.Now().Add(time.Minute).Unix(),
	}, secret)
	if err != nil {
		t.Fatalf("SignRunTicket: %v", err)
	}
	callCount := 0
	a := &adapter{
		runtimeHostID: "host_test", runTicketSecret: secret,
		invoke: func(_ context.Context, method string, params map[string]any, _ time.Duration) ([]byte, []byte, error) {
			callCount++
			if method != "enterprise.runtime.abort" {
				t.Fatalf("method=%q", method)
			}
			if params["runId"] != "run_abort" || params["reservationId"] != "reservation_allowed" || params["fencingToken"] != int64(17) || params["reason"] != "user_cancelled" {
				t.Fatalf("abort params=%#v", params)
			}
			return []byte(`{"runId":"run_abort","status":"aborting"}`), nil, nil
		},
	}
	request := func(body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/enterprise.runtime/runs/run_abort/abort", strings.NewReader(body))
		req.Header.Set("Authorization", "RunTicket "+ticket)
		rec := httptest.NewRecorder()
		a.ServeHTTP(rec, req)
		return rec
	}

	valid := request(`{"reservationId":"reservation_allowed","fencingToken":17,"reason":"user_cancelled"}`)
	if valid.Code != http.StatusOK || callCount != 1 {
		t.Fatalf("valid abort status=%d calls=%d body=%s", valid.Code, callCount, valid.Body.String())
	}
	for _, body := range []string{
		`{"runId":"run_other","reservationId":"reservation_allowed","fencingToken":17,"reason":"user_cancelled"}`,
		`{"reservationId":"reservation_tampered","fencingToken":17,"reason":"user_cancelled"}`,
		`{"reservationId":"reservation_allowed","fencingToken":18,"reason":"user_cancelled"}`,
	} {
		rec := request(body)
		if rec.Code != http.StatusForbidden || callCount != 1 {
			t.Fatalf("tampered abort status=%d calls=%d body=%s", rec.Code, callCount, rec.Body.String())
		}
		var response map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil || response["errorCode"] != "RUNTIME_PERMISSION_DENIED" {
			t.Fatalf("tampered abort response=%#v err=%v", response, err)
		}
	}
}

func TestAdapterPostStripsTransportMethodAndReturnsGatewayJSON(t *testing.T) {
	var gotMethod string
	var gotParams map[string]any
	a := &adapter{
		timeout:              time.Second,
		tenantID:             "tenant_test",
		defaultRuntimeConfig: "huahuo-default",
		runtimeConfigPath:    "/runtime/config.json",
		runtimeStateDir:      "/runtime/state",
		runtimeLogsDir:       "/runtime/logs",
		runtimeTmpRoot:       "/runtime/tmp",
		invoke: func(_ context.Context, method string, params map[string]any, _ time.Duration) ([]byte, []byte, error) {
			gotMethod = method
			gotParams = params
			return []byte(`{"status":"succeeded","finalAnswer":"ok"}`), nil, nil
		},
	}
	req := httptest.NewRequest(http.MethodPost, "/enterprise.runtime.run", strings.NewReader(`{"method":"enterprise.runtime.run","runId":"run_1","tenantId":"tenant_test","userId":"user_test","workspaceId":"workspace_test","threadId":"thread_test","runtimeConfigId":"huahuo-default","workspace":{"realPath":"/workspace","accessMode":"read"},"productSession":{"threadId":"thread_test","openclawSessionKey":"runtime:tenant:tenant_test:user:user_test:workspace:workspace_test:thread:thread_test"},"input":{"message":"hello"},"workspaceDir":"/legacy/workspace","inputMessage":"legacy hello","authPoolId":"legacy-pool","timeoutSec":1}`))
	rec := httptest.NewRecorder()

	a.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("POST status=%d body=%s", rec.Code, rec.Body.String())
	}
	if gotMethod != "enterprise.runtime.run" {
		t.Fatalf("method=%q", gotMethod)
	}
	if _, exists := gotParams["method"]; exists {
		t.Fatalf("transport method leaked into gateway params: %#v", gotParams)
	}
	for _, key := range []string{"workspaceDir", "inputMessage", "authPoolId", "timeoutSec"} {
		if _, exists := gotParams[key]; exists {
			t.Fatalf("legacy/transport key %s leaked into OpenClaw params: %#v", key, gotParams)
		}
	}
	if gotParams["runId"] != "run_1" {
		t.Fatalf("params=%#v", gotParams)
	}
	if gotParams["runtimeConfigId"] != "huahuo-default" || gotParams["tenantId"] != "tenant_test" {
		t.Fatalf("full RuntimeRunSpec params not preserved: %#v", gotParams)
	}
	workspace := gotParams["workspace"].(map[string]any)
	input := gotParams["input"].(map[string]any)
	runtimeBody := gotParams["runtime"].(map[string]any)
	if workspace["realPath"] != "/workspace" || input["message"] != "hello" {
		t.Fatalf("full RuntimeRunSpec was overwritten by legacy fields: %#v", gotParams)
	}
	if runtimeBody["configPath"] != "/runtime/config.json" || runtimeBody["stateDir"] != "/runtime/state" || runtimeBody["logsDir"] != "/runtime/logs" || runtimeBody["tmpRoot"] != "/runtime/tmp" {
		t.Fatalf("runtime defaults not injected into full RuntimeRunSpec: %#v", runtimeBody)
	}
}

func TestAdapterProductionRejectsLegacySyncRuntimeRunBeforeGateway(t *testing.T) {
	called := false
	a := &adapter{
		runtimeEnvironment: "prelaunch",
		invoke: func(context.Context, string, map[string]any, time.Duration) ([]byte, []byte, error) {
			called = true
			return nil, nil, nil
		},
	}
	req := httptest.NewRequest(http.MethodPost, "/enterprise.runtime.run", strings.NewReader(`{"runId":"legacy_run"}`))
	rec := httptest.NewRecorder()

	a.ServeHTTP(rec, req)

	if rec.Code != http.StatusGone || called {
		t.Fatalf("legacy production status=%d called=%v body=%s", rec.Code, called, rec.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body["errorCode"] != "RUNTIME_LEGACY_CONTRACT_DISABLED" {
		t.Fatalf("legacy production body=%#v", body)
	}
}

func TestAdapterUsesRuntimeTimeoutHeaderWithoutLeakingItToGatewayParams(t *testing.T) {
	var gotTimeout time.Duration
	var gotParams map[string]any
	a := &adapter{
		timeout:              time.Second,
		tenantID:             "tenant_test",
		defaultRuntimeConfig: "huahuo-default",
		invoke: func(_ context.Context, _ string, params map[string]any, timeout time.Duration) ([]byte, []byte, error) {
			gotTimeout = timeout
			gotParams = params
			return []byte(`{"status":"succeeded","finalAnswer":"ok"}`), nil, nil
		},
	}
	req := httptest.NewRequest(http.MethodPost, "/enterprise.runtime.run", strings.NewReader(`{"runId":"run_header_timeout","tenantId":"tenant_test","userId":"user_test","workspaceId":"workspace_test","threadId":"thread_test","runtimeConfigId":"huahuo-default","workspace":{"realPath":"/workspace","accessMode":"read"},"productSession":{"threadId":"thread_test","openclawSessionKey":"runtime:tenant:tenant_test:user:user_test:workspace:workspace_test:thread:thread_test"},"input":{"message":"hello"}}`))
	req.Header.Set("X-Huahuo-Runtime-Timeout-Sec", "120")
	rec := httptest.NewRecorder()

	a.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("POST status=%d body=%s", rec.Code, rec.Body.String())
	}
	if gotTimeout != 135*time.Second {
		t.Fatalf("gateway timeout = %s, want 135s", gotTimeout)
	}
	if _, exists := gotParams["timeoutSec"]; exists {
		t.Fatalf("transport timeout leaked into OpenClaw params: %#v", gotParams)
	}
}

func TestAdapterNormalizesLegacyHuahuoPayloadToRuntimeRunSpec(t *testing.T) {
	var gotParams map[string]any
	a := &adapter{
		timeout:              time.Second,
		tenantID:             "tenant_test",
		defaultRuntimeConfig: "huahuo-default",
		runtimeConfigPath:    "/runtime/config.json",
		runtimeStateDir:      "/runtime/state",
		runtimeLogsDir:       "/runtime/logs",
		runtimeTmpRoot:       "/runtime/tmp",
		invoke: func(_ context.Context, _ string, params map[string]any, _ time.Duration) ([]byte, []byte, error) {
			gotParams = params
			return []byte(`{"status":"succeeded","finalAnswer":"ok"}`), nil, nil
		},
	}
	req := httptest.NewRequest(http.MethodPost, "/enterprise.runtime.run", strings.NewReader(`{"method":"enterprise.runtime.run","runId":"run_legacy","workspaceDir":"/workspace","inputMessage":"hello","authPoolId":"huahuo-default","userId":"user_test","workspaceId":"workspace_test","threadId":"thread_test","productSession":{"threadId":"thread_test","openclawSessionKey":"oc:ps:bound-session"},"maxToolCalls":8,"timeoutSec":1}`))
	rec := httptest.NewRecorder()

	a.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("POST status=%d body=%s", rec.Code, rec.Body.String())
	}
	for _, key := range []string{"workspaceDir", "inputMessage", "authPoolId", "maxToolCalls", "timeoutSec", "method"} {
		if _, exists := gotParams[key]; exists {
			t.Fatalf("legacy/transport key %s leaked into OpenClaw params: %#v", key, gotParams)
		}
	}
	workspace := gotParams["workspace"].(map[string]any)
	input := gotParams["input"].(map[string]any)
	runtimeBody := gotParams["runtime"].(map[string]any)
	if gotParams["tenantId"] != "tenant_test" || gotParams["runtimeConfigId"] != "huahuo-default" || workspace["realPath"] != "/workspace" || input["message"] != "hello" {
		t.Fatalf("legacy payload normalization mismatch: %#v", gotParams)
	}
	productSession := gotParams["productSession"].(map[string]any)
	if productSession["openclawSessionKey"] != "oc:ps:bound-session" {
		t.Fatalf("legacy payload must preserve backend-provided product session: %#v", gotParams)
	}
	if runtimeBody["configPath"] != "/runtime/config.json" || runtimeBody["stateDir"] != "/runtime/state" || runtimeBody["logsDir"] != "/runtime/logs" || runtimeBody["tmpRoot"] != "/runtime/tmp" {
		t.Fatalf("legacy payload runtime injection mismatch: %#v", runtimeBody)
	}
}

func TestAdapterLegacyPayloadDerivesFormalWorkspacePath(t *testing.T) {
	dataRoot := t.TempDir()
	t.Setenv("HUAHUO_DATA_ROOT", dataRoot)
	var gotParams map[string]any
	a := &adapter{
		timeout:              time.Second,
		tenantID:             "tenant_test",
		defaultRuntimeConfig: "huahuo-default",
		invoke: func(_ context.Context, _ string, params map[string]any, _ time.Duration) ([]byte, []byte, error) {
			gotParams = params
			return []byte(`{"status":"succeeded","finalAnswer":"ok"}`), nil, nil
		},
	}
	req := httptest.NewRequest(http.MethodPost, "/enterprise.runtime.run", strings.NewReader(`{"runId":"run_legacy_path","inputMessage":"hello","authPoolId":"huahuo-default","userId":"user_test","workspaceId":"workspace_test","threadId":"thread_test","productSession":{"threadId":"thread_test","openclawSessionKey":"oc:ps:bound-session"},"timeoutSec":1}`))
	rec := httptest.NewRecorder()

	a.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("POST status=%d body=%s", rec.Code, rec.Body.String())
	}
	workspace := gotParams["workspace"].(map[string]any)
	want := filepath.Join(dataRoot, "workspaces", "tenants", "tenant_default", "users", "user_test", "workspaces", "workspace_test")
	if workspace["realPath"] != want {
		t.Fatalf("workspace realPath=%v want %s", workspace["realPath"], want)
	}
}

func TestAdapterRejectsLegacyPayloadWithoutProductSession(t *testing.T) {
	called := false
	a := &adapter{
		timeout:              time.Second,
		tenantID:             "tenant_test",
		defaultRuntimeConfig: "huahuo-default",
		invoke: func(_ context.Context, _ string, _ map[string]any, _ time.Duration) ([]byte, []byte, error) {
			called = true
			return []byte(`{"status":"succeeded","finalAnswer":"ok"}`), nil, nil
		},
	}
	req := httptest.NewRequest(http.MethodPost, "/enterprise.runtime.run", strings.NewReader(`{"runId":"run_missing_session","inputMessage":"hello","authPoolId":"huahuo-default","userId":"user_test","workspaceId":"workspace_test","threadId":"thread_test","timeoutSec":1}`))
	rec := httptest.NewRecorder()

	a.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("POST status=%d body=%s", rec.Code, rec.Body.String())
	}
	if called {
		t.Fatalf("adapter invoked OpenClaw without backend-provided product session")
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body["errorCode"] != "RUNTIME_INPUT_INVALID" {
		t.Fatalf("error body = %#v", body)
	}
}

func TestAdapterMapsGatewayValidationFailure(t *testing.T) {
	a := &adapter{
		timeout: time.Second,
		invoke: func(context.Context, string, map[string]any, time.Duration) ([]byte, []byte, error) {
			return nil, []byte("invalid enterprise.runtime.run params: must have required property 'runId'"), fmt.Errorf("exit 1")
		},
	}
	req := httptest.NewRequest(http.MethodPost, "/enterprise.runtime.run", strings.NewReader(`{}`))
	rec := httptest.NewRecorder()

	a.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	body := map[string]any{}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body["errorCode"] != "RUNTIME_INPUT_INVALID" {
		t.Fatalf("body=%#v", body)
	}
}

func TestAdapterClassifiesGatewayRuntimeFailures(t *testing.T) {
	cases := []struct {
		name   string
		stderr string
		want   string
		status int
	}{
		{name: "provider config", stderr: "provider config missing: runtime config not found", want: "PROVIDER_CONFIG_MISSING", status: http.StatusBadGateway},
		{name: "no session found", stderr: "Gateway sessions.resolve failed: No session found for runtime:tenant:tenant_test:user:user_test", want: "PROVIDER_CONFIG_MISSING", status: http.StatusBadGateway},
		{name: "provider auth", stderr: "gateway token unauthorized", want: "PROVIDER_AUTH_FAILED", status: http.StatusBadGateway},
		{name: "model input", stderr: "model input unsupported: context length exceeds token limit", want: "MODEL_INPUT_UNSUPPORTED", status: http.StatusBadRequest},
		{name: "runtime timeout", stderr: "deadline exceeded waiting for final response", want: "RUNTIME_TIMEOUT", status: http.StatusBadGateway},
		{name: "tool budget", stderr: "tool call budget exhausted", want: "RUNTIME_TOOL_BUDGET_EXCEEDED", status: http.StatusBadGateway},
		{name: "stalled", stderr: "runtime stalled with no progress", want: "RUNTIME_RUN_STALLED", status: http.StatusBadGateway},
		{name: "workspace forbidden", stderr: "workspace permission denied", want: "WORKSPACE_FORBIDDEN", status: http.StatusForbidden},
		{name: "attachment invalid", stderr: "attachment invalid for runtime run", want: "ATTACHMENT_INVALID", status: http.StatusBadRequest},
		{name: "parse failed", stderr: "invalid json: unexpected token", want: "AI_RESULT_PARSE_FAILED", status: http.StatusBadGateway},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a := &adapter{
				timeout:              time.Second,
				tenantID:             "tenant_test",
				defaultRuntimeConfig: "huahuo-default",
				invoke: func(context.Context, string, map[string]any, time.Duration) ([]byte, []byte, error) {
					return nil, []byte(tc.stderr), fmt.Errorf("exit 1")
				},
			}
			req := httptest.NewRequest(http.MethodPost, "/enterprise.runtime.run", strings.NewReader(`{"runId":"run_classify","tenantId":"tenant_test","userId":"user_test","workspaceId":"workspace_test","threadId":"thread_test","runtimeConfigId":"huahuo-default","workspace":{"realPath":"/workspace","accessMode":"read"},"productSession":{"threadId":"thread_test","openclawSessionKey":"runtime:tenant:tenant_test:user:user_test:workspace:workspace_test:thread:thread_test"},"input":{"message":"hello"}}`))
			rec := httptest.NewRecorder()

			a.ServeHTTP(rec, req)

			if rec.Code != tc.status {
				t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
			}
			body := map[string]any{}
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if body["errorCode"] != tc.want {
				t.Fatalf("body=%#v want errorCode=%s", body, tc.want)
			}
		})
	}
}

func TestAdapterSanitizesInputMessageBeforeGatewayParams(t *testing.T) {
	var gotParams map[string]any
	a := &adapter{
		timeout:              time.Second,
		tenantID:             "tenant_test",
		defaultRuntimeConfig: "huahuo-default",
		invoke: func(_ context.Context, _ string, params map[string]any, _ time.Duration) ([]byte, []byte, error) {
			gotParams = params
			return []byte(`{"status":"succeeded","finalAnswer":"ok"}`), nil, nil
		},
	}
	rawPrompt := "Task: minutes_generation\nAgent profile: recording_postprocess_agent\nBusiness parameters:\n" +
		strings.Repeat("large prompt ", 120) +
		"\nRead skills/meeting_minutes/SKILL.md and /home/data/huahuo/workspaces/u1/raw"
	payload := map[string]any{
		"runId":           "run_sanitize",
		"tenantId":        "tenant_test",
		"userId":          "user_test",
		"workspaceId":     "workspace_test",
		"threadId":        "thread_test",
		"runtimeConfigId": "huahuo-default",
		"workspace":       map[string]any{"realPath": "/workspace", "accessMode": "read"},
		"productSession":  map[string]any{"threadId": "thread_test", "openclawSessionKey": "runtime:tenant:tenant_test:user:user_test:workspace:workspace_test:thread:thread_test"},
		"input":           map[string]any{"message": rawPrompt},
	}
	raw, _ := json.Marshal(payload)
	req := httptest.NewRequest(http.MethodPost, "/enterprise.runtime.run", strings.NewReader(string(raw)))
	rec := httptest.NewRecorder()

	a.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("POST status=%d body=%s", rec.Code, rec.Body.String())
	}
	input := gotParams["input"].(map[string]any)
	message := input["message"].(string)
	if message != "Use the configured runtime instructions and available runtime context." {
		t.Fatalf("sanitized message=%q", message)
	}
	for _, forbidden := range []string{"SKILL.md", "/home/data/", "Business parameters", "large prompt"} {
		if strings.Contains(message, forbidden) {
			t.Fatalf("input.message leaked %q: %s", forbidden, message)
		}
	}
}

func TestAdapterPreservesConfiguredLongInputMessage(t *testing.T) {
	t.Setenv("HUAHUO_OPENCLAW_ADAPTER_INPUT_MAX_RUNES", "65536")
	message := strings.Repeat("长材料内容", 5000)
	got := sanitizeAdapterInputMessage(message)
	if got != message {
		t.Fatalf("configured long input was changed: gotRunes=%d wantRunes=%d", len([]rune(got)), len([]rune(message)))
	}
}

func TestAdapterTruncatesAboveConfiguredInputLimit(t *testing.T) {
	t.Setenv("HUAHUO_OPENCLAW_ADAPTER_INPUT_MAX_RUNES", "1024")
	message := strings.Repeat("长", 1400)
	got := sanitizeAdapterInputMessage(message)
	if !strings.HasSuffix(got, " [truncated]") {
		t.Fatalf("oversized input missing truncation marker: %q", got)
	}
	if len([]rune(strings.TrimSuffix(got, " [truncated]"))) != 1024 {
		t.Fatalf("oversized input truncated at %d runes, want 1024", len([]rune(strings.TrimSuffix(got, " [truncated]"))))
	}
}

func TestAdapterRedactsGatewayErrorMessage(t *testing.T) {
	a := &adapter{
		timeout: time.Second,
		invoke: func(context.Context, string, map[string]any, time.Duration) ([]byte, []byte, error) {
			return nil, []byte(`gateway token unauthorized {"privateExecutionContext":{"runTicket":"private-run-ticket-secret"}} Authorization: Bearer sk-secret /home/data/huahuo/workspaces/u1?signature=abc`), fmt.Errorf("exit 1")
		},
	}
	req := httptest.NewRequest(http.MethodPost, "/enterprise.runtime.run", strings.NewReader(`{"runId":"run_redact","tenantId":"tenant_test","userId":"user_test","workspaceId":"workspace_test","threadId":"thread_test","runtimeConfigId":"huahuo-default","workspace":{"realPath":"/workspace","accessMode":"read"},"productSession":{"threadId":"thread_test","openclawSessionKey":"runtime:tenant:tenant_test:user:user_test:workspace:workspace_test:thread:thread_test"},"input":{"message":"hello"}}`))
	rec := httptest.NewRecorder()

	a.ServeHTTP(rec, req)

	body := map[string]any{}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body["errorCode"] != "PROVIDER_AUTH_FAILED" {
		t.Fatalf("body=%#v", body)
	}
	message := strings.ToLower(fmt.Sprint(body["providerMessage"]))
	for _, forbidden := range []string{"bearer", "sk-secret", "private-run-ticket-secret", "/home/data/", "signature=abc", "runticket"} {
		if strings.Contains(message, forbidden) {
			t.Fatalf("providerMessage leaked %q: %#v", forbidden, body)
		}
	}
}

func TestNewAdapterFromEnvPrefersGatewayNativeToken(t *testing.T) {
	t.Setenv("OPENCLAW_GATEWAY_TOKEN", "gateway-native-test-token")
	t.Setenv("HUAHUO_OPENCLAW_GATEWAY_TOKEN", "legacy-test-token")

	a := newAdapterFromEnv()

	if a.token != "gateway-native-test-token" {
		t.Fatalf("adapter did not prefer gateway-native token")
	}
}

func TestNewAdapterFromEnvFallsBackToLegacyHuahuoToken(t *testing.T) {
	t.Setenv("OPENCLAW_GATEWAY_TOKEN", "")
	t.Setenv("HUAHUO_OPENCLAW_GATEWAY_TOKEN", "legacy-test-token")

	a := newAdapterFromEnv()

	if a.token != "legacy-test-token" {
		t.Fatalf("adapter did not fall back to legacy token")
	}
}

func TestNewAdapterFromEnvProductionRequiresExplicitDurableJTIStore(t *testing.T) {
	t.Setenv("HUAHUO_ENV", "prelaunch")
	t.Setenv("HUAHUO_RUNTIME_RUN_TICKET_JTI_STORE_DIR", "")
	a := newAdapterFromEnv()
	if err := a.validateDurableRunTicketJTIStoreConfiguration(); err == nil || !strings.Contains(err.Error(), "RUNTIME_STORAGE_UNAVAILABLE") {
		t.Fatalf("production durable-store validation error=%v", err)
	}
}

func TestNewAdapterFromEnvProductionBuildsFileRunTicketJTIStore(t *testing.T) {
	t.Setenv("HUAHUO_ENV", "prelaunch")
	t.Setenv("HUAHUO_RUNTIME_RUN_TICKET_JTI_STORE_DIR", filepath.Join(t.TempDir(), "run-ticket-jtis"))
	a := newAdapterFromEnv()
	if _, ok := a.materializer.JTIs.(*runtimepkg.FileRunTicketJTIStore); !ok {
		t.Fatalf("production JTI store type=%T", a.materializer.JTIs)
	}
}

func TestAdapterProductionRejectsMemoryRunTicketJTIStore(t *testing.T) {
	a := &adapter{
		runtimeEnvironment: "prelaunch",
		materializer: runtimepkg.NewRuntimeWorkspaceMaterializer(
			"host_test", t.TempDir(), "ticket-secret", nil, runtimepkg.NewMemoryRunTicketJTIStore(),
		),
	}
	if err := a.validateDurableRunTicketJTIStoreConfiguration(); err == nil || !strings.Contains(err.Error(), "RUNTIME_STORAGE_UNAVAILABLE") {
		t.Fatalf("production memory-store validation error=%v", err)
	}
}

func TestNewAdapterFromEnvUsesOpenClawRuntimeFallbacks(t *testing.T) {
	t.Setenv("HUAHUO_OPENCLAW_ENTERPRISE_RUNTIME_CONFIG_PATH", "")
	t.Setenv("OPENCLAW_ENTERPRISE_RUNTIME_CONFIG", "")
	t.Setenv("OPENCLAW_RUNTIME_CONFIG_PATH", "/runtime/fallback-config.json")
	t.Setenv("HUAHUO_OPENCLAW_RUNTIME_LOGS_DIR", "")
	t.Setenv("OPENCLAW_ENTERPRISE_RUNTIME_LOGS_DIR", "")
	t.Setenv("OPENCLAW_RUNTIME_LOGS_DIR", "/runtime/fallback-logs")
	t.Setenv("HUAHUO_OPENCLAW_RUNTIME_TMP_ROOT", "")
	t.Setenv("OPENCLAW_ENTERPRISE_RUNTIME_TMP_ROOT", "")
	t.Setenv("OPENCLAW_RUNTIME_TMP_ROOT", "/runtime/fallback-tmp")

	a := newAdapterFromEnv()

	if a.runtimeConfigPath != "/runtime/fallback-config.json" || a.runtimeLogsDir != "/runtime/fallback-logs" || a.runtimeTmpRoot != "/runtime/fallback-tmp" {
		t.Fatalf("adapter runtime fallback mismatch: %#v", a)
	}
}

func TestNewAdapterFromEnvDoesNotUseAuthPoolAsDefaultRuntimeConfig(t *testing.T) {
	t.Setenv("HUAHUO_OPENCLAW_RUNTIME_CONFIG_ID", "")
	t.Setenv("HUAHUO_OPENCLAW_RUNTIME_AUTH_POOL_ID", "runtime-model-default")

	a := newAdapterFromEnv()

	if a.defaultRuntimeConfig != "huahuo-default" {
		t.Fatalf("default runtime config = %q, want huahuo-default", a.defaultRuntimeConfig)
	}

	t.Setenv("HUAHUO_OPENCLAW_RUNTIME_CONFIG_ID", "huahuo-topic-generation")
	a = newAdapterFromEnv()
	if a.defaultRuntimeConfig != "huahuo-topic-generation" {
		t.Fatalf("configured runtime config = %q, want huahuo-topic-generation", a.defaultRuntimeConfig)
	}
}

func TestAdapterLocalAdmissionEnforcesTotalAndScopeLimits(t *testing.T) {
	a := &adapter{
		maxActiveRuns: 2, maxProductThreadRuns: 1, maxDetachedTaskRuns: 2,
	}
	a.admission = NewHostAdmissionController(a.maxActiveRuns, a.maxProductThreadRuns, a.maxDetachedTaskRuns)
	first, err := a.acquireRunPermit("run_product_1", "product_thread")
	if err != nil || !first.Acquired {
		t.Fatalf("first permit=%#v err=%v", first, err)
	}
	duplicate, err := a.acquireRunPermit("run_product_1", "product_thread")
	if err != nil || duplicate.Acquired {
		t.Fatalf("idempotent permit=%#v err=%v", duplicate, err)
	}
	if _, err := a.acquireRunPermit("run_product_2", "product_thread"); err == nil {
		t.Fatal("product-thread scope limit was not enforced")
	}
	if permit, err := a.acquireRunPermit("run_detached_1", "detached_task"); err != nil || !permit.Acquired {
		t.Fatalf("detached permit=%#v err=%v", permit, err)
	}
	if _, err := a.acquireRunPermit("run_detached_2", "detached_task"); err == nil {
		t.Fatal("total host limit was not enforced")
	}
	if !a.releaseRunPermit("run_product_1") {
		t.Fatal("expected product permit release")
	}
	if permit, err := a.acquireRunPermit("run_detached_2", "detached_task"); err != nil || !permit.Acquired {
		t.Fatalf("capacity was not restored after release: permit=%#v err=%v", permit, err)
	}
}

func TestAdapterTerminalObservationReleasesLocalPermit(t *testing.T) {
	a := &adapter{
		maxActiveRuns: 1, maxProductThreadRuns: 1, maxDetachedTaskRuns: 1,
	}
	a.admission = NewHostAdmissionController(a.maxActiveRuns, a.maxProductThreadRuns, a.maxDetachedTaskRuns)
	if _, err := a.acquireRunPermit("run_terminal", "detached_task"); err != nil {
		t.Fatalf("acquire terminal permit: %v", err)
	}
	a.observeRuntimeResponse("enterprise.runtime.status", map[string]any{"runId": "run_terminal"}, []byte(`{"runId":"run_terminal","status":"succeeded"}`))
	if got := a.activeRunCount(); got != 0 {
		t.Fatalf("terminal permit count=%d want 0", got)
	}
	if a.releaseRunPermit("run_terminal") {
		t.Fatal("terminal release must be idempotent")
	}
}

func TestAdapterRejectsInvalidLocalAdmissionConfiguration(t *testing.T) {
	a := &adapter{maxActiveRuns: 2, maxProductThreadRuns: 3, maxDetachedTaskRuns: 1}
	a.admission = NewHostAdmissionController(a.maxActiveRuns, a.maxProductThreadRuns, a.maxDetachedTaskRuns)
	if err := a.validateLocalRunCapacity(); err == nil {
		t.Fatal("scope capacity above total capacity must fail closed")
	}
}

func TestNewAdapterFromEnvRejectsMalformedCapacity(t *testing.T) {
	t.Setenv("HUAHUO_RUNTIME_MAX_ACTIVE_RUNS", "not-a-number")
	a := newAdapterFromEnv()
	if err := a.validateLocalRunCapacity(); err == nil {
		t.Fatal("malformed capacity must not fall back to a production default")
	}
}

func TestNewAdapterFromEnvUsesEightRunLocalHardLimitByDefault(t *testing.T) {
	t.Setenv("HUAHUO_RUNTIME_MAX_ACTIVE_RUNS", "")
	t.Setenv("HUAHUO_RUNTIME_MAX_PRODUCT_THREAD_RUNS", "")
	t.Setenv("HUAHUO_RUNTIME_MAX_DETACHED_TASK_RUNS", "")

	a := newAdapterFromEnv()
	if a.maxActiveRuns != 8 {
		t.Fatalf("maxActiveRuns=%d want 8", a.maxActiveRuns)
	}
	if a.maxProductThreadRuns != 8 || a.maxDetachedTaskRuns != 8 {
		t.Fatalf("default scope capacities=(%d,%d) want (8,8)", a.maxProductThreadRuns, a.maxDetachedTaskRuns)
	}
	if err := a.validateLocalRunCapacity(); err != nil {
		t.Fatalf("default local capacity must be valid: %v", err)
	}
}
