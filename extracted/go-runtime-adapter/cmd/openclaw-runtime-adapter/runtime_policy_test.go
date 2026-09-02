package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
	"time"

	runtimepkg "huahuoai/backend/source/internal/runtime"
)

func TestRuntimePolicySignatureMatchesRuntimeCanonicalFixture(t *testing.T) {
	secret := "0123456789abcdef0123456789abcdef"
	planHash := "sha256:" + strings.Repeat("a", 64)
	policy, err := (&adapter{runTicketSecret: secret, runtimePolicyKeyID: "runtime-policy-v1"}).signRuntimePolicy(
		runtimepkg.AgentRunPlan{RequiredTools: []string{"read"}, ToolBudget: testRuntimeToolBudget()},
		runtimepkg.RunTicketClaims{RunID: "run_policy_fixture", CapabilityHash: "capability_fixture", PlanHash: planHash, ExpiresAt: 1700000300},
		"sha256:"+strings.Repeat("b", 64),
		"sha256:"+strings.Repeat("c", 64),
		planHash,
		time.Unix(1700000000, 0).UTC(),
	)
	if err != nil {
		t.Fatal(err)
	}
	const wantSignature = "sha256:e2a3965bf9d3a0e62a03e21fa55d7c18cebb940133b84f63833ef791c2575005"
	if policy.Signature != wantSignature {
		t.Fatalf("signature=%s want=%s", policy.Signature, wantSignature)
	}
	if policy.Version != runtimePolicyVersion || policy.Algorithm != runtimePolicyAlgorithm || policy.IssuedAt != 1700000000 || policy.ExpiresAt != 1700000300 ||
		policy.WorkspaceAccessMode != runtimepkg.RuntimeWorkspaceAccessRead || policy.WriteLease != nil ||
		!runtimeStringSliceEqual(policy.RequiredTools, []string{"read"}) || !runtimeStringSliceEqual(policy.AllowedTools, []string{"read"}) {
		t.Fatalf("policy=%+v", policy)
	}
}

func TestRuntimePolicySigningFailsClosedWithoutUsableKeyMaterial(t *testing.T) {
	for _, adapter := range []*adapter{
		{runTicketSecret: "short", runtimePolicyKeyID: "runtime-policy-v1"},
		{runTicketSecret: strings.Repeat("s", 32), runtimePolicyKeyID: ""},
		{runTicketSecret: strings.Repeat("s", 32), runtimePolicyKeyID: "invalid key"},
	} {
		if err := adapter.validateRuntimePolicySigningConfiguration(); err == nil || err.Error() != "RUNTIME_TOOL_BUDGET_UNSUPPORTED" {
			t.Fatalf("policy signing configuration error=%v", err)
		}
	}
}

func TestRuntimePolicyRejectsNonCanonicalBindingIdentifiers(t *testing.T) {
	secret := "0123456789abcdef0123456789abcdef"
	planHash := "sha256:" + strings.Repeat("a", 64)
	for name, mutate := range map[string]func(*runtimepkg.RunTicketClaims){
		"run_id":          func(claims *runtimepkg.RunTicketClaims) { claims.RunID = "run<unsafe>" },
		"capability_hash": func(claims *runtimepkg.RunTicketClaims) { claims.CapabilityHash = "capability unsafe" },
	} {
		t.Run(name, func(t *testing.T) {
			claims := runtimepkg.RunTicketClaims{RunID: "run_policy_identifier", CapabilityHash: "capability_fixture", PlanHash: planHash, ExpiresAt: 1700000300}
			mutate(&claims)
			_, err := (&adapter{runTicketSecret: secret, runtimePolicyKeyID: "runtime-policy-v1"}).signRuntimePolicy(
				runtimepkg.AgentRunPlan{RequiredTools: []string{"read"}, ToolBudget: testRuntimeToolBudget()},
				claims,
				"sha256:"+strings.Repeat("b", 64),
				"sha256:"+strings.Repeat("c", 64),
				planHash,
				time.Unix(1700000000, 0).UTC(),
			)
			if err == nil || err.Error() != "RUNTIME_PERMISSION_DENIED" {
				t.Fatalf("noncanonical policy binding error=%v", err)
			}
		})
	}
}

func TestRuntimePolicyKeepsExplicitEmptyAllowList(t *testing.T) {
	secret := "0123456789abcdef0123456789abcdef"
	planHash := "sha256:" + strings.Repeat("a", 64)
	policy, err := (&adapter{runTicketSecret: secret, runtimePolicyKeyID: "runtime-policy-v1"}).signRuntimePolicy(
		runtimepkg.AgentRunPlan{RequiredTools: []string{}, ToolBudget: testRuntimeToolBudget()},
		runtimepkg.RunTicketClaims{RunID: "run_policy_empty", CapabilityHash: "capability_fixture", PlanHash: planHash, ExpiresAt: 1700000300},
		"sha256:"+strings.Repeat("b", 64),
		"sha256:"+strings.Repeat("c", 64),
		planHash,
		time.Unix(1700000000, 0).UTC(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if policy.RequiredTools == nil || policy.AllowedTools == nil || len(policy.RequiredTools) != 0 || len(policy.AllowedTools) != 0 {
		t.Fatalf("empty policy lists must be explicit: %#v", policy)
	}
	encoded, err := json.Marshal(runtimePolicyMap(policy))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(encoded), `"requiredTools":[]`) || !strings.Contains(string(encoded), `"allowedTools":[]`) {
		t.Fatalf("runtime policy lost explicit empty lists: %s", encoded)
	}
}

func TestRuntimePolicySignsBoundStagingWriteLease(t *testing.T) {
	secret := "0123456789abcdef0123456789abcdef"
	planHash := "sha256:" + strings.Repeat("a", 64)
	manifestHash := "sha256:" + strings.Repeat("b", 64)
	plan := runtimepkg.AgentRunPlan{
		AgentRunID: "run_policy_staging", RequiredTools: []string{"read", "write"}, WriteMode: "runtime_staging", ToolBudget: testRuntimeToolBudget(),
	}
	claims := runtimepkg.RunTicketClaims{
		RunID: plan.AgentRunID, WorkspaceID: "workspace_policy_staging", CapabilityHash: "capability_fixture", PlanHash: planHash, ExpiresAt: 1700000300,
	}
	a := &adapter{runTicketSecret: secret, runtimePolicyKeyID: "runtime-policy-v1"}
	policy, err := a.signRuntimePolicy(plan, claims, manifestHash, "sha256:"+strings.Repeat("c", 64), planHash, time.Unix(1700000000, 0).UTC())
	if err != nil {
		t.Fatal(err)
	}
	if policy.WorkspaceAccessMode != runtimepkg.RuntimeWorkspaceAccessWrite || policy.WriteLease == nil ||
		policy.WriteLease.RunID != claims.RunID || policy.WriteLease.WorkspaceID != claims.WorkspaceID ||
		policy.WriteLease.WorkspaceManifestHash != manifestHash || policy.WriteLease.ExpiresAt != policy.ExpiresAt ||
		!runtimeStringSliceEqual(policy.WriteLease.AllowedRoots, []string{"output", "staging"}) {
		t.Fatalf("staging write lease=%+v policy=%+v", policy.WriteLease, policy)
	}
	tampered := policy
	tamperedLease := *policy.WriteLease
	tamperedLease.AllowedRoots = []string{"output", "materials"}
	tampered.WriteLease = &tamperedLease
	if signature := runtimePolicyTestSignature(t, secret, tampered); signature == policy.Signature {
		t.Fatalf("policy signature did not bind write lease: %+v", tampered)
	}
}

func TestRuntimePolicySignatureBindsExactAllowedTools(t *testing.T) {
	secret := "0123456789abcdef0123456789abcdef"
	planHash := "sha256:" + strings.Repeat("a", 64)
	policy, err := (&adapter{runTicketSecret: secret, runtimePolicyKeyID: "runtime-policy-v1"}).signRuntimePolicy(
		runtimepkg.AgentRunPlan{RequiredTools: []string{"read", "workspace_search"}, ToolBudget: testRuntimeToolBudget()},
		runtimepkg.RunTicketClaims{RunID: "run_policy_tools", CapabilityHash: "capability_fixture", PlanHash: planHash, ExpiresAt: 1700000300},
		"sha256:"+strings.Repeat("b", 64),
		"sha256:"+strings.Repeat("c", 64),
		planHash,
		time.Unix(1700000000, 0).UTC(),
	)
	if err != nil {
		t.Fatal(err)
	}
	tampered := policy
	tampered.AllowedTools = []string{"read"}
	if signature := runtimePolicyTestSignature(t, secret, tampered); signature == policy.Signature {
		t.Fatalf("policy signature did not bind changed allow-list: %+v", tampered)
	}
	tampered = policy
	tampered.AllowedTools = nil
	if signature := runtimePolicyTestSignature(t, secret, tampered); signature == policy.Signature {
		t.Fatalf("policy signature did not bind missing allow-list: %+v", tampered)
	}
}

func TestRuntimePolicyGatewayVerificationRejectsTamperedEnvelope(t *testing.T) {
	secret := "0123456789abcdef0123456789abcdef"
	now := time.Unix(1700000000, 0).UTC()
	planHash := "sha256:" + strings.Repeat("a", 64)
	manifestHash := "sha256:" + strings.Repeat("b", 64)
	dispatchIdentity := "sha256:" + strings.Repeat("c", 64)
	plan := runtimepkg.AgentRunPlan{
		RequiredTools: []string{"read", "workspace_search"},
		ToolBudget:    testRuntimeToolBudget(),
	}
	claims := runtimepkg.RunTicketClaims{
		RunID: "run_policy_gateway", CapabilityHash: "capability_fixture", PlanHash: planHash, ExpiresAt: 1700000300,
	}
	a := &adapter{runTicketSecret: secret, runtimePolicyKeyID: "runtime-policy-v1"}
	policy, err := a.signRuntimePolicy(plan, claims, manifestHash, dispatchIdentity, planHash, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.verifyRuntimePolicyForGateway(policy, plan, claims, manifestHash, dispatchIdentity, planHash, now); err != nil {
		t.Fatalf("valid policy rejected: %v", err)
	}

	for name, tamper := range map[string]func(*runtimePolicyEnvelope){
		"allow_list": func(value *runtimePolicyEnvelope) { value.AllowedTools = []string{"read"} },
		"budget":     func(value *runtimePolicyEnvelope) { value.ToolBudget.MaxToolCalls++ },
		"signature":  func(value *runtimePolicyEnvelope) { value.Signature = "sha256:" + strings.Repeat("0", 64) },
	} {
		t.Run(name, func(t *testing.T) {
			tampered := policy
			tampered.RequiredTools = append([]string{}, policy.RequiredTools...)
			tampered.AllowedTools = append([]string{}, policy.AllowedTools...)
			tamper(&tampered)
			if err := a.verifyRuntimePolicyForGateway(tampered, plan, claims, manifestHash, dispatchIdentity, planHash, now); err == nil || err.Error() != "RUNTIME_PERMISSION_DENIED" {
				t.Fatalf("tampered policy error=%v", err)
			}
		})
	}
}

func runtimePolicyTestSignature(t *testing.T, secret string, policy runtimePolicyEnvelope) string {
	t.Helper()
	canonical, err := canonicalRuntimePolicyPayload(policy)
	if err != nil {
		t.Fatal(err)
	}
	derived := hmac.New(sha256.New, []byte(secret))
	_, _ = derived.Write([]byte(runtimePolicyKeyContext))
	signer := hmac.New(sha256.New, derived.Sum(nil))
	_, _ = signer.Write(canonical)
	return "sha256:" + hex.EncodeToString(signer.Sum(nil))
}
