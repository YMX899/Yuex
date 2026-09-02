package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	runtimepkg "huahuoai/backend/source/internal/runtime"
)

const (
	runtimePolicyVersion       = "huahuo.runtime-policy.v1"
	runtimePolicyAlgorithm     = "HS256"
	runtimePolicyKeyContext    = "huahuo/runtime-policy/v1"
	runtimePolicyMaximumTTL    = 15 * time.Minute
	runtimePolicyMinimumSecret = 32
)

type runtimePolicyEnvelope struct {
	Version               string                                 `json:"version"`
	Algorithm             string                                 `json:"algorithm"`
	KeyID                 string                                 `json:"keyId"`
	RunID                 string                                 `json:"runId"`
	IdempotencyKey        string                                 `json:"idempotencyKey"`
	WorkspaceManifestHash string                                 `json:"workspaceManifestHash"`
	DispatchIdentity      string                                 `json:"dispatchIdentity"`
	CapabilityHash        string                                 `json:"capabilityHash"`
	PlanHash              string                                 `json:"planHash"`
	IssuedAt              int64                                  `json:"issuedAt"`
	ExpiresAt             int64                                  `json:"expiresAt"`
	WorkspaceAccessMode   string                                 `json:"workspaceAccessMode"`
	WriteLease            *runtimepkg.RuntimeWorkspaceWriteLease `json:"writeLease"`
	RequiredTools         []string                               `json:"requiredTools"`
	AllowedTools          []string                               `json:"allowedTools"`
	ToolBudget            runtimepkg.RuntimeToolBudget           `json:"toolBudget"`
	Signature             string                                 `json:"signature"`
}

type unsignedRuntimePolicy struct {
	Version               string                                 `json:"version"`
	Algorithm             string                                 `json:"algorithm"`
	KeyID                 string                                 `json:"keyId"`
	RunID                 string                                 `json:"runId"`
	IdempotencyKey        string                                 `json:"idempotencyKey"`
	WorkspaceManifestHash string                                 `json:"workspaceManifestHash"`
	DispatchIdentity      string                                 `json:"dispatchIdentity"`
	CapabilityHash        string                                 `json:"capabilityHash"`
	PlanHash              string                                 `json:"planHash"`
	IssuedAt              int64                                  `json:"issuedAt"`
	ExpiresAt             int64                                  `json:"expiresAt"`
	WorkspaceAccessMode   string                                 `json:"workspaceAccessMode"`
	WriteLease            *runtimepkg.RuntimeWorkspaceWriteLease `json:"writeLease"`
	RequiredTools         []string                               `json:"requiredTools"`
	AllowedTools          []string                               `json:"allowedTools"`
	ToolBudget            runtimepkg.RuntimeToolBudget           `json:"toolBudget"`
}

func (a *adapter) validateRuntimePolicySigningConfiguration() error {
	if a == nil || len(a.runTicketSecret) < runtimePolicyMinimumSecret || !validRuntimePolicyKeyID(a.runtimePolicyKeyID) {
		return fmt.Errorf("RUNTIME_TOOL_BUDGET_UNSUPPORTED")
	}
	return nil
}

func (a *adapter) signRuntimePolicy(plan runtimepkg.AgentRunPlan, claims runtimepkg.RunTicketClaims, manifestHash, dispatchIdentity, planHash string, now time.Time) (runtimePolicyEnvelope, error) {
	if err := a.validateRuntimePolicySigningConfiguration(); err != nil {
		return runtimePolicyEnvelope{}, err
	}
	if err := runtimepkg.ValidateAgentRunToolPolicy(plan); err != nil {
		return runtimePolicyEnvelope{}, fmt.Errorf("RUNTIME_PERMISSION_DENIED")
	}
	if !validRuntimePolicyIdentifier(claims.RunID) || claims.PlanHash != planHash || !validRuntimePolicySHA256(manifestHash) || !validRuntimePolicySHA256(dispatchIdentity) ||
		!validRuntimePolicyIdentifier(claims.CapabilityHash) || !validRuntimePolicySHA256(planHash) {
		return runtimePolicyEnvelope{}, fmt.Errorf("RUNTIME_PERMISSION_DENIED")
	}
	now = now.UTC().Truncate(time.Second)
	expiresAt := now.Add(runtimePolicyMaximumTTL).Unix()
	if claims.ExpiresAt < expiresAt {
		expiresAt = claims.ExpiresAt
	}
	if expiresAt <= now.Unix() {
		return runtimePolicyEnvelope{}, fmt.Errorf("RUNTIME_PERMISSION_DENIED")
	}
	mount, err := runtimepkg.RuntimeWorkspaceMountForPlan(plan)
	if err != nil {
		return runtimePolicyEnvelope{}, fmt.Errorf("RUNTIME_PERMISSION_DENIED")
	}
	// Preserve an explicit empty array for no-tool Runs. A missing/null allow-list
	// would hand policy interpretation back to the Runtime.
	tools := append([]string{}, plan.RequiredTools...)
	policy := runtimePolicyEnvelope{
		Version: runtimePolicyVersion, Algorithm: runtimePolicyAlgorithm, KeyID: a.runtimePolicyKeyID,
		RunID: claims.RunID, IdempotencyKey: claims.RunID, WorkspaceManifestHash: manifestHash,
		DispatchIdentity: dispatchIdentity, CapabilityHash: claims.CapabilityHash, PlanHash: planHash,
		IssuedAt: now.Unix(), ExpiresAt: expiresAt,
		WorkspaceAccessMode: mount.AccessMode,
		RequiredTools:       tools, AllowedTools: append([]string{}, tools...), ToolBudget: plan.ToolBudget,
	}
	if mount.AccessMode == runtimepkg.RuntimeWorkspaceAccessWrite {
		lease, leaseErr := runtimepkg.NewRuntimeWorkspaceWriteLease(plan, claims.RunID, claims.WorkspaceID, manifestHash, expiresAt)
		if leaseErr != nil {
			return runtimePolicyEnvelope{}, fmt.Errorf("RUNTIME_PERMISSION_DENIED")
		}
		policy.WriteLease = &lease
	}
	canonical, err := canonicalRuntimePolicyPayload(policy)
	if err != nil {
		return runtimePolicyEnvelope{}, fmt.Errorf("RUNTIME_PERMISSION_DENIED")
	}
	derived := hmac.New(sha256.New, []byte(a.runTicketSecret))
	_, _ = derived.Write([]byte(runtimePolicyKeyContext))
	signer := hmac.New(sha256.New, derived.Sum(nil))
	_, _ = signer.Write(canonical)
	policy.Signature = "sha256:" + hex.EncodeToString(signer.Sum(nil))
	return policy, nil
}

// verifyRuntimePolicyForGateway proves that the exact envelope about to cross
// the Adapter/Gateway boundary was derived from the trusted RunTicket facts.
// It validates only the Adapter's outbound message; Gateway-side schema and
// HMAC verification are a separate required contract, not an Adapter claim.
func (a *adapter) verifyRuntimePolicyForGateway(policy runtimePolicyEnvelope, plan runtimepkg.AgentRunPlan, claims runtimepkg.RunTicketClaims, manifestHash, dispatchIdentity, planHash string, now time.Time) error {
	expected, err := a.signRuntimePolicy(plan, claims, manifestHash, dispatchIdentity, planHash, now)
	if err != nil {
		return err
	}
	actualPayload, err := canonicalRuntimePolicyPayload(policy)
	if err != nil {
		return fmt.Errorf("RUNTIME_PERMISSION_DENIED")
	}
	expectedPayload, err := canonicalRuntimePolicyPayload(expected)
	if err != nil || !hmac.Equal(actualPayload, expectedPayload) ||
		!hmac.Equal([]byte(policy.Signature), []byte(expected.Signature)) {
		return fmt.Errorf("RUNTIME_PERMISSION_DENIED")
	}
	return nil
}

func canonicalRuntimePolicyPayload(policy runtimePolicyEnvelope) ([]byte, error) {
	return json.Marshal(unsignedRuntimePolicy{
		Version: policy.Version, Algorithm: policy.Algorithm, KeyID: policy.KeyID,
		RunID: policy.RunID, IdempotencyKey: policy.IdempotencyKey,
		WorkspaceManifestHash: policy.WorkspaceManifestHash, DispatchIdentity: policy.DispatchIdentity,
		CapabilityHash: policy.CapabilityHash, PlanHash: policy.PlanHash,
		IssuedAt: policy.IssuedAt, ExpiresAt: policy.ExpiresAt,
		WorkspaceAccessMode: policy.WorkspaceAccessMode, WriteLease: policy.WriteLease,
		RequiredTools: policy.RequiredTools, AllowedTools: policy.AllowedTools, ToolBudget: policy.ToolBudget,
	})
}

func runtimePolicyMap(policy runtimePolicyEnvelope) map[string]any {
	return map[string]any{
		"version": policy.Version, "algorithm": policy.Algorithm, "keyId": policy.KeyID,
		"runId": policy.RunID, "idempotencyKey": policy.IdempotencyKey,
		"workspaceManifestHash": policy.WorkspaceManifestHash, "dispatchIdentity": policy.DispatchIdentity,
		"capabilityHash": policy.CapabilityHash, "planHash": policy.PlanHash,
		"issuedAt": policy.IssuedAt, "expiresAt": policy.ExpiresAt,
		"workspaceAccessMode": policy.WorkspaceAccessMode, "writeLease": policy.WriteLease,
		"requiredTools": append([]string{}, policy.RequiredTools...),
		"allowedTools":  append([]string{}, policy.AllowedTools...),
		"toolBudget":    policy.ToolBudget, "signature": policy.Signature,
	}
}

func validRuntimePolicyKeyID(value string) bool {
	value = strings.TrimSpace(value)
	if len(value) == 0 || len(value) > 128 {
		return false
	}
	for index, character := range value {
		if index == 0 {
			if !(character >= 'A' && character <= 'Z' || character >= 'a' && character <= 'z' || character >= '0' && character <= '9') {
				return false
			}
			continue
		}
		if !(character >= 'A' && character <= 'Z' || character >= 'a' && character <= 'z' || character >= '0' && character <= '9' || character == '.' || character == '_' || character == ':' || character == '-') {
			return false
		}
	}
	return true
}

// HMAC JSON must stay byte-identical between Go's encoder and Node's
// JSON.stringify. Policy-bound identifiers therefore use an ASCII-only form
// instead of admitting values whose escaping differs by runtime.
func validRuntimePolicyIdentifier(value string) bool {
	if value == "" || len(value) > 512 || strings.TrimSpace(value) != value {
		return false
	}
	for index, character := range value {
		if index == 0 {
			if !(character >= 'A' && character <= 'Z' || character >= 'a' && character <= 'z' || character >= '0' && character <= '9') {
				return false
			}
			continue
		}
		if !(character >= 'A' && character <= 'Z' || character >= 'a' && character <= 'z' || character >= '0' && character <= '9' || character == '.' || character == '_' || character == ':' || character == '-') {
			return false
		}
	}
	return true
}

func validRuntimePolicySHA256(value string) bool {
	value = strings.ToLower(strings.TrimSpace(value))
	if !strings.HasPrefix(value, "sha256:") || len(value) != len("sha256:")+64 {
		return false
	}
	for _, character := range value[len("sha256:"):] {
		if (character < '0' || character > '9') && (character < 'a' || character > 'f') {
			return false
		}
	}
	return true
}
