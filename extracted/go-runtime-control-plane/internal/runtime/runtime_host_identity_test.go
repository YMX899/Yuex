package runtime

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"testing"
	"time"
)

func TestEd25519RuntimeHostHeartbeatVerifierRejectsReplayStaleSignatureAndRevocation(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := NewEd25519RuntimeHostHeartbeatSigner("key-current", privateKey)
	if err != nil {
		t.Fatal(err)
	}
	nonces := NewMemoryRuntimeHostNonceStore()
	verifier, err := NewEd25519RuntimeHostHeartbeatVerifier([]RuntimeHostVerificationKey{{KeyID: "key-current", PublicKey: publicKey}}, nil, 30*time.Second, 2*time.Minute, nonces)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 13, 9, 0, 0, 0, time.UTC)
	verifier.now = func() time.Time { return now }
	nonces.now = func() time.Time { return now }
	principal := RuntimeHostPrincipal{RuntimeHostID: "host-1", InstanceID: "instance-1", Environment: "test", CertificateID: "cert-1"}
	heartbeat, err := signer.SignHeartbeat("POST", "/internal/v1/runtime-hosts/host-1/heartbeat", principal, RuntimeHostHeartbeat{
		Sequence: 1, ObservedAt: now, CapabilityHash: "cap-1", SignatureKeyID: "ignored",
		SafeHealth: map[string]any{"gatewayConnected": true, "localAdmission": map[string]any{"activeRuns": 0}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := verifier.VerifyHeartbeat(context.Background(), principal, "POST", "/internal/v1/runtime-hosts/host-1/heartbeat", heartbeat); err != nil {
		t.Fatalf("valid heartbeat rejected: %v", err)
	}
	if err := verifier.VerifyHeartbeat(context.Background(), principal, "POST", "/internal/v1/runtime-hosts/host-1/heartbeat", heartbeat); !errors.Is(err, ErrRuntimeHeartbeatStale) {
		t.Fatalf("replay error=%v", err)
	}

	tampered, err := signer.SignHeartbeat("POST", "/internal/v1/runtime-hosts/host-1/heartbeat", principal, RuntimeHostHeartbeat{Sequence: 2, ObservedAt: now, CapabilityHash: "cap-1"})
	if err != nil {
		t.Fatal(err)
	}
	tampered.Signature = "invalid"
	if err := verifier.VerifyHeartbeat(context.Background(), principal, "POST", "/internal/v1/runtime-hosts/host-1/heartbeat", tampered); !errors.Is(err, ErrRuntimeHeartbeatStale) {
		t.Fatalf("signature error=%v", err)
	}

	staleTimestamp, err := signer.SignHeartbeat("POST", "/internal/v1/runtime-hosts/host-1/heartbeat", principal, RuntimeHostHeartbeat{Sequence: 3, ObservedAt: now.Add(-31 * time.Second), CapabilityHash: "cap-1"})
	if err != nil {
		t.Fatal(err)
	}
	if err := verifier.VerifyHeartbeat(context.Background(), principal, "POST", "/internal/v1/runtime-hosts/host-1/heartbeat", staleTimestamp); !errors.Is(err, ErrRuntimeHeartbeatStale) {
		t.Fatalf("timestamp error=%v", err)
	}

	revoked, err := NewEd25519RuntimeHostHeartbeatVerifier([]RuntimeHostVerificationKey{{KeyID: "key-current", PublicKey: publicKey}}, []string{"cert-1"}, 30*time.Second, 2*time.Minute, NewMemoryRuntimeHostNonceStore())
	if err != nil {
		t.Fatal(err)
	}
	if err := revoked.VerifyPrincipal(context.Background(), principal); !errors.Is(err, ErrRuntimeHostUnauthorized) {
		t.Fatalf("revoked certificate error=%v", err)
	}
}

func TestEd25519RuntimeHostRegistrationVerifierRejectsReplayTamperAndStaleTimestamp(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := NewEd25519RuntimeHostHeartbeatSigner("key-registration", privateKey)
	if err != nil {
		t.Fatal(err)
	}
	nonces := NewMemoryRuntimeHostNonceStore()
	verifier, err := NewEd25519RuntimeHostHeartbeatVerifier([]RuntimeHostVerificationKey{{KeyID: "key-registration", PublicKey: publicKey}}, nil, 30*time.Second, 2*time.Minute, nonces)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 16, 10, 0, 0, 0, time.UTC)
	verifier.now = func() time.Time { return now }
	nonces.now = func() time.Time { return now }
	principal := RuntimeHostPrincipal{RuntimeHostID: "host-registration", InstanceID: "instance-registration", Environment: "test", CertificateID: "cert-registration"}
	registration := RuntimeHostRegistration{
		Endpoint: "https://runtime.internal", Zone: "test-zone", RuntimeVersion: "runtime-v1", AdapterVersion: "adapter-v1",
		Capabilities: RuntimeCapabilitySnapshot{
			CapabilityHash: "cap-registration", MaxToolCallsSupported: 200,
			SupportsPerRunBudget: true, SupportsBudgetWarning: true, SupportsForcedAbort: true,
			Tools: []ToolCapability{{Name: "read", Status: "ready", SchemaHash: "sha256:read"}},
		},
		SessionStoreID: "store-registration", MaxActiveRuns: 8, MaxProductThreadRuns: 5, MaxDetachedTaskRuns: 3,
	}
	proof, err := signer.SignRegistration("POST", "/internal/v1/runtime-hosts/register", principal, registration, RuntimeHostRegistrationProof{ObservedAt: now})
	if err != nil {
		t.Fatal(err)
	}
	if err := verifier.VerifyRegistration(context.Background(), principal, "POST", "/internal/v1/runtime-hosts/register", registration, proof); err != nil {
		t.Fatalf("valid registration rejected: %v", err)
	}
	if err := verifier.VerifyRegistration(context.Background(), principal, "POST", "/internal/v1/runtime-hosts/register", registration, proof); !errors.Is(err, ErrRuntimeHeartbeatStale) {
		t.Fatalf("registration replay error=%v", err)
	}

	tamperCases := []struct {
		name   string
		mutate func(*RuntimeHostRegistration, *RuntimeHostRegistrationProof, *string, *string, *RuntimeHostPrincipal)
		want   error
	}{
		{name: "endpoint", mutate: func(registration *RuntimeHostRegistration, _ *RuntimeHostRegistrationProof, _ *string, _ *string, _ *RuntimeHostPrincipal) {
			registration.Endpoint = "https://other.internal"
		}, want: ErrRuntimeHeartbeatStale},
		{name: "zone", mutate: func(registration *RuntimeHostRegistration, _ *RuntimeHostRegistrationProof, _ *string, _ *string, _ *RuntimeHostPrincipal) {
			registration.Zone = "other-zone"
		}, want: ErrRuntimeHeartbeatStale},
		{name: "runtime_version", mutate: func(registration *RuntimeHostRegistration, _ *RuntimeHostRegistrationProof, _ *string, _ *string, _ *RuntimeHostPrincipal) {
			registration.RuntimeVersion = "runtime-v2"
		}, want: ErrRuntimeHeartbeatStale},
		{name: "adapter_version", mutate: func(registration *RuntimeHostRegistration, _ *RuntimeHostRegistrationProof, _ *string, _ *string, _ *RuntimeHostPrincipal) {
			registration.AdapterVersion = "adapter-v2"
		}, want: ErrRuntimeHeartbeatStale},
		{name: "capability_hash", mutate: func(registration *RuntimeHostRegistration, _ *RuntimeHostRegistrationProof, _ *string, _ *string, _ *RuntimeHostPrincipal) {
			registration.Capabilities.CapabilityHash = "cap-other"
		}, want: ErrRuntimeHeartbeatStale},
		{name: "capability_tools", mutate: func(registration *RuntimeHostRegistration, _ *RuntimeHostRegistrationProof, _ *string, _ *string, _ *RuntimeHostPrincipal) {
			registration.Capabilities.Tools = append([]ToolCapability(nil), registration.Capabilities.Tools...)
			registration.Capabilities.Tools[0].SchemaHash = "sha256:other"
		}, want: ErrRuntimeHeartbeatStale},
		{name: "capability_budget", mutate: func(registration *RuntimeHostRegistration, _ *RuntimeHostRegistrationProof, _ *string, _ *string, _ *RuntimeHostPrincipal) {
			registration.Capabilities.MaxToolCallsSupported = 201
		}, want: ErrRuntimeHeartbeatStale},
		{name: "session_store", mutate: func(registration *RuntimeHostRegistration, _ *RuntimeHostRegistrationProof, _ *string, _ *string, _ *RuntimeHostPrincipal) {
			registration.SessionStoreID = "store-other"
		}, want: ErrRuntimeHeartbeatStale},
		{name: "max_active_runs", mutate: func(registration *RuntimeHostRegistration, _ *RuntimeHostRegistrationProof, _ *string, _ *string, _ *RuntimeHostPrincipal) {
			registration.MaxActiveRuns = 9
		}, want: ErrRuntimeHeartbeatStale},
		{name: "max_product_thread_runs", mutate: func(registration *RuntimeHostRegistration, _ *RuntimeHostRegistrationProof, _ *string, _ *string, _ *RuntimeHostPrincipal) {
			registration.MaxProductThreadRuns = 6
		}, want: ErrRuntimeHeartbeatStale},
		{name: "max_detached_task_runs", mutate: func(registration *RuntimeHostRegistration, _ *RuntimeHostRegistrationProof, _ *string, _ *string, _ *RuntimeHostPrincipal) {
			registration.MaxDetachedTaskRuns = 4
		}, want: ErrRuntimeHeartbeatStale},
		{name: "observed_at", mutate: func(_ *RuntimeHostRegistration, proof *RuntimeHostRegistrationProof, _ *string, _ *string, _ *RuntimeHostPrincipal) {
			proof.ObservedAt = now.Add(time.Second)
		}, want: ErrRuntimeHeartbeatStale},
		{name: "nonce", mutate: func(_ *RuntimeHostRegistration, proof *RuntimeHostRegistrationProof, _ *string, _ *string, _ *RuntimeHostPrincipal) {
			proof.Nonce = "other-nonce"
		}, want: ErrRuntimeHeartbeatStale},
		{name: "body_hash", mutate: func(_ *RuntimeHostRegistration, proof *RuntimeHostRegistrationProof, _ *string, _ *string, _ *RuntimeHostPrincipal) {
			proof.BodySHA256 = "sha256:tampered"
		}, want: ErrRuntimeHeartbeatStale},
		{name: "signature", mutate: func(_ *RuntimeHostRegistration, proof *RuntimeHostRegistrationProof, _ *string, _ *string, _ *RuntimeHostPrincipal) {
			proof.Signature = "tampered"
		}, want: ErrRuntimeHeartbeatStale},
		{name: "signature_key", mutate: func(_ *RuntimeHostRegistration, proof *RuntimeHostRegistrationProof, _ *string, _ *string, _ *RuntimeHostPrincipal) {
			proof.SignatureKeyID = "unknown"
		}, want: ErrRuntimeHostUnauthorized},
		{name: "method", mutate: func(_ *RuntimeHostRegistration, _ *RuntimeHostRegistrationProof, method *string, _ *string, _ *RuntimeHostPrincipal) {
			*method = "PUT"
		}, want: ErrRuntimeHeartbeatStale},
		{name: "path", mutate: func(_ *RuntimeHostRegistration, _ *RuntimeHostRegistrationProof, _ *string, path *string, _ *RuntimeHostPrincipal) {
			*path = "/internal/v1/runtime-hosts/other/register"
		}, want: ErrRuntimeHeartbeatStale},
		{name: "principal_host", mutate: func(_ *RuntimeHostRegistration, _ *RuntimeHostRegistrationProof, _ *string, _ *string, principal *RuntimeHostPrincipal) {
			principal.RuntimeHostID = "other-host"
		}, want: ErrRuntimeHeartbeatStale},
		{name: "principal_instance", mutate: func(_ *RuntimeHostRegistration, _ *RuntimeHostRegistrationProof, _ *string, _ *string, principal *RuntimeHostPrincipal) {
			principal.InstanceID = "other-instance"
		}, want: ErrRuntimeHeartbeatStale},
		{name: "principal_environment", mutate: func(_ *RuntimeHostRegistration, _ *RuntimeHostRegistrationProof, _ *string, _ *string, principal *RuntimeHostPrincipal) {
			principal.Environment = "other"
		}, want: ErrRuntimeHeartbeatStale},
	}
	for _, testCase := range tamperCases {
		t.Run(testCase.name, func(t *testing.T) {
			proof, err := signer.SignRegistration("POST", "/internal/v1/runtime-hosts/register", principal, registration, RuntimeHostRegistrationProof{ObservedAt: now})
			if err != nil {
				t.Fatal(err)
			}
			candidateRegistration := registration
			candidateProof := proof
			candidateMethod := "POST"
			candidatePath := "/internal/v1/runtime-hosts/register"
			candidatePrincipal := principal
			testCase.mutate(&candidateRegistration, &candidateProof, &candidateMethod, &candidatePath, &candidatePrincipal)
			if err := verifier.VerifyRegistration(context.Background(), candidatePrincipal, candidateMethod, candidatePath, candidateRegistration, candidateProof); !errors.Is(err, testCase.want) {
				t.Fatalf("registration tamper error=%v want=%v", err, testCase.want)
			}
		})
	}

	staleProof, err := signer.SignRegistration("POST", "/internal/v1/runtime-hosts/register", principal, registration, RuntimeHostRegistrationProof{ObservedAt: now.Add(-31 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	if err := verifier.VerifyRegistration(context.Background(), principal, "POST", "/internal/v1/runtime-hosts/register", registration, staleProof); !errors.Is(err, ErrRuntimeHeartbeatStale) {
		t.Fatalf("registration timestamp error=%v", err)
	}
}

func TestEd25519RuntimeHostHeartbeatVerifierRejectsRevokedKey(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := NewEd25519RuntimeHostHeartbeatSigner("key-revoked", privateKey)
	if err != nil {
		t.Fatal(err)
	}
	verifier, err := NewEd25519RuntimeHostHeartbeatVerifier([]RuntimeHostVerificationKey{{KeyID: "key-revoked", PublicKey: publicKey, Revoked: true}}, nil, time.Second, 2*time.Second, NewMemoryRuntimeHostNonceStore())
	if err != nil {
		t.Fatal(err)
	}
	principal := RuntimeHostPrincipal{RuntimeHostID: "host", InstanceID: "instance", Environment: "test", CertificateID: "cert"}
	heartbeat, err := signer.SignHeartbeat("POST", "/internal/v1/runtime-hosts/host/heartbeat", principal, RuntimeHostHeartbeat{Sequence: 1, ObservedAt: time.Now().UTC(), CapabilityHash: "cap"})
	if err != nil {
		t.Fatal(err)
	}
	if err := verifier.VerifyHeartbeat(context.Background(), principal, "POST", "/internal/v1/runtime-hosts/host/heartbeat", heartbeat); !errors.Is(err, ErrRuntimeHostUnauthorized) {
		t.Fatalf("revoked key error=%v", err)
	}
}

func TestEd25519RuntimeHostHeartbeatVerifierSupportsBoundedKeyOverlapAndRevocation(t *testing.T) {
	oldPublic, oldPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	newPublic, newPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	oldSigner, err := NewEd25519RuntimeHostHeartbeatSigner("key-old", oldPrivate)
	if err != nil {
		t.Fatal(err)
	}
	newSigner, err := NewEd25519RuntimeHostHeartbeatSigner("key-new", newPrivate)
	if err != nil {
		t.Fatal(err)
	}
	nonces := NewMemoryRuntimeHostNonceStore()
	verifier, err := NewEd25519RuntimeHostHeartbeatVerifier([]RuntimeHostVerificationKey{
		{KeyID: "key-old", PublicKey: oldPublic}, {KeyID: "key-new", PublicKey: newPublic},
	}, nil, time.Minute, 2*time.Minute, nonces)
	if err != nil {
		t.Fatal(err)
	}
	principal := RuntimeHostPrincipal{RuntimeHostID: "host", InstanceID: "instance", Environment: "test", CertificateID: "cert"}
	for sequence, signer := range map[int64]RuntimeHostHeartbeatSigner{1: oldSigner, 2: newSigner} {
		heartbeat, err := signer.SignHeartbeat("POST", "/internal/v1/runtime-hosts/host/heartbeat", principal, RuntimeHostHeartbeat{Sequence: sequence, ObservedAt: time.Now().UTC(), CapabilityHash: "cap"})
		if err != nil {
			t.Fatal(err)
		}
		if err := verifier.VerifyHeartbeat(context.Background(), principal, "POST", "/internal/v1/runtime-hosts/host/heartbeat", heartbeat); err != nil {
			t.Fatalf("overlap key sequence=%d rejected: %v", sequence, err)
		}
	}
	old := verifier.keys["key-old"]
	old.Revoked = true
	verifier.keys["key-old"] = old
	heartbeat, err := oldSigner.SignHeartbeat("POST", "/internal/v1/runtime-hosts/host/heartbeat", principal, RuntimeHostHeartbeat{Sequence: 3, ObservedAt: time.Now().UTC(), CapabilityHash: "cap"})
	if err != nil {
		t.Fatal(err)
	}
	if err := verifier.VerifyHeartbeat(context.Background(), principal, "POST", "/internal/v1/runtime-hosts/host/heartbeat", heartbeat); !errors.Is(err, ErrRuntimeHostUnauthorized) {
		t.Fatalf("revoked overlap key error=%v", err)
	}
}
