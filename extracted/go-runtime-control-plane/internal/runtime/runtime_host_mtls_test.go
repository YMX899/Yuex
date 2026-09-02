package runtime

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"testing"
	"time"
)

func TestRuntimeHostIdentityMaterialLoadsEd25519SignerAndVerifier(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	privateDER, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		t.Fatal(err)
	}
	publicDER, err := x509.MarshalPKIXPublicKey(publicKey)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("HUAHUO_TEST_RUNTIME_HOST_PRIVATE", string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privateDER})))
	t.Setenv("HUAHUO_TEST_RUNTIME_HOST_PUBLIC", string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: publicDER})))
	t.Setenv("HUAHUO_TEST_RUNTIME_HOST_REVOKED", "[]")
	signer, err := LoadEd25519RuntimeHostHeartbeatSigner("key-test", "env://HUAHUO_TEST_RUNTIME_HOST_PRIVATE")
	if err != nil {
		t.Fatal(err)
	}
	verifier, err := LoadEd25519RuntimeHostHeartbeatVerifier("key-test", "env://HUAHUO_TEST_RUNTIME_HOST_PUBLIC", "env://HUAHUO_TEST_RUNTIME_HOST_REVOKED", 30*time.Second, 2*time.Minute, NewMemoryRuntimeHostNonceStore())
	if err != nil {
		t.Fatal(err)
	}
	principal := RuntimeHostPrincipal{Environment: "test", RuntimeHostID: "host_test", InstanceID: "instance_test", CertificateID: "cert_test"}
	heartbeat, err := signer.SignHeartbeat("POST", "/internal/v1/runtime-hosts/host_test/heartbeat", principal, RuntimeHostHeartbeat{Sequence: 1, ObservedAt: time.Now().UTC(), CapabilityHash: "cap_test"})
	if err != nil {
		t.Fatal(err)
	}
	if err := verifier.VerifyHeartbeat(context.Background(), principal, "POST", "/internal/v1/runtime-hosts/host_test/heartbeat", heartbeat); err != nil {
		t.Fatal(err)
	}
}
