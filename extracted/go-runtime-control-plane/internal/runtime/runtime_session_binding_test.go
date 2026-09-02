package runtime

import (
	"context"
	"testing"
)

func TestProductSessionBindingCreateReuseAndRotate(t *testing.T) {
	repository := NewRuntimeHostRepository(nil)
	command := ProductSessionBindingCommand{
		ThreadID: "thread_binding", TenantID: "tenant_1", UserID: "user_1", WorkspaceID: "workspace_1",
		AgentProfile: "work_ai_agent", ContextGeneration: 3, ManifestVersion: "manifest-v1",
		AgentHash: "sha256:agent", SessionKeyEncryptionSecret: "runtime-session-secret-test",
	}
	first, err := repository.EnsureProductSessionBinding(context.Background(), command)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := repository.EnsureProductSessionBinding(context.Background(), command)
	if err != nil {
		t.Fatal(err)
	}
	if replayed.BindingID != first.BindingID || replayed.OpenClawSessionKey != first.OpenClawSessionKey || first.SessionGeneration != 1 {
		t.Fatalf("binding replay changed identity: first=%+v replayed=%+v", first, replayed)
	}

	rotated, err := repository.RotateProductSessionGeneration(context.Background(), ProductSessionHostBinding{
		TenantID: first.TenantID, ThreadID: first.ThreadID, AgentProfile: first.AgentProfile, ContextGeneration: first.ContextGeneration,
		SessionGeneration: first.SessionGeneration,
	}, command, "host_offline", "medium")
	if err != nil {
		t.Fatal(err)
	}
	if rotated.SessionGeneration != 2 || rotated.RecoveredFromGeneration != 1 || rotated.RecoveryMode != "host_offline" || rotated.OpenClawSessionKey == first.OpenClawSessionKey || rotated.RuntimeHostID != "" {
		t.Fatalf("rotated binding=%+v", rotated)
	}
	if _, err := repository.GetProductSessionBinding(context.Background(), ProductSessionHostBinding{
		TenantID: first.TenantID, ThreadID: first.ThreadID, AgentProfile: first.AgentProfile, ContextGeneration: first.ContextGeneration,
		SessionGeneration: first.SessionGeneration,
	}, command.SessionKeyEncryptionSecret); err == nil {
		t.Fatal("orphaned generation remained active")
	}
}

func TestRuntimeSessionCipherUsesAuthenticatedEncryption(t *testing.T) {
	binding := ProductSessionBinding{TenantID: "tenant_cipher", ThreadID: "thread_cipher", AgentProfile: "agent", ContextGeneration: 1, SessionGeneration: 1}
	aad := productSessionBindingAAD(binding)
	ciphertext, err := encryptRuntimeSessionKey("oc:ps:test-key", "runtime-session-secret-test", aad)
	if err != nil {
		t.Fatal(err)
	}
	if ciphertext == "oc:ps:test-key" || len(ciphertext) <= len(runtimeSessionCipherPrefix) {
		t.Fatalf("ciphertext leaked plaintext: %q", ciphertext)
	}
	plain, err := decryptRuntimeSessionKey(ciphertext, "runtime-session-secret-test", aad)
	if err != nil || plain != "oc:ps:test-key" {
		t.Fatalf("decrypt plain=%q err=%v", plain, err)
	}
	if _, err := decryptRuntimeSessionKey(ciphertext, "wrong-runtime-session-secret", aad); err == nil {
		t.Fatal("wrong secret decrypted session key")
	}
}
