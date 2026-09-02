package runtime

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRuntimeWorkspaceMaterializerMaterializesInlineManifestFromAccessibleTmpRoot(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	manifest := materializerTestManifest(now)
	secret := "runtime-ticket-test-secret"
	ticket := materializerTestTicket(t, secret, manifest, now)
	materializer := NewRuntimeWorkspaceMaterializer(manifest.RuntimeHostID, t.TempDir(), secret, nil, NewMemoryRunTicketJTIStore())
	materializer.Now = func() time.Time { return now }

	first, err := materializer.Materialize(context.Background(), ticket, manifest)
	if err != nil {
		t.Fatalf("materialize inline manifest: %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(first.Root, "input", "request.md")); err != nil || string(got) != "hello" {
		t.Fatalf("materialized input=%q err=%v", got, err)
	}
	second, err := materializer.Materialize(context.Background(), ticket, manifest)
	if err != nil || second.Root != first.Root || second.ManifestHash != manifest.ManifestHash {
		t.Fatalf("matching materialization retry root=%q manifest=%q err=%v", second.Root, second.ManifestHash, err)
	}
}

func TestRuntimeWorkspaceMaterializerRejectsCorruptCachedMaterializedFiles(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	tests := []struct {
		name    string
		corrupt func(*testing.T, string)
	}{
		{
			name: "same-size hash mismatch",
			corrupt: func(t *testing.T, root string) {
				t.Helper()
				path := filepath.Join(root, "input", "request.md")
				if err := os.Chmod(path, 0o640); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, []byte("jello"), 0o640); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "size mismatch",
			corrupt: func(t *testing.T, root string) {
				t.Helper()
				path := filepath.Join(root, "input", "request.md")
				if err := os.Chmod(path, 0o640); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, []byte("too long"), 0o640); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "non-regular file",
			corrupt: func(t *testing.T, root string) {
				t.Helper()
				path := filepath.Join(root, "input", "request.md")
				if err := os.Chmod(path, 0o640); err != nil {
					t.Fatal(err)
				}
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				if err := os.Mkdir(path, 0o750); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "symlinked file",
			corrupt: func(t *testing.T, root string) {
				t.Helper()
				path := filepath.Join(root, "input", "request.md")
				if err := os.Chmod(path, 0o640); err != nil {
					t.Fatal(err)
				}
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				target := filepath.Join(t.TempDir(), "request.md")
				if err := os.WriteFile(target, []byte("hello"), 0o640); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(target, path); err != nil {
					t.Skipf("symlink unavailable on this platform: %v", err)
				}
			},
		},
		{
			name: "symlinked parent directory",
			corrupt: func(t *testing.T, root string) {
				t.Helper()
				path := filepath.Join(root, "input", "request.md")
				if err := os.Chmod(path, 0o640); err != nil {
					t.Fatal(err)
				}
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				inputRoot := filepath.Join(root, "input")
				if err := os.Remove(inputRoot); err != nil {
					t.Fatal(err)
				}
				target := t.TempDir()
				if err := os.WriteFile(filepath.Join(target, "request.md"), []byte("hello"), 0o640); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(target, inputRoot); err != nil {
					t.Skipf("symlink unavailable on this platform: %v", err)
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			manifest := materializerTestManifest(now)
			secret := "runtime-ticket-test-secret"
			ticket := materializerTestTicket(t, secret, manifest, now)
			materializer := NewRuntimeWorkspaceMaterializer(manifest.RuntimeHostID, t.TempDir(), secret, nil, NewMemoryRunTicketJTIStore())
			materializer.Now = func() time.Time { return now }
			workspace, err := materializer.Materialize(context.Background(), ticket, manifest)
			if err != nil {
				t.Fatalf("initial materialization: %v", err)
			}
			test.corrupt(t, workspace.Root)
			if _, err := materializer.Materialize(context.Background(), ticket, manifest); err == nil || err.Error() != "RUNTIME_WORKSPACE_MATERIALIZATION_FAILED" {
				t.Fatalf("corrupt cached materialization error=%v, want RUNTIME_WORKSPACE_MATERIALIZATION_FAILED", err)
			}
		})
	}
}

func TestRuntimeWorkspaceMaterializerRejectsSymlinkedTmpRoot(t *testing.T) {
	target := t.TempDir()
	link := filepath.Join(t.TempDir(), "runtime-root-link")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlink unavailable on this platform: %v", err)
	}
	if _, err := validatedMaterializationRoot(link); err == nil {
		t.Fatal("symlinked runtime tmp root must be rejected")
	}
}

func TestRuntimeWorkspaceMaterializerFailsClosedWhenRunTicketJTIStoreIsUnavailable(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	manifest := materializerTestManifest(now)
	secret := "runtime-ticket-test-secret"
	ticket := materializerTestTicket(t, secret, manifest, now)
	materializer := NewRuntimeWorkspaceMaterializer(manifest.RuntimeHostID, t.TempDir(), secret, nil, NewUnavailableRunTicketJTIStore())
	materializer.Now = func() time.Time { return now }

	_, err := materializer.Materialize(context.Background(), ticket, manifest)
	if !errors.Is(err, ErrRunTicketJTIStoreUnavailable) {
		t.Fatalf("materialization error=%v", err)
	}
}

func TestRuntimeWorkspaceMaterializerFailsClosedWhenRunTicketJTIStoreIsNil(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	manifest := materializerTestManifest(now)
	secret := "runtime-ticket-test-secret"
	ticket := materializerTestTicket(t, secret, manifest, now)
	materializer := NewRuntimeWorkspaceMaterializer(manifest.RuntimeHostID, t.TempDir(), secret, nil, nil)
	materializer.Now = func() time.Time { return now }

	_, err := materializer.Materialize(context.Background(), ticket, manifest)
	if !errors.Is(err, ErrRunTicketJTIStoreUnavailable) {
		t.Fatalf("nil-store materialization error=%v", err)
	}
}

func materializerTestManifest(now time.Time) RuntimeInputManifest {
	manifest := RuntimeInputManifest{
		SchemaVersion: "runtime_input_manifest.v1", RunID: "run_materializer_test", RuntimeHostID: "host_test",
		TenantID: "tenant_test", UserID: "user_test", WorkspaceID: "workspace_test",
		WorkspaceVersion: 3, ThreadWorkspaceBindingVersion: 2, ContextGeneration: 4,
		MetaRelease: "release-v5", AgentProfile: "general_agent", AgentHash: strings.Repeat("a", 64),
		SkillProfiles:  []RuntimeSkillProfile{{Profile: "general_chat", Hash: strings.Repeat("b", 64)}},
		CapabilityHash: "capability-v5", Files: []RuntimeManifestEntry{NewInlineRuntimeEntry("input/request.md", []byte("hello"))},
		ExpiresAt: now.Add(5 * time.Minute),
	}
	manifest.ManifestHash = (WorkspaceComposer{}).ComputeManifestHash(manifest)
	return manifest
}

func materializerTestTicket(t *testing.T, secret string, manifest RuntimeInputManifest, now time.Time) string {
	t.Helper()
	ticket, err := SignRunTicket(RunTicketClaims{
		RunID: manifest.RunID, TenantID: manifest.TenantID, ReservationID: "reservation_1", RuntimeHostID: manifest.RuntimeHostID,
		CapabilityHash: manifest.CapabilityHash, WorkspaceID: manifest.WorkspaceID,
		WorkspaceVersion: manifest.WorkspaceVersion, ContextGeneration: manifest.ContextGeneration,
		InputManifestHash: manifest.ManifestHash, PlanHash: "sha256:" + strings.Repeat("c", 64), FencingToken: 7, JTI: "ticket_jti_materializer",
		IssuedAt: now.Add(-time.Second).Unix(), ExpiresAt: manifest.ExpiresAt.Unix(),
	}, secret)
	if err != nil {
		t.Fatal(err)
	}
	return ticket
}
