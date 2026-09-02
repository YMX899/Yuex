package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	runtimepkg "huahuoai/backend/source/internal/runtime"
)

func TestHostManifestSourceResolverFetchesSignedRemoteObjectAndUsesHashCache(t *testing.T) {
	now := time.Date(2026, 7, 18, 10, 0, 0, 0, time.UTC)
	body := []byte("remote-image-bytes")
	var calls atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		if request.Method != http.MethodGet || request.URL.Query().Get("signature") != "opaque" || request.Header.Get("Accept-Encoding") != "identity" {
			t.Fatalf("remote request=%s %s headers=%#v", request.Method, request.URL.String(), request.Header)
		}
		w.Header().Set("Content-Type", "image/png")
		w.Header().Set("Content-Length", "18")
		_, _ = w.Write(body)
	}))
	defer server.Close()

	root := t.TempDir()
	resolver := hostManifestSourceResolver{
		objectCacheRoot: root, remoteHTTPClient: server.Client(), remoteFetchTimeout: time.Second,
		now: func() time.Time { return now },
	}
	manifest := remoteObjectReadTestManifest(t, "run_remote_one", now, server.URL+"/objects/one.png?signature=opaque", body, "image/png", now.Add(4*time.Minute))
	materializer := runtimepkg.NewRuntimeWorkspaceMaterializer(manifest.RuntimeHostID, root, "remote-object-test-secret", resolver, runtimepkg.NewMemoryRunTicketJTIStore())
	materializer.Now = func() time.Time { return now }
	first, err := materializer.Materialize(context.Background(), remoteObjectReadTestTicket(t, manifest, now), manifest)
	if err != nil {
		t.Fatalf("materialize remote object: %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(first.Root, "input", "attachments", "01.png")); err != nil || string(got) != string(body) {
		t.Fatalf("materialized remote input=%q err=%v", got, err)
	}
	if calls.Load() != 1 {
		t.Fatalf("remote calls=%d want=1", calls.Load())
	}
	hash := sha256.Sum256(body)
	if _, err := os.Stat(filepath.Join(root, "runtime-object-cache", "sha256-"+hex.EncodeToString(hash[:])+".blob")); err != nil {
		t.Fatalf("cache file missing: %v", err)
	}

	// A distinct Run with the same immutable object can use the verified Host
	// cache, but must still carry its own unexpired signed read reference.
	secondManifest := remoteObjectReadTestManifest(t, "run_remote_two", now, server.URL+"/objects/one.png?signature=opaque", body, "image/png", now.Add(4*time.Minute))
	secondMaterializer := runtimepkg.NewRuntimeWorkspaceMaterializer(secondManifest.RuntimeHostID, root, "remote-object-test-secret", resolver, runtimepkg.NewMemoryRunTicketJTIStore())
	secondMaterializer.Now = func() time.Time { return now }
	if _, err := secondMaterializer.Materialize(context.Background(), remoteObjectReadTestTicket(t, secondManifest, now), secondManifest); err != nil {
		t.Fatalf("materialize cached remote object: %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("verified cache should avoid another remote fetch, calls=%d", calls.Load())
	}
}

func TestHostManifestSourceResolverFailsClosedForRemoteObjectMismatchExpiryAndFailure(t *testing.T) {
	now := time.Date(2026, 7, 18, 10, 0, 0, 0, time.UTC)
	wantBody := []byte("remote-image-bytes")
	wrongHashBody := []byte("remote-image-byteX")
	if len(wrongHashBody) != len(wantBody) {
		t.Fatal("test body lengths must match")
	}
	tests := []struct {
		name        string
		body        []byte
		contentType string
		status      int
		expiresAt   time.Time
		wantCalls   int32
	}{
		{name: "hash mismatch", body: wrongHashBody, contentType: "image/png", status: http.StatusOK, expiresAt: now.Add(4 * time.Minute), wantCalls: 1},
		{name: "mime mismatch", body: wantBody, contentType: "image/jpeg", status: http.StatusOK, expiresAt: now.Add(4 * time.Minute), wantCalls: 1},
		{name: "expired reference", body: wantBody, contentType: "image/png", status: http.StatusOK, expiresAt: now.Add(-time.Second), wantCalls: 0},
		{name: "provider failure", body: nil, contentType: "image/png", status: http.StatusForbidden, expiresAt: now.Add(4 * time.Minute), wantCalls: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				calls.Add(1)
				w.Header().Set("Content-Type", test.contentType)
				w.WriteHeader(test.status)
				_, _ = w.Write(test.body)
			}))
			defer server.Close()
			root := t.TempDir()
			resolver := hostManifestSourceResolver{
				objectCacheRoot: root, remoteHTTPClient: server.Client(), remoteFetchTimeout: time.Second,
				now: func() time.Time { return now },
			}
			manifest := remoteObjectReadTestManifest(t, "run_remote_failure", now, server.URL+"/objects/one.png?signature=opaque", wantBody, "image/png", test.expiresAt)
			materializer := runtimepkg.NewRuntimeWorkspaceMaterializer(manifest.RuntimeHostID, root, "remote-object-test-secret", resolver, runtimepkg.NewMemoryRunTicketJTIStore())
			materializer.Now = func() time.Time { return now }
			if _, err := materializer.Materialize(context.Background(), remoteObjectReadTestTicket(t, manifest, now), manifest); err == nil || err.Error() != "RUNTIME_WORKSPACE_MATERIALIZATION_FAILED" {
				t.Fatalf("remote failure error=%v", err)
			}
			if calls.Load() != test.wantCalls {
				t.Fatalf("remote calls=%d want=%d", calls.Load(), test.wantCalls)
			}
		})
	}
}

func TestHostManifestSourceResolverPreservesMountedObjectReferenceFlow(t *testing.T) {
	now := time.Date(2026, 7, 18, 10, 0, 0, 0, time.UTC)
	body := []byte("mounted-image-bytes")
	root := t.TempDir()
	localPath := filepath.Join(root, "media", "resources", "mounted.png")
	if err := os.MkdirAll(filepath.Dir(localPath), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(localPath, body, 0o440); err != nil {
		t.Fatal(err)
	}
	manifest := remoteObjectReadTestManifest(t, "run_mounted_reference", now, "", body, "image/png", time.Time{})
	manifest.Files[0].ObjectRead = nil
	manifest.Files[0].SourceRef = "media/resources/mounted.png"
	manifest.ManifestHash = (runtimepkg.WorkspaceComposer{}).ComputeManifestHash(manifest)
	resolver := hostManifestSourceResolver{objectCacheRoot: root, now: func() time.Time { return now }}
	materializer := runtimepkg.NewRuntimeWorkspaceMaterializer(manifest.RuntimeHostID, root, "remote-object-test-secret", resolver, runtimepkg.NewMemoryRunTicketJTIStore())
	materializer.Now = func() time.Time { return now }
	materialized, err := materializer.Materialize(context.Background(), remoteObjectReadTestTicket(t, manifest, now), manifest)
	if err != nil {
		t.Fatalf("materialize mounted object reference: %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(materialized.Root, "input", "attachments", "01.png")); err != nil || string(got) != string(body) {
		t.Fatalf("mounted materialized content=%q err=%v", got, err)
	}
}

func remoteObjectReadTestManifest(t *testing.T, runID string, now time.Time, readURL string, body []byte, mimeType string, readExpiry time.Time) runtimepkg.RuntimeInputManifest {
	t.Helper()
	hash := sha256.Sum256(body)
	entry := runtimepkg.RuntimeManifestEntry{
		LogicalPath: "input/attachments/01.png", SourceType: "object_ref", SourceRef: "runtime-attachments/resource_remote_object",
		SizeBytes: int64(len(body)), SHA256: "sha256:" + hex.EncodeToString(hash[:]),
	}
	if readURL != "" {
		entry.ObjectRead = &runtimepkg.RuntimeObjectReadReference{URL: readURL, ExpiresAt: readExpiry, MIMEType: mimeType}
	}
	manifest := runtimepkg.RuntimeInputManifest{
		SchemaVersion: "runtime_input_manifest.v1", RunID: runID, RuntimeHostID: "host_remote_object",
		TenantID: "tenant_remote_object", UserID: "user_remote_object", WorkspaceID: "workspace_remote_object",
		WorkspaceVersion: 1, ThreadWorkspaceBindingVersion: 1, ContextGeneration: 1,
		MetaRelease: "release_remote_object", AgentProfile: "visual_chat_agent", MetaWorkspaceKey: "visual_chat", MetaWorkspaceVersion: "v1",
		InputPolicyHash: "sha256:" + strings.Repeat("c", 64), AgentHash: strings.Repeat("a", 64),
		SkillProfiles: []runtimepkg.RuntimeSkillProfile{{Profile: "visual_chat", Hash: strings.Repeat("b", 64)}}, CapabilityHash: "capability_remote_object",
		Attachments: []runtimepkg.AgentRunInputAttachmentIdentity{{
			ResourceID: "resource_remote_object", Usage: "primary_input", MIMEType: mimeType, SizeBytes: int64(len(body)), SHA256: entry.SHA256,
			Width: 1, Height: 1, LogicalPath: entry.LogicalPath,
		}},
		Files:     []runtimepkg.RuntimeManifestEntry{entry, runtimepkg.NewInlineRuntimeEntry("input/attachments.json", []byte(`{"schemaVersion":"runtime_input_attachments.v1","items":[{"referenceResourceId":"resource_remote_object"}]}`))},
		ExpiresAt: now.Add(5 * time.Minute),
	}
	manifest.ManifestHash = (runtimepkg.WorkspaceComposer{}).ComputeManifestHash(manifest)
	return manifest
}

func remoteObjectReadTestTicket(t *testing.T, manifest runtimepkg.RuntimeInputManifest, now time.Time) string {
	t.Helper()
	ticket, err := runtimepkg.SignRunTicket(runtimepkg.RunTicketClaims{
		RunID: manifest.RunID, TenantID: manifest.TenantID, ReservationID: "reservation_remote_object", RuntimeHostID: manifest.RuntimeHostID,
		CapabilityHash: manifest.CapabilityHash, WorkspaceID: manifest.WorkspaceID, WorkspaceVersion: manifest.WorkspaceVersion,
		ContextGeneration: manifest.ContextGeneration, InputManifestHash: manifest.ManifestHash, PlanHash: "sha256:" + strings.Repeat("d", 64),
		FencingToken: 1, JTI: "jti_" + manifest.RunID, IssuedAt: now.Add(-time.Second).Unix(), ExpiresAt: manifest.ExpiresAt.Unix(),
	}, "remote-object-test-secret")
	if err != nil {
		t.Fatal(err)
	}
	return ticket
}
