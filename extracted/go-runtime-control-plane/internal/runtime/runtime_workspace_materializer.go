package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type RuntimeManifestSourceResolver interface {
	Resolve(ctx context.Context, manifest RuntimeInputManifest, entry RuntimeManifestEntry) ([]byte, error)
}

// ErrRunTicketJTIStoreUnavailable means the Host cannot prove replay protection
// from durable state. Callers must fail closed rather than accept through a
// process-local cache.
var ErrRunTicketJTIStoreUnavailable = errors.New("RUNTIME_STORAGE_UNAVAILABLE")

type RunTicketJTIStore interface {
	Consume(ctx context.Context, jti string, expiresAt time.Time) (bool, error)
}

// RunTicketJTIStoreHealth is optional for test-only stores, but production
// Adapter startup requires it so a missing or unavailable durable store cannot
// become an in-memory acceptance path.
type RunTicketJTIStoreHealth interface {
	Health(ctx context.Context) error
}

// RunTicketJTIStoreDurability distinguishes a test/local memory cache from a
// Host state store. Production Adapter startup must require true.
type RunTicketJTIStoreDurability interface {
	Durable() bool
}

// RunTicketJTIStoreProbe proves that a configured durable store can create,
// sync and remove a private state record before production Adapter startup.
type RunTicketJTIStoreProbe interface {
	Probe(ctx context.Context) error
}

type MemoryRunTicketJTIStore struct {
	mu   sync.Mutex
	used map[string]time.Time
}

func NewMemoryRunTicketJTIStore() *MemoryRunTicketJTIStore {
	return &MemoryRunTicketJTIStore{used: map[string]time.Time{}}
}

func (s *MemoryRunTicketJTIStore) Consume(ctx context.Context, jti string, expiresAt time.Time) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	for key, expiry := range s.used {
		if !expiry.After(now) {
			delete(s.used, key)
		}
	}
	if _, exists := s.used[jti]; exists {
		return false, nil
	}
	s.used[jti] = expiresAt
	return true, nil
}

func (s *MemoryRunTicketJTIStore) Health(ctx context.Context) error {
	if s == nil {
		return ErrRunTicketJTIStoreUnavailable
	}
	return ctx.Err()
}

func (*MemoryRunTicketJTIStore) Durable() bool { return false }

type MaterializedWorkspace struct {
	RunID        string    `json:"runId"`
	Root         string    `json:"-"`
	InputRoot    string    `json:"-"`
	OutputRoot   string    `json:"-"`
	StagingRoot  string    `json:"-"`
	ManifestHash string    `json:"manifestHash"`
	ExpiresAt    time.Time `json:"expiresAt"`
}

type RuntimeWorkspaceMaterializer struct {
	RuntimeHostID string
	TmpRoot       string
	TicketSecret  string
	Sources       RuntimeManifestSourceResolver
	JTIs          RunTicketJTIStore
	Now           func() time.Time
}

func NewRuntimeWorkspaceMaterializer(runtimeHostID, tmpRoot, ticketSecret string, sources RuntimeManifestSourceResolver, jtis RunTicketJTIStore) RuntimeWorkspaceMaterializer {
	if jtis == nil {
		jtis = NewUnavailableRunTicketJTIStore()
	}
	return RuntimeWorkspaceMaterializer{RuntimeHostID: runtimeHostID, TmpRoot: tmpRoot, TicketSecret: ticketSecret, Sources: sources, JTIs: jtis, Now: time.Now}
}

func (m RuntimeWorkspaceMaterializer) Materialize(ctx context.Context, runTicket string, manifest RuntimeInputManifest) (MaterializedWorkspace, error) {
	now := time.Now().UTC()
	if m.Now != nil {
		now = m.Now().UTC()
	}
	claims, err := VerifyRunTicket(runTicket, m.TicketSecret, now)
	if err != nil {
		return MaterializedWorkspace{}, materializationError()
	}
	computedHash := (WorkspaceComposer{}).ComputeManifestHash(manifest)
	if m.RuntimeHostID == "" || claims.RuntimeHostID != m.RuntimeHostID || manifest.RuntimeHostID != m.RuntimeHostID || claims.RunID != manifest.RunID ||
		claims.WorkspaceID != manifest.WorkspaceID || claims.WorkspaceVersion != manifest.WorkspaceVersion || claims.ContextGeneration != manifest.ContextGeneration ||
		claims.CapabilityHash != manifest.CapabilityHash || claims.InputManifestHash == "" || claims.InputManifestHash != manifest.ManifestHash || computedHash != manifest.ManifestHash ||
		!manifest.ExpiresAt.After(now) || claims.ExpiresAt != manifest.ExpiresAt.Unix() {
		return MaterializedWorkspace{}, materializationError()
	}
	if err := (WorkspaceComposer{}).ValidateLogicalFiles(manifest); err != nil {
		return MaterializedWorkspace{}, materializationError()
	}
	baseRoot, err := validatedMaterializationRoot(m.TmpRoot)
	if err != nil {
		return MaterializedWorkspace{}, materializationError()
	}
	if !safeRuntimeSegment(manifest.RunID) {
		return MaterializedWorkspace{}, materializationError()
	}
	finalRoot := filepath.Join(baseRoot, "runtime-workspaces", manifest.RunID)
	if m.JTIs == nil {
		return MaterializedWorkspace{}, ErrRunTicketJTIStoreUnavailable
	}
	if health, ok := m.JTIs.(RunTicketJTIStoreHealth); ok {
		if err := health.Health(ctx); err != nil {
			return MaterializedWorkspace{}, ErrRunTicketJTIStoreUnavailable
		}
	}
	if existing, ok := existingMaterializedWorkspace(finalRoot, manifest, now); ok {
		return existing, nil
	} else if _, err := os.Lstat(finalRoot); !os.IsNotExist(err) {
		return MaterializedWorkspace{}, materializationError()
	}
	consumed, consumeErr := m.JTIs.Consume(ctx, claims.JTI, time.Unix(claims.ExpiresAt, 0).UTC())
	if consumeErr != nil {
		return MaterializedWorkspace{}, ErrRunTicketJTIStoreUnavailable
	}
	if !consumed {
		return MaterializedWorkspace{}, materializationError()
	}
	tmpDir, err := os.MkdirTemp(filepath.Join(baseRoot, "runtime-workspaces"), ".materializing-"+manifest.RunID+"-")
	if err != nil {
		return MaterializedWorkspace{}, materializationError()
	}
	committed := false
	defer func() {
		if !committed {
			_ = os.RemoveAll(tmpDir)
		}
	}()
	for _, entry := range manifest.Files {
		if err := ctx.Err(); err != nil {
			return MaterializedWorkspace{}, err
		}
		content, err := m.resolveEntry(ctx, manifest, entry)
		if err != nil || int64(len(content)) != entry.SizeBytes || sha256Bytes(content) != normalizeSHA256(entry.SHA256) {
			return MaterializedWorkspace{}, materializationError()
		}
		target, err := safeMaterializationJoin(tmpDir, entry.LogicalPath)
		if err != nil {
			return MaterializedWorkspace{}, materializationError()
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o750); err != nil {
			return MaterializedWorkspace{}, materializationError()
		}
		file, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o440)
		if err != nil {
			return MaterializedWorkspace{}, materializationError()
		}
		_, writeErr := file.Write(content)
		closeErr := file.Close()
		if writeErr != nil || closeErr != nil {
			return MaterializedWorkspace{}, materializationError()
		}
	}
	for _, dir := range []string{"input", "output", "staging"} {
		if err := os.MkdirAll(filepath.Join(tmpDir, dir), 0o750); err != nil {
			return MaterializedWorkspace{}, materializationError()
		}
	}
	marker := map[string]any{"runId": manifest.RunID, "runtimeHostId": manifest.RuntimeHostID, "manifestHash": manifest.ManifestHash, "expiresAt": manifest.ExpiresAt}
	markerRaw, _ := json.Marshal(marker)
	if err := os.WriteFile(filepath.Join(tmpDir, ".materialization.json"), markerRaw, 0o440); err != nil {
		return MaterializedWorkspace{}, materializationError()
	}
	if err := os.Rename(tmpDir, finalRoot); err != nil {
		return MaterializedWorkspace{}, materializationError()
	}
	committed = true
	return MaterializedWorkspace{RunID: manifest.RunID, Root: finalRoot, InputRoot: filepath.Join(finalRoot, "input"), OutputRoot: filepath.Join(finalRoot, "output"), StagingRoot: filepath.Join(finalRoot, "staging"), ManifestHash: manifest.ManifestHash, ExpiresAt: manifest.ExpiresAt}, nil
}

func (m RuntimeWorkspaceMaterializer) Cleanup(ctx context.Context, runID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !safeRuntimeSegment(runID) {
		return materializationError()
	}
	baseRoot, err := validatedMaterializationRoot(m.TmpRoot)
	if err != nil {
		return materializationError()
	}
	target := filepath.Join(baseRoot, "runtime-workspaces", runID)
	if _, err := os.Lstat(target); os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return materializationError()
	}
	quarantine := filepath.Join(baseRoot, "runtime-workspaces", fmt.Sprintf(".cleanup-%s-%d", runID, time.Now().UTC().UnixNano()))
	if err := os.Rename(target, quarantine); err != nil {
		return materializationError()
	}
	if err := os.RemoveAll(quarantine); err != nil {
		return materializationError()
	}
	return nil
}

func (m RuntimeWorkspaceMaterializer) resolveEntry(ctx context.Context, manifest RuntimeInputManifest, entry RuntimeManifestEntry) ([]byte, error) {
	if entry.SourceType == "inline" {
		return []byte(entry.InlineContent), nil
	}
	if m.Sources == nil {
		return nil, materializationError()
	}
	return m.Sources.Resolve(ctx, manifest, entry)
}

func validatedMaterializationRoot(value string) (string, error) {
	root, err := filepath.Abs(strings.TrimSpace(value))
	if err != nil || strings.TrimSpace(value) == "" {
		return "", err
	}
	if err := ensureMaterializationDirectory(root); err != nil {
		return "", err
	}
	runtimeWorkspacesRoot := filepath.Join(root, "runtime-workspaces")
	if err := ensureMaterializationDirectory(runtimeWorkspacesRoot); err != nil {
		return "", err
	}
	return root, nil
}

func ensureMaterializationDirectory(directory string) error {
	info, err := os.Lstat(directory)
	if err == nil {
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return fmt.Errorf("unsafe runtime tmp root")
		}
		return nil
	}
	if !os.IsNotExist(err) {
		return err
	}
	if err := os.MkdirAll(directory, 0o750); err != nil {
		return err
	}
	info, err = os.Lstat(directory)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("unsafe runtime tmp root")
	}
	return nil
}

func safeMaterializationJoin(root, logicalPath string) (string, error) {
	normalized, err := normalizeRuntimeLogicalPath(logicalPath)
	if err != nil {
		return "", err
	}
	target := filepath.Join(root, filepath.FromSlash(normalized))
	rel, err := filepath.Rel(root, target)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("path escape")
	}
	return target, nil
}

func sha256Bytes(value []byte) string {
	sum := sha256.Sum256(value)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func materializationError() error { return fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED") }

func existingMaterializedWorkspace(root string, manifest RuntimeInputManifest, now time.Time) (MaterializedWorkspace, bool) {
	rootInfo, rootErr := os.Lstat(root)
	if rootErr != nil || rootInfo.Mode()&os.ModeSymlink != 0 || !rootInfo.IsDir() {
		return MaterializedWorkspace{}, false
	}
	markerRaw, ok := readCachedMaterializationMarker(root)
	if !ok {
		return MaterializedWorkspace{}, false
	}
	var marker struct {
		RunID         string    `json:"runId"`
		RuntimeHostID string    `json:"runtimeHostId"`
		ManifestHash  string    `json:"manifestHash"`
		ExpiresAt     time.Time `json:"expiresAt"`
	}
	if json.Unmarshal(markerRaw, &marker) != nil || marker.RunID != manifest.RunID || marker.RuntimeHostID != manifest.RuntimeHostID ||
		marker.ManifestHash != manifest.ManifestHash || !marker.ExpiresAt.Equal(manifest.ExpiresAt) || !marker.ExpiresAt.After(now) {
		return MaterializedWorkspace{}, false
	}
	for _, dir := range []string{"input", "output", "staging"} {
		info, statErr := os.Lstat(filepath.Join(root, dir))
		if statErr != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return MaterializedWorkspace{}, false
		}
	}
	for _, entry := range manifest.Files {
		if !validCachedMaterializationFile(root, entry) {
			return MaterializedWorkspace{}, false
		}
	}
	return MaterializedWorkspace{
		RunID: manifest.RunID, Root: root, InputRoot: filepath.Join(root, "input"),
		OutputRoot: filepath.Join(root, "output"), StagingRoot: filepath.Join(root, "staging"),
		ManifestHash: manifest.ManifestHash, ExpiresAt: manifest.ExpiresAt,
	}, true
}

const runtimeMaterializationMarkerMaxBytes = 64 * 1024

// cachedMaterializationRegularFile walks each relative path component without
// following links. A final-file Lstat alone is insufficient because a replaced
// parent directory could redirect a cached immutable input outside the run
// root.
func cachedMaterializationRegularFile(root, logicalPath string) (string, os.FileInfo, bool) {
	normalized, err := normalizeRuntimeLogicalPath(logicalPath)
	if err != nil || normalized != logicalPath {
		return "", nil, false
	}
	target, err := safeMaterializationJoin(root, normalized)
	if err != nil {
		return "", nil, false
	}
	rootInfo, err := os.Lstat(root)
	if err != nil || rootInfo.Mode()&os.ModeSymlink != 0 || !rootInfo.IsDir() {
		return "", nil, false
	}
	current := root
	parts := strings.Split(normalized, "/")
	for index, part := range parts {
		current = filepath.Join(current, filepath.FromSlash(part))
		info, statErr := os.Lstat(current)
		if statErr != nil || info.Mode()&os.ModeSymlink != 0 {
			return "", nil, false
		}
		if index == len(parts)-1 {
			if !info.Mode().IsRegular() {
				return "", nil, false
			}
			return target, info, true
		}
		if !info.IsDir() {
			return "", nil, false
		}
	}
	return "", nil, false
}

func validCachedMaterializationFile(root string, entry RuntimeManifestEntry) bool {
	target, before, ok := cachedMaterializationRegularFile(root, entry.LogicalPath)
	if !ok || before.Size() != entry.SizeBytes {
		return false
	}
	file, err := os.Open(target)
	if err != nil {
		return false
	}
	opened, statErr := file.Stat()
	if statErr != nil || !opened.Mode().IsRegular() || opened.Size() != entry.SizeBytes || !os.SameFile(before, opened) {
		_ = file.Close()
		return false
	}
	hash := sha256.New()
	bytesRead, readErr := io.Copy(hash, io.LimitReader(file, entry.SizeBytes+1))
	closeErr := file.Close()
	if readErr != nil || closeErr != nil || bytesRead != entry.SizeBytes ||
		"sha256:"+hex.EncodeToString(hash.Sum(nil)) != normalizeSHA256(entry.SHA256) {
		return false
	}
	_, after, ok := cachedMaterializationRegularFile(root, entry.LogicalPath)
	return ok && after.Size() == entry.SizeBytes && os.SameFile(opened, after)
}

func readCachedMaterializationMarker(root string) ([]byte, bool) {
	target, before, ok := cachedMaterializationRegularFile(root, ".materialization.json")
	if !ok || before.Size() > runtimeMaterializationMarkerMaxBytes {
		return nil, false
	}
	file, err := os.Open(target)
	if err != nil {
		return nil, false
	}
	opened, statErr := file.Stat()
	if statErr != nil || !opened.Mode().IsRegular() || opened.Size() != before.Size() || !os.SameFile(before, opened) {
		_ = file.Close()
		return nil, false
	}
	raw, readErr := io.ReadAll(io.LimitReader(file, runtimeMaterializationMarkerMaxBytes+1))
	closeErr := file.Close()
	if readErr != nil || closeErr != nil || int64(len(raw)) != before.Size() || len(raw) > runtimeMaterializationMarkerMaxBytes {
		return nil, false
	}
	_, after, ok := cachedMaterializationRegularFile(root, ".materialization.json")
	if !ok || after.Size() != before.Size() || !os.SameFile(opened, after) {
		return nil, false
	}
	return raw, true
}
