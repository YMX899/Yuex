package services

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"time"

	"huahuoai/backend/source/internal/config"
	"huahuoai/backend/source/internal/domain"
	storageprovider "huahuoai/backend/source/internal/providers/storage"
)

const (
	RetentionRuntimeWorkspaceTTL = 72 * time.Hour
	RetentionAgentRunDraftTTL    = 24 * time.Hour
	RetentionProviderRawTTL      = 30 * 24 * time.Hour
	RetentionQueueArchiveTTL     = 30 * 24 * time.Hour
	RetentionBatchSize           = 100
)

var retentionRunID = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

// RetentionStore is intentionally narrow: all durable state transitions stay
// in persistence, while filesystem/object-store effects remain in this
// service. A nil store is unavailable, never an in-memory success fallback.
type RetentionStore interface {
	ListTerminalRuntimeRunIDs(ctx context.Context, olderThan time.Time, limit int) ([]string, error)
	CompactTerminalAgentRunDraftEvents(ctx context.Context, olderThan time.Time, limit int) (int, int, error)
	ClaimExpiredProviderRawArtifacts(ctx context.Context, olderThan time.Time, limit int, claimID string) ([]domain.RetentionArtifactCandidate, error)
	CompleteProviderRawArtifact(ctx context.Context, candidate domain.RetentionArtifactCandidate, cleanupErr error) error
	ArchiveTerminalQueueRecords(ctx context.Context, queueName string, olderThan time.Time, limit int) (int, error)
	ListExpiredResourceIDs(ctx context.Context, olderThan time.Time, limit int) ([]string, error)
	ExpireResources(ctx context.Context, resourceIDs []string) (int, error)
	RecordRetentionRun(ctx context.Context, summary domain.RetentionRunSummary) (string, error)
}

// WorkspaceEmbeddingRetentionStore is deliberately narrower than the
// provider ledger interface. The retention worker can request a bounded,
// aggregate cleanup result, but cannot read provider input, vectors, keys,
// endpoint configuration, or raw provider responses.
type WorkspaceEmbeddingRetentionStore interface {
	HasDurableAdmission() bool
	CleanupWorkspaceEmbeddingRetention(ctx context.Context, policy domain.WorkspaceEmbeddingRetentionPolicy) (domain.WorkspaceEmbeddingRetentionResult, error)
}

type RetentionService struct {
	Store                     RetentionStore
	Storage                   storageprovider.ObjectStorage
	RuntimeRoot               string
	WorkspaceEmbeddingStore   WorkspaceEmbeddingRetentionStore
	WorkspaceEmbeddingEnabled bool
	WorkspaceEmbeddingPolicy  domain.WorkspaceEmbeddingRetentionPolicy
	Now                       func() time.Time
}

func NewRetentionService(store RetentionStore, objectStorage storageprovider.ObjectStorage, runtimeRoot string) *RetentionService {
	return &RetentionService{
		Store:       store,
		Storage:     objectStorage,
		RuntimeRoot: strings.TrimSpace(runtimeRoot),
		Now:         func() time.Time { return time.Now().UTC() },
	}
}

// NewRetentionServiceWithWorkspaceEmbeddingRetention preserves the original
// constructor for existing retention callers while making embedding lifecycle
// ownership explicit. The dedicated embedding store is not interchangeable
// with the product RetentionStore.
func NewRetentionServiceWithWorkspaceEmbeddingRetention(store RetentionStore, objectStorage storageprovider.ObjectStorage, runtimeRoot string, embeddingStore WorkspaceEmbeddingRetentionStore, embeddingEnabled bool, policy domain.WorkspaceEmbeddingRetentionPolicy) *RetentionService {
	service := NewRetentionService(store, objectStorage, runtimeRoot)
	service.WorkspaceEmbeddingStore = embeddingStore
	service.WorkspaceEmbeddingEnabled = embeddingEnabled
	service.WorkspaceEmbeddingPolicy = policy
	return service
}

func NewRetentionServiceForContainer(container *Container) *RetentionService {
	if container == nil || container.Repos == nil {
		return NewRetentionService(nil, nil, "")
	}
	settings := config.NormalizeRetentionSettings(container.Settings.Retention)
	return NewRetentionServiceWithWorkspaceEmbeddingRetention(
		container.Repos.Retention,
		container.ObjectStorage,
		container.Settings.RuntimeRoot,
		container.Repos.WorkspaceEmbeddingProvider,
		container.Settings.Embedding.Enabled,
		domain.WorkspaceEmbeddingRetentionPolicy{
			RequestTTL:         time.Duration(settings.EmbeddingRequestTTLSeconds) * time.Second,
			AdmissionBucketTTL: time.Duration(settings.EmbeddingAdmissionBucketTTLSeconds) * time.Second,
			TerminalLeaseTTL:   time.Duration(settings.EmbeddingTerminalLeaseTTLSeconds) * time.Second,
			BatchSize:          settings.EmbeddingBatchSize,
		},
	)
}

func (s *RetentionService) ClassifyArtifact(artifact domain.RetentionArtifactCandidate) map[string]any {
	if artifact.Class == domain.RetentionArtifactProviderRaw {
		return map[string]any{"retentionClass": "operational", "ttlHours": int(RetentionProviderRawTTL.Hours()), "deletable": true}
	}
	return map[string]any{"retentionClass": "persistent", "ttlHours": 0, "deletable": false}
}

// FindExpiredArtifacts claims provider raw candidates. Claiming is deliberate:
// callers must either complete each cleanup attempt or leave a durable failed
// state for a later retry.
func (s *RetentionService) FindExpiredArtifacts(ctx context.Context, olderThan time.Time, limit int, claimID string) ([]domain.RetentionArtifactCandidate, error) {
	if err := s.requireStore(); err != nil {
		return nil, err
	}
	if olderThan.IsZero() {
		olderThan = s.now().Add(-RetentionProviderRawTTL)
	}
	return s.Store.ClaimExpiredProviderRawArtifacts(ctx, olderThan, normalizedRetentionLimit(limit), claimID)
}

func (s *RetentionService) CleanupRuntimeWorkspaces(ctx context.Context, olderThan time.Time, limit int) (domain.RetentionCleanupResult, error) {
	result := domain.RetentionCleanupResult{Stage: "runtime_workspace"}
	if err := s.requireStore(); err != nil {
		return result, err
	}
	if olderThan.IsZero() {
		olderThan = s.now().Add(-RetentionRuntimeWorkspaceTTL)
	}
	runIDs, err := s.Store.ListTerminalRuntimeRunIDs(ctx, olderThan, normalizedRetentionLimit(limit))
	if err != nil {
		return result, err
	}
	result.Candidates = len(runIDs)
	for _, runID := range runIDs {
		if err := s.removeRuntimeWorkspace(runID); err != nil {
			result.Failed++
			result.Errors = append(result.Errors, retentionSafeError(err))
			continue
		}
		result.Deleted++
	}
	if result.Failed > 0 {
		return result, fmt.Errorf("RETENTION_RUNTIME_WORKSPACE_CLEANUP_FAILED")
	}
	return result, nil
}

func (s *RetentionService) CleanupAgentRunDraftEvents(ctx context.Context, olderThan time.Time, limit int) (domain.RetentionCleanupResult, error) {
	result := domain.RetentionCleanupResult{Stage: "agent_run_draft_events"}
	if err := s.requireStore(); err != nil {
		return result, err
	}
	if olderThan.IsZero() {
		olderThan = s.now().Add(-RetentionAgentRunDraftTTL)
	}
	candidates, deleted, err := s.Store.CompactTerminalAgentRunDraftEvents(ctx, olderThan, normalizedRetentionLimit(limit))
	if err != nil {
		return result, err
	}
	result.Candidates = candidates
	result.Deleted = deleted
	return result, nil
}

func (s *RetentionService) CleanupProviderRawArtifacts(ctx context.Context, candidates []domain.RetentionArtifactCandidate) (domain.RetentionCleanupResult, error) {
	result := domain.RetentionCleanupResult{Stage: "provider_raw", Candidates: len(candidates)}
	if err := s.requireStore(); err != nil {
		return result, err
	}
	if s.Storage == nil {
		return result, fmt.Errorf("RETENTION_STORAGE_UNAVAILABLE")
	}
	for _, candidate := range candidates {
		if candidate.Class != domain.RetentionArtifactProviderRaw || strings.TrimSpace(candidate.ArtifactID) == "" || strings.TrimSpace(candidate.ObjectKey) == "" || strings.TrimSpace(candidate.ClaimID) == "" {
			result.Failed++
			result.Errors = append(result.Errors, "RETENTION_ARTIFACT_INVALID")
			continue
		}
		deleteErr := s.Storage.DeleteTransient(storageprovider.StorageRef{Bucket: candidate.Bucket, Key: candidate.ObjectKey})
		if completeErr := s.Store.CompleteProviderRawArtifact(ctx, candidate, deleteErr); completeErr != nil {
			result.Failed++
			result.Errors = append(result.Errors, retentionSafeError(completeErr))
			continue
		}
		if deleteErr != nil {
			result.Failed++
			result.Errors = append(result.Errors, retentionSafeError(deleteErr))
			continue
		}
		result.Deleted++
	}
	if result.Failed > 0 {
		return result, fmt.Errorf("RETENTION_PROVIDER_RAW_CLEANUP_FAILED")
	}
	return result, nil
}

func (s *RetentionService) ArchiveQueueRecords(ctx context.Context, queueName string, olderThan time.Time, limit int) (domain.RetentionCleanupResult, error) {
	result := domain.RetentionCleanupResult{Stage: "queue_archive"}
	if err := s.requireStore(); err != nil {
		return result, err
	}
	if olderThan.IsZero() {
		olderThan = s.now().Add(-RetentionQueueArchiveTTL)
	}
	count, err := s.Store.ArchiveTerminalQueueRecords(ctx, queueName, olderThan, normalizedRetentionLimit(limit))
	if err != nil {
		return result, err
	}
	result.Archived = count
	return result, nil
}

func (s *RetentionService) ExpireResources(ctx context.Context, olderThan time.Time, limit int) (domain.RetentionCleanupResult, error) {
	result := domain.RetentionCleanupResult{Stage: "resource_expiry"}
	if err := s.requireStore(); err != nil {
		return result, err
	}
	if olderThan.IsZero() {
		olderThan = s.now()
	}
	resourceIDs, err := s.Store.ListExpiredResourceIDs(ctx, olderThan, normalizedRetentionLimit(limit))
	if err != nil {
		return result, err
	}
	result.Candidates = len(resourceIDs)
	count, err := s.Store.ExpireResources(ctx, resourceIDs)
	if err != nil {
		return result, err
	}
	result.Expired = count
	return result, nil
}

func (s *RetentionService) CreateRetentionAudit(ctx context.Context, summary domain.RetentionRunSummary) (string, error) {
	if err := s.requireStore(); err != nil {
		return "", err
	}
	return s.Store.RecordRetentionRun(ctx, summary)
}

// WorkspaceEmbeddingRetentionEnabled determines whether the scheduler owns
// this stage. An intentionally disabled embedding provider has no ledger or
// admission lifecycle to scan; an enabled provider without a durable store is
// handled as a failed stage instead of a skip.
func (s *RetentionService) WorkspaceEmbeddingRetentionEnabled() bool {
	return s != nil && s.WorkspaceEmbeddingEnabled
}

func (s *RetentionService) CleanupWorkspaceEmbedding(ctx context.Context) (domain.RetentionCleanupResult, error) {
	result := domain.RetentionCleanupResult{Stage: "workspace_embedding"}
	if !s.WorkspaceEmbeddingRetentionEnabled() {
		return result, nil
	}
	if s.WorkspaceEmbeddingStore == nil || !s.WorkspaceEmbeddingStore.HasDurableAdmission() {
		return result, fmt.Errorf("RETENTION_EMBEDDING_UNAVAILABLE")
	}
	policy := s.WorkspaceEmbeddingPolicy
	cleanup, err := s.WorkspaceEmbeddingStore.CleanupWorkspaceEmbeddingRetention(ctx, policy)
	if err != nil {
		return result, fmt.Errorf("RETENTION_EMBEDDING_UNAVAILABLE")
	}
	result.Candidates = cleanup.Candidates()
	result.Deleted = cleanup.Deleted()
	result.Expired = cleanup.ActiveLeasesExpired
	return result, nil
}

func (s *RetentionService) removeRuntimeWorkspace(runID string) error {
	if !retentionRunID.MatchString(strings.TrimSpace(runID)) {
		return fmt.Errorf("RETENTION_PATH_INVALID")
	}
	runtimeRoot := strings.TrimSpace(s.RuntimeRoot)
	if runtimeRoot == "" {
		return fmt.Errorf("RETENTION_RUNTIME_ROOT_UNAVAILABLE")
	}
	root := filepath.Clean(filepath.Join(runtimeRoot, "tmp", "runtime-workspaces"))
	rootInfo, err := os.Lstat(root)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil || rootInfo.Mode()&os.ModeSymlink != 0 || !rootInfo.IsDir() {
		return fmt.Errorf("RETENTION_ROOT_INVALID")
	}
	resolvedRoot, err := filepath.EvalSymlinks(root)
	if err == nil {
		if !retentionPathsEqual(resolvedRoot, root) {
			return fmt.Errorf("RETENTION_ROOT_INVALID")
		}
	} else if runtime.GOOS != "windows" || !os.IsPermission(err) {
		return fmt.Errorf("RETENTION_ROOT_INVALID")
	}
	target := filepath.Clean(filepath.Join(root, runID))
	relative, err := filepath.Rel(root, target)
	if err != nil || relative == "." || strings.Contains(filepath.ToSlash(relative), "/") || strings.HasPrefix(relative, "..") {
		return fmt.Errorf("RETENTION_PATH_INVALID")
	}
	info, err := os.Lstat(target)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("RETENTION_PATH_INVALID")
	}
	return os.RemoveAll(target)
}

func (s *RetentionService) requireStore() error {
	if s == nil || s.Store == nil {
		return fmt.Errorf("RETENTION_UNAVAILABLE")
	}
	return nil
}

func (s *RetentionService) now() time.Time {
	if s != nil && s.Now != nil {
		return s.Now().UTC()
	}
	return time.Now().UTC()
}

func normalizedRetentionLimit(limit int) int {
	if limit <= 0 || limit > RetentionBatchSize {
		return RetentionBatchSize
	}
	return limit
}

func retentionPathsEqual(left, right string) bool {
	left = filepath.Clean(left)
	right = filepath.Clean(right)
	if runtime.GOOS == "windows" {
		return strings.EqualFold(left, right)
	}
	return left == right
}

func retentionSafeError(err error) string {
	if err == nil {
		return ""
	}
	value := strings.ToUpper(strings.TrimSpace(err.Error()))
	switch {
	case strings.Contains(value, "RETENTION_ARTIFACT_INVALID"):
		return "RETENTION_ARTIFACT_INVALID"
	case strings.Contains(value, "RETENTION_ARTIFACT_STALE"):
		return "RETENTION_ARTIFACT_STALE"
	case strings.Contains(value, "RETENTION_UNAVAILABLE"):
		return "RETENTION_UNAVAILABLE"
	case strings.Contains(value, "RETENTION_EMBEDDING_UNAVAILABLE"):
		return "RETENTION_EMBEDDING_UNAVAILABLE"
	case strings.Contains(value, "RETENTION_"):
		return "RETENTION_CLEANUP_FAILED"
	default:
		return "RETENTION_CLEANUP_FAILED"
	}
}
