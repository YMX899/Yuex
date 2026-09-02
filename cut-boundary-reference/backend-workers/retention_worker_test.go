package workers

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"huahuoai/backend/source/internal/domain"
	storageprovider "huahuoai/backend/source/internal/providers/storage"
	"huahuoai/backend/source/internal/queue"
	"huahuoai/backend/source/internal/services"
)

type retentionWorkerStore struct {
	recorded    domain.RetentionRunSummary
	recordErr   error
	recordCalls int
	draftCalls  int
}

type retentionWorkspaceEmbeddingStore struct {
	durable bool
	result  domain.WorkspaceEmbeddingRetentionResult
	err     error
	calls   int
}

func (s *retentionWorkspaceEmbeddingStore) HasDurableAdmission() bool {
	return s != nil && s.durable
}

func (s *retentionWorkspaceEmbeddingStore) CleanupWorkspaceEmbeddingRetention(context.Context, domain.WorkspaceEmbeddingRetentionPolicy) (domain.WorkspaceEmbeddingRetentionResult, error) {
	s.calls++
	return s.result, s.err
}

func (s *retentionWorkerStore) ListTerminalRuntimeRunIDs(context.Context, time.Time, int) ([]string, error) {
	return nil, nil
}
func (s *retentionWorkerStore) CompactTerminalAgentRunDraftEvents(context.Context, time.Time, int) (int, int, error) {
	s.draftCalls++
	return 0, 0, nil
}
func (s *retentionWorkerStore) ClaimExpiredProviderRawArtifacts(context.Context, time.Time, int, string) ([]domain.RetentionArtifactCandidate, error) {
	return nil, nil
}
func (s *retentionWorkerStore) CompleteProviderRawArtifact(context.Context, domain.RetentionArtifactCandidate, error) error {
	return nil
}
func (s *retentionWorkerStore) ArchiveTerminalQueueRecords(context.Context, string, time.Time, int) (int, error) {
	return 2, nil
}
func (s *retentionWorkerStore) ListExpiredResourceIDs(context.Context, time.Time, int) ([]string, error) {
	return nil, nil
}
func (s *retentionWorkerStore) ExpireResources(context.Context, []string) (int, error) { return 0, nil }
func (s *retentionWorkerStore) RecordRetentionRun(_ context.Context, summary domain.RetentionRunSummary) (string, error) {
	s.recorded = summary
	s.recordCalls++
	if s.recordErr != nil {
		return "", s.recordErr
	}
	return "retention_audit_worker", nil
}

func TestRetentionWorkerRunOnceUsesPerStageDistributedLeasesAndAudits(t *testing.T) {
	store := &retentionWorkerStore{}
	service := services.NewRetentionService(store, storageprovider.ConfigMissingStorage{}, t.TempDir())
	worker := NewRetentionWorker(service, queue.NewMemoryDistributedLockManager())
	worker.Now = func() time.Time { return time.Date(2026, 7, 15, 0, 0, 0, 0, time.UTC) }
	summary, err := worker.RunOnce(context.Background(), "worker-retention-1")
	if err != nil {
		t.Fatal(err)
	}
	if summary.Status != "succeeded" || len(summary.Results) != 5 || store.recorded.RunID != summary.RunID || store.draftCalls != 1 {
		t.Fatalf("summary=%#v recorded=%#v", summary, store.recorded)
	}
	for _, stage := range []string{"runtime_workspace", "agent_run_draft_events", "provider_raw", "queue_archive", "resource_expiry"} {
		lease, err := worker.Locks.Acquire(context.Background(), "system_job:retention:"+stage, "test-owner", "test-run", time.Second)
		if err != nil {
			t.Fatalf("lease %s must be released after stage: %v", stage, err)
		}
		if err := worker.Locks.Release(context.Background(), lease); err != nil {
			t.Fatalf("release verification lease %s: %v", stage, err)
		}
	}
}

func TestRetentionWorkerRejectsMissingDependencies(t *testing.T) {
	worker := NewRetentionWorker(nil, nil)
	if _, err := worker.RunOnce(context.Background(), "worker"); err == nil || err.Error() != "RETENTION_UNAVAILABLE" {
		t.Fatalf("missing dependency err=%v", err)
	}
}

func TestRetentionWorkerRunsDedicatedWorkspaceEmbeddingRetentionStageWhenEnabled(t *testing.T) {
	store := &retentionWorkerStore{}
	embedding := &retentionWorkspaceEmbeddingStore{
		durable: true,
		result:  domain.WorkspaceEmbeddingRetentionResult{ProviderRequestsDeleted: 1, AdmissionBucketsDeleted: 1, ActiveLeasesExpired: 1, TerminalLeasesDeleted: 1},
	}
	service := services.NewRetentionServiceWithWorkspaceEmbeddingRetention(
		store,
		storageprovider.ConfigMissingStorage{},
		t.TempDir(),
		embedding,
		true,
		domain.WorkspaceEmbeddingRetentionPolicy{RequestTTL: time.Hour, AdmissionBucketTTL: time.Hour, TerminalLeaseTTL: time.Hour, BatchSize: 10},
	)
	worker := NewRetentionWorker(service, queue.NewMemoryDistributedLockManager())
	summary, err := worker.RunOnce(context.Background(), "worker-retention-embedding")
	if err != nil || summary.Status != "succeeded" || len(summary.Results) != 6 || embedding.calls != 1 {
		t.Fatalf("embedding retention summary=%#v err=%v calls=%d", summary, err, embedding.calls)
	}
	var embeddingResult domain.RetentionCleanupResult
	for _, result := range summary.Results {
		if result.Stage == "workspace_embedding" {
			embeddingResult = result
		}
	}
	if embeddingResult.Deleted != 3 || embeddingResult.Expired != 1 || embeddingResult.Candidates != 4 {
		t.Fatalf("workspace embedding audit result=%#v", embeddingResult)
	}
	lease, err := worker.Locks.Acquire(context.Background(), "system_job:retention:workspace_embedding", "test-owner", "test-run", time.Second)
	if err != nil {
		t.Fatalf("workspace embedding stage lease was not released: %v", err)
	}
	if err := worker.Locks.Release(context.Background(), lease); err != nil {
		t.Fatalf("release workspace embedding verification lease: %v", err)
	}
}

func TestRetentionWorkerFailsClosedForEnabledWorkspaceEmbeddingWithoutDurableStore(t *testing.T) {
	store := &retentionWorkerStore{}
	service := services.NewRetentionServiceWithWorkspaceEmbeddingRetention(
		store,
		storageprovider.ConfigMissingStorage{},
		t.TempDir(),
		&retentionWorkspaceEmbeddingStore{durable: false},
		true,
		domain.WorkspaceEmbeddingRetentionPolicy{RequestTTL: time.Hour, AdmissionBucketTTL: time.Hour, TerminalLeaseTTL: time.Hour, BatchSize: 10},
	)
	worker := NewRetentionWorker(service, queue.NewMemoryDistributedLockManager())
	summary, err := worker.RunOnce(context.Background(), "worker-retention-embedding-unavailable")
	if err == nil || err.Error() != "RETENTION_RUN_PARTIAL_FAILURE" || summary.Status != "failed" || len(summary.Results) != 6 || store.recordCalls != 1 {
		t.Fatalf("missing durable embedding store summary=%#v err=%v auditCalls=%d", summary, err, store.recordCalls)
	}
}

func TestRetentionWorkerFailsClosedWhenDurableAuditCannotBeWritten(t *testing.T) {
	auditErr := errors.New("retention audit database unavailable")
	store := &retentionWorkerStore{recordErr: auditErr}
	worker := NewRetentionWorker(
		services.NewRetentionService(store, storageprovider.ConfigMissingStorage{}, t.TempDir()),
		queue.NewMemoryDistributedLockManager(),
	)

	summary, err := worker.RunOnce(context.Background(), "worker-retention-audit-failure")
	if !errors.Is(err, auditErr) {
		t.Fatalf("audit write failure err=%v, want %v", err, auditErr)
	}
	if summary.Status != "failed" || store.recordCalls != 1 {
		t.Fatalf("audit failure must not report successful retention run: summary=%#v calls=%d", summary, store.recordCalls)
	}
}

func TestRetentionWorkerRenewsSlowStageLeaseBeforeOriginalTTLExpires(t *testing.T) {
	worker := NewRetentionWorker(
		services.NewRetentionService(&retentionWorkerStore{}, storageprovider.ConfigMissingStorage{}, t.TempDir()),
		queue.NewMemoryDistributedLockManager(),
	)
	worker.LeaseTTL = 120 * time.Millisecond
	worker.LeaseRenewInterval = 30 * time.Millisecond

	started := make(chan struct{})
	releaseStage := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		_, err := worker.withLease(context.Background(), "slow_stage", "retention-worker", func(ctx context.Context) (domain.RetentionCleanupResult, error) {
			close(started)
			select {
			case <-releaseStage:
				return domain.RetentionCleanupResult{Stage: "slow_stage"}, nil
			case <-ctx.Done():
				return domain.RetentionCleanupResult{Stage: "slow_stage"}, ctx.Err()
			}
		})
		result <- err
	}()
	<-started

	// Wait beyond the original lease TTL. Without the renewal loop, this
	// competing owner would acquire the same stage key successfully.
	time.Sleep(220 * time.Millisecond)
	if lease, err := worker.Locks.Acquire(context.Background(), "system_job:retention:slow_stage", "competing-worker", "competing-run", time.Second); err == nil {
		_ = worker.Locks.Release(context.Background(), lease)
		t.Fatal("competing owner acquired a long-running retention stage after its original lease TTL")
	} else if err.Error() != "SERVICE_BUSY" {
		t.Fatalf("competing lease error=%v, want SERVICE_BUSY", err)
	}

	close(releaseStage)
	if err := <-result; err != nil {
		t.Fatalf("slow stage with successful renewal failed: %v", err)
	}
	lease, err := worker.Locks.Acquire(context.Background(), "system_job:retention:slow_stage", "next-worker", "next-run", time.Second)
	if err != nil {
		t.Fatalf("stage lease was not released after slow stage: %v", err)
	}
	if err := worker.Locks.Release(context.Background(), lease); err != nil {
		t.Fatalf("release post-stage lease: %v", err)
	}
}

func TestRetentionWorkerRejectsUnsafeLeaseRenewalTiming(t *testing.T) {
	worker := NewRetentionWorker(
		services.NewRetentionService(&retentionWorkerStore{}, storageprovider.ConfigMissingStorage{}, t.TempDir()),
		queue.NewMemoryDistributedLockManager(),
	)
	worker.LeaseTTL = 90 * time.Millisecond
	worker.LeaseRenewInterval = 31 * time.Millisecond
	if _, _, err := worker.leaseTiming(); err == nil || err.Error() != "RETENTION_LOCK_CONFIG_INVALID" {
		t.Fatalf("unsafe lease timing err=%v", err)
	}
	if _, err := worker.withLease(context.Background(), "invalid_timing", "worker", func(context.Context) (domain.RetentionCleanupResult, error) {
		return domain.RetentionCleanupResult{}, fmt.Errorf("must not run")
	}); err == nil || err.Error() != "RETENTION_LOCK_CONFIG_INVALID" {
		t.Fatalf("unsafe lease timing stage err=%v", err)
	}
}
