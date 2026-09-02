package workers

import (
	"context"
	"fmt"
	"strings"
	"time"

	"huahuoai/backend/source/internal/domain"
	"huahuoai/backend/source/internal/queue"
	"huahuoai/backend/source/internal/services"
)

const (
	retentionLeaseTTL           = 2 * time.Minute
	retentionLeaseRenewInterval = retentionLeaseTTL / 3
	retentionDefaultPeriod      = 15 * time.Minute
)

type RetentionWorker struct {
	Service            *services.RetentionService
	Locks              *queue.DistributedLockManager
	Interval           time.Duration
	LeaseTTL           time.Duration
	LeaseRenewInterval time.Duration
	Now                func() time.Time
}

func NewRetentionWorker(service *services.RetentionService, locks *queue.DistributedLockManager) *RetentionWorker {
	return &RetentionWorker{
		Service: service, Locks: locks, Interval: retentionDefaultPeriod,
		LeaseTTL: retentionLeaseTTL, LeaseRenewInterval: retentionLeaseRenewInterval,
		Now: func() time.Time { return time.Now().UTC() },
	}
}

// Start is a long-lived scheduler consumer. Each pass holds an independent
// distributed lease per artifact class, so worker replicas cannot clean the
// same class concurrently.
func (w *RetentionWorker) Start(ctx context.Context, workerID string) error {
	if err := w.validate(); err != nil {
		return err
	}
	if _, err := w.RunOnce(ctx, workerID); err != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	interval := w.Interval
	if interval <= 0 {
		interval = retentionDefaultPeriod
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			// Each failed pass has a durable run audit and candidate retry state.
			// Keep the scheduler alive for the next bounded recovery attempt.
			if _, err := w.RunOnce(ctx, workerID); err != nil && ctx.Err() != nil {
				return ctx.Err()
			}
		}
	}
}

func (w *RetentionWorker) RunOnce(ctx context.Context, workerID string) (domain.RetentionRunSummary, error) {
	if err := w.validate(); err != nil {
		return domain.RetentionRunSummary{}, err
	}
	workerID = strings.TrimSpace(workerID)
	if workerID == "" {
		return domain.RetentionRunSummary{}, fmt.Errorf("RETENTION_WORKER_ID_REQUIRED")
	}
	now := w.now()
	summary := domain.RetentionRunSummary{RunID: fmt.Sprintf("retention_%d", now.UnixNano()), StartedAt: now, Status: "succeeded"}
	stages := []func(context.Context, string) (domain.RetentionCleanupResult, error){
		w.RunRuntimeWorkspaceCleanup,
		w.RunAgentRunDraftEventCleanup,
		w.RunProviderArtifactCleanup,
		w.RunQueueArchive,
		w.RunResourceExpiry,
	}
	if w.Service.WorkspaceEmbeddingRetentionEnabled() {
		stages = append(stages, w.RunWorkspaceEmbeddingRetention)
	}
	for _, stage := range stages {
		result, err := stage(ctx, workerID+":"+summary.RunID)
		summary.Results = append(summary.Results, result)
		if err != nil {
			summary.Failed++
			summary.Status = "failed"
		}
	}
	summary.EndedAt = w.now()
	if _, err := w.Service.CreateRetentionAudit(ctx, summary); err != nil {
		// A run without its required durable audit must never be surfaced as a
		// successful cleanup pass, even when its four cleanup stages succeeded.
		summary.Status = "failed"
		return summary, err
	}
	if summary.Failed > 0 {
		return summary, fmt.Errorf("RETENTION_RUN_PARTIAL_FAILURE")
	}
	return summary, nil
}

func (w *RetentionWorker) RunRuntimeWorkspaceCleanup(ctx context.Context, owner string) (domain.RetentionCleanupResult, error) {
	return w.withLease(ctx, "runtime_workspace", owner, func(stageCtx context.Context) (domain.RetentionCleanupResult, error) {
		return w.Service.CleanupRuntimeWorkspaces(stageCtx, w.now().Add(-services.RetentionRuntimeWorkspaceTTL), services.RetentionBatchSize)
	})
}

func (w *RetentionWorker) RunAgentRunDraftEventCleanup(ctx context.Context, owner string) (domain.RetentionCleanupResult, error) {
	return w.withLease(ctx, "agent_run_draft_events", owner, func(stageCtx context.Context) (domain.RetentionCleanupResult, error) {
		return w.Service.CleanupAgentRunDraftEvents(stageCtx, w.now().Add(-services.RetentionAgentRunDraftTTL), services.RetentionBatchSize)
	})
}

func (w *RetentionWorker) RunProviderArtifactCleanup(ctx context.Context, owner string) (domain.RetentionCleanupResult, error) {
	return w.withLease(ctx, "provider_raw", owner, func(stageCtx context.Context) (domain.RetentionCleanupResult, error) {
		candidates, err := w.Service.FindExpiredArtifacts(stageCtx, w.now().Add(-services.RetentionProviderRawTTL), services.RetentionBatchSize, owner)
		if err != nil {
			return domain.RetentionCleanupResult{Stage: "provider_raw"}, err
		}
		return w.Service.CleanupProviderRawArtifacts(stageCtx, candidates)
	})
}

func (w *RetentionWorker) RunQueueArchive(ctx context.Context, owner string) (domain.RetentionCleanupResult, error) {
	return w.withLease(ctx, "queue_archive", owner, func(stageCtx context.Context) (domain.RetentionCleanupResult, error) {
		return w.Service.ArchiveQueueRecords(stageCtx, "", w.now().Add(-services.RetentionQueueArchiveTTL), services.RetentionBatchSize)
	})
}

func (w *RetentionWorker) RunResourceExpiry(ctx context.Context, owner string) (domain.RetentionCleanupResult, error) {
	return w.withLease(ctx, "resource_expiry", owner, func(stageCtx context.Context) (domain.RetentionCleanupResult, error) {
		return w.Service.ExpireResources(stageCtx, w.now(), services.RetentionBatchSize)
	})
}

// RunWorkspaceEmbeddingRetention owns the content-free embedding ledger and
// admission lifecycle. The service rejects an enabled embedding configuration
// that lacks its dedicated durable store, so this stage cannot silently become
// a successful no-op after Provider accounting has been enabled.
func (w *RetentionWorker) RunWorkspaceEmbeddingRetention(ctx context.Context, owner string) (domain.RetentionCleanupResult, error) {
	return w.withLease(ctx, "workspace_embedding", owner, func(stageCtx context.Context) (domain.RetentionCleanupResult, error) {
		return w.Service.CleanupWorkspaceEmbedding(stageCtx)
	})
}

func (w *RetentionWorker) withLease(ctx context.Context, class, owner string, run func(context.Context) (domain.RetentionCleanupResult, error)) (domain.RetentionCleanupResult, error) {
	if w == nil || w.Locks == nil || run == nil {
		return domain.RetentionCleanupResult{Stage: class}, fmt.Errorf("RETENTION_LOCK_UNAVAILABLE")
	}
	leaseTTL, renewInterval, err := w.leaseTiming()
	if err != nil {
		return domain.RetentionCleanupResult{Stage: class}, err
	}
	lease, err := w.Locks.Acquire(ctx, "system_job:retention:"+class, owner, class, leaseTTL)
	if err != nil {
		return domain.RetentionCleanupResult{Stage: class}, err
	}
	defer func() { _ = w.Locks.Release(context.Background(), lease) }()

	stageCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stopRenewal := make(chan struct{})
	renewalDone := make(chan struct{})
	renewalErr := make(chan error, 1)
	go func() {
		defer close(renewalDone)
		ticker := time.NewTicker(renewInterval)
		defer ticker.Stop()
		current := lease
		for {
			select {
			case <-stopRenewal:
				return
			case <-stageCtx.Done():
				return
			case <-ticker.C:
				renewed, renewErr := w.Locks.Renew(stageCtx, current, leaseTTL)
				if renewErr != nil {
					select {
					case renewalErr <- fmt.Errorf("RETENTION_LOCK_RENEW_FAILED"):
					default:
					}
					cancel()
					return
				}
				current = renewed
			}
		}
	}()

	result, runErr := run(stageCtx)
	close(stopRenewal)
	cancel()
	<-renewalDone
	select {
	case err := <-renewalErr:
		return result, err
	default:
		return result, runErr
	}
}

func (w *RetentionWorker) leaseTiming() (time.Duration, time.Duration, error) {
	leaseTTL := retentionLeaseTTL
	renewInterval := retentionLeaseRenewInterval
	if w != nil && w.LeaseTTL > 0 {
		leaseTTL = w.LeaseTTL
	}
	if w != nil && w.LeaseRenewInterval > 0 {
		renewInterval = w.LeaseRenewInterval
	}
	if leaseTTL <= 0 || renewInterval <= 0 || renewInterval > leaseTTL/3 {
		return 0, 0, fmt.Errorf("RETENTION_LOCK_CONFIG_INVALID")
	}
	return leaseTTL, renewInterval, nil
}

func (w *RetentionWorker) validate() error {
	if w == nil || w.Service == nil || w.Locks == nil {
		return fmt.Errorf("RETENTION_UNAVAILABLE")
	}
	return nil
}

func (w *RetentionWorker) now() time.Time {
	if w != nil && w.Now != nil {
		return w.Now().UTC()
	}
	return time.Now().UTC()
}
