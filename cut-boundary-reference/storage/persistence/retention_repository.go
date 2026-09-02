package persistence

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"huahuoai/backend/source/internal/domain"
)

// RetentionRepository owns only operational-artifact lifecycle facts. It does
// not delete product facts, usage records, audits, or task lineage.
type RetentionRepository struct {
	db *Database
}

// terminalRuntimeRunStatuses is deliberately kept next to the retention
// query. Runtime workspace cleanup is allowed only after Product-facing Run
// state is terminal, including an orphaned Run after recovery has converged
// it. New nonterminal Runtime states must not become cleanup candidates by
// accident.
var terminalRuntimeRunStatuses = []string{
	"succeeded",
	"failed",
	"timeout",
	"cancelled",
	"orphaned",
}

func NewRetentionRepository(db *Database) *RetentionRepository {
	return &RetentionRepository{db: db}
}

func (r *RetentionRepository) ListTerminalRuntimeRunIDs(ctx context.Context, olderThan time.Time, limit int) ([]string, error) {
	if !r.ready() {
		return nil, fmt.Errorf("RETENTION_UNAVAILABLE")
	}
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := r.db.Pool.Query(ctx, `
select agent_run_id
from agent_runs
where status = any($1::text[])
  and updated_at <= $2
order by updated_at asc
limit $3`, terminalRuntimeRunStatuses, olderThan.UTC(), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids := make([]string, 0, limit)
	for rows.Next() {
		var runID string
		if err := rows.Scan(&runID); err != nil {
			return nil, err
		}
		ids = append(ids, runID)
	}
	return ids, rows.Err()
}

// CompactTerminalAgentRunDraftEvents removes only the public event prefix of
// an expired terminal Run. Keeping the terminal row makes the canonical result
// replayable, while moving the oldest public sequence forward lets the existing
// SSE gap contract recover stale cursors without creating internal holes.
func (r *RetentionRepository) CompactTerminalAgentRunDraftEvents(ctx context.Context, olderThan time.Time, limit int) (int, int, error) {
	if !r.ready() {
		return 0, 0, fmt.Errorf("RETENTION_UNAVAILABLE")
	}
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	var candidates, deleted int
	err := r.db.Pool.QueryRow(ctx, `
with event_bounds as (
  select events.run_id,
         min(events.sequence) filter (where events.event_type = 'draft_delta') first_draft_sequence,
         max(events.sequence) filter (
           where events.event_type in ('succeeded','failed','cancelled','timeout','aborted','orphaned')
              or coalesce(events.safe_payload->>'status','') in ('succeeded','failed','cancelled','timeout','aborted','orphaned')
         ) terminal_sequence,
         max(events.occurred_at) filter (
           where events.event_type in ('succeeded','failed','cancelled','timeout','aborted','orphaned')
              or coalesce(events.safe_payload->>'status','') in ('succeeded','failed','cancelled','timeout','aborted','orphaned')
         ) terminal_at
  from runtime_run_events events
  join agent_runs runs on runs.agent_run_id = events.run_id
  where runs.status = any($1::text[])
    and events.visibility = 'app_safe'
  group by events.run_id
), candidates as materialized (
  select run_id, terminal_sequence
  from event_bounds
  where first_draft_sequence < terminal_sequence
    and terminal_at <= $2
  order by terminal_at asc, run_id asc
  limit $3
), deleted as (
  delete from runtime_run_events events
  using candidates
  where events.run_id = candidates.run_id
    and events.visibility = 'app_safe'
    and events.sequence < candidates.terminal_sequence
  returning events.run_id
)
select (select count(*) from candidates), count(*) from deleted`, terminalRuntimeRunStatuses, olderThan.UTC(), limit).Scan(&candidates, &deleted)
	if err != nil {
		return 0, 0, err
	}
	return candidates, deleted, nil
}

// ClaimExpiredProviderRawArtifacts makes a cleanup attempt exclusive before
// the external storage deletion. A crashed worker leaves a visible deleting
// record for operational recovery rather than allowing duplicate success.
func (r *RetentionRepository) ClaimExpiredProviderRawArtifacts(ctx context.Context, olderThan time.Time, limit int, claimID string) ([]domain.RetentionArtifactCandidate, error) {
	if !r.ready() {
		return nil, fmt.Errorf("RETENTION_UNAVAILABLE")
	}
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	claimID = strings.TrimSpace(claimID)
	if claimID == "" {
		var err error
		claimID, err = newRetentionID("retention_claim")
		if err != nil {
			return nil, err
		}
	}
	tx, err := r.db.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rows, err := tx.Query(ctx, `
select artifact_id, artifact_class, object_bucket, object_key, expires_at
from retention_artifacts
where artifact_class = $1
  and status in ('active','failed','deleting')
  and expires_at <= $2
  and (
    status = 'active'
    or (status = 'failed' and last_attempt_at <= now() - interval '5 minutes')
    or (status = 'deleting' and claimed_at <= now() - interval '5 minutes')
  )
order by expires_at asc
for update skip locked
limit $3`, domain.RetentionArtifactProviderRaw, olderThan.UTC(), limit)
	if err != nil {
		return nil, err
	}
	candidates := []domain.RetentionArtifactCandidate{}
	for rows.Next() {
		var candidate domain.RetentionArtifactCandidate
		if err := rows.Scan(&candidate.ArtifactID, &candidate.Class, &candidate.Bucket, &candidate.ObjectKey, &candidate.ExpiresAt); err != nil {
			rows.Close()
			return nil, err
		}
		candidate.ClaimID = claimID
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	for _, candidate := range candidates {
		if _, err := tx.Exec(ctx, `
update retention_artifacts
set status = 'deleting', claim_id = $2, claimed_at = now(), last_attempt_at = now(), attempt_count = attempt_count + 1, updated_at = now()
where artifact_id = $1`, candidate.ArtifactID, claimID); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return candidates, nil
}

func (r *RetentionRepository) CompleteProviderRawArtifact(ctx context.Context, candidate domain.RetentionArtifactCandidate, cleanupErr error) error {
	if !r.ready() {
		return fmt.Errorf("RETENTION_UNAVAILABLE")
	}
	if strings.TrimSpace(candidate.ArtifactID) == "" || strings.TrimSpace(candidate.ClaimID) == "" {
		return fmt.Errorf("RETENTION_ARTIFACT_INVALID")
	}
	if cleanupErr == nil {
		result, err := r.db.Pool.Exec(ctx, `
update retention_artifacts
set status = 'deleted', deleted_at = now(), claim_id = null, claimed_at = null, last_error_code = null, updated_at = now()
where artifact_id = $1 and status = 'deleting' and claim_id = $2`, candidate.ArtifactID, candidate.ClaimID)
		if err != nil {
			return err
		}
		if result.RowsAffected() != 1 {
			return fmt.Errorf("RETENTION_ARTIFACT_STALE")
		}
		return nil
	}
	result, err := r.db.Pool.Exec(ctx, `
update retention_artifacts
set status = 'failed', claim_id = null, claimed_at = null, last_error_code = $3, updated_at = now()
where artifact_id = $1 and status = 'deleting' and claim_id = $2`, candidate.ArtifactID, candidate.ClaimID, retentionErrorCode(cleanupErr))
	if err != nil {
		return err
	}
	if result.RowsAffected() != 1 {
		return fmt.Errorf("RETENTION_ARTIFACT_STALE")
	}
	return nil
}

func (r *RetentionRepository) ArchiveTerminalQueueRecords(ctx context.Context, queueName string, olderThan time.Time, limit int) (int, error) {
	if !r.ready() {
		return 0, fmt.Errorf("RETENTION_UNAVAILABLE")
	}
	if limit <= 0 || limit > 1000 {
		limit = 250
	}
	result, err := r.db.Pool.Exec(ctx, `
with candidates as (
  select queue_id
  from task_queue_records
  where archived_at is null
    and status in ('succeeded','failed','timeout','dead_letter','ignored','cancelled')
    and updated_at <= $1
    and ($2 = '' or queue_name = $2)
  order by updated_at asc
  limit $3
  for update skip locked
)
update task_queue_records q
set archived_at = now(), updated_at = now()
from candidates c
where q.queue_id = c.queue_id`, olderThan.UTC(), strings.TrimSpace(queueName), limit)
	if err != nil {
		return 0, err
	}
	return int(result.RowsAffected()), nil
}

func (r *RetentionRepository) ListExpiredResourceIDs(ctx context.Context, olderThan time.Time, limit int) ([]string, error) {
	if !r.ready() {
		return nil, fmt.Errorf("RETENTION_UNAVAILABLE")
	}
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := r.db.Pool.Query(ctx, `
select resource_id
from resource_indexes
where status = 'available'
  and retention_expires_at is not null
  and retention_expires_at <= $1
order by retention_expires_at asc
limit $2`, olderThan.UTC(), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids := []string{}
	for rows.Next() {
		var resourceID string
		if err := rows.Scan(&resourceID); err != nil {
			return nil, err
		}
		ids = append(ids, resourceID)
	}
	return ids, rows.Err()
}

func (r *RetentionRepository) ExpireResources(ctx context.Context, resourceIDs []string) (int, error) {
	if !r.ready() {
		return 0, fmt.Errorf("RETENTION_UNAVAILABLE")
	}
	ids := compactRetentionIDs(resourceIDs)
	if len(ids) == 0 {
		return 0, nil
	}
	result, err := r.db.Pool.Exec(ctx, `
update resource_indexes
set status = 'expired', updated_at = now()
where resource_id = any($1::text[])
  and status = 'available'`, ids)
	if err != nil {
		return 0, err
	}
	return int(result.RowsAffected()), nil
}

func (r *RetentionRepository) RecordRetentionRun(ctx context.Context, summary domain.RetentionRunSummary) (string, error) {
	if !r.ready() {
		return "", fmt.Errorf("RETENTION_UNAVAILABLE")
	}
	runID := strings.TrimSpace(summary.RunID)
	if runID == "" {
		var err error
		runID, err = newRetentionID("retention")
		if err != nil {
			return "", err
		}
	}
	payloadRaw, err := json.Marshal(map[string]any{
		"startedAt": summary.StartedAt.UTC().Format(time.RFC3339Nano),
		"endedAt":   summary.EndedAt.UTC().Format(time.RFC3339Nano),
		"failed":    summary.Failed,
	})
	if err != nil {
		return "", err
	}
	stages := make([]map[string]any, 0, len(summary.Results))
	for _, stage := range summary.Results {
		stages = append(stages, map[string]any{
			"stage": stage.Stage, "candidates": stage.Candidates, "deleted": stage.Deleted,
			"archived": stage.Archived, "expired": stage.Expired, "failed": stage.Failed,
			"errors": safeRetentionErrorCodes(stage.Errors),
		})
	}
	resultRaw, err := json.Marshal(map[string]any{"stages": stages, "failed": summary.Failed})
	if err != nil {
		return "", err
	}
	_, err = r.db.Pool.Exec(ctx, `
insert into system_job_runs(run_id, job_type, status, payload, result)
values ($1, 'retention', $2, $3::jsonb, $4::jsonb)
on conflict (run_id) do update
set status = excluded.status, payload = excluded.payload, result = excluded.result, updated_at = now()`, runID, retentionRunStatus(summary), string(payloadRaw), string(resultRaw))
	if err != nil {
		return "", err
	}
	return runID, nil
}

func (r *RetentionRepository) RegisterProviderRawArtifact(ctx context.Context, artifactID, bucket, objectKey string, expiresAt time.Time) error {
	if !r.ready() {
		return fmt.Errorf("RETENTION_UNAVAILABLE")
	}
	if strings.TrimSpace(artifactID) == "" || strings.TrimSpace(objectKey) == "" || expiresAt.IsZero() {
		return fmt.Errorf("RETENTION_ARTIFACT_INVALID")
	}
	_, err := r.db.Pool.Exec(ctx, `
insert into retention_artifacts(artifact_id, artifact_class, object_bucket, object_key, expires_at, status)
values ($1, $2, $3, $4, $5, 'active')
on conflict (artifact_id) do update
set object_bucket = excluded.object_bucket, object_key = excluded.object_key, expires_at = excluded.expires_at,
    status = case when retention_artifacts.status = 'deleted' then 'active' else retention_artifacts.status end,
    updated_at = now()`, strings.TrimSpace(artifactID), domain.RetentionArtifactProviderRaw, strings.TrimSpace(bucket), strings.TrimSpace(objectKey), expiresAt.UTC())
	return err
}

func (r *RetentionRepository) ready() bool {
	return r != nil && r.db != nil && !r.db.Disabled && r.db.Pool != nil
}

func compactRetentionIDs(values []string) []string {
	seen := map[string]bool{}
	out := []string{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
	}
	return out
}

func retentionRunStatus(summary domain.RetentionRunSummary) string {
	if summary.Failed > 0 || strings.EqualFold(summary.Status, "failed") {
		return "failed"
	}
	return "succeeded"
}

func retentionErrorCode(err error) string {
	return retentionSafeCode(err)
}

func safeRetentionErrorCodes(values []string) []string {
	seen := map[string]bool{}
	out := []string{}
	for _, value := range values {
		code := retentionSafeCode(fmt.Errorf("%s", value))
		if code == "" || seen[code] {
			continue
		}
		seen[code] = true
		out = append(out, code)
	}
	return out
}

func retentionSafeCode(err error) string {
	if err == nil {
		return ""
	}
	value := strings.ToUpper(strings.TrimSpace(err.Error()))
	switch {
	case strings.Contains(value, "RETENTION_ARTIFACT_STALE"):
		return "RETENTION_ARTIFACT_STALE"
	case strings.Contains(value, "RETENTION_ARTIFACT_INVALID"):
		return "RETENTION_ARTIFACT_INVALID"
	case strings.Contains(value, "RETENTION_UNAVAILABLE"):
		return "RETENTION_UNAVAILABLE"
	case strings.Contains(value, "PROVIDER_CONFIG_MISSING"):
		return "RETENTION_STORAGE_UNAVAILABLE"
	case strings.Contains(value, "RETENTION_"):
		return "RETENTION_CLEANUP_FAILED"
	default:
		return "RETENTION_CLEANUP_FAILED"
	}
}

func newRetentionID(prefix string) (string, error) {
	bytes := make([]byte, 10)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return prefix + "_" + hex.EncodeToString(bytes) + "_" + fmt.Sprint(time.Now().UTC().UnixNano()), nil
}
