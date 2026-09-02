package persistence

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	queuecontract "huahuoai/backend/source/internal/queue"

	"github.com/jackc/pgx/v5"
)

type QueueRecord = map[string]any

type QueueLeaseProof = queuecontract.QueueLeaseProof

type AdminReplayCommand struct {
	QueueID        string
	OperatorID     string
	Reason         string
	IdempotencyKey string
}

var ErrStaleQueueLease = queuecontract.ErrStaleQueueLease
var ErrNoQueueWork = queuecontract.ErrNoQueueWork
var ErrQueueReplayInvalid = errors.New("QUEUE_REPLAY_INVALID")
var ErrQueueReplayNotFound = errors.New("QUEUE_REPLAY_NOT_FOUND")
var ErrQueueReplayConflict = errors.New("QUEUE_REPLAY_CONFLICT")
var ErrQueueReplayForbidden = errors.New("QUEUE_REPLAY_FORBIDDEN")
var ErrRuntimeTerminalConvergenceNotFound = errors.New("RUNTIME_TERMINAL_CONVERGENCE_NOT_FOUND")
var ErrRuntimeTerminalRecoveryQueueInvalid = errors.New("RUNTIME_TERMINAL_RECOVERY_QUEUE_INVALID")

const RuntimeTerminalLegacyContractBlocked = "RUNTIME_TERMINAL_LEGACY_CONTRACT_BLOCKED"

// ErrMaterialQueueProjectionOwned means a caller attempted to use the generic
// queue state machine for a MaterialRepository-owned queue projection. Material
// jobs lease and transition through material_processing_jobs instead.
var ErrMaterialQueueProjectionOwned = errors.New("MATERIAL_QUEUE_PROJECTION_OWNED")

type QueueRepository struct {
	db                  *Database
	mu                  sync.Mutex
	records             map[string]map[string]any
	recoveryRuns        map[string]map[string]any
	terminalCompletions map[string]string
}

// RuntimeTerminalRecoveryQueueCommand names the immutable terminal
// convergence that a fresh runtime_events record must resume. The command is
// deliberately scoped to a single dispatch/run pair; a recovery queue cannot
// be used as a generic replay primitive for another Runtime event.
type RuntimeTerminalRecoveryQueueCommand struct {
	ConvergenceID string
	DispatchID    string
	RunID         string
	RuntimeHostID string
}

// RuntimeTerminalRecoveryQueueResult identifies the durable, consumable
// recovery queue. Skipped is true only when the convergence was completed by a
// concurrent worker after it had been listed for recovery.
type RuntimeTerminalRecoveryQueueResult struct {
	Record      QueueRecord
	QueueID     string
	Generation  int
	Reused      bool
	Skipped     bool
	Deferred    bool
	Blocked     bool
	BlockerCode string
}

func NewQueueRepository(db ...*Database) *QueueRepository {
	repo := &QueueRepository{records: map[string]map[string]any{}, recoveryRuns: map[string]map[string]any{}, terminalCompletions: map[string]string{}}
	if len(db) > 0 {
		repo.db = db[0]
	}
	return repo
}

func (r *QueueRepository) Enqueue(command map[string]any) map[string]any {
	if materialQueueProjectionCommand(command) && (!r.queueMemoryAllowed() || !isMaterialQueueProjectionCreateCommand(command)) {
		return materialQueueProjectionRejectedFromCommand(command, "enqueue")
	}
	if record, err := r.enqueuePostgres(context.Background(), command); err == nil {
		return record
	} else if !r.queueMemoryAllowed() {
		return queueDurableFailureRecordFromCommand(command, "enqueue", err)
	}
	return r.enqueueMemory(command)
}

// EnqueueRuntimeTerminalRecovery creates a new runtime_events queue record
// for an incomplete terminal convergence without ever reviving its source
// queue. The convergence row is locked for the entire serializable
// transaction, making concurrent reconcilers reuse an active recovery record
// and making each subsequent terminal recovery an explicit lineage generation.
func (r *QueueRepository) EnqueueRuntimeTerminalRecovery(ctx context.Context, command RuntimeTerminalRecoveryQueueCommand) (RuntimeTerminalRecoveryQueueResult, error) {
	ctx = queueContext(ctx)
	if err := validateRuntimeTerminalRecoveryQueueCommand(command); err != nil {
		return RuntimeTerminalRecoveryQueueResult{}, err
	}
	if !r.queuePostgresReady() {
		return RuntimeTerminalRecoveryQueueResult{}, fmt.Errorf("queue postgres disabled")
	}
	result := RuntimeTerminalRecoveryQueueResult{}
	err := r.db.WithSerializableRetry(ctx, "runtime terminal recovery queue", 3, func(tx *Tx) error {
		var err error
		result, err = r.enqueueRuntimeTerminalRecoveryInTx(ctx, tx, command)
		return err
	})
	return result, err
}

func (r *QueueRepository) Lease(ctx context.Context, queueName, workerID string, leaseTTL time.Duration, taskTypes ...string) (QueueRecord, QueueLeaseProof, error) {
	ctx = queueContext(ctx)
	if queueName == "" || workerID == "" {
		return nil, QueueLeaseProof{}, fmt.Errorf("queue name and worker id are required")
	}
	if isMaterialQueueName(queueName) {
		return nil, QueueLeaseProof{}, ErrMaterialQueueProjectionOwned
	}
	if leaseTTL <= 0 {
		leaseTTL = time.Minute
	}
	tokenHash, err := newQueueLeaseTokenHash()
	if err != nil {
		return nil, QueueLeaseProof{}, err
	}
	if record, proof, err := r.leasePostgres(ctx, queueName, workerID, tokenHash, leaseTTL, taskTypes...); err == nil {
		return record, proof, nil
	} else if errors.Is(err, pgx.ErrNoRows) {
		return nil, QueueLeaseProof{}, ErrNoQueueWork
	} else if !r.queueMemoryAllowed() {
		return nil, QueueLeaseProof{}, err
	}
	return r.leaseMemory(queueName, workerID, tokenHash, leaseTTL, taskTypes...)
}

// LeaseByID is the fenced bridge for workers whose business repository owns
// lane selection but whose queue mirror must still be leased before mutation.
func (r *QueueRepository) LeaseByID(ctx context.Context, queueID, workerID string, leaseTTL time.Duration) (QueueRecord, QueueLeaseProof, error) {
	ctx = queueContext(ctx)
	if queueID == "" || workerID == "" {
		return nil, QueueLeaseProof{}, fmt.Errorf("queue id and worker id are required")
	}
	if isMaterialQueueProjectionID(queueID) {
		return nil, QueueLeaseProof{}, ErrMaterialQueueProjectionOwned
	}
	if leaseTTL <= 0 {
		leaseTTL = time.Minute
	}
	tokenHash, err := newQueueLeaseTokenHash()
	if err != nil {
		return nil, QueueLeaseProof{}, err
	}
	if record, proof, err := r.leaseByIDPostgres(ctx, queueID, workerID, tokenHash, leaseTTL); err == nil {
		return record, proof, nil
	} else if errors.Is(err, pgx.ErrNoRows) {
		return nil, QueueLeaseProof{}, ErrNoQueueWork
	} else if !r.queueMemoryAllowed() {
		return nil, QueueLeaseProof{}, err
	}
	return r.leaseByIDMemory(queueID, workerID, tokenHash, leaseTTL)
}

func (r *QueueRepository) MarkRunning(ctx context.Context, proof QueueLeaseProof) (QueueRecord, error) {
	if err := validateQueueLeaseProof(proof); err != nil {
		return nil, err
	}
	if err := rejectMaterialQueueProjectionProof(proof); err != nil {
		return nil, err
	}
	return r.mutateWithProof(queueContext(ctx), proof, "running", "", false, 0)
}

func (r *QueueRepository) Heartbeat(ctx context.Context, proof QueueLeaseProof, leaseTTL time.Duration) (QueueLeaseProof, error) {
	ctx = queueContext(ctx)
	if err := validateQueueLeaseProof(proof); err != nil {
		return QueueLeaseProof{}, err
	}
	if err := rejectMaterialQueueProjectionProof(proof); err != nil {
		return QueueLeaseProof{}, err
	}
	if leaseTTL <= 0 {
		leaseTTL = time.Minute
	}
	if refreshed, err := r.heartbeatPostgres(ctx, proof, leaseTTL); err == nil {
		return refreshed, nil
	} else if !r.queueMemoryAllowed() {
		return QueueLeaseProof{}, err
	}
	return r.heartbeatMemory(proof, leaseTTL)
}

func (r *QueueRepository) Complete(ctx context.Context, proof QueueLeaseProof) (QueueRecord, error) {
	return r.mutateWithProof(queueContext(ctx), proof, "succeeded", "", false, 0)
}

// CompleteTerminalConvergence makes the queue acknowledgement and convergence
// checkpoint one commit, closing the crash window between those two facts.
func (r *QueueRepository) CompleteTerminalConvergence(ctx context.Context, proof QueueLeaseProof, convergenceID string) (QueueRecord, error) {
	ctx = queueContext(ctx)
	if err := validateQueueLeaseProof(proof); err != nil || strings.TrimSpace(convergenceID) == "" {
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("convergence id is required")
	}
	if err := rejectMaterialQueueProjectionProof(proof); err != nil {
		return nil, err
	}
	if record, err := r.completeTerminalConvergencePostgres(ctx, proof, convergenceID); err == nil {
		return record, nil
	} else if !r.queueMemoryAllowed() {
		return nil, err
	}
	return r.completeTerminalConvergenceMemory(proof, convergenceID)
}

// BlockRuntimeTerminalRecoveryLegacyContract is the narrow terminal path for
// a RuntimeEventWorker that has already proved an immutable legacy contract
// mismatch. It records an auditable blocker and dead-letters only the exact
// leased source/recovery queue. It never projects product success, appends a
// public event, or acknowledges terminal convergence completion.
func (r *QueueRepository) BlockRuntimeTerminalRecoveryLegacyContract(ctx context.Context, proof QueueLeaseProof, convergenceID string) (QueueRecord, error) {
	ctx = queueContext(ctx)
	if err := validateQueueLeaseProof(proof); err != nil || strings.TrimSpace(convergenceID) == "" {
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("convergence id is required")
	}
	if err := rejectMaterialQueueProjectionProof(proof); err != nil {
		return nil, err
	}
	if !r.queuePostgresReady() {
		return nil, fmt.Errorf("queue postgres disabled")
	}
	return r.blockRuntimeTerminalRecoveryLegacyContractPostgres(ctx, proof, convergenceID)
}

func (r *QueueRepository) Fail(ctx context.Context, proof QueueLeaseProof, errorCode string, retryable bool) (QueueRecord, error) {
	status := "failed"
	if retryable {
		status = "retry_wait"
	}
	return r.mutateWithProof(queueContext(ctx), proof, status, errorCode, retryable, 0)
}

func (r *QueueRepository) ScheduleRetry(ctx context.Context, proof QueueLeaseProof, delay time.Duration, errorCode string) (QueueRecord, error) {
	if delay < 0 {
		delay = 0
	}
	return r.mutateWithProof(queueContext(ctx), proof, "retry_wait", errorCode, true, delay)
}

func (r *QueueRepository) MarkTimeout(ctx context.Context, proof QueueLeaseProof, errorCode string) (QueueRecord, error) {
	return r.mutateWithProof(queueContext(ctx), proof, "timeout", errorCode, false, 0)
}

func (r *QueueRepository) UpdateErrorSummary(ctx context.Context, proof QueueLeaseProof, errorSummary map[string]any) (QueueRecord, error) {
	if len(errorSummary) == 0 {
		return nil, fmt.Errorf("queue error summary is required")
	}
	ctx = queueContext(ctx)
	if err := validateQueueLeaseProof(proof); err != nil {
		return nil, err
	}
	if err := rejectMaterialQueueProjectionProof(proof); err != nil {
		return nil, err
	}
	if record, err := r.updateQueueErrorSummaryPostgres(ctx, proof, errorSummary); err == nil {
		return record, nil
	} else if !r.queueMemoryAllowed() {
		return nil, err
	}
	return r.updateQueueErrorSummaryMemory(proof, errorSummary)
}

func (r *QueueRepository) DeadLetter(ctx context.Context, proof QueueLeaseProof, errorCode string) (QueueRecord, error) {
	return r.mutateWithProof(queueContext(ctx), proof, "dead_letter", errorCode, false, 0)
}

func (r *QueueRepository) ReplayDeadLetter(ctx context.Context, command AdminReplayCommand) (QueueRecord, error) {
	ctx = queueContext(ctx)
	command = normalizeAdminReplayCommand(command)
	if err := validateAdminReplayCommand(command); err != nil {
		return nil, err
	}
	if isMaterialQueueProjectionID(command.QueueID) {
		return nil, ErrMaterialQueueProjectionOwned
	}
	if r.queuePostgresReady() {
		var record QueueRecord
		err := r.db.WithSerializableRetry(ctx, "queue dead-letter replay", 3, func(tx *Tx) error {
			var err error
			record, err = r.ReplayDeadLetterInTx(ctx, tx, command)
			return err
		})
		return record, err
	}
	if !r.queueMemoryAllowed() {
		return nil, fmt.Errorf("queue postgres disabled")
	}
	return r.replayDeadLetterMemory(command)
}

// ReplayDeadLetterInTx creates a new immutable attempt lineage without owning
// the transaction. Admin service callers use it to commit replay, audit and API
// idempotency completion atomically.
func (r *QueueRepository) ReplayDeadLetterInTx(ctx context.Context, tx *Tx, command AdminReplayCommand) (QueueRecord, error) {
	ctx = queueContext(ctx)
	command = normalizeAdminReplayCommand(command)
	if err := validateAdminReplayCommand(command); err != nil {
		return nil, err
	}
	if r == nil || tx == nil || tx.tx == nil {
		return nil, fmt.Errorf("queue replay transaction disabled")
	}
	source, err := loadAdminReplaySourceInTx(ctx, tx, command.QueueID)
	if err != nil {
		return nil, err
	}
	if isMaterialQueueName(source.queueName) {
		return nil, ErrMaterialQueueProjectionOwned
	}
	if err := validateAdminReplaySource(source); err != nil {
		return nil, err
	}

	seriesID, err := newQueueAttemptSeriesID()
	if err != nil {
		return nil, err
	}
	suffix := deterministicReplaySuffix(command)
	replayQueueID := command.QueueID + ":replay:" + suffix
	_, err = tx.ExecRaw(ctx, `
insert into task_queue_records(
  queue_id, queue_name, task_type, task_id, dedupe_key, status, priority,
  attempt, max_attempts, available_at, payload, error_summary, attempt_series_id,
  replayed_from_queue_id, replayed_by, replay_reason
)
select $2, queue_name, task_type, task_id,
       coalesce(dedupe_key, queue_id) || ':replay:' || $3,
       'pending', priority, 0, max_attempts, now(), payload, '{}'::jsonb, $4, queue_id, $5, $6
from task_queue_records
where queue_id = $1 and status = 'dead_letter'
on conflict (queue_id) do nothing`, command.QueueID, replayQueueID, suffix, seriesID, command.OperatorID, command.Reason)
	if err != nil {
		return nil, err
	}
	replay, err := getQueueRecordInTx(ctx, tx, replayQueueID)
	if err != nil {
		return nil, err
	}
	if stringOr(replay["replayedFromQueueId"], "") != command.QueueID ||
		stringOr(replay["replayedBy"], "") != command.OperatorID ||
		stringOr(replay["replayReason"], "") != command.Reason {
		return nil, fmt.Errorf("%w: replay lineage does not match the command", ErrQueueReplayConflict)
	}
	return replay, nil
}

func (r *QueueRepository) mutateWithProof(ctx context.Context, proof QueueLeaseProof, status, errorCode string, retryable bool, retryDelay time.Duration) (QueueRecord, error) {
	if err := validateQueueLeaseProof(proof); err != nil {
		return nil, err
	}
	if err := rejectMaterialQueueProjectionProof(proof); err != nil {
		return nil, err
	}
	if record, err := r.updateQueueStatusPostgres(ctx, proof, status, errorCode, retryable, retryDelay); err == nil {
		return record, nil
	} else if !r.queueMemoryAllowed() {
		return nil, err
	}
	return r.updateQueueMemory(proof, status, errorCode, retryable, retryDelay)
}

func queueContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func validateQueueLeaseProof(proof QueueLeaseProof) error {
	if proof.QueueID == "" || proof.WorkerID == "" || proof.Attempt <= 0 || proof.TokenHash == "" || proof.FencingToken <= 0 || proof.LeaseExpiresAt.IsZero() {
		return ErrStaleQueueLease
	}
	return nil
}

func rejectMaterialQueueProjectionProof(proof QueueLeaseProof) error {
	if isMaterialQueueProjectionID(proof.QueueID) {
		return ErrMaterialQueueProjectionOwned
	}
	return nil
}

func isMaterialQueueName(queueName string) bool {
	switch strings.TrimSpace(queueName) {
	case queuecontract.QueueMaterialExtract,
		queuecontract.QueueMaterialMinutes,
		queuecontract.QueueMaterialSummary,
		queuecontract.QueueMaterialDeposit,
		queuecontract.QueueMaterialRegenerate,
		queuecontract.QueueMaterialWrite,
		queuecontract.QueueMaterialRecovery:
		return true
	default:
		return false
	}
}

func isMaterialQueueProjectionID(queueID string) bool {
	queueName, _, found := strings.Cut(strings.TrimSpace(queueID), ":")
	return found && isMaterialQueueName(queueName)
}

func materialQueueProjectionCommand(command map[string]any) bool {
	return isMaterialQueueName(stringOr(command["queueName"], "")) ||
		isMaterialQueueProjectionID(stringOr(command["queueId"], ""))
}

// isMaterialQueueProjectionCreateCommand is the constrained handoff used by
// MaterialRepository's in-memory test mirror. It creates only the observable
// projection for an already-created Material job; it is not a generic queue
// execution path. Generic leasing and every generic queue transition remain
// rejected below.
func isMaterialQueueProjectionCreateCommand(command map[string]any) bool {
	queueName := stringOr(command["queueName"], "")
	taskID := stringOr(command["taskId"], "")
	if !isMaterialQueueName(queueName) || taskID == "" ||
		stringOr(command["queueId"], "") != queueName+":"+taskID ||
		!strings.HasPrefix(stringOr(command["dedupeKey"], ""), queueName+":") {
		return false
	}
	payload := mapValue(command["payload"])
	if stringOr(payload["materialId"], "") == "" {
		return false
	}
	jobID := stringOr(payload["materialJobId"], stringOr(payload["materialRecoveryJobId"], ""))
	return jobID == taskID || (jobID == "" && stringOr(payload["retryOfJobId"], "") != "")
}

func materialQueueProjectionRejectedFromCommand(command map[string]any, operation string) map[string]any {
	queueName := stringOr(command["queueName"], "")
	queueID := stringOr(command["queueId"], "")
	if queueID == "" {
		queueID = queueName + ":" + stringOr(command["taskId"], "")
	}
	return materialQueueProjectionRejectedRecord(queueID, operation)
}

func materialQueueProjectionRejectedRecord(queueID, operation string) map[string]any {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	return map[string]any{
		"queueId":      queueID,
		"status":       "failed",
		"storage":      "not_persisted",
		"errorCode":    ErrMaterialQueueProjectionOwned.Error(),
		"errorSummary": map[string]any{"errorCode": ErrMaterialQueueProjectionOwned.Error(), "retryable": false, "operation": operation},
		"createdAt":    now,
		"updatedAt":    now,
	}
}

func newQueueLeaseTokenHash() (string, error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("generate queue lease token: %w", err)
	}
	sum := sha256.Sum256(raw)
	return fmt.Sprintf("%x", sum[:]), nil
}

func newQueueAttemptSeriesID() (string, error) {
	token, err := newQueueLeaseTokenHash()
	if err != nil {
		return "", err
	}
	return "qas_" + token[:32], nil
}

func deterministicReplaySuffix(command AdminReplayCommand) string {
	sum := sha256.Sum256([]byte(command.QueueID + "\x00" + command.OperatorID + "\x00" + command.IdempotencyKey))
	return fmt.Sprintf("%x", sum[:8])
}

func allowedQueueSourceStatuses(target string) []string {
	if target == "running" {
		return []string{"leased"}
	}
	return []string{"leased", "running"}
}

type activeQueueLeaseSnapshot struct {
	maxAttempts     int
	attemptSeriesID string
	payload         []byte
}

func (r *QueueRepository) activeQueueLeaseSnapshotTx(ctx context.Context, tx pgx.Tx, proof QueueLeaseProof, target string) (activeQueueLeaseSnapshot, error) {
	var snapshot activeQueueLeaseSnapshot
	err := tx.QueryRow(ctx, `
select max_attempts, coalesce(attempt_series_id, ''), payload
from task_queue_records
where queue_id = $1
  and queue_name not in ('material_extract','material_minutes','material_summary','material_deposit','material_regenerate','material_write','material_recovery')
  and status = any($7::text[])
  and lease_owner = $2
  and attempt = $3
  and lease_token_hash = $4
  and lease_fencing_token = $5
  and lease_expires_at = $6
  and lease_expires_at > now()
for update`, proof.QueueID, proof.WorkerID, proof.Attempt, proof.TokenHash, proof.FencingToken, proof.LeaseExpiresAt, allowedQueueSourceStatuses(target)).Scan(&snapshot.maxAttempts, &snapshot.attemptSeriesID, &snapshot.payload)
	if errors.Is(err, pgx.ErrNoRows) {
		return activeQueueLeaseSnapshot{}, ErrStaleQueueLease
	}
	return snapshot, err
}

func (r *QueueRepository) insertQueueDeadLetterAuditTx(ctx context.Context, tx pgx.Tx, queueID, seriesID string, attempt int, owner, tokenHash string, fencingToken int64, reason string, rawError, rawPayload []byte) error {
	if seriesID == "" {
		return fmt.Errorf("queue attempt series missing for dead-letter audit")
	}
	auditID, err := newQueueAttemptSeriesID()
	if err != nil {
		return err
	}
	if reason == "" {
		reason = "LEASE_EXHAUSTED"
	}
	_, err = tx.Exec(ctx, `
insert into task_queue_dead_letter_audit(
  audit_id, queue_id, attempt_series_id, attempt, lease_owner,
  lease_token_hash, lease_fencing_token, reason, safe_error_summary, payload_snapshot
) values ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10::jsonb)`, auditID, queueID, seriesID, attempt, owner, tokenHash, fencingToken, reason, string(rawError), string(rawPayload))
	return err
}

type adminReplaySource struct {
	queueName         string
	taskType          string
	taskID            string
	status            string
	attempt           int
	maxAttempts       int
	attemptSeriesID   string
	leaseOwner        string
	leaseExpiresAt    time.Time
	leaseExpiresAtSet bool
	deadLetteredAt    time.Time
	deadLetteredAtSet bool
	deadLetterReason  string
	errorSummary      []byte
}

func loadAdminReplaySourceInTx(ctx context.Context, tx *Tx, queueID string) (adminReplaySource, error) {
	var source adminReplaySource
	err := tx.QueryRowRaw(ctx, `
select queue_name, task_type, task_id, status, attempt, max_attempts,
       coalesce(attempt_series_id, ''), coalesce(lease_owner, ''),
       coalesce(lease_expires_at, '0001-01-01'::timestamptz), lease_expires_at is not null,
       coalesce(dead_lettered_at, '0001-01-01'::timestamptz), dead_lettered_at is not null,
       coalesce(dead_letter_reason, ''), coalesce(error_summary, '{}'::jsonb)
from task_queue_records
where queue_id = $1
for update`, queueID).Scan(
		&source.queueName, &source.taskType, &source.taskID, &source.status,
		&source.attempt, &source.maxAttempts, &source.attemptSeriesID, &source.leaseOwner,
		&source.leaseExpiresAt, &source.leaseExpiresAtSet, &source.deadLetteredAt,
		&source.deadLetteredAtSet, &source.deadLetterReason, &source.errorSummary,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return adminReplaySource{}, ErrQueueReplayNotFound
	}
	return source, err
}

func validateAdminReplayCommand(command AdminReplayCommand) error {
	if strings.TrimSpace(command.QueueID) == "" || strings.TrimSpace(command.OperatorID) == "" ||
		strings.TrimSpace(command.Reason) == "" || strings.TrimSpace(command.IdempotencyKey) == "" {
		return fmt.Errorf("%w: queueId, operatorId, reason and idempotencyKey are required", ErrQueueReplayInvalid)
	}
	return nil
}

// normalizeAdminReplayCommand keeps user-provided reason text canonical while
// preserving the idempotency key exactly as the API middleware stored it.
func normalizeAdminReplayCommand(command AdminReplayCommand) AdminReplayCommand {
	command.QueueID = strings.TrimSpace(command.QueueID)
	command.OperatorID = strings.TrimSpace(command.OperatorID)
	command.Reason = strings.TrimSpace(command.Reason)
	return command
}

func validateAdminReplaySource(source adminReplaySource) error {
	if isMaterialQueueName(source.queueName) {
		return ErrMaterialQueueProjectionOwned
	}
	if source.status != "dead_letter" {
		return fmt.Errorf("%w: queue record is not dead_letter", ErrQueueReplayConflict)
	}
	if source.leaseOwner != "" || source.leaseExpiresAtSet {
		return fmt.Errorf("%w: dead-letter queue record still owns a lease", ErrQueueReplayConflict)
	}
	if strings.TrimSpace(source.taskID) == "" || strings.TrimSpace(source.taskType) == "" ||
		source.attempt <= 0 || source.maxAttempts <= 0 || source.attempt > source.maxAttempts ||
		strings.TrimSpace(source.attemptSeriesID) == "" {
		return fmt.Errorf("%w: dead-letter queue record is incomplete", ErrQueueReplayConflict)
	}
	if !adminReplayTaskAllowed(source.queueName, source.taskType) {
		return fmt.Errorf("%w: queue/task type is not replayable", ErrQueueReplayForbidden)
	}
	if !source.deadLetteredAtSet && strings.TrimSpace(source.deadLetterReason) == "" && !hasJSONFields(source.errorSummary) {
		return fmt.Errorf("%w: dead-letter evidence is missing", ErrQueueReplayConflict)
	}
	return nil
}

func hasJSONFields(raw []byte) bool {
	if len(raw) == 0 {
		return false
	}
	var value map[string]any
	return json.Unmarshal(raw, &value) == nil && len(value) > 0
}

func adminReplayTaskAllowed(queueName, taskType string) bool {
	queueName = strings.TrimSpace(queueName)
	taskType = strings.TrimSpace(taskType)
	if queueName == "" || taskType == "" {
		return false
	}
	switch queueName {
	case queuecontract.QueueAgentPlanning:
		return taskType == "agent_planning"
	case queuecontract.QueueRuntimeEvents:
		return taskType == "runtime_event_ingest"
	case queuecontract.QueueRuntimeAbort:
		return taskType == "runtime_abort"
	case queuecontract.QueueWorkspaceIndex:
		return taskType == "workspace_index"
	case queuecontract.QueueWorkspaceEmbedding:
		return taskType == "workspace_embedding"
	case queuecontract.QueueAIRuntimeInteractive, queuecontract.QueueAIRuntimeRecording, queuecontract.QueueAIRuntimeBackground:
		return taskType == "runtime_dispatch"
	case queuecontract.QueueAIRuntime:
		return taskType == "runtime_dispatch" || strings.HasPrefix(taskType, "work_ai_") ||
			strings.HasPrefix(taskType, "feed_ai_") || strings.HasSuffix(taskType, "_generation")
	case queuecontract.QueueASR:
		return taskType == "file_recognition" || taskType == "lightweight_recognition" || strings.HasPrefix(taskType, "asr_")
	case queuecontract.QueueRecording:
		switch taskType {
		case "recording_final_transcript", "final_transcript_generation", "minutes_generation", "summary_generation", "recording_deposit", "workspace_write":
			return true
		default:
			return false
		}
	case queuecontract.QueueHotspotPlatform:
		return strings.HasPrefix(taskType, "hotspot_")
	case queuecontract.QueueWorkspaceWrite, queuecontract.QueueWorkspaceSync, queuecontract.QueueNotificationOutbox,
		queuecontract.QueueRecovery, queuecontract.QueueCompensation, queuecontract.QueueQuotaReservationCleanup:
		return true
	default:
		return false
	}
}

func (r *QueueRepository) MarkIgnored(queueID, reason, operatorID string) map[string]any {
	if isMaterialQueueProjectionID(queueID) {
		return materialQueueProjectionRejectedRecord(queueID, "mark_ignored")
	}
	if record, err := r.adminQueueActionPostgres(context.Background(), queueID, "ignored", reason, operatorID); err == nil {
		return record
	} else if errors.Is(err, ErrMaterialQueueProjectionOwned) {
		return materialQueueProjectionRejectedRecord(queueID, "mark_ignored")
	} else if !r.queueMemoryAllowed() {
		return queueDurableFailureRecord(queueID, "mark_ignored", err)
	}
	record := r.adminUpdateQueueMemory(queueID, "ignored", reason)
	record["reason"] = reason
	record["operatorId"] = operatorID
	return record
}

func (r *QueueRepository) QueueSummary(filters map[string]any) map[string]any {
	if summary, err := r.queueSummaryPostgres(context.Background(), filters); err == nil {
		return summary
	} else if !r.queueMemoryAllowed() {
		return queueDurableFailureSummary("queue_summary", err)
	}
	return r.queueSummaryMemory(filters)
}

func (r *QueueRepository) ListQueueRecords(filters map[string]any) []map[string]any {
	if records, err := r.listQueueRecordsPostgres(context.Background(), filters); err == nil {
		return records
	} else if !r.queueMemoryAllowed() {
		return []map[string]any{queueDurableFailureRecord("queue_records", "list_queue_records", err)}
	}
	return r.listQueueRecordsMemory(filters)
}

func (r *QueueRepository) ListRecoveryRuns(filters map[string]any) []map[string]any {
	if runs, err := r.listRecoveryRunsPostgres(context.Background(), filters); err == nil {
		return runs
	} else if !r.queueMemoryAllowed() {
		return []map[string]any{queueDurableFailureRecord("recovery_runs", "list_recovery_runs", err)}
	}
	return r.listRecoveryRunsMemory(filters)
}

func (r *QueueRepository) GetRecoveryRun(runID string) map[string]any {
	if run, err := r.getRecoveryRunPostgres(context.Background(), runID); err == nil {
		return run
	} else if errors.Is(err, pgx.ErrNoRows) {
		return map[string]any{"runId": runID, "status": "not_found", "actions": []any{}}
	} else if !r.queueMemoryAllowed() {
		return queueDurableFailureRecord(runID, "get_recovery_run", err)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if run, ok := r.recoveryRuns[runID]; ok {
		return copyMap(run)
	}
	return map[string]any{"runId": runID, "status": "not_found", "actions": []any{}}
}

func (r *QueueRepository) RecordRecoveryRun(runID, jobType, businessDate, status string, payload, result map[string]any) map[string]any {
	if run, err := r.recordRecoveryRunPostgres(context.Background(), runID, jobType, businessDate, status, payload, result); err == nil {
		return run
	} else if !r.queueMemoryAllowed() {
		return queueDurableFailureRecord(runID, "record_recovery_run", err)
	}
	if runID == "" {
		runID = "system_job_" + fmt.Sprint(time.Now().UTC().UnixNano())
	}
	if status == "" {
		status = "running"
	}
	run := map[string]any{"runId": runID, "jobType": jobType, "businessDate": businessDate, "status": status, "payload": mapValue(payload), "result": mapValue(result), "createdAt": time.Now().UTC().Format(time.RFC3339), "updatedAt": time.Now().UTC().Format(time.RFC3339)}
	r.mu.Lock()
	r.recoveryRuns[runID] = run
	r.mu.Unlock()
	return copyMap(run)
}

func (r *QueueRepository) RecoverExpiredLeases(ctx context.Context, now time.Time) (int, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	ctx = queueContext(ctx)
	if count, err := r.recoverExpiredLeasesPostgres(ctx, now); err == nil {
		return count, nil
	} else if !r.queueMemoryAllowed() {
		return 0, err
	}
	return r.recoverExpiredLeasesMemory(now), nil
}

func (r *QueueRepository) RecoverRunningWithoutHeartbeat(ctx context.Context, now time.Time) (int, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	ctx = queueContext(ctx)
	if recovered, err := r.recoverRunningWithoutHeartbeatPostgres(ctx, now); err == nil {
		return recovered, nil
	} else if !r.queueMemoryAllowed() {
		return 0, err
	}
	return r.recoverRunningWithoutHeartbeatMemory(now), nil
}

func (r *QueueRepository) RequeueRecoverableQueue(ctx context.Context, queueName, errorCode, reason string) (int, error) {
	ctx = queueContext(ctx)
	if isMaterialQueueName(queueName) {
		return 0, ErrMaterialQueueProjectionOwned
	}
	if recovered, err := r.requeueRecoverableQueuePostgres(ctx, queueName, errorCode, reason); err == nil {
		return recovered, nil
	} else if !r.queueMemoryAllowed() {
		return 0, err
	}
	return r.requeueRecoverableQueueMemory(queueName, errorCode, reason), nil
}

func (r *QueueRepository) MarkDeadLetterCandidates(ctx context.Context, now time.Time) (int, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	ctx = queueContext(ctx)
	if count, err := r.markDeadLetterCandidatesPostgres(ctx, now); err == nil {
		return count, nil
	} else if !r.queueMemoryAllowed() {
		return 0, err
	}
	return r.markDeadLetterCandidatesMemory(now), nil
}

func (r *QueueRepository) enqueuePostgres(ctx context.Context, command map[string]any) (map[string]any, error) {
	if r == nil || r.db == nil || r.db.Disabled || r.db.Pool == nil {
		return nil, fmt.Errorf("queue postgres disabled")
	}
	queueName := stringOr(command["queueName"], "default")
	taskID := stringOr(command["taskId"], "")
	if taskID == "" {
		return nil, fmt.Errorf("task id required")
	}
	taskType := stringOr(command["taskType"], "unknown")
	queueID := stringOr(command["queueId"], queueName+":"+taskID)
	priority := intValue(command["priority"])
	if priority == 0 {
		priority = 100
	}
	maxAttempts := intValue(command["maxAttempts"])
	if maxAttempts == 0 {
		maxAttempts = 3
	}
	availableAt := timeValue(command["availableAt"], time.Now().UTC().Add(-time.Second))
	payload := mapValue(command["payload"])
	if len(payload) == 0 {
		payload = copyMap(command)
	}
	rawPayload, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	attemptSeriesID, err := newQueueAttemptSeriesID()
	if err != nil {
		return nil, err
	}
	_, err = r.db.Pool.Exec(ctx, `
insert into task_queue_records(queue_id, queue_name, task_type, task_id, dedupe_key, status, priority, max_attempts, available_at, payload, attempt_series_id)
values ($1, $2, $3, $4, nullif($5, ''), 'pending', $6, $7, $8, $9::jsonb, $10)
on conflict (queue_id) do update set
  queue_name = case
    when task_queue_records.status in ('failed','timeout','dead_letter','succeeded','ignored') then task_queue_records.queue_name
    else excluded.queue_name
  end,
  task_type = case
    when task_queue_records.status in ('failed','timeout','dead_letter','succeeded','ignored') then task_queue_records.task_type
    else excluded.task_type
  end,
  task_id = case
    when task_queue_records.status in ('failed','timeout','dead_letter','succeeded','ignored') then task_queue_records.task_id
    else excluded.task_id
  end,
  dedupe_key = case
    when task_queue_records.status in ('failed','timeout','dead_letter','succeeded','ignored') then task_queue_records.dedupe_key
    else excluded.dedupe_key
  end,
  status = task_queue_records.status,
  priority = case
    when task_queue_records.status in ('failed','timeout','dead_letter','succeeded','ignored') then task_queue_records.priority
    else excluded.priority
  end,
  max_attempts = case
    when task_queue_records.status in ('failed','timeout','dead_letter','succeeded','ignored') then task_queue_records.max_attempts
    else excluded.max_attempts
  end,
  available_at = case
    when task_queue_records.status in ('failed','timeout','dead_letter','succeeded','ignored') then task_queue_records.available_at
    else task_queue_records.available_at
  end,
  payload = case
    when task_queue_records.status in ('failed','timeout','dead_letter','succeeded','ignored') then task_queue_records.payload
    else excluded.payload
  end,
  lease_owner = task_queue_records.lease_owner,
  lease_expires_at = task_queue_records.lease_expires_at,
  error_summary = task_queue_records.error_summary,
  updated_at = case
    when task_queue_records.status in ('failed','timeout','dead_letter','succeeded','ignored') then task_queue_records.updated_at
    else now()
  end`,
		queueID, queueName, taskType, taskID, stringOr(command["dedupeKey"], ""), priority, maxAttempts, availableAt, string(rawPayload), attemptSeriesID)
	if err != nil {
		return nil, err
	}
	return r.getQueueRecordPostgres(ctx, queueID)
}

func (r *QueueRepository) enqueueRuntimeTerminalRecoveryInTx(ctx context.Context, tx *Tx, command RuntimeTerminalRecoveryQueueCommand) (RuntimeTerminalRecoveryQueueResult, error) {
	if r == nil || tx == nil || tx.tx == nil {
		return RuntimeTerminalRecoveryQueueResult{}, fmt.Errorf("runtime terminal recovery queue transaction disabled")
	}
	rows, err := tx.Query(ctx, `
	select queue_id,queue_completed_at is not null as completed,coalesce(last_error_code,'') as last_error_code
from runtime_terminal_convergences
where convergence_id=@convergence
  and dispatch_id=@dispatch
  and run_id=@run
for update`, map[string]any{
		"convergence": command.ConvergenceID,
		"dispatch":    command.DispatchID,
		"run":         command.RunID,
	})
	if err != nil {
		return RuntimeTerminalRecoveryQueueResult{}, err
	}
	if len(rows) != 1 {
		return RuntimeTerminalRecoveryQueueResult{}, ErrRuntimeTerminalConvergenceNotFound
	}
	if queueRecordBool(rows[0]["completed"]) {
		return RuntimeTerminalRecoveryQueueResult{Skipped: true}, nil
	}
	if runtimeTerminalRecoveryBlockerCode(stringOr(rows[0]["last_error_code"], "")) != "" {
		return RuntimeTerminalRecoveryQueueResult{Blocked: true, BlockerCode: stringOr(rows[0]["last_error_code"], "")}, nil
	}
	originalQueueID := stringOr(rows[0]["queue_id"], "")
	if originalQueueID == "" {
		return r.blockRuntimeTerminalRecoveryInTx(ctx, tx, command.ConvergenceID, "RUNTIME_TERMINAL_RECOVERY_QUEUE_INVALID_SOURCE")
	}
	originalRecord, err := getQueueRecordInTx(ctx, tx, originalQueueID)
	if err != nil {
		return r.blockRuntimeTerminalRecoveryInTx(ctx, tx, command.ConvergenceID, "RUNTIME_TERMINAL_RECOVERY_QUEUE_INVALID_SOURCE")
	}
	if !runtimeTerminalRecoverySourceRecordMatches(originalRecord, command) {
		return r.blockRuntimeTerminalRecoveryInTx(ctx, tx, command.ConvergenceID, "RUNTIME_TERMINAL_RECOVERY_QUEUE_INVALID_SOURCE")
	}
	if !runtimeTerminalRecoveryTerminalStatus(stringOr(originalRecord["status"], "")) {
		return RuntimeTerminalRecoveryQueueResult{Deferred: true}, nil
	}

	lineages, err := tx.Query(ctx, `
select lineage.generation,lineage.source_queue_id,lineage.recovery_queue_id,lineage.recovery_dedupe_key
from runtime_terminal_convergence_recovery_queue_lineage lineage
join task_queue_records queue_record on queue_record.queue_id=lineage.recovery_queue_id
where lineage.convergence_id=@convergence
order by lineage.generation
for update of lineage,queue_record`, map[string]any{"convergence": command.ConvergenceID})
	if err != nil {
		return RuntimeTerminalRecoveryQueueResult{}, err
	}

	sourceQueueID := originalQueueID
	generation := 1
	for index, lineage := range lineages {
		lineageGeneration := intValue(lineage["generation"])
		lineageSourceQueueID := stringOr(lineage["source_queue_id"], "")
		lineageRecoveryQueueID := stringOr(lineage["recovery_queue_id"], "")
		lineageDedupeKey := stringOr(lineage["recovery_dedupe_key"], "")
		if lineageGeneration != generation || lineageSourceQueueID != sourceQueueID ||
			lineageRecoveryQueueID != runtimeTerminalRecoveryQueueID(command.ConvergenceID, generation) ||
			lineageDedupeKey != runtimeTerminalRecoveryDedupeKey(command.ConvergenceID, generation) {
			return r.blockRuntimeTerminalRecoveryInTx(ctx, tx, command.ConvergenceID, "RUNTIME_TERMINAL_RECOVERY_QUEUE_INVALID_LINEAGE")
		}
		record, getErr := getQueueRecordInTx(ctx, tx, lineageRecoveryQueueID)
		if getErr != nil || !runtimeTerminalRecoveryQueueRecordMatches(record, command, sourceQueueID, generation) {
			return r.blockRuntimeTerminalRecoveryInTx(ctx, tx, command.ConvergenceID, "RUNTIME_TERMINAL_RECOVERY_QUEUE_INVALID_LINEAGE")
		}
		status := stringOr(record["status"], "")
		if runtimeTerminalRecoveryRunnableStatus(status) {
			if index != len(lineages)-1 {
				return r.blockRuntimeTerminalRecoveryInTx(ctx, tx, command.ConvergenceID, "RUNTIME_TERMINAL_RECOVERY_QUEUE_INVALID_LINEAGE")
			}
			return RuntimeTerminalRecoveryQueueResult{Record: record, QueueID: lineageRecoveryQueueID, Generation: generation, Reused: true}, nil
		}
		if !runtimeTerminalRecoveryTerminalStatus(status) {
			return r.blockRuntimeTerminalRecoveryInTx(ctx, tx, command.ConvergenceID, "RUNTIME_TERMINAL_RECOVERY_QUEUE_INVALID_LINEAGE")
		}
		sourceQueueID = lineageRecoveryQueueID
		generation++
	}
	if sourceQueueID == "" || generation < 1 {
		return r.blockRuntimeTerminalRecoveryInTx(ctx, tx, command.ConvergenceID, "RUNTIME_TERMINAL_RECOVERY_QUEUE_INVALID_LINEAGE")
	}

	queueID := runtimeTerminalRecoveryQueueID(command.ConvergenceID, generation)
	dedupeKey := runtimeTerminalRecoveryDedupeKey(command.ConvergenceID, generation)
	seriesID, err := newQueueAttemptSeriesID()
	if err != nil {
		return RuntimeTerminalRecoveryQueueResult{}, err
	}
	payload, err := json.Marshal(map[string]any{
		"runId":                 command.RunID,
		"dispatchId":            command.DispatchID,
		"runtimeHostId":         command.RuntimeHostID,
		"terminalConvergenceId": command.ConvergenceID,
		"terminalSourceQueueId": sourceQueueID,
		"recoveryGeneration":    generation,
	})
	if err != nil {
		return RuntimeTerminalRecoveryQueueResult{}, err
	}
	if _, err := tx.ExecRaw(ctx, `
insert into task_queue_records(
  queue_id,queue_name,task_type,task_id,dedupe_key,status,priority,
  max_attempts,available_at,payload,error_summary,attempt_series_id
) values($1,'runtime_events','runtime_event_ingest',$2,$3,'pending',150,7200,now(),$4::jsonb,'{}'::jsonb,$5)
on conflict(queue_id) do nothing`, queueID, command.RunID, dedupeKey, string(payload), seriesID); err != nil {
		return RuntimeTerminalRecoveryQueueResult{}, err
	}
	if _, err := tx.ExecRaw(ctx, `
insert into runtime_terminal_convergence_recovery_queue_lineage(
  convergence_id,generation,source_queue_id,recovery_queue_id,recovery_dedupe_key
) values($1,$2,$3,$4,$5)
on conflict(convergence_id,generation) do nothing`, command.ConvergenceID, generation, sourceQueueID, queueID, dedupeKey); err != nil {
		return RuntimeTerminalRecoveryQueueResult{}, err
	}
	record, err := getQueueRecordInTx(ctx, tx, queueID)
	if err != nil {
		return RuntimeTerminalRecoveryQueueResult{}, err
	}
	if !runtimeTerminalRecoveryQueueRecordMatches(record, command, sourceQueueID, generation) {
		return r.blockRuntimeTerminalRecoveryInTx(ctx, tx, command.ConvergenceID, "RUNTIME_TERMINAL_RECOVERY_QUEUE_INVALID_LINEAGE")
	}
	return RuntimeTerminalRecoveryQueueResult{Record: record, QueueID: queueID, Generation: generation}, nil
}

// blockRuntimeTerminalRecoveryInTx records an auditable, non-retryable
// recovery blocker. It intentionally does not advance any terminal effect:
// public event and queue completion remain pending for operator remediation.
func (r *QueueRepository) blockRuntimeTerminalRecoveryInTx(ctx context.Context, tx *Tx, convergenceID, code string) (RuntimeTerminalRecoveryQueueResult, error) {
	if tx == nil || tx.tx == nil || strings.TrimSpace(convergenceID) == "" || !strings.HasPrefix(code, "RUNTIME_TERMINAL_RECOVERY_QUEUE_INVALID_") {
		return RuntimeTerminalRecoveryQueueResult{}, ErrRuntimeTerminalRecoveryQueueInvalid
	}
	_, err := tx.ExecRaw(ctx, `
update runtime_terminal_convergences
set last_error_code=$2,
    attempt_count=case when last_error_code is distinct from $2 then attempt_count+1 else attempt_count end,
    updated_at=case when last_error_code is distinct from $2 then now() else updated_at end
where convergence_id=$1
  and queue_completed_at is null`, convergenceID, code)
	if err != nil {
		return RuntimeTerminalRecoveryQueueResult{}, err
	}
	return RuntimeTerminalRecoveryQueueResult{Blocked: true, BlockerCode: code}, nil
}

func validateRuntimeTerminalRecoveryQueueCommand(command RuntimeTerminalRecoveryQueueCommand) error {
	if strings.TrimSpace(command.ConvergenceID) == "" || strings.TrimSpace(command.DispatchID) == "" || strings.TrimSpace(command.RunID) == "" || strings.TrimSpace(command.RuntimeHostID) == "" {
		return ErrRuntimeTerminalRecoveryQueueInvalid
	}
	return nil
}

func runtimeTerminalRecoveryQueueID(convergenceID string, generation int) string {
	sum := sha256.Sum256([]byte(convergenceID))
	return fmt.Sprintf("runtime_events:recovery:%x:%d", sum[:12], generation)
}

func runtimeTerminalRecoveryDedupeKey(convergenceID string, generation int) string {
	sum := sha256.Sum256([]byte("runtime_terminal_recovery\x00" + convergenceID))
	return fmt.Sprintf("runtime_terminal_recovery:%x:%d", sum[:12], generation)
}

func runtimeTerminalRecoverySourceRecordMatches(record QueueRecord, command RuntimeTerminalRecoveryQueueCommand) bool {
	if record == nil || stringOr(record["queueName"], "") != queuecontract.QueueRuntimeEvents ||
		stringOr(record["taskType"], "") != "runtime_event_ingest" || stringOr(record["taskId"], "") != command.RunID {
		return false
	}
	payload := mapValue(record["payload"])
	return stringOr(payload["runId"], "") == command.RunID &&
		stringOr(payload["dispatchId"], "") == command.DispatchID &&
		stringOr(payload["runtimeHostId"], "") == command.RuntimeHostID
}

func runtimeTerminalRecoveryQueueRecordMatches(record QueueRecord, command RuntimeTerminalRecoveryQueueCommand, sourceQueueID string, generation int) bool {
	if !runtimeTerminalRecoverySourceRecordMatches(record, command) ||
		stringOr(record["queueId"], "") != runtimeTerminalRecoveryQueueID(command.ConvergenceID, generation) ||
		stringOr(record["dedupeKey"], "") != runtimeTerminalRecoveryDedupeKey(command.ConvergenceID, generation) {
		return false
	}
	payload := mapValue(record["payload"])
	return stringOr(payload["terminalConvergenceId"], "") == command.ConvergenceID &&
		stringOr(payload["terminalSourceQueueId"], "") == sourceQueueID &&
		intValue(payload["recoveryGeneration"]) == generation
}

func runtimeTerminalRecoveryRunnableStatus(status string) bool {
	switch status {
	case "pending", "retry_wait", "leased", "running":
		return true
	default:
		return false
	}
}

func runtimeTerminalRecoveryTerminalStatus(status string) bool {
	switch status {
	case "failed", "timeout", "dead_letter", "succeeded", "ignored":
		return true
	default:
		return false
	}
}

func runtimeTerminalRecoveryBlockerCode(code string) string {
	code = strings.TrimSpace(code)
	if code == RuntimeTerminalLegacyContractBlocked || strings.HasPrefix(code, "RUNTIME_TERMINAL_RECOVERY_QUEUE_INVALID_") {
		return code
	}
	return ""
}

func queueRecordBool(value any) bool {
	typed, ok := value.(bool)
	return ok && typed
}

func (r *QueueRepository) leasePostgres(ctx context.Context, queueName, workerID, tokenHash string, leaseTTL time.Duration, taskTypes ...string) (QueueRecord, QueueLeaseProof, error) {
	if r == nil || r.db == nil || r.db.Disabled || r.db.Pool == nil {
		return nil, QueueLeaseProof{}, fmt.Errorf("queue postgres disabled")
	}
	if leaseTTL <= 0 {
		leaseTTL = 60 * time.Second
	}
	allowedTaskTypes := normalizedQueueTaskTypes(taskTypes...)
	if err := r.deadLetterExpiredExhaustedLeasesPostgres(ctx, queueName, allowedTaskTypes); err != nil {
		return nil, QueueLeaseProof{}, err
	}
	if _, err := r.db.Pool.Exec(ctx, `
update task_queue_records
set status = 'retry_wait',
    lease_owner = null,
    lease_token_hash = null,
    lease_expires_at = null,
    available_at = now(),
    error_summary = coalesce(error_summary, '{}'::jsonb) || jsonb_build_object('recoveryReason', 'expired_lease', 'retryable', true),
    updated_at = now()
where queue_name = $1
  and queue_name not in ('material_extract','material_minutes','material_summary','material_deposit','material_regenerate','material_write','material_recovery')
  and status = 'leased'
  and lease_expires_at is not null
  and lease_expires_at <= now()
  and attempt < max_attempts
  and (cardinality($2::text[]) = 0 or task_type = any($2::text[]))`, queueName, allowedTaskTypes); err != nil {
		return nil, QueueLeaseProof{}, err
	}
	var queueID string
	var attempt int
	var fencingToken int64
	var leaseExpiresAt time.Time
	err := r.db.Pool.QueryRow(ctx, `
with candidate as (
  select queue_id
  from task_queue_records
  where queue_name = $1
    and queue_name not in ('material_extract','material_minutes','material_summary','material_deposit','material_regenerate','material_write','material_recovery')
    and status in ('pending', 'retry_wait')
    and attempt < max_attempts
    and available_at <= now()
    and (cardinality($5::text[]) = 0 or task_type = any($5::text[]))
  order by priority desc, created_at asc
  limit 1
  for update skip locked
)
update task_queue_records
set status = 'leased',
    lease_owner = $2,
    lease_expires_at = now() + ($3::bigint * interval '1 millisecond'),
    lease_token_hash = $4,
    lease_fencing_token = lease_fencing_token + 1,
    heartbeat_at = now(),
    attempt = attempt + 1,
    updated_at = now()
where queue_id in (select queue_id from candidate)
returning queue_id, attempt, lease_fencing_token, lease_expires_at`, queueName, workerID, leaseTTL.Milliseconds(), tokenHash, allowedTaskTypes).Scan(&queueID, &attempt, &fencingToken, &leaseExpiresAt)
	if err != nil {
		return nil, QueueLeaseProof{}, err
	}
	record, err := r.getQueueRecordPostgres(ctx, queueID)
	if err != nil {
		return nil, QueueLeaseProof{}, err
	}
	return record, QueueLeaseProof{QueueID: queueID, WorkerID: workerID, Attempt: attempt, TokenHash: tokenHash, FencingToken: fencingToken, LeaseExpiresAt: leaseExpiresAt.UTC()}, nil
}

func (r *QueueRepository) leaseByIDPostgres(ctx context.Context, queueID, workerID, tokenHash string, leaseTTL time.Duration) (QueueRecord, QueueLeaseProof, error) {
	if !r.queuePostgresReady() {
		return nil, QueueLeaseProof{}, fmt.Errorf("queue postgres disabled")
	}
	var attempt int
	var fencingToken int64
	var leaseExpiresAt time.Time
	err := r.db.Pool.QueryRow(ctx, `
update task_queue_records
set status = 'leased',
    lease_owner = $2,
    lease_expires_at = now() + ($3::bigint * interval '1 millisecond'),
    lease_token_hash = $4,
    lease_fencing_token = lease_fencing_token + 1,
    heartbeat_at = now(),
    attempt = attempt + 1,
    updated_at = now()
where queue_id = $1
  and queue_name not in ('material_extract','material_minutes','material_summary','material_deposit','material_regenerate','material_write','material_recovery')
  and status in ('pending', 'retry_wait')
  and attempt < max_attempts
  and available_at <= now()
returning attempt, lease_fencing_token, lease_expires_at`, queueID, workerID, leaseTTL.Milliseconds(), tokenHash).Scan(&attempt, &fencingToken, &leaseExpiresAt)
	if err != nil {
		return nil, QueueLeaseProof{}, err
	}
	record, err := r.getQueueRecordPostgres(ctx, queueID)
	if err != nil {
		return nil, QueueLeaseProof{}, err
	}
	return record, QueueLeaseProof{QueueID: queueID, WorkerID: workerID, Attempt: attempt, TokenHash: tokenHash, FencingToken: fencingToken, LeaseExpiresAt: leaseExpiresAt.UTC()}, nil
}

func (r *QueueRepository) deadLetterExpiredExhaustedLeasesPostgres(ctx context.Context, queueName string, taskTypes []string) error {
	if r == nil || r.db == nil || r.db.Disabled || r.db.Pool == nil {
		return fmt.Errorf("queue postgres disabled")
	}
	tx, err := r.db.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := r.deadLetterExpiredQueueBatchTx(ctx, tx, time.Now().UTC(), queueName, taskTypes, "LEASE_EXHAUSTED"); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (r *QueueRepository) heartbeatPostgres(ctx context.Context, proof QueueLeaseProof, leaseTTL time.Duration) (QueueLeaseProof, error) {
	if r == nil || r.db == nil || r.db.Disabled || r.db.Pool == nil {
		return QueueLeaseProof{}, fmt.Errorf("queue postgres disabled")
	}
	if leaseTTL <= 0 {
		leaseTTL = 60 * time.Second
	}
	var leaseExpiresAt time.Time
	err := r.db.Pool.QueryRow(ctx, `
update task_queue_records
set lease_expires_at = now() + ($7::bigint * interval '1 millisecond'),
    heartbeat_at = now(),
    updated_at = now()
where queue_id = $1
  and queue_name not in ('material_extract','material_minutes','material_summary','material_deposit','material_regenerate','material_write','material_recovery')
  and status in ('leased', 'running')
  and lease_owner = $2
  and attempt = $3
  and lease_token_hash = $4
  and lease_fencing_token = $5
  and lease_expires_at = $6
  and lease_expires_at > now()
returning lease_expires_at`, proof.QueueID, proof.WorkerID, proof.Attempt, proof.TokenHash, proof.FencingToken, proof.LeaseExpiresAt, leaseTTL.Milliseconds()).Scan(&leaseExpiresAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return QueueLeaseProof{}, ErrStaleQueueLease
		}
		return QueueLeaseProof{}, err
	}
	proof.LeaseExpiresAt = leaseExpiresAt.UTC()
	return proof, nil
}

func (r *QueueRepository) updateQueueStatusPostgres(ctx context.Context, proof QueueLeaseProof, status, errorCode string, retryable bool, retryDelay time.Duration) (QueueRecord, error) {
	if r == nil || r.db == nil || r.db.Disabled || r.db.Pool == nil {
		return nil, fmt.Errorf("queue postgres disabled")
	}
	if err := validateQueueLeaseProof(proof); err != nil {
		return nil, err
	}
	tx, err := r.db.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	snapshot, err := r.activeQueueLeaseSnapshotTx(ctx, tx, proof, status)
	if err != nil {
		return nil, err
	}
	actualStatus := status
	if status == "retry_wait" && snapshot.maxAttempts <= proof.Attempt {
		actualStatus = "dead_letter"
		retryable = false
		if errorCode == "" {
			errorCode = "LEASE_EXHAUSTED"
		}
	}
	errorSummary := map[string]any{"attempt": proof.Attempt, "maxAttempts": snapshot.maxAttempts}
	if errorCode != "" {
		errorSummary["errorCode"] = errorCode
		errorSummary["retryable"] = retryable
	}
	if actualStatus == "retry_wait" {
		errorSummary["nextAttempt"] = proof.Attempt + 1
		errorSummary["retryDelaySeconds"] = int(retryDelay.Seconds())
	}
	rawError, err := json.Marshal(errorSummary)
	if err != nil {
		return nil, err
	}
	tag, err := tx.Exec(ctx, `
update task_queue_records
set status = $2,
    lease_owner = case when $2 = 'running' then lease_owner else null end,
    lease_token_hash = case when $2 = 'running' then lease_token_hash else null end,
    lease_expires_at = case when $2 = 'running' then lease_expires_at else null end,
    available_at = case when $2 = 'retry_wait' then now() + ($8::bigint * interval '1 millisecond') else available_at end,
    dead_lettered_at = case when $2 = 'dead_letter' then now() else dead_lettered_at end,
    dead_letter_reason = case when $2 = 'dead_letter' then coalesce(nullif($7, ''), 'LEASE_EXHAUSTED') else dead_letter_reason end,
    error_summary = case when $2 = 'succeeded' then '{}'::jsonb else coalesce(error_summary, '{}'::jsonb) || $9::jsonb end,
    updated_at = now()
where queue_id = $1
  and queue_name not in ('material_extract','material_minutes','material_summary','material_deposit','material_regenerate','material_write','material_recovery')
  and status = any($11::text[])
  and lease_owner = $3
  and attempt = $4
  and lease_token_hash = $5
  and lease_fencing_token = $6
  and lease_expires_at = $10
  and lease_expires_at > now()`, proof.QueueID, actualStatus, proof.WorkerID, proof.Attempt, proof.TokenHash, proof.FencingToken, errorCode, retryDelay.Milliseconds(), string(rawError), proof.LeaseExpiresAt, allowedQueueSourceStatuses(status))
	if err != nil {
		return nil, err
	}
	if tag.RowsAffected() != 1 {
		return nil, ErrStaleQueueLease
	}
	if (actualStatus == "failed" || actualStatus == "timeout" || actualStatus == "dead_letter") && !retryable {
		recordingStatus := actualStatus
		if recordingStatus == "dead_letter" {
			recordingStatus = "failed"
		}
		if err = r.compensateRecordingQueueFailureTx(ctx, tx, proof.QueueID, recordingStatus, rawError); err != nil {
			return nil, err
		}
		if err = r.compensateAIRuntimeQueueFailureTx(ctx, tx, proof.QueueID, actualStatus, rawError); err != nil {
			return nil, err
		}
	}
	if actualStatus == "dead_letter" {
		if err = r.insertQueueDeadLetterAuditTx(ctx, tx, proof.QueueID, snapshot.attemptSeriesID, proof.Attempt, proof.WorkerID, proof.TokenHash, proof.FencingToken, errorCode, rawError, snapshot.payload); err != nil {
			return nil, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	return r.getQueueRecordPostgres(ctx, proof.QueueID)
}

func (r *QueueRepository) completeTerminalConvergencePostgres(ctx context.Context, proof QueueLeaseProof, convergenceID string) (QueueRecord, error) {
	if !r.queuePostgresReady() {
		return nil, fmt.Errorf("queue postgres disabled")
	}
	if err := r.assertRuntimeTerminalConvergenceQueueLinked(ctx, convergenceID, proof.QueueID); err != nil {
		return nil, err
	}
	var alreadyComplete bool
	err := r.db.Pool.QueryRow(ctx, `
select exists(
  select 1
  from runtime_terminal_convergences c
  join task_queue_records q on q.queue_id = $2
  where c.convergence_id = $1 and c.queue_completed_at is not null and q.status = 'succeeded'
    and q.queue_name not in ('material_extract','material_minutes','material_summary','material_deposit','material_regenerate','material_write','material_recovery')
)`, convergenceID, proof.QueueID).Scan(&alreadyComplete)
	if err != nil {
		return nil, err
	}
	if alreadyComplete {
		return r.getQueueRecordPostgres(ctx, proof.QueueID)
	}
	tx, err := r.db.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	if _, err := r.activeQueueLeaseSnapshotTx(ctx, tx, proof, "succeeded"); err != nil {
		return nil, err
	}
	tag, err := tx.Exec(ctx, `
update task_queue_records
set status = 'succeeded', lease_owner = null, lease_token_hash = null,
    lease_expires_at = null, error_summary = '{}'::jsonb, updated_at = now()
where queue_id = $1 and status in ('leased','running')
  and queue_name not in ('material_extract','material_minutes','material_summary','material_deposit','material_regenerate','material_write','material_recovery')
  and lease_owner = $2 and attempt = $3 and lease_token_hash = $4
  and lease_fencing_token = $5 and lease_expires_at = $6 and lease_expires_at > now()`,
		proof.QueueID, proof.WorkerID, proof.Attempt, proof.TokenHash, proof.FencingToken, proof.LeaseExpiresAt)
	if err != nil {
		return nil, err
	}
	if tag.RowsAffected() != 1 {
		return nil, ErrStaleQueueLease
	}
	tag, err = tx.Exec(ctx, `
update runtime_terminal_convergences
set queue_completed_at = coalesce(queue_completed_at, now()), last_error_code = null,
    attempt_count = attempt_count + 1, updated_at = now()
where convergence_id = $1`, convergenceID)
	if err != nil {
		return nil, err
	}
	if tag.RowsAffected() != 1 {
		return nil, fmt.Errorf("terminal convergence not found")
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return r.getQueueRecordPostgres(ctx, proof.QueueID)
}

func (r *QueueRepository) blockRuntimeTerminalRecoveryLegacyContractPostgres(ctx context.Context, proof QueueLeaseProof, convergenceID string) (QueueRecord, error) {
	tx, err := r.db.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	var lockedConvergenceID string
	err = tx.QueryRow(ctx, `
select convergence.convergence_id
from runtime_terminal_convergences convergence
where convergence.convergence_id=$1
  and convergence.queue_completed_at is null
  and (
    convergence.queue_id=$2
    or exists(
      select 1
      from runtime_terminal_convergence_recovery_queue_lineage lineage
      where lineage.convergence_id=convergence.convergence_id
        and lineage.recovery_queue_id=$2
    )
  )
for update`, convergenceID, proof.QueueID).Scan(&lockedConvergenceID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrRuntimeTerminalRecoveryQueueInvalid
	}
	if err != nil {
		return nil, err
	}
	snapshot, err := r.activeQueueLeaseSnapshotTx(ctx, tx, proof, "dead_letter")
	if err != nil {
		return nil, err
	}
	rawError, err := json.Marshal(map[string]any{
		"errorCode":   RuntimeTerminalLegacyContractBlocked,
		"retryable":   false,
		"attempt":     proof.Attempt,
		"maxAttempts": snapshot.maxAttempts,
	})
	if err != nil {
		return nil, err
	}
	tag, err := tx.Exec(ctx, `
update task_queue_records
set status='dead_letter',lease_owner=null,lease_token_hash=null,lease_expires_at=null,
    dead_lettered_at=coalesce(dead_lettered_at,now()),
    dead_letter_reason=$7,
    error_summary=coalesce(error_summary,'{}'::jsonb) || $8::jsonb,
    updated_at=now()
where queue_id=$1
  and queue_name='runtime_events'
  and task_type='runtime_event_ingest'
  and status in ('leased','running')
  and lease_owner=$2
  and attempt=$3
  and lease_token_hash=$4
  and lease_fencing_token=$5
  and lease_expires_at=$6
  and lease_expires_at>now()`, proof.QueueID, proof.WorkerID, proof.Attempt, proof.TokenHash, proof.FencingToken, proof.LeaseExpiresAt, RuntimeTerminalLegacyContractBlocked, string(rawError))
	if err != nil {
		return nil, err
	}
	if tag.RowsAffected() != 1 {
		return nil, ErrStaleQueueLease
	}
	if err := r.insertQueueDeadLetterAuditTx(ctx, tx, proof.QueueID, snapshot.attemptSeriesID, proof.Attempt, proof.WorkerID, proof.TokenHash, proof.FencingToken, RuntimeTerminalLegacyContractBlocked, rawError, snapshot.payload); err != nil {
		return nil, err
	}
	tag, err = tx.Exec(ctx, `
update runtime_terminal_convergences
set last_error_code=$2,attempt_count=attempt_count+1,updated_at=now()
where convergence_id=$1
  and queue_completed_at is null`, convergenceID, RuntimeTerminalLegacyContractBlocked)
	if err != nil {
		return nil, err
	}
	if tag.RowsAffected() != 1 {
		return nil, ErrRuntimeTerminalConvergenceNotFound
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return r.getQueueRecordPostgres(ctx, proof.QueueID)
}

func (r *QueueRepository) assertRuntimeTerminalConvergenceQueueLinked(ctx context.Context, convergenceID, queueID string) error {
	var linked bool
	err := r.db.Pool.QueryRow(ctx, `
select exists(
  select 1
  from runtime_terminal_convergences convergence
  where convergence.convergence_id=$1
    and (
      convergence.queue_id=$2
      or exists(
        select 1
        from runtime_terminal_convergence_recovery_queue_lineage lineage
        where lineage.convergence_id=convergence.convergence_id
          and lineage.recovery_queue_id=$2
      )
    )
)`, convergenceID, queueID).Scan(&linked)
	if err != nil {
		return err
	}
	if !linked {
		return ErrRuntimeTerminalRecoveryQueueInvalid
	}
	return nil
}

func (r *QueueRepository) compensateRecordingQueueFailurePostgres(ctx context.Context, queueID, status string, rawError []byte) error {
	if r == nil || r.db == nil || r.db.Disabled || r.db.Pool == nil {
		return fmt.Errorf("queue postgres disabled")
	}
	tx, err := r.db.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err = r.compensateRecordingQueueFailureTx(ctx, tx, queueID, status, rawError); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (r *QueueRepository) compensateRecordingQueueFailureTx(ctx context.Context, tx pgx.Tx, queueID, status string, rawError []byte) error {
	if tx == nil {
		return fmt.Errorf("queue postgres transaction unavailable")
	}
	_, err := tx.Exec(ctx, `
with failed_queue as (
  select q.queue_id, q.task_id, q.task_type, q.payload,
         coalesce(
           nullif(q.payload->>'recordingId', ''),
           st.recording_id,
           nullif(split_part(q.queue_id, ':', 3), '')
         ) as recording_id,
         coalesce(nullif(q.payload->>'traceId', ''), '') as trace_id,
         coalesce(st.recording_id, '') as subtask_recording_id
  from task_queue_records q
  left join recording_sub_tasks st on st.recording_sub_task_id = q.task_id
  where q.queue_id = $1
    and (q.queue_name = 'recording' or q.queue_id like 'recording:%' or q.task_type in ('minutes_generation','summary_generation','recording_deposit','workspace_write'))
    and q.task_type in ('recording_final_transcript','final_transcript_generation','minutes_generation','summary_generation','recording_deposit','workspace_write')
),
updated_subtask as (
  update recording_sub_tasks st
  set status = $2,
      payload = st.payload || jsonb_build_object('errorSummary', $3::jsonb),
      updated_at = now()
  from failed_queue fq
  where (
      st.recording_sub_task_id = fq.task_id
      or (st.recording_id = fq.recording_id and st.task_type = fq.task_type)
    )
    and st.status <> 'succeeded'
  returning st.recording_id, st.task_type
),
affected as (
  select recording_id, task_type from updated_subtask
  union
  select recording_id, task_type from failed_queue where recording_id <> ''
),
updated_recording as (
  update recording_assets ra
  set transcript_status = case
        when affected.task_type in ('recording_final_transcript','final_transcript_generation') then $2
        else transcript_status
      end,
      minutes_status = case
        when affected.task_type = 'minutes_generation' then $2
        else minutes_status
      end,
      summary_status = case
        when affected.task_type = 'summary_generation' then $2
        else summary_status
      end,
      deposit_status = case
        when affected.task_type in ('recording_deposit','workspace_write') and ra.deposit_status <> 'deposited' then $2
        else deposit_status
      end,
      asset_version = asset_version + 1,
      updated_at = now()
  from affected
  where ra.recording_id = affected.recording_id
  returning ra.recording_id, ra.user_id, affected.task_type
)
insert into task_status_events(task_status_event_id, target_type, target_id, user_id, status, stage, trace_id, error_summary)
select 'task_status_event_queue_failure_' || md5($1 || updated_recording.recording_id || updated_recording.task_type || now()::text),
       'recording',
       updated_recording.recording_id,
       updated_recording.user_id,
       $2,
       updated_recording.task_type,
       coalesce((select trace_id from failed_queue limit 1), ''),
       $3::jsonb
from updated_recording
on conflict (task_status_event_id) do nothing`, queueID, status, string(rawError))
	return err
}

func aiRuntimeQueueNames() []string {
	return []string{
		queuecontract.QueueAIRuntime,
		queuecontract.QueueAIRuntimeInteractive,
		queuecontract.QueueAIRuntimeRecording,
		queuecontract.QueueAIRuntimeBackground,
	}
}

func isAIRuntimeQueueName(queueName string) bool {
	for _, candidate := range aiRuntimeQueueNames() {
		if queueName == candidate {
			return true
		}
	}
	return false
}

func (r *QueueRepository) compensateAIRuntimeQueueFailureTx(ctx context.Context, tx pgx.Tx, queueID, queueStatus string, rawError []byte) error {
	if tx == nil {
		return fmt.Errorf("queue postgres transaction unavailable")
	}
	var queueName, queueTaskType, queueTaskID string
	if err := tx.QueryRow(ctx, `
select queue_name, task_type, task_id
from task_queue_records
where queue_id = $1`, queueID).Scan(&queueName, &queueTaskType, &queueTaskID); err != nil {
		return err
	}
	if !isAIRuntimeQueueName(queueName) {
		return nil
	}
	if queueTaskID == "" {
		return fmt.Errorf("ai runtime queue missing task id")
	}
	agentRunID := ""
	productTaskID := queueTaskID
	if queueTaskType == "runtime_dispatch" {
		agentRunID = queueTaskID
		if err := tx.QueryRow(ctx, `select coalesce(task_id, '') from agent_runs where agent_run_id = $1`, agentRunID).Scan(&productTaskID); err != nil {
			return fmt.Errorf("ai runtime agent run missing: %s: %w", agentRunID, err)
		}
	}
	if productTaskID != "" {
		var taskExists bool
		if err := tx.QueryRow(ctx, `select exists(select 1 from ai_tasks where task_id = $1)`, productTaskID).Scan(&taskExists); err != nil {
			return err
		}
		if !taskExists {
			return fmt.Errorf("ai runtime queue task missing: %s", productTaskID)
		}
	}
	taskStatus := queueStatus
	if taskStatus == "dead_letter" {
		taskStatus = "failed"
	}
	if taskStatus != "timeout" {
		taskStatus = "failed"
	}
	eventType := taskStatus
	title := "生成失败"
	if taskStatus == "timeout" {
		title = "任务超时"
	}
	_, err := tx.Exec(ctx, `
with failed_queue as (
  select q.queue_id,
         $7::text as agent_run_id,
         $8::text as task_id,
         q.task_type,
         q.payload,
         q.error_summary,
         coalesce(nullif(q.payload->>'traceId', ''), nullif(q.error_summary->>'traceId', ''), '') as trace_id
  from task_queue_records q
  where q.queue_id = $1 and q.queue_name = any($9::text[])
),
normalized as (
  select fq.queue_id,
         fq.agent_run_id,
         fq.task_id,
         fq.task_type,
         fq.payload,
         fq.trace_id,
         coalesce(nullif(fq.error_summary->>'errorCode', ''), nullif($5::jsonb->>'errorCode', ''), 'RUNTIME_RUN_STALLED') as error_code,
         coalesce(fq.error_summary, '{}'::jsonb) || $5::jsonb || jsonb_build_object(
           'queueStatus', $2::text,
           'taskStatus', $3::text,
           'stage', 'queue_terminal',
           'retryable', false
         ) as error_summary
  from failed_queue fq
),
latest_run as (
  select rr.run_id, rr.task_id
  from runtime_run_records rr
  join normalized n on n.task_id = rr.task_id
  where rr.status not in ('succeeded', 'failed', 'timeout', 'forbidden')
  order by rr.updated_at desc
  limit 1
),
updated_agent_run as (
  update agent_runs ar
  set status = $3::text,
      error_summary = n.error_summary || jsonb_build_object(
        'code', n.error_code,
        'errorCode', n.error_code
      ),
      updated_at = now()
  from normalized n
  where n.agent_run_id <> ''
    and ar.agent_run_id = n.agent_run_id
    and ar.status not in ('succeeded', 'failed', 'cancelled', 'timeout')
  returning ar.agent_run_id, ar.task_id
),
updated_task as (
  update ai_tasks t
  set status = $3::text,
      error_summary = n.error_summary || jsonb_build_object('errorCode', n.error_code),
      result_snapshot = t.result_snapshot || jsonb_build_object(
        'stage', 'queue_terminal',
        'queueId', n.queue_id,
        'queueStatus', $2::text
      ),
      updated_at = now()
  from normalized n
  where n.task_id <> ''
    and t.task_id = n.task_id
    and t.status not in ('succeeded', 'failed', 'timeout', 'forbidden', 'quota_insufficient', 'ignored')
  returning t.task_id, t.user_id, coalesce(t.thread_id, '') as thread_id, n.queue_id, n.trace_id, n.error_code, n.error_summary
),
task_row as (
  select t.task_id, t.user_id, coalesce(t.thread_id, '') as thread_id, n.queue_id, n.trace_id, n.error_code, n.error_summary
  from ai_tasks t
  join normalized n on n.task_id = t.task_id
  where n.task_id <> ''
),
updated_run as (
  update runtime_run_records rr
  set status = $3::text,
      result_snapshot = rr.result_snapshot || jsonb_build_object(
        'stage', 'queue_terminal',
        'queueId', (select queue_id from normalized limit 1),
        'queueStatus', $2::text
      ),
      error_summary = (select error_summary || jsonb_build_object('errorCode', error_code) from normalized limit 1),
      updated_at = now()
  from latest_run lr
  where rr.run_id = lr.run_id
  returning rr.run_id, rr.task_id
),
status_event as (
  insert into task_status_events(task_status_event_id, target_type, target_id, user_id, status, stage, trace_id, error_summary)
  select 'task_status_event_ai_runtime_terminal_' || md5(tr.queue_id || ':' || tr.task_id || ':' || $3::text),
         'ai_task',
         tr.task_id,
         tr.user_id,
         $3::text,
         'queue_terminal',
         nullif(tr.trace_id, ''),
         tr.error_summary || jsonb_build_object('errorCode', tr.error_code)
  from task_row tr
  on conflict (task_status_event_id) do nothing
  returning task_status_event_id
),
progress_event as (
  insert into ai_task_events(event_id, task_id, thread_id, run_id, message_id, event_type, visibility, title, summary, delta_text, redaction_version)
  select 'agent_event_ai_runtime_terminal_' || md5(tr.task_id || ':' || $4),
         tr.task_id,
         nullif(tr.thread_id, ''),
         nullif((select run_id from updated_run limit 1), ''),
         null,
         $4::text,
         'app_safe',
         $6::text,
         'code=' || tr.error_code || ' stage=queue_terminal retryable=false',
         null,
         'app_safe_v1'
  from task_row tr
  where not exists (
    select 1 from ai_task_events existing
    where existing.task_id = tr.task_id and existing.event_type = $4::text
  )
  on conflict (event_id) do nothing
  returning event_id
)
select 1`, queueID, queueStatus, taskStatus, eventType, string(rawError), title, agentRunID, productTaskID, aiRuntimeQueueNames())
	return err
}

func (r *QueueRepository) updateQueueErrorSummaryPostgres(ctx context.Context, proof QueueLeaseProof, errorSummary map[string]any) (QueueRecord, error) {
	if r == nil || r.db == nil || r.db.Disabled || r.db.Pool == nil {
		return nil, fmt.Errorf("queue postgres disabled")
	}
	rawError, err := json.Marshal(mapValue(errorSummary))
	if err != nil {
		return nil, err
	}
	tag, err := r.db.Pool.Exec(ctx, `
update task_queue_records
set error_summary = $2::jsonb,
    updated_at = now()
where queue_id = $1
  and queue_name not in ('material_extract','material_minutes','material_summary','material_deposit','material_regenerate','material_write','material_recovery')
  and status in ('leased', 'running')
  and lease_owner = $3
  and attempt = $4
  and lease_token_hash = $5
  and lease_fencing_token = $6
  and lease_expires_at = $7
  and lease_expires_at > now()`, proof.QueueID, string(rawError), proof.WorkerID, proof.Attempt, proof.TokenHash, proof.FencingToken, proof.LeaseExpiresAt)
	if err != nil {
		return nil, err
	}
	if tag.RowsAffected() != 1 {
		return nil, ErrStaleQueueLease
	}
	return r.getQueueRecordPostgres(ctx, proof.QueueID)
}

func (r *QueueRepository) adminQueueActionPostgres(ctx context.Context, queueID, status, reason, operatorID string) (map[string]any, error) {
	if r == nil || r.db == nil || r.db.Disabled || r.db.Pool == nil {
		return nil, fmt.Errorf("queue postgres disabled")
	}
	errorSummary := map[string]any{"reason": reason, "operatorId": operatorID, "adminAction": status}
	rawError, err := json.Marshal(errorSummary)
	if err != nil {
		return nil, err
	}
	tag, err := r.db.Pool.Exec(ctx, `
update task_queue_records
set status = $2,
    lease_owner = null,
    lease_token_hash = null,
    lease_expires_at = null,
    error_summary = error_summary || $3::jsonb,
    updated_at = now()
where queue_id = $1
  and queue_name not in ('material_extract','material_minutes','material_summary','material_deposit','material_regenerate','material_write','material_recovery')`, queueID, status, string(rawError))
	if err != nil {
		return nil, err
	}
	if tag.RowsAffected() == 0 {
		record, getErr := r.getQueueRecordPostgres(ctx, queueID)
		if getErr != nil {
			return nil, getErr
		}
		if isMaterialQueueName(stringOr(record["queueName"], "")) {
			return nil, ErrMaterialQueueProjectionOwned
		}
		return record, nil
	}
	return r.getQueueRecordPostgres(ctx, queueID)
}

func (r *QueueRepository) recoverExpiredLeasesPostgres(ctx context.Context, now time.Time) (int, error) {
	if !r.queuePostgresReady() {
		return 0, fmt.Errorf("queue postgres disabled")
	}
	tx, err := r.db.Pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx, `
update task_queue_records
set status = 'retry_wait',
    lease_owner = null,
    lease_token_hash = null,
    lease_expires_at = null,
    available_at = least(available_at, $1),
    error_summary = coalesce(error_summary, '{}'::jsonb) || jsonb_build_object(
      'recoveryReason', 'expired_lease',
      'retryable', true
    ),
    updated_at = now()
where status = 'leased'
  and lease_expires_at is not null
  and lease_expires_at <= $1
  and attempt < max_attempts
  and queue_name not in ('material_extract','material_minutes','material_summary','material_deposit','material_regenerate','material_write','material_recovery')`, now)
	if err != nil {
		return 0, err
	}
	exhausted, err := r.deadLetterExpiredQueueBatchTx(ctx, tx, now, "", nil, "LEASE_EXHAUSTED")
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()) + exhausted, nil
}

func (r *QueueRepository) recoverRunningWithoutHeartbeatPostgres(ctx context.Context, now time.Time) (int, error) {
	if !r.queuePostgresReady() {
		return 0, fmt.Errorf("queue postgres disabled")
	}
	tx, err := r.db.Pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, `
with candidates as (
  select queue_id, attempt, coalesce(lease_owner, '') as lease_owner,
         coalesce(lease_token_hash, '') as lease_token_hash,
         lease_fencing_token, lease_expires_at
  from task_queue_records
  where status = 'running'
    and lease_expires_at is not null
    and lease_expires_at <= $1
    and queue_name not in ('material_extract','material_minutes','material_summary','material_deposit','material_regenerate','material_write','material_recovery')
  order by priority desc, updated_at asc
  limit 100
  for update skip locked
), timed_out as (
  update task_queue_records q
  set status = 'timeout',
      error_summary = coalesce(q.error_summary, '{}'::jsonb) || jsonb_build_object('errorCode', 'RUNTIME_RUN_STALLED', 'retryable', false, 'recoveryReason', 'running_without_heartbeat'),
      updated_at = now()
  from candidates c
  where q.queue_id = c.queue_id
    and q.status = 'running'
    and q.attempt = c.attempt
    and coalesce(q.lease_owner, '') = c.lease_owner
    and coalesce(q.lease_token_hash, '') = c.lease_token_hash
    and q.lease_fencing_token = c.lease_fencing_token
    and q.lease_expires_at = c.lease_expires_at
    and q.lease_expires_at <= $1
  returning q.queue_id, q.error_summary
)
select queue_id, error_summary from timed_out`, now)
	if err != nil {
		return 0, err
	}
	type timedOut struct {
		queueID string
		raw     []byte
	}
	items := []timedOut{}
	for rows.Next() {
		var item timedOut
		if err := rows.Scan(&item.queueID, &item.raw); err != nil {
			rows.Close()
			return 0, err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, err
	}
	rows.Close()
	for _, item := range items {
		if err := r.compensateRecordingQueueFailureTx(ctx, tx, item.queueID, "timeout", item.raw); err != nil {
			return 0, err
		}
		if err := r.compensateAIRuntimeQueueFailureTx(ctx, tx, item.queueID, "timeout", item.raw); err != nil {
			return 0, err
		}
		if _, err := tx.Exec(ctx, `update task_queue_records set lease_owner = null, lease_token_hash = null, lease_expires_at = null where queue_id = $1 and status = 'timeout'`, item.queueID); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return len(items), nil
}

func (r *QueueRepository) requeueRecoverableQueuePostgres(ctx context.Context, queueName, errorCode, reason string) (int, error) {
	if !r.queuePostgresReady() {
		return 0, fmt.Errorf("queue postgres disabled")
	}
	tag, err := r.db.Pool.Exec(ctx, `
update task_queue_records
set status = 'retry_wait',
    lease_owner = null,
    lease_token_hash = null,
    lease_expires_at = null,
    available_at = now(),
    error_summary = coalesce(error_summary, '{}'::jsonb) || jsonb_build_object('reason', $3::text, 'operatorId', 'recovery', 'adminAction', 'retry_wait', 'retryable', true),
    updated_at = now()
where queue_name = $1
  and queue_name not in ('material_extract','material_minutes','material_summary','material_deposit','material_regenerate','material_write','material_recovery')
  and status in ('failed','timeout')
  and attempt < max_attempts
  and ($2 = '' or error_summary->>'errorCode' = $2 or error_summary->>'code' = $2)
	`, queueName, errorCode, reason)
	if err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
}

func (r *QueueRepository) markDeadLetterCandidatesPostgres(ctx context.Context, now time.Time) (int, error) {
	if !r.queuePostgresReady() {
		return 0, fmt.Errorf("queue postgres disabled")
	}
	tx, err := r.db.Pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	count, err := r.deadLetterFailedQueueBatchTx(ctx, tx, now, "LEASE_EXHAUSTED")
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return count, nil
}

type queueDeadLetterSnapshot struct {
	queueID        string
	attemptSeries  string
	attempt        int
	leaseOwner     string
	leaseTokenHash string
	fencingToken   int64
	rawError       []byte
	rawPayload     []byte
}

func (r *QueueRepository) deadLetterExpiredQueueBatchTx(ctx context.Context, tx pgx.Tx, now time.Time, queueName string, taskTypes []string, reason string) (int, error) {
	rows, err := tx.Query(ctx, `
with candidates as (
  select queue_id
  from task_queue_records
  where status in ('leased', 'running')
    and lease_expires_at is not null
    and lease_expires_at <= $1
    and attempt >= max_attempts
    and ($2 = '' or queue_name = $2)
    and (cardinality($3::text[]) = 0 or task_type = any($3::text[]))
    and queue_name not in ('material_extract','material_minutes','material_summary','material_deposit','material_regenerate','material_write','material_recovery')
  order by updated_at asc
  limit 100
  for update skip locked
), exhausted as (
  update task_queue_records q
  set status = 'dead_letter',
      error_summary = coalesce(q.error_summary, '{}'::jsonb) || jsonb_build_object('errorCode', $4::text, 'retryable', false, 'attempt', q.attempt, 'maxAttempts', q.max_attempts),
      dead_lettered_at = now(),
      dead_letter_reason = $4,
      updated_at = now()
  from candidates c
  where q.queue_id = c.queue_id
  returning q.queue_id, coalesce(q.attempt_series_id, '') as attempt_series_id, q.attempt, coalesce(q.lease_owner, '') as lease_owner, coalesce(q.lease_token_hash, '') as lease_token_hash, q.lease_fencing_token, q.error_summary, q.payload
)
select queue_id, attempt_series_id, attempt, lease_owner, lease_token_hash, lease_fencing_token, error_summary, payload from exhausted`, now, queueName, taskTypes, reason)
	if err != nil {
		return 0, err
	}
	items, err := scanQueueDeadLetterSnapshots(rows)
	rows.Close()
	if err != nil {
		return 0, err
	}
	for _, item := range items {
		if err := r.finishQueueDeadLetterTx(ctx, tx, item, reason); err != nil {
			return 0, err
		}
	}
	return len(items), nil
}

func (r *QueueRepository) deadLetterFailedQueueBatchTx(ctx context.Context, tx pgx.Tx, now time.Time, reason string) (int, error) {
	rows, err := tx.Query(ctx, `
with candidates as (
  select queue_id
  from task_queue_records
  where status = 'failed' and attempt >= max_attempts and updated_at <= $1
    and queue_name not in ('material_extract','material_minutes','material_summary','material_deposit','material_regenerate','material_write','material_recovery')
  order by updated_at asc
  limit 100
  for update skip locked
), exhausted as (
  update task_queue_records q
  set status = 'dead_letter',
      error_summary = coalesce(q.error_summary, '{}'::jsonb) || jsonb_build_object('errorCode', $2::text, 'retryable', false, 'attempt', q.attempt, 'maxAttempts', q.max_attempts),
      dead_lettered_at = now(),
      dead_letter_reason = $2,
      updated_at = now()
  from candidates c
  where q.queue_id = c.queue_id
  returning q.queue_id, coalesce(q.attempt_series_id, '') as attempt_series_id, q.attempt, coalesce(q.lease_owner, '') as lease_owner, coalesce(q.lease_token_hash, '') as lease_token_hash, q.lease_fencing_token, q.error_summary, q.payload
)
select queue_id, attempt_series_id, attempt, lease_owner, lease_token_hash, lease_fencing_token, error_summary, payload from exhausted`, now, reason)
	if err != nil {
		return 0, err
	}
	items, err := scanQueueDeadLetterSnapshots(rows)
	rows.Close()
	if err != nil {
		return 0, err
	}
	for _, item := range items {
		if err := r.finishQueueDeadLetterTx(ctx, tx, item, reason); err != nil {
			return 0, err
		}
	}
	return len(items), nil
}

func scanQueueDeadLetterSnapshots(rows pgx.Rows) ([]queueDeadLetterSnapshot, error) {
	items := []queueDeadLetterSnapshot{}
	for rows.Next() {
		var item queueDeadLetterSnapshot
		if err := rows.Scan(&item.queueID, &item.attemptSeries, &item.attempt, &item.leaseOwner, &item.leaseTokenHash, &item.fencingToken, &item.rawError, &item.rawPayload); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (r *QueueRepository) finishQueueDeadLetterTx(ctx context.Context, tx pgx.Tx, item queueDeadLetterSnapshot, reason string) error {
	if err := r.compensateRecordingQueueFailureTx(ctx, tx, item.queueID, "failed", item.rawError); err != nil {
		return err
	}
	if err := r.compensateAIRuntimeQueueFailureTx(ctx, tx, item.queueID, "dead_letter", item.rawError); err != nil {
		return err
	}
	if err := r.insertQueueDeadLetterAuditTx(ctx, tx, item.queueID, item.attemptSeries, item.attempt, item.leaseOwner, item.leaseTokenHash, item.fencingToken, reason, item.rawError, item.rawPayload); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `update task_queue_records set lease_owner = null, lease_token_hash = null, lease_expires_at = null where queue_id = $1 and status = 'dead_letter'`, item.queueID)
	return err
}

func scanQueueIDs(rows pgx.Rows) ([]string, error) {
	out := []string{}
	for rows.Next() {
		var queueID string
		if err := rows.Scan(&queueID); err != nil {
			return nil, err
		}
		out = append(out, queueID)
	}
	return out, rows.Err()
}

func (r *QueueRepository) queueSummaryPostgres(ctx context.Context, filters map[string]any) (map[string]any, error) {
	if r == nil || r.db == nil || r.db.Disabled || r.db.Pool == nil {
		return nil, fmt.Errorf("queue postgres disabled")
	}
	rows, err := r.db.Pool.Query(ctx, `
select queue_name, status, count(*)
from task_queue_records
where ($1 = '' or queue_name = $1)
group by queue_name, status
order by queue_name, status`, stringOr(filters["queueName"], ""))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	grouped := map[string]map[string]any{}
	for rows.Next() {
		var queueName, status string
		var count int
		if err := rows.Scan(&queueName, &status, &count); err != nil {
			return nil, err
		}
		if _, ok := grouped[queueName]; !ok {
			grouped[queueName] = map[string]any{"queueName": queueName, "counts": map[string]int{}}
		}
		grouped[queueName]["counts"].(map[string]int)[status] = count
	}
	queues := []map[string]any{}
	for _, item := range grouped {
		counts := item["counts"].(map[string]int)
		backlog := counts["pending"]
		running := counts["leased"] + counts["running"]
		dead := counts["dead_letter"] + counts["ignored"]
		item["backlog"] = backlog
		item["running"] = running
		item["deadLetter"] = dead
		queues = append(queues, item)
	}
	return map[string]any{"queues": queues, "status": "rds_backed", "activeBackend": "rds_durable"}, rows.Err()
}

func (r *QueueRepository) listQueueRecordsPostgres(ctx context.Context, filters map[string]any) ([]map[string]any, error) {
	if r == nil || r.db == nil || r.db.Disabled || r.db.Pool == nil {
		return nil, fmt.Errorf("queue postgres disabled")
	}
	rows, err := r.db.Pool.Query(ctx, `
select queue_id, queue_name, task_type, task_id, coalesce(dedupe_key, ''), status, priority, attempt, max_attempts,
       coalesce(lease_owner, ''), available_at, coalesce(lease_expires_at, '0001-01-01'::timestamptz), lease_expires_at is not null,
       coalesce(lease_token_hash, ''), lease_fencing_token,
       coalesce(heartbeat_at, '0001-01-01'::timestamptz), heartbeat_at is not null,
       coalesce(attempt_series_id, ''), coalesce(dead_lettered_at, '0001-01-01'::timestamptz), dead_lettered_at is not null,
       coalesce(dead_letter_reason, ''), coalesce(replayed_from_queue_id, ''), coalesce(replayed_by, ''), coalesce(replay_reason, ''),
       payload, error_summary, created_at, updated_at
from task_queue_records
where ($1 = '' or queue_name = $1)
  and ($2 = '' or status = $2)
  and ($3 = '' or task_id = $3)
  and ($4 = '' or queue_id = $4)
order by updated_at desc
limit 100`, stringOr(filters["queueName"], ""), stringOr(filters["status"], ""), stringOr(filters["taskId"], ""), stringOr(filters["queueId"], ""))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []map[string]any{}
	for rows.Next() {
		record, err := scanQueueRecord(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, record)
	}
	return out, rows.Err()
}

func (r *QueueRepository) getQueueRecordPostgres(ctx context.Context, queueID string) (map[string]any, error) {
	row := r.db.Pool.QueryRow(ctx, `
select queue_id, queue_name, task_type, task_id, coalesce(dedupe_key, ''), status, priority, attempt, max_attempts,
       coalesce(lease_owner, ''), available_at, coalesce(lease_expires_at, '0001-01-01'::timestamptz), lease_expires_at is not null,
       coalesce(lease_token_hash, ''), lease_fencing_token,
       coalesce(heartbeat_at, '0001-01-01'::timestamptz), heartbeat_at is not null,
       coalesce(attempt_series_id, ''), coalesce(dead_lettered_at, '0001-01-01'::timestamptz), dead_lettered_at is not null,
       coalesce(dead_letter_reason, ''), coalesce(replayed_from_queue_id, ''), coalesce(replayed_by, ''), coalesce(replay_reason, ''),
       payload, error_summary, created_at, updated_at
from task_queue_records
where queue_id = $1`, queueID)
	return scanQueueRecord(row)
}

func getQueueRecordInTx(ctx context.Context, tx *Tx, queueID string) (map[string]any, error) {
	if tx == nil || tx.tx == nil {
		return nil, fmt.Errorf("queue transaction disabled")
	}
	row := tx.QueryRowRaw(ctx, `
select queue_id, queue_name, task_type, task_id, coalesce(dedupe_key, ''), status, priority, attempt, max_attempts,
       coalesce(lease_owner, ''), available_at, coalesce(lease_expires_at, '0001-01-01'::timestamptz), lease_expires_at is not null,
       coalesce(lease_token_hash, ''), lease_fencing_token,
       coalesce(heartbeat_at, '0001-01-01'::timestamptz), heartbeat_at is not null,
       coalesce(attempt_series_id, ''), coalesce(dead_lettered_at, '0001-01-01'::timestamptz), dead_lettered_at is not null,
       coalesce(dead_letter_reason, ''), coalesce(replayed_from_queue_id, ''), coalesce(replayed_by, ''), coalesce(replay_reason, ''),
       payload, error_summary, created_at, updated_at
from task_queue_records
where queue_id = $1`, queueID)
	return scanQueueRecord(row)
}

func (r *QueueRepository) listRecoveryRunsPostgres(ctx context.Context, filters map[string]any) ([]map[string]any, error) {
	if r == nil || r.db == nil || r.db.Disabled || r.db.Pool == nil {
		return nil, fmt.Errorf("recovery postgres disabled")
	}
	rows, err := r.db.Pool.Query(ctx, `
select run_id, job_type, coalesce(business_date::text, ''), status, payload, result, created_at, updated_at
from system_job_runs
where ($1 = '' or job_type = $1)
  and ($2 = '' or status = $2)
order by updated_at desc
limit 100`, stringOr(filters["jobType"], ""), stringOr(filters["status"], ""))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []map[string]any{}
	for rows.Next() {
		run, err := scanSystemJobRun(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, run)
	}
	return out, rows.Err()
}

func (r *QueueRepository) getRecoveryRunPostgres(ctx context.Context, runID string) (map[string]any, error) {
	if !r.queuePostgresReady() {
		return nil, fmt.Errorf("recovery postgres disabled")
	}
	row := r.db.Pool.QueryRow(ctx, `
select run_id, job_type, coalesce(business_date::text, ''), status, payload, result, created_at, updated_at
from system_job_runs
where run_id = $1`, runID)
	return scanSystemJobRun(row)
}

func (r *QueueRepository) recordRecoveryRunPostgres(ctx context.Context, runID, jobType, businessDate, status string, payload, result map[string]any) (map[string]any, error) {
	if !r.queuePostgresReady() {
		return nil, fmt.Errorf("recovery postgres disabled")
	}
	if runID == "" {
		runID = "system_job_" + fmt.Sprint(time.Now().UTC().UnixNano())
	}
	if jobType == "" {
		jobType = "recovery"
	}
	if status == "" {
		status = "running"
	}
	payloadRaw, err := json.Marshal(mapValue(payload))
	if err != nil {
		return nil, err
	}
	resultRaw, err := json.Marshal(mapValue(result))
	if err != nil {
		return nil, err
	}
	_, err = r.db.Pool.Exec(ctx, `
insert into system_job_runs(run_id, job_type, business_date, status, payload, result)
values ($1, $2, nullif($3, '')::date, $4, $5::jsonb, $6::jsonb)
on conflict (run_id) do update set status = excluded.status, payload = excluded.payload, result = excluded.result, updated_at = now()`,
		runID, jobType, businessDate, status, string(payloadRaw), string(resultRaw))
	if err != nil {
		return nil, err
	}
	return r.getRecoveryRunPostgres(ctx, runID)
}

func (r *QueueRepository) queuePostgresReady() bool {
	return r != nil && r.db != nil && !r.db.Disabled && r.db.Pool != nil
}

func (r *QueueRepository) queueMemoryAllowed() bool {
	return r != nil && (r.db == nil || r.db.Disabled)
}

func queueDurableFailureRecordFromCommand(command map[string]any, operation string, err error) map[string]any {
	queueName := stringOr(command["queueName"], "default")
	taskID := stringOr(command["taskId"], "")
	queueID := stringOr(command["queueId"], "")
	if queueID == "" && taskID != "" {
		queueID = queueName + ":" + taskID
	}
	if queueID == "" {
		queueID = queueName
	}
	record := queueDurableFailureRecord(queueID, operation, err)
	record["queueName"] = queueName
	record["taskType"] = stringOr(command["taskType"], "unknown")
	record["taskId"] = taskID
	record["dedupeKey"] = stringOr(command["dedupeKey"], "")
	return record
}

func queueDurableFailureRecord(queueID, operation string, err error) map[string]any {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	return map[string]any{
		"queueId":       queueID,
		"runId":         queueID,
		"status":        "failed",
		"activeBackend": "rds_durable",
		"storage":       "rds_durable",
		"errorCode":     "QUEUE_DURABLE_BACKEND_UNAVAILABLE",
		"errorSummary":  queueDurableFailureSummaryPayload(operation, err),
		"createdAt":     now,
		"updatedAt":     now,
	}
}

func queueDurableFailureSummary(operation string, err error) map[string]any {
	return map[string]any{
		"queues":        []map[string]any{},
		"status":        "degraded",
		"activeBackend": "rds_durable",
		"ok":            false,
		"errorCode":     "QUEUE_DURABLE_BACKEND_UNAVAILABLE",
		"errorSummary":  queueDurableFailureSummaryPayload(operation, err),
	}
}

func queueDurableFailureSummaryPayload(operation string, err error) map[string]any {
	message := ""
	if err != nil {
		message = err.Error()
		if len(message) > 240 {
			message = message[:240]
		}
	}
	return map[string]any{
		"errorCode": "QUEUE_DURABLE_BACKEND_UNAVAILABLE",
		"retryable": true,
		"operation": operation,
		"detail":    message,
	}
}

func (r *QueueRepository) enqueueMemory(command map[string]any) map[string]any {
	r.mu.Lock()
	defer r.mu.Unlock()
	queueName := stringOr(command["queueName"], "default")
	taskID := stringOr(command["taskId"], "")
	if taskID == "" {
		taskID = "task_" + fmt.Sprint(time.Now().UTC().UnixNano())
	}
	queueID := stringOr(command["queueId"], queueName+":"+taskID)
	if existing, ok := r.records[queueID]; ok {
		return copyMap(existing)
	}
	attemptSeriesID, err := newQueueAttemptSeriesID()
	if err != nil {
		return queueDurableFailureRecord(queueID, "enqueue", err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	record := map[string]any{
		"queueId":           queueID,
		"queueName":         queueName,
		"taskType":          stringOr(command["taskType"], "unknown"),
		"taskId":            taskID,
		"dedupeKey":         stringOr(command["dedupeKey"], ""),
		"status":            "pending",
		"priority":          defaultInt(command["priority"], 100),
		"attempt":           0,
		"maxAttempts":       defaultInt(command["maxAttempts"], 3),
		"attemptSeriesId":   attemptSeriesID,
		"leaseFencingToken": int64(0),
		"availableAt":       timeValue(command["availableAt"], time.Now().UTC()).UTC().Format(time.RFC3339),
		"payload":           mapValue(command["payload"]),
		"errorSummary":      map[string]any{},
		"createdAt":         now,
		"updatedAt":         now,
		"storage":           "memory",
	}
	if len(record["payload"].(map[string]any)) == 0 {
		record["payload"] = copyMap(command)
	}
	r.records[queueID] = record
	return copyMap(record)
}

func (r *QueueRepository) recoverExpiredLeasesMemory(now time.Time) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	recovered := 0
	for _, record := range r.records {
		if isMaterialQueueName(stringOr(record["queueName"], "")) {
			continue
		}
		status := stringOr(record["status"], "")
		if status != "leased" && status != "running" {
			continue
		}
		leaseExpiresAt := timeValue(record["leaseExpiresAt"], time.Time{})
		if leaseExpiresAt.IsZero() || leaseExpiresAt.After(now) {
			continue
		}
		if status == "running" && defaultInt(record["attempt"], 0) < defaultInt(record["maxAttempts"], 3) {
			continue
		}
		if defaultInt(record["attempt"], 0) >= defaultInt(record["maxAttempts"], 3) {
			markExpiredLeaseDeadLetterMemory(record, now)
		} else {
			record["status"] = "retry_wait"
			delete(record, "leaseOwner")
			delete(record, "leaseTokenHash")
			delete(record, "leaseExpiresAt")
			errorSummary := mapValue(record["errorSummary"])
			errorSummary["recoveryReason"] = "expired_lease"
			errorSummary["retryable"] = true
			record["errorSummary"] = errorSummary
			record["updatedAt"] = now.Format(time.RFC3339Nano)
		}
		recovered++
	}
	return recovered
}

func (r *QueueRepository) recoverRunningWithoutHeartbeatMemory(now time.Time) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	recovered := 0
	for _, record := range r.records {
		if isMaterialQueueName(stringOr(record["queueName"], "")) {
			continue
		}
		if stringOr(record["status"], "") != "running" {
			continue
		}
		leaseExpiresAt := timeValue(record["leaseExpiresAt"], time.Time{})
		if leaseExpiresAt.IsZero() || leaseExpiresAt.After(now) {
			continue
		}
		record["status"] = "timeout"
		delete(record, "leaseOwner")
		delete(record, "leaseTokenHash")
		delete(record, "leaseExpiresAt")
		errorSummary := mapValue(record["errorSummary"])
		errorSummary["errorCode"] = "RUNTIME_RUN_STALLED"
		errorSummary["retryable"] = false
		errorSummary["recoveryReason"] = "running_without_heartbeat"
		record["errorSummary"] = errorSummary
		record["updatedAt"] = now.Format(time.RFC3339Nano)
		recovered++
	}
	return recovered
}

func (r *QueueRepository) requeueRecoverableQueueMemory(queueName, errorCode, reason string) int {
	if isMaterialQueueName(queueName) {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	recovered := 0
	now := time.Now().UTC()
	for _, record := range r.records {
		if stringOr(record["queueName"], "") != queueName {
			continue
		}
		status := stringOr(record["status"], "")
		if status != "failed" && status != "timeout" {
			continue
		}
		if defaultInt(record["attempt"], 0) >= defaultInt(record["maxAttempts"], 3) {
			continue
		}
		summary := mapValue(record["errorSummary"])
		if errorCode != "" && stringOr(summary["errorCode"], stringOr(summary["code"], "")) != errorCode {
			continue
		}
		record["status"] = "retry_wait"
		delete(record, "leaseOwner")
		delete(record, "leaseTokenHash")
		delete(record, "leaseExpiresAt")
		summary["reason"] = reason
		summary["operatorId"] = "recovery"
		summary["adminAction"] = "pending"
		record["errorSummary"] = summary
		record["updatedAt"] = now.Format(time.RFC3339Nano)
		recovered++
	}
	return recovered
}

func (r *QueueRepository) markDeadLetterCandidatesMemory(now time.Time) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	recovered := 0
	for _, record := range r.records {
		if isMaterialQueueName(stringOr(record["queueName"], "")) {
			continue
		}
		if stringOr(record["status"], "") != "failed" {
			continue
		}
		if defaultInt(record["attempt"], 0) < defaultInt(record["maxAttempts"], 3) {
			continue
		}
		markExpiredLeaseDeadLetterMemory(record, now)
		recovered++
	}
	return recovered
}

func (r *QueueRepository) leaseMemory(queueName, workerID, tokenHash string, leaseTTL time.Duration, taskTypes ...string) (QueueRecord, QueueLeaseProof, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	allowedTaskTypes := queueTaskTypeSet(taskTypes...)
	now := time.Now().UTC()
	var selected map[string]any
	for _, record := range r.records {
		if isMaterialQueueName(stringOr(record["queueName"], "")) {
			continue
		}
		if queueName != "" && stringOr(record["queueName"], "") != queueName {
			continue
		}
		if len(allowedTaskTypes) > 0 && !allowedTaskTypes[stringOr(record["taskType"], "")] {
			continue
		}
		status := stringOr(record["status"], "")
		leaseExpired := false
		if status == "leased" || status == "running" {
			leaseExpiresAt := timeValue(record["leaseExpiresAt"], time.Time{})
			expired := !leaseExpiresAt.IsZero() && !leaseExpiresAt.After(now)
			attempt := defaultInt(record["attempt"], 0)
			maxAttempts := defaultInt(record["maxAttempts"], 3)
			if expired && attempt >= maxAttempts {
				markExpiredLeaseDeadLetterMemory(record, now)
				continue
			}
			if status == "running" {
				continue
			}
			leaseExpired = expired
		}
		if status != "pending" && status != "retry_wait" && !leaseExpired {
			continue
		}
		if defaultInt(record["attempt"], 0) >= defaultInt(record["maxAttempts"], 3) {
			if leaseExpired {
				markExpiredLeaseDeadLetterMemory(record, now)
			}
			continue
		}
		availableAt := timeValue(record["availableAt"], now)
		if availableAt.After(now) {
			continue
		}
		if selected == nil || queueRecordSortsBefore(record, selected) {
			selected = record
		}
	}
	if selected == nil {
		return nil, QueueLeaseProof{}, ErrNoQueueWork
	}
	if leaseTTL <= 0 {
		leaseTTL = time.Minute
	}
	selected["status"] = "leased"
	selected["leaseOwner"] = workerID
	leaseExpiresAt := now.Add(leaseTTL)
	selected["leaseExpiresAt"] = leaseExpiresAt.Format(time.RFC3339Nano)
	selected["leaseTokenHash"] = tokenHash
	selected["leaseFencingToken"] = int64Value(selected["leaseFencingToken"]) + 1
	selected["heartbeatAt"] = now.Format(time.RFC3339Nano)
	selected["attempt"] = defaultInt(selected["attempt"], 0) + 1
	selected["updatedAt"] = now.Format(time.RFC3339Nano)
	proof := QueueLeaseProof{
		QueueID: stringOr(selected["queueId"], ""), WorkerID: workerID,
		Attempt: defaultInt(selected["attempt"], 0), TokenHash: tokenHash,
		FencingToken: int64Value(selected["leaseFencingToken"]), LeaseExpiresAt: leaseExpiresAt,
	}
	return copyMap(selected), proof, nil
}

func (r *QueueRepository) leaseByIDMemory(queueID, workerID, tokenHash string, leaseTTL time.Duration) (QueueRecord, QueueLeaseProof, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	record := r.records[queueID]
	if isMaterialQueueName(stringOr(record["queueName"], "")) {
		return nil, QueueLeaseProof{}, ErrMaterialQueueProjectionOwned
	}
	if record == nil || (stringOr(record["status"], "") != "pending" && stringOr(record["status"], "") != "retry_wait") ||
		defaultInt(record["attempt"], 0) >= defaultInt(record["maxAttempts"], 3) || timeValue(record["availableAt"], time.Now().UTC()).After(time.Now().UTC()) {
		return nil, QueueLeaseProof{}, ErrNoQueueWork
	}
	now := time.Now().UTC()
	leaseExpiresAt := now.Add(leaseTTL)
	record["status"] = "leased"
	record["leaseOwner"] = workerID
	record["leaseExpiresAt"] = leaseExpiresAt.Format(time.RFC3339Nano)
	record["leaseTokenHash"] = tokenHash
	record["leaseFencingToken"] = int64Value(record["leaseFencingToken"]) + 1
	record["heartbeatAt"] = now.Format(time.RFC3339Nano)
	record["attempt"] = defaultInt(record["attempt"], 0) + 1
	record["updatedAt"] = now.Format(time.RFC3339Nano)
	proof := QueueLeaseProof{QueueID: queueID, WorkerID: workerID, Attempt: defaultInt(record["attempt"], 0), TokenHash: tokenHash, FencingToken: int64Value(record["leaseFencingToken"]), LeaseExpiresAt: leaseExpiresAt}
	return copyMap(record), proof, nil
}

func normalizedQueueTaskTypes(taskTypes ...string) []string {
	seen := map[string]bool{}
	normalized := make([]string, 0, len(taskTypes))
	for _, taskType := range taskTypes {
		taskType = strings.TrimSpace(taskType)
		if taskType == "" || seen[taskType] {
			continue
		}
		seen[taskType] = true
		normalized = append(normalized, taskType)
	}
	return normalized
}

func queueTaskTypeSet(taskTypes ...string) map[string]bool {
	normalized := normalizedQueueTaskTypes(taskTypes...)
	if len(normalized) == 0 {
		return nil
	}
	set := make(map[string]bool, len(normalized))
	for _, taskType := range normalized {
		set[taskType] = true
	}
	return set
}

func queueRecordSortsBefore(candidate, current map[string]any) bool {
	candidatePriority := defaultInt(candidate["priority"], 0)
	currentPriority := defaultInt(current["priority"], 0)
	if candidatePriority != currentPriority {
		return candidatePriority > currentPriority
	}
	candidateCreatedAt := timeValue(candidate["createdAt"], time.Time{})
	currentCreatedAt := timeValue(current["createdAt"], time.Time{})
	if candidateCreatedAt.IsZero() || currentCreatedAt.IsZero() {
		return stringOr(candidate["queueId"], "") < stringOr(current["queueId"], "")
	}
	return candidateCreatedAt.Before(currentCreatedAt)
}

func markExpiredLeaseDeadLetterMemory(record map[string]any, now time.Time) {
	record["status"] = "dead_letter"
	delete(record, "leaseOwner")
	delete(record, "leaseTokenHash")
	delete(record, "leaseExpiresAt")
	errorSummary := mapValue(record["errorSummary"])
	errorCode := stringOr(errorSummary["errorCode"], "LEASE_EXHAUSTED")
	errorSummary["errorCode"] = errorCode
	errorSummary["retryable"] = false
	errorSummary["deadLetterReason"] = "lease_exhausted"
	errorSummary["attempt"] = defaultInt(record["attempt"], 0)
	errorSummary["maxAttempts"] = defaultInt(record["maxAttempts"], 3)
	record["errorSummary"] = errorSummary
	record["deadLetterReason"] = errorCode
	record["updatedAt"] = now.Format(time.RFC3339)
}

func (r *QueueRepository) heartbeatMemory(proof QueueLeaseProof, leaseTTL time.Duration) (QueueLeaseProof, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if leaseTTL <= 0 {
		leaseTTL = time.Minute
	}
	record := r.records[proof.QueueID]
	if isMaterialQueueName(stringOr(record["queueName"], "")) {
		return QueueLeaseProof{}, ErrMaterialQueueProjectionOwned
	}
	now := time.Now().UTC()
	if !queueMemoryProofMatches(record, proof, now, "leased", "running") {
		return QueueLeaseProof{}, ErrStaleQueueLease
	}
	proof.LeaseExpiresAt = now.Add(leaseTTL)
	record["leaseExpiresAt"] = proof.LeaseExpiresAt.Format(time.RFC3339Nano)
	record["heartbeatAt"] = now.Format(time.RFC3339Nano)
	record["updatedAt"] = now.Format(time.RFC3339Nano)
	return proof, nil
}

func (r *QueueRepository) updateQueueMemory(proof QueueLeaseProof, status, errorCode string, retryable bool, retryDelay time.Duration) (QueueRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	record := r.records[proof.QueueID]
	if isMaterialQueueName(stringOr(record["queueName"], "")) {
		return nil, ErrMaterialQueueProjectionOwned
	}
	now := time.Now().UTC()
	allowed := allowedQueueSourceStatuses(status)
	if !queueMemoryProofMatches(record, proof, now, allowed...) {
		return nil, ErrStaleQueueLease
	}
	if status == "" {
		return nil, fmt.Errorf("queue target status is required")
	}
	maxAttempts := defaultInt(record["maxAttempts"], 3)
	if status == "retry_wait" && proof.Attempt >= maxAttempts {
		status = "dead_letter"
		retryable = false
		if errorCode == "" {
			errorCode = "LEASE_EXHAUSTED"
		}
	}
	record["status"] = status
	if status != "running" {
		delete(record, "leaseOwner")
		delete(record, "leaseTokenHash")
		delete(record, "leaseExpiresAt")
	}
	if status == "retry_wait" {
		record["availableAt"] = now.Add(retryDelay).Format(time.RFC3339Nano)
	}
	if errorCode != "" {
		errorSummary := mapValue(record["errorSummary"])
		errorSummary["errorCode"] = errorCode
		errorSummary["retryable"] = retryable
		errorSummary["attempt"] = proof.Attempt
		errorSummary["maxAttempts"] = maxAttempts
		if status == "retry_wait" {
			errorSummary["nextAttempt"] = proof.Attempt + 1
			errorSummary["retryDelaySeconds"] = int(retryDelay.Seconds())
		}
		if status == "dead_letter" {
			errorSummary["deadLetterReason"] = "lease_exhausted"
		}
		record["errorSummary"] = errorSummary
	} else if status == "succeeded" {
		record["errorSummary"] = map[string]any{}
	}
	if status == "dead_letter" {
		record["deadLetteredAt"] = now.Format(time.RFC3339Nano)
		record["deadLetterReason"] = errorCode
	}
	record["updatedAt"] = now.Format(time.RFC3339Nano)
	return copyMap(record), nil
}

func (r *QueueRepository) completeTerminalConvergenceMemory(proof QueueLeaseProof, convergenceID string) (QueueRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if queueID, ok := r.terminalCompletions[convergenceID]; ok {
		record := r.records[queueID]
		if queueID != proof.QueueID || record == nil || stringOr(record["status"], "") != "succeeded" {
			return nil, ErrStaleQueueLease
		}
		return copyMap(record), nil
	}
	record := r.records[proof.QueueID]
	if isMaterialQueueName(stringOr(record["queueName"], "")) {
		return nil, ErrMaterialQueueProjectionOwned
	}
	if !queueMemoryProofMatches(record, proof, time.Now().UTC(), "leased", "running") {
		return nil, ErrStaleQueueLease
	}
	record["status"] = "succeeded"
	record["errorSummary"] = map[string]any{}
	delete(record, "leaseOwner")
	delete(record, "leaseTokenHash")
	delete(record, "leaseExpiresAt")
	record["updatedAt"] = time.Now().UTC().Format(time.RFC3339Nano)
	r.terminalCompletions[convergenceID] = proof.QueueID
	return copyMap(record), nil
}

func (r *QueueRepository) updateQueueErrorSummaryMemory(proof QueueLeaseProof, errorSummary map[string]any) (QueueRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	record := r.records[proof.QueueID]
	if isMaterialQueueName(stringOr(record["queueName"], "")) {
		return nil, ErrMaterialQueueProjectionOwned
	}
	if !queueMemoryProofMatches(record, proof, time.Now().UTC(), "leased", "running") {
		return nil, ErrStaleQueueLease
	}
	record["errorSummary"] = mapValue(errorSummary)
	record["updatedAt"] = time.Now().UTC().Format(time.RFC3339Nano)
	return copyMap(record), nil
}

func (r *QueueRepository) adminUpdateQueueMemory(queueID, status, reason string) QueueRecord {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now().UTC()
	record := r.records[queueID]
	if record == nil {
		return map[string]any{"queueId": queueID, "status": "not_found"}
	}
	if isMaterialQueueName(stringOr(record["queueName"], "")) {
		return materialQueueProjectionRejectedRecord(queueID, "mark_ignored")
	}
	record["status"] = status
	delete(record, "leaseOwner")
	delete(record, "leaseTokenHash")
	delete(record, "leaseExpiresAt")
	errorSummary := mapValue(record["errorSummary"])
	errorSummary["reason"] = reason
	record["errorSummary"] = errorSummary
	record["updatedAt"] = now.Format(time.RFC3339Nano)
	return copyMap(record)
}

func queueMemoryProofMatches(record QueueRecord, proof QueueLeaseProof, now time.Time, allowedStatuses ...string) bool {
	if record == nil || stringOr(record["queueId"], "") != proof.QueueID || stringOr(record["leaseOwner"], "") != proof.WorkerID ||
		defaultInt(record["attempt"], 0) != proof.Attempt || stringOr(record["leaseTokenHash"], "") != proof.TokenHash ||
		int64Value(record["leaseFencingToken"]) != proof.FencingToken {
		return false
	}
	leaseExpiresAt := timeValue(record["leaseExpiresAt"], time.Time{})
	if leaseExpiresAt.IsZero() || !leaseExpiresAt.Equal(proof.LeaseExpiresAt) || !leaseExpiresAt.After(now) {
		return false
	}
	status := stringOr(record["status"], "")
	for _, allowed := range allowedStatuses {
		if status == allowed {
			return true
		}
	}
	return false
}

func (r *QueueRepository) replayDeadLetterMemory(command AdminReplayCommand) (QueueRecord, error) {
	command = normalizeAdminReplayCommand(command)
	r.mu.Lock()
	defer r.mu.Unlock()
	source := r.records[command.QueueID]
	if source == nil {
		return nil, ErrQueueReplayNotFound
	}
	rawError, _ := json.Marshal(mapValue(source["errorSummary"]))
	_, leaseExpiresAtSet := source["leaseExpiresAt"]
	_, deadLetteredAtSet := source["deadLetteredAt"]
	if err := validateAdminReplaySource(adminReplaySource{
		queueName:         stringOr(source["queueName"], ""),
		taskType:          stringOr(source["taskType"], ""),
		taskID:            stringOr(source["taskId"], ""),
		status:            stringOr(source["status"], ""),
		attempt:           defaultInt(source["attempt"], 0),
		maxAttempts:       defaultInt(source["maxAttempts"], 0),
		attemptSeriesID:   stringOr(source["attemptSeriesId"], ""),
		leaseOwner:        stringOr(source["leaseOwner"], ""),
		leaseExpiresAtSet: leaseExpiresAtSet,
		deadLetteredAtSet: deadLetteredAtSet,
		deadLetterReason:  stringOr(source["deadLetterReason"], ""),
		errorSummary:      rawError,
	}); err != nil {
		return nil, err
	}
	suffix := deterministicReplaySuffix(command)
	queueID := command.QueueID + ":replay:" + suffix
	if existing := r.records[queueID]; existing != nil {
		if stringOr(existing["replayedFromQueueId"], "") != command.QueueID ||
			stringOr(existing["replayedBy"], "") != command.OperatorID ||
			stringOr(existing["replayReason"], "") != command.Reason {
			return nil, fmt.Errorf("%w: replay lineage does not match the command", ErrQueueReplayConflict)
		}
		return copyReplayQueueRecord(existing), nil
	}
	seriesID, err := newQueueAttemptSeriesID()
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	// Replay owns a distinct payload snapshot. A shallow map copy would let a
	// caller mutate the original dead-letter evidence through the new lineage.
	replay := copyReplayQueueRecord(source)
	replay["queueId"] = queueID
	replay["dedupeKey"] = stringOr(source["dedupeKey"], command.QueueID) + ":replay:" + suffix
	replay["status"] = "pending"
	replay["attempt"] = 0
	replay["attemptSeriesId"] = seriesID
	replay["replayedFromQueueId"] = command.QueueID
	replay["replayedBy"] = command.OperatorID
	replay["replayReason"] = command.Reason
	replay["availableAt"] = now.Format(time.RFC3339Nano)
	replay["errorSummary"] = map[string]any{}
	replay["createdAt"] = now.Format(time.RFC3339Nano)
	replay["updatedAt"] = now.Format(time.RFC3339Nano)
	delete(replay, "leaseOwner")
	delete(replay, "leaseTokenHash")
	delete(replay, "leaseExpiresAt")
	delete(replay, "deadLetteredAt")
	delete(replay, "deadLetterReason")
	r.records[queueID] = replay
	return copyReplayQueueRecord(replay), nil
}

func copyReplayQueueRecord(record QueueRecord) QueueRecord {
	out := copyMap(record)
	for _, key := range []string{"payload", "errorSummary"} {
		if value, ok := record[key]; ok {
			out[key] = cloneReplayQueueValue(value)
		}
	}
	return out
}

func cloneReplayQueueValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, child := range typed {
			out[key] = cloneReplayQueueValue(child)
		}
		return out
	case []any:
		out := make([]any, len(typed))
		for index, child := range typed {
			out[index] = cloneReplayQueueValue(child)
		}
		return out
	case []map[string]any:
		out := make([]map[string]any, len(typed))
		for index, child := range typed {
			out[index] = cloneReplayQueueValue(child).(map[string]any)
		}
		return out
	case map[string]string:
		out := make(map[string]string, len(typed))
		for key, child := range typed {
			out[key] = child
		}
		return out
	case []string:
		return append([]string(nil), typed...)
	default:
		return value
	}
}

func (r *QueueRepository) queueSummaryMemory(filters map[string]any) map[string]any {
	records := r.listQueueRecordsMemory(filters)
	grouped := map[string]map[string]any{}
	for _, record := range records {
		queueName := stringOr(record["queueName"], "unknown")
		status := stringOr(record["status"], "unknown")
		if _, ok := grouped[queueName]; !ok {
			grouped[queueName] = map[string]any{"queueName": queueName, "counts": map[string]int{}}
		}
		grouped[queueName]["counts"].(map[string]int)[status]++
	}
	queues := []map[string]any{}
	for _, item := range grouped {
		counts := item["counts"].(map[string]int)
		item["backlog"] = counts["pending"]
		item["running"] = counts["leased"] + counts["running"]
		item["deadLetter"] = counts["dead_letter"] + counts["ignored"]
		queues = append(queues, item)
	}
	return map[string]any{"queues": queues, "status": "memory", "activeBackend": "memory"}
}

func (r *QueueRepository) listQueueRecordsMemory(filters map[string]any) []map[string]any {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := []map[string]any{}
	for _, record := range r.records {
		if stringOr(filters["queueName"], "") != "" && stringOr(record["queueName"], "") != stringOr(filters["queueName"], "") {
			continue
		}
		if stringOr(filters["status"], "") != "" && stringOr(record["status"], "") != stringOr(filters["status"], "") {
			continue
		}
		if stringOr(filters["taskId"], "") != "" && stringOr(record["taskId"], "") != stringOr(filters["taskId"], "") {
			continue
		}
		if stringOr(filters["queueId"], "") != "" && stringOr(record["queueId"], "") != stringOr(filters["queueId"], "") {
			continue
		}
		out = append(out, copyMap(record))
	}
	return out
}

func (r *QueueRepository) listRecoveryRunsMemory(filters map[string]any) []map[string]any {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := []map[string]any{}
	for _, run := range r.recoveryRuns {
		if stringOr(filters["jobType"], "") != "" && stringOr(run["jobType"], "") != stringOr(filters["jobType"], "") {
			continue
		}
		if stringOr(filters["status"], "") != "" && stringOr(run["status"], "") != stringOr(filters["status"], "") {
			continue
		}
		out = append(out, copyMap(run))
	}
	return out
}

type queueScanner interface {
	Scan(dest ...any) error
}

func scanQueueRecord(scanner queueScanner) (map[string]any, error) {
	var queueID, queueName, taskType, taskID, dedupeKey, status, leaseOwner string
	var leaseTokenHash, attemptSeriesID, deadLetterReason, replayedFromQueueID, replayedBy, replayReason string
	var priority, attempt, maxAttempts int
	var fencingToken int64
	var availableAt, leaseExpiresAt, heartbeatAt, deadLetteredAt, createdAt, updatedAt time.Time
	var leaseExpiresAtValid, heartbeatAtValid, deadLetteredAtValid bool
	var payloadRaw, errorRaw []byte
	if err := scanner.Scan(
		&queueID, &queueName, &taskType, &taskID, &dedupeKey, &status, &priority, &attempt, &maxAttempts,
		&leaseOwner, &availableAt, &leaseExpiresAt, &leaseExpiresAtValid, &leaseTokenHash, &fencingToken,
		&heartbeatAt, &heartbeatAtValid, &attemptSeriesID, &deadLetteredAt, &deadLetteredAtValid,
		&deadLetterReason, &replayedFromQueueID, &replayedBy, &replayReason,
		&payloadRaw, &errorRaw, &createdAt, &updatedAt,
	); err != nil {
		return nil, err
	}
	out := map[string]any{
		"queueId":           queueID,
		"queueName":         queueName,
		"taskType":          taskType,
		"taskId":            taskID,
		"dedupeKey":         dedupeKey,
		"status":            status,
		"priority":          priority,
		"attempt":           attempt,
		"maxAttempts":       maxAttempts,
		"leaseOwner":        leaseOwner,
		"leaseTokenHash":    leaseTokenHash,
		"leaseFencingToken": fencingToken,
		"attemptSeriesId":   attemptSeriesID,
		"availableAt":       availableAt.UTC().Format(time.RFC3339),
		"payload":           jsonMap(payloadRaw),
		"errorSummary":      jsonMap(errorRaw),
		"createdAt":         createdAt.UTC().Format(time.RFC3339),
		"updatedAt":         updatedAt.UTC().Format(time.RFC3339),
	}
	if leaseExpiresAtValid {
		out["leaseExpiresAt"] = leaseExpiresAt.UTC().Format(time.RFC3339)
	}
	if heartbeatAtValid {
		out["heartbeatAt"] = heartbeatAt.UTC().Format(time.RFC3339Nano)
	}
	if deadLetteredAtValid {
		out["deadLetteredAt"] = deadLetteredAt.UTC().Format(time.RFC3339Nano)
	}
	if deadLetterReason != "" {
		out["deadLetterReason"] = deadLetterReason
	}
	if replayedFromQueueID != "" {
		out["replayedFromQueueId"] = replayedFromQueueID
		out["replayedBy"] = replayedBy
		out["replayReason"] = replayReason
	}
	return out, nil
}

func scanSystemJobRun(scanner queueScanner) (map[string]any, error) {
	var runID, jobType, businessDate, status string
	var payloadRaw, resultRaw []byte
	var createdAt, updatedAt time.Time
	if err := scanner.Scan(&runID, &jobType, &businessDate, &status, &payloadRaw, &resultRaw, &createdAt, &updatedAt); err != nil {
		return nil, err
	}
	out := map[string]any{
		"runId":     runID,
		"jobType":   jobType,
		"status":    status,
		"payload":   jsonMap(payloadRaw),
		"result":    jsonMap(resultRaw),
		"createdAt": createdAt.UTC().Format(time.RFC3339),
		"updatedAt": updatedAt.UTC().Format(time.RFC3339),
	}
	if businessDate != "" {
		out["businessDate"] = businessDate
	}
	return out, nil
}

func timeValue(value any, fallback time.Time) time.Time {
	switch typed := value.(type) {
	case time.Time:
		return typed
	case string:
		if parsed, err := time.Parse(time.RFC3339, typed); err == nil {
			return parsed
		}
		if parsed, err := time.Parse("2006-01-02", typed); err == nil {
			return parsed
		}
	}
	return fallback
}

func defaultInt(value any, fallback int) int {
	resolved := intValue(value)
	if resolved == 0 {
		return fallback
	}
	return resolved
}
