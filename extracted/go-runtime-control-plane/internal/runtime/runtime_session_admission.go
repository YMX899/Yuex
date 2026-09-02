package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"huahuoai/backend/source/internal/persistence"
	"huahuoai/backend/source/internal/queue"

	"github.com/jackc/pgx/v5"
)

type ProductSessionAdmissionKey struct {
	TenantID          string
	ThreadID          string
	AgentProfile      string
	ContextGeneration int64
	SessionGeneration int
}

func (k ProductSessionAdmissionKey) RedisKey() string {
	if validateProductSessionAdmissionKey(k) != nil {
		return ""
	}
	return fmt.Sprintf("runtime:session:%s:%s:%s:%d:%d", encodeRuntimeKeySegment(k.TenantID), encodeRuntimeKeySegment(k.ThreadID), encodeRuntimeKeySegment(k.AgentProfile), k.ContextGeneration, k.SessionGeneration)
}

type RuntimeSessionAdmission struct {
	AdmissionID     string
	Key             ProductSessionAdmissionKey
	BindingID       string
	RunID           string
	OwnerInstanceID string
	LeaseTokenHash  string
	FencingToken    int64
	State           string
	ReservationID   string
	DispatchID      string
	ExpiresAt       time.Time
	LastRenewedAt   time.Time
	Version         int64
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

type RuntimeSessionAdmissionLease struct {
	Admission RuntimeSessionAdmission
	Lease     queue.DistributedLease
}

type ProductSessionAdmissionCommand struct {
	Key             ProductSessionAdmissionKey
	BindingID       string
	RunID           string
	OwnerInstanceID string
	TTL             time.Duration
}

type RuntimeSessionAdmissionRecoveryReport struct {
	Scanned    int
	Recovering int
	Expired    int
}

// RuntimeSessionTerminalCleanupReport exposes only durable cleanup counts.
// It never includes a Redis key, token, token hash, or fencing value.
type RuntimeSessionTerminalCleanupReport struct {
	Claimed   int
	Completed int
	Retried   int
	Stale     int
}

// runtimeSessionTerminalCleanupClaim carries the exact durable proof needed
// by the internal cleanup worker. It is deliberately unexported so API and
// worker callers cannot use a hash-only proof as a general lock capability.
type runtimeSessionTerminalCleanupClaim struct {
	ConvergenceID string
	Admission     RuntimeSessionAdmission
	OwnerID       string
	FencingToken  int64
	LeaseExpires  time.Time
	Attempt       int
}

// RuntimeSessionAdmissionCleanupReport exposes generic admission-cleanup
// counts without leaking Redis/Tair proof material.
type RuntimeSessionAdmissionCleanupReport struct {
	Claimed   int
	Completed int
	Retried   int
	Stale     int
}

// runtimeSessionAdmissionCleanupClaim is the proof-bearing internal form of
// an admission-level cleanup record. The raw Redis token never reaches this
// structure or its durable counterpart.
type runtimeSessionAdmissionCleanupClaim struct {
	Admission RuntimeSessionAdmission
	Origin    string
	Reason    string
	OwnerID   string
	Fence     int64
	ExpiresAt time.Time
	Attempt   int
}

type runtimeSessionAdmissionCleanupRecord struct {
	Admission RuntimeSessionAdmission
	Origin    string
	Reason    string
	Status    string
	Attempt   int
	OwnerID   string
	Fence     int64
	ExpiresAt time.Time
	NextTryAt time.Time
	LastError string
	Completed time.Time
}

type RuntimeSessionAdmissionService struct {
	DB      *persistence.Database
	Locks   *queue.DistributedLockManager
	Now     func() time.Time
	mu      sync.Mutex
	items   map[string]RuntimeSessionAdmission
	handles map[string]RuntimeSessionAdmissionLease
	cleanup map[string]runtimeSessionAdmissionCleanupRecord
	// afterAdmissionCleanupEnqueue is a test-only crash point between the
	// durable admission/outbox commit and the external Redis/Tair cleanup.
	afterAdmissionCleanupEnqueue func() error
}

func NewRuntimeSessionAdmissionService(db *persistence.Database, locks *queue.DistributedLockManager) *RuntimeSessionAdmissionService {
	return &RuntimeSessionAdmissionService{DB: db, Locks: locks, Now: func() time.Time { return time.Now().UTC() }, items: map[string]RuntimeSessionAdmission{}, handles: map[string]RuntimeSessionAdmissionLease{}, cleanup: map[string]runtimeSessionAdmissionCleanupRecord{}}
}

func (s *RuntimeSessionAdmissionService) Acquire(ctx context.Context, command ProductSessionAdmissionCommand) (RuntimeSessionAdmissionLease, error) {
	if s == nil || s.Locks == nil || command.BindingID == "" || command.RunID == "" || command.OwnerInstanceID == "" || command.TTL <= 0 || validateProductSessionAdmissionKey(command.Key) != nil {
		return RuntimeSessionAdmissionLease{}, fmt.Errorf("INVALID_ARGUMENT")
	}
	redisLease, err := s.Locks.Acquire(ctx, command.Key.RedisKey(), command.OwnerInstanceID, command.RunID, command.TTL)
	if err != nil {
		if err.Error() == "SERVICE_BUSY" {
			return RuntimeSessionAdmissionLease{}, fmt.Errorf("RUNTIME_SESSION_BUSY")
		}
		return RuntimeSessionAdmissionLease{}, err
	}
	now := s.now()
	admission := RuntimeSessionAdmission{
		AdmissionID: stableRuntimeAdmissionID(command.Key, command.RunID, redisLease.FencingToken), Key: command.Key,
		BindingID: command.BindingID, RunID: command.RunID, OwnerInstanceID: command.OwnerInstanceID,
		LeaseTokenHash: redisLease.TokenHash, FencingToken: redisLease.FencingToken, State: "acquired",
		ExpiresAt: redisLease.ExpiresAt, LastRenewedAt: now, Version: 1, CreatedAt: now, UpdatedAt: now,
	}
	if s.postgresReady() {
		err = s.DB.WithTx(ctx, func(tx *persistence.Tx) error {
			bindings, err := tx.Query(ctx, `select binding_id from thread_agent_runtime_bindings where binding_id=@binding and tenant_id=@tenant and thread_id=@thread and agent_profile=@profile and context_generation=@context and session_generation=@session and status='active' for update`, map[string]any{
				"binding": command.BindingID, "tenant": command.Key.TenantID, "thread": command.Key.ThreadID,
				"profile": command.Key.AgentProfile, "context": command.Key.ContextGeneration, "session": command.Key.SessionGeneration,
			})
			if err != nil || len(bindings) != 1 {
				return fmt.Errorf("RUNTIME_SESSION_BINDING_UNAVAILABLE")
			}
			return tx.Exec(ctx, `insert into runtime_session_admissions(admission_id,tenant_id,thread_id,agent_profile,context_generation,session_generation,binding_id,run_id,owner_instance_id,lease_token_hash,fencing_token,state,expires_at,last_renewed_at,version) values(@id,@tenant,@thread,@profile,@context,@session,@binding,@run,@owner,@hash,@fencing,'acquired',@expires,@renewed,1)`, map[string]any{
				"id": admission.AdmissionID, "tenant": admission.Key.TenantID, "thread": admission.Key.ThreadID,
				"profile": admission.Key.AgentProfile, "context": admission.Key.ContextGeneration,
				"session": admission.Key.SessionGeneration, "binding": admission.BindingID, "run": admission.RunID,
				"owner": admission.OwnerInstanceID, "hash": admission.LeaseTokenHash, "fencing": admission.FencingToken,
				"expires": admission.ExpiresAt, "renewed": admission.LastRenewedAt,
			})
		})
	} else {
		s.mu.Lock()
		for _, current := range s.items {
			if current.Key == command.Key && runtimeSessionAdmissionActive(current.State) {
				err = fmt.Errorf("RUNTIME_SESSION_BUSY")
				break
			}
		}
		if err == nil {
			s.items[admission.AdmissionID] = admission
		}
		s.mu.Unlock()
	}
	if err != nil {
		_ = s.Locks.Release(ctx, redisLease)
		if runtimeUniqueViolation(err) {
			return RuntimeSessionAdmissionLease{}, fmt.Errorf("RUNTIME_SESSION_BUSY")
		}
		return RuntimeSessionAdmissionLease{}, err
	}
	handle := RuntimeSessionAdmissionLease{Admission: admission, Lease: redisLease}
	s.mu.Lock()
	s.handles[admission.AdmissionID] = handle
	s.mu.Unlock()
	return handle, nil
}

func (s *RuntimeSessionAdmissionService) Renew(ctx context.Context, handle RuntimeSessionAdmissionLease, ttl time.Duration) (RuntimeSessionAdmissionLease, error) {
	if ttl <= 0 || validateRuntimeSessionAdmissionLease(handle) != nil {
		return RuntimeSessionAdmissionLease{}, fmt.Errorf("INVALID_ARGUMENT")
	}
	lease, err := s.Locks.Renew(ctx, handle.Lease, ttl)
	if err != nil {
		return RuntimeSessionAdmissionLease{}, err
	}
	if s.postgresReady() {
		result, err := s.DB.Pool.Exec(ctx, `update runtime_session_admissions set expires_at=$6,last_renewed_at=now(),version=version+1,updated_at=now() where admission_id=$1 and owner_instance_id=$2 and run_id=$3 and lease_token_hash=$4 and fencing_token=$5 and state in('acquired','reservation_bound','dispatch_bound') and expires_at>now()`, handle.Admission.AdmissionID, handle.Admission.OwnerInstanceID, handle.Admission.RunID, handle.Admission.LeaseTokenHash, handle.Admission.FencingToken, lease.ExpiresAt)
		if err != nil || result.RowsAffected() != 1 {
			return RuntimeSessionAdmissionLease{}, fmt.Errorf("STALE_FENCING_TOKEN")
		}
	} else {
		s.mu.Lock()
		current, ok := s.items[handle.Admission.AdmissionID]
		if !ok || !runtimeAdmissionMatches(current, handle.Admission) || !runtimeSessionAdmissionActive(current.State) {
			s.mu.Unlock()
			return RuntimeSessionAdmissionLease{}, fmt.Errorf("STALE_FENCING_TOKEN")
		}
		current.ExpiresAt, current.LastRenewedAt, current.UpdatedAt = lease.ExpiresAt, s.now(), s.now()
		current.Version++
		s.items[current.AdmissionID] = current
		handle.Admission = current
		s.mu.Unlock()
	}
	handle.Lease = lease
	handle.Admission.ExpiresAt = lease.ExpiresAt
	handle.Admission.LastRenewedAt = s.now()
	if s.postgresReady() {
		handle.Admission.Version++
	}
	s.mu.Lock()
	s.handles[handle.Admission.AdmissionID] = handle
	s.mu.Unlock()
	return handle, nil
}

func (s *RuntimeSessionAdmissionService) AssertActive(ctx context.Context, handle RuntimeSessionAdmissionLease) error {
	if validateRuntimeSessionAdmissionLease(handle) != nil {
		return fmt.Errorf("INVALID_ARGUMENT")
	}
	if err := s.Locks.AssertActiveLease(ctx, handle.Lease, 0); err != nil {
		return err
	}
	if s.postgresReady() {
		var exists bool
		err := s.DB.Pool.QueryRow(ctx, `select true from runtime_session_admissions where admission_id=$1 and owner_instance_id=$2 and run_id=$3 and lease_token_hash=$4 and fencing_token=$5 and state in('acquired','reservation_bound','dispatch_bound') and expires_at>now()`, handle.Admission.AdmissionID, handle.Admission.OwnerInstanceID, handle.Admission.RunID, handle.Admission.LeaseTokenHash, handle.Admission.FencingToken).Scan(&exists)
		if err != nil || !exists {
			return fmt.Errorf("STALE_FENCING_TOKEN")
		}
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.items[handle.Admission.AdmissionID]
	if !ok || !runtimeAdmissionMatches(current, handle.Admission) || !runtimeSessionAdmissionActive(current.State) || !current.ExpiresAt.After(s.now()) {
		return fmt.Errorf("STALE_FENCING_TOKEN")
	}
	return nil
}

// AssertActiveDispatchFence validates the durable Product-session admission
// that is bound to one exact Runtime dispatch. It deliberately reconstructs
// only a hash-bound, read-only distributed-lease assertion: callers cannot use
// this proof to renew or release the lease because the raw random token is not
// available outside the protected owner handle.
func (s *RuntimeSessionAdmissionService) AssertActiveDispatchFence(ctx context.Context, runID, reservationID, dispatchID string) error {
	if s == nil || s.Locks == nil || strings.TrimSpace(runID) == "" || strings.TrimSpace(reservationID) == "" || strings.TrimSpace(dispatchID) == "" {
		return fmt.Errorf("INVALID_ARGUMENT")
	}
	admission, err := s.activeDispatchAdmission(ctx, runID, reservationID, dispatchID)
	if err != nil {
		return err
	}
	key := admission.Key.RedisKey()
	if key == "" {
		return fmt.Errorf("STALE_FENCING_TOKEN")
	}
	// AssertActiveLease compares the full hash-bound owner/run/scope/fence
	// identity. A blank Token is intentional here: this is not a capability to
	// mutate the Redis/Tair lease.
	if err := s.Locks.AssertActiveLease(ctx, queue.DistributedLease{
		Key: key, Scope: key, OwnerInstanceID: admission.OwnerInstanceID, RunID: admission.RunID,
		TokenHash: admission.LeaseTokenHash, FencingToken: admission.FencingToken, ExpiresAt: admission.ExpiresAt,
	}, 0); err != nil {
		return err
	}
	// A release or rebinding can race the Redis assertion. Re-read the durable
	// identity so a changed admission cannot authorize the subsequent abort.
	current, err := s.activeDispatchAdmission(ctx, runID, reservationID, dispatchID)
	if err != nil {
		return err
	}
	if !runtimeSessionAdmissionDispatchFenceMatches(admission, current) {
		return fmt.Errorf("STALE_FENCING_TOKEN")
	}
	return nil
}

func (s *RuntimeSessionAdmissionService) activeDispatchAdmission(ctx context.Context, runID, reservationID, dispatchID string) (RuntimeSessionAdmission, error) {
	if s.postgresReady() {
		rows, err := s.DB.Pool.Query(ctx, `
select admission_id,tenant_id,thread_id,agent_profile,context_generation,session_generation,binding_id,run_id,owner_instance_id,lease_token_hash,fencing_token,state,coalesce(reservation_id,''),coalesce(dispatch_id,''),expires_at,last_renewed_at,version,created_at,updated_at
from runtime_session_admissions
where run_id=$1 and reservation_id=$2 and dispatch_id=$3
  and state='dispatch_bound' and expires_at>now()
order by created_at desc
limit 2`, runID, reservationID, dispatchID)
		if err != nil {
			return RuntimeSessionAdmission{}, fmt.Errorf("RUNTIME_STORAGE_UNAVAILABLE")
		}
		defer rows.Close()
		matches := make([]RuntimeSessionAdmission, 0, 1)
		for rows.Next() {
			admission, scanErr := scanRuntimeSessionAdmission(rows.Scan)
			if scanErr != nil || !validRuntimeSessionAdmissionDispatchFence(admission, runID, reservationID, dispatchID) {
				return RuntimeSessionAdmission{}, fmt.Errorf("RUNTIME_STORAGE_UNAVAILABLE")
			}
			matches = append(matches, admission)
		}
		if err := rows.Err(); err != nil {
			return RuntimeSessionAdmission{}, fmt.Errorf("RUNTIME_STORAGE_UNAVAILABLE")
		}
		if len(matches) != 1 {
			return RuntimeSessionAdmission{}, fmt.Errorf("STALE_FENCING_TOKEN")
		}
		return matches[0], nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	matches := make([]RuntimeSessionAdmission, 0, 1)
	for _, candidate := range s.items {
		if validRuntimeSessionAdmissionDispatchFence(candidate, runID, reservationID, dispatchID) && candidate.ExpiresAt.After(s.now()) {
			matches = append(matches, candidate)
		}
	}
	if len(matches) != 1 {
		return RuntimeSessionAdmission{}, fmt.Errorf("STALE_FENCING_TOKEN")
	}
	return matches[0], nil
}

func scanRuntimeSessionAdmission(scan func(...any) error) (RuntimeSessionAdmission, error) {
	var admission RuntimeSessionAdmission
	err := scan(
		&admission.AdmissionID, &admission.Key.TenantID, &admission.Key.ThreadID, &admission.Key.AgentProfile,
		&admission.Key.ContextGeneration, &admission.Key.SessionGeneration, &admission.BindingID, &admission.RunID,
		&admission.OwnerInstanceID, &admission.LeaseTokenHash, &admission.FencingToken, &admission.State,
		&admission.ReservationID, &admission.DispatchID, &admission.ExpiresAt, &admission.LastRenewedAt,
		&admission.Version, &admission.CreatedAt, &admission.UpdatedAt,
	)
	return admission, err
}

func validRuntimeSessionAdmissionDispatchFence(admission RuntimeSessionAdmission, runID, reservationID, dispatchID string) bool {
	return admission.AdmissionID != "" && admission.RunID == runID && admission.ReservationID == reservationID && admission.DispatchID == dispatchID &&
		admission.OwnerInstanceID != "" && admission.LeaseTokenHash != "" && admission.FencingToken > 0 && admission.State == "dispatch_bound" &&
		!admission.ExpiresAt.IsZero() && validateProductSessionAdmissionKey(admission.Key) == nil
}

func runtimeSessionAdmissionDispatchFenceMatches(left, right RuntimeSessionAdmission) bool {
	return left.AdmissionID == right.AdmissionID && left.Key == right.Key && left.BindingID == right.BindingID &&
		left.RunID == right.RunID && left.OwnerInstanceID == right.OwnerInstanceID && left.LeaseTokenHash == right.LeaseTokenHash &&
		left.FencingToken == right.FencingToken && left.ReservationID == right.ReservationID && left.DispatchID == right.DispatchID &&
		left.State == "dispatch_bound" && right.State == "dispatch_bound"
}

func (s *RuntimeSessionAdmissionService) BindReservation(ctx context.Context, handle RuntimeSessionAdmissionLease, reservationID string) error {
	return s.bind(ctx, handle, reservationID, "", "reservation_bound")
}

func (s *RuntimeSessionAdmissionService) BindDispatch(ctx context.Context, handle RuntimeSessionAdmissionLease, dispatchID string) error {
	return s.bind(ctx, handle, "", dispatchID, "dispatch_bound")
}

func (s *RuntimeSessionAdmissionService) bind(ctx context.Context, handle RuntimeSessionAdmissionLease, reservationID, dispatchID, state string) error {
	if reservationID == "" && dispatchID == "" {
		return fmt.Errorf("INVALID_ARGUMENT")
	}
	if err := s.AssertActive(ctx, handle); err != nil {
		return err
	}
	if s.postgresReady() {
		query := `update runtime_session_admissions set reservation_id=$6,state='reservation_bound',version=version+1,updated_at=now() where admission_id=$1 and owner_instance_id=$2 and run_id=$3 and lease_token_hash=$4 and fencing_token=$5 and state in('acquired','reservation_bound') and (reservation_id is null or reservation_id=$6)`
		value := reservationID
		if dispatchID != "" {
			query = `update runtime_session_admissions set dispatch_id=$6,state='dispatch_bound',version=version+1,updated_at=now() where admission_id=$1 and owner_instance_id=$2 and run_id=$3 and lease_token_hash=$4 and fencing_token=$5 and state in('reservation_bound','dispatch_bound') and reservation_id is not null and (dispatch_id is null or dispatch_id=$6)`
			value = dispatchID
		}
		result, err := s.DB.Pool.Exec(ctx, query, handle.Admission.AdmissionID, handle.Admission.OwnerInstanceID, handle.Admission.RunID, handle.Admission.LeaseTokenHash, handle.Admission.FencingToken, value)
		if err != nil || result.RowsAffected() != 1 {
			return fmt.Errorf("STALE_FENCING_TOKEN")
		}
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.items[handle.Admission.AdmissionID]
	if !ok || !runtimeAdmissionMatches(current, handle.Admission) {
		return fmt.Errorf("STALE_FENCING_TOKEN")
	}
	if reservationID != "" {
		if current.ReservationID != "" && current.ReservationID != reservationID {
			return fmt.Errorf("STALE_FENCING_TOKEN")
		}
		current.ReservationID = reservationID
	} else {
		if current.ReservationID == "" || current.DispatchID != "" && current.DispatchID != dispatchID {
			return fmt.Errorf("STALE_FENCING_TOKEN")
		}
		current.DispatchID = dispatchID
	}
	current.State, current.UpdatedAt = state, s.now()
	current.Version++
	s.items[current.AdmissionID] = current
	return nil
}

func (s *RuntimeSessionAdmissionService) Release(ctx context.Context, handle RuntimeSessionAdmissionLease, reason string) (bool, error) {
	if s == nil || s.Locks == nil || validateRuntimeSessionAdmissionLease(handle) != nil || !validRuntimeAdmissionReleaseReason(reason) {
		return false, fmt.Errorf("INVALID_ARGUMENT")
	}
	var (
		changed bool
		err     error
	)
	if s.postgresReady() {
		changed, err = s.releaseAdmissionPostgres(ctx, handle, reason)
	} else {
		changed, err = s.releaseAdmissionMemory(handle, reason)
	}
	if err != nil {
		return false, err
	}
	if s.afterAdmissionCleanupEnqueue != nil {
		return changed, s.afterAdmissionCleanupEnqueue()
	}
	_, err = s.DrainAdmissionCleanup(ctx, "runtime-session-release:"+handle.Admission.RunID, 1)
	return changed, err
}

// ReleaseTerminalLease releases only the external Redis/Tair capability after
// its authoritative PostgreSQL admission state has already reached a terminal
// state. Direct in-process callers retain the raw token; Runtime terminal
// convergence instead uses the durable cleanup outbox and proof-only drain so
// a process crash cannot discard post-commit cleanup work.
func (s *RuntimeSessionAdmissionService) ReleaseTerminalLease(ctx context.Context, handle RuntimeSessionAdmissionLease) error {
	if s == nil || s.Locks == nil || validateRuntimeSessionAdmissionLease(handle) != nil {
		return fmt.Errorf("INVALID_ARGUMENT")
	}
	if err := s.Locks.Release(ctx, handle.Lease); err != nil && err.Error() != "STALE_FENCING_TOKEN" {
		return err
	}
	s.mu.Lock()
	delete(s.handles, handle.Admission.AdmissionID)
	s.mu.Unlock()
	return nil
}

// releaseAdmissionPostgres commits the authoritative admission transition and
// its generic cleanup record together. The subsequent Redis/Tair operation is
// deliberately outside this transaction and is replayable from the record.
func (s *RuntimeSessionAdmissionService) releaseAdmissionPostgres(ctx context.Context, handle RuntimeSessionAdmissionLease, reason string) (bool, error) {
	changed := false
	err := s.DB.WithTx(ctx, func(tx *persistence.Tx) error {
		rows, err := tx.Query(ctx, `
select state,coalesce(release_reason,''),owner_instance_id,run_id,lease_token_hash,fencing_token
from runtime_session_admissions
where admission_id=@id
for update`, map[string]any{"id": handle.Admission.AdmissionID})
		if err != nil || len(rows) != 1 {
			if err != nil {
				return err
			}
			return fmt.Errorf("STALE_FENCING_TOKEN")
		}
		row := rows[0]
		if fmt.Sprint(row["owner_instance_id"]) != handle.Admission.OwnerInstanceID || fmt.Sprint(row["run_id"]) != handle.Admission.RunID || fmt.Sprint(row["lease_token_hash"]) != handle.Admission.LeaseTokenHash || runtimeHostInt64(row["fencing_token"]) != handle.Admission.FencingToken {
			return fmt.Errorf("STALE_FENCING_TOKEN")
		}
		state := fmt.Sprint(row["state"])
		effectiveReason := reason
		if state == "released" || state == "expired" {
			if stored := fmt.Sprint(row["release_reason"]); validRuntimeAdmissionReleaseReason(stored) {
				effectiveReason = stored
			}
		} else {
			if err := s.Locks.AssertActiveLease(ctx, handle.Lease, 0); err != nil {
				return err
			}
			if err := tx.Exec(ctx, `
update runtime_session_admissions
set state='released',release_reason=@reason,version=version+1,updated_at=now()
where admission_id=@id`, map[string]any{"id": handle.Admission.AdmissionID, "reason": reason}); err != nil {
				return err
			}
			state = "released"
			changed = true
		}
		admission := handle.Admission
		admission.State = state
		return s.enqueueAdmissionCleanupInTx(ctx, tx, admission, runtimeSessionAdmissionCleanupOrigin(effectiveReason), effectiveReason)
	})
	return changed, err
}

func (s *RuntimeSessionAdmissionService) releaseAdmissionMemory(handle RuntimeSessionAdmissionLease, reason string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.items[handle.Admission.AdmissionID]
	if !ok || !runtimeAdmissionMatches(current, handle.Admission) {
		return false, fmt.Errorf("STALE_FENCING_TOKEN")
	}
	changed := false
	effectiveReason := reason
	if current.State == "released" || current.State == "expired" {
		if record, ok := s.cleanup[current.AdmissionID]; ok && validRuntimeAdmissionReleaseReason(record.Reason) {
			effectiveReason = record.Reason
		}
	} else {
		current.State, current.UpdatedAt = "released", s.now()
		current.Version++
		s.items[current.AdmissionID] = current
		changed = true
	}
	if err := s.ensureAdmissionCleanupMemoryLocked(current, runtimeSessionAdmissionCleanupOrigin(effectiveReason), effectiveReason); err != nil {
		return false, err
	}
	return changed, nil
}

// enqueueAdmissionCleanupInTx records a generic cleanup work item for direct
// admission releases. It intentionally has no terminal convergence foreign
// key: normal Runtime terminal convergence uses migration 043 instead.
func (s *RuntimeSessionAdmissionService) enqueueAdmissionCleanupInTx(ctx context.Context, tx *persistence.Tx, admission RuntimeSessionAdmission, origin, reason string) error {
	if tx == nil || admission.AdmissionID == "" || admission.RunID == "" || admission.OwnerInstanceID == "" || admission.LeaseTokenHash == "" || admission.FencingToken < 1 || !stringInRuntime(admission.State, []string{"released", "expired"}) || !validRuntimeSessionAdmissionCleanupOrigin(origin) || !validRuntimeAdmissionReleaseReason(reason) {
		return fmt.Errorf("INVALID_ARGUMENT")
	}
	terminalRows, err := tx.Query(ctx, `
select terminal.convergence_id
from runtime_session_terminal_cleanup_outbox terminal
join runtime_terminal_convergences convergence on convergence.convergence_id=terminal.convergence_id
where terminal.admission_id=@admission and convergence.run_id=@run
for update of terminal,convergence`, map[string]any{"admission": admission.AdmissionID, "run": admission.RunID})
	if err != nil {
		return err
	}
	if len(terminalRows) > 0 {
		// A real terminal convergence is the sole durable owner for this exact
		// admission/run. Do not create duplicate generic cleanup work.
		return nil
	}
	if err := tx.Exec(ctx, `
insert into runtime_session_admission_cleanup_outbox(
  admission_id,run_id,owner_instance_id,lease_token_hash,fencing_token,cleanup_origin,release_reason,status,next_attempt_at
)
values(@admission,@run,@owner,@hash,@fencing,@origin,@reason,'pending',now())
on conflict(admission_id) do nothing`, map[string]any{
		"admission": admission.AdmissionID, "run": admission.RunID, "owner": admission.OwnerInstanceID,
		"hash": admission.LeaseTokenHash, "fencing": admission.FencingToken,
		"origin": origin, "reason": reason,
	}); err != nil {
		return err
	}
	rows, err := tx.Query(ctx, `
select o.run_id,o.owner_instance_id,o.lease_token_hash,o.fencing_token,
       a.run_id as admission_run_id,a.owner_instance_id as admission_owner,a.lease_token_hash as admission_hash,a.fencing_token as admission_fencing
from runtime_session_admission_cleanup_outbox o
join runtime_session_admissions a on a.admission_id=o.admission_id
where o.admission_id=@admission
for update of o,a`, map[string]any{"admission": admission.AdmissionID})
	if err != nil {
		return err
	}
	if len(rows) != 1 || fmt.Sprint(rows[0]["run_id"]) != admission.RunID || fmt.Sprint(rows[0]["owner_instance_id"]) != admission.OwnerInstanceID || fmt.Sprint(rows[0]["lease_token_hash"]) != admission.LeaseTokenHash || runtimeHostInt64(rows[0]["fencing_token"]) != admission.FencingToken || fmt.Sprint(rows[0]["admission_run_id"]) != admission.RunID || fmt.Sprint(rows[0]["admission_owner"]) != admission.OwnerInstanceID || fmt.Sprint(rows[0]["admission_hash"]) != admission.LeaseTokenHash || runtimeHostInt64(rows[0]["admission_fencing"]) != admission.FencingToken {
		return fmt.Errorf("RUNTIME_SESSION_CLEANUP_CONFLICT")
	}
	return nil
}

func (s *RuntimeSessionAdmissionService) ensureAdmissionCleanupMemoryLocked(admission RuntimeSessionAdmission, origin, reason string) error {
	if s.cleanup == nil {
		s.cleanup = map[string]runtimeSessionAdmissionCleanupRecord{}
	}
	if existing, ok := s.cleanup[admission.AdmissionID]; ok {
		if !runtimeSessionAdmissionCleanupIdentityMatches(existing.Admission, admission) {
			return fmt.Errorf("RUNTIME_SESSION_CLEANUP_CONFLICT")
		}
		return nil
	}
	s.cleanup[admission.AdmissionID] = runtimeSessionAdmissionCleanupRecord{
		Admission: admission, Origin: origin, Reason: reason, Status: "pending", NextTryAt: s.now(),
	}
	return nil
}

// DrainAdmissionCleanup is the generic counterpart of terminal-convergence
// cleanup. It only consumes a durable released/expired admission whose
// immutable run/owner/hash/fence values still match the outbox record.
func (s *RuntimeSessionAdmissionService) DrainAdmissionCleanup(ctx context.Context, workerID string, limit int) (RuntimeSessionAdmissionCleanupReport, error) {
	if s == nil || s.Locks == nil || strings.TrimSpace(workerID) == "" || limit < 1 {
		return RuntimeSessionAdmissionCleanupReport{}, fmt.Errorf("INVALID_ARGUMENT")
	}
	if limit > 500 {
		limit = 500
	}
	if !s.postgresReady() {
		return s.drainAdmissionCleanupMemory(ctx, workerID, limit)
	}
	report := RuntimeSessionAdmissionCleanupReport{}
	for report.Claimed < limit {
		claim, found, err := s.claimAdmissionCleanup(ctx, workerID, 30*time.Second)
		if err != nil || !found {
			return report, err
		}
		report.Claimed++
		proof, proofErr := runtimeSessionAdmissionCleanupProof(claim)
		if proofErr != nil {
			if retryErr := s.retryAdmissionCleanup(ctx, claim, runtimeSessionAdmissionCleanupErrorCode(proofErr)); retryErr != nil {
				return report, retryErr
			}
			report.Retried++
			continue
		}
		releaseErr := s.Locks.ReleaseTerminalLeaseProof(ctx, proof)
		if releaseErr == nil || releaseErr.Error() == "STALE_FENCING_TOKEN" {
			if completeErr := s.completeAdmissionCleanup(ctx, claim); completeErr != nil {
				return report, completeErr
			}
			if releaseErr != nil {
				report.Stale++
			}
			report.Completed++
			s.mu.Lock()
			delete(s.handles, claim.Admission.AdmissionID)
			s.mu.Unlock()
			continue
		}
		if retryErr := s.retryAdmissionCleanup(ctx, claim, runtimeSessionAdmissionCleanupErrorCode(releaseErr)); retryErr != nil {
			return report, retryErr
		}
		report.Retried++
	}
	return report, nil
}

// BackfillAdmissionCleanup closes the rolling-upgrade window where an older
// process released an admission after migration 044 ran but before it started
// writing the generic outbox. A matching, same-run 043 record already owns a
// real terminal convergence and is deliberately left to the terminal drain.
func (s *RuntimeSessionAdmissionService) BackfillAdmissionCleanup(ctx context.Context, limit int) (int, error) {
	if s == nil || limit < 1 {
		return 0, fmt.Errorf("INVALID_ARGUMENT")
	}
	if limit > 500 {
		limit = 500
	}
	if !s.postgresReady() {
		return s.backfillAdmissionCleanupMemory(limit)
	}
	created := 0
	for created < limit {
		found := false
		err := s.DB.WithTx(ctx, func(tx *persistence.Tx) error {
			rows, err := tx.Query(ctx, `
select a.admission_id,a.tenant_id,a.thread_id,a.agent_profile,a.context_generation,a.session_generation,a.binding_id,a.run_id,a.owner_instance_id,a.lease_token_hash,a.fencing_token,a.state,coalesce(a.reservation_id,''),coalesce(a.dispatch_id,''),a.expires_at,a.last_renewed_at,a.version,a.created_at,a.updated_at,coalesce(a.release_reason,'') as release_reason
from runtime_session_admissions a
where a.state in ('released','expired')
  and not exists (
    select 1 from runtime_session_admission_cleanup_outbox generic
    where generic.admission_id=a.admission_id
      and generic.run_id=a.run_id
      and generic.owner_instance_id=a.owner_instance_id
      and generic.lease_token_hash=a.lease_token_hash
      and generic.fencing_token=a.fencing_token
  )
  and not exists (
    select 1
    from runtime_session_terminal_cleanup_outbox terminal
    join runtime_terminal_convergences convergence on convergence.convergence_id=terminal.convergence_id
    where terminal.admission_id=a.admission_id
      and convergence.run_id=a.run_id
  )
order by a.updated_at,a.admission_id
for update of a skip locked
limit 1`, nil)
			if err != nil {
				return err
			}
			if len(rows) == 0 {
				return nil
			}
			if len(rows) != 1 {
				return fmt.Errorf("RUNTIME_SESSION_CLEANUP_CONFLICT")
			}
			admission, err := runtimeSessionAdmissionFromMap(rows[0])
			if err != nil {
				return err
			}
			reason := runtimeSessionAdmissionCleanupBackfillReason(admission.State, strings.TrimSpace(fmt.Sprint(rows[0]["release_reason"])))
			if err := s.enqueueAdmissionCleanupInTx(ctx, tx, admission, runtimeSessionAdmissionCleanupOrigin(reason), reason); err != nil {
				return err
			}
			found = true
			return nil
		})
		if err != nil {
			return created, err
		}
		if !found {
			return created, nil
		}
		created++
	}
	return created, nil
}

func (s *RuntimeSessionAdmissionService) backfillAdmissionCleanupMemory(limit int) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	created := 0
	ids := make([]string, 0, len(s.items))
	for admissionID := range s.items {
		ids = append(ids, admissionID)
	}
	sort.Strings(ids)
	for _, admissionID := range ids {
		if created >= limit {
			break
		}
		admission := s.items[admissionID]
		if !stringInRuntime(admission.State, []string{"released", "expired"}) {
			continue
		}
		if existing, exists := s.cleanup[admissionID]; exists {
			if !runtimeSessionAdmissionCleanupIdentityMatches(existing.Admission, admission) {
				return created, fmt.Errorf("RUNTIME_SESSION_CLEANUP_CONFLICT")
			}
			continue
		}
		reason := runtimeSessionAdmissionCleanupBackfillReason(admission.State, "")
		if err := s.ensureAdmissionCleanupMemoryLocked(admission, runtimeSessionAdmissionCleanupOrigin(reason), reason); err != nil {
			return created, err
		}
		created++
	}
	return created, nil
}

func (s *RuntimeSessionAdmissionService) drainAdmissionCleanupMemory(ctx context.Context, workerID string, limit int) (RuntimeSessionAdmissionCleanupReport, error) {
	report := RuntimeSessionAdmissionCleanupReport{}
	for report.Claimed < limit {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		claim, found, err := s.claimAdmissionCleanupMemory(workerID, 30*time.Second)
		if err != nil || !found {
			return report, err
		}
		report.Claimed++
		proof, proofErr := runtimeSessionAdmissionCleanupProof(claim)
		if proofErr != nil {
			if retryErr := s.retryAdmissionCleanupMemory(claim, runtimeSessionAdmissionCleanupErrorCode(proofErr)); retryErr != nil {
				return report, retryErr
			}
			report.Retried++
			continue
		}
		releaseErr := s.Locks.ReleaseTerminalLeaseProof(ctx, proof)
		if releaseErr == nil || releaseErr.Error() == "STALE_FENCING_TOKEN" {
			if completeErr := s.completeAdmissionCleanupMemory(claim); completeErr != nil {
				return report, completeErr
			}
			if releaseErr != nil {
				report.Stale++
			}
			report.Completed++
			s.mu.Lock()
			delete(s.handles, claim.Admission.AdmissionID)
			s.mu.Unlock()
			continue
		}
		if retryErr := s.retryAdmissionCleanupMemory(claim, runtimeSessionAdmissionCleanupErrorCode(releaseErr)); retryErr != nil {
			return report, retryErr
		}
		report.Retried++
	}
	return report, nil
}

func (s *RuntimeSessionAdmissionService) claimAdmissionCleanupMemory(workerID string, ttl time.Duration) (runtimeSessionAdmissionCleanupClaim, bool, error) {
	if strings.TrimSpace(workerID) == "" || ttl <= 0 {
		return runtimeSessionAdmissionCleanupClaim{}, false, fmt.Errorf("INVALID_ARGUMENT")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	ids := make([]string, 0, len(s.cleanup))
	for admissionID, record := range s.cleanup {
		if (record.Status == "pending" && !record.NextTryAt.After(now)) || (record.Status == "running" && !record.ExpiresAt.After(now)) {
			ids = append(ids, admissionID)
		}
	}
	sort.Slice(ids, func(i, j int) bool {
		left, right := s.cleanup[ids[i]], s.cleanup[ids[j]]
		if left.NextTryAt.Equal(right.NextTryAt) {
			return ids[i] < ids[j]
		}
		return left.NextTryAt.Before(right.NextTryAt)
	})
	for _, admissionID := range ids {
		record := s.cleanup[admissionID]
		current, ok := s.items[admissionID]
		if !ok || !runtimeSessionAdmissionCleanupIdentityMatches(record.Admission, current) || !stringInRuntime(current.State, []string{"released", "expired"}) {
			continue
		}
		record.Status = "running"
		record.OwnerID = workerID
		record.Fence++
		record.ExpiresAt = now.Add(ttl)
		record.Attempt++
		s.cleanup[admissionID] = record
		return runtimeSessionAdmissionCleanupClaim{
			Admission: record.Admission, Origin: record.Origin, Reason: record.Reason,
			OwnerID: record.OwnerID, Fence: record.Fence, ExpiresAt: record.ExpiresAt, Attempt: record.Attempt,
		}, true, nil
	}
	return runtimeSessionAdmissionCleanupClaim{}, false, nil
}

func (s *RuntimeSessionAdmissionService) completeAdmissionCleanupMemory(claim runtimeSessionAdmissionCleanupClaim) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.cleanup[claim.Admission.AdmissionID]
	current, active := s.items[claim.Admission.AdmissionID]
	if !ok || !active || !runtimeSessionAdmissionCleanupIdentityMatches(record.Admission, claim.Admission) || !runtimeSessionAdmissionCleanupIdentityMatches(current, claim.Admission) || !stringInRuntime(current.State, []string{"released", "expired"}) || record.Status != "running" || record.OwnerID != claim.OwnerID || record.Fence != claim.Fence || !record.ExpiresAt.Equal(claim.ExpiresAt) || !record.ExpiresAt.After(s.now()) {
		return fmt.Errorf("STALE_FENCING_TOKEN")
	}
	record.Status = "succeeded"
	record.OwnerID = ""
	record.ExpiresAt = time.Time{}
	record.LastError = ""
	record.Completed = s.now()
	s.cleanup[claim.Admission.AdmissionID] = record
	return nil
}

func (s *RuntimeSessionAdmissionService) retryAdmissionCleanupMemory(claim runtimeSessionAdmissionCleanupClaim, code string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.cleanup[claim.Admission.AdmissionID]
	current, active := s.items[claim.Admission.AdmissionID]
	if !ok || !active || !runtimeSessionAdmissionCleanupIdentityMatches(record.Admission, claim.Admission) || !runtimeSessionAdmissionCleanupIdentityMatches(current, claim.Admission) || !stringInRuntime(current.State, []string{"released", "expired"}) || record.Status != "running" || record.OwnerID != claim.OwnerID || record.Fence != claim.Fence || !record.ExpiresAt.Equal(claim.ExpiresAt) || !record.ExpiresAt.After(s.now()) {
		return fmt.Errorf("STALE_FENCING_TOKEN")
	}
	record.Status = "pending"
	record.OwnerID = ""
	record.ExpiresAt = time.Time{}
	record.LastError = code
	record.NextTryAt = s.now().Add(runtimeSessionTerminalCleanupRetryDelay(claim.Attempt))
	s.cleanup[claim.Admission.AdmissionID] = record
	return nil
}

func (s *RuntimeSessionAdmissionService) claimAdmissionCleanup(ctx context.Context, workerID string, ttl time.Duration) (runtimeSessionAdmissionCleanupClaim, bool, error) {
	if strings.TrimSpace(workerID) == "" || ttl <= 0 {
		return runtimeSessionAdmissionCleanupClaim{}, false, fmt.Errorf("INVALID_ARGUMENT")
	}
	row := s.DB.Pool.QueryRow(ctx, `
with candidate as (
  select o.admission_id
  from runtime_session_admission_cleanup_outbox o
  join runtime_session_admissions a on a.admission_id=o.admission_id
  where (
    (o.status='pending' and o.next_attempt_at<=now()) or
    (o.status='running' and o.lease_expires_at<=now())
  )
    and a.state in ('released','expired')
    and o.run_id=a.run_id
    and o.owner_instance_id=a.owner_instance_id
    and o.lease_token_hash=a.lease_token_hash
    and o.fencing_token=a.fencing_token
  order by o.next_attempt_at,o.created_at
  for update of o,a skip locked
  limit 1
)
update runtime_session_admission_cleanup_outbox o
set status='running',lease_owner=$1,lease_fencing_token=o.lease_fencing_token+1,
    lease_expires_at=now()+$2::interval,attempt_count=o.attempt_count+1,updated_at=now()
from candidate, runtime_session_admissions a
where o.admission_id=candidate.admission_id and a.admission_id=o.admission_id
returning o.lease_owner,o.lease_fencing_token,o.lease_expires_at,o.attempt_count,o.cleanup_origin,o.release_reason,
          a.admission_id,a.tenant_id,a.thread_id,a.agent_profile,a.context_generation,a.session_generation,
          a.binding_id,a.run_id,a.owner_instance_id,a.lease_token_hash,a.fencing_token,a.state,
          coalesce(a.reservation_id,''),coalesce(a.dispatch_id,''),a.expires_at,a.last_renewed_at,a.version,
          a.created_at,a.updated_at`, workerID, fmt.Sprintf("%f seconds", ttl.Seconds()))
	claim, err := scanRuntimeSessionAdmissionCleanupClaim(row.Scan)
	if err == pgx.ErrNoRows {
		return runtimeSessionAdmissionCleanupClaim{}, false, nil
	}
	if err != nil {
		return runtimeSessionAdmissionCleanupClaim{}, false, err
	}
	return claim, true, nil
}

func (s *RuntimeSessionAdmissionService) completeAdmissionCleanup(ctx context.Context, claim runtimeSessionAdmissionCleanupClaim) error {
	result, err := s.DB.Pool.Exec(ctx, `
update runtime_session_admission_cleanup_outbox o
set status='succeeded',lease_owner=null,lease_expires_at=null,last_error_code=null,
    completed_at=coalesce(completed_at,now()),updated_at=now()
from runtime_session_admissions a
where o.admission_id=$1 and o.status='running' and o.lease_owner=$2
  and o.lease_fencing_token=$3 and o.lease_expires_at=$4 and o.lease_expires_at>now()
  and a.admission_id=o.admission_id and a.state in ('released','expired')
  and o.run_id=$5 and a.run_id=$5
  and o.owner_instance_id=$6 and a.owner_instance_id=$6
  and o.lease_token_hash=$7 and a.lease_token_hash=$7
  and o.fencing_token=$8 and a.fencing_token=$8`,
		claim.Admission.AdmissionID, claim.OwnerID, claim.Fence, claim.ExpiresAt,
		claim.Admission.RunID, claim.Admission.OwnerInstanceID, claim.Admission.LeaseTokenHash, claim.Admission.FencingToken)
	if err != nil {
		return err
	}
	if result.RowsAffected() != 1 {
		return fmt.Errorf("STALE_FENCING_TOKEN")
	}
	return nil
}

func (s *RuntimeSessionAdmissionService) retryAdmissionCleanup(ctx context.Context, claim runtimeSessionAdmissionCleanupClaim, code string) error {
	result, err := s.DB.Pool.Exec(ctx, `
update runtime_session_admission_cleanup_outbox o
set status='pending',lease_owner=null,lease_expires_at=null,last_error_code=$5,
    next_attempt_at=now()+$6::interval,updated_at=now()
from runtime_session_admissions a
where o.admission_id=$1 and o.status='running' and o.lease_owner=$2
  and o.lease_fencing_token=$3 and o.lease_expires_at=$4 and o.lease_expires_at>now()
  and a.admission_id=o.admission_id and a.state in ('released','expired')
  and o.run_id=$7 and a.run_id=$7
  and o.owner_instance_id=$8 and a.owner_instance_id=$8
  and o.lease_token_hash=$9 and a.lease_token_hash=$9
  and o.fencing_token=$10 and a.fencing_token=$10`,
		claim.Admission.AdmissionID, claim.OwnerID, claim.Fence, claim.ExpiresAt, code,
		fmt.Sprintf("%f seconds", runtimeSessionTerminalCleanupRetryDelay(claim.Attempt).Seconds()),
		claim.Admission.RunID, claim.Admission.OwnerInstanceID, claim.Admission.LeaseTokenHash, claim.Admission.FencingToken)
	if err != nil {
		return err
	}
	if result.RowsAffected() != 1 {
		return fmt.Errorf("STALE_FENCING_TOKEN")
	}
	return nil
}

func scanRuntimeSessionAdmissionCleanupClaim(scan func(...any) error) (runtimeSessionAdmissionCleanupClaim, error) {
	var claim runtimeSessionAdmissionCleanupClaim
	admission := RuntimeSessionAdmission{}
	err := scan(
		&claim.OwnerID, &claim.Fence, &claim.ExpiresAt, &claim.Attempt, &claim.Origin, &claim.Reason,
		&admission.AdmissionID, &admission.Key.TenantID, &admission.Key.ThreadID, &admission.Key.AgentProfile,
		&admission.Key.ContextGeneration, &admission.Key.SessionGeneration, &admission.BindingID, &admission.RunID,
		&admission.OwnerInstanceID, &admission.LeaseTokenHash, &admission.FencingToken, &admission.State,
		&admission.ReservationID, &admission.DispatchID, &admission.ExpiresAt, &admission.LastRenewedAt,
		&admission.Version, &admission.CreatedAt, &admission.UpdatedAt,
	)
	if err != nil {
		return runtimeSessionAdmissionCleanupClaim{}, err
	}
	claim.Admission = admission
	return claim, nil
}

// EnqueueTerminalLeaseCleanupInTx writes the durable post-commit Redis/Tair
// cleanup work inside the Runtime terminal transaction. It persists only the
// convergence and admission identities; the raw random lease token is never
// stored and the drain reconstructs a restricted proof from the terminal DB
// admission row.
func (s *RuntimeSessionAdmissionService) EnqueueTerminalLeaseCleanupInTx(ctx context.Context, tx *persistence.Tx, convergenceID, admissionID string) error {
	if s == nil || tx == nil || strings.TrimSpace(convergenceID) == "" || strings.TrimSpace(admissionID) == "" {
		return fmt.Errorf("INVALID_ARGUMENT")
	}
	if err := tx.Exec(ctx, `
insert into runtime_session_terminal_cleanup_outbox(convergence_id,admission_id,status,next_attempt_at)
values(@convergence,@admission,'pending',now())
on conflict(convergence_id) do nothing`, map[string]any{"convergence": convergenceID, "admission": admissionID}); err != nil {
		return err
	}
	rows, err := tx.Query(ctx, `
select o.admission_id,a.run_id as admission_run_id,c.run_id as convergence_run_id
from runtime_session_terminal_cleanup_outbox o
join runtime_session_admissions a on a.admission_id=o.admission_id
join runtime_terminal_convergences c on c.convergence_id=o.convergence_id
where o.convergence_id=@convergence
for update of o,a,c`, map[string]any{"convergence": convergenceID})
	if err != nil {
		return err
	}
	if len(rows) != 1 || fmt.Sprint(rows[0]["admission_id"]) != admissionID || fmt.Sprint(rows[0]["admission_run_id"]) != fmt.Sprint(rows[0]["convergence_run_id"]) {
		return fmt.Errorf("RUNTIME_SESSION_CLEANUP_CONFLICT")
	}
	return nil
}

// DrainTerminalLeaseCleanup claims terminal cleanup records only after the
// linked convergence and admission have committed terminal PostgreSQL state.
// A stale Redis proof is a successful cleanup outcome: the original owner can
// no longer be live, and the exact full proof prevents deleting a successor.
func (s *RuntimeSessionAdmissionService) DrainTerminalLeaseCleanup(ctx context.Context, workerID string, limit int) (RuntimeSessionTerminalCleanupReport, error) {
	if strings.TrimSpace(workerID) == "" || limit < 1 {
		return RuntimeSessionTerminalCleanupReport{}, fmt.Errorf("INVALID_ARGUMENT")
	}
	if limit > 500 {
		limit = 500
	}
	if !s.postgresReady() {
		return RuntimeSessionTerminalCleanupReport{}, nil
	}
	if s.Locks == nil {
		return RuntimeSessionTerminalCleanupReport{}, fmt.Errorf("RUNTIME_SESSION_CLEANUP_UNAVAILABLE")
	}
	report := RuntimeSessionTerminalCleanupReport{}
	for report.Claimed < limit {
		claim, found, err := s.claimTerminalLeaseCleanup(ctx, workerID, 30*time.Second)
		if err != nil || !found {
			return report, err
		}
		report.Claimed++
		proof, proofErr := runtimeSessionTerminalCleanupProof(claim.Admission)
		if proofErr != nil {
			if retryErr := s.retryTerminalLeaseCleanup(ctx, claim, runtimeSessionTerminalCleanupErrorCode(proofErr)); retryErr != nil {
				return report, retryErr
			}
			report.Retried++
			continue
		}
		releaseErr := s.Locks.ReleaseTerminalLeaseProof(ctx, proof)
		if releaseErr == nil || releaseErr.Error() == "STALE_FENCING_TOKEN" {
			if completeErr := s.completeTerminalLeaseCleanup(ctx, claim); completeErr != nil {
				return report, completeErr
			}
			if releaseErr != nil {
				report.Stale++
			}
			report.Completed++
			s.mu.Lock()
			delete(s.handles, claim.Admission.AdmissionID)
			s.mu.Unlock()
			continue
		}
		if retryErr := s.retryTerminalLeaseCleanup(ctx, claim, runtimeSessionTerminalCleanupErrorCode(releaseErr)); retryErr != nil {
			return report, retryErr
		}
		report.Retried++
	}
	return report, nil
}

func (s *RuntimeSessionAdmissionService) claimTerminalLeaseCleanup(ctx context.Context, workerID string, ttl time.Duration) (runtimeSessionTerminalCleanupClaim, bool, error) {
	if ttl <= 0 {
		return runtimeSessionTerminalCleanupClaim{}, false, fmt.Errorf("INVALID_ARGUMENT")
	}
	row := s.DB.Pool.QueryRow(ctx, `
with candidate as (
  select o.convergence_id
  from runtime_session_terminal_cleanup_outbox o
  join runtime_session_admissions a on a.admission_id=o.admission_id
  join runtime_terminal_convergences c on c.convergence_id=o.convergence_id
  where (
    (o.status='pending' and o.next_attempt_at<=now()) or
    (o.status='running' and o.lease_expires_at<=now())
  )
    and a.state in ('released','expired')
    and c.session_released_at is not null
    and c.queue_completed_at is not null
	and a.run_id=c.run_id
  order by o.next_attempt_at,o.created_at
  for update of o,a,c skip locked
  limit 1
)
update runtime_session_terminal_cleanup_outbox o
set status='running',lease_owner=$1,lease_fencing_token=o.lease_fencing_token+1,
    lease_expires_at=now()+$2::interval,attempt_count=o.attempt_count+1,updated_at=now()
from candidate, runtime_session_admissions a
where o.convergence_id=candidate.convergence_id and a.admission_id=o.admission_id
returning o.convergence_id,o.lease_owner,o.lease_fencing_token,o.lease_expires_at,o.attempt_count,
          a.admission_id,a.tenant_id,a.thread_id,a.agent_profile,a.context_generation,a.session_generation,
          a.binding_id,a.run_id,a.owner_instance_id,a.lease_token_hash,a.fencing_token,a.state,
          coalesce(a.reservation_id,''),coalesce(a.dispatch_id,''),a.expires_at,a.last_renewed_at,a.version,
          a.created_at,a.updated_at`, workerID, fmt.Sprintf("%f seconds", ttl.Seconds()))
	claim, err := scanRuntimeSessionTerminalCleanupClaim(row.Scan)
	if err == pgx.ErrNoRows {
		return runtimeSessionTerminalCleanupClaim{}, false, nil
	}
	if err != nil {
		return runtimeSessionTerminalCleanupClaim{}, false, err
	}
	return claim, true, nil
}

func (s *RuntimeSessionAdmissionService) completeTerminalLeaseCleanup(ctx context.Context, claim runtimeSessionTerminalCleanupClaim) error {
	result, err := s.DB.Pool.Exec(ctx, `
update runtime_session_terminal_cleanup_outbox o
set status='succeeded',lease_owner=null,lease_expires_at=null,last_error_code=null,
    completed_at=coalesce(completed_at,now()),updated_at=now()
from runtime_session_admissions a, runtime_terminal_convergences c
where o.convergence_id=$1 and o.status='running' and o.lease_owner=$2
  and o.lease_fencing_token=$3 and o.lease_expires_at=$4 and o.lease_expires_at>now()
  and a.admission_id=o.admission_id and a.admission_id=$5
  and a.state in ('released','expired')
  and c.convergence_id=o.convergence_id
  and a.run_id=c.run_id
  and c.session_released_at is not null and c.queue_completed_at is not null`,
		claim.ConvergenceID, claim.OwnerID, claim.FencingToken, claim.LeaseExpires, claim.Admission.AdmissionID)
	if err != nil {
		return err
	}
	if result.RowsAffected() != 1 {
		return fmt.Errorf("STALE_FENCING_TOKEN")
	}
	return nil
}

func (s *RuntimeSessionAdmissionService) retryTerminalLeaseCleanup(ctx context.Context, claim runtimeSessionTerminalCleanupClaim, code string) error {
	result, err := s.DB.Pool.Exec(ctx, `
update runtime_session_terminal_cleanup_outbox o
set status='pending',lease_owner=null,lease_expires_at=null,last_error_code=$5,
    next_attempt_at=now()+$6::interval,updated_at=now()
from runtime_session_admissions a, runtime_terminal_convergences c
where o.convergence_id=$1 and o.status='running' and o.lease_owner=$2
  and o.lease_fencing_token=$3 and o.lease_expires_at=$4 and o.lease_expires_at>now()
  and a.admission_id=o.admission_id and a.admission_id=$7
  and a.state in ('released','expired')
  and c.convergence_id=o.convergence_id
  and a.run_id=c.run_id
  and c.session_released_at is not null and c.queue_completed_at is not null`,
		claim.ConvergenceID, claim.OwnerID, claim.FencingToken, claim.LeaseExpires, code,
		fmt.Sprintf("%f seconds", runtimeSessionTerminalCleanupRetryDelay(claim.Attempt).Seconds()), claim.Admission.AdmissionID)
	if err != nil {
		return err
	}
	if result.RowsAffected() != 1 {
		return fmt.Errorf("STALE_FENCING_TOKEN")
	}
	return nil
}

func scanRuntimeSessionTerminalCleanupClaim(scan func(...any) error) (runtimeSessionTerminalCleanupClaim, error) {
	var claim runtimeSessionTerminalCleanupClaim
	admission := RuntimeSessionAdmission{}
	err := scan(
		&claim.ConvergenceID, &claim.OwnerID, &claim.FencingToken, &claim.LeaseExpires, &claim.Attempt,
		&admission.AdmissionID, &admission.Key.TenantID, &admission.Key.ThreadID, &admission.Key.AgentProfile,
		&admission.Key.ContextGeneration, &admission.Key.SessionGeneration, &admission.BindingID, &admission.RunID,
		&admission.OwnerInstanceID, &admission.LeaseTokenHash, &admission.FencingToken, &admission.State,
		&admission.ReservationID, &admission.DispatchID, &admission.ExpiresAt, &admission.LastRenewedAt,
		&admission.Version, &admission.CreatedAt, &admission.UpdatedAt,
	)
	if err != nil {
		return runtimeSessionTerminalCleanupClaim{}, err
	}
	claim.Admission = admission
	return claim, nil
}

func runtimeSessionTerminalCleanupProof(admission RuntimeSessionAdmission) (queue.TerminalLeaseReleaseProof, error) {
	key := admission.Key.RedisKey()
	if key == "" || admission.OwnerInstanceID == "" || admission.RunID == "" || admission.LeaseTokenHash == "" || admission.FencingToken < 1 || !stringInRuntime(admission.State, []string{"released", "expired"}) {
		return queue.TerminalLeaseReleaseProof{}, fmt.Errorf("INVALID_ARGUMENT")
	}
	return queue.TerminalLeaseReleaseProof{
		Key: key, Scope: key, OwnerInstanceID: admission.OwnerInstanceID, RunID: admission.RunID,
		TokenHash: admission.LeaseTokenHash, FencingToken: admission.FencingToken,
	}, nil
}

func runtimeSessionAdmissionCleanupProof(claim runtimeSessionAdmissionCleanupClaim) (queue.TerminalLeaseReleaseProof, error) {
	if !validRuntimeSessionAdmissionCleanupOrigin(claim.Origin) || !validRuntimeAdmissionReleaseReason(claim.Reason) {
		return queue.TerminalLeaseReleaseProof{}, fmt.Errorf("INVALID_ARGUMENT")
	}
	return runtimeSessionTerminalCleanupProof(claim.Admission)
}

func runtimeSessionAdmissionCleanupOrigin(reason string) string {
	switch strings.TrimSpace(reason) {
	case "orphaned", "recovered":
		return "orphan_recovery"
	case "lease_expired":
		return "lease_expiry"
	default:
		return "direct_release"
	}
}

func runtimeSessionAdmissionCleanupBackfillReason(state, reason string) string {
	reason = strings.TrimSpace(reason)
	if validRuntimeAdmissionReleaseReason(reason) {
		return reason
	}
	if state == "expired" {
		return "lease_expired"
	}
	return "recovered"
}

func validRuntimeSessionAdmissionCleanupOrigin(origin string) bool {
	return stringInRuntime(strings.TrimSpace(origin), []string{"direct_release", "orphan_recovery", "lease_expiry"})
}

func runtimeSessionAdmissionCleanupIdentityMatches(left, right RuntimeSessionAdmission) bool {
	return left.AdmissionID == right.AdmissionID && left.Key == right.Key && left.BindingID == right.BindingID &&
		left.RunID == right.RunID && left.OwnerInstanceID == right.OwnerInstanceID &&
		left.LeaseTokenHash == right.LeaseTokenHash && left.FencingToken == right.FencingToken
}

func runtimeSessionTerminalCleanupRetryDelay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	if attempt > 12 {
		attempt = 12
	}
	return time.Duration(attempt*5) * time.Second
}

func runtimeSessionTerminalCleanupErrorCode(err error) string {
	if err == nil {
		return "RUNTIME_SESSION_CLEANUP_FAILED"
	}
	switch err.Error() {
	case "DISTRIBUTED_LOCK_UNAVAILABLE":
		return "RUNTIME_SESSION_CLEANUP_UNAVAILABLE"
	case "INVALID_ARGUMENT":
		return "RUNTIME_SESSION_CLEANUP_INVALID_PROOF"
	default:
		return "RUNTIME_SESSION_CLEANUP_FAILED"
	}
}

func runtimeSessionAdmissionCleanupErrorCode(err error) string {
	return runtimeSessionTerminalCleanupErrorCode(err)
}

// ActiveHandleByRunID returns only a protected in-process capability handle.
// It is never reconstructed from PostgreSQL after restart; recovery then waits
// for lease expiry and uses a new fencing attempt.
func (s *RuntimeSessionAdmissionService) ActiveHandleByRunID(ctx context.Context, runID string) (RuntimeSessionAdmissionLease, error) {
	if strings.TrimSpace(runID) == "" {
		return RuntimeSessionAdmissionLease{}, fmt.Errorf("INVALID_ARGUMENT")
	}
	s.mu.Lock()
	var handle RuntimeSessionAdmissionLease
	for _, candidate := range s.handles {
		if candidate.Admission.RunID == runID {
			handle = candidate
			break
		}
	}
	s.mu.Unlock()
	if handle.Admission.AdmissionID == "" {
		return RuntimeSessionAdmissionLease{}, fmt.Errorf("RUNTIME_SESSION_ADMISSION_UNAVAILABLE")
	}
	if err := s.AssertActive(ctx, handle); err != nil {
		return RuntimeSessionAdmissionLease{}, err
	}
	return handle, nil
}

func (s *RuntimeSessionAdmissionService) ReleasedOrExpiredByRunID(ctx context.Context, runID string) (bool, error) {
	if strings.TrimSpace(runID) == "" {
		return false, fmt.Errorf("INVALID_ARGUMENT")
	}
	if s.postgresReady() {
		var state string
		err := s.DB.Pool.QueryRow(ctx, `select state from runtime_session_admissions where run_id=$1 order by created_at desc limit 1`, runID).Scan(&state)
		if err != nil {
			return false, err
		}
		return state == "released" || state == "expired", nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, item := range s.items {
		if item.RunID == runID {
			return item.State == "released" || item.State == "expired", nil
		}
	}
	return false, fmt.Errorf("NOT_FOUND")
}

func (s *RuntimeSessionAdmissionService) Recover(ctx context.Context, now time.Time, limit int) (RuntimeSessionAdmissionRecoveryReport, error) {
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}
	report := RuntimeSessionAdmissionRecoveryReport{}
	if s.postgresReady() {
		rows, err := s.DB.Pool.Query(ctx, `select a.admission_id,coalesce(a.reservation_id,''),coalesce(a.dispatch_id,''),coalesce(r.state,''),coalesce(d.state,'')
from runtime_session_admissions a
left join runtime_slot_reservations r on r.reservation_id=a.reservation_id
left join runtime_run_dispatches d on d.dispatch_id=a.dispatch_id
where a.state in('acquired','reservation_bound','dispatch_bound','recovering') and a.expires_at<=$1
order by a.expires_at limit $2`, now, limit)
		if err != nil {
			return report, err
		}
		type recoveryCandidate struct {
			id, reservationID, dispatchID, reservationState, dispatchState string
		}
		candidates := []recoveryCandidate{}
		for rows.Next() {
			var candidate recoveryCandidate
			if err := rows.Scan(&candidate.id, &candidate.reservationID, &candidate.dispatchID, &candidate.reservationState, &candidate.dispatchState); err != nil {
				rows.Close()
				return report, err
			}
			candidates = append(candidates, candidate)
		}
		rows.Close()
		for _, candidate := range candidates {
			report.Scanned++
			activeDispatch := stringInRuntime(candidate.dispatchState, []string{"created", "sent", "submit_unknown", "accepted", "materializing", "running", "finalizing", "recovering"})
			activeReservation := stringInRuntime(candidate.reservationState, []string{"reserved", "accepted", "running"})
			if activeDispatch || activeReservation {
				result, err := s.DB.Pool.Exec(ctx, `update runtime_session_admissions set state='recovering',expires_at=$2,version=version+1,updated_at=now()
where admission_id=$1 and state in('acquired','reservation_bound','dispatch_bound','recovering') and expires_at<=$3`, candidate.id, now.Add(30*time.Second), now)
				if err != nil {
					return report, err
				}
				if result.RowsAffected() == 1 {
					report.Recovering++
				}
				continue
			}
			expired, err := s.expireAdmissionAndEnqueueCleanup(ctx, candidate.id, now)
			if err != nil {
				return report, err
			}
			if expired {
				report.Expired++
			}
		}
		return report, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, item := range s.items {
		if report.Scanned >= limit {
			break
		}
		if runtimeSessionAdmissionActive(item.State) && !item.ExpiresAt.After(now) {
			report.Scanned++
			item.State, item.UpdatedAt = "expired", now
			item.Version++
			s.items[id] = item
			if err := s.ensureAdmissionCleanupMemoryLocked(item, "lease_expiry", "lease_expired"); err != nil {
				return report, err
			}
			report.Expired++
		}
	}
	return report, nil
}

// expireAdmissionAndEnqueueCleanup closes an expired admission only when the
// generic cleanup record can be stored in the same Product transaction.
func (s *RuntimeSessionAdmissionService) expireAdmissionAndEnqueueCleanup(ctx context.Context, admissionID string, now time.Time) (bool, error) {
	changed := false
	err := s.DB.WithTx(ctx, func(tx *persistence.Tx) error {
		rows, err := tx.Query(ctx, `
select admission_id,tenant_id,thread_id,agent_profile,context_generation,session_generation,binding_id,run_id,owner_instance_id,lease_token_hash,fencing_token,state,coalesce(reservation_id,''),coalesce(dispatch_id,''),expires_at,last_renewed_at,version,created_at,updated_at
from runtime_session_admissions
where admission_id=@id and state in('acquired','reservation_bound','dispatch_bound','recovering') and expires_at<=@now
for update`, map[string]any{"id": admissionID, "now": now})
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			return nil
		}
		if len(rows) != 1 {
			return fmt.Errorf("RUNTIME_SESSION_CLEANUP_CONFLICT")
		}
		admission, err := runtimeSessionAdmissionFromMap(rows[0])
		if err != nil {
			return err
		}
		if err := tx.Exec(ctx, `update runtime_session_admissions set state='expired',release_reason='lease_expired',version=version+1,updated_at=now() where admission_id=@id`, map[string]any{"id": admissionID}); err != nil {
			return err
		}
		admission.State = "expired"
		if err := s.enqueueAdmissionCleanupInTx(ctx, tx, admission, "lease_expiry", "lease_expired"); err != nil {
			return err
		}
		changed = true
		return nil
	})
	return changed, err
}

func runtimeSessionAdmissionFromMap(row map[string]any) (RuntimeSessionAdmission, error) {
	text := func(key string) (string, bool) {
		value, ok := row[key]
		if !ok || value == nil {
			return "", false
		}
		switch typed := value.(type) {
		case string:
			return strings.TrimSpace(typed), true
		case []byte:
			return strings.TrimSpace(string(typed)), true
		default:
			return strings.TrimSpace(fmt.Sprint(typed)), true
		}
	}
	timestamp := func(key string) (time.Time, bool) {
		value, ok := row[key].(time.Time)
		return value, ok && !value.IsZero()
	}
	admissionID, hasAdmissionID := text("admission_id")
	tenantID, hasTenantID := text("tenant_id")
	threadID, hasThreadID := text("thread_id")
	agentProfile, hasAgentProfile := text("agent_profile")
	bindingID, hasBindingID := text("binding_id")
	runID, hasRunID := text("run_id")
	ownerID, hasOwnerID := text("owner_instance_id")
	tokenHash, hasTokenHash := text("lease_token_hash")
	state, hasState := text("state")
	reservationID, _ := text("reservation_id")
	dispatchID, _ := text("dispatch_id")
	expiresAt, hasExpiresAt := timestamp("expires_at")
	lastRenewedAt, hasLastRenewedAt := timestamp("last_renewed_at")
	createdAt, hasCreatedAt := timestamp("created_at")
	updatedAt, hasUpdatedAt := timestamp("updated_at")
	admission := RuntimeSessionAdmission{
		AdmissionID: admissionID,
		Key: ProductSessionAdmissionKey{
			TenantID: tenantID, ThreadID: threadID, AgentProfile: agentProfile,
			ContextGeneration: runtimeHostInt64(row["context_generation"]), SessionGeneration: int(runtimeHostInt64(row["session_generation"])),
		},
		BindingID: bindingID, RunID: runID, OwnerInstanceID: ownerID, LeaseTokenHash: tokenHash,
		FencingToken: runtimeHostInt64(row["fencing_token"]), State: state,
		ReservationID: reservationID, DispatchID: dispatchID, ExpiresAt: expiresAt, LastRenewedAt: lastRenewedAt,
		Version: runtimeHostInt64(row["version"]), CreatedAt: createdAt, UpdatedAt: updatedAt,
	}
	if !hasAdmissionID || !hasTenantID || !hasThreadID || !hasAgentProfile || !hasBindingID || !hasRunID || !hasOwnerID || !hasTokenHash || !hasState || !hasExpiresAt || !hasLastRenewedAt || !hasCreatedAt || !hasUpdatedAt || validateProductSessionAdmissionKey(admission.Key) != nil || admission.FencingToken < 1 || admission.Version < 1 || !stringInRuntime(admission.State, []string{"acquired", "reservation_bound", "dispatch_bound", "recovering", "released", "expired"}) {
		return RuntimeSessionAdmission{}, fmt.Errorf("RUNTIME_SESSION_CLEANUP_CONFLICT")
	}
	return admission, nil
}

func (s *RuntimeSessionAdmissionService) now() time.Time {
	if s.Now != nil {
		return s.Now().UTC()
	}
	return time.Now().UTC()
}

func (s *RuntimeSessionAdmissionService) postgresReady() bool {
	return s != nil && s.DB != nil && !s.DB.Disabled && s.DB.Pool != nil
}

func validateProductSessionAdmissionKey(key ProductSessionAdmissionKey) error {
	if strings.TrimSpace(key.TenantID) == "" || strings.TrimSpace(key.ThreadID) == "" || strings.TrimSpace(key.AgentProfile) == "" || key.ContextGeneration < 1 || key.SessionGeneration < 1 {
		return fmt.Errorf("INVALID_ARGUMENT")
	}
	return nil
}

func validateRuntimeSessionAdmissionLease(handle RuntimeSessionAdmissionLease) error {
	if handle.Admission.AdmissionID == "" || handle.Admission.LeaseTokenHash == "" || handle.Admission.FencingToken < 1 || handle.Lease.Token == "" || handle.Lease.TokenHash != handle.Admission.LeaseTokenHash || handle.Lease.FencingToken != handle.Admission.FencingToken {
		return fmt.Errorf("INVALID_ARGUMENT")
	}
	return nil
}

func runtimeSessionAdmissionActive(state string) bool {
	return stringInRuntime(state, []string{"acquired", "reservation_bound", "dispatch_bound", "recovering"})
}

func runtimeAdmissionMatches(current, expected RuntimeSessionAdmission) bool {
	return current.AdmissionID == expected.AdmissionID && current.OwnerInstanceID == expected.OwnerInstanceID && current.RunID == expected.RunID && current.LeaseTokenHash == expected.LeaseTokenHash && current.FencingToken == expected.FencingToken
}

func validRuntimeAdmissionReleaseReason(reason string) bool {
	return stringInRuntime(strings.TrimSpace(reason), []string{"succeeded", "failed", "timeout", "aborted", "orphaned", "reservation_failed", "dispatch_failed", "lease_expired", "recovered"})
}

func stableRuntimeAdmissionID(key ProductSessionAdmissionKey, runID string, fencing int64) string {
	sum := sha256.Sum256([]byte(key.RedisKey() + "\x00" + runID + "\x00" + fmt.Sprint(fencing)))
	return "admission_" + hex.EncodeToString(sum[:])[:32]
}

func encodeRuntimeKeySegment(value string) string {
	const digits = "0123456789ABCDEF"
	var out strings.Builder
	for _, b := range []byte(value) {
		if b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= '0' && b <= '9' || b == '-' || b == '_' || b == '.' {
			out.WriteByte(b)
			continue
		}
		out.WriteByte('%')
		out.WriteByte(digits[b>>4])
		out.WriteByte(digits[b&15])
	}
	return out.String()
}
