package runtime

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"huahuoai/backend/source/internal/persistence"
)

const (
	runtimeHostRecoveryPrepared  = "prepared"
	runtimeHostRecoveryCompleted = "completed"
	// A recovery snapshot is a control-plane proof, not a bulk listing API.
	// One extra row is read so an incomplete truncated view is never attested.
	runtimeHostRecoveryMaxFacts = 512
)

// GetHostRecoverySnapshot reads a bounded, canonical, hash-only view of the
// current Host occupancy. Production-like callers require a durable store;
// process-local maps are only a test/local harness and never a fallback.
func (r *RuntimeHostRepository) GetHostRecoverySnapshot(ctx context.Context, principal RuntimeHostPrincipal) (RuntimeHostRecoverySnapshot, error) {
	if r == nil {
		return RuntimeHostRecoverySnapshot{}, fmt.Errorf("RUNTIME_STORAGE_UNAVAILABLE")
	}
	if r.postgresReady() {
		snapshot, err := r.hostRecoverySnapshotPostgres(ctx, principal)
		return snapshot, runtimeHostRecoveryStorageError(err)
	}
	if runtimeHostRecoveryAttestationRequired(principal.Environment) {
		return RuntimeHostRecoverySnapshot{}, fmt.Errorf("RUNTIME_STORAGE_UNAVAILABLE")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.hostRecoverySnapshotMemoryLocked(principal)
}

// NormalizeRuntimeHostRecoverySnapshot validates a Host-scoped external
// snapshot before it can be compared with Backend state or used to rebuild
// local admission. It returns the canonical fact order that participates in
// FactSetHash; callers must reject the original assertion when this fails.
func NormalizeRuntimeHostRecoverySnapshot(principal RuntimeHostPrincipal, snapshot RuntimeHostRecoverySnapshot) (RuntimeHostRecoverySnapshot, error) {
	if err := validateRuntimeHostRecoverySnapshotAssertion(principal, &snapshot); err != nil {
		return RuntimeHostRecoverySnapshot{}, err
	}
	return snapshot, nil
}

// CompareRuntimeHostRecoverySnapshots requires exact identity, generation,
// revision, state, hash, and fact equality. It is deliberately stricter than
// comparing counters: an absent, extra, stale, or differently bound Gateway
// fact leaves a restarted Host closed.
func CompareRuntimeHostRecoverySnapshots(asserted, current RuntimeHostRecoverySnapshot) error {
	return compareRuntimeHostRecoverySnapshot(asserted, current)
}

// BeginHostRecoveryAttestation is the SCM-defined recovery boundary. It has no
// caller-supplied identity outside the certificate-bound principal.
func (r *RuntimeHostRepository) BeginHostRecoveryAttestation(ctx context.Context, principal RuntimeHostPrincipal, asserted RuntimeHostRecoverySnapshot) (RuntimeHostRecoveryAttestation, error) {
	return r.BeginHostRecoveryAttestationWithCorrelation(ctx, principal, asserted, "")
}

// BeginHostRecoveryAttestationWithCorrelation atomically compares the
// caller's canonical Gateway/Backend fact assertion with durable Backend
// facts. Correlation is a bounded safe tracing value and never affects the
// attestation key or admission decision.
func (r *RuntimeHostRepository) BeginHostRecoveryAttestationWithCorrelation(ctx context.Context, principal RuntimeHostPrincipal, asserted RuntimeHostRecoverySnapshot, correlationID string) (RuntimeHostRecoveryAttestation, error) {
	if err := validateRuntimeHostRecoverySnapshotAssertion(principal, &asserted); err != nil {
		return RuntimeHostRecoveryAttestation{}, err
	}
	if !validRuntimeHostRecoveryCorrelationID(correlationID) {
		return RuntimeHostRecoveryAttestation{}, fmt.Errorf("INVALID_ARGUMENT")
	}
	if r == nil {
		return RuntimeHostRecoveryAttestation{}, fmt.Errorf("RUNTIME_STORAGE_UNAVAILABLE")
	}
	if r.postgresReady() {
		attestation, err := r.beginHostRecoveryAttestationPostgres(ctx, principal, asserted, correlationID)
		return attestation, runtimeHostRecoveryStorageError(err)
	}
	if runtimeHostRecoveryAttestationRequired(principal.Environment) {
		return RuntimeHostRecoveryAttestation{}, fmt.Errorf("RUNTIME_STORAGE_UNAVAILABLE")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	current, err := r.hostRecoverySnapshotMemoryLocked(principal)
	if err != nil {
		return RuntimeHostRecoveryAttestation{}, err
	}
	if err := compareRuntimeHostRecoverySnapshot(asserted, current); err != nil {
		return RuntimeHostRecoveryAttestation{}, err
	}
	key := runtimeHostRecoveryAttestationKey(current)
	if existing, ok := r.recoveryAttestations[key]; ok {
		return existing, nil
	}
	host := r.hosts[principal.RuntimeHostID]
	if host.Status != "registering" || host.RecoveryState != "pending" {
		return RuntimeHostRecoveryAttestation{}, fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
	}
	attestationID, err := newRuntimeHostRecoveryAttestationID()
	if err != nil {
		return RuntimeHostRecoveryAttestation{}, fmt.Errorf("RUNTIME_STORAGE_UNAVAILABLE: %w", err)
	}
	attestation := RuntimeHostRecoveryAttestation{
		AttestationID: attestationID, RuntimeHostID: principal.RuntimeHostID, InstanceID: principal.InstanceID,
		InstanceGeneration: current.InstanceGeneration, RecoveryRevision: current.RecoveryRevision,
		FactSetHash: current.FactSetHash, State: runtimeHostRecoveryPrepared, CorrelationID: correlationID,
		CreatedAt: time.Now().UTC(),
	}
	if r.recoveryAttestations == nil {
		r.recoveryAttestations = map[string]RuntimeHostRecoveryAttestation{}
	}
	r.recoveryAttestations[key] = attestation
	return attestation, nil
}

// CompleteHostRecoveryAttestation repeats the fact-set compare-and-set under
// the same Host/occupancy locks. No heartbeat, counter reconciliation, or
// zero-counter observation can invoke this transition.
func (r *RuntimeHostRepository) CompleteHostRecoveryAttestation(ctx context.Context, principal RuntimeHostPrincipal, attestationID string) (RuntimeHostRecoveryAttestation, error) {
	if strings.TrimSpace(attestationID) == "" || attestationID != strings.TrimSpace(attestationID) {
		return RuntimeHostRecoveryAttestation{}, fmt.Errorf("INVALID_ARGUMENT")
	}
	if r == nil {
		return RuntimeHostRecoveryAttestation{}, fmt.Errorf("RUNTIME_STORAGE_UNAVAILABLE")
	}
	if r.postgresReady() {
		attestation, err := r.completeHostRecoveryAttestationPostgres(ctx, principal, attestationID)
		return attestation, runtimeHostRecoveryStorageError(err)
	}
	if runtimeHostRecoveryAttestationRequired(principal.Environment) {
		return RuntimeHostRecoveryAttestation{}, fmt.Errorf("RUNTIME_STORAGE_UNAVAILABLE")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	var attestation RuntimeHostRecoveryAttestation
	found := false
	for _, candidate := range r.recoveryAttestations {
		if candidate.AttestationID == attestationID && candidate.RuntimeHostID == principal.RuntimeHostID {
			attestation = candidate
			found = true
			break
		}
	}
	if !found {
		return RuntimeHostRecoveryAttestation{}, fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
	}
	current, err := r.hostRecoverySnapshotMemoryLocked(principal)
	if err != nil {
		return RuntimeHostRecoveryAttestation{}, err
	}
	if err := compareRuntimeHostRecoveryAttestation(attestation, current, principal); err != nil {
		return RuntimeHostRecoveryAttestation{}, err
	}
	host := r.hosts[principal.RuntimeHostID]
	if attestation.State == runtimeHostRecoveryCompleted {
		if host.Status != "ready" || host.RecoveryState != "reconciled" {
			return RuntimeHostRecoveryAttestation{}, fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
		}
		return attestation, nil
	}
	if attestation.State != runtimeHostRecoveryPrepared || host.Status != "registering" || host.RecoveryState != "pending" {
		return RuntimeHostRecoveryAttestation{}, fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
	}
	now := time.Now().UTC()
	host.Status = "ready"
	host.RecoveryState = "reconciled"
	host.UpdatedAt = now
	r.hosts[host.RuntimeHostID] = host
	attestation.State = runtimeHostRecoveryCompleted
	attestation.CompletedAt = &now
	r.recoveryAttestations[runtimeHostRecoveryAttestationKey(current)] = attestation
	return attestation, nil
}

func (r *RuntimeHostRepository) hostRecoverySnapshotMemoryLocked(principal RuntimeHostPrincipal) (RuntimeHostRecoverySnapshot, error) {
	host, ok := r.hosts[principal.RuntimeHostID]
	if !ok || host.InstanceID != principal.InstanceID || host.Environment != principal.Environment {
		return RuntimeHostRecoverySnapshot{}, fmt.Errorf("RUNTIME_HOST_UNAUTHORIZED")
	}
	return runtimeHostRecoverySnapshotFromMaps(host, r.reservations, r.dispatches)
}

func (r *RuntimeHostRepository) hostRecoverySnapshotPostgres(ctx context.Context, principal RuntimeHostPrincipal) (RuntimeHostRecoverySnapshot, error) {
	var snapshot RuntimeHostRecoverySnapshot
	err := r.db.WithTx(ctx, func(tx *persistence.Tx) error {
		host, err := runtimeHostRecoveryHostPostgres(ctx, tx, principal)
		if err != nil {
			return err
		}
		reservations, dispatches, err := runtimeHostRecoveryFactsPostgres(ctx, tx, host)
		if err != nil {
			return err
		}
		snapshot, err = runtimeHostRecoverySnapshotFromFacts(host, reservations, dispatches)
		return err
	})
	return snapshot, err
}

func (r *RuntimeHostRepository) beginHostRecoveryAttestationPostgres(ctx context.Context, principal RuntimeHostPrincipal, asserted RuntimeHostRecoverySnapshot, correlationID string) (RuntimeHostRecoveryAttestation, error) {
	var result RuntimeHostRecoveryAttestation
	err := r.db.WithTx(ctx, func(tx *persistence.Tx) error {
		host, err := runtimeHostRecoveryHostPostgres(ctx, tx, principal)
		if err != nil {
			return err
		}
		reservations, dispatches, err := runtimeHostRecoveryFactsPostgres(ctx, tx, host)
		if err != nil {
			return err
		}
		current, err := runtimeHostRecoverySnapshotFromFacts(host, reservations, dispatches)
		if err != nil {
			return err
		}
		if err := compareRuntimeHostRecoverySnapshot(asserted, current); err != nil {
			return err
		}
		rows, err := tx.Query(ctx, `select attestation_id,runtime_host_id,instance_id,instance_generation,recovery_revision,fact_set_hash,state,correlation_id,created_at,completed_at
from runtime_host_recovery_attestations
where runtime_host_id=@host and instance_generation=@generation and recovery_revision=@revision and fact_set_hash=@hash
for update`, map[string]any{
			"host": principal.RuntimeHostID, "generation": current.InstanceGeneration,
			"revision": current.RecoveryRevision, "hash": current.FactSetHash,
		})
		if err != nil {
			return err
		}
		if len(rows) == 1 {
			result, err = runtimeHostRecoveryAttestationFromMap(rows[0])
			return err
		}
		if len(rows) != 0 {
			return fmt.Errorf("RUNTIME_STORAGE_UNAVAILABLE")
		}
		if host.Status != "registering" || host.RecoveryState != "pending" {
			return fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
		}
		attestationID, err := newRuntimeHostRecoveryAttestationID()
		if err != nil {
			return fmt.Errorf("RUNTIME_STORAGE_UNAVAILABLE: %w", err)
		}
		result = RuntimeHostRecoveryAttestation{
			AttestationID: attestationID, RuntimeHostID: principal.RuntimeHostID, InstanceID: principal.InstanceID,
			InstanceGeneration: current.InstanceGeneration, RecoveryRevision: current.RecoveryRevision,
			FactSetHash: current.FactSetHash, State: runtimeHostRecoveryPrepared, CorrelationID: correlationID,
			CreatedAt: time.Now().UTC(),
		}
		return tx.Exec(ctx, `insert into runtime_host_recovery_attestations(attestation_id,runtime_host_id,instance_id,instance_generation,recovery_revision,fact_set_hash,state,correlation_id,created_at)
values(@id,@host,@instance,@generation,@revision,@hash,'prepared',@correlation,@created)`, map[string]any{
			"id": result.AttestationID, "host": result.RuntimeHostID, "instance": result.InstanceID,
			"generation": result.InstanceGeneration, "revision": result.RecoveryRevision, "hash": result.FactSetHash,
			"correlation": result.CorrelationID, "created": result.CreatedAt,
		})
	})
	return result, err
}

func (r *RuntimeHostRepository) completeHostRecoveryAttestationPostgres(ctx context.Context, principal RuntimeHostPrincipal, attestationID string) (RuntimeHostRecoveryAttestation, error) {
	var result RuntimeHostRecoveryAttestation
	err := r.db.WithTx(ctx, func(tx *persistence.Tx) error {
		host, err := runtimeHostRecoveryHostPostgres(ctx, tx, principal)
		if err != nil {
			return err
		}
		rows, err := tx.Query(ctx, `select attestation_id,runtime_host_id,instance_id,instance_generation,recovery_revision,fact_set_hash,state,correlation_id,created_at,completed_at
from runtime_host_recovery_attestations where attestation_id=@id and runtime_host_id=@host for update`, map[string]any{"id": attestationID, "host": principal.RuntimeHostID})
		if err != nil {
			return err
		}
		if len(rows) != 1 {
			return fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
		}
		result, err = runtimeHostRecoveryAttestationFromMap(rows[0])
		if err != nil {
			return err
		}
		reservations, dispatches, err := runtimeHostRecoveryFactsPostgres(ctx, tx, host)
		if err != nil {
			return err
		}
		current, err := runtimeHostRecoverySnapshotFromFacts(host, reservations, dispatches)
		if err != nil {
			return err
		}
		if err := compareRuntimeHostRecoveryAttestation(result, current, principal); err != nil {
			return err
		}
		if result.State == runtimeHostRecoveryCompleted {
			if host.Status != "ready" || host.RecoveryState != "reconciled" {
				return fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
			}
			return nil
		}
		if result.State != runtimeHostRecoveryPrepared || host.Status != "registering" || host.RecoveryState != "pending" {
			return fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
		}
		tag, err := tx.ExecRaw(ctx, `update runtime_hosts set status='ready',recovery_state='reconciled',updated_at=now()
where runtime_host_id=$1 and instance_id=$2 and environment=$3 and instance_generation=$4 and recovery_revision=$5
and status='registering' and recovery_state='pending'`, principal.RuntimeHostID, principal.InstanceID, principal.Environment, result.InstanceGeneration, result.RecoveryRevision)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
		}
		now := time.Now().UTC()
		if err := tx.Exec(ctx, `update runtime_host_recovery_attestations set state='completed',completed_at=@completed
where attestation_id=@id and state='prepared'`, map[string]any{"id": result.AttestationID, "completed": now}); err != nil {
			return err
		}
		result.State = runtimeHostRecoveryCompleted
		result.CompletedAt = &now
		return nil
	})
	return result, err
}

func runtimeHostRecoveryHostPostgres(ctx context.Context, tx *persistence.Tx, principal RuntimeHostPrincipal) (RuntimeHost, error) {
	rows, err := tx.Query(ctx, runtimeHostSelect+` where runtime_host_id=@host for update`, map[string]any{"host": principal.RuntimeHostID})
	if err != nil {
		return RuntimeHost{}, err
	}
	if len(rows) != 1 {
		return RuntimeHost{}, fmt.Errorf("RUNTIME_HOST_UNAUTHORIZED")
	}
	host, err := runtimeHostFromMap(rows[0])
	if err != nil {
		return RuntimeHost{}, err
	}
	if host.InstanceID != principal.InstanceID || host.Environment != principal.Environment {
		return RuntimeHost{}, fmt.Errorf("RUNTIME_HOST_UNAUTHORIZED")
	}
	if host.InstanceGeneration < 1 || host.RecoveryRevision < 1 {
		return RuntimeHost{}, fmt.Errorf("RUNTIME_STORAGE_UNAVAILABLE")
	}
	return host, nil
}

func runtimeHostRecoveryFactsPostgres(ctx context.Context, tx *persistence.Tx, host RuntimeHost) ([]RuntimeSlotReservation, []RuntimeDispatch, error) {
	reservationRows, err := tx.Query(ctx, `select reservation_id,run_id,runtime_host_id,coalesce(assigned_runtime_host_instance_id,'') assigned_runtime_host_instance_id,coalesce(assigned_runtime_host_instance_generation,0) assigned_runtime_host_instance_generation,owner_instance_id,state,fencing_token,lease_token_hash,capability_hash,execution_scope,execution_scope_source,coalesce(dispatch_id,'') dispatch_id,expires_at,last_renewed_at,version,created_at,updated_at
from runtime_slot_reservations
where runtime_host_id=@host and state in ('reserved','accepted','running')
order by reservation_id limit 513 for update`, map[string]any{"host": host.RuntimeHostID})
	if err != nil {
		return nil, nil, err
	}
	reservations := make([]RuntimeSlotReservation, 0, len(reservationRows))
	for _, row := range reservationRows {
		reservation, err := runtimeReservationFromMap(row)
		if err != nil {
			return nil, nil, err
		}
		reservations = append(reservations, reservation)
	}
	if len(reservations) > runtimeHostRecoveryMaxFacts {
		return nil, nil, fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
	}
	dispatchRows, err := tx.Query(ctx, `select dispatch_id,run_id,reservation_id,runtime_host_id,coalesce(assigned_runtime_host_instance_id,'') assigned_runtime_host_instance_id,coalesce(assigned_runtime_host_instance_generation,0) assigned_runtime_host_instance_generation,coalesce(dispatch_identity,'') dispatch_identity,state,fencing_token,run_ticket_jti_hash,input_manifest_hash,event_cursor,event_lower_bound,coalesce(event_upper_bound,0) event_upper_bound,coalesce(event_gap_expected_sequence,0) event_gap_expected_sequence,coalesce(event_gap_observed_sequence,0) event_gap_observed_sequence
from runtime_run_dispatches
where runtime_host_id=@host and state in ('created','sent','submit_unknown','retry_same_host','accepted','materializing','running','finalizing','recovering')
order by dispatch_id limit 513 for update`, map[string]any{"host": host.RuntimeHostID})
	if err != nil {
		return nil, nil, err
	}
	dispatches := make([]RuntimeDispatch, 0, len(dispatchRows))
	for _, row := range dispatchRows {
		dispatches = append(dispatches, runtimeHostRecoveryDispatchFromMap(row))
	}
	if len(dispatches) > runtimeHostRecoveryMaxFacts {
		return nil, nil, fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
	}
	return reservations, dispatches, nil
}

func runtimeHostRecoverySnapshotFromMaps(host RuntimeHost, reservations map[string]RuntimeSlotReservation, dispatches map[string]RuntimeDispatch) (RuntimeHostRecoverySnapshot, error) {
	items := make([]RuntimeSlotReservation, 0, len(reservations))
	for _, reservation := range reservations {
		items = append(items, reservation)
	}
	runs := make([]RuntimeDispatch, 0, len(dispatches))
	for _, dispatch := range dispatches {
		runs = append(runs, dispatch)
	}
	return runtimeHostRecoverySnapshotFromFacts(host, items, runs)
}

func runtimeHostRecoverySnapshotFromFacts(host RuntimeHost, reservations []RuntimeSlotReservation, dispatches []RuntimeDispatch) (RuntimeHostRecoverySnapshot, error) {
	facts, err := runtimeHostRecoveryFacts(host, reservations, dispatches)
	if err != nil {
		return RuntimeHostRecoverySnapshot{}, err
	}
	canonicalFacts, factSetHash, err := canonicalRuntimeHostRecoveryFacts(facts)
	if err != nil {
		return RuntimeHostRecoverySnapshot{}, err
	}
	return RuntimeHostRecoverySnapshot{
		RuntimeHostID: host.RuntimeHostID, InstanceID: host.InstanceID, Environment: host.Environment,
		InstanceGeneration: host.InstanceGeneration, RecoveryRevision: host.RecoveryRevision,
		RecoveryState: host.RecoveryState, Facts: canonicalFacts, FactSetHash: factSetHash,
	}, nil
}

func runtimeHostRecoveryFacts(host RuntimeHost, reservations []RuntimeSlotReservation, dispatches []RuntimeDispatch) ([]RuntimeHostRecoveryFact, error) {
	if host.RuntimeHostID == "" || host.InstanceID == "" || host.Environment == "" || host.InstanceGeneration < 1 || host.RecoveryRevision < 1 {
		return nil, fmt.Errorf("RUNTIME_STORAGE_UNAVAILABLE")
	}
	byReservation := make(map[string]RuntimeSlotReservation, len(reservations))
	for _, reservation := range reservations {
		if !activeRuntimeReservationState(reservation.State) {
			continue
		}
		if err := validateRuntimeHostRecoveryReservation(host, reservation); err != nil {
			return nil, err
		}
		if _, exists := byReservation[reservation.ReservationID]; exists {
			return nil, fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
		}
		byReservation[reservation.ReservationID] = reservation
	}
	byDispatch := make(map[string]RuntimeDispatch, len(dispatches))
	for _, dispatch := range dispatches {
		if !activeRuntimeDispatchState(dispatch.State) {
			continue
		}
		if dispatch.EventGapExpectedSequence != 0 || dispatch.EventGapObservedSequence != 0 || dispatch.EventUpperBound > dispatch.EventCursor {
			return nil, fmt.Errorf("RUNTIME_EVENT_GAP")
		}
		if _, exists := byDispatch[dispatch.ReservationID]; exists {
			return nil, fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
		}
		byDispatch[dispatch.ReservationID] = dispatch
	}
	facts := make([]RuntimeHostRecoveryFact, 0, len(byReservation))
	for reservationID, reservation := range byReservation {
		fact := RuntimeHostRecoveryFact{
			RunID: reservation.RunID, RuntimeHostID: reservation.RuntimeHostID,
			AssignedRuntimeHostInstanceID:         reservation.AssignedRuntimeHostInstanceID,
			AssignedRuntimeHostInstanceGeneration: reservation.AssignedRuntimeHostInstanceGeneration,
			ReservationID:                         reservation.ReservationID, FencingToken: reservation.FencingToken,
			ExecutionScope: reservation.ExecutionScope, CapabilityHash: reservation.CapabilityHash,
			Status: reservation.State,
		}
		if dispatch, ok := byDispatch[reservationID]; ok {
			if err := validateRuntimeHostRecoveryDispatch(host, reservation, dispatch); err != nil {
				return nil, err
			}
			fact.DispatchID = dispatch.DispatchID
			fact.DispatchIdentity = dispatch.DispatchIdentity
			fact.RunTicketJTIHash = dispatch.RunTicketJTIHash
			fact.ManifestHash = dispatch.InputManifestHash
			fact.Status = dispatch.State
			fact.LastEventSequence = dispatch.EventCursor
			delete(byDispatch, reservationID)
		}
		facts = append(facts, fact)
	}
	// An active dispatch without an active, assignment-bound reservation cannot
	// safely reconstruct occupancy. Do not infer scope from a legacy row.
	if len(byDispatch) != 0 {
		return nil, fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
	}
	if len(facts) > runtimeHostRecoveryMaxFacts {
		return nil, fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
	}
	return facts, nil
}

func validateRuntimeHostRecoveryReservation(host RuntimeHost, reservation RuntimeSlotReservation) error {
	if reservation.ReservationID == "" || reservation.RunID == "" || reservation.RuntimeHostID != host.RuntimeHostID ||
		reservation.AssignedRuntimeHostInstanceID == "" || reservation.AssignedRuntimeHostInstanceGeneration < 1 ||
		reservation.AssignedRuntimeHostInstanceID != host.InstanceID || reservation.AssignedRuntimeHostInstanceGeneration != host.InstanceGeneration ||
		reservation.FencingToken < 1 || reservation.CapabilityHash == "" || !validRuntimeExecutionScope(reservation.ExecutionScope) {
		return fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
	}
	return nil
}

func validateRuntimeHostRecoveryDispatch(host RuntimeHost, reservation RuntimeSlotReservation, dispatch RuntimeDispatch) error {
	if dispatch.DispatchID == "" || dispatch.RunID != reservation.RunID || dispatch.ReservationID != reservation.ReservationID ||
		dispatch.RuntimeHostID != host.RuntimeHostID || dispatch.FencingToken != reservation.FencingToken ||
		dispatch.AssignedRuntimeHostInstanceID == "" || dispatch.AssignedRuntimeHostInstanceGeneration < 1 ||
		dispatch.AssignedRuntimeHostInstanceID != reservation.AssignedRuntimeHostInstanceID ||
		dispatch.AssignedRuntimeHostInstanceGeneration != reservation.AssignedRuntimeHostInstanceGeneration ||
		dispatch.AssignedRuntimeHostInstanceID != host.InstanceID || dispatch.AssignedRuntimeHostInstanceGeneration != host.InstanceGeneration ||
		!runtimeHostRecoverySHA256(dispatch.RunTicketJTIHash) || !runtimeHostRecoverySHA256(dispatch.InputManifestHash) || !runtimeHostRecoverySHA256(dispatch.DispatchIdentity) {
		return fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
	}
	expected, err := canonicalRuntimeDispatchIdentity(reservation, dispatch)
	if err != nil || expected != dispatch.DispatchIdentity {
		return fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
	}
	return nil
}

func bindDispatchToReservation(dispatch *RuntimeDispatch, reservation RuntimeSlotReservation) error {
	if dispatch == nil || reservation.ReservationID == "" || dispatch.ReservationID != reservation.ReservationID ||
		dispatch.RunID != reservation.RunID || dispatch.RuntimeHostID != reservation.RuntimeHostID ||
		dispatch.FencingToken != reservation.FencingToken || dispatch.OwnerInstanceID != reservation.OwnerInstanceID ||
		dispatch.LeaseTokenHash != reservation.LeaseTokenHash || reservation.AssignedRuntimeHostInstanceID == "" ||
		reservation.AssignedRuntimeHostInstanceGeneration < 1 {
		return fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
	}
	dispatch.AssignedRuntimeHostInstanceID = reservation.AssignedRuntimeHostInstanceID
	dispatch.AssignedRuntimeHostInstanceGeneration = reservation.AssignedRuntimeHostInstanceGeneration
	identity, err := canonicalRuntimeDispatchIdentity(reservation, *dispatch)
	if err != nil {
		return err
	}
	dispatch.DispatchIdentity = identity
	return nil
}

func canonicalRuntimeDispatchIdentity(reservation RuntimeSlotReservation, dispatch RuntimeDispatch) (string, error) {
	if reservation.ReservationID == "" || reservation.RunID == "" || reservation.RuntimeHostID == "" ||
		reservation.AssignedRuntimeHostInstanceID == "" || reservation.AssignedRuntimeHostInstanceGeneration < 1 ||
		reservation.FencingToken < 1 || reservation.CapabilityHash == "" || !validRuntimeExecutionScope(reservation.ExecutionScope) ||
		dispatch.DispatchID == "" || dispatch.RunID != reservation.RunID || dispatch.ReservationID != reservation.ReservationID ||
		dispatch.RuntimeHostID != reservation.RuntimeHostID || dispatch.FencingToken != reservation.FencingToken ||
		dispatch.AssignedRuntimeHostInstanceID != reservation.AssignedRuntimeHostInstanceID ||
		dispatch.AssignedRuntimeHostInstanceGeneration != reservation.AssignedRuntimeHostInstanceGeneration ||
		dispatch.RunTicketJTIHash == "" || dispatch.InputManifestHash == "" {
		return "", fmt.Errorf("INVALID_ARGUMENT")
	}
	payload, err := json.Marshal(struct {
		Version                               string `json:"version"`
		RunID                                 string `json:"runId"`
		RuntimeHostID                         string `json:"runtimeHostId"`
		AssignedRuntimeHostInstanceID         string `json:"assignedRuntimeHostInstanceId"`
		AssignedRuntimeHostInstanceGeneration int64  `json:"assignedRuntimeHostInstanceGeneration"`
		ReservationID                         string `json:"reservationId"`
		DispatchID                            string `json:"dispatchId"`
		FencingToken                          int64  `json:"fencingToken"`
		ExecutionScope                        string `json:"executionScope"`
		CapabilityHash                        string `json:"capabilityHash"`
		RunTicketJTIHash                      string `json:"runTicketJtiHash"`
		ManifestHash                          string `json:"manifestHash"`
	}{
		Version: "runtime-host-recovery.v1", RunID: reservation.RunID, RuntimeHostID: reservation.RuntimeHostID,
		AssignedRuntimeHostInstanceID:         reservation.AssignedRuntimeHostInstanceID,
		AssignedRuntimeHostInstanceGeneration: reservation.AssignedRuntimeHostInstanceGeneration,
		ReservationID:                         reservation.ReservationID, DispatchID: dispatch.DispatchID, FencingToken: reservation.FencingToken,
		ExecutionScope: reservation.ExecutionScope, CapabilityHash: reservation.CapabilityHash,
		RunTicketJTIHash: dispatch.RunTicketJTIHash, ManifestHash: dispatch.InputManifestHash,
	})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

func canonicalRuntimeHostRecoveryFacts(facts []RuntimeHostRecoveryFact) ([]RuntimeHostRecoveryFact, string, error) {
	// The cross-runtime recovery proof is JSON-hashed. Preserve an empty fact
	// set as [] rather than null so Go and the Gateway produce the same proof.
	canonical := make([]RuntimeHostRecoveryFact, len(facts))
	copy(canonical, facts)
	for _, fact := range canonical {
		if err := validateRuntimeHostRecoveryFact(fact); err != nil {
			return nil, "", err
		}
	}
	sort.Slice(canonical, func(i, j int) bool {
		if canonical[i].RunID != canonical[j].RunID {
			return canonical[i].RunID < canonical[j].RunID
		}
		if canonical[i].DispatchID != canonical[j].DispatchID {
			return canonical[i].DispatchID < canonical[j].DispatchID
		}
		return canonical[i].ReservationID < canonical[j].ReservationID
	})
	for index := range canonical {
		if index > 0 && canonical[index-1].RunID == canonical[index].RunID && canonical[index-1].DispatchID == canonical[index].DispatchID && canonical[index-1].ReservationID == canonical[index].ReservationID {
			return nil, "", fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
		}
	}
	payload, err := json.Marshal(struct {
		Version string                    `json:"version"`
		Facts   []RuntimeHostRecoveryFact `json:"facts"`
	}{Version: "runtime-host-recovery.v1", Facts: canonical})
	if err != nil {
		return nil, "", err
	}
	sum := sha256.Sum256(payload)
	return canonical, "sha256:" + hex.EncodeToString(sum[:]), nil
}

func validateRuntimeHostRecoveryFact(fact RuntimeHostRecoveryFact) error {
	if !canonicalRuntimeHostRecoveryString(fact.RunID) || !canonicalRuntimeHostRecoveryString(fact.RuntimeHostID) ||
		!canonicalRuntimeHostRecoveryString(fact.AssignedRuntimeHostInstanceID) || fact.AssignedRuntimeHostInstanceGeneration < 1 ||
		!canonicalRuntimeHostRecoveryString(fact.ReservationID) || fact.FencingToken < 1 ||
		!validRuntimeExecutionScope(fact.ExecutionScope) || !canonicalRuntimeHostRecoveryString(fact.CapabilityHash) ||
		!canonicalRuntimeHostRecoveryString(fact.Status) || fact.LastEventSequence < 0 {
		return fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
	}
	if fact.DispatchID == "" {
		if fact.DispatchIdentity != "" || fact.RunTicketJTIHash != "" || fact.ManifestHash != "" {
			return fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
		}
		return nil
	}
	if !canonicalRuntimeHostRecoveryString(fact.DispatchID) || !runtimeHostRecoverySHA256(fact.DispatchIdentity) ||
		!runtimeHostRecoverySHA256(fact.RunTicketJTIHash) || !runtimeHostRecoverySHA256(fact.ManifestHash) {
		return fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
	}
	return nil
}

func validateRuntimeHostRecoverySnapshotAssertion(principal RuntimeHostPrincipal, snapshot *RuntimeHostRecoverySnapshot) error {
	if snapshot == nil || snapshot.RuntimeHostID != principal.RuntimeHostID || snapshot.InstanceID != principal.InstanceID || snapshot.Environment != principal.Environment ||
		principal.RuntimeHostID == "" || principal.InstanceID == "" || principal.Environment == "" {
		return fmt.Errorf("RUNTIME_HOST_UNAUTHORIZED")
	}
	if snapshot.InstanceGeneration < 1 || snapshot.RecoveryRevision < 1 || !runtimeHostRecoverySHA256(snapshot.FactSetHash) {
		return fmt.Errorf("INVALID_ARGUMENT")
	}
	facts, hash, err := canonicalRuntimeHostRecoveryFacts(snapshot.Facts)
	if err != nil {
		return err
	}
	if hash != snapshot.FactSetHash {
		return fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
	}
	snapshot.Facts = facts
	return nil
}

func compareRuntimeHostRecoverySnapshot(asserted, current RuntimeHostRecoverySnapshot) error {
	if asserted.RuntimeHostID != current.RuntimeHostID || asserted.InstanceID != current.InstanceID || asserted.Environment != current.Environment {
		return fmt.Errorf("RUNTIME_HOST_UNAUTHORIZED")
	}
	if asserted.InstanceGeneration != current.InstanceGeneration {
		return fmt.Errorf("RUNTIME_HOST_REREGISTRATION_REQUIRED")
	}
	if asserted.RecoveryRevision != current.RecoveryRevision || asserted.RecoveryState != current.RecoveryState || asserted.FactSetHash != current.FactSetHash || !runtimeHostRecoveryFactsEqual(asserted.Facts, current.Facts) {
		return fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
	}
	return nil
}

func compareRuntimeHostRecoveryAttestation(attestation RuntimeHostRecoveryAttestation, current RuntimeHostRecoverySnapshot, principal RuntimeHostPrincipal) error {
	if attestation.RuntimeHostID != principal.RuntimeHostID || attestation.InstanceID != principal.InstanceID {
		return fmt.Errorf("RUNTIME_HOST_REREGISTRATION_REQUIRED")
	}
	if attestation.InstanceGeneration != current.InstanceGeneration || attestation.RecoveryRevision != current.RecoveryRevision {
		return fmt.Errorf("RUNTIME_HOST_REREGISTRATION_REQUIRED")
	}
	if attestation.FactSetHash != current.FactSetHash {
		return fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
	}
	return nil
}

func runtimeHostRecoveryAttestationFromMap(row map[string]any) (RuntimeHostRecoveryAttestation, error) {
	item := RuntimeHostRecoveryAttestation{
		AttestationID: runtimeHostRecoveryMapString(row, "attestation_id"), RuntimeHostID: runtimeHostRecoveryMapString(row, "runtime_host_id"),
		InstanceID: runtimeHostRecoveryMapString(row, "instance_id"), InstanceGeneration: runtimeHostInt64(row["instance_generation"]),
		RecoveryRevision: runtimeHostInt64(row["recovery_revision"]), FactSetHash: runtimeHostRecoveryMapString(row, "fact_set_hash"),
		State: runtimeHostRecoveryMapString(row, "state"), CorrelationID: runtimeHostRecoveryMapString(row, "correlation_id"),
	}
	if created, ok := row["created_at"].(time.Time); ok {
		item.CreatedAt = created
	}
	if completed, ok := row["completed_at"].(time.Time); ok {
		item.CompletedAt = &completed
	}
	if item.AttestationID == "" || item.RuntimeHostID == "" || item.InstanceID == "" || item.InstanceGeneration < 1 || item.RecoveryRevision < 1 ||
		item.FactSetHash == "" || (item.State != runtimeHostRecoveryPrepared && item.State != runtimeHostRecoveryCompleted) {
		return RuntimeHostRecoveryAttestation{}, fmt.Errorf("RUNTIME_STORAGE_UNAVAILABLE")
	}
	return item, nil
}

func runtimeHostRecoveryDispatchFromMap(row map[string]any) RuntimeDispatch {
	return RuntimeDispatch{
		DispatchID: runtimeHostRecoveryMapString(row, "dispatch_id"), RunID: runtimeHostRecoveryMapString(row, "run_id"),
		ReservationID: runtimeHostRecoveryMapString(row, "reservation_id"), RuntimeHostID: runtimeHostRecoveryMapString(row, "runtime_host_id"),
		AssignedRuntimeHostInstanceID:         runtimeHostRecoveryMapString(row, "assigned_runtime_host_instance_id"),
		AssignedRuntimeHostInstanceGeneration: runtimeHostInt64(row["assigned_runtime_host_instance_generation"]),
		DispatchIdentity:                      runtimeHostRecoveryMapString(row, "dispatch_identity"), State: runtimeHostRecoveryMapString(row, "state"),
		FencingToken: runtimeHostInt64(row["fencing_token"]), RunTicketJTIHash: runtimeHostRecoveryMapString(row, "run_ticket_jti_hash"),
		InputManifestHash: runtimeHostRecoveryMapString(row, "input_manifest_hash"), EventCursor: runtimeHostInt64(row["event_cursor"]),
		EventLowerBound: runtimeHostInt64(row["event_lower_bound"]), EventUpperBound: runtimeHostInt64(row["event_upper_bound"]),
		EventGapExpectedSequence: runtimeHostInt64(row["event_gap_expected_sequence"]), EventGapObservedSequence: runtimeHostInt64(row["event_gap_observed_sequence"]),
	}
}

func runtimeHostRecoveryStorageError(err error) error {
	if err == nil || runtimeHostRecoveryErrorCode(err) != "" {
		return err
	}
	return fmt.Errorf("RUNTIME_STORAGE_UNAVAILABLE: %w", err)
}

func runtimeHostRecoveryErrorCode(err error) string {
	if err == nil {
		return ""
	}
	for _, code := range []string{"INVALID_ARGUMENT", "RUNTIME_HOST_UNAUTHORIZED", "RUNTIME_HOST_REREGISTRATION_REQUIRED", "RUNTIME_CAPACITY_UNAVAILABLE", "RUNTIME_STORAGE_UNAVAILABLE", "RUNTIME_EVENT_GAP"} {
		if strings.Contains(err.Error(), code) {
			return code
		}
	}
	return ""
}

func runtimeHostRecoveryAttestationKey(snapshot RuntimeHostRecoverySnapshot) string {
	return snapshot.RuntimeHostID + "\x00" + fmt.Sprint(snapshot.InstanceGeneration) + "\x00" + fmt.Sprint(snapshot.RecoveryRevision) + "\x00" + snapshot.FactSetHash
}

func newRuntimeHostRecoveryAttestationID() (string, error) {
	bytes := make([]byte, 18)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return "runtime_recovery_attestation_" + hex.EncodeToString(bytes), nil
}

func validRuntimeHostRecoveryCorrelationID(value string) bool {
	if value == "" {
		return true
	}
	if len(value) > 128 || value != strings.TrimSpace(value) {
		return false
	}
	for _, character := range value {
		if (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') || (character >= '0' && character <= '9') || character == '.' || character == '_' || character == ':' || character == '-' {
			continue
		}
		return false
	}
	return true
}

func validRuntimeExecutionScope(value string) bool {
	return value == "product_thread" || value == "detached_task"
}

func activeRuntimeReservationState(value string) bool {
	return value == "reserved" || value == "accepted" || value == "running"
}

func canonicalRuntimeHostRecoveryString(value string) bool {
	return value != "" && value == strings.TrimSpace(value) && len(value) <= 1024
}

func runtimeHostRecoverySHA256(value string) bool {
	if !strings.HasPrefix(value, "sha256:") || len(value) != len("sha256:")+64 {
		return false
	}
	for _, character := range value[len("sha256:"):] {
		if (character < '0' || character > '9') && (character < 'a' || character > 'f') {
			return false
		}
	}
	return true
}

func runtimeHostRecoveryMapString(row map[string]any, key string) string {
	value, ok := row[key]
	if !ok || value == nil {
		return ""
	}
	return fmt.Sprint(value)
}

func runtimeHostRecoveryFactsEqual(left, right []RuntimeHostRecoveryFact) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if !runtimeHostRecoveryFactEqual(left[index], right[index]) {
			return false
		}
	}
	return true
}

func runtimeHostRecoveryFactEqual(left, right RuntimeHostRecoveryFact) bool {
	return left.RunID == right.RunID && left.RuntimeHostID == right.RuntimeHostID &&
		left.AssignedRuntimeHostInstanceID == right.AssignedRuntimeHostInstanceID &&
		left.AssignedRuntimeHostInstanceGeneration == right.AssignedRuntimeHostInstanceGeneration &&
		left.ReservationID == right.ReservationID && left.DispatchID == right.DispatchID &&
		left.FencingToken == right.FencingToken && left.ExecutionScope == right.ExecutionScope &&
		left.CapabilityHash == right.CapabilityHash && left.DispatchIdentity == right.DispatchIdentity &&
		left.RunTicketJTIHash == right.RunTicketJTIHash && left.ManifestHash == right.ManifestHash &&
		left.Status == right.Status && left.LastEventSequence == right.LastEventSequence
}
