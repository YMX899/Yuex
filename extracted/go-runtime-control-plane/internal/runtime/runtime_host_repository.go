package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"huahuoai/backend/source/internal/persistence"

	"github.com/jackc/pgx/v5/pgconn"
)

type RuntimeHost struct {
	RuntimeHostID             string                    `json:"runtimeHostId"`
	InstanceID                string                    `json:"instanceId"`
	Environment               string                    `json:"environment"`
	Endpoint                  string                    `json:"endpoint"`
	Zone                      string                    `json:"zone"`
	Status                    string                    `json:"status"`
	RuntimeVersion            string                    `json:"runtimeVersion"`
	AdapterVersion            string                    `json:"adapterVersion"`
	CapabilityHash            string                    `json:"capabilityHash"`
	Capabilities              RuntimeCapabilitySnapshot `json:"capabilities"`
	SessionStoreID            string                    `json:"sessionStoreId"`
	MaxActiveRuns             int                       `json:"maxActiveRuns"`
	ActiveRuns                int                       `json:"activeRuns"`
	ReservedRuns              int                       `json:"reservedRuns"`
	ReportedActiveRuns        int                       `json:"reportedActiveRuns"`
	ReportedReservedRuns      int                       `json:"reportedReservedRuns"`
	MaxProductThreadRuns      int                       `json:"maxProductThreadRuns"`
	ActiveProductThreadRuns   int                       `json:"activeProductThreadRuns"`
	ReservedProductThreadRuns int                       `json:"reservedProductThreadRuns"`
	MaxDetachedTaskRuns       int                       `json:"maxDetachedTaskRuns"`
	ActiveDetachedTaskRuns    int                       `json:"activeDetachedTaskRuns"`
	ReservedDetachedTaskRuns  int                       `json:"reservedDetachedTaskRuns"`
	InstanceGeneration        int64                     `json:"instanceGeneration"`
	RecoveryRevision          int64                     `json:"recoveryRevision"`
	RecoveryState             string                    `json:"recoveryState"`
	HeartbeatSequence         int64                     `json:"heartbeatSequence"`
	LastHeartbeatAt           time.Time                 `json:"lastHeartbeatAt"`
	DrainDeadlineAt           time.Time                 `json:"drainDeadlineAt,omitempty"`
	UpdatedAt                 time.Time                 `json:"updatedAt"`
}

type RuntimeHostIdentity struct {
	RuntimeHostID string
	InstanceID    string
	Environment   string
	// CertificateID is a non-secret certificate serial/fingerprint reference
	// produced only after mTLS verification. It never becomes a persisted Host
	// fact or an API response field.
	CertificateID string
}

type RuntimeHostPrincipal = RuntimeHostIdentity

type RuntimeHostRegistration struct {
	Endpoint             string
	Zone                 string
	RuntimeVersion       string
	AdapterVersion       string
	Capabilities         RuntimeCapabilitySnapshot
	SessionStoreID       string
	MaxActiveRuns        int
	MaxProductThreadRuns int
	MaxDetachedTaskRuns  int
}

type RuntimeHostHeartbeat struct {
	Sequence       int64
	ObservedAt     time.Time
	ActiveRuns     int
	ReservedRuns   int
	CapabilityHash string
	SafeHealth     map[string]any
	SignatureKeyID string
	Nonce          string
	BodySHA256     string
	Signature      string
}

type RuntimeSlotReservation struct {
	ReservationID string `json:"reservationId"`
	RunID         string `json:"runId"`
	RuntimeHostID string `json:"runtimeHostId"`
	// AssignedRuntimeHost* are the Host-process binding for recovery. They are
	// deliberately distinct from OwnerInstanceID, which identifies the Worker
	// that owns the Scheduler lease.
	AssignedRuntimeHostInstanceID         string    `json:"assignedRuntimeHostInstanceId"`
	AssignedRuntimeHostInstanceGeneration int64     `json:"assignedRuntimeHostInstanceGeneration"`
	OwnerInstanceID                       string    `json:"ownerInstanceId"`
	State                                 string    `json:"state"`
	FencingToken                          int64     `json:"fencingToken"`
	LeaseTokenHash                        string    `json:"-"`
	CapabilityHash                        string    `json:"capabilityHash"`
	ExecutionScope                        string    `json:"executionScope"`
	ExecutionScopeSource                  string    `json:"executionScopeSource"`
	DispatchID                            string    `json:"dispatchId,omitempty"`
	Version                               int64     `json:"version"`
	LastRenewedAt                         time.Time `json:"lastRenewedAt"`
	ExpiresAt                             time.Time `json:"expiresAt"`
	CreatedAt                             time.Time `json:"createdAt"`
	UpdatedAt                             time.Time `json:"updatedAt"`
}

const (
	runtimeExecutionScopeSourceExplicit           = "explicit"
	runtimeExecutionScopeSourceLegacyUnclassified = "legacy_unclassified"
)

type RuntimeDispatch struct {
	DispatchID    string `json:"dispatchId"`
	RunID         string `json:"runId"`
	ReservationID string `json:"reservationId"`
	// CapacityReservationID and CapacityReservedVersion are the exact durable
	// capacity generation admitted before this dispatch. They are deliberately
	// nullable in storage so legacy rows stay visibly unbound; new dispatches
	// must supply both and are never rebound by RunID lookup.
	CapacityReservationID                 string `json:"capacityReservationId,omitempty"`
	CapacityReservedVersion               int64  `json:"capacityReservedVersion,omitempty"`
	RuntimeHostID                         string `json:"runtimeHostId"`
	AssignedRuntimeHostInstanceID         string `json:"assignedRuntimeHostInstanceId"`
	AssignedRuntimeHostInstanceGeneration int64  `json:"assignedRuntimeHostInstanceGeneration"`
	// DispatchIdentity is a canonical SHA-256 binding, never a ticket or JTI.
	DispatchIdentity         string     `json:"dispatchIdentity"`
	DispatchAttempt          int        `json:"dispatchAttempt"`
	PlanVersion              int        `json:"planVersion"`
	State                    string     `json:"state"`
	FencingToken             int64      `json:"fencingToken"`
	RunTicketJTIHash         string     `json:"-"`
	TicketExpiresAt          time.Time  `json:"runTicketExpiresAt"`
	InputManifestHash        string     `json:"inputManifestHash"`
	RuntimeRequestID         string     `json:"-"`
	AbortRequestedAt         *time.Time `json:"abortRequestedAt,omitempty"`
	AbortStatus              string     `json:"abortStatus,omitempty"`
	OwnerInstanceID          string     `json:"ownerInstanceId"`
	LeaseTokenHash           string     `json:"-"`
	LeaseExpiresAt           time.Time  `json:"leaseExpiresAt"`
	RecoveryOwnerInstanceID  string     `json:"recoveryOwnerInstanceId,omitempty"`
	RecoveryFencingToken     int64      `json:"recoveryFencingToken,omitempty"`
	RecoveryExpiresAt        time.Time  `json:"recoveryExpiresAt,omitempty"`
	RecoveryAttempt          int        `json:"recoveryAttempt"`
	NextRecoveryCheckAt      time.Time  `json:"nextRecoveryCheckAt,omitempty"`
	EventCursor              int64      `json:"eventCursor"`
	EventLowerBound          int64      `json:"eventLowerBound"`
	EventUpperBound          int64      `json:"eventUpperBound,omitempty"`
	EventGapExpectedSequence int64      `json:"eventGapExpectedSequence,omitempty"`
	EventGapObservedSequence int64      `json:"eventGapObservedSequence,omitempty"`
	Version                  int64      `json:"version"`
	CreatedAt                time.Time  `json:"createdAt"`
	UpdatedAt                time.Time  `json:"updatedAt"`
}

type RuntimeHostRunEvent struct {
	EventID        string         `json:"eventId"`
	RunID          string         `json:"runId"`
	DispatchID     string         `json:"dispatchId,omitempty"`
	RuntimeHostID  string         `json:"runtimeHostId,omitempty"`
	Sequence       int64          `json:"sequence"`
	SourceSequence int64          `json:"sourceSequence,omitempty"`
	EventType      string         `json:"eventType"`
	Visibility     string         `json:"visibility"`
	SafePayload    map[string]any `json:"safePayload"`
	UsageDelta     map[string]any `json:"usageDelta"`
	OccurredAt     time.Time      `json:"occurredAt"`
}

// RuntimeHostRecoveryFact is the hash-only, bounded recovery record shared
// with the Gateway. It intentionally excludes tickets, raw JTIs, session
// state, content, paths, credentials, signatures and lease tokens.
type RuntimeHostRecoveryFact struct {
	RunID                                 string `json:"runId"`
	RuntimeHostID                         string `json:"runtimeHostId"`
	AssignedRuntimeHostInstanceID         string `json:"assignedRuntimeHostInstanceId"`
	AssignedRuntimeHostInstanceGeneration int64  `json:"assignedRuntimeHostInstanceGeneration"`
	ReservationID                         string `json:"reservationId"`
	DispatchID                            string `json:"dispatchId,omitempty"`
	FencingToken                          int64  `json:"fencingToken"`
	ExecutionScope                        string `json:"executionScope"`
	CapabilityHash                        string `json:"capabilityHash"`
	DispatchIdentity                      string `json:"dispatchIdentity,omitempty"`
	RunTicketJTIHash                      string `json:"runTicketJtiHash,omitempty"`
	ManifestHash                          string `json:"manifestHash,omitempty"`
	Status                                string `json:"status"`
	LastEventSequence                     int64  `json:"lastEventSequence"`
}

// RuntimeHostRecoverySnapshot is an assertion of Backend-owned durable state,
// not an admission grant. A Host remains closed until an attestation completes.
type RuntimeHostRecoverySnapshot struct {
	RuntimeHostID      string                    `json:"runtimeHostId"`
	InstanceID         string                    `json:"instanceId"`
	Environment        string                    `json:"environment"`
	InstanceGeneration int64                     `json:"instanceGeneration"`
	RecoveryRevision   int64                     `json:"recoveryRevision"`
	RecoveryState      string                    `json:"recoveryState"`
	Facts              []RuntimeHostRecoveryFact `json:"facts"`
	FactSetHash        string                    `json:"factSetHash"`
}

// RuntimeHostRecoveryAttestation stores only the opaque, hash-bound proof
// needed to make the final ready/reconciled compare-and-set idempotent.
type RuntimeHostRecoveryAttestation struct {
	AttestationID      string     `json:"attestationId"`
	RuntimeHostID      string     `json:"runtimeHostId"`
	InstanceID         string     `json:"instanceId"`
	InstanceGeneration int64      `json:"instanceGeneration"`
	RecoveryRevision   int64      `json:"recoveryRevision"`
	FactSetHash        string     `json:"factSetHash"`
	State              string     `json:"state"`
	CorrelationID      string     `json:"correlationId,omitempty"`
	CreatedAt          time.Time  `json:"createdAt"`
	CompletedAt        *time.Time `json:"completedAt,omitempty"`
}

type ProductSessionHostBinding struct {
	TenantID          string
	ThreadID          string
	AgentProfile      string
	ContextGeneration int64
	SessionGeneration int
	RuntimeHostID     string
	SessionStoreID    string
}

type RuntimeHostRepository struct {
	db                     *persistence.Database
	mu                     sync.Mutex
	hosts                  map[string]RuntimeHost
	reservations           map[string]RuntimeSlotReservation
	activeReservationByRun map[string]string
	dispatches             map[string]RuntimeDispatch
	runtimeRunRecords      map[string]RuntimeRunRecordV1
	toolInvocations        map[string]runtimeToolAuditInvocation
	events                 map[string][]RuntimeHostRunEvent
	dispatchEventCursors   map[string]int64
	sessionHosts           map[string]string
	sessionBindings        map[string]ProductSessionBinding
	recoveryAttestations   map[string]RuntimeHostRecoveryAttestation
}

func NewRuntimeHostRepository(db *persistence.Database) *RuntimeHostRepository {
	return &RuntimeHostRepository{
		db: db, hosts: map[string]RuntimeHost{}, reservations: map[string]RuntimeSlotReservation{},
		activeReservationByRun: map[string]string{}, dispatches: map[string]RuntimeDispatch{},
		runtimeRunRecords: map[string]RuntimeRunRecordV1{},
		toolInvocations:   map[string]runtimeToolAuditInvocation{},
		events:            map[string][]RuntimeHostRunEvent{}, dispatchEventCursors: map[string]int64{}, sessionHosts: map[string]string{},
		sessionBindings:      map[string]ProductSessionBinding{},
		recoveryAttestations: map[string]RuntimeHostRecoveryAttestation{},
	}
}

// runtimeHostRecoveryAttestationRequired keeps the production-like control
// plane closed until the durable Gateway/Backend recovery protocol proves the
// Host's occupancy. Test and local repositories retain their explicit
// in-memory reconciliation behavior; they are not a production fallback.
func runtimeHostRecoveryAttestationRequired(environment string) bool {
	switch strings.ToLower(strings.TrimSpace(environment)) {
	case "prelaunch", "prod", "production":
		return true
	default:
		return false
	}
}

func (r *RuntimeHostRepository) RegisterHost(ctx context.Context, identity RuntimeHostIdentity, command RuntimeHostRegistration) (RuntimeHost, error) {
	if identity.RuntimeHostID == "" || identity.InstanceID == "" || identity.Environment == "" || command.Endpoint == "" || command.MaxActiveRuns < 1 || command.Capabilities.CapabilityHash == "" {
		return RuntimeHost{}, fmt.Errorf("INVALID_ARGUMENT")
	}
	if err := ValidateAgentFacingRuntimeTools(command.Capabilities.Tools); err != nil {
		return RuntimeHost{}, err
	}
	// Only production-like Hosts are real execution authorities. Test/local
	// repositories deliberately model partial capability fixtures for isolated
	// lease and recovery tests, while every prelaunch/prod registration must
	// prove the Gateway execution contract before it can ever become ready.
	if runtimeHostRecoveryAttestationRequired(identity.Environment) {
		if err := ValidateRuntimeCapabilitySnapshot(command.Capabilities); err != nil {
			return RuntimeHost{}, err
		}
	}
	if command.MaxProductThreadRuns <= 0 {
		command.MaxProductThreadRuns = command.MaxActiveRuns
	}
	if command.MaxDetachedTaskRuns <= 0 {
		command.MaxDetachedTaskRuns = command.MaxActiveRuns
	}
	if command.MaxProductThreadRuns > command.MaxActiveRuns || command.MaxDetachedTaskRuns > command.MaxActiveRuns {
		return RuntimeHost{}, fmt.Errorf("INVALID_ARGUMENT")
	}
	recoveryState := "reconciled"
	if runtimeHostRecoveryAttestationRequired(identity.Environment) {
		recoveryState = "pending"
	}
	host := RuntimeHost{
		RuntimeHostID: identity.RuntimeHostID, InstanceID: identity.InstanceID, Environment: identity.Environment,
		Endpoint: command.Endpoint, Zone: command.Zone, Status: "registering", RuntimeVersion: command.RuntimeVersion,
		AdapterVersion: command.AdapterVersion, CapabilityHash: command.Capabilities.CapabilityHash,
		Capabilities: command.Capabilities, SessionStoreID: command.SessionStoreID,
		MaxActiveRuns: command.MaxActiveRuns, MaxProductThreadRuns: command.MaxProductThreadRuns,
		MaxDetachedTaskRuns: command.MaxDetachedTaskRuns, InstanceGeneration: 1, RecoveryRevision: 1, RecoveryState: recoveryState,
		UpdatedAt: time.Now().UTC(),
	}
	if r.postgresReady() {
		raw, _ := json.Marshal(command.Capabilities)
		result, err := r.db.Pool.Exec(ctx, `
insert into runtime_hosts(runtime_host_id,instance_id,environment,endpoint,zone,status,runtime_version,adapter_version,capability_hash,capability_snapshot,session_store_id,max_active_runs,max_product_thread_runs,max_detached_task_runs,recovery_state,recovery_revision)
values($1,$2,$3,$4,$5,'registering',$6,$7,$8,$9::jsonb,$10,$11,$12,$13,$14,1)
on conflict(runtime_host_id) do update set
  instance_id=excluded.instance_id, endpoint=excluded.endpoint, zone=excluded.zone,
  status='registering',
  runtime_version=excluded.runtime_version,
  adapter_version=excluded.adapter_version, capability_hash=excluded.capability_hash,
  capability_snapshot=excluded.capability_snapshot, session_store_id=excluded.session_store_id,
  max_active_runs=excluded.max_active_runs,
  max_product_thread_runs=excluded.max_product_thread_runs,
  max_detached_task_runs=excluded.max_detached_task_runs,
  instance_generation=case when runtime_hosts.instance_id=excluded.instance_id then runtime_hosts.instance_generation else runtime_hosts.instance_generation+1 end,
  recovery_state=case
    when lower(excluded.environment) in ('prelaunch','prod','production') then 'pending'
    when runtime_hosts.active_runs=0 and runtime_hosts.reserved_runs=0 then 'reconciled'
    else 'pending'
  end,
  heartbeat_sequence=greatest(runtime_hosts.heartbeat_sequence,coalesce((select max(h.sequence) from runtime_host_heartbeats h where h.runtime_host_id=runtime_hosts.runtime_host_id),0)),
  last_heartbeat_at=null,
  registration_revision=runtime_hosts.registration_revision+1,
  recovery_revision=runtime_hosts.recovery_revision+1,
  updated_at=now()
where runtime_hosts.environment=excluded.environment`,
			identity.RuntimeHostID, identity.InstanceID, identity.Environment, command.Endpoint, command.Zone,
			command.RuntimeVersion, command.AdapterVersion, command.Capabilities.CapabilityHash, string(raw),
			command.SessionStoreID, command.MaxActiveRuns, command.MaxProductThreadRuns, command.MaxDetachedTaskRuns, recoveryState)
		if err != nil {
			return RuntimeHost{}, err
		}
		if result.RowsAffected() != 1 {
			return RuntimeHost{}, fmt.Errorf("RUNTIME_HOST_UNAUTHORIZED")
		}
		return r.GetHost(ctx, identity.RuntimeHostID)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if current, ok := r.hosts[host.RuntimeHostID]; ok {
		if current.Environment != identity.Environment {
			return RuntimeHost{}, fmt.Errorf("RUNTIME_HOST_UNAUTHORIZED")
		}
		host.ActiveRuns = current.ActiveRuns
		host.ReservedRuns = current.ReservedRuns
		host.ActiveProductThreadRuns = current.ActiveProductThreadRuns
		host.ReservedProductThreadRuns = current.ReservedProductThreadRuns
		host.ActiveDetachedTaskRuns = current.ActiveDetachedTaskRuns
		host.ReservedDetachedTaskRuns = current.ReservedDetachedTaskRuns
		host.ReportedActiveRuns = current.ReportedActiveRuns
		host.ReportedReservedRuns = current.ReportedReservedRuns
		host.HeartbeatSequence = current.HeartbeatSequence
		host.InstanceGeneration = current.InstanceGeneration
		host.RecoveryRevision = current.RecoveryRevision + 1
		if host.RecoveryRevision < 1 {
			host.RecoveryRevision = 1
		}
		if current.InstanceID != identity.InstanceID {
			host.InstanceGeneration++
		}
		if runtimeHostRecoveryAttestationRequired(identity.Environment) || host.ActiveRuns > 0 || host.ReservedRuns > 0 {
			host.RecoveryState = "pending"
		}
	}
	r.hosts[host.RuntimeHostID] = host
	return host, nil
}

func (r *RuntimeHostRepository) HeartbeatHost(ctx context.Context, identity RuntimeHostIdentity, heartbeat RuntimeHostHeartbeat) (RuntimeHost, error) {
	if heartbeat.Sequence < 1 || heartbeat.SignatureKeyID == "" {
		return RuntimeHost{}, fmt.Errorf("INVALID_ARGUMENT")
	}
	if heartbeat.ObservedAt.IsZero() {
		heartbeat.ObservedAt = time.Now().UTC()
	}
	if r.postgresReady() {
		return r.heartbeatPostgres(ctx, identity, heartbeat)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	host, ok := r.hosts[identity.RuntimeHostID]
	if !ok || host.InstanceID != identity.InstanceID || host.Environment != identity.Environment {
		return RuntimeHost{}, fmt.Errorf("RUNTIME_HOST_UNAUTHORIZED")
	}
	if heartbeat.Sequence <= host.HeartbeatSequence {
		return RuntimeHost{}, fmt.Errorf("RUNTIME_HEARTBEAT_STALE")
	}
	if heartbeat.CapabilityHash == "" || heartbeat.CapabilityHash != host.CapabilityHash {
		return RuntimeHost{}, fmt.Errorf("RUNTIME_HOST_CAPABILITY_MISMATCH")
	}
	host.HeartbeatSequence = heartbeat.Sequence
	host.LastHeartbeatAt = heartbeat.ObservedAt
	host.ReportedActiveRuns = heartbeat.ActiveRuns
	host.ReportedReservedRuns = heartbeat.ReservedRuns
	if (host.Status == "registering" || host.Status == "unhealthy") && host.RecoveryState == "reconciled" {
		host.Status = "ready"
	}
	host.UpdatedAt = time.Now().UTC()
	r.hosts[host.RuntimeHostID] = host
	return host, nil
}

func (r *RuntimeHostRepository) GetHost(ctx context.Context, hostID string) (RuntimeHost, error) {
	if r.postgresReady() {
		return r.getHostPostgres(ctx, hostID)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	host, ok := r.hosts[hostID]
	if !ok {
		return RuntimeHost{}, fmt.Errorf("NOT_FOUND")
	}
	return host, nil
}

func (r *RuntimeHostRepository) ListEligibleHosts(ctx context.Context, capabilityHash, runtimeVersion, adapterVersion string, heartbeatAfter time.Time) ([]RuntimeHost, error) {
	if r.postgresReady() {
		return r.listHostsPostgres(ctx, capabilityHash, runtimeVersion, adapterVersion, heartbeatAfter)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	out := []RuntimeHost{}
	for _, host := range r.hosts {
		if host.Status != "ready" || host.RecoveryState != "reconciled" || host.CapabilityHash != capabilityHash ||
			runtimeVersion != "" && host.RuntimeVersion != runtimeVersion ||
			adapterVersion != "" && host.AdapterVersion != adapterVersion ||
			!heartbeatAfter.IsZero() && host.LastHeartbeatAt.Before(heartbeatAfter) {
			continue
		}
		out = append(out, host)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].RuntimeHostID < out[j].RuntimeHostID })
	return out, nil
}

// ListReadyHosts returns only Hosts that have completed recovery and reported a
// fresh heartbeat. Planning uses it to bind a plan to a live capability hash;
// it must not infer readiness from the static agent catalog.
func (r *RuntimeHostRepository) ListReadyHosts(ctx context.Context, heartbeatAfter time.Time) ([]RuntimeHost, error) {
	if r == nil {
		return nil, fmt.Errorf("RUNTIME_TOOL_UNAVAILABLE")
	}
	if r.postgresReady() {
		rows, err := r.db.Pool.Query(ctx, runtimeHostSelect+` where status='ready' and recovery_state='reconciled' and ($1::timestamptz is null or last_heartbeat_at>=$1) order by runtime_host_id`, nullableRuntimeTime(heartbeatAfter))
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		out := []RuntimeHost{}
		for rows.Next() {
			host, scanErr := scanRuntimeHost(rows)
			if scanErr != nil {
				return nil, scanErr
			}
			out = append(out, host)
		}
		return out, rows.Err()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	out := []RuntimeHost{}
	for _, host := range r.hosts {
		if host.Status != "ready" || host.RecoveryState != "reconciled" || (!heartbeatAfter.IsZero() && host.LastHeartbeatAt.Before(heartbeatAfter)) {
			continue
		}
		out = append(out, host)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].RuntimeHostID < out[j].RuntimeHostID })
	return out, nil
}

func (r *RuntimeHostRepository) ListHostsAdmin(ctx context.Context, status string, limit int) ([]RuntimeHost, error) {
	limit, err := runtimeAdminLimit(limit)
	if err != nil {
		return nil, err
	}
	status = strings.TrimSpace(status)
	if r.postgresReady() {
		rows, err := r.db.Pool.Query(ctx, runtimeHostSelect+` where ($1='' or status=$1) order by updated_at desc,runtime_host_id limit $2`, status, limit)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		out := []RuntimeHost{}
		for rows.Next() {
			host, err := scanRuntimeHost(rows)
			if err != nil {
				return nil, err
			}
			out = append(out, host)
		}
		return out, rows.Err()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	out := []RuntimeHost{}
	for _, host := range r.hosts {
		if status == "" || host.Status == status {
			out = append(out, host)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].UpdatedAt.After(out[j].UpdatedAt) })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (r *RuntimeHostRepository) SetHostStatus(ctx context.Context, hostID, status string, deadline time.Time) error {
	if !validRuntimeHostStatus(status) {
		return fmt.Errorf("INVALID_ARGUMENT")
	}
	if r.postgresReady() {
		result, err := r.db.Pool.Exec(ctx, `update runtime_hosts set status=$2,drain_deadline_at=$3,updated_at=now() where runtime_host_id=$1`, hostID, nullableRuntimeTime(deadline))
		if err != nil {
			return err
		}
		if result.RowsAffected() != 1 {
			return fmt.Errorf("NOT_FOUND")
		}
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	host, ok := r.hosts[hostID]
	if !ok {
		return fmt.Errorf("NOT_FOUND")
	}
	host.Status = status
	host.DrainDeadlineAt = deadline
	host.UpdatedAt = time.Now().UTC()
	r.hosts[hostID] = host
	return nil
}

func (r *RuntimeHostRepository) GetReservation(ctx context.Context, reservationID string) (RuntimeSlotReservation, error) {
	if r.postgresReady() {
		var item RuntimeSlotReservation
		err := r.db.Pool.QueryRow(ctx, `select reservation_id,run_id,runtime_host_id,coalesce(assigned_runtime_host_instance_id,''),coalesce(assigned_runtime_host_instance_generation,0),owner_instance_id,state,fencing_token,lease_token_hash,capability_hash,execution_scope,execution_scope_source,coalesce(dispatch_id,''),expires_at,last_renewed_at,version,created_at,updated_at from runtime_slot_reservations where reservation_id=$1`, reservationID).Scan(
			&item.ReservationID, &item.RunID, &item.RuntimeHostID, &item.AssignedRuntimeHostInstanceID, &item.AssignedRuntimeHostInstanceGeneration, &item.OwnerInstanceID, &item.State,
			&item.FencingToken, &item.LeaseTokenHash, &item.CapabilityHash, &item.ExecutionScope, &item.ExecutionScopeSource, &item.DispatchID,
			&item.ExpiresAt, &item.LastRenewedAt, &item.Version, &item.CreatedAt, &item.UpdatedAt,
		)
		return item, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	item, ok := r.reservations[reservationID]
	if !ok {
		return RuntimeSlotReservation{}, fmt.Errorf("NOT_FOUND")
	}
	return item, nil
}

func (r *RuntimeHostRepository) ListReservationsAdmin(ctx context.Context, runID, hostID, status string, limit int) ([]RuntimeSlotReservation, error) {
	limit, err := runtimeAdminLimit(limit)
	if err != nil {
		return nil, err
	}
	runID, hostID, status = strings.TrimSpace(runID), strings.TrimSpace(hostID), strings.TrimSpace(status)
	if r.postgresReady() {
		rows, err := r.db.Pool.Query(ctx, `select reservation_id,run_id,runtime_host_id,coalesce(assigned_runtime_host_instance_id,''),coalesce(assigned_runtime_host_instance_generation,0),owner_instance_id,state,fencing_token,lease_token_hash,capability_hash,execution_scope,execution_scope_source,coalesce(dispatch_id,''),expires_at,last_renewed_at,version,created_at,updated_at from runtime_slot_reservations where ($1='' or run_id=$1) and ($2='' or runtime_host_id=$2) and ($3='' or state=$3) order by updated_at desc,reservation_id limit $4`, runID, hostID, status, limit)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		out := []RuntimeSlotReservation{}
		for rows.Next() {
			item, err := scanRuntimeReservation(rows)
			if err != nil {
				return nil, err
			}
			out = append(out, item)
		}
		return out, rows.Err()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	out := []RuntimeSlotReservation{}
	for _, item := range r.reservations {
		if (runID == "" || item.RunID == runID) && (hostID == "" || item.RuntimeHostID == hostID) && (status == "" || item.State == status) {
			out = append(out, item)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].UpdatedAt.After(out[j].UpdatedAt) })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (r *RuntimeHostRepository) ExpireReservations(ctx context.Context, now time.Time) (int, error) {
	if r.postgresReady() {
		rows, err := r.db.Pool.Query(ctx, `select r.reservation_id,r.runtime_host_id,r.owner_instance_id,r.lease_token_hash,r.fencing_token
from runtime_slot_reservations r
where r.state='reserved' and r.expires_at<=$1
  and not exists (
    select 1 from runtime_run_dispatches d
    where d.reservation_id=r.reservation_id
      and d.state in('sent','submit_unknown','retry_same_host','accepted','materializing','running','finalizing','recovering')
  )
order by r.expires_at limit 500`, now)
		if err != nil {
			return 0, err
		}
		defer rows.Close()
		fences := []ReservationFence{}
		for rows.Next() {
			var fence ReservationFence
			if err := rows.Scan(&fence.ReservationID, &fence.RuntimeHostID, &fence.OwnerInstanceID, &fence.LeaseTokenHash, &fence.FencingToken); err != nil {
				return 0, err
			}
			fences = append(fences, fence)
		}
		if err := rows.Err(); err != nil {
			return 0, err
		}
		count := 0
		for _, fence := range fences {
			changed, err := r.releaseReservationFenced(ctx, ReservationReleaseCommand{Fence: fence, Reason: "lease_expired"}, "expired")
			if err != nil && err.Error() != "STALE_FENCING_TOKEN" {
				return count, err
			}
			if changed {
				count++
			}
		}
		return count, nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	count := 0
	for id, reservation := range r.reservations {
		if reservation.State == "reserved" && !reservation.ExpiresAt.After(now) {
			protected := false
			for _, dispatch := range r.dispatches {
				if dispatch.ReservationID == reservation.ReservationID && stringInRuntime(dispatch.State, []string{"sent", "submit_unknown", "retry_same_host", "accepted", "materializing", "running", "finalizing", "recovering"}) {
					protected = true
					break
				}
			}
			if protected {
				continue
			}
			reservation.State = "expired"
			reservation.UpdatedAt = now
			r.reservations[id] = reservation
			delete(r.activeReservationByRun, reservation.RunID)
			host := r.hosts[reservation.RuntimeHostID]
			decrementRuntimeHostReserved(&host, reservation.ExecutionScope)
			r.hosts[host.RuntimeHostID] = host
			count++
		}
	}
	return count, nil
}

func (r *RuntimeHostRepository) CreateDispatch(ctx context.Context, dispatch RuntimeDispatch) (RuntimeDispatch, error) {
	return r.createDispatch(ctx, dispatch, nil)
}

func (r *RuntimeHostRepository) GetDispatch(ctx context.Context, dispatchID string) (RuntimeDispatch, error) {
	if r.postgresReady() {
		return scanRuntimeDispatchFull(r.db.Pool.QueryRow(ctx, runtimeDispatchSelect+` where dispatch_id=$1`, dispatchID))
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	item, ok := r.dispatches[dispatchID]
	if !ok {
		return RuntimeDispatch{}, fmt.Errorf("NOT_FOUND")
	}
	return item, nil
}

func (r *RuntimeHostRepository) ListDispatchesAdmin(ctx context.Context, runID, hostID, status string, limit int) ([]RuntimeDispatch, error) {
	limit, err := runtimeAdminLimit(limit)
	if err != nil {
		return nil, err
	}
	runID, hostID, status = strings.TrimSpace(runID), strings.TrimSpace(hostID), strings.TrimSpace(status)
	if r.postgresReady() {
		rows, err := r.db.Pool.Query(ctx, `select dispatch_id,run_id,reservation_id,coalesce(capacity_reservation_id,''),coalesce(capacity_reserved_version,0),runtime_host_id,dispatch_attempt,plan_version,state,fencing_token,run_ticket_jti_hash,run_ticket_expires_at,input_manifest_hash,coalesce(runtime_request_id,''),abort_requested_at,coalesce(abort_status,''),created_at,updated_at from runtime_run_dispatches where ($1='' or run_id=$1) and ($2='' or runtime_host_id=$2) and ($3='' or state=$3) order by updated_at desc,dispatch_attempt desc limit $4`, runID, hostID, status, limit)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		out := []RuntimeDispatch{}
		for rows.Next() {
			item, err := scanRuntimeDispatch(rows)
			if err != nil {
				return nil, err
			}
			out = append(out, item)
		}
		return out, rows.Err()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	out := []RuntimeDispatch{}
	for _, item := range r.dispatches {
		if (runID == "" || item.RunID == runID) && (hostID == "" || item.RuntimeHostID == hostID) && (status == "" || item.State == status) {
			out = append(out, item)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].UpdatedAt.After(out[j].UpdatedAt) })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (r *RuntimeHostRepository) GetActiveDispatchByRunID(ctx context.Context, runID string) (RuntimeDispatch, error) {
	if runID == "" {
		return RuntimeDispatch{}, fmt.Errorf("INVALID_ARGUMENT")
	}
	if r.postgresReady() {
		var item RuntimeDispatch
		err := r.db.Pool.QueryRow(ctx, `select dispatch_id,run_id,reservation_id,coalesce(capacity_reservation_id,''),coalesce(capacity_reserved_version,0),runtime_host_id,dispatch_attempt,plan_version,state,fencing_token,run_ticket_jti_hash,run_ticket_expires_at,input_manifest_hash,coalesce(runtime_request_id,''),abort_requested_at,coalesce(abort_status,''),created_at,updated_at from runtime_run_dispatches where run_id=$1 and state in('created','sent','submit_unknown','retry_same_host','accepted','materializing','running','finalizing','recovering') order by dispatch_attempt desc limit 1`, runID).Scan(
			&item.DispatchID, &item.RunID, &item.ReservationID, &item.CapacityReservationID, &item.CapacityReservedVersion, &item.RuntimeHostID, &item.DispatchAttempt, &item.PlanVersion,
			&item.State, &item.FencingToken, &item.RunTicketJTIHash, &item.TicketExpiresAt,
			&item.InputManifestHash, &item.RuntimeRequestID, &item.AbortRequestedAt, &item.AbortStatus, &item.CreatedAt, &item.UpdatedAt,
		)
		return item, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	var selected RuntimeDispatch
	for _, item := range r.dispatches {
		if item.RunID == runID && activeRuntimeDispatchState(item.State) && item.DispatchAttempt > selected.DispatchAttempt {
			selected = item
		}
	}
	if selected.DispatchID == "" {
		return RuntimeDispatch{}, fmt.Errorf("NOT_FOUND")
	}
	return selected, nil
}

func (r *RuntimeHostRepository) MarkDispatchAbortStatus(ctx context.Context, dispatchID, hostID string, fencing int64, status string) error {
	if !stringInRuntime(status, []string{"requested", "accepted", "failed", "terminal"}) {
		return fmt.Errorf("INVALID_ARGUMENT")
	}
	if r.postgresReady() {
		result, err := r.db.Pool.Exec(ctx, `update runtime_run_dispatches set abort_requested_at=coalesce(abort_requested_at,now()),abort_status=$4,updated_at=now() where dispatch_id=$1 and runtime_host_id=$2 and fencing_token=$3 and state in('created','sent','submit_unknown','retry_same_host','accepted','materializing','running','finalizing','recovering')`, dispatchID, hostID, fencing, status)
		if err != nil {
			return err
		}
		if result.RowsAffected() != 1 {
			return fmt.Errorf("STALE_FENCING_TOKEN")
		}
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	dispatch, ok := r.dispatches[dispatchID]
	if !ok || dispatch.RuntimeHostID != hostID || dispatch.FencingToken != fencing || !activeRuntimeDispatchState(dispatch.State) {
		return fmt.Errorf("STALE_FENCING_TOKEN")
	}
	now := time.Now().UTC()
	if dispatch.AbortRequestedAt == nil {
		dispatch.AbortRequestedAt = &now
	}
	dispatch.AbortStatus = status
	dispatch.UpdatedAt = now
	r.dispatches[dispatchID] = dispatch
	return nil
}

func (r *RuntimeHostRepository) MarkDispatchTerminal(ctx context.Context, dispatchID, hostID string, fencing int64, terminalStatus, errorCode string) error {
	if terminalStatus == "cancelled" {
		terminalStatus = "aborted"
	}
	if !stringInRuntime(terminalStatus, []string{"succeeded", "failed", "timeout", "aborted", "rejected", "orphaned"}) {
		return fmt.Errorf("INVALID_ARGUMENT")
	}
	if r.postgresReady() {
		result, err := r.db.Pool.Exec(ctx, `update runtime_run_dispatches set state=$4,terminal_at=coalesce(terminal_at,now()),error_code=nullif($5,''),abort_status=case when $4='aborted' then 'terminal' else abort_status end,updated_at=now() where dispatch_id=$1 and runtime_host_id=$2 and fencing_token=$3 and state in('created','sent','accepted','materializing','running','finalizing',$4)`, dispatchID, hostID, fencing, terminalStatus, errorCode)
		if err != nil {
			return err
		}
		if result.RowsAffected() != 1 {
			return fmt.Errorf("STALE_FENCING_TOKEN")
		}
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	dispatch, ok := r.dispatches[dispatchID]
	if !ok || dispatch.RuntimeHostID != hostID || dispatch.FencingToken != fencing || (!activeRuntimeDispatchState(dispatch.State) && dispatch.State != terminalStatus) {
		return fmt.Errorf("STALE_FENCING_TOKEN")
	}
	dispatch.State = terminalStatus
	if terminalStatus == "aborted" {
		dispatch.AbortStatus = "terminal"
	}
	dispatch.UpdatedAt = time.Now().UTC()
	r.dispatches[dispatchID] = dispatch
	return nil
}

func (r *RuntimeHostRepository) ListRunEvents(ctx context.Context, runID string, after int64, limit int) ([]RuntimeHostRunEvent, error) {
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}
	if r.postgresReady() {
		rows, err := r.db.Pool.Query(ctx, `select runtime_run_event_id,run_id,coalesce(dispatch_id,''),coalesce(runtime_host_id,''),sequence,coalesce(source_sequence,0),event_type,visibility,safe_payload,usage_delta,occurred_at from runtime_run_events where run_id=$1 and sequence>$2 order by sequence limit $3`, runID, after, limit)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		out := []RuntimeHostRunEvent{}
		for rows.Next() {
			var event RuntimeHostRunEvent
			var payload, usage []byte
			if err := rows.Scan(&event.EventID, &event.RunID, &event.DispatchID, &event.RuntimeHostID, &event.Sequence, &event.SourceSequence, &event.EventType, &event.Visibility, &payload, &usage, &event.OccurredAt); err != nil {
				return nil, err
			}
			_ = json.Unmarshal(payload, &event.SafePayload)
			_ = json.Unmarshal(usage, &event.UsageDelta)
			out = append(out, event)
		}
		return out, rows.Err()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	out := []RuntimeHostRunEvent{}
	for _, event := range r.events[runID] {
		if event.Sequence > after {
			out = append(out, event)
			if len(out) == limit {
				break
			}
		}
	}
	return out, nil
}

func (r *RuntimeHostRepository) LastSourceEventSequence(ctx context.Context, dispatchID string) (int64, error) {
	if r.postgresReady() {
		var sequence int64
		err := r.db.Pool.QueryRow(ctx, `select coalesce(max(source_sequence),0) from runtime_run_events where dispatch_id=$1`, dispatchID).Scan(&sequence)
		return sequence, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	var last int64
	for _, items := range r.events {
		for _, event := range items {
			if event.DispatchID == dispatchID && event.SourceSequence > last {
				last = event.SourceSequence
			}
		}
	}
	return last, nil
}

func (r *RuntimeHostRepository) BindProductSessionHost(ctx context.Context, binding ProductSessionHostBinding) error {
	if strings.TrimSpace(binding.TenantID) == "" {
		return fmt.Errorf("INVALID_ARGUMENT")
	}
	key := productSessionHostKey(binding)
	if r.postgresReady() {
		result, err := r.db.Pool.Exec(ctx, `update thread_agent_runtime_bindings set runtime_host_id=$6,session_store_id=nullif($7,''),updated_at=now() where tenant_id=$1 and thread_id=$2 and agent_profile=$3 and context_generation=$4 and session_generation=$5 and status='active' and (runtime_host_id is null or runtime_host_id=$6)`, binding.TenantID, binding.ThreadID, binding.AgentProfile, binding.ContextGeneration, binding.SessionGeneration, binding.RuntimeHostID, binding.SessionStoreID)
		if err != nil {
			return err
		}
		if result.RowsAffected() != 1 {
			return fmt.Errorf("SESSION_HOST_AFFINITY_CONFLICT")
		}
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	current, ok := r.sessionBindings[key]
	if !ok || current.Status != "active" {
		return fmt.Errorf("RUNTIME_SESSION_BINDING_UNAVAILABLE")
	}
	if existing := r.sessionHosts[key]; existing != "" && existing != binding.RuntimeHostID {
		return fmt.Errorf("SESSION_HOST_AFFINITY_CONFLICT")
	}
	r.sessionHosts[key] = binding.RuntimeHostID
	current.RuntimeHostID = binding.RuntimeHostID
	current.SessionStoreID = binding.SessionStoreID
	current.UpdatedAt = time.Now().UTC()
	r.sessionBindings[key] = current
	return nil
}

func (r *RuntimeHostRepository) GetProductSessionHost(ctx context.Context, binding ProductSessionHostBinding) (string, error) {
	if strings.TrimSpace(binding.TenantID) == "" {
		return "", fmt.Errorf("INVALID_ARGUMENT")
	}
	if r.postgresReady() {
		var hostID string
		err := r.db.Pool.QueryRow(ctx, `select runtime_host_id from thread_agent_runtime_bindings where tenant_id=$1 and thread_id=$2 and agent_profile=$3 and context_generation=$4 and session_generation=$5 and status='active' and runtime_host_id is not null`, binding.TenantID, binding.ThreadID, binding.AgentProfile, binding.ContextGeneration, binding.SessionGeneration).Scan(&hostID)
		return hostID, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	hostID := r.sessionHosts[productSessionHostKey(binding)]
	if hostID == "" {
		return "", fmt.Errorf("NOT_FOUND")
	}
	return hostID, nil
}

func (r *RuntimeHostRepository) heartbeatPostgres(ctx context.Context, identity RuntimeHostIdentity, heartbeat RuntimeHostHeartbeat) (RuntimeHost, error) {
	err := r.db.WithTx(ctx, func(tx *persistence.Tx) error {
		rows, err := tx.Query(ctx, `select heartbeat_sequence,max_active_runs,capability_hash from runtime_hosts where runtime_host_id=@host and instance_id=@instance and environment=@environment for update`, map[string]any{"host": identity.RuntimeHostID, "instance": identity.InstanceID, "environment": identity.Environment})
		if err != nil {
			return err
		}
		if len(rows) != 1 {
			return fmt.Errorf("RUNTIME_HOST_UNAUTHORIZED")
		}
		if heartbeat.Sequence <= runtimeHostInt64(rows[0]["heartbeat_sequence"]) {
			return fmt.Errorf("RUNTIME_HEARTBEAT_STALE")
		}
		if heartbeat.CapabilityHash == "" || fmt.Sprint(rows[0]["capability_hash"]) != heartbeat.CapabilityHash {
			return fmt.Errorf("RUNTIME_HOST_CAPABILITY_MISMATCH")
		}
		maxRuns := int(runtimeHostInt64(rows[0]["max_active_runs"]))
		health, _ := json.Marshal(heartbeat.SafeHealth)
		if err := tx.Exec(ctx, `insert into runtime_host_heartbeats(heartbeat_id,runtime_host_id,instance_id,sequence,observed_at,active_runs,reserved_runs,free_slots,capability_hash,safe_health,signature_key_id) values(@id,@host,@instance,@sequence,@observed,@active,@reserved,@free,@capability,@health::jsonb,@key)`, map[string]any{
			"id": fmt.Sprintf("heartbeat_%s_%d", identity.RuntimeHostID, heartbeat.Sequence), "host": identity.RuntimeHostID,
			"instance": identity.InstanceID, "sequence": heartbeat.Sequence, "observed": heartbeat.ObservedAt,
			"active": heartbeat.ActiveRuns, "reserved": heartbeat.ReservedRuns,
			"free":       runtimeMaxInt(0, maxRuns-heartbeat.ActiveRuns-heartbeat.ReservedRuns),
			"capability": heartbeat.CapabilityHash, "health": string(health), "key": heartbeat.SignatureKeyID,
		}); err != nil {
			return err
		}
		return tx.Exec(ctx, `update runtime_hosts set heartbeat_sequence=@sequence,last_heartbeat_at=@observed,reported_active_runs=@active,reported_reserved_runs=@reserved,reported_at=now(),status=case when status in('registering','unhealthy') and recovery_state='reconciled' then 'ready' else status end,updated_at=now() where runtime_host_id=@host`, map[string]any{"sequence": heartbeat.Sequence, "observed": heartbeat.ObservedAt, "active": heartbeat.ActiveRuns, "reserved": heartbeat.ReservedRuns, "host": identity.RuntimeHostID})
	})
	if err != nil {
		return RuntimeHost{}, err
	}
	return r.GetHost(ctx, identity.RuntimeHostID)
}

func (r *RuntimeHostRepository) getHostPostgres(ctx context.Context, hostID string) (RuntimeHost, error) {
	return scanRuntimeHost(r.db.Pool.QueryRow(ctx, runtimeHostSelect+` where runtime_host_id=$1`, hostID))
}

func (r *RuntimeHostRepository) listHostsPostgres(ctx context.Context, capabilityHash, runtimeVersion, adapterVersion string, heartbeatAfter time.Time) ([]RuntimeHost, error) {
	rows, err := r.db.Pool.Query(ctx, runtimeHostSelect+` where status='ready' and recovery_state='reconciled' and capability_hash=$1 and ($2='' or runtime_version=$2) and ($3='' or adapter_version=$3) and ($4::timestamptz is null or last_heartbeat_at>=$4) order by runtime_host_id`, capabilityHash, runtimeVersion, adapterVersion, nullableRuntimeTime(heartbeatAfter))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []RuntimeHost{}
	for rows.Next() {
		host, err := scanRuntimeHost(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, host)
	}
	return out, rows.Err()
}

const runtimeHostColumns = `runtime_host_id,instance_id,environment,endpoint,zone,status,runtime_version,adapter_version,capability_hash,capability_snapshot,session_store_id,max_active_runs,active_runs,reserved_runs,reported_active_runs,reported_reserved_runs,max_product_thread_runs,active_product_thread_runs,reserved_product_thread_runs,max_detached_task_runs,active_detached_task_runs,reserved_detached_task_runs,instance_generation,recovery_revision,recovery_state,heartbeat_sequence,last_heartbeat_at,drain_deadline_at,updated_at`

const runtimeHostSelect = `select ` + runtimeHostColumns + ` from runtime_hosts`

type runtimeHostScanner interface{ Scan(...any) error }

func scanRuntimeHost(row runtimeHostScanner) (RuntimeHost, error) {
	var host RuntimeHost
	var raw []byte
	var heartbeat, deadline *time.Time
	err := row.Scan(&host.RuntimeHostID, &host.InstanceID, &host.Environment, &host.Endpoint, &host.Zone, &host.Status, &host.RuntimeVersion, &host.AdapterVersion, &host.CapabilityHash, &raw, &host.SessionStoreID, &host.MaxActiveRuns, &host.ActiveRuns, &host.ReservedRuns, &host.ReportedActiveRuns, &host.ReportedReservedRuns, &host.MaxProductThreadRuns, &host.ActiveProductThreadRuns, &host.ReservedProductThreadRuns, &host.MaxDetachedTaskRuns, &host.ActiveDetachedTaskRuns, &host.ReservedDetachedTaskRuns, &host.InstanceGeneration, &host.RecoveryRevision, &host.RecoveryState, &host.HeartbeatSequence, &heartbeat, &deadline, &host.UpdatedAt)
	if err != nil {
		return RuntimeHost{}, err
	}
	_ = json.Unmarshal(raw, &host.Capabilities)
	if heartbeat != nil {
		host.LastHeartbeatAt = *heartbeat
	}
	if deadline != nil {
		host.DrainDeadlineAt = *deadline
	}
	return host, nil
}

func scanRuntimeReservation(row runtimeHostScanner) (RuntimeSlotReservation, error) {
	var item RuntimeSlotReservation
	err := row.Scan(
		&item.ReservationID, &item.RunID, &item.RuntimeHostID, &item.AssignedRuntimeHostInstanceID, &item.AssignedRuntimeHostInstanceGeneration, &item.OwnerInstanceID, &item.State,
		&item.FencingToken, &item.LeaseTokenHash, &item.CapabilityHash, &item.ExecutionScope, &item.ExecutionScopeSource, &item.DispatchID,
		&item.ExpiresAt, &item.LastRenewedAt, &item.Version, &item.CreatedAt, &item.UpdatedAt,
	)
	return item, err
}

func scanRuntimeDispatch(row runtimeHostScanner) (RuntimeDispatch, error) {
	var item RuntimeDispatch
	err := row.Scan(
		&item.DispatchID, &item.RunID, &item.ReservationID, &item.CapacityReservationID, &item.CapacityReservedVersion, &item.RuntimeHostID, &item.DispatchAttempt, &item.PlanVersion,
		&item.State, &item.FencingToken, &item.RunTicketJTIHash, &item.TicketExpiresAt,
		&item.InputManifestHash, &item.RuntimeRequestID, &item.AbortRequestedAt, &item.AbortStatus, &item.CreatedAt, &item.UpdatedAt,
	)
	return item, err
}

// HasCapacityReservationBinding reports whether a dispatch is bound to one
// exact capacity reservation generation. Callers must not repair an unbound
// legacy row by searching for an arbitrary capacity reservation by RunID.
func (d RuntimeDispatch) HasCapacityReservationBinding() bool {
	return strings.TrimSpace(d.CapacityReservationID) != "" && d.CapacityReservedVersion >= 1
}

// IsLegacyCapacityUnbound distinguishes a migrated historical row from an
// incomplete binding. Such rows may only be handled by explicit recovery; they
// are never treated as a new dispatch or auto-bound from their RunID.
func (d RuntimeDispatch) IsLegacyCapacityUnbound() bool {
	return d.CapacityReservationID == "" && d.CapacityReservedVersion == 0
}

// MatchesCapacityReservation prevents a delayed dispatch event from settling a
// later re-reservation that reuses the stable capacity reservation ID. An
// accepted transition increments the row once; a released or expired row is
// still the same generation only when it has not returned to reserved.
func (d RuntimeDispatch) MatchesCapacityReservation(reservation RuntimeCapacityReservation) bool {
	if !d.HasCapacityReservationBinding() || reservation.ReservationID != d.CapacityReservationID {
		return false
	}
	if reservation.Version == d.CapacityReservedVersion || reservation.Version == d.CapacityReservedVersion+1 {
		return true
	}
	return (reservation.State == "released" || reservation.State == "expired") && reservation.Version > d.CapacityReservedVersion
}

func runtimeAdminLimit(limit int) (int, error) {
	if limit == 0 {
		return 50, nil
	}
	if limit < 1 || limit > 200 {
		return 0, fmt.Errorf("INVALID_ARGUMENT")
	}
	return limit, nil
}

func (r *RuntimeHostRepository) postgresReady() bool {
	return r != nil && r.db != nil && !r.db.Disabled && r.db.Pool != nil
}

func runtimeUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

func nullableRuntimeTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value
}

func validRuntimeHostStatus(value string) bool {
	switch value {
	case "registering", "ready", "draining", "unhealthy", "offline":
		return true
	default:
		return false
	}
}

func activeRuntimeDispatchState(value string) bool {
	switch value {
	case "created", "sent", "submit_unknown", "retry_same_host", "accepted", "materializing", "running", "finalizing", "recovering":
		return true
	default:
		return false
	}
}

func productSessionHostKey(binding ProductSessionHostBinding) string {
	return fmt.Sprintf("%s\x00%s\x00%s\x00%d\x00%d", binding.TenantID, binding.ThreadID, binding.AgentProfile, binding.ContextGeneration, binding.SessionGeneration)
}

func sanitizeRuntimeHostEvent(value map[string]any) map[string]any {
	out := map[string]any{}
	for key, item := range value {
		switch key {
		case "content", "text", "query", "snippet", "embedding", "sessionKey", "openclawSessionKey", "realPath", "providerBody", "toolArgs", "runTicket":
			continue
		}
		out[key] = item
	}
	return out
}

func runtimeHostInt64(value any) int64 {
	switch typed := value.(type) {
	case int64:
		return typed
	case int:
		return int64(typed)
	case float64:
		return int64(typed)
	default:
		var out int64
		_, _ = fmt.Sscan(fmt.Sprint(value), &out)
		return out
	}
}

func runtimeMaxInt(left, right int) int {
	if left > right {
		return left
	}
	return right
}

func stringInRuntime(value string, values []string) bool {
	for _, item := range values {
		if value == item {
			return true
		}
	}
	return false
}
