package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"huahuoai/backend/source/internal/persistence"

	"github.com/jackc/pgx/v5"
)

type RuntimeCapacityDimension struct {
	Key          string `json:"key"`
	Limit        int    `json:"limit"`
	ObservedUsed int    `json:"observedUsed"`
	Requested    int    `json:"requested"`
	Version      int64  `json:"version"`
}

type RuntimeCapacityDimensions struct {
	Model    RuntimeCapacityDimension `json:"model"`
	AuthPool RuntimeCapacityDimension `json:"authPool"`
	Tool     RuntimeCapacityDimension `json:"tool"`
	Tenant   RuntimeCapacityDimension `json:"tenant"`
	User     RuntimeCapacityDimension `json:"user"`
}

type RuntimeCapacityCommand struct {
	RunID           string
	SnapshotVersion int64
	Dimensions      RuntimeCapacityDimensions
	TTL             time.Duration
}

type RuntimeCapacityReservation struct {
	ReservationID   string
	RunID           string
	SnapshotVersion int64
	Dimensions      RuntimeCapacityDimensions
	State           string
	ExpiresAt       time.Time
	AcceptedAt      time.Time
	ReleasedAt      time.Time
	Version         int64
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

type RuntimeCapacityRecoveryReport struct {
	Scanned int
	Expired int
}

type RuntimeCapacityAdmissionService struct {
	DB    *persistence.Database
	Now   func() time.Time
	mu    sync.Mutex
	items map[string]RuntimeCapacityReservation
}

func NewRuntimeCapacityAdmissionService(db *persistence.Database) *RuntimeCapacityAdmissionService {
	return &RuntimeCapacityAdmissionService{DB: db, Now: func() time.Time { return time.Now().UTC() }, items: map[string]RuntimeCapacityReservation{}}
}

func (s *RuntimeCapacityAdmissionService) Reserve(ctx context.Context, command RuntimeCapacityCommand) (RuntimeCapacityReservation, error) {
	if err := validateRuntimeCapacityCommand(command); err != nil {
		return RuntimeCapacityReservation{}, err
	}
	now := s.now()
	var reservation RuntimeCapacityReservation
	if s.postgresReady() {
		err := s.DB.WithSerializableRetry(ctx, "runtime_capacity_reserve", 3, func(tx *persistence.Tx) error {
			// Terminal retry generations must not share an identity: an old
			// delayed terminal event must never release a replacement lease.
			if err := tx.Exec(ctx, `select pg_advisory_xact_lock(hashtextextended(@key,0))`, map[string]any{"key": "runtime-capacity:run:" + command.RunID}); err != nil {
				return err
			}
			versionRows, err := tx.Query(ctx, `select coalesce(max(version),0) version from runtime_capacity_reservations where run_id=@run`, map[string]any{"run": command.RunID})
			if err != nil || len(versionRows) != 1 {
				return err
			}
			reservation = runtimeCapacityReservationFor(command, now, runtimeHostInt64(versionRows[0]["version"])+1)
			dimensions := runtimeCapacityDimensionList(command.Dimensions)
			sort.Slice(dimensions, func(i, j int) bool {
				return dimensions[i].name+dimensions[i].value.Key < dimensions[j].name+dimensions[j].value.Key
			})
			for _, dimension := range dimensions {
				lockKey := "runtime-capacity:" + dimension.name + ":" + dimension.value.Key
				if err := tx.Exec(ctx, `select pg_advisory_xact_lock(hashtextextended(@key,0))`, map[string]any{"key": lockKey}); err != nil {
					return err
				}
				column, keyColumn := runtimeCapacityColumns(dimension.name)
				rows, err := tx.Query(ctx, `select coalesce(sum(`+column+`),0) used from runtime_capacity_reservations where `+keyColumn+`=@key and state in('reserved','accepted','recovering') and (state='accepted' or expires_at>now())`, map[string]any{"key": dimension.value.Key})
				if err != nil || len(rows) != 1 {
					return err
				}
				reserved := int(runtimeHostInt64(rows[0]["used"]))
				used := reserved
				if dimension.value.ObservedUsed > used {
					used = dimension.value.ObservedUsed
				}
				if used+dimension.value.Requested > dimension.value.Limit {
					return fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
				}
			}
			snapshot, _ := json.Marshal(command.Dimensions)
			rows, err := tx.Query(ctx, `insert into runtime_capacity_reservations(capacity_reservation_id,run_id,snapshot_version,state,model_capacity_key,auth_pool_capacity_key,tool_capacity_key,tenant_capacity_key,user_capacity_key,model_units,auth_pool_units,tool_units,tenant_units,user_units,dimension_snapshot,expires_at,version) values(@id,@run,@snapshot,'reserved',@modelKey,@authKey,@toolKey,@tenantKey,@userKey,@modelUnits,@authUnits,@toolUnits,@tenantUnits,@userUnits,@dimensions::jsonb,@expires,@version)
returning version`, map[string]any{
				"id": reservation.ReservationID, "run": reservation.RunID, "snapshot": reservation.SnapshotVersion,
				"modelKey": command.Dimensions.Model.Key, "authKey": command.Dimensions.AuthPool.Key,
				"toolKey": command.Dimensions.Tool.Key, "tenantKey": command.Dimensions.Tenant.Key, "userKey": command.Dimensions.User.Key,
				"modelUnits": command.Dimensions.Model.Requested, "authUnits": command.Dimensions.AuthPool.Requested,
				"toolUnits": command.Dimensions.Tool.Requested, "tenantUnits": command.Dimensions.Tenant.Requested,
				"userUnits": command.Dimensions.User.Requested, "dimensions": string(snapshot), "expires": reservation.ExpiresAt, "version": reservation.Version,
			})
			if err != nil || len(rows) != 1 {
				if err != nil {
					return err
				}
				return fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
			}
			reservation.Version = runtimeHostInt64(rows[0]["version"])
			return nil
		})
		if err != nil {
			if runtimeUniqueViolation(err) {
				return RuntimeCapacityReservation{}, fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
			}
			return RuntimeCapacityReservation{}, err
		}
		return reservation, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, current := range s.items {
		if current.RunID == command.RunID && runtimeCapacityActive(current.State) {
			return RuntimeCapacityReservation{}, fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
		}
	}
	for _, dimension := range runtimeCapacityDimensionList(command.Dimensions) {
		used := dimension.value.ObservedUsed
		for _, current := range s.items {
			if !runtimeCapacityActive(current.State) || current.State == "reserved" && !current.ExpiresAt.After(now) {
				continue
			}
			for _, active := range runtimeCapacityDimensionList(current.Dimensions) {
				if active.name == dimension.name && active.value.Key == dimension.value.Key {
					used += active.value.Requested
				}
			}
		}
		if used+dimension.value.Requested > dimension.value.Limit {
			return RuntimeCapacityReservation{}, fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
		}
	}
	reservation = runtimeCapacityReservationFor(command, now, s.nextMemoryReservationVersion(command.RunID))
	s.items[reservation.ReservationID] = reservation
	return reservation, nil
}

func (s *RuntimeCapacityAdmissionService) AssertActive(ctx context.Context, reservation RuntimeCapacityReservation) error {
	if reservation.ReservationID == "" || reservation.RunID == "" || reservation.SnapshotVersion < 1 {
		return fmt.Errorf("INVALID_ARGUMENT")
	}
	if s.postgresReady() {
		var exists bool
		err := s.DB.Pool.QueryRow(ctx, `select true from runtime_capacity_reservations where capacity_reservation_id=$1 and run_id=$2 and snapshot_version=$3 and state in('reserved','accepted') and (state='accepted' or expires_at>now())`, reservation.ReservationID, reservation.RunID, reservation.SnapshotVersion).Scan(&exists)
		if err != nil || !exists {
			return fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
		}
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.items[reservation.ReservationID]
	if !ok || current.RunID != reservation.RunID || current.SnapshotVersion != reservation.SnapshotVersion || current.State != "accepted" && (current.State != "reserved" || !current.ExpiresAt.After(s.now())) {
		return fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
	}
	return nil
}

func (s *RuntimeCapacityAdmissionService) CommitAccepted(ctx context.Context, reservation RuntimeCapacityReservation) error {
	if reservation.ReservationID == "" || reservation.RunID == "" || reservation.SnapshotVersion < 1 || reservation.Version < 1 {
		return fmt.Errorf("INVALID_ARGUMENT")
	}
	if s.postgresReady() {
		err := s.DB.WithSerializableRetry(ctx, "runtime_capacity_accept", 3, func(tx *persistence.Tx) error {
			return s.CommitAcceptedTx(ctx, tx, reservation)
		})
		if err == nil {
			return nil
		}
		if strings.Contains(err.Error(), "RUNTIME_CAPACITY_UNAVAILABLE") {
			return fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
		}
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.commitAcceptedMemory(reservation)
}

// CommitAcceptedTx applies the exact capacity-generation CAS within a caller
// owned transaction. RuntimeScheduler uses it only after proving that the
// transaction belongs to the same durable database as the Host repository.
func (s *RuntimeCapacityAdmissionService) CommitAcceptedTx(ctx context.Context, tx *persistence.Tx, reservation RuntimeCapacityReservation) error {
	if tx == nil || reservation.ReservationID == "" || reservation.RunID == "" || reservation.SnapshotVersion < 1 || reservation.Version < 1 {
		return fmt.Errorf("INVALID_ARGUMENT")
	}
	rows, err := tx.Query(ctx, `select state,version,expires_at from runtime_capacity_reservations where capacity_reservation_id=@id and run_id=@run and snapshot_version=@snapshot for update`, map[string]any{
		"id": reservation.ReservationID, "run": reservation.RunID, "snapshot": reservation.SnapshotVersion,
	})
	if err != nil || len(rows) != 1 {
		return fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
	}
	state := fmt.Sprint(rows[0]["state"])
	version := runtimeHostInt64(rows[0]["version"])
	if state == "accepted" && version == reservation.Version+1 {
		return nil
	}
	if state != "reserved" || version != reservation.Version {
		return fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
	}
	expiresAt, ok := rows[0]["expires_at"].(time.Time)
	if !ok || !expiresAt.After(s.now()) {
		return fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
	}
	result, err := tx.ExecRaw(ctx, `update runtime_capacity_reservations set state='accepted',accepted_at=coalesce(accepted_at,now()),version=version+1,updated_at=now() where capacity_reservation_id=$1 and run_id=$2 and snapshot_version=$3 and version=$4 and state='reserved' and expires_at>now()`, reservation.ReservationID, reservation.RunID, reservation.SnapshotVersion, reservation.Version)
	if err != nil || result.RowsAffected() != 1 {
		return fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
	}
	return nil
}

func (s *RuntimeCapacityAdmissionService) commitAcceptedMemory(reservation RuntimeCapacityReservation) error {
	current, ok := s.items[reservation.ReservationID]
	if !ok || current.RunID != reservation.RunID || current.SnapshotVersion != reservation.SnapshotVersion {
		return fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
	}
	if current.State == "accepted" && current.Version == reservation.Version+1 {
		return nil
	}
	if current.State != "reserved" || current.Version != reservation.Version || !current.ExpiresAt.After(s.now()) {
		return fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
	}
	current.State, current.AcceptedAt, current.UpdatedAt = "accepted", s.now(), s.now()
	current.Version++
	s.items[current.ReservationID] = current
	return nil
}

func (s *RuntimeCapacityAdmissionService) GetActiveByRunID(ctx context.Context, runID string) (RuntimeCapacityReservation, error) {
	if strings.TrimSpace(runID) == "" {
		return RuntimeCapacityReservation{}, fmt.Errorf("INVALID_ARGUMENT")
	}
	if s.postgresReady() {
		var item RuntimeCapacityReservation
		var dimensions []byte
		var acceptedAt, releasedAt *time.Time
		err := s.DB.Pool.QueryRow(ctx, `select capacity_reservation_id,run_id,snapshot_version,state,dimension_snapshot,expires_at,accepted_at,released_at,version,created_at,updated_at from runtime_capacity_reservations where run_id=$1 and state in('reserved','accepted','recovering') order by created_at desc limit 1`, runID).Scan(&item.ReservationID, &item.RunID, &item.SnapshotVersion, &item.State, &dimensions, &item.ExpiresAt, &acceptedAt, &releasedAt, &item.Version, &item.CreatedAt, &item.UpdatedAt)
		if err != nil {
			return RuntimeCapacityReservation{}, runtimeCapacityLookupError(err)
		}
		if acceptedAt != nil {
			item.AcceptedAt = *acceptedAt
		}
		if releasedAt != nil {
			item.ReleasedAt = *releasedAt
		}
		if err := json.Unmarshal(dimensions, &item.Dimensions); err != nil {
			return RuntimeCapacityReservation{}, err
		}
		return item, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, item := range s.items {
		if item.RunID == runID && runtimeCapacityActive(item.State) {
			return item, nil
		}
	}
	return RuntimeCapacityReservation{}, fmt.Errorf("NOT_FOUND")
}

func (s *RuntimeCapacityAdmissionService) GetLatestByRunID(ctx context.Context, runID string) (RuntimeCapacityReservation, error) {
	if strings.TrimSpace(runID) == "" {
		return RuntimeCapacityReservation{}, fmt.Errorf("INVALID_ARGUMENT")
	}
	if s.postgresReady() {
		var item RuntimeCapacityReservation
		var dimensions []byte
		var acceptedAt, releasedAt *time.Time
		err := s.DB.Pool.QueryRow(ctx, `select capacity_reservation_id,run_id,snapshot_version,state,dimension_snapshot,expires_at,accepted_at,released_at,version,created_at,updated_at
from runtime_capacity_reservations where run_id=$1 order by version desc,created_at desc limit 1`, runID).Scan(
			&item.ReservationID, &item.RunID, &item.SnapshotVersion, &item.State, &dimensions, &item.ExpiresAt,
			&acceptedAt, &releasedAt, &item.Version, &item.CreatedAt, &item.UpdatedAt,
		)
		if err != nil {
			return RuntimeCapacityReservation{}, runtimeCapacityLookupError(err)
		}
		if acceptedAt != nil {
			item.AcceptedAt = *acceptedAt
		}
		if releasedAt != nil {
			item.ReleasedAt = *releasedAt
		}
		if err := json.Unmarshal(dimensions, &item.Dimensions); err != nil {
			return RuntimeCapacityReservation{}, err
		}
		return item, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var latest RuntimeCapacityReservation
	for _, item := range s.items {
		if item.RunID != runID {
			continue
		}
		if latest.ReservationID == "" || item.Version > latest.Version || item.Version == latest.Version && item.CreatedAt.After(latest.CreatedAt) {
			latest = item
		}
	}
	if latest.ReservationID == "" {
		return RuntimeCapacityReservation{}, fmt.Errorf("NOT_FOUND")
	}
	return latest, nil
}

// PostgreSQL returns pgx.ErrNoRows for an absent reservation. Callers use the
// repository-wide NOT_FOUND contract to distinguish that normal recovery case
// from storage failure.
func runtimeCapacityLookupError(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("NOT_FOUND")
	}
	return err
}

func (s *RuntimeCapacityAdmissionService) Release(ctx context.Context, reservation RuntimeCapacityReservation, actualUsage map[string]any) (bool, error) {
	if reservation.ReservationID == "" || reservation.RunID == "" {
		return false, fmt.Errorf("INVALID_ARGUMENT")
	}
	usage := sanitizeRuntimeCapacityUsage(actualUsage)
	if s.postgresReady() {
		payload, _ := json.Marshal(usage)
		result, err := s.DB.Pool.Exec(ctx, `update runtime_capacity_reservations set state='released',actual_usage=$4::jsonb,released_at=coalesce(released_at,now()),release_reason='terminal',version=version+1,updated_at=now() where capacity_reservation_id=$1 and run_id=$2 and snapshot_version=$3 and state in('reserved','accepted','recovering') and (version=$5 or version=$6)`, reservation.ReservationID, reservation.RunID, reservation.SnapshotVersion, string(payload), reservation.Version, reservation.Version+1)
		if err != nil {
			return false, err
		}
		return result.RowsAffected() == 1, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.items[reservation.ReservationID]
	if !ok || current.RunID != reservation.RunID || current.SnapshotVersion != reservation.SnapshotVersion {
		return false, fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
	}
	if current.State == "released" || current.State == "expired" {
		return false, nil
	}
	if current.Version != reservation.Version && current.Version != reservation.Version+1 {
		return false, nil
	}
	current.State, current.ReleasedAt, current.UpdatedAt = "released", s.now(), s.now()
	current.Version++
	s.items[current.ReservationID] = current
	return true, nil
}

func (s *RuntimeCapacityAdmissionService) Recover(ctx context.Context, now time.Time, limit int) (RuntimeCapacityRecoveryReport, error) {
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}
	report := RuntimeCapacityRecoveryReport{}
	if s.postgresReady() {
		result, err := s.DB.Pool.Exec(ctx, `with candidates as (select capacity_reservation_id from runtime_capacity_reservations where state in('reserved','recovering') and expires_at<=$1 order by expires_at limit $2 for update skip locked) update runtime_capacity_reservations r set state='expired',released_at=coalesce(released_at,now()),release_reason='lease_expired',version=version+1,updated_at=now() from candidates c where r.capacity_reservation_id=c.capacity_reservation_id`, now, limit)
		if err != nil {
			return report, err
		}
		report.Scanned, report.Expired = int(result.RowsAffected()), int(result.RowsAffected())
		return report, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, item := range s.items {
		if report.Scanned >= limit {
			break
		}
		if (item.State == "reserved" || item.State == "recovering") && !item.ExpiresAt.After(now) {
			report.Scanned++
			item.State, item.ReleasedAt, item.UpdatedAt = "expired", now, now
			item.Version++
			s.items[id] = item
			report.Expired++
		}
	}
	return report, nil
}

type namedRuntimeCapacityDimension struct {
	name  string
	value RuntimeCapacityDimension
}

func runtimeCapacityDimensionList(dimensions RuntimeCapacityDimensions) []namedRuntimeCapacityDimension {
	return []namedRuntimeCapacityDimension{{"model", dimensions.Model}, {"auth_pool", dimensions.AuthPool}, {"tool", dimensions.Tool}, {"tenant", dimensions.Tenant}, {"user", dimensions.User}}
}

func runtimeCapacityColumns(name string) (string, string) {
	switch name {
	case "model":
		return "model_units", "model_capacity_key"
	case "auth_pool":
		return "auth_pool_units", "auth_pool_capacity_key"
	case "tool":
		return "tool_units", "tool_capacity_key"
	case "tenant":
		return "tenant_units", "tenant_capacity_key"
	default:
		return "user_units", "user_capacity_key"
	}
}

func validateRuntimeCapacityCommand(command RuntimeCapacityCommand) error {
	if strings.TrimSpace(command.RunID) == "" || command.SnapshotVersion < 1 || command.TTL <= 0 {
		return fmt.Errorf("INVALID_ARGUMENT")
	}
	for _, dimension := range runtimeCapacityDimensionList(command.Dimensions) {
		value := dimension.value
		if strings.TrimSpace(value.Key) == "" || value.Version < 1 || value.Version != command.SnapshotVersion || value.Limit < 0 || value.ObservedUsed < 0 || value.Requested <= 0 || value.ObservedUsed > value.Limit {
			return fmt.Errorf("INVALID_ARGUMENT")
		}
		if value.Limit == 0 {
			return fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
		}
	}
	return nil
}

func runtimeCapacityActive(state string) bool {
	return stringInRuntime(state, []string{"reserved", "accepted", "recovering"})
}

func runtimeCapacityReservationFor(command RuntimeCapacityCommand, now time.Time, version int64) RuntimeCapacityReservation {
	return RuntimeCapacityReservation{
		ReservationID: stableRuntimeCapacityID(command.RunID, command.SnapshotVersion, version),
		RunID:         command.RunID, SnapshotVersion: command.SnapshotVersion,
		Dimensions: command.Dimensions, State: "reserved", ExpiresAt: now.Add(command.TTL),
		Version: version, CreatedAt: now, UpdatedAt: now,
	}
}

func (s *RuntimeCapacityAdmissionService) nextMemoryReservationVersion(runID string) int64 {
	var latest int64
	for _, item := range s.items {
		if item.RunID == runID && item.Version > latest {
			latest = item.Version
		}
	}
	return latest + 1
}

func stableRuntimeCapacityID(runID string, snapshotVersion, generation int64) string {
	sum := sha256.Sum256([]byte(runID + "\x00" + fmt.Sprint(snapshotVersion) + "\x00" + fmt.Sprint(generation)))
	return "capacity_" + hex.EncodeToString(sum[:])[:32]
}

func sanitizeRuntimeCapacityUsage(input map[string]any) map[string]any {
	out := map[string]any{}
	for key, value := range input {
		switch key {
		case "modelTokens", "toolCalls", "durationMs", "inputTokens", "outputTokens":
			out[key] = value
		}
	}
	return out
}

func (s *RuntimeCapacityAdmissionService) now() time.Time {
	if s.Now != nil {
		return s.Now().UTC()
	}
	return time.Now().UTC()
}

func (s *RuntimeCapacityAdmissionService) postgresReady() bool {
	return s != nil && s.DB != nil && !s.DB.Disabled && s.DB.Pool != nil
}
