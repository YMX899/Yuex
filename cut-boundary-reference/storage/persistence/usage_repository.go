package persistence

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"huahuoai/backend/source/internal/config"

	"github.com/jackc/pgx/v5"
)

var (
	ErrQuotaInsufficient        = errors.New("quota insufficient")
	ErrUsageIdempotencyConflict = errors.New("usage idempotency conflict")
)

// QuotaAdmissionReservationCommand is the transaction-owned admission input
// used by AgentRun confirmation. The caller owns the surrounding Run/Plan/
// queue transaction; this repository deliberately does not open another one.
type QuotaAdmissionReservationCommand struct {
	PermissionCheckID string
	TraceID           string
	UserID            string
	WorkspaceID       string
	TaskID            string
	TaskType          string
	Estimate          map[string]any
	Meters            map[string]int
	ExpiresAt         time.Time
}

type QuotaAdmissionReservation struct {
	PermissionCheckID string
	ReservationID     string
	Allowed           bool
	DenyReason        string
	QuotaSnapshot     map[string]any
}

type UsageRepository struct {
	mu                sync.Mutex
	usage             map[string]map[string]any
	checks            map[string]map[string]any
	reserves          map[string]map[string]any
	adjustments       map[string]map[string]any
	memoryMemberships map[string]map[string]any
	featureFlagConfig *ConfigRepository
	featureFlagEnv    string
	db                *Database
}

type PermissionCheckRepository interface {
	CreatePermissionCheck(traceID, userID, workspaceID, taskType string, estimate map[string]any) string
	MarkPermissionAllowed(permissionCheckID string, quotaSnapshot map[string]any, reservationID string) map[string]any
	MarkPermissionDenied(permissionCheckID, denyReason string, quotaSnapshot map[string]any) map[string]any
}

type UsageRecordRepository interface {
	CreateUsageRecordOnce(command map[string]any) (map[string]any, bool)
	SettleUsageOnce(command map[string]any, reservationID string) (map[string]any, bool, error)
	ListMissingUsageCandidates(window string, taskTypes []string) []map[string]any
}

type QuotaBalanceRepository interface {
	LockQuotaForUser(userID string) map[string]any
}

type QuotaReservationRepository interface {
	CreateReservation(permissionCheckID, taskID, taskType string, meters map[string]int, expiresAt string) (string, error)
	SettleReservation(reservationID string, actualUsageRefs []string) map[string]any
	ReleaseReservation(reservationID, reason string) map[string]any
	ExpireReservation(reservationID, recoveryRunID string) map[string]any
}

type QuotaAdjustmentRepository interface {
	CreateQuotaAdjustment(operatorID, userID, quotaType string, delta int, reason string) (map[string]any, error)
}

// MetaWorkspaceEntitlement is the minimum user-scoped authorization fact
// needed by L1 Meta Workspace routing. It is intentionally separate from
// quota admission: the App, upload service, and planning worker must all
// make the same availability decision before a Run can be created or used.
// A negative MembershipLevel is a fail-closed snapshot.
type MetaWorkspaceEntitlement struct {
	MembershipLevel int
	Features        map[string]bool
}

type MetaWorkspaceEntitlementRepository interface {
	ResolveMetaWorkspaceEntitlement(ctx context.Context, userID string) (MetaWorkspaceEntitlement, error)
}

var (
	_ PermissionCheckRepository          = (*UsageRepository)(nil)
	_ UsageRecordRepository              = (*UsageRepository)(nil)
	_ QuotaBalanceRepository             = (*UsageRepository)(nil)
	_ QuotaReservationRepository         = (*UsageRepository)(nil)
	_ QuotaAdjustmentRepository          = (*UsageRepository)(nil)
	_ MetaWorkspaceEntitlementRepository = (*UsageRepository)(nil)
)

func NewUsageRepository(db ...*Database) *UsageRepository {
	repo := &UsageRepository{usage: map[string]map[string]any{}, checks: map[string]map[string]any{}, reserves: map[string]map[string]any{}, adjustments: map[string]map[string]any{}, memoryMemberships: map[string]map[string]any{}, featureFlagEnv: "local"}
	if len(db) > 0 {
		repo.db = db[0]
	}
	return repo
}

// AttachFeatureFlagConfig supplies the explicit in-memory test mirror for
// AgentRun admission. Durable confirmations read and lock feature_flags using
// their caller-owned transaction instead of consulting this repository.
func (r *UsageRepository) AttachFeatureFlagConfig(repository *ConfigRepository) {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.featureFlagConfig = repository
	r.mu.Unlock()
}

// SetFeatureFlagEnvironment is process-owned configuration, never App input.
// It makes environment-scoped global flag rules deterministic in the explicit
// in-memory test backend; PostgreSQL admission uses the same subject field.
func (r *UsageRepository) SetFeatureFlagEnvironment(environment string) {
	if r == nil {
		return
	}
	environment = strings.TrimSpace(environment)
	if environment == "" {
		environment = "local"
	}
	r.mu.Lock()
	r.featureFlagEnv = environment
	r.mu.Unlock()
}

// ResolveMetaWorkspaceEntitlement derives the request user's currently
// active membership tier and membership-level feature grants. A configured
// durable store is authoritative: unavailable, malformed, expired, blocked,
// or disabled state never falls back to a process-local allow decision.
func (r *UsageRepository) ResolveMetaWorkspaceEntitlement(ctx context.Context, userID string) (MetaWorkspaceEntitlement, error) {
	userID = strings.TrimSpace(userID)
	if r == nil || ctx == nil || userID == "" {
		return MetaWorkspaceEntitlement{}, fmt.Errorf("meta workspace entitlement unavailable")
	}
	if r.postgresEnabled() {
		return r.resolveMetaWorkspaceEntitlementPostgres(ctx, userID)
	}
	if r.db != nil && !r.db.Disabled {
		return MetaWorkspaceEntitlement{}, fmt.Errorf("meta workspace entitlement durable store unavailable")
	}
	return r.resolveMetaWorkspaceEntitlementMemory(userID, time.Now().UTC()), nil
}

func (r *UsageRepository) resolveMetaWorkspaceEntitlementPostgres(ctx context.Context, userID string) (MetaWorkspaceEntitlement, error) {
	if err := ctx.Err(); err != nil {
		return MetaWorkspaceEntitlement{}, err
	}
	var membershipStatus, levelStatus, levelCode string
	var expiresAt *time.Time
	var featureFlagsRaw, quotaConfigRaw []byte
	err := r.db.Pool.QueryRow(ctx, `
select m.status, m.expires_at, ml.status, ml.feature_flags, ml.quota_config, m.level_code
from memberships m
join membership_levels ml on ml.level_code = m.level_code
where m.user_id = $1
order by case when m.status = 'active' then 0 when m.status = 'trial' then 1 when m.status = 'free' then 2 else 3 end, m.updated_at desc
limit 1`, userID).Scan(&membershipStatus, &expiresAt, &levelStatus, &featureFlagsRaw, &quotaConfigRaw, &levelCode)
	if errors.Is(err, pgx.ErrNoRows) {
		err = r.db.Pool.QueryRow(ctx, `
select ml.status, ml.feature_flags, ml.quota_config, ml.level_code
from membership_levels ml
where ml.level_code = 'trial'
limit 1`).Scan(&levelStatus, &featureFlagsRaw, &quotaConfigRaw, &levelCode)
		if errors.Is(err, pgx.ErrNoRows) {
			return deniedMetaWorkspaceEntitlement(), nil
		}
		if err != nil {
			return MetaWorkspaceEntitlement{}, fmt.Errorf("load default meta workspace entitlement: %w", err)
		}
		membershipStatus = "trial"
	} else if err != nil {
		return MetaWorkspaceEntitlement{}, fmt.Errorf("load meta workspace entitlement: %w", err)
	}
	features, err := metaWorkspaceEntitlementFeatureFlags(featureFlagsRaw)
	if err != nil {
		return MetaWorkspaceEntitlement{}, fmt.Errorf("decode meta workspace feature state: %w", err)
	}
	quotaConfig, err := metaWorkspaceEntitlementObject(quotaConfigRaw)
	if err != nil {
		return MetaWorkspaceEntitlement{}, fmt.Errorf("decode meta workspace membership tier: %w", err)
	}
	if !metaWorkspaceMembershipActive(membershipStatus, levelStatus, expiresAt, time.Now().UTC()) {
		return deniedMetaWorkspaceEntitlement(), nil
	}
	return MetaWorkspaceEntitlement{
		MembershipLevel: metaWorkspaceMembershipLevel(levelCode, quotaConfig),
		Features:        features,
	}, nil
}

func (r *UsageRepository) resolveMetaWorkspaceEntitlementMemory(userID string, now time.Time) MetaWorkspaceEntitlement {
	r.mu.Lock()
	policy := copyMap(r.memoryMemberships[userID])
	r.mu.Unlock()
	membershipStatus := stringOr(policy["status"], "trial")
	levelStatus := stringOr(policy["levelStatus"], "active")
	var expiresAt *time.Time
	if value := timeValue(policy["expiresAt"], time.Time{}); !value.IsZero() {
		value = value.UTC()
		expiresAt = &value
	}
	if !metaWorkspaceMembershipActive(membershipStatus, levelStatus, expiresAt, now) {
		return deniedMetaWorkspaceEntitlement()
	}
	features, ok := metaWorkspaceEntitlementFeaturesFromValue(policy["featureFlags"])
	if !ok {
		return deniedMetaWorkspaceEntitlement()
	}
	if policy["featureFlags"] == nil {
		// This mirrors the explicit in-memory trial policy already used by
		// quota admission. It exists only when the durable database is disabled.
		features = map[string]bool{"feed_ai": true, "work_ai": true}
	}
	quotaConfig, quotaConfigOK := metaWorkspaceEntitlementMapFromValue(policy["quotaConfig"])
	if !quotaConfigOK {
		return deniedMetaWorkspaceEntitlement()
	}
	if level, present := metaWorkspaceEntitlementInteger(policy["membershipLevel"]); present {
		return MetaWorkspaceEntitlement{MembershipLevel: level, Features: features}
	}
	return MetaWorkspaceEntitlement{
		MembershipLevel: metaWorkspaceMembershipLevel(stringOr(policy["levelCode"], "trial"), quotaConfig),
		Features:        features,
	}
}

func deniedMetaWorkspaceEntitlement() MetaWorkspaceEntitlement {
	return MetaWorkspaceEntitlement{MembershipLevel: -1, Features: map[string]bool{}}
}

func metaWorkspaceMembershipActive(membershipStatus, levelStatus string, expiresAt *time.Time, now time.Time) bool {
	if !stringIn(strings.TrimSpace(membershipStatus), []string{"free", "trial", "active"}) || strings.TrimSpace(levelStatus) != "active" {
		return false
	}
	return expiresAt == nil || expiresAt.After(now)
}

func metaWorkspaceEntitlementFeatureFlags(raw []byte) (map[string]bool, error) {
	values, err := metaWorkspaceEntitlementObject(raw)
	if err != nil {
		return nil, err
	}
	features, ok := metaWorkspaceEntitlementFeaturesFromValue(values)
	if !ok {
		return nil, fmt.Errorf("feature flags must be boolean")
	}
	return features, nil
}

func metaWorkspaceEntitlementObject(raw []byte) (map[string]any, error) {
	if len(raw) == 0 {
		return map[string]any{}, nil
	}
	values := map[string]any{}
	if err := json.Unmarshal(raw, &values); err != nil || values == nil {
		if err == nil {
			err = fmt.Errorf("object is null")
		}
		return nil, err
	}
	return values, nil
}

func metaWorkspaceEntitlementFeaturesFromValue(value any) (map[string]bool, bool) {
	if typed, ok := value.(map[string]bool); ok && typed != nil {
		features := make(map[string]bool, len(typed))
		for key, enabled := range typed {
			if strings.TrimSpace(key) == "" {
				return nil, false
			}
			features[key] = enabled
		}
		return features, true
	}
	values, ok := metaWorkspaceEntitlementMapFromValue(value)
	if !ok {
		return nil, false
	}
	features := make(map[string]bool, len(values))
	for key, value := range values {
		enabled, valid := value.(bool)
		if !valid || strings.TrimSpace(key) == "" {
			return nil, false
		}
		features[key] = enabled
	}
	return features, true
}

func metaWorkspaceEntitlementMapFromValue(value any) (map[string]any, bool) {
	if value == nil {
		return map[string]any{}, true
	}
	if typed, ok := value.(map[string]any); ok && typed != nil {
		return copyMap(typed), true
	}
	return nil, false
}

func metaWorkspaceMembershipLevel(levelCode string, quotaConfig map[string]any) int {
	for _, value := range []any{quotaConfig["metaWorkspaceMembershipLevel"], quotaConfig["membershipLevel"]} {
		if level, present := metaWorkspaceEntitlementInteger(value); present {
			return level
		}
	}
	switch strings.ToLower(strings.TrimSpace(levelCode)) {
	case "free":
		return 0
	case "trial":
		return 1
	case "pilot_paid", "basic", "paid", "pro":
		return 2
	case "enterprise":
		return 3
	default:
		return 0
	}
}

func metaWorkspaceEntitlementInteger(value any) (int, bool) {
	switch typed := value.(type) {
	case int:
		return boundedMetaWorkspaceMembershipLevel(typed)
	case int32:
		return boundedMetaWorkspaceMembershipLevel(int(typed))
	case int64:
		return boundedMetaWorkspaceMembershipLevel(int(typed))
	case float64:
		if typed != float64(int(typed)) {
			return 0, false
		}
		return boundedMetaWorkspaceMembershipLevel(int(typed))
	case json.Number:
		parsed, err := typed.Int64()
		if err != nil {
			return 0, false
		}
		return boundedMetaWorkspaceMembershipLevel(int(parsed))
	default:
		return 0, false
	}
}

func boundedMetaWorkspaceMembershipLevel(level int) (int, bool) {
	if level < 0 || level > 1000 {
		return 0, false
	}
	return level, true
}

func (r *UsageRepository) CreatePermissionCheck(traceID, userID, workspaceID, taskType string, estimate map[string]any) string {
	if id, err := r.createPermissionCheckPostgres(context.Background(), traceID, userID, workspaceID, taskType, estimate); err == nil {
		return id
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	id := stringOr(estimate["permissionCheckId"], "perm_check_"+usageIDPart(traceID+"_"+userID+"_"+taskType+"_"+fmt.Sprint(time.Now().UTC().UnixNano())))
	r.checks[id] = map[string]any{"permissionCheckId": id, "traceId": traceID, "userId": userID, "workspaceId": workspaceID, "taskType": taskType, "status": "check_requested", "estimate": copyMap(estimate), "createdAt": time.Now().UTC().Format(time.RFC3339)}
	return id
}

func (r *UsageRepository) MarkPermissionAllowed(permissionCheckID string, quotaSnapshot map[string]any, reservationID string) map[string]any {
	if check, err := r.markPermissionPostgres(context.Background(), permissionCheckID, "allowed", "", quotaSnapshot, reservationID); err == nil {
		return check
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	check := copyMap(r.checks[permissionCheckID])
	check["permissionCheckId"] = permissionCheckID
	check["status"] = "allowed"
	check["result"] = "allowed"
	check["allowed"] = true
	check["reservationId"] = reservationID
	check["quotaSnapshot"] = quotaSnapshot
	r.checks[permissionCheckID] = check
	return copyMap(check)
}

func (r *UsageRepository) MarkPermissionDenied(permissionCheckID, denyReason string, quotaSnapshot map[string]any) map[string]any {
	if check, err := r.markPermissionPostgres(context.Background(), permissionCheckID, "denied", denyReason, quotaSnapshot, ""); err == nil {
		return check
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	check := copyMap(r.checks[permissionCheckID])
	check["permissionCheckId"] = permissionCheckID
	check["status"] = "denied"
	check["result"] = "denied"
	check["allowed"] = false
	check["denyReason"] = denyReason
	check["quotaSnapshot"] = quotaSnapshot
	r.checks[permissionCheckID] = check
	return copyMap(check)
}

func (r *UsageRepository) LockQuotaForUser(userID string) map[string]any {
	if lock, err := r.lockQuotaForUserPostgres(context.Background(), userID); err == nil {
		return lock
	}
	return map[string]any{"userId": userID, "locked": true}
}

func (r *UsageRepository) CreateReservation(permissionCheckID, taskID, taskType string, meters map[string]int, expiresAt string) (string, error) {
	if r.postgresEnabled() {
		return r.createReservationPostgres(context.Background(), permissionCheckID, taskID, taskType, meters, expiresAt)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	id := "reservation_" + taskID
	check := r.checks[permissionCheckID]
	r.reserves[id] = map[string]any{"reservationId": id, "permissionCheckId": permissionCheckID, "userId": stringOr(check["userId"], ""), "workspaceId": stringOr(check["workspaceId"], ""), "taskId": taskID, "taskType": taskType, "meters": meters, "expiresAt": expiresAt, "status": "reserved", "createdAt": time.Now().UTC().Format(time.RFC3339), "updatedAt": time.Now().UTC().Format(time.RFC3339)}
	return id, nil
}

// AdmitAndReserveInTx is the only durable admission primitive for operations
// that must atomically pair permission evidence, quota reservation, and an
// application state transition. It intentionally accepts a caller-owned Tx so
// it cannot commit a reservation before the caller commits its Run/Plan/queue
// changes. A quota denial is a committed audit result with Allowed=false; it
// creates no reservation and leaves the caller responsible for retaining its
// pre-admission business state.
func (r *UsageRepository) AdmitAndReserveInTx(ctx context.Context, tx *Tx, command QuotaAdmissionReservationCommand) (QuotaAdmissionReservation, error) {
	if r == nil || tx == nil || tx.tx == nil {
		return QuotaAdmissionReservation{}, fmt.Errorf("quota admission transaction disabled")
	}
	command.PermissionCheckID = strings.TrimSpace(command.PermissionCheckID)
	command.TraceID = strings.TrimSpace(command.TraceID)
	command.UserID = strings.TrimSpace(command.UserID)
	command.WorkspaceID = strings.TrimSpace(command.WorkspaceID)
	command.TaskID = strings.TrimSpace(command.TaskID)
	command.TaskType = strings.TrimSpace(command.TaskType)
	if command.PermissionCheckID == "" || command.TraceID == "" || command.UserID == "" || command.TaskID == "" || command.TaskType == "" {
		return QuotaAdmissionReservation{}, fmt.Errorf("INVALID_ARGUMENT")
	}
	if command.Estimate == nil {
		command.Estimate = map[string]any{}
	}
	meters := sortedPositiveQuotaMeters(command.Meters)
	if len(meters) == 0 {
		meters = []quotaMeterAmount{{quotaType: "generation", amount: 1}}
	}
	if command.ExpiresAt.IsZero() {
		command.ExpiresAt = time.Now().UTC().Add(90 * time.Minute)
	}

	var checkUserID, checkWorkspaceID, checkTaskType, checkStatus, checkDenyReason string
	var existingSnapshotRaw []byte
	checkErr := tx.QueryRowRaw(ctx, `
select user_id, coalesce(workspace_id, ''), task_type, status, coalesce(deny_reason, ''), quota_snapshot
from permission_checks
where permission_check_id = $1
for update`, command.PermissionCheckID).Scan(&checkUserID, &checkWorkspaceID, &checkTaskType, &checkStatus, &checkDenyReason, &existingSnapshotRaw)
	if errors.Is(checkErr, pgx.ErrNoRows) {
		if _, err := tx.ExecRaw(ctx, `
insert into permission_checks(permission_check_id, trace_id, user_id, workspace_id, task_type, status, estimate)
values ($1, $2, $3, nullif($4, ''), $5, 'allowed', $6::jsonb)`,
			command.PermissionCheckID, command.TraceID, command.UserID, command.WorkspaceID, command.TaskType, jsonString(mapValue(command.Estimate))); err != nil {
			return QuotaAdmissionReservation{}, err
		}
		checkUserID, checkWorkspaceID, checkTaskType, checkStatus = command.UserID, command.WorkspaceID, command.TaskType, "allowed"
	} else if checkErr != nil {
		return QuotaAdmissionReservation{}, checkErr
	}
	if checkUserID != command.UserID || checkWorkspaceID != command.WorkspaceID || checkTaskType != command.TaskType {
		return QuotaAdmissionReservation{}, fmt.Errorf("quota admission idempotency conflict")
	}
	if checkStatus == "denied" {
		return QuotaAdmissionReservation{PermissionCheckID: command.PermissionCheckID, Allowed: false, DenyReason: firstUsageAdmissionDenyReason(checkDenyReason), QuotaSnapshot: jsonMap(existingSnapshotRaw)}, nil
	}
	if checkStatus != "allowed" {
		return QuotaAdmissionReservation{}, fmt.Errorf("quota permission status conflict: %s", checkStatus)
	}
	membershipDenyReason, featureSubject, err := r.membershipAdmissionDenyReasonInTx(ctx, tx, command.UserID, command.TaskType)
	if err != nil {
		return QuotaAdmissionReservation{}, err
	}
	if membershipDenyReason != "" {
		snapshot, snapshotErr := r.AdminQuotaSnapshotInTx(ctx, tx, command.UserID, map[string]any{})
		if snapshotErr != nil {
			return QuotaAdmissionReservation{}, snapshotErr
		}
		if updateErr := updatePermissionCheckSnapshotInTx(ctx, tx, command.PermissionCheckID, "denied", membershipDenyReason, snapshot); updateErr != nil {
			return QuotaAdmissionReservation{}, updateErr
		}
		return QuotaAdmissionReservation{PermissionCheckID: command.PermissionCheckID, Allowed: false, DenyReason: membershipDenyReason, QuotaSnapshot: snapshot}, nil
	}
	if globalDenyReason, globalErr := r.globalRuntimeFeatureAdmissionDenyReasonInTx(ctx, tx, command.TaskType, featureSubject); globalErr != nil {
		return QuotaAdmissionReservation{}, globalErr
	} else if globalDenyReason != "" {
		snapshot, snapshotErr := r.AdminQuotaSnapshotInTx(ctx, tx, command.UserID, map[string]any{})
		if snapshotErr != nil {
			return QuotaAdmissionReservation{}, snapshotErr
		}
		if updateErr := updatePermissionCheckSnapshotInTx(ctx, tx, command.PermissionCheckID, "denied", globalDenyReason, snapshot); updateErr != nil {
			return QuotaAdmissionReservation{}, updateErr
		}
		return QuotaAdmissionReservation{PermissionCheckID: command.PermissionCheckID, Allowed: false, DenyReason: globalDenyReason, QuotaSnapshot: snapshot}, nil
	}

	var reservationID, reservationStatus, reservationUserID, reservationWorkspaceID, reservationTaskID, reservationTaskType string
	reservationErr := tx.QueryRowRaw(ctx, `
select reservation_id, status, user_id, coalesce(workspace_id, ''), coalesce(task_id, ''), task_type
from quota_reservations
where permission_check_id = $1
for update`, command.PermissionCheckID).Scan(
		&reservationID, &reservationStatus, &reservationUserID, &reservationWorkspaceID, &reservationTaskID, &reservationTaskType,
	)
	if reservationErr == nil {
		if reservationStatus != "reserved" || reservationUserID != command.UserID || reservationWorkspaceID != command.WorkspaceID || reservationTaskID != command.TaskID || reservationTaskType != command.TaskType {
			return QuotaAdmissionReservation{}, fmt.Errorf("quota reservation idempotency conflict")
		}
		if err := r.validateReservationMetersInTx(ctx, tx, reservationID, meters); err != nil {
			return QuotaAdmissionReservation{}, err
		}
		snapshot, err := r.AdminQuotaSnapshotInTx(ctx, tx, command.UserID, map[string]any{})
		if err != nil {
			return QuotaAdmissionReservation{}, err
		}
		if err := updatePermissionCheckSnapshotInTx(ctx, tx, command.PermissionCheckID, "allowed", "", snapshot); err != nil {
			return QuotaAdmissionReservation{}, err
		}
		return QuotaAdmissionReservation{PermissionCheckID: command.PermissionCheckID, ReservationID: reservationID, Allowed: true, QuotaSnapshot: snapshot}, nil
	}
	if !errors.Is(reservationErr, pgx.ErrNoRows) {
		return QuotaAdmissionReservation{}, reservationErr
	}

	for _, meter := range meters {
		balanceID, err := r.ensureQuotaBalanceInTx(ctx, tx, command.UserID, command.WorkspaceID, meter.quotaType)
		if err != nil {
			return QuotaAdmissionReservation{}, err
		}
		if err := r.lockAndCheckQuotaBalanceInTx(ctx, tx, balanceID, meter); err != nil {
			if !errors.Is(err, ErrQuotaInsufficient) {
				return QuotaAdmissionReservation{}, err
			}
			snapshot, snapshotErr := r.AdminQuotaSnapshotInTx(ctx, tx, command.UserID, map[string]any{})
			if snapshotErr != nil {
				return QuotaAdmissionReservation{}, snapshotErr
			}
			if updateErr := updatePermissionCheckSnapshotInTx(ctx, tx, command.PermissionCheckID, "denied", "QUOTA_INSUFFICIENT", snapshot); updateErr != nil {
				return QuotaAdmissionReservation{}, updateErr
			}
			return QuotaAdmissionReservation{PermissionCheckID: command.PermissionCheckID, Allowed: false, DenyReason: "QUOTA_INSUFFICIENT", QuotaSnapshot: snapshot}, nil
		}
	}

	reservationID = "reservation_" + usageIDPart(command.TaskID)
	if _, err := tx.ExecRaw(ctx, `
insert into quota_reservations(reservation_id, permission_check_id, user_id, workspace_id, task_id, task_type, status, expires_at)
values ($1, $2, $3, nullif($4, ''), $5, $6, 'reserved', $7)`,
		reservationID, command.PermissionCheckID, command.UserID, command.WorkspaceID, command.TaskID, command.TaskType, command.ExpiresAt.UTC()); err != nil {
		return QuotaAdmissionReservation{}, err
	}
	for _, meter := range meters {
		if err := r.upsertReservationItemInTx(ctx, tx, reservationID, command.UserID, command.WorkspaceID, meter.quotaType, meter.amount); err != nil {
			return QuotaAdmissionReservation{}, err
		}
	}
	snapshot, err := r.AdminQuotaSnapshotInTx(ctx, tx, command.UserID, map[string]any{})
	if err != nil {
		return QuotaAdmissionReservation{}, err
	}
	if err := updatePermissionCheckSnapshotInTx(ctx, tx, command.PermissionCheckID, "allowed", "", snapshot); err != nil {
		return QuotaAdmissionReservation{}, err
	}
	return QuotaAdmissionReservation{PermissionCheckID: command.PermissionCheckID, ReservationID: reservationID, Allowed: true, QuotaSnapshot: snapshot}, nil
}

func updatePermissionCheckSnapshotInTx(ctx context.Context, tx *Tx, permissionCheckID, status, denyReason string, snapshot map[string]any) error {
	if tx == nil || tx.tx == nil {
		return fmt.Errorf("quota admission transaction disabled")
	}
	tag, err := tx.ExecRaw(ctx, `
update permission_checks
set status = $2,
    quota_snapshot = $3::jsonb,
    deny_reason = nullif($4, '')
where permission_check_id = $1`, permissionCheckID, usagePermissionStatus(status), jsonString(mapValue(snapshot)), denyReason)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("permission check not found")
	}
	return nil
}

func (r *UsageRepository) membershipAdmissionDenyReasonInTx(ctx context.Context, tx *Tx, userID, taskType string) (string, config.FeatureFlagSubject, error) {
	subject := config.FeatureFlagSubject{UserID: userID, Environment: r.featureFlagEnvironment()}
	featureKey := membershipFeatureForTaskType(taskType)
	var membershipStatus, levelStatus, levelCode string
	var expiresAt *time.Time
	var featureFlagsRaw []byte
	err := tx.QueryRowRaw(ctx, `
select m.status, m.expires_at, ml.status, ml.feature_flags, m.level_code, coalesce(u.phone_hash, '')
from memberships m
join membership_levels ml on ml.level_code=m.level_code
left join users u on u.user_id=m.user_id
where m.user_id=$1
order by case when m.status='active' then 0 when m.status='trial' then 1 when m.status='free' then 2 else 3 end, m.updated_at desc
limit 1`, userID).Scan(&membershipStatus, &expiresAt, &levelStatus, &featureFlagsRaw, &levelCode, &subject.PhoneHash)
	if errors.Is(err, pgx.ErrNoRows) {
		err = tx.QueryRowRaw(ctx, `
select ml.status, ml.feature_flags, coalesce(u.phone_hash, '')
from membership_levels ml
left join users u on u.user_id=$1
where ml.level_code='trial'
limit 1`, userID).Scan(&levelStatus, &featureFlagsRaw, &subject.PhoneHash)
		if errors.Is(err, pgx.ErrNoRows) {
			return "MEMBER_EXPIRED", subject, nil
		}
		if err != nil {
			return "", subject, err
		}
		membershipStatus = "trial"
		levelCode = "trial"
	} else if err != nil {
		return "", subject, err
	}
	subject.MemberLevel = levelCode
	if membershipStatus == "blocked" {
		return "MEMBER_BLOCKED", subject, nil
	}
	if membershipStatus == "expired" || levelStatus != "active" || expiresAt != nil && !expiresAt.After(time.Now().UTC()) {
		return "MEMBER_EXPIRED", subject, nil
	}
	if !stringIn(membershipStatus, []string{"free", "trial", "active"}) {
		return "MEMBER_EXPIRED", subject, nil
	}
	if featureKey == "" {
		return "", subject, nil
	}
	flags := jsonMap(featureFlagsRaw)
	allowed, ok := flags[featureKey].(bool)
	if !ok || !allowed {
		return "FEATURE_NOT_ALLOWED", subject, nil
	}
	return "", subject, nil
}

// memoryMembershipAdmissionDenyReason mirrors the durable membership gate for
// the explicit in-memory test backend. With no injected test policy it models
// the active trial level used by the production fallback; an injected policy
// must carry the same status, expiry and feature decisions as PostgreSQL.
func (r *UsageRepository) memoryMembershipAdmissionDenyReason(userID, taskType string, now time.Time) string {
	denyReason, _ := r.memoryMembershipAdmissionSubject(userID, taskType, now)
	return denyReason
}

func (r *UsageRepository) memoryMembershipAdmissionSubject(userID, taskType string, now time.Time) (string, config.FeatureFlagSubject) {
	if r == nil {
		return "MEMBER_EXPIRED", config.FeatureFlagSubject{UserID: userID, Environment: "local"}
	}
	r.mu.Lock()
	policy := copyMap(r.memoryMemberships[userID])
	environment := r.featureFlagEnv
	r.mu.Unlock()
	if environment == "" {
		environment = "local"
	}
	internalUser, _ := policy["internalUser"].(bool)
	subject := config.FeatureFlagSubject{
		UserID: userID, PhoneHash: stringOr(policy["phoneHash"], ""),
		MemberLevel: stringOr(policy["levelCode"], "trial"), Environment: environment, InternalUser: internalUser,
	}
	membershipStatus := stringOr(policy["status"], "trial")
	levelStatus := stringOr(policy["levelStatus"], "active")
	if membershipStatus == "blocked" {
		return "MEMBER_BLOCKED", subject
	}
	if membershipStatus == "expired" || levelStatus != "active" {
		return "MEMBER_EXPIRED", subject
	}
	if expiresAt := timeValue(policy["expiresAt"], time.Time{}); !expiresAt.IsZero() && !expiresAt.After(now) {
		return "MEMBER_EXPIRED", subject
	}
	if !stringIn(membershipStatus, []string{"free", "trial", "active"}) {
		return "MEMBER_EXPIRED", subject
	}
	featureKey := membershipFeatureForTaskType(taskType)
	if featureKey == "" {
		return "", subject
	}
	flags := mapValue(policy["featureFlags"])
	if len(flags) == 0 && policy["featureFlags"] == nil {
		flags = map[string]any{"feed_ai": true, "work_ai": true}
	}
	allowed, ok := flags[featureKey].(bool)
	if !ok || !allowed {
		return "FEATURE_NOT_ALLOWED", subject
	}
	return "", subject
}

// globalRuntimeFeatureAdmissionDenyReasonInTx evaluates the platform-wide
// Work/Feed Runtime kill switch inside the same transaction that owns the
// permission check, reservation, Run, task and runtime queue. Unknown task
// types deliberately have no Work/Feed global flag mapping; they remain under
// their own persisted Plan, membership and quota admission contracts.
func (r *UsageRepository) globalRuntimeFeatureAdmissionDenyReasonInTx(ctx context.Context, tx *Tx, taskType string, subject config.FeatureFlagSubject) (string, error) {
	flagKey, disabledCode, applies := runtimeGlobalFeatureFlagForTaskType(taskType)
	if !applies {
		return "", nil
	}
	var variant, status string
	var rulesRaw []byte
	err := tx.QueryRowRaw(ctx, `
select variant, rules, status
from feature_flags
where flag_key=$1
for update`, flagKey).Scan(&variant, &rulesRaw, &status)
	if errors.Is(err, pgx.ErrNoRows) {
		return "APP_CONFIG_UNAVAILABLE", nil
	}
	if err != nil {
		return "", err
	}
	if status == "disabled" {
		return disabledCode, nil
	}
	if status != "active" || !validRuntimeFeatureFlagVariant(variant) {
		return "APP_CONFIG_UNAVAILABLE", nil
	}
	rules := map[string]any{}
	if len(rulesRaw) == 0 || json.Unmarshal(rulesRaw, &rules) != nil || rules == nil {
		return "APP_CONFIG_UNAVAILABLE", nil
	}
	decision := config.NewFeatureFlagEvaluator([]map[string]any{{
		"flagKey": flagKey, "variant": variant, "rules": rules,
	}}).Evaluate(flagKey, subject)
	if !decision.Allowed {
		return disabledCode, nil
	}
	return "", nil
}

func (r *UsageRepository) memoryGlobalRuntimeFeatureAdmissionDenyReason(taskType string, subject config.FeatureFlagSubject) string {
	flagKey, disabledCode, applies := runtimeGlobalFeatureFlagForTaskType(taskType)
	if !applies {
		return ""
	}
	if r == nil {
		return "APP_CONFIG_UNAVAILABLE"
	}
	r.mu.Lock()
	configRepository := r.featureFlagConfig
	r.mu.Unlock()
	if configRepository == nil {
		return "APP_CONFIG_UNAVAILABLE"
	}
	var row map[string]any
	for _, candidate := range configRepository.ListFeatureFlags(map[string]any{}) {
		if stringOr(candidate["flagKey"], "") == flagKey {
			row = candidate
			break
		}
	}
	// The explicit memory backend mirrors local test defaults when the Config
	// repository has no persisted override. A configured PostgreSQL store never
	// takes this branch: a missing mandatory runtime flag fails closed above.
	if row == nil {
		return ""
	}
	status := stringOr(row["status"], "active")
	variant := stringOr(row["variant"], "")
	if status == "disabled" {
		return disabledCode
	}
	if status != "active" || !validRuntimeFeatureFlagVariant(variant) {
		return "APP_CONFIG_UNAVAILABLE"
	}
	decision := config.NewFeatureFlagEvaluator([]map[string]any{row}).Evaluate(flagKey, subject)
	if !decision.Allowed {
		return disabledCode
	}
	return ""
}

func runtimeGlobalFeatureFlagForTaskType(taskType string) (string, string, bool) {
	switch membershipFeatureForTaskType(taskType) {
	case "work_ai":
		return "runtime_work_ai_enabled", "WORK_AI_DISABLED", true
	case "feed_ai":
		return "runtime_feed_ai_enabled", "FEED_AI_DISABLED", true
	default:
		return "", "", false
	}
}

func validRuntimeFeatureFlagVariant(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "on", "off", "read_only", "readonly", "queue_only", "queueonly", "true", "false", "disabled":
		return true
	default:
		return false
	}
}

func (r *UsageRepository) featureFlagEnvironment() string {
	if r == nil {
		return "local"
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if strings.TrimSpace(r.featureFlagEnv) == "" {
		return "local"
	}
	return r.featureFlagEnv
}

func membershipFeatureForTaskType(taskType string) string {
	taskType = strings.TrimSpace(taskType)
	switch taskType {
	case "workspace_chat":
		// workspace_chat is a neutral task-card/quota projection. It uses the
		// generic Work capability gate, never a static Agent or Feed route.
		return "work_ai"
	case "feed_ai_chat":
		return "feed_ai"
	case "minutes_generation", "summary_generation", "material_deposit_generation":
		return "work_ai"
	default:
		if strings.HasPrefix(taskType, "work_ai_") {
			return "work_ai"
		}
	}
	return ""
}

func firstUsageAdmissionDenyReason(value string) string {
	switch strings.TrimSpace(value) {
	case "MEMBER_EXPIRED", "MEMBER_BLOCKED", "FEATURE_NOT_ALLOWED", "WORK_AI_DISABLED", "FEED_AI_DISABLED", "APP_CONFIG_UNAVAILABLE", "QUOTA_INSUFFICIENT":
		return strings.TrimSpace(value)
	default:
		return "QUOTA_INSUFFICIENT"
	}
}

func (r *UsageRepository) SettleReservation(reservationID string, actualUsageRefs []string) map[string]any {
	if r.postgresEnabled() {
		if reservation, err := r.setReservationStatusPostgres(context.Background(), reservationID, "settled", actualUsageRefs); err == nil {
			return reservation
		}
		return reservationPersistenceFailure(reservationID)
	}
	return r.setReservationStatus(reservationID, "settled", actualUsageRefs)
}

func (r *UsageRepository) ReleaseReservation(reservationID, reason string) map[string]any {
	if r.postgresEnabled() {
		if reservation, err := r.setReservationStatusPostgres(context.Background(), reservationID, "released", []string{reason}); err == nil {
			return reservation
		}
		return reservationPersistenceFailure(reservationID)
	}
	return r.setReservationStatus(reservationID, "released", []string{reason})
}

func (r *UsageRepository) ExpireReservation(reservationID, recoveryRunID string) map[string]any {
	if r.postgresEnabled() {
		if reservation, err := r.setReservationStatusPostgres(context.Background(), reservationID, "expired", []string{recoveryRunID}); err == nil {
			return reservation
		}
		return reservationPersistenceFailure(reservationID)
	}
	return r.setReservationStatus(reservationID, "expired", []string{recoveryRunID})
}

func (r *UsageRepository) CreateUsageRecordOnce(command map[string]any) (map[string]any, bool) {
	if r.postgresEnabled() {
		if record, duplicate, err := r.createUsageRecordOncePostgres(context.Background(), command); err == nil {
			return record, duplicate
		}
		return map[string]any{"status": "failed", "errorCode": "USAGE_RECORD_FAILED"}, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	id, _ := command["usageKey"].(string)
	if id == "" {
		id = usageDedupeKey(command)
	}
	if existing, ok := r.usage[id]; ok {
		return copyMap(existing), true
	}
	record := copyMap(command)
	record["usageRecordId"] = id
	record["usageKey"] = id
	if record["createdAt"] == nil {
		record["createdAt"] = time.Now().UTC().Format(time.RFC3339)
	}
	if record["amount"] == nil {
		record["amount"] = 1
	}
	r.usage[id] = record
	return copyMap(record), false
}

func (r *UsageRepository) SettleUsageOnce(command map[string]any, reservationID string) (map[string]any, bool, error) {
	if !r.postgresEnabled() {
		record, duplicate := r.CreateUsageRecordOnce(command)
		if stringOr(record["status"], "") == "failed" {
			return nil, false, fmt.Errorf("usage record failed")
		}
		settled := r.SettleReservation(reservationID, usageRecordRefs(record))
		settled["usage"] = record
		return settled, duplicate, nil
	}
	var record map[string]any
	var settled map[string]any
	var duplicate bool
	err := r.db.WithSerializableRetry(context.Background(), "usage_settlement", 3, func(tx *Tx) error {
		reservation, err := r.lockQuotaReservationInTx(context.Background(), tx, reservationID)
		if err != nil {
			return err
		}
		normalized, err := usageCommandForReservation(command, reservation)
		if err != nil {
			return err
		}
		record, duplicate, err = r.createUsageRecordOnceInTx(context.Background(), tx, normalized)
		if err != nil {
			return err
		}
		settled, err = r.settleUsageReservationInTx(context.Background(), tx, reservation, usageRecordRefs(record))
		return err
	})
	if err != nil {
		return nil, false, err
	}
	settled["usage"] = record
	return settled, duplicate, nil
}

type lockedQuotaReservation struct {
	reservationID     string
	permissionCheckID string
	userID            string
	workspaceID       string
	taskID            string
	asrTaskID         string
	status            string
	createdAt         time.Time
}

func (r *UsageRepository) lockQuotaReservationInTx(ctx context.Context, tx *Tx, reservationID string) (lockedQuotaReservation, error) {
	var reservation lockedQuotaReservation
	err := tx.QueryRowRaw(ctx, `
select reservation_id, coalesce(permission_check_id, ''), user_id, coalesce(workspace_id, ''),
       coalesce(task_id, ''), coalesce(asr_task_id, ''), status, created_at
from quota_reservations
where reservation_id = $1
for update`, reservationID).Scan(
		&reservation.reservationID, &reservation.permissionCheckID, &reservation.userID, &reservation.workspaceID,
		&reservation.taskID, &reservation.asrTaskID, &reservation.status, &reservation.createdAt,
	)
	return reservation, err
}

func usageCommandForReservation(command map[string]any, reservation lockedQuotaReservation) (map[string]any, error) {
	normalized := copyMap(command)
	for _, identity := range []struct {
		key      string
		expected string
	}{
		{key: "userId", expected: reservation.userID},
		{key: "workspaceId", expected: reservation.workspaceID},
		{key: "taskId", expected: reservation.taskID},
		{key: "asrTaskId", expected: reservation.asrTaskID},
		{key: "permissionCheckId", expected: reservation.permissionCheckID},
	} {
		provided := stringOr(normalized[identity.key], "")
		if provided != "" && provided != identity.expected {
			return nil, fmt.Errorf("usage reservation identity conflict: %s", identity.key)
		}
		if identity.expected != "" {
			normalized[identity.key] = identity.expected
		} else {
			delete(normalized, identity.key)
		}
	}
	normalized["quotaAccountingAt"] = reservation.createdAt.UTC().Format(time.RFC3339Nano)
	return normalized, nil
}

func (r *UsageRepository) settleUsageReservationInTx(ctx context.Context, tx *Tx, reservation lockedQuotaReservation, refs []string) (map[string]any, error) {
	switch reservation.status {
	case "reserved", "settled":
		_, after, err := r.UpdateQuotaReservationStatusInTx(ctx, tx, reservation.reservationID, "settled", refs)
		return after, err
	case "expired", "released":
		if err := r.markReservationItemsSettledInTx(ctx, tx, reservation.reservationID); err != nil {
			return nil, err
		}
		tag, err := tx.ExecRaw(ctx, `
update quota_reservations
set status = 'settled', updated_at = now()
where reservation_id = $1 and status = $2`, reservation.reservationID, reservation.status)
		if err != nil {
			return nil, err
		}
		if tag.RowsAffected() != 1 {
			return nil, fmt.Errorf("quota reservation late settlement conflict")
		}
		after, err := r.getQuotaReservationInTx(ctx, tx, reservation.reservationID)
		if err != nil {
			return nil, err
		}
		after["refs"] = refs
		return after, nil
	default:
		return nil, fmt.Errorf("quota reservation status conflict: %s", reservation.status)
	}
}

func (r *UsageRepository) markReservationItemsSettledInTx(ctx context.Context, tx *Tx, reservationID string) error {
	tag, err := tx.ExecRaw(ctx, `
update quota_reservation_items
set settled_amount = reserved_amount
where reservation_id = $1`, reservationID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("quota reservation items missing")
	}
	return nil
}

func (r *UsageRepository) ListMissingUsageCandidates(window string, taskTypes []string) []map[string]any {
	if candidates, err := r.listMissingUsageCandidatesPostgres(context.Background(), window, taskTypes); err == nil {
		return candidates
	}
	return []map[string]any{}
}

func (r *UsageRepository) ListExpiredReservations(now time.Time) []map[string]any {
	if r.usagePostgresReady() {
		items, err := r.adminListQuotaReservationsPostgres(context.Background(), map[string]any{"status": "reserved", "endAt": now.Format(time.RFC3339), "limit": 200})
		if err == nil {
			out := []map[string]any{}
			for _, item := range items {
				if expires := parseUsageTime(stringOr(item["expiresAt"], ""), now.Add(time.Hour)); !expires.After(now) {
					active, activeErr := r.quotaReservationHasActiveRun(context.Background(), stringOr(item["taskId"], ""))
					if activeErr != nil {
						return []map[string]any{}
					}
					if active {
						continue
					}
					out = append(out, item)
				}
			}
			return out
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	out := []map[string]any{}
	for _, reservation := range r.reserves {
		if stringOr(reservation["status"], "") != "reserved" {
			continue
		}
		expiresAt := parseUsageTime(stringOr(reservation["expiresAt"], ""), now.Add(time.Hour))
		if !expiresAt.After(now) {
			out = append(out, copyMap(reservation))
		}
	}
	return out
}

func (r *UsageRepository) quotaReservationHasActiveRun(ctx context.Context, taskID string) (bool, error) {
	if taskID == "" || !r.postgresEnabled() {
		return false, nil
	}
	var active bool
	err := r.db.Pool.QueryRow(ctx, `
select exists (
  select 1
  from agent_runs
  where task_id = $1
    and status in (
      'created','resolving_intent','planning','awaiting_confirmation','admission_pending',
      'queued','running','finalizing','aborting','orphaned'
    )
)`, taskID).Scan(&active)
	return active, err
}

func (r *UsageRepository) QuotaSummary(userID string) map[string]any {
	if r.usagePostgresReady() {
		if snapshot, err := r.AdminQuotaSnapshot(userID, map[string]any{}); err == nil {
			return snapshot
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	used := map[string]float64{}
	for _, record := range r.usage {
		if userID != "" && stringOr(record["userId"], "") != userID {
			continue
		}
		meterType := stringOr(record["meterType"], stringOr(record["quotaType"], "generation"))
		used[meterType] += floatValue(record["amount"], float64(intValue(record["amount"])))
	}
	reserved := map[string]float64{}
	activeReservations := []any{}
	for _, reservation := range r.reserves {
		if userID != "" && stringOr(reservation["userId"], "") != userID {
			continue
		}
		if stringOr(reservation["status"], "") != "reserved" {
			continue
		}
		activeReservations = append(activeReservations, copyMap(reservation))
		for meterType, amount := range intMap(reservation["meters"]) {
			reserved[meterType] += float64(amount)
		}
	}
	adjusted := map[string]float64{}
	for _, adjustment := range r.adjustments {
		if userID != "" && stringOr(adjustment["userId"], "") != userID {
			continue
		}
		quotaType := stringOr(adjustment["quotaType"], "generation")
		adjusted[quotaType] += floatValue(adjustment["delta"], float64(intValue(adjustment["delta"])))
	}
	balances := []any{}
	for _, quotaType := range []string{"generation", "asr_seconds", "token", "workspace_storage"} {
		limit := defaultQuotaLimit(quotaType)
		balances = append(balances, map[string]any{
			"quotaType":       quotaType,
			"limitAmount":     limit,
			"usedAmount":      used[quotaType],
			"reservedAmount":  reserved[quotaType],
			"adjustedAmount":  adjusted[quotaType],
			"remainingAmount": limit + adjusted[quotaType] - used[quotaType] - reserved[quotaType],
		})
	}
	recentAdjustments := []any{}
	for _, adjustment := range r.adjustments {
		if userID == "" || stringOr(adjustment["userId"], "") == userID {
			recentAdjustments = append(recentAdjustments, copyMap(adjustment))
		}
	}
	return map[string]any{"userId": userID, "balances": balances, "activeReservations": activeReservations, "recentAdjustments": recentAdjustments}
}

func (r *UsageRepository) CreateQuotaAdjustment(operatorID, userID, quotaType string, delta int, reason string) (map[string]any, error) {
	if adjustment, err := r.createQuotaAdjustmentPostgres(context.Background(), operatorID, userID, quotaType, delta, reason); err == nil {
		return adjustment, nil
	} else if r.usagePostgresReady() {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if quotaType == "" {
		quotaType = "generation"
	}
	id := "quota_adjustment_" + fmt.Sprint(time.Now().UTC().UnixNano())
	adjustment := map[string]any{"adjustmentId": id, "operatorId": operatorID, "userId": userID, "quotaType": quotaType, "delta": delta, "reason": reason, "createdAt": time.Now().UTC().Format(time.RFC3339)}
	r.adjustments[id] = adjustment
	return copyMap(adjustment), nil
}

func (r *UsageRepository) CreateQuotaAdjustmentInTx(ctx context.Context, tx *Tx, operatorID, userID, quotaType string, delta int, reason string) (map[string]any, error) {
	if r == nil || tx == nil || tx.tx == nil {
		return nil, fmt.Errorf("quota adjustment transaction disabled")
	}
	if quotaType == "" {
		quotaType = "generation"
	}
	balanceID, err := r.ensureQuotaBalanceInTx(ctx, tx, userID, "", quotaType)
	if err != nil {
		return nil, err
	}
	id := "quota_adjustment_" + usageIDPart(operatorID+"_"+userID+"_"+quotaType+"_"+fmt.Sprint(time.Now().UTC().UnixNano()))
	_, err = tx.ExecRaw(ctx, `
insert into quota_adjustments(quota_adjustment_id, user_id, quota_balance_id, operator_id, quota_type, delta_amount, reason)
values ($1, $2, $3, $4, $5, $6, $7)
on conflict (quota_adjustment_id) do nothing`, id, userID, balanceID, stringDefault(operatorID, "admin"), quotaType, delta, stringDefault(reason, "admin_unspecified_reason"))
	if err != nil {
		return nil, err
	}
	tag, err := tx.ExecRaw(ctx, `update quota_balances set adjusted_amount = adjusted_amount + $3, updated_at = now() where quota_balance_id = $1 and quota_type = $2`, balanceID, quotaType, delta)
	if err != nil {
		return nil, err
	}
	if tag.RowsAffected() == 0 {
		return nil, fmt.Errorf("quota balance not updated")
	}
	return map[string]any{"adjustmentId": id, "operatorId": operatorID, "userId": userID, "quotaType": quotaType, "delta": delta, "reason": reason, "quotaBalanceId": balanceID}, nil
}

func (r *UsageRepository) AdminUsageSummary(userID string, filters map[string]any) (map[string]any, error) {
	if !r.usagePostgresReady() {
		return map[string]any{"userId": userID, "usageTotals": []any{}, "permissionTotals": []any{}, "quotaSnapshot": map[string]any{"userId": userID, "balances": []any{}, "activeReservations": []any{}, "recentAdjustments": []any{}}}, nil
	}
	if summary, err := r.adminUsageSummaryPostgres(context.Background(), userID, filters); err == nil {
		return summary, nil
	}
	return nil, fmt.Errorf("usage summary unavailable")
}

func (r *UsageRepository) AdminListUsageRecords(filters map[string]any) ([]map[string]any, error) {
	if !r.usagePostgresReady() {
		return []map[string]any{}, nil
	}
	return r.adminListUsageRecordsPostgres(context.Background(), filters)
}

func (r *UsageRepository) AdminListPermissionChecks(filters map[string]any) ([]map[string]any, error) {
	if !r.usagePostgresReady() {
		return []map[string]any{}, nil
	}
	return r.adminListPermissionChecksPostgres(context.Background(), filters)
}

func (r *UsageRepository) AdminListQuotaReservations(filters map[string]any) ([]map[string]any, error) {
	if !r.usagePostgresReady() {
		return []map[string]any{}, nil
	}
	return r.adminListQuotaReservationsPostgres(context.Background(), filters)
}

func (r *UsageRepository) AdminQuotaSnapshot(userID string, filters map[string]any) (map[string]any, error) {
	if !r.usagePostgresReady() {
		snapshot := r.QuotaSummary(userID)
		quotaType := stringOr(filters["quotaType"], "")
		snapshot["quotaType"] = quotaType
		if quotaType != "" {
			filtered := []any{}
			for _, raw := range anySlice(snapshot["balances"]) {
				balance := mapValue(raw)
				if stringOr(balance["quotaType"], "") == quotaType {
					filtered = append(filtered, balance)
				}
			}
			snapshot["balances"] = filtered
			filteredAdjustments := []any{}
			for _, raw := range anySlice(snapshot["recentAdjustments"]) {
				adjustment := mapValue(raw)
				if stringOr(adjustment["quotaType"], "") == quotaType {
					filteredAdjustments = append(filteredAdjustments, adjustment)
				}
			}
			snapshot["recentAdjustments"] = filteredAdjustments
		}
		return snapshot, nil
	}
	return r.adminQuotaSnapshotPostgres(context.Background(), userID, filters)
}

func (r *UsageRepository) GetQuotaReservation(reservationID string) (map[string]any, error) {
	if !r.usagePostgresReady() {
		r.mu.Lock()
		defer r.mu.Unlock()
		if reservation, ok := r.reserves[reservationID]; ok {
			return copyMap(reservation), nil
		}
		return nil, fmt.Errorf("quota reservation not found")
	}
	return r.getQuotaReservationPostgres(context.Background(), reservationID)
}

func (r *UsageRepository) setReservationStatus(reservationID, status string, refs []string) map[string]any {
	r.mu.Lock()
	defer r.mu.Unlock()
	record := r.reserves[reservationID]
	if record == nil {
		record = map[string]any{"reservationId": reservationID}
	}
	record["status"] = status
	record["refs"] = refs
	r.reserves[reservationID] = record
	return record
}

func (r *UsageRepository) usagePostgresReady() bool {
	return r != nil && r.db != nil && !r.db.Disabled && r.db.Pool != nil
}

func (r *UsageRepository) createPermissionCheckPostgres(ctx context.Context, traceID, userID, workspaceID, taskType string, estimate map[string]any) (string, error) {
	if r == nil || r.db == nil || r.db.Disabled || r.db.Pool == nil {
		return "", fmt.Errorf("usage postgres disabled")
	}
	if taskType == "" {
		taskType = "unknown"
	}
	status := usagePermissionStatus(stringOr(estimate["status"], "allowed"))
	id := stringOr(estimate["permissionCheckId"], "perm_check_"+usageIDPart(traceID+"_"+userID+"_"+fmt.Sprint(time.Now().UTC().UnixNano())))
	_, err := r.db.Pool.Exec(ctx, `
insert into permission_checks(permission_check_id, trace_id, user_id, workspace_id, task_type, status, estimate)
values ($1, $2, $3, nullif($4, ''), $5, $6, $7::jsonb)
on conflict (permission_check_id) do nothing`, id, stringDefault(traceID, "trace_"+id), userID, workspaceID, taskType, status, jsonString(mapValue(estimate)))
	if err != nil {
		return "", err
	}
	return id, nil
}

func (r *UsageRepository) markPermissionPostgres(ctx context.Context, permissionCheckID, status, denyReason string, quotaSnapshot map[string]any, reservationID string) (map[string]any, error) {
	if r == nil || r.db == nil || r.db.Disabled || r.db.Pool == nil {
		return nil, fmt.Errorf("usage postgres disabled")
	}
	status = usagePermissionStatus(status)
	_, err := r.db.Pool.Exec(ctx, `
update permission_checks
set status = $2,
    quota_snapshot = $3::jsonb,
    deny_reason = nullif($4, '')
where permission_check_id = $1`, permissionCheckID, status, jsonString(mapValue(quotaSnapshot)), denyReason)
	if err != nil {
		return nil, err
	}
	out := map[string]any{"permissionCheckId": permissionCheckID, "result": status, "quotaSnapshot": quotaSnapshot}
	out["status"] = status
	out["allowed"] = status == "allowed"
	if reservationID != "" {
		out["reservationId"] = reservationID
	}
	if denyReason != "" {
		out["denyReason"] = denyReason
	}
	return out, nil
}

func (r *UsageRepository) lockQuotaForUserPostgres(ctx context.Context, userID string) (map[string]any, error) {
	if r == nil || r.db == nil || r.db.Disabled || r.db.Pool == nil {
		return nil, fmt.Errorf("usage postgres disabled")
	}
	rows, err := r.db.Pool.Query(ctx, `
select quota_type, used_amount::float8, reserved_amount::float8, adjusted_amount::float8
from quota_balances
where user_id = $1
order by quota_type`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	balances := []map[string]any{}
	for rows.Next() {
		var quotaType string
		var used, reserved, adjusted float64
		if err := rows.Scan(&quotaType, &used, &reserved, &adjusted); err != nil {
			return nil, err
		}
		balances = append(balances, map[string]any{"quotaType": quotaType, "usedAmount": used, "reservedAmount": reserved, "adjustedAmount": adjusted})
	}
	return map[string]any{"userId": userID, "locked": true, "lockMode": "postgres_snapshot", "balances": balances}, rows.Err()
}

func (r *UsageRepository) createReservationPostgres(ctx context.Context, permissionCheckID, taskID, taskType string, meters map[string]int, expiresAt string) (string, error) {
	if r == nil || r.db == nil || r.db.Disabled || r.db.Pool == nil {
		return "", fmt.Errorf("usage postgres disabled")
	}
	if taskType == "" {
		taskType = "unknown"
	}
	id := "reservation_" + usageIDPart(stringDefault(taskID, permissionCheckID))
	expires := parseUsageTime(expiresAt, time.Now().UTC().Add(90*time.Minute))
	meterItems := sortedPositiveQuotaMeters(meters)
	if len(meterItems) == 0 {
		meterItems = []quotaMeterAmount{{quotaType: "generation", amount: 1}}
	}
	resultID := id
	var decisionErr error
	err := r.db.WithSerializableRetry(ctx, "quota_reservation_create", 3, func(tx *Tx) error {
		var userID, workspaceID, permissionStatus string
		if err := tx.QueryRowRaw(ctx, `select user_id, coalesce(workspace_id, ''), status from permission_checks where permission_check_id = $1 for update`, permissionCheckID).Scan(&userID, &workspaceID, &permissionStatus); err != nil {
			return err
		}
		if permissionStatus != "allowed" {
			return fmt.Errorf("%w: permission check is %s", ErrQuotaInsufficient, permissionStatus)
		}

		var existingID, existingStatus, existingTaskID, existingTaskType string
		existingErr := tx.QueryRowRaw(ctx, `
select reservation_id, status, coalesce(task_id, ''), task_type
from quota_reservations
where permission_check_id = $1
for update`, permissionCheckID).Scan(&existingID, &existingStatus, &existingTaskID, &existingTaskType)
		if existingErr == nil {
			if existingStatus != "reserved" {
				return fmt.Errorf("quota reservation status conflict: %s", existingStatus)
			}
			if existingTaskID != taskID || existingTaskType != taskType {
				return fmt.Errorf("quota reservation idempotency conflict")
			}
			if err := r.validateReservationMetersInTx(ctx, tx, existingID, meterItems); err != nil {
				return err
			}
			resultID = existingID
			return nil
		}
		if !errors.Is(existingErr, pgx.ErrNoRows) {
			return existingErr
		}

		for _, item := range meterItems {
			balanceID, err := r.ensureQuotaBalanceInTx(ctx, tx, userID, workspaceID, item.quotaType)
			if err != nil {
				return err
			}
			if err := r.lockAndCheckQuotaBalanceInTx(ctx, tx, balanceID, item); err != nil {
				if errors.Is(err, ErrQuotaInsufficient) {
					decisionErr = err
					_, updateErr := tx.ExecRaw(ctx, `
update permission_checks
set status = 'denied', deny_reason = 'QUOTA_INSUFFICIENT'
where permission_check_id = $1`, permissionCheckID)
					return updateErr
				}
				return err
			}
		}
		tag, err := tx.ExecRaw(ctx, `
insert into quota_reservations(reservation_id, permission_check_id, user_id, workspace_id, task_id, task_type, status, expires_at)
values ($1, $2, $3, nullif($4, ''), nullif($5, ''), $6, 'reserved', $7)
on conflict (permission_check_id) do nothing`, id, permissionCheckID, userID, workspaceID, taskID, taskType, expires)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return fmt.Errorf("quota reservation idempotency conflict")
		}
		for _, item := range meterItems {
			if err := r.upsertReservationItemInTx(ctx, tx, id, userID, workspaceID, item.quotaType, item.amount); err != nil {
				return err
			}
		}
		return nil
	})
	if err == nil && decisionErr != nil {
		return "", decisionErr
	}
	return resultID, err
}

type quotaMeterAmount struct {
	quotaType string
	amount    float64
}

func sortedPositiveQuotaMeters(meters map[string]int) []quotaMeterAmount {
	quotaTypes := make([]string, 0, len(meters))
	for quotaType, amount := range meters {
		if strings.TrimSpace(quotaType) == "" || amount <= 0 {
			continue
		}
		quotaTypes = append(quotaTypes, quotaType)
	}
	sort.Strings(quotaTypes)
	items := make([]quotaMeterAmount, 0, len(quotaTypes))
	for _, quotaType := range quotaTypes {
		items = append(items, quotaMeterAmount{quotaType: quotaType, amount: float64(meters[quotaType])})
	}
	return items
}

func (r *UsageRepository) validateReservationMetersInTx(ctx context.Context, tx *Tx, reservationID string, expected []quotaMeterAmount) error {
	items, err := r.reservationBalanceItemsInTx(ctx, tx, reservationID)
	if err != nil {
		return err
	}
	actual := make(map[string]float64, len(items))
	for _, item := range items {
		actual[item.quotaType] = item.amount
	}
	if len(actual) != len(expected) {
		return fmt.Errorf("quota reservation idempotency conflict")
	}
	for _, item := range expected {
		if actual[item.quotaType] != item.amount {
			return fmt.Errorf("quota reservation idempotency conflict")
		}
	}
	return nil
}

func (r *UsageRepository) lockAndCheckQuotaBalanceInTx(ctx context.Context, tx *Tx, balanceID string, requested quotaMeterAmount) error {
	var limitAmount, usedAmount, reservedAmount, adjustedAmount float64
	if err := tx.QueryRowRaw(ctx, `
select coalesce(limit_amount, 0)::float8, used_amount::float8, reserved_amount::float8, adjusted_amount::float8
from quota_balances
where quota_balance_id = $1
for update`, balanceID).Scan(&limitAmount, &usedAmount, &reservedAmount, &adjustedAmount); err != nil {
		return err
	}
	remaining := limitAmount + adjustedAmount - usedAmount - reservedAmount
	if remaining < requested.amount {
		return fmt.Errorf("%w: %s remaining %.3f requested %.3f", ErrQuotaInsufficient, requested.quotaType, remaining, requested.amount)
	}
	return nil
}

func (r *UsageRepository) setReservationStatusPostgres(ctx context.Context, reservationID, newStatus string, refs []string) (map[string]any, error) {
	if r == nil || r.db == nil || r.db.Disabled || r.db.Pool == nil {
		return nil, fmt.Errorf("usage postgres disabled")
	}
	var after map[string]any
	err := r.db.WithSerializableRetry(ctx, "quota_reservation_status", 3, func(tx *Tx) error {
		_, next, err := r.UpdateQuotaReservationStatusInTx(ctx, tx, reservationID, newStatus, refs)
		if err != nil {
			return err
		}
		after = next
		return nil
	})
	return after, err
}

func (r *UsageRepository) UpdateQuotaReservationStatusInTx(ctx context.Context, tx *Tx, reservationID, newStatus string, refs []string) (map[string]any, map[string]any, error) {
	if r == nil || tx == nil || tx.tx == nil {
		return nil, nil, fmt.Errorf("quota reservation transaction disabled")
	}
	newStatus = usageReservationStatus(newStatus)
	var oldStatus, userID, workspaceID string
	err := tx.QueryRowRaw(ctx, `select status, user_id, coalesce(workspace_id, '') from quota_reservations where reservation_id = $1 for update`, reservationID).Scan(&oldStatus, &userID, &workspaceID)
	if err != nil {
		return nil, nil, err
	}
	before, err := r.getQuotaReservationInTx(ctx, tx, reservationID)
	if err != nil {
		return nil, nil, err
	}
	if oldStatus != "reserved" && newStatus != oldStatus {
		return before, before, fmt.Errorf("quota reservation status conflict: %s", oldStatus)
	}
	if oldStatus == "reserved" && newStatus != "reserved" {
		if err := r.applyReservationBalanceDeltaInTx(ctx, tx, reservationID, userID, workspaceID, newStatus); err != nil {
			return nil, nil, err
		}
	}
	tag, err := tx.ExecRaw(ctx, `update quota_reservations set status = $2, updated_at = now() where reservation_id = $1 and status = $3`, reservationID, newStatus, oldStatus)
	if err != nil {
		return nil, nil, err
	}
	if tag.RowsAffected() == 0 {
		return nil, nil, fmt.Errorf("quota reservation status conflict")
	}
	after, err := r.getQuotaReservationInTx(ctx, tx, reservationID)
	if err != nil {
		return nil, nil, err
	}
	after["refs"] = refs
	return before, after, nil
}

func (r *UsageRepository) createUsageRecordOncePostgres(ctx context.Context, command map[string]any) (map[string]any, bool, error) {
	if r == nil || r.db == nil || r.db.Disabled || r.db.Pool == nil {
		return nil, false, fmt.Errorf("usage postgres disabled")
	}
	var record map[string]any
	var duplicate bool
	err := r.db.WithSerializableRetry(ctx, "usage_record_create", 3, func(tx *Tx) error {
		var err error
		record, duplicate, err = r.createUsageRecordOnceInTx(ctx, tx, command)
		return err
	})
	return record, duplicate, err
}

func (r *UsageRepository) createUsageRecordOnceInTx(ctx context.Context, tx *Tx, command map[string]any) (map[string]any, bool, error) {
	if r == nil || tx == nil || tx.tx == nil {
		return nil, false, fmt.Errorf("usage record transaction disabled")
	}
	taskID := stringOr(command["taskId"], "")
	asrTaskID := stringOr(command["asrTaskId"], "")
	meterType := stringOr(command["meterType"], stringOr(command["quotaType"], "generation"))
	attempt := intValue(command["attempt"])
	if attempt <= 0 {
		attempt = 1
	}
	permissionCheckID := stringOr(command["permissionCheckId"], "")
	userID := stringOr(command["userId"], "")
	workspaceID := stringOr(command["workspaceId"], "")
	if userID == "" && permissionCheckID != "" {
		if err := tx.QueryRowRaw(ctx, `select user_id, coalesce(workspace_id, '') from permission_checks where permission_check_id = $1`, permissionCheckID).Scan(&userID, &workspaceID); err != nil {
			return nil, false, err
		}
	}
	amount := floatValue(command["amount"], float64(intValue(command["amount"])))
	if amount <= 0 {
		amount = 1
	}
	costAmount := floatValue(command["costAmount"], 0)
	accountingAt := parseUsageTime(stringOr(command["quotaAccountingAt"], ""), time.Now().UTC())
	payload := copyMap(command)
	delete(payload, "quotaAccountingAt")
	id := stringOr(command["usageRecordId"], stringOr(command["usageKey"], "usage_"+usageIDPart(taskID+"_"+asrTaskID+"_"+meterType+"_"+fmt.Sprint(attempt))))
	tag, err := tx.ExecRaw(ctx, `
insert into usage_records(usage_record_id, user_id, workspace_id, task_id, asr_task_id, permission_check_id, meter_type, attempt, amount, cost_amount, payload)
values ($1, $2, nullif($3, ''), nullif($4, ''), nullif($5, ''), nullif($6, ''), $7, $8, $9, $10, $11::jsonb)
on conflict do nothing`, id, userID, workspaceID, taskID, asrTaskID, permissionCheckID, meterType, attempt, amount, costAmount, jsonString(payload))
	if err != nil {
		return nil, false, err
	}
	if tag.RowsAffected() == 0 {
		existing, err := r.findExistingUsageInTx(ctx, tx, id, taskID, asrTaskID, meterType, attempt)
		if err != nil {
			return nil, false, err
		}
		if err := validateExistingUsage(existing, userID, workspaceID, taskID, asrTaskID, permissionCheckID, meterType, attempt, amount); err != nil {
			return nil, false, err
		}
		return existing, true, nil
	}
	if userID != "" {
		balanceID, err := r.ensureQuotaBalanceAtInTx(ctx, tx, userID, workspaceID, meterType, accountingAt)
		if err != nil {
			return nil, false, err
		}
		tag, err := tx.ExecRaw(ctx, `update quota_balances set used_amount = used_amount + $2, version = version + 1, updated_at = now() where quota_balance_id = $1`, balanceID, amount)
		if err != nil {
			return nil, false, err
		}
		if tag.RowsAffected() == 0 {
			return nil, false, fmt.Errorf("quota balance missing for usage record")
		}
	}
	out := copyMap(command)
	out["usageRecordId"] = id
	out["meterType"] = meterType
	out["attempt"] = attempt
	out["amount"] = amount
	return out, false, nil
}

func (r *UsageRepository) listMissingUsageCandidatesPostgres(ctx context.Context, window string, taskTypes []string) ([]map[string]any, error) {
	if r == nil || r.db == nil || r.db.Disabled || r.db.Pool == nil {
		return nil, fmt.Errorf("usage postgres disabled")
	}
	cutoff := time.Now().UTC().Add(-parseUsageWindow(window))
	rows, err := r.db.Pool.Query(ctx, `
select task_id, task_type, user_id, workspace_id, updated_at
from ai_tasks
where status = 'succeeded'
  and updated_at >= $1
  and not exists (select 1 from usage_records where usage_records.task_id = ai_tasks.task_id)
order by updated_at desc
limit 100`, cutoff)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	allowedTypes := map[string]bool{}
	for _, taskType := range taskTypes {
		allowedTypes[taskType] = true
	}
	out := []map[string]any{}
	for rows.Next() {
		var taskID, taskType, userID, workspaceID string
		var updatedAt time.Time
		if err := rows.Scan(&taskID, &taskType, &userID, &workspaceID, &updatedAt); err != nil {
			return nil, err
		}
		if len(allowedTypes) > 0 && !allowedTypes[taskType] {
			continue
		}
		out = append(out, map[string]any{"taskId": taskID, "taskType": taskType, "userId": userID, "workspaceId": workspaceID, "updatedAt": updatedAt.UTC().Format(time.RFC3339)})
	}
	return out, rows.Err()
}

func (r *UsageRepository) createQuotaAdjustmentPostgres(ctx context.Context, operatorID, userID, quotaType string, delta int, reason string) (map[string]any, error) {
	if r == nil || r.db == nil || r.db.Disabled || r.db.Pool == nil {
		return nil, fmt.Errorf("usage postgres disabled")
	}
	var adjustment map[string]any
	err := r.db.WithSerializableRetry(ctx, "quota_adjustment", 3, func(tx *Tx) error {
		row, err := r.CreateQuotaAdjustmentInTx(ctx, tx, operatorID, userID, quotaType, delta, reason)
		if err != nil {
			return err
		}
		adjustment = row
		return nil
	})
	return adjustment, err
}

func (r *UsageRepository) adminUsageSummaryPostgres(ctx context.Context, userID string, filters map[string]any) (map[string]any, error) {
	if r == nil || r.db == nil || r.db.Disabled || r.db.Pool == nil {
		return nil, fmt.Errorf("usage postgres disabled")
	}
	if userID == "" {
		return nil, fmt.Errorf("user id required")
	}
	startAt, endAt := usageRange(filters)
	usageRows, err := r.db.Pool.Query(ctx, `
select meter_type, count(*), coalesce(sum(amount), 0)::float8, coalesce(sum(cost_amount), 0)::float8
from usage_records
where user_id = $1 and created_at >= $2 and created_at < $3
group by meter_type
order by meter_type`, userID, startAt, endAt)
	if err != nil {
		return nil, err
	}
	defer usageRows.Close()
	usageTotals := []map[string]any{}
	for usageRows.Next() {
		var meterType string
		var count int
		var amount, cost float64
		if err := usageRows.Scan(&meterType, &count, &amount, &cost); err != nil {
			return nil, err
		}
		usageTotals = append(usageTotals, map[string]any{"meterType": meterType, "recordCount": count, "amount": amount, "costAmount": cost})
	}
	if err := usageRows.Err(); err != nil {
		return nil, err
	}
	permissionRows, err := r.db.Pool.Query(ctx, `
select status, count(*)
from permission_checks
where user_id = $1 and created_at >= $2 and created_at < $3
group by status
order by status`, userID, startAt, endAt)
	if err != nil {
		return nil, err
	}
	defer permissionRows.Close()
	permissionTotals := []map[string]any{}
	for permissionRows.Next() {
		var status string
		var count int
		if err := permissionRows.Scan(&status, &count); err != nil {
			return nil, err
		}
		permissionTotals = append(permissionTotals, map[string]any{"status": status, "count": count})
	}
	if err := permissionRows.Err(); err != nil {
		return nil, err
	}
	snapshot, err := r.adminQuotaSnapshotPostgres(ctx, userID, filters)
	if err != nil {
		return nil, err
	}
	return map[string]any{"userId": userID, "startAt": startAt.Format(time.RFC3339), "endAt": endAt.Format(time.RFC3339), "usageTotals": usageTotals, "permissionTotals": permissionTotals, "quotaSnapshot": snapshot}, nil
}

func (r *UsageRepository) adminListUsageRecordsPostgres(ctx context.Context, filters map[string]any) ([]map[string]any, error) {
	if r == nil || r.db == nil || r.db.Disabled || r.db.Pool == nil {
		return nil, fmt.Errorf("usage postgres disabled")
	}
	startAt, endAt := usageRange(filters)
	rows, err := r.db.Pool.Query(ctx, `
select usage_record_id, user_id, coalesce(workspace_id, ''), coalesce(task_id, ''), coalesce(asr_task_id, ''), coalesce(permission_check_id, ''), meter_type, attempt, amount::float8, coalesce(cost_amount, 0)::float8, payload, created_at
from usage_records
where ($1 = '' or user_id = $1)
  and ($2 = '' or task_id = $2)
  and ($3 = '' or asr_task_id = $3)
  and ($4 = '' or meter_type = $4)
  and created_at >= $5 and created_at < $6
order by created_at desc
limit $7`, stringOr(filters["userId"], ""), stringOr(filters["taskId"], ""), stringOr(filters["asrTaskId"], ""), stringOr(filters["meterType"], ""), startAt, endAt, usageLimit(filters))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []map[string]any{}
	for rows.Next() {
		var id, userID, workspaceID, taskID, asrTaskID, permissionCheckID, meterType string
		var attempt int
		var amount, cost float64
		var payloadRaw []byte
		var createdAt time.Time
		if err := rows.Scan(&id, &userID, &workspaceID, &taskID, &asrTaskID, &permissionCheckID, &meterType, &attempt, &amount, &cost, &payloadRaw, &createdAt); err != nil {
			return nil, err
		}
		out = append(out, map[string]any{"usageRecordId": id, "userId": userID, "workspaceId": workspaceID, "taskId": taskID, "asrTaskId": asrTaskID, "permissionCheckId": permissionCheckID, "meterType": meterType, "attempt": attempt, "amount": amount, "costAmount": cost, "payload": jsonMap(payloadRaw), "createdAt": createdAt.UTC().Format(time.RFC3339), "plaintext": false})
	}
	return out, rows.Err()
}

func (r *UsageRepository) adminListPermissionChecksPostgres(ctx context.Context, filters map[string]any) ([]map[string]any, error) {
	if r == nil || r.db == nil || r.db.Disabled || r.db.Pool == nil {
		return nil, fmt.Errorf("usage postgres disabled")
	}
	startAt, endAt := usageRange(filters)
	rows, err := r.db.Pool.Query(ctx, `
select permission_check_id, trace_id, user_id, coalesce(workspace_id, ''), task_type, status, estimate, quota_snapshot, coalesce(deny_reason, ''), created_at
from permission_checks
where ($1 = '' or user_id = $1)
  and ($2 = '' or task_type = $2)
  and ($3 = '' or status = $3)
  and created_at >= $4 and created_at < $5
  and ($6 = '' or deny_reason = $6)
order by created_at desc
limit $7`, stringOr(filters["userId"], ""), stringOr(filters["taskType"], ""), stringOr(filters["status"], ""), startAt, endAt, stringOr(filters["denyReason"], ""), usageLimit(filters))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []map[string]any{}
	for rows.Next() {
		var id, traceID, userID, workspaceID, taskType, status, denyReason string
		var estimateRaw, snapshotRaw []byte
		var createdAt time.Time
		if err := rows.Scan(&id, &traceID, &userID, &workspaceID, &taskType, &status, &estimateRaw, &snapshotRaw, &denyReason, &createdAt); err != nil {
			return nil, err
		}
		out = append(out, map[string]any{"permissionCheckId": id, "traceId": traceID, "userId": userID, "workspaceId": workspaceID, "taskType": taskType, "status": status, "estimate": jsonMap(estimateRaw), "quotaSnapshot": jsonMap(snapshotRaw), "denyReason": denyReason, "createdAt": createdAt.UTC().Format(time.RFC3339)})
	}
	return out, rows.Err()
}

func (r *UsageRepository) adminListQuotaReservationsPostgres(ctx context.Context, filters map[string]any) ([]map[string]any, error) {
	if r == nil || r.db == nil || r.db.Disabled || r.db.Pool == nil {
		return nil, fmt.Errorf("usage postgres disabled")
	}
	startAt, endAt := usageRange(filters)
	rows, err := r.db.Pool.Query(ctx, `
select r.reservation_id, coalesce(r.permission_check_id, ''), r.user_id, coalesce(r.workspace_id, ''), coalesce(r.task_id, ''), coalesce(r.asr_task_id, ''), r.task_type, r.status, r.expires_at, r.created_at, r.updated_at,
       coalesce(jsonb_agg(jsonb_build_object('quotaType', i.quota_type, 'reservedAmount', i.reserved_amount, 'settledAmount', i.settled_amount) order by i.quota_type) filter (where i.reservation_item_id is not null), '[]'::jsonb)
from quota_reservations r
left join quota_reservation_items i on i.reservation_id = r.reservation_id
where ($1 = '' or r.user_id = $1)
  and ($2 = '' or r.status = $2)
  and ($3 = '' or r.task_id = $3)
  and r.created_at >= $4 and r.created_at < $5
  and ($6 = '' or exists (select 1 from quota_reservation_items qi where qi.reservation_id = r.reservation_id and qi.quota_type = $6))
group by r.reservation_id
order by r.created_at desc
limit $7`, stringOr(filters["userId"], ""), stringOr(filters["status"], ""), stringOr(filters["taskId"], ""), startAt, endAt, stringOr(filters["quotaType"], ""), usageLimit(filters))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []map[string]any{}
	for rows.Next() {
		reservation, err := scanQuotaReservation(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, reservation)
	}
	return out, rows.Err()
}

func (r *UsageRepository) adminQuotaSnapshotPostgres(ctx context.Context, userID string, filters map[string]any) (map[string]any, error) {
	if r == nil || r.db == nil || r.db.Disabled || r.db.Pool == nil {
		return nil, fmt.Errorf("usage postgres disabled")
	}
	if userID == "" {
		return nil, fmt.Errorf("user id required")
	}
	quotaTypeFilter := stringOr(filters["quotaType"], "")
	if quotaTypeFilter != "" {
		if _, err := r.ensureQuotaBalance(ctx, userID, "", quotaTypeFilter); err != nil {
			return nil, err
		}
	} else if err := r.ensureDefaultQuotaBalancesPostgres(ctx, userID, ""); err != nil {
		return nil, err
	}
	rows, err := r.db.Pool.Query(ctx, `
select quota_balance_id, coalesce(workspace_id, ''), quota_type, period_start, period_end, coalesce(limit_amount, 0)::float8, used_amount::float8, reserved_amount::float8, adjusted_amount::float8, version, updated_at
from quota_balances
where user_id = $1 and ($2 = '' or quota_type = $2)
order by quota_type`, userID, quotaTypeFilter)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	balances := []map[string]any{}
	for rows.Next() {
		var balanceID, workspaceID, quotaType string
		var start, end, updatedAt time.Time
		var limit, used, reserved, adjusted float64
		var version int
		if err := rows.Scan(&balanceID, &workspaceID, &quotaType, &start, &end, &limit, &used, &reserved, &adjusted, &version, &updatedAt); err != nil {
			return nil, err
		}
		remaining := limit + adjusted - used - reserved
		if limit == 0 {
			remaining = adjusted - used - reserved
		}
		balances = append(balances, map[string]any{"quotaBalanceId": balanceID, "workspaceId": workspaceID, "quotaType": quotaType, "periodStart": start.UTC().Format(time.RFC3339), "periodEnd": end.UTC().Format(time.RFC3339), "limitAmount": limit, "usedAmount": used, "reservedAmount": reserved, "adjustedAmount": adjusted, "remainingAmount": remaining, "version": version, "updatedAt": updatedAt.UTC().Format(time.RFC3339)})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	reservations, err := r.adminListQuotaReservationsPostgres(ctx, map[string]any{"userId": userID, "status": "reserved", "quotaType": quotaTypeFilter, "limit": 20})
	if err != nil {
		return nil, err
	}
	adjustments, err := r.adminListQuotaAdjustmentsPostgres(ctx, userID, quotaTypeFilter)
	if err != nil {
		return nil, err
	}
	return map[string]any{"userId": userID, "quotaType": quotaTypeFilter, "balances": balances, "activeReservations": reservations, "recentAdjustments": adjustments}, nil
}

func (r *UsageRepository) AdminQuotaSnapshotInTx(ctx context.Context, tx *Tx, userID string, filters map[string]any) (map[string]any, error) {
	if r == nil || tx == nil || tx.tx == nil {
		return nil, fmt.Errorf("quota snapshot transaction disabled")
	}
	if userID == "" {
		return nil, fmt.Errorf("user id required")
	}
	quotaTypeFilter := stringOr(filters["quotaType"], "")
	if quotaTypeFilter != "" {
		if _, err := r.ensureQuotaBalanceInTx(ctx, tx, userID, "", quotaTypeFilter); err != nil {
			return nil, err
		}
	} else {
		for _, quotaType := range quotaTypesWithDefaultBalances() {
			if _, err := r.ensureQuotaBalanceInTx(ctx, tx, userID, "", quotaType); err != nil {
				return nil, err
			}
		}
	}
	rows, err := tx.QueryRaw(ctx, `
select quota_balance_id, coalesce(workspace_id, ''), quota_type, period_start, period_end, coalesce(limit_amount, 0)::float8, used_amount::float8, reserved_amount::float8, adjusted_amount::float8, version, updated_at
from quota_balances
where user_id = $1 and ($2 = '' or quota_type = $2)
order by quota_type`, userID, quotaTypeFilter)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	balances := []map[string]any{}
	for rows.Next() {
		var balanceID, workspaceID, quotaType string
		var start, end, updatedAt time.Time
		var limit, used, reserved, adjusted float64
		var version int
		if err := rows.Scan(&balanceID, &workspaceID, &quotaType, &start, &end, &limit, &used, &reserved, &adjusted, &version, &updatedAt); err != nil {
			return nil, err
		}
		remaining := limit + adjusted - used - reserved
		if limit == 0 {
			remaining = adjusted - used - reserved
		}
		balances = append(balances, map[string]any{"quotaBalanceId": balanceID, "workspaceId": workspaceID, "quotaType": quotaType, "periodStart": start.UTC().Format(time.RFC3339), "periodEnd": end.UTC().Format(time.RFC3339), "limitAmount": limit, "usedAmount": used, "reservedAmount": reserved, "adjustedAmount": adjusted, "remainingAmount": remaining, "version": version, "updatedAt": updatedAt.UTC().Format(time.RFC3339)})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	reservations, err := r.listReservedQuotaReservationsInTx(ctx, tx, userID, quotaTypeFilter)
	if err != nil {
		return nil, err
	}
	adjustments, err := r.listQuotaAdjustmentsInTx(ctx, tx, userID, quotaTypeFilter)
	if err != nil {
		return nil, err
	}
	return map[string]any{"userId": userID, "quotaType": quotaTypeFilter, "balances": balances, "activeReservations": reservations, "recentAdjustments": adjustments}, nil
}

func (r *UsageRepository) adminListQuotaAdjustmentsPostgres(ctx context.Context, userID, quotaType string) ([]map[string]any, error) {
	rows, err := r.db.Pool.Query(ctx, `
select quota_adjustment_id, user_id, coalesce(quota_balance_id, ''), operator_id, quota_type, delta_amount::float8, reason, created_at
from quota_adjustments
where user_id = $1 and ($2 = '' or quota_type = $2)
order by created_at desc
limit 20`, userID, quotaType)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []map[string]any{}
	for rows.Next() {
		var id, rowUserID, balanceID, operatorID, rowQuotaType, reason string
		var delta float64
		var createdAt time.Time
		if err := rows.Scan(&id, &rowUserID, &balanceID, &operatorID, &rowQuotaType, &delta, &reason, &createdAt); err != nil {
			return nil, err
		}
		out = append(out, map[string]any{"adjustmentId": id, "userId": rowUserID, "quotaBalanceId": balanceID, "operatorId": operatorID, "quotaType": rowQuotaType, "delta": delta, "reason": reason, "createdAt": createdAt.UTC().Format(time.RFC3339)})
	}
	return out, rows.Err()
}

func (r *UsageRepository) listQuotaAdjustmentsInTx(ctx context.Context, tx *Tx, userID, quotaType string) ([]map[string]any, error) {
	rows, err := tx.QueryRaw(ctx, `
select quota_adjustment_id, user_id, coalesce(quota_balance_id, ''), operator_id, quota_type, delta_amount::float8, reason, created_at
from quota_adjustments
where user_id = $1 and ($2 = '' or quota_type = $2)
order by created_at desc
limit 20`, userID, quotaType)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []map[string]any{}
	for rows.Next() {
		var id, rowUserID, balanceID, operatorID, rowQuotaType, reason string
		var delta float64
		var createdAt time.Time
		if err := rows.Scan(&id, &rowUserID, &balanceID, &operatorID, &rowQuotaType, &delta, &reason, &createdAt); err != nil {
			return nil, err
		}
		out = append(out, map[string]any{"adjustmentId": id, "userId": rowUserID, "quotaBalanceId": balanceID, "operatorId": operatorID, "quotaType": rowQuotaType, "delta": delta, "reason": reason, "createdAt": createdAt.UTC().Format(time.RFC3339)})
	}
	return out, rows.Err()
}

func (r *UsageRepository) listReservedQuotaReservationsInTx(ctx context.Context, tx *Tx, userID, quotaType string) ([]map[string]any, error) {
	rows, err := tx.QueryRaw(ctx, `
select r.reservation_id, coalesce(r.permission_check_id, ''), r.user_id, coalesce(r.workspace_id, ''), coalesce(r.task_id, ''), coalesce(r.asr_task_id, ''), r.task_type, r.status, r.expires_at, r.created_at, r.updated_at,
       coalesce(jsonb_agg(jsonb_build_object('quotaType', i.quota_type, 'reservedAmount', i.reserved_amount, 'settledAmount', i.settled_amount) order by i.quota_type) filter (where i.reservation_item_id is not null), '[]'::jsonb)
from quota_reservations r
left join quota_reservation_items i on i.reservation_id = r.reservation_id
where r.user_id = $1
  and r.status = 'reserved'
  and ($2 = '' or i.quota_type = $2)
group by r.reservation_id
order by r.created_at desc
limit 20`, userID, quotaType)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []map[string]any{}
	for rows.Next() {
		reservation, err := scanQuotaReservation(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, reservation)
	}
	return out, rows.Err()
}

func (r *UsageRepository) getQuotaReservationPostgres(ctx context.Context, reservationID string) (map[string]any, error) {
	if r == nil || r.db == nil || r.db.Disabled || r.db.Pool == nil {
		return nil, fmt.Errorf("usage postgres disabled")
	}
	rows, err := r.db.Pool.Query(ctx, `
select r.reservation_id, coalesce(r.permission_check_id, ''), r.user_id, coalesce(r.workspace_id, ''), coalesce(r.task_id, ''), coalesce(r.asr_task_id, ''), r.task_type, r.status, r.expires_at, r.created_at, r.updated_at,
       coalesce(jsonb_agg(jsonb_build_object('quotaType', i.quota_type, 'reservedAmount', i.reserved_amount, 'settledAmount', i.settled_amount) order by i.quota_type) filter (where i.reservation_item_id is not null), '[]'::jsonb)
from quota_reservations r
left join quota_reservation_items i on i.reservation_id = r.reservation_id
where r.reservation_id = $1
group by r.reservation_id`, reservationID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		if rows.Err() != nil {
			return nil, rows.Err()
		}
		return nil, fmt.Errorf("quota reservation not found")
	}
	return scanQuotaReservation(rows)
}

func (r *UsageRepository) getQuotaReservationInTx(ctx context.Context, tx *Tx, reservationID string) (map[string]any, error) {
	rows, err := tx.QueryRaw(ctx, `
select r.reservation_id, coalesce(r.permission_check_id, ''), r.user_id, coalesce(r.workspace_id, ''), coalesce(r.task_id, ''), coalesce(r.asr_task_id, ''), r.task_type, r.status, r.expires_at, r.created_at, r.updated_at,
       coalesce(jsonb_agg(jsonb_build_object('quotaType', i.quota_type, 'reservedAmount', i.reserved_amount, 'settledAmount', i.settled_amount) order by i.quota_type) filter (where i.reservation_item_id is not null), '[]'::jsonb)
from quota_reservations r
left join quota_reservation_items i on i.reservation_id = r.reservation_id
where r.reservation_id = $1
group by r.reservation_id`, reservationID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		if rows.Err() != nil {
			return nil, rows.Err()
		}
		return nil, fmt.Errorf("quota reservation not found")
	}
	return scanQuotaReservation(rows)
}

func (r *UsageRepository) upsertReservationItemInTx(ctx context.Context, tx *Tx, reservationID, userID, workspaceID, quotaType string, amount float64) error {
	itemID := "reservation_item_" + usageIDPart(reservationID+"_"+quotaType)
	var balanceDelta float64
	err := tx.QueryRowRaw(ctx, `
with existing as (
  select reserved_amount::float8 as old_amount
  from quota_reservation_items
  where reservation_id = $2 and quota_type = $3
),
upserted as (
insert into quota_reservation_items(reservation_item_id, reservation_id, quota_type, reserved_amount)
values ($1, $2, $3, $4)
on conflict (reservation_id, quota_type) do update set reserved_amount = excluded.reserved_amount
returning reserved_amount::float8 as new_amount
)
select coalesce((select new_amount from upserted), $4::float8) - coalesce((select old_amount from existing), 0)::float8`, itemID, reservationID, quotaType, amount).Scan(&balanceDelta)
	if err != nil {
		return err
	}
	balanceID, err := r.ensureQuotaBalanceInTx(ctx, tx, userID, workspaceID, quotaType)
	if err != nil {
		return err
	}
	if balanceDelta == 0 {
		return nil
	}
	tag, err := tx.ExecRaw(ctx, `update quota_balances set reserved_amount = reserved_amount + $2, version = version + 1, updated_at = now() where quota_balance_id = $1`, balanceID, balanceDelta)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("quota balance missing for reservation")
	}
	return nil
}

func (r *UsageRepository) applyReservationBalanceDelta(ctx context.Context, reservationID, userID, workspaceID, status string) error {
	items, err := r.reservationBalanceItems(ctx, reservationID)
	if err != nil {
		return err
	}
	for _, item := range items {
		tag, updateErr := r.db.Pool.Exec(ctx, `
update quota_balances b
set reserved_amount = b.reserved_amount - $4, version = version + 1, updated_at = now()
from quota_reservations r
where r.reservation_id = $1
  and b.user_id = $2 and b.quota_type = $3
  and b.period_start <= r.created_at and b.period_end > r.created_at
  and b.reserved_amount >= $4`, reservationID, userID, item.quotaType, item.amount)
		err = updateErr
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return fmt.Errorf("quota reservation balance mismatch")
		}
		if status == "settled" {
			if _, err = r.db.Pool.Exec(ctx, `update quota_reservation_items set settled_amount = reserved_amount where reservation_id = $1 and quota_type = $2`, reservationID, item.quotaType); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *UsageRepository) applyReservationBalanceDeltaInTx(ctx context.Context, tx *Tx, reservationID, userID, workspaceID, status string) error {
	items, err := r.reservationBalanceItemsInTx(ctx, tx, reservationID)
	if err != nil {
		return err
	}
	for _, item := range items {
		tag, updateErr := tx.ExecRaw(ctx, `
update quota_balances b
set reserved_amount = b.reserved_amount - $4, version = version + 1, updated_at = now()
from quota_reservations r
where r.reservation_id = $1
  and b.user_id = $2 and b.quota_type = $3
  and b.period_start <= r.created_at and b.period_end > r.created_at
  and b.reserved_amount >= $4`, reservationID, userID, item.quotaType, item.amount)
		if updateErr != nil {
			return updateErr
		}
		if tag.RowsAffected() != 1 {
			return fmt.Errorf("quota reservation balance mismatch")
		}
		if status == "settled" {
			if _, updateErr = tx.ExecRaw(ctx, `update quota_reservation_items set settled_amount = reserved_amount where reservation_id = $1 and quota_type = $2`, reservationID, item.quotaType); updateErr != nil {
				return updateErr
			}
		}
	}
	return nil
}

type reservationBalanceItem struct {
	quotaType string
	amount    float64
}

func (r *UsageRepository) reservationBalanceItems(ctx context.Context, reservationID string) ([]reservationBalanceItem, error) {
	rows, err := r.db.Pool.Query(ctx, `select quota_type, reserved_amount::float8 from quota_reservation_items where reservation_id = $1`, reservationID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []reservationBalanceItem{}
	for rows.Next() {
		var item reservationBalanceItem
		if err := rows.Scan(&item.quotaType, &item.amount); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (r *UsageRepository) reservationBalanceItemsInTx(ctx context.Context, tx *Tx, reservationID string) ([]reservationBalanceItem, error) {
	rows, err := tx.QueryRaw(ctx, `select quota_type, reserved_amount::float8 from quota_reservation_items where reservation_id = $1`, reservationID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []reservationBalanceItem{}
	for rows.Next() {
		var item reservationBalanceItem
		if err := rows.Scan(&item.quotaType, &item.amount); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (r *UsageRepository) postgresEnabled() bool {
	return r != nil && r.db != nil && !r.db.Disabled && r.db.Pool != nil
}

func reservationPersistenceFailure(reservationID string) map[string]any {
	return map[string]any{"reservationId": reservationID, "status": "failed", "errorCode": "QUOTA_RESERVATION_FAILED"}
}

func (r *UsageRepository) ensureQuotaBalance(ctx context.Context, userID, workspaceID, quotaType string) (string, error) {
	start, end := currentUsagePeriod()
	id := "quota_balance_" + usageIDPart(userID+"_"+quotaType+"_"+start.Format("20060102"))
	limit := quotaLimitForUser(ctx, r.db.Pool, userID, quotaType)
	var balanceID string
	err := r.db.Pool.QueryRow(ctx, `
insert into quota_balances(quota_balance_id, user_id, workspace_id, quota_type, period_start, period_end, limit_amount)
values ($1, $2, nullif($3, ''), $4, $5, $6, $7)
on conflict (user_id, quota_type, period_start, period_end) do update set
  limit_amount = coalesce(quota_balances.limit_amount, excluded.limit_amount),
  updated_at = now()
returning quota_balance_id`, id, userID, workspaceID, quotaType, start, end, limit).Scan(&balanceID)
	return balanceID, err
}

func (r *UsageRepository) ensureQuotaBalanceInTx(ctx context.Context, tx *Tx, userID, workspaceID, quotaType string) (string, error) {
	return r.ensureQuotaBalanceAtInTx(ctx, tx, userID, workspaceID, quotaType, time.Now().UTC())
}

func (r *UsageRepository) ensureQuotaBalanceAtInTx(ctx context.Context, tx *Tx, userID, workspaceID, quotaType string, accountingAt time.Time) (string, error) {
	if r == nil || tx == nil || tx.tx == nil {
		return "", fmt.Errorf("quota balance transaction disabled")
	}
	start, end := usagePeriodAt(accountingAt)
	id := "quota_balance_" + usageIDPart(userID+"_"+quotaType+"_"+start.Format("20060102"))
	limit := quotaLimitForUser(ctx, txQuotaLimitQueryer{tx: tx}, userID, quotaType)
	var balanceID string
	err := tx.QueryRowRaw(ctx, `
insert into quota_balances(quota_balance_id, user_id, workspace_id, quota_type, period_start, period_end, limit_amount)
values ($1, $2, nullif($3, ''), $4, $5, $6, $7)
on conflict (user_id, quota_type, period_start, period_end) do update set
  limit_amount = coalesce(quota_balances.limit_amount, excluded.limit_amount),
  updated_at = now()
returning quota_balance_id`, id, userID, workspaceID, quotaType, start, end, limit).Scan(&balanceID)
	return balanceID, err
}

func (r *UsageRepository) ensureDefaultQuotaBalancesPostgres(ctx context.Context, userID, workspaceID string) error {
	for _, quotaType := range quotaTypesWithDefaultBalances() {
		if _, err := r.ensureQuotaBalance(ctx, userID, workspaceID, quotaType); err != nil {
			return err
		}
	}
	return nil
}

type quotaLimitQueryer interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type txQuotaLimitQueryer struct {
	tx *Tx
}

func (q txQuotaLimitQueryer) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return q.tx.QueryRowRaw(ctx, sql, args...)
}

func quotaLimitForUser(ctx context.Context, queryer quotaLimitQueryer, userID, quotaType string) float64 {
	fallback := defaultQuotaLimit(quotaType)
	if queryer == nil || quotaType == "" {
		return fallback
	}
	if userID != "" {
		if raw, ok := quotaConfigValue(ctx, queryer, `
select coalesce(ml.quota_config ->> $2, '')
from memberships m
join membership_levels ml on ml.level_code = m.level_code
where m.user_id = $1
  and m.status in ('trial','active')
  and ml.status = 'active'
order by case when m.status = 'active' then 0 else 1 end, m.updated_at desc
limit 1`, userID, quotaType); ok {
			return parseQuotaLimit(raw, fallback)
		}
	}
	if raw, ok := quotaConfigValue(ctx, queryer, `
select coalesce(quota_config ->> $1, '')
from membership_levels
where level_code = 'trial' and status = 'active'
limit 1`, quotaType); ok {
		return parseQuotaLimit(raw, fallback)
	}
	return fallback
}

func quotaConfigValue(ctx context.Context, queryer quotaLimitQueryer, query string, args ...any) (string, bool) {
	var raw string
	if err := queryer.QueryRow(ctx, query, args...).Scan(&raw); err != nil {
		return "", false
	}
	return raw, true
}

func parseQuotaLimit(raw string, fallback float64) float64 {
	raw = strings.TrimSpace(raw)
	if raw == "" || strings.EqualFold(raw, "null") {
		return fallback
	}
	limit, err := strconv.ParseFloat(raw, 64)
	if err != nil || limit < 0 {
		return fallback
	}
	return limit
}

func quotaTypesWithDefaultBalances() []string {
	return []string{"generation", "asr_seconds", "token", "workspace_storage"}
}

func (r *UsageRepository) findExistingUsageInTx(ctx context.Context, tx *Tx, usageRecordID, taskID, asrTaskID, meterType string, attempt int) (map[string]any, error) {
	var id, userID, workspaceID, existingTaskID, existingASRTaskID, permissionCheckID, existingMeterType string
	var existingAttempt int
	var amount float64
	if err := tx.QueryRowRaw(ctx, `
select usage_record_id, user_id, coalesce(workspace_id, ''), coalesce(task_id, ''), coalesce(asr_task_id, ''),
       coalesce(permission_check_id, ''), meter_type, attempt, amount::float8
from usage_records
where usage_record_id = $1
   or ($2 <> '' and task_id = $2 and meter_type = $4 and attempt = $5)
   or ($3 <> '' and asr_task_id = $3 and meter_type = $4 and attempt = $5)
order by case when usage_record_id = $1 then 0 else 1 end
limit 1`, usageRecordID, taskID, asrTaskID, meterType, attempt).Scan(
		&id, &userID, &workspaceID, &existingTaskID, &existingASRTaskID,
		&permissionCheckID, &existingMeterType, &existingAttempt, &amount,
	); err != nil {
		return nil, err
	}
	return map[string]any{
		"usageRecordId": id, "usageKey": id, "userId": userID, "workspaceId": workspaceID,
		"taskId": existingTaskID, "asrTaskId": existingASRTaskID, "permissionCheckId": permissionCheckID,
		"meterType": existingMeterType, "attempt": existingAttempt, "amount": amount,
	}, nil
}

func validateExistingUsage(existing map[string]any, userID, workspaceID, taskID, asrTaskID, permissionCheckID, meterType string, attempt int, amount float64) error {
	checks := []struct {
		field    string
		expected string
	}{
		{field: "userId", expected: userID},
		{field: "workspaceId", expected: workspaceID},
		{field: "taskId", expected: taskID},
		{field: "asrTaskId", expected: asrTaskID},
		{field: "meterType", expected: meterType},
	}
	for _, check := range checks {
		if stringOr(existing[check.field], "") != check.expected {
			return fmt.Errorf("%w: %s", ErrUsageIdempotencyConflict, check.field)
		}
	}
	existingPermissionCheckID := stringOr(existing["permissionCheckId"], "")
	if existingPermissionCheckID != "" && permissionCheckID != "" && existingPermissionCheckID != permissionCheckID {
		return fmt.Errorf("%w: permissionCheckId", ErrUsageIdempotencyConflict)
	}
	if intValue(existing["attempt"]) != attempt {
		return fmt.Errorf("%w: attempt", ErrUsageIdempotencyConflict)
	}
	if floatValue(existing["amount"], 0) != amount {
		return fmt.Errorf("%w: amount", ErrUsageIdempotencyConflict)
	}
	return nil
}

func usageRecordRefs(record map[string]any) []string {
	refs := []string{}
	if usageKey := stringOr(record["usageKey"], ""); usageKey != "" {
		refs = append(refs, usageKey)
	}
	if usageRecordID := stringOr(record["usageRecordId"], ""); usageRecordID != "" && usageRecordID != stringOr(record["usageKey"], "") {
		refs = append(refs, usageRecordID)
	}
	return refs
}

type quotaReservationScanner interface {
	Scan(...any) error
}

func scanQuotaReservation(row quotaReservationScanner) (map[string]any, error) {
	var reservationID, permissionCheckID, userID, workspaceID, taskID, asrTaskID, taskType, status string
	var expiresAt, createdAt, updatedAt time.Time
	var itemsRaw []byte
	if err := row.Scan(&reservationID, &permissionCheckID, &userID, &workspaceID, &taskID, &asrTaskID, &taskType, &status, &expiresAt, &createdAt, &updatedAt, &itemsRaw); err != nil {
		return nil, err
	}
	items := []any{}
	_ = json.Unmarshal(itemsRaw, &items)
	return map[string]any{"reservationId": reservationID, "permissionCheckId": permissionCheckID, "userId": userID, "workspaceId": workspaceID, "taskId": taskID, "asrTaskId": asrTaskID, "taskType": taskType, "status": status, "expiresAt": expiresAt.UTC().Format(time.RFC3339), "createdAt": createdAt.UTC().Format(time.RFC3339), "updatedAt": updatedAt.UTC().Format(time.RFC3339), "items": items}, nil
}

func usageRange(filters map[string]any) (time.Time, time.Time) {
	now := time.Now().UTC()
	startAtRaw := stringOr(filters["startAt"], stringOr(filters["from"], ""))
	endAtRaw := stringOr(filters["endAt"], stringOr(filters["to"], ""))
	startAt := parseUsageTime(startAtRaw, now.AddDate(0, 0, -30))
	endAt := parseUsageTime(endAtRaw, now.Add(24*time.Hour))
	if !endAt.After(startAt) {
		endAt = startAt.Add(24 * time.Hour)
	}
	return startAt, endAt
}

func usageLimit(filters map[string]any) int {
	limit := intValue(filters["limit"])
	if limit <= 0 {
		limit = 100
	}
	if limit > 200 {
		return 200
	}
	return limit
}

func usagePermissionStatus(status string) string {
	if status == "denied" {
		return "denied"
	}
	return "allowed"
}

func usageReservationStatus(status string) string {
	switch status {
	case "settled", "released", "expired":
		return status
	default:
		return "reserved"
	}
}

func currentUsagePeriod() (time.Time, time.Time) {
	return usagePeriodAt(time.Now().UTC())
}

func usagePeriodAt(value time.Time) (time.Time, time.Time) {
	now := value.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	start := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
	return start, start.AddDate(0, 1, 0)
}

func parseUsageTime(raw string, fallback time.Time) time.Time {
	if raw == "" {
		return fallback
	}
	parsed, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return fallback
	}
	return parsed.UTC()
}

func parseUsageWindow(raw string) time.Duration {
	if raw == "" {
		return 24 * time.Hour
	}
	parsed, err := time.ParseDuration(raw)
	if err != nil {
		return 24 * time.Hour
	}
	return parsed
}

func floatValue(value any, fallback float64) float64 {
	switch typed := value.(type) {
	case float64:
		return typed
	case float32:
		return float64(typed)
	case int:
		return float64(typed)
	case int64:
		return float64(typed)
	default:
		return fallback
	}
}

func usageDedupeKey(command map[string]any) string {
	taskID := stringOr(command["taskId"], "")
	asrTaskID := stringOr(command["asrTaskId"], "")
	meterType := stringOr(command["meterType"], stringOr(command["quotaType"], "generation"))
	attempt := intValue(command["attempt"])
	if attempt <= 0 {
		attempt = 1
	}
	return "usage_" + usageIDPart(firstNonEmptyString(taskID, asrTaskID, "manual")+"_"+meterType+"_"+fmt.Sprint(attempt))
}

func intMap(value any) map[string]int {
	out := map[string]int{}
	switch typed := value.(type) {
	case map[string]int:
		for key, amount := range typed {
			out[key] = amount
		}
	case map[string]any:
		for key, amount := range typed {
			out[key] = intValue(amount)
		}
	}
	return out
}

func defaultQuotaLimit(quotaType string) float64 {
	switch quotaType {
	case "asr_seconds":
		return 36000
	case "token":
		return 1000000
	case "workspace_storage":
		return 1073741824
	default:
		return 100
	}
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func usageIDPart(input string) string {
	builder := strings.Builder{}
	for _, ch := range input {
		if ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' {
			builder.WriteRune(ch)
			continue
		}
		builder.WriteByte('_')
	}
	out := strings.Trim(builder.String(), "_")
	if out == "" {
		return fmt.Sprint(time.Now().UTC().UnixNano())
	}
	if len(out) > 140 {
		return out[:140]
	}
	return out
}
