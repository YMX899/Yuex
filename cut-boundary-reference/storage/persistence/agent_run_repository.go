package persistence

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"huahuoai/backend/source/internal/domain"

	"github.com/jackc/pgx/v5"
)

type AgentRunRecord struct {
	AgentRunID         string         `json:"agentRunId"`
	TenantID           string         `json:"tenantId,omitempty"`
	UserID             string         `json:"userId,omitempty"`
	WorkspaceID        string         `json:"workspaceId"`
	ThreadID           string         `json:"threadId,omitempty"`
	TaskID             string         `json:"taskId,omitempty"`
	IdempotencyKey     string         `json:"-"`
	RequestHash        string         `json:"-"`
	RequestSnapshot    map[string]any `json:"-"`
	Status             string         `json:"status"`
	RoutingMode        string         `json:"routingMode,omitempty"`
	SourceSurface      string         `json:"sourceSurface,omitempty"`
	WorkspaceVersion   int64          `json:"workspaceVersion"`
	BindingVersion     int64          `json:"workspaceBindingVersion"`
	ContextGeneration  int64          `json:"contextGeneration"`
	IntentSnapshot     map[string]any `json:"-"`
	PlanSnapshot       map[string]any `json:"-"`
	ExecutionIdentity  map[string]any `json:"-"`
	Routing            map[string]any `json:"routing,omitempty"`
	Clarification      map[string]any `json:"clarification,omitempty"`
	PublicResult       map[string]any `json:"result,omitempty"`
	ErrorSummary       map[string]any `json:"error,omitempty"`
	CancelRequestedAt  *time.Time     `json:"-"`
	CancelReasonCode   string         `json:"-"`
	CancelReasonHash   string         `json:"-"`
	SubmitAuthorizedAt *time.Time     `json:"-"`
	CreatedAt          time.Time      `json:"createdAt"`
	UpdatedAt          time.Time      `json:"updatedAt"`
}

type CreateAgentRunCommand struct {
	Record AgentRunRecord
}

type AgentRunPlanRecord struct {
	AgentRunID     string
	PlanVersion    int
	PlanStatus     string
	AgentRunStatus string
	Plan           map[string]any
}

// AgentRunConfirmationCommand contains only server-derived facts. In
// particular, RuntimeLane is recomputed from the persisted Plan before it is
// accepted, and neither task, quota nor ownership data may come from App.
type AgentRunConfirmationCommand struct {
	AgentRunID     string
	TenantID       string
	UserID         string
	PlanVersion    int
	IdempotencyKey string
	RuntimeLane    string
	ConfirmedAt    time.Time
}

type AgentRunConfirmationResult struct {
	TaskID            string
	PermissionCheckID string
	ReservationID     string
	Replayed          bool
}

type AgentRunEvent struct {
	AgentRunID string         `json:"-"`
	Sequence   int64          `json:"sequence"`
	EventType  string         `json:"eventType"`
	Status     string         `json:"status"`
	SafeData   map[string]any `json:"data,omitempty"`
	CreatedAt  time.Time      `json:"createdAt"`
}

type AgentRunEventPage struct {
	Items                   []AgentRunEvent `json:"items"`
	NextAfterSequence       int64           `json:"nextAfterSequence"`
	HasMore                 bool            `json:"hasMore"`
	OldestAvailableSequence int64           `json:"oldestAvailableSequence"`
	LatestSequence          int64           `json:"latestSequence"`
	TerminalSequence        *int64          `json:"terminalSequence,omitempty"`
	Gap                     bool            `json:"gap"`
}

// AgentRunEventBounds exposes only retained public-event sequence facts. It
// intentionally has no payload, dispatch, host, or Runtime-internal fields.
type AgentRunEventBounds struct {
	OldestAvailableSequence int64  `json:"oldestAvailableSequence"`
	LatestSequence          int64  `json:"latestSequence"`
	TerminalSequence        *int64 `json:"terminalSequence,omitempty"`
}

// CancelDecision records the durable outcome of one cancel request. It is an
// internal coordination result; the App receives the subsequently reloaded
// AgentRun view rather than any dispatch or reservation identity.
type CancelDecision struct {
	Status        string
	AbortEnqueued bool
	StateChanged  bool
	Terminal      bool
}

type ThreadWorkspaceBinding struct {
	// TenantID and UserID are internal ownership facts. They intentionally do
	// not expand the public thread-binding response, but every repository
	// resolution and memory-mirror transition must carry them.
	TenantID            string    `json:"-"`
	UserID              string    `json:"-"`
	ThreadID            string    `json:"threadId"`
	PreviousWorkspaceID string    `json:"previousWorkspaceId,omitempty"`
	ActiveWorkspaceID   string    `json:"activeWorkspaceId"`
	BindingVersion      int64     `json:"bindingVersion"`
	ContextGeneration   int64     `json:"contextGeneration"`
	SwitchedAt          time.Time `json:"switchedAt,omitempty"`
	// Replayed is internal only. Replaying an old switch response must not
	// overwrite the Thread's current read-model binding.
	Replayed bool `json:"-"`
}

// threadWorkspaceSwitchCommand is the durable request identity shared by the
// normal repository path and the distributed-fence transaction path.
type threadWorkspaceSwitchCommand struct {
	TenantID               string
	UserID                 string
	ThreadID               string
	TargetWorkspaceID      string
	ExpectedBindingVersion int64
	IdempotencyKey         string
}

// threadWorkspaceSwitchBeforeApply runs after an exact idempotency replay has
// been returned and the Thread row is locked, but before a new switch/no-op
// ledger entry can commit. The fenced caller locks and validates both
// Workspace rows through this hook in the same PostgreSQL transaction.
type threadWorkspaceSwitchBeforeApply func(current ThreadWorkspaceBinding) error

// threadWorkspaceSwitchIdempotencyRecord mirrors the durable
// thread_workspace_switch_idempotency fact for local tests. A key is valid
// only for one Thread and one semantic switch request; storing the returned
// binding ensures a later switch cannot change a replayed response.
type threadWorkspaceSwitchIdempotencyRecord struct {
	TenantID               string
	UserID                 string
	ThreadID               string
	TargetWorkspaceID      string
	ExpectedBindingVersion int64
	Binding                ThreadWorkspaceBinding
}

type RunWorkspaceContextRecord = domain.RunWorkspaceContextRecord

type AgentRunEventNotifier interface {
	Subscribe(agentRunID string) (<-chan struct{}, func())
	Notify(agentRunID string)
	ActiveSubscriptions(agentRunID string) int
}

type localAgentRunEventNotifier struct {
	mu               sync.Mutex
	subscribers      map[string]map[uint64]chan struct{}
	nextSubscriberID uint64
	recoveryCursor   uint64
}

type AgentRunRepository struct {
	db                *Database
	mu                sync.Mutex
	runs              map[string]AgentRunRecord
	idempotency       map[string]string
	plans             map[string]map[int]map[string]any
	events            map[string][]AgentRunEvent
	threadBindings    map[string]ThreadWorkspaceBinding
	threadHistory     map[string][]ThreadWorkspaceBinding
	threadSwitchKeys  map[string]threadWorkspaceSwitchIdempotencyRecord
	workspaceContexts map[string]RunWorkspaceContextRecord
	eventIdempotency  map[string]AgentRunEvent
	eventNotifier     AgentRunEventNotifier
}

// The in-memory repositories are test-only mirrors with no shared database
// transaction manager. This lock gives their confirmation path one serialized
// commit boundary; production never reaches this fallback when a database is
// configured.
var agentRunConfirmationMemoryMu sync.Mutex

func NewAgentRunRepository(db *Database) *AgentRunRepository {
	return NewAgentRunRepositoryWithNotifier(db, nil)
}

func NewAgentRunRepositoryWithNotifier(db *Database, notifier AgentRunEventNotifier) *AgentRunRepository {
	if notifier == nil {
		notifier = &localAgentRunEventNotifier{subscribers: map[string]map[uint64]chan struct{}{}}
	}
	return &AgentRunRepository{
		db: db, runs: map[string]AgentRunRecord{}, idempotency: map[string]string{},
		plans: map[string]map[int]map[string]any{}, events: map[string][]AgentRunEvent{},
		threadBindings: map[string]ThreadWorkspaceBinding{}, threadHistory: map[string][]ThreadWorkspaceBinding{}, threadSwitchKeys: map[string]threadWorkspaceSwitchIdempotencyRecord{},
		workspaceContexts: map[string]RunWorkspaceContextRecord{},
		eventIdempotency:  map[string]AgentRunEvent{},
		eventNotifier:     notifier,
	}
}

func (r *AgentRunRepository) SaveWorkspaceContext(ctx context.Context, record RunWorkspaceContextRecord) error {
	if record.RunID == "" || record.TenantID == "" || record.UserID == "" || record.WorkspaceID == "" || record.ManifestHash == "" || record.CapabilityHash == "" {
		return fmt.Errorf("INVALID_ARGUMENT")
	}
	if record.Status == "" {
		record.Status = "frozen"
	}
	if r.db != nil && !r.db.Disabled && r.db.Pool != nil {
		readRoots, _ := json.Marshal(record.AllowedReadRoots)
		writeRoots, _ := json.Marshal(record.AllowedWriteRoots)
		skills, _ := json.Marshal(record.SelectedSkills)
		knowledge, _ := json.Marshal(record.SelectedKnowledgeRefs)
		manifest, _ := json.Marshal(record.ContextManifest)
		return r.db.WithSerializableRetry(ctx, "freeze run workspace context", 3, func(tx *Tx) error {
			existing, err := scanRunWorkspaceContextRow(tx.QueryRowRaw(ctx, runWorkspaceContextSelect+` where run_id=$1 for update`, record.RunID))
			if err == nil {
				if !sameRunWorkspaceContext(existing, record) {
					return fmt.Errorf("WORKSPACE_VERSION_CONFLICT")
				}
				return nil
			}
			if !errors.Is(err, pgx.ErrNoRows) {
				return err
			}
			// Freeze must observe the same Workspace/binding tuple that created the
			// Run. Lock the rows only when first materializing the context: an exact
			// retry must keep returning its historical immutable snapshot even after
			// a later Workspace write or Thread switch.
			if err := validateRunWorkspaceContextBindingInTx(ctx, tx, record); err != nil {
				return err
			}
			_, err = tx.ExecRaw(ctx, `insert into run_workspace_contexts(run_id,agent_run_id,tenant_id,user_id,workspace_id,workspace_version,index_version,thread_id,thread_workspace_binding_version,context_generation,session_generation,l1_agent_profile,manifest_version,capability_hash,agent_relative_root,allowed_read_roots,allowed_write_roots,selected_skills,selected_knowledge_refs,user_timezone,context_manifest,manifest_hash,status) values($1,nullif($2,''),$3,$4,$5,$6,$7,nullif($8,''),$9,$10,$11,$12,$13,$14,nullif($15,''),$16::jsonb,$17::jsonb,$18::jsonb,$19::jsonb,$20,$21::jsonb,$22,$23)`, record.RunID, record.AgentRunID, record.TenantID, record.UserID, record.WorkspaceID, record.WorkspaceVersion, record.IndexVersion, record.ThreadID, record.ThreadWorkspaceBindingVersion, record.ContextGeneration, record.SessionGeneration, record.L1AgentProfile, record.ManifestVersion, record.CapabilityHash, record.AgentRelativeRoot, string(readRoots), string(writeRoots), string(skills), string(knowledge), record.UserTimezone, string(manifest), record.ManifestHash, record.Status)
			return err
		})
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing, ok := r.workspaceContexts[record.RunID]; ok {
		if !sameRunWorkspaceContext(existing, record) {
			return fmt.Errorf("WORKSPACE_VERSION_CONFLICT")
		}
		return nil
	}
	record.ContextManifest = copyMap(record.ContextManifest)
	r.workspaceContexts[record.RunID] = record
	return nil
}

func (r *AgentRunRepository) GetWorkspaceContextByRunID(ctx context.Context, runID string) (RunWorkspaceContextRecord, error) {
	if r.db != nil && !r.db.Disabled && r.db.Pool != nil {
		return scanRunWorkspaceContextRow(r.db.Pool.QueryRow(ctx, runWorkspaceContextSelect+` where run_id=$1`, runID))
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	record, ok := r.workspaceContexts[runID]
	if !ok {
		return RunWorkspaceContextRecord{}, fmt.Errorf("NOT_FOUND")
	}
	record.ContextManifest = copyMap(record.ContextManifest)
	return record, nil
}

const runWorkspaceContextSelect = `select run_id,coalesce(agent_run_id,''),tenant_id,user_id,workspace_id,workspace_version,index_version,coalesce(thread_id,''),coalesce(thread_workspace_binding_version,0),context_generation,coalesce(session_generation,0),l1_agent_profile,manifest_version,capability_hash,coalesce(agent_relative_root,''),allowed_read_roots,allowed_write_roots,selected_skills,selected_knowledge_refs,coalesce(user_timezone,''),context_manifest,manifest_hash,status from run_workspace_contexts`

type runWorkspaceContextRowScanner interface{ Scan(...any) error }

func scanRunWorkspaceContextRow(row runWorkspaceContextRowScanner) (RunWorkspaceContextRecord, error) {
	var record RunWorkspaceContextRecord
	var readRoots, writeRoots, skills, knowledge, manifest []byte
	err := row.Scan(&record.RunID, &record.AgentRunID, &record.TenantID, &record.UserID, &record.WorkspaceID, &record.WorkspaceVersion, &record.IndexVersion, &record.ThreadID, &record.ThreadWorkspaceBindingVersion, &record.ContextGeneration, &record.SessionGeneration, &record.L1AgentProfile, &record.ManifestVersion, &record.CapabilityHash, &record.AgentRelativeRoot, &readRoots, &writeRoots, &skills, &knowledge, &record.UserTimezone, &manifest, &record.ManifestHash, &record.Status)
	if err != nil {
		return RunWorkspaceContextRecord{}, err
	}
	_ = json.Unmarshal(readRoots, &record.AllowedReadRoots)
	_ = json.Unmarshal(writeRoots, &record.AllowedWriteRoots)
	_ = json.Unmarshal(skills, &record.SelectedSkills)
	_ = json.Unmarshal(knowledge, &record.SelectedKnowledgeRefs)
	_ = json.Unmarshal(manifest, &record.ContextManifest)
	return record, nil
}

func validateRunWorkspaceContextBindingInTx(ctx context.Context, tx *Tx, record RunWorkspaceContextRecord) error {
	if !validRunWorkspaceContextManifest(record) {
		return fmt.Errorf("AGENT_PLAN_INVALID")
	}
	if record.AgentRunID != "" {
		var tenantID, userID, workspaceID, status string
		var workspaceVersion, bindingVersion, contextGeneration int64
		err := tx.QueryRowRaw(ctx, `select tenant_id,user_id,workspace_id,status,workspace_version,workspace_binding_version,context_generation from agent_runs where agent_run_id=$1 for share`, record.AgentRunID).Scan(&tenantID, &userID, &workspaceID, &status, &workspaceVersion, &bindingVersion, &contextGeneration)
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("NOT_FOUND")
		}
		if err != nil {
			return err
		}
		if status != "planning" || tenantID != record.TenantID || userID != record.UserID || workspaceID != record.WorkspaceID ||
			workspaceVersion != record.WorkspaceVersion || bindingVersion != record.ThreadWorkspaceBindingVersion || contextGeneration != record.ContextGeneration {
			return fmt.Errorf("WORKSPACE_VERSION_CONFLICT")
		}
	}

	var tenantID, userID, workspaceStatus string
	var workspaceVersion, indexVersion int64
	err := tx.QueryRowRaw(ctx, `select tenant_id,user_id,status,workspace_version,index_version from workspaces where workspace_id=$1 for share`, record.WorkspaceID).Scan(&tenantID, &userID, &workspaceStatus, &workspaceVersion, &indexVersion)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("NOT_FOUND")
	}
	if err != nil {
		return err
	}
	if tenantID != record.TenantID || userID != record.UserID {
		return fmt.Errorf("RUNTIME_PERMISSION_DENIED")
	}
	if workspaceStatus != "ready" && workspaceStatus != "active" {
		return fmt.Errorf("WORKSPACE_NOT_READY")
	}
	if workspaceVersion != record.WorkspaceVersion || indexVersion != record.IndexVersion {
		return fmt.Errorf("WORKSPACE_VERSION_CONFLICT")
	}
	return nil
}

func sameRunWorkspaceContext(left, right RunWorkspaceContextRecord) bool {
	if left.RunID != right.RunID || left.AgentRunID != right.AgentRunID || left.TenantID != right.TenantID || left.UserID != right.UserID ||
		left.WorkspaceID != right.WorkspaceID || left.WorkspaceVersion != right.WorkspaceVersion || left.IndexVersion != right.IndexVersion ||
		left.ThreadID != right.ThreadID || left.ThreadWorkspaceBindingVersion != right.ThreadWorkspaceBindingVersion ||
		left.ContextGeneration != right.ContextGeneration || left.SessionGeneration != right.SessionGeneration || left.L1AgentProfile != right.L1AgentProfile ||
		left.AgentRelativeRoot != right.AgentRelativeRoot || left.ManifestVersion != right.ManifestVersion || left.CapabilityHash != right.CapabilityHash ||
		left.UserTimezone != right.UserTimezone ||
		left.ManifestHash != right.ManifestHash || left.Status != right.Status {
		return false
	}
	return canonicalRunWorkspaceContextValue(left.AllowedReadRoots) == canonicalRunWorkspaceContextValue(right.AllowedReadRoots) &&
		canonicalRunWorkspaceContextValue(left.AllowedWriteRoots) == canonicalRunWorkspaceContextValue(right.AllowedWriteRoots) &&
		canonicalRunWorkspaceContextValue(left.SelectedSkills) == canonicalRunWorkspaceContextValue(right.SelectedSkills) &&
		canonicalRunWorkspaceContextValue(left.SelectedKnowledgeRefs) == canonicalRunWorkspaceContextValue(right.SelectedKnowledgeRefs) &&
		canonicalRunWorkspaceContextValue(left.ContextManifest) == canonicalRunWorkspaceContextValue(right.ContextManifest)
}

func canonicalRunWorkspaceContextValue(value any) string {
	raw, err := json.Marshal(value)
	if err != nil {
		return ""
	}
	return string(raw)
}

// ValidateFrozenWorkspaceContextForPlan is the dispatch-side revalidation of
// the immutable Workspace snapshot. FinalizePlanningSuccess performs the same
// comparison inside its durable transaction; Dispatcher repeats it before a
// Runtime handoff so a legacy/directly persisted Plan cannot bypass the
// freeze-to-Plan binding.
func (r *AgentRunRepository) ValidateFrozenWorkspaceContextForPlan(ctx context.Context, run AgentRunRecord, plan map[string]any) error {
	frozen, err := r.GetWorkspaceContextByRunID(ctx, run.AgentRunID)
	if err != nil {
		return err
	}
	return validateFrozenWorkspaceContextForPlan(run, frozen, plan)
}

func validateFrozenWorkspaceContextForPlan(run AgentRunRecord, frozen RunWorkspaceContextRecord, plan map[string]any) error {
	if run.AgentRunID == "" || frozen.Status != "frozen" || frozen.AgentRunID != run.AgentRunID ||
		frozen.TenantID != run.TenantID || frozen.UserID != run.UserID || frozen.WorkspaceID != run.WorkspaceID ||
		frozen.WorkspaceVersion != run.WorkspaceVersion || frozen.ThreadID != run.ThreadID ||
		frozen.ThreadWorkspaceBindingVersion != run.BindingVersion || frozen.ContextGeneration != run.ContextGeneration ||
		!validRunWorkspaceContextManifest(frozen) {
		return fmt.Errorf("WORKSPACE_VERSION_CONFLICT")
	}
	if strings.TrimSpace(stringOr(plan["workspaceContextManifestHash"], "")) == "" ||
		stringOr(plan["workspaceContextManifestHash"], "") != frozen.ManifestHash ||
		int64Value(plan["workspaceVersion"]) != frozen.WorkspaceVersion || int64Value(plan["indexVersion"]) != frozen.IndexVersion ||
		stringOr(plan["capabilityHash"], "") != frozen.CapabilityHash ||
		stringOr(plan["l1AgentProfile"], "") != frozen.L1AgentProfile ||
		stringOr(plan["agentRelativeRoot"], "") != frozen.AgentRelativeRoot ||
		stringOr(plan["manifestVersion"], "") != frozen.ManifestVersion ||
		canonicalRunWorkspaceContextValue(plan["selectedSkillProfiles"]) != canonicalRunWorkspaceContextValue(frozen.SelectedSkills) ||
		canonicalRunWorkspaceContextValue(plan["selectedKnowledgeRefs"]) != canonicalRunWorkspaceContextValue(frozen.SelectedKnowledgeRefs) ||
		canonicalRunWorkspaceContextValue(plan["requiredTools"]) != canonicalRunWorkspaceContextValue(frozen.ContextManifest["requiredTools"]) {
		return fmt.Errorf("WORKSPACE_VERSION_CONFLICT")
	}
	return nil
}

func validRunWorkspaceContextManifest(record RunWorkspaceContextRecord) bool {
	raw, err := json.Marshal(record.ContextManifest)
	if err != nil {
		return false
	}
	sum := sha256.Sum256(raw)
	return record.ManifestHash == "sha256:"+hex.EncodeToString(sum[:])
}

func (r *AgentRunRepository) CreateRun(ctx context.Context, command CreateAgentRunCommand) (AgentRunRecord, bool, error) {
	if r.db != nil && !r.db.Disabled && r.db.Pool != nil {
		return r.createRunPostgres(ctx, command.Record)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	key := command.Record.UserID + "\x00" + command.Record.IdempotencyKey
	if id := r.idempotency[key]; id != "" {
		existing := r.runs[id]
		if !CompareRequestHash(existing.RequestHash, command.Record.RequestHash) {
			return AgentRunRecord{}, true, fmt.Errorf("IDEMPOTENCY_KEY_CONFLICT")
		}
		return copyAgentRun(existing), true, nil
	}
	now := time.Now().UTC()
	command.Record.CreatedAt, command.Record.UpdatedAt = now, now
	command.Record.IntentSnapshot = copyMap(command.Record.IntentSnapshot)
	command.Record.PlanSnapshot = map[string]any{}
	command.Record.PublicResult = map[string]any{}
	command.Record.ErrorSummary = map[string]any{}
	r.runs[command.Record.AgentRunID] = copyAgentRun(command.Record)
	r.idempotency[key] = command.Record.AgentRunID
	return copyAgentRun(command.Record), false, nil
}

func (r *AgentRunRepository) GetRun(ctx context.Context, tenantID, userID, agentRunID string) (AgentRunRecord, error) {
	if r.db != nil && !r.db.Disabled && r.db.Pool != nil {
		return r.getRunPostgres(ctx, tenantID, userID, agentRunID)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	record, ok := r.runs[agentRunID]
	if !ok || record.UserID != userID || record.TenantID != tenantID {
		return AgentRunRecord{}, fmt.Errorf("NOT_FOUND")
	}
	return copyAgentRun(record), nil
}

// FindByIdempotency returns the scoped durable AgentRun record. The caller
// must separately compare the request hash before treating a retry as a
// replay; this avoids accepting a semantic key conflict as success.
func (r *AgentRunRepository) FindByIdempotency(ctx context.Context, userID, key string) (AgentRunRecord, error) {
	userID, key = strings.TrimSpace(userID), strings.TrimSpace(key)
	if userID == "" || key == "" {
		return AgentRunRecord{}, fmt.Errorf("NOT_FOUND")
	}
	if r.db != nil && !r.db.Disabled && r.db.Pool != nil {
		return r.findByIdempotencyPostgres(ctx, userID, key)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	id := r.idempotency[userID+"\x00"+key]
	record, ok := r.runs[id]
	if !ok {
		return AgentRunRecord{}, fmt.Errorf("NOT_FOUND")
	}
	return copyAgentRun(record), nil
}

// CompareRequestHash performs a constant-time equality check for the
// canonical request hashes stored with an idempotency key. Empty hashes never
// compare equal because they are not a valid replay proof.
func CompareRequestHash(stored, candidate string) bool {
	stored, candidate = strings.TrimSpace(stored), strings.TrimSpace(candidate)
	if stored == "" || candidate == "" || len(stored) != len(candidate) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(stored), []byte(candidate)) == 1
}

func (r *AgentRunRepository) GetRunInternal(ctx context.Context, agentRunID string) (AgentRunRecord, error) {
	if strings.TrimSpace(agentRunID) == "" {
		return AgentRunRecord{}, fmt.Errorf("NOT_FOUND")
	}
	if r.db != nil && !r.db.Disabled && r.db.Pool != nil {
		return r.scanRunRow(r.db.Pool.QueryRow(ctx, agentRunSelect+` where agent_run_id=$1`, agentRunID))
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	record, ok := r.runs[agentRunID]
	if !ok {
		return AgentRunRecord{}, fmt.Errorf("NOT_FOUND")
	}
	return copyAgentRun(record), nil
}

func (r *AgentRunRepository) SaveIntent(ctx context.Context, agentRunID string, intent map[string]any, resolverVersion string) error {
	intent = copyMap(intent)
	intent["resolverVersion"] = resolverVersion
	if r.db != nil && !r.db.Disabled && r.db.Pool != nil {
		raw, _ := json.Marshal(intent)
		result, err := r.db.Pool.Exec(ctx, `update agent_runs set intent_snapshot=$2::jsonb, status='planning', updated_at=now() where agent_run_id=$1 and status in ('created','resolving_intent')`, agentRunID, string(raw))
		if err != nil {
			return err
		}
		if result.RowsAffected() != 1 {
			return fmt.Errorf("AGENT_PLAN_INVALID")
		}
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	record, ok := r.runs[agentRunID]
	if !ok || (record.Status != "created" && record.Status != "resolving_intent") {
		return fmt.Errorf("AGENT_PLAN_INVALID")
	}
	record.IntentSnapshot = intent
	record.Status = "planning"
	record.UpdatedAt = time.Now().UTC()
	r.runs[agentRunID] = record
	return nil
}

func (r *AgentRunRepository) SavePlan(ctx context.Context, record AgentRunPlanRecord) error {
	if record.AgentRunID == "" || record.PlanVersion < 1 || !validAgentPlanStatus(record.PlanStatus) || !validAgentRunStatus(record.AgentRunStatus) {
		return fmt.Errorf("AGENT_PLAN_INVALID")
	}
	plan := copyMap(record.Plan)
	plan["status"] = record.PlanStatus
	if r.db != nil && !r.db.Disabled && r.db.Pool != nil {
		raw, _ := json.Marshal(plan)
		selectedSkills, _ := json.Marshal(plan["selectedSkillProfiles"])
		knowledge, _ := json.Marshal(plan["selectedKnowledgeRefs"])
		tools, _ := json.Marshal(plan["requiredTools"])
		output, _ := json.Marshal(plan["outputContract"])
		return r.db.WithTx(ctx, func(tx *Tx) error {
			runRows, err := tx.Query(ctx, `select status from agent_runs where agent_run_id=@run for update`, map[string]any{"run": record.AgentRunID})
			if err != nil {
				return err
			}
			if len(runRows) != 1 || stringOr(runRows[0]["status"], "") != "planning" {
				return fmt.Errorf("AGENT_PLAN_INVALID")
			}
			inserted, err := tx.ExecRaw(ctx, `insert into agent_run_plans(agent_run_plan_id,agent_run_id,plan_version,status,task_type,l1_agent_profile,selected_skills,selected_knowledge_refs,required_tools,output_contract,workspace_version,index_version,manifest_version,capability_hash,safe_plan_summary) values ($1,$2,$3,$4,$5,$6,$7::jsonb,$8::jsonb,$9::jsonb,$10::jsonb,$11,$12,$13,$14,$15) on conflict(agent_run_id,plan_version) do nothing`,
				fmt.Sprintf("agent_plan_%s_%d", record.AgentRunID, record.PlanVersion), record.AgentRunID, record.PlanVersion, record.PlanStatus,
				fmt.Sprint(plan["taskType"]), fmt.Sprint(plan["l1AgentProfile"]), string(selectedSkills), string(knowledge), string(tools), string(output),
				int64Value(plan["workspaceVersion"]), int64Value(plan["indexVersion"]), fmt.Sprint(plan["manifestVersion"]), fmt.Sprint(plan["capabilityHash"]), fmt.Sprint(plan["safePlanSummary"]),
			)
			if err != nil {
				return err
			}
			if inserted.RowsAffected() != 1 {
				return fmt.Errorf("AGENT_PLAN_INVALID")
			}
			updated, err := tx.ExecRaw(ctx, `update agent_runs set plan_snapshot=$1::jsonb,status=$2,updated_at=now() where agent_run_id=$3 and status='planning'`, string(raw), record.AgentRunStatus, record.AgentRunID)
			if err != nil {
				return err
			}
			if updated.RowsAffected() != 1 {
				return fmt.Errorf("AGENT_PLAN_INVALID")
			}
			return nil
		})
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	run, ok := r.runs[record.AgentRunID]
	if !ok || run.Status != "planning" {
		return fmt.Errorf("AGENT_PLAN_INVALID")
	}
	if r.plans[record.AgentRunID] != nil && r.plans[record.AgentRunID][record.PlanVersion] != nil {
		return fmt.Errorf("AGENT_PLAN_INVALID")
	}
	if r.plans[record.AgentRunID] == nil {
		r.plans[record.AgentRunID] = map[int]map[string]any{}
	}
	r.plans[record.AgentRunID][record.PlanVersion] = copyMap(plan)
	run.PlanSnapshot = copyMap(plan)
	run.Status = record.AgentRunStatus
	run.UpdatedAt = time.Now().UTC()
	r.runs[record.AgentRunID] = run
	return nil
}

func (r *AgentRunRepository) MarkPlanStatus(ctx context.Context, agentRunID string, planVersion int, expected, next string) error {
	if agentRunID == "" || planVersion < 1 || !validAgentPlanStatus(expected) || !validAgentPlanStatus(next) {
		return fmt.Errorf("AGENT_PLAN_INVALID")
	}
	if r.db != nil && !r.db.Disabled && r.db.Pool != nil {
		result, err := r.db.Pool.Exec(ctx, `update agent_run_plans set status=$4,updated_at=now() where agent_run_id=$1 and plan_version=$2 and status=$3`, agentRunID, planVersion, expected, next)
		if err != nil {
			return err
		}
		if result.RowsAffected() != 1 {
			return fmt.Errorf("AGENT_PLAN_EXPIRED")
		}
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	plan := r.plans[agentRunID][planVersion]
	if plan == nil || fmt.Sprint(plan["status"]) != expected {
		return fmt.Errorf("AGENT_PLAN_EXPIRED")
	}
	plan = copyMap(plan)
	plan["status"] = next
	r.plans[agentRunID][planVersion] = plan
	return nil
}

func (r *AgentRunRepository) ConfirmPlanAndEnqueue(ctx context.Context, agentRunID string, planVersion int, lane string, queueRepo *QueueRepository) error {
	if agentRunID == "" || planVersion < 1 || lane == "" || queueRepo == nil {
		return fmt.Errorf("AGENT_PLAN_INVALID")
	}
	// Legacy callers do not carry the authenticated owner, Usage repository and
	// AiTask boundary required by the SCM confirmation contract. Keep the symbol
	// fail-closed while source callers migrate to
	// ConfirmPlanAndEnqueueWithAdmission.
	return fmt.Errorf("AGENT_RUN_ADMISSION_UNAVAILABLE")
}

// AuthorizeDispatchSubmit is the cancel/submit race fence. It is deliberately
// persisted before the outbound Runtime request: a cancel that commits first
// prevents submit, while a later cancel observes a durable active owner and
// requests the single abort path.
func (r *AgentRunRepository) AuthorizeDispatchSubmit(ctx context.Context, agentRunID, dispatchID, reservationID string, fencingToken int64) error {
	agentRunID, dispatchID, reservationID = strings.TrimSpace(agentRunID), strings.TrimSpace(dispatchID), strings.TrimSpace(reservationID)
	if agentRunID == "" || dispatchID == "" || reservationID == "" || fencingToken < 1 {
		return fmt.Errorf("INVALID_ARGUMENT")
	}
	if r.db != nil && !r.db.Disabled && r.db.Pool != nil {
		return r.db.WithTx(ctx, func(tx *Tx) error {
			runRows, err := tx.Query(ctx, `select status,cancel_requested_at,submit_authorized_at from agent_runs where agent_run_id=@run for update`, map[string]any{"run": agentRunID})
			if err != nil {
				return err
			}
			if len(runRows) != 1 {
				return fmt.Errorf("NOT_FOUND")
			}
			dispatchRows, err := tx.Query(ctx, `select run_id,reservation_id,fencing_token,state from runtime_run_dispatches where dispatch_id=@dispatch for update`, map[string]any{"dispatch": dispatchID})
			if err != nil {
				return err
			}
			if len(dispatchRows) != 1 || fmt.Sprint(dispatchRows[0]["run_id"]) != agentRunID || fmt.Sprint(dispatchRows[0]["reservation_id"]) != reservationID || int64Value(dispatchRows[0]["fencing_token"]) != fencingToken {
				return fmt.Errorf("STALE_FENCING_TOKEN")
			}
			reservationRows, err := tx.Query(ctx, `select run_id,fencing_token,state from runtime_slot_reservations where reservation_id=@reservation for update`, map[string]any{"reservation": reservationID})
			if err != nil {
				return err
			}
			if len(reservationRows) != 1 || fmt.Sprint(reservationRows[0]["run_id"]) != agentRunID || int64Value(reservationRows[0]["fencing_token"]) != fencingToken {
				return fmt.Errorf("STALE_FENCING_TOKEN")
			}
			status := fmt.Sprint(runRows[0]["status"])
			cancelRequested := runRows[0]["cancel_requested_at"] != nil
			if cancelRequested || status == "cancelled" || status == "aborting" {
				return fmt.Errorf("AGENT_RUN_CANCELLED")
			}
			if status == "dispatched" && runRows[0]["submit_authorized_at"] != nil && fmt.Sprint(dispatchRows[0]["state"]) == "created" && fmt.Sprint(reservationRows[0]["state"]) == "reserved" {
				return nil
			}
			if status != "queued" || fmt.Sprint(dispatchRows[0]["state"]) != "created" || fmt.Sprint(reservationRows[0]["state"]) != "reserved" {
				return fmt.Errorf("AGENT_PLAN_EXPIRED")
			}
			return tx.Exec(ctx, `update agent_runs set status='dispatched',submit_authorized_at=coalesce(submit_authorized_at,now()),updated_at=now() where agent_run_id=@run and status='queued' and cancel_requested_at is null`, map[string]any{"run": agentRunID})
		})
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	run, ok := r.runs[agentRunID]
	if !ok {
		return fmt.Errorf("NOT_FOUND")
	}
	if run.CancelRequestedAt != nil || run.Status == "cancelled" || run.Status == "aborting" {
		return fmt.Errorf("AGENT_RUN_CANCELLED")
	}
	if run.Status == "dispatched" && run.SubmitAuthorizedAt != nil {
		return nil
	}
	if run.Status != "queued" {
		return fmt.Errorf("AGENT_PLAN_EXPIRED")
	}
	now := time.Now().UTC()
	run.Status, run.SubmitAuthorizedAt, run.UpdatedAt = "dispatched", &now, now
	r.runs[agentRunID] = run
	return nil
}

// RequestCancelAndEnqueue persists a safe cancellation intent. Only a Run
// without a durable dispatch/reservation owner can become locally cancelled;
// every reserving/dispatched/accepted path becomes aborting and has one
// identifier-only Runtime abort job.
func (r *AgentRunRepository) RequestCancelAndEnqueue(ctx context.Context, agentRunID, reasonCode, reasonHash string, queueRepo *QueueRepository) (CancelDecision, error) {
	agentRunID, reasonCode, reasonHash = strings.TrimSpace(agentRunID), strings.TrimSpace(reasonCode), strings.TrimSpace(reasonHash)
	if agentRunID == "" || !validAgentRunCancelReason(reasonCode) || !validAgentRunReasonHash(reasonHash) || queueRepo == nil {
		return CancelDecision{}, fmt.Errorf("INVALID_ARGUMENT")
	}
	const lane = "runtime_abort"
	queueID := lane + ":" + agentRunID
	if r.db != nil && !r.db.Disabled && r.db.Pool != nil {
		decision := CancelDecision{}
		rawPayload, _ := json.Marshal(map[string]any{"agentRunId": agentRunID})
		err := r.db.WithTx(ctx, func(tx *Tx) error {
			runRows, err := tx.Query(ctx, `select status,cancel_requested_at from agent_runs where agent_run_id=@run for update`, map[string]any{"run": agentRunID})
			if err != nil {
				return err
			}
			if len(runRows) != 1 {
				return fmt.Errorf("NOT_FOUND")
			}
			status := fmt.Sprint(runRows[0]["status"])
			if terminalAgentRunStatus(status) {
				decision = CancelDecision{Status: status, Terminal: true}
				return nil
			}
			dispatchRows, err := tx.Query(ctx, `select dispatch_id from runtime_run_dispatches where run_id=@run and state in ('created','sent','submit_unknown','retry_same_host','accepted','materializing','running','finalizing','recovering') order by dispatch_attempt for update`, map[string]any{"run": agentRunID})
			if err != nil {
				return err
			}
			reservationRows, err := tx.Query(ctx, `select reservation_id from runtime_slot_reservations where run_id=@run and state in ('reserved','accepted','running') order by reservation_id for update`, map[string]any{"run": agentRunID})
			if err != nil {
				return err
			}
			owned := len(dispatchRows) > 0 || len(reservationRows) > 0 || activeAgentRunOwnershipStatus(status)
			if !owned {
				if err := tx.Exec(ctx, `update agent_runs set status='cancelled',cancel_requested_at=coalesce(cancel_requested_at,now()),cancel_reason_code=coalesce(nullif(cancel_reason_code,''),@reasonCode),cancel_reason_hash=coalesce(nullif(cancel_reason_hash,''),@reasonHash),error_summary=coalesce(error_summary,'{}'::jsonb)||jsonb_build_object('code',@reasonCode),updated_at=now() where agent_run_id=@run`, map[string]any{"run": agentRunID, "reasonCode": reasonCode, "reasonHash": reasonHash}); err != nil {
					return err
				}
				decision = CancelDecision{Status: "cancelled", StateChanged: status != "cancelled"}
				return nil
			}
			if err := tx.Exec(ctx, `update agent_runs set status='aborting',cancel_requested_at=coalesce(cancel_requested_at,now()),cancel_reason_code=coalesce(nullif(cancel_reason_code,''),@reasonCode),cancel_reason_hash=coalesce(nullif(cancel_reason_hash,''),@reasonHash),error_summary=coalesce(error_summary,'{}'::jsonb)||jsonb_build_object('code',@reasonCode),updated_at=now() where agent_run_id=@run`, map[string]any{"run": agentRunID, "reasonCode": reasonCode, "reasonHash": reasonHash}); err != nil {
				return err
			}
			if err := tx.Exec(ctx, `insert into task_queue_records(queue_id,queue_name,task_type,task_id,dedupe_key,status,priority,max_attempts,available_at,payload,attempt_series_id) values(@queueId,@lane,'runtime_abort',@run,@dedupe,'pending',200,8,now(),@payload::jsonb,default) on conflict(queue_id) do nothing`, map[string]any{"queueId": queueID, "lane": lane, "run": agentRunID, "dedupe": "runtime_abort:" + agentRunID, "payload": string(rawPayload)}); err != nil {
				return err
			}
			decision = CancelDecision{Status: "aborting", AbortEnqueued: true, StateChanged: status != "aborting"}
			return nil
		})
		return decision, err
	}

	r.mu.Lock()
	run, ok := r.runs[agentRunID]
	if !ok {
		r.mu.Unlock()
		return CancelDecision{}, fmt.Errorf("NOT_FOUND")
	}
	if terminalAgentRunStatus(run.Status) {
		decision := CancelDecision{Status: run.Status, Terminal: true}
		r.mu.Unlock()
		return decision, nil
	}
	now := time.Now().UTC()
	if run.CancelRequestedAt == nil {
		run.CancelRequestedAt = &now
		run.CancelReasonCode = reasonCode
		run.CancelReasonHash = reasonHash
	}
	owned := activeAgentRunOwnershipStatus(run.Status)
	decision := CancelDecision{}
	if !owned {
		run.Status = "cancelled"
		run.ErrorSummary = map[string]any{"code": reasonCode}
		run.UpdatedAt = now
		r.runs[agentRunID] = run
		decision = CancelDecision{Status: "cancelled", StateChanged: true}
		r.mu.Unlock()
		return decision, nil
	}
	changed := run.Status != "aborting"
	run.Status = "aborting"
	run.ErrorSummary = map[string]any{"code": reasonCode}
	run.UpdatedAt = now
	r.runs[agentRunID] = run
	r.mu.Unlock()
	queued := queueRepo.Enqueue(map[string]any{"queueId": queueID, "queueName": lane, "taskType": "runtime_abort", "taskId": agentRunID, "dedupeKey": "runtime_abort:" + agentRunID, "priority": 200, "maxAttempts": 8, "payload": map[string]any{"agentRunId": agentRunID}})
	if fmt.Sprint(queued["queueId"]) != queueID || fmt.Sprint(queued["status"]) == "durable_failure" {
		return CancelDecision{}, fmt.Errorf("SERVICE_BUSY")
	}
	return CancelDecision{Status: "aborting", AbortEnqueued: true, StateChanged: changed}, nil
}

// RequestAbortAndEnqueue remains a narrow internal compatibility entry point
// for non-user failures. New API cancellation uses RequestCancelAndEnqueue so
// every reason is an allowlisted code and the queue remains identifier-only.
func (r *AgentRunRepository) RequestAbortAndEnqueue(ctx context.Context, agentRunID, reasonHash string, queueRepo *QueueRepository) error {
	_, err := r.RequestCancelAndEnqueue(ctx, agentRunID, "LEASE_LOST", reasonHash, queueRepo)
	return err
}

func (r *AgentRunRepository) GetPlan(ctx context.Context, agentRunID string, version int) (map[string]any, error) {
	if r.db != nil && !r.db.Disabled && r.db.Pool != nil {
		var raw []byte
		err := r.db.Pool.QueryRow(ctx, `select ar.plan_snapshot from agent_runs ar join agent_run_plans p on p.agent_run_id=ar.agent_run_id where ar.agent_run_id=$1 and p.plan_version=$2`, agentRunID, version).Scan(&raw)
		if err != nil {
			return nil, err
		}
		var plan map[string]any
		if err := json.Unmarshal(raw, &plan); err != nil {
			return nil, err
		}
		return plan, nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	plan := r.plans[agentRunID][version]
	if plan == nil {
		return nil, fmt.Errorf("NOT_FOUND")
	}
	return copyMap(plan), nil
}

// LastThreadAgentProfile returns a previously persisted, ownership-scoped L1
// profile for diagnostics and historical presentation only. Dynamic planning
// does not consume it: a prior thread result cannot resolve a current catalog
// ambiguity or select a new Run.
func (r *AgentRunRepository) LastThreadAgentProfile(ctx context.Context, tenantID, userID, workspaceID, threadID string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	tenantID, userID, workspaceID, threadID = strings.TrimSpace(tenantID), strings.TrimSpace(userID), strings.TrimSpace(workspaceID), strings.TrimSpace(threadID)
	if tenantID == "" || userID == "" || workspaceID == "" || threadID == "" {
		return "", nil
	}
	if r.db != nil && !r.db.Disabled && r.db.Pool != nil {
		var profile string
		err := r.db.Pool.QueryRow(ctx, `select p.l1_agent_profile
			from agent_runs ar
			join agent_run_plans p on p.agent_run_id=ar.agent_run_id
			where ar.tenant_id=$1 and ar.user_id=$2 and ar.workspace_id=$3 and ar.thread_id=$4
			  and p.l1_agent_profile <> ''
			order by ar.created_at desc,p.plan_version desc
			limit 1`, tenantID, userID, workspaceID, threadID).Scan(&profile)
		if errors.Is(err, pgx.ErrNoRows) {
			return "", nil
		}
		if err != nil {
			return "", err
		}
		return strings.TrimSpace(profile), nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	var latest time.Time
	latestVersion := -1
	profile := ""
	for runID, run := range r.runs {
		if run.TenantID != tenantID || run.UserID != userID || run.WorkspaceID != workspaceID || run.ThreadID != threadID {
			continue
		}
		for version, plan := range r.plans[runID] {
			candidate := strings.TrimSpace(fmt.Sprint(plan["l1AgentProfile"]))
			if candidate == "" || candidate == "<nil>" {
				continue
			}
			if run.CreatedAt.After(latest) || (run.CreatedAt.Equal(latest) && version > latestVersion) {
				latest, latestVersion, profile = run.CreatedAt, version, candidate
			}
		}
	}
	return profile, nil
}

func (r *AgentRunRepository) AppendPublicEvent(ctx context.Context, event AgentRunEvent) error {
	return r.appendPublicEvent(ctx, event, "")
}

func (r *AgentRunRepository) AppendPublicEventIdempotent(ctx context.Context, event AgentRunEvent, idempotencyKey string) error {
	idempotencyKey = strings.TrimSpace(idempotencyKey)
	if idempotencyKey == "" || strings.TrimSpace(event.AgentRunID) == "" || strings.TrimSpace(event.EventType) == "" {
		return fmt.Errorf("INVALID_ARGUMENT")
	}
	return r.appendPublicEvent(ctx, event, idempotencyKey)
}

// AppendPublicEventIdempotentInTx is the transaction-owned counterpart of
// AppendPublicEventIdempotent. It preserves the same app-safe redaction,
// per-run sequence and idempotency checks, but deliberately does not notify
// subscribers before the caller's outer transaction commits.
func (r *AgentRunRepository) AppendPublicEventIdempotentInTx(ctx context.Context, tx *Tx, event AgentRunEvent, idempotencyKey string) (bool, error) {
	idempotencyKey = strings.TrimSpace(idempotencyKey)
	if r == nil || tx == nil || tx.tx == nil || idempotencyKey == "" || strings.TrimSpace(event.AgentRunID) == "" || strings.TrimSpace(event.EventType) == "" {
		return false, fmt.Errorf("INVALID_ARGUMENT")
	}
	if event.CreatedAt.IsZero() {
		event.CreatedAt = time.Now().UTC()
	}
	event.SafeData = sanitizeAgentEventMap(event.SafeData, 0)
	if event.Status != "" {
		if _, ok := event.SafeData["status"]; !ok {
			event.SafeData["status"] = event.Status
		}
	}
	eventID := stableAgentRunEventID(event.AgentRunID, idempotencyKey)
	if err := tx.Exec(ctx, `select pg_advisory_xact_lock(hashtextextended(@run, 0))`, map[string]any{"run": event.AgentRunID}); err != nil {
		return false, err
	}
	existingRows, err := tx.Query(ctx, `select run_id,sequence,event_type,safe_payload from runtime_run_events where runtime_run_event_id=@id`, map[string]any{"id": eventID})
	if err != nil {
		return false, err
	}
	if len(existingRows) == 1 {
		existing := AgentRunEvent{
			AgentRunID: fmt.Sprint(existingRows[0]["run_id"]),
			Sequence:   int64Value(existingRows[0]["sequence"]),
			EventType:  fmt.Sprint(existingRows[0]["event_type"]),
			SafeData:   mapFromAgentRunValue(existingRows[0]["safe_payload"]),
		}
		existing.Status = fmt.Sprint(existing.SafeData["status"])
		if !sameAgentRunEvent(existing, event) {
			return false, fmt.Errorf("EVENT_IDEMPOTENCY_CONFLICT")
		}
		return false, nil
	}
	if len(existingRows) > 1 {
		return false, fmt.Errorf("EVENT_IDEMPOTENCY_CONFLICT")
	}
	rows, err := tx.Query(ctx, `select coalesce(max(sequence),0) sequence from runtime_run_events where run_id=@run`, map[string]any{"run": event.AgentRunID})
	if err != nil || len(rows) != 1 {
		if err != nil {
			return false, err
		}
		return false, fmt.Errorf("EVENT_SEQUENCE_CONFLICT")
	}
	event.Sequence = int64Value(rows[0]["sequence"]) + 1
	raw, _ := json.Marshal(event.SafeData)
	payloadHash := agentRunEventPayloadHash(event.EventType, raw)
	if err := tx.Exec(ctx, `insert into runtime_run_events(runtime_run_event_id,run_id,sequence,event_type,visibility,safe_payload,usage_delta,payload_hash,occurred_at) values(@id,@run,@sequence,@type,'app_safe',@payload::jsonb,'{}'::jsonb,@payloadHash,@occurredAt)`, map[string]any{"id": eventID, "run": event.AgentRunID, "sequence": event.Sequence, "type": event.EventType, "payload": string(raw), "payloadHash": payloadHash, "occurredAt": event.CreatedAt}); err != nil {
		return false, err
	}
	return true, nil
}

// NotifyPublicEvent publishes a committed app-safe event to the configured
// notifier. Transaction-owned callers invoke it only after their outer commit.
func (r *AgentRunRepository) NotifyPublicEvent(agentRunID string) {
	if r == nil || strings.TrimSpace(agentRunID) == "" {
		return
	}
	r.notifyPublicEventSubscribers(agentRunID)
}

func (r *AgentRunRepository) appendPublicEvent(ctx context.Context, event AgentRunEvent, idempotencyKey string) error {
	if event.CreatedAt.IsZero() {
		event.CreatedAt = time.Now().UTC()
	}
	event.SafeData = sanitizeAgentEventMap(event.SafeData, 0)
	if event.Status != "" {
		if _, ok := event.SafeData["status"]; !ok {
			event.SafeData["status"] = event.Status
		}
	}
	eventID := ""
	if idempotencyKey != "" {
		eventID = stableAgentRunEventID(event.AgentRunID, idempotencyKey)
	}
	if r.db != nil && !r.db.Disabled && r.db.Pool != nil {
		inserted := false
		err := r.db.WithTx(ctx, func(tx *Tx) error {
			if err := tx.Exec(ctx, `select pg_advisory_xact_lock(hashtextextended(@run, 0))`, map[string]any{"run": event.AgentRunID}); err != nil {
				return err
			}
			if eventID != "" {
				existingRows, queryErr := tx.Query(ctx, `select run_id,sequence,event_type,safe_payload from runtime_run_events where runtime_run_event_id=@id`, map[string]any{"id": eventID})
				if queryErr != nil {
					return queryErr
				}
				if len(existingRows) == 1 {
					existing := AgentRunEvent{
						AgentRunID: fmt.Sprint(existingRows[0]["run_id"]),
						Sequence:   int64Value(existingRows[0]["sequence"]),
						EventType:  fmt.Sprint(existingRows[0]["event_type"]),
						SafeData:   mapFromAgentRunValue(existingRows[0]["safe_payload"]),
					}
					existing.Status = fmt.Sprint(existing.SafeData["status"])
					if !sameAgentRunEvent(existing, event) {
						return fmt.Errorf("EVENT_IDEMPOTENCY_CONFLICT")
					}
					return nil
				}
				if len(existingRows) > 1 {
					return fmt.Errorf("EVENT_IDEMPOTENCY_CONFLICT")
				}
			}
			rows, err := tx.Query(ctx, `select coalesce(max(sequence),0) sequence from runtime_run_events where run_id=@run`, map[string]any{"run": event.AgentRunID})
			if err != nil || len(rows) != 1 {
				return err
			}
			last := int64Value(rows[0]["sequence"])
			if event.Sequence <= 0 {
				event.Sequence = last + 1
			} else if event.Sequence <= last {
				duplicate, queryErr := tx.Query(ctx, `select 1 from runtime_run_events where run_id=@run and sequence=@sequence`, map[string]any{"run": event.AgentRunID, "sequence": event.Sequence})
				if queryErr != nil {
					return queryErr
				}
				if len(duplicate) == 1 {
					return nil
				}
				return fmt.Errorf("EVENT_SEQUENCE_CONFLICT")
			} else if event.Sequence != last+1 {
				return fmt.Errorf("EVENT_SEQUENCE_CONFLICT")
			}
			raw, _ := json.Marshal(event.SafeData)
			insertID := eventID
			if insertID == "" {
				insertID = fmt.Sprintf("agent_event_%s_%d", event.AgentRunID, event.Sequence)
			}
			payloadHash := agentRunEventPayloadHash(event.EventType, raw)
			if err := tx.Exec(ctx, `insert into runtime_run_events(runtime_run_event_id,run_id,sequence,event_type,visibility,safe_payload,usage_delta,payload_hash,occurred_at) values(@id,@run,@sequence,@type,'app_safe',@payload::jsonb,'{}'::jsonb,@payloadHash,@occurredAt)`, map[string]any{"id": insertID, "run": event.AgentRunID, "sequence": event.Sequence, "type": event.EventType, "payload": string(raw), "payloadHash": payloadHash, "occurredAt": event.CreatedAt}); err != nil {
				return err
			}
			inserted = true
			return nil
		})
		if err == nil && inserted {
			r.notifyPublicEventSubscribers(event.AgentRunID)
		}
		return err
	}
	r.mu.Lock()
	if idempotencyKey != "" {
		key := event.AgentRunID + "\x00" + idempotencyKey
		if existing, ok := r.eventIdempotency[key]; ok {
			if !sameAgentRunEvent(existing, event) {
				r.mu.Unlock()
				return fmt.Errorf("EVENT_IDEMPOTENCY_CONFLICT")
			}
			r.mu.Unlock()
			return nil
		}
	}
	for _, existing := range r.events[event.AgentRunID] {
		if existing.Sequence == event.Sequence {
			r.mu.Unlock()
			return nil
		}
	}
	last := int64(0)
	if items := r.events[event.AgentRunID]; len(items) > 0 {
		last = items[len(items)-1].Sequence
	}
	if event.Sequence <= 0 {
		event.Sequence = last + 1
	} else if event.Sequence != last+1 {
		r.mu.Unlock()
		return fmt.Errorf("EVENT_SEQUENCE_CONFLICT")
	}
	r.events[event.AgentRunID] = append(r.events[event.AgentRunID], event)
	if idempotencyKey != "" {
		r.eventIdempotency[event.AgentRunID+"\x00"+idempotencyKey] = event
	}
	r.mu.Unlock()
	r.notifyPublicEventSubscribers(event.AgentRunID)
	return nil
}

func stableAgentRunEventID(agentRunID, idempotencyKey string) string {
	sum := sha256.Sum256([]byte(agentRunID + "\x00" + idempotencyKey))
	return "agent_event_idem_" + hex.EncodeToString(sum[:])
}

func agentRunEventPayloadHash(eventType string, payload []byte) string {
	sum := sha256.Sum256(append(append([]byte(eventType+"\x00"), payload...), []byte("\x00{}")...))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func sameAgentRunEvent(existing, candidate AgentRunEvent) bool {
	if existing.AgentRunID != candidate.AgentRunID || existing.EventType != candidate.EventType || existing.Status != candidate.Status {
		return false
	}
	if candidate.Sequence > 0 && existing.Sequence != candidate.Sequence {
		return false
	}
	return agentRunJSON(sanitizeAgentEventMap(existing.SafeData, 0)) == agentRunJSON(sanitizeAgentEventMap(candidate.SafeData, 0))
}

func mapFromAgentRunValue(value any) map[string]any {
	if mapped, ok := value.(map[string]any); ok {
		return sanitizeAgentEventMap(mapped, 0)
	}
	var raw []byte
	switch typed := value.(type) {
	case string:
		raw = []byte(typed)
	case []byte:
		raw = typed
	case json.RawMessage:
		raw = []byte(typed)
	default:
		encoded, err := json.Marshal(value)
		if err != nil {
			return map[string]any{}
		}
		raw = encoded
	}
	var mapped map[string]any
	if err := json.Unmarshal(raw, &mapped); err != nil {
		return map[string]any{}
	}
	return sanitizeAgentEventMap(mapped, 0)
}

// GetPublicEventBounds reads only app-safe retained sequence metadata. It is
// useful for stream lifecycle checks; ListPublicEvents still reads its page
// and bounds in one statement snapshot to prevent a replay race.
func (r *AgentRunRepository) GetPublicEventBounds(ctx context.Context, agentRunID string) (AgentRunEventBounds, error) {
	agentRunID = strings.TrimSpace(agentRunID)
	if agentRunID == "" {
		return AgentRunEventBounds{}, fmt.Errorf("INVALID_ARGUMENT")
	}
	if r.db != nil && !r.db.Disabled && r.db.Pool != nil {
		var bounds AgentRunEventBounds
		var terminal int64
		err := r.db.Pool.QueryRow(ctx, `select coalesce(min(sequence),0),coalesce(max(sequence),0),coalesce(max(sequence) filter (where event_type in ('succeeded','failed','cancelled','timeout','aborted','orphaned') or coalesce(safe_payload->>'status','') in ('succeeded','failed','cancelled','timeout','aborted','orphaned')),0) from runtime_run_events where run_id=$1 and visibility='app_safe'`, agentRunID).Scan(&bounds.OldestAvailableSequence, &bounds.LatestSequence, &terminal)
		if err != nil {
			return AgentRunEventBounds{}, err
		}
		if terminal > 0 {
			bounds.TerminalSequence = &terminal
		}
		return bounds, nil
	}
	r.mu.Lock()
	items := append([]AgentRunEvent(nil), r.events[agentRunID]...)
	r.mu.Unlock()
	oldest, latest, terminal := eventSequenceBounds(items)
	bounds := AgentRunEventBounds{OldestAvailableSequence: oldest, LatestSequence: latest}
	if terminal > 0 {
		bounds.TerminalSequence = &terminal
	}
	return bounds, nil
}

func (r *AgentRunRepository) ListPublicEvents(ctx context.Context, agentRunID string, afterSequence int64, limit int) (AgentRunEventPage, error) {
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}
	if r.db != nil && !r.db.Disabled && r.db.Pool != nil {
		rows, err := r.db.Pool.Query(ctx, `with bounds as (
select coalesce(min(sequence),0) oldest_sequence,
       coalesce(max(sequence),0) latest_sequence,
       coalesce(max(sequence) filter (where event_type in ('succeeded','failed','cancelled','timeout','aborted','orphaned') or coalesce(safe_payload->>'status','') in ('succeeded','failed','cancelled','timeout','aborted','orphaned')),0) terminal_sequence
from runtime_run_events where run_id=$1 and visibility='app_safe'
)
select bounds.oldest_sequence,bounds.latest_sequence,bounds.terminal_sequence,
       coalesce(page.sequence,0),coalesce(page.event_type,''),coalesce(page.status,''),
       coalesce(page.safe_payload,'{}'::jsonb),coalesce(page.occurred_at,to_timestamp(0))
from bounds
left join lateral (
  select sequence,event_type,coalesce(safe_payload->>'status','') status,safe_payload,occurred_at
  from runtime_run_events
  where run_id=$1 and visibility='app_safe' and sequence>$2
  order by sequence asc limit $3
) page on true
order by page.sequence asc nulls last`, agentRunID, afterSequence, limit+1)
		if err != nil {
			return AgentRunEventPage{}, err
		}
		defer rows.Close()
		items := []AgentRunEvent{}
		var oldest, latest, terminal int64
		for rows.Next() {
			var e AgentRunEvent
			var raw []byte
			if err := rows.Scan(&oldest, &latest, &terminal, &e.Sequence, &e.EventType, &e.Status, &raw, &e.CreatedAt); err != nil {
				return AgentRunEventPage{}, err
			}
			if e.Sequence == 0 {
				continue
			}
			e.AgentRunID = agentRunID
			_ = json.Unmarshal(raw, &e.SafeData)
			e.SafeData = sanitizeAgentEventMap(e.SafeData, 0)
			items = append(items, e)
		}
		return pageAgentEvents(items, afterSequence, limit, oldest, latest, terminal), rows.Err()
	}
	r.mu.Lock()
	stored := append([]AgentRunEvent(nil), r.events[agentRunID]...)
	r.mu.Unlock()
	oldest, latest, terminal := eventSequenceBounds(stored)
	if retainedAgentEventGap(afterSequence, oldest) {
		return pageAgentEvents(nil, afterSequence, limit, oldest, latest, terminal), nil
	}
	items := []AgentRunEvent{}
	for _, event := range stored {
		if event.Sequence > afterSequence {
			items = append(items, event)
		}
	}
	return pageAgentEvents(items, afterSequence, limit, oldest, latest, terminal), nil
}

func (r *AgentRunRepository) SubscribePublicEvents(agentRunID string) (<-chan struct{}, func()) {
	return r.eventNotifier.Subscribe(agentRunID)
}

func (r *AgentRunRepository) ActivePublicEventSubscriptions(agentRunID string) int {
	return r.eventNotifier.ActiveSubscriptions(agentRunID)
}

func (r *AgentRunRepository) EventNotifierHealth(ctx context.Context) (AgentRunEventNotifierHealth, error) {
	if r == nil || r.eventNotifier == nil {
		return AgentRunEventNotifierHealth{Backend: "unavailable", Status: "unavailable"}, fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
	}
	provider, ok := r.eventNotifier.(agentRunEventNotifierHealthProvider)
	if !ok {
		return AgentRunEventNotifierHealth{Backend: "local_test_only", Status: "test_only", OK: true}, nil
	}
	return provider.Health(ctx)
}

func (r *AgentRunRepository) EventNotifierMetrics() AgentRunEventNotifierMetrics {
	if r == nil || r.eventNotifier == nil {
		return AgentRunEventNotifierMetrics{Backend: "unavailable"}
	}
	provider, ok := r.eventNotifier.(agentRunEventNotifierHealthProvider)
	if !ok {
		return AgentRunEventNotifierMetrics{Backend: "local_test_only", ActiveSubscriptions: r.eventNotifier.ActiveSubscriptions("")}
	}
	return provider.Metrics()
}

func (n *localAgentRunEventNotifier) Subscribe(agentRunID string) (<-chan struct{}, func()) {
	wakeup := make(chan struct{}, 1)
	n.mu.Lock()
	n.nextSubscriberID++
	id := n.nextSubscriberID
	if n.subscribers[agentRunID] == nil {
		n.subscribers[agentRunID] = map[uint64]chan struct{}{}
	}
	n.subscribers[agentRunID][id] = wakeup
	n.mu.Unlock()

	var once sync.Once
	cancel := func() {
		once.Do(func() {
			n.mu.Lock()
			delete(n.subscribers[agentRunID], id)
			if len(n.subscribers[agentRunID]) == 0 {
				delete(n.subscribers, agentRunID)
			}
			n.mu.Unlock()
		})
	}
	return wakeup, cancel
}

func (n *localAgentRunEventNotifier) ActiveSubscriptions(agentRunID string) int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return len(n.subscribers[agentRunID])
}

func (n *localAgentRunEventNotifier) Notify(agentRunID string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, wakeup := range n.subscribers[agentRunID] {
		select {
		case wakeup <- struct{}{}:
		default:
		}
	}
}

func (r *AgentRunRepository) PrunePublicEventsBefore(ctx context.Context, agentRunID string, oldestToKeep int64) error {
	if oldestToKeep <= 1 {
		return nil
	}
	if r.db != nil && !r.db.Disabled && r.db.Pool != nil {
		if _, err := r.db.Pool.Exec(ctx, `delete from runtime_run_events where run_id=$1 and visibility='app_safe' and sequence<$2`, agentRunID, oldestToKeep); err != nil {
			return err
		}
	} else {
		r.mu.Lock()
		items := r.events[agentRunID]
		kept := make([]AgentRunEvent, 0, len(items))
		for _, event := range items {
			if event.Sequence >= oldestToKeep {
				kept = append(kept, event)
			}
		}
		r.events[agentRunID] = kept
		r.mu.Unlock()
	}
	r.notifyPublicEventSubscribers(agentRunID)
	return nil
}

func (r *AgentRunRepository) notifyPublicEventSubscribers(agentRunID string) {
	r.eventNotifier.Notify(agentRunID)
}

func (r *AgentRunRepository) UpdateStatusVersioned(ctx context.Context, agentRunID string, expected []string, next string, patch map[string]any) error {
	if r.db != nil && !r.db.Disabled && r.db.Pool != nil {
		result, err := r.db.Pool.Exec(ctx, `update agent_runs set status=$2, public_result=case when $3::jsonb='{}'::jsonb then public_result else $3::jsonb end, error_summary=case when $4::jsonb='{}'::jsonb then error_summary else $4::jsonb end, updated_at=now() where agent_run_id=$1 and status=any($5)`, agentRunID, next, agentRunJSON(patch["publicResult"]), agentRunJSON(patch["errorSummary"]), expected)
		if err != nil {
			return err
		}
		if result.RowsAffected() != 1 {
			return fmt.Errorf("AGENT_PLAN_EXPIRED")
		}
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	record, ok := r.runs[agentRunID]
	if !ok || !stringIn(record.Status, expected) {
		return fmt.Errorf("AGENT_PLAN_EXPIRED")
	}
	record.Status = next
	record.UpdatedAt = time.Now().UTC()
	if v, ok := patch["publicResult"].(map[string]any); ok {
		record.PublicResult = copyMap(v)
	}
	if v, ok := patch["errorSummary"].(map[string]any); ok {
		record.ErrorSummary = copyMap(v)
	}
	r.runs[agentRunID] = record
	return nil
}

// ResolveThreadBinding resolves a Thread's active Workspace only when the
// caller's tenant and user both own that Workspace. chat_threads has no
// tenant_id of its own, so workspaces is the durable tenant ownership fact.
func (r *AgentRunRepository) ResolveThreadBinding(ctx context.Context, tenantID, userID, threadID string) (ThreadWorkspaceBinding, error) {
	tenantID = strings.TrimSpace(tenantID)
	userID = strings.TrimSpace(userID)
	threadID = strings.TrimSpace(threadID)
	if tenantID == "" || userID == "" || threadID == "" {
		return ThreadWorkspaceBinding{}, fmt.Errorf("NOT_FOUND")
	}
	if r.db != nil && !r.db.Disabled {
		if r.db.Pool == nil {
			return ThreadWorkspaceBinding{}, fmt.Errorf("WORKSPACE_WRITE_STORE_UNAVAILABLE")
		}
		var b ThreadWorkspaceBinding
		err := r.db.Pool.QueryRow(ctx, `
select t.thread_id, w.tenant_id, w.user_id,
       coalesce(t.active_workspace_id, t.workspace_id),
       t.workspace_binding_version, t.context_generation, t.updated_at
from chat_threads t
join workspaces w on w.workspace_id = coalesce(t.active_workspace_id, t.workspace_id)
where t.thread_id = $1
  and t.user_id = $3
  and w.tenant_id = $2
  and w.user_id = $3`, threadID, tenantID, userID).Scan(&b.ThreadID, &b.TenantID, &b.UserID, &b.ActiveWorkspaceID, &b.BindingVersion, &b.ContextGeneration, &b.SwitchedAt)
		if errors.Is(err, pgx.ErrNoRows) {
			return ThreadWorkspaceBinding{}, fmt.Errorf("NOT_FOUND")
		}
		return b, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	b, ok := r.threadBindings[threadID]
	if !ok || b.TenantID != tenantID || b.UserID != userID {
		return ThreadWorkspaceBinding{}, fmt.Errorf("NOT_FOUND")
	}
	return b, nil
}

// SeedThreadBinding exists only for the explicit no-database test mirror.
// A configured durable store must never silently establish a memory-only
// binding when the durable binding is missing or unavailable.
// It cannot establish or overwrite another tenant/user's binding.
func (r *AgentRunRepository) SeedThreadBinding(binding ThreadWorkspaceBinding) error {
	binding.TenantID = strings.TrimSpace(binding.TenantID)
	binding.UserID = strings.TrimSpace(binding.UserID)
	binding.ThreadID = strings.TrimSpace(binding.ThreadID)
	binding.ActiveWorkspaceID = strings.TrimSpace(binding.ActiveWorkspaceID)
	if binding.TenantID == "" || binding.UserID == "" || binding.ThreadID == "" || binding.ActiveWorkspaceID == "" || binding.BindingVersion < 1 || binding.ContextGeneration < 1 {
		return fmt.Errorf("INVALID_ARGUMENT")
	}
	if r.db != nil && !r.db.Disabled {
		return fmt.Errorf("WORKSPACE_WRITE_STORE_UNAVAILABLE")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing, ok := r.threadBindings[binding.ThreadID]; ok {
		// Older local test fixtures may not have carried the new identity facts.
		// They can be repaired only after the service has validated the durable
		// Workspace owner; a scoped binding itself is never overwritten.
		if existing.TenantID == "" && existing.UserID == "" {
			r.threadBindings[binding.ThreadID] = binding
			return nil
		}
		if existing.TenantID != binding.TenantID || existing.UserID != binding.UserID {
			return fmt.Errorf("NOT_FOUND")
		}
		return nil
	}
	r.threadBindings[binding.ThreadID] = binding
	return nil
}

func (r *AgentRunRepository) SwitchThreadWorkspace(ctx context.Context, tenantID, userID, threadID, targetWorkspaceID string, expectedVersion int64, idempotencyKey string) (ThreadWorkspaceBinding, error) {
	command, err := normalizeThreadWorkspaceSwitchCommand(threadWorkspaceSwitchCommand{
		TenantID: tenantID, UserID: userID, ThreadID: threadID, TargetWorkspaceID: targetWorkspaceID,
		ExpectedBindingVersion: expectedVersion, IdempotencyKey: idempotencyKey,
	})
	if err != nil {
		return ThreadWorkspaceBinding{}, err
	}
	if r.db != nil && !r.db.Disabled {
		// A configured Product database must never use the legacy unfenced
		// mutation. The service first acquires exact Redis/Tair Workspace leases
		// and then calls SwitchThreadWorkspaceFenced.
		return ThreadWorkspaceBinding{}, fmt.Errorf("DISTRIBUTED_LOCK_UNAVAILABLE")
	}
	tenantID = command.TenantID
	userID = command.UserID
	threadID = command.ThreadID
	targetWorkspaceID = command.TargetWorkspaceID
	expectedVersion = command.ExpectedBindingVersion
	idempotencyKey = command.IdempotencyKey
	r.mu.Lock()
	defer r.mu.Unlock()
	key := threadWorkspaceSwitchIdempotencyMapKey(threadID, idempotencyKey)
	if record, ok := r.threadSwitchKeys[key]; ok {
		if record.TenantID != tenantID || record.UserID != userID || record.ThreadID != threadID {
			return ThreadWorkspaceBinding{}, fmt.Errorf("NOT_FOUND")
		}
		if record.TargetWorkspaceID != targetWorkspaceID || record.ExpectedBindingVersion != expectedVersion {
			return ThreadWorkspaceBinding{}, fmt.Errorf("IDEMPOTENCY_KEY_CONFLICT")
		}
		binding := record.Binding
		binding.Replayed = true
		return binding, nil
	}
	b, ok := r.threadBindings[threadID]
	if !ok || b.TenantID != tenantID || b.UserID != userID {
		return ThreadWorkspaceBinding{}, fmt.Errorf("NOT_FOUND")
	}
	if b.BindingVersion != expectedVersion {
		return b, fmt.Errorf("THREAD_WORKSPACE_VERSION_CONFLICT")
	}
	if b.ActiveWorkspaceID == targetWorkspaceID {
		// A no-op still creates a durable semantic idempotency response. Freeze
		// its timestamp before recording it so every exact replay returns the
		// same result instead of manufacturing a new response time in the
		// service layer.
		if b.SwitchedAt.IsZero() {
			b.SwitchedAt = time.Now().UTC()
		}
		r.threadSwitchKeys[key] = threadWorkspaceSwitchIdempotencyRecord{
			TenantID: tenantID, UserID: userID, ThreadID: threadID,
			TargetWorkspaceID: targetWorkspaceID, ExpectedBindingVersion: expectedVersion, Binding: b,
		}
		return b, nil
	}
	b.PreviousWorkspaceID = b.ActiveWorkspaceID
	b.ActiveWorkspaceID = targetWorkspaceID
	b.BindingVersion++
	b.ContextGeneration++
	b.SwitchedAt = time.Now().UTC()
	r.threadBindings[threadID] = b
	r.threadHistory[threadID] = append(r.threadHistory[threadID], b)
	r.threadSwitchKeys[key] = threadWorkspaceSwitchIdempotencyRecord{
		TenantID: tenantID, UserID: userID, ThreadID: threadID,
		TargetWorkspaceID: targetWorkspaceID, ExpectedBindingVersion: expectedVersion, Binding: b,
	}
	return b, nil
}

// FindThreadWorkspaceSwitchReplay reads a Thread-local semantic replay record
// without opening a new mutation transaction. It lets the service return an
// already committed response even when a later Workspace lifecycle change
// would make that historical target unavailable for a fresh switch.
func (r *AgentRunRepository) FindThreadWorkspaceSwitchReplay(ctx context.Context, tenantID, userID, threadID, targetWorkspaceID string, expectedVersion int64, idempotencyKey string) (ThreadWorkspaceBinding, bool, error) {
	command, err := normalizeThreadWorkspaceSwitchCommand(threadWorkspaceSwitchCommand{
		TenantID: tenantID, UserID: userID, ThreadID: threadID, TargetWorkspaceID: targetWorkspaceID,
		ExpectedBindingVersion: expectedVersion, IdempotencyKey: idempotencyKey,
	})
	if err != nil {
		return ThreadWorkspaceBinding{}, false, err
	}
	if r != nil && r.db != nil && !r.db.Disabled {
		if r.db.Pool == nil {
			return ThreadWorkspaceBinding{}, false, fmt.Errorf("WORKSPACE_WRITE_STORE_UNAVAILABLE")
		}
		rows, queryErr := r.db.Query(ctx, `
select tenant_id, user_id, target_workspace_id, expected_binding_version,
       previous_workspace_id, active_workspace_id, binding_version,
       context_generation, switched_at
from thread_workspace_switch_idempotency
where thread_id = @thread
  and idempotency_key = @key`, map[string]any{"thread": command.ThreadID, "key": command.IdempotencyKey})
		if queryErr != nil {
			return ThreadWorkspaceBinding{}, false, queryErr
		}
		if len(rows) == 0 {
			return ThreadWorkspaceBinding{}, false, nil
		}
		if len(rows) != 1 {
			return ThreadWorkspaceBinding{}, false, fmt.Errorf("IDEMPOTENCY_KEY_CONFLICT")
		}
		record := rows[0]
		if stringOr(record["tenant_id"], "") != command.TenantID || stringOr(record["user_id"], "") != command.UserID {
			return ThreadWorkspaceBinding{}, false, fmt.Errorf("NOT_FOUND")
		}
		if stringOr(record["target_workspace_id"], "") != command.TargetWorkspaceID || int64Value(record["expected_binding_version"]) != command.ExpectedBindingVersion {
			return ThreadWorkspaceBinding{}, false, fmt.Errorf("IDEMPOTENCY_KEY_CONFLICT")
		}
		return ThreadWorkspaceBinding{
			TenantID:            command.TenantID,
			UserID:              command.UserID,
			ThreadID:            command.ThreadID,
			PreviousWorkspaceID: stringOr(record["previous_workspace_id"], ""),
			ActiveWorkspaceID:   stringOr(record["active_workspace_id"], ""),
			BindingVersion:      int64Value(record["binding_version"]),
			ContextGeneration:   int64Value(record["context_generation"]),
			SwitchedAt:          timeValue(record["switched_at"], time.Time{}).UTC(),
			Replayed:            true,
		}, true, nil
	}
	if r == nil {
		return ThreadWorkspaceBinding{}, false, fmt.Errorf("NOT_FOUND")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	record, found := r.threadSwitchKeys[threadWorkspaceSwitchIdempotencyMapKey(command.ThreadID, command.IdempotencyKey)]
	if !found {
		return ThreadWorkspaceBinding{}, false, nil
	}
	if record.TenantID != command.TenantID || record.UserID != command.UserID || record.ThreadID != command.ThreadID {
		return ThreadWorkspaceBinding{}, false, fmt.Errorf("NOT_FOUND")
	}
	if record.TargetWorkspaceID != command.TargetWorkspaceID || record.ExpectedBindingVersion != command.ExpectedBindingVersion {
		return ThreadWorkspaceBinding{}, false, fmt.Errorf("IDEMPOTENCY_KEY_CONFLICT")
	}
	binding := record.Binding
	binding.Replayed = true
	return binding, true, nil
}

func threadWorkspaceSwitchIdempotencyMapKey(threadID, idempotencyKey string) string {
	return threadID + "\x00" + idempotencyKey
}

// ListThreadWorkspaceHistory returns only durable binding transitions. It is
// intentionally an internal read model; callers establish tenant and user
// ownership before exposing any result.
func (r *AgentRunRepository) ListThreadWorkspaceHistory(ctx context.Context, tenantID, userID, threadID, cursor string, limit int) ([]ThreadWorkspaceBinding, string, error) {
	if strings.TrimSpace(tenantID) == "" || strings.TrimSpace(userID) == "" || strings.TrimSpace(threadID) == "" {
		return nil, "", fmt.Errorf("INVALID_ARGUMENT")
	}
	beforeVersion, err := parseThreadWorkspaceHistoryCursor(cursor)
	if err != nil {
		return nil, "", err
	}
	if limit == 0 {
		limit = 50
	}
	if limit < 1 || limit > 100 {
		return nil, "", fmt.Errorf("INVALID_ARGUMENT")
	}
	if r.db != nil && !r.db.Disabled {
		if r.db.Pool == nil {
			return nil, "", fmt.Errorf("WORKSPACE_WRITE_STORE_UNAVAILABLE")
		}
		return r.listThreadWorkspaceHistoryPostgres(ctx, tenantID, userID, threadID, beforeVersion, limit)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	binding, ok := r.threadBindings[threadID]
	if !ok || binding.TenantID != tenantID || binding.UserID != userID {
		return nil, "", fmt.Errorf("NOT_FOUND")
	}
	history := r.threadHistory[threadID]
	items := make([]ThreadWorkspaceBinding, 0, limit+1)
	for index := len(history) - 1; index >= 0; index-- {
		item := history[index]
		if beforeVersion > 0 && item.BindingVersion >= beforeVersion {
			continue
		}
		items = append(items, item)
		if len(items) > limit {
			break
		}
	}
	return pageThreadWorkspaceHistory(items, limit)
}

func (r *AgentRunRepository) listThreadWorkspaceHistoryPostgres(ctx context.Context, tenantID, userID, threadID string, beforeVersion int64, limit int) ([]ThreadWorkspaceBinding, string, error) {
	rows, err := r.db.Pool.Query(ctx, `
select thread_id, tenant_id, user_id, coalesce(previous_workspace_id, ''), workspace_id, binding_version, context_generation, created_at
from chat_thread_workspace_history
where tenant_id = $1
  and user_id = $2
  and thread_id = $3
  and ($4 = 0 or binding_version < $4)
order by binding_version desc
limit $5`, tenantID, userID, threadID, beforeVersion, limit+1)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	items := make([]ThreadWorkspaceBinding, 0, limit+1)
	for rows.Next() {
		var item ThreadWorkspaceBinding
		if err := rows.Scan(&item.ThreadID, &item.TenantID, &item.UserID, &item.PreviousWorkspaceID, &item.ActiveWorkspaceID, &item.BindingVersion, &item.ContextGeneration, &item.SwitchedAt); err != nil {
			return nil, "", err
		}
		item.SwitchedAt = item.SwitchedAt.UTC()
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	return pageThreadWorkspaceHistory(items, limit)
}

func parseThreadWorkspaceHistoryCursor(cursor string) (int64, error) {
	cursor = strings.TrimSpace(cursor)
	if cursor == "" {
		return 0, nil
	}
	version, err := strconv.ParseInt(cursor, 10, 64)
	if err != nil || version < 1 {
		return 0, fmt.Errorf("INVALID_ARGUMENT")
	}
	return version, nil
}

func pageThreadWorkspaceHistory(items []ThreadWorkspaceBinding, limit int) ([]ThreadWorkspaceBinding, string, error) {
	if len(items) <= limit {
		return items, "", nil
	}
	items = items[:limit]
	return items, strconv.FormatInt(items[len(items)-1].BindingVersion, 10), nil
}

func (r *AgentRunRepository) createRunPostgres(ctx context.Context, record AgentRunRecord) (AgentRunRecord, bool, error) {
	existing, err := r.findByIdempotencyPostgres(ctx, record.UserID, record.IdempotencyKey)
	if err == nil {
		if !CompareRequestHash(existing.RequestHash, record.RequestHash) {
			return AgentRunRecord{}, true, fmt.Errorf("IDEMPOTENCY_KEY_CONFLICT")
		}
		return existing, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return AgentRunRecord{}, false, err
	}
	err = r.db.WithTx(ctx, func(tx *Tx) error {
		requestSnapshot, _ := json.Marshal(record.RequestSnapshot)
		intentSnapshot, _ := json.Marshal(record.IntentSnapshot)
		if err := tx.Exec(ctx, `insert into agent_runs(agent_run_id,tenant_id,user_id,workspace_id,workspace_version,workspace_binding_version,context_generation,thread_id,task_id,idempotency_key,request_hash,request_snapshot,intent_snapshot,status,routing_mode,source_surface) values(@id,@tenant,@user,@workspace,@workspaceVersion,@bindingVersion,@contextGeneration,nullif(@thread,''),nullif(@task,''),@key,@hash,@request::jsonb,@intent::jsonb,@status,@routing,@surface)`, map[string]any{"id": record.AgentRunID, "tenant": record.TenantID, "user": record.UserID, "workspace": record.WorkspaceID, "workspaceVersion": record.WorkspaceVersion, "bindingVersion": record.BindingVersion, "contextGeneration": record.ContextGeneration, "thread": record.ThreadID, "task": record.TaskID, "key": record.IdempotencyKey, "hash": record.RequestHash, "request": string(requestSnapshot), "intent": string(intentSnapshot), "status": record.Status, "routing": record.RoutingMode, "surface": record.SourceSurface}); err != nil {
			return err
		}
		payload, _ := json.Marshal(map[string]any{"agentRunId": record.AgentRunID, "tenantId": record.TenantID, "userId": record.UserID, "workspaceId": record.WorkspaceID})
		return tx.Exec(ctx, `insert into task_queue_records(queue_id,queue_name,task_type,task_id,dedupe_key,status,priority,payload,attempt_series_id) values(@queue,'agent_planning','agent_planning',@run,@dedupe,'pending',50,@payload::jsonb,default)`, map[string]any{"queue": "queue_plan_" + record.AgentRunID, "run": record.AgentRunID, "dedupe": "agent_planning:" + record.AgentRunID, "payload": string(payload)})
	})
	if err != nil {
		return AgentRunRecord{}, false, err
	}
	created, getErr := r.getRunPostgres(ctx, record.TenantID, record.UserID, record.AgentRunID)
	return created, false, getErr
}

func (r *AgentRunRepository) findByIdempotencyPostgres(ctx context.Context, userID, key string) (AgentRunRecord, error) {
	return r.scanRunRow(r.db.Pool.QueryRow(ctx, agentRunSelect+` where user_id=$1 and idempotency_key=$2`, userID, key))
}
func (r *AgentRunRepository) getRunPostgres(ctx context.Context, tenantID, userID, id string) (AgentRunRecord, error) {
	return r.scanRunRow(r.db.Pool.QueryRow(ctx, agentRunSelect+` where tenant_id=$1 and user_id=$2 and agent_run_id=$3`, tenantID, userID, id))
}

const agentRunSelect = `select agent_run_id,tenant_id,user_id,workspace_id,workspace_version,workspace_binding_version,context_generation,coalesce(thread_id,''),coalesce(task_id,''),idempotency_key,request_hash,coalesce(request_snapshot,'{}'::jsonb),status,routing_mode,coalesce(source_surface,''),coalesce(intent_snapshot,'{}'::jsonb),coalesce(plan_snapshot,'{}'::jsonb),coalesce(public_result,'{}'::jsonb),coalesce(error_summary,'{}'::jsonb),cancel_requested_at,coalesce(cancel_reason_code,''),coalesce(cancel_reason_hash,''),submit_authorized_at,created_at,updated_at from agent_runs`

type agentRunRowScanner interface{ Scan(...any) error }

func (r *AgentRunRepository) scanRunRow(row agentRunRowScanner) (AgentRunRecord, error) {
	var out AgentRunRecord
	var request, intent, plan, result, summary []byte
	err := row.Scan(&out.AgentRunID, &out.TenantID, &out.UserID, &out.WorkspaceID, &out.WorkspaceVersion, &out.BindingVersion, &out.ContextGeneration, &out.ThreadID, &out.TaskID, &out.IdempotencyKey, &out.RequestHash, &request, &out.Status, &out.RoutingMode, &out.SourceSurface, &intent, &plan, &result, &summary, &out.CancelRequestedAt, &out.CancelReasonCode, &out.CancelReasonHash, &out.SubmitAuthorizedAt, &out.CreatedAt, &out.UpdatedAt)
	if err != nil {
		return AgentRunRecord{}, err
	}
	_ = json.Unmarshal(request, &out.RequestSnapshot)
	_ = json.Unmarshal(intent, &out.IntentSnapshot)
	_ = json.Unmarshal(plan, &out.PlanSnapshot)
	_ = json.Unmarshal(result, &out.PublicResult)
	_ = json.Unmarshal(summary, &out.ErrorSummary)
	return out, nil
}

// switchThreadWorkspacePostgresTx is the canonical Thread Workspace mutation.
// A fenced caller supplies beforeApply to validate both Workspace lease proofs
// and durable versions before any binding/history/idempotency write occurs.
func (r *AgentRunRepository) switchThreadWorkspacePostgresTx(ctx context.Context, tx *Tx, command threadWorkspaceSwitchCommand, beforeApply threadWorkspaceSwitchBeforeApply) (ThreadWorkspaceBinding, error) {
	command, err := normalizeThreadWorkspaceSwitchCommand(command)
	if err != nil {
		return ThreadWorkspaceBinding{}, err
	}
	rows, err := tx.Query(ctx, `
select t.thread_id, w.tenant_id, w.user_id,
       coalesce(t.active_workspace_id, t.workspace_id) active_workspace_id,
       t.workspace_binding_version, t.context_generation, t.updated_at
from chat_threads t
join workspaces w on w.workspace_id = coalesce(t.active_workspace_id, t.workspace_id)
where t.thread_id = @thread
  and t.user_id = @user
  and w.tenant_id = @tenant
  and w.user_id = @user
for update of t`, map[string]any{"thread": command.ThreadID, "tenant": command.TenantID, "user": command.UserID})
	if err != nil {
		return ThreadWorkspaceBinding{}, err
	}
	if len(rows) != 1 {
		return ThreadWorkspaceBinding{}, fmt.Errorf("NOT_FOUND")
	}
	current := ThreadWorkspaceBinding{
		TenantID:          stringOr(rows[0]["tenant_id"], ""),
		UserID:            stringOr(rows[0]["user_id"], ""),
		ThreadID:          command.ThreadID,
		ActiveWorkspaceID: stringOr(rows[0]["active_workspace_id"], ""),
		BindingVersion:    int64Value(rows[0]["workspace_binding_version"]),
		ContextGeneration: int64Value(rows[0]["context_generation"]),
		SwitchedAt:        timeValue(rows[0]["updated_at"], time.Time{}).UTC(),
	}
	idempotencyRows, err := tx.Query(ctx, `
select tenant_id, user_id, target_workspace_id, expected_binding_version,
       previous_workspace_id, active_workspace_id, binding_version,
       context_generation, switched_at
from thread_workspace_switch_idempotency
where thread_id = @thread
  and idempotency_key = @key
for update`, map[string]any{"thread": command.ThreadID, "key": command.IdempotencyKey})
	if err != nil {
		return ThreadWorkspaceBinding{}, err
	}
	if len(idempotencyRows) > 1 {
		return ThreadWorkspaceBinding{}, fmt.Errorf("IDEMPOTENCY_KEY_CONFLICT")
	}
	if len(idempotencyRows) == 1 {
		record := idempotencyRows[0]
		if stringOr(record["tenant_id"], "") != command.TenantID || stringOr(record["user_id"], "") != command.UserID {
			return ThreadWorkspaceBinding{}, fmt.Errorf("NOT_FOUND")
		}
		if stringOr(record["target_workspace_id"], "") != command.TargetWorkspaceID || int64Value(record["expected_binding_version"]) != command.ExpectedBindingVersion {
			return ThreadWorkspaceBinding{}, fmt.Errorf("IDEMPOTENCY_KEY_CONFLICT")
		}
		return ThreadWorkspaceBinding{
			TenantID:            command.TenantID,
			UserID:              command.UserID,
			ThreadID:            command.ThreadID,
			PreviousWorkspaceID: stringOr(record["previous_workspace_id"], ""),
			ActiveWorkspaceID:   stringOr(record["active_workspace_id"], ""),
			BindingVersion:      int64Value(record["binding_version"]),
			ContextGeneration:   int64Value(record["context_generation"]),
			SwitchedAt:          timeValue(record["switched_at"], time.Time{}).UTC(),
			Replayed:            true,
		}, nil
	}
	if current.BindingVersion != command.ExpectedBindingVersion {
		return current, fmt.Errorf("THREAD_WORKSPACE_VERSION_CONFLICT")
	}
	if current.ActiveWorkspaceID == command.TargetWorkspaceID {
		if beforeApply != nil {
			if err := beforeApply(current); err != nil {
				return current, err
			}
		}
		current.SwitchedAt = time.Now().UTC()
		return current, insertThreadWorkspaceSwitchIdempotency(ctx, tx, current, command.TargetWorkspaceID, command.ExpectedBindingVersion, command.IdempotencyKey)
	}
	workspaceRows, err := tx.Query(ctx, `select workspace_id from workspaces where workspace_id=@workspace and tenant_id=@tenant and user_id=@user and status='ready'`, map[string]any{"workspace": command.TargetWorkspaceID, "tenant": command.TenantID, "user": command.UserID})
	if err != nil {
		return ThreadWorkspaceBinding{}, err
	}
	if len(workspaceRows) != 1 {
		return ThreadWorkspaceBinding{}, fmt.Errorf("WORKSPACE_NOT_READY")
	}
	if beforeApply != nil {
		if err := beforeApply(current); err != nil {
			return current, err
		}
	}
	out := ThreadWorkspaceBinding{TenantID: command.TenantID, UserID: command.UserID, ThreadID: command.ThreadID, PreviousWorkspaceID: current.ActiveWorkspaceID, ActiveWorkspaceID: command.TargetWorkspaceID, BindingVersion: current.BindingVersion + 1, ContextGeneration: current.ContextGeneration + 1, SwitchedAt: time.Now().UTC()}
	if err := tx.Exec(ctx, `update chat_threads set active_workspace_id=@workspace,workspace_binding_version=@version,context_generation=@generation,updated_at=@switchedAt where thread_id=@thread and user_id=@user`, map[string]any{"workspace": command.TargetWorkspaceID, "version": out.BindingVersion, "generation": out.ContextGeneration, "switchedAt": out.SwitchedAt, "thread": command.ThreadID, "user": command.UserID}); err != nil {
		return ThreadWorkspaceBinding{}, err
	}
	if err := tx.Exec(ctx, `insert into chat_thread_workspace_history(history_id,thread_id,tenant_id,user_id,previous_workspace_id,workspace_id,binding_version,context_generation,idempotency_key,reason,changed_by_type,changed_by_id) values(@id,@thread,@tenant,@user,@previous,@workspace,@version,@generation,@key,'user_workspace_switch','user',@user)`, map[string]any{"id": fmt.Sprintf("thread_workspace_%s_%d", command.ThreadID, out.BindingVersion), "thread": command.ThreadID, "tenant": command.TenantID, "user": command.UserID, "previous": out.PreviousWorkspaceID, "workspace": command.TargetWorkspaceID, "version": out.BindingVersion, "generation": out.ContextGeneration, "key": command.IdempotencyKey}); err != nil {
		return ThreadWorkspaceBinding{}, err
	}
	if err := insertThreadWorkspaceSwitchIdempotency(ctx, tx, out, command.TargetWorkspaceID, command.ExpectedBindingVersion, command.IdempotencyKey); err != nil {
		return ThreadWorkspaceBinding{}, err
	}
	return out, nil
}

func normalizeThreadWorkspaceSwitchCommand(command threadWorkspaceSwitchCommand) (threadWorkspaceSwitchCommand, error) {
	command.TenantID = strings.TrimSpace(command.TenantID)
	command.UserID = strings.TrimSpace(command.UserID)
	command.ThreadID = strings.TrimSpace(command.ThreadID)
	command.TargetWorkspaceID = strings.TrimSpace(command.TargetWorkspaceID)
	command.IdempotencyKey = strings.TrimSpace(command.IdempotencyKey)
	if command.TenantID == "" || command.UserID == "" || command.ThreadID == "" || command.TargetWorkspaceID == "" || command.IdempotencyKey == "" || command.ExpectedBindingVersion < 1 {
		return threadWorkspaceSwitchCommand{}, fmt.Errorf("INVALID_ARGUMENT")
	}
	return command, nil
}

func insertThreadWorkspaceSwitchIdempotency(ctx context.Context, tx *Tx, binding ThreadWorkspaceBinding, targetWorkspaceID string, expectedBindingVersion int64, idempotencyKey string) error {
	if binding.SwitchedAt.IsZero() {
		binding.SwitchedAt = time.Now().UTC()
	}
	return tx.Exec(ctx, `
insert into thread_workspace_switch_idempotency (
  thread_id, idempotency_key, tenant_id, user_id, target_workspace_id,
  expected_binding_version, previous_workspace_id, active_workspace_id,
  binding_version, context_generation, switched_at
) values (
  @thread, @key, @tenant, @user, @target,
  @expected, @previous, @active,
  @version, @generation, @switchedAt
	)`, map[string]any{
		"thread":     binding.ThreadID,
		"key":        idempotencyKey,
		"tenant":     binding.TenantID,
		"user":       binding.UserID,
		"target":     targetWorkspaceID,
		"expected":   expectedBindingVersion,
		"previous":   nullableThreadWorkspaceID(binding.PreviousWorkspaceID),
		"active":     binding.ActiveWorkspaceID,
		"version":    binding.BindingVersion,
		"generation": binding.ContextGeneration,
		"switchedAt": binding.SwitchedAt,
	})
}

func nullableThreadWorkspaceID(workspaceID string) any {
	if workspaceID == "" {
		return nil
	}
	return workspaceID
}

func pageAgentEvents(items []AgentRunEvent, afterSequence int64, limit int, oldest, latest, terminal int64) AgentRunEventPage {
	page := AgentRunEventPage{
		Items:                   items,
		NextAfterSequence:       afterSequence,
		OldestAvailableSequence: oldest,
		LatestSequence:          latest,
		Gap:                     retainedAgentEventGap(afterSequence, oldest),
	}
	if terminal > 0 {
		page.TerminalSequence = &terminal
	}
	if len(page.Items) > limit {
		page.Items = page.Items[:limit]
		page.HasMore = true
	}
	if len(page.Items) > 0 {
		page.NextAfterSequence = page.Items[len(page.Items)-1].Sequence
	}
	if page.NextAfterSequence < latest && !page.Gap {
		page.HasMore = true
	}
	return page
}

func retainedAgentEventGap(afterSequence, oldest int64) bool {
	return oldest > 1 && afterSequence < oldest-1
}

func eventSequenceBounds(items []AgentRunEvent) (oldest, latest, terminal int64) {
	if len(items) == 0 {
		return 0, 0, 0
	}
	oldest = items[0].Sequence
	latest = items[len(items)-1].Sequence
	for _, event := range items {
		if terminalAgentRunEvent(event) {
			terminal = event.Sequence
		}
	}
	return oldest, latest, terminal
}

func terminalAgentRunEvent(event AgentRunEvent) bool {
	return stringIn(event.EventType, []string{"succeeded", "failed", "cancelled", "timeout", "aborted", "orphaned"}) ||
		stringIn(event.Status, []string{"succeeded", "failed", "cancelled", "timeout", "aborted", "orphaned"})
}
func copyAgentRun(in AgentRunRecord) AgentRunRecord {
	in.RequestSnapshot = copyMap(in.RequestSnapshot)
	in.IntentSnapshot = copyMap(in.IntentSnapshot)
	in.PlanSnapshot = copyMap(in.PlanSnapshot)
	in.ExecutionIdentity = copyMap(in.ExecutionIdentity)
	in.Routing = copyMap(in.Routing)
	in.Clarification = copyMap(in.Clarification)
	in.PublicResult = copyMap(in.PublicResult)
	in.ErrorSummary = copyMap(in.ErrorSummary)
	in.CancelRequestedAt = copyAgentRunTime(in.CancelRequestedAt)
	in.SubmitAuthorizedAt = copyAgentRunTime(in.SubmitAuthorizedAt)
	return in
}

func copyAgentRunTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}
func agentRunJSON(v any) string {
	if v == nil {
		return "{}"
	}
	raw, _ := json.Marshal(v)
	return string(raw)
}
func int64Value(v any) int64 {
	switch n := v.(type) {
	case int:
		return int64(n)
	case int64:
		return n
	case float64:
		return int64(n)
	case string:
		var out int64
		_, _ = fmt.Sscan(n, &out)
		return out
	}
	return 0
}
func stringIn(value string, values []string) bool {
	for _, item := range values {
		if item == value {
			return true
		}
	}
	return false
}

func validAgentPlanStatus(value string) bool {
	return stringIn(value, []string{"draft", "validated", "awaiting_confirmation", "confirmed", "executing", "succeeded", "failed", "cancelled"})
}

func validAgentRunStatus(value string) bool {
	return stringIn(value, []string{"created", "resolving_intent", "planning", "awaiting_confirmation", "admission_pending", "queued", "reserving", "dispatched", "accepted", "materializing", "running", "finalizing", "aborting", "succeeded", "failed", "cancelled", "timeout", "orphaned"})
}

func terminalAgentRunStatus(value string) bool {
	return stringIn(value, []string{"succeeded", "failed", "cancelled", "timeout", "aborted", "orphaned"})
}

func activeAgentRunOwnershipStatus(value string) bool {
	return stringIn(value, []string{"reserving", "dispatched", "accepted", "materializing", "running", "finalizing", "aborting", "orphaned"})
}

func validAgentRunCancelReason(value string) bool {
	return stringIn(value, []string{"USER_CANCELLED", "TIMEOUT", "BUDGET_EXCEEDED", "LEASE_LOST"})
}

func validAgentRunReasonHash(value string) bool {
	if len(value) != len("sha256:")+64 || !strings.HasPrefix(value, "sha256:") {
		return false
	}
	for _, char := range value[len("sha256:"):] {
		if !((char >= '0' && char <= '9') || (char >= 'a' && char <= 'f')) {
			return false
		}
	}
	return true
}

func sanitizeAgentEventMap(value map[string]any, depth int) map[string]any {
	if value == nil || depth > 6 {
		return map[string]any{}
	}
	out := map[string]any{}
	for key, item := range value {
		if forbiddenAgentEventKey(key) {
			continue
		}
		if safe, ok := sanitizeAgentEventValue(item, depth+1); ok {
			out[key] = safe
		}
	}
	return out
}

func sanitizeAgentEventValue(value any, depth int) (any, bool) {
	if depth > 6 {
		return nil, false
	}
	switch typed := value.(type) {
	case map[string]any:
		return sanitizeAgentEventMap(typed, depth), true
	case []map[string]any:
		items := make([]any, 0, len(typed))
		for _, child := range typed {
			items = append(items, sanitizeAgentEventMap(child, depth))
		}
		return items, true
	case []any:
		items := make([]any, 0, len(typed))
		for _, child := range typed {
			if safe, ok := sanitizeAgentEventValue(child, depth); ok {
				items = append(items, safe)
			}
		}
		return items, true
	default:
		return value, true
	}
}

func forbiddenAgentEventKey(key string) bool {
	normalized := strings.NewReplacer("_", "", "-", "").Replace(strings.ToLower(strings.TrimSpace(key)))
	switch normalized {
	case "content", "text", "snippet", "summary", "evidence", "excerpt", "embedding", "prompt",
		"sessionkey", "openclawsessionkey", "realpath", "rootpath", "providerbody", "toolargs", "args", "query",
		"rawplan", "hostendpoint", "runtimehostid", "runtimeinstanceid", "reservationid", "dispatchid", "runticket",
		"jti", "leasetoken", "leasetokenhash", "fencingtoken", "authpoolid", "errorstack", "stacktrace":
		return true
	default:
		return false
	}
}
