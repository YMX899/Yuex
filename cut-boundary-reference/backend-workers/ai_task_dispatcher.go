package workers

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"huahuoai/backend/source/internal/domain"
	"huahuoai/backend/source/internal/persistence"
	storageprovider "huahuoai/backend/source/internal/providers/storage"
	"huahuoai/backend/source/internal/queue"
	runtimepkg "huahuoai/backend/source/internal/runtime"
	"huahuoai/backend/source/internal/services"
	workspacepkg "huahuoai/backend/source/internal/workspace"
)

type RuntimeManifestProvider interface {
	Build(ctx context.Context, run persistence.AgentRunRecord, plan runtimepkg.AgentRunPlan, frozen domain.RunWorkspaceContextRecord, host runtimepkg.RuntimeHost) (runtimepkg.RuntimeManifestBuildInputs, error)
}

type RuntimeCapacityCommandProvider interface {
	Resolve(ctx context.Context, run persistence.AgentRunRecord, plan runtimepkg.AgentRunPlan) (runtimepkg.RuntimeCapacityCommand, error)
}

type RuntimeCapacityCommandProviderFunc func(context.Context, persistence.AgentRunRecord, runtimepkg.AgentRunPlan) (runtimepkg.RuntimeCapacityCommand, error)

func (f RuntimeCapacityCommandProviderFunc) Resolve(ctx context.Context, run persistence.AgentRunRecord, plan runtimepkg.AgentRunPlan) (runtimepkg.RuntimeCapacityCommand, error) {
	return f(ctx, run, plan)
}

type RepositoryRuntimeCapacityProvider struct {
	Repos       *persistence.Repositories
	Environment string
}

type runtimeCapacitySnapshot struct {
	SnapshotVersion int64
	ModelLimit      int
	AuthPoolLimit   int
	ToolLimit       int
	TenantLimit     int
	UserLimit       int
	AuthPoolID      string
}

const (
	runtimeCapacityRetryInitialDelay = 2 * time.Second
	runtimeCapacityRetryMaximumDelay = 30 * time.Second
)

func (p RepositoryRuntimeCapacityProvider) Resolve(ctx context.Context, run persistence.AgentRunRecord, plan runtimepkg.AgentRunPlan) (runtimepkg.RuntimeCapacityCommand, error) {
	if p.Repos == nil || p.Repos.Config == nil {
		return runtimepkg.RuntimeCapacityCommand{}, fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
	}
	environment := strings.TrimSpace(p.Environment)
	if environment == "" {
		environment = "local"
	}
	record, err := p.Repos.Config.GetRequiredSystemConfig(ctx, "runtime_capacity_v1", environment)
	if err != nil {
		return runtimepkg.RuntimeCapacityCommand{}, fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
	}
	snapshot, err := parseRuntimeCapacitySnapshot(record, environment)
	if err != nil {
		return runtimepkg.RuntimeCapacityCommand{}, fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
	}
	runtimeConfigID, runtimeConfigErr := runtimeConfigIDForPlan(plan)
	if runtimeConfigErr != nil {
		return runtimepkg.RuntimeCapacityCommand{}, runtimeConfigErr
	}
	if runtimeConfigID == "" || run.TenantID == "" || run.UserID == "" {
		return runtimepkg.RuntimeCapacityCommand{}, fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
	}
	toolKey := "tools:none"
	if len(plan.RequiredTools) > 0 {
		tools := append([]string{}, plan.RequiredTools...)
		sort.Strings(tools)
		toolKey = "tools:" + dispatcherSHA256([]byte(strings.Join(tools, "\x00")))
	}
	dimension := func(key string, limit int) runtimepkg.RuntimeCapacityDimension {
		return runtimepkg.RuntimeCapacityDimension{Key: key, Limit: limit, Requested: 1, Version: snapshot.SnapshotVersion}
	}
	return runtimepkg.RuntimeCapacityCommand{
		RunID: run.AgentRunID, SnapshotVersion: snapshot.SnapshotVersion, TTL: 2 * time.Minute,
		Dimensions: runtimepkg.RuntimeCapacityDimensions{
			Model: dimension("model:"+runtimeConfigID, snapshot.ModelLimit), AuthPool: dimension("auth:"+snapshot.AuthPoolID, snapshot.AuthPoolLimit),
			Tool: dimension(toolKey, snapshot.ToolLimit), Tenant: dimension("tenant:"+run.TenantID, snapshot.TenantLimit),
			User: dimension("user:"+run.TenantID+":"+run.UserID, snapshot.UserLimit),
		},
	}, nil
}

// parseRuntimeCapacitySnapshot protects admission from a permissive generic
// system-config read. The stored revision and payload snapshot must name the
// same immutable capacity revision before any reservation can be created.
func parseRuntimeCapacitySnapshot(record map[string]any, environment string) (runtimeCapacitySnapshot, error) {
	if record == nil || workerMapString(record, "configKey") != "runtime_capacity_v1" || workerMapString(record, "environment") != environment || workerMapString(record, "status") != "active" {
		return runtimeCapacitySnapshot{}, fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
	}
	revision, ok := runtimeCapacityStoredRevision(record["version"])
	if !ok {
		return runtimeCapacitySnapshot{}, fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
	}
	payload := aiWorkerMap(record["payload"])
	snapshotVersion, ok := runtimeCapacityPayloadInteger(payload["snapshotVersion"])
	if !ok || snapshotVersion != revision {
		return runtimeCapacitySnapshot{}, fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
	}
	modelLimit, ok := runtimeCapacityPayloadLimit(payload["modelLimit"])
	if !ok {
		return runtimeCapacitySnapshot{}, fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
	}
	authPoolLimit, ok := runtimeCapacityPayloadLimit(payload["authPoolLimit"])
	if !ok {
		return runtimeCapacitySnapshot{}, fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
	}
	toolLimit, ok := runtimeCapacityPayloadLimit(payload["toolServiceLimit"])
	if !ok {
		return runtimeCapacitySnapshot{}, fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
	}
	tenantLimit, ok := runtimeCapacityPayloadLimit(payload["tenantLimit"])
	if !ok {
		return runtimeCapacitySnapshot{}, fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
	}
	userLimit, ok := runtimeCapacityPayloadLimit(payload["userLimit"])
	if !ok {
		return runtimeCapacitySnapshot{}, fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
	}
	authPoolID, ok := payload["authPoolId"].(string)
	if !ok || strings.TrimSpace(authPoolID) == "" {
		return runtimeCapacitySnapshot{}, fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
	}
	return runtimeCapacitySnapshot{
		SnapshotVersion: snapshotVersion, ModelLimit: modelLimit, AuthPoolLimit: authPoolLimit,
		ToolLimit: toolLimit, TenantLimit: tenantLimit, UserLimit: userLimit,
		AuthPoolID: strings.TrimSpace(authPoolID),
	}, nil
}

func runtimeCapacityStoredRevision(value any) (int64, bool) {
	text, ok := value.(string)
	if !ok || text == "" || strings.TrimSpace(text) != text {
		return 0, false
	}
	revision, err := strconv.ParseInt(text, 10, 64)
	if err != nil || revision < 1 || strconv.FormatInt(revision, 10) != text {
		return 0, false
	}
	return revision, true
}

func runtimeCapacityPayloadLimit(value any) (int, bool) {
	parsed, ok := runtimeCapacityPayloadInteger(value)
	if !ok || parsed > int64(^uint(0)>>1) {
		return 0, false
	}
	return int(parsed), true
}

func runtimeCapacityPayloadInteger(value any) (int64, bool) {
	switch typed := value.(type) {
	case int:
		if typed < 1 {
			return 0, false
		}
		return int64(typed), true
	case int32:
		if typed < 1 {
			return 0, false
		}
		return int64(typed), true
	case int64:
		if typed < 1 {
			return 0, false
		}
		return typed, true
	case float64:
		if math.IsNaN(typed) || math.IsInf(typed, 0) || typed < 1 || math.Trunc(typed) != typed || typed > float64(int64(^uint64(0)>>1)) {
			return 0, false
		}
		parsed := int64(typed)
		if parsed < 1 || float64(parsed) != typed {
			return 0, false
		}
		return parsed, true
	default:
		return 0, false
	}
}

type FilesystemRuntimeManifestProvider struct {
	Root                  string
	DataRoot              string
	MetaRelease           string
	SkillHashes           map[string]string
	Resources             persistence.ResourceIndexRepository
	AttachmentReadIssuer  storageprovider.RuntimeAttachmentReadReferenceIssuer
	HuokeTopicStateLoader func(userID, workspaceID, threadID, excludeTaskID string) map[string]any
	Now                   func() time.Time
	// ExpiresIn is normally zero, which keeps the production ten-minute
	// manifest lifetime. The root-only B4 fixture CLI sets five minutes so a
	// captured submit ticket cannot become a long-lived test capability.
	ExpiresIn time.Duration
}

func (p FilesystemRuntimeManifestProvider) Build(_ context.Context, run persistence.AgentRunRecord, plan runtimepkg.AgentRunPlan, frozen domain.RunWorkspaceContextRecord, _ runtimepkg.RuntimeHost) (runtimepkg.RuntimeManifestBuildInputs, error) {
	if strings.TrimSpace(p.Root) == "" || strings.TrimSpace(p.MetaRelease) == "" {
		return runtimepkg.RuntimeManifestBuildInputs{}, fmt.Errorf("AGENT_PROFILE_UNAVAILABLE")
	}
	files, err := p.agentManifestEntries(plan, frozen)
	if err != nil {
		return runtimepkg.RuntimeManifestBuildInputs{}, err
	}
	skillHashes, skillFiles, err := p.skillManifestEntries(plan)
	if err != nil {
		return runtimepkg.RuntimeManifestBuildInputs{}, err
	}
	files = append(files, skillFiles...)
	files, skillHashes, err = p.applyFormalWorkspacePackage(run, plan, frozen, files, skillHashes)
	if err != nil {
		return runtimepkg.RuntimeManifestBuildInputs{}, err
	}
	knowledgeFiles, err := p.selectedKnowledgeManifestEntries(plan, frozen)
	if err != nil {
		return runtimepkg.RuntimeManifestBuildInputs{}, err
	}
	files = append(files, knowledgeFiles...)
	for path, value := range map[string]any{
		"input/request.json": run.RequestSnapshot, "input/task-intent.json": run.IntentSnapshot,
		"input/workspace-context.json": frozen, "capabilities/plan.json": plan,
	} {
		raw, _ := json.Marshal(value)
		files = append(files, runtimepkg.NewInlineRuntimeEntry(path, raw))
	}
	materialFiles, err := p.materialInputEntries(run, plan)
	if err != nil {
		return runtimepkg.RuntimeManifestBuildInputs{}, err
	}
	files = append(files, materialFiles...)
	expiresAt := p.now().Add(p.manifestTTL())
	attachmentFiles, err := p.attachmentInputEntriesWithExpiry(run, plan, expiresAt)
	if err != nil {
		return runtimepkg.RuntimeManifestBuildInputs{}, err
	}
	files = append(files, attachmentFiles...)
	workspaceFiles, err := p.huokeTopicWorkspaceEntries(run, plan, frozen)
	if err != nil {
		return runtimepkg.RuntimeManifestBuildInputs{}, err
	}
	files = append(files, workspaceFiles...)
	selfMediaWorkspaceFiles, err := p.selfMediaCreationWorkspaceEntries(run, plan, frozen)
	if err != nil {
		return runtimepkg.RuntimeManifestBuildInputs{}, err
	}
	files = append(files, selfMediaWorkspaceFiles...)
	rensheWorkspaceFiles, err := p.rensheContentWorkspaceEntries(run, plan, frozen)
	if err != nil {
		return runtimepkg.RuntimeManifestBuildInputs{}, err
	}
	files = append(files, rensheWorkspaceFiles...)
	fayaWorkspaceFiles, err := p.fayaGerminationWorkspaceEntries(run, plan, frozen)
	if err != nil {
		return runtimepkg.RuntimeManifestBuildInputs{}, err
	}
	files = append(files, fayaWorkspaceFiles...)
	files, err = p.sealDynamicFormalWorkspaceEntries(run, plan, frozen, files)
	if err != nil {
		return runtimepkg.RuntimeManifestBuildInputs{}, err
	}
	if err := p.verifyFormalWorkspaceManifestEntries(run, files); err != nil {
		return runtimepkg.RuntimeManifestBuildInputs{}, err
	}
	return runtimepkg.RuntimeManifestBuildInputs{
		MetaRelease: p.MetaRelease, AgentHash: normalizeDispatcherHash(plan.AgentHash),
		SkillHashes: skillHashes, Files: files, ExpiresAt: expiresAt,
	}, nil
}

func (p FilesystemRuntimeManifestProvider) attachmentInputEntries(run persistence.AgentRunRecord, plan runtimepkg.AgentRunPlan) ([]runtimepkg.RuntimeManifestEntry, error) {
	return p.attachmentInputEntriesWithExpiry(run, plan, p.now().Add(10*time.Minute))
}

func (p FilesystemRuntimeManifestProvider) attachmentInputEntriesWithExpiry(run persistence.AgentRunRecord, plan runtimepkg.AgentRunPlan, manifestExpiry time.Time) ([]runtimepkg.RuntimeManifestEntry, error) {
	if len(plan.InputAttachments) == 0 {
		return nil, nil
	}
	if p.Resources == nil || manifestExpiry.IsZero() {
		return nil, fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
	}
	entries := make([]runtimepkg.RuntimeManifestEntry, 0, len(plan.InputAttachments)+1)
	publicManifest := make([]map[string]any, 0, len(plan.InputAttachments))
	for _, attachment := range plan.InputAttachments {
		resource, ok := p.Resources.GetResource(attachment.ResourceID)
		metadata := aiWorkerMap(resource["metadata"])
		if !ok || workerMapString(resource, "userId") != run.UserID || workerMapString(resource, "workspaceId") != run.WorkspaceID ||
			workerMapString(resource, "status") != "available" || workerMapString(metadata, "tenantId") != run.TenantID ||
			workerMapString(metadata, "metaWorkspaceKey") != plan.MetaWorkspaceKey || workerMapString(metadata, "metaWorkspaceVersion") != plan.MetaWorkspaceVersion ||
			workerMapString(metadata, "inputPolicyHash") != plan.InputPolicyHash ||
			workerMapString(metadata, "inputUsage") != attachment.Usage || strings.ToLower(workerMapString(metadata, "mimeType")) != attachment.MIMEType ||
			workerAttachmentInt64(metadata["sizeBytes"]) != attachment.SizeBytes || normalizeWorkerAttachmentSHA256(workerMapString(metadata, "sha256")) != attachment.SHA256 ||
			aiWorkerInt(metadata["width"]) != attachment.Width || aiWorkerInt(metadata["height"]) != attachment.Height {
			return nil, fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
		}
		storageRef, parseErr := storageprovider.ParseStorageRef(workerMapString(resource, "storageRef"))
		if parseErr != nil || strings.TrimSpace(storageRef.Key) == "" {
			return nil, fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
		}
		entry := runtimepkg.RuntimeManifestEntry{
			LogicalPath: attachment.LogicalPath, SourceType: "object_ref", SourceRef: storageRef.Key,
			SizeBytes: attachment.SizeBytes, SHA256: attachment.SHA256,
		}
		if p.AttachmentReadIssuer != nil {
			// Leave a small deadline margin so a Host never accepts a URL after
			// the manifest RunTicket itself has already expired.
			readExpiry := manifestExpiry.Add(-5 * time.Second)
			readTTL := readExpiry.Sub(p.now())
			if readTTL <= 0 {
				return nil, fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
			}
			readURL, readErr := p.AttachmentReadIssuer.CreateRuntimeAttachmentReadURL(storageRef, readTTL)
			if readErr != nil {
				return nil, fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
			}
			// The remote resolver does not need an object key or resource ID. Keep
			// sourceRef opaque and deterministic so it cannot become another path
			// or identifier disclosure channel in an internal manifest dump.
			entry.SourceRef = "runtime-attachments/" + strings.TrimPrefix(strings.ToLower(attachment.SHA256), "sha256:")
			entry.ObjectRead = &runtimepkg.RuntimeObjectReadReference{
				URL: readURL, ExpiresAt: readExpiry, MIMEType: attachment.MIMEType,
			}
		}
		entries = append(entries, entry)
		publicManifest = append(publicManifest, map[string]any{
			"referenceResourceId": attachment.ResourceID, "usage": attachment.Usage, "mimeType": attachment.MIMEType,
			"sizeBytes": attachment.SizeBytes, "width": attachment.Width, "height": attachment.Height, "logicalPath": attachment.LogicalPath,
		})
	}
	raw, marshalErr := json.Marshal(map[string]any{"schemaVersion": "runtime_input_attachments.v1", "items": publicManifest})
	if marshalErr != nil {
		return nil, fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
	}
	entries = append(entries, runtimepkg.NewInlineRuntimeEntry("input/attachments.json", raw))
	return entries, nil
}

func (p FilesystemRuntimeManifestProvider) now() time.Time {
	if p.Now != nil {
		return p.Now().UTC()
	}
	return time.Now().UTC()
}

func (p FilesystemRuntimeManifestProvider) manifestTTL() time.Duration {
	if p.ExpiresIn > 0 && p.ExpiresIn <= 5*time.Minute {
		return p.ExpiresIn
	}
	return 10 * time.Minute
}

func (p FilesystemRuntimeManifestProvider) materialInputEntries(run persistence.AgentRunRecord, plan runtimepkg.AgentRunPlan) ([]runtimepkg.RuntimeManifestEntry, error) {
	if plan.TaskType != "minutes_generation" && plan.TaskType != "summary_generation" && plan.TaskType != "material_deposit_generation" {
		return nil, nil
	}
	refs := aiWorkerMap(run.RequestSnapshot["businessRefs"])
	materialID := workerMapString(refs, "materialId")
	if materialID == "" {
		return nil, nil
	}
	resolver := workspacepkg.NewPathResolver(p.DataRoot, "")
	root, err := resolver.ResolveFormalWorkspace(run.TenantID, run.UserID, run.WorkspaceID)
	if err != nil {
		return nil, fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
	}
	entries := []runtimepkg.RuntimeManifestEntry{}
	sourcePath, err := domain.MaterialRelativePath(materialID, domain.MaterialVariantSource)
	if err != nil {
		return nil, fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
	}
	sourceLogical := "input/transcript.md"
	if plan.TaskType == "material_deposit_generation" {
		sourceLogical = "input/source.md"
	}
	sourceEntry, err := formalWorkspaceManifestEntry(resolver, root, sourceLogical, sourcePath)
	if err != nil {
		return nil, err
	}
	entries = append(entries, sourceEntry)
	if plan.TaskType == "summary_generation" || plan.TaskType == "material_deposit_generation" {
		minutesPath, pathErr := domain.MaterialRelativePath(materialID, domain.MaterialVariantMinutes)
		if pathErr == nil {
			if minutesEntry, entryErr := formalWorkspaceManifestEntry(resolver, root, "input/minutes.md", minutesPath); entryErr == nil {
				entries = append(entries, minutesEntry)
			} else if !os.IsNotExist(entryErr) && !strings.Contains(entryErr.Error(), "MATERIAL_VARIANT_NOT_FOUND") {
				return nil, entryErr
			}
		}
	}
	return entries, nil
}

func formalWorkspaceManifestEntry(resolver workspacepkg.PathResolver, root, logicalPath, sourceRef string) (runtimepkg.RuntimeManifestEntry, error) {
	target, err := resolver.SafeJoin(root, sourceRef)
	if err != nil {
		return runtimepkg.RuntimeManifestEntry{}, fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
	}
	content, err := os.ReadFile(target)
	if err != nil {
		if os.IsNotExist(err) {
			return runtimepkg.RuntimeManifestEntry{}, fmt.Errorf("MATERIAL_VARIANT_NOT_FOUND")
		}
		return runtimepkg.RuntimeManifestEntry{}, fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
	}
	return runtimepkg.RuntimeManifestEntry{LogicalPath: logicalPath, SourceType: "formal_workspace_ref", SourceRef: sourceRef, SizeBytes: int64(len(content)), SHA256: dispatcherSHA256(content)}, nil
}

type AITaskDispatcher struct {
	Repos                      *persistence.Repositories
	Hosts                      *runtimepkg.RuntimeHostRepository
	Scheduler                  *runtimepkg.RuntimeScheduler
	Client                     runtimepkg.AsyncOpenClawClient
	CapabilityReader           runtimepkg.RuntimeCapabilityReader
	Composer                   runtimepkg.WorkspaceComposer
	ManifestProvider           RuntimeManifestProvider
	CapacityProvider           RuntimeCapacityCommandProvider
	TicketSecret               string
	SessionKeyEncryptionSecret string
	// RuntimeConfigVersions is the sole dispatch source of signed config
	// versions, keyed by the frozen Plan RuntimeConfigID. RuntimeConfigVersion
	// is retained only for isolated diagnostics that need to observe a legacy
	// setting; it must never become a dispatch fallback.
	RuntimeConfigVersions runtimepkg.RuntimeConfigVersions
	RuntimeConfigVersion  string
	InstanceID            string
	LeaseTTL              time.Duration
	IdleSleep             time.Duration
}

func NewAITaskDispatcher(repos *persistence.Repositories, hosts *runtimepkg.RuntimeHostRepository, scheduler *runtimepkg.RuntimeScheduler, client runtimepkg.AsyncOpenClawClient, provider RuntimeManifestProvider, ticketSecret, sessionKeyEncryptionSecret, _ string, instanceID string) AITaskDispatcher {
	return NewAITaskDispatcherWithCapabilityReader(
		repos, hosts, scheduler, client, provider, ticketSecret, sessionKeyEncryptionSecret, "", instanceID, nil,
	)
}

// NewAITaskDispatcherWithCapabilityReader constructs the Runtime submit path
// with the same Host-local capability reader used by planning. The selected
// reservation Host can differ from Planning's candidate, so dispatch performs
// a final fail-closed handshake before materialization or submission.
func NewAITaskDispatcherWithCapabilityReader(repos *persistence.Repositories, hosts *runtimepkg.RuntimeHostRepository, scheduler *runtimepkg.RuntimeScheduler, client runtimepkg.AsyncOpenClawClient, provider RuntimeManifestProvider, ticketSecret, sessionKeyEncryptionSecret, runtimeConfigVersion string, instanceID string, capabilityReader runtimepkg.RuntimeCapabilityReader) AITaskDispatcher {
	dispatcher := AITaskDispatcher{
		Repos: repos, Hosts: hosts, Scheduler: scheduler, Client: client, Composer: runtimepkg.NewWorkspaceComposer(),
		CapabilityReader: capabilityReader,
		ManifestProvider: provider, TicketSecret: strings.TrimSpace(ticketSecret), SessionKeyEncryptionSecret: strings.TrimSpace(sessionKeyEncryptionSecret), RuntimeConfigVersion: runtimeConfigVersion,
		InstanceID: strings.TrimSpace(instanceID), LeaseTTL: 60 * time.Second, IdleSleep: 500 * time.Millisecond,
	}
	dispatcher.CapacityProvider = RepositoryRuntimeCapacityProvider{Repos: repos, Environment: os.Getenv("HUAHUO_ENV")}
	dispatcher.configureLeaseSupervisor()
	return dispatcher
}

func (w AITaskDispatcher) Run(ctx context.Context, workerID, lane string) error {
	if w.Repos == nil || w.Repos.Queue == nil || w.Scheduler == nil || w.Scheduler.Capacity == nil || w.Client == nil || w.CapabilityReader == nil || w.ManifestProvider == nil || w.CapacityProvider == nil {
		return fmt.Errorf("RUNTIME_ADMISSION_REJECTED")
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		record, proof, err := w.Repos.Queue.Lease(ctx, lane, workerID, w.LeaseTTL, "runtime_dispatch")
		if errors.Is(err, persistence.ErrNoQueueWork) {
			if !sleepContext(ctx, w.IdleSleep) {
				return ctx.Err()
			}
			continue
		}
		if err != nil {
			return err
		}
		w.ProcessWithProof(ctx, record, proof, lane)
	}
}

func (w AITaskDispatcher) Process(ctx context.Context, queueRecord map[string]any, workerID, lane string) map[string]any {
	proof, err := dispatcherQueueProof(queueRecord, workerID)
	if err != nil {
		return map[string]any{"status": "failed", "errorCode": "STALE_QUEUE_LEASE"}
	}
	return w.ProcessWithProof(ctx, queueRecord, proof, lane)
}

func (w AITaskDispatcher) ProcessWithProof(ctx context.Context, queueRecord map[string]any, proof persistence.QueueLeaseProof, lane string) map[string]any {
	queueID := workerMapString(queueRecord, "queueId")
	payload := aiWorkerMap(queueRecord["payload"])
	runID := firstWorkerString(workerMapString(payload, "agentRunId"), workerMapString(queueRecord, "taskId"))
	planVersion := aiWorkerInt(payload["planVersion"])
	if queueID == "" || queueID != proof.QueueID || runID == "" || planVersion < 1 || w.Repos == nil || w.Repos.AgentRuns == nil || w.Repos.Queue == nil {
		return map[string]any{"status": "failed", "errorCode": "AGENT_PLAN_INVALID"}
	}
	if _, err := w.Repos.Queue.MarkRunning(ctx, proof); err != nil {
		return map[string]any{"queueId": queueID, "agentRunId": runID, "status": "failed", "errorCode": "STALE_QUEUE_LEASE"}
	}
	run, err := w.Repos.AgentRuns.GetRunInternal(ctx, runID)
	if err != nil || run.Status != "queued" {
		return w.fail(ctx, proof, runID, "AGENT_PLAN_EXPIRED", false)
	}
	planMap, err := w.Repos.AgentRuns.GetPlan(ctx, runID, planVersion)
	if err != nil {
		return w.fail(ctx, proof, runID, "AGENT_PLAN_EXPIRED", false)
	}
	var plan runtimepkg.AgentRunPlan
	rawPlan, _ := json.Marshal(planMap)
	if json.Unmarshal(rawPlan, &plan) != nil || plan.AgentRunID != runID || plan.CapabilityHash == "" {
		return w.fail(ctx, proof, runID, "AGENT_PLAN_INVALID", false)
	}
	runtimeConfigID, err := runtimeConfigIDForPlan(plan)
	if err != nil {
		return w.fail(ctx, proof, runID, "AGENT_PLAN_INVALID", false)
	}
	runtimeConfigVersion, err := w.runtimeConfigVersionForPlan(runtimeConfigID)
	if err != nil {
		return w.fail(ctx, proof, runID, "AGENT_PLAN_INVALID", false)
	}
	frozen, err := w.Repos.AgentRuns.GetWorkspaceContextByRunID(ctx, runID)
	if err != nil || frozen.Status != "frozen" || frozen.CapabilityHash != plan.CapabilityHash {
		return w.fail(ctx, proof, runID, "AGENT_PLAN_EXPIRED", false)
	}
	if err := w.validateFrozenWorkspaceForManifest(ctx, run, planMap, frozen); err != nil {
		// Plans created through a legacy/direct persistence path must not bypass
		// the immutable Run Workspace context just because their scalar version
		// still happens to match.
		return w.fail(ctx, proof, runID, "AGENT_PLAN_EXPIRED", false)
	}
	// Fail before any capacity/session/slot admission when the Worker was not
	// constructed with the required Host-local capability boundary.
	if w.CapabilityReader == nil {
		return w.fail(ctx, proof, runID, "RUNTIME_TOOL_UNAVAILABLE", true)
	}
	capacityCommand, err := w.CapacityProvider.Resolve(ctx, run, plan)
	if err != nil {
		return w.fail(ctx, proof, runID, "RUNTIME_CAPACITY_UNAVAILABLE", true)
	}
	capacityReservation, err := w.Scheduler.Capacity.Reserve(ctx, capacityCommand)
	if err != nil {
		return w.fail(ctx, proof, runID, planningErrorCode(err), true)
	}
	sessionRef, sessionRecord, err := w.productSession(ctx, run, plan, frozen)
	if err != nil {
		_, _ = w.Scheduler.Capacity.Release(ctx, capacityReservation, nil)
		return w.fail(ctx, proof, runID, planningErrorCode(err), false)
	}
	sessionThreadID, threadOK := sessionRef["threadId"].(string)
	sessionKey, sessionOK := sessionRef["openclawSessionKey"].(string)
	if !threadOK || !sessionOK || sessionThreadID == "" || sessionKey == "" ||
		strings.TrimSpace(sessionThreadID) != sessionThreadID || strings.TrimSpace(sessionKey) != sessionKey {
		_, _ = w.Scheduler.Capacity.Release(ctx, capacityReservation, nil)
		return w.fail(ctx, proof, runID, "RUNTIME_SESSION_BINDING_UNAVAILABLE", false)
	}
	sessionBinding := runtimepkg.ProductSessionHostBinding{TenantID: run.TenantID, ThreadID: run.ThreadID, AgentProfile: plan.L1AgentProfile, ContextGeneration: run.ContextGeneration, SessionGeneration: frozen.SessionGeneration}
	sessionAdmission := runtimepkg.ProductSessionAdmissionCommand{}
	if plan.ExecutionScope == "product_thread" {
		sessionAdmission = runtimepkg.ProductSessionAdmissionCommand{
			Key:       runtimepkg.ProductSessionAdmissionKey{TenantID: run.TenantID, ThreadID: run.ThreadID, AgentProfile: plan.L1AgentProfile, ContextGeneration: run.ContextGeneration, SessionGeneration: frozen.SessionGeneration},
			BindingID: sessionRecord.BindingID, RunID: runID, OwnerInstanceID: firstWorkerString(w.InstanceID, proof.WorkerID), TTL: 30 * time.Second,
		}
	}
	reservationLease, err := w.Scheduler.Reserve(ctx, runtimepkg.ScheduleCommand{
		RunID: runID, OwnerInstanceID: firstWorkerString(w.InstanceID, proof.WorkerID), ExecutionScope: plan.ExecutionScope,
		CapabilityHash: plan.CapabilityHash, RequiredTools: plan.RequiredTools, SessionBinding: sessionBinding,
		SessionAdmission: sessionAdmission, CapacityReservation: capacityReservation, ReservationTTL: 30 * time.Second,
	})
	if err != nil {
		_, _ = w.Scheduler.Capacity.Release(ctx, capacityReservation, nil)
		return w.fail(ctx, proof, runID, planningErrorCode(err), true)
	}
	reservation, host := reservationLease.Reservation, reservationLease.Host
	if err := w.validateReservedHostCapabilities(ctx, host, plan); err != nil {
		_ = w.Scheduler.ReleaseBeforeAccept(ctx, reservationLease, "capability_handshake_failed")
		return w.fail(ctx, proof, runID, planningErrorCode(runtimeCapabilityAdmissionError(err)), true)
	}
	// The frozen context can be overtaken while capacity/session/Host admission
	// is in progress. Recheck the live Workspace immediately before reading any
	// formal Workspace files into the manifest.
	if err := w.validateFrozenWorkspaceForManifest(ctx, run, planMap, frozen); err != nil {
		_ = w.Scheduler.ReleaseBeforeAccept(ctx, reservationLease, "workspace_version_conflict")
		return w.fail(ctx, proof, runID, "AGENT_PLAN_EXPIRED", false)
	}
	inputs, err := w.ManifestProvider.Build(ctx, run, plan, frozen, host)
	if err != nil {
		_ = w.Scheduler.ReleaseBeforeAccept(ctx, reservationLease, "dispatch_failed")
		return w.fail(ctx, proof, runID, planningErrorCode(err), false)
	}
	inputs.RuntimeHostID = host.RuntimeHostID
	manifest, err := w.Composer.BuildInputManifest(ctx, plan, frozen, inputs)
	if err != nil {
		_ = w.Scheduler.ReleaseBeforeAccept(ctx, reservationLease, "dispatch_failed")
		return w.fail(ctx, proof, runID, planningErrorCode(err), false)
	}
	// The provider has now read and hashed its formal Workspace inputs. Do not
	// sign or submit that manifest if a W2 Workspace/context replaced the W1
	// facts that were frozen into this Run while it was being built.
	if err := w.validateFrozenWorkspaceForManifest(ctx, run, planMap, frozen); err != nil {
		_ = w.Scheduler.ReleaseBeforeAccept(ctx, reservationLease, "workspace_version_conflict")
		return w.fail(ctx, proof, runID, "AGENT_PLAN_EXPIRED", false)
	}
	dispatchID := fmt.Sprintf("dispatch_%s_%d", runID, reservation.FencingToken)
	ticketExpiry := manifest.ExpiresAt
	planHash, err := runtimepkg.ComputeAgentRunPlanHash(plan)
	if usesStableTestAdapterCompatibility(host) {
		planHash, err = runtimepkg.ComputeAgentRunPlanHashForStableV05Adapter(plan)
	}
	if err != nil {
		_ = w.Scheduler.ReleaseBeforeAccept(ctx, reservationLease, "dispatch_failed")
		return w.fail(ctx, proof, runID, "AGENT_PLAN_INVALID", false)
	}
	inputMessage := runtimeInputMessageForPlan(plan, manifest, stringValueFromNestedMap(run.RequestSnapshot, "input", "text"))
	submitBinding := &runtimepkg.RunTicketSubmitBinding{
		Version: runtimepkg.RuntimeSubmitBindingV2, InputMessageHash: runtimepkg.RunTicketInputMessageHash(inputMessage),
		RuntimeConfigID: runtimeConfigID, RuntimeConfigVersion: runtimeConfigVersion,
		ProductSessionHash: runtimepkg.RunTicketProductSessionHash(sessionThreadID, sessionKey),
	}
	if usesStableTestAdapterCompatibility(host) {
		submitBinding = nil
	}
	ticket, err := runtimepkg.SignRunTicket(runtimepkg.RunTicketClaims{
		RunID: runID, TenantID: run.TenantID, ReservationID: reservation.ReservationID, RuntimeHostID: host.RuntimeHostID,
		CapabilityHash: plan.CapabilityHash, WorkspaceID: run.WorkspaceID, WorkspaceVersion: frozen.WorkspaceVersion,
		ContextGeneration: frozen.ContextGeneration, InputManifestHash: manifest.ManifestHash,
		PlanHash: planHash, SubmitBinding: submitBinding,
		FencingToken: reservation.FencingToken, JTI: dispatchID,
		IssuedAt: time.Now().UTC().Unix(), ExpiresAt: ticketExpiry.Unix(),
	}, w.TicketSecret)
	if err != nil {
		_ = w.Scheduler.ReleaseBeforeAccept(ctx, reservationLease, "dispatch_failed")
		return w.fail(ctx, proof, runID, "RUNTIME_PERMISSION_DENIED", false)
	}
	runtimeRecord, err := runtimepkg.NewRuntimeRunRecordV1(run, plan, frozen, runtimeConfigID, runtimeConfigVersion)
	if err != nil {
		_ = w.Scheduler.ReleaseBeforeAccept(ctx, reservationLease, "dispatch_failed")
		return w.fail(ctx, proof, runID, planningErrorCode(err), false)
	}
	dispatch, err := w.Hosts.CreateDispatchWithRuntimeRunRecord(ctx, runtimepkg.RuntimeDispatch{
		DispatchID: dispatchID, RunID: runID, ReservationID: reservation.ReservationID,
		CapacityReservationID: reservationLease.Capacity.ReservationID, CapacityReservedVersion: reservationLease.Capacity.Version,
		RuntimeHostID: host.RuntimeHostID, DispatchAttempt: proof.Attempt,
		PlanVersion:  planVersion,
		FencingToken: reservation.FencingToken, RunTicketJTIHash: runtimepkg.RunTicketJTIHash(dispatchID),
		TicketExpiresAt: ticketExpiry, InputManifestHash: manifest.ManifestHash,
		OwnerInstanceID: reservation.OwnerInstanceID, LeaseTokenHash: reservation.LeaseTokenHash, LeaseExpiresAt: reservation.ExpiresAt,
	}, runtimeRecord)
	if err != nil {
		_ = w.Scheduler.ReleaseBeforeAccept(ctx, reservationLease, "dispatch_failed")
		return w.fail(ctx, proof, runID, planningErrorCode(err), true)
	}
	if err := w.Scheduler.BindDispatch(ctx, reservationLease, dispatch.DispatchID); err != nil {
		_ = w.Scheduler.ReleaseBeforeAccept(ctx, reservationLease, "dispatch_failed")
		return w.fail(ctx, proof, runID, planningErrorCode(err), false)
	}
	proof, err = w.Repos.Queue.Heartbeat(ctx, proof, w.LeaseTTL)
	if err != nil {
		_ = w.Scheduler.ReleaseBeforeAccept(ctx, reservationLease, "dispatch_failed")
		return map[string]any{"queueId": queueID, "agentRunId": runID, "status": "failed", "errorCode": "STALE_QUEUE_LEASE"}
	}
	if w.Scheduler.LeaseSupervisor == nil {
		_ = w.Scheduler.ReleaseBeforeAccept(ctx, reservationLease, "dispatch_failed")
		return w.fail(ctx, proof, runID, "RUNTIME_ADMISSION_REJECTED", true)
	}
	ownershipTTL := time.Until(reservation.ExpiresAt)
	if ownershipTTL <= 0 {
		_ = w.Scheduler.ReleaseBeforeAccept(ctx, reservationLease, "dispatch_failed")
		return w.fail(ctx, proof, runID, "STALE_FENCING_TOKEN", true)
	}
	if err := w.Scheduler.LeaseSupervisor.Track(ctx, reservationLease, dispatch.DispatchID, ownershipTTL); err != nil {
		_ = w.Scheduler.ReleaseBeforeAccept(ctx, reservationLease, "dispatch_failed")
		return w.fail(ctx, proof, runID, planningErrorCode(err), true)
	}
	if err := w.Repos.AgentRuns.AuthorizeDispatchSubmit(ctx, runID, dispatch.DispatchID, reservation.ReservationID, reservation.FencingToken); err != nil {
		if err.Error() == "AGENT_RUN_CANCELLED" {
			current, getErr := w.Repos.AgentRuns.GetRunInternal(ctx, runID)
			if getErr == nil && current.Status == "cancelled" {
				_ = w.Scheduler.ReleaseBeforeAccept(ctx, reservationLease, "cancelled_before_submit")
			}
			if _, completeErr := w.Repos.Queue.Complete(ctx, proof); completeErr != nil {
				return staleQueueLeaseDispatchResult(queueID, runID)
			}
			if getErr == nil && current.Status == "aborting" {
				w.enqueueRuntimeEventRecovery(runID, dispatch.DispatchID, host.RuntimeHostID)
			}
			return map[string]any{"queueId": queueID, "agentRunId": runID, "dispatchId": dispatch.DispatchID, "status": firstWorkerString(current.Status, "aborting"), "lane": lane}
		}
		_ = w.Scheduler.ReleaseBeforeAccept(ctx, reservationLease, "dispatch_failed")
		return w.fail(ctx, proof, runID, planningErrorCode(err), true)
	}
	if err := w.Hosts.MarkDispatchSent(ctx, dispatch.DispatchID, reservationLease.Fence()); err != nil {
		_ = w.Scheduler.ReleaseBeforeAccept(ctx, reservationLease, "dispatch_failed")
		return w.fail(ctx, proof, runID, planningErrorCode(err), true)
	}
	result, err := w.Client.Submit(ctx, host, runtimepkg.AsyncRuntimeSubmitRequest{
		RunID: runID, ReservationID: reservation.ReservationID, FencingToken: reservation.FencingToken,
		CapabilityHash: plan.CapabilityHash, InputMessage: inputMessage,
		RuntimeConfigID: runtimeConfigID, RuntimeConfigVersion: runtimeConfigVersion,
		RunTicket: ticket, RunTicketJTI: dispatchID, InputManifest: manifest, Plan: plan, ProductSessionRef: sessionRef,
	})
	if err != nil {
		if errors.Is(err, runtimepkg.ErrRuntimeSubmitCaptured) {
			// The root-only B4 fixture CLI has written the exact signed request
			// and deliberately stopped before Host I/O. Close every durable
			// reservation path before marking the synthetic fixture Run terminal;
			// it must never enter normal submit-unknown recovery.
			_ = w.Hosts.MarkDispatchTerminal(ctx, dispatch.DispatchID, host.RuntimeHostID, reservation.FencingToken, "rejected", "RUNTIME_SUBMIT_CAPTURED")
			_ = w.Scheduler.ReleaseBeforeAccept(ctx, reservationLease, "submit_binding_fixture_captured")
			result := w.fail(ctx, proof, runID, "RUNTIME_SUBMIT_CAPTURED", false)
			if workerMapString(result, "errorCode") != "RUNTIME_SUBMIT_CAPTURED" {
				return result
			}
			if planErr := finalizeCapturedSubmitPlan(ctx, w.Repos.AgentRuns, runID, planVersion, planMap); planErr != nil {
				log.Printf("runtime submit capture plan finalization failed run=%s err=%v", runID, planErr)
				result["errorCode"] = "RUNTIME_SUBMIT_CAPTURED_PLAN_FINALIZATION_FAILED"
			}
			return result
		}
		// Keep the runtime request opaque, but leave an actionable operator signal.
		errorCode := planningErrorCode(err)
		log.Printf("runtime submit failed run=%s host=%s code=%s diagnostic=%v", runID, host.RuntimeHostID, errorCode, runtimepkg.RuntimeFailureSummary(err, runtimepkg.RuntimeRunResult{}))
		_ = w.Hosts.MarkDispatchSubmitUnknown(ctx, dispatch.DispatchID, reservationLease.Fence(), time.Now().UTC().Add(5*time.Second))
		if _, completeErr := w.Repos.Queue.Complete(ctx, proof); completeErr != nil {
			return staleQueueLeaseDispatchResult(queueID, runID)
		}
		w.enqueueRuntimeEventRecovery(runID, dispatch.DispatchID, host.RuntimeHostID)
		return map[string]any{"queueId": queueID, "agentRunId": runID, "dispatchId": dispatch.DispatchID, "status": "submit_unknown", "lane": lane}
	}
	if err := w.Scheduler.Accept(ctx, reservationLease, dispatch.DispatchID, result.RuntimeRequestID); err != nil {
		_ = w.Hosts.MarkDispatchSubmitUnknown(ctx, dispatch.DispatchID, reservationLease.Fence(), time.Now().UTC())
		if _, completeErr := w.Repos.Queue.Complete(ctx, proof); completeErr != nil {
			return staleQueueLeaseDispatchResult(queueID, runID)
		}
		w.enqueueRuntimeEventRecovery(runID, dispatch.DispatchID, host.RuntimeHostID)
		return map[string]any{"queueId": queueID, "agentRunId": runID, "dispatchId": dispatch.DispatchID, "status": "submit_unknown", "errorCode": planningErrorCode(err), "lane": lane}
	}
	if err := w.Repos.AgentRuns.UpdateStatusVersioned(ctx, runID, []string{"dispatched"}, "accepted", nil); err != nil {
		current, getErr := w.Repos.AgentRuns.GetRunInternal(ctx, runID)
		if getErr == nil && current.Status == "aborting" {
			if _, completeErr := w.Repos.Queue.Complete(ctx, proof); completeErr != nil {
				return staleQueueLeaseDispatchResult(queueID, runID)
			}
			w.enqueueRuntimeEventRecovery(runID, dispatch.DispatchID, host.RuntimeHostID)
			return map[string]any{"queueId": queueID, "agentRunId": runID, "dispatchId": dispatch.DispatchID, "status": "aborting", "lane": lane}
		}
		return w.fail(ctx, proof, runID, planningErrorCode(err), false)
	}
	if err := w.Repos.AgentRuns.UpdateStatusVersioned(ctx, runID, []string{"accepted"}, "running", nil); err != nil {
		current, getErr := w.Repos.AgentRuns.GetRunInternal(ctx, runID)
		if getErr == nil && current.Status == "aborting" {
			if _, completeErr := w.Repos.Queue.Complete(ctx, proof); completeErr != nil {
				return staleQueueLeaseDispatchResult(queueID, runID)
			}
			w.enqueueRuntimeEventRecovery(runID, dispatch.DispatchID, host.RuntimeHostID)
			return map[string]any{"queueId": queueID, "agentRunId": runID, "dispatchId": dispatch.DispatchID, "status": "aborting", "lane": lane}
		}
		return w.fail(ctx, proof, runID, planningErrorCode(err), false)
	}
	runningRun, _ := w.Repos.AgentRuns.GetRunInternal(ctx, runID)
	if err := services.NewAgentRunProductProjector(w.Repos, time.Now).ProjectRunning(runningRun); err != nil {
		return w.fail(ctx, proof, runID, "PRODUCT_TASK_PROJECTION_FAILED", true)
	}
	planStatus := workerMapString(planMap, "status")
	if planStatus == "validated" || planStatus == "confirmed" {
		_ = w.Repos.AgentRuns.MarkPlanStatus(ctx, runID, planVersion, planStatus, "executing")
	}
	if _, err := w.Repos.Queue.Complete(ctx, proof); err != nil {
		return staleQueueLeaseDispatchResult(queueID, runID)
	}
	w.enqueueRuntimeEventRecovery(runID, dispatch.DispatchID, host.RuntimeHostID)
	_ = w.Repos.AgentRuns.AppendPublicEvent(ctx, persistence.AgentRunEvent{AgentRunID: runID, EventType: "running", Status: "running", SafeData: map[string]any{"status": "running", "routing": publicDispatchRouting(plan)}})
	return map[string]any{"queueId": queueID, "agentRunId": runID, "dispatchId": dispatch.DispatchID, "status": "running", "lane": lane}
}

func (w AITaskDispatcher) runtimeConfigVersionForPlan(runtimeConfigID string) (string, error) {
	return w.RuntimeConfigVersions.VersionFor(runtimeConfigID)
}

func (w AITaskDispatcher) validateFrozenWorkspaceForManifest(ctx context.Context, run persistence.AgentRunRecord, plan map[string]any, frozen domain.RunWorkspaceContextRecord) error {
	if w.Repos == nil || w.Repos.AgentRuns == nil || w.Repos.Workspace == nil {
		return domain.ErrorCode("WORKSPACE_VERSION_CONFLICT")
	}
	if err := w.Repos.AgentRuns.ValidateFrozenWorkspaceContextForPlan(ctx, run, plan); err != nil {
		return err
	}
	return workspacepkg.NewViewService(w.Repos.Workspace, w.Repos.AgentRuns).ValidateFrozenForDispatch(ctx, frozen)
}

// finalizeCapturedSubmitPlan applies only after the root-only B4 fixture has
// durably failed its Run/queue before Host I/O. The capture must not leave a
// validated plan beside a terminal failed Run, because cleanup deliberately
// rejects nonterminal lifecycle facts instead of weakening its state gate.
func finalizeCapturedSubmitPlan(ctx context.Context, agentRuns *persistence.AgentRunRepository, runID string, planVersion int, plan map[string]any) error {
	if agentRuns == nil {
		return fmt.Errorf("AGENT_PLAN_EXPIRED")
	}
	status := workerMapString(plan, "status")
	if status != "validated" && status != "confirmed" {
		return fmt.Errorf("AGENT_PLAN_EXPIRED")
	}
	return agentRuns.MarkPlanStatus(ctx, runID, planVersion, status, "failed")
}

func publicDispatchRouting(plan runtimepkg.AgentRunPlan) map[string]any {
	routing := map[string]any{"state": "selected"}
	if strings.TrimSpace(plan.MetaWorkspaceKey) != "" {
		routing["selectedMetaWorkspace"] = map[string]any{
			"metaWorkspaceKey": plan.MetaWorkspaceKey,
			"version":          plan.MetaWorkspaceVersion,
			"inputPolicyHash":  plan.InputPolicyHash,
		}
	}
	return routing
}

func runtimeInputMessageForPlan(plan runtimepkg.AgentRunPlan, manifest runtimepkg.RuntimeInputManifest, message string) string {
	if plan.TaskType == "work_ai_renshe_content" {
		return runtimepkg.BuildRensheContentTurnMessage(message)
	}
	if plan.TaskType == "work_ai_faya_germination" {
		return runtimepkg.BuildFayaGerminationTurnMessage(message)
	}
	if plan.TaskType == "work_ai_huoke_content" {
		return runtimepkg.BuildHuokeContentTurnMessage(message)
	}
	if plan.TaskType == "work_ai_huoke_topic_strategy" {
		profileContextVersion := fmt.Sprintf("wv:%d|iv:%d|cg:%d", plan.WorkspaceVersion, plan.IndexVersion, manifest.ContextGeneration)
		return runtimepkg.BuildHuokeTopicStrategyTurnMessage(message, profileContextVersion, runtimeManifestHasFile(manifest, "input/consultation_state.json"))
	}
	return message
}

func runtimeManifestHasFile(manifest runtimepkg.RuntimeInputManifest, logicalPath string) bool {
	for _, entry := range manifest.Files {
		if entry.LogicalPath == logicalPath {
			return true
		}
	}
	return false
}

func runtimeConfigIDForPlan(plan runtimepkg.AgentRunPlan) (string, error) {
	// Runtime configuration is a frozen Plan identity. A worker must not infer
	// it from a task type, a mutable Agent profile, or process configuration.
	configured := strings.TrimSpace(plan.RuntimeConfigID)
	if configured == "" || configured != plan.RuntimeConfigID {
		return "", fmt.Errorf("AGENT_PLAN_INVALID")
	}
	return configured, nil
}

// validateReservedHostCapabilities is intentionally after Scheduler.Reserve:
// planning can only select a capability hash, while the scheduler may reserve
// a different ready Host with that hash. A final live document check binds the
// frozen allow-list and budget to the exact Host that will receive Submit.
// It neither writes Host state nor substitutes the registration snapshot.
func (w AITaskDispatcher) validateReservedHostCapabilities(ctx context.Context, host runtimepkg.RuntimeHost, plan runtimepkg.AgentRunPlan) error {
	if w.CapabilityReader == nil || strings.TrimSpace(host.RuntimeHostID) == "" || strings.TrimSpace(plan.CapabilityHash) == "" ||
		strings.TrimSpace(host.CapabilityHash) != strings.TrimSpace(plan.CapabilityHash) ||
		strings.TrimSpace(host.Capabilities.CapabilityHash) != strings.TrimSpace(plan.CapabilityHash) {
		return domain.ErrorCode("RUNTIME_TOOL_UNAVAILABLE")
	}
	if err := validateReservedHostPlanAvailability(host, host.Capabilities, plan.RequiredTools, plan.ToolBudget); err != nil {
		return runtimeCapabilityAdmissionError(err)
	}
	freshReader, ok := w.CapabilityReader.(runtimepkg.RuntimeFreshCapabilityReader)
	if !ok {
		return domain.ErrorCode("RUNTIME_TOOL_UNAVAILABLE")
	}
	live, err := freshReader.GetFreshCapabilities(ctx, host)
	if err != nil {
		return runtimeCapabilityAdmissionError(err)
	}
	if strings.TrimSpace(live.CapabilityHash) == "" || live.CapabilityHash != plan.CapabilityHash {
		return domain.ErrorCode("RUNTIME_TOOL_UNAVAILABLE")
	}
	if err := runtimepkg.ValidateRuntimeSubmitBindingCapability(live.SubmitBinding); err != nil {
		if !usesStableTestAdapterCompatibility(host) {
			return runtimeCapabilityAdmissionError(err)
		}
	}
	if err := validateReservedHostPlanAvailability(host, live.PlannerSnapshot(), plan.RequiredTools, plan.ToolBudget); err != nil {
		return runtimeCapabilityAdmissionError(err)
	}
	return nil
}

// validateReservedHostPlanAvailability keeps the normal capability contract
// fail-closed. The one test-only v0.5 Adapter predates submit-binding
// projection, so its missing field is treated as a legacy transport detail
// only after the Host identity and explicit process flag have both matched.
func validateReservedHostPlanAvailability(host runtimepkg.RuntimeHost, capabilities runtimepkg.RuntimeCapabilitySnapshot, requiredTools []string, budget runtimepkg.RuntimeToolBudget) error {
	err := runtimepkg.ValidateRuntimePlanAvailability(capabilities, requiredTools, budget)
	if err == nil || !usesStableTestAdapterCompatibility(host) || !missingRuntimeSubmitBinding(capabilities.SubmitBinding) {
		return err
	}

	// Substitute the currently required binding only for validation of the
	// remaining immutable capability contract. The legacy adapter still
	// receives a ticket without submitBinding below.
	compatibilitySnapshot := capabilities
	compatibilitySnapshot.SubmitBinding = runtimepkg.RuntimeSubmitBindingCapability{
		Version:            runtimepkg.RuntimeSubmitBindingV2,
		ProductSessionHash: true,
	}
	return runtimepkg.ValidateRuntimePlanAvailability(compatibilitySnapshot, requiredTools, budget)
}

func missingRuntimeSubmitBinding(binding runtimepkg.RuntimeSubmitBindingCapability) bool {
	return strings.TrimSpace(binding.Version) == "" && !binding.ProductSessionHash
}

// usesStableTestAdapterCompatibility is pinned to the isolated test Adapter.
// It cannot apply unless that service explicitly enables legacy ticket support
// and identifies itself as the single v0.5 test Host.
func usesStableTestAdapterCompatibility(host runtimepkg.RuntimeHost) bool {
	return os.Getenv("HUAHUO_RUNTIME_LEGACY_RUN_TICKET_COMPAT") == "1" &&
		host.RuntimeHostID == "runtime-host-test-1" &&
		host.Environment == "test" && host.AdapterVersion == "v0.5"
}

// Capability discovery errors may contain transport-specific details. The
// scheduling boundary exposes only the two documented safe outcomes.
func runtimeCapabilityAdmissionError(err error) error {
	if planningErrorCode(err) == "RUNTIME_TOOL_BUDGET_UNSUPPORTED" {
		return domain.ErrorCode("RUNTIME_TOOL_BUDGET_UNSUPPORTED")
	}
	return domain.ErrorCode("RUNTIME_TOOL_UNAVAILABLE")
}

func dispatchExecutionIdentity(plan runtimepkg.AgentRunPlan, runtimeConfigID string) map[string]any {
	skills := append([]string{}, plan.SelectedSkillProfiles...)
	sort.Strings(skills)
	return map[string]any{
		"taskType":        plan.TaskType,
		"agentProfile":    plan.L1AgentProfile,
		"skillProfiles":   skills,
		"runtimeConfigId": runtimeConfigID,
	}
}

func (w AITaskDispatcher) productSession(ctx context.Context, run persistence.AgentRunRecord, plan runtimepkg.AgentRunPlan, frozen domain.RunWorkspaceContextRecord) (map[string]any, runtimepkg.ProductSessionBinding, error) {
	if run.ThreadID == "" {
		return map[string]any{
			"threadId":           "detached:" + run.AgentRunID,
			"openclawSessionKey": fmt.Sprintf("runtime:tenant:%s:user:%s:workspace:%s:run:%s", run.TenantID, run.UserID, run.WorkspaceID, run.AgentRunID),
		}, runtimepkg.ProductSessionBinding{}, nil
	}
	if w.Hosts == nil || frozen.SessionGeneration < 1 {
		return nil, runtimepkg.ProductSessionBinding{}, fmt.Errorf("RUNTIME_INPUT_INVALID")
	}
	binding, err := w.Hosts.GetProductSessionBinding(ctx, runtimepkg.ProductSessionHostBinding{
		TenantID: run.TenantID, ThreadID: run.ThreadID, AgentProfile: plan.L1AgentProfile, ContextGeneration: run.ContextGeneration,
		SessionGeneration: frozen.SessionGeneration,
	}, w.SessionKeyEncryptionSecret)
	if err != nil || binding.TenantID != run.TenantID || binding.UserID != run.UserID || binding.WorkspaceID != run.WorkspaceID {
		return nil, runtimepkg.ProductSessionBinding{}, fmt.Errorf("RUNTIME_SESSION_BINDING_UNAVAILABLE")
	}
	return map[string]any{
		"threadId": binding.ThreadID, "openclawSessionKey": binding.OpenClawSessionKey,
		"sessionStoreId": binding.SessionStoreID, "agentProfile": binding.AgentProfile,
		"contextGeneration": binding.ContextGeneration, "sessionGeneration": binding.SessionGeneration,
	}, binding, nil
}

func (w AITaskDispatcher) fail(ctx context.Context, proof persistence.QueueLeaseProof, runID, code string, retryable bool) map[string]any {
	log.Printf("runtime dispatch failed run=%s code=%s retryable=%t", runID, code, retryable)
	if retryable {
		if _, err := w.Repos.Queue.ScheduleRetry(ctx, proof, runtimeDispatchRetryDelay(proof, code), code); err != nil {
			return staleQueueLeaseDispatchResult(proof.QueueID, runID)
		}
	} else {
		if _, err := w.Repos.Queue.Fail(ctx, proof, code, false); err != nil {
			return staleQueueLeaseDispatchResult(proof.QueueID, runID)
		}
		errorSummary := map[string]any{"code": code, "stage": "runtime_dispatch", "retryable": false}
		if err := w.Repos.AgentRuns.UpdateStatusVersioned(ctx, runID, []string{"queued", "reserving", "dispatched", "accepted", "materializing", "running"}, "failed", map[string]any{"errorSummary": errorSummary}); err == nil {
			if failedRun, getErr := w.Repos.AgentRuns.GetRunInternal(ctx, runID); getErr == nil {
				_ = services.NewAgentRunProductProjector(w.Repos, time.Now).ProjectTerminal(failedRun, "failed", nil, errorSummary, nil)
				_ = w.Repos.AgentRuns.AppendPublicEventIdempotent(ctx, persistence.AgentRunEvent{
					AgentRunID: runID,
					EventType:  "failed",
					Status:     "failed",
					SafeData:   errorSummary,
				}, "runtime-dispatch-failed:"+proof.QueueID)
			}
		}
	}
	return map[string]any{"queueId": proof.QueueID, "agentRunId": runID, "status": "failed", "errorCode": code, "retryable": retryable}
}

func runtimeDispatchRetryDelay(proof persistence.QueueLeaseProof, code string) time.Duration {
	if code != "RUNTIME_CAPACITY_UNAVAILABLE" {
		return time.Second
	}
	attempt := proof.Attempt
	if attempt < 1 {
		attempt = 1
	}
	delay := runtimeCapacityRetryInitialDelay
	for retry := 1; retry < attempt && delay < runtimeCapacityRetryMaximumDelay; retry++ {
		delay *= 2
	}
	if delay > runtimeCapacityRetryMaximumDelay {
		return runtimeCapacityRetryMaximumDelay
	}
	return delay
}

func staleQueueLeaseDispatchResult(queueID, runID string) map[string]any {
	return map[string]any{"queueId": queueID, "agentRunId": runID, "status": "aborted", "errorCode": "STALE_QUEUE_LEASE"}
}

func (w AITaskDispatcher) enqueueRuntimeEventRecovery(runID, dispatchID, hostID string) {
	w.Repos.Queue.Enqueue(map[string]any{
		"queueId": queue.QueueRuntimeEvents + ":" + dispatchID, "queueName": queue.QueueRuntimeEvents,
		"taskType": "runtime_event_ingest", "taskId": runID, "dedupeKey": "runtime_event_ingest:" + dispatchID,
		"priority": 100, "maxAttempts": 7200,
		"payload": map[string]any{"runId": runID, "dispatchId": dispatchID, "runtimeHostId": hostID},
	})
}

func (w AITaskDispatcher) configureLeaseSupervisor() {
	if w.Scheduler == nil || w.Scheduler.LeaseSupervisor == nil || w.Repos == nil || w.Repos.AgentRuns == nil || w.Repos.Queue == nil {
		return
	}
	w.Scheduler.LeaseSupervisor.SetLeaseLossHandler(func(ctx context.Context, loss runtimepkg.RuntimeLeaseLoss) {
		reasonHash := dispatcherSHA256([]byte("runtime_lease_lost:" + loss.DispatchID + ":" + loss.ErrorCode))
		_, _ = w.Repos.AgentRuns.RequestCancelAndEnqueue(ctx, loss.RunID, "LEASE_LOST", reasonHash, w.Repos.Queue)
		w.enqueueRuntimeEventRecovery(loss.RunID, loss.DispatchID, loss.Handle.Reservation.RuntimeHostID)
		_ = w.Repos.AgentRuns.AppendPublicEventIdempotent(ctx, persistence.AgentRunEvent{
			AgentRunID: loss.RunID, EventType: "aborting", Status: "aborting",
			SafeData: map[string]any{"status": "aborting", "code": loss.ErrorCode},
		}, "runtime-lease-lost:"+loss.DispatchID)
	})
}

func dispatcherQueueProof(record map[string]any, workerID string) (persistence.QueueLeaseProof, error) {
	expiresAt := time.Time{}
	switch value := record["leaseExpiresAt"].(type) {
	case time.Time:
		expiresAt = value
	case string:
		expiresAt, _ = time.Parse(time.RFC3339Nano, value)
	}
	proof := persistence.QueueLeaseProof{
		QueueID: workerMapString(record, "queueId"), WorkerID: workerID,
		Attempt: aiWorkerInt(record["attempt"]), TokenHash: workerMapString(record, "leaseTokenHash"),
		FencingToken: int64(aiWorkerInt(record["leaseFencingToken"])), LeaseExpiresAt: expiresAt,
	}
	if proof.QueueID == "" || proof.WorkerID == "" || proof.Attempt < 1 || proof.TokenHash == "" || proof.FencingToken < 1 || proof.LeaseExpiresAt.IsZero() {
		return persistence.QueueLeaseProof{}, fmt.Errorf("STALE_QUEUE_LEASE")
	}
	return proof, nil
}

func dispatcherSHA256(value []byte) string {
	sum := sha256.Sum256(value)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func normalizeDispatcherHash(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return ""
	}
	if !strings.HasPrefix(value, "sha256:") {
		value = "sha256:" + value
	}
	return value
}

func stringValueFromNestedMap(value map[string]any, parent, key string) string {
	return workerMapString(aiWorkerMap(value[parent]), key)
}
