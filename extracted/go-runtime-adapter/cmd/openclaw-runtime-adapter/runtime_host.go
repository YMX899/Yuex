package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	runtimepkg "huahuoai/backend/source/internal/runtime"
)

type hostManifestSourceResolver struct {
	dataRoot           string
	metaRoot           string
	objectCacheRoot    string
	workspaceTenantID  string
	remoteHTTPClient   *http.Client
	remoteFetchTimeout time.Duration
	now                func() time.Time
}

type runtimeHostPermit struct {
	RunID    string
	Scope    string
	Acquired bool
}

const privateExecutionContextVersion = "huahuo.private-run-context.v1"

type HostAdmissionController struct {
	mu                   sync.Mutex
	maxActiveRuns        int
	maxProductThreadRuns int
	maxDetachedTaskRuns  int
	activeRuns           map[string]string
	recoveryPending      bool
	identityBlocked      bool
}

func NewHostAdmissionController(maxActiveRuns, maxProductThreadRuns, maxDetachedTaskRuns int) *HostAdmissionController {
	return &HostAdmissionController{
		maxActiveRuns: maxActiveRuns, maxProductThreadRuns: maxProductThreadRuns,
		maxDetachedTaskRuns: maxDetachedTaskRuns, activeRuns: map[string]string{},
	}
}

func (c *HostAdmissionController) Validate() error {
	if c == nil || c.maxActiveRuns < 1 || c.maxProductThreadRuns < 1 || c.maxDetachedTaskRuns < 1 ||
		c.maxProductThreadRuns > c.maxActiveRuns || c.maxDetachedTaskRuns > c.maxActiveRuns {
		return fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
	}
	return nil
}

func (c *HostAdmissionController) Acquire(runID, scope string) (runtimeHostPermit, error) {
	runID = strings.TrimSpace(runID)
	scope = strings.TrimSpace(scope)
	if c.Validate() != nil || runID == "" || (scope != "product_thread" && scope != "detached_task") {
		return runtimeHostPermit{}, fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.identityBlocked {
		return runtimeHostPermit{}, fmt.Errorf("RUNTIME_HOST_UNAUTHORIZED")
	}
	if c.recoveryPending {
		return runtimeHostPermit{}, fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
	}
	if currentScope, exists := c.activeRuns[runID]; exists {
		if currentScope != scope {
			return runtimeHostPermit{}, fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
		}
		return runtimeHostPermit{RunID: runID, Scope: scope}, nil
	}
	if len(c.activeRuns) >= c.maxActiveRuns {
		return runtimeHostPermit{}, fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
	}
	scopeCount := 0
	for _, currentScope := range c.activeRuns {
		if currentScope == scope {
			scopeCount++
		}
	}
	scopeLimit := c.maxDetachedTaskRuns
	if scope == "product_thread" {
		scopeLimit = c.maxProductThreadRuns
	}
	if scopeCount >= scopeLimit {
		return runtimeHostPermit{}, fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
	}
	c.activeRuns[runID] = scope
	return runtimeHostPermit{RunID: runID, Scope: scope, Acquired: true}, nil
}

// HoldForRecovery prevents production admission until startup reconciliation
// has rebuilt durable occupancy and verified the Backend reservation fence.
func (c *HostAdmissionController) HoldForRecovery() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.identityBlocked {
		c.recoveryPending = true
	}
}

// MarkReadyAfterRecovery opens admission only after an explicit successful
// recovery. A revoked identity remains permanently blocked for this process.
func (c *HostAdmissionController) MarkReadyAfterRecovery() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.identityBlocked {
		c.recoveryPending = false
	}
}

// StageRecoveryPermits atomically rebuilds occupancy from an already verified
// Backend/Gateway fact set. It never opens admission itself; Complete CAS must
// succeed before MarkReadyAfterRecovery can expose these permits to dispatch.
func (c *HostAdmissionController) StageRecoveryPermits(facts []runtimepkg.RuntimeHostRecoveryFact) error {
	if c == nil || c.Validate() != nil {
		return fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.identityBlocked || !c.recoveryPending || len(c.activeRuns) != 0 {
		return fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
	}
	staged := make(map[string]string, len(facts))
	productThread := 0
	detachedTask := 0
	for _, fact := range facts {
		if strings.TrimSpace(fact.RunID) == "" || !runtimeHostRecoveryFactStageable(fact) {
			return fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
		}
		if _, exists := staged[fact.RunID]; exists {
			return fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
		}
		staged[fact.RunID] = fact.ExecutionScope
		switch fact.ExecutionScope {
		case "product_thread":
			productThread++
		case "detached_task":
			detachedTask++
		}
	}
	if len(staged) > c.maxActiveRuns || productThread > c.maxProductThreadRuns || detachedTask > c.maxDetachedTaskRuns {
		return fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
	}
	c.activeRuns = staged
	return nil
}

// ClearRecoveryPermits discards staged restart occupancy after any failed or
// changed attestation. It is intentionally a no-op once admission is ready so
// a later recovery error cannot erase live process permits.
func (c *HostAdmissionController) ClearRecoveryPermits() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.recoveryPending {
		c.activeRuns = map[string]string{}
	}
}

func runtimeHostRecoveryFactStageable(fact runtimepkg.RuntimeHostRecoveryFact) bool {
	if fact.ExecutionScope != "product_thread" && fact.ExecutionScope != "detached_task" || fact.LastEventSequence < 0 {
		return false
	}
	switch fact.Status {
	case "reserved", "created", "sent", "submit_unknown", "retry_same_host", "accepted", "materializing", "running", "finalizing", "recovering":
		return true
	default:
		return false
	}
}

// Block prevents new dispatch admission after the Backend rejects this Host's
// certificate-bound identity. Existing runs retain their permits so their
// terminal/abort paths can converge, but a fresh process or identity reload is
// required before another Run may start.
func (c *HostAdmissionController) Block() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.identityBlocked = true
	c.recoveryPending = true
}

func (c *HostAdmissionController) Release(runID string) bool {
	if c == nil || strings.TrimSpace(runID) == "" {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.activeRuns[runID]; !exists {
		return false
	}
	delete(c.activeRuns, runID)
	return true
}

func (c *HostAdmissionController) Snapshot() (total, productThread, detached int) {
	if c == nil {
		return 0, 0, 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, scope := range c.activeRuns {
		total++
		if scope == "product_thread" {
			productThread++
		} else if scope == "detached_task" {
			detached++
		}
	}
	return total, productThread, detached
}

func (a *adapter) validateLocalRunCapacity() error {
	if a == nil || a.admission == nil {
		return fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
	}
	return a.admission.Validate()
}

func (a *adapter) acquireRunPermit(runID, scope string) (runtimeHostPermit, error) {
	if a == nil || a.admission == nil {
		return runtimeHostPermit{}, fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
	}
	return a.admission.Acquire(runID, scope)
}

func (a *adapter) blockRuntimeHostDispatch() {
	if a == nil || a.admission == nil {
		return
	}
	a.admission.Block()
}

func (a *adapter) releaseRunPermit(runID string) bool {
	if a == nil || a.admission == nil {
		return false
	}
	return a.admission.Release(runID)
}

func newHostManifestSourceResolver() hostManifestSourceResolver {
	dataRoot := firstAdapterNonEmpty(os.Getenv("HUAHUO_DATA_ROOT"), os.Getenv("DATA_ROOT"), "/home/data/huahuo")
	return hostManifestSourceResolver{
		dataRoot:           dataRoot,
		metaRoot:           firstAdapterNonEmpty(os.Getenv("HUAHUO_RUNTIME_CONFIG_ROOT"), os.Getenv("RUNTIME_CONFIG_ROOT"), "/home/huahuo-runtime/config"),
		objectCacheRoot:    firstAdapterNonEmpty(os.Getenv("HUAHUO_RUNTIME_OBJECT_CACHE_ROOT"), os.Getenv("RUNTIME_OBJECT_CACHE_ROOT"), dataRoot),
		workspaceTenantID:  firstAdapterNonEmpty(os.Getenv("HUAHUO_WORKSPACE_TENANT_ID"), os.Getenv("WORKSPACE_TENANT_ID")),
		remoteFetchTimeout: time.Duration(envInt("HUAHUO_RUNTIME_OBJECT_READ_TIMEOUT_SECONDS", 30)) * time.Second,
		now:                time.Now,
	}
}

func (r hostManifestSourceResolver) Resolve(ctx context.Context, manifest runtimepkg.RuntimeInputManifest, entry runtimepkg.RuntimeManifestEntry) ([]byte, error) {
	var root string
	switch entry.SourceType {
	case "formal_workspace_ref":
		tenantID := r.workspaceTenantID
		if tenantID == "" {
			tenantID = manifest.TenantID
		}
		root = filepath.Join(r.dataRoot, "workspaces", "tenants", safeSegment(tenantID), "users", safeSegment(manifest.UserID), "workspaces", safeSegment(manifest.WorkspaceID))
	case "meta_release_ref":
		root = r.metaRoot
	case "object_ref":
		if entry.ObjectRead != nil {
			return r.fetchRemoteObject(ctx, manifest, entry)
		}
		root = r.objectCacheRoot
	default:
		return nil, fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
	}
	return readManifestSource(root, entry.SourceRef)
}

// fetchRemoteObject turns a Dispatcher-issued, provider-signed read URL into
// a Host-local immutable cache file. It deliberately performs all checks
// before the generic materializer exposes a path to OpenClaw.
func (r hostManifestSourceResolver) fetchRemoteObject(ctx context.Context, manifest runtimepkg.RuntimeInputManifest, entry runtimepkg.RuntimeManifestEntry) ([]byte, error) {
	if entry.ObjectRead == nil || entry.SizeBytes < 1 || !adapterValidSHA256(entry.SHA256) ||
		!validRemoteObjectReadURL(entry.ObjectRead.URL) || !validRemoteObjectReadMIME(entry.ObjectRead.MIMEType) {
		return nil, fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
	}
	now := time.Now().UTC()
	if r.now != nil {
		now = r.now().UTC()
	}
	if !entry.ObjectRead.ExpiresAt.After(now) || entry.ObjectRead.ExpiresAt.After(manifest.ExpiresAt) {
		return nil, fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
	}
	cacheRoot, err := safeRuntimeObjectCacheRoot(r.objectCacheRoot)
	if err != nil {
		return nil, fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
	}
	cachePath := filepath.Join(cacheRoot, "sha256-"+strings.TrimPrefix(strings.ToLower(entry.SHA256), "sha256:")+".blob")
	if cached, ok := validCachedRuntimeObject(cachePath, entry); ok {
		return cached, nil
	}
	if _, err := os.Lstat(cachePath); err == nil {
		// Never overwrite an unexpected or corrupt cache entry; it might have
		// been tampered with outside the Adapter process.
		return nil, fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
	}
	timeout := r.remoteFetchTimeout
	if timeout <= 0 || timeout > 2*time.Minute {
		timeout = 30 * time.Second
	}
	fetchCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	request, err := http.NewRequestWithContext(fetchCtx, http.MethodGet, entry.ObjectRead.URL, nil)
	if err != nil {
		return nil, fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
	}
	request.Header.Set("Accept", entry.ObjectRead.MIMEType)
	request.Header.Set("Accept-Encoding", "identity")
	response, err := r.remoteObjectHTTPClient(timeout).Do(request)
	if err != nil {
		return nil, fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK || response.Request == nil || response.Request.URL == nil || response.Request.URL.String() != request.URL.String() ||
		response.Header.Get("Content-Encoding") != "" || (response.ContentLength >= 0 && response.ContentLength != entry.SizeBytes) ||
		canonicalRemoteObjectMIME(response.Header.Get("Content-Type")) != entry.ObjectRead.MIMEType {
		return nil, fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
	}
	temporary, err := os.CreateTemp(cacheRoot, ".object-fetch-")
	if err != nil {
		return nil, fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
	}
	temporaryName := temporary.Name()
	defer func() { _ = os.Remove(temporaryName) }()
	hash := sha256.New()
	written, copyErr := io.Copy(io.MultiWriter(temporary, hash), io.LimitReader(response.Body, entry.SizeBytes+1))
	closeErr := temporary.Close()
	if copyErr != nil || closeErr != nil || written != entry.SizeBytes || "sha256:"+hex.EncodeToString(hash.Sum(nil)) != normalizeAdapterSHA256(entry.SHA256) {
		return nil, fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
	}
	if err := os.Chmod(temporaryName, 0o440); err != nil {
		return nil, fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
	}
	if err := os.Rename(temporaryName, cachePath); err != nil {
		if cached, ok := validCachedRuntimeObject(cachePath, entry); ok {
			return cached, nil
		}
		return nil, fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
	}
	return os.ReadFile(cachePath)
}

func (r hostManifestSourceResolver) remoteObjectHTTPClient(timeout time.Duration) *http.Client {
	client := r.remoteHTTPClient
	if client == nil {
		client = &http.Client{}
	}
	copyClient := *client
	copyClient.Timeout = timeout
	copyClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &copyClient
}

func safeRuntimeObjectCacheRoot(root string) (string, error) {
	absRoot, err := filepath.Abs(strings.TrimSpace(root))
	if err != nil || strings.TrimSpace(root) == "" {
		return "", fmt.Errorf("invalid object cache root")
	}
	info, err := os.Lstat(absRoot)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return "", fmt.Errorf("invalid object cache root")
	}
	cacheRoot := filepath.Join(absRoot, "runtime-object-cache")
	if err := os.Mkdir(cacheRoot, 0o750); err != nil && !os.IsExist(err) {
		return "", err
	}
	info, err = os.Lstat(cacheRoot)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return "", fmt.Errorf("invalid object cache root")
	}
	return cacheRoot, nil
}

func validCachedRuntimeObject(path string, entry runtimepkg.RuntimeManifestEntry) ([]byte, bool) {
	info, err := os.Lstat(path)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Size() != entry.SizeBytes {
		return nil, false
	}
	content, err := os.ReadFile(path)
	if err != nil || int64(len(content)) != entry.SizeBytes {
		return nil, false
	}
	hash := sha256.Sum256(content)
	if "sha256:"+hex.EncodeToString(hash[:]) != normalizeAdapterSHA256(entry.SHA256) {
		return nil, false
	}
	return content, true
}

func validRemoteObjectReadURL(raw string) bool {
	if raw == "" || raw != strings.TrimSpace(raw) || len(raw) > 8192 {
		return false
	}
	parsed, err := url.Parse(raw)
	return err == nil && strings.EqualFold(parsed.Scheme, "https") && parsed.Host != "" && parsed.User == nil && parsed.Opaque == "" &&
		parsed.Fragment == "" && parsed.RawQuery != "" && parsed.Path != ""
}

func validRemoteObjectReadMIME(value string) bool {
	return canonicalRemoteObjectMIME(value) != ""
}

func canonicalRemoteObjectMIME(value string) string {
	parsed, _, err := mime.ParseMediaType(strings.TrimSpace(value))
	if err != nil {
		return ""
	}
	switch strings.ToLower(parsed) {
	case "image/jpeg", "image/png", "image/webp":
		return strings.ToLower(parsed)
	default:
		return ""
	}
}

func adapterValidSHA256(value string) bool {
	value = strings.TrimPrefix(strings.ToLower(strings.TrimSpace(value)), "sha256:")
	if len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func normalizeAdapterSHA256(value string) string {
	return "sha256:" + strings.TrimPrefix(strings.ToLower(strings.TrimSpace(value)), "sha256:")
}

func readManifestSource(root, sourceRef string) ([]byte, error) {
	root = strings.TrimSpace(root)
	ref := strings.TrimSpace(sourceRef)
	if root == "" || ref == "" || filepath.IsAbs(ref) || strings.Contains(ref, "..") || strings.ContainsAny(ref, ":\\") {
		return nil, fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
	}
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
	}
	target := filepath.Join(absRoot, filepath.FromSlash(ref))
	rel, err := filepath.Rel(absRoot, target)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return nil, fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
	}
	resolvedRoot, err := filepath.EvalSymlinks(absRoot)
	if err != nil {
		return nil, fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
	}
	resolvedTarget, err := filepath.EvalSymlinks(target)
	if err != nil {
		return nil, fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
	}
	resolvedRel, err := filepath.Rel(resolvedRoot, resolvedTarget)
	if err != nil || resolvedRel == ".." || strings.HasPrefix(resolvedRel, ".."+string(filepath.Separator)) {
		return nil, fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
	}
	info, err := os.Stat(resolvedTarget)
	if err != nil || !info.Mode().IsRegular() {
		return nil, fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
	}
	return os.ReadFile(resolvedTarget)
}

func (a *adapter) prepareAsyncSubmit(ctx context.Context, authorization string, params map[string]any) (map[string]any, runtimeHostPermit, error) {
	var request runtimepkg.AsyncRuntimeSubmitRequest
	raw, err := json.Marshal(params)
	if err != nil || json.Unmarshal(raw, &request) != nil {
		return nil, runtimeHostPermit{}, fmt.Errorf("RUNTIME_INPUT_INVALID")
	}
	ticket := runTicketFromAuthorization(authorization)
	if request.RunID == "" || request.ReservationID == "" || request.FencingToken < 1 || request.CapabilityHash == "" ||
		strings.TrimSpace(request.InputMessage) == "" || strings.TrimSpace(request.RuntimeConfigID) == "" ||
		!runtimepkg.ValidRuntimeSubmitConfigVersion(request.RuntimeConfigVersion) || ticket == "" {
		return nil, runtimeHostPermit{}, fmt.Errorf("RUNTIME_INPUT_INVALID")
	}
	threadID, sessionKey, sessionOK := adapterSubmitProductSessionIdentity(request.ProductSessionRef)
	if !sessionOK || request.Plan.AgentRunID == "" || request.Plan.AgentRunID != request.RunID {
		return nil, runtimeHostPermit{}, fmt.Errorf("RUNTIME_INPUT_INVALID")
	}
	message := request.InputMessage
	if len([]rune(message)) > adapterInputMaxRunes() {
		return nil, runtimeHostPermit{}, fmt.Errorf("RUNTIME_INPUT_INVALID")
	}
	request.RunTicket = ticket
	claims, err := runtimepkg.VerifyRunTicket(ticket, a.runTicketSecret, time.Now().UTC())
	if err != nil || claims.RunID != request.RunID || claims.TenantID == "" || claims.TenantID != request.InputManifest.TenantID ||
		claims.ReservationID != request.ReservationID ||
		claims.RuntimeHostID != a.runtimeHostID || claims.FencingToken != request.FencingToken ||
		claims.CapabilityHash != request.CapabilityHash || claims.WorkspaceID != request.InputManifest.WorkspaceID ||
		claims.WorkspaceVersion != request.InputManifest.WorkspaceVersion || claims.ContextGeneration != request.InputManifest.ContextGeneration ||
		claims.InputManifestHash == "" || claims.InputManifestHash != request.InputManifest.ManifestHash || claims.JTI == "" || claims.PlanHash == "" ||
		claims.SubmitBinding == nil || claims.SubmitBinding.Version != runtimepkg.RuntimeSubmitBindingV2 ||
		claims.SubmitBinding.InputMessageHash != runtimepkg.RunTicketInputMessageHash(request.InputMessage) ||
		claims.SubmitBinding.RuntimeConfigID != request.RuntimeConfigID ||
		claims.SubmitBinding.RuntimeConfigVersion != request.RuntimeConfigVersion ||
		claims.SubmitBinding.ProductSessionHash != runtimepkg.RunTicketProductSessionHash(threadID, sessionKey) {
		log.Printf("runtime submit authorization rejected stage=run_ticket run=%s reservation=%s", request.RunID, request.ReservationID)
		return nil, runtimeHostPermit{}, fmt.Errorf("RUNTIME_PERMISSION_DENIED")
	}
	// RuntimeConfigID is part of the signed Plan hash. RuntimeConfigVersion is
	// part of the signed submit binding. Neither can be overridden by a body
	// replay after the Dispatcher has issued the ticket.
	if request.RuntimeConfigID != request.Plan.RuntimeConfigID {
		log.Printf("runtime submit authorization rejected stage=runtime_config run=%s reservation=%s", request.RunID, request.ReservationID)
		return nil, runtimeHostPermit{}, fmt.Errorf("RUNTIME_PERMISSION_DENIED")
	}
	planHash, err := runtimepkg.ComputeAgentRunPlanHash(request.Plan)
	if err != nil || planHash != claims.PlanHash {
		log.Printf("runtime submit authorization rejected stage=plan_hash run=%s ticket_plan_hash=%s request_plan_hash=%s", request.RunID, claims.PlanHash, planHash)
		return nil, runtimeHostPermit{}, fmt.Errorf("RUNTIME_PERMISSION_DENIED")
	}
	if request.Plan.L1AgentProfile != request.InputManifest.AgentProfile ||
		request.Plan.MetaWorkspaceKey != request.InputManifest.MetaWorkspaceKey ||
		request.Plan.MetaWorkspaceVersion != request.InputManifest.MetaWorkspaceVersion ||
		request.Plan.InputPolicyHash != request.InputManifest.InputPolicyHash ||
		!runtimepkg.SameAgentRunInputAttachmentIdentities(request.Plan.InputAttachments, request.InputManifest.Attachments) {
		log.Printf("runtime submit authorization rejected stage=meta_workspace_identity run=%s", request.RunID)
		return nil, runtimeHostPermit{}, fmt.Errorf("RUNTIME_PERMISSION_DENIED")
	}
	dispatchIdentity := adapterStableSHA256(fmt.Sprintf("%s\x00%s\x00%d\x00%s\x00%s", request.RunID,
		request.ReservationID, request.FencingToken, a.runtimeHostID, request.CapabilityHash))
	policyNow := time.Now().UTC()
	runtimePolicy, err := a.signRuntimePolicy(request.Plan, claims, request.InputManifest.ManifestHash, dispatchIdentity, planHash, policyNow)
	if err != nil {
		return nil, runtimeHostPermit{}, err
	}
	if err := a.verifyRuntimePolicyForGateway(runtimePolicy, request.Plan, claims, request.InputManifest.ManifestHash, dispatchIdentity, planHash, policyNow); err != nil {
		log.Printf("runtime submit authorization rejected stage=runtime_policy run=%s reservation=%s", request.RunID, request.ReservationID)
		return nil, runtimeHostPermit{}, err
	}
	permit, err := a.acquireRunPermit(request.RunID, request.Plan.ExecutionScope)
	if err != nil {
		return nil, runtimeHostPermit{}, err
	}
	keepPermit := false
	defer func() {
		if permit.Acquired && !keepPermit {
			a.releaseRunPermit(permit.RunID)
		}
	}()
	materialized, err := a.materializer.Materialize(ctx, ticket, request.InputManifest)
	if err != nil {
		if errors.Is(err, runtimepkg.ErrRunTicketJTIStoreUnavailable) {
			return nil, runtimeHostPermit{}, fmt.Errorf("RUNTIME_STORAGE_UNAVAILABLE")
		}
		return nil, runtimeHostPermit{}, fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
	}
	workspace := map[string]any{"realPath": materialized.Root, "accessMode": runtimePolicy.WorkspaceAccessMode}
	if runtimePolicy.WriteLease != nil {
		workspace["writeLease"] = runtimePolicy.WriteLease
	}
	input := map[string]any{"message": message}
	if len(request.InputManifest.Attachments) > 0 {
		attachments := make([]map[string]any, 0, len(request.InputManifest.Attachments))
		for _, attachment := range request.InputManifest.Attachments {
			attachmentPath, pathErr := materializedRuntimeAttachmentPath(materialized.Root, attachment.LogicalPath)
			if pathErr != nil {
				return nil, runtimeHostPermit{}, pathErr
			}
			attachments = append(attachments, map[string]any{"name": attachment.ResourceID, "path": attachmentPath, "kind": "image"})
		}
		input["attachments"] = attachments
	}
	spec := map[string]any{
		"runId": request.RunID, "tenantId": request.InputManifest.TenantID,
		"userId": request.InputManifest.UserID, "workspaceId": request.InputManifest.WorkspaceID,
		"threadId": threadID, "runtimeConfigId": request.RuntimeConfigID,
		"workspace": workspace,
		"productSession": map[string]any{
			// The Gateway session identity is limited to this stable Backend-owned
			// pair. Routing, prompt and output metadata belongs to the durable Run
			// audit, never to a model-visible Product Session envelope.
			"threadId": threadID, "openclawSessionKey": sessionKey,
		},
		"tools":         map[string]any{"allow": append([]string{}, runtimePolicy.AllowedTools...)},
		"runtimePolicy": runtimePolicyMap(runtimePolicy),
		"input":         input,
	}
	spec["runtimeConfigVersion"] = request.RuntimeConfigVersion
	if runtimeBody := a.runtimeBody(nil); len(runtimeBody) > 0 {
		spec["runtime"] = runtimeBody
	}
	keepPermit = true
	return map[string]any{
		"spec": spec, "idempotencyKey": request.RunID,
		"workspaceManifestHash": request.InputManifest.ManifestHash,
		"runTicketJtiHash":      adapterStableSHA256(claims.JTI),
		"dispatchIdentity":      dispatchIdentity,
		"reservationId":         request.ReservationID, "fencingToken": request.FencingToken,
		"capabilityHash": request.CapabilityHash, "runtimeHostId": a.runtimeHostID,
		"privateExecutionContext": map[string]any{
			"version":   privateExecutionContextVersion,
			"runTicket": ticket,
		},
	}, permit, nil
}

func adapterSubmitProductSessionIdentity(input map[string]any) (string, string, bool) {
	threadID, threadOK := input["threadId"].(string)
	sessionKey, sessionOK := input["openclawSessionKey"].(string)
	if !threadOK || !sessionOK || !validAdapterSubmitSessionValue(threadID, 256) || !validAdapterSubmitSessionValue(sessionKey, 1024) {
		return "", "", false
	}
	return threadID, sessionKey, true
}

func validAdapterSubmitSessionValue(value string, maximum int) bool {
	if value == "" || value != strings.TrimSpace(value) || len(value) > maximum {
		return false
	}
	for _, character := range value {
		if (character >= 'A' && character <= 'Z') || (character >= 'a' && character <= 'z') ||
			(character >= '0' && character <= '9') || character == '_' || character == '-' || character == '.' || character == ':' {
			continue
		}
		return false
	}
	return true
}

func materializedRuntimeAttachmentPath(root, logicalPath string) (string, error) {
	root = strings.TrimSpace(root)
	logicalPath = strings.TrimSpace(logicalPath)
	if root == "" || logicalPath == "" || filepath.IsAbs(logicalPath) || strings.Contains(logicalPath, "..") || strings.ContainsAny(logicalPath, ":\\") {
		return "", fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
	}
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return "", fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
	}
	target := filepath.Join(absRoot, filepath.FromSlash(logicalPath))
	relative, err := filepath.Rel(absRoot, target)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
	}
	info, err := os.Stat(target)
	if err != nil || !info.Mode().IsRegular() {
		return "", fmt.Errorf("RUNTIME_WORKSPACE_MATERIALIZATION_FAILED")
	}
	return target, nil
}

func adapterStableSHA256(value string) string {
	sum := sha256.Sum256([]byte(value))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func (a *adapter) authorizeAsyncRun(authorization, runID string) error {
	ticket := runTicketFromAuthorization(authorization)
	if ticket == "" || runID == "" || a.runTicketSecret == "" || a.runtimeHostID == "" {
		return fmt.Errorf("RUNTIME_PERMISSION_DENIED")
	}
	claims, err := runtimepkg.VerifyRunTicket(ticket, a.runTicketSecret, time.Now().UTC())
	if err != nil || claims.RunID != runID || claims.RuntimeHostID != a.runtimeHostID {
		return fmt.Errorf("RUNTIME_PERMISSION_DENIED")
	}
	return nil
}

// prepareAsyncAbort accepts only the bound async-abort contract. Status and
// event reads are authorized by run/Host, while an abort must also prove the
// exact reservation and fencing token from its RunTicket before Gateway I/O.
func (a *adapter) prepareAsyncAbort(authorization string, params map[string]any) (map[string]any, error) {
	var request runtimepkg.AsyncRuntimeAbortRequest
	raw, err := json.Marshal(params)
	if err != nil || json.Unmarshal(raw, &request) != nil || request.RunID == "" || request.ReservationID == "" || request.FencingToken < 1 {
		return nil, fmt.Errorf("RUNTIME_INPUT_INVALID")
	}
	ticket := runTicketFromAuthorization(authorization)
	if ticket == "" || a.runTicketSecret == "" || a.runtimeHostID == "" {
		return nil, fmt.Errorf("RUNTIME_PERMISSION_DENIED")
	}
	claims, err := runtimepkg.VerifyRunTicket(ticket, a.runTicketSecret, time.Now().UTC())
	if err != nil || claims.RunID != request.RunID || claims.ReservationID != request.ReservationID ||
		claims.RuntimeHostID != a.runtimeHostID || claims.FencingToken != request.FencingToken {
		return nil, fmt.Errorf("RUNTIME_PERMISSION_DENIED")
	}
	return map[string]any{
		"runId": request.RunID, "reservationId": request.ReservationID,
		"fencingToken": request.FencingToken, "reason": request.Reason,
	}, nil
}

func runTicketFromAuthorization(value string) string {
	const prefix = "RunTicket "
	value = strings.TrimSpace(value)
	if !strings.HasPrefix(value, prefix) {
		return ""
	}
	return strings.TrimSpace(strings.TrimPrefix(value, prefix))
}
