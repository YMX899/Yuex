package runtime

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	RuntimeErrorProviderConfigMissing = "PROVIDER_CONFIG_MISSING"
	RuntimeErrorProviderAuthFailed    = "PROVIDER_AUTH_FAILED"
	RuntimeErrorModelInputUnsupported = "MODEL_INPUT_UNSUPPORTED"
	RuntimeErrorAttachmentInvalid     = "ATTACHMENT_INVALID"
	RuntimeErrorWorkspaceForbidden    = "WORKSPACE_FORBIDDEN"
	RuntimeErrorTimeout               = "RUNTIME_TIMEOUT"
	RuntimeErrorToolLoopDetected      = "RUNTIME_TOOL_LOOP_DETECTED"
	RuntimeErrorToolBudgetExceeded    = "RUNTIME_TOOL_BUDGET_EXCEEDED"
	RuntimeErrorRunStalled            = "RUNTIME_RUN_STALLED"
	RuntimeErrorInputInvalid          = "RUNTIME_INPUT_INVALID"
	RuntimeErrorAIResultParseFailed   = "AI_RESULT_PARSE_FAILED"
	RuntimeErrorFailed                = "RUNTIME_FAILED"

	runtimeTimeoutHeader      = "X-Huahuo-Runtime-Timeout-Sec"
	runtimeMaxToolCallsHeader = "X-Huahuo-Runtime-Max-Tool-Calls"
	// The Runtime Host capability contract currently has a hard 200-call
	// limit and a one-hour protection window. Keeping the transport limits
	// here prevents an untrusted caller from widening a signed Run policy by
	// sending a hand-written HTTP header.
	runtimeMaximumTimeoutSeconds = 3600
)

type RuntimeRunSpec struct {
	RunID                string
	TenantID             string
	UserID               string
	WorkspaceID          string
	ThreadID             string
	RuntimeConfigID      string
	RuntimeConfigVersion string
	Workspace            RuntimeWorkspaceSpec
	ProductSession       RuntimeProductSessionSpec
	ModelOverride        RuntimeModelOverrideSpec
	Tools                RuntimeToolsSpec
	Plugins              RuntimePluginsSpec
	Runtime              RuntimeExecutionSpec
	Input                RuntimeInputSpec

	// Legacy Huahuo fields are accepted only as compatibility inputs. They are
	// normalized before transport and must never be sent to OpenClaw.
	WorkspaceDir string
	InputMessage string
	AuthPoolID   string
	MaxToolCalls int
	TimeoutSec   int
}

type RuntimeWorkspaceSpec struct {
	RealPath   string                      `json:"realPath"`
	AccessMode string                      `json:"accessMode"`
	WriteLease *RuntimeWorkspaceWriteLease `json:"writeLease,omitempty"`
}

type RuntimeProductSessionSpec struct {
	ThreadID           string         `json:"threadId"`
	OpenClawSessionKey string         `json:"openclawSessionKey"`
	Metadata           map[string]any `json:"metadata,omitempty"`
}

type RuntimeModelOverrideSpec struct {
	ModelProfileID string         `json:"modelProfileId,omitempty"`
	Provider       string         `json:"provider,omitempty"`
	Model          string         `json:"model,omitempty"`
	Fallbacks      []string       `json:"fallbacks,omitempty"`
	AuthPoolID     string         `json:"authPoolId,omitempty"`
	TimeoutSeconds int            `json:"timeoutSeconds,omitempty"`
	Thinking       string         `json:"thinking,omitempty"`
	Reasoning      string         `json:"reasoning,omitempty"`
	MaxTokens      int            `json:"maxTokens,omitempty"`
	Params         map[string]any `json:"params,omitempty"`
}

type RuntimeToolsSpec struct {
	ProfileID string   `json:"profileId,omitempty"`
	Allow     []string `json:"allow,omitempty"`
	Deny      []string `json:"deny,omitempty"`
}

type RuntimePluginsSpec struct {
	Enabled  []string `json:"enabled,omitempty"`
	Disabled []string `json:"disabled,omitempty"`
}

type RuntimeExecutionSpec struct {
	StateDir   string `json:"stateDir,omitempty"`
	ConfigPath string `json:"configPath,omitempty"`
	LogsDir    string `json:"logsDir,omitempty"`
	TmpRoot    string `json:"tmpRoot,omitempty"`
}

type RuntimeInputSpec struct {
	Message     string              `json:"message"`
	Attachments []RuntimeAttachment `json:"attachments,omitempty"`
}

type RuntimeAttachment struct {
	Name string `json:"name"`
	Path string `json:"path"`
	Kind string `json:"kind,omitempty"`
}

type RuntimeRunResult struct {
	RunID               string         `json:"runId"`
	Status              string         `json:"status"`
	FinalAnswer         string         `json:"finalAnswer"`
	Usage               map[string]any `json:"usage"`
	ErrorCode           string         `json:"errorCode,omitempty"`
	DownstreamRequestID string         `json:"downstreamRequestId,omitempty"`
	HTTPStatus          int            `json:"httpStatus,omitempty"`
	ProviderCode        string         `json:"providerCode,omitempty"`
	ProviderMessage     string         `json:"providerMessage,omitempty"`
	ProviderRequestID   string         `json:"providerRequestId,omitempty"`
	ErrorSummary        map[string]any `json:"errorSummary,omitempty"`
	SessionSummary      string         `json:"sessionSummary,omitempty"`
	Logs                []string       `json:"logs,omitempty"`
}

type RuntimeDiagnosticError struct {
	Code                string
	SafeMessage         string
	HTTPStatus          int
	ProviderCode        string
	ProviderMessage     string
	ProviderRequestID   string
	DownstreamRequestID string
	Status              string
}

func (e *RuntimeDiagnosticError) Error() string {
	if e == nil {
		return RuntimeErrorFailed
	}
	return NormalizeRuntimeFailureCode(e.Code)
}

func (e *RuntimeDiagnosticError) Summary() map[string]any {
	if e == nil {
		return runtimeFailureSummary(RuntimeErrorFailed, "", 0, "", "", "", "")
	}
	summary := runtimeFailureSummary(e.Code, e.Status, e.HTTPStatus, e.ProviderCode, e.ProviderMessage, e.ProviderRequestID, e.DownstreamRequestID)
	if e.SafeMessage != "" {
		summary["safeMessage"] = sanitizeRuntimeSummaryText(e.SafeMessage)
	}
	return summary
}

type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

type HTTPTransportOpenClawClient struct {
	Endpoint               string
	AuthRef                string
	DefaultRuntimeConfigID string
	DefaultTenantID        string
	RuntimeConfigPath      string
	RuntimeStateDir        string
	RuntimeLogsDir         string
	RuntimeTmpRoot         string
	HTTPClient             HTTPClient
	RequireRuntimeHostMTLS bool
	Timeout                time.Duration
	// submitCapture is intentionally not exported. Only the protected B4
	// factory may attach a pre-I/O capture sink to a transport instance.
	submitCapture RuntimeSubmitCaptureSink
}

// HTTPTransportOpenClawClient is shared by capability discovery and the async
// RuntimeHost submit/status/events/abort protocol.
func NewHTTPTransportOpenClawClient(endpoint, authRef string, client HTTPClient) HTTPTransportOpenClawClient {
	return HTTPTransportOpenClawClient{Endpoint: strings.TrimRight(endpoint, "/"), AuthRef: authRef, DefaultTenantID: "huahuo-prelaunch", HTTPClient: client, Timeout: 3600 * time.Second}
}

func (c HTTPTransportOpenClawClient) CanonicalRunSpec(spec RuntimeRunSpec) RuntimeRunSpec {
	canonical := spec
	canonical.TenantID = firstRuntimeNonEmpty(spec.TenantID, c.DefaultTenantID, "huahuo-prelaunch")
	canonical.UserID = strings.TrimSpace(spec.UserID)
	canonical.WorkspaceID = strings.TrimSpace(spec.WorkspaceID)
	canonical.ThreadID = strings.TrimSpace(spec.ThreadID)
	canonical.RuntimeConfigID = firstRuntimeNonEmpty(spec.RuntimeConfigID, spec.AuthPoolID, c.DefaultRuntimeConfigID)
	// Runtime config version is frozen by the Worker from the configured
	// runtimeConfigs map. Canonicalization must preserve a missing or malformed
	// value so ValidateRunSpec can fail before any transport I/O; it must never
	// synthesize a global "v1" fallback.
	canonical.RuntimeConfigVersion = spec.RuntimeConfigVersion
	canonical.Workspace = RuntimeWorkspaceSpec{
		RealPath:   firstRuntimeNonEmpty(spec.Workspace.RealPath, spec.WorkspaceDir, runtimeWorkspacePath(canonical.UserID, canonical.WorkspaceID)),
		AccessMode: firstRuntimeNonEmpty(spec.Workspace.AccessMode, RuntimeWorkspaceAccessRead),
	}
	// The legacy synchronous transport has no signed per-Run policy channel.
	// It must never turn a writable path into native write authority.
	if canonical.Workspace.AccessMode != RuntimeWorkspaceAccessRead {
		canonical.Workspace.AccessMode = RuntimeWorkspaceAccessRead
	}
	canonical.ProductSession.ThreadID = strings.TrimSpace(spec.ProductSession.ThreadID)
	canonical.ProductSession.OpenClawSessionKey = strings.TrimSpace(spec.ProductSession.OpenClawSessionKey)
	if metadata, valid := canonicalRuntimeProductSessionMetadata(spec.ProductSession.Metadata); valid {
		canonical.ProductSession.Metadata = metadata
	} else {
		canonical.ProductSession.Metadata = nil
	}
	canonical.Runtime = RuntimeExecutionSpec{
		StateDir:   firstRuntimeNonEmpty(spec.Runtime.StateDir, c.RuntimeStateDir),
		ConfigPath: firstRuntimeNonEmpty(spec.Runtime.ConfigPath, c.RuntimeConfigPath),
		LogsDir:    firstRuntimeNonEmpty(spec.Runtime.LogsDir, c.RuntimeLogsDir),
		TmpRoot:    firstRuntimeNonEmpty(spec.Runtime.TmpRoot, c.RuntimeTmpRoot),
	}
	canonical.Input.Message = sanitizeRuntimeInputMessage(firstRuntimeNonEmpty(spec.Input.Message, spec.InputMessage))
	if attachments, valid := canonicalRuntimeAttachments(spec.Input.Attachments); valid {
		canonical.Input.Attachments = attachments
	} else {
		canonical.Input.Attachments = nil
	}
	return canonical
}

// ValidateRunSpec is the fail-closed companion to the legacy compatibility
// canonicalizer. It is not an active Runtime Host transport gate: the ticketed
// async client owns its separate pre-I/O validation. Any future legacy
// transport owner must invoke this helper before I/O rather than treating the
// canonicalizer's omission behavior as authorization.
func (c HTTPTransportOpenClawClient) ValidateRunSpec(spec RuntimeRunSpec) error {
	if _, valid := canonicalRuntimeProductSessionMetadata(spec.ProductSession.Metadata); !valid {
		return fmt.Errorf(RuntimeErrorInputInvalid)
	}
	if _, valid := canonicalRuntimeAttachments(spec.Input.Attachments); !valid {
		return fmt.Errorf(RuntimeErrorInputInvalid)
	}
	canonical := c.CanonicalRunSpec(spec)
	if runtimeSpecMissingRequired(canonical) || canonical.ThreadID != canonical.ProductSession.ThreadID ||
		!validRuntimeProductSessionIdentity(canonical.ProductSession.ThreadID, canonical.ProductSession.OpenClawSessionKey) ||
		!validRuntimeTransportBudget(canonical.TimeoutSec, canonical.MaxToolCalls, false) {
		return fmt.Errorf(RuntimeErrorInputInvalid)
	}
	return nil
}

func (s RuntimeRunSpec) OpenClawParams() map[string]any {
	body := map[string]any{
		"runId":                s.RunID,
		"tenantId":             s.TenantID,
		"userId":               s.UserID,
		"workspaceId":          s.WorkspaceID,
		"threadId":             s.ThreadID,
		"runtimeConfigId":      s.RuntimeConfigID,
		"runtimeConfigVersion": s.RuntimeConfigVersion,
		"workspace": map[string]any{
			"realPath":   s.Workspace.RealPath,
			"accessMode": s.Workspace.AccessMode,
		},
		"productSession": map[string]any{
			"threadId":           s.ProductSession.ThreadID,
			"openclawSessionKey": s.ProductSession.OpenClawSessionKey,
		},
		"input": map[string]any{
			"message": sanitizeRuntimeInputMessage(s.Input.Message),
		},
	}
	if s.Workspace.AccessMode == RuntimeWorkspaceAccessWrite && s.Workspace.WriteLease != nil {
		body["workspace"].(map[string]any)["writeLease"] = s.Workspace.WriteLease
	}
	if metadata, valid := canonicalRuntimeProductSessionMetadata(s.ProductSession.Metadata); valid && len(metadata) > 0 {
		body["productSession"].(map[string]any)["metadata"] = metadata
	}
	if runtimeExecutionConfigured(s.Runtime) {
		runtimeBody := map[string]any{}
		if s.Runtime.StateDir != "" {
			runtimeBody["stateDir"] = s.Runtime.StateDir
		}
		if s.Runtime.ConfigPath != "" {
			runtimeBody["configPath"] = s.Runtime.ConfigPath
		}
		if s.Runtime.LogsDir != "" {
			runtimeBody["logsDir"] = s.Runtime.LogsDir
		}
		if s.Runtime.TmpRoot != "" {
			runtimeBody["tmpRoot"] = s.Runtime.TmpRoot
		}
		body["runtime"] = runtimeBody
	}
	if modelOverrideBody := runtimeModelOverrideBody(s.ModelOverride); len(modelOverrideBody) > 0 {
		body["modelOverride"] = modelOverrideBody
	}
	if toolsBody := runtimeToolsBody(s.Tools); len(toolsBody) > 0 {
		body["tools"] = toolsBody
	}
	if pluginsBody := runtimePluginsBody(s.Plugins); len(pluginsBody) > 0 {
		body["plugins"] = pluginsBody
	}
	if canonicalAttachments, valid := canonicalRuntimeAttachments(s.Input.Attachments); valid && len(canonicalAttachments) > 0 {
		attachments := make([]map[string]any, 0, len(canonicalAttachments))
		for _, attachment := range canonicalAttachments {
			item := map[string]any{"name": attachment.Name, "path": attachment.Path}
			if attachment.Kind != "" {
				item["kind"] = attachment.Kind
			}
			attachments = append(attachments, item)
		}
		body["input"].(map[string]any)["attachments"] = attachments
	}
	return body
}

var runtimeProductSessionMetadataKeys = []string{
	"promptKey",
	"agentKey",
	"workspaceRef",
	"inputFiles",
	"outputContract",
	"messageMode",
	"workspaceMode",
}

// canonicalRuntimeProductSessionMetadata accepts the small, documented
// metadata vocabulary only. This envelope is not a generic escape hatch for
// prompt text, storage URLs, local paths, or worker-private session state.
func canonicalRuntimeProductSessionMetadata(input map[string]any) (map[string]any, bool) {
	if len(input) == 0 {
		return nil, true
	}
	for key := range input {
		if !runtimeProductSessionMetadataKeyAllowed(key) {
			return nil, false
		}
	}
	out := make(map[string]any, len(input))
	for _, key := range runtimeProductSessionMetadataKeys {
		value, exists := input[key]
		if !exists {
			continue
		}
		switch key {
		case "inputFiles":
			files, valid := canonicalRuntimeInputFiles(value)
			if !valid {
				return nil, false
			}
			out[key] = files
		case "outputContract":
			contract, valid := canonicalRuntimeMetadataObject(value, 0)
			if !valid || !validRuntimeOutputContract(contract) {
				return nil, false
			}
			out[key] = contract
		default:
			text, ok := value.(string)
			if !ok || !validRuntimeMetadataIdentifier(text, 256) {
				return nil, false
			}
			out[key] = strings.TrimSpace(text)
		}
	}
	return out, true
}

func runtimeProductSessionMetadataKeyAllowed(key string) bool {
	for _, allowed := range runtimeProductSessionMetadataKeys {
		if key == allowed {
			return true
		}
	}
	return false
}

func canonicalRuntimeInputFiles(value any) ([]string, bool) {
	var raw []string
	switch typed := value.(type) {
	case []string:
		raw = typed
	case []any:
		raw = make([]string, 0, len(typed))
		for _, item := range typed {
			text, ok := item.(string)
			if !ok {
				return nil, false
			}
			raw = append(raw, text)
		}
	default:
		return nil, false
	}
	if len(raw) > 64 {
		return nil, false
	}
	seen := make(map[string]bool, len(raw))
	out := make([]string, 0, len(raw))
	for _, item := range raw {
		logicalPath, valid := canonicalRuntimeLogicalPath(item, false)
		if !valid || seen[logicalPath] {
			return nil, false
		}
		seen[logicalPath] = true
		out = append(out, logicalPath)
	}
	return out, true
}

func canonicalRuntimeMetadataObject(value any, depth int) (map[string]any, bool) {
	if depth > 4 {
		return nil, false
	}
	input, ok := value.(map[string]any)
	if !ok || len(input) == 0 || len(input) > 64 {
		return nil, false
	}
	out := make(map[string]any, len(input))
	for key, item := range input {
		if !validRuntimeMetadataKey(key) || runtimeSensitiveProjectionKey(key) {
			return nil, false
		}
		canonical, valid := canonicalRuntimeMetadataValue(item, depth+1)
		if !valid {
			return nil, false
		}
		out[key] = canonical
	}
	return out, true
}

func canonicalRuntimeMetadataValue(value any, depth int) (any, bool) {
	if depth > 4 {
		return nil, false
	}
	switch typed := value.(type) {
	case string:
		if !validRuntimeMetadataIdentifier(typed, 512) {
			return nil, false
		}
		return strings.TrimSpace(typed), true
	case bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, json.Number:
		return typed, true
	case map[string]any:
		return canonicalRuntimeMetadataObject(typed, depth)
	case []string:
		if len(typed) > 64 {
			return nil, false
		}
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			canonical, valid := canonicalRuntimeMetadataValue(item, depth+1)
			if !valid {
				return nil, false
			}
			out = append(out, canonical.(string))
		}
		return out, true
	case []any:
		if len(typed) > 64 {
			return nil, false
		}
		out := make([]any, 0, len(typed))
		for _, item := range typed {
			canonical, valid := canonicalRuntimeMetadataValue(item, depth+1)
			if !valid {
				return nil, false
			}
			out = append(out, canonical)
		}
		return out, true
	default:
		return nil, false
	}
}

// Metadata is identity/configuration data, never a second prompt channel.
// Keep it deliberately smaller than a general JSON document: values can name
// schemas and registered contracts, but cannot contain prose, paths, URLs, or
// credential-shaped text.
func validRuntimeMetadataIdentifier(value string, maximum int) bool {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > maximum || runtimeTransportForbiddenString(value) {
		return false
	}
	for _, character := range value {
		if (character >= 'A' && character <= 'Z') || (character >= 'a' && character <= 'z') ||
			(character >= '0' && character <= '9') || character == '_' || character == '-' ||
			character == '.' || character == ':' || character == '@' {
			continue
		}
		return false
	}
	return true
}

func validRuntimeOutputContract(contract map[string]any) bool {
	if len(contract) == 0 {
		return false
	}
	for key, value := range contract {
		if runtimeOutputContractForbiddenKey(key) || !validRuntimeOutputContractValue(value) {
			return false
		}
	}
	return true
}

func runtimeOutputContractForbiddenKey(key string) bool {
	normalized := normalizedRuntimeSensitiveKey(key)
	if normalized == "" || runtimeSensitiveProjectionKey(key) {
		return true
	}
	for _, marker := range []string{"instruction", "body", "content", "message", "system", "transcript", "cwd", "directory", "url"} {
		if strings.Contains(normalized, marker) {
			return true
		}
	}
	return false
}

func validRuntimeOutputContractValue(value any) bool {
	switch typed := value.(type) {
	case string:
		return validRuntimeMetadataIdentifier(typed, 512)
	case bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, json.Number:
		return true
	case []string:
		if len(typed) > 64 {
			return false
		}
		for _, item := range typed {
			if !validRuntimeMetadataIdentifier(item, 512) {
				return false
			}
		}
		return true
	case []any:
		if len(typed) > 64 {
			return false
		}
		for _, item := range typed {
			if !validRuntimeOutputContractValue(item) {
				return false
			}
		}
		return true
	case map[string]any:
		return validRuntimeOutputContract(typed)
	default:
		return false
	}
}

func validRuntimeMetadataKey(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > 96 || value != strings.TrimSpace(value) {
		return false
	}
	for _, character := range value {
		if (character >= 'A' && character <= 'Z') || (character >= 'a' && character <= 'z') ||
			(character >= '0' && character <= '9') || character == '_' || character == '-' || character == '.' {
			continue
		}
		return false
	}
	return true
}

func canonicalRuntimeAttachments(input []RuntimeAttachment) ([]RuntimeAttachment, bool) {
	if len(input) > 16 {
		return nil, false
	}
	seenNames := make(map[string]bool, len(input))
	seenPaths := make(map[string]bool, len(input))
	out := make([]RuntimeAttachment, 0, len(input))
	for _, attachment := range input {
		name := strings.TrimSpace(attachment.Name)
		logicalPath, valid := canonicalRuntimeLogicalPath(attachment.Path, true)
		kind := strings.TrimSpace(attachment.Kind)
		if name == "" || len(name) > 256 || runtimeTransportForbiddenString(name) || !valid || seenNames[name] || seenPaths[logicalPath] ||
			(len(kind) > 64 || runtimeTransportForbiddenString(kind)) {
			return nil, false
		}
		seenNames[name] = true
		seenPaths[logicalPath] = true
		out = append(out, RuntimeAttachment{Name: name, Path: logicalPath, Kind: kind})
	}
	return out, true
}

func canonicalRuntimeLogicalPath(value string, attachmentOnly bool) (string, bool) {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > 1024 || runtimeTransportForbiddenString(value) {
		return "", false
	}
	logicalPath, err := normalizeRuntimeLogicalPath(value)
	if err != nil || logicalPath != value {
		return "", false
	}
	if attachmentOnly && !strings.HasPrefix(logicalPath, "input/attachments/") {
		return "", false
	}
	return logicalPath, true
}

func validRuntimeProductSessionIdentity(threadID, sessionKey string) bool {
	threadID = strings.TrimSpace(threadID)
	sessionKey = strings.TrimSpace(sessionKey)
	return validRuntimeSessionIdentityPart(threadID, 256) && validRuntimeSessionIdentityPart(sessionKey, 1024)
}

func validRuntimeSessionIdentityPart(value string, maximum int) bool {
	if value == "" || len(value) > maximum || runtimeTransportForbiddenString(value) {
		return false
	}
	for _, character := range value {
		if (character >= 'A' && character <= 'Z') || (character >= 'a' && character <= 'z') ||
			(character >= '0' && character <= '9') || character == '_' || character == '-' ||
			character == '.' || character == ':' {
			continue
		}
		return false
	}
	return true
}

func validRuntimeTransportBudget(timeoutSec, maxToolCalls int, requireBoth bool) bool {
	if timeoutSec < 0 || maxToolCalls < 0 {
		return false
	}
	if requireBoth && (timeoutSec < 1 || maxToolCalls < 1) {
		return false
	}
	if timeoutSec > runtimeMaximumTimeoutSeconds || maxToolCalls > DefaultRuntimeToolBudgetExecutionContract().HardMaxToolCalls {
		return false
	}
	return true
}

func runtimeTransportForbiddenString(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" {
		return false
	}
	lower := strings.ToLower(strings.ReplaceAll(value, "\\", "/"))
	return strings.HasPrefix(lower, "file:") || strings.Contains(lower, "://") || runtimeTextContainsHostPath(value) || looksLikeSignedRuntimeURL(value) ||
		strings.Contains(lower, "secret://") || runtimeTextContainsSensitiveAssignment(value) || runtimeTextLooksLikeBackendPrompt(value)
}

func looksLikeRuntimeHostPath(value string) bool {
	normalized := strings.Trim(strings.ReplaceAll(value, "\\", "/"), " \t\r\n\"'`.,;:()[]{}<>")
	if normalized == "" {
		return false
	}
	if strings.HasPrefix(normalized, "/") || strings.HasPrefix(normalized, "//") || strings.HasPrefix(normalized, "~") {
		return true
	}
	if len(normalized) >= 3 && ((normalized[0] >= 'A' && normalized[0] <= 'Z') || (normalized[0] >= 'a' && normalized[0] <= 'z')) && normalized[1] == ':' && normalized[2] == '/' {
		return true
	}
	return false
}

func runtimeTextContainsHostPath(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" {
		return false
	}
	lower := strings.ToLower(strings.ReplaceAll(value, "\\", "/"))
	for _, marker := range []string{"/home/data/", "/home/huahuo-runtime/", "/tmp/runtime-workspaces/", "/runtime-workspaces/", "/openclaw/"} {
		if strings.Contains(lower, marker) {
			return true
		}
	}
	for _, token := range strings.Fields(value) {
		if runtimeTokenContainsHostPath(token) {
			return true
		}
	}
	return runtimeTokenContainsHostPath(value)
}

func runtimeTokenContainsHostPath(token string) bool {
	token = strings.Trim(token, " \t\r\n\"'`.,;:()[]{}<>")
	if token == "" {
		return false
	}
	if looksLikeRuntimeHostPath(token) || looksLikeRuntimePath(token) {
		return true
	}
	for _, separator := range []string{"=", ":"} {
		index := strings.Index(token, separator)
		if index < 0 || index+len(separator) >= len(token) {
			continue
		}
		candidate := strings.Trim(token[index+len(separator):], " \t\r\n\"'`.,;:()[]{}<>")
		if looksLikeRuntimeHostPath(candidate) || looksLikeRuntimePath(candidate) {
			return true
		}
	}
	return false
}

func runtimeTextContainsSensitiveAssignment(value string) bool {
	normalized := strings.NewReplacer("_", "", "-", "", ".", "", " ", "", "\t", "", "\r", "", "\n", "").Replace(strings.ToLower(value))
	for _, marker := range []string{"apikey", "xapikey", "authorization", "accesstoken", "refreshtoken", "credential", "runticket", "openclawsessionkey", "sessionkey", "sessiontoken"} {
		if strings.Contains(normalized, marker) {
			return true
		}
	}
	return strings.Contains(normalized, "bearer") || strings.Contains(normalized, "secret=") || strings.Contains(normalized, "cookie=") ||
		strings.Contains(normalized, "session=oc:") || strings.Contains(normalized, "session:oc:")
}

func runtimeTextLooksLikeBackendPrompt(value string) bool {
	lower := strings.ToLower(value)
	for _, marker := range []string{"## loaded effective skill", "<system", "system_prompt", "prompt:", "instructions:", "ignore previous instructions"} {
		if strings.Contains(lower, marker) {
			return true
		}
	}
	return false
}

func (c HTTPTransportOpenClawClient) httpClient() HTTPClient {
	if c.HTTPClient != nil {
		return c.HTTPClient
	}
	return http.DefaultClient
}

func (c HTTPTransportOpenClawClient) NormalizeResult(rawResult map[string]any) RuntimeRunResult {
	return c.NormalizeResultWithBudget(rawResult, 0)
}

func (c HTTPTransportOpenClawClient) NormalizeResultWithBudget(rawResult map[string]any, timeoutBudget int) RuntimeRunResult {
	resultPayload := openclawResultPayload(rawResult)
	status := openclawStringValue(rawResult["status"])
	if status == "" {
		status = openclawStringValue(resultPayload["status"])
	}
	if status == "" {
		status = "failed"
	}
	providerCode, providerMessage, providerRequestID := openclawProviderFieldsFromMap(rawResult)
	nestedProviderCode, nestedProviderMessage, nestedProviderRequestID := openclawProviderFieldsFromMap(resultPayload)
	rawErrorCode := firstRuntimeNonEmpty(openclawStringValue(rawResult["errorCode"]), openclawStringValue(resultPayload["errorCode"]))
	result := RuntimeRunResult{
		RunID:               firstRuntimeNonEmpty(openclawStringValue(rawResult["runId"]), openclawStringValue(resultPayload["runId"])),
		Status:              normalizeRuntimeStatus(status),
		FinalAnswer:         redactRuntimeText(openclawFinalAnswer(rawResult, resultPayload)),
		Usage:               sanitizeRuntimeUsage(firstRuntimeMap(openclawMapValue(rawResult["usage"]), openclawMapValue(resultPayload["usage"]))),
		DownstreamRequestID: sanitizeRuntimeOpaqueIdentifier(firstRuntimeNonEmpty(openclawStringValue(rawResult["downstreamRequestId"]), openclawStringValue(resultPayload["downstreamRequestId"]))),
		HTTPStatus:          firstRuntimeInt(openclawIntValue(rawResult["httpStatus"]), openclawIntValue(resultPayload["httpStatus"])),
		ProviderCode:        firstRuntimeNonEmpty(providerCode, nestedProviderCode),
		ProviderMessage:     firstRuntimeNonEmpty(providerMessage, nestedProviderMessage),
		ProviderRequestID:   sanitizeRuntimeOpaqueIdentifier(firstRuntimeNonEmpty(providerRequestID, nestedProviderRequestID)),
		SessionSummary:      redactRuntimeText(firstRuntimeNonEmpty(openclawStringValue(rawResult["sessionSummary"]), openclawStringValue(resultPayload["sessionSummary"]))),
		Logs:                firstRuntimeLogs(redactRuntimeLogs(rawResult["logs"]), redactRuntimeLogs(resultPayload["logs"])),
	}
	if result.Usage == nil {
		result.Usage = map[string]any{}
	}
	if timeoutBudget > 0 {
		result.Usage["timeoutBudgetSec"] = timeoutBudget
	}
	if result.Status != "succeeded" || result.HTTPStatus >= http.StatusBadRequest {
		result.ErrorCode = classifyOpenClawFailure(result.HTTPStatus, firstRuntimeNonEmpty(rawErrorCode, result.ProviderCode), result.ProviderMessage)
		if result.Status == "timeout" && result.ErrorCode == RuntimeErrorFailed {
			result.ErrorCode = RuntimeErrorTimeout
		}
		if result.Status == "forbidden" && result.ErrorCode == RuntimeErrorFailed && result.HTTPStatus < http.StatusBadRequest {
			result.ErrorCode = RuntimeErrorWorkspaceForbidden
		}
		if result.HTTPStatus >= http.StatusBadRequest {
			result.Status = runtimeFailureStatus(result.ErrorCode)
		}
	} else {
		result.ErrorCode = ""
	}
	if result.Status != "succeeded" {
		result.ErrorSummary = result.SafeErrorSummary()
	}
	return result
}

func openclawResultPayload(raw map[string]any) map[string]any {
	return openclawResultPayloadAtDepth(raw, 0)
}

func openclawResultPayloadAtDepth(raw map[string]any, depth int) map[string]any {
	if len(raw) == 0 || depth > 4 {
		return map[string]any{}
	}
	for _, key := range []string{"result", "output", "response", "payload"} {
		if nested := openclawMapValue(raw[key]); len(nested) > 0 {
			if child := openclawResultPayloadAtDepth(nested, depth+1); len(child) > 0 {
				return mergeRuntimePayloadFallback(child, nested)
			}
			return nested
		}
	}
	for _, key := range []string{"runRecord", "data"} {
		container := openclawMapValue(raw[key])
		if len(container) == 0 {
			continue
		}
		if nested := openclawResultPayloadAtDepth(container, depth+1); len(nested) > 0 {
			return mergeRuntimePayloadFallback(nested, container)
		}
		return container
	}
	return map[string]any{}
}

func openclawFinalAnswer(raw, nested map[string]any) string {
	for _, source := range []map[string]any{raw, nested} {
		for _, key := range []string{"finalAnswer", "answer", "content", "text"} {
			if value := openclawStringValue(source[key]); value != "" {
				return value
			}
		}
		if value := openclawStringValue(source["message"]); value != "" && !strings.HasPrefix(value, "map[") {
			return value
		}
		if message := openclawMapValue(source["message"]); len(message) > 0 {
			for _, key := range []string{"content", "text"} {
				if value := openclawStringValue(message[key]); value != "" {
					return value
				}
			}
		}
		if choice := firstRuntimeMapSliceItem(source["choices"]); len(choice) > 0 {
			if value := openclawStringValue(choice["text"]); value != "" {
				return value
			}
			if message := openclawMapValue(choice["message"]); len(message) > 0 {
				for _, key := range []string{"content", "text"} {
					if value := openclawStringValue(message[key]); value != "" {
						return value
					}
				}
			}
		}
	}
	return ""
}

func (r RuntimeRunResult) SafeErrorSummary() map[string]any {
	return runtimeFailureSummary(r.ErrorCode, r.Status, r.HTTPStatus, r.ProviderCode, r.ProviderMessage, r.ProviderRequestID, r.DownstreamRequestID)
}

func (c HTTPTransportOpenClawClient) MapTransportError(err error) string {
	return MapTransportError(err)
}

func MapTransportError(err error) string {
	if err == nil {
		return ""
	}
	text := strings.ToLower(err.Error())
	switch {
	case strings.Contains(text, "no session found") || strings.Contains(text, "sessions.resolve"):
		return RuntimeErrorProviderConfigMissing
	case strings.Contains(text, "attachment") || strings.Contains(text, "signed url") || strings.Contains(text, "presigned") || strings.Contains(text, "x-amz-signature") || strings.Contains(text, "x-oss-signature"):
		return RuntimeErrorAttachmentInvalid
	case strings.Contains(text, "timeout"), strings.Contains(text, "deadline"):
		return RuntimeErrorTimeout
	case strings.Contains(text, "tool loop"), strings.Contains(text, "loop detected"):
		return RuntimeErrorToolLoopDetected
	case strings.Contains(text, "tool") && (strings.Contains(text, "budget") || strings.Contains(text, "limit") || strings.Contains(text, "exhausted")):
		return RuntimeErrorToolBudgetExceeded
	case strings.Contains(text, "stalled"):
		return RuntimeErrorRunStalled
	case strings.Contains(text, "model") && strings.Contains(text, "input"), strings.Contains(text, "input unsupported"), strings.Contains(text, "unsupported input"), strings.Contains(text, "context length"), strings.Contains(text, "token limit"):
		return RuntimeErrorModelInputUnsupported
	case strings.Contains(text, "workspace") && (strings.Contains(text, "forbidden") || strings.Contains(text, "denied")):
		return RuntimeErrorWorkspaceForbidden
	case strings.Contains(text, "provider_config_missing"), strings.Contains(text, "config"):
		return RuntimeErrorProviderConfigMissing
	case strings.Contains(text, "auth"), strings.Contains(text, "unauthorized"), strings.Contains(text, "forbidden"):
		return RuntimeErrorProviderAuthFailed
	case strings.Contains(text, "parse"), strings.Contains(text, "invalid json"):
		return RuntimeErrorAIResultParseFailed
	default:
		return RuntimeErrorFailed
	}
}

func NormalizeRuntimeFailureCode(code string) string {
	code = strings.ToUpper(strings.TrimSpace(code))
	switch code {
	case "", "SUCCEEDED", "SUCCESS", "OK":
		return ""
	case "TIMEOUT":
		return RuntimeErrorTimeout
	case "STALLED":
		return RuntimeErrorRunStalled
	case "AUTH_FAILED", "UNAUTHORIZED", "FORBIDDEN":
		return RuntimeErrorProviderAuthFailed
	case "CONFIG_MISSING", "NOT_CONFIGURED":
		return RuntimeErrorProviderConfigMissing
	case "INPUT_UNSUPPORTED", "BAD_INPUT", "INVALID_INPUT", "RUNTIME_MODEL_INPUT_UNSUPPORTED":
		return RuntimeErrorModelInputUnsupported
	case "RUNTIME_INPUT_INVALID":
		return RuntimeErrorInputInvalid
	case "ATTACHMENT_UNSUPPORTED", "INVALID_ATTACHMENT":
		return RuntimeErrorAttachmentInvalid
	case "WORKSPACE_PERMISSION_DENIED", "RUNTIME_PERMISSION_DENIED":
		return RuntimeErrorWorkspaceForbidden
	case "TOOL_BUDGET_EXCEEDED":
		return RuntimeErrorToolBudgetExceeded
	case "TOOL_LOOP_DETECTED":
		return RuntimeErrorToolLoopDetected
	case "PARSE_FAILED", "INVALID_JSON":
		return RuntimeErrorAIResultParseFailed
	}
	if IsRuntimeFailureCode(code) {
		return code
	}
	return RuntimeErrorFailed
}

func IsRuntimeFailureCode(code string) bool {
	switch code {
	case RuntimeErrorProviderConfigMissing, RuntimeErrorProviderAuthFailed, RuntimeErrorModelInputUnsupported, RuntimeErrorAttachmentInvalid, RuntimeErrorWorkspaceForbidden, RuntimeErrorTimeout, RuntimeErrorToolLoopDetected, RuntimeErrorToolBudgetExceeded, RuntimeErrorRunStalled, RuntimeErrorInputInvalid, RuntimeErrorAIResultParseFailed, RuntimeErrorFailed:
		return true
	default:
		return false
	}
}

func RuntimeErrorCode(err error) string {
	if err == nil {
		return ""
	}
	var diagnostic *RuntimeDiagnosticError
	if errors.As(err, &diagnostic) {
		return NormalizeRuntimeFailureCode(diagnostic.Code)
	}
	text := strings.TrimSpace(err.Error())
	if IsRuntimeFailureCode(text) {
		return text
	}
	return MapTransportError(err)
}

func RuntimeFailureSummary(err error, fallback RuntimeRunResult) map[string]any {
	if err == nil {
		return map[string]any{}
	}
	var diagnostic *RuntimeDiagnosticError
	if errors.As(err, &diagnostic) {
		summary := diagnostic.Summary()
		mergeRuntimeFailureFallback(summary, fallback)
		return summary
	}
	code := RuntimeErrorCode(err)
	summary := runtimeFailureSummary(code, fallback.Status, fallback.HTTPStatus, fallback.ProviderCode, fallback.ProviderMessage, fallback.ProviderRequestID, fallback.DownstreamRequestID)
	mergeRuntimeFailureFallback(summary, fallback)
	return summary
}

func newRuntimeDiagnosticError(code string, httpStatus int, providerCode, providerMessage, providerRequestID, downstreamRequestID string) *RuntimeDiagnosticError {
	normalized := NormalizeRuntimeFailureCode(firstRuntimeNonEmpty(code, RuntimeErrorFailed))
	return &RuntimeDiagnosticError{
		Code:                normalized,
		SafeMessage:         runtimeSafeMessage(normalized),
		HTTPStatus:          httpStatus,
		ProviderCode:        sanitizeProviderCode(providerCode),
		ProviderMessage:     sanitizeRuntimeSummaryText(providerMessage),
		ProviderRequestID:   sanitizeRuntimeSummaryText(providerRequestID),
		DownstreamRequestID: sanitizeRuntimeSummaryText(downstreamRequestID),
		Status:              runtimeFailureStatus(normalized),
	}
}

func runtimeFailureSummary(code, status string, httpStatus int, providerCode, providerMessage, providerRequestID, downstreamRequestID string) map[string]any {
	code = NormalizeRuntimeFailureCode(firstRuntimeNonEmpty(code, RuntimeErrorFailed))
	status = firstRuntimeNonEmpty(status, runtimeFailureStatus(code))
	summary := map[string]any{
		"code":        code,
		"errorCode":   code,
		"status":      status,
		"retryable":   runtimeFailureRetryable(code),
		"safeMessage": runtimeSafeMessage(code),
	}
	if downstreamRequestID != "" {
		summary["downstreamRequestId"] = sanitizeRuntimeSummaryText(downstreamRequestID)
	}
	if httpStatus > 0 {
		summary["httpStatus"] = httpStatus
	}
	if providerCode != "" {
		summary["providerCode"] = sanitizeProviderCode(providerCode)
	}
	if providerMessage != "" {
		summary["providerMessage"] = sanitizeRuntimeSummaryText(providerMessage)
	}
	if providerRequestID != "" {
		summary["providerRequestId"] = sanitizeRuntimeSummaryText(providerRequestID)
	}
	return summary
}

func mergeRuntimeFailureFallback(summary map[string]any, fallback RuntimeRunResult) {
	if summary == nil {
		return
	}
	if summary["downstreamRequestId"] == nil && fallback.DownstreamRequestID != "" {
		summary["downstreamRequestId"] = sanitizeRuntimeSummaryText(fallback.DownstreamRequestID)
	}
	if summary["httpStatus"] == nil && fallback.HTTPStatus > 0 {
		summary["httpStatus"] = fallback.HTTPStatus
	}
	if summary["providerCode"] == nil && fallback.ProviderCode != "" {
		summary["providerCode"] = sanitizeProviderCode(fallback.ProviderCode)
	}
	if summary["providerMessage"] == nil && fallback.ProviderMessage != "" {
		summary["providerMessage"] = sanitizeRuntimeSummaryText(fallback.ProviderMessage)
	}
	if summary["providerRequestId"] == nil && fallback.ProviderRequestID != "" {
		summary["providerRequestId"] = sanitizeRuntimeSummaryText(fallback.ProviderRequestID)
	}
}

func runtimeFailureStatus(code string) string {
	switch NormalizeRuntimeFailureCode(code) {
	case RuntimeErrorTimeout, RuntimeErrorRunStalled:
		return "timeout"
	case RuntimeErrorWorkspaceForbidden:
		return "forbidden"
	default:
		return "failed"
	}
}

func runtimeFailureRetryable(code string) bool {
	switch NormalizeRuntimeFailureCode(code) {
	case RuntimeErrorTimeout, RuntimeErrorRunStalled, RuntimeErrorToolBudgetExceeded, RuntimeErrorAIResultParseFailed, RuntimeErrorFailed:
		return true
	default:
		return false
	}
}

func runtimeSafeMessage(code string) string {
	switch NormalizeRuntimeFailureCode(code) {
	case RuntimeErrorProviderConfigMissing:
		return "provider configuration missing"
	case RuntimeErrorProviderAuthFailed:
		return "provider authentication failed"
	case RuntimeErrorModelInputUnsupported:
		return "model input unsupported"
	case RuntimeErrorAttachmentInvalid:
		return "attachment invalid"
	case RuntimeErrorWorkspaceForbidden:
		return "workspace forbidden"
	case RuntimeErrorTimeout:
		return "runtime timed out"
	case RuntimeErrorToolLoopDetected:
		return "runtime tool loop detected"
	case RuntimeErrorToolBudgetExceeded:
		return "runtime tool budget exceeded"
	case RuntimeErrorRunStalled:
		return "runtime run stalled"
	case RuntimeErrorInputInvalid:
		return "runtime input invalid"
	case RuntimeErrorAIResultParseFailed:
		return "AI result could not be parsed"
	default:
		return "runtime failed"
	}
}

func classifyOpenClawFailure(httpStatus int, providerCode, providerMessage string) string {
	switch httpStatus {
	case http.StatusUnauthorized, http.StatusForbidden:
		return RuntimeErrorProviderAuthFailed
	case http.StatusRequestTimeout, http.StatusGatewayTimeout:
		return RuntimeErrorTimeout
	case http.StatusNotFound:
		return RuntimeErrorProviderConfigMissing
	}
	normalizedProviderCode := NormalizeRuntimeFailureCode(providerCode)
	if normalizedProviderCode != "" && normalizedProviderCode != RuntimeErrorFailed {
		return normalizedProviderCode
	}
	if normalizedProviderCode == RuntimeErrorFailed && strings.EqualFold(strings.TrimSpace(providerCode), RuntimeErrorFailed) {
		// A generic provider code is not authoritative. Let the safe message and
		// HTTP status select a more precise registered failure when possible.
		normalizedProviderCode = ""
	}
	mapped := MapTransportError(fmt.Errorf("%s %s", providerCode, providerMessage))
	if mapped != RuntimeErrorFailed {
		return mapped
	}
	if httpStatus == http.StatusBadRequest {
		return RuntimeErrorModelInputUnsupported
	}
	return RuntimeErrorFailed
}

func openclawProviderFieldsFromPayload(payload []byte) (string, string, string) {
	raw := map[string]any{}
	if err := json.Unmarshal(payload, &raw); err != nil {
		return "", "", ""
	}
	return openclawProviderFieldsFromEnvelope(raw, 0)
}

func openclawProviderFieldsFromEnvelope(raw map[string]any, depth int) (string, string, string) {
	if len(raw) == 0 || depth > 4 {
		return "", "", ""
	}
	parentCode, parentMessage, parentRequestID := openclawProviderFieldsFromMap(raw)
	for _, key := range []string{"result", "output", "response", "payload", "runRecord", "data"} {
		nested := openclawMapValue(raw[key])
		if len(nested) == 0 {
			continue
		}
		code, message, requestID := openclawProviderFieldsFromEnvelope(nested, depth+1)
		if code != "" || message != "" || requestID != "" {
			return firstRuntimeNonEmpty(code, parentCode), firstRuntimeNonEmpty(message, parentMessage), firstRuntimeNonEmpty(requestID, parentRequestID)
		}
	}
	return parentCode, parentMessage, parentRequestID
}

func openclawProviderFieldsFromMap(raw map[string]any) (string, string, string) {
	if raw == nil {
		return "", "", ""
	}
	providerCode := firstRuntimeNonEmpty(
		openclawStringValue(raw["providerCode"]),
		openclawStringValue(raw["errorCode"]),
		openclawStringValue(raw["code"]),
	)
	providerMessage := firstRuntimeNonEmpty(
		openclawStringValue(raw["providerMessage"]),
		openclawStringValue(raw["errorMessage"]),
		openclawStringValue(raw["message"]),
	)
	providerRequestID := firstRuntimeNonEmpty(
		openclawStringValue(raw["providerRequestId"]),
		openclawStringValue(raw["provider_request_id"]),
		openclawStringValue(raw["requestId"]),
		openclawStringValue(raw["request_id"]),
	)
	if nested, ok := raw["error"].(map[string]any); ok {
		providerCode = firstRuntimeNonEmpty(openclawStringValue(nested["providerCode"]), openclawStringValue(nested["code"]), providerCode)
		providerMessage = firstRuntimeNonEmpty(openclawStringValue(nested["providerMessage"]), openclawStringValue(nested["message"]), providerMessage)
		providerRequestID = firstRuntimeNonEmpty(openclawStringValue(nested["providerRequestId"]), openclawStringValue(nested["requestId"]), openclawStringValue(nested["request_id"]), providerRequestID)
	} else if raw["error"] != nil {
		providerMessage = firstRuntimeNonEmpty(openclawStringValue(raw["error"]), providerMessage)
	}
	return sanitizeProviderCode(providerCode), sanitizeRuntimeSummaryText(providerMessage), sanitizeRuntimeSummaryText(providerRequestID)
}

func runtimeProviderRequestIDFromHeader(header http.Header) string {
	if header == nil {
		return ""
	}
	return sanitizeRuntimeSummaryText(firstRuntimeNonEmpty(
		header.Get("X-Request-Id"),
		header.Get("X-Request-ID"),
		header.Get("X-OpenClaw-Request-Id"),
		header.Get("X-Provider-Request-Id"),
	))
}

func runtimeDownstreamRequestIDFromHeader(header http.Header) string {
	if header == nil {
		return ""
	}
	return sanitizeRuntimeOpaqueIdentifier(firstRuntimeNonEmpty(
		header.Get("X-Downstream-Request-Id"),
		header.Get("X-Runtime-Request-Id"),
		header.Get("X-Request-Id"),
		header.Get("X-Request-ID"),
	))
}

func normalizeRuntimeStatus(status string) string {
	switch strings.ToLower(status) {
	case "succeeded", "success", "completed":
		return "succeeded"
	case "timeout":
		return "timeout"
	case "forbidden":
		return "forbidden"
	default:
		return "failed"
	}
}

func redactRuntimeLogs(value any) []string {
	out := []string{}
	switch typed := value.(type) {
	case []string:
		for _, item := range typed {
			out = append(out, redactRuntimeText(item))
		}
	case []any:
		for _, item := range typed {
			out = append(out, redactRuntimeText(fmt.Sprint(item)))
		}
	}
	return out
}

func redactRuntimeText(value string) string {
	if value == "" {
		return ""
	}
	value = strings.ReplaceAll(value, "\\", "/")
	lower := strings.ToLower(value)
	if runtimeTextLooksLikeBackendPrompt(value) || strings.Contains(lower, "prompt=") || strings.Contains(lower, "inputmessage") {
		return "[runtime-input-redacted]"
	}
	if strings.Contains(lower, "secret://") || strings.Contains(lower, "sk-") || strings.Contains(lower, "ark-") || strings.Contains(lower, "bearer ") || runtimeTextContainsSensitiveAssignment(value) || (strings.Contains(lower, "secret") && strings.Contains(lower, "token")) {
		return "[secret-redacted]"
	}
	if runtimeTextContainsHostPath(value) {
		return "[runtime-path-redacted]"
	}
	parts := strings.Fields(value)
	for i, part := range parts {
		trimmed := strings.Trim(part, " \t\r\n\"'`.,;:()[]{}<>")
		if looksLikeSignedRuntimeURL(trimmed) {
			parts[i] = "[signed-url-redacted]"
			continue
		}
		if looksLikeRuntimeSessionSecret(trimmed) {
			parts[i] = "[runtime-session-redacted]"
			continue
		}
		if looksLikeRuntimeSkillPath(trimmed) {
			parts[i] = "[runtime-skill-redacted]"
			continue
		}
		if runtimeTokenContainsHostPath(trimmed) {
			parts[i] = "[runtime-path-redacted]"
		}
	}
	redacted := strings.Join(parts, " ")
	if strings.Contains(redacted, "sk-") {
		redacted = "[secret-redacted]"
	}
	return redacted
}

func looksLikeSignedRuntimeURL(value string) bool {
	lower := strings.ToLower(strings.TrimSpace(value))
	if !strings.Contains(lower, "://") && !strings.HasPrefix(lower, "oss:") && !strings.HasPrefix(lower, "s3:") {
		return false
	}
	for _, marker := range []string{
		"signature=", "x-amz-signature", "x-amz-credential", "x-amz-security-token",
		"x-oss-signature", "x-oss-credential", "awsaccesskeyid=", "access_token=",
		"refresh_token=", "token=", "credential=", "security-token=", "?sig=", "&sig=",
	} {
		if strings.Contains(lower, marker) {
			return true
		}
	}
	return false
}

func looksLikeRuntimeSessionSecret(value string) bool {
	lower := strings.ToLower(strings.TrimSpace(value))
	return strings.Contains(lower, "openclawsessionkey") || strings.Contains(lower, "sessionkey=") ||
		strings.HasPrefix(lower, "oc:") || strings.HasPrefix(lower, "runtime:tenant:")
}

func sanitizeRuntimeOpaqueIdentifier(value string) string {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > 256 || runtimeTransportForbiddenString(value) || looksLikeRuntimeSessionSecret(value) || runtimeOpaqueIdentifierLooksSensitive(value) {
		return ""
	}
	for _, character := range value {
		if (character >= 'A' && character <= 'Z') || (character >= 'a' && character <= 'z') || (character >= '0' && character <= '9') ||
			character == '_' || character == '-' || character == '.' || character == ':' {
			continue
		}
		return ""
	}
	return value
}

func runtimeOpaqueIdentifierLooksSensitive(value string) bool {
	lower := strings.ToLower(strings.TrimSpace(value))
	if strings.HasPrefix(lower, "sk-") || strings.HasPrefix(lower, "ark-") || runtimeTextContainsSensitiveAssignment(value) {
		return true
	}
	return strings.HasPrefix(value, "eyJ") && strings.Count(value, ".") >= 2
}

func sanitizeRuntimeUsage(value map[string]any) map[string]any {
	out := map[string]any{}
	for key, item := range value {
		if !runtimeUsageKeyAllowed(key) {
			continue
		}
		switch typed := item.(type) {
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, json.Number, bool:
			out[key] = typed
		}
	}
	return out
}

func runtimeUsageKeyAllowed(key string) bool {
	switch key {
	case "tokens", "totalTokens", "inputTokens", "outputTokens", "reasoningTokens", "cachedTokens", "modelTokens", "toolCalls", "durationMs", "wallTimeMs", "timeoutBudgetSec":
		return true
	default:
		return false
	}
}

// sanitizeRuntimeResultMap is used at the Runtime Host boundary before a
// status or event can enter durable/admin-safe storage. It leaves structured
// model output available to the frozen parser while removing raw transport
// fields and recursively redacting paths, signed URLs, and credentials.
func sanitizeRuntimeResultMap(input map[string]any) map[string]any {
	return sanitizeRuntimeResultMapAtDepth(input, 0)
}

func sanitizeRuntimeResultMapAtDepth(input map[string]any, depth int) map[string]any {
	if len(input) == 0 || depth > 6 {
		return map[string]any{}
	}
	out := make(map[string]any, len(input))
	for key, item := range input {
		if !validRuntimeMetadataKey(key) || runtimeSensitiveProjectionKey(key) {
			continue
		}
		if value, keep := sanitizeRuntimeResultValue(item, depth+1); keep {
			out[key] = value
		}
	}
	return out
}

func sanitizeRuntimeResultValue(value any, depth int) (any, bool) {
	if depth > 6 {
		return nil, false
	}
	switch typed := value.(type) {
	case nil:
		return nil, true
	case string:
		return redactRuntimeText(typed), true
	case bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, json.Number:
		return typed, true
	case map[string]any:
		return sanitizeRuntimeResultMapAtDepth(typed, depth), true
	case []string:
		if len(typed) > 128 {
			return nil, false
		}
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			out = append(out, redactRuntimeText(item))
		}
		return out, true
	case []any:
		if len(typed) > 128 {
			return nil, false
		}
		out := make([]any, 0, len(typed))
		for _, item := range typed {
			canonical, keep := sanitizeRuntimeResultValue(item, depth+1)
			if keep {
				out = append(out, canonical)
			}
		}
		return out, true
	default:
		return nil, false
	}
}

func runtimeSensitiveProjectionKey(key string) bool {
	normalized := normalizedRuntimeSensitiveKey(key)
	switch normalized {
	case "authorization", "token", "accesstoken", "refreshtoken", "apikey", "secret", "credential", "cookie",
		"runticket", "openclawsessionkey", "sessionkey", "sessionpath", "realpath", "workspacepath", "runtimepath",
		"objectread", "writelease", "providerbody", "requestbody", "responsebody", "raw", "rawbody", "prompt", "inputmessage", "transcript":
		return true
	default:
		for _, marker := range []string{"authorization", "apikey", "accesstoken", "refreshtoken", "credential", "secret", "cookie", "runticket", "openclawsessionkey", "sessionkey", "sessiontoken", "prompt", "inputmessage", "transcript"} {
			if strings.Contains(normalized, marker) {
				return true
			}
		}
		return strings.HasSuffix(normalized, "token")
	}
}

func normalizedRuntimeSensitiveKey(key string) string {
	return strings.ToLower(strings.NewReplacer("_", "", "-", "", ".", "", " ", "").Replace(strings.TrimSpace(key)))
}

func sanitizeRuntimeInputMessage(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	value = redactRuntimeText(value)
	value = strings.TrimSpace(strings.Join(strings.Fields(value), " "))
	if value == "" {
		return ""
	}
	if runtimeLooksLikeBackendPrompt(value) {
		return "Use the configured runtime instructions and available runtime context."
	}
	return limitRuntimeInputMessage(value)
}

func limitRuntimeInputMessage(value string) string {
	return value
}

func runtimeLooksLikeBackendPrompt(value string) bool {
	lower := strings.ToLower(value)
	for _, marker := range []string{
		"## loaded effective skill",
		"do not read /home/agent-runtime/openclaw-data/plugin-skills",
		"expected_topic_skill_contract",
		"expected_profile_skill_contract",
		"expected_minutes_skill_contract",
	} {
		if strings.Contains(lower, marker) {
			return true
		}
	}
	markers := []string{
		"task:",
		"agent profile:",
		"required capability:",
		"prompt template:",
		"business parameters:",
		"context summary and relative references:",
		"output: return exactly one json object",
	}
	matches := 0
	for _, marker := range markers {
		if strings.Contains(lower, marker) {
			matches++
		}
	}
	return matches >= 2
}

func sanitizeRuntimeSummaryText(value string) string {
	value = strings.TrimSpace(redactRuntimeText(value))
	if value == "" {
		return ""
	}
	if strings.HasPrefix(value, "{") || strings.HasPrefix(value, "[") {
		return "[provider-message-redacted]"
	}
	runes := []rune(value)
	if len(runes) > 240 {
		value = string(runes[:240])
	}
	return value
}

func sanitizeProviderCode(value string) string {
	value = strings.TrimSpace(value)
	if value == "" || runtimeOpaqueIdentifierLooksSensitive(value) {
		return ""
	}
	var builder strings.Builder
	for _, ch := range value {
		switch {
		case ch >= 'A' && ch <= 'Z', ch >= 'a' && ch <= 'z', ch >= '0' && ch <= '9':
			builder.WriteRune(ch)
		case ch == '_' || ch == '-' || ch == '.':
			builder.WriteRune(ch)
		}
		if builder.Len() >= 96 {
			break
		}
	}
	return builder.String()
}

func looksLikeRuntimePath(value string) bool {
	value = strings.Trim(value, " \t\r\n\"'`.,;:()[]{}<>")
	normalized := strings.ReplaceAll(value, "\\", "/")
	if len(normalized) >= 2 && ((normalized[0] >= 'A' && normalized[0] <= 'Z') || (normalized[0] >= 'a' && normalized[0] <= 'z')) && normalized[1] == ':' {
		if len(normalized) == 2 || normalized[2] != ' ' && normalized[2] != '\t' {
			return true
		}
	}
	if strings.HasPrefix(normalized, "//") {
		return true
	}
	lower := strings.ToLower(normalized)
	return strings.Contains(lower, "/workspaces/") || strings.Contains(lower, "/runtime-workspaces/") || strings.Contains(lower, "/openclaw/")
}

func looksLikeRuntimeSkillPath(value string) bool {
	lower := strings.ToLower(strings.Trim(value, " \t\r\n\"'`.,;:()[]{}<>"))
	return lower == "skill.md" || strings.Contains(lower, "skill.md") || strings.Contains(lower, "/runtime-skills/") || strings.Contains(lower, "/skills/")
}

func firstRuntimeNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func OpenClawRuntimeMethodURL(endpoint, method string) string {
	endpoint = strings.TrimRight(strings.TrimSpace(endpoint), "/")
	if endpoint == "" {
		return ""
	}
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return openClawRuntimeMethodURLString(endpoint, method)
	}
	parsed.Path = openClawRuntimeMethodPath(parsed.Path, method)
	return parsed.String()
}

func openClawRuntimeMethodURLString(endpoint, method string) string {
	path := openClawRuntimeMethodPath(endpoint, method)
	if strings.HasPrefix(path, "/") && !strings.HasPrefix(endpoint, "/") {
		return strings.TrimRight(endpoint, "/") + "/" + method
	}
	return path
}

func openClawRuntimeMethodPath(pathValue, method string) string {
	pathValue = strings.TrimRight(pathValue, "/")
	if pathValue == "" || pathValue == "/" {
		return "/" + method
	}
	if openClawRuntimePathMethod(pathValue) != "" {
		prefix, _ := splitOpenClawRuntimePathMethod(pathValue)
		if prefix == "" {
			return "/" + method
		}
		return prefix + "/" + method
	}
	return pathValue
}

func openClawRuntimePathMethod(pathValue string) string {
	_, leaf := splitOpenClawRuntimePathMethod(pathValue)
	switch leaf {
	case "enterprise.runtime.run", "enterprise.runtime.abort":
		return leaf
	default:
		return ""
	}
}

func splitOpenClawRuntimePathMethod(pathValue string) (string, string) {
	pathValue = strings.TrimRight(pathValue, "/")
	idx := strings.LastIndex(pathValue, "/")
	if idx < 0 {
		return "", pathValue
	}
	return pathValue[:idx], pathValue[idx+1:]
}

func openclawStringValue(value any) string {
	if value == nil {
		return ""
	}
	if typed, ok := value.(string); ok {
		return typed
	}
	return fmt.Sprint(value)
}

func openclawMapValue(value any) map[string]any {
	if typed, ok := value.(map[string]any); ok {
		out := map[string]any{}
		for key, item := range typed {
			out[key] = item
		}
		return out
	}
	return map[string]any{}
}

func mergeRuntimePayloadFallback(primary, fallback map[string]any) map[string]any {
	out := map[string]any{}
	for key, value := range primary {
		out[key] = value
	}
	for key, value := range fallback {
		if _, exists := out[key]; !exists {
			out[key] = value
		}
	}
	return out
}

func firstRuntimeMap(values ...map[string]any) map[string]any {
	for _, value := range values {
		if len(value) > 0 {
			return value
		}
	}
	return nil
}

func firstRuntimeInt(values ...int) int {
	for _, value := range values {
		if value != 0 {
			return value
		}
	}
	return 0
}

func firstRuntimeLogs(values ...[]string) []string {
	for _, value := range values {
		if len(value) > 0 {
			return value
		}
	}
	return nil
}

func firstRuntimeMapSliceItem(value any) map[string]any {
	switch typed := value.(type) {
	case []map[string]any:
		if len(typed) > 0 {
			return openclawMapValue(typed[0])
		}
	case []any:
		if len(typed) > 0 {
			return openclawMapValue(typed[0])
		}
	}
	return map[string]any{}
}

func openclawIntValue(value any) int {
	switch typed := value.(type) {
	case int:
		return typed
	case int32:
		return int(typed)
	case int64:
		return int(typed)
	case float64:
		return int(typed)
	case json.Number:
		out, _ := typed.Int64()
		return int(out)
	default:
		return 0
	}
}

func runtimeSpecMissingRequired(spec RuntimeRunSpec) bool {
	return strings.TrimSpace(spec.RunID) == "" ||
		strings.TrimSpace(spec.TenantID) == "" ||
		strings.TrimSpace(spec.UserID) == "" ||
		strings.TrimSpace(spec.WorkspaceID) == "" ||
		strings.TrimSpace(spec.ThreadID) == "" ||
		strings.TrimSpace(spec.RuntimeConfigID) == "" ||
		!ValidRuntimeSubmitConfigVersion(spec.RuntimeConfigVersion) ||
		strings.TrimSpace(spec.Workspace.RealPath) == "" ||
		strings.TrimSpace(spec.Workspace.AccessMode) == "" ||
		strings.TrimSpace(spec.ProductSession.ThreadID) == "" ||
		strings.TrimSpace(spec.ProductSession.OpenClawSessionKey) == "" ||
		strings.TrimSpace(spec.Input.Message) == ""
}

func runtimeExecutionConfigured(spec RuntimeExecutionSpec) bool {
	return spec.StateDir != "" || spec.ConfigPath != "" || spec.LogsDir != "" || spec.TmpRoot != ""
}

func runtimeModelOverrideBody(spec RuntimeModelOverrideSpec) map[string]any {
	body := map[string]any{}
	if spec.ModelProfileID != "" {
		body["modelProfileId"] = spec.ModelProfileID
	}
	if spec.Provider != "" {
		body["provider"] = spec.Provider
	}
	if spec.Model != "" {
		body["model"] = spec.Model
	}
	if len(spec.Fallbacks) > 0 {
		body["fallbacks"] = spec.Fallbacks
	}
	if spec.AuthPoolID != "" {
		body["authPoolId"] = spec.AuthPoolID
	}
	if spec.TimeoutSeconds > 0 {
		body["timeoutSeconds"] = spec.TimeoutSeconds
	}
	if spec.Thinking != "" {
		body["thinking"] = spec.Thinking
	}
	if spec.Reasoning != "" {
		body["reasoning"] = spec.Reasoning
	}
	if spec.MaxTokens > 0 {
		body["maxTokens"] = spec.MaxTokens
	}
	if len(spec.Params) > 0 {
		body["params"] = spec.Params
	}
	return body
}

func runtimeToolsBody(spec RuntimeToolsSpec) map[string]any {
	body := map[string]any{}
	if spec.ProfileID != "" {
		body["profileId"] = spec.ProfileID
	}
	if spec.Allow != nil {
		body["allow"] = spec.Allow
	}
	if len(spec.Deny) > 0 {
		body["deny"] = spec.Deny
	}
	return body
}

func runtimePluginsBody(spec RuntimePluginsSpec) map[string]any {
	body := map[string]any{}
	if len(spec.Enabled) > 0 {
		body["enabled"] = spec.Enabled
	}
	if len(spec.Disabled) > 0 {
		body["disabled"] = spec.Disabled
	}
	return body
}

func runtimeSessionKey(prefix string, parts ...string) string {
	if strings.TrimSpace(prefix) == "" {
		prefix = "oc:s"
	}
	normalized := make([]string, 0, len(parts))
	for _, part := range parts {
		normalized = append(normalized, strings.TrimSpace(part))
	}
	sum := sha256.Sum256([]byte(strings.Join(normalized, "\x1f")))
	return prefix + ":" + hex.EncodeToString(sum[:])[:32]
}

func RuntimeSessionKeyHash(sessionKey string) string {
	return runtimeSessionKeyHash(sessionKey)
}

func runtimeSessionKeyHash(sessionKey string) string {
	sessionKey = strings.TrimSpace(sessionKey)
	if sessionKey == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(sessionKey))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func runtimeWorkspacePath(userID, workspaceID string) string {
	userID = runtimeSafeSegment(userID)
	workspaceID = runtimeSafeSegment(workspaceID)
	if userID == "" || workspaceID == "" {
		return ""
	}
	root := firstRuntimeNonEmpty(
		os.Getenv("HUAHUO_DATA_WORKSPACES_ROOT"),
		os.Getenv("DATA_WORKSPACES_ROOT"),
	)
	if root == "" {
		if dataRoot := firstRuntimeNonEmpty(os.Getenv("HUAHUO_DATA_ROOT"), os.Getenv("DATA_ROOT")); dataRoot != "" {
			root = filepath.Join(dataRoot, "workspaces")
		}
	}
	if root == "" {
		root = "/home/data/huahuo/workspaces"
	}
	return filepath.Join(root, "tenants", runtimeWorkspaceTenantID(), "users", userID, "workspaces", workspaceID)
}

func runtimeWorkspaceTenantID() string {
	return firstRuntimeNonEmpty(os.Getenv("HUAHUO_WORKSPACE_TENANT_ID"), os.Getenv("WORKSPACE_TENANT_ID"), "tenant_default")
}

func runtimePathHasTraversal(pathValue string) bool {
	normalized := strings.ReplaceAll(strings.TrimSpace(pathValue), "\\", "/")
	if normalized == "" {
		return false
	}
	for _, segment := range strings.Split(normalized, "/") {
		if segment == ".." {
			return true
		}
	}
	return false
}

func runtimeSafeSegment(value string) string {
	value = strings.TrimSpace(value)
	var builder strings.Builder
	for _, ch := range value {
		switch {
		case ch >= 'A' && ch <= 'Z', ch >= 'a' && ch <= 'z', ch >= '0' && ch <= '9':
			builder.WriteRune(ch)
		case ch == '-' || ch == '_':
			builder.WriteRune(ch)
		default:
			builder.WriteByte('_')
		}
		if builder.Len() >= 96 {
			break
		}
	}
	return strings.Trim(builder.String(), "_")
}

func NewRuntimeRequestID(runID string) string {
	if runID == "" {
		runID = fmt.Sprint(time.Now().UTC().UnixNano())
	}
	return "openclaw_req_" + runID
}
