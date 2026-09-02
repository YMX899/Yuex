package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"huahuoai/backend/source/internal/domain"
)

type RuntimeInputManifest struct {
	SchemaVersion                 string                            `json:"schemaVersion"`
	RunID                         string                            `json:"runId"`
	RuntimeHostID                 string                            `json:"runtimeHostId"`
	TenantID                      string                            `json:"tenantId"`
	UserID                        string                            `json:"userId"`
	WorkspaceID                   string                            `json:"workspaceId"`
	WorkspaceVersion              int64                             `json:"workspaceVersion"`
	ThreadWorkspaceBindingVersion int64                             `json:"threadWorkspaceBindingVersion"`
	ContextGeneration             int64                             `json:"contextGeneration"`
	MetaRelease                   string                            `json:"metaRelease"`
	AgentProfile                  string                            `json:"agentProfile"`
	MetaWorkspaceKey              string                            `json:"metaWorkspaceKey,omitempty"`
	MetaWorkspaceVersion          string                            `json:"metaWorkspaceVersion,omitempty"`
	InputPolicyHash               string                            `json:"inputPolicyHash,omitempty"`
	Attachments                   []AgentRunInputAttachmentIdentity `json:"attachments,omitempty"`
	AgentHash                     string                            `json:"agentHash"`
	SkillProfiles                 []RuntimeSkillProfile             `json:"skillProfiles"`
	CapabilityHash                string                            `json:"capabilityHash"`
	Files                         []RuntimeManifestEntry            `json:"files"`
	ManifestHash                  string                            `json:"manifestHash"`
	ExpiresAt                     time.Time                         `json:"expiresAt"`
}

type RuntimeSkillProfile struct {
	Profile string `json:"profile"`
	Hash    string `json:"hash"`
}

type RuntimeManifestEntry struct {
	LogicalPath   string `json:"logicalPath"`
	SourceType    string `json:"sourceType"`
	SourceRef     string `json:"sourceRef,omitempty"`
	InlineContent string `json:"inlineContent,omitempty"`
	// ObjectRead is an internal, short-lived capability used only while a
	// Runtime Host materializes an object_ref that is not mounted locally.
	// It is carried in the Backend-to-Host request, never copied into the
	// model-visible input files or public Run projection.
	ObjectRead *RuntimeObjectReadReference `json:"objectRead,omitempty"`
	SizeBytes  int64                       `json:"sizeBytes"`
	SHA256     string                      `json:"sha256"`
}

// RuntimeObjectReadReference binds a remote object fetch to one immutable
// manifest file. URL is a provider-signed, expiring read capability, not an
// App upload/download URL. The materializer validates URL shape, expiry,
// MIME, exact length and SHA-256 before it exposes any local path to Runtime.
type RuntimeObjectReadReference struct {
	URL       string    `json:"url"`
	ExpiresAt time.Time `json:"expiresAt"`
	MIMEType  string    `json:"mimeType"`
}

type AsyncRuntimeSubmitRequest struct {
	RunID                string               `json:"runId"`
	ReservationID        string               `json:"reservationId"`
	FencingToken         int64                `json:"fencingToken"`
	CapabilityHash       string               `json:"capabilityHash"`
	InputMessage         string               `json:"inputMessage"`
	RuntimeConfigID      string               `json:"runtimeConfigId"`
	RuntimeConfigVersion string               `json:"runtimeConfigVersion,omitempty"`
	RunTicket            string               `json:"-"`
	RunTicketJTI         string               `json:"-"`
	InputManifest        RuntimeInputManifest `json:"inputManifest"`
	Plan                 AgentRunPlan         `json:"plan"`
	ProductSessionRef    map[string]any       `json:"productSessionRef,omitempty"`
}

type AsyncRuntimeSubmitResult struct {
	RunID            string `json:"runId"`
	Status           string `json:"status"`
	RuntimeRequestID string `json:"runtimeRequestId"`
	AcceptedSequence int64  `json:"acceptedSequence"`
}

type AsyncRuntimeStatus struct {
	RunID             string         `json:"runId"`
	Status            string         `json:"status"`
	RuntimeRequestID  string         `json:"runtimeRequestId,omitempty"`
	LastEventSequence int64          `json:"lastEventSequence"`
	Result            map[string]any `json:"result,omitempty"`
	Error             map[string]any `json:"error,omitempty"`
	Usage             map[string]any `json:"usage,omitempty"`
}

type AsyncRuntimeEventPage struct {
	Items                   []AsyncRuntimeEvent `json:"items"`
	NextAfterSequence       int64               `json:"nextAfterSequence"`
	HasMore                 bool                `json:"hasMore"`
	OldestAvailableSequence int64               `json:"oldestAvailableSequence"`
	LatestSequence          int64               `json:"latestSequence"`
	TerminalSequence        int64               `json:"terminalSequence"`
	Gap                     bool                `json:"gap"`
}

type AsyncRuntimeEvent struct {
	Sequence    int64          `json:"sequence"`
	EventType   string         `json:"eventType"`
	Status      string         `json:"status"`
	Timestamp   time.Time      `json:"timestamp"`
	Data        map[string]any `json:"data,omitempty"`
	SafePayload map[string]any `json:"safePayload,omitempty"`
	UsageDelta  map[string]any `json:"usageDelta,omitempty"`
	OccurredAt  time.Time      `json:"occurredAt,omitempty"`
}

type AsyncRuntimeAbortRequest struct {
	RunID         string `json:"runId"`
	ReservationID string `json:"reservationId"`
	FencingToken  int64  `json:"fencingToken"`
	Reason        string `json:"reason"`
	RunTicket     string `json:"-"`
}

type AsyncRuntimeAbortResult struct {
	RunID  string `json:"runId"`
	Status string `json:"status"`
}

type AsyncOpenClawClient interface {
	Submit(ctx context.Context, host RuntimeHost, request AsyncRuntimeSubmitRequest) (AsyncRuntimeSubmitResult, error)
	GetStatus(ctx context.Context, host RuntimeHost, runID, runTicket string) (AsyncRuntimeStatus, error)
	ListEvents(ctx context.Context, host RuntimeHost, runID, runTicket string, afterSequence int64, limit, waitMs int) (AsyncRuntimeEventPage, error)
	AbortAsync(ctx context.Context, host RuntimeHost, request AsyncRuntimeAbortRequest) (AsyncRuntimeAbortResult, error)
}

func (c HTTPTransportOpenClawClient) Submit(ctx context.Context, host RuntimeHost, request AsyncRuntimeSubmitRequest) (AsyncRuntimeSubmitResult, error) {
	canonical, err := canonicalAsyncRuntimeSubmitRequest(request, host)
	if err != nil {
		return AsyncRuntimeSubmitResult{}, fmt.Errorf("RUNTIME_INPUT_INVALID")
	}
	budget, valid := runtimeSubmitTransportBudget(canonical.Plan.ToolBudget)
	if !valid {
		return AsyncRuntimeSubmitResult{}, fmt.Errorf("RUNTIME_INPUT_INVALID")
	}
	if c.submitCapture != nil {
		// Marshal the same canonical struct that the Host transport serializes.
		// json.Marshal is deterministic for this struct/map shape, and the sink
		// receives a copy so it cannot mutate the later transport request.
		raw, marshalErr := json.Marshal(canonical)
		if marshalErr != nil {
			return AsyncRuntimeSubmitResult{}, fmt.Errorf("RUNTIME_INPUT_INVALID")
		}
		record := RuntimeSubmitCaptureRecord{
			RunID: canonical.RunID, RunTicket: canonical.RunTicket,
			CanonicalRequest: append([]byte(nil), raw...),
		}
		if captureErr := c.submitCapture.CaptureAsyncRuntimeSubmit(ctx, record); captureErr != nil {
			return AsyncRuntimeSubmitResult{}, captureErr
		}
	}
	requestCtx, cancel := c.runtimeHostRequestContext(ctx, budget.TimeoutSec)
	defer cancel()
	var result AsyncRuntimeSubmitResult
	err = c.doRuntimeHostJSONWithOptions(requestCtx, host, http.MethodPost, "/enterprise.runtime/runs", canonical.RunTicket, canonical, &result, runtimeHostRequestOptions{TimeoutSec: budget.TimeoutSec, MaxToolCalls: budget.MaxToolCalls})
	if err != nil {
		return AsyncRuntimeSubmitResult{}, err
	}
	result.Status = strings.ToLower(strings.TrimSpace(result.Status))
	result.RuntimeRequestID = sanitizeRuntimeOpaqueIdentifier(result.RuntimeRequestID)
	if result.RunID != canonical.RunID || result.Status != "accepted" || result.RuntimeRequestID == "" {
		// The response body is never logged here. These shape facts are enough to
		// diagnose an Adapter contract mismatch without exposing a model result,
		// ticket, or provider diagnostic.
		return AsyncRuntimeSubmitResult{}, newRuntimeDiagnosticError(
			RuntimeErrorFailed,
			http.StatusOK,
			"",
			fmt.Sprintf("unexpected async submit response: matching_run_id=%t accepted_status=%t runtime_request_id_present=%t accepted_sequence_present=%t",
				result.RunID == canonical.RunID,
				result.Status == "accepted",
				result.RuntimeRequestID != "",
				result.AcceptedSequence > 0,
			),
			"",
			"",
		)
	}
	return result, nil
}

func (c HTTPTransportOpenClawClient) GetStatus(ctx context.Context, host RuntimeHost, runID, runTicket string) (AsyncRuntimeStatus, error) {
	if strings.TrimSpace(runID) == "" || strings.TrimSpace(runTicket) == "" {
		return AsyncRuntimeStatus{}, fmt.Errorf("RUNTIME_INPUT_INVALID")
	}
	var result AsyncRuntimeStatus
	err := c.doRuntimeHostJSON(ctx, host, http.MethodGet, "/enterprise.runtime/runs/"+url.PathEscape(runID), runTicket, nil, &result)
	if err != nil {
		return AsyncRuntimeStatus{}, err
	}
	if result.RunID != runID {
		return AsyncRuntimeStatus{}, fmt.Errorf("RUNTIME_FAILED")
	}
	return c.normalizeAsyncRuntimeStatus(result, runID)
}

func (c HTTPTransportOpenClawClient) ListEvents(ctx context.Context, host RuntimeHost, runID, runTicket string, afterSequence int64, limit, waitMs int) (AsyncRuntimeEventPage, error) {
	if strings.TrimSpace(runID) == "" || strings.TrimSpace(runTicket) == "" || afterSequence < 0 || limit < 1 || limit > 500 || waitMs < 0 || waitMs > 25000 {
		return AsyncRuntimeEventPage{}, fmt.Errorf("INVALID_ARGUMENT")
	}
	path := "/enterprise.runtime/runs/" + url.PathEscape(runID) + "/events?afterSequence=" + strconv.FormatInt(afterSequence, 10) + "&limit=" + strconv.Itoa(limit) + "&waitMs=" + strconv.Itoa(waitMs)
	var result AsyncRuntimeEventPage
	if err := c.doRuntimeHostJSON(ctx, host, http.MethodGet, path, runTicket, nil, &result); err != nil {
		return AsyncRuntimeEventPage{}, err
	}
	return normalizeAsyncRuntimeEventPage(result, afterSequence)
}

func (c HTTPTransportOpenClawClient) AbortAsync(ctx context.Context, host RuntimeHost, request AsyncRuntimeAbortRequest) (AsyncRuntimeAbortResult, error) {
	if strings.TrimSpace(request.RunID) == "" || strings.TrimSpace(request.ReservationID) == "" || request.FencingToken < 1 || strings.TrimSpace(request.RunTicket) == "" {
		return AsyncRuntimeAbortResult{}, fmt.Errorf("RUNTIME_INPUT_INVALID")
	}
	var result AsyncRuntimeAbortResult
	path := "/enterprise.runtime/runs/" + url.PathEscape(request.RunID) + "/abort"
	if err := c.doRuntimeHostJSON(ctx, host, http.MethodPost, path, request.RunTicket, request, &result); err != nil {
		return AsyncRuntimeAbortResult{}, err
	}
	result.Status = strings.ToLower(strings.TrimSpace(result.Status))
	if result.RunID != request.RunID || (result.Status != "aborting" && result.Status != "aborted") {
		return AsyncRuntimeAbortResult{}, fmt.Errorf("RUNTIME_ABORT_FAILED")
	}
	return result, nil
}

func canonicalAsyncRuntimeSubmitRequest(request AsyncRuntimeSubmitRequest, host RuntimeHost) (AsyncRuntimeSubmitRequest, error) {
	if request.RunID == "" || request.ReservationID == "" || request.FencingToken < 1 || request.CapabilityHash == "" ||
		strings.TrimSpace(request.InputMessage) == "" || strings.TrimSpace(request.RuntimeConfigID) == "" ||
		!ValidRuntimeSubmitConfigVersion(request.RuntimeConfigVersion) ||
		strings.TrimSpace(request.RunTicket) == "" ||
		request.InputManifest.RunID != request.RunID || request.InputManifest.RuntimeHostID != host.RuntimeHostID || request.InputManifest.CapabilityHash != request.CapabilityHash ||
		request.Plan.AgentRunID != request.RunID || request.Plan.CapabilityHash != request.CapabilityHash || request.Plan.RuntimeConfigID != request.RuntimeConfigID || request.Plan.L1AgentProfile != request.InputManifest.AgentProfile ||
		request.Plan.MetaWorkspaceKey != request.InputManifest.MetaWorkspaceKey || request.Plan.MetaWorkspaceVersion != request.InputManifest.MetaWorkspaceVersion ||
		request.Plan.InputPolicyHash != request.InputManifest.InputPolicyHash || !SameAgentRunInputAttachmentIdentities(request.Plan.InputAttachments, request.InputManifest.Attachments) {
		return AsyncRuntimeSubmitRequest{}, fmt.Errorf("RUNTIME_INPUT_INVALID")
	}
	composer := WorkspaceComposer{}
	if request.InputManifest.ManifestHash == "" || request.InputManifest.ManifestHash != composer.ComputeManifestHash(request.InputManifest) ||
		request.InputManifest.ExpiresAt.IsZero() || !request.InputManifest.ExpiresAt.After(time.Now().UTC()) || composer.ValidateLogicalFiles(request.InputManifest) != nil {
		return AsyncRuntimeSubmitRequest{}, fmt.Errorf("RUNTIME_INPUT_INVALID")
	}
	productSession, err := canonicalAsyncProductSessionRef(request.ProductSessionRef)
	if err != nil {
		return AsyncRuntimeSubmitRequest{}, err
	}
	request.ProductSessionRef = productSession
	// The exact body is signed by the RunTicket submit binding. Do not trim,
	// redact, compress, replace, or default these values here: any
	// transformation would make the Backend/Adapter/Gateway input identity
	// ambiguous. The Host validates the same bytes against the signed claim
	// before execution.
	return request, nil
}

// canonicalAsyncProductSessionRef narrows the Backend-to-Host envelope to the
// stable product-thread identity. Worker-only binding fields are accepted only
// for compatibility and are discarded; arbitrary fields, local paths, signed
// URLs, and credentials fail before any Host request is issued.
func canonicalAsyncProductSessionRef(input map[string]any) (map[string]any, error) {
	if len(input) == 0 {
		return nil, fmt.Errorf("RUNTIME_INPUT_INVALID")
	}
	for key, value := range input {
		switch key {
		case "threadId", "openclawSessionKey":
			if _, ok := value.(string); !ok {
				return nil, fmt.Errorf("RUNTIME_INPUT_INVALID")
			}
		case "sessionStoreId", "agentProfile":
			if text, ok := value.(string); !ok || runtimeTransportForbiddenString(text) {
				return nil, fmt.Errorf("RUNTIME_INPUT_INVALID")
			}
		case "contextGeneration", "sessionGeneration":
			switch value.(type) {
			case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, json.Number:
			default:
				return nil, fmt.Errorf("RUNTIME_INPUT_INVALID")
			}
		case "metadata":
			metadata, ok := value.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("RUNTIME_INPUT_INVALID")
			}
			if _, valid := canonicalRuntimeProductSessionMetadata(metadata); !valid {
				return nil, fmt.Errorf("RUNTIME_INPUT_INVALID")
			}
		default:
			return nil, fmt.Errorf("RUNTIME_INPUT_INVALID")
		}
	}
	threadID, _ := input["threadId"].(string)
	sessionKey, _ := input["openclawSessionKey"].(string)
	if threadID != strings.TrimSpace(threadID) || sessionKey != strings.TrimSpace(sessionKey) || !validRuntimeProductSessionIdentity(threadID, sessionKey) {
		return nil, fmt.Errorf("RUNTIME_INPUT_INVALID")
	}
	return map[string]any{
		"threadId":           threadID,
		"openclawSessionKey": sessionKey,
	}, nil
}

type runtimeSubmitBudget struct {
	TimeoutSec   int
	MaxToolCalls int
}

func runtimeSubmitTransportBudget(budget RuntimeToolBudget) (runtimeSubmitBudget, bool) {
	if err := ValidateRuntimeToolBudget(budget); err != nil ||
		!validRuntimeTransportBudget(budget.MaxWallTimeSeconds, budget.MaxToolCalls, true) {
		return runtimeSubmitBudget{}, false
	}
	return runtimeSubmitBudget{TimeoutSec: budget.MaxWallTimeSeconds, MaxToolCalls: budget.MaxToolCalls}, true
}

func (c HTTPTransportOpenClawClient) runtimeHostRequestContext(ctx context.Context, timeoutSec int) (context.Context, context.CancelFunc) {
	timeout := c.Timeout
	if timeoutSec > 0 {
		// The adapter keeps a small terminal-write grace window around the
		// signed runtime budget. The per-Run budget intentionally overrides the
		// client default; it must never silently fall back to an old 60 second cap.
		timeout = time.Duration(timeoutSec+15) * time.Second
	}
	if timeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, timeout)
}

func (c HTTPTransportOpenClawClient) normalizeAsyncRuntimeStatus(result AsyncRuntimeStatus, expectedRunID string) (AsyncRuntimeStatus, error) {
	status, valid := normalizeAsyncRuntimeStatusValue(result.Status)
	if !valid || result.RunID != expectedRunID || result.LastEventSequence < 0 {
		return AsyncRuntimeStatus{}, fmt.Errorf("RUNTIME_FAILED")
	}
	result.Status = status
	result.RuntimeRequestID = sanitizeRuntimeOpaqueIdentifier(result.RuntimeRequestID)
	result.Result = sanitizeRuntimeResultMap(result.Result)
	result.Usage = sanitizeRuntimeUsage(result.Usage)
	if status == "aborted" {
		result.Error = sanitizeAsyncAbortedError(result.Error)
		return result, nil
	}
	if status == "failed" || status == "timeout" || status == "forbidden" {
		raw := map[string]any{
			"runId": result.RunID, "status": status, "result": result.Result,
			"error": result.Error, "usage": result.Usage,
		}
		normalized := c.NormalizeResult(raw)
		result.Error = normalized.SafeErrorSummary()
		// RuntimeEventWorker historically reads `message` for the explicit user
		// cancellation race. It is the same safe text, never a provider body.
		result.Error["message"] = normalized.SafeErrorSummary()["safeMessage"]
		return result, nil
	}
	if len(result.Error) > 0 {
		result.Error = sanitizeRuntimeDiagnosticMap(result.Error)
	}
	return result, nil
}

func normalizeAsyncRuntimeStatusValue(value string) (string, bool) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "accepted":
		return "accepted", true
	case "materializing":
		return "materializing", true
	case "running":
		return "running", true
	case "finalizing":
		return "finalizing", true
	case "aborting":
		return "aborting", true
	case "aborted":
		return "aborted", true
	case "succeeded", "success", "completed":
		return "succeeded", true
	case "failed", "failure", "error":
		return "failed", true
	case "timeout", "timed_out":
		return "timeout", true
	case "forbidden":
		return "forbidden", true
	default:
		return "", false
	}
}

func sanitizeAsyncAbortedError(input map[string]any) map[string]any {
	code := strings.ToUpper(strings.TrimSpace(firstRuntimeNonEmpty(openclawStringValue(input["code"]), openclawStringValue(input["errorCode"]))))
	if code != "RUNTIME_ABORTED" && code != "USER_CANCELLED" {
		code = "RUNTIME_ABORTED"
	}
	return map[string]any{"code": code, "message": "runtime aborted"}
}

func sanitizeRuntimeDiagnosticMap(input map[string]any) map[string]any {
	code := NormalizeRuntimeFailureCode(firstRuntimeNonEmpty(openclawStringValue(input["code"]), openclawStringValue(input["errorCode"])))
	if code == "" || code == RuntimeErrorFailed {
		code = classifyOpenClawFailure(0, openclawStringValue(input["providerCode"]), firstRuntimeNonEmpty(openclawStringValue(input["message"]), openclawStringValue(input["providerMessage"])))
	}
	summary := runtimeFailureSummary(code, "", 0,
		openclawStringValue(input["providerCode"]),
		firstRuntimeNonEmpty(openclawStringValue(input["providerMessage"]), openclawStringValue(input["message"])),
		firstRuntimeNonEmpty(openclawStringValue(input["providerRequestId"]), openclawStringValue(input["requestId"])),
		openclawStringValue(input["downstreamRequestId"]),
	)
	summary["message"] = summary["safeMessage"]
	return summary
}

func normalizeAsyncRuntimeEventPage(page AsyncRuntimeEventPage, afterSequence int64) (AsyncRuntimeEventPage, error) {
	if afterSequence < 0 || page.NextAfterSequence < 0 || page.OldestAvailableSequence < 0 || page.LatestSequence < 0 || page.TerminalSequence < 0 {
		return AsyncRuntimeEventPage{}, fmt.Errorf("RUNTIME_EVENT_GAP")
	}
	if page.OldestAvailableSequence > 0 && page.LatestSequence > 0 && page.OldestAvailableSequence > page.LatestSequence ||
		page.TerminalSequence > 0 && page.LatestSequence > 0 && page.TerminalSequence > page.LatestSequence {
		return AsyncRuntimeEventPage{}, fmt.Errorf("RUNTIME_EVENT_GAP")
	}
	previous := afterSequence
	for index := range page.Items {
		event := &page.Items[index]
		if event.Sequence < 1 || event.Sequence <= previous || !validRuntimeMetadataKey(event.EventType) ||
			page.LatestSequence > 0 && event.Sequence > page.LatestSequence {
			return AsyncRuntimeEventPage{}, fmt.Errorf("RUNTIME_EVENT_GAP")
		}
		if event.Status != "" {
			status, valid := normalizeAsyncRuntimeStatusValue(event.Status)
			if !valid {
				return AsyncRuntimeEventPage{}, fmt.Errorf("RUNTIME_EVENT_GAP")
			}
			event.Status = status
		}
		// Tool receipts are a separate hash-only transport contract. Validate
		// their original envelopes before generic redaction: otherwise a raw
		// requestBody/prompt/session field could be silently removed here and
		// make an invalid receipt look safe to the Event Worker.
		if strings.HasPrefix(event.EventType, "tool.call.") {
			if !IsRuntimeToolAuditEventType(event.EventType) || len(event.UsageDelta) != 0 {
				return AsyncRuntimeEventPage{}, fmt.Errorf("RUNTIME_EVENT_GAP")
			}
			payload, err := NormalizeRuntimeToolAuditEventPayload(event.EventType, event.Data, event.SafePayload)
			if err != nil {
				return AsyncRuntimeEventPage{}, fmt.Errorf("RUNTIME_EVENT_GAP")
			}
			event.Data = nil
			event.SafePayload = payload
			event.UsageDelta = nil
		} else {
			event.Data = sanitizeRuntimeResultMap(event.Data)
			event.SafePayload = sanitizeRuntimeResultMap(event.SafePayload)
			event.UsageDelta = sanitizeRuntimeUsage(event.UsageDelta)
		}
		previous = event.Sequence
	}
	if len(page.Items) > 0 && page.NextAfterSequence != 0 && page.NextAfterSequence != previous {
		return AsyncRuntimeEventPage{}, fmt.Errorf("RUNTIME_EVENT_GAP")
	}
	if len(page.Items) == 0 && page.HasMore {
		return AsyncRuntimeEventPage{}, fmt.Errorf("RUNTIME_EVENT_GAP")
	}
	return page, nil
}

func (c HTTPTransportOpenClawClient) doRuntimeHostJSON(ctx context.Context, host RuntimeHost, method, path, runTicket string, body any, target any) error {
	return c.doRuntimeHostJSONWithOptions(ctx, host, method, path, runTicket, body, target, runtimeHostRequestOptions{})
}

type runtimeHostRequestOptions struct {
	TimeoutSec   int
	MaxToolCalls int
}

func (c HTTPTransportOpenClawClient) doRuntimeHostJSONWithOptions(ctx context.Context, host RuntimeHost, method, path, runTicket string, body any, target any, options runtimeHostRequestOptions) error {
	if !validRuntimeTransportBudget(options.TimeoutSec, options.MaxToolCalls, options.TimeoutSec != 0 || options.MaxToolCalls != 0) {
		return fmt.Errorf("RUNTIME_INPUT_INVALID")
	}
	if host.Endpoint == "" || host.RuntimeHostID == "" {
		return fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
	}
	requestClient := c.httpClient()
	if c.RequireRuntimeHostMTLS {
		endpoint, err := url.Parse(host.Endpoint)
		if err != nil || !strings.EqualFold(endpoint.Scheme, "https") || endpoint.Host == "" || !RuntimeHostMTLSClientConfigured(c.HTTPClient) {
			return domain.ErrorCode("RUNTIME_HOST_UNAUTHORIZED")
		}
		boundClient, ok := c.HTTPClient.(*runtimeHostMTLSHTTPClient)
		if !ok {
			return domain.ErrorCode("RUNTIME_HOST_UNAUTHORIZED")
		}
		requestClient, err = boundClient.RuntimeHostClient(host)
		if err != nil {
			return domain.ErrorCode("RUNTIME_HOST_UNAUTHORIZED")
		}
	}
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(raw)
	}
	request, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(host.Endpoint, "/")+path, reader)
	if err != nil {
		return err
	}
	request.Header.Set("Accept", "application/json")
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if runTicket != "" {
		request.Header.Set("Authorization", "RunTicket "+runTicket)
	}
	request.Header.Set("X-Runtime-Host-Id", host.RuntimeHostID)
	if options.TimeoutSec > 0 {
		request.Header.Set(runtimeTimeoutHeader, strconv.Itoa(options.TimeoutSec))
	}
	if options.MaxToolCalls > 0 {
		request.Header.Set(runtimeMaxToolCallsHeader, strconv.Itoa(options.MaxToolCalls))
	}
	response, err := requestClient.Do(request)
	if err != nil {
		return fmt.Errorf("%s", MapTransportError(err))
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		raw, _ := io.ReadAll(io.LimitReader(response.Body, 64<<10))
		if code := classifiedRuntimeHostErrorCode(raw); code != "" {
			return domain.ErrorCode(code)
		}
		providerCode, providerMessage, providerRequestID := openclawProviderFieldsFromPayload(raw)
		classification := classifyOpenClawFailure(response.StatusCode, providerCode, providerMessage)
		if (response.StatusCode == http.StatusTooManyRequests || response.StatusCode == http.StatusServiceUnavailable) && classification == RuntimeErrorFailed {
			// A Host-level overload without a more specific registered Runtime or
			// provider error remains a capacity signal. Do not apply this fallback
			// when the bounded body identified configuration/auth/input failure.
			return domain.ErrorCode("RUNTIME_CAPACITY_UNAVAILABLE")
		}
		return newRuntimeDiagnosticError(
			classification,
			response.StatusCode,
			providerCode,
			providerMessage,
			firstRuntimeNonEmpty(providerRequestID, runtimeProviderRequestIDFromHeader(response.Header)),
			runtimeDownstreamRequestIDFromHeader(response.Header),
		)
	}
	if target == nil {
		return nil
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 2<<20)).Decode(target); err != nil {
		return fmt.Errorf("AI_RESULT_PARSE_FAILED")
	}
	return nil
}

func classifiedRuntimeHostErrorCode(raw []byte) string {
	var envelope struct {
		ErrorCode string `json:"errorCode"`
		Code      string `json:"code"`
		Error     struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if len(raw) == 0 || json.Unmarshal(raw, &envelope) != nil {
		return ""
	}
	code := strings.TrimSpace(envelope.ErrorCode)
	if code == "" {
		code = strings.TrimSpace(envelope.Code)
	}
	if code == "" {
		code = strings.TrimSpace(envelope.Error.Code)
	}
	switch code {
	case "RUNTIME_STORAGE_UNAVAILABLE", "RUNTIME_CAPACITY_UNAVAILABLE", "RUNTIME_ABORT_FAILED",
		"RUNTIME_EVENT_GAP", "RUNTIME_RUN_NOT_FOUND", "STALE_FENCING_TOKEN", "RUNTIME_PERMISSION_DENIED",
		"RUNTIME_WORKSPACE_MATERIALIZATION_FAILED", "RUNTIME_TOOL_UNAVAILABLE",
		"RUNTIME_TOOL_BUDGET_EXCEEDED", "RUNTIME_TIMEOUT", "AI_RESULT_PARSE_FAILED":
		return code
	default:
		return ""
	}
}
