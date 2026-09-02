package routes

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"huahuoai/backend/source/internal/api"
	"huahuoai/backend/source/internal/config"
	"huahuoai/backend/source/internal/domain"
	"huahuoai/backend/source/internal/persistence"
	"huahuoai/backend/source/internal/services"
	"huahuoai/backend/source/internal/transport"
)

const agentRunEventStreamPageSize = 500

var agentRunEventReplaySlots = make(chan struct{}, 32)

type confirmAgentRunRequest struct {
	ExpectedPlanVersion int    `json:"expectedPlanVersion"`
	Decision            string `json:"decision"`
}
type cancelAgentRunRequest struct {
	Reason string `json:"reason"`
}

func RegisterAgentRunRoutes(app *api.App) {
	app.AddWithStatus(http.MethodPost, "/api/v1/agent/runs", api.AuthRequired, true, http.StatusAccepted, createAgentRun)
	app.Add(http.MethodGet, "/api/v1/agent/runs/{agentRunId}", api.AuthRequired, false, getAgentRun)
	app.Add(http.MethodGet, "/api/v1/agent/runs/{agentRunId}/events", api.AuthRequired, false, listAgentRunEvents)
	app.AddStream(http.MethodGet, "/api/v1/agent/runs/{agentRunId}/events/stream", api.AuthRequired, streamAgentRunEvents)
	app.AddWithStatus(http.MethodPost, "/api/v1/agent/runs/{agentRunId}/confirm", api.AuthRequired, true, http.StatusAccepted, confirmAgentRun)
	app.AddWithStatus(http.MethodPost, "/api/v1/agent/runs/{agentRunId}/cancel", api.AuthRequired, true, http.StatusAccepted, cancelAgentRun)
}

func createAgentRun(ctx *api.Context) (any, *domain.APIError) {
	var request domain.AgentRunRequest
	if err := api.DecodeStrictJSON(ctx.Request, &request); err != nil {
		return nil, err
	}
	if err := validatePublicAgentRunCreateRequest(request); err != nil {
		return nil, err
	}
	record, err := agentRunService(ctx).Create(ctx.Request.Context(), agentAuthContext(ctx), request, ctx.IdempotencyKey)
	if err != nil {
		return nil, toRouteAPIError(err)
	}
	return map[string]any{"run": record, "nextAction": map[string]any{"type": "poll_agent_run", "agentRunId": record.AgentRunID, "afterSequence": 0}}, nil
}

// validatePublicAgentRunCreateRequest protects the narrow App-facing contract
// even though AgentRunRequest is also used by server-owned bridges. In
// particular, ClientContext is intentionally open for ordinary App metadata,
// but it must never become a backdoor for a profile, provider, Runtime, tool,
// session, or filesystem selector.
func validatePublicAgentRunCreateRequest(request domain.AgentRunRequest) *domain.APIError {
	if request.IntentHint.TaskType != "" || request.IntentHint.Operation != "" || len(request.BusinessRefs) > 0 {
		return domain.ErrorCode("INVALID_ARGUMENT")
	}
	for key := range request.ClientContext {
		if publicAgentRunReservedClientContextKey(key) {
			return domain.ErrorCode("INVALID_ARGUMENT")
		}
	}
	return nil
}

func publicAgentRunReservedClientContextKey(key string) bool {
	// ClientContext remains open for ordinary presentation metadata, but its
	// spelling must not create an alternate path to Runtime tool selection.
	// In particular, OpenClaw's spec.tools.allow is an internal Runtime field,
	// not an App input. Normalize its dotted spelling with the existing
	// case/underscore/hyphen variants before comparing reserved names.
	canonical := strings.ToLower(strings.NewReplacer("_", "", "-", "", ".", "").Replace(strings.TrimSpace(key)))
	switch canonical {
	case "compatibilitytaskprojection", "userid", "tenantid", "tasktype",
		"agentprofile", "l1agentprofile", "skillprofile", "selectedskillprofiles",
		"runtimeconfigid", "runtimeconfigversion", "model", "provider", "authpool", "authpoolid",
		"toolpolicy", "toolpolicyprofile", "tools", "requiredtools", "spectoolsallow",
		"prompt", "prompttemplate", "prompttemplateid", "prompttemplateversion",
		"sessionkey", "openclawsessionkey", "productsession", "productthread",
		"path", "realpath", "workspacepath", "workspacedir", "rootpath", "relativeroot":
		return true
	default:
		return false
	}
}
func getAgentRun(ctx *api.Context) (any, *domain.APIError) {
	record, err := agentRunService(ctx).Get(ctx.Request.Context(), agentAuthContext(ctx), ctx.Params["agentRunId"])
	if err != nil {
		return nil, toRouteAPIError(err)
	}
	return record, nil
}
func listAgentRunEvents(ctx *api.Context) (any, *domain.APIError) {
	after, err := strconv.ParseInt(ctx.Request.URL.Query().Get("afterSequence"), 10, 64)
	if ctx.Request.URL.Query().Get("afterSequence") == "" {
		after = 0
		err = nil
	}
	limit, limitErr := strconv.Atoi(ctx.Request.URL.Query().Get("limit"))
	if ctx.Request.URL.Query().Get("limit") == "" {
		limit = 100
		limitErr = nil
	}
	if err != nil || limitErr != nil {
		return nil, domain.ErrorCode("INVALID_ARGUMENT")
	}
	page, serviceErr := agentRunService(ctx).ListEvents(ctx.Request.Context(), agentAuthContext(ctx), ctx.Params["agentRunId"], after, limit)
	if serviceErr != nil {
		return nil, toRouteAPIError(serviceErr)
	}
	return page, nil
}

func streamAgentRunEvents(ctx *api.Context) *domain.APIError {
	httpConfig, err := transport.LoadHTTPConfig(ctx.Services.Settings.Environment)
	if err != nil {
		return domain.ErrorCode(transport.RuntimeTransportConfigInvalid)
	}
	cursor, cursorErr := exclusiveAgentRunEventCursor(ctx.Request)
	if cursorErr != nil {
		return cursorErr
	}
	runID := ctx.Params["agentRunId"]
	record, err := agentRunService(ctx).Get(ctx.Request.Context(), agentAuthContext(ctx), runID)
	if err != nil {
		return toRouteAPIError(err)
	}
	if ctx.Services == nil || ctx.Services.Repos == nil || ctx.Services.Repos.AgentRuns == nil {
		return domain.ErrorCode("SERVICE_BUSY")
	}
	if configErr := config.ValidateSSEAdmissionSettings(ctx.Services.Settings.Environment, ctx.Services.Settings.SSEAdmission); configErr != nil {
		return domain.ErrorCode(transport.RuntimeTransportConfigInvalid)
	}
	if fanoutErr := requireAgentRunEventFanout(ctx); fanoutErr != nil {
		return fanoutErr
	}
	if productionLikeSSEEnvironment(ctx.Services.Settings.Environment) && strings.TrimSpace(ctx.Services.InstanceID) == "" {
		// Production leases must retain the configured API instance identity.
		// Do not replace a missing process identity with a generated local value.
		return domain.ErrorCode(transport.RuntimeCapacityUnavailable)
	}
	if ctx.Services.SSEAdmission == nil {
		return domain.ErrorCode(transport.RuntimeCapacityUnavailable)
	}
	admission := ctx.Services.SSEAdmission
	lease, admissionErr := admission.AcquireSSEConnection(ctx.Request.Context(), transport.SSEAdmissionCommand{
		TenantID: record.TenantID, UserID: record.UserID, AgentRunID: runID, APIInstanceID: ctx.Services.InstanceID,
	})
	if admissionErr != nil {
		return agentRunSSEAdmissionError(ctx, admissionErr)
	}
	defer func() { releaseAgentRunSSEAdmission(admission, lease) }()

	wakeup, unsubscribe := ctx.Services.Repos.AgentRuns.SubscribePublicEvents(runID)
	defer unsubscribe()
	page, err := ctx.Services.Repos.AgentRuns.ListPublicEvents(ctx.Request.Context(), runID, cursor, agentRunEventStreamPageSize)
	if err != nil {
		return domain.ErrorCode("SERVICE_BUSY")
	}
	page = services.ProjectPublicAgentRunEventPage(page)

	stream := newAgentRunSSEWriter(ctx.ResponseWriter, httpConfig.SSEWriteTimeout)
	started, startErr := stream.start()
	if startErr != nil {
		if !started {
			return domain.ErrorCode(transport.RuntimeTransportConfigInvalid)
		}
		return nil
	}
	heartbeat := time.NewTicker(httpConfig.SSEHeartbeat)
	defer heartbeat.Stop()
	renewInterval := admission.SSEAdmissionConfig().RenewInterval
	if renewInterval <= 0 {
		return nil
	}
	renewal := time.NewTicker(renewInterval)
	defer renewal.Stop()
	for {
		terminal, nextCursor, writeErr := stream.writePage(page, cursor)
		if writeErr != nil || terminal {
			return nil
		}
		cursor = nextCursor
		if page.HasMore {
			page, err = ctx.Services.Repos.AgentRuns.ListPublicEvents(ctx.Request.Context(), runID, cursor, agentRunEventStreamPageSize)
			if err != nil {
				return nil
			}
			page = services.ProjectPublicAgentRunEventPage(page)
			continue
		}

		select {
		case <-ctx.Request.Context().Done():
			return nil
		case <-renewal.C:
			renewed, renewErr := admission.RenewSSEConnection(ctx.Request.Context(), lease)
			if renewErr != nil {
				// Do not advance Last-Event-ID for an admission-control failure.
				// The client resumes from the last durable event after reconnecting.
				_ = stream.writeAdmissionRenewalError()
				return nil
			}
			lease = renewed
			continue
		case <-heartbeat.C:
			if err := stream.writeHeartbeat(); err != nil {
				return nil
			}
			continue
		case <-wakeup:
		}

		page, err = listAgentRunEventsBounded(ctx, runID, cursor)
		if err != nil {
			return nil
		}
	}
}

func requireAgentRunEventFanout(ctx *api.Context) *domain.APIError {
	if ctx == nil || ctx.Services == nil || ctx.Services.Repos == nil || ctx.Services.Repos.AgentRuns == nil {
		return domain.ErrorCode("SERVICE_BUSY")
	}
	checkContext, cancel := context.WithTimeout(ctx.Request.Context(), 3*time.Second)
	defer cancel()
	health, err := ctx.Services.Repos.AgentRuns.EventNotifierHealth(checkContext)
	if err != nil || !health.OK {
		return domain.ErrorCode(transport.RuntimeCapacityUnavailable)
	}
	if productionLikeSSEEnvironment(ctx.Services.Settings.Environment) && health.Backend != "redis_pubsub" {
		return domain.ErrorCode(transport.RuntimeCapacityUnavailable)
	}
	return nil
}

func productionLikeSSEEnvironment(environment string) bool {
	switch strings.ToLower(strings.TrimSpace(environment)) {
	case "prelaunch", "prod", "production":
		return true
	default:
		return false
	}
}

func agentRunSSEAdmissionError(ctx *api.Context, err error) *domain.APIError {
	code, scope, retryAfter := transport.SSEAdmissionErrorDetails(err)
	if code != transport.RuntimeAdmissionRejected {
		if code == transport.RuntimeTransportConfigInvalid {
			return domain.ErrorCode(transport.RuntimeTransportConfigInvalid)
		}
		return domain.ErrorCode(transport.RuntimeCapacityUnavailable)
	}
	if scope != transport.SSEAdmissionScopeGlobal && scope != transport.SSEAdmissionScopeTenant && scope != transport.SSEAdmissionScopeUser {
		return domain.ErrorCode(transport.RuntimeCapacityUnavailable)
	}
	retryAfterSeconds := int((retryAfter + time.Second - 1) / time.Second)
	if retryAfterSeconds <= 0 {
		retryAfterSeconds = 1
	}
	ctx.ResponseWriter.Header().Set("Retry-After", strconv.Itoa(retryAfterSeconds))
	apiErr := domain.ErrorCode(transport.RuntimeAdmissionRejected)
	apiErr.Details = map[string]any{
		"limitScope":   string(scope),
		"retryAfterMs": retryAfter.Milliseconds(),
	}
	return apiErr
}

func releaseAgentRunSSEAdmission(admission transport.SSEAdmission, lease transport.SSEAdmissionLease) {
	if admission == nil {
		return
	}
	releaseContext, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, _ = admission.ReleaseSSEConnection(releaseContext, lease)
}

func exclusiveAgentRunEventCursor(request *http.Request) (int64, *domain.APIError) {
	queryValues, queryPresent := request.URL.Query()["afterSequence"]
	if queryPresent && len(queryValues) != 1 {
		return 0, domain.ErrorCode("INVALID_ARGUMENT")
	}
	queryRaw := ""
	if queryPresent {
		queryRaw = strings.TrimSpace(queryValues[0])
		if queryRaw == "" {
			return 0, domain.ErrorCode("INVALID_ARGUMENT")
		}
	}
	headerValues := request.Header.Values("Last-Event-ID")
	if len(headerValues) > 1 {
		return 0, domain.ErrorCode("INVALID_ARGUMENT")
	}
	headerRaw := ""
	if len(headerValues) == 1 {
		headerRaw = strings.TrimSpace(headerValues[0])
		if headerRaw == "" {
			return 0, domain.ErrorCode("INVALID_ARGUMENT")
		}
	}
	queryCursor, err := parseAgentRunEventCursor(queryRaw, queryPresent)
	if err != nil {
		return 0, domain.ErrorCode("INVALID_ARGUMENT")
	}
	headerCursor, err := parseAgentRunEventCursor(headerRaw, headerRaw != "")
	if err != nil {
		return 0, domain.ErrorCode("INVALID_ARGUMENT")
	}
	if queryPresent && headerRaw != "" && queryCursor != headerCursor {
		return 0, domain.ErrorCode("INVALID_ARGUMENT")
	}
	if headerRaw != "" {
		return headerCursor, nil
	}
	return queryCursor, nil
}

func parseAgentRunEventCursor(raw string, present bool) (int64, error) {
	if !present {
		return 0, nil
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value < 0 {
		return 0, fmt.Errorf("invalid event cursor")
	}
	return value, nil
}

func listAgentRunEventsBounded(ctx *api.Context, runID string, cursor int64) (persistence.AgentRunEventPage, error) {
	pollContext, cancel := context.WithTimeout(ctx.Request.Context(), 2*time.Second)
	defer cancel()
	select {
	case agentRunEventReplaySlots <- struct{}{}:
		defer func() { <-agentRunEventReplaySlots }()
	case <-pollContext.Done():
		return persistence.AgentRunEventPage{}, pollContext.Err()
	}
	page, err := ctx.Services.Repos.AgentRuns.ListPublicEvents(pollContext, runID, cursor, agentRunEventStreamPageSize)
	if err != nil {
		return persistence.AgentRunEventPage{}, err
	}
	return services.ProjectPublicAgentRunEventPage(page), nil
}

type agentRunSSEWriter struct {
	response   http.ResponseWriter
	controller *http.ResponseController
	writeLimit time.Duration
}

func newAgentRunSSEWriter(response http.ResponseWriter, writeLimit time.Duration) agentRunSSEWriter {
	return agentRunSSEWriter{response: response, controller: http.NewResponseController(response), writeLimit: writeLimit}
}

func (w agentRunSSEWriter) start() (bool, error) {
	if !supportsAgentRunSSEWriter(w.response) {
		return false, http.ErrNotSupported
	}
	if err := w.prepareWrite(); err != nil {
		return false, err
	}
	w.response.Header().Set("Content-Type", "text/event-stream")
	w.response.Header().Set("Cache-Control", "no-cache, no-transform")
	w.response.Header().Set("Connection", "keep-alive")
	w.response.Header().Set("X-Accel-Buffering", "no")
	w.response.WriteHeader(http.StatusOK)
	return true, w.flush()
}

func supportsAgentRunSSEWriter(response http.ResponseWriter) bool {
	for response != nil {
		_, hasWriteDeadline := response.(interface{ SetWriteDeadline(time.Time) error })
		_, hasFlushError := response.(interface{ FlushError() error })
		_, hasFlush := response.(http.Flusher)
		if hasWriteDeadline && (hasFlushError || hasFlush) {
			return true
		}
		unwrapper, ok := response.(interface{ Unwrap() http.ResponseWriter })
		if !ok {
			return false
		}
		response = unwrapper.Unwrap()
	}
	return false
}

func (w agentRunSSEWriter) writePage(page persistence.AgentRunEventPage, cursor int64) (bool, int64, error) {
	if page.Gap {
		resumeAfter := page.OldestAvailableSequence - 1
		if resumeAfter < 0 {
			resumeAfter = 0
		}
		payload := map[string]any{"error": map[string]any{
			"code":                    "RUNTIME_EVENT_GAP",
			"retryable":               false,
			"oldestAvailableSequence": page.OldestAvailableSequence,
			"latestSequence":          page.LatestSequence,
			"resumeAfterSequence":     resumeAfter,
		}}
		return true, resumeAfter, w.write(strconv.FormatInt(resumeAfter, 10), "gap", payload)
	}
	terminalEventWritten := false
	for index, event := range page.Items {
		eventName := safeAgentRunSSEEventName(event.EventType)
		lastEventOnCurrentSnapshot := index == len(page.Items)-1 && !page.HasMore
		if (page.TerminalSequence != nil && event.Sequence == *page.TerminalSequence && event.Sequence == page.LatestSequence) ||
			(lastEventOnCurrentSnapshot && terminalAgentRunSSEEvent(event)) {
			eventName = "terminal"
			terminalEventWritten = true
		}
		if err := w.write(strconv.FormatInt(event.Sequence, 10), eventName, event); err != nil {
			return false, cursor, err
		}
		cursor = event.Sequence
	}
	terminal := terminalEventWritten || (page.TerminalSequence != nil && cursor >= *page.TerminalSequence && cursor >= page.LatestSequence)
	return terminal, cursor, nil
}

func terminalAgentRunSSEEvent(event persistence.AgentRunEvent) bool {
	switch event.EventType {
	case "succeeded", "failed", "cancelled", "timeout":
		return true
	}
	switch event.Status {
	case "succeeded", "failed", "cancelled", "timeout":
		return true
	default:
		return false
	}
}

func safeAgentRunSSEEventName(value string) string {
	if value == "" {
		return "message"
	}
	for _, char := range value {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') || (char >= '0' && char <= '9') || char == '_' || char == '-' || char == '.' {
			continue
		}
		return "message"
	}
	return value
}

func (w agentRunSSEWriter) writeHeartbeat() error {
	if err := w.prepareWrite(); err != nil {
		return err
	}
	if _, err := fmt.Fprint(w.response, ": heartbeat\n\n"); err != nil {
		return err
	}
	return w.flush()
}

func (w agentRunSSEWriter) writeAdmissionRenewalError() error {
	return w.write("", "error", map[string]any{"error": map[string]any{
		"code":      transport.RuntimeCapacityUnavailable,
		"retryable": true,
	}})
}

func (w agentRunSSEWriter) write(id, eventName string, payload any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	if err := w.prepareWrite(); err != nil {
		return err
	}
	if id != "" {
		if _, err := fmt.Fprintf(w.response, "id: %s\n", id); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintf(w.response, "event: %s\ndata: %s\n\n", eventName, raw); err != nil {
		return err
	}
	return w.flush()
}

func (w agentRunSSEWriter) prepareWrite() error {
	if w.writeLimit <= 0 {
		return domain.ErrorCode(transport.RuntimeTransportConfigInvalid)
	}
	err := w.controller.SetWriteDeadline(time.Now().Add(w.writeLimit))
	return err
}

func (w agentRunSSEWriter) flush() error {
	return w.controller.Flush()
}
func confirmAgentRun(ctx *api.Context) (any, *domain.APIError) {
	var request confirmAgentRunRequest
	if err := api.DecodeStrictJSON(ctx.Request, &request); err != nil {
		return nil, err
	}
	if request.Decision != "approve" {
		return nil, domain.ErrorCode("INVALID_ARGUMENT")
	}
	record, err := agentRunService(ctx).Confirm(ctx.Request.Context(), agentAuthContext(ctx), ctx.Params["agentRunId"], request.ExpectedPlanVersion, ctx.IdempotencyKey)
	if err != nil {
		return nil, toRouteAPIError(err)
	}
	return record, nil
}
func cancelAgentRun(ctx *api.Context) (any, *domain.APIError) {
	var request cancelAgentRunRequest
	if err := api.DecodeStrictJSON(ctx.Request, &request); err != nil {
		return nil, err
	}
	if request.Reason != "user_cancelled" {
		return nil, domain.ErrorCode("INVALID_ARGUMENT")
	}
	record, err := agentRunService(ctx).Cancel(ctx.Request.Context(), agentAuthContext(ctx), ctx.Params["agentRunId"], "USER_CANCELLED", ctx.IdempotencyKey)
	if err != nil {
		return nil, toRouteAPIError(err)
	}
	return record, nil
}
func agentRunService(ctx *api.Context) services.AgentRunService {
	return services.NewAgentRunServiceWithMetaWorkspaceAdmission(ctx.Services.Repos, ctx.Services.Now, newMetaWorkspaceAdmissionService(ctx.Services))
}
func agentAuthContext(ctx *api.Context) domain.AuthContext {
	return domain.AuthContext{TenantID: ctx.TenantID, UserID: ctx.UserID, WorkspaceID: ctx.WorkspaceID}
}
func toRouteAPIError(err error) *domain.APIError {
	if apiErr, ok := err.(*domain.APIError); ok {
		return apiErr
	}
	return domain.ErrorCode("INTERNAL_ERROR")
}
