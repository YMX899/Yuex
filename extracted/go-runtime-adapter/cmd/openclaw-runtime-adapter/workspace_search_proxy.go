package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	runtimepkg "huahuoai/backend/source/internal/runtime"
)

const (
	workspaceSearchProxyPath             = "/internal/v1/runtime/workspace-search"
	workspaceSearchProxyMaxRequestBytes  = 64 << 10
	workspaceSearchProxyMaxResponseBytes = 256 << 10
	workspaceSearchProxyToolCallIDHeader = "X-Huahuo-Tool-Call-Id"
)

func (a *adapter) newWorkspaceSearchProxyServer() (*http.Server, net.Listener, error) {
	if err := a.validateWorkspaceSearchProxyAddress(); err != nil {
		return nil, nil, err
	}
	listener, err := net.Listen("tcp", a.workspaceSearchProxyAddr)
	if err != nil {
		return nil, nil, err
	}
	return &http.Server{
		Handler:           a.workspaceSearchProxyHandler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       30 * time.Second,
	}, listener, nil
}

func (a *adapter) validateWorkspaceSearchProxyAddress() error {
	if a == nil {
		return fmt.Errorf("RUNTIME_INPUT_INVALID")
	}
	addr := strings.TrimSpace(a.workspaceSearchProxyAddr)
	if addr == "" {
		addr = defaultWorkspaceSearchProxyAddr
		a.workspaceSearchProxyAddr = addr
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil || host == "" || port == "" {
		return fmt.Errorf("RUNTIME_INPUT_INVALID")
	}
	ip := net.ParseIP(strings.Trim(host, "[]"))
	if ip == nil || !ip.IsLoopback() {
		return fmt.Errorf("RUNTIME_INPUT_INVALID")
	}
	value, err := strconv.Atoi(port)
	if err != nil || value < 1 || value > 65535 {
		return fmt.Errorf("RUNTIME_INPUT_INVALID")
	}
	return nil
}

func (a *adapter) workspaceSearchProxyHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != workspaceSearchProxyPath {
			writeJSON(w, http.StatusNotFound, map[string]any{"errorCode": "NOT_FOUND"})
			return
		}
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"errorCode": "METHOD_NOT_ALLOWED"})
			return
		}
		if !workspaceSearchProxyRemoteLoopback(r.RemoteAddr) {
			writeJSON(w, http.StatusForbidden, map[string]any{"errorCode": "RUNTIME_PERMISSION_DENIED"})
			return
		}
		a.proxyWorkspaceSearch(w, r)
	})
}

func workspaceSearchProxyRemoteLoopback(remoteAddr string) bool {
	host, _, err := net.SplitHostPort(strings.TrimSpace(remoteAddr))
	if err != nil {
		return false
	}
	ip := net.ParseIP(strings.Trim(host, "[]"))
	return ip != nil && ip.IsLoopback()
}

func (a *adapter) proxyWorkspaceSearch(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	toolCallID, validToolCallID := workspaceSearchProxyToolCallID(r.Header)
	if !validToolCallID {
		// Tool-call correlation is a private bridge header. Do not return the
		// caller-supplied value in an error body or log it at this boundary.
		writeJSON(w, http.StatusBadRequest, map[string]any{"errorCode": "WORKSPACE_SEARCH_INPUT_INVALID"})
		return
	}
	if !strings.HasPrefix(strings.ToLower(strings.TrimSpace(r.Header.Get("Content-Type"))), "application/json") {
		writeJSON(w, http.StatusBadRequest, map[string]any{"errorCode": "WORKSPACE_SEARCH_INPUT_INVALID"})
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, workspaceSearchProxyMaxRequestBytes+1))
	if err != nil || len(body) == 0 || len(body) > workspaceSearchProxyMaxRequestBytes || !validWorkspaceSearchProxyJSON(body) {
		writeJSON(w, http.StatusBadRequest, map[string]any{"errorCode": "WORKSPACE_SEARCH_INPUT_INVALID"})
		return
	}
	claims, err := a.verifyWorkspaceSearchProxyTicket(r.Header.Get("Authorization"), r.Header.Get("X-Run-Id"))
	if err != nil {
		writeJSON(w, http.StatusForbidden, map[string]any{"errorCode": "RUNTIME_PERMISSION_DENIED"})
		return
	}
	endpoint, err := a.workspaceSearchBackendEndpoint()
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"errorCode": "WORKSPACE_SEARCH_UNAVAILABLE"})
		return
	}
	request, err := http.NewRequestWithContext(r.Context(), http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"errorCode": "WORKSPACE_SEARCH_UNAVAILABLE"})
		return
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Authorization", "RunTicket "+runTicketFromAuthorization(r.Header.Get("Authorization")))
	request.Header.Set("X-Run-Id", claims.RunID)
	request.Header.Set("X-Runtime-Host-Id", a.runtimeHostID)
	request.Header.Set("X-Runtime-Instance-Id", a.runtimeInstanceID)
	request.Header.Set("X-Runtime-Environment", a.runtimeEnvironment)
	request.Header.Set(workspaceSearchProxyToolCallIDHeader, toolCallID)
	if traceID := safeWorkspaceSearchTraceID(r.Header.Get("X-Trace-Id")); traceID != "" {
		request.Header.Set("X-Trace-Id", traceID)
	}
	client := a.backendHTTPClient
	if client == nil {
		if adapterProductionLike(a.runtimeEnvironment) {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{"errorCode": "WORKSPACE_SEARCH_UNAVAILABLE"})
			return
		}
		client = http.DefaultClient
	}
	response, err := client.Do(request)
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"errorCode": "WORKSPACE_SEARCH_UNAVAILABLE"})
		return
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(io.LimitReader(response.Body, workspaceSearchProxyMaxResponseBytes+1))
	if err != nil || len(responseBody) > workspaceSearchProxyMaxResponseBytes {
		writeJSON(w, http.StatusBadGateway, map[string]any{"errorCode": "WORKSPACE_SEARCH_RESPONSE_INVALID"})
		return
	}
	if response.StatusCode >= http.StatusOK && response.StatusCode < http.StatusMultipleChoices {
		responseBody, err = sanitizeWorkspaceSearchProxySuccessResponse(responseBody)
		if err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]any{"errorCode": "WORKSPACE_SEARCH_RESPONSE_INVALID"})
			return
		}
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(response.StatusCode)
	_, _ = w.Write(responseBody)
}

// sanitizeWorkspaceSearchProxySuccessResponse provides defense in depth for
// the model-visible hop. The Backend route already projects an allow-listed
// response, but an older/misconfigured Backend must not disclose provider
// deployment identity through the Adapter relay.
func sanitizeWorkspaceSearchProxySuccessResponse(raw []byte) ([]byte, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	var payload any
	if err := decoder.Decode(&payload); err != nil {
		return nil, err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return nil, fmt.Errorf("invalid response JSON")
	}
	removeWorkspaceSearchProviderIdentity(payload)
	return json.Marshal(payload)
}

func removeWorkspaceSearchProviderIdentity(value any) {
	switch typed := value.(type) {
	case map[string]any:
		delete(typed, "embeddingModel")
		delete(typed, "embeddingVersion")
		for _, child := range typed {
			removeWorkspaceSearchProviderIdentity(child)
		}
	case []any:
		for _, child := range typed {
			removeWorkspaceSearchProviderIdentity(child)
		}
	}
}

func (a *adapter) verifyWorkspaceSearchProxyTicket(authorization, runID string) (runtimepkg.RunTicketClaims, error) {
	ticket := runTicketFromAuthorization(authorization)
	if a == nil || ticket == "" || strings.TrimSpace(runID) == "" || strings.TrimSpace(a.runTicketSecret) == "" || strings.TrimSpace(a.runtimeHostID) == "" {
		return runtimepkg.RunTicketClaims{}, fmt.Errorf("RUNTIME_PERMISSION_DENIED")
	}
	claims, err := runtimepkg.VerifyRunTicket(ticket, a.runTicketSecret, time.Now().UTC())
	if err != nil || claims.RunID != strings.TrimSpace(runID) || claims.RuntimeHostID != a.runtimeHostID {
		return runtimepkg.RunTicketClaims{}, fmt.Errorf("RUNTIME_PERMISSION_DENIED")
	}
	return claims, nil
}

func (a *adapter) workspaceSearchBackendEndpoint() (string, error) {
	if a == nil {
		return "", fmt.Errorf("WORKSPACE_SEARCH_UNAVAILABLE")
	}
	base, err := url.Parse(strings.TrimSpace(a.backendURL))
	if err != nil || base.Host == "" || base.User != nil || base.RawQuery != "" || base.Fragment != "" || (base.Path != "" && base.Path != "/") {
		return "", fmt.Errorf("WORKSPACE_SEARCH_UNAVAILABLE")
	}
	if adapterProductionLike(a.runtimeEnvironment) && !adapterInternalHTTPSURL(a.backendURL) {
		return "", fmt.Errorf("WORKSPACE_SEARCH_UNAVAILABLE")
	}
	if !adapterProductionLike(a.runtimeEnvironment) && base.Scheme != "http" && base.Scheme != "https" {
		return "", fmt.Errorf("WORKSPACE_SEARCH_UNAVAILABLE")
	}
	base.Path = workspaceSearchProxyPath
	return base.String(), nil
}

func validWorkspaceSearchProxyJSON(raw []byte) bool {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	value := map[string]any{}
	if decoder.Decode(&value) != nil || len(value) == 0 {
		return false
	}
	var extra any
	return decoder.Decode(&extra) == io.EOF
}

func safeWorkspaceSearchTraceID(value string) string {
	value = strings.TrimSpace(value)
	if len(value) == 0 || len(value) > 128 {
		return ""
	}
	for _, character := range value {
		if !(character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || character >= '0' && character <= '9' || character == '.' || character == '_' || character == '-') {
			return ""
		}
	}
	return value
}

// workspaceSearchProxyToolCallID accepts the opaque correlation identifier
// only when the local request contains exactly one safe header value. Scan all
// header keys case-insensitively so a manually constructed request cannot use
// different key casing to bypass the one-value rule.
func workspaceSearchProxyToolCallID(header http.Header) (string, bool) {
	values := make([]string, 0, 1)
	for name, candidateValues := range header {
		if strings.EqualFold(name, workspaceSearchProxyToolCallIDHeader) {
			values = append(values, candidateValues...)
		}
	}
	if len(values) != 1 {
		return "", false
	}
	toolCallID := values[0]
	if strings.TrimSpace(toolCallID) != toolCallID || !validWorkspaceSearchProxyToolCallID(toolCallID) {
		return "", false
	}
	return toolCallID, true
}

func validWorkspaceSearchProxyToolCallID(value string) bool {
	if len(value) == 0 || len(value) > 256 {
		return false
	}
	for _, character := range value {
		if character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' ||
			character >= '0' && character <= '9' || character == '.' || character == '_' || character == ':' || character == '-' {
			continue
		}
		return false
	}
	return true
}
