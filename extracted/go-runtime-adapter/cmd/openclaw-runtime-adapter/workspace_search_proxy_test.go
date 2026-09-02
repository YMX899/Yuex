package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	runtimepkg "huahuoai/backend/source/internal/runtime"
)

func TestWorkspaceSearchProxyForwardsOnlyBoundLoopbackRequest(t *testing.T) {
	secret := strings.Repeat("s", 32)
	ticket := workspaceSearchProxyTicket(t, secret, "run_proxy", "host_proxy")
	toolCallID := "tool-call_proxy:1"
	backendCalls := 0
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backendCalls++
		if r.Method != http.MethodPost || r.URL.Path != workspaceSearchProxyPath || r.Header.Get("Authorization") != "RunTicket "+ticket ||
			r.Header.Get("X-Run-Id") != "run_proxy" || r.Header.Get("X-Runtime-Host-Id") != "host_proxy" ||
			r.Header.Get("X-Runtime-Instance-Id") != "instance_proxy" || r.Header.Get("X-Runtime-Environment") != "test" ||
			r.Header.Get("X-Trace-Id") != "trace_proxy" || r.Header.Get(workspaceSearchProxyToolCallIDHeader) != toolCallID {
			t.Fatalf("forwarded request=%#v", r.Header)
		}
		var request map[string]any
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil || request["query"] != "pricing meeting" {
			t.Fatalf("forwarded body=%#v err=%v", request, err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"success":true,"data":{"embeddingModel":"private-provider-deployment","embeddingVersion":"private:1536:v1","results":[{"path":"materials/pricing.md","kind":"minutes","embeddingModel":"nested-private-provider"}]}}`))
	}))
	defer backend.Close()

	adapter := &adapter{
		backendURL: backend.URL, backendHTTPClient: backend.Client(), runtimeHostID: "host_proxy", runtimeInstanceID: "instance_proxy",
		runtimeEnvironment: "test", runTicketSecret: secret, workspaceSearchProxyAddr: "127.0.0.1:18791",
	}
	req := httptest.NewRequest(http.MethodPost, workspaceSearchProxyPath, strings.NewReader(`{"query":"pricing meeting","retrievalMode":"hybrid"}`))
	req.RemoteAddr = "127.0.0.1:45001"
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "RunTicket "+ticket)
	req.Header.Set("X-Run-Id", "run_proxy")
	req.Header.Set("X-Trace-Id", "trace_proxy")
	req.Header.Set(workspaceSearchProxyToolCallIDHeader, toolCallID)
	rec := httptest.NewRecorder()
	adapter.workspaceSearchProxyHandler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || backendCalls != 1 || strings.Contains(rec.Body.String(), ticket) || !strings.Contains(rec.Body.String(), "materials/pricing.md") || strings.Contains(rec.Body.String(), "embeddingModel") || strings.Contains(rec.Body.String(), "embeddingVersion") || strings.Contains(rec.Body.String(), "private-provider") {
		t.Fatalf("proxy status=%d calls=%d body=%s", rec.Code, backendCalls, rec.Body.String())
	}
}

func TestWorkspaceSearchProxyRejectsMalformedSuccessfulBackendJSON(t *testing.T) {
	secret := strings.Repeat("s", 32)
	ticket := workspaceSearchProxyTicket(t, secret, "run_proxy_bad_json", "host_proxy_bad_json")
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"success":true`))
	}))
	defer backend.Close()
	adapter := &adapter{backendURL: backend.URL, backendHTTPClient: backend.Client(), runtimeHostID: "host_proxy_bad_json", runtimeInstanceID: "instance_proxy", runtimeEnvironment: "test", runTicketSecret: secret}
	req := httptest.NewRequest(http.MethodPost, workspaceSearchProxyPath, strings.NewReader(`{"query":"pricing"}`))
	req.RemoteAddr = "127.0.0.1:45001"
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "RunTicket "+ticket)
	req.Header.Set("X-Run-Id", "run_proxy_bad_json")
	req.Header.Set(workspaceSearchProxyToolCallIDHeader, "tool-call_bad-json:1")
	rec := httptest.NewRecorder()
	adapter.workspaceSearchProxyHandler().ServeHTTP(rec, req)
	if rec.Code != http.StatusBadGateway || strings.Contains(rec.Body.String(), "embedding") {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestWorkspaceSearchProxyRejectsNonLoopbackAndForgedRunBinding(t *testing.T) {
	secret := strings.Repeat("s", 32)
	ticket := workspaceSearchProxyTicket(t, secret, "run_proxy_reject", "host_proxy_reject")
	backendCalls := 0
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backendCalls++
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"success":true,"data":{"results":[]}}`))
	}))
	defer backend.Close()
	adapter := &adapter{
		backendURL: backend.URL, backendHTTPClient: backend.Client(), runtimeHostID: "host_proxy_reject", runtimeInstanceID: "instance_proxy",
		runtimeEnvironment: "test", runTicketSecret: secret,
	}
	for name, mutate := range map[string]func(*http.Request){
		"non loopback": func(request *http.Request) { request.RemoteAddr = "198.51.100.1:45001" },
		"wrong run":    func(request *http.Request) { request.Header.Set("X-Run-Id", "run_other") },
		"wrong host ticket": func(request *http.Request) {
			wrong := workspaceSearchProxyTicket(t, secret, "run_proxy_reject", "host_other")
			request.Header.Set("Authorization", "RunTicket "+wrong)
		},
	} {
		t.Run(name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, workspaceSearchProxyPath, strings.NewReader(`{"query":"pricing"}`))
			req.RemoteAddr = "127.0.0.1:45001"
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", "RunTicket "+ticket)
			req.Header.Set("X-Run-Id", "run_proxy_reject")
			req.Header.Set(workspaceSearchProxyToolCallIDHeader, "tool-call_reject:1")
			mutate(req)
			rec := httptest.NewRecorder()
			adapter.workspaceSearchProxyHandler().ServeHTTP(rec, req)
			if rec.Code != http.StatusForbidden || strings.Contains(rec.Body.String(), ticket) {
				t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
			}
		})
	}
	if backendCalls != 0 {
		t.Fatalf("rejected requests reached backend calls=%d", backendCalls)
	}
}

func TestWorkspaceSearchProxyRejectsInvalidToolCallIDBeforeBackendIO(t *testing.T) {
	secret := strings.Repeat("s", 32)
	ticket := workspaceSearchProxyTicket(t, secret, "run_proxy_tool_call", "host_proxy_tool_call")
	backendCalls := 0
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backendCalls++
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"success":true,"data":{"results":[]}}`))
	}))
	defer backend.Close()
	adapter := &adapter{
		backendURL: backend.URL, backendHTTPClient: backend.Client(), runtimeHostID: "host_proxy_tool_call", runtimeInstanceID: "instance_proxy",
		runtimeEnvironment: "test", runTicketSecret: secret,
	}
	for _, testCase := range []struct {
		name         string
		mutateHeader func(http.Header)
		mustNotEcho  string
	}{
		{name: "missing", mustNotEcho: "not-present"},
		{name: "repeated", mutateHeader: func(header http.Header) {
			header.Add(workspaceSearchProxyToolCallIDHeader, "tool-call_two")
		}, mustNotEcho: "tool-call_two"},
		{name: "case variant repeated", mutateHeader: func(header http.Header) {
			header["x-huahuo-tool-call-id"] = []string{"tool-call_case_variant"}
		}, mustNotEcho: "tool-call_case_variant"},
		{name: "surrounding whitespace", mutateHeader: func(header http.Header) {
			header.Set(workspaceSearchProxyToolCallIDHeader, " tool-call_whitespace")
		}, mustNotEcho: "tool-call_whitespace"},
		{name: "unsafe character", mutateHeader: func(header http.Header) {
			header.Set(workspaceSearchProxyToolCallIDHeader, "tool call")
		}, mustNotEcho: "tool call"},
		{name: "over length", mutateHeader: func(header http.Header) {
			header.Set(workspaceSearchProxyToolCallIDHeader, strings.Repeat("a", 257))
		}, mustNotEcho: strings.Repeat("a", 257)},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, workspaceSearchProxyPath, strings.NewReader(`{"query":"pricing"}`))
			req.RemoteAddr = "127.0.0.1:45001"
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", "RunTicket "+ticket)
			req.Header.Set("X-Run-Id", "run_proxy_tool_call")
			if testCase.name != "missing" {
				req.Header.Set(workspaceSearchProxyToolCallIDHeader, "tool-call_one")
			}
			if testCase.mutateHeader != nil {
				testCase.mutateHeader(req.Header)
			}
			rec := httptest.NewRecorder()
			adapter.workspaceSearchProxyHandler().ServeHTTP(rec, req)
			if rec.Code != http.StatusBadRequest || strings.Contains(rec.Body.String(), testCase.mustNotEcho) || strings.Contains(rec.Body.String(), ticket) {
				t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
			}
		})
	}
	if backendCalls != 0 {
		t.Fatalf("invalid tool-call requests reached backend calls=%d", backendCalls)
	}
}

func TestWorkspaceSearchProxyToolCallIDAcceptsOnlyOneSafeValue(t *testing.T) {
	for _, testCase := range []struct {
		name   string
		header http.Header
		want   string
		valid  bool
	}{
		{name: "valid", header: http.Header{workspaceSearchProxyToolCallIDHeader: []string{"tool-call.1:_-"}}, want: "tool-call.1:_-", valid: true},
		{name: "maximum length", header: http.Header{workspaceSearchProxyToolCallIDHeader: []string{strings.Repeat("a", 256)}}, want: strings.Repeat("a", 256), valid: true},
		{name: "missing", header: http.Header{}, valid: false},
		{name: "empty", header: http.Header{workspaceSearchProxyToolCallIDHeader: []string{""}}, valid: false},
		{name: "duplicate", header: http.Header{workspaceSearchProxyToolCallIDHeader: []string{"one", "two"}}, valid: false},
		{name: "case variant duplicate", header: http.Header{workspaceSearchProxyToolCallIDHeader: []string{"one"}, "x-huahuo-tool-call-id": []string{"two"}}, valid: false},
		{name: "whitespace", header: http.Header{workspaceSearchProxyToolCallIDHeader: []string{" one"}}, valid: false},
		{name: "unsafe character", header: http.Header{workspaceSearchProxyToolCallIDHeader: []string{"one/two"}}, valid: false},
		{name: "over length", header: http.Header{workspaceSearchProxyToolCallIDHeader: []string{strings.Repeat("a", 257)}}, valid: false},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			got, valid := workspaceSearchProxyToolCallID(testCase.header)
			if got != testCase.want || valid != testCase.valid {
				t.Fatalf("value=%q valid=%t wantValue=%q wantValid=%t", got, valid, testCase.want, testCase.valid)
			}
		})
	}
}

func TestWorkspaceSearchProxyAddressRequiresLoopback(t *testing.T) {
	for _, address := range []string{"0.0.0.0:18791", "192.0.2.10:18791", "localhost:18791", "127.0.0.1:not-a-port"} {
		adapter := &adapter{workspaceSearchProxyAddr: address}
		if err := adapter.validateWorkspaceSearchProxyAddress(); err == nil {
			t.Fatalf("address %q must be rejected", address)
		}
	}
	adapter := &adapter{workspaceSearchProxyAddr: "127.0.0.1:18791"}
	if err := adapter.validateWorkspaceSearchProxyAddress(); err != nil {
		t.Fatalf("loopback address error=%v", err)
	}
}

func workspaceSearchProxyTicket(t *testing.T, secret, runID, hostID string) string {
	t.Helper()
	now := time.Now().UTC().Truncate(time.Second)
	ticket, err := runtimepkg.SignRunTicket(runtimepkg.RunTicketClaims{
		RunID: runID, TenantID: "tenant_proxy", ReservationID: "reservation_proxy", RuntimeHostID: hostID,
		CapabilityHash: "cap_proxy", WorkspaceID: "workspace_proxy", WorkspaceVersion: 1, ContextGeneration: 1,
		InputManifestHash: runtimepkg.RunTicketJTIHash("manifest_proxy"), PlanHash: runtimepkg.RunTicketJTIHash("plan_proxy"),
		FencingToken: 1, JTI: "jti_proxy_" + runID, IssuedAt: now.Add(-time.Second).Unix(), ExpiresAt: now.Add(5 * time.Minute).Unix(),
	}, secret)
	if err != nil {
		t.Fatal(err)
	}
	return ticket
}
