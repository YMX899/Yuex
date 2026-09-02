package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	runtimepkg "huahuoai/backend/source/internal/runtime"
)

const (
	defaultAdapterAddr              = "127.0.0.1:18790"
	defaultGatewayURL               = "ws://127.0.0.1:18789"
	defaultWorkspaceSearchProxyAddr = "127.0.0.1:18791"
	defaultInputMaxRunes            = 32768
	maximumInputMaxRunes            = 262144
)

type gatewayInvoker func(ctx context.Context, method string, params map[string]any, timeout time.Duration) ([]byte, []byte, error)

type adapter struct {
	addr                      string
	token                     string
	gatewayURL                string
	gatewayRecoveryURL        string
	tenantID                  string
	runtimeConfigPath         string
	runtimeStateDir           string
	runtimeLogsDir            string
	runtimeTmpRoot            string
	workspaceSearchProxyAddr  string
	runtimeHostID             string
	runTicketSecret           string
	runTicketJTIStoreDir      string
	runTicketJTIStoreErr      error
	runtimePolicyKeyID        string
	defaultRuntimeConfig      string
	timeout                   time.Duration
	invoke                    gatewayInvoker
	controlInvoke             gatewayInvoker
	materializer              runtimepkg.RuntimeWorkspaceMaterializer
	backendURL                string
	backendHTTPClient         runtimepkg.HTTPClient
	hostServiceToken          string
	runtimeInstanceID         string
	runtimeEnvironment        string
	runtimeHostEndpoint       string
	runtimeZone               string
	runtimeVersion            string
	adapterVersion            string
	sessionStoreID            string
	maxActiveRuns             int
	maxProductThreadRuns      int
	maxDetachedTaskRuns       int
	admission                 *HostAdmissionController
	heartbeatSigner           runtimepkg.RuntimeHostHeartbeatSigner
	hostMTLSTrustRef          string
	hostMTLSCertRef           string
	hostMTLSKeyRef            string
	hostServerCertRef         string
	hostServerKeyRef          string
	backendTrustRef           string
	heartbeatSigningRef       string
	heartbeatKeyID            string
	hostRevocationRef         string
	gatewayRecoveryTrustRef   string
	gatewayRecoveryCertRef    string
	gatewayRecoveryKeyRef     string
	gatewayRecoveryServerName string
	recoveryBackend           hostRecoveryBackendClient
	recoveryGateway           hostRecoveryGatewayClient
	recoveryGatewayErr        error
	recoveryHostMu            sync.Mutex
	recoveryRegisteredHost    runtimepkg.RuntimeHost
}

func main() {
	server := newAdapterFromEnv()
	if strings.TrimSpace(server.token) == "" {
		fmt.Fprintln(os.Stderr, "openclaw runtime adapter token is not configured")
		os.Exit(1)
	}
	if err := server.validateLocalRunCapacity(); err != nil {
		fmt.Fprintln(os.Stderr, "openclaw runtime adapter capacity is invalid")
		os.Exit(1)
	}
	if err := server.validateRuntimePolicySigningConfiguration(); err != nil {
		fmt.Fprintln(os.Stderr, "openclaw runtime adapter policy signing configuration is invalid")
		os.Exit(1)
	}
	if err := server.validateDurableRunTicketJTIStoreConfiguration(); err != nil {
		fmt.Fprintln(os.Stderr, "openclaw runtime adapter durable run ticket store is unavailable")
		os.Exit(1)
	}
	if err := server.validateHostRegistrationConfiguration(); err != nil {
		fmt.Fprintf(os.Stderr, "openclaw runtime adapter host identity configuration is invalid: %v\n", err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	httpServer := &http.Server{
		Addr:              server.addr,
		Handler:           server,
		ReadHeaderTimeout: 5 * time.Second,
	}
	workspaceSearchProxy, workspaceSearchListener, err := server.newWorkspaceSearchProxyServer()
	if err != nil {
		fmt.Fprintln(os.Stderr, "openclaw runtime adapter workspace search proxy is invalid")
		os.Exit(1)
	}
	errC := make(chan error, 2)
	go server.runHostRegistrationLoop(ctx)
	go func() { errC <- server.serveRuntimeHostHTTP(httpServer) }()
	go func() { errC <- workspaceSearchProxy.Serve(workspaceSearchListener) }()
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Duration(envInt("HUAHUO_HTTP_SHUTDOWN_TIMEOUT_SECONDS", 15))*time.Second)
		defer cancel()
		if err := workspaceSearchProxy.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fmt.Fprintf(os.Stderr, "openclaw workspace search proxy shutdown failed: %v\n", err)
		}
		if err := httpServer.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fmt.Fprintf(os.Stderr, "openclaw runtime adapter shutdown failed: %v\n", err)
		}
	case err := <-errC:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			fmt.Fprintf(os.Stderr, "openclaw runtime adapter failed: %v\n", err)
			os.Exit(1)
		}
	}
}

func newAdapterFromEnv() *adapter {
	maxActiveRuns := envCapacityInt("HUAHUO_RUNTIME_MAX_ACTIVE_RUNS", 8)
	maxProductThreadRuns := envCapacityInt("HUAHUO_RUNTIME_MAX_PRODUCT_THREAD_RUNS", maxActiveRuns)
	maxDetachedTaskRuns := envCapacityInt("HUAHUO_RUNTIME_MAX_DETACHED_TASK_RUNS", maxActiveRuns)
	a := &adapter{
		addr:               envOrDefault("HUAHUO_OPENCLAW_ADAPTER_ADDR", defaultAdapterAddr),
		token:              gatewayTokenFromEnv(),
		gatewayURL:         envOrDefault("HUAHUO_OPENCLAW_GATEWAY_URL", defaultGatewayURL),
		gatewayRecoveryURL: strings.TrimSpace(os.Getenv("HUAHUO_RUNTIME_HOST_GATEWAY_RECOVERY_URL")),
		tenantID:           envOrDefault("HUAHUO_OPENCLAW_TENANT_ID", "huahuo-prelaunch"),
		runtimeConfigPath: firstAdapterNonEmpty(
			os.Getenv("HUAHUO_OPENCLAW_ENTERPRISE_RUNTIME_CONFIG_PATH"),
			os.Getenv("OPENCLAW_ENTERPRISE_RUNTIME_CONFIG"),
			os.Getenv("OPENCLAW_RUNTIME_CONFIG_PATH"),
		),
		runtimeStateDir:           firstAdapterNonEmpty(os.Getenv("HUAHUO_OPENCLAW_RUNTIME_STATE_DIR"), os.Getenv("OPENCLAW_STATE_DIR")),
		runtimeLogsDir:            firstAdapterNonEmpty(os.Getenv("HUAHUO_OPENCLAW_RUNTIME_LOGS_DIR"), os.Getenv("OPENCLAW_ENTERPRISE_RUNTIME_LOGS_DIR"), os.Getenv("OPENCLAW_RUNTIME_LOGS_DIR")),
		runtimeTmpRoot:            firstAdapterNonEmpty(os.Getenv("HUAHUO_OPENCLAW_RUNTIME_TMP_ROOT"), os.Getenv("OPENCLAW_ENTERPRISE_RUNTIME_TMP_ROOT"), os.Getenv("OPENCLAW_RUNTIME_TMP_ROOT")),
		workspaceSearchProxyAddr:  envOrDefault("HUAHUO_RUNTIME_WORKSPACE_SEARCH_PROXY_ADDR", defaultWorkspaceSearchProxyAddr),
		runtimeHostID:             strings.TrimSpace(os.Getenv("HUAHUO_RUNTIME_HOST_ID")),
		runTicketSecret:           os.Getenv("HUAHUO_RUNTIME_RUN_TICKET_SECRET"),
		runTicketJTIStoreDir:      strings.TrimSpace(os.Getenv("HUAHUO_RUNTIME_RUN_TICKET_JTI_STORE_DIR")),
		runtimePolicyKeyID:        strings.TrimSpace(os.Getenv("HUAHUO_RUNTIME_POLICY_KEY_ID")),
		backendURL:                strings.TrimRight(strings.TrimSpace(os.Getenv("HUAHUO_BACKEND_INTERNAL_URL")), "/"),
		hostServiceToken:          strings.TrimSpace(os.Getenv("HUAHUO_RUNTIME_HOST_SERVICE_TOKEN")),
		runtimeInstanceID:         firstAdapterNonEmpty(os.Getenv("HUAHUO_RUNTIME_INSTANCE_ID"), os.Getenv("HOSTNAME")),
		runtimeEnvironment:        firstAdapterNonEmpty(os.Getenv("HUAHUO_ENV"), "local"),
		runtimeHostEndpoint:       strings.TrimRight(strings.TrimSpace(os.Getenv("HUAHUO_RUNTIME_HOST_ENDPOINT")), "/"),
		runtimeZone:               firstAdapterNonEmpty(os.Getenv("HUAHUO_RUNTIME_ZONE"), "local"),
		runtimeVersion:            firstAdapterNonEmpty(os.Getenv("OPENCLAW_VERSION"), "2026.6.2"),
		adapterVersion:            firstAdapterNonEmpty(os.Getenv("HUAHUO_RUNTIME_ADAPTER_VERSION"), "v0.5"),
		sessionStoreID:            firstAdapterNonEmpty(os.Getenv("HUAHUO_RUNTIME_SESSION_STORE_ID"), "local"),
		hostMTLSTrustRef:          firstAdapterNonEmpty(os.Getenv("HUAHUO_RUNTIME_HOST_BACKEND_TRUST_FILE"), os.Getenv("HUAHUO_RUNTIME_HOST_MTLS_TRUST_REF")),
		hostMTLSCertRef:           firstAdapterNonEmpty(os.Getenv("HUAHUO_RUNTIME_HOST_BACKEND_CLIENT_MTLS_CERT_FILE"), os.Getenv("HUAHUO_RUNTIME_HOST_MTLS_CERT_REF")),
		hostMTLSKeyRef:            firstAdapterNonEmpty(os.Getenv("HUAHUO_RUNTIME_HOST_BACKEND_CLIENT_MTLS_KEY_FILE"), os.Getenv("HUAHUO_RUNTIME_HOST_MTLS_KEY_REF")),
		hostServerCertRef:         firstAdapterNonEmpty(os.Getenv("HUAHUO_RUNTIME_HOST_SERVER_MTLS_CERT_FILE"), os.Getenv("HUAHUO_RUNTIME_HOST_SERVER_MTLS_CERT_REF")),
		hostServerKeyRef:          firstAdapterNonEmpty(os.Getenv("HUAHUO_RUNTIME_HOST_SERVER_MTLS_KEY_FILE"), os.Getenv("HUAHUO_RUNTIME_HOST_SERVER_MTLS_KEY_REF")),
		backendTrustRef:           firstAdapterNonEmpty(os.Getenv("HUAHUO_RUNTIME_HOST_BACKEND_TRUST_FILE"), os.Getenv("HUAHUO_RUNTIME_HOST_BACKEND_TRUST_REF")),
		heartbeatSigningRef:       firstAdapterNonEmpty(os.Getenv("HUAHUO_RUNTIME_HOST_HEARTBEAT_SIGNING_KEY_FILE"), os.Getenv("HUAHUO_RUNTIME_HOST_HEARTBEAT_SIGNING_KEY_REF")),
		heartbeatKeyID:            strings.TrimSpace(os.Getenv("HUAHUO_RUNTIME_HOST_HEARTBEAT_KEY_ID")),
		hostRevocationRef:         firstAdapterNonEmpty(os.Getenv("HUAHUO_RUNTIME_HOST_REVOCATION_FILE"), os.Getenv("HUAHUO_RUNTIME_HOST_REVOCATION_REF")),
		gatewayRecoveryTrustRef:   firstAdapterNonEmpty(os.Getenv("HUAHUO_RUNTIME_HOST_GATEWAY_TRUST_FILE"), os.Getenv("HUAHUO_RUNTIME_HOST_GATEWAY_MTLS_TRUST_REF")),
		gatewayRecoveryCertRef:    firstAdapterNonEmpty(os.Getenv("HUAHUO_RUNTIME_HOST_GATEWAY_CLIENT_MTLS_CERT_FILE"), os.Getenv("HUAHUO_RUNTIME_HOST_GATEWAY_MTLS_CERT_REF")),
		gatewayRecoveryKeyRef:     firstAdapterNonEmpty(os.Getenv("HUAHUO_RUNTIME_HOST_GATEWAY_CLIENT_MTLS_KEY_FILE"), os.Getenv("HUAHUO_RUNTIME_HOST_GATEWAY_MTLS_KEY_REF")),
		gatewayRecoveryServerName: strings.TrimSpace(os.Getenv("HUAHUO_RUNTIME_HOST_GATEWAY_MTLS_SERVER_NAME")),
		maxActiveRuns:             maxActiveRuns,
		maxProductThreadRuns:      maxProductThreadRuns,
		maxDetachedTaskRuns:       maxDetachedTaskRuns,
		defaultRuntimeConfig: firstAdapterNonEmpty(
			os.Getenv("HUAHUO_OPENCLAW_RUNTIME_CONFIG_ID"),
			"huahuo-default",
		),
		timeout: time.Duration(envInt("HUAHUO_OPENCLAW_ADAPTER_TIMEOUT_SECONDS", 75)) * time.Second,
	}
	if a.runtimeTmpRoot == "" {
		a.runtimeTmpRoot = filepath.Join(os.TempDir(), "huahuo-runtime")
	}
	a.admission = NewHostAdmissionController(maxActiveRuns, maxProductThreadRuns, maxDetachedTaskRuns)
	if adapterProductionLike(a.runtimeEnvironment) {
		// Do not accept a production Run before durable occupancy and the
		// Backend reservation fence have been reconstructed. The current
		// implementation intentionally remains closed until that recovery
		// path is available rather than assuming an empty local process.
		a.admission.HoldForRecovery()
	}
	if a.runtimeInstanceID == "" {
		a.runtimeInstanceID = fmt.Sprintf("adapter-%d", os.Getpid())
	}
	if a.runtimeHostEndpoint == "" {
		a.runtimeHostEndpoint = "http://" + a.addr
	}
	var jtiStore runtimepkg.RunTicketJTIStore
	if adapterProductionLike(a.runtimeEnvironment) {
		store, err := runtimepkg.NewFileRunTicketJTIStore(a.runTicketJTIStoreDir)
		if err != nil {
			a.runTicketJTIStoreErr = err
			jtiStore = runtimepkg.NewUnavailableRunTicketJTIStore()
		} else {
			jtiStore = store
		}
	} else {
		jtiStore = runtimepkg.NewMemoryRunTicketJTIStore()
	}
	a.materializer = runtimepkg.NewRuntimeWorkspaceMaterializer(
		a.runtimeHostID,
		a.runtimeTmpRoot,
		a.runTicketSecret,
		newHostManifestSourceResolver(),
		jtiStore,
	)
	gateway := newPersistentGatewayClient(a.gatewayURL, a.token)
	controlGateway := newPersistentGatewayClient(a.gatewayURL, a.token)
	a.invoke = gateway.Invoke
	a.controlInvoke = controlGateway.Invoke
	if a.heartbeatSigningRef != "" && a.heartbeatKeyID != "" {
		if signer, err := runtimepkg.LoadEd25519RuntimeHostHeartbeatSigner(a.heartbeatKeyID, a.heartbeatSigningRef); err == nil {
			a.heartbeatSigner = signer
		}
	}
	if a.hostMTLSTrustRef != "" && a.hostMTLSCertRef != "" && a.hostMTLSKeyRef != "" {
		if client, err := runtimepkg.NewRuntimeHostMTLSHTTPClient(runtimepkg.RuntimeHostMTLSConfig{
			TrustRef: a.hostMTLSTrustRef, CertificateRef: a.hostMTLSCertRef, PrivateKeyRef: a.hostMTLSKeyRef,
			ServerName: strings.TrimSpace(os.Getenv("HUAHUO_RUNTIME_HOST_MTLS_SERVER_NAME")),
		}); err == nil {
			a.backendHTTPClient = client
		}
	}
	a.recoveryBackend = &adapterBackendRecoveryClient{adapter: a}
	if gatewayRecoverySettingsConfigured(a) {
		client, err := newMTLSGatewayRecoverySnapshotClient(a.gatewayRecoveryURL, runtimepkg.RuntimeHostMTLSConfig{
			TrustRef: a.gatewayRecoveryTrustRef, CertificateRef: a.gatewayRecoveryCertRef,
			PrivateKeyRef: a.gatewayRecoveryKeyRef, ServerName: a.gatewayRecoveryServerName,
		})
		if err != nil {
			a.recoveryGatewayErr = err
		} else {
			a.recoveryGateway = client
		}
	} else if gatewayRecoverySettingsPartiallyConfigured(a) {
		a.recoveryGatewayErr = fmt.Errorf("RUNTIME_HOST_IDENTITY_UNAVAILABLE")
	}
	return a
}

func (a *adapter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	method, allowedMethod, routeParams := runtimeMethodFromRequest(r)
	if method == "" {
		writeJSON(w, http.StatusNotFound, map[string]any{"errorCode": "NOT_FOUND"})
		return
	}
	if r.Method == http.MethodOptions {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if r.Method != allowedMethod {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"errorCode": "METHOD_NOT_ALLOWED"})
		return
	}
	a.handleRuntimeMethod(w, r, method, routeParams)
}

func (a *adapter) validateHostRegistrationConfiguration() error {
	if a == nil || !adapterProductionLike(a.runtimeEnvironment) {
		return nil
	}
	if strings.TrimSpace(a.backendURL) == "" || strings.TrimSpace(a.runtimeHostID) == "" || strings.TrimSpace(a.runtimeInstanceID) == "" ||
		strings.TrimSpace(a.runtimeHostEndpoint) == "" || strings.TrimSpace(a.hostMTLSTrustRef) == "" ||
		strings.TrimSpace(a.hostMTLSCertRef) == "" || strings.TrimSpace(a.hostMTLSKeyRef) == "" ||
		strings.TrimSpace(a.hostServerCertRef) == "" || strings.TrimSpace(a.hostServerKeyRef) == "" || strings.TrimSpace(a.backendTrustRef) == "" ||
		strings.TrimSpace(a.heartbeatSigningRef) == "" || strings.TrimSpace(a.heartbeatKeyID) == "" ||
		strings.TrimSpace(a.hostRevocationRef) == "" || a.heartbeatSigner == nil || !runtimepkg.RuntimeHostMTLSClientConfigured(a.backendHTTPClient) {
		return fmt.Errorf("RUNTIME_HOST_IDENTITY_CONFIGURATION_INCOMPLETE")
	}
	if err := a.validateRuntimePolicySigningConfiguration(); err != nil {
		return err
	}
	if !adapterInternalHTTPSURL(a.backendURL) {
		return fmt.Errorf("RUNTIME_HOST_IDENTITY_BACKEND_ENDPOINT_INVALID")
	}
	if !adapterInternalHTTPSURL(a.runtimeHostEndpoint) {
		return fmt.Errorf("RUNTIME_HOST_IDENTITY_HOST_ENDPOINT_INVALID")
	}
	return nil
}

func (a *adapter) validateDurableRunTicketJTIStoreConfiguration() error {
	if a == nil || !adapterProductionLike(a.runtimeEnvironment) {
		return nil
	}
	if a.runTicketJTIStoreErr != nil || a.materializer.JTIs == nil {
		return fmt.Errorf("RUNTIME_STORAGE_UNAVAILABLE")
	}
	health, ok := a.materializer.JTIs.(runtimepkg.RunTicketJTIStoreHealth)
	if !ok || health.Health(context.Background()) != nil {
		return fmt.Errorf("RUNTIME_STORAGE_UNAVAILABLE")
	}
	durable, ok := a.materializer.JTIs.(runtimepkg.RunTicketJTIStoreDurability)
	if !ok || !durable.Durable() {
		return fmt.Errorf("RUNTIME_STORAGE_UNAVAILABLE")
	}
	probe, ok := a.materializer.JTIs.(runtimepkg.RunTicketJTIStoreProbe)
	if !ok || probe.Probe(context.Background()) != nil {
		return fmt.Errorf("RUNTIME_STORAGE_UNAVAILABLE")
	}
	return nil
}

func (a *adapter) serveRuntimeHostHTTP(httpServer *http.Server) error {
	if !adapterProductionLike(a.runtimeEnvironment) {
		return httpServer.ListenAndServe()
	}
	tlsConfig, err := runtimepkg.LoadRuntimeHostMTLSServerConfig(runtimepkg.RuntimeHostMTLSConfig{
		TrustRef: a.backendTrustRef, CertificateRef: a.hostServerCertRef, PrivateKeyRef: a.hostServerKeyRef,
	})
	if err != nil {
		return fmt.Errorf("RUNTIME_HOST_IDENTITY_UNAVAILABLE")
	}
	listener, err := tls.Listen("tcp", httpServer.Addr, tlsConfig)
	if err != nil {
		return err
	}
	return httpServer.Serve(listener)
}

func adapterProductionLike(environment string) bool {
	switch strings.ToLower(strings.TrimSpace(environment)) {
	case "prelaunch", "prod", "production":
		return true
	default:
		return false
	}
}

func adapterInternalHTTPSURL(value string) bool {
	parsed, err := url.Parse(strings.TrimSpace(value))
	if err != nil || !strings.EqualFold(parsed.Scheme, "https") || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Path != "" && parsed.Path != "/") {
		return false
	}
	host := strings.TrimSuffix(strings.ToLower(parsed.Hostname()), ".")
	if host == "" || host == "localhost" {
		return false
	}
	if ip := net.ParseIP(host); ip != nil && (ip.IsLoopback() || ip.IsUnspecified()) {
		return false
	}
	return true
}

func (a *adapter) handleRuntimeMethod(w http.ResponseWriter, r *http.Request, method string, routeParams map[string]any) {
	defer r.Body.Close()
	if method == "enterprise.runtime.run" && adapterProductionLike(a.runtimeEnvironment) {
		writeJSON(w, http.StatusGone, map[string]any{"errorCode": "RUNTIME_LEGACY_CONTRACT_DISABLED"})
		return
	}
	raw, err := io.ReadAll(io.LimitReader(r.Body, 4<<20))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"errorCode": "RUNTIME_INPUT_INVALID"})
		return
	}
	params := map[string]any{}
	if len(bytes.TrimSpace(raw)) > 0 {
		if err := json.Unmarshal(raw, &params); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"errorCode": "RUNTIME_INPUT_INVALID"})
			return
		}
	}
	if method == "enterprise.runtime.abort" {
		pathRunID := stringValue(routeParams["runId"])
		bodyRunID := stringValue(params["runId"])
		if pathRunID != "" && bodyRunID != "" && bodyRunID != pathRunID {
			writeJSON(w, http.StatusForbidden, map[string]any{"errorCode": "RUNTIME_PERMISSION_DENIED"})
			return
		}
	}
	for key, value := range routeParams {
		params[key] = value
	}
	delete(params, "method")
	permit := runtimeHostPermit{}
	if method == "enterprise.runtime.submit" {
		params, permit, err = a.prepareAsyncSubmit(r.Context(), r.Header.Get("Authorization"), params)
		if err != nil {
			writeJSON(w, adapterRuntimeHTTPStatus(adapterRuntimeErrorCode(err, nil)), map[string]any{"errorCode": adapterRuntimeErrorCode(err, nil)})
			return
		}
	} else if method == "enterprise.runtime.abort" {
		params, err = a.prepareAsyncAbort(r.Header.Get("Authorization"), params)
		if err != nil {
			code := adapterRuntimeErrorCode(err, nil)
			writeJSON(w, adapterRuntimeHTTPStatus(code), map[string]any{"errorCode": code})
			return
		}
	} else if strings.HasPrefix(method, "enterprise.runtime.") && method != "enterprise.runtime.run" && method != "enterprise.runtime.capabilities" {
		if err := a.authorizeAsyncRun(r.Header.Get("Authorization"), stringValue(params["runId"])); err != nil {
			writeJSON(w, http.StatusForbidden, map[string]any{"errorCode": "RUNTIME_PERMISSION_DENIED"})
			return
		}
	}
	normalizedParams, err := a.normalizeRuntimeParams(method, params)
	if err != nil {
		if permit.Acquired {
			a.releaseRunPermit(permit.RunID)
		}
		writeJSON(w, http.StatusBadRequest, map[string]any{"errorCode": "RUNTIME_INPUT_INVALID"})
		return
	}
	// The async submit policy is part of the signed Runtime contract. Do not
	// project it away for an older Gateway: an unsupported Gateway must reject
	// the submission rather than execute with an unverifiable tool policy.
	gatewayParams := normalizedParams
	timeout := a.timeout
	if timeoutSec := intValue(params["timeoutSec"]); timeoutSec > 0 {
		timeout = time.Duration(timeoutSec+15) * time.Second
	} else if timeoutSec := envIntFromString(r.Header.Get("X-Huahuo-Runtime-Timeout-Sec"), 0); timeoutSec > 0 {
		timeout = time.Duration(timeoutSec+15) * time.Second
	}
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()
	stdout, stderr, err := a.invoke(ctx, method, gatewayParams, timeout)
	if err != nil {
		if permit.Acquired {
			a.releaseRunPermit(permit.RunID)
		}
		code := adapterRuntimeErrorCode(err, stderr)
		status := adapterRuntimeHTTPStatus(code)
		message := safeStderr(stderr)
		writeJSON(w, status, map[string]any{"errorCode": code, "providerMessage": message})
		return
	}
	if !json.Valid(bytes.TrimSpace(stdout)) {
		if permit.Acquired {
			a.releaseRunPermit(permit.RunID)
		}
		writeJSON(w, http.StatusBadGateway, map[string]any{"errorCode": "AI_RESULT_PARSE_FAILED"})
		return
	}
	if method == "enterprise.runtime.capabilities" {
		stdout, err = projectAdapterRuntimeCapabilities(stdout)
		if err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]any{"errorCode": "RUNTIME_TOOL_UNAVAILABLE"})
			return
		}
	}
	stdout = normalizeFayaGatewayFinalAnswer(stdout)
	stdout = normalizeHuokeTopicGatewayFinalAnswer(stdout)
	a.observeRuntimeResponse(method, gatewayParams, stdout)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(bytes.TrimSpace(stdout))
}

// projectAdapterRuntimeCapabilities adds only the assertion owned by this
// Adapter binary. Gateway capabilities remain otherwise unchanged. An older
// Adapter does not execute this code and therefore cannot be mistaken for a
// verifier of the Backend's v2 model-start ticket binding.
func projectAdapterRuntimeCapabilities(raw []byte) ([]byte, error) {
	var document map[string]any
	if err := json.Unmarshal(raw, &document); err != nil || document == nil {
		return nil, fmt.Errorf("runtime capabilities malformed")
	}
	document["submitBinding"] = map[string]any{
		"version":            runtimepkg.RuntimeSubmitBindingV2,
		"productSessionHash": true,
	}
	return json.Marshal(document)
}

func (a *adapter) normalizeRuntimeParams(method string, params map[string]any) (map[string]any, error) {
	if method != "enterprise.runtime.run" {
		out := map[string]any{}
		for key, value := range params {
			if key != "method" {
				out[key] = value
			}
		}
		return out, nil
	}
	if isFullRuntimeSpec(params) {
		return a.normalizeFullRuntimeSpec(params)
	}
	runID := stringValue(params["runId"])
	inputMessage := sanitizeAdapterInputMessage(firstAdapterNonEmpty(stringValue(params["inputMessage"]), stringValueFromMap(params, "input", "message")))
	workspaceID := stringValue(params["workspaceId"])
	threadID := stringValue(params["threadId"])
	productSession := mapValue(params["productSession"])
	productSessionThreadID := stringValue(productSession["threadId"])
	productSessionKey := stringValue(productSession["openclawSessionKey"])
	userID := stringValue(params["userId"])
	workspaceRealPath := firstAdapterNonEmpty(stringValue(params["workspaceDir"]), stringValueFromMap(params, "workspace", "realPath"), workspacePath(userID, workspaceID))
	runtimeConfigID := firstAdapterNonEmpty(stringValue(params["runtimeConfigId"]), stringValue(params["authPoolId"]), a.defaultRuntimeConfig)
	if runID == "" || inputMessage == "" || userID == "" || workspaceID == "" || threadID == "" || productSessionThreadID == "" || productSessionKey == "" || workspaceRealPath == "" || runtimeConfigID == "" {
		return nil, fmt.Errorf("runtime spec missing required fields")
	}
	out := map[string]any{
		"runId":                runID,
		"tenantId":             firstAdapterNonEmpty(stringValue(params["tenantId"]), a.tenantID, "huahuo-prelaunch"),
		"userId":               userID,
		"workspaceId":          workspaceID,
		"threadId":             threadID,
		"runtimeConfigId":      runtimeConfigID,
		"runtimeConfigVersion": firstAdapterNonEmpty(stringValue(params["runtimeConfigVersion"]), "v1"),
		"workspace": map[string]any{
			"realPath":   workspaceRealPath,
			"accessMode": runtimepkg.RuntimeWorkspaceAccessRead,
		},
		"productSession": map[string]any{
			"threadId":           productSessionThreadID,
			"openclawSessionKey": productSessionKey,
		},
		"input": map[string]any{
			"message": inputMessage,
		},
	}
	if requestedAccessMode := firstAdapterNonEmpty(stringValueFromMap(params, "workspace", "accessMode"), stringValue(params["accessMode"])); requestedAccessMode != "" && requestedAccessMode != runtimepkg.RuntimeWorkspaceAccessRead {
		return nil, fmt.Errorf("unsigned legacy runtime write is forbidden")
	}
	if runtimeBody := a.runtimeBody(mapValue(params["runtime"])); len(runtimeBody) > 0 {
		out["runtime"] = runtimeBody
	}
	return validateAdapterRuntimeSpec(out)
}

func (a *adapter) normalizeFullRuntimeSpec(params map[string]any) (map[string]any, error) {
	out := map[string]any{}
	for _, key := range []string{"runId", "tenantId", "userId", "workspaceId", "threadId", "runtimeConfigId", "runtimeConfigVersion", "workspace", "productSession", "modelOverride", "plugins", "input"} {
		if value, ok := params[key]; ok {
			out[key] = value
		}
	}
	input := mapValue(out["input"])
	if len(input) > 0 {
		input["message"] = sanitizeAdapterInputMessage(stringValue(input["message"]))
		out["input"] = input
	}
	workspace := mapValue(out["workspace"])
	if len(workspace) > 0 {
		if requestedAccessMode := stringValue(workspace["accessMode"]); requestedAccessMode != "" && requestedAccessMode != runtimepkg.RuntimeWorkspaceAccessRead {
			return nil, fmt.Errorf("unsigned legacy runtime write is forbidden")
		}
		workspace["accessMode"] = runtimepkg.RuntimeWorkspaceAccessRead
		delete(workspace, "writeLease")
		out["workspace"] = workspace
	}
	runtimeBody := a.runtimeBody(mapValue(params["runtime"]))
	if len(runtimeBody) > 0 {
		out["runtime"] = runtimeBody
	}
	return validateAdapterRuntimeSpec(out)
}

func validateAdapterRuntimeSpec(params map[string]any) (map[string]any, error) {
	workspace := mapValue(params["workspace"])
	productSession := mapValue(params["productSession"])
	input := mapValue(params["input"])
	if stringValue(params["runId"]) == "" ||
		stringValue(params["tenantId"]) == "" ||
		stringValue(params["userId"]) == "" ||
		stringValue(params["workspaceId"]) == "" ||
		stringValue(params["threadId"]) == "" ||
		stringValue(params["runtimeConfigId"]) == "" ||
		stringValue(workspace["realPath"]) == "" ||
		stringValue(workspace["accessMode"]) != runtimepkg.RuntimeWorkspaceAccessRead ||
		stringValue(productSession["threadId"]) == "" ||
		stringValue(productSession["openclawSessionKey"]) == "" ||
		stringValue(input["message"]) == "" {
		return nil, fmt.Errorf("runtime spec missing required fields")
	}
	return params, nil
}

func (a *adapter) runtimeBody(existing map[string]any) map[string]any {
	out := map[string]any{}
	for _, key := range []string{"stateDir", "configPath", "logsDir", "tmpRoot"} {
		if value := stringValue(existing[key]); value != "" {
			out[key] = value
		}
	}
	if out["stateDir"] == nil && a.runtimeStateDir != "" {
		out["stateDir"] = a.runtimeStateDir
	}
	if out["configPath"] == nil && a.runtimeConfigPath != "" {
		out["configPath"] = a.runtimeConfigPath
	}
	if out["logsDir"] == nil && a.runtimeLogsDir != "" {
		out["logsDir"] = a.runtimeLogsDir
	}
	if out["tmpRoot"] == nil && a.runtimeTmpRoot != "" {
		out["tmpRoot"] = a.runtimeTmpRoot
	}
	return out
}

func isFullRuntimeSpec(params map[string]any) bool {
	return stringValue(params["tenantId"]) != "" &&
		stringValue(params["userId"]) != "" &&
		stringValue(params["workspaceId"]) != "" &&
		stringValue(params["threadId"]) != "" &&
		stringValue(params["runtimeConfigId"]) != "" &&
		len(mapValue(params["workspace"])) > 0 &&
		len(mapValue(params["productSession"])) > 0 &&
		len(mapValue(params["input"])) > 0
}

func runtimeMethodFromRequest(r *http.Request) (string, string, map[string]any) {
	path := strings.Trim(r.URL.Path, "/")
	switch path {
	case "enterprise.runtime.run":
		return "enterprise.runtime.run", http.MethodPost, nil
	case "enterprise.runtime.abort":
		return "enterprise.runtime.abort", http.MethodPost, nil
	case "enterprise.runtime/runs":
		return "enterprise.runtime.submit", http.MethodPost, nil
	case "enterprise.runtime/capabilities":
		return "enterprise.runtime.capabilities", http.MethodGet, nil
	}
	prefix := "enterprise.runtime/runs/"
	if !strings.HasPrefix(path, prefix) {
		return "", "", nil
	}
	remainder := strings.TrimPrefix(path, prefix)
	parts := strings.Split(remainder, "/")
	if len(parts) == 1 && parts[0] != "" {
		return "enterprise.runtime.status", http.MethodGet, map[string]any{"runId": parts[0]}
	}
	if len(parts) == 2 && parts[0] != "" && parts[1] == "events" {
		after, _ := strconv.ParseInt(r.URL.Query().Get("afterSequence"), 10, 64)
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		waitMs, _ := strconv.Atoi(r.URL.Query().Get("waitMs"))
		if limit == 0 {
			limit = 100
		}
		return "enterprise.runtime.events", http.MethodGet, map[string]any{"runId": parts[0], "afterSequence": after, "limit": limit, "waitMs": waitMs}
	}
	if len(parts) == 2 && parts[0] != "" && parts[1] == "abort" {
		return "enterprise.runtime.abort", http.MethodPost, map[string]any{"runId": parts[0]}
	}
	return "", "", nil
}

func writeJSON(w http.ResponseWriter, status int, body map[string]any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func safeStderr(raw []byte) string {
	text := strings.TrimSpace(string(raw))
	if text == "" {
		return "runtime gateway call failed"
	}
	text = redactAdapterDiagnosticText(text)
	if text == "" {
		return "runtime gateway call failed"
	}
	if len(text) > 512 {
		text = text[:512]
	}
	return text
}

func adapterRuntimeErrorCode(err error, stderr []byte) string {
	text := strings.ToLower(strings.TrimSpace(strings.Join([]string{string(stderr), errorString(err)}, " ")))
	for _, code := range []string{
		"PROVIDER_CONFIG_MISSING",
		"PROVIDER_AUTH_FAILED",
		"MODEL_INPUT_UNSUPPORTED",
		"RUNTIME_TIMEOUT",
		"RUNTIME_TOOL_LOOP_DETECTED",
		"RUNTIME_TOOL_BUDGET_EXCEEDED",
		"RUNTIME_TOOL_BUDGET_UNSUPPORTED",
		"RUNTIME_RUN_STALLED",
		"RUNTIME_RUN_NOT_FOUND",
		"WORKSPACE_FORBIDDEN",
		"ATTACHMENT_INVALID",
		"AI_RESULT_PARSE_FAILED",
		"RUNTIME_INPUT_INVALID",
		"RUNTIME_PERMISSION_DENIED",
		"RUNTIME_HOST_UNAUTHORIZED",
		"RUNTIME_WORKSPACE_MATERIALIZATION_FAILED",
		"RUNTIME_CAPACITY_UNAVAILABLE",
		"RUNTIME_STORAGE_UNAVAILABLE",
		"RUNTIME_ABORT_FAILED",
	} {
		if strings.Contains(strings.ToUpper(text), code) {
			return code
		}
	}
	switch {
	case (strings.Contains(text, "runtimepolicy") || strings.Contains(text, "runtime policy")) &&
		(strings.Contains(text, "unsupported") || strings.Contains(text, "unknown") || strings.Contains(text, "not allowed") ||
			strings.Contains(text, "additional propert") || strings.Contains(text, "schema")):
		// An older Gateway schema cannot safely execute an async Run after
		// rejecting the signed policy. Surface a retryable capability failure;
		// never retry with the policy removed.
		return "RUNTIME_TOOL_BUDGET_UNSUPPORTED"
	case strings.Contains(text, "no session found") || strings.Contains(text, "sessions.resolve"):
		return "PROVIDER_CONFIG_MISSING"
	case strings.Contains(text, "invalid enterprise.runtime") ||
		strings.Contains(text, "must have required property") ||
		strings.Contains(text, "runtime spec missing required fields"):
		return "RUNTIME_INPUT_INVALID"
	case strings.Contains(text, "workspace") && (strings.Contains(text, "forbidden") || strings.Contains(text, "denied") || strings.Contains(text, "permission")):
		return "WORKSPACE_FORBIDDEN"
	case strings.Contains(text, "attachment"):
		return "ATTACHMENT_INVALID"
	case strings.Contains(text, "tool") && (strings.Contains(text, "budget") || strings.Contains(text, "limit") || strings.Contains(text, "exhausted")):
		return "RUNTIME_TOOL_BUDGET_EXCEEDED"
	case strings.Contains(text, "tool loop") || strings.Contains(text, "loop detected"):
		return "RUNTIME_TOOL_LOOP_DETECTED"
	case strings.Contains(text, "stalled") || strings.Contains(text, "no progress"):
		return "RUNTIME_RUN_STALLED"
	case strings.Contains(text, "timeout") || strings.Contains(text, "timed out") || strings.Contains(text, "deadline"):
		return "RUNTIME_TIMEOUT"
	case strings.Contains(text, "model input") ||
		strings.Contains(text, "input unsupported") ||
		strings.Contains(text, "unsupported input") ||
		strings.Contains(text, "context length") ||
		strings.Contains(text, "token limit"):
		return "MODEL_INPUT_UNSUPPORTED"
	case strings.Contains(text, "provider config") ||
		strings.Contains(text, "config missing") ||
		strings.Contains(text, "not configured") ||
		strings.Contains(text, "runtime config") && strings.Contains(text, "not found") ||
		strings.Contains(text, "configpath") && strings.Contains(text, "missing"):
		return "PROVIDER_CONFIG_MISSING"
	case strings.Contains(text, "unauthorized") ||
		strings.Contains(text, "authentication") ||
		strings.Contains(text, "auth failed") ||
		strings.Contains(text, "invalid token") ||
		strings.Contains(text, "gateway token"):
		return "PROVIDER_AUTH_FAILED"
	case strings.Contains(text, "parse") ||
		strings.Contains(text, "invalid json") ||
		strings.Contains(text, "unexpected token"):
		return "AI_RESULT_PARSE_FAILED"
	default:
		return "RUNTIME_FAILED"
	}
}

func adapterRuntimeHTTPStatus(code string) int {
	switch code {
	case "RUNTIME_INPUT_INVALID", "MODEL_INPUT_UNSUPPORTED", "ATTACHMENT_INVALID":
		return http.StatusBadRequest
	case "RUNTIME_RUN_NOT_FOUND":
		return http.StatusNotFound
	case "RUNTIME_PERMISSION_DENIED", "WORKSPACE_FORBIDDEN", "RUNTIME_HOST_UNAUTHORIZED":
		return http.StatusForbidden
	case "RUNTIME_TOOL_LOOP_DETECTED":
		return http.StatusConflict
	case "RUNTIME_CAPACITY_UNAVAILABLE", "RUNTIME_STORAGE_UNAVAILABLE", "RUNTIME_TOOL_BUDGET_UNSUPPORTED":
		return http.StatusServiceUnavailable
	default:
		return http.StatusBadGateway
	}
}

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func redactAdapterDiagnosticText(text string) string {
	lower := strings.ToLower(text)
	if strings.Contains(lower, "authorization:") ||
		strings.Contains(lower, "bearer ") ||
		strings.Contains(lower, "runticket") ||
		strings.Contains(lower, "sk-") ||
		strings.Contains(lower, "access_token") ||
		strings.Contains(lower, "refresh_token") ||
		strings.Contains(lower, "secret=") ||
		strings.Contains(lower, "token=") {
		return "runtime gateway call failed"
	}
	parts := strings.Fields(strings.ReplaceAll(text, "\\", "/"))
	for i, part := range parts {
		trimmed := strings.Trim(part, " \t\r\n\"'`.,;:()[]{}<>")
		lowerPart := strings.ToLower(trimmed)
		switch {
		case strings.Contains(lowerPart, "/home/data/") ||
			strings.Contains(lowerPart, "/home/huahuo-runtime/") ||
			strings.Contains(lowerPart, "/tmp/runtime-workspaces/") ||
			strings.Contains(lowerPart, "/openclaw/"):
			parts[i] = "[runtime-path-redacted]"
		case strings.Contains(lowerPart, "signature=") ||
			strings.Contains(lowerPart, "x-oss-signature") ||
			strings.Contains(lowerPart, "x-amz-signature") ||
			strings.Contains(lowerPart, "signedurl"):
			parts[i] = "[signed-url-redacted]"
		case strings.Contains(lowerPart, "skill.md") ||
			strings.Contains(lowerPart, "/skills/") ||
			strings.Contains(lowerPart, "/runtime-skills/"):
			parts[i] = "[runtime-skill-redacted]"
		}
	}
	return strings.TrimSpace(strings.Join(parts, " "))
}

func sanitizeAdapterInputMessage(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	value = redactAdapterDiagnosticText(value)
	value = strings.TrimSpace(strings.Join(strings.Fields(value), " "))
	if value == "" {
		return ""
	}
	if adapterLooksLikeBackendPrompt(value) {
		return "Use the configured runtime instructions and available runtime context."
	}
	runes := []rune(value)
	limit := adapterInputMaxRunes()
	if len(runes) > limit {
		return strings.TrimSpace(string(runes[:limit])) + " [truncated]"
	}
	return value
}

func adapterInputMaxRunes() int {
	limit := envInt("HUAHUO_OPENCLAW_ADAPTER_INPUT_MAX_RUNES", defaultInputMaxRunes)
	if limit < 800 {
		return 800
	}
	if limit > maximumInputMaxRunes {
		return maximumInputMaxRunes
	}
	return limit
}

func adapterLooksLikeBackendPrompt(value string) bool {
	lower := strings.ToLower(value)
	for _, marker := range []string{
		"## loaded effective skill",
		"skill.md",
		"do not read /home/agent-runtime/openclaw-data/plugin-skills",
		"expected_topic_skill_contract",
		"expected_profile_skill_contract",
		"expected_minutes_skill_contract",
	} {
		if strings.Contains(lower, marker) {
			return true
		}
	}
	matches := 0
	for _, marker := range []string{
		"task:",
		"agent profile:",
		"required capability:",
		"prompt template:",
		"business parameters:",
		"context summary and relative references:",
		"output: return exactly one json object",
	} {
		if strings.Contains(lower, marker) {
			matches++
		}
	}
	return matches >= 2
}

func envOrDefault(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func gatewayTokenFromEnv() string {
	if value := strings.TrimSpace(os.Getenv("OPENCLAW_GATEWAY_TOKEN")); value != "" {
		return value
	}
	return strings.TrimSpace(os.Getenv("HUAHUO_OPENCLAW_GATEWAY_TOKEN"))
}

func envInt(key string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return envIntFromString(value, fallback)
}

func envCapacityInt(key string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0
	}
	return parsed
}

func envIntFromString(value string, fallback int) int {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func intValue(value any) int {
	switch typed := value.(type) {
	case int:
		return typed
	case float64:
		return int(typed)
	case json.Number:
		parsed, _ := typed.Int64()
		return int(parsed)
	default:
		return 0
	}
}

func firstAdapterNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func stringValue(value any) string {
	if value == nil {
		return ""
	}
	if text, ok := value.(string); ok {
		return strings.TrimSpace(text)
	}
	return strings.TrimSpace(fmt.Sprint(value))
}

func stringValueFromMap(params map[string]any, key, nestedKey string) string {
	return stringValue(mapValue(params[key])[nestedKey])
}

func mapValue(value any) map[string]any {
	if typed, ok := value.(map[string]any); ok {
		return typed
	}
	return map[string]any{}
}

func arrayValue(value any) []any {
	if typed, ok := value.([]any); ok {
		return typed
	}
	return nil
}

func workspacePath(userID, workspaceID string) string {
	userID = safeSegment(userID)
	workspaceID = safeSegment(workspaceID)
	if userID == "" || workspaceID == "" {
		return ""
	}
	root := firstAdapterNonEmpty(os.Getenv("HUAHUO_DATA_WORKSPACES_ROOT"), os.Getenv("DATA_WORKSPACES_ROOT"))
	if root == "" {
		if dataRoot := firstAdapterNonEmpty(os.Getenv("HUAHUO_DATA_ROOT"), os.Getenv("DATA_ROOT")); dataRoot != "" {
			root = filepath.Join(dataRoot, "workspaces")
		}
	}
	if root == "" {
		root = "/home/data/huahuo/workspaces"
	}
	return filepath.Join(root, "tenants", adapterWorkspaceTenantID(), "users", userID, "workspaces", workspaceID)
}

func adapterWorkspaceTenantID() string {
	return firstAdapterNonEmpty(os.Getenv("HUAHUO_WORKSPACE_TENANT_ID"), os.Getenv("WORKSPACE_TENANT_ID"), "tenant_default")
}

func safeSegment(value string) string {
	var builder strings.Builder
	for _, ch := range strings.TrimSpace(value) {
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
