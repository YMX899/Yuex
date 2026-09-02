//go:build linux

package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	cryptorand "crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	goruntime "runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	runtimepkg "huahuoai/backend/source/internal/runtime"

	"github.com/gorilla/websocket"
)

// TestAdapterCandidateBinaryLoopbackValidation is deliberately opt-in because
// it compiles and starts a second Adapter process. The child receives a
// complete, fixture-only environment: it cannot inherit a live Backend,
// Gateway, database, JTI store, Runtime root, or credential from the caller.
//
// It is a candidate-binary validation, not a release action. In particular it
// never uses systemd, an active Adapter port, a host-owned Runtime root, or a
// production Host identity. The command is documented in the paired Ops
// runbook and is suitable for a production host only because all its I/O is
// constrained to loopback and a t.TempDir().
func TestAdapterCandidateBinaryLoopbackValidation(t *testing.T) {
	if os.Getenv("HUAHUO_RUNTIME_ADAPTER_CANDIDATE_VALIDATION") != "1" {
		t.Skip("set HUAHUO_RUNTIME_ADAPTER_CANDIDATE_VALIDATION=1 to start an isolated candidate Adapter")
	}

	tmp := candidateValidationNewTemporaryRoot(t)
	privateKey, publicKey := candidateValidationKeyPair(t)
	keyPath := filepath.Join(tmp, "heartbeat-private-key.pem")
	privateDER, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privateDER}), 0o600); err != nil {
		t.Fatal(err)
	}

	const (
		hostID       = "candidate-adapter-host"
		instanceID   = "candidate-adapter-instance"
		environment  = "candidate_validation"
		keyID        = "candidate-validation-key"
		gatewayToken = "candidate-validation-gateway-token"
	)
	capabilities := readyRegistrationCapabilities()
	capabilities.RuntimeVersion = "candidate-runtime"
	capabilities.AdapterVersion = "candidate-adapter"
	capabilities.CapabilityHash = "candidate-capability-v2"

	verifier, err := runtimepkg.NewEd25519RuntimeHostHeartbeatVerifier(
		[]runtimepkg.RuntimeHostVerificationKey{{KeyID: keyID, PublicKey: publicKey}},
		nil,
		30*time.Second,
		2*time.Minute,
		runtimepkg.NewMemoryRuntimeHostNonceStore(),
	)
	if err != nil {
		t.Fatal(err)
	}
	probe := newCandidateAdapterProbe(t, verifier, hostID, instanceID, environment, capabilities.CapabilityHash)
	backend := httptest.NewServer(probe.backendHandler())
	defer backend.Close()
	if err := candidateValidationLoopbackURL(backend.URL, "http"); err != nil {
		t.Fatalf("fixture Backend is not loopback-only: %v", err)
	}

	gateway := httptest.NewServer(candidateGatewayHandler(t, gatewayToken, capabilities))
	defer gateway.Close()
	gatewayURL := "ws" + strings.TrimPrefix(gateway.URL, "http")
	if err := candidateValidationLoopbackURL(gatewayURL, "ws"); err != nil {
		t.Fatalf("fixture Gateway is not loopback-only: %v", err)
	}

	adapterAddr := candidateValidationUnusedLoopbackAddress(t)
	proxyAddr := candidateValidationUnusedLoopbackAddress(t)
	endpoint := "http://" + adapterAddr
	if err := candidateValidationLoopbackURL(endpoint, "http"); err != nil {
		t.Fatalf("candidate endpoint is not loopback-only: %v", err)
	}

	candidate := filepath.Join(tmp, "openclaw-runtime-adapter-candidate")
	candidateSHA256 := candidateValidationPrepareCandidate(t, candidate)
	t.Logf("isolated candidate sha256=%s", candidateSHA256)

	stateRoot := filepath.Join(tmp, "state")
	tmpRoot := filepath.Join(tmp, "runtime-tmp")
	logsRoot := filepath.Join(tmp, "logs")
	for _, root := range []string{stateRoot, tmpRoot, logsRoot} {
		if err := os.MkdirAll(root, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	configPath := filepath.Join(tmp, "openclaw-enterprise-runtime.json")
	if err := os.WriteFile(configPath, []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	var childOutput bytes.Buffer
	command := exec.Command(candidate)
	command.Dir = tmp
	command.Stdout = &childOutput
	command.Stderr = &childOutput
	command.Env = candidateValidationEnvironment(tmp, adapterAddr, proxyAddr, gatewayURL, backend.URL, endpoint, keyPath, hostID, instanceID, environment, keyID, gatewayToken, stateRoot, tmpRoot, logsRoot, configPath)
	if err := command.Start(); err != nil {
		t.Fatalf("start isolated Adapter candidate: %v", err)
	}
	exit := make(chan error, 1)
	go func() { exit <- command.Wait() }()
	stopped := false
	t.Cleanup(func() {
		if !stopped {
			_ = command.Process.Signal(syscall.SIGTERM)
			select {
			case <-exit:
			case <-time.After(5 * time.Second):
				_ = command.Process.Kill()
				<-exit
			}
		}
	})

	capabilityResponse := candidateValidationWaitForCapabilities(t, endpoint, exit, &childOutput)
	var advertised runtimepkg.RuntimeCapabilities
	if err := json.Unmarshal(capabilityResponse, &advertised); err != nil {
		t.Fatalf("decode candidate capability response: %v: %s", err, candidateValidationSafeOutput(capabilityResponse))
	}
	if advertised.CapabilityHash != capabilities.CapabilityHash {
		t.Fatalf("candidate capability hash=%q want %q", advertised.CapabilityHash, capabilities.CapabilityHash)
	}
	if advertised.SubmitBinding.Version != runtimepkg.RuntimeSubmitBindingV2 || !advertised.SubmitBinding.ProductSessionHash {
		t.Fatalf("candidate submit binding=%+v want exact %s product-session hash", advertised.SubmitBinding, runtimepkg.RuntimeSubmitBindingV2)
	}
	if err := runtimepkg.ValidateRuntimeCapabilities(advertised); err != nil {
		t.Fatalf("candidate advertised invalid capability document: %v", err)
	}

	select {
	case <-probe.heartbeat:
	case <-time.After(10 * time.Second):
		t.Fatalf("candidate did not complete signed registration and heartbeat: %s", probe.failureOr(candidateValidationSafeOutput(childOutput.Bytes())))
	}
	if err := probe.failure(); err != nil {
		t.Fatal(err)
	}
	if probe.registrationCount() != 1 || probe.heartbeatCount() < 1 || probe.lastHeartbeatSequence() != 42 {
		t.Fatalf("candidate control-plane facts registration=%d heartbeat=%d sequence=%d", probe.registrationCount(), probe.heartbeatCount(), probe.lastHeartbeatSequence())
	}

	if err := command.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("send SIGTERM to candidate: %v", err)
	}
	select {
	case err := <-exit:
		stopped = true
		if err != nil {
			t.Fatalf("candidate did not exit cleanly after SIGTERM: %v: %s", err, candidateValidationSafeOutput(childOutput.Bytes()))
		}
	case <-time.After(10 * time.Second):
		_ = command.Process.Kill()
		<-exit
		t.Fatal("candidate did not exit within bounded SIGTERM cleanup window")
	}

	if _, err := os.Stat(tmpRoot); err != nil {
		t.Fatalf("candidate temporary runtime root missing before cleanup verification: %v", err)
	}
	for _, root := range []string{stateRoot, tmpRoot, logsRoot} {
		entries, err := os.ReadDir(root)
		if err != nil {
			t.Fatalf("read candidate-only root %s: %v", root, err)
		}
		if len(entries) != 0 {
			t.Fatalf("candidate left state in fixture-only root %s: %v", root, candidateValidationEntryNames(entries))
		}
	}
	if err := candidateValidationAddressAvailable(adapterAddr); err != nil {
		t.Fatalf("candidate Adapter listener was not released: %v", err)
	}
	if err := candidateValidationAddressAvailable(proxyAddr); err != nil {
		t.Fatalf("candidate workspace-search listener was not released: %v", err)
	}
}

type candidateAdapterProbe struct {
	t                 *testing.T
	verifier          runtimepkg.RuntimeHostHeartbeatVerifier
	hostID            string
	instanceID        string
	environment       string
	capabilityHash    string
	mu                sync.Mutex
	registrations     int
	heartbeats        int
	lastSequence      int64
	firstFailure      error
	heartbeat         chan struct{}
	heartbeatObserved sync.Once
}

func newCandidateAdapterProbe(t *testing.T, verifier runtimepkg.RuntimeHostHeartbeatVerifier, hostID, instanceID, environment, capabilityHash string) *candidateAdapterProbe {
	return &candidateAdapterProbe{
		t: t, verifier: verifier, hostID: hostID, instanceID: instanceID, environment: environment,
		capabilityHash: capabilityHash, heartbeat: make(chan struct{}),
	}
}

func (p *candidateAdapterProbe) backendHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := candidateValidationRemoteLoopback(r.RemoteAddr); err != nil {
			p.recordFailure(err)
			candidateValidationBackendError(w)
			return
		}
		if r.Header.Get("X-Runtime-Host-Id") != p.hostID || r.Header.Get("X-Runtime-Instance-Id") != p.instanceID || r.Header.Get("X-Runtime-Environment") != p.environment {
			p.recordFailure(fmt.Errorf("candidate control-plane identity headers are not fixture-bound"))
			candidateValidationBackendError(w)
			return
		}
		switch r.URL.Path {
		case "/internal/v1/runtime-hosts/register":
			p.handleRegistration(w, r)
		case "/internal/v1/runtime-hosts/" + p.hostID + "/heartbeat":
			p.handleHeartbeat(w, r)
		default:
			p.recordFailure(fmt.Errorf("candidate requested unexpected Backend path %q", r.URL.Path))
			candidateValidationBackendError(w)
		}
	})
}

func (p *candidateAdapterProbe) handleRegistration(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		p.recordFailure(fmt.Errorf("candidate registration method=%s", r.Method))
		candidateValidationBackendError(w)
		return
	}
	var body struct {
		Endpoint             string                               `json:"endpoint"`
		Zone                 string                               `json:"zone"`
		RuntimeVersion       string                               `json:"runtimeVersion"`
		AdapterVersion       string                               `json:"adapterVersion"`
		Capabilities         runtimepkg.RuntimeCapabilitySnapshot `json:"capabilities"`
		SessionStoreID       string                               `json:"sessionStoreId"`
		MaxActiveRuns        int                                  `json:"maxActiveRuns"`
		MaxProductThreadRuns int                                  `json:"maxProductThreadRuns"`
		MaxDetachedTaskRuns  int                                  `json:"maxDetachedTaskRuns"`
		ObservedAt           time.Time                            `json:"observedAt"`
		SignatureKeyID       string                               `json:"signatureKeyId"`
		Nonce                string                               `json:"nonce"`
		BodySHA256           string                               `json:"bodySha256"`
		Signature            string                               `json:"signature"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&body); err != nil {
		p.recordFailure(fmt.Errorf("decode candidate registration: %w", err))
		candidateValidationBackendError(w)
		return
	}
	registration := runtimepkg.RuntimeHostRegistration{
		Endpoint: body.Endpoint, Zone: body.Zone, RuntimeVersion: body.RuntimeVersion, AdapterVersion: body.AdapterVersion,
		Capabilities: body.Capabilities, SessionStoreID: body.SessionStoreID, MaxActiveRuns: body.MaxActiveRuns,
		MaxProductThreadRuns: body.MaxProductThreadRuns, MaxDetachedTaskRuns: body.MaxDetachedTaskRuns,
	}
	proof := runtimepkg.RuntimeHostRegistrationProof{
		ObservedAt: body.ObservedAt, SignatureKeyID: body.SignatureKeyID, Nonce: body.Nonce,
		BodySHA256: body.BodySHA256, Signature: body.Signature,
	}
	principal := runtimepkg.RuntimeHostPrincipal{RuntimeHostID: p.hostID, InstanceID: p.instanceID, Environment: p.environment, CertificateID: "adapter-attested"}
	if err := p.verifier.VerifyRegistration(r.Context(), principal, r.Method, r.URL.Path, registration, proof); err != nil {
		p.recordFailure(fmt.Errorf("candidate registration signature: %w", err))
		candidateValidationBackendError(w)
		return
	}
	if registration.Endpoint == "" || registration.Capabilities.CapabilityHash != p.capabilityHash || registration.MaxActiveRuns != 8 || registration.MaxProductThreadRuns != 8 || registration.MaxDetachedTaskRuns != 8 {
		p.recordFailure(fmt.Errorf("candidate registration contract is incomplete"))
		candidateValidationBackendError(w)
		return
	}
	p.mu.Lock()
	p.registrations++
	p.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{"success": true, "data": runtimepkg.RuntimeHost{
		RuntimeHostID: p.hostID, InstanceID: p.instanceID, Environment: p.environment, HeartbeatSequence: 41,
		InstanceGeneration: 1, RecoveryRevision: 1, RecoveryState: "pending",
	}})
}

func (p *candidateAdapterProbe) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		p.recordFailure(fmt.Errorf("candidate heartbeat method=%s", r.Method))
		candidateValidationBackendError(w)
		return
	}
	var body struct {
		Sequence       int64          `json:"sequence"`
		ObservedAt     time.Time      `json:"observedAt"`
		ActiveRuns     int            `json:"activeRuns"`
		ReservedRuns   int            `json:"reservedRuns"`
		CapabilityHash string         `json:"capabilityHash"`
		SafeHealth     map[string]any `json:"safeHealth"`
		SignatureKeyID string         `json:"signatureKeyId"`
		Nonce          string         `json:"nonce"`
		BodySHA256     string         `json:"bodySha256"`
		Signature      string         `json:"signature"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&body); err != nil {
		p.recordFailure(fmt.Errorf("decode candidate heartbeat: %w", err))
		candidateValidationBackendError(w)
		return
	}
	heartbeat := runtimepkg.RuntimeHostHeartbeat{
		Sequence: body.Sequence, ObservedAt: body.ObservedAt, ActiveRuns: body.ActiveRuns, ReservedRuns: body.ReservedRuns,
		CapabilityHash: body.CapabilityHash, SafeHealth: body.SafeHealth, SignatureKeyID: body.SignatureKeyID,
		Nonce: body.Nonce, BodySHA256: body.BodySHA256, Signature: body.Signature,
	}
	principal := runtimepkg.RuntimeHostPrincipal{RuntimeHostID: p.hostID, InstanceID: p.instanceID, Environment: p.environment, CertificateID: "adapter-attested"}
	if err := p.verifier.VerifyHeartbeat(r.Context(), principal, r.Method, r.URL.Path, heartbeat); err != nil {
		p.recordFailure(fmt.Errorf("candidate heartbeat signature: %w", err))
		candidateValidationBackendError(w)
		return
	}
	if heartbeat.Sequence != 42 || heartbeat.CapabilityHash != p.capabilityHash || heartbeat.ActiveRuns != 0 || heartbeat.ReservedRuns != 0 {
		p.recordFailure(fmt.Errorf("candidate heartbeat contract sequence=%d capability=%q active=%d reserved=%d", heartbeat.Sequence, heartbeat.CapabilityHash, heartbeat.ActiveRuns, heartbeat.ReservedRuns))
		candidateValidationBackendError(w)
		return
	}
	p.mu.Lock()
	p.heartbeats++
	p.lastSequence = heartbeat.Sequence
	p.mu.Unlock()
	p.heartbeatObserved.Do(func() { close(p.heartbeat) })
	writeJSON(w, http.StatusOK, map[string]any{"success": true})
}

func (p *candidateAdapterProbe) recordFailure(err error) {
	if err == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.firstFailure == nil {
		p.firstFailure = err
	}
}

func (p *candidateAdapterProbe) failure() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.firstFailure
}

func (p *candidateAdapterProbe) failureOr(fallback string) string {
	if err := p.failure(); err != nil {
		return err.Error()
	}
	return fallback
}

func (p *candidateAdapterProbe) registrationCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.registrations
}

func (p *candidateAdapterProbe) heartbeatCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.heartbeats
}

func (p *candidateAdapterProbe) lastHeartbeatSequence() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.lastSequence
}

func candidateGatewayHandler(t *testing.T, token string, capabilities runtimepkg.RuntimeCapabilities) http.Handler {
	t.Helper()
	rawCapabilities, err := json.Marshal(capabilities)
	if err != nil {
		t.Fatal(err)
	}
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := candidateValidationRemoteLoopback(r.RemoteAddr); err != nil {
			http.Error(w, "loopback required", http.StatusForbidden)
			return
		}
		connection, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer connection.Close()
		if err := connection.WriteJSON(map[string]any{"type": "event", "event": "connect.challenge", "payload": map[string]any{"nonce": "candidate-validation-nonce"}}); err != nil {
			return
		}
		var connect map[string]any
		if err := connection.ReadJSON(&connect); err != nil || gatewayStringValue(connect["method"]) != "connect" || gatewayMapValue(gatewayMapValue(connect["params"])["auth"])["token"] != token {
			return
		}
		if err := connection.WriteJSON(map[string]any{"type": "res", "id": connect["id"], "ok": true, "payload": map[string]any{"type": "hello-ok", "protocol": gatewayProtocolVersion}}); err != nil {
			return
		}
		for {
			var frame map[string]any
			if err := connection.ReadJSON(&frame); err != nil {
				return
			}
			if gatewayStringValue(frame["method"]) != "enterprise.runtime.capabilities" {
				_ = connection.WriteJSON(map[string]any{"type": "res", "id": frame["id"], "ok": false, "error": map[string]any{"code": "RUNTIME_INPUT_INVALID"}})
				continue
			}
			var payload any
			if err := json.Unmarshal(rawCapabilities, &payload); err != nil {
				return
			}
			if err := connection.WriteJSON(map[string]any{"type": "res", "id": frame["id"], "ok": true, "payload": payload}); err != nil {
				return
			}
		}
	})
}

func candidateValidationWaitForCapabilities(t *testing.T, endpoint string, exited <-chan error, output *bytes.Buffer) []byte {
	t.Helper()
	client := &http.Client{Transport: &http.Transport{Proxy: nil}, Timeout: 500 * time.Millisecond}
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		response, err := client.Get(endpoint + "/enterprise.runtime/capabilities")
		if err == nil {
			body, readErr := io.ReadAll(io.LimitReader(response.Body, 1<<20))
			response.Body.Close()
			if readErr == nil && response.StatusCode == http.StatusOK {
				return body
			}
		}
		select {
		case err := <-exited:
			t.Fatalf("candidate exited before capability handshake: %v: %s", err, candidateValidationSafeOutput(output.Bytes()))
		case <-time.After(100 * time.Millisecond):
		}
	}
	t.Fatalf("candidate capability endpoint did not become ready: %s", candidateValidationSafeOutput(output.Bytes()))
	return nil
}

func candidateValidationEnvironment(root, adapterAddr, proxyAddr, gatewayURL, backendURL, endpoint, keyPath, hostID, instanceID, environment, keyID, gatewayToken, stateRoot, tmpRoot, logsRoot, configPath string) []string {
	values := []string{
		"HOME=" + root,
		"TMPDIR=" + tmpRoot,
		"TMP=" + tmpRoot,
		"TEMP=" + tmpRoot,
		"HUAHUO_ENV=" + environment,
		"HUAHUO_OPENCLAW_ADAPTER_ADDR=" + adapterAddr,
		"HUAHUO_RUNTIME_WORKSPACE_SEARCH_PROXY_ADDR=" + proxyAddr,
		"HUAHUO_OPENCLAW_GATEWAY_URL=" + gatewayURL,
		"OPENCLAW_GATEWAY_TOKEN=" + gatewayToken,
		"HUAHUO_BACKEND_INTERNAL_URL=" + backendURL,
		"HUAHUO_RUNTIME_HOST_ID=" + hostID,
		"HUAHUO_RUNTIME_INSTANCE_ID=" + instanceID,
		"HUAHUO_RUNTIME_HOST_ENDPOINT=" + endpoint,
		"HUAHUO_RUNTIME_ZONE=candidate-loopback",
		"OPENCLAW_VERSION=2026.6.2",
		"HUAHUO_RUNTIME_ADAPTER_VERSION=candidate-validation",
		"HUAHUO_RUNTIME_SESSION_STORE_ID=candidate-validation",
		"HUAHUO_RUNTIME_RUN_TICKET_SECRET=" + strings.Repeat("v", 48),
		"HUAHUO_RUNTIME_POLICY_KEY_ID=candidate-validation-policy",
		"HUAHUO_RUNTIME_HOST_HEARTBEAT_SIGNING_KEY_FILE=" + keyPath,
		"HUAHUO_RUNTIME_HOST_HEARTBEAT_KEY_ID=" + keyID,
		"HUAHUO_OPENCLAW_RUNTIME_STATE_DIR=" + stateRoot,
		"HUAHUO_OPENCLAW_RUNTIME_TMP_ROOT=" + tmpRoot,
		"HUAHUO_OPENCLAW_RUNTIME_LOGS_DIR=" + logsRoot,
		"HUAHUO_OPENCLAW_ENTERPRISE_RUNTIME_CONFIG_PATH=" + configPath,
		"HUAHUO_HTTP_SHUTDOWN_TIMEOUT_SECONDS=3",
	}
	for _, value := range values {
		key, _, _ := strings.Cut(value, "=")
		if strings.Contains(strings.ToLower(key), "database") || strings.Contains(strings.ToLower(key), "redis") || strings.Contains(strings.ToLower(key), "dsn") || strings.Contains(strings.ToLower(key), "password") {
			panic("candidate validation environment accidentally contains a data-store variable")
		}
	}
	return values
}

// candidateValidationPrepareCandidate normally compiles current source under
// the test temp root. A host without Go may instead supply a separately built
// candidate through HUAHUO_RUNTIME_ADAPTER_CANDIDATE_BINARY. The harness still
// copies that executable under an explicitly removed fixture root before start,
// so it never executes an active Adapter path in place.
func candidateValidationPrepareCandidate(t *testing.T, destination string) string {
	t.Helper()
	if supplied := strings.TrimSpace(os.Getenv("HUAHUO_RUNTIME_ADAPTER_CANDIDATE_BINARY")); supplied != "" {
		if err := candidateValidationCopySuppliedCandidate(filepath.Dir(destination), destination, supplied, candidateValidationDefaultArtifactPolicy); err != nil {
			t.Fatalf("copy supplied candidate: %v", err)
		}
	} else {
		offlineModuleCache := strings.TrimSpace(os.Getenv("HUAHUO_RUNTIME_ADAPTER_CANDIDATE_GOMODCACHE"))
		layout, err := candidateValidationPrepareBuildLayout(filepath.Dir(destination), offlineModuleCache)
		if err != nil {
			t.Fatalf("prepare isolated Adapter build cache: %v", err)
		}
		goBinary := filepath.Join(goruntime.GOROOT(), "bin", "go")
		goInfo, err := os.Stat(goBinary)
		if err != nil || !goInfo.Mode().IsRegular() || goInfo.Mode()&0o111 == 0 {
			t.Fatalf("trusted Go toolchain is not executable: info=%v err=%v", goInfo, err)
		}
		buildCtx, cancelBuild := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancelBuild()
		build := exec.CommandContext(buildCtx, goBinary, "build", "-mod=readonly", "-trimpath", "-buildvcs=false", "-o", destination, ".")
		build.Dir = "."
		build.Env = candidateValidationBuildEnvironment(layout)
		buildOutput, err := build.CombinedOutput()
		if err != nil {
			t.Fatalf("build isolated Adapter candidate: %v: %s", err, candidateValidationSafeOutput(buildOutput))
		}
	}
	info, err := os.Stat(destination)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&0o111 == 0 {
		t.Fatalf("candidate binary is not a regular executable: info=%v err=%v", info, err)
	}
	return candidateValidationFileSHA256(t, destination)
}

func candidateValidationFileSHA256(t *testing.T, path string) string {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() < 0 || info.Size() > candidateValidationMaximumCandidateBytes {
		t.Fatalf("candidate binary exceeds %d byte limit", candidateValidationMaximumCandidateBytes)
	}
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		t.Fatal(err)
	}
	return "sha256:" + hex.EncodeToString(hash.Sum(nil))
}

func candidateValidationKeyPair(t *testing.T) (ed25519.PrivateKey, ed25519.PublicKey) {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(cryptorand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return privateKey, publicKey
}

func candidateValidationUnusedLoopbackAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

func candidateValidationAddressAvailable(address string) error {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	return listener.Close()
}

func candidateValidationLoopbackURL(value, scheme string) error {
	parsed, err := url.Parse(value)
	if err != nil || parsed.Scheme != scheme || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.Hostname() == "" {
		return fmt.Errorf("invalid %s URL", scheme)
	}
	ip := net.ParseIP(parsed.Hostname())
	if ip == nil || !ip.IsLoopback() {
		return fmt.Errorf("non-loopback host %q", parsed.Hostname())
	}
	return nil
}

func candidateValidationRemoteLoopback(remoteAddr string) error {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		return fmt.Errorf("invalid remote address")
	}
	ip := net.ParseIP(strings.Trim(host, "[]"))
	if ip == nil || !ip.IsLoopback() {
		return fmt.Errorf("non-loopback remote address")
	}
	return nil
}

func candidateValidationBackendError(w http.ResponseWriter) {
	writeJSON(w, http.StatusForbidden, map[string]any{"success": false, "error": map[string]any{"code": "RUNTIME_HOST_UNAUTHORIZED"}})
}

func candidateValidationEntryNames(entries []os.DirEntry) []string {
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	return names
}

func candidateValidationSafeOutput(raw []byte) string {
	text := strings.TrimSpace(string(raw))
	if len(text) > 2048 {
		text = text[:2048] + " [truncated]"
	}
	return text
}
