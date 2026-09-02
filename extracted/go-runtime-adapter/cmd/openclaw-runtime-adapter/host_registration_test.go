package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	runtimepkg "huahuoai/backend/source/internal/runtime"
)

func TestHostRegistrationLoopContinuesPersistedHeartbeatSequence(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	heartbeatSequence := make(chan int64, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/internal/v1/runtime-hosts/register":
			writeJSON(w, http.StatusOK, map[string]any{"success": true, "data": map[string]any{"heartbeatSequence": 41}})
		case "/internal/v1/runtime-hosts/host-restart/heartbeat":
			var body struct {
				Sequence int64 `json:"sequence"`
			}
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
				heartbeatSequence <- -1
			} else {
				heartbeatSequence <- body.Sequence
			}
			writeJSON(w, http.StatusOK, map[string]any{"success": true, "data": map[string]any{"heartbeatSequence": body.Sequence}})
			cancel()
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	capabilities := readyRegistrationCapabilities()
	rawCapabilities, err := json.Marshal(capabilities)
	if err != nil {
		t.Fatal(err)
	}
	adapter := &adapter{
		backendURL: server.URL, hostServiceToken: "service-token", runtimeHostID: "host-restart",
		runtimeInstanceID: "static-instance", runtimeEnvironment: "test", runtimeHostEndpoint: "http://runtime-host",
		runtimeZone: "test", runtimeVersion: "runtime-v1", adapterVersion: "adapter-v1", sessionStoreID: "store-v1",
		maxActiveRuns: 2, maxProductThreadRuns: 2, maxDetachedTaskRuns: 2,
		heartbeatSigner: testRuntimeHostSigner(t),
		invoke: func(context.Context, string, map[string]any, time.Duration) ([]byte, []byte, error) {
			return rawCapabilities, nil, nil
		},
	}
	adapter.admission = NewHostAdmissionController(2, 2, 2)

	adapter.runHostRegistrationLoop(ctx)

	select {
	case sequence := <-heartbeatSequence:
		if sequence != 42 {
			t.Fatalf("first heartbeat after restart sequence=%d want 42", sequence)
		}
	default:
		t.Fatal("registration loop did not send a heartbeat")
	}
}

func TestHostRegistrationRetryDelayIsBoundedAndDeterministic(t *testing.T) {
	policy := hostRegistrationRetryPolicy{
		MinDelay: 100 * time.Millisecond,
		MaxDelay: 400 * time.Millisecond,
		Jitter: func(base time.Duration) time.Duration {
			return base / 4
		},
	}

	for _, test := range []struct {
		failures int
		want     time.Duration
	}{
		{failures: 1, want: 125 * time.Millisecond},
		{failures: 2, want: 250 * time.Millisecond},
		{failures: 3, want: 400 * time.Millisecond},
		{failures: 99, want: 400 * time.Millisecond},
	} {
		if got := hostRegistrationRetryDelay(policy, test.failures); got != test.want {
			t.Fatalf("retry delay failures=%d got=%s want=%s", test.failures, got, test.want)
		}
	}

	policy.Jitter = func(time.Duration) time.Duration { return -time.Hour }
	if got := hostRegistrationRetryDelay(policy, 1); got != 100*time.Millisecond {
		t.Fatalf("negative jitter escaped minimum bound: %s", got)
	}
	policy.Jitter = func(time.Duration) time.Duration { return time.Hour }
	if got := hostRegistrationRetryDelay(policy, 1); got != 400*time.Millisecond {
		t.Fatalf("positive jitter escaped maximum bound: %s", got)
	}
	policy.Jitter = func(base time.Duration) time.Duration { return base / 10 }
	maximumJitterBase := policy.MaxDelay - policy.MaxDelay/6
	if got, want := hostRegistrationRetryDelay(policy, 99), maximumJitterBase+maximumJitterBase/10; got != want || got >= policy.MaxDelay {
		t.Fatalf("capped retry lost jitter: got=%s want=%s max=%s", got, want, policy.MaxDelay)
	}
}

func TestHostRegistrationLoopUsesJitteredRetryAndResetsAfterHealthyHeartbeat(t *testing.T) {
	registrationCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/internal/v1/runtime-hosts/register":
			registrationCalls++
			if registrationCalls == 1 {
				writeJSON(w, http.StatusServiceUnavailable, map[string]any{"success": false})
				return
			}
			writeJSON(w, http.StatusOK, map[string]any{"success": true, "data": map[string]any{"heartbeatSequence": 7}})
		case "/internal/v1/runtime-hosts/host-backoff/heartbeat":
			writeJSON(w, http.StatusOK, map[string]any{"success": true})
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	adapter := testRegistrationAdapter(t, server.URL, "host-backoff")
	rawCapabilities, err := json.Marshal(readyRegistrationCapabilities())
	if err != nil {
		t.Fatal(err)
	}
	capabilityCalls := 0
	adapter.invoke = func(context.Context, string, map[string]any, time.Duration) ([]byte, []byte, error) {
		capabilityCalls++
		if capabilityCalls == 3 {
			return nil, nil, errors.New("capability unavailable")
		}
		return rawCapabilities, nil, nil
	}

	delays := make([]time.Duration, 0, 3)
	adapter.runHostRegistrationLoopWithRetryPolicy(context.Background(), hostRegistrationRetryPolicy{
		MinDelay: 100 * time.Millisecond, MaxDelay: 800 * time.Millisecond, HeartbeatDelay: 900 * time.Millisecond,
		Jitter: func(time.Duration) time.Duration { return 10 * time.Millisecond },
		Wait: func(_ context.Context, delay time.Duration) bool {
			delays = append(delays, delay)
			return len(delays) < 3
		},
	})

	want := []time.Duration{110 * time.Millisecond, 900 * time.Millisecond, 110 * time.Millisecond}
	if registrationCalls != 2 || capabilityCalls != 3 || len(delays) != len(want) {
		t.Fatalf("registration=%d capability=%d delays=%v", registrationCalls, capabilityCalls, delays)
	}
	for index := range want {
		if delays[index] != want[index] {
			t.Fatalf("delay[%d]=%s want=%s all=%v", index, delays[index], want[index], delays)
		}
	}
}

func TestHostRegistrationLoopKeepsBackoffAfterRegistrationBeforeHeartbeatFailure(t *testing.T) {
	registrationCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/internal/v1/runtime-hosts/register":
			registrationCalls++
			if registrationCalls == 1 {
				writeJSON(w, http.StatusServiceUnavailable, map[string]any{"success": false})
				return
			}
			writeJSON(w, http.StatusOK, map[string]any{"success": true, "data": map[string]any{"heartbeatSequence": 7}})
		case "/internal/v1/runtime-hosts/host-registration-reset/heartbeat":
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{"success": false})
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	adapter := testRegistrationAdapter(t, server.URL, "host-registration-reset")
	rawCapabilities, err := json.Marshal(readyRegistrationCapabilities())
	if err != nil {
		t.Fatal(err)
	}
	adapter.invoke = func(context.Context, string, map[string]any, time.Duration) ([]byte, []byte, error) {
		return rawCapabilities, nil, nil
	}

	delays := make([]time.Duration, 0, 2)
	adapter.runHostRegistrationLoopWithRetryPolicy(context.Background(), hostRegistrationRetryPolicy{
		MinDelay: 100 * time.Millisecond, MaxDelay: 800 * time.Millisecond,
		Jitter: func(time.Duration) time.Duration { return 10 * time.Millisecond },
		Wait: func(_ context.Context, delay time.Duration) bool {
			delays = append(delays, delay)
			return len(delays) < 2
		},
	})

	want := []time.Duration{110 * time.Millisecond, 210 * time.Millisecond}
	if registrationCalls != 2 || len(delays) != len(want) {
		t.Fatalf("registration=%d delays=%v", registrationCalls, delays)
	}
	for index := range want {
		if delays[index] != want[index] {
			t.Fatalf("delay[%d]=%s want=%s all=%v", index, delays[index], want[index], delays)
		}
	}
}

func TestWaitForHostRegistrationDelayHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	started := time.Now()
	if waitForHostRegistrationDelay(ctx, time.Hour) {
		t.Fatal("cancelled registration wait returned success")
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("cancelled registration wait did not return promptly: %s", elapsed)
	}
}

func TestHostRegistrationLoopStopsBeforeCapabilityLoadWhenCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	adapter := testRegistrationAdapter(t, "http://127.0.0.1:1", "host-cancelled")
	adapter.invoke = func(context.Context, string, map[string]any, time.Duration) ([]byte, []byte, error) {
		t.Fatal("cancelled registration loop attempted capability load")
		return nil, nil, nil
	}
	adapter.runHostRegistrationLoopWithRetryPolicy(ctx, hostRegistrationRetryPolicy{
		Wait: func(context.Context, time.Duration) bool {
			t.Fatal("cancelled registration loop attempted a retry wait")
			return false
		},
	})
}

func TestExplicitReregistrationInstructionResumesFromServerSequence(t *testing.T) {
	registerCount := 0
	heartbeats := make([]int64, 0, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/internal/v1/runtime-hosts/register":
			registerCount++
			sequence := int64(41)
			if registerCount > 1 {
				sequence = 571
			}
			writeJSON(w, http.StatusOK, map[string]any{"success": true, "data": map[string]any{"heartbeatSequence": sequence}})
		case "/internal/v1/runtime-hosts/host-retry/heartbeat":
			var body struct {
				Sequence int64 `json:"sequence"`
			}
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
				t.Errorf("decode heartbeat: %v", err)
			}
			heartbeats = append(heartbeats, body.Sequence)
			if len(heartbeats) == 1 {
				writeJSON(w, http.StatusConflict, map[string]any{"success": false, "error": map[string]any{"code": "RUNTIME_HOST_REREGISTRATION_REQUIRED", "retryable": true}})
				return
			}
			writeJSON(w, http.StatusOK, map[string]any{"success": true, "data": map[string]any{"heartbeatSequence": body.Sequence}})
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	adapter := testRegistrationAdapter(t, server.URL, "host-retry")
	capabilities := readyRegistrationCapabilities()
	registeredHash, sequence := adapter.reportRuntimeHost(context.Background(), capabilities, "", 0)
	if registeredHash != "" || sequence != 42 {
		t.Fatalf("failed heartbeat retained registration: hash=%q sequence=%d", registeredHash, sequence)
	}
	registeredHash, sequence = adapter.reportRuntimeHost(context.Background(), capabilities, registeredHash, sequence)
	if registeredHash != capabilities.CapabilityHash || sequence != 572 {
		t.Fatalf("reregistration did not resume server sequence: hash=%q sequence=%d", registeredHash, sequence)
	}
	if registerCount != 2 || len(heartbeats) != 2 || heartbeats[0] != 42 || heartbeats[1] != 572 {
		t.Fatalf("registerCount=%d heartbeats=%v", registerCount, heartbeats)
	}
}

func TestHeartbeatConflictWithoutExplicitInstructionDoesNotReregister(t *testing.T) {
	registerCount := 0
	heartbeats := make([]int64, 0, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/internal/v1/runtime-hosts/register":
			registerCount++
			writeJSON(w, http.StatusOK, map[string]any{"success": true, "data": map[string]any{"heartbeatSequence": 41}})
		case "/internal/v1/runtime-hosts/host-stale/heartbeat":
			var body struct {
				Sequence int64 `json:"sequence"`
			}
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
				t.Errorf("decode heartbeat: %v", err)
			}
			heartbeats = append(heartbeats, body.Sequence)
			if len(heartbeats) == 1 {
				writeJSON(w, http.StatusConflict, map[string]any{"success": false, "error": map[string]any{"code": "RUNTIME_HEARTBEAT_STALE"}})
				return
			}
			writeJSON(w, http.StatusOK, map[string]any{"success": true})
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	adapter := testRegistrationAdapter(t, server.URL, "host-stale")
	capabilities := readyRegistrationCapabilities()
	registeredHash, sequence := adapter.reportRuntimeHost(context.Background(), capabilities, "", 0)
	if registeredHash != capabilities.CapabilityHash || sequence != 42 {
		t.Fatalf("stale heartbeat discarded registration: hash=%q sequence=%d", registeredHash, sequence)
	}
	registeredHash, sequence = adapter.reportRuntimeHost(context.Background(), capabilities, registeredHash, sequence)
	if registeredHash != capabilities.CapabilityHash || sequence != 43 {
		t.Fatalf("follow-up heartbeat did not retain registration: hash=%q sequence=%d", registeredHash, sequence)
	}
	if registerCount != 1 || len(heartbeats) != 2 || heartbeats[0] != 42 || heartbeats[1] != 43 {
		t.Fatalf("registerCount=%d heartbeats=%v", registerCount, heartbeats)
	}
}

func TestRuntimeHostBackendErrorOnlyRecognizesExplicitReregistration(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want bool
	}{
		{name: "explicit instruction", raw: `{"success":false,"error":{"code":"RUNTIME_HOST_REREGISTRATION_REQUIRED","retryable":true}}`, want: true},
		{name: "stale heartbeat", raw: `{"success":false,"error":{"code":"RUNTIME_HEARTBEAT_STALE"}}`},
		{name: "malformed envelope", raw: `{"success":false,"error":"invalid"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := runtimeHostReregistrationRequired(newRuntimeHostBackendError(http.StatusConflict, []byte(tt.raw))); got != tt.want {
				t.Fatalf("runtimeHostReregistrationRequired()=%v want %v", got, tt.want)
			}
		})
	}
}

func TestHostIdentityRejectionStopsRegistrationAndBlocksNewDispatch(t *testing.T) {
	registrationCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/internal/v1/runtime-hosts/register" {
			http.NotFound(w, request)
			return
		}
		registrationCalls++
		writeJSON(w, http.StatusForbidden, map[string]any{"success": false, "error": map[string]any{"code": "RUNTIME_HOST_UNAUTHORIZED"}})
	}))
	defer server.Close()

	adapter := testRegistrationAdapter(t, server.URL, "host-identity-rejected")
	rawCapabilities, err := json.Marshal(readyRegistrationCapabilities())
	if err != nil {
		t.Fatal(err)
	}
	adapter.invoke = func(context.Context, string, map[string]any, time.Duration) ([]byte, []byte, error) {
		return rawCapabilities, nil, nil
	}
	adapter.runHostRegistrationLoopWithRetryPolicy(context.Background(), hostRegistrationRetryPolicy{
		Wait: func(context.Context, time.Duration) bool {
			t.Fatal("identity rejection must stop before retry wait")
			return false
		},
	})
	if registrationCalls != 1 {
		t.Fatalf("registration calls=%d want 1", registrationCalls)
	}
	if _, err := adapter.acquireRunPermit("run-after-identity-rejection", "product_thread"); err == nil || err.Error() != "RUNTIME_HOST_UNAUTHORIZED" {
		t.Fatalf("new dispatch after identity rejection error=%v", err)
	}
}

func TestAdapterHeartbeatSignsCanonicalEnvelopeWhenSignerConfigured(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := runtimepkg.NewEd25519RuntimeHostHeartbeatSigner("key-adapter", privateKey)
	if err != nil {
		t.Fatal(err)
	}
	verifier, err := runtimepkg.NewEd25519RuntimeHostHeartbeatVerifier([]runtimepkg.RuntimeHostVerificationKey{{KeyID: "key-adapter", PublicKey: publicKey}}, nil, time.Minute, 2*time.Minute, runtimepkg.NewMemoryRuntimeHostNonceStore())
	if err != nil {
		t.Fatal(err)
	}
	verified := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
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
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			verified <- err
			writeJSON(response, http.StatusBadRequest, map[string]any{"success": false})
			return
		}
		verified <- verifier.VerifyHeartbeat(request.Context(), runtimepkg.RuntimeHostPrincipal{
			RuntimeHostID: "host-signed", InstanceID: "instance-signed", Environment: "test", CertificateID: "adapter-attested",
		}, request.Method, request.URL.Path, runtimepkg.RuntimeHostHeartbeat{
			Sequence: body.Sequence, ObservedAt: body.ObservedAt, ActiveRuns: body.ActiveRuns, ReservedRuns: body.ReservedRuns,
			CapabilityHash: body.CapabilityHash, SafeHealth: body.SafeHealth, SignatureKeyID: body.SignatureKeyID,
			Nonce: body.Nonce, BodySHA256: body.BodySHA256, Signature: body.Signature,
		})
		writeJSON(response, http.StatusOK, map[string]any{"success": true})
	}))
	defer server.Close()
	adapter := testRegistrationAdapter(t, server.URL, "host-signed")
	adapter.runtimeInstanceID = "instance-signed"
	adapter.heartbeatSigner = signer
	if err := adapter.heartbeatRuntimeHost(context.Background(), "cap-signed", 1); err != nil {
		t.Fatal(err)
	}
	if err := <-verified; err != nil {
		t.Fatalf("adapter heartbeat was not verifiable: %v", err)
	}
}

func TestAdapterRegistrationSignsCanonicalEnvelopeWhenSignerConfigured(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := runtimepkg.NewEd25519RuntimeHostHeartbeatSigner("key-registration", privateKey)
	if err != nil {
		t.Fatal(err)
	}
	verifier, err := runtimepkg.NewEd25519RuntimeHostHeartbeatVerifier([]runtimepkg.RuntimeHostVerificationKey{{KeyID: "key-registration", PublicKey: publicKey}}, nil, time.Minute, 2*time.Minute, runtimepkg.NewMemoryRuntimeHostNonceStore())
	if err != nil {
		t.Fatal(err)
	}
	verified := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
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
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			verified <- err
			writeJSON(response, http.StatusBadRequest, map[string]any{"success": false})
			return
		}
		registration := runtimepkg.RuntimeHostRegistration{
			Endpoint: body.Endpoint, Zone: body.Zone, RuntimeVersion: body.RuntimeVersion, AdapterVersion: body.AdapterVersion,
			Capabilities: body.Capabilities, SessionStoreID: body.SessionStoreID, MaxActiveRuns: body.MaxActiveRuns,
			MaxProductThreadRuns: body.MaxProductThreadRuns, MaxDetachedTaskRuns: body.MaxDetachedTaskRuns,
		}
		verified <- verifier.VerifyRegistration(request.Context(), runtimepkg.RuntimeHostPrincipal{
			RuntimeHostID: "host-registration", InstanceID: "instance-registration", Environment: "test", CertificateID: "adapter-attested",
		}, request.Method, request.URL.Path, registration, runtimepkg.RuntimeHostRegistrationProof{
			ObservedAt: body.ObservedAt, SignatureKeyID: body.SignatureKeyID, Nonce: body.Nonce,
			BodySHA256: body.BodySHA256, Signature: body.Signature,
		})
		writeJSON(response, http.StatusOK, map[string]any{"success": true, "data": map[string]any{"heartbeatSequence": 0}})
	}))
	defer server.Close()
	adapter := testRegistrationAdapter(t, server.URL, "host-registration")
	adapter.runtimeInstanceID = "instance-registration"
	adapter.heartbeatSigner = signer
	if _, err := adapter.registerRuntimeHost(context.Background(), readyRegistrationCapabilities()); err != nil {
		t.Fatal(err)
	}
	if err := <-verified; err != nil {
		t.Fatalf("adapter registration was not verifiable: %v", err)
	}
}

func TestAdapterRegistrationAndHeartbeatRejectMissingSigner(t *testing.T) {
	adapter := testRegistrationAdapter(t, "http://127.0.0.1:1", "host-no-signer")
	adapter.heartbeatSigner = nil
	if _, err := adapter.registerRuntimeHost(context.Background(), readyRegistrationCapabilities()); err == nil || err.Error() != "RUNTIME_HOST_IDENTITY_UNAVAILABLE" {
		t.Fatalf("registration missing signer error=%v", err)
	}
	if err := adapter.heartbeatRuntimeHost(context.Background(), "cap-no-signer", 1); err == nil || err.Error() != "RUNTIME_HOST_IDENTITY_UNAVAILABLE" {
		t.Fatalf("heartbeat missing signer error=%v", err)
	}
}

func TestAdapterProductionHostRegistrationConfigurationFailsClosed(t *testing.T) {
	adapter := &adapter{
		backendURL: "http://127.0.0.1:8080", runtimeHostID: "host-prod", runtimeInstanceID: "instance-prod",
		runtimeEnvironment: "production", runtimeHostEndpoint: "http://127.0.0.1:18790",
	}
	if err := adapter.validateHostRegistrationConfiguration(); err == nil {
		t.Fatal("production Host registration accepted missing mTLS/signing/revocation configuration")
	}
}

func TestAdapterProductionHostRegistrationRejectsDefaultHTTPClient(t *testing.T) {
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := runtimepkg.NewEd25519RuntimeHostHeartbeatSigner("key-prod", privateKey)
	if err != nil {
		t.Fatal(err)
	}
	adapter := &adapter{
		backendURL: "https://backend.internal", runtimeHostID: "host-prod", runtimeInstanceID: "instance-prod",
		runtimeEnvironment: "production", runtimeHostEndpoint: "https://runtime.internal",
		hostMTLSTrustRef: "secret://server-env/TRUST", hostMTLSCertRef: "secret://server-env/CERT", hostMTLSKeyRef: "secret://server-env/KEY",
		hostServerCertRef: "secret://server-env/SERVER_CERT", hostServerKeyRef: "secret://server-env/SERVER_KEY", backendTrustRef: "secret://server-env/BACKEND_TRUST",
		heartbeatSigningRef: "secret://server-env/SIGN", heartbeatKeyID: "key-prod", hostRevocationRef: "secret://server-env/REVOKED",
		heartbeatSigner: signer, backendHTTPClient: http.DefaultClient,
	}
	if err := adapter.validateHostRegistrationConfiguration(); err == nil {
		t.Fatal("production Host registration accepted http.DefaultClient instead of the dedicated mTLS client")
	}
}

func TestAdapterProductionDoesNotFallbackToPlainRuntimeHostListener(t *testing.T) {
	adapter := &adapter{runtimeEnvironment: "production"}
	if err := adapter.serveRuntimeHostHTTP(&http.Server{Addr: "127.0.0.1:0", Handler: http.NotFoundHandler()}); err == nil || err.Error() != "RUNTIME_HOST_IDENTITY_UNAVAILABLE" {
		t.Fatalf("production Adapter listener error=%v, want fail-closed mTLS configuration error", err)
	}
}

func readyRegistrationCapabilities() runtimepkg.RuntimeCapabilities {
	tools := make([]runtimepkg.ToolCapability, 0, 3)
	for _, name := range []string{"read", "write", "workspace_search"} {
		tools = append(tools, runtimepkg.CanonicalAgentFacingToolCapability(name, "ready"))
	}
	return runtimepkg.RuntimeCapabilities{
		RuntimeVersion: "runtime-v1", AdapterVersion: "adapter-v1", CapabilityHash: "cap-v1", Tools: tools,
		FilesystemPolicy: runtimepkg.RuntimeFilesystemPolicy{
			WorkspaceOnlyReady: true, AbsolutePathRejected: true, SymlinkEscapeRejected: true,
		},
		Abort:         runtimepkg.RuntimeAbortCapability{Supported: true, AuthorizationReady: true},
		SubmitBinding: runtimepkg.RuntimeSubmitBindingCapability{Version: runtimepkg.RuntimeSubmitBindingV2, ProductSessionHash: true},
		BudgetCapabilities: runtimepkg.RuntimeBudgetCapabilities{
			MaxToolCallsSupported: 200, DefaultMaxToolCalls: 200, SupportsPerRunBudget: true,
			SupportsBudgetWarning: true, SupportsForcedAbort: true,
			ExecutionContract: runtimepkg.DefaultRuntimeToolBudgetExecutionContract(),
		},
	}
}

func TestRuntimeCapabilitiesReadyRejectsInvalidSubmitBinding(t *testing.T) {
	for name, mutate := range map[string]func(*runtimepkg.RuntimeCapabilities){
		"missing": func(capabilities *runtimepkg.RuntimeCapabilities) {
			capabilities.SubmitBinding = runtimepkg.RuntimeSubmitBindingCapability{}
		},
		"legacy": func(capabilities *runtimepkg.RuntimeCapabilities) {
			capabilities.SubmitBinding.Version = "runtime_submit_binding.v1"
		},
		"missing_product_session_hash": func(capabilities *runtimepkg.RuntimeCapabilities) {
			capabilities.SubmitBinding.ProductSessionHash = false
		},
	} {
		t.Run(name, func(t *testing.T) {
			capabilities := readyRegistrationCapabilities()
			mutate(&capabilities)
			if runtimeCapabilitiesReady(capabilities) {
				t.Fatalf("Adapter registration accepted submit binding=%+v", capabilities.SubmitBinding)
			}
		})
	}
}

func TestLoadRuntimeCapabilitiesProjectsAdapterOwnedSubmitBinding(t *testing.T) {
	rawCapabilities := readyRegistrationCapabilities()
	rawCapabilities.SubmitBinding = runtimepkg.RuntimeSubmitBindingCapability{}
	raw, err := json.Marshal(rawCapabilities)
	if err != nil {
		t.Fatal(err)
	}
	adapter := &adapter{invoke: func(context.Context, string, map[string]any, time.Duration) ([]byte, []byte, error) {
		return raw, nil, nil
	}}
	loaded, err := adapter.loadRuntimeCapabilities(context.Background())
	if err != nil {
		t.Fatalf("binding-free Gateway document must be projected by this Adapter: %v", err)
	}
	want := runtimepkg.RuntimeSubmitBindingCapability{Version: runtimepkg.RuntimeSubmitBindingV2, ProductSessionHash: true}
	if loaded.SubmitBinding != want || !runtimeCapabilitiesReady(loaded) {
		t.Fatalf("projected capabilities binding=%+v ready=%v", loaded.SubmitBinding, runtimeCapabilitiesReady(loaded))
	}
}

func TestLoadRuntimeCapabilitiesUsesDedicatedControlInvoker(t *testing.T) {
	rawCapabilities := readyRegistrationCapabilities()
	rawCapabilities.SubmitBinding = runtimepkg.RuntimeSubmitBindingCapability{}
	raw, err := json.Marshal(rawCapabilities)
	if err != nil {
		t.Fatal(err)
	}
	businessCalls := 0
	controlCalls := 0
	adapter := &adapter{
		invoke: func(context.Context, string, map[string]any, time.Duration) ([]byte, []byte, error) {
			businessCalls++
			return nil, nil, errors.New("business invoker must not load Host capabilities")
		},
		controlInvoke: func(_ context.Context, method string, _ map[string]any, timeout time.Duration) ([]byte, []byte, error) {
			controlCalls++
			if method != "enterprise.runtime.capabilities" || timeout != 5*time.Second {
				t.Fatalf("control capability call method=%s timeout=%s", method, timeout)
			}
			return raw, nil, nil
		},
	}
	if _, err := adapter.loadRuntimeCapabilities(context.Background()); err != nil {
		t.Fatalf("control capability load failed: %v", err)
	}
	if businessCalls != 0 || controlCalls != 1 {
		t.Fatalf("businessCalls=%d controlCalls=%d", businessCalls, controlCalls)
	}
}

func TestLoadRuntimeCapabilitiesFailureLogsOnlySafeStageAndCode(t *testing.T) {
	var output bytes.Buffer
	previousOutput := log.Writer()
	previousFlags := log.Flags()
	previousPrefix := log.Prefix()
	log.SetOutput(&output)
	log.SetFlags(0)
	log.SetPrefix("")
	t.Cleanup(func() {
		log.SetOutput(previousOutput)
		log.SetFlags(previousFlags)
		log.SetPrefix(previousPrefix)
	})

	const sensitive = "https://runtime.private/enterprise?ticket=ticket-secret certificate=cert-secret payload={credential:credential-secret} signature=signature-secret"
	adapter := &adapter{controlInvoke: func(context.Context, string, map[string]any, time.Duration) ([]byte, []byte, error) {
		return nil, nil, errors.New(sensitive)
	}}
	if _, err := adapter.loadRuntimeCapabilities(context.Background()); err == nil {
		t.Fatal("capability load unexpectedly succeeded")
	}

	got := output.String()
	want := "runtime host capability load failed stage=gateway_invoke code=RUNTIME_CAPACITY_UNAVAILABLE\n"
	if got != want {
		t.Fatalf("capability failure log = %q, want %q", got, want)
	}
	for _, forbidden := range []string{"runtime.private", "ticket-secret", "cert-secret", "payload", "credential-secret", "signature-secret", "detail="} {
		if strings.Contains(got, forbidden) {
			t.Fatalf("capability failure log leaked %q: %q", forbidden, got)
		}
	}
}

func TestRuntimeCapabilitiesReadyRejectsMissingToolBudgetExecutionContract(t *testing.T) {
	capabilities := readyRegistrationCapabilities()
	capabilities.BudgetCapabilities.ExecutionContract = runtimepkg.RuntimeToolBudgetExecutionContract{}
	if runtimeCapabilitiesReady(capabilities) {
		t.Fatal("Host without Gateway tool execution contract was accepted")
	}
}

func TestRuntimeCapabilitiesReadyAllowsDynamicPerToolDegradation(t *testing.T) {
	capabilities := readyRegistrationCapabilities()
	for index := range capabilities.Tools {
		if capabilities.Tools[index].Name == "workspace_search" {
			capabilities.Tools[index].Status = "degraded"
			capabilities.Tools[index].SchemaHash = ""
		}
	}
	if !runtimeCapabilitiesReady(capabilities) {
		t.Fatal("a Host with a valid global runtime contract and an unavailable optional tool must remain registerable")
	}
	for index := range capabilities.Tools {
		if capabilities.Tools[index].Name == "read" {
			capabilities.Tools[index].Status = "ready"
			capabilities.Tools[index].SchemaHash = ""
		}
	}
	if runtimeCapabilitiesReady(capabilities) {
		t.Fatal("a ready tool without a schema hash must be rejected")
	}
}

func TestRuntimeCapabilitiesReadyRejectsRawFilesystemTools(t *testing.T) {
	capabilities := readyRegistrationCapabilities()
	capabilities.Tools = append(capabilities.Tools, runtimepkg.ToolCapability{Name: "find", Status: "ready", SchemaHash: "find_v1"})
	if runtimeCapabilitiesReady(capabilities) {
		t.Fatal("raw filesystem tools must not be registered as Agent-facing capabilities")
	}
}

func testRegistrationAdapter(t *testing.T, backendURL, hostID string) *adapter {
	t.Helper()
	adapter := &adapter{
		backendURL: backendURL, hostServiceToken: "service-token", runtimeHostID: hostID,
		runtimeInstanceID: "static-instance", runtimeEnvironment: "test", runtimeHostEndpoint: "http://runtime-host",
		runtimeZone: "test", runtimeVersion: "runtime-v1", adapterVersion: "adapter-v1", sessionStoreID: "store-v1",
		maxActiveRuns: 2, maxProductThreadRuns: 2, maxDetachedTaskRuns: 2,
		heartbeatSigner: testRuntimeHostSigner(t),
	}
	adapter.admission = NewHostAdmissionController(2, 2, 2)
	return adapter
}

func testRuntimeHostSigner(t *testing.T) runtimepkg.RuntimeHostHeartbeatSigner {
	t.Helper()
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := runtimepkg.NewEd25519RuntimeHostHeartbeatSigner("test-registration-key", privateKey)
	if err != nil {
		t.Fatal(err)
	}
	return signer
}
