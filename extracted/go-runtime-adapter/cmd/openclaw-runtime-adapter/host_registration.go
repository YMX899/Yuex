package main

import (
	"bytes"
	"context"
	cryptorand "crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/big"
	"net/http"
	"strings"
	"time"

	runtimepkg "huahuoai/backend/source/internal/runtime"
)

const (
	defaultHostRegistrationRetryMinDelay  = time.Second
	defaultHostRegistrationRetryMaxDelay  = 30 * time.Second
	defaultHostRegistrationHeartbeatDelay = 5 * time.Second
)

// hostRegistrationRetryPolicy keeps failed Host bootstrap attempts from
// stampeding the Backend. Jitter and waiting are injected so retry behavior is
// deterministic in tests without replacing the production cancellation path.
type hostRegistrationRetryPolicy struct {
	MinDelay       time.Duration
	MaxDelay       time.Duration
	HeartbeatDelay time.Duration
	Jitter         func(base time.Duration) time.Duration
	Wait           func(ctx context.Context, delay time.Duration) bool
}

func defaultHostRegistrationRetryPolicy() hostRegistrationRetryPolicy {
	return hostRegistrationRetryPolicy{
		MinDelay:       defaultHostRegistrationRetryMinDelay,
		MaxDelay:       defaultHostRegistrationRetryMaxDelay,
		HeartbeatDelay: defaultHostRegistrationHeartbeatDelay,
		Jitter:         randomHostRegistrationJitter,
		Wait:           waitForHostRegistrationDelay,
	}
}

func (a *adapter) runHostRegistrationLoop(ctx context.Context) {
	a.runHostRegistrationLoopWithRetryPolicy(ctx, defaultHostRegistrationRetryPolicy())
}

func (a *adapter) runHostRegistrationLoopWithRetryPolicy(ctx context.Context, policy hostRegistrationRetryPolicy) {
	if a == nil || a.backendURL == "" || a.runtimeHostID == "" || a.runtimeInstanceID == "" || a.runtimeHostEndpoint == "" {
		return
	}
	if err := a.validateHostRegistrationConfiguration(); err != nil {
		return
	}
	policy = normalizeHostRegistrationRetryPolicy(policy)
	sequence := int64(0)
	registeredCapabilityHash := ""
	consecutiveFailures := 0
	recovered := !adapterProductionLike(a.runtimeEnvironment)
	registrationFailureCode := ""
	recoveryFailureDiagnostic := ""
	for {
		if ctx.Err() != nil {
			return
		}
		capabilities, err := a.loadRuntimeCapabilities(ctx)
		delay := policy.HeartbeatDelay
		if err != nil {
			code := runtimeHostRecoveryCoordinatorError(err).Error()
			if code != registrationFailureCode {
				log.Printf("runtime host capability pending code=%s", code)
				registrationFailureCode = code
			}
			consecutiveFailures++
			delay = hostRegistrationRetryDelay(policy, consecutiveFailures)
		} else {
			registeredNow := registeredCapabilityHash != capabilities.CapabilityHash
			if registeredNow && adapterProductionLike(a.runtimeEnvironment) {
				// A new Backend registration may advance Host generation. Close
				// before the register/heartbeat round-trip so no prior ready
				// permit can cross the new recovery fence.
				a.admission.HoldForRecovery()
				recovered = false
			}
			registeredCapabilityHash, sequence, err = a.reportRuntimeHostWithResult(ctx, capabilities, registeredCapabilityHash, sequence)
			if err != nil {
				code := runtimeHostRecoveryCoordinatorError(err).Error()
				if code != registrationFailureCode {
					log.Printf("runtime host registration pending code=%s", code)
					registrationFailureCode = code
				}
				if runtimeHostIdentityRejected(err) {
					log.Printf("runtime host registration closed code=%s", code)
					a.blockRuntimeHostDispatch()
					return
				}
				if runtimeHostReregistrationRequired(err) {
					recovered = false
				}
				consecutiveFailures++
				delay = hostRegistrationRetryDelay(policy, consecutiveFailures)
			} else {
				// A successful heartbeat proves the current registration is live
				// and resets retry state for any later capability outage.
				consecutiveFailures = 0
				registrationFailureCode = ""
				if !recovered {
					if err := a.RecoverHostAdmission(ctx); err != nil {
						code := runtimeHostRecoveryCoordinatorError(err).Error()
						diagnostic := runtimeHostRecoveryFailureStage(err) + ":" + code
						if diagnostic != recoveryFailureDiagnostic {
							log.Printf("runtime host recovery pending stage=%s code=%s", runtimeHostRecoveryFailureStage(err), code)
							recoveryFailureDiagnostic = diagnostic
						}
						consecutiveFailures++
						delay = hostRegistrationRetryDelay(policy, consecutiveFailures)
					} else {
						recovered = true
						if recoveryFailureDiagnostic != "" {
							log.Printf("runtime host recovery reconciled")
							recoveryFailureDiagnostic = ""
						}
					}
				}
			}
		}
		if !policy.Wait(ctx, delay) {
			return
		}
	}
}

func (a *adapter) reportRuntimeHost(ctx context.Context, capabilities runtimepkg.RuntimeCapabilities, registeredCapabilityHash string, sequence int64) (string, int64) {
	registeredCapabilityHash, sequence, _ = a.reportRuntimeHostWithResult(ctx, capabilities, registeredCapabilityHash, sequence)
	return registeredCapabilityHash, sequence
}

func (a *adapter) reportRuntimeHostWithResult(ctx context.Context, capabilities runtimepkg.RuntimeCapabilities, registeredCapabilityHash string, sequence int64) (string, int64, error) {
	if registeredCapabilityHash != capabilities.CapabilityHash {
		registeredSequence, err := a.registerRuntimeHost(ctx, capabilities)
		if err != nil {
			return registeredCapabilityHash, sequence, err
		}
		sequence = registeredSequence
	}
	sequence++
	if err := a.heartbeatRuntimeHost(ctx, capabilities.CapabilityHash, sequence); err != nil {
		if runtimeHostReregistrationRequired(err) {
			return "", sequence, err
		}
		// A transient or stale heartbeat does not invalidate a successful
		// registration. Preserve the hash so the next loop sends another
		// heartbeat with a higher sequence instead of re-registering.
		return capabilities.CapabilityHash, sequence, err
	}
	return capabilities.CapabilityHash, sequence, nil
}

func normalizeHostRegistrationRetryPolicy(policy hostRegistrationRetryPolicy) hostRegistrationRetryPolicy {
	if policy.MinDelay <= 0 {
		policy.MinDelay = defaultHostRegistrationRetryMinDelay
	}
	if policy.MaxDelay <= 0 {
		policy.MaxDelay = defaultHostRegistrationRetryMaxDelay
	}
	if policy.MaxDelay < policy.MinDelay {
		policy.MaxDelay = policy.MinDelay
	}
	if policy.HeartbeatDelay <= 0 {
		policy.HeartbeatDelay = defaultHostRegistrationHeartbeatDelay
	}
	if policy.Jitter == nil {
		policy.Jitter = randomHostRegistrationJitter
	}
	if policy.Wait == nil {
		policy.Wait = waitForHostRegistrationDelay
	}
	return policy
}

func hostRegistrationRetryDelay(policy hostRegistrationRetryPolicy, consecutiveFailures int) time.Duration {
	policy = normalizeHostRegistrationRetryPolicy(policy)
	if consecutiveFailures < 1 {
		consecutiveFailures = 1
	}

	base := policy.MinDelay
	for attempt := 1; attempt < consecutiveFailures && base < policy.MaxDelay; attempt++ {
		if base > policy.MaxDelay/2 {
			base = policy.MaxDelay
			break
		}
		base *= 2
	}
	if base > policy.MaxDelay {
		base = policy.MaxDelay
	}

	// Keep headroom for the positive production jitter once exponential backoff
	// reaches its cap. Without this, every Host would clamp to the same maximum
	// delay during a prolonged Backend outage.
	jitterBase := base
	maximumJitterBase := policy.MaxDelay - policy.MaxDelay/6
	if maximumJitterBase >= policy.MinDelay && jitterBase > maximumJitterBase {
		jitterBase = maximumJitterBase
	}
	jitter := policy.Jitter(jitterBase)
	if jitter > 0 && jitter > policy.MaxDelay-jitterBase {
		return policy.MaxDelay
	}
	if jitter < 0 && jitter < policy.MinDelay-jitterBase {
		return policy.MinDelay
	}
	delay := jitterBase + jitter
	if delay < policy.MinDelay {
		return policy.MinDelay
	}
	if delay > policy.MaxDelay {
		return policy.MaxDelay
	}
	return delay
}

func randomHostRegistrationJitter(base time.Duration) time.Duration {
	maximum := base / 5
	if maximum <= 0 {
		return 0
	}
	value, err := cryptorand.Int(cryptorand.Reader, big.NewInt(int64(maximum)+1))
	if err != nil {
		return maximum / 2
	}
	return time.Duration(value.Int64())
}

func waitForHostRegistrationDelay(ctx context.Context, delay time.Duration) bool {
	if delay <= 0 {
		select {
		case <-ctx.Done():
			return false
		default:
			return true
		}
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (a *adapter) loadRuntimeCapabilities(ctx context.Context) (runtimepkg.RuntimeCapabilities, error) {
	invoke := a.controlInvoke
	if invoke == nil {
		invoke = a.invoke
	}
	raw, _, err := invoke(ctx, "enterprise.runtime.capabilities", map[string]any{}, 5*time.Second)
	if err != nil {
		log.Printf("runtime host capability load failed stage=gateway_invoke code=%s", runtimeHostRecoveryCoordinatorError(err).Error())
		return runtimepkg.RuntimeCapabilities{}, err
	}
	// The Gateway owns tool/budget facts, while this Adapter owns the v2
	// model-start ticket verifier. Register the same projected document that
	// its public capability endpoint exposes; an older Adapter cannot execute
	// this projection and therefore cannot claim the assertion.
	projected, err := projectAdapterRuntimeCapabilities(raw)
	if err != nil {
		log.Printf("runtime host capability load failed stage=adapter_projection code=RUNTIME_TOOL_UNAVAILABLE")
		return runtimepkg.RuntimeCapabilities{}, fmt.Errorf("RUNTIME_TOOL_UNAVAILABLE")
	}
	var capabilities runtimepkg.RuntimeCapabilities
	if err := json.Unmarshal(projected, &capabilities); err != nil {
		log.Printf("runtime host capability load failed stage=decode code=RUNTIME_TOOL_UNAVAILABLE")
		return runtimepkg.RuntimeCapabilities{}, fmt.Errorf("RUNTIME_TOOL_UNAVAILABLE")
	}
	if !runtimeCapabilitiesReady(capabilities) {
		validationErr := runtimepkg.ValidateRuntimeCapabilities(capabilities)
		log.Printf("runtime host capability load failed stage=validate code=%s", runtimeHostRecoveryCoordinatorError(validationErr).Error())
		return runtimepkg.RuntimeCapabilities{}, fmt.Errorf("RUNTIME_TOOL_UNAVAILABLE")
	}
	return capabilities, nil
}

func runtimeCapabilitiesReady(capabilities runtimepkg.RuntimeCapabilities) bool {
	return capabilities.CapabilityHash != "" && runtimepkg.ValidateRuntimeCapabilities(capabilities) == nil
}

func (a *adapter) registerRuntimeHost(ctx context.Context, capabilities runtimepkg.RuntimeCapabilities) (int64, error) {
	host, err := a.registerRuntimeHostRecord(ctx, capabilities)
	if err != nil {
		return 0, err
	}
	return host.HeartbeatSequence, nil
}

func (a *adapter) registerRuntimeHostRecord(ctx context.Context, capabilities runtimepkg.RuntimeCapabilities) (runtimepkg.RuntimeHost, error) {
	if a.heartbeatSigner == nil {
		return runtimepkg.RuntimeHost{}, fmt.Errorf("RUNTIME_HOST_IDENTITY_UNAVAILABLE")
	}
	registration := runtimepkg.RuntimeHostRegistration{
		Endpoint: a.runtimeHostEndpoint, Zone: a.runtimeZone,
		RuntimeVersion: a.runtimeVersion, AdapterVersion: a.adapterVersion,
		Capabilities: capabilities.PlannerSnapshot(), SessionStoreID: a.sessionStoreID,
		MaxActiveRuns: a.maxActiveRuns, MaxProductThreadRuns: a.maxProductThreadRuns,
		MaxDetachedTaskRuns: a.maxDetachedTaskRuns,
	}
	proof, err := a.heartbeatSigner.SignRegistration(http.MethodPost, "/internal/v1/runtime-hosts/register", a.runtimeHostPrincipal(), registration, runtimepkg.RuntimeHostRegistrationProof{ObservedAt: time.Now().UTC()})
	if err != nil {
		return runtimepkg.RuntimeHost{}, fmt.Errorf("RUNTIME_HOST_IDENTITY_UNAVAILABLE")
	}
	body := map[string]any{
		"endpoint": registration.Endpoint, "zone": registration.Zone,
		"runtimeVersion": registration.RuntimeVersion, "adapterVersion": registration.AdapterVersion,
		"capabilities": registration.Capabilities, "sessionStoreId": registration.SessionStoreID,
		"maxActiveRuns": registration.MaxActiveRuns, "maxProductThreadRuns": registration.MaxProductThreadRuns,
		"maxDetachedTaskRuns": registration.MaxDetachedTaskRuns,
		"observedAt":          proof.ObservedAt, "signatureKeyId": proof.SignatureKeyID,
		"nonce": proof.Nonce, "bodySha256": proof.BodySHA256, "signature": proof.Signature,
	}
	var host runtimepkg.RuntimeHost
	if err := a.postBackendHost(ctx, "/internal/v1/runtime-hosts/register", body, &host); err != nil {
		return runtimepkg.RuntimeHost{}, err
	}
	a.setRecoveryRegisteredHost(host)
	log.Printf("runtime host registration received generation=%d recovery_revision=%d recovery_state=%s", host.InstanceGeneration, host.RecoveryRevision, host.RecoveryState)
	return host, nil
}

func (a *adapter) runtimeHostPrincipal() runtimepkg.RuntimeHostPrincipal {
	return runtimepkg.RuntimeHostPrincipal{
		RuntimeHostID: a.runtimeHostID, InstanceID: a.runtimeInstanceID, Environment: a.runtimeEnvironment,
		CertificateID: "adapter-attested",
	}
}

func (a *adapter) heartbeatRuntimeHost(ctx context.Context, capabilityHash string, sequence int64) error {
	if a.heartbeatSigner == nil {
		return fmt.Errorf("RUNTIME_HOST_IDENTITY_UNAVAILABLE")
	}
	total, productThread, detached := a.activeRunCounts()
	heartbeat := runtimepkg.RuntimeHostHeartbeat{
		Sequence: sequence, ObservedAt: time.Now().UTC(), ActiveRuns: total, ReservedRuns: 0,
		CapabilityHash: capabilityHash, SignatureKeyID: "service-token",
		SafeHealth: map[string]any{
			"gatewayConnected": true, "registrationLoop": true,
			"localAdmission": map[string]any{
				"activeRuns": total, "activeProductThreadRuns": productThread,
				"activeDetachedTaskRuns": detached,
			},
		},
	}
	var err error
	heartbeat, err = a.heartbeatSigner.SignHeartbeat(http.MethodPost, "/internal/v1/runtime-hosts/"+a.runtimeHostID+"/heartbeat", a.runtimeHostPrincipal(), heartbeat)
	if err != nil {
		return fmt.Errorf("RUNTIME_HOST_IDENTITY_UNAVAILABLE")
	}
	body := map[string]any{
		"sequence": heartbeat.Sequence, "observedAt": heartbeat.ObservedAt,
		"activeRuns": heartbeat.ActiveRuns, "reservedRuns": heartbeat.ReservedRuns,
		"capabilityHash": heartbeat.CapabilityHash, "safeHealth": heartbeat.SafeHealth,
		"signatureKeyId": heartbeat.SignatureKeyID,
	}
	body["nonce"] = heartbeat.Nonce
	body["bodySha256"] = heartbeat.BodySHA256
	body["signature"] = heartbeat.Signature
	return a.postBackendHost(ctx, "/internal/v1/runtime-hosts/"+a.runtimeHostID+"/heartbeat", body, nil)
}

func (a *adapter) postBackendHost(ctx context.Context, path string, body map[string]any, output any) error {
	return a.postBackendHostWithGeneration(ctx, path, body, output, 0)
}

func (a *adapter) postBackendHostWithGeneration(ctx context.Context, path string, body map[string]any, output any, instanceGeneration int64) error {
	raw, _ := json.Marshal(body)
	return a.requestBackendHost(ctx, http.MethodPost, path, raw, output, instanceGeneration)
}

func (a *adapter) getBackendHost(ctx context.Context, path string, instanceGeneration int64, output any) error {
	return a.requestBackendHost(ctx, http.MethodGet, path, nil, output, instanceGeneration)
}

func (a *adapter) requestBackendHost(ctx context.Context, method, path string, raw []byte, output any, instanceGeneration int64) error {
	requestCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var body io.Reader
	if len(raw) != 0 {
		body = bytes.NewReader(raw)
	}
	request, err := http.NewRequestWithContext(requestCtx, method, a.backendURL+path, body)
	if err != nil {
		return err
	}
	if method != http.MethodGet {
		request.Header.Set("Content-Type", "application/json")
	}
	if strings.TrimSpace(a.hostServiceToken) != "" {
		request.Header.Set("Authorization", "Service "+a.hostServiceToken)
	}
	request.Header.Set("X-Runtime-Host-Id", a.runtimeHostID)
	request.Header.Set("X-Runtime-Instance-Id", a.runtimeInstanceID)
	request.Header.Set("X-Runtime-Environment", a.runtimeEnvironment)
	if instanceGeneration > 0 {
		request.Header.Set("X-Runtime-Instance-Generation", fmt.Sprint(instanceGeneration))
	}
	client := a.backendHTTPClient
	if client == nil {
		if adapterProductionLike(a.runtimeEnvironment) {
			return fmt.Errorf("RUNTIME_HOST_IDENTITY_UNAVAILABLE")
		}
		client = http.DefaultClient
	}
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	responseBody, readErr := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if readErr != nil {
		return fmt.Errorf("RUNTIME_HOST_REGISTRATION_FAILED")
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return newRuntimeHostBackendError(response.StatusCode, responseBody)
	}
	if len(bytes.TrimSpace(responseBody)) == 0 {
		if output == nil {
			return nil
		}
		return fmt.Errorf("RUNTIME_HOST_REGISTRATION_FAILED")
	}
	var envelope struct {
		Success bool            `json:"success"`
		Data    json.RawMessage `json:"data"`
		Error   *struct {
			Code      string `json:"code"`
			Retryable bool   `json:"retryable"`
		} `json:"error"`
	}
	if err := json.Unmarshal(responseBody, &envelope); err != nil || !envelope.Success {
		return newRuntimeHostBackendError(response.StatusCode, responseBody)
	}
	if output != nil {
		if len(envelope.Data) == 0 || json.Unmarshal(envelope.Data, output) != nil {
			return fmt.Errorf("RUNTIME_HOST_REGISTRATION_FAILED")
		}
	}
	return nil
}

type runtimeHostBackendError struct {
	statusCode int
	code       string
	retryable  bool
}

func (e *runtimeHostBackendError) Error() string {
	if e != nil && e.code != "" {
		return e.code
	}
	return "RUNTIME_HOST_REGISTRATION_FAILED"
}

func newRuntimeHostBackendError(statusCode int, raw []byte) error {
	backendErr := &runtimeHostBackendError{statusCode: statusCode}
	var envelope struct {
		Success bool `json:"success"`
		Error   *struct {
			Code      string `json:"code"`
			Retryable bool   `json:"retryable"`
		} `json:"error"`
	}
	if json.Unmarshal(raw, &envelope) == nil && !envelope.Success && envelope.Error != nil {
		backendErr.code = strings.TrimSpace(envelope.Error.Code)
		backendErr.retryable = envelope.Error.Retryable
	}
	return backendErr
}

func runtimeHostReregistrationRequired(err error) bool {
	var backendErr *runtimeHostBackendError
	return errors.As(err, &backendErr) && backendErr.statusCode == http.StatusConflict && backendErr.code == "RUNTIME_HOST_REREGISTRATION_REQUIRED"
}

func runtimeHostIdentityRejected(err error) bool {
	var backendErr *runtimeHostBackendError
	return errors.As(err, &backendErr) && (backendErr.statusCode == http.StatusUnauthorized || backendErr.statusCode == http.StatusForbidden || backendErr.code == "RUNTIME_HOST_UNAUTHORIZED")
}

func (a *adapter) observeRuntimeResponse(method string, params map[string]any, raw []byte) {
	var response map[string]any
	if json.Unmarshal(raw, &response) != nil {
		return
	}
	runID := stringValue(response["runId"])
	if runID == "" {
		runID = stringValue(params["runId"])
		if runID == "" {
			runID = stringValue(mapValue(params["spec"])["runId"])
		}
	}
	if runID == "" {
		return
	}
	status := strings.ToLower(stringValue(response["status"]))
	if runtimeHostTerminal(status) {
		a.releaseRunPermit(runID)
		return
	}
	if method == "enterprise.runtime.submit" && !runtimeHostActive(status) {
		a.releaseRunPermit(runID)
	}
}

func (a *adapter) activeRunCount() int {
	total, _, _ := a.activeRunCounts()
	return total
}

func (a *adapter) activeRunCounts() (total, productThread, detached int) {
	if a == nil || a.admission == nil {
		return 0, 0, 0
	}
	return a.admission.Snapshot()
}

func runtimeHostTerminal(status string) bool {
	switch status {
	case "succeeded", "failed", "timeout", "aborted", "cancelled":
		return true
	default:
		return false
	}
}

func runtimeHostActive(status string) bool {
	switch status {
	case "accepted", "materializing", "running", "finalizing":
		return true
	default:
		return false
	}
}
