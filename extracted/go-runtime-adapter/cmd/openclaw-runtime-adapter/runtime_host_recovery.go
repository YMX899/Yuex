package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"

	runtimepkg "huahuoai/backend/source/internal/runtime"

	"github.com/gorilla/websocket"
)

const runtimeHostRecoveryVersion = "runtime-host-recovery.v1"

// runtimeHostRecoveryStageError adds an operator-safe failure stage without
// changing the canonical Runtime error code observed by callers.
type runtimeHostRecoveryStageError struct {
	stage string
	code  string
}

func (e *runtimeHostRecoveryStageError) Error() string { return e.code }

// gatewayRecoverySnapshotError keeps the lower-level recovery phase for
// operator logs while retaining the existing canonical Runtime error code.
// It never includes request content, ticket data, or TLS material.
type gatewayRecoverySnapshotError struct {
	stage string
	err   error
}

func (e *gatewayRecoverySnapshotError) Error() string {
	if e == nil || e.err == nil {
		return "RUNTIME_CAPACITY_UNAVAILABLE"
	}
	return e.err.Error()
}

func (e *gatewayRecoverySnapshotError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.err
}

func runtimeHostRecoveryFailure(stage string, err error) error {
	return &runtimeHostRecoveryStageError{stage: stage, code: runtimeHostRecoveryCoordinatorError(err).Error()}
}

func runtimeHostRecoveryFailureStage(err error) string {
	var staged *runtimeHostRecoveryStageError
	if errors.As(err, &staged) && staged != nil && staged.stage != "" {
		return staged.stage
	}
	return "unknown"
}

func runtimeGatewayRecoverySnapshotStage(err error) string {
	var staged *gatewayRecoverySnapshotError
	if errors.As(err, &staged) && staged != nil && staged.stage != "" {
		return staged.stage
	}
	return ""
}

func gatewayRecoverySnapshotFailure(stage string, err error) error {
	return &gatewayRecoverySnapshotError{stage: stage, err: err}
}

// hostRecoveryBackendClient and hostRecoveryGatewayClient keep the
// coordinator testable without substituting an unauthenticated transport for
// production. newAdapterFromEnv wires the Backend implementation to its
// marked mTLS HTTP client and the Gateway implementation to a separate mTLS
// WebSocket dialer.
type hostRecoveryBackendClient interface {
	Snapshot(context.Context, runtimepkg.RuntimeHostPrincipal) (runtimepkg.RuntimeHostRecoverySnapshot, error)
	Begin(context.Context, runtimepkg.RuntimeHostPrincipal, runtimepkg.RuntimeHostRecoverySnapshot, string) (runtimepkg.RuntimeHostRecoveryAttestation, error)
	Complete(context.Context, runtimepkg.RuntimeHostPrincipal, string, runtimepkg.RuntimeHostRecoverySnapshot) (runtimepkg.RuntimeHostRecoveryAttestation, error)
}

type hostRecoveryGatewayClient interface {
	Snapshot(context.Context, runtimepkg.RuntimeHostRecoverySnapshot) (runtimepkg.RuntimeHostRecoverySnapshot, error)
}

type adapterBackendRecoveryClient struct{ adapter *adapter }

func (c *adapterBackendRecoveryClient) Snapshot(ctx context.Context, principal runtimepkg.RuntimeHostPrincipal) (runtimepkg.RuntimeHostRecoverySnapshot, error) {
	if c == nil || c.adapter == nil {
		return runtimepkg.RuntimeHostRecoverySnapshot{}, fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
	}
	var snapshot runtimepkg.RuntimeHostRecoverySnapshot
	path := "/internal/v1/runtime-hosts/" + principal.RuntimeHostID + "/recovery-snapshot"
	if err := c.adapter.getBackendHost(ctx, path, c.adapter.recoveryRegisteredGeneration(), &snapshot); err != nil {
		return runtimepkg.RuntimeHostRecoverySnapshot{}, runtimeHostRecoveryCoordinatorError(err)
	}
	return snapshot, nil
}

func (c *adapterBackendRecoveryClient) Begin(ctx context.Context, principal runtimepkg.RuntimeHostPrincipal, snapshot runtimepkg.RuntimeHostRecoverySnapshot, correlationID string) (runtimepkg.RuntimeHostRecoveryAttestation, error) {
	if c == nil || c.adapter == nil {
		return runtimepkg.RuntimeHostRecoveryAttestation{}, fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
	}
	body := map[string]any{
		"runtimeHostId": snapshot.RuntimeHostID, "instanceId": snapshot.InstanceID, "environment": snapshot.Environment,
		"instanceGeneration": snapshot.InstanceGeneration, "recoveryRevision": snapshot.RecoveryRevision,
		"recoveryState": snapshot.RecoveryState, "factSetHash": snapshot.FactSetHash, "facts": snapshot.Facts,
		"correlationId": correlationID,
	}
	var attestation runtimepkg.RuntimeHostRecoveryAttestation
	path := "/internal/v1/runtime-hosts/" + principal.RuntimeHostID + "/recovery-attestations"
	if err := c.adapter.postBackendHostWithGeneration(ctx, path, body, &attestation, snapshot.InstanceGeneration); err != nil {
		return runtimepkg.RuntimeHostRecoveryAttestation{}, runtimeHostRecoveryCoordinatorError(err)
	}
	return attestation, nil
}

func (c *adapterBackendRecoveryClient) Complete(ctx context.Context, principal runtimepkg.RuntimeHostPrincipal, attestationID string, snapshot runtimepkg.RuntimeHostRecoverySnapshot) (runtimepkg.RuntimeHostRecoveryAttestation, error) {
	if c == nil || c.adapter == nil {
		return runtimepkg.RuntimeHostRecoveryAttestation{}, fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
	}
	body := map[string]any{
		"runtimeHostId": snapshot.RuntimeHostID, "instanceId": snapshot.InstanceID, "environment": snapshot.Environment,
		"instanceGeneration": snapshot.InstanceGeneration, "recoveryRevision": snapshot.RecoveryRevision,
		"recoveryState": snapshot.RecoveryState, "factSetHash": snapshot.FactSetHash,
	}
	var attestation runtimepkg.RuntimeHostRecoveryAttestation
	path := "/internal/v1/runtime-hosts/" + principal.RuntimeHostID + "/recovery-attestations/" + attestationID + "/complete"
	if err := c.adapter.postBackendHostWithGeneration(ctx, path, body, &attestation, snapshot.InstanceGeneration); err != nil {
		return runtimepkg.RuntimeHostRecoveryAttestation{}, runtimeHostRecoveryCoordinatorError(err)
	}
	return attestation, nil
}

type mtlsGatewayRecoverySnapshotClient struct {
	endpoint string
	dialer   *websocket.Dialer
}

func newMTLSGatewayRecoverySnapshotClient(endpoint string, config runtimepkg.RuntimeHostMTLSConfig) (hostRecoveryGatewayClient, error) {
	if !adapterGatewayRecoveryWSSURL(endpoint) {
		return nil, fmt.Errorf("RUNTIME_HOST_IDENTITY_UNAVAILABLE")
	}
	tlsConfig, err := runtimepkg.NewRuntimeHostMTLSClientTLSConfig(config)
	if err != nil {
		return nil, err
	}
	dialer := *websocket.DefaultDialer
	dialer.TLSClientConfig = tlsConfig
	dialer.HandshakeTimeout = 5 * time.Second
	return &mtlsGatewayRecoverySnapshotClient{endpoint: strings.TrimRight(endpoint, "/"), dialer: &dialer}, nil
}

func (c *mtlsGatewayRecoverySnapshotClient) Snapshot(ctx context.Context, identity runtimepkg.RuntimeHostRecoverySnapshot) (runtimepkg.RuntimeHostRecoverySnapshot, error) {
	if c == nil || c.dialer == nil || strings.TrimSpace(c.endpoint) == "" {
		return runtimepkg.RuntimeHostRecoverySnapshot{}, fmt.Errorf("RUNTIME_PERMISSION_DENIED")
	}
	requestCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	connection, _, err := c.dialer.DialContext(requestCtx, c.endpoint+"/enterprise-runtime/recovery", nil)
	if err != nil {
		return runtimepkg.RuntimeHostRecoverySnapshot{}, gatewayRecoverySnapshotFailure("dial", runtimeHostRecoveryCoordinatorError(err))
	}
	defer connection.Close()
	requestID := gatewayRequestID()
	if err := connection.WriteJSON(map[string]any{
		"id": requestID, "method": "enterprise.runtime.recovery.snapshot",
		"params": map[string]any{
			"runtimeHostId":      identity.RuntimeHostID,
			"instanceGeneration": identity.InstanceGeneration,
			"recoveryRevision":   identity.RecoveryRevision,
		},
	}); err != nil {
		return runtimepkg.RuntimeHostRecoverySnapshot{}, gatewayRecoverySnapshotFailure("write", runtimeHostRecoveryCoordinatorError(err))
	}
	var envelope struct {
		ID      string          `json:"id"`
		OK      bool            `json:"ok"`
		Payload json.RawMessage `json:"payload"`
		Error   struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := connection.ReadJSON(&envelope); err != nil {
		return runtimepkg.RuntimeHostRecoverySnapshot{}, gatewayRecoverySnapshotFailure("read", runtimeHostRecoveryCoordinatorError(err))
	}
	if envelope.ID != requestID || !envelope.OK {
		if strings.TrimSpace(envelope.Error.Code) != "" {
			return runtimepkg.RuntimeHostRecoverySnapshot{}, gatewayRecoverySnapshotFailure("response", runtimeHostRecoveryCoordinatorError(fmt.Errorf("%s", envelope.Error.Code)))
		}
		return runtimepkg.RuntimeHostRecoverySnapshot{}, gatewayRecoverySnapshotFailure("response", fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE"))
	}
	var response struct {
		Version string `json:"version"`
		runtimepkg.RuntimeHostRecoverySnapshot
	}
	if json.Unmarshal(envelope.Payload, &response) != nil || response.Version != runtimeHostRecoveryVersion {
		return runtimepkg.RuntimeHostRecoverySnapshot{}, gatewayRecoverySnapshotFailure("decode", fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE"))
	}
	return response.RuntimeHostRecoverySnapshot, nil
}

// RecoverHostAdmission is the only path that can open a production Host after
// process restart. Every error leaves the controller closed. It compares both
// durable sources before staging permits, then completes the Backend CAS and
// only then calls MarkReadyAfterRecovery.
func (a *adapter) RecoverHostAdmission(ctx context.Context) (err error) {
	if a == nil || a.admission == nil || !adapterProductionLike(a.runtimeEnvironment) {
		return fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
	}
	a.admission.HoldForRecovery()
	principal, err := a.recoveryPrincipal()
	if err != nil {
		return runtimeHostRecoveryFailure("principal", err)
	}
	if a.recoveryBackend == nil || a.recoveryGateway == nil {
		if a.recoveryGatewayErr != nil {
			return runtimeHostRecoveryFailure("gateway_configuration", a.recoveryGatewayErr)
		}
		return runtimeHostRecoveryFailure("client_configuration", fmt.Errorf("RUNTIME_PERMISSION_DENIED"))
	}
	backendSnapshot, err := a.recoveryBackend.Snapshot(ctx, principal)
	if err != nil {
		return runtimeHostRecoveryFailure("backend_snapshot", err)
	}
	backendSnapshot, err = runtimepkg.NormalizeRuntimeHostRecoverySnapshot(principal, backendSnapshot)
	if err != nil {
		return runtimeHostRecoveryFailure("backend_snapshot", err)
	}
	gatewaySnapshot, err := a.recoveryGateway.Snapshot(ctx, backendSnapshot)
	if err != nil {
		stage := "gateway_snapshot"
		if detail := runtimeGatewayRecoverySnapshotStage(err); detail != "" {
			stage += "." + detail
		}
		return runtimeHostRecoveryFailure(stage, err)
	}
	gatewaySnapshot, err = runtimepkg.NormalizeRuntimeHostRecoverySnapshot(principal, gatewaySnapshot)
	if err != nil {
		return runtimeHostRecoveryFailure("gateway_snapshot", err)
	}
	if err := runtimepkg.CompareRuntimeHostRecoverySnapshots(gatewaySnapshot, backendSnapshot); err != nil {
		return runtimeHostRecoveryFailure("snapshot_compare", err)
	}
	attestation, err := a.recoveryBackend.Begin(ctx, principal, backendSnapshot, recoveryCorrelationID(backendSnapshot))
	if err != nil || attestation.State != "prepared" || attestation.AttestationID == "" ||
		attestation.RuntimeHostID != principal.RuntimeHostID || attestation.InstanceID != principal.InstanceID ||
		attestation.InstanceGeneration != backendSnapshot.InstanceGeneration || attestation.RecoveryRevision != backendSnapshot.RecoveryRevision ||
		attestation.FactSetHash != backendSnapshot.FactSetHash {
		if err != nil {
			return runtimeHostRecoveryFailure("attestation_begin", err)
		}
		return runtimeHostRecoveryFailure("attestation_begin", fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE"))
	}
	if err := a.admission.StageRecoveryPermits(backendSnapshot.Facts); err != nil {
		return runtimeHostRecoveryFailure("permit_stage", err)
	}
	staged := true
	defer func() {
		if staged {
			a.admission.ClearRecoveryPermits()
		}
	}()
	completed, err := a.recoveryBackend.Complete(ctx, principal, attestation.AttestationID, backendSnapshot)
	if err != nil {
		return runtimeHostRecoveryFailure("attestation_complete", err)
	}
	if completed.AttestationID != attestation.AttestationID || completed.State != "completed" || completed.RuntimeHostID != principal.RuntimeHostID ||
		completed.InstanceID != principal.InstanceID || completed.InstanceGeneration != backendSnapshot.InstanceGeneration ||
		completed.RecoveryRevision != backendSnapshot.RecoveryRevision || completed.FactSetHash != backendSnapshot.FactSetHash {
		return runtimeHostRecoveryFailure("attestation_complete", fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE"))
	}
	a.admission.MarkReadyAfterRecovery()
	staged = false
	return nil
}

func (a *adapter) setRecoveryRegisteredHost(host runtimepkg.RuntimeHost) {
	if a == nil {
		return
	}
	a.recoveryHostMu.Lock()
	a.recoveryRegisteredHost = host
	a.recoveryHostMu.Unlock()
}

func (a *adapter) recoveryPrincipal() (runtimepkg.RuntimeHostPrincipal, error) {
	if a == nil {
		return runtimepkg.RuntimeHostPrincipal{}, fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
	}
	a.recoveryHostMu.Lock()
	host := a.recoveryRegisteredHost
	a.recoveryHostMu.Unlock()
	if host.RuntimeHostID != a.runtimeHostID || host.InstanceID != a.runtimeInstanceID || host.Environment != a.runtimeEnvironment ||
		host.InstanceGeneration < 1 || host.RecoveryRevision < 1 || host.RecoveryState != "pending" {
		return runtimepkg.RuntimeHostPrincipal{}, fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
	}
	return runtimepkg.RuntimeHostPrincipal{RuntimeHostID: host.RuntimeHostID, InstanceID: host.InstanceID, Environment: host.Environment}, nil
}

func (a *adapter) recoveryRegisteredGeneration() int64 {
	if a == nil {
		return 0
	}
	a.recoveryHostMu.Lock()
	defer a.recoveryHostMu.Unlock()
	return a.recoveryRegisteredHost.InstanceGeneration
}

func recoveryCorrelationID(snapshot runtimepkg.RuntimeHostRecoverySnapshot) string {
	return "adapter-recovery:" + snapshot.RuntimeHostID + ":" + snapshot.InstanceID + ":" + fmt.Sprint(snapshot.InstanceGeneration) + ":" + fmt.Sprint(snapshot.RecoveryRevision)
}

func runtimeHostRecoveryCoordinatorError(err error) error {
	if err == nil {
		return nil
	}
	for _, code := range []string{"RUNTIME_PERMISSION_DENIED", "RUNTIME_EVENT_GAP", "RUNTIME_HOST_REREGISTRATION_REQUIRED", "RUNTIME_HOST_UNAUTHORIZED", "RUNTIME_STORAGE_UNAVAILABLE", "RUNTIME_CAPACITY_UNAVAILABLE", "RUNTIME_HOST_IDENTITY_UNAVAILABLE"} {
		if strings.Contains(err.Error(), code) {
			return fmt.Errorf("%s", code)
		}
	}
	return fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
}

func gatewayRecoverySettingsConfigured(a *adapter) bool {
	return a != nil && strings.TrimSpace(a.gatewayRecoveryURL) != "" && strings.TrimSpace(a.gatewayRecoveryTrustRef) != "" &&
		strings.TrimSpace(a.gatewayRecoveryCertRef) != "" && strings.TrimSpace(a.gatewayRecoveryKeyRef) != ""
}

func gatewayRecoverySettingsPartiallyConfigured(a *adapter) bool {
	if a == nil {
		return false
	}
	return strings.TrimSpace(a.gatewayRecoveryURL) != "" || strings.TrimSpace(a.gatewayRecoveryTrustRef) != "" ||
		strings.TrimSpace(a.gatewayRecoveryCertRef) != "" || strings.TrimSpace(a.gatewayRecoveryKeyRef) != ""
}

func adapterGatewayRecoveryWSSURL(value string) bool {
	parsed, err := url.Parse(strings.TrimSpace(value))
	if err != nil || !strings.EqualFold(parsed.Scheme, "wss") || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Path != "" && parsed.Path != "/") {
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
