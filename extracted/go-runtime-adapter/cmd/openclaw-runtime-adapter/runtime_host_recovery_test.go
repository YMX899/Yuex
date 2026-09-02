package main

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"huahuoai/backend/source/internal/config"
	"huahuoai/backend/source/internal/persistence"
	runtimepkg "huahuoai/backend/source/internal/runtime"

	"github.com/gorilla/websocket"
)

type recoveryBackendFixture struct {
	snapshot      runtimepkg.RuntimeHostRecoverySnapshot
	beginErr      error
	completeErr   error
	beginCalls    int
	completeCalls int
}

func (f *recoveryBackendFixture) Snapshot(_ context.Context, _ runtimepkg.RuntimeHostPrincipal) (runtimepkg.RuntimeHostRecoverySnapshot, error) {
	return f.snapshot, nil
}

func (f *recoveryBackendFixture) Begin(_ context.Context, principal runtimepkg.RuntimeHostPrincipal, snapshot runtimepkg.RuntimeHostRecoverySnapshot, _ string) (runtimepkg.RuntimeHostRecoveryAttestation, error) {
	f.beginCalls++
	if f.beginErr != nil {
		return runtimepkg.RuntimeHostRecoveryAttestation{}, f.beginErr
	}
	return runtimepkg.RuntimeHostRecoveryAttestation{
		AttestationID: "attestation-recovery-1", RuntimeHostID: principal.RuntimeHostID, InstanceID: principal.InstanceID,
		InstanceGeneration: snapshot.InstanceGeneration, RecoveryRevision: snapshot.RecoveryRevision,
		FactSetHash: snapshot.FactSetHash, State: "prepared",
	}, nil
}

func (f *recoveryBackendFixture) Complete(_ context.Context, principal runtimepkg.RuntimeHostPrincipal, id string, snapshot runtimepkg.RuntimeHostRecoverySnapshot) (runtimepkg.RuntimeHostRecoveryAttestation, error) {
	f.completeCalls++
	if f.completeErr != nil {
		return runtimepkg.RuntimeHostRecoveryAttestation{}, f.completeErr
	}
	return runtimepkg.RuntimeHostRecoveryAttestation{
		AttestationID: id, RuntimeHostID: principal.RuntimeHostID, InstanceID: principal.InstanceID,
		InstanceGeneration: snapshot.InstanceGeneration, RecoveryRevision: snapshot.RecoveryRevision,
		FactSetHash: snapshot.FactSetHash, State: "completed",
	}, nil
}

type recoveryGatewayFixture struct {
	snapshot runtimepkg.RuntimeHostRecoverySnapshot
	err      error
}

// postgresRecoveryBackendClient exercises the production repository CAS while
// the test's Gateway assertion deliberately echoes the exact durable snapshot.
// It is not a substitute for the deployed mTLS Gateway proof.
type postgresRecoveryBackendClient struct {
	repository *runtimepkg.RuntimeHostRepository
}

func (c postgresRecoveryBackendClient) Snapshot(ctx context.Context, principal runtimepkg.RuntimeHostPrincipal) (runtimepkg.RuntimeHostRecoverySnapshot, error) {
	return c.repository.GetHostRecoverySnapshot(ctx, principal)
}

func (c postgresRecoveryBackendClient) Begin(ctx context.Context, principal runtimepkg.RuntimeHostPrincipal, snapshot runtimepkg.RuntimeHostRecoverySnapshot, correlationID string) (runtimepkg.RuntimeHostRecoveryAttestation, error) {
	return c.repository.BeginHostRecoveryAttestationWithCorrelation(ctx, principal, snapshot, correlationID)
}

func (c postgresRecoveryBackendClient) Complete(ctx context.Context, principal runtimepkg.RuntimeHostPrincipal, attestationID string, _ runtimepkg.RuntimeHostRecoverySnapshot) (runtimepkg.RuntimeHostRecoveryAttestation, error) {
	return c.repository.CompleteHostRecoveryAttestation(ctx, principal, attestationID)
}

func (f recoveryGatewayFixture) Snapshot(_ context.Context, _ runtimepkg.RuntimeHostRecoverySnapshot) (runtimepkg.RuntimeHostRecoverySnapshot, error) {
	if f.err != nil {
		return runtimepkg.RuntimeHostRecoverySnapshot{}, f.err
	}
	return f.snapshot, nil
}

func TestRecoverHostAdmissionStagesOnlyExactAttestedFacts(t *testing.T) {
	snapshot := adapterRecoverySnapshot(t, nil)
	backend := &recoveryBackendFixture{snapshot: snapshot}
	adapter := recoveryAdapterFixture(snapshot, backend, recoveryGatewayFixture{snapshot: snapshot})

	if err := adapter.RecoverHostAdmission(context.Background()); err != nil {
		t.Fatalf("RecoverHostAdmission: %v", err)
	}
	if backend.beginCalls != 1 || backend.completeCalls != 1 {
		t.Fatalf("attestation calls begin=%d complete=%d", backend.beginCalls, backend.completeCalls)
	}
	if total, product, detached := adapter.admission.Snapshot(); total != 1 || product != 1 || detached != 0 {
		t.Fatalf("recovered permits total=%d product=%d detached=%d", total, product, detached)
	}
	permit, err := adapter.acquireRunPermit("run-after-recovery", "detached_task")
	if err != nil || !permit.Acquired {
		t.Fatalf("admission was not opened after complete CAS: permit=%+v err=%v", permit, err)
	}
	adapter.releaseRunPermit(permit.RunID)
}

func TestRecoverHostAdmissionKeepsClosedOnGatewayDenyMismatchAndCASFailure(t *testing.T) {
	snapshot := adapterRecoverySnapshot(t, nil)
	mismatched := adapterRecoverySnapshot(t, func(fact *runtimepkg.RuntimeHostRecoveryFact) { fact.ExecutionScope = "detached_task" })
	for _, test := range []struct {
		name        string
		gateway     recoveryGatewayFixture
		completeErr error
		wantCode    string
	}{
		{name: "gateway permission denied", gateway: recoveryGatewayFixture{err: fmt.Errorf("RUNTIME_PERMISSION_DENIED")}, wantCode: "RUNTIME_PERMISSION_DENIED"},
		{name: "gateway fact mismatch", gateway: recoveryGatewayFixture{snapshot: mismatched}, wantCode: "RUNTIME_CAPACITY_UNAVAILABLE"},
		{name: "complete CAS failed", gateway: recoveryGatewayFixture{snapshot: snapshot}, completeErr: fmt.Errorf("RUNTIME_EVENT_GAP"), wantCode: "RUNTIME_EVENT_GAP"},
	} {
		t.Run(test.name, func(t *testing.T) {
			backend := &recoveryBackendFixture{snapshot: snapshot, completeErr: test.completeErr}
			adapter := recoveryAdapterFixture(snapshot, backend, test.gateway)
			err := adapter.RecoverHostAdmission(context.Background())
			if err == nil || err.Error() != test.wantCode {
				t.Fatalf("RecoverHostAdmission error=%v want %s", err, test.wantCode)
			}
			if _, err := adapter.acquireRunPermit("run-must-stay-closed", "product_thread"); err == nil || err.Error() != "RUNTIME_CAPACITY_UNAVAILABLE" {
				t.Fatalf("failed recovery reopened admission: %v", err)
			}
			if total, _, _ := adapter.admission.Snapshot(); total != 0 {
				t.Fatalf("failed recovery retained staged permits=%d", total)
			}
			if test.name == "gateway permission denied" && (backend.beginCalls != 0 || backend.completeCalls != 0) {
				t.Fatalf("Gateway denial reached Backend attestation: begin=%d complete=%d", backend.beginCalls, backend.completeCalls)
			}
			if test.name == "gateway fact mismatch" && (backend.beginCalls != 0 || backend.completeCalls != 0) {
				t.Fatalf("Gateway mismatch reached Backend attestation: begin=%d complete=%d", backend.beginCalls, backend.completeCalls)
			}
		})
	}
}

func TestRuntimeGatewayRecoverySnapshotStagePreservesCanonicalCode(t *testing.T) {
	err := gatewayRecoverySnapshotFailure("read", fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE"))
	if got := runtimeGatewayRecoverySnapshotStage(err); got != "read" {
		t.Fatalf("gateway recovery stage=%q want read", got)
	}
	wrapped := runtimeHostRecoveryFailure("gateway_snapshot."+runtimeGatewayRecoverySnapshotStage(err), err)
	if got := runtimeHostRecoveryFailureStage(wrapped); got != "gateway_snapshot.read" {
		t.Fatalf("recovery stage=%q want gateway_snapshot.read", got)
	}
	if got := runtimeHostRecoveryCoordinatorError(wrapped).Error(); got != "RUNTIME_CAPACITY_UNAVAILABLE" {
		t.Fatalf("canonical code=%q", got)
	}
}

// This probe is excluded from ordinary test runs. Production operators opt in
// with a one-shot environment flag after sourcing the existing protected
// Adapter files; it logs only fact counts and canonical error codes.
func TestLiveRecoverySnapshotProbes(t *testing.T) {
	if os.Getenv("HUAHUO_LIVE_RUNTIME_RECOVERY_PROBE") != "1" {
		t.Skip("live recovery probe is opt-in")
	}
	generation, err := strconv.ParseInt(os.Getenv("HUAHUO_LIVE_RUNTIME_RECOVERY_GENERATION"), 10, 64)
	if err != nil || generation < 1 {
		t.Fatal("live recovery generation is invalid")
	}
	revision, err := strconv.ParseInt(os.Getenv("HUAHUO_LIVE_RUNTIME_RECOVERY_REVISION"), 10, 64)
	if err != nil || revision < 1 {
		t.Fatal("live recovery revision is invalid")
	}
	adapter := newAdapterFromEnv()
	if adapter.recoveryBackend == nil || adapter.recoveryGateway == nil {
		t.Fatal("live recovery clients are not configured")
	}
	adapter.setRecoveryRegisteredHost(runtimepkg.RuntimeHost{
		RuntimeHostID: adapter.runtimeHostID, InstanceID: adapter.runtimeInstanceID, Environment: adapter.runtimeEnvironment,
		InstanceGeneration: generation, RecoveryRevision: revision, RecoveryState: "pending",
	})
	principal, err := adapter.recoveryPrincipal()
	if err != nil {
		t.Fatalf("live recovery principal: %v", err)
	}
	backendSnapshot, err := adapter.recoveryBackend.Snapshot(context.Background(), principal)
	if err != nil {
		t.Fatalf("live backend recovery snapshot: %s", runtimeHostRecoveryCoordinatorError(err))
	}
	gatewaySnapshot, err := adapter.recoveryGateway.Snapshot(context.Background(), backendSnapshot)
	if err != nil {
		t.Fatalf("live gateway recovery snapshot: %s", runtimeHostRecoveryCoordinatorError(err))
	}
	if err := runtimepkg.CompareRuntimeHostRecoverySnapshots(gatewaySnapshot, backendSnapshot); err != nil {
		expectedEmptyFactHash := adapterRecoveryHash(`{"version":"runtime-host-recovery.v1","facts":[]}`)
		t.Fatalf("live recovery snapshot comparison: %s identity_equal=%t generation_equal=%t revision_equal=%t state_equal=%t backend_state=%s gateway_state=%s hash_equal=%t backend_hash_expected=%t gateway_hash_expected=%t backend_facts=%d gateway_facts=%d",
			runtimeHostRecoveryCoordinatorError(err),
			gatewaySnapshot.RuntimeHostID == backendSnapshot.RuntimeHostID && gatewaySnapshot.InstanceID == backendSnapshot.InstanceID && gatewaySnapshot.Environment == backendSnapshot.Environment,
			gatewaySnapshot.InstanceGeneration == backendSnapshot.InstanceGeneration,
			gatewaySnapshot.RecoveryRevision == backendSnapshot.RecoveryRevision,
			gatewaySnapshot.RecoveryState == backendSnapshot.RecoveryState,
			backendSnapshot.RecoveryState,
			gatewaySnapshot.RecoveryState,
			gatewaySnapshot.FactSetHash == backendSnapshot.FactSetHash,
			backendSnapshot.FactSetHash == expectedEmptyFactHash,
			gatewaySnapshot.FactSetHash == expectedEmptyFactHash,
			len(backendSnapshot.Facts), len(gatewaySnapshot.Facts),
		)
	}
	t.Logf("live recovery snapshots matched facts=%d", len(backendSnapshot.Facts))
}

func TestRecoverHostAdmissionRequiresConfiguredGatewayPrincipalBridge(t *testing.T) {
	snapshot := adapterRecoverySnapshot(t, nil)
	backend := &recoveryBackendFixture{snapshot: snapshot}
	adapter := recoveryAdapterFixture(snapshot, backend, nil)
	if err := adapter.RecoverHostAdmission(context.Background()); err == nil || err.Error() != "RUNTIME_PERMISSION_DENIED" {
		t.Fatalf("missing Gateway mTLS principal bridge error=%v", err)
	}
	if _, err := adapter.acquireRunPermit("run-unverified-gateway", "product_thread"); err == nil || err.Error() != "RUNTIME_CAPACITY_UNAVAILABLE" {
		t.Fatalf("unverified Gateway opened admission: %v", err)
	}
}

func TestMTLSGatewayRecoverySnapshotUsesDedicatedOneShotProtocol(t *testing.T) {
	snapshot := adapterRecoverySnapshot(t, nil)
	upgrader := websocket.Upgrader{}
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/enterprise-runtime/recovery" {
			http.NotFound(writer, request)
			return
		}
		connection, err := upgrader.Upgrade(writer, request, nil)
		if err != nil {
			return
		}
		defer connection.Close()
		var frame struct {
			ID     string         `json:"id"`
			Method string         `json:"method"`
			Params map[string]any `json:"params"`
		}
		if err := connection.ReadJSON(&frame); err != nil {
			t.Errorf("ReadJSON: %v", err)
			return
		}
		if frame.Method != "enterprise.runtime.recovery.snapshot" || len(frame.Params) != 3 ||
			frame.Params["runtimeHostId"] != snapshot.RuntimeHostID || frame.Params["instanceGeneration"] != float64(snapshot.InstanceGeneration) ||
			frame.Params["recoveryRevision"] != float64(snapshot.RecoveryRevision) {
			t.Errorf("unexpected recovery frame: %+v", frame)
			return
		}
		payload := struct {
			Version string `json:"version"`
			runtimepkg.RuntimeHostRecoverySnapshot
		}{Version: runtimeHostRecoveryVersion, RuntimeHostRecoverySnapshot: snapshot}
		if err := connection.WriteJSON(map[string]any{"id": frame.ID, "ok": true, "payload": payload}); err != nil {
			t.Errorf("WriteJSON: %v", err)
		}
	}))
	defer server.Close()
	dialer := *websocket.DefaultDialer
	dialer.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} // httptest owns this ephemeral certificate.
	client := &mtlsGatewayRecoverySnapshotClient{
		endpoint: "wss" + strings.TrimPrefix(server.URL, "https"),
		dialer:   &dialer,
	}
	actual, err := client.Snapshot(context.Background(), snapshot)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if actual.RuntimeHostID != snapshot.RuntimeHostID || actual.InstanceGeneration != snapshot.InstanceGeneration || actual.RecoveryRevision != snapshot.RecoveryRevision || actual.FactSetHash != snapshot.FactSetHash {
		t.Fatalf("unexpected recovery snapshot: %+v", actual)
	}
}

func TestStageRecoveryPermitsRejectsDuplicateRunAndCapacityOverflow(t *testing.T) {
	controller := NewHostAdmissionController(1, 1, 1)
	controller.HoldForRecovery()
	fact := adapterRecoverySnapshot(t, nil).Facts[0]
	if err := controller.StageRecoveryPermits([]runtimepkg.RuntimeHostRecoveryFact{fact, fact}); err == nil || err.Error() != "RUNTIME_CAPACITY_UNAVAILABLE" {
		t.Fatalf("duplicate recovered run error=%v", err)
	}
	second := fact
	second.RunID = "run-recovery-2"
	second.ReservationID = "reservation-recovery-2"
	if err := controller.StageRecoveryPermits([]runtimepkg.RuntimeHostRecoveryFact{fact, second}); err == nil || err.Error() != "RUNTIME_CAPACITY_UNAVAILABLE" {
		t.Fatalf("capacity overflow error=%v", err)
	}
}

// TestRecoverHostAdmissionPostgresStagesNonemptyPermitsWhenConfigured proves
// the nonempty recovery path against PostgreSQL without touching a deployed
// Host. It creates one active fact in each execution scope, re-registers the
// same Host instance into pending recovery, and verifies the coordinator only
// opens admission after the repository CAS commits the exact durable facts.
func TestRecoverHostAdmissionPostgresStagesNonemptyPermitsWhenConfigured(t *testing.T) {
	if os.Getenv("HUAHUO_RUNTIME_V1_LIVE_POSTGRES") != "1" {
		t.Skip("HUAHUO_RUNTIME_V1_LIVE_POSTGRES=1 is required")
	}
	dsn := strings.TrimSpace(os.Getenv("HUAHUO_TEST_DATABASE_DSN"))
	if dsn == "" {
		t.Skip("HUAHUO_TEST_DATABASE_DSN is required")
	}
	database, err := persistence.NewDatabase(config.Settings{
		DatabaseDSN: dsn,
		Database: config.DatabaseSettings{
			PoolMinConns: 1, PoolMaxConns: 4, ConnectTimeoutSeconds: 5, StatementTimeoutSeconds: 10,
		},
	})
	if err != nil || database.Disabled || database.Pool == nil {
		if database != nil {
			database.Close()
		}
		t.Fatalf("open live PostgreSQL: %v", err)
	}
	defer database.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	suffix := strconv.FormatInt(time.Now().UTC().UnixNano(), 10)
	hostID := "runtime-v1-permit-recovery-host-" + suffix
	instanceID := "runtime-v1-permit-recovery-instance-" + suffix
	runProduct := "runtime-v1-permit-recovery-product-" + suffix
	runDetached := "runtime-v1-permit-recovery-detached-" + suffix
	reservationProduct := "runtime-v1-permit-recovery-reservation-product-" + suffix
	reservationDetached := "runtime-v1-permit-recovery-reservation-detached-" + suffix
	dispatchProduct := "runtime-v1-permit-recovery-dispatch-product-" + suffix
	dispatchDetached := "runtime-v1-permit-recovery-dispatch-detached-" + suffix
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = database.Pool.Exec(cleanupCtx, "delete from runtime_host_recovery_attestations where runtime_host_id=$1", hostID)
		_, _ = database.Pool.Exec(cleanupCtx, "delete from runtime_run_dispatches where runtime_host_id=$1", hostID)
		_, _ = database.Pool.Exec(cleanupCtx, "delete from runtime_slot_reservations where runtime_host_id=$1", hostID)
		_, _ = database.Pool.Exec(cleanupCtx, "delete from runtime_hosts where runtime_host_id=$1", hostID)
		_, _ = database.Pool.Exec(cleanupCtx, "delete from runtime_capacity_reservations where run_id in ($1,$2)", runProduct, runDetached)
	}()

	repository := runtimepkg.NewRuntimeHostRepository(database)
	identity := runtimepkg.RuntimeHostIdentity{RuntimeHostID: hostID, InstanceID: instanceID, Environment: "production"}
	principal := runtimepkg.RuntimeHostPrincipal{RuntimeHostID: hostID, InstanceID: instanceID, Environment: "production"}
	capabilities := runtimepkg.RuntimeCapabilitySnapshot{
		CapabilityHash: "runtime-v1-permit-recovery-capability-" + suffix,
		Tools: []runtimepkg.ToolCapability{
			runtimepkg.CanonicalAgentFacingToolCapability("read", "ready"),
		},
		MaxToolCallsSupported: 200, SupportsPerRunBudget: true, SupportsBudgetWarning: true, SupportsForcedAbort: true,
		BudgetExecution: runtimepkg.DefaultRuntimeToolBudgetExecutionContract(),
		SubmitBinding: runtimepkg.RuntimeSubmitBindingCapability{
			Version: runtimepkg.RuntimeSubmitBindingV2, ProductSessionHash: true,
		},
	}
	registration := runtimepkg.RuntimeHostRegistration{
		Endpoint: "https://runtime-v1-permit-recovery.invalid", RuntimeVersion: "runtime-v1-live", AdapterVersion: "adapter-v1-live",
		Capabilities: capabilities, MaxActiveRuns: 2, MaxProductThreadRuns: 1, MaxDetachedTaskRuns: 1,
	}
	initialHost, err := repository.RegisterHost(ctx, identity, registration)
	if err != nil {
		t.Fatalf("register production-like Host: %v", err)
	}
	if initialHost.Status != "registering" || initialHost.RecoveryState != "pending" {
		t.Fatalf("initial Host recovery state=%s/%s", initialHost.Status, initialHost.RecoveryState)
	}
	emptySnapshot, err := repository.GetHostRecoverySnapshot(ctx, principal)
	if err != nil || len(emptySnapshot.Facts) != 0 {
		t.Fatalf("read initial empty snapshot facts=%d err=%v", len(emptySnapshot.Facts), err)
	}
	emptyAttestation, err := repository.BeginHostRecoveryAttestationWithCorrelation(ctx, principal, emptySnapshot, "runtime-v1-permit-initial:"+suffix)
	if err != nil {
		t.Fatalf("begin initial empty recovery: %v", err)
	}
	if _, err := repository.CompleteHostRecoveryAttestation(ctx, principal, emptyAttestation.AttestationID); err != nil {
		t.Fatalf("complete initial empty recovery: %v", err)
	}
	if _, err := repository.HeartbeatHost(ctx, identity, runtimepkg.RuntimeHostHeartbeat{
		Sequence: 1, ObservedAt: time.Now().UTC(), CapabilityHash: capabilities.CapabilityHash, SignatureKeyID: "runtime-v1-permit-recovery",
	}); err != nil {
		t.Fatalf("heartbeat ready Host: %v", err)
	}

	capacity := runtimepkg.NewRuntimeCapacityAdmissionService(database)
	reserveCapacity := func(runID, label string) runtimepkg.RuntimeCapacityReservation {
		dimension := func(name string) runtimepkg.RuntimeCapacityDimension {
			return runtimepkg.RuntimeCapacityDimension{Key: "runtime-v1-permit-recovery-" + suffix + "-" + label + "-" + name, Limit: 1, Requested: 1, Version: 1}
		}
		reservation, reserveErr := capacity.Reserve(ctx, runtimepkg.RuntimeCapacityCommand{
			RunID: runID, SnapshotVersion: 1, TTL: time.Minute,
			Dimensions: runtimepkg.RuntimeCapacityDimensions{
				Model: dimension("model"), AuthPool: dimension("auth"), Tool: dimension("tool"), Tenant: dimension("tenant"), User: dimension("user"),
			},
		})
		if reserveErr != nil {
			t.Fatalf("reserve %s capacity: %v", label, reserveErr)
		}
		return reservation
	}
	createFact := func(runID, reservationID, dispatchID, scope, label string, fence int64) {
		capacityReservation := reserveCapacity(runID, label)
		reservation, _, reserveErr := repository.TryReserveSlot(ctx, runtimepkg.AtomicReservationCommand{
			ReservationID: reservationID, RunID: runID, AffinityRuntimeHostID: hostID, OwnerInstanceID: "runtime-v1-permit-recovery-owner-" + label,
			ExecutionScope: scope, CapabilityHash: capabilities.CapabilityHash, RequiredTools: []string{"read"},
			LeaseTokenHash: "sha256:" + strings.Repeat(label[:1], 64), FencingToken: fence,
			ExpiresAt: time.Now().UTC().Add(time.Minute), HeartbeatAfter: time.Now().UTC().Add(-time.Minute),
		})
		if reserveErr != nil {
			t.Fatalf("reserve %s Host Slot: %v", label, reserveErr)
		}
		if _, createErr := repository.CreateDispatch(ctx, runtimepkg.RuntimeDispatch{
			DispatchID: dispatchID, RunID: runID, ReservationID: reservation.ReservationID, RuntimeHostID: hostID,
			CapacityReservationID: capacityReservation.ReservationID, CapacityReservedVersion: capacityReservation.Version,
			DispatchAttempt: 1, PlanVersion: 1, FencingToken: reservation.FencingToken,
			RunTicketJTIHash: adapterRecoveryHash("runtime-v1-permit-recovery-jti-" + label), TicketExpiresAt: time.Now().UTC().Add(time.Minute),
			InputManifestHash: adapterRecoveryHash("runtime-v1-permit-recovery-manifest-" + label),
			OwnerInstanceID:   reservation.OwnerInstanceID, LeaseTokenHash: reservation.LeaseTokenHash, LeaseExpiresAt: reservation.ExpiresAt,
		}); createErr != nil {
			t.Fatalf("create %s dispatch: %v", label, createErr)
		}
	}
	createFact(runProduct, reservationProduct, dispatchProduct, "product_thread", "product", 101)
	createFact(runDetached, reservationDetached, dispatchDetached, "detached_task", "detached", 102)

	pendingHost, err := repository.RegisterHost(ctx, identity, registration)
	if err != nil {
		t.Fatalf("re-register Host for recovery: %v", err)
	}
	if pendingHost.Status != "registering" || pendingHost.RecoveryState != "pending" || pendingHost.InstanceGeneration != initialHost.InstanceGeneration || pendingHost.RecoveryRevision <= initialHost.RecoveryRevision {
		t.Fatalf("re-registered Host=%+v initial=%+v", pendingHost, initialHost)
	}
	pendingSnapshot, err := repository.GetHostRecoverySnapshot(ctx, principal)
	if err != nil {
		t.Fatalf("read nonempty recovery snapshot: %v", err)
	}
	if len(pendingSnapshot.Facts) != 2 || pendingSnapshot.Facts[0].ExecutionScope != "detached_task" || pendingSnapshot.Facts[1].ExecutionScope != "product_thread" {
		t.Fatalf("nonempty recovery facts=%+v", pendingSnapshot.Facts)
	}
	for _, fact := range pendingSnapshot.Facts {
		if fact.AssignedRuntimeHostInstanceID != instanceID || fact.AssignedRuntimeHostInstanceGeneration != pendingHost.InstanceGeneration || fact.DispatchID == "" || fact.Status != "created" {
			t.Fatalf("nonempty recovery fact is not assigned/stageable: %+v", fact)
		}
	}

	backend := postgresRecoveryBackendClient{repository: repository}
	adapter := recoveryAdapterFixture(pendingSnapshot, backend, recoveryGatewayFixture{snapshot: pendingSnapshot})
	adapter.maxActiveRuns, adapter.maxProductThreadRuns, adapter.maxDetachedTaskRuns = 2, 1, 1
	adapter.admission = NewHostAdmissionController(2, 1, 1)
	adapter.admission.HoldForRecovery()
	if err := adapter.RecoverHostAdmission(ctx); err != nil {
		t.Fatalf("recover Host admission from PostgreSQL facts: %v", err)
	}
	if total, product, detached := adapter.admission.Snapshot(); total != 2 || product != 1 || detached != 1 {
		t.Fatalf("reconstructed local permits total=%d product=%d detached=%d", total, product, detached)
	}
	if _, err := adapter.acquireRunPermit("runtime-v1-permit-recovery-overflow-"+suffix, "product_thread"); err == nil || err.Error() != "RUNTIME_CAPACITY_UNAVAILABLE" {
		t.Fatalf("reconstructed permit limits admitted extra product Run: %v", err)
	}
	reconciledHost, err := repository.GetHost(ctx, hostID)
	if err != nil || reconciledHost.Status != "ready" || reconciledHost.RecoveryState != "reconciled" {
		t.Fatalf("recovery CAS did not reconcile Host=%+v err=%v", reconciledHost, err)
	}
}

func recoveryAdapterFixture(snapshot runtimepkg.RuntimeHostRecoverySnapshot, backend hostRecoveryBackendClient, gateway hostRecoveryGatewayClient) *adapter {
	a := &adapter{
		runtimeEnvironment: snapshot.Environment, runtimeHostID: snapshot.RuntimeHostID, runtimeInstanceID: snapshot.InstanceID,
		maxActiveRuns: 2, maxProductThreadRuns: 2, maxDetachedTaskRuns: 2,
		recoveryBackend: backend, recoveryGateway: gateway,
	}
	a.admission = NewHostAdmissionController(a.maxActiveRuns, a.maxProductThreadRuns, a.maxDetachedTaskRuns)
	a.admission.HoldForRecovery()
	a.setRecoveryRegisteredHost(runtimepkg.RuntimeHost{
		RuntimeHostID: snapshot.RuntimeHostID, InstanceID: snapshot.InstanceID, Environment: snapshot.Environment,
		InstanceGeneration: snapshot.InstanceGeneration, RecoveryRevision: snapshot.RecoveryRevision, RecoveryState: snapshot.RecoveryState,
	})
	return a
}

func adapterRecoverySnapshot(t *testing.T, mutate func(*runtimepkg.RuntimeHostRecoveryFact)) runtimepkg.RuntimeHostRecoverySnapshot {
	t.Helper()
	fact := runtimepkg.RuntimeHostRecoveryFact{
		RunID: "run-recovery-1", RuntimeHostID: "host-recovery-1", AssignedRuntimeHostInstanceID: "instance-recovery-1",
		AssignedRuntimeHostInstanceGeneration: 3, ReservationID: "reservation-recovery-1", DispatchID: "dispatch-recovery-1",
		FencingToken: 7, ExecutionScope: "product_thread", CapabilityHash: "capability-recovery-1",
		DispatchIdentity: adapterRecoveryHash("dispatch"), RunTicketJTIHash: adapterRecoveryHash("jti"), ManifestHash: adapterRecoveryHash("manifest"),
		Status: "running", LastEventSequence: 4,
	}
	if mutate != nil {
		mutate(&fact)
	}
	facts := []runtimepkg.RuntimeHostRecoveryFact{fact}
	payload, err := json.Marshal(struct {
		Version string                               `json:"version"`
		Facts   []runtimepkg.RuntimeHostRecoveryFact `json:"facts"`
	}{Version: runtimeHostRecoveryVersion, Facts: facts})
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(payload)
	return runtimepkg.RuntimeHostRecoverySnapshot{
		RuntimeHostID: "host-recovery-1", InstanceID: "instance-recovery-1", Environment: "production",
		InstanceGeneration: 3, RecoveryRevision: 9, RecoveryState: "pending", Facts: facts,
		FactSetHash: "sha256:" + hex.EncodeToString(sum[:]),
	}
}

func adapterRecoveryHash(value string) string {
	sum := sha256.Sum256([]byte(value))
	return "sha256:" + hex.EncodeToString(sum[:])
}
