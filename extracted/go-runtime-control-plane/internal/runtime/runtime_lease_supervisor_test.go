package runtime

import (
	"context"
	"testing"
	"time"

	"huahuoai/backend/source/internal/queue"
)

func TestRuntimeLeaseSupervisorRenewsAcceptedReservationAndDispatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	scheduler, handle, dispatchID := acceptedRuntimeLeaseForSupervisorTest(t, ctx, 180*time.Millisecond, "detached_task")

	if err := scheduler.LeaseSupervisor.Track(ctx, handle, dispatchID, 180*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	defer scheduler.LeaseSupervisor.Stop(handle.Reservation.RunID)

	time.Sleep(320 * time.Millisecond)
	reservation, err := scheduler.Hosts.GetReservation(ctx, handle.Reservation.ReservationID)
	if err != nil || !reservation.ExpiresAt.After(time.Now().UTC()) || reservation.State != "accepted" {
		t.Fatalf("reservation=%+v err=%v", reservation, err)
	}
	dispatch, err := scheduler.Hosts.GetDispatch(ctx, dispatchID)
	if err != nil || !dispatch.LeaseExpiresAt.After(time.Now().UTC()) || dispatch.State != "accepted" {
		t.Fatalf("dispatch=%+v err=%v", dispatch, err)
	}
	if !scheduler.LeaseSupervisor.IsTracking(handle.Reservation.RunID) {
		t.Fatal("accepted run lost its lease owner")
	}
}

func TestRuntimeLeaseSupervisorMarksRecoveryAfterExactLeaseLoss(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	scheduler, handle, dispatchID := acceptedRuntimeLeaseForSupervisorTest(t, ctx, 150*time.Millisecond, "detached_task")
	lost := make(chan RuntimeLeaseLoss, 1)
	scheduler.LeaseSupervisor.SetLeaseLossHandler(func(_ context.Context, loss RuntimeLeaseLoss) {
		lost <- loss
	})
	if err := scheduler.LeaseSupervisor.Track(ctx, handle, dispatchID, 150*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if err := scheduler.Locks.Release(ctx, handle.Lease); err != nil {
		t.Fatal(err)
	}

	select {
	case loss := <-lost:
		if loss.RunID != handle.Reservation.RunID || loss.DispatchID != dispatchID || loss.ErrorCode != "STALE_FENCING_TOKEN" {
			t.Fatalf("loss=%+v", loss)
		}
	case <-time.After(time.Second):
		t.Fatal("lease loss was not observed")
	}
	dispatch, err := scheduler.Hosts.GetDispatch(ctx, dispatchID)
	if err != nil || dispatch.State != "recovering" || dispatch.NextRecoveryCheckAt.IsZero() {
		t.Fatalf("dispatch=%+v err=%v", dispatch, err)
	}
	if scheduler.LeaseSupervisor.IsTracking(handle.Reservation.RunID) {
		t.Fatal("lost owner remained active")
	}
}

func TestRuntimeLeaseSupervisorReleasesSessionAfterRemoteTerminal(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	scheduler, handle, dispatchID := acceptedRuntimeLeaseForSupervisorTest(t, ctx, 180*time.Millisecond, "product_thread")
	lost := make(chan RuntimeLeaseLoss, 1)
	scheduler.LeaseSupervisor.SetLeaseLossHandler(func(_ context.Context, loss RuntimeLeaseLoss) { lost <- loss })
	if err := scheduler.LeaseSupervisor.Track(ctx, handle, dispatchID, 180*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if err := scheduler.Hosts.FinalizeDispatchAndReleaseSlot(ctx, DispatchTerminalCommand{
		Fence: handle.Fence(), DispatchID: dispatchID, TerminalStatus: "succeeded",
	}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for scheduler.LeaseSupervisor.IsTracking(handle.Reservation.RunID) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if scheduler.LeaseSupervisor.IsTracking(handle.Reservation.RunID) {
		t.Fatal("terminal ownership was not stopped")
	}
	released, err := scheduler.Sessions.ReleasedOrExpiredByRunID(ctx, handle.Reservation.RunID)
	if err != nil || !released {
		t.Fatalf("session released=%v err=%v", released, err)
	}
	select {
	case loss := <-lost:
		t.Fatalf("terminal release was reported as lease loss: %+v", loss)
	default:
	}
}

func acceptedRuntimeLeaseForSupervisorTest(t *testing.T, ctx context.Context, ttl time.Duration, scope string) (*RuntimeScheduler, RuntimeReservationLease, string) {
	t.Helper()
	hosts := NewRuntimeHostRepository(nil)
	identity := RuntimeHostIdentity{RuntimeHostID: "host-supervisor", InstanceID: "instance-supervisor", Environment: "test"}
	capabilities := runtimeTestCapabilities("cap-supervisor", CanonicalAgentFacingToolCapability("read", "ready"))
	if _, err := hosts.RegisterHost(ctx, identity, RuntimeHostRegistration{Endpoint: "http://host-supervisor", Capabilities: capabilities, MaxActiveRuns: 4}); err != nil {
		t.Fatal(err)
	}
	if _, err := hosts.HeartbeatHost(ctx, identity, RuntimeHostHeartbeat{Sequence: 1, ObservedAt: time.Now().UTC(), CapabilityHash: capabilities.CapabilityHash, SignatureKeyID: "test-key"}); err != nil {
		t.Fatal(err)
	}
	locks := queue.NewMemoryDistributedLockManager()
	scheduler := NewRuntimeScheduler(hosts, locks)
	runID := "run-supervisor-" + time.Now().UTC().Format("150405.000000000")
	capacity, err := scheduler.Capacity.Reserve(ctx, testCapacityCommand(runID, 10))
	if err != nil {
		t.Fatal(err)
	}
	command := ScheduleCommand{
		RunID: runID, OwnerInstanceID: "worker-supervisor", ExecutionScope: scope,
		CapabilityHash: capabilities.CapabilityHash, RequiredTools: []string{"read"},
		CapacityReservation: capacity, ReservationTTL: ttl,
	}
	if scope == "product_thread" {
		binding, bindingErr := hosts.EnsureProductSessionBinding(ctx, ProductSessionBindingCommand{
			ThreadID: "thread-" + runID, TenantID: "tenant-supervisor", UserID: "user-supervisor", WorkspaceID: "workspace-supervisor",
			AgentProfile: "agent-supervisor", ContextGeneration: 1, ManifestVersion: "manifest-v1",
			AgentHash: "sha256:agent", SessionKeyEncryptionSecret: "runtime-session-secret-test",
		})
		if bindingErr != nil {
			t.Fatal(bindingErr)
		}
		command.SessionBinding = ProductSessionHostBinding{
			TenantID: binding.TenantID, ThreadID: binding.ThreadID, AgentProfile: binding.AgentProfile,
			ContextGeneration: binding.ContextGeneration, SessionGeneration: binding.SessionGeneration,
		}
		command.SessionAdmission = ProductSessionAdmissionCommand{
			Key: ProductSessionAdmissionKey{
				TenantID: binding.TenantID, ThreadID: binding.ThreadID, AgentProfile: binding.AgentProfile,
				ContextGeneration: binding.ContextGeneration, SessionGeneration: binding.SessionGeneration,
			},
			BindingID: binding.BindingID, RunID: runID, OwnerInstanceID: "worker-supervisor", TTL: ttl,
		}
	}
	handle, err := scheduler.Reserve(ctx, command)
	if err != nil {
		t.Fatal(err)
	}
	dispatchID := "dispatch-" + runID
	if _, err := hosts.CreateDispatch(ctx, RuntimeDispatch{
		DispatchID: dispatchID, RunID: runID, ReservationID: handle.Reservation.ReservationID,
		RuntimeHostID: handle.Host.RuntimeHostID, DispatchAttempt: 1, PlanVersion: 1,
		FencingToken: handle.Reservation.FencingToken, RunTicketJTIHash: "sha256:jti",
		TicketExpiresAt: time.Now().UTC().Add(time.Minute), InputManifestHash: "sha256:manifest",
		OwnerInstanceID: handle.Reservation.OwnerInstanceID, LeaseTokenHash: handle.Reservation.LeaseTokenHash,
		LeaseExpiresAt: handle.Reservation.ExpiresAt,
	}); err != nil {
		t.Fatal(err)
	}
	if err := scheduler.Accept(ctx, handle, dispatchID, "runtime-request"); err != nil {
		t.Fatal(err)
	}
	return scheduler, handle, dispatchID
}
