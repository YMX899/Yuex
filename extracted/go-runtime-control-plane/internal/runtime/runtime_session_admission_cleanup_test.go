package runtime

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"huahuoai/backend/source/internal/queue"
)

func TestRuntimeSessionAdmissionCleanupDrainReleasesDurableMemoryRecord(t *testing.T) {
	ctx := context.Background()
	locks := queue.NewMemoryDistributedLockManager()
	service := NewRuntimeSessionAdmissionService(nil, locks)
	handle := mustAcquireRuntimeSessionAdmission(t, service, "run-cleanup", time.Second)
	service.afterAdmissionCleanupEnqueue = func() error { return fmt.Errorf("injected_post_commit_crash") }
	if changed, err := service.Release(ctx, handle, "succeeded"); !changed || err == nil {
		t.Fatalf("release changed=%v err=%v", changed, err)
	}
	service.afterAdmissionCleanupEnqueue = nil

	record := service.cleanup[handle.Admission.AdmissionID]
	if record.Status != "pending" || record.Origin != "direct_release" || record.Reason != "succeeded" {
		t.Fatalf("durable cleanup record=%+v", record)
	}
	report, err := service.DrainAdmissionCleanup(ctx, "cleanup-worker", 1)
	if err != nil || report.Claimed != 1 || report.Completed != 1 || report.Retried != 0 || report.Stale != 0 {
		t.Fatalf("drain report=%+v err=%v", report, err)
	}
	if err := locks.AssertActiveLease(ctx, handle.Lease, 0); err == nil || err.Error() != "STALE_FENCING_TOKEN" {
		t.Fatalf("released lease still active err=%v", err)
	}
	record = service.cleanup[handle.Admission.AdmissionID]
	if record.Status != "succeeded" || record.Completed.IsZero() || record.OwnerID != "" || !record.ExpiresAt.IsZero() {
		t.Fatalf("completed cleanup record=%+v", record)
	}
	if _, ok := service.handles[handle.Admission.AdmissionID]; ok {
		t.Fatal("completed cleanup retained raw-token handle")
	}
}

func TestRuntimeSessionAdmissionCleanupDrainTreatsSuccessorProofAsStaleSuccess(t *testing.T) {
	ctx := context.Background()
	locks := queue.NewMemoryDistributedLockManager()
	firstService := NewRuntimeSessionAdmissionService(nil, locks)
	first := mustAcquireRuntimeSessionAdmission(t, firstService, "run-cleanup-first", 25*time.Millisecond)
	firstService.afterAdmissionCleanupEnqueue = func() error { return fmt.Errorf("injected_post_commit_crash") }
	if changed, err := firstService.Release(ctx, first, "orphaned"); !changed || err == nil {
		t.Fatalf("release changed=%v err=%v", changed, err)
	}
	firstService.afterAdmissionCleanupEnqueue = nil

	secondService := NewRuntimeSessionAdmissionService(nil, locks)
	var successor RuntimeSessionAdmissionLease
	deadline := time.Now().Add(time.Second)
	for {
		var err error
		successor, err = secondService.Acquire(ctx, ProductSessionAdmissionCommand{
			Key: first.Admission.Key, BindingID: first.Admission.BindingID, RunID: "run-cleanup-successor", OwnerInstanceID: "worker-successor", TTL: time.Second,
		})
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("successor did not acquire after first lease expiry: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	report, err := firstService.DrainAdmissionCleanup(ctx, "cleanup-worker", 1)
	if err != nil || report.Claimed != 1 || report.Completed != 1 || report.Stale != 1 || report.Retried != 0 {
		t.Fatalf("stale cleanup report=%+v err=%v", report, err)
	}
	if err := locks.AssertActiveLease(ctx, successor.Lease, 0); err != nil {
		t.Fatalf("stale cleanup removed successor lease: %v", err)
	}
	record := firstService.cleanup[first.Admission.AdmissionID]
	if record.Status != "succeeded" || record.Origin != "orphan_recovery" {
		t.Fatalf("stale cleanup record=%+v", record)
	}
}

func TestRuntimeSessionAdmissionCleanupInvalidRecordRetriesWithoutDeletingLease(t *testing.T) {
	ctx := context.Background()
	locks := queue.NewMemoryDistributedLockManager()
	service := NewRuntimeSessionAdmissionService(nil, locks)
	handle := mustAcquireRuntimeSessionAdmission(t, service, "run-cleanup-invalid", time.Second)
	service.afterAdmissionCleanupEnqueue = func() error { return fmt.Errorf("injected_post_commit_crash") }
	if changed, err := service.Release(ctx, handle, "failed"); !changed || err == nil {
		t.Fatalf("release changed=%v err=%v", changed, err)
	}
	service.afterAdmissionCleanupEnqueue = nil
	record := service.cleanup[handle.Admission.AdmissionID]
	record.Origin = "unsafe_origin"
	service.cleanup[handle.Admission.AdmissionID] = record

	report, err := service.DrainAdmissionCleanup(ctx, "cleanup-worker", 1)
	if err != nil || report.Claimed != 1 || report.Retried != 1 || report.Completed != 0 {
		t.Fatalf("invalid cleanup report=%+v err=%v", report, err)
	}
	if err := locks.AssertActiveLease(ctx, handle.Lease, 0); err != nil {
		t.Fatalf("invalid cleanup deleted live lease: %v", err)
	}
	record = service.cleanup[handle.Admission.AdmissionID]
	if record.Status != "pending" || record.LastError != "RUNTIME_SESSION_CLEANUP_INVALID_PROOF" || !record.NextTryAt.After(time.Now().UTC()) {
		t.Fatalf("invalid cleanup record=%+v", record)
	}
}

func TestRuntimeSessionAdmissionRecoveryAndBackfillCreateGenericCleanupRecords(t *testing.T) {
	ctx := context.Background()
	locks := queue.NewMemoryDistributedLockManager()
	service := NewRuntimeSessionAdmissionService(nil, locks)
	base := time.Now().UTC()
	service.Now = func() time.Time { return base }

	expired := mustAcquireRuntimeSessionAdmission(t, service, "run-cleanup-expired", time.Minute)
	service.mu.Lock()
	item := service.items[expired.Admission.AdmissionID]
	item.ExpiresAt = base
	service.items[expired.Admission.AdmissionID] = item
	service.mu.Unlock()
	recovery, err := service.Recover(ctx, base, 1)
	if err != nil || recovery.Expired != 1 {
		t.Fatalf("recovery=%+v err=%v", recovery, err)
	}
	record := service.cleanup[expired.Admission.AdmissionID]
	if record.Status != "pending" || record.Origin != "lease_expiry" || record.Reason != "lease_expired" {
		t.Fatalf("expired cleanup record=%+v", record)
	}

	legacy, err := service.Acquire(ctx, ProductSessionAdmissionCommand{
		Key: ProductSessionAdmissionKey{
			TenantID: "tenant-cleanup", ThreadID: "thread-cleanup-legacy", AgentProfile: "agent-cleanup", ContextGeneration: 1, SessionGeneration: 1,
		},
		BindingID: "binding-cleanup-legacy", RunID: "run-cleanup-legacy", OwnerInstanceID: "worker-cleanup", TTL: time.Minute,
	})
	if err != nil {
		t.Fatalf("acquire legacy admission: %v", err)
	}
	service.mu.Lock()
	legacyItem := service.items[legacy.Admission.AdmissionID]
	legacyItem.State = "released"
	service.items[legacy.Admission.AdmissionID] = legacyItem
	service.mu.Unlock()
	created, err := service.BackfillAdmissionCleanup(ctx, 10)
	if err != nil || created != 1 {
		t.Fatalf("backfill created=%d err=%v", created, err)
	}
	record = service.cleanup[legacy.Admission.AdmissionID]
	if record.Status != "pending" || record.Origin != "orphan_recovery" || record.Reason != "recovered" {
		t.Fatalf("backfilled cleanup record=%+v", record)
	}
}

func TestRuntimeSessionAdmissionBackfillFailsClosedForMismatchedExistingCleanup(t *testing.T) {
	ctx := context.Background()
	service := NewRuntimeSessionAdmissionService(nil, queue.NewMemoryDistributedLockManager())
	handle := mustAcquireRuntimeSessionAdmission(t, service, "run-cleanup-mismatch", time.Minute)

	service.mu.Lock()
	admission := service.items[handle.Admission.AdmissionID]
	admission.State = "released"
	service.items[admission.AdmissionID] = admission
	foreign := admission
	foreign.RunID = "run-cleanup-foreign"
	service.cleanup[admission.AdmissionID] = runtimeSessionAdmissionCleanupRecord{
		Admission: foreign, Origin: "direct_release", Reason: "recovered", Status: "pending", NextTryAt: service.now(),
	}
	service.mu.Unlock()

	created, err := service.BackfillAdmissionCleanup(ctx, 10)
	if created != 0 || err == nil || err.Error() != "RUNTIME_SESSION_CLEANUP_CONFLICT" {
		t.Fatalf("backfill created=%d err=%v", created, err)
	}
}

func TestRuntimeSessionAdmissionFromMapRequiresCompleteTenantScopedIdentity(t *testing.T) {
	now := time.Now().UTC()
	row := map[string]any{
		"admission_id": "admission-map", "tenant_id": "tenant-map", "thread_id": "thread-map", "agent_profile": "agent-map",
		"context_generation": int64(1), "session_generation": int32(1), "binding_id": "binding-map", "run_id": "run-map",
		"owner_instance_id": "worker-map", "lease_token_hash": "sha256:map", "fencing_token": int64(1), "state": "recovering",
		"reservation_id": "", "dispatch_id": "", "expires_at": now, "last_renewed_at": now,
		"version": int64(1), "created_at": now, "updated_at": now,
	}
	admission, err := runtimeSessionAdmissionFromMap(row)
	if err != nil || admission.Key.RedisKey() == "" || admission.RunID != "run-map" {
		t.Fatalf("admission=%+v err=%v", admission, err)
	}
	delete(row, "tenant_id")
	if _, err := runtimeSessionAdmissionFromMap(row); err == nil || err.Error() != "RUNTIME_SESSION_CLEANUP_CONFLICT" {
		t.Fatalf("missing tenant identity err=%v", err)
	}
}

func TestRuntimeSessionAdmissionCleanupOriginIsBounded(t *testing.T) {
	for reason, want := range map[string]string{
		"succeeded":     "direct_release",
		"orphaned":      "orphan_recovery",
		"recovered":     "orphan_recovery",
		"lease_expired": "lease_expiry",
	} {
		if got := runtimeSessionAdmissionCleanupOrigin(reason); got != want || !validRuntimeSessionAdmissionCleanupOrigin(got) {
			t.Fatalf("reason=%q origin=%q want=%q", reason, got, want)
		}
	}
	if validRuntimeSessionAdmissionCleanupOrigin("arbitrary") {
		t.Fatal("unbounded cleanup origin accepted")
	}
}

func TestRuntimeTerminalCleanupSQLRequiresSameRunAtEveryDrainStep(t *testing.T) {
	source, err := os.ReadFile("runtime_session_admission.go")
	if err != nil {
		t.Fatalf("read runtime session admission source: %v", err)
	}
	body := string(source)
	for _, method := range []string{
		"func (s *RuntimeSessionAdmissionService) claimTerminalLeaseCleanup",
		"func (s *RuntimeSessionAdmissionService) completeTerminalLeaseCleanup",
		"func (s *RuntimeSessionAdmissionService) retryTerminalLeaseCleanup",
	} {
		start := strings.Index(body, method)
		if start < 0 {
			t.Fatalf("missing terminal cleanup method %q", method)
		}
		rest := body[start+len(method):]
		end := strings.Index(rest, "\nfunc ")
		if end < 0 {
			end = len(rest)
		}
		if !strings.Contains(rest[:end], "a.run_id=c.run_id") {
			t.Fatalf("%s must bind admission and convergence to the same run", method)
		}
	}
}

func TestGenericCleanupEnqueueDefersToSameRunTerminalOutbox(t *testing.T) {
	source, err := os.ReadFile("runtime_session_admission.go")
	if err != nil {
		t.Fatalf("read runtime session admission source: %v", err)
	}
	body := string(source)
	start := strings.Index(body, "func (s *RuntimeSessionAdmissionService) enqueueAdmissionCleanupInTx")
	if start < 0 {
		t.Fatal("missing generic cleanup enqueue")
	}
	rest := body[start:]
	end := strings.Index(rest, "\nfunc ")
	if end < 0 {
		end = len(rest)
	}
	section := rest[:end]
	for _, required := range []string{
		"runtime_session_terminal_cleanup_outbox terminal",
		"convergence.run_id=@run",
		"if len(terminalRows) > 0",
	} {
		if !strings.Contains(section, required) {
			t.Fatalf("generic cleanup enqueue must defer to same-run terminal cleanup: %q", required)
		}
	}
}

func TestGenericCleanupBackfillRequiresFullExistingOutboxIdentity(t *testing.T) {
	source, err := os.ReadFile("runtime_session_admission.go")
	if err != nil {
		t.Fatalf("read runtime session admission source: %v", err)
	}
	body := string(source)
	start := strings.Index(body, "func (s *RuntimeSessionAdmissionService) BackfillAdmissionCleanup")
	if start < 0 {
		t.Fatal("missing generic cleanup backfill")
	}
	rest := body[start:]
	end := strings.Index(rest, "\nfunc ")
	if end < 0 {
		end = len(rest)
	}
	section := rest[:end]
	for _, required := range []string{
		"generic.run_id=a.run_id",
		"generic.owner_instance_id=a.owner_instance_id",
		"generic.lease_token_hash=a.lease_token_hash",
		"generic.fencing_token=a.fencing_token",
	} {
		if !strings.Contains(section, required) {
			t.Fatalf("generic cleanup backfill must require exact existing outbox identity: %q", required)
		}
	}
}

func TestRuntimeSessionAdmissionAssertActiveDispatchFenceUsesDurableHashProof(t *testing.T) {
	ctx := context.Background()
	locks := queue.NewMemoryDistributedLockManager()
	service := NewRuntimeSessionAdmissionService(nil, locks)
	handle := mustAcquireRuntimeSessionAdmission(t, service, "run-dispatch-fence", time.Minute)
	if err := service.BindReservation(ctx, handle, "reservation-dispatch-fence"); err != nil {
		t.Fatalf("bind reservation: %v", err)
	}
	if err := service.BindDispatch(ctx, handle, "dispatch-fence"); err != nil {
		t.Fatalf("bind dispatch: %v", err)
	}

	// Abort workers may run in a different process and do not receive the raw
	// owner token. Deleting the protected local handle proves this assertion is
	// based on the durable admission identity plus the hash-bound live lease.
	service.mu.Lock()
	delete(service.handles, handle.Admission.AdmissionID)
	service.mu.Unlock()
	if err := service.AssertActiveDispatchFence(ctx, handle.Admission.RunID, "reservation-dispatch-fence", "dispatch-fence"); err != nil {
		t.Fatalf("exact dispatch admission fence rejected: %v", err)
	}
	for _, wrong := range [][2]string{{"reservation-other", "dispatch-fence"}, {"reservation-dispatch-fence", "dispatch-other"}} {
		if err := service.AssertActiveDispatchFence(ctx, handle.Admission.RunID, wrong[0], wrong[1]); err == nil || err.Error() != "STALE_FENCING_TOKEN" {
			t.Fatalf("wrong dispatch fence reservation=%s dispatch=%s err=%v", wrong[0], wrong[1], err)
		}
	}
	if err := locks.Release(ctx, handle.Lease); err != nil {
		t.Fatalf("release live lease: %v", err)
	}
	if err := service.AssertActiveDispatchFence(ctx, handle.Admission.RunID, "reservation-dispatch-fence", "dispatch-fence"); err == nil || err.Error() != "STALE_FENCING_TOKEN" {
		t.Fatalf("released lease still authorized abort err=%v", err)
	}
}

func mustAcquireRuntimeSessionAdmission(t *testing.T, service *RuntimeSessionAdmissionService, runID string, ttl time.Duration) RuntimeSessionAdmissionLease {
	t.Helper()
	handle, err := service.Acquire(context.Background(), ProductSessionAdmissionCommand{
		Key: ProductSessionAdmissionKey{
			TenantID: "tenant-cleanup", ThreadID: "thread-cleanup", AgentProfile: "agent-cleanup", ContextGeneration: 1, SessionGeneration: 1,
		},
		BindingID: "binding-cleanup", RunID: runID, OwnerInstanceID: "worker-cleanup", TTL: ttl,
	})
	if err != nil {
		t.Fatalf("acquire runtime session admission: %v", err)
	}
	return handle
}
