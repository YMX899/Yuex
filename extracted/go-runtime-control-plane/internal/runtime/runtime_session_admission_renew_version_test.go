package runtime

import (
	"context"
	"testing"
	"time"

	"huahuoai/backend/source/internal/queue"
)

func TestRuntimeSessionAdmissionRenewMemoryKeepsHandleVersionInSync(t *testing.T) {
	service := NewRuntimeSessionAdmissionService(nil, queue.NewMemoryDistributedLockManager())
	lease, err := service.Acquire(context.Background(), ProductSessionAdmissionCommand{
		Key: ProductSessionAdmissionKey{
			TenantID: "tenant_renew_version", ThreadID: "thread_renew_version", AgentProfile: "agent_renew_version",
			ContextGeneration: 1, SessionGeneration: 1,
		},
		BindingID: "binding_renew_version", RunID: "run_renew_version", OwnerInstanceID: "worker_renew_version", TTL: time.Minute,
	})
	if err != nil {
		t.Fatalf("acquire admission: %v", err)
	}

	renewed, err := service.Renew(context.Background(), lease, time.Minute)
	if err != nil {
		t.Fatalf("renew admission: %v", err)
	}
	if renewed.Admission.Version != lease.Admission.Version+1 {
		t.Fatalf("renewed handle version=%d, want %d", renewed.Admission.Version, lease.Admission.Version+1)
	}

	service.mu.Lock()
	stored := service.items[renewed.Admission.AdmissionID]
	service.mu.Unlock()
	if stored.Version != renewed.Admission.Version {
		t.Fatalf("stored version=%d differs from renewed handle version=%d", stored.Version, renewed.Admission.Version)
	}
}
