package runtime

import (
	"context"
	"strings"
	"testing"
	"time"

	"huahuoai/backend/source/internal/domain"
	"huahuoai/backend/source/internal/persistence"
)

func TestRuntimeRunRecordV1FailsClosedWhenConfiguredDatabaseIsUnavailable(t *testing.T) {
	repository := NewRuntimeHostRepository(&persistence.Database{DSN: "postgres://configured-but-unavailable", Disabled: false})
	record := runtimeRunRecordV1TestRecord()
	_, err := repository.CreateDispatchWithRuntimeRunRecord(context.Background(), RuntimeDispatch{
		DispatchID: "dispatch_runtime_record_unavailable",
		RunID:      record.RunID, ReservationID: "reservation_runtime_record_unavailable",
		CapacityReservationID: "capacity_runtime_record_unavailable", CapacityReservedVersion: 1,
		RuntimeHostID: "host_runtime_record_unavailable", DispatchAttempt: 1, PlanVersion: 1,
		FencingToken: 1, RunTicketJTIHash: "sha256:runtime_record_unavailable",
		TicketExpiresAt: time.Now().UTC().Add(time.Minute), InputManifestHash: "sha256:runtime_record_unavailable",
		OwnerInstanceID: "runtime_record_worker", LeaseTokenHash: "sha256:runtime_record_lease", LeaseExpiresAt: time.Now().UTC().Add(time.Minute),
	}, record)
	if err == nil || !strings.Contains(err.Error(), "RUNTIME_RUN_RECORD_UNAVAILABLE") {
		t.Fatalf("configured database must fail closed instead of using memory err=%v", err)
	}
}

func TestNewRuntimeRunRecordV1RejectsFrozenContextMismatch(t *testing.T) {
	run, plan, frozen := runtimeRunRecordV1ConstructorFixture()
	frozen.WorkspaceVersion++
	_, err := NewRuntimeRunRecordV1(run, plan, frozen, "runtime-config-context", "v1")
	if err == nil || !strings.Contains(err.Error(), "AGENT_PLAN_EXPIRED") {
		t.Fatalf("mismatched frozen context err=%v", err)
	}
}

func TestNewRuntimeRunRecordV1RejectsPlanAndFrozenIdentityMismatch(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*persistence.AgentRunRecord, *AgentRunPlan, *domain.RunWorkspaceContextRecord)
	}{
		{
			name: "plan agent run",
			mutate: func(_ *persistence.AgentRunRecord, plan *AgentRunPlan, _ *domain.RunWorkspaceContextRecord) {
				plan.AgentRunID = "run_other"
			},
		},
		{
			name: "plan workspace version",
			mutate: func(_ *persistence.AgentRunRecord, plan *AgentRunPlan, _ *domain.RunWorkspaceContextRecord) {
				plan.WorkspaceVersion++
			},
		},
		{
			name: "plan context manifest",
			mutate: func(_ *persistence.AgentRunRecord, plan *AgentRunPlan, _ *domain.RunWorkspaceContextRecord) {
				plan.WorkspaceContextManifestHash = "sha256:other"
			},
		},
		{
			name: "frozen tenant",
			mutate: func(_ *persistence.AgentRunRecord, _ *AgentRunPlan, frozen *domain.RunWorkspaceContextRecord) {
				frozen.TenantID = "tenant_other"
			},
		},
		{
			name: "frozen state",
			mutate: func(_ *persistence.AgentRunRecord, _ *AgentRunPlan, frozen *domain.RunWorkspaceContextRecord) {
				frozen.Status = "stale"
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			run, plan, frozen := runtimeRunRecordV1ConstructorFixture()
			test.mutate(&run, &plan, &frozen)
			_, err := NewRuntimeRunRecordV1(run, plan, frozen, "runtime-config-context", "v1")
			if err == nil || !strings.Contains(err.Error(), "AGENT_PLAN_EXPIRED") {
				t.Fatalf("mismatched immutable context err=%v", err)
			}
		})
	}
}

func TestRuntimeRunRecordV1HydratesAndValidatesCanonicalSnapshot(t *testing.T) {
	record := runtimeRunRecordV1TestRecord()
	record.RuntimeHostID = "host_runtime_record_snapshot"
	record.ReservationID = "reservation_runtime_record_snapshot"
	record.DispatchID = "dispatch_runtime_record_snapshot"
	record.DispatchAttempt = 1
	record.FencingToken = 1
	record.InputManifestHash = "sha256:runtime_record_snapshot"
	record.ConfigSnapshot = runtimeRunRecordV1ConfigSnapshot(record)
	record.TenantID = ""
	record.RuntimeConfigID = ""
	record.RuntimeConfigVersion = ""
	record.CapabilityHash = ""
	record.InputManifestHash = ""
	record.PlanVersion = 0
	record.DispatchID = ""

	if err := hydrateRuntimeRunRecordV1Config(&record); err != nil {
		t.Fatalf("hydrate canonical runtime run record snapshot: %v", err)
	}
	if record.TenantID != "tenant_runtime_record_unavailable" || record.RuntimeConfigID != "runtime-config-unavailable" ||
		record.RuntimeConfigVersion != "v1" || record.CapabilityHash != "sha256:runtime_record_unavailable" ||
		record.DispatchID != "dispatch_runtime_record_snapshot" || record.PlanVersion != 1 {
		t.Fatalf("hydrated runtime run record=%+v", record)
	}
	record.Status = "succeeded"
	record.LastEventSequence = 3
	record.ConfigSnapshot["status"] = record.Status
	record.ConfigSnapshot["terminalStatus"] = record.Status
	record.ConfigSnapshot["terminalSourceSequence"] = record.LastEventSequence
	if err := hydrateRuntimeRunRecordV1Config(&record); err != nil {
		t.Fatalf("hydrate terminal runtime run record snapshot: %v", err)
	}

	record.ConfigSnapshot["agentRunId"] = "run_other"
	if err := hydrateRuntimeRunRecordV1Config(&record); err == nil || !strings.Contains(err.Error(), "RUNTIME_RUN_RECORD_UNAVAILABLE") {
		t.Fatalf("mismatched canonical snapshot err=%v", err)
	}
}

func TestRuntimeRunRecordV1JSONRejectsUnserializableSnapshots(t *testing.T) {
	if _, err := runtimeRunRecordV1JSON(map[string]any{"invalid": make(chan struct{})}); err == nil || !strings.Contains(err.Error(), "RUNTIME_RUN_RECORD_INVALID") {
		t.Fatalf("unserializable runtime record snapshot err=%v", err)
	}
}

func runtimeRunRecordV1TestRecord() RuntimeRunRecordV1 {
	return RuntimeRunRecordV1{
		RunID:                         "run_runtime_record_unavailable",
		AgentRunID:                    "run_runtime_record_unavailable",
		TenantID:                      "tenant_runtime_record_unavailable",
		UserID:                        "user_runtime_record_unavailable",
		ThreadID:                      "thread_runtime_record_unavailable",
		WorkspaceID:                   "workspace_runtime_record_unavailable",
		WorkspaceVersion:              1,
		IndexVersion:                  0,
		ThreadWorkspaceBindingVersion: 1,
		ContextGeneration:             1,
		SessionGeneration:             1,
		ExecutionScope:                string(ScopeProductThread),
		PlanVersion:                   1,
		RuntimeConfigID:               "runtime-config-unavailable",
		RuntimeConfigVersion:          "v1",
		CapabilityHash:                "sha256:runtime_record_unavailable",
		Status:                        "created",
	}
}

func runtimeRunRecordV1ConstructorFixture() (persistence.AgentRunRecord, AgentRunPlan, domain.RunWorkspaceContextRecord) {
	manifestHash := "sha256:runtime_record_context"
	run := persistence.AgentRunRecord{
		AgentRunID: "run_runtime_record_context", TenantID: "tenant_runtime_record_context", UserID: "user_runtime_record_context",
		WorkspaceID: "workspace_runtime_record_context", ThreadID: "thread_runtime_record_context",
		WorkspaceVersion: 1, BindingVersion: 1, ContextGeneration: 1,
	}
	plan := AgentRunPlan{
		SchemaVersion: "agent_run_plan.v1", AgentRunID: run.AgentRunID, PlanVersion: 1,
		ExecutionScope: string(ScopeProductThread), WorkspaceVersion: run.WorkspaceVersion, IndexVersion: 0,
		WorkspaceContextManifestHash: manifestHash, CapabilityHash: "sha256:runtime_record_context",
	}
	frozen := domain.RunWorkspaceContextRecord{
		RunID: run.AgentRunID, AgentRunID: run.AgentRunID, TenantID: run.TenantID, UserID: run.UserID,
		WorkspaceID: run.WorkspaceID, WorkspaceVersion: run.WorkspaceVersion, IndexVersion: plan.IndexVersion,
		ThreadID: run.ThreadID, ThreadWorkspaceBindingVersion: run.BindingVersion, ContextGeneration: run.ContextGeneration,
		SessionGeneration: 1, CapabilityHash: plan.CapabilityHash, ManifestHash: manifestHash, Status: "frozen",
	}
	return run, plan, frozen
}
