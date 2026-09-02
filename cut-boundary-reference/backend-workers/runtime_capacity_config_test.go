package workers

import (
	"context"
	"strings"
	"testing"

	"huahuoai/backend/source/internal/persistence"
	runtimepkg "huahuoai/backend/source/internal/runtime"
)

func TestParseRuntimeCapacitySnapshotRejectsNonCanonicalConfig(t *testing.T) {
	newRecord := func() map[string]any {
		return map[string]any{
			"configKey": "runtime_capacity_v1", "environment": "prelaunch", "status": "active", "version": "7",
			"payload": map[string]any{
				"snapshotVersion": float64(7), "modelLimit": float64(11), "authPoolLimit": float64(12),
				"toolServiceLimit": float64(13), "tenantLimit": float64(14), "userLimit": float64(15),
				"authPoolId": "runtime-model-default",
			},
		}
	}

	if snapshot, err := parseRuntimeCapacitySnapshot(newRecord(), "prelaunch"); err != nil || snapshot.SnapshotVersion != 7 || snapshot.AuthPoolID != "runtime-model-default" {
		t.Fatalf("canonical capacity snapshot=%+v err=%v", snapshot, err)
	}

	cases := []struct {
		name   string
		mutate func(map[string]any)
	}{
		{"inactive", func(record map[string]any) { record["status"] = "disabled" }},
		{"wrong_environment", func(record map[string]any) { record["environment"] = "production" }},
		{"noncanonical_revision", func(record map[string]any) { record["version"] = "07" }},
		{"revision_snapshot_mismatch", func(record map[string]any) { record["version"] = "8" }},
		{"string_snapshot", func(record map[string]any) { record["payload"].(map[string]any)["snapshotVersion"] = "7" }},
		{"fractional_limit", func(record map[string]any) { record["payload"].(map[string]any)["modelLimit"] = float64(1.5) }},
		{"missing_auth_pool", func(record map[string]any) { record["payload"].(map[string]any)["authPoolId"] = "" }},
	}
	for _, item := range cases {
		t.Run(item.name, func(t *testing.T) {
			record := newRecord()
			item.mutate(record)
			if _, err := parseRuntimeCapacitySnapshot(record, "prelaunch"); err == nil || !strings.Contains(err.Error(), "RUNTIME_CAPACITY_UNAVAILABLE") {
				t.Fatalf("parse err=%v, want RUNTIME_CAPACITY_UNAVAILABLE", err)
			}
		})
	}
}

func TestRepositoryRuntimeCapacityProviderRequiresAuthoritativeActiveSnapshot(t *testing.T) {
	payload := map[string]any{
		"snapshotVersion": 7, "modelLimit": 11, "authPoolLimit": 12, "toolServiceLimit": 13,
		"tenantLimit": 14, "userLimit": 15, "authPoolId": "runtime-model-default",
	}
	run := persistence.AgentRunRecord{AgentRunID: "run_capacity_snapshot", TenantID: "tenant_capacity", UserID: "user_capacity"}
	plan := runtimepkg.AgentRunPlan{RuntimeConfigID: "runtime-default", RequiredTools: []string{"read", "workspace_search"}}

	t.Run("explicit_memory_test_mirror", func(t *testing.T) {
		repos := persistence.NewRepositories(nil)
		repos.Config.UpsertSystemConfig("runtime_capacity_v1", payload, "7", "test")
		provider := RepositoryRuntimeCapacityProvider{Repos: repos, Environment: "prelaunch"}
		command, err := provider.Resolve(context.Background(), run, plan)
		if err != nil {
			t.Fatalf("resolve canonical memory snapshot: %v", err)
		}
		if command.SnapshotVersion != 7 || command.Dimensions.AuthPool.Key != "auth:runtime-model-default" || command.Dimensions.Model.Limit != 11 {
			t.Fatalf("unexpected capacity command=%+v", command)
		}
	})

	t.Run("declared_durable_store_cannot_fallback_to_memory", func(t *testing.T) {
		repos := persistence.NewRepositories(&persistence.Database{})
		repos.Config.UpsertSystemConfig("runtime_capacity_v1", payload, "7", "test")
		provider := RepositoryRuntimeCapacityProvider{Repos: repos, Environment: "prelaunch"}
		if _, err := provider.Resolve(context.Background(), run, plan); err == nil || !strings.Contains(err.Error(), "RUNTIME_CAPACITY_UNAVAILABLE") {
			t.Fatalf("durable store outage err=%v, want RUNTIME_CAPACITY_UNAVAILABLE", err)
		}
	})

	t.Run("capacity_snapshot_runtime_config_cannot_replace_frozen_plan_identity", func(t *testing.T) {
		repos := persistence.NewRepositories(nil)
		legacyPayload := map[string]any{
			"snapshotVersion": 7, "modelLimit": 11, "authPoolLimit": 12, "toolServiceLimit": 13,
			"tenantLimit": 14, "userLimit": 15, "authPoolId": "runtime-model-default", "runtimeConfigId": "huahuo-default",
		}
		repos.Config.UpsertSystemConfig("runtime_capacity_v1", legacyPayload, "7", "test")
		provider := RepositoryRuntimeCapacityProvider{Repos: repos, Environment: "prelaunch"}
		missingFrozenConfig := runtimepkg.AgentRunPlan{TaskType: "work_ai_self_media_creation", RequiredTools: []string{"read"}}
		if _, err := provider.Resolve(context.Background(), run, missingFrozenConfig); err == nil || !strings.Contains(err.Error(), "AGENT_PLAN_INVALID") {
			t.Fatalf("capacity runtime config must not become a Plan fallback: err=%v", err)
		}
	})
}
