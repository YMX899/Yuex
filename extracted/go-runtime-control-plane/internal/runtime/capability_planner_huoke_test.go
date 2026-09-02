package runtime

import (
	"reflect"
	"testing"

	"huahuoai/backend/source/internal/domain"
)

func TestHuokeTopicPlanUsesReadAndWriteOnly(t *testing.T) {
	plan, err := NewCapabilityPlanner().BuildDeterministicPlan(domain.TaskIntent{
		AgentRunID: "run_huoke", ResolvedTaskType: "work_ai_huoke_topic_strategy",
		ExecutionScope: "product_thread", RiskClass: "generative", ExpectedOutput: "structured_json",
		RequiredCapabilities: []string{"workspace_read", "workspace_staging_write"},
	}, L1AgentRouteResult{
		AgentProfile: "huoke_neirong_agent", AgentHash: "sha256:huoke", ManifestVersion: "manifest-huoke",
	})
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"read", "write"}; !reflect.DeepEqual(plan.RequiredTools, want) {
		t.Fatalf("required tools = %#v, want %#v", plan.RequiredTools, want)
	}
	if plan.WriteMode != "runtime_staging" {
		t.Fatalf("write mode = %q, want runtime_staging", plan.WriteMode)
	}
}
