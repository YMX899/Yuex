package runtime

import (
	"testing"

	"huahuoai/backend/source/internal/domain"
)

func TestRuntimeToolBudgetForIntentUsesVisualProfileLimit(t *testing.T) {
	visual := runtimeToolBudgetForIntent(domain.TaskIntent{ResolvedTaskType: "work_ai_visual_chat"})
	if visual.MaxToolCalls != 8 || visual.SoftToolCallLimit != 6 || visual.FinalizationReserve != 1 ||
		visual.MaxSearchCalls != 6 || visual.MaxWriteCalls != 0 {
		t.Fatalf("visual tool budget = %#v", visual)
	}
	if err := ValidateRuntimeToolBudget(visual); err != nil {
		t.Fatalf("visual tool budget must remain valid: %v", err)
	}

	general := runtimeToolBudgetForIntent(domain.TaskIntent{ResolvedTaskType: "work_ai_general_chat"})
	if general.MaxToolCalls != 200 || general.SoftToolCallLimit != 160 || general.FinalizationReserve != 10 ||
		general.MaxSearchCalls != 60 || general.MaxWriteCalls != 20 {
		t.Fatalf("non-visual tool budget changed: %#v", general)
	}
}
