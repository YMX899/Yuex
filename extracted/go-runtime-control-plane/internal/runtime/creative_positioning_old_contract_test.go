package runtime

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestTopicGenerationTopLevelPromotionDoesNotForwardLegacyContentLineID(t *testing.T) {
	raw, err := json.Marshal(map[string]any{
		"topics":                []any{map[string]any{"title": "Canonical topic"}},
		"creativePositioningId": "creative_positioning_current",
		"contentLineId":         "creative_positioning_stale",
	})
	if err != nil {
		t.Fatalf("marshal topic result: %v", err)
	}
	data, ok := topicGenerationTopLevelData(string(raw))
	if !ok || data["creativePositioningId"] != "creative_positioning_current" {
		t.Fatalf("canonical topic promotion mismatch: %#v", data)
	}
	if _, legacy := data["contentLineId"]; legacy {
		t.Fatalf("topic promotion forwarded legacy contentLineId: %#v", data)
	}
}

func TestRuntimeParserRejectsNestedLegacyContentLineID(t *testing.T) {
	plan, err := NewAgentProfileResolver().Resolve("work_ai_topic_generation")
	if err != nil {
		t.Fatalf("resolve topic plan: %v", err)
	}
	raw, err := json.Marshal(map[string]any{
		"schemaVersion": plan.OutputSchemaVersion,
		"taskType":      plan.TaskType,
		"skillProfile":  plan.SkillProfile,
		"status":        "succeeded",
		"data": map[string]any{
			"topics":        []any{map[string]any{"title": "Legacy topic"}},
			"contentLineId": "creative_positioning_legacy_nested",
		},
	})
	if err != nil {
		t.Fatalf("marshal legacy topic result: %v", err)
	}
	_, parseErr := NewOutputParser().Parse(plan, RuntimeRunRecord{}, RuntimeRunResult{Status: "succeeded", FinalAnswer: string(raw)})
	if parseErr == nil || !strings.Contains(parseErr.Error(), "RUNTIME_INPUT_INVALID") {
		t.Fatalf("legacy runtime positioning key must fail closed, got %v", parseErr)
	}
}

func TestHotspotSuggestionParserEmitsCanonicalCreativePositioningOnly(t *testing.T) {
	parser := NewOutputParser()
	suggestion, err := parser.parseHotspotSuggestion(map[string]any{
		"suggestionId":          "suggestion_canonical_positioning",
		"creativePositioningId": "creative_positioning_current",
		"payload":               map[string]any{"title": "Canonical hotspot"},
	})
	if err != nil {
		t.Fatalf("parse canonical hotspot suggestion: %v", err)
	}
	raw, err := json.Marshal(suggestion)
	if err != nil {
		t.Fatalf("marshal canonical hotspot suggestion: %v", err)
	}
	if suggestion.CreativePositioningID != "creative_positioning_current" || strings.Contains(string(raw), "contentLineId") {
		t.Fatalf("hotspot suggestion emitted old contract: %s", raw)
	}

	if _, err := parser.parseHotspotSuggestion(map[string]any{
		"contentLineId": "creative_positioning_legacy",
		"payload":       map[string]any{"title": "Legacy hotspot"},
	}); err == nil || !strings.Contains(err.Error(), "RUNTIME_INPUT_INVALID") {
		t.Fatalf("legacy hotspot suggestion must fail closed, got %v", err)
	}
}
