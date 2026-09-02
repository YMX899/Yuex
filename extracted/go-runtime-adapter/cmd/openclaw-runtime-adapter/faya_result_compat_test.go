package main

import (
	"encoding/json"
	"testing"
)

func TestNormalizeFayaGatewayFinalAnswerUsesEnvelopeOverTransportSummary(t *testing.T) {
	envelope := `{"schemaVersion":"viewpoint_germination.result.v2","taskType":"work_ai_faya_germination","skillProfile":"viewpoint_germination","status":"succeeded","data":{"reply":"ok"}}`
	stdout, err := json.Marshal(map[string]any{
		"status":       "succeeded",
		"finalAnswer":  "```json\n" + envelope + "\n```",
		"parsedResult": map[string]any{"summary": "not the envelope"},
	})
	if err != nil {
		t.Fatal(err)
	}
	normalized := normalizeFayaGatewayFinalAnswer(stdout)
	response := map[string]any{}
	if err := json.Unmarshal(normalized, &response); err != nil {
		t.Fatal(err)
	}
	parsed, ok := response["parsedResult"].(map[string]any)
	if !ok || parsed["schemaVersion"] != fayaResultSchema || parsed["taskType"] != fayaResultTaskType {
		t.Fatalf("parsedResult=%#v", response["parsedResult"])
	}
}

func TestNormalizeFayaGatewayFinalAnswerLeavesNonFayaResultUntouched(t *testing.T) {
	stdout, err := json.Marshal(map[string]any{
		"status":       "succeeded",
		"finalAnswer":  "A normal non-Faya answer.",
		"parsedResult": map[string]any{"summary": "unchanged"},
	})
	if err != nil {
		t.Fatal(err)
	}
	normalized := normalizeFayaGatewayFinalAnswer(stdout)
	if string(normalized) != string(stdout) {
		t.Fatalf("other runtime response changed: %s", normalized)
	}
}
