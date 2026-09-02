package main

import (
	"encoding/json"
	"testing"
)

func TestNormalizeHuokeTopicGatewayFinalAnswerUnwrapsOnlyValidFence(t *testing.T) {
	envelope, err := json.Marshal(map[string]any{
		"schemaVersion": huokeTopicResultSchema,
		"taskType":      huokeTopicResultTaskType,
		"skillProfile":  huokeTopicSkillProfile,
		"status":        "succeeded",
		"data": map[string]any{
			"reply": "structured topic guidance",
			"consultationStatePatch": map[string]any{
				"schemaVersion":    "huoke_topic_strategy.state_patch.v1",
				"baseStateVersion": 1,
				"stateVersion":     2,
				"patch":            map[string]any{"currentSubjectId": "final_topic_guidance"},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := json.Marshal(map[string]any{
		"status":       "succeeded",
		"finalAnswer":  "```json\n" + string(envelope) + "\n```",
		"parsedResult": map[string]any{"summary": "preserved"},
	})
	if err != nil {
		t.Fatal(err)
	}
	normalized := normalizeHuokeTopicGatewayFinalAnswer(stdout)
	response := map[string]any{}
	if err := json.Unmarshal(normalized, &response); err != nil {
		t.Fatal(err)
	}
	if response["finalAnswer"] != string(envelope) {
		t.Fatalf("finalAnswer = %#v, want raw envelope", response["finalAnswer"])
	}
	parsed, ok := response["parsedResult"].(map[string]any)
	if !ok || parsed["summary"] != "preserved" {
		t.Fatalf("parsedResult must remain untouched: %#v", response["parsedResult"])
	}
}

func TestNormalizeHuokeTopicGatewayFinalAnswerExtractsValidTrailingEnvelope(t *testing.T) {
	envelope := `{"schemaVersion":"huoke_topic_strategy.result.v1","taskType":"work_ai_huoke_topic_strategy","skillProfile":"huoke_topic_strategy","status":"succeeded","data":{"reply":"structured topic guidance","consultationStatePatch":{"schemaVersion":"huoke_topic_strategy.state_patch.v1","baseStateVersion":1,"stateVersion":2,"patch":{"currentSubjectId":"audience_conversion_target"}}},"assetWriteIntent":null}`
	stdout, err := json.Marshal(map[string]any{
		"status":       "succeeded",
		"finalAnswer":  "## Visible draft\n\nThis text must not hide the valid transport.\n\n" + envelope,
		"parsedResult": map[string]any{"summary": "preserved"},
	})
	if err != nil {
		t.Fatal(err)
	}
	normalized := normalizeHuokeTopicGatewayFinalAnswer(stdout)
	response := map[string]any{}
	if err := json.Unmarshal(normalized, &response); err != nil {
		t.Fatal(err)
	}
	if response["finalAnswer"] != envelope {
		t.Fatalf("finalAnswer = %#v, want trailing envelope", response["finalAnswer"])
	}
	parsed, ok := response["parsedResult"].(map[string]any)
	if !ok || parsed["summary"] != "preserved" {
		t.Fatalf("parsedResult must remain untouched: %#v", response["parsedResult"])
	}
}

func TestNormalizeHuokeTopicGatewayFinalAnswerLeavesUnsafeOrOtherResultsUntouched(t *testing.T) {
	valid := `{"schemaVersion":"huoke_topic_strategy.result.v1","taskType":"work_ai_huoke_topic_strategy","skillProfile":"huoke_topic_strategy","status":"succeeded","data":{"reply":"ok","consultationStatePatch":{"schemaVersion":"huoke_topic_strategy.state_patch.v1"}}}`
	for name, finalAnswer := range map[string]string{
		"plain_markdown":       "## topic guidance",
		"raw_envelope":         valid,
		"prose_before_fence":   "transport note\n```json\n" + valid + "\n```",
		"raw_trailing_text":    "transport note\n" + valid + "\ntransport complete",
		"trailing_text":        "```json\n" + valid + "\n```\ntransport complete",
		"non_huoke_envelope":   "```json\n" + `{"schemaVersion":"viewpoint_germination.result.v2","taskType":"work_ai_faya_germination","skillProfile":"viewpoint_germination","status":"succeeded","data":{"reply":"ok"}}` + "\n```",
		"missing_state_patch":  "```json\n" + `{"schemaVersion":"huoke_topic_strategy.result.v1","taskType":"work_ai_huoke_topic_strategy","skillProfile":"huoke_topic_strategy","status":"succeeded","data":{"reply":"ok"}}` + "\n```",
		"full_state_not_patch": "```json\n" + `{"schemaVersion":"huoke_topic_strategy.result.v1","taskType":"work_ai_huoke_topic_strategy","skillProfile":"huoke_topic_strategy","status":"succeeded","data":{"reply":"ok","consultationState":{}}}` + "\n```",
	} {
		t.Run(name, func(t *testing.T) {
			stdout, err := json.Marshal(map[string]any{"status": "succeeded", "finalAnswer": finalAnswer})
			if err != nil {
				t.Fatal(err)
			}
			if normalized := normalizeHuokeTopicGatewayFinalAnswer(stdout); string(normalized) != string(stdout) {
				t.Fatalf("unsafe result changed: %s", normalized)
			}
		})
	}
}
