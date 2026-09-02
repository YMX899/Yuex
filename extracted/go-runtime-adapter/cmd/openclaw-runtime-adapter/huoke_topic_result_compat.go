package main

import (
	"encoding/json"
	"strings"
)

const (
	huokeTopicResultSchema   = "huoke_topic_strategy.result.v1"
	huokeTopicResultTaskType = "work_ai_huoke_topic_strategy"
	huokeTopicSkillProfile   = "huoke_topic_strategy"
)

// Keep the Worker as the authority for Huoke state validation. This only
// isolates a complete transport envelope from known model wrappers.
func normalizeHuokeTopicGatewayFinalAnswer(stdout []byte) []byte {
	response := map[string]any{}
	if json.Unmarshal(stdout, &response) != nil || strings.TrimSpace(stringValue(response["status"])) != "succeeded" {
		return stdout
	}
	candidate, ok := huokeTopicFencedEnvelopeCandidate(stringValue(response["finalAnswer"]))
	if !ok {
		candidate, ok = huokeTopicTrailingEnvelopeCandidate(stringValue(response["finalAnswer"]))
	}
	if !ok {
		return stdout
	}
	envelope := map[string]any{}
	if json.Unmarshal([]byte(candidate), &envelope) != nil || !validHuokeTopicGatewayEnvelope(envelope) {
		return stdout
	}
	response["finalAnswer"] = candidate
	normalized, err := json.Marshal(response)
	if err != nil {
		return stdout
	}
	return normalized
}

func huokeTopicFencedEnvelopeCandidate(raw string) (string, bool) {
	trimmed := strings.TrimSpace(strings.TrimPrefix(raw, "\ufeff"))
	if !strings.HasPrefix(strings.ToLower(trimmed), "```json") {
		return "", false
	}
	fenced := strings.TrimSpace(trimmed[len("```json"):])
	if !strings.HasSuffix(fenced, "```") {
		return "", false
	}
	envelope := strings.TrimSpace(strings.TrimSuffix(fenced, "```"))
	if !strings.HasPrefix(envelope, "{") || !json.Valid([]byte(envelope)) {
		return "", false
	}
	return envelope, true
}

// Some models emit a user-visible draft before the required transport object.
// Accept only a complete JSON object at the very end; trailing prose or a
// partial object remains invalid and is left for the Worker to reject.
func huokeTopicTrailingEnvelopeCandidate(raw string) (string, bool) {
	trimmed := strings.TrimSpace(strings.TrimPrefix(raw, "\ufeff"))
	if trimmed == "" || json.Valid([]byte(trimmed)) {
		return "", false
	}
	for index := strings.LastIndex(trimmed, "{"); index > 0; index = strings.LastIndex(trimmed[:index], "{") {
		if strings.TrimSpace(trimmed[:index]) == "" {
			continue
		}
		candidate := strings.TrimSpace(trimmed[index:])
		if json.Valid([]byte(candidate)) {
			return candidate, true
		}
	}
	return "", false
}

func validHuokeTopicGatewayEnvelope(envelope map[string]any) bool {
	if stringValue(envelope["schemaVersion"]) != huokeTopicResultSchema ||
		stringValue(envelope["taskType"]) != huokeTopicResultTaskType ||
		stringValue(envelope["skillProfile"]) != huokeTopicSkillProfile ||
		strings.TrimSpace(stringValue(envelope["status"])) != "succeeded" {
		return false
	}
	data, ok := envelope["data"].(map[string]any)
	if !ok || strings.TrimSpace(stringValue(data["reply"])) == "" {
		return false
	}
	if _, hasFullState := data["consultationState"]; hasFullState {
		return false
	}
	if _, hasLegacyDelta := data["consultationStateDelta"]; hasLegacyDelta {
		return false
	}
	patch, ok := data["consultationStatePatch"].(map[string]any)
	return ok && len(patch) > 0
}
