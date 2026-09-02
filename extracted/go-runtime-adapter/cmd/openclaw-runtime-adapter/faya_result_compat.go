package main

import (
	"encoding/json"
	"strings"
)

const (
	fayaRuntimeConfigID = "huahuo-faya-germination"
	fayaResultSchema    = "viewpoint_germination.result.v2"
	fayaResultTaskType  = "work_ai_faya_germination"
	fayaSkillProfile    = "viewpoint_germination"
)

// The current Worker consumes parsedResult before finalAnswer for Faya runs.
// Status polling transports only runId, so the strict self-identifying
// envelope, rather than runtimeConfigId, is the safe compatibility selector.
func normalizeFayaGatewayFinalAnswer(stdout []byte) []byte {
	response := map[string]any{}
	if json.Unmarshal(stdout, &response) != nil || strings.TrimSpace(stringValue(response["status"])) != "succeeded" {
		return stdout
	}
	candidate, ok := fayaGatewayEnvelopeCandidate(stringValue(response["finalAnswer"]))
	if !ok {
		return stdout
	}
	envelope := map[string]any{}
	if json.Unmarshal([]byte(candidate), &envelope) != nil ||
		stringValue(envelope["schemaVersion"]) != fayaResultSchema ||
		stringValue(envelope["taskType"]) != fayaResultTaskType ||
		stringValue(envelope["skillProfile"]) != fayaSkillProfile {
		return stdout
	}
	response["parsedResult"] = envelope
	normalized, err := json.Marshal(response)
	if err != nil {
		return stdout
	}
	return normalized
}

func fayaGatewayEnvelopeCandidate(raw string) (string, bool) {
	trimmed := strings.TrimSpace(strings.TrimPrefix(raw, "\ufeff"))
	if strings.HasPrefix(trimmed, "{") {
		return trimmed, true
	}
	markerIndex := strings.Index(strings.ToLower(trimmed), "```json")
	if markerIndex < 0 {
		return "", false
	}
	fenced := trimmed[markerIndex+len("```json"):]
	objectStart := strings.Index(fenced, "{")
	if objectStart < 0 || strings.TrimSpace(fenced[:objectStart]) != "" {
		return "", false
	}
	object, ok := fayaBalancedJSONObjectAt(fenced, objectStart)
	if !ok || strings.TrimSpace(fenced[objectStart+len(object):]) != "```" {
		return "", false
	}
	return object, true
}

func fayaBalancedJSONObjectAt(raw string, start int) (string, bool) {
	depth := 0
	inString := false
	escaped := false
	for index := start; index < len(raw); index++ {
		char := raw[index]
		if inString {
			if escaped {
				escaped = false
				continue
			}
			switch char {
			case '\\':
				escaped = true
			case '"':
				inString = false
			}
			continue
		}
		switch char {
		case '"':
			inString = true
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return raw[start : index+1], true
			}
			if depth < 0 {
				return "", false
			}
		}
	}
	return "", false
}
