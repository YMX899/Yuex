package runtime

import (
	"encoding/json"
	"fmt"
	"strings"
)

func runtimeRecordingPostprocessTask(taskType string) bool {
	switch taskType {
	case "minutes_generation", "summary_generation", "material_deposit_generation":
		return true
	default:
		return false
	}
}

func runtimeOutputContract(plan ProfilePlan) map[string]any {
	contract := map[string]any{
		"schemaVersion": plan.OutputSchemaVersion,
		"finalAnswer":   "json",
	}
	if plan.TaskType == "feed_ai_chat" || plan.TaskType == "work_ai_general_chat" || plan.TaskType == "work_ai_topic_generation" || plan.TaskType == "work_ai_self_media_creation" || plan.TaskType == "work_ai_visual_chat" {
		contract["finalAnswer"] = "text_or_json"
	}
	if runtimeRecordingPostprocessTask(plan.TaskType) {
		contract["finalAnswer"] = "markdown_or_json"
	}
	if plan.ExecutionScope == ScopeDetachedTask && contract["finalAnswer"] == "json" {
		contract["resultPath"] = "output/result.json"
	}
	return contract
}

func stringValue(value any) string {
	if value == nil {
		return ""
	}
	if text, ok := value.(string); ok {
		return text
	}
	return fmt.Sprint(value)
}

func runtimeIntValue(value any) int {
	switch typed := value.(type) {
	case int:
		return typed
	case int32:
		return int(typed)
	case int64:
		return int(typed)
	case float32:
		return int(typed)
	case float64:
		return int(typed)
	case json.Number:
		out, _ := typed.Int64()
		return int(out)
	default:
		return 0
	}
}

func runtimeStringValue(value any) string {
	if value == nil {
		return ""
	}
	if text, ok := value.(string); ok {
		return strings.TrimSpace(text)
	}
	return strings.TrimSpace(fmt.Sprint(value))
}

func runtimeMapValue(value any) map[string]any {
	if typed, ok := value.(map[string]any); ok {
		return typed
	}
	return map[string]any{}
}

func firstRuntimeNonNil(values ...any) any {
	for _, value := range values {
		if value != nil {
			return value
		}
	}
	return nil
}
