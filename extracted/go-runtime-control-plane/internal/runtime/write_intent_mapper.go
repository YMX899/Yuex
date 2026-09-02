package runtime

import "fmt"

type WorkspaceWriteCommandDTO struct {
	WorkspaceID    string           `json:"workspaceId"`
	SourceType     string           `json:"sourceType"`
	IdempotencyKey string           `json:"idempotencyKey"`
	Operations     []map[string]any `json:"operations"`
	TaskID         string           `json:"taskId,omitempty"`
	RunID          string           `json:"runId,omitempty"`
}

type WriteIntentMapper struct{}

// WriteIntentMapper is a deterministic mapper. It adds task/run idempotency
// keys to safe Workspace commands, but Workspace services own job state,
// transactions, queue submission, retry, and permission enforcement.
func NewWriteIntentMapper() WriteIntentMapper {
	return WriteIntentMapper{}
}

func (m WriteIntentMapper) MapAssetWriteIntent(task RuntimeRunCommand, result RuntimeParsedResult) (WorkspaceWriteCommandDTO, bool) {
	if len(result.AssetWriteIntent) == 0 {
		return WorkspaceWriteCommandDTO{}, false
	}
	command, err := m.mapIntent(task, result)
	if err != nil {
		return WorkspaceWriteCommandDTO{}, false
	}
	return command, true
}

func (m WriteIntentMapper) MapProfileMaintenanceIntent(task RuntimeRunCommand, intent map[string]any) (WorkspaceWriteCommandDTO, error) {
	return m.mapProfileMaintenanceIntent(task, intent)
}

func (m WriteIntentMapper) mapProfileMaintenanceIntent(task RuntimeRunCommand, intent map[string]any) (WorkspaceWriteCommandDTO, error) {
	items := mapSlice(intent["profileItems"])
	if len(items) == 0 {
		items = mapSlice(intent["items"])
	}
	hasStructuredItems := len(items) > 0
	for _, key := range []string{"selfDescriptionSignals", "lifeEvents", "viewpoints", "expressions", "preferredExpressions", "synonyms"} {
		if len(mapSlice(intent[key])) > 0 {
			hasStructuredItems = true
			break
		}
	}
	if !hasStructuredItems {
		return WorkspaceWriteCommandDTO{}, fmt.Errorf("AI_RESULT_PARSE_FAILED")
	}
	operation := copyRuntimeMap(intent)
	operation["operation"] = "upsert_profile_items"
	if len(items) > 0 {
		operation["items"] = items
	}
	sourceType := "profile_maintenance"
	if task.TaskType == "profile_deposit" || parserString(intent["source"]) == "work_ai_profile_deposit" {
		sourceType = "work_ai_profile_deposit"
	} else if parserString(intent["source"]) == "feed_ai_profile_deposit" {
		sourceType = "feed_ai_profile_deposit"
	}
	return m.command(task, sourceType, []map[string]any{operation}), nil
}

func (m WriteIntentMapper) mapRecordingMinutesIntent(task RuntimeRunCommand, result RuntimeParsedResult) (WorkspaceWriteCommandDTO, error) {
	intent := result.AssetWriteIntent
	payload := mapValueForParser(intent["payload"])
	if len(payload) == 0 {
		payload = copyRuntimeMap(result.Data)
	}
	if len(payload) == 0 {
		payload = copyRuntimeMap(intent)
	}
	if containsUnsafePath(payload) {
		return WorkspaceWriteCommandDTO{}, fmt.Errorf("WORKSPACE_WRITE_FORBIDDEN")
	}
	recordingID := parserString(intent["recordingId"])
	if recordingID == "" {
		recordingID = parserString(payload["recordingId"])
	}
	return m.command(task, "recording_minutes", []map[string]any{{"operation": "store_recording_minutes", "recordingId": recordingID, "payload": payload}}), nil
}

func (m WriteIntentMapper) mapRecordingSummaryIntent(task RuntimeRunCommand, result RuntimeParsedResult) (WorkspaceWriteCommandDTO, error) {
	intent := result.AssetWriteIntent
	payload := mapValueForParser(intent["payload"])
	if len(payload) == 0 {
		payload = copyRuntimeMap(result.Data)
	}
	if len(payload) == 0 {
		payload = copyRuntimeMap(intent)
	}
	if containsUnsafePath(payload) {
		return WorkspaceWriteCommandDTO{}, fmt.Errorf("WORKSPACE_WRITE_FORBIDDEN")
	}
	recordingID := parserString(intent["recordingId"])
	if recordingID == "" {
		recordingID = parserString(payload["recordingId"])
	}
	return m.command(task, "recording_summary", []map[string]any{{"operation": "store_recording_summary", "recordingId": recordingID, "payload": payload}}), nil
}

func (m WriteIntentMapper) mapTopicResultIntent(task RuntimeRunCommand, intent map[string]any) (WorkspaceWriteCommandDTO, error) {
	return m.command(task, "work_ai_result_index", []map[string]any{{"operation": "work_ai_result_index", "taskId": task.TaskID, "runId": task.RunID, "payload": mapValueForParser(intent["payload"])}}), nil
}

func (m WriteIntentMapper) mapHotspotSuggestionIntent(task RuntimeRunCommand, intent map[string]any) (WorkspaceWriteCommandDTO, error) {
	payload := mapValueForParser(intent["payload"])
	if len(payload) == 0 {
		payload = intent
	}
	if parserString(payload["title"]) == "" && parserString(payload["topic"]) == "" {
		return WorkspaceWriteCommandDTO{}, fmt.Errorf("AI_RESULT_PARSE_FAILED")
	}
	return m.command(task, "hotspot_suggestion", []map[string]any{{"operation": "hotspot_suggestion", "payload": payload}}), nil
}

func (m WriteIntentMapper) rejectUnsupportedIntent(intent map[string]any) error {
	operation := parserString(intent["operation"])
	if operation == "patch_structured_asset" || operation == "raw_file_write" {
		return fmt.Errorf("ASSET_PATCH_NOT_ALLOWED")
	}
	return fmt.Errorf("WORKSPACE_WRITE_FAILED")
}

func (m WriteIntentMapper) mapIntent(task RuntimeRunCommand, result RuntimeParsedResult) (WorkspaceWriteCommandDTO, error) {
	intent := result.AssetWriteIntent
	if containsUnsafePath(intent) {
		return WorkspaceWriteCommandDTO{}, fmt.Errorf("WORKSPACE_WRITE_FORBIDDEN")
	}
	switch parserString(intent["operation"]) {
	case "upsert_profile_items":
		return m.mapProfileMaintenanceIntent(task, intent)
	case "store_recording_minutes", "recording_minutes_candidate":
		return m.mapRecordingMinutesIntent(task, result)
	case "store_recording_summary", "recording_summary_candidate":
		return m.mapRecordingSummaryIntent(task, result)
	case "work_ai_result_index":
		return m.mapTopicResultIntent(task, intent)
	case "hotspot_suggestion":
		return m.mapHotspotSuggestionIntent(task, intent)
	default:
		return WorkspaceWriteCommandDTO{}, m.rejectUnsupportedIntent(intent)
	}
}

func (m WriteIntentMapper) command(task RuntimeRunCommand, sourceType string, operations []map[string]any) WorkspaceWriteCommandDTO {
	for i := range operations {
		operations[i]["sourceTaskId"] = task.TaskID
		operations[i]["sourceRunId"] = task.RunID
		operations[i]["sourceUserId"] = task.UserID
	}
	return WorkspaceWriteCommandDTO{
		WorkspaceID:    task.WorkspaceID,
		SourceType:     sourceType,
		IdempotencyKey: task.RunID + ":" + task.TaskID + ":writeback",
		Operations:     operations,
		TaskID:         task.TaskID,
		RunID:          task.RunID,
	}
}

func copyRuntimeMap(input map[string]any) map[string]any {
	out := map[string]any{}
	for key, value := range input {
		out[key] = value
	}
	return out
}
