package runtime

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

type RuntimeParsedResult struct {
	SchemaVersion    string         `json:"schemaVersion"`
	TaskID           string         `json:"taskId"`
	RunID            string         `json:"runId"`
	TaskType         string         `json:"taskType"`
	SkillProfile     string         `json:"skillProfile"`
	Status           string         `json:"status"`
	Error            map[string]any `json:"error,omitempty"`
	Data             map[string]any `json:"data,omitempty"`
	AssetWriteIntent map[string]any `json:"assetWriteIntent,omitempty"`
	ParsedType       string         `json:"parsedType,omitempty"`
}

type TopicGenerationResult struct {
	Topics []map[string]any `json:"topics"`
}

type HotspotSuggestion struct {
	SuggestionID          string         `json:"suggestionId"`
	CreativePositioningID string         `json:"creativePositioningId,omitempty"`
	Payload               map[string]any `json:"payload"`
}

type OutputParser struct{}

// OutputParser is deterministic and side-effect free. It validates Runtime
// output and write intents, while Orchestrator and Workspace services own task
// status, idempotency, transactions, and queued write execution.
func NewOutputParser() OutputParser {
	return OutputParser{}
}

func (p OutputParser) ParseResultJSON(workspaceDir string, expected ProfilePlan, runRecords ...RuntimeRunRecord) (RuntimeParsedResult, error) {
	data, err := os.ReadFile(filepath.Join(workspaceDir, "output", "result.json"))
	if err != nil {
		return RuntimeParsedResult{}, parseError("AI_RESULT_PARSE_FAILED")
	}
	runRecord := RuntimeRunRecord{}
	if len(runRecords) > 0 {
		runRecord = runRecords[0]
	}
	return p.parse(expected, runRecord, RuntimeRunResult{RunID: runRecord.RunID, Status: "succeeded", FinalAnswer: string(data)})
}

func (p OutputParser) ParseFinalAnswer(answer string, expected ProfilePlan) (RuntimeParsedResult, error) {
	return p.parse(expected, RuntimeRunRecord{}, RuntimeRunResult{Status: "succeeded", FinalAnswer: answer})
}

// ParseAgentRunResultForPlan binds terminal parsing to the immutable plan that
// was dispatched to Runtime. It never derives the parser from AiTask.taskType,
// which is only an APP compatibility projection.
func (p OutputParser) ParseAgentRunResultForPlan(result map[string]any, plan AgentRunPlan, runID, taskID string) (map[string]any, error) {
	expected, err := plan.TerminalOutputProfile(runID)
	if err != nil {
		return nil, err
	}
	return p.ParseAgentRunResult(result, expected, runID, taskID)
}

func (p OutputParser) ParseAgentRunResult(result map[string]any, expected ProfilePlan, runID, taskID string) (map[string]any, error) {
	if runtimeHuokeTopicResultContract(expected) {
		answer := strings.TrimSpace(stringValue(result["finalAnswer"]))
		if answer == "" {
			parsed, ok := result["parsedResult"].(map[string]any)
			if !ok || len(parsed) == 0 {
				return nil, parseError("AI_RESULT_PARSE_FAILED")
			}
			raw, err := json.Marshal(parsed)
			if err != nil {
				return nil, parseError("AI_RESULT_PARSE_FAILED")
			}
			answer = string(raw)
		}
		answer = repairHuokeTopicDataReplyObjectTypo(answer)
		if markdownReply, ok := huokeTopicPlainMarkdownFallbackCandidate(answer); ok {
			parsed := RuntimeParsedResult{
				SchemaVersion: expected.OutputSchemaVersion,
				TaskID:        taskID,
				RunID:         runID,
				TaskType:      expected.TaskType,
				SkillProfile:  expected.SkillProfile,
				Status:        "succeeded",
				Data:          map[string]any{"reply": markdownReply},
				ParsedType:    "huoke_topic_strategy",
			}
			if err := validateRuntimeEnvelope(expected, RuntimeRunRecord{RunID: runID, TaskID: taskID}, parsed); err != nil {
				return nil, err
			}
			if _, err := p.parseGeneralChat(parsed.Data); err != nil {
				return nil, err
			}
			return RuntimeParsedResultMap(parsed), nil
		}
		validated, err := p.Parse(expected, RuntimeRunRecord{RunID: runID, TaskID: taskID}, RuntimeRunResult{RunID: runID, Status: "succeeded", FinalAnswer: answer})
		if err != nil {
			return nil, err
		}
		return RuntimeParsedResultMap(validated), nil
	}
	if runtimeFayaV2ResultContract(expected) {
		answer := strings.TrimSpace(stringValue(result["finalAnswer"]))
		if answer != "" {
			validated, err := p.Parse(expected, RuntimeRunRecord{RunID: runID, TaskID: taskID}, RuntimeRunResult{RunID: runID, Status: "succeeded", FinalAnswer: answer})
			if err != nil {
				return nil, err
			}
			return RuntimeParsedResultMap(validated), nil
		}
	}
	if parsed, ok := result["parsedResult"].(map[string]any); ok && len(parsed) > 0 {
		if runtimeFayaV2ResultContract(expected) {
			raw, err := json.Marshal(parsed)
			if err != nil {
				return nil, parseError("AI_RESULT_PARSE_FAILED")
			}
			validated, err := p.Parse(expected, RuntimeRunRecord{RunID: runID, TaskID: taskID}, RuntimeRunResult{RunID: runID, Status: "succeeded", FinalAnswer: string(raw)})
			if err != nil {
				return nil, err
			}
			return RuntimeParsedResultMap(validated), nil
		}
		return parsed, nil
	}
	answer := strings.TrimSpace(stringValue(result["finalAnswer"]))
	if answer == "" {
		return nil, parseError("AI_RESULT_PARSE_FAILED")
	}
	parsed, err := p.Parse(expected, RuntimeRunRecord{RunID: runID, TaskID: taskID}, RuntimeRunResult{RunID: runID, Status: "succeeded", FinalAnswer: answer})
	if err != nil {
		return nil, err
	}
	return RuntimeParsedResultMap(parsed), nil
}

// repairHuokeTopicDataReplyObjectTypo accepts one observed transport typo from
// the Huoke model path: `"data":"reply":...` instead of
// `"data":{"reply":...`. The repaired document must still be valid JSON and
// subsequently pass the complete frozen Huoke envelope and state-patch checks.
// It is deliberately not a general JSON repair mechanism.
func repairHuokeTopicDataReplyObjectTypo(raw string) string {
	trimmed := strings.TrimSpace(strings.TrimPrefix(raw, "\ufeff"))
	const malformed = `"data":"reply":`
	if strings.Count(trimmed, malformed) != 1 {
		return raw
	}
	repaired := strings.Replace(trimmed, malformed, `"data":{"reply":`, 1)
	if !json.Valid([]byte(repaired)) {
		return raw
	}
	return repaired
}

// huokeTopicPlainMarkdownFallbackCandidate accepts only a visible reply that
// cannot be mistaken for the state-bearing result protocol. State is then
// carried forward by WorkAIService instead of being inferred from prose.
func huokeTopicPlainMarkdownFallbackCandidate(raw string) (string, bool) {
	text := strings.TrimSpace(strings.TrimPrefix(raw, "\ufeff"))
	if text == "" || runtimeTextLooksLikeAgentFailure(text) || runtimeAnswerLooksLikeJSON(text) {
		return "", false
	}
	for _, candidate := range runtimeJSONCandidates(text) {
		payload := map[string]any{}
		if json.Unmarshal([]byte(candidate), &payload) == nil && len(payload) > 0 {
			return "", false
		}
	}
	normalized := strings.ToLower(text)
	for _, marker := range []string{
		"```json",
		"\"schemaversion\"",
		"\"tasktype\"",
		"\"skillprofile\"",
		"\"consultationstate\"",
		"consultationstatepatch",
		"statepatch",
		"\"stateversion\"",
		"\"assetwriteintent\"",
		"huoke_topic_strategy",
	} {
		if strings.Contains(normalized, marker) {
			return "", false
		}
	}
	return text, true
}

func (p OutputParser) Parse(task ProfilePlan, runRecord RuntimeRunRecord, runtimeResult RuntimeRunResult) (RuntimeParsedResult, error) {
	return p.parse(task, runRecord, runtimeResult)
}

func (p OutputParser) parse(task ProfilePlan, runRecord RuntimeRunRecord, runtimeResult RuntimeRunResult) (RuntimeParsedResult, error) {
	if runtimeResult.Status != "" && runtimeResult.Status != "succeeded" {
		return RuntimeParsedResult{Status: "failed", Error: map[string]any{"code": runtimeResult.ErrorCode}}, nil
	}
	if runtimeHuokeTopicResultContract(task) {
		strictEnvelope, ok := strictHuokeTopicEnvelopeJSON(runtimeResult.FinalAnswer)
		if !ok {
			return RuntimeParsedResult{}, parseError("AI_RESULT_PARSE_FAILED")
		}
		runtimeResult.FinalAnswer = strictEnvelope
	}
	if runtimeFayaV2ResultContract(task) {
		strictEnvelope, ok := strictRuntimeEnvelopeJSON(runtimeResult.FinalAnswer)
		if !ok {
			return RuntimeParsedResult{}, parseError("AI_RESULT_PARSE_FAILED")
		}
		runtimeResult.FinalAnswer = strictEnvelope
	}
	huokeQuotedDraft, huokeQuotedDraftOK := runtimeHuokeQuotedDraftCandidate(task, runtimeResult.FinalAnswer)
	if runtimeHuokeResultContract(task) && !huokeQuotedDraftOK && !runtimeHuokePlainQuestion(task, runtimeResult.FinalAnswer) {
		return RuntimeParsedResult{}, parseError("AI_RESULT_PARSE_FAILED")
	}
	if huokeQuotedDraftOK {
		runtimeResult.FinalAnswer = huokeQuotedDraft
	}
	if runtimeHuokeTopicResultContract(task) && !runtimeAnswerLooksLikeJSON(runtimeResult.FinalAnswer) {
		return RuntimeParsedResult{}, parseError("AI_RESULT_PARSE_FAILED")
	}
	if runtimeFayaV2ResultContract(task) && !runtimeAnswerLooksLikeJSON(runtimeResult.FinalAnswer) {
		return RuntimeParsedResult{}, parseError("AI_RESULT_PARSE_FAILED")
	}
	if runtimeHuokeTopicUnsafeMixedEnvelope(task, runtimeResult.FinalAnswer) {
		return RuntimeParsedResult{}, parseError("AI_RESULT_PARSE_FAILED")
	}
	var result RuntimeParsedResult
	if rensheResult, ok := rensheThoughtsResponseRuntimeResult(task, runtimeResult.FinalAnswer); ok {
		result = rensheResult
	} else if textResult, ok := textArtifactRuntimeResult(task, runtimeResult.FinalAnswer); ok && (!runtimeAnswerLooksLikeJSON(runtimeResult.FinalAnswer) || huokeQuotedDraftOK) {
		result = textResult
	} else if runtimeHuokeTopicResultContract(task) || runtimeFayaV2ResultContract(task) {
		if err := unmarshalStrictRuntimeParsedResult(runtimeResult.FinalAnswer, &result); err != nil {
			return RuntimeParsedResult{}, parseError("AI_RESULT_PARSE_FAILED")
		}
	} else if err := unmarshalRuntimeParsedResult([]byte(runtimeResult.FinalAnswer), &result); err != nil {
		if runtimeAnswerLooksLikeJSON(runtimeResult.FinalAnswer) {
			return RuntimeParsedResult{}, parseError("AI_RESULT_PARSE_FAILED")
		}
		textResult, ok := textArtifactRuntimeResult(task, runtimeResult.FinalAnswer)
		if !ok {
			return RuntimeParsedResult{}, parseError("AI_RESULT_PARSE_FAILED")
		}
		result = textResult
	}
	result = promoteTopicGenerationTopLevelData(task, runtimeResult.FinalAnswer, result)
	if runtimeLegacyContentLineIDPresent(result.Data) || runtimeLegacyContentLineIDPresent(result.AssetWriteIntent) {
		return RuntimeParsedResult{}, parseError("RUNTIME_INPUT_INVALID")
	}
	if runtimeTopicPartialStatusAccepted(task, result) {
		if result.Data == nil {
			result.Data = map[string]any{}
		}
		if _, ok := result.Data["providerStatus"]; !ok {
			result.Data["providerStatus"] = result.Status
		}
		result.Status = "succeeded"
	}
	if err := validateRuntimeEnvelope(task, runRecord, result); err != nil {
		return RuntimeParsedResult{}, err
	}
	result = canonicalizeRuntimeEnvelope(task, runRecord, result)
	if runRecord.TaskID != "" {
		result.TaskID = runRecord.TaskID
	}
	if runRecord.RunID != "" {
		result.RunID = runRecord.RunID
	}
	if len(result.AssetWriteIntent) > 0 {
		if err := p.validateAssetWriteIntent(result.AssetWriteIntent); err != nil {
			return RuntimeParsedResult{}, err
		}
	}
	switch task.PromptTemplateID + ":" + task.OutputSchemaVersion {
	case "work_ai.topic_generation.v1:topic_generation.result.v1":
		if _, err := p.parseTopicGeneration(result.Data); err != nil {
			return RuntimeParsedResult{}, err
		}
		result.ParsedType = "topic_generation"
	case "work_ai.general_chat.v1:general_chat.result.v1":
		if _, err := p.parseGeneralChat(result.Data); err != nil {
			return RuntimeParsedResult{}, err
		}
		result.ParsedType = "general_chat"
	case "feed_ai.positioning_profile_builder.v1:positioning_profile_builder.result.v1":
		if _, err := p.parsePositioningProfileBuilder(result.Data); err != nil {
			return RuntimeParsedResult{}, err
		}
		result.ParsedType = "positioning_profile_builder"
	case "work_ai.renshe_content.v1:renshe_content_creation.result.v1":
		if _, err := p.parseGeneralChat(result.Data); err != nil {
			return RuntimeParsedResult{}, err
		}
		result.ParsedType = "renshe_content_creation"
	case "work_ai.huoke_content.v1:huoke_content_creation.result.v1":
		if _, err := p.parseGeneralChat(result.Data); err != nil {
			return RuntimeParsedResult{}, err
		}
		result.ParsedType = "huoke_content_creation"
	case "work_ai.huoke_topic_strategy.v1:huoke_topic_strategy.result.v1":
		if _, err := p.parseGeneralChat(result.Data); err != nil {
			return RuntimeParsedResult{}, err
		}
		update, present, err := ParseHuokeTopicStateUpdate(result.Data)
		if err != nil || !present {
			return RuntimeParsedResult{}, parseError("AI_RESULT_PARSE_FAILED")
		}
		if err := ValidateHuokeTopicExecutionAudit(result.Data, update); err != nil {
			return RuntimeParsedResult{}, parseError("AI_RESULT_PARSE_FAILED")
		}
		result.ParsedType = "huoke_topic_strategy"
	case "work_ai.self_media_creation.v1:self_media_creation_advisor.result.v1":
		if _, err := p.parseGeneralChat(result.Data); err != nil {
			return RuntimeParsedResult{}, err
		}
		result.ParsedType = "self_media_creation"
	case "work_ai.visual_chat.v1:visual_chat_assistant.result.v1":
		if _, err := p.parseGeneralChat(result.Data); err != nil {
			return RuntimeParsedResult{}, err
		}
		result.ParsedType = "visual_chat"
	case "work_ai.faya_germination.v1:viewpoint_germination.result.v1":
		normalizeFayaGerminationReplyData(result.Data)
		if _, err := p.parseGeneralChat(result.Data); err != nil {
			return RuntimeParsedResult{}, err
		}
		result.ParsedType = "viewpoint_germination"
	case "work_ai.faya_germination.v2:viewpoint_germination.result.v2":
		if err := normalizeFayaGerminationV2(&result); err != nil {
			return RuntimeParsedResult{}, err
		}
		result.ParsedType = "viewpoint_germination"
	case "feed_ai.profile_maintenance.v1:profile_maintenance.result.v1":
		if _, err := p.parseProfileMaintenance(result.Data); err != nil {
			return RuntimeParsedResult{}, err
		}
		result.ParsedType = "profile_maintenance"
	case "recording.meeting_minutes.v1:meeting_minutes.result.v1":
		if _, err := p.parseMeetingMinutes(result.Data); err != nil {
			return RuntimeParsedResult{}, err
		}
		result.ParsedType = "meeting_minutes"
	case "recording.asset_summary.v1:asset_summary.result.v1":
		if _, err := p.parseAssetSummary(result.Data); err != nil {
			return RuntimeParsedResult{}, err
		}
		result.ParsedType = "asset_summary"
	case "material.deposit.v1:material_deposit.result.v1":
		if _, err := p.parseMaterialDeposit(result.Data); err != nil {
			return RuntimeParsedResult{}, err
		}
		result.ParsedType = "material_deposit"
	case "hotspot.suggestion.v1:hotspot_suggestion.result.v1":
		if _, err := p.parseHotspotSuggestion(result.Data); err != nil {
			return RuntimeParsedResult{}, err
		}
		result.ParsedType = "hotspot_suggestion"
	default:
		if runtimeDynamicGenericResultContract(task) {
			if _, err := p.parseGeneralChat(result.Data); err != nil {
				return RuntimeParsedResult{}, err
			}
			result.ParsedType = "dynamic_generic"
			break
		}
		return RuntimeParsedResult{}, parseError("SKILL_TASK_MISMATCH")
	}
	return result, nil
}

func unmarshalStrictRuntimeParsedResult(raw string, result *RuntimeParsedResult) error {
	trimmed := strings.TrimSpace(strings.TrimPrefix(raw, "\ufeff"))
	if trimmed == "" || trimmed[0] != '{' {
		return parseError("AI_RESULT_PARSE_FAILED")
	}
	var parsed RuntimeParsedResult
	if err := json.Unmarshal([]byte(trimmed), &parsed); err != nil {
		return err
	}
	if !runtimeParsedResultHasRecognizedFields(parsed) {
		return parseError("AI_RESULT_PARSE_FAILED")
	}
	*result = parsed
	return nil
}

func unmarshalRuntimeParsedResult(raw []byte, result *RuntimeParsedResult) error {
	for _, candidate := range runtimeJSONCandidates(string(raw)) {
		if err := unmarshalRuntimeParsedResultCandidate(candidate, result); err == nil {
			return nil
		}
		repaired := repairUnescapedJSONQuotes(candidate)
		if repaired != candidate {
			if err := unmarshalRuntimeParsedResultCandidate(repaired, result); err == nil {
				return nil
			}
		}
		promoted := promoteNestedAssetWriteIntent(repaired)
		if promoted != repaired {
			if err := unmarshalRuntimeParsedResultCandidate(promoted, result); err == nil {
				return nil
			}
		}
	}
	return parseError("AI_RESULT_PARSE_FAILED")
}

func RuntimeParsedResultMap(parsed RuntimeParsedResult) map[string]any {
	out := map[string]any{
		"schemaVersion": parsed.SchemaVersion,
		"taskId":        parsed.TaskID,
		"runId":         parsed.RunID,
		"taskType":      parsed.TaskType,
		"skillProfile":  parsed.SkillProfile,
		"status":        parsed.Status,
	}
	if parsed.Error != nil {
		out["error"] = parsed.Error
	}
	if parsed.Data != nil {
		out["data"] = parsed.Data
	}
	if parsed.AssetWriteIntent != nil {
		out["assetWriteIntent"] = parsed.AssetWriteIntent
	}
	if parsed.ParsedType != "" {
		out["parsedType"] = parsed.ParsedType
	}
	return out
}

func unmarshalRuntimeParsedResultCandidate(raw string, result *RuntimeParsedResult) error {
	var parsed RuntimeParsedResult
	if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
		return err
	}
	if !runtimeParsedResultHasRecognizedFields(parsed) {
		if unwrapped, ok := unwrapRuntimeSchemaKeyResult(raw); ok {
			parsed = unwrapped
		}
	}
	*result = parsed
	return nil
}

func runtimeParsedResultHasRecognizedFields(result RuntimeParsedResult) bool {
	return result.SchemaVersion != "" ||
		result.TaskID != "" ||
		result.RunID != "" ||
		result.TaskType != "" ||
		result.SkillProfile != "" ||
		result.Status != "" ||
		len(result.Error) > 0 ||
		len(result.Data) > 0 ||
		len(result.AssetWriteIntent) > 0 ||
		result.ParsedType != ""
}

func unwrapRuntimeSchemaKeyResult(raw string) (RuntimeParsedResult, bool) {
	var wrapped map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &wrapped); err != nil || len(wrapped) != 1 {
		return RuntimeParsedResult{}, false
	}
	for key, value := range wrapped {
		if !strings.HasSuffix(strings.TrimSpace(key), ".result.v1") && !strings.HasSuffix(strings.TrimSpace(key), ".result.v2") {
			return RuntimeParsedResult{}, false
		}
		var parsed RuntimeParsedResult
		if err := json.Unmarshal(value, &parsed); err != nil {
			return RuntimeParsedResult{}, false
		}
		if !runtimeParsedResultHasRecognizedFields(parsed) {
			return RuntimeParsedResult{}, false
		}
		if parsed.SchemaVersion == "" {
			parsed.SchemaVersion = strings.TrimSpace(key)
		}
		return parsed, true
	}
	return RuntimeParsedResult{}, false
}

func textArtifactRuntimeResult(task ProfilePlan, raw string) (RuntimeParsedResult, bool) {
	text := strings.TrimSpace(strings.TrimPrefix(raw, "\ufeff"))
	if text == "" || runtimeTextLooksLikeAgentFailure(text) {
		return RuntimeParsedResult{}, false
	}
	if runtimeDynamicGenericResultContract(task) {
		return RuntimeParsedResult{
			Status: "succeeded", Data: map[string]any{"reply": text}, ParsedType: "dynamic_generic",
		}, true
	}
	switch task.PromptTemplateID + ":" + task.OutputSchemaVersion {
	case "work_ai.topic_generation.v1:topic_generation.result.v1":
		return RuntimeParsedResult{
			Status: "succeeded",
			Data: map[string]any{
				"reply":        text,
				"artifactText": text,
			},
			ParsedType: "topic_generation",
		}, true
	case "work_ai.general_chat.v1:general_chat.result.v1":
		return RuntimeParsedResult{
			Status: "succeeded",
			Data: map[string]any{
				"reply": text,
			},
			ParsedType: "general_chat",
		}, true
	case "feed_ai.positioning_profile_builder.v1:positioning_profile_builder.result.v1":
		return RuntimeParsedResult{
			Status: "succeeded",
			Data: map[string]any{
				"reply":         text,
				"replyMarkdown": text,
			},
			ParsedType: "positioning_profile_builder",
		}, true
	case "work_ai.renshe_content.v1:renshe_content_creation.result.v1":
		return RuntimeParsedResult{
			Status: "succeeded",
			Data: map[string]any{
				"reply": text,
			},
			ParsedType: "renshe_content_creation",
		}, true
	case "work_ai.huoke_content.v1:huoke_content_creation.result.v1":
		return RuntimeParsedResult{
			Status: "succeeded",
			Data: map[string]any{
				"reply": text,
			},
			ParsedType: "huoke_content_creation",
		}, true
	case "work_ai.self_media_creation.v1:self_media_creation_advisor.result.v1":
		return RuntimeParsedResult{
			Status: "succeeded",
			Data: map[string]any{
				"reply": text,
			},
			ParsedType: "self_media_creation",
		}, true
	case "work_ai.visual_chat.v1:visual_chat_assistant.result.v1":
		return RuntimeParsedResult{
			Status: "succeeded",
			Data: map[string]any{
				"reply": text,
			},
			ParsedType: "visual_chat",
		}, true
	case "work_ai.faya_germination.v1:viewpoint_germination.result.v1":
		return RuntimeParsedResult{
			Status: "succeeded",
			Data: map[string]any{
				"reply": text,
			},
			ParsedType: "viewpoint_germination",
		}, true
	case "feed_ai.profile_maintenance.v1:profile_maintenance.result.v1":
		return RuntimeParsedResult{
			Status: "succeeded",
			Data: map[string]any{
				"reply": text,
				"depositSummary": map[string]any{
					"profileUpdated":             false,
					"selfDescriptionSignalCount": 0,
					"eventCount":                 0,
					"viewpointCount":             0,
					"expressionCount":            0,
					"preferredExpressionCount":   0,
					"synonymCount":               0,
				},
			},
			ParsedType: "profile_maintenance",
		}, true
	case "recording.meeting_minutes.v1:meeting_minutes.result.v1":
		if runtimeTextLooksLikeCompletionOnly(text) {
			return RuntimeParsedResult{}, false
		}
		return RuntimeParsedResult{
			Status: "succeeded",
			Data: map[string]any{
				"minutesMarkdown": text,
				"minutes":         markdownMinutesPayload(text),
			},
			ParsedType: "meeting_minutes",
		}, true
	case "recording.asset_summary.v1:asset_summary.result.v1":
		if runtimeTextLooksLikeCompletionOnly(text) {
			return RuntimeParsedResult{}, false
		}
		return RuntimeParsedResult{
			Status: "succeeded",
			Data: map[string]any{
				"summary":         markdownSummaryText(text),
				"summaryMarkdown": text,
			},
			ParsedType: "asset_summary",
		}, true
	case "material.deposit.v1:material_deposit.result.v1":
		if runtimeTextLooksLikeCompletionOnly(text) {
			return RuntimeParsedResult{}, false
		}
		return RuntimeParsedResult{
			Status: "succeeded",
			Data: map[string]any{
				"depositMarkdown": text,
			},
			ParsedType: "material_deposit",
		}, true
	default:
		return RuntimeParsedResult{}, false
	}
}

// rensheThoughtsResponseRuntimeResult accepts the only non-envelope JSON shape
// observed on the Renshe model path. The provider emits hidden reasoning in
// thoughts and the user-visible Markdown in response. It is deliberately
// scoped to Renshe and rejects extra fields so JSON from another contract can
// never be projected as a successful chat reply.
func rensheThoughtsResponseRuntimeResult(task ProfilePlan, raw string) (RuntimeParsedResult, bool) {
	if task.TaskType != "work_ai_renshe_content" ||
		task.SkillProfile != "renshe_content_creation" ||
		task.PromptTemplateID != "work_ai.renshe_content.v1" ||
		task.OutputSchemaVersion != "renshe_content_creation.result.v1" {
		return RuntimeParsedResult{}, false
	}

	text := strings.TrimSpace(strings.TrimPrefix(raw, "\ufeff"))
	if text == "" || !strings.HasPrefix(text, "{") {
		return RuntimeParsedResult{}, false
	}

	var payload map[string]json.RawMessage
	if err := json.Unmarshal([]byte(text), &payload); err != nil || len(payload) == 0 || len(payload) > 2 {
		return RuntimeParsedResult{}, false
	}
	responseRaw, ok := payload["response"]
	if !ok {
		return RuntimeParsedResult{}, false
	}
	for key, value := range payload {
		if key != "response" && key != "thoughts" {
			return RuntimeParsedResult{}, false
		}
		var textValue string
		if err := json.Unmarshal(value, &textValue); err != nil {
			return RuntimeParsedResult{}, false
		}
	}

	var response string
	if err := json.Unmarshal(responseRaw, &response); err != nil {
		return RuntimeParsedResult{}, false
	}
	response = strings.TrimSpace(response)
	if response == "" || runtimeAnswerLooksLikeJSON(response) {
		return RuntimeParsedResult{}, false
	}
	return textArtifactRuntimeResult(task, response)
}

func promoteTopicGenerationTopLevelData(task ProfilePlan, raw string, result RuntimeParsedResult) RuntimeParsedResult {
	if task.PromptTemplateID+":"+task.OutputSchemaVersion != "work_ai.topic_generation.v1:topic_generation.result.v1" || len(result.Data) > 0 {
		return result
	}
	for _, candidate := range runtimeJSONCandidates(raw) {
		if data, ok := topicGenerationTopLevelData(candidate); ok {
			result.Data = data
			return result
		}
		repaired := repairUnescapedJSONQuotes(candidate)
		if repaired != candidate {
			if data, ok := topicGenerationTopLevelData(repaired); ok {
				result.Data = data
				return result
			}
		}
	}
	return result
}

func topicGenerationTopLevelData(raw string) (map[string]any, bool) {
	var payload map[string]any
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		return nil, false
	}
	if len(mapSlice(payload["topics"])) == 0 {
		return nil, false
	}
	data := map[string]any{}
	for _, key := range []string{
		"topics",
		"reply",
		"artifactText",
		"text",
		"creativePositioningId",
		"materialScope",
		"hotspotId",
		"hotspotSuggestionId",
		"note",
		"generatedAt",
	} {
		if value, ok := payload[key]; ok {
			data[key] = value
		}
	}
	return data, true
}

func runtimeTopicPartialStatusAccepted(task ProfilePlan, result RuntimeParsedResult) bool {
	if task.PromptTemplateID+":"+task.OutputSchemaVersion != "work_ai.topic_generation.v1:topic_generation.result.v1" {
		return false
	}
	if strings.ToLower(strings.TrimSpace(result.Status)) != "partial" {
		return false
	}
	return len(mapSlice(result.Data["topics"])) > 0
}

func runtimeFayaNoViableSeedStatusAccepted(task ProfilePlan, result RuntimeParsedResult) bool {
	if task.PromptTemplateID+":"+task.OutputSchemaVersion != "work_ai.faya_germination.v1:viewpoint_germination.result.v1" && !runtimeFayaV2ResultContract(task) {
		return false
	}
	return strings.ToLower(strings.TrimSpace(result.Status)) == "no_viable_seed" && strings.TrimSpace(parserString(result.Data["reply"])) != ""
}

// runtimeDynamicGenericResultContract is the explicit generic reply protocol
// for an active catalog Skill that has no specialized parser. Its identity is
// frozen in AgentRunPlan.TerminalOutput and still binds task/skill/schema.
func runtimeDynamicGenericResultContract(task ProfilePlan) bool {
	return safeCatalogIdentifier(task.TaskType) && safeCatalogIdentifier(task.SkillProfile) &&
		task.PromptTemplateID == "dynamic."+task.SkillProfile+".v1" &&
		task.PromptTemplateVersion == "v1.0.0" &&
		task.OutputSchemaVersion == task.SkillProfile+".result.v1"
}

func runtimeAnswerLooksLikeJSON(raw string) bool {
	trimmed := strings.TrimSpace(strings.TrimPrefix(raw, "\ufeff"))
	if strings.HasPrefix(trimmed, "```json") || strings.HasPrefix(trimmed, "```JSON") {
		return true
	}
	return strings.HasPrefix(trimmed, "{")
}

func strictRuntimeEnvelopeJSON(raw string) (string, bool) {
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
	object, ok := balancedJSONObjectAt(fenced, objectStart)
	if !ok || strings.TrimSpace(fenced[objectStart+len(object):]) != "```" {
		return "", false
	}
	return object, true
}

func strictHuokeTopicEnvelopeJSON(raw string) (string, bool) {
	trimmed := strings.TrimSpace(strings.TrimPrefix(raw, "\ufeff"))
	if strings.HasPrefix(trimmed, "{") {
		return trimmed, true
	}
	if !strings.HasPrefix(strings.ToLower(trimmed), "```json") {
		return "", false
	}
	return strictRuntimeEnvelopeJSON(trimmed)
}

func runtimeHuokeQuotedDraftJSON(task ProfilePlan, raw string) bool {
	_, ok := runtimeHuokeQuotedDraftCandidate(task, raw)
	return ok
}

func runtimeHuokeQuotedDraftCandidate(task ProfilePlan, raw string) (string, bool) {
	if !runtimeHuokeResultContract(task) {
		return "", false
	}
	for _, candidate := range runtimeJSONCandidates(raw) {
		for _, attempt := range []string{candidate, repairUnescapedJSONQuotes(candidate)} {
			payload := map[string]any{}
			if err := json.Unmarshal([]byte(attempt), &payload); err != nil {
				continue
			}
			if _, isEnvelope := payload["schemaVersion"]; isEnvelope {
				continue
			}
			strategy, _ := payload["strategy"].(string)
			question, _ := payload["question"].(string)
			mode, _ := payload["mode"].(string)
			normalizedMode := strings.ToLower(strings.TrimSpace(mode))
			questionMode := strings.TrimSpace(question) != "" && normalizedMode == "question"
			_, hasOffers := payload["offerQuotes"]
			if !questionMode && (strings.TrimSpace(strategy) == "" || !hasOffers) {
				continue
			}
			if !questionMode && normalizedMode == "full" && !runtimeHuokeHasFilmingEvidence(payload) {
				continue
			}
			canonical := map[string]any{}
			for _, key := range []string{
				"mode", "strategy", "audienceHook", "offerQuotes", "ctaQuote", "castQuote", "locationQuote", "visibleQuote", "question", "shots",
				"siteReports", "creativePlan", "spokenCopy", "shotPlan", "risks", "qualityCheck",
			} {
				if value, exists := payload[key]; exists {
					canonical[key] = value
				}
			}
			encoded, err := json.Marshal(canonical)
			if err == nil {
				return string(encoded), true
			}
		}
	}
	return "", false
}

func runtimeHuokeHasFilmingEvidence(payload map[string]any) bool {
	for _, key := range []string{"castQuote", "locationQuote", "visibleQuote"} {
		if strings.TrimSpace(fmt.Sprint(payload[key])) != "" {
			return true
		}
	}
	return len(mapSlice(payload["shots"])) > 0 || len(mapSlice(payload["shotPlan"])) > 0
}

func runtimeHuokeResultContract(task ProfilePlan) bool {
	return task.TaskType == "work_ai_huoke_content" && task.OutputSchemaVersion == "huoke_content_creation.result.v1"
}

func runtimeHuokeTopicResultContract(task ProfilePlan) bool {
	return task.TaskType == "work_ai_huoke_topic_strategy" && task.OutputSchemaVersion == "huoke_topic_strategy.result.v1"
}

func runtimeHuokeTopicUnsafeMixedEnvelope(task ProfilePlan, raw string) bool {
	if !runtimeHuokeTopicResultContract(task) || runtimeAnswerLooksLikeJSON(raw) {
		return false
	}
	if strings.Contains(raw, `"schemaVersion"`) && strings.Contains(raw, "huoke_topic_strategy.result.v1") {
		return true
	}
	for _, marker := range []string{
		`"consultationState"`,
		`"consultationStatePatch"`,
		`"consultationStateDelta"`,
		`"moduleLedger"`,
		`"strategyAssessments"`,
	} {
		if strings.Contains(raw, marker) {
			return true
		}
	}
	return false
}

func runtimeHuokePlainQuestion(task ProfilePlan, raw string) bool {
	if !runtimeHuokeResultContract(task) {
		return false
	}
	text := strings.TrimSpace(raw)
	runes := []rune(text)
	if len(runes) == 0 || len(runes) > 120 || strings.Contains(text, "\n") || strings.Contains(text, "```") || strings.HasPrefix(text, "#") {
		return false
	}
	return strings.HasSuffix(text, "?") || strings.HasSuffix(text, "？")
}

func runtimeTextLooksLikeAgentFailure(raw string) bool {
	normalized := strings.ToLower(strings.TrimSpace(raw))
	return strings.Contains(normalized, "agent couldn't generate a response") ||
		strings.Contains(normalized, "agent could not generate a response") ||
		strings.Contains(normalized, "couldn't generate a response") ||
		strings.Contains(normalized, "could not generate a response")
}

func runtimeTextLooksLikeCompletionOnly(raw string) bool {
	normalized := strings.ToLower(strings.TrimSpace(strings.TrimPrefix(raw, "\ufeff")))
	normalized = strings.Trim(normalized, ".!！。 \t\r\n")
	switch normalized {
	case "done", "ok", "okay", "completed", "complete", "finished", "success", "succeeded",
		"已完成", "完成", "处理完成", "已处理完成", "任务完成":
		return true
	default:
		return false
	}
}

func markdownMinutesPayload(markdown string) map[string]any {
	title := firstMarkdownHeading(markdown)
	if title == "" {
		title = firstMarkdownLine(markdown)
	}
	preamble, sections := splitMarkdownMinutesDocument(markdown)
	summaryLines := markdownMinutesSectionLines(sections, "summary")
	overview := firstMinutesPlainParagraph(summaryLines)
	if overview == "" {
		overview = firstMarkdownParagraph(markdown, title)
	}
	if overview == "" {
		overview = title
	}
	payload := map[string]any{
		"schemaVersion": "recording.minutes.v1",
		"title":         truncateParserText(title, 120),
		"overview":      truncateParserText(overview, 600),
	}
	metadata := parseMarkdownMinutesMetadata(preamble)
	for _, key := range []string{"meetingTopic", "meetingTime", "selfSpeaker", "sourceFile"} {
		if value := parserString(metadata[key]); value != "" {
			payload[key] = value
		}
	}
	participants := parseMarkdownMinutesParticipants(metadata, markdownMinutesSectionLines(sections, "participants"))
	if len(participants) > 0 {
		payload["participants"] = participants
	}
	topics := parseMarkdownMinutesTopics(append(summaryLines, markdownMinutesSectionLines(sections, "topics")...))
	if len(topics) > 0 {
		payload["sections"] = topics
	}
	decisions := parseMarkdownMinutesDecisions(markdownMinutesSectionLines(sections, "decisions"))
	if len(decisions) > 0 {
		payload["decisions"] = decisions
	}
	actionItems := parseMarkdownMinutesActionItems(markdownMinutesSectionLines(sections, "actions"))
	if len(actionItems) > 0 {
		payload["actionItems"] = actionItems
	}
	openQuestions := parseMarkdownMinutesList(markdownMinutesSectionLines(sections, "open_questions"))
	if len(openQuestions) > 0 {
		payload["openQuestions"] = openQuestions
	}
	chapters := parseMarkdownMinutesChapters(markdownMinutesSectionLines(sections, "chapters"))
	if len(chapters) > 0 {
		payload["chapters"] = chapters
		payload["smartChapters"] = chapters
	}
	quotes := parseMarkdownMinutesQuotes(markdownMinutesSectionLines(sections, "quotes"))
	if len(quotes) > 0 {
		payload["quoteHighlights"] = quotes
	}
	return payload
}

type markdownMinutesSection struct {
	Kind  string
	Lines []string
}

var (
	markdownMinutesMetadataPattern    = regexp.MustCompile(`^\s*>\s*(会议主题|会议时间|录音时间|参会人|发言人|用户本人|原始文件)\s*[：:]\s*(.*?)\s*$`)
	markdownMinutesOwnerPattern       = regexp.MustCompile(`(?:负责人|Owner)\s*[：:]\s*([^；;，,\n]+)`)
	markdownMinutesTimePattern        = regexp.MustCompile(`(?:时间|期限|截止时间|截止)\s*[：:]\s*([^；;\n]+)`)
	markdownMinutesDonePattern        = regexp.MustCompile(`(?:完成标准|验收标准|目标结果)\s*[：:]\s*([^；;\n]+)`)
	markdownMinutesSourcePattern      = regexp.MustCompile(`（来自\s*([^）]+)）`)
	markdownMinutesAtOwnerPattern     = regexp.MustCompile(`(?:^|\s)@([^\s@]+)\s*$`)
	markdownMinutesActionFieldPattern = regexp.MustCompile(`[；;]\s*(?:负责人|Owner|时间|期限|截止时间|截止|完成标准|验收标准|目标结果)\s*[：:]`)
	markdownMinutesChapterPattern     = regexp.MustCompile(`^\s*\[(\d{1,2}:\d{2}(?::\d{2})?)\]\s*\*{0,2}(.+?)\*{0,2}\s*$`)
	markdownMinutesQuoteByPattern     = regexp.MustCompile(`^[-—]{1,2}\s*([^，,]+?)(?:[，,]\s*(\d{1,2}:\d{2}(?::\d{2})?))?\s*$`)
)

func splitMarkdownMinutesDocument(markdown string) ([]string, []markdownMinutesSection) {
	lines := strings.Split(strings.ReplaceAll(markdown, "\r\n", "\n"), "\n")
	preamble := []string{}
	sections := []markdownMinutesSection{}
	seenTitle := false
	current := -1
	for _, line := range lines {
		if heading, ok := markdownMinutesHeading(line); ok {
			if !seenTitle {
				seenTitle = true
				continue
			}
			kind := markdownMinutesSectionKind(heading)
			if kind == "stop" {
				break
			}
			if kind != "" {
				sections = append(sections, markdownMinutesSection{Kind: kind})
				current = len(sections) - 1
				continue
			}
		}
		if current >= 0 {
			sections[current].Lines = append(sections[current].Lines, line)
		} else if seenTitle {
			preamble = append(preamble, line)
		}
	}
	return preamble, sections
}

func markdownMinutesHeading(line string) (string, bool) {
	trimmed := strings.TrimSpace(line)
	if !strings.HasPrefix(trimmed, "#") {
		return "", false
	}
	text := strings.TrimSpace(strings.TrimLeft(trimmed, "#"))
	return strings.Trim(text, "`*_ "), text != ""
}

func markdownMinutesSectionKind(heading string) string {
	normalized := strings.ToLower(strings.Map(func(r rune) rune {
		switch r {
		case ' ', '\t', '/', '／', '-', '_', '：', ':':
			return -1
		default:
			return r
		}
	}, heading))
	switch {
	case strings.Contains(normalized, "内容素材拆解") || strings.Contains(normalized, "contentmaterials"):
		return "stop"
	case normalized == "总结" || normalized == "概览" || normalized == "会议概览" || normalized == "会议总结" || normalized == "summary" || normalized == "overview" || normalized == "meetingoverview" || normalized == "meetingsummary":
		return "summary"
	case strings.Contains(normalized, "参会") || normalized == "发言人" || normalized == "participants" || normalized == "speakers":
		return "participants"
	case strings.Contains(normalized, "关键议题") || normalized == "议题" || normalized == "topics" || normalized == "keytopics":
		return "topics"
	case strings.Contains(normalized, "待办") || strings.Contains(normalized, "行动项") || strings.Contains(normalized, "后续行动") || normalized == "todos" || normalized == "actionitems" || normalized == "followupactions":
		return "actions"
	case strings.Contains(normalized, "决策") || strings.Contains(normalized, "关键判断") || normalized == "decisions" || normalized == "keydecisions":
		return "decisions"
	case strings.Contains(normalized, "待确认") || strings.Contains(normalized, "未决问题") || strings.Contains(normalized, "风险与问题") || normalized == "openquestions" || normalized == "unresolvedquestions":
		return "open_questions"
	case strings.Contains(normalized, "智能章节") || normalized == "章节" || normalized == "smartchapters" || normalized == "chapters":
		return "chapters"
	case strings.Contains(normalized, "关键原话") || strings.Contains(normalized, "原话摘录") || normalized == "keyquotes" || normalized == "quotes":
		return "quotes"
	case strings.Contains(normalized, "相关链接") || strings.Contains(normalized, "相关会议纪要"):
		return "ignored"
	default:
		return ""
	}
}

func markdownMinutesSectionLines(sections []markdownMinutesSection, kind string) []string {
	lines := []string{}
	for _, section := range sections {
		if section.Kind == kind {
			lines = append(lines, section.Lines...)
		}
	}
	return lines
}

func parseMarkdownMinutesMetadata(lines []string) map[string]any {
	metadata := map[string]any{}
	for _, line := range lines {
		match := markdownMinutesMetadataPattern.FindStringSubmatch(line)
		if len(match) != 3 {
			continue
		}
		value := cleanMarkdownMinutesText(match[2])
		switch match[1] {
		case "会议时间", "录音时间":
			metadata["meetingTime"] = value
		case "会议主题":
			metadata["meetingTopic"] = value
		case "参会人", "发言人":
			metadata["participantsText"] = value
		case "用户本人":
			metadata["selfSpeaker"] = value
		case "原始文件":
			metadata["sourceFile"] = value
		}
	}
	return metadata
}

func parseMarkdownMinutesParticipants(metadata map[string]any, sectionLines []string) []any {
	values := []string{}
	if raw := parserString(metadata["participantsText"]); raw != "" {
		values = append(values, raw)
	}
	for _, line := range sectionLines {
		if _, text, _, ok := parseMarkdownMinutesBullet(line); ok {
			values = append(values, text)
		}
	}
	seen := map[string]bool{}
	participants := []any{}
	for _, value := range values {
		value = strings.ReplaceAll(value, "@", "、")
		for _, name := range strings.FieldsFunc(value, func(r rune) bool {
			switch r {
			case '、', '，', ',', ';', '；', '|', '｜':
				return true
			default:
				return false
			}
		}) {
			name = cleanMarkdownMinutesText(name)
			if name == "" || seen[name] {
				continue
			}
			seen[name] = true
			participants = append(participants, map[string]any{"displayName": name})
		}
	}
	return participants
}

func firstMinutesPlainParagraph(lines []string) string {
	paragraph := []string{}
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			if len(paragraph) > 0 {
				break
			}
			continue
		}
		if strings.HasPrefix(trimmed, "#") || strings.HasPrefix(trimmed, ">") || strings.HasPrefix(trimmed, "![") {
			continue
		}
		if _, _, _, ok := parseMarkdownMinutesBullet(line); ok {
			if len(paragraph) > 0 {
				break
			}
			continue
		}
		paragraph = append(paragraph, cleanMarkdownMinutesText(trimmed))
	}
	return strings.TrimSpace(strings.Join(paragraph, " "))
}

func parseMarkdownMinutesTopics(lines []string) []any {
	minimumIndent := -1
	for _, line := range lines {
		indent, _, status, ok := parseMarkdownMinutesBullet(line)
		if !ok || status != "" {
			continue
		}
		if minimumIndent < 0 || indent < minimumIndent {
			minimumIndent = indent
		}
	}
	if minimumIndent < 0 {
		return nil
	}
	topics := []any{}
	var current map[string]any
	points := []any{}
	var roots []*markdownMinutesTopicNode
	var stack []*markdownMinutesTopicNode
	flush := func() {
		if current == nil {
			return
		}
		if len(points) > 0 {
			current["points"] = points
			parts := []string{}
			for _, point := range points {
				parts = append(parts, strings.TrimSpace(fmt.Sprint(point)))
				if len(parts) == 3 {
					break
				}
			}
			current["summary"] = truncateParserText(strings.Join(parts, "；"), 500)
		}
		if len(roots) > 0 {
			children := make([]any, 0, len(roots))
			for _, root := range roots {
				children = append(children, root.payload())
			}
			current["children"] = children
		}
		topics = append(topics, current)
		current = nil
		points = []any{}
		roots = nil
		stack = nil
	}
	for _, line := range lines {
		indent, text, status, ok := parseMarkdownMinutesBullet(line)
		if !ok || status != "" || text == "" {
			continue
		}
		if indent == minimumIndent {
			flush()
			title := truncateParserText(text, 160)
			current = map[string]any{"heading": title, "title": title}
			continue
		}
		if current != nil {
			pointText := truncateParserText(text, 500)
			points = append(points, pointText)
			node := &markdownMinutesTopicNode{indent: indent, text: pointText}
			for len(stack) > 0 && stack[len(stack)-1].indent >= indent {
				stack = stack[:len(stack)-1]
			}
			if len(stack) == 0 {
				roots = append(roots, node)
			} else {
				parent := stack[len(stack)-1]
				parent.children = append(parent.children, node)
			}
			stack = append(stack, node)
		}
	}
	flush()
	return topics
}

type markdownMinutesTopicNode struct {
	indent   int
	text     string
	children []*markdownMinutesTopicNode
}

func (n *markdownMinutesTopicNode) payload() map[string]any {
	payload := map[string]any{"text": n.text}
	if len(n.children) > 0 {
		children := make([]any, 0, len(n.children))
		for _, child := range n.children {
			children = append(children, child.payload())
		}
		payload["children"] = children
	}
	return payload
}

func parseMarkdownMinutesDecisions(lines []string) []any {
	items := parseMarkdownMinutesList(lines)
	decisions := make([]any, 0, len(items))
	for _, item := range items {
		decisions = append(decisions, map[string]any{"content": item})
	}
	return decisions
}

func parseMarkdownMinutesActionItems(lines []string) []any {
	items := []any{}
	for _, line := range lines {
		_, text, status, ok := parseMarkdownMinutesBullet(line)
		if !ok || text == "" {
			continue
		}
		if status == "" {
			status = "open"
		}
		item := map[string]any{
			"content": truncateParserText(cleanMarkdownMinutesActionContent(text), 600),
			"status":  status,
		}
		if match := markdownMinutesOwnerPattern.FindStringSubmatch(text); len(match) == 2 {
			item["ownerName"] = cleanMarkdownMinutesText(match[1])
		} else if match := markdownMinutesAtOwnerPattern.FindStringSubmatch(text); len(match) == 2 {
			item["ownerName"] = cleanMarkdownMinutesText(match[1])
		}
		if match := markdownMinutesTimePattern.FindStringSubmatch(text); len(match) == 2 {
			item["due"] = cleanMarkdownMinutesText(match[1])
		}
		if match := markdownMinutesDonePattern.FindStringSubmatch(text); len(match) == 2 {
			item["completionCriteria"] = cleanMarkdownMinutesText(match[1])
		}
		if match := markdownMinutesSourcePattern.FindStringSubmatch(text); len(match) == 2 {
			item["sourceSpeaker"] = cleanMarkdownMinutesText(match[1])
		}
		items = append(items, item)
	}
	return items
}

func cleanMarkdownMinutesActionContent(text string) string {
	content := text
	if location := markdownMinutesActionFieldPattern.FindStringIndex(content); len(location) == 2 {
		content = content[:location[0]]
	}
	content = markdownMinutesSourcePattern.ReplaceAllString(content, "")
	content = markdownMinutesAtOwnerPattern.ReplaceAllString(content, "")
	return cleanMarkdownMinutesText(content)
}

func parseMarkdownMinutesList(lines []string) []any {
	minimumIndent := -1
	type listItem struct {
		indent int
		text   string
	}
	candidates := []listItem{}
	for _, line := range lines {
		indent, text, _, ok := parseMarkdownMinutesBullet(line)
		if !ok || text == "" {
			continue
		}
		candidates = append(candidates, listItem{indent: indent, text: text})
		if minimumIndent < 0 || indent < minimumIndent {
			minimumIndent = indent
		}
	}
	items := []any{}
	for _, candidate := range candidates {
		if candidate.indent == minimumIndent {
			items = append(items, truncateParserText(candidate.text, 600))
		}
	}
	return items
}

func parseMarkdownMinutesChapters(lines []string) []any {
	chapters := []any{}
	for index := 0; index < len(lines); index++ {
		match := markdownMinutesChapterPattern.FindStringSubmatch(strings.TrimSpace(lines[index]))
		if len(match) != 3 {
			continue
		}
		chapter := map[string]any{
			"startTime": match[1],
			"title":     cleanMarkdownMinutesText(match[2]),
		}
		for next := index + 1; next < len(lines); next++ {
			trimmed := strings.TrimSpace(lines[next])
			if trimmed == "" || trimmed == ">" {
				continue
			}
			if markdownMinutesChapterPattern.MatchString(trimmed) {
				break
			}
			if strings.HasPrefix(trimmed, ">") {
				chapter["summary"] = truncateParserText(cleanMarkdownMinutesText(strings.TrimPrefix(trimmed, ">")), 500)
			}
			break
		}
		chapters = append(chapters, chapter)
	}
	return chapters
}

func parseMarkdownMinutesQuotes(lines []string) []any {
	quotes := []any{}
	for index := 0; index < len(lines); index++ {
		trimmed := strings.TrimSpace(lines[index])
		if !strings.HasPrefix(trimmed, ">") {
			continue
		}
		text := strings.TrimSpace(strings.TrimPrefix(trimmed, ">"))
		if text == "" || text == ">" || !(strings.HasPrefix(text, "“") || strings.HasPrefix(text, "\"") || strings.HasPrefix(text, "「")) {
			continue
		}
		quote := map[string]any{"text": strings.Trim(cleanMarkdownMinutesText(text), "\"“”「」")}
		for next := index + 1; next < len(lines); next++ {
			byline := strings.TrimSpace(lines[next])
			if byline == "" || byline == ">" {
				continue
			}
			byline = strings.TrimSpace(strings.TrimPrefix(byline, ">"))
			if match := markdownMinutesQuoteByPattern.FindStringSubmatch(byline); len(match) >= 2 {
				quote["speakerName"] = cleanMarkdownMinutesText(match[1])
				if len(match) == 3 && match[2] != "" {
					quote["time"] = match[2]
				}
			}
			break
		}
		quotes = append(quotes, quote)
	}
	return quotes
}

func parseMarkdownMinutesBullet(line string) (int, string, string, bool) {
	expanded := strings.ReplaceAll(line, "\t", "    ")
	trimmedLeft := strings.TrimLeft(expanded, " ")
	indent := len(expanded) - len(trimmedLeft)
	if len(trimmedLeft) < 2 || !strings.ContainsRune("-*+", rune(trimmedLeft[0])) || trimmedLeft[1] != ' ' {
		return 0, "", "", false
	}
	text := strings.TrimSpace(trimmedLeft[2:])
	status := ""
	if len(text) >= 3 && text[0] == '[' && text[2] == ']' {
		switch text[1] {
		case 'x', 'X':
			status = "done"
		case ' ':
			status = "open"
		default:
			return 0, "", "", false
		}
		text = strings.TrimSpace(text[3:])
	}
	return indent, cleanMarkdownMinutesText(text), status, true
}

func cleanMarkdownMinutesText(text string) string {
	text = strings.TrimSpace(text)
	text = strings.ReplaceAll(text, "**", "")
	text = strings.ReplaceAll(text, "__", "")
	text = strings.Trim(text, "`*_ ")
	return strings.TrimSpace(text)
}

func markdownSummaryText(markdown string) string {
	if paragraph := firstMarkdownParagraph(markdown, ""); paragraph != "" {
		return truncateParserText(paragraph, 1000)
	}
	return truncateParserText(firstMarkdownLine(markdown), 1000)
}

func firstMarkdownHeading(markdown string) string {
	for _, line := range strings.Split(markdown, "\n") {
		trimmed := strings.TrimSpace(line)
		if !strings.HasPrefix(trimmed, "#") {
			continue
		}
		return stripMarkdownLine(trimmed)
	}
	return ""
}

func firstMarkdownLine(markdown string) string {
	for _, line := range strings.Split(markdown, "\n") {
		if text := stripMarkdownLine(line); text != "" {
			return text
		}
	}
	return ""
}

func firstMarkdownParagraph(markdown, skip string) string {
	skip = strings.TrimSpace(skip)
	for _, block := range strings.Split(markdown, "\n\n") {
		lines := []string{}
		for _, line := range strings.Split(block, "\n") {
			if text := stripMarkdownLine(line); text != "" {
				lines = append(lines, text)
			}
		}
		text := strings.TrimSpace(strings.Join(lines, " "))
		if text != "" && text != skip {
			return text
		}
	}
	return ""
}

func stripMarkdownLine(line string) string {
	text := strings.TrimSpace(line)
	text = strings.TrimLeft(text, "#")
	text = strings.TrimLeft(text, "-*+> 0123456789.")
	text = strings.TrimSpace(text)
	text = strings.Trim(text, "`*_")
	return strings.TrimSpace(text)
}

func truncateParserText(text string, limit int) string {
	text = strings.TrimSpace(text)
	if limit <= 0 {
		return text
	}
	runes := []rune(text)
	if len(runes) <= limit {
		return text
	}
	return string(runes[:limit])
}

func runtimeJSONCandidates(raw string) []string {
	trimmed := strings.TrimSpace(strings.TrimPrefix(raw, "\ufeff"))
	if trimmed == "" {
		return nil
	}
	candidates := []string{}
	seen := map[string]bool{}
	add := func(value string) {
		value = strings.TrimSpace(value)
		if value == "" || seen[value] {
			return
		}
		seen[value] = true
		candidates = append(candidates, value)
	}
	add(trimmed)
	for index, char := range trimmed {
		if char != '{' {
			continue
		}
		if object, ok := balancedJSONObjectAt(trimmed, index); ok {
			add(object)
		}
	}
	return candidates
}

func balancedJSONObjectAt(raw string, start int) (string, bool) {
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

func repairUnescapedJSONQuotes(raw string) string {
	var builder strings.Builder
	builder.Grow(len(raw) + 16)
	inString := false
	escaped := false
	for i, r := range raw {
		if !inString {
			builder.WriteRune(r)
			if r == '"' {
				inString = true
			}
			continue
		}
		if escaped {
			builder.WriteRune(r)
			escaped = false
			continue
		}
		if r == '\\' {
			builder.WriteRune(r)
			escaped = true
			continue
		}
		if r == '"' {
			if jsonQuoteLooksLikeStringEnd(raw[i+1:]) {
				builder.WriteRune(r)
				inString = false
			} else {
				builder.WriteString(`\"`)
			}
			continue
		}
		builder.WriteRune(r)
	}
	return builder.String()
}

func jsonQuoteLooksLikeStringEnd(rest string) bool {
	trimmed := strings.TrimLeft(rest, " \t\r\n")
	if trimmed == "" {
		return true
	}
	switch trimmed[0] {
	case ':', ',', '}', ']':
		return true
	default:
		return false
	}
}

func promoteNestedAssetWriteIntent(raw string) string {
	dataIndex := strings.Index(raw, `"data":{`)
	assetIndex := strings.Index(raw, `,"assetWriteIntent":`)
	if dataIndex < 0 || assetIndex < 0 || assetIndex < dataIndex {
		return raw
	}
	return raw[:assetIndex] + "}" + raw[assetIndex:]
}

func (p OutputParser) ParseTopicGeneration(result map[string]any) (TopicGenerationResult, error) {
	return p.parseTopicGeneration(result)
}

func (p OutputParser) parseTopicGeneration(result map[string]any) (TopicGenerationResult, error) {
	topics := mapSlice(result["topics"])
	if len(topics) == 0 {
		if parserString(result["reply"]) != "" || parserString(result["artifactText"]) != "" || parserString(result["text"]) != "" {
			return TopicGenerationResult{Topics: []map[string]any{}}, nil
		}
	}
	if len(topics) == 0 {
		return TopicGenerationResult{}, parseError("AI_RESULT_PARSE_FAILED")
	}
	for _, topic := range topics {
		if parserString(topic["title"]) == "" {
			return TopicGenerationResult{}, parseError("AI_RESULT_PARSE_FAILED")
		}
	}
	return TopicGenerationResult{Topics: topics}, nil
}

func (p OutputParser) parseGeneralChat(result map[string]any) (map[string]any, error) {
	if parserString(result["reply"]) == "" {
		return nil, parseError("AI_RESULT_PARSE_FAILED")
	}
	return result, nil
}

func normalizeFayaGerminationReplyData(result map[string]any) {
	reply := parserString(result["reply"])
	if reply == "" {
		return
	}
	result["reply"] = normalizeFayaGerminationReply(reply)
}

func normalizeFayaGerminationReply(raw string) string {
	text := strings.TrimSpace(strings.ReplaceAll(raw, "\r\n", "\n"))
	lines := strings.Split(text, "\n")
	if len(lines) == 0 {
		return ""
	}
	firstLine := strings.TrimSpace(lines[0])
	if !strings.HasPrefix(firstLine, "#") || strings.TrimSpace(strings.TrimLeft(firstLine, "#")) != "发芽后的材料" {
		return text
	}
	return strings.TrimSpace(strings.Join(lines[1:], "\n"))
}

func (p OutputParser) parsePositioningProfileBuilder(result map[string]any) (map[string]any, error) {
	reply := firstRuntimeNonEmpty(parserString(result["reply"]), parserString(result["replyMarkdown"]))
	if reply == "" {
		return nil, parseError("AI_RESULT_PARSE_FAILED")
	}
	reply = normalizePositioningReplyMarkdown(reply)
	result["reply"] = reply
	result["replyMarkdown"] = reply
	return result, nil
}

func normalizePositioningReplyMarkdown(raw string) string {
	text := strings.TrimSpace(strings.ReplaceAll(raw, "\r\n", "\n"))
	if text == "" {
		return ""
	}

	replacements := []struct {
		old string
		new string
	}{
		{" ```huahuo-positioning-progress ", "\n\n```huahuo-positioning-progress\n"},
		{"} ```", "}\n```"},
		{" --- # ", "\n\n---\n\n# "},
		{" --- ## ", "\n\n---\n\n## "},
		{" --- ### ", "\n\n---\n\n### "},
		{" ### ", "\n\n### "},
		{" ## ", "\n\n## "},
		{"| |", "|\n|"},
		{" - ", "\n- "},
		{" > ", "\n\n> "},
	}
	for _, replacement := range replacements {
		text = strings.ReplaceAll(text, replacement.old, replacement.new)
	}
	for i := 1; i <= 12; i++ {
		marker := fmt.Sprintf(" %d. ", i)
		text = strings.ReplaceAll(text, marker, "\n"+strings.TrimSpace(marker)+" ")
	}

	for strings.Contains(text, "\n\n\n") {
		text = strings.ReplaceAll(text, "\n\n\n", "\n\n")
	}
	return strings.TrimSpace(text)
}

func (p OutputParser) parseProfileMaintenance(result map[string]any) (map[string]any, error) {
	if parserString(result["reply"]) == "" && len(mapSlice(result["profileItems"])) == 0 {
		return nil, parseError("AI_RESULT_PARSE_FAILED")
	}
	return result, nil
}

func (p OutputParser) parseMeetingMinutes(result map[string]any) (map[string]any, error) {
	minutes := mapValueForParser(result["minutes"])
	if len(minutes) == 0 && parserString(result["minutesMarkdown"]) == "" && parserString(result["summary"]) == "" {
		return nil, parseError("AI_RESULT_PARSE_FAILED")
	}
	if len(minutes) > 0 && parserString(minutes["schemaVersion"]) != "" && parserString(minutes["schemaVersion"]) != "recording.minutes.v1" {
		return nil, parseError("AI_RESULT_PARSE_FAILED")
	}
	return result, nil
}

func (p OutputParser) parseAssetSummary(result map[string]any) (map[string]any, error) {
	if parserString(result["summary"]) == "" {
		return nil, parseError("AI_RESULT_PARSE_FAILED")
	}
	return result, nil
}

func (p OutputParser) parseMaterialDeposit(result map[string]any) (map[string]any, error) {
	if parserString(result["depositMarkdown"]) == "" {
		return nil, parseError("AI_RESULT_PARSE_FAILED")
	}
	return result, nil
}

func (p OutputParser) parseHotspotSuggestion(result map[string]any) (HotspotSuggestion, error) {
	if runtimeLegacyContentLineIDPresent(result) {
		return HotspotSuggestion{}, parseError("RUNTIME_INPUT_INVALID")
	}
	payload := mapValueForParser(result["payload"])
	if len(payload) == 0 {
		payload = result
	}
	if parserString(payload["title"]) == "" && parserString(payload["topic"]) == "" {
		return HotspotSuggestion{}, parseError("AI_RESULT_PARSE_FAILED")
	}
	return HotspotSuggestion{SuggestionID: parserString(result["suggestionId"]), CreativePositioningID: parserString(result["creativePositioningId"]), Payload: payload}, nil
}

func runtimeLegacyContentLineIDPresent(value any) bool {
	switch typed := value.(type) {
	case map[string]any:
		for key, item := range typed {
			if key == "contentLineId" || runtimeLegacyContentLineIDPresent(item) {
				return true
			}
		}
	case map[string]string:
		if _, exists := typed["contentLineId"]; exists {
			return true
		}
	case []any:
		for _, item := range typed {
			if runtimeLegacyContentLineIDPresent(item) {
				return true
			}
		}
	case []map[string]any:
		for _, item := range typed {
			if runtimeLegacyContentLineIDPresent(item) {
				return true
			}
		}
	}
	return false
}

func (p OutputParser) ValidateAssetWriteIntent(intent map[string]any) error {
	return p.validateAssetWriteIntent(intent)
}

func (p OutputParser) validateAssetWriteIntent(intent map[string]any) error {
	operation := parserString(intent["operation"])
	switch operation {
	case "upsert_profile_items", "append_daily_asset", "store_recording_minutes", "recording_minutes_candidate", "store_recording_summary", "recording_summary_candidate", "work_ai_result_index", "hotspot_suggestion":
	default:
		return parseError("WORKSPACE_WRITE_FAILED")
	}
	for _, key := range []string{"path", "realPath", "absolutePath", "workspaceDir"} {
		if parserString(intent[key]) != "" {
			return parseError("WORKSPACE_WRITE_FAILED")
		}
	}
	if containsUnsafePath(intent) {
		return parseError("WORKSPACE_WRITE_FAILED")
	}
	return nil
}

func validateRuntimeEnvelope(task ProfilePlan, runRecord RuntimeRunRecord, result RuntimeParsedResult) error {
	if result.Status != "" && result.Status != "succeeded" && !runtimeFayaNoViableSeedStatusAccepted(task, result) {
		return parseError("AI_RESULT_PARSE_FAILED")
	}
	if result.SchemaVersion != "" && result.SchemaVersion != task.OutputSchemaVersion {
		return parseError("RUNTIME_INPUT_INVALID")
	}
	if result.TaskType != "" && result.TaskType != task.TaskType {
		return parseError("SKILL_TASK_MISMATCH")
	}
	if result.SkillProfile != "" && result.SkillProfile != task.SkillProfile {
		return parseError("SKILL_TASK_MISMATCH")
	}
	if runRecord.TaskID != "" && result.TaskID != "" && result.TaskID != runRecord.TaskID {
		return parseError("RUNTIME_INPUT_INVALID")
	}
	if runRecord.RunID != "" && result.RunID != "" && !runtimeRunIDMatchesRunRecord(runRecord.RunID, result.RunID) {
		return parseError("RUNTIME_INPUT_INVALID")
	}
	return nil
}

func canonicalizeRuntimeEnvelope(task ProfilePlan, runRecord RuntimeRunRecord, result RuntimeParsedResult) RuntimeParsedResult {
	if result.SchemaVersion == "" {
		result.SchemaVersion = task.OutputSchemaVersion
	}
	if result.TaskType == "" {
		result.TaskType = task.TaskType
	}
	if result.SkillProfile == "" {
		result.SkillProfile = task.SkillProfile
	}
	if result.Status == "" {
		result.Status = "succeeded"
	}
	if result.TaskID == "" {
		result.TaskID = runRecord.TaskID
	}
	if result.RunID == "" {
		result.RunID = runRecord.RunID
	}
	return result
}

func runtimeRunIDMatchesRunRecord(recordRunID, resultRunID string) bool {
	recordRunID = strings.TrimSpace(recordRunID)
	resultRunID = strings.TrimSpace(resultRunID)
	if recordRunID == "" || resultRunID == "" {
		return false
	}
	if resultRunID == recordRunID {
		return true
	}
	baseRunID, ok := stripNumericRunAttemptSuffix(recordRunID)
	return ok && resultRunID == baseRunID
}

func stripNumericRunAttemptSuffix(runID string) (string, bool) {
	index := strings.LastIndex(runID, "_")
	if index <= 0 || index == len(runID)-1 {
		return "", false
	}
	suffix := runID[index+1:]
	for _, r := range suffix {
		if r < '0' || r > '9' {
			return "", false
		}
	}
	return runID[:index], true
}

func parseError(code string) error {
	return fmt.Errorf(code)
}

func mapSlice(value any) []map[string]any {
	switch typed := value.(type) {
	case []map[string]any:
		return typed
	case []any:
		out := make([]map[string]any, 0, len(typed))
		for _, item := range typed {
			if mapped, ok := item.(map[string]any); ok {
				out = append(out, mapped)
			}
		}
		return out
	default:
		return nil
	}
}

func mapValueForParser(value any) map[string]any {
	if typed, ok := value.(map[string]any); ok {
		return typed
	}
	return map[string]any{}
}

func containsUnsafePath(value any) bool {
	switch typed := value.(type) {
	case string:
		return runtimeStringContainsUnsafePath(typed)
	case map[string]any:
		for _, item := range typed {
			if containsUnsafePath(item) {
				return true
			}
		}
	case []any:
		for _, item := range typed {
			if containsUnsafePath(item) {
				return true
			}
		}
	case []map[string]any:
		for _, item := range typed {
			if containsUnsafePath(item) {
				return true
			}
		}
	}
	return false
}

func runtimeStringContainsUnsafePath(value string) bool {
	normalized := strings.ReplaceAll(strings.TrimSpace(value), "\\", "/")
	normalized = strings.Trim(normalized, " \t\r\n\"'`.,;:()[]{}<>")
	if normalized == "" {
		return false
	}
	lower := strings.ToLower(normalized)
	if runtimePathHasTraversal(normalized) || strings.HasPrefix(normalized, "/") || looksLikeRuntimePath(normalized) {
		return true
	}
	for _, marker := range []string{
		"/home/data/",
		"/home/huahuo-runtime/",
		"/tmp/runtime-workspaces/",
		"/runtime-workspaces/",
		"/workspaces/tenants/",
		"file://",
		"signature=",
		"x-amz-signature",
		"x-oss-signature",
		"access_token=",
		"token=",
	} {
		if strings.Contains(lower, marker) {
			return true
		}
	}
	return false
}

func parserString(value any) string {
	if value == nil {
		return ""
	}
	if typed, ok := value.(string); ok {
		return strings.TrimSpace(typed)
	}
	return strings.TrimSpace(fmt.Sprint(value))
}
