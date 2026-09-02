package runtime

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
)

const (
	HuokeTopicStateSchemaVersion          = "huoke_topic_strategy.state.v1"
	HuokeTopicStatePatchSchemaVersion     = "huoke_topic_strategy.state_patch.v1"
	HuokeTopicStateTransportSchemaVersion = "huoke_topic_strategy.state_transport.v1"
	huokeTopicStateMaxBytes               = 256 << 10
	huokeTopicStateMaxDepth               = 20
)

var huokeTopicModuleKeys = []string{
	"WF-01", "WF-02", "WF-03", "WF-04", "WF-05", "WF-06", "WF-07", "WF-08", "WF-09",
}

var huokeTopicContentStationKeys = []string{
	"C0", "C1", "C2", "C3", "C4", "C5", "C6", "C7", "C8", "C9", "C10", "C11", "C12", "C13", "C14",
}

var huokeTopicStrategyKeys = []string{
	"extreme_test",
	"advantage_comparison",
	"sensory_desire",
	"offer_benefit",
	"selling_point_list",
	"demand_creation",
	"authentic_persona",
	"value_reframing",
	"element_replacement",
	"buyer_voice",
	"relationship_care",
}

var huokeTopicReservedScopeKeys = []string{
	"tenantId", "userId", "workspaceId", "threadId", "messageId", "taskId", "runId", "deviceId", "sessionKey", "openclawSessionKey",
}

var huokeTopicRequiredStateKeys = []string{
	"schemaVersion", "stateVersion", "profileContextVersion", "executionMode", "delegationScope",
	"currentSubjectId", "subjectStatus", "explicitUserDecision", "recommendedNextSubjectId", "currentSubject",
	"acquisitionContext", "moduleLedger", "strategyOverride", "strategyAssessmentMode", "strategyAssessments",
	"primaryStrategy", "secondaryStrategy", "selectionReason", "strategyBrief", "evidenceValidation",
	"topicCandidates", "leadingTopicCandidateId", "candidateComparison", "finalRecommendation", "claims",
	"sideClues", "invalidations", "resumePoint",
}

var huokeTopicSubjectIDs = map[string]bool{
	"audience_conversion_target":    true,
	"persuasion_assets_constraints": true,
	"strategy_fit_selection":        true,
	"strategy_brief_validation":     true,
	"topic_candidate_synthesis":     true,
	"final_topic_guidance":          true,
	"revision_switch_resume":        true,
}

type HuokeTopicStateUpdate struct {
	Kind             string
	BaseStateVersion int
	StateVersion     int
	Patch            map[string]any
}

// NewHuokeTopicInitialConsultationState creates the backend-owned baseline
// supplied to a new Huoke topic thread. Models only submit versioned patches
// against this stable skeleton.
func NewHuokeTopicInitialConsultationState(profileContextVersion string) (map[string]any, error) {
	profileContextVersion = strings.TrimSpace(profileContextVersion)
	if !validHuokeTopicProfileContextVersion(profileContextVersion) {
		return nil, fmt.Errorf("invalid huoke topic profileContextVersion")
	}
	modules := map[string]any{}
	for _, key := range huokeTopicModuleKeys {
		modules[key] = map[string]any{"applicability": "required", "status": "not_started"}
	}
	modules["WF-09"] = map[string]any{"applicability": "conditional", "status": "idle", "decision": nil}
	strategies := map[string]any{}
	for _, key := range huokeTopicStrategyKeys {
		strategies[key] = map[string]any{}
	}
	state := map[string]any{
		"schemaVersion": HuokeTopicStateSchemaVersion, "stateVersion": 1,
		"profileContextVersion": profileContextVersion, "executionMode": "targeted_diagnosis",
		"delegationScope": nil, "currentSubjectId": "audience_conversion_target", "subjectStatus": "exploring",
		"explicitUserDecision": nil, "recommendedNextSubjectId": nil,
		"currentSubject":     map[string]any{"roundCount": 1, "dryReplyCount": 0, "artifactVersion": 1, "acceptedVersion": nil},
		"acquisitionContext": map[string]any{}, "moduleLedger": modules, "strategyOverride": nil,
		"contentStationLedger":   newHuokeTopicContentStationLedger(),
		"strategyAssessmentMode": "targeted_diagnosis", "strategyAssessments": strategies,
		"primaryStrategy": nil, "secondaryStrategy": nil, "selectionReason": nil,
		"strategyBrief": map[string]any{}, "evidenceValidation": map[string]any{}, "topicCandidates": map[string]any{},
		"leadingTopicCandidateId": nil, "candidateComparison": map[string]any{}, "finalRecommendation": nil,
		"claims": map[string]any{}, "sideClues": map[string]any{}, "invalidations": map[string]any{}, "resumePoint": nil,
	}
	if err := ValidateHuokeTopicConsultationState(state); err != nil {
		return nil, err
	}
	return state, nil
}

// ParseHuokeTopicStateUpdate accepts state only from structured result data.
// Natural Markdown is outside this contract and cannot create trusted state.
func ParseHuokeTopicStateUpdate(data map[string]any) (HuokeTopicStateUpdate, bool, error) {
	if len(data) == 0 {
		return HuokeTopicStateUpdate{}, false, nil
	}
	if _, hasFull := data["consultationState"]; hasFull {
		return HuokeTopicStateUpdate{}, true, fmt.Errorf("huoke topic result must use consultationStatePatch only")
	}
	if _, hasDelta := data["consultationStateDelta"]; hasDelta {
		return HuokeTopicStateUpdate{}, true, fmt.Errorf("huoke topic consultationStateDelta is unsupported")
	}
	patchValue, hasPatch := data["consultationStatePatch"]
	if !hasPatch {
		return HuokeTopicStateUpdate{}, false, nil
	}

	wrapper, ok := patchValue.(map[string]any)
	if !ok {
		return HuokeTopicStateUpdate{}, true, fmt.Errorf("huoke topic consultationStatePatch must be an object")
	}
	if strings.TrimSpace(runtimeStringValue(wrapper["schemaVersion"])) != HuokeTopicStatePatchSchemaVersion {
		return HuokeTopicStateUpdate{}, true, fmt.Errorf("unsupported huoke topic state patch schema")
	}
	baseVersion, ok := huokeTopicExactNonNegativeInt(wrapper["baseStateVersion"])
	if !ok {
		return HuokeTopicStateUpdate{}, true, fmt.Errorf("invalid huoke topic baseStateVersion")
	}
	stateVersion, ok := huokeTopicExactPositiveInt(wrapper["stateVersion"])
	if !ok || stateVersion != baseVersion+1 {
		return HuokeTopicStateUpdate{}, true, fmt.Errorf("invalid huoke topic stateVersion")
	}

	patch := runtimeMapValue(firstRuntimeNonNil(wrapper["patch"], wrapper["changes"]))
	if len(patch) == 0 {
		patch = map[string]any{}
		for key, value := range wrapper {
			switch key {
			case "schemaVersion", "baseStateVersion", "stateVersion", "patch", "changes":
				continue
			default:
				patch[key] = value
			}
		}
	}
	if len(patch) == 0 {
		return HuokeTopicStateUpdate{}, true, fmt.Errorf("empty huoke topic state patch")
	}
	patch = cloneHuokeTopicStateMap(patch)
	if err := validateHuokeTopicJSONValue(patch, 0); err != nil {
		return HuokeTopicStateUpdate{}, true, err
	}
	if _, exists := patch["schemaVersion"]; exists {
		return HuokeTopicStateUpdate{}, true, fmt.Errorf("state patch cannot replace schemaVersion")
	}
	if _, exists := patch["stateVersion"]; exists {
		return HuokeTopicStateUpdate{}, true, fmt.Errorf("state patch cannot replace stateVersion")
	}
	if key := huokeTopicReservedScopeKey(patch); key != "" {
		return HuokeTopicStateUpdate{}, true, fmt.Errorf("state patch cannot set backend scope key %s", key)
	}
	return HuokeTopicStateUpdate{
		Kind:             "patch",
		BaseStateVersion: baseVersion,
		StateVersion:     stateVersion,
		Patch:            patch,
	}, true, nil
}

func ValidateHuokeTopicExecutionAudit(data map[string]any, update HuokeTopicStateUpdate) error {
	if strings.TrimSpace(runtimeStringValue(update.Patch["executionMode"])) != "auto_full_pipeline" {
		return nil
	}
	audit, ok := data["executionAudit"].(map[string]any)
	if !ok {
		return fmt.Errorf("auto_full_pipeline requires executionAudit")
	}
	if strings.TrimSpace(runtimeStringValue(audit["executionMode"])) != "auto_full_pipeline" {
		return fmt.Errorf("invalid huoke topic executionAudit mode")
	}
	phases := huokeTopicStringSlice(audit["completedJourneyPhases"])
	expectedPhases := []string{"method_portfolio", "method_discovery", "topic_draft"}
	if len(phases) != len(expectedPhases) {
		return fmt.Errorf("invalid huoke topic executionAudit phases")
	}
	for index, expected := range expectedPhases {
		if phases[index] != expected {
			return fmt.Errorf("invalid huoke topic executionAudit phases")
		}
	}
	portfolio, ok := audit["methodPortfolio"].(map[string]any)
	if !ok {
		return fmt.Errorf("invalid huoke topic executionAudit methodPortfolio")
	}
	if count, ok := huokeTopicExactNonNegativeInt(portfolio["methodCount"]); !ok || count != 12 {
		return fmt.Errorf("invalid huoke topic executionAudit method count")
	}
	tiers, ok := portfolio["tierCounts"].(map[string]any)
	if !ok || !huokeTopicAuditCountEquals(tiers, "tier1", 2) || !huokeTopicAuditCountEquals(tiers, "tier2", 7) || !huokeTopicAuditCountEquals(tiers, "tier3", 3) {
		return fmt.Errorf("invalid huoke topic executionAudit tier counts")
	}
	if strings.TrimSpace(runtimeStringValue(portfolio["selectedMethodId"])) == "" {
		return fmt.Errorf("invalid huoke topic executionAudit selected method")
	}
	if count, ok := huokeTopicExactNonNegativeInt(audit["topicStrengtheningMethodCount"]); !ok || count != 19 {
		return fmt.Errorf("invalid huoke topic executionAudit topic strengthening count")
	}
	atomicization, ok := audit["atomicization"].(map[string]any)
	if !ok {
		return fmt.Errorf("invalid huoke topic executionAudit atomicization")
	}
	if count, ok := huokeTopicExactNonNegativeInt(atomicization["deduplicatedExplorationAtomCount"]); !ok || count < 30 {
		return fmt.Errorf("invalid huoke topic executionAudit exploration atom count")
	}
	if !huokeTopicAuditCountEquals(atomicization, "sourceAtomRetentionRate", 1) || !huokeTopicAuditCountEquals(atomicization, "explorationAtomIntegrationRate", 1) || atomicization["uncompressed"] != true {
		return fmt.Errorf("invalid huoke topic executionAudit atomicization coverage")
	}
	if audit["openingReady"] != true || audit["terminalVisualReady"] != true {
		return fmt.Errorf("invalid huoke topic executionAudit terminal readiness")
	}
	auditLedger, exists := audit["contentStationLedger"]
	if !exists {
		return fmt.Errorf("huoke topic executionAudit missing contentStationLedger")
	}
	if err := validateHuokeTopicContentStationLedger(auditLedger); err != nil {
		return err
	}
	if err := validateHuokeTopicCompletedVideoStations(auditLedger); err != nil {
		return err
	}
	for stationID, value := range runtimeMapValue(auditLedger) {
		outcome := strings.TrimSpace(runtimeStringValue(runtimeMapValue(value)["outcome"]))
		if outcome != "completed" && outcome != "skipped" {
			return fmt.Errorf("huoke topic executionAudit station %s is incomplete", stationID)
		}
	}
	patchLedger, exists := update.Patch["contentStationLedger"]
	if !exists {
		return fmt.Errorf("auto_full_pipeline patch requires contentStationLedger")
	}
	if err := validateHuokeTopicContentStationLedger(patchLedger); err != nil {
		return err
	}
	if err := validateHuokeTopicCompletedVideoStations(patchLedger); err != nil {
		return err
	}
	auditJSON, auditErr := json.Marshal(auditLedger)
	patchJSON, patchErr := json.Marshal(patchLedger)
	if auditErr != nil || patchErr != nil || string(auditJSON) != string(patchJSON) {
		return fmt.Errorf("huoke topic executionAudit ledger does not match patch")
	}
	return nil
}

func huokeTopicStringSlice(value any) []string {
	switch typed := value.(type) {
	case []string:
		return append([]string(nil), typed...)
	case []any:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			text := strings.TrimSpace(runtimeStringValue(item))
			if text == "" {
				return nil
			}
			out = append(out, text)
		}
		return out
	default:
		return nil
	}
}

func huokeTopicAuditCountEquals(value map[string]any, key string, expected int) bool {
	actual, ok := huokeTopicExactNonNegativeInt(value[key])
	return ok && actual == expected
}

func ApplyHuokeTopicStateUpdate(previous map[string]any, update HuokeTopicStateUpdate) (map[string]any, error) {
	previousVersion := HuokeTopicConsultationStateVersion(previous)
	switch update.Kind {
	case "patch":
		if update.BaseStateVersion != previousVersion || update.StateVersion != previousVersion+1 {
			return nil, fmt.Errorf("huoke topic patch base state version conflict")
		}
		state := cloneHuokeTopicStateMap(previous)
		state = huokeTopicMergePatch(state, update.Patch)
		state["schemaVersion"] = HuokeTopicStateSchemaVersion
		state["stateVersion"] = update.StateVersion
		if err := ValidateHuokeTopicConsultationState(state); err != nil {
			return nil, err
		}
		return state, nil
	default:
		return nil, fmt.Errorf("unsupported huoke topic state update")
	}
}

func ValidateHuokeTopicConsultationState(state map[string]any) error {
	if len(state) == 0 {
		return fmt.Errorf("empty huoke topic consultation state")
	}
	if strings.TrimSpace(runtimeStringValue(state["schemaVersion"])) != HuokeTopicStateSchemaVersion {
		return fmt.Errorf("unsupported huoke topic consultation state schema")
	}
	if _, ok := huokeTopicExactPositiveInt(state["stateVersion"]); !ok {
		return fmt.Errorf("invalid huoke topic consultation state version")
	}
	if key := huokeTopicReservedScopeKey(state); key != "" {
		return fmt.Errorf("huoke topic consultation state cannot set backend scope key %s", key)
	}
	if err := validateHuokeTopicJSONValue(state, 0); err != nil {
		return err
	}
	if err := validateHuokeTopicCanonicalShape(state); err != nil {
		return err
	}
	if err := validateHuokeTopicStableObject(state["moduleLedger"], huokeTopicModuleKeys, "moduleLedger"); err != nil {
		return err
	}
	if err := validateHuokeTopicStableObject(state["strategyAssessments"], huokeTopicStrategyKeys, "strategyAssessments"); err != nil {
		return err
	}
	if ledger, exists := state["contentStationLedger"]; exists {
		if err := validateHuokeTopicContentStationLedger(ledger); err != nil {
			return err
		}
	}
	if runtimeStringValue(state["executionMode"]) == "auto_full_pipeline" {
		if strings.TrimSpace(runtimeStringValue(state["journeyPhase"])) != "topic_draft" {
			return fmt.Errorf("auto_full_pipeline must finish at topic_draft")
		}
		ledger, exists := state["contentStationLedger"]
		if !exists {
			return fmt.Errorf("auto_full_pipeline requires contentStationLedger")
		}
		for stationID, value := range runtimeMapValue(ledger) {
			outcome := strings.TrimSpace(runtimeStringValue(runtimeMapValue(value)["outcome"]))
			if outcome == "pending" || outcome == "needs_return" {
				return fmt.Errorf("auto_full_pipeline station %s is incomplete", stationID)
			}
		}
		if err := validateHuokeTopicCompletedVideoStations(ledger); err != nil {
			return err
		}
	}
	encoded, err := json.Marshal(state)
	if err != nil || len(encoded) > huokeTopicStateMaxBytes {
		return fmt.Errorf("huoke topic consultation state is too large")
	}
	return nil
}

func validateHuokeTopicCanonicalShape(state map[string]any) error {
	for _, key := range huokeTopicRequiredStateKeys {
		if _, exists := state[key]; !exists {
			return fmt.Errorf("huoke topic consultation state missing %s", key)
		}
	}
	if !validHuokeTopicProfileContextVersion(runtimeStringValue(state["profileContextVersion"])) {
		return fmt.Errorf("invalid huoke topic profileContextVersion")
	}
	if !huokeTopicEnumValue(state["executionMode"], "guided", "auto_full_pipeline", "auto_select", "supported_override", "fallback_from_override", "targeted_diagnosis", "revise_or_switch") {
		return fmt.Errorf("invalid huoke topic executionMode")
	}
	if !huokeTopicEnumValue(state["subjectStatus"], "exploring", "usable_draft", "awaiting_user_direction", "accepted_for_now", "needs_review", "paused") {
		return fmt.Errorf("invalid huoke topic subjectStatus")
	}
	if !huokeTopicEnumValue(state["strategyAssessmentMode"], "all_11", "supported_override_only", "targeted_diagnosis") {
		return fmt.Errorf("invalid huoke topic strategyAssessmentMode")
	}
	if !huokeTopicSubjectIDs[strings.TrimSpace(runtimeStringValue(state["currentSubjectId"]))] {
		return fmt.Errorf("invalid huoke topic currentSubjectId")
	}
	if next := state["recommendedNextSubjectId"]; next != nil && !huokeTopicSubjectIDs[strings.TrimSpace(runtimeStringValue(next))] {
		return fmt.Errorf("invalid huoke topic recommendedNextSubjectId")
	}
	if decision := state["explicitUserDecision"]; decision != nil && !huokeTopicEnumValue(decision, "deepen", "accept_continue", "revise", "redirect", "freeform", "pause", "delegated_end_to_end", "switch_strategy") {
		return fmt.Errorf("invalid huoke topic explicitUserDecision")
	}
	if scope := state["delegationScope"]; scope != nil && strings.TrimSpace(runtimeStringValue(scope)) == "" {
		return fmt.Errorf("invalid huoke topic delegationScope")
	}
	for _, key := range []string{"currentSubject", "acquisitionContext", "strategyBrief", "evidenceValidation", "topicCandidates", "candidateComparison", "claims", "sideClues", "invalidations"} {
		if _, ok := state[key].(map[string]any); !ok {
			return fmt.Errorf("huoke topic %s must be an object", key)
		}
	}
	for _, key := range []string{"strategyOverride", "finalRecommendation", "resumePoint"} {
		if state[key] != nil {
			if _, ok := state[key].(map[string]any); !ok {
				return fmt.Errorf("huoke topic %s must be an object or null", key)
			}
		}
	}
	for _, key := range []string{"primaryStrategy", "secondaryStrategy", "selectionReason", "leadingTopicCandidateId"} {
		if state[key] != nil {
			if _, ok := state[key].(string); !ok {
				return fmt.Errorf("huoke topic %s must be a string or null", key)
			}
		}
	}
	current := state["currentSubject"].(map[string]any)
	for _, key := range []string{"roundCount", "dryReplyCount", "artifactVersion", "acceptedVersion"} {
		if _, exists := current[key]; !exists {
			return fmt.Errorf("huoke topic currentSubject missing %s", key)
		}
	}
	if value, ok := huokeTopicExactPositiveInt(current["roundCount"]); !ok || value < 1 {
		return fmt.Errorf("invalid huoke topic currentSubject roundCount")
	}
	if _, ok := huokeTopicExactNonNegativeInt(current["dryReplyCount"]); !ok {
		return fmt.Errorf("invalid huoke topic currentSubject dryReplyCount")
	}
	if _, ok := huokeTopicExactPositiveInt(current["artifactVersion"]); !ok {
		return fmt.Errorf("invalid huoke topic currentSubject artifactVersion")
	}
	if current["acceptedVersion"] != nil {
		if _, ok := huokeTopicExactPositiveInt(current["acceptedVersion"]); !ok {
			return fmt.Errorf("invalid huoke topic currentSubject acceptedVersion")
		}
	}
	return nil
}

func newHuokeTopicContentStationLedger() map[string]any {
	ledger := make(map[string]any, len(huokeTopicContentStationKeys))
	for _, stationID := range huokeTopicContentStationKeys {
		applicability := "conditional"
		switch stationID {
		case "C0", "C1", "C2", "C3", "C4", "C5", "C10", "C12", "C13", "C14":
			applicability = "required"
		}
		ledger[stationID] = map[string]any{
			"applicability": applicability,
			"outcome":       "pending",
			"inputs":        map[string]any{},
			"outputs":       map[string]any{},
			"decision":      nil,
			"skipReason":    nil,
			"returnTarget":  nil,
		}
	}
	return ledger
}

func validateHuokeTopicContentStationLedger(value any) error {
	ledger, ok := value.(map[string]any)
	if !ok || len(ledger) != len(huokeTopicContentStationKeys) {
		return fmt.Errorf("huoke topic contentStationLedger must contain C0-C14")
	}
	validStationIDs := make(map[string]bool, len(huokeTopicContentStationKeys))
	for _, stationID := range huokeTopicContentStationKeys {
		validStationIDs[stationID] = true
		record, ok := ledger[stationID].(map[string]any)
		if !ok {
			return fmt.Errorf("huoke topic contentStationLedger item %s must be an object", stationID)
		}
		for _, field := range []string{"applicability", "outcome", "inputs", "outputs", "decision", "skipReason", "returnTarget"} {
			if _, exists := record[field]; !exists {
				return fmt.Errorf("huoke topic contentStationLedger item %s missing %s", stationID, field)
			}
		}
		if !huokeTopicEnumValue(record["applicability"], "required", "conditional", "not_applicable") {
			return fmt.Errorf("invalid huoke topic content station applicability for %s", stationID)
		}
		if !huokeTopicEnumValue(record["outcome"], "pending", "completed", "skipped", "needs_return") {
			return fmt.Errorf("invalid huoke topic content station outcome for %s", stationID)
		}
		if _, ok := record["inputs"].(map[string]any); !ok {
			return fmt.Errorf("huoke topic content station inputs for %s must be an object", stationID)
		}
		if _, ok := record["outputs"].(map[string]any); !ok {
			return fmt.Errorf("huoke topic content station outputs for %s must be an object", stationID)
		}
		for _, field := range []string{"decision", "skipReason"} {
			if record[field] != nil && strings.TrimSpace(runtimeStringValue(record[field])) == "" {
				return fmt.Errorf("invalid huoke topic content station %s for %s", field, stationID)
			}
		}
		outcome := strings.TrimSpace(runtimeStringValue(record["outcome"]))
		applicability := strings.TrimSpace(runtimeStringValue(record["applicability"]))
		if outcome == "skipped" {
			if applicability == "required" || strings.TrimSpace(runtimeStringValue(record["skipReason"])) == "" {
				return fmt.Errorf("invalid skipped huoke topic content station %s", stationID)
			}
		}
		returnTarget := strings.TrimSpace(runtimeStringValue(record["returnTarget"]))
		if outcome == "needs_return" {
			if !validStationIDs[returnTarget] {
				return fmt.Errorf("invalid huoke topic content station return target for %s", stationID)
			}
		} else if record["returnTarget"] != nil {
			return fmt.Errorf("unexpected huoke topic content station return target for %s", stationID)
		}
	}
	return nil
}

func validateHuokeTopicCompletedVideoStations(value any) error {
	ledger := runtimeMapValue(value)
	for _, stationID := range []string{"C0", "C1", "C2", "C3", "C4", "C5", "C10", "C12", "C13", "C14"} {
		record := runtimeMapValue(ledger[stationID])
		if strings.TrimSpace(runtimeStringValue(record["applicability"])) != "required" || strings.TrimSpace(runtimeStringValue(record["outcome"])) != "completed" {
			return fmt.Errorf("auto_full_pipeline required video station %s must be completed", stationID)
		}
	}
	return nil
}

func validHuokeTopicProfileContextVersion(value string) bool {
	parts := strings.Split(strings.TrimSpace(value), "|")
	if len(parts) != 3 {
		return false
	}
	for index, prefix := range []string{"wv:", "iv:", "cg:"} {
		if !strings.HasPrefix(parts[index], prefix) {
			return false
		}
		number := strings.TrimPrefix(parts[index], prefix)
		parsed, err := strconv.ParseInt(number, 10, 64)
		if err != nil || parsed < 0 {
			return false
		}
	}
	return true
}

func huokeTopicEnumValue(value any, allowed ...string) bool {
	actual, ok := value.(string)
	if !ok {
		return false
	}
	actual = strings.TrimSpace(actual)
	for _, candidate := range allowed {
		if actual == candidate {
			return true
		}
	}
	return false
}

func HuokeTopicConsultationStateVersion(state map[string]any) int {
	version, _ := huokeTopicExactPositiveInt(state["stateVersion"])
	return version
}

func CloneHuokeTopicConsultationState(state map[string]any) map[string]any {
	return cloneHuokeTopicStateMap(state)
}

func validateHuokeTopicStableObject(value any, required []string, name string) error {
	object, ok := value.(map[string]any)
	if !ok || len(object) != len(required) {
		return fmt.Errorf("huoke topic %s must contain its stable keys", name)
	}
	for _, key := range required {
		item, exists := object[key]
		if !exists {
			return fmt.Errorf("huoke topic %s missing %s", name, key)
		}
		if _, ok := item.(map[string]any); !ok {
			return fmt.Errorf("huoke topic %s item %s must be an object", name, key)
		}
	}
	return nil
}

func validateHuokeTopicJSONValue(value any, depth int) error {
	if depth > huokeTopicStateMaxDepth {
		return fmt.Errorf("huoke topic consultation state is too deep")
	}
	switch typed := value.(type) {
	case nil, bool, string, json.Number:
		return nil
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return nil
	case float32:
		if math.IsNaN(float64(typed)) || math.IsInf(float64(typed), 0) {
			return fmt.Errorf("invalid huoke topic numeric state value")
		}
		return nil
	case float64:
		if math.IsNaN(typed) || math.IsInf(typed, 0) {
			return fmt.Errorf("invalid huoke topic numeric state value")
		}
		return nil
	case map[string]any:
		for key, item := range typed {
			if strings.TrimSpace(key) == "" || len(key) > 128 {
				return fmt.Errorf("invalid huoke topic state key")
			}
			if err := validateHuokeTopicJSONValue(item, depth+1); err != nil {
				return err
			}
		}
		return nil
	case []any:
		for _, item := range typed {
			if err := validateHuokeTopicJSONValue(item, depth+1); err != nil {
				return err
			}
		}
		return nil
	case []string:
		return nil
	case []map[string]any:
		for _, item := range typed {
			if err := validateHuokeTopicJSONValue(item, depth+1); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("unsupported huoke topic state value")
	}
}

func huokeTopicMergePatch(target, patch map[string]any) map[string]any {
	out := cloneHuokeTopicStateMap(target)
	for key, value := range patch {
		// The station ledger is a complete C0-C14 snapshot. Replacing it as one
		// value preserves explicit null optional fields under JSON merge-patch.
		if key == "contentStationLedger" {
			out[key] = cloneHuokeTopicStateValue(value)
			continue
		}
		if value == nil {
			delete(out, key)
			continue
		}
		patchMap, patchIsMap := value.(map[string]any)
		if !patchIsMap {
			out[key] = cloneHuokeTopicStateValue(value)
			continue
		}
		targetMap, _ := out[key].(map[string]any)
		out[key] = huokeTopicMergePatch(targetMap, patchMap)
	}
	return out
}

func cloneHuokeTopicStateMap(value map[string]any) map[string]any {
	if value == nil {
		return map[string]any{}
	}
	cloned, _ := cloneHuokeTopicStateValue(value).(map[string]any)
	if cloned == nil {
		return map[string]any{}
	}
	return cloned
}

func cloneHuokeTopicStateValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, item := range typed {
			out[key] = cloneHuokeTopicStateValue(item)
		}
		return out
	case []any:
		out := make([]any, len(typed))
		for i, item := range typed {
			out[i] = cloneHuokeTopicStateValue(item)
		}
		return out
	case []string:
		return append([]string(nil), typed...)
	case []map[string]any:
		out := make([]map[string]any, len(typed))
		for i, item := range typed {
			out[i] = cloneHuokeTopicStateMap(item)
		}
		return out
	default:
		return value
	}
}

func huokeTopicExactPositiveInt(value any) (int, bool) {
	result, ok := huokeTopicExactNonNegativeInt(value)
	return result, ok && result > 0
}

func huokeTopicExactNonNegativeInt(value any) (int, bool) {
	var result int64
	switch typed := value.(type) {
	case int:
		if typed < 0 {
			return 0, false
		}
		return typed, true
	case int32:
		result = int64(typed)
	case int64:
		result = typed
	case float64:
		if typed < 0 || typed != math.Trunc(typed) || typed > float64(math.MaxInt) {
			return 0, false
		}
		return int(typed), true
	case json.Number:
		parsed, err := typed.Int64()
		if err != nil {
			return 0, false
		}
		result = parsed
	default:
		return 0, false
	}
	if result < 0 || uint64(result) > uint64(math.MaxInt) {
		return 0, false
	}
	return int(result), true
}

func huokeTopicReservedScopeKey(value map[string]any) string {
	for _, key := range huokeTopicReservedScopeKeys {
		if _, exists := value[key]; exists {
			return key
		}
	}
	return ""
}
