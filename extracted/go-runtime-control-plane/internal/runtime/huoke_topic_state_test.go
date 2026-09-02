package runtime

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestHuokeTopicStatePatchAndVersionRules(t *testing.T) {
	baseline, err := NewHuokeTopicInitialConsultationState("wv:1|iv:1|cg:1")
	if err != nil || HuokeTopicConsultationStateVersion(baseline) != 1 {
		t.Fatalf("create baseline: state=%#v err=%v", baseline, err)
	}
	patchData := map[string]any{"consultationStatePatch": map[string]any{
		"schemaVersion":    HuokeTopicStatePatchSchemaVersion,
		"baseStateVersion": 1,
		"stateVersion":     2,
		"patch": map[string]any{
			"currentSubjectId": "final_topic_guidance",
			"moduleLedger":     map[string]any{"WF-07": map[string]any{"decision": "select candidates"}},
		},
	}}
	patchUpdate, found, err := ParseHuokeTopicStateUpdate(patchData)
	if err != nil || !found || patchUpdate.Kind != "patch" {
		t.Fatalf("parse patch update: update=%#v found=%t err=%v", patchUpdate, found, err)
	}
	applied, err := ApplyHuokeTopicStateUpdate(baseline, patchUpdate)
	if err != nil || HuokeTopicConsultationStateVersion(applied) != 2 || applied["currentSubjectId"] != "final_topic_guidance" {
		t.Fatalf("apply patch update: state=%#v err=%v", applied, err)
	}
	wf07 := runtimeMapValue(runtimeMapValue(applied["moduleLedger"])["WF-07"])
	if wf07["decision"] != "select candidates" {
		t.Fatalf("merge patch did not preserve nested module state: %#v", wf07)
	}

	stale := patchUpdate
	stale.BaseStateVersion = 1
	stale.StateVersion = 2
	if _, err := ApplyHuokeTopicStateUpdate(applied, stale); err == nil {
		t.Fatal("stale patch must be rejected")
	}
}

func TestNewHuokeTopicInitialConsultationStateProvidesPatchableBaseline(t *testing.T) {
	baseline, err := NewHuokeTopicInitialConsultationState("wv:7|iv:3|cg:4")
	if err != nil || HuokeTopicConsultationStateVersion(baseline) != 1 {
		t.Fatalf("baseline=%#v err=%v", baseline, err)
	}
	update, found, err := ParseHuokeTopicStateUpdate(map[string]any{"consultationStatePatch": map[string]any{
		"schemaVersion": HuokeTopicStatePatchSchemaVersion, "baseStateVersion": 1, "stateVersion": 2,
		"patch": map[string]any{"currentSubject": map[string]any{"roundCount": 2}},
	}})
	if err != nil || !found || update.Kind != "patch" {
		t.Fatalf("parse baseline patch: update=%#v found=%t err=%v", update, found, err)
	}
	applied, err := ApplyHuokeTopicStateUpdate(baseline, update)
	if err != nil || HuokeTopicConsultationStateVersion(applied) != 2 {
		t.Fatalf("apply baseline patch: state=%#v err=%v", applied, err)
	}
}

func TestHuokeTopicStateRequiresAllStableModuleAndStrategyKeys(t *testing.T) {
	state := runtimeHuokeTopicTestState(1, "strategy_fit_selection")
	delete(runtimeMapValue(state["moduleLedger"]), "WF-09")
	if err := ValidateHuokeTopicConsultationState(state); err == nil {
		t.Fatal("state missing a workflow module key must be rejected")
	}

	state = runtimeHuokeTopicTestState(1, "strategy_fit_selection")
	delete(runtimeMapValue(state["strategyAssessments"]), "relationship_care")
	if err := ValidateHuokeTopicConsultationState(state); err == nil {
		t.Fatal("state missing a strategy key must be rejected")
	}
}

func TestHuokeTopicStateRequiresCanonicalTopLevelShapeAndEnums(t *testing.T) {
	for _, key := range huokeTopicRequiredStateKeys {
		state := runtimeHuokeTopicTestState(1, "strategy_fit_selection")
		delete(state, key)
		if err := ValidateHuokeTopicConsultationState(state); err == nil {
			t.Fatalf("state missing canonical field %s must be rejected", key)
		}
	}
	for key, value := range map[string]any{
		"profileContextVersion":  "wv:example|iv:1|cg:1",
		"executionMode":          "invented_mode",
		"subjectStatus":          "complete",
		"strategyAssessmentMode": "partial_11",
		"currentSubjectId":       "invented_subject",
		"explicitUserDecision":   "silence_means_accept",
	} {
		state := runtimeHuokeTopicTestState(1, "strategy_fit_selection")
		state[key] = value
		if err := ValidateHuokeTopicConsultationState(state); err == nil {
			t.Fatalf("state with invalid %s must be rejected", key)
		}
	}
}

func TestHuokeTopicStateAcceptsUnifiedExecutionModesAndValidatesContentStations(t *testing.T) {
	for _, mode := range []string{"guided", "auto_full_pipeline"} {
		state := runtimeHuokeTopicTestState(1, "final_topic_guidance")
		state["executionMode"] = mode
		if mode == "auto_full_pipeline" {
			state["journeyPhase"] = "topic_draft"
			ledger := newHuokeTopicContentStationLedger()
			for _, stationID := range huokeTopicContentStationKeys {
				record := runtimeMapValue(ledger[stationID])
				record["outcome"] = "completed"
				record["decision"] = "evaluated"
			}
			state["contentStationLedger"] = ledger
		}
		if err := ValidateHuokeTopicConsultationState(state); err != nil {
			t.Fatalf("valid execution mode %s rejected: %v", mode, err)
		}
	}

	state := runtimeHuokeTopicTestState(1, "strategy_fit_selection")
	ledger := newHuokeTopicContentStationLedger()
	delete(ledger, "C14")
	state["contentStationLedger"] = ledger
	if err := ValidateHuokeTopicConsultationState(state); err == nil {
		t.Fatal("content station ledger missing C14 must be rejected")
	}

	state = runtimeHuokeTopicTestState(1, "strategy_fit_selection")
	ledger = newHuokeTopicContentStationLedger()
	runtimeMapValue(ledger["C6"])["outcome"] = "invented"
	state["contentStationLedger"] = ledger
	if err := ValidateHuokeTopicConsultationState(state); err == nil {
		t.Fatal("invalid content station outcome must be rejected")
	}

	state = runtimeHuokeTopicTestState(1, "final_topic_guidance")
	state["executionMode"] = "auto_full_pipeline"
	state["journeyPhase"] = "topic_draft"
	state["contentStationLedger"] = newHuokeTopicContentStationLedger()
	if err := ValidateHuokeTopicConsultationState(state); err == nil {
		t.Fatal("auto full pipeline with pending stations must be rejected")
	}
}

func TestHuokeTopicAutoFullPipelineRequiresValidMatchingExecutionAudit(t *testing.T) {
	ledger := newHuokeTopicContentStationLedger()
	for _, stationID := range huokeTopicContentStationKeys {
		record := runtimeMapValue(ledger[stationID])
		record["outcome"] = "completed"
		record["decision"] = "evaluated"
	}
	update := HuokeTopicStateUpdate{Kind: "patch", BaseStateVersion: 1, StateVersion: 2, Patch: map[string]any{
		"executionMode":        "auto_full_pipeline",
		"contentStationLedger": cloneHuokeTopicStateMap(ledger),
	}}
	audit := map[string]any{
		"executionMode":          "auto_full_pipeline",
		"completedJourneyPhases": []any{"method_portfolio", "method_discovery", "topic_draft"},
		"methodPortfolio": map[string]any{
			"methodCount":      12,
			"tierCounts":       map[string]any{"tier1": 2, "tier2": 7, "tier3": 3},
			"selectedMethodId": "demand_creation",
		},
		"contentStationLedger":          cloneHuokeTopicStateMap(ledger),
		"topicStrengtheningMethodCount": 19,
		"atomicization": map[string]any{
			"deduplicatedExplorationAtomCount": 30,
			"sourceAtomRetentionRate":          1,
			"explorationAtomIntegrationRate":   1,
			"uncompressed":                     true,
		},
		"openingReady":        true,
		"terminalVisualReady": true,
	}
	data := map[string]any{"executionAudit": audit}
	if err := ValidateHuokeTopicExecutionAudit(data, update); err != nil {
		t.Fatalf("valid auto full pipeline audit rejected: %v", err)
	}
	baseline, err := NewHuokeTopicInitialConsultationState("wv:1|iv:1|cg:1")
	if err != nil {
		t.Fatal(err)
	}
	update.Patch["journeyPhase"] = "topic_draft"
	update.Patch["currentSubjectId"] = "final_topic_guidance"
	update.Patch["subjectStatus"] = "usable_draft"
	if _, err := ApplyHuokeTopicStateUpdate(baseline, update); err != nil {
		t.Fatalf("apply full station ledger with explicit null fields: %v", err)
	}

	if err := ValidateHuokeTopicExecutionAudit(map[string]any{}, update); err == nil {
		t.Fatal("missing auto full pipeline audit must be rejected")
	}

	badAudit := cloneHuokeTopicStateMap(audit)
	runtimeMapValue(badAudit["methodPortfolio"])["methodCount"] = 11
	if err := ValidateHuokeTopicExecutionAudit(map[string]any{"executionAudit": badAudit}, update); err == nil {
		t.Fatal("invalid method count must be rejected")
	}

	badAudit = cloneHuokeTopicStateMap(audit)
	runtimeMapValue(runtimeMapValue(badAudit["contentStationLedger"])["C14"])["decision"] = "different"
	if err := ValidateHuokeTopicExecutionAudit(map[string]any{"executionAudit": badAudit}, update); err == nil {
		t.Fatal("audit and patch station ledgers must match")
	}
}

func TestHuokeTopicStateRejectsOversizedAndUntrustedPatchShapes(t *testing.T) {
	state := runtimeHuokeTopicTestState(1, "strategy_fit_selection")
	state["oversized"] = strings.Repeat("x", huokeTopicStateMaxBytes)
	if err := ValidateHuokeTopicConsultationState(state); err == nil {
		t.Fatal("state beyond the runtime inline limit must be rejected")
	}

	for name, data := range map[string]map[string]any{
		"full state": {
			"consultationState": runtimeHuokeTopicTestState(1, "strategy_fit_selection"),
		},
		"legacy delta": {
			"consultationStateDelta": map[string]any{
				"baseStateVersion": 1,
				"stateVersion":     2,
			},
		},
		"replace schema": {
			"consultationStatePatch": map[string]any{
				"schemaVersion":    HuokeTopicStatePatchSchemaVersion,
				"baseStateVersion": 0,
				"stateVersion":     1,
				"patch":            map[string]any{"schemaVersion": "forged"},
			},
		},
		"set backend scope": {
			"consultationStatePatch": map[string]any{
				"schemaVersion":    HuokeTopicStatePatchSchemaVersion,
				"baseStateVersion": 0,
				"stateVersion":     1,
				"patch":            map[string]any{"workspaceId": "forged_workspace"},
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			if _, found, err := ParseHuokeTopicStateUpdate(data); !found || err == nil {
				t.Fatalf("invalid update accepted: found=%t err=%v", found, err)
			}
		})
	}

	state = runtimeHuokeTopicTestState(1, "strategy_fit_selection")
	state["threadId"] = "forged_thread"
	if err := ValidateHuokeTopicConsultationState(state); err == nil {
		t.Fatal("canonical state must not carry model-provided backend scope identifiers")
	}
}

func TestParseAgentRunResultUsesHuokeFinalAnswerOverUnvalidatedParsedResult(t *testing.T) {
	plan, err := NewAgentProfileResolver().Resolve("work_ai_huoke_topic_strategy")
	if err != nil {
		t.Fatal(err)
	}
	finalAnswer, err := json.Marshal(map[string]any{
		"schemaVersion": "huoke_topic_strategy.result.v1",
		"taskType":      "work_ai_huoke_topic_strategy",
		"skillProfile":  "huoke_topic_strategy",
		"status":        "succeeded",
		"data": map[string]any{
			"reply": "请先说说你想通过账号推广什么产品或服务。",
			"consultationStatePatch": map[string]any{
				"schemaVersion":    HuokeTopicStatePatchSchemaVersion,
				"baseStateVersion": 1,
				"stateVersion":     2,
				"patch":            map[string]any{"currentSubjectId": "audience_conversion_target"},
			},
		},
		"assetWriteIntent": nil,
	})
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := NewOutputParser().ParseAgentRunResult(map[string]any{
		"parsedResult": map[string]any{"data": map[string]any{"reply": "incomplete transport result"}},
		"finalAnswer":  string(finalAnswer),
	}, plan, "run_huoke_final", "task_huoke_final")
	if err != nil {
		t.Fatalf("parse final Huoke envelope: %v", err)
	}
	if parsed["parsedType"] != "huoke_topic_strategy" || runtimeMapValue(parsed["data"])["reply"] != "请先说说你想通过账号推广什么产品或服务。" {
		t.Fatalf("final Huoke envelope was not authoritative: %#v", parsed)
	}
}

func TestParseAgentRunResultAcceptsHuokeTopicPlainMarkdownWithoutStatePatch(t *testing.T) {
	plan, err := NewAgentProfileResolver().Resolve("work_ai_huoke_topic_strategy")
	if err != nil {
		t.Fatal(err)
	}
	answer := "## Topic direction\n\nStart from the buyer's immediate concern, then ask one concrete follow-up."
	parsed, err := NewOutputParser().ParseAgentRunResult(map[string]any{
		"finalAnswer": answer,
	}, plan, "run_huoke_markdown", "task_huoke_markdown")
	if err != nil {
		t.Fatalf("parse plain Huoke Markdown: %v", err)
	}
	data := runtimeMapValue(parsed["data"])
	if parsed["parsedType"] != "huoke_topic_strategy" || data["reply"] != answer {
		t.Fatalf("plain Huoke Markdown was not preserved: %#v", parsed)
	}
	if _, exists := data["consultationStatePatch"]; exists {
		t.Fatalf("plain Huoke Markdown must not invent a state patch: %#v", data)
	}
}

func TestParseAgentRunResultRejectsHuokeTopicProtocolLikeMarkdown(t *testing.T) {
	plan, err := NewAgentProfileResolver().Resolve("work_ai_huoke_topic_strategy")
	if err != nil {
		t.Fatal(err)
	}
	answer := "Here is the result:\n```json\n{\"schemaVersion\":\"huoke_topic_strategy.result.v1\",\"data\":{\"reply\":\"incomplete\"}}\n```"
	if _, err := NewOutputParser().ParseAgentRunResult(map[string]any{
		"finalAnswer": answer,
	}, plan, "run_huoke_protocol", "task_huoke_protocol"); err == nil {
		t.Fatal("protocol-like Huoke Markdown must not use the plain reply fallback")
	}
}

func TestParseAgentRunResultRepairsOnlyHuokeDataReplyObjectTypo(t *testing.T) {
	plan, err := NewAgentProfileResolver().Resolve("work_ai_huoke_topic_strategy")
	if err != nil {
		t.Fatal(err)
	}
	malformed := `{"schemaVersion":"huoke_topic_strategy.result.v1","taskType":"work_ai_huoke_topic_strategy","skillProfile":"huoke_topic_strategy","status":"succeeded","data":"reply":"usable guidance","consultationStatePatch":{"schemaVersion":"huoke_topic_strategy.state_patch.v1","baseStateVersion":3,"stateVersion":4,"patch":{"journeyPhase":"topic_draft","currentSubjectId":"strategy_fit_selection","subjectStatus":"usable_draft","profileContextVersion":"wv:1|iv:1|cg:1"}}},"assetWriteIntent":null}`
	parsed, err := NewOutputParser().ParseAgentRunResult(map[string]any{
		"finalAnswer": malformed,
	}, plan, "run_huoke_typo", "task_huoke_typo")
	if err != nil {
		t.Fatalf("repair observed Huoke data/reply typo: %v", err)
	}
	data := runtimeMapValue(parsed["data"])
	if parsed["parsedType"] != "huoke_topic_strategy" || data["reply"] != "usable guidance" {
		t.Fatalf("unexpected repaired result: %#v", parsed)
	}
	if _, found, err := ParseHuokeTopicStateUpdate(data); err != nil || !found {
		t.Fatalf("state patch must still be validated after repair: found=%t err=%v data=%#v", found, err, data)
	}

	unsafe := strings.Replace(malformed, `"data":"reply":`, `"data":"headline":`, 1)
	if _, err := NewOutputParser().ParseAgentRunResult(map[string]any{
		"finalAnswer": unsafe,
	}, plan, "run_huoke_unrelated_typo", "task_huoke_unrelated_typo"); err == nil {
		t.Fatal("unrelated malformed JSON must not be repaired")
	}
}

func runtimeHuokeTopicTestState(version int, subject string) map[string]any {
	modules := map[string]any{}
	for _, key := range huokeTopicModuleKeys {
		modules[key] = map[string]any{
			"applicability": "required",
			"status":        "not_started",
			"decision":      nil,
			"inputs":        map[string]any{},
			"reasoning":     []any{},
			"outputs":       map[string]any{},
			"sourceRefs":    []any{},
		}
	}
	strategies := map[string]any{}
	for _, key := range huokeTopicStrategyKeys {
		strategies[key] = map[string]any{
			"applicability": "required",
			"status":        "conditional",
		}
	}
	return map[string]any{
		"schemaVersion":            HuokeTopicStateSchemaVersion,
		"stateVersion":             version,
		"profileContextVersion":    "wv:1|iv:1|cg:1",
		"executionMode":            "auto_select",
		"delegationScope":          "topic_guidance_end_to_end",
		"currentSubjectId":         subject,
		"subjectStatus":            "exploring",
		"explicitUserDecision":     "delegated_end_to_end",
		"recommendedNextSubjectId": nil,
		"currentSubject": map[string]any{
			"roundCount": 1, "dryReplyCount": 0, "artifactVersion": version, "acceptedVersion": nil,
		},
		"acquisitionContext":      map[string]any{},
		"moduleLedger":            modules,
		"strategyOverride":        nil,
		"strategyAssessmentMode":  "all_11",
		"strategyAssessments":     strategies,
		"primaryStrategy":         nil,
		"secondaryStrategy":       nil,
		"selectionReason":         nil,
		"strategyBrief":           map[string]any{},
		"evidenceValidation":      map[string]any{},
		"topicCandidates":         map[string]any{},
		"leadingTopicCandidateId": nil,
		"candidateComparison":     map[string]any{},
		"finalRecommendation":     nil,
		"claims":                  map[string]any{},
		"sideClues":               map[string]any{},
		"invalidations":           map[string]any{},
		"resumePoint":             nil,
	}
}
