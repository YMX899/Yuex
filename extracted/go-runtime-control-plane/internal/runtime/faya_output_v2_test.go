package runtime

import (
	"encoding/json"
	"testing"
)

func TestValidateFayaV2InsightAllowsNonCausalCoreMoves(t *testing.T) {
	for _, testCase := range []struct {
		name      string
		form      string
		depthMove string
	}{
		{name: "reframe", form: "question", depthMove: "reframe"},
		{name: "naming", form: "naming", depthMove: "naming"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			insight := map[string]any{
				"insightId":            "insight-001",
				"form":                 testCase.form,
				"claim":                "这不是原文表面的结论。",
				"semanticDelta":        "它让一个未被说清的问题显露出来。",
				"mechanismKey":         testCase.depthMove,
				"depthMove":            testCase.depthMove,
				"latentStructure":      "材料中的未被命名结构。",
				"profileContribution":  "创作者的真实工作位置使这个结构可见。",
				"nearestSourceClaimId": "claim-001",
				"materialAtomIds":      []any{"atom-001"},
				"profileAnchorIds":     []any{"anchor-001"},
				"scope": map[string]any{
					"evidenceStatus":   "interpretation",
					"populationScope":  "named_scene",
					"failsWhen":        "材料没有同类的具体语境时。",
					"verificationNeed": nil,
				},
			}

			if err := validateFayaV2Insight(insight, []string{"claim-001"}, []string{"anchor-001"}, map[string]struct{}{}, true); err != nil {
				t.Fatalf("core insight with %s depth move was rejected: %v", testCase.depthMove, err)
			}
		})
	}
}

func TestOutputParserFayaV2UsesReplyCompatibilityShapeForMalformedReport(t *testing.T) {
	result := fayaV2TestResult()
	report := result.Data["report"].(map[string]any)
	delete(report["creatorWorld"].(map[string]any), "interpretivePosition")
	branch := fayaV2TestInsight("branch-001", "branch-mechanism")
	delete(branch, "nearestSourceClaimId")
	report["branches"] = []any{branch}

	raw, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("marshal result: %v", err)
	}
	parsed, err := NewOutputParser().ParseFinalAnswer(string(raw), fayaV2TestPlan())
	if err != nil {
		t.Fatalf("malformed hidden report should keep the reply: %v", err)
	}
	if got, want := parsed.Data["reply"], "Visible reply"; got != want {
		t.Fatalf("reply = %q, want %q", got, want)
	}
	if _, exists := parsed.Data["report"]; exists {
		t.Fatal("malformed hidden report must not be persisted")
	}
}

func TestOutputParserFayaV2UsesReplyCompatibilityShapeWithoutReport(t *testing.T) {
	result := fayaV2TestResult()
	delete(result.Data, "report")

	raw, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("marshal result: %v", err)
	}
	parsed, err := NewOutputParser().ParseFinalAnswer(string(raw), fayaV2TestPlan())
	if err != nil {
		t.Fatalf("missing hidden report should keep the reply: %v", err)
	}
	if got, want := parsed.Data["reply"], "Visible reply"; got != want {
		t.Fatalf("reply = %q, want %q", got, want)
	}
}

func TestOutputParserFayaV2DoesNotDowngradeNamedScenePopulationClaim(t *testing.T) {
	result := fayaV2TestResult()
	report := result.Data["report"].(map[string]any)
	delete(report["creatorWorld"].(map[string]any), "interpretivePosition")
	report["coreInsight"].(map[string]any)["claim"] = "Most customers face this same tradeoff."

	raw, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("marshal result: %v", err)
	}
	if _, err := NewOutputParser().ParseFinalAnswer(string(raw), fayaV2TestPlan()); err == nil {
		t.Fatal("named_scene population claim must not be downgraded to reply-only output")
	}
}

func TestOutputParserFayaV2RejectsUnsafeCompatibilityEnvelopes(t *testing.T) {
	for _, testCase := range []struct {
		name   string
		mutate func(*RuntimeParsedResult)
	}{
		{
			name: "wrong schema",
			mutate: func(result *RuntimeParsedResult) {
				result.SchemaVersion = "other.result.v1"
			},
		},
		{
			name: "wrong task",
			mutate: func(result *RuntimeParsedResult) {
				result.TaskType = "work_ai_general_chat"
			},
		},
		{
			name: "asset write intent",
			mutate: func(result *RuntimeParsedResult) {
				result.AssetWriteIntent = map[string]any{"operation": "write_file"}
			},
		},
		{
			name: "empty reply",
			mutate: func(result *RuntimeParsedResult) {
				result.Data["reply"] = ""
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			result := fayaV2TestResult()
			testCase.mutate(&result)
			raw, err := json.Marshal(result)
			if err != nil {
				t.Fatalf("marshal result: %v", err)
			}
			if _, err := NewOutputParser().ParseFinalAnswer(string(raw), fayaV2TestPlan()); err == nil {
				t.Fatal("unsafe Faya V2 envelope was accepted")
			}
		})
	}
}

func fayaV2TestPlan() ProfilePlan {
	return ProfilePlan{
		TaskType:            "work_ai_faya_germination",
		SkillProfile:        "viewpoint_germination",
		PromptTemplateID:    fayaGerminationV2PromptTemplate,
		OutputSchemaVersion: fayaGerminationV2Schema,
	}
}

func fayaV2TestResult() RuntimeParsedResult {
	return RuntimeParsedResult{
		SchemaVersion: fayaGerminationV2Schema,
		TaskType:      "work_ai_faya_germination",
		SkillProfile:  "viewpoint_germination",
		Status:        "succeeded",
		Data: map[string]any{
			"reply": "Visible reply",
			"report": map[string]any{
				"mode": "viewpoint",
				"creatorWorld": map[string]any{
					"domain":               "flower retail",
					"interpretivePosition": "the creator observes retail work",
					"anchorIds":            []any{"anchor-001"},
				},
				"materialReading": map[string]any{
					"surfaceAccount":   "A workplace observation.",
					"baselineClaimIds": []any{"claim-001"},
					"tensionIds":       []any{"tension-001"},
				},
				"coreInsight": fayaV2TestInsight("core-001", "core-mechanism"),
			},
		},
	}
}

func fayaV2TestInsight(id, mechanismKey string) map[string]any {
	return map[string]any{
		"insightId":            id,
		"form":                 "mechanism",
		"claim":                "The named workplace exposes a repeatable tradeoff.",
		"semanticDelta":        "The tradeoff is not explicit in the source account.",
		"mechanismKey":         mechanismKey,
		"depthMove":            "downward",
		"latentStructure":      "The observation has a hidden operating constraint.",
		"profileContribution":  "The creator can recognize the operating constraint.",
		"nearestSourceClaimId": "claim-001",
		"materialAtomIds":      []any{"atom-001"},
		"profileAnchorIds":     []any{"anchor-001"},
		"scope": map[string]any{
			"evidenceStatus":  "interpretation",
			"populationScope": "named_scene",
			"failsWhen":       "The observed scene materially changes.",
		},
	}
}
