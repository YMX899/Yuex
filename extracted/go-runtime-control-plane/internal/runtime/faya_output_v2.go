package runtime

import (
	"regexp"
	"strings"
)

const (
	fayaGerminationV2PromptTemplate = "work_ai.faya_germination.v2"
	fayaGerminationV2Schema         = "viewpoint_germination.result.v2"
)

var fayaPopulationClaimPattern = regexp.MustCompile(`(?:\b(?:all|most|usually|typically|customers|readers|consumers|people)\b|所有(?:人|客户|读者|消费者)|大多数|通常|一般(?:人|客户|读者|消费者)?|客户(?:都|通常)|读者(?:都|通常)|消费者(?:都|通常)|人们(?:都|通常))`)

func runtimeFayaV2ResultContract(task ProfilePlan) bool {
	return task.TaskType == "work_ai_faya_germination" &&
		task.PromptTemplateID == fayaGerminationV2PromptTemplate &&
		task.OutputSchemaVersion == fayaGerminationV2Schema
}

func validateFayaGerminationV2(result RuntimeParsedResult) error {
	if err := validateFayaGerminationV2Envelope(result); err != nil {
		return err
	}
	switch strings.TrimSpace(result.Status) {
	case "succeeded":
		return validateFayaV2SucceededData(result.Data)
	case "no_viable_seed":
		return validateFayaV2NoViableData(result.Data)
	default:
		return parseError("AI_RESULT_PARSE_FAILED")
	}
}

func validateFayaGerminationV2Envelope(result RuntimeParsedResult) error {
	if result.SchemaVersion != fayaGerminationV2Schema || result.TaskType != "work_ai_faya_germination" || result.SkillProfile != "viewpoint_germination" {
		return parseError("SKILL_TASK_MISMATCH")
	}
	if len(result.AssetWriteIntent) > 0 {
		return parseError("WORKSPACE_WRITE_FAILED")
	}
	switch strings.TrimSpace(result.Status) {
	case "succeeded", "no_viable_seed":
		return nil
	default:
		return parseError("AI_RESULT_PARSE_FAILED")
	}
}

// normalizeFayaGerminationV2 keeps the chat contract available when only the
// hidden analytical report is malformed. It never relaxes envelope safeguards.
func normalizeFayaGerminationV2(result *RuntimeParsedResult) error {
	if result == nil {
		return parseError("AI_RESULT_PARSE_FAILED")
	}
	if err := validateFayaGerminationV2Envelope(*result); err != nil {
		return err
	}
	// The reply-only compatibility shape is for incomplete hidden report data,
	// never for an explicit scope violation that would widen a named scene.
	if err := validateFayaV2CompatibilityScope(result.Data); err != nil {
		return err
	}
	if err := validateFayaGerminationV2(*result); err == nil {
		return nil
	}
	reply := strings.TrimSpace(parserString(result.Data["reply"]))
	if reply == "" {
		return parseError("AI_RESULT_PARSE_FAILED")
	}
	result.Data = map[string]any{"reply": reply}
	return nil
}

func validateFayaV2CompatibilityScope(data map[string]any) error {
	report, ok := fayaMap(data["report"])
	if !ok {
		return nil
	}
	insights := make([]map[string]any, 0, 1+len(mapSlice(report["branches"])))
	if core, ok := fayaMap(report["coreInsight"]); ok {
		insights = append(insights, core)
	}
	insights = append(insights, mapSlice(report["branches"])...)
	for _, insight := range insights {
		if fayaV2NamedSceneUsesPopulationClaim(insight) {
			return parseError("AI_RESULT_PARSE_FAILED")
		}
	}
	return nil
}

func validateFayaV2SucceededData(data map[string]any) error {
	if strings.TrimSpace(parserString(data["reply"])) == "" {
		return parseError("AI_RESULT_PARSE_FAILED")
	}
	report, ok := fayaMap(data["report"])
	if !ok {
		return parseError("AI_RESULT_PARSE_FAILED")
	}
	if !fayaAllowed(parserString(report["mode"]), "phenomenon", "field_record", "viewpoint", "experience", "research") {
		return parseError("AI_RESULT_PARSE_FAILED")
	}
	creatorWorld, ok := fayaMap(report["creatorWorld"])
	if !ok || fayaRequiredString(creatorWorld, "domain") == "" || fayaRequiredString(creatorWorld, "interpretivePosition") == "" {
		return parseError("AI_RESULT_PARSE_FAILED")
	}
	creatorAnchorIDs, ok := fayaRequiredStrings(creatorWorld, "anchorIds")
	if !ok {
		return parseError("AI_RESULT_PARSE_FAILED")
	}
	materialReading, ok := fayaMap(report["materialReading"])
	if !ok || fayaRequiredString(materialReading, "surfaceAccount") == "" {
		return parseError("AI_RESULT_PARSE_FAILED")
	}
	baselineIDs, ok := fayaRequiredStrings(materialReading, "baselineClaimIds")
	if !ok {
		return parseError("AI_RESULT_PARSE_FAILED")
	}
	if _, ok := fayaRequiredStrings(materialReading, "tensionIds"); !ok {
		return parseError("AI_RESULT_PARSE_FAILED")
	}
	core, ok := fayaMap(report["coreInsight"])
	if !ok {
		return parseError("AI_RESULT_PARSE_FAILED")
	}
	mechanismKeys := map[string]struct{}{}
	if err := validateFayaV2Insight(core, baselineIDs, creatorAnchorIDs, mechanismKeys, true); err != nil {
		return err
	}
	for _, branch := range mapSlice(report["branches"]) {
		if err := validateFayaV2Insight(branch, baselineIDs, creatorAnchorIDs, mechanismKeys, false); err != nil {
			return err
		}
	}
	return nil
}

func validateFayaV2NoViableData(data map[string]any) error {
	if strings.TrimSpace(parserString(data["reply"])) == "" {
		return parseError("AI_RESULT_PARSE_FAILED")
	}
	if !fayaAllowed(parserString(data["reason"]), "missing_profile_anchor", "insufficient_material", "no_real_bridge", "source_repetition_only", "qualification_boundary", "evidence_risk") {
		return parseError("AI_RESULT_PARSE_FAILED")
	}
	if report, exists := data["report"]; exists && report != nil {
		return parseError("AI_RESULT_PARSE_FAILED")
	}
	return nil
}

func validateFayaV2Insight(insight map[string]any, baselineIDs, creatorAnchorIDs []string, mechanismKeys map[string]struct{}, core bool) error {
	for _, key := range []string{"insightId", "form", "claim", "semanticDelta", "mechanismKey"} {
		if fayaRequiredString(insight, key) == "" {
			return parseError("AI_RESULT_PARSE_FAILED")
		}
	}
	if !fayaAllowed(parserString(insight["form"]), "mechanism", "relation", "tradeoff", "condition", "principle", "naming", "experience", "question") {
		return parseError("AI_RESULT_PARSE_FAILED")
	}
	if core {
		if !fayaAllowed(parserString(insight["depthMove"]), "downward", "horizontal", "upward", "reverse", "reframe", "naming") || fayaRequiredString(insight, "latentStructure") == "" {
			return parseError("AI_RESULT_PARSE_FAILED")
		}
		if fayaRequiredString(insight, "profileContribution") == "" {
			return parseError("AI_RESULT_PARSE_FAILED")
		}
	}
	if !fayaStringIn(parserString(insight["nearestSourceClaimId"]), baselineIDs) {
		return parseError("AI_RESULT_PARSE_FAILED")
	}
	if _, ok := fayaRequiredStrings(insight, "materialAtomIds"); !ok {
		return parseError("AI_RESULT_PARSE_FAILED")
	}
	profileAnchorIDs, ok := fayaRequiredStrings(insight, "profileAnchorIds")
	if !ok || !fayaAllIn(profileAnchorIDs, creatorAnchorIDs) {
		return parseError("AI_RESULT_PARSE_FAILED")
	}
	mechanismKey := strings.TrimSpace(parserString(insight["mechanismKey"]))
	if _, duplicate := mechanismKeys[mechanismKey]; duplicate {
		return parseError("AI_RESULT_PARSE_FAILED")
	}
	mechanismKeys[mechanismKey] = struct{}{}
	scope, ok := fayaMap(insight["scope"])
	if !ok || !fayaAllowed(parserString(scope["evidenceStatus"]), "interpretation", "hypothesis", "verified") || !fayaAllowed(parserString(scope["populationScope"]), "named_scene", "source_scope", "externally_verified_scope") {
		return parseError("AI_RESULT_PARSE_FAILED")
	}
	if strings.TrimSpace(parserString(scope["failsWhen"])) == "" && strings.TrimSpace(parserString(scope["verificationNeed"])) == "" {
		return parseError("AI_RESULT_PARSE_FAILED")
	}
	if fayaV2NamedSceneUsesPopulationClaim(insight) {
		return parseError("AI_RESULT_PARSE_FAILED")
	}
	return nil
}

func fayaV2NamedSceneUsesPopulationClaim(insight map[string]any) bool {
	scope, ok := fayaMap(insight["scope"])
	return ok && parserString(scope["populationScope"]) == "named_scene" &&
		fayaPopulationClaimPattern.MatchString(parserString(insight["claim"])+" "+parserString(insight["latentStructure"]))
}

func fayaMap(value any) (map[string]any, bool) {
	mapped, ok := value.(map[string]any)
	return mapped, ok && len(mapped) > 0
}

func fayaRequiredString(values map[string]any, key string) string {
	return strings.TrimSpace(parserString(values[key]))
}

func fayaRequiredStrings(values map[string]any, key string) ([]string, bool) {
	raw, exists := values[key]
	if !exists {
		return nil, false
	}
	items := []string{}
	switch typed := raw.(type) {
	case []string:
		items = append(items, typed...)
	case []any:
		for _, item := range typed {
			value := strings.TrimSpace(parserString(item))
			if value == "" {
				return nil, false
			}
			items = append(items, value)
		}
	default:
		return nil, false
	}
	if len(items) == 0 {
		return nil, false
	}
	return items, true
}

func fayaAllowed(value string, allowed ...string) bool {
	for _, candidate := range allowed {
		if strings.TrimSpace(value) == candidate {
			return true
		}
	}
	return false
}

func fayaStringIn(value string, values []string) bool {
	for _, candidate := range values {
		if value == candidate {
			return true
		}
	}
	return false
}

func fayaAllIn(values, allowed []string) bool {
	for _, value := range values {
		if !fayaStringIn(value, allowed) {
			return false
		}
	}
	return true
}
