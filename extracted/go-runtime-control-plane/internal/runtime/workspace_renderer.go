package runtime

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type RuntimeWorkspaceRenderCommand struct {
	RunID              string
	WorkspaceDir       string
	Plan               ProfilePlan
	Task               RuntimeRunCommand
	Prompt             PromptSnapshot
	Inputs             map[string]string
	SkillContent       string
	SourceWorkspaceDir string
}

type RuntimeWorkspaceRenderer struct {
	Mirror MetaMirror
}

const (
	rensheRuntimeFallbackAgents = "# Renshe Runtime\n\nUse the loaded `renshe_content_creation` Skill and follow the user's current request. This task always stays in one content-creation workflow: process every supplied input and keep Profile as optional creative evidence, never a prerequisite. Do not route into positioning consultation, expose runtime mechanics, or invent facts.\n"
	rensheRuntimeFallbackSoul   = "# Renshe Soul\n\nUnderstand the material's audience value first, then shape it with relevant industry, product, audience, and context. Keep facts distinct from interpretation, and use real personal evidence only when it strengthens the content without taking over.\n"
	rensheRuntimeFallbackTools  = "# Tool Rules\n\nUse the current request first. Read only files that materially help the current answer, and do not expose internal paths or tool mechanics.\n"
)

func NewRuntimeWorkspaceRenderer() RuntimeWorkspaceRenderer {
	return NewRuntimeWorkspaceRendererWithMirror(NewMetaMirror(os.Getenv("HUAHUO_RUNTIME_CONFIG_ROOT")))
}

func NewRuntimeWorkspaceRendererWithMirror(mirror MetaMirror) RuntimeWorkspaceRenderer {
	if strings.TrimSpace(mirror.ConfigRoot) == "" {
		mirror = NewMetaMirror("")
	}
	return RuntimeWorkspaceRenderer{Mirror: mirror}
}

func (r RuntimeWorkspaceRenderer) RenderDetachedWorkspace(command RuntimeWorkspaceRenderCommand) error {
	_, err := r.RenderRuntimeWorkspace(command)
	return err
}

func (r RuntimeWorkspaceRenderer) RenderRunWorkspace(command RuntimeWorkspaceRenderCommand) error {
	_, err := r.RenderRuntimeWorkspace(command)
	return err
}

func (r RuntimeWorkspaceRenderer) RenderRuntimeWorkspace(command RuntimeWorkspaceRenderCommand) (map[string]any, error) {
	if err := validateRuntimeRenderCommand(command); err != nil {
		return nil, err
	}
	if command.Plan.ExecutionScope == ScopeProductThread && isFormalWorkspacePath(command.WorkspaceDir) {
		return r.renderFormalProductThreadWorkspace(command)
	}
	if err := AssertRuntimeRoot(command.WorkspaceDir); err != nil {
		return nil, err
	}
	dirs := []string{
		command.WorkspaceDir,
		filepath.Join(command.WorkspaceDir, "input"),
		filepath.Join(command.WorkspaceDir, "output"),
		filepath.Join(command.WorkspaceDir, "output", "artifacts"),
	}
	if command.Plan.ExecutionScope == ScopeDetachedTask {
		if strings.TrimSpace(command.SkillContent) == "" {
			return nil, fmt.Errorf("RUNTIME_INPUT_INVALID")
		}
		dirs = append(dirs, filepath.Join(command.WorkspaceDir, "skills"), filepath.Join(command.WorkspaceDir, "skills", command.Plan.SkillProfile))
	}
	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0o750); err != nil {
			return nil, err
		}
	}

	renderedFiles := []string{}
	if command.Plan.ExecutionScope == ScopeDetachedTask {
		files, err := r.renderDetachedWorkspaceTemplates(command)
		if err != nil {
			return nil, err
		}
		renderedFiles = append(renderedFiles, files...)
	} else {
		standardFiles := map[string]string{
			"AGENTS.md": runtimeAgentsBoundary(command.Plan),
			"SOUL.md":   "Runtime workspace.\n",
			"USER.md":   "Workspace ID: " + command.Task.WorkspaceID + "\n",
			"TOOLS.md":  runtimeToolsBoundary(command.Plan),
			"MEMORY.md": "No long-term memory in runtime workspace.\n",
		}
		for name, content := range standardFiles {
			path := filepath.Join(command.WorkspaceDir, name)
			if _, err := os.Stat(path); os.IsNotExist(err) {
				if err := os.WriteFile(path, []byte(content), 0o640); err != nil {
					return nil, err
				}
				renderedFiles = append(renderedFiles, filepath.ToSlash(name))
			} else if err != nil {
				return nil, err
			}
		}
	}

	snapshotFiles, inputFiles, err := writeRuntimeInputSnapshot(command)
	if err != nil {
		return nil, err
	}
	renderedFiles = append(renderedFiles, snapshotFiles...)
	if command.Plan.ExecutionScope == ScopeDetachedTask {
		skillRel := filepath.Join("skills", command.Plan.SkillProfile, "SKILL.md")
		skillPath, err := runtimeWorkspaceSafeJoin(command.WorkspaceDir, skillRel)
		if err != nil {
			return nil, err
		}
		if err := os.WriteFile(skillPath, []byte(command.SkillContent), 0o640); err != nil {
			return nil, err
		}
		renderedFiles = append(renderedFiles, filepath.ToSlash(skillRel))
	}
	if command.Plan.TaskType == "work_ai_renshe_content" {
		evidenceFiles, err := renderRensheEvidenceView(command.WorkspaceDir, command.SourceWorkspaceDir, command.SkillContent, r.Mirror.ConfigRoot, command.Task.Context.UserRequest)
		if err != nil {
			return nil, err
		}
		renderedFiles = append(renderedFiles, evidenceFiles...)
	}
	sort.Strings(renderedFiles)
	return map[string]any{
		"runId":          command.RunID,
		"taskType":       command.Plan.TaskType,
		"skillProfile":   command.Plan.SkillProfile,
		"executionScope": string(command.Plan.ExecutionScope),
		"files":          renderedFiles,
		"inputFiles":     inputFiles,
		"promptHash":     command.Prompt.Hash,
	}, nil
}

func (r RuntimeWorkspaceRenderer) renderFormalProductThreadWorkspace(command RuntimeWorkspaceRenderCommand) (map[string]any, error) {
	if command.Plan.WorkspaceMode != WorkspaceModeFormalUserWorkspace && command.Plan.WorkspaceMode != WorkspaceModeFormalUserWorkspaceInputSnapshot {
		return nil, fmt.Errorf("RUNTIME_INPUT_INVALID")
	}
	effectiveSkillHash, err := r.validateFormalWorkspaceSelectedSkill(command)
	if err != nil {
		return nil, err
	}
	quarantinedArtifacts, err := r.quarantineFormalWorkspaceRuntimeArtifacts(command)
	if err != nil {
		return nil, err
	}
	renderedFiles := []string{}
	inputFiles := []string{}
	if command.Plan.WorkspaceMode == WorkspaceModeFormalUserWorkspaceInputSnapshot {
		renderedFiles, inputFiles, err = writeRuntimeInputSnapshot(command)
		if err != nil {
			return nil, err
		}
	}
	return map[string]any{
		"runId":                       command.RunID,
		"taskType":                    command.Plan.TaskType,
		"skillProfile":                command.Plan.SkillProfile,
		"executionScope":              string(command.Plan.ExecutionScope),
		"workspaceMode":               string(command.Plan.WorkspaceMode),
		"files":                       renderedFiles,
		"inputFiles":                  inputFiles,
		"promptHash":                  command.Prompt.Hash,
		"effectiveSkillHash":          effectiveSkillHash,
		"effectiveSkillSource":        "formal_user_workspace_read_only",
		"quarantinedArtifacts":        quarantinedArtifacts,
		"quarantinedArtifactCount":    len(quarantinedArtifacts),
		"requiresHostMaterialization": true,
	}, nil
}

func writeRuntimeInputSnapshot(command RuntimeWorkspaceRenderCommand) ([]string, []string, error) {
	for _, dir := range []string{
		filepath.Join(command.WorkspaceDir, "input"),
		filepath.Join(command.WorkspaceDir, "output"),
		filepath.Join(command.WorkspaceDir, "output", "artifacts"),
	} {
		if err := os.MkdirAll(dir, 0o750); err != nil {
			return nil, nil, err
		}
	}
	files := map[string]string{
		filepath.Join("input", "user_request.md"): command.Task.Context.UserRequest,
		filepath.Join("input", "context.md"):      command.Task.Context.Context,
	}
	safeInputs := redactRuntimeTaskParameters(command.Task.Context.Inputs)
	renderTask := command.Task
	renderTask.Context = RuntimeContextBundle{
		UserRequest: redactRuntimeTaskText(command.Task.Context.UserRequest),
		Context:     redactRuntimeTaskText(command.Task.Context.Context),
		Inputs:      safeInputs,
	}
	taskJSON, _ := json.MarshalIndent(renderTask, "", "  ")
	files[filepath.Join("input", "task.json")] = string(taskJSON)
	assetsJSON, _ := json.MarshalIndent(safeInputs, "", "  ")
	if len(assetsJSON) == 0 || string(assetsJSON) == "null" {
		assetsJSON = []byte("{}")
	}
	files[filepath.Join("input", "assets.json")] = string(assetsJSON)
	for name, content := range command.Inputs {
		rel, err := runtimeInputRelativePath(name)
		if err != nil {
			return nil, nil, err
		}
		if isReservedRuntimeInputPath(rel) {
			return nil, nil, fmt.Errorf("RUNTIME_INPUT_INVALID")
		}
		files[rel] = content
	}
	renderedFiles := make([]string, 0, len(files))
	for name, content := range files {
		path, err := runtimeWorkspaceSafeJoin(command.WorkspaceDir, name)
		if err != nil {
			return nil, nil, err
		}
		if err := os.WriteFile(path, []byte(content), 0o640); err != nil {
			return nil, nil, err
		}
		renderedFiles = append(renderedFiles, filepath.ToSlash(name))
	}
	inputFiles := []string{"input/assets.json", "input/context.md", "input/task.json", "input/user_request.md"}
	for name := range command.Inputs {
		rel, err := runtimeInputRelativePath(name)
		if err != nil {
			return nil, nil, err
		}
		inputFiles = append(inputFiles, filepath.ToSlash(rel))
	}
	sort.Strings(renderedFiles)
	sort.Strings(inputFiles)
	return renderedFiles, inputFiles, nil
}

func renderRensheEvidenceView(viewDir, sourceDir, skillContent, configRoot, userRequest string) ([]string, error) {
	candidates := []string{}
	if strings.TrimSpace(sourceDir) != "" {
		if err := AssertRuntimeRoot(sourceDir); err != nil {
			return nil, err
		}
		candidates = append(candidates,
			"resources/overview.md",
			"resources/materials.md",
			"resources/profile.md",
			"resources/creative.md",
			"resources/files.md",
			"profile/preference-boundaries.md",
			"profile/user-positioning/positioning-profile.md",
			"内容.md",
		)
		materialCandidates, err := rensheMaterialCandidatesFromIndex(sourceDir, userRequest)
		if err != nil {
			return nil, err
		}
		candidates = append(candidates, materialCandidates...)
	}
	rendered := []string{}
	copied := []string{}
	seen := map[string]bool{}
	copiedSet := map[string]bool{}
	for _, rel := range candidates {
		rel = filepath.ToSlash(filepath.Clean(filepath.FromSlash(rel)))
		if rel == "." || seen[rel] {
			continue
		}
		seen[rel] = true
		src, err := runtimeWorkspaceSafeJoin(sourceDir, filepath.FromSlash(rel))
		if err != nil {
			return nil, err
		}
		info, err := os.Stat(src)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if !info.Mode().IsRegular() {
			continue
		}
		dst, err := runtimeWorkspaceSafeJoin(viewDir, filepath.FromSlash(rel))
		if err != nil {
			return nil, err
		}
		if err := os.MkdirAll(filepath.Dir(dst), 0o750); err != nil {
			return nil, err
		}
		if err := copyRuntimeFile(src, dst, 0o640); err != nil {
			return nil, err
		}
		normalized := filepath.ToSlash(rel)
		rendered = append(rendered, normalized)
		copied = append(copied, normalized)
		copiedSet[normalized] = true
	}
	if strings.TrimSpace(configRoot) != "" {
		referenceFiles, err := copyRuntimeMarkdownTree(
			filepath.Join(filepath.Clean(configRoot), "runtime-skills", "renshe_content_creation", "references"),
			viewDir,
			filepath.FromSlash("skills/renshe_content_creation/references"),
			nil,
		)
		if err != nil {
			return nil, err
		}
		for _, rel := range referenceFiles {
			rendered = append(rendered, rel)
			copied = append(copied, rel)
			copiedSet[rel] = true
		}

		agentFiles, err := copyRuntimeMarkdownTree(
			filepath.Join(filepath.Clean(configRoot), "runtime-agents", "renshe_neirong_agent"),
			viewDir,
			"",
			rensheRuntimeAgentMarkdownApproved,
		)
		if err != nil {
			return nil, err
		}
		for _, rel := range agentFiles {
			rendered = append(rendered, rel)
			copiedSet[rel] = true
		}
	}
	generated := map[string]string{
		"AGENTS.md": rensheRuntimeFallbackAgents,
		"SOUL.md":   rensheRuntimeFallbackSoul,
		"TOOLS.md":  rensheRuntimeFallbackTools,
	}
	if strings.TrimSpace(skillContent) != "" {
		generated["skills/renshe_content_creation/SKILL.md"] = skillContent
	}
	for rel, content := range generated {
		if copiedSet[rel] {
			continue
		}
		path, err := runtimeWorkspaceSafeJoin(viewDir, filepath.FromSlash(rel))
		if err != nil {
			return nil, err
		}
		if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
			return nil, err
		}
		if err := os.WriteFile(path, []byte(content), 0o640); err != nil {
			return nil, err
		}
		rendered = append(rendered, rel)
		copiedSet[rel] = true
	}
	routeRel := filepath.FromSlash("resources/renshe-evidence-route.md")
	routePath, err := runtimeWorkspaceSafeJoin(viewDir, routeRel)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(routePath), 0o750); err != nil {
		return nil, err
	}
	if err := os.WriteFile(routePath, []byte(rensheEvidenceRouteContent(copied)), 0o640); err != nil {
		return nil, err
	}
	rendered = append(rendered, filepath.ToSlash(routeRel))
	sort.Strings(rendered)
	return rendered, nil
}

func copyRuntimeMarkdownTree(sourceRoot, viewDir, destinationPrefix string, approved func(string) bool) ([]string, error) {
	info, err := os.Stat(sourceRoot)
	if os.IsNotExist(err) {
		return []string{}, nil
	}
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("RUNTIME_INPUT_INVALID")
	}

	rendered := []string{}
	err = filepath.WalkDir(sourceRoot, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == sourceRoot {
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			if entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if entry.IsDir() || !entry.Type().IsRegular() || !strings.EqualFold(filepath.Ext(entry.Name()), ".md") {
			return nil
		}
		relative, err := filepath.Rel(sourceRoot, path)
		if err != nil {
			return err
		}
		relative = filepath.Clean(relative)
		if relative == "." || filepath.IsAbs(relative) || runtimePathHasTraversal(filepath.ToSlash(relative)) {
			return fmt.Errorf("RUNTIME_INPUT_INVALID")
		}
		if approved != nil && !approved(filepath.ToSlash(relative)) {
			return nil
		}
		destinationRelative := filepath.Join(destinationPrefix, relative)
		destination, err := runtimeWorkspaceSafeJoin(viewDir, destinationRelative)
		if err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Dir(destination), 0o750); err != nil {
			return err
		}
		if err := copyRuntimeFileReplacing(path, destination, 0o640); err != nil {
			return err
		}
		rendered = append(rendered, filepath.ToSlash(destinationRelative))
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(rendered)
	return rendered, nil
}

func rensheRuntimeAgentMarkdownApproved(relative string) bool {
	relative = filepath.ToSlash(filepath.Clean(filepath.FromSlash(relative)))
	if strings.HasPrefix(relative, "knowledge/") || strings.HasPrefix(relative, "protocols/") {
		return true
	}
	switch relative {
	case "AGENTS.md", "SOUL.md", "TOOLS.md", "MEMORY.md", "USER.md", "IDENTITY.md", "HEARTBEAT.md":
		return true
	default:
		return false
	}
}

func copyRuntimeFileReplacing(src, dst string, mode os.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, mode)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(out, in)
	closeErr := out.Close()
	if copyErr != nil {
		return copyErr
	}
	return closeErr
}

func rensheMaterialCandidatesFromIndex(sourceDir, userRequest string) ([]string, error) {
	indexPath, err := runtimeWorkspaceSafeJoin(sourceDir, filepath.FromSlash("resources/materials.md"))
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(indexPath)
	if os.IsNotExist(err) {
		return []string{}, nil
	}
	if err != nil {
		return nil, err
	}
	return parseRensheMaterialIndexCandidates(string(data), userRequest), nil
}

func parseRensheMaterialIndexCandidates(markdown, userRequest string) []string {
	out := []string{}
	seen := map[string]bool{}
	for _, line := range strings.Split(markdown, "\n") {
		if !strings.Contains(line, "materials/") {
			continue
		}
		selected := append(backtickedPathsWithPrefix(line, "materials/processed/"), backtickedPathsWithPrefix(line, "materials/raw/")...)
		if !rensheMaterialIndexLineRequested(line, selected, userRequest) {
			continue
		}
		for _, rel := range selected {
			normalized, ok := normalizeRensheMaterialRel(rel)
			if !ok || seen[normalized] {
				continue
			}
			seen[normalized] = true
			out = append(out, normalized)
		}
	}
	return out
}

func rensheMaterialIndexLineRequested(line string, paths []string, userRequest string) bool {
	query := strings.ToLower(strings.TrimSpace(userRequest))
	if query == "" {
		return false
	}
	keys := []string{}
	columns := strings.Split(line, "|")
	if len(columns) > 2 {
		keys = append(keys, strings.Trim(strings.TrimSpace(columns[1]), "`"), strings.TrimSpace(columns[2]))
	}
	for _, path := range paths {
		name := filepath.Base(filepath.FromSlash(path))
		keys = append(keys, path, name, strings.TrimSuffix(name, filepath.Ext(name)))
	}
	for _, key := range keys {
		normalized := strings.ToLower(strings.Trim(strings.TrimSpace(key), "`"))
		if normalized == "" || normalized == "周会" || normalized == "会议" || normalized == "纪要" || len([]rune(normalized)) < 2 {
			continue
		}
		if strings.Contains(query, normalized) {
			return true
		}
	}
	return false
}

func backtickedPathsWithPrefix(text, prefix string) []string {
	out := []string{}
	remaining := text
	for {
		start := strings.Index(remaining, "`")
		if start < 0 {
			return out
		}
		remaining = remaining[start+1:]
		end := strings.Index(remaining, "`")
		if end < 0 {
			return out
		}
		value := strings.TrimSpace(remaining[:end])
		if strings.HasPrefix(value, prefix) {
			out = append(out, value)
		}
		remaining = remaining[end+1:]
	}
}

func normalizeRensheMaterialRel(rel string) (string, bool) {
	rel = strings.TrimSpace(rel)
	if rel == "" || filepath.IsAbs(rel) || strings.Contains(rel, "\x00") {
		return "", false
	}
	normalized := filepath.ToSlash(filepath.Clean(filepath.FromSlash(rel)))
	if normalized == "." || strings.HasPrefix(normalized, "../") || strings.Contains(normalized, "/../") {
		return "", false
	}
	if !strings.HasPrefix(normalized, "materials/processed/") && !strings.HasPrefix(normalized, "materials/raw/") {
		return "", false
	}
	return normalized, true
}

func rensheEvidenceRouteContent(files []string) string {
	lines := []string{
		"# Renshe Evidence Route",
		"",
		"This run view contains the active renshe Skill references plus bounded evidence copied from the user's formal workspace.",
		"Use Skill references as method only. Treat the current user material as the first evidence source and process it without an admission test.",
		"When the user explicitly asks to combine a named meeting material with the positioning profile, read both before answering. Let relevant industry, product, target-audience, and current-context evidence shape judgment early; Profile remains optional and an empty Profile never blocks the work.",
		"Analyze all four value routes before recommending one primary route and at most one supporting route. Continue after the user selects a candidate or explicitly delegates the choice.",
		"Use Profile, story, viewpoint, daily, and content files only when the current request or creative route needs them. Persona-six-method candidate design reads real evidence at the topic stage; ordinary content considers zero to three natural personal-fusion positions only after the outline.",
		"When prior workspace evidence is needed, start with `resources/overview.md`, then use `workspace_search` or the Available files list below to locate a narrow material path.",
		"Use an index summary as evidence only when it names a real story, service moment, viewpoint, mistake, choice, observed pain, or concrete creator proof.",
		"When an index or search result points to meeting files, open only material paths that are also listed below under Available files.",
		"Use `resources/profile.md` and the listed canonical Profile projections only when the answer needs verified personal evidence. Missing personal evidence never blocks content work, and optional fusion must not overpower the material.",
		"If a narrow read does not reveal relevant evidence, stop reading and continue from the available material. Ask one precise question only when the missing fact would change the requested result.",
		"",
		"Available files:",
	}
	if len(files) == 0 {
		lines = append(lines, "- none")
	} else {
		for _, file := range files {
			lines = append(lines, "- `"+file+"`")
		}
	}
	return strings.Join(lines, "\n") + "\n"
}

type formalWorkspaceRuntimeArtifact struct {
	relativePath string
	directory    bool
}

var formalWorkspaceRuntimeArtifacts = []formalWorkspaceRuntimeArtifact{
	{relativePath: "input", directory: true},
	{relativePath: "output", directory: true},
	{relativePath: "result.json"},
	{relativePath: filepath.Join(".openclaw", "tmp"), directory: true},
}

type formalWorkspaceArtifactMove struct {
	relativePath string
	source       string
	destination  string
}

func (r RuntimeWorkspaceRenderer) validateFormalWorkspaceSelectedSkill(command RuntimeWorkspaceRenderCommand) (string, error) {
	if err := validateFormalWorkspaceRoot(command.WorkspaceDir); err != nil || strings.TrimSpace(command.Plan.SkillProfile) == "" || !safeRuntimeSegment(command.Plan.SkillProfile) {
		return "", fmt.Errorf("RUNTIME_INPUT_INVALID")
	}
	skillPath, err := runtimeWorkspaceSafeJoin(command.WorkspaceDir, filepath.Join("skills", command.Plan.SkillProfile, "SKILL.md"))
	if err != nil {
		return "", fmt.Errorf("RUNTIME_INPUT_INVALID")
	}
	info, err := os.Lstat(skillPath)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return "", fmt.Errorf("RUNTIME_INPUT_INVALID")
	}
	data, err := os.ReadFile(skillPath)
	if err != nil || strings.TrimSpace(string(data)) == "" {
		return "", fmt.Errorf("RUNTIME_INPUT_INVALID")
	}
	return skillContentHash(string(data)), nil
}

func (r RuntimeWorkspaceRenderer) quarantineFormalWorkspaceRuntimeArtifacts(command RuntimeWorkspaceRenderCommand) ([]string, error) {
	if err := validateFormalWorkspaceRoot(command.WorkspaceDir); err != nil {
		return nil, fmt.Errorf("RUNTIME_INPUT_INVALID")
	}
	moves := make([]formalWorkspaceArtifactMove, 0, len(formalWorkspaceRuntimeArtifacts))
	for _, artifact := range formalWorkspaceRuntimeArtifacts {
		if artifact.relativePath == filepath.Join(".openclaw", "tmp") {
			parent, err := runtimeWorkspaceSafeJoin(command.WorkspaceDir, ".openclaw")
			if err != nil {
				return nil, fmt.Errorf("RUNTIME_INPUT_INVALID")
			}
			if info, err := os.Lstat(parent); err == nil && (info.Mode()&os.ModeSymlink != 0 || !info.IsDir()) {
				return nil, fmt.Errorf("RUNTIME_INPUT_INVALID")
			} else if err != nil && !os.IsNotExist(err) {
				return nil, fmt.Errorf("RUNTIME_INPUT_INVALID")
			}
		}
		source, err := runtimeWorkspaceSafeJoin(command.WorkspaceDir, artifact.relativePath)
		if err != nil {
			return nil, fmt.Errorf("RUNTIME_INPUT_INVALID")
		}
		info, err := os.Lstat(source)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil || info.Mode()&os.ModeSymlink != 0 || info.IsDir() != artifact.directory || (!artifact.directory && !info.Mode().IsRegular()) {
			return nil, fmt.Errorf("RUNTIME_INPUT_INVALID")
		}
		moves = append(moves, formalWorkspaceArtifactMove{relativePath: filepath.ToSlash(artifact.relativePath), source: source})
	}
	if len(moves) == 0 {
		return []string{}, nil
	}
	quarantineRunRoot, err := formalWorkspaceQuarantineRunRoot(command.WorkspaceDir, command.RunID)
	if err != nil {
		return nil, fmt.Errorf("RUNTIME_INPUT_INVALID")
	}
	for index := range moves {
		destination, err := runtimeWorkspaceSafeJoin(quarantineRunRoot, filepath.FromSlash(moves[index].relativePath))
		if err != nil {
			return nil, fmt.Errorf("RUNTIME_INPUT_INVALID")
		}
		if err := ensureFormalWorkspaceQuarantineDirectory(filepath.Dir(destination)); err != nil {
			return nil, fmt.Errorf("RUNTIME_INPUT_INVALID")
		}
		if _, err := os.Lstat(destination); !os.IsNotExist(err) {
			return nil, fmt.Errorf("RUNTIME_INPUT_INVALID")
		}
		moves[index].destination = destination
	}
	moved := make([]formalWorkspaceArtifactMove, 0, len(moves))
	for _, move := range moves {
		if err := moveFormalWorkspaceArtifact(move.source, move.destination); err != nil {
			rollbackFormalWorkspaceArtifactMoves(moved)
			return nil, fmt.Errorf("RUNTIME_INPUT_INVALID")
		}
		moved = append(moved, move)
	}
	artifacts := make([]string, 0, len(moved))
	for _, move := range moved {
		artifacts = append(artifacts, move.relativePath)
	}
	return artifacts, nil
}

func validateFormalWorkspaceRoot(workspaceDir string) error {
	if strings.TrimSpace(workspaceDir) == "" {
		return fmt.Errorf("RUNTIME_INPUT_INVALID")
	}
	info, err := os.Lstat(workspaceDir)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("RUNTIME_INPUT_INVALID")
	}
	return nil
}

func formalWorkspaceQuarantineRunRoot(workspaceDir, runID string) (string, error) {
	runtimeRoot := strings.TrimSpace(os.Getenv("HUAHUO_RUNTIME_ROOT"))
	if runtimeRoot == "" {
		runtimeRoot = strings.TrimSpace(os.Getenv("OPENCLAW_RUNTIME_ROOT"))
	}
	if runtimeRoot == "" || !filepath.IsAbs(runtimeRoot) {
		return "", fmt.Errorf("RUNTIME_INPUT_INVALID")
	}
	absRuntimeRoot, err := filepath.Abs(runtimeRoot)
	if err != nil || AssertRuntimeRoot(absRuntimeRoot) != nil {
		return "", fmt.Errorf("RUNTIME_INPUT_INVALID")
	}
	absWorkspaceRoot, err := filepath.Abs(workspaceDir)
	if err != nil || runtimePathContains(absRuntimeRoot, absWorkspaceRoot) || runtimePathContains(absWorkspaceRoot, absRuntimeRoot) {
		return "", fmt.Errorf("RUNTIME_INPUT_INVALID")
	}
	if err := ensureFormalWorkspaceQuarantineDirectory(absRuntimeRoot); err != nil {
		return "", fmt.Errorf("RUNTIME_INPUT_INVALID")
	}
	quarantineRoot, err := runtimeWorkspaceSafeJoin(absRuntimeRoot, "formal-workspace-quarantine")
	if err != nil {
		return "", fmt.Errorf("RUNTIME_INPUT_INVALID")
	}
	if err := ensureFormalWorkspaceQuarantineDirectory(quarantineRoot); err != nil {
		return "", fmt.Errorf("RUNTIME_INPUT_INVALID")
	}
	if !safeRuntimeSegment(runID) {
		return "", fmt.Errorf("RUNTIME_INPUT_INVALID")
	}
	runRoot, err := runtimeWorkspaceSafeJoin(quarantineRoot, runID)
	if err != nil {
		return "", fmt.Errorf("RUNTIME_INPUT_INVALID")
	}
	if err := ensureFormalWorkspaceQuarantineDirectory(runRoot); err != nil {
		return "", fmt.Errorf("RUNTIME_INPUT_INVALID")
	}
	return runRoot, nil
}

func ensureFormalWorkspaceQuarantineDirectory(path string) error {
	if err := os.MkdirAll(path, 0o700); err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("RUNTIME_INPUT_INVALID")
	}
	return nil
}

func runtimePathContains(root, path string) bool {
	relative, err := filepath.Rel(filepath.Clean(root), filepath.Clean(path))
	return err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

func moveFormalWorkspaceArtifact(source, destination string) error {
	if err := os.Rename(source, destination); err == nil {
		return nil
	}
	if err := copyFormalWorkspaceArtifact(source, destination); err != nil {
		return err
	}
	if err := os.RemoveAll(source); err != nil {
		_ = os.RemoveAll(destination)
		return err
	}
	return nil
}

func copyFormalWorkspaceArtifact(source, destination string) error {
	info, err := os.Lstat(source)
	if err != nil || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("RUNTIME_INPUT_INVALID")
	}
	if !info.IsDir() {
		if !info.Mode().IsRegular() {
			return fmt.Errorf("RUNTIME_INPUT_INVALID")
		}
		return copyRuntimeFile(source, destination, info.Mode().Perm())
	}
	if err := os.Mkdir(destination, info.Mode().Perm()); err != nil {
		return err
	}
	copyErr := filepath.WalkDir(source, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == source {
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("RUNTIME_INPUT_INVALID")
		}
		relative, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		target, err := runtimeWorkspaceSafeJoin(destination, relative)
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return os.Mkdir(target, 0o700)
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("RUNTIME_INPUT_INVALID")
		}
		fileInfo, err := entry.Info()
		if err != nil {
			return err
		}
		return copyRuntimeFile(path, target, fileInfo.Mode().Perm())
	})
	if copyErr != nil {
		_ = os.RemoveAll(destination)
	}
	return copyErr
}

func rollbackFormalWorkspaceArtifactMoves(moves []formalWorkspaceArtifactMove) {
	for index := len(moves) - 1; index >= 0; index-- {
		_ = moveFormalWorkspaceArtifact(moves[index].destination, moves[index].source)
	}
}

func (r RuntimeWorkspaceRenderer) renderDetachedWorkspaceTemplates(command RuntimeWorkspaceRenderCommand) ([]string, error) {
	renderedFiles := []string{}
	for _, rel := range []string{
		"AGENTS.md",
		"SOUL.md",
		"USER.md",
		"TOOLS.md",
		"MEMORY.md",
		filepath.Join("input", "README.md"),
		filepath.Join("output", "README.md"),
		filepath.Join("skills", "README.md"),
	} {
		content, err := r.readDetachedTemplate(rel)
		if err != nil {
			return nil, fmt.Errorf("RUNTIME_INPUT_INVALID")
		}
		path, err := runtimeWorkspaceSafeJoin(command.WorkspaceDir, rel)
		if err != nil {
			return nil, err
		}
		if err := os.WriteFile(path, []byte(renderDetachedTemplate(content, command)), 0o640); err != nil {
			return nil, err
		}
		renderedFiles = append(renderedFiles, filepath.ToSlash(rel))
	}
	return renderedFiles, nil
}

func (r RuntimeWorkspaceRenderer) readDetachedTemplate(relative string) (string, error) {
	if strings.TrimSpace(relative) == "" || filepath.IsAbs(relative) || runtimePathHasTraversal(strings.ReplaceAll(relative, "\\", "/")) {
		return "", fmt.Errorf("RUNTIME_INPUT_INVALID")
	}
	data, err := os.ReadFile(r.Mirror.WorkspaceTemplatePath("detached-runtime-workspace", relative))
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func renderDetachedTemplate(content string, command RuntimeWorkspaceRenderCommand) string {
	values := map[string]string{
		"taskId":       command.Task.TaskID,
		"runId":        command.RunID,
		"taskType":     command.Plan.TaskType,
		"userId":       command.Task.UserID,
		"workspaceId":  command.Task.WorkspaceID,
		"recordingId":  detachedTemplateRecordingID(command),
		"skillProfile": command.Plan.SkillProfile,
	}
	rendered := content
	for key, value := range values {
		rendered = strings.ReplaceAll(rendered, "{{"+key+"}}", value)
	}
	return rendered
}

func detachedTemplateRecordingID(command RuntimeWorkspaceRenderCommand) string {
	inputs := command.Task.Context.Inputs
	snapshot := runtimeMapValue(inputs["inputSnapshot"])
	return firstRuntimeNonEmpty(
		runtimeStringValue(inputs["recordingId"]),
		runtimeStringValue(snapshot["recordingId"]),
		runtimeStringValue(snapshot["recording_id"]),
	)
}

func validateRuntimeRenderCommand(command RuntimeWorkspaceRenderCommand) error {
	if strings.TrimSpace(command.RunID) == "" || strings.TrimSpace(command.WorkspaceDir) == "" || strings.TrimSpace(command.Plan.SkillProfile) == "" {
		return fmt.Errorf("RUNTIME_INPUT_INVALID")
	}
	if !safeRuntimeSegment(command.RunID) || !safeRuntimeSegment(command.Plan.SkillProfile) {
		return fmt.Errorf("RUNTIME_INPUT_INVALID")
	}
	return nil
}

func AssertRuntimeRoot(path string) error {
	raw := strings.TrimSpace(path)
	if runtimePathHasTraversal(raw) {
		return fmt.Errorf("RUNTIME_INPUT_INVALID")
	}
	clean := filepath.Clean(raw)
	if clean == "" || clean == "." {
		return fmt.Errorf("RUNTIME_INPUT_INVALID")
	}
	volume := filepath.VolumeName(clean)
	withoutVolume := strings.TrimPrefix(clean, volume)
	if withoutVolume == string(filepath.Separator) || withoutVolume == "" {
		return fmt.Errorf("RUNTIME_INPUT_INVALID")
	}
	return nil
}

func (r RuntimeWorkspaceRenderer) AssertRuntimeRoot(path string) error {
	return AssertRuntimeRoot(path)
}

func runtimeWorkspaceSafeJoin(root, relative string) (string, error) {
	if strings.TrimSpace(root) == "" || strings.TrimSpace(relative) == "" || filepath.IsAbs(relative) {
		return "", fmt.Errorf("RUNTIME_INPUT_INVALID")
	}
	cleanRoot := filepath.Clean(root)
	cleanPath := filepath.Join(cleanRoot, relative)
	rel, err := filepath.Rel(cleanRoot, cleanPath)
	if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("RUNTIME_INPUT_INVALID")
	}
	return cleanPath, nil
}

func runtimeInputRelativePath(name string) (string, error) {
	raw := strings.TrimSpace(name)
	normalizedRaw := strings.ReplaceAll(raw, "\\", "/")
	clean := filepath.Clean(raw)
	if clean == "." || clean == "" || filepath.IsAbs(clean) || strings.Contains(raw, ":") || runtimePathHasTraversal(normalizedRaw) || strings.HasPrefix(clean, "..") || strings.Contains(normalizedRaw, "../") || strings.Contains(normalizedRaw, "/..") {
		return "", fmt.Errorf("RUNTIME_INPUT_INVALID")
	}
	clean = strings.TrimPrefix(clean, "input"+string(filepath.Separator))
	return filepath.Join("input", clean), nil
}

func isFormalWorkspacePath(path string) bool {
	normalized := filepath.ToSlash(filepath.Clean(path))
	return strings.Contains(normalized, "/workspaces/tenants/") &&
		strings.Contains(normalized, "/users/") &&
		strings.Contains(normalized, "/workspaces/")
}

func copyRuntimeFile(src, dst string, mode os.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_WRONLY, mode)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(out, in)
	closeErr := out.Close()
	if copyErr != nil {
		return copyErr
	}
	return closeErr
}

func isReservedRuntimeInputPath(path string) bool {
	switch filepath.ToSlash(path) {
	case "input/task.json", "input/context.md", "input/user_request.md", "input/assets.json":
		return true
	default:
		return false
	}
}

func runtimeToolsBoundary(plan ProfilePlan) string {
	if plan.TaskType == "work_ai_renshe_content" {
		return rensheRuntimeFallbackTools
	}
	return "Read input files. Use output files only when the task contract explicitly requires an artifact.\n"
}

func runtimeAgentsBoundary(plan ProfilePlan) string {
	if plan.ExecutionScope == ScopeDetachedTask {
		lines := []string{
			"Task agent boundary for " + plan.TaskType + ".",
			"Use only the selected workspace skill at skills/" + plan.SkillProfile + "/SKILL.md when a skill is needed.",
			"Do not invent absolute skill paths or browse server/runtime directories.",
		}
		return strings.Join(lines, "\n") + "\n"
	}
	if plan.TaskType == "work_ai_renshe_content" {
		return rensheRuntimeFallbackAgents
	}
	return "Task agent boundary for " + plan.TaskType + "\n"
}

func safeRuntimeSegment(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" || value == "." || strings.Trim(value, ".") == "" || strings.Contains(value, "/") || strings.Contains(value, "\\") || strings.Contains(value, "..") {
		return false
	}
	for _, r := range value {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '-' || r == '.' {
			continue
		}
		return false
	}
	return true
}

func redactRuntimeTaskParameters(input map[string]any) map[string]any {
	if input == nil {
		return map[string]any{}
	}
	out := map[string]any{}
	for key, value := range input {
		out[key] = redactRuntimeTaskValue(key, value)
	}
	return out
}

func redactRuntimeTaskValue(key string, value any) any {
	if shouldRedactRuntimeTaskKey(key) {
		return "[redacted]"
	}
	switch typed := value.(type) {
	case string:
		if runtimeTaskStringLooksSensitive(typed) {
			return "[redacted]"
		}
		return typed
	case map[string]any:
		return redactRuntimeTaskParameters(typed)
	case map[string]string:
		out := map[string]any{}
		for itemKey, itemValue := range typed {
			out[itemKey] = redactRuntimeTaskValue(itemKey, itemValue)
		}
		return out
	case []any:
		out := make([]any, 0, len(typed))
		for _, item := range typed {
			out = append(out, redactRuntimeTaskValue(key, item))
		}
		return out
	case []string:
		out := make([]any, 0, len(typed))
		for _, item := range typed {
			out = append(out, redactRuntimeTaskValue(key, item))
		}
		return out
	case []map[string]any:
		out := make([]map[string]any, 0, len(typed))
		for _, item := range typed {
			out = append(out, redactRuntimeTaskParameters(item))
		}
		return out
	default:
		return value
	}
}

func redactRuntimeTaskText(value string) string {
	if strings.TrimSpace(value) == "" {
		return ""
	}
	return "[redacted]"
}

func shouldRedactRuntimeTaskKey(key string) bool {
	normalized := strings.ToLower(strings.ReplaceAll(strings.ReplaceAll(strings.TrimSpace(key), "_", ""), "-", ""))
	for _, marker := range []string{
		"transcript",
		"message",
		"prompt",
		"content",
		"signedurl",
		"token",
		"secret",
		"raw",
		"providerraw",
		"rawrequest",
		"rawresponse",
		"url",
		"path",
		"realpath",
		"absolutepath",
		"workspacedir",
		"original",
		"fulltext",
		"body",
		"document",
	} {
		if strings.Contains(normalized, marker) {
			return true
		}
	}
	return false
}

func runtimeTaskStringLooksSensitive(value string) bool {
	normalized := strings.ToLower(strings.ReplaceAll(value, "\\", "/"))
	if len([]rune(value)) > 512 {
		return true
	}
	for _, marker := range []string{
		"/home/data/",
		"/home/huahuo-runtime/",
		"/tmp/runtime-workspaces/",
		"authorization:",
		"bearer ",
		"x-signature=",
		"signature=",
		"token=",
		"secret=",
	} {
		if strings.Contains(normalized, marker) {
			return true
		}
	}
	return false
}
