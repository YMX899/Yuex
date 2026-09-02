package runtime

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type MetaManifest struct {
	Version string                      `json:"version"`
	Status  string                      `json:"status"`
	Files   []MetaManifestFile          `json:"files"`
	ByPath  map[string]MetaManifestFile `json:"-"`
	ByID    map[string]MetaManifestFile `json:"-"`
}

type MetaManifestFile struct {
	MetaFileID             string   `json:"metaFileId"`
	RelativePath           string   `json:"relativePath"`
	Kind                   string   `json:"kind"`
	Version                string   `json:"version"`
	MergePolicy            string   `json:"mergePolicy"`
	ProtectedSections      []string `json:"protectedSections"`
	UserAppendableSections []string `json:"userAppendableSections"`
	Status                 string   `json:"status,omitempty"`
	Hash                   string   `json:"hash,omitempty"`
}

type RuntimeSkillEntry struct {
	SkillProfile    string `json:"skillProfile"`
	MetaFileID      string `json:"metaFileId"`
	RelativePath    string `json:"relativePath"`
	Version         string `json:"version"`
	Status          string `json:"status"`
	Hash            string `json:"hash,omitempty"`
	ManifestVersion string `json:"manifestVersion"`
	Path            string `json:"-"`
}

type MetaMirror struct {
	ConfigRoot string
}

func NewMetaMirror(configRoot string) MetaMirror {
	if configRoot == "" {
		configRoot = "/home/huahuo-runtime/config"
	}
	return MetaMirror{ConfigRoot: filepath.Clean(configRoot)}
}

func (m MetaMirror) LoadManifest() (MetaManifest, error) {
	path := filepath.Join(m.ConfigRoot, "templates", "meta-manifest.json")
	data, err := os.ReadFile(path)
	if err != nil {
		return MetaManifest{}, err
	}
	var manifest MetaManifest
	if err := json.Unmarshal(trimUTF8BOM(data), &manifest); err != nil {
		return MetaManifest{}, err
	}
	manifest.ByPath = map[string]MetaManifestFile{}
	manifest.ByID = map[string]MetaManifestFile{}
	for _, file := range manifest.Files {
		relative := normalizeManifestRelativePath(file.RelativePath)
		file.RelativePath = relative
		manifest.ByPath[relative] = file
		if file.MetaFileID != "" {
			manifest.ByID[file.MetaFileID] = file
		}
	}
	return manifest, nil
}

func trimUTF8BOM(content []byte) []byte {
	return bytes.TrimPrefix(content, []byte{0xEF, 0xBB, 0xBF})
}

func (m MetaMirror) Validate() error {
	manifest, err := m.LoadManifest()
	if err != nil {
		return err
	}
	if strings.TrimSpace(manifest.Version) == "" || len(manifest.Files) == 0 {
		return fmt.Errorf("meta manifest invalid")
	}
	if status := strings.TrimSpace(manifest.Status); status != "" && status != "active" {
		return fmt.Errorf("meta manifest invalid")
	}
	seenIDs := map[string]bool{}
	seenPaths := map[string]bool{}
	for _, file := range manifest.Files {
		if strings.TrimSpace(file.RelativePath) == "" || strings.TrimSpace(file.MetaFileID) == "" {
			return fmt.Errorf("meta manifest file invalid")
		}
		if seenIDs[file.MetaFileID] || seenPaths[file.RelativePath] {
			return fmt.Errorf("meta manifest file invalid")
		}
		seenIDs[file.MetaFileID] = true
		seenPaths[file.RelativePath] = true
		if !manifestRelativePathAllowed(file.RelativePath) {
			return fmt.Errorf("meta manifest file invalid")
		}
		switch file.Kind {
		case "runtime_skill":
			if _, err := m.ResolveRuntimeSkillFile(runtimeSkillProfileFromRelative(file.RelativePath)); err != nil {
				return err
			}
		case "workspace_standard_file", "workspace_knowledge_file":
			if _, err := os.Stat(m.resolveWorkspaceManifestPath(file.RelativePath)); err != nil {
				return err
			}
		}
	}
	return nil
}

func (m MetaMirror) RuntimeSkillEntries() []RuntimeSkillEntry {
	manifest, err := m.LoadManifest()
	if err != nil {
		return nil
	}
	entries := []RuntimeSkillEntry{}
	for _, file := range manifest.Files {
		if file.Kind != "runtime_skill" {
			continue
		}
		skillProfile := runtimeSkillProfileFromRelative(file.RelativePath)
		if skillProfile == "" {
			continue
		}
		entries = append(entries, RuntimeSkillEntry{
			SkillProfile:    skillProfile,
			MetaFileID:      file.MetaFileID,
			RelativePath:    file.RelativePath,
			Version:         file.Version,
			Status:          statusOrActive(file.Status),
			Hash:            strings.TrimSpace(file.Hash),
			ManifestVersion: manifest.Version,
			Path:            filepath.Join(m.ConfigRoot, filepath.FromSlash(file.RelativePath)),
		})
	}
	return entries
}

func (m MetaMirror) ResolveRuntimeSkillFile(skillProfile string) (RuntimeSkillEntry, error) {
	if !safeRuntimeSkillProfile(skillProfile) {
		return RuntimeSkillEntry{}, fmt.Errorf("SKILL_UNAVAILABLE")
	}
	if _, err := m.LoadManifest(); err != nil {
		return RuntimeSkillEntry{}, fmt.Errorf("SKILL_UNAVAILABLE")
	}
	for _, entry := range m.RuntimeSkillEntries() {
		if entry.SkillProfile == skillProfile {
			if _, statErr := os.Stat(entry.Path); statErr != nil {
				return RuntimeSkillEntry{}, fmt.Errorf("SKILL_UNAVAILABLE")
			}
			return entry, nil
		}
	}
	return RuntimeSkillEntry{}, fmt.Errorf("SKILL_UNAVAILABLE")
}

func (m MetaMirror) SkillPath(skillProfile string) string {
	if entry, err := m.ResolveRuntimeSkillFile(skillProfile); err == nil && entry.Path != "" {
		return entry.Path
	}
	return ""
}

func (m MetaMirror) WorkspaceTemplatePath(template, name string) string {
	return filepath.Join(m.ConfigRoot, "templates", "workspace", template, name)
}

func (m MetaMirror) resolveWorkspaceManifestPath(relative string) string {
	relative = normalizeManifestRelativePath(relative)
	if strings.HasPrefix(relative, "workspace/") {
		return filepath.Join(m.ConfigRoot, "templates", filepath.FromSlash(relative))
	}
	if strings.HasPrefix(relative, "knowledge/") {
		candidate := filepath.Join(m.ConfigRoot, "templates", filepath.FromSlash(relative))
		if _, err := os.Stat(candidate); err == nil {
			return candidate
		}
	}
	return filepath.Join(m.ConfigRoot, filepath.FromSlash(relative))
}

func normalizeManifestRelativePath(relative string) string {
	return strings.Trim(strings.ReplaceAll(strings.TrimSpace(relative), "\\", "/"), "/")
}

func manifestRelativePathAllowed(relative string) bool {
	relative = normalizeManifestRelativePath(relative)
	if relative == "" || strings.HasPrefix(relative, "../") || strings.Contains(relative, "/../") || strings.HasSuffix(relative, "/..") || filepath.IsAbs(relative) {
		return false
	}
	return strings.HasPrefix(relative, "runtime-skills/") ||
		strings.HasPrefix(relative, "workspace/") ||
		strings.HasPrefix(relative, "knowledge/")
}

func runtimeSkillProfileFromRelative(relative string) string {
	relative = normalizeManifestRelativePath(relative)
	const prefix = "runtime-skills/"
	const suffix = "/SKILL.md"
	if !strings.HasPrefix(relative, prefix) || !strings.HasSuffix(relative, suffix) {
		return ""
	}
	skillProfile := strings.TrimSuffix(strings.TrimPrefix(relative, prefix), suffix)
	if !safeRuntimeSkillProfile(skillProfile) {
		return ""
	}
	return skillProfile
}

func safeRuntimeSkillProfile(skillProfile string) bool {
	if skillProfile == "" || strings.Contains(skillProfile, "/") || strings.Contains(skillProfile, "\\") || strings.Contains(skillProfile, "..") {
		return false
	}
	for _, ch := range skillProfile {
		if ch >= 'a' && ch <= 'z' || ch >= '0' && ch <= '9' || ch == '_' {
			continue
		}
		return false
	}
	return true
}

func statusOrActive(status string) string {
	status = strings.TrimSpace(status)
	if status == "" {
		return "active"
	}
	return status
}
