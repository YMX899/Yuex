package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCandidateValidationCopyRejectsProtectedAdapterPaths(t *testing.T) {
	root := t.TempDir()
	protectedRoot := filepath.Join(root, "runtime-root")
	fixtureRoot := filepath.Join(root, "fixture")
	active := filepath.Join(protectedRoot, "bin", "openclaw-runtime-adapter")
	writeCandidateValidationExecutable(t, active, "active")
	if err := os.MkdirAll(fixtureRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	policy := candidateValidationArtifactPolicy{ForbiddenRoots: []string{protectedRoot}, KnownActiveFiles: []string{active}}

	for _, supplied := range []string{
		active,
		filepath.Join(protectedRoot, "bin", "nested", "candidate"),
		protectedRoot + string(os.PathSeparator) + "bin" + string(os.PathSeparator) + ".." + string(os.PathSeparator) + "bin" + string(os.PathSeparator) + "openclaw-runtime-adapter",
	} {
		if err := candidateValidationCopySuppliedCandidate(fixtureRoot, filepath.Join(fixtureRoot, "candidate-"+strings.ReplaceAll(filepath.Base(supplied), ".", "_")), supplied, policy); err == nil {
			t.Fatalf("protected supplied path %q was accepted", supplied)
		}
	}
}

func TestCandidateValidationCopyRejectsSymbolicLinkAliases(t *testing.T) {
	root := t.TempDir()
	fixtureRoot := filepath.Join(root, "fixture")
	regular := filepath.Join(root, "candidate")
	writeCandidateValidationExecutable(t, regular, "candidate")
	if err := os.MkdirAll(fixtureRoot, 0o700); err != nil {
		t.Fatal(err)
	}

	leafAlias := filepath.Join(root, "candidate-leaf-alias")
	createCandidateValidationSymlinkOrSkip(t, regular, leafAlias)
	if err := candidateValidationCopySuppliedCandidate(fixtureRoot, filepath.Join(fixtureRoot, "candidate-leaf"), leafAlias, candidateValidationArtifactPolicy{}); err == nil {
		t.Fatal("leaf symbolic-link candidate alias was accepted")
	}

	protectedRoot := filepath.Join(root, "protected")
	active := filepath.Join(protectedRoot, "openclaw-runtime-adapter")
	writeCandidateValidationExecutable(t, active, "active")
	parentAlias := filepath.Join(root, "protected-alias")
	createCandidateValidationSymlinkOrSkip(t, protectedRoot, parentAlias)
	policy := candidateValidationArtifactPolicy{ForbiddenRoots: []string{protectedRoot}, KnownActiveFiles: []string{active}}
	if err := candidateValidationCopySuppliedCandidate(fixtureRoot, filepath.Join(fixtureRoot, "candidate-parent"), filepath.Join(parentAlias, "openclaw-runtime-adapter"), policy); err == nil {
		t.Fatal("parent symbolic-link active Adapter alias was accepted")
	}
}

func TestCandidateValidationCopyRejectsResolvedProtectedRootAlias(t *testing.T) {
	root := t.TempDir()
	fixtureRoot := filepath.Join(root, "fixture")
	realProtectedRoot := filepath.Join(root, "real-protected")
	protectedRootAlias := filepath.Join(root, "protected-root-alias")
	active := filepath.Join(realProtectedRoot, "openclaw-runtime-adapter")
	writeCandidateValidationExecutable(t, active, "active")
	if err := os.MkdirAll(fixtureRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	createCandidateValidationSymlinkOrSkip(t, realProtectedRoot, protectedRootAlias)
	policy := candidateValidationArtifactPolicy{ForbiddenRoots: []string{protectedRootAlias}}
	if err := candidateValidationCopySuppliedCandidate(fixtureRoot, filepath.Join(fixtureRoot, "candidate"), active, policy); err == nil {
		t.Fatal("candidate beneath a resolved protected-root alias was accepted")
	}
}

func TestCandidateValidationCopyRejectsSameFileIdentity(t *testing.T) {
	root := t.TempDir()
	fixtureRoot := filepath.Join(root, "fixture")
	active := filepath.Join(root, "active", "openclaw-runtime-adapter")
	alias := filepath.Join(root, "candidate-hardlink")
	writeCandidateValidationExecutable(t, active, "active")
	if err := os.Link(active, alias); err != nil {
		t.Skipf("hard links are unavailable in this test filesystem: %v", err)
	}
	if err := os.MkdirAll(fixtureRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	policy := candidateValidationArtifactPolicy{KnownActiveFiles: []string{active}}
	if err := candidateValidationCopySuppliedCandidate(fixtureRoot, filepath.Join(fixtureRoot, "candidate"), alias, policy); err == nil {
		t.Fatal("hard-link alias of active Adapter artifact was accepted")
	}
}

func TestCandidateValidationCopyAllowsIndependentFixtureCandidate(t *testing.T) {
	root := t.TempDir()
	fixtureRoot := filepath.Join(root, "fixture")
	protectedRoot := filepath.Join(root, "runtime")
	supplied := filepath.Join(root, "candidate")
	active := filepath.Join(protectedRoot, "openclaw-runtime-adapter")
	writeCandidateValidationExecutable(t, supplied, "candidate")
	writeCandidateValidationExecutable(t, active, "active")
	if err := os.MkdirAll(fixtureRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	policy := candidateValidationArtifactPolicy{ForbiddenRoots: []string{protectedRoot}, KnownActiveFiles: []string{active}}
	destination := filepath.Join(fixtureRoot, "candidate")
	if err := candidateValidationCopySuppliedCandidate(fixtureRoot, destination, supplied, policy); err != nil {
		t.Fatalf("independent temporary candidate was rejected: %v", err)
	}
	content, err := os.ReadFile(destination)
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "candidate" {
		t.Fatalf("copied candidate=%q", content)
	}
}

func TestCandidateValidationPathWithinUsesComponentBoundaries(t *testing.T) {
	root := t.TempDir()
	protectedRoot := filepath.Join(root, "huahuo-runtime")
	sibling := filepath.Join(root, "huahuo-runtime-shadow", "candidate")
	if candidateValidationPathWithin(sibling, protectedRoot) {
		t.Fatalf("sibling path %q was treated as child of %q", sibling, protectedRoot)
	}
}

func TestCandidateValidationTemporaryBaseRejectsProtectedRoot(t *testing.T) {
	root := t.TempDir()
	protectedRoot := filepath.Join(root, "runtime-root")
	base := filepath.Join(protectedRoot, "tmp")
	if err := os.MkdirAll(base, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := candidateValidationTemporaryBase(base, candidateValidationArtifactPolicy{ForbiddenRoots: []string{protectedRoot}}); err == nil {
		t.Fatal("temporary base under a protected Adapter root was accepted")
	}
	sibling := filepath.Join(root, "runtime-root-shadow")
	if err := os.MkdirAll(sibling, 0o700); err != nil {
		t.Fatal(err)
	}
	if resolved, err := candidateValidationTemporaryBase(sibling, candidateValidationArtifactPolicy{ForbiddenRoots: []string{protectedRoot}}); err != nil || resolved != sibling {
		t.Fatalf("sibling temporary base was rejected: resolved=%q err=%v", resolved, err)
	}
}

func TestCandidateValidationBuildLayoutIsFixtureBoundAndOffline(t *testing.T) {
	root := t.TempDir()
	seed := filepath.Join(root, "seed")
	if err := os.MkdirAll(filepath.Join(seed, "cache", "download"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(seed, "cache", "download", "seed.txt"), []byte("seed"), 0o600); err != nil {
		t.Fatal(err)
	}
	buildRoot := filepath.Join(root, "build")
	layout, err := candidateValidationPrepareBuildLayout(buildRoot, seed)
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{layout.Home, layout.Temp, layout.GoTemp, layout.GoCache, layout.GoPath, layout.GoModCache} {
		if !candidateValidationPathWithin(path, buildRoot) {
			t.Fatalf("build path outside fixture root: %q", path)
		}
	}
	if copied, err := os.ReadFile(filepath.Join(layout.GoModCache, "cache", "download", "seed.txt")); err != nil || string(copied) != "seed" {
		t.Fatalf("offline module-cache seed was not copied: value=%q err=%v", copied, err)
	}
	environment := candidateValidationEnvironmentMap(candidateValidationBuildEnvironment(layout))
	for key, want := range map[string]string{
		"GOPROXY":     "off",
		"GOSUMDB":     "off",
		"GOTOOLCHAIN": "local",
		"GOWORK":      "off",
		"GOENV":       "off",
		"GOVCS":       "*:off",
		"GOMODCACHE":  layout.GoModCache,
		"GOCACHE":     layout.GoCache,
		"GOTMPDIR":    layout.GoTemp,
		"TMPDIR":      layout.Temp,
	} {
		if environment[key] != want {
			t.Fatalf("build environment %s=%q want %q", key, environment[key], want)
		}
	}
	if _, err := candidateValidationPrepareBuildLayout(filepath.Join(root, "missing-cache"), ""); err == nil {
		t.Fatal("missing offline module-cache seed was accepted")
	}
}

func TestCandidateValidationTemporaryRootRemovalIsAsserted(t *testing.T) {
	root, err := os.MkdirTemp("", "huahuo-runtime-adapter-candidate-cleanup-")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "artifact"), []byte("fixture"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := candidateValidationRemoveTemporaryRoot(root); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(root); !os.IsNotExist(err) {
		t.Fatalf("temporary root remains after explicit cleanup: %v", err)
	}
}

func writeCandidateValidationExecutable(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o700); err != nil {
		t.Fatal(err)
	}
}

func createCandidateValidationSymlinkOrSkip(t *testing.T, target, link string) {
	t.Helper()
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symbolic links are unavailable in this test environment: %v", err)
	}
}

func candidateValidationEnvironmentMap(values []string) map[string]string {
	result := make(map[string]string, len(values))
	for _, value := range values {
		key, value, found := strings.Cut(value, "=")
		if found {
			result[key] = value
		}
	}
	return result
}
