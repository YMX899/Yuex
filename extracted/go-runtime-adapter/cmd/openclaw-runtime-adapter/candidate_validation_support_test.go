package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

const candidateValidationMaximumCandidateBytes int64 = 128 << 20

// candidateValidationArtifactPolicy describes host-owned paths that an
// opt-in candidate check must never read as its supplied executable.
type candidateValidationArtifactPolicy struct {
	ForbiddenRoots   []string
	KnownActiveFiles []string
}

var candidateValidationDefaultArtifactPolicy = candidateValidationArtifactPolicy{
	ForbiddenRoots: []string{
		"/home/huahuo-runtime",
		"/home/agent-runtime",
		"/opt/huahuoai-backend/source/bin",
	},
	KnownActiveFiles: []string{
		"/opt/huahuoai-backend/source/bin/openclaw-runtime-adapter",
		"/home/huahuo-runtime/bin/openclaw-runtime-adapter",
	},
}

type candidateValidationBuildLayout struct {
	Root       string
	Home       string
	Temp       string
	GoTemp     string
	GoCache    string
	GoPath     string
	GoModCache string
}

func candidateValidationNewTemporaryRoot(t *testing.T) string {
	t.Helper()
	base, err := candidateValidationTemporaryBase(os.TempDir(), candidateValidationDefaultArtifactPolicy)
	if err != nil {
		t.Fatalf("validate candidate temporary base: %v", err)
	}
	root, err := os.MkdirTemp(base, "huahuo-runtime-adapter-candidate-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := candidateValidationRemoveTemporaryRoot(root); err != nil {
			t.Errorf("remove candidate validation temporary root: %v", err)
		}
	})
	return root
}

func candidateValidationTemporaryBase(base string, policy candidateValidationArtifactPolicy) (string, error) {
	if !filepath.IsAbs(base) {
		return "", fmt.Errorf("candidate temporary base must be absolute")
	}
	base = filepath.Clean(base)
	info, err := os.Stat(base)
	if err != nil {
		return "", fmt.Errorf("stat candidate temporary base: %w", err)
	}
	if !info.IsDir() {
		return "", fmt.Errorf("candidate temporary base is not a directory")
	}
	if err := candidateValidationRejectSymlinkPath(base); err != nil {
		return "", fmt.Errorf("candidate temporary base is not direct: %w", err)
	}
	for _, root := range policy.ForbiddenRoots {
		if !filepath.IsAbs(root) {
			return "", fmt.Errorf("configured forbidden root is not absolute")
		}
		cleanRoot := filepath.Clean(root)
		if candidateValidationPathWithin(base, cleanRoot) {
			return "", fmt.Errorf("candidate temporary base is under a protected Adapter root")
		}
		canonicalRoot, rootErr := filepath.EvalSymlinks(cleanRoot)
		if rootErr == nil && candidateValidationPathWithin(base, canonicalRoot) {
			return "", fmt.Errorf("candidate temporary base is under a resolved protected Adapter root")
		}
		if rootErr != nil && !errors.Is(rootErr, os.ErrNotExist) {
			return "", fmt.Errorf("resolve protected Adapter root: %w", rootErr)
		}
	}
	return base, nil
}

func candidateValidationRemoveTemporaryRoot(root string) error {
	if err := os.RemoveAll(root); err != nil {
		return err
	}
	if _, err := os.Lstat(root); !errors.Is(err, os.ErrNotExist) {
		if err == nil {
			return fmt.Errorf("temporary root remains after cleanup")
		}
		return fmt.Errorf("verify temporary root removal: %w", err)
	}
	return nil
}

func candidateValidationBuildLayoutForRoot(root string) (candidateValidationBuildLayout, error) {
	if !filepath.IsAbs(root) {
		return candidateValidationBuildLayout{}, fmt.Errorf("candidate build root must be absolute")
	}
	root = filepath.Clean(root)
	layout := candidateValidationBuildLayout{
		Root:       root,
		Home:       filepath.Join(root, "build-home"),
		Temp:       filepath.Join(root, "build-tmp"),
		GoTemp:     filepath.Join(root, "build-go-tmp"),
		GoCache:    filepath.Join(root, "build-go-cache"),
		GoPath:     filepath.Join(root, "build-go-path"),
		GoModCache: filepath.Join(root, "build-go-path", "pkg", "mod"),
	}
	for _, path := range []string{layout.Home, layout.Temp, layout.GoTemp, layout.GoCache, layout.GoPath, layout.GoModCache} {
		if !candidateValidationPathWithin(path, root) {
			return candidateValidationBuildLayout{}, fmt.Errorf("candidate build path escapes temporary root")
		}
	}
	return layout, nil
}

func candidateValidationBuildEnvironment(layout candidateValidationBuildLayout) []string {
	return []string{
		"HOME=" + layout.Home,
		"XDG_CACHE_HOME=" + filepath.Join(layout.Root, "build-xdg-cache"),
		"XDG_CONFIG_HOME=" + filepath.Join(layout.Root, "build-xdg-config"),
		"TMPDIR=" + layout.Temp,
		"TMP=" + layout.Temp,
		"TEMP=" + layout.Temp,
		"GOTMPDIR=" + layout.GoTemp,
		"GOCACHE=" + layout.GoCache,
		"GOPATH=" + layout.GoPath,
		"GOMODCACHE=" + layout.GoModCache,
		"GOPROXY=off",
		"GOSUMDB=off",
		"GONOPROXY=*",
		"GONOSUMDB=*",
		"GOTOOLCHAIN=local",
		"GOWORK=off",
		"GOENV=off",
		"GOVCS=*:off",
		"PATH=/usr/bin:/bin",
	}
}

func candidateValidationPrepareBuildLayout(root, offlineModuleCache string) (candidateValidationBuildLayout, error) {
	layout, err := candidateValidationBuildLayoutForRoot(root)
	if err != nil {
		return candidateValidationBuildLayout{}, err
	}
	for _, path := range []string{layout.Home, layout.Temp, layout.GoTemp, layout.GoCache, layout.GoPath} {
		if err := os.MkdirAll(path, 0o700); err != nil {
			return candidateValidationBuildLayout{}, err
		}
	}
	if err := candidateValidationCopyOfflineModuleCache(offlineModuleCache, layout.GoModCache); err != nil {
		return candidateValidationBuildLayout{}, err
	}
	return layout, nil
}

// candidateValidationCopyOfflineModuleCache makes the Go build's complete
// writable module-cache view fixture-local. The seed is read only as input;
// GOPROXY=off then makes a missing dependency a hard failure instead of a
// background download.
func candidateValidationCopyOfflineModuleCache(seed, destination string) error {
	if strings.TrimSpace(seed) == "" || !filepath.IsAbs(seed) {
		return fmt.Errorf("offline module cache seed must be an absolute path")
	}
	canonicalSeed, err := filepath.EvalSymlinks(filepath.Clean(seed))
	if err != nil {
		return fmt.Errorf("resolve offline module cache seed: %w", err)
	}
	seedInfo, err := os.Stat(canonicalSeed)
	if err != nil {
		return fmt.Errorf("stat offline module cache seed: %w", err)
	}
	if !seedInfo.IsDir() {
		return fmt.Errorf("offline module cache seed is not a directory")
	}
	if !filepath.IsAbs(destination) || filepath.Clean(destination) != destination {
		return fmt.Errorf("offline module cache destination must be a clean absolute path")
	}
	if err := os.MkdirAll(filepath.Dir(destination), 0o700); err != nil {
		return fmt.Errorf("create fixture module cache parent: %w", err)
	}
	if err := os.Mkdir(destination, 0o700); err != nil {
		return fmt.Errorf("create fixture module cache: %w", err)
	}
	return filepath.WalkDir(canonicalSeed, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relative, err := filepath.Rel(canonicalSeed, path)
		if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(os.PathSeparator)) || filepath.IsAbs(relative) {
			return fmt.Errorf("offline module cache path escapes seed")
		}
		if relative == "." {
			return nil
		}
		target := filepath.Join(destination, relative)
		if !candidateValidationPathWithin(target, destination) {
			return fmt.Errorf("offline module cache path escapes fixture")
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("offline module cache contains a symbolic link")
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		switch {
		case info.IsDir():
			return os.Mkdir(target, 0o700)
		case info.Mode().IsRegular():
			return candidateValidationCopyRegularFile(path, target, 0o600, -1)
		default:
			return fmt.Errorf("offline module cache contains a non-regular entry")
		}
	})
}

func candidateValidationCopySuppliedCandidate(fixtureRoot, destination, supplied string, policy candidateValidationArtifactPolicy) error {
	if !filepath.IsAbs(fixtureRoot) || !filepath.IsAbs(destination) || !filepath.IsAbs(supplied) {
		return fmt.Errorf("candidate paths must be absolute")
	}
	fixtureRoot = filepath.Clean(fixtureRoot)
	destination = filepath.Clean(destination)
	supplied = filepath.Clean(supplied)
	if !candidateValidationPathWithin(destination, fixtureRoot) || destination == fixtureRoot {
		return fmt.Errorf("candidate destination escapes fixture root")
	}
	if err := candidateValidationRejectSymlinkPath(fixtureRoot); err != nil {
		return fmt.Errorf("fixture root is not a direct directory: %w", err)
	}
	if err := candidateValidationRejectSymlinkPath(filepath.Dir(destination)); err != nil {
		return fmt.Errorf("candidate destination parent is not a direct directory: %w", err)
	}
	validatedInfo, err := candidateValidationRejectArtifactPath(supplied, policy)
	if err != nil {
		return err
	}

	input, err := os.Open(supplied)
	if err != nil {
		return fmt.Errorf("open supplied candidate: %w", err)
	}
	defer input.Close()
	inputInfo, err := input.Stat()
	if err != nil {
		return fmt.Errorf("stat opened supplied candidate: %w", err)
	}
	if !os.SameFile(validatedInfo, inputInfo) {
		return fmt.Errorf("supplied candidate changed after validation")
	}
	if err := candidateValidationRejectActiveArtifactIdentity(supplied, inputInfo, policy); err != nil {
		return err
	}
	if inputInfo.Size() < 0 || inputInfo.Size() > candidateValidationMaximumCandidateBytes {
		return fmt.Errorf("supplied candidate exceeds %d byte limit", candidateValidationMaximumCandidateBytes)
	}
	if err := candidateValidationCopyRegularOpenFile(input, destination, 0o700, inputInfo.Size()); err != nil {
		return fmt.Errorf("copy supplied candidate: %w", err)
	}
	return nil
}

func candidateValidationRejectArtifactPath(path string, policy candidateValidationArtifactPolicy) (os.FileInfo, error) {
	if err := candidateValidationRejectSymlinkPath(path); err != nil {
		return nil, fmt.Errorf("supplied candidate symbolic-link alias is forbidden: %w", err)
	}
	canonicalPath, err := filepath.EvalSymlinks(path)
	if err != nil {
		return nil, fmt.Errorf("resolve supplied candidate: %w", err)
	}
	for _, root := range policy.ForbiddenRoots {
		if !filepath.IsAbs(root) {
			return nil, fmt.Errorf("configured forbidden root is not absolute")
		}
		cleanRoot := filepath.Clean(root)
		if candidateValidationPathWithin(path, cleanRoot) || candidateValidationPathWithin(canonicalPath, cleanRoot) {
			return nil, fmt.Errorf("supplied candidate is under a protected Adapter path")
		}
		canonicalRoot, rootErr := filepath.EvalSymlinks(cleanRoot)
		if rootErr == nil && candidateValidationPathWithin(canonicalPath, canonicalRoot) {
			return nil, fmt.Errorf("supplied candidate is under a resolved protected Adapter path")
		}
		if rootErr != nil && !errors.Is(rootErr, os.ErrNotExist) {
			return nil, fmt.Errorf("resolve protected Adapter root: %w", rootErr)
		}
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("stat supplied candidate: %w", err)
	}
	if !candidateValidationRegularExecutable(info) {
		return nil, fmt.Errorf("supplied candidate is not a regular executable")
	}
	if err := candidateValidationRejectActiveArtifactIdentity(path, info, policy); err != nil {
		return nil, err
	}
	return info, nil
}

func candidateValidationRegularExecutable(info os.FileInfo) bool {
	if !info.Mode().IsRegular() {
		return false
	}
	// The candidate process itself is Linux-only. Windows cannot represent the
	// Unix execute bit in os.FileMode, so portable guard tests retain only the
	// regular-file assertion there.
	return runtime.GOOS == "windows" || info.Mode()&0o111 != 0
}

func candidateValidationRejectActiveArtifactIdentity(candidatePath string, candidateInfo os.FileInfo, policy candidateValidationArtifactPolicy) error {
	for _, activePath := range policy.KnownActiveFiles {
		if !filepath.IsAbs(activePath) {
			return fmt.Errorf("configured active Adapter path is not absolute")
		}
		activePath = filepath.Clean(activePath)
		activeInfo, err := os.Stat(activePath)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return fmt.Errorf("stat protected active Adapter artifact: %w", err)
		}
		canonicalActivePath, err := filepath.EvalSymlinks(activePath)
		if err != nil {
			return fmt.Errorf("resolve protected active Adapter artifact: %w", err)
		}
		if candidateValidationPathWithin(candidatePath, activePath) || candidateValidationPathWithin(candidatePath, canonicalActivePath) || os.SameFile(candidateInfo, activeInfo) {
			return fmt.Errorf("supplied candidate aliases an active Adapter artifact")
		}
	}
	return nil
}

func candidateValidationRejectSymlinkPath(path string) error {
	canonical, err := filepath.EvalSymlinks(path)
	if err != nil {
		return err
	}
	if filepath.Clean(path) != filepath.Clean(canonical) {
		return fmt.Errorf("symbolic-link component")
	}
	return nil
}

func candidateValidationPathWithin(path, root string) bool {
	relative, err := filepath.Rel(filepath.Clean(root), filepath.Clean(path))
	if err != nil {
		return false
	}
	return relative == "." || (relative != ".." && !strings.HasPrefix(relative, ".."+string(os.PathSeparator)) && !filepath.IsAbs(relative))
}

func candidateValidationCopyRegularFile(source, destination string, permissions os.FileMode, expectedBytes int64) error {
	input, err := os.Open(source)
	if err != nil {
		return err
	}
	defer input.Close()
	return candidateValidationCopyRegularOpenFile(input, destination, permissions, expectedBytes)
}

func candidateValidationCopyRegularOpenFile(input *os.File, destination string, permissions os.FileMode, expectedBytes int64) error {
	output, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, permissions)
	if err != nil {
		return err
	}
	bytesWritten, copyErr := io.Copy(output, input)
	closeErr := output.Close()
	if copyErr != nil || closeErr != nil {
		_ = os.Remove(destination)
		return fmt.Errorf("copy=%v close=%v", copyErr, closeErr)
	}
	if expectedBytes >= 0 && bytesWritten != expectedBytes {
		_ = os.Remove(destination)
		return fmt.Errorf("copied %d bytes, expected %d", bytesWritten, expectedBytes)
	}
	return nil
}
