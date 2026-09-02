package runtime

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadRuntimeConfigVersionsLoadsExactRuntimeConfigVersions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "openclaw-enterprise-runtime.json")
	if err := os.WriteFile(path, []byte(`{"runtimeConfigs":[{"id":"huahuo-default","version":"v1"},{"id":"huahuo-faya-germination","version":"v3"}]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	versions, err := LoadRuntimeConfigVersions(path)
	if err != nil {
		t.Fatal(err)
	}
	for id, want := range map[string]string{"huahuo-default": "v1", "huahuo-faya-germination": "v3"} {
		got, err := versions.VersionFor(id)
		if err != nil || got != want {
			t.Fatalf("version id=%q got=%q err=%v want=%q", id, got, err, want)
		}
	}
	if _, err := versions.VersionFor("huahuo-missing"); err == nil || err.Error() != "RUNTIME_CONFIG_VERSION_INVALID" {
		t.Fatalf("missing runtime config error=%v", err)
	}
	if _, err := versions.VersionFor(" huahuo-default"); err == nil || err.Error() != "RUNTIME_CONFIG_VERSION_INVALID" {
		t.Fatalf("whitespace-normalized runtime config id error=%v", err)
	}
}

func TestLoadRuntimeConfigVersionsRejectsInvalidIdentityOrVersion(t *testing.T) {
	for name, body := range map[string]string{
		"missing":    `{}`,
		"duplicate":  `{"runtimeConfigs":[{"id":"huahuo-default","version":"v1"},{"id":"huahuo-default","version":"v2"}]}`,
		"whitespace": `{"runtimeConfigs":[{"id":" huahuo-default","version":"v1"}]}`,
		"invalid":    `{"runtimeConfigs":[{"id":"huahuo-default","version":"v1/unsafe"}]}`,
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "runtime.json")
			if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := LoadRuntimeConfigVersions(path); err == nil || err.Error() != "RUNTIME_CONFIG_VERSION_INVALID" {
				t.Fatalf("error=%v", err)
			}
		})
	}
}
