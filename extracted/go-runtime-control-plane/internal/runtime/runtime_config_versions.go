package runtime

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// RuntimeConfigVersions is the immutable ID-to-version view used when the
// Worker signs a Runtime submission. The Gateway reads the same config file.
type RuntimeConfigVersions map[string]string

// LoadRuntimeConfigVersions reads only the runtime config identity/version
// pairs. Model endpoints, pools, and secret references remain opaque here.
func LoadRuntimeConfigVersions(path string) (RuntimeConfigVersions, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, fmt.Errorf("RUNTIME_CONFIG_VERSION_INVALID")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("RUNTIME_CONFIG_VERSION_INVALID")
	}
	var document struct {
		RuntimeConfigs []struct {
			ID      string `json:"id"`
			Version string `json:"version"`
		} `json:"runtimeConfigs"`
	}
	if err := json.Unmarshal(raw, &document); err != nil || len(document.RuntimeConfigs) == 0 {
		return nil, fmt.Errorf("RUNTIME_CONFIG_VERSION_INVALID")
	}
	versions := make(RuntimeConfigVersions, len(document.RuntimeConfigs))
	for _, config := range document.RuntimeConfigs {
		id := strings.TrimSpace(config.ID)
		version := strings.TrimSpace(config.Version)
		if id == "" || id != config.ID || !ValidRuntimeSubmitConfigVersion(version) || version != config.Version {
			return nil, fmt.Errorf("RUNTIME_CONFIG_VERSION_INVALID")
		}
		if _, exists := versions[id]; exists {
			return nil, fmt.Errorf("RUNTIME_CONFIG_VERSION_INVALID")
		}
		versions[id] = version
	}
	return versions, nil
}

func (v RuntimeConfigVersions) VersionFor(runtimeConfigID string) (string, error) {
	if runtimeConfigID == "" || strings.TrimSpace(runtimeConfigID) != runtimeConfigID {
		return "", fmt.Errorf("RUNTIME_CONFIG_VERSION_INVALID")
	}
	version, ok := v[runtimeConfigID]
	if !ok || !ValidRuntimeSubmitConfigVersion(version) {
		return "", fmt.Errorf("RUNTIME_CONFIG_VERSION_INVALID")
	}
	return version, nil
}
