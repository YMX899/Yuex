package runtime

import (
	"strings"

	"huahuoai/backend/source/internal/domain"
)

const (
	RuntimeWorkspaceAccessRead  = "read"
	RuntimeWorkspaceAccessWrite = "write"
	runtimeWriteLeaseVersion    = "huahuo.runtime-write-lease.v1"
)

var runtimeStagingOutputWriteRoots = []string{"output", "staging"}

// RuntimeWorkspaceMount is the only Plan-derived filesystem authorization
// that may reach the Runtime. Formal Workspace writes stay on the Workspace
// Service path; native write is restricted to this Run's materialized output
// and staging directories.
type RuntimeWorkspaceMount struct {
	AccessMode        string
	AllowedWriteRoots []string
}

// RuntimeWorkspaceWriteLease is carried inside the signed Runtime policy. Its
// roots are relative to the per-Run materialized workspace, never a formal
// Workspace or host path.
type RuntimeWorkspaceWriteLease struct {
	Version               string   `json:"version"`
	RunID                 string   `json:"runId"`
	WorkspaceID           string   `json:"workspaceId"`
	WorkspaceManifestHash string   `json:"workspaceManifestHash"`
	AllowedRoots          []string `json:"allowedRoots"`
	ExpiresAt             int64    `json:"expiresAt"`
}

// RuntimeWorkspaceMountForPlan fails closed. An omitted legacy WriteMode is
// treated as read-only, while any request for native write needs an explicit
// staging-capable Plan mode and the signed write tool policy. Formal asset
// intent preserves its separate authorization semantics but receives no
// broader native filesystem roots than an ordinary runtime_staging Run.
func RuntimeWorkspaceMountForPlan(plan AgentRunPlan) (RuntimeWorkspaceMount, error) {
	hasWrite := false
	for _, tool := range plan.RequiredTools {
		if tool == "write" {
			hasWrite = true
			break
		}
	}

	switch strings.TrimSpace(plan.WriteMode) {
	case "", "none":
		if hasWrite {
			return RuntimeWorkspaceMount{}, domain.ErrorCode("RUNTIME_PERMISSION_DENIED")
		}
		return RuntimeWorkspaceMount{AccessMode: RuntimeWorkspaceAccessRead, AllowedWriteRoots: []string{}}, nil
	case "runtime_staging", "asset_write_intent":
		if !hasWrite {
			return RuntimeWorkspaceMount{}, domain.ErrorCode("AGENT_PLAN_INVALID")
		}
		return RuntimeWorkspaceMount{
			AccessMode:        RuntimeWorkspaceAccessWrite,
			AllowedWriteRoots: append([]string(nil), runtimeStagingOutputWriteRoots...),
		}, nil
	default:
		return RuntimeWorkspaceMount{}, domain.ErrorCode("AGENT_PLAN_INVALID")
	}
}

// NewRuntimeWorkspaceWriteLease creates the bounded lease body that the
// Runtime policy signer binds to the Run, Workspace and manifest. It does not
// contain a secret; its authority is the enclosing Runtime policy signature.
func NewRuntimeWorkspaceWriteLease(plan AgentRunPlan, runID, workspaceID, manifestHash string, expiresAt int64) (RuntimeWorkspaceWriteLease, error) {
	mount, err := RuntimeWorkspaceMountForPlan(plan)
	if err != nil {
		return RuntimeWorkspaceWriteLease{}, err
	}
	if mount.AccessMode != RuntimeWorkspaceAccessWrite || strings.TrimSpace(runID) == "" || runID != plan.AgentRunID ||
		strings.TrimSpace(workspaceID) == "" || !validSHA256(manifestHash) || expiresAt <= 0 {
		return RuntimeWorkspaceWriteLease{}, domain.ErrorCode("RUNTIME_PERMISSION_DENIED")
	}
	return RuntimeWorkspaceWriteLease{
		Version: runtimeWriteLeaseVersion, RunID: runID, WorkspaceID: workspaceID,
		WorkspaceManifestHash: normalizeSHA256(manifestHash),
		AllowedRoots:          append([]string(nil), mount.AllowedWriteRoots...),
		ExpiresAt:             expiresAt,
	}, nil
}
