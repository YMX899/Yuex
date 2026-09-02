package runtime

import (
	"context"
	"strings"
	"time"

	"huahuoai/backend/source/internal/domain"
)

// RunTicketWorkspaceSearchProbe exercises the exact HMAC signing and
// verification boundary used by the internal Workspace Search route. The
// generated ticket is ephemeral and is never sent, logged, or persisted.
// It only proves that a real RunTicket can bind the selected Host and
// capability hash before Planning declares workspace_search usable.
type RunTicketWorkspaceSearchProbe struct {
	Secret string
	Now    func() time.Time
}

func NewRunTicketWorkspaceSearchProbe(secret string) RunTicketWorkspaceSearchProbe {
	return RunTicketWorkspaceSearchProbe{Secret: strings.TrimSpace(secret), Now: func() time.Time { return time.Now().UTC() }}
}

func (p RunTicketWorkspaceSearchProbe) VerifyWorkspaceSearchTicket(ctx context.Context, runtimeHostID, capabilityHash string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	runtimeHostID = strings.TrimSpace(runtimeHostID)
	capabilityHash = strings.TrimSpace(capabilityHash)
	if runtimeHostID == "" || capabilityHash == "" || strings.TrimSpace(p.Secret) == "" {
		return domain.ErrorCode("RUNTIME_TOOL_UNAVAILABLE")
	}
	now := time.Now().UTC()
	if p.Now != nil {
		now = p.Now().UTC()
	}
	claims := RunTicketClaims{
		RunID: "workspace-search-readiness", TenantID: "tenant-workspace-search-readiness", ReservationID: "workspace-search-readiness",
		RuntimeHostID: runtimeHostID, CapabilityHash: capabilityHash,
		WorkspaceID: "workspace-search-readiness", WorkspaceVersion: 1, ContextGeneration: 1,
		InputManifestHash: RunTicketJTIHash("workspace-search-readiness-input"), PlanHash: RunTicketJTIHash("workspace-search-readiness-plan"),
		FencingToken: 1, JTI: "workspace-search-readiness", IssuedAt: now.Add(-time.Second).Unix(), ExpiresAt: now.Add(time.Minute).Unix(),
	}
	ticket, err := SignRunTicket(claims, p.Secret)
	if err != nil {
		return domain.ErrorCode("RUNTIME_TOOL_UNAVAILABLE")
	}
	verified, err := VerifyRunTicket(ticket, p.Secret, now)
	if err != nil || verified.RuntimeHostID != runtimeHostID || verified.CapabilityHash != capabilityHash || verified.RunID != claims.RunID || verified.WorkspaceID != claims.WorkspaceID {
		return domain.ErrorCode("RUNTIME_TOOL_UNAVAILABLE")
	}
	return nil
}
