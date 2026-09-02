package runtime

import (
	"context"
	"testing"
	"time"
)

func TestRunTicketWorkspaceSearchProbeVerifiesHostAndCapabilityBinding(t *testing.T) {
	now := time.Date(2026, 7, 15, 10, 0, 0, 0, time.UTC)
	probe := NewRunTicketWorkspaceSearchProbe("workspace-search-probe-secret")
	probe.Now = func() time.Time { return now }
	if err := probe.VerifyWorkspaceSearchTicket(context.Background(), "host_workspace_search", "capability_workspace_search"); err != nil {
		t.Fatalf("workspace search ticket probe error=%v", err)
	}
}

func TestRunTicketWorkspaceSearchProbeFailsClosedWithoutSecretOrBinding(t *testing.T) {
	if err := NewRunTicketWorkspaceSearchProbe("").VerifyWorkspaceSearchTicket(context.Background(), "host", "capability"); err == nil || err.Error() != "RUNTIME_TOOL_UNAVAILABLE" {
		t.Fatalf("missing secret error=%v", err)
	}
	if err := NewRunTicketWorkspaceSearchProbe("secret").VerifyWorkspaceSearchTicket(context.Background(), "", "capability"); err == nil || err.Error() != "RUNTIME_TOOL_UNAVAILABLE" {
		t.Fatalf("missing host error=%v", err)
	}
}
