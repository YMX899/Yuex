package runtime

import "testing"

func TestRuntimeWorkspaceMountForPlanFailsClosedAndBoundsNativeWrite(t *testing.T) {
	budget := RuntimeToolBudget{
		MaxToolCalls: 200, SoftToolCallLimit: 160, FinalizationReserve: 10,
		MaxRepeatedCalls: 2, MaxNoProgressCalls: 4, MaxSearchCalls: 60,
		MaxWriteCalls: 20, MaxReadBytes: 1024, MaxWallTimeSeconds: 60,
	}
	cases := []struct {
		name       string
		plan       AgentRunPlan
		accessMode string
		wantRoots  []string
		wantError  string
	}{
		{
			name:       "omitted legacy write mode is read only",
			plan:       AgentRunPlan{RequiredTools: []string{"read"}, ToolBudget: budget},
			accessMode: RuntimeWorkspaceAccessRead,
			wantRoots:  []string{},
		},
		{
			name:       "explicit none is read only",
			plan:       AgentRunPlan{RequiredTools: []string{"read", "workspace_search"}, WriteMode: "none", ToolBudget: budget},
			accessMode: RuntimeWorkspaceAccessRead,
			wantRoots:  []string{},
		},
		{
			name:      "write without staging authorization is denied",
			plan:      AgentRunPlan{RequiredTools: []string{"read", "write"}, WriteMode: "none", ToolBudget: budget},
			wantError: "RUNTIME_PERMISSION_DENIED",
		},
		{
			name:      "staging authorization requires native write tool",
			plan:      AgentRunPlan{RequiredTools: []string{"read"}, WriteMode: "runtime_staging", ToolBudget: budget},
			wantError: "AGENT_PLAN_INVALID",
		},
		{
			name:       "staging authorization has only bounded roots",
			plan:       AgentRunPlan{RequiredTools: []string{"read", "write"}, WriteMode: "runtime_staging", ToolBudget: budget},
			accessMode: RuntimeWorkspaceAccessWrite,
			wantRoots:  []string{"output", "staging"},
		},
		{
			name:       "formal asset intent keeps native write bounded to Run roots",
			plan:       AgentRunPlan{RequiredTools: []string{"read", "write"}, WriteMode: "asset_write_intent", ToolBudget: budget},
			accessMode: RuntimeWorkspaceAccessWrite,
			wantRoots:  []string{"output", "staging"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mount, err := RuntimeWorkspaceMountForPlan(tc.plan)
			if tc.wantError != "" {
				if err == nil || err.Error() != tc.wantError {
					t.Fatalf("mount error=%v, want %s", err, tc.wantError)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if mount.AccessMode != tc.accessMode || !sameMountPolicyStringSet(mount.AllowedWriteRoots, tc.wantRoots) {
				t.Fatalf("mount=%+v, want mode=%s roots=%v", mount, tc.accessMode, tc.wantRoots)
			}
		})
	}
}

func TestRuntimeWorkspaceWriteLeaseBindsRunWorkspaceManifestAndRoots(t *testing.T) {
	plan := AgentRunPlan{AgentRunID: "run_write", RequiredTools: []string{"read", "write"}, WriteMode: "runtime_staging"}
	lease, err := NewRuntimeWorkspaceWriteLease(plan, "run_write", "workspace_1", "sha256:"+"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 123)
	if err != nil {
		t.Fatal(err)
	}
	if lease.Version != runtimeWriteLeaseVersion || lease.RunID != "run_write" || lease.WorkspaceID != "workspace_1" ||
		lease.WorkspaceManifestHash != "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" ||
		!sameMountPolicyStringSet(lease.AllowedRoots, []string{"output", "staging"}) || lease.ExpiresAt != 123 {
		t.Fatalf("unexpected write lease: %+v", lease)
	}
	if _, err := NewRuntimeWorkspaceWriteLease(plan, "run_other", "workspace_1", lease.WorkspaceManifestHash, 123); err == nil || err.Error() != "RUNTIME_PERMISSION_DENIED" {
		t.Fatalf("mismatched run lease error=%v", err)
	}
}

func sameMountPolicyStringSet(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	counts := make(map[string]int, len(left))
	for _, value := range left {
		counts[value]++
	}
	for _, value := range right {
		counts[value]--
	}
	for _, count := range counts {
		if count != 0 {
			return false
		}
	}
	return true
}
