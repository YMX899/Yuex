package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"huahuoai/backend/source/internal/domain"
)

func TestValidateL1AgentManifestRejectsUnsafeAgentProfile(t *testing.T) {
	manifest := L1AgentManifest{Version: "manifest_v1", Agents: []L1AgentManifestEntry{{
		AgentProfile: "workspace agent", DisplayName: "Workspace Agent", Status: "active", Version: "v1", Hash: "agent_hash",
		RelativeRoot: "agents/workspace_agent", IntentCategories: []string{"workspace_lookup"}, TaskTypes: []string{"workspace_lookup"},
		CandidateSkillProfiles: []string{"general_chat"}, KnowledgeRoots: []string{"knowledge/navigation"},
		ToolPolicyProfile: "workspace_read_only", ExecutionScopes: []string{"product_thread"}, MaxCandidateSkills: 1,
	}}}
	if err := ValidateL1AgentManifestForDynamicPlanning(manifest); err == nil || err.Error() != "AGENT_PLAN_INVALID" {
		t.Fatalf("unsafe Agent profile err=%v, want AGENT_PLAN_INVALID", err)
	}
}

func TestL1AgentRouterRoutesExplicitPublicMetaWorkspace(t *testing.T) {
	intent := metaWorkspaceTestIntent()
	manifest := L1AgentManifest{Version: "manifest_meta_v1", Agents: []L1AgentManifestEntry{
		metaWorkspaceAgent("general_agent", "general", true, "active", 100),
		metaWorkspaceAgent("internal_agent", "internal", false, "active", 200),
	}}
	result, err := NewL1AgentRouter().Route(context.Background(), L1AgentRouteCommand{
		Intent: intent, Manifest: manifest, Permissions: metaWorkspacePermissions(), ExpectedMetaWorkspaceKey: "general",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.AgentProfile != "general_agent" || result.MetaWorkspaceKey != "general" || result.MetaWorkspaceVersion != "v1" || result.InputPolicyHash == "" || result.RouteReason != "expected_meta_workspace_key" {
		t.Fatalf("explicit route=%+v", result)
	}
}

func TestL1AgentRouterProjectsAtomicMetaWorkspaceIdentity(t *testing.T) {
	intent := metaWorkspaceTestIntent()
	internalManifest := L1AgentManifest{Version: "manifest_internal_v1", Agents: []L1AgentManifestEntry{
		metaWorkspaceAgent("internal_agent", "", false, "active", 100),
	}}
	internal, err := NewL1AgentRouter().Route(context.Background(), L1AgentRouteCommand{
		Intent: intent, Manifest: internalManifest, Permissions: metaWorkspacePermissions(),
	})
	if err != nil {
		t.Fatalf("internal route: %v", err)
	}
	if internal.MetaWorkspaceKey != "" || internal.MetaWorkspaceVersion != "" || internal.InputPolicyHash != "" {
		t.Fatalf("internal route must project an all-empty Meta identity: %+v", internal)
	}

	publicManifest := L1AgentManifest{Version: "manifest_public_v1", Agents: []L1AgentManifestEntry{
		metaWorkspaceAgent("visual_agent", "visual_chat", true, "active", 100),
	}}
	public, err := NewL1AgentRouter().Route(context.Background(), L1AgentRouteCommand{
		Intent: intent, Manifest: publicManifest, Permissions: metaWorkspacePermissions(), ExpectedMetaWorkspaceKey: "visual_chat",
	})
	if err != nil {
		t.Fatalf("public route: %v", err)
	}
	if public.MetaWorkspaceKey != "visual_chat" || public.MetaWorkspaceVersion != "v1" || public.InputPolicyHash == "" {
		t.Fatalf("public route must project a complete Meta identity: %+v", public)
	}
	publicIntent := intent
	publicIntent.AgentRunID = "public_meta_route_plan"
	publicIntent.ExpectedOutput = "text"
	plan, err := NewCapabilityPlanner().BuildDeterministicPlan(publicIntent, public)
	if err != nil || plan.MetaWorkspaceKey != public.MetaWorkspaceKey || plan.MetaWorkspaceVersion != public.MetaWorkspaceVersion || plan.InputPolicyHash != public.InputPolicyHash {
		t.Fatalf("public route must remain valid through planning: plan=%+v err=%v", plan, err)
	}

	publicManifest.Agents[0].InputPolicyHash = ""
	_, err = NewL1AgentRouter().Route(context.Background(), L1AgentRouteCommand{
		Intent: intent, Manifest: publicManifest, Permissions: metaWorkspacePermissions(), ExpectedMetaWorkspaceKey: "visual_chat",
	})
	assertMetaWorkspaceRouteError(t, err, "META_WORKSPACE_UNAVAILABLE")
}

func TestL1AgentRouterRejectsUnavailablePublicMetaWorkspace(t *testing.T) {
	intent := metaWorkspaceTestIntent()
	cases := []struct {
		name     string
		manifest L1AgentManifest
		expected string
	}{
		{
			name: "unknown", manifest: L1AgentManifest{Version: "v1", Agents: []L1AgentManifestEntry{metaWorkspaceAgent("general_agent", "general", true, "active", 100)}}, expected: "missing",
		},
		{
			name: "disabled", manifest: L1AgentManifest{Version: "v1", Agents: []L1AgentManifestEntry{metaWorkspaceAgent("general_agent", "general", true, "inactive", 100)}}, expected: "general",
		},
		{
			name: "incompatible", manifest: L1AgentManifest{Version: "v1", Agents: []L1AgentManifestEntry{func() L1AgentManifestEntry {
				entry := metaWorkspaceAgent("other_agent", "other", true, "active", 100)
				entry.TaskTypes = []string{"other_task"}
				return entry
			}()}}, expected: "other",
		},
		{
			name: "internal only", manifest: L1AgentManifest{Version: "v1", Agents: []L1AgentManifestEntry{metaWorkspaceAgent("internal_agent", "internal", false, "active", 100)}}, expected: "internal",
		},
	}
	for _, item := range cases {
		t.Run(item.name, func(t *testing.T) {
			_, err := NewL1AgentRouter().Route(context.Background(), L1AgentRouteCommand{
				Intent: intent, Manifest: item.manifest, Permissions: metaWorkspacePermissions(), ExpectedMetaWorkspaceKey: item.expected,
			})
			assertMetaWorkspaceRouteError(t, err, "META_WORKSPACE_UNAVAILABLE")
		})
	}
}

func TestL1AgentRouterRequiresSelectionForMultiplePublicMetaWorkspaces(t *testing.T) {
	intent := metaWorkspaceTestIntent()
	manifest := L1AgentManifest{Version: "v1", Agents: []L1AgentManifestEntry{
		metaWorkspaceAgent("alpha_agent", "alpha", true, "active", 200),
		metaWorkspaceAgent("beta_agent", "beta", true, "active", 100),
	}}
	_, err := NewL1AgentRouter().Route(context.Background(), L1AgentRouteCommand{
		Intent: intent, Manifest: manifest, Permissions: metaWorkspacePermissions(),
	})
	assertMetaWorkspaceRouteError(t, err, "META_WORKSPACE_SELECTION_REQUIRED")
}

func TestL1AgentRouterRejectsPriorityShadowedCatalogAlternatives(t *testing.T) {
	intent := metaWorkspaceTestIntent()
	duplicatePublicManifest := L1AgentManifest{Version: "v1", Agents: []L1AgentManifestEntry{
		metaWorkspaceAgent("first_agent", "only", true, "active", 100),
		metaWorkspaceAgent("second_agent", "only", true, "active", 200),
	}}
	_, err := NewL1AgentRouter().Route(context.Background(), L1AgentRouteCommand{
		Intent: intent, Manifest: duplicatePublicManifest, Permissions: metaWorkspacePermissions(),
	})
	assertMetaWorkspaceRouteError(t, err, "AGENT_PROFILE_UNAVAILABLE")
	_, err = NewL1AgentRouter().Route(context.Background(), L1AgentRouteCommand{
		Intent: intent, Manifest: duplicatePublicManifest, Permissions: metaWorkspacePermissions(), ExpectedMetaWorkspaceKey: "only",
	})
	assertMetaWorkspaceRouteError(t, err, "META_WORKSPACE_UNAVAILABLE")
	_, err = ResolvePublicMetaWorkspace(duplicatePublicManifest, metaWorkspacePermissions(), "only")
	assertMetaWorkspaceRouteError(t, err, "META_WORKSPACE_UNAVAILABLE")

	internalIntent := intent
	internalIntent.ExecutionScope = "detached_task"
	internalManifest := L1AgentManifest{Version: "v1", Agents: []L1AgentManifestEntry{
		metaWorkspaceAgent("internal_low", "", false, "active", 100),
		metaWorkspaceAgent("internal_high", "", false, "active", 200),
	}}
	for index := range internalManifest.Agents {
		internalManifest.Agents[index].ExecutionScopes = []string{"detached_task"}
	}
	_, err = NewL1AgentRouter().Route(context.Background(), L1AgentRouteCommand{
		Intent: internalIntent, Manifest: internalManifest, Permissions: metaWorkspacePermissions(),
	})
	assertMetaWorkspaceRouteError(t, err, "AGENT_PROFILE_UNAVAILABLE")
}

func TestL1AgentRouterUsesSolePublicMetaWorkspaceBeforePrivatePriority(t *testing.T) {
	intent := metaWorkspaceTestIntent()
	manifest := L1AgentManifest{Version: "v1", Agents: []L1AgentManifestEntry{
		metaWorkspaceAgent("public_agent", "general", true, "active", 1),
		metaWorkspaceAgent("private_agent", "private", false, "active", 999),
	}}
	result, err := NewL1AgentRouter().Route(context.Background(), L1AgentRouteCommand{
		Intent: intent, Manifest: manifest, Permissions: metaWorkspacePermissions(),
	})
	if err != nil || result.AgentProfile != "public_agent" || result.MetaWorkspaceKey != "general" || result.RouteReason != "single_public_meta_workspace" {
		t.Fatalf("public discriminator route=%+v err=%v", result, err)
	}
}

func TestListPublicMetaWorkspacesIsNarrowAndStable(t *testing.T) {
	manifest := L1AgentManifest{Version: "v1", Agents: []L1AgentManifestEntry{
		metaWorkspaceAgent("z_agent", "zeta", true, "active", 100),
		metaWorkspaceAgent("a_agent", "alpha", true, "active", 100),
		metaWorkspaceAgent("a_second_agent", "alpha_second", true, "active", 100),
		metaWorkspaceAgent("private_agent", "private", false, "active", 100),
		metaWorkspaceAgent("disabled_agent", "disabled", true, "inactive", 100),
		metaWorkspaceAgent("duplicate_alpha", "alpha", true, "active", 90),
	}}
	manifest.Agents[0].DisplayName = "Zeta Workspace"
	manifest.Agents[1].DisplayName = "Alpha Workspace"
	manifest.Agents[2].DisplayName = "Alpha Workspace"
	manifest.Agents[5].DisplayName = "Alpha Workspace"
	items := ListPublicMetaWorkspaces(manifest)
	want := []PublicMetaWorkspace{
		publicMetaWorkspaceTestValue("alpha_second", "Alpha Workspace"),
		publicMetaWorkspaceTestValue("zeta", "Zeta Workspace"),
	}
	if !reflect.DeepEqual(items, want) {
		t.Fatalf("public meta workspaces=%#v want=%#v", items, want)
	}
	raw, err := json.Marshal(items)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"agentProfile", "hash", "relativeRoot", "skill"} {
		if strings.Contains(string(raw), forbidden) {
			t.Fatalf("public meta workspace leaked %q: %s", forbidden, raw)
		}
	}
}

func TestValidateL1AgentManifestRejectsInvalidPublicMetaWorkspace(t *testing.T) {
	manifest := L1AgentManifest{Version: "v1", Agents: []L1AgentManifestEntry{metaWorkspaceAgent("agent", "", true, "active", 100)}}
	if err := ValidateL1AgentManifestForDynamicPlanning(manifest); err == nil || err.Error() != "AGENT_PLAN_INVALID" {
		t.Fatalf("missing public Meta Workspace err=%v", err)
	}
	manifest.Agents[0].MetaWorkspaceKey = "shared"
	duplicate := metaWorkspaceAgent("agent_b", "shared", true, "active", 90)
	duplicate.DisplayName = manifest.Agents[0].DisplayName
	manifest.Agents = append(manifest.Agents, duplicate)
	if err := ValidateL1AgentManifestForDynamicPlanning(manifest); err == nil || err.Error() != "AGENT_PLAN_INVALID" {
		t.Fatalf("duplicate public Meta Workspace err=%v", err)
	}
}

func TestL1AgentRouterUsesServerOwnedCandidateDiscriminatorBeforePriority(t *testing.T) {
	tests := []struct {
		name     string
		intent   domain.TaskIntent
		manifest L1AgentManifest
		want     string
	}{
		{
			name: "canonical level two positioning", intent: domain.TaskIntent{
				Category: "profile_understanding", ResolvedTaskType: "profile_understanding", ExecutionScope: "product_thread",
				CandidateL1Agents: []string{"dingwei_lv2_agent"},
			},
			manifest: l1CandidateDiscriminatorManifest("profile_understanding", "profile_understanding", []L1AgentManifestEntry{
				metaWorkspaceAgent("dingwei_lv1_agent", "workspace.positioning-l1", false, "active", 100),
				metaWorkspaceAgent("dingwei_lv2_agent", "workspace.positioning-l2", false, "active", 90),
			}),
			want: "dingwei_lv2_agent",
		},
		{
			name: "workspace research over generic work ai", intent: domain.TaskIntent{
				Category: "workspace_lookup", ResolvedTaskType: "workspace_lookup", ExecutionScope: "product_thread",
				CandidateL1Agents: []string{"workspace_research_agent"},
			},
			manifest: l1CandidateDiscriminatorManifest("workspace_lookup", "workspace_lookup", []L1AgentManifestEntry{
				metaWorkspaceAgent("workspace_research_agent", "workspace.research", false, "active", 110),
				metaWorkspaceAgent("work_ai_agent", "work_ai", true, "active", 100),
			}),
			want: "workspace_research_agent",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := NewL1AgentRouter().Route(context.Background(), L1AgentRouteCommand{
				Intent: test.intent, Manifest: test.manifest, Permissions: metaWorkspacePermissions(),
			})
			if err != nil || result.AgentProfile != test.want || result.RouteReason != "single_eligible_agent" {
				t.Fatalf("server-owned route=%+v err=%v", result, err)
			}
		})
	}
}

func TestL1AgentRouterRequiresPublicSelectionForGenericContentAlternatives(t *testing.T) {
	intent := domain.TaskIntent{Category: "content_creation", ResolvedTaskType: "work_ai_content_creation", ExecutionScope: "product_thread"}
	manifest := l1CandidateDiscriminatorManifest("content_creation", "work_ai_content_creation", []L1AgentManifestEntry{
		metaWorkspaceAgent("renshe_neirong_agent", "renshe_content", true, "active", 100),
		metaWorkspaceAgent("huoke_neirong_agent", "huoke_content", true, "active", 90),
	})
	_, err := NewL1AgentRouter().Route(context.Background(), L1AgentRouteCommand{
		Intent: intent, Manifest: manifest, Permissions: metaWorkspacePermissions(),
	})
	assertMetaWorkspaceRouteError(t, err, "META_WORKSPACE_SELECTION_REQUIRED")

	for key, want := range map[string]string{"renshe_content": "renshe_neirong_agent", "huoke_content": "huoke_neirong_agent"} {
		result, routeErr := NewL1AgentRouter().Route(context.Background(), L1AgentRouteCommand{
			Intent: intent, Manifest: manifest, Permissions: metaWorkspacePermissions(), ExpectedMetaWorkspaceKey: key,
		})
		if routeErr != nil || result.AgentProfile != want || result.RouteReason != "expected_meta_workspace_key" {
			t.Fatalf("explicit content key %q route=%+v err=%v", key, result, routeErr)
		}
	}
}

func l1CandidateDiscriminatorManifest(category, taskType string, agents []L1AgentManifestEntry) L1AgentManifest {
	for index := range agents {
		agents[index].IntentCategories = []string{category}
		agents[index].TaskTypes = []string{taskType}
	}
	return L1AgentManifest{Version: "candidate-discriminator-v1", Agents: agents}
}

func metaWorkspaceTestIntent() domain.TaskIntent {
	return domain.TaskIntent{
		Category: "general_conversation", ResolvedTaskType: "work_ai_general_chat", ExecutionScope: "product_thread",
	}
}

func metaWorkspacePermissions() AgentPermissionSnapshot {
	return AgentPermissionSnapshot{Features: map[string]bool{}, MembershipLevel: 1}
}

func metaWorkspaceAgent(profile, key string, public bool, status string, priority int) L1AgentManifestEntry {
	policy := metaWorkspaceTestInputPolicy(key)
	return L1AgentManifestEntry{
		AgentProfile: profile, DisplayName: "Workspace " + profile, MetaWorkspaceKey: key, PublicSelectable: public,
		DefaultTaskType: "work_ai_general_chat",
		InputPolicy:     policy, InputPolicyHash: metaWorkspaceInputPolicyHash(policy),
		Status: status, Version: "v1", Hash: "hash_" + profile, RelativeRoot: "agents/" + profile,
		IntentCategories: []string{"general_conversation"}, TaskTypes: []string{"work_ai_general_chat"},
		CandidateSkillProfiles: []string{"general_chat"}, ToolPolicyProfile: "workspace_read_only",
		ExecutionScopes: []string{"product_thread"}, MaxCandidateSkills: 1, Priority: priority,
	}
}

func metaWorkspaceTestInputPolicy(key string) MetaWorkspaceInputPolicy {
	_ = key
	return MetaWorkspaceInputPolicy{Usage: "none", AcceptsText: true, AcceptedImageMIMETypes: []string{}}
}

func publicMetaWorkspaceTestValue(key, displayName string) PublicMetaWorkspace {
	policy := metaWorkspaceTestInputPolicy(key)
	return PublicMetaWorkspace{MetaWorkspaceKey: key, DisplayName: displayName, Version: "v1", InputPolicy: policy, InputPolicyHash: metaWorkspaceInputPolicyHash(policy)}
}

func assertMetaWorkspaceRouteError(t *testing.T, err error, want string) {
	t.Helper()
	var apiErr *domain.APIError
	if !errors.As(err, &apiErr) || apiErr.Code != want {
		t.Fatalf("error=%v api=%#v want=%s", err, apiErr, want)
	}
}
