package runtime

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestRuntimeCapabilityClientCacheDoesNotExposeMutableToolSlice(t *testing.T) {
	client := NewRuntimeCapabilityClient(HTTPTransportOpenClawClient{})
	host := RuntimeHost{RuntimeHostID: "host_capability_cache", CapabilityHash: "capability_cache"}
	capabilities := runtimeCapabilityClientTestCapabilities(host.CapabilityHash)
	client.cache[host.RuntimeHostID+"\x00"+host.CapabilityHash] = runtimeCapabilityCacheEntry{
		value: capabilities, expiresAt: time.Now().UTC().Add(time.Minute),
	}

	first, err := client.GetCapabilities(context.Background(), host)
	if err != nil {
		t.Fatalf("get cached capabilities: %v", err)
	}
	first.Tools[0].Status = "degraded"
	second, err := client.GetCapabilities(context.Background(), host)
	if err != nil {
		t.Fatalf("get cached capabilities after caller mutation: %v", err)
	}
	if second.Tools[0].Status != "ready" {
		t.Fatalf("caller mutation poisoned cached capability: %+v", second.Tools[0])
	}
}

func TestRuntimeCapabilityClientRejectsZeroToolBudgetAndMissingCapabilityHash(t *testing.T) {
	client := NewRuntimeCapabilityClient(HTTPTransportOpenClawClient{})
	capabilities := runtimeCapabilityClientTestCapabilities("capability_validation")
	if err := client.ValidateRequiredTools(capabilities, []string{"read"}, RuntimeToolBudget{}); err == nil || err.Error() != "RUNTIME_TOOL_BUDGET_UNSUPPORTED" {
		t.Fatalf("zero tool budget error=%v, want RUNTIME_TOOL_BUDGET_UNSUPPORTED", err)
	}
	capabilities.CapabilityHash = ""
	if err := ValidateRuntimeCapabilities(capabilities); err == nil || err.Error() != "RUNTIME_TOOL_UNAVAILABLE" {
		t.Fatalf("missing capability hash error=%v, want RUNTIME_TOOL_UNAVAILABLE", err)
	}
}

func TestRuntimeSubmitBindingCapabilityRequiresExactV2Contract(t *testing.T) {
	valid := RuntimeSubmitBindingCapability{Version: RuntimeSubmitBindingV2, ProductSessionHash: true}
	if err := ValidateRuntimeSubmitBindingCapability(valid); err != nil {
		t.Fatalf("valid v2 submit binding capability error=%v", err)
	}
	for name, capability := range map[string]RuntimeSubmitBindingCapability{
		"missing":                 {},
		"legacy_version":          {Version: "runtime_submit_binding.v1", ProductSessionHash: true},
		"missing_product_session": {Version: RuntimeSubmitBindingV2},
		"unexpected_version":      {Version: "runtime_submit_binding.v2 ", ProductSessionHash: true},
	} {
		t.Run(name, func(t *testing.T) {
			if err := ValidateRuntimeSubmitBindingCapability(capability); err == nil || err.Error() != "RUNTIME_TOOL_UNAVAILABLE" {
				t.Fatalf("capability=%+v error=%v", capability, err)
			}
		})
	}
}

func TestRuntimeCapabilityDocumentAndPlannerSnapshotRequireExactV2SubmitBinding(t *testing.T) {
	for name, mutate := range map[string]func(*RuntimeCapabilities){
		"missing": func(capabilities *RuntimeCapabilities) {
			capabilities.SubmitBinding = RuntimeSubmitBindingCapability{}
		},
		"legacy": func(capabilities *RuntimeCapabilities) {
			capabilities.SubmitBinding.Version = "runtime_submit_binding.v1"
		},
		"missing_product_session_hash": func(capabilities *RuntimeCapabilities) {
			capabilities.SubmitBinding.ProductSessionHash = false
		},
	} {
		t.Run(name, func(t *testing.T) {
			capabilities := runtimeCapabilityClientTestCapabilities("capability_submit_binding_" + name)
			mutate(&capabilities)
			if err := ValidateRuntimeCapabilities(capabilities); err == nil || err.Error() != "RUNTIME_TOOL_UNAVAILABLE" {
				t.Fatalf("live document capability=%+v error=%v", capabilities.SubmitBinding, err)
			}
			if err := ValidateRuntimePlanAvailability(capabilities.PlannerSnapshot(), []string{"read"}, defaultRuntimeToolBudget()); err == nil || err.Error() != "RUNTIME_TOOL_UNAVAILABLE" {
				t.Fatalf("planner snapshot capability=%+v error=%v", capabilities.SubmitBinding, err)
			}
		})
	}
}

func TestRuntimeCapabilityClientFreshProbeBypassesSameHashCache(t *testing.T) {
	host := RuntimeHost{RuntimeHostID: "host_capability_fresh", CapabilityHash: "capability_fresh"}
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet || request.URL.Path != "/enterprise.runtime/capabilities" {
			t.Fatalf("request=%s %s", request.Method, request.URL.Path)
		}
		requests++
		capabilities := runtimeCapabilityClientTestCapabilities(host.CapabilityHash)
		if requests > 1 {
			// Simulate an Adapter restart/downgrade that leaves its Host identity
			// and capability hash unchanged.
			capabilities.SubmitBinding = RuntimeSubmitBindingCapability{}
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(capabilities); err != nil {
			t.Fatal(err)
		}
	}))
	defer server.Close()
	host.Endpoint = server.URL
	client := NewRuntimeCapabilityClient(HTTPTransportOpenClawClient{HTTPClient: server.Client()})

	if _, err := client.GetCapabilities(context.Background(), host); err != nil {
		t.Fatalf("seed cached capability: %v", err)
	}
	if _, err := client.GetFreshCapabilities(context.Background(), host); err == nil || err.Error() != "RUNTIME_TOOL_UNAVAILABLE" {
		t.Fatalf("fresh probe accepted downgraded same-hash Host: %v", err)
	}
	if requests != 2 {
		t.Fatalf("capability requests=%d, want cache seed plus fresh final probe", requests)
	}
}

func TestRuntimeCapabilityClientProjectsOnlyTheExplicitStableTestAdapterCompatibility(t *testing.T) {
	t.Setenv("HUAHUO_RUNTIME_LEGACY_RUN_TICKET_COMPAT", "1")
	host := RuntimeHost{
		RuntimeHostID: "runtime-host-test-1", Environment: "test", AdapterVersion: "v0.5", CapabilityHash: "capability_legacy_test_adapter",
	}
	capabilities := runtimeCapabilityClientTestCapabilities(host.CapabilityHash)
	capabilities.SubmitBinding = RuntimeSubmitBindingCapability{}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet || request.URL.Path != "/enterprise.runtime/capabilities" {
			t.Fatalf("request=%s %s", request.Method, request.URL.Path)
		}
		writer.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(writer).Encode(capabilities); err != nil {
			t.Fatal(err)
		}
	}))
	defer server.Close()
	host.Endpoint = server.URL

	loaded, err := NewRuntimeCapabilityClient(HTTPTransportOpenClawClient{HTTPClient: server.Client()}).GetFreshCapabilities(context.Background(), host)
	if err != nil {
		t.Fatalf("explicit legacy test Host compatibility failed: %v", err)
	}
	if loaded.SubmitBinding != (RuntimeSubmitBindingCapability{Version: RuntimeSubmitBindingV2, ProductSessionHash: true}) {
		t.Fatalf("projected binding=%+v", loaded.SubmitBinding)
	}

	host.RuntimeHostID = "runtime-host-other"
	if _, err := NewRuntimeCapabilityClient(HTTPTransportOpenClawClient{HTTPClient: server.Client()}).GetFreshCapabilities(context.Background(), host); err == nil || err.Error() != "RUNTIME_TOOL_UNAVAILABLE" {
		t.Fatalf("non-test Host compatibility err=%v", err)
	}
}

func TestRuntimeToolBudgetCannotExceedGatewayExecutionHardMaximum(t *testing.T) {
	capabilities := runtimeCapabilityClientTestCapabilities("capability_hard_max")
	// The Host may advertise a larger generic capacity, but the signed Gateway
	// execution contract is the per-Run enforcement ceiling.
	capabilities.BudgetCapabilities.MaxToolCallsSupported = 400
	snapshot := capabilities.PlannerSnapshot()
	budget := defaultRuntimeToolBudget()
	budget.MaxToolCalls = DefaultRuntimeToolBudgetExecutionContract().HardMaxToolCalls + 1

	client := NewRuntimeCapabilityClient(HTTPTransportOpenClawClient{})
	if err := client.ValidateRequiredTools(capabilities, []string{"read"}, budget); err == nil || err.Error() != "RUNTIME_TOOL_BUDGET_UNSUPPORTED" {
		t.Fatalf("capability boundary accepted budget beyond Gateway hard maximum: %v", err)
	}
	if err := ValidateRuntimePlanAvailability(snapshot, []string{"read"}, budget); err == nil || err.Error() != "RUNTIME_TOOL_BUDGET_UNSUPPORTED" {
		t.Fatalf("plan availability accepted budget beyond Gateway hard maximum: %v", err)
	}
	if err := ValidateRuntimePlanAvailability(snapshot, []string{"read"}, RuntimeToolBudget{}); err == nil || err.Error() != "RUNTIME_TOOL_BUDGET_UNSUPPORTED" {
		t.Fatalf("plan availability accepted zero tool budget: %v", err)
	}
	if err := ValidateRuntimeToolBudget(budget); err == nil || err.Error() != "AGENT_PLAN_INVALID" {
		t.Fatalf("signed plan accepted budget beyond Gateway hard maximum: %v", err)
	}
}

func runtimeCapabilityClientTestCapabilities(capabilityHash string) RuntimeCapabilities {
	return RuntimeCapabilities{
		CapabilityHash: capabilityHash,
		Tools:          []ToolCapability{CanonicalAgentFacingToolCapability("read", "ready")},
		FilesystemPolicy: RuntimeFilesystemPolicy{
			WorkspaceOnlyReady: true, AbsolutePathRejected: true, SymlinkEscapeRejected: true,
		},
		Abort:         RuntimeAbortCapability{Supported: true, AuthorizationReady: true},
		SubmitBinding: RuntimeSubmitBindingCapability{Version: RuntimeSubmitBindingV2, ProductSessionHash: true},
		BudgetCapabilities: RuntimeBudgetCapabilities{
			MaxToolCallsSupported: 200, DefaultMaxToolCalls: 200,
			SupportsPerRunBudget: true, SupportsBudgetWarning: true, SupportsForcedAbort: true,
			ExecutionContract: DefaultRuntimeToolBudgetExecutionContract(),
		},
	}
}
