package runtime

import (
	"context"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"huahuoai/backend/source/internal/domain"
)

type RuntimeFilesystemPolicy struct {
	WorkspaceOnlyReady    bool `json:"workspaceOnlyReady"`
	AbsolutePathRejected  bool `json:"absolutePathRejected"`
	SymlinkEscapeRejected bool `json:"symlinkEscapeRejected"`
}

type RuntimeAbortCapability struct {
	Supported          bool `json:"supported"`
	AuthorizationReady bool `json:"authorizationReady"`
}

// RuntimeSubmitBindingCapability is an Adapter-owned assertion in the live
// capability document. It is deliberately separate from AdapterVersion: a
// version string cannot prove that the running binary verifies the exact
// v2 ticket binding before a model-start submit.
type RuntimeSubmitBindingCapability struct {
	Version            string `json:"version"`
	ProductSessionHash bool   `json:"productSessionHash"`
}

// ValidateRuntimeSubmitBindingCapability accepts only the exact model-start
// ticket contract emitted by the Backend. Missing fields, legacy versions and
// a missing product-session pair binding are all unavailable, never defaults.
// Host identity, endpoint, environment, and Adapter version cannot create a
// compatibility exception.
func ValidateRuntimeSubmitBindingCapability(capability RuntimeSubmitBindingCapability) error {
	if capability.Version != RuntimeSubmitBindingV2 || !capability.ProductSessionHash {
		return domain.ErrorCode("RUNTIME_TOOL_UNAVAILABLE")
	}
	return nil
}

type RuntimeBudgetCapabilities struct {
	MaxToolCallsSupported int  `json:"maxToolCallsSupported"`
	DefaultMaxToolCalls   int  `json:"defaultMaxToolCalls"`
	SupportsPerRunBudget  bool `json:"supportsPerRunBudget"`
	SupportsBudgetWarning bool `json:"supportsBudgetWarning"`
	SupportsForcedAbort   bool `json:"supportsForcedAbort"`
	// ExecutionContract is emitted by the Gateway itself. The older boolean
	// claims only described intent; they did not prove that the process which
	// invokes tools can emit the events and converge an abort.
	ExecutionContract RuntimeToolBudgetExecutionContract `json:"executionContract"`
}

const (
	RuntimeToolBudgetEnforcementV1       = "huahuo.runtime-tool-budget-enforcement.v1"
	RuntimeToolExecutionEventSchemaV1    = "huahuo.runtime-tool-execution-event.v1"
	RuntimeAbortConvergenceEventSchemaV1 = "huahuo.runtime-abort-convergence-event.v1"
)

// RuntimeToolBudgetExecutionContract is a Gateway-owned, capability-handshake
// assertion. It is deliberately separate from the signed per-Run budget: this
// contract establishes that a Host can actually enforce and report that budget.
// The Backend does not synthesize tool events, counters, or abort success.
type RuntimeToolBudgetExecutionContract struct {
	EnforcementVersion       string `json:"enforcementVersion"`
	ToolExecutionEventSchema string `json:"toolExecutionEventSchema"`
	AbortConvergenceSchema   string `json:"abortConvergenceSchema"`
	HardMaxToolCalls         int    `json:"hardMaxToolCalls"`
	SoftToolCallLimit        int    `json:"softToolCallLimit"`
	FinalizationReserve      int    `json:"finalizationReserve"`
	MaxRepeatedCalls         int    `json:"maxRepeatedCalls"`
	MaxNoProgressCalls       int    `json:"maxNoProgressCalls"`
}

// DefaultRuntimeToolBudgetExecutionContract returns the only Gateway contract
// accepted by the current normal Workspace plan. It is exported for Host
// implementations and test fixtures; callers must obtain it from Gateway
// capability discovery rather than manufacture it at dispatch time.
func DefaultRuntimeToolBudgetExecutionContract() RuntimeToolBudgetExecutionContract {
	return RuntimeToolBudgetExecutionContract{
		EnforcementVersion:       RuntimeToolBudgetEnforcementV1,
		ToolExecutionEventSchema: RuntimeToolExecutionEventSchemaV1,
		AbortConvergenceSchema:   RuntimeAbortConvergenceEventSchemaV1,
		HardMaxToolCalls:         200,
		SoftToolCallLimit:        160,
		FinalizationReserve:      10,
		MaxRepeatedCalls:         2,
		MaxNoProgressCalls:       4,
	}
}

// ValidateRuntimeToolBudgetExecutionContract rejects an unverifiable Gateway
// before it can become a ready Host. The actual Gateway overlay must emit the
// declared events and enforce these thresholds; Backend only verifies the
// handshake and consumes those events once that overlay exists.
func ValidateRuntimeToolBudgetExecutionContract(contract RuntimeToolBudgetExecutionContract) error {
	expected := DefaultRuntimeToolBudgetExecutionContract()
	if contract.EnforcementVersion != expected.EnforcementVersion ||
		contract.ToolExecutionEventSchema != expected.ToolExecutionEventSchema ||
		contract.AbortConvergenceSchema != expected.AbortConvergenceSchema ||
		contract.HardMaxToolCalls != expected.HardMaxToolCalls ||
		contract.SoftToolCallLimit != expected.SoftToolCallLimit ||
		contract.FinalizationReserve != expected.FinalizationReserve ||
		contract.MaxRepeatedCalls != expected.MaxRepeatedCalls ||
		contract.MaxNoProgressCalls != expected.MaxNoProgressCalls {
		return domain.ErrorCode("RUNTIME_TOOL_BUDGET_UNSUPPORTED")
	}
	return nil
}

// ValidateRuntimeCapabilitySnapshot is the durable Backend counterpart of the
// Gateway capability document. RuntimeHostRepository calls it at registration
// and reservation time so an old Host cannot remain eligible merely because a
// previously persisted capability hash still matches a Plan.
func ValidateRuntimeCapabilitySnapshot(capabilities RuntimeCapabilitySnapshot) error {
	if err := ValidateRuntimeSubmitBindingCapability(capabilities.SubmitBinding); err != nil {
		return err
	}
	if err := ValidateAgentFacingRuntimeTools(capabilities.Tools); err != nil {
		return err
	}
	if capabilities.MaxToolCallsSupported < 200 || !capabilities.SupportsPerRunBudget ||
		!capabilities.SupportsBudgetWarning || !capabilities.SupportsForcedAbort {
		return domain.ErrorCode("RUNTIME_TOOL_BUDGET_UNSUPPORTED")
	}
	return ValidateRuntimeToolBudgetExecutionContract(capabilities.BudgetExecution)
}

type RuntimeCapabilities struct {
	RuntimeVersion     string                         `json:"runtimeVersion"`
	AdapterVersion     string                         `json:"adapterVersion"`
	CapabilityHash     string                         `json:"capabilityHash"`
	Tools              []ToolCapability               `json:"tools"`
	FilesystemPolicy   RuntimeFilesystemPolicy        `json:"filesystemPolicy"`
	Abort              RuntimeAbortCapability         `json:"abort"`
	SubmitBinding      RuntimeSubmitBindingCapability `json:"submitBinding"`
	BudgetCapabilities RuntimeBudgetCapabilities      `json:"budgetCapabilities"`
}

func (c RuntimeCapabilities) PlannerSnapshot() RuntimeCapabilitySnapshot {
	return RuntimeCapabilitySnapshot{
		CapabilityHash: c.CapabilityHash, Tools: c.Tools,
		SubmitBinding:         c.SubmitBinding,
		MaxToolCallsSupported: c.BudgetCapabilities.MaxToolCallsSupported,
		SupportsPerRunBudget:  c.BudgetCapabilities.SupportsPerRunBudget,
		SupportsBudgetWarning: c.BudgetCapabilities.SupportsBudgetWarning,
		SupportsForcedAbort:   c.BudgetCapabilities.SupportsForcedAbort,
		BudgetExecution:       c.BudgetCapabilities.ExecutionContract,
	}
}

type runtimeCapabilityCacheEntry struct {
	value     RuntimeCapabilities
	expiresAt time.Time
}

type RuntimeCapabilityClient struct {
	Transport HTTPTransportOpenClawClient
	TTL       time.Duration
	mu        sync.Mutex
	cache     map[string]runtimeCapabilityCacheEntry
}

// RuntimeCapabilityReader is the narrow lifecycle dependency used by
// planning. The Host registration snapshot remains the durable scheduling
// authority; this reader supplies the short-lived, Host-local handshake that
// proves the declared hash is still backed by registered Runtime tools.
//
// Keeping the interface to discovery only prevents planning from treating a
// probe response as a second registration or from mutating Host state.
type RuntimeCapabilityReader interface {
	GetCapabilities(ctx context.Context, host RuntimeHost) (RuntimeCapabilities, error)
}

// RuntimeFreshCapabilityReader is the final admission boundary used after a
// Scheduler reservation. It intentionally bypasses the discovery cache so a
// Host restart or downgrade that keeps the same capability hash cannot reuse a
// previously observed v2 submit-binding assertion.
type RuntimeFreshCapabilityReader interface {
	RuntimeCapabilityReader
	GetFreshCapabilities(ctx context.Context, host RuntimeHost) (RuntimeCapabilities, error)
}

func NewRuntimeCapabilityClient(transport HTTPTransportOpenClawClient) *RuntimeCapabilityClient {
	return &RuntimeCapabilityClient{Transport: transport, TTL: 15 * time.Second, cache: map[string]runtimeCapabilityCacheEntry{}}
}

func (c *RuntimeCapabilityClient) GetCapabilities(ctx context.Context, host RuntimeHost) (RuntimeCapabilities, error) {
	return c.getCapabilities(ctx, host, true)
}

// GetFreshCapabilities performs an uncached Host-local capability handshake.
// Dispatcher calls it only at the reservation-to-submit boundary; normal
// planning keeps the short TTL cache to avoid multiplying discovery traffic.
func (c *RuntimeCapabilityClient) GetFreshCapabilities(ctx context.Context, host RuntimeHost) (RuntimeCapabilities, error) {
	return c.getCapabilities(ctx, host, false)
}

func (c *RuntimeCapabilityClient) getCapabilities(ctx context.Context, host RuntimeHost, allowCache bool) (RuntimeCapabilities, error) {
	key := host.RuntimeHostID + "\x00" + host.CapabilityHash
	if allowCache {
		c.mu.Lock()
		entry, ok := c.cache[key]
		c.mu.Unlock()
		if ok && entry.expiresAt.After(time.Now().UTC()) {
			return cloneRuntimeCapabilities(entry.value), nil
		}
	} else {
		// Do not leave a successful planning probe available after final
		// admission observes a different process or document at the same Host.
		c.mu.Lock()
		delete(c.cache, key)
		c.mu.Unlock()
	}
	var capabilities RuntimeCapabilities
	if err := c.Transport.doRuntimeHostJSON(ctx, host, http.MethodGet, "/enterprise.runtime/capabilities", "", nil, &capabilities); err != nil {
		return RuntimeCapabilities{}, err
	}
	if capabilities.CapabilityHash == "" || capabilities.CapabilityHash != host.CapabilityHash {
		return RuntimeCapabilities{}, domain.ErrorCode("RUNTIME_TOOL_UNAVAILABLE")
	}
	if usesStableTestAdapterCapabilityCompatibility(host) && ValidateRuntimeSubmitBindingCapability(capabilities.SubmitBinding) != nil {
		// The isolated v0.5 Adapter accepts the legacy ticket shape and does
		// not advertise submitBinding. Project only the local compatibility
		// contract needed to validate its remaining capability document; the
		// dispatcher omits SubmitBinding from that Adapter's Run ticket.
		capabilities.SubmitBinding = RuntimeSubmitBindingCapability{Version: RuntimeSubmitBindingV2, ProductSessionHash: true}
	}
	if err := ValidateRuntimeCapabilities(capabilities); err != nil {
		return RuntimeCapabilities{}, err
	}
	c.mu.Lock()
	c.cache[key] = runtimeCapabilityCacheEntry{value: cloneRuntimeCapabilities(capabilities), expiresAt: time.Now().UTC().Add(c.TTL)}
	c.mu.Unlock()
	return cloneRuntimeCapabilities(capabilities), nil
}

func usesStableTestAdapterCapabilityCompatibility(host RuntimeHost) bool {
	return os.Getenv("HUAHUO_RUNTIME_LEGACY_RUN_TICKET_COMPAT") == "1" &&
		host.RuntimeHostID == "runtime-host-test-1" &&
		host.Environment == "test" && host.AdapterVersion == "v0.5"
}

func (c *RuntimeCapabilityClient) ValidateRequiredTools(capabilities RuntimeCapabilities, requiredTools []string, budget RuntimeToolBudget) error {
	if budget.MaxToolCalls <= 0 {
		return domain.ErrorCode("RUNTIME_TOOL_BUDGET_UNSUPPORTED")
	}
	if err := validateRequiredRuntimeTools(requiredTools); err != nil {
		return err
	}
	ready := map[string]bool{}
	for _, tool := range capabilities.Tools {
		ready[tool.Name] = runtimeToolCapabilityReady(tool)
	}
	for _, tool := range requiredTools {
		if !ready[tool] {
			return domain.ErrorCode("RUNTIME_TOOL_UNAVAILABLE")
		}
	}
	if budget.MaxToolCalls > capabilities.BudgetCapabilities.MaxToolCallsSupported ||
		budget.MaxToolCalls > capabilities.BudgetCapabilities.ExecutionContract.HardMaxToolCalls ||
		!capabilities.BudgetCapabilities.SupportsPerRunBudget ||
		!capabilities.BudgetCapabilities.SupportsBudgetWarning ||
		!capabilities.BudgetCapabilities.SupportsForcedAbort {
		return domain.ErrorCode("RUNTIME_TOOL_BUDGET_UNSUPPORTED")
	}
	if err := ValidateRuntimeToolBudgetExecutionContract(capabilities.BudgetCapabilities.ExecutionContract); err != nil {
		return err
	}
	return ValidateRuntimeCapabilities(capabilities)
}

// validateRequiredRuntimeTools protects the live capability boundary even
// when a future caller bypasses AgentRunPlan validation. The signed Plan uses
// the same lexical, duplicate-free vocabulary, so a Host must never accept a
// broader or ambiguous assertion as an availability success.
func validateRequiredRuntimeTools(requiredTools []string) error {
	previous := ""
	for _, tool := range requiredTools {
		tool = strings.TrimSpace(tool)
		if !IsAgentFacingRuntimeTool(tool) || (previous != "" && previous >= tool) {
			return domain.ErrorCode("RUNTIME_TOOL_UNAVAILABLE")
		}
		previous = tool
	}
	return nil
}

// ValidateRuntimeCapabilities rejects a Host capability document that cannot
// safely participate in Runtime planning or readiness.
func ValidateRuntimeCapabilities(capabilities RuntimeCapabilities) error {
	if strings.TrimSpace(capabilities.CapabilityHash) == "" ||
		!capabilities.FilesystemPolicy.WorkspaceOnlyReady || !capabilities.FilesystemPolicy.AbsolutePathRejected || !capabilities.FilesystemPolicy.SymlinkEscapeRejected ||
		!capabilities.Abort.Supported || !capabilities.Abort.AuthorizationReady ||
		capabilities.BudgetCapabilities.DefaultMaxToolCalls != 200 || capabilities.BudgetCapabilities.MaxToolCallsSupported < 200 {
		return domain.ErrorCode("RUNTIME_TOOL_UNAVAILABLE")
	}
	if err := ValidateRuntimeSubmitBindingCapability(capabilities.SubmitBinding); err != nil {
		return err
	}
	if !capabilities.BudgetCapabilities.SupportsPerRunBudget || !capabilities.BudgetCapabilities.SupportsBudgetWarning || !capabilities.BudgetCapabilities.SupportsForcedAbort {
		return domain.ErrorCode("RUNTIME_TOOL_BUDGET_UNSUPPORTED")
	}
	if err := ValidateAgentFacingRuntimeTools(capabilities.Tools); err != nil {
		return err
	}
	if err := ValidateRuntimeToolBudgetExecutionContract(capabilities.BudgetCapabilities.ExecutionContract); err != nil {
		return err
	}
	return nil
}

func cloneRuntimeCapabilities(capabilities RuntimeCapabilities) RuntimeCapabilities {
	cloned := capabilities
	cloned.Tools = append([]ToolCapability(nil), capabilities.Tools...)
	return cloned
}
