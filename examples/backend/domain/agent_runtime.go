package domain

// AuthContext is derived by Backend authentication. The client does not get
// to choose TenantID or UserID.
type AuthContext struct {
	TenantID    string `json:"tenantId"`
	UserID      string `json:"userId"`
	WorkspaceID string `json:"workspaceId"`
}

type AgentRunRequest struct {
	Input       AgentRunInput        `json:"input"`
	ThreadID    string               `json:"threadId"`
	WorkspaceID string               `json:"workspaceId"`
	Attachments []AgentRunAttachment `json:"attachments,omitempty"`
}

type AgentRunInput struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type AgentRunAttachment struct {
	ResourceID string `json:"resourceId"`
	Usage      string `json:"usage,omitempty"`
}

// AgentRun is the Backend's durable link between a product request and one
// Runtime execution. RunID is also used for status, event, cancel and retry
// correlation.
type AgentRun struct {
	RunID             string
	TenantID          string
	UserID            string
	WorkspaceID       string
	ThreadID          string
	IdempotencyKey    string
	Status            string
	WorkspaceVersion  int64
	ContextGeneration int
	RuntimeCursor     int64
	PublicResult      map[string]any
	ErrorSummary      map[string]any
}

type RuntimeEvent struct {
	RunID       string
	Sequence    int64
	Type        string
	Status      string
	SafePayload map[string]any
}

// RawUsage is metering evidence. Prices and credits remain Backend policy.
type RawUsage struct {
	RunID        string
	InputTokens  int64
	OutputTokens int64
	ToolCalls    int64
	DurationMS   int64
}
