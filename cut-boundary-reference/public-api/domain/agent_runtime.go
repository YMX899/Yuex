package domain

type AuthContext struct {
	TenantID    string `json:"tenantId"`
	UserID      string `json:"userId"`
	WorkspaceID string `json:"workspaceId"`
}

type AgentRunInput struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type AgentRunAttachment struct {
	ResourceID string `json:"resourceId"`
	Usage      string `json:"usage,omitempty"`
	// UsageHint is accepted only as a compatibility alias and is normalized
	// before a Run hash or snapshot is created.
	UsageHint  string `json:"usageHint,omitempty"`
}

type AgentRunIntentHint struct {
	SourceSurface string `json:"sourceSurface,omitempty"`
	TaskType      string `json:"taskType,omitempty"`
	Operation     string `json:"operation,omitempty"`
}

type AgentRunRequest struct {
	AgentRunID               string               `json:"-"`
	Input                    AgentRunInput        `json:"input"`
	ThreadID                 string               `json:"threadId,omitempty"`
	WorkspaceID              string               `json:"workspaceId,omitempty"`
	ExpectedMetaWorkspaceKey string               `json:"expectedMetaWorkspaceKey,omitempty"`
	Attachments              []AgentRunAttachment `json:"attachments,omitempty"`
	IntentHint               AgentRunIntentHint   `json:"intentHint,omitempty"`
	BusinessRefs             map[string]string    `json:"businessRefs,omitempty"`
	ClientContext            map[string]string    `json:"clientContext,omitempty"`
	InternalFields           map[string]any       `json:"-"`
}

type TaskIntent struct {
	SchemaVersion         string   `json:"schemaVersion"`
	AgentRunID            string   `json:"agentRunId"`
	Category              string   `json:"category"`
	ResolvedTaskType      string   `json:"resolvedTaskType"`
	ExecutionScope        string   `json:"executionScope"`
	RiskClass             string   `json:"riskClass"`
	ExpectedOutput        string   `json:"expectedOutput"`
	CandidateL1Agents     []string `json:"candidateL1Agents"`
	RequiredCapabilities  []string `json:"requiredCapabilities"`
	Confidence            float64  `json:"confidence"`
	RequiresClarification bool     `json:"requiresClarification"`
	RequiresConfirmation  bool     `json:"requiresConfirmation"`
	ResolverVersion       string   `json:"resolverVersion"`
	SafeEvidenceSummary   []string `json:"safeEvidenceSummary"`
}
