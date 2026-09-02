package runtime

// RuntimeRunCommand is the backend-owned task context shared by prompt,
// workspace rendering, output parsing and formal write-intent mapping. Runtime
// execution itself is owned by AgentRun planning/dispatch/event workers.
type RuntimeRunCommand struct {
	TaskID             string               `json:"taskId"`
	RunID              string               `json:"runId"`
	TaskType           string               `json:"taskType"`
	Attempt            int                  `json:"attempt"`
	TenantID           string               `json:"tenantId"`
	UserID             string               `json:"userId"`
	WorkspaceID        string               `json:"workspaceId"`
	ThreadID           string               `json:"threadId,omitempty"`
	OpenClawSessionKey string               `json:"openclawSessionKey,omitempty"`
	TraceID            string               `json:"traceId"`
	Context            RuntimeContextBundle `json:"context"`
}
