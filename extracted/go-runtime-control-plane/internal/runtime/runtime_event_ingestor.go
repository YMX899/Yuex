package runtime

import "strings"

// AgentProgressEventAppender is the narrow persistence boundary used by the
// coarse runtime progress publisher. It deliberately does not model OpenClaw
// tool/draft streams; those require a real runtime event outlet.
type AgentProgressEventAppender interface {
	AppendAgentProgressEvent(taskID, threadID, runID, messageID, eventType, visibility, title, summary, deltaText string) map[string]any
}

type RuntimeEventIngestor struct {
	Appender AgentProgressEventAppender
}

type RuntimeLifecycleEvent struct {
	TaskID    string
	ThreadID  string
	RunID     string
	MessageID string
	EventType string
	Title     string
	Summary   string
}

func NewRuntimeEventIngestor(appender AgentProgressEventAppender) RuntimeEventIngestor {
	return RuntimeEventIngestor{Appender: appender}
}

func (i RuntimeEventIngestor) PublishLifecycleEvent(event RuntimeLifecycleEvent) (map[string]any, bool) {
	event.TaskID = strings.TrimSpace(event.TaskID)
	event.EventType = normalizeRuntimeLifecycleEventType(event.EventType)
	if i.Appender == nil || event.TaskID == "" || event.EventType == "" {
		return nil, false
	}
	return i.Appender.AppendAgentProgressEvent(
		event.TaskID,
		strings.TrimSpace(event.ThreadID),
		strings.TrimSpace(event.RunID),
		strings.TrimSpace(event.MessageID),
		event.EventType,
		"app_safe",
		event.Title,
		event.Summary,
		"",
	), true
}

func (i RuntimeEventIngestor) PublishArtifactState(taskID, threadID, runID, messageID, title, summary string) (map[string]any, bool) {
	return i.PublishLifecycleEvent(RuntimeLifecycleEvent{
		TaskID:    taskID,
		ThreadID:  threadID,
		RunID:     runID,
		MessageID: messageID,
		EventType: "finalizing",
		Title:     title,
		Summary:   summary,
	})
}

func normalizeRuntimeLifecycleEventType(eventType string) string {
	switch strings.TrimSpace(eventType) {
	case "task_queued", "runtime_started", "reading_context", "finalizing", "succeeded", "failed", "timeout":
		return eventType
	default:
		return ""
	}
}
