package persistence

import (
	"context"
	"errors"

	"github.com/YMX899/Yuex/examples/backend/domain"
)

// These ports make the ownership boundary explicit. A real Backend implements
// them with transactions, unique constraints, an outbox, and recovery jobs.
type Workspace struct {
	ID      string
	Version int64
}
type ThreadBinding struct {
	WorkspaceID       string
	ContextGeneration int
}
type Message struct{ ID string }

type WorkspaceRepository interface {
	GetOwned(context.Context, string, string, string) (Workspace, error)
}
type ThreadRepository interface {
	ResolveWorkspace(context.Context, string, string) (ThreadBinding, error)
}
type MessageRepository interface {
	AppendUser(context.Context, string, string, string) (Message, error)
	AppendAssistantOnce(context.Context, string, string, string) error
	AppendFailureOnce(context.Context, string, string, map[string]any) error
}
type RunRepository interface {
	CreateOnce(context.Context, domain.AgentRun, string, []domain.AgentRunAttachment) (domain.AgentRun, error)
	Get(context.Context, string) (domain.AgentRun, error)
	AppendEventOnce(context.Context, domain.RuntimeEvent) (bool, error)
	AdvanceCursor(context.Context, string, int64) error
	ConvergeTerminalOnce(context.Context, string, domain.RuntimeEvent) error
	FailProjectionOnce(context.Context, string, string) error
}
type RunOutbox interface {
	EnqueueRun(context.Context, string) error
}
type IDFactory interface{ FromKey(kind, key string) string }
type EventPublisher interface {
	Publish(string, domain.RuntimeEvent)
}
type OutputValidator interface {
	Validate(map[string]any) (string, error)
}

type ReservationCommand struct {
	RunID, TenantID, UserID, WorkspaceID string
	EstimatedCredits                     int64
}
type UsageRepository interface {
	ReserveOnce(context.Context, ReservationCommand) (string, error)
	RecordRawUsageOnce(context.Context, string, domain.RawUsage) (string, error)
	SettleReservationOnce(context.Context, string, string) error
	ReleaseReservationOnce(context.Context, string, string) error
}
type RawUsageReader interface {
	GetRawUsage(context.Context, string) (domain.RawUsage, error)
}

var ErrInvalidArgument = errors.New("invalid argument")
