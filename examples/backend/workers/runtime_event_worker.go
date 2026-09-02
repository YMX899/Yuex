package workers

import (
	"context"

	"github.com/YMX899/Yuex/examples/backend/domain"
	"github.com/YMX899/Yuex/examples/backend/persistence"
)

// Poll imports an ordered page after the last committed cursor. Both event
// insertion and cursor advancement must be idempotent because a worker can
// crash after commit and receive the same page again.
func (w RuntimeEventWorker) Poll(ctx context.Context, run domain.AgentRun) error {
	page, err := w.Runtime.ListEvents(ctx, run.RunID, run.RuntimeCursor, 100)
	if err != nil {
		return err
	}
	for _, event := range page.Events {
		inserted, err := w.Runs.AppendEventOnce(ctx, event)
		if err != nil {
			return err
		}
		if inserted {
			w.Publish.Publish(run.RunID, event)
		}
		if err := w.Runs.AdvanceCursor(ctx, run.RunID, event.Sequence); err != nil {
			return err
		}
	}

	if page.Terminal == nil {
		return nil
	}
	// Terminal convergence is the only path that changes a Run to its final
	// state. The projector and settlement steps use their own idempotency keys.
	if err := w.Runs.ConvergeTerminalOnce(ctx, run.RunID, *page.Terminal); err != nil {
		return err
	}
	if err := w.Projector.ProjectTerminal(ctx, run.RunID); err != nil {
		return err
	}
	return w.Settlement.SettleTerminal(ctx, run.RunID)
}

type RuntimeEventPage struct {
	Events   []domain.RuntimeEvent
	Terminal *domain.RuntimeEvent
}

type RuntimeClient interface {
	ListEvents(ctx context.Context, runID string, after int64, limit int) (RuntimeEventPage, error)
}

type RuntimeEventWorker struct {
	Runtime    RuntimeClient
	Runs       persistence.RunRepository
	Publish    persistence.EventPublisher
	Projector  TerminalProjector
	Settlement TerminalSettlement
}

type TerminalProjector interface {
	ProjectTerminal(context.Context, string) error
}
type TerminalSettlement interface {
	SettleTerminal(context.Context, string) error
}
