package services

import (
	"context"

	"github.com/YMX899/Yuex/examples/backend/persistence"
)

// ProjectTerminal translates execution output into product data. Runtime
// events are evidence; they do not directly become trusted user content.
func (p AgentRunProductProjector) ProjectTerminal(ctx context.Context, runID string) error {
	run, err := p.Runs.Get(ctx, runID)
	if err != nil {
		return err
	}
	if !isTerminal(run.Status) {
		return nil
	}

	key := "terminal-message:" + run.RunID
	if run.Status == "succeeded" {
		answer, err := p.Outputs.Validate(run.PublicResult)
		if err != nil {
			return p.Runs.FailProjectionOnce(ctx, run.RunID, "RESULT_VALIDATION_FAILED")
		}
		return p.Messages.AppendAssistantOnce(ctx, run.ThreadID, key, answer)
	}
	return p.Messages.AppendFailureOnce(ctx, run.ThreadID, key, run.ErrorSummary)
}

func isTerminal(status string) bool {
	switch status {
	case "succeeded", "failed", "cancelled", "timeout", "aborted", "orphaned":
		return true
	default:
		return false
	}
}

type AgentRunProductProjector struct {
	Runs     persistence.RunRepository
	Messages persistence.MessageRepository
	Outputs  persistence.OutputValidator
}
