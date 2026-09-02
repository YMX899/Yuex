package services

import (
	"context"
	"strings"

	"github.com/YMX899/Yuex/examples/backend/domain"
	"github.com/YMX899/Yuex/examples/backend/persistence"
)

type ProductAgentRunCommand struct {
	TenantID       string
	UserID         string
	WorkspaceID    string
	ThreadID       string
	InputText      string
	Attachments    []domain.AgentRunAttachment
	IdempotencyKey string
}

// Create converts an authorized product message into exactly one durable Run.
// A real service creates the message, Run and outbox item in one transaction.
func (b ProductAgentRunBridge) Create(ctx context.Context, command ProductAgentRunCommand) (domain.AgentRun, error) {
	workspace, err := b.Workspaces.GetOwned(ctx, command.TenantID, command.UserID, command.WorkspaceID)
	if err != nil {
		return domain.AgentRun{}, err
	}
	if strings.TrimSpace(command.InputText) == "" {
		return domain.AgentRun{}, ErrInvalidArgument
	}

	binding, err := b.Threads.ResolveWorkspace(ctx, command.ThreadID, workspace.ID)
	if err != nil {
		return domain.AgentRun{}, err
	}

	message, err := b.Messages.AppendUser(ctx, command.ThreadID, command.UserID, command.InputText)
	if err != nil {
		return domain.AgentRun{}, err
	}

	run := domain.AgentRun{
		RunID:             b.IDs.FromKey("run", command.UserID+":"+command.IdempotencyKey),
		TenantID:          command.TenantID,
		UserID:            command.UserID,
		WorkspaceID:       binding.WorkspaceID,
		ThreadID:          command.ThreadID,
		IdempotencyKey:    command.IdempotencyKey,
		Status:            "planning",
		WorkspaceVersion:  workspace.Version,
		ContextGeneration: binding.ContextGeneration,
	}

	created, err := b.Runs.CreateOnce(ctx, run, message.ID, command.Attachments)
	if err != nil {
		return domain.AgentRun{}, err
	}
	if err := b.Outbox.EnqueueRun(ctx, created.RunID); err != nil {
		return domain.AgentRun{}, err
	}
	return created, nil
}

type ProductAgentRunBridge struct {
	Workspaces persistence.WorkspaceRepository
	Threads    persistence.ThreadRepository
	Messages   persistence.MessageRepository
	Runs       persistence.RunRepository
	Outbox     persistence.RunOutbox
	IDs        persistence.IDFactory
}

var ErrInvalidArgument = persistence.ErrInvalidArgument
