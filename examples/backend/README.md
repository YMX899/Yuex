# Backend Example

This directory shows the minimum product Backend responsibilities around Yuex. It is a reading example, not a Go module and not intended to compile.

The snippets were copied from the production Backend flow, then reduced and renamed so the ownership and call order are visible without bringing an entire application into this repository. Authentication middleware, concrete database code, migrations, queue implementations, error mapping, retries, and recovery jobs are intentionally omitted.

```text
Frontend
  -> API route: authenticate, authorize, accept a message
  -> ProductAgentRunBridge: persist the message and create one idempotent Run
  -> Runtime: plan, queue, dispatch, execute
  -> RuntimeEventWorker: ingest ordered Runtime events
  -> AgentRunProductProjector: write the terminal assistant message/result
  -> UsageSettlement: record raw usage once and settle reserved credits once
  -> Frontend: read status/events/result through the Backend
```

The Backend owns product facts: tenants, users, permissions, threads, messages, assets, prices, memberships, credits, and invoices. Yuex owns execution facts: Run state, scheduling, leases, fencing, Runtime events, recovery, and raw usage. Keeping that boundary prevents a Runtime retry from creating a second message or charging twice.

## Files

| File | What it demonstrates | Reduced from |
| --- | --- | --- |
| `domain/agent_runtime.go` | Request, identity, Run and raw usage shapes | `backend/source/internal/domain/agent_runtime.go` |
| `api/agent_run_routes.go` | Public create/status/events/cancel endpoints | `backend/source/internal/api/routes/agent_run_routes.go` |
| `services/product_agent_run_bridge.go` | Conversation-to-Run handoff and idempotency | `backend/source/internal/services/product_agent_run_bridge.go`, `agent_run_service.go` |
| `workers/runtime_event_worker.go` | Cursor-based event ingestion and terminal detection | `backend/source/internal/workers/runtime_event_worker.go` |
| `services/agent_run_product_projector.go` | Final assistant message and product result writeback | `backend/source/internal/services/agent_run_product_projector.go` |
| `services/usage_settlement.go` | Reservation and idempotent usage settlement | `backend/source/internal/persistence/usage_repository.go` |
| `persistence/ports.go` | The storage boundary a real Backend must implement | The repositories used by the files above |

## What a real integration must add

- Authentication that derives `tenantId` and `userId` on the server.
- Authorization for the selected `workspaceId`, thread, and attachments.
- Durable transactions and unique constraints for idempotency keys.
- A queue or outbox so a committed Run is eventually submitted.
- Runtime API authentication, timeouts, retries, and cursor persistence.
- Output validation before a result becomes a product message or asset.
- Product-specific pricing, quota reservation, settlement, refunds, and invoices.
- Recovery jobs for stuck Runs, stale reservations, and partial writeback.

Do not pass database credentials, provider keys, physical Workspace paths, or Harness Session Keys to the browser or model.
