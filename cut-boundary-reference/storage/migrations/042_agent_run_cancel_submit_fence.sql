-- Forward-only AgentRun cancel/submit fence. Existing migrations are immutable.
--
-- The AgentRun is the durable arbitration point between a user cancellation
-- and an outbound Runtime submit. A raw user reason never enters this table;
-- only an allowlisted code and SHA-256 digest are retained.

alter table agent_runs
  add column if not exists cancel_requested_at timestamptz,
  add column if not exists cancel_reason_code text,
  add column if not exists cancel_reason_hash text,
  add column if not exists submit_authorized_at timestamptz;

alter table agent_runs
  drop constraint if exists agent_runs_status_check;
alter table agent_runs
  add constraint agent_runs_status_check check (status in (
    'created','resolving_intent','planning','awaiting_confirmation','admission_pending',
    'queued','reserving','dispatched','accepted','materializing','running','finalizing',
    'aborting','succeeded','failed','cancelled','timeout','orphaned'
  ));

alter table agent_runs
  drop constraint if exists agent_runs_cancel_reason_check;
alter table agent_runs
  add constraint agent_runs_cancel_reason_check check (
    (
      cancel_requested_at is null
      and cancel_reason_code is null
      and cancel_reason_hash is null
    )
    or (
      cancel_requested_at is not null
      and cancel_reason_code in ('USER_CANCELLED','TIMEOUT','BUDGET_EXCEEDED','LEASE_LOST')
      and cancel_reason_hash ~ '^sha256:[0-9a-f]{64}$'
    )
  ) not valid;
alter table agent_runs validate constraint agent_runs_cancel_reason_check;

create index if not exists idx_agent_runs_cancel_requested
  on agent_runs(updated_at desc, agent_run_id)
  where cancel_requested_at is not null;
