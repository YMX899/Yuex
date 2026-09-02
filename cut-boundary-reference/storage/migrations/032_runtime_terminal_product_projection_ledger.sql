-- Durable Product-side idempotency for Runtime terminal convergence.
-- Runtime event ownership and Product writeback use different worker pools, so
-- the ledger is the replay boundary when a process dies after a Product effect
-- commits but before runtime_terminal_convergences.product_projected_at is set.

create table if not exists runtime_terminal_product_projections (
  convergence_id text primary key references runtime_terminal_convergences(convergence_id),
  run_id text not null references agent_runs(agent_run_id),
  task_id text,
  projection_key text not null unique,
  snapshot_hash text not null,
  snapshot jsonb not null,
  state text not null check (state in ('pending','applying','completed')),
  fencing_token bigint not null default 0 check (fencing_token >= 0),
  lease_owner text,
  lease_expires_at timestamptz,
  last_error_code text,
  completed_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  check (
    (state = 'completed' and completed_at is not null and lease_owner is null and lease_expires_at is null) or
    (state = 'applying' and lease_owner is not null and lease_expires_at is not null) or
    (state = 'pending' and lease_owner is null and lease_expires_at is null)
  )
);

create index if not exists idx_runtime_terminal_product_projection_recovery
  on runtime_terminal_product_projections(state, lease_expires_at, updated_at)
  where state <> 'completed';

create index if not exists idx_runtime_terminal_product_projection_run
  on runtime_terminal_product_projections(run_id, created_at desc);
