-- Forward-only Runtime Host restart recovery facts and attestation CAS.
-- Existing active rows cannot be backfilled by guessing a Host process identity.

alter table runtime_hosts
  add column if not exists recovery_revision bigint not null default 1;

alter table runtime_slot_reservations
  add column if not exists assigned_runtime_host_instance_id text;
alter table runtime_slot_reservations
  add column if not exists assigned_runtime_host_instance_generation bigint;

alter table runtime_run_dispatches
  add column if not exists assigned_runtime_host_instance_id text;
alter table runtime_run_dispatches
  add column if not exists assigned_runtime_host_instance_generation bigint;
alter table runtime_run_dispatches
  add column if not exists dispatch_identity text;

-- A missing assignment on a legacy active row is intentionally not repaired
-- here. Recovery v1 rejects it and keeps the Host closed instead of inventing
-- a process binding from Scheduler ownership or zero counters.
create table if not exists runtime_host_recovery_attestations (
  attestation_id text primary key,
  runtime_host_id text not null references runtime_hosts(runtime_host_id),
  instance_id text not null,
  instance_generation bigint not null,
  recovery_revision bigint not null,
  fact_set_hash text not null,
  state text not null check (state in ('prepared','completed')),
  correlation_id text not null default '',
  created_at timestamptz not null default now(),
  completed_at timestamptz,
  unique(runtime_host_id, instance_generation, recovery_revision, fact_set_hash)
);

create index if not exists idx_runtime_host_recovery_attestations_host_state
  on runtime_host_recovery_attestations(runtime_host_id, instance_generation, recovery_revision, state, created_at desc);
create index if not exists idx_runtime_reservations_host_assignment_recovery
  on runtime_slot_reservations(runtime_host_id, assigned_runtime_host_instance_generation, state);
create index if not exists idx_runtime_dispatches_host_assignment_recovery
  on runtime_run_dispatches(runtime_host_id, assigned_runtime_host_instance_generation, state);
