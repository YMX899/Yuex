-- Forward-only Runtime concurrency, fencing, recovery and terminal convergence.
-- Published migrations 022/023 and Material migration 024 remain immutable.

alter table runtime_hosts add column if not exists reported_active_runs int not null default 0;
alter table runtime_hosts add column if not exists reported_reserved_runs int not null default 0;
alter table runtime_hosts add column if not exists max_product_thread_runs int not null default 1;
alter table runtime_hosts add column if not exists active_product_thread_runs int not null default 0;
alter table runtime_hosts add column if not exists reserved_product_thread_runs int not null default 0;
alter table runtime_hosts add column if not exists max_detached_task_runs int not null default 1;
alter table runtime_hosts add column if not exists active_detached_task_runs int not null default 0;
alter table runtime_hosts add column if not exists reserved_detached_task_runs int not null default 0;
alter table runtime_hosts add column if not exists instance_generation bigint not null default 1;
alter table runtime_hosts add column if not exists recovery_state text not null default 'pending';
alter table runtime_hosts add column if not exists reported_at timestamptz;
alter table runtime_hosts add constraint runtime_hosts_total_counter_bounds check (
  active_runs >= 0 and reserved_runs >= 0 and active_runs + reserved_runs <= max_active_runs
) not valid;
alter table runtime_hosts add constraint runtime_hosts_product_counter_bounds check (
  max_product_thread_runs >= 0 and active_product_thread_runs >= 0 and reserved_product_thread_runs >= 0 and
  active_product_thread_runs + reserved_product_thread_runs <= max_product_thread_runs
) not valid;
alter table runtime_hosts add constraint runtime_hosts_detached_counter_bounds check (
  max_detached_task_runs >= 0 and active_detached_task_runs >= 0 and reserved_detached_task_runs >= 0 and
  active_detached_task_runs + reserved_detached_task_runs <= max_detached_task_runs
) not valid;

alter table runtime_slot_reservations add column if not exists execution_scope text not null default 'detached_task';
alter table runtime_slot_reservations add column if not exists execution_scope_source text not null default 'legacy_unclassified';
alter table runtime_slot_reservations add column if not exists last_renewed_at timestamptz;
alter table runtime_slot_reservations add column if not exists version bigint not null default 1;
alter table runtime_slot_reservations add column if not exists dispatch_id text;
alter table runtime_slot_reservations drop constraint if exists runtime_slot_reservations_state_check;
update runtime_slot_reservations
set state='released', released_at=coalesce(released_at,updated_at), release_reason=coalesce(release_reason,'legacy_aborted')
where state='aborted';
alter table runtime_slot_reservations add constraint runtime_slot_reservations_state_check
  check (state in ('reserved','accepted','running','released','expired'));
alter table runtime_slot_reservations add constraint runtime_slot_reservation_scope_check
  check (execution_scope in ('product_thread','detached_task')) not valid;

update runtime_slot_reservations r
set execution_scope=case when a.thread_id is null then 'detached_task' else 'product_thread' end,
    execution_scope_source='agent_run_backfill'
from agent_runs a
where r.run_id=a.agent_run_id
  and r.execution_scope_source='legacy_unclassified';
alter table runtime_slot_reservations alter column execution_scope_source set default 'explicit';

-- Existing reservations predate execution_scope. Preserve total occupancy but
-- keep Hosts with any active legacy reservation out of admission until the
-- reconciler classifies their session scope and verifies the counters.
with reservation_counts as (
  select runtime_host_id,
         count(*) filter (where state='reserved')::int as reserved_count,
         count(*) filter (where state in ('accepted','running'))::int as active_count,
         count(*) filter (where state='reserved' and execution_scope='product_thread')::int as reserved_product_count,
         count(*) filter (where state in ('accepted','running') and execution_scope='product_thread')::int as active_product_count,
         count(*) filter (where state='reserved' and execution_scope='detached_task')::int as reserved_detached_count,
         count(*) filter (where state in ('accepted','running') and execution_scope='detached_task')::int as active_detached_count,
         count(*) filter (where execution_scope_source='legacy_unclassified')::int as unclassified_count
  from runtime_slot_reservations
  where state in ('reserved','accepted','running')
  group by runtime_host_id
)
update runtime_hosts h
set active_runs=coalesce(c.active_count,0),
    reserved_runs=coalesce(c.reserved_count,0),
    max_active_runs=greatest(h.max_active_runs,coalesce(c.active_count,0)+coalesce(c.reserved_count,0)),
    max_product_thread_runs=greatest(h.max_product_thread_runs,coalesce(c.active_product_count,0)+coalesce(c.reserved_product_count,0)),
    active_product_thread_runs=coalesce(c.active_product_count,0),
    reserved_product_thread_runs=coalesce(c.reserved_product_count,0),
    max_detached_task_runs=greatest(h.max_detached_task_runs,coalesce(c.active_detached_count,0)+coalesce(c.reserved_detached_count,0)),
    active_detached_task_runs=coalesce(c.active_detached_count,0),
    reserved_detached_task_runs=coalesce(c.reserved_detached_count,0),
    recovery_state=case
      when coalesce(c.unclassified_count,0)=0 then 'reconciled'
      else 'pending'
    end,
    updated_at=now()
from (select h2.runtime_host_id,
             coalesce(rc.active_count,0) as active_count,
             coalesce(rc.reserved_count,0) as reserved_count,
             coalesce(rc.active_product_count,0) as active_product_count,
             coalesce(rc.reserved_product_count,0) as reserved_product_count,
             coalesce(rc.active_detached_count,0) as active_detached_count,
             coalesce(rc.reserved_detached_count,0) as reserved_detached_count,
             coalesce(rc.unclassified_count,0) as unclassified_count
      from runtime_hosts h2
      left join reservation_counts rc on rc.runtime_host_id=h2.runtime_host_id) c
where c.runtime_host_id=h.runtime_host_id;

alter table runtime_run_dispatches drop constraint if exists runtime_run_dispatches_state_check;
alter table runtime_run_dispatches add constraint runtime_run_dispatches_state_check check (state in (
  'created','sent','submit_unknown','accepted','materializing','running','finalizing','recovering',
  'terminal','succeeded','failed','timeout','aborted','rejected','orphaned'
));
alter table runtime_run_dispatches add column if not exists owner_instance_id text;
alter table runtime_run_dispatches add column if not exists lease_token_hash text;
alter table runtime_run_dispatches add column if not exists lease_expires_at timestamptz;
alter table runtime_run_dispatches add column if not exists recovery_owner_instance_id text;
alter table runtime_run_dispatches add column if not exists recovery_fencing_token bigint;
alter table runtime_run_dispatches add column if not exists recovery_expires_at timestamptz;
alter table runtime_run_dispatches add column if not exists recovery_attempt int not null default 0;
alter table runtime_run_dispatches add column if not exists next_recovery_check_at timestamptz;
alter table runtime_run_dispatches add column if not exists event_cursor bigint not null default 0;
alter table runtime_run_dispatches add column if not exists event_lower_bound bigint not null default 1;
alter table runtime_run_dispatches add column if not exists event_upper_bound bigint;
alter table runtime_run_dispatches add column if not exists event_gap_detected_at timestamptz;
alter table runtime_run_dispatches add column if not exists event_gap_expected_sequence bigint;
alter table runtime_run_dispatches add column if not exists event_gap_observed_sequence bigint;
alter table runtime_run_dispatches add column if not exists version bigint not null default 1;
drop index if exists uq_runtime_dispatch_active_run;
create unique index if not exists uq_runtime_dispatch_active_run
  on runtime_run_dispatches(run_id)
  where state in ('created','sent','submit_unknown','accepted','materializing','running','finalizing','recovering');

alter table runtime_run_events add column if not exists payload_hash text;
update runtime_run_events
set payload_hash = md5(event_type || ':' || safe_payload::text || ':' || usage_delta::text)
where payload_hash is null;
alter table runtime_run_events alter column payload_hash set not null;

alter table task_queue_records add column if not exists lease_token_hash text;
alter table task_queue_records add column if not exists lease_fencing_token bigint not null default 0;
alter table task_queue_records add column if not exists heartbeat_at timestamptz;
alter table task_queue_records add column if not exists attempt_series_id text;
alter table task_queue_records add column if not exists dead_lettered_at timestamptz;
alter table task_queue_records add column if not exists dead_letter_reason text;
alter table task_queue_records add column if not exists replayed_from_queue_id text references task_queue_records(queue_id);
alter table task_queue_records add column if not exists replayed_by text;
alter table task_queue_records add column if not exists replay_reason text;
update task_queue_records set attempt_series_id='qas_'||md5(queue_id||':'||created_at::text) where attempt_series_id is null;
alter table task_queue_records alter column attempt_series_id set not null;
alter table task_queue_records drop constraint if exists task_queue_records_status_check;
alter table task_queue_records add constraint task_queue_records_status_check check (status in ('pending','leased','running','retry_wait','succeeded','failed','timeout','dead_letter','ignored'));
update task_queue_records set max_attempts=greatest(max_attempts,attempt,1) where max_attempts<attempt or max_attempts<1;
alter table task_queue_records add constraint task_queue_attempt_nonnegative_check check (attempt >= 0) not valid;
alter table task_queue_records add constraint task_queue_max_attempts_positive_check check (max_attempts > 0) not valid;
alter table task_queue_records add constraint task_queue_attempt_bound_check check (attempt <= max_attempts) not valid;
drop index if exists uq_task_queue_active_dedupe;
create unique index if not exists uq_task_queue_active_dedupe
  on task_queue_records(dedupe_key)
  where dedupe_key is not null and status in ('pending','leased','running','retry_wait');

create table if not exists runtime_session_admissions (
  admission_id text primary key,
  tenant_id text not null,
  thread_id text not null,
  agent_profile text not null,
  context_generation bigint not null,
  session_generation int not null,
  binding_id text not null,
  run_id text not null,
  owner_instance_id text not null,
  lease_token_hash text not null,
  fencing_token bigint not null,
  state text not null check (state in ('acquired','reservation_bound','dispatch_bound','recovering','released','expired')),
  reservation_id text references runtime_slot_reservations(reservation_id),
  dispatch_id text references runtime_run_dispatches(dispatch_id),
  expires_at timestamptz not null,
  last_renewed_at timestamptz not null,
  release_reason text,
  version bigint not null default 1,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);
create unique index if not exists uq_runtime_session_admission_active
  on runtime_session_admissions(tenant_id,thread_id,agent_profile,context_generation,session_generation)
  where state in ('acquired','reservation_bound','dispatch_bound','recovering');
create index if not exists idx_runtime_session_admission_recovery
  on runtime_session_admissions(state,expires_at,updated_at);

create table if not exists runtime_capacity_reservations (
  capacity_reservation_id text primary key,
  run_id text not null,
  snapshot_version bigint not null,
  state text not null check (state in ('reserved','accepted','released','expired','recovering')),
  model_capacity_key text not null,
  auth_pool_capacity_key text not null,
  tool_capacity_key text not null,
  tenant_capacity_key text not null,
  user_capacity_key text not null,
  model_units int not null check (model_units > 0),
  auth_pool_units int not null check (auth_pool_units > 0),
  tool_units int not null check (tool_units > 0),
  tenant_units int not null check (tenant_units > 0),
  user_units int not null check (user_units > 0),
  dimension_snapshot jsonb not null default '{}'::jsonb,
  actual_usage jsonb not null default '{}'::jsonb,
  expires_at timestamptz not null,
  accepted_at timestamptz,
  released_at timestamptz,
  release_reason text,
  version bigint not null default 1,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);
create unique index if not exists uq_runtime_capacity_active_run
  on runtime_capacity_reservations(run_id) where state in ('reserved','accepted','recovering');
create index if not exists idx_runtime_capacity_recovery
  on runtime_capacity_reservations(state,expires_at,updated_at);

create table if not exists runtime_terminal_convergences (
  convergence_id text primary key,
  dispatch_id text not null references runtime_run_dispatches(dispatch_id),
  terminal_source_sequence bigint not null,
  terminal_status text not null,
  events_verified_at timestamptz,
  product_projected_at timestamptz,
  usage_settled_at timestamptz,
  agent_run_converged_at timestamptz,
  dispatch_finalized_at timestamptz,
  session_released_at timestamptz,
  public_event_appended_at timestamptz,
  queue_completed_at timestamptz,
  last_error_code text,
  attempt_count int not null default 0,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique(dispatch_id, terminal_source_sequence)
);
create index if not exists idx_runtime_terminal_convergence_incomplete
  on runtime_terminal_convergences(updated_at)
  where queue_completed_at is null;

create table if not exists task_queue_dead_letter_audit (
  audit_id text primary key,
  queue_id text not null references task_queue_records(queue_id),
  attempt_series_id text not null,
  attempt int not null,
  lease_owner text not null,
  lease_token_hash text not null,
  lease_fencing_token bigint not null,
  reason text not null,
  safe_error_summary jsonb not null default '{}'::jsonb,
  payload_snapshot jsonb not null default '{}'::jsonb,
  replayed_from_queue_id text,
  replayed_by text,
  replay_reason text,
  created_at timestamptz not null default now()
);
create index if not exists idx_task_queue_dead_letter_audit_queue
  on task_queue_dead_letter_audit(queue_id,created_at desc);

create index if not exists idx_runtime_hosts_recovery
  on runtime_hosts(status,recovery_state,last_heartbeat_at);
create index if not exists idx_runtime_dispatch_recovery_due
  on runtime_run_dispatches(state,next_recovery_check_at,updated_at);
create index if not exists idx_runtime_reservation_scope_recovery
  on runtime_slot_reservations(execution_scope,state,expires_at);
create index if not exists idx_task_queue_dead_letter
  on task_queue_records(status,dead_lettered_at) where status='dead_letter';
