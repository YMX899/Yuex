-- RuntimeHost/Slot admission, durable async dispatch, sequenced events and fencing.

create sequence if not exists runtime_fencing_token_seq;

create table if not exists runtime_hosts (
  runtime_host_id text primary key,
  instance_id text not null unique,
  environment text not null,
  endpoint text not null,
  zone text not null default '',
  status text not null check (status in ('registering','ready','draining','unhealthy','offline')),
  runtime_version text not null,
  adapter_version text not null,
  capability_hash text not null,
  capability_snapshot jsonb not null default '{}'::jsonb,
  session_store_id text not null,
  max_active_runs int not null check (max_active_runs > 0),
  active_runs int not null default 0 check (active_runs >= 0),
  reserved_runs int not null default 0 check (reserved_runs >= 0),
  registration_revision bigint not null default 1,
  heartbeat_sequence bigint not null default 0,
  last_heartbeat_at timestamptz,
  drain_deadline_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);
create index if not exists idx_runtime_hosts_admission
  on runtime_hosts(status, capability_hash, zone, active_runs, reserved_runs, last_heartbeat_at);

create table if not exists runtime_host_heartbeats (
  heartbeat_id text primary key,
  runtime_host_id text not null references runtime_hosts(runtime_host_id),
  instance_id text not null,
  sequence bigint not null,
  observed_at timestamptz not null,
  active_runs int not null,
  reserved_runs int not null,
  free_slots int not null,
  capability_hash text not null,
  safe_health jsonb not null default '{}'::jsonb,
  signature_key_id text not null,
  created_at timestamptz not null default now(),
  unique(runtime_host_id, sequence)
);
create index if not exists idx_runtime_host_heartbeats_host_time
  on runtime_host_heartbeats(runtime_host_id, observed_at desc);

create table if not exists runtime_slot_reservations (
  reservation_id text primary key,
  run_id text not null,
  runtime_host_id text not null references runtime_hosts(runtime_host_id),
  owner_instance_id text not null,
  state text not null check (state in ('reserved','accepted','running','released','expired','aborted')),
  fencing_token bigint not null,
  lease_token_hash text not null,
  capability_hash text not null,
  expires_at timestamptz not null,
  accepted_at timestamptz,
  released_at timestamptz,
  release_reason text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique(run_id, fencing_token)
);
create unique index if not exists uq_runtime_slot_reservation_active_run
  on runtime_slot_reservations(run_id)
  where state in ('reserved','accepted','running');
create index if not exists idx_runtime_slot_reservations_recovery
  on runtime_slot_reservations(state, expires_at);
create index if not exists idx_runtime_slot_reservations_host
  on runtime_slot_reservations(runtime_host_id, state, expires_at);

create table if not exists runtime_run_dispatches (
  dispatch_id text primary key,
  run_id text not null,
  reservation_id text not null references runtime_slot_reservations(reservation_id),
  runtime_host_id text not null references runtime_hosts(runtime_host_id),
  dispatch_attempt int not null,
  plan_version int not null,
  state text not null check (state in (
    'created','sent','accepted','materializing','running','finalizing',
    'succeeded','failed','timeout','aborted','rejected','orphaned'
  )),
  fencing_token bigint not null,
  run_ticket_jti_hash text not null unique,
  run_ticket_expires_at timestamptz not null,
  input_manifest_hash text not null,
  runtime_request_id text,
  accepted_at timestamptz,
  abort_requested_at timestamptz,
  abort_status text check (abort_status is null or abort_status in ('requested','accepted','failed','terminal')),
  terminal_at timestamptz,
  error_code text,
  error_summary jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique(run_id, dispatch_attempt)
);
create unique index if not exists uq_runtime_dispatch_active_run
  on runtime_run_dispatches(run_id)
  where state in ('created','sent','accepted','materializing','running','finalizing');
create index if not exists idx_runtime_dispatch_recovery
  on runtime_run_dispatches(state, updated_at);
create index if not exists idx_runtime_dispatch_host
  on runtime_run_dispatches(runtime_host_id, state, updated_at);

create table if not exists runtime_run_events (
  runtime_run_event_id text primary key,
  run_id text not null,
  dispatch_id text references runtime_run_dispatches(dispatch_id),
  runtime_host_id text references runtime_hosts(runtime_host_id),
  sequence bigint not null,
  source_sequence bigint,
  event_type text not null,
  visibility text not null default 'admin_safe' check (visibility in ('app_safe','admin_safe','internal')),
  safe_payload jsonb not null default '{}'::jsonb,
  usage_delta jsonb not null default '{}'::jsonb,
  trace_id text,
  occurred_at timestamptz not null,
  ingested_at timestamptz not null default now(),
  unique(run_id, sequence),
  unique(dispatch_id, source_sequence)
);
create index if not exists idx_runtime_run_events_replay
  on runtime_run_events(run_id, sequence);
create index if not exists idx_runtime_run_events_host_time
  on runtime_run_events(runtime_host_id, occurred_at desc) where runtime_host_id is not null;
create index if not exists idx_runtime_run_events_source_replay
  on runtime_run_events(dispatch_id, source_sequence) where source_sequence is not null;

alter table runtime_run_records add column if not exists runtime_host_id text references runtime_hosts(runtime_host_id);
alter table runtime_run_records add column if not exists reservation_id text references runtime_slot_reservations(reservation_id);
alter table runtime_run_records add column if not exists dispatch_attempt int not null default 0;
alter table runtime_run_records add column if not exists last_event_sequence bigint not null default 0;
alter table runtime_run_records add column if not exists abort_requested_at timestamptz;
alter table runtime_run_records add column if not exists abort_status text;
alter table runtime_run_records add column if not exists orphaned_downstream boolean not null default false;
alter table runtime_run_records add column if not exists fencing_token bigint;
alter table runtime_run_records add column if not exists session_generation int;
alter table runtime_run_records add column if not exists recovery_mode text;
alter table runtime_run_records add column if not exists recovered_from_generation int;
alter table runtime_run_records add column if not exists context_loss_risk text;
alter table runtime_run_records add column if not exists runtime_request_id text;

create index if not exists idx_runtime_runs_host_status
  on runtime_run_records(runtime_host_id, status, updated_at desc);
create index if not exists idx_runtime_runs_abort_recovery
  on runtime_run_records(abort_status, updated_at) where abort_requested_at is not null;

alter table thread_agent_runtime_bindings add column if not exists runtime_host_id text references runtime_hosts(runtime_host_id);
alter table thread_agent_runtime_bindings add column if not exists session_store_id text;
alter table thread_agent_runtime_bindings add column if not exists host_binding_generation int not null default 1;
alter table thread_agent_runtime_bindings add column if not exists last_dispatch_id text references runtime_run_dispatches(dispatch_id);
alter table thread_agent_runtime_bindings add column if not exists last_event_sequence bigint not null default 0;
alter table thread_agent_runtime_bindings add column if not exists recovery_mode text;
alter table thread_agent_runtime_bindings add column if not exists recovered_from_generation int;
alter table thread_agent_runtime_bindings add column if not exists context_loss_risk text;
alter table thread_agent_runtime_bindings add column if not exists rotated_at timestamptz;

create index if not exists idx_thread_agent_bindings_host
  on thread_agent_runtime_bindings(runtime_host_id, status, updated_at desc)
  where runtime_host_id is not null;
