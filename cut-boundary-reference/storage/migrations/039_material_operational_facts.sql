-- Forward-only Material operational facts. These tables are independent from
-- quota usage_records: parser byte measurements and durable Material audit
-- evidence must not change quota balances or reservations.

create table if not exists material_usage_facts (
  material_usage_fact_id text primary key,
  tenant_id text not null,
  user_id text not null references users(user_id),
  workspace_id text not null references workspaces(workspace_id),
  material_id text not null references workspace_materials(material_id),
  material_processing_job_id text references material_processing_jobs(material_processing_job_id),
  agent_run_id text references agent_runs(agent_run_id),
  variant_kind text check (variant_kind is null or variant_kind in ('source','minutes','summary','deposit')),
  attempt int not null check (attempt >= 0),
  fact_key text not null,
  payload_hash text not null check (payload_hash ~ '^[0-9a-f]{64}$'),
  stage text not null check (stage in ('extract','generation')),
  outcome text not null check (outcome in ('succeeded','failed','partial')),
  parser_name text,
  parser_version text,
  provider text,
  model text,
  prompt_version text,
  input_bytes bigint not null default 0 check (input_bytes >= 0),
  output_bytes bigint not null default 0 check (output_bytes >= 0),
  input_tokens bigint not null default 0 check (input_tokens >= 0),
  output_tokens bigint not null default 0 check (output_tokens >= 0),
  cost_microunits bigint not null default 0 check (cost_microunits >= 0),
  cost_currency text,
  duration_millis bigint not null default 0 check (duration_millis >= 0),
  result_code text not null,
  trace_id text not null,
  created_at timestamptz not null default now(),
  unique (tenant_id, fact_key),
  check (
    (stage = 'extract'
      and parser_name is not null and parser_version is not null
      and provider is null and model is null and prompt_version is null
      and agent_run_id is null and input_tokens = 0 and output_tokens = 0
      and cost_microunits = 0 and cost_currency is null)
    or
    (stage = 'generation'
      and parser_name is null and parser_version is null
      and provider is not null and model is not null and prompt_version is not null
      and agent_run_id is not null and input_bytes = 0 and output_bytes = 0
      and ((cost_microunits = 0 and cost_currency is null)
        or (cost_microunits > 0 and cost_currency ~ '^[A-Z]{3}$')))
  )
);

create index if not exists idx_material_usage_facts_scope_time
  on material_usage_facts(tenant_id, material_id, created_at desc);
create index if not exists idx_material_usage_facts_job_attempt
  on material_usage_facts(material_processing_job_id, attempt, created_at desc)
  where material_processing_job_id is not null;
create index if not exists idx_material_usage_facts_agent_run
  on material_usage_facts(agent_run_id, created_at desc)
  where agent_run_id is not null;

create table if not exists material_audit_events (
  material_audit_event_id text primary key,
  tenant_id text not null,
  user_id text not null references users(user_id),
  workspace_id text not null references workspaces(workspace_id),
  material_id text not null references workspace_materials(material_id),
  material_processing_job_id text references material_processing_jobs(material_processing_job_id),
  material_proposal_id text references material_revision_proposals(material_proposal_id),
  material_revision_id text references workspace_material_revisions(material_revision_id),
  agent_run_id text references agent_runs(agent_run_id),
  variant_kind text check (variant_kind is null or variant_kind in ('source','minutes','summary','deposit')),
  event_key text not null,
  payload_hash text not null check (payload_hash ~ '^[0-9a-f]{64}$'),
  event_type text not null check (event_type in (
    'proposal_ready','proposal_applied','proposal_rejected','proposal_generation_failed',
    'proposal_apply_failed','revision_committed','revision_recovered','revision_rolled_back',
    'revision_failed','job_retry_wait','job_failed','job_dead_letter','recovery_started','recovery_blocked'
  )),
  actor_type text not null check (actor_type in ('system','runtime','user','admin')),
  actor_id text not null,
  attempt int not null check (attempt >= 0),
  write_fencing_token bigint not null default 0 check (write_fencing_token >= 0),
  old_version int not null default 0 check (old_version >= 0),
  new_version int not null default 0 check (new_version >= 0),
  old_source_version int not null default 0 check (old_source_version >= 0),
  new_source_version int not null default 0 check (new_source_version >= 0),
  old_content_hash text check (old_content_hash is null or old_content_hash ~ '^[0-9a-f]{64}$'),
  new_content_hash text check (new_content_hash is null or new_content_hash ~ '^[0-9a-f]{64}$'),
  result_code text not null,
  trace_id text not null,
  created_at timestamptz not null default now(),
  unique (tenant_id, event_key)
);

create index if not exists idx_material_audit_events_scope_time
  on material_audit_events(tenant_id, material_id, created_at desc);
create index if not exists idx_material_audit_events_job_attempt
  on material_audit_events(material_processing_job_id, attempt, created_at desc)
  where material_processing_job_id is not null;
create index if not exists idx_material_audit_events_proposal
  on material_audit_events(material_proposal_id, created_at desc)
  where material_proposal_id is not null;
create index if not exists idx_material_audit_events_revision
  on material_audit_events(material_revision_id, created_at desc)
  where material_revision_id is not null;
