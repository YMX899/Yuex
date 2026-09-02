-- Agent Runtime V0.5 dynamic routing, immutable Workspace snapshots,
-- MaterialPackage and the durable Workspace index outbox.

alter table workspaces add column if not exists workspace_version bigint not null default 1;
alter table workspaces add column if not exists index_version bigint not null default 0;
alter table workspaces drop constraint if exists workspaces_user_id_key;
create index if not exists idx_workspaces_user_status on workspaces(user_id, status, updated_at desc);

alter table chat_threads add column if not exists active_workspace_id text references workspaces(workspace_id);
alter table chat_threads add column if not exists workspace_binding_version bigint not null default 1;
alter table chat_threads add column if not exists context_generation bigint not null default 1;
alter table chat_threads add column if not exists routing_mode text not null default 'dynamic';
alter table chat_threads add column if not exists source_surface text;

update chat_threads
set active_workspace_id = workspace_id,
    source_surface = coalesce(source_surface, scene)
where active_workspace_id is null;

create index if not exists idx_chat_threads_active_workspace
  on chat_threads(user_id, active_workspace_id, updated_at desc);

create table if not exists chat_thread_workspace_history (
  history_id text primary key,
  thread_id text not null references chat_threads(thread_id),
  tenant_id text not null,
  user_id text not null references users(user_id),
  previous_workspace_id text references workspaces(workspace_id),
  workspace_id text not null references workspaces(workspace_id),
  binding_version bigint not null,
  context_generation bigint not null,
  idempotency_key text not null,
  reason text not null,
  changed_by_type text not null default 'user' check (changed_by_type in ('user','system','admin')),
  changed_by_id text,
  created_at timestamptz not null default now(),
  unique(thread_id, binding_version),
  unique(user_id, idempotency_key)
);
create index if not exists idx_thread_workspace_history_thread_time
  on chat_thread_workspace_history(thread_id, created_at desc);
create index if not exists idx_thread_workspace_history_workspace
  on chat_thread_workspace_history(workspace_id, created_at desc);

create table if not exists thread_agent_runtime_bindings (
  binding_id text primary key,
  thread_id text not null references chat_threads(thread_id),
  tenant_id text not null,
  user_id text not null references users(user_id),
  agent_profile text not null,
  session_generation int not null default 1,
  context_generation bigint not null,
  openclaw_session_key_ciphertext text not null,
  openclaw_session_key_hash text not null,
  workspace_id text not null references workspaces(workspace_id),
  manifest_version text not null,
  agent_hash text not null,
  status text not null default 'active' check (status in ('active','rotated','revoked','orphaned')),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique(thread_id, agent_profile, context_generation, session_generation),
  unique(openclaw_session_key_hash)
);
create index if not exists idx_thread_agent_bindings_lookup
  on thread_agent_runtime_bindings(thread_id, agent_profile, context_generation, status);
create index if not exists idx_thread_agent_bindings_workspace
  on thread_agent_runtime_bindings(workspace_id, status, updated_at desc);

insert into thread_agent_runtime_bindings (
  binding_id, thread_id, tenant_id, user_id, agent_profile, session_generation,
  context_generation, openclaw_session_key_ciphertext, openclaw_session_key_hash,
  workspace_id, manifest_version, agent_hash, status, created_at, updated_at
)
select
  'binding_legacy_' || md5(b.thread_id || ':' || b.agent_profile),
  b.thread_id, b.tenant_id, b.user_id, b.agent_profile, 1, 1,
  b.openclaw_session_key_ciphertext, b.openclaw_session_key_hash,
  b.workspace_id, 'legacy-v1', 'legacy:' || md5(b.agent_profile), 'active',
  b.created_at, b.updated_at
from chat_thread_runtime_bindings b
on conflict do nothing;

alter table chat_messages add column if not exists workspace_id text references workspaces(workspace_id);
alter table chat_messages add column if not exists workspace_version bigint;
alter table chat_messages add column if not exists thread_workspace_binding_version bigint;
alter table chat_messages add column if not exists context_generation bigint;

update chat_messages m
set workspace_id = coalesce(m.workspace_id, t.active_workspace_id, t.workspace_id),
    workspace_version = coalesce(m.workspace_version, w.workspace_version, 1),
    thread_workspace_binding_version = coalesce(m.thread_workspace_binding_version, t.workspace_binding_version, 1),
    context_generation = coalesce(m.context_generation, t.context_generation, 1)
from chat_threads t
left join workspaces w on w.workspace_id = coalesce(t.active_workspace_id, t.workspace_id)
where m.thread_id = t.thread_id
  and (m.workspace_id is null or m.workspace_version is null or
       m.thread_workspace_binding_version is null or m.context_generation is null);

alter table ai_tasks add column if not exists workspace_version bigint;
alter table ai_tasks add column if not exists thread_workspace_binding_version bigint;
alter table ai_tasks add column if not exists context_generation bigint;

update ai_tasks a
set workspace_version = coalesce(a.workspace_version, w.workspace_version, 1),
    thread_workspace_binding_version = coalesce(
      a.thread_workspace_binding_version,
      (select t.workspace_binding_version from chat_threads t where t.thread_id = a.thread_id),
      1
    ),
    context_generation = coalesce(
      a.context_generation,
      (select t.context_generation from chat_threads t where t.thread_id = a.thread_id),
      1
    )
from workspaces w
where a.workspace_id = w.workspace_id
  and (a.workspace_version is null or a.thread_workspace_binding_version is null or a.context_generation is null);

alter table runtime_run_records add column if not exists user_id text references users(user_id);
alter table runtime_run_records add column if not exists workspace_version bigint;
alter table runtime_run_records add column if not exists index_version bigint;
alter table runtime_run_records add column if not exists thread_workspace_binding_version bigint;
alter table runtime_run_records add column if not exists context_generation bigint;
alter table runtime_run_records add column if not exists agent_run_id text;

update runtime_run_records r
set user_id = coalesce(r.user_id, a.user_id),
    workspace_version = coalesce(r.workspace_version, w.workspace_version, 1),
    index_version = coalesce(r.index_version, w.index_version, 0),
    thread_workspace_binding_version = coalesce(r.thread_workspace_binding_version, t.workspace_binding_version, 1),
    context_generation = coalesce(r.context_generation, t.context_generation, 1)
from ai_tasks a
left join workspaces w on w.workspace_id = a.workspace_id
left join chat_threads t on t.thread_id = a.thread_id
where r.task_id = a.task_id
  and (r.user_id is null or r.workspace_version is null or r.index_version is null or
       r.thread_workspace_binding_version is null or r.context_generation is null);

create table if not exists agent_runs (
  agent_run_id text primary key,
  tenant_id text not null,
  user_id text not null references users(user_id),
  workspace_id text not null references workspaces(workspace_id),
  workspace_version bigint not null,
  workspace_binding_version bigint not null,
  context_generation bigint not null,
  thread_id text references chat_threads(thread_id),
  task_id text references ai_tasks(task_id),
  idempotency_key text not null,
  request_hash text not null,
  request_snapshot jsonb not null default '{}'::jsonb,
  status text not null check (status in (
    'created','resolving_intent','planning','awaiting_confirmation','admission_pending',
    'queued','running','finalizing','aborting','succeeded','failed','cancelled','timeout','orphaned'
  )),
  routing_mode text not null check (routing_mode in ('dynamic','legacy_adapter')),
  source_surface text,
  intent_snapshot jsonb not null default '{}'::jsonb,
  plan_snapshot jsonb not null default '{}'::jsonb,
  public_result jsonb not null default '{}'::jsonb,
  error_summary jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique(user_id, idempotency_key)
);
create index if not exists idx_agent_runs_user_status_time
  on agent_runs(user_id, status, created_at desc);
create index if not exists idx_agent_runs_workspace_time
  on agent_runs(workspace_id, created_at desc);
create index if not exists idx_agent_runs_thread_time
  on agent_runs(thread_id, created_at desc) where thread_id is not null;

create table if not exists agent_run_plans (
  agent_run_plan_id text primary key,
  agent_run_id text not null references agent_runs(agent_run_id),
  plan_version int not null,
  status text not null check (status in ('draft','validated','awaiting_confirmation','confirmed','executing','succeeded','failed','cancelled')),
  task_type text not null,
  l1_agent_profile text not null,
  selected_skills jsonb not null default '[]'::jsonb,
  selected_knowledge_refs jsonb not null default '[]'::jsonb,
  required_tools jsonb not null default '[]'::jsonb,
  output_contract jsonb not null default '{}'::jsonb,
  workspace_version bigint not null,
  index_version bigint not null,
  manifest_version text not null,
  capability_hash text not null,
  safe_plan_summary text,
  confirmation_key_hash text,
  confirmed_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique(agent_run_id, plan_version)
);
create index if not exists idx_agent_run_plans_run_status
  on agent_run_plans(agent_run_id, status, plan_version desc);

create table if not exists run_workspace_contexts (
  run_id text primary key,
  agent_run_id text references agent_runs(agent_run_id),
  tenant_id text not null,
  user_id text not null references users(user_id),
  workspace_id text not null references workspaces(workspace_id),
  workspace_version bigint not null,
  index_version bigint not null,
  thread_id text references chat_threads(thread_id),
  thread_workspace_binding_version bigint,
  context_generation bigint not null,
  session_generation int,
  l1_agent_profile text not null,
  manifest_version text not null,
  capability_hash text not null,
  allowed_read_roots jsonb not null default '[]'::jsonb,
  allowed_write_roots jsonb not null default '[]'::jsonb,
  selected_skills jsonb not null default '[]'::jsonb,
  selected_knowledge_refs jsonb not null default '[]'::jsonb,
  context_manifest jsonb not null default '{}'::jsonb,
  manifest_hash text not null,
  status text not null default 'frozen' check (status in ('frozen','materializing','ready','released','failed')),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);
create index if not exists idx_run_workspace_contexts_workspace
  on run_workspace_contexts(workspace_id, workspace_version, created_at desc);

create table if not exists runtime_tool_invocations (
  tool_invocation_id text primary key,
  run_id text not null references runtime_run_records(run_id),
  tool_name text not null,
  args_hash text not null,
  workspace_version bigint not null,
  result_fingerprint text,
  status text not null check (status in ('started','succeeded','failed','rejected','aborted')),
  repeat_count int not null default 1,
  cache_hit boolean not null default false,
  duration_ms int,
  error_code text,
  safe_summary jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);
create index if not exists idx_runtime_tool_invocations_run_time
  on runtime_tool_invocations(run_id, created_at);
create index if not exists idx_runtime_tool_invocations_repeat
  on runtime_tool_invocations(run_id, tool_name, args_hash, workspace_version, created_at);

create table if not exists workspace_materials (
  material_id text primary key,
  tenant_id text not null,
  user_id text not null references users(user_id),
  workspace_id text not null references workspaces(workspace_id),
  source_type text not null check (source_type in ('recording','upload','work_ai_chat','feed_ai_chat','manual')),
  source_object_id text,
  title text not null,
  material_time timestamptz not null,
  source_version int not null default 1,
  status text not null check (status in ('processing','ready','ready_with_stale_variants','failed','archived')),
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique(workspace_id, source_type, source_object_id)
);
create index if not exists idx_workspace_materials_timeline
  on workspace_materials(workspace_id, status, material_time desc, material_id desc);

create table if not exists workspace_material_variants (
  material_variant_id text primary key,
  material_id text not null references workspace_materials(material_id),
  variant_kind text not null check (variant_kind in ('source','minutes','summary','deposit')),
  relative_path text not null,
  current_revision int not null default 1,
  base_source_version int not null default 1,
  base_minutes_revision int,
  status text not null check (status in ('processing','current','stale','failed','archived')),
  content_hash text,
  semantic_hash text,
  size_bytes bigint,
  stale_reason text,
  last_editor_type text,
  last_editor_id text,
  user_edited boolean not null default false,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique(material_id, variant_kind),
  unique(material_id, variant_kind, current_revision)
);
create index if not exists idx_material_variants_material_status
  on workspace_material_variants(material_id, status, variant_kind);

create table if not exists workspace_material_revisions (
  material_revision_id text primary key,
  material_variant_id text not null references workspace_material_variants(material_variant_id),
  material_id text not null references workspace_materials(material_id),
  variant_kind text not null,
  revision int not null,
  base_source_version int not null,
  base_minutes_revision int,
  content_hash text not null,
  semantic_hash text,
  size_bytes bigint not null default 0,
  snapshot_ref_ciphertext text,
  editor_type text not null check (editor_type in ('user','agent','system','admin')),
  editor_id text,
  edit_reason text,
  trace_id text,
  task_id text,
  run_id text,
  created_at timestamptz not null default now(),
  unique(material_variant_id, revision)
);
create index if not exists idx_material_revisions_material_kind
  on workspace_material_revisions(material_id, variant_kind, revision desc);

create table if not exists material_processing_jobs (
  material_processing_job_id text primary key,
  material_id text not null references workspace_materials(material_id),
  variant_kind text not null check (variant_kind in ('source','minutes','summary','deposit')),
  job_type text not null check (job_type in ('extract','generate','regenerate','recover')),
  dedupe_key text not null unique,
  status text not null check (status in ('pending','leased','running','retry_wait','succeeded','failed','dead_letter','cancelled')),
  input_snapshot jsonb not null default '{}'::jsonb,
  result_snapshot jsonb not null default '{}'::jsonb,
  error_summary jsonb not null default '{}'::jsonb,
  attempt int not null default 0,
  max_attempts int not null default 5,
  available_at timestamptz not null default now(),
  lease_owner text,
  lease_expires_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);
create index if not exists idx_material_jobs_pick
  on material_processing_jobs(status, available_at, created_at);

create table if not exists workspace_file_index (
  workspace_file_index_id text primary key,
  tenant_id text not null,
  user_id text not null references users(user_id),
  workspace_id text not null references workspaces(workspace_id),
  relative_path text not null,
  workspace_layer text not null,
  file_kind text not null,
  source_type text,
  source_id text,
  mime_type text,
  size_bytes bigint,
  content_hash text,
  search_text text,
  metadata jsonb not null default '{}'::jsonb,
  workspace_version bigint not null,
  status text not null default 'active' check (status in ('active','archived','deleted','invalid')),
  indexed_at timestamptz,
  deleted_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique(workspace_id, relative_path)
);
create index if not exists idx_workspace_file_index_lookup
  on workspace_file_index(workspace_id, status, workspace_layer, file_kind, updated_at desc);
create index if not exists idx_workspace_file_index_source
  on workspace_file_index(workspace_id, source_type, source_id) where source_id is not null;

create table if not exists workspace_index_jobs (
  workspace_index_job_id text primary key,
  workspace_id text not null references workspaces(workspace_id),
  target_workspace_version bigint not null,
  trigger_type text not null,
  dedupe_key text not null unique,
  status text not null check (status in ('pending','leased','running','retry_wait','succeeded','failed','dead_letter')),
  changed_files jsonb not null default '[]'::jsonb,
  attempt int not null default 0,
  max_attempts int not null default 5,
  error_summary jsonb not null default '{}'::jsonb,
  available_at timestamptz not null default now(),
  lease_owner text,
  lease_expires_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);
create index if not exists idx_workspace_index_jobs_pick
  on workspace_index_jobs(status, available_at, created_at);
create index if not exists idx_workspace_index_jobs_workspace
  on workspace_index_jobs(workspace_id, target_workspace_version desc);

create table if not exists workspace_search_index_outbox (
  event_id text primary key,
  tenant_id text not null,
  user_id text not null references users(user_id),
  workspace_id text not null references workspaces(workspace_id),
  workspace_version bigint not null,
  operation text not null check (operation in ('upsert','move','archive','delete','reconcile')),
  relative_path text not null,
  previous_relative_path text,
  content_hash text,
  status text not null default 'pending' check (status in ('pending','leased','running','succeeded','failed','dead_letter')),
  attempt int not null default 0,
  available_at timestamptz not null default now(),
  lease_owner text,
  lease_expires_at timestamptz,
  error_summary jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);
create index if not exists idx_workspace_search_outbox_pick
  on workspace_search_index_outbox(status, available_at, created_at);
create index if not exists idx_workspace_search_outbox_workspace
  on workspace_search_index_outbox(workspace_id, workspace_version, created_at);
