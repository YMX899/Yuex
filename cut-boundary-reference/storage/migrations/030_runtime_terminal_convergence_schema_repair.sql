-- Repair the published v025 terminal convergence table without rewriting v025.
-- Some databases applied an earlier v025 shape that did not persist recovery
-- identity or the immutable terminal snapshot.

alter table runtime_terminal_convergences add column if not exists run_id text;
alter table runtime_terminal_convergences add column if not exists queue_id text;
alter table runtime_terminal_convergences add column if not exists terminal_snapshot jsonb;
alter table runtime_terminal_convergences add column if not exists snapshot_hash text;

update runtime_terminal_convergences c
set run_id = d.run_id
from runtime_run_dispatches d
where c.run_id is null and d.dispatch_id = c.dispatch_id;

update runtime_terminal_convergences c
set queue_id = coalesce(
  (
    select q.queue_id
    from task_queue_records q
    where q.queue_id = 'runtime_events:' || c.dispatch_id
    limit 1
  ),
  (
    select q.queue_id
    from task_queue_records q
    where q.queue_name = 'runtime_events'
      and q.task_id = c.run_id
      and coalesce(q.payload->>'dispatchId', q.payload->>'dispatch_id') = c.dispatch_id
    order by q.created_at desc
    limit 1
  )
)
where c.queue_id is null;

-- An incomplete legacy row has no immutable snapshot to resume from. Removing
-- only that checkpoint lets the durable runtime_events job rebuild it through
-- the existing idempotent convergence steps.
delete from runtime_terminal_convergences
where queue_completed_at is null
  and (terminal_snapshot is null or snapshot_hash is null);

do $$
begin
  if exists (
    select 1
    from runtime_terminal_convergences
    where run_id is null or queue_id is null
  ) then
    raise exception 'runtime terminal convergence identity backfill failed';
  end if;
end
$$;

-- Completed legacy rows are retained for audit. They are never selected for
-- recovery, so a minimal identity snapshot is sufficient for the new contract.
update runtime_terminal_convergences c
set terminal_snapshot = jsonb_build_object(
  'runId', c.run_id,
  'dispatchId', c.dispatch_id,
  'terminalSourceSequence', c.terminal_source_sequence,
  'terminalStatus', c.terminal_status,
  'safeResult', '{}'::jsonb,
  'safeError', '{}'::jsonb,
  'actualUsage', '{}'::jsonb,
  'queueId', c.queue_id,
  'sessionRequired', false,
  'capacityReservationId', '',
  'capacitySnapshotVersion', 0
)
where c.terminal_snapshot is null;

update runtime_terminal_convergences
set snapshot_hash = 'legacy:' || md5(convergence_id || ':' || terminal_snapshot::text)
where snapshot_hash is null;

alter table runtime_terminal_convergences alter column run_id set not null;
alter table runtime_terminal_convergences alter column queue_id set not null;
alter table runtime_terminal_convergences alter column terminal_snapshot set not null;
alter table runtime_terminal_convergences alter column snapshot_hash set not null;

do $$
begin
  if not exists (
    select 1 from pg_constraint
    where conrelid = 'runtime_terminal_convergences'::regclass
      and conname = 'runtime_terminal_convergences_run_id_fkey'
  ) then
    alter table runtime_terminal_convergences
      add constraint runtime_terminal_convergences_run_id_fkey
      foreign key (run_id) references agent_runs(agent_run_id);
  end if;

  if not exists (
    select 1 from pg_constraint
    where conrelid = 'runtime_terminal_convergences'::regclass
      and conname = 'runtime_terminal_convergences_queue_id_fkey'
  ) then
    alter table runtime_terminal_convergences
      add constraint runtime_terminal_convergences_queue_id_fkey
      foreign key (queue_id) references task_queue_records(queue_id);
  end if;
end
$$;
