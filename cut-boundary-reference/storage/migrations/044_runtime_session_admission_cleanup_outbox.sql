-- Forward-only durable cleanup for direct Product-session admission release.
--
-- Migration 043 owns a real Runtime terminal convergence. Scheduler rollback,
-- orphan recovery and legacy terminal paths do not have a convergence row, so
-- they use this admission-keyed outbox instead. The raw Redis/Tair token and
-- the derived Redis key are intentionally never persisted.

create table if not exists runtime_session_admission_cleanup_outbox (
  admission_id text primary key references runtime_session_admissions(admission_id),
  run_id text not null,
  owner_instance_id text not null,
  lease_token_hash text not null,
  fencing_token bigint not null check (fencing_token > 0),
  cleanup_origin text not null check (cleanup_origin in ('direct_release','orphan_recovery','lease_expiry')),
  release_reason text not null check (release_reason in ('succeeded','failed','timeout','aborted','orphaned','reservation_failed','dispatch_failed','lease_expired','recovered')),
  status text not null default 'pending' check (status in ('pending','running','succeeded')),
  attempt_count int not null default 0 check (attempt_count >= 0),
  next_attempt_at timestamptz not null default now(),
  lease_owner text,
  lease_fencing_token bigint not null default 0 check (lease_fencing_token >= 0),
  lease_expires_at timestamptz,
  last_error_code text check (last_error_code is null or last_error_code ~ '^[A-Z0-9_]{1,128}$'),
  completed_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  check (
    (status = 'pending' and lease_owner is null and lease_expires_at is null and completed_at is null)
    or
    (status = 'running' and lease_owner is not null and lease_fencing_token > 0 and lease_expires_at is not null and completed_at is null)
    or
    (status = 'succeeded' and lease_owner is null and lease_expires_at is null and completed_at is not null)
  )
);

create index if not exists idx_runtime_session_admission_cleanup_outbox_pick
  on runtime_session_admission_cleanup_outbox(next_attempt_at, created_at)
  where status <> 'succeeded';

create index if not exists idx_runtime_session_admission_cleanup_outbox_run
  on runtime_session_admission_cleanup_outbox(run_id, created_at desc);

-- Backfill legacy released/expired admissions that are not already owned by
-- the terminal-convergence outbox. This is idempotent and preserves only the
-- immutable proof hash/fencing tuple needed for exact Redis/Tair deletion.
insert into runtime_session_admission_cleanup_outbox(
  admission_id,run_id,owner_instance_id,lease_token_hash,fencing_token,
  cleanup_origin,release_reason,status,next_attempt_at
)
select
  a.admission_id,a.run_id,a.owner_instance_id,a.lease_token_hash,a.fencing_token,
  case when coalesce(a.release_reason,'') in ('orphaned','recovered') then 'orphan_recovery'
       when coalesce(a.release_reason,'') = 'lease_expired' then 'lease_expiry'
       else 'direct_release' end,
  case when a.release_reason in ('succeeded','failed','timeout','aborted','orphaned','reservation_failed','dispatch_failed','lease_expired','recovered')
       then a.release_reason
       when a.state = 'expired' then 'lease_expired'
       else 'recovered' end,
  'pending',now()
from runtime_session_admissions a
where a.state in ('released','expired')
  and not exists (
    select 1
    from runtime_session_terminal_cleanup_outbox terminal
    join runtime_terminal_convergences convergence on convergence.convergence_id=terminal.convergence_id
    where terminal.admission_id=a.admission_id
      and convergence.run_id=a.run_id
  )
on conflict(admission_id) do nothing;
