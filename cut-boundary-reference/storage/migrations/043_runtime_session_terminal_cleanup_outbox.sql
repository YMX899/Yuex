-- Durable post-commit cleanup for terminal Product-session Redis/Tair leases.
--
-- The terminal PostgreSQL transaction records only convergence/admission
-- identity. The raw random Redis token is never persisted; the recovery
-- worker reconstructs a restricted proof from the terminal admission's
-- token hash, owner, run, scope and fencing token.

create table if not exists runtime_session_terminal_cleanup_outbox (
  convergence_id text primary key references runtime_terminal_convergences(convergence_id),
  admission_id text not null references runtime_session_admissions(admission_id),
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

create index if not exists idx_runtime_session_terminal_cleanup_outbox_pick
  on runtime_session_terminal_cleanup_outbox(next_attempt_at, created_at)
  where status <> 'succeeded';

create index if not exists idx_runtime_session_terminal_cleanup_outbox_admission
  on runtime_session_terminal_cleanup_outbox(admission_id, created_at desc);
