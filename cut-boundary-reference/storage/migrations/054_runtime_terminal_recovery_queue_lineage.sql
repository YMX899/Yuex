-- Terminal convergence recovery must never reactivate a dead-lettered,
-- timed-out, failed, or succeeded runtime_events queue record. Each fresh
-- recovery queue is instead linked to the immutable convergence checkpoint and
-- to the queue record that caused this recovery generation.

create table if not exists runtime_terminal_convergence_recovery_queue_lineage (
  convergence_id text not null references runtime_terminal_convergences(convergence_id) on delete cascade,
  generation integer not null check (generation > 0),
  source_queue_id text not null references task_queue_records(queue_id),
  recovery_queue_id text primary key references task_queue_records(queue_id),
  recovery_dedupe_key text not null unique,
  created_at timestamptz not null default now(),
  unique(convergence_id,generation)
);

create index if not exists idx_runtime_terminal_recovery_queue_lineage_convergence
  on runtime_terminal_convergence_recovery_queue_lineage(convergence_id,generation desc);
