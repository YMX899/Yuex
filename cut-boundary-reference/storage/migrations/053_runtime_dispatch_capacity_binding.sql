-- Runtime dispatches created after this migration must name the exact
-- capacity reservation generation that admitted them. Historical rows remain
-- nullable and explicitly unbound; no migration may infer a binding by run_id.

alter table runtime_run_dispatches
  add column if not exists capacity_reservation_id text references runtime_capacity_reservations(capacity_reservation_id);
alter table runtime_run_dispatches
  add column if not exists capacity_reserved_version bigint;

alter table runtime_run_dispatches
  drop constraint if exists runtime_run_dispatches_capacity_binding_check;
alter table runtime_run_dispatches
  add constraint runtime_run_dispatches_capacity_binding_check check (
    (capacity_reservation_id is null and capacity_reserved_version is null)
    or (capacity_reservation_id is not null and capacity_reserved_version >= 1)
  );

create index if not exists idx_runtime_dispatches_capacity_binding
  on runtime_run_dispatches(capacity_reservation_id, capacity_reserved_version)
  where capacity_reservation_id is not null;
