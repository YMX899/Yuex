package runtime

import (
	"context"
	"fmt"
	"sort"
	"time"

	"huahuoai/backend/source/internal/persistence"
)

type RuntimeHostReconcileReport struct {
	UnhealthyHostIDs     []string
	OfflineHostIDs       []string
	DrainDeadlineHostIDs []string
	ExpiredReservations  int
}

func (r *RuntimeHostRepository) ReconcileHostHealth(ctx context.Context, now time.Time, unhealthyAfter, offlineAfter time.Duration) (RuntimeHostReconcileReport, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if unhealthyAfter <= 0 {
		unhealthyAfter = 30 * time.Second
	}
	if offlineAfter <= unhealthyAfter {
		offlineAfter = 90 * time.Second
	}
	if r.postgresReady() {
		return r.reconcileHostHealthPostgres(ctx, now, unhealthyAfter, offlineAfter)
	}
	return r.reconcileHostHealthMemory(ctx, now, unhealthyAfter, offlineAfter)
}

func (r *RuntimeHostRepository) ListActiveDispatchesByHost(ctx context.Context, hostIDs []string) ([]RuntimeDispatch, error) {
	if len(hostIDs) == 0 {
		return []RuntimeDispatch{}, nil
	}
	if r.postgresReady() {
		rows, err := r.db.Pool.Query(ctx, `select dispatch_id,run_id,reservation_id,coalesce(capacity_reservation_id,''),coalesce(capacity_reserved_version,0),runtime_host_id,dispatch_attempt,plan_version,state,fencing_token,run_ticket_jti_hash,run_ticket_expires_at,input_manifest_hash,abort_requested_at,coalesce(abort_status,''),created_at,updated_at from runtime_run_dispatches where runtime_host_id=any($1) and state in('created','sent','submit_unknown','retry_same_host','accepted','materializing','running','finalizing','recovering') order by runtime_host_id,created_at`, hostIDs)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		out := []RuntimeDispatch{}
		for rows.Next() {
			var item RuntimeDispatch
			if err := rows.Scan(&item.DispatchID, &item.RunID, &item.ReservationID, &item.CapacityReservationID, &item.CapacityReservedVersion, &item.RuntimeHostID, &item.DispatchAttempt, &item.PlanVersion, &item.State, &item.FencingToken, &item.RunTicketJTIHash, &item.TicketExpiresAt, &item.InputManifestHash, &item.AbortRequestedAt, &item.AbortStatus, &item.CreatedAt, &item.UpdatedAt); err != nil {
				return nil, err
			}
			out = append(out, item)
		}
		return out, rows.Err()
	}
	wanted := map[string]bool{}
	for _, hostID := range hostIDs {
		wanted[hostID] = true
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	out := []RuntimeDispatch{}
	for _, dispatch := range r.dispatches {
		if wanted[dispatch.RuntimeHostID] && activeRuntimeDispatchState(dispatch.State) {
			out = append(out, dispatch)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].RuntimeHostID != out[j].RuntimeHostID {
			return out[i].RuntimeHostID < out[j].RuntimeHostID
		}
		return out[i].CreatedAt.Before(out[j].CreatedAt)
	})
	return out, nil
}

// ListOrphanedDispatchesNeedingAdmissionRecovery finds terminal orphaned
// dispatches whose Slot may already be released while capacity or session
// admission remains active. This is the durable crash-replay boundary for an
// interrupted offline/status recovery: accepted capacity must not be expired
// solely by TTL because the original provider call may still have been live.
func (r *RuntimeHostRepository) ListOrphanedDispatchesNeedingAdmissionRecovery(ctx context.Context, limit int) ([]RuntimeDispatch, error) {
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}
	if r.postgresReady() {
		rows, err := r.db.Pool.Query(ctx, runtimeDispatchSelect+` d
where d.state='orphaned'
  and (
    exists (
      select 1 from runtime_capacity_reservations c
      where c.run_id=d.run_id and c.state in ('reserved','accepted','recovering')
    )
    or exists (
      select 1 from runtime_session_admissions a
      where a.run_id=d.run_id and a.state in ('acquired','reservation_bound','dispatch_bound','recovering')
    )
  )
order by d.updated_at,d.dispatch_id
limit $1`, limit)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		out := []RuntimeDispatch{}
		for rows.Next() {
			item, scanErr := scanRuntimeDispatchFull(rows)
			if scanErr != nil {
				return nil, scanErr
			}
			out = append(out, item)
		}
		return out, rows.Err()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	out := []RuntimeDispatch{}
	for _, dispatch := range r.dispatches {
		if dispatch.State == "orphaned" {
			out = append(out, dispatch)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].UpdatedAt.Equal(out[j].UpdatedAt) {
			return out[i].DispatchID < out[j].DispatchID
		}
		return out[i].UpdatedAt.Before(out[j].UpdatedAt)
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (r *RuntimeHostRepository) RecalculateHostCounters(ctx context.Context) error {
	if r.postgresReady() {
		// A Slot terminal transition locks its reservation before it updates the
		// same Host counters. Lock the Host first, then derive its counts, so a
		// repair snapshot cannot read a pre-terminal reservation and overwrite a
		// counter decrement that committed while the repair was waiting.
		return r.db.WithTx(ctx, func(tx *persistence.Tx) error {
			rows, err := tx.Query(ctx, `select runtime_host_id from runtime_hosts order by runtime_host_id for update`, map[string]any{})
			if err != nil {
				return err
			}
			for _, row := range rows {
				hostID := fmt.Sprint(row["runtime_host_id"])
				if hostID == "" {
					return fmt.Errorf("RUNTIME_HOST_COUNTER_DRIFT")
				}
				if err := tx.Exec(ctx, `update runtime_hosts h set
reserved_runs=(select count(*)::int from runtime_slot_reservations r where r.runtime_host_id=h.runtime_host_id and r.state='reserved'),
active_runs=(select count(*)::int from runtime_slot_reservations r where r.runtime_host_id=h.runtime_host_id and r.state in('accepted','running')),
reserved_product_thread_runs=(select count(*)::int from runtime_slot_reservations r where r.runtime_host_id=h.runtime_host_id and r.state='reserved' and r.execution_scope='product_thread'),
active_product_thread_runs=(select count(*)::int from runtime_slot_reservations r where r.runtime_host_id=h.runtime_host_id and r.state in('accepted','running') and r.execution_scope='product_thread'),
reserved_detached_task_runs=(select count(*)::int from runtime_slot_reservations r where r.runtime_host_id=h.runtime_host_id and r.state='reserved' and r.execution_scope='detached_task'),
active_detached_task_runs=(select count(*)::int from runtime_slot_reservations r where r.runtime_host_id=h.runtime_host_id and r.state in('accepted','running') and r.execution_scope='detached_task'),
recovery_state=case
when lower(h.environment) in ('prelaunch','prod','production') then h.recovery_state
when exists(
  select 1 from runtime_slot_reservations r
  where r.runtime_host_id=h.runtime_host_id
    and r.state in('reserved','accepted','running')
    and r.execution_scope_source='legacy_unclassified'
) then 'pending' else 'reconciled' end,
updated_at=now()
where h.runtime_host_id=@host`, map[string]any{"host": hostID}); err != nil {
					return err
				}
			}
			return nil
		})
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for hostID, host := range r.hosts {
		hasActiveLegacyUnclassifiedReservation := false
		host.ActiveRuns = 0
		host.ReservedRuns = 0
		host.ActiveProductThreadRuns = 0
		host.ReservedProductThreadRuns = 0
		host.ActiveDetachedTaskRuns = 0
		host.ReservedDetachedTaskRuns = 0
		for _, reservation := range r.reservations {
			if reservation.RuntimeHostID != hostID {
				continue
			}
			if activeRuntimeReservationState(reservation.State) && reservation.ExecutionScopeSource == runtimeExecutionScopeSourceLegacyUnclassified {
				hasActiveLegacyUnclassifiedReservation = true
			}
			switch reservation.State {
			case "reserved":
				incrementRuntimeHostReserved(&host, reservation.ExecutionScope)
			case "accepted", "running":
				incrementRuntimeHostActive(&host, reservation.ExecutionScope)
			}
		}
		if !runtimeHostRecoveryAttestationRequired(host.Environment) {
			if hasActiveLegacyUnclassifiedReservation {
				host.RecoveryState = "pending"
			} else {
				host.RecoveryState = "reconciled"
			}
		}
		r.hosts[hostID] = host
	}
	return nil
}

func (r *RuntimeHostRepository) reconcileHostHealthPostgres(ctx context.Context, now time.Time, unhealthyAfter, offlineAfter time.Duration) (RuntimeHostReconcileReport, error) {
	report := RuntimeHostReconcileReport{}
	offlineRows, err := r.db.Pool.Query(ctx, `update runtime_hosts set status='offline',updated_at=now() where status in('registering','ready','unhealthy') and coalesce(last_heartbeat_at,updated_at)<=$1 returning runtime_host_id`, now.Add(-offlineAfter))
	if err != nil {
		return report, err
	}
	for offlineRows.Next() {
		var hostID string
		if err := offlineRows.Scan(&hostID); err != nil {
			offlineRows.Close()
			return report, err
		}
		report.OfflineHostIDs = append(report.OfflineHostIDs, hostID)
	}
	if err := offlineRows.Err(); err != nil {
		offlineRows.Close()
		return report, err
	}
	offlineRows.Close()
	report.OfflineHostIDs = report.OfflineHostIDs[:0]
	allOfflineRows, err := r.db.Pool.Query(ctx, `select runtime_host_id from runtime_hosts where status='offline' order by runtime_host_id`)
	if err != nil {
		return report, err
	}
	for allOfflineRows.Next() {
		var hostID string
		if err := allOfflineRows.Scan(&hostID); err != nil {
			allOfflineRows.Close()
			return report, err
		}
		report.OfflineHostIDs = append(report.OfflineHostIDs, hostID)
	}
	if err := allOfflineRows.Err(); err != nil {
		allOfflineRows.Close()
		return report, err
	}
	allOfflineRows.Close()

	unhealthyRows, err := r.db.Pool.Query(ctx, `update runtime_hosts set status='unhealthy',updated_at=now() where status in('registering','ready') and coalesce(last_heartbeat_at,updated_at)<=$1 returning runtime_host_id`, now.Add(-unhealthyAfter))
	if err != nil {
		return report, err
	}
	for unhealthyRows.Next() {
		var hostID string
		if err := unhealthyRows.Scan(&hostID); err != nil {
			unhealthyRows.Close()
			return report, err
		}
		report.UnhealthyHostIDs = append(report.UnhealthyHostIDs, hostID)
	}
	if err := unhealthyRows.Err(); err != nil {
		unhealthyRows.Close()
		return report, err
	}
	unhealthyRows.Close()

	drainRows, err := r.db.Pool.Query(ctx, `select runtime_host_id from runtime_hosts where status='draining' and drain_deadline_at is not null and drain_deadline_at<=$1 order by runtime_host_id`, now)
	if err != nil {
		return report, err
	}
	for drainRows.Next() {
		var hostID string
		if err := drainRows.Scan(&hostID); err != nil {
			drainRows.Close()
			return report, err
		}
		report.DrainDeadlineHostIDs = append(report.DrainDeadlineHostIDs, hostID)
	}
	if err := drainRows.Err(); err != nil {
		drainRows.Close()
		return report, err
	}
	drainRows.Close()

	report.ExpiredReservations, err = r.ExpireReservations(ctx, now)
	if err == nil {
		err = r.RecalculateHostCounters(ctx)
	}
	return report, err
}

func (r *RuntimeHostRepository) reconcileHostHealthMemory(ctx context.Context, now time.Time, unhealthyAfter, offlineAfter time.Duration) (RuntimeHostReconcileReport, error) {
	report := RuntimeHostReconcileReport{}
	r.mu.Lock()
	for hostID, host := range r.hosts {
		last := host.LastHeartbeatAt
		if last.IsZero() {
			last = host.UpdatedAt
		}
		if stringInRuntime(host.Status, []string{"registering", "ready", "unhealthy"}) && !last.After(now.Add(-offlineAfter)) {
			host.Status = "offline"
			host.UpdatedAt = now
			r.hosts[hostID] = host
			report.OfflineHostIDs = append(report.OfflineHostIDs, hostID)
			continue
		}
		if host.Status == "offline" {
			report.OfflineHostIDs = append(report.OfflineHostIDs, hostID)
		}
		if stringInRuntime(host.Status, []string{"registering", "ready"}) && !last.After(now.Add(-unhealthyAfter)) {
			host.Status = "unhealthy"
			host.UpdatedAt = now
			r.hosts[hostID] = host
			report.UnhealthyHostIDs = append(report.UnhealthyHostIDs, hostID)
		}
		if host.Status == "draining" && !host.DrainDeadlineAt.IsZero() && !host.DrainDeadlineAt.After(now) {
			report.DrainDeadlineHostIDs = append(report.DrainDeadlineHostIDs, hostID)
		}
	}
	r.mu.Unlock()
	var err error
	report.ExpiredReservations, err = r.ExpireReservations(ctx, now)
	if err == nil {
		err = r.RecalculateHostCounters(ctx)
	}
	sort.Strings(report.UnhealthyHostIDs)
	sort.Strings(report.OfflineHostIDs)
	sort.Strings(report.DrainDeadlineHostIDs)
	return report, err
}

func uniqueRuntimeHostIDs(values ...[]string) []string {
	seen := map[string]bool{}
	out := []string{}
	for _, items := range values {
		for _, item := range items {
			if item == "" || seen[item] {
				continue
			}
			seen[item] = true
			out = append(out, item)
		}
	}
	sort.Strings(out)
	return out
}
