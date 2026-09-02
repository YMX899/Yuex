package runtime

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type RuntimeLeaseLoss struct {
	RunID      string
	DispatchID string
	Handle     RuntimeReservationLease
	ErrorCode  string
}

type RuntimeLeaseLossHandler func(context.Context, RuntimeLeaseLoss)

type runtimeLeaseOwnership struct {
	dispatchID string
	handle     RuntimeReservationLease
	ttl        time.Duration
	cancel     context.CancelFunc
}

// RuntimeLeaseSupervisor owns plaintext lease handles for active dispatches.
// Durable repositories keep only hashes/fencing facts; a replacement process
// must recover after expiry instead of reconstructing the old capability.
type RuntimeLeaseSupervisor struct {
	scheduler *RuntimeScheduler

	mu       sync.Mutex
	owners   map[string]*runtimeLeaseOwnership
	onLoss   RuntimeLeaseLossHandler
	interval func(time.Duration) time.Duration
}

func NewRuntimeLeaseSupervisor(scheduler *RuntimeScheduler) *RuntimeLeaseSupervisor {
	return &RuntimeLeaseSupervisor{
		scheduler: scheduler,
		owners:    map[string]*runtimeLeaseOwnership{},
		interval:  runtimeLeaseRenewInterval,
	}
}

func (s *RuntimeLeaseSupervisor) SetLeaseLossHandler(handler RuntimeLeaseLossHandler) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.onLoss = handler
	s.mu.Unlock()
}

func (s *RuntimeLeaseSupervisor) Track(ctx context.Context, handle RuntimeReservationLease, dispatchID string, ttl time.Duration) error {
	if s == nil || s.scheduler == nil || handle.Reservation.RunID == "" || handle.Reservation.ReservationID == "" || dispatchID == "" || ttl <= 0 {
		return fmt.Errorf("INVALID_ARGUMENT")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	runID := handle.Reservation.RunID
	ownerCtx, cancel := context.WithCancel(ctx)
	owner := &runtimeLeaseOwnership{dispatchID: dispatchID, handle: handle, ttl: ttl, cancel: cancel}

	s.mu.Lock()
	if previous := s.owners[runID]; previous != nil {
		previous.cancel()
	}
	s.owners[runID] = owner
	s.mu.Unlock()

	go s.renewLoop(ownerCtx, runID, owner)
	return nil
}

func (s *RuntimeLeaseSupervisor) Stop(runID string) bool {
	if s == nil || runID == "" {
		return false
	}
	s.mu.Lock()
	owner := s.owners[runID]
	if owner != nil {
		delete(s.owners, runID)
	}
	s.mu.Unlock()
	if owner == nil {
		return false
	}
	owner.cancel()
	return true
}

func (s *RuntimeLeaseSupervisor) IsTracking(runID string) bool {
	if s == nil || runID == "" {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.owners[runID] != nil
}

func (s *RuntimeLeaseSupervisor) renewLoop(ctx context.Context, runID string, owner *runtimeLeaseOwnership) {
	interval := s.interval(owner.ttl)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			renewed, err := s.scheduler.Renew(ctx, owner.handle, owner.ttl)
			if err != nil {
				if s.finishTerminalOwnership(runID, owner) {
					return
				}
				s.handleLeaseLoss(runID, owner, err)
				return
			}
			owner.handle = renewed
			s.mu.Lock()
			if current := s.owners[runID]; current == owner {
				current.handle = renewed
			}
			s.mu.Unlock()
		}
	}
}

func (s *RuntimeLeaseSupervisor) finishTerminalOwnership(runID string, owner *runtimeLeaseOwnership) bool {
	if s.scheduler == nil || s.scheduler.Hosts == nil {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	dispatch, err := s.scheduler.Hosts.GetDispatch(ctx, owner.dispatchID)
	if err != nil || !runtimeDispatchTerminal(dispatch.State) {
		return false
	}
	if err := s.scheduler.ReleaseAfterTerminal(ctx, owner.handle); err != nil {
		return false
	}
	s.mu.Lock()
	if current := s.owners[runID]; current == owner {
		delete(s.owners, runID)
	}
	s.mu.Unlock()
	return true
}

func (s *RuntimeLeaseSupervisor) handleLeaseLoss(runID string, owner *runtimeLeaseOwnership, renewErr error) {
	s.mu.Lock()
	if current := s.owners[runID]; current != owner {
		s.mu.Unlock()
		return
	}
	delete(s.owners, runID)
	handler := s.onLoss
	s.mu.Unlock()

	recoveryCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if s.scheduler != nil && s.scheduler.Hosts != nil {
		_ = s.scheduler.Hosts.MarkDispatchLeaseLost(recoveryCtx, owner.dispatchID, owner.handle.Fence(), time.Now().UTC())
	}
	if handler != nil {
		handler(recoveryCtx, RuntimeLeaseLoss{
			RunID: runID, DispatchID: owner.dispatchID, Handle: owner.handle,
			ErrorCode: runtimeLeaseLossErrorCode(renewErr),
		})
	}
}

func runtimeLeaseRenewInterval(ttl time.Duration) time.Duration {
	interval := ttl / 3
	if interval < 10*time.Millisecond {
		return 10 * time.Millisecond
	}
	return interval
}

func runtimeLeaseLossErrorCode(err error) string {
	if err == nil {
		return "STALE_FENCING_TOKEN"
	}
	switch err.Error() {
	case "STALE_FENCING_TOKEN", "DISTRIBUTED_LOCK_UNAVAILABLE", "RUNTIME_CAPACITY_UNAVAILABLE":
		return err.Error()
	default:
		return "RUNTIME_RUN_STALLED"
	}
}

func runtimeDispatchTerminal(state string) bool {
	return stringInRuntime(state, []string{"succeeded", "failed", "timeout", "aborted", "rejected", "orphaned"})
}
