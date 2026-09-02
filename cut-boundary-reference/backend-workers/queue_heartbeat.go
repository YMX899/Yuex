package workers

import (
	"context"
	"sync"
	"time"

	"huahuoai/backend/source/internal/persistence"
)

type queueRepositoryHeartbeat struct {
	cancel     context.CancelFunc
	done       chan struct{}
	stop       chan struct{}
	stopOnce   sync.Once
	cancelOnce sync.Once
	mu         sync.RWMutex
	proof      persistence.QueueLeaseProof
	err        error
}

func startQueueRepositoryHeartbeat(ctx context.Context, repo *persistence.QueueRepository, proof persistence.QueueLeaseProof, leaseTTL time.Duration) (context.Context, *queueRepositoryHeartbeat) {
	if ctx == nil {
		ctx = context.Background()
	}
	leaseCtx, cancel := context.WithCancel(ctx)
	heartbeat := &queueRepositoryHeartbeat{cancel: cancel, done: make(chan struct{}), stop: make(chan struct{}), proof: proof}
	if repo == nil || proof.QueueID == "" {
		close(heartbeat.done)
		return leaseCtx, heartbeat
	}
	if leaseTTL <= 0 {
		leaseTTL = time.Minute
	}
	interval := leaseTTL / 3
	if interval < 10*time.Millisecond {
		interval = 10 * time.Millisecond
	}
	if interval > 10*time.Second {
		interval = 10 * time.Second
	}
	refresh := func() bool {
		heartbeat.mu.RLock()
		current := heartbeat.proof
		heartbeat.mu.RUnlock()
		refreshed, err := repo.Heartbeat(leaseCtx, current, leaseTTL)
		if err != nil {
			heartbeat.mu.Lock()
			heartbeat.err = err
			heartbeat.mu.Unlock()
			cancel()
			return false
		}
		heartbeat.mu.Lock()
		heartbeat.proof = refreshed
		heartbeat.mu.Unlock()
		return true
	}
	if !refresh() {
		close(heartbeat.done)
		return leaseCtx, heartbeat
	}
	go func() {
		defer close(heartbeat.done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-heartbeat.stop:
				return
			case <-leaseCtx.Done():
				return
			case <-ticker.C:
				if !refresh() {
					return
				}
			}
		}
	}()
	return leaseCtx, heartbeat
}

func (h *queueRepositoryHeartbeat) Stop() (persistence.QueueLeaseProof, error) {
	if h == nil {
		return persistence.QueueLeaseProof{}, nil
	}
	h.cancelOnce.Do(h.cancel)
	h.stopOnce.Do(func() { close(h.stop) })
	<-h.done
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.proof, h.err
}

// Freeze stops renewal without cancelling the caller's work context. Terminal
// convergence uses it immediately before the final queue completion so its
// final persistence reads can still finish after the proof is frozen.
func (h *queueRepositoryHeartbeat) Freeze() (persistence.QueueLeaseProof, error) {
	if h == nil {
		return persistence.QueueLeaseProof{}, nil
	}
	h.stopOnce.Do(func() { close(h.stop) })
	<-h.done
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.proof, h.err
}

func (h *queueRepositoryHeartbeat) Proof() persistence.QueueLeaseProof {
	if h == nil {
		return persistence.QueueLeaseProof{}
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.proof
}
