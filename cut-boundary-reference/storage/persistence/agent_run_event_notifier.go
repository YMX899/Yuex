package persistence

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

type AgentRunEventFanoutConfig struct {
	Environment         string
	Channel             string
	Subscribe           bool
	PublishQueueSize    int
	PublishTimeout      time.Duration
	ReconnectBackoff    time.Duration
	RecoveryInterval    time.Duration
	RecoveryBatchSize   int
	SafetySweepInterval time.Duration
}

type AgentRunEventNotifierHealth struct {
	OK                bool
	Status            string
	Backend           string
	PublisherReady    bool
	SubscriberReady   bool
	RecoveryAvailable bool
}

type AgentRunEventNotifierMetrics struct {
	Backend              string
	SubscriberReady      bool
	Closed               bool
	ActiveSubscriptions  int
	LocalWakeups         uint64
	PublishedWakeups     uint64
	PublishFailures      uint64
	PublishDrops         uint64
	ReceivedWakeups      uint64
	RecoveryWakeups      uint64
	SubscriberReconnects uint64
	RunningWorkers       int64
}

type agentRunEventNotifierHealthProvider interface {
	Health(context.Context) (AgentRunEventNotifierHealth, error)
	Metrics() AgentRunEventNotifierMetrics
	Close() error
}

type agentRunEventWakeupEnvelope struct {
	Version          int    `json:"v"`
	Environment      string `json:"environment"`
	AgentRunID       string `json:"agentRunId"`
	SourceInstanceID string `json:"sourceInstanceId"`
}

type redisAgentRunEventNotifier struct {
	client     redis.UniversalClient
	local      *localAgentRunEventNotifier
	config     AgentRunEventFanoutConfig
	instanceID string

	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	closeOnce sync.Once
	closed    atomic.Bool

	publishQueue chan string
	pendingMu    sync.Mutex
	pending      map[string]bool

	recoveryTrigger   chan struct{}
	recoveryMu        sync.Mutex
	recoveryRemaining int

	subscriberReady      atomic.Bool
	localWakeups         atomic.Uint64
	publishedWakeups     atomic.Uint64
	publishFailures      atomic.Uint64
	publishDrops         atomic.Uint64
	receivedWakeups      atomic.Uint64
	recoveryWakeups      atomic.Uint64
	subscriberReconnects atomic.Uint64
	runningWorkers       atomic.Int64
}

func NewRedisAgentRunEventNotifier(addr, password string, db int, config AgentRunEventFanoutConfig) AgentRunEventNotifier {
	ctx, cancel := context.WithCancel(context.Background())
	notifier := &redisAgentRunEventNotifier{
		local:           &localAgentRunEventNotifier{subscribers: map[string]map[uint64]chan struct{}{}},
		config:          config,
		instanceID:      newAgentRunEventNotifierID(),
		ctx:             ctx,
		cancel:          cancel,
		publishQueue:    make(chan string, maxInt(config.PublishQueueSize, 1)),
		pending:         map[string]bool{},
		recoveryTrigger: make(chan struct{}, 1),
	}
	if strings.TrimSpace(addr) == "" || validateAgentRunEventFanoutConfig(config) != nil {
		return notifier
	}
	notifier.client = redis.NewClient(&redis.Options{Addr: strings.TrimSpace(addr), Password: password, DB: db})
	notifier.startWorker(notifier.publishLoop)
	if config.Subscribe {
		notifier.startWorker(notifier.subscribeLoop)
		notifier.startWorker(notifier.recoveryLoop)
	}
	return notifier
}

func (n *redisAgentRunEventNotifier) Subscribe(agentRunID string) (<-chan struct{}, func()) {
	return n.local.Subscribe(agentRunID)
}

func (n *redisAgentRunEventNotifier) Notify(agentRunID string) {
	agentRunID = strings.TrimSpace(agentRunID)
	if agentRunID == "" {
		return
	}
	n.local.Notify(agentRunID)
	n.localWakeups.Add(1)
	if n.closed.Load() || n.client == nil {
		n.publishFailures.Add(1)
		n.triggerRecovery()
		return
	}
	n.pendingMu.Lock()
	if n.pending[agentRunID] {
		n.pendingMu.Unlock()
		return
	}
	n.pending[agentRunID] = true
	n.pendingMu.Unlock()
	select {
	case n.publishQueue <- agentRunID:
	default:
		n.pendingMu.Lock()
		delete(n.pending, agentRunID)
		n.pendingMu.Unlock()
		n.publishDrops.Add(1)
		n.triggerRecovery()
	}
}

func (n *redisAgentRunEventNotifier) ActiveSubscriptions(agentRunID string) int {
	return n.local.ActiveSubscriptions(agentRunID)
}

func (n *redisAgentRunEventNotifier) Health(ctx context.Context) (AgentRunEventNotifierHealth, error) {
	health := AgentRunEventNotifierHealth{Backend: "redis_pubsub", Status: "unavailable", RecoveryAvailable: n.config.Subscribe && n.config.RecoveryBatchSize > 0 && n.config.RecoveryInterval > 0}
	if n == nil || n.client == nil || validateAgentRunEventFanoutConfig(n.config) != nil || n.closed.Load() {
		return health, fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
	}
	if err := n.client.Ping(ctx).Err(); err != nil {
		return health, fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
	}
	health.PublisherReady = true
	if !n.config.Subscribe {
		// Worker instances only publish. Ping alone does not prove that their
		// Redis ACL permits the Pub/Sub operation needed to wake API streams.
		probeID := "fanout-health-" + newAgentRunEventNotifierID()
		if err := n.publishWakeup(ctx, probeID); err != nil {
			return health, fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
		}
		health.OK = true
		health.Status = "ok"
		return health, nil
	}
	if !n.subscriberReady.Load() {
		return health, fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
	}
	health.SubscriberReady = true
	probeID := "fanout-health-" + newAgentRunEventNotifierID()
	wakeup, unsubscribe := n.local.Subscribe(probeID)
	defer unsubscribe()
	if err := n.publishWakeup(ctx, probeID); err != nil {
		return health, fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
	}
	select {
	case <-ctx.Done():
		return health, fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
	case <-wakeup:
		health.OK = true
		health.Status = "ok"
		return health, nil
	}
}

func (n *redisAgentRunEventNotifier) Metrics() AgentRunEventNotifierMetrics {
	backend := "unavailable"
	if n != nil && n.client != nil {
		backend = "redis_pubsub"
	}
	if n == nil {
		return AgentRunEventNotifierMetrics{Backend: backend}
	}
	return AgentRunEventNotifierMetrics{
		Backend:              backend,
		SubscriberReady:      n.subscriberReady.Load(),
		Closed:               n.closed.Load(),
		ActiveSubscriptions:  n.local.TotalSubscriptions(),
		LocalWakeups:         n.localWakeups.Load(),
		PublishedWakeups:     n.publishedWakeups.Load(),
		PublishFailures:      n.publishFailures.Load(),
		PublishDrops:         n.publishDrops.Load(),
		ReceivedWakeups:      n.receivedWakeups.Load(),
		RecoveryWakeups:      n.recoveryWakeups.Load(),
		SubscriberReconnects: n.subscriberReconnects.Load(),
		RunningWorkers:       n.runningWorkers.Load(),
	}
}

func (n *redisAgentRunEventNotifier) Close() error {
	if n == nil {
		return nil
	}
	var closeErr error
	n.closeOnce.Do(func() {
		n.closed.Store(true)
		n.subscriberReady.Store(false)
		n.cancel()
		if n.client != nil {
			closeErr = n.client.Close()
		}
		n.wg.Wait()
	})
	return closeErr
}

func (n *redisAgentRunEventNotifier) startWorker(worker func()) {
	n.wg.Add(1)
	n.runningWorkers.Add(1)
	go func() {
		defer n.wg.Done()
		defer n.runningWorkers.Add(-1)
		worker()
	}()
}

func (n *redisAgentRunEventNotifier) publishLoop() {
	for {
		select {
		case <-n.ctx.Done():
			return
		case agentRunID := <-n.publishQueue:
			n.pendingMu.Lock()
			delete(n.pending, agentRunID)
			n.pendingMu.Unlock()
			publishCtx, cancel := context.WithTimeout(n.ctx, n.config.PublishTimeout)
			err := n.publishWakeup(publishCtx, agentRunID)
			cancel()
			if err != nil {
				n.publishFailures.Add(1)
				n.triggerRecovery()
				continue
			}
			n.publishedWakeups.Add(1)
		}
	}
}

func (n *redisAgentRunEventNotifier) subscribeLoop() {
	for {
		if n.ctx.Err() != nil {
			return
		}
		pubsub := n.client.Subscribe(n.ctx, n.config.Channel)
		if _, err := pubsub.Receive(n.ctx); err != nil {
			_ = pubsub.Close()
			n.subscriberReady.Store(false)
			n.subscriberReconnects.Add(1)
			n.triggerRecovery()
			if !n.waitReconnect() {
				return
			}
			continue
		}
		n.subscriberReady.Store(true)
		n.triggerRecovery()
		for {
			message, err := pubsub.ReceiveMessage(n.ctx)
			if err != nil {
				_ = pubsub.Close()
				n.subscriberReady.Store(false)
				n.subscriberReconnects.Add(1)
				n.triggerRecovery()
				break
			}
			var envelope agentRunEventWakeupEnvelope
			if json.Unmarshal([]byte(message.Payload), &envelope) != nil || envelope.Version != 1 || envelope.Environment != n.config.Environment || !validAgentRunEventWakeupID(envelope.AgentRunID) {
				continue
			}
			n.receivedWakeups.Add(1)
			n.local.Notify(envelope.AgentRunID)
		}
		if !n.waitReconnect() {
			return
		}
	}
}

func (n *redisAgentRunEventNotifier) recoveryLoop() {
	recoveryTicker := time.NewTicker(n.config.RecoveryInterval)
	safetyTicker := time.NewTicker(n.config.SafetySweepInterval)
	defer recoveryTicker.Stop()
	defer safetyTicker.Stop()
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-n.recoveryTrigger:
			n.beginRecovery()
		case <-safetyTicker.C:
			n.beginRecovery()
		case <-recoveryTicker.C:
			n.recoverBatch()
		}
	}
}

func (n *redisAgentRunEventNotifier) publishWakeup(ctx context.Context, agentRunID string) error {
	if n.client == nil || !validAgentRunEventWakeupID(agentRunID) {
		return fmt.Errorf("RUNTIME_CAPACITY_UNAVAILABLE")
	}
	payload, err := json.Marshal(agentRunEventWakeupEnvelope{Version: 1, Environment: n.config.Environment, AgentRunID: agentRunID, SourceInstanceID: n.instanceID})
	if err != nil {
		return err
	}
	return n.client.Publish(ctx, n.config.Channel, payload).Err()
}

func (n *redisAgentRunEventNotifier) waitReconnect() bool {
	timer := time.NewTimer(n.config.ReconnectBackoff)
	defer timer.Stop()
	select {
	case <-n.ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (n *redisAgentRunEventNotifier) triggerRecovery() {
	if !n.config.Subscribe {
		return
	}
	select {
	case n.recoveryTrigger <- struct{}{}:
	default:
	}
}

func (n *redisAgentRunEventNotifier) beginRecovery() {
	total := n.local.TotalSubscriptions()
	n.recoveryMu.Lock()
	if total > n.recoveryRemaining {
		n.recoveryRemaining = total
	}
	n.recoveryMu.Unlock()
}

func (n *redisAgentRunEventNotifier) recoverBatch() {
	total := n.local.TotalSubscriptions()
	n.recoveryMu.Lock()
	if n.recoveryRemaining <= 0 {
		if n.subscriberReady.Load() || total == 0 {
			n.recoveryMu.Unlock()
			return
		}
		n.recoveryRemaining = total
	}
	limit := minInt(n.config.RecoveryBatchSize, n.recoveryRemaining)
	n.recoveryMu.Unlock()
	attempted, _ := n.local.NotifyRecoveryBatch(limit)
	if attempted > 0 {
		n.recoveryWakeups.Add(uint64(attempted))
	}
	n.recoveryMu.Lock()
	n.recoveryRemaining -= attempted
	if attempted == 0 {
		n.recoveryRemaining = 0
	}
	n.recoveryMu.Unlock()
}

func (n *localAgentRunEventNotifier) NotifyRecoveryBatch(limit int) (int, int) {
	if n == nil || limit <= 0 {
		return 0, 0
	}
	type subscriberRef struct {
		runID string
		id    uint64
		ch    chan struct{}
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	refs := []subscriberRef{}
	for runID, subscribers := range n.subscribers {
		for id, wakeup := range subscribers {
			refs = append(refs, subscriberRef{runID: runID, id: id, ch: wakeup})
		}
	}
	sort.Slice(refs, func(left, right int) bool {
		if refs[left].runID == refs[right].runID {
			return refs[left].id < refs[right].id
		}
		return refs[left].runID < refs[right].runID
	})
	if len(refs) == 0 {
		return 0, 0
	}
	attempts := minInt(limit, len(refs))
	start := int(n.recoveryCursor % uint64(len(refs)))
	for offset := 0; offset < attempts; offset++ {
		ref := refs[(start+offset)%len(refs)]
		select {
		case ref.ch <- struct{}{}:
		default:
		}
	}
	n.recoveryCursor = uint64((start + attempts) % len(refs))
	return attempts, len(refs)
}

func (n *localAgentRunEventNotifier) TotalSubscriptions() int {
	if n == nil {
		return 0
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	total := 0
	for _, subscribers := range n.subscribers {
		total += len(subscribers)
	}
	return total
}

func (n *localAgentRunEventNotifier) Health(context.Context) (AgentRunEventNotifierHealth, error) {
	return AgentRunEventNotifierHealth{OK: true, Status: "test_only", Backend: "local_test_only", PublisherReady: true, SubscriberReady: true, RecoveryAvailable: false}, nil
}

func (n *localAgentRunEventNotifier) Metrics() AgentRunEventNotifierMetrics {
	return AgentRunEventNotifierMetrics{Backend: "local_test_only", SubscriberReady: true, ActiveSubscriptions: n.TotalSubscriptions()}
}

func (n *localAgentRunEventNotifier) Close() error { return nil }

func validateAgentRunEventFanoutConfig(config AgentRunEventFanoutConfig) error {
	if strings.TrimSpace(config.Environment) == "" || strings.TrimSpace(config.Channel) == "" || len(config.Channel) > 128 ||
		config.PublishQueueSize <= 0 || config.PublishTimeout <= 0 || config.ReconnectBackoff <= 0 ||
		config.RecoveryInterval <= 0 || config.RecoveryBatchSize <= 0 || config.SafetySweepInterval <= 0 {
		return fmt.Errorf("RUNTIME_TRANSPORT_CONFIG_INVALID")
	}
	return nil
}

func validAgentRunEventWakeupID(agentRunID string) bool {
	agentRunID = strings.TrimSpace(agentRunID)
	return agentRunID != "" && len(agentRunID) <= 256 && !strings.ContainsAny(agentRunID, "\r\n\x00")
}

func newAgentRunEventNotifierID() string {
	buffer := make([]byte, 12)
	if _, err := rand.Read(buffer); err != nil {
		return fmt.Sprintf("fallback-%d", time.Now().UTC().UnixNano())
	}
	return hex.EncodeToString(buffer)
}

func minInt(left, right int) int {
	if left < right {
		return left
	}
	return right
}

func maxInt(left, right int) int {
	if left > right {
		return left
	}
	return right
}
