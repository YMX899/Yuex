package workers

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"huahuoai/backend/source/internal/domain"
	"huahuoai/backend/source/internal/persistence"
	"huahuoai/backend/source/internal/queue"
	runtimepkg "huahuoai/backend/source/internal/runtime"
	"huahuoai/backend/source/internal/services"
)

func TestRuntimeSessionRequiredForExecutionScopeUsesFrozenPlanScope(t *testing.T) {
	for name, fixture := range map[string]struct {
		scope string
		want  bool
		code  string
	}{
		"product_thread": {scope: string(runtimepkg.ScopeProductThread), want: true},
		"detached_task":  {scope: string(runtimepkg.ScopeDetachedTask), want: false},
		"unknown":        {scope: "legacy_scene_adapter", code: "AGENT_PLAN_INVALID"},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := runtimeSessionRequiredForExecutionScope(fixture.scope)
			if fixture.code != "" {
				if err == nil || err.Error() != fixture.code {
					t.Fatalf("scope=%q error=%v want %s", fixture.scope, err, fixture.code)
				}
				return
			}
			if err != nil || got != fixture.want {
				t.Fatalf("scope=%q sessionRequired=%t err=%v want=%t", fixture.scope, got, err, fixture.want)
			}
		})
	}
}

func TestRuntimeEventPayloadForStorageUsesStrictToolAuditBoundary(t *testing.T) {
	hash := func(character string) string { return "sha256:" + strings.Repeat(character, 64) }
	source := runtimepkg.AsyncRuntimeEvent{
		EventType: "tool.call.started", Status: "running",
		SafePayload: map[string]any{
			"schemaVersion": "huahuo.runtime-tool-execution-event.v1",
			"toolName":      "workspace_search",
			"toolCallHash":  hash("a"),
			"argsHash":      hash("b"),
			"outcome":       "started",
			"durationMs":    0,
			"bytes":         0,
			"call":          1,
			"repeat":        1,
		},
	}
	safe, usage, err := runtimeEventPayloadForStorage(source)
	if err != nil {
		t.Fatalf("normalize tool receipt: %v", err)
	}
	if safe["status"] != nil || len(usage) != 0 || safe["toolName"] != "workspace_search" {
		t.Fatalf("tool receipt payload=%#v usage=%#v", safe, usage)
	}

	source.SafePayload["query"] = "must-not-persist"
	if _, _, err := runtimeEventPayloadForStorage(source); err == nil || err.Error() != "RUNTIME_EVENT_GAP" {
		t.Fatalf("raw query error=%v, want RUNTIME_EVENT_GAP", err)
	}
	delete(source.SafePayload, "query")
	source.UsageDelta = map[string]any{"content": "must-not-persist"}
	if _, _, err := runtimeEventPayloadForStorage(source); err == nil || err.Error() != "RUNTIME_EVENT_GAP" {
		t.Fatalf("tool usage payload error=%v, want RUNTIME_EVENT_GAP", err)
	}
}

func TestRuntimeEventPayloadForStorageProjectsOnlyAssistantDraftFields(t *testing.T) {
	source := runtimepkg.AsyncRuntimeEvent{
		EventType: "assistant.delta",
		Status:    "accepted",
		Data: map[string]any{
			"deltaText": "streamed answer",
			"replace":   true,
			"internal":  "must-not-persist",
		},
		SafePayload: map[string]any{"provider": "must-not-persist"},
		UsageDelta:  map[string]any{"modelTokens": 9},
	}
	eventType, visibility := runtimeEventStorageRoute(source.EventType)
	safe, usage, err := runtimeEventPayloadForStorage(source)
	if err != nil {
		t.Fatal(err)
	}
	if eventType != "draft_delta" || visibility != "app_safe" {
		t.Fatalf("route type=%q visibility=%q", eventType, visibility)
	}
	if len(safe) != 3 || safe["deltaText"] != "streamed answer" || safe["replace"] != true || safe["status"] != "running" {
		t.Fatalf("draft payload=%#v", safe)
	}
	if len(usage) != 0 {
		t.Fatalf("draft usage=%#v", usage)
	}
}

func TestNormalizeUserCancellationTimeoutPreservesOrdinaryTimeouts(t *testing.T) {
	now := time.Now().UTC()
	for name, fixture := range map[string]struct {
		run        persistence.AgentRunRecord
		runtime    runtimepkg.AsyncRuntimeStatus
		wantStatus string
		wantCode   string
	}{
		"no durable cancellation intent": {
			run:        persistence.AgentRunRecord{Status: "running"},
			runtime:    runtimepkg.AsyncRuntimeStatus{Status: "timeout", Error: map[string]any{"code": "RUNTIME_TIMEOUT"}},
			wantStatus: "timeout",
			wantCode:   "RUNTIME_TIMEOUT",
		},
		"cancellation intent without explicit runtime cancellation marker": {
			run: persistence.AgentRunRecord{
				Status: "aborting", CancelRequestedAt: &now, CancelReasonCode: "USER_CANCELLED",
			},
			runtime:    runtimepkg.AsyncRuntimeStatus{Status: "timeout", Error: map[string]any{"code": "RUNTIME_TIMEOUT"}},
			wantStatus: "timeout",
			wantCode:   "RUNTIME_TIMEOUT",
		},
		"explicit runtime cancellation marker": {
			run: persistence.AgentRunRecord{
				Status: "aborting", CancelRequestedAt: &now, CancelReasonCode: "USER_CANCELLED",
			},
			runtime:    runtimepkg.AsyncRuntimeStatus{Status: "timeout", Result: map[string]any{"must": "not survive"}, Error: map[string]any{"code": "RUNTIME_ABORTED"}},
			wantStatus: "aborted",
			wantCode:   "RUNTIME_ABORTED",
		},
	} {
		t.Run(name, func(t *testing.T) {
			got := normalizeUserCancellationTimeout(fixture.run, fixture.runtime)
			if got.Status != fixture.wantStatus || workerMapString(got.Error, "code") != fixture.wantCode {
				t.Fatalf("normalized status=%q error=%#v, want status=%q code=%q", got.Status, got.Error, fixture.wantStatus, fixture.wantCode)
			}
			if fixture.wantStatus == "aborted" && got.Result != nil {
				t.Fatalf("explicit cancellation must drop runtime result: %#v", got.Result)
			}
		})
	}
}

func TestLegacyTerminalRecoveryCapacityBypassRequiresSettledHistoricalFacts(t *testing.T) {
	recovery := runtimepkg.TerminalConvergenceRecovery{
		RunID: "run_legacy", DispatchID: "dispatch_legacy", UsageSettled: true,
	}
	dispatch := runtimepkg.RuntimeDispatch{DispatchID: recovery.DispatchID, RunID: recovery.RunID, State: "succeeded"}
	run := persistence.AgentRunRecord{AgentRunID: recovery.RunID, Status: "succeeded"}
	if !legacyTerminalRecoveryMayCompleteWithoutCapacity(recovery, dispatch, run) {
		t.Fatal("settled terminal legacy row must be eligible only for remaining convergence steps")
	}
	for name, mutate := range map[string]func(*runtimepkg.TerminalConvergenceRecovery, *runtimepkg.RuntimeDispatch, *persistence.AgentRunRecord){
		"usage not settled": func(recovery *runtimepkg.TerminalConvergenceRecovery, _ *runtimepkg.RuntimeDispatch, _ *persistence.AgentRunRecord) {
			recovery.UsageSettled = false
		},
		"bound dispatch": func(_ *runtimepkg.TerminalConvergenceRecovery, dispatch *runtimepkg.RuntimeDispatch, _ *persistence.AgentRunRecord) {
			dispatch.CapacityReservationID, dispatch.CapacityReservedVersion = "capacity_new", 1
		},
		"half-bound dispatch id only": func(_ *runtimepkg.TerminalConvergenceRecovery, dispatch *runtimepkg.RuntimeDispatch, _ *persistence.AgentRunRecord) {
			dispatch.CapacityReservationID = "capacity_partial"
		},
		"half-bound dispatch version only": func(_ *runtimepkg.TerminalConvergenceRecovery, dispatch *runtimepkg.RuntimeDispatch, _ *persistence.AgentRunRecord) {
			dispatch.CapacityReservedVersion = 1
		},
		"snapshot has capacity": func(recovery *runtimepkg.TerminalConvergenceRecovery, _ *runtimepkg.RuntimeDispatch, _ *persistence.AgentRunRecord) {
			recovery.CapacityReservationID, recovery.CapacitySnapshotVersion, recovery.CapacityReservedVersion = "capacity_old", 1, 1
		},
		"snapshot has capacity id only": func(recovery *runtimepkg.TerminalConvergenceRecovery, _ *runtimepkg.RuntimeDispatch, _ *persistence.AgentRunRecord) {
			recovery.CapacityReservationID = "capacity_partial"
		},
		"snapshot has capacity generation only": func(recovery *runtimepkg.TerminalConvergenceRecovery, _ *runtimepkg.RuntimeDispatch, _ *persistence.AgentRunRecord) {
			recovery.CapacitySnapshotVersion = 1
		},
		"snapshot has reserved generation only": func(recovery *runtimepkg.TerminalConvergenceRecovery, _ *runtimepkg.RuntimeDispatch, _ *persistence.AgentRunRecord) {
			recovery.CapacityReservedVersion = 1
		},
		"active run": func(_ *runtimepkg.TerminalConvergenceRecovery, _ *runtimepkg.RuntimeDispatch, run *persistence.AgentRunRecord) {
			run.Status = "running"
		},
		"active dispatch": func(_ *runtimepkg.TerminalConvergenceRecovery, dispatch *runtimepkg.RuntimeDispatch, _ *persistence.AgentRunRecord) {
			dispatch.State = "running"
		},
	} {
		t.Run(name, func(t *testing.T) {
			candidateRecovery, candidateDispatch, candidateRun := recovery, dispatch, run
			mutate(&candidateRecovery, &candidateDispatch, &candidateRun)
			if legacyTerminalRecoveryMayCompleteWithoutCapacity(candidateRecovery, candidateDispatch, candidateRun) {
				t.Fatalf("%s must retain the normal exact-capacity fence", name)
			}
		})
	}
}

func TestLegacyTerminalRecoveryUnsafeCapacityContractBlocksOnlyContradictoryLegacySnapshots(t *testing.T) {
	legacyDispatch := runtimepkg.RuntimeDispatch{DispatchID: "dispatch_legacy", RunID: "run_legacy"}
	for name, fixture := range map[string]struct {
		recovery runtimepkg.TerminalConvergenceRecovery
		dispatch runtimepkg.RuntimeDispatch
		want     bool
	}{
		"legacy unbound with capacity id": {
			recovery: runtimepkg.TerminalConvergenceRecovery{CapacityReservationID: "capacity_legacy"}, dispatch: legacyDispatch, want: true,
		},
		"legacy unbound with snapshot generation": {
			recovery: runtimepkg.TerminalConvergenceRecovery{CapacitySnapshotVersion: 1}, dispatch: legacyDispatch, want: true,
		},
		"legacy unbound with reserved generation": {
			recovery: runtimepkg.TerminalConvergenceRecovery{CapacityReservedVersion: 1}, dispatch: legacyDispatch, want: true,
		},
		"ordinary capacity miss has no snapshot capacity": {
			recovery: runtimepkg.TerminalConvergenceRecovery{}, dispatch: legacyDispatch, want: false,
		},
		"normally bound dispatch is not a legacy contract blocker": {
			recovery: runtimepkg.TerminalConvergenceRecovery{CapacityReservationID: "capacity_bound", CapacitySnapshotVersion: 1, CapacityReservedVersion: 1},
			dispatch: runtimepkg.RuntimeDispatch{DispatchID: legacyDispatch.DispatchID, RunID: legacyDispatch.RunID, CapacityReservationID: "capacity_bound", CapacityReservedVersion: 1},
			want:     false,
		},
		"half-bound dispatch id only is not a legacy contract blocker": {
			recovery: runtimepkg.TerminalConvergenceRecovery{CapacityReservationID: "capacity_partial"},
			dispatch: runtimepkg.RuntimeDispatch{DispatchID: legacyDispatch.DispatchID, RunID: legacyDispatch.RunID, CapacityReservationID: "capacity_partial"},
			want:     false,
		},
		"half-bound dispatch generation only is not a legacy contract blocker": {
			recovery: runtimepkg.TerminalConvergenceRecovery{CapacityReservedVersion: 1},
			dispatch: runtimepkg.RuntimeDispatch{DispatchID: legacyDispatch.DispatchID, RunID: legacyDispatch.RunID, CapacityReservedVersion: 1},
			want:     false,
		},
	} {
		t.Run(name, func(t *testing.T) {
			if got := legacyTerminalRecoveryHasUnsafeCapacityContract(fixture.recovery, fixture.dispatch); got != fixture.want {
				t.Fatalf("legacy unsafe-capacity blocker=%t want=%t recovery=%+v dispatch=%+v", got, fixture.want, fixture.recovery, fixture.dispatch)
			}
		})
	}
}

func TestRuntimeTerminalPublicResultHuokeExposesOnlyVisibleReply(t *testing.T) {
	envelope, err := json.Marshal(map[string]any{
		"schemaVersion": "huoke_topic_strategy.result.v1",
		"taskType":      huokeTopicTaskType,
		"skillProfile":  huokeTopicSkillProfile,
		"status":        "succeeded",
		"data": map[string]any{
			"reply": "最终建议选题：新手第一次进健身房，先做哪三件事",
			"consultationStatePatch": map[string]any{
				"schemaVersion":    runtimepkg.HuokeTopicStatePatchSchemaVersion,
				"baseStateVersion": 1,
				"stateVersion":     2,
				"patch":            map[string]any{"currentSubjectId": "final_topic_guidance"},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	raw := map[string]any{
		"runId":       "run_huoke_public",
		"status":      "succeeded",
		"queue":       map[string]any{"status": "done"},
		"logs":        []any{string(envelope)},
		"finalAnswer": string(envelope),
	}
	run := persistence.AgentRunRecord{
		AgentRunID: "run_huoke_public", TaskID: "task_huoke_public",
	}
	public := runtimeTerminalPublicResult(run, runtimeEventTestTerminalPlan(t, run.AgentRunID, huokeTopicTaskType), raw)
	if public["finalAnswer"] != "最终建议选题：新手第一次进健身房，先做哪三件事" {
		t.Fatalf("public result did not expose only data.reply: %#v", public)
	}
	encoded, _ := json.Marshal(public)
	for _, forbidden := range []string{"consultationState", "moduleLedger", "strategyAssessments", "logs"} {
		if strings.Contains(string(encoded), forbidden) {
			t.Fatalf("public Huoke result leaked %q: %s", forbidden, encoded)
		}
	}
}

func runtimeEventTestTerminalPlan(t *testing.T, runID, taskType string) runtimepkg.AgentRunPlan {
	t.Helper()
	profile, err := runtimepkg.NewAgentProfileResolver().Resolve(taskType)
	if err != nil {
		t.Fatalf("resolve test output profile: %v", err)
	}
	plan, err := runtimepkg.NewCapabilityPlanner().BuildDeterministicPlan(domain.TaskIntent{
		AgentRunID: runID, ResolvedTaskType: taskType, ExecutionScope: string(profile.ExecutionScope),
		Category: "test", ExpectedOutput: "text",
	}, runtimepkg.L1AgentRouteResult{
		AgentProfile: profile.AgentProfile, AgentHash: "sha256:test-agent", ManifestVersion: "test-manifest",
	})
	if err != nil {
		t.Fatalf("build test terminal plan: %v", err)
	}
	return plan
}

type runtimeEventPageClient struct {
	page runtimepkg.AsyncRuntimeEventPage
}

func (c runtimeEventPageClient) Submit(context.Context, runtimepkg.RuntimeHost, runtimepkg.AsyncRuntimeSubmitRequest) (runtimepkg.AsyncRuntimeSubmitResult, error) {
	return runtimepkg.AsyncRuntimeSubmitResult{}, nil
}

func (c runtimeEventPageClient) GetStatus(context.Context, runtimepkg.RuntimeHost, string, string) (runtimepkg.AsyncRuntimeStatus, error) {
	return runtimepkg.AsyncRuntimeStatus{}, nil
}

func (c runtimeEventPageClient) ListEvents(context.Context, runtimepkg.RuntimeHost, string, string, int64, int, int) (runtimepkg.AsyncRuntimeEventPage, error) {
	return c.page, nil
}

func (c runtimeEventPageClient) AbortAsync(context.Context, runtimepkg.RuntimeHost, runtimepkg.AsyncRuntimeAbortRequest) (runtimepkg.AsyncRuntimeAbortResult, error) {
	return runtimepkg.AsyncRuntimeAbortResult{}, nil
}

type runtimeEventRecordingClient struct {
	page          runtimepkg.AsyncRuntimeEventPage
	calls         int
	afterSequence int64
	limit         int
	waitMs        int
}

func (c *runtimeEventRecordingClient) Submit(context.Context, runtimepkg.RuntimeHost, runtimepkg.AsyncRuntimeSubmitRequest) (runtimepkg.AsyncRuntimeSubmitResult, error) {
	return runtimepkg.AsyncRuntimeSubmitResult{}, nil
}

func (c *runtimeEventRecordingClient) GetStatus(context.Context, runtimepkg.RuntimeHost, string, string) (runtimepkg.AsyncRuntimeStatus, error) {
	return runtimepkg.AsyncRuntimeStatus{}, nil
}

func (c *runtimeEventRecordingClient) ListEvents(_ context.Context, _ runtimepkg.RuntimeHost, _, _ string, afterSequence int64, limit, waitMs int) (runtimepkg.AsyncRuntimeEventPage, error) {
	c.calls++
	c.afterSequence = afterSequence
	c.limit = limit
	c.waitMs = waitMs
	return c.page, nil
}

func (c *runtimeEventRecordingClient) AbortAsync(context.Context, runtimepkg.RuntimeHost, runtimepkg.AsyncRuntimeAbortRequest) (runtimepkg.AsyncRuntimeAbortResult, error) {
	return runtimepkg.AsyncRuntimeAbortResult{}, nil
}

type runtimeEventCountingClient struct {
	listEventsCalls int
	getStatusCalls  int
}

func (c *runtimeEventCountingClient) Submit(context.Context, runtimepkg.RuntimeHost, runtimepkg.AsyncRuntimeSubmitRequest) (runtimepkg.AsyncRuntimeSubmitResult, error) {
	return runtimepkg.AsyncRuntimeSubmitResult{}, nil
}

func (c *runtimeEventCountingClient) GetStatus(context.Context, runtimepkg.RuntimeHost, string, string) (runtimepkg.AsyncRuntimeStatus, error) {
	c.getStatusCalls++
	return runtimepkg.AsyncRuntimeStatus{}, context.DeadlineExceeded
}

func (c *runtimeEventCountingClient) ListEvents(context.Context, runtimepkg.RuntimeHost, string, string, int64, int, int) (runtimepkg.AsyncRuntimeEventPage, error) {
	c.listEventsCalls++
	return runtimepkg.AsyncRuntimeEventPage{}, context.DeadlineExceeded
}

func (c *runtimeEventCountingClient) AbortAsync(context.Context, runtimepkg.RuntimeHost, runtimepkg.AsyncRuntimeAbortRequest) (runtimepkg.AsyncRuntimeAbortResult, error) {
	return runtimepkg.AsyncRuntimeAbortResult{}, nil
}

func TestRuntimeEventWorkerCompletesTerminalDispatchWithoutRuntimePolling(t *testing.T) {
	for _, state := range []string{"succeeded", "failed", "timeout", "aborted", "rejected", "orphaned"} {
		t.Run(state, func(t *testing.T) {
			ctx := context.Background()
			repos, hosts, queueRecord, proof := runtimeEventTerminalDispatchFixture(t, state)
			client := &runtimeEventCountingClient{}
			worker := NewRuntimeEventWorker(repos, hosts, nil, client, "terminal-dispatch-ticket")

			result := worker.Process(ctx, queueRecord, proof)
			if result["status"] != state || result["terminalDispatch"] != true {
				t.Fatalf("terminal dispatch result=%#v", result)
			}
			if client.listEventsCalls != 0 || client.getStatusCalls != 0 {
				t.Fatalf("terminal dispatch must not poll Runtime: listEvents=%d getStatus=%d", client.listEventsCalls, client.getStatusCalls)
			}
			records := repos.Queue.ListQueueRecords(map[string]any{"queueId": workerMapString(queueRecord, "queueId")})
			if len(records) != 1 || records[0]["status"] != "succeeded" {
				t.Fatalf("terminal dispatch queue was not acknowledged: %#v", records)
			}
		})
	}
}

func TestRuntimeEventWorkerTerminalDispatchQueueAckFailsClosedOnStaleLease(t *testing.T) {
	ctx := context.Background()
	repos, hosts, queueRecord, proof := runtimeEventTerminalDispatchFixture(t, "orphaned")
	if _, err := repos.Queue.MarkRunning(ctx, proof); err != nil {
		t.Fatal(err)
	}
	_, heartbeat := startQueueRepositoryHeartbeat(ctx, repos.Queue, proof, time.Minute)
	heartbeat.mu.Lock()
	heartbeat.proof.FencingToken++
	heartbeat.mu.Unlock()

	worker := NewRuntimeEventWorker(repos, hosts, nil, &runtimeEventCountingClient{}, "terminal-dispatch-ticket")
	result := worker.completeTerminalDispatchQueue(ctx, heartbeat, workerMapString(queueRecord, "queueId"), workerMapString(queueRecord, "taskId"), "orphaned")
	if result["status"] != "aborted" || result["errorCode"] != "STALE_QUEUE_LEASE" {
		t.Fatalf("stale terminal dispatch acknowledgement=%#v", result)
	}
	records := repos.Queue.ListQueueRecords(map[string]any{"queueId": workerMapString(queueRecord, "queueId")})
	if len(records) != 1 || records[0]["status"] != "running" {
		t.Fatalf("stale proof must not acknowledge or retry queue: %#v", records)
	}
}

func runtimeEventTerminalDispatchFixture(t *testing.T, terminalState string) (*persistence.Repositories, *runtimepkg.RuntimeHostRepository, persistence.QueueRecord, persistence.QueueLeaseProof) {
	t.Helper()
	ctx := context.Background()
	repos := persistence.NewRepositories(nil)
	hosts := runtimepkg.NewRuntimeHostRepository(nil)
	runID := "run_terminal_dispatch_" + terminalState
	hostID := "host_terminal_dispatch_" + terminalState
	reservationID := "reservation_terminal_dispatch_" + terminalState
	dispatchID := "dispatch_terminal_dispatch_" + terminalState
	queueID := "runtime_events:" + dispatchID
	identity := runtimepkg.RuntimeHostIdentity{RuntimeHostID: hostID, InstanceID: "instance_terminal_dispatch", Environment: "test"}
	if _, err := hosts.RegisterHost(ctx, identity, runtimepkg.RuntimeHostRegistration{
		Endpoint: "http://" + hostID, Capabilities: runtimepkg.RuntimeCapabilitySnapshot{CapabilityHash: "cap-terminal-dispatch"}, MaxActiveRuns: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := hosts.HeartbeatHost(ctx, identity, runtimepkg.RuntimeHostHeartbeat{
		Sequence: 1, ObservedAt: time.Now().UTC(), CapabilityHash: "cap-terminal-dispatch", SignatureKeyID: "test-key",
	}); err != nil {
		t.Fatal(err)
	}
	reservation, _, err := hosts.TryReserveSlot(ctx, runtimepkg.AtomicReservationCommand{
		ReservationID: reservationID, RunID: runID, OwnerInstanceID: "terminal-dispatch-worker", ExecutionScope: "detached_task",
		CapabilityHash: "cap-terminal-dispatch", LeaseTokenHash: "sha256:terminal-dispatch", FencingToken: 1,
		ExpiresAt: time.Now().UTC().Add(time.Minute), HeartbeatAfter: time.Now().UTC().Add(-time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := hosts.CreateDispatch(ctx, runtimepkg.RuntimeDispatch{
		DispatchID: dispatchID, RunID: runID, ReservationID: reservationID, RuntimeHostID: hostID,
		DispatchAttempt: 1, PlanVersion: 1, FencingToken: reservation.FencingToken, RunTicketJTIHash: "sha256:terminal-dispatch-jti",
		TicketExpiresAt: time.Now().UTC().Add(time.Minute), InputManifestHash: "sha256:terminal-dispatch-manifest",
		OwnerInstanceID: "terminal-dispatch-worker", LeaseTokenHash: "sha256:terminal-dispatch", LeaseExpiresAt: time.Now().UTC().Add(time.Minute),
	}); err != nil {
		t.Fatal(err)
	}
	fence := runtimepkg.ReservationFence{
		ReservationID: reservationID, RuntimeHostID: hostID, OwnerInstanceID: "terminal-dispatch-worker", LeaseTokenHash: "sha256:terminal-dispatch", FencingToken: reservation.FencingToken,
	}
	if err := hosts.FinalizeDispatchAndReleaseSlot(ctx, runtimepkg.DispatchTerminalCommand{
		Fence: fence, DispatchID: dispatchID, TerminalStatus: terminalState, ErrorCode: "RUNTIME_RUN_STALLED",
	}); err != nil {
		t.Fatal(err)
	}
	repos.Queue.Enqueue(map[string]any{
		"queueId": queueID, "queueName": queue.QueueRuntimeEvents, "taskType": "runtime_event_ingest", "taskId": runID,
		"payload": map[string]any{"runId": runID, "dispatchId": dispatchID, "runtimeHostId": hostID},
	})
	record, proof, err := repos.Queue.Lease(ctx, queue.QueueRuntimeEvents, "terminal-dispatch-event-worker", time.Minute, "runtime_event_ingest")
	if err != nil {
		t.Fatal(err)
	}
	return repos, hosts, record, proof
}

func TestRuntimeEventWorkerRejectsExplicitAndRetainedRangeGaps(t *testing.T) {
	for name, page := range map[string]runtimepkg.AsyncRuntimeEventPage{
		"explicit":   {Gap: true, OldestAvailableSequence: 1, LatestSequence: 4},
		"retained":   {OldestAvailableSequence: 5, LatestSequence: 7},
		"incomplete": {OldestAvailableSequence: 1, LatestSequence: 3, NextAfterSequence: 1, HasMore: false},
	} {
		t.Run(name, func(t *testing.T) {
			worker := RuntimeEventWorker{Client: runtimeEventPageClient{page: page}}
			_, err := worker.ingestEventPages(context.Background(), runtimepkg.RuntimeHost{}, "run_1", "dispatch_1", "ticket", 1)
			if err == nil || !strings.Contains(err.Error(), "RUNTIME_EVENT_GAP") {
				t.Fatalf("error=%v, want RUNTIME_EVENT_GAP", err)
			}
		})
	}
}

func TestRuntimeEventWorkerIngestsDraftWithBoundedLongPollAndNotifies(t *testing.T) {
	ctx := context.Background()
	const (
		runID         = "run_streaming_draft"
		hostID        = "host_streaming_draft"
		reservationID = "reservation_streaming_draft"
		dispatchID    = "dispatch_streaming_draft"
	)
	repos := persistence.NewRepositories(nil)
	hosts := runtimepkg.NewRuntimeHostRepository(nil)
	identity := runtimepkg.RuntimeHostIdentity{RuntimeHostID: hostID, InstanceID: "instance_streaming_draft", Environment: "test"}
	if _, err := hosts.RegisterHost(ctx, identity, runtimepkg.RuntimeHostRegistration{
		Endpoint: "http://" + hostID, Capabilities: runtimepkg.RuntimeCapabilitySnapshot{CapabilityHash: "cap-streaming-draft"}, MaxActiveRuns: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := hosts.HeartbeatHost(ctx, identity, runtimepkg.RuntimeHostHeartbeat{
		Sequence: 1, ObservedAt: time.Now().UTC(), CapabilityHash: "cap-streaming-draft", SignatureKeyID: "test-key",
	}); err != nil {
		t.Fatal(err)
	}
	reservation, _, err := hosts.TryReserveSlot(ctx, runtimepkg.AtomicReservationCommand{
		ReservationID: reservationID, RunID: runID, OwnerInstanceID: "streaming-draft-worker", ExecutionScope: "detached_task",
		CapabilityHash: "cap-streaming-draft", LeaseTokenHash: "streaming-draft-lease", FencingToken: 1,
		ExpiresAt: time.Now().UTC().Add(time.Minute), HeartbeatAfter: time.Now().UTC().Add(-time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := hosts.CreateDispatch(ctx, runtimepkg.RuntimeDispatch{
		DispatchID: dispatchID, RunID: runID, ReservationID: reservationID, RuntimeHostID: hostID,
		DispatchAttempt: 1, PlanVersion: 1, FencingToken: reservation.FencingToken, RunTicketJTIHash: "streaming-draft-jti",
		TicketExpiresAt: time.Now().UTC().Add(time.Minute), InputManifestHash: "streaming-draft-manifest",
		OwnerInstanceID: "streaming-draft-worker", LeaseTokenHash: "streaming-draft-lease", LeaseExpiresAt: time.Now().UTC().Add(time.Minute),
	}); err != nil {
		t.Fatal(err)
	}

	wakeup, unsubscribe := repos.AgentRuns.SubscribePublicEvents(runID)
	defer unsubscribe()
	client := &runtimeEventRecordingClient{page: runtimepkg.AsyncRuntimeEventPage{
		Items: []runtimepkg.AsyncRuntimeEvent{{
			Sequence: 1, EventType: "assistant.delta", Timestamp: time.Now().UTC(),
			Data: map[string]any{"deltaText": "streamed answer", "replace": false, "internal": "drop"},
		}},
		OldestAvailableSequence: 1, LatestSequence: 1, NextAfterSequence: 1,
	}}
	worker := RuntimeEventWorker{Repos: repos, Hosts: hosts, Client: client}
	after, err := worker.ingestEventPages(ctx, runtimepkg.RuntimeHost{RuntimeHostID: hostID}, runID, dispatchID, "ticket", 0)
	if err != nil {
		t.Fatal(err)
	}
	if after != 1 || client.calls != 1 || client.afterSequence != 0 || client.limit != 500 || client.waitMs != 20000 {
		t.Fatalf("after=%d calls=%d request=(after=%d limit=%d waitMs=%d)", after, client.calls, client.afterSequence, client.limit, client.waitMs)
	}
	select {
	case <-wakeup:
	default:
		t.Fatal("committed draft event did not notify public subscribers")
	}
	events, err := hosts.ListRunEvents(ctx, runID, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].EventType != "draft_delta" || events[0].Visibility != "app_safe" ||
		events[0].SafePayload["deltaText"] != "streamed answer" || events[0].SafePayload["replace"] != false || events[0].SafePayload["status"] != "running" {
		t.Fatalf("stored draft events=%#v", events)
	}
}

func TestRuntimeEventWorkerTerminalQueueCompleterUsesLatestHeartbeatProof(t *testing.T) {
	ctx := context.Background()
	repos := persistence.NewRepositories(nil)
	const queueID = "runtime_events:terminal_queue_heartbeat"
	repos.Queue.Enqueue(map[string]any{
		"queueId": queueID, "queueName": queue.QueueRuntimeEvents, "taskType": "runtime_event_ingest", "taskId": "run_terminal_queue_heartbeat",
	})
	_, initialProof, err := repos.Queue.Lease(ctx, queue.QueueRuntimeEvents, "event-worker-terminal-queue", time.Minute, "runtime_event_ingest")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repos.Queue.MarkRunning(ctx, initialProof); err != nil {
		t.Fatal(err)
	}
	leaseCtx, heartbeat := startQueueRepositoryHeartbeat(ctx, repos.Queue, initialProof, time.Minute)
	defer heartbeat.Stop()

	// The completer owns the heartbeat until it reaches the final queue step,
	// where it freezes whichever proof is current and records completion.
	worker := RuntimeEventWorker{Repos: repos}
	if err := worker.terminalQueueCompleter(heartbeat)(ctx, runtimepkg.TerminalConvergenceCommand{}, "terminal:fresh-proof:1"); err != nil {
		t.Fatalf("terminal queue completion with latest proof: %v", err)
	}
	if err := leaseCtx.Err(); err != nil {
		t.Fatalf("final queue freeze cancelled convergence context: %v", err)
	}
	records := repos.Queue.ListQueueRecords(map[string]any{"queueId": queueID})
	if len(records) != 1 || records[0]["status"] != "succeeded" {
		t.Fatalf("terminal queue record=%#v", records)
	}
}

func TestRuntimeEventWorkerReconstructsIncompleteTerminalConvergenceWithoutRuntimePolling(t *testing.T) {
	ctx := context.Background()
	repos := persistence.NewRepositories(nil)
	hosts := runtimepkg.NewRuntimeHostRepository(nil)
	scheduler := runtimepkg.NewRuntimeScheduler(hosts, queue.NewMemoryDistributedLockManager())
	const (
		runID         = "run_terminal_recovery"
		hostID        = "host_terminal_recovery"
		reservationID = "reservation_terminal_recovery"
		dispatchID    = "dispatch_terminal_recovery"
		queueID       = "runtime_events:terminal_recovery"
	)
	if _, _, err := repos.AgentRuns.CreateRun(ctx, persistence.CreateAgentRunCommand{Record: persistence.AgentRunRecord{
		AgentRunID: runID, TenantID: "tenant_terminal", UserID: "user_terminal", WorkspaceID: "workspace_terminal",
		IdempotencyKey: "terminal-recovery-idempotency", RequestHash: "terminal-recovery-hash", Status: "planning",
		WorkspaceVersion: 1, BindingVersion: 1, ContextGeneration: 1,
	}}); err != nil {
		t.Fatal(err)
	}
	persistedPlan := runtimeEventTestTerminalPlan(t, runID, "work_ai_general_chat")
	// This recovery fixture deliberately models a detached reservation. Keep
	// the immutable Plan in the same scope so sessionRequired=false is a real
	// frozen-contract fact rather than an old ThreadID inference artifact.
	persistedPlan.ExecutionScope = string(runtimepkg.ScopeDetachedTask)
	if err := repos.AgentRuns.SavePlan(ctx, persistence.AgentRunPlanRecord{
		AgentRunID: runID, PlanVersion: 1, PlanStatus: "executing", AgentRunStatus: "running",
		Plan: valueMap(persistedPlan),
	}); err != nil {
		t.Fatal(err)
	}
	identity := runtimepkg.RuntimeHostIdentity{RuntimeHostID: hostID, InstanceID: "instance_terminal_recovery", Environment: "test"}
	if _, err := hosts.RegisterHost(ctx, identity, runtimepkg.RuntimeHostRegistration{Endpoint: "http://" + hostID, Capabilities: runtimepkg.RuntimeCapabilitySnapshot{CapabilityHash: "cap-terminal-recovery"}, MaxActiveRuns: 4}); err != nil {
		t.Fatal(err)
	}
	if _, err := hosts.HeartbeatHost(ctx, identity, runtimepkg.RuntimeHostHeartbeat{Sequence: 1, ObservedAt: time.Now().UTC(), CapabilityHash: "cap-terminal-recovery", SignatureKeyID: "test-key"}); err != nil {
		t.Fatal(err)
	}
	dimension := func(key string) runtimepkg.RuntimeCapacityDimension {
		return runtimepkg.RuntimeCapacityDimension{Key: key, Limit: 10, Requested: 1, Version: 1}
	}
	capacityReservation, err := scheduler.Capacity.Reserve(ctx, runtimepkg.RuntimeCapacityCommand{
		RunID: runID, SnapshotVersion: 1, TTL: time.Minute,
		Dimensions: runtimepkg.RuntimeCapacityDimensions{Model: dimension("model-terminal"), AuthPool: dimension("auth-terminal"), Tool: dimension("tool-terminal"), Tenant: dimension("tenant-terminal"), User: dimension("user-terminal")},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := scheduler.Capacity.CommitAccepted(ctx, capacityReservation); err != nil {
		t.Fatal(err)
	}
	reservation, _, err := hosts.TryReserveSlot(ctx, runtimepkg.AtomicReservationCommand{
		ReservationID: reservationID, RunID: runID, OwnerInstanceID: "worker-terminal", ExecutionScope: "detached_task",
		CapabilityHash: "cap-terminal-recovery", LeaseTokenHash: "sha256:terminal-recovery", FencingToken: 1,
		ExpiresAt: time.Now().UTC().Add(time.Minute), HeartbeatAfter: time.Now().UTC().Add(-time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := hosts.CreateDispatch(ctx, runtimepkg.RuntimeDispatch{
		DispatchID: dispatchID, RunID: runID, ReservationID: reservationID, CapacityReservationID: capacityReservation.ReservationID, CapacityReservedVersion: capacityReservation.Version, RuntimeHostID: hostID,
		DispatchAttempt: 1, PlanVersion: 1, FencingToken: reservation.FencingToken, RunTicketJTIHash: "sha256:terminal-recovery-jti",
		TicketExpiresAt: time.Now().UTC().Add(time.Minute), InputManifestHash: "sha256:terminal-recovery-manifest",
		OwnerInstanceID: "worker-terminal", LeaseTokenHash: "sha256:terminal-recovery", LeaseExpiresAt: time.Now().UTC().Add(time.Minute),
	}); err != nil {
		t.Fatal(err)
	}
	fence := runtimepkg.ReservationFence{ReservationID: reservationID, RuntimeHostID: hostID, OwnerInstanceID: "worker-terminal", LeaseTokenHash: "sha256:terminal-recovery", FencingToken: reservation.FencingToken}
	if err := hosts.ConfirmDispatchAccepted(ctx, runtimepkg.DispatchAcceptedCommand{Fence: fence, DispatchID: dispatchID, RuntimeRequestID: "request-terminal-recovery"}); err != nil {
		t.Fatal(err)
	}
	if err := hosts.AppendRunEventAndAdvanceCursor(ctx, runtimepkg.RuntimeHostRunEvent{
		EventID: "event-terminal-recovery", RunID: runID, DispatchID: dispatchID, RuntimeHostID: hostID,
		SourceSequence: 1, EventType: "run.succeeded", Visibility: "internal", SafePayload: map[string]any{"status": "succeeded"},
	}, 0); err != nil {
		t.Fatal(err)
	}
	repos.Queue.Enqueue(map[string]any{
		"queueId": queueID, "queueName": queue.QueueRuntimeEvents, "taskType": "runtime_event_ingest", "taskId": runID,
		"payload": map[string]any{"runId": runID, "dispatchId": dispatchID, "runtimeHostId": hostID},
	})
	eventJob, proof, err := repos.Queue.Lease(ctx, queue.QueueRuntimeEvents, "event-worker-terminal-recovery", time.Minute, "runtime_event_ingest")
	if err != nil {
		t.Fatal(err)
	}
	worker := NewRuntimeEventWorker(repos, hosts, scheduler, nil, "terminal-recovery-ticket")
	converger := runtimepkg.NewRuntimeTerminalConverger(nil, hosts, scheduler.Sessions, scheduler.Capacity, repos.Queue)
	worker.newConverger = func() *runtimepkg.RuntimeTerminalConverger { return converger }
	converger.ProjectProduct = func(context.Context, runtimepkg.TerminalConvergenceCommand, string) error {
		return context.DeadlineExceeded
	}
	converger.ConvergeAgentRun = func(context.Context, runtimepkg.TerminalConvergenceCommand, string) error { return nil }
	converger.AppendPublicEvent = func(context.Context, runtimepkg.TerminalConvergenceCommand, string) error { return nil }
	if _, err := converger.Converge(ctx, runtimepkg.TerminalConvergenceCommand{
		DispatchID: dispatchID, RunID: runID, TerminalSourceSequence: 1, TerminalStatus: "succeeded",
		SafeResult: map[string]any{"finalAnswer": "persisted before restart"}, ActualUsage: map[string]any{"modelTokens": 7}, QueueProof: proof,
		CapacityReservation: capacityReservation, DispatchTerminal: runtimepkg.DispatchTerminalCommand{Fence: fence, DispatchID: dispatchID, TerminalStatus: "succeeded"},
	}); err != context.DeadlineExceeded {
		t.Fatalf("expected injected product projection failure, got %v", err)
	}
	if recovery, found, err := converger.FindIncompleteByQueueID(ctx, queueID); err != nil || !found || recovery.DispatchID != dispatchID {
		t.Fatalf("incomplete terminal recovery=%+v found=%t err=%v", recovery, found, err)
	}

	result := worker.Process(ctx, eventJob, proof)
	if result["status"] != "succeeded" || result["recoveredTerminalConvergence"] != true {
		t.Fatalf("recovered event result=%#v", result)
	}
	finalRun, err := repos.AgentRuns.GetRunInternal(ctx, runID)
	if err != nil || finalRun.Status != "succeeded" || finalRun.PublicResult["finalAnswer"] != "persisted before restart" {
		t.Fatalf("final run=%+v err=%v", finalRun, err)
	}
	finalDispatch, err := hosts.GetDispatch(ctx, dispatchID)
	if err != nil || finalDispatch.State != "succeeded" {
		t.Fatalf("final dispatch=%+v err=%v", finalDispatch, err)
	}
	incomplete, err := converger.ListIncomplete(ctx, 10)
	if err != nil || len(incomplete) != 0 {
		t.Fatalf("incomplete=%+v err=%v", incomplete, err)
	}
}

func TestRuntimeEventWorkerRecoversImmutableFallbackAsProductFailure(t *testing.T) {
	ctx := context.Background()
	repos := persistence.NewRepositories(nil)
	hosts := runtimepkg.NewRuntimeHostRepository(nil)
	scheduler := runtimepkg.NewRuntimeScheduler(hosts, queue.NewMemoryDistributedLockManager())
	const (
		runID         = "run_terminal_fallback_recovery"
		taskID        = "task_terminal_fallback_recovery"
		hostID        = "host_terminal_fallback_recovery"
		reservationID = "reservation_terminal_fallback_recovery"
		dispatchID    = "dispatch_terminal_fallback_recovery"
		queueID       = "runtime_events:terminal_fallback_recovery"
		userID        = "user_terminal_fallback_recovery"
		workspaceID   = "workspace_terminal_fallback_recovery"
	)
	usageService := services.NewPermissionUsageService(repos)
	admission := usageService.CheckAdmission(userID, workspaceID, "work_ai_renshe_content", map[string]any{"generation": 1})
	quotaReservation := usageService.ReserveQuota(workerMapString(admission, "permissionCheckId"), taskID, "work_ai_renshe_content", map[string]int{"generation": 1})
	quotaReservationID := workerMapString(quotaReservation, "reservationId")
	if quotaReservationID == "" {
		t.Fatalf("quota reservation failed: %#v", quotaReservation)
	}
	repos.ChatTasks.CreateAiTask(taskID, "work_ai_renshe_content", userID, workspaceID, map[string]any{"reservationId": quotaReservationID})
	if _, _, err := repos.AgentRuns.CreateRun(ctx, persistence.CreateAgentRunCommand{Record: persistence.AgentRunRecord{
		AgentRunID: runID, TenantID: "tenant_terminal", UserID: userID, WorkspaceID: workspaceID, TaskID: taskID,
		IdempotencyKey: "terminal-fallback-idempotency", RequestHash: "terminal-fallback-hash", Status: "planning",
		WorkspaceVersion: 1, BindingVersion: 1, ContextGeneration: 1,
		IntentSnapshot: map[string]any{"resolvedTaskType": "work_ai_renshe_content"},
	}}); err != nil {
		t.Fatal(err)
	}
	persistedPlan := runtimeEventTestTerminalPlan(t, runID, "work_ai_renshe_content")
	// The test exercises Product projection fallback, not session ownership.
	// Align its frozen Plan with the detached reservation and convergence
	// snapshot so the recovery path can validate scope consistency.
	persistedPlan.ExecutionScope = string(runtimepkg.ScopeDetachedTask)
	if err := repos.AgentRuns.SavePlan(ctx, persistence.AgentRunPlanRecord{
		AgentRunID: runID, PlanVersion: 1, PlanStatus: "executing", AgentRunStatus: "running",
		Plan: valueMap(persistedPlan),
	}); err != nil {
		t.Fatal(err)
	}
	identity := runtimepkg.RuntimeHostIdentity{RuntimeHostID: hostID, InstanceID: "instance_terminal_fallback", Environment: "test"}
	if _, err := hosts.RegisterHost(ctx, identity, runtimepkg.RuntimeHostRegistration{Endpoint: "http://" + hostID, Capabilities: runtimepkg.RuntimeCapabilitySnapshot{CapabilityHash: "cap-terminal-fallback"}, MaxActiveRuns: 4}); err != nil {
		t.Fatal(err)
	}
	if _, err := hosts.HeartbeatHost(ctx, identity, runtimepkg.RuntimeHostHeartbeat{Sequence: 1, ObservedAt: time.Now().UTC(), CapabilityHash: "cap-terminal-fallback", SignatureKeyID: "test-key"}); err != nil {
		t.Fatal(err)
	}
	dimension := func(key string) runtimepkg.RuntimeCapacityDimension {
		return runtimepkg.RuntimeCapacityDimension{Key: key, Limit: 10, Requested: 1, Version: 1}
	}
	capacityReservation, err := scheduler.Capacity.Reserve(ctx, runtimepkg.RuntimeCapacityCommand{
		RunID: runID, SnapshotVersion: 1, TTL: time.Minute,
		Dimensions: runtimepkg.RuntimeCapacityDimensions{Model: dimension("model-terminal-fallback"), AuthPool: dimension("auth-terminal-fallback"), Tool: dimension("tool-terminal-fallback"), Tenant: dimension("tenant-terminal-fallback"), User: dimension("user-terminal-fallback")},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := scheduler.Capacity.CommitAccepted(ctx, capacityReservation); err != nil {
		t.Fatal(err)
	}
	reservation, _, err := hosts.TryReserveSlot(ctx, runtimepkg.AtomicReservationCommand{
		ReservationID: reservationID, RunID: runID, OwnerInstanceID: "worker-terminal-fallback", ExecutionScope: "detached_task",
		CapabilityHash: "cap-terminal-fallback", LeaseTokenHash: "sha256:terminal-fallback", FencingToken: 1,
		ExpiresAt: time.Now().UTC().Add(time.Minute), HeartbeatAfter: time.Now().UTC().Add(-time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := hosts.CreateDispatch(ctx, runtimepkg.RuntimeDispatch{
		DispatchID: dispatchID, RunID: runID, ReservationID: reservationID, CapacityReservationID: capacityReservation.ReservationID, CapacityReservedVersion: capacityReservation.Version, RuntimeHostID: hostID,
		DispatchAttempt: 1, PlanVersion: 1, FencingToken: reservation.FencingToken, RunTicketJTIHash: "sha256:terminal-fallback-jti",
		TicketExpiresAt: time.Now().UTC().Add(time.Minute), InputManifestHash: "sha256:terminal-fallback-manifest",
		OwnerInstanceID: "worker-terminal-fallback", LeaseTokenHash: "sha256:terminal-fallback", LeaseExpiresAt: time.Now().UTC().Add(time.Minute),
	}); err != nil {
		t.Fatal(err)
	}
	fence := runtimepkg.ReservationFence{ReservationID: reservationID, RuntimeHostID: hostID, OwnerInstanceID: "worker-terminal-fallback", LeaseTokenHash: "sha256:terminal-fallback", FencingToken: reservation.FencingToken}
	if err := hosts.ConfirmDispatchAccepted(ctx, runtimepkg.DispatchAcceptedCommand{Fence: fence, DispatchID: dispatchID, RuntimeRequestID: "request-terminal-fallback"}); err != nil {
		t.Fatal(err)
	}
	if err := hosts.AppendRunEventAndAdvanceCursor(ctx, runtimepkg.RuntimeHostRunEvent{
		EventID: "event-terminal-fallback", RunID: runID, DispatchID: dispatchID, RuntimeHostID: hostID,
		SourceSequence: 1, EventType: "run.succeeded", Visibility: "internal", SafePayload: map[string]any{"status": "succeeded"},
	}, 0); err != nil {
		t.Fatal(err)
	}
	repos.Queue.Enqueue(map[string]any{
		"queueId": queueID, "queueName": queue.QueueRuntimeEvents, "taskType": "runtime_event_ingest", "taskId": runID,
		"payload": map[string]any{"runId": runID, "dispatchId": dispatchID, "runtimeHostId": hostID},
	})
	eventJob, proof, err := repos.Queue.Lease(ctx, queue.QueueRuntimeEvents, "event-worker-terminal-fallback", time.Minute, "runtime_event_ingest")
	if err != nil {
		t.Fatal(err)
	}
	worker := NewRuntimeEventWorker(repos, hosts, scheduler, nil, "terminal-fallback-ticket")
	converger := runtimepkg.NewRuntimeTerminalConverger(nil, hosts, scheduler.Sessions, scheduler.Capacity, repos.Queue)
	worker.newConverger = func() *runtimepkg.RuntimeTerminalConverger { return converger }
	converger.ProjectProduct = func(context.Context, runtimepkg.TerminalConvergenceCommand, string) error {
		return context.DeadlineExceeded
	}
	converger.ConvergeAgentRun = func(context.Context, runtimepkg.TerminalConvergenceCommand, string) error { return nil }
	converger.AppendPublicEvent = func(context.Context, runtimepkg.TerminalConvergenceCommand, string) error { return nil }
	fallback := map[string]any{"finalAnswer": "⚠️ Agent couldn't generate a response. Please try again."}
	if _, err := converger.Converge(ctx, runtimepkg.TerminalConvergenceCommand{
		DispatchID: dispatchID, RunID: runID, TerminalSourceSequence: 1, TerminalStatus: "succeeded",
		SafeResult: fallback, QueueProof: proof, CapacityReservation: capacityReservation,
		DispatchTerminal: runtimepkg.DispatchTerminalCommand{Fence: fence, DispatchID: dispatchID, TerminalStatus: "succeeded"},
	}); err != context.DeadlineExceeded {
		t.Fatalf("expected injected product projection failure, got %v", err)
	}
	if recovery, found, err := converger.FindIncompleteByQueueID(ctx, queueID); err != nil || !found || recovery.DispatchID != dispatchID {
		t.Fatalf("incomplete terminal recovery=%+v found=%t err=%v", recovery, found, err)
	}

	result := worker.Process(ctx, eventJob, proof)
	if result["status"] != "failed" || result["recoveredTerminalConvergence"] != true {
		t.Fatalf("recovered fallback result=%#v", result)
	}
	finalRun, err := repos.AgentRuns.GetRunInternal(ctx, runID)
	if err != nil || finalRun.Status != "failed" || workerMapString(finalRun.ErrorSummary, "code") != "AI_RESULT_PARSE_FAILED" {
		t.Fatalf("final run=%+v err=%v", finalRun, err)
	}
	finalTask, err := repos.ChatTasks.GetAiTask(taskID)
	if err != nil || finalTask["status"] != "failed" || workerMapString(aiWorkerMap(finalTask["errorSummary"]), "code") != "AI_RESULT_PARSE_FAILED" {
		t.Fatalf("final task=%#v err=%v", finalTask, err)
	}
	finalDispatch, err := hosts.GetDispatch(ctx, dispatchID)
	if err != nil || finalDispatch.State != "succeeded" {
		t.Fatalf("source dispatch should remain succeeded: %+v err=%v", finalDispatch, err)
	}
	latestCapacity, err := scheduler.Capacity.GetLatestByRunID(ctx, runID)
	if err != nil || latestCapacity.State != "released" {
		t.Fatalf("capacity was not released: %+v err=%v", latestCapacity, err)
	}
	storedQuota, err := repos.Usage.GetQuotaReservation(quotaReservationID)
	if err != nil || workerMapString(storedQuota, "status") != "released" {
		t.Fatalf("quota was not released: %#v err=%v", storedQuota, err)
	}
	queueRecords := repos.Queue.ListQueueRecords(map[string]any{"queueId": queueID})
	if len(queueRecords) != 1 || queueRecords[0]["status"] != "succeeded" {
		t.Fatalf("runtime event queue did not complete: %#v", queueRecords)
	}
	incomplete, err := converger.ListIncomplete(ctx, 10)
	if err != nil || len(incomplete) != 0 {
		t.Fatalf("incomplete convergence remains: %+v err=%v", incomplete, err)
	}
}
