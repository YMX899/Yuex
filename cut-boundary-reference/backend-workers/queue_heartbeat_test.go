package workers

import (
	"context"
	"errors"
	"testing"
	"time"

	"huahuoai/backend/source/internal/persistence"
	"huahuoai/backend/source/internal/queue"
)

func TestQueueRepositoryHeartbeatKeepsLongRunningRecordLeased(t *testing.T) {
	repo := persistence.NewQueueRepository()
	queueID := "asr:heartbeat-long-running"
	repo.Enqueue(map[string]any{
		"queueId":     queueID,
		"queueName":   queue.QueueASR,
		"taskType":    "asr_file_recognition",
		"taskId":      "heartbeat-long-running",
		"maxAttempts": 3,
	})
	leaseTTL := 40 * time.Millisecond
	_, proof, err := repo.Lease(context.Background(), queue.QueueASR, "worker-one", leaseTTL)
	if err != nil {
		t.Fatal("initial lease failed")
	}
	if _, err := repo.MarkRunning(context.Background(), proof); err != nil {
		t.Fatalf("mark running: %v", err)
	}
	_, heartbeat := startQueueRepositoryHeartbeat(context.Background(), repo, proof, leaseTTL)
	time.Sleep(leaseTTL * 3)
	if stolen, _, err := repo.Lease(context.Background(), queue.QueueASR, "worker-two", leaseTTL); err == nil {
		_, _ = heartbeat.Stop()
		t.Fatalf("running record was leased while heartbeat was active: %#v", stolen)
	}
	_, _ = heartbeat.Stop()
	time.Sleep(leaseTTL * 2)
	if recovered, _, err := repo.Lease(context.Background(), queue.QueueASR, "worker-two", leaseTTL); !errors.Is(err, persistence.ErrNoQueueWork) {
		t.Fatalf("expired running record must not be stolen before recovery: record=%#v err=%v", recovered, err)
	}
	if count, err := repo.RecoverRunningWithoutHeartbeat(context.Background(), time.Now().UTC()); err != nil || count != 1 {
		t.Fatalf("recover stalled running record: count=%d err=%v", count, err)
	}
	records := repo.ListQueueRecords(map[string]any{"queueId": queueID})
	if len(records) != 1 || records[0]["status"] != "timeout" {
		t.Fatalf("stalled running record must converge to timeout: %#v", records)
	}
}

func TestQueueRepositoryHeartbeatFreezeKeepsTerminalContextAlive(t *testing.T) {
	repo := persistence.NewQueueRepository()
	repo.Enqueue(map[string]any{
		"queueId": "runtime_events:heartbeat-freeze", "queueName": queue.QueueRuntimeEvents,
		"taskType": "runtime_event_ingest", "taskId": "run-heartbeat-freeze",
	})
	_, proof, err := repo.Lease(context.Background(), queue.QueueRuntimeEvents, "worker-freeze", time.Minute, "runtime_event_ingest")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.MarkRunning(context.Background(), proof); err != nil {
		t.Fatal(err)
	}
	leaseCtx, heartbeat := startQueueRepositoryHeartbeat(context.Background(), repo, proof, time.Minute)
	if _, err := heartbeat.Freeze(); err != nil {
		t.Fatalf("freeze heartbeat: %v", err)
	}
	if err := leaseCtx.Err(); err != nil {
		t.Fatalf("freeze cancelled terminal work context: %v", err)
	}
	if _, err := heartbeat.Stop(); err != nil {
		t.Fatalf("stop frozen heartbeat: %v", err)
	}
	if leaseCtx.Err() == nil {
		t.Fatal("stop did not cancel terminal work context")
	}
}
