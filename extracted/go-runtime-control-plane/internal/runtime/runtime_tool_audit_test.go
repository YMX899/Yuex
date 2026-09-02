package runtime

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"huahuoai/backend/source/internal/config"
	"huahuoai/backend/source/internal/persistence"
)

func TestRuntimeToolAuditRejectsRawPayloadFields(t *testing.T) {
	for _, key := range []string{
		"id", "toolCallId", "args", "result", "query", "path", "content", "session", "ticket", "provider",
	} {
		t.Run(key, func(t *testing.T) {
			payload := runtimeToolAuditTestPayload("started")
			payload[key] = "must-not-persist"
			if _, err := parseRuntimeToolAuditEvent("tool.call.started", payload); err == nil || err.Error() != "RUNTIME_EVENT_GAP" {
				t.Fatalf("raw field %q error=%v, want RUNTIME_EVENT_GAP", key, err)
			}
		})
	}
}

func TestRuntimeToolAuditNormalizesOnlyDisjointSafeTransportFields(t *testing.T) {
	data := map[string]any{
		"schemaVersion": "huahuo.runtime-tool-execution-event.v1",
		"toolName":      "workspace_search",
		"toolCallHash":  runtimeToolAuditTestHash("a"),
	}
	safePayload := map[string]any{
		"argsHash":   runtimeToolAuditTestHash("b"),
		"outcome":    "started",
		"durationMs": 0,
		"bytes":      0,
		"call":       1,
		"repeat":     1,
	}
	normalized, err := NormalizeRuntimeToolAuditEventPayload("tool.call.started", data, safePayload)
	if err != nil {
		t.Fatalf("normalize valid receipt: %v", err)
	}
	if len(normalized) != 9 || normalized["toolName"] != "workspace_search" || normalized["status"] != nil {
		t.Fatalf("normalized receipt=%#v", normalized)
	}

	safePayload["argsHash"] = runtimeToolAuditTestHash("c")
	data["argsHash"] = runtimeToolAuditTestHash("b")
	if _, err := NormalizeRuntimeToolAuditEventPayload("tool.call.started", data, safePayload); err == nil || err.Error() != "RUNTIME_EVENT_GAP" {
		t.Fatalf("duplicate envelope key error=%v, want RUNTIME_EVENT_GAP", err)
	}
}

func TestRuntimeToolAuditProjectionIsIdempotentAndTerminalizesOpenCalls(t *testing.T) {
	ctx := context.Background()
	repository := NewRuntimeHostRepository(nil)
	const runID = "runtime-tool-audit-run"
	const dispatchID = "runtime-tool-audit-dispatch"
	repository.dispatches[dispatchID] = RuntimeDispatch{DispatchID: dispatchID, RunID: runID}
	repository.runtimeRunRecords[runID] = RuntimeRunRecordV1{RunID: runID, WorkspaceVersion: 7}

	started := RuntimeHostRunEvent{
		EventID: "runtime-tool-audit-event-1", RunID: runID, DispatchID: dispatchID, SourceSequence: 1,
		EventType: "tool.call.started", Visibility: "admin_safe", SafePayload: runtimeToolAuditTestPayload("started"),
	}
	if err := repository.AppendRunEventAndAdvanceCursor(ctx, started, 0); err != nil {
		t.Fatalf("append started receipt: %v", err)
	}
	if err := repository.AppendRunEventAndAdvanceCursor(ctx, started, 0); err != nil {
		t.Fatalf("replay started receipt: %v", err)
	}
	invocationID := runtimeToolInvocationID(runID, runtimeToolAuditTestHash("a"))
	stored, ok := repository.toolInvocations[invocationID]
	if !ok || stored.Status != "started" || stored.WorkspaceVersion != 7 || stored.ArgsHash != runtimeToolAuditTestHash("b") {
		t.Fatalf("started invocation=%+v found=%t", stored, ok)
	}

	finishedPayload := runtimeToolAuditTestPayload("succeeded")
	finishedPayload["durationMs"] = 17
	finishedPayload["bytes"] = 128
	finishedPayload["resultFingerprint"] = runtimeToolAuditTestHash("c")
	finished := RuntimeHostRunEvent{
		EventID: "runtime-tool-audit-event-2", RunID: runID, DispatchID: dispatchID, SourceSequence: 2,
		EventType: "tool.call.finished", Visibility: "admin_safe", SafePayload: finishedPayload,
	}
	if err := repository.AppendRunEventAndAdvanceCursor(ctx, finished, 1); err != nil {
		t.Fatalf("append finished receipt: %v", err)
	}
	if err := repository.AppendRunEventAndAdvanceCursor(ctx, finished, 1); err != nil {
		t.Fatalf("replay finished receipt: %v", err)
	}
	stored = repository.toolInvocations[invocationID]
	if stored.Status != "succeeded" || stored.DurationMs != 17 || stored.Bytes != 128 || stored.ResultFingerprint != runtimeToolAuditTestHash("c") {
		t.Fatalf("finished invocation=%+v", stored)
	}

	openPayload := runtimeToolAuditTestPayload("started")
	openPayload["toolCallHash"] = runtimeToolAuditTestHash("d")
	openPayload["call"] = 2
	open := RuntimeHostRunEvent{
		EventID: "runtime-tool-audit-event-3", RunID: runID, DispatchID: dispatchID, SourceSequence: 3,
		EventType: "tool.call.started", Visibility: "admin_safe", SafePayload: openPayload,
	}
	if err := repository.AppendRunEventAndAdvanceCursor(ctx, open, 2); err != nil {
		t.Fatalf("append open receipt: %v", err)
	}
	repository.abortOutstandingRuntimeToolInvocationsMemory(runID)
	if terminal := repository.toolInvocations[runtimeToolInvocationID(runID, runtimeToolAuditTestHash("d"))]; terminal.Status != "aborted" || terminal.ErrorCode != "RUNTIME_ABORTED" || !terminal.TerminalConverged {
		t.Fatalf("open invocation did not converge to aborted: %+v", terminal)
	}
	if terminal := repository.toolInvocations[invocationID]; terminal.Status != "succeeded" || terminal.TerminalConverged {
		t.Fatalf("completed invocation was rewritten: %+v", terminal)
	}
}

func TestRuntimeToolAuditAllowsDirectRejectedReceipt(t *testing.T) {
	ctx := context.Background()
	repository := NewRuntimeHostRepository(nil)
	const runID = "runtime-tool-audit-direct-rejected-run"
	const dispatchID = "runtime-tool-audit-direct-rejected-dispatch"
	repository.dispatches[dispatchID] = RuntimeDispatch{DispatchID: dispatchID, RunID: runID}
	repository.runtimeRunRecords[runID] = RuntimeRunRecordV1{RunID: runID, WorkspaceVersion: 1}

	payload := runtimeToolAuditTestPayload("rejected")
	payload["errorCode"] = "RUNTIME_PERMISSION_DENIED"
	event := RuntimeHostRunEvent{
		EventID: "runtime-tool-audit-direct-rejected-event", RunID: runID, DispatchID: dispatchID, SourceSequence: 1,
		EventType: "tool.call.rejected", Visibility: "admin_safe", SafePayload: payload,
	}
	if err := repository.AppendRunEventAndAdvanceCursor(ctx, event, 0); err != nil {
		t.Fatalf("append direct rejected receipt: %v", err)
	}
	if err := repository.AppendRunEventAndAdvanceCursor(ctx, event, 0); err != nil {
		t.Fatalf("replay direct rejected receipt: %v", err)
	}
	invocation := repository.toolInvocations[runtimeToolInvocationID(runID, runtimeToolAuditTestHash("a"))]
	if invocation.Status != "rejected" || invocation.ErrorCode != "RUNTIME_PERMISSION_DENIED" || invocation.ResultFingerprint != "" || invocation.DurationMs != 0 || invocation.Bytes != 0 {
		t.Fatalf("direct rejected invocation=%+v", invocation)
	}
	if len(repository.events[runID]) != 1 || repository.dispatchEventCursors[dispatchID] != 1 {
		t.Fatalf("direct rejected receipt cursor/events mismatch: cursor=%d events=%#v", repository.dispatchEventCursors[dispatchID], repository.events[runID])
	}
}

func TestRuntimeToolAuditRejectAfterStartedFailsClosed(t *testing.T) {
	ctx := context.Background()
	repository := NewRuntimeHostRepository(nil)
	const runID = "runtime-tool-audit-reject-after-start-run"
	const dispatchID = "runtime-tool-audit-reject-after-start-dispatch"
	repository.dispatches[dispatchID] = RuntimeDispatch{DispatchID: dispatchID, RunID: runID}
	repository.runtimeRunRecords[runID] = RuntimeRunRecordV1{RunID: runID, WorkspaceVersion: 1}

	started := RuntimeHostRunEvent{
		EventID: "runtime-tool-audit-reject-after-start-event-1", RunID: runID, DispatchID: dispatchID, SourceSequence: 1,
		EventType: "tool.call.started", Visibility: "admin_safe", SafePayload: runtimeToolAuditTestPayload("started"),
	}
	if err := repository.AppendRunEventAndAdvanceCursor(ctx, started, 0); err != nil {
		t.Fatalf("append started receipt: %v", err)
	}
	rejectedPayload := runtimeToolAuditTestPayload("rejected")
	rejectedPayload["errorCode"] = "RUNTIME_PERMISSION_DENIED"
	err := repository.AppendRunEventAndAdvanceCursor(ctx, RuntimeHostRunEvent{
		EventID: "runtime-tool-audit-reject-after-start-event-2", RunID: runID, DispatchID: dispatchID, SourceSequence: 2,
		EventType: "tool.call.rejected", Visibility: "admin_safe", SafePayload: rejectedPayload,
	}, 1)
	if err == nil || err.Error() != "RUNTIME_EVENT_GAP" {
		t.Fatalf("rejected receipt after started error=%v, want RUNTIME_EVENT_GAP", err)
	}
	invocation := repository.toolInvocations[runtimeToolInvocationID(runID, runtimeToolAuditTestHash("a"))]
	if invocation.Status != "started" || invocation.ErrorCode != "" || invocation.ResultFingerprint != "" || invocation.DurationMs != 0 || invocation.Bytes != 0 {
		t.Fatalf("rejected receipt rewrote started invocation=%+v", invocation)
	}
	if len(repository.events[runID]) != 1 || repository.dispatchEventCursors[dispatchID] != 1 {
		t.Fatalf("failed rejected receipt advanced cursor or stored event: cursor=%d events=%#v", repository.dispatchEventCursors[dispatchID], repository.events[runID])
	}
}

func TestRuntimeToolAuditRejectsFinishedReceiptWithoutStartedReceipt(t *testing.T) {
	ctx := context.Background()
	repository := NewRuntimeHostRepository(nil)
	const runID = "runtime-tool-audit-missing-start-run"
	const dispatchID = "runtime-tool-audit-missing-start-dispatch"
	repository.dispatches[dispatchID] = RuntimeDispatch{DispatchID: dispatchID, RunID: runID}
	repository.runtimeRunRecords[runID] = RuntimeRunRecordV1{RunID: runID, WorkspaceVersion: 1}
	payload := runtimeToolAuditTestPayload("succeeded")
	payload["durationMs"] = 1
	payload["resultFingerprint"] = runtimeToolAuditTestHash("c")
	err := repository.AppendRunEventAndAdvanceCursor(ctx, RuntimeHostRunEvent{
		EventID: "runtime-tool-audit-missing-start-event", RunID: runID, DispatchID: dispatchID, SourceSequence: 1,
		EventType: "tool.call.finished", Visibility: "admin_safe", SafePayload: payload,
	}, 0)
	if err == nil || err.Error() != "RUNTIME_EVENT_GAP" {
		t.Fatalf("terminal receipt without start error=%v, want RUNTIME_EVENT_GAP", err)
	}
	if len(repository.events[runID]) != 0 || len(repository.toolInvocations) != 0 {
		t.Fatalf("failed projection mutated memory state: events=%#v invocations=%#v", repository.events[runID], repository.toolInvocations)
	}
	if repository.dispatchEventCursors[dispatchID] != 0 {
		t.Fatalf("failed projection advanced event cursor: %d", repository.dispatchEventCursors[dispatchID])
	}
}

func TestRuntimeToolAuditPostgresProjectionWhenConfigured(t *testing.T) {
	if strings.TrimSpace(os.Getenv("HUAHUO_RUNTIME_V1_LIVE_POSTGRES")) != "1" {
		t.Skip("HUAHUO_RUNTIME_V1_LIVE_POSTGRES=1 is required")
	}
	dsn := strings.TrimSpace(os.Getenv("HUAHUO_TEST_DATABASE_DSN"))
	if dsn == "" {
		t.Skip("HUAHUO_TEST_DATABASE_DSN is required")
	}
	database, err := persistence.NewDatabase(config.Settings{
		DatabaseDSN: dsn,
		Database: config.DatabaseSettings{
			PoolMinConns: 1, PoolMaxConns: 4, ConnectTimeoutSeconds: 5, StatementTimeoutSeconds: 10,
		},
	})
	if err != nil || database.Disabled || database.Pool == nil {
		if database != nil {
			database.Close()
		}
		t.Fatalf("open live PostgreSQL: %v", err)
	}
	t.Cleanup(func() { database.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	suffix := strconv.FormatInt(time.Now().UTC().UnixNano(), 36)
	runID := "runtime-tool-audit-live-" + suffix
	t.Cleanup(func() {
		_, _ = database.Pool.Exec(context.Background(), `delete from runtime_tool_invocations where run_id=$1`, runID)
		_, _ = database.Pool.Exec(context.Background(), `delete from runtime_run_records where run_id=$1`, runID)
	})
	if _, err := database.Pool.Exec(ctx, `
insert into runtime_run_records(run_id,attempt,status,workspace_version,execution_scope)
values($1,1,'running',7,'detached_task')`, runID); err != nil {
		t.Fatalf("seed runtime run record: %v", err)
	}

	started, err := parseRuntimeToolAuditEvent("tool.call.started", runtimeToolAuditTestPayload("started"))
	if err != nil {
		t.Fatal(err)
	}
	if err := database.WithTx(ctx, func(tx *persistence.Tx) error {
		return projectRuntimeToolInvocationTx(ctx, tx, runID, started)
	}); err != nil {
		t.Fatalf("insert started invocation: %v", err)
	}
	if err := database.WithTx(ctx, func(tx *persistence.Tx) error {
		return projectRuntimeToolInvocationTx(ctx, tx, runID, started)
	}); err != nil {
		t.Fatalf("replay started invocation: %v", err)
	}

	finishedPayload := runtimeToolAuditTestPayload("succeeded")
	finishedPayload["durationMs"] = 17
	finishedPayload["bytes"] = 128
	finishedPayload["resultFingerprint"] = runtimeToolAuditTestHash("c")
	finished, err := parseRuntimeToolAuditEvent("tool.call.finished", finishedPayload)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.WithTx(ctx, func(tx *persistence.Tx) error {
		return projectRuntimeToolInvocationTx(ctx, tx, runID, finished)
	}); err != nil {
		t.Fatalf("transition finished invocation: %v", err)
	}
	if err := database.WithTx(ctx, func(tx *persistence.Tx) error {
		return projectRuntimeToolInvocationTx(ctx, tx, runID, finished)
	}); err != nil {
		t.Fatalf("replay finished invocation: %v", err)
	}

	directRejectedPayload := runtimeToolAuditTestPayload("rejected")
	directRejectedPayload["toolCallHash"] = runtimeToolAuditTestHash("e")
	directRejectedPayload["call"] = 3
	directRejectedPayload["errorCode"] = "RUNTIME_PERMISSION_DENIED"
	directRejected, err := parseRuntimeToolAuditEvent("tool.call.rejected", directRejectedPayload)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.WithTx(ctx, func(tx *persistence.Tx) error {
		return projectRuntimeToolInvocationTx(ctx, tx, runID, directRejected)
	}); err != nil {
		t.Fatalf("insert direct rejected invocation: %v", err)
	}
	if err := database.WithTx(ctx, func(tx *persistence.Tx) error {
		return projectRuntimeToolInvocationTx(ctx, tx, runID, directRejected)
	}); err != nil {
		t.Fatalf("replay direct rejected invocation: %v", err)
	}

	openPayload := runtimeToolAuditTestPayload("started")
	openPayload["toolCallHash"] = runtimeToolAuditTestHash("d")
	openPayload["call"] = 2
	open, err := parseRuntimeToolAuditEvent("tool.call.started", openPayload)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.WithTx(ctx, func(tx *persistence.Tx) error {
		return projectRuntimeToolInvocationTx(ctx, tx, runID, open)
	}); err != nil {
		t.Fatalf("insert open invocation: %v", err)
	}
	rejectedAfterStartPayload := runtimeToolAuditTestPayload("rejected")
	rejectedAfterStartPayload["toolCallHash"] = open.ToolCallHash
	rejectedAfterStartPayload["call"] = open.Call
	rejectedAfterStartPayload["errorCode"] = "RUNTIME_PERMISSION_DENIED"
	rejectedAfterStart, err := parseRuntimeToolAuditEvent("tool.call.rejected", rejectedAfterStartPayload)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.WithTx(ctx, func(tx *persistence.Tx) error {
		return projectRuntimeToolInvocationTx(ctx, tx, runID, rejectedAfterStart)
	}); err == nil || err.Error() != "RUNTIME_EVENT_GAP" {
		t.Fatalf("rejected receipt after started error=%v, want RUNTIME_EVENT_GAP", err)
	}
	var openBeforeConvergence string
	if err := database.Pool.QueryRow(ctx, `select status from runtime_tool_invocations where tool_invocation_id=$1`, runtimeToolInvocationID(runID, open.ToolCallHash)).Scan(&openBeforeConvergence); err != nil {
		t.Fatalf("read open invocation before terminal convergence: %v", err)
	}
	if openBeforeConvergence != "started" {
		t.Fatalf("rejected receipt rewrote open invocation status=%q", openBeforeConvergence)
	}
	if err := database.WithTx(ctx, func(tx *persistence.Tx) error {
		return abortOutstandingRuntimeToolInvocationsTx(ctx, tx, runID)
	}); err != nil {
		t.Fatalf("terminalize open invocation: %v", err)
	}

	var finishedStatus, rejectedStatus, openStatus, openCode string
	if err := database.Pool.QueryRow(ctx, `select status from runtime_tool_invocations where tool_invocation_id=$1`, runtimeToolInvocationID(runID, started.ToolCallHash)).Scan(&finishedStatus); err != nil {
		t.Fatalf("read finished invocation: %v", err)
	}
	if err := database.Pool.QueryRow(ctx, `select status from runtime_tool_invocations where tool_invocation_id=$1`, runtimeToolInvocationID(runID, directRejected.ToolCallHash)).Scan(&rejectedStatus); err != nil {
		t.Fatalf("read direct rejected invocation: %v", err)
	}
	if err := database.Pool.QueryRow(ctx, `select status,coalesce(error_code,'') from runtime_tool_invocations where tool_invocation_id=$1`, runtimeToolInvocationID(runID, open.ToolCallHash)).Scan(&openStatus, &openCode); err != nil {
		t.Fatalf("read terminalized invocation: %v", err)
	}
	if finishedStatus != "succeeded" || rejectedStatus != "rejected" || openStatus != "aborted" || openCode != "RUNTIME_ABORTED" {
		t.Fatalf("stored statuses finished=%q rejected=%q open=%q code=%q", finishedStatus, rejectedStatus, openStatus, openCode)
	}
}

func runtimeToolAuditTestPayload(outcome string) map[string]any {
	return map[string]any{
		"schemaVersion": "huahuo.runtime-tool-execution-event.v1",
		"toolName":      "workspace_search",
		"toolCallHash":  runtimeToolAuditTestHash("a"),
		"argsHash":      runtimeToolAuditTestHash("b"),
		"outcome":       outcome,
		"durationMs":    0,
		"bytes":         0,
		"call":          1,
		"repeat":        1,
	}
}

func runtimeToolAuditTestHash(character string) string {
	return "sha256:" + strings.Repeat(character, 64)
}
