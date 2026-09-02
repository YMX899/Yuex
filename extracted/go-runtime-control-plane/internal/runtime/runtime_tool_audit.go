package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"

	"huahuoai/backend/source/internal/persistence"
)

const (
	runtimeToolAuditSchemaVersion = "huahuo.runtime-tool-execution-event.v1"
	runtimeToolAuditMaxCall       = 400
	runtimeToolAuditMaxDurationMs = 24 * 60 * 60 * 1000
	runtimeToolAuditMaxBytes      = 64 * 1024 * 1024
)

// runtimeToolAuditEvent is the complete hash-only tool execution boundary.
// It intentionally has no raw call ID, arguments, result, query, path,
// session, ticket, provider, or user-content field.
type runtimeToolAuditEvent struct {
	EventType         string
	SchemaVersion     string
	ToolName          string
	ToolCallHash      string
	ArgsHash          string
	ResultFingerprint string
	Outcome           string
	DurationMs        int
	Bytes             int
	Call              int
	Repeat            int
	ErrorCode         string
}

// IsRuntimeToolAuditEventType identifies the only Runtime events that may
// populate the durable tool-invocation audit. The spelling is deliberately
// exact because the event type participates in source-event identity.
func IsRuntimeToolAuditEventType(eventType string) bool {
	switch eventType {
	case "tool.call.started", "tool.call.finished", "tool.call.rejected":
		return true
	default:
		return false
	}
}

// NormalizeRuntimeToolAuditEventPayload merges the two legacy transport
// envelopes only for the strict tool-event contract. Duplicate keys are an
// ambiguity, not a precedence rule. This runs before generic sanitization so a
// raw sensitive field cannot be silently stripped and make an invalid tool
// event appear valid.
func NormalizeRuntimeToolAuditEventPayload(eventType string, data, safePayload map[string]any) (map[string]any, error) {
	if !IsRuntimeToolAuditEventType(eventType) {
		return nil, fmt.Errorf("RUNTIME_EVENT_GAP")
	}
	merged := make(map[string]any, len(data)+len(safePayload))
	for _, source := range []map[string]any{data, safePayload} {
		for key, value := range source {
			if _, exists := merged[key]; exists {
				return nil, fmt.Errorf("RUNTIME_EVENT_GAP")
			}
			merged[key] = value
		}
	}
	parsed, err := parseRuntimeToolAuditEvent(eventType, merged)
	if err != nil {
		return nil, err
	}
	return parsed.safePayload(), nil
}

func parseRuntimeToolAuditEvent(eventType string, payload map[string]any) (runtimeToolAuditEvent, error) {
	if !IsRuntimeToolAuditEventType(eventType) {
		return runtimeToolAuditEvent{}, fmt.Errorf("RUNTIME_EVENT_GAP")
	}
	allowed := map[string]bool{
		"schemaVersion": true, "toolName": true, "toolCallHash": true, "argsHash": true,
		"outcome": true, "durationMs": true, "bytes": true, "call": true, "repeat": true,
		"resultFingerprint": true, "errorCode": true,
	}
	for key := range payload {
		if !allowed[key] {
			return runtimeToolAuditEvent{}, fmt.Errorf("RUNTIME_EVENT_GAP")
		}
	}

	value := runtimeToolAuditEvent{EventType: eventType}
	var ok bool
	if value.SchemaVersion, ok = runtimeToolAuditRequiredString(payload, "schemaVersion"); !ok || value.SchemaVersion != runtimeToolAuditSchemaVersion {
		return runtimeToolAuditEvent{}, fmt.Errorf("RUNTIME_EVENT_GAP")
	}
	if value.ToolName, ok = runtimeToolAuditRequiredString(payload, "toolName"); !ok || !IsAgentFacingRuntimeTool(value.ToolName) {
		return runtimeToolAuditEvent{}, fmt.Errorf("RUNTIME_EVENT_GAP")
	}
	if value.ToolCallHash, ok = runtimeToolAuditRequiredString(payload, "toolCallHash"); !ok || !validRuntimeToolAuditHash(value.ToolCallHash) {
		return runtimeToolAuditEvent{}, fmt.Errorf("RUNTIME_EVENT_GAP")
	}
	if value.ArgsHash, ok = runtimeToolAuditRequiredString(payload, "argsHash"); !ok || !validRuntimeToolAuditHash(value.ArgsHash) {
		return runtimeToolAuditEvent{}, fmt.Errorf("RUNTIME_EVENT_GAP")
	}
	if value.Outcome, ok = runtimeToolAuditRequiredString(payload, "outcome"); !ok {
		return runtimeToolAuditEvent{}, fmt.Errorf("RUNTIME_EVENT_GAP")
	}
	if value.DurationMs, ok = runtimeToolAuditRequiredInt(payload, "durationMs", 0, runtimeToolAuditMaxDurationMs); !ok {
		return runtimeToolAuditEvent{}, fmt.Errorf("RUNTIME_EVENT_GAP")
	}
	if value.Bytes, ok = runtimeToolAuditRequiredInt(payload, "bytes", 0, runtimeToolAuditMaxBytes); !ok {
		return runtimeToolAuditEvent{}, fmt.Errorf("RUNTIME_EVENT_GAP")
	}
	if value.Call, ok = runtimeToolAuditRequiredInt(payload, "call", 1, runtimeToolAuditMaxCall); !ok {
		return runtimeToolAuditEvent{}, fmt.Errorf("RUNTIME_EVENT_GAP")
	}
	if value.Repeat, ok = runtimeToolAuditRequiredInt(payload, "repeat", 1, runtimeToolAuditMaxCall); !ok {
		return runtimeToolAuditEvent{}, fmt.Errorf("RUNTIME_EVENT_GAP")
	}
	if value.Repeat > value.Call {
		return runtimeToolAuditEvent{}, fmt.Errorf("RUNTIME_EVENT_GAP")
	}
	if value.ResultFingerprint, ok = runtimeToolAuditOptionalString(payload, "resultFingerprint"); !ok || (value.ResultFingerprint != "" && !validRuntimeToolAuditHash(value.ResultFingerprint)) {
		return runtimeToolAuditEvent{}, fmt.Errorf("RUNTIME_EVENT_GAP")
	}
	if value.ErrorCode, ok = runtimeToolAuditOptionalString(payload, "errorCode"); !ok || (value.ErrorCode != "" && !validRuntimeToolAuditErrorCode(value.ErrorCode)) {
		return runtimeToolAuditEvent{}, fmt.Errorf("RUNTIME_EVENT_GAP")
	}

	switch eventType {
	case "tool.call.started":
		if value.Outcome != "started" || value.ResultFingerprint != "" || value.ErrorCode != "" || value.DurationMs != 0 || value.Bytes != 0 {
			return runtimeToolAuditEvent{}, fmt.Errorf("RUNTIME_EVENT_GAP")
		}
	case "tool.call.finished":
		if value.Outcome != "succeeded" && value.Outcome != "failed" && value.Outcome != "aborted" || value.ResultFingerprint == "" {
			return runtimeToolAuditEvent{}, fmt.Errorf("RUNTIME_EVENT_GAP")
		}
		if (value.Outcome == "failed" || value.Outcome == "aborted") != (value.ErrorCode != "") {
			return runtimeToolAuditEvent{}, fmt.Errorf("RUNTIME_EVENT_GAP")
		}
	case "tool.call.rejected":
		if value.Outcome != "rejected" || value.ResultFingerprint != "" || value.ErrorCode == "" || value.DurationMs != 0 || value.Bytes != 0 {
			return runtimeToolAuditEvent{}, fmt.Errorf("RUNTIME_EVENT_GAP")
		}
	}
	return value, nil
}

func runtimeToolAuditRequiredString(payload map[string]any, key string) (string, bool) {
	value, exists := payload[key]
	if !exists {
		return "", false
	}
	text, ok := value.(string)
	if !ok || text == "" || text != strings.TrimSpace(text) || len(text) > 256 {
		return "", false
	}
	return text, true
}

func runtimeToolAuditOptionalString(payload map[string]any, key string) (string, bool) {
	value, exists := payload[key]
	if !exists {
		return "", true
	}
	text, ok := value.(string)
	if !ok || text == "" || text != strings.TrimSpace(text) || len(text) > 256 {
		return "", false
	}
	return text, true
}

func runtimeToolAuditRequiredInt(payload map[string]any, key string, minimum, maximum int) (int, bool) {
	value, exists := payload[key]
	if !exists {
		return 0, false
	}
	parsed, ok := runtimeToolAuditInt(value)
	if !ok || parsed < int64(minimum) || parsed > int64(maximum) {
		return 0, false
	}
	return int(parsed), true
}

func runtimeToolAuditInt(value any) (int64, bool) {
	switch typed := value.(type) {
	case int:
		return int64(typed), true
	case int8:
		return int64(typed), true
	case int16:
		return int64(typed), true
	case int32:
		return int64(typed), true
	case int64:
		return typed, true
	case uint:
		if uint64(typed) > math.MaxInt64 {
			return 0, false
		}
		return int64(typed), true
	case uint8:
		return int64(typed), true
	case uint16:
		return int64(typed), true
	case uint32:
		return int64(typed), true
	case uint64:
		if typed > math.MaxInt64 {
			return 0, false
		}
		return int64(typed), true
	case float64:
		if math.IsNaN(typed) || math.IsInf(typed, 0) || math.Trunc(typed) != typed || typed < math.MinInt64 || typed > math.MaxInt64 {
			return 0, false
		}
		return int64(typed), true
	case float32:
		converted := float64(typed)
		if math.IsNaN(converted) || math.IsInf(converted, 0) || math.Trunc(converted) != converted || converted < math.MinInt64 || converted > math.MaxInt64 {
			return 0, false
		}
		return int64(converted), true
	case json.Number:
		parsed, err := strconv.ParseInt(string(typed), 10, 64)
		return parsed, err == nil
	default:
		return 0, false
	}
}

func validRuntimeToolAuditHash(value string) bool {
	if len(value) != len("sha256:")+64 || !strings.HasPrefix(value, "sha256:") {
		return false
	}
	for _, character := range value[len("sha256:"):] {
		if !(character >= '0' && character <= '9' || character >= 'a' && character <= 'f') {
			return false
		}
	}
	return true
}

func validRuntimeToolAuditErrorCode(value string) bool {
	switch value {
	case "RUNTIME_ABORTED", "RUNTIME_ABORT_FAILED", "RUNTIME_FAILED", "RUNTIME_INPUT_INVALID",
		"RUNTIME_PERMISSION_DENIED", "RUNTIME_RUN_STALLED", "RUNTIME_TIMEOUT",
		"RUNTIME_TOOL_BUDGET_EXCEEDED", "RUNTIME_TOOL_LOOP_DETECTED", "RUNTIME_TOOL_UNAVAILABLE":
		return true
	default:
		return false
	}
}

func (event runtimeToolAuditEvent) safePayload() map[string]any {
	payload := map[string]any{
		"schemaVersion": event.SchemaVersion,
		"toolName":      event.ToolName,
		"toolCallHash":  event.ToolCallHash,
		"argsHash":      event.ArgsHash,
		"outcome":       event.Outcome,
		"durationMs":    event.DurationMs,
		"bytes":         event.Bytes,
		"call":          event.Call,
		"repeat":        event.Repeat,
	}
	if event.ResultFingerprint != "" {
		payload["resultFingerprint"] = event.ResultFingerprint
	}
	if event.ErrorCode != "" {
		payload["errorCode"] = event.ErrorCode
	}
	return payload
}

func runtimeToolInvocationID(runID, toolCallHash string) string {
	digest := sha256.Sum256([]byte(runID + "\x00" + toolCallHash))
	return "runtime_tool_" + hex.EncodeToString(digest[:])
}

func (event runtimeToolAuditEvent) summaryJSON() string {
	payload, _ := json.Marshal(map[string]any{
		"schemaVersion": event.SchemaVersion,
		"toolCallHash":  event.ToolCallHash,
		"call":          event.Call,
		"repeat":        event.Repeat,
		"bytes":         event.Bytes,
	})
	return string(payload)
}

type runtimeToolAuditInvocation struct {
	InvocationID      string
	RunID             string
	ToolName          string
	ArgsHash          string
	WorkspaceVersion  int64
	ResultFingerprint string
	Status            string
	Repeat            int
	DurationMs        int
	Bytes             int
	Call              int
	ErrorCode         string
	SchemaVersion     string
	ToolCallHash      string
	CacheHit          bool
	TerminalConverged bool
}

// projectRuntimeToolInvocationTx mirrors only a validated hash-only tool
// event. The Runtime's workspace value is deliberately ignored: the version
// comes from the durable Run record locked in this same transaction.
func projectRuntimeToolInvocationTx(ctx context.Context, tx *persistence.Tx, runID string, event runtimeToolAuditEvent) error {
	runRows, err := tx.Query(ctx, `select workspace_version from runtime_run_records where run_id=@run for update`, map[string]any{"run": runID})
	if err != nil {
		return err
	}
	if len(runRows) != 1 {
		return fmt.Errorf("RUNTIME_RUN_RECORD_UNAVAILABLE")
	}
	workspaceVersion := runtimeHostInt64(runRows[0]["workspace_version"])
	if workspaceVersion < 1 {
		return fmt.Errorf("RUNTIME_RUN_RECORD_UNAVAILABLE")
	}

	invocationID := runtimeToolInvocationID(runID, event.ToolCallHash)
	rows, err := tx.Query(ctx, `
select tool_name,args_hash,workspace_version,coalesce(result_fingerprint,'') as result_fingerprint,
	       status,repeat_count,cache_hit,coalesce(duration_ms,0) as duration_ms,coalesce(error_code,'') as error_code,
	       coalesce(safe_summary->>'schemaVersion','') as schema_version,
	       coalesce(safe_summary->>'toolCallHash','') as tool_call_hash,
	       coalesce((safe_summary->>'call')::bigint,0) as call,
	       coalesce((safe_summary->>'bytes')::bigint,0) as bytes
from runtime_tool_invocations
where tool_invocation_id=@id
for update`, map[string]any{"id": invocationID})
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		switch event.Outcome {
		case "started", "rejected":
			return insertRuntimeToolInvocationTx(ctx, tx, invocationID, runID, workspaceVersion, event)
		default:
			return fmt.Errorf("RUNTIME_EVENT_GAP")
		}
	}
	if len(rows) != 1 || !runtimeToolInvocationImmutableMatches(rows[0], workspaceVersion, event) {
		return fmt.Errorf("RUNTIME_EVENT_GAP")
	}
	status := fmt.Sprint(rows[0]["status"])
	if status == "started" {
		if event.Outcome == "started" {
			if runtimeToolInvocationMatchesEvent(rows[0], event) {
				return nil
			}
			return fmt.Errorf("RUNTIME_EVENT_GAP")
		}
		// A rejected receipt is a direct terminal fact, not a completion of a
		// previously reserved call. Only finished may close an existing start.
		if event.EventType != "tool.call.finished" {
			return fmt.Errorf("RUNTIME_EVENT_GAP")
		}
		return updateRuntimeToolInvocationTerminalTx(ctx, tx, invocationID, event)
	}
	if runtimeToolInvocationMatchesEvent(rows[0], event) {
		return nil
	}
	return fmt.Errorf("RUNTIME_EVENT_GAP")
}

func insertRuntimeToolInvocationTx(ctx context.Context, tx *persistence.Tx, invocationID, runID string, workspaceVersion int64, event runtimeToolAuditEvent) error {
	resultFingerprint := any(nil)
	if event.ResultFingerprint != "" {
		resultFingerprint = event.ResultFingerprint
	}
	errorCode := any(nil)
	if event.ErrorCode != "" {
		errorCode = event.ErrorCode
	}
	return tx.Exec(ctx, `
insert into runtime_tool_invocations(
  tool_invocation_id,run_id,tool_name,args_hash,workspace_version,result_fingerprint,
  status,repeat_count,cache_hit,duration_ms,error_code,safe_summary
) values(
  @id,@run,@tool,@args,@workspace,@result,@status,@repeat,false,@duration,@error,@summary::jsonb
)`, map[string]any{
		"id": invocationID, "run": runID, "tool": event.ToolName, "args": event.ArgsHash,
		"workspace": workspaceVersion, "result": resultFingerprint, "status": event.Outcome,
		"repeat": event.Repeat, "duration": event.DurationMs, "error": errorCode, "summary": event.summaryJSON(),
	})
}

func updateRuntimeToolInvocationTerminalTx(ctx context.Context, tx *persistence.Tx, invocationID string, event runtimeToolAuditEvent) error {
	resultFingerprint := any(nil)
	if event.ResultFingerprint != "" {
		resultFingerprint = event.ResultFingerprint
	}
	errorCode := any(nil)
	if event.ErrorCode != "" {
		errorCode = event.ErrorCode
	}
	tag, err := tx.ExecRaw(ctx, `
update runtime_tool_invocations
set result_fingerprint=$2,status=$3,repeat_count=$4,duration_ms=$5,error_code=$6,safe_summary=$7::jsonb
where tool_invocation_id=$1 and status='started'`, invocationID, resultFingerprint, event.Outcome, event.Repeat, event.DurationMs, errorCode, event.summaryJSON())
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("RUNTIME_EVENT_GAP")
	}
	return nil
}

func runtimeToolInvocationImmutableMatches(row map[string]any, workspaceVersion int64, event runtimeToolAuditEvent) bool {
	return fmt.Sprint(row["tool_name"]) == event.ToolName &&
		fmt.Sprint(row["args_hash"]) == event.ArgsHash &&
		runtimeHostInt64(row["workspace_version"]) == workspaceVersion &&
		!runtimeToolInvocationBool(row["cache_hit"]) &&
		fmt.Sprint(row["schema_version"]) == event.SchemaVersion &&
		fmt.Sprint(row["tool_call_hash"]) == event.ToolCallHash &&
		runtimeHostInt64(row["call"]) == int64(event.Call)
}

func runtimeToolInvocationMatchesEvent(row map[string]any, event runtimeToolAuditEvent) bool {
	return fmt.Sprint(row["status"]) == event.Outcome &&
		fmt.Sprint(row["result_fingerprint"]) == event.ResultFingerprint &&
		runtimeHostInt64(row["repeat_count"]) == int64(event.Repeat) &&
		runtimeHostInt64(row["duration_ms"]) == int64(event.DurationMs) &&
		fmt.Sprint(row["error_code"]) == event.ErrorCode &&
		fmt.Sprint(row["schema_version"]) == event.SchemaVersion &&
		fmt.Sprint(row["tool_call_hash"]) == event.ToolCallHash &&
		runtimeHostInt64(row["call"]) == int64(event.Call) &&
		runtimeHostInt64(row["bytes"]) == int64(event.Bytes)
}

func runtimeToolInvocationBool(value any) bool {
	typed, ok := value.(bool)
	return ok && typed
}

// projectRuntimeToolInvocationMemoryLocked mirrors the database state machine
// for contract tests. The caller owns RuntimeHostRepository.mu.
func (r *RuntimeHostRepository) projectRuntimeToolInvocationMemoryLocked(runID string, event runtimeToolAuditEvent) error {
	record, ok := r.runtimeRunRecords[runID]
	if !ok || record.WorkspaceVersion < 1 {
		return fmt.Errorf("RUNTIME_RUN_RECORD_UNAVAILABLE")
	}
	invocationID := runtimeToolInvocationID(runID, event.ToolCallHash)
	stored, exists := r.toolInvocations[invocationID]
	if !exists {
		if event.Outcome != "started" && event.Outcome != "rejected" {
			return fmt.Errorf("RUNTIME_EVENT_GAP")
		}
		r.toolInvocations[invocationID] = runtimeToolAuditInvocation{
			InvocationID: invocationID, RunID: runID, ToolName: event.ToolName, ArgsHash: event.ArgsHash,
			WorkspaceVersion: record.WorkspaceVersion, ResultFingerprint: event.ResultFingerprint,
			Status: event.Outcome, Repeat: event.Repeat, DurationMs: event.DurationMs, Bytes: event.Bytes,
			Call: event.Call, ErrorCode: event.ErrorCode, SchemaVersion: event.SchemaVersion,
			ToolCallHash: event.ToolCallHash,
		}
		return nil
	}
	if stored.ToolName != event.ToolName || stored.ArgsHash != event.ArgsHash || stored.WorkspaceVersion != record.WorkspaceVersion || stored.CacheHit ||
		stored.SchemaVersion != event.SchemaVersion || stored.ToolCallHash != event.ToolCallHash || stored.Call != event.Call {
		return fmt.Errorf("RUNTIME_EVENT_GAP")
	}
	if stored.Status == "started" {
		if event.Outcome == "started" {
			if runtimeToolAuditInvocationMatchesEvent(stored, event) {
				return nil
			}
			return fmt.Errorf("RUNTIME_EVENT_GAP")
		}
		// Keep the in-memory contract mirror identical to the durable rule.
		if event.EventType != "tool.call.finished" {
			return fmt.Errorf("RUNTIME_EVENT_GAP")
		}
		stored.ResultFingerprint = event.ResultFingerprint
		stored.Status = event.Outcome
		stored.Repeat = event.Repeat
		stored.DurationMs = event.DurationMs
		stored.Bytes = event.Bytes
		stored.Call = event.Call
		stored.ErrorCode = event.ErrorCode
		stored.SchemaVersion = event.SchemaVersion
		stored.ToolCallHash = event.ToolCallHash
		r.toolInvocations[invocationID] = stored
		return nil
	}
	if runtimeToolAuditInvocationMatchesEvent(stored, event) {
		return nil
	}
	return fmt.Errorf("RUNTIME_EVENT_GAP")
}

func runtimeToolAuditInvocationMatchesEvent(stored runtimeToolAuditInvocation, event runtimeToolAuditEvent) bool {
	return stored.Status == event.Outcome && stored.ResultFingerprint == event.ResultFingerprint &&
		stored.Repeat == event.Repeat && stored.DurationMs == event.DurationMs && stored.Bytes == event.Bytes &&
		stored.Call == event.Call && stored.ErrorCode == event.ErrorCode && stored.SchemaVersion == event.SchemaVersion &&
		stored.ToolCallHash == event.ToolCallHash
}

// abortOutstandingRuntimeToolInvocationsTx converges only open reservations.
// It never rewrites a Runtime-provided terminal outcome and is deliberately
// called in the terminal Run transaction after the durable Run record update.
func abortOutstandingRuntimeToolInvocationsTx(ctx context.Context, tx *persistence.Tx, runID string) error {
	_, err := tx.ExecRaw(ctx, `
update runtime_tool_invocations
set status='aborted',error_code='RUNTIME_ABORTED',
    safe_summary=safe_summary || '{"terminalConvergence":true}'::jsonb
where run_id=$1 and status='started'`, runID)
	return err
}

func (r *RuntimeHostRepository) abortOutstandingRuntimeToolInvocationsMemory(runID string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for id, invocation := range r.toolInvocations {
		if invocation.RunID != runID || invocation.Status != "started" {
			continue
		}
		invocation.Status = "aborted"
		invocation.ErrorCode = "RUNTIME_ABORTED"
		invocation.TerminalConverged = true
		r.toolInvocations[id] = invocation
	}
}
