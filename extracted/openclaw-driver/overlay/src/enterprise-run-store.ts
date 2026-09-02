import { createHash } from "node:crypto";
import { mkdirSync } from "node:fs";
import { dirname, join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { validateVerifiedRuntimePolicy, type VerifiedRuntimePolicy } from "./runtime-policy.js";

export type StoredRunStatus =
  | "accepted" | "materializing" | "ready" | "running" | "finalizing"
  | "succeeded" | "failed" | "timeout" | "aborting" | "aborted"
  | "recovering" | "orphaned";

export type StoredRunIdentity = {
  runId: string;
  idempotencyKey: string;
  sessionBindingHash: string;
  workspaceManifestHash: string;
};

// These values are already bound by the private RunTicket verifier before the
// Registry accepts a production Run. They are safe control-plane identifiers,
// not a ticket, lease token, Workspace path, session key, or user content.
// A complete restart-recovery fact is recorded separately because the current
// submit envelope does not yet carry the signed execution scope or Backend
// assigned Host process generation.
export type StoredRunRecoveryBinding = {
  runtimeHostId: string;
  reservationId: string;
  fencingToken: number;
  capabilityHash: string;
};

export type RuntimeHostRecoveryFactInput = {
  runId: string;
  runtimeHostId: string;
  assignedRuntimeHostInstanceId: string;
  assignedRuntimeHostInstanceGeneration: number;
  reservationId: string;
  dispatchId: string;
  fencingToken: number;
  executionScope: "product_thread" | "detached_task";
  capabilityHash: string;
  dispatchIdentity: string;
  runTicketJtiHash: string;
  manifestHash: string;
};

export type RuntimeHostRecoveryFact = RuntimeHostRecoveryFactInput & {
  status: StoredRunStatus;
  lastEventSequence: number;
};

export type RuntimeHostRecoverySnapshotIdentity = {
  runtimeHostId: string;
  instanceId: string;
  environment: string;
  instanceGeneration: number;
  recoveryRevision: number;
  recoveryState: "pending";
};

export type RuntimeHostRecoverySnapshot = RuntimeHostRecoverySnapshotIdentity & {
  version: "runtime-host-recovery.v1";
  facts: RuntimeHostRecoveryFact[];
  factSetHash: string;
};

export type StoredRunEvent = {
  sequence: number;
  eventType: string;
  status: StoredRunStatus;
  timestamp: string;
  data?: Record<string, unknown>;
};

export type StoredRunRecord = StoredRunIdentity & {
  jtiHash: string;
  dispatchIdentity: string;
  recoveryBinding?: StoredRunRecoveryBinding;
  status: StoredRunStatus;
  nextSequence: number;
  terminalSequence: number;
  terminalResult?: unknown;
  terminalError?: unknown;
  runtimePolicy?: VerifiedRuntimePolicy;
  toolCalls: number;
  repeatedCalls: Record<string, number>;
  noProgressCalls: number;
  toolCounts: Record<string, number>;
  readBytes: number;
  abortCode?: string;
  deadlineAt: string;
  lastOutcome?: {
    toolName: string;
    argsHash: string;
    resultHash: string;
  };
  createdAt: string;
  updatedAt: string;
};

export type StoredEventPage = {
  items: StoredRunEvent[];
  nextAfterSequence: number;
  hasMore: boolean;
  oldestAvailableSequence: number;
  latestSequence: number;
  terminalSequence: number;
  gap: boolean;
};

// An orphaned Run is terminal for this Gateway instance. The Backend owns the
// authorized retry as a new attempt; this store must never resurrect it.
const TERMINAL = new Set<StoredRunStatus>(["succeeded", "failed", "timeout", "aborted", "orphaned"]);
const DEFAULT_MAX_REPLAY_EVENTS = 2000;
const MAX_RUNTIME_HOST_RECOVERY_FACTS = 512;
const RUNTIME_TOOL_AUDIT_SCHEMA_VERSION = "huahuo.runtime-tool-execution-event.v1";
const MAX_RUNTIME_TOOL_AUDIT_DURATION_MS = 24 * 60 * 60 * 1000;

export class EnterpriseRunStore {
  readonly path: string;
  readonly durable: boolean;
  private readonly database: DatabaseSync;
  private readonly maxReplayEvents: number;

  constructor(path = ":memory:", maxReplayEvents = DEFAULT_MAX_REPLAY_EVENTS) {
    if (!path || !Number.isInteger(maxReplayEvents) || maxReplayEvents < 100) throw storeError("INVALID_ARGUMENT", "invalid run store configuration");
    this.path = path;
    this.durable = path !== ":memory:";
    this.maxReplayEvents = maxReplayEvents;
    if (this.durable) mkdirSync(dirname(path), { recursive: true, mode: 0o700 });
    this.database = new DatabaseSync(path);
    this.database.exec("PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000;");
    if (this.durable) this.database.exec("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA wal_autocheckpoint=1000;");
    this.migrate();
  }

  static fromEnvironment(): EnterpriseRunStore {
    const explicit = String(process.env.HUAHUO_RUNTIME_RUN_STORE_PATH ?? "").trim();
    const stateRoot = String(process.env.OPENCLAW_STATE_DIR ?? "").trim();
    return new EnterpriseRunStore(explicit || (stateRoot ? join(stateRoot, "enterprise-runs.sqlite") : ":memory:"));
  }

  accept(
    identity: StoredRunIdentity,
    jtiHash: string,
    dispatchIdentity: string,
    runtimePolicy: VerifiedRuntimePolicy,
    recoveryBinding?: StoredRunRecoveryBinding,
  ): { created: boolean; record: StoredRunRecord } {
    validateIdentity(identity);
    validateHash(jtiHash, "jtiHash");
    validateHash(dispatchIdentity, "dispatchIdentity");
    validateRuntimePolicy(identity, dispatchIdentity, runtimePolicy);
    if (recoveryBinding) validateRecoveryBinding(recoveryBinding, runtimePolicy);
    return this.transaction(() => {
      const existing = this.get(identity.runId);
      if (existing) {
        if (!sameIdentity(existing, identity) || existing.jtiHash !== jtiHash || existing.dispatchIdentity !== dispatchIdentity ||
          existing.runtimePolicy?.policyHash !== runtimePolicy.policyHash || !sameRecoveryBinding(existing.recoveryBinding, recoveryBinding)) {
          throw storeError("RUNTIME_RUN_CONFLICT", "run identity changed");
        }
        const consumed = this.database.prepare("select run_id, dispatch_identity from runtime_consumed_run_ticket_jtis where jti_hash=?").get(jtiHash) as Record<string, unknown> | undefined;
        if (!consumed || String(consumed.run_id) !== identity.runId || String(consumed.dispatch_identity) !== dispatchIdentity) {
          throw storeError("RUNTIME_RUN_CONFLICT", "run ticket identity changed");
        }
        return { created: false, record: existing };
      }
      const consumed = this.database.prepare("select run_id from runtime_consumed_run_ticket_jtis where jti_hash=?").get(jtiHash) as Record<string, unknown> | undefined;
      if (consumed) throw storeError("RUNTIME_RUN_CONFLICT", "run ticket already consumed");
      const now = new Date().toISOString();
      const deadlineAt = new Date(Date.now() + runtimePolicy.toolBudget.maxWallTimeSeconds * 1000).toISOString();
      this.database.prepare(`insert into runtime_runs(
        run_id,idempotency_key,session_binding_hash,workspace_manifest_hash,jti_hash,dispatch_identity,status,
        runtime_host_id,reservation_id,fencing_token,capability_hash,
        next_sequence,terminal_sequence,terminal_result_json,terminal_error_json,runtime_policy_json,runtime_policy_hash,
        tool_calls,repeated_calls_json,no_progress_calls,tool_counts_json,read_bytes,abort_code,deadline_at,
        last_outcome_tool_name,last_outcome_args_hash,last_outcome_result_hash,created_at,updated_at
      ) values(?,?,?,?,?,?,?,?,?,?,?,2,0,null,null,?,?,0,'{}',0,'{}',0,null,?,null,null,null,?,?)`).run(
        identity.runId, identity.idempotencyKey, identity.sessionBindingHash, identity.workspaceManifestHash,
        jtiHash, dispatchIdentity, "accepted",
        recoveryBinding?.runtimeHostId ?? null, recoveryBinding?.reservationId ?? null,
        recoveryBinding?.fencingToken ?? null, recoveryBinding?.capabilityHash ?? null,
        JSON.stringify(runtimePolicy), runtimePolicy.policyHash, deadlineAt, now, now,
      );
      this.database.prepare("insert into runtime_consumed_run_ticket_jtis(jti_hash,run_id,dispatch_identity,consumed_at) values(?,?,?,?)").run(jtiHash, identity.runId, dispatchIdentity, now);
      this.insertEvent(identity.runId, 1, "run.accepted", "accepted", now, undefined);
      return { created: true, record: this.require(identity.runId) };
    });
  }

  get(runId: string): StoredRunRecord | undefined {
    if (!runId) return undefined;
    const row = this.database.prepare("select * from runtime_runs where run_id=?").get(runId) as Record<string, unknown> | undefined;
    return row ? rowToRun(row) : undefined;
  }

  appendEvent(runId: string, event: Omit<StoredRunEvent, "sequence" | "timestamp"> & { timestamp?: string }): StoredRunEvent {
    validateEvent(runId, event.eventType, event.status);
    return this.transaction(() => {
      const record = this.require(runId);
      if (record.status === "orphaned") throw storeError("RUNTIME_RUN_CONFLICT", "orphaned run requires a new attempt");
      // An external abort is durable before its process-local signal. A
      // session-queued execution must not revive that Run by appending a late
      // run.started/running event after the caller has cancelled it.
      if (record.status === "aborting" && record.abortCode === "RUNTIME_ABORTED") {
        throw storeError("RUNTIME_ABORTED", "run is aborting");
      }
      const timestamp = event.timestamp || new Date().toISOString();
      const sequence = record.nextSequence;
      const stored: StoredRunEvent = { sequence, eventType: event.eventType, status: event.status, timestamp, ...(event.data ? { data: event.data } : {}) };
      this.insertEvent(runId, sequence, event.eventType, event.status, timestamp, event.data);
      this.database.prepare("update runtime_runs set status=?,next_sequence=?,updated_at=? where run_id=?").run(event.status, sequence + 1, timestamp, runId);
      this.trimEvents(runId, sequence);
      return stored;
    });
  }

  listEvents(runId: string, afterSequence = 0, limit = 100): StoredEventPage {
    if (!Number.isInteger(afterSequence) || afterSequence < 0 || !Number.isInteger(limit) || limit < 1 || limit > 500) throw storeError("INVALID_ARGUMENT", "invalid event cursor");
    const record = this.require(runId);
    const bounds = this.database.prepare("select coalesce(min(sequence),0) oldest,coalesce(max(sequence),0) latest from runtime_run_events where run_id=?").get(runId) as Record<string, unknown>;
    const oldest = Number(bounds.oldest ?? 0);
    const latest = Number(bounds.latest ?? 0);
    const gap = oldest > 0 && afterSequence < oldest - 1;
    const effectiveAfter = gap ? oldest - 1 : afterSequence;
    const rows = this.database.prepare("select sequence,event_type,status,timestamp,data_json from runtime_run_events where run_id=? and sequence>? order by sequence limit ?").all(runId, effectiveAfter, limit + 1) as Array<Record<string, unknown>>;
    const items = rows.slice(0, limit).map(rowToEvent);
    return {
      items,
      nextAfterSequence: items.at(-1)?.sequence ?? afterSequence,
      hasMore: rows.length > limit,
      oldestAvailableSequence: oldest,
      latestSequence: latest,
      terminalSequence: record.terminalSequence,
      gap,
    };
  }

  complete(runId: string, terminal: { status: Extract<StoredRunStatus, "succeeded" | "failed" | "timeout" | "aborted">; result?: unknown; error?: unknown }): { changed: boolean; record: StoredRunRecord; event?: StoredRunEvent } {
    return this.transaction(() => {
      const current = this.require(runId);
      if (TERMINAL.has(current.status)) {
        if (current.status !== terminal.status) throw storeError("RUNTIME_RUN_CONFLICT", "terminal status changed");
        return { changed: false, record: current };
      }
      // A confirmed external abort wins a race with the underlying model
      // transport. Some model implementations surface an AbortSignal as a
      // timeout; persisting that as a timeout would turn a user cancellation
      // into a retryable runtime failure.
      if (current.abortCode === "RUNTIME_ABORTED") {
        terminal = { status: "aborted", error: { code: "RUNTIME_ABORTED" } };
      }
      const timestamp = new Date().toISOString();
      const sequence = current.nextSequence;
      const event: StoredRunEvent = { sequence, eventType: `run.${terminal.status}`, status: terminal.status, timestamp };
      this.insertEvent(runId, sequence, event.eventType, event.status, timestamp, undefined);
      this.database.prepare(`update runtime_runs set status=?,next_sequence=?,terminal_sequence=?,terminal_result_json=?,terminal_error_json=?,updated_at=? where run_id=?`).run(
        terminal.status, sequence + 1, sequence, encodeJSON(terminal.result), encodeJSON(terminal.error), timestamp, runId,
      );
      this.trimEvents(runId, sequence);
      return { changed: true, record: this.require(runId), event };
    });
  }

  requestExternalAbort(runId: string): { changed: boolean; record: StoredRunRecord; event?: StoredRunEvent } {
    return this.transaction(() => {
      const current = this.require(runId);
      if (TERMINAL.has(current.status) || current.status === "aborting") return { changed: false, record: current };
      const timestamp = new Date().toISOString();
      const event: StoredRunEvent = {
        sequence: current.nextSequence,
        eventType: "run.abort.requested",
        status: "aborting",
        timestamp,
        data: { code: "RUNTIME_ABORTED" },
      };
      this.insertEvent(runId, event.sequence, event.eventType, event.status, timestamp, event.data);
      this.database.prepare("update runtime_runs set status=?,next_sequence=?,abort_code=?,updated_at=? where run_id=?").run(
        "aborting", current.nextSequence + 1, "RUNTIME_ABORTED", timestamp, runId,
      );
      this.trimEvents(runId, event.sequence);
      return { changed: true, record: this.require(runId), event };
    });
  }

  assertToolCallAllowed(runId: string, input: {
    toolName: string;
    toolCallId: string;
    toolCallHash: string;
    argsHash: string;
  }): { allowed: boolean; record: StoredRunRecord; events: StoredRunEvent[]; code?: string } {
    if (!input.toolName || !input.toolCallId || input.toolName.length > 128 || input.toolCallId.length > 512 ||
      !isRuntimeToolAuditHash(input.toolCallHash) || !isRuntimeToolAuditHash(input.argsHash) ||
      input.toolCallHash !== toolCallKey(input.toolCallId)) {
      throw storeError("INVALID_ARGUMENT", "invalid tool call reservation");
    }
    return this.transaction(() => {
      const current = this.require(runId);
      if (TERMINAL.has(current.status) || current.status === "aborting") {
        return { allowed: false, record: current, events: [], code: current.abortCode ?? "RUNTIME_ABORTED" };
      }
      if (!current.runtimePolicy) throw storeError("RUNTIME_POLICY_INVALID", "runtime policy is missing from durable run state");
      const policy = current.runtimePolicy;
      const budget = policy.toolBudget;
      const toolName = input.toolName;
      const callKey = toolCallKey(input.toolCallId);
      const duplicate = this.database.prepare("select call_key from runtime_tool_call_reservations where run_id=? and call_key=?").get(runId, callKey);
      if (duplicate) return this.rejectToolCall(current, input, "RUNTIME_PERMISSION_DENIED", "duplicate_tool_call", true);
      if (isDeadlineElapsed(current.deadlineAt)) return this.rejectToolCall(current, input, "RUNTIME_TIMEOUT", "max_wall_time");
      if (!policy.allowedTools.includes(toolName)) return this.rejectToolCall(current, input, "RUNTIME_PERMISSION_DENIED", "tool_not_allowed");
      const category = toolCategory(toolName);
      const nextToolCall = current.toolCalls + 1;
      if (current.toolCalls >= budget.maxToolCalls) return this.rejectToolCall(current, input, "RUNTIME_TOOL_BUDGET_EXCEEDED", "max_tool_calls");
      if (category === "search" && current.toolCalls >= budget.maxToolCalls - budget.finalizationReserve) {
        return this.rejectToolCall(current, input, "RUNTIME_TOOL_BUDGET_EXCEEDED", "finalization_reserve");
      }
      if (category && categoryBudgetReached(current.toolCounts, category, budget)) {
        return this.rejectToolCall(current, input, "RUNTIME_TOOL_BUDGET_EXCEEDED", `${category}_quota`);
      }
      const timestamp = new Date().toISOString();
      const toolCounts = { ...current.toolCounts };
      if (category) toolCounts[category] = (toolCounts[category] ?? 0) + 1;
      const events: StoredRunEvent[] = [];
      let next = current.nextSequence;
      const append = (eventType: string, status: StoredRunStatus, data?: Record<string, unknown>) => {
        const event: StoredRunEvent = { sequence: next++, eventType, status, timestamp, ...(data ? { data } : {}) };
        this.insertEvent(runId, event.sequence, eventType, status, timestamp, data);
        events.push(event);
      };
      append("tool.call.started", current.status, runtimeToolAuditPayload({
        toolName,
        toolCallHash: input.toolCallHash,
        argsHash: input.argsHash,
        outcome: "started",
        durationMs: 0,
        bytes: 0,
        call: nextToolCall,
        repeat: (current.repeatedCalls[`${toolName}:${input.argsHash}`] ?? 0) + 1,
      }));
      if (nextToolCall >= budget.softToolCallLimit && nextToolCall < budget.maxToolCalls - budget.finalizationReserve) {
        append("budget.warning", current.status, { toolCalls: nextToolCall, remaining: budget.maxToolCalls - nextToolCall });
      }
      if (nextToolCall === budget.maxToolCalls - budget.finalizationReserve) {
        append("budget.finalization.reserve", current.status, { remainingForSearch: 0 });
      }
      this.database.prepare("insert into runtime_tool_call_reservations(run_id,call_key,tool_name,tool_call_hash,args_hash,call_number,outcome_recorded,created_at) values(?,?,?,?,?,?,0,?)").run(
        runId, callKey, toolName, input.toolCallHash, input.argsHash, nextToolCall, timestamp,
      );
      this.database.prepare("update runtime_runs set next_sequence=?,tool_calls=?,tool_counts_json=?,updated_at=? where run_id=?").run(
        next, nextToolCall, JSON.stringify(toolCounts), timestamp, runId,
      );
      this.trimEvents(runId, next - 1);
      return { allowed: true, record: this.require(runId), events };
    });
  }

  rejectToolCallForPolicy(runId: string, toolName: string, code: string, reason: string): {
    allowed: false;
    record: StoredRunRecord;
    events: StoredRunEvent[];
    code: string;
  } {
    if (!toolName || toolName.length > 128 || !code || code.length > 128 || !reason || reason.length > 128) {
      throw storeError("INVALID_ARGUMENT", "invalid tool policy rejection");
    }
    return this.transaction(() => {
      const current = this.require(runId);
      if (TERMINAL.has(current.status) || current.status === "aborting") {
        return { allowed: false, record: current, events: [], code: current.abortCode ?? code };
      }
      const toolCallHash = rejectedToolAuditHash(runId, toolName, reason, current.nextSequence);
      const argsHash = rejectedToolAuditHash(runId, toolName, `${reason}:args`, current.nextSequence);
      return this.rejectToolCall(current, { toolName, toolCallId: toolCallHash, toolCallHash, argsHash }, code, reason);
    });
  }

  recordToolOutcome(runId: string, outcome: {
    toolName: string;
    toolCallId: string;
    argsHash: string;
    resultFingerprint: string;
    progress?: boolean;
    resultBytes?: number;
  }): { record: StoredRunRecord; events: StoredRunEvent[]; abortCode?: string } {
    if (!outcome.toolName || !outcome.toolCallId || !isRuntimeToolAuditHash(outcome.argsHash) || !isRuntimeToolAuditHash(outcome.resultFingerprint)) {
      throw storeError("INVALID_ARGUMENT", "invalid tool outcome");
    }
    if (outcome.progress !== undefined && typeof outcome.progress !== "boolean") throw storeError("INVALID_ARGUMENT", "invalid tool progress outcome");
    if (outcome.resultBytes !== undefined && (!Number.isSafeInteger(outcome.resultBytes) || outcome.resultBytes < 0)) throw storeError("INVALID_ARGUMENT", "invalid tool byte outcome");
    return this.transaction(() => {
      const current = this.require(runId);
      if (TERMINAL.has(current.status) || current.status === "aborting") return { record: current, events: [] };
      if (!current.runtimePolicy) throw storeError("RUNTIME_POLICY_INVALID", "runtime policy is missing from durable run state");
      const budget = current.runtimePolicy.toolBudget;
      const reservation = this.database.prepare("select tool_name,tool_call_hash,args_hash,call_number,outcome_recorded,created_at from runtime_tool_call_reservations where run_id=? and call_key=?").get(runId, toolCallKey(outcome.toolCallId)) as Record<string, unknown> | undefined;
      if (!reservation || String(reservation.tool_name) !== outcome.toolName || String(reservation.args_hash) !== outcome.argsHash ||
        !isRuntimeToolAuditHash(String(reservation.tool_call_hash)) || Number(reservation.outcome_recorded) !== 0) {
        throw storeError("RUNTIME_POLICY_INVALID", "tool outcome has no matching reservation");
      }
      const repeatedCalls = { ...current.repeatedCalls };
      // Repetition is defined by the model's invocation, not by whether a
      // non-deterministic tool happened to return a different result.
      const fingerprint = `${outcome.toolName}:${String(reservation.args_hash)}`;
      const repeated = (repeatedCalls[fingerprint] ?? 0) + 1;
      repeatedCalls[fingerprint] = repeated;
      const toolCalls = current.toolCalls;
      const previousOutcome = current.lastOutcome;
      // A new argument which keeps producing the same result is still no
      // progress. Count the first result in the streak so a four-result policy
      // aborts on the fourth result, as opposed to a fifth invocation.
      const noProgressCalls = previousOutcome && previousOutcome.toolName === outcome.toolName &&
        previousOutcome.resultHash === outcome.resultFingerprint
        ? current.noProgressCalls + 1
        : 1;
      const readBytes = current.readBytes + (outcome.toolName === "read" ? outcome.resultBytes ?? 0 : 0);
      const events: StoredRunEvent[] = [];
      let next = current.nextSequence;
      const timestamp = new Date().toISOString();
      const append = (eventType: string, status: StoredRunStatus, data?: Record<string, unknown>) => {
        const event: StoredRunEvent = { sequence: next++, eventType, status, timestamp, ...(data ? { data } : {}) };
        this.insertEvent(runId, event.sequence, eventType, status, timestamp, data);
        events.push(event);
      };
      let status: StoredRunStatus = current.status;
      let abortCode: string | undefined;
      if (toolCalls > budget.maxToolCalls) abortCode = "RUNTIME_TOOL_BUDGET_EXCEEDED";
      else if (readBytes > budget.maxReadBytes) abortCode = "RUNTIME_TOOL_BUDGET_EXCEEDED";
      else if (repeated > budget.maxRepeatedCalls) abortCode = "RUNTIME_TOOL_LOOP_DETECTED";
      else if (noProgressCalls >= budget.maxNoProgressCalls) abortCode = "RUNTIME_RUN_STALLED";
      const durationMs = runtimeToolAuditDurationMs(String(reservation.created_at));
      append("tool.call.finished", current.status, runtimeToolAuditPayload({
        toolName: outcome.toolName,
        toolCallHash: String(reservation.tool_call_hash),
        argsHash: String(reservation.args_hash),
        resultFingerprint: outcome.resultFingerprint,
        outcome: abortCode ? "aborted" : "succeeded",
        durationMs,
        bytes: outcome.resultBytes ?? 0,
        call: Number(reservation.call_number),
        repeat: repeated,
        ...(abortCode ? { errorCode: abortCode } : {}),
      }));
      if (abortCode) {
        status = "aborting";
        append("run.policy.abort", status, { code: abortCode, toolCalls, noProgressCalls });
      }
      this.database.prepare("update runtime_tool_call_reservations set outcome_recorded=1 where run_id=? and call_key=?").run(runId, toolCallKey(outcome.toolCallId));
      this.database.prepare("update runtime_runs set status=?,next_sequence=?,repeated_calls_json=?,no_progress_calls=?,read_bytes=?,abort_code=?,last_outcome_tool_name=?,last_outcome_args_hash=?,last_outcome_result_hash=?,updated_at=? where run_id=?").run(
        status, next, JSON.stringify(repeatedCalls), noProgressCalls, readBytes, abortCode ?? null,
        outcome.toolName, String(reservation.args_hash), outcome.resultFingerprint, timestamp, runId,
      );
      this.trimEvents(runId, next - 1);
      return { record: this.require(runId), events, ...(abortCode ? { abortCode } : {}) };
    });
  }

  abortForPolicy(runId: string, code: string, reason: string): { changed: boolean; record: StoredRunRecord; event?: StoredRunEvent } {
    return this.transaction(() => {
      const current = this.require(runId);
      if (TERMINAL.has(current.status) || current.status === "aborting") return { changed: false, record: current };
      const timestamp = new Date().toISOString();
      const event: StoredRunEvent = {
        sequence: current.nextSequence,
        eventType: "run.policy.abort",
        status: "aborting",
        timestamp,
        data: { code, reason },
      };
      this.insertEvent(runId, event.sequence, event.eventType, event.status, timestamp, event.data);
      this.database.prepare("update runtime_runs set status=?,next_sequence=?,abort_code=?,updated_at=? where run_id=?").run(
        "aborting", current.nextSequence + 1, code, timestamp, runId,
      );
      this.trimEvents(runId, event.sequence);
      return { changed: true, record: this.require(runId), event };
    });
  }

  private rejectToolCall(current: StoredRunRecord, input: { toolName: string; toolCallId: string; toolCallHash: string; argsHash: string }, code: string, reason: string, forceDistinctAuditHash = false): { allowed: false; record: StoredRunRecord; events: StoredRunEvent[]; code: string } {
    const timestamp = new Date().toISOString();
    const canEmitAuditReceipt = isRuntimeAuditToolName(input.toolName);
    const toolCallHash = forceDistinctAuditHash
      ? rejectedToolAuditHash(current.runId, input.toolCallHash, reason, current.nextSequence)
      : input.toolCallHash;
    const argsHash = forceDistinctAuditHash
      ? rejectedToolAuditHash(current.runId, input.argsHash, `${reason}:args`, current.nextSequence)
      : input.argsHash;
    const event: StoredRunEvent = canEmitAuditReceipt
      ? {
          sequence: current.nextSequence,
          eventType: "tool.call.rejected",
          status: "aborting",
          timestamp,
          data: runtimeToolAuditPayload({
            toolName: input.toolName,
            toolCallHash,
            argsHash,
            outcome: "rejected",
            durationMs: 0,
            bytes: 0,
            call: Math.min(400, current.toolCalls + 1),
            repeat: 1,
            errorCode: code,
          }),
        }
      : {
          sequence: current.nextSequence,
          eventType: "run.policy.abort",
          status: "aborting",
          timestamp,
          data: { code, reason },
        };
    this.insertEvent(current.runId, event.sequence, event.eventType, event.status, timestamp, event.data);
    this.database.prepare("update runtime_runs set status=?,next_sequence=?,abort_code=?,updated_at=? where run_id=?").run(
      "aborting", current.nextSequence + 1, code, timestamp, current.runId,
    );
    this.trimEvents(current.runId, event.sequence);
    return { allowed: false, record: this.require(current.runId), events: [event], code };
  }

  listNonTerminal(): StoredRunRecord[] {
    const rows = this.database.prepare("select * from runtime_runs where status not in ('succeeded','failed','timeout','aborted','orphaned') order by created_at").all() as Array<Record<string, unknown>>;
    return rows.map(rowToRun);
  }

  recoverNonTerminal(): StoredRunRecord[] {
    return this.listNonTerminal();
  }

  // A complete fact may only be recorded after the Registry already accepted
  // the same Run/JTI/dispatch/policy binding. This gives the later Adapter
  // recovery client a durable, immutable payload to compare with Backend
  // facts; it is not an admission grant and never resumes a model loop.
  recordRecoveryFact(input: RuntimeHostRecoveryFactInput): RuntimeHostRecoveryFact {
    validateRecoveryFactInput(input);
    return this.transaction(() => {
      const record = this.require(input.runId);
      const binding = record.recoveryBinding;
      if (!binding || binding.runtimeHostId !== input.runtimeHostId || binding.reservationId !== input.reservationId ||
        binding.fencingToken !== input.fencingToken || binding.capabilityHash !== input.capabilityHash ||
        record.dispatchIdentity !== input.dispatchIdentity || record.jtiHash !== input.runTicketJtiHash ||
        record.workspaceManifestHash !== input.manifestHash) {
        throw storeError("RUNTIME_RUN_CONFLICT", "recovery fact does not match accepted run identity");
      }
      const existing = this.database.prepare(`select run_id,runtime_host_id,assigned_instance_id,assigned_instance_generation,
        reservation_id,dispatch_id,fencing_token,execution_scope,capability_hash,dispatch_identity,jti_hash,manifest_hash
        from runtime_run_recovery_facts where run_id=?`).get(input.runId) as Record<string, unknown> | undefined;
      if (existing && !sameRecoveryFactInput(rowToRecoveryFactInput(existing), input)) {
        throw storeError("RUNTIME_RUN_CONFLICT", "recovery fact changed");
      }
      if (!existing) {
        this.database.prepare(`insert into runtime_run_recovery_facts(
          run_id,runtime_host_id,assigned_instance_id,assigned_instance_generation,reservation_id,dispatch_id,
          fencing_token,execution_scope,capability_hash,dispatch_identity,jti_hash,manifest_hash,created_at
        ) values(?,?,?,?,?,?,?,?,?,?,?,?,?)`).run(
          input.runId, input.runtimeHostId, input.assignedRuntimeHostInstanceId, input.assignedRuntimeHostInstanceGeneration,
          input.reservationId, input.dispatchId, input.fencingToken, input.executionScope, input.capabilityHash,
          input.dispatchIdentity, input.runTicketJtiHash, input.manifestHash, new Date().toISOString(),
        );
      }
      return { ...input, status: record.status, lastEventSequence: record.nextSequence - 1 };
    });
  }

  // The store is Host-local, so a non-terminal row that lacks an exact Host
  // binding is recovery uncertainty, not evidence of an empty Host. The
  // caller must keep the Adapter admission controller closed in that case.
  getHostRecoverySnapshot(identity: RuntimeHostRecoverySnapshotIdentity): RuntimeHostRecoverySnapshot {
    validateRecoverySnapshotIdentity(identity);
    if (!this.durable) throw storeError("RUNTIME_STORAGE_UNAVAILABLE", "durable recovery snapshot store is not configured");
    return this.transaction(() => {
      // Read one extra row so a truncated Host view is never attested as an
      // empty/partial snapshot. This bound matches Backend recovery v1.
      const active = this.database.prepare(`select * from runtime_runs
        where status not in ('succeeded','failed','timeout','aborted','orphaned') order by run_id limit ?`).all(MAX_RUNTIME_HOST_RECOVERY_FACTS + 1) as Array<Record<string, unknown>>;
      if (active.length > MAX_RUNTIME_HOST_RECOVERY_FACTS) {
        throw storeError("RUNTIME_CAPACITY_UNAVAILABLE", "recovery fact set exceeds bounded snapshot limit");
      }
      const facts: RuntimeHostRecoveryFact[] = [];
      for (const row of active) {
        const record = rowToRun(row);
        if (!record.recoveryBinding || record.recoveryBinding.runtimeHostId !== identity.runtimeHostId) {
          throw storeError("RUNTIME_CAPACITY_UNAVAILABLE", "active run has no matching host recovery binding");
        }
        const factRow = this.database.prepare(`select run_id,runtime_host_id,assigned_instance_id,assigned_instance_generation,
          reservation_id,dispatch_id,fencing_token,execution_scope,capability_hash,dispatch_identity,jti_hash,manifest_hash
          from runtime_run_recovery_facts where run_id=?`).get(record.runId) as Record<string, unknown> | undefined;
        if (!factRow) throw storeError("RUNTIME_CAPACITY_UNAVAILABLE", "active run recovery fact is incomplete");
        const fact = rowToRecoveryFactInput(factRow);
        if (fact.runtimeHostId !== identity.runtimeHostId || fact.assignedRuntimeHostInstanceId !== identity.instanceId ||
          fact.assignedRuntimeHostInstanceGeneration !== identity.instanceGeneration) {
          throw storeError("RUNTIME_CAPACITY_UNAVAILABLE", "active run is assigned to a different host instance");
        }
        if (record.nextSequence < 2 || !this.hasLatestEvent(record.runId, record.nextSequence - 1)) {
          throw storeError("RUNTIME_EVENT_GAP", "active run event boundary is uncertain");
        }
        facts.push({ ...fact, status: record.status, lastEventSequence: record.nextSequence - 1 });
      }
      const canonical = canonicalRecoveryFacts(facts);
      return {
        version: "runtime-host-recovery.v1",
        runtimeHostId: identity.runtimeHostId,
        instanceId: identity.instanceId,
        environment: identity.environment,
        instanceGeneration: identity.instanceGeneration,
        recoveryRevision: identity.recoveryRevision,
        recoveryState: identity.recoveryState,
        facts: canonical,
        factSetHash: recoveryFactSetHash(canonical),
      };
    });
  }

  // The model loop and its Gateway request context are process-local. A
  // reopened durable record cannot be safely resumed or reattached from this
  // store alone, so make the recovery decision durable and visible instead of
  // leaving a replayed submit attached to a permanently running row.
  orphanUnrecoverableAfterGatewayRestart(): StoredRunRecord[] {
    return this.transaction(() => {
      const candidates = this.database.prepare("select * from runtime_runs where status not in ('succeeded','failed','timeout','aborted','orphaned') order by created_at").all() as Array<Record<string, unknown>>;
      const recovered: StoredRunRecord[] = [];
      for (const row of candidates) {
        const current = rowToRun(row);
        const timestamp = new Date().toISOString();
        const recoveringSequence = current.nextSequence;
        const orphanedSequence = recoveringSequence + 1;
        const error = { code: "RUNTIME_RUN_ORPHANED", reason: "gateway_restart", retryable: true };
        this.insertEvent(current.runId, recoveringSequence, "run.recovery.started", "recovering", timestamp, {
          reason: "gateway_restart",
          recovery: "orphaned",
        });
        this.insertEvent(current.runId, orphanedSequence, "run.orphaned", "orphaned", timestamp, error);
        this.database.prepare(`update runtime_runs
          set status=?,next_sequence=?,terminal_sequence=?,terminal_error_json=?,abort_code=?,updated_at=?
          where run_id=? and status not in ('succeeded','failed','timeout','aborted','orphaned')`).run(
          "orphaned", orphanedSequence + 1, orphanedSequence, encodeJSON(error), error.code, timestamp, current.runId,
        );
        this.trimEvents(current.runId, orphanedSequence);
        recovered.push(this.require(current.runId));
      }
      return recovered;
    });
  }

  close(): void {
    this.database.close();
  }

  private migrate(): void {
    this.database.exec(`
      create table if not exists runtime_runs(
        run_id text primary key,
        idempotency_key text not null,
        session_binding_hash text not null,
        workspace_manifest_hash text not null,
        jti_hash text not null unique,
        dispatch_identity text not null,
        status text not null,
        runtime_host_id text,
        reservation_id text,
        fencing_token integer,
        capability_hash text,
        next_sequence integer not null,
        terminal_sequence integer not null default 0,
        terminal_result_json text,
        terminal_error_json text,
        runtime_policy_json text not null default '{}',
        runtime_policy_hash text,
        tool_calls integer not null default 0,
        repeated_calls_json text not null default '{}',
        no_progress_calls integer not null default 0,
        tool_counts_json text not null default '{}',
        read_bytes integer not null default 0,
        abort_code text,
        deadline_at text,
        last_outcome_tool_name text,
        last_outcome_args_hash text,
        last_outcome_result_hash text,
        created_at text not null,
        updated_at text not null
      );
      create table if not exists runtime_run_events(
        run_id text not null references runtime_runs(run_id) on delete cascade,
        sequence integer not null,
        event_type text not null,
        status text not null,
        timestamp text not null,
        data_json text,
        payload_hash text not null,
        primary key(run_id,sequence)
      );
      create table if not exists runtime_consumed_run_ticket_jtis(
        jti_hash text primary key,
        run_id text not null references runtime_runs(run_id) on delete cascade,
        dispatch_identity text not null,
        consumed_at text not null
      );
      create table if not exists runtime_tool_call_reservations(
        run_id text not null references runtime_runs(run_id) on delete cascade,
        call_key text not null,
        tool_name text not null,
        tool_call_hash text not null default '',
        args_hash text not null default '',
        call_number integer not null,
        outcome_recorded integer not null default 0,
        created_at text not null,
        primary key(run_id,call_key)
      );
      create table if not exists runtime_run_recovery_facts(
        run_id text primary key references runtime_runs(run_id) on delete cascade,
        runtime_host_id text not null,
        assigned_instance_id text not null,
        assigned_instance_generation integer not null,
        reservation_id text not null,
        dispatch_id text not null,
        fencing_token integer not null,
        execution_scope text not null,
        capability_hash text not null,
        dispatch_identity text not null,
        jti_hash text not null,
        manifest_hash text not null,
        created_at text not null
      );
    `);
    this.ensureColumn("runtime_runs", "runtime_host_id", "text");
    this.ensureColumn("runtime_runs", "reservation_id", "text");
    this.ensureColumn("runtime_runs", "fencing_token", "integer");
    this.ensureColumn("runtime_runs", "capability_hash", "text");
    this.ensureColumn("runtime_runs", "runtime_policy_json", "text not null default '{}'");
    this.ensureColumn("runtime_runs", "runtime_policy_hash", "text");
    this.ensureColumn("runtime_runs", "no_progress_calls", "integer not null default 0");
    this.ensureColumn("runtime_runs", "tool_counts_json", "text not null default '{}'");
    this.ensureColumn("runtime_runs", "read_bytes", "integer not null default 0");
    this.ensureColumn("runtime_runs", "abort_code", "text");
    this.ensureColumn("runtime_runs", "deadline_at", "text");
    this.ensureColumn("runtime_runs", "last_outcome_tool_name", "text");
    this.ensureColumn("runtime_runs", "last_outcome_args_hash", "text");
    this.ensureColumn("runtime_runs", "last_outcome_result_hash", "text");
    this.ensureColumn("runtime_tool_call_reservations", "tool_call_hash", "text not null default ''");
    this.ensureColumn("runtime_tool_call_reservations", "args_hash", "text not null default ''");
    // Existing pre-V1 stores need their columns before an index can reference
    // them. SQLite aborts the whole batch if the index comes first.
    this.database.exec(`
      create index if not exists idx_runtime_run_events_replay on runtime_run_events(run_id,sequence);
      create index if not exists idx_runtime_runs_non_terminal on runtime_runs(status,updated_at);
      create index if not exists idx_runtime_runs_recovery_host on runtime_runs(runtime_host_id,status,run_id);
      create index if not exists idx_runtime_run_recovery_facts_host on runtime_run_recovery_facts(runtime_host_id,assigned_instance_id,assigned_instance_generation,run_id);
    `);
  }

  private require(runId: string): StoredRunRecord {
    const record = this.get(runId);
    if (!record) throw storeError("RUNTIME_RUN_NOT_FOUND", "run not found");
    return record;
  }

  private hasLatestEvent(runId: string, sequence: number): boolean {
    const row = this.database.prepare("select sequence from runtime_run_events where run_id=? and sequence=?").get(runId, sequence) as Record<string, unknown> | undefined;
    return Boolean(row && Number(row.sequence) === sequence);
  }

  private ensureColumn(table: string, column: string, definition: string): void {
    const columns = this.database.prepare(`pragma table_info(${table})`).all() as Array<Record<string, unknown>>;
    if (!columns.some((entry) => String(entry.name) === column)) this.database.exec(`alter table ${table} add column ${column} ${definition}`);
  }

  private insertEvent(runId: string, sequence: number, eventType: string, status: StoredRunStatus, timestamp: string, data?: Record<string, unknown>): void {
    const dataJSON = data ? JSON.stringify(data) : null;
    const payloadHash = `sha256:${createHash("sha256").update(`${eventType}\0${status}\0${dataJSON ?? ""}`).digest("hex")}`;
    this.database.prepare("insert into runtime_run_events(run_id,sequence,event_type,status,timestamp,data_json,payload_hash) values(?,?,?,?,?,?,?)").run(runId, sequence, eventType, status, timestamp, dataJSON, payloadHash);
  }

  private trimEvents(runId: string, latestSequence: number): void {
    const retainAfter = latestSequence - this.maxReplayEvents;
    if (retainAfter > 0) this.database.prepare("delete from runtime_run_events where run_id=? and sequence<=?").run(runId, retainAfter);
  }

  private transaction<T>(work: () => T): T {
    this.database.exec("BEGIN IMMEDIATE");
    try {
      const result = work();
      this.database.exec("COMMIT");
      return result;
    } catch (error) {
      this.database.exec("ROLLBACK");
      throw error;
    }
  }
}

function rowToRun(row: Record<string, unknown>): StoredRunRecord {
  return {
    runId: String(row.run_id),
    idempotencyKey: String(row.idempotency_key),
    sessionBindingHash: String(row.session_binding_hash),
    workspaceManifestHash: String(row.workspace_manifest_hash),
    jtiHash: String(row.jti_hash),
    dispatchIdentity: String(row.dispatch_identity),
    recoveryBinding: row.runtime_host_id && row.reservation_id && row.fencing_token !== null && row.fencing_token !== undefined && row.capability_hash
      ? {
          runtimeHostId: String(row.runtime_host_id),
          reservationId: String(row.reservation_id),
          fencingToken: Number(row.fencing_token),
          capabilityHash: String(row.capability_hash),
        }
      : undefined,
    status: String(row.status) as StoredRunStatus,
    nextSequence: Number(row.next_sequence),
    terminalSequence: Number(row.terminal_sequence ?? 0),
    terminalResult: decodeJSON(row.terminal_result_json),
    terminalError: decodeJSON(row.terminal_error_json),
    runtimePolicy: decodeRuntimePolicy(row.runtime_policy_json),
    toolCalls: Number(row.tool_calls ?? 0),
    repeatedCalls: (decodeJSON(row.repeated_calls_json) as Record<string, number> | undefined) ?? {},
    noProgressCalls: Number(row.no_progress_calls ?? 0),
    toolCounts: (decodeJSON(row.tool_counts_json) as Record<string, number> | undefined) ?? {},
    readBytes: Number(row.read_bytes ?? 0),
    abortCode: row.abort_code ? String(row.abort_code) : undefined,
    deadlineAt: String(row.deadline_at ?? ""),
    lastOutcome: row.last_outcome_tool_name && row.last_outcome_args_hash && row.last_outcome_result_hash !== null && row.last_outcome_result_hash !== undefined
      ? {
          toolName: String(row.last_outcome_tool_name),
          argsHash: String(row.last_outcome_args_hash),
          resultHash: String(row.last_outcome_result_hash),
        }
      : undefined,
    createdAt: String(row.created_at),
    updatedAt: String(row.updated_at),
  };
}

function rowToEvent(row: Record<string, unknown>): StoredRunEvent {
  const data = decodeJSON(row.data_json) as Record<string, unknown> | undefined;
  return {
    sequence: Number(row.sequence),
    eventType: String(row.event_type),
    status: String(row.status) as StoredRunStatus,
    timestamp: String(row.timestamp),
    ...(data ? { data } : {}),
  };
}

function validateIdentity(identity: StoredRunIdentity): void {
  for (const value of Object.values(identity)) if (!value || value.length > 512) throw storeError("INVALID_ARGUMENT", "invalid run identity");
}

function validateHash(value: string, label: string): void {
  if (!/^sha256:[a-f0-9]{64}$/i.test(value)) throw storeError("INVALID_ARGUMENT", `invalid ${label}`);
}

function validateRecoveryBinding(binding: StoredRunRecoveryBinding, policy: VerifiedRuntimePolicy): void {
  if (!safeRecoveryString(binding.runtimeHostId) || !safeRecoveryString(binding.reservationId) ||
    !Number.isSafeInteger(binding.fencingToken) || binding.fencingToken < 1 ||
    !safeRecoveryString(binding.capabilityHash) || binding.capabilityHash !== policy.capabilityHash) {
    throw storeError("INVALID_ARGUMENT", "invalid recovery binding");
  }
}

function sameRecoveryBinding(left: StoredRunRecoveryBinding | undefined, right: StoredRunRecoveryBinding | undefined): boolean {
  return left?.runtimeHostId === right?.runtimeHostId && left?.reservationId === right?.reservationId &&
    left?.fencingToken === right?.fencingToken && left?.capabilityHash === right?.capabilityHash;
}

function validateRecoveryFactInput(fact: RuntimeHostRecoveryFactInput): void {
  const strings = [
    fact.runId, fact.runtimeHostId, fact.assignedRuntimeHostInstanceId, fact.reservationId,
    fact.dispatchId, fact.capabilityHash,
  ];
  if (strings.some((value) => !safeRecoveryString(value)) || !Number.isSafeInteger(fact.assignedRuntimeHostInstanceGeneration) ||
    fact.assignedRuntimeHostInstanceGeneration < 1 || !Number.isSafeInteger(fact.fencingToken) || fact.fencingToken < 1 ||
    (fact.executionScope !== "product_thread" && fact.executionScope !== "detached_task")) {
    throw storeError("INVALID_ARGUMENT", "invalid recovery fact");
  }
  validateHash(fact.dispatchIdentity, "dispatchIdentity");
  validateHash(fact.runTicketJtiHash, "runTicketJtiHash");
  validateHash(fact.manifestHash, "manifestHash");
}

function validateRecoverySnapshotIdentity(identity: RuntimeHostRecoverySnapshotIdentity): void {
  const strings = [identity.runtimeHostId, identity.instanceId, identity.environment];
  if (strings.some((value) => !safeRecoveryString(value)) || !Number.isSafeInteger(identity.instanceGeneration) ||
    identity.instanceGeneration < 1 || !Number.isSafeInteger(identity.recoveryRevision) || identity.recoveryRevision < 1 ||
    identity.recoveryState !== "pending") {
    throw storeError("INVALID_ARGUMENT", "invalid host recovery snapshot identity");
  }
}

function safeRecoveryString(value: string): boolean {
  // The Backend accepts a wider control-plane string surface, but the current
  // cross-language hash proof must not silently diverge on locale ordering or
  // JSON HTML/unicode escaping. Restrict new Gateway facts to the canonical
  // identifier alphabet until both sides share one serializer.
  return typeof value === "string" && /^[A-Za-z0-9][A-Za-z0-9._:-]{0,1023}$/.test(value);
}

function rowToRecoveryFactInput(row: Record<string, unknown>): RuntimeHostRecoveryFactInput {
  const fact: RuntimeHostRecoveryFactInput = {
    runId: String(row.run_id ?? ""),
    runtimeHostId: String(row.runtime_host_id ?? ""),
    assignedRuntimeHostInstanceId: String(row.assigned_instance_id ?? ""),
    assignedRuntimeHostInstanceGeneration: Number(row.assigned_instance_generation ?? 0),
    reservationId: String(row.reservation_id ?? ""),
    dispatchId: String(row.dispatch_id ?? ""),
    fencingToken: Number(row.fencing_token ?? 0),
    executionScope: String(row.execution_scope ?? "") as RuntimeHostRecoveryFactInput["executionScope"],
    capabilityHash: String(row.capability_hash ?? ""),
    dispatchIdentity: String(row.dispatch_identity ?? ""),
    runTicketJtiHash: String(row.jti_hash ?? ""),
    manifestHash: String(row.manifest_hash ?? ""),
  };
  try {
    validateRecoveryFactInput(fact);
    return fact;
  } catch {
    throw storeError("RUNTIME_STORAGE_UNAVAILABLE", "corrupt recovery fact state");
  }
}

function sameRecoveryFactInput(left: RuntimeHostRecoveryFactInput, right: RuntimeHostRecoveryFactInput): boolean {
  return left.runId === right.runId && left.runtimeHostId === right.runtimeHostId &&
    left.assignedRuntimeHostInstanceId === right.assignedRuntimeHostInstanceId &&
    left.assignedRuntimeHostInstanceGeneration === right.assignedRuntimeHostInstanceGeneration &&
    left.reservationId === right.reservationId && left.dispatchId === right.dispatchId &&
    left.fencingToken === right.fencingToken && left.executionScope === right.executionScope &&
    left.capabilityHash === right.capabilityHash && left.dispatchIdentity === right.dispatchIdentity &&
    left.runTicketJtiHash === right.runTicketJtiHash && left.manifestHash === right.manifestHash;
}

function canonicalRecoveryFacts(facts: RuntimeHostRecoveryFact[]): RuntimeHostRecoveryFact[] {
  const canonical = [...facts].sort((left, right) =>
    compareRecoveryIdentifier(left.runId, right.runId) || compareRecoveryIdentifier(left.dispatchId, right.dispatchId) ||
    compareRecoveryIdentifier(left.reservationId, right.reservationId),
  );
  for (let index = 0; index < canonical.length; index += 1) {
    const fact = canonical[index]!;
    validateRecoveryFactInput(fact);
    if (!safeRecoveryString(fact.status) || !Number.isSafeInteger(fact.lastEventSequence) || fact.lastEventSequence < 0) {
      throw storeError("RUNTIME_CAPACITY_UNAVAILABLE", "invalid recovery fact state");
    }
    const previous = canonical[index - 1];
    if (previous && previous.runId === fact.runId && previous.dispatchId === fact.dispatchId && previous.reservationId === fact.reservationId) {
      throw storeError("RUNTIME_CAPACITY_UNAVAILABLE", "duplicate recovery fact");
    }
  }
  return canonical;
}

function compareRecoveryIdentifier(left: string, right: string): number {
  return left < right ? -1 : left > right ? 1 : 0;
}

// This property ordering matches Backend RuntimeHostRecoveryFact JSON exactly.
// The hash can therefore be compared byte-for-byte once the Adapter obtains a
// certificate-bound Backend snapshot; the current Gateway RPC does not expose
// that authorization context and intentionally remains closed.
function recoveryFactSetHash(facts: RuntimeHostRecoveryFact[]): string {
  const payload = JSON.stringify({
    version: "runtime-host-recovery.v1",
    facts: facts.map((fact) => ({
      runId: fact.runId,
      runtimeHostId: fact.runtimeHostId,
      assignedRuntimeHostInstanceId: fact.assignedRuntimeHostInstanceId,
      assignedRuntimeHostInstanceGeneration: fact.assignedRuntimeHostInstanceGeneration,
      reservationId: fact.reservationId,
      dispatchId: fact.dispatchId,
      fencingToken: fact.fencingToken,
      executionScope: fact.executionScope,
      capabilityHash: fact.capabilityHash,
      dispatchIdentity: fact.dispatchIdentity,
      runTicketJtiHash: fact.runTicketJtiHash,
      manifestHash: fact.manifestHash,
      status: fact.status,
      lastEventSequence: fact.lastEventSequence,
    })),
  });
  return `sha256:${createHash("sha256").update(payload).digest("hex")}`;
}

function validateEvent(runId: string, eventType: string, status: StoredRunStatus): void {
  if (!runId || !eventType || !status || eventType.length > 128) throw storeError("INVALID_ARGUMENT", "invalid run event");
}

function sameIdentity(record: StoredRunRecord, identity: StoredRunIdentity): boolean {
  return record.runId === identity.runId && record.idempotencyKey === identity.idempotencyKey && record.sessionBindingHash === identity.sessionBindingHash && record.workspaceManifestHash === identity.workspaceManifestHash;
}

function validateRuntimePolicy(identity: StoredRunIdentity, dispatchIdentity: string, policy: VerifiedRuntimePolicy): void {
  try {
    validateVerifiedRuntimePolicy(policy);
  } catch (error) {
    throw storeError(String((error as { code?: unknown }).code ?? "RUNTIME_POLICY_INVALID"), "invalid runtime policy");
  }
  if (policy.runId !== identity.runId || policy.idempotencyKey !== identity.idempotencyKey ||
    policy.workspaceManifestHash !== identity.workspaceManifestHash || policy.dispatchIdentity !== dispatchIdentity) {
    throw storeError("RUNTIME_RUN_CONFLICT", "runtime policy binding changed");
  }
}

function decodeRuntimePolicy(value: unknown): VerifiedRuntimePolicy | undefined {
  const decoded = decodeJSON(value);
  if (!decoded || typeof decoded !== "object" || Array.isArray(decoded) || Object.keys(decoded as object).length === 0) return undefined;
  try {
    validateVerifiedRuntimePolicy(decoded as VerifiedRuntimePolicy);
    return decoded as VerifiedRuntimePolicy;
  } catch {
    throw storeError("RUNTIME_STORAGE_UNAVAILABLE", "corrupt runtime policy state");
  }
}

function toolCategory(toolName: string): "search" | "write" | undefined {
  if (toolName === "workspace_search") return "search";
  if (toolName === "write") return "write";
  return undefined;
}

function categoryBudgetReached(counts: Record<string, number>, category: "search" | "write", budget: VerifiedRuntimePolicy["toolBudget"]): boolean {
  const current = counts[category] ?? 0;
  if (category === "search") return current >= budget.maxSearchCalls;
  return current >= budget.maxWriteCalls;
}

function toolCallKey(toolCallId: string): string {
  return runtimeToolAuditHash("call", toolCallId);
}

function isRuntimeAuditToolName(toolName: string): boolean {
  return toolName === "read" || toolName === "write" || toolName === "workspace_search";
}

function isRuntimeToolAuditHash(value: string): boolean {
  return /^sha256:[a-f0-9]{64}$/i.test(value);
}

function runtimeToolAuditHash(kind: string, value: string): string {
  return `sha256:${createHash("sha256").update(`huahuo.runtime.tool.${kind}.v1\\0`, "utf8").update(value, "utf8").digest("hex")}`;
}

function rejectedToolAuditHash(runId: string, value: string, reason: string, sequence: number): string {
  return runtimeToolAuditHash("reject", `${runId}\\0${value}\\0${reason}\\0${sequence}`);
}

function runtimeToolAuditDurationMs(createdAt: string): number {
  const startedAt = Date.parse(createdAt);
  if (!Number.isFinite(startedAt)) throw storeError("RUNTIME_STORAGE_UNAVAILABLE", "tool reservation timestamp is invalid");
  return Math.max(0, Math.min(MAX_RUNTIME_TOOL_AUDIT_DURATION_MS, Date.now() - startedAt));
}

function runtimeToolAuditPayload(input: {
  toolName: string;
  toolCallHash: string;
  argsHash: string;
  outcome: "started" | "succeeded" | "failed" | "aborted" | "rejected";
  durationMs: number;
  bytes: number;
  call: number;
  repeat: number;
  resultFingerprint?: string;
  errorCode?: string;
}): Record<string, unknown> {
  if (!isRuntimeAuditToolName(input.toolName) || !isRuntimeToolAuditHash(input.toolCallHash) || !isRuntimeToolAuditHash(input.argsHash) ||
    !Number.isSafeInteger(input.durationMs) || input.durationMs < 0 || input.durationMs > MAX_RUNTIME_TOOL_AUDIT_DURATION_MS ||
    !Number.isSafeInteger(input.bytes) || input.bytes < 0 || !Number.isSafeInteger(input.call) || input.call < 1 || input.call > 400 ||
    !Number.isSafeInteger(input.repeat) || input.repeat < 1 || input.repeat > input.call ||
    (input.resultFingerprint !== undefined && !isRuntimeToolAuditHash(input.resultFingerprint)) ||
    (input.errorCode !== undefined && !isRuntimeToolAuditErrorCode(input.errorCode))) {
    throw storeError("RUNTIME_POLICY_INVALID", "invalid tool audit receipt");
  }
  if (input.outcome === "started" && (input.durationMs !== 0 || input.bytes !== 0 || input.resultFingerprint !== undefined || input.errorCode !== undefined)) {
    throw storeError("RUNTIME_POLICY_INVALID", "invalid started tool receipt");
  }
  if ((input.outcome === "succeeded" || input.outcome === "failed" || input.outcome === "aborted") && !input.resultFingerprint) {
    throw storeError("RUNTIME_POLICY_INVALID", "tool result fingerprint is required");
  }
  if ((input.outcome === "failed" || input.outcome === "aborted") && input.errorCode === undefined) {
    throw storeError("RUNTIME_POLICY_INVALID", "tool terminal receipt error is invalid");
  }
  if (input.outcome === "succeeded" && input.errorCode !== undefined) throw storeError("RUNTIME_POLICY_INVALID", "successful tool receipt has an error");
  if (input.outcome === "rejected" && (input.durationMs !== 0 || input.bytes !== 0 || input.resultFingerprint !== undefined || input.errorCode === undefined)) {
    throw storeError("RUNTIME_POLICY_INVALID", "invalid rejected tool receipt");
  }
  return {
    schemaVersion: RUNTIME_TOOL_AUDIT_SCHEMA_VERSION,
    toolName: input.toolName,
    toolCallHash: input.toolCallHash,
    argsHash: input.argsHash,
    outcome: input.outcome,
    durationMs: input.durationMs,
    bytes: input.bytes,
    call: input.call,
    repeat: input.repeat,
    ...(input.resultFingerprint ? { resultFingerprint: input.resultFingerprint } : {}),
    ...(input.errorCode ? { errorCode: input.errorCode } : {}),
  };
}

function isRuntimeToolAuditErrorCode(value: string): boolean {
  return value === "RUNTIME_ABORTED" || value === "RUNTIME_ABORT_FAILED" || value === "RUNTIME_FAILED" ||
    value === "RUNTIME_INPUT_INVALID" || value === "RUNTIME_PERMISSION_DENIED" || value === "RUNTIME_RUN_STALLED" ||
    value === "RUNTIME_TIMEOUT" || value === "RUNTIME_TOOL_BUDGET_EXCEEDED" || value === "RUNTIME_TOOL_LOOP_DETECTED" ||
    value === "RUNTIME_TOOL_UNAVAILABLE";
}

function isDeadlineElapsed(deadlineAt: string): boolean {
  const deadline = Date.parse(deadlineAt);
  return !Number.isFinite(deadline) || deadline <= Date.now();
}

function encodeJSON(value: unknown): string | null {
  return value === undefined ? null : JSON.stringify(value);
}

function decodeJSON(value: unknown): unknown {
  if (value === null || value === undefined || value === "") return undefined;
  try { return JSON.parse(String(value)); } catch { throw storeError("RUNTIME_STORAGE_UNAVAILABLE", "corrupt run store json"); }
}

function storeError(code: string, message: string): Error & { code: string } {
  return Object.assign(new Error(message), { code });
}

export const enterpriseRunStore = EnterpriseRunStore.fromEnvironment();
