import { createHash } from "node:crypto";
import {
  EnterpriseRunStore,
  enterpriseRunStore,
  type StoredEventPage,
  type RuntimeHostRecoveryFact,
  type RuntimeHostRecoveryFactInput,
  type RuntimeHostRecoverySnapshot,
  type RuntimeHostRecoverySnapshotIdentity,
  type StoredRunEvent,
  type StoredRunIdentity,
  type StoredRunRecoveryBinding,
  type StoredRunStatus,
} from "./enterprise-run-store.js";
import {
  assertRuntimeWorkspaceMount,
  assertRuntimeWorkspaceNativeWritePath,
  type RuntimeWorkspaceMount,
  type VerifiedRuntimePolicy,
} from "./runtime-policy.js";

export type EnterpriseRunStatus = StoredRunStatus;
export type EnterpriseRunEvent = StoredRunEvent;
export type EnterpriseRunIdentity = StoredRunIdentity;

export type EnterpriseRunAcceptance = {
  jtiHash: string;
  dispatchIdentity: string;
};

export type EnterpriseRunRecoveryBinding = StoredRunRecoveryBinding;
export type EnterpriseRuntimeHostRecoveryFact = RuntimeHostRecoveryFactInput;
export type EnterpriseRuntimeHostRecoverySnapshot = RuntimeHostRecoverySnapshot;
export type EnterpriseRuntimeHostRecoverySnapshotIdentity = RuntimeHostRecoverySnapshotIdentity;

export type EnterpriseRunWorkspaceGuard = {
  workspaceId: string;
  mount: RuntimeWorkspaceMount;
};

export type EnterpriseProductSessionRef = Readonly<{
  threadId: string;
  openclawSessionKey: string;
}>;

export function submitReplayResponse(registration: { created: boolean; status: EnterpriseRunStatus }): {
  status: "accepted" | "orphaned";
  recovery?: { state: "orphaned"; retry: "new_attempt"; code: "RUNTIME_RUN_ORPHANED" };
} {
  if (!registration.created && registration.status === "orphaned") {
    return { status: "orphaned", recovery: { state: "orphaned", retry: "new_attempt", code: "RUNTIME_RUN_ORPHANED" } };
  }
  return { status: "accepted" };
}

type EnterpriseRunControl = {
  controller: AbortController;
  terminalPromise: Promise<void>;
  resolveTerminal: () => void;
  workspaceGuard?: EnterpriseRunWorkspaceGuard;
};

const TERMINAL = new Set<EnterpriseRunStatus>(["succeeded", "failed", "timeout", "aborted", "orphaned"]);

export class EnterpriseRunRegistry {
  private readonly controls = new Map<string, EnterpriseRunControl>();
  private readonly sessionTails = new Map<string, Promise<void>>();
  private readonly store: EnterpriseRunStore;

  constructor(store = new EnterpriseRunStore(":memory:")) {
    this.store = store;
    for (const record of this.store.orphanUnrecoverableAfterGatewayRestart()) this.ensureControl(record.runId);
  }

  register(
    identity: EnterpriseRunIdentity,
    acceptance: EnterpriseRunAcceptance,
    runtimePolicy: VerifiedRuntimePolicy,
    workspaceGuard: EnterpriseRunWorkspaceGuard,
    recoveryBinding?: EnterpriseRunRecoveryBinding,
  ): { created: boolean; status: EnterpriseRunStatus } {
    validateIdentity(identity);
    if (!acceptance?.jtiHash || !acceptance?.dispatchIdentity || !runtimePolicy || !workspaceGuard?.workspaceId) {
      throw enterpriseError("RUNTIME_PERMISSION_DENIED", "signed runtime policy, dispatch proof and workspace guard are required");
    }
    const verifiedMount = assertRuntimeWorkspaceMount(workspaceGuard.mount, runtimePolicy, workspaceGuard.workspaceId);
    const verifiedGuard = cloneWorkspaceGuard({ workspaceId: workspaceGuard.workspaceId, mount: verifiedMount });
    const accepted = this.store.accept(identity, acceptance.jtiHash, acceptance.dispatchIdentity, runtimePolicy, recoveryBinding);
    const control = this.ensureControl(identity.runId);
    if (accepted.created) {
      control.workspaceGuard = verifiedGuard;
    } else if (control.workspaceGuard && !sameWorkspaceGuard(control.workspaceGuard, verifiedGuard)) {
      throw enterpriseError("RUNTIME_RUN_CONFLICT", "runtime workspace mount changed for an accepted run");
    } else if (!control.workspaceGuard && !TERMINAL.has(accepted.record.status)) {
      throw enterpriseError("RUNTIME_STORAGE_UNAVAILABLE", "live runtime workspace guard is unavailable");
    }
    return { created: accepted.created, status: accepted.record.status };
  }

  isDurable(): boolean {
    return this.store.durable;
  }

  assertDurableReady(): void {
    if (!this.store.durable) throw enterpriseError("RUNTIME_STORAGE_UNAVAILABLE", "durable enterprise run store is not configured");
  }

  getAbortSignal(runId: string): AbortSignal {
    this.require(runId);
    return this.ensureControl(runId).controller.signal;
  }

  appendEvent(runId: string, eventType: string, status?: EnterpriseRunStatus, data?: Record<string, unknown>): EnterpriseRunEvent {
    const record = this.require(runId);
    return this.store.appendEvent(runId, {
      eventType,
      status: status ?? record.status,
      ...(data ? { data: sanitizeEventData(data) } : {}),
    });
  }

  assertToolCallAllowed(runId: string, input: { toolName: string; toolCallId: string; args: unknown }): void {
    const toolName = bounded(input.toolName, 128);
    const toolCallId = bounded(input.toolCallId, 512);
    const toolCallHash = runtimeToolCallHash(toolCallId);
    const argsHash = runtimeToolArgsHash(input.args);
    const record = this.require(runId);
    if (toolName === "write" && record.runtimePolicy?.allowedTools.includes("write")) {
      const control = this.ensureControl(runId);
      try {
        if (!control.workspaceGuard) {
          throw enterpriseError("RUNTIME_STORAGE_UNAVAILABLE", "live runtime workspace guard is unavailable");
        }
        assertRuntimeWorkspaceNativeWritePath(
          toolPath(input.args),
          control.workspaceGuard.mount,
          record.runtimePolicy,
          control.workspaceGuard.workspaceId,
        );
      } catch (error) {
        const code = runtimeErrorCode(error, "RUNTIME_PERMISSION_DENIED");
        const rejected = this.store.rejectToolCallForPolicy(runId, toolName, code, "workspace_write_guard");
        control.controller.abort(enterpriseError(rejected.code, rejected.code));
        throw enterpriseError(rejected.code, rejected.code);
      }
    }
    const decision = this.store.assertToolCallAllowed(runId, {
      toolName,
      toolCallId,
      toolCallHash,
      argsHash,
    });
    if (decision.allowed) return;
    const code = decision.code ?? "RUNTIME_ABORTED";
    this.ensureControl(runId).controller.abort(enterpriseError(code, code));
    throw enterpriseError(code, code);
  }

  recordToolOutcome(runId: string, input: {
    toolName: string;
    toolCallId: string;
    argsHash: string;
    resultHash?: string;
    progress?: boolean;
    resultBytes?: number;
  }): void {
    const result = this.store.recordToolOutcome(runId, {
      toolName: bounded(input.toolName, 128),
      toolCallId: bounded(input.toolCallId, 512),
      argsHash: runtimeToolHashReference(input.argsHash),
      resultFingerprint: runtimeToolHashReference(input.resultHash ?? ""),
      ...(input.progress !== undefined ? { progress: input.progress } : {}),
      ...(input.resultBytes !== undefined ? { resultBytes: input.resultBytes } : {}),
    });
    if (result.abortCode) this.ensureControl(runId).controller.abort(enterpriseError(result.abortCode, result.abortCode));
  }

  armWallTime(runId: string): () => void {
    const record = this.require(runId);
    const deadline = Date.parse(record.deadlineAt);
    const delay = Number.isFinite(deadline) ? Math.max(0, deadline - Date.now()) : 0;
    const timer = setTimeout(() => {
      const result = this.store.abortForPolicy(runId, "RUNTIME_TIMEOUT", "max_wall_time");
      if (result.changed) this.ensureControl(runId).controller.abort(enterpriseError("RUNTIME_TIMEOUT", "RUNTIME_TIMEOUT"));
    }, delay);
    timer.unref?.();
    return () => clearTimeout(timer);
  }

  complete(runId: string, status: Extract<EnterpriseRunStatus, "succeeded" | "failed" | "timeout" | "aborted">, result?: unknown, error?: unknown): void {
    const completed = this.store.complete(runId, { status, result: sanitizeTerminal(result), error: sanitizeTerminal(error) });
    if (completed.changed || TERMINAL.has(completed.record.status)) this.ensureControl(runId).resolveTerminal();
  }

  async abort(runId: string, reason: string, timeoutMs = 30_000): Promise<{ runId: string; status: EnterpriseRunStatus }> {
    const record = this.require(runId);
    if (TERMINAL.has(record.status)) return { runId, status: record.status };
    this.store.requestExternalAbort(runId);
    const control = this.ensureControl(runId);
    control.controller.abort(enterpriseError("RUNTIME_ABORTED", bounded(reason, 256)));
    await Promise.race([
      control.terminalPromise,
      new Promise((_, reject) => setTimeout(() => reject(enterpriseError("RUNTIME_ABORT_FAILED", "terminal abort acknowledgement timed out")), timeoutMs)),
    ]);
    const terminal = this.require(runId);
    if (!TERMINAL.has(terminal.status)) throw enterpriseError("RUNTIME_ABORT_FAILED", "run did not reach terminal state");
    return { runId, status: terminal.status };
  }

  status(runId: string): Record<string, unknown> {
    const record = this.require(runId);
    return {
      runId,
      status: record.status,
      lastEventSequence: record.nextSequence - 1,
      terminalSequence: record.terminalSequence,
      toolCalls: record.toolCalls,
      noProgressCalls: record.noProgressCalls,
      toolCounts: record.toolCounts,
      readBytes: record.readBytes,
      runtimePolicy: record.runtimePolicy ? {
        policyHash: record.runtimePolicy.policyHash,
        version: record.runtimePolicy.version,
        requiredTools: record.runtimePolicy.requiredTools,
        allowedTools: record.runtimePolicy.allowedTools,
        toolBudget: record.runtimePolicy.toolBudget,
      } : undefined,
      deadlineAt: record.deadlineAt,
      ...(record.abortCode ? { abortCode: record.abortCode } : {}),
      createdAt: record.createdAt,
      updatedAt: record.updatedAt,
      ...(record.status === "orphaned" ? { recovery: { state: "orphaned", retry: "new_attempt" } } : {}),
      ...(TERMINAL.has(record.status) ? { result: record.terminalResult, error: record.terminalError } : {}),
    };
  }

  events(runId: string, afterSequence = 0, limit = 100): StoredEventPage {
    return this.store.listEvents(runId, afterSequence, limit);
  }

  listNonTerminal(): Array<Record<string, unknown>> {
    return this.store.listNonTerminal().map((record) => ({
      runId: record.runId,
      status: record.status,
      dispatchIdentity: record.dispatchIdentity,
      updatedAt: record.updatedAt,
    }));
  }

  // This is intentionally a local Registry API, not an unauthenticated
  // Gateway RPC. The future Adapter recovery bridge must first prove the
  // certificate-bound Host principal, then invoke this immutable-fact write.
  recordAuthorizedRecoveryFact(fact: EnterpriseRuntimeHostRecoveryFact): RuntimeHostRecoveryFact {
    return this.store.recordRecoveryFact(fact);
  }

  // Returns only hash-safe occupancy facts from durable storage. It neither
  // reconstructs a model loop nor makes the Host ready; the Adapter must
  // compare this with Backend and complete Backend attestation first.
  snapshotForAuthorizedHost(identity: EnterpriseRuntimeHostRecoverySnapshotIdentity): EnterpriseRuntimeHostRecoverySnapshot {
    return this.store.getHostRecoverySnapshot(identity);
  }

  async serializeSession<T>(sessionBindingHash: string, work: () => Promise<T>): Promise<T> {
    const previous = this.sessionTails.get(sessionBindingHash) ?? Promise.resolve();
    let release: () => void = () => {};
    const current = new Promise<void>((resolve) => { release = resolve; });
    const tail = previous.then(() => current);
    this.sessionTails.set(sessionBindingHash, tail);
    await previous;
    try { return await work(); }
    finally {
      release();
      if (this.sessionTails.get(sessionBindingHash) === tail) this.sessionTails.delete(sessionBindingHash);
    }
  }

  private require(runId: string) {
    const record = this.store.get(runId);
    if (!record) throw enterpriseError("RUNTIME_RUN_NOT_FOUND", "run not found");
    return record;
  }

  private ensureControl(runId: string): EnterpriseRunControl {
    const existing = this.controls.get(runId);
    if (existing) return existing;
    let resolveTerminal: () => void = () => {};
    const terminalPromise = new Promise<void>((resolve) => { resolveTerminal = resolve; });
    const control = { controller: new AbortController(), terminalPromise, resolveTerminal };
    this.controls.set(runId, control);
    const record = this.store.get(runId);
    if (record && TERMINAL.has(record.status)) {
      if (record.status === "orphaned") {
        control.controller.abort(enterpriseError(record.abortCode ?? "RUNTIME_RUN_ORPHANED", "gateway restart requires recovery"));
      }
      resolveTerminal();
    }
    return control;
  }
}

function validateIdentity(identity: EnterpriseRunIdentity): void {
  for (const value of Object.values(identity)) if (!value || value.length > 512) throw enterpriseError("INVALID_ARGUMENT", "invalid run identity");
}

function sanitizeEventData(data: Record<string, unknown>): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(data)) {
    if (/token|secret|sessionkey|realpath|workspace(dir|root)|content|prompt|message/i.test(key)) continue;
    out[key] = typeof value === "string" ? bounded(value, 512) : value;
  }
  return out;
}

function sanitizeTerminal(value: unknown): unknown {
  if (!value || typeof value !== "object") return value;
  const raw = JSON.stringify(value, (key, child) => /token|secret|sessionkey|realpath|workspace(dir|root)/i.test(key) ? undefined : child);
  return JSON.parse(raw);
}

function bounded(value: string, max: number): string {
  return String(value ?? "").slice(0, max);
}

function toolPath(args: unknown): unknown {
  return args && typeof args === "object" && !Array.isArray(args) && Object.getPrototypeOf(args) === Object.prototype
    ? (args as Record<string, unknown>).path
    : undefined;
}

function runtimeErrorCode(error: unknown, fallback: string): string {
  const code = error && typeof error === "object" ? String((error as { code?: unknown }).code ?? "") : "";
  return /^[A-Z][A-Z0-9_]{1,127}$/.test(code) ? code : fallback;
}

function cloneWorkspaceGuard(guard: EnterpriseRunWorkspaceGuard): EnterpriseRunWorkspaceGuard {
  if (guard.mount.accessMode === "read") {
    return { workspaceId: guard.workspaceId, mount: { realPath: guard.mount.realPath, accessMode: "read" } };
  }
  return {
    workspaceId: guard.workspaceId,
    mount: {
      realPath: guard.mount.realPath,
      accessMode: "write",
      writeLease: { ...guard.mount.writeLease, allowedRoots: [...guard.mount.writeLease.allowedRoots] },
    },
  };
}

function sameWorkspaceGuard(left: EnterpriseRunWorkspaceGuard, right: EnterpriseRunWorkspaceGuard): boolean {
  return JSON.stringify(left) === JSON.stringify(right);
}

export function stableIdentityHash(value: unknown): string {
  return `sha256:${createHash("sha256").update(JSON.stringify(value)).digest("hex")}`;
}

// Tool receipts cross the Runtime/Backend boundary. These domain-separated
// hashes are the only representation of a native tool-call ID, arguments or
// result that may leave the Runtime process. JSON stringify failures are an
// input failure before a tool reservation can be created.
export function runtimeToolCallHash(toolCallId: string): string {
  if (!toolCallId || toolCallId.length > 512) throw enterpriseError("RUNTIME_INPUT_INVALID", "tool call id is invalid");
  return runtimeToolHash("call", toolCallId);
}

export function runtimeToolArgsHash(args: unknown): string {
  let serialized: string | undefined;
  try { serialized = JSON.stringify(args); } catch { throw enterpriseError("RUNTIME_INPUT_INVALID", "tool arguments are not serializable"); }
  if (serialized === undefined) throw enterpriseError("RUNTIME_INPUT_INVALID", "tool arguments are not serializable");
  return runtimeToolHash("args", serialized);
}

export function runtimeToolHashReference(value: string): string {
  const normalized = value.trim();
  if (/^sha256:[a-f0-9]{64}$/i.test(normalized)) return normalized.toLowerCase();
  return runtimeToolHash("value", value);
}

function runtimeToolHash(kind: string, value: string): string {
  return `sha256:${createHash("sha256").update(`huahuo.runtime.tool.${kind}.v1\\0`, "utf8").update(value, "utf8").digest("hex")}`;
}

// Product-session execution is serialized by the stable Backend-owned
// identity only. Metadata is intentionally excluded: it can change with a
// task, output contract or future audit field while the native OpenClaw
// session remains the same JSONL transcript.
export function productSessionExecutionRef(value: unknown): EnterpriseProductSessionRef {
  const session = value && typeof value === "object" && !Array.isArray(value)
    ? value as Record<string, unknown>
    : {};
  const threadId = typeof session.threadId === "string" ? session.threadId.trim() : "";
  const openclawSessionKey = typeof session.openclawSessionKey === "string" ? session.openclawSessionKey.trim() : "";
  if (!threadId || !openclawSessionKey) {
    throw enterpriseError("RUNTIME_INPUT_INVALID", "product session identity is invalid");
  }
  return Object.freeze({ threadId, openclawSessionKey });
}

export function productSessionSerializationKey(value: unknown): string {
  return stableIdentityHash(productSessionExecutionRef(value));
}

export function enterpriseError(code: string, message: string): Error & { code: string } {
  return Object.assign(new Error(message), { code });
}

export const enterpriseRunRegistry = new EnterpriseRunRegistry(enterpriseRunStore);
