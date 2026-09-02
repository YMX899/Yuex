import { createHash, createHmac, timingSafeEqual } from "node:crypto";
import { lstatSync, realpathSync } from "node:fs";
import { isAbsolute, relative, resolve, sep } from "node:path";

export const RUNTIME_POLICY_VERSION = "huahuo.runtime-policy.v1";
export const RUNTIME_POLICY_ALGORITHM = "HS256";
export const RUNTIME_POLICY_KEY_DERIVATION_CONTEXT = "huahuo/runtime-policy/v1";
export const RUNTIME_WORKSPACE_ACCESS_READ = "read";
export const RUNTIME_WORKSPACE_ACCESS_WRITE = "write";
export const RUNTIME_WORKSPACE_WRITE_LEASE_VERSION = "huahuo.runtime-write-lease.v1";
export const RUNTIME_STAGING_WRITE_ROOTS = ["output", "staging"] as const;
export const DEFAULT_RUNTIME_TOOL_BUDGET: RuntimeToolBudget = Object.freeze({
  maxToolCalls: 200,
  softToolCallLimit: 160,
  finalizationReserve: 10,
  maxRepeatedCalls: 2,
  maxNoProgressCalls: 4,
  maxSearchCalls: 60,
  maxWriteCalls: 20,
  maxReadBytes: 64 * 1024 * 1024,
  maxWallTimeSeconds: 1800,
});
export const MAX_TOOL_CALLS_SUPPORTED = 400;
export const SUPPORTED_RUNTIME_POLICY_TOOLS = [
  "read",
  "workspace_search",
  "write",
] as const;
export const RUNTIME_TOOL_BUDGET_ENFORCEMENT_VERSION = "huahuo.runtime-tool-budget-enforcement.v1";
export const RUNTIME_TOOL_EXECUTION_EVENT_SCHEMA = "huahuo.runtime-tool-execution-event.v1";
export const RUNTIME_ABORT_CONVERGENCE_EVENT_SCHEMA = "huahuo.runtime-abort-convergence-event.v1";

export type RuntimeToolBudgetExecutionContract = {
  enforcementVersion: string;
  toolExecutionEventSchema: string;
  abortConvergenceSchema: string;
  hardMaxToolCalls: number;
  softToolCallLimit: number;
  finalizationReserve: number;
  maxRepeatedCalls: number;
  maxNoProgressCalls: number;
};

// This is emitted verbatim by enterprise.runtime.capabilities. Backend uses
// it to reject a Gateway that only claims budget/abort support but cannot
// prove the exact event and forced-abort enforcement contract.
export const DEFAULT_RUNTIME_TOOL_BUDGET_EXECUTION_CONTRACT: Readonly<RuntimeToolBudgetExecutionContract> = Object.freeze({
  enforcementVersion: RUNTIME_TOOL_BUDGET_ENFORCEMENT_VERSION,
  toolExecutionEventSchema: RUNTIME_TOOL_EXECUTION_EVENT_SCHEMA,
  abortConvergenceSchema: RUNTIME_ABORT_CONVERGENCE_EVENT_SCHEMA,
  hardMaxToolCalls: DEFAULT_RUNTIME_TOOL_BUDGET.maxToolCalls,
  softToolCallLimit: DEFAULT_RUNTIME_TOOL_BUDGET.softToolCallLimit,
  finalizationReserve: DEFAULT_RUNTIME_TOOL_BUDGET.finalizationReserve,
  maxRepeatedCalls: DEFAULT_RUNTIME_TOOL_BUDGET.maxRepeatedCalls,
  maxNoProgressCalls: DEFAULT_RUNTIME_TOOL_BUDGET.maxNoProgressCalls,
});

export function runtimeToolBudgetExecutionContract(): RuntimeToolBudgetExecutionContract {
  return { ...DEFAULT_RUNTIME_TOOL_BUDGET_EXECUTION_CONTRACT };
}

export type RuntimeToolBudget = {
  maxToolCalls: number;
  softToolCallLimit: number;
  finalizationReserve: number;
  maxRepeatedCalls: number;
  maxNoProgressCalls: number;
  maxSearchCalls: number;
  maxWriteCalls: number;
  maxReadBytes: number;
  maxWallTimeSeconds: number;
};

export type RuntimeWorkspaceAccessMode =
  | typeof RUNTIME_WORKSPACE_ACCESS_READ
  | typeof RUNTIME_WORKSPACE_ACCESS_WRITE;

export type RuntimeWorkspaceWriteLease = {
  version: typeof RUNTIME_WORKSPACE_WRITE_LEASE_VERSION;
  runId: string;
  workspaceId: string;
  workspaceManifestHash: string;
  allowedRoots: string[];
  expiresAt: number;
};

// This is the policy-relevant projection of spec.workspace. The host-local
// realPath is intentionally not HMAC-bound because it is materialized by the
// Adapter after the ticket is verified; all writable authority is instead
// bound to the Run, Workspace, manifest and bounded relative roots.
export type RuntimeWorkspaceReadMount = {
  realPath: string;
  accessMode: typeof RUNTIME_WORKSPACE_ACCESS_READ;
};

export type RuntimeWorkspaceWriteMount = {
  realPath: string;
  accessMode: typeof RUNTIME_WORKSPACE_ACCESS_WRITE;
  writeLease: RuntimeWorkspaceWriteLease;
};

export type RuntimeWorkspaceMount = RuntimeWorkspaceReadMount | RuntimeWorkspaceWriteMount;

export type RuntimePolicyEnvelope = {
  version: typeof RUNTIME_POLICY_VERSION;
  algorithm: typeof RUNTIME_POLICY_ALGORITHM;
  keyId: string;
  runId: string;
  idempotencyKey: string;
  workspaceManifestHash: string;
  dispatchIdentity: string;
  capabilityHash: string;
  planHash: string;
  issuedAt: number;
  expiresAt: number;
  workspaceAccessMode: RuntimeWorkspaceAccessMode;
  writeLease: RuntimeWorkspaceWriteLease | null;
  requiredTools: string[];
  allowedTools: string[];
  toolBudget: RuntimeToolBudget;
  signature: string;
};

export type VerifiedRuntimePolicy = Omit<RuntimePolicyEnvelope, "signature"> & {
  policyHash: string;
};

export type RuntimePolicyBinding = {
  runId: string;
  idempotencyKey: string;
  workspaceId: string;
  workspaceManifestHash: string;
  dispatchIdentity: string;
  capabilityHash: string;
};

export type RuntimePolicyVerifierConfig = {
  keyId: string;
  runTicketSecret: string;
};

const KEY_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/;
const POLICY_IDENTIFIER_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,511}$/;
const HASH_PATTERN = /^sha256:[a-f0-9]{64}$/i;
const TOOL_NAMES = new Set<string>(SUPPORTED_RUNTIME_POLICY_TOOLS);
const MAX_POLICY_LIFETIME_SECONDS = 15 * 60;
const MAX_CLOCK_SKEW_SECONDS = 60;

export function runtimePolicyVerifierConfigFromEnvironment(environment: NodeJS.ProcessEnv = process.env): RuntimePolicyVerifierConfig | undefined {
  const keyId = String(environment.HUAHUO_RUNTIME_POLICY_KEY_ID ?? "").trim();
  const runTicketSecret = String(environment.HUAHUO_RUNTIME_RUN_TICKET_SECRET ?? "");
  if (!KEY_ID_PATTERN.test(keyId) || Buffer.byteLength(runTicketSecret, "utf8") < 32) return undefined;
  return { keyId, runTicketSecret };
}

export function verifyRuntimePolicy(
  raw: unknown,
  binding: RuntimePolicyBinding,
  config: RuntimePolicyVerifierConfig | undefined = runtimePolicyVerifierConfigFromEnvironment(),
  nowSeconds = Math.floor(Date.now() / 1000),
): VerifiedRuntimePolicy {
  if (!config) throw runtimePolicyError("RUNTIME_TOOL_BUDGET_UNSUPPORTED", "runtime policy verifier is unavailable");
  const policy = readRuntimePolicy(raw);
  validateBinding(policy, binding);
  validatePolicyShape(policy, nowSeconds);
  if (policy.keyId !== config.keyId) throw runtimePolicyError("RUNTIME_PERMISSION_DENIED", "runtime policy key is not active");
  const expected = runtimePolicySignature(policy, config.runTicketSecret);
  if (!safeEqualSignature(policy.signature, expected)) throw runtimePolicyError("RUNTIME_PERMISSION_DENIED", "runtime policy signature is invalid");
  const { signature: _signature, ...unsigned } = policy;
  return { ...unsigned, policyHash: `sha256:${createHash("sha256").update(canonicalRuntimePolicyPayload(unsigned)).digest("hex")}` };
}

export function signRuntimePolicy(policy: Omit<RuntimePolicyEnvelope, "signature">, runTicketSecret: string): RuntimePolicyEnvelope {
  if (Buffer.byteLength(runTicketSecret, "utf8") < 32) throw runtimePolicyError("RUNTIME_TOOL_BUDGET_UNSUPPORTED", "runtime policy signing key is unavailable");
  validatePolicyShape({ ...policy, signature: "sha256:" + "0".repeat(64) }, Math.floor(Date.now() / 1000), false);
  return { ...policy, signature: runtimePolicySignature(policy, runTicketSecret) };
}

export function assertExactRuntimeToolsAllow(raw: unknown, expected: readonly string[]): string[] {
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) {
    throw runtimePolicyError("RUNTIME_INPUT_INVALID", "spec.tools.allow is required");
  }
  const tools = raw as Record<string, unknown>;
  if (Object.keys(tools).length !== 1 || !Object.hasOwn(tools, "allow")) {
    throw runtimePolicyError("RUNTIME_INPUT_INVALID", "spec.tools must contain only allow:string[]");
  }
  const allow = readToolList(tools.allow, "tools.allow");
  if (!sameToolList(allow, expected)) {
    throw runtimePolicyError("RUNTIME_PERMISSION_DENIED", "spec.tools.allow does not match the signed runtime policy");
  }
  return allow;
}

// This is the last Gateway-owned boundary before the canonical spec reaches
// OpenClaw. It rejects an Adapter/Gateway disagreement instead of allowing a
// signed staging lease to be paired with a different mount. The actual
// OpenClaw host filesystem mount is installed separately and must not claim
// readiness until its own pinned-source and live proof exists.
export function assertRuntimeWorkspaceMount(
  raw: unknown,
  policy: VerifiedRuntimePolicy,
  workspaceId: string,
  nowSeconds = Math.floor(Date.now() / 1000),
): RuntimeWorkspaceMount {
  validateVerifiedRuntimePolicyForExecution(policy, nowSeconds);
  if (!safeIdentifier(workspaceId)) {
    throw runtimePolicyError("RUNTIME_PERMISSION_DENIED", "workspace binding is invalid");
  }
  const mount = readRuntimeWorkspaceMount(raw);
  if (mount.accessMode !== policy.workspaceAccessMode) {
    throw runtimePolicyError("RUNTIME_PERMISSION_DENIED", "workspace access mode does not match the signed runtime policy");
  }
  if (policy.workspaceAccessMode === RUNTIME_WORKSPACE_ACCESS_READ) {
    return mount;
  }
  const lease = policy.writeLease;
  if (mount.accessMode !== RUNTIME_WORKSPACE_ACCESS_WRITE || !lease || !sameWriteLease(mount.writeLease, lease) || lease.workspaceId !== workspaceId) {
    throw runtimePolicyError("RUNTIME_PERMISSION_DENIED", "workspace staging lease does not match the signed runtime policy");
  }
  return mount;
}

// Every Gateway-owned native write boundary must call this immediately before
// forwarding a Core write request. It deliberately re-verifies the admitted
// mount and signed policy instead of treating a prior submit-time check as
// durable authorization. Paths are logical POSIX Workspace-relative paths.
export function assertRuntimeWorkspaceWritePath(
  rawPath: unknown,
  rawMount: unknown,
  policy: VerifiedRuntimePolicy,
  workspaceId: string,
  nowSeconds = Math.floor(Date.now() / 1000),
): string {
  const mount = assertRuntimeWorkspaceMount(rawMount, policy, workspaceId, nowSeconds);
  if (mount.accessMode !== RUNTIME_WORKSPACE_ACCESS_WRITE || !policy.allowedTools.includes("write")) {
    throw runtimePolicyError("RUNTIME_PERMISSION_DENIED", "runtime workspace is read-only");
  }

  const relativePath = readRuntimeWorkspaceWritePath(rawPath);
  const root = relativePath.slice(0, relativePath.indexOf("/"));
  if (!mount.writeLease.allowedRoots.includes(root)) {
    throw runtimePolicyError("RUNTIME_PERMISSION_DENIED", "runtime write path is outside the signed staging roots");
  }
  return relativePath;
}

// This is the native filesystem half of the write guard. It is intentionally
// synchronous because Core invokes the Registry callback immediately before
// the actual tool function and the callback must be able to stop that call.
// Every extant component is lstat'ed and realpath'ed: a symlink or Windows
// reparse point present at preflight cannot redirect an otherwise valid
// output/staging path beyond the admitted mount. This is an admission check,
// not a descriptor-relative or O_NOFOLLOW write primitive: Core must provide
// that separately to close a post-check path-replacement race.
export function assertRuntimeWorkspaceNativeWritePath(
  rawPath: unknown,
  rawMount: unknown,
  policy: VerifiedRuntimePolicy,
  workspaceId: string,
  nowSeconds = Math.floor(Date.now() / 1000),
): string {
  const relativePath = assertRuntimeWorkspaceWritePath(rawPath, rawMount, policy, workspaceId, nowSeconds);
  const mount = assertRuntimeWorkspaceMount(rawMount, policy, workspaceId, nowSeconds);
  if (mount.accessMode !== RUNTIME_WORKSPACE_ACCESS_WRITE) {
    throw runtimePolicyError("RUNTIME_PERMISSION_DENIED", "runtime workspace is read-only");
  }

  const workspaceRoot = resolveNativeWorkspaceRoot(mount.realPath);
  const target = resolve(workspaceRoot, ...relativePath.split("/"));
  if (!nativePathIsContained(workspaceRoot, target)) {
    throw runtimePolicyError("RUNTIME_PERMISSION_DENIED", "runtime write target is outside the native workspace root");
  }

  assertNativeWorkspaceSegments(workspaceRoot, target, relativePath.split("/"));
  return target;
}

export function canonicalRuntimePolicyPayload(policy: Omit<RuntimePolicyEnvelope, "signature">): string {
  return JSON.stringify({
    version: policy.version,
    algorithm: policy.algorithm,
    keyId: policy.keyId,
    runId: policy.runId,
    idempotencyKey: policy.idempotencyKey,
    workspaceManifestHash: policy.workspaceManifestHash,
    dispatchIdentity: policy.dispatchIdentity,
    capabilityHash: policy.capabilityHash,
    planHash: policy.planHash,
    issuedAt: policy.issuedAt,
    expiresAt: policy.expiresAt,
    workspaceAccessMode: policy.workspaceAccessMode,
    writeLease: canonicalWriteLease(policy.writeLease),
    requiredTools: policy.requiredTools,
    allowedTools: policy.allowedTools,
    toolBudget: canonicalToolBudget(policy.toolBudget),
  });
}

export function runtimePolicySignature(policy: Omit<RuntimePolicyEnvelope, "signature">, runTicketSecret: string): string {
  const derivedKey = createHmac("sha256", runTicketSecret).update(RUNTIME_POLICY_KEY_DERIVATION_CONTEXT, "utf8").digest();
  const signature = createHmac("sha256", derivedKey).update(canonicalRuntimePolicyPayload(policy), "utf8").digest("hex");
  return `sha256:${signature}`;
}

export function validateVerifiedRuntimePolicy(policy: VerifiedRuntimePolicy): void {
  validateVerifiedRuntimePolicyAt(policy, Math.floor(Date.now() / 1000), false);
}

function validateVerifiedRuntimePolicyForExecution(policy: VerifiedRuntimePolicy, nowSeconds: number): void {
  validateVerifiedRuntimePolicyAt(policy, nowSeconds, true);
}

function validateVerifiedRuntimePolicyAt(policy: VerifiedRuntimePolicy, nowSeconds: number, checkTime: boolean): void {
  const { policyHash, ...unsigned } = policy;
  validatePolicyShape({ ...unsigned, signature: "sha256:" + "0".repeat(64) }, nowSeconds, checkTime);
  if (!HASH_PATTERN.test(policyHash) || policyHash !== `sha256:${createHash("sha256").update(canonicalRuntimePolicyPayload(unsigned)).digest("hex")}`) {
    throw runtimePolicyError("RUNTIME_POLICY_INVALID", "runtime policy hash is invalid");
  }
}

export function runtimePolicyError(code: string, message: string): Error & { code: string } {
  return Object.assign(new Error(message), { code });
}

function readRuntimePolicy(raw: unknown): RuntimePolicyEnvelope {
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) throw runtimePolicyError("RUNTIME_INPUT_INVALID", "runtimePolicy is required");
  const value = raw as Record<string, unknown>;
  const expectedKeys = new Set([
    "version", "algorithm", "keyId", "runId", "idempotencyKey", "workspaceManifestHash", "dispatchIdentity",
    "capabilityHash", "planHash", "issuedAt", "expiresAt", "workspaceAccessMode", "writeLease", "requiredTools", "allowedTools", "toolBudget", "signature",
  ]);
  if (Object.keys(value).length !== expectedKeys.size || Object.keys(value).some((key) => !expectedKeys.has(key))) {
    throw runtimePolicyError("RUNTIME_INPUT_INVALID", "runtimePolicy fields are incomplete or unsupported");
  }
  return {
    version: readString(value.version, "version") as typeof RUNTIME_POLICY_VERSION,
    algorithm: readString(value.algorithm, "algorithm") as typeof RUNTIME_POLICY_ALGORITHM,
    keyId: readString(value.keyId, "keyId"),
    runId: readString(value.runId, "runId"),
    idempotencyKey: readString(value.idempotencyKey, "idempotencyKey"),
    workspaceManifestHash: readString(value.workspaceManifestHash, "workspaceManifestHash"),
    dispatchIdentity: readString(value.dispatchIdentity, "dispatchIdentity"),
    capabilityHash: readString(value.capabilityHash, "capabilityHash"),
    planHash: readString(value.planHash, "planHash"),
    issuedAt: readInteger(value.issuedAt, "issuedAt"),
    expiresAt: readInteger(value.expiresAt, "expiresAt"),
    workspaceAccessMode: readString(value.workspaceAccessMode, "workspaceAccessMode") as RuntimeWorkspaceAccessMode,
    writeLease: readWriteLease(value.writeLease),
    requiredTools: readToolList(value.requiredTools, "requiredTools"),
    allowedTools: readToolList(value.allowedTools, "allowedTools"),
    toolBudget: readToolBudget(value.toolBudget),
    signature: readString(value.signature, "signature"),
  };
}

function readToolList(raw: unknown, field: string): string[] {
  if (!Array.isArray(raw) || raw.some((tool) => typeof tool !== "string" || tool !== tool.trim())) {
    throw runtimePolicyError("RUNTIME_INPUT_INVALID", `runtimePolicy.${field} must be a string array`);
  }
  return [...raw];
}

function readToolBudget(raw: unknown): RuntimeToolBudget {
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) throw runtimePolicyError("RUNTIME_INPUT_INVALID", "runtimePolicy.toolBudget is required");
  const value = raw as Record<string, unknown>;
  const names: Array<keyof RuntimeToolBudget> = [
    "maxToolCalls", "softToolCallLimit", "finalizationReserve", "maxRepeatedCalls", "maxNoProgressCalls",
    "maxSearchCalls", "maxWriteCalls", "maxReadBytes", "maxWallTimeSeconds",
  ];
  if (Object.keys(value).length !== names.length || names.some((name) => !Object.hasOwn(value, name))) {
    throw runtimePolicyError("RUNTIME_INPUT_INVALID", "runtimePolicy.toolBudget fields are incomplete");
  }
  return Object.fromEntries(names.map((name) => [name, readInteger(value[name], `toolBudget.${name}`)])) as RuntimeToolBudget;
}

function validateBinding(policy: RuntimePolicyEnvelope, binding: RuntimePolicyBinding): void {
  if (policy.runId !== binding.runId || policy.idempotencyKey !== binding.idempotencyKey ||
    policy.workspaceManifestHash !== binding.workspaceManifestHash || policy.dispatchIdentity !== binding.dispatchIdentity ||
    policy.capabilityHash !== binding.capabilityHash || !safeIdentifier(binding.workspaceId) ||
    (policy.workspaceAccessMode === RUNTIME_WORKSPACE_ACCESS_WRITE && policy.writeLease?.workspaceId !== binding.workspaceId)) {
    throw runtimePolicyError("RUNTIME_PERMISSION_DENIED", "runtime policy binding mismatch");
  }
}

function validatePolicyShape(policy: RuntimePolicyEnvelope, nowSeconds: number, checkTime = true): void {
  if (policy.version !== RUNTIME_POLICY_VERSION || policy.algorithm !== RUNTIME_POLICY_ALGORITHM || !KEY_ID_PATTERN.test(policy.keyId) ||
    !safeIdentifier(policy.runId) || !safeIdentifier(policy.idempotencyKey) || !HASH_PATTERN.test(policy.workspaceManifestHash) ||
    !HASH_PATTERN.test(policy.dispatchIdentity) || !safeIdentifier(policy.capabilityHash) || !HASH_PATTERN.test(policy.planHash) ||
    !HASH_PATTERN.test(policy.signature)) {
    throw runtimePolicyError("RUNTIME_INPUT_INVALID", "runtimePolicy is malformed");
  }
  if ((checkTime && !Number.isSafeInteger(nowSeconds)) || !Number.isSafeInteger(policy.issuedAt) || !Number.isSafeInteger(policy.expiresAt) || policy.expiresAt <= policy.issuedAt ||
    policy.expiresAt - policy.issuedAt > MAX_POLICY_LIFETIME_SECONDS ||
    (checkTime && (policy.issuedAt > nowSeconds + MAX_CLOCK_SKEW_SECONDS || policy.expiresAt <= nowSeconds))) {
    throw runtimePolicyError("RUNTIME_PERMISSION_DENIED", "runtimePolicy is expired or outside the allowed clock window");
  }
  if (!isSortedUniqueKnownToolList(policy.requiredTools) || !isSortedUniqueKnownToolList(policy.allowedTools) ||
    !sameToolList(policy.requiredTools, policy.allowedTools)) {
    throw runtimePolicyError("RUNTIME_INPUT_INVALID", "runtimePolicy.requiredTools is invalid");
  }
  validateWorkspacePolicy(policy);
  validateToolBudget(policy.toolBudget);
}

function validateWorkspacePolicy(policy: RuntimePolicyEnvelope): void {
  if (policy.workspaceAccessMode === RUNTIME_WORKSPACE_ACCESS_READ) {
    if (policy.writeLease !== null || policy.allowedTools.includes("write")) {
      throw runtimePolicyError("RUNTIME_PERMISSION_DENIED", "read-only runtime policy cannot authorize write");
    }
    return;
  }
  if (policy.workspaceAccessMode !== RUNTIME_WORKSPACE_ACCESS_WRITE || !policy.allowedTools.includes("write") ||
    policy.toolBudget.maxWriteCalls < 1 || !policy.writeLease) {
    throw runtimePolicyError("RUNTIME_PERMISSION_DENIED", "runtime staging write policy is incomplete");
  }
  const lease = policy.writeLease;
  if (lease.version !== RUNTIME_WORKSPACE_WRITE_LEASE_VERSION || !safeIdentifier(lease.runId) ||
    !safeIdentifier(lease.workspaceId) || !HASH_PATTERN.test(lease.workspaceManifestHash) ||
    !sameStringList(lease.allowedRoots, RUNTIME_STAGING_WRITE_ROOTS) || !Number.isSafeInteger(lease.expiresAt) ||
    lease.runId !== policy.runId || lease.workspaceManifestHash !== policy.workspaceManifestHash || lease.expiresAt !== policy.expiresAt) {
    throw runtimePolicyError("RUNTIME_PERMISSION_DENIED", "runtime staging write lease is invalid");
  }
}

function isSortedUniqueKnownToolList(tools: unknown): tools is string[] {
  return Array.isArray(tools) && tools.length <= SUPPORTED_RUNTIME_POLICY_TOOLS.length &&
    tools.every((tool) => typeof tool === "string" && TOOL_NAMES.has(tool)) &&
    tools.every((tool, index) => index === 0 || tools[index - 1] < tool);
}

function sameToolList(left: readonly string[], right: readonly string[]): boolean {
  return left.length === right.length && left.every((tool, index) => tool === right[index]);
}

function sameStringList(left: readonly string[], right: readonly string[]): boolean {
  return left.length === right.length && left.every((value, index) => value === right[index]);
}

function validateToolBudget(budget: RuntimeToolBudget): void {
  const values = Object.values(budget);
  if (values.some((value) => !Number.isSafeInteger(value)) || budget.maxToolCalls < 1 || budget.maxToolCalls > MAX_TOOL_CALLS_SUPPORTED ||
    budget.softToolCallLimit < 1 || budget.softToolCallLimit >= budget.maxToolCalls ||
    budget.finalizationReserve < 1 || budget.finalizationReserve >= budget.maxToolCalls ||
    budget.softToolCallLimit + budget.finalizationReserve > budget.maxToolCalls ||
    budget.maxRepeatedCalls !== DEFAULT_RUNTIME_TOOL_BUDGET.maxRepeatedCalls ||
    budget.maxNoProgressCalls !== DEFAULT_RUNTIME_TOOL_BUDGET.maxNoProgressCalls ||
    budget.maxSearchCalls < 0 || budget.maxSearchCalls > budget.maxToolCalls ||
    budget.maxWriteCalls < 0 || budget.maxWriteCalls > budget.maxToolCalls ||
    budget.maxReadBytes < 1 || budget.maxReadBytes > DEFAULT_RUNTIME_TOOL_BUDGET.maxReadBytes ||
    budget.maxWallTimeSeconds < 1 || budget.maxWallTimeSeconds > 3600) {
    throw runtimePolicyError("RUNTIME_TOOL_BUDGET_UNSUPPORTED", "runtimePolicy.toolBudget is unsupported");
  }
}

function canonicalToolBudget(budget: RuntimeToolBudget): RuntimeToolBudget {
  return {
    maxToolCalls: budget.maxToolCalls,
    softToolCallLimit: budget.softToolCallLimit,
    finalizationReserve: budget.finalizationReserve,
    maxRepeatedCalls: budget.maxRepeatedCalls,
    maxNoProgressCalls: budget.maxNoProgressCalls,
    maxSearchCalls: budget.maxSearchCalls,
    maxWriteCalls: budget.maxWriteCalls,
    maxReadBytes: budget.maxReadBytes,
    maxWallTimeSeconds: budget.maxWallTimeSeconds,
  };
}

function canonicalWriteLease(lease: RuntimeWorkspaceWriteLease | null): RuntimeWorkspaceWriteLease | null {
  if (lease === null) return null;
  return {
    version: lease.version,
    runId: lease.runId,
    workspaceId: lease.workspaceId,
    workspaceManifestHash: lease.workspaceManifestHash,
    allowedRoots: [...lease.allowedRoots],
    expiresAt: lease.expiresAt,
  };
}

function readWriteLease(raw: unknown): RuntimeWorkspaceWriteLease | null {
  if (raw === null) return null;
  const value = readPlainRecord(raw, "runtimePolicy.writeLease");
  const names = ["version", "runId", "workspaceId", "workspaceManifestHash", "allowedRoots", "expiresAt"];
  if (Object.keys(value).length !== names.length || names.some((name) => !Object.hasOwn(value, name))) {
    throw runtimePolicyError("RUNTIME_INPUT_INVALID", "runtimePolicy.writeLease fields are incomplete or unsupported");
  }
  const allowedRoots = readStringArray(value.allowedRoots, "runtimePolicy.writeLease.allowedRoots");
  return {
    version: readString(value.version, "runtimePolicy.writeLease.version") as typeof RUNTIME_WORKSPACE_WRITE_LEASE_VERSION,
    runId: readString(value.runId, "runtimePolicy.writeLease.runId"),
    workspaceId: readString(value.workspaceId, "runtimePolicy.writeLease.workspaceId"),
    workspaceManifestHash: readString(value.workspaceManifestHash, "runtimePolicy.writeLease.workspaceManifestHash"),
    allowedRoots,
    expiresAt: readInteger(value.expiresAt, "runtimePolicy.writeLease.expiresAt"),
  };
}

function readRuntimeWorkspaceMount(raw: unknown): RuntimeWorkspaceMount {
  const value = readPlainRecord(raw, "spec.workspace");
  if (!Object.hasOwn(value, "realPath") || !Object.hasOwn(value, "accessMode")) {
    throw runtimePolicyError("RUNTIME_INPUT_INVALID", "spec.workspace fields are incomplete or unsupported");
  }
  const realPath = readString(value.realPath, "spec.workspace.realPath");
  if (!safeRuntimeWorkspaceRoot(realPath)) {
    throw runtimePolicyError("RUNTIME_PERMISSION_DENIED", "spec.workspace.realPath is not a safe host-local workspace root");
  }
  const accessMode = readString(value.accessMode, "spec.workspace.accessMode") as RuntimeWorkspaceAccessMode;
  if (accessMode === RUNTIME_WORKSPACE_ACCESS_READ) {
    if (Object.keys(value).length !== 2 || Object.hasOwn(value, "writeLease")) {
      throw runtimePolicyError("RUNTIME_INPUT_INVALID", "read-only spec.workspace must contain only realPath and accessMode");
    }
    return { realPath, accessMode };
  }
  if (accessMode === RUNTIME_WORKSPACE_ACCESS_WRITE) {
    if (Object.keys(value).length !== 3 || !Object.hasOwn(value, "writeLease")) {
      throw runtimePolicyError("RUNTIME_INPUT_INVALID", "writable spec.workspace requires exactly one complete writeLease");
    }
    const writeLease = readWriteLease(value.writeLease);
    if (writeLease === null) {
      throw runtimePolicyError("RUNTIME_INPUT_INVALID", "writable spec.workspace writeLease cannot be null");
    }
    return { realPath, accessMode, writeLease };
  }
  throw runtimePolicyError("RUNTIME_INPUT_INVALID", "spec.workspace.accessMode is unsupported");
}

function readRuntimeWorkspaceWritePath(raw: unknown): string {
  if (typeof raw !== "string" || raw.length === 0 || raw.length > 4096 || raw !== raw.trim() ||
    raw.startsWith("/") || raw.includes("\\") || /[\u0000-\u001f\u007f]/.test(raw)) {
    throw runtimePolicyError("RUNTIME_INPUT_INVALID", "runtime write path must be a relative POSIX path");
  }
  const segments = raw.split("/");
  if (segments.length < 2 || segments.some((segment) =>
    segment.length === 0 || segment === "." || segment === ".." || segment.includes(":"),
  )) {
    throw runtimePolicyError("RUNTIME_PERMISSION_DENIED", "runtime write path is not a bounded relative path");
  }
  return raw;
}

function resolveNativeWorkspaceRoot(rawRoot: string): string {
  const enteredRoot = resolve(rawRoot);
  const enteredStat = readNativePathStat(enteredRoot, "runtime workspace native root is unavailable");
  if (!enteredStat.isDirectory() || enteredStat.isSymbolicLink()) {
    throw runtimePolicyError("RUNTIME_PERMISSION_DENIED", "runtime workspace native root is not a directory");
  }

  const resolvedRoot = readNativeRealPath(enteredRoot, "runtime workspace native root is unavailable");
  if (!sameNativePath(enteredRoot, resolvedRoot)) {
    throw runtimePolicyError("RUNTIME_PERMISSION_DENIED", "runtime workspace native root contains a symlink or reparse point");
  }
  const resolvedStat = readNativePathStat(resolvedRoot, "runtime workspace native root is unavailable");
  if (!resolvedStat.isDirectory() || resolvedStat.isSymbolicLink()) {
    throw runtimePolicyError("RUNTIME_PERMISSION_DENIED", "runtime workspace native root is not a directory");
  }
  return resolvedRoot;
}

function assertNativeWorkspaceSegments(workspaceRoot: string, target: string, segments: string[]): void {
  let current = workspaceRoot;
  for (let index = 0; index < segments.length; index += 1) {
    current = resolve(current, segments[index]!);
    if (!nativePathIsContained(workspaceRoot, current)) {
      throw runtimePolicyError("RUNTIME_PERMISSION_DENIED", "runtime write target is outside the native workspace root");
    }

    let stat: NonNullable<ReturnType<typeof lstatSync>>;
    try {
      stat = lstatSync(current);
    } catch (error) {
      if (isMissingNativePath(error)) return;
      throw runtimePolicyError("RUNTIME_PERMISSION_DENIED", "runtime write target cannot be inspected");
    }
    if (stat.isSymbolicLink()) {
      throw runtimePolicyError("RUNTIME_PERMISSION_DENIED", "runtime write target contains a symlink or reparse point");
    }
    if (index < segments.length - 1 && !stat.isDirectory()) {
      throw runtimePolicyError("RUNTIME_PERMISSION_DENIED", "runtime write target has a non-directory parent");
    }
    if (index === segments.length - 1 && !stat.isFile()) {
      throw runtimePolicyError("RUNTIME_PERMISSION_DENIED", "runtime write target is not a regular file");
    }

    const resolvedCurrent = readNativeRealPath(current, "runtime write target cannot be resolved");
    if (!nativePathIsContained(workspaceRoot, resolvedCurrent) || !sameNativePath(current, resolvedCurrent)) {
      throw runtimePolicyError("RUNTIME_PERMISSION_DENIED", "runtime write target contains a symlink or reparse point");
    }
  }

  if (!sameNativePath(current, target)) {
    throw runtimePolicyError("RUNTIME_PERMISSION_DENIED", "runtime write target is invalid");
  }
}

function readNativePathStat(path: string, message: string): NonNullable<ReturnType<typeof lstatSync>> {
  try {
    const stat = lstatSync(path);
    if (!stat) throw runtimePolicyError("RUNTIME_PERMISSION_DENIED", message);
    return stat;
  } catch {
    throw runtimePolicyError("RUNTIME_PERMISSION_DENIED", message);
  }
}

function readNativeRealPath(path: string, message: string): string {
  try {
    return realpathSync.native(path);
  } catch {
    throw runtimePolicyError("RUNTIME_PERMISSION_DENIED", message);
  }
}

function isMissingNativePath(error: unknown): boolean {
  return Boolean(error && typeof error === "object" && (error as NodeJS.ErrnoException).code === "ENOENT");
}

function nativePathIsContained(root: string, candidate: string): boolean {
  const pathFromRoot = relative(root, candidate);
  return pathFromRoot === "" || (!isAbsolute(pathFromRoot) && pathFromRoot !== ".." && !pathFromRoot.startsWith(`..${sep}`));
}

function sameNativePath(left: string, right: string): boolean {
  const normalize = (path: string) => {
    const trimmed = path.length > 1 ? path.replace(/[\\/]+$/, "") : path;
    return process.platform === "win32" ? trimmed.toLocaleLowerCase("en-US") : trimmed;
  };
  return normalize(left) === normalize(right);
}

function sameWriteLease(left: RuntimeWorkspaceWriteLease, right: RuntimeWorkspaceWriteLease): boolean {
  return left.version === right.version && left.runId === right.runId && left.workspaceId === right.workspaceId &&
    left.workspaceManifestHash === right.workspaceManifestHash && left.expiresAt === right.expiresAt &&
    sameStringList(left.allowedRoots, right.allowedRoots);
}

function readPlainRecord(raw: unknown, field: string): Record<string, unknown> {
  if (!raw || typeof raw !== "object" || Array.isArray(raw) || Object.getPrototypeOf(raw) !== Object.prototype) {
    throw runtimePolicyError("RUNTIME_INPUT_INVALID", `${field} must be a plain object`);
  }
  return raw as Record<string, unknown>;
}

function readString(raw: unknown, field: string): string {
  if (typeof raw !== "string") throw runtimePolicyError("RUNTIME_INPUT_INVALID", `runtimePolicy.${field} must be a string`);
  return raw;
}

function readStringArray(raw: unknown, field: string): string[] {
  if (!Array.isArray(raw) || raw.some((value) => typeof value !== "string" || value !== value.trim())) {
    throw runtimePolicyError("RUNTIME_INPUT_INVALID", `${field} must be a string array`);
  }
  return [...raw];
}

function readInteger(raw: unknown, field: string): number {
  if (!Number.isSafeInteger(raw)) throw runtimePolicyError("RUNTIME_INPUT_INVALID", `runtimePolicy.${field} must be an integer`);
  return Number(raw);
}

function safeRuntimeWorkspaceRoot(value: string): boolean {
  if (!value.startsWith("/") || value.length > 4096 || /[\u0000-\u001f\u007f]/.test(value)) return false;
  const segments = value.split("/");
  return segments.every((segment, index) => index === 0 || (segment !== "" && segment !== "." && segment !== ".."));
}

function safeIdentifier(value: string): boolean {
  return POLICY_IDENTIFIER_PATTERN.test(value);
}

function safeEqualSignature(left: string, right: string): boolean {
  if (!HASH_PATTERN.test(left) || !HASH_PATTERN.test(right)) return false;
  return timingSafeEqual(Buffer.from(left, "utf8"), Buffer.from(right, "utf8"));
}
