import { AsyncLocalStorage } from "node:async_hooks";
import { createHash, createHmac, timingSafeEqual } from "node:crypto";

export const PRIVATE_RUN_CONTEXT_VERSION = "huahuo.private-run-context.v1";
export const PRIVATE_RUN_CONTEXT_BRIDGE_SYMBOL = Symbol.for("huahuo.private-run-context.v1");
export const RUNTIME_SUBMIT_BINDING_VERSION = "runtime_submit_binding.v2";

const MAX_CLOCK_SKEW_SECONDS = 60;
const MAX_RUN_TICKET_BYTES = 8192;
const MAX_RUN_TICKET_TTL_SECONDS = 15 * 60;
const MAX_WORKSPACE_SEARCH_REQUEST_BYTES = 64 * 1024;
const MAX_WORKSPACE_SEARCH_RESPONSE_BYTES = 256 * 1024;
const DEFAULT_WORKSPACE_SEARCH_TIMEOUT_MS = 12_000;
const WORKSPACE_SEARCH_PROXY_PATH = "/internal/v1/runtime/workspace-search";
const WORKSPACE_SEARCH_TOOL_CALL_ID_HEADER = "x-huahuo-tool-call-id";
const HASH_PATTERN = /^sha256:[a-f0-9]{64}$/;
const WORKSPACE_SEARCH_TOOL_CALL_ID_PATTERN = /^[A-Za-z0-9._:-]{1,256}$/;
const RUNTIME_SUBMIT_INPUT_HASH_DOMAIN = "huahuo.runtime.submit.input.v1\x00";
const RUNTIME_SUBMIT_PRODUCT_SESSION_HASH_DOMAIN = "huahuo.runtime.submit.product_session.v1\x00";

export type PrivateExecutionContextEnvelope = {
  version: typeof PRIVATE_RUN_CONTEXT_VERSION;
  runTicket: string;
};

export type PrivateRunContextBinding = {
  runId: string;
  tenantId: string;
  workspaceId: string;
  runtimeHostId: string;
  reservationId: string;
  fencingToken: number;
  capabilityHash: string;
  workspaceManifestHash: string;
  runTicketJtiHash: string;
  planHash: string;
  workspaceSearchAllowed: boolean;
  // This is required only by the async model-start submit path. Generic
  // ticketed operations intentionally omit it and remain unbound.
  submitBinding?: PrivateRunContextSubmitBinding;
};

export type PrivateRunContextSubmitBinding = {
  inputMessage: string;
  runtimeConfigId: string;
  runtimeConfigVersion: string;
  productSessionThreadId: string;
  productSessionKey: string;
};

// This value is intentionally process-local. It is never added to RuntimeRunSpec,
// durable registry state, events, status responses, or model-visible tool context.
export type PrivateRunContext = Readonly<{
  runId: string;
  tenantId: string;
  workspaceId: string;
  runtimeHostId: string;
  reservationId: string;
  fencingToken: number;
  capabilityHash: string;
  workspaceManifestHash: string;
  runTicketJtiHash: string;
  planHash: string;
  workspaceSearchAllowed: boolean;
  runTicket: string;
}>;

type RunTicketClaims = {
  runId: string;
  tenantId: string;
  reservationId: string;
  runtimeHostId: string;
  capabilityHash: string;
  workspaceId: string;
  workspaceVersion: number;
  contextGeneration: number;
  inputManifestHash: string;
  planHash: string;
  submitBinding?: RunTicketSubmitBinding;
  fencingToken: number;
  jti: string;
  exp: number;
  iat: number;
};

type RunTicketSubmitBinding = {
  version: string;
  inputMessageHash: string;
  runtimeConfigId: string;
  runtimeConfigVersion: string;
  productSessionHash: string;
};

type PrivateRunContextBridge = Readonly<{
  // This intentionally exposes an operation, never the raw ticket or the
  // surrounding private context, to a plugin. OpenClaw's plugin tool factory
  // context has no per-call Run ID, so identity must come exclusively from
  // the active ALS scope rather than a model-visible or optional SDK field.
  workspaceSearch: (input: Record<string, unknown>, traceId: string | undefined, toolCallId: string) => Promise<unknown>;
}>;

const storage = new AsyncLocalStorage<PrivateRunContext>();
const bridge: PrivateRunContextBridge = Object.freeze({
  workspaceSearch: (input, traceId, toolCallId) => callWorkspaceSearchProxy(input, traceId, toolCallId),
});

installPrivateRunContextBridge();

export function verifyPrivateExecutionContext(
  raw: unknown,
  binding: PrivateRunContextBinding,
  environment: NodeJS.ProcessEnv = process.env,
  nowSeconds = Math.floor(Date.now() / 1000),
): PrivateRunContext {
  let envelope: PrivateExecutionContextEnvelope;
  try {
    envelope = readPrivateExecutionContextEnvelope(raw);
  } catch {
    throw privateRunContextError("envelope");
  }
  const secret = String(environment.HUAHUO_RUNTIME_RUN_TICKET_SECRET ?? "");
  if (Buffer.byteLength(secret, "utf8") < 32) throw privateRunContextError("secret");
  let claims: RunTicketClaims;
  try {
    claims = verifyRunTicket(envelope.runTicket, secret, nowSeconds);
  } catch {
    throw privateRunContextError("ticket");
  }
  try {
    assertTicketBinding(claims, binding);
  } catch {
    throw privateRunContextError("binding");
  }
  return Object.freeze({
    runId: claims.runId,
    tenantId: claims.tenantId,
    workspaceId: claims.workspaceId,
    runtimeHostId: claims.runtimeHostId,
    reservationId: claims.reservationId,
    fencingToken: claims.fencingToken,
    capabilityHash: claims.capabilityHash,
    workspaceManifestHash: claims.inputManifestHash,
    runTicketJtiHash: `sha256:${createHash("sha256").update(claims.jti, "utf8").digest("hex")}`,
    planHash: claims.planHash,
    workspaceSearchAllowed: binding.workspaceSearchAllowed,
    runTicket: envelope.runTicket,
  });
}

export function runWithPrivateRunContext<T>(context: PrivateRunContext, callback: () => T): T {
  return storage.run(context, callback);
}

// Keep this byte-for-byte aligned with runtime.RunTicketInputMessageHash in
// Backend. The caller supplies the original JSON string; this function never
// trims, normalizes, or otherwise rewrites model input before hashing it.
export function runtimeSubmitInputMessageHash(value: string): string {
  return `sha256:${createHash("sha256")
    .update(RUNTIME_SUBMIT_INPUT_HASH_DOMAIN, "utf8")
    .update(value, "utf8")
    .digest("hex")}`;
}

export function runtimeSubmitProductSessionHash(threadId: string, openclawSessionKey: string): string {
  return `sha256:${createHash("sha256")
    .update(RUNTIME_SUBMIT_PRODUCT_SESSION_HASH_DOMAIN, "utf8")
    .update(threadId, "utf8")
    .update("\x00", "utf8")
    .update(openclawSessionKey, "utf8")
    .digest("hex")}`;
}

function installPrivateRunContextBridge(): void {
  const existing = Object.getOwnPropertyDescriptor(globalThis, PRIVATE_RUN_CONTEXT_BRIDGE_SYMBOL);
  if (existing) {
    if (existing.value !== bridge || existing.configurable || existing.writable || existing.enumerable) {
      throw privateRunContextError();
    }
    return;
  }
  Object.defineProperty(globalThis, PRIVATE_RUN_CONTEXT_BRIDGE_SYMBOL, {
    value: bridge,
    configurable: false,
    enumerable: false,
    writable: false,
  });
}

function readPrivateExecutionContextEnvelope(raw: unknown): PrivateExecutionContextEnvelope {
  const value = asRecord(raw);
  if (Object.keys(value).length !== 2 || value.version !== PRIVATE_RUN_CONTEXT_VERSION || !compactRunTicket(value.runTicket)) {
    throw privateRunContextError();
  }
  return { version: PRIVATE_RUN_CONTEXT_VERSION, runTicket: value.runTicket };
}

function verifyRunTicket(ticket: string, secret: string, nowSeconds: number): RunTicketClaims {
  const parts = ticket.split(".");
  if (parts.length !== 3 || parts.some((part) => !part)) throw privateRunContextError();
  const signed = `${parts[0]}.${parts[1]}`;
  const signature = decodeBase64URL(parts[2]);
  const expected = createHmac("sha256", secret).update(signed, "utf8").digest();
  if (!signature || signature.length !== expected.length || !timingSafeEqual(signature, expected)) throw privateRunContextError();
  const header = decodeJSON(parts[0]);
  if (Object.keys(header).length !== 2 || header.alg !== "HS256" || header.typ !== "HUAHUO-RUN-TICKET") throw privateRunContextError();
  const rawClaims = decodeJSON(parts[1]);
  const claims: RunTicketClaims = {
    runId: stringField(rawClaims, "runId"),
    tenantId: stringField(rawClaims, "tenantId"),
    reservationId: stringField(rawClaims, "reservationId"),
    runtimeHostId: stringField(rawClaims, "runtimeHostId"),
    capabilityHash: stringField(rawClaims, "capabilityHash"),
    workspaceId: stringField(rawClaims, "workspaceId"),
    workspaceVersion: integerField(rawClaims, "workspaceVersion"),
    contextGeneration: integerField(rawClaims, "contextGeneration"),
    inputManifestHash: stringField(rawClaims, "inputManifestHash"),
    planHash: stringField(rawClaims, "planHash"),
    submitBinding: readRunTicketSubmitBinding(rawClaims),
    fencingToken: integerField(rawClaims, "fencingToken"),
    jti: stringField(rawClaims, "jti"),
    exp: integerField(rawClaims, "exp"),
    iat: integerField(rawClaims, "iat"),
  };
  if (!nonEmptyString(claims.runId) || !nonEmptyString(claims.tenantId) || !nonEmptyString(claims.reservationId) ||
    !nonEmptyString(claims.runtimeHostId) || !nonEmptyString(claims.capabilityHash) || !nonEmptyString(claims.workspaceId) ||
    claims.workspaceVersion < 1 || claims.contextGeneration < 1 || !HASH_PATTERN.test(claims.inputManifestHash) ||
    !HASH_PATTERN.test(claims.planHash) || claims.fencingToken < 1 || !nonEmptyString(claims.jti) || claims.iat < 1 ||
    claims.exp <= claims.iat || claims.exp - claims.iat > MAX_RUN_TICKET_TTL_SECONDS || claims.exp <= nowSeconds || claims.iat > nowSeconds + MAX_CLOCK_SKEW_SECONDS ||
    !validRunTicketSubmitBinding(claims.submitBinding)) {
    throw privateRunContextError();
  }
  return claims;
}

function assertTicketBinding(claims: RunTicketClaims, binding: PrivateRunContextBinding): void {
  if (!validBinding(binding) || claims.runId !== binding.runId || claims.tenantId !== binding.tenantId ||
    claims.workspaceId !== binding.workspaceId || claims.runtimeHostId !== binding.runtimeHostId ||
    claims.reservationId !== binding.reservationId || claims.fencingToken !== binding.fencingToken ||
    claims.capabilityHash !== binding.capabilityHash || claims.inputManifestHash !== binding.workspaceManifestHash ||
    claims.planHash !== binding.planHash || `sha256:${createHash("sha256").update(claims.jti, "utf8").digest("hex")}` !== binding.runTicketJtiHash ||
    !matchesSubmitBinding(claims.submitBinding, binding.submitBinding)) {
    throw privateRunContextError();
  }
}

function validBinding(binding: PrivateRunContextBinding): boolean {
  return nonEmptyString(binding.runId) && nonEmptyString(binding.tenantId) && nonEmptyString(binding.workspaceId) &&
    nonEmptyString(binding.runtimeHostId) && nonEmptyString(binding.reservationId) && nonEmptyString(binding.capabilityHash) &&
    Number.isSafeInteger(binding.fencingToken) && binding.fencingToken > 0 && HASH_PATTERN.test(binding.workspaceManifestHash) &&
    HASH_PATTERN.test(binding.runTicketJtiHash) && HASH_PATTERN.test(binding.planHash) && typeof binding.workspaceSearchAllowed === "boolean" &&
    (binding.submitBinding === undefined || validPrivateRunContextSubmitBinding(binding.submitBinding));
}

function readRunTicketSubmitBinding(claims: Record<string, unknown>): RunTicketSubmitBinding | undefined {
  if (!Object.hasOwn(claims, "submitBinding")) return undefined;
  const binding = asRecord(claims.submitBinding);
  return {
    version: stringField(binding, "version"),
    inputMessageHash: stringField(binding, "inputMessageHash"),
    runtimeConfigId: stringField(binding, "runtimeConfigId"),
    runtimeConfigVersion: stringField(binding, "runtimeConfigVersion"),
    productSessionHash: stringField(binding, "productSessionHash"),
  };
}

function validRunTicketSubmitBinding(binding: RunTicketSubmitBinding | undefined): boolean {
  return binding === undefined || (
    binding.version === RUNTIME_SUBMIT_BINDING_VERSION &&
    HASH_PATTERN.test(binding.inputMessageHash) &&
    validRunTicketBindingIdentifier(binding.runtimeConfigId) &&
    validRunTicketBindingVersion(binding.runtimeConfigVersion) &&
    HASH_PATTERN.test(binding.productSessionHash)
  );
}

function validPrivateRunContextSubmitBinding(binding: PrivateRunContextSubmitBinding): boolean {
  return typeof binding.inputMessage === "string" &&
    validRunTicketBindingIdentifier(binding.runtimeConfigId) &&
    validRunTicketBindingVersion(binding.runtimeConfigVersion) &&
    validPrivateSessionThreadID(binding.productSessionThreadId) &&
    validPrivateSessionKey(binding.productSessionKey);
}

function matchesSubmitBinding(ticket: RunTicketSubmitBinding | undefined, expected: PrivateRunContextSubmitBinding | undefined): boolean {
  if (expected === undefined) return ticket === undefined;
  return ticket !== undefined && ticket.version === RUNTIME_SUBMIT_BINDING_VERSION &&
    ticket.inputMessageHash === runtimeSubmitInputMessageHash(expected.inputMessage) &&
    ticket.runtimeConfigId === expected.runtimeConfigId &&
    ticket.runtimeConfigVersion === expected.runtimeConfigVersion &&
    ticket.productSessionHash === runtimeSubmitProductSessionHash(expected.productSessionThreadId, expected.productSessionKey);
}

function validRunTicketBindingIdentifier(value: string): boolean {
  return value.length > 0 && value.length <= 256 && /^[A-Za-z0-9_.-]+$/.test(value);
}

function validPrivateSessionThreadID(value: unknown): value is string {
  return typeof value === "string" && value.length > 0 && value.length <= 256 &&
    value === value.trim() && /^[A-Za-z0-9_.:-]+$/.test(value);
}

function validPrivateSessionKey(value: unknown): value is string {
  return typeof value === "string" && value.length > 0 && value.length <= 1024 &&
    value === value.trim() && /^[A-Za-z0-9_.:-]+$/.test(value);
}

function validRunTicketBindingVersion(value: string): boolean {
  return validRunTicketBindingIdentifier(value);
}

function decodeBase64URL(value: string): Buffer | undefined {
  if (!/^[A-Za-z0-9_-]+$/.test(value)) return undefined;
  try { return Buffer.from(value, "base64url"); } catch { return undefined; }
}

function decodeJSON(value: string): Record<string, unknown> {
  const decoded = decodeBase64URL(value);
  if (!decoded) throw privateRunContextError();
  try { return asRecord(JSON.parse(decoded.toString("utf8"))); } catch { throw privateRunContextError(); }
}

function asRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : {};
}

function stringField(value: Record<string, unknown>, key: string): string {
  return typeof value[key] === "string" ? value[key] : "";
}

function integerField(value: Record<string, unknown>, key: string): number {
  return Number.isSafeInteger(value[key]) ? Number(value[key]) : 0;
}

function nonEmptyString(value: unknown): value is string {
  return typeof value === "string" && value.length > 0 && value.length <= 512 && value === value.trim() && !/[\u0000-\u001f\u007f]/.test(value);
}

function compactRunTicket(value: unknown): value is string {
  return typeof value === "string" && Buffer.byteLength(value, "utf8") <= MAX_RUN_TICKET_BYTES &&
    /^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$/.test(value);
}

async function callWorkspaceSearchProxy(input: Record<string, unknown>, traceId: string | undefined, toolCallId: string): Promise<unknown> {
  const context = storage.getStore();
  if (!context || !context.workspaceSearchAllowed || !isPlainRecord(input) || !validWorkspaceSearchToolCallID(toolCallId)) {
    throw workspaceSearchError("WORKSPACE_SEARCH_FORBIDDEN");
  }
  const endpoint = workspaceSearchProxyURL();
  const body = JSON.stringify(input);
  if (Buffer.byteLength(body, "utf8") > MAX_WORKSPACE_SEARCH_REQUEST_BYTES) {
    throw workspaceSearchError("WORKSPACE_SEARCH_INPUT_INVALID");
  }
  const fetchImpl = globalThis.fetch;
  if (typeof fetchImpl !== "function") throw workspaceSearchError("WORKSPACE_SEARCH_UNAVAILABLE");
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), DEFAULT_WORKSPACE_SEARCH_TIMEOUT_MS);
  try {
    const response = await fetchImpl(endpoint, {
      method: "POST",
      headers: compactHeaders({
        "content-type": "application/json",
        accept: "application/json",
        authorization: `RunTicket ${context.runTicket}`,
        "x-run-id": context.runId,
        "x-trace-id": safeTraceID(traceId),
        [WORKSPACE_SEARCH_TOOL_CALL_ID_HEADER]: toolCallId,
      }),
      body,
      signal: controller.signal,
    });
    const envelope = asRecord(await boundedJSON(response));
    if (!response.ok) throw workspaceSearchError(workspaceSearchResponseErrorCode(response.status, envelope));
    if (envelope.success !== true || !Object.hasOwn(envelope, "data")) {
      throw workspaceSearchError("WORKSPACE_SEARCH_RESPONSE_INVALID");
    }
    return envelope.data;
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError") throw workspaceSearchError("WORKSPACE_SEARCH_TIMEOUT");
    if (hasErrorCode(error)) throw error;
    throw workspaceSearchError("WORKSPACE_SEARCH_UNAVAILABLE");
  } finally {
    clearTimeout(timer);
  }
}

function workspaceSearchProxyURL(): string {
  const configured = String(process.env.HUAHUO_RUNTIME_WORKSPACE_SEARCH_PROXY_URL ?? "http://127.0.0.1:18791");
  let endpoint: URL;
  try { endpoint = new URL(configured); } catch { throw workspaceSearchError("WORKSPACE_SEARCH_UNAVAILABLE"); }
  if (endpoint.protocol !== "http:" || (endpoint.hostname !== "127.0.0.1" && endpoint.hostname !== "[::1]" && endpoint.hostname !== "::1") ||
    !endpoint.port || endpoint.username || endpoint.password || endpoint.search || endpoint.hash || (endpoint.pathname !== "/" && endpoint.pathname !== "")) {
    throw workspaceSearchError("WORKSPACE_SEARCH_UNAVAILABLE");
  }
  endpoint.pathname = WORKSPACE_SEARCH_PROXY_PATH;
  return endpoint.toString();
}

async function boundedJSON(response: Response): Promise<unknown> {
  const text = await response.text();
  if (Buffer.byteLength(text, "utf8") > MAX_WORKSPACE_SEARCH_RESPONSE_BYTES) {
    throw workspaceSearchError("WORKSPACE_SEARCH_RESPONSE_INVALID");
  }
  try { return JSON.parse(text); } catch { throw workspaceSearchError("WORKSPACE_SEARCH_RESPONSE_INVALID"); }
}

function workspaceSearchResponseErrorCode(status: number, envelope: Record<string, unknown>): string {
  const error = asRecord(envelope.error);
  const code = typeof error.code === "string" ? error.code : "";
  if (code === "WORKSPACE_SEARCH_INPUT_INVALID" || code === "WORKSPACE_VECTOR_SEARCH_UNAVAILABLE" || code === "WORKSPACE_SEARCH_INDEX_STALE") return code;
  if (code === "RUNTIME_PERMISSION_DENIED" || code === "RUNTIME_HOST_UNAUTHORIZED" || status === 401 || status === 403) return "WORKSPACE_SEARCH_FORBIDDEN";
  if (status === 400 || status === 422) return "WORKSPACE_SEARCH_INPUT_INVALID";
  if (status === 409) return "WORKSPACE_SEARCH_INDEX_STALE";
  if (status === 503) return "WORKSPACE_SEARCH_UNAVAILABLE";
  return "WORKSPACE_SEARCH_FAILED";
}

function isPlainRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value) && Object.getPrototypeOf(value) === Object.prototype;
}

function compactHeaders(headers: Record<string, string | undefined>): Record<string, string> {
  return Object.fromEntries(Object.entries(headers).filter(([, value]) => value !== undefined && value !== "")) as Record<string, string>;
}

function safeTraceID(value: unknown): string | undefined {
  return typeof value === "string" && /^[A-Za-z0-9._-]{1,128}$/.test(value) ? value : undefined;
}

function validWorkspaceSearchToolCallID(value: unknown): value is string {
  return typeof value === "string" && WORKSPACE_SEARCH_TOOL_CALL_ID_PATTERN.test(value);
}

function hasErrorCode(value: unknown): value is Error & { code: string } {
  return Boolean(value) && typeof value === "object" && typeof (value as { code?: unknown }).code === "string";
}

function workspaceSearchError(code: string): Error & { code: string } {
  return Object.assign(new Error("workspace search proxy request failed"), { code });
}

function privateRunContextError(stage?: "envelope" | "secret" | "ticket" | "binding"): Error & { code: string } {
  const suffix = stage ? ` at ${stage}` : "";
  return Object.assign(new Error(`private run context is invalid${suffix}`), { code: "RUNTIME_PERMISSION_DENIED" });
}
