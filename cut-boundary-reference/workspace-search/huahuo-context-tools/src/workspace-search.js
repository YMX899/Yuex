import { createHash, randomUUID } from "node:crypto";

export const WORKSPACE_SEARCH_PLUGIN_ID = "huahuo-context-tools";
export const WORKSPACE_SEARCH_PLUGIN_VERSION = "0.5.0";
export const WORKSPACE_SEARCH_SCHEMA_ID = "huahuo.workspace_search.v1";

const DEFAULT_LIMIT = 10;
const HARD_LIMIT = 20;
const RETRIEVAL_MODES = new Set(["hybrid", "semantic", "keyword"]);
const SCOPES = new Set(["materials", "profile", "creative", "projects", "hotspots"]);
const KINDS = new Set([
  "source", "transcript", "minutes", "summary", "deposit", "viewpoint", "story", "product",
  "positioning", "creative_asset", "project", "hotspot", "note",
]);
const SOURCE_TYPES = new Set(["recording", "upload", "work_ai_chat", "feed_ai_chat", "manual"]);
const INPUT_KEYS = new Set([
  "query", "retrievalMode", "timeExpression", "fromTime", "toTime", "timeField", "scopes",
  "kinds", "sourceTypes", "materialId", "limit",
]);
const FORBIDDEN_RESULT_KEYS = new Set([
  "content", "text", "snippet", "summary", "evidence", "excerpt", "embedding",
  "tenantId", "userId", "workspaceId", "root", "rootPath", "realPath", "sessionKey",
  "openclawSessionKey", "runId", "runTicket", "authorization", "nextCursor", "runtimeHostId",
  "reservationId", "fencingToken", "capabilityHash", "workspaceManifestHash", "inputManifestHash",
  "runTicketJtiHash", "planHash",
]);
const ALLOWED_RESULT_KEYS = new Set([
  "path", "kind", "materialId", "variant", "contentTime", "updatedAt", "score", "matchMode", "stale",
]);
const REQUIRED_RESPONSE_KEYS = new Set([
  "queryFingerprint", "workspaceVersion", "indexVersion", "indexStatus", "results",
]);
// Older relays can still include these internal provider identity fields. They
// are tolerated only for wire compatibility and are deliberately omitted from
// the model-visible result below.
const REDACTED_RESPONSE_KEYS = new Set(["embeddingModel", "embeddingVersion"]);
const ALLOWED_RESPONSE_KEYS = new Set([...REQUIRED_RESPONSE_KEYS, ...REDACTED_RESPONSE_KEYS]);
const MATCH_MODES = new Set(["hybrid", "semantic", "keyword", "metadata"]);
const INDEX_STATUSES = new Set(["current", "stale"]);
const TOOL_CALL_ID_PATTERN = /^[A-Za-z0-9._:-]{1,256}$/;

export const workspaceSearchParameters = {
  type: "object",
  additionalProperties: false,
  properties: {
    query: { type: "string", maxLength: 4000, description: "Natural-language or keyword query." },
    retrievalMode: { type: "string", enum: ["hybrid", "semantic", "keyword"] },
    timeExpression: { type: "string", maxLength: 200 },
    fromTime: { type: "string", maxLength: 64 },
    toTime: { type: "string", maxLength: 64 },
    timeField: { type: "string", enum: ["content_time", "updated_at"] },
    scopes: { type: "array", maxItems: 20, uniqueItems: true, items: { type: "string", enum: [...SCOPES] } },
    kinds: { type: "array", maxItems: 20, uniqueItems: true, items: { type: "string", enum: [...KINDS] } },
    sourceTypes: { type: "array", maxItems: 20, uniqueItems: true, items: { type: "string", enum: [...SOURCE_TYPES] } },
    materialId: { type: "string", maxLength: 256 },
    limit: { type: "integer", minimum: 1, maximum: HARD_LIMIT },
  },
};

// This identity is computed from the exact schema object handed to
// registerTool. A generated environment contract may describe it, but cannot
// make an unregistered plugin appear ready.
export const WORKSPACE_SEARCH_SCHEMA_HASH = `sha256:${createHash("sha256").update(JSON.stringify(workspaceSearchParameters), "utf8").digest("hex")}`;
export const workspaceSearchToolCapability = Object.freeze({
  name: "workspace_search",
  source: "plugin",
  pluginId: WORKSPACE_SEARCH_PLUGIN_ID,
  pluginVersion: WORKSPACE_SEARCH_PLUGIN_VERSION,
  schemaId: WORKSPACE_SEARCH_SCHEMA_ID,
  schemaHash: WORKSPACE_SEARCH_SCHEMA_HASH,
});

export function createWorkspaceSearchTool(context = {}, dependencies = {}) {
  return {
    name: "workspace_search",
    label: "Search workspace paths",
    description: "Locate relevant paths in the current Run Workspace using keyword and vector retrieval. Read selected files separately.",
    parameters: workspaceSearchParameters,
    execute: async (runtimeToolCallId, rawParams) => {
      const toolCallId = requireTrustedToolCallId(runtimeToolCallId);
      const params = asRecord(rawParams);
      const trusted = requireTrustedRuntimeContext(context);
      const request = buildSearchRequest(params);
      const payload = await callWorkspaceSearch(trusted, request, toolCallId);
      const sanitized = sanitizeSearchResponse(payload, request);
      return { content: [{ type: "text", text: JSON.stringify(sanitized) }], details: sanitized };
    },
  };
}

export async function probeWorkspaceSearch(context = {}, _dependencies = {}) {
  const trusted = requireTrustedRuntimeContext(context);
  try {
    const request = { query: "readiness", retrievalMode: "hybrid", limit: 1 };
    const payload = await callWorkspaceSearch(trusted, request, createProbeToolCallId());
    sanitizeSearchResponse(payload, request);
    return { ready: true, code: "ready" };
  } catch (error) {
    return { ready: false, code: error?.code || "WORKSPACE_SEARCH_UNAVAILABLE" };
  }
}

function requireTrustedRuntimeContext(context) {
  const runtime = asRecord(context.runtime);
  const traceId = firstString(context.traceId, runtime.traceId);
  const bridge = readPrivateWorkspaceSearchBridge();
  // OpenClaw's registered plugin tool context intentionally has no trustworthy
  // per-call Run identifier. The immutable bridge resolves Run identity from
  // Gateway-owned AsyncLocalStorage, never from this model-adjacent context.
  return { traceId, bridge };
}

function readPrivateWorkspaceSearchBridge() {
  const symbol = Symbol.for("huahuo.private-run-context.v1");
  const descriptor = Object.getOwnPropertyDescriptor(globalThis, symbol);
  const bridge = descriptor?.value;
  if (!descriptor || descriptor.configurable || descriptor.writable || descriptor.enumerable || !bridge || typeof bridge.workspaceSearch !== "function" || Object.prototype.hasOwnProperty.call(bridge, "get")) {
    throw toolError("WORKSPACE_SEARCH_FORBIDDEN", "trusted Runtime search context is unavailable");
  }
  return bridge;
}

function buildSearchRequest(params) {
  rejectIdentityFields(params);
  assertKnownInputFields(params);
  const query = boundedString(params, "query", 4000);
  const retrievalMode = optionalEnum(params, "retrievalMode", RETRIEVAL_MODES);
  const timeExpression = boundedString(params, "timeExpression", 200);
  const fromTime = boundedString(params, "fromTime", 64);
  const toTime = boundedString(params, "toTime", 64);
  const timeField = optionalEnum(params, "timeField", new Set(["content_time", "updated_at"]));
  const scopes = enumArray(params, "scopes", SCOPES);
  const kinds = enumArray(params, "kinds", KINDS);
  const sourceTypes = enumArray(params, "sourceTypes", SOURCE_TYPES);
  const materialId = boundedString(params, "materialId", 256);
  const limit = boundedLimit(params.limit);
  if (!query && !materialId && !timeExpression && !fromTime && !toTime && !kinds && !sourceTypes) throw toolError("WORKSPACE_SEARCH_INPUT_INVALID", "query or a bounded filter is required");
  if (!query && retrievalMode && retrievalMode !== "keyword") throw toolError("WORKSPACE_SEARCH_INPUT_INVALID", "semantic retrieval requires a query");
  return compactObject({
    query: query || undefined, retrievalMode: retrievalMode || undefined, timeExpression: timeExpression || undefined,
    fromTime: fromTime || undefined, toTime: toTime || undefined, timeField: timeField || undefined,
    scopes, kinds, sourceTypes, materialId: materialId || undefined, limit,
  });
}

async function callWorkspaceSearch(trusted, request, toolCallId) {
  try {
    // The bridge keeps this out of the model-visible request body and forwards
    // it only as the internal X-Huahuo-Tool-Call-Id transport header.
    return await trusted.bridge.workspaceSearch(request, trusted.traceId || undefined, toolCallId);
  } catch (error) {
    if (error?.code) throw error;
    throw toolError("WORKSPACE_SEARCH_UNAVAILABLE", "Workspace Search Service is unavailable");
  }
}

function sanitizeSearchResponse(payload, request = {}) {
  const source = asRecord(payload);
  assertNoForbiddenKeys(source);
  assertKnownResponseFields(source);
  for (const key of REQUIRED_RESPONSE_KEYS) {
    if (!Object.prototype.hasOwnProperty.call(source, key)) {
      throw toolError("WORKSPACE_SEARCH_RESPONSE_INVALID", `Search Service response is missing ${key}`);
    }
  }
  if (!isSha256Fingerprint(source.queryFingerprint)) throw toolError("WORKSPACE_SEARCH_RESPONSE_INVALID", "Search Service returned an invalid query fingerprint");
  const workspaceVersion = requiredNonNegativeInteger(source.workspaceVersion, "workspaceVersion");
  const indexVersion = requiredNonNegativeInteger(source.indexVersion, "indexVersion");
  const indexStatus = requiredIndexStatus(source.indexStatus);
  if (indexStatus === "current" && indexVersion !== workspaceVersion) {
    throw toolError("WORKSPACE_SEARCH_INDEX_STALE", "Workspace Search index is not current");
  }
  if (!Array.isArray(source.results) || source.results.length > HARD_LIMIT || source.results.length > boundedLimit(request.limit)) {
    throw toolError("WORKSPACE_SEARCH_RESPONSE_INVALID", "Search Service returned an invalid result list");
  }
  return compactObject({
    queryFingerprint: source.queryFingerprint,
    workspaceVersion,
    indexVersion,
    indexStatus,
    results: source.results.map(sanitizeResult),
  });
}

function sanitizeResult(value) {
  const result = asRecord(value);
  assertNoForbiddenKeys(result);
  for (const key of Object.keys(result)) {
    if (!ALLOWED_RESULT_KEYS.has(key)) throw toolError("WORKSPACE_SEARCH_RESPONSE_INVALID", "Search Service returned an unknown result field");
  }
  const resultPath = readString(result, "path");
  if (!isSafeRelativePath(resultPath)) throw toolError("WORKSPACE_SEARCH_RESPONSE_INVALID", "Search Service returned an unsafe path");
  const sanitized = { path: resultPath };
  for (const key of ALLOWED_RESULT_KEYS) {
    if (key === "path" || result[key] === undefined || result[key] === null) continue;
    if (key === "score") {
      if (!Number.isFinite(result[key])) throw toolError("WORKSPACE_SEARCH_RESPONSE_INVALID", "Search Service returned an invalid score");
      sanitized[key] = result[key];
      continue;
    }
    if (key === "stale") {
      if (typeof result[key] !== "boolean") throw toolError("WORKSPACE_SEARCH_RESPONSE_INVALID", "Search Service returned an invalid stale marker");
      sanitized[key] = result[key];
      continue;
    }
    if (key === "kind" && (!KINDS.has(result[key]))) throw toolError("WORKSPACE_SEARCH_RESPONSE_INVALID", "Search Service returned an invalid kind");
    if (key === "matchMode" && (!MATCH_MODES.has(result[key]))) throw toolError("WORKSPACE_SEARCH_RESPONSE_INVALID", "Search Service returned an invalid match mode");
    if (typeof result[key] !== "string") throw toolError("WORKSPACE_SEARCH_RESPONSE_INVALID", "Search Service returned an invalid result field");
    sanitized[key] = result[key].slice(0, 512);
  }
  return sanitized;
}

function assertKnownResponseFields(source) {
  for (const key of Object.keys(source)) {
    if (!ALLOWED_RESPONSE_KEYS.has(key)) throw toolError("WORKSPACE_SEARCH_RESPONSE_INVALID", "Search Service returned an unknown response field");
  }
}

function assertNoForbiddenKeys(value) {
  if (Array.isArray(value)) { for (const item of value) assertNoForbiddenKeys(item); return; }
  if (!value || typeof value !== "object") return;
  for (const [key, child] of Object.entries(value)) {
    if (FORBIDDEN_RESULT_KEYS.has(key)) throw toolError("WORKSPACE_SEARCH_RESPONSE_INVALID", "Search Service returned forbidden fields");
    assertNoForbiddenKeys(child);
  }
}

function rejectIdentityFields(params) {
  for (const key of ["tenantId", "userId", "workspaceId", "runId", "root", "rootPath", "realPath", "runTicket", "authorization"]) {
    if (Object.prototype.hasOwnProperty.call(params, key)) throw toolError("WORKSPACE_SEARCH_INPUT_INVALID", "Workspace identity must come from Runtime context");
  }
}

function assertKnownInputFields(params) {
  for (const key of Object.keys(params)) {
    if (!INPUT_KEYS.has(key)) throw toolError("WORKSPACE_SEARCH_INPUT_INVALID", "Workspace Search received an unknown field");
  }
}

function boundedString(params, key, maxLength) {
  if (!Object.prototype.hasOwnProperty.call(params, key)) return "";
  if (typeof params[key] !== "string") throw toolError("WORKSPACE_SEARCH_INPUT_INVALID", `Workspace Search ${key} must be a string`);
  const value = params[key].trim();
  if (value.length > maxLength) throw toolError("WORKSPACE_SEARCH_INPUT_INVALID", `Workspace Search ${key} is too long`);
  return value;
}

function optionalEnum(params, key, allowed) {
  const value = boundedString(params, key, 64);
  if (value && !allowed.has(value)) throw toolError("WORKSPACE_SEARCH_INPUT_INVALID", `Workspace Search ${key} is invalid`);
  return value;
}

function enumArray(params, key, allowed) {
  if (!Object.prototype.hasOwnProperty.call(params, key)) return undefined;
  const values = params[key];
  if (!Array.isArray(values) || values.length > HARD_LIMIT) throw toolError("WORKSPACE_SEARCH_INPUT_INVALID", `Workspace Search ${key} is invalid`);
  const seen = new Set();
  for (const value of values) {
    if (typeof value !== "string" || !allowed.has(value) || seen.has(value)) throw toolError("WORKSPACE_SEARCH_INPUT_INVALID", `Workspace Search ${key} is invalid`);
    seen.add(value);
  }
  return [...seen].sort();
}

function boundedLimit(value) {
  if (value === undefined) return DEFAULT_LIMIT;
  if (!Number.isSafeInteger(value) || value < 1 || value > HARD_LIMIT) throw toolError("WORKSPACE_SEARCH_INPUT_INVALID", "Workspace Search limit is invalid");
  return value;
}

function isSafeRelativePath(value) { return Boolean(value) && !value.startsWith("/") && !value.startsWith("\\") && !value.includes("\\") && !value.includes("://") && !value.split("/").includes(".."); }
function isSha256Fingerprint(value) { return typeof value === "string" && /^sha256:[a-f0-9]{64}$/.test(value); }
function requireTrustedToolCallId(value) {
  if (!isTrustedToolCallId(value)) throw toolError("WORKSPACE_SEARCH_FORBIDDEN", "trusted Runtime tool call context is unavailable");
  return value;
}
function createProbeToolCallId() { return `probe:${randomUUID()}`; }
function isTrustedToolCallId(value) { return typeof value === "string" && TOOL_CALL_ID_PATTERN.test(value); }
function toolError(code, message) { const error = new Error(message); error.code = code; return error; }
function asRecord(value) { return value && typeof value === "object" && !Array.isArray(value) ? value : {}; }
function readString(value, key) { return typeof value[key] === "string" ? value[key].trim() : ""; }
function firstString(...values) { for (const value of values) if (typeof value === "string" && value.trim()) return value.trim(); return ""; }
function requiredNonNegativeInteger(value, name) {
  if (!Number.isSafeInteger(value) || value < 0) throw toolError("WORKSPACE_SEARCH_RESPONSE_INVALID", `Search Service returned an invalid ${name}`);
  return value;
}
function requiredIndexStatus(value) {
  if (!INDEX_STATUSES.has(value)) throw toolError("WORKSPACE_SEARCH_RESPONSE_INVALID", "Search Service returned an invalid index status");
  return value;
}
function compactObject(value) { return Object.fromEntries(Object.entries(value).filter(([, item]) => item !== undefined && item !== "")); }
