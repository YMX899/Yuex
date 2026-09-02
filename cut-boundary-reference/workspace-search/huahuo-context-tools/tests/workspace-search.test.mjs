import assert from "node:assert/strict";
import { createWorkspaceSearchTool, probeWorkspaceSearch, workspaceSearchParameters } from "../src/workspace-search.js";

const PRIVATE_RUN_CONTEXT_BRIDGE_SYMBOL = Symbol.for("huahuo.private-run-context.v1");
const context = {
  // Model-visible values are never an authorization or Run-identity source.
  runId: "model-visible-run-id-must-be-ignored",
  runTicket: "model-visible-ticket-must-be-ignored",
  runtime: { runId: "model-visible-runtime-run-id-must-be-ignored", runTicket: "model-visible-runtime-ticket-must-be-ignored" },
  traceId: "trace_search",
};
let captured;
let bridgeInvocationCount = 0;
const validResponse = () => ({
  queryFingerprint: "sha256:af9b7e9c3a03cd121f19fa3d4599d32e1b0a90fae253161a987cd28b9d7d202e", workspaceVersion: 4, indexVersion: 4, indexStatus: "current",
  results: [{ path: "materials/processed/mat_recording_123.minutes.md" }],
});
let responseFactory = validResponse;

assert.deepEqual(Object.keys(workspaceSearchParameters.properties).sort(), [
  "fromTime", "kinds", "limit", "materialId", "query", "retrievalMode", "scopes", "sourceTypes",
  "timeExpression", "timeField", "toTime",
].sort());
assert.equal(workspaceSearchParameters.properties.limit.minimum, 1);
assert.equal(workspaceSearchParameters.properties.limit.maximum, 20);

// A visible Ticket never becomes a fallback authorization channel.
const missingPrivateContextTool = createWorkspaceSearchTool(context, { backendBaseUrl: "https://backend.invalid", fetchImpl: () => { throw new Error("must not fetch directly"); } });
await assert.rejects(() => missingPrivateContextTool.execute("call_no_private_context", { query: "x" }), (error) => error.code === "WORKSPACE_SEARCH_FORBIDDEN");

Object.defineProperty(globalThis, PRIVATE_RUN_CONTEXT_BRIDGE_SYMBOL, {
  value: Object.freeze({
    workspaceSearch: async (request, traceId, toolCallId) => {
      bridgeInvocationCount += 1;
      captured = { request, traceId, toolCallId };
      return responseFactory(request);
    },
  }),
  configurable: false,
  enumerable: false,
  writable: false,
});

const tool = createWorkspaceSearchTool(context, { backendBaseUrl: "https://backend.invalid", fetchImpl: () => { throw new Error("must not fetch directly"); } });
const result = await tool.execute("call_1", {
  query: "pricing meeting", retrievalMode: "hybrid", timeExpression: "last week", timeField: "content_time",
  scopes: ["materials"], kinds: ["minutes"], sourceTypes: ["recording"], materialId: "mat_recording_123", limit: 5,
});
assert.deepEqual(captured, {
  request: {
    query: "pricing meeting", retrievalMode: "hybrid", timeExpression: "last week", timeField: "content_time",
    scopes: ["materials"], kinds: ["minutes"], sourceTypes: ["recording"], materialId: "mat_recording_123", limit: 5,
  },
  traceId: "trace_search",
  toolCallId: "call_1",
});
assert.equal(result.details.results[0].path, "materials/processed/mat_recording_123.minutes.md");
for (const forbidden of ["content", "text", "snippet", "summary", "evidence", "embedding", "embeddingModel", "embeddingVersion", "workspaceId", "rootPath", "nextCursor", "runTicket", "toolCallId", "call_1", "model-visible-run-id-must-be-ignored", "model-visible-ticket-must-be-ignored"]) {
  assert.equal(JSON.stringify(result.details).includes(`\"${forbidden}\"`), false);
}

const bridgeCallsBeforeInvalidToolCallIds = bridgeInvocationCount;
for (const invalidToolCallId of [undefined, null, "", " ", "tool/call", "tool\ncall", "x".repeat(257), {}, []]) {
  await assert.rejects(() => tool.execute(invalidToolCallId, { query: "x" }), (error) => error.code === "WORKSPACE_SEARCH_FORBIDDEN");
}
assert.equal(bridgeInvocationCount, bridgeCallsBeforeInvalidToolCallIds);

const visibleRunIdDecoyTool = createWorkspaceSearchTool({ ...context, runId: "run_forged", runtime: { runId: "run_forged_runtime" } });
await visibleRunIdDecoyTool.execute("call_visible_run_decoy", { query: "x" });
assert.equal(captured.toolCallId, "call_visible_run_decoy");

await assert.rejects(() => tool.execute("call_forged", { query: "x", workspaceId: "forged" }), (error) => error.code === "WORKSPACE_SEARCH_INPUT_INVALID");
for (const legacyInput of [
  { query: "x", mode: "keyword" },
  { query: "x", filters: { kinds: ["minutes"] } },
  { query: "x", cursor: "opaque" },
  { query: "x", retrievalMode: "exact" },
  { query: "x", timeField: "contentTime" },
  { query: "x", limit: 21 },
]) {
  await assert.rejects(() => tool.execute("call_legacy", legacyInput), (error) => error.code === "WORKSPACE_SEARCH_INPUT_INVALID");
}

await tool.execute("call_default_limit", { query: "default limit" });
assert.equal(captured.request.limit, 10);

responseFactory = () => ({ ...validResponse(), results: [{ path: "../../secret", kind: "file" }] });
await assert.rejects(() => tool.execute("call_unsafe", { query: "x" }), (error) => error.code === "WORKSPACE_SEARCH_RESPONSE_INVALID");
responseFactory = () => ({ ...validResponse(), results: [{ path: "profile/a.md", kind: "file", snippet: "leak" }] });
await assert.rejects(() => tool.execute("call_leak", { query: "x" }), (error) => error.code === "WORKSPACE_SEARCH_RESPONSE_INVALID");
responseFactory = () => ({ ...validResponse(), runId: "run_leaked", results: [] });
await assert.rejects(() => tool.execute("call_identity_leak", { query: "x" }), (error) => error.code === "WORKSPACE_SEARCH_RESPONSE_INVALID");
responseFactory = () => ({ ...validResponse(), results: [], nextCursor: "old-contract" });
await assert.rejects(() => tool.execute("call_paginated", { query: "x" }), (error) => error.code === "WORKSPACE_SEARCH_RESPONSE_INVALID");
responseFactory = () => ({ results: [] });
assert.deepEqual(await probeWorkspaceSearch(context), { ready: false, code: "WORKSPACE_SEARCH_RESPONSE_INVALID" });
assert.match(captured.toolCallId, /^probe:[A-Za-z0-9._:-]{1,256}$/);
responseFactory = () => ({
  ...validResponse(), indexVersion: 3, indexStatus: "stale",
  results: [{ path: "notes/note_1.raw.md", kind: "note", matchMode: "hybrid", stale: true }],
});
const staleNote = await tool.execute("call_stale_note", { query: "x", retrievalMode: "hybrid" });
assert.deepEqual(staleNote.details, {
  queryFingerprint: "sha256:af9b7e9c3a03cd121f19fa3d4599d32e1b0a90fae253161a987cd28b9d7d202e",
  workspaceVersion: 4,
  indexVersion: 3,
  indexStatus: "stale",
  results: [{ path: "notes/note_1.raw.md", kind: "note", matchMode: "hybrid", stale: true }],
});
responseFactory = () => ({ ...validResponse(), indexVersion: 3 });
await assert.rejects(() => tool.execute("call_version_mismatch", { query: "x" }), (error) => error.code === "WORKSPACE_SEARCH_INDEX_STALE");
responseFactory = () => ({ ...validResponse(), embeddingModel: "private-provider-deployment", embeddingVersion: "private:1536:v1" });
const legacyProviderIdentity = await tool.execute("call_legacy_provider_identity", { query: "x", retrievalMode: "hybrid" });
assert.equal(Object.hasOwn(legacyProviderIdentity.details, "embeddingModel"), false);
assert.equal(Object.hasOwn(legacyProviderIdentity.details, "embeddingVersion"), false);
assert.equal(JSON.stringify(legacyProviderIdentity.details).includes("private-provider"), false);
responseFactory = () => ({ ...validResponse(), unexpected: "old-contract" });
await assert.rejects(() => tool.execute("call_unknown_response_field", { query: "x" }), (error) => error.code === "WORKSPACE_SEARCH_RESPONSE_INVALID");
responseFactory = validResponse;
const metadataOnly = await tool.execute("call_metadata_only", { materialId: "mat_recording_123", retrievalMode: "keyword" });
assert.equal(metadataOnly.details.embeddingModel, undefined);
responseFactory = () => { throw Object.assign(new Error("timed out"), { code: "WORKSPACE_SEARCH_TIMEOUT" }); };
await assert.rejects(() => tool.execute("call_timeout", { query: "x" }), (error) => error.code === "WORKSPACE_SEARCH_TIMEOUT");

responseFactory = validResponse;
assert.deepEqual(await probeWorkspaceSearch(context), { ready: true, code: "ready" });
const firstProbeToolCallId = captured.toolCallId;
assert.match(firstProbeToolCallId, /^probe:[A-Za-z0-9._:-]{1,256}$/);
responseFactory = () => { throw Object.assign(new Error("vector unavailable"), { code: "WORKSPACE_VECTOR_SEARCH_UNAVAILABLE" }); };
assert.deepEqual(await probeWorkspaceSearch(context), { ready: false, code: "WORKSPACE_VECTOR_SEARCH_UNAVAILABLE" });
assert.match(captured.toolCallId, /^probe:[A-Za-z0-9._:-]{1,256}$/);
assert.notEqual(captured.toolCallId, firstProbeToolCallId);
