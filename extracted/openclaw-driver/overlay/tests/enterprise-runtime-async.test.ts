import assert from "node:assert/strict";
import { createHash, createHmac } from "node:crypto";
import { mkdirSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { EnterpriseRunRegistry, productSessionExecutionRef, productSessionSerializationKey, runtimeToolArgsHash, stableIdentityHash, submitReplayResponse } from "../src/enterprise-run-registry.ts";
import { EnterpriseRunStore } from "../src/enterprise-run-store.ts";
import {
  PRIVATE_RUN_CONTEXT_BRIDGE_SYMBOL,
  PRIVATE_RUN_CONTEXT_VERSION,
  RUNTIME_SUBMIT_BINDING_VERSION,
  runWithPrivateRunContext,
  runtimeSubmitInputMessageHash,
  runtimeSubmitProductSessionHash,
  verifyPrivateExecutionContext,
} from "../src/private-run-context.ts";
import {
  DEFAULT_RUNTIME_TOOL_BUDGET,
  RUNTIME_POLICY_ALGORITHM,
  RUNTIME_POLICY_VERSION,
  RUNTIME_STAGING_WRITE_ROOTS,
  RUNTIME_WORKSPACE_ACCESS_READ,
  RUNTIME_WORKSPACE_ACCESS_WRITE,
  RUNTIME_WORKSPACE_WRITE_LEASE_VERSION,
  assertRuntimeWorkspaceMount,
  assertRuntimeWorkspaceWritePath,
  assertExactRuntimeToolsAllow,
  runtimePolicySignature,
  signRuntimePolicy,
  verifyRuntimePolicy,
  type RuntimePolicyEnvelope,
  type RuntimeToolBudget,
} from "../src/runtime-policy.ts";

const policyConfig = { keyId: "runtime-policy-test-v1", runTicketSecret: "runtime-policy-test-secret-0123456789" };
const now = Math.floor(Date.now() / 1000);
const privateRunTicketSecret = "private-run-ticket-test-secret-0123456789";
const identity = {
  runId: "run_1",
  idempotencyKey: "idem_1",
  sessionBindingHash: stableIdentityHash("session_1"),
  workspaceManifestHash: stableIdentityHash("manifest_1"),
};
const workspaceId = "workspace_1";

function acceptance(runId: string) {
  return {
    jtiHash: stableIdentityHash(`jti:${runId}`),
    dispatchIdentity: stableIdentityHash(`dispatch:${runId}`),
  };
}

function sha256Text(value: string): string {
  return `sha256:${createHash("sha256").update(value, "utf8").digest("hex")}`;
}

const privateSubmitInput = "  \u82b1\u706b exact input\nincluding trailing space ";
const privateRuntimeConfigId = "runtime.config.private";
const privateRuntimeConfigVersion = "2026.07.19";
const privateProductSessionThreadId = "thread_private_context";
const privateProductSessionKey = "oc:session:private_context";

function privateSubmitBindingClaim(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    version: RUNTIME_SUBMIT_BINDING_VERSION,
    inputMessageHash: runtimeSubmitInputMessageHash(privateSubmitInput),
    runtimeConfigId: privateRuntimeConfigId,
    runtimeConfigVersion: privateRuntimeConfigVersion,
    productSessionHash: runtimeSubmitProductSessionHash(privateProductSessionThreadId, privateProductSessionKey),
    ...overrides,
  };
}

function signPrivateRunTicket(
  claims: Record<string, unknown>,
  secret = privateRunTicketSecret,
  headerValue: Record<string, unknown> = { alg: "HS256", typ: "HUAHUO-RUN-TICKET" },
): string {
  const header = Buffer.from(JSON.stringify(headerValue)).toString("base64url");
  const payload = Buffer.from(JSON.stringify(claims)).toString("base64url");
  const signed = `${header}.${payload}`;
  return `${signed}.${createHmac("sha256", secret).update(signed, "utf8").digest("base64url")}`;
}

function privateRunTicketFixture(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    runId: "run_private_context",
    tenantId: "tenant_private",
    reservationId: "reservation_private",
    runtimeHostId: "host_private",
    capabilityHash: "capability_private",
    workspaceId: "workspace_private",
    workspaceVersion: 3,
    contextGeneration: 5,
    inputManifestHash: `sha256:${"a".repeat(64)}`,
    planHash: `sha256:${"b".repeat(64)}`,
    submitBinding: privateSubmitBindingClaim(),
    fencingToken: 7,
    jti: "jti_private_context",
    iat: now - 1,
    exp: now + 300,
    ...overrides,
  };
}

function privateRunTicketBinding(claims: Record<string, unknown>) {
  return {
    runId: String(claims.runId),
    tenantId: String(claims.tenantId),
    workspaceId: String(claims.workspaceId),
    runtimeHostId: String(claims.runtimeHostId),
    reservationId: String(claims.reservationId),
    fencingToken: Number(claims.fencingToken),
    capabilityHash: String(claims.capabilityHash),
    workspaceManifestHash: String(claims.inputManifestHash),
    runTicketJtiHash: sha256Text(String(claims.jti)),
    planHash: String(claims.planHash),
    workspaceSearchAllowed: true,
    submitBinding: {
      inputMessage: privateSubmitInput,
      runtimeConfigId: privateRuntimeConfigId,
      runtimeConfigVersion: privateRuntimeConfigVersion,
      productSessionThreadId: privateProductSessionThreadId,
      productSessionKey: privateProductSessionKey,
    },
  };
}

function policyFor(
  runIdentity: typeof identity,
  proof = acceptance(runIdentity.runId),
  overrides: Partial<{
    toolBudget: RuntimeToolBudget;
    requiredTools: string[];
    allowedTools: string[];
    expiresAt: number;
    signature: string;
    capabilityHash: string;
  }> = {},
) {
  const unsigned: Omit<RuntimePolicyEnvelope, "signature"> = {
    version: RUNTIME_POLICY_VERSION,
    algorithm: RUNTIME_POLICY_ALGORITHM,
    keyId: policyConfig.keyId,
    runId: runIdentity.runId,
    idempotencyKey: runIdentity.idempotencyKey,
    workspaceManifestHash: runIdentity.workspaceManifestHash,
    dispatchIdentity: proof.dispatchIdentity,
    capabilityHash: overrides.capabilityHash ?? "capability-test-v1",
    planHash: stableIdentityHash(`plan:${runIdentity.runId}`),
    issuedAt: now - 1,
    expiresAt: overrides.expiresAt ?? now + 300,
    workspaceAccessMode: RUNTIME_WORKSPACE_ACCESS_READ,
    writeLease: null,
    requiredTools: overrides.requiredTools ?? ["read"],
    allowedTools: overrides.allowedTools ?? overrides.requiredTools ?? ["read"],
    toolBudget: overrides.toolBudget ?? { ...DEFAULT_RUNTIME_TOOL_BUDGET },
  };
  const signed = signRuntimePolicy(unsigned, policyConfig.runTicketSecret);
  const envelope = overrides.signature ? { ...signed, signature: overrides.signature } : signed;
  return verifyRuntimePolicy(envelope, {
    runId: runIdentity.runId,
    idempotencyKey: runIdentity.idempotencyKey,
    workspaceId,
    workspaceManifestHash: runIdentity.workspaceManifestHash,
    dispatchIdentity: proof.dispatchIdentity,
    capabilityHash: unsigned.capabilityHash,
  }, policyConfig, now);
}

function register(
  registry: EnterpriseRunRegistry,
  runIdentity = identity,
  proof = acceptance(runIdentity.runId),
  overrides: Parameters<typeof policyFor>[2] = {},
) {
  return registry.register(runIdentity, proof, policyFor(runIdentity, proof, overrides), readWorkspaceGuard(runIdentity));
}

function readWorkspaceGuard(runIdentity = identity) {
  return {
    workspaceId,
    mount: {
      realPath: `/var/lib/huahuo/runtime-workspaces/${runIdentity.runId}`,
      accessMode: RUNTIME_WORKSPACE_ACCESS_READ,
    },
  } as const;
}

function executeTool(
  registry: EnterpriseRunRegistry,
  runId: string,
  toolCallId: string,
  toolName: string,
  argsHash: string,
  resultHash: string,
  resultBytes?: number,
) {
  const args = { fixtureArgs: argsHash };
  registry.assertToolCallAllowed(runId, { toolName, toolCallId, args });
  registry.recordToolOutcome(runId, { toolName, toolCallId, argsHash: runtimeToolArgsHash(args), resultHash, ...(resultBytes === undefined ? {} : { resultBytes }) });
}

function assertHashOnlyToolReceipt(event: { eventType: string; data?: Record<string, unknown> }, expected: "started" | "succeeded" | "aborted" | "rejected") {
  const data = event.data ?? {};
  const allowed = ["schemaVersion", "toolName", "toolCallHash", "argsHash", "outcome", "durationMs", "bytes", "call", "repeat", "resultFingerprint", "errorCode"];
  assert.deepEqual(Object.keys(data).sort(), Object.keys(data).filter((key) => allowed.includes(key)).sort());
  assert.equal(data.schemaVersion, "huahuo.runtime-tool-execution-event.v1");
  assert.equal(data.outcome, expected);
  assert.match(String(data.toolCallHash), /^sha256:[a-f0-9]{64}$/);
  assert.match(String(data.argsHash), /^sha256:[a-f0-9]{64}$/);
  assert.equal(Number.isSafeInteger(data.call), true);
  assert.equal(Number.isSafeInteger(data.repeat), true);
  if (expected === "started" || expected === "rejected") {
    assert.equal(data.durationMs, 0);
    assert.equal(data.bytes, 0);
  } else {
    assert.match(String(data.resultFingerprint), /^sha256:[a-f0-9]{64}$/);
  }
}

async function main() {
  const stableProductSession = { threadId: "thread_session_identity", openclawSessionKey: "oc:ps:session_identity" };
  const sessionKeyWithFirstMetadata = productSessionSerializationKey({
    ...stableProductSession,
    metadata: { promptKey: "first_task", agentKey: "agent_a", outputContract: { format: "markdown" } },
  });
  const sessionKeyWithChangedMetadata = productSessionSerializationKey({
    ...stableProductSession,
    metadata: { promptKey: "second_task", agentKey: "agent_b", workspaceRef: "workspace:other", outputContract: { format: "json" } },
  });
  assert.equal(sessionKeyWithFirstMetadata, sessionKeyWithChangedMetadata);
  assert.notEqual(sessionKeyWithFirstMetadata, productSessionSerializationKey({ ...stableProductSession, threadId: "other_thread" }));
  assert.deepEqual(productSessionExecutionRef({ ...stableProductSession, metadata: { promptKey: "must_not_forward" } }), stableProductSession);
  assert.throws(
    () => productSessionSerializationKey({ threadId: stableProductSession.threadId }),
    (error: any) => error.code === "RUNTIME_INPUT_INVALID",
  );

  const privateClaims = privateRunTicketFixture();
  const privateTicket = signPrivateRunTicket(privateClaims);
  const privateBinding = privateRunTicketBinding(privateClaims);
  assert.ok(Buffer.byteLength(privateTicket, "utf8") > 512);
  const privateContext = verifyPrivateExecutionContext(
    { version: PRIVATE_RUN_CONTEXT_VERSION, runTicket: privateTicket },
    privateBinding,
    { HUAHUO_RUNTIME_RUN_TICKET_SECRET: privateRunTicketSecret },
    now,
  );
  assert.equal(privateContext.runId, privateClaims.runId);
  assert.equal(privateContext.workspaceManifestHash, privateClaims.inputManifestHash);
  assert.equal(privateContext.planHash, privateClaims.planHash);
  assert.equal(privateContext.runTicket, privateTicket);
  for (const diagnosticCase of [
    { stage: "envelope", raw: { version: PRIVATE_RUN_CONTEXT_VERSION, runTicket: privateTicket, unexpected: true }, binding: privateBinding, environment: { HUAHUO_RUNTIME_RUN_TICKET_SECRET: privateRunTicketSecret } },
    { stage: "secret", raw: { version: PRIVATE_RUN_CONTEXT_VERSION, runTicket: privateTicket }, binding: privateBinding, environment: { HUAHUO_RUNTIME_RUN_TICKET_SECRET: "short" } },
    { stage: "ticket", raw: { version: PRIVATE_RUN_CONTEXT_VERSION, runTicket: `${privateTicket}A` }, binding: privateBinding, environment: { HUAHUO_RUNTIME_RUN_TICKET_SECRET: privateRunTicketSecret } },
    { stage: "binding", raw: { version: PRIVATE_RUN_CONTEXT_VERSION, runTicket: privateTicket }, binding: { ...privateBinding, tenantId: "tenant_other" }, environment: { HUAHUO_RUNTIME_RUN_TICKET_SECRET: privateRunTicketSecret } },
  ]) {
    assert.throws(
      () => verifyPrivateExecutionContext(diagnosticCase.raw, diagnosticCase.binding, diagnosticCase.environment, now),
      (error: any) => error.code === "RUNTIME_PERMISSION_DENIED" && error.message === `private run context is invalid at ${diagnosticCase.stage}`,
    );
  }
  const submitBindingGoldenInput = "  line one\r\nline two  ";
  assert.equal(
    runtimeSubmitInputMessageHash(submitBindingGoldenInput),
    "sha256:45fdd18f6594fbd395c24a9fa5af276709f5d3789ccf96fb38e04ccaea829a4d",
  );
  assert.notEqual(runtimeSubmitInputMessageHash(submitBindingGoldenInput), runtimeSubmitInputMessageHash(submitBindingGoldenInput.trim()));
  assert.notEqual(runtimeSubmitInputMessageHash(submitBindingGoldenInput), runtimeSubmitInputMessageHash(submitBindingGoldenInput.replace("\r\n", "\n")));
  assert.equal(
    runtimeSubmitInputMessageHash(privateSubmitInput),
    `sha256:${createHash("sha256").update("huahuo.runtime.submit.input.v1\x00", "utf8").update(privateSubmitInput, "utf8").digest("hex")}`,
  );
  assert.equal(
    runtimeSubmitProductSessionHash(privateProductSessionThreadId, privateProductSessionKey),
    `sha256:${createHash("sha256").update("huahuo.runtime.submit.product_session.v1\x00", "utf8").update(privateProductSessionThreadId, "utf8").update("\x00", "utf8").update(privateProductSessionKey, "utf8").digest("hex")}`,
  );
  assert.notEqual(
    runtimeSubmitProductSessionHash(privateProductSessionThreadId, privateProductSessionKey),
    runtimeSubmitProductSessionHash(`${privateProductSessionThreadId}_other`, privateProductSessionKey),
  );

  for (const invalidTicket of [
    `${privateTicket}A`,
    signPrivateRunTicket(privateClaims, `${privateRunTicketSecret}-wrong`),
    signPrivateRunTicket(privateClaims, privateRunTicketSecret, { alg: "HS256", typ: "HUAHUO-RUN-TICKET", kid: "unexpected" }),
    signPrivateRunTicket(privateClaims, privateRunTicketSecret, { alg: "HS512", typ: "HUAHUO-RUN-TICKET" }),
    ` ${privateTicket}`,
  ]) {
    assert.throws(
      () => verifyPrivateExecutionContext(
        { version: PRIVATE_RUN_CONTEXT_VERSION, runTicket: invalidTicket },
        privateBinding,
        { HUAHUO_RUNTIME_RUN_TICKET_SECRET: privateRunTicketSecret },
        now,
      ),
      (error: any) => error.code === "RUNTIME_PERMISSION_DENIED",
    );
  }

  for (const mutateBinding of [
    (value: typeof privateBinding) => ({ ...value, tenantId: "tenant_other" }),
    (value: typeof privateBinding) => ({ ...value, workspaceId: "workspace_other" }),
    (value: typeof privateBinding) => ({ ...value, runtimeHostId: "host_other" }),
    (value: typeof privateBinding) => ({ ...value, reservationId: "reservation_other" }),
    (value: typeof privateBinding) => ({ ...value, fencingToken: value.fencingToken + 1 }),
    (value: typeof privateBinding) => ({ ...value, capabilityHash: "capability_other" }),
    (value: typeof privateBinding) => ({ ...value, workspaceManifestHash: `sha256:${"c".repeat(64)}` }),
    (value: typeof privateBinding) => ({ ...value, runTicketJtiHash: `sha256:${"d".repeat(64)}` }),
    (value: typeof privateBinding) => ({ ...value, planHash: `sha256:${"e".repeat(64)}` }),
    (value: typeof privateBinding) => ({ ...value, submitBinding: { ...value.submitBinding, inputMessage: `${privateSubmitInput}\n` } }),
    (value: typeof privateBinding) => ({ ...value, submitBinding: { ...value.submitBinding, runtimeConfigId: "runtime.config.changed" } }),
    (value: typeof privateBinding) => ({ ...value, submitBinding: { ...value.submitBinding, runtimeConfigVersion: "2026.07.20" } }),
    (value: typeof privateBinding) => ({ ...value, submitBinding: { ...value.submitBinding, productSessionThreadId: `${privateProductSessionThreadId}_other` } }),
    (value: typeof privateBinding) => ({ ...value, submitBinding: { ...value.submitBinding, productSessionKey: `${privateProductSessionKey}:other` } }),
  ]) {
    assert.throws(
      () => verifyPrivateExecutionContext(
        { version: PRIVATE_RUN_CONTEXT_VERSION, runTicket: privateTicket },
        mutateBinding(privateBinding),
        { HUAHUO_RUNTIME_RUN_TICKET_SECRET: privateRunTicketSecret },
        now,
      ),
      (error: any) => error.code === "RUNTIME_PERMISSION_DENIED",
    );
  }
  for (const invalidClaims of [
    privateRunTicketFixture({ tenantId: "" }),
    privateRunTicketFixture({ tenantId: " tenant_private" }),
    privateRunTicketFixture({ inputManifestHash: "manifest-not-a-hash" }),
    privateRunTicketFixture({ inputManifestHash: `sha256:${"A".repeat(64)}` }),
    privateRunTicketFixture({ planHash: "plan-not-a-hash" }),
    privateRunTicketFixture({ planHash: `sha256:${"B".repeat(64)}` }),
    privateRunTicketFixture({ jti: "" }),
    privateRunTicketFixture({ jti: "jti_private_context " }),
    privateRunTicketFixture({ exp: now }),
    privateRunTicketFixture({ iat: now + 61 }),
    privateRunTicketFixture({ iat: now + 1, exp: now + 1 }),
    privateRunTicketFixture({ exp: now + 901 }),
    privateRunTicketFixture({ submitBinding: null }),
    privateRunTicketFixture({ submitBinding: privateSubmitBindingClaim({ version: "runtime_submit_binding.v0" }) }),
    privateRunTicketFixture({ submitBinding: privateSubmitBindingClaim({ inputMessageHash: "not-a-sha256" }) }),
    privateRunTicketFixture({ submitBinding: privateSubmitBindingClaim({ runtimeConfigId: "runtime config invalid" }) }),
    privateRunTicketFixture({ submitBinding: privateSubmitBindingClaim({ runtimeConfigVersion: "version invalid" }) }),
    privateRunTicketFixture({ submitBinding: privateSubmitBindingClaim({ productSessionHash: "not-a-sha256" }) }),
    privateRunTicketFixture({ submitBinding: privateSubmitBindingClaim({ productSessionHash: undefined }) }),
  ]) {
    assert.throws(
      () => verifyPrivateExecutionContext(
        { version: PRIVATE_RUN_CONTEXT_VERSION, runTicket: signPrivateRunTicket(invalidClaims) },
        privateBinding,
        { HUAHUO_RUNTIME_RUN_TICKET_SECRET: privateRunTicketSecret },
        now,
      ),
      (error: any) => error.code === "RUNTIME_PERMISSION_DENIED",
    );
  }
  for (const changedBinding of [
    privateSubmitBindingClaim({ inputMessageHash: runtimeSubmitInputMessageHash(`${privateSubmitInput}\n`) }),
    privateSubmitBindingClaim({ runtimeConfigId: "runtime.config.changed" }),
    privateSubmitBindingClaim({ runtimeConfigVersion: "2026.07.20" }),
    privateSubmitBindingClaim({ productSessionHash: runtimeSubmitProductSessionHash(`${privateProductSessionThreadId}_other`, privateProductSessionKey) }),
    privateSubmitBindingClaim({ productSessionHash: runtimeSubmitProductSessionHash(privateProductSessionThreadId, `${privateProductSessionKey}:other`) }),
  ]) {
    assert.throws(
      () => verifyPrivateExecutionContext(
        { version: PRIVATE_RUN_CONTEXT_VERSION, runTicket: signPrivateRunTicket(privateRunTicketFixture({ submitBinding: changedBinding })) },
        privateBinding,
        { HUAHUO_RUNTIME_RUN_TICKET_SECRET: privateRunTicketSecret },
        now,
      ),
      (error: any) => error.code === "RUNTIME_PERMISSION_DENIED",
    );
  }
  const emptyVersionTicket = signPrivateRunTicket(privateRunTicketFixture({
    submitBinding: privateSubmitBindingClaim({ runtimeConfigVersion: "" }),
  }));
  assert.throws(
    () => verifyPrivateExecutionContext(
      { version: PRIVATE_RUN_CONTEXT_VERSION, runTicket: emptyVersionTicket },
      { ...privateBinding, submitBinding: { ...privateBinding.submitBinding, runtimeConfigVersion: "" } },
      { HUAHUO_RUNTIME_RUN_TICKET_SECRET: privateRunTicketSecret },
      now,
    ),
    (error: any) => error.code === "RUNTIME_PERMISSION_DENIED",
  );
  const legacyUnboundClaims = privateRunTicketFixture();
  delete legacyUnboundClaims.submitBinding;
  const legacyUnboundTicket = signPrivateRunTicket(legacyUnboundClaims);
  assert.throws(
    () => verifyPrivateExecutionContext(
      { version: PRIVATE_RUN_CONTEXT_VERSION, runTicket: legacyUnboundTicket },
      privateBinding,
      { HUAHUO_RUNTIME_RUN_TICKET_SECRET: privateRunTicketSecret },
      now,
    ),
    (error: any) => error.code === "RUNTIME_PERMISSION_DENIED",
  );
  const { submitBinding: ignoredSubmitBinding, ...genericTicketBinding } = privateBinding;
  assert.doesNotThrow(() => verifyPrivateExecutionContext(
    { version: PRIVATE_RUN_CONTEXT_VERSION, runTicket: legacyUnboundTicket },
    genericTicketBinding,
    { HUAHUO_RUNTIME_RUN_TICKET_SECRET: privateRunTicketSecret },
    now,
  ));
  assert.throws(
    () => verifyPrivateExecutionContext(
      { version: PRIVATE_RUN_CONTEXT_VERSION, runTicket: privateTicket, unexpected: true },
      privateBinding,
      { HUAHUO_RUNTIME_RUN_TICKET_SECRET: privateRunTicketSecret },
      now,
    ),
    (error: any) => error.code === "RUNTIME_PERMISSION_DENIED",
  );

  const privateBridgeDescriptor = Object.getOwnPropertyDescriptor(globalThis, PRIVATE_RUN_CONTEXT_BRIDGE_SYMBOL);
  assert.equal(privateBridgeDescriptor?.configurable, false);
  assert.equal(privateBridgeDescriptor?.enumerable, false);
  assert.equal(privateBridgeDescriptor?.writable, false);
  const privateBridge = privateBridgeDescriptor?.value as { workspaceSearch?: (input: Record<string, unknown>, traceId: string | undefined, toolCallId: string) => Promise<unknown>; get?: () => unknown } | undefined;
  assert.ok(privateBridge && typeof privateBridge.workspaceSearch === "function");
  assert.equal(privateBridge.get, undefined);
  assert.deepEqual(Object.keys(privateBridge), ["workspaceSearch"]);

  const originalFetch = globalThis.fetch;
  const originalProxyURL = process.env.HUAHUO_RUNTIME_WORKSPACE_SEARCH_PROXY_URL;
  const proxyCalls: Array<{ url: string; headers: Record<string, string>; body: unknown }> = [];
  process.env.HUAHUO_RUNTIME_WORKSPACE_SEARCH_PROXY_URL = "http://127.0.0.1:18791";
  globalThis.fetch = async (url, init) => {
    const headers = init?.headers as Record<string, string>;
    proxyCalls.push({ url: String(url), headers, body: JSON.parse(String(init?.body ?? "{}")) });
    await new Promise((resolve) => setTimeout(resolve, headers["x-run-id"] === "run_private_context_second" ? 1 : 5));
    const data = headers["x-trace-id"] === "trace_plugin"
      ? {
          queryFingerprint: `sha256:${"a".repeat(64)}`,
          workspaceVersion: 7,
          indexVersion: 6,
          indexStatus: "stale",
          results: [{ path: `notes/${headers["x-run-id"]}.raw.md`, kind: "note", matchMode: "hybrid", stale: true }],
        }
      : { results: [{ path: `materials/${headers["x-run-id"]}.md` }] };
    return new Response(JSON.stringify({ success: true, data }), { status: 200 });
  };
  try {
    await assert.rejects(
      () => privateBridge.workspaceSearch!({ query: "outside" }, "trace_outside", "tool_call_outside"),
      (error: any) => error.code === "WORKSPACE_SEARCH_FORBIDDEN",
    );
    const scopedResult = await runWithPrivateRunContext(privateContext, () => privateBridge.workspaceSearch!({ query: "inside" }, "trace_private", "tool_call_private"));
    assert.deepEqual(scopedResult, { results: [{ path: "materials/run_private_context.md" }] });
    const noSearchContext = verifyPrivateExecutionContext(
      { version: PRIVATE_RUN_CONTEXT_VERSION, runTicket: privateTicket },
      { ...privateBinding, workspaceSearchAllowed: false },
      { HUAHUO_RUNTIME_RUN_TICKET_SECRET: privateRunTicketSecret },
      now,
    );
    await assert.rejects(
      () => runWithPrivateRunContext(noSearchContext, () => privateBridge.workspaceSearch!({ query: "not-authorized" }, "trace_denied", "tool_call_denied")),
      (error: any) => error.code === "WORKSPACE_SEARCH_FORBIDDEN",
    );
    const secondPrivateContext = Object.freeze({ ...privateContext, runId: "run_private_context_second" });
    const concurrentResults = await Promise.all([
      runWithPrivateRunContext(privateContext, () => privateBridge.workspaceSearch!({ query: "first" }, "trace_first", "tool_call_first")),
      runWithPrivateRunContext(secondPrivateContext, () => privateBridge.workspaceSearch!({ query: "second" }, "trace_second", "tool_call_second")),
    ]);
    assert.deepEqual(concurrentResults, [
      { results: [{ path: "materials/run_private_context.md" }] },
      { results: [{ path: "materials/run_private_context_second.md" }] },
    ]);
    const { createWorkspaceSearchTool } = await import("../../openclaw-extensions/huahuo-context-tools/src/workspace-search.js");
    const actualPluginTool = createWorkspaceSearchTool({
      // The actual Plugin SDK tool context deliberately omits Run ID.
      runId: "model-visible-decoy-must-not-be-read", traceId: "trace_plugin",
      runTicket: "model-visible-ticket-must-not-be-read",
    });
    const actualPluginResult = await runWithPrivateRunContext(
      privateContext,
      () => actualPluginTool.execute("plugin_call", { query: "plugin bridge", retrievalMode: "hybrid" }),
    );
    assert.deepEqual(actualPluginResult.details, {
      queryFingerprint: `sha256:${"a".repeat(64)}`,
      workspaceVersion: 7,
      indexVersion: 6,
      indexStatus: "stale",
      results: [{ path: "notes/run_private_context.raw.md", kind: "note", matchMode: "hybrid", stale: true }],
    });
    assert.equal(JSON.stringify(actualPluginResult.details).includes(privateTicket), false);
    assert.equal(proxyCalls.length, 4);
    const pluginProxyCall = proxyCalls.find((call) => call.headers["x-trace-id"] === "trace_plugin");
    assert.deepEqual(pluginProxyCall?.body, { query: "plugin bridge", retrievalMode: "hybrid", limit: 10 });
    assert.equal(pluginProxyCall?.headers.authorization, `RunTicket ${privateTicket}`);
    assert.equal(pluginProxyCall?.headers["x-run-id"], privateClaims.runId);
    assert.equal(pluginProxyCall?.headers["x-huahuo-tool-call-id"], "plugin_call");
    for (const call of proxyCalls) {
      assert.equal(call.url, "http://127.0.0.1:18791/internal/v1/runtime/workspace-search");
      assert.equal(call.headers.authorization, `RunTicket ${privateTicket}`);
      assert.equal(call.headers["content-type"], "application/json");
      assert.ok(typeof call.headers["x-run-id"] === "string");
      assert.match(call.headers["x-huahuo-tool-call-id"], /^[A-Za-z0-9._:-]{1,256}$/);
      assert.equal(JSON.stringify(call.body).includes(privateTicket), false);
      assert.equal(Object.hasOwn(call.body as Record<string, unknown>, "toolCallId"), false);
    }
    const proxyCallCountBeforeInvalidToolCallID = proxyCalls.length;
    for (const invalidToolCallID of ["", "tool/call", "tool\ncall", "x".repeat(257)]) {
      await assert.rejects(
        () => runWithPrivateRunContext(privateContext, () => privateBridge.workspaceSearch!({ query: "invalid-tool-call" }, "trace_invalid", invalidToolCallID)),
        (error: any) => error.code === "WORKSPACE_SEARCH_FORBIDDEN",
      );
    }
    assert.equal(proxyCalls.length, proxyCallCountBeforeInvalidToolCallID);
    const proxyCallCountBeforeInvalidURL = proxyCalls.length;
    for (const invalidProxyURL of ["https://127.0.0.1:18791", "http://127.0.0.1:18791?unsafe=true", "http://127.0.0.1:18791/not-the-proxy"]) {
      process.env.HUAHUO_RUNTIME_WORKSPACE_SEARCH_PROXY_URL = invalidProxyURL;
      await assert.rejects(
        () => runWithPrivateRunContext(privateContext, () => privateBridge.workspaceSearch!({ query: "invalid-proxy" }, undefined, "tool_call_invalid_proxy")),
        (error: any) => error.code === "WORKSPACE_SEARCH_UNAVAILABLE",
      );
      assert.equal(proxyCalls.length, proxyCallCountBeforeInvalidURL);
    }
    process.env.HUAHUO_RUNTIME_WORKSPACE_SEARCH_PROXY_URL = "http://127.0.0.1:18791";
  } finally {
    globalThis.fetch = originalFetch;
    if (originalProxyURL === undefined) delete process.env.HUAHUO_RUNTIME_WORKSPACE_SEARCH_PROXY_URL;
    else process.env.HUAHUO_RUNTIME_WORKSPACE_SEARCH_PROXY_URL = originalProxyURL;
  }

  const canonicalFixtureSignature = runtimePolicySignature({
    version: RUNTIME_POLICY_VERSION,
    algorithm: RUNTIME_POLICY_ALGORITHM,
    keyId: "runtime-policy-v1",
    runId: "run_policy_fixture",
    idempotencyKey: "run_policy_fixture",
    workspaceManifestHash: "sha256:" + "b".repeat(64),
    dispatchIdentity: "sha256:" + "c".repeat(64),
    capabilityHash: "capability_fixture",
    planHash: "sha256:" + "a".repeat(64),
    issuedAt: 1_700_000_000,
    expiresAt: 1_700_000_300,
    workspaceAccessMode: RUNTIME_WORKSPACE_ACCESS_READ,
    writeLease: null,
    requiredTools: ["read"],
    allowedTools: ["read"],
    toolBudget: { ...DEFAULT_RUNTIME_TOOL_BUDGET },
  }, "0123456789abcdef0123456789abcdef");
  assert.equal(canonicalFixtureSignature, "sha256:e2a3965bf9d3a0e62a03e21fa55d7c18cebb940133b84f63833ef791c2575005");

  const firstAcceptance = acceptance(identity.runId);
  const firstPolicy = policyFor(identity, firstAcceptance);
  assert.equal(firstPolicy.toolBudget.maxToolCalls, 200);
  assert.equal(firstPolicy.toolBudget.softToolCallLimit, 160);
  assert.equal(firstPolicy.toolBudget.finalizationReserve, 10);
  assert.equal(firstPolicy.toolBudget.maxRepeatedCalls, 2);
  assert.equal(firstPolicy.toolBudget.maxNoProgressCalls, 4);
  assert.deepEqual(firstPolicy.requiredTools, ["read"]);
  assert.deepEqual(firstPolicy.allowedTools, ["read"]);
  const noToolPolicy = policyFor({ ...identity, runId: "run_no_tools", idempotencyKey: "idem_no_tools" }, acceptance("run_no_tools"), {
    requiredTools: [], allowedTools: [],
  });
  assert.deepEqual(noToolPolicy.requiredTools, []);
  assert.deepEqual(noToolPolicy.allowedTools, []);
  assert.deepEqual(assertExactRuntimeToolsAllow({ allow: [] }, []), []);
  assert.deepEqual(assertExactRuntimeToolsAllow({ allow: ["read", "workspace_search"] }, ["read", "workspace_search"]), ["read", "workspace_search"]);
  for (const invalidTools of [undefined, null, {}, { allow: null }, { allow: [" read"] }, { allow: [], deny: [] }]) {
    assert.throws(() => assertExactRuntimeToolsAllow(invalidTools, []), (error: any) => error.code === "RUNTIME_INPUT_INVALID");
  }
  assert.throws(
    () => assertExactRuntimeToolsAllow({ allow: ["read"] }, []),
    (error: any) => error.code === "RUNTIME_PERMISSION_DENIED",
  );
  assert.throws(
    () => verifyRuntimePolicy(undefined, {
      runId: identity.runId, idempotencyKey: identity.idempotencyKey, workspaceManifestHash: identity.workspaceManifestHash,
      workspaceId, dispatchIdentity: firstAcceptance.dispatchIdentity, capabilityHash: "capability-test-v1",
    }, policyConfig, now),
    (error: any) => error.code === "RUNTIME_INPUT_INVALID",
  );
  assert.throws(
    () => policyFor(identity, firstAcceptance, { signature: "sha256:" + "0".repeat(64) }),
    (error: any) => error.code === "RUNTIME_PERMISSION_DENIED",
  );
  const signedPolicy = signRuntimePolicy({
    version: RUNTIME_POLICY_VERSION, algorithm: RUNTIME_POLICY_ALGORITHM, keyId: policyConfig.keyId,
    runId: identity.runId, idempotencyKey: identity.idempotencyKey, workspaceManifestHash: identity.workspaceManifestHash,
    dispatchIdentity: firstAcceptance.dispatchIdentity, capabilityHash: "capability-test-v1", planHash: stableIdentityHash("allowed-tools-plan"),
    issuedAt: now - 1, expiresAt: now + 300, workspaceAccessMode: RUNTIME_WORKSPACE_ACCESS_READ, writeLease: null,
    requiredTools: ["read"], allowedTools: ["read"], toolBudget: { ...DEFAULT_RUNTIME_TOOL_BUDGET },
  }, policyConfig.runTicketSecret);
  const { allowedTools: _allowedTools, ...missingAllowedTools } = signedPolicy;
  assert.throws(
    () => verifyRuntimePolicy(missingAllowedTools, {
      runId: identity.runId, idempotencyKey: identity.idempotencyKey, workspaceManifestHash: identity.workspaceManifestHash,
      workspaceId, dispatchIdentity: firstAcceptance.dispatchIdentity, capabilityHash: "capability-test-v1",
    }, policyConfig, now),
    (error: any) => error.code === "RUNTIME_INPUT_INVALID",
  );
  assert.throws(
    () => verifyRuntimePolicy({ ...signedPolicy, requiredTools: undefined } as any, {
      runId: identity.runId, idempotencyKey: identity.idempotencyKey, workspaceManifestHash: identity.workspaceManifestHash,
      workspaceId, dispatchIdentity: firstAcceptance.dispatchIdentity, capabilityHash: "capability-test-v1",
    }, policyConfig, now),
    (error: any) => error.code === "RUNTIME_INPUT_INVALID",
  );
  assert.throws(
    () => verifyRuntimePolicy({ ...signedPolicy, allowedTools: null } as any, {
      runId: identity.runId, idempotencyKey: identity.idempotencyKey, workspaceManifestHash: identity.workspaceManifestHash,
      workspaceId, dispatchIdentity: firstAcceptance.dispatchIdentity, capabilityHash: "capability-test-v1",
    }, policyConfig, now),
    (error: any) => error.code === "RUNTIME_INPUT_INVALID",
  );
  assert.throws(
    () => verifyRuntimePolicy({ ...signedPolicy, allowedTools: ["read", "workspace_search"] }, {
      runId: identity.runId, idempotencyKey: identity.idempotencyKey, workspaceManifestHash: identity.workspaceManifestHash,
      workspaceId, dispatchIdentity: firstAcceptance.dispatchIdentity, capabilityHash: "capability-test-v1",
    }, policyConfig, now),
    (error: any) => error.code === "RUNTIME_INPUT_INVALID",
  );
  assert.throws(
    () => policyFor(identity, firstAcceptance, { requiredTools: ["workspace_material_search"], allowedTools: ["workspace_material_search"] }),
    (error: any) => error.code === "RUNTIME_INPUT_INVALID",
  );
  assert.throws(
    () => policyFor(identity, firstAcceptance, { requiredTools: ["read"], allowedTools: ["read", "workspace_search"] }),
    (error: any) => error.code === "RUNTIME_INPUT_INVALID",
  );
  assert.throws(
    () => policyFor(identity, firstAcceptance, { requiredTools: ["ls"], allowedTools: ["ls"] }),
    (error: any) => error.code === "RUNTIME_INPUT_INVALID",
  );
  assert.throws(
    () => policyFor(identity, firstAcceptance, { capabilityHash: "capability unsafe" }),
    (error: any) => error.code === "RUNTIME_INPUT_INVALID",
  );
  assert.throws(
    () => policyFor({ ...identity, runId: "run<unsafe>", idempotencyKey: "idem_unsafe" }, acceptance("run<unsafe>")),
    (error: any) => error.code === "RUNTIME_INPUT_INVALID",
  );
  const validPolicyForUnsupportedTest = signRuntimePolicy({
    version: RUNTIME_POLICY_VERSION, algorithm: RUNTIME_POLICY_ALGORITHM, keyId: policyConfig.keyId,
    runId: identity.runId, idempotencyKey: identity.idempotencyKey, workspaceManifestHash: identity.workspaceManifestHash,
    dispatchIdentity: firstAcceptance.dispatchIdentity, capabilityHash: "capability-test-v1", planHash: stableIdentityHash("unsupported-plan"),
    issuedAt: now - 1, expiresAt: now + 300, workspaceAccessMode: RUNTIME_WORKSPACE_ACCESS_READ, writeLease: null,
    requiredTools: ["read"], allowedTools: ["read"],
    toolBudget: { ...DEFAULT_RUNTIME_TOOL_BUDGET },
  }, policyConfig.runTicketSecret);
  const unsupportedEnvelope = { ...validPolicyForUnsupportedTest, toolBudget: { ...DEFAULT_RUNTIME_TOOL_BUDGET, maxToolCalls: 401 } };
  assert.throws(
    () => verifyRuntimePolicy(unsupportedEnvelope, {
      runId: identity.runId, idempotencyKey: identity.idempotencyKey, workspaceManifestHash: identity.workspaceManifestHash,
      workspaceId, dispatchIdentity: firstAcceptance.dispatchIdentity, capabilityHash: "capability-test-v1",
    }, policyConfig, now),
    (error: any) => error.code === "RUNTIME_TOOL_BUDGET_UNSUPPORTED",
  );

  // The write lease is not a descriptive field. It is HMAC-bound to this Run,
  // Workspace and manifest, and the Gateway admission mount must be byte-for-
  // byte equivalent to the verified lease before the core Runtime is invoked.
  const stagingExpiry = now + 300;
  const stagingUnsigned: Omit<RuntimePolicyEnvelope, "signature"> = {
    version: RUNTIME_POLICY_VERSION,
    algorithm: RUNTIME_POLICY_ALGORITHM,
    keyId: policyConfig.keyId,
    runId: identity.runId,
    idempotencyKey: identity.idempotencyKey,
    workspaceManifestHash: identity.workspaceManifestHash,
    dispatchIdentity: firstAcceptance.dispatchIdentity,
    capabilityHash: "capability-test-v1",
    planHash: stableIdentityHash("staging-policy-plan"),
    issuedAt: now - 1,
    expiresAt: stagingExpiry,
    workspaceAccessMode: RUNTIME_WORKSPACE_ACCESS_WRITE,
    writeLease: {
      version: RUNTIME_WORKSPACE_WRITE_LEASE_VERSION,
      runId: identity.runId,
      workspaceId,
      workspaceManifestHash: identity.workspaceManifestHash,
      allowedRoots: [...RUNTIME_STAGING_WRITE_ROOTS],
      expiresAt: stagingExpiry,
    },
    requiredTools: ["read", "write"],
    allowedTools: ["read", "write"],
    toolBudget: { ...DEFAULT_RUNTIME_TOOL_BUDGET },
  };
  const stagingPolicy = verifyRuntimePolicy(
    signRuntimePolicy(stagingUnsigned, policyConfig.runTicketSecret),
    {
      runId: identity.runId,
      idempotencyKey: identity.idempotencyKey,
      workspaceId,
      workspaceManifestHash: identity.workspaceManifestHash,
      dispatchIdentity: firstAcceptance.dispatchIdentity,
      capabilityHash: "capability-test-v1",
    },
    policyConfig,
    now,
  );
  const stagingMount = {
    realPath: "/tmp",
    accessMode: RUNTIME_WORKSPACE_ACCESS_WRITE,
    writeLease: stagingUnsigned.writeLease,
  };
  mkdirSync(resolve("/tmp"), { recursive: true });
  assert.deepEqual(assertRuntimeWorkspaceMount(stagingMount, stagingPolicy, workspaceId), stagingMount);
  assert.deepEqual(
    assertRuntimeWorkspaceMount({ realPath: "/var/lib/huahuo/runtime-workspaces/run_1", accessMode: RUNTIME_WORKSPACE_ACCESS_READ }, firstPolicy, workspaceId),
    { realPath: "/var/lib/huahuo/runtime-workspaces/run_1", accessMode: RUNTIME_WORKSPACE_ACCESS_READ },
  );
  assert.equal(assertRuntimeWorkspaceWritePath("output/result.md", stagingMount, stagingPolicy, workspaceId, now), "output/result.md");
  assert.equal(assertRuntimeWorkspaceWritePath("staging/drafts/result.json", stagingMount, stagingPolicy, workspaceId, now), "staging/drafts/result.json");
  for (const invalidPath of [
    undefined, null, "", " ", "output", "staging", "/output/result.md", "output/", "output//result.md",
    "output/./result.md", "output/../materials/result.md", "staging/../output/result.md", "../output/result.md",
    "materials/result.md", "profile/result.md", "output\\result.md", "C:/output/result.md", "output/result:copy.md",
    "output/\u0000result.md",
  ]) {
    assert.throws(
      () => assertRuntimeWorkspaceWritePath(invalidPath, stagingMount, stagingPolicy, workspaceId, now),
      (error: any) => error.code === "RUNTIME_INPUT_INVALID" || error.code === "RUNTIME_PERMISSION_DENIED",
    );
  }
  assert.throws(
    () => assertRuntimeWorkspaceWritePath("output/should-not-write.md", { realPath: stagingMount.realPath, accessMode: RUNTIME_WORKSPACE_ACCESS_READ }, firstPolicy, workspaceId, now),
    (error: any) => error.code === "RUNTIME_PERMISSION_DENIED",
  );
  assert.throws(
    () => assertRuntimeWorkspaceMount(stagingMount, stagingPolicy, workspaceId, stagingExpiry),
    (error: any) => error.code === "RUNTIME_PERMISSION_DENIED",
  );
  assert.throws(
    () => assertRuntimeWorkspaceWritePath("output/expired.md", stagingMount, stagingPolicy, workspaceId, stagingExpiry),
    (error: any) => error.code === "RUNTIME_PERMISSION_DENIED",
  );
  assert.throws(
    () => assertRuntimeWorkspaceWritePath("output/invalid-clock.md", stagingMount, stagingPolicy, workspaceId, Number.NaN),
    (error: any) => error.code === "RUNTIME_PERMISSION_DENIED",
  );
  const mutatedVerifiedLeasePolicy = {
    ...stagingPolicy,
    writeLease: { ...stagingPolicy.writeLease!, workspaceId: "workspace_other" },
  };
  assert.throws(
    () => assertRuntimeWorkspaceWritePath("output/tampered.md", stagingMount, mutatedVerifiedLeasePolicy, workspaceId, now),
    (error: any) => error.code === "RUNTIME_POLICY_INVALID" || error.code === "RUNTIME_PERMISSION_DENIED",
  );
  for (const invalidMount of [
    { ...stagingMount, realPath: "relative/workspace" },
    { ...stagingMount, realPath: "/var/lib/huahuo/../escape" },
    { ...stagingMount, accessMode: RUNTIME_WORKSPACE_ACCESS_READ },
    { realPath: stagingMount.realPath, accessMode: RUNTIME_WORKSPACE_ACCESS_READ, writeLease: null },
    { realPath: stagingMount.realPath, accessMode: RUNTIME_WORKSPACE_ACCESS_READ, writeLease: stagingMount.writeLease },
    { ...stagingMount, writeLease: { ...stagingMount.writeLease, runId: "run_other" } },
    { ...stagingMount, writeLease: { ...stagingMount.writeLease, workspaceId: "workspace_other" } },
    { ...stagingMount, writeLease: { ...stagingMount.writeLease, workspaceManifestHash: `sha256:${"d".repeat(64)}` } },
    { ...stagingMount, writeLease: { ...stagingMount.writeLease, allowedRoots: ["materials", "output"] } },
    { ...stagingMount, writeLease: { ...stagingMount.writeLease, expiresAt: stagingExpiry + 1 } },
    { realPath: stagingMount.realPath, accessMode: stagingMount.accessMode },
    { ...stagingMount, unexpected: true },
  ]) {
    assert.throws(
      () => assertRuntimeWorkspaceMount(invalidMount, stagingPolicy, workspaceId),
      (error: any) => error.code === "RUNTIME_PERMISSION_DENIED" || error.code === "RUNTIME_INPUT_INVALID",
    );
  }
  assert.throws(
    () => verifyRuntimePolicy(
      signRuntimePolicy({ ...stagingUnsigned, writeLease: null }, policyConfig.runTicketSecret),
      {
        runId: identity.runId,
        idempotencyKey: identity.idempotencyKey,
        workspaceId,
        workspaceManifestHash: identity.workspaceManifestHash,
        dispatchIdentity: firstAcceptance.dispatchIdentity,
        capabilityHash: "capability-test-v1",
      },
      policyConfig,
      now,
    ),
    (error: any) => error.code === "RUNTIME_PERMISSION_DENIED",
  );
  assert.throws(
    () => signRuntimePolicy({ ...stagingUnsigned, writeLease: { ...stagingUnsigned.writeLease!, allowedRoots: ["materials", "output"] } }, policyConfig.runTicketSecret),
    (error: any) => error.code === "RUNTIME_PERMISSION_DENIED",
  );
  assert.throws(
    () => verifyRuntimePolicy(
      signRuntimePolicy(stagingUnsigned, policyConfig.runTicketSecret),
      {
        runId: identity.runId,
        idempotencyKey: identity.idempotencyKey,
        workspaceId: "workspace_other",
        workspaceManifestHash: identity.workspaceManifestHash,
        dispatchIdentity: firstAcceptance.dispatchIdentity,
        capabilityHash: "capability-test-v1",
      },
      policyConfig,
      now,
    ),
    (error: any) => error.code === "RUNTIME_PERMISSION_DENIED",
  );

  const registry = new EnterpriseRunRegistry();
  assert.throws(() => (registry as any).register(identity, firstAcceptance), (error: any) => error.code === "RUNTIME_PERMISSION_DENIED");
  assert.deepEqual(registry.register(identity, firstAcceptance, firstPolicy, readWorkspaceGuard()), { created: true, status: "accepted" });
  assert.deepEqual(registry.register(identity, firstAcceptance, firstPolicy, readWorkspaceGuard()), { created: false, status: "accepted" });
  assert.throws(
    () => registry.register({ ...identity, workspaceManifestHash: stableIdentityHash("other") }, firstAcceptance, firstPolicy, readWorkspaceGuard()),
    (error: any) => error.code === "RUNTIME_RUN_CONFLICT",
  );

  const stagingRegistry = new EnterpriseRunRegistry();
  assert.deepEqual(
    stagingRegistry.register(identity, firstAcceptance, stagingPolicy, { workspaceId, mount: stagingMount }),
    { created: true, status: "accepted" },
  );
  const validWriteArgs = { path: "output/result.md", content: "not persisted by the guard" };
  stagingRegistry.assertToolCallAllowed(identity.runId, {
    toolName: "write",
    toolCallId: "write_valid_output",
    args: validWriteArgs,
  });
  stagingRegistry.recordToolOutcome(identity.runId, {
    toolName: "write",
    toolCallId: "write_valid_output",
    argsHash: runtimeToolArgsHash(validWriteArgs),
    resultHash: stableIdentityHash("write_ok"),
    progress: true,
  });
  assert.throws(
    () => stagingRegistry.assertToolCallAllowed(identity.runId, {
      toolName: "write",
      toolCallId: "write_escape",
      args: { path: "../output/escape.md", content: "must-never-be-persisted" },
    }),
    (error: any) => error.code === "RUNTIME_INPUT_INVALID" || error.code === "RUNTIME_PERMISSION_DENIED",
  );
  assert.equal(stagingRegistry.status(identity.runId).status, "aborting");
  assert.doesNotMatch(JSON.stringify(stagingRegistry.events(identity.runId)), /must-never-be-persisted|not persisted by the guard/);

  const toolReceiptRegistry = new EnterpriseRunRegistry();
  const toolReceiptIdentity = { ...identity, runId: "run_tool_receipt", idempotencyKey: "idem_tool_receipt" };
  register(toolReceiptRegistry, toolReceiptIdentity);
  const toolReceiptArgs = { path: "materials/never-persist.md", query: "never-persist" };
  toolReceiptRegistry.assertToolCallAllowed(toolReceiptIdentity.runId, {
    toolName: "read",
    toolCallId: "private_tool_call_id_must_not_escape",
    args: toolReceiptArgs,
  });
  toolReceiptRegistry.recordToolOutcome(toolReceiptIdentity.runId, {
    toolName: "read",
    toolCallId: "private_tool_call_id_must_not_escape",
    argsHash: runtimeToolArgsHash(toolReceiptArgs),
    resultHash: "private_result_must_not_escape",
    resultBytes: 3,
  });
  const toolReceipts = toolReceiptRegistry.events(toolReceiptIdentity.runId, 0, 20).items.filter((event) => event.eventType.startsWith("tool.call."));
  assert.equal(toolReceipts.length, 2);
  assertHashOnlyToolReceipt(toolReceipts[0]!, "started");
  assertHashOnlyToolReceipt(toolReceipts[1]!, "succeeded");
  assert.equal((toolReceipts[0]!.data ?? {}).toolCallHash, (toolReceipts[1]!.data ?? {}).toolCallHash);
  assert.equal((toolReceipts[0]!.data ?? {}).argsHash, (toolReceipts[1]!.data ?? {}).argsHash);
  assert.doesNotMatch(JSON.stringify(toolReceipts), /private_tool_call_id_must_not_escape|never-persist|private_result_must_not_escape/);

  registry.appendEvent("run_1", "workspace.ready", "ready");
  registry.appendEvent("run_1", "run.started", "running");
  const page = registry.events("run_1", 0, 2);
  assert.equal(page.items[0].sequence, 1);
  assert.equal(page.items[1].sequence, 2);
  assert.equal(page.hasMore, true);

  for (let index = 0; index < 160; index += 1) {
    executeTool(registry, "run_1", `read_${index}`, "read", `args_${index}`, `result_${index}`, 1);
  }
  const firstStatus = registry.status("run_1");
  assert.equal(firstStatus.toolCalls, 160);
  assert.equal(firstStatus.readBytes, 160);
  assert.equal((firstStatus.runtimePolicy as any).toolBudget.finalizationReserve, 10);
  assert.equal(registry.events("run_1", 0, 500).items.some((event) => event.eventType === "budget.warning"), true);
  assert.deepEqual((firstStatus.runtimePolicy as any).allowedTools, ["read"]);

  const repeatRegistry = new EnterpriseRunRegistry();
  const repeatIdentity = { ...identity, runId: "run_repeat", idempotencyKey: "idem_repeat" };
  register(repeatRegistry, repeatIdentity, acceptance(repeatIdentity.runId), { requiredTools: ["workspace_search"], allowedTools: ["workspace_search"] });
  executeTool(repeatRegistry, "run_repeat", "repeat_1", "workspace_search", "same", "same");
  executeTool(repeatRegistry, "run_repeat", "repeat_2", "workspace_search", "same", "same");
  executeTool(repeatRegistry, "run_repeat", "repeat_3", "workspace_search", "same", "same");
  assert.equal(repeatRegistry.getAbortSignal("run_repeat").aborted, true);
  assert.equal(repeatRegistry.status("run_repeat").abortCode, "RUNTIME_TOOL_LOOP_DETECTED");

  const noProgressRegistry = new EnterpriseRunRegistry();
  const noProgressIdentity = { ...identity, runId: "run_no_progress", idempotencyKey: "idem_no_progress" };
  register(noProgressRegistry, noProgressIdentity, acceptance(noProgressIdentity.runId), {
    requiredTools: ["workspace_search"], allowedTools: ["workspace_search"],
  });
  for (let index = 1; index <= 4; index += 1) {
    executeTool(noProgressRegistry, "run_no_progress", `no_progress_${index}`, "workspace_search", `different_args_${index}`, "same_result");
  }
  assert.equal(noProgressRegistry.getAbortSignal("run_no_progress").aborted, true);
  assert.equal(noProgressRegistry.status("run_no_progress").noProgressCalls, 4);
  assert.equal(noProgressRegistry.status("run_no_progress").abortCode, "RUNTIME_RUN_STALLED");

  const reserveRegistry = new EnterpriseRunRegistry();
  const reserveIdentity = { ...identity, runId: "run_reserve", idempotencyKey: "idem_reserve" };
  const reserveAcceptance = acceptance(reserveIdentity.runId);
  const reservePolicy = policyFor(reserveIdentity, reserveAcceptance, {
    requiredTools: ["read", "workspace_search"],
    allowedTools: ["read", "workspace_search"],
    toolBudget: { ...DEFAULT_RUNTIME_TOOL_BUDGET, maxToolCalls: 10, softToolCallLimit: 5, finalizationReserve: 2, maxSearchCalls: 10, maxWriteCalls: 10 },
  });
  reserveRegistry.register(reserveIdentity, reserveAcceptance, reservePolicy, readWorkspaceGuard(reserveIdentity));
  for (let index = 0; index < 8; index += 1) {
    executeTool(reserveRegistry, "run_reserve", `reserve_${index}`, "read", `reserve_${index}`, `reserve_result_${index}`);
  }
  assert.throws(
    () => reserveRegistry.assertToolCallAllowed("run_reserve", { toolName: "workspace_search", toolCallId: "reserve_search_rejected", args: {} }),
    (error: any) => error.code === "RUNTIME_TOOL_BUDGET_EXCEEDED",
  );
  assert.equal(reserveRegistry.getAbortSignal("run_reserve").aborted, true);
  assert.equal(reserveRegistry.status("run_reserve").abortCode, "RUNTIME_TOOL_BUDGET_EXCEEDED");
  const reserveRejection = reserveRegistry.events("run_reserve", 0, 100).items.find((event) => event.eventType === "tool.call.rejected");
  assert.ok(reserveRejection);
  assertHashOnlyToolReceipt(reserveRejection, "rejected");

  const hardBudgetRegistry = new EnterpriseRunRegistry();
  const hardBudgetIdentity = { ...identity, runId: "run_hard_budget", idempotencyKey: "idem_hard_budget" };
  register(hardBudgetRegistry, hardBudgetIdentity, acceptance(hardBudgetIdentity.runId), {
    toolBudget: { ...DEFAULT_RUNTIME_TOOL_BUDGET, maxToolCalls: 10, softToolCallLimit: 5, finalizationReserve: 2, maxSearchCalls: 10, maxWriteCalls: 10 },
  });
  for (let index = 0; index < 10; index += 1) {
    executeTool(hardBudgetRegistry, "run_hard_budget", `hard_${index}`, "read", `hard_${index}`, `hard_result_${index}`);
  }
  assert.equal(hardBudgetRegistry.status("run_hard_budget").toolCalls, 10);
  assert.throws(
    () => hardBudgetRegistry.assertToolCallAllowed("run_hard_budget", { toolName: "read", toolCallId: "hard_201", args: {} }),
    (error: any) => error.code === "RUNTIME_TOOL_BUDGET_EXCEEDED",
  );
  assert.equal(hardBudgetRegistry.getAbortSignal("run_hard_budget").aborted, true);

  const deniedToolRegistry = new EnterpriseRunRegistry();
  const deniedToolIdentity = { ...identity, runId: "run_denied_tool", idempotencyKey: "idem_denied_tool" };
  register(deniedToolRegistry, deniedToolIdentity);
  assert.throws(
    () => deniedToolRegistry.assertToolCallAllowed("run_denied_tool", { toolName: "ls", toolCallId: "denied_ls", args: {} }),
    (error: any) => error.code === "RUNTIME_PERMISSION_DENIED",
  );

  const abortRegistry = new EnterpriseRunRegistry();
  const abortIdentity = { ...identity, runId: "run_abort", idempotencyKey: "idem_abort" };
  register(abortRegistry, abortIdentity);
  const abortPromise = abortRegistry.abort("run_abort", "user_cancel", 1000);
  assert.equal(abortRegistry.getAbortSignal("run_abort").aborted, true);
  assert.equal(abortRegistry.status("run_abort").status, "aborting");
  assert.equal(abortRegistry.status("run_abort").abortCode, "RUNTIME_ABORTED");
  assert.throws(
    () => abortRegistry.appendEvent("run_abort", "run.started", "running"),
    (error: any) => error.code === "RUNTIME_ABORTED",
  );
  abortRegistry.complete("run_abort", "timeout", { unsafe: "discarded" }, { code: "RUNTIME_INTERNAL_ERROR" });
  assert.deepEqual(await abortPromise, { runId: "run_abort", status: "aborted" });
  const abortStatus = abortRegistry.status("run_abort");
  assert.equal(abortStatus.status, "aborted");
  assert.deepEqual(abortStatus.error, { code: "RUNTIME_ABORTED" });
  assert.equal(abortStatus.result, undefined);
  assert.equal(abortRegistry.events("run_abort").items.at(-1)?.eventType, "run.aborted");

  const timeoutRegistry = new EnterpriseRunRegistry();
  const timeoutIdentity = { ...identity, runId: "run_normal_timeout", idempotencyKey: "idem_normal_timeout" };
  register(timeoutRegistry, timeoutIdentity);
  timeoutRegistry.complete("run_normal_timeout", "timeout", undefined, { code: "RUNTIME_TIMEOUT" });
  assert.equal(timeoutRegistry.status("run_normal_timeout").status, "timeout");
  assert.deepEqual(timeoutRegistry.status("run_normal_timeout").error, { code: "RUNTIME_TIMEOUT" });

  const serialized: string[] = [];
  await Promise.all([
    abortRegistry.serializeSession("session", async () => { serialized.push("a1"); await new Promise((resolve) => setTimeout(resolve, 10)); serialized.push("a2"); }),
    abortRegistry.serializeSession("session", async () => { serialized.push("b1"); serialized.push("b2"); }),
  ]);
  assert.deepEqual(serialized, ["a1", "a2", "b1", "b2"]);

  const durableRoot = mkdtempSync(join(tmpdir(), "huahuo-enterprise-run-store-"));
  const durablePath = join(durableRoot, "runs.sqlite");
  const durableIdentity = { ...identity, runId: "run_durable", idempotencyKey: "idem_durable" };
  const durableAcceptance = acceptance(durableIdentity.runId);
  const durablePolicy = policyFor(durableIdentity, durableAcceptance, { requiredTools: ["workspace_search"], allowedTools: ["workspace_search"] });
  try {
    const firstStore = new EnterpriseRunStore(durablePath);
    const firstRegistry = new EnterpriseRunRegistry(firstStore);
    assert.equal(firstRegistry.isDurable(), true);
    assert.deepEqual(firstRegistry.register(durableIdentity, durableAcceptance, durablePolicy, readWorkspaceGuard(durableIdentity)), { created: true, status: "accepted" });
    firstRegistry.appendEvent("run_durable", "run.started", "running");
    executeTool(firstRegistry, "run_durable", "durable_workspace_search", "workspace_search", "sha256:args", "sha256:result");
    assert.equal(firstRegistry.listNonTerminal().some((run) => run.runId === "run_durable"), true);
    firstStore.close();

    const restartedStore = new EnterpriseRunStore(durablePath);
    const restartedRegistry = new EnterpriseRunRegistry(restartedStore);
    const durableStatus = restartedRegistry.status("run_durable");
    assert.equal(durableStatus.status, "orphaned");
    assert.equal(durableStatus.toolCalls, 1);
    assert.equal((durableStatus.runtimePolicy as any).policyHash, durablePolicy.policyHash);
    assert.equal((durableStatus.recovery as any).state, "orphaned");
    assert.equal((durableStatus.error as any).code, "RUNTIME_RUN_ORPHANED");
    assert.equal(restartedRegistry.getAbortSignal("run_durable").aborted, true);
    assert.equal(restartedRegistry.listNonTerminal().some((run) => run.runId === "run_durable"), false);
    const restartEvents = restartedRegistry.events("run_durable", 0, 100).items;
    assert.deepEqual(restartEvents.slice(-2).map((event) => [event.eventType, event.status]), [
      ["run.recovery.started", "recovering"],
      ["run.orphaned", "orphaned"],
    ]);
    assert.equal(restartedRegistry.events("run_durable", 0, 100).terminalSequence, restartEvents.at(-1)?.sequence);
    assert.deepEqual(restartedRegistry.register(durableIdentity, durableAcceptance, durablePolicy, readWorkspaceGuard(durableIdentity)), { created: false, status: "orphaned" });
    assert.deepEqual(submitReplayResponse({ created: false, status: "orphaned" }), {
      status: "orphaned",
      recovery: { state: "orphaned", retry: "new_attempt", code: "RUNTIME_RUN_ORPHANED" },
    });
    assert.deepEqual(submitReplayResponse({ created: false, status: "running" }), { status: "accepted" });
    assert.throws(
      () => restartedRegistry.assertToolCallAllowed("run_durable", { toolName: "workspace_search", toolCallId: "restarted_tool", args: {} }),
      (error: any) => error.code === "RUNTIME_RUN_ORPHANED",
    );
    assert.throws(
      () => restartedRegistry.appendEvent("run_durable", "run.restarted", "running"),
      (error: any) => error.code === "RUNTIME_RUN_CONFLICT",
    );
    assert.throws(
      () => restartedRegistry.register(durableIdentity, { ...durableAcceptance, jtiHash: stableIdentityHash("different_jti") }, durablePolicy, readWorkspaceGuard(durableIdentity)),
      (error: any) => error.code === "RUNTIME_RUN_CONFLICT",
    );
    assert.throws(
      () => restartedRegistry.complete("run_durable", "succeeded", { safe: true }),
      (error: any) => error.code === "RUNTIME_RUN_CONFLICT",
    );
    restartedStore.close();

    const terminalStore = new EnterpriseRunStore(durablePath);
    const terminalRegistry = new EnterpriseRunRegistry(terminalStore);
    assert.equal(terminalRegistry.status("run_durable").status, "orphaned");
    assert.equal(terminalRegistry.getAbortSignal("run_durable").aborted, true);
    assert.equal(terminalRegistry.events("run_durable", 0, 100).terminalSequence > 0, true);
    terminalStore.close();
  } finally {
    removeDurableTestDirectory(durableRoot);
  }
}

function removeDurableTestDirectory(path: string): void {
  try {
    (globalThis as { gc?: () => void }).gc?.();
    rmSync(path, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
  } catch (error) {
    // The documented test command exposes GC. Keep this fallback for local
    // ad-hoc Windows invocations that omit it; every non-Windows cleanup
    // failure remains a test failure.
    if (process.platform !== "win32" || (error as NodeJS.ErrnoException).code !== "EPERM") throw error;
  }
}

void main().catch((error) => {
  process.nextTick(() => { throw error; });
});
