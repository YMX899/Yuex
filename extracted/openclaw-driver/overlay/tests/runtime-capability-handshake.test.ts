import assert from "node:assert/strict";
import {
  generatedRuntimeCapabilityContractHash,
  runtimeCapabilityDocument,
  runtimeCapabilityHandshake,
} from "../src/runtime-capability-handshake.ts";
import { readFileSync } from "node:fs";
import { registerWorkspaceSearchTool } from "../../openclaw-extensions/huahuo-context-tools/src/registered-tool-probe.js";
import { workspaceSearchToolCapability } from "../../openclaw-extensions/huahuo-context-tools/src/workspace-search.js";

const schemaHashes = {
  read: "sha256:134f19bcabe3e29d63c5cebb38f1d2556759fd08adad6bc90a4b4d3cd1fb8441",
  workspace_search: "sha256:0cb790780b9b8d1538d54dc309e4377e30c885aff21c2027748b9efefdb20d80",
  write: "sha256:e98a2484f667cf7c22d76ca103bf2022bf9113dc63fe38b899e71c328cb1e833",
};

const coreToolSchemas = JSON.parse(readFileSync(new URL("../../config/openclaw-core-tool-schemas-2026.6.2.json", import.meta.url), "utf8")) as {
  schemas: { read: unknown; write: unknown };
};

function materializedCoreTools() {
  return [
    { name: "read", parameters: coreToolSchemas.schemas.read, execute() {} },
    { name: "write", parameters: coreToolSchemas.schemas.write, execute() {} },
  ];
}

function environmentWith(schema = schemaHashes): NodeJS.ProcessEnv {
  return {
    HUAHUO_RUNTIME_CAPABILITY_CONTRACT_READY: "true",
    HUAHUO_RUNTIME_CAPABILITY_HASH: generatedRuntimeCapabilityContractHash(schema),
    HUAHUO_TOOL_SCHEMA_HASH_READ: schema.read,
    HUAHUO_TOOL_SCHEMA_HASH_WORKSPACE_SEARCH: schema.workspace_search,
    HUAHUO_TOOL_SCHEMA_HASH_WRITE: schema.write,
    // Static ready flags are legacy inputs and must never make any tool ready.
    HUAHUO_RUNTIME_TOOL_READY_READ: "true",
    HUAHUO_RUNTIME_TOOL_READY_WORKSPACE_SEARCH: "true",
    HUAHUO_RUNTIME_TOOL_READY_WRITE: "true",
  };
}

const beforeRegistration = runtimeCapabilityHandshake({
  environment: environmentWith(), durable: true, policyVerifierReady: true, coreTools: materializedCoreTools(),
});
assert.equal(beforeRegistration.tools.find((tool) => tool.name === "workspace_search")?.status, "degraded");
assert.equal(beforeRegistration.readyTools.has("workspace_search"), false);
assert.equal(beforeRegistration.readyTools.has("read"), true);
assert.equal(beforeRegistration.readyTools.has("write"), true);

const staticReadyFlagsOnly = runtimeCapabilityHandshake({
  environment: environmentWith(), durable: true, policyVerifierReady: true,
});
assert.equal(staticReadyFlagsOnly.readyTools.has("read"), false);
assert.equal(staticReadyFlagsOnly.readyTools.has("write"), false);

const malformedCoreRegistrations = runtimeCapabilityHandshake({
  environment: environmentWith(), durable: true, policyVerifierReady: true,
  coreTools: [
    { name: "read", parameters: {}, execute() {} },
    { name: "write", parameters: coreToolSchemas.schemas.write, execute: undefined },
  ],
});
assert.equal(malformedCoreRegistrations.readyTools.has("read"), false);
assert.equal(malformedCoreRegistrations.readyTools.has("write"), false);

const discoveryRegistrations: Array<{ factory: (context: unknown) => unknown; names: string[] }> = [];
registerWorkspaceSearchTool({
  registrationMode: "discovery",
  registerTool(factory: (context: unknown) => unknown, options: { names: string[] }) {
    discoveryRegistrations.push({ factory, names: options.names });
  },
});
assert.deepEqual(discoveryRegistrations.map((item) => item.names), [["workspace_search"]]);
const afterDiscoveryRegistration = runtimeCapabilityHandshake({
  environment: environmentWith(), durable: true, policyVerifierReady: true, coreTools: materializedCoreTools(),
});
assert.equal(afterDiscoveryRegistration.readyTools.has("workspace_search"), false);

for (const registrationMode of [undefined, "tool-discovery", "setup-only", "setup-runtime", "cli-metadata", "unexpected"]) {
  registerWorkspaceSearchTool({
    ...(registrationMode === undefined ? {} : { registrationMode }),
    registerTool() {},
  });
  const afterNonLiveRegistration = runtimeCapabilityHandshake({
    environment: environmentWith(), durable: true, policyVerifierReady: true, coreTools: materializedCoreTools(),
  });
  assert.equal(afterNonLiveRegistration.readyTools.has("workspace_search"), false, `non-live mode ${String(registrationMode)} must not publish readiness`);
}

registerWorkspaceSearchTool({ registrationMode: "full", registerTool() {} });
const afterLifecycleMissingRegistration = runtimeCapabilityHandshake({
  environment: environmentWith(), durable: true, policyVerifierReady: true, coreTools: materializedCoreTools(),
});
assert.equal(afterLifecycleMissingRegistration.readyTools.has("workspace_search"), false);

const registrations: Array<{ factory: (context: unknown) => unknown; names: string[] }> = [];
const lifecycleRegistrations: Array<{ id: string; cleanup?: (context: { reason: string; runId?: string }) => void }> = [];
const liveAPI = {
  registrationMode: "full",
  registerTool(factory: (context: unknown) => unknown, options: { names: string[] }) {
    registrations.push({ factory, names: options.names });
  },
  lifecycle: {
    registerRuntimeLifecycle(registration: { id: string; cleanup?: (context: { reason: string; runId?: string }) => void }) {
      lifecycleRegistrations.push(registration);
    },
  },
};
registerWorkspaceSearchTool(liveAPI);
assert.deepEqual(registrations.map((item) => item.names), [["workspace_search"]]);
assert.throws(() => registerWorkspaceSearchTool(liveAPI), (error: unknown) => (error as { code?: string }).code === "WORKSPACE_SEARCH_PLUGIN_ALREADY_REGISTERED");

const afterRegistration = runtimeCapabilityHandshake({
  environment: environmentWith(), durable: true, policyVerifierReady: true, coreTools: materializedCoreTools(),
});
const search = afterRegistration.tools.find((tool) => tool.name === "workspace_search");
assert.deepEqual(search, { ...workspaceSearchToolCapability, status: "ready" });
assert.equal(afterRegistration.readyTools.has("workspace_search"), true);
assert.equal(afterRegistration.policyReady, true);

const document = runtimeCapabilityDocument({
  handshake: afterRegistration,
  runtimeVersion: "2026.6.2",
  adapterVersion: "adapter-test",
});
assert.deepEqual(document.tools.map((tool) => tool.name), ["read", "workspace_search", "write"]);
assert.equal(document.tools.some((tool) => ["ls", "find", "grep", "workspace_material_search"].includes(tool.name)), false);
assert.deepEqual(document.filesystemPolicy, {
  workspaceOnlyReady: true,
  absolutePathRejected: true,
  symlinkEscapeRejected: true,
});
assert.deepEqual(document.abort, { supported: true, authorizationReady: true });
assert.deepEqual(document.budgetCapabilities.executionContract, {
  enforcementVersion: "huahuo.runtime-tool-budget-enforcement.v1",
  toolExecutionEventSchema: "huahuo.runtime-tool-execution-event.v1",
  abortConvergenceSchema: "huahuo.runtime-abort-convergence-event.v1",
  hardMaxToolCalls: 200,
  softToolCallLimit: 160,
  finalizationReserve: 10,
  maxRepeatedCalls: 2,
  maxNoProgressCalls: 4,
});
assert.equal(document.budgetCapabilities.maxToolCallsSupported, 400);
assert.equal(document.budgetCapabilities.defaultMaxToolCalls, 200);
assert.equal(document.budgetCapabilities.supportsPerRunBudget, true);
assert.equal(document.budgetCapabilities.supportsBudgetWarning, true);
assert.equal(document.budgetCapabilities.supportsForcedAbort, true);
assert.equal(document.budgetCapabilities.runtimePolicyVersion, "huahuo.runtime-policy.v1");
assert.equal(document.budgetCapabilities.requiredPolicySignature, "HS256");

const capabilityMethodSource = readFileSync(new URL("../src/enterprise-runtime-methods.ts", import.meta.url), "utf8");
assert.match(capabilityMethodSource, /\.\.\.runtimeCapabilityDocument\(\{/);
assert.doesNotMatch(capabilityMethodSource, /tools:\s*capabilities\.tools/);
assert.match(capabilityMethodSource, /budgetExecution:\s*ReturnType<typeof runtimeCapabilityHandshake>\["budgetExecution"\]/);

// The probe invokes the exact factory passed to the real registerTool call.
const probeTool = registrations[0]!.factory({ runtime: { capabilityProbe: true } }) as { name?: string; parameters?: unknown; execute?: unknown };
assert.equal(probeTool.name, "workspace_search");
assert.equal(typeof probeTool.execute, "function");

assert.equal(lifecycleRegistrations.length, 1);
assert.equal(lifecycleRegistrations[0]?.id, "huahuo-context-tools.workspace-search-registration");
lifecycleRegistrations[0]?.cleanup?.({ reason: "shutdown" });
const afterLifecycleCleanup = runtimeCapabilityHandshake({
  environment: environmentWith(), durable: true, policyVerifierReady: true, coreTools: materializedCoreTools(),
});
assert.equal(afterLifecycleCleanup.readyTools.has("workspace_search"), false);
registerWorkspaceSearchTool(liveAPI);
const afterLifecycleReregistration = runtimeCapabilityHandshake({
  environment: environmentWith(), durable: true, policyVerifierReady: true, coreTools: materializedCoreTools(),
});
assert.equal(afterLifecycleReregistration.readyTools.has("workspace_search"), true);

const badSchema = { ...schemaHashes, workspace_search: "sha256:" + "0".repeat(64) };
const mismatchedSchema = runtimeCapabilityHandshake({
  environment: environmentWith(badSchema), durable: true, policyVerifierReady: true, coreTools: materializedCoreTools(),
});
assert.equal(mismatchedSchema.tools.find((tool) => tool.name === "workspace_search")?.status, "degraded");
assert.equal(mismatchedSchema.readyTools.has("workspace_search"), false);
