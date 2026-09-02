import { createHash } from "node:crypto";
import {
  DEFAULT_RUNTIME_TOOL_BUDGET,
  MAX_TOOL_CALLS_SUPPORTED,
  RUNTIME_POLICY_ALGORITHM,
  RUNTIME_POLICY_VERSION,
  SUPPORTED_RUNTIME_POLICY_TOOLS,
  runtimeToolBudgetExecutionContract,
  type RuntimeToolBudgetExecutionContract,
} from "./runtime-policy.js";

export const REGISTERED_TOOL_PROBE_SYMBOL = Symbol.for("huahuo.runtime.registered-tools.v1");

type ToolMetadata = {
  source: string;
  pluginId: string;
  pluginVersion: string;
  schemaId: string;
  schemaHash: string;
};

type RegisteredWorkspaceSearchProbe = {
  probeWorkspaceSearch: () => unknown;
};

// The capability endpoint receives the concrete Core tool definitions from
// the same runtime assembly that will execute a Run. Environment flags may
// describe an intended contract, but cannot make a Core tool dispatchable.
export type MaterializedCoreTool = {
  name: string;
  parameters: unknown;
  execute: unknown;
};

export type RuntimeCapabilityTool = ToolMetadata & {
  name: string;
  status: "ready" | "degraded";
  schemaHash: string;
};

export type RuntimeCapabilityHandshake = {
  schemaHashes: Record<string, string>;
  readyTools: Set<string>;
  tools: RuntimeCapabilityTool[];
  catalogReady: boolean;
  policyReady: boolean;
  budgetExecution: RuntimeToolBudgetExecutionContract;
  capabilityHash: string;
};

const EXPECTED_TOOL_METADATA: Record<string, ToolMetadata> = Object.freeze({
  read: Object.freeze({
    source: "openclaw_core",
    pluginId: "openclaw-core",
    pluginVersion: "2026.6.2",
    schemaId: "openclaw.core.read.v2026.6.2",
    schemaHash: "sha256:134f19bcabe3e29d63c5cebb38f1d2556759fd08adad6bc90a4b4d3cd1fb8441",
  }),
  workspace_search: Object.freeze({
    source: "plugin",
    pluginId: "huahuo-context-tools",
    pluginVersion: "0.5.0",
    schemaId: "huahuo.workspace_search.v1",
    schemaHash: "sha256:0cb790780b9b8d1538d54dc309e4377e30c885aff21c2027748b9efefdb20d80",
  }),
  write: Object.freeze({
    source: "openclaw_core",
    pluginId: "openclaw-core",
    pluginVersion: "2026.6.2",
    schemaId: "openclaw.core.write.v2026.6.2",
    schemaHash: "sha256:e98a2484f667cf7c22d76ca103bf2022bf9113dc63fe38b899e71c328cb1e833",
  }),
});

// runtimeCapabilityHandshake is called by the Host registration loop through
// enterprise.runtime.capabilities. In particular, workspace_search readiness
// is derived from the plugin's actual registerTool factory, never from
// HUAHUO_RUNTIME_TOOL_READY_WORKSPACE_SEARCH.
export function runtimeCapabilityHandshake(options: {
  environment?: NodeJS.ProcessEnv;
  durable: boolean;
  policyVerifierReady: boolean;
  /**
   * Native Core tools materialized by the active Gateway tool pipeline. This
   * is deliberately an object inventory, not an environment-ready flag.
   */
  coreTools?: readonly MaterializedCoreTool[];
}): RuntimeCapabilityHandshake {
  const environment = options.environment ?? process.env;
  const schemaHashes = capabilitySchemaHashes(environment);
  const contractHash = stableIdentityHash(canonicalGeneratedCapabilityContract(schemaHashes));
  const configuredContractHash = stringValue(environment.HUAHUO_RUNTIME_CAPABILITY_HASH);
  const contractReady = environment.HUAHUO_RUNTIME_CAPABILITY_CONTRACT_READY === "true" &&
    configuredContractHash === contractHash && staticSchemasMatch(schemaHashes);
  const workspaceSearchProbe = probeRegisteredWorkspaceSearchTool();
  const coreToolProbe = probeMaterializedCoreTools(options.coreTools);
  const readyTools = new Set<string>();
  for (const name of SUPPORTED_RUNTIME_POLICY_TOOLS) {
    if (!contractReady || !schemaHashes[name]) continue;
    if (name === "workspace_search") {
      if (workspaceSearchProbe.ready && workspaceSearchProbe.schemaHash === schemaHashes[name]) readyTools.add(name);
      continue;
    }
    if (coreToolProbe[name]?.ready && coreToolProbe[name]?.schemaHash === schemaHashes[name]) readyTools.add(name);
  }
  const tools = SUPPORTED_RUNTIME_POLICY_TOOLS.map((name) => capabilityToolRecord(name, readyTools.has(name)));
  const budgetExecution = runtimeToolBudgetExecutionContract();
  const capabilityHash = stableIdentityHash(canonicalLiveCapabilityIdentity(tools, budgetExecution));
  const catalogReady = contractReady && options.durable;
  return {
    schemaHashes,
    readyTools,
    tools,
    catalogReady,
    policyReady: catalogReady && options.policyVerifierReady,
    budgetExecution,
    capabilityHash,
  };
}

// This is the only document that the Gateway capability method may return.
// It deliberately projects the closed semantic tool set instead of any raw
// OpenClaw registry inventory, which can include host-internal commands.
export function runtimeCapabilityDocument(options: {
  handshake: RuntimeCapabilityHandshake;
  runtimeVersion: string;
  adapterVersion: string;
}): {
  runtimeVersion: string;
  adapterVersion: string;
  capabilityHash: string;
  tools: RuntimeCapabilityTool[];
  filesystemPolicy: { workspaceOnlyReady: boolean; absolutePathRejected: boolean; symlinkEscapeRejected: boolean };
  abort: { supported: boolean; authorizationReady: boolean };
  budgetCapabilities: {
    defaultMaxToolCalls: number;
    maxToolCallsSupported: number;
    supportsPerRunBudget: boolean;
    supportsBudgetWarning: boolean;
    supportsForcedAbort: boolean;
    runtimePolicyVersion: string;
    requiredPolicySignature: string;
    executionContract: RuntimeToolBudgetExecutionContract;
  };
} {
  const { handshake } = options;
  return {
    runtimeVersion: options.runtimeVersion,
    adapterVersion: options.adapterVersion,
    capabilityHash: handshake.capabilityHash,
    tools: handshake.tools.map((tool) => ({ ...tool })),
    filesystemPolicy: {
      workspaceOnlyReady: handshake.catalogReady,
      absolutePathRejected: handshake.catalogReady,
      symlinkEscapeRejected: handshake.catalogReady,
    },
    abort: { supported: true, authorizationReady: handshake.catalogReady },
    budgetCapabilities: {
      defaultMaxToolCalls: DEFAULT_RUNTIME_TOOL_BUDGET.maxToolCalls,
      maxToolCallsSupported: MAX_TOOL_CALLS_SUPPORTED,
      supportsPerRunBudget: handshake.policyReady,
      supportsBudgetWarning: handshake.policyReady,
      supportsForcedAbort: handshake.policyReady,
      runtimePolicyVersion: RUNTIME_POLICY_VERSION,
      requiredPolicySignature: RUNTIME_POLICY_ALGORITHM,
      executionContract: { ...handshake.budgetExecution },
    },
  };
}

export function generatedRuntimeCapabilityContractHash(schemaHashes: Record<string, string>): string {
  return stableIdentityHash(canonicalGeneratedCapabilityContract(schemaHashes));
}

function capabilitySchemaHashes(environment: NodeJS.ProcessEnv): Record<string, string> {
  const out: Record<string, string> = {};
  for (const name of SUPPORTED_RUNTIME_POLICY_TOOLS) {
    const value = environment[`HUAHUO_TOOL_SCHEMA_HASH_${name.toUpperCase()}`];
    if (typeof value === "string" && /^sha256:[a-f0-9]{64}$/i.test(value)) out[name] = value.toLowerCase();
  }
  return out;
}

function staticSchemasMatch(schemaHashes: Record<string, string>): boolean {
  return SUPPORTED_RUNTIME_POLICY_TOOLS.every((name) => schemaHashes[name] === EXPECTED_TOOL_METADATA[name]?.schemaHash);
}

function capabilityToolRecord(name: string, ready: boolean): RuntimeCapabilityTool {
  const metadata = EXPECTED_TOOL_METADATA[name]!;
  return {
    name,
    source: metadata.source,
    pluginId: metadata.pluginId,
    pluginVersion: metadata.pluginVersion,
    schemaId: metadata.schemaId,
    schemaHash: ready ? metadata.schemaHash : "",
    status: ready ? "ready" : "degraded",
  };
}

function probeRegisteredWorkspaceSearchTool(): { ready: boolean; schemaHash: string } {
  const descriptor = Object.getOwnPropertyDescriptor(globalThis, REGISTERED_TOOL_PROBE_SYMBOL);
  if (!descriptor || descriptor.configurable || descriptor.enumerable || descriptor.writable) return { ready: false, schemaHash: "" };
  const registry = descriptor.value as Partial<RegisteredWorkspaceSearchProbe> | undefined;
  if (!registry || typeof registry.probeWorkspaceSearch !== "function") return { ready: false, schemaHash: "" };
  try {
    const result = asRecord(registry.probeWorkspaceSearch());
    const expected = EXPECTED_TOOL_METADATA.workspace_search;
    if (result.ready !== true || result.name !== "workspace_search" || result.source !== expected.source ||
      result.pluginId !== expected.pluginId || result.pluginVersion !== expected.pluginVersion ||
      result.schemaId !== expected.schemaId || result.schemaHash !== expected.schemaHash) {
      return { ready: false, schemaHash: "" };
    }
    return { ready: true, schemaHash: expected.schemaHash };
  } catch {
    return { ready: false, schemaHash: "" };
  }
}

function probeMaterializedCoreTools(tools: readonly MaterializedCoreTool[] | undefined): Record<"read" | "write", { ready: boolean; schemaHash: string }> {
  const unavailable = { ready: false, schemaHash: "" };
  if (!Array.isArray(tools)) return { read: unavailable, write: unavailable };
  const result: Record<"read" | "write", { ready: boolean; schemaHash: string }> = {
    read: unavailable,
    write: unavailable,
  };
  for (const name of ["read", "write"] as const) {
    const matches = tools.filter((tool) => tool?.name === name);
    if (matches.length !== 1) continue;
    const tool = matches[0]!;
    const schemaHash = schemaIdentityHash(tool.parameters);
    if (!schemaHash || typeof tool.execute !== "function") continue;
    result[name] = { ready: true, schemaHash };
  }
  return result;
}

function schemaIdentityHash(parameters: unknown): string {
  if (!parameters || typeof parameters !== "object" || Array.isArray(parameters)) return "";
  try {
    const serialized = JSON.stringify(parameters);
    return typeof serialized === "string" ? `sha256:${createHash("sha256").update(serialized, "utf8").digest("hex")}` : "";
  } catch {
    return "";
  }
}

function canonicalGeneratedCapabilityContract(schemaHashes: Record<string, string>): Record<string, unknown> {
  return {
    required: SUPPORTED_RUNTIME_POLICY_TOOLS,
    schemaHashes,
    ready: true,
    maxToolCallsSupported: MAX_TOOL_CALLS_SUPPORTED,
    supportsPerRunBudget: true,
    supportsBudgetWarning: true,
    supportsForcedAbort: true,
  };
}

function canonicalLiveCapabilityIdentity(tools: RuntimeCapabilityTool[], budgetExecution: RuntimeToolBudgetExecutionContract): Record<string, unknown> {
  return {
    tools,
    ready: true,
    maxToolCallsSupported: MAX_TOOL_CALLS_SUPPORTED,
    supportsPerRunBudget: true,
    supportsBudgetWarning: true,
    supportsForcedAbort: true,
    budgetExecution,
  };
}

function stableIdentityHash(value: unknown): string {
  return `sha256:${createHash("sha256").update(JSON.stringify(value), "utf8").digest("hex")}`;
}

function asRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : {};
}

function stringValue(value: unknown): string {
  return typeof value === "string" ? value.trim() : "";
}
