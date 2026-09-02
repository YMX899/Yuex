import { createHash } from "node:crypto";
import { validateRuntimeRunSpec } from "../../../packages/gateway-protocol/src/index.js";
import { enterpriseError, enterpriseRunRegistry, productSessionExecutionRef, productSessionSerializationKey, stableIdentityHash, submitReplayResponse } from "../../enterprise-runtime/enterprise-run-registry.js";
import {
  assertExactRuntimeToolsAllow,
  assertRuntimeWorkspaceMount,
  runtimePolicyVerifierConfigFromEnvironment,
  verifyRuntimePolicy,
  type VerifiedRuntimePolicy,
} from "../../enterprise-runtime/runtime-policy.js";
import { enterpriseRuntimeHandlers } from "./enterprise-runtime.js";
import { projectFayaStatusResult } from "./faya-status-result-compat.js";
import { projectThoughtsResponseStatusResult } from "./thoughts-response-status-compat.js";
import { runWithPrivateRunContext, verifyPrivateExecutionContext, type PrivateRunContext } from "./private-run-context.js";
import { runtimeCapabilityDocument, runtimeCapabilityHandshake } from "../../enterprise-runtime/runtime-capability-handshake.js";
import { rejectUnverifiedRecoverySnapshotAccess } from "../../enterprise-runtime/runtime-host-recovery.js";
import { createOpenClawCodingTools } from "../../agents/agent-tools.js";
import type { GatewayRequestHandlerOptions, GatewayRequestHandlers } from "./types.js";

const RUN_METHOD = "enterprise.runtime.run";
const ASYNC_METHODS = {
  submit: "enterprise.runtime.submit",
  status: "enterprise.runtime.status",
  events: "enterprise.runtime.events",
  abort: "enterprise.runtime.abort",
  capabilities: "enterprise.runtime.capabilities",
  recoverySnapshot: "enterprise.runtime.recovery.snapshot",
} as const;

export const enterpriseRuntimeAsyncHandlers: GatewayRequestHandlers = {
  [ASYNC_METHODS.submit]: async (options) => {
    enterpriseRunRegistry.assertDurableReady();
    // Keep the raw private envelope out of every execution/event/status path.
    // Only this admission block may inspect it before it is replaced by ALS.
    const { params: rawParams, ...executionOptions } = options;
    const params = asRecord(rawParams);
    const spec = assertExactAsyncSubmitEnvelope(params);
    const runId = stringValue(spec.runId);
    const workspaceId = stringValue(spec.workspaceId);
    const idempotencyKey = stringValue(params.idempotencyKey);
    const workspaceManifestHash = stringValue(params.workspaceManifestHash);
    const jtiHash = stringValue(params.runTicketJtiHash);
    const dispatchIdentity = stringValue(params.dispatchIdentity);
    const capabilityHash = stringValue(params.capabilityHash);
    const runtimeHostId = stringValue(params.runtimeHostId);
    const reservationId = stringValue(params.reservationId);
    const fencingToken = integerValue(params.fencingToken);
    if (!runId || !workspaceId || !idempotencyKey || !isSHA256(workspaceManifestHash) || !isSHA256(jtiHash) || !isSHA256(dispatchIdentity) || !capabilityHash || !runtimeHostId || !reservationId || fencingToken < 1) {
      throw enterpriseError("RUNTIME_INPUT_INVALID", "immutable async runtime submission fields are required");
    }
    const capabilities = capabilitySnapshot();
    if (!capabilities.policyVerifier || !capabilities.catalogReady) throw enterpriseError("RUNTIME_TOOL_BUDGET_UNSUPPORTED", "runtime policy capability is unavailable");
    if (capabilityHash !== capabilities.capabilityHash) throw enterpriseError("RUNTIME_PERMISSION_DENIED", "runtime capability hash mismatch");
    const runtimePolicy = verifyRuntimePolicy(spec.runtimePolicy, {
      runId,
      idempotencyKey,
      workspaceId,
      workspaceManifestHash,
      dispatchIdentity,
      capabilityHash,
    }, capabilities.policyVerifier);
    assertExactRuntimeToolsAllow(spec.tools, runtimePolicy.allowedTools);
    assertAllowedToolsReady(runtimePolicy, capabilities.readyTools);
    const workspaceMount = assertRuntimeWorkspaceMount(spec.workspace, runtimePolicy, workspaceId);
    // Extract and validate the exact unnormalised product-session pair before
    // any session serialization. The ticket binds these bytes; accepting a
    // trimmed or rewritten pair would let Gateway execute a different native
    // OpenClaw transcript than the Backend authorized.
    const submitBinding = exactSubmitBindingFromSpec(spec);
    const sessionBindingHash = productSessionSerializationKey(spec.productSession);
    const privateRunContext = verifyPrivateExecutionContext(params.privateExecutionContext, {
      runId,
      tenantId: stringValue(spec.tenantId),
      workspaceId,
      runtimeHostId,
      reservationId,
      fencingToken,
      capabilityHash,
      workspaceManifestHash,
      runTicketJtiHash: jtiHash,
      planHash: runtimePolicy.planHash,
      workspaceSearchAllowed: runtimePolicy.allowedTools.includes("workspace_search"),
      submitBinding,
    });
    const registration = enterpriseRunRegistry.register(
      { runId, idempotencyKey, sessionBindingHash, workspaceManifestHash },
      { jtiHash, dispatchIdentity },
      runtimePolicy,
      { workspaceId, mount: workspaceMount },
      { runtimeHostId, reservationId, fencingToken, capabilityHash },
    );
    if (registration.created) void executeRegisteredRun(executionOptions, executableSpec(spec, workspaceMount), sessionBindingHash, privateRunContext);
    const replay = submitReplayResponse(registration);
    executionOptions.respond(true, {
      runId,
      // A transport replay of an active/terminal Run remains accepted for
      // compatibility. A Run orphaned by Gateway restart is different: expose
      // it so the Adapter/Backend can reconcile and create a new attempt.
      status: replay.status,
      runtimeRequestId: stableIdentityHash({ runId, idempotencyKey }),
      acceptedSequence: enterpriseRunRegistry.status(runId).lastEventSequence,
      ...(replay.recovery ? { recovery: replay.recovery } : {}),
    }, undefined);
  },
  [ASYNC_METHODS.status]: ({ params, respond }) => {
    const status = enterpriseRunRegistry.status(stringValue(asRecord(params).runId));
    respond(true, projectThoughtsResponseStatusResult(projectFayaStatusResult(status)), undefined);
  },
  [ASYNC_METHODS.events]: ({ params, respond }) => {
    const input = asRecord(params);
    respond(true, enterpriseRunRegistry.events(stringValue(input.runId), integerValue(input.afterSequence), integerValue(input.limit) || 100), undefined);
  },
  [ASYNC_METHODS.abort]: async ({ params, respond }) => {
    const input = asRecord(params);
    const result = await enterpriseRunRegistry.abort(stringValue(input.runId), stringValue(input.reason) || "external_abort");
    respond(true, result, undefined);
  },
  [ASYNC_METHODS.capabilities]: ({ respond }) => {
    const capabilities = capabilitySnapshot();
    respond(true, {
      ...runtimeCapabilityDocument({
        handshake: capabilities,
        runtimeVersion: process.env.OPENCLAW_VERSION ?? "unknown",
        adapterVersion: process.env.HUAHUO_RUNTIME_ADAPTER_VERSION ?? "unknown",
      }),
      recovery: {
        snapshot: { supported: false, code: "RUNTIME_PERMISSION_DENIED" },
        // The Registry has a durable canonical snapshot API, but this Gateway
        // plugin receives no certificate-bound Host principal or mTLS context.
        // Do not expose local occupancy facts until the Adapter bridge can
        // prove that identity and bind the assertion to Backend generation.
        admissionReady: false,
      },
    }, undefined);
  },
  [ASYNC_METHODS.recoverySnapshot]: () => {
    return rejectUnverifiedRecoverySnapshotAccess();
  },
};

async function executeRegisteredRun(options: Omit<GatewayRequestHandlerOptions, "params">, spec: Record<string, unknown>, sessionBindingHash: string, privateRunContext: PrivateRunContext): Promise<void> {
    const runId = stringValue(spec.runId);
    try {
      await runWithPrivateRunContext(privateRunContext, async () => {
        await enterpriseRunRegistry.serializeSession(sessionBindingHash, async () => {
          const disarmWallTime = enterpriseRunRegistry.armWallTime(runId);
          try {
            enterpriseRunRegistry.appendEvent(runId, "run.started", "running");
            await enterpriseRuntimeHandlers[RUN_METHOD]!({
              ...options,
              params: spec,
              respond: (ok, payload, error) => {
                if (!ok) enterpriseRunRegistry.complete(runId, "failed", undefined, error);
                else {
                  const status = stringValue(asRecord(payload).status);
                  enterpriseRunRegistry.complete(runId, status === "timeout" ? "timeout" : status === "aborted" ? "aborted" : status === "succeeded" ? "succeeded" : "failed", payload, asRecord(payload).error);
                }
              },
            });
          } finally {
            disarmWallTime();
          }
        });
      });
  } catch (error) {
    const aborted = enterpriseRunRegistry.getAbortSignal(runId).aborted;
    enterpriseRunRegistry.complete(runId, aborted ? "aborted" : "failed", undefined, safeError(error));
  }
}

function capabilitySnapshot(): {
  schemaHashes: Record<string, string>;
  readyTools: Set<string>;
  tools: ReturnType<typeof runtimeCapabilityHandshake>["tools"];
  catalogReady: boolean;
  policyReady: boolean;
  budgetExecution: ReturnType<typeof runtimeCapabilityHandshake>["budgetExecution"];
  policyVerifier: ReturnType<typeof runtimePolicyVerifierConfigFromEnvironment>;
  capabilityHash: string;
} {
  const policyVerifier = runtimePolicyVerifierConfigFromEnvironment();
  const handshake = runtimeCapabilityHandshake({
    durable: enterpriseRunRegistry.isDurable(),
    policyVerifierReady: Boolean(policyVerifier),
    coreTools: materializeCoreToolsForCapabilityProbe(),
  });
  return {
    ...handshake,
    policyVerifier,
  };
}

function materializeCoreToolsForCapabilityProbe(): ReturnType<typeof createOpenClawCodingTools> | undefined {
  try {
    // This creates the exact native Core tool objects through the same Gateway
    // assembly path used for Runs. A missing constructor, duplicate name,
    // invalid schema, or missing handler therefore degrades the handshake.
    return createOpenClawCodingTools({
      includeCoreTools: true,
      toolConstructionPlan: {
        includeBaseCodingTools: true,
        includeShellTools: false,
        includeChannelTools: false,
        includeOpenClawTools: false,
        includePluginTools: false,
      },
    });
  } catch {
    return undefined;
  }
}
function assertAllowedToolsReady(policy: VerifiedRuntimePolicy, readyTools: Set<string>): void {
  for (const tool of policy.allowedTools) {
    if (!readyTools.has(tool)) throw enterpriseError("RUNTIME_TOOL_UNAVAILABLE", `allowed tool is unavailable: ${tool}`);
  }
}

function assertExactAsyncSubmitEnvelope(params: Record<string, unknown>): Record<string, unknown> {
  const expected = new Set([
    "spec", "idempotencyKey", "workspaceManifestHash", "runTicketJtiHash",
    "dispatchIdentity", "reservationId", "fencingToken", "capabilityHash",
    "runtimeHostId", "privateExecutionContext",
  ]);
  if (Object.keys(params).length !== expected.size || Object.keys(params).some((key) => !expected.has(key))) {
    throw enterpriseError("RUNTIME_INPUT_INVALID", "async runtime submission fields are incomplete or unsupported");
  }
  const spec = asRecord(params.spec);
  if (!Object.hasOwn(spec, "runtimePolicy") || !validateRuntimeRunSpec(spec)) {
    throw enterpriseError("RUNTIME_INPUT_INVALID", "async runtime spec or signed policy is invalid");
  }
  if (!safeSubmissionID(stringValue(params.idempotencyKey)) || !isSHA256(stringValue(params.workspaceManifestHash)) ||
    !isSHA256(stringValue(params.runTicketJtiHash)) || !isSHA256(stringValue(params.dispatchIdentity)) ||
    !safeSubmissionID(stringValue(params.reservationId)) || !Number.isSafeInteger(params.fencingToken) ||
    Number(params.fencingToken) < 1 || !safeSubmissionID(stringValue(params.capabilityHash)) ||
    !safeSubmissionID(stringValue(params.runtimeHostId))) {
    throw enterpriseError("RUNTIME_INPUT_INVALID", "immutable async runtime submission fields are invalid");
  }
  return spec;
}

function safeSubmissionID(value: string): boolean {
  return value.length > 0 && value.length <= 512 && !/[\u0000-\u001f\u007f]/.test(value);
}

export function exactSubmitBindingFromSpec(spec: Record<string, unknown>): {
  inputMessage: string;
  runtimeConfigId: string;
  runtimeConfigVersion: string;
  productSessionThreadId: string;
  productSessionKey: string;
} {
  const input = asRecord(spec.input);
  const productSession = asRecord(spec.productSession);
  const inputMessage = rawStringValue(input.message);
  const runtimeConfigId = rawStringValue(spec.runtimeConfigId);
  if (!Object.hasOwn(spec, "runtimeConfigVersion")) {
    throw enterpriseError("RUNTIME_INPUT_INVALID", "async runtime submit binding version is required");
  }
  const runtimeConfigVersion = rawStringValue(spec.runtimeConfigVersion);
  const productSessionThreadId = rawStringValue(productSession.threadId);
  const productSessionKey = rawStringValue(productSession.openclawSessionKey);
  // Do not use stringValue here: leading/trailing whitespace in the model
  // turn, configuration, or native-session pair must alter the signed
  // comparison. The session values are intentionally not normalized.
  if (inputMessage === undefined || runtimeConfigId === undefined || runtimeConfigVersion === undefined ||
    productSessionThreadId === undefined || productSessionKey === undefined ||
    !validRuntimeSubmitBindingIdentifier(runtimeConfigId) || !validRuntimeSubmitBindingIdentifier(runtimeConfigVersion) ||
    !validRuntimeSubmitSessionValue(productSessionThreadId, 256) || !validRuntimeSubmitSessionValue(productSessionKey, 1024)) {
    throw enterpriseError("RUNTIME_INPUT_INVALID", "async runtime submit binding fields are invalid");
  }
  return { inputMessage, runtimeConfigId, runtimeConfigVersion, productSessionThreadId, productSessionKey };
}

function validRuntimeSubmitBindingIdentifier(value: string): boolean {
  return value.length > 0 && value.length <= 256 && /^[A-Za-z0-9_.-]+$/.test(value);
}

function validRuntimeSubmitSessionValue(value: string, maximum: number): boolean {
  return value.length > 0 && value.length <= maximum && value === value.trim() &&
    /^[A-Za-z0-9_.:-]+$/.test(value);
}

function executableSpec(spec: Record<string, unknown>, workspaceMount: ReturnType<typeof assertRuntimeWorkspaceMount>): Record<string, unknown> {
  const execution = { ...spec };
  // Do not forward routing/prompt/output metadata into the native session or
  // model context. The selected Agent/Skill is already materialized in the
  // signed run workspace; session continuity needs only this stable pair.
  execution.productSession = productSessionExecutionRef(spec.productSession);
  execution.workspace = workspaceMount.accessMode === "read"
    ? { realPath: workspaceMount.realPath, accessMode: workspaceMount.accessMode }
    : {
        realPath: workspaceMount.realPath,
        accessMode: workspaceMount.accessMode,
        writeLease: workspaceMount.writeLease,
      };
  delete execution.runtimePolicy;
  delete execution.privateExecutionContext;
  return execution;
}
function safeError(error: unknown): Record<string, unknown> { return { code: stringValue(asRecord(error).code) || "RUNTIME_FAILED", messageHash: `sha256:${createHash("sha256").update(error instanceof Error ? error.message : String(error)).digest("hex")}` }; }
function asRecord(value: unknown): Record<string, any> { return value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, any> : {}; }
function rawStringValue(value: unknown): string | undefined { return typeof value === "string" ? value : undefined; }
function stringValue(value: unknown): string { return typeof value === "string" ? value.trim() : ""; }
function integerValue(value: unknown): number { return Number.isInteger(value) ? Number(value) : 0; }
function isSHA256(value: string): boolean { return /^sha256:[a-f0-9]{64}$/i.test(value); }
