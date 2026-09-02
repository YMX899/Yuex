import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdtempSync, rmSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { DatabaseSync } from "node:sqlite";
import { EnterpriseRunRegistry, type EnterpriseRuntimeHostRecoveryFact, type EnterpriseRuntimeHostRecoverySnapshotIdentity } from "../src/enterprise-run-registry.ts";
import { EnterpriseRunStore } from "../src/enterprise-run-store.ts";
import {
  DEFAULT_RUNTIME_TOOL_BUDGET,
  RUNTIME_POLICY_ALGORITHM,
  RUNTIME_POLICY_VERSION,
  RUNTIME_WORKSPACE_ACCESS_READ,
  signRuntimePolicy,
  verifyRuntimePolicy,
  type RuntimePolicyEnvelope,
} from "../src/runtime-policy.ts";
import {
  createEnterpriseRuntimeRecoveryRequestContext,
  handleEnterpriseRuntimeRecoverySnapshot,
  recoveryPrincipalFromAuthorizedTLSSocket,
  recoveryPrincipalFromVerifiedUriSan,
  rejectUnverifiedRecoverySnapshotAccess,
  startGatewayWithEnterpriseRuntimeRecovery,
} from "../src/runtime-host-recovery.ts";

const policyConfig = { keyId: "recovery-policy-v1", runTicketSecret: "runtime-recovery-policy-secret-0123456789" };
const now = Math.floor(Date.now() / 1000);
const hostIdentity: EnterpriseRuntimeHostRecoverySnapshotIdentity = {
  runtimeHostId: "runtime-host-1",
  instanceId: "runtime-instance-1",
  environment: "prelaunch",
  instanceGeneration: 3,
  recoveryRevision: 7,
  recoveryState: "pending",
};

function sha256(value: string): string {
  return `sha256:${createHash("sha256").update(value).digest("hex")}`;
}

function policyFor(identity: { runId: string; idempotencyKey: string; workspaceManifestHash: string }, dispatchIdentity: string) {
  const unsigned: Omit<RuntimePolicyEnvelope, "signature"> = {
    version: RUNTIME_POLICY_VERSION,
    algorithm: RUNTIME_POLICY_ALGORITHM,
    keyId: policyConfig.keyId,
    runId: identity.runId,
    idempotencyKey: identity.idempotencyKey,
    workspaceManifestHash: identity.workspaceManifestHash,
    dispatchIdentity,
    capabilityHash: "capability-recovery-v1",
    planHash: sha256("plan-recovery-v1"),
    issuedAt: now - 1,
    expiresAt: now + 300,
    workspaceAccessMode: RUNTIME_WORKSPACE_ACCESS_READ,
    writeLease: null,
    requiredTools: ["read"],
    allowedTools: ["read"],
    toolBudget: { ...DEFAULT_RUNTIME_TOOL_BUDGET },
  };
  return verifyRuntimePolicy(signRuntimePolicy(unsigned, policyConfig.runTicketSecret), {
    runId: identity.runId,
    idempotencyKey: identity.idempotencyKey,
    workspaceId: "workspace-recovery-1",
    workspaceManifestHash: identity.workspaceManifestHash,
    dispatchIdentity,
    capabilityHash: unsigned.capabilityHash,
  }, policyConfig, now);
}

function expectedHash(facts: Record<string, unknown>[]): string {
  return sha256(JSON.stringify({ version: "runtime-host-recovery.v1", facts }));
}

async function main(): Promise<void> {
  const root = mkdtempSync(join(tmpdir(), "huahuo-runtime-host-recovery-"));
  try {
    const legacyPath = join(root, "legacy-enterprise-runs.sqlite");
    const legacy = new DatabaseSync(legacyPath);
    legacy.exec("create table runtime_runs(run_id text primary key,status text not null,updated_at text not null)");
    legacy.close();
    const migratedLegacyStore = new EnterpriseRunStore(legacyPath);
    migratedLegacyStore.close();
    const migratedInspector = new DatabaseSync(legacyPath);
    const migratedColumns = migratedInspector.prepare("pragma table_info(runtime_runs)").all() as Array<{ name: string }>;
    assert.equal(migratedColumns.some((column) => column.name === "runtime_host_id"), true);
    migratedInspector.close();

    // A normal socket or a TLS connection without a verified client
    // certificate can never manufacture a Recovery principal.
    assert.throws(
      () => recoveryPrincipalFromAuthorizedTLSSocket({ authorized: false } as any, "prelaunch"),
      (error: any) => error.code === "RUNTIME_HOST_UNAUTHORIZED",
    );
    assert.throws(
      () => recoveryPrincipalFromVerifiedUriSan(
        "spiffe://huahuo/runtime-host/other-environment/runtime-host-1/runtime-instance-1",
        "prelaunch",
      ),
      (error: any) => error.code === "RUNTIME_HOST_UNAUTHORIZED",
    );

    // Enabled recovery startup is transactional with the ordinary Gateway:
    // recovery failure prevents the normal bind, and a normal-bind failure
    // closes an already-bound recovery listener before propagating the error.
    let primaryStarts = 0;
    let recoveryCloses = 0;
    const primaryFailure = new Error("primary gateway bind failed");
    await assert.rejects(
      () => startGatewayWithEnterpriseRuntimeRecovery({
        startRecoveryListener: async () => ({
          host: "172.18.102.92",
          port: 18792,
          recoveryConfigSha256: sha256("recovery-config"),
          close: async () => { recoveryCloses += 1; },
        }),
        startPrimaryGateway: async () => {
          primaryStarts += 1;
          throw primaryFailure;
        },
      }),
      (error: unknown) => error === primaryFailure,
    );
    assert.equal(primaryStarts, 1);
    assert.equal(recoveryCloses, 1);

    let forbiddenPrimaryStart = 0;
    await assert.rejects(
      () => startGatewayWithEnterpriseRuntimeRecovery({
        startRecoveryListener: async () => { throw new Error("recovery preflight failed"); },
        startPrimaryGateway: async () => { forbiddenPrimaryStart += 1; },
      }),
      /recovery preflight failed/,
    );
    assert.equal(forbiddenPrimaryStart, 0);
    assert.throws(
      () => recoveryPrincipalFromVerifiedUriSan(
        "spiffe://untrusted.example/runtime-host/prelaunch/runtime-host-1/runtime-instance-1",
        "prelaunch",
      ),
      (error: any) => error.code === "RUNTIME_HOST_UNAUTHORIZED",
    );

    const store = new EnterpriseRunStore(join(root, "enterprise-runs.sqlite"));
    const registry = new EnterpriseRunRegistry(store);
    const identity = {
      runId: "run-recovery-1",
      idempotencyKey: "idem-recovery-1",
      sessionBindingHash: sha256("session-recovery-1"),
      workspaceManifestHash: sha256("manifest-recovery-1"),
    };
    const acceptance = { jtiHash: sha256("jti-recovery-1"), dispatchIdentity: sha256("dispatch-recovery-1") };
    const runtimePolicy = policyFor(identity, acceptance.dispatchIdentity);
    const binding = {
      runtimeHostId: hostIdentity.runtimeHostId,
      reservationId: "reservation-recovery-1",
      fencingToken: 23,
      capabilityHash: runtimePolicy.capabilityHash,
    };
    registry.register(identity, acceptance, runtimePolicy, {
      workspaceId: "workspace-recovery-1",
      mount: { realPath: "/runtime/workspaces/run-recovery-1", accessMode: RUNTIME_WORKSPACE_ACCESS_READ },
    }, binding);

    // A run with only the submit-time binding cannot be mistaken for proven
    // restart occupancy. Scope and Backend-assigned process generation are
    // mandatory before a snapshot is exposed.
    assert.throws(
      () => registry.snapshotForAuthorizedHost(hostIdentity),
      (error: any) => error.code === "RUNTIME_CAPACITY_UNAVAILABLE",
    );

    const fact: EnterpriseRuntimeHostRecoveryFact = {
      runId: identity.runId,
      runtimeHostId: hostIdentity.runtimeHostId,
      assignedRuntimeHostInstanceId: hostIdentity.instanceId,
      assignedRuntimeHostInstanceGeneration: hostIdentity.instanceGeneration,
      reservationId: binding.reservationId,
      dispatchId: "dispatch-recovery-1",
      fencingToken: binding.fencingToken,
      executionScope: "product_thread",
      capabilityHash: binding.capabilityHash,
      dispatchIdentity: acceptance.dispatchIdentity,
      runTicketJtiHash: acceptance.jtiHash,
      manifestHash: identity.workspaceManifestHash,
    };
    const recorded = registry.recordAuthorizedRecoveryFact(fact);
    assert.equal(recorded.status, "accepted");
    assert.equal(recorded.lastEventSequence, 1);
    assert.deepEqual(registry.recordAuthorizedRecoveryFact(fact), recorded);
    assert.throws(
      () => registry.recordAuthorizedRecoveryFact({ ...fact, executionScope: "detached_task" }),
      (error: any) => error.code === "RUNTIME_RUN_CONFLICT",
    );

    const recoveryContext = createEnterpriseRuntimeRecoveryRequestContext({
      runtimeHostId: hostIdentity.runtimeHostId,
      instanceId: hostIdentity.instanceId,
      environment: hostIdentity.environment,
    });
    // No caller-controlled Host field can cross the URI-SAN principal boundary.
    assert.throws(
      () => handleEnterpriseRuntimeRecoverySnapshot({
        runtimeHostId: "runtime-host-other",
        instanceGeneration: hostIdentity.instanceGeneration,
        recoveryRevision: hostIdentity.recoveryRevision,
      }, recoveryContext, registry),
      (error: any) => error.code === "RUNTIME_HOST_UNAUTHORIZED",
    );
    assert.throws(
      () => handleEnterpriseRuntimeRecoverySnapshot({
        runtimeHostId: hostIdentity.runtimeHostId,
        instanceGeneration: hostIdentity.instanceGeneration + 1,
        recoveryRevision: hostIdentity.recoveryRevision,
      }, recoveryContext, registry),
      (error: any) => error.code === "RUNTIME_CAPACITY_UNAVAILABLE",
    );
    assert.throws(
      () => handleEnterpriseRuntimeRecoverySnapshot({
        runtimeHostId: hostIdentity.runtimeHostId,
        instanceGeneration: hostIdentity.instanceGeneration,
        recoveryRevision: hostIdentity.recoveryRevision,
      }, undefined, registry),
      (error: any) => error.code === "RUNTIME_PERMISSION_DENIED",
    );
    const authorizedSnapshot = handleEnterpriseRuntimeRecoverySnapshot({
      runtimeHostId: hostIdentity.runtimeHostId,
      instanceGeneration: hostIdentity.instanceGeneration,
      recoveryRevision: hostIdentity.recoveryRevision,
    }, recoveryContext, registry);
    assert.equal(authorizedSnapshot.runtimeHostId, hostIdentity.runtimeHostId);
    assert.equal(authorizedSnapshot.instanceId, hostIdentity.instanceId);
    assert.equal(authorizedSnapshot.instanceGeneration, hostIdentity.instanceGeneration);
    assert.equal(authorizedSnapshot.environment, hostIdentity.environment);

    let snapshot = registry.snapshotForAuthorizedHost(hostIdentity);
    const expectedAcceptedFact = {
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
      status: "accepted",
      lastEventSequence: 1,
    };
    assert.deepEqual(snapshot.facts, [expectedAcceptedFact]);
    assert.equal(snapshot.factSetHash, expectedHash([expectedAcceptedFact]));
    assert.throws(
      () => registry.snapshotForAuthorizedHost({ ...hostIdentity, instanceGeneration: hostIdentity.instanceGeneration + 1 }),
      (error: any) => error.code === "RUNTIME_CAPACITY_UNAVAILABLE",
    );

    registry.appendEvent(identity.runId, "run.started", "running");
    snapshot = registry.snapshotForAuthorizedHost(hostIdentity);
    const expectedRunningFact = { ...expectedAcceptedFact, status: "running", lastEventSequence: 2 };
    assert.deepEqual(snapshot.facts, [expectedRunningFact]);
    assert.equal(snapshot.factSetHash, expectedHash([expectedRunningFact]));

    registry.complete(identity.runId, "aborted", undefined, { code: "RUNTIME_ABORTED" });
    snapshot = registry.snapshotForAuthorizedHost(hostIdentity);
    assert.deepEqual(snapshot.facts, []);
    assert.equal(snapshot.factSetHash, expectedHash([]));
    store.close();

    const boundedStore = new EnterpriseRunStore(join(root, "bounded-enterprise-runs.sqlite"));
    const boundedRegistry = new EnterpriseRunRegistry(boundedStore);
    for (let index = 0; index <= 512; index += 1) {
      const suffix = String(index).padStart(3, "0");
      const boundedIdentity = {
        runId: `run-bounded-${suffix}`,
        idempotencyKey: `idem-bounded-${suffix}`,
        sessionBindingHash: sha256(`session-bounded-${suffix}`),
        workspaceManifestHash: sha256(`manifest-bounded-${suffix}`),
      };
      const boundedAcceptance = { jtiHash: sha256(`jti-bounded-${suffix}`), dispatchIdentity: sha256(`dispatch-bounded-${suffix}`) };
      const boundedPolicy = policyFor(boundedIdentity, boundedAcceptance.dispatchIdentity);
      boundedRegistry.register(boundedIdentity, boundedAcceptance, boundedPolicy, {
        workspaceId: "workspace-recovery-1",
        mount: { realPath: `/runtime/workspaces/run-bounded-${suffix}`, accessMode: RUNTIME_WORKSPACE_ACCESS_READ },
      }, {
        runtimeHostId: hostIdentity.runtimeHostId,
        reservationId: `reservation-bounded-${suffix}`,
        fencingToken: index + 1,
        capabilityHash: boundedPolicy.capabilityHash,
      });
    }
    assert.throws(
      () => boundedRegistry.snapshotForAuthorizedHost(hostIdentity),
      (error: any) => error.code === "RUNTIME_CAPACITY_UNAVAILABLE",
    );
    boundedStore.close();

    const gapPath = join(root, "gap-enterprise-runs.sqlite");
    const gapStore = new EnterpriseRunStore(gapPath);
    const gapRegistry = new EnterpriseRunRegistry(gapStore);
    const gapIdentity = {
      runId: "run-gap-1",
      idempotencyKey: "idem-gap-1",
      sessionBindingHash: sha256("session-gap-1"),
      workspaceManifestHash: sha256("manifest-gap-1"),
    };
    const gapAcceptance = { jtiHash: sha256("jti-gap-1"), dispatchIdentity: sha256("dispatch-gap-1") };
    const gapPolicy = policyFor(gapIdentity, gapAcceptance.dispatchIdentity);
    const gapBinding = { runtimeHostId: hostIdentity.runtimeHostId, reservationId: "reservation-gap-1", fencingToken: 701, capabilityHash: gapPolicy.capabilityHash };
    gapRegistry.register(gapIdentity, gapAcceptance, gapPolicy, {
      workspaceId: "workspace-recovery-1",
      mount: { realPath: "/runtime/workspaces/run-gap-1", accessMode: RUNTIME_WORKSPACE_ACCESS_READ },
    }, gapBinding);
    gapRegistry.recordAuthorizedRecoveryFact({
      runId: gapIdentity.runId,
      runtimeHostId: gapBinding.runtimeHostId,
      assignedRuntimeHostInstanceId: hostIdentity.instanceId,
      assignedRuntimeHostInstanceGeneration: hostIdentity.instanceGeneration,
      reservationId: gapBinding.reservationId,
      dispatchId: "dispatch-gap-1",
      fencingToken: gapBinding.fencingToken,
      executionScope: "detached_task",
      capabilityHash: gapBinding.capabilityHash,
      dispatchIdentity: gapAcceptance.dispatchIdentity,
      runTicketJtiHash: gapAcceptance.jtiHash,
      manifestHash: gapIdentity.workspaceManifestHash,
    });
    const mutator = new DatabaseSync(gapPath);
    mutator.prepare("delete from runtime_run_events where run_id=? and sequence=1").run(gapIdentity.runId);
    mutator.close();
    assert.throws(
      () => gapRegistry.snapshotForAuthorizedHost(hostIdentity),
      (error: any) => error.code === "RUNTIME_EVENT_GAP",
    );
    gapStore.close();

    const memoryRegistry = new EnterpriseRunRegistry(new EnterpriseRunStore(":memory:"));
    assert.throws(
      () => memoryRegistry.snapshotForAuthorizedHost(hostIdentity),
      (error: any) => error.code === "RUNTIME_STORAGE_UNAVAILABLE",
    );
    assert.throws(
      () => rejectUnverifiedRecoverySnapshotAccess(),
      (error: any) => error.code === "RUNTIME_PERMISSION_DENIED",
    );
  } finally {
    try { rmSync(root, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 }); }
    catch (error) {
      if (process.platform !== "win32" || (error as NodeJS.ErrnoException).code !== "EPERM") throw error;
    }
  }
}

void main().catch((error) => {
  process.nextTick(() => { throw error; });
});
