import assert from "node:assert/strict";
import { createHash, generateKeyPairSync, randomBytes, sign } from "node:crypto";
import { mkdtempSync, rmSync } from "node:fs";
import { createServer as createNetServer } from "node:net";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { connect as connectTls, type TLSSocket } from "node:tls";
import { EnterpriseRunRegistry, type EnterpriseRuntimeHostRecoveryFact } from "../src/enterprise-run-registry.ts";
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
  preflightEnterpriseRuntimeRecoveryGateway,
  rejectUnverifiedRecoverySnapshotAccess,
  type EnterpriseRuntimeRecoveryFileSystem,
  type EnterpriseRuntimeRecoveryListenerConfig,
  type EnterpriseRuntimeRecoverySecurityPolicy,
  startEnterpriseRuntimeRecoveryWss,
} from "../src/runtime-host-recovery.ts";

const RECOVERY_METHOD = "enterprise.runtime.recovery.snapshot";
const environment = "prelaunch";
const runtimeHostId = "runtime-host-mtls-e2e";
const instanceId = "runtime-instance-mtls-e2e";
const generation = 17;
const revision = 29;
const now = Math.floor(Date.now() / 1000);
const policyConfig = { keyId: "mtls-e2e-policy-v1", runTicketSecret: "runtime-mtls-e2e-policy-secret-0123456789" };

type KeyPair = { publicKey: Buffer; privateKey: string };
type TestCertificate = { certificate: string; privateKey: string };
type RecoveryCertificates = {
  ca: TestCertificate;
  server: TestCertificate;
  correctClient: TestCertificate;
  wrongSpiffeClient: TestCertificate;
  wrongEnvironmentClient: TestCertificate;
  duplicateUriSanClient: TestCertificate;
  foreignCaCorrectClient: TestCertificate;
};

type RecoveryPreflightFixture = {
  readonly config: EnterpriseRuntimeRecoveryListenerConfig;
  readonly securityPolicy: EnterpriseRuntimeRecoverySecurityPolicy;
  readonly fileSystem: EnterpriseRuntimeRecoveryFileSystem;
  readonly paths: {
    readonly configPath: string;
    readonly configDirectory: string;
    readonly secureRoot: string;
    readonly materialDirectory: string;
    readonly certPath: string;
    readonly keyPath: string;
    readonly caPath: string;
  };
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
    capabilityHash: "capability-mtls-e2e-v1",
    planHash: sha256("plan-mtls-e2e-v1"),
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
    workspaceId: "workspace-mtls-e2e-1",
    workspaceManifestHash: identity.workspaceManifestHash,
    dispatchIdentity,
    capabilityHash: unsigned.capabilityHash,
  }, policyConfig, now);
}

async function main(): Promise<void> {
  const root = mkdtempSync(join(tmpdir(), "huahuo-runtime-recovery-mtls-e2e-"));
  let listener: Awaited<ReturnType<typeof startEnterpriseRuntimeRecoveryWss>>;
  let store: EnterpriseRunStore | undefined;
  try {
    const certificates = createRecoveryCertificates();
    const disabledPort = await reserveLocalPort();
    // A missing or explicitly disabled config never probes its certificate
    // paths or binds a port. The ordinary Gateway must retain its separate
    // fail-closed recovery RPC boundary in either state.
    assert.equal(await startEnterpriseRuntimeRecoveryWss({ config: undefined }), undefined);
    assert.equal(await startEnterpriseRuntimeRecoveryWss({
      config: {
        enabled: false,
        host: "127.0.0.1",
        port: disabledPort,
        certPath: join(root, "must-not-be-read.crt"),
        keyPath: join(root, "must-not-be-read.key"),
        caPath: join(root, "must-not-be-read-ca.crt"),
        environment,
      },
    }), undefined);
    await assert.rejects(
      () => startEnterpriseRuntimeRecoveryWss({
        config: { enabled: true, host: "127.0.0.1", port: disabledPort, environment },
      }),
      (error: any) => error.code === "RUNTIME_HOST_UNAUTHORIZED",
    );
    const seeded = seedRecoverySnapshot(join(root, "enterprise-runs.sqlite"));
    store = seeded.store;
    const port = await reserveLocalPort();
    const fixture = createRecoveryPreflightFixture(root, certificates, port);
    const preflight = await preflightEnterpriseRuntimeRecoveryGateway({
      config: fixture.config,
      securityPolicy: fixture.securityPolicy,
      fileSystem: fixture.fileSystem,
    });
    assert.match(preflight?.recoveryConfigSha256 ?? "", /^sha256:[a-f0-9]{64}$/);
    assert.equal(preflight?.recoveryConfigSha256.includes(fixture.paths.keyPath), false);

    // Every enabled production listener is guarded by the same material
    // preflight as the normal Gateway startup injection. These synthetic
    // files exercise the contract without reading a host PEM or config.
    await assertPreflightRejected(createRecoveryPreflightFixture(root, certificates, port, {
      modes: new Map([[fixture.paths.secureRoot, 0o755]]),
    }), "secure-root-mode");
    await assertPreflightRejected(createRecoveryPreflightFixture(root, certificates, port, {
      uids: new Map([[fixture.paths.materialDirectory, 1000]]),
    }), "material-directory-owner");
    await assertPreflightRejected(createRecoveryPreflightFixture(root, certificates, port, {
      modes: new Map([[fixture.paths.keyPath, 0o644]]),
    }), "private-key-mode");
    await assertPreflightRejected(createRecoveryPreflightFixture(root, certificates, port, {
      symlinks: new Set([fixture.paths.configPath]),
    }), "config-symlink");
    await assertPreflightRejected(createRecoveryPreflightFixture(root, certificates, port, {
      modes: new Map([[fixture.paths.configDirectory, 0o775]]),
    }), "config-directory-mode");
    await assertPreflightRejected(createRecoveryPreflightFixture(root, certificates, port, {
      configText: JSON.stringify({ gateway: { enterpriseRuntimeRecovery: { ...fixture.config, environment: "other" } } }),
    }), "loaded-config-mismatch");
    await assertPreflightRejected(createRecoveryPreflightFixture(root, certificates, port, {
      contentOverrides: new Map([[fixture.paths.caPath, Buffer.from("not-a-certificate", "utf8")]]),
    }), "certificate-chain");
    await assertPreflightRejected(createRecoveryPreflightFixture(root, certificates, port, {
      configOverride: { host: "8.8.8.8" },
      securityPolicyOverride: { expectedHost: "8.8.8.8" },
    }), "public-listener-host");

    const logs: string[] = [];
    listener = await startEnterpriseRuntimeRecoveryWss({
      config: fixture.config,
      securityPolicy: fixture.securityPolicy,
      fileSystem: fixture.fileSystem,
      registry: seeded.registry,
      log: { info: (message) => logs.push(message) },
    });
    assert.ok(listener, "enabled recovery mTLS listener must bind");
    assert.equal(listener.port, port);
    assert.equal(listener.recoveryConfigSha256, preflight?.recoveryConfigSha256);
    assert.equal(logs.some((message) => message.includes(fixture.paths.keyPath) || message.includes("BEGIN")), false);

    // TLS is rejected before any HTTP or WebSocket request is accepted when a
    // peer cannot present a certificate trusted by the dedicated recovery CA.
    await assertClientCertificateRejected(port, certificates.ca.certificate, "without a client certificate");
    // A URI-SAN is not a trust anchor. A client with the exact expected
    // principal but a certificate from a foreign CA must fail at mTLS before
    // the listener can parse an HTTP upgrade or RPC payload.
    await assertClientCertificateRejected(
      port,
      certificates.ca.certificate,
      "with an untrusted client certificate",
      certificates.foreignCaCorrectClient,
    );

    // These peers are CA-trusted, so a 403 proves URI-SAN principal parsing is
    // performed after the real mTLS handshake instead of trusting caller JSON.
    assert.equal((await invokeRecovery(port, certificates, certificates.wrongSpiffeClient)).status, 403);
    assert.equal((await invokeRecovery(port, certificates, certificates.wrongEnvironmentClient)).status, 403);
    assert.equal((await invokeRecovery(port, certificates, certificates.duplicateUriSanClient)).status, 403);

    // A verified Host certificate is necessary but not sufficient. The
    // bounded RPC schema, its URI-SAN Host binding and the assigned instance
    // generation all remain fail-closed on the live TLS listener.
    assertRecoveryError(await invokeRecovery(port, certificates, certificates.correctClient, {
      id: "recovery-cross-host",
      method: RECOVERY_METHOD,
      params: { runtimeHostId: "runtime-host-other", instanceGeneration: generation, recoveryRevision: revision },
    }), "RUNTIME_HOST_UNAUTHORIZED");
    assertRecoveryError(await invokeRecovery(port, certificates, certificates.correctClient, {
      id: "recovery-stale-generation",
      method: RECOVERY_METHOD,
      params: { runtimeHostId, instanceGeneration: generation + 1, recoveryRevision: revision },
    }), "RUNTIME_CAPACITY_UNAVAILABLE");
    assertRecoveryError(await invokeRecovery(port, certificates, certificates.correctClient, {
      id: "recovery-extra-param",
      method: RECOVERY_METHOD,
      params: { runtimeHostId, instanceGeneration: generation, recoveryRevision: revision, callerHostId: runtimeHostId },
    }), "RUNTIME_INPUT_INVALID");
    assertRecoveryError(await invokeRecovery(port, certificates, certificates.correctClient, {
      id: "recovery-wrong-method",
      method: "enterprise.runtime.recovery.complete",
      params: { runtimeHostId, instanceGeneration: generation, recoveryRevision: revision },
    }), "RUNTIME_PERMISSION_DENIED");

    const response = await invokeRecovery(port, certificates, certificates.correctClient, {
      id: "recovery-mtls-e2e",
      method: RECOVERY_METHOD,
      params: { runtimeHostId, instanceGeneration: generation, recoveryRevision: revision },
    });
    assert.equal(response.status, 101);
    assert.equal(response.body?.id, "recovery-mtls-e2e");
    assert.equal(response.body?.ok, true);
    assert.equal(response.body?.payload?.version, "runtime-host-recovery.v1");
    assert.equal(response.body?.payload?.runtimeHostId, runtimeHostId);
    assert.equal(response.body?.payload?.instanceId, instanceId);
    assert.equal(response.body?.payload?.instanceGeneration, generation);
    assert.equal(response.body?.payload?.recoveryRevision, revision);
    assert.equal(response.body?.payload?.recoveryState, "pending");
    assert.equal(response.body?.payload?.environment, environment);
    assert.match(response.body?.payload?.factSetHash ?? "", /^sha256:[a-f0-9]{64}$/);
    assert.equal(response.body?.payload?.facts.length, 1);
    assert.deepEqual(response.body?.payload?.facts[0], {
      runId: "run-mtls-e2e-1",
      runtimeHostId,
      assignedRuntimeHostInstanceId: instanceId,
      assignedRuntimeHostInstanceGeneration: generation,
      reservationId: "reservation-mtls-e2e-1",
      dispatchId: "dispatch-mtls-e2e-1",
      fencingToken: 53,
      executionScope: "product_thread",
      capabilityHash: "capability-mtls-e2e-v1",
      dispatchIdentity: sha256("dispatch-mtls-e2e-1"),
      runTicketJtiHash: sha256("jti-mtls-e2e-1"),
      manifestHash: sha256("manifest-mtls-e2e-1"),
      status: "accepted",
      lastEventSequence: 1,
    });

    // A second recovery read is a read-only replay. It yields the exact same
    // durable proof instead of creating an admission, permit or model-loop
    // side effect in the Gateway listener.
    const replayEvidenceBefore = {
      status: seeded.registry.status("run-mtls-e2e-1"),
      events: seeded.registry.events("run-mtls-e2e-1", 0, 100),
    };
    const replay = await invokeRecovery(port, certificates, certificates.correctClient, {
      id: "recovery-mtls-e2e-replay",
      method: RECOVERY_METHOD,
      params: { runtimeHostId, instanceGeneration: generation, recoveryRevision: revision },
    });
    assert.equal(replay.status, 101);
    assert.equal(replay.body?.ok, true);
    assert.deepEqual(replay.body?.payload, response.body?.payload);
    assert.deepEqual(seeded.registry.status("run-mtls-e2e-1"), replayEvidenceBefore.status);
    assert.deepEqual(seeded.registry.events("run-mtls-e2e-1", 0, 100), replayEvidenceBefore.events);

    // The ordinary Gateway handler intentionally has no mTLS principal and
    // remains fail-closed even while the separate listener succeeds.
    assert.throws(
      () => rejectUnverifiedRecoverySnapshotAccess(),
      (error: any) => error.code === "RUNTIME_PERMISSION_DENIED",
    );

    // This is a deliberate graceful close/reopen, not a SIGKILL or power-loss
    // fault injection. It proves the local Store/Registry boundary does not
    // resurrect an old model loop after a new Registry instance starts.
    await listener?.close();
    listener = undefined;
    store?.close();
    store = undefined;
    const restartedStore = new EnterpriseRunStore(join(root, "enterprise-runs.sqlite"));
    const restartedRegistry = new EnterpriseRunRegistry(restartedStore);
    store = restartedStore;
    assert.equal(restartedRegistry.status("run-mtls-e2e-1").status, "orphaned");
    assert.equal((restartedRegistry.status("run-mtls-e2e-1").error as { code?: string } | undefined)?.code, "RUNTIME_RUN_ORPHANED");
    const restartedFixture = createRecoveryPreflightFixture(root, certificates, port);
    listener = await startEnterpriseRuntimeRecoveryWss({
      config: restartedFixture.config,
      securityPolicy: restartedFixture.securityPolicy,
      fileSystem: restartedFixture.fileSystem,
      registry: restartedRegistry,
    });
    const afterRestart = await invokeRecovery(port, certificates, certificates.correctClient, {
      id: "recovery-after-process-death",
      method: RECOVERY_METHOD,
      params: { runtimeHostId, instanceGeneration: generation, recoveryRevision: revision },
    });
    assert.equal(afterRestart.status, 101);
    assert.equal(afterRestart.body?.ok, true);
    assert.deepEqual(afterRestart.body?.payload?.facts, []);
    assert.equal(afterRestart.body?.payload?.factSetHash, sha256(JSON.stringify({ version: "runtime-host-recovery.v1", facts: [] })));
  } finally {
    await listener?.close();
    store?.close();
    try { rmSync(root, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 }); }
    catch (error) {
      if (process.platform !== "win32" || (error as NodeJS.ErrnoException).code !== "EPERM") throw error;
    }
  }
}

function assertRecoveryError(response: { status: number; body?: any }, code: string): void {
  assert.equal(response.status, 101);
  assert.equal(response.body?.ok, false);
  assert.equal(response.body?.error?.code, code);
  assert.equal(Object.hasOwn(response.body ?? {}, "payload"), false);
  assert.equal(Object.hasOwn(response.body ?? {}, "facts"), false);
}

function seedRecoverySnapshot(databasePath: string): { store: EnterpriseRunStore; registry: EnterpriseRunRegistry } {
  const store = new EnterpriseRunStore(databasePath);
  const registry = new EnterpriseRunRegistry(store);
  const identity = {
    runId: "run-mtls-e2e-1",
    idempotencyKey: "idem-mtls-e2e-1",
    sessionBindingHash: sha256("session-mtls-e2e-1"),
    workspaceManifestHash: sha256("manifest-mtls-e2e-1"),
  };
  const acceptance = { jtiHash: sha256("jti-mtls-e2e-1"), dispatchIdentity: sha256("dispatch-mtls-e2e-1") };
  const runtimePolicy = policyFor(identity, acceptance.dispatchIdentity);
  const binding = {
    runtimeHostId,
    reservationId: "reservation-mtls-e2e-1",
    fencingToken: 53,
    capabilityHash: runtimePolicy.capabilityHash,
  };
  registry.register(identity, acceptance, runtimePolicy, {
    workspaceId: "workspace-mtls-e2e-1",
    mount: { realPath: "/runtime/workspaces/run-mtls-e2e-1", accessMode: RUNTIME_WORKSPACE_ACCESS_READ },
  }, binding);
  const fact: EnterpriseRuntimeHostRecoveryFact = {
    runId: identity.runId,
    runtimeHostId,
    assignedRuntimeHostInstanceId: instanceId,
    assignedRuntimeHostInstanceGeneration: generation,
    reservationId: binding.reservationId,
    dispatchId: "dispatch-mtls-e2e-1",
    fencingToken: binding.fencingToken,
    executionScope: "product_thread",
    capabilityHash: binding.capabilityHash,
    dispatchIdentity: acceptance.dispatchIdentity,
    runTicketJtiHash: acceptance.jtiHash,
    manifestHash: identity.workspaceManifestHash,
  };
  assert.equal(registry.recordAuthorizedRecoveryFact(fact).status, "accepted");
  return { store, registry };
}

async function reserveLocalPort(): Promise<number> {
  return await new Promise<number>((resolve, reject) => {
    const server = createNetServer();
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      if (!address || typeof address === "string") {
        server.close(() => reject(new Error("local TCP listener did not expose a port")));
        return;
      }
      server.close((error) => error ? reject(error) : resolve(address.port));
    });
  });
}

async function assertClientCertificateRejected(
  port: number,
  ca: string,
  description: string,
  client?: TestCertificate,
): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    const socket = connectTls({
      host: "127.0.0.1",
      port,
      servername: "localhost",
      ca,
      rejectUnauthorized: true,
      minVersion: "TLSv1.3",
      ...(client ? { cert: client.certificate, key: client.privateKey } : {}),
    });
    let timer: NodeJS.Timeout | undefined;
    const fail = (error: Error) => {
      clearTimeout(timer);
      socket.destroy();
      reject(error);
    };
    const rejected = () => {
      clearTimeout(timer);
      socket.destroy();
      resolve();
    };
    timer = setTimeout(() => fail(new Error(`mTLS listener accepted a connection ${description}`)), 2_000);
    socket.once("error", rejected);
    socket.once("close", rejected);
    socket.once("secureConnect", () => {
      // Some TLS stacks emit secureConnect before receiving the server's fatal
      // certificate_required alert. A subsequent close/error is still required.
      socket.once("data", () => fail(new Error(`mTLS listener returned application data ${description}`)));
    });
  });
}

async function assertPreflightRejected(fixture: RecoveryPreflightFixture, label: string): Promise<void> {
  await assert.rejects(
    () => preflightEnterpriseRuntimeRecoveryGateway({
      config: fixture.config,
      securityPolicy: fixture.securityPolicy,
      fileSystem: fixture.fileSystem,
    }),
    (error: any) => {
      assert.equal(error?.code, "RUNTIME_HOST_UNAUTHORIZED", `${label} must fail closed`);
      assert.equal(String(error?.message ?? "").includes(fixture.paths.secureRoot), false, `${label} must not expose a path`);
      assert.equal(String(error?.message ?? "").includes("PRIVATE KEY"), false, `${label} must not expose key material`);
      return true;
    },
  );
}

async function invokeRecovery(
  port: number,
  certificates: RecoveryCertificates,
  client: TestCertificate,
  request?: { id: string; method: string; params: Record<string, unknown> },
): Promise<{ status: number; body?: any }> {
  const socket = await connectRecoveryClient(port, certificates.ca.certificate, client);
  return await new Promise<{ status: number; body?: any }>((resolve, reject) => {
    let settled = false;
    let buffered = Buffer.alloc(0);
    let headerLength = -1;
    let status: number | undefined;
    let sentRequest = false;
    const timer = setTimeout(() => done(new Error("recovery mTLS WebSocket response timed out")), 3_000);
    const done = (result: { status: number; body?: any } | Error) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      socket.destroy();
      if (result instanceof Error) reject(result);
      else resolve(result);
    };
    socket.on("error", (error) => done(error));
    socket.on("close", () => {
      if (!settled) done(new Error("recovery mTLS listener closed before a response"));
    });
    socket.on("data", (chunk) => {
      buffered = Buffer.concat([buffered, chunk]);
      if (headerLength < 0) {
        const separator = buffered.indexOf("\r\n\r\n");
        if (separator < 0) return;
        headerLength = separator + 4;
        const header = buffered.subarray(0, separator).toString("ascii");
        const match = /^HTTP\/1\.1\s+(\d{3})\b/.exec(header);
        if (!match) return done(new Error("recovery listener returned an invalid HTTP upgrade response"));
        status = Number(match[1]);
        if (status !== 101) return done({ status });
      }
      if (!sentRequest) {
        sentRequest = true;
        if (!request) return done(new Error("a successful recovery connection requires a request"));
        socket.write(maskedTextFrame(JSON.stringify(request)));
      }
      const body = decodeServerTextFrame(buffered.subarray(headerLength));
      if (!body) return;
      try { done({ status: status!, body: JSON.parse(body) }); }
      catch { done(new Error("recovery listener returned an invalid JSON frame")); }
    });
    socket.write([
      "GET /enterprise-runtime/recovery HTTP/1.1",
      "Host: localhost",
      "Upgrade: websocket",
      "Connection: Upgrade",
      `Sec-WebSocket-Key: ${randomBytes(16).toString("base64")}`,
      "Sec-WebSocket-Version: 13",
      "",
      "",
    ].join("\r\n"));
  });
}

async function connectRecoveryClient(port: number, ca: string, client: TestCertificate): Promise<TLSSocket> {
  return await new Promise<TLSSocket>((resolve, reject) => {
    const socket = connectTls({
      host: "127.0.0.1",
      port,
      servername: "localhost",
      ca,
      cert: client.certificate,
      key: client.privateKey,
      rejectUnauthorized: true,
      minVersion: "TLSv1.3",
    });
    socket.once("secureConnect", () => resolve(socket));
    socket.once("error", reject);
  });
}

function maskedTextFrame(text: string): Buffer {
  const payload = Buffer.from(text, "utf8");
  assert.ok(payload.length <= 8 * 1024, "test recovery request must fit listener limit");
  const header = payload.length < 126 ? Buffer.from([0x81, 0x80 | payload.length]) : Buffer.from([0x81, 0xfe, payload.length >> 8, payload.length & 0xff]);
  const mask = randomBytes(4);
  const body = Buffer.allocUnsafe(payload.length);
  for (let index = 0; index < payload.length; index += 1) body[index] = payload[index]! ^ mask[index % 4]!;
  return Buffer.concat([header, mask, body]);
}

function decodeServerTextFrame(buffer: Buffer): string | undefined {
  if (buffer.length < 2 || buffer[0] !== 0x81 || (buffer[1]! & 0x80) !== 0) return undefined;
  let length = buffer[1]! & 0x7f;
  let offset = 2;
  if (length === 126) {
    if (buffer.length < 4) return undefined;
    length = buffer.readUInt16BE(2);
    offset = 4;
  }
  if (length === 127 || buffer.length < offset + length) return undefined;
  return buffer.subarray(offset, offset + length).toString("utf8");
}

function createRecoveryCertificates(): RecoveryCertificates {
  const caKey = createKeyPair();
  const ca = issueCertificate({ subject: "huahuo-recovery-test-ca", publicKey: caKey.publicKey, signerKey: caKey.privateKey, issuer: "huahuo-recovery-test-ca", isCa: true });
  const foreignCaKey = createKeyPair();
  const foreignCa = issueCertificate({ subject: "foreign-recovery-test-ca", publicKey: foreignCaKey.publicKey, signerKey: foreignCaKey.privateKey, issuer: "foreign-recovery-test-ca", isCa: true });
  const issueLeaf = (subject: string, san: Buffer[]) => {
    const key = createKeyPair();
    return {
      certificate: issueCertificate({ subject, publicKey: key.publicKey, signerKey: caKey.privateKey, issuer: "huahuo-recovery-test-ca", isCa: false, san }),
      privateKey: key.privateKey,
    };
  };
  const foreignClientKey = createKeyPair();
  const foreignClientCertificate = issueCertificate({
    subject: "runtime-host-foreign-ca",
    publicKey: foreignClientKey.publicKey,
    signerKey: foreignCaKey.privateKey,
    issuer: "foreign-recovery-test-ca",
    isCa: false,
    san: [generalName(0x86, `spiffe://huahuo/runtime-host/${environment}/${runtimeHostId}/${instanceId}`)],
  });
  const foreignCaCorrectClient: TestCertificate = {
    certificate: `${foreignClientCertificate}${foreignCa}`,
    privateKey: foreignClientKey.privateKey,
  };
  return {
    ca: { certificate: ca, privateKey: caKey.privateKey },
    server: issueLeaf("localhost", [generalName(0x82, "localhost")]),
    correctClient: issueLeaf("runtime-host-correct", [generalName(0x86, `spiffe://huahuo/runtime-host/${environment}/${runtimeHostId}/${instanceId}`)]),
    wrongSpiffeClient: issueLeaf("runtime-host-wrong-spiffe", [generalName(0x86, `spiffe://untrusted.example/runtime-host/${environment}/${runtimeHostId}/${instanceId}`)]),
    wrongEnvironmentClient: issueLeaf("runtime-host-wrong-environment", [generalName(0x86, `spiffe://huahuo/runtime-host/production/${runtimeHostId}/${instanceId}`)]),
    duplicateUriSanClient: issueLeaf("runtime-host-duplicate-uri", [
      generalName(0x86, `spiffe://huahuo/runtime-host/${environment}/${runtimeHostId}/${instanceId}`),
      generalName(0x86, `spiffe://huahuo/runtime-host/${environment}/${runtimeHostId}/runtime-instance-duplicate`),
    ]),
    foreignCaCorrectClient,
  };
}

function createRecoveryPreflightFixture(
  root: string,
  certificates: RecoveryCertificates,
  port: number,
  options: {
    configOverride?: Partial<EnterpriseRuntimeRecoveryListenerConfig>;
    securityPolicyOverride?: Partial<EnterpriseRuntimeRecoverySecurityPolicy>;
    configText?: string;
    contentOverrides?: Map<string, Buffer>;
    modes?: Map<string, number>;
    uids?: Map<string, number>;
    symlinks?: Set<string>;
  } = {},
): RecoveryPreflightFixture {
  const secureRoot = resolve(root, "secure-env");
  const materialDirectory = resolve(secureRoot, "runtime-host-mtls-test");
  const configPath = resolve(root, "gateway-config", "openclaw-gateway.json");
  const configDirectory = dirname(configPath);
  const certPath = resolve(materialDirectory, "host.crt");
  const keyPath = resolve(materialDirectory, "host.key");
  const caPath = resolve(materialDirectory, "ca.crt");
  const config: EnterpriseRuntimeRecoveryListenerConfig = {
    enabled: true,
    host: "127.0.0.1",
    port,
    certPath,
    keyPath,
    caPath,
    environment,
    ...options.configOverride,
  };
  const policy: EnterpriseRuntimeRecoverySecurityPolicy = {
    gatewayConfigPath: configPath,
    approvedSecureRoot: secureRoot,
    expectedHost: config.host ?? "",
    expectedPort: config.port ?? 0,
    expectedEnvironment: config.environment ?? "",
    ...options.securityPolicyOverride,
  };
  const contents = new Map<string, Buffer>([
    [configPath, Buffer.from(options.configText ?? JSON.stringify({ gateway: { enterpriseRuntimeRecovery: config } }), "utf8")],
    [certPath, Buffer.from(certificates.server.certificate, "utf8")],
    [keyPath, Buffer.from(certificates.server.privateKey, "utf8")],
    [caPath, Buffer.from(certificates.ca.certificate, "utf8")],
  ]);
  for (const [path, content] of options.contentOverrides ?? []) contents.set(resolve(path), Buffer.from(content));
  const directories = new Set([secureRoot, materialDirectory, configDirectory]);
  const modes = new Map<string, number>([
    [secureRoot, 0o700],
    [materialDirectory, 0o700],
    [configDirectory, 0o700],
    [configPath, 0o644],
    [certPath, 0o644],
    [keyPath, 0o600],
    [caPath, 0o644],
  ]);
  for (const [path, mode] of options.modes ?? []) modes.set(resolve(path), mode);
  const uids = new Map<string, number>();
  for (const [path, uid] of options.uids ?? []) uids.set(resolve(path), uid);
  const symlinks = new Set([...options.symlinks ?? []].map((path) => resolve(path)));
  const fileSystem: EnterpriseRuntimeRecoveryFileSystem = {
    readFile: async (path) => {
      const value = contents.get(resolve(path));
      if (!value) throw new Error("fixture file is unavailable");
      return Buffer.from(value);
    },
    lstat: async (path) => {
      const canonical = resolve(path);
      const symbolic = symlinks.has(canonical);
      if (directories.has(canonical)) {
        return {
          uid: uids.get(canonical) ?? 0,
          mode: modes.get(canonical) ?? 0o700,
          isFile: () => false,
          isDirectory: () => true,
          isSymbolicLink: () => symbolic,
        };
      }
      if (contents.has(canonical)) {
        return {
          uid: uids.get(canonical) ?? 0,
          mode: modes.get(canonical) ?? 0o644,
          isFile: () => true,
          isDirectory: () => false,
          isSymbolicLink: () => symbolic,
        };
      }
      throw new Error("fixture path is unavailable");
    },
    realpath: async (path) => resolve(path),
  };
  return {
    config,
    securityPolicy: policy,
    fileSystem,
    paths: { configPath, configDirectory, secureRoot, materialDirectory, certPath, keyPath, caPath },
  };
}

function createKeyPair(): KeyPair {
  const pair = generateKeyPairSync("rsa", {
    modulusLength: 2048,
    publicKeyEncoding: { type: "spki", format: "der" },
    privateKeyEncoding: { type: "pkcs8", format: "pem" },
  });
  return { publicKey: pair.publicKey, privateKey: pair.privateKey };
}

// Minimal RFC 5280 encoder for ephemeral tests. It emits only the extensions
// Node TLS needs for a CA, a localhost server, and mTLS client URI-SANs.
function issueCertificate(input: { subject: string; issuer: string; publicKey: Buffer; signerKey: string; isCa: boolean; san?: Buffer[] }): string {
  const algorithm = sequence(oid("1.2.840.113549.1.1.11"), der(0x05, Buffer.alloc(0)));
  const extensions = [
    extension("2.5.29.19", sequence(booleanDer(input.isCa)), true),
    extension("2.5.29.15", der(0x03, Buffer.from(input.isCa ? [0x02, 0x04] : [0x07, 0x80])), true),
  ];
  if (!input.isCa) extensions.push(extension("2.5.29.37", sequence(oid("1.3.6.1.5.5.7.3.1"), oid("1.3.6.1.5.5.7.3.2"))));
  if (input.san?.length) extensions.push(extension("2.5.29.17", sequence(...input.san)));
  const validity = sequence(utcTime(new Date(Date.now() - 60_000)), utcTime(new Date(Date.now() + 24 * 60 * 60_000)));
  const tbs = sequence(
    der(0xa0, integer(2)),
    integer(randomBytes(12)),
    algorithm,
    distinguishedName(input.issuer),
    validity,
    distinguishedName(input.subject),
    input.publicKey,
    der(0xa3, sequence(...extensions)),
  );
  const signature = sign("sha256", tbs, input.signerKey);
  return toPem("CERTIFICATE", sequence(tbs, algorithm, der(0x03, Buffer.concat([Buffer.from([0]), signature]))));
}

function extension(identifier: string, value: Buffer, critical = false): Buffer {
  return sequence(oid(identifier), ...(critical ? [booleanDer(true)] : []), der(0x04, value));
}

function distinguishedName(commonName: string): Buffer {
  return sequence(der(0x31, sequence(oid("2.5.4.3"), der(0x0c, Buffer.from(commonName, "utf8")))));
}

function generalName(tag: number, value: string): Buffer {
  return der(tag, Buffer.from(value, "ascii"));
}

function utcTime(value: Date): Buffer {
  const two = (part: number) => String(part).padStart(2, "0");
  return der(0x17, Buffer.from(`${two(value.getUTCFullYear() % 100)}${two(value.getUTCMonth() + 1)}${two(value.getUTCDate())}${two(value.getUTCHours())}${two(value.getUTCMinutes())}${two(value.getUTCSeconds())}Z`, "ascii"));
}

function booleanDer(value: boolean): Buffer {
  return der(0x01, Buffer.from([value ? 0xff : 0x00]));
}

function integer(value: number | Buffer): Buffer {
  let body: Buffer;
  if (typeof value === "number") {
    const bytes: number[] = [];
    let remaining = value;
    do { bytes.unshift(remaining & 0xff); remaining >>>= 8; } while (remaining > 0);
    body = Buffer.from(bytes);
  } else {
    body = Buffer.from(value);
    while (body.length > 1 && body[0] === 0) body = body.subarray(1);
  }
  if ((body[0]! & 0x80) !== 0) body = Buffer.concat([Buffer.from([0]), body]);
  return der(0x02, body);
}

function oid(value: string): Buffer {
  const parts = value.split(".").map((part) => Number(part));
  assert.equal(parts.length >= 2, true);
  const bytes = [40 * parts[0]! + parts[1]!];
  for (const part of parts.slice(2)) {
    const encoded = [part & 0x7f];
    for (let remaining = part >>> 7; remaining > 0; remaining >>>= 7) encoded.unshift(0x80 | (remaining & 0x7f));
    bytes.push(...encoded);
  }
  return der(0x06, Buffer.from(bytes));
}

function sequence(...items: Buffer[]): Buffer {
  return der(0x30, Buffer.concat(items));
}

function der(tag: number, body: Buffer): Buffer {
  return Buffer.concat([Buffer.from([tag]), derLength(body.length), body]);
}

function derLength(length: number): Buffer {
  if (length < 128) return Buffer.from([length]);
  const bytes: number[] = [];
  for (let remaining = length; remaining > 0; remaining >>>= 8) bytes.unshift(remaining & 0xff);
  return Buffer.from([0x80 | bytes.length, ...bytes]);
}

function toPem(label: string, derValue: Buffer): string {
  const encoded = derValue.toString("base64").replace(/.{1,64}/g, "$&\n");
  return `-----BEGIN ${label}-----\n${encoded}-----END ${label}-----\n`;
}

void main().catch((error) => {
  process.nextTick(() => { throw error; });
});
