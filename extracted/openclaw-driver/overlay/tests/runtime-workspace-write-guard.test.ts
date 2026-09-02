import assert from "node:assert/strict";
import { mkdir, mkdtemp, readFile, rm, symlink, writeFile } from "node:fs/promises";
import { dirname, isAbsolute, join, relative, resolve, sep } from "node:path";
import test from "node:test";
import { EnterpriseRunRegistry, runtimeToolArgsHash, stableIdentityHash } from "../src/enterprise-run-registry.ts";
import {
  DEFAULT_RUNTIME_TOOL_BUDGET,
  RUNTIME_POLICY_ALGORITHM,
  RUNTIME_POLICY_VERSION,
  RUNTIME_STAGING_WRITE_ROOTS,
  RUNTIME_WORKSPACE_ACCESS_READ,
  RUNTIME_WORKSPACE_ACCESS_WRITE,
  RUNTIME_WORKSPACE_WRITE_LEASE_VERSION,
  assertRuntimeWorkspaceMount,
  signRuntimePolicy,
  verifyRuntimePolicy,
  type RuntimePolicyEnvelope,
} from "../src/runtime-policy.ts";

const now = Math.floor(Date.now() / 1000);
const policyConfig = { keyId: "write-guard-test", runTicketSecret: "write-guard-test-secret-0123456789" };
const workspaceId = "workspace_write_guard";
const identity = {
  runId: "run_write_guard",
  idempotencyKey: "idem_write_guard",
  sessionBindingHash: stableIdentityHash("session_write_guard"),
  workspaceManifestHash: stableIdentityHash("manifest_write_guard"),
};
const acceptance = {
  jtiHash: stableIdentityHash("jti_write_guard"),
  dispatchIdentity: stableIdentityHash("dispatch_write_guard"),
};

test("workspace mount parser is an exact read/write discriminated union", () => {
  const readPolicy = verifiedPolicy("read");
  const readMount = { realPath: "/var/lib/huahuo/runtime-workspaces/run_read", accessMode: RUNTIME_WORKSPACE_ACCESS_READ } as const;
  assert.deepEqual(assertRuntimeWorkspaceMount(readMount, readPolicy, workspaceId, now), readMount);

  for (const invalid of [
    { ...readMount, writeLease: null },
    { ...readMount, writeLease: writeLease() },
    { realPath: readMount.realPath, accessMode: RUNTIME_WORKSPACE_ACCESS_WRITE },
    { realPath: readMount.realPath, accessMode: RUNTIME_WORKSPACE_ACCESS_WRITE, writeLease: null },
    { ...readMount, unexpected: true },
  ]) {
    assert.throws(
      () => assertRuntimeWorkspaceMount(invalid, readPolicy, workspaceId, now),
      (error: any) => error.code === "RUNTIME_INPUT_INVALID" || error.code === "RUNTIME_PERMISSION_DENIED",
    );
  }
});

test("real tool pre-execution guard permits only leased output/staging writes", async (context) => {
  const workspace = await createTemporaryWorkspace(context);
  const policy = verifiedPolicy("write");
  const mount = {
    realPath: workspace.mount.realPath,
    accessMode: RUNTIME_WORKSPACE_ACCESS_WRITE,
    writeLease: writeLease(),
  } as const;
  const registry = new EnterpriseRunRegistry();
  assert.deepEqual(registry.register(identity, acceptance, policy, { workspaceId, mount }), { created: true, status: "accepted" });

  const directWriteArgs = { path: "output/result.md", content: "private-body-not-persisted" };
  registry.assertToolCallAllowed(identity.runId, {
    toolName: "write",
    toolCallId: "write_output",
    args: directWriteArgs,
  });
  registry.recordToolOutcome(identity.runId, {
    toolName: "write",
    toolCallId: "write_output",
    argsHash: runtimeToolArgsHash(directWriteArgs),
    resultHash: stableIdentityHash("write_succeeded"),
    progress: true,
  });

  assert.throws(
    () => registry.assertToolCallAllowed(identity.runId, {
      toolName: "write",
      toolCallId: "write_escape",
      args: { path: "../output/escape.md", content: "escape-body-not-persisted" },
    }),
    (error: any) => error.code === "RUNTIME_INPUT_INVALID" || error.code === "RUNTIME_PERMISSION_DENIED",
  );
  assert.equal(registry.status(identity.runId).status, "aborting");
  const persisted = JSON.stringify({ status: registry.status(identity.runId), events: registry.events(identity.runId) });
  assert.doesNotMatch(persisted, /private-body-not-persisted|escape-body-not-persisted|\.\.\/output/);
  assert.match(persisted, /run_write_guard/);
});

test("accepted run cannot swap its process-local workspace mount", () => {
  const policy = verifiedPolicy("write");
  const mount = {
    realPath: "/var/lib/huahuo/runtime-workspaces/run_write_guard",
    accessMode: RUNTIME_WORKSPACE_ACCESS_WRITE,
    writeLease: writeLease(),
  } as const;
  const registry = new EnterpriseRunRegistry();
  registry.register(identity, acceptance, policy, { workspaceId, mount });
  assert.throws(
    () => registry.register(identity, acceptance, policy, {
      workspaceId,
      mount: { ...mount, realPath: "/var/lib/huahuo/runtime-workspaces/other" },
    }),
    (error: any) => error.code === "RUNTIME_RUN_CONFLICT",
  );
});

test("accepted writable run aborts when its lease expires before native write execution", (context) => {
  const expiresAt = now + 30;
  const policy = verifiedPolicy("write", expiresAt);
  const mount = {
    realPath: "/var/lib/huahuo/runtime-workspaces/run_write_guard",
    accessMode: RUNTIME_WORKSPACE_ACCESS_WRITE,
    writeLease: writeLease(expiresAt),
  } as const;
  const registry = new EnterpriseRunRegistry();
  registry.register(identity, acceptance, policy, { workspaceId, mount });

  context.mock.method(Date, "now", () => (expiresAt + 1) * 1000);
  assert.throws(
    () => registry.assertToolCallAllowed(identity.runId, {
      toolName: "write",
      toolCallId: "write_after_lease_expiry",
      args: { path: "output/late.md", content: "expired-content-not-persisted" },
    }),
    (error: any) => error.code === "RUNTIME_PERMISSION_DENIED",
  );

  const status = registry.status(identity.runId);
  assert.equal(status.status, "aborting");
  assert.equal(status.abortCode, "RUNTIME_PERMISSION_DENIED");
  const persisted = JSON.stringify({ status, events: registry.events(identity.runId) });
  assert.match(persisted, /"toolCallHash":"sha256:/);
  assert.doesNotMatch(persisted, /workspace_write_guard|output\/late\.md|expired-content-not-persisted|write_after_lease_expiry/);
});

test("real filesystem write executes only after the pre-execution guard admits an output or staging path", async (context) => {
  const workspace = await createTemporaryWorkspace(context);
  const policy = verifiedPolicy("write");
  const registry = registerWorkspaceRun(policy, workspace.mount);

  const output = await nativeWriteAfterGuard(registry, workspace, "write_actual_output", "output/result.md", "output-body");
  const staging = await nativeWriteAfterGuard(registry, workspace, "write_actual_staging", "staging/drafts/result.json", "staging-body");

  assert.equal(await readFile(output, "utf8"), "output-body");
  assert.equal(await readFile(staging, "utf8"), "staging-body");
  assert.equal(relative(workspace.root, output).split(sep).join("/"), "output/result.md");
  assert.equal(relative(workspace.root, staging).split(sep).join("/"), "staging/drafts/result.json");
});

test("read-only mount rejects a native write before any filesystem mutation", async (context) => {
  const workspace = await createTemporaryWorkspace(context);
  const registry = registerWorkspaceRun(verifiedPolicy("read"), workspace.readMount);

  await assert.rejects(
    nativeWriteAfterGuard(registry, workspace, "write_read_only", "output/rejected.md", "must-not-write"),
    hasRuntimeCode("RUNTIME_PERMISSION_DENIED"),
  );
  await assertPathMissing(join(workspace.root, "output", "rejected.md"));
  assertAbortState(registry, "RUNTIME_PERMISSION_DENIED");
});

test("native write rejects non-staging roots and path traversal before mutation", async (context) => {
  const rootEscape = await createTemporaryWorkspace(context);
  const rootRegistry = registerWorkspaceRun(verifiedPolicy("write"), rootEscape.mount);
  await assert.rejects(
    nativeWriteAfterGuard(rootRegistry, rootEscape, "write_materials", "materials/private.md", "must-not-write"),
    hasRuntimeCode("RUNTIME_PERMISSION_DENIED"),
  );
  await assertPathMissing(join(rootEscape.root, "materials", "private.md"));
  assertAbortState(rootRegistry, "RUNTIME_PERMISSION_DENIED");

  const traversal = await createTemporaryWorkspace(context);
  const traversalRegistry = registerWorkspaceRun(verifiedPolicy("write"), traversal.mount);
  await assert.rejects(
    nativeWriteAfterGuard(traversalRegistry, traversal, "write_traversal", "output/../outside.md", "must-not-write"),
    hasRuntimeCode("RUNTIME_PERMISSION_DENIED"),
  );
  await assertPathMissing(join(traversal.root, "outside.md"));
  assertAbortState(traversalRegistry, "RUNTIME_PERMISSION_DENIED");
});

test("native write rejects an allowed path whose parent is a symlink or Windows junction", async (context) => {
  const workspace = await createTemporaryWorkspace(context);
  const localTmp = resolve("/tmp");
  const outside = await mkdtemp(join(localTmp, "huahuo-runtime-write-guard-outside-"));
  context.after(async () => { await rm(outside, { recursive: true, force: true, maxRetries: 3 }); });
  await symlink(outside, join(workspace.root, "output", "escape-link"), process.platform === "win32" ? "junction" : "dir");

  const registry = registerWorkspaceRun(verifiedPolicy("write"), workspace.mount);
  await assert.rejects(
    nativeWriteAfterGuard(registry, workspace, "write_symlink_escape", "output/escape-link/leaked.md", "must-not-write"),
    hasRuntimeCode("RUNTIME_PERMISSION_DENIED"),
  );
  await assertPathMissing(join(outside, "leaked.md"));
  assertAbortState(registry, "RUNTIME_PERMISSION_DENIED");
});

test("native write rejects a workspace mount reached through a symlink or Windows junction", async (context) => {
  const workspace = await createTemporaryWorkspace(context);
  const alias = join(resolve("/tmp"), `huahuo-runtime-write-guard-alias-${Date.now()}-${Math.random().toString(16).slice(2)}`);
  await symlink(workspace.root, alias, process.platform === "win32" ? "junction" : "dir");
  context.after(async () => { await rm(alias, { recursive: true, force: true, maxRetries: 3 }); });

  const registry = registerWorkspaceRun(verifiedPolicy("write"), {
    ...workspace.mount,
    realPath: runtimeMountPath(alias),
  });
  await assert.rejects(
    nativeWriteAfterGuard(registry, workspace, "write_mount_symlink", "output/rejected.md", "must-not-write"),
    hasRuntimeCode("RUNTIME_PERMISSION_DENIED"),
  );
  await assertPathMissing(join(workspace.root, "output", "rejected.md"));
  assertAbortState(registry, "RUNTIME_PERMISSION_DENIED");
});

test("admitted write is rechecked at native execution time after lease expiry", async (context) => {
  const workspace = await createTemporaryWorkspace(context);
  const expiresAt = now + 30;
  const registry = registerWorkspaceRun(verifiedPolicy("write", expiresAt), workspace.mountWithLease(expiresAt));

  context.mock.method(Date, "now", () => (expiresAt + 1) * 1000);
  await assert.rejects(
    nativeWriteAfterGuard(registry, workspace, "write_expired_native", "output/late.md", "must-not-write"),
    hasRuntimeCode("RUNTIME_PERMISSION_DENIED"),
  );
  await assertPathMissing(join(workspace.root, "output", "late.md"));
  assertAbortState(registry, "RUNTIME_PERMISSION_DENIED");
});

test("duplicate and already-aborted native write calls never reach the filesystem", async (context) => {
  const workspace = await createTemporaryWorkspace(context);
  const registry = registerWorkspaceRun(verifiedPolicy("write"), workspace.mount);
  const first = await nativeWriteAfterGuard(registry, workspace, "write_duplicate", "output/first.md", "first-body");

  await assert.rejects(
    nativeWriteAfterGuard(registry, workspace, "write_duplicate", "output/second.md", "must-not-write"),
    hasRuntimeCode("RUNTIME_PERMISSION_DENIED"),
  );
  assert.equal(await readFile(first, "utf8"), "first-body");
  await assertPathMissing(join(workspace.root, "output", "second.md"));
  assertAbortState(registry, "RUNTIME_PERMISSION_DENIED");

  await assert.rejects(
    nativeWriteAfterGuard(registry, workspace, "write_after_abort", "output/third.md", "must-not-write"),
    hasRuntimeCode("RUNTIME_PERMISSION_DENIED"),
  );
  await assertPathMissing(join(workspace.root, "output", "third.md"));
});

function writeLease(expiresAt = now + 300) {
  return {
    version: RUNTIME_WORKSPACE_WRITE_LEASE_VERSION,
    runId: identity.runId,
    workspaceId,
    workspaceManifestHash: identity.workspaceManifestHash,
    allowedRoots: [...RUNTIME_STAGING_WRITE_ROOTS],
    expiresAt,
  };
}

function verifiedPolicy(mode: "read" | "write", expiresAt = now + 300) {
  const write = mode === "write";
  const unsigned: Omit<RuntimePolicyEnvelope, "signature"> = {
    version: RUNTIME_POLICY_VERSION,
    algorithm: RUNTIME_POLICY_ALGORITHM,
    keyId: policyConfig.keyId,
    runId: identity.runId,
    idempotencyKey: identity.idempotencyKey,
    workspaceManifestHash: identity.workspaceManifestHash,
    dispatchIdentity: acceptance.dispatchIdentity,
    capabilityHash: "capability-write-guard",
    planHash: stableIdentityHash("plan_write_guard"),
    issuedAt: now - 1,
    expiresAt,
    workspaceAccessMode: write ? RUNTIME_WORKSPACE_ACCESS_WRITE : RUNTIME_WORKSPACE_ACCESS_READ,
    writeLease: write ? writeLease(expiresAt) : null,
    requiredTools: write ? ["read", "write"] : ["read"],
    allowedTools: write ? ["read", "write"] : ["read"],
    toolBudget: { ...DEFAULT_RUNTIME_TOOL_BUDGET },
  };
  return verifyRuntimePolicy(signRuntimePolicy(unsigned, policyConfig.runTicketSecret), {
    runId: identity.runId,
    idempotencyKey: identity.idempotencyKey,
    workspaceId,
    workspaceManifestHash: identity.workspaceManifestHash,
    dispatchIdentity: acceptance.dispatchIdentity,
    capabilityHash: unsigned.capabilityHash,
  }, policyConfig, now);
}

type TemporaryWorkspace = {
  root: string;
  mount: { realPath: string; accessMode: typeof RUNTIME_WORKSPACE_ACCESS_WRITE; writeLease: ReturnType<typeof writeLease> };
  readMount: { realPath: string; accessMode: typeof RUNTIME_WORKSPACE_ACCESS_READ };
  mountWithLease: (expiresAt: number) => { realPath: string; accessMode: typeof RUNTIME_WORKSPACE_ACCESS_WRITE; writeLease: ReturnType<typeof writeLease> };
};

async function createTemporaryWorkspace(context: test.TestContext): Promise<TemporaryWorkspace> {
  // The signed Runtime contract is POSIX because production runs on Linux. On
  // Windows, Node maps /tmp to the current drive's root, so keep a POSIX mount
  // value for the real guard and the matching native path for actual writes.
  const localTmp = resolve("/tmp");
  await mkdir(localTmp, { recursive: true });
  const root = await mkdtemp(join(localTmp, "huahuo-runtime-write-guard-"));
  await Promise.all([mkdir(join(root, "output"), { recursive: true }), mkdir(join(root, "staging"), { recursive: true })]);
  context.after(async () => { await rm(root, { recursive: true, force: true, maxRetries: 3 }); });

  const realPath = runtimeMountPath(root);
  return {
    root,
    mount: { realPath, accessMode: RUNTIME_WORKSPACE_ACCESS_WRITE, writeLease: writeLease() },
    readMount: { realPath, accessMode: RUNTIME_WORKSPACE_ACCESS_READ },
    mountWithLease: (expiresAt) => ({ realPath, accessMode: RUNTIME_WORKSPACE_ACCESS_WRITE, writeLease: writeLease(expiresAt) }),
  };
}

function runtimeMountPath(root: string): string {
  const relativeRoot = relative(resolve("/tmp"), root);
  assert.ok(relativeRoot && !relativeRoot.startsWith("..") && !isAbsolute(relativeRoot), "temporary workspace must remain below /tmp");
  return `/tmp/${relativeRoot.split(sep).join("/")}`;
}

function registerWorkspaceRun(policy: ReturnType<typeof verifiedPolicy>, mount: TemporaryWorkspace["mount"] | TemporaryWorkspace["readMount"]) {
  const registry = new EnterpriseRunRegistry();
  assert.deepEqual(registry.register(identity, acceptance, policy, { workspaceId, mount }), { created: true, status: "accepted" });
  return registry;
}

async function nativeWriteAfterGuard(
  registry: EnterpriseRunRegistry,
  workspace: TemporaryWorkspace,
  toolCallId: string,
  relativePath: string,
  content: string,
): Promise<string> {
  // This deliberately calls the production Registry pre-execution boundary
  // before invoking Node's real filesystem write; no successful write is
  // mocked or synthesized by the test.
  const args = { path: relativePath, content };
  registry.assertToolCallAllowed(identity.runId, {
    toolName: "write",
    toolCallId,
    args,
  });
  const target = join(workspace.root, ...relativePath.split("/"));
  await mkdir(dirname(target), { recursive: true });
  await writeFile(target, content, "utf8");
  registry.recordToolOutcome(identity.runId, {
    toolName: "write",
    toolCallId,
    argsHash: runtimeToolArgsHash(args),
    resultHash: stableIdentityHash({ relativePath, bytes: Buffer.byteLength(content, "utf8") }),
    progress: true,
  });
  return target;
}

function hasRuntimeCode(expected: string) {
  return (error: unknown) => Boolean(error && typeof error === "object" && (error as { code?: unknown }).code === expected);
}

async function assertPathMissing(path: string): Promise<void> {
  await assert.rejects(readFile(path), (error: any) => error?.code === "ENOENT");
}

function assertAbortState(registry: EnterpriseRunRegistry, expectedCode: string): void {
  const status = registry.status(identity.runId);
  assert.equal(status.status, "aborting");
  assert.equal(status.abortCode, expectedCode);
}
