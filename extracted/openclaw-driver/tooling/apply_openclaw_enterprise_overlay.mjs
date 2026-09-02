import { createHash, randomUUID } from "node:crypto";
import { readFile, lstat, mkdir, readdir, rename, rm, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

export const EXPECTED_OPENCLAW_VERSION = "2026.6.2";
export const BUNDLE_SCHEMA = "huahuo.openclaw-enterprise-overlay.bundle.v1";

// These are the only reviewed transitional states from the early V1 Overlay
// publication. They are deliberately source-pinned here rather than learned
// from a Runtime root or accepted from a caller-supplied bundle. A target in
// any other state remains fail-closed as OVERLAY_SOURCE_DRIFT.
export const LEGACY_ACCEPTED_BEFORE_HASHES = Object.freeze({
  "src/enterprise-runtime/enterprise-run-registry.ts": Object.freeze([
    "sha256:bc170980804f2f44d47eb5c4f8acd3f2994fd635c47e1042dae8fb44d3f03683",
  ]),
  "src/enterprise-runtime/enterprise-run-store.ts": Object.freeze([
    "sha256:cb00411ed2e13b598bc203fed424b85c5abab5ae493d06370aa9b06ea678eca8",
  ]),
  "src/enterprise-runtime/runtime-capability-handshake.ts": Object.freeze([
    "sha256:38f490f2f54566ef2546824d2d334b4d596f930f860bb4a861f3cb414446eaaa",
  ]),
  "src/enterprise-runtime/runtime-host-recovery.ts": Object.freeze([
    "sha256:78defa3b2448fe7417a361055e26b8ab96443d9d3eed8f1510d1dd54ca6eb0be",
  ]),
  "src/enterprise-runtime/runtime-policy.ts": Object.freeze([
    "sha256:8273e0a3273ed3a449b66b67d30acd8002443847cbbddb4b8dab9dbbea3c3928",
  ]),
  "src/gateway/server-methods/enterprise-runtime-methods.ts": Object.freeze([
    "sha256:fde7a9a2b892c5e020af464abe4d618f203432766218e252a0500e46825ac1fd",
  ]),
  "src/gateway/server-methods/faya-status-result-compat.ts": Object.freeze([
    "sha256:59c1c713b3f3804d68bbedad4215520e4ebd3e166300d90ef5dbeddff3383740",
  ]),
  "src/gateway/server-methods/private-run-context.ts": Object.freeze([
    "sha256:fd9f566c44cabb599267afc59f076a039f1d0a81f5e757a845f2bc1441790022",
  ]),
});

const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const DEFAULT_INSTALLER_PATH = path.join(scriptDir, "apply_openclaw_enterprise_overlay.ps1");
const OVERLAY_REQUIRED_FILES = [
  "src/enterprise-run-store.ts",
  "src/enterprise-run-registry.ts",
  "src/runtime-policy.ts",
  "src/runtime-host-recovery.ts",
  "src/private-run-context.ts",
  "src/enterprise-runtime-methods.ts",
  "src/faya-status-result-compat.ts",
  "src/thoughts-response-status-compat.ts",
  "src/runtime-capability-handshake.ts",
  "tests/enterprise-runtime-async.test.ts",
  "tests/thoughts-response-status-compat.test.ts",
  "tests/runtime-capability-handshake.test.ts",
];

const COPIED_OVERLAY_FILES = new Map([
  ["src/enterprise-run-store.ts", "src/enterprise-runtime/enterprise-run-store.ts"],
  ["src/enterprise-run-registry.ts", "src/enterprise-runtime/enterprise-run-registry.ts"],
  ["src/runtime-policy.ts", "src/enterprise-runtime/runtime-policy.ts"],
  ["src/runtime-host-recovery.ts", "src/enterprise-runtime/runtime-host-recovery.ts"],
  ["src/private-run-context.ts", "src/gateway/server-methods/private-run-context.ts"],
  ["src/enterprise-runtime-methods.ts", "src/gateway/server-methods/enterprise-runtime-methods.ts"],
  ["src/faya-status-result-compat.ts", "src/gateway/server-methods/faya-status-result-compat.ts"],
  ["src/thoughts-response-status-compat.ts", "src/gateway/server-methods/thoughts-response-status-compat.ts"],
  ["src/runtime-capability-handshake.ts", "src/enterprise-runtime/runtime-capability-handshake.ts"],
]);

const PATCHED_TARGETS = [
  "packages/gateway-protocol/src/schema/enterprise-runtime.ts",
  "src/config/types.gateway.ts",
  "src/config/zod-schema.ts",
  "src/gateway/server.impl.ts",
  "src/gateway/server-methods.ts",
  "src/gateway/methods/core-descriptors.ts",
  "src/gateway/server-methods/enterprise-runtime.ts",
  "src/enterprise-runtime/event-logger.ts",
  "src/enterprise-runtime/agent-runner.ts",
  "src/agents/command/types.ts",
  "src/agents/agent-command.ts",
  "src/agents/command/attempt-execution.ts",
  "src/agents/embedded-agent-runner/run/params.ts",
  "src/agents/embedded-agent-runner/run.ts",
  "src/agents/embedded-agent-runner/run/types.ts",
  "src/agents/embedded-agent-runner/run/attempt.ts",
  "src/agents/agent-tools.ts",
  "src/agents/agent-tools.before-tool-call.ts",
  "src/enterprise-runtime/config/run-config.ts",
];

const ALLOWED_TARGETS = new Set([...COPIED_OVERLAY_FILES.values(), ...PATCHED_TARGETS]);
export const OVERLAY_BUNDLE_TARGET_COUNT = ALLOWED_TARGETS.size;
const SHA256_PATTERN = /^sha256:[a-f0-9]{64}$/;

export class OverlayError extends Error {
  constructor(code) {
    super(code);
    this.code = code;
  }
}

export async function createOverlayBundle(options) {
  const beforeRoot = await resolveDirectory(options.beforeSourceRoot, "PINNED_OPENCLAW_SOURCE_MISSING");
  const afterRoot = await resolveDirectory(options.afterSourceRoot, "OVERLAY_AFTER_SOURCE_MISSING");
  const overlayRoot = await resolveDirectory(options.overlayRoot, "OVERLAY_SOURCE_MISSING");
  const installerPath = await resolveRegularFile(options.installerPath || DEFAULT_INSTALLER_PATH, "OVERLAY_INSTALLER_MISSING");
  await assertOpenClawVersion(beforeRoot);
  await assertOpenClawVersion(afterRoot);
  await assertOnlyAllowListedSourceChanges(beforeRoot, afterRoot);
  const overlayTreeSha256 = await overlayTreeHash(overlayRoot);
  const files = [];

  for (const relativePath of [...ALLOWED_TARGETS].sort()) {
    await assertSafeParentDirectories(beforeRoot, relativePath);
    await assertSafeParentDirectories(afterRoot, relativePath);
    const before = await readRegularFileOrMissing(resolveSafeTarget(beforeRoot, relativePath));
    const after = await readRegularFileOrMissing(resolveSafeTarget(afterRoot, relativePath));
    if (!ALLOWED_TARGETS.has(relativePath) || !after || !before && !isCopiedTarget(relativePath)) {
      throw new OverlayError("OVERLAY_BUNDLE_TARGET_INVALID");
    }
    files.push({
      relativePath,
      beforeSha256: before?.sha256 ?? null,
      acceptedBeforeSha256: [...legacyAcceptedBeforeHashes(relativePath)],
      afterSha256: after.sha256,
      contentBase64: after.content.toString("base64"),
    });
  }

  assertRequiredBundleTargets(files);
  await assertCopiedOverlayTargets(files, overlayRoot);
  const manifest = {
    schema: BUNDLE_SCHEMA,
    openclawVersion: EXPECTED_OPENCLAW_VERSION,
    installerSha256: await fileHash(installerPath),
    overlayTreeSha256,
    files,
  };
  const serialized = Buffer.from(`${JSON.stringify(manifest, null, 2)}\n`, "utf8");
  if (options.outputPath) await writeBundle(options.outputPath, serialized);
  return { bundleSha256: sha256(serialized), verifiedFileCount: files.length, changedFileCount: changedFileCount(files), manifest };
}

export async function applyOverlayBundle(options, testHooks = undefined) {
  // Optional hooks are used only by local fault-injection regressions. The CLI
  // never supplies them and production calls use node:fs operations.
  const renameTarget = testHooks?.renameTarget ?? rename;
  const writeStage = testHooks?.writeStage ?? writeFile;
  const expectedBundleSha256 = requireHash(options.expectedBundleSha256, "OVERLAY_BUNDLE_HASH_REQUIRED");
  const sourceRoot = await resolveDirectory(options.openclawSourceRoot, "PINNED_OPENCLAW_SOURCE_MISSING");
  const overlayRoot = await resolveDirectory(options.overlayRoot, "OVERLAY_SOURCE_MISSING");
  const installerPath = await resolveRegularFile(options.installerPath || DEFAULT_INSTALLER_PATH, "OVERLAY_INSTALLER_MISSING");
  await assertOpenClawVersion(sourceRoot);
  const bundle = await readVerifiedBundle(options.bundlePath, expectedBundleSha256, overlayRoot, installerPath);
  const prepared = await prepareBundleWrites(sourceRoot, bundle.files);

  if (options.dryRun) {
    return { applied: false, dryRun: true, verifiedFileCount: prepared.length, changedFileCount: prepared.filter((item) => !item.unchanged).length, bundleSha256: expectedBundleSha256 };
  }

  const transactionId = randomUUID();
  const staged = [];
  const swapped = [];
  try {
    for (const item of prepared.filter((candidate) => !candidate.unchanged)) {
      const stagePath = `${item.targetPath}.huahuo-overlay-${transactionId}.next`;
      // Register before the write: a partial write failure still needs cleanup.
      const stage = { ...item, stagePath };
      staged.push(stage);
      await writeStage(stagePath, item.content, { flag: "wx", mode: 0o600 });
    }
    for (const item of staged) {
      const backupPath = item.exists ? `${item.targetPath}.huahuo-overlay-${transactionId}.previous` : null;
      // Record the transaction before the first destructive rename. If the
      // replacement rename fails after the original moved aside, rollback can
      // still restore that original target.
      const swap = { ...item, backupPath, originalMoved: false, replacementMoved: false };
      swapped.push(swap);
      if (backupPath) {
        await renameTarget(item.targetPath, backupPath);
        swap.originalMoved = true;
      }
      await renameTarget(item.stagePath, item.targetPath);
      swap.replacementMoved = true;
    }
    for (const item of prepared) {
      if ((await fileHash(item.targetPath)) !== item.afterSha256) throw new OverlayError("OVERLAY_APPLY_VERIFY_FAILED");
    }
    await Promise.all(swapped.map((item) => item.backupPath ? rm(item.backupPath, { force: true }) : undefined));
    return { applied: true, dryRun: false, verifiedFileCount: prepared.length, changedFileCount: prepared.filter((item) => !item.unchanged).length, bundleSha256: expectedBundleSha256 };
  } catch (error) {
    await rollback(swapped, staged);
    if (error instanceof OverlayError) throw error;
    throw new OverlayError("OVERLAY_APPLY_FAILED");
  } finally {
    await Promise.all(staged.map((item) => rm(item.stagePath, { force: true }).catch(() => {})));
  }
}

async function readVerifiedBundle(bundlePath, expectedBundleSha256, overlayRoot, installerPath) {
  const bundleFile = await resolveRegularFile(bundlePath, "OVERLAY_BUNDLE_MISSING");
  const bytes = await readFile(bundleFile);
  if (sha256(bytes) !== expectedBundleSha256) throw new OverlayError("OVERLAY_BUNDLE_HASH_MISMATCH");
  let bundle;
  try {
    bundle = JSON.parse(bytes.toString("utf8"));
  } catch {
    throw new OverlayError("OVERLAY_BUNDLE_SCHEMA_INVALID");
  }
  if (!bundle || bundle.schema !== BUNDLE_SCHEMA || bundle.openclawVersion !== EXPECTED_OPENCLAW_VERSION || !Array.isArray(bundle.files) || bundle.files.length === 0) {
    throw new OverlayError("OVERLAY_BUNDLE_SCHEMA_INVALID");
  }
  if (bundle.installerSha256 !== await fileHash(installerPath)) throw new OverlayError("OVERLAY_BUNDLE_INSTALLER_MISMATCH");
  if (bundle.overlayTreeSha256 !== await overlayTreeHash(overlayRoot)) throw new OverlayError("OVERLAY_BUNDLE_OVERLAY_MISMATCH");
  const seen = new Set();
  for (const file of bundle.files) validateBundleFile(file, seen);
  assertRequiredBundleTargets(bundle.files);
  await assertCopiedOverlayTargets(bundle.files, overlayRoot);
  return bundle;
}

async function prepareBundleWrites(sourceRoot, files) {
  const prepared = [];
  for (const file of files) {
    const targetPath = resolveSafeTarget(sourceRoot, file.relativePath);
    await assertSafeParentDirectories(sourceRoot, file.relativePath);
    const current = await readRegularFileOrMissing(targetPath);
    const matchesBefore = current ? file.beforeSha256 === current.sha256 : file.beforeSha256 === null;
    const matchesAfter = Boolean(current && current.sha256 === file.afterSha256);
    const matchesLegacyBefore = Boolean(current && file.acceptedBeforeSha256.includes(current.sha256));
    if (!matchesBefore && !matchesAfter && !matchesLegacyBefore) throw new OverlayError("OVERLAY_SOURCE_DRIFT");
    const content = decodeCanonicalBase64(file.contentBase64);
    if (sha256(content) !== file.afterSha256) throw new OverlayError("OVERLAY_BUNDLE_CONTENT_INVALID");
    prepared.push({ targetPath, exists: Boolean(current), unchanged: matchesAfter, content, afterSha256: file.afterSha256 });
  }
  return prepared;
}

async function rollback(swapped, staged) {
  for (const item of [...swapped].reverse()) {
    try {
      if (item.replacementMoved) await rm(item.targetPath, { force: true });
      if (item.originalMoved && item.backupPath) await rename(item.backupPath, item.targetPath);
    } catch {
      // The caller receives the original fail-closed error. No success receipt is emitted.
    }
  }
  await Promise.all(staged.map((item) => item.backupPath ? rm(item.backupPath, { force: true }).catch(() => {}) : undefined));
}

function validateBundleFile(file, seen) {
  if (!file || typeof file !== "object" || typeof file.relativePath !== "string" || !ALLOWED_TARGETS.has(file.relativePath) || seen.has(file.relativePath)) {
    throw new OverlayError("OVERLAY_BUNDLE_PATH_INVALID");
  }
  seen.add(file.relativePath);
  if (file.beforeSha256 !== null && !isHash(file.beforeSha256) || !isHash(file.afterSha256) || typeof file.contentBase64 !== "string") {
    throw new OverlayError("OVERLAY_BUNDLE_SCHEMA_INVALID");
  }
  const expectedLegacy = legacyAcceptedBeforeHashes(file.relativePath);
  if (!Array.isArray(file.acceptedBeforeSha256) || file.acceptedBeforeSha256.length !== expectedLegacy.length || file.acceptedBeforeSha256.some((value, index) => value !== expectedLegacy[index])) {
    throw new OverlayError("OVERLAY_BUNDLE_LEGACY_BASELINE_INVALID");
  }
}

function legacyAcceptedBeforeHashes(relativePath) {
  return LEGACY_ACCEPTED_BEFORE_HASHES[relativePath] ?? [];
}

function assertRequiredBundleTargets(files) {
  const targets = new Set(files.map((file) => file.relativePath));
  if (targets.size !== ALLOWED_TARGETS.size || [...ALLOWED_TARGETS].some((target) => !targets.has(target))) {
    throw new OverlayError("OVERLAY_BUNDLE_INCOMPLETE");
  }
}

async function assertCopiedOverlayTargets(files, overlayRoot) {
  const byTarget = new Map(files.map((file) => [file.relativePath, file]));
  for (const [overlayRelativePath, targetRelativePath] of COPIED_OVERLAY_FILES) {
    const file = byTarget.get(targetRelativePath);
    if (!file) throw new OverlayError("OVERLAY_BUNDLE_COPY_TARGET_MISSING");
    await assertSafeParentDirectories(overlayRoot, overlayRelativePath);
    const overlaySource = await resolveRegularFile(resolveSafeTarget(overlayRoot, overlayRelativePath), "OVERLAY_SOURCE_MISSING");
    if (file.afterSha256 !== await fileHash(overlaySource)) {
      throw new OverlayError("OVERLAY_BUNDLE_COPY_CONTENT_MISMATCH");
    }
  }
}

async function overlayTreeHash(overlayRoot) {
  const entries = [];
  for (const relativePath of OVERLAY_REQUIRED_FILES) {
    await assertSafeParentDirectories(overlayRoot, relativePath);
    const sourcePath = await resolveRegularFile(resolveSafeTarget(overlayRoot, relativePath), "OVERLAY_SOURCE_MISSING");
    entries.push(`${relativePath}:${await fileHash(sourcePath)}`);
  }
  return sha256(Buffer.from(entries.join("\n"), "utf8"));
}

async function assertOpenClawVersion(root) {
  const packageFile = await resolveRegularFile(path.join(root, "package.json"), "PINNED_OPENCLAW_PACKAGE_MISSING");
  let packageJson;
  try {
    packageJson = JSON.parse(await readFile(packageFile, "utf8"));
  } catch {
    throw new OverlayError("PINNED_OPENCLAW_PACKAGE_INVALID");
  }
  if (packageJson.version !== EXPECTED_OPENCLAW_VERSION) throw new OverlayError("PINNED_OPENCLAW_VERSION_MISMATCH");
}

async function resolveDirectory(candidate, code) {
  if (!candidate) throw new OverlayError(code);
  const stats = await lstat(candidate).catch(() => null);
  if (!stats || !stats.isDirectory() || stats.isSymbolicLink()) throw new OverlayError(code);
  return path.resolve(candidate);
}

async function resolveRegularFile(candidate, code) {
  if (!candidate) throw new OverlayError(code);
  const stats = await lstat(candidate).catch(() => null);
  if (!stats || !stats.isFile() || stats.isSymbolicLink()) throw new OverlayError(code);
  return path.resolve(candidate);
}

async function readRegularFileOrMissing(candidate) {
  const stats = await lstat(candidate).catch(() => null);
  if (!stats) return null;
  if (!stats.isFile() || stats.isSymbolicLink()) throw new OverlayError("OVERLAY_TARGET_SYMLINK_FORBIDDEN");
  return { content: await readFile(candidate), sha256: await fileHash(candidate) };
}

async function assertSafeParentDirectories(root, relativePath) {
  let current = root;
  for (const segment of relativePath.split("/").slice(0, -1)) {
    current = path.join(current, segment);
    const stats = await lstat(current).catch(() => null);
    if (!stats || !stats.isDirectory() || stats.isSymbolicLink()) throw new OverlayError("OVERLAY_TARGET_DIRECTORY_INVALID");
  }
}

function resolveSafeTarget(root, relativePath) {
  if (typeof relativePath !== "string" || relativePath.includes("\\") || relativePath.startsWith("/") || relativePath.includes("../") || relativePath === "..") {
    throw new OverlayError("OVERLAY_BUNDLE_PATH_INVALID");
  }
  const candidate = path.resolve(root, relativePath);
  if (candidate !== path.join(root, ...relativePath.split("/"))) throw new OverlayError("OVERLAY_BUNDLE_PATH_INVALID");
  return candidate;
}

function isCopiedTarget(relativePath) { return [...COPIED_OVERLAY_FILES.values()].includes(relativePath); }
function isHash(value) { return typeof value === "string" && SHA256_PATTERN.test(value); }
function requireHash(value, code) { if (!isHash(value)) throw new OverlayError(code); return value; }
function sha256(value) { return `sha256:${createHash("sha256").update(value).digest("hex")}`; }
function changedFileCount(files) { return files.filter((file) => file.beforeSha256 !== file.afterSha256).length; }
async function fileHash(candidate) { return sha256(await readFile(candidate)); }

function decodeCanonicalBase64(value) {
  if (typeof value !== "string" || !/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(value)) {
    throw new OverlayError("OVERLAY_BUNDLE_CONTENT_INVALID");
  }
  const decoded = Buffer.from(value, "base64");
  if (decoded.toString("base64") !== value) throw new OverlayError("OVERLAY_BUNDLE_CONTENT_INVALID");
  return decoded;
}

async function assertOnlyAllowListedSourceChanges(beforeRoot, afterRoot) {
  const [beforeFiles, afterFiles] = await Promise.all([listSourceFiles(beforeRoot), listSourceFiles(afterRoot)]);
  const paths = new Set([...beforeFiles.keys(), ...afterFiles.keys()]);
  for (const relativePath of paths) {
    if (ALLOWED_TARGETS.has(relativePath)) continue;
    if (beforeFiles.get(relativePath) !== afterFiles.get(relativePath)) {
      throw new OverlayError("OVERLAY_BUNDLE_TARGET_INVALID");
    }
  }
}

async function listSourceFiles(root) {
  const files = new Map();
  await walk(root, "");
  return files;

  async function walk(directory, relativeDirectory) {
    const entries = await readdir(directory, { withFileTypes: true });
    for (const entry of entries) {
      // Git metadata and installed dependencies are not OpenClaw source. The
      // latter is rebuilt from the pinned lockfile and commonly contains
      // package-manager links that are intentionally outside this bundle.
      if (!relativeDirectory && (entry.name === ".git" || entry.name === "node_modules")) continue;
      const relativePath = relativeDirectory ? `${relativeDirectory}/${entry.name}` : entry.name;
      const absolutePath = path.join(directory, entry.name);
      const stats = await lstat(absolutePath);
      if (stats.isSymbolicLink()) throw new OverlayError("OVERLAY_SOURCE_TREE_INVALID");
      if (stats.isDirectory()) {
        await walk(absolutePath, relativePath);
      } else if (stats.isFile()) {
        files.set(relativePath, await fileHash(absolutePath));
      } else {
        throw new OverlayError("OVERLAY_SOURCE_TREE_INVALID");
      }
    }
  }
}

async function writeBundle(outputPath, content) {
  const parent = path.dirname(path.resolve(outputPath));
  await mkdir(parent, { recursive: true, mode: 0o700 });
  await writeFile(outputPath, content, { flag: "wx", mode: 0o600 });
}

function parseArguments(argv) {
  const [command, ...args] = argv;
  if (command !== "create" && command !== "apply") throw new OverlayError("OVERLAY_USAGE_INVALID");
  const values = {};
  for (let index = 0; index < args.length; index += 1) {
    const key = args[index];
    if (key === "--dry-run" && command === "apply") { values.dryRun = true; continue; }
    if (!key.startsWith("--") || index + 1 >= args.length) throw new OverlayError("OVERLAY_USAGE_INVALID");
    values[key.slice(2)] = args[++index];
  }
  return command === "create" ? {
    command,
    beforeSourceRoot: values["before-source-root"], afterSourceRoot: values["after-source-root"], overlayRoot: values["overlay-root"],
    installerPath: values["installer-path"], outputPath: values.output,
  } : {
    command,
    openclawSourceRoot: values["openclaw-source-root"], overlayRoot: values["overlay-root"], bundlePath: values.bundle,
    expectedBundleSha256: values["expected-bundle-sha256"], installerPath: values["installer-path"], dryRun: Boolean(values.dryRun),
  };
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    const options = parseArguments(process.argv.slice(2));
    const result = options.command === "create" ? await createOverlayBundle(options) : await applyOverlayBundle(options);
    process.stdout.write(`${JSON.stringify({ ok: true, ...result })}\n`);
  } catch (error) {
    const code = error instanceof OverlayError ? error.code : "OVERLAY_APPLY_FAILED";
    process.stderr.write(`${code}\n`);
    process.exitCode = 1;
  }
}
