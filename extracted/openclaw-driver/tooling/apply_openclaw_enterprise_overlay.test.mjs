import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { cp, mkdtemp, mkdir, readFile, readdir, rename, rm, symlink, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { applyOverlayBundle, createOverlayBundle, LEGACY_ACCEPTED_BEFORE_HASHES, OVERLAY_BUNDLE_TARGET_COUNT, OverlayError } from "./apply_openclaw_enterprise_overlay.mjs";

const here = path.dirname(fileURLToPath(import.meta.url));
const repoOverlay = path.resolve(here, "..", "openclaw-enterprise-runtime-overlay");
const installerPath = path.join(here, "apply_openclaw_enterprise_overlay.ps1");
const PATCHED_TARGETS = [
  "packages/gateway-protocol/src/schema/enterprise-runtime.ts", "src/config/types.gateway.ts", "src/config/zod-schema.ts", "src/gateway/server.impl.ts",
  "src/gateway/server-methods.ts", "src/gateway/methods/core-descriptors.ts", "src/gateway/server-methods/enterprise-runtime.ts",
  "src/enterprise-runtime/event-logger.ts", "src/enterprise-runtime/agent-runner.ts", "src/agents/command/types.ts", "src/agents/agent-command.ts",
  "src/agents/command/attempt-execution.ts", "src/agents/embedded-agent-runner/run/params.ts", "src/agents/embedded-agent-runner/run.ts",
  "src/agents/embedded-agent-runner/run/types.ts", "src/agents/embedded-agent-runner/run/attempt.ts", "src/agents/agent-tools.ts",
  "src/agents/agent-tools.before-tool-call.ts", "src/enterprise-runtime/config/run-config.ts",
];

test("creates and applies a hash-pinned bundle without a shell fallback", async (t) => {
  const root = await mkdtemp(path.join(os.tmpdir(), "huahuo-overlay-bundle-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  const before = path.join(root, "before");
  const after = path.join(root, "after");
  const target = path.join(root, "target");
  await makeSource(before);
  await cp(before, after, { recursive: true });
  await cp(before, target, { recursive: true });
  await applyReviewedChanges(after);

  const bundlePath = path.join(root, "overlay.bundle.json");
  const bundle = await createOverlayBundle({ beforeSourceRoot: before, afterSourceRoot: after, overlayRoot: repoOverlay, installerPath, outputPath: bundlePath });
  assert.equal(OVERLAY_BUNDLE_TARGET_COUNT, 28);
  const beforeBytes = await readFile(path.join(target, "src/config/types.gateway.ts"), "utf8");
  const dryRun = await applyOverlayBundle({ openclawSourceRoot: target, overlayRoot: repoOverlay, installerPath, bundlePath, expectedBundleSha256: bundle.bundleSha256, dryRun: true });
  assert.equal(dryRun.dryRun, true);
  assert.equal(await readFile(path.join(target, "src/config/types.gateway.ts"), "utf8"), beforeBytes);

  const applied = await applyOverlayBundle({ openclawSourceRoot: target, overlayRoot: repoOverlay, installerPath, bundlePath, expectedBundleSha256: bundle.bundleSha256 });
  assert.equal(applied.applied, true);
  assert.equal(await readFile(path.join(target, "src/config/types.gateway.ts"), "utf8"), "patched gateway type\n");
  assert.equal(await readFile(path.join(target, "src/enterprise-runtime/runtime-policy.ts"), "utf8"), await readFile(path.join(repoOverlay, "src/runtime-policy.ts"), "utf8"));
  const reapplied = await applyOverlayBundle({ openclawSourceRoot: target, overlayRoot: repoOverlay, installerPath, bundlePath, expectedBundleSha256: bundle.bundleSha256 });
  assert.equal(reapplied.changedFileCount, 0);
});

test("rejects after-tree changes outside the reviewed target allow-list", async (t) => {
  const root = await mkdtemp(path.join(os.tmpdir(), "huahuo-overlay-unlisted-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  const before = path.join(root, "before");
  const after = path.join(root, "after");
  await makeSource(before);
  await cp(before, after, { recursive: true });
  await applyReviewedChanges(after);
  await writeFile(path.join(after, "unreviewed-source-change.ts"), "export const unexpected = true;\n", "utf8");
  await assert.rejects(
    () => createOverlayBundle({ beforeSourceRoot: before, afterSourceRoot: after, overlayRoot: repoOverlay, installerPath }),
    hasCode("OVERLAY_BUNDLE_TARGET_INVALID"),
  );
});

test("rejects an intermediate symlink in the reviewed overlay tree", async (t) => {
  const root = await mkdtemp(path.join(os.tmpdir(), "huahuo-overlay-parent-link-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  const before = path.join(root, "before");
  const after = path.join(root, "after");
  const overlay = path.join(root, "overlay");
  const externalSource = path.join(root, "external-src");
  await makeSource(before);
  await cp(before, after, { recursive: true });
  await applyReviewedChanges(after);
  await cp(repoOverlay, overlay, { recursive: true });
  await rename(path.join(overlay, "src"), externalSource);
  await symlink(externalSource, path.join(overlay, "src"), process.platform === "win32" ? "junction" : "dir");
  await assert.rejects(
    () => createOverlayBundle({ beforeSourceRoot: before, afterSourceRoot: after, overlayRoot: overlay, installerPath }),
    hasCode("OVERLAY_TARGET_DIRECTORY_INVALID"),
  );
});

test("rejects source drift before writing any target", async (t) => {
  const root = await mkdtemp(path.join(os.tmpdir(), "huahuo-overlay-drift-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  const before = path.join(root, "before");
  const after = path.join(root, "after");
  const target = path.join(root, "target");
  await makeSource(before); await cp(before, after, { recursive: true }); await cp(before, target, { recursive: true }); await applyReviewedChanges(after);
  const bundlePath = path.join(root, "overlay.bundle.json");
  const bundle = await createOverlayBundle({ beforeSourceRoot: before, afterSourceRoot: after, overlayRoot: repoOverlay, installerPath, outputPath: bundlePath });
  await writeFile(path.join(target, "src/config/types.gateway.ts"), "unreviewed drift\n", "utf8");
  await assert.rejects(() => applyOverlayBundle({ openclawSourceRoot: target, overlayRoot: repoOverlay, installerPath, bundlePath, expectedBundleSha256: bundle.bundleSha256 }), hasCode("OVERLAY_SOURCE_DRIFT"));
  assert.equal(await readFile(path.join(target, "src/config/types.gateway.ts"), "utf8"), "unreviewed drift\n");
});

test("creates a complete canonical bundle from a partially overlaid source", async (t) => {
  const root = await mkdtemp(path.join(os.tmpdir(), "huahuo-overlay-partial-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  const before = path.join(root, "before");
  const after = path.join(root, "after");
  const target = path.join(root, "target");
  await makeSource(before); await cp(before, after, { recursive: true }); await applyReviewedChanges(after);
  await writeFile(path.join(before, "src/config/types.gateway.ts"), await readFile(path.join(after, "src/config/types.gateway.ts")));
  await cp(before, target, { recursive: true });
  const bundlePath = path.join(root, "overlay.bundle.json");
  const bundle = await createOverlayBundle({ beforeSourceRoot: before, afterSourceRoot: after, overlayRoot: repoOverlay, installerPath, outputPath: bundlePath });
  const gatewayType = bundle.manifest.files.find((file) => file.relativePath === "src/config/types.gateway.ts");
  assert.equal(bundle.verifiedFileCount, OVERLAY_BUNDLE_TARGET_COUNT);
  assert.equal(gatewayType.beforeSha256, gatewayType.afterSha256);
  const applied = await applyOverlayBundle({ openclawSourceRoot: target, overlayRoot: repoOverlay, installerPath, bundlePath, expectedBundleSha256: bundle.bundleSha256 });
  assert.equal(applied.verifiedFileCount, OVERLAY_BUNDLE_TARGET_COUNT);
  assert.equal(applied.changedFileCount, OVERLAY_BUNDLE_TARGET_COUNT - 1);
  assert.equal(await readFile(path.join(target, "src/config/types.gateway.ts"), "utf8"), "patched gateway type\n");
});

test("reconciles mixed before and reviewed-after candidate targets", async (t) => {
  const root = await mkdtemp(path.join(os.tmpdir(), "huahuo-overlay-reconcile-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  const before = path.join(root, "before");
  const after = path.join(root, "after");
  const target = path.join(root, "target");
  await makeSource(before);
  await cp(before, after, { recursive: true });
  await cp(before, target, { recursive: true });
  await applyReviewedChanges(after);
  await writeFile(
    path.join(target, "src/config/types.gateway.ts"),
    await readFile(path.join(after, "src/config/types.gateway.ts")),
  );

  const bundlePath = path.join(root, "overlay.bundle.json");
  const bundle = await createOverlayBundle({ beforeSourceRoot: before, afterSourceRoot: after, overlayRoot: repoOverlay, installerPath, outputPath: bundlePath });
  const reconciled = await applyOverlayBundle({ openclawSourceRoot: target, overlayRoot: repoOverlay, installerPath, bundlePath, expectedBundleSha256: bundle.bundleSha256 });
  assert.equal(reconciled.changedFileCount, OVERLAY_BUNDLE_TARGET_COUNT - 1);
  assert.equal(await readFile(path.join(target, "src/config/types.gateway.ts"), "utf8"), "patched gateway type\n");
});

test("binds only the reviewed legacy V1 baseline hashes", async (t) => {
  const root = await mkdtemp(path.join(os.tmpdir(), "huahuo-overlay-legacy-baseline-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  const before = path.join(root, "before");
  const after = path.join(root, "after");
  const target = path.join(root, "target");
  await makeSource(before);
  await cp(before, after, { recursive: true });
  await cp(before, target, { recursive: true });
  await applyReviewedChanges(after);

  const bundlePath = path.join(root, "overlay.bundle.json");
  const bundle = await createOverlayBundle({ beforeSourceRoot: before, afterSourceRoot: after, overlayRoot: repoOverlay, installerPath, outputPath: bundlePath });
  assert.deepEqual(LEGACY_ACCEPTED_BEFORE_HASHES, {
    "src/enterprise-runtime/enterprise-run-registry.ts": ["sha256:bc170980804f2f44d47eb5c4f8acd3f2994fd635c47e1042dae8fb44d3f03683"],
    "src/enterprise-runtime/enterprise-run-store.ts": ["sha256:cb00411ed2e13b598bc203fed424b85c5abab5ae493d06370aa9b06ea678eca8"],
    "src/enterprise-runtime/runtime-capability-handshake.ts": ["sha256:38f490f2f54566ef2546824d2d334b4d596f930f860bb4a861f3cb414446eaaa"],
    "src/enterprise-runtime/runtime-host-recovery.ts": ["sha256:78defa3b2448fe7417a361055e26b8ab96443d9d3eed8f1510d1dd54ca6eb0be"],
    "src/enterprise-runtime/runtime-policy.ts": ["sha256:8273e0a3273ed3a449b66b67d30acd8002443847cbbddb4b8dab9dbbea3c3928"],
    "src/gateway/server-methods/enterprise-runtime-methods.ts": ["sha256:fde7a9a2b892c5e020af464abe4d618f203432766218e252a0500e46825ac1fd"],
    "src/gateway/server-methods/faya-status-result-compat.ts": ["sha256:59c1c713b3f3804d68bbedad4215520e4ebd3e166300d90ef5dbeddff3383740"],
    "src/gateway/server-methods/private-run-context.ts": ["sha256:fd9f566c44cabb599267afc59f076a039f1d0a81f5e757a845f2bc1441790022"],
  });
  const registry = bundle.manifest.files.find((file) => file.relativePath === "src/enterprise-runtime/enterprise-run-registry.ts");
  assert.deepEqual(registry.acceptedBeforeSha256, LEGACY_ACCEPTED_BEFORE_HASHES[registry.relativePath]);
  const ordinaryTarget = bundle.manifest.files.find((file) => file.relativePath === "src/config/types.gateway.ts");
  assert.deepEqual(ordinaryTarget.acceptedBeforeSha256, []);

  const tampered = structuredClone(bundle.manifest);
  tampered.files.find((file) => file.relativePath === "src/config/types.gateway.ts").acceptedBeforeSha256 = [LEGACY_ACCEPTED_BEFORE_HASHES[registry.relativePath][0]];
  const bytes = Buffer.from(`${JSON.stringify(tampered, null, 2)}\n`, "utf8");
  await writeFile(bundlePath, bytes);
  const expectedBundleSha256 = `sha256:${createHash("sha256").update(bytes).digest("hex")}`;
  await assert.rejects(
    () => applyOverlayBundle({ openclawSourceRoot: target, overlayRoot: repoOverlay, installerPath, bundlePath, expectedBundleSha256 }),
    hasCode("OVERLAY_BUNDLE_LEGACY_BASELINE_INVALID"),
  );

  const legacyTargetTampered = structuredClone(bundle.manifest);
  legacyTargetTampered.files.find((file) => file.relativePath === registry.relativePath).acceptedBeforeSha256.push("sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
  const legacyTargetBytes = Buffer.from(`${JSON.stringify(legacyTargetTampered, null, 2)}\n`, "utf8");
  await writeFile(bundlePath, legacyTargetBytes);
  const legacyTargetBundleSha256 = `sha256:${createHash("sha256").update(legacyTargetBytes).digest("hex")}`;
  await assert.rejects(
    () => applyOverlayBundle({ openclawSourceRoot: target, overlayRoot: repoOverlay, installerPath, bundlePath, expectedBundleSha256: legacyTargetBundleSha256 }),
    hasCode("OVERLAY_BUNDLE_LEGACY_BASELINE_INVALID"),
  );
});

test("restores an original target when replacement rename fails after backup move", async (t) => {
  const root = await mkdtemp(path.join(os.tmpdir(), "huahuo-overlay-rollback-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  const before = path.join(root, "before");
  const after = path.join(root, "after");
  const target = path.join(root, "target");
  await makeSource(before);
  await cp(before, after, { recursive: true });
  await cp(before, target, { recursive: true });
  await applyReviewedChanges(after);
  const bundlePath = path.join(root, "overlay.bundle.json");
  const bundle = await createOverlayBundle({ beforeSourceRoot: before, afterSourceRoot: after, overlayRoot: repoOverlay, installerPath, outputPath: bundlePath });
  const original = await readFile(path.join(target, "packages/gateway-protocol/src/schema/enterprise-runtime.ts"), "utf8");
  let renameCalls = 0;
  const failSecondRename = async (from, to) => {
    renameCalls += 1;
    if (renameCalls === 2) throw new Error("injected replacement rename failure");
    await rename(from, to);
  };
  await assert.rejects(
    () => applyOverlayBundle(
      { openclawSourceRoot: target, overlayRoot: repoOverlay, installerPath, bundlePath, expectedBundleSha256: bundle.bundleSha256 },
      { renameTarget: failSecondRename },
    ),
    hasCode("OVERLAY_APPLY_FAILED"),
  );
  assert.equal(await readFile(path.join(target, "packages/gateway-protocol/src/schema/enterprise-runtime.ts"), "utf8"), original);
  const artifacts = (await readdir(path.join(target, "packages/gateway-protocol/src/schema"))).filter((name) => name.includes(".huahuo-overlay-"));
  assert.deepEqual(artifacts, []);
});

test("cleans a partially created staging file when staging write fails", async (t) => {
  const root = await mkdtemp(path.join(os.tmpdir(), "huahuo-overlay-stage-failure-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  const before = path.join(root, "before");
  const after = path.join(root, "after");
  const target = path.join(root, "target");
  await makeSource(before);
  await cp(before, after, { recursive: true });
  await cp(before, target, { recursive: true });
  await applyReviewedChanges(after);
  const bundlePath = path.join(root, "overlay.bundle.json");
  const bundle = await createOverlayBundle({ beforeSourceRoot: before, afterSourceRoot: after, overlayRoot: repoOverlay, installerPath, outputPath: bundlePath });
  const original = await readFile(path.join(target, "packages/gateway-protocol/src/schema/enterprise-runtime.ts"), "utf8");
  const partialStageFailure = async (stagePath, content) => {
    await writeFile(stagePath, content, { flag: "wx", mode: 0o600 });
    throw new Error("injected stage write failure");
  };
  await assert.rejects(
    () => applyOverlayBundle(
      { openclawSourceRoot: target, overlayRoot: repoOverlay, installerPath, bundlePath, expectedBundleSha256: bundle.bundleSha256 },
      { writeStage: partialStageFailure },
    ),
    hasCode("OVERLAY_APPLY_FAILED"),
  );
  assert.equal(await readFile(path.join(target, "packages/gateway-protocol/src/schema/enterprise-runtime.ts"), "utf8"), original);
  const artifacts = (await readdir(path.join(target, "packages/gateway-protocol/src/schema"))).filter((name) => name.includes(".huahuo-overlay-"));
  assert.deepEqual(artifacts, []);
});

test("requires an explicit expected bundle hash", async (t) => {
  const root = await mkdtemp(path.join(os.tmpdir(), "huahuo-overlay-hash-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  await makeSource(root);
  await assert.rejects(() => applyOverlayBundle({ openclawSourceRoot: root, overlayRoot: repoOverlay, installerPath, bundlePath: path.join(root, "missing.bundle") }), hasCode("OVERLAY_BUNDLE_HASH_REQUIRED"));
});

test("rejects noncanonical Base64 before any target mutation", async (t) => {
  const root = await mkdtemp(path.join(os.tmpdir(), "huahuo-overlay-base64-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  const before = path.join(root, "before");
  const after = path.join(root, "after");
  const target = path.join(root, "target");
  await makeSource(before);
  await cp(before, after, { recursive: true });
  await cp(before, target, { recursive: true });
  await applyReviewedChanges(after);
  const bundlePath = path.join(root, "overlay.bundle.json");
  const bundle = await createOverlayBundle({ beforeSourceRoot: before, afterSourceRoot: after, overlayRoot: repoOverlay, installerPath, outputPath: bundlePath });
  const malformed = structuredClone(bundle.manifest);
  malformed.files[0].contentBase64 = `${malformed.files[0].contentBase64}\n`;
  const bytes = Buffer.from(`${JSON.stringify(malformed, null, 2)}\n`, "utf8");
  await writeFile(bundlePath, bytes);
  const expectedBundleSha256 = `sha256:${createHash("sha256").update(bytes).digest("hex")}`;
  const beforeBytes = await readFile(path.join(target, "src/config/types.gateway.ts"), "utf8");
  await assert.rejects(
    () => applyOverlayBundle({ openclawSourceRoot: target, overlayRoot: repoOverlay, installerPath, bundlePath, expectedBundleSha256 }),
    hasCode("OVERLAY_BUNDLE_CONTENT_INVALID"),
  );
  assert.equal(await readFile(path.join(target, "src/config/types.gateway.ts"), "utf8"), beforeBytes);
});

test("binds required compatibility and capability regression sources into the reviewed overlay tree", async (t) => {
  const root = await mkdtemp(path.join(os.tmpdir(), "huahuo-overlay-required-tests-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  const before = path.join(root, "before");
  const after = path.join(root, "after");
  const target = path.join(root, "target");
  const overlay = path.join(root, "overlay");
  await makeSource(before);
  await cp(before, after, { recursive: true });
  await cp(before, target, { recursive: true });
  await cp(repoOverlay, overlay, { recursive: true });
  await applyReviewedChanges(after);

  const bundlePath = path.join(root, "overlay.bundle.json");
  const bundle = await createOverlayBundle({ beforeSourceRoot: before, afterSourceRoot: after, overlayRoot: overlay, installerPath, outputPath: bundlePath });
  await writeFile(path.join(overlay, "tests/thoughts-response-status-compat.test.ts"), "tampered regression source\n", "utf8");
  await assert.rejects(
    () => applyOverlayBundle({ openclawSourceRoot: target, overlayRoot: overlay, installerPath, bundlePath, expectedBundleSha256: bundle.bundleSha256 }),
    hasCode("OVERLAY_BUNDLE_OVERLAY_MISMATCH"),
  );

  await rm(path.join(overlay, "tests/runtime-capability-handshake.test.ts"));
  await assert.rejects(
    () => createOverlayBundle({ beforeSourceRoot: before, afterSourceRoot: after, overlayRoot: overlay, installerPath, outputPath: path.join(root, "missing-test.bundle.json") }),
    hasCode("OVERLAY_SOURCE_MISSING"),
  );
});

test("uses filesystem transactions only and has no shell or remote fallback", async () => {
  const source = await readFile(path.join(here, "apply_openclaw_enterprise_overlay.mjs"), "utf8");
  assert.doesNotMatch(source, /node:child_process|\bspawn\(|\bexec\(|\bssh\b/i);
});

test("pins recovery preflight before the ordinary Gateway bind and retains normal close wiring", async () => {
  const installer = await readFile(installerPath, "utf8");
  assert.match(
    installer,
    /import \{ startEnterpriseRuntimeRecoveryWss, startGatewayWithEnterpriseRuntimeRecovery \} from "\.\.\/enterprise-runtime\/runtime-host-recovery\.js";/,
  );
  const startup = installer.indexOf("enterpriseRuntimeRecoveryListener = await startGatewayWithEnterpriseRuntimeRecovery({");
  const recoveryStart = installer.indexOf("startRecoveryListener: () => startupTrace.measure(\"enterprise.recovery-mtls.listen\"");
  const normalStart = installer.indexOf("startPrimaryGateway: () => startupTrace.measure(\"http.listen\", () => startListening())");
  assert.ok(startup >= 0, "installer must bind recovery and normal Gateway through one lifecycle helper");
  assert.ok(recoveryStart > startup, "recovery preflight/listen must occur inside the lifecycle helper");
  assert.ok(normalStart > recoveryStart, "ordinary Gateway bind must occur only after recovery preflight/listen");
  assert.match(installer, /gatewayConfigPath: process\.env\.OPENCLAW_CONFIG_PATH,/);
  assert.match(installer, /\$gatewayRecoveryImportPriorOverlayState\s*=/);
  assert.match(installer, /\$gatewayRecoveryStartPriorOverlayState\s*=/);
  assert.match(installer, /\$priorOriginalCount\s*=/);
  assert.ok(
    installer.includes("if (enterpriseRuntimeRecoveryListener) {`n      await enterpriseRuntimeRecoveryListener.close();`n      enterpriseRuntimeRecoveryListener = undefined;"),
    "normal Gateway close must close the recovery listener before forgetting it",
  );
});

async function makeSource(root) {
  await mkdir(path.join(root, "src/config"), { recursive: true });
  await mkdir(path.join(root, "src/enterprise-runtime"), { recursive: true });
  await mkdir(path.join(root, "src/gateway/server-methods"), { recursive: true });
  await writeFile(path.join(root, "package.json"), JSON.stringify({ version: "2026.6.2" }), "utf8");
  for (const relativePath of PATCHED_TARGETS) {
    const target = path.join(root, relativePath);
    await mkdir(path.dirname(target), { recursive: true });
    await writeFile(target, `original ${relativePath}\n`, "utf8");
  }
}

async function applyReviewedChanges(root) {
  for (const relativePath of PATCHED_TARGETS) await writeFile(path.join(root, relativePath), relativePath === "src/config/types.gateway.ts" ? "patched gateway type\n" : `patched ${relativePath}\n`, "utf8");
  for (const [from, to] of [
    ["src/enterprise-run-store.ts", "src/enterprise-runtime/enterprise-run-store.ts"],
    ["src/enterprise-run-registry.ts", "src/enterprise-runtime/enterprise-run-registry.ts"],
    ["src/runtime-policy.ts", "src/enterprise-runtime/runtime-policy.ts"],
    ["src/runtime-host-recovery.ts", "src/enterprise-runtime/runtime-host-recovery.ts"],
    ["src/runtime-capability-handshake.ts", "src/enterprise-runtime/runtime-capability-handshake.ts"],
  ]) {
    await writeFile(path.join(root, to), await readFile(path.join(repoOverlay, from)));
  }
  for (const [from, to] of [
    ["src/private-run-context.ts", "src/gateway/server-methods/private-run-context.ts"],
    ["src/enterprise-runtime-methods.ts", "src/gateway/server-methods/enterprise-runtime-methods.ts"],
    ["src/faya-status-result-compat.ts", "src/gateway/server-methods/faya-status-result-compat.ts"],
    ["src/thoughts-response-status-compat.ts", "src/gateway/server-methods/thoughts-response-status-compat.ts"],
  ]) {
    await mkdir(path.dirname(path.join(root, to)), { recursive: true });
    await writeFile(path.join(root, to), await readFile(path.join(repoOverlay, from)));
  }
}

function hasCode(expected) {
  return (error) => error instanceof OverlayError && error.code === expected;
}
