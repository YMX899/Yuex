import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

const installer = await readFile(new URL("./apply_openclaw_enterprise_overlay.ps1", import.meta.url), "utf8");
const golden = JSON.parse(await readFile(new URL("./fixtures/runtime-workspace-go-to-typescript.golden.json", import.meta.url), "utf8"));

const workspacePatch = `    workspace: Type.Union([
      Type.Object(
        {
          realPath: NonEmptyString,
          accessMode: Type.Literal("read"),
        },
        { additionalProperties: false },
      ),
      Type.Object(
        {
          realPath: NonEmptyString,
          accessMode: Type.Literal("write"),
          writeLease: RuntimeWorkspaceWriteLeaseSchema,
        },
        { additionalProperties: false },
      ),
    ]),`;

test("pins the exact discriminated spec.workspace protocol patch", () => {
  assert.equal(installer.split(workspacePatch).length - 1, 1);
  assert.match(installer, /allowedRoots: Type\.Tuple\(\[Type\.Literal\("output"\), Type\.Literal\("staging"\)\]\)/);
  assert.match(installer, /"runtime_workspace_write_lease_schema"/);
});

test("accepts the Go-to-TypeScript golden workspace forms and keeps read mode lease-free", () => {
  assert.deepEqual(Object.keys(golden.read).sort(), ["accessMode", "realPath"]);
  assert.equal(validateWorkspace(golden.read), true);
  assert.equal(validateWorkspace(golden.write), true);
});

test("rejects malformed and widened workspace schema values", () => {
  const malformed = [
    { ...golden.read, writeLease: golden.write.writeLease },
    { ...golden.read, writeLease: null },
    { realPath: golden.write.realPath, accessMode: "write" },
    { ...golden.read, arbitrary: true },
    { ...golden.write, arbitrary: true },
    { ...golden.write, writeLease: null },
    { ...golden.write, writeLease: { ...golden.write.writeLease, arbitrary: true } },
    { ...golden.write, writeLease: { ...golden.write.writeLease, version: "v2" } },
    { ...golden.write, writeLease: { ...golden.write.writeLease, workspaceManifestHash: "sha256:bad" } },
    { ...golden.write, writeLease: { ...golden.write.writeLease, workspaceManifestHash: `sha256:${"A".repeat(64)}` } },
    { ...golden.write, writeLease: { ...golden.write.writeLease, allowedRoots: ["staging", "output"] } },
    { ...golden.write, writeLease: { ...golden.write.writeLease, allowedRoots: ["output", "other"] } },
    { ...golden.write, writeLease: { ...golden.write.writeLease, expiresAt: -1 } },
  ];
  for (const value of malformed) assert.equal(validateWorkspace(value), false, JSON.stringify(value));
});

test("pins the real Core tool pre-execution write guard wiring", () => {
  assert.match(installer, /ctx\.onBeforeToolCall\(\{ toolName: normalizedToolName, toolCallId, args: executeParams \}\)/);
  assert.match(installer, /enterpriseRunRegistry\.assertToolCallAllowed\(ctx\.runId, call\)/);
});

function validateWorkspace(value) {
  if (!isRecord(value) || !nonEmptyString(value.realPath)) return false;
  if (value.accessMode === "read") return exactKeys(value, ["realPath", "accessMode"]);
  if (value.accessMode === "write") {
    return exactKeys(value, ["realPath", "accessMode", "writeLease"]) && validateWriteLease(value.writeLease);
  }
  return false;
}

function validateWriteLease(value) {
  const keys = ["version", "runId", "workspaceId", "workspaceManifestHash", "allowedRoots", "expiresAt"];
  return isRecord(value) && exactKeys(value, keys) &&
    value.version === "huahuo.runtime-write-lease.v1" && nonEmptyString(value.runId) && nonEmptyString(value.workspaceId) &&
    /^sha256:[a-f0-9]{64}$/.test(value.workspaceManifestHash) && Array.isArray(value.allowedRoots) &&
    value.allowedRoots.length === 2 && value.allowedRoots[0] === "output" && value.allowedRoots[1] === "staging" &&
    Number.isInteger(value.expiresAt) && value.expiresAt >= 0;
}

function exactKeys(value, keys) {
  return Object.keys(value).length === keys.length && keys.every((key) => Object.hasOwn(value, key));
}

function isRecord(value) { return value !== null && typeof value === "object" && !Array.isArray(value); }
function nonEmptyString(value) { return typeof value === "string" && value.length > 0; }
