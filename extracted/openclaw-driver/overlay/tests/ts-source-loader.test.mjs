import assert from "node:assert/strict";
import path from "node:path";
import test from "node:test";
import { fileURLToPath, pathToFileURL } from "node:url";
import { resolve } from "./ts-source-loader.mjs";

const overlayRoot = fileURLToPath(new URL("../", import.meta.url));
const registryURL = pathToFileURL(path.join(overlayRoot, "src", "enterprise-run-registry.ts")).href;

test("maps only a missing local JS sibling to checked-in TypeScript", async () => {
  let delegated = false;
  const result = await resolve("./enterprise-run-store.js", { parentURL: registryURL }, async () => {
    delegated = true;
    return { url: "fallback:unexpected" };
  });
  assert.equal(delegated, false);
  assert.equal(fileURLToPath(result.url), path.join(overlayRoot, "src", "enterprise-run-store.ts"));
  assert.equal(result.shortCircuit, true);
});

test("delegates an escaping or missing local specifier to Node", async () => {
  const delegated = [];
  const nextResolve = async (specifier) => {
    delegated.push(specifier);
    return { url: `fallback:${specifier}` };
  };
  const escaping = await resolve("../../outside.js", { parentURL: registryURL }, nextResolve);
  const missing = await resolve("./missing.js", { parentURL: registryURL }, nextResolve);
  assert.deepEqual(escaping, { url: "fallback:../../outside.js" });
  assert.deepEqual(missing, { url: "fallback:./missing.js" });
  assert.deepEqual(delegated, ["../../outside.js", "./missing.js"]);
});
