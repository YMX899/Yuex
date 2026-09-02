import { existsSync, realpathSync } from "node:fs";
import { isAbsolute, relative } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const overlayRoot = realpathSync(fileURLToPath(new URL("../", import.meta.url)));

function isOverlayPath(candidate) {
  const relativePath = relative(overlayRoot, candidate);
  return relativePath !== "" && relativePath !== ".." && !relativePath.startsWith("..\\") && !relativePath.startsWith("../") && !isAbsolute(relativePath);
}

// The production overlay intentionally keeps NodeNext-style .js specifiers so
// the files can be copied into OpenClaw's emitted build. Checked-in source
// tests have no emitted JavaScript, so map only a missing local sibling .js
// module to its real .ts source. Everything else remains Node's resolver.
export async function resolve(specifier, context, nextResolve) {
  if (typeof specifier !== "string" || !context.parentURL || !specifier.endsWith(".js") || !(specifier.startsWith("./") || specifier.startsWith("../"))) {
    return nextResolve(specifier, context);
  }
  let jsPath;
  try {
    const jsURL = new URL(specifier, context.parentURL);
    if (jsURL.protocol !== "file:") return nextResolve(specifier, context);
    jsPath = fileURLToPath(jsURL);
  } catch {
    return nextResolve(specifier, context);
  }
  if (!isOverlayPath(jsPath) || existsSync(jsPath)) return nextResolve(specifier, context);

  const tsPath = jsPath.slice(0, -3) + ".ts";
  if (!existsSync(tsPath)) return nextResolve(specifier, context);
  const realTsPath = realpathSync(tsPath);
  if (!isOverlayPath(realTsPath)) return nextResolve(specifier, context);
  return { url: pathToFileURL(realTsPath).href, shortCircuit: true };
}
