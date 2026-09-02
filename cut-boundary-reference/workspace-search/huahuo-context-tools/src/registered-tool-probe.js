import {
  createWorkspaceSearchTool,
  workspaceSearchParameters,
  workspaceSearchToolCapability,
} from "./workspace-search.js";

// The Gateway and OpenClaw plugins run in the same Host process. This symbol
// is an immutable, operation-only bridge: it exposes neither a tool factory
// nor a Runtime ticket to the capability handshake.
export const REGISTERED_TOOL_PROBE_SYMBOL = Symbol.for("huahuo.runtime.registered-tools.v1");

let workspaceSearchRegistration;
const registeredPluginAPIs = new WeakSet();

const registeredToolProbe = Object.freeze({
  probeWorkspaceSearch() {
    if (!workspaceSearchRegistration) return { ready: false, code: "WORKSPACE_SEARCH_PLUGIN_NOT_REGISTERED" };
    return workspaceSearchRegistration.probe();
  },
});

installRegisteredToolProbe();

export function registerWorkspaceSearchTool(api) {
  if (!api || (typeof api !== "object" && typeof api !== "function") || typeof api.registerTool !== "function") {
    throw registeredToolProbeError("WORKSPACE_SEARCH_PLUGIN_API_INVALID");
  }
  if (registeredPluginAPIs.has(api)) {
    throw registeredToolProbeError("WORKSPACE_SEARCH_PLUGIN_ALREADY_REGISTERED");
  }

  const factory = (context) => createWorkspaceSearchTool(context);
  // A plugin module is shared by full and discovery registries. Each distinct
  // OpenClaw API must receive the real registration; only full activation may
  // make the process-wide capability probe report the tool as ready.
  api.registerTool(factory, { names: [workspaceSearchToolCapability.name] });
  registeredPluginAPIs.add(api);
  if (registrationPublishesReadiness(api)) {
    const registration = Object.freeze({
      probe() {
        try {
          const tool = factory(Object.freeze({ runtime: Object.freeze({ capabilityProbe: true }) }));
          if (!tool || tool.name !== workspaceSearchToolCapability.name || tool.parameters !== workspaceSearchParameters ||
            typeof tool.execute !== "function") {
            return { ready: false, code: "WORKSPACE_SEARCH_REGISTERED_TOOL_INVALID" };
          }
          return { ready: true, code: "ready", ...workspaceSearchToolCapability };
        } catch {
          return { ready: false, code: "WORKSPACE_SEARCH_REGISTERED_TOOL_INVALID" };
        }
      }
    });
    if (registerLifecycleInvalidation(api, registration)) {
      workspaceSearchRegistration = registration;
    }
  }
  return workspaceSearchToolCapability;
}

function registrationPublishesReadiness(api) {
  // OpenClaw always supplies a lifecycle mode. Treat missing, malformed, and
  // future modes as non-dispatchable rather than allowing a test-shaped API to
  // make the process-wide capability handshake appear ready.
  return api.registrationMode === "full";
}

function registerLifecycleInvalidation(api, registration) {
  const lifecycle = api.lifecycle;
  if (!lifecycle || typeof lifecycle.registerRuntimeLifecycle !== "function") return false;
  try {
    lifecycle.registerRuntimeLifecycle({
      id: "huahuo-context-tools.workspace-search-registration",
      cleanup(context) {
        // Run-scoped cleanup must not make a still-active plugin look absent.
        // Registry replacement and process shutdown have no Run id and revoke
        // the process-wide probe until a new full registration completes.
        if (!context?.runId && workspaceSearchRegistration === registration) {
          workspaceSearchRegistration = undefined;
          registeredPluginAPIs.delete(api);
        }
      },
    });
    return true;
  } catch {
    return false;
  }
}

function installRegisteredToolProbe() {
  const existing = Object.getOwnPropertyDescriptor(globalThis, REGISTERED_TOOL_PROBE_SYMBOL);
  if (existing) {
    if (existing.value !== registeredToolProbe || existing.configurable || existing.writable || existing.enumerable) {
      throw registeredToolProbeError("WORKSPACE_SEARCH_PROBE_REGISTRY_CONFLICT");
    }
    return;
  }
  Object.defineProperty(globalThis, REGISTERED_TOOL_PROBE_SYMBOL, {
    value: registeredToolProbe,
    configurable: false,
    enumerable: false,
    writable: false,
  });
}

function registeredToolProbeError(code) {
  return Object.assign(new Error("workspace search registered-tool probe is unavailable"), { code });
}
