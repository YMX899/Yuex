import { definePluginEntry } from "openclaw/plugin-sdk/plugin-entry";
import { registerWorkspaceSearchTool } from "./src/registered-tool-probe.js";

export default definePluginEntry({
  id: "huahuo-context-tools",
  name: "Huahuo Context Tools",
  description: "Run-authorized path-only hybrid Workspace search backed by the Huahuo Search Service.",
  register(api) {
    registerWorkspaceSearchTool(api);
  },
});
