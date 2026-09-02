import { createHash } from "node:crypto";
import { mkdir, readdir, readFile, stat, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

// Filesystem discovery and the material compatibility adapter stay inside the
// Backend Search Service. The model can receive only this semantic catalog.
const REQUIRED_TOOLS = ["read", "workspace_search", "write"];
const FORBIDDEN_AGENT_FACING_TOOLS = ["ls", "find", "grep", "workspace_material_search", "huahuo_image_generate"];
const LEGACY_PROJECTION_AGENT_PROFILE = "feed_ai_agent";
const LEGACY_PROJECTION_TASK_TYPE = "feed_ai_chat";
const LEGACY_PROJECTION_STATUS = "legacy";
const SHA256_PATTERN = /^sha256:[0-9a-f]{64}$/;
const CATALOG_IDENTIFIER_PATTERN = /^[A-Za-z0-9_.-]+$/;
const META_WORKSPACE_KEY_PATTERN = /^workspace\.[a-z0-9]+(?:-[a-z0-9]+)*$/;
const PUBLIC_META_WORKSPACE_KEY_PATTERN = /^[a-z0-9]+(?:_[a-z0-9]+)*$/;
const INPUT_IMAGE_MIME_TYPES = new Set(["image/jpeg", "image/png", "image/webp"]);
const MAX_INPUT_FILES = 16;
const MAX_INPUT_BYTES = 100 * 1024 * 1024;
const MAX_INPUT_WIDTH = 8192;
const MAX_INPUT_HEIGHT = 8192;
const MAX_INPUT_PIXELS = 40_000_000;
const USER_WORKSPACE_TEMPLATE_PREFIX = "workspace/user-workspace/";
const V03_CANONICAL_USER_WORKSPACE_BOOTSTRAP = Object.freeze([
  { metaFileId: "workspace.user.AGENTS", relativePath: "workspace/user-workspace/AGENTS.md", kind: "workspace_standard_file" },
  { metaFileId: "workspace.user.SOUL", relativePath: "workspace/user-workspace/SOUL.md", kind: "workspace_standard_file" },
  { metaFileId: "workspace.user.USER", relativePath: "workspace/user-workspace/USER.md", kind: "workspace_standard_file" },
  { metaFileId: "workspace.user.TOOLS", relativePath: "workspace/user-workspace/TOOLS.md", kind: "workspace_standard_file" },
  { metaFileId: "workspace.user.MEMORY", relativePath: "workspace/user-workspace/MEMORY.md", kind: "workspace_standard_file" },
  { metaFileId: "workspace.user.profile.preference_boundaries", relativePath: "workspace/user-workspace/profile/preference-boundaries.md", kind: "workspace_standard_file" },
  { metaFileId: "workspace.user.profile.user_positioning", relativePath: "workspace/user-workspace/profile/user-positioning/positioning-profile.md", kind: "workspace_standard_file" },
  { metaFileId: "workspace.user.content", relativePath: "workspace/user-workspace/内容.md", kind: "workspace_standard_file" },
  { metaFileId: "workspace.user.resources.overview", relativePath: "workspace/user-workspace/resources/overview.md", kind: "workspace_generated_navigation" },
]);
const TASK_CAPABILITIES = new Set([
  "workspace_search",
  "workspace_read",
  "recording_read",
  "workspace_staging_write",
  "asset_write_intent",
]);
const EXECUTION_SCOPES = new Set(["product_thread", "detached_task"]);

export async function generateRuntimeContracts(options) {
  const opsSourceRoot = path.resolve(options.opsSourceRoot);
  const runtimeConfigRoot = path.resolve(options.runtimeConfigRoot);
  const agentSourceRoot = path.resolve(options.agentSourceRoot);
  const coreSnapshotPath = path.join(opsSourceRoot, "config", "openclaw-core-tool-schemas-2026.6.2.json");
  const coreSnapshot = JSON.parse(await readFile(coreSnapshotPath, "utf8"));
  await verifyOpenClawSource(coreSnapshot, options.openclawSourceRoot);

  const contextTools = await import(pathToFileURL(path.join(opsSourceRoot, "openclaw-extensions", "huahuo-context-tools", "src", "workspace-search.js")));
  const schemas = {
    read: coreSnapshot.schemas.read,
    workspace_search: contextTools.workspaceSearchParameters,
    write: coreSnapshot.schemas.write,
  };
  for (const name of REQUIRED_TOOLS) {
    if (!schemas[name] || typeof schemas[name] !== "object") throw new Error(`runtime_tool_schema_missing:${name}`);
  }
  const schemaHashes = Object.fromEntries(REQUIRED_TOOLS.map((name) => [name, stableIdentityHash(schemas[name])]));
  const maxToolCallsSupported = 400;
  const capabilityIdentity = {
    required: REQUIRED_TOOLS,
    schemaHashes,
    ready: true,
    maxToolCallsSupported,
    supportsPerRunBudget: true,
    supportsBudgetWarning: true,
    supportsForcedAbort: true,
  };
  const capabilityHash = stableIdentityHash(capabilityIdentity);

  const metaManifestPath = await firstExisting([
    path.join(agentSourceRoot, "meta-manifest.json"),
    path.join(agentSourceRoot, "templates", "meta-manifest.json"),
  ]);
  const routingManifestPath = await firstExisting([
    path.join(agentSourceRoot, "agent-routing-manifest.json"),
    path.join(agentSourceRoot, "templates", "agent-routing-manifest.json"),
  ]);
  const packageRoot = path.dirname(routingManifestPath);
  const metaManifest = JSON.parse(await readFile(metaManifestPath, "utf8"));
  const routingManifest = JSON.parse(await readFile(routingManifestPath, "utf8"));
  await assertV03UserWorkspaceBootstrapManifest(metaManifest, packageRoot);
  assertNoLegacyAgentFacingToolValues(routingManifest, "agent_routing_manifest");
  const agents = await loadAgents(packageRoot, routingManifest);
  const skills = await loadSkills(packageRoot, metaManifest, routingManifest, agents, options.runtimeSkillMirrorRoot);
  const publicWorkspaces = agents.filter((agent) => agent.publicSelectable).map(projectPublicWorkspace).sort((left, right) => left.metaWorkspaceKey.localeCompare(right.metaWorkspaceKey));
  const allowedAgents = Object.fromEntries(agents.map((entry) => [entry.agentProfile, true]));
  const allowedSkills = Object.fromEntries(skills.map((entry) => [entry.skillProfile, true]));
  const allowedKnowledge = Object.fromEntries([
    ...agents.flatMap((entry) => entry.knowledgeRoots),
    ...skills.flatMap((entry) => entry.knowledgeRefs),
  ].map((reference) => [reference, true]));
  const capabilities = {
    capabilityHash,
    // This is a build-time contract, never a live Runtime Host readiness claim.
    // The Host capability handshake supplies the only `ready` statuses.
    tools: REQUIRED_TOOLS.map((name) => ({ name, status: "degraded", schemaHash: schemaHashes[name] })),
    maxToolCallsSupported,
    supportsPerRunBudget: capabilityIdentity.supportsPerRunBudget,
    supportsBudgetWarning: capabilityIdentity.supportsBudgetWarning,
    supportsForcedAbort: capabilityIdentity.supportsForcedAbort,
  };
  const catalog = {
    manifest: { version: String(routingManifest.version), agents },
    publicWorkspaces,
    skills,
    capabilities,
    agentPermissions: { features: {}, membershipLevel: 1 },
    plannerPermissions: { allowedAgents, allowedSkills, allowedKnowledge, allowFormalWrite: true },
  };
  validateCatalog(catalog);

  const env = [
    "# This proves the generated schema contract only. Live tool readiness is set by the Runtime Host probe.",
    "HUAHUO_RUNTIME_CAPABILITY_CONTRACT_READY=true",
    ...REQUIRED_TOOLS.map((name) => `HUAHUO_TOOL_SCHEMA_HASH_${name.toUpperCase()}=${schemaHashes[name]}`),
    `HUAHUO_RUNTIME_CAPABILITY_HASH=${capabilityHash}`,
    "",
  ].join("\n");
  const catalogText = `${JSON.stringify(catalog, null, 2)}\n`;
  if (!options.verifyOnly) {
    await mkdir(runtimeConfigRoot, { recursive: true });
    await writeFile(path.join(runtimeConfigRoot, "runtime-capabilities.env"), env, { encoding: "utf8", mode: 0o640 });
    await writeFile(path.join(runtimeConfigRoot, "agent-planning-catalog.json"), catalogText, { encoding: "utf8", mode: 0o640 });
  } else {
    await verifyGeneratedFile(path.join(runtimeConfigRoot, "runtime-capabilities.env"), env);
    await verifyGeneratedFile(path.join(runtimeConfigRoot, "agent-planning-catalog.json"), catalogText);
  }
  return { capabilityHash, schemaHashes, agentCount: agents.length, skillCount: skills.length, verifyOnly: Boolean(options.verifyOnly) };
}

async function loadSkills(agentSourceRoot, metaManifest, routingManifest, agents, runtimeSkillMirrorRoot) {
  const entries = Array.isArray(metaManifest.files) ? metaManifest.files.filter((entry) => entry.kind === "runtime_skill" && entry.status === "active") : [];
  if (entries.length === 0) throw new Error("runtime_skill_manifest_empty");
  if (!Array.isArray(routingManifest.skills) || routingManifest.skills.length === 0) throw new Error("runtime_skill_routing_manifest_invalid");

  const activeEntries = new Map();
  for (const entry of entries) {
    const skillProfile = requiredIdentifier(String(entry.metaFileId || "").replace("runtime.skill.", ""), "runtime_skill_profile");
    if (activeEntries.has(skillProfile)) throw new Error(`runtime_skill_duplicate:${skillProfile}`);
    const relativePath = requiredRelativePath(entry.relativePath, `runtime_skill_path:${skillProfile}`);
    const sourcePath = path.join(agentSourceRoot, relativePath.replaceAll("/", path.sep));
    const body = await readFile(sourcePath);
    const actualHash = sha256(body);
    if (normalizeHash(entry.hash) !== actualHash) throw new Error(`runtime_skill_hash_mismatch:${skillProfile}`);
    assertNoLegacyRuntimeSkillToolNames(body.toString("utf8"), skillProfile);
    activeEntries.set(skillProfile, { actualHash, relativePath });
  }

  const knownAgents = new Set(agents.map((entry) => entry.agentProfile));
  const seen = new Set();
  const out = [];
  for (const source of routingManifest.skills) {
    const skillProfile = requiredIdentifier(source.skillProfile, "runtime_skill_profile");
    if (seen.has(skillProfile) || !activeEntries.has(skillProfile)) throw new Error(`runtime_skill_route_invalid:${skillProfile}`);
    seen.add(skillProfile);
    const taskTypes = requiredStringArray(source.taskTypes, `runtime_skill_task_types:${skillProfile}`);
    if (taskTypes.includes(LEGACY_PROJECTION_TASK_TYPE)) {
      throw new Error(`runtime_skill_legacy_task_forbidden:${skillProfile}`);
    }
    const allowedAgentProfiles = requiredIdentifierArray(source.allowedAgentProfiles, `runtime_skill_allowed_agents:${skillProfile}`);
    if (allowedAgentProfiles.includes(LEGACY_PROJECTION_AGENT_PROFILE)) {
      throw new Error(`runtime_skill_legacy_agent_forbidden:${skillProfile}`);
    }
    for (const agentProfile of allowedAgentProfiles) {
      if (!knownAgents.has(agentProfile)) throw new Error(`runtime_skill_unknown_agent:${skillProfile}:${agentProfile}`);
    }
    const requiredCapabilities = optionalIdentifierArray(source.requiredCapabilities, `runtime_skill_required_capabilities:${skillProfile}`);
    for (const capability of requiredCapabilities) {
      if (!TASK_CAPABILITIES.has(capability)) throw new Error(`runtime_skill_unknown_capability:${skillProfile}:${capability}`);
    }
    out.push({
      skillProfile,
      status: "active",
      hash: activeEntries.get(skillProfile).actualHash,
      taskTypes,
      intentCategories: optionalStringArray(source.intentCategories, `runtime_skill_intent_categories:${skillProfile}`),
      allowedAgentProfiles,
      requiredCapabilities,
      knowledgeRefs: optionalRelativePathArray(source.knowledgeRefs, `runtime_skill_knowledge_refs:${skillProfile}`),
      priority: normalizedPriority(source.priority, `runtime_skill_priority:${skillProfile}`),
    });
  }
  if (seen.size !== activeEntries.size) {
    const missing = [...activeEntries.keys()].filter((profile) => !seen.has(profile)).sort();
    throw new Error(`runtime_skill_route_missing:${missing.join(",")}`);
  }
  await assertRuntimeSkillManifestClosure(agentSourceRoot, activeEntries, runtimeSkillMirrorRoot);
  return out.sort((left, right) => left.skillProfile.localeCompare(right.skillProfile));
}

async function assertRuntimeSkillManifestClosure(agentSourceRoot, activeEntries, runtimeSkillMirrorRoot) {
  const expectedSkillFiles = new Map();
  for (const [skillProfile, entry] of activeEntries) {
    const relativePath = entry.relativePath.replaceAll("\\", "/");
    if (!new RegExp(`^runtime-skills/${escapeRegExp(skillProfile)}/SKILL\\.md$`).test(relativePath)) {
      throw new Error(`runtime_skill_manifest_path_invalid:${skillProfile}`);
    }
    const skillRelativePath = relativePath.slice("runtime-skills/".length);
    if (expectedSkillFiles.has(skillRelativePath)) throw new Error(`runtime_skill_manifest_duplicate_path:${relativePath}`);
    expectedSkillFiles.set(skillRelativePath, entry.actualHash);
  }
  await assertSkillFileSet(path.join(agentSourceRoot, "runtime-skills"), expectedSkillFiles, "runtime_skill_manifest_closure");
  if (!runtimeSkillMirrorRoot) return;
  await assertSkillFileSet(path.resolve(runtimeSkillMirrorRoot), expectedSkillFiles, "runtime_skill_manifest_closure_mirror");
}

async function assertSkillFileSet(skillRoot, expected, errorPrefix) {
  const actualPaths = await listSkillFiles(skillRoot);
  for (const relativePath of actualPaths) {
    if (!expected.has(relativePath)) throw new Error(`${errorPrefix}_undeclared:${relativePath}`);
  }
  for (const [relativePath, expectedHash] of expected) {
    if (!actualPaths.includes(relativePath)) throw new Error(`${errorPrefix}_missing:${relativePath}`);
    const actualHash = sha256(await readFile(path.join(skillRoot, relativePath.replaceAll("/", path.sep))));
    if (actualHash !== expectedHash) throw new Error(`${errorPrefix}_hash_mismatch:${relativePath}`);
  }
}

async function listSkillFiles(skillRoot) {
  let entries;
  try {
    entries = await readdir(skillRoot, { withFileTypes: true });
  } catch {
    throw new Error("runtime_skill_root_missing");
  }
  const files = [];
  async function walk(directory, prefix) {
    const children = directory === skillRoot ? entries : await readdir(directory, { withFileTypes: true });
    for (const child of children) {
      const relativePath = prefix ? `${prefix}/${child.name}` : child.name;
      if (child.isDirectory()) {
        await walk(path.join(directory, child.name), relativePath);
      } else if (child.isFile() && child.name === "SKILL.md") {
        files.push(relativePath);
      }
    }
  }
  await walk(skillRoot, "");
  return files.sort();
}

function assertNoLegacyAgentFacingToolValues(value, label) {
  if (Array.isArray(value)) {
    value.forEach((item, index) => assertNoLegacyAgentFacingToolValues(item, `${label}[${index}]`));
    return;
  }
  if (value && typeof value === "object") {
    for (const [key, child] of Object.entries(value)) assertNoLegacyAgentFacingToolValues(child, `${label}.${key}`);
    return;
  }
  if (typeof value === "string" && FORBIDDEN_AGENT_FACING_TOOLS.includes(value.trim())) {
    throw new Error(`agent_facing_legacy_tool_forbidden:${value.trim()}:${label}`);
  }
}

async function assertV03UserWorkspaceBootstrapManifest(manifest, packageRoot) {
  if (!Array.isArray(manifest?.files)) throw new Error("workspace_bootstrap_manifest_files_invalid");

  const expectedByID = new Map(V03_CANONICAL_USER_WORKSPACE_BOOTSTRAP.map((entry) => [entry.metaFileId, entry]));
  const seen = new Set();

  for (const entry of manifest.files) {
    const metaFileId = String(entry?.metaFileId || "").trim();
    const relativePath = String(entry?.relativePath || "").trim().replaceAll("\\", "/");
    if (!relativePath.startsWith(USER_WORKSPACE_TEMPLATE_PREFIX) || String(entry?.status || "").trim() !== "active") continue;

    const expected = expectedByID.get(metaFileId);
    if (!expected) {
      throw new Error(`workspace_bootstrap_legacy_active:${metaFileId || "missing_meta_file_id"}:${relativePath || "missing_relative_path"}`);
    }
    if (relativePath !== expected.relativePath || String(entry?.kind || "") !== expected.kind) {
      throw new Error(`workspace_bootstrap_canonical_mismatch:${metaFileId}:${relativePath || "missing_relative_path"}`);
    }
    if (seen.has(metaFileId)) throw new Error(`workspace_bootstrap_canonical_duplicate:${metaFileId}`);
    seen.add(metaFileId);
  }

  for (const expected of V03_CANONICAL_USER_WORKSPACE_BOOTSTRAP) {
    if (!seen.has(expected.metaFileId)) throw new Error(`workspace_bootstrap_canonical_missing:${expected.metaFileId}`);
  }

  await assertV03UserWorkspaceTemplateTree(packageRoot);
}

async function assertV03UserWorkspaceTemplateTree(packageRoot) {
  const templateRoot = path.join(packageRoot, "workspace", "user-workspace");
  const expected = new Set(V03_CANONICAL_USER_WORKSPACE_BOOTSTRAP.map((entry) => entry.relativePath.slice(USER_WORKSPACE_TEMPLATE_PREFIX.length)));
  const actual = new Set();

  async function walk(directory, prefix) {
    let children;
    try {
      children = await readdir(directory, { withFileTypes: true });
    } catch (error) {
      if (error?.code === "ENOENT") throw new Error("workspace_bootstrap_template_root_missing");
      throw new Error("workspace_bootstrap_template_root_unreadable");
    }
    for (const child of children) {
      const relativePath = prefix ? `${prefix}/${child.name}` : child.name;
      if (child.isSymbolicLink()) throw new Error(`workspace_bootstrap_template_symlink_forbidden:${relativePath}`);
      if (child.isDirectory()) {
        await walk(path.join(directory, child.name), relativePath);
        continue;
      }
      if (!child.isFile()) throw new Error(`workspace_bootstrap_template_entry_invalid:${relativePath}`);
      actual.add(relativePath);
    }
  }

  await walk(templateRoot, "");
  for (const relativePath of actual) {
    if (!expected.has(relativePath)) throw new Error(`workspace_bootstrap_template_legacy_file:${relativePath}`);
  }
  for (const relativePath of expected) {
    if (!actual.has(relativePath)) throw new Error(`workspace_bootstrap_template_file_missing:${relativePath}`);
  }
}

function assertNoLegacyRuntimeSkillToolNames(body, skillProfile) {
  assertNoLegacyRuntimeToolMentions(body, skillProfile, "runtime_skill_legacy_tool_forbidden");
}

function assertNoLegacyRuntimeAgentToolNames(body, agentProfile, fileName) {
  assertNoLegacyRuntimeToolMentions(body, `${agentProfile}:${fileName}`, "runtime_agent_legacy_tool_forbidden");
}

function assertNoLegacyRuntimeToolMentions(body, label, errorPrefix) {
  for (const tool of FORBIDDEN_AGENT_FACING_TOOLS) {
    const pattern = new RegExp(`(?:[\\x60"'])${escapeRegExp(tool)}(?:[\\x60"'])`, "gi");
    for (let match = pattern.exec(body); match; match = pattern.exec(body)) {
      const lineStart = body.lastIndexOf("\n", match.index) + 1;
      const lineEnd = body.indexOf("\n", match.index);
      const line = body.slice(lineStart, lineEnd === -1 ? body.length : lineEnd);
      const offset = match.index - lineStart;
      if (!isExplicitlyForbiddenLegacyToolMention(line, offset, match[0].length)) {
        throw new Error(`${errorPrefix}:${label}:${tool}`);
      }
    }
  }
}

function isExplicitlyForbiddenLegacyToolMention(line, toolOffset, toolLength) {
  const before = line.slice(0, toolOffset);
  const after = line.slice(toolOffset + toolLength);
  return /(?:do\s+not|don't|never|must\s+not|cannot|can't|forbidden|den(?:y|ied)|prohibited)\s*(?:call|use|invoke)?\s*$/i.test(before) ||
    /(?:is\s+not|not\s+(?:an?\s+)?(?:agent\s+)?tool|not\s+available|forbidden|must\s+not|cannot|can't|do\s+not\s+(?:call|use|invoke)|不得|禁止|不可|不能|不是|禁用)/i.test(after);
}

async function loadAgents(agentSourceRoot, routingManifest) {
  if (!routingManifest.version || !Array.isArray(routingManifest.agents) || routingManifest.agents.length === 0) throw new Error("agent_routing_manifest_invalid");
  const seen = new Set();
  const seenMetaWorkspaceKeys = new Set();
  const out = [];
  for (const source of routingManifest.agents) {
    const agentProfile = requiredIdentifier(source.agentProfile, "agent_profile");
    if (seen.has(agentProfile)) throw new Error(`agent_profile_invalid:${agentProfile}`);
    seen.add(agentProfile);
    const publicSelectable = requiredBoolean(source.publicSelectable, `agent_public_selectable:${agentProfile}`);
    const status = String(source.status || "").trim();
    const metaWorkspaceKey = status === LEGACY_PROJECTION_STATUS
      ? requiredMetaWorkspaceKey(source.metaWorkspaceKey, agentProfile)
      : publicSelectable
      ? requiredPublicMetaWorkspaceKey(source.metaWorkspaceKey, agentProfile)
      : requiredMetaWorkspaceKey(source.metaWorkspaceKey, agentProfile);
    if (seenMetaWorkspaceKeys.has(metaWorkspaceKey)) throw new Error(`agent_meta_workspace_key_duplicate:${metaWorkspaceKey}`);
    seenMetaWorkspaceKeys.add(metaWorkspaceKey);
    const inputPolicy = normalizeInputPolicy(source.inputPolicy, agentProfile);
    const inputPolicyHash = canonicalIdentityHash(inputPolicy);
    if (Object.hasOwn(source, "categories")) throw new Error(`agent_legacy_categories_forbidden:${agentProfile}`);
    if (status === LEGACY_PROJECTION_STATUS) {
      assertLegacyProjectionAgentDeclaration(source, agentProfile);
      if (publicSelectable) throw new Error(`agent_public_selectable_legacy:${agentProfile}`);
      continue;
    }
    if (status !== "active") throw new Error(`agent_status_invalid:${agentProfile}`);
    if (agentProfile === LEGACY_PROJECTION_AGENT_PROFILE) {
      throw new Error(`legacy_projection_active:${agentProfile}`);
    }
    const relativeRoot = requiredRelativePath(source.relativeRoot, `agent_relative_root:${agentProfile}`);
    if (relativeRoot !== `agents/${agentProfile}`) throw new Error(`agent_relative_root_invalid:${agentProfile}`);
    const agentRoot = path.join(agentSourceRoot, relativeRoot.replaceAll("/", path.sep));
    const body = await readFile(path.join(agentRoot, "AGENTS.md"));
    assertNoLegacyRuntimeAgentToolNames(body.toString("utf8"), agentProfile, "AGENTS.md");
    for (const fileName of ["SOUL.md", "TOOLS.md", "MEMORY.md"]) {
      try {
        const agentFile = await readFile(path.join(agentRoot, fileName), "utf8");
        assertNoLegacyRuntimeAgentToolNames(agentFile, agentProfile, fileName);
      } catch (error) {
        if (!error || error.code !== "ENOENT") throw error;
      }
    }
    const knowledgeRoots = optionalRelativePathArray(source.knowledgeRoots, `agent_knowledge_roots:${agentProfile}`);
    for (const root of knowledgeRoots) {
      let info;
      try {
        info = await stat(path.join(agentSourceRoot, relativeRoot.replaceAll("/", path.sep), root.replaceAll("/", path.sep)));
      } catch {
        throw new Error(`agent_knowledge_root_missing:${agentProfile}:${root}`);
      }
      if (!info.isDirectory()) throw new Error(`agent_knowledge_root_missing:${agentProfile}:${root}`);
    }
    const executionScopes = requiredStringArray(source.executionScopes, `agent_execution_scopes:${agentProfile}`);
    for (const scope of executionScopes) {
      if (!EXECUTION_SCOPES.has(scope)) throw new Error(`agent_execution_scope_invalid:${agentProfile}:${scope}`);
    }
    assertPublicSelectableExecutionScopes(agentProfile, publicSelectable, executionScopes);
    const taskTypes = requiredStringArray(source.taskTypes, `agent_task_types:${agentProfile}`);
    if (taskTypes.includes(LEGACY_PROJECTION_TASK_TYPE)) {
      throw new Error(`legacy_projection_task_active:${agentProfile}`);
    }
    const defaultTaskType = Object.hasOwn(source, "defaultTaskType")
      ? requiredIdentifier(source.defaultTaskType, `agent_default_task_type:${agentProfile}`)
      : "";
    if (publicSelectable && !defaultTaskType) {
      throw new Error(`agent_public_default_task_type_missing:${agentProfile}`);
    }
    if (defaultTaskType && !taskTypes.includes(defaultTaskType)) {
      throw new Error(`agent_default_task_type_invalid:${agentProfile}`);
    }
    out.push({
      agentProfile,
      displayName: requiredText(source.displayName, `agent_display_name:${agentProfile}`),
      status: "active",
      version: requiredText(source.version, `agent_version:${agentProfile}`),
      hash: sha256(body),
      metaWorkspaceKey,
      publicSelectable,
      inputPolicy,
      inputPolicyHash,
      relativeRoot,
      intentCategories: requiredStringArray(source.intentCategories, `agent_intent_categories:${agentProfile}`),
      taskTypes,
      ...(defaultTaskType ? { defaultTaskType } : {}),
      candidateSkillProfiles: requiredIdentifierArray(source.candidateSkillProfiles, `agent_candidate_skills:${agentProfile}`),
      knowledgeRoots,
      toolPolicyProfile: requiredIdentifier(source.toolPolicyProfile, `agent_tool_policy:${agentProfile}`),
      executionScopes,
      maxCandidateSkills: boundedCandidateSkillCount(source.maxCandidateSkills, agentProfile),
      requiredFeatures: optionalIdentifierArray(source.requiredFeatures, `agent_required_features:${agentProfile}`),
      minimumMembership: normalizedNonNegativeInteger(source.minimumMembership, `agent_minimum_membership:${agentProfile}`),
      priority: normalizedPriority(source.priority, `agent_priority:${agentProfile}`),
    });
  }
  return out.sort((left, right) => left.agentProfile.localeCompare(right.agentProfile));
}

function assertLegacyProjectionAgentDeclaration(source, agentProfile) {
  if (agentProfile !== LEGACY_PROJECTION_AGENT_PROFILE || source.legacyProjectionOnly !== true) {
    throw new Error(`legacy_projection_declaration_invalid:${agentProfile}`);
  }
  const relativeRoot = requiredRelativePath(source.relativeRoot, `agent_relative_root:${agentProfile}`);
  if (relativeRoot !== `agents/${LEGACY_PROJECTION_AGENT_PROFILE}`) {
    throw new Error(`legacy_projection_declaration_invalid:${agentProfile}`);
  }
  const taskTypes = requiredStringArray(source.taskTypes, `agent_task_types:${agentProfile}`);
  if (taskTypes.length !== 1 || taskTypes[0] !== LEGACY_PROJECTION_TASK_TYPE) {
    throw new Error(`legacy_projection_declaration_invalid:${agentProfile}`);
  }
  if (optionalStringArray(source.intentCategories, `agent_intent_categories:${agentProfile}`).length > 0 ||
      optionalStringArray(source.candidateSkillProfiles, `agent_candidate_skills:${agentProfile}`).length > 0 ||
      optionalStringArray(source.knowledgeRoots, `agent_knowledge_roots:${agentProfile}`).length > 0 ||
      optionalStringArray(source.executionScopes, `agent_execution_scopes:${agentProfile}`).length > 0 ||
      String(source.toolPolicyProfile || "").trim() !== "") {
    throw new Error(`legacy_projection_declaration_invalid:${agentProfile}`);
  }
}

async function verifyOpenClawSource(snapshot, openclawSourceRoot) {
  if (snapshot.openclawVersion !== "2026.6.2") throw new Error("openclaw_version_unpinned");
  if (!openclawSourceRoot) return;
  const root = path.resolve(openclawSourceRoot);
  const packageJson = JSON.parse(await readFile(path.join(root, "package.json"), "utf8"));
  if (packageJson.version !== snapshot.openclawVersion) throw new Error("openclaw_version_mismatch");
  for (const [name, source] of Object.entries(snapshot.sourceFiles)) {
    const actual = sha256(await readFile(path.join(root, source.relativePath.replaceAll("/", path.sep))));
    if (actual !== normalizeHash(source.sha256)) throw new Error(`openclaw_tool_source_mismatch:${name}`);
  }
}

function validateCatalog(catalog) {
  if (!SHA256_PATTERN.test(catalog.capabilities.capabilityHash)) throw new Error("runtime_capability_hash_invalid");
  for (const tool of catalog.capabilities.tools) if (!SHA256_PATTERN.test(tool.schemaHash)) throw new Error(`runtime_tool_hash_invalid:${tool.name}`);
  const seenMetaWorkspaceKeys = new Set();
  for (const agent of catalog.manifest.agents) {
    if (!SHA256_PATTERN.test(agent.hash)) throw new Error(`agent_hash_invalid:${agent.agentProfile}`);
    if (agent.agentProfile === LEGACY_PROJECTION_AGENT_PROFILE || agent.taskTypes.includes(LEGACY_PROJECTION_TASK_TYPE)) {
      throw new Error(`legacy_projection_catalog_active:${agent.agentProfile}`);
    }
    const publicSelectable = requiredBoolean(agent.publicSelectable, `agent_public_selectable:${agent.agentProfile}`);
    const metaWorkspaceKey = publicSelectable
      ? requiredPublicMetaWorkspaceKey(agent.metaWorkspaceKey, agent.agentProfile)
      : requiredMetaWorkspaceKey(agent.metaWorkspaceKey, agent.agentProfile);
    if (seenMetaWorkspaceKeys.has(metaWorkspaceKey)) throw new Error(`agent_meta_workspace_key_duplicate:${metaWorkspaceKey}`);
    seenMetaWorkspaceKeys.add(metaWorkspaceKey);
    assertPublicSelectableExecutionScopes(agent.agentProfile, publicSelectable, agent.executionScopes);
    const defaultTaskType = Object.hasOwn(agent, "defaultTaskType")
      ? requiredIdentifier(agent.defaultTaskType, `agent_default_task_type:${agent.agentProfile}`)
      : "";
    if (publicSelectable && !defaultTaskType) {
      throw new Error(`agent_public_default_task_type_missing:${agent.agentProfile}`);
    }
    if (defaultTaskType && !agent.taskTypes.includes(defaultTaskType)) {
      throw new Error(`agent_default_task_type_invalid:${agent.agentProfile}`);
    }
    const inputPolicy = normalizeInputPolicy(agent.inputPolicy, agent.agentProfile);
    if (agent.inputPolicyHash !== canonicalIdentityHash(inputPolicy)) throw new Error(`agent_input_policy_hash_invalid:${agent.agentProfile}`);
  }
  validatePublicWorkspaceCatalog(catalog);
  for (const skill of catalog.skills) if (!SHA256_PATTERN.test(skill.hash)) throw new Error(`skill_hash_invalid:${skill.skillProfile}`);
  const skills = new Map(catalog.skills.map((skill) => [skill.skillProfile, skill]));
  for (const agent of catalog.manifest.agents) {
    for (const candidate of agent.candidateSkillProfiles) {
      const skill = skills.get(candidate);
      if (!skill || !skill.allowedAgentProfiles.includes(agent.agentProfile)) {
        throw new Error(`runtime_agent_candidate_invalid:${agent.agentProfile}:${candidate}`);
      }
      if (!sharesValue(agent.taskTypes, skill.taskTypes)) {
        throw new Error(`runtime_agent_candidate_task_mismatch:${agent.agentProfile}:${candidate}`);
      }
    }
    for (const taskType of agent.taskTypes) {
      if (!agent.candidateSkillProfiles.some((profile) => skills.get(profile)?.taskTypes.includes(taskType))) {
        throw new Error(`runtime_agent_task_uncovered:${agent.agentProfile}:${taskType}`);
      }
    }
  }
  for (const skill of catalog.skills) {
    for (const reference of skill.knowledgeRefs) {
      for (const agentProfile of skill.allowedAgentProfiles) {
        const agent = catalog.manifest.agents.find((entry) => entry.agentProfile === agentProfile);
        if (!agent || !agent.knowledgeRoots.some((root) => reference === root || reference.startsWith(`${root}/`))) {
          throw new Error(`runtime_skill_knowledge_outside_agent_root:${skill.skillProfile}:${agentProfile}:${reference}`);
        }
      }
    }
  }
}

async function verifyGeneratedFile(filePath, expected) {
  const actual = await readFile(filePath, "utf8");
  if (actual !== expected) throw new Error(`generated_contract_stale:${path.basename(filePath)}`);
}

async function firstExisting(paths) {
  for (const candidate of paths) {
    try {
      await readFile(candidate);
      return candidate;
    } catch (error) {
      if (!error || error.code !== "ENOENT") throw error;
    }
  }
  throw new Error(`runtime_contract_source_missing:${paths.map((item) => path.basename(item)).join(",")}`);
}

function stableIdentityHash(value) { return sha256(Buffer.from(JSON.stringify(value), "utf8")); }
function canonicalIdentityHash(value) { return sha256(Buffer.from(canonicalJson(value), "utf8")); }
function sha256(value) { return `sha256:${createHash("sha256").update(value).digest("hex")}`; }
function canonicalJson(value) {
  if (Array.isArray(value)) return `[${value.map((entry) => canonicalJson(entry)).join(",")}]`;
  if (value && typeof value === "object") {
    return `{${Object.keys(value).sort().map((key) => `${JSON.stringify(key)}:${canonicalJson(value[key])}`).join(",")}}`;
  }
  return JSON.stringify(value);
}
function escapeRegExp(value) { return String(value).replace(/[.*+?^${}()|[\]\\]/g, "\\$&"); }
function normalizeHash(value) { const text = String(value || "").toLowerCase().trim(); return text.startsWith("sha256:") ? text : `sha256:${text}`; }
function requiredText(value, label) {
  const text = String(value || "").trim();
  if (!text) throw new Error(`${label}_missing`);
  return text;
}
function requiredIdentifier(value, label) {
  const text = requiredText(value, label);
  if (!CATALOG_IDENTIFIER_PATTERN.test(text)) throw new Error(`${label}_invalid`);
  return text;
}
function requiredMetaWorkspaceKey(value, agentProfile) {
  const key = requiredText(value, `agent_meta_workspace_key:${agentProfile}`);
  if (!META_WORKSPACE_KEY_PATTERN.test(key)) throw new Error(`agent_meta_workspace_key_invalid:${agentProfile}`);
  return key;
}
function requiredPublicMetaWorkspaceKey(value, agentProfile) {
  const key = requiredText(value, `agent_meta_workspace_key:${agentProfile}`);
  if (!PUBLIC_META_WORKSPACE_KEY_PATTERN.test(key)) throw new Error(`agent_public_meta_workspace_key_invalid:${agentProfile}`);
  return key;
}
function normalizeInputPolicy(value, agentProfile) {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error(`agent_input_policy_missing:${agentProfile}`);
  const usage = requiredIdentifier(value.usage, `agent_input_policy_usage:${agentProfile}`);
  const acceptsText = requiredBoolean(value.acceptsText, `agent_input_policy_accepts_text:${agentProfile}`);
  if (!acceptsText) throw new Error(`agent_input_policy_text_required:${agentProfile}`);
  const acceptedImageMimeTypes = optionalStringArray(value.acceptedImageMimeTypes, `agent_input_policy_image_mime_types:${agentProfile}`).sort();
  if (acceptedImageMimeTypes.some((mimeType) => !INPUT_IMAGE_MIME_TYPES.has(mimeType))) {
    throw new Error(`agent_input_policy_image_mime_type_invalid:${agentProfile}`);
  }
  const imageRequired = requiredBoolean(value.imageRequired, `agent_input_policy_image_required:${agentProfile}`);
  const maxFiles = boundedInputLimit(value.maxFiles, `agent_input_policy_max_files:${agentProfile}`, MAX_INPUT_FILES);
  const maxBytes = boundedInputLimit(value.maxBytes, `agent_input_policy_max_bytes:${agentProfile}`, MAX_INPUT_BYTES);
  const maxBytesPerFile = boundedInputLimit(value.maxBytesPerFile, `agent_input_policy_max_bytes_per_file:${agentProfile}`, MAX_INPUT_BYTES);
  const maxWidth = boundedInputLimit(value.maxWidth, `agent_input_policy_max_width:${agentProfile}`, MAX_INPUT_WIDTH);
  const maxHeight = boundedInputLimit(value.maxHeight, `agent_input_policy_max_height:${agentProfile}`, MAX_INPUT_HEIGHT);
  const maxPixels = boundedInputLimit(value.maxPixels, `agent_input_policy_max_pixels:${agentProfile}`, MAX_INPUT_PIXELS);
  if (acceptedImageMimeTypes.length === 0) {
    if (usage !== "none") throw new Error(`agent_input_policy_none_usage_required:${agentProfile}`);
    if (imageRequired || maxFiles !== 0 || maxBytes !== 0 || maxBytesPerFile !== 0 || maxWidth !== 0 || maxHeight !== 0 || maxPixels !== 0) throw new Error(`agent_input_policy_image_limits_invalid:${agentProfile}`);
  } else if (usage !== "primary_input" || maxFiles < 1 || maxBytes < 1 || maxBytesPerFile < 1 || maxBytesPerFile > maxBytes || maxWidth < 1 || maxHeight < 1 || maxPixels < 1 || maxPixels > maxWidth * maxHeight) {
    throw new Error(`agent_input_policy_image_limits_invalid:${agentProfile}`);
  }
  if (imageRequired && (usage !== "primary_input" || ![...INPUT_IMAGE_MIME_TYPES].every((mimeType) => acceptedImageMimeTypes.includes(mimeType)))) {
    throw new Error(`agent_primary_input_image_required:${agentProfile}`);
  }
  return { usage, acceptsText, acceptedImageMimeTypes, imageRequired, maxFiles, maxBytes, maxBytesPerFile, maxWidth, maxHeight, maxPixels };
}
function boundedInputLimit(value, label, maximum) {
  const number = Number(value);
  if (!Number.isInteger(number) || number < 0 || number > maximum) throw new Error(`${label}_invalid`);
  return number;
}
function requiredBoolean(value, label) {
  if (value === undefined) throw new Error(`${label}_missing`);
  if (typeof value !== "boolean") throw new Error(`${label}_invalid`);
  return value;
}
function projectPublicWorkspace(agent) {
  return {
    metaWorkspaceKey: agent.metaWorkspaceKey,
    displayName: agent.displayName,
    inputPolicy: agent.inputPolicy,
    inputPolicyHash: agent.inputPolicyHash,
  };
}
function validatePublicWorkspaceCatalog(catalog) {
  if (!Array.isArray(catalog.publicWorkspaces)) throw new Error("public_workspace_catalog_invalid");
  const expected = catalog.manifest.agents.filter((agent) => agent.publicSelectable).map(projectPublicWorkspace).sort((left, right) => left.metaWorkspaceKey.localeCompare(right.metaWorkspaceKey));
  if (JSON.stringify(catalog.publicWorkspaces) !== JSON.stringify(expected)) throw new Error("public_workspace_catalog_mismatch");
  for (const workspace of catalog.publicWorkspaces) {
    for (const internalField of ["agentProfile", "relativeRoot", "taskTypes", "defaultTaskType", "candidateSkillProfiles", "knowledgeRoots", "toolPolicyProfile", "executionScopes"]) {
      if (Object.hasOwn(workspace, internalField)) throw new Error(`public_workspace_catalog_internal_field:${internalField}`);
    }
    const metaWorkspaceKey = requiredPublicMetaWorkspaceKey(workspace.metaWorkspaceKey, "public_workspace");
    if (!requiredText(workspace.displayName, `public_workspace_display_name:${metaWorkspaceKey}`)) throw new Error(`public_workspace_display_name_missing:${metaWorkspaceKey}`);
    const inputPolicy = normalizeInputPolicy(workspace.inputPolicy, metaWorkspaceKey);
    if (workspace.inputPolicyHash !== canonicalIdentityHash(inputPolicy)) throw new Error(`public_workspace_input_policy_hash_invalid:${metaWorkspaceKey}`);
  }
}
function assertPublicSelectableExecutionScopes(agentProfile, publicSelectable, executionScopes) {
  if (publicSelectable && (executionScopes.length !== 1 || executionScopes[0] !== "product_thread")) {
    throw new Error(`agent_public_selectable_scope_invalid:${agentProfile}`);
  }
}
function requiredRelativePath(value, label) {
  const text = requiredText(value, label).replaceAll("\\", "/");
  if (text.startsWith("/") || text.includes(":") || text === "." || text === ".." || text.startsWith("../") || text.includes("/../") || path.posix.normalize(text) !== text) {
    throw new Error(`${label}_invalid`);
  }
  return text;
}
function requiredStringArray(value, label) {
  const items = optionalStringArray(value, label);
  if (items.length === 0) throw new Error(`${label}_missing`);
  return items;
}
function optionalStringArray(value, label) {
  if (value == null) return [];
  if (!Array.isArray(value)) throw new Error(`${label}_invalid`);
  const items = value.map((item) => String(item || "").trim());
  if (items.some((item) => !item) || new Set(items).size !== items.length) throw new Error(`${label}_invalid`);
  return items;
}
function requiredIdentifierArray(value, label) {
  const items = requiredStringArray(value, label);
  if (items.some((item) => !CATALOG_IDENTIFIER_PATTERN.test(item))) throw new Error(`${label}_invalid`);
  return items;
}
function optionalIdentifierArray(value, label) {
  const items = optionalStringArray(value, label);
  if (items.some((item) => !CATALOG_IDENTIFIER_PATTERN.test(item))) throw new Error(`${label}_invalid`);
  return items;
}
function optionalRelativePathArray(value, label) {
  const items = optionalStringArray(value, label);
  return items.map((item) => requiredRelativePath(item, label));
}
function normalizedPriority(value, label) {
  const number = Number(value);
  if (!Number.isInteger(number)) throw new Error(`${label}_invalid`);
  return number;
}
function normalizedNonNegativeInteger(value, label) {
  const number = Number(value ?? 0);
  if (!Number.isInteger(number) || number < 0) throw new Error(`${label}_invalid`);
  return number;
}
function boundedCandidateSkillCount(value, agentProfile) {
  const number = normalizedNonNegativeInteger(value, `agent_max_candidate_skills:${agentProfile}`);
  if (number < 1 || number > 8) throw new Error(`agent_max_candidate_skills_invalid:${agentProfile}`);
  return number;
}
function sharesValue(left, right) { return left.some((item) => right.includes(item)); }

function parseArgs(argv) {
  const values = {};
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === "--verify-only") values.verifyOnly = true;
    else if (arg.startsWith("--")) values[arg.slice(2)] = argv[++index];
  }
  const scriptDir = path.dirname(fileURLToPath(import.meta.url));
  return {
    opsSourceRoot: values["ops-source-root"] || path.resolve(scriptDir, ".."),
    agentSourceRoot: values["agent-source-root"], runtimeConfigRoot: values["runtime-config-root"],
    openclawSourceRoot: values["openclaw-source-root"], runtimeSkillMirrorRoot: values["runtime-skill-mirror-root"], verifyOnly: Boolean(values.verifyOnly),
  };
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const options = parseArgs(process.argv.slice(2));
  if (!options.agentSourceRoot || !options.runtimeConfigRoot) {
    process.stderr.write("usage: node generate_runtime_contracts.mjs --agent-source-root <path> --runtime-config-root <path> [--runtime-skill-mirror-root <path>] [--openclaw-source-root <path>] [--verify-only]\n");
    process.exitCode = 2;
  } else {
    generateRuntimeContracts(options).then((result) => process.stdout.write(`${JSON.stringify({ ok: true, ...result })}\n`)).catch((error) => {
      process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
      process.exitCode = 1;
    });
  }
}
