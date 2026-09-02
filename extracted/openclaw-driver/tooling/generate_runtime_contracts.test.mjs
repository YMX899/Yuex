import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdtemp, mkdir, readFile, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { generateRuntimeContracts } from "./generate_runtime_contracts.mjs";

const opsSourceRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

const V03_CANONICAL_USER_WORKSPACE_BOOTSTRAP = [
  { metaFileId: "workspace.user.AGENTS", relativePath: "workspace/user-workspace/AGENTS.md", kind: "workspace_standard_file" },
  { metaFileId: "workspace.user.SOUL", relativePath: "workspace/user-workspace/SOUL.md", kind: "workspace_standard_file" },
  { metaFileId: "workspace.user.USER", relativePath: "workspace/user-workspace/USER.md", kind: "workspace_standard_file" },
  { metaFileId: "workspace.user.TOOLS", relativePath: "workspace/user-workspace/TOOLS.md", kind: "workspace_standard_file" },
  { metaFileId: "workspace.user.MEMORY", relativePath: "workspace/user-workspace/MEMORY.md", kind: "workspace_standard_file" },
  { metaFileId: "workspace.user.profile.preference_boundaries", relativePath: "workspace/user-workspace/profile/preference-boundaries.md", kind: "workspace_standard_file" },
  { metaFileId: "workspace.user.profile.user_positioning", relativePath: "workspace/user-workspace/profile/user-positioning/positioning-profile.md", kind: "workspace_standard_file" },
  { metaFileId: "workspace.user.content", relativePath: "workspace/user-workspace/内容.md", kind: "workspace_standard_file" },
  { metaFileId: "workspace.user.resources.overview", relativePath: "workspace/user-workspace/resources/overview.md", kind: "workspace_generated_navigation" },
];

test("generates a backend-valid dynamic planning catalog from manifest facts", async () => {
  const fixture = await createFixture({
    agents: [dynamicAgent({
      agentProfile: "work_ai_agent",
      intentCategories: ["general_conversation"],
      taskTypes: ["work_ai_general_chat"],
      candidateSkillProfiles: ["general_chat"],
      knowledgeRoots: ["knowledge/general"],
    })],
    skills: [dynamicSkill({
      skillProfile: "general_chat",
      taskTypes: ["work_ai_general_chat"],
      intentCategories: ["general_conversation"],
      allowedAgentProfiles: ["work_ai_agent"],
    })],
    agentBodies: { work_ai_agent: "# Test Agent\n" },
    skillBodies: { general_chat: "# Test Skill\n" },
  });

  const result = await generateRuntimeContracts({ opsSourceRoot, agentSourceRoot: fixture.agentRoot, runtimeConfigRoot: fixture.configRoot });
  assert.equal(result.agentCount, 1);
  assert.equal(result.skillCount, 1);
  assert.match(result.capabilityHash, /^sha256:[0-9a-f]{64}$/);
  const env = await readFile(path.join(fixture.configRoot, "runtime-capabilities.env"), "utf8");
  assert.match(env, /HUAHUO_RUNTIME_CAPABILITY_CONTRACT_READY=true/);
  assert.match(env, /HUAHUO_TOOL_SCHEMA_HASH_WORKSPACE_SEARCH=sha256:[0-9a-f]{64}/);
  assert.doesNotMatch(env, /WORKSPACE_MATERIAL_SEARCH/);
  assert.doesNotMatch(env, /HUAHUO_IMAGE_GENERATE/);

  const catalog = JSON.parse(await readFile(path.join(fixture.configRoot, "agent-planning-catalog.json"), "utf8"));
  const agent = catalog.manifest.agents[0];
  const { inputPolicyHash, ...agentWithoutInputPolicyHash } = agent;
  assert.match(inputPolicyHash, /^sha256:[0-9a-f]{64}$/);
  assert.deepEqual(agentWithoutInputPolicyHash, {
    agentProfile: "work_ai_agent",
    displayName: "work_ai_agent display",
    status: "active",
    version: "v1",
    hash: hash("# Test Agent\n"),
    metaWorkspaceKey: "workspace.work-ai",
    publicSelectable: false,
    inputPolicy: textInputPolicy(),
    relativeRoot: "agents/work_ai_agent",
    intentCategories: ["general_conversation"],
    taskTypes: ["work_ai_general_chat"],
    candidateSkillProfiles: ["general_chat"],
    knowledgeRoots: ["knowledge/general"],
    toolPolicyProfile: "workspace_read_only",
    executionScopes: ["product_thread"],
    maxCandidateSkills: 1,
    requiredFeatures: [],
    minimumMembership: 0,
    priority: 100,
  });
  assert.equal(Object.hasOwn(agent, "categories"), false);
  assert.deepEqual(catalog.skills, [{
    skillProfile: "general_chat",
    status: "active",
    hash: hash("# Test Skill\n"),
    taskTypes: ["work_ai_general_chat"],
    intentCategories: ["general_conversation"],
    allowedAgentProfiles: ["work_ai_agent"],
    requiredCapabilities: [],
    knowledgeRefs: [],
    priority: 100,
  }]);
  assert.equal(catalog.plannerPermissions.allowedAgents.work_ai_agent, true);
  assert.equal(catalog.plannerPermissions.allowedSkills.general_chat, true);
  assert.equal(catalog.plannerPermissions.allowedKnowledge["knowledge/general"], true);
  assert.deepEqual(catalog.publicWorkspaces, []);
  assert.deepEqual(catalog.agentPermissions, { features: {}, membershipLevel: 1 });
  assert.equal(catalog.capabilities.maxToolCallsSupported, 400);
  assert.deepEqual(catalog.capabilities.tools.map((tool) => tool.name), ["read", "workspace_search", "write"]);
  assert.equal(catalog.capabilities.tools.some((tool) => tool.name === "huahuo_image_generate"), false);
  assert.equal(catalog.capabilities.tools.every((tool) => tool.status === "degraded"), true);
  await generateRuntimeContracts({ opsSourceRoot, agentSourceRoot: fixture.agentRoot, runtimeConfigRoot: fixture.configRoot, verifyOnly: true });
});

test("preserves App Workspace selection metadata for an active public Agent", async () => {
  const fixture = await createFixture({
    agents: [dynamicAgent({
      agentProfile: "work_ai_agent",
      metaWorkspaceKey: "work_ai",
      publicSelectable: true,
      defaultTaskType: "work_ai_general_chat",
      intentCategories: ["general_conversation"],
      taskTypes: ["work_ai_general_chat"],
      candidateSkillProfiles: ["general_chat"],
    })],
    skills: [dynamicSkill({
      skillProfile: "general_chat",
      taskTypes: ["work_ai_general_chat"],
      intentCategories: ["general_conversation"],
      allowedAgentProfiles: ["work_ai_agent"],
    })],
  });

  await generateRuntimeContracts({ opsSourceRoot, agentSourceRoot: fixture.agentRoot, runtimeConfigRoot: fixture.configRoot });
  const catalog = JSON.parse(await readFile(path.join(fixture.configRoot, "agent-planning-catalog.json"), "utf8"));
  assert.deepEqual(
    { metaWorkspaceKey: catalog.manifest.agents[0].metaWorkspaceKey, publicSelectable: catalog.manifest.agents[0].publicSelectable },
    { metaWorkspaceKey: "work_ai", publicSelectable: true },
  );
  assert.deepEqual(catalog.publicWorkspaces, [{
    metaWorkspaceKey: "work_ai",
    displayName: "work_ai_agent display",
    inputPolicy: textInputPolicy(),
    inputPolicyHash: catalog.manifest.agents[0].inputPolicyHash,
  }]);
  assert.equal(catalog.manifest.agents[0].defaultTaskType, "work_ai_general_chat");
  assert.equal(Object.keys(catalog.publicWorkspaces[0]).some((key) => ["agentProfile", "relativeRoot", "taskTypes", "defaultTaskType", "candidateSkillProfiles", "knowledgeRoots"].includes(key)), false);
});

test("publishes a sanitized visual Workspace with its required image input policy", async () => {
  const fixture = await createFixture({
    agents: [dynamicAgent({
      agentProfile: "visual_chat_agent",
      metaWorkspaceKey: "visual_chat",
      publicSelectable: true,
      defaultTaskType: "work_ai_visual_chat",
      inputPolicy: visualInputPolicy(),
      intentCategories: ["visual_chat"],
      taskTypes: ["work_ai_visual_chat"],
      candidateSkillProfiles: ["visual_chat_assistant"],
      knowledgeRoots: ["knowledge/self-media-creation", "knowledge/content-visual-design"],
    })],
    skills: [dynamicSkill({
      skillProfile: "visual_chat_assistant",
      taskTypes: ["work_ai_visual_chat"],
      intentCategories: ["visual_chat"],
      allowedAgentProfiles: ["visual_chat_agent"],
      requiredCapabilities: ["workspace_search", "workspace_read"],
      knowledgeRefs: ["knowledge/content-visual-design/OVERVIEW.md"],
    })],
  });

  await generateRuntimeContracts({ opsSourceRoot, agentSourceRoot: fixture.agentRoot, runtimeConfigRoot: fixture.configRoot });
  const catalog = JSON.parse(await readFile(path.join(fixture.configRoot, "agent-planning-catalog.json"), "utf8"));
  assert.deepEqual(catalog.publicWorkspaces, [{
    metaWorkspaceKey: "visual_chat",
    displayName: "visual_chat_agent display",
    inputPolicy: visualInputPolicy(),
    inputPolicyHash: catalog.manifest.agents[0].inputPolicyHash,
  }]);
  assert.equal(catalog.publicWorkspaces[0].inputPolicy.imageRequired, true);
  assert.deepEqual(catalog.publicWorkspaces[0].inputPolicy.acceptedImageMimeTypes, ["image/jpeg", "image/png", "image/webp"]);
  assert.deepEqual(
    {
      maxFiles: catalog.publicWorkspaces[0].inputPolicy.maxFiles,
      maxBytes: catalog.publicWorkspaces[0].inputPolicy.maxBytes,
      maxBytesPerFile: catalog.publicWorkspaces[0].inputPolicy.maxBytesPerFile,
      maxWidth: catalog.publicWorkspaces[0].inputPolicy.maxWidth,
      maxHeight: catalog.publicWorkspaces[0].inputPolicy.maxHeight,
      maxPixels: catalog.publicWorkspaces[0].inputPolicy.maxPixels,
    },
    { maxFiles: 1, maxBytes: 10485760, maxBytesPerFile: 10485760, maxWidth: 8192, maxHeight: 8192, maxPixels: 40000000 },
  );
  const requestAttachment = { resourceId: "resource_visual_001", usage: "primary_input" };
  assert.equal(requestAttachment.usage, catalog.publicWorkspaces[0].inputPolicy.usage);
  assert.equal(catalog.manifest.agents[0].defaultTaskType, "work_ai_visual_chat");
  assert.equal(JSON.stringify(catalog.publicWorkspaces), JSON.stringify(catalog.publicWorkspaces).replace(/agentProfile|relativeRoot|candidateSkillProfiles|knowledgeRoots|taskTypes|defaultTaskType/g, ""));
});

test("normalizes image MIME ordering before computing deterministic input policy hashes", async () => {
  const policies = [
    visualInputPolicy(),
    { ...visualInputPolicy(), acceptedImageMimeTypes: ["image/webp", "image/jpeg", "image/png"] },
  ];
  const inputPolicyHashes = [];
  for (const inputPolicy of policies) {
    const fixture = await createFixture({
      agents: [dynamicAgent({
        agentProfile: "visual_chat_agent",
        metaWorkspaceKey: "visual_chat",
        publicSelectable: true,
        defaultTaskType: "work_ai_visual_chat",
        inputPolicy,
        intentCategories: ["visual_chat"],
        taskTypes: ["work_ai_visual_chat"],
        candidateSkillProfiles: ["visual_chat_assistant"],
      })],
      skills: [dynamicSkill({
        skillProfile: "visual_chat_assistant",
        taskTypes: ["work_ai_visual_chat"],
        intentCategories: ["visual_chat"],
        allowedAgentProfiles: ["visual_chat_agent"],
      })],
    });
    await generateRuntimeContracts({ opsSourceRoot, agentSourceRoot: fixture.agentRoot, runtimeConfigRoot: fixture.configRoot });
    const catalog = JSON.parse(await readFile(path.join(fixture.configRoot, "agent-planning-catalog.json"), "utf8"));
    inputPolicyHashes.push(catalog.manifest.agents[0].inputPolicyHash);
  }
  assert.equal(inputPolicyHashes[0], inputPolicyHashes[1]);
});

test("rejects invalid App Workspace selection metadata", async () => {
  const base = () => dynamicAgent({
    agentProfile: "work_ai_agent",
    intentCategories: ["general_conversation"],
    taskTypes: ["work_ai_general_chat"],
    candidateSkillProfiles: ["general_chat"],
  });
  const skill = dynamicSkill({
    skillProfile: "general_chat",
    taskTypes: ["work_ai_general_chat"],
    intentCategories: ["general_conversation"],
    allowedAgentProfiles: ["work_ai_agent"],
  });
  const cases = [
    {
      name: "missing key",
      mutate: (agent) => delete agent.metaWorkspaceKey,
      error: /agent_meta_workspace_key:work_ai_agent_missing/,
    },
    {
      name: "non-boolean flag",
      mutate: (agent) => { agent.publicSelectable = "true"; },
      error: /agent_public_selectable:work_ai_agent_invalid/,
    },
    {
      name: "missing input policy",
      mutate: (agent) => delete agent.inputPolicy,
      error: /agent_input_policy_missing:work_ai_agent/,
    },
    {
      name: "unsupported image MIME type",
      mutate: (agent) => { agent.inputPolicy = { ...visualInputPolicy(), acceptedImageMimeTypes: ["image/gif"] }; },
      error: /agent_input_policy_image_mime_type_invalid:work_ai_agent/,
    },
    {
      name: "image input must declare primary attachment usage",
      mutate: (agent) => { agent.agentProfile = "visual_chat_agent"; agent.relativeRoot = "agents/visual_chat_agent"; agent.metaWorkspaceKey = "workspace.visual-chat"; agent.inputPolicy = { ...visualInputPolicy(), usage: "none" }; },
      error: /agent_input_policy_image_limits_invalid:visual_chat_agent/,
    },
    {
      name: "attachment-free input must use none usage",
      mutate: (agent) => { agent.inputPolicy = { ...textInputPolicy(), usage: "work_ai" }; },
      error: /agent_input_policy_none_usage_required:work_ai_agent/,
    },
    {
      name: "attachment-free input must use zero image dimensions",
      mutate: (agent) => { agent.inputPolicy = { ...textInputPolicy(), maxWidth: 1 }; },
      error: /agent_input_policy_image_limits_invalid:work_ai_agent/,
    },
    {
      name: "image input must declare non-zero dimensions",
      mutate: (agent) => { agent.agentProfile = "visual_chat_agent"; agent.relativeRoot = "agents/visual_chat_agent"; agent.metaWorkspaceKey = "workspace.visual-chat"; agent.inputPolicy = { ...visualInputPolicy(), maxPixels: 0 }; },
      error: /agent_input_policy_image_limits_invalid:visual_chat_agent/,
    },
    {
      name: "public Agent must use a stable product Workspace key",
      mutate: (agent) => { agent.publicSelectable = true; },
      error: /agent_public_meta_workspace_key_invalid:work_ai_agent/,
    },
    {
      name: "public Agent must declare its default task type",
      mutate: (agent) => { agent.publicSelectable = true; agent.metaWorkspaceKey = "work_ai"; },
      error: /agent_public_default_task_type_missing:work_ai_agent/,
    },
    {
      name: "default task type must belong to the Agent task types",
      mutate: (agent) => { agent.defaultTaskType = "workspace_lookup"; },
      error: /agent_default_task_type_invalid:work_ai_agent/,
    },
    {
      name: "public detached Agent",
      mutate: (agent) => { agent.publicSelectable = true; agent.metaWorkspaceKey = "work_ai"; agent.executionScopes = ["detached_task"]; },
      error: /agent_public_selectable_scope_invalid:work_ai_agent/,
    },
    {
      name: "public mixed-scope Agent",
      mutate: (agent) => { agent.publicSelectable = true; agent.metaWorkspaceKey = "work_ai"; agent.executionScopes = ["product_thread", "detached_task"]; },
      error: /agent_public_selectable_scope_invalid:work_ai_agent/,
    },
  ];
  for (const entry of cases) {
    const agent = base();
    entry.mutate(agent);
    const fixture = await createFixture({ agents: [agent], skills: [skill] });
    await assert.rejects(
      () => generateRuntimeContracts({ opsSourceRoot, agentSourceRoot: fixture.agentRoot, runtimeConfigRoot: fixture.configRoot }),
      entry.error,
      entry.name,
    );
  }
});

test("rejects duplicate App Workspace keys and public legacy projections", async () => {
  const skills = [dynamicSkill({
    skillProfile: "general_chat",
    taskTypes: ["work_ai_general_chat"],
    intentCategories: ["general_conversation"],
    allowedAgentProfiles: ["work_ai_agent", "workspace_research_agent"],
  })];
  const duplicateFixture = await createFixture({
    agents: [
      dynamicAgent({ agentProfile: "work_ai_agent", metaWorkspaceKey: "workspace.work-ai", intentCategories: ["general_conversation"], taskTypes: ["work_ai_general_chat"], candidateSkillProfiles: ["general_chat"] }),
      dynamicAgent({ agentProfile: "workspace_research_agent", metaWorkspaceKey: "workspace.work-ai", intentCategories: ["workspace_lookup"], taskTypes: ["work_ai_general_chat"], candidateSkillProfiles: ["general_chat"] }),
    ],
    skills,
  });
  await assert.rejects(
    () => generateRuntimeContracts({ opsSourceRoot, agentSourceRoot: duplicateFixture.agentRoot, runtimeConfigRoot: duplicateFixture.configRoot }),
    /agent_meta_workspace_key_duplicate:workspace\.work-ai/,
  );

  const legacy = legacyFeedAIProjectionAgent();
  legacy.publicSelectable = true;
  const legacyFixture = await createFixture({
    agents: [dynamicAgent({ agentProfile: "work_ai_agent", intentCategories: ["general_conversation"], taskTypes: ["work_ai_general_chat"], candidateSkillProfiles: ["general_chat"] }), legacy],
    skills: [dynamicSkill({ skillProfile: "general_chat", taskTypes: ["work_ai_general_chat"], intentCategories: ["general_conversation"], allowedAgentProfiles: ["work_ai_agent"] })],
  });
  await assert.rejects(
    () => generateRuntimeContracts({ opsSourceRoot, agentSourceRoot: legacyFixture.agentRoot, runtimeConfigRoot: legacyFixture.configRoot }),
    /agent_public_selectable_legacy:feed_ai_agent/,
  );
});

test("rejects each active V0.3-incompatible user Workspace bootstrap path", async () => {
  const legacyEntries = [
    {
      entry: { metaFileId: "workspace.user.content", relativePath: "workspace/user-workspace/content.md", kind: "workspace_standard_file" },
      error: /workspace_bootstrap_canonical_mismatch:workspace\.user\.content:workspace\/user-workspace\/content\.md/,
    },
    {
      entry: { metaFileId: "workspace.user.resources.index", relativePath: "workspace/user-workspace/resources/index.md", kind: "workspace_generated_navigation" },
      error: /workspace_bootstrap_legacy_active:workspace\.user\.resources\.index:workspace\/user-workspace\/resources\/index\.md/,
    },
    {
      entry: { metaFileId: "workspace.user.resources.asset_index", relativePath: "workspace/user-workspace/resources/asset-index.md", kind: "workspace_generated_navigation" },
      error: /workspace_bootstrap_legacy_active:workspace\.user\.resources\.asset_index:workspace\/user-workspace\/resources\/asset-index\.md/,
    },
    {
      entry: { metaFileId: "workspace.user.resources.material_index", relativePath: "workspace/user-workspace/resources/material-index.md", kind: "workspace_generated_navigation" },
      error: /workspace_bootstrap_legacy_active:workspace\.user\.resources\.material_index:workspace\/user-workspace\/resources\/material-index\.md/,
    },
    {
      entry: { metaFileId: "workspace.user.resources.text_file_index", relativePath: "workspace/user-workspace/resources/text-file-index.md", kind: "workspace_generated_navigation" },
      error: /workspace_bootstrap_legacy_active:workspace\.user\.resources\.text_file_index:workspace\/user-workspace\/resources\/text-file-index\.md/,
    },
    {
      entry: { metaFileId: "workspace.user.profile.assets.viewpoints.overview", relativePath: "workspace/user-workspace/profile/assets/viewpoints/overview.md", kind: "workspace_standard_file" },
      error: /workspace_bootstrap_legacy_active:workspace\.user\.profile\.assets\.viewpoints\.overview:workspace\/user-workspace\/profile\/assets\/viewpoints\/overview\.md/,
    },
    {
      entry: { metaFileId: "workspace.user.daily_assets.overview", relativePath: "workspace/user-workspace/daily-assets/overview.md", kind: "workspace_standard_file" },
      error: /workspace_bootstrap_legacy_active:workspace\.user\.daily_assets\.overview:workspace\/user-workspace\/daily-assets\/overview\.md/,
    },
    {
      entry: { metaFileId: "workspace.user.skills.README", relativePath: "workspace/user-workspace/skills/README.md", kind: "workspace_standard_file" },
      error: /workspace_bootstrap_legacy_active:workspace\.user\.skills\.README:workspace\/user-workspace\/skills\/README\.md/,
    },
  ];

  for (const legacy of legacyEntries) {
    const fixture = await createFixture({
      agents: [dynamicAgent({
        agentProfile: "work_ai_agent",
        intentCategories: ["general_conversation"],
        taskTypes: ["work_ai_general_chat"],
        candidateSkillProfiles: ["general_chat"],
      })],
      skills: [dynamicSkill({
        skillProfile: "general_chat",
        taskTypes: ["work_ai_general_chat"],
        intentCategories: ["general_conversation"],
        allowedAgentProfiles: ["work_ai_agent"],
      })],
      workspaceEntries: [...canonicalWorkspaceEntries(), { ...legacy.entry, status: "active" }],
    });

    await assert.rejects(
      () => generateRuntimeContracts({ opsSourceRoot, agentSourceRoot: fixture.agentRoot, runtimeConfigRoot: fixture.configRoot }),
      legacy.error,
    );
  }
});

test("requires exactly the nine active V0.3 canonical user Workspace bootstrap entries", async () => {
  const fixture = await createFixture({
    agents: [dynamicAgent({
      agentProfile: "work_ai_agent",
      intentCategories: ["general_conversation"],
      taskTypes: ["work_ai_general_chat"],
      candidateSkillProfiles: ["general_chat"],
    })],
    skills: [dynamicSkill({
      skillProfile: "general_chat",
      taskTypes: ["work_ai_general_chat"],
      intentCategories: ["general_conversation"],
      allowedAgentProfiles: ["work_ai_agent"],
    })],
    workspaceEntries: canonicalWorkspaceEntries().filter((entry) => entry.metaFileId !== "workspace.user.resources.overview"),
  });

  await assert.rejects(
    () => generateRuntimeContracts({ opsSourceRoot, agentSourceRoot: fixture.agentRoot, runtimeConfigRoot: fixture.configRoot }),
    /workspace_bootstrap_canonical_missing:workspace\.user\.resources\.overview/,
  );

  const duplicateFixture = await createFixture({
    agents: [dynamicAgent({
      agentProfile: "work_ai_agent",
      intentCategories: ["general_conversation"],
      taskTypes: ["work_ai_general_chat"],
      candidateSkillProfiles: ["general_chat"],
    })],
    skills: [dynamicSkill({
      skillProfile: "general_chat",
      taskTypes: ["work_ai_general_chat"],
      intentCategories: ["general_conversation"],
      allowedAgentProfiles: ["work_ai_agent"],
    })],
    workspaceEntries: [...canonicalWorkspaceEntries(), canonicalWorkspaceEntries()[0]],
  });

  await assert.rejects(
    () => generateRuntimeContracts({ opsSourceRoot, agentSourceRoot: duplicateFixture.agentRoot, runtimeConfigRoot: duplicateFixture.configRoot }),
    /workspace_bootstrap_canonical_duplicate:workspace\.user\.AGENTS/,
  );
});

test("rejects noncanonical physical user Workspace template files while allowing empty materials", async () => {
  const legacyPaths = [
    "content.md",
    "resources/index.md",
    "skills/general_chat/SKILL.md",
  ];
  for (const legacyPath of legacyPaths) {
    const fixture = await createFixture({
      agents: [dynamicAgent({
        agentProfile: "work_ai_agent",
        intentCategories: ["general_conversation"],
        taskTypes: ["work_ai_general_chat"],
        candidateSkillProfiles: ["general_chat"],
      })],
      skills: [dynamicSkill({
        skillProfile: "general_chat",
        taskTypes: ["work_ai_general_chat"],
        intentCategories: ["general_conversation"],
        allowedAgentProfiles: ["work_ai_agent"],
      })],
      workspaceTemplateFiles: [...canonicalWorkspaceTemplateFiles(), legacyPath],
    });

    await assert.rejects(
      () => generateRuntimeContracts({ opsSourceRoot, agentSourceRoot: fixture.agentRoot, runtimeConfigRoot: fixture.configRoot }),
      new RegExp(`workspace_bootstrap_template_legacy_file:${escapeRegExp(legacyPath)}`),
    );
  }

  const missingFixture = await createFixture({
    agents: [dynamicAgent({
      agentProfile: "work_ai_agent",
      intentCategories: ["general_conversation"],
      taskTypes: ["work_ai_general_chat"],
      candidateSkillProfiles: ["general_chat"],
    })],
    skills: [dynamicSkill({
      skillProfile: "general_chat",
      taskTypes: ["work_ai_general_chat"],
      intentCategories: ["general_conversation"],
      allowedAgentProfiles: ["work_ai_agent"],
    })],
    workspaceTemplateFiles: canonicalWorkspaceTemplateFiles().filter((relativePath) => relativePath !== "内容.md"),
  });

  await assert.rejects(
    () => generateRuntimeContracts({ opsSourceRoot, agentSourceRoot: missingFixture.agentRoot, runtimeConfigRoot: missingFixture.configRoot }),
    /workspace_bootstrap_template_file_missing:内容\.md/,
  );
});

test("fails closed when a runtime Skill manifest hash is stale", async () => {
  const fixture = await createFixture({
    agents: [dynamicAgent({
      agentProfile: "work_ai_agent",
      intentCategories: ["general_conversation"],
      taskTypes: ["work_ai_general_chat"],
      candidateSkillProfiles: ["general_chat"],
    })],
    skills: [dynamicSkill({
      skillProfile: "general_chat",
      taskTypes: ["work_ai_general_chat"],
      intentCategories: ["general_conversation"],
      allowedAgentProfiles: ["work_ai_agent"],
    })],
    skillBodies: { general_chat: "changed" },
    metaHashes: { general_chat: hash("old") },
  });
  await assert.rejects(
    () => generateRuntimeContracts({ opsSourceRoot, agentSourceRoot: fixture.agentRoot, runtimeConfigRoot: fixture.configRoot }),
    /runtime_skill_hash_mismatch:general_chat/,
  );
});

test("rejects an undeclared runtime Skill before generating a catalog", async () => {
  const fixture = await createFixture({
    agents: [dynamicAgent({
      agentProfile: "work_ai_agent",
      intentCategories: ["general_conversation"],
      taskTypes: ["work_ai_general_chat"],
      candidateSkillProfiles: ["general_chat"],
    })],
    skills: [dynamicSkill({
      skillProfile: "general_chat",
      taskTypes: ["work_ai_general_chat"],
      intentCategories: ["general_conversation"],
      allowedAgentProfiles: ["work_ai_agent"],
    })],
  });
  const orphan = path.join(fixture.agentRoot, "runtime-skills", "orphan_skill");
  await mkdir(orphan, { recursive: true });
  await writeFile(path.join(orphan, "SKILL.md"), "# orphan\n");

  await assert.rejects(
    () => generateRuntimeContracts({ opsSourceRoot, agentSourceRoot: fixture.agentRoot, runtimeConfigRoot: fixture.configRoot }),
    /runtime_skill_manifest_closure_undeclared:orphan_skill\/SKILL\.md/,
  );
});

test("rejects an undeclared release-runtime Skill mirror", async () => {
  const fixture = await createFixture({
    agents: [dynamicAgent({
      agentProfile: "work_ai_agent",
      intentCategories: ["general_conversation"],
      taskTypes: ["work_ai_general_chat"],
      candidateSkillProfiles: ["general_chat"],
    })],
    skills: [dynamicSkill({
      skillProfile: "general_chat",
      taskTypes: ["work_ai_general_chat"],
      intentCategories: ["general_conversation"],
      allowedAgentProfiles: ["work_ai_agent"],
    })],
  });
  const mirrorRoot = path.join(fixture.configRoot, "release-runtime-skills");
  await mkdir(path.join(mirrorRoot, "general_chat"), { recursive: true });
  await writeFile(path.join(mirrorRoot, "general_chat", "SKILL.md"), await readFile(path.join(fixture.agentRoot, "runtime-skills", "general_chat", "SKILL.md")));
  await mkdir(path.join(mirrorRoot, "orphan_skill"), { recursive: true });
  await writeFile(path.join(mirrorRoot, "orphan_skill", "SKILL.md"), "# orphan\n");

  await assert.rejects(
    () => generateRuntimeContracts({ opsSourceRoot, agentSourceRoot: fixture.agentRoot, runtimeConfigRoot: fixture.configRoot, runtimeSkillMirrorRoot: mirrorRoot }),
    /runtime_skill_manifest_closure_mirror_undeclared:orphan_skill\/SKILL\.md/,
  );
});

test("rejects a legacy Agent-facing tool named by an active runtime Skill", async () => {
  const fixture = await createFixture({
    agents: [dynamicAgent({
      agentProfile: "work_ai_agent",
      intentCategories: ["general_conversation"],
      taskTypes: ["work_ai_general_chat"],
      candidateSkillProfiles: ["general_chat"],
    })],
    skills: [dynamicSkill({
      skillProfile: "general_chat",
      taskTypes: ["work_ai_general_chat"],
      intentCategories: ["general_conversation"],
      allowedAgentProfiles: ["work_ai_agent"],
    })],
    skillBodies: { general_chat: "# general_chat\nCall `workspace_material_search`.\n" },
  });

  await assert.rejects(
    () => generateRuntimeContracts({ opsSourceRoot, agentSourceRoot: fixture.agentRoot, runtimeConfigRoot: fixture.configRoot }),
    /runtime_skill_legacy_tool_forbidden:general_chat:workspace_material_search/,
  );
});

test("rejects a legacy Agent-facing tool named by an active Agent document", async () => {
  const fixture = await createFixture({
    agents: [dynamicAgent({
      agentProfile: "work_ai_agent",
      intentCategories: ["general_conversation"],
      taskTypes: ["work_ai_general_chat"],
      candidateSkillProfiles: ["general_chat"],
    })],
    skills: [dynamicSkill({
      skillProfile: "general_chat",
      taskTypes: ["work_ai_general_chat"],
      intentCategories: ["general_conversation"],
      allowedAgentProfiles: ["work_ai_agent"],
    })],
    agentBodies: { work_ai_agent: "# Test Agent\nUse `grep`.\n" },
  });

  await assert.rejects(
    () => generateRuntimeContracts({ opsSourceRoot, agentSourceRoot: fixture.agentRoot, runtimeConfigRoot: fixture.configRoot }),
    /runtime_agent_legacy_tool_forbidden:work_ai_agent:AGENTS\.md:grep/,
  );
});

test("rejects an unsupported image-generation tool named by an active Agent document", async () => {
  const fixture = await createFixture({
    agents: [dynamicAgent({
      agentProfile: "work_ai_agent",
      intentCategories: ["general_conversation"],
      taskTypes: ["work_ai_general_chat"],
      candidateSkillProfiles: ["general_chat"],
    })],
    skills: [dynamicSkill({
      skillProfile: "general_chat",
      taskTypes: ["work_ai_general_chat"],
      intentCategories: ["general_conversation"],
      allowedAgentProfiles: ["work_ai_agent"],
    })],
    agentBodies: { work_ai_agent: "# Test Agent\nUse `huahuo_image_generate`.\n" },
  });

  await assert.rejects(
    () => generateRuntimeContracts({ opsSourceRoot, agentSourceRoot: fixture.agentRoot, runtimeConfigRoot: fixture.configRoot }),
    /runtime_agent_legacy_tool_forbidden:work_ai_agent:AGENTS\.md:huahuo_image_generate/,
  );
});

test("allows an explicit denial of a legacy Agent-facing tool without registering it", async () => {
  const fixture = await createFixture({
    agents: [dynamicAgent({
      agentProfile: "work_ai_agent",
      intentCategories: ["general_conversation"],
      taskTypes: ["work_ai_general_chat"],
      candidateSkillProfiles: ["general_chat"],
    })],
    skills: [dynamicSkill({
      skillProfile: "general_chat",
      taskTypes: ["work_ai_general_chat"],
      intentCategories: ["general_conversation"],
      allowedAgentProfiles: ["work_ai_agent"],
    })],
    agentBodies: { work_ai_agent: "# Test Agent\n`grep` is not an Agent tool and must not be called.\n" },
  });

  const result = await generateRuntimeContracts({ opsSourceRoot, agentSourceRoot: fixture.agentRoot, runtimeConfigRoot: fixture.configRoot });
  assert.equal(result.agentCount, 1);
});

test("rejects legacy categories-only agent entries before publishing a catalog", async () => {
  const agent = dynamicAgent({
    agentProfile: "work_ai_agent",
    intentCategories: ["general_conversation"],
    taskTypes: ["work_ai_general_chat"],
    candidateSkillProfiles: ["general_chat"],
  });
  delete agent.intentCategories;
  delete agent.candidateSkillProfiles;
  agent.categories = ["general_conversation"];
  const fixture = await createFixture({
    agents: [agent],
    skills: [dynamicSkill({
      skillProfile: "general_chat",
      taskTypes: ["work_ai_general_chat"],
      intentCategories: ["general_conversation"],
      allowedAgentProfiles: ["work_ai_agent"],
    })],
  });
  await assert.rejects(
    () => generateRuntimeContracts({ opsSourceRoot, agentSourceRoot: fixture.agentRoot, runtimeConfigRoot: fixture.configRoot }),
    /agent_legacy_categories_forbidden:work_ai_agent/,
  );
});

test("rejects missing required dynamic Agent facts", async () => {
  const agent = dynamicAgent({
    agentProfile: "work_ai_agent",
    intentCategories: ["general_conversation"],
    taskTypes: ["work_ai_general_chat"],
    candidateSkillProfiles: ["general_chat"],
  });
  delete agent.intentCategories;
  const fixture = await createFixture({
    agents: [agent],
    skills: [dynamicSkill({
      skillProfile: "general_chat",
      taskTypes: ["work_ai_general_chat"],
      intentCategories: ["general_conversation"],
      allowedAgentProfiles: ["work_ai_agent"],
    })],
  });
  await assert.rejects(
    () => generateRuntimeContracts({ opsSourceRoot, agentSourceRoot: fixture.agentRoot, runtimeConfigRoot: fixture.configRoot }),
    /agent_intent_categories:work_ai_agent_missing/,
  );
});

test("rejects a legacy categories field even when dynamic facts are present", async () => {
  const agent = dynamicAgent({
    agentProfile: "work_ai_agent",
    intentCategories: ["general_conversation"],
    taskTypes: ["work_ai_general_chat"],
    candidateSkillProfiles: ["general_chat"],
  });
  agent.categories = ["general_conversation"];
  const fixture = await createFixture({
    agents: [agent],
    skills: [dynamicSkill({
      skillProfile: "general_chat",
      taskTypes: ["work_ai_general_chat"],
      intentCategories: ["general_conversation"],
      allowedAgentProfiles: ["work_ai_agent"],
    })],
  });
  await assert.rejects(
    () => generateRuntimeContracts({ opsSourceRoot, agentSourceRoot: fixture.agentRoot, runtimeConfigRoot: fixture.configRoot }),
    /agent_legacy_categories_forbidden:work_ai_agent/,
  );
});

test("quarantines the legacy FeedAI projection outside the generated dynamic catalog", async () => {
  const fixture = await createFixture({
    agents: [
      dynamicAgent({
        agentProfile: "work_ai_agent",
        intentCategories: ["general_conversation"],
        taskTypes: ["work_ai_general_chat"],
        candidateSkillProfiles: ["general_chat"],
      }),
      legacyFeedAIProjectionAgent(),
    ],
    skills: [dynamicSkill({
      skillProfile: "general_chat",
      taskTypes: ["work_ai_general_chat"],
      intentCategories: ["general_conversation"],
      allowedAgentProfiles: ["work_ai_agent"],
    })],
  });

  const result = await generateRuntimeContracts({ opsSourceRoot, agentSourceRoot: fixture.agentRoot, runtimeConfigRoot: fixture.configRoot });
  assert.equal(result.agentCount, 1);
  const catalog = JSON.parse(await readFile(path.join(fixture.configRoot, "agent-planning-catalog.json"), "utf8"));
  assert.equal(catalog.manifest.agents.some((agent) => agent.agentProfile === "feed_ai_agent"), false);
  assert.equal(catalog.manifest.agents.some((agent) => agent.taskTypes.includes("feed_ai_chat")), false);
  assert.equal(Object.hasOwn(catalog.plannerPermissions.allowedAgents, "feed_ai_agent"), false);
  await generateRuntimeContracts({ opsSourceRoot, agentSourceRoot: fixture.agentRoot, runtimeConfigRoot: fixture.configRoot, verifyOnly: true });
});

test("rejects attempts to reactivate the legacy FeedAI planner route", async () => {
  const fixture = await createFixture({
    agents: [dynamicAgent({
      agentProfile: "feed_ai_agent",
      intentCategories: ["profile_understanding"],
      taskTypes: ["feed_ai_chat"],
      candidateSkillProfiles: ["positioning_profile_builder"],
    })],
    skills: [dynamicSkill({
      skillProfile: "positioning_profile_builder",
      taskTypes: ["feed_ai_chat"],
      intentCategories: ["profile_understanding"],
      allowedAgentProfiles: ["feed_ai_agent"],
    })],
  });

  await assert.rejects(
    () => generateRuntimeContracts({ opsSourceRoot, agentSourceRoot: fixture.agentRoot, runtimeConfigRoot: fixture.configRoot }),
    /legacy_projection_active:feed_ai_agent/,
  );
});

test("preserves Huoke strategy and content candidates without a fixed generator task map", async () => {
  const fixture = await createFixture({
    agents: [dynamicAgent({
      agentProfile: "huoke_neirong_agent",
      intentCategories: ["content_creation", "topic_generation"],
      taskTypes: ["work_ai_huoke_topic_strategy", "work_ai_huoke_content"],
      candidateSkillProfiles: ["huoke_topic_strategy", "huoke_content_creation"],
      knowledgeRoots: ["knowledge/huoke-full-funnel"],
      toolPolicyProfile: "workspace_staging_write",
      priority: 90,
    })],
    skills: [
      dynamicSkill({
        skillProfile: "huoke_topic_strategy",
        taskTypes: ["work_ai_huoke_topic_strategy"],
        intentCategories: ["topic_generation"],
        allowedAgentProfiles: ["huoke_neirong_agent"],
        requiredCapabilities: ["workspace_read", "workspace_staging_write"],
      }),
      dynamicSkill({
        skillProfile: "huoke_content_creation",
        taskTypes: ["work_ai_huoke_content"],
        intentCategories: ["content_creation"],
        allowedAgentProfiles: ["huoke_neirong_agent"],
        requiredCapabilities: ["workspace_search", "workspace_read"],
      }),
    ],
  });
  await generateRuntimeContracts({ opsSourceRoot, agentSourceRoot: fixture.agentRoot, runtimeConfigRoot: fixture.configRoot });
  const catalog = JSON.parse(await readFile(path.join(fixture.configRoot, "agent-planning-catalog.json"), "utf8"));
  assert.deepEqual(catalog.manifest.agents[0].candidateSkillProfiles, ["huoke_topic_strategy", "huoke_content_creation"]);
  assert.equal(catalog.manifest.agents[0].toolPolicyProfile, "workspace_staging_write");
  const strategy = catalog.skills.find((skill) => skill.skillProfile === "huoke_topic_strategy");
  assert.deepEqual(strategy.requiredCapabilities, ["workspace_read", "workspace_staging_write"]);
});

test("preserves public Faya routing and package knowledge roots in planner permissions", async () => {
  const fixture = await createFixture({
    agents: [
      dynamicAgent({
        agentProfile: "faya_agent",
        metaWorkspaceKey: "faya_germination",
        publicSelectable: true,
        defaultTaskType: "work_ai_faya_germination",
        intentCategories: ["viewpoint_germination"],
        taskTypes: ["work_ai_faya_germination"],
        candidateSkillProfiles: ["viewpoint_germination"],
        knowledgeRoots: ["knowledge/viewpoint-germination"],
      }),
      dynamicAgent({
        agentProfile: "self_media_creation_agent",
        intentCategories: ["self_media_creation"],
        taskTypes: ["work_ai_self_media_creation"],
        candidateSkillProfiles: ["self_media_creation_advisor"],
        knowledgeRoots: ["knowledge/self-media-creation"],
      }),
    ],
    skills: [
      dynamicSkill({
        skillProfile: "viewpoint_germination",
        taskTypes: ["work_ai_faya_germination"],
        intentCategories: ["viewpoint_germination"],
        allowedAgentProfiles: ["faya_agent"],
        requiredCapabilities: ["workspace_search", "workspace_read"],
      }),
      dynamicSkill({
        skillProfile: "self_media_creation_advisor",
        taskTypes: ["work_ai_self_media_creation"],
        intentCategories: ["self_media_creation"],
        allowedAgentProfiles: ["self_media_creation_agent"],
        requiredCapabilities: ["workspace_search", "workspace_read"],
      }),
    ],
  });
  await generateRuntimeContracts({ opsSourceRoot, agentSourceRoot: fixture.agentRoot, runtimeConfigRoot: fixture.configRoot });
  const catalog = JSON.parse(await readFile(path.join(fixture.configRoot, "agent-planning-catalog.json"), "utf8"));
  const faya = catalog.manifest.agents.find((agent) => agent.agentProfile === "faya_agent");
  assert.equal(faya.defaultTaskType, "work_ai_faya_germination");
  assert.deepEqual(catalog.publicWorkspaces, [{
    metaWorkspaceKey: "faya_germination",
    displayName: "faya_agent display",
    inputPolicy: textInputPolicy(),
    inputPolicyHash: faya.inputPolicyHash,
  }]);
  assert.equal(Object.keys(catalog.publicWorkspaces[0]).some((key) => ["agentProfile", "relativeRoot", "taskTypes", "defaultTaskType", "candidateSkillProfiles", "knowledgeRoots"].includes(key)), false);
  assert.equal(catalog.plannerPermissions.allowedKnowledge["knowledge/viewpoint-germination"], true);
  assert.equal(catalog.plannerPermissions.allowedKnowledge["knowledge/self-media-creation"], true);
  assert.deepEqual(catalog.skills.find((skill) => skill.skillProfile === "viewpoint_germination").allowedAgentProfiles, ["faya_agent"]);
  assert.deepEqual(catalog.skills.find((skill) => skill.skillProfile === "self_media_creation_advisor").allowedAgentProfiles, ["self_media_creation_agent"]);
});

function dynamicAgent({
  agentProfile,
  metaWorkspaceKey = `workspace.${agentProfile.replace(/_agent$/, "").replaceAll("_", "-")}`,
  publicSelectable = false,
  inputPolicy = textInputPolicy(),
  intentCategories,
  taskTypes,
  defaultTaskType,
  candidateSkillProfiles,
  knowledgeRoots = [],
  toolPolicyProfile = "workspace_read_only",
  executionScopes = ["product_thread"],
  priority = 100,
}) {
  const agent = {
    agentProfile,
    displayName: `${agentProfile} display`,
    status: "active",
    version: "v1",
    metaWorkspaceKey,
    publicSelectable,
    inputPolicy,
    relativeRoot: `agents/${agentProfile}`,
    intentCategories,
    taskTypes,
    candidateSkillProfiles,
    knowledgeRoots,
    toolPolicyProfile,
    executionScopes,
    maxCandidateSkills: 1,
    priority,
  };
  if (defaultTaskType !== undefined) agent.defaultTaskType = defaultTaskType;
  return agent;
}

function legacyFeedAIProjectionAgent() {
  return {
    agentProfile: "feed_ai_agent",
    displayName: "Feed AI legacy projection",
    status: "legacy",
    version: "legacy-v1",
    metaWorkspaceKey: "workspace.feed-ai-legacy",
    publicSelectable: false,
    inputPolicy: textInputPolicy(),
    legacyProjectionOnly: true,
    relativeRoot: "agents/feed_ai_agent",
    taskTypes: ["feed_ai_chat"],
  };
}

function textInputPolicy() {
  return {
    usage: "none",
    acceptsText: true,
    acceptedImageMimeTypes: [],
    imageRequired: false,
    maxFiles: 0,
    maxBytes: 0,
    maxBytesPerFile: 0,
    maxWidth: 0,
    maxHeight: 0,
    maxPixels: 0,
  };
}

function visualInputPolicy() {
  return {
    usage: "primary_input",
    acceptsText: true,
    acceptedImageMimeTypes: ["image/jpeg", "image/png", "image/webp"],
    imageRequired: true,
    maxFiles: 1,
    maxBytes: 10485760,
    maxBytesPerFile: 10485760,
    maxWidth: 8192,
    maxHeight: 8192,
    maxPixels: 40000000,
  };
}

function dynamicSkill({
  skillProfile,
  taskTypes,
  intentCategories = [],
  allowedAgentProfiles,
  requiredCapabilities = [],
  knowledgeRefs = [],
  priority = 100,
}) {
  return { skillProfile, taskTypes, intentCategories, allowedAgentProfiles, requiredCapabilities, knowledgeRefs, priority };
}

async function createFixture({
  agents,
  skills,
  agentBodies = {},
  skillBodies = {},
  metaHashes = {},
  workspaceEntries = canonicalWorkspaceEntries(),
  workspaceTemplateFiles = canonicalWorkspaceTemplateFiles(),
}) {
  const root = await mkdtemp(path.join(os.tmpdir(), "huahuo-runtime-contracts-"));
  const agentRoot = path.join(root, "agent");
  const configRoot = path.join(root, "config");
  const workspaceTemplateRoot = path.join(agentRoot, "workspace", "user-workspace");
  await mkdir(path.join(workspaceTemplateRoot, "materials"), { recursive: true });
  for (const relativePath of workspaceTemplateFiles) {
    const destination = path.join(workspaceTemplateRoot, relativePath.replaceAll("/", path.sep));
    await mkdir(path.dirname(destination), { recursive: true });
    await writeFile(destination, `# ${relativePath}\n`);
  }
  for (const agent of agents) {
    const relativeRoot = agent.relativeRoot || `agents/${agent.agentProfile}`;
    const directory = path.join(agentRoot, relativeRoot);
    await mkdir(directory, { recursive: true });
    const body = agentBodies[agent.agentProfile] || `# ${agent.agentProfile}\n`;
    await writeFile(path.join(directory, "AGENTS.md"), body);
    for (const knowledgeRoot of agent.knowledgeRoots || []) {
      await mkdir(path.join(directory, knowledgeRoot), { recursive: true });
    }
  }
  const files = [];
  for (const skill of skills) {
    const directory = path.join(agentRoot, "runtime-skills", skill.skillProfile);
    await mkdir(directory, { recursive: true });
    const body = skillBodies[skill.skillProfile] || `# ${skill.skillProfile}\n`;
    await writeFile(path.join(directory, "SKILL.md"), body);
    files.push({
      metaFileId: `runtime.skill.${skill.skillProfile}`,
      relativePath: `runtime-skills/${skill.skillProfile}/SKILL.md`,
      kind: "runtime_skill",
      status: "active",
      hash: metaHashes[skill.skillProfile] || hash(body),
    });
  }
  for (const entry of workspaceEntries) {
    files.push({
      ...entry,
      status: entry.status || "active",
      hash: hash(`${entry.metaFileId}:${entry.relativePath}`),
    });
  }
  await writeFile(path.join(agentRoot, "agent-routing-manifest.json"), JSON.stringify({ version: "test-v2", agents, skills }));
  await writeFile(path.join(agentRoot, "meta-manifest.json"), JSON.stringify({ files }));
  return { agentRoot, configRoot };
}

function canonicalWorkspaceEntries() { return V03_CANONICAL_USER_WORKSPACE_BOOTSTRAP.map((entry) => ({ ...entry })); }

function canonicalWorkspaceTemplateFiles() {
  return V03_CANONICAL_USER_WORKSPACE_BOOTSTRAP.map((entry) => entry.relativePath.slice("workspace/user-workspace/".length));
}

function escapeRegExp(value) { return String(value).replace(/[.*+?^${}()|[\]\\]/g, "\\$&"); }

function hash(value) { return `sha256:${createHash("sha256").update(value).digest("hex")}`; }
