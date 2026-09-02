param(
  [string]$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..\..")).Path,
  [string]$SourceMetaRoot = "",
  [string]$TargetConfigRoot = "/home/huahuo-runtime/config",
  [string]$ReleaseRevision = "local",
  [string]$OpenClawSourceRoot = "",
  [switch]$DryRun
)

$ErrorActionPreference = "Stop"

function Resolve-SourceMetaRoot {
  param([string]$Root, [string]$Override)
  if ($Override -and (Test-Path -LiteralPath $Override)) { return Normalize-SourceMetaRoot -Path $Override }
  $candidates = @(
    (Join-Path $Root "agent/server-runtime-meta/templates"),
    (Join-Path $Root "docs/products/huahuo-ai/04-system-facing-design/templates"),
    (Join-Path $Root "docs/products/huahuo-ai/04-system-facing-design"),
    (Join-Path $Root "docs/products/huahuo-ai")
  )
  foreach ($candidate in $candidates) {
    if (Test-Path -LiteralPath $candidate) {
      $normalized = Normalize-SourceMetaRoot -Path $candidate
      if (Test-Path -LiteralPath (Join-Path $normalized "meta-manifest.json")) { return $normalized }
    }
  }
  throw "runtime_meta_missing"
}

function Normalize-SourceMetaRoot {
  param([string]$Path)
  $resolved = (Resolve-Path -LiteralPath $Path).Path
  if (Test-Path -LiteralPath (Join-Path $resolved "meta-manifest.json")) { return $resolved }
  $templates = Join-Path $resolved "templates"
  if (Test-Path -LiteralPath (Join-Path $templates "meta-manifest.json")) {
    return (Resolve-Path -LiteralPath $templates).Path
  }
  return $resolved
}

function Validate-MetaManifest {
  param([string]$Root)
  $manifest = Join-Path $Root "meta-manifest.json"
  if (Test-Path -LiteralPath $manifest) {
    try {
      $json = Get-Content -Raw -Encoding UTF8 -LiteralPath $manifest | ConvertFrom-Json
      $activation = Test-SkillActivationStatus -Root $Root -Manifest $json
      if (-not $activation.ok) { throw "runtime_skill_unavailable" }
      return [ordered]@{ ok = $true; path = $manifest; source = "manifest"; itemCount = @($json.PSObject.Properties).Count; activation = $activation }
    } catch {
      if ($_.Exception.Message -eq "runtime_skill_unavailable") { throw "runtime_skill_unavailable" }
      throw "runtime_meta_manifest_invalid"
    }
  }
  throw "runtime_meta_manifest_invalid"
}

function Test-SkillActivationStatus {
  param([string]$Root, [object]$Manifest)
  $invalid = @()
  if ($Manifest -and $Manifest.runtimeSkills) {
    foreach ($skill in @($Manifest.runtimeSkills)) {
      $status = [string]$skill.status
      $referenced = [bool]$skill.routeReferenced
      if ($status -notin @("active", "disabled") -and ($referenced -or $status -ne "planned")) {
        $invalid += [ordered]@{ skill = $skill.name; status = $status; reason = "executable skill must be active or disabled" }
      }
    }
  }
  $skillFiles = @(Get-ChildItem -LiteralPath $Root -Recurse -File -Filter "SKILL.md" -ErrorAction SilentlyContinue)
  foreach ($file in $skillFiles) {
    $text = Get-Content -Raw -Encoding UTF8 -LiteralPath $file.FullName
    $headerText = (($text -split "`r?`n" | Select-Object -First 40) -join "`n")
    if ($headerText -match '(?m)^status:\s*(\S+)') {
      $status = $Matches[1]
      if ($status -notin @("active", "disabled")) {
        $invalid += [ordered]@{ skill = $file.FullName; status = $status; reason = "runtime skill file must be active or disabled" }
      }
    }
  }
  return [ordered]@{ ok = ($invalid.Count -eq 0); invalid = $invalid }
}

function Copy-Tree {
  param([string]$Source, [string]$Target)
  if ($DryRun) { return 0 }
  if (-not (Test-Path -LiteralPath $Target)) { New-Item -ItemType Directory -Force -Path $Target | Out-Null }
  foreach ($item in @(Get-ChildItem -LiteralPath $Source -Force -ErrorAction Stop)) {
    Copy-Item -LiteralPath $item.FullName -Destination $Target -Recurse -Force
  }
  return @(Get-ChildItem -LiteralPath $Target -Recurse -File -ErrorAction SilentlyContinue).Count
}

function Copy-RequiredTree {
  param([string]$Source, [string]$Target, [string]$Code)
  if (-not (Test-Path -LiteralPath $Source)) { throw $Code }
  Copy-Tree -Source $Source -Target $Target
}

function Get-ActiveRuntimeAgentProfiles {
  param([string]$RoutingManifestPath)
  if (-not (Test-Path -LiteralPath $RoutingManifestPath)) { throw "runtime_agent_manifest_invalid" }
  try {
    $manifest = Get-Content -Raw -Encoding UTF8 -LiteralPath $RoutingManifestPath | ConvertFrom-Json
  } catch {
    throw "runtime_agent_manifest_invalid"
  }
  if ($null -eq $manifest -or [string]$manifest.version -ne "l1-agent-manifest.v2" -or $null -eq $manifest.agents) {
    throw "runtime_agent_manifest_invalid"
  }

  $profiles = @()
  $seen = @{}
  foreach ($agent in @($manifest.agents)) {
    if ([string]$agent.status -ne "active") { continue }
    $profile = [string]$agent.agentProfile
    if ($profile -notmatch '^[a-z][a-z0-9_]*$' -or $seen.ContainsKey($profile)) {
      throw "runtime_agent_manifest_invalid"
    }
    if ([string]$agent.relativeRoot -ne ("agents/" + $profile)) {
      throw "runtime_agent_manifest_invalid"
    }
    $seen[$profile] = $true
    $profiles += $profile
  }
  if ($profiles.Count -eq 0) { throw "runtime_agent_manifest_invalid" }
  return @($profiles | Sort-Object)
}

function Copy-MetaMirror {
  param([string]$Source, [string]$Target)
  if ($DryRun) { return 0 }
  $targetTemplates = Join-Path $Target "templates"
  $targetSkills = Join-Path $Target "runtime-skills"
  $targetAgents = Join-Path $Target "runtime-agents"
  New-Item -ItemType Directory -Force -Path $targetTemplates, $targetSkills, $targetAgents | Out-Null

  Copy-Item -LiteralPath (Join-Path $Source "meta-manifest.json") -Destination (Join-Path $targetTemplates "meta-manifest.json") -Force
  $copied = 1
  $copied += Copy-RequiredTree -Source (Join-Path $Source "workspace") -Target (Join-Path $targetTemplates "workspace") -Code "runtime_meta_missing"
  $knowledgeSource = Join-Path $Source "knowledge"
  if (Test-Path -LiteralPath $knowledgeSource) {
    $copied += Copy-Tree -Source $knowledgeSource -Target (Join-Path $targetTemplates "knowledge")
  }
  $copied += Copy-RequiredTree -Source (Join-Path $Source "runtime-skills") -Target $targetSkills -Code "runtime_skill_unavailable"
  $routingManifest = Join-Path $Source "agent-routing-manifest.json"
  $activeProfiles = Get-ActiveRuntimeAgentProfiles -RoutingManifestPath $routingManifest
  foreach ($profile in $activeProfiles) {
    $copied += Copy-RequiredTree -Source (Join-Path (Join-Path $Source "agents") $profile) -Target (Join-Path $targetAgents $profile) -Code "runtime_agent_unavailable"
  }
  if (-not $DryRun) { Copy-Item -LiteralPath $routingManifest -Destination (Join-Path $Target "agent-routing-manifest.json") -Force }
  $copied += 1
  return $copied
}

function Test-RuntimeAgentPackageClosure {
  param([string]$Target)
  $errors = @()
  $catalogPath = Join-Path $Target "agent-planning-catalog.json"
  try {
    $catalog = Get-Content -Raw -Encoding UTF8 -LiteralPath $catalogPath | ConvertFrom-Json
  } catch {
    return [ordered]@{ ok = $false; errors = @("catalog_invalid") }
  }

  $skillsByProfile = @{}
  foreach ($skill in @($catalog.skills | Where-Object { [string]$_.status -eq "active" })) {
    $profile = [string]$skill.skillProfile
    if ([string]::IsNullOrWhiteSpace($profile) -or $skillsByProfile.ContainsKey($profile)) {
      $errors += "skill_invalid"
      continue
    }
    $skillsByProfile[$profile] = $skill
  }

  $activeAgents = @($catalog.manifest.agents | Where-Object { [string]$_.status -eq "active" })
  $expectedProfiles = @{}
  foreach ($agent in $activeAgents) {
    $profile = [string]$agent.agentProfile
    $relativeRoot = [string]$agent.relativeRoot
    if ($profile -notmatch '^[a-z][a-z0-9_]*$' -or $expectedProfiles.ContainsKey($profile) -or $relativeRoot -ne ("agents/" + $profile)) {
      $errors += "agent_invalid:${profile}"
      continue
    }
    $expectedProfiles[$profile] = $true
    $agentRoot = Join-Path (Join-Path $Target "runtime-agents") $profile
    foreach ($relative in @("AGENTS.md", "SOUL.md", "TOOLS.md", "capability-catalog.json")) {
      if (-not (Test-Path -LiteralPath (Join-Path $agentRoot $relative) -PathType Leaf)) {
        $errors += "agent_core_missing:${profile}:${relative}"
      }
    }
    $agentsPath = Join-Path $agentRoot "AGENTS.md"
    if (-not (Test-Path -LiteralPath $agentsPath -PathType Leaf)) { continue }
    $actualHash = "sha256:" + (Get-FileHash -LiteralPath $agentsPath -Algorithm SHA256).Hash.ToLowerInvariant()
    if ($actualHash -ne [string]$agent.hash) {
      $errors += "agent_hash_mismatch:${profile}"
    }

    $capabilityPath = Join-Path $agentRoot "capability-catalog.json"
    try {
      $capabilityCatalog = Get-Content -Raw -Encoding UTF8 -LiteralPath $capabilityPath | ConvertFrom-Json
    } catch {
      $errors += "agent_capability_catalog_invalid:${profile}"
      continue
    }
    if ([string]$capabilityCatalog.schemaVersion -ne "huahuo.agent_capability_catalog.v1" -or [string]$capabilityCatalog.agentProfile -ne $profile -or @($capabilityCatalog.capabilities).Count -eq 0) {
      $errors += "agent_capability_catalog_invalid:${profile}"
      continue
    }

    $coveredTasks = @{}
    $seenBindings = @{}
    foreach ($binding in @($capabilityCatalog.capabilities)) {
      $taskType = [string]$binding.taskType
      $skillProfile = [string]$binding.skillProfile
      $bindingKey = $taskType + [char]0 + $skillProfile
      $candidateSkillProfiles = @($agent.candidateSkillProfiles | ForEach-Object { [string]$_ })
      $agentTaskTypes = @($agent.taskTypes | ForEach-Object { [string]$_ })
      if ([string]::IsNullOrWhiteSpace([string]$binding.scene) -or [string]::IsNullOrWhiteSpace($taskType) -or [string]::IsNullOrWhiteSpace($skillProfile) -or [string]::IsNullOrWhiteSpace([string]$binding.outputSchemaVersion) -or $binding.visibleOwner -ne $true -or [string]$binding.status -ne "active" -or $seenBindings.ContainsKey($bindingKey) -or $agentTaskTypes -notcontains $taskType -or $candidateSkillProfiles -notcontains $skillProfile) {
        $errors += "agent_capability_binding_invalid:${profile}:${taskType}:${skillProfile}"
        continue
      }
      if (-not $skillsByProfile.ContainsKey($skillProfile)) {
        $errors += "agent_capability_skill_missing:${profile}:${skillProfile}"
        continue
      }
      $skill = $skillsByProfile[$skillProfile]
      $skillTaskTypes = @($skill.taskTypes | ForEach-Object { [string]$_ })
      $allowedAgentProfiles = @($skill.allowedAgentProfiles | ForEach-Object { [string]$_ })
      if ($skillTaskTypes -notcontains $taskType -or $allowedAgentProfiles -notcontains $profile) {
        $errors += "agent_capability_skill_mismatch:${profile}:${taskType}:${skillProfile}"
        continue
      }
      $seenBindings[$bindingKey] = $true
      $coveredTasks[$taskType] = $true
    }
    foreach ($taskType in @($agent.taskTypes | ForEach-Object { [string]$_ })) {
      if (-not $coveredTasks.ContainsKey($taskType)) {
        $errors += "agent_task_uncovered:${profile}:${taskType}"
      }
    }
  }

  $runtimeAgentsRoot = Join-Path $Target "runtime-agents"
  if (-not (Test-Path -LiteralPath $runtimeAgentsRoot -PathType Container)) {
    $errors += "runtime_agents_missing"
  } else {
    foreach ($entry in @(Get-ChildItem -LiteralPath $runtimeAgentsRoot -Force)) {
      if (-not $entry.PSIsContainer -or -not $expectedProfiles.ContainsKey($entry.Name)) {
        $errors += "runtime_agent_unexpected:$($entry.Name)"
      }
    }
  }
  return [ordered]@{ ok = ($errors.Count -eq 0); errors = $errors; activeAgentProfiles = @($expectedProfiles.Keys | Sort-Object) }
}

function Test-MirrorLayout {
  param([string]$Target)
  $required = @(
    "templates/meta-manifest.json",
    "templates/workspace/user-workspace/AGENTS.md",
    "templates/workspace/detached-runtime-workspace/AGENTS.md",
    "runtime-skills/topic_generation/SKILL.md",
    "runtime-skills/general_chat/SKILL.md",
    "runtime-skills/profile_maintenance/SKILL.md",
    "runtime-skills/meeting_minutes/SKILL.md",
    "runtime-skills/asset_summary/SKILL.md",
    "runtime-skills/hotspot_suggestion/SKILL.md"
    "runtime-skills/viewpoint_germination/SKILL.md"
    "agent-routing-manifest.json"
    "runtime-capabilities.env"
    "agent-planning-catalog.json"
  )
  $activeProfiles = Get-ActiveRuntimeAgentProfiles -RoutingManifestPath (Join-Path $Target "agent-routing-manifest.json")
  foreach ($profile in $activeProfiles) {
    $required += "runtime-agents/$profile/AGENTS.md"
  }
  $missing = @($required | Where-Object { -not (Test-Path -LiteralPath (Join-Path $Target $_)) })
  $unexpected = @()
  foreach ($path in @("agents", "runtime-agents/feed_ai_agent")) {
    if (Test-Path -LiteralPath (Join-Path $Target $path)) { $unexpected += $path }
  }
  $closure = Test-RuntimeAgentPackageClosure -Target $Target
  return [ordered]@{ ok = ($missing.Count -eq 0 -and $unexpected.Count -eq 0 -and $closure.ok); missing = $missing; unexpected = $unexpected; required = $required; activeAgentProfiles = $activeProfiles; closure = $closure }
}

function Publish-MirrorSubtrees {
  param([string]$CandidateRoot, [string]$TargetRoot)
  if ($DryRun) { return }
  $subtrees = @("templates", "runtime-skills", "runtime-agents", "agent-routing-manifest.json", "runtime-capabilities.env", "agent-planning-catalog.json")
  if (-not (Test-Path -LiteralPath $TargetRoot)) { New-Item -ItemType Directory -Force -Path $TargetRoot | Out-Null }
  $rollback = @()
  try {
    foreach ($subtree in $subtrees) {
      $candidate = Join-Path $CandidateRoot $subtree
      $active = Join-Path $TargetRoot $subtree
      $previous = Join-Path $TargetRoot (".$subtree.previous")
      if (-not (Test-Path -LiteralPath $candidate)) { throw "runtime_meta_layout_invalid" }
      if (Test-Path -LiteralPath $previous) { Remove-Item -LiteralPath $previous -Recurse -Force }
      $hadActive = Test-Path -LiteralPath $active
      if ($hadActive) { Move-Item -LiteralPath $active -Destination $previous -Force }
      $rollback += [ordered]@{ active = $active; previous = $previous; hadActive = $hadActive }
      Move-Item -LiteralPath $candidate -Destination $active -Force
    }
  } catch {
    for ($i = $rollback.Count - 1; $i -ge 0; $i--) {
      $entry = $rollback[$i]
      if (Test-Path -LiteralPath $entry.active) { Remove-Item -LiteralPath $entry.active -Recurse -Force }
      if ($entry.hadActive -and (Test-Path -LiteralPath $entry.previous)) {
        Move-Item -LiteralPath $entry.previous -Destination $entry.active -Force
      }
    }
    throw "runtime_config_write_failed"
  }
}

function Write-MirrorRevision {
  param([string]$Target, [string]$Revision, [object]$ManifestSummary, [int]$CopiedFiles)
  $summary = [ordered]@{
    releaseRevision = $Revision
    manifest = $ManifestSummary
    copiedFiles = $CopiedFiles
    generatedAt = (Get-Date).ToUniversalTime().ToString("o")
  }
  if (-not $DryRun) {
    $summary | ConvertTo-Json -Depth 10 | Set-Content -Encoding UTF8 -LiteralPath (Join-Path $Target "mirror-revision.json")
  }
  return $summary
}

function Ensure-RuntimeContractEnvironment {
  param([string]$Target)
  $envPath = Join-Path $Target "huahuo-runtime.env"
  $catalogPath = Join-Path $Target "agent-planning-catalog.json"
  $entry = "HUAHUO_AGENT_PLANNING_CATALOG_PATH=$catalogPath"
  if ($DryRun) {
    return [ordered]@{ ok = $true; dryRun = $true; envPath = $envPath; catalogPath = $catalogPath }
  }
  $lines = @()
  if (Test-Path -LiteralPath $envPath) {
    $lines = @(Get-Content -Encoding UTF8 -LiteralPath $envPath)
  }
  $updated = @()
  $replaced = $false
  foreach ($line in $lines) {
    if ($line -match '^\s*HUAHUO_AGENT_PLANNING_CATALOG_PATH=') {
      if (-not $replaced) { $updated += $entry }
      $replaced = $true
      continue
    }
    $updated += $line
  }
  if (-not $replaced) {
    if ($updated.Count -gt 0 -and -not [string]::IsNullOrWhiteSpace([string]$updated[-1])) { $updated += "" }
    $updated += $entry
  }
  $tmpPath = "$envPath.next"
  [System.IO.File]::WriteAllLines($tmpPath, [string[]]$updated, (New-Object System.Text.UTF8Encoding($false)))
  Move-Item -LiteralPath $tmpPath -Destination $envPath -Force
  return [ordered]@{ ok = $true; dryRun = $false; envPath = $envPath; catalogPath = $catalogPath }
}

function Generate-RuntimeContracts {
  param([string]$Root, [string]$Source, [string]$Target, [string]$OpenClawRoot)
  $generator = Join-Path $Root "ops/source/runtime/generate_runtime_contracts.mjs"
  if (-not (Test-Path -LiteralPath $generator)) { throw "runtime_contract_generator_missing" }
  $runtimeConfigRoot = $Target
  $runtimeSkillMirrorRoot = Join-Path $Target "runtime-skills"
  $validationRoot = ""
  if ($DryRun) {
    $validationRoot = Join-Path ([System.IO.Path]::GetTempPath()) ("runtime-meta-contract-validation-" + [guid]::NewGuid().ToString("N"))
    [void](New-Item -ItemType Directory -Path $validationRoot -Force)
    $runtimeConfigRoot = $validationRoot
    $runtimeSkillMirrorRoot = Join-Path $Source "runtime-skills"
  }
  $args = @(
    $generator,
    "--agent-source-root", $Source,
    "--runtime-config-root", $runtimeConfigRoot,
    "--runtime-skill-mirror-root", $runtimeSkillMirrorRoot
  )
  if (-not [string]::IsNullOrWhiteSpace($OpenClawRoot)) {
    if (-not (Test-Path -LiteralPath $OpenClawRoot)) { throw "openclaw_source_missing" }
    $args += @("--openclaw-source-root", $OpenClawRoot)
  }
  try {
    $output = & node @args | Out-String
    if ($LASTEXITCODE -ne 0) { throw "runtime_contract_generation_failed" }
    try { return $output | ConvertFrom-Json } catch { throw "runtime_contract_generation_failed" }
  } finally {
    if ($validationRoot -and (Test-Path -LiteralPath $validationRoot)) {
      Remove-Item -LiteralPath $validationRoot -Recurse -Force
    }
  }
}

function ResolveSourceMetaRoot {
  param([string]$Root, [string]$Override)
  Resolve-SourceMetaRoot -Root $Root -Override $Override
}

function ValidateMetaManifest {
  param([string]$Root)
  Validate-MetaManifest -Root $Root
}

function CopyWorkspaceTemplates {
  param([string]$Source, [string]$Target)
  Copy-MetaMirror -Source $Source -Target $Target
}

function CopyRuntimeSkillPackages {
  param([string]$Source, [string]$Target)
  Copy-MetaMirror -Source $Source -Target $Target
}

function WriteMirrorRevision {
  param([string]$Target, [string]$Revision, [object]$ManifestSummary, [int]$CopiedFiles)
  Write-MirrorRevision -Target $Target -Revision $Revision -ManifestSummary $ManifestSummary -CopiedFiles $CopiedFiles
}

$sourceRoot = Resolve-SourceMetaRoot -Root $RepoRoot -Override $SourceMetaRoot
$manifestSummary = Validate-MetaManifest -Root $sourceRoot
$targetParent = Split-Path -Parent $TargetConfigRoot
$targetLeaf = Split-Path -Leaf $TargetConfigRoot
if ([string]::IsNullOrWhiteSpace($targetParent)) { $targetParent = "." }
$tmpTarget = Join-Path $targetParent (".$targetLeaf.$ReleaseRevision.next")
$activeTarget = $TargetConfigRoot

if (-not $DryRun) {
  if (-not (Test-Path -LiteralPath $targetParent)) { New-Item -ItemType Directory -Force -Path $targetParent | Out-Null }
  if (Test-Path -LiteralPath $tmpTarget) { Remove-Item -LiteralPath $tmpTarget -Recurse -Force }
  New-Item -ItemType Directory -Force -Path $tmpTarget | Out-Null
}

$copied = Copy-MetaMirror -Source $sourceRoot -Target $tmpTarget
$runtimeContracts = Generate-RuntimeContracts -Root $RepoRoot -Source $sourceRoot -Target $tmpTarget -OpenClawRoot $OpenClawSourceRoot
$layout = if ($DryRun) { [ordered]@{ ok = $true; missing = @(); required = @() } } else { Test-MirrorLayout -Target $tmpTarget }
if (-not $layout.ok) { throw "runtime_meta_layout_invalid" }
if (-not $DryRun) {
  Publish-MirrorSubtrees -CandidateRoot $tmpTarget -TargetRoot $activeTarget
  Remove-Item -LiteralPath $tmpTarget -Recurse -Force
}
$runtimeEnvironment = Ensure-RuntimeContractEnvironment -Target $activeTarget
$summary = Write-MirrorRevision -Target $activeTarget -Revision $ReleaseRevision -ManifestSummary $manifestSummary -CopiedFiles $copied

[ordered]@{
  ok = $true
  sourceRoot = $sourceRoot
  targetConfigRoot = $TargetConfigRoot
  dryRun = [bool]$DryRun
  layout = $layout
  runtimeContracts = $runtimeContracts
  runtimeEnvironment = $runtimeEnvironment
  summary = $summary
} | ConvertTo-Json -Depth 10
