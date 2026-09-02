param(
  [string]$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..\..")).Path,
  [string]$SourceExtensionsRoot = "",
  [string]$TargetExtensionsRoot = "/home/agent-runtime/openclaw-enterprise-runtime/dist/extensions",
  [string]$ReleaseRevision = "local",
  [switch]$DryRun
)

$ErrorActionPreference = "Stop"

function Resolve-SourceExtensionsRoot {
  param([string]$Root, [string]$Override)
  if ($Override -and (Test-Path -LiteralPath $Override)) {
    return (Resolve-Path -LiteralPath $Override).Path
  }
  $candidate = Join-Path $Root "ops/source/openclaw-extensions"
  if (Test-Path -LiteralPath $candidate) {
    return (Resolve-Path -LiteralPath $candidate).Path
  }
  throw "openclaw_extension_source_missing"
}

function Read-PluginManifest {
  param([string]$PluginRoot)
  $manifestPath = Join-Path $PluginRoot "openclaw.plugin.json"
  if (-not (Test-Path -LiteralPath $manifestPath)) { throw "openclaw_extension_manifest_invalid" }
  try {
    return Get-Content -Raw -Encoding UTF8 -LiteralPath $manifestPath | ConvertFrom-Json
  } catch {
    throw "openclaw_extension_manifest_invalid"
  }
}

function Test-HuahuoContextToolsLayout {
  param([string]$PluginRoot)
  $required = @(
    "openclaw.plugin.json",
    "package.json",
    "index.js",
    "src/registered-tool-probe.js",
    "src/workspace-search.js",
    "tests/workspace-search.test.mjs"
  )
  $missing = @($required | Where-Object { -not (Test-Path -LiteralPath (Join-Path $PluginRoot $_)) })
  if ($missing.Count -gt 0) { throw "openclaw_extension_layout_invalid" }
  $manifest = Read-PluginManifest -PluginRoot $PluginRoot
  $tools = @($manifest.contracts.tools | ForEach-Object { [string]$_ })
  if ($tools.Count -ne 1 -or $tools[0] -ne "workspace_search") { throw "openclaw_extension_manifest_invalid" }
  if ([string]$manifest.id -ne "huahuo-context-tools") { throw "openclaw_extension_manifest_invalid" }
  if ($manifest.enabledByDefault -ne $true) { throw "openclaw_extension_manifest_invalid" }
  if ($manifest.activation.onStartup -ne $true) { throw "openclaw_extension_manifest_invalid" }
  return [ordered]@{ ok = $true; id = [string]$manifest.id; tools = $tools; missing = $missing }
}

function Copy-OwnedPlugin {
  param([string]$SourceRoot, [string]$TargetRoot, [string]$PluginId)
  $source = Join-Path $SourceRoot $PluginId
  if (-not (Test-Path -LiteralPath $source)) { throw "openclaw_extension_source_missing" }
  if ($PluginId -ne "huahuo-context-tools") { throw "openclaw_extension_manifest_invalid" }
  $layout = Test-HuahuoContextToolsLayout -PluginRoot $source
  if ($DryRun) { return [ordered]@{ pluginId = $PluginId; copiedFiles = 0; layout = $layout } }
  if (-not (Test-Path -LiteralPath $TargetRoot)) { New-Item -ItemType Directory -Force -Path $TargetRoot | Out-Null }
  $target = Join-Path $TargetRoot $PluginId
  $tmpTarget = Join-Path $TargetRoot (".$PluginId.$ReleaseRevision.next")
  try {
    if (Test-Path -LiteralPath $tmpTarget) { Remove-Item -LiteralPath $tmpTarget -Recurse -Force }
    New-Item -ItemType Directory -Force -Path $tmpTarget | Out-Null
    foreach ($item in @(Get-ChildItem -LiteralPath $source -Force -ErrorAction Stop)) {
      Copy-Item -LiteralPath $item.FullName -Destination $tmpTarget -Recurse -Force
    }
    Test-HuahuoContextToolsLayout -PluginRoot $tmpTarget | Out-Null
    if (Test-Path -LiteralPath $target) { Remove-Item -LiteralPath $target -Recurse -Force }
    Move-Item -LiteralPath $tmpTarget -Destination $target -Force
    $copied = @(Get-ChildItem -LiteralPath $target -Recurse -File -ErrorAction SilentlyContinue).Count
    return [ordered]@{ pluginId = $PluginId; copiedFiles = $copied; layout = $layout; target = $target }
  } catch {
    if (Test-Path -LiteralPath $tmpTarget) { Remove-Item -LiteralPath $tmpTarget -Recurse -Force }
    throw "openclaw_extension_publish_failed"
  }
}

$sourceRoot = Resolve-SourceExtensionsRoot -Root $RepoRoot -Override $SourceExtensionsRoot
$published = @(
  Copy-OwnedPlugin -SourceRoot $sourceRoot -TargetRoot $TargetExtensionsRoot -PluginId "huahuo-context-tools"
)

[ordered]@{
  ok = $true
  dryRun = [bool]$DryRun
  sourceExtensionsRoot = $sourceRoot
  targetExtensionsRoot = $TargetExtensionsRoot
  releaseRevision = $ReleaseRevision
  published = $published
  generatedAt = (Get-Date).ToUniversalTime().ToString("o")
} | ConvertTo-Json -Depth 10
