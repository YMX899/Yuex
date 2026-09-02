param(
  [string]$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..\..")).Path,
  [string]$OpenClawSourceRoot = "",
  [string]$AdapterBinaryPath = "",
  [string]$AdapterServiceUnitPath = "",
  [string]$RuntimeConfigRoot = "",
  [string]$AgentSourceRoot = "",
  [string]$RuntimeSkillMirrorRoot = "",
  [string]$OverlayRoot = "",
  [string]$ReceiptPath = "",
  [string]$ReplacementAdapterPath = "",
  [switch]$BuildAdapter
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ExpectedOpenClawVersion = "2026.6.2"
$ExpectedAdapterSha256 = "7c740bb83072b35d73c8bdb7fc70d58bc3010fd0d7f8b1db64eb89cd0ce68d97"

if ([string]::IsNullOrWhiteSpace($ReceiptPath)) {
  $ReceiptPath = Join-Path ([System.IO.Path]::GetTempPath()) ("huahuo-pinned-openclaw-gate-" + $PID + ".json")
}
if ([string]::IsNullOrWhiteSpace($OverlayRoot)) {
  $OverlayRoot = Join-Path $RepoRoot "ops/source/openclaw-enterprise-runtime-overlay"
}
if ([string]::IsNullOrWhiteSpace($AgentSourceRoot)) {
  $AgentSourceRoot = Join-Path $RepoRoot "agent/server-runtime-meta/templates"
}

$checks = [System.Collections.Generic.List[object]]::new()
function Add-GateCheck {
  param([string]$Name, [bool]$Ok, [string]$Code, [string]$Detail = "")
  $checks.Add([ordered]@{ name = $Name; ok = $Ok; code = $Code; detail = $Detail })
}

function Test-DirectoryInput {
  param([string]$Name, [string]$Path, [string]$MissingCode)
  $ok = -not [string]::IsNullOrWhiteSpace($Path) -and (Test-Path -LiteralPath $Path -PathType Container)
  Add-GateCheck -Name $Name -Ok $ok -Code $(if ($ok) { "OK" } else { $MissingCode })
  return $ok
}

function Test-FileInput {
  param([string]$Name, [string]$Path, [string]$MissingCode)
  $ok = -not [string]::IsNullOrWhiteSpace($Path) -and (Test-Path -LiteralPath $Path -PathType Leaf)
  Add-GateCheck -Name $Name -Ok $ok -Code $(if ($ok) { "OK" } else { $MissingCode })
  return $ok
}

function Test-RegularFile {
  param([string]$Path)
  try {
    $item = Get-Item -LiteralPath $Path -Force -ErrorAction Stop
    return -not [bool]$item.PSIsContainer -and [string]::IsNullOrEmpty([string]$item.LinkType)
  } catch {
    return $false
  }
}

function Invoke-OverlayDryRun {
  param([string]$Installer, [string]$SourceRoot, [string]$ResolvedOverlayRoot)
  try {
    & $Installer -OpenClawSourceRoot $SourceRoot -OverlayRoot $ResolvedOverlayRoot -ExpectedVersion $ExpectedOpenClawVersion -DryRun | Out-Null
    Add-GateCheck -Name "overlay_anchor_dry_run" -Ok $true -Code "OK"
  } catch {
    # Do not copy source paths or upstream error text into the durable receipt.
    Add-GateCheck -Name "overlay_anchor_dry_run" -Ok $false -Code "PINNED_OPENCLAW_OVERLAY_DRYRUN_FAILED"
  }
}

function Invoke-ContractVerifyOnly {
  param([string]$Generator, [string]$SourceRoot, [string]$ConfigRoot, [string]$AgentRoot, [string]$SkillRoot)
  $node = Get-Command node -ErrorAction SilentlyContinue
  if ($null -eq $node) {
    Add-GateCheck -Name "runtime_contract_verify_only" -Ok $false -Code "PINNED_OPENCLAW_NODE_MISSING"
    return
  }
  try {
    $args = @(
      $Generator,
      "--agent-source-root", $AgentRoot,
      "--runtime-config-root", $ConfigRoot,
      "--openclaw-source-root", $SourceRoot,
      "--verify-only"
    )
    if (-not [string]::IsNullOrWhiteSpace($SkillRoot)) {
      $args += @("--runtime-skill-mirror-root", $SkillRoot)
    }
    & $node.Source @args 2>$null | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "contract_verify_only_failed" }
    Add-GateCheck -Name "runtime_contract_verify_only" -Ok $true -Code "OK"
  } catch {
    Add-GateCheck -Name "runtime_contract_verify_only" -Ok $false -Code "PINNED_OPENCLAW_CONTRACT_VERIFY_FAILED"
  }
}

$repoOk = Test-DirectoryInput -Name "repo_root" -Path $RepoRoot -MissingCode "PINNED_OPENCLAW_REPOSITORY_MISSING"
$installer = Join-Path $RepoRoot "ops/source/runtime/apply_openclaw_enterprise_overlay.ps1"
$generator = Join-Path $RepoRoot "ops/source/runtime/generate_runtime_contracts.mjs"
$snapshot = Join-Path $RepoRoot "ops/source/config/openclaw-core-tool-schemas-2026.6.2.json"
$overlayFiles = @(
  "src/enterprise-run-store.ts",
  "src/enterprise-run-registry.ts",
  "src/runtime-policy.ts",
  "src/private-run-context.ts",
  "src/enterprise-runtime-methods.ts"
)

$installerOk = Test-FileInput -Name "overlay_installer" -Path $installer -MissingCode "PINNED_OPENCLAW_OVERLAY_INSTALLER_MISSING"
$generatorOk = Test-FileInput -Name "runtime_contract_generator" -Path $generator -MissingCode "PINNED_OPENCLAW_CONTRACT_GENERATOR_MISSING"
$snapshotOk = Test-FileInput -Name "core_tool_snapshot" -Path $snapshot -MissingCode "PINNED_OPENCLAW_TOOL_SNAPSHOT_MISSING"
$overlayOk = Test-DirectoryInput -Name "overlay_source_root" -Path $OverlayRoot -MissingCode "PINNED_OPENCLAW_OVERLAY_SOURCE_MISSING"
if ($overlayOk) {
  $missingOverlay = @($overlayFiles | Where-Object { -not (Test-Path -LiteralPath (Join-Path $OverlayRoot $_) -PathType Leaf) })
  Add-GateCheck -Name "overlay_source_closure" -Ok ($missingOverlay.Count -eq 0) -Code $(if ($missingOverlay.Count -eq 0) { "OK" } else { "PINNED_OPENCLAW_OVERLAY_SOURCE_INCOMPLETE" })
}

if ($snapshotOk) {
  try {
    $coreSnapshot = Get-Content -LiteralPath $snapshot -Raw -Encoding UTF8 | ConvertFrom-Json
    $snapshotPinned = [string]$coreSnapshot.openclawVersion -eq $ExpectedOpenClawVersion
    $sourceHashes = @($coreSnapshot.sourceFiles.PSObject.Properties | ForEach-Object { [string]$_.Value.sha256 })
    $hashesPinned = $sourceHashes.Count -eq 2 -and @($sourceHashes | Where-Object { $_ -notmatch '^[a-f0-9]{64}$' }).Count -eq 0
    Add-GateCheck -Name "core_tool_snapshot_version" -Ok $snapshotPinned -Code $(if ($snapshotPinned) { "OK" } else { "PINNED_OPENCLAW_SNAPSHOT_VERSION_MISMATCH" })
    Add-GateCheck -Name "core_tool_snapshot_hashes" -Ok $hashesPinned -Code $(if ($hashesPinned) { "OK" } else { "PINNED_OPENCLAW_SNAPSHOT_HASH_INVALID" })
  } catch {
    Add-GateCheck -Name "core_tool_snapshot_parse" -Ok $false -Code "PINNED_OPENCLAW_TOOL_SNAPSHOT_INVALID"
  }
}

$sourceOk = Test-DirectoryInput -Name "openclaw_source_root" -Path $OpenClawSourceRoot -MissingCode "PINNED_OPENCLAW_SOURCE_MISSING"
$packageOk = $false
if ($sourceOk) {
  $packagePath = Join-Path $OpenClawSourceRoot "package.json"
  if (Test-Path -LiteralPath $packagePath -PathType Leaf) {
    try {
      $package = Get-Content -LiteralPath $packagePath -Raw -Encoding UTF8 | ConvertFrom-Json
      $packageOk = [string]$package.version -eq $ExpectedOpenClawVersion
      Add-GateCheck -Name "openclaw_package_version" -Ok $packageOk -Code $(if ($packageOk) { "OK" } else { "PINNED_OPENCLAW_VERSION_MISMATCH" })
    } catch {
      Add-GateCheck -Name "openclaw_package_version" -Ok $false -Code "PINNED_OPENCLAW_PACKAGE_INVALID"
    }
  } else {
    Add-GateCheck -Name "openclaw_package_version" -Ok $false -Code "PINNED_OPENCLAW_PACKAGE_MISSING"
  }
}

if ($BuildAdapter) {
  Add-GateCheck -Name "adapter_rebuild" -Ok $false -Code "PINNED_ADAPTER_REBUILD_FORBIDDEN"
} else {
  Add-GateCheck -Name "adapter_rebuild" -Ok $true -Code "OK"
}
if (-not [string]::IsNullOrWhiteSpace($ReplacementAdapterPath)) {
  Add-GateCheck -Name "adapter_replacement" -Ok $false -Code "PINNED_ADAPTER_REPLACEMENT_FORBIDDEN"
} else {
  Add-GateCheck -Name "adapter_replacement" -Ok $true -Code "OK"
}

$adapterOk = Test-FileInput -Name "adapter_binary" -Path $AdapterBinaryPath -MissingCode "PINNED_ADAPTER_BINARY_MISSING"
$adapterRegular = $false
if ($adapterOk) {
  $adapterRegular = Test-RegularFile -Path $AdapterBinaryPath
  Add-GateCheck -Name "adapter_binary_regular_file" -Ok $adapterRegular -Code $(if ($adapterRegular) { "OK" } else { "PINNED_ADAPTER_BINARY_NOT_REGULAR" })
  if ($adapterRegular) {
    try {
      $actualAdapterHash = (Get-FileHash -LiteralPath $AdapterBinaryPath -Algorithm SHA256).Hash.ToLowerInvariant()
      Add-GateCheck -Name "adapter_binary_sha256" -Ok ($actualAdapterHash -eq $ExpectedAdapterSha256) -Code $(if ($actualAdapterHash -eq $ExpectedAdapterSha256) { "OK" } else { "PINNED_ADAPTER_SHA256_MISMATCH" })
    } catch {
      Add-GateCheck -Name "adapter_binary_sha256" -Ok $false -Code "PINNED_ADAPTER_HASH_UNREADABLE"
    }
  }
}

$unitOk = Test-FileInput -Name "adapter_service_unit" -Path $AdapterServiceUnitPath -MissingCode "PINNED_ADAPTER_SERVICE_UNIT_MISSING"
if ($unitOk -and $adapterOk) {
  try {
    $unitText = Get-Content -LiteralPath $AdapterServiceUnitPath -Raw -Encoding UTF8
    $binaryFullPath = (Resolve-Path -LiteralPath $AdapterBinaryPath).Path
    $execStartPattern = "(?m)^ExecStart=" + [regex]::Escape($binaryFullPath) + "(?:\s|$)"
    Add-GateCheck -Name "adapter_service_unit_binding" -Ok ($unitText -match $execStartPattern) -Code $(if ($unitText -match $execStartPattern) { "OK" } else { "PINNED_ADAPTER_SERVICE_BINDING_MISMATCH" })
  } catch {
    Add-GateCheck -Name "adapter_service_unit_binding" -Ok $false -Code "PINNED_ADAPTER_SERVICE_UNIT_INVALID"
  }
}

$configOk = Test-DirectoryInput -Name "runtime_config_root" -Path $RuntimeConfigRoot -MissingCode "PINNED_OPENCLAW_RUNTIME_CONFIG_MISSING"
$agentOk = Test-DirectoryInput -Name "agent_source_root" -Path $AgentSourceRoot -MissingCode "PINNED_OPENCLAW_AGENT_SOURCE_MISSING"
$skillOk = Test-DirectoryInput -Name "runtime_skill_mirror_root" -Path $RuntimeSkillMirrorRoot -MissingCode "PINNED_OPENCLAW_RUNTIME_SKILL_MIRROR_MISSING"

if ($sourceOk -and $packageOk -and $installerOk -and $overlayOk) {
  Invoke-OverlayDryRun -Installer $installer -SourceRoot $OpenClawSourceRoot -ResolvedOverlayRoot $OverlayRoot
}
if ($sourceOk -and $packageOk -and $generatorOk -and $configOk -and $agentOk -and $skillOk) {
  Invoke-ContractVerifyOnly -Generator $generator -SourceRoot $OpenClawSourceRoot -ConfigRoot $RuntimeConfigRoot -AgentRoot $AgentSourceRoot -SkillRoot $RuntimeSkillMirrorRoot
}

$blocking = @($checks | Where-Object { -not $_.ok })
$receipt = [ordered]@{
  schema = "huahuo.pinned-openclaw-gate-receipt.v1"
  ok = ($blocking.Count -eq 0)
  expectedOpenClawVersion = $ExpectedOpenClawVersion
  expectedAdapterSha256 = $ExpectedAdapterSha256
  checks = @($checks)
  blockers = @($blocking | ForEach-Object { $_.code } | Sort-Object -Unique)
  generatedAt = (Get-Date).ToUniversalTime().ToString("o")
}

try {
  $receiptDirectory = Split-Path -Parent $ReceiptPath
  if (-not [string]::IsNullOrWhiteSpace($receiptDirectory) -and -not (Test-Path -LiteralPath $receiptDirectory)) {
    New-Item -ItemType Directory -Force -Path $receiptDirectory | Out-Null
  }
  [System.IO.File]::WriteAllText($ReceiptPath, ($receipt | ConvertTo-Json -Depth 8), [System.Text.UTF8Encoding]::new($false))
} catch {
  $receipt.ok = $false
  $receipt.blockers = @($receipt.blockers + "PINNED_OPENCLAW_RECEIPT_WRITE_FAILED" | Sort-Object -Unique)
}

$receipt | ConvertTo-Json -Depth 8
if (-not $receipt.ok) { exit 1 }
