param(
  [string]$DataRoot = "/home/data/huahuo",
  [string]$RuntimeRoot = "/home/huahuo-runtime",
  [switch]$CreateMissing,
  [string]$ReportPath = ""
)

$ErrorActionPreference = "Stop"

function New-Check {
  param([string]$Name, [bool]$Ok, [string]$Code, [string]$Message)
  [ordered]@{ name = $Name; ok = $Ok; code = $Code; message = $Message }
}

function Ensure-Dir {
  param([string]$Path)
  if ((Test-Path -LiteralPath $Path) -and (Get-Item -LiteralPath $Path).PSIsContainer) { return $true }
  if ($CreateMissing) {
    New-Item -ItemType Directory -Force -Path $Path | Out-Null
    return $true
  }
  return $false
}

function Normalize-PathText {
  param([string]$Path)
  try {
    return [System.IO.Path]::GetFullPath($Path)
  } catch {
    return $Path
  }
}

$checks = @()
$dataSubdirs = @("workspaces", "media", "media/transient", "exports", "backups", "log-archive")
$runtimeSubdirs = @("state", "sessions", "logs", "tmp", "locks", "run-artifacts", "node-compile-cache", "config", "tmp/runtime-workspaces")

$checks += New-Check "data_root" (Ensure-Dir $DataRoot) "data_root_missing" "Data root must exist."
$checks += New-Check "runtime_root" (Ensure-Dir $RuntimeRoot) "runtime_root_missing" "Runtime root must exist."

foreach ($subdir in $dataSubdirs) {
  $path = Join-Path $DataRoot $subdir
  $checks += New-Check "data_subdir_$subdir" (Ensure-Dir $path) "data_root_missing" "Required data subdir: $subdir"
}
foreach ($subdir in $runtimeSubdirs) {
  $path = Join-Path $RuntimeRoot $subdir
  $checks += New-Check "runtime_subdir_$subdir" (Ensure-Dir $path) "runtime_root_missing" "Required runtime subdir: $subdir"
}

$dataFull = Normalize-PathText $DataRoot
$runtimeFull = Normalize-PathText $RuntimeRoot
$checks += New-Check "root_separation" ($dataFull -ne $runtimeFull -and -not $runtimeFull.StartsWith($dataFull.TrimEnd("\","/"))) "root_escape_detected" "Runtime root must not live under data root."

$forbiddenNames = @("postgres", "postgresql", "redis", "tair", "locks-active")
foreach ($name in $forbiddenNames) {
  $path = Join-Path $DataRoot $name
  $checks += New-Check "no_active_state_under_data_$name" (-not (Test-Path -LiteralPath $path)) "active_state_under_data_root" "DB/Redis/active lock directory must not be under data root."
}

$runtimeTmp = Join-Path $RuntimeRoot "tmp/runtime-workspaces"
$checks += New-Check "runtime_tmp_cleanup_managed" (Test-Path -LiteralPath $runtimeTmp) "runtime_tmp_not_cleanup_managed" "Detached runtime workspace tmp root must exist for retention cleanup."

$blocking = @($checks | Where-Object { -not $_.ok })
$report = [ordered]@{
  ok = ($blocking.Count -eq 0)
  dataRoot = $DataRoot
  runtimeRoot = $RuntimeRoot
  blocking = $blocking
  checks = $checks
  generatedAt = (Get-Date).ToUniversalTime().ToString("o")
}

if ($ReportPath) {
  $dir = Split-Path -Parent $ReportPath
  if ($dir -and -not (Test-Path -LiteralPath $dir)) { New-Item -ItemType Directory -Force -Path $dir | Out-Null }
  $report | ConvertTo-Json -Depth 10 | Set-Content -Encoding UTF8 -LiteralPath $ReportPath
}

$report | ConvertTo-Json -Depth 10
if (-not $report.ok) { exit 1 }
