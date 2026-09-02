param(
  [Parameter(Mandatory = $true)][string]$OpenClawSourceRoot,
  [string]$OverlayRoot = "",
  [string]$ExpectedVersion = "2026.6.2",
  [switch]$AllowReviewedBaselineCopyReplacement,
  [switch]$DryRun
)

$ErrorActionPreference = "Stop"
$PinnedOpenClawVersion = "2026.6.2"
if ($ExpectedVersion -ne $PinnedOpenClawVersion) {
  throw "openclaw_overlay_expected_version_invalid:$ExpectedVersion"
}

if ([string]::IsNullOrWhiteSpace($OverlayRoot)) {
  $OverlayRoot = Join-Path $PSScriptRoot "..\openclaw-enterprise-runtime-overlay"
}

function Read-Utf8File([string]$Path) {
  if (-not (Test-RegularFilePath -Path $Path)) { throw "openclaw_overlay_target_not_regular:$Path" }
  return Get-Content -Raw -Encoding UTF8 -LiteralPath $Path
}

function Set-Utf8File([string]$Path, [string]$Content) {
  [System.IO.File]::WriteAllText($Path, $Content, [System.Text.UTF8Encoding]::new($false))
}

function Test-RegularDirectoryChain([string]$Path) {
  $item = Get-Item -LiteralPath $Path -Force -ErrorAction SilentlyContinue
  if ($null -eq $item) { return $false }
  while ($null -ne $item) {
    if (-not $item.PSIsContainer -or (($item.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0)) {
      return $false
    }
    $parent = $item.Parent
    if ($null -eq $parent) { break }
    $item = Get-Item -LiteralPath $parent.FullName -Force -ErrorAction SilentlyContinue
  }
  return $true
}

function Test-RegularFilePath([string]$Path) {
  $parent = Split-Path -Parent $Path
  if (-not (Test-RegularDirectoryChain -Path $parent)) { return $false }
  $item = Get-Item -LiteralPath $Path -Force -ErrorAction SilentlyContinue
  return $null -ne $item -and -not $item.PSIsContainer -and (($item.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -eq 0)
}

function Test-RegularOverlayFile([string]$Path) {
  return Test-RegularFilePath -Path $Path
}

function Resolve-RegularDirectory([string]$Path, [string]$Code) {
  if (-not (Test-RegularDirectoryChain -Path $Path)) { throw "${Code}:$Path" }
  return (Get-Item -LiteralPath $Path -Force).FullName
}

function Test-ByteArrayEqual {
  param([AllowNull()][byte[]]$Left, [AllowNull()][byte[]]$Right)
  if ($null -eq $Left -or $null -eq $Right) { return $null -eq $Left -and $null -eq $Right }
  if ($Left.Length -ne $Right.Length) { return $false }
  for ($index = 0; $index -lt $Left.Length; $index += 1) {
    if ($Left[$index] -ne $Right[$index]) { return $false }
  }
  return $true
}

function Get-ByteSha256([byte[]]$Bytes) {
  $hasher = [System.Security.Cryptography.SHA256]::Create()
  try {
    return ([System.BitConverter]::ToString($hasher.ComputeHash($Bytes))).Replace("-", "").ToLowerInvariant()
  } finally {
    $hasher.Dispose()
  }
}

function Read-RegularFileBytesOrMissing([string]$Path) {
  $item = Get-Item -LiteralPath $Path -Force -ErrorAction SilentlyContinue
  if ($null -eq $item) { return $null }
  if (-not (Test-RegularFilePath -Path $Path)) {
    throw "openclaw_overlay_target_not_regular:$Path"
  }
  return [System.IO.File]::ReadAllBytes($Path)
}

function Write-NewFileBytes([string]$Path, [byte[]]$Bytes) {
  $stream = [System.IO.File]::Open($Path, [System.IO.FileMode]::CreateNew, [System.IO.FileAccess]::Write, [System.IO.FileShare]::None)
  try {
    $stream.Write($Bytes, 0, $Bytes.Length)
  } finally {
    $stream.Dispose()
  }
}

function Move-OverlayFile([string]$Source, [string]$Destination) {
  [System.IO.File]::Move($Source, $Destination)
}

function Invoke-OverlayWriteTransaction {
  param([object[]]$Writes, [switch]$DryRun)

  $changed = [System.Collections.Generic.List[object]]::new()
  foreach ($write in $Writes) {
    if ($null -eq $write -or [string]::IsNullOrWhiteSpace([string]$write.TargetPath) -or $null -eq $write.AfterBytes) {
      throw "openclaw_overlay_transaction_input_invalid"
    }
    $parent = Split-Path -Parent ([string]$write.TargetPath)
    if (-not (Test-RegularDirectoryChain -Path $parent)) {
      throw "openclaw_overlay_target_parent_invalid:$parent"
    }
    [byte[]]$current = Read-RegularFileBytesOrMissing -Path ([string]$write.TargetPath)
    $matchesBefore = Test-ByteArrayEqual -Left $current -Right $write.BeforeBytes
    $matchesAfter = $null -ne $current -and (Test-ByteArrayEqual -Left $current -Right $write.AfterBytes)
    if (-not $matchesBefore -and -not $matchesAfter) {
      throw "openclaw_overlay_source_drift:$($write.Label)"
    }
    if (-not $matchesAfter) {
      $changed.Add([pscustomobject]@{
        Label = [string]$write.Label
        TargetPath = [string]$write.TargetPath
        BeforeBytes = $write.BeforeBytes
        AfterBytes = $write.AfterBytes
        AfterSha256 = Get-ByteSha256 -Bytes $write.AfterBytes
        Exists = ($null -ne $current)
      })
    }
  }

  if ($DryRun) {
    return [ordered]@{ changedTargetCount = $changed.Count; applied = $false; dryRun = $true }
  }
  if ($changed.Count -eq 0) {
    return [ordered]@{ changedTargetCount = 0; applied = $true; dryRun = $false }
  }

  $transactionId = [guid]::NewGuid().ToString("N")
  $staged = [System.Collections.Generic.List[object]]::new()
  $swapped = [System.Collections.Generic.List[object]]::new()
  try {
    foreach ($entry in $changed) {
      $stagePath = "$($entry.TargetPath).huahuo-overlay-$transactionId.next"
      # Register before the write so a partial staging failure is removed by
      # finally without ever touching a target.
      $stage = [pscustomobject]@{ Entry = $entry; StagePath = $stagePath }
      $staged.Add($stage)
      Write-NewFileBytes -Path $stagePath -Bytes $entry.AfterBytes
    }

    foreach ($stage in $staged) {
      $entry = $stage.Entry
      $backupPath = if ($entry.Exists) { "$($entry.TargetPath).huahuo-overlay-$transactionId.previous" } else { $null }
      # Keep a rollback record before moving the original. A failure between
      # the two moves must restore the original rather than leave its target
      # path absent.
      $swap = [pscustomobject]@{
        TargetPath = $entry.TargetPath
        StagePath = $stage.StagePath
        BackupPath = $backupPath
        OriginalMoved = $false
        ReplacementMoved = $false
      }
      $swapped.Add($swap)
      if ($backupPath) {
        Move-OverlayFile -Source $entry.TargetPath -Destination $backupPath
        $swap.OriginalMoved = $true
      }
      Move-OverlayFile -Source $stage.StagePath -Destination $entry.TargetPath
      $swap.ReplacementMoved = $true
    }

    foreach ($entry in $changed) {
      [byte[]]$actual = Read-RegularFileBytesOrMissing -Path $entry.TargetPath
      if (-not (Test-ByteArrayEqual -Left $actual -Right $entry.AfterBytes) -or (Get-ByteSha256 -Bytes $actual) -ne $entry.AfterSha256) {
        throw "openclaw_overlay_apply_verify_failed:$($entry.Label)"
      }
    }
    foreach ($swap in $swapped) {
      if ($swap.BackupPath) {
        try { [System.IO.File]::Delete($swap.BackupPath) } catch { }
      }
    }
    return [ordered]@{ changedTargetCount = $changed.Count; verifiedTargetCount = $changed.Count; applied = $true; dryRun = $false }
  } catch {
    for ($swapIndex = $swapped.Count - 1; $swapIndex -ge 0; $swapIndex -= 1) {
      $swap = $swapped[$swapIndex]
      try {
        if ($swap.ReplacementMoved -and (Test-Path -LiteralPath $swap.TargetPath)) {
          [System.IO.File]::Delete($swap.TargetPath)
        }
        if ($swap.OriginalMoved -and $swap.BackupPath -and (Test-Path -LiteralPath $swap.BackupPath)) {
          Move-OverlayFile -Source $swap.BackupPath -Destination $swap.TargetPath
        }
      } catch {
        # Preserve the original transaction failure; no success receipt follows.
      }
    }
    throw
  } finally {
    foreach ($stage in $staged) {
      try {
        if (Test-Path -LiteralPath $stage.StagePath) { [System.IO.File]::Delete($stage.StagePath) }
      } catch { }
    }
  }
}

function Replace-ExactOnce([string]$Content, [string]$Needle, [string]$Replacement, [string]$Label, [string[]]$AcceptedPriorStates = @()) {
  # The pinned Runtime tree is built on Linux and stores LF text. Normalize
  # the installer literals before exact matching so a Windows maintainer does
  # not turn a reviewed source state into an artificial CRLF drift failure.
  $Content = $Content.Replace("`r`n", "`n").Replace("`r", "`n")
  $Needle = $Needle.Replace("`r`n", "`n").Replace("`r", "`n")
  $Replacement = $Replacement.Replace("`r`n", "`n").Replace("`r", "`n")
  $AcceptedPriorStates = @($AcceptedPriorStates | ForEach-Object { ([string]$_).Replace("`r`n", "`n").Replace("`r", "`n") })
  $originalCount = ([regex]::Matches($Content, [regex]::Escape($Needle))).Count
  $replacementCount = ([regex]::Matches($Content, [regex]::Escape($Replacement))).Count

  # A target can contain its original anchor as a prefix. Remove the complete
  # desired replacement before checking for residual old anchors, so a partly
  # installed Overlay cannot be applied twice and a mixed state never passes.
  if ($replacementCount -eq 1) {
    $residual = $Content.Replace($Replacement, "")
    $residualOriginalCount = ([regex]::Matches($residual, [regex]::Escape($Needle))).Count
    if ($residualOriginalCount -eq 0) { return $Content }
  }
  if ($replacementCount -eq 0) {
    foreach ($priorState in $AcceptedPriorStates) {
      $priorCount = ([regex]::Matches($Content, [regex]::Escape($priorState))).Count
      $priorOriginalCount = ([regex]::Matches($priorState, [regex]::Escape($Needle))).Count
      # A reviewed prior Overlay may intentionally contain the original anchor
      # as its prefix. Accept it only when that is the sole original occurrence
      # in the target; otherwise a mixed old/new source remains fail-closed.
      if ($priorCount -eq 1 -and (($priorOriginalCount -eq 0 -and $originalCount -eq 0) -or ($priorOriginalCount -eq 1 -and $originalCount -eq 1))) {
        return $Content.Replace($priorState, $Replacement)
      }
    }
    if ($originalCount -eq 1) {
      return $Content.Replace($Needle, $Replacement)
    }
  }
  throw "openclaw_overlay_anchor_mismatch:${Label}:original=${originalCount}:replacement=${replacementCount}"
}

$overlay = Resolve-RegularDirectory -Path $OverlayRoot -Code "openclaw_overlay_root_invalid"
$overlayFiles = @(
  "src/enterprise-run-store.ts",
  "src/enterprise-run-registry.ts",
  "src/runtime-policy.ts",
  "src/runtime-host-recovery.ts",
  "src/private-run-context.ts",
  "src/enterprise-runtime-methods.ts",
  "src/faya-status-result-compat.ts",
  "src/thoughts-response-status-compat.ts",
  "src/runtime-capability-handshake.ts",
  "tests/enterprise-runtime-async.test.ts",
  "tests/thoughts-response-status-compat.test.ts",
  "tests/runtime-capability-handshake.test.ts"
)
$missingOverlay = @($overlayFiles | Where-Object { -not (Test-RegularOverlayFile (Join-Path $overlay $_)) })
if ($missingOverlay.Count -gt 0) { throw "openclaw_overlay_source_missing:$($missingOverlay -join ',')" }

# Validate the complete reviewed Overlay before resolving or reading any
# candidate source. A partial Overlay must not be able to probe a pinned tree.
$sourceRoot = Resolve-RegularDirectory -Path $OpenClawSourceRoot -Code "openclaw_overlay_source_root_invalid"
$package = Read-Utf8File (Join-Path $sourceRoot "package.json") | ConvertFrom-Json
if ([string]$package.version -ne $PinnedOpenClawVersion) {
  throw "openclaw_overlay_version_mismatch:$($package.version):$PinnedOpenClawVersion"
}

$targets = [ordered]@{
  protocolSchema = Join-Path $sourceRoot "packages/gateway-protocol/src/schema/enterprise-runtime.ts"
  gatewayTypes = Join-Path $sourceRoot "src/config/types.gateway.ts"
  gatewaySchema = Join-Path $sourceRoot "src/config/zod-schema.ts"
  gatewayServer = Join-Path $sourceRoot "src/gateway/server.impl.ts"
  serverMethods = Join-Path $sourceRoot "src/gateway/server-methods.ts"
  descriptors = Join-Path $sourceRoot "src/gateway/methods/core-descriptors.ts"
  enterpriseHandler = Join-Path $sourceRoot "src/gateway/server-methods/enterprise-runtime.ts"
  eventLogger = Join-Path $sourceRoot "src/enterprise-runtime/event-logger.ts"
  agentRunner = Join-Path $sourceRoot "src/enterprise-runtime/agent-runner.ts"
  commandTypes = Join-Path $sourceRoot "src/agents/command/types.ts"
  agentCommand = Join-Path $sourceRoot "src/agents/agent-command.ts"
  attemptExecution = Join-Path $sourceRoot "src/agents/command/attempt-execution.ts"
  embeddedRunParams = Join-Path $sourceRoot "src/agents/embedded-agent-runner/run/params.ts"
  embeddedRun = Join-Path $sourceRoot "src/agents/embedded-agent-runner/run.ts"
  embeddedRunTypes = Join-Path $sourceRoot "src/agents/embedded-agent-runner/run/types.ts"
  embeddedRunAttempt = Join-Path $sourceRoot "src/agents/embedded-agent-runner/run/attempt.ts"
  agentTools = Join-Path $sourceRoot "src/agents/agent-tools.ts"
  beforeToolCall = Join-Path $sourceRoot "src/agents/agent-tools.before-tool-call.ts"
  runConfig = Join-Path $sourceRoot "src/enterprise-runtime/config/run-config.ts"
}

$content = [ordered]@{}
$beforeBytes = [ordered]@{}
foreach ($entry in $targets.GetEnumerator()) {
  $content[$entry.Key] = Read-Utf8File $entry.Value
  $beforeBytes[$entry.Key] = [System.IO.File]::ReadAllBytes($entry.Value)
}

# The recovery listener is deliberately independent of the ordinary
# Gateway WS/Token port. It requires a peer certificate issued by this
# dedicated trust root and derives every Host principal from URI-SAN.
$gatewayRecoveryTypeAnchor = @'
export type GatewayConfig = {
'@
$gatewayRecoveryTypeReplacement = @'
export type GatewayEnterpriseRuntimeRecoveryConfig = {
  /** Enables the dedicated mTLS-only Runtime Host recovery WSS listener. */
  enabled?: boolean;
  /** Bind address. Defaults to loopback when omitted. */
  host?: string;
  /** Dedicated listener port; it may not reuse the regular Gateway port. */
  port?: number;
  /** PEM server certificate for the dedicated recovery listener. */
  certPath?: string;
  /** PEM server private key for the dedicated recovery listener. */
  keyPath?: string;
  /** PEM trust root used to verify Runtime Host client certificates. */
  caPath?: string;
  /** Exact environment segment required in Host SPIFFE URI-SANs. */
  environment?: string;
};

export type GatewayConfig = {
'@
$content.gatewayTypes = Replace-ExactOnce $content.gatewayTypes $gatewayRecoveryTypeAnchor $gatewayRecoveryTypeReplacement "gateway_recovery_listener_type"

$gatewayRecoveryPropertyAnchor = '  tls?: GatewayTlsConfig;'
$gatewayRecoveryPropertyReplacement = @'
  tls?: GatewayTlsConfig;
  /**
   * Dedicated mTLS recovery listener for Runtime Hosts. This is never a
   * replacement for normal Gateway WebSocket/Token authentication.
   */
  enterpriseRuntimeRecovery?: GatewayEnterpriseRuntimeRecoveryConfig;
'@.TrimEnd("`r", "`n")
$content.gatewayTypes = Replace-ExactOnce $content.gatewayTypes $gatewayRecoveryPropertyAnchor $gatewayRecoveryPropertyReplacement "gateway_recovery_listener_property"

$gatewayRecoverySchemaAnchor = @'
        http: z
'@
$gatewayRecoverySchemaReplacement = @'
        enterpriseRuntimeRecovery: z
          .object({
            enabled: z.boolean().optional(),
            host: z.string().min(1).optional(),
            port: z.number().int().min(1).max(65_535).optional(),
            certPath: z.string().min(1).optional(),
            keyPath: z.string().min(1).optional(),
            caPath: z.string().min(1).optional(),
            environment: z.string().regex(/^[A-Za-z0-9][A-Za-z0-9._:-]{0,1023}$/).optional(),
          })
          .strict()
          .optional(),
        http: z
'@
$content.gatewaySchema = Replace-ExactOnce $content.gatewaySchema $gatewayRecoverySchemaAnchor $gatewayRecoverySchemaReplacement "gateway_recovery_listener_schema"

$gatewayRecoveryImportAnchor = 'import { createDefaultDeps } from "../cli/deps.js";'
$gatewayRecoveryImportReplacement = $gatewayRecoveryImportAnchor + "`n" + 'import { startEnterpriseRuntimeRecoveryWss, startGatewayWithEnterpriseRuntimeRecovery } from "../enterprise-runtime/runtime-host-recovery.js";'
$gatewayRecoveryImportPriorOverlayState = $gatewayRecoveryImportAnchor + "`n" + 'import { startEnterpriseRuntimeRecoveryWss } from "../enterprise-runtime/runtime-host-recovery.js";'
$content.gatewayServer = Replace-ExactOnce $content.gatewayServer $gatewayRecoveryImportAnchor $gatewayRecoveryImportReplacement "gateway_recovery_listener_import" @($gatewayRecoveryImportPriorOverlayState)

$gatewayRecoveryVariableAnchor = '  let clearFallbackGatewayContextForServer = () => {};'
$gatewayRecoveryVariableReplacement = '  let enterpriseRuntimeRecoveryListener: Awaited<ReturnType<typeof startEnterpriseRuntimeRecoveryWss>>;`n'.Replace('`n', "`n") + $gatewayRecoveryVariableAnchor
$content.gatewayServer = Replace-ExactOnce $content.gatewayServer $gatewayRecoveryVariableAnchor $gatewayRecoveryVariableReplacement "gateway_recovery_listener_variable"

$gatewayRecoveryStartAnchor = '    await startupTrace.measure("http.listen", () => startListening());'
$gatewayRecoveryStartReplacement = @'
    enterpriseRuntimeRecoveryListener = await startGatewayWithEnterpriseRuntimeRecovery({
      startRecoveryListener: () => startupTrace.measure("enterprise.recovery-mtls.listen", () =>
        startEnterpriseRuntimeRecoveryWss({
          config: cfgAtStart.gateway?.enterpriseRuntimeRecovery,
          gatewayConfigPath: process.env.OPENCLAW_CONFIG_PATH,
          log: log.child("enterprise-recovery-mtls"),
        }),
      ),
      startPrimaryGateway: () => startupTrace.measure("http.listen", () => startListening()),
    });
'@.TrimEnd("`r", "`n")
$gatewayRecoveryStartPriorOverlayState = @'
    await startupTrace.measure("http.listen", () => startListening());
    enterpriseRuntimeRecoveryListener = await startupTrace.measure("enterprise.recovery-mtls.listen", () =>
      startEnterpriseRuntimeRecoveryWss({
        config: cfgAtStart.gateway?.enterpriseRuntimeRecovery,
        log: log.child("enterprise-recovery-mtls"),
      }),
    );
'@.TrimEnd("`r", "`n")
$content.gatewayServer = Replace-ExactOnce $content.gatewayServer $gatewayRecoveryStartAnchor $gatewayRecoveryStartReplacement "gateway_recovery_listener_start" @($gatewayRecoveryStartPriorOverlayState)

$gatewayRecoveryCloseAnchor = '  const createCloseHandler = () => async (optsValue?: GatewayCloseOptions) => {`n    const channelIds = listLoadedChannelPlugins().map((plugin) => plugin.id as ChannelId);'.Replace('`n', "`n")
$gatewayRecoveryCloseReplacement = '  const createCloseHandler = () => async (optsValue?: GatewayCloseOptions) => {`n    if (enterpriseRuntimeRecoveryListener) {`n      await enterpriseRuntimeRecoveryListener.close();`n      enterpriseRuntimeRecoveryListener = undefined;`n    }`n    const channelIds = listLoadedChannelPlugins().map((plugin) => plugin.id as ChannelId);'.Replace('`n', "`n")
$content.gatewayServer = Replace-ExactOnce $content.gatewayServer $gatewayRecoveryCloseAnchor $gatewayRecoveryCloseReplacement "gateway_recovery_listener_close"

# `enterprise.runtime.submit` carries a signed per-Run policy. The async
# handler verifies the HMAC and bindings; this pinned Gateway schema rejects
# malformed or unknown envelope fields before that handler sees them.
$runtimePolicySchemaAnchor = @'
export const RuntimeRunSpecSchema = Type.Object(
'@
$runtimePolicySchemaReplacement = @'
export const RuntimeToolBudgetSchema = Type.Object(
  {
    maxToolCalls: Type.Integer({ minimum: 1, maximum: 400 }),
    softToolCallLimit: Type.Integer({ minimum: 1, maximum: 399 }),
    finalizationReserve: Type.Integer({ minimum: 1, maximum: 399 }),
    maxRepeatedCalls: Type.Integer({ minimum: 1, maximum: 32 }),
    maxNoProgressCalls: Type.Integer({ minimum: 1, maximum: 32 }),
    maxSearchCalls: Type.Integer({ minimum: 0, maximum: 400 }),
    maxWriteCalls: Type.Integer({ minimum: 0, maximum: 400 }),
    maxReadBytes: Type.Integer({ minimum: 1, maximum: 67108864 }),
    maxWallTimeSeconds: Type.Integer({ minimum: 1, maximum: 3600 }),
  },
  { additionalProperties: false },
);

export const RuntimeWorkspaceWriteLeaseSchema = Type.Object(
  {
    version: Type.Literal("huahuo.runtime-write-lease.v1"),
    runId: NonEmptyString,
    workspaceId: NonEmptyString,
    workspaceManifestHash: Type.String({ pattern: "^sha256:[a-f0-9]{64}$" }),
    allowedRoots: Type.Tuple([Type.Literal("output"), Type.Literal("staging")]),
    expiresAt: Type.Integer({ minimum: 0 }),
  },
  { additionalProperties: false },
);

export const RuntimePolicyEnvelopeSchema = Type.Object(
  {
    version: Type.Literal("huahuo.runtime-policy.v1"),
    algorithm: Type.Literal("HS256"),
    keyId: NonEmptyString,
    runId: NonEmptyString,
    idempotencyKey: NonEmptyString,
    workspaceManifestHash: Type.String({ pattern: "^sha256:[a-f0-9]{64}$" }),
    dispatchIdentity: Type.String({ pattern: "^sha256:[a-f0-9]{64}$" }),
    capabilityHash: NonEmptyString,
    planHash: Type.String({ pattern: "^sha256:[a-f0-9]{64}$" }),
    issuedAt: Type.Integer({ minimum: 0 }),
    expiresAt: Type.Integer({ minimum: 0 }),
    workspaceAccessMode: Type.Union([Type.Literal("read"), Type.Literal("write")]),
    writeLease: Type.Union([RuntimeWorkspaceWriteLeaseSchema, Type.Null()]),
    requiredTools: Type.Array(NonEmptyString),
    allowedTools: Type.Array(NonEmptyString),
    toolBudget: RuntimeToolBudgetSchema,
    signature: Type.String({ pattern: "^sha256:[a-f0-9]{64}$" }),
  },
  { additionalProperties: false },
);

export const RuntimeRunSpecSchema = Type.Object(
'@
$content.protocolSchema = Replace-ExactOnce $content.protocolSchema $runtimePolicySchemaAnchor $runtimePolicySchemaReplacement "runtime_policy_schema_definitions"

$runtimeWorkspaceSchemaAnchor = @'
    workspace: Type.Object(
      {
        realPath: NonEmptyString,
        accessMode: Type.Union([Type.Literal("read"), Type.Literal("write")]),
      },
      { additionalProperties: false },
    ),
'@
$runtimeWorkspaceSchemaReplacement = @'
    workspace: Type.Union([
      Type.Object(
        {
          realPath: NonEmptyString,
          accessMode: Type.Literal("read"),
        },
        { additionalProperties: false },
      ),
      Type.Object(
        {
          realPath: NonEmptyString,
          accessMode: Type.Literal("write"),
          writeLease: RuntimeWorkspaceWriteLeaseSchema,
        },
        { additionalProperties: false },
      ),
    ]),
'@
$content.protocolSchema = Replace-ExactOnce $content.protocolSchema $runtimeWorkspaceSchemaAnchor $runtimeWorkspaceSchemaReplacement "runtime_workspace_write_lease_schema"

$runtimePolicyFieldAnchor = @'
    input: Type.Object(
      {
        message: NonEmptyString,
        attachments: Type.Optional(Type.Array(RuntimeAttachmentSchema)),
      },
'@
$runtimePolicyFieldReplacement = @'
    runtimePolicy: Type.Optional(RuntimePolicyEnvelopeSchema),
    input: Type.Object(
      {
        message: NonEmptyString,
        attachments: Type.Optional(Type.Array(RuntimeAttachmentSchema)),
      },
'@
$content.protocolSchema = Replace-ExactOnce $content.protocolSchema $runtimePolicyFieldAnchor $runtimePolicyFieldReplacement "runtime_policy_schema_field"

$loaderAnchor = @'
const loadEnterpriseRuntimeHandlers = lazyHandlerModule(
  () => import("./server-methods/enterprise-runtime.js"),
  (module) => module.enterpriseRuntimeHandlers,
);
'@
$loaderReplacement = $loaderAnchor + @'
const loadEnterpriseRuntimeAsyncHandlers = lazyHandlerModule(
  () => import("./server-methods/enterprise-runtime-methods.js"),
  (module) => module.enterpriseRuntimeAsyncHandlers,
);
'@
$content.serverMethods = Replace-ExactOnce $content.serverMethods $loaderAnchor $loaderReplacement "server_methods_loader"

$handlerAnchor = @'
  ...createLazyCoreHandlers({
    methods: ["enterprise.runtime.run"],
    loadHandlers: loadEnterpriseRuntimeHandlers,
  }),
'@
$handlerReplacement = $handlerAnchor + @'
  ...createLazyCoreHandlers({
    methods: [
      "enterprise.runtime.submit",
      "enterprise.runtime.status",
      "enterprise.runtime.events",
      "enterprise.runtime.abort",
      "enterprise.runtime.capabilities",
    ],
    loadHandlers: loadEnterpriseRuntimeAsyncHandlers,
  }),
'@
$content.serverMethods = Replace-ExactOnce $content.serverMethods $handlerAnchor $handlerReplacement "server_methods_handlers"

$descriptorAnchor = '  { name: "enterprise.runtime.run", scope: "operator.write", startup: true },'
$descriptorReplacement = @'
  { name: "enterprise.runtime.run", scope: "operator.write", startup: true },
  { name: "enterprise.runtime.submit", scope: "operator.write", startup: true },
  { name: "enterprise.runtime.status", scope: "operator.read", startup: true },
  { name: "enterprise.runtime.events", scope: "operator.read", startup: true },
  { name: "enterprise.runtime.abort", scope: "operator.write", startup: true },
  { name: "enterprise.runtime.capabilities", scope: "operator.read", startup: true },
'@
$content.descriptors = Replace-ExactOnce $content.descriptors $descriptorAnchor $descriptorReplacement.TrimEnd("`r", "`n") "core_descriptors"

$handlerImportAnchor = 'import { RuntimeEventLogger } from "../../enterprise-runtime/event-logger.js";'
$handlerImportReplacement = $handlerImportAnchor + "`n" + 'import { enterpriseRunRegistry } from "../../enterprise-runtime/enterprise-run-registry.js";'
$content.enterpriseHandler = Replace-ExactOnce $content.enterpriseHandler $handlerImportAnchor $handlerImportReplacement "enterprise_handler_import"
$handlerPolicyDirectAnchor = '  [ENTERPRISE_RUNTIME_METHOD]: async ({ params, respond }) => {`n    if (!validateRuntimeRunSpec(params)) {'.Replace('`n', "`n")
$handlerPolicyDirectReplacement = '  [ENTERPRISE_RUNTIME_METHOD]: async ({ params, respond }) => {`n    if (Object.hasOwn(params, "runtimePolicy")) {`n      respond(false, undefined, errorShape(ErrorCodes.INVALID_REQUEST, "runtimePolicy is only valid for enterprise.runtime.submit"));`n      return;`n    }`n    if (!validateRuntimeRunSpec(params)) {'.Replace('`n', "`n")
$content.enterpriseHandler = Replace-ExactOnce $content.enterpriseHandler $handlerPolicyDirectAnchor $handlerPolicyDirectReplacement "enterprise_handler_rejects_direct_runtime_policy"
$content.enterpriseHandler = Replace-ExactOnce $content.enterpriseHandler '          ctx!.abortSignal = abort.signal;' '          ctx!.abortSignal = AbortSignal.any([abort.signal, enterpriseRunRegistry.getAbortSignal(ctx!.runId)]);' "enterprise_handler_abort"

$content.eventLogger = Replace-ExactOnce $content.eventLogger 'import fs from "node:fs/promises";' 'import { createHash } from "node:crypto";`nimport fs from "node:fs/promises";'.Replace('`n', "`n") "event_logger_crypto"
$eventImportAnchor = 'import type { RuntimeRunContext } from "./types.js";'
$content.eventLogger = Replace-ExactOnce $content.eventLogger $eventImportAnchor ($eventImportAnchor + "`n" + 'import { enterpriseRunRegistry } from "./enterprise-run-registry.js";') "event_logger_registry"
$content.eventLogger = Replace-ExactOnce $content.eventLogger '      openclawSessionKey: this.ctx.session.sessionKey,`n      workspaceDir: this.ctx.workspace.root,'.Replace('`n', "`n") '      sessionKeyHash: createHash("sha256").update(this.ctx.session.sessionKey).digest("hex"),`n      workspaceRef: `workspace:${this.ctx.workspaceId}`,'.Replace('`n', "`n") "event_logger_redaction"
$content.eventLogger = Replace-ExactOnce $content.eventLogger '  async event(event: RuntimeEvent): Promise<void> {`n    await fs.mkdir(this.ctx.dirs.runDir, { recursive: true });'.Replace('`n', "`n") '  async event(event: RuntimeEvent): Promise<void> {`n    enterpriseRunRegistry.appendEvent(this.ctx.runId, event.eventType, undefined, event);`n    await fs.mkdir(this.ctx.dirs.runDir, { recursive: true });'.Replace('`n', "`n") "event_logger_sequence"

$runnerImportAnchor = 'import type { RuntimeRunContext } from "./types.js";'
$content.agentRunner = Replace-ExactOnce $content.agentRunner $runnerImportAnchor ($runnerImportAnchor + "`n" + 'import { enterpriseRunRegistry } from "./enterprise-run-registry.js";') "agent_runner_registry"
$runnerContextAnchor = '      config: runConfig,`n    },'.Replace('`n', "`n")
$runnerContextReplacement = '      config: runConfig,`n      onBeforeToolCall: (call) => enterpriseRunRegistry.assertToolCallAllowed(ctx.runId, call),`n      onToolOutcome: (outcome) => enterpriseRunRegistry.recordToolOutcome(ctx.runId, outcome),`n    },'.Replace('`n', "`n")
# The prior reviewed Overlay revision installed durable tool outcomes but not
# the pre-execution guard. It is a single exact migration state, not a fuzzy
# fallback: any altered or duplicated form still fails closed.
$runnerContextPriorOverlayState = '      config: runConfig,`n      onToolOutcome: (outcome) => enterpriseRunRegistry.recordToolOutcome(ctx.runId, outcome),`n    },'.Replace('`n', "`n")
$content.agentRunner = Replace-ExactOnce $content.agentRunner $runnerContextAnchor $runnerContextReplacement "agent_runner_tool_outcome" @($runnerContextPriorOverlayState)

$typesAnchor = '    config?: OpenClawConfig;`n  };'.Replace('`n', "`n")
$typesReplacement = '    config?: OpenClawConfig;`n    onBeforeToolCall?: (call: { toolName: string; toolCallId: string; args: unknown }) => void;`n    onToolOutcome?: (outcome: { toolName: string; toolCallId: string; argsHash: string; resultHash?: string; progress?: boolean; resultBytes?: number }) => void;`n  };'.Replace('`n', "`n")
# The prior Overlay used this narrower outcome observation before the signed
# pre-execution callback and progress/call-id facts were introduced.
$typesPriorOverlayState = '    config?: OpenClawConfig;`n    onToolOutcome?: (outcome: { toolName: string; argsHash: string; resultHash?: string }) => void;`n  };'.Replace('`n', "`n")
$content.commandTypes = Replace-ExactOnce $content.commandTypes $typesAnchor $typesReplacement "command_types_tool_outcome" @($typesPriorOverlayState)

$commandAnchor = '              deferTerminalLifecycleEnd: true,`n            });'.Replace('`n', "`n")
$commandReplacement = '              deferTerminalLifecycleEnd: true,`n              onBeforeToolCall: opts.enterpriseRuntime?.onBeforeToolCall,`n              onToolOutcome: opts.enterpriseRuntime?.onToolOutcome,`n            });'.Replace('`n', "`n")
$commandPriorOverlayState = '              deferTerminalLifecycleEnd: true,`n              onToolOutcome: opts.enterpriseRuntime?.onToolOutcome,`n            });'.Replace('`n', "`n")
$content.agentCommand = Replace-ExactOnce $content.agentCommand $commandAnchor $commandReplacement "agent_command_tool_outcome" @($commandPriorOverlayState)

$attemptParamsAnchor = '  onUserMessagePersisted?: (message: Extract<AgentMessage, { role: "user" }>) => void;`n}) {'.Replace('`n', "`n")
$attemptParamsReplacement = '  onUserMessagePersisted?: (message: Extract<AgentMessage, { role: "user" }>) => void;`n  onBeforeToolCall?: (call: { toolName: string; toolCallId: string; args: unknown }) => void;`n  onToolOutcome?: (outcome: { toolName: string; toolCallId: string; argsHash: string; resultHash?: string; progress?: boolean; resultBytes?: number }) => void;`n}) {'.Replace('`n', "`n")
$content.attemptExecution = Replace-ExactOnce $content.attemptExecution $attemptParamsAnchor $attemptParamsReplacement "attempt_execution_callback_params"
$attemptRunAnchor = '    onAgentEvent: params.onAgentEvent,`n    deferTerminalLifecycleEnd: params.deferTerminalLifecycleEnd,'.Replace('`n', "`n")
$attemptRunReplacement = '    onAgentEvent: params.onAgentEvent,`n    onBeforeToolCall: params.onBeforeToolCall,`n    onToolOutcome: params.onToolOutcome,`n    deferTerminalLifecycleEnd: params.deferTerminalLifecycleEnd,'.Replace('`n', "`n")
$content.attemptExecution = Replace-ExactOnce $content.attemptExecution $attemptRunAnchor $attemptRunReplacement "attempt_execution_callback_forwarding"

$paramsImportAnchor = 'import type { EmbeddedAgentExecutionPhase } from "../execution-phase.js";'
$paramsImportReplacement = $paramsImportAnchor + "`n" + 'import type { ToolBeforeCallObserver, ToolOutcomeObserver } from "../../agent-tools.before-tool-call.js";'
$content.embeddedRunParams = Replace-ExactOnce $content.embeddedRunParams $paramsImportAnchor $paramsImportReplacement "embedded_run_params_callback_import"
$paramsCallbacksAnchor = '  runId: string;`n  abortSignal?: AbortSignal;'.Replace('`n', "`n")
$paramsCallbacksReplacement = '  runId: string;`n  abortSignal?: AbortSignal;`n  /** Enterprise callback invoked immediately before a real tool function executes. */`n  onBeforeToolCall?: ToolBeforeCallObserver;`n  /** Enterprise callback invoked after a real tool function completes. */`n  onToolOutcome?: ToolOutcomeObserver;'.Replace('`n', "`n")
$content.embeddedRunParams = Replace-ExactOnce $content.embeddedRunParams $paramsCallbacksAnchor $paramsCallbacksReplacement "embedded_run_params_callbacks"

$runTypesImportAnchor = 'import type { ToolOutcomeObserver } from "../../agent-tools.before-tool-call.js";'
$runTypesImportReplacement = 'import type { ToolBeforeCallObserver, ToolOutcomeObserver } from "../../agent-tools.before-tool-call.js";'
$content.embeddedRunTypes = Replace-ExactOnce $content.embeddedRunTypes $runTypesImportAnchor $runTypesImportReplacement "embedded_run_types_callback_import"
$runTypesCallbacksAnchor = '  /** Live observer called after wrapped tool outcomes are recorded. */`n  onToolOutcome?: ToolOutcomeObserver;'.Replace('`n', "`n")
$runTypesCallbacksReplacement = '  /** Live callback invoked immediately before a wrapped tool executes. */`n  onBeforeToolCall?: ToolBeforeCallObserver;`n  /** Live observer called after wrapped tool outcomes are recorded. */`n  onToolOutcome?: ToolOutcomeObserver;'.Replace('`n', "`n")
$content.embeddedRunTypes = Replace-ExactOnce $content.embeddedRunTypes $runTypesCallbacksAnchor $runTypesCallbacksReplacement "embedded_run_types_callbacks"

$runToolOutcomeImportAnchor = 'import { resolveProcessToolScopeKey } from "../agent-tools.js";'
$runToolOutcomeImportReplacement = $runToolOutcomeImportAnchor + "`n" + 'import type { ToolOutcomeObservation } from "../agent-tools.before-tool-call.js";'
$content.embeddedRun = Replace-ExactOnce $content.embeddedRun $runToolOutcomeImportAnchor $runToolOutcomeImportReplacement "embedded_run_outcome_import"
$runOutcomeAnchor = @'
      const observePostCompactionToolOutcome = (
        observation: PostCompactionGuardObservation,
      ): void => {
        const verdict = postCompactionGuard.observe(observation);
        if (verdict.shouldAbort) {
          postCompactionAbortError ??= PostCompactionLoopPersistedError.fromVerdict(verdict);
          postCompactionAbortController?.abort(postCompactionAbortError);
        }
      };
'@
$runOutcomeReplacement = $runOutcomeAnchor + @'
      const observeToolOutcome = (observation: ToolOutcomeObservation): void => {
        observePostCompactionToolOutcome(observation);
        params.onToolOutcome?.(observation);
      };
'@
$content.embeddedRun = Replace-ExactOnce $content.embeddedRun $runOutcomeAnchor.TrimEnd("`r", "`n") $runOutcomeReplacement.TrimEnd("`r", "`n") "embedded_run_callback_composition"
$runAttemptAnchor = '            onToolOutcome: observePostCompactionToolOutcome,`n            onRunProgress: notifyRunProgress,'.Replace('`n', "`n")
$runAttemptReplacement = '            onBeforeToolCall: params.onBeforeToolCall,`n            onToolOutcome: observeToolOutcome,`n            onRunProgress: notifyRunProgress,'.Replace('`n', "`n")
$content.embeddedRun = Replace-ExactOnce $content.embeddedRun $runAttemptAnchor $runAttemptReplacement "embedded_run_callback_forwarding"

$attemptToolAnchor = '            recordToolPrepStage: (name) => corePluginToolStages.mark(name),`n            onToolOutcome: params.onToolOutcome,`n            skillsSnapshot: skillsSnapshotForRun,'.Replace('`n', "`n")
$attemptToolReplacement = '            recordToolPrepStage: (name) => corePluginToolStages.mark(name),`n            onBeforeToolCall: params.onBeforeToolCall,`n            onToolOutcome: params.onToolOutcome,`n            skillsSnapshot: skillsSnapshotForRun,'.Replace('`n', "`n")
$content.embeddedRunAttempt = Replace-ExactOnce $content.embeddedRunAttempt $attemptToolAnchor $attemptToolReplacement "embedded_attempt_agent_tools_callback"
$attemptCatalogAnchor = '      }),`n      onToolOutcome: params.onToolOutcome,`n    };'.Replace('`n', "`n")
$attemptCatalogReplacement = '      }),`n      onBeforeToolCall: params.onBeforeToolCall,`n      onToolOutcome: params.onToolOutcome,`n    };'.Replace('`n', "`n")
$content.embeddedRunAttempt = Replace-ExactOnce $content.embeddedRunAttempt $attemptCatalogAnchor $attemptCatalogReplacement "embedded_attempt_catalog_callback"
$attemptClientAnchor = '              loopDetection: clientToolLoopDetection,`n              onToolOutcome: params.onToolOutcome,`n            },'.Replace('`n', "`n")
$attemptClientReplacement = '              loopDetection: clientToolLoopDetection,`n              onBeforeToolCall: params.onBeforeToolCall,`n              onToolOutcome: params.onToolOutcome,`n            },'.Replace('`n', "`n")
$content.embeddedRunAttempt = Replace-ExactOnce $content.embeddedRunAttempt $attemptClientAnchor $attemptClientReplacement "embedded_attempt_client_callback"

$agentToolsImportAnchor = '  type ToolOutcomeObserver,`n  wrapToolWithBeforeToolCallHook,'.Replace('`n', "`n")
$agentToolsImportReplacement = '  type ToolBeforeCallObserver,`n  type ToolOutcomeObserver,`n  wrapToolWithBeforeToolCallHook,'.Replace('`n', "`n")
$content.agentTools = Replace-ExactOnce $content.agentTools $agentToolsImportAnchor $agentToolsImportReplacement "agent_tools_callback_import"
$agentToolsOptionAnchor = '  /** Live observer called after wrapped tool outcomes are recorded. */`n  onToolOutcome?: ToolOutcomeObserver;'.Replace('`n', "`n")
$agentToolsOptionReplacement = '  /** Live callback invoked immediately before a wrapped tool executes. */`n  onBeforeToolCall?: ToolBeforeCallObserver;`n  /** Live observer called after wrapped tool outcomes are recorded. */`n  onToolOutcome?: ToolOutcomeObserver;'.Replace('`n', "`n")
$content.agentTools = Replace-ExactOnce $content.agentTools $agentToolsOptionAnchor $agentToolsOptionReplacement "agent_tools_callback_option"
$agentToolsContextAnchor = '        loopDetection: resolveToolLoopDetectionConfig({ cfg: options?.config, agentId }),`n        onToolOutcome: options?.onToolOutcome,'.Replace('`n', "`n")
$agentToolsContextReplacement = '        loopDetection: resolveToolLoopDetectionConfig({ cfg: options?.config, agentId }),`n        onBeforeToolCall: options?.onBeforeToolCall,`n        onToolOutcome: options?.onToolOutcome,'.Replace('`n', "`n")
$content.agentTools = Replace-ExactOnce $content.agentTools $agentToolsContextAnchor $agentToolsContextReplacement "agent_tools_callback_context"

$beforeToolTypesAnchor = @'
export type ToolOutcomeObservation = {
  toolName: string;
  argsHash: string;
  resultHash: string;
};

export type ToolOutcomeObserver = (observation: ToolOutcomeObservation) => void;
'@
$beforeToolTypesReplacement = @'
export type ToolBeforeCallObservation = {
  toolName: string;
  toolCallId: string;
  args: unknown;
};

export type ToolBeforeCallObserver = (observation: ToolBeforeCallObservation) => void;

export type ToolOutcomeObservation = {
  toolName: string;
  toolCallId: string;
  argsHash: string;
  resultHash: string;
  progress: boolean;
  resultBytes?: number;
};

export type ToolOutcomeObserver = (observation: ToolOutcomeObservation) => void;
'@
$content.beforeToolCall = Replace-ExactOnce $content.beforeToolCall $beforeToolTypesAnchor.TrimEnd("`r", "`n") $beforeToolTypesReplacement.TrimEnd("`r", "`n") "before_tool_call_callback_types"
$beforeToolContextAnchor = '  loopDetection?: ToolLoopDetectionConfig;`n  onToolOutcome?: ToolOutcomeObserver;'.Replace('`n', "`n")
$beforeToolContextReplacement = '  loopDetection?: ToolLoopDetectionConfig;`n  onBeforeToolCall?: ToolBeforeCallObserver;`n  onToolOutcome?: ToolOutcomeObserver;'.Replace('`n', "`n")
$content.beforeToolCall = Replace-ExactOnce $content.beforeToolCall $beforeToolContextAnchor $beforeToolContextReplacement "before_tool_call_callback_context"
$beforeToolLoggerAnchor = 'const log = createSubsystemLogger("agents/tools");'
$beforeToolLoggerReplacement = $beforeToolLoggerAnchor + "`n" + 'const MAX_LIVE_TOOL_OUTCOME_RUNS = 4096;' + "`n" + 'const lastLiveToolOutcomeByRun = new Map<string, Pick<ToolOutcomeObservation, "toolName" | "argsHash" | "resultHash">>();'
$content.beforeToolCall = Replace-ExactOnce $content.beforeToolCall $beforeToolLoggerAnchor $beforeToolLoggerReplacement "before_tool_call_progress_state"
$beforeToolOutcomeAnchor = @'
    if (record?.resultHash && args.ctx.onToolOutcome) {
      recordedOutcome = {
        toolName: record.toolName,
        argsHash: record.argsHash,
        resultHash: record.resultHash,
      };
    }
'@
$beforeToolOutcomeReplacement = @'
    if (record?.resultHash && args.ctx.onToolOutcome && args.toolCallId) {
      const prior = args.ctx.runId ? lastLiveToolOutcomeByRun.get(args.ctx.runId) : undefined;
      const progress = !prior || prior.toolName !== record.toolName || prior.argsHash !== record.argsHash || prior.resultHash !== record.resultHash;
      recordedOutcome = {
        toolName: record.toolName,
        toolCallId: args.toolCallId,
        argsHash: record.argsHash,
        resultHash: record.resultHash,
        progress,
        resultBytes: boundedToolOutcomeBytes(args.result ?? args.error),
      };
      if (args.ctx.runId) {
        lastLiveToolOutcomeByRun.set(args.ctx.runId, recordedOutcome);
        if (lastLiveToolOutcomeByRun.size > MAX_LIVE_TOOL_OUTCOME_RUNS) {
          const oldest = lastLiveToolOutcomeByRun.keys().next().value;
          if (oldest) lastLiveToolOutcomeByRun.delete(oldest);
        }
      }
    }
'@
$content.beforeToolCall = Replace-ExactOnce $content.beforeToolCall $beforeToolOutcomeAnchor.TrimEnd("`r", "`n") $beforeToolOutcomeReplacement.TrimEnd("`r", "`n") "before_tool_call_outcome_payload"
$beforeToolRecordEndAnchor = '  if (recordedOutcome) {`n    args.ctx.onToolOutcome?.(recordedOutcome);`n  }`n}'.Replace('`n', "`n")
$beforeToolRecordEndReplacement = '  if (recordedOutcome) {`n    args.ctx.onToolOutcome?.(recordedOutcome);`n  }`n}`n`nfunction boundedToolOutcomeBytes(value: unknown): number {`n  try {`n    const serialized = JSON.stringify(value);`n    return Math.min(Buffer.byteLength(serialized ?? "", "utf8"), 64 * 1024 * 1024 + 1);`n  } catch {`n    return 0;`n  }`n}'.Replace('`n', "`n")
$content.beforeToolCall = Replace-ExactOnce $content.beforeToolCall $beforeToolRecordEndAnchor $beforeToolRecordEndReplacement "before_tool_call_outcome_size"
$beforeToolExecutionAnchor = '      recordAdjustedParamsForToolCall(toolCallId, executeParams, ctx?.runId);`n      const normalizedToolName = normalizeToolName(toolName || "tool");'.Replace('`n', "`n")
$beforeToolExecutionReplacement = '      recordAdjustedParamsForToolCall(toolCallId, executeParams, ctx?.runId);`n      const normalizedToolName = normalizeToolName(toolName || "tool");`n      if (ctx?.onBeforeToolCall) {`n        if (!toolCallId) throw new Error("Runtime tool call id is required.");`n        ctx.onBeforeToolCall({ toolName: normalizedToolName, toolCallId, args: executeParams });`n      }'.Replace('`n', "`n")
$content.beforeToolCall = Replace-ExactOnce $content.beforeToolCall $beforeToolExecutionAnchor $beforeToolExecutionReplacement "before_tool_call_preflight"

$runConfigAnchor = '    plugins: {`n      ...baseConfig.plugins,'.Replace('`n', "`n")
$runConfigReplacement = '    tools: {`n      ...baseConfig.tools,`n      loopDetection: { enabled: true, historySize: 30, warningThreshold: 1, criticalThreshold: 3, globalCircuitBreakerThreshold: 4, unknownToolThreshold: 2, detectors: { genericRepeat: true, knownPollNoProgress: true, pingPong: true }, postCompactionGuard: { windowSize: 3 } },`n    },`n    plugins: {`n      ...baseConfig.plugins,'.Replace('`n', "`n")
$content.runConfig = Replace-ExactOnce $content.runConfig $runConfigAnchor $runConfigReplacement "run_config_loop_detection"

$copyTargets = [ordered]@{
  enterpriseRunStore = @{ source = "src/enterprise-run-store.ts"; target = "src/enterprise-runtime/enterprise-run-store.ts" }
  enterpriseRunRegistry = @{ source = "src/enterprise-run-registry.ts"; target = "src/enterprise-runtime/enterprise-run-registry.ts" }
  runtimePolicy = @{ source = "src/runtime-policy.ts"; target = "src/enterprise-runtime/runtime-policy.ts" }
  runtimeHostRecovery = @{ source = "src/runtime-host-recovery.ts"; target = "src/enterprise-runtime/runtime-host-recovery.ts" }
  privateRunContext = @{ source = "src/private-run-context.ts"; target = "src/gateway/server-methods/private-run-context.ts" }
  enterpriseRuntimeMethods = @{ source = "src/enterprise-runtime-methods.ts"; target = "src/gateway/server-methods/enterprise-runtime-methods.ts" }
  fayaStatusResultCompat = @{ source = "src/faya-status-result-compat.ts"; target = "src/gateway/server-methods/faya-status-result-compat.ts" }
  thoughtsResponseStatusCompat = @{ source = "src/thoughts-response-status-compat.ts"; target = "src/gateway/server-methods/thoughts-response-status-compat.ts" }
  runtimeCapabilityHandshake = @{ source = "src/runtime-capability-handshake.ts"; target = "src/enterprise-runtime/runtime-capability-handshake.ts" }
}
$plannedWrites = [System.Collections.Generic.List[object]]::new()
foreach ($entry in $copyTargets.GetEnumerator()) {
  $overlaySource = Join-Path $overlay $entry.Value.source
  if (-not (Test-RegularOverlayFile -Path $overlaySource)) { throw "openclaw_overlay_source_missing:$($entry.Value.source)" }
  $targetPath = Join-Path $sourceRoot $entry.Value.target
  $baselineBytes = $null
  if ($AllowReviewedBaselineCopyReplacement.IsPresent) {
    # Production artifact assembly captures this exact prior byte state into
    # the immutable bundle. The normal standalone installer remains strict
    # and rejects an unknown pre-existing copied module.
    $baselineBytes = Read-RegularFileBytesOrMissing -Path $targetPath
  }
  $plannedWrites.Add([pscustomobject]@{
    Label = [string]$entry.Key
    TargetPath = $targetPath
    BeforeBytes = $baselineBytes
    AfterBytes = [System.IO.File]::ReadAllBytes($overlaySource)
  })
}
$utf8 = [System.Text.UTF8Encoding]::new($false)
foreach ($entry in $targets.GetEnumerator()) {
  $plannedWrites.Add([pscustomobject]@{
    Label = [string]$entry.Key
    TargetPath = [string]$entry.Value
    BeforeBytes = $beforeBytes[$entry.Key]
    AfterBytes = $utf8.GetBytes($content[$entry.Key])
  })
}
$transaction = Invoke-OverlayWriteTransaction -Writes @($plannedWrites) -DryRun:$DryRun
$verifiedTargetCount = if ($null -ne $transaction.verifiedTargetCount) { [int]$transaction.verifiedTargetCount } else { 0 }

[ordered]@{
  ok = $true
  dryRun = [bool]$DryRun
  openClawVersion = [string]$package.version
  sourceRoot = $sourceRoot
  overlayRoot = $overlay
  patchedTargets = @($targets.Values)
  changedTargetCount = [int]$transaction.changedTargetCount
  verifiedTargetCount = $verifiedTargetCount
  generatedAt = (Get-Date).ToUniversalTime().ToString("o")
} | ConvertTo-Json -Depth 6
