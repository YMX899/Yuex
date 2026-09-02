# Enterprise Runtime Overlay Source Tests

The checked-in overlay is TypeScript source, while its production imports use
NodeNext `.js` specifiers for the emitted OpenClaw build. Do not edit those
production specifiers to make source tests run.

Run the async Enterprise Runtime test from this directory with the test-only
loader. It maps only missing relative `.js` siblings inside this overlay to
their checked-in `.ts` sources; it never writes generated JavaScript.

```powershell
node --expose-gc --import ./tests/register-ts-source-loader.mjs --test ./tests/ts-source-loader.test.mjs ./tests/enterprise-runtime-async.test.ts ./tests/runtime-host-recovery.test.ts
```

The local Runtime contract checks are complete only when the source async,
Workspace Search, and generator tests all pass:

```powershell
node --expose-gc --import ./tests/register-ts-source-loader.mjs --test ./tests/ts-source-loader.test.mjs ./tests/enterprise-runtime-async.test.ts
node --expose-gc --import ./tests/register-ts-source-loader.mjs --test ./tests/runtime-host-recovery.test.ts
node --expose-gc --import ./tests/register-ts-source-loader.mjs --test ./tests/runtime-host-recovery-mtls-e2e.test.ts
node --expose-gc --import ./tests/register-ts-source-loader.mjs --test ./tests/runtime-workspace-write-guard.test.ts
node --expose-gc --import ./tests/register-ts-source-loader.mjs --test ./tests/thoughts-response-status-compat.test.ts ./tests/runtime-capability-handshake.test.ts
node --test ../openclaw-extensions/huahuo-context-tools/tests/workspace-search.test.mjs
node --test ../runtime/generate_runtime_contracts.test.mjs
node --test ../runtime/runtime_workspace_schema.test.mjs
```

`runtime-workspace-write-guard.test.ts` uses real temporary directories and
`node:fs/promises` writes after calling the production Registry pre-execution
callback. It proves read-only rejection, `output/` and `staging/` writes,
non-staging/traversal rejection, admission-to-execution lease expiry, and
duplicate/aborted calls without a successful-path write stub. Before every
admitted native write, the production Registry now resolves the mount with
`lstat`/`realpath`, validates each extant target component, and rejects a
symlink or Windows junction/reparse escape with `RUNTIME_PERMISSION_DENIED`
before Core receives the tool call. The focused test performs real attempted
writes through an external `output/` link and through a linked mount; neither
creates a file outside the admitted Workspace.

This source-level evidence does not replace a compiled pinned-Gateway install
and live Runtime write/abort exercise. More importantly, the Core callback is
an admission check only: it does not pass a directory handle or an
`O_NOFOLLOW`/descriptor-relative write to Core. A filesystem actor can still
replace a component after the callback and before Core opens the path. Atomic
reparse-race prevention in the actual Core writer is therefore a blocker; the
write guard remains partial and must not be represented as an end-to-end
native-write containment proof.

The loader requires a Node release with native TypeScript type stripping and
the `node:sqlite` module. It is test-only and is not copied by the production
overlay installer.

## Pinned Gateway Build Artifact

The Gateway overlay must never be applied in-place to an arbitrary installed
OpenClaw tree. A capability endpoint that exposes raw commands such as `ls`,
`find`, or `grep` is evidence that the pinned async handler is not active.

Prepare a fresh, immutable OpenClaw `2026.6.2` source artifact on a controlled
build host where PowerShell is available. The source must pass the exact
overlay dry-run before it is packaged:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File ops/source/runtime/apply_openclaw_enterprise_overlay.ps1 `
  -OpenClawSourceRoot <clean-openclaw-2026.6.2-source> `
  -OverlayRoot ops/source/openclaw-enterprise-runtime-overlay
```

The target Linux host needs only the resulting reviewed source artifact, Node,
and pnpm. It must not use the old mismatched source tree or attempt to run the
PowerShell installer:

```bash
tar -xzf <reviewed-openclaw-overlay-artifact>.tar.gz -C <release-root>
cd <release-root>/openclaw-2026.6.2
pnpm install --frozen-lockfile
pnpm build
```

Before activation, run the Node contract generator with
`--openclaw-source-root <release-root>/openclaw-2026.6.2`; it verifies the
pinned `read`/`write` source hashes. Do not rebuild or replace the separately
pinned Runtime Adapter binary as part of this Gateway overlay rollout.

The active `/enterprise.runtime/capabilities` response must contain exactly
`read`, `workspace_search`, and `write`, plus the exact 200-call budget
execution contract. `ls`, `find`, `grep`, `workspace_material_search`, a
missing execution contract, or any hash/schema mismatch is a release blocker.

## Gateway Restart Recovery

The SQLite Run Store persists acceptance, JTI consumption, events and tool
counters, but it does not persist a live OpenClaw model loop or its request
context. At Gateway startup, every durable non-terminal Run is atomically
recorded as `recovering` and then `orphaned` with a safe
`RUNTIME_RUN_ORPHANED` recovery record. The Gateway never replays that model
loop or consumes the JTI again. A same-identity submit exposes the durable
`orphaned` state; Backend reconciliation must authorize a new attempt.

## Runtime Host Recovery Snapshot

The Run Store separately persists the submit-time Host/reservation/fencing/
capability binding and can record a complete, immutable hash-only recovery
fact after the trusted Host bridge supplies the signed execution scope and
Backend-assigned Host instance generation. A durable snapshot is canonicalized
with the Backend `runtime-host-recovery.v1` fact ordering and reads at most
512 facts plus one overflow row. It rejects a
non-durable store, an old/unbound active Run, missing complete fact, changed
fact, Host generation mismatch, or uncertain latest event boundary with a
fail-closed runtime error.

The current Gateway request handler has no certificate-bound Host principal or
mTLS context. `enterprise.runtime.recovery.snapshot` therefore deliberately
returns `RUNTIME_PERMISSION_DENIED`; it does not expose local occupancy or
open admission. The future Adapter recovery bridge must prove the Host
principal, compare the Gateway and Backend fact sets, complete Backend's
attestation, then and only then rebuild permits and open admission.
