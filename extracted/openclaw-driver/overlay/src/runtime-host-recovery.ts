import { createHash, createPrivateKey, X509Certificate } from "node:crypto";
import { lstat, readFile, realpath } from "node:fs/promises";
import type { IncomingMessage } from "node:http";
import { createServer, type Server } from "node:https";
import { dirname, isAbsolute, relative, resolve } from "node:path";
import type { Duplex } from "node:stream";
import type { TLSSocket } from "node:tls";
import {
  EnterpriseRunRegistry,
  enterpriseError,
  enterpriseRunRegistry,
  type EnterpriseRuntimeHostRecoverySnapshot,
} from "./enterprise-run-registry.js";

const RECOVERY_METHOD = "enterprise.runtime.recovery.snapshot";
const RECOVERY_PATH = "/enterprise-runtime/recovery";
const MAX_RECOVERY_MESSAGE_BYTES = 8 * 1024;
const RECOVERY_REQUEST_TIMEOUT_MS = 10_000;
const RECOVERY_SPIFFE_PREFIX = "spiffe://huahuo/runtime-host/";
const CONTROL_PLANE_ID = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,1023}$/;
const RECOVERY_MATERIAL_DIRECTORY = /^runtime-host-mtls-[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/;
const RECOVERY_CONFIG_SCHEMA = "huahuo.gateway.enterprise-runtime-recovery-config.v1";
const recoveryContextBrand: unique symbol = Symbol("enterprise-runtime-recovery-mtls-context");

export type EnterpriseRuntimeRecoveryPrincipal = {
  runtimeHostId: string;
  instanceId: string;
  environment: string;
};

// This value is created only after the dedicated listener has verified the
// TLS handshake and leaf URI-SAN. It deliberately cannot be reconstructed
// from an RPC header, a request param, or a normal Gateway client context.
export type EnterpriseRuntimeRecoveryRequestContext = {
  readonly principal: Readonly<EnterpriseRuntimeRecoveryPrincipal>;
  readonly transport: "gateway-recovery-mtls.v1";
  readonly [recoveryContextBrand]?: true;
};

export type EnterpriseRuntimeRecoveryListenerConfig = {
  enabled?: boolean;
  host?: string;
  port?: number;
  certPath?: string;
  keyPath?: string;
  caPath?: string;
  environment?: string;
};

type RecoveryGatewayPathStat = {
  readonly uid: number;
  readonly mode: number;
  isFile: () => boolean;
  isDirectory: () => boolean;
  isSymbolicLink: () => boolean;
};

// This interface is deliberately injectable only at the local module boundary
// so source tests can exercise root/mode/link rejection without depending on
// the host OS. The installed Gateway always uses Node's real filesystem API.
export type EnterpriseRuntimeRecoveryFileSystem = {
  readFile: (path: string) => Promise<Buffer>;
  lstat: (path: string) => Promise<RecoveryGatewayPathStat>;
  realpath: (path: string) => Promise<string>;
};

export type EnterpriseRuntimeRecoverySecurityPolicy = {
  readonly gatewayConfigPath: string;
  readonly approvedSecureRoot: string;
  readonly expectedHost: string;
  readonly expectedPort: number;
  readonly expectedEnvironment: string;
};

export type EnterpriseRuntimeRecoveryConfigPreflight = {
  readonly recoveryConfigSha256: string;
};

export type EnterpriseRuntimeRecoveryListener = {
  readonly host: string;
  readonly port: number;
  readonly recoveryConfigSha256: string;
  close: () => Promise<void>;
};

type ValidatedRecoveryListenerConfig = {
  host: string;
  port: number;
  certPath: string;
  keyPath: string;
  caPath: string;
  environment: string;
};

type RecoveryRpcRequest = {
  id: string | number | null;
  method: string;
  params: Record<string, unknown>;
};

type ValidatedRecoverySecurityPolicy = EnterpriseRuntimeRecoverySecurityPolicy;

type PreparedRecoveryGatewayConfig = {
  readonly config: ValidatedRecoveryListenerConfig;
  readonly cert: Buffer;
  readonly key: Buffer;
  readonly ca: Buffer;
  readonly recoveryConfigSha256: string;
};

const DEFAULT_ENTERPRISE_RUNTIME_RECOVERY_SECURITY_POLICY: Readonly<EnterpriseRuntimeRecoverySecurityPolicy> = Object.freeze({
  gatewayConfigPath: "/home/huahuo-runtime/config/openclaw-gateway.json",
  approvedSecureRoot: "/home/huahuo-runtime/secure-env",
  expectedHost: "172.18.102.92",
  expectedPort: 18792,
  expectedEnvironment: "prelaunch",
});

const runtimeRecoveryFileSystem: EnterpriseRuntimeRecoveryFileSystem = Object.freeze({
  readFile: (path) => readFile(path),
  lstat: (path) => lstat(path),
  realpath: (path) => realpath(path),
});

// The ordinary Gateway RPC listener has no certificate-bound Host principal.
// It must retain this exact fail-closed response even after the separate mTLS
// listener is installed.
export function rejectUnverifiedRecoverySnapshotAccess(): never {
  throw enterpriseError("RUNTIME_PERMISSION_DENIED", "host recovery snapshot requires certificate-bound authorization");
}

export function createEnterpriseRuntimeRecoveryRequestContext(
  principal: EnterpriseRuntimeRecoveryPrincipal,
): EnterpriseRuntimeRecoveryRequestContext {
  assertPrincipal(principal);
  return Object.freeze({
    principal: Object.freeze({ ...principal }),
    transport: "gateway-recovery-mtls.v1" as const,
    [recoveryContextBrand]: true as const,
  });
}

// This is the single recovery-snapshot handler. The regular Gateway RPC has
// no branded context and therefore cannot reach it; the mTLS listener creates
// the context from its verified peer certificate before dispatching here.
export function handleEnterpriseRuntimeRecoverySnapshot(
  params: unknown,
  context: EnterpriseRuntimeRecoveryRequestContext | undefined,
  registry: EnterpriseRunRegistry = enterpriseRunRegistry,
): EnterpriseRuntimeHostRecoverySnapshot {
  if (!context || context[recoveryContextBrand] !== true || context.transport !== "gateway-recovery-mtls.v1") {
    return rejectUnverifiedRecoverySnapshotAccess();
  }
  const principal = context.principal;
  assertPrincipal(principal);
  const input = asExactSnapshotParams(params);
  if (input.runtimeHostId !== principal.runtimeHostId) {
    throw enterpriseError("RUNTIME_HOST_UNAUTHORIZED", "recovery principal does not own requested runtime host");
  }
  return registry.snapshotForAuthorizedHost({
    runtimeHostId: principal.runtimeHostId,
    instanceId: principal.instanceId,
    environment: principal.environment,
    instanceGeneration: input.instanceGeneration,
    recoveryRevision: input.recoveryRevision,
    recoveryState: "pending",
  });
}

// Extract a principal only from a successfully verified TLS peer. URI-SAN is
// intentionally parsed from the raw peer certificate rather than any HTTP
// metadata so a reverse proxy, header, or JSON field cannot impersonate a Host.
export function recoveryPrincipalFromAuthorizedTLSSocket(
  socket: TLSSocket,
  expectedEnvironment: string,
): EnterpriseRuntimeRecoveryPrincipal {
  if (!socket.authorized) {
    throw enterpriseError("RUNTIME_HOST_UNAUTHORIZED", "recovery mTLS peer is not authorized");
  }
  if (!CONTROL_PLANE_ID.test(expectedEnvironment)) {
    throw enterpriseError("RUNTIME_HOST_UNAUTHORIZED", "recovery listener environment is invalid");
  }
  const peer = socket.getPeerCertificate(true) as { raw?: Buffer } | undefined;
  if (!peer?.raw || peer.raw.length === 0) {
    throw enterpriseError("RUNTIME_HOST_UNAUTHORIZED", "recovery mTLS peer certificate is unavailable");
  }
  let certificate: X509Certificate;
  try {
    certificate = new X509Certificate(peer.raw);
  } catch {
    throw enterpriseError("RUNTIME_HOST_UNAUTHORIZED", "recovery mTLS peer certificate is invalid");
  }
  const uriSans = parseUriSubjectAltNames(certificate.subjectAltName);
  if (uriSans.length !== 1) {
    throw enterpriseError("RUNTIME_HOST_UNAUTHORIZED", "recovery certificate must contain exactly one URI-SAN principal");
  }
  return recoveryPrincipalFromVerifiedUriSan(uriSans[0]!, expectedEnvironment);
}

// Kept separately for focused unit coverage. Callers may use this only after
// a TLS stack has verified the peer chain and extracted the URI-SAN from that
// verified leaf certificate; it is not an alternate HTTP identity input.
export function recoveryPrincipalFromVerifiedUriSan(
  uriSan: string,
  expectedEnvironment: string,
): EnterpriseRuntimeRecoveryPrincipal {
  if (!CONTROL_PLANE_ID.test(expectedEnvironment)) {
    throw enterpriseError("RUNTIME_HOST_UNAUTHORIZED", "recovery listener environment is invalid");
  }
  const principal = parseRecoverySpiffeUri(uriSan);
  if (!principal || principal.environment !== expectedEnvironment) {
    throw enterpriseError("RUNTIME_HOST_UNAUTHORIZED", "recovery certificate environment is not authorized");
  }
  return principal;
}

// The normal Gateway startup path uses this helper so an enabled recovery
// listener is all-or-nothing with the ordinary Gateway bind. In particular, a
// malformed recovery config must fail before the normal listener accepts work,
// and a later normal-listener bind error must not strand a recovery endpoint.
export async function startGatewayWithEnterpriseRuntimeRecovery(params: {
  startRecoveryListener: () => Promise<EnterpriseRuntimeRecoveryListener | undefined>;
  startPrimaryGateway: () => Promise<void>;
}): Promise<EnterpriseRuntimeRecoveryListener | undefined> {
  let recoveryListener: EnterpriseRuntimeRecoveryListener | undefined;
  try {
    recoveryListener = await params.startRecoveryListener();
    await params.startPrimaryGateway();
    return recoveryListener;
  } catch (error) {
    if (recoveryListener) {
      try {
        await recoveryListener.close();
      } catch {
        throw enterpriseError("RUNTIME_STORAGE_UNAVAILABLE", "gateway recovery listener cleanup failed during startup");
      }
    }
    throw error;
  }
}

// Performs the file/material boundary independently of release tooling. It
// reads only the approved Gateway config and root-owned material, returns a
// redacted configuration hash, and never exposes raw paths or private bytes.
export async function preflightEnterpriseRuntimeRecoveryGateway(params: {
  config: EnterpriseRuntimeRecoveryListenerConfig | undefined;
  gatewayConfigPath?: string;
  securityPolicy?: EnterpriseRuntimeRecoverySecurityPolicy;
  fileSystem?: EnterpriseRuntimeRecoveryFileSystem;
}): Promise<EnterpriseRuntimeRecoveryConfigPreflight | undefined> {
  const config = validateRecoveryListenerConfig(params.config);
  if (!config) return undefined;
  const prepared = await prepareEnterpriseRuntimeRecoveryGatewayConfig({
    config,
    gatewayConfigPath: params.gatewayConfigPath,
    securityPolicy: params.securityPolicy,
    fileSystem: params.fileSystem,
  });
  return Object.freeze({ recoveryConfigSha256: prepared.recoveryConfigSha256 });
}

export async function startEnterpriseRuntimeRecoveryWss(params: {
  config: EnterpriseRuntimeRecoveryListenerConfig | undefined;
  gatewayConfigPath?: string;
  securityPolicy?: EnterpriseRuntimeRecoverySecurityPolicy;
  fileSystem?: EnterpriseRuntimeRecoveryFileSystem;
  registry?: EnterpriseRunRegistry;
  log?: { info?: (message: string) => void; warn?: (message: string) => void };
}): Promise<EnterpriseRuntimeRecoveryListener | undefined> {
  const config = validateRecoveryListenerConfig(params.config);
  if (!config) return undefined;
  const prepared = await prepareEnterpriseRuntimeRecoveryGatewayConfig({
    config,
    gatewayConfigPath: params.gatewayConfigPath,
    securityPolicy: params.securityPolicy,
    fileSystem: params.fileSystem,
  });
  const server = createServer(
    {
      cert: prepared.cert,
      key: prepared.key,
      ca: prepared.ca,
      requestCert: true,
      rejectUnauthorized: true,
      minVersion: "TLSv1.3",
      honorCipherOrder: true,
    },
    (_request, response) => {
      response.statusCode = 404;
      response.setHeader("content-type", "text/plain; charset=utf-8");
      response.end("not found");
    },
  );
  const clients = new Set<TLSSocket>();
  const registry = params.registry ?? enterpriseRunRegistry;
  server.on("upgrade", (request, socket, head) => {
    if (!isRecoveryUpgradePath(request)) {
      denyUpgrade(socket, 404);
      return;
    }
    let context: EnterpriseRuntimeRecoveryRequestContext;
    try {
      context = createEnterpriseRuntimeRecoveryRequestContext(
        recoveryPrincipalFromAuthorizedTLSSocket(socket as TLSSocket, prepared.config.environment),
      );
    } catch {
      denyUpgrade(socket, 403);
      return;
    }
    const tlsSocket = socket as TLSSocket;
    if (!acceptRecoveryWebSocket(request, tlsSocket)) {
      denyUpgrade(tlsSocket, 403);
      return;
    }
    clients.add(tlsSocket);
    tlsSocket.once("close", () => clients.delete(tlsSocket));
    serveOneRecoverySnapshot(tlsSocket, Buffer.from(head), context, registry);
  });
  await listen(server, prepared.config.host, prepared.config.port);
  const address = server.address();
  if (!address || typeof address === "string") {
    await closeServer(server, clients);
    throw enterpriseError("RUNTIME_STORAGE_UNAVAILABLE", "recovery listener has no TCP address");
  }
  params.log?.info?.(`enterprise runtime recovery mTLS listener bound to ${prepared.config.host}:${address.port} config=${prepared.recoveryConfigSha256}`);
  let closePromise: Promise<void> | undefined;
  return Object.freeze({
    host: prepared.config.host,
    port: address.port,
    recoveryConfigSha256: prepared.recoveryConfigSha256,
    close: () => {
      closePromise ??= closeServer(server, clients);
      return closePromise;
    },
  });
}

function validateRecoveryListenerConfig(
  value: EnterpriseRuntimeRecoveryListenerConfig | undefined,
): ValidatedRecoveryListenerConfig | undefined {
  if (!value || value.enabled !== true) return undefined;
  const host = typeof value.host === "string" && value.host.trim() ? value.host.trim() : "127.0.0.1";
  const port = value.port;
  const certPath = stringConfig(value.certPath);
  const keyPath = stringConfig(value.keyPath);
  const caPath = stringConfig(value.caPath);
  const environment = stringConfig(value.environment);
  if (typeof port !== "number" || !Number.isSafeInteger(port) || port < 1 || port > 65_535 || !certPath || !keyPath || !caPath ||
    !environment || !CONTROL_PLANE_ID.test(environment)) {
    throw enterpriseError("RUNTIME_HOST_UNAUTHORIZED", "recovery mTLS listener configuration is incomplete");
  }
  return { host, port, certPath, keyPath, caPath, environment };
}

async function prepareEnterpriseRuntimeRecoveryGatewayConfig(params: {
  config: ValidatedRecoveryListenerConfig;
  gatewayConfigPath?: string;
  securityPolicy?: EnterpriseRuntimeRecoverySecurityPolicy;
  fileSystem?: EnterpriseRuntimeRecoveryFileSystem;
}): Promise<PreparedRecoveryGatewayConfig> {
  const fileSystem = params.fileSystem ?? runtimeRecoveryFileSystem;
  try {
    const policy = resolveRecoverySecurityPolicy(params.gatewayConfigPath, params.securityPolicy);
    assertRecoveryListenerMatchesPolicy(params.config, policy);
    const configPath = canonicalAbsolutePath(policy.gatewayConfigPath);
    const secureRoot = canonicalAbsolutePath(policy.approvedSecureRoot);
    await assertRootOwnedDirectory(fileSystem, secureRoot);
    await assertRootOwnedConfigDirectory(fileSystem, dirname(configPath));
    const configBytes = await readRootOwnedConfig(fileSystem, configPath);
    assertGatewayConfigMatchesRuntimeConfig(configBytes, params.config);

    const materialDirectory = assertRecoveryMaterialLayout(params.config, secureRoot);
    await assertRootOwnedDirectory(fileSystem, materialDirectory);
    const [cert, key, ca] = await Promise.all([
      readRecoveryMaterial(fileSystem, params.config.certPath, "certificate"),
      readRecoveryMaterial(fileSystem, params.config.keyPath, "private_key"),
      readRecoveryMaterial(fileSystem, params.config.caPath, "trust"),
    ]);
    const fingerprints = assertRecoveryCertificateChain(cert, key, ca);
    const recoveryConfigSha256 = recoveryGatewayConfigHash({
      configBytes,
      config: params.config,
      serverCertificateSha256: fingerprints.serverCertificateSha256,
      certificateAuthoritySha256: fingerprints.certificateAuthoritySha256,
    });
    return Object.freeze({ config: params.config, cert, key, ca, recoveryConfigSha256 });
  } catch {
    // File names, real paths, PEM parse errors, and private material are never
    // allowed to leave this Gateway-local preflight boundary.
    throw enterpriseError("RUNTIME_HOST_UNAUTHORIZED", "recovery gateway configuration or material is invalid");
  }
}

function resolveRecoverySecurityPolicy(
  gatewayConfigPath: string | undefined,
  suppliedPolicy: EnterpriseRuntimeRecoverySecurityPolicy | undefined,
): ValidatedRecoverySecurityPolicy {
  const policy = suppliedPolicy ?? {
    ...DEFAULT_ENTERPRISE_RUNTIME_RECOVERY_SECURITY_POLICY,
    gatewayConfigPath: gatewayConfigPath ?? process.env.OPENCLAW_CONFIG_PATH ?? "",
  };
  if (!suppliedPolicy && policy.gatewayConfigPath !== DEFAULT_ENTERPRISE_RUNTIME_RECOVERY_SECURITY_POLICY.gatewayConfigPath) {
    throw enterpriseError("RUNTIME_HOST_UNAUTHORIZED", "recovery gateway configuration path is invalid");
  }
  if (!isAbsolute(policy.gatewayConfigPath) || !isAbsolute(policy.approvedSecureRoot) ||
    !isPrivateOrLoopbackIpv4(policy.expectedHost) || !Number.isSafeInteger(policy.expectedPort) ||
    policy.expectedPort < 1 || policy.expectedPort > 65_535 || !CONTROL_PLANE_ID.test(policy.expectedEnvironment)) {
    throw enterpriseError("RUNTIME_HOST_UNAUTHORIZED", "recovery gateway security policy is invalid");
  }
  return Object.freeze({
    gatewayConfigPath: canonicalAbsolutePath(policy.gatewayConfigPath),
    approvedSecureRoot: canonicalAbsolutePath(policy.approvedSecureRoot),
    expectedHost: policy.expectedHost,
    expectedPort: policy.expectedPort,
    expectedEnvironment: policy.expectedEnvironment,
  });
}

function assertRecoveryListenerMatchesPolicy(
  config: ValidatedRecoveryListenerConfig,
  policy: ValidatedRecoverySecurityPolicy,
): void {
  if (config.host !== policy.expectedHost || config.port !== policy.expectedPort || config.environment !== policy.expectedEnvironment ||
    !isPrivateOrLoopbackIpv4(config.host)) {
    throw enterpriseError("RUNTIME_HOST_UNAUTHORIZED", "recovery gateway listener binding is invalid");
  }
}

function assertRecoveryMaterialLayout(config: ValidatedRecoveryListenerConfig, secureRoot: string): string {
  const relativeCertPath = relative(secureRoot, canonicalAbsolutePath(config.certPath));
  const relativeKeyPath = relative(secureRoot, canonicalAbsolutePath(config.keyPath));
  const relativeCaPath = relative(secureRoot, canonicalAbsolutePath(config.caPath));
  const parts = [relativeCertPath, relativeKeyPath, relativeCaPath].map((value) => value.split(/[\\/]/));
  if (parts.some((segments) => segments.length !== 2 || segments[0]!.startsWith("..") || !RECOVERY_MATERIAL_DIRECTORY.test(segments[0]!)) ||
    parts[0]![0] !== parts[1]![0] || parts[0]![0] !== parts[2]![0] ||
    parts[0]![1] !== "host.crt" || parts[1]![1] !== "host.key" || parts[2]![1] !== "ca.crt") {
    throw enterpriseError("RUNTIME_HOST_UNAUTHORIZED", "recovery gateway material layout is invalid");
  }
  return resolve(secureRoot, parts[0]![0]!);
}

async function readRootOwnedConfig(fileSystem: EnterpriseRuntimeRecoveryFileSystem, configPath: string): Promise<Buffer> {
  const stat = await lstatRegularPath(fileSystem, configPath);
  if (stat.uid !== 0 || !isNonWritableByGroupOrOther(stat.mode)) {
    throw enterpriseError("RUNTIME_HOST_UNAUTHORIZED", "recovery gateway config ownership is invalid");
  }
  return await fileSystem.readFile(configPath);
}

async function assertRootOwnedConfigDirectory(fileSystem: EnterpriseRuntimeRecoveryFileSystem, directoryPath: string): Promise<void> {
  const canonical = canonicalAbsolutePath(directoryPath);
  const stat = await fileSystem.lstat(canonical);
  if (!stat.isDirectory() || stat.isSymbolicLink() || stat.uid !== 0 || !isNonWritableByGroupOrOther(stat.mode) ||
    !sameCanonicalPath(await fileSystem.realpath(canonical), canonical)) {
    throw enterpriseError("RUNTIME_HOST_UNAUTHORIZED", "recovery gateway config directory is invalid");
  }
}

async function readRecoveryMaterial(
  fileSystem: EnterpriseRuntimeRecoveryFileSystem,
  materialPath: string,
  kind: "certificate" | "private_key" | "trust",
): Promise<Buffer> {
  const stat = await lstatRegularPath(fileSystem, materialPath);
  const mode = stat.mode & 0o777;
  const expectedMode = kind === "private_key" ? mode === 0o600 : (mode === 0o600 || mode === 0o644);
  if (stat.uid !== 0 || !expectedMode) {
    throw enterpriseError("RUNTIME_HOST_UNAUTHORIZED", "recovery gateway material ownership is invalid");
  }
  return await fileSystem.readFile(canonicalAbsolutePath(materialPath));
}

async function assertRootOwnedDirectory(fileSystem: EnterpriseRuntimeRecoveryFileSystem, directoryPath: string): Promise<void> {
  const stat = await fileSystem.lstat(directoryPath);
  if (!stat.isDirectory() || stat.isSymbolicLink() || stat.uid !== 0 || (stat.mode & 0o777) !== 0o700 ||
    !sameCanonicalPath(await fileSystem.realpath(directoryPath), directoryPath)) {
    throw enterpriseError("RUNTIME_HOST_UNAUTHORIZED", "recovery gateway secure directory is invalid");
  }
}

async function lstatRegularPath(fileSystem: EnterpriseRuntimeRecoveryFileSystem, path: string): Promise<RecoveryGatewayPathStat> {
  const canonical = canonicalAbsolutePath(path);
  const stat = await fileSystem.lstat(canonical);
  if (!stat.isFile() || stat.isSymbolicLink() || !sameCanonicalPath(await fileSystem.realpath(canonical), canonical)) {
    throw enterpriseError("RUNTIME_HOST_UNAUTHORIZED", "recovery gateway file is invalid");
  }
  return stat;
}

function assertGatewayConfigMatchesRuntimeConfig(configBytes: Buffer, config: ValidatedRecoveryListenerConfig): void {
  let parsed: unknown;
  try {
    parsed = JSON.parse(configBytes.toString("utf8"));
  } catch {
    throw enterpriseError("RUNTIME_HOST_UNAUTHORIZED", "recovery gateway config is invalid");
  }
  const recovery = isRecord(parsed) && isRecord(parsed.gateway) ? parsed.gateway.enterpriseRuntimeRecovery : undefined;
  const expectedKeys = ["caPath", "certPath", "enabled", "environment", "host", "keyPath", "port"];
  if (!isRecord(recovery) || Object.keys(recovery).sort().join(",") !== expectedKeys.join(",") || recovery.enabled !== true ||
    recovery.host !== config.host || recovery.port !== config.port || recovery.certPath !== config.certPath ||
    recovery.keyPath !== config.keyPath || recovery.caPath !== config.caPath || recovery.environment !== config.environment) {
    throw enterpriseError("RUNTIME_HOST_UNAUTHORIZED", "recovery gateway config does not match loaded runtime config");
  }
}

function assertRecoveryCertificateChain(cert: Buffer, key: Buffer, ca: Buffer): {
  serverCertificateSha256: string;
  certificateAuthoritySha256: string;
} {
  try {
    const certificate = new X509Certificate(cert);
    const authority = new X509Certificate(ca);
    const privateKey = createPrivateKey(key);
    const now = Date.now();
    const validFrom = Date.parse(certificate.validFrom);
    const validTo = Date.parse(certificate.validTo);
    const authorityValidFrom = Date.parse(authority.validFrom);
    const authorityValidTo = Date.parse(authority.validTo);
    if (certificate.ca || !authority.ca || !certificate.checkPrivateKey(privateKey) || !certificate.verify(authority.publicKey) ||
      !Number.isFinite(validFrom) || !Number.isFinite(validTo) || validFrom > now || validTo <= now ||
      !Number.isFinite(authorityValidFrom) || !Number.isFinite(authorityValidTo) || authorityValidFrom > now || authorityValidTo <= now) {
      throw new Error("invalid recovery certificate chain");
    }
    return {
      serverCertificateSha256: certificateFingerprintSha256(certificate),
      certificateAuthoritySha256: certificateFingerprintSha256(authority),
    };
  } catch {
    throw enterpriseError("RUNTIME_HOST_UNAUTHORIZED", "recovery gateway certificate material is invalid");
  }
}

function recoveryGatewayConfigHash(input: {
  configBytes: Buffer;
  config: ValidatedRecoveryListenerConfig;
  serverCertificateSha256: string;
  certificateAuthoritySha256: string;
}): string {
  const payload = JSON.stringify({
    schema: RECOVERY_CONFIG_SCHEMA,
    configFileSha256: sha256Bytes(input.configBytes),
    listener: {
      enabled: true,
      host: input.config.host,
      port: input.config.port,
      environment: input.config.environment,
    },
    tls: {
      minVersion: "TLSv1.3",
      requestCert: true,
      rejectUnauthorized: true,
    },
    material: {
      layout: ["host.crt", "host.key", "ca.crt"],
      serverCertificateSha256: input.serverCertificateSha256,
      certificateAuthoritySha256: input.certificateAuthoritySha256,
    },
  });
  return sha256Bytes(Buffer.from(payload, "utf8"));
}

function certificateFingerprintSha256(certificate: X509Certificate): string {
  return `sha256:${certificate.fingerprint256.replaceAll(":", "").toLowerCase()}`;
}

function sha256Bytes(value: Buffer): string {
  return `sha256:${createHash("sha256").update(value).digest("hex")}`;
}

function canonicalAbsolutePath(value: string): string {
  if (!isAbsolute(value)) throw enterpriseError("RUNTIME_HOST_UNAUTHORIZED", "recovery gateway path is invalid");
  return resolve(value);
}

function sameCanonicalPath(left: string, right: string): boolean {
  return resolve(left) === resolve(right);
}

function isNonWritableByGroupOrOther(mode: number): boolean {
  return (mode & 0o022) === 0;
}

function isPrivateOrLoopbackIpv4(value: string): boolean {
  const octets = value.split(".").map((part) => Number(part));
  if (octets.length !== 4 || octets.some((part) => !Number.isInteger(part) || part < 0 || part > 255)) return false;
  const [first, second] = octets as [number, number, number, number];
  return first === 10 || first === 127 || (first === 172 && second >= 16 && second <= 31) || (first === 192 && second === 168);
}

function serveOneRecoverySnapshot(
  socket: TLSSocket,
  initial: Buffer,
  context: EnterpriseRuntimeRecoveryRequestContext,
  registry: EnterpriseRunRegistry,
): void {
  let completed = false;
  let buffered = initial;
  const timeout = setTimeout(() => {
    if (!completed) socket.end(websocketTextFrame(JSON.stringify({ id: null, ok: false, error: { code: "RUNTIME_PERMISSION_DENIED" } })));
  }, RECOVERY_REQUEST_TIMEOUT_MS);
  timeout.unref?.();
  socket.once("close", () => clearTimeout(timeout));
  const receive = (chunk?: Buffer) => {
    if (completed) return;
    if (chunk) buffered = Buffer.concat([buffered, chunk]);
    const decoded = decodeMaskedWebSocketTextFrame(buffered);
    if (decoded.kind === "incomplete") return;
    completed = true;
    clearTimeout(timeout);
    if (decoded.kind !== "text") {
      sendRecoveryResponse(socket, null, false, undefined, "RUNTIME_PERMISSION_DENIED");
      return;
    }
    const request = parseRecoveryRequest(decoded.payload);
    if (!request || request.method !== RECOVERY_METHOD) {
      sendRecoveryResponse(socket, null, false, undefined, "RUNTIME_PERMISSION_DENIED");
      return;
    }
    try {
      const snapshot = handleEnterpriseRuntimeRecoverySnapshot(request.params, context, registry);
      sendRecoveryResponse(socket, request.id, true, snapshot);
    } catch (error) {
      sendRecoveryResponse(socket, request.id, false, undefined, recoveryErrorCode(error));
    }
  };
  receive();
  socket.on("data", receive);
  socket.once("error", () => clearTimeout(timeout));
}

function asExactSnapshotParams(value: unknown): { runtimeHostId: string; instanceGeneration: number; recoveryRevision: number } {
  if (!isRecord(value)) throw enterpriseError("RUNTIME_INPUT_INVALID", "recovery snapshot params are invalid");
  const expected = new Set(["runtimeHostId", "instanceGeneration", "recoveryRevision"]);
  if (Object.keys(value).length !== expected.size || Object.keys(value).some((key) => !expected.has(key))) {
    throw enterpriseError("RUNTIME_INPUT_INVALID", "recovery snapshot params are incomplete or unsupported");
  }
  const runtimeHostId = typeof value.runtimeHostId === "string" ? value.runtimeHostId : "";
  const instanceGeneration = value.instanceGeneration;
  const recoveryRevision = value.recoveryRevision;
  if (!CONTROL_PLANE_ID.test(runtimeHostId) || typeof instanceGeneration !== "number" ||
    !Number.isSafeInteger(instanceGeneration) || instanceGeneration < 1 || typeof recoveryRevision !== "number" ||
    !Number.isSafeInteger(recoveryRevision) || recoveryRevision < 1) {
    throw enterpriseError("RUNTIME_INPUT_INVALID", "recovery snapshot params are invalid");
  }
  return { runtimeHostId, instanceGeneration, recoveryRevision };
}

function assertPrincipal(principal: EnterpriseRuntimeRecoveryPrincipal): void {
  if (!principal || !CONTROL_PLANE_ID.test(principal.runtimeHostId) || !CONTROL_PLANE_ID.test(principal.instanceId) ||
    !CONTROL_PLANE_ID.test(principal.environment)) {
    throw enterpriseError("RUNTIME_HOST_UNAUTHORIZED", "recovery host principal is invalid");
  }
}

function parseUriSubjectAltNames(subjectAltName: string | undefined): string[] {
  if (!subjectAltName) return [];
  // Node escapes separator characters in subjectAltName. Recovery URI-SANs are
  // restricted to the identifier alphabet, so any escaped candidate is denied.
  return subjectAltName.split(",").map((entry) => entry.trim())
    .filter((entry) => entry.startsWith("URI:"))
    .map((entry) => entry.slice("URI:".length));
}

function parseRecoverySpiffeUri(uri: string): EnterpriseRuntimeRecoveryPrincipal | undefined {
  if (!uri.startsWith(RECOVERY_SPIFFE_PREFIX) || uri.includes("\\")) return undefined;
  const segments = uri.slice(RECOVERY_SPIFFE_PREFIX.length).split("/");
  if (segments.length !== 3 || segments.some((segment) => !CONTROL_PLANE_ID.test(segment))) return undefined;
  return { environment: segments[0]!, runtimeHostId: segments[1]!, instanceId: segments[2]! };
}

function isRecoveryUpgradePath(request: IncomingMessage): boolean {
  return request.url === RECOVERY_PATH && typeof request.headers.upgrade === "string" &&
    request.headers.upgrade.toLowerCase() === "websocket";
}

function parseRecoveryRequest(raw: Buffer): RecoveryRpcRequest | undefined {
  let value: unknown;
  try {
    const text = raw.toString("utf8");
    if (Buffer.byteLength(text, "utf8") > MAX_RECOVERY_MESSAGE_BYTES) return undefined;
    value = JSON.parse(text);
  } catch {
    return undefined;
  }
  if (!isRecord(value) || !Object.hasOwn(value, "id") || typeof value.method !== "string" || !isRecord(value.params)) {
    return undefined;
  }
  const id = value.id;
  if (!(typeof id === "string" || typeof id === "number" || id === null) || (typeof id === "string" && id.length > 128)) return undefined;
  return { id, method: value.method, params: value.params };
}

function acceptRecoveryWebSocket(request: IncomingMessage, socket: TLSSocket): boolean {
  const connection = request.headers.connection;
  const upgrade = request.headers.upgrade;
  const version = request.headers["sec-websocket-version"];
  const key = request.headers["sec-websocket-key"];
  if (typeof connection !== "string" || !/(^|,)\s*upgrade\s*(,|$)/i.test(connection) ||
    typeof upgrade !== "string" || upgrade.toLowerCase() !== "websocket" || version !== "13" ||
    typeof key !== "string" || !isWebSocketKey(key)) {
    return false;
  }
  const accept = createHash("sha1").update(`${key}258EAFA5-E914-47DA-95CA-C5AB0DC85B11`).digest("base64");
  socket.write(`HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ${accept}\r\n\r\n`);
  return true;
}

function isWebSocketKey(value: string): boolean {
  try {
    return Buffer.from(value, "base64").length === 16;
  } catch {
    return false;
  }
}

type DecodedRecoveryFrame =
  | { kind: "incomplete" }
  | { kind: "invalid" }
  | { kind: "text"; payload: Buffer };

function decodeMaskedWebSocketTextFrame(buffer: Buffer): DecodedRecoveryFrame {
  if (buffer.length < 2) return { kind: "incomplete" };
  const fin = (buffer[0]! & 0x80) !== 0;
  const opcode = buffer[0]! & 0x0f;
  const masked = (buffer[1]! & 0x80) !== 0;
  let length = buffer[1]! & 0x7f;
  let offset = 2;
  if (!fin || opcode !== 0x1 || !masked) return { kind: "invalid" };
  if (length === 126) {
    if (buffer.length < offset + 2) return { kind: "incomplete" };
    length = buffer.readUInt16BE(offset);
    offset += 2;
  } else if (length === 127) {
    // The protocol's 64-bit length form is never needed for a bounded control
    // message. Reject it before allocating or waiting for attacker input.
    return { kind: "invalid" };
  }
  if (length > MAX_RECOVERY_MESSAGE_BYTES) return { kind: "invalid" };
  if (buffer.length < offset + 4 + length) return { kind: "incomplete" };
  const mask = buffer.subarray(offset, offset + 4);
  offset += 4;
  const payload = Buffer.allocUnsafe(length);
  for (let index = 0; index < length; index += 1) payload[index] = buffer[offset + index]! ^ mask[index % 4]!;
  return { kind: "text", payload };
}

function websocketTextFrame(text: string): Buffer {
  const payload = Buffer.from(text, "utf8");
  if (payload.length > MAX_RECOVERY_MESSAGE_BYTES) {
    throw enterpriseError("RUNTIME_PERMISSION_DENIED", "recovery response exceeds control-plane limit");
  }
  if (payload.length < 126) return Buffer.concat([Buffer.from([0x81, payload.length]), payload]);
  const header = Buffer.allocUnsafe(4);
  header[0] = 0x81;
  header[1] = 126;
  header.writeUInt16BE(payload.length, 2);
  return Buffer.concat([header, payload]);
}

function sendRecoveryResponse(
  socket: TLSSocket,
  id: string | number | null,
  ok: boolean,
  payload?: EnterpriseRuntimeHostRecoverySnapshot,
  code?: string,
): void {
  if (socket.destroyed) return;
  socket.end(websocketTextFrame(JSON.stringify(ok ? { id, ok: true, payload } : { id, ok: false, error: { code } })));
}

function recoveryErrorCode(error: unknown): string {
  const code = error && typeof error === "object" ? String((error as { code?: unknown }).code ?? "") : "";
  return /^[A-Z][A-Z0-9_]{1,127}$/.test(code) ? code : "RUNTIME_PERMISSION_DENIED";
}

function denyUpgrade(socket: Duplex, status: 403 | 404): void {
  socket.write(`HTTP/1.1 ${status} ${status === 403 ? "Forbidden" : "Not Found"}\r\nConnection: close\r\nContent-Length: 0\r\n\r\n`);
  socket.destroy();
}

function listen(server: Server, host: string, port: number): Promise<void> {
  return new Promise((resolve, reject) => {
    const onError = (error: Error) => {
      server.off("listening", onListening);
      reject(error);
    };
    const onListening = () => {
      server.off("error", onError);
      resolve();
    };
    server.once("error", onError);
    server.once("listening", onListening);
    server.listen(port, host);
  });
}

async function closeServer(server: Server, clients: Set<TLSSocket>): Promise<void> {
  for (const client of clients) client.destroy();
  await new Promise<void>((resolve, reject) => server.close((error) => {
    if (error && (error as NodeJS.ErrnoException).code !== "ERR_SERVER_NOT_RUNNING") {
      reject(error);
      return;
    }
    resolve();
  }));
}

function stringConfig(value: unknown): string | undefined {
  return typeof value === "string" && value.trim() ? value.trim() : undefined;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value) && Object.getPrototypeOf(value) === Object.prototype;
}
