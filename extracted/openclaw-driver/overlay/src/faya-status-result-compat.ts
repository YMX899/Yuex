const FAYA_SCHEMA_VERSION = "viewpoint_germination.result.v2";
const FAYA_TASK_TYPE = "work_ai_faya_germination";
const FAYA_SKILL_PROFILE = "viewpoint_germination";
const FAYA_REPLY_COMPATIBILITY_REASON = "qualification_boundary";

type RecordValue = Record<string, unknown>;
type FayaEnvelopeProjection = { envelope: RecordValue };

// Older Workers consume parsedResult, while this Runtime Host keeps the model
// envelope in result.finalAnswer. Only project a self-identifying Faya result.
export function projectFayaStatusResult(status: unknown): unknown {
  const statusRecord = recordValue(status);
  if (!statusRecord || statusRecord.status !== "succeeded") return status;

  const result = recordValue(statusRecord.result);
  const projection = parseFayaEnvelope(result?.finalAnswer);
  if (!result || !projection) return status;

  const envelope = fayaReplyCompatibilityEnvelope(projection.envelope);
  if (!envelope) return status;

  return {
    ...statusRecord,
    result: {
      ...result,
      finalAnswer: JSON.stringify(envelope),
      parsedResult: envelope,
    },
  };
}

function parseFayaEnvelope(value: unknown): FayaEnvelopeProjection | undefined {
  if (typeof value !== "string") return undefined;
  const candidate = unwrapCompleteJSONFence(value);
  if (!candidate) return undefined;

  const parsed = parseJSONObject(candidate);
  if (parsed && isFayaEnvelope(parsed)) return { envelope: parsed };

  const repaired = parseJSONObject(repairSingleMissingRootBrace(candidate) ?? "");
  if (repaired && isFayaEnvelope(repaired)) return { envelope: repaired };

  const recovered = recoverMalformedFayaReplyEnvelope(candidate);
  return recovered ? { envelope: recovered } : undefined;
}

function recoverMalformedFayaReplyEnvelope(candidate: string): RecordValue | undefined {
  const replyMatch = /"data"\s*:\s*\{[\s\S]*?"reply"\s*:\s*"/.exec(candidate);
  if (!replyMatch) return undefined;
  const replyStart = replyMatch.index + replyMatch[0].length;
  const header = candidate.slice(0, replyStart);
  if (!fayaHeaderHasSelector(header, "schemaVersion", FAYA_SCHEMA_VERSION) ||
      !fayaHeaderHasSelector(header, "taskType", FAYA_TASK_TYPE) ||
      !fayaHeaderHasSelector(header, "skillProfile", FAYA_SKILL_PROFILE)) return undefined;

  const status = recoverMalformedHeaderString(header, "status");
  if (status === false || !fayaModelStatusAccepted(status)) return undefined;
  const taskBinding = recoverMalformedHeaderBinding(header, "taskId");
  const runBinding = recoverMalformedHeaderBinding(header, "runId");
  if (!taskBinding || !runBinding) return undefined;

  const replyEnd = findMalformedFayaReplyEnd(candidate, replyStart);
  if (replyEnd < replyStart || malformedFayaHasWriteIntent(candidate.slice(replyEnd))) return undefined;
  const reply = decodeLooseJSONString(candidate.slice(replyStart, replyEnd));
  if (!stringValue(reply)) return undefined;

  return {
    schemaVersion: FAYA_SCHEMA_VERSION,
    taskType: FAYA_TASK_TYPE,
    skillProfile: FAYA_SKILL_PROFILE,
    ...taskBinding,
    ...runBinding,
    ...(typeof status === "string" && status.trim() ? { status: status.trim() } : {}),
    data: { reply },
  };
}

function fayaHeaderHasSelector(header: string, key: string, expected: string): boolean {
  return new RegExp(`"${escapeRegExp(key)}"\\s*:\\s*"${escapeRegExp(expected)}"`).test(header);
}

function recoverMalformedHeaderString(header: string, key: string): string | false | undefined {
  const field = new RegExp(`"${escapeRegExp(key)}"\\s*:`);
  if (!field.test(header)) return undefined;
  const match = new RegExp(`"${escapeRegExp(key)}"\\s*:\\s*"([^"\\\\]*(?:\\\\.[^"\\\\]*)*)"`).exec(header);
  if (!match) return false;
  return decodeLooseJSONString(match[1]) ?? false;
}

function recoverMalformedHeaderBinding(source: string, key: "taskId" | "runId"): RecordValue | undefined {
  const value = recoverMalformedHeaderString(source, key);
  if (value === false) return undefined;
  if (value === undefined || !value.trim()) return {};
  return { [key]: value.trim() };
}

function findMalformedFayaReplyEnd(candidate: string, replyStart: number): number {
  const tail = candidate.slice(replyStart);
  const patterns = [
    /"\s*,\s*"report"\s*:\s*\{/,
    /"\s*,\s*"reason"\s*:\s*"/,
    /"\s*\}\s*,\s*"assetWriteIntent"\s*:/,
    /"\s*\}\s*\}\s*$/,
  ];
  let boundary = -1;
  for (const pattern of patterns) {
    const match = pattern.exec(tail);
    if (match && (boundary < 0 || match.index < boundary)) boundary = match.index;
  }
  return boundary < 0 ? -1 : replyStart + boundary;
}

function malformedFayaHasWriteIntent(suffix: string): boolean {
  const matches = [...suffix.matchAll(/"assetWriteIntent"\s*:/g)];
  const last = matches.at(-1);
  if (!last || last.index === undefined) return false;
  const value = suffix.slice(last.index + last[0].length).trim();
  return !/^(?:null|\{\s*\})\s*\}?\s*$/.test(value);
}

function decodeLooseJSONString(value: string): string | undefined {
  let decoded = "";
  for (let index = 0; index < value.length; index += 1) {
    const character = value[index];
    if (character !== "\\") {
      decoded += character;
      continue;
    }
    const escaped = value[++index];
    if (escaped === undefined) return undefined;
    if (escaped === '"' || escaped === "\\" || escaped === "/") decoded += escaped;
    else if (escaped === "b") decoded += "\b";
    else if (escaped === "f") decoded += "\f";
    else if (escaped === "n") decoded += "\n";
    else if (escaped === "r") decoded += "\r";
    else if (escaped === "t") decoded += "\t";
    else if (escaped === "u") {
      const hex = value.slice(index + 1, index + 5);
      if (!/^[0-9a-f]{4}$/i.test(hex)) return undefined;
      decoded += String.fromCharCode(Number.parseInt(hex, 16));
      index += 4;
    } else return undefined;
  }
  return decoded;
}

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function fayaReplyCompatibilityEnvelope(source: RecordValue): RecordValue | undefined {
  const data = recordValue(source.data);
  const reply = stringValue(data?.reply);
  if (!reply || !fayaModelStatusAccepted(source.status) || fayaHasWriteIntent(source.assetWriteIntent)) return undefined;

  const taskBinding = fayaModelBinding(source, "taskId");
  const runBinding = fayaModelBinding(source, "runId");
  if (!taskBinding || !runBinding) return undefined;

  return {
    schemaVersion: FAYA_SCHEMA_VERSION,
    taskType: FAYA_TASK_TYPE,
    skillProfile: FAYA_SKILL_PROFILE,
    ...taskBinding,
    ...runBinding,
    status: "no_viable_seed",
    data: {
      reply,
      reason: FAYA_REPLY_COMPATIBILITY_REASON,
    },
  };
}

function fayaModelStatusAccepted(value: unknown): boolean {
  if (value === undefined || value === null) return true;
  if (typeof value !== "string") return false;
  const status = value.trim();
  return status === "" || status === "succeeded" || status === "no_viable_seed";
}

function fayaHasWriteIntent(value: unknown): boolean {
  if (value === undefined || value === null) return false;
  const intent = recordValue(value);
  return !intent || Object.keys(intent).length > 0;
}

function fayaModelBinding(source: RecordValue, key: "taskId" | "runId"): RecordValue | undefined {
  const value = source[key];
  if (value === undefined || value === null) return {};
  if (typeof value !== "string") return undefined;
  const trimmed = value.trim();
  return trimmed ? { [key]: trimmed } : {};
}

function parseJSONObject(value: string): RecordValue | undefined {
  if (!value) return undefined;
  try {
    return recordValue(JSON.parse(value));
  } catch {
    return undefined;
  }
}

function unwrapCompleteJSONFence(value: string): string {
  const trimmed = value.trim();
  const fenced = /^```(?:json)?[ \t]*\r?\n([\s\S]*?)\r?\n?```\s*$/i.exec(trimmed);
  return (fenced?.[1] ?? trimmed).trim();
}

function repairSingleMissingRootBrace(value: string): string | undefined {
  const stack: string[] = [];
  let inString = false;
  let escaped = false;

  for (const character of value) {
    if (inString) {
      if (escaped) escaped = false;
      else if (character === "\\") escaped = true;
      else if (character === "\"") inString = false;
      continue;
    }
    if (character === "\"") {
      inString = true;
      continue;
    }
    if (character === "{" || character === "[") {
      stack.push(character);
      continue;
    }
    if (character === "}" || character === "]") {
      const opener = stack.pop();
      if ((character === "}" && opener !== "{") || (character === "]" && opener !== "[")) return undefined;
    }
  }

  return !inString && stack.length === 1 && stack[0] === "{" ? `${value}}` : undefined;
}

function isFayaEnvelope(value: RecordValue): boolean {
  return value.schemaVersion === FAYA_SCHEMA_VERSION &&
    value.taskType === FAYA_TASK_TYPE &&
    value.skillProfile === FAYA_SKILL_PROFILE;
}

function stringValue(value: unknown): string {
  return typeof value === "string" ? value.trim() : "";
}

function recordValue(value: unknown): RecordValue | undefined {
  return value && typeof value === "object" && !Array.isArray(value) ? value as RecordValue : undefined;
}
