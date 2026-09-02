type RecordValue = Record<string, unknown>;

// Some providers return a transport wrapper with hidden thoughts, model
// metadata, and a user-visible response. Accept a broad read-only wrapper,
// but never turn an action-shaped or nested result into a chat reply.
export function projectThoughtsResponseStatusResult(status: unknown): unknown {
  const statusRecord = recordValue(status);
  if (!statusRecord || statusRecord.status !== "succeeded") return status;

  const result = recordValue(statusRecord.result);
  const reply = compatibleResponse(result?.finalAnswer);
  if (!result || reply === undefined) return status;

  return {
    ...statusRecord,
    result: {
      ...result,
      finalAnswer: reply,
    },
  };
}

function compatibleResponse(value: unknown): string | undefined {
  if (typeof value !== "string") return undefined;
  const raw = value.trim().replace(/^\uFEFF/, "");
  if (!raw.startsWith("{")) return undefined;

  let payload: RecordValue;
  try {
    const parsed = JSON.parse(raw);
    payload = recordValue(parsed) ?? {};
  } catch {
    return undefined;
  }

  if (typeof payload.response !== "string") return undefined;
  if (Object.entries(payload).some(([key, entry]) => unsafeWrapperField(key, entry))) return undefined;

  const reply = payload.response.trim();
  return reply && !reply.startsWith("{") ? reply : undefined;
}

function unsafeWrapperField(key: string, value: unknown): boolean {
  const normalized = key.replace(/[^a-z0-9]/gi, "").toLowerCase();
  if ([
    "action", "actions", "assetwriteintent",
    "command", "commands",
    "file", "files",
    "operation", "operations",
    "patch", "patches",
    "tool", "tools", "toolcall", "toolcalls",
    "write", "writes", "writeplan",
  ].includes(normalized)) {
    return true;
  }
  return value !== null && typeof value === "object";
}

function recordValue(value: unknown): RecordValue | undefined {
  return value && typeof value === "object" && !Array.isArray(value) ? value as RecordValue : undefined;
}
