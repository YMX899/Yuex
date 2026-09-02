import assert from "node:assert/strict";
import test from "node:test";
import { projectFayaStatusResult } from "../src/faya-status-result-compat.ts";

const envelope = {
  schemaVersion: "viewpoint_germination.result.v2",
  taskType: "work_ai_faya_germination",
  skillProfile: "viewpoint_germination",
  status: "succeeded",
  data: {
    reply: "ok",
    report: {
      creatorWorld: { interpretivePosition: "optional report data" },
    },
  },
};

const compatibilityEnvelope = {
  schemaVersion: "viewpoint_germination.result.v2",
  taskType: "work_ai_faya_germination",
  skillProfile: "viewpoint_germination",
  status: "no_viable_seed",
  data: { reply: "ok", reason: "qualification_boundary" },
};

test("projects a succeeded raw Faya V2 final answer into the reply compatibility shape", () => {
  const status = { status: "succeeded", result: { finalAnswer: JSON.stringify(envelope), parsedResult: null } };

  const projected = projectFayaStatusResult(status) as Record<string, any>;

  assert.notEqual(projected, status);
  assert.deepEqual(projected.result.parsedResult, compatibilityEnvelope);
  assert.equal(projected.result.finalAnswer, JSON.stringify(compatibilityEnvelope));
  assert.equal(status.result.parsedResult, null);
});

test("projects a complete fenced Faya V2 final answer", () => {
  const status = { status: "succeeded", result: { finalAnswer: `\`\`\`json\n${JSON.stringify(envelope)}\n\`\`\`` } };

  const projected = projectFayaStatusResult(status) as Record<string, any>;

  assert.deepEqual(projected.result.parsedResult, compatibilityEnvelope);
});

test("repairs a Faya V2 final answer missing only its final root brace", () => {
  const status = { status: "succeeded", result: { finalAnswer: JSON.stringify(envelope).slice(0, -1) } };

  const projected = projectFayaStatusResult(status) as Record<string, any>;

  assert.deepEqual(projected.result.parsedResult, compatibilityEnvelope);
  assert.equal(projected.result.finalAnswer, JSON.stringify(compatibilityEnvelope));
});

test("recovers a Faya V2 reply containing unescaped double quotes", () => {
  const finalAnswer = '{"schemaVersion":"viewpoint_germination.result.v2","taskType":"work_ai_faya_germination","skillProfile":"viewpoint_germination","status":"succeeded","data":{"reply":"# 深度洞察\\n\\n耐心资本的"耐心"本身也有保质期，从"能用"到"好用"。","report":{"mode":"viewpoint"}},"assetWriteIntent":null}';
  const status = { status: "succeeded", result: { finalAnswer } };

  const projected = projectFayaStatusResult(status) as Record<string, any>;

  assert.deepEqual(projected.result.parsedResult, {
    ...compatibilityEnvelope,
    data: {
      reply: '# 深度洞察\n\n耐心资本的"耐心"本身也有保质期，从"能用"到"好用"。',
      reason: "qualification_boundary",
    },
  });
});

test("recovers a complete Faya reply when the hidden report tail is truncated", () => {
  const finalAnswer = '{"schemaVersion":"viewpoint_germination.result.v2","taskType":"work_ai_faya_germination","skillProfile":"viewpoint_germination","status":"succeeded","data":{"reply":"完整回复","report":{"mode":"viewpoint","broken":';
  const status = { status: "succeeded", result: { finalAnswer } };

  const projected = projectFayaStatusResult(status) as Record<string, any>;

  assert.deepEqual(projected.result.parsedResult, {
    ...compatibilityEnvelope,
    data: { reply: "完整回复", reason: "qualification_boundary" },
  });
});

test("retains valid model task and run bindings for backend validation", () => {
  const status = {
    status: "succeeded",
    result: { finalAnswer: JSON.stringify({ ...envelope, taskId: "task-001", runId: "run-001" }) },
  };

  const projected = projectFayaStatusResult(status) as Record<string, any>;

  assert.deepEqual(projected.result.parsedResult, {
    ...compatibilityEnvelope,
    taskId: "task-001",
    runId: "run-001",
  });
});

test("does not project failed, malformed, unsafe, or non-Faya statuses", () => {
  const statuses = [
    { status: "failed", result: { finalAnswer: JSON.stringify(envelope) } },
    { status: "succeeded", result: { finalAnswer: "not json" } },
    { status: "succeeded", result: { finalAnswer: '{"schemaVersion":"viewpoint_germination.result.v2","taskType":"work_ai_faya_germination","skillProfile":"viewpoint_germination","data":{"reply":"no boundary' } },
    { status: "succeeded", result: { finalAnswer: '{"schemaVersion":"viewpoint_germination.result.v2","taskType":"work_ai_faya_germination","skillProfile":"viewpoint_germination","data":{"reply":"bad\\q","report":{}}}' } },
    { status: "succeeded", result: { finalAnswer: JSON.stringify({ ...envelope, skillProfile: "other" }) } },
    { status: "succeeded", result: { finalAnswer: JSON.stringify({ ...envelope, status: "failed" }) } },
    { status: "succeeded", result: { finalAnswer: JSON.stringify({ ...envelope, data: { reply: "" } }) } },
    { status: "succeeded", result: { finalAnswer: JSON.stringify({ ...envelope, assetWriteIntent: { operation: "write_file" } }) } },
    { status: "succeeded", result: { finalAnswer: JSON.stringify({ ...envelope, taskId: 42 }) } },
    { status: "succeeded", result: { finalAnswer: "plain answer", parsedResult: { reply: "preserved" } } },
  ];

  for (const status of statuses) assert.equal(projectFayaStatusResult(status), status);
});
