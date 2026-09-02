import assert from "node:assert/strict";
import test from "node:test";
import { projectThoughtsResponseStatusResult } from "../src/thoughts-response-status-compat.ts";

test("projects a read-only provider response wrapper with benign metadata into a visible final answer", () => {
  const source = { status: "succeeded", result: { finalAnswer: '{"thoughts":"private","reasoning":"brief","status":"complete","model":"provider-a","response":"Visible reply"}' } };
  const projected = projectThoughtsResponseStatusResult(source) as Record<string, any>;

  assert.notEqual(projected, source);
  assert.equal(projected.result.finalAnswer, "Visible reply");
  assert.equal(source.result.finalAnswer, '{"thoughts":"private","reasoning":"brief","status":"complete","model":"provider-a","response":"Visible reply"}');
});

test("does not project malformed, nested, failed, or action-shaped results", () => {
  const cases = [
    { status: "failed", result: { finalAnswer: '{"response":"Visible reply"}' } },
    { status: "succeeded", result: { finalAnswer: '{"response":""}' } },
    { status: "succeeded", result: { finalAnswer: '{"response":{"reply":"Visible reply"}}' } },
    { status: "succeeded", result: { finalAnswer: '{"response":"{\\"reply\\":\\"Visible reply\\"}"}' } },
    { status: "succeeded", result: { finalAnswer: '{"response":"Visible reply","assetWriteIntent":"write_file"}' } },
    { status: "succeeded", result: { finalAnswer: '{"response":"Visible reply","tool_calls":"read"}' } },
    { status: "succeeded", result: { finalAnswer: '{"response":"Visible reply","file":"output.md"}' } },
    { status: "succeeded", result: { finalAnswer: '{"response":"Visible reply","patch":"diff"}' } },
    { status: "succeeded", result: { finalAnswer: '{"response":"Visible reply","operation":"write"}' } },
    { status: "succeeded", result: { finalAnswer: '{"response":"Visible reply","writes":"output.md"}' } },
  ];

  for (const source of cases) assert.equal(projectThoughtsResponseStatusResult(source), source);
});
