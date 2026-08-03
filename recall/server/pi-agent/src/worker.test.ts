import assert from "node:assert/strict";
import test from "node:test";

import { validateStart } from "./worker.js";

const schema = { type: "object", properties: {}, required: [], additionalProperties: false };
const tools = ["search", "find", "open", "exec", "finish"].map((name) => ({
  name,
  description: `${name} tool`,
  input_schema: schema,
  effect: "read",
  approval: "never",
}));

test("accepts only the closed Recall tool catalog", () => {
  const start = validateStart({
    session_id: "run_123",
    prompt: { parts: [{ type: "text", text: "question" }] },
    prompt_sections: [{ id: "role", content: "investigate" }],
    tools,
    model: { alias: "gemma-4-31b", thinking: "low" },
  });
  assert.deepEqual(start.tools.map((tool) => tool.name), ["search", "find", "open", "exec", "finish"]);
  assert.throws(() => validateStart({
    session_id: "run_123",
    prompt: { parts: [{ type: "text", text: "question" }] },
    tools: tools.slice(0, -1),
    model: { alias: "gemma-4-31b" },
  }), /closed Recall catalog/);
});

test("rejects mutable or approval-gated tools", () => {
  const unsafe = tools.map((tool) => ({ ...tool }));
  unsafe[3].effect = "write";
  assert.throws(() => validateStart({
    session_id: "run_123",
    prompt: { parts: [{ type: "text", text: "question" }] },
    tools: unsafe,
    model: { alias: "gemma-4-31b" },
  }), /read-only/);
});
