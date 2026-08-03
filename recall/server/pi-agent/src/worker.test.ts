import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import test from "node:test";
import { fileURLToPath } from "node:url";
import type { Api, Model } from "@earendil-works/pi-ai";

import { openAiCompatibleModel } from "./model.js";
import {
  executionModeForTool,
  streamOpenAiCompletions,
  validateStart,
} from "./worker.js";

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

test("serializes grounded finish while allowing retrieval concurrency", () => {
  assert.equal(executionModeForTool("search"), "parallel");
  assert.equal(executionModeForTool("open"), "parallel");
  assert.equal(executionModeForTool("finish"), "sequential");
});

test("normalizes an impossible model mismatch into a Pi error stream", async () => {
  const configured = openAiCompatibleModel(
    "gemma-4-31b",
    "http://127.0.0.1:4000",
    true,
  );
  const wrongApi = {
    ...configured,
    api: "anthropic-messages",
  } as unknown as Model<Api>;
  const events = [];
  const stream = await streamOpenAiCompletions(wrongApi, {
    systemPrompt: "test",
    messages: [],
    tools: [],
  });
  for await (const event of stream) {
    events.push(event);
  }
  assert.equal(events.length, 1);
  assert.equal(events[0]?.type, "error");
  if (events[0]?.type === "error") {
    assert.equal(events[0].error.stopReason, "error");
    assert.match(events[0].error.errorMessage || "", /unsupported model API/);
  }
});

test("fails closed when the host closes stdin before a terminal", async () => {
  const workerPath = fileURLToPath(new URL("./worker.js", import.meta.url));
  const child = spawn(process.execPath, [workerPath], {
    stdio: ["pipe", "ignore", "pipe"],
  });
  const stderr: Buffer[] = [];
  child.stderr.on("data", (chunk: Buffer) => stderr.push(chunk));
  child.stdin.end();
  const [code] = await once(child, "exit");
  assert.equal(code, 1);
  assert.match(Buffer.concat(stderr).toString(), /stdin closed before terminal/);
});
