import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { createServer } from "node:http";
import test from "node:test";
import { fileURLToPath } from "node:url";
import type { Api, AssistantMessage, Model } from "@earendil-works/pi-ai";

import { openAiCompatibleModel } from "./model.js";
import {
  executionModeForTool,
  failureCodeForModelMessage,
  failureCodeForStopReason,
  retryableModelFailure,
  streamOpenAiCompletions,
  validateStart,
} from "./worker.js";

const schema = { type: "object", properties: {}, required: [], additionalProperties: false };
const tools = ["search", "map", "find", "open", "exec", "finish"].map((name) => ({
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
  assert.deepEqual(start.tools.map((tool) => tool.name), ["search", "map", "find", "open", "exec", "finish"]);
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
  assert.equal(executionModeForTool("map"), "parallel");
  assert.equal(executionModeForTool("open"), "parallel");
  assert.equal(executionModeForTool("finish"), "sequential");
});

test("classifies model termination from typed stop reasons only", () => {
  assert.equal(failureCodeForStopReason("error"), "pi_model_failed");
  assert.equal(failureCodeForStopReason("aborted"), "pi_model_aborted");
  assert.equal(failureCodeForStopReason("stop"), undefined);
  assert.equal(failureCodeForStopReason("provider said timeout in prose"), undefined);
});

function failedMessage(errorMessage: string, stopReason: "error" | "aborted" = "error"): AssistantMessage {
  return {
    role: "assistant",
    content: [],
    api: "openai-completions",
    provider: "cerebras",
    model: "gemma-4-31b",
    usage: {
      input: 0,
      output: 0,
      cacheRead: 0,
      cacheWrite: 0,
      totalTokens: 0,
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
    },
    stopReason,
    errorMessage,
    timestamp: Date.now(),
  };
}

test("classifies provider failures without forwarding raw diagnostics", () => {
  assert.equal(failureCodeForModelMessage(failedMessage("Request timed out.")), "pi_model_timeout");
  assert.equal(failureCodeForModelMessage(failedMessage("429 Too Many Requests")), "pi_model_rate_limited");
  assert.equal(failureCodeForModelMessage(failedMessage("503 service unavailable")), "pi_model_unavailable");
  assert.equal(failureCodeForModelMessage(failedMessage("maximum context length exceeded")), "pi_model_context_overflow");
  assert.equal(failureCodeForModelMessage(failedMessage("401 invalid API key")), "pi_model_auth_failed");
  assert.equal(failureCodeForModelMessage(failedMessage("400 bad request")), "pi_model_bad_request");
  assert.equal(failureCodeForModelMessage(failedMessage("cancelled", "aborted")), "pi_model_aborted");
  assert.equal(retryableModelFailure("pi_model_timeout"), true);
  assert.equal(retryableModelFailure("pi_model_unavailable"), true);
  assert.equal(retryableModelFailure("pi_model_auth_failed"), false);
  assert.equal(retryableModelFailure("pi_model_context_overflow"), false);
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

async function providerRetryCase(
  firstStatus: number,
  retryAfterMs?: number,
): Promise<{ calls: number; elapsedMs: number }> {
  let calls = 0;
  const server = createServer((_request, response) => {
    calls += 1;
    if (calls === 1) {
      response.writeHead(firstStatus, {
        "content-type": "application/json",
        ...(retryAfterMs === undefined ? {} : { "retry-after-ms": String(retryAfterMs) }),
      });
      response.end(JSON.stringify({ error: { message: "synthetic provider failure" } }));
      return;
    }
    const chunks = [
      {
        id: "chatcmpl-retry",
        object: "chat.completion.chunk",
        created: 1,
        model: "gemma-4-31b",
        choices: [{ index: 0, delta: { role: "assistant", content: "ok" }, finish_reason: null }],
      },
      {
        id: "chatcmpl-retry",
        object: "chat.completion.chunk",
        created: 1,
        model: "gemma-4-31b",
        choices: [{ index: 0, delta: {}, finish_reason: "stop" }],
      },
    ];
    const body = chunks.map((chunk) => `data: ${JSON.stringify(chunk)}\n\n`).join("") + "data: [DONE]\n\n";
    response.writeHead(200, { "content-type": "text/event-stream" });
    response.end(body);
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const address = server.address();
  assert.ok(address && typeof address !== "string");
  try {
    const model = openAiCompatibleModel(
      "gemma-4-31b",
      `http://127.0.0.1:${address.port}/v1`,
      true,
    );
    const events = [];
    const started = Date.now();
    const stream = await streamOpenAiCompletions(model, {
      systemPrompt: "test",
      messages: [{ role: "user", content: "test", timestamp: Date.now() }],
      tools: [],
    }, { apiKey: "synthetic-key" });
    for await (const event of stream) events.push(event);
    assert.ok(events.length > 0);
    return { calls, elapsedMs: Date.now() - started };
  } finally {
    server.close();
    await once(server, "close");
  }
}

test("retries one fresh request for a retryable provider failure", async () => {
  assert.equal((await providerRetryCase(503)).calls, 2);
});

test("honors provider retry timing before a rate-limit retry", async () => {
  const result = await providerRetryCase(429, 700);
  assert.equal(result.calls, 2);
  assert.ok(result.elapsedMs >= 600, `retry returned after only ${result.elapsedMs}ms`);
});

test("does not retry a non-retryable provider failure", async () => {
  assert.equal((await providerRetryCase(400)).calls, 1);
});

test("discards a dropped partial SSE stream and retries the complete model request", async () => {
  let calls = 0;
  const server = createServer((_request, response) => {
    calls += 1;
    response.writeHead(200, { "content-type": "text/event-stream" });
    if (calls === 1) {
      response.write(`data: ${JSON.stringify({
        id: "chatcmpl-dropped",
        object: "chat.completion.chunk",
        created: 1,
        model: "gemma-4-31b",
        choices: [{ index: 0, delta: { role: "assistant", content: "discard-me" }, finish_reason: null }],
      })}\n\n`);
      response.destroy();
      return;
    }
    const chunks = [
      {
        id: "chatcmpl-recovered",
        object: "chat.completion.chunk",
        created: 1,
        model: "gemma-4-31b",
        choices: [{ index: 0, delta: { role: "assistant", content: "recovered" }, finish_reason: null }],
      },
      {
        id: "chatcmpl-recovered",
        object: "chat.completion.chunk",
        created: 1,
        model: "gemma-4-31b",
        choices: [{ index: 0, delta: {}, finish_reason: "stop" }],
      },
    ];
    response.end(chunks.map((chunk) => `data: ${JSON.stringify(chunk)}\n\n`).join("") + "data: [DONE]\n\n");
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const address = server.address();
  assert.ok(address && typeof address !== "string");
  try {
    const model = openAiCompatibleModel(
      "gemma-4-31b",
      `http://127.0.0.1:${address.port}/v1`,
      true,
    );
    const events = [];
    const stream = await streamOpenAiCompletions(model, {
      systemPrompt: "test",
      messages: [{ role: "user", content: "test", timestamp: Date.now() }],
      tools: [],
    }, { apiKey: "synthetic-key" });
    for await (const event of stream) events.push(event);
    assert.equal(calls, 2);
    assert.doesNotMatch(JSON.stringify(events), /discard-me/);
    assert.match(JSON.stringify(events), /recovered/);
  } finally {
    server.close();
    await once(server, "close");
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
