import { Agent, type AgentTool, type StreamFn } from "@earendil-works/pi-agent-core";
import {
  createAssistantMessageEventStream,
  type Api,
  type AssistantMessage,
  type AssistantMessageEvent,
  type Model,
} from "@earendil-works/pi-ai";
import { streamSimple } from "@earendil-works/pi-ai/api/openai-completions";
import { createInterface } from "node:readline";
import type { TSchema } from "typebox";

import { modelEnvironment, openAiCompatibleModel, PROTOCOL } from "./model.js";

const MAX_FRAME_BYTES = 1_000_000;
const MODEL_STREAM_ATTEMPTS = 3;

function failedModelStream(
  model: Model<Api>,
  message: string,
  stopReason: "error" | "aborted" = "error",
) {
  const stream = createAssistantMessageEventStream();
  const error: AssistantMessage = {
    role: "assistant",
    content: [],
    api: model.api,
    provider: model.provider,
    model: model.id,
    usage: {
      input: 0,
      output: 0,
      cacheRead: 0,
      cacheWrite: 0,
      totalTokens: 0,
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
    },
    stopReason,
    errorMessage: message,
    timestamp: Date.now(),
  };
  stream.push({ type: "error", reason: stopReason, error });
  return stream;
}

export function failureCodeForModelMessage(message: AssistantMessage): string | undefined {
  if (message.stopReason === "aborted") return "pi_model_aborted";
  if (message.stopReason !== "error") return undefined;
  const detail = (message.errorMessage || "").toLowerCase();
  if (/context.{0,40}(length|window|token)|maximum context|too many tokens/.test(detail)) {
    return "pi_model_context_overflow";
  }
  if (/(^|\D)(401|403)(\D|$)|unauthorized|authentication|invalid api key/.test(detail)) {
    return "pi_model_auth_failed";
  }
  if (/(^|\D)429(\D|$)|rate.?limit|too many requests/.test(detail)) {
    return "pi_model_rate_limited";
  }
  if (/(^|\D)(408)(\D|$)|timed? out|timeout|etimedout|headers timeout/.test(detail)) {
    return "pi_model_timeout";
  }
  if (
    /(^|\D)(500|502|503|504)(\D|$)|unavailable|overloaded|connection|econnreset|fetch failed|terminated/.test(detail)
  ) {
    return "pi_model_unavailable";
  }
  if (/(^|\D)(400|404|422)(\D|$)|bad request|unprocessable/.test(detail)) {
    return "pi_model_bad_request";
  }
  return "pi_model_failed";
}

export function retryableModelFailure(code: string | undefined): boolean {
  return code === "pi_model_timeout"
    || code === "pi_model_rate_limited"
    || code === "pi_model_unavailable";
}

function safeFailureMessage(code: string): string {
  return {
    pi_model_timeout: "Model request timed out",
    pi_model_rate_limited: "Model provider rate limited the request",
    pi_model_unavailable: "Model provider was unavailable",
    pi_model_context_overflow: "Model context limit was exceeded",
    pi_model_auth_failed: "Model provider rejected authentication",
    pi_model_bad_request: "Model provider rejected the request",
    pi_model_aborted: "Model request was cancelled",
    pi_model_failed: "Model provider request failed",
  }[code] || "Pi agent failed";
}

async function retryPause(
  attempt: number,
  code: string | undefined,
  signal?: AbortSignal,
): Promise<void> {
  const milliseconds = (code === "pi_model_rate_limited" ? 5_000 : 500) * 2 ** attempt;
  await new Promise<void>((resolve, reject) => {
    if (signal?.aborted) {
      reject(new Error("model retry cancelled"));
      return;
    }
    const onAbort = () => {
      clearTimeout(timer);
      reject(new Error("model retry cancelled"));
    };
    const timer = setTimeout(() => {
      signal?.removeEventListener("abort", onAbort);
      resolve();
    }, milliseconds);
    signal?.addEventListener("abort", onAbort, { once: true });
  });
}

export const streamOpenAiCompletions: StreamFn = (model, context, options) => {
  if (model.api !== "openai-completions") {
    return failedModelStream(model, "Recall Pi received an unsupported model API");
  }
  const output = createAssistantMessageEventStream();
  void (async () => {
    for (let attempt = 0; attempt < MODEL_STREAM_ATTEMPTS; attempt += 1) {
      const buffered: AssistantMessageEvent[] = [];
      const stream = streamSimple(model as Model<"openai-completions">, context, {
        ...options,
        // Let pi-ai honor Retry-After and its standard provider backoff before
        // our complete-stream retry handles failures that arrive after headers
        // or partial SSE output.
        maxRetries: 2,
        maxRetryDelayMs: Math.min(options?.maxRetryDelayMs ?? 60_000, 60_000),
      });
      let failure: AssistantMessage | undefined;
      for await (const event of stream) {
        buffered.push(event);
        if (event.type === "error") failure = event.error;
      }
      const code = failure ? failureCodeForModelMessage(failure) : undefined;
      if (
        failure
        && retryableModelFailure(code)
        && attempt + 1 < MODEL_STREAM_ATTEMPTS
        && !options?.signal?.aborted
      ) {
        await retryPause(attempt, code, options?.signal);
        continue;
      }
      for (const event of buffered) output.push(event);
      output.end();
      return;
    }
  })().catch((error) => {
    const fallback = failedModelStream(
      model,
      error instanceof Error && error.message === "model retry cancelled"
        ? "Model request was cancelled"
        : "Recall Pi model retry failed",
      error instanceof Error && error.message === "model retry cancelled"
        ? "aborted"
        : "error",
    );
    void (async () => {
      for await (const event of fallback) output.push(event);
      output.end();
    })();
  });
  return output;
};

export function executionModeForTool(name: string): "parallel" | "sequential" {
  return name === "finish" ? "sequential" : "parallel";
}

export function failureCodeForStopReason(stopReason: string): string | undefined {
  if (stopReason === "error") return "pi_model_failed";
  if (stopReason === "aborted") return "pi_model_aborted";
  return undefined;
}

type JsonObject = Record<string, unknown>;
type InputFrame = {
  v: typeof PROTOCOL;
  turn_id: string;
  seq: number;
  type: "turn.start" | "tool.result";
  at: string;
  data: JsonObject;
};

type ToolResultFrame = {
  call_id: string;
  status: "ok" | "error";
  value?: unknown;
  error?: { code?: string; message: string };
};

type ToolDefinition = {
  name: string;
  description: string;
  input_schema: JsonObject;
  effect: "read";
  approval: "never";
  timeout_ms?: number;
  terminate_turn?: boolean;
};

type StartData = {
  session_id: string;
  prompt: { parts: Array<{ type: string; text?: string }> };
  prompt_sections?: Array<{ id: string; content: string }>;
  tools: ToolDefinition[];
  model: { alias: string; thinking?: string };
};

type PendingTool = {
  name: string;
  resolve: (frame: ToolResultFrame) => void;
  reject: (error: Error) => void;
};

function object(value: unknown, name: string): JsonObject {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new Error(`${name} must be an object`);
  }
  return value as JsonObject;
}

function text(value: unknown, name: string): string {
  if (typeof value !== "string" || !value) throw new Error(`${name} is required`);
  return value;
}

function parseFrame(line: string): InputFrame {
  if (Buffer.byteLength(line) + 1 > MAX_FRAME_BYTES) {
    throw new Error("input frame exceeds its bound");
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(line);
  } catch {
    throw new Error("input frame is not valid JSON");
  }
  const frame = object(parsed, "frame");
  if (
    frame.v !== PROTOCOL
    || !Number.isSafeInteger(frame.seq)
    || !["turn.start", "tool.result"].includes(String(frame.type))
  ) {
    throw new Error("input frame violated the protocol");
  }
  text(frame.turn_id, "frame.turn_id");
  object(frame.data, "frame.data");
  return frame as unknown as InputFrame;
}

export function validateStart(value: unknown): StartData {
  const data = object(value, "turn.start.data");
  text(data.session_id, "turn.start.data.session_id");
  const prompt = object(data.prompt, "turn.start.data.prompt");
  if (!Array.isArray(prompt.parts) || prompt.parts.length === 0) {
    throw new Error("turn.start.data.prompt.parts is invalid");
  }
  if (!Array.isArray(data.tools) || data.tools.length !== 6) {
    throw new Error("turn.start.data.tools must contain the closed Recall catalog");
  }
  const names = data.tools.map((item, index) => {
    const tool = object(item, `turn.start.data.tools[${index}]`);
    if (tool.effect !== "read" || tool.approval !== "never") {
      throw new Error("Recall Pi tools must be read-only and pre-authorized");
    }
    text(tool.description, `turn.start.data.tools[${index}].description`);
    object(tool.input_schema, `turn.start.data.tools[${index}].input_schema`);
    return text(tool.name, `turn.start.data.tools[${index}].name`);
  });
  if (
    new Set(names).size !== names.length
    || names.join(",") !== "search,map,find,open,exec,finish"
  ) {
    throw new Error("Recall Pi tool catalog is invalid");
  }
  const model = object(data.model, "turn.start.data.model");
  text(model.alias, "turn.start.data.model.alias");
  return data as unknown as StartData;
}

function systemPrompt(start: StartData): string {
  const sections = start.prompt_sections || [];
  if (sections.length === 0) {
    throw new Error("Recall Pi system prompt is missing");
  }
  return sections.map(({ id, content }) => `## ${id}\n${content}`).join("\n\n");
}

function promptText(start: StartData): string {
  return start.prompt.parts
    .filter((part) => part.type === "text" && typeof part.text === "string")
    .map((part) => part.text)
    .join("\n")
    .trim();
}

function thinkingLevel(value?: string): "minimal" | "low" | "medium" | "high" | "xhigh" | "max" {
  if (value === "off") return "minimal";
  if (["minimal", "low", "medium", "high", "xhigh", "max"].includes(String(value))) {
    return value as ReturnType<typeof thinkingLevel>;
  }
  return "low";
}

class Worker {
  private outputSequence = 0;
  private expectedInputSequence = 0;
  private turnId = "unknown";
  private pending = new Map<string, PendingTool>();
  private finished = false;
  private terminal = false;
  private agent: Agent | undefined;

  send(type: string, data: JsonObject): void {
    const frame = {
      v: PROTOCOL,
      turn_id: this.turnId,
      seq: this.outputSequence++,
      type,
      at: new Date().toISOString(),
      data,
    };
    const line = `${JSON.stringify(frame)}\n`;
    if (Buffer.byteLength(line) > MAX_FRAME_BYTES) {
      throw new Error("output frame exceeds its bound");
    }
    process.stdout.write(line);
  }

  async receive(frame: InputFrame): Promise<void> {
    if (frame.seq !== this.expectedInputSequence++) {
      throw new Error("input sequence is invalid");
    }
    if (frame.type === "turn.start") {
      if (this.agent || this.turnId !== "unknown") throw new Error("duplicate turn.start");
      this.turnId = frame.turn_id;
      await this.start(validateStart(frame.data));
      return;
    }
    if (frame.turn_id !== this.turnId) throw new Error("turn identity changed");
    this.resolveTool(frame.data);
  }

  private invoke(definition: ToolDefinition, callId: string, argumentsValue: unknown): Promise<ToolResultFrame> {
    if (this.finished) return Promise.reject(new Error("tool call followed accepted finish"));
    if (this.pending.has(callId)) return Promise.reject(new Error("duplicate tool call id"));
    return new Promise((resolve, reject) => {
      this.pending.set(callId, { name: definition.name, resolve, reject });
      this.send("tool.invoke", {
        call_id: callId,
        name: definition.name,
        arguments: object(argumentsValue, "tool arguments"),
        parent_event_id: null,
        effect: "read",
        approval: "never",
        timeout_hint_ms: definition.timeout_ms ?? null,
        idempotency: "none",
        readback: "result",
      });
    });
  }

  private resolveTool(value: unknown): void {
    const data = object(value, "tool.result.data") as ToolResultFrame;
    const callId = text(data.call_id, "tool.result.data.call_id");
    const pending = this.pending.get(callId);
    if (!pending) throw new Error("tool result has no pending invocation");
    this.pending.delete(callId);
    if (!["ok", "error"].includes(String(data.status))) {
      pending.reject(new Error("tool result status is invalid"));
      return;
    }
    pending.resolve(data);
  }

  private tools(definitions: ToolDefinition[]): AgentTool<TSchema, unknown>[] {
    return definitions.map((definition) => ({
      name: definition.name,
      label: definition.name,
      description: definition.description,
      parameters: definition.input_schema as TSchema,
      executionMode: executionModeForTool(definition.name),
      execute: async (toolCallId: string, params: unknown) => {
        const result = await this.invoke(definition, toolCallId, params);
        if (result.status === "error") {
          throw new Error(result.error?.message || "Recall rejected the evidence operation");
        }
        if (definition.name === "finish") this.finished = true;
        const rendered = typeof result.value === "string"
          ? result.value
          : JSON.stringify(result.value ?? null);
        return { content: [{ type: "text", text: rendered }], details: result.value };
      },
    }));
  }

  private async start(start: StartData): Promise<void> {
    const environment = modelEnvironment();
    const level = thinkingLevel(start.model.thinking);
    const agent = new Agent({
      sessionId: start.session_id,
      getApiKey: () => environment.apiKey,
      streamFn: streamOpenAiCompletions,
      afterToolCall: async ({ toolCall }) => (
        toolCall.name === "finish" && this.finished ? { terminate: true } : undefined
      ),
    });
    this.agent = agent;
    let failureCode = "pi_agent_failed";
    agent.subscribe((event) => {
      if (event.type !== "message_end" || event.message.role !== "assistant") return;
      failureCode = failureCodeForModelMessage(event.message) ?? failureCode;
    });
    agent.state.systemPrompt = systemPrompt(start);
    agent.state.model = openAiCompatibleModel(
      start.model.alias,
      environment.baseUrl,
      level !== "minimal",
    );
    agent.state.thinkingLevel = level;
    agent.state.tools = this.tools(start.tools);
    agent.followUp({
      role: "user",
      content: (
        "Before ending, call finish with the grounded answer or precise evidence gap. "
        + "Do not answer only in plain text."
      ),
      timestamp: Date.now(),
    });

    try {
      await agent.prompt(promptText(start));
      await agent.waitForIdle();
      if (!this.finished && failureCode === "pi_agent_failed") {
        await agent.prompt(
          "You ended without calling finish. Call finish now with the grounded answer "
          + "supported by already opened receipts, or with the precise evidence gap. "
          + "Do not write another plain-text response and do not call any other tool.",
        );
        await agent.waitForIdle();
      }
      if (!this.finished) {
        if (failureCode === "pi_agent_failed") failureCode = "pi_finish_missing";
        throw new Error("agent ended without a grounded finish");
      }
      if (this.pending.size !== 0) throw new Error("agent ended with unresolved tool calls");
      this.terminal = true;
      this.send("terminal.complete", {
        status: "complete",
        unresolved_call_ids: [],
        model_attestation: {
          model_alias: start.model.alias,
          route_kind: environment.routeKind,
          provider: environment.provider,
          route_identity: new URL(environment.baseUrl).hostname,
        },
      });
    } catch (error) {
      for (const pending of this.pending.values()) pending.reject(new Error("turn terminated"));
      this.pending.clear();
      this.terminal = true;
      this.send("terminal.failed", {
        status: "failed",
        unresolved_call_ids: [],
        reason: {
          code: failureCode,
          message: safeFailureMessage(failureCode),
        },
      });
    }
  }

  abort(): void {
    this.agent?.abort();
    for (const pending of this.pending.values()) pending.reject(new Error("turn interrupted"));
    this.pending.clear();
  }

  isTerminal(): boolean {
    return this.terminal;
  }
}

export function main(): void {
  const worker = new Worker();
  const input = createInterface({ input: process.stdin, crlfDelay: Infinity });
  let failed = false;
  input.on("line", (line) => {
    if (failed || !line) return;
    let frame: InputFrame;
    try {
      frame = parseFrame(line);
    } catch (error) {
      failed = true;
      process.stderr.write(`recall-pi protocol error: ${error instanceof Error ? error.message : "invalid input"}\n`);
      worker.abort();
      process.exitCode = 1;
      input.close();
      return;
    }
    void worker.receive(frame).catch((error) => {
      failed = true;
      process.stderr.write(`recall-pi runtime error: ${error instanceof Error ? error.message : "invalid input"}\n`);
      worker.abort();
      process.exitCode = 1;
      input.close();
    });
  });
  input.once("close", () => {
    if (failed || worker.isTerminal()) return;
    failed = true;
    process.stderr.write("recall-pi protocol error: stdin closed before terminal\n");
    worker.abort();
    process.exitCode = 1;
  });
  process.once("SIGTERM", () => worker.abort());
  process.once("SIGINT", () => worker.abort());
}

if (import.meta.url === `file://${process.argv[1]}`) {
  void main();
}
