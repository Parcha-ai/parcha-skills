import assert from "node:assert/strict";
import test from "node:test";

import { modelEnvironment, openAiCompatibleModel, PROTOCOL } from "./model.js";

test("pins the Recall-owned protocol and OpenAI-compatible model", () => {
  assert.equal(PROTOCOL, "recall.pi-run.v1");
  const model = openAiCompatibleModel("gemma-4-31b", "http://127.0.0.1:4000", true);
  assert.equal(model.id, "gemma-4-31b");
  assert.equal(model.api, "openai-completions");
  assert.equal(model.baseUrl, "http://127.0.0.1:4000/v1");
  assert.equal(model.reasoning, true);
});

test("accepts only explicit model configuration", () => {
  assert.deepEqual(modelEnvironment({
    RECALL_PI_MODEL_BASE_URL: "https://api.example.test/v1/",
    RECALL_PI_API_KEY: "not-a-secret",
    RECALL_PI_ROUTE_KIND: "direct_provider",
    RECALL_PI_PROVIDER: "openai-compatible",
  }), {
    baseUrl: "https://api.example.test/v1",
    apiKey: "not-a-secret",
    routeKind: "direct_provider",
    provider: "openai-compatible",
  });
  assert.throws(() => modelEnvironment({}), /configuration|URL/);
  assert.throws(() => modelEnvironment({
    RECALL_PI_MODEL_BASE_URL: "https://user:secret@example.test/v1",
    RECALL_PI_API_KEY: "secret",
    RECALL_PI_ROUTE_KIND: "direct_provider",
    RECALL_PI_PROVIDER: "openai-compatible",
  }), /configuration/);
});
