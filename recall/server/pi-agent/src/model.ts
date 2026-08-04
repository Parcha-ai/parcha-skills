import type { Model } from "@earendil-works/pi-ai";

export const PROTOCOL = "recall.pi-run.v1" as const;

export type ModelEnvironment = {
  baseUrl: string;
  apiKey: string;
  routeKind: "private_broker" | "direct_provider";
  provider: "broker" | "openai-compatible";
};

export function modelEnvironment(
  environment: NodeJS.ProcessEnv = process.env,
): ModelEnvironment {
  const baseUrl = (environment.RECALL_PI_MODEL_BASE_URL || "").replace(/\/+$/, "");
  const apiKey = environment.RECALL_PI_API_KEY || "";
  const routeKind = environment.RECALL_PI_ROUTE_KIND;
  const provider = environment.RECALL_PI_PROVIDER;
  let url: URL;
  try {
    url = new URL(baseUrl);
  } catch {
    throw new Error("Recall Pi model URL is invalid");
  }
  if (
    !["http:", "https:"].includes(url.protocol)
    || !url.hostname
    || url.username
    || url.password
    || url.search
    || url.hash
    || !apiKey
    || !["private_broker", "direct_provider"].includes(String(routeKind))
    || !["broker", "openai-compatible"].includes(String(provider))
  ) {
    throw new Error("Recall Pi model configuration is invalid");
  }
  return {
    baseUrl,
    apiKey,
    routeKind: routeKind as ModelEnvironment["routeKind"],
    provider: provider as ModelEnvironment["provider"],
  };
}

export function openAiCompatibleModel(
  alias: string,
  baseUrl: string,
  reasoning: boolean,
): Model<"openai-completions"> {
  if (!alias || alias.length > 160) {
    throw new Error("Recall Pi model alias is invalid");
  }
  return {
    id: alias,
    name: alias,
    api: "openai-completions",
    provider: "recall" as Model<"openai-completions">["provider"],
    baseUrl: baseUrl.endsWith("/v1") ? baseUrl : `${baseUrl}/v1`,
    reasoning,
    input: ["text"],
    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
    contextWindow: 131_072,
    maxTokens: 32_768,
  };
}
