"""Provider-neutral configuration for Recall's Pi model runtime."""

from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import urlsplit


THINKING_LEVELS = {"off", "minimal", "low", "medium", "high", "xhigh"}


@dataclass(frozen=True)
class PiModelRuntime:
    """One explicit OpenAI-compatible model route, without secret material."""

    alias: str
    base_url: str
    thinking: str
    route_kind: str
    provider: str
    provider_key_file: str | None
    route_identity: str

    @classmethod
    def from_environment(cls, environment: dict[str, str]) -> "PiModelRuntime":
        try:
            base_url = environment["RECALL_AGENT_MODEL_BASE_URL"].rstrip("/")
            alias = environment["RECALL_AGENT_MODEL_ALIAS"]
        except KeyError as error:
            raise RuntimeError(
                "Recall Pi agent model configuration is incomplete"
            ) from error
        key_file = environment.get("RECALL_AGENT_MODEL_KEY_FILE")
        route_kind = "direct_provider" if key_file else "private_broker"
        provider = "openai-compatible" if key_file else "broker"
        thinking = environment.get("RECALL_AGENT_THINKING", "low")
        identity = urlsplit(base_url).hostname or ""
        if (
            not base_url
            or not identity
            or not alias
            or len(alias) > 160
            or thinking not in THINKING_LEVELS
        ):
            raise RuntimeError("Recall Pi agent model configuration is invalid")
        return cls(
            alias=alias,
            base_url=base_url,
            thinking=thinking,
            route_kind=route_kind,
            provider=provider,
            provider_key_file=key_file,
            route_identity=identity,
        )
