"""Synthetic TypeSafe HTTP shapes, not invented SDK convenience attributes."""

from __future__ import annotations

import copy
import time


def primitive_questions() -> dict:
    return {
        "urgent": {"type": "noul", "instructions": "Does the message convey urgency?"},
        "source": {
            "type": "choice",
            "instructions": "Which source does the message explicitly request?",
            "criteria": {"chat": "Chat", "code": "Code", "none": "Neither"},
        },
        "quality": {
            "type": "score",
            "instructions": "How directly does the evidence answer the question?",
            "criteria": ["Unrelated", "Useful context", "Direct answer"],
        },
    }


def primitive_response() -> dict:
    return {
        "model": "jev-1.13.0",
        "answers": {
            "urgent": {"type": "noul", "noul": 0.92},
            "source": {
                "type": "choice",
                "choice": "chat",
                "probabilities": {"chat": 0.85, "code": 0.08, "none": 0.07},
                "confidence": 0.82,
            },
            "quality": {
                "type": "score",
                "score": 1.6,
                "legend": {
                    "0": "Unrelated",
                    "1": "Useful context",
                    "2": "Direct answer",
                },
                "probabilities": {"0": 0.05, "1": 0.3, "2": 0.65},
                "confidence": 0.78,
            },
        },
        "usage": {"input_tokens": 312, "output_tokens": 48},
    }


class FakeJudgmentTransport:
    """Return an explicit wire response; record calls for contract assertions."""

    def __init__(self, payload=None, *, error=None, delay=0.0):
        self.payload = primitive_response() if payload is None else payload
        self.error = error
        self.delay = delay
        self.calls: list[dict] = []

    def post(self, *, url, headers, body, timeout):
        self.calls.append(
            {
                "url": url,
                "headers": dict(headers),
                "body": copy.deepcopy(body),
                "timeout": timeout,
            }
        )
        if self.delay:
            time.sleep(self.delay)
        if self.error is not None:
            raise self.error
        return copy.deepcopy(self.payload)
