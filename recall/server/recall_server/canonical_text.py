from __future__ import annotations

MAX_CANONICAL_TEXT_BYTES = 8_000_000
MAX_CANONICAL_CHUNK_BYTES = 24_000


def canonical_text_chunks(text: str) -> list[str]:
    """Split retrieval text into lossless, embedding-safe UTF-8 chunks."""

    if not isinstance(text, str):
        raise TypeError("canonical text must be a string")
    encoded = text.encode()
    if not encoded:
        return [""]
    chunks: list[str] = []
    offset = 0
    while offset < len(encoded):
        end = min(offset + MAX_CANONICAL_CHUNK_BYTES, len(encoded))
        while end < len(encoded) and end > offset and encoded[end] & 0xC0 == 0x80:
            end -= 1
        if end == offset:
            raise ValueError("canonical chunk boundary is invalid")
        candidate = encoded[offset:end].decode()
        if end < len(encoded):
            window_start = max(0, len(candidate) - 2_048)
            split = max(
                candidate.rfind("\n", window_start),
                candidate.rfind(" ", window_start),
            )
            if split >= window_start:
                candidate = candidate[: split + 1]
                end = offset + len(candidate.encode())
        chunks.append(candidate)
        offset = end
    return chunks
