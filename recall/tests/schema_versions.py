"""Exact shipped migration expectations, including the reserved optional 071 gap."""
from pathlib import Path

from recall_server import SCHEMA_VERSION


def expected_schema_versions() -> list[int]:
    # Keep the contiguous core explicit: do not derive it from whatever files
    # happen to remain on disk. Only the named optional 071 may be absent.
    if SCHEMA_VERSION not in (70, 71, 72):
        raise AssertionError(f"unreviewed latest schema version: {SCHEMA_VERSION}")
    versions = list(range(1, 71))
    schema = Path(__file__).resolve().parents[1] / "server" / "schema"
    if list(schema.glob("071_*.sql")):
        versions.append(71)
    if SCHEMA_VERSION == 72:
        versions.append(72)
    if max(versions) != SCHEMA_VERSION:
        raise AssertionError("latest shipped migration differs from declared schema version")
    return versions
