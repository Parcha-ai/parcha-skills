"""Exact shipped migration expectations, including the reserved optional 071 gap."""
from pathlib import Path

from recall_server import SCHEMA_VERSION


def expected_schema_versions() -> list[int]:
    # Keep the contiguous core explicit: do not derive it from whatever files
    # happen to remain on disk. Only the named optional 071 may be absent.
    if SCHEMA_VERSION not in (70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84):
        raise AssertionError(f"unreviewed latest schema version: {SCHEMA_VERSION}")
    versions = list(range(1, 71))
    schema = Path(__file__).resolve().parents[1] / "server" / "schema"
    if list(schema.glob("071_*.sql")):
        versions.append(71)
    if SCHEMA_VERSION in (72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84):
        versions.append(72)
    if SCHEMA_VERSION in (73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84):
        versions.append(73)
    if SCHEMA_VERSION in (74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84):
        versions.append(74)
    if SCHEMA_VERSION in (75, 76, 77, 78, 79, 80, 81, 82, 83, 84):
        versions.append(75)
    if SCHEMA_VERSION in (76, 77, 78, 79, 80, 81, 82, 83, 84):
        versions.append(76)
    if SCHEMA_VERSION in (77, 78, 79, 80, 81, 82, 83, 84):
        versions.append(77)
    if SCHEMA_VERSION in (78, 79, 80, 81, 82, 83, 84):
        versions.append(78)
    if SCHEMA_VERSION in (79, 80, 81, 82, 83, 84):
        versions.append(79)
    if SCHEMA_VERSION in (80, 81, 82, 83, 84):
        versions.append(80)
    if SCHEMA_VERSION in (81, 82, 83, 84):
        versions.append(81)
    if SCHEMA_VERSION in (82, 83, 84):
        versions.append(82)
    if SCHEMA_VERSION in (83, 84):
        versions.append(83)
    if SCHEMA_VERSION == 84:
        versions.append(84)
    if max(versions) != SCHEMA_VERSION:
        raise AssertionError("latest shipped migration differs from declared schema version")
    return versions
