from pathlib import Path


def test_linux_collector_uses_enrolled_principal() -> None:
    unit = (
        Path(__file__).resolve().parents[1]
        / "server"
        / "deploy"
        / "recall-collector@.service"
    ).read_text()

    assert "EnvironmentFile=%h/.config/recall-brain/collector-%i.env" in unit
    assert "--principal-id ${RECALL_PRINCIPAL_ID}" in unit
    assert "--source-id ${RECALL_SOURCE_ID}" in unit
    assert "--interval ${RECALL_INTERVAL_SECONDS}" in unit


def test_only_codex_example_enables_the_archive_root() -> None:
    deploy = Path(__file__).resolve().parents[1] / "server" / "deploy"
    codex = (deploy / "collector-codex.env.example").read_text()
    claude = (deploy / "collector-claude.env.example").read_text()

    assert "RECALL_ARCHIVE_ROOT=/home/owner/.codex/archived_sessions" in codex
    assert "RECALL_ARCHIVE_ROOT" not in claude
