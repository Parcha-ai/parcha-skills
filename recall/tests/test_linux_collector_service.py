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
