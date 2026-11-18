import json

import pytest

from almanak.cli import main
from almanak.configuration.selection import require_connector
from almanak.configuration.yaml import ConfigurationError
from almanak.connectors.composition import CONNECTOR_IDS, installed_connectors
from almanak.reports import support
from scripts.legacy_configuration.author import read_author


def test_all_trusted_ids_are_public_choices():
    assert tuple(name for name, _ in installed_connectors()) == CONNECTOR_IDS
    for identity in CONNECTOR_IDS:
        assert require_connector(str(identity)) == identity


@pytest.mark.parametrize("value", [None, [], 1, "uniswap-v3", "UNISWAP_V3", "invented"])
def test_unknown_connectors_never_select_a_binding(value):
    with pytest.raises(ConfigurationError) as failure:
        require_connector(value)
    assert failure.value.diagnostic()["code"] == "unknown_connector"
    assert "Supported connectors:" in str(failure.value)
    assert "almanak support" in failure.value.diagnostic()["remedy"]


@pytest.mark.parametrize("composed", [False, True])
def test_connector_typo_retains_author_file_field_and_line(tmp_path, composed):
    if composed:
        text = (
            "strategy: {decision_interval: 5s, allocations: {}}\n"
            "assets: {}\naccounts: {}\nmarkets:\n  selected:\n    connector: uniswap-v3\n"
        )
        field, line = "markets.selected.connector", 6
    else:
        text = (
            "strategy:\n  connector: uniswap-v3\n  decision_interval: 5s\n"
            "  allocations: {}\nassets: {}\naccounts: {}\nmarkets: {}\n"
        )
        field, line = "strategy.connector", 2
    path = tmp_path / "config.yaml"
    path.write_text(text)
    with pytest.raises(ConfigurationError) as failure:
        read_author(path)
    detail = failure.value.diagnostic()
    assert (detail["file"], detail["field"], detail["line"]) == (str(path), field, line)
    assert detail["code"] == "unknown_connector"
    assert "Did you mean 'uniswap_v3'?" in detail["message"]
    assert "almanak support" in detail["remedy"]


def test_filters_preserve_exact_admission_declarations():
    expected = [row for row in support()["uniswap_v3"] if row["chain"] == "eip155:8453"]
    assert support("uniswap_v3", "eip155:8453") == {"uniswap_v3": expected}
    assert support("aave_v4", "eip155:56") == {}
    assert support(chain="eip155:8453") == {
        name: selected
        for name, rows in support().items()
        if (selected := [row for row in rows if row["chain"] == "eip155:8453"])
    }


@pytest.mark.parametrize("chain", ["8453", "eip155:08453", "base", 8453])
def test_chain_filters_require_caip_identity(chain):
    with pytest.raises(ConfigurationError) as failure:
        support(chain=chain)
    assert failure.value.field == "--chain"


def test_cli_filters_and_selector_error(monkeypatch, capsys):
    monkeypatch.setattr(
        "sys.argv",
        ["almanak", "support", "--connector", "uniswap_v3", "--chain", "eip155:8453", "--json"],
    )
    assert main() == 0
    assert json.loads(capsys.readouterr().out) == support("uniswap_v3", "eip155:8453")
    monkeypatch.setattr("sys.argv", ["almanak", "support", "--connector", "uniswap-v3"])
    assert main() == 1
    detail = json.loads(capsys.readouterr().err)
    assert detail["field"] == "--connector"
    assert "Did you mean 'uniswap_v3'?" in detail["message"]
