from pathlib import Path

import pytest

from scripts.ci import check_strategies

MATRIX = {
    "uniswap_v3": {"eip155:1"},
    "aave_v3": {"eip155:1", "eip155:42161"},
    "gmx_v2": {"eip155:42161"},
}


def test_named_connectors_reads_top_level_and_market_bindings(tmp_path):
    config = tmp_path / "config.yaml"
    config.write_text(
        "strategy:\n  connector: uniswap_v3\n"
        "markets:\n  a:\n    connector: aave_v3\n  b:\n    connector: gmx_v2\n"
    )
    assert check_strategies.named_connectors(config) == ["aave_v3", "gmx_v2", "uniswap_v3"]
    config.write_text("strategy: {}\n")
    assert check_strategies.named_connectors(config) == []


def test_classify_accepts_valid_and_declared_refusals_only():
    classify = check_strategies.classify
    assert classify({"valid": True}, ["uniswap_v3"], MATRIX) == ("valid", "")
    refused = {"valid": False, "message": "Value error, connector does not support eip155:56"}
    assert classify(refused, ["uniswap_v3"], MATRIX)[0] == "refused"
    assert classify(refused, ["aave_v3", "gmx_v2"], MATRIX)[0] == "refused"
    supported = {"valid": False, "message": "Value error, connector does not support eip155:1"}
    assert classify(supported, ["uniswap_v3"], MATRIX)[0] == "failed"
    assert classify(supported, [], MATRIX)[0] == "failed"
    other = {"valid": False, "message": "Value error, unknown field"}
    assert classify(other, ["uniswap_v3"], MATRIX) == ("failed", "Value error, unknown field")


def write_strategy(root, name, connector):
    directory = root / "strategies" / name
    directory.mkdir(parents=True)
    (directory / "strategy.py").write_text("")
    (directory / "config.yaml").write_text(f"strategy:\n  connector: {connector}\n")
    return directory


def test_main_reports_every_strategy_and_fails_on_undeclared_outcomes(tmp_path, capsys):
    write_strategy(tmp_path, "ok", "uniswap_v3")
    write_strategy(tmp_path, "candidate", "uniswap_v3")
    write_strategy(tmp_path, "broken", "aave_v3")
    (tmp_path / "strategies" / "docs_only").mkdir()
    reports = {
        "ok": {"valid": True},
        "candidate": {"valid": False, "message": "connector does not support eip155:56"},
        "broken": {"valid": False, "message": "connector does not support eip155:1"},
    }
    summary = tmp_path / "summary.md"
    check = lambda directory: reports[directory.name]  # noqa: E731
    assert check_strategies.main(["--summary", str(summary)], tmp_path, MATRIX, check) == 1
    text = summary.read_text()
    assert "1 valid, 1 refused by the public support matrix, 1 failed." in text
    assert "`broken`: failed" in text and "`candidate`: refused" in text and "`ok`" not in text
    assert "::error::strategy broken failed check" in capsys.readouterr().out
    (tmp_path / "strategies" / "broken" / "strategy.py").unlink()
    assert check_strategies.main([], tmp_path, MATRIX, check) == 0


def test_main_fails_when_no_strategy_exists(tmp_path):
    (tmp_path / "strategies").mkdir()
    assert check_strategies.main([], tmp_path, MATRIX, lambda d: {"valid": True}) == 1


def test_check_runs_the_public_cli_for_a_converted_reference():
    report = check_strategies.run_check(Path("strategies") / "aave_v4_weth_supply")
    assert report["valid"] is True and not report["ready"]
    assert {s["stage"] for s in report["stages"] if s["status"] == "passed"} == {
        "configuration",
        "sandbox",
    }


def test_check_runs_the_public_cli_for_a_composed_reference():
    report = check_strategies.run_check(Path("strategies") / "arbitrum_aave_borrow_and_trade_gmx")
    assert report["valid"] is True
    assert {s["stage"] for s in report["stages"] if s["status"] == "passed"} == {
        "configuration",
        "sandbox",
    }


def test_check_command_always_dispatches_to_the_public_cli():
    directory = Path("strategies") / "aave_v4_weth_supply"
    assert check_strategies.check_command(Path("strategies") / "other")[-3:] == [
        "check",
        "strategies/other",
        "--json",
    ]
    assert check_strategies.check_command(directory)[-3:] == ["check", str(directory), "--json"]


def test_check_bounds_each_strategy_process():
    with pytest.raises(ValueError, match="exceeded"):
        check_strategies.run_check(Path("strategies") / "aave_v4_weth_supply", timeout=0.01)
