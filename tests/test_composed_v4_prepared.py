"""Actual mixed reference replay keeps captured protocol effects and one cash ledger."""

import gzip
import json
import socket
from pathlib import Path

import httpx

from almanak.backtest.cli import GatewayProcess
from almanak.backtest.engine import Journal, Simulator
from almanak.connectors.composed.economic_binding import ComposedEconomic, Manifest
from almanak.connectors.composed.economic_model import EconomicDriver
from almanak.connectors.composed.economic_state import State
from almanak.strategy import StrategyDefinition
from almanak.worker_session import preloaded_workers
from tests.composed_captures import with_dataset
from tests.legacy_references import current_imports_if_legacy, reviewed_current_imports
from tests.scope_only_sources import reviewed_record

ROOT = Path(__file__).parents[1]
CAPTURE = (
    ROOT
    / "docs/internal/reports/evidence/2026-09-09-rollout/v4-composed-base-prepared"
    / "runs/pinned-base_borrow_and_trade_v4-economic"
)


def restore(source, destination):
    for path in source.rglob("*.gz"):
        relative = path.relative_to(source).with_suffix("")
        output = destination / relative
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_bytes(gzip.decompress(path.read_bytes()))


def test_captured_lending_and_v4_effects_reproduce_with_shared_cash(tmp_path, monkeypatch):
    def forbid(*args, **kwargs):
        raise AssertionError("prepared reference attempted network or gateway access")

    monkeypatch.setattr(httpx.Client, "send", forbid)
    monkeypatch.setattr(socket.socket, "connect", forbid)
    monkeypatch.setattr(GatewayProcess, "__enter__", forbid)
    restore(CAPTURE / "dataset", tmp_path / "dataset")
    restore(CAPTURE / "run", tmp_path / "expected")
    raw = json.loads((tmp_path / "expected/manifest.json").read_bytes())
    manifest = Manifest.model_validate_json(json.dumps(reviewed_record(raw)))
    assert reviewed_current_imports(manifest.strategy) == StrategyDefinition.read(
        ROOT / "strategies/base_borrow_and_trade_v4/strategy.py"
    )
    manifest = with_dataset(manifest, raw, tmp_path / "dataset")
    expected = Journal(tmp_path / "expected/state.sqlite", State).load()
    book = ComposedEconomic.open_book(manifest)
    strategy = current_imports_if_legacy(manifest.strategy)
    model = EconomicDriver(manifest.config, manifest.settings, strategy, book)
    journal = Journal(tmp_path / "current.sqlite", State)
    journal.create(manifest.initial)
    try:
        with preloaded_workers():
            actual = Simulator(journal, model).run()
        assert actual == expected
        report = ComposedEconomic.report(actual, model)
    finally:
        book.close()
    assert report["closed"] and not report["pending"]
    assert report["completed_intents"] == report["delivered_outcomes"] == 6
    keys = {balance.key for balance in actual.portfolio.balances}
    assert len(keys) == len(report["wallets"])
    assert {balance.key for balance in manifest.config.inventory} <= keys
    token_wallets = [row for row in report["wallets"] if "/erc20:" in row["key"]["asset"]]
    # The explicit native payer inventory remains separate from the two traded assets.
    assert len(token_wallets) == 2
    assert [item.pending.intent.kind for item in actual.completed] == [
        "supply",
        "borrow",
        "swap_exact_input",
        "exit_exchange",
        "repay",
        "withdraw",
    ]
    exchange = report["components"]["uniswap_v4"]["positions"][0]
    assert exchange["complete"] and "pnl_excluding_gas" not in exchange
    contributions = report["gas_payers"][0]["contributions"]
    assert {entry["connector"] for entry in contributions} == {"aave_v3", "uniswap_v4"}
    assert all(entry["measurement_status"] == "excluded" for entry in contributions)
