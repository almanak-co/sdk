"""The actual Avalanche borrowing reference over its immutable prepared archive."""

import gzip
import json
import lzma

import httpx

from almanak.backtest.cli import sdk_digest, write_json
from almanak.backtest.economic_cli import reproduce, resume
from almanak.backtest.economic_state import EconomicState
from almanak.backtest.engine import Journal, Simulator
from almanak.connectors.aave_v4.economic_binding import AaveEconomic, Manifest
from almanak.strategy import StrategyDefinition
from almanak.worker_session import preloaded_workers
from tests.uniswap_support import ROOT


def test_actual_avalanche_borrowing_runs_and_reproduces_offline(tmp_path, monkeypatch):
    from types import SimpleNamespace

    from tests.test_uniswap_economic import no_http

    monkeypatch.setattr(httpx.Client, "send", no_http)
    root = ROOT / "tests/fixtures"
    raw = json.loads(gzip.decompress((root / "aave-v4-avalanche-economic.json.gz").read_bytes()))
    dataset = tmp_path / "dataset"
    dataset.mkdir()
    (dataset / "data.sqlite").write_bytes(
        lzma.decompress((root / "aave-v4-avalanche-economic.sqlite.xz").read_bytes())
    )
    write_json(dataset / "dataset.json", raw["dataset_manifest"])
    from tests.lending_captures import current_capture

    # The bundle predates scope-only authority; read its manifest in today's interface.
    manifest = Manifest.model_validate_json(json.dumps(current_capture(raw["manifest"])))
    manifest = manifest.model_copy(
        update={
            "dataset": dataset,
            "sdk_digest": sdk_digest(),
            "strategy": StrategyDefinition.read(
                ROOT / "strategies/aave_v4_avalanche_borrowing/strategy.py"
            ),
        }
    )
    expected = EconomicState.model_validate_json(json.dumps(raw["state"]))
    from tests.dx_replay import current_memory

    expected = expected.model_copy(
        update={"memory": current_memory(manifest.strategy, expected.memory)}
    )
    book = AaveEconomic.open_book(manifest)
    directory = tmp_path / "run"
    directory.mkdir()
    write_json(directory / "manifest.json", manifest)
    journal = Journal(directory / "state.sqlite", EconomicState)
    journal.create(manifest.initial)
    try:
        model = AaveEconomic.driver(manifest.config, manifest.settings, manifest.strategy, book)
        with preloaded_workers():
            state = Simulator(journal, model).run()
        assert state == expected
        assert [c.pending.intent.kind for c in state.completed] == [
            "supply",
            "borrow",
            "repay",
            "withdraw",
        ]
        assert model.closed(state, model.observe(state))
        assert {str(b.key.chain.chain_id) for b in state.portfolio.balances} == {"eip155:43114"}
    finally:
        book.close()
    assert resume(SimpleNamespace(run=directory))["closed"]
    assert reproduce(SimpleNamespace(run=directory))["reproduced"]
