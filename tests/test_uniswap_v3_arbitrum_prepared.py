"""Captured Arbitrum pool samples execute the actual references without provider access."""

import gzip
import json
from pathlib import Path

import httpx
import pytest

from almanak.backtest.engine import Journal, Simulator
from almanak.connectors.uniswap_v3.economic_binding import Manifest, UniswapEconomic
from almanak.worker_session import preloaded_workers
from tests.exchange_scope import without_exchange_budgets, without_repeated_entry_quotes
from tests.legacy_references import historical_strategy_current_imports
from tests.qa.frozen_reference import archived_reference
from tests.test_uniswap_v3_economic_runner import assert_roundtrip

ROOT = Path(__file__).parents[1]
EVIDENCE = (
    ROOT / "docs/internal/reports/evidence/2026-09-07-rollout/uniswap-v3-arbitrum-prepared-final"
)


def captured(directory, name):
    return gzip.decompress((EVIDENCE / directory / (name + ".gz")).read_bytes())


@pytest.mark.parametrize("style", ["trading", "lp"])
def test_actual_arbitrum_reference_reproduces_complete_prepared_state(tmp_path, monkeypatch, style):
    def forbidden(*args, **kwargs):
        raise AssertionError("offline Arbitrum reference attempted provider access")

    monkeypatch.setattr(httpx.Client, "send", forbidden)
    directory = "arbitrum-" + style
    dataset = tmp_path / "dataset"
    dataset.mkdir()
    for name in ("dataset.json", "data.sqlite"):
        (dataset / name).write_bytes(captured(directory + "/dataset", name))
    manifest = Manifest.model_validate_json(
        json.dumps(
            without_exchange_budgets(json.loads(captured(directory + "/run", "manifest.json")))
        )
    )
    archive = EVIDENCE / json.loads((EVIDENCE / "manifest.json").read_text())["source_archive"]
    strategy = archived_reference(archive.parent, archive.name, f"uniswap_v3_arbitrum_{style}")
    assert manifest.strategy == strategy
    strategy = historical_strategy_current_imports(strategy)
    manifest = manifest.model_copy(update={"dataset": dataset})
    book = UniswapEconomic.open_book(manifest)
    try:
        driver = UniswapEconomic.driver(manifest.config, manifest.settings, strategy, book)
        journal = Journal(tmp_path / "current.sqlite", type(manifest.initial))
        journal.create(manifest.initial)
        with preloaded_workers():
            state = Simulator(journal, driver).run()
        assert_roundtrip(state, driver, style, close_intent="exit_exchange")
        expected = tmp_path / "expected.sqlite"
        expected.write_bytes(captured(directory + "/run", "state.sqlite"))
        retained = json.loads(Journal(expected, type(manifest.initial)).load().model_dump_json())
        current = json.loads(state.model_dump_json())
        assert without_repeated_entry_quotes(current, retained) == retained
        assert {str(t.fund.chain.chain_id) for t in book.timelines} == {"eip155:42161"}
    finally:
        book.close()
