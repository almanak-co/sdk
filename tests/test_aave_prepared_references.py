"""Current Ethereum and multi-chain author files against retained prepared inputs."""

import json
from types import SimpleNamespace

import httpx
import pytest

from almanak.backtest.economic_prepare import prepare
from almanak.backtest.economic_state import EconomicState
from almanak.backtest.engine import Journal, Simulator
from almanak.backtest.inputs import HarnessSelection
from almanak.backtest.prepared import load_dataset
from almanak.backtest.progress import ProgressJob
from almanak.backtest.progress_types import ProgressSnapshot
from almanak.connectors.aave_v4.economic_binding import result_report
from almanak.strategy import StrategyDefinition
from almanak.worker_session import preloaded_workers
from scripts.legacy_configuration.author import read_author
from scripts.legacy_configuration.backtest import read_backtest
from tests.aave_economic_support import prepared, provision_archive, strategy_name
from tests.legacy_references import harness_directory
from tests.test_uniswap_economic import no_http
from tests.uniswap_support import ROOT


@pytest.mark.parametrize("name,count", [("ethereum-reference", 12), ("multichain", 8)])
def test_current_reference_exactly_replays_its_full_economic_state(
    tmp_path, monkeypatch, name, count
):
    monkeypatch.setattr(httpx.Client, "send", no_http)
    manifest, expected, model = prepared(tmp_path, name)
    journal = Journal(tmp_path / "state.sqlite", EconomicState)
    journal.create(manifest.initial)
    try:
        with preloaded_workers():
            actual = Simulator(journal, model).run()
        assert actual == expected
        assert len(actual.completed) == count
        report = result_report(actual, model)
        assert report["closed"] and report["accounting_complete"]
        assert all(p["total_debt"] == "0" and p["shares"] == 0 for p in report["positions"])
        chains = {p.payload.target.fund.chain for p in actual.portfolio.positions}
        assert len(chains) == (2 if name == "multichain" else 1)
        assert all(c.pending.intent.target.fund.chain in chains for c in actual.completed)
    finally:
        model.book.close()


def test_multichain_archive_preparation_keeps_both_collectors_and_exact_inputs(tmp_path):
    directory, manifest, _ = provision_archive(tmp_path)
    name = strategy_name("multichain")
    harness = harness_directory(name)
    strategy = StrategyDefinition.read(ROOT / "strategies" / name / "strategy.py")
    author = read_author(harness / "config.yaml")
    settings = read_backtest(harness / "backtest-economic.yaml", author)
    args = SimpleNamespace(
        strategy=strategy.filename,
        config=harness / "config.yaml",
        backtest_config=harness / "backtest-economic.yaml",
        output=directory,
        env_file=None,
        _backtest_inputs=HarnessSelection(author, settings, strategy),
    )
    progress_path = tmp_path / "progress.json"
    with ProgressJob(progress_path, "retained-multichain-proof"):
        result = prepare(args)
    assert result["provider_activity"]["collection"]["cache_hits"] > 0
    snapshot = ProgressSnapshot.model_validate_json(progress_path.read_text())
    events = [p for p in snapshot.phases if p.phase_id.startswith("preparation.event_collection.")]
    assert len(events) == 2 and all(p.finished for p in events)
    samples = [
        p for p in snapshot.phases if p.phase_id.startswith("preparation.accounting_samples.")
    ]
    assert len(samples) == 4 and all(p.completed_units == p.total_units == 26 for p in samples)
    _, actual = load_dataset(directory)
    _, original = load_dataset(manifest.dataset)
    try:
        assert (
            actual.db.execute("SELECT body FROM inputs ORDER BY source,time").fetchall()
            == original.db.execute("SELECT body FROM inputs ORDER BY source,time").fetchall()
        )
        retained = set(actual.db.execute("SELECT digest,body FROM evidence").fetchall())
        captured = set(original.db.execute("SELECT digest,body FROM evidence").fetchall())
        assert captured < retained
        # New datasets additionally freeze admission checks that older captures
        # kept only in the acquisition cache, plus the change history of the ETH feed
        # contracts bound in avalanche-oracles.json after this dataset was captured.
        # Economic inputs remain exact above.
        added = [json.loads(body)["request"] for _, body in retained - captured]
        assert {r["chain"]["chain_id"] for r in added} == {"eip155:1", "eip155:43114"}
        assert {r["method"] for r in added} <= {
            "eth_call",
            "eth_getCode",
            "eth_getStorageAt",
            "eth_getLogs",
            "eth_getBlockByNumber",
        }
        assert any(r["method"] == "eth_getCode" for r in added)
        assert {r["params"][0]["address"] for r in added if r["method"] == "eth_getLogs"} == {
            "0x976b3d034e162d8bd72d6b9c989d545b839003b0",
            "0x9ff520924dc295074f43a8a01ee30a4d39a4223b",
        }
    finally:
        actual.close()
        original.close()
