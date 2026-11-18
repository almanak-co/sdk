"""Captured shared-fund runs preserve borrowed cash and cross-protocol recovery."""

import gzip
import json
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

from almanak.asset_amounts import AssetAmounts
from almanak.backtest.engine import Journal, Simulator
from almanak.connectors.composed.economic_binding import Manifest
from almanak.connectors.composed.economic_book import Book
from almanak.connectors.composed.economic_model import EconomicDriver
from almanak.connectors.composed.economic_reporting import report
from almanak.connectors.composed.execution import validate_inventory
from almanak.connectors.curve import models as curve
from almanak.connectors.curve.economic_data import MODEL as CURVE_MODEL
from almanak.connectors.curve.operations import Mint, Swap
from almanak.connectors.curve.support import candidate_support as curve_support
from almanak.connectors.interfaces import Connector
from almanak.connectors.morpho_blue import models as morpho
from almanak.connectors.morpho_blue.economic_data import MODEL as MORPHO_MODEL
from almanak.connectors.morpho_blue.support import candidate_support as morpho_support
from almanak.connectors.support import HistoricalSupport, RunMode
from almanak.effects import AssetDelta, apply_assets
from almanak.gateway.ethereum import DataError
from almanak.models import AssetAmount
from almanak.strategy import StrategyDefinition
from almanak.worker_session import preloaded_workers
from tests.composed_captures import with_dataset
from tests.legacy_references import current_imports_if_legacy, reviewed_current_imports
from tests.scope_only_sources import reviewed_record

ROOT = Path(__file__).parents[1]
EVIDENCE = (
    ROOT
    / "docs/internal/reports/evidence/2026-09-07-rollout/curve-morpho-composed-refactored/prepared"
)


def raw(path):
    return gzip.decompress((EVIDENCE / (path + ".gz")).read_bytes())


@pytest.fixture
def captured(tmp_path, monkeypatch):
    original = Connector.support_for

    def selected(self, chain):
        binding = {
            curve.Config: (curve_support, CURVE_MODEL),
            morpho.Config: (morpho_support, MORPHO_MODEL),
        }.get(self.codec.config)
        if binding is None:
            return original(self, chain)
        factory, model = binding
        row = next(value for value in factory() if value.chain == chain)
        return row.model_copy(
            update={
                "modes": (RunMode.BACKTEST,),
                "historical": (
                    HistoricalSupport(
                        model=model, mode="economic", actions=row.actions, variants=row.variants
                    ),
                ),
            }
        )

    monkeypatch.setattr(Connector, "support_for", selected)

    def forbidden(*args, **kwargs):
        raise AssertionError("offline shared Curve cash proof attempted HTTP")

    monkeypatch.setattr(httpx.Client, "send", forbidden)
    monkeypatch.setattr(httpx.AsyncClient, "send", forbidden)
    recorded = json.loads(raw("run/manifest.json"))
    manifest = Manifest.model_validate_json(json.dumps(reviewed_record(recorded)))
    assert reviewed_current_imports(manifest.strategy) == StrategyDefinition.read(
        ROOT / "strategies/ethereum_morpho_curve_borrow_and_trade/strategy.py"
    )
    dataset = tmp_path / "dataset"
    for source in (EVIDENCE / "dataset").rglob("*.gz"):
        relative = source.relative_to(EVIDENCE / "dataset").with_suffix("")
        target = dataset / relative
        target.parent.mkdir(exist_ok=True, parents=True)
        target.write_bytes(gzip.decompress(source.read_bytes()))
    manifest = with_dataset(manifest, recorded, dataset)
    expected = tmp_path / "expected.sqlite"
    expected.write_bytes(raw("run/state.sqlite"))
    return manifest, dataset, Journal(expected, type(manifest.initial)).load()


@pytest.mark.parametrize("stage", (None, "pending_commit", "effects_commit"))
def test_actual_reference_recovers_retained_shared_cash_without_duplicate_effects(
    captured, tmp_path, stage
):
    manifest, dataset, expected = captured
    initial = (
        manifest.initial
        if stage is None
        else type(manifest.initial).model_validate_json(
            raw(f"proof/curve-composed-recovery-{stage}-interrupted.json")
        )
    )
    journal = Journal(tmp_path / "state.sqlite", type(manifest.initial))
    journal.create(initial)
    book = Book(dataset, manifest.config, manifest.settings, manifest.dataset_sha256)
    try:
        strategy = current_imports_if_legacy(manifest.strategy)
        model = EconomicDriver(manifest.config, manifest.settings, strategy, book)
        with preloaded_workers():
            result = Simulator(journal, model).run()
        assert result == expected
        assert [step.pending.intent.kind for step in result.completed] == [
            "supply",
            "borrow",
            "swap_exact_input",
            "exit_exchange",
            "repay",
            "withdraw",
        ]
        assert all(step.transition.result.success for step in result.completed)
        measured = report(result, model)
        assert measured["closed"] and measured["delivered_outcomes"] == 6
        assert set(measured["components"]) == {"curve", "morpho_blue"}
        assert len({balance.key for balance in result.portfolio.balances}) == len(
            result.portfolio.balances
        )
        assert all(position.payload.empty for position in result.portfolio.positions[:2])
        assert result.portfolio.positions[-1].payload.owned.position is None
    finally:
        book.close()


def test_component_reports_drop_every_quote_valuation_trace(monkeypatch):
    from almanak.connectors.composed import economic_reporting
    from almanak.connectors.identifiers import ConnectorId

    native = {
        "run_id": "run",
        "mode": "economic",
        "gas_payers": [],
        "positions": [],
        "valuation": "explicit_sources",
        "valuation_prices": [{"value": "3000"}],
        "equity_curve": [{"time": 1}],
        "price_series": [{"time": 1}],
        "metrics": {"sharpe_ratio": None},
        "valuation_quote": "USD",
        "net_quote_return": {"quote": "USD", "value": "0", "return_pct": 0.0},
    }
    monkeypatch.setattr(economic_reporting, "component_state", lambda state, connector: state)
    monkeypatch.setattr(economic_reporting, "aave_v4_report", lambda state, driver: dict(native))
    state = SimpleNamespace(
        run_id="run",
        mode="economic",
        time=10,
        schedule=SimpleNamespace(model_dump=lambda mode: {}, opportunities=lambda seconds: 0),
        pending=None,
        completed=(),
        delivered=0,
        events_delivered=0,
        interval=SimpleNamespace(report=lambda: {}, effective=SimpleNamespace(seconds=60)),
        evaluations=0,
        portfolio=SimpleNamespace(balances=()),
    )
    model = SimpleNamespace(
        components={ConnectorId("aave_v4"): SimpleNamespace()},
        closed=lambda state, observed: True,
        observe=lambda state: (),
        strategy=SimpleNamespace(sha256="0" * 64),
    )
    measured = economic_reporting.report(state, model)
    component = measured["components"]["aave_v4"]
    for field in ("equity_curve", "price_series", "metrics", "valuation_quote", "valuation_prices"):
        assert field not in component
    assert component["valuation"] == "asset_units_only"
    assert component["net_quote_return"] is None


def test_borrowed_cash_and_each_addition_coin_are_checked_against_one_ledger(captured):
    manifest, _, _ = captured
    target = manifest.config.target("trade")
    capital = next(
        value
        for value in manifest.config.allocation("trade").capital.root
        if value.asset == target.funding_asset
    )
    operation = Swap(
        target=target,
        amount=capital,
        minimum=AssetAmount(asset=target.trading_asset, amount=1),
        deadline=manifest.initial.time + 100,
    )
    state = SimpleNamespace(inventory=manifest.config.inventory)
    with pytest.raises(DataError, match="retained fund inventory"):
        validate_inventory(state, operation)
    key = next(balance.key for balance in state.inventory if balance.key.asset == capital.asset)
    # The loan's amount is the strategy's decision; borrowing exactly the trade's capital
    # funds the swap once.
    borrowed = capital.amount
    funded = SimpleNamespace(
        inventory=apply_assets(state.inventory, (AssetDelta(key=key, amount=borrowed),))
    )
    validate_inventory(funded, operation)
    spent = SimpleNamespace(
        inventory=apply_assets(funded.inventory, (AssetDelta(key=key, amount=-borrowed),))
    )
    with pytest.raises(DataError, match="retained fund inventory"):
        validate_inventory(spent, operation)
    target = target.model_copy(update={"liquidity_enabled": True})
    addition = Mint(
        target=target,
        amounts=AssetAmounts(tuple(AssetAmount(asset=asset, amount=1) for asset in target.assets)),
        minimum_shares=AssetAmount(asset=target.pool.shares_asset, amount=1),
        deadline=operation.deadline,
    )
    with pytest.raises(DataError, match="retained fund inventory"):
        validate_inventory(funded, addition)
