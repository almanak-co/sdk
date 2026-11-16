"""Economic execution retains inventory, limits, evidence and payer cost boundaries."""

from types import SimpleNamespace

import pytest

from almanak.backtest.engine import Journal, Simulator
from almanak.connectors.uniswap_v4.economic_config import EconomicCosts
from almanak.connectors.uniswap_v4.economic_model import quote_at
from almanak.connectors.uniswap_v4.economic_prepare import prepare
from almanak.connectors.uniswap_v4.economic_state import State
from almanak.gateway.ethereum import DataError
from almanak.worker_session import preloaded_workers
from tests.uniswap_economic_support import archive_selection, prepared, provision_archive
from tests.uniswap_support import ROOT


@pytest.mark.parametrize("failure", ["inventory", "slippage", "stale"])
def test_refused_fill_cannot_change_portfolio(tmp_path, failure):
    manifest, expected, model = prepared(tmp_path)
    pending = expected.completed[0].pending
    intent = pending.intent
    initial = manifest.initial
    if failure == "inventory":
        position = initial.portfolio.positions[0]
        owned = position.payload.owned
        reduced = owned.inventory0.model_copy(update={"amount": owned.inventory0.amount - 1})
        payload = position.payload.model_copy(
            update={"owned": owned.model_copy(update={"inventory0": reduced})}
        )
        initial = initial.model_copy(
            update={
                "portfolio": initial.portfolio.model_copy(
                    update={"positions": (position.model_copy(update={"payload": payload}),)}
                )
            }
        )
    elif failure == "slippage":
        minimum = expected.completed[0].transition.result.fill.output.amount + 1
        intent = intent.model_copy(
            update={"minimum_output": intent.minimum_output.model_copy(update={"amount": minimum})}
        )
    else:
        timeline = model.book.timelines[intent.target]
        timeline.requirement = timeline.requirement.model_copy(update={"max_age_seconds": 20})
    journal = Journal(tmp_path / "state.sqlite", State)
    state = initial.model_copy(update={"pending": pending.model_copy(update={"intent": intent})})
    journal.create(state)
    try:
        with pytest.raises((ValueError, DataError), match="inventory|minimum|observation age"):
            Simulator(journal, model).tick()
        assert journal.load() == state
    finally:
        model.book.close()


def test_fixed_costs_debit_only_the_explicit_operator_in_native_chain_units(tmp_path):
    manifest, expected, model = prepared(tmp_path, "avalanche", "lp")
    model.settings = model.settings.model_copy(
        update={
            "costs": EconomicCosts(
                kind="fixed_native", swap=5, mint=7, collect=11, burn=13, revoke=17
            )
        }
    )
    journal = Journal(tmp_path / "state.sqlite", State)
    journal.create(manifest.initial)
    try:
        with preloaded_workers():
            actual = Simulator(journal, model).run()
        assert model.closed(actual, model.observe(actual))
        assert sum(c.transition.result.gas_paid.amount for c in actual.completed) == 41
        for balance in actual.portfolio.balances:
            original = expected.portfolio.balance(balance.key)
            payer = balance.key.account == "operator" and balance.key.asset.namespace == "slip44"
            assert balance.amount == original - (41 if payer else 0)
    finally:
        model.book.close()


def test_wrong_pool_time_and_boundary_proof_are_refused(tmp_path):
    manifest, _, model = prepared(tmp_path, "ethereum", "lp")
    target = manifest.config.target("lp")
    timeline = model.book.timelines[target]
    row = timeline.rows[0]
    try:
        other = target.model_copy(update={"pool": target.pool.model_copy(update={"fee": 3000})})
        with pytest.raises(ValueError, match="another pool"):
            quote_at(row, other, manifest.config.allocations[0].capital, row.available_at)
        with pytest.raises(ValueError, match="another chain, pool or block"):
            model.book.validate_row(other, row)
        with pytest.raises(ValueError, match="time or tick bounds"):
            model.book.validate_row(
                target, row.model_copy(update={"source_time": row.source_time - 1})
            )
        baseline = model.book.baselines[target]
        wrong = baseline.model_copy(update={"initial_gross": (0, 0)})
        with pytest.raises(DataError, match="initialized"):
            model.book.validate_baseline(target, wrong, timeline)
    finally:
        model.book.close()


@pytest.mark.parametrize("chain,kind", [("ethereum", "lp"), ("avalanche", "trading")])
def test_fresh_preparation_from_retained_archive_preserves_exact_payloads(tmp_path, chain, kind):
    from almanak.backtest.prepared import load_dataset

    directory, manifest, _ = provision_archive(tmp_path, chain, kind)
    name = f"uniswap_v4_{'avalanche_' if chain == 'avalanche' else ''}{kind}"
    strategy = ROOT / "strategies" / name / "strategy.py"
    args = SimpleNamespace(
        strategy=strategy,
        output=directory,
        env_file=None,
        _backtest_inputs=archive_selection(name, manifest.settings),
    )
    result = prepare(args)
    _, actual = load_dataset(directory)
    _, original = load_dataset(manifest.dataset)
    try:
        from tests.uniswap_economic_support import assert_archive_payloads

        assert_archive_payloads(actual, original)

    finally:
        actual.close()
        original.close()
    before = result["provider_activity"]["attempts"]
    cached = prepare(args)
    assert cached["cache_state"] == "completed_dataset"
    assert cached["provider_activity"]["attempts"] == before
    changed = manifest.settings.model_copy(
        update={"data": manifest.settings.data.model_copy(update={"max_age_seconds": 4000})}
    )
    (tmp_path / "changed.json").write_text(changed.model_dump_json())
    args = SimpleNamespace(
        strategy=strategy,
        output=directory,
        env_file=None,
        _backtest_inputs=archive_selection(name, changed),
    )
    with pytest.raises(ValueError, match="resume cannot change"):
        prepare(args)
