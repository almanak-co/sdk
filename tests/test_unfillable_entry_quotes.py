"""An entry quote the market cannot fill never fails the observation an exit needs.

Entries repeat while funding inventory remains (blueprint 27 section 7), so the entry
quote is measured beside exit and rebalance quotes. A reverting Quoter, a pool without
liquidity for the input or a modeled swap beyond prepared tick coverage leaves the
entry quote absent with `entry_quote_error`; every other failure still fails. Exit
observations and teardown measure no entry quote at all.
"""

from types import SimpleNamespace

import pytest

from almanak.connectors import dex_residual
from almanak.connectors.dex_residual import QuoteUnfillable, entry_or_reason
from almanak.connectors.uniswap_v3.observations import Snapshot as V3Snapshot
from almanak.connectors.uniswap_v4 import economic_model as v4_economic
from almanak.connectors.uniswap_v4.backtest import transition_effects
from almanak.connectors.uniswap_v4.observations import quotes_for as v4_quotes_for
from almanak.connectors.uniswap_v4.state import Snapshot as V4Snapshot
from almanak.connectors.v3_pool_observations import quotes_for as v3_quotes_for
from almanak.dex_intents import ExitExchange
from almanak.gateway.ethereum import DataError, Reverted
from almanak.models import AssetAmount
from tests import test_uniswap_v4_exit_residual as v4_live
from tests import test_v3_pool_exit_residual as v3_live
from tests.exchange_dust import holding
from tests.test_uniswap_v3_economic_model import component, execute
from tests.uniswap_economic_support import prepared

COVERAGE = "entry quote unavailable: swap requires liquidity outside prepared tick coverage"
REVERTED = "entry quote unavailable: the quote call reverted"


def failing_entry(funding, error, measured):
    """A quote that fails for the funding asset only and records every requested input."""
    requested = []

    def quote(amount):
        requested.append(amount.asset)
        if amount.asset == funding:
            raise error
        return measured(amount)

    return quote, requested


@pytest.mark.parametrize(
    ("error", "reason"),
    [
        (Reverted("eth_call: RPC rejected the requested read (JSON-RPC 3)", b""), REVERTED),
        (
            DataError("liquidity", "V3 Quoter returned no output for requested input"),
            "entry quote unavailable: V3 Quoter returned no output for requested input",
        ),
        (QuoteUnfillable("swap requires liquidity outside prepared tick coverage"), COVERAGE),
    ],
)
def test_an_unfillable_entry_quote_is_recorded_with_its_reason(error, reason):
    def measure():
        raise error

    assert entry_or_reason(measure) == {"entry_quote_error": reason}


@pytest.mark.parametrize(
    "error",
    [
        DataError("transport", "eth_call: transport failed"),
        DataError("rpc", "eth_call: RPC rejected the requested read"),
        DataError("unavailable", "eth_call: requested data is unavailable"),
        ValueError("swap requires unprepared initialized tick liquidity"),
    ],
)
def test_provider_and_validation_failures_still_fail_the_entry_quote(error):
    def measure():
        raise error

    with pytest.raises(type(error)):
        entry_or_reason(measure)


def test_empty_reserves_are_unfillable_only_as_the_existing_value_error():
    quote = SimpleNamespace(amount_out=SimpleNamespace(amount=0))
    with pytest.raises(QuoteUnfillable) as raised:
        dex_residual.require_liquidity(quote, (0, 5))
    assert isinstance(raised.value, ValueError)


def v3_snapshot(quotes):
    raw = v3_live.CLOSED.model_dump()
    raw.update(entry_quote=None, exit_quote=None)
    raw.update(
        {
            key: value if isinstance(value, str) else value.model_dump()
            for key, value in quotes.items()
        }
    )
    return V3Snapshot.model_validate(raw)


@pytest.mark.parametrize(
    "error",
    [
        DataError("liquidity", "V3 Quoter returned no output for requested input"),
        Reverted("eth_call: RPC rejected the requested read (JSON-RPC 3)", b""),
    ],
)
def test_v3_live_entry_failure_keeps_the_exit_quote_and_a_valid_snapshot(error):
    owned = v3_live.CLOSED.owned
    target = owned.target
    quote, requested = failing_entry(
        target.funding_currency, error, lambda a: v3_live.measured(a, output=1)
    )
    result = v3_quotes_for(v3_live.CONFIG, owned, quote)
    assert set(result) == {"exit_quote", "entry_quote_error"}
    assert result["exit_quote"].amount_in.asset == target.counter_currency
    assert result["entry_quote_error"].startswith("entry quote unavailable: ")
    observed = v3_snapshot(result)
    assert observed.entry_quote is None and observed.exit_quote == result["exit_quote"]
    assert observed.model_dump(mode="json")["entry_quote_error"] == result["entry_quote_error"]
    assert "entry_quote_error" not in v3_live.CLOSED.model_dump(mode="json")
    with pytest.raises(ValueError, match="records as unmeasured"):
        v3_snapshot({**result, "entry_quote": v3_live.measured(requested_entry(owned), output=1)})
    # An exit observation does not ask for the entry quote at all.
    requested.clear()
    assert set(v3_quotes_for(v3_live.CONFIG, owned, quote, entry=False)) == {"exit_quote"}
    assert target.funding_currency not in requested


def requested_entry(owned):
    funding = next(
        v for v in (owned.inventory0, owned.inventory1) if v.asset == owned.target.funding_currency
    )
    return AssetAmount(asset=funding.asset, amount=1)


def v3_economic():
    model, state = component(0)
    target = model.config.target(model.config.allocations[0].name)
    state = holding(model, state, {target.counter_currency: 10**15})
    real = model.simulation.quote_at
    requested = []

    def quote_at(record, quoted, amount, time, *, entry_size=None):
        requested.append(amount.asset)
        if entry_size is not None and amount.asset == target.funding_currency:
            amount = amount.model_copy(update={"amount": entry_size})
        return real(record, quoted, amount, time)

    return model, state, target, quote_at, requested


def test_v3_economic_entry_beyond_prepared_coverage_keeps_the_exit_quote():
    model, state, target, quote_at, _ = v3_economic()
    # An entry slice larger than the prepared ticks can fill, as after a tick drift.
    model.simulation = SimpleNamespace(
        quote_at=lambda *args: quote_at(*args, entry_size=2**126),
        simulate=model.simulation.simulate,
    )
    observed = model.observe(state)[0]
    sample = model.book.at(target, state.time).payload
    assert observed.entry_quote is None
    # The recorded reason names the coverage the swap left and what stayed unfilled.
    assert observed.entry_quote_error.startswith(
        f"{COVERAGE} [{sample.lower}, {sample.upper}]: the price moved from tick "
    )
    assert f"of {2**126} input base units unfilled" in observed.entry_quote_error
    assert observed.exit_quote is not None
    assert observed.exit_quote.amount_in.asset == target.counter_currency
    # The exit is still decided and executed from that observation.
    state, result = execute(model, state, ExitExchange[type(target)](target=target))
    assert result.result.success


def test_v3_economic_exit_and_teardown_observations_measure_no_entry_quote():
    model, state, target, quote_at, requested = v3_economic()
    model.simulation = SimpleNamespace(quote_at=quote_at)
    running = model.observe(state)[0]
    assert running.entry_quote is not None and target.funding_currency in requested
    requested.clear()
    teardown = model.observe(state.model_copy(update={"mode": "teardown"}))[0]
    assert teardown.entry_quote is None and teardown.entry_quote_error is None
    assert teardown.exit_quote is not None and target.funding_currency not in requested
    requested.clear()
    payload = state.portfolio.positions[0].payload
    exiting = model.snapshot(state, payload, state.time, entry=False)
    assert exiting.entry_quote is None and exiting.exit_quote is not None
    assert target.funding_currency not in requested


def v4_snapshot(quotes):
    raw = v4_live.with_counter(v4_live.BURNED.after, 10**15).model_dump()
    raw.update(entry_quote=None, exit_quote=None)
    raw.update(
        {
            key: value if isinstance(value, str) else value.model_dump()
            for key, value in quotes.items()
        }
    )
    return V4Snapshot.model_validate(raw)


def test_v4_live_lp_entry_revert_keeps_the_exit_quote_and_skips_the_split_on_exit():
    observed = v4_live.with_counter(v4_live.BURNED.after, 10**15)
    owned, target = observed.owned, observed.target
    assert target.liquidity_enabled and owned.position is None
    error = Reverted("eth_call: RPC rejected the requested read (JSON-RPC 3)", b"")
    quote, requested = failing_entry(
        target.funding_currency, error, lambda a: v4_live.measured(a, output=1)
    )
    result = v4_quotes_for(v4_live.STATE.config, owned, observed.pool, quote)
    assert set(result) == {"exit_quote", "entry_quote_error"}
    assert result["entry_quote_error"] == REVERTED
    snapshot = v4_snapshot(result)
    assert snapshot.entry_quote is None and snapshot.exit_quote == result["exit_quote"]
    with pytest.raises(ValueError, match="records as unmeasured"):
        v4_snapshot({**result, "entry_quote": v4_live.measured(requested_entry(owned), output=1)})
    # Without the entry quote no LP entry split searches the Quoter.
    requested.clear()
    exit_only = v4_quotes_for(v4_live.STATE.config, owned, observed.pool, quote, entry=False)
    assert set(exit_only) == {"exit_quote"} and requested == [target.counter_currency]


def v4_economic_state(tmp_path, monkeypatch, *, entry_size=None):
    _, expected, model = prepared(tmp_path)
    target = model.config.target(model.config.allocations[0].name)
    held = expected.portfolio.positions[0]
    counter = 10**15
    field = "inventory0" if target.assets[0] == target.counter_currency else "inventory1"
    owned = held.payload.owned.model_copy(
        update={field: AssetAmount(asset=target.counter_currency, amount=counter)}
    )
    payload = held.payload.model_copy(update={"owned": owned})
    deltas = tuple(counter if asset == target.counter_currency else 0 for asset in target.assets)
    native = model.config.allocations[0].native_asset
    effects = transition_effects(model.settings, held, payload, deltas, native, 0)
    state = expected.model_copy(
        update={"mode": "running", "portfolio": expected.portfolio.apply(effects)}
    )
    real, requested = v4_economic.quote_at, []

    def quote_at(record, quoted, amount, time):
        requested.append(amount.asset)
        if entry_size is not None and amount.asset == target.funding_currency:
            amount = amount.model_copy(update={"amount": entry_size})
        return real(record, quoted, amount, time)

    monkeypatch.setattr(v4_economic, "quote_at", quote_at)
    return model, state, target, requested


def test_v4_economic_entry_beyond_prepared_coverage_keeps_the_exit_quote(tmp_path, monkeypatch):
    model, state, target, _ = v4_economic_state(tmp_path, monkeypatch, entry_size=2**126)
    try:
        observed = model.observe(state)[0]
        assert observed.entry_quote is None
        assert observed.entry_quote_error.startswith(f"{COVERAGE} [")
        assert f"of {2**126} input base units unfilled" in observed.entry_quote_error
        assert observed.exit_quote is not None
        assert observed.exit_quote.amount.asset == target.counter_currency
    finally:
        model.book.close()


def test_v4_economic_exit_and_teardown_observations_measure_no_entry_quote(tmp_path, monkeypatch):
    model, state, target, requested = v4_economic_state(tmp_path, monkeypatch)
    try:
        running = model.observe(state)[0]
        assert running.entry_quote is not None and target.funding_currency in requested
        requested.clear()
        teardown = model.observe(state.model_copy(update={"mode": "teardown"}))[0]
        assert teardown.entry_quote is None and teardown.entry_quote_error is None
        assert teardown.exit_quote is not None and target.funding_currency not in requested
        requested.clear()
        payload = state.portfolio.positions[0].payload
        exiting = model.snapshot(state, payload, state.time, entry=False)
        assert exiting.entry_quote is None and exiting.exit_quote is not None
        assert target.funding_currency not in requested
    finally:
        model.book.close()
