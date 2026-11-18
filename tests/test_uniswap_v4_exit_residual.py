"""Counter dust whose exact-size exit quote returns nothing is a measured residual.

Built on the committed Ethereum V4 LP lifecycle: its burn leaves WETH counter
inventory, which these cases shrink to dust. A zero-output quote is retained as
`exit_residual`, never offered as a swap, and lets exits and closure complete.
"""

from types import SimpleNamespace

import pytest

from almanak.connectors.uniswap_v4 import backtest, reader
from almanak.connectors.uniswap_v4.api import Actions
from almanak.connectors.uniswap_v4.exit import cleared, leg, obligations
from almanak.connectors.uniswap_v4.lifecycle import validate_closure
from almanak.connectors.uniswap_v4.models import Target
from almanak.connectors.uniswap_v4.observations import quotes_for, residual_measurement
from almanak.connectors.uniswap_v4.reader import Quote
from almanak.connectors.uniswap_v4.state import Snapshot
from almanak.dex_intents import ExitExchange
from almanak.gateway.ethereum import DataError
from almanak.models import AssetAmount
from tests.uniswap_support import live_state

STATE = live_state("lp")
BURNED = next(o for o in STATE.outcomes if o.operation.kind == "burn")
CLOSED = STATE.closure[0]
NAME = STATE.config.allocations[0].name
TARGET = STATE.config.target(NAME)
DUST = 23_270


def measured(amount, output=0, block=None):
    other = next(a for a in TARGET.assets if a != amount.asset)
    return Quote(
        block=block or BURNED.after.block,
        amount=amount,
        output=AssetAmount(asset=other, amount=output),
        quoter_gas=1,
    )


def with_counter(observed, amount, **update):
    owned = observed.owned
    counter = AssetAmount(asset=TARGET.counter_currency, amount=amount)
    field = "inventory0" if TARGET.assets[0] == TARGET.counter_currency else "inventory1"
    wallet = "wallet0" if field == "inventory0" else "wallet1"
    raw = observed.model_dump()
    raw.update(owned=owned.model_copy(update={field: counter}).model_dump(), **update)
    raw[wallet] = AssetAmount(asset=counter.asset, amount=max(amount, 0)).model_dump()
    return Snapshot.model_validate(raw)


def dust():
    return AssetAmount(asset=TARGET.counter_currency, amount=DUST)


def residual_after(observed=BURNED.after):
    return with_counter(
        observed, DUST, exit_residual=measured(dust(), block=observed.block).model_dump()
    )


def test_quoter_zero_output_is_a_measurement_and_overflow_stays_an_error():
    calls = []

    def call(result):
        def respond(*args, **kwargs):
            calls.append(args)
            return result, 21_000

        return SimpleNamespace(call=respond)

    quote = reader.quote(call(0), TARGET, dust(), BURNED.after.block)
    assert quote.output.amount == 0 and quote.amount == dust() and quote.quoter_gas == 21_000
    with pytest.raises(DataError, match="exceeds uint128"):
        reader.quote(call(2**128), TARGET, dust(), BURNED.after.block)
    assert len(calls) == 2


def test_zero_output_exit_is_retained_as_residual_and_other_zero_quotes_are_absent():
    owned = residual_after().owned
    result = quotes_for(STATE.config, owned, BURNED.after.pool, lambda amount: measured(amount))
    assert set(result) == {"exit_residual"}
    assert result["exit_residual"].amount == dust()
    positive = quotes_for(
        STATE.config, owned, BURNED.after.pool, lambda amount: measured(amount, output=1)
    )
    # The burn returned funding inventory, so a repeated entry is offered beside the exit.
    assert set(positive) == {"entry_quote", "exit_quote"}
    fresh = STATE.baseline[0].owned
    entry = quotes_for(STATE.config, fresh, STATE.baseline[0].pool, lambda a: measured(a))
    assert entry == {}


def test_snapshots_admit_zero_output_only_as_the_whole_closed_counter_residual():
    observed = residual_after()
    assert observed.residuals == (observed.exit_residual,)
    assert "exit_residual" not in BURNED.after.model_dump(mode="json")
    zero = measured(dust()).model_dump()
    with pytest.raises(ValueError, match="positive measured output"):
        with_counter(BURNED.after, DUST, exit_quote=zero)
    with pytest.raises(ValueError, match="all closed counter inventory"):
        with_counter(BURNED.after, DUST + 1, exit_residual=zero)
    with pytest.raises(ValueError, match="all closed counter inventory"):
        with_counter(BURNED.after, DUST, exit_residual=measured(dust(), output=1).model_dump())
    with pytest.raises(ValueError, match="all closed counter inventory"):
        with_counter(
            BURNED.after,
            DUST,
            exit_residual=zero,
            exit_quote=measured(dust(), output=1).model_dump(),
        )
    stale = BURNED.before.block
    with pytest.raises(ValueError, match="observed block and funding asset"):
        with_counter(BURNED.after, DUST, exit_residual=measured(dust(), block=stale).model_dump())
    funding = AssetAmount(asset=TARGET.funding_currency, amount=DUST)
    with pytest.raises(ValueError, match="observed block and funding asset"):
        with_counter(BURNED.after, DUST, exit_residual=measured(funding).model_dump())


def test_measured_residual_clears_the_exit_while_unmeasured_dust_remains_an_obligation():
    measured_dust = residual_after()
    unmeasured = with_counter(BURNED.after, DUST)
    assert measured_dust.needs_cleanup and unmeasured.needs_cleanup
    assert leg(STATE.config, measured_dust).kind == "revoke_exchange"
    with pytest.raises(ValueError):
        leg(STATE.config, unmeasured)
    revoked = STATE.outcomes[-1]
    assert revoked.after.owned.position is None and not revoked.after.needs_cleanup
    settled = residual_after(revoked.after)
    assert cleared(settled) and obligations(settled) == "clear"
    assert obligations(with_counter(revoked.after, DUST)) == "remaining"
    with pytest.raises(ValueError, match="already has verified economic closure"):
        leg(STATE.config, settled)
    context = SimpleNamespace(config=STATE.config, require_observation=lambda name: settled)
    assert Actions(context).exit(NAME) is None


def test_exit_outcome_completes_on_the_residual_its_own_leg_measured():
    revoked = STATE.outcomes[-1]
    exiting = revoked.decision.model_copy(update={"intent": ExitExchange[Target](target=TARGET)})
    measured_leg = revoked.model_copy(
        update={"decision": exiting, "after": residual_after(revoked.after)}
    )
    assert measured_leg.strategy_result().complete
    unmeasured_leg = revoked.model_copy(
        update={"decision": exiting, "after": with_counter(revoked.after, DUST)}
    )
    assert not unmeasured_leg.strategy_result().complete


def test_closure_accepts_only_counter_dust_measured_in_the_closure_observation():
    owned = residual_after(CLOSED).owned
    state = STATE.model_copy(update={"owned": (owned,)})
    validate_closure(state, (residual_after(CLOSED),))
    with pytest.raises(ValueError, match="unmeasured non-funding inventory"):
        validate_closure(state, (with_counter(CLOSED, DUST),))


def test_leg_measurement_keeps_unavailable_quotes_as_a_visible_obligation():
    owned = residual_after().owned

    def unavailable(amount):
        raise DataError("unavailable", "provider timeout")

    assert residual_measurement(owned, unavailable) == {}
    assert residual_measurement(owned, lambda a: measured(a, output=1)) == {}
    assert residual_measurement(owned, measured)["exit_residual"].amount == dust()
    assert residual_measurement(BURNED.before.owned, measured) == {}


def test_historical_exit_holding_only_residual_completes_without_a_step():
    model = object.__new__(backtest.UniswapModel)
    model.config = STATE.config
    used = {"a" * 64}
    model.histories = {TARGET.fund.chain: SimpleNamespace(reader=SimpleNamespace(used=used))}
    observed = residual_after(STATE.outcomes[-1].after)
    assert model.exit_settled(observed)
    assert not model.exit_settled(with_counter(STATE.outcomes[-1].after, DUST))
    before = SimpleNamespace(payload=SimpleNamespace(target=TARGET, owned=observed.owned))
    pending = SimpleNamespace(intent=ExitExchange[Target](target=TARGET), identity="dust-exit")
    transition = model.settle(before, pending, observed)
    result = transition.result
    assert transition.complete and result.complete and result.steps == ()
    assert result.after == observed.owned and result.before == observed
    assert result.gas_paid.amount == 0 and result.simulation.inputs == tuple(used)
    assert not transition.effects.assets and not transition.effects.positions
    state = SimpleNamespace(portfolio=SimpleNamespace(positions=(before,)))
    assert model.closed(state, (observed,))
    assert not model.closed(state, ())
