"""V3-family counter dust whose exact-size exit quote returns nothing is a measured residual.

Built on the committed Uniswap V3 operation capture: the observation after its
close leaves WETH counter inventory and live allowances, which these cases shrink
to dust. Pancake and Slipstream share the family observation and closure code.
"""

import gzip
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak.connectors import dex_residual
from almanak.connectors.aerodrome_slipstream import reader as slipstream_reader
from almanak.connectors.concentrated_math import sqrt_at_tick
from almanak.connectors.dex_reporting import residual_report
from almanak.connectors.pancakeswap_v3 import reader as pancake_reader
from almanak.connectors.uniswap_v3 import reader as uniswap_reader
from almanak.connectors.uniswap_v3.api import Actions
from almanak.connectors.uniswap_v3.authority import declared
from almanak.connectors.uniswap_v3.models import Parameters
from almanak.connectors.uniswap_v3.observations import Snapshot
from almanak.connectors.uniswap_v3.planning import operation
from almanak.connectors.uniswap_v3.positions import Quote
from almanak.connectors.v3_pool_lifecycle import has_obligations, validate_closure
from almanak.connectors.v3_pool_observations import quotes_for
from almanak.connectors.v3_pool_reader import consumed_quote
from almanak.connectors.v3_pool_state import exit_complete
from almanak.dex_intents import ExitExchange
from almanak.gateway.ethereum import DataError
from almanak.models import AssetAmount
from tests.exchange_scope import without_exchange_budgets

CAPTURE = json.loads(
    gzip.decompress(
        (Path(__file__).with_name("fixtures") / "uniswap-v3-operations.json.gz").read_bytes()
    )
)[0]
CONFIG = Parameters.model_validate_json(json.dumps(without_exchange_budgets(CAPTURE["config"])))
CLOSED = Snapshot.model_validate_json(
    json.dumps(
        next(
            s["before"]
            for s in CAPTURE["steps"]
            if s["operation"]["kind"] == "approve" and s["before"]["owned"]["position"] is None
            if s["before"]["exit_quote"] is not None
            and s["before"]["owned"]["inventory0"]["amount"] != 50000000
        )
    )
)
TARGET = CLOSED.target
NAME = TARGET.allocation
DUST = 23_270


def dust(amount=DUST):
    return AssetAmount(asset=TARGET.counter_currency, amount=amount)


def measured(amount, output=0):
    other = next(asset for asset in TARGET.assets if asset != amount.asset)
    return Quote(
        source=TARGET,
        block=CLOSED.block,
        amount_in=amount,
        amount_out=AssetAmount(asset=other, amount=output),
        sqrt_price_after=CLOSED.pool.sqrt_price_x96,
        initialized_ticks_crossed=0,
        calculation="contract_quoter",
    )


def observed(amount=DUST, *, cleanup=True, **update):
    raw = CLOSED.model_dump()
    field = "inventory0" if TARGET.assets[0] == TARGET.counter_currency else "inventory1"
    raw["owned"][field] = dust(amount).model_dump()
    raw["exit_quote"] = None
    if not cleanup:
        raw["allowances"] = tuple({**a, "amount": 0} for a in raw["allowances"])
    raw.update(update)
    return Snapshot.model_validate(raw)


def residual(**update):
    return observed(exit_residual=measured(dust()).model_dump(), **update)


def consumed(result, amount):
    return consumed_quote(result, TARGET.pool, amount, "V3")


@pytest.mark.parametrize("index", [0, 1])
def test_quoter_zero_output_counts_only_when_the_price_stayed_inside_the_limit(index):
    amount = AssetAmount(asset=TARGET.assets[index], amount=DUST)
    zero_for_one = amount.asset == TARGET.pool.currency0
    limit = sqrt_at_tick(-887272) + 1 if zero_for_one else sqrt_at_tick(887272) - 1
    inside = (0, CLOSED.pool.sqrt_price_x96, 0, 1)
    assert consumed(inside, amount) == inside
    with pytest.raises(DataError, match="Quoter returned no output"):
        consumed((0, limit, 3, 1), amount)
    assert consumed((5, limit, 3, 1), amount) == (5, limit, 3, 1)


@pytest.mark.parametrize("module", [uniswap_reader, pancake_reader, slipstream_reader])
def test_each_family_reader_admits_a_consumed_zero_output(module, monkeypatch):
    for name in ("verify_pool", "verify_periphery", "require_no_discount"):
        if hasattr(module, name):
            monkeypatch.setattr(module, name, lambda *args, **kwargs: None)
    source = SimpleNamespace(
        currency0=TARGET.pool.currency0,
        currency1=TARGET.pool.currency1,
        fee=500,
        tick_spacing=10,
    )
    amount = AssetAmount(asset=source.currency0, amount=DUST)
    limit = sqrt_at_tick(-887272) + 1

    def reader(result):
        return SimpleNamespace(call=lambda *args, **kwargs: result)

    inside = (0, CLOSED.pool.sqrt_price_x96, 0, 1)
    assert (
        module.quote(
            reader(inside), source, TARGET.contracts, CLOSED.block, amount, source.currency1
        )
        == inside
    )
    with pytest.raises(DataError, match="no output"):
        module.quote(
            reader((0, limit, 0, 1)),
            source,
            TARGET.contracts,
            CLOSED.block,
            amount,
            source.currency1,
        )


def test_zero_output_exit_is_retained_as_residual_and_zero_entries_are_absent():
    owned = observed().owned
    result = quotes_for(CONFIG, owned, measured)
    assert set(result) == {"exit_residual"} and result["exit_residual"].amount_in == dust()
    # Funding inventory remains after the exit, so the entry slice is offered again
    # beside the exit quote (blueprint 27 section 7).
    priced = quotes_for(CONFIG, owned, lambda a: measured(a, output=1))
    assert set(priced) == {"entry_quote", "exit_quote"}
    fresh = CLOSED.owned.model_copy(update={"entry_complete": False, "position": None})
    assert "entry_quote" not in quotes_for(CONFIG, fresh, measured)


def test_snapshots_admit_zero_output_only_as_the_whole_closed_counter_residual():
    assert residual().residuals == (residual().exit_residual,)
    assert "exit_residual" not in CLOSED.model_dump(mode="json")
    zero = measured(dust()).model_dump()
    with pytest.raises(ValueError, match="positive measured output"):
        observed(exit_quote=zero)
    with pytest.raises(ValueError, match="all closed counter inventory"):
        observed(DUST + 1, exit_residual=zero)
    with pytest.raises(ValueError, match="all closed counter inventory"):
        observed(exit_residual=measured(dust(), output=1).model_dump())


def test_exit_completion_planning_and_actions_follow_the_measured_residual():
    state = SimpleNamespace(capabilities=declared(CONFIG))
    exit_intent = ExitExchange[type(TARGET)](target=TARGET)
    with_cleanup = residual()
    assert not exit_complete(with_cleanup)
    step = operation(CONFIG, state, exit_intent, with_cleanup)
    assert step.kind == "approve" and step.amount == 0
    settled = residual(cleanup=False)
    assert exit_complete(settled) and not has_obligations(settled.owned, settled.exit_residual)
    with pytest.raises(ValueError, match="no remaining leg"):
        operation(CONFIG, state, exit_intent, settled)
    unmeasured = observed(cleanup=False)
    assert not exit_complete(unmeasured) and has_obligations(unmeasured.owned)
    with pytest.raises(ValueError, match="explicit exit quote"):
        operation(CONFIG, state, exit_intent, unmeasured)
    context = SimpleNamespace(config=CONFIG, require_observation=lambda name: settled)
    assert Actions(context).exit(NAME) is None
    context.require_observation = lambda name: unmeasured
    assert Actions(context).exit(NAME) == exit_intent


def test_closure_accepts_only_counter_dust_measured_in_the_closure_observation():
    settled = residual(cleanup=False)
    state = SimpleNamespace(config=CONFIG, owned=(settled.owned,))
    validate_closure(state, (settled,))
    with pytest.raises(ValueError, match="unmeasured non-funding inventory"):
        validate_closure(state, (observed(cleanup=False),))


def test_leg_measurement_and_reports_retain_only_measured_residuals():
    owned = observed().owned

    def unavailable(amount):
        raise DataError("unavailable", "provider timeout")

    assert dex_residual.residual_measurement(owned, unavailable) == {}
    assert dex_residual.residual_measurement(owned, lambda a: measured(a, output=1)) == {}
    assert dex_residual.residual_measurement(owned, measured)["exit_residual"].amount_in == dust()
    assert residual_report(None) == {} and residual_report(observed()) == {}
    reported = residual_report(residual())["exit_residuals"]
    assert [r["amount_in"] for r in reported] == [dust().model_dump(mode="json")]
