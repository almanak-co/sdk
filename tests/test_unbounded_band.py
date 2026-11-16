"""A liquidity band without an envelope recentres around the price for the whole deployment."""

import json
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from jsonschema import Draft202012Validator

from almanak.connectors.band_fee_growth import (
    CoverageExceeded,
    validate_band_coverage,
    validate_covered,
    validate_envelope,
)
from almanak.connectors.uniswap_v3.api import Actions
from almanak.connectors.uniswap_v3.authority import admit, declared
from almanak.connectors.uniswap_v3.domain import TickRange
from almanak.connectors.uniswap_v4 import planning as v4_planning
from almanak.connectors.uniswap_v4.models import Range
from almanak.connectors.uniswap_v4.validation import mint_limits
from almanak.connectors.v3_pool_author_inputs import BandInput, width_from_bps
from almanak.connectors.v3_pool_authority import admit_centered
from almanak.connectors.v3_pool_domain import MAX_TICK, TickBand
from almanak.dex_intents import TickBounds
from tests import test_banded_liquidity as v3
from tests import test_uniswap_v4_banded_liquidity as v4
from tests.test_dx_schemas import SCHEMAS

WIDTH = 1000
FAR = 200_000


def test_unbounded_band_centres_at_any_price_and_clamps_only_at_protocol_ticks():
    band = TickBand(width=WIDTH)
    assert not band.bounded
    for tick in (v3.TICK, v3.TICK + FAR, v3.TICK - FAR):
        selected = band.centered(tick, v3.SPACING)
        assert selected.upper - selected.lower == WIDTH
        assert selected.lower <= tick < selected.upper
        assert band.validate_range(v3.bounds(selected), v3.SPACING) == selected
    edge = MAX_TICK // v3.SPACING * v3.SPACING
    assert band.centered(MAX_TICK, v3.SPACING) == TickRange(lower=edge - WIDTH, upper=edge)
    assert band.centered(-MAX_TICK, v3.SPACING) == TickRange(lower=-edge, upper=-edge + WIDTH)
    centred = band.centered(v3.TICK, v3.SPACING)
    for lower, upper in (
        (centred.lower + 1, centred.upper + 1),
        (centred.lower, centred.upper + v3.SPACING),
    ):
        with pytest.raises(ValueError, match="tick spacing|band width"):
            band.validate_range(TickBounds(lower=lower, upper=upper), v3.SPACING)


def test_band_envelope_is_both_bounds_or_neither_and_the_width_fits_the_pool():
    with pytest.raises(ValueError, match="both lower and upper"):
        TickBand(lower=-1000, width=WIDTH)
    with pytest.raises(ValueError, match="both lower and upper"):
        BandInput(upper=1000, width_bps=500)
    with pytest.raises(ValueError, match="usable tick range"):
        TickBand(width=1_774_560).validate_pool(SimpleNamespace(tick_spacing=10))
    assert TickBand(width=WIDTH).model_dump(mode="json") == {"width": WIDTH}
    assert BandInput(width_bps=500).resolve(10) == {"width": width_from_bps(500, 10)}
    assert BandInput(lower=-1000, upper=1000, width=200).resolve(10) == {
        "lower": -1000,
        "upper": 1000,
        "width": 200,
    }


def test_allocation_without_an_envelope_validates_for_v3_and_v4():
    v3_target = v3.band_parameters(width=WIDTH).target("position")
    v4_target = v4.band_config(width=WIDTH).target(v4.NAME)
    assert v3_target.band == v4_target.band == TickBand(width=WIDTH)
    assert "lower" not in v3_target.model_dump(mode="json")["band"]


def test_open_liquidity_action_follows_the_price_far_from_any_former_envelope():
    config = v3.band_parameters(width=WIDTH)
    owned = v3.owned_for(config, 50 * 10**6, 2 * 10**16, entry_complete=True)
    tick = v3.TICK + FAR
    observed = SimpleNamespace(
        target=owned.target,
        owned=owned,
        pool=SimpleNamespace(sqrt_price_x96=v3.pool().sqrt_price_x96, tick=tick),
        block=v3.block(),
        effective_at=datetime.fromtimestamp(v3.block().timestamp, UTC),
    )
    context = SimpleNamespace(require_observation=lambda name: observed, config=config)
    intent = Actions(context).open_liquidity("position")
    selected = TickBand(width=WIDTH).centered(tick, v3.SPACING)
    assert intent.range == v3.bounds(selected)
    admit(declared(config), intent)
    admit_centered(intent, tick)


def test_gateway_admits_an_unbounded_mint_only_centred_on_the_decision_tick():
    config = v3.band_parameters(width=WIDTH)
    owned = v3.owned_for(config, 50 * 10**6, 2 * 10**16, entry_complete=True)
    centred = TickBand(width=WIDTH).centered(v3.TICK, v3.SPACING)
    shifted = TickRange(lower=centred.lower + v3.SPACING, upper=centred.upper + v3.SPACING)
    admit_centered(v3.open_intent(config, owned, centred), v3.TICK)
    with pytest.raises(ValueError, match="centred on the decision tick"):
        admit_centered(v3.open_intent(config, owned, shifted), v3.TICK)
    # An envelope remains the author's bound: any aligned range inside it is admitted.
    bounded = v3.band_parameters()
    owned = v3.owned_for(bounded, 50 * 10**6, 2 * 10**16, entry_complete=True)
    admit_centered(v3.open_intent(bounded, owned, v3.SHIFTED), v3.TICK)


def v4_mint_state(config, owned):
    decision = SimpleNamespace(target=owned.target, pool=v4.POOL)
    return SimpleNamespace(decision=SimpleNamespace(observations=(decision,)))


def test_v4_mint_limits_refuse_an_off_centre_unbounded_band_range():
    config = v4.band_config(width=WIDTH)
    owned = v4.owned_for(config, 50 * 10**6, 2 * 10**16, entry_complete=True)
    allocation = config.allocation(v4.NAME)
    centred = TickBand(width=WIDTH).centered(v4.POOL.tick, v4.SPACING)
    centred = Range(lower=centred.lower, upper=centred.upper)
    shifted = Range(lower=centred.lower + v4.SPACING, upper=centred.upper + v4.SPACING)
    before = v4.before_for(owned)
    state = v4_mint_state(config, owned)
    mint_limits(state, v4.open_intent(owned, centred), before, allocation)
    operation = v4_planning.operation(config, None, v4.open_intent(owned, shifted), before)
    assert operation.range == shifted
    with pytest.raises(ValueError, match="centred on the decision tick"):
        mint_limits(state, v4.open_intent(owned, shifted), before, allocation)
    bounded = v4.band_config()
    owned = v4.owned_for(bounded, 50 * 10**6, 2 * 10**16, entry_complete=True)
    mint_limits(
        v4_mint_state(bounded, owned),
        v4.open_intent(owned, v4.SHIFTED),
        v4.before_for(owned),
        bounded.allocation(v4.NAME),
    )


def test_unbounded_band_coverage_holds_one_aligned_width_and_every_opened_position():
    band = TickBand(width=WIDTH)
    validate_envelope(band, -WIDTH // 2, WIDTH // 2, 10)
    validate_envelope(band, -MAX_TICK, MAX_TICK, 10)
    with pytest.raises(ValueError, match="narrower than one liquidity band width"):
        validate_envelope(band, 0, WIDTH - 10, 10)
    # Wide enough in ticks, but no aligned position of the width fits inside it.
    with pytest.raises(ValueError, match="narrower than one liquidity band width"):
        validate_envelope(band, -995, 5, 10)
    with pytest.raises(ValueError, match="inside explicit prepared tick coverage"):
        validate_envelope(TickBand(lower=-2000, upper=2000, width=WIDTH), -1000, 1000, 10)
    validate_covered(-2000, 2000, TickRange(lower=-1000, upper=0))
    with pytest.raises(CoverageExceeded, match="widen data.coverage"):
        validate_covered(-2000, 2000, TickRange(lower=1500, upper=2500))
    assert not issubclass(CoverageExceeded, ValueError)


def test_book_open_refuses_coverage_away_from_the_first_unbounded_position():
    def sample(tick):
        return SimpleNamespace(lower=-2000, upper=2000, pool=SimpleNamespace(tick=tick))

    validate_band_coverage(TickBand(width=WIDTH), sample(0), 10)
    with pytest.raises(CoverageExceeded):
        validate_band_coverage(TickBand(width=WIDTH), sample(5000), 10)
    # An envelope inside the coverage is checked as before, wherever the price starts.
    validate_band_coverage(TickBand(lower=-1000, upper=1000, width=WIDTH), sample(5000), 10)


@pytest.mark.parametrize(
    "connector", ["uniswap_v3", "uniswap_v4", "pancakeswap_v3", "aerodrome_slipstream"]
)
def test_editor_schema_accepts_an_unbounded_band_and_refuses_half_an_envelope(connector):
    document = json.loads((SCHEMAS / f"positions/{connector}.schema.json").read_text())
    checker = Draft202012Validator({**document["$defs"]["Band"], "$defs": document["$defs"]})
    assert not list(checker.iter_errors({"width_bps": 500}))
    assert not list(checker.iter_errors({"lower": -1000, "upper": 1000, "width": 200}))
    assert list(checker.iter_errors({"lower": -1000, "width": 200}))
    assert list(checker.iter_errors({"lower": -1000, "upper": 1000}))


def v3_open_case(*, simulated):
    """A V3 band open with its decision and current observations at the captured tick."""
    config = v3.band_parameters(width=WIDTH)
    owned = v3.owned_for(config, 50 * 10**6, 2 * 10**16, entry_complete=True)
    time = v3.block().timestamp
    observation = SimpleNamespace(
        target=owned.target,
        owned=owned,
        position=owned.position,
        wallet0=owned.inventory0,
        wallet1=owned.inventory1,
        simulated=simulated,
        pool=v3.pool(),
        block=v3.block(),
        native_balance=SimpleNamespace(amount=10**18),
        effective_at=datetime.fromtimestamp(time, UTC),
    )
    centred = TickBand(width=WIDTH).centered(v3.TICK, v3.SPACING)
    shifted = TickRange(lower=centred.lower + v3.SPACING, upper=centred.upper + v3.SPACING)
    return config, owned, observation, centred, shifted


def test_live_v3_validation_refuses_an_off_centre_unbounded_open():
    from almanak.connectors.v3_pool_validation import validate_operation

    config, owned, before, centred, shifted = v3_open_case(simulated=False)
    operation = SimpleNamespace(target=owned.target, allocation="position")

    def check(selected):
        intent = v3.open_intent(config, owned, selected)
        state = SimpleNamespace(
            decision=SimpleNamespace(intent=intent, observations=(before,)),
            capabilities=declared(config),
            config=config,
            owned=(owned,),
            mode="running",
            outcomes=(),
        )
        validate_operation(None, state, operation, before, lambda *args: operation)

    check(centred)
    with pytest.raises(ValueError, match="centred on the decision tick"):
        check(shifted)


def test_economic_v3_admission_refuses_an_off_centre_unbounded_open():
    from almanak.connectors.v3_economic_validation import validate

    config, owned, observed, centred, shifted = v3_open_case(simulated=True)
    time = int(observed.effective_at.timestamp())

    def check(selected):
        intent = v3.open_intent(config, owned, selected)
        pending = SimpleNamespace(intent=intent, decision_time=time, eligible_at=time)
        validate(config, SimpleNamespace(mode="running"), pending, observed, observed)

    check(centred)
    with pytest.raises(ValueError, match="centred on the decision tick"):
        check(shifted)
