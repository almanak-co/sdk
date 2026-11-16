"""Band references wait outside the envelope and re-swap after a failed mint."""

import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak.connectors.v3_pool_domain import TickBand

ROOT = Path(__file__).parents[1]
BANDS = (
    "uniswap_v3_robinhood_lp_band",
    "uniswap_v4_robinhood_lp_band",
    "pancakeswap_v3_bnb_lp_band",
    "aerodrome_slipstream_base_lp_band",
)
BAND = TickBand(lower=-1000, upper=1000, width=200)


@pytest.fixture(params=BANDS)
def band(request):
    path = ROOT / "strategies" / request.param / "strategy.py"
    spec = importlib.util.spec_from_file_location(request.param, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def context(module, memory, tick, *, quote=True):
    observed = SimpleNamespace(
        pool=SimpleNamespace(tick=tick),
        target=SimpleNamespace(band=BAND, pool=SimpleNamespace(tick_spacing=10)),
        rebalance_quote=object() if quote else None,
    )
    actions = SimpleNamespace(
        swap=lambda name, rebalance: ("swap", name),
        open_liquidity=lambda name: ("open", name),
    )
    return SimpleNamespace(memory=memory, actions=actions), observed


def test_outside_the_envelope_no_position_opens(band):
    allocation = SimpleNamespace(name="position")
    for tick in (-1001, -1500, 1000, 1500):
        ctx, observed = context(band, band.State(), tick)
        assert band.enter_liquidity(ctx, allocation, observed) is None
    ctx, observed = context(band, band.State(), 990)
    assert band.enter_liquidity(ctx, allocation, observed) == ("swap", "position")
    rebalanced = band.State(rebalanced={"position": True})
    ctx, observed = context(band, rebalanced, -1000)
    assert band.enter_liquidity(ctx, allocation, observed) == ("open", "position")


def outcome(module, memory, kind, status):
    target = SimpleNamespace(allocation="position")
    result = SimpleNamespace(status=status, intent=SimpleNamespace(kind=kind, target=target))
    return module.on_outcome(SimpleNamespace(memory=memory, result=result))


def test_a_failed_open_clears_the_rebalance_flag(band):
    rebalanced = band.State(rebalanced={"position": True})
    assert outcome(band, rebalanced, "open_liquidity", "failed").rebalanced == {"position": False}
    assert outcome(band, rebalanced, "swap_exact_input", "failed") is rebalanced
    swapped = outcome(band, band.State(), "swap_exact_input", "completed")
    assert swapped.rebalanced == {"position": True}


def test_a_band_says_why_it_holds_or_recentres(band):
    allocation = SimpleNamespace(name="position", hold_seconds=None)
    position = SimpleNamespace(range=SimpleNamespace(lower=-100, upper=100))
    actions = SimpleNamespace(close_liquidity=lambda name: ("close", name))
    ctx = SimpleNamespace(memory=band.State(), teardown=False, actions=actions)

    def observed(tick):
        return SimpleNamespace(
            pool=SimpleNamespace(tick=tick), owned=SimpleNamespace(position=position)
        )

    assert band.decide_allocation(ctx, allocation, observed(0)) == (
        None,
        "Price inside the range, nothing to rebalance",
    )
    assert band.decide_allocation(ctx, allocation, observed(150)) == (
        ("close", "position"),
        "Price left the range, recentring",
    )
    ctx, outside = context(band, band.State(), 1500)
    outside.owned = SimpleNamespace(position=None)
    ctx.teardown = False
    intent, reason = band.decide_allocation(ctx, allocation, outside)
    assert intent is None and reason == "Price is past the band; waiting for it to return"
