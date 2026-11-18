"""Aerodrome and Curve economic fills against literal pool samples, without evidence.

Only attributed inventory bounds an input (blueprint 27); cumulative spending is
reported, never a limit. Pool math that needs prepared samples is stubbed where
the case is about ownership and inventory, not the invariant itself.
"""

from types import SimpleNamespace

import pytest

from almanak.asset_amounts import AssetAmounts
from almanak.caip import AssetType
from almanak.connectors.aerodrome import economic_simulation as aerodrome
from almanak.connectors.aerodrome.positions import Position as AerodromePosition
from almanak.connectors.curve import economic_simulation as curve
from almanak.connectors.curve.positions import Position as CurvePosition
from almanak.dex_intents import (
    IncreaseLiquidity,
    IncreasePoolLiquidity,
    OpenLiquidity,
    OpenPoolLiquidity,
    SwapExactInput,
)
from almanak.models import AssetAmount

A = AssetType("eip155:8453/erc20:0x" + "aa" * 20)
B = AssetType("eip155:8453/erc20:0x" + "bb" * 20)
C = AssetType("eip155:8453/erc20:0x" + "cc" * 20)
SHARES = AssetType("eip155:8453/erc20:0x" + "dd" * 20)


def value(asset, units):
    return AssetAmount(asset=asset, amount=units)


class Payload(SimpleNamespace):
    def model_copy(self, update):
        return Payload(**{**vars(self), **update})


def record_moves(monkeypatch, module):
    def moved(owned, deltas, **changes):
        return SimpleNamespace(source=owned, deltas=tuple(deltas), **changes)

    monkeypatch.setattr(module, "moved", moved)


# Aerodrome additions mint fungible shares at the sample's proportional price.

AERODROME = SimpleNamespace(pool=SimpleNamespace(shares_asset=SHARES), assets=(A, B))
SAMPLE = SimpleNamespace(
    payload=SimpleNamespace(
        pool=SimpleNamespace(reserve0=1000, reserve1=2000, total_supply=500, index0=11, index1=12)
    )
)


def aerodrome_payload(position=None, inventory=(1000, 1000)):
    owned = SimpleNamespace(
        position=position, inventory0=value(A, inventory[0]), inventory1=value(B, inventory[1])
    )
    return Payload(owned=owned, deposit0=5, deposit1=6)


def aerodrome_intent(kind, maximum0=100, maximum1=300):
    return kind.model_construct(
        target=AERODROME, maximum0=value(A, maximum0), maximum1=value(B, maximum1)
    )


@pytest.fixture
def aerodrome_model(monkeypatch):
    record_moves(monkeypatch, aerodrome)
    monkeypatch.setattr(aerodrome, "at_indices", lambda old, pool: old)


def test_aerodrome_open_mints_proportional_shares_and_reports_deposits(aerodrome_model):
    result, deltas, fees, shares, kind, quote = aerodrome.added(
        None, aerodrome_intent(OpenLiquidity), aerodrome_payload(), SAMPLE
    )
    # 100 of A prices 200 of B at the 1000:2000 reserves and mints 50 of 500 shares.
    assert (deltas, fees, shares, kind, quote) == ((-100, -200), (0, 0), 50, "mint", None)
    position = result.owned.position
    assert position == AerodromePosition(
        shares=value(SHARES, 50), fees0=value(A, 0), fees1=value(B, 0), index0=11, index1=12
    )
    assert result.owned.deltas == (-100, -200) and result.owned.entry_complete
    assert (result.deposit0, result.deposit1) == (105, 306)


def test_aerodrome_increase_adds_shares_and_keeps_accrued_fees(aerodrome_model):
    old = AerodromePosition(
        shares=value(SHARES, 50), fees0=value(A, 3), fees1=value(B, 4), index0=1, index1=2
    )
    result, deltas, _, shares, kind, _ = aerodrome.added(
        None, aerodrome_intent(IncreaseLiquidity, 300, 200), aerodrome_payload(old), SAMPLE
    )
    # The B budget binds: 200 of B prices 100 of A.
    assert (deltas, shares, kind) == ((-100, -200), 50, "increase")
    assert result.owned.position == AerodromePosition(
        shares=value(SHARES, 100), fees0=value(A, 3), fees1=value(B, 4), index0=11, index1=12
    )


def test_aerodrome_additions_refuse_lost_ownership_and_uncovered_inventory(aerodrome_model):
    old = AerodromePosition(
        shares=value(SHARES, 50), fees0=value(A, 0), fees1=value(B, 0), index0=1, index1=2
    )
    with pytest.raises(ValueError, match="retain hypothetical shares"):
        aerodrome.added(None, aerodrome_intent(OpenLiquidity), aerodrome_payload(old), SAMPLE)
    with pytest.raises(ValueError, match="retain hypothetical shares"):
        aerodrome.added(None, aerodrome_intent(IncreaseLiquidity), aerodrome_payload(), SAMPLE)
    with pytest.raises(ValueError, match="exceeds attributed inventory"):
        aerodrome.added(
            None, aerodrome_intent(OpenLiquidity), aerodrome_payload(inventory=(99, 1000)), SAMPLE
        )


# Curve swaps and additions on the virtual overlay.

CURVE = SimpleNamespace(
    pool=SimpleNamespace(shares_asset=SHARES), assets=(A, B, C), funding_asset=A
)
RECORD = SimpleNamespace(payload=SimpleNamespace(pool="sample"))


def curve_payload(position=None, *, entry_complete=False, held=1000):
    owned = SimpleNamespace(
        position=position, entry_complete=entry_complete, amount=lambda asset: held
    )
    spent = AssetAmounts((value(A, 1), value(B, 2), value(C, 3)))
    return Payload(owned=owned, spent=spent, target=CURVE)


@pytest.fixture
def curve_model(monkeypatch):
    record_moves(monkeypatch, curve)
    seen = SimpleNamespace(quotes=[], minted=[])

    def quote_at(record, owned, amount, output_asset, time):
        seen.quotes.append((record, amount, output_asset, time))
        return SimpleNamespace(amount_out=value(output_asset, 95))

    def minted(state, amounts):
        seen.minted.append((state, amounts))
        return 40

    monkeypatch.setattr(curve, "quote_at", quote_at)
    monkeypatch.setattr(curve, "virtual_pool", lambda pool, owned, time: ("virtual", pool, time))
    monkeypatch.setattr(curve.economic_pool, "minted", minted)
    return seen


def curve_swap(asset=A, output=B, units=100, minimum=90):
    return SwapExactInput.model_construct(
        target=CURVE, amount=value(asset, units), minimum_output=value(output, minimum)
    )


def test_curve_swap_fills_at_the_modeled_quote_and_completes_funding_entry(curve_model):
    intent = curve_swap()
    result, deltas, fees, shares, kind, quote = curve.swap_fill(
        None, intent, curve_payload(), RECORD, 77
    )
    assert curve_model.quotes == [(RECORD, intent.amount, B, 77)]
    assert (deltas, fees, shares, kind) == ((-100, 95, 0), (), 0, "swap")
    assert quote.amount_out == value(B, 95)
    assert result.owned.entry_complete is True
    assert result.spent == AssetAmounts((value(A, 101), value(B, 2), value(C, 3)))


def test_curve_swap_out_of_a_non_funding_coin_keeps_entry_state(curve_model):
    result, deltas, *_ = curve.swap_fill(
        None, curve_swap(asset=C, output=A), curve_payload(), RECORD, 77
    )
    assert deltas == (95, 0, -100)
    assert result.owned.entry_complete is False
    result, *_ = curve.swap_fill(
        None, curve_swap(asset=C, output=A), curve_payload(entry_complete=True), RECORD, 77
    )
    assert result.owned.entry_complete is True


def test_curve_swap_refuses_its_minimum_and_uncovered_inventory(curve_model):
    with pytest.raises(ValueError, match="violates its retained minimum"):
        curve.swap_fill(None, curve_swap(minimum=96), curve_payload(), RECORD, 77)
    with pytest.raises(ValueError, match="exceeds attributed inventory"):
        curve.swap_fill(None, curve_swap(units=1001), curve_payload(), RECORD, 77)


def curve_addition(kind, amounts=(10, 20, 0)):
    maximums = SimpleNamespace(
        root=tuple(value(asset, units) for asset, units in zip((A, B, C), amounts, strict=True))
    )
    return kind.model_construct(target=CURVE, maximums=maximums)


def test_curve_open_mints_on_the_virtual_pool(curve_model):
    result, deltas, fees, minted, kind, quote = curve.added(
        None, curve_addition(OpenPoolLiquidity), curve_payload(), RECORD, 77
    )
    assert curve_model.minted == [(("virtual", "sample", 77), (10, 20, 0))]
    assert (deltas, fees, minted, kind, quote) == ((-10, -20, 0), (), 40, "mint", None)
    assert result.owned.position == CurvePosition(shares=value(SHARES, 40))
    assert result.owned.entry_complete is True
    assert result.spent == AssetAmounts((value(A, 11), value(B, 22), value(C, 3)))


def test_curve_increase_adds_to_the_retained_shares(curve_model):
    old = CurvePosition(shares=value(SHARES, 60))
    result, _, _, minted, kind, _ = curve.added(
        None, curve_addition(IncreasePoolLiquidity), curve_payload(old), RECORD, 77
    )
    assert (minted, kind) == (40, "increase")
    assert result.owned.position == CurvePosition(shares=value(SHARES, 100))


def test_curve_additions_refuse_lost_ownership_and_uncovered_inventory(curve_model):
    old = CurvePosition(shares=value(SHARES, 60))
    with pytest.raises(ValueError, match="preserve hypothetical share ownership"):
        curve.added(None, curve_addition(OpenPoolLiquidity), curve_payload(old), RECORD, 77)
    with pytest.raises(ValueError, match="preserve hypothetical share ownership"):
        curve.added(None, curve_addition(IncreasePoolLiquidity), curve_payload(), RECORD, 77)
    with pytest.raises(ValueError, match="exceeds attributed inventory"):
        curve.added(
            None, curve_addition(OpenPoolLiquidity, (10, 1001, 0)), curve_payload(), RECORD, 77
        )
    assert curve_model.minted == []
