"""VIB-5890 — a carry's borrowed-and-held ``TOKEN`` inventory must be PRICED.

A carry trade does ``SUPPLY WBNB → BORROW USDC → SWAP USDC→USDT (hold)`` and
reports the held USDT as a ``PositionType.TOKEN`` position carrying the amount
(``details["amount"]``, ``origin == "swapped_from_borrow"``) but leaving
``value_usd`` unmeasured (``Decimal("0")``). Unlike LP / lending / perp / vault
positions, a bare spot holding had no repricer, so it fell through to the
verbatim ``$0`` strategy value.

The debt-netted NAV read path
(``almanak.framework.valuation.net_debt``; ``NAV = total_value_usd − debt_mark``)
then subtracted the BORROW leg WITHOUT the offsetting held inventory, so the
open-position-NAV slice read ``collateral − debt + $0`` — a phantom ~−30% cliff
and inflated drawdown on an economically flat position (real P&L ≈ gas only).

Post-fix: ``PortfolioValuer`` prices the held token as ``amount × price(asset)``
(blueprint 27 §7.4 spot rule), preferring the LIVE wallet balance over the
cached amount, so the held inventory enters ``total_value_usd`` and the
debt-netted slice recovers net equity (``collateral − debt + held``). See the
frozen evidence DB
``docs/internal/quant-user-runs/20260716-2110-noneth-pancakeswap_aave_carry_bsc``
(deployment ``deployment:400dcfee3a7c``).
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak.framework.portfolio.models import ValueConfidence
from almanak.framework.teardown.models import (
    PositionInfo,
    PositionType,
    TeardownPositionSummary,
)
from almanak.framework.valuation.net_debt import net_debt_from_snapshot
from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

# The carry's three legs, matching the frozen db.sqlite snapshot shape.
_COLLATERAL_USD = Decimal("3.144")  # SUPPLY WBNB, on-chain-marked
_DEBT_USD = Decimal("0.94")  # BORROW USDC (negative leg)
_HELD_USDT_AMOUNT = Decimal("0.940524611877894408")  # swapped-from-borrow, HELD
_USDT_PRICE = Decimal("1")


def _carry_strategy(
    *,
    token_amount: str | None = str(_HELD_USDT_AMOUNT),
    origin: str | None = "swapped_from_borrow",
) -> MagicMock:
    strategy = MagicMock()
    strategy.deployment_id = "deployment:400dcfee3a7c"
    strategy.chain = "bsc"
    strategy.wallet_address = "0xafeB2f5c213b5e7F37c3Fc171dfCb6270d07e21a"
    # USDT is intentionally NOT tracked into wallet_balances — mirrors the frozen
    # DB where wallet_balances_json held only native BNB. This is the case that
    # makes the held USDT a genuine (non-wallet-overlapping) deployed holding.
    strategy._get_tracked_tokens.return_value = ["WBNB", "USDC", "BNB"]
    token_details: dict[str, str] = {"asset": "USDT"}
    if origin is not None:
        token_details["origin"] = origin
    if token_amount is not None:
        token_details["amount"] = token_amount
    strategy.get_open_positions.return_value = TeardownPositionSummary(
        deployment_id="deployment:400dcfee3a7c",
        timestamp=datetime.now(UTC),
        positions=[
            PositionInfo(
                position_type=PositionType.SUPPLY,
                position_id="aave-v3-supply-WBNB-bsc",
                chain="bsc",
                protocol="aave_v3",
                value_usd=_COLLATERAL_USD,
                details={"asset": "WBNB", "amount": "0.0055"},
            ),
            PositionInfo(
                position_type=PositionType.BORROW,
                position_id="aave-v3-borrow-USDC-bsc",
                chain="bsc",
                protocol="aave_v3",
                value_usd=-_DEBT_USD,
                details={"asset": "USDC", "amount": "0.94"},
            ),
            PositionInfo(
                position_type=PositionType.TOKEN,
                position_id="pancakeswap-swap-USDT-bsc",
                chain="bsc",
                protocol="pancakeswap_v3",
                value_usd=Decimal("0"),  # the bug: unmeasured despite a known amount
                details=token_details,
            ),
        ],
    )
    return strategy


def _carry_market(
    *,
    usdt_price: object = _USDT_PRICE,
    usdt_balance: object | None = _HELD_USDT_AMOUNT,
) -> MagicMock:
    """Carry-shaped market.

    ``usdt_balance`` seeds the LIVE balance the repricer prefers (set ``None`` to
    make the balance read fail → cached-amount fallback). ``usdt_price`` may be a
    poisoned value (NaN / 0 / negative) to exercise the unmeasured-price path.
    """
    market = MagicMock()
    prices = {"USDT": usdt_price, "USDC": Decimal("1"), "BNB": Decimal("571.56")}
    balances = {"BNB": Decimal("0.0024626808")}  # native gas, as in the frozen DB
    if usdt_balance is not None:
        balances["USDT"] = usdt_balance

    def mock_price(token: str, quote: str = "USD", *, chain: str | None = None):
        if token in prices:
            return prices[token]
        raise ValueError(f"No price for {token}")

    def mock_balance(token: str, protocol: str | None = None, *, chain: str | None = None, price=None):
        if token in balances:
            result = MagicMock()
            result.balance = balances[token]
            return result
        raise ValueError(f"No balance for {token}")

    market.price = mock_price
    market.balance = mock_balance
    return market


def _usdt_position(snapshot) -> object:
    return next(
        p
        for p in snapshot.positions
        if p.position_type == PositionType.TOKEN and p.details.get("asset") == "USDT"
    )


def test_held_usdt_token_is_priced_to_its_amount():
    """The swapped-from-borrow USDT position is valued ≈ amount × price, not $0."""
    snapshot = PortfolioValuer().value(_carry_strategy(), _carry_market())

    usdt = _usdt_position(snapshot)
    expected = _HELD_USDT_AMOUNT * _USDT_PRICE
    assert usdt.value_usd == expected
    assert usdt.value_usd > Decimal("0")  # the regression: it was "0"
    assert usdt.details.get("valuation_source") == "spot_amount_price"


def test_open_position_slice_recovers_net_equity_no_phantom_cliff():
    """The debt-netted open-position slice ≈ net equity (collateral − debt + held).

    This is the dashboard NAV-chart / drawdown input (see
    ``dashboard_service`` ``total_value_usd − debt_mark`` and
    ``quant_aggregations`` wallet-NAV fold). Pre-fix it read
    ``collateral − debt + $0`` (the phantom cliff); post-fix the held USDT
    offsets the debt.
    """
    snapshot = PortfolioValuer().value(_carry_strategy(), _carry_market())

    _count, debt_mark, _debt_cost, _net_cost = net_debt_from_snapshot(snapshot)
    open_position_slice = snapshot.total_value_usd - debt_mark

    held = _HELD_USDT_AMOUNT * _USDT_PRICE
    net_equity = _COLLATERAL_USD - _DEBT_USD + held
    assert open_position_slice == net_equity

    # And explicitly NOT the pre-fix phantom slice (collateral − debt + $0), which
    # is a ~30% haircut on a flat carry — the drawdown badge the ticket flags.
    phantom_slice = _COLLATERAL_USD - _DEBT_USD
    assert open_position_slice != phantom_slice
    assert open_position_slice - phantom_slice == held


def test_repriced_token_has_matching_token_price_record():
    """A repriced TOKEN row must persist with an auditable price entry.

    ``_build_token_price_records`` iterates the token list (not ``prices``), and
    the held USDT is NOT a tracked token, so the repricer's mark must be threaded
    into BOTH the price map and the record-token set, else positions_json carries a
    priced row with no matching token_prices_json entry (internally inconsistent).
    """
    snapshot = PortfolioValuer().value(_carry_strategy(), _carry_market())

    assert any(
        rec.get("symbol") == "USDT" and Decimal(rec["price_usd"]) == _USDT_PRICE
        for rec in snapshot.token_prices.values()
    ), snapshot.token_prices


def test_live_balance_is_preferred_over_cached_amount():
    """The mark uses the LIVE wallet balance (self-correcting for drift), not the
    strategy's cached ``details["amount"]``."""
    live_balance = Decimal("0.95")  # deliberately != cached _HELD_USDT_AMOUNT
    snapshot = PortfolioValuer().value(
        _carry_strategy(), _carry_market(usdt_balance=live_balance)
    )

    usdt = _usdt_position(snapshot)
    assert usdt.details.get("spot_amount_source") == "live_balance"
    assert usdt.value_usd == live_balance * _USDT_PRICE
    assert usdt.details.get("spot_amount") == str(live_balance)


def test_cached_amount_used_when_live_balance_unavailable():
    """When the live balance read fails, the cached amount is the fallback (and the
    source is stamped so the staleness is auditable)."""
    snapshot = PortfolioValuer().value(
        _carry_strategy(), _carry_market(usdt_balance=None)
    )

    usdt = _usdt_position(snapshot)
    assert usdt.details.get("spot_amount_source") == "cached_amount"
    assert usdt.value_usd == _HELD_USDT_AMOUNT * _USDT_PRICE


@pytest.mark.parametrize("bad_price", [Decimal("NaN"), Decimal("Infinity"), Decimal("0"), Decimal("-1")])
def test_non_finite_or_non_positive_price_is_unmeasured_not_fabricated(bad_price):
    """Empty ≠ Zero: a NaN / Inf / zero / negative price never yields a fabricated
    $0 or a NaN mark — the holding is unmeasured and the snapshot drops to
    UNAVAILABLE."""
    snapshot = PortfolioValuer().value(
        _carry_strategy(), _carry_market(usdt_price=bad_price)
    )

    usdt = _usdt_position(snapshot)
    assert usdt.value_usd == Decimal("0")  # unmeasured — NOT a priced value / NaN
    assert usdt.value_usd.is_finite()
    assert usdt.details.get("valuation_source") != "spot_amount_price"
    # The caller stamps ``no_path`` when a repricer produces no measurement.
    assert usdt.details.get("valuation_status") == "no_path"
    assert snapshot.value_confidence == ValueConfidence.UNAVAILABLE


def test_unavailable_price_leaves_holding_unmeasured_not_fabricated_zero():
    """No USDT price at all (oracle raises) → unmeasured, UNAVAILABLE, no $0."""
    market = _carry_market()
    good_price = market.price

    def price_without_usdt(token: str, quote: str = "USD", *, chain: str | None = None):
        if token == "USDT":
            raise ValueError("No price for USDT")
        return good_price(token, quote, chain=chain)

    market.price = price_without_usdt

    snapshot = PortfolioValuer().value(_carry_strategy(), market)

    usdt = _usdt_position(snapshot)
    assert usdt.value_usd == Decimal("0")
    assert usdt.details.get("valuation_status") == "no_path"
    assert snapshot.value_confidence == ValueConfidence.UNAVAILABLE


@pytest.mark.parametrize("bad_amount", ["NaN", "Infinity", "0", "-1", "not-a-number"])
def test_unmeasurable_amount_is_unavailable_not_measured_zero(bad_amount):
    """A declared held-inventory row we cannot size (poisoned cached amount, no
    live balance) must NOT crash the snapshot AND must NOT be booked as a measured
    $0 — it is UNMEASURED (repriced=False → UNAVAILABLE), per Empty ≠ Zero
    (codex #3)."""
    snapshot = PortfolioValuer().value(
        _carry_strategy(token_amount=bad_amount),
        _carry_market(usdt_balance=None),  # force the cached-amount path
    )

    usdt = _usdt_position(snapshot)
    assert usdt.value_usd == Decimal("0")  # not fabricated, not crashed
    assert usdt.value_usd.is_finite()
    assert usdt.details.get("valuation_source") != "spot_amount_price"
    assert usdt.details.get("valuation_status") == "no_path"
    assert snapshot.value_confidence == ValueConfidence.UNAVAILABLE


def test_non_finite_live_balance_falls_back_to_cached_amount():
    """A NaN/Inf live balance is rejected (Empty ≠ Zero) and the cached amount is
    used instead — a poisoned balance never poisons the mark."""
    snapshot = PortfolioValuer().value(
        _carry_strategy(), _carry_market(usdt_balance=Decimal("NaN"))
    )

    usdt = _usdt_position(snapshot)
    assert usdt.details.get("spot_amount_source") == "cached_amount"
    assert usdt.value_usd == _HELD_USDT_AMOUNT * _USDT_PRICE


def test_measured_live_zero_does_not_use_stale_cached_amount():
    """Codex #2 — a MEASURED live zero (wallet emptied) is authoritative: the mark
    is $0, NOT the stale positive cached amount that no longer exists on-chain."""
    snapshot = PortfolioValuer().value(
        _carry_strategy(token_amount=str(_HELD_USDT_AMOUNT)),  # positive cached
        _carry_market(usdt_balance=Decimal("0")),  # live measured zero
    )

    usdt = _usdt_position(snapshot)
    assert usdt.value_usd == Decimal("0")  # holding is gone — not the cached 0.94
    assert usdt.value_usd != _HELD_USDT_AMOUNT * _USDT_PRICE
    assert usdt.details.get("spot_amount_source") == "live_balance"


def test_non_carry_zero_token_is_not_repriced():
    """Codex #1 — the repricer is scoped to ``origin == "swapped_from_borrow"``.
    A non-carry strategy's intentionally-$0 TOKEN (asset + amount, but no carry
    origin marker) is left at its verbatim value — NOT repriced, and NOT dragged to
    UNAVAILABLE."""
    snapshot = PortfolioValuer().value(
        _carry_strategy(origin=None),  # plain TOKEN, no swapped_from_borrow marker
        _carry_market(),
    )

    usdt = _usdt_position(snapshot)
    assert usdt.value_usd == Decimal("0")  # verbatim strategy value, untouched
    assert usdt.details.get("valuation_source") != "spot_amount_price"
    assert usdt.details.get("valuation_status") != "no_path"


def test_product_overflow_degrades_to_unavailable():
    """Codex #4 — an extreme-exponent ``amount × price`` (finite positive operands)
    that overflows the Decimal context must degrade to UNAVAILABLE, not abort the
    snapshot or persist a NaN/Inf mark."""
    snapshot = PortfolioValuer().value(
        _carry_strategy(token_amount="1E999999"),  # finite, positive, huge
        _carry_market(usdt_balance=None, usdt_price=Decimal("1E999999")),
    )

    usdt = _usdt_position(snapshot)
    assert usdt.value_usd == Decimal("0")
    assert usdt.value_usd.is_finite()
    assert usdt.details.get("valuation_source") != "spot_amount_price"
    assert usdt.details.get("valuation_status") == "no_path"
    assert snapshot.value_confidence == ValueConfidence.UNAVAILABLE
