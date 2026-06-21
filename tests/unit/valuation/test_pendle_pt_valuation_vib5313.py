"""VIB-5313 — Pendle PT NAV is sourced from the gateway PT/USD price authority.

These drive the FULL ``PortfolioValuer.value()`` path (strategy positions →
merge → reprice → snapshot), so they fail if the gateway price never reaches a
real NAV computation — the inert-layer trap (a contract change nothing feeds).

A Pendle strategy reports its PT holding as a discovered position
(``protocol="pendle"``, often under ``PositionType.SUPPLY`` — exp8). The valuer
must intercept it by protocol identity, read the gateway PT/USD via
``MarketSnapshot.pt_price``, and value ``pt_amount × price``.

Scenarios pin the money contract:
  1. HIGH price   → position NAV = pt_amount × price, snapshot HIGH.
  2. UNAVAILABLE  → unmeasured: no_path, snapshot UNAVAILABLE, NOT a booked $0.
  3. STALE        → valued but degraded: snapshot ESTIMATED (never folded to HIGH).
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

from almanak.framework.market.models import PtPriceData
from almanak.framework.portfolio.models import ValueConfidence
from almanak.framework.teardown.models import PositionInfo, PositionType
from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

_PT_SYMBOL = "PT-sUSDe-26DEC2024"
_CHAIN = "ethereum"


def _pendle_position() -> PositionInfo:
    """How a Pendle strategy reports its PT: protocol=pendle, SUPPLY type,
    placeholder value_usd=0, PT symbol in details (exp8 / pendle_aave_spread)."""
    return PositionInfo(
        position_type=PositionType.SUPPLY,
        position_id="pendle-pt-position",
        chain=_CHAIN,
        protocol="pendle",
        value_usd=Decimal("0"),
        details={"pt_token": _PT_SYMBOL, "from_token": "USDe"},
    )


def _pt_price(*, price, confidence, underlying=Decimal("1.0"), rate=Decimal("0.95")) -> PtPriceData:
    return PtPriceData(
        symbol=_PT_SYMBOL,
        chain=_CHAIN,
        price=price,
        confidence=confidence,
        underlying_price=underlying,
        pt_to_asset_rate=rate,
        days_to_maturity=120,
        source="composition:getPtToAssetRate×oracle",
    )


def _market(*, pt_amount: Decimal, pt_price: PtPriceData) -> MagicMock:
    market = MagicMock()
    # Native gas token (ETH) must be priceable so the snapshot stays HIGH — an
    # unpriced native folds wallet_data_incomplete → ESTIMATED, masking the
    # contract under test.
    market.price.side_effect = lambda token, *a, **k: (
        Decimal("2000") if token in ("ETH", "WETH") else Decimal("0")
    )
    # Only the PT symbol has a (non-native) wallet balance; the native gas row
    # resolves to a measured zero.
    market.balance.side_effect = lambda token, *a, **k: MagicMock(
        balance=pt_amount if token == _PT_SYMBOL else Decimal("0")
    )
    market.pt_price.return_value = pt_price
    return market


def _run_value(*, pt_amount: Decimal, pt_price: PtPriceData):
    """Drive the full value() path with one strategy-reported Pendle position."""
    valuer = PortfolioValuer(gateway_client=None)
    strategy = MagicMock()
    strategy.deployment_id = "pendle-strat"
    strategy.chain = _CHAIN
    strategy.wallet_address = "0xWallet"
    strategy._get_tracked_tokens.return_value = []

    market = _market(pt_amount=pt_amount, pt_price=pt_price)

    with (
        patch.object(valuer, "_get_strategy_positions", return_value=([_pendle_position()], False)),
        patch.object(valuer, "_build_discovery_config", return_value=None),
    ):
        return valuer.value(strategy, market)


def _pendle_legs(snapshot):
    return [p for p in snapshot.positions if (p.protocol or "").lower() == "pendle"]


class TestPendlePtNavVib5313:
    def test_high_price_books_gateway_mark(self):
        """HIGH gateway price → NAV = pt_amount × price, snapshot HIGH, mark sourced from gateway."""
        snapshot = _run_value(
            pt_amount=Decimal("100"),
            pt_price=_pt_price(price=Decimal("0.95"), confidence=ValueConfidence.HIGH),
        )
        legs = _pendle_legs(snapshot)
        assert len(legs) == 1
        # 100 PT × $0.95 gateway PT/USD mark = $95
        assert legs[0].value_usd == Decimal("95")
        assert snapshot.total_value_usd == Decimal("95")
        assert snapshot.value_confidence == ValueConfidence.HIGH
        assert legs[0].details.get("valuation_status") != "no_path"
        assert legs[0].details.get("price_confidence") == str(ValueConfidence.HIGH)
        assert legs[0].details.get("price_source")  # gateway source stamped

    def test_unavailable_price_is_unmeasured_not_zero(self):
        """UNAVAILABLE gateway price → no_path, snapshot UNAVAILABLE — never a booked $0."""
        snapshot = _run_value(
            pt_amount=Decimal("100"),
            pt_price=_pt_price(price=None, confidence=ValueConfidence.UNAVAILABLE),
        )
        legs = _pendle_legs(snapshot)
        assert len(legs) == 1
        assert legs[0].details.get("valuation_status") == "no_path"
        # Empty ≠ Zero: the snapshot must surface "we couldn't value this",
        # not a measured zero NAV.
        assert snapshot.value_confidence == ValueConfidence.UNAVAILABLE

    def test_stale_price_degrades_to_estimated(self):
        """STALE gateway price → valued, but snapshot ESTIMATED (never HIGH)."""
        snapshot = _run_value(
            pt_amount=Decimal("100"),
            pt_price=_pt_price(price=Decimal("0.94"), confidence=ValueConfidence.STALE),
        )
        legs = _pendle_legs(snapshot)
        assert len(legs) == 1
        assert legs[0].value_usd == Decimal("94")  # 100 × $0.94 — value still booked
        assert legs[0].details.get("valuation_status") == "estimated"
        assert snapshot.value_confidence == ValueConfidence.ESTIMATED

    def test_pt_price_called_with_symbol_and_chain(self):
        """The repricer sources price via MarketSnapshot.pt_price keyed on the PT symbol."""
        valuer = PortfolioValuer(gateway_client=None)
        strategy = MagicMock()
        strategy.deployment_id = "pendle-strat"
        strategy.chain = _CHAIN
        strategy.wallet_address = "0xWallet"
        strategy._get_tracked_tokens.return_value = []
        market = _market(
            pt_amount=Decimal("10"),
            pt_price=_pt_price(price=Decimal("0.95"), confidence=ValueConfidence.HIGH),
        )
        with (
            patch.object(valuer, "_get_strategy_positions", return_value=([_pendle_position()], False)),
            patch.object(valuer, "_build_discovery_config", return_value=None),
        ):
            valuer.value(strategy, market)
        market.pt_price.assert_called_once_with(_PT_SYMBOL, _CHAIN)
