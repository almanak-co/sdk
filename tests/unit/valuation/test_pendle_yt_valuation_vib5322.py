"""VIB-5322 — held-YT NAV is sourced from the gateway YT/USD price authority.

These drive the FULL ``PortfolioValuer.value()`` path (strategy positions →
merge → reprice → snapshot), so they fail if the gateway YT mark never reaches a
real NAV computation — the inert-layer trap (a contract change nothing feeds).
Mirrors ``test_pendle_pt_valuation_vib5313.py`` for the YT primitive.

A Pendle strategy reports its held YT as a discovered position
(``protocol="pendle"``, often under ``PositionType.SUPPLY``). The valuer must
intercept it by DATA SHAPE (``details.yt_token``), read the gateway YT/USD mark
via ``MarketSnapshot.pt_price`` (which composes ``yt_usd = (1 − pt_to_asset_rate)
× underlying/USD`` for a YT symbol), and value ``yt_amount × mark``.

Scenarios pin the money contract:
  1. HIGH price       → position NAV = yt_amount × mark, snapshot HIGH.
  2. Maturity zero    → MEASURED $0 NAV, snapshot HIGH (a worthless YT is a real
                        measured zero, NOT unmeasured — Empty ≠ Zero both ways).
  3. UNAVAILABLE      → unmeasured: no_path, snapshot UNAVAILABLE, NOT a booked $0.
  4. STALE            → valued but degraded: snapshot ESTIMATED (never folded to HIGH).
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

from almanak.framework.market.models import PtPriceData
from almanak.framework.portfolio.models import ValueConfidence
from almanak.framework.teardown.models import PositionInfo, PositionType
from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

_YT_SYMBOL = "YT-sUSDe-13AUG2026"
_CHAIN = "ethereum"


def _pendle_yt_position() -> PositionInfo:
    """How a Pendle strategy reports its held YT: protocol=pendle, SUPPLY type,
    placeholder value_usd=0, YT symbol in details (mirrors the PT shape)."""
    return PositionInfo(
        position_type=PositionType.SUPPLY,
        position_id="pendle-yt-position",
        chain=_CHAIN,
        protocol="pendle",
        value_usd=Decimal("0"),
        details={"yt_token": _YT_SYMBOL, "from_token": "USDe"},
    )


def _yt_price(*, price, confidence, underlying=Decimal("2.0"), rate=Decimal("0.05")) -> PtPriceData:
    """Gateway YT/USD mark for a YT symbol. ``pt_to_asset_rate`` echoes the YT
    complement rate the gateway composed the mark from (display transparency)."""
    return PtPriceData(
        symbol=_YT_SYMBOL,
        chain=_CHAIN,
        price=price,
        confidence=confidence,
        underlying_price=underlying,
        pt_to_asset_rate=rate,
        days_to_maturity=120,
        source="composition:yt=(1−getPtToAssetRate)×oracle",
    )


def _market(*, yt_amount: Decimal, yt_price: PtPriceData) -> MagicMock:
    market = MagicMock()
    # Native gas token (ETH) must be priceable so the snapshot stays HIGH — an
    # unpriced native folds wallet_data_incomplete → ESTIMATED, masking the
    # contract under test.
    market.price.side_effect = lambda token, *a, **k: (
        Decimal("2000") if token in ("ETH", "WETH") else Decimal("0")
    )
    # Only the YT symbol has a (non-native) wallet balance; the native gas row
    # resolves to a measured zero.
    market.balance.side_effect = lambda token, *a, **k: MagicMock(
        balance=yt_amount if token == _YT_SYMBOL else Decimal("0")
    )
    market.pt_price.return_value = yt_price
    return market


def _run_value(*, yt_amount: Decimal, yt_price: PtPriceData):
    """Drive the full value() path with one strategy-reported held-YT position."""
    valuer = PortfolioValuer(gateway_client=None)
    strategy = MagicMock()
    strategy.deployment_id = "pendle-strat"
    strategy.chain = _CHAIN
    strategy.wallet_address = "0xWallet"
    strategy._get_tracked_tokens.return_value = []

    market = _market(yt_amount=yt_amount, yt_price=yt_price)

    with (
        patch.object(valuer, "_get_strategy_positions", return_value=([_pendle_yt_position()], False)),
        patch.object(valuer, "_build_discovery_config", return_value=None),
    ):
        return valuer.value(strategy, market)


def _pendle_legs(snapshot):
    return [p for p in snapshot.positions if (p.protocol or "").lower() == "pendle"]


class TestPendleYtNavVib5322:
    def test_high_price_books_gateway_yt_mark(self):
        """HIGH gateway YT mark → NAV = yt_amount × mark, snapshot HIGH."""
        snapshot = _run_value(
            yt_amount=Decimal("100"),
            yt_price=_yt_price(price=Decimal("0.10"), confidence=ValueConfidence.HIGH),
        )
        legs = _pendle_legs(snapshot)
        assert len(legs) == 1
        # 100 YT × $0.10 gateway YT/USD mark = $10
        assert legs[0].value_usd == Decimal("10.00")
        assert snapshot.total_value_usd == Decimal("10.00")
        assert snapshot.value_confidence == ValueConfidence.HIGH
        assert legs[0].details.get("valuation_status") != "no_path"
        assert legs[0].details.get("price_confidence") == str(ValueConfidence.HIGH)
        assert legs[0].details.get("source") == "yt_inventory_lots"
        assert legs[0].details.get("yt_symbol") == _YT_SYMBOL
        assert legs[0].details.get("price_source")  # gateway source stamped

    def test_maturity_zero_is_measured_zero_not_unmeasured(self):
        """A measured $0 YT mark (post-maturity worthless YT) → $0 NAV, snapshot HIGH.

        Empty ≠ Zero the OTHER way: a worthless YT is a real measured zero, not an
        unmeasured price. The leg books a measured $0 (repriced=True), and the
        snapshot stays HIGH — it must NOT degrade to UNAVAILABLE as if the price
        were missing.
        """
        snapshot = _run_value(
            yt_amount=Decimal("100"),
            yt_price=_yt_price(
                price=Decimal("0"),
                confidence=ValueConfidence.HIGH,
                rate=Decimal("0"),  # complement rate 0 → fully decayed YT
            ),
        )
        legs = _pendle_legs(snapshot)
        assert len(legs) == 1
        assert legs[0].value_usd == Decimal("0")
        # Measured zero, NOT the unmeasured no_path marker.
        assert legs[0].details.get("valuation_status") != "no_path"
        assert legs[0].details.get("mark_unmeasured") is not True
        assert snapshot.value_confidence == ValueConfidence.HIGH

    def test_unavailable_price_is_unmeasured_not_zero(self):
        """UNAVAILABLE gateway price → no_path, snapshot UNAVAILABLE — never a booked $0."""
        snapshot = _run_value(
            yt_amount=Decimal("100"),
            yt_price=_yt_price(price=None, confidence=ValueConfidence.UNAVAILABLE),
        )
        legs = _pendle_legs(snapshot)
        assert len(legs) == 1
        det = legs[0].details
        assert det.get("valuation_status") == "no_path"
        assert det.get("mark_unmeasured") is True
        assert det.get("source") == "yt_inventory_lots"
        assert det.get("yt_symbol") == _YT_SYMBOL
        assert det.get("quantity") == "100"
        # Empty ≠ Zero: "we couldn't value this", not a measured zero NAV.
        assert snapshot.value_confidence == ValueConfidence.UNAVAILABLE

    def test_stale_price_degrades_to_estimated(self):
        """STALE gateway YT mark → valued, but snapshot ESTIMATED (never HIGH)."""
        snapshot = _run_value(
            yt_amount=Decimal("100"),
            yt_price=_yt_price(price=Decimal("0.20"), confidence=ValueConfidence.STALE),
        )
        legs = _pendle_legs(snapshot)
        assert len(legs) == 1
        assert legs[0].value_usd == Decimal("20.00")  # 100 × $0.20 — value still booked
        assert legs[0].details.get("valuation_status") == "estimated"
        assert snapshot.value_confidence == ValueConfidence.ESTIMATED

    def test_pt_price_called_with_yt_symbol_and_chain(self):
        """The repricer sources the YT mark via MarketSnapshot.pt_price keyed on the YT symbol."""
        valuer = PortfolioValuer(gateway_client=None)
        strategy = MagicMock()
        strategy.deployment_id = "pendle-strat"
        strategy.chain = _CHAIN
        strategy.wallet_address = "0xWallet"
        strategy._get_tracked_tokens.return_value = []
        market = _market(
            yt_amount=Decimal("10"),
            yt_price=_yt_price(price=Decimal("0.10"), confidence=ValueConfidence.HIGH),
        )
        with (
            patch.object(valuer, "_get_strategy_positions", return_value=([_pendle_yt_position()], False)),
            patch.object(valuer, "_build_discovery_config", return_value=None),
        ):
            valuer.value(strategy, market)
        market.pt_price.assert_called_once_with(_YT_SYMBOL, _CHAIN)
