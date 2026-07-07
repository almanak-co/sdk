"""VIB-5664 — parity + behaviour tests for the extracted LP repricing engine.

The repricing body was extracted from ``PortfolioValuer._reprice_lp_on_chain_enriched``
into the module-level ``lp_repricer.reprice_lp_position`` so the portfolio valuer
and ``MarketSnapshot.lp_position_value`` share ONE code path. These tests pin:

  * PARITY: the shared helper produces byte-identical output to the valuer method
    for the same stubbed inputs (guards against the two paths ever diverging).
  * Empty ≠ Zero: unmeasured inputs → ``None``; genuinely empty position →
    measured ``(0, {...})``.
  * The typed-result builder (``build_lp_position_value_result``) splits
    value/fees/total correctly and maps the zero-liquidity case.
"""

from decimal import Decimal
from unittest.mock import MagicMock

from almanak.framework.teardown.models import PositionInfo, PositionType
from almanak.framework.valuation.lp_position_reader import LPPositionOnChain, PoolSlot0
from almanak.framework.valuation.lp_repricer import (
    build_lp_position_value_result,
    default_decimals_fn,
    reprice_lp_position,
)
from almanak.framework.valuation.portfolio_valuer import PortfolioValuer


def _make_market(prices):
    market = MagicMock()

    def mock_price(token, quote="USD"):
        if token in prices:
            return prices[token]
        raise ValueError(f"No price for {token}")

    market.price = mock_price
    return market


def _stub_reader(*, liquidity=10_000_000_000, owed0=0, owed1=0, slot0_tick=0):
    reader = MagicMock()
    reader.read_position.return_value = LPPositionOnChain(
        token_id=12345,
        token0="0x82af49447d8a07e3bd95bd0d56f35241523fbab1",  # WETH (Arbitrum)
        token1="0xaf88d065e77c8cc2239327c5edb3a432268e5831",  # USDC (Arbitrum)
        fee=500,
        tick_lower=-100,
        tick_upper=100,
        liquidity=liquidity,
        tokens_owed0=owed0,
        tokens_owed1=owed1,
    )
    reader.read_pool_slot0.return_value = PoolSlot0(sqrt_price_x96=2**96, tick=slot0_tick)
    return reader


def _position(pool="0x1111111111111111111111111111111111111111"):
    return PositionInfo(
        position_type=PositionType.LP,
        position_id="12345",
        chain="arbitrum",
        protocol="uniswap_v3",
        value_usd=Decimal("0"),
        details={"pool_address": pool, "token0": "WETH", "token1": "USDC"},
    )


class TestRepriceParity:
    def test_shared_helper_matches_valuer_method(self):
        """The extracted helper == the valuer's method for identical inputs."""
        prices = {"WETH": Decimal("3500"), "USDC": Decimal("1")}

        valuer = PortfolioValuer()
        valuer._lp_reader = _stub_reader(owed0=1_000_000_000_000_000, owed1=2_000_000)
        old = valuer._reprice_lp_on_chain_enriched(_position(), "arbitrum", _make_market(prices))

        new = reprice_lp_position(
            _stub_reader(owed0=1_000_000_000_000_000, owed1=2_000_000),
            _position(),
            "arbitrum",
            _make_market(prices).price,
            default_decimals_fn,
        )

        assert old is not None and new is not None
        old_total, old_enriched = old
        new_total, new_enriched = new
        assert new_total == old_total
        assert new_enriched == old_enriched
        # Fees were added (owed0/owed1 > 0)
        assert Decimal(new_enriched["fees_usd"]) > 0

    def test_zero_liquidity_returns_measured_zero(self):
        new = reprice_lp_position(
            _stub_reader(liquidity=0, owed0=0, owed1=0),
            _position(),
            "arbitrum",
            _make_market({"WETH": Decimal("3500"), "USDC": Decimal("1")}).price,
            default_decimals_fn,
        )
        assert new == (Decimal("0"), {"position_id": "12345", "liquidity": "0"})

    def test_reader_miss_returns_none(self):
        reader = MagicMock()
        reader.read_position.return_value = None
        new = reprice_lp_position(
            reader,
            _position(),
            "arbitrum",
            _make_market({"WETH": Decimal("3500"), "USDC": Decimal("1")}).price,
            default_decimals_fn,
        )
        assert new is None

    def test_unmeasured_price_returns_none(self):
        """A raising price_fn (unmeasured price) → None, never a fabricated $0."""
        new = reprice_lp_position(
            _stub_reader(),
            _position(),
            "arbitrum",
            _make_market({"USDC": Decimal("1")}).price,  # WETH price missing → raises
            default_decimals_fn,
        )
        assert new is None


class TestBuildResult:
    def test_splits_value_fees_total(self):
        prices = {"WETH": Decimal("3500"), "USDC": Decimal("1")}
        output = reprice_lp_position(
            _stub_reader(owed0=1_000_000_000_000_000, owed1=2_000_000),
            _position(),
            "arbitrum",
            _make_market(prices).price,
            default_decimals_fn,
        )
        result = build_lp_position_value_result(output)
        assert result is not None
        total, _ = output
        assert result.total_usd == total
        # value + fees reconstitutes total
        assert result.value_usd + result.fees_usd == result.total_usd
        assert result.fees_usd > 0
        assert result.in_range is True  # tick 0 within [-100, 100]

    def test_none_in_none_out(self):
        assert build_lp_position_value_result(None) is None

    def test_zero_liquidity_maps_to_zero_result(self):
        result = build_lp_position_value_result((Decimal("0"), {"position_id": "12345", "liquidity": "0"}))
        assert result is not None
        assert result.value_usd == Decimal("0")
        assert result.fees_usd == Decimal("0")
        assert result.total_usd == Decimal("0")
        assert result.in_range is False
        assert result.liquidity == 0
