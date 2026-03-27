"""Tests for LP position valuation (V3 math).

Tests the pure math in lp_valuer.py and hex decoding in lp_position_reader.py.
"""

from decimal import Decimal

import pytest

from almanak.framework.valuation.lp_valuer import (
    LPPositionValue,
    LPTokenAmounts,
    _tick_to_sqrt_price,
    get_token_amounts,
    get_token_amounts_from_sqrt_price,
    value_lp_position,
)


class TestGetTokenAmounts:
    """Test V3 token amount calculation."""

    def test_price_in_range_returns_both_tokens(self):
        """Within range, position holds a mix of both tokens."""
        amounts = get_token_amounts(
            liquidity=1_000_000_000_000,
            tick_lower=-887220,
            tick_upper=887220,
            current_tick=0,
        )
        assert amounts.amount0 > 0
        assert amounts.amount1 > 0

    def test_price_below_range_all_token0(self):
        """Below range, position is 100% token0."""
        amounts = get_token_amounts(
            liquidity=1_000_000_000_000,
            tick_lower=100,
            tick_upper=200,
            current_tick=50,  # Below lower
        )
        assert amounts.amount0 > 0
        assert amounts.amount1 == Decimal("0")

    def test_price_above_range_all_token1(self):
        """Above range, position is 100% token1."""
        amounts = get_token_amounts(
            liquidity=1_000_000_000_000,
            tick_lower=100,
            tick_upper=200,
            current_tick=250,  # Above upper
        )
        assert amounts.amount0 == Decimal("0")
        assert amounts.amount1 > 0

    def test_zero_liquidity_returns_zero(self):
        amounts = get_token_amounts(liquidity=0, tick_lower=-100, tick_upper=100, current_tick=0)
        assert amounts.amount0 == 0
        assert amounts.amount1 == 0

    def test_invalid_tick_range_returns_zero(self):
        amounts = get_token_amounts(liquidity=1000, tick_lower=200, tick_upper=100, current_tick=150)
        assert amounts.amount0 == 0
        assert amounts.amount1 == 0

    def test_negative_liquidity_returns_zero(self):
        amounts = get_token_amounts(liquidity=-1000, tick_lower=-100, tick_upper=100, current_tick=0)
        assert amounts.amount0 == 0
        assert amounts.amount1 == 0

    def test_token_amounts_non_negative(self):
        """Amounts should never be negative regardless of inputs."""
        for tick in [-1000, -100, 0, 100, 1000]:
            amounts = get_token_amounts(
                liquidity=1_000_000,
                tick_lower=-500,
                tick_upper=500,
                current_tick=tick,
            )
            assert amounts.amount0 >= 0
            assert amounts.amount1 >= 0

    def test_at_lower_tick_boundary(self):
        """At exactly the lower tick, should be all token0."""
        amounts = get_token_amounts(
            liquidity=1_000_000_000_000,
            tick_lower=100,
            tick_upper=200,
            current_tick=100,
        )
        # At lower boundary, sqrt_price == sqrt_price_lower
        # token0 = L * (1/sqrt_lower - 1/sqrt_upper) > 0
        # token1 = L * (sqrt_lower - sqrt_lower) = 0
        assert amounts.amount0 > 0
        assert amounts.amount1 == Decimal("0")

    def test_at_upper_tick_boundary(self):
        """At exactly the upper tick, should be all token1."""
        amounts = get_token_amounts(
            liquidity=1_000_000_000_000,
            tick_lower=100,
            tick_upper=200,
            current_tick=200,
        )
        assert amounts.amount0 == Decimal("0")
        assert amounts.amount1 > 0


class TestGetTokenAmountsFromSqrtPrice:
    """Test token amounts using sqrtPriceX96."""

    def test_basic_calculation(self):
        """sqrtPriceX96 = 2^96 means price = 1.0 (tick ~0)."""
        q96 = 2**96
        amounts = get_token_amounts_from_sqrt_price(
            liquidity=1_000_000_000_000,
            tick_lower=-887220,
            tick_upper=887220,
            sqrt_price_x96=q96,  # price = 1.0
        )
        assert amounts.amount0 > 0
        assert amounts.amount1 > 0

    def test_zero_liquidity(self):
        amounts = get_token_amounts_from_sqrt_price(
            liquidity=0, tick_lower=-100, tick_upper=100, sqrt_price_x96=2**96
        )
        assert amounts.amount0 == 0
        assert amounts.amount1 == 0


class TestValueLpPosition:
    """Test full LP position USD valuation."""

    def test_basic_valuation(self):
        """Position with known parameters produces non-zero USD value."""
        result = value_lp_position(
            liquidity=10_000_000_000_000_000,  # Realistic liquidity
            tick_lower=-887220,
            tick_upper=887220,
            current_tick=0,
            token0_price_usd=Decimal("3000"),  # ETH
            token1_price_usd=Decimal("1"),  # USDC
            token0_decimals=18,
            token1_decimals=6,
        )
        assert isinstance(result, LPPositionValue)
        assert result.value_usd > 0
        assert result.amount0 >= 0
        assert result.amount1 >= 0
        assert result.token0_value_usd >= 0
        assert result.token1_value_usd >= 0
        assert result.in_range is True

    def test_out_of_range_below(self):
        result = value_lp_position(
            liquidity=10_000_000_000_000_000,
            tick_lower=100,
            tick_upper=200,
            current_tick=50,
            token0_price_usd=Decimal("3000"),
            token1_price_usd=Decimal("1"),
            token0_decimals=18,
            token1_decimals=6,
        )
        assert result.in_range is False
        assert result.amount1 == Decimal("0")
        # All value from token0
        assert result.token1_value_usd == Decimal("0")

    def test_out_of_range_above(self):
        result = value_lp_position(
            liquidity=10_000_000_000_000_000,
            tick_lower=100,
            tick_upper=200,
            current_tick=250,
            token0_price_usd=Decimal("3000"),
            token1_price_usd=Decimal("1"),
            token0_decimals=18,
            token1_decimals=6,
        )
        assert result.in_range is False
        assert result.amount0 == Decimal("0")
        assert result.token0_value_usd == Decimal("0")

    def test_value_sum_equals_total(self):
        """token0_value + token1_value should equal total value_usd."""
        result = value_lp_position(
            liquidity=10_000_000_000_000_000,
            tick_lower=-887220,
            tick_upper=887220,
            current_tick=0,
            token0_price_usd=Decimal("3000"),
            token1_price_usd=Decimal("1"),
            token0_decimals=18,
            token1_decimals=6,
        )
        assert result.value_usd == result.token0_value_usd + result.token1_value_usd

    def test_zero_liquidity_zero_value(self):
        result = value_lp_position(
            liquidity=0,
            tick_lower=-100,
            tick_upper=100,
            current_tick=0,
            token0_price_usd=Decimal("3000"),
            token1_price_usd=Decimal("1"),
            token0_decimals=18,
            token1_decimals=6,
        )
        assert result.value_usd == Decimal("0")

    def test_different_decimals(self):
        """WBTC (8 decimals) / USDC (6 decimals) pair."""
        result = value_lp_position(
            liquidity=10_000_000_000_000_000,
            tick_lower=-887220,
            tick_upper=887220,
            current_tick=0,
            token0_price_usd=Decimal("60000"),  # BTC
            token1_price_usd=Decimal("1"),  # USDC
            token0_decimals=8,
            token1_decimals=6,
        )
        assert result.value_usd > 0


class TestTickToSqrtPrice:
    """Test tick to sqrt(price) conversion."""

    def test_tick_zero_gives_one(self):
        """At tick 0, price = 1.0001^0 = 1.0, sqrt(1.0) = 1.0."""
        sqrt_p = _tick_to_sqrt_price(0)
        assert abs(sqrt_p - Decimal("1")) < Decimal("0.0001")

    def test_positive_tick(self):
        """Positive tick gives sqrt(price) > 1."""
        sqrt_p = _tick_to_sqrt_price(1000)
        assert sqrt_p > Decimal("1")

    def test_negative_tick(self):
        """Negative tick gives sqrt(price) < 1."""
        sqrt_p = _tick_to_sqrt_price(-1000)
        assert sqrt_p < Decimal("1")

    def test_symmetry(self):
        """tick and -tick should give reciprocal sqrt prices."""
        sqrt_pos = _tick_to_sqrt_price(1000)
        sqrt_neg = _tick_to_sqrt_price(-1000)
        # sqrt(1/x) = 1/sqrt(x), so product should be ~1.0
        product = sqrt_pos * sqrt_neg
        assert abs(product - Decimal("1")) < Decimal("0.001")

    def test_known_values(self):
        """Verify against known tick-price relationships.

        At tick 23027 (common for ETH/USDC at ~$2000):
        price ~= 1.0001^23027 ~= 10.0 (in pool units)
        """
        sqrt_p = _tick_to_sqrt_price(23027)
        price = sqrt_p * sqrt_p
        # Should be approximately 10.0
        assert Decimal("9") < price < Decimal("11")


class TestLPPositionReaderHexParsing:
    """Test hex response parsing for on-chain data."""

    def test_parse_position_hex_valid(self):
        from almanak.framework.valuation.lp_position_reader import _parse_position_hex

        # Build a fake positions() response: 12 words of 32 bytes each
        # Solidity ABI encodes int24 as sign-extended int256 (256-bit 2's complement)
        words = [
            0,  # nonce
            0x0000000000000000000000001234567890ABCDEF1234567890ABCDEF12345678,  # operator
            0x000000000000000000000000C02AAA39B223FE8D0A0E5C4F27EAD9083C756CC2,  # token0 (WETH)
            0x000000000000000000000000A0B86991C6218B36C1D19D4A2E9EB0CE3606EB48,  # token1 (USDC)
            3000,  # fee
            (-887220) & ((1 << 256) - 1),  # tickLower: ABI-standard int256 sign extension
            887220,  # tickUpper
            500_000_000_000,  # liquidity
            0,  # feeGrowthInside0
            0,  # feeGrowthInside1
            1000000,  # tokensOwed0
            2000000,  # tokensOwed1
        ]

        hex_data = "0x"
        for w in words:
            hex_data += hex(w)[2:].zfill(64)

        result = _parse_position_hex(hex_data, token_id=42)
        assert result is not None
        assert result.token_id == 42
        assert result.fee == 3000
        assert result.tick_lower == -887220
        assert result.tick_upper == 887220
        assert result.liquidity == 500_000_000_000
        assert result.tokens_owed0 == 1000000
        assert result.tokens_owed1 == 2000000
        assert result.is_active is True

    def test_parse_position_hex_too_short(self):
        from almanak.framework.valuation.lp_position_reader import _parse_position_hex

        result = _parse_position_hex("0x1234", token_id=1)
        assert result is None

    def test_parse_slot0_hex_valid(self):
        from almanak.framework.valuation.lp_position_reader import _parse_slot0_hex

        # slot0: sqrtPriceX96 = 2^96 (price ~= 1.0), tick = 0
        q96 = 2**96
        hex_data = "0x" + hex(q96)[2:].zfill(64) + "0" * 64
        result = _parse_slot0_hex(hex_data)
        assert result is not None
        assert result.sqrt_price_x96 == q96
        assert result.tick == 0

    def test_parse_slot0_hex_too_short(self):
        from almanak.framework.valuation.lp_position_reader import _parse_slot0_hex

        result = _parse_slot0_hex("0x1234")
        assert result is None

    def test_parse_position_hex_zero_liquidity(self):
        from almanak.framework.valuation.lp_position_reader import _parse_position_hex

        # 12 words all zero
        hex_data = "0x" + "0" * 64 * 12
        result = _parse_position_hex(hex_data, token_id=99)
        assert result is not None
        assert result.liquidity == 0
        assert result.is_active is False


class TestLPPositionReaderIntegration:
    """Test LPPositionReader with mock gateway."""

    def test_no_gateway_returns_none(self):
        from almanak.framework.valuation.lp_position_reader import LPPositionReader

        reader = LPPositionReader(gateway_client=None)
        result = reader.read_position("ethereum", 12345)
        assert result is None

    def test_no_gateway_slot0_returns_none(self):
        from almanak.framework.valuation.lp_position_reader import LPPositionReader

        reader = LPPositionReader(gateway_client=None)
        result = reader.read_pool_slot0("ethereum", "0x1234")
        assert result is None

    def test_resolve_uniswap_v3_address(self):
        from almanak.framework.valuation.lp_position_reader import LPPositionReader

        reader = LPPositionReader(gateway_client=None)
        addr = reader._resolve_position_manager("ethereum", "uniswap_v3")
        assert addr == "0xC36442b4a4522E871399CD717aBDD847Ab11FE88"

    def test_resolve_sushiswap_v3_address(self):
        from almanak.framework.valuation.lp_position_reader import LPPositionReader

        reader = LPPositionReader(gateway_client=None)
        addr = reader._resolve_position_manager("ethereum", "sushiswap_v3")
        assert addr == "0x2214A42d8e2A1d20635c2cb0664422c528B6A432"

    def test_resolve_unknown_protocol_falls_back(self):
        from almanak.framework.valuation.lp_position_reader import LPPositionReader

        reader = LPPositionReader(gateway_client=None)
        addr = reader._resolve_position_manager("ethereum", "unknown_protocol")
        # Falls back to Uniswap V3 address
        assert addr == "0xC36442b4a4522E871399CD717aBDD847Ab11FE88"


class TestPortfolioValuerLPRepricing:
    """Test that PortfolioValuer re-prices LP positions."""

    def _make_strategy(
        self,
        positions=None,
        tracked_tokens=None,
    ):
        """Create a mock strategy for testing."""
        from unittest.mock import MagicMock

        from almanak.framework.teardown.models import PositionInfo, PositionType, TeardownPositionSummary

        strategy = MagicMock()
        strategy.strategy_id = "test-strategy"
        strategy.chain = "ethereum"
        strategy._get_tracked_tokens.return_value = tracked_tokens or ["ETH", "USDC"]

        if positions is not None:
            summary = TeardownPositionSummary(
                strategy_id="test-strategy",
                timestamp=None,
                positions=positions,
            )
            strategy.get_open_positions.return_value = summary
        else:
            del strategy.get_open_positions

        return strategy

    def _make_market(self, prices=None, balances=None):
        from unittest.mock import MagicMock

        market = MagicMock()
        _prices = prices or {}
        _balances = balances or {}

        def mock_price(token, quote="USD"):
            if token in _prices:
                return _prices[token]
            raise ValueError(f"No price for {token}")

        def mock_balance(token):
            return _balances.get(token, Decimal("0"))

        market.price.side_effect = mock_price
        market.balance.side_effect = mock_balance
        return market

    def test_lp_position_falls_back_to_strategy_value_without_gateway(self):
        """Without gateway client, LP positions use strategy-reported value."""
        from almanak.framework.teardown.models import PositionInfo, PositionType
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        strategy = self._make_strategy(
            positions=[
                PositionInfo(
                    position_type=PositionType.LP,
                    position_id="12345",
                    chain="ethereum",
                    protocol="uniswap_v3",
                    value_usd=Decimal("5000"),
                    details={
                        "token0": "WETH",
                        "token1": "USDC",
                        "pool": "0xpool",
                    },
                )
            ],
        )

        market = self._make_market(
            prices={"ETH": Decimal("3000"), "USDC": Decimal("1")},
            balances={"ETH": Decimal("0"), "USDC": Decimal("0")},
        )

        valuer = PortfolioValuer()  # No gateway client
        snapshot = valuer.value(strategy, market)

        # Should fall back to strategy-reported $5000
        assert snapshot.total_value_usd == Decimal("5000")
        assert len(snapshot.positions) == 1
        assert snapshot.positions[0].value_usd == Decimal("5000")

    def test_non_lp_position_passes_through(self):
        """Non-LP positions (TOKEN, SUPPLY, etc.) always use strategy value."""
        from almanak.framework.teardown.models import PositionInfo, PositionType
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        strategy = self._make_strategy(
            positions=[
                PositionInfo(
                    position_type=PositionType.TOKEN,
                    position_id="token-pos",
                    chain="ethereum",
                    protocol="wallet",
                    value_usd=Decimal("1000"),
                    details={},
                )
            ],
        )

        market = self._make_market(
            prices={"ETH": Decimal("3000"), "USDC": Decimal("1")},
            balances={"ETH": Decimal("0"), "USDC": Decimal("0")},
        )

        valuer = PortfolioValuer()
        snapshot = valuer.value(strategy, market)
        assert snapshot.positions[0].value_usd == Decimal("1000")

    def test_set_gateway_client(self):
        """set_gateway_client updates the LP reader."""
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        valuer = PortfolioValuer()
        assert valuer._lp_reader._gateway is None

        mock_gw = object()
        valuer.set_gateway_client(mock_gw)
        assert valuer._lp_reader._gateway is mock_gw

    def test_extract_token_id_numeric(self):
        """Numeric position_id is extracted correctly."""
        from unittest.mock import MagicMock

        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        pos = MagicMock()
        pos.position_id = "12345"
        pos.details = {}

        assert PortfolioValuer._extract_token_id(pos) == 12345

    def test_extract_token_id_non_numeric(self):
        """Non-numeric position_id returns None."""
        from unittest.mock import MagicMock

        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        pos = MagicMock()
        pos.position_id = "aerodrome-lp-0xpool-base"
        pos.details = {}

        assert PortfolioValuer._extract_token_id(pos) is None

    def test_extract_token_id_from_details(self):
        """Token ID in details dict is found."""
        from unittest.mock import MagicMock

        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        pos = MagicMock()
        pos.position_id = "some-label"
        pos.details = {"token_id": 99999}

        assert PortfolioValuer._extract_token_id(pos) == 99999

    def test_price_ratio_to_tick_eth_usdc(self):
        """ETH=$3000, USDC=$1 should give reasonable tick for WETH/USDC pool."""
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        tick = PortfolioValuer._price_ratio_to_tick(
            token0_price=Decimal("3000"),
            token1_price=Decimal("1"),
            token0_decimals=18,
            token1_decimals=6,
        )
        # WETH/USDC tick for price ~3000 with decimal adjustment is large positive
        assert isinstance(tick, int)
        # Should be in valid tick range
        assert -887272 <= tick <= 887272

    def test_wallet_plus_lp_position_total(self):
        """Total value should include both wallet and position values."""
        from almanak.framework.teardown.models import PositionInfo, PositionType
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        strategy = self._make_strategy(
            positions=[
                PositionInfo(
                    position_type=PositionType.LP,
                    position_id="lp-label",  # Non-numeric, will use fallback
                    chain="ethereum",
                    protocol="uniswap_v3",
                    value_usd=Decimal("5000"),
                    details={},
                )
            ],
        )

        market = self._make_market(
            prices={"ETH": Decimal("3000"), "USDC": Decimal("1")},
            balances={"ETH": Decimal("1"), "USDC": Decimal("500")},
        )

        valuer = PortfolioValuer()
        snapshot = valuer.value(strategy, market)

        # Wallet: 1 ETH * $3000 + 500 USDC * $1 = $3500
        # Position: $5000 (fallback)
        assert snapshot.total_value_usd == Decimal("8500")
        assert snapshot.available_cash_usd == Decimal("3500")

    def test_lp_repricing_with_mocked_on_chain_data(self):
        """End-to-end test: mock gateway returns position data, V3 math re-prices."""
        from unittest.mock import MagicMock, patch

        from almanak.framework.teardown.models import PositionInfo, PositionType
        from almanak.framework.valuation.lp_position_reader import LPPositionOnChain, PoolSlot0
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        strategy = self._make_strategy(
            positions=[
                PositionInfo(
                    position_type=PositionType.LP,
                    position_id="12345",
                    chain="ethereum",
                    protocol="uniswap_v3",
                    value_usd=Decimal("999"),  # Strategy-reported (should be overridden)
                    details={
                        "token0": "WETH",
                        "token1": "USDC",
                        "pool": "0xPoolAddress",
                    },
                )
            ],
        )

        market = self._make_market(
            prices={"ETH": Decimal("3000"), "USDC": Decimal("1"), "WETH": Decimal("3000")},
            balances={"ETH": Decimal("0"), "USDC": Decimal("0")},
        )

        # Mock on-chain position data
        mock_position = LPPositionOnChain(
            token_id=12345,
            token0="0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
            token1="0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            fee=3000,
            tick_lower=-887220,
            tick_upper=887220,
            liquidity=10_000_000_000_000_000,
            tokens_owed0=0,
            tokens_owed1=0,
        )
        mock_slot0 = PoolSlot0(sqrt_price_x96=2**96, tick=0)

        valuer = PortfolioValuer()

        with (
            patch.object(valuer._lp_reader, "read_position", return_value=mock_position),
            patch.object(valuer._lp_reader, "read_pool_slot0", return_value=mock_slot0),
            patch(
                "almanak.framework.valuation.portfolio_valuer.PortfolioValuer._get_token_decimals",
                side_effect=lambda symbol, chain: 18 if symbol == "WETH" else 6,
            ),
        ):
            snapshot = valuer.value(strategy, market)

        # Compute expected value deterministically using same inputs
        from almanak.framework.valuation.lp_valuer import value_lp_position as vlp

        expected = vlp(
            liquidity=10_000_000_000_000_000,
            tick_lower=-887220,
            tick_upper=887220,
            current_tick=0,
            token0_price_usd=Decimal("3000"),
            token1_price_usd=Decimal("1"),
            token0_decimals=18,
            token1_decimals=6,
            sqrt_price_x96=2**96,
        )

        assert len(snapshot.positions) == 1
        repriced_value = snapshot.positions[0].value_usd
        assert repriced_value == expected.value_usd, (
            f"Expected ${expected.value_usd}, got ${repriced_value}"
        )
        assert repriced_value > 0
        assert repriced_value != Decimal("999"), "Should not be strategy fallback"

    def test_unknown_decimals_falls_back_to_strategy_value(self):
        """When token decimals are unknown, falls back to strategy-reported value."""
        from unittest.mock import patch

        from almanak.framework.teardown.models import PositionInfo, PositionType
        from almanak.framework.valuation.lp_position_reader import LPPositionOnChain
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        strategy = self._make_strategy(
            positions=[
                PositionInfo(
                    position_type=PositionType.LP,
                    position_id="12345",
                    chain="ethereum",
                    protocol="uniswap_v3",
                    value_usd=Decimal("7777"),
                    details={"token0": "UNKNOWN_TOKEN", "token1": "USDC", "pool": "0xPool"},
                )
            ],
        )

        market = self._make_market(
            prices={"ETH": Decimal("3000"), "USDC": Decimal("1"), "UNKNOWN_TOKEN": Decimal("50")},
            balances={"ETH": Decimal("0"), "USDC": Decimal("0")},
        )

        mock_position = LPPositionOnChain(
            token_id=12345, token0="0x1111", token1="0x2222",
            fee=3000, tick_lower=-100, tick_upper=100, liquidity=1000,
            tokens_owed0=0, tokens_owed1=0,
        )

        valuer = PortfolioValuer()

        with (
            patch.object(valuer._lp_reader, "read_position", return_value=mock_position),
            patch(
                "almanak.framework.valuation.portfolio_valuer.PortfolioValuer._get_token_decimals",
                return_value=None,  # Unknown decimals
            ),
        ):
            snapshot = valuer.value(strategy, market)

        # Should fall back to strategy-reported $7777
        assert snapshot.positions[0].value_usd == Decimal("7777")

    def test_fee_only_position_valued_correctly(self):
        """Position with zero liquidity but uncollected fees is valued by fees alone."""
        from unittest.mock import patch

        from almanak.framework.teardown.models import PositionInfo, PositionType
        from almanak.framework.valuation.lp_position_reader import LPPositionOnChain, PoolSlot0
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        strategy = self._make_strategy(
            positions=[
                PositionInfo(
                    position_type=PositionType.LP,
                    position_id="55555",
                    chain="ethereum",
                    protocol="uniswap_v3",
                    value_usd=Decimal("0"),  # Strategy reports $0
                    details={"token0": "WETH", "token1": "USDC", "pool": "0xPool"},
                )
            ],
        )

        market = self._make_market(
            prices={"ETH": Decimal("3000"), "USDC": Decimal("1"), "WETH": Decimal("3000")},
            balances={"ETH": Decimal("0"), "USDC": Decimal("0")},
        )

        # Zero liquidity but 0.5 WETH and 1000 USDC in uncollected fees
        mock_position = LPPositionOnChain(
            token_id=55555,
            token0="0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
            token1="0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            fee=3000,
            tick_lower=-887220,
            tick_upper=887220,
            liquidity=0,  # No liquidity
            tokens_owed0=500_000_000_000_000_000,  # 0.5 WETH (18 decimals)
            tokens_owed1=1_000_000_000,  # 1000 USDC (6 decimals)
        )
        mock_slot0 = PoolSlot0(sqrt_price_x96=2**96, tick=0)

        valuer = PortfolioValuer()

        with (
            patch.object(valuer._lp_reader, "read_position", return_value=mock_position),
            patch.object(valuer._lp_reader, "read_pool_slot0", return_value=mock_slot0),
            patch(
                "almanak.framework.valuation.portfolio_valuer.PortfolioValuer._get_token_decimals",
                side_effect=lambda symbol, chain: 18 if symbol == "WETH" else 6,
            ),
        ):
            snapshot = valuer.value(strategy, market)

        # Value = 0.5 WETH * $3000 + 1000 USDC * $1 = $1500 + $1000 = $2500
        assert len(snapshot.positions) == 1
        expected_fees = Decimal("0.5") * Decimal("3000") + Decimal("1000") * Decimal("1")
        assert snapshot.positions[0].value_usd == expected_fees
