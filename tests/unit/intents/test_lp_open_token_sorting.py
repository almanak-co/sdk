"""Tests for LP_OPEN price range inversion when tokens are sorted by address (VIB-181).

When _parse_pool_info() reorders tokens so that token0.address < token1.address,
the price range and amounts must be inverted/swapped accordingly.
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.intents.compiler import IntentCompiler


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_token_info(symbol: str, address: str, decimals: int = 18):
    """Create a lightweight TokenInfo-like object."""
    ti = MagicMock()
    ti.symbol = symbol
    ti.address = address
    ti.decimals = decimals
    ti.is_native = False
    return ti


# Two tokens where the "natural" order (first listed) differs from address order.
# WBNB address (0xbb...) > USDT address (0x55...)
WBNB = _make_token_info("WBNB", "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c", 18)
USDT = _make_token_info("USDT", "0x55d398326f99059fF775485246999027B3197955", 18)

# Two tokens already in sorted order (USDC addr < WETH addr, typical on Arbitrum)
USDC = _make_token_info("USDC", "0xaf88d065e77c8cC2239327C5EDb3A432268e5831", 6)
WETH = _make_token_info("WETH", "0xfFf9976782d46CC05630D1f6eBAb18b2324d6B14", 18)


# ---------------------------------------------------------------------------
# _parse_pool_info tests
# ---------------------------------------------------------------------------


class TestParsePoolInfoTokenSwapFlag:
    """Verify _parse_pool_info returns correct tokens_swapped flag."""

    @pytest.fixture()
    def compiler(self):
        c = IntentCompiler.__new__(IntentCompiler)
        c.chain = "bsc"
        return c

    def test_no_swap_when_already_sorted(self, compiler):
        """Tokens already in address order -> tokens_swapped=False."""
        with patch.object(compiler, "_resolve_token", side_effect=[USDC, WETH]):
            result = compiler._parse_pool_info("USDC/WETH/3000")
        assert result is not None
        token0, token1, fee, swapped = result
        assert token0.symbol == "USDC"
        assert token1.symbol == "WETH"
        assert fee == 3000
        assert swapped is False

    def test_swap_when_reverse_order(self, compiler):
        """Tokens in reverse address order -> tokens_swapped=True, tokens reordered."""
        with patch.object(compiler, "_resolve_token", side_effect=[WBNB, USDT]):
            result = compiler._parse_pool_info("WBNB/USDT/500")
        assert result is not None
        token0, token1, fee, swapped = result
        # USDT has lower address -> becomes token0
        assert token0.symbol == "USDT"
        assert token1.symbol == "WBNB"
        assert fee == 500
        assert swapped is True

    def test_pool_address_format_returns_false(self, compiler):
        """Pool address format (0x...) always returns tokens_swapped=False."""
        with patch.object(compiler, "_resolve_token", side_effect=[WETH, USDC]):
            result = compiler._parse_pool_info("0xABCDEF1234567890ABCDEF1234567890ABCDEF12")
        assert result is not None
        _, _, _, swapped = result
        assert swapped is False

    def test_default_fee_tier_without_fee(self, compiler):
        """Pool without fee part -> default 3000."""
        with patch.object(compiler, "_resolve_token", side_effect=[USDC, WETH]):
            result = compiler._parse_pool_info("USDC/WETH")
        assert result is not None
        _, _, fee, _ = result
        assert fee == 3000

    def test_returns_four_element_tuple(self, compiler):
        """Return type is a 4-tuple (token0, token1, fee, swapped)."""
        with patch.object(compiler, "_resolve_token", side_effect=[USDC, WETH]):
            result = compiler._parse_pool_info("USDC/WETH/3000")
        assert result is not None
        assert len(result) == 4

    def test_same_address_no_swap(self, compiler):
        """Edge case: identical addresses (shouldn't happen, but no crash)."""
        same = _make_token_info("A", "0xaaaa", 18)
        same2 = _make_token_info("B", "0xaaaa", 18)
        with patch.object(compiler, "_resolve_token", side_effect=[same, same2]):
            result = compiler._parse_pool_info("A/B/3000")
        assert result is not None
        _, _, _, swapped = result
        assert swapped is False


# ---------------------------------------------------------------------------
# Price range inversion math tests
# ---------------------------------------------------------------------------


class TestPriceRangeInversionMath:
    """Verify the mathematical correctness of price range inversion."""

    def test_basic_inversion(self):
        """1/upper < 1/lower for any 0 < lower < upper."""
        lower = Decimal("550")
        upper = Decimal("670")
        new_lower = Decimal(1) / upper
        new_upper = Decimal(1) / lower
        assert new_lower < new_upper

    def test_round_trip(self):
        """Inverting twice returns the original values."""
        lower = Decimal("550")
        upper = Decimal("670")
        inv_lower = Decimal(1) / upper
        inv_upper = Decimal(1) / lower
        restored_lower = Decimal(1) / inv_upper
        restored_upper = Decimal(1) / inv_lower
        assert abs(restored_lower - lower) < Decimal("1e-20")
        assert abs(restored_upper - upper) < Decimal("1e-20")

    def test_preserves_ordering_various_ranges(self):
        """Inversion preserves lower < upper for various ranges."""
        test_cases = [
            (Decimal("0.001"), Decimal("100")),
            (Decimal("550"), Decimal("670")),
            (Decimal("0.0001"), Decimal("0.001")),
            (Decimal("1"), Decimal("2")),
            (Decimal("0.000001"), Decimal("1000000")),
        ]
        for lower, upper in test_cases:
            new_lower = Decimal(1) / upper
            new_upper = Decimal(1) / lower
            assert new_lower < new_upper, f"Failed for ({lower}, {upper})"

    def test_narrow_range(self):
        """Very narrow range inverts correctly."""
        lower = Decimal("3399.99")
        upper = Decimal("3400.01")
        new_lower = Decimal(1) / upper
        new_upper = Decimal(1) / lower
        assert new_lower < new_upper
        assert new_lower > 0
        assert new_upper > 0


# ---------------------------------------------------------------------------
# Integration: verify inversion is wired into _compile_lp_open
# ---------------------------------------------------------------------------


class TestCompileLPOpenInversion:
    """Verify _compile_lp_open applies inversion when tokens_swapped=True."""

    @pytest.fixture()
    def compiler(self):
        c = IntentCompiler.__new__(IntentCompiler)
        c.chain = "bsc"
        c.default_lp_slippage = Decimal("0.20")
        c.default_deadline_seconds = 600
        c.wallet_address = "0x" + "11" * 20
        return c

    def _make_lp_intent(self, *, range_lower, range_upper, amount0, amount1, pool="WBNB/USDT/500"):
        intent = MagicMock()
        intent.pool = pool
        intent.protocol = "pancakeswap_v3"
        intent.range_lower = Decimal(str(range_lower))
        intent.range_upper = Decimal(str(range_upper))
        intent.amount0 = Decimal(str(amount0))
        intent.amount1 = Decimal(str(amount1))
        intent.intent_id = "test-intent-001"
        intent.max_slippage = None
        return intent

    @patch("almanak.framework.intents.pool_validation.validate_v3_pool")
    @patch("almanak.framework.intents.compiler.UniswapV3LPAdapter")
    def test_ticks_use_inverted_prices(self, MockAdapter, mock_validate, compiler):
        """_price_to_tick receives inverted prices when tokens were swapped."""
        mock_adapter = MockAdapter.return_value
        mock_adapter.get_position_manager_address.return_value = "0x" + "AA" * 20
        mock_adapter.get_mint_calldata.return_value = b"\x00" * 32
        mock_adapter.estimate_mint_gas.return_value = 500000
        mock_validate.return_value = MagicMock(is_valid=True)

        intent = self._make_lp_intent(
            range_lower=550,
            range_upper=670,
            amount0=Decimal("0.165"),
            amount1=Decimal("100"),
        )

        tick_calls = []
        original_price_to_tick = IntentCompiler._price_to_tick

        def tracking_price_to_tick(price, token0_decimals=18, token1_decimals=18):
            tick_calls.append(price)
            return original_price_to_tick(price, token0_decimals, token1_decimals)

        with (
            patch.object(compiler, "_parse_pool_info", return_value=(USDT, WBNB, 500, True)),
            patch.object(compiler, "_validate_pool", return_value=None),
            patch.object(compiler, "_get_chain_rpc_url", return_value="http://localhost:8545"),
            patch.object(IntentCompiler, "_price_to_tick", side_effect=tracking_price_to_tick),
            patch.object(compiler, "_build_approve_tx", return_value=[]),
        ):
            compiler._compile_lp_open(intent)

        assert len(tick_calls) == 2
        expected_lower = Decimal(1) / Decimal("670")
        expected_upper = Decimal(1) / Decimal("550")
        assert tick_calls[0] == expected_lower
        assert tick_calls[1] == expected_upper

    @patch("almanak.framework.intents.pool_validation.validate_v3_pool")
    @patch("almanak.framework.intents.compiler.UniswapV3LPAdapter")
    def test_amounts_swapped_when_tokens_swapped(self, MockAdapter, mock_validate, compiler):
        """amount0 and amount1 are swapped when tokens are reordered."""
        mock_adapter = MockAdapter.return_value
        mock_adapter.get_position_manager_address.return_value = "0x" + "AA" * 20
        mock_adapter.get_mint_calldata.return_value = b"\x00" * 32
        mock_adapter.estimate_mint_gas.return_value = 500000
        mock_validate.return_value = MagicMock(is_valid=True)

        intent = self._make_lp_intent(
            range_lower=550,
            range_upper=670,
            amount0=Decimal("0.165"),  # user's WBNB
            amount1=Decimal("100"),    # user's USDT
        )

        approve_amounts = {}

        def mock_build_approve(token_addr, spender, amount):
            approve_amounts[token_addr] = amount
            return []

        with (
            patch.object(compiler, "_parse_pool_info", return_value=(USDT, WBNB, 500, True)),
            patch.object(compiler, "_validate_pool", return_value=None),
            patch.object(compiler, "_get_chain_rpc_url", return_value="http://localhost:8545"),
            patch.object(IntentCompiler, "_price_to_tick", return_value=0),
            patch.object(compiler, "_build_approve_tx", side_effect=mock_build_approve),
        ):
            compiler._compile_lp_open(intent)

        # After swap: USDT is token0 (gets amount1=100), WBNB is token1 (gets amount0=0.165)
        assert approve_amounts[USDT.address] == int(Decimal("100") * Decimal(10**18))
        assert approve_amounts[WBNB.address] == int(Decimal("0.165") * Decimal(10**18))

    @patch("almanak.framework.intents.pool_validation.validate_v3_pool")
    @patch("almanak.framework.intents.compiler.UniswapV3LPAdapter")
    def test_no_inversion_when_not_swapped(self, MockAdapter, mock_validate, compiler):
        """When tokens are NOT swapped, range and amounts stay as-is."""
        mock_adapter = MockAdapter.return_value
        mock_adapter.get_position_manager_address.return_value = "0x" + "AA" * 20
        mock_adapter.get_mint_calldata.return_value = b"\x00" * 32
        mock_adapter.estimate_mint_gas.return_value = 500000
        mock_validate.return_value = MagicMock(is_valid=True)

        intent = self._make_lp_intent(
            pool="USDC/WETH/3000",
            range_lower=3000,
            range_upper=4000,
            amount0=Decimal("1000"),
            amount1=Decimal("0.5"),
        )

        tick_calls = []
        original_price_to_tick = IntentCompiler._price_to_tick

        def tracking_price_to_tick(price, token0_decimals=18, token1_decimals=18):
            tick_calls.append(price)
            return original_price_to_tick(price, token0_decimals, token1_decimals)

        with (
            patch.object(compiler, "_parse_pool_info", return_value=(USDC, WETH, 3000, False)),
            patch.object(compiler, "_validate_pool", return_value=None),
            patch.object(compiler, "_get_chain_rpc_url", return_value="http://localhost:8545"),
            patch.object(IntentCompiler, "_price_to_tick", side_effect=tracking_price_to_tick),
            patch.object(compiler, "_build_approve_tx", return_value=[]),
        ):
            compiler._compile_lp_open(intent)

        assert len(tick_calls) == 2
        assert tick_calls[0] == Decimal("3000")
        assert tick_calls[1] == Decimal("4000")
