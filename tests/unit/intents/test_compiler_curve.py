"""Unit tests for Curve Finance intent compilation paths.

Tests verify that IntentCompiler correctly compiles SwapIntent, LPOpenIntent,
and LPCloseIntent for the curve protocol by mocking the CurveAdapter.
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.intents import LPCloseIntent, LPOpenIntent, SwapIntent
from almanak.framework.intents.compiler import (
    CompilationStatus,
    IntentCompiler,
    IntentCompilerConfig,
)

# Patch targets — lazy-imported from the source module inside compile methods.
# When compiler does `from almanak.framework.connectors.curve.adapter import CurveAdapter`,
# it fetches the object from the source module, so we patch there.
ADAPTER_MODULE = "almanak.framework.connectors.curve.adapter"
CURVE_ADAPTER_CLS = f"{ADAPTER_MODULE}.CurveAdapter"
CURVE_CONFIG_CLS = f"{ADAPTER_MODULE}.CurveConfig"
CURVE_POOLS_PATH = f"{ADAPTER_MODULE}.CURVE_POOLS"
CURVE_ADDRESSES_PATH = f"{ADAPTER_MODULE}.CURVE_ADDRESSES"

# Minimal CURVE_POOLS fixture — uses USDC/USDT/DAI which exist in the token registry.
# FRAX is not in the default registry, so we avoid it for unit tests.
MOCK_CURVE_POOLS = {
    "ethereum": {
        "usdc_usdt": {
            "address": "0xDcEF968d416a41Cdac0ED8702fAC8128A64241A2",
            "lp_token": "0x3175Df0976dFA876431C2E9eE6Bc45b65d3473CC",
            "coins": ["USDC", "USDT"],
            "coin_addresses": [
                "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
                "0xdAC17F958D2ee523a2206206994597C13D831ec7",
            ],
            "pool_type": "stableswap",
            "n_coins": 2,
        },
        "3pool": {
            "address": "0xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7",
            "lp_token": "0x6c3F90f043a72FA612cbac8115EE7e52BDe6E490",
            "coins": ["DAI", "USDC", "USDT"],
            "coin_addresses": [
                "0x6B175474E89094C44Da98b954EedeAC495271d0F",
                "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
                "0xdAC17F958D2ee523a2206206994597C13D831ec7",
            ],
            "pool_type": "stableswap",
            "n_coins": 3,
        },
    }
}

MOCK_CURVE_ADDRESSES = {
    "ethereum": {
        "router": "0x16C6521Dff6baB339122a0FE25a9116693265353",
        "address_provider": "0x5ffe7FB82894076ECB99A30D6A32e969e6e35E98",
        "stableswap_factory": "0x6A8cbed756804B16E05E741eDaBd5cB544AE21bf",
        "twocrypto_factory": "0x98EE851a00abeE0d95D08cF4CA2BdCE32aeaAF7F",
        "tricrypto_factory": "0x0c0e5f2fF0ff18a3be9b835635039256dC4B4963",
        "crv_token": "0xD533a949740bb3306d119CC777fa900bA034cd52",
    },
}

TEST_WALLET = "0x1234567890123456789012345678901234567890"
USDC_USDT_POOL = "0xDcEF968d416a41Cdac0ED8702fAC8128A64241A2"
THREEPOOL = "0xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7"


def _make_mock_tx(description: str = "Curve swap USDC -> USDT", gas: int = 246_000) -> MagicMock:
    """Create a mock TransactionData (Curve adapter format)."""
    tx = MagicMock()
    tx.gas_estimate = gas
    tx.description = description
    tx.to_dict.return_value = {
        "to": USDC_USDT_POOL,
        "value": "0",
        "data": "0x3df02124" + "00" * 128,
        "gas_estimate": gas,
        "description": description,
        "tx_type": "swap",
    }
    return tx


def _make_mock_swap_result(success: bool = True, error: str | None = None) -> MagicMock:
    """Create a mock SwapResult."""
    result = MagicMock()
    result.success = success
    result.error = error
    if success:
        approve_tx = _make_mock_tx("Approve USDC", 46_000)
        swap_tx = _make_mock_tx("Curve swap USDC -> USDT", 200_000)
        result.transactions = [approve_tx, swap_tx]
    else:
        result.transactions = []
    return result


def _make_mock_liq_result(success: bool = True, error: str | None = None, op: str = "add_liquidity") -> MagicMock:
    """Create a mock LiquidityResult."""
    result = MagicMock()
    result.success = success
    result.error = error
    if success:
        tx = _make_mock_tx(f"Curve {op}", 250_000)
        result.transactions = [tx]
    else:
        result.transactions = []
    return result


@pytest.fixture
def compiler():
    """IntentCompiler for Ethereum with placeholder prices."""
    config = IntentCompilerConfig(allow_placeholder_prices=True)
    return IntentCompiler(chain="ethereum", wallet_address=TEST_WALLET, config=config)


# =============================================================================
# SWAP
# =============================================================================


class TestCurveSwap:
    """Tests for _compile_swap_curve routing."""

    @patch(CURVE_POOLS_PATH, MOCK_CURVE_POOLS)
    @patch(CURVE_ADDRESSES_PATH, MOCK_CURVE_ADDRESSES)
    @patch(CURVE_ADAPTER_CLS)
    @patch(CURVE_CONFIG_CLS)
    def test_swap_success_auto_pool_lookup(self, mock_config_cls, mock_adapter_cls, compiler):
        """Curve swap auto-selects pool by token pair and returns success."""
        mock_adapter = MagicMock()
        mock_adapter.swap.return_value = _make_mock_swap_result(success=True)
        mock_adapter_cls.return_value = mock_adapter

        intent = SwapIntent(
            from_token="USDC",
            to_token="USDT",
            amount_usd=Decimal("1000"),
            protocol="curve",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle is not None
        assert result.action_bundle.metadata["pool_name"] == "usdc_usdt"
        assert result.action_bundle.metadata["protocol"] == "curve"
        assert len(result.action_bundle.transactions) == 2  # approve + swap
        mock_adapter.swap.assert_called_once()

    @patch(CURVE_POOLS_PATH, MOCK_CURVE_POOLS)
    @patch(CURVE_ADDRESSES_PATH, MOCK_CURVE_ADDRESSES)
    @patch(CURVE_ADAPTER_CLS)
    @patch(CURVE_CONFIG_CLS)
    def test_swap_with_token_amount(self, mock_config_cls, mock_adapter_cls, compiler):
        """Curve swap works with direct token amount (not USD)."""
        mock_adapter = MagicMock()
        mock_adapter.swap.return_value = _make_mock_swap_result(success=True)
        mock_adapter_cls.return_value = mock_adapter

        intent = SwapIntent(
            from_token="USDC",
            to_token="USDT",
            amount=Decimal("500"),
            protocol="curve",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS
        call_kwargs = mock_adapter.swap.call_args
        # amount_in should be 500 (the direct token amount)
        assert call_kwargs.kwargs["amount_in"] == Decimal("500")

    @patch(CURVE_POOLS_PATH, MOCK_CURVE_POOLS)
    @patch(CURVE_ADDRESSES_PATH, MOCK_CURVE_ADDRESSES)
    def test_swap_no_pool_found_returns_failed(self, compiler):
        """Swap fails with helpful error when no pool matches the token pair."""
        intent = SwapIntent(
            from_token="WETH",
            to_token="WBTC",

            amount_usd=Decimal("1000"),
            protocol="curve",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "No Curve pool found" in result.error
        assert "WETH" in result.error or "WBTC" in result.error

    @patch(CURVE_POOLS_PATH, MOCK_CURVE_POOLS)
    @patch(CURVE_ADDRESSES_PATH, {})
    def test_swap_unsupported_chain_returns_failed(self):
        """Swap fails when chain is not supported by Curve."""
        config = IntentCompilerConfig(allow_placeholder_prices=True)
        compiler = IntentCompiler(chain="avalanche", wallet_address=TEST_WALLET, config=config)

        intent = SwapIntent(
            from_token="USDC",
            to_token="USDT",
            amount_usd=Decimal("1000"),
            protocol="curve",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "Curve is not supported on avalanche" in result.error

    @patch(CURVE_POOLS_PATH, MOCK_CURVE_POOLS)
    @patch(CURVE_ADDRESSES_PATH, MOCK_CURVE_ADDRESSES)
    @patch(CURVE_ADAPTER_CLS)
    @patch(CURVE_CONFIG_CLS)
    def test_swap_adapter_failure_propagates(self, mock_config_cls, mock_adapter_cls, compiler):
        """Adapter failure is propagated as CompilationStatus.FAILED."""
        mock_adapter = MagicMock()
        mock_adapter.swap.return_value = _make_mock_swap_result(success=False, error="pool is paused")
        mock_adapter_cls.return_value = mock_adapter

        intent = SwapIntent(
            from_token="USDC",
            to_token="USDT",
            amount_usd=Decimal("1000"),
            protocol="curve",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "pool is paused" in result.error


# =============================================================================
# LP OPEN
# =============================================================================


class TestCurveLPOpen:
    """Tests for _compile_lp_open_curve routing."""

    @patch(CURVE_POOLS_PATH, MOCK_CURVE_POOLS)
    @patch(CURVE_ADDRESSES_PATH, MOCK_CURVE_ADDRESSES)
    @patch(CURVE_ADAPTER_CLS)
    @patch(CURVE_CONFIG_CLS)
    def test_lp_open_by_pool_name_success(self, mock_config_cls, mock_adapter_cls, compiler):
        """LP open with pool name auto-resolves to address."""
        mock_adapter = MagicMock()
        mock_adapter.add_liquidity.return_value = _make_mock_liq_result(success=True)
        mock_adapter_cls.return_value = mock_adapter

        intent = LPOpenIntent(
            pool="usdc_usdt",
            amount0=Decimal("500"),
            amount1=Decimal("500"),
            range_lower=Decimal("1"),
            range_upper=Decimal("2"),
            protocol="curve",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle is not None
        assert result.action_bundle.metadata["pool_name"] == "usdc_usdt"
        assert result.action_bundle.metadata["protocol"] == "curve"
        mock_adapter.add_liquidity.assert_called_once()

    @patch(CURVE_POOLS_PATH, MOCK_CURVE_POOLS)
    @patch(CURVE_ADDRESSES_PATH, MOCK_CURVE_ADDRESSES)
    @patch(CURVE_ADAPTER_CLS)
    @patch(CURVE_CONFIG_CLS)
    def test_lp_open_by_pool_address_success(self, mock_config_cls, mock_adapter_cls, compiler):
        """LP open with explicit pool address also resolves correctly."""
        mock_adapter = MagicMock()
        mock_adapter.add_liquidity.return_value = _make_mock_liq_result(success=True)
        mock_adapter_cls.return_value = mock_adapter

        intent = LPOpenIntent(
            pool=USDC_USDT_POOL,
            amount0=Decimal("500"),
            amount1=Decimal("500"),
            range_lower=Decimal("1"),
            range_upper=Decimal("2"),
            protocol="curve",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle.metadata["pool_name"] == "usdc_usdt"

    @patch(CURVE_POOLS_PATH, MOCK_CURVE_POOLS)
    @patch(CURVE_ADDRESSES_PATH, MOCK_CURVE_ADDRESSES)
    @patch(CURVE_ADAPTER_CLS)
    @patch(CURVE_CONFIG_CLS)
    def test_lp_open_3pool_pads_amounts(self, mock_config_cls, mock_adapter_cls, compiler):
        """For 3-coin pools, amounts list is padded to n_coins with 0."""
        mock_adapter = MagicMock()
        mock_adapter.add_liquidity.return_value = _make_mock_liq_result(success=True)
        mock_adapter_cls.return_value = mock_adapter

        intent = LPOpenIntent(
            pool="3pool",
            amount0=Decimal("500"),  # DAI
            amount1=Decimal("500"),  # USDC
            range_lower=Decimal("1"),
            range_upper=Decimal("2"),
            protocol="curve",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS
        call_kwargs = mock_adapter.add_liquidity.call_args
        amounts = call_kwargs.kwargs["amounts"]
        assert len(amounts) == 3  # padded to n_coins=3
        assert amounts[2] == Decimal("0")  # third coin = 0

    @patch(CURVE_POOLS_PATH, MOCK_CURVE_POOLS)
    @patch(CURVE_ADDRESSES_PATH, MOCK_CURVE_ADDRESSES)
    def test_lp_open_unknown_pool_returns_failed(self, compiler):
        """LP open fails with helpful error for unknown pool."""
        intent = LPOpenIntent(
            pool="nonexistent_pool",
            amount0=Decimal("500"),
            amount1=Decimal("500"),
            range_lower=Decimal("1"),
            range_upper=Decimal("2"),
            protocol="curve",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "Unknown Curve pool" in result.error


# =============================================================================
# LP CLOSE
# =============================================================================


class TestCurveLPClose:
    """Tests for _compile_lp_close_curve routing."""

    @patch(CURVE_POOLS_PATH, MOCK_CURVE_POOLS)
    @patch(CURVE_ADDRESSES_PATH, MOCK_CURVE_ADDRESSES)
    @patch(CURVE_ADAPTER_CLS)
    @patch(CURVE_CONFIG_CLS)
    def test_lp_close_by_pool_name_success(self, mock_config_cls, mock_adapter_cls, compiler):
        """LP close by pool name succeeds and parses position_id as LP amount."""
        mock_adapter = MagicMock()
        mock_adapter.remove_liquidity.return_value = _make_mock_liq_result(success=True, op="remove_liquidity")
        mock_adapter_cls.return_value = mock_adapter

        intent = LPCloseIntent(
            position_id="100.5",
            pool="usdc_usdt",
            protocol="curve",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle is not None
        assert result.action_bundle.metadata["pool_name"] == "usdc_usdt"
        assert result.action_bundle.metadata["lp_amount"] == "100.5"
        mock_adapter.remove_liquidity.assert_called_once_with(
            pool_address=USDC_USDT_POOL,
            lp_amount=Decimal("100.5"),
            slippage_bps=50,
        )

    @patch(CURVE_POOLS_PATH, MOCK_CURVE_POOLS)
    @patch(CURVE_ADDRESSES_PATH, MOCK_CURVE_ADDRESSES)
    def test_lp_close_invalid_position_id_returns_failed(self, compiler):
        """LP close fails when position_id is not a valid decimal amount."""
        intent = LPCloseIntent(
            position_id="not_a_number",
            pool="usdc_usdt",
            protocol="curve",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "Invalid position_id" in result.error
        assert "not_a_number" in result.error

    @patch(CURVE_POOLS_PATH, MOCK_CURVE_POOLS)
    @patch(CURVE_ADDRESSES_PATH, MOCK_CURVE_ADDRESSES)
    def test_lp_close_missing_pool_returns_failed(self, compiler):
        """LP close fails when pool is not provided."""
        intent = LPCloseIntent(
            position_id="100.0",
            pool=None,
            protocol="curve",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "intent.pool must be set" in result.error

    @patch(CURVE_POOLS_PATH, MOCK_CURVE_POOLS)
    @patch(CURVE_ADDRESSES_PATH, MOCK_CURVE_ADDRESSES)
    @patch(CURVE_ADAPTER_CLS)
    @patch(CURVE_CONFIG_CLS)
    def test_lp_close_adapter_failure_propagates(self, mock_config_cls, mock_adapter_cls, compiler):
        """Adapter remove_liquidity failure is propagated as FAILED."""
        mock_adapter = MagicMock()
        mock_adapter.remove_liquidity.return_value = _make_mock_liq_result(
            success=False, error="slippage exceeded"
        )
        mock_adapter_cls.return_value = mock_adapter

        intent = LPCloseIntent(
            position_id="100.0",
            pool="usdc_usdt",
            protocol="curve",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "slippage exceeded" in result.error
