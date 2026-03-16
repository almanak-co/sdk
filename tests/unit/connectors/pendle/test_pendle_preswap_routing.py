"""Tests for Pendle pre-swap routing when tokenIn != tokenMintSy.

When the input token (e.g., WETH) differs from the token that mints SY
(e.g., wstETH), the compiler should insert a Uniswap V3 pre-swap step
to convert tokenIn -> tokenMintSy before calling the Pendle router.

Covers:
- Direct mint path (no routing needed, regression test)
- Pre-swap routing with WETH -> wstETH on Arbitrum
- Native ETH handling (msg.value for pre-swap)
- Error cases: missing price data, missing Uniswap V3 router
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.intents.compiler import (
    CompilationStatus,
    IntentCompiler,
)
from almanak.framework.intents.vocabulary import SwapIntent


# ============================================================================
# Fixtures
# ============================================================================

# Arbitrum wstETH market config
ARB_WSTETH_MARKET = "0xf78452e0f5c0b95fc5dc8353b8cd1e06e53fa25b"
ARB_WSTETH_ADDRESS = "0x5979d7b546e38e414f7e9822514be443a4800529"
ARB_WETH_ADDRESS = "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"


@pytest.fixture
def compiler_arbitrum():
    """Create an IntentCompiler for Arbitrum with mocked dependencies."""
    from almanak.framework.intents.compiler import IntentCompilerConfig

    price_oracle = {
        "WETH": Decimal("3000"),
        "wstETH": Decimal("3500"),
        "WSTETH": Decimal("3500"),
        "ETH": Decimal("3000"),
    }
    config = IntentCompilerConfig(allow_placeholder_prices=True)

    compiler = IntentCompiler(
        chain="arbitrum",
        wallet_address="0x" + "11" * 20,
        rpc_url="http://localhost:8545",
        price_oracle=price_oracle,
        config=config,
    )
    # Mock allowance query to avoid RPC calls
    compiler._query_allowance = MagicMock(return_value=0)
    return compiler


@pytest.fixture
def compiler_ethereum():
    """Create an IntentCompiler for Ethereum with mocked dependencies."""
    from almanak.framework.intents.compiler import IntentCompilerConfig

    price_oracle = {
        "sUSDe": Decimal("1.10"),
        "SUSDE": Decimal("1.10"),
        "USDC": Decimal("1.00"),
    }
    config = IntentCompilerConfig(allow_placeholder_prices=True)

    compiler = IntentCompiler(
        chain="ethereum",
        wallet_address="0x" + "11" * 20,
        rpc_url="http://localhost:8545",
        price_oracle=price_oracle,
        config=config,
    )
    compiler._query_allowance = MagicMock(return_value=0)
    return compiler


# ============================================================================
# Direct mint path (no routing needed) -- regression test
# ============================================================================


class TestDirectMintPath:
    """When tokenIn == tokenMintSy, no pre-swap should be inserted."""

    def test_wsteth_to_pt_wsteth_no_preswap(self, compiler_arbitrum):
        """wstETH -> PT-wstETH on Arbitrum should compile without pre-swap."""
        intent = SwapIntent(
            from_token="wstETH",
            to_token="PT-wstETH-25JUN2026",
            amount=Decimal("1"),
            max_slippage=Decimal("0.01"),
            protocol="pendle",
        )

        result = compiler_arbitrum.compile(intent)

        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle is not None
        txs = result.action_bundle.transactions
        # Should have approve + Pendle swap (no pre-swap)
        # Descriptions should NOT contain "Pre-swap"
        descriptions = [tx.get("description", "") for tx in txs]
        assert not any("Pre-swap" in d for d in descriptions), (
            f"Direct mint path should not have pre-swap transactions. Got: {descriptions}"
        )


# ============================================================================
# Pre-swap routing: WETH -> wstETH -> PT-wstETH
# ============================================================================


class TestPreSwapRouting:
    """When tokenIn != tokenMintSy, a Uniswap V3 pre-swap should be inserted."""

    def test_weth_to_pt_wsteth_inserts_preswap(self, compiler_arbitrum):
        """WETH -> PT-wstETH should compile with a Uniswap V3 pre-swap."""
        intent = SwapIntent(
            from_token="WETH",
            to_token="PT-wstETH-25JUN2026",
            amount=Decimal("1"),
            max_slippage=Decimal("0.01"),
            protocol="pendle",
        )

        result = compiler_arbitrum.compile(intent)

        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle is not None
        txs = result.action_bundle.transactions
        descriptions = [tx.get("description", "") for tx in txs]

        # Should contain a pre-swap transaction
        pre_swap_txs = [d for d in descriptions if "Pre-swap" in d]
        assert len(pre_swap_txs) == 1, f"Expected 1 pre-swap TX, got {len(pre_swap_txs)}: {descriptions}"

        # Pre-swap should mention Uniswap V3
        assert "Uniswap V3" in pre_swap_txs[0]

    def test_preswap_has_correct_tx_count(self, compiler_arbitrum):
        """WETH -> PT-wstETH should have: approve WETH + pre-swap + approve wstETH + Pendle swap."""
        intent = SwapIntent(
            from_token="WETH",
            to_token="PT-wstETH-25JUN2026",
            amount=Decimal("1"),
            max_slippage=Decimal("0.01"),
            protocol="pendle",
        )

        result = compiler_arbitrum.compile(intent)

        assert result.status == CompilationStatus.SUCCESS
        txs = result.action_bundle.transactions
        # At minimum: approve(WETH for Uniswap) + swap(WETH->wstETH) + approve(wstETH for Pendle) + swap(wstETH->PT)
        assert len(txs) >= 3, f"Expected at least 3 transactions, got {len(txs)}"

    def test_preswap_metadata_includes_routing(self, compiler_arbitrum):
        """ActionBundle metadata should indicate the swap used pre-swap routing."""
        intent = SwapIntent(
            from_token="WETH",
            to_token="PT-wstETH-25JUN2026",
            amount=Decimal("1"),
            max_slippage=Decimal("0.01"),
            protocol="pendle",
        )

        result = compiler_arbitrum.compile(intent)

        assert result.status == CompilationStatus.SUCCESS
        meta = result.action_bundle.metadata
        assert meta["protocol"] == "pendle"
        assert meta["swap_type"] == "token_to_pt"

    def test_preswap_amount_has_safety_buffer(self, compiler_arbitrum):
        """The Pendle step should use a buffered amount (less than full pre-swap output)."""
        intent = SwapIntent(
            from_token="WETH",
            to_token="PT-wstETH-25JUN2026",
            amount=Decimal("10"),
            max_slippage=Decimal("0.01"),
            protocol="pendle",
        )

        result = compiler_arbitrum.compile(intent)

        assert result.status == CompilationStatus.SUCCESS
        meta = result.action_bundle.metadata

        # The amount_in in metadata should be the buffered amount (not the original 10 WETH)
        pendle_amount_in = int(meta["amount_in"])
        original_amount = 10 * 10**18  # 10 WETH in wei

        # Pendle input should be less than original (it's the buffered pre-swap output)
        assert pendle_amount_in < original_amount, (
            f"Pendle amount_in ({pendle_amount_in}) should be < original ({original_amount}) "
            f"due to pre-swap price conversion and safety buffer"
        )


    def test_no_double_slippage_on_pendle_step(self, compiler_arbitrum):
        """VIB-576: min_amount_out for the Pendle step must equal amount_in (raw estimate).

        The SDK applies slippage internally, so the compiler must NOT reduce
        min_amount_out by slippage again. Previously, both compiler and SDK
        applied slippage, compounding the worst-case loss.
        """
        intent = SwapIntent(
            from_token="WETH",
            to_token="PT-wstETH-25JUN2026",
            amount=Decimal("1"),
            max_slippage=Decimal("0.01"),
            protocol="pendle",
        )

        result = compiler_arbitrum.compile(intent)

        assert result.status == CompilationStatus.SUCCESS
        meta = result.action_bundle.metadata

        # The compiler passes min_amount_out == amount_in to the adapter,
        # and the SDK applies slippage_bps internally.
        pendle_amount_in = int(meta["amount_in"])
        pendle_min_out = int(meta["min_amount_out"])

        assert pendle_min_out == pendle_amount_in, (
            f"min_amount_out ({pendle_min_out}) should equal amount_in ({pendle_amount_in}) "
            f"because the Pendle SDK applies slippage internally. "
            f"Double slippage detected (VIB-576)."
        )

    def test_no_double_slippage_direct_mint(self, compiler_arbitrum):
        """VIB-576: Direct mint path should also not double-count slippage."""
        intent = SwapIntent(
            from_token="wstETH",
            to_token="PT-wstETH-25JUN2026",
            amount=Decimal("1"),
            max_slippage=Decimal("0.02"),
            protocol="pendle",
        )

        result = compiler_arbitrum.compile(intent)

        assert result.status == CompilationStatus.SUCCESS
        meta = result.action_bundle.metadata

        amount_in = int(meta["amount_in"])
        min_out = int(meta["min_amount_out"])

        assert min_out == amount_in, (
            f"Direct mint: min_amount_out ({min_out}) should equal amount_in ({amount_in}). "
            f"Slippage is applied once by the SDK, not by the compiler."
        )


# ============================================================================
# Native ETH handling
# ============================================================================


class TestNativeETHPreSwap:
    """Native ETH -> PT-wstETH should wrap ETH and route through pre-swap."""

    def test_eth_to_pt_wsteth_compiles(self, compiler_arbitrum):
        """ETH (native) -> PT-wstETH should compile successfully."""
        intent = SwapIntent(
            from_token="ETH",
            to_token="PT-wstETH-25JUN2026",
            amount=Decimal("1"),
            max_slippage=Decimal("0.01"),
            protocol="pendle",
        )

        result = compiler_arbitrum.compile(intent)

        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle is not None
        txs = result.action_bundle.transactions

        # Should have pre-swap + Pendle swap
        descriptions = [tx.get("description", "") for tx in txs]
        pre_swap_txs = [d for d in descriptions if "Pre-swap" in d]
        assert len(pre_swap_txs) == 1, f"Expected pre-swap for native ETH, got: {descriptions}"

    def test_eth_preswap_has_value(self, compiler_arbitrum):
        """Native ETH pre-swap should have msg.value set."""
        intent = SwapIntent(
            from_token="ETH",
            to_token="PT-wstETH-25JUN2026",
            amount=Decimal("1"),
            max_slippage=Decimal("0.01"),
            protocol="pendle",
        )

        result = compiler_arbitrum.compile(intent)

        assert result.status == CompilationStatus.SUCCESS
        txs = result.action_bundle.transactions

        # Find the pre-swap TX
        pre_swap = None
        for tx in txs:
            if "Pre-swap" in tx.get("description", ""):
                pre_swap = tx
                break

        assert pre_swap is not None
        # Native ETH swap should have value > 0
        assert int(pre_swap.get("value", "0")) > 0


# ============================================================================
# Selling PT/YT -- no pre-swap needed
# ============================================================================


class TestSellingPTNoPreSwap:
    """When selling PT -> token, no pre-swap routing should be applied."""

    def test_pt_to_wsteth_no_preswap(self, compiler_arbitrum):
        """PT-wstETH -> wstETH should not trigger pre-swap routing."""
        intent = SwapIntent(
            from_token="PT-wstETH-25JUN2026",
            to_token="wstETH",
            amount=Decimal("1"),
            max_slippage=Decimal("0.01"),
            protocol="pendle",
        )

        result = compiler_arbitrum.compile(intent)

        assert result.status == CompilationStatus.SUCCESS
        txs = result.action_bundle.transactions
        descriptions = [tx.get("description", "") for tx in txs]
        assert not any("Pre-swap" in d for d in descriptions), (
            f"Selling PT should not have pre-swap. Got: {descriptions}"
        )


# ============================================================================
# Sell direction min_amount_out discount (VIB-1366)
# ============================================================================


class TestSellDirectionMinAmountDiscount:
    """PT/YT sell directions should use a discounted min_amount_out (VIB-1366).

    PT/YT trades at a discount to the underlying token. Using a 1:1 estimate
    for min_amount_out causes all sell transactions to revert with
    INSUFFICIENT_TOKEN_OUT because the actual output is less than the input.
    """

    def test_pt_sell_min_amount_out_is_discounted(self, compiler_arbitrum):
        """PT-wstETH -> wstETH should use min_amount_out = amount_in // 2, not amount_in."""
        from almanak.framework.connectors.pendle.adapter import PendleSwapParams

        captured_params = []
        original_init = PendleSwapParams.__init__

        def capturing_init(self, **kwargs):
            captured_params.append(kwargs)
            original_init(self, **kwargs)

        intent = SwapIntent(
            from_token="PT-wstETH-25JUN2026",
            to_token="wstETH",
            amount=Decimal("1"),
            max_slippage=Decimal("0.01"),
            protocol="pendle",
        )

        with patch.object(PendleSwapParams, "__init__", capturing_init):
            result = compiler_arbitrum.compile(intent)

        assert result.status == CompilationStatus.SUCCESS, f"Compilation failed: {result.error}"
        assert len(captured_params) == 1, f"Expected 1 PendleSwapParams, got {len(captured_params)}"

        params = captured_params[0]
        assert params["swap_type"] == "pt_to_token"
        assert params["min_amount_out"] == params["amount_in"] // 2, (
            f"Sell direction min_amount_out should be amount_in // 2 "
            f"(got {params['min_amount_out']}, expected {params['amount_in'] // 2})"
        )

    def test_pt_buy_min_amount_out_is_1to1(self, compiler_arbitrum):
        """wstETH -> PT-wstETH should use min_amount_out = amount_in (1:1 estimate)."""
        from almanak.framework.connectors.pendle.adapter import PendleSwapParams

        captured_params = []
        original_init = PendleSwapParams.__init__

        def capturing_init(self, **kwargs):
            captured_params.append(kwargs)
            original_init(self, **kwargs)

        intent = SwapIntent(
            from_token="wstETH",
            to_token="PT-wstETH-25JUN2026",
            amount=Decimal("1"),
            max_slippage=Decimal("0.01"),
            protocol="pendle",
        )

        with patch.object(PendleSwapParams, "__init__", capturing_init):
            result = compiler_arbitrum.compile(intent)

        assert result.status == CompilationStatus.SUCCESS, f"Compilation failed: {result.error}"
        assert len(captured_params) == 1, f"Expected 1 PendleSwapParams, got {len(captured_params)}"

        params = captured_params[0]
        assert params["swap_type"] == "token_to_pt"
        assert params["min_amount_out"] == params["amount_in"], (
            f"Buy direction min_amount_out should equal amount_in "
            f"(got {params['min_amount_out']}, expected {params['amount_in']})"
        )

    def test_yt_sell_min_amount_out_uses_deeper_discount(self, compiler_ethereum):
        """YT-sUSDe -> sUSDe should use min_amount_out = amount_in // 100 (1% floor).

        YT represents only yield, which can approach zero near maturity.
        A 50% floor (used for PT) would still cause reverts for YT sells.
        """
        from almanak.framework.connectors.pendle.adapter import PendleSwapParams

        captured_params = []
        original_init = PendleSwapParams.__init__

        def capturing_init(self, **kwargs):
            captured_params.append(kwargs)
            original_init(self, **kwargs)

        intent = SwapIntent(
            from_token="YT-sUSDe-7MAY2026",
            to_token="sUSDe",
            amount=Decimal("100"),
            max_slippage=Decimal("0.01"),
            protocol="pendle",
        )

        with patch.object(PendleSwapParams, "__init__", capturing_init):
            result = compiler_ethereum.compile(intent)

        assert result.status == CompilationStatus.SUCCESS, f"Compilation failed: {result.error}"
        assert len(captured_params) == 1, f"Expected 1 PendleSwapParams, got {len(captured_params)}"

        params = captured_params[0]
        assert params["swap_type"] == "yt_to_token"
        assert params["min_amount_out"] == params["amount_in"] // 100, (
            f"YT sell min_amount_out should be amount_in // 100 "
            f"(got {params['min_amount_out']}, expected {params['amount_in'] // 100})"
        )


# ============================================================================
# Error cases
# ============================================================================


class TestPreSwapErrors:
    """Error handling for pre-swap routing edge cases."""

    def test_missing_price_data_fails_gracefully(self, compiler_arbitrum):
        """If price oracle can't price the SY token, compilation should fail cleanly."""
        # Override _calculate_expected_output to simulate missing price data
        original = compiler_arbitrum._calculate_expected_output

        def broken_estimate(amount_in, from_token, to_token):
            raise ValueError("No price for WSTETH")

        compiler_arbitrum._calculate_expected_output = broken_estimate

        intent = SwapIntent(
            from_token="WETH",
            to_token="PT-wstETH-25JUN2026",
            amount=Decimal("1"),
            max_slippage=Decimal("0.01"),
            protocol="pendle",
        )

        result = compiler_arbitrum.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "price" in result.error.lower() or "estimate" in result.error.lower()

        # Restore
        compiler_arbitrum._calculate_expected_output = original

    def test_no_uniswap_router_fails_gracefully(self, compiler_arbitrum):
        """If no Uniswap V3 router exists for the chain, compilation should fail with guidance."""
        # Patch PROTOCOL_ROUTERS to remove uniswap_v3 for arbitrum
        with patch(
            "almanak.framework.intents.compiler.PROTOCOL_ROUTERS",
            {"arbitrum": {}},
        ):
            intent = SwapIntent(
                from_token="WETH",
                to_token="PT-wstETH-25JUN2026",
                amount=Decimal("1"),
                max_slippage=Decimal("0.01"),
                protocol="pendle",
            )

            result = compiler_arbitrum.compile(intent)

            assert result.status == CompilationStatus.FAILED
            assert "uniswap" in result.error.lower() or "router" in result.error.lower()

    def test_extreme_slippage_fails_gracefully(self, compiler_arbitrum):
        """If max_slippage >= 100%, the buffered amount would be zero -- should fail cleanly."""
        intent = SwapIntent(
            from_token="WETH",
            to_token="PT-wstETH-25JUN2026",
            amount=Decimal("1"),
            max_slippage=Decimal("1.0"),  # 100% slippage
            protocol="pendle",
        )

        result = compiler_arbitrum.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "slippage" in result.error.lower() or "amount" in result.error.lower()
