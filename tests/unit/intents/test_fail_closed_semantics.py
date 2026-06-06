"""Fail-closed semantics for compiler pool validation, price-impact guard, and tick math (VIB-3160).

Verifies that the compiler treats ambiguous/unverifiable conditions as
compilation failures on the production path rather than silently producing
a doomed transaction.
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak import IntentCompiler, IntentCompilerConfig, SwapIntent
from almanak.connectors._strategy_base.pool_validation_base import PoolValidationReason, PoolValidationResult
from almanak.framework.intents.compiler import CompilationStatus

ADAPTER_CLS = "almanak.framework.intents.compiler.DefaultSwapAdapter"
V3_REGISTRY_QUOTE = "almanak.connectors.uniswap_v3.compiler.UniswapV3Compiler._quote_swap_via_registry"


def _make_compiler(
    *,
    allow_placeholder_prices: bool = False,
    price_oracle: dict[str, Decimal] | None = None,
) -> IntentCompiler:
    if price_oracle is None:
        price_oracle = {
            "ETH": Decimal("2000"),
            "WETH": Decimal("2000"),
            "USDC": Decimal("1"),
        }
    config = IntentCompilerConfig(allow_placeholder_prices=allow_placeholder_prices)
    return IntentCompiler(
        chain="arbitrum",
        wallet_address="0x1234567890123456789012345678901234567890",
        config=config,
        price_oracle=price_oracle,
    )


# ---------------------------------------------------------------------------
# Defect 1: pool validation fail-closed on RPC_FAILED / NOT_FOUND
# ---------------------------------------------------------------------------


class TestPoolValidationFailClosed:
    """`_validate_pool` must fail the compilation when the validator attempted
    an on-chain check but received an RPC error, and must pass-through warnings
    when validation is legitimately impossible (no RPC configured, unknown chain)."""

    def _compiler(self) -> IntentCompiler:
        return _make_compiler()

    def test_rpc_failed_blocks_compilation(self) -> None:
        compiler = self._compiler()
        result = PoolValidationResult(
            exists=None,
            reason=PoolValidationReason.RPC_FAILED,
            warning="RPC call to factory failed",
        )
        out = compiler._validate_pool(result, intent_id="test-1")
        assert out is not None
        assert out.status == CompilationStatus.FAILED
        assert "RPC call" in (out.error or "")

    def test_not_found_blocks_compilation(self) -> None:
        compiler = self._compiler()
        result = PoolValidationResult(
            exists=False,
            reason=PoolValidationReason.NOT_FOUND,
            error="No uniswap_v3 pool found for ...",
        )
        out = compiler._validate_pool(result, intent_id="test-2")
        assert out is not None
        assert out.status == CompilationStatus.FAILED
        assert "No uniswap_v3 pool" in (out.error or "")

    def test_rpc_unavailable_warns_and_proceeds(self) -> None:
        compiler = self._compiler()
        result = PoolValidationResult(
            exists=None,
            reason=PoolValidationReason.RPC_UNAVAILABLE,
            warning="No RPC URL available",
        )
        out = compiler._validate_pool(result, intent_id="test-3")
        assert out is None

    def test_factory_missing_warns_and_proceeds(self) -> None:
        compiler = self._compiler()
        result = PoolValidationResult(
            exists=None,
            reason=PoolValidationReason.FACTORY_MISSING,
            warning="No factory for chain",
        )
        out = compiler._validate_pool(result, intent_id="test-4")
        assert out is None

    def test_protocol_unknown_warns_and_proceeds(self) -> None:
        compiler = self._compiler()
        result = PoolValidationResult(
            exists=None,
            reason=PoolValidationReason.PROTOCOL_UNKNOWN,
            warning="Unknown protocol",
        )
        out = compiler._validate_pool(result, intent_id="test-5")
        assert out is None

    def test_confirmed_passes(self) -> None:
        compiler = self._compiler()
        result = PoolValidationResult(
            exists=True,
            reason=PoolValidationReason.CONFIRMED,
            pool_address="0xdeadbeef",
        )
        out = compiler._validate_pool(result, intent_id="test-6")
        assert out is None

    def test_rpc_failed_in_placeholder_mode_warns_not_fails(self) -> None:
        # Permission discovery / unit tests legitimately run with unreachable RPC;
        # relaxing RPC_FAILED to a warning keeps offline flows compilable.
        compiler = _make_compiler(allow_placeholder_prices=True)
        compiler._using_placeholders = True
        result = PoolValidationResult(
            exists=None,
            reason=PoolValidationReason.RPC_FAILED,
            warning="RPC call to factory failed",
        )
        out = compiler._validate_pool(result, intent_id="test-7")
        assert out is None

    def test_rpc_failed_in_permission_discovery_warns_not_fails(self) -> None:
        # permission_discovery is the second offline class _validate_pool honors;
        # prove RPC_FAILED is relaxed to a warning there too (not only in
        # placeholder-price mode), matching the offline_mode expression used
        # in the price-impact guard.
        compiler = self._compiler()
        compiler._config.permission_discovery = True
        result = PoolValidationResult(
            exists=None,
            reason=PoolValidationReason.RPC_FAILED,
            warning="RPC call to factory failed",
        )
        out = compiler._validate_pool(result, intent_id="test-8")
        assert out is None

    def test_not_configured_warns_and_proceeds(self) -> None:
        # NOT_CONFIGURED covers protocols/chains that deliberately skip on-chain
        # pool checks (e.g. bridge aggregators that route through a meta-router
        # rather than a canonical pool). Treat as a warning, not a hard fail.
        compiler = self._compiler()
        result = PoolValidationResult(
            exists=None,
            reason=PoolValidationReason.NOT_CONFIGURED,
            warning="Pool validation not configured for this protocol",
        )
        out = compiler._validate_pool(result, intent_id="test-9")
        assert out is None


# ---------------------------------------------------------------------------
# Defect 2: price-impact guard fail-closed on quoter=None for live swaps
# ---------------------------------------------------------------------------


def _make_mock_adapter(quoter_amount: int | None, selected_fee: int = 3000) -> MagicMock:
    adapter = MagicMock()
    adapter.get_router_address.return_value = "0xE592427A0AEce92De3Edee1F18E0157C05861564"
    adapter.select_fee_tier.return_value = selected_fee
    adapter.get_quoted_amount_out.return_value = quoter_amount
    # selected_fee_tier=None bypasses the pool-existence validation block so we isolate
    # the quoter-None handling under test.
    adapter.last_fee_selection = {"selected_fee_tier": None}
    adapter.get_swap_calldata.return_value = bytes.fromhex("abcdef")
    adapter.estimate_gas.return_value = 200_000
    return adapter


class TestPriceImpactGuardFailClosed:
    """When the on-chain quoter returns None for a live (non-placeholder) swap,
    the compiler must fail rather than degrade to an oracle-only estimate."""

    @patch(V3_REGISTRY_QUOTE, return_value=None)
    @patch(ADAPTER_CLS)
    def test_quoter_none_live_swap_fails_closed(self, mock_adapter_cls, mock_registry_quote) -> None:
        compiler = _make_compiler()
        mock_adapter = _make_mock_adapter(quoter_amount=None)
        mock_adapter_cls.return_value = mock_adapter

        intent = SwapIntent(from_token="USDC", to_token="WETH", amount_usd=Decimal("100"))
        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "on-chain quoter returned no amount" in (result.error or "").lower()
        # Prove the failure came from the quoter path, not an unrelated early exit.
        mock_registry_quote.assert_called_once()
        mock_adapter.get_quoted_amount_out.assert_called()

    @patch(V3_REGISTRY_QUOTE, return_value=None)
    @patch(ADAPTER_CLS)
    def test_quoter_none_placeholder_mode_still_passes(self, mock_adapter_cls, mock_registry_quote) -> None:
        compiler = _make_compiler(allow_placeholder_prices=True)
        compiler._using_placeholders = True

        mock_adapter = _make_mock_adapter(quoter_amount=None)
        mock_adapter_cls.return_value = mock_adapter

        intent = SwapIntent(from_token="USDC", to_token="WETH", amount_usd=Decimal("100"))
        result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS
        mock_registry_quote.assert_called_once()

    @patch(V3_REGISTRY_QUOTE, return_value=None)
    @patch(ADAPTER_CLS)
    def test_quoter_none_permission_discovery_still_passes(self, mock_adapter_cls, mock_registry_quote) -> None:
        """Permission-discovery runs legitimately have no RPC and must be able to
        emit calldata shapes — mirror the offline-mode exemption _validate_pool uses."""
        compiler = _make_compiler()
        compiler._config.permission_discovery = True  # opt-in the offline classification

        mock_adapter = _make_mock_adapter(quoter_amount=None)
        mock_adapter_cls.return_value = mock_adapter

        intent = SwapIntent(from_token="USDC", to_token="WETH", amount_usd=Decimal("100"))
        result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS
        mock_registry_quote.assert_called_once()


# ---------------------------------------------------------------------------
# Defect 3: tick math Decimal precision for decimal-asymmetric pairs
# ---------------------------------------------------------------------------


class TestTickMathDecimalPrecision:
    """`_price_to_tick` must produce a deterministic tick across equivalent
    input representations that a float-based implementation rounds apart."""

    def test_weth_usdc_boundary_price_is_deterministic(self) -> None:
        # WETH/USDC: decimals0=18, decimals1=6; adjusted = price / 10^12.
        # Two Decimal representations of the same value that differ only in
        # trailing digits must map to the identical tick.
        price_a = Decimal("2000")
        price_b = Decimal("2000.000000000000000")
        tick_a = IntentCompiler._price_to_tick(price_a, token0_decimals=18, token1_decimals=6)
        tick_b = IntentCompiler._price_to_tick(price_b, token0_decimals=18, token1_decimals=6)
        assert tick_a == tick_b

    def test_weth_usdc_tick_matches_expected_formula(self) -> None:
        # Reference computed with 50-digit Decimal arithmetic:
        # floor(ln(2000 / 10^12) / ln(1.0001)) = -200312
        tick = IntentCompiler._price_to_tick(Decimal("2000"), token0_decimals=18, token1_decimals=6)
        assert tick == -200312

    def test_tick_is_integer_and_bounded(self) -> None:
        for p in ["0.000001", "1", "3400", "1e6"]:
            tick = IntentCompiler._price_to_tick(Decimal(p), token0_decimals=18, token1_decimals=6)
            assert isinstance(tick, int)
            assert IntentCompiler.UNISWAP_MIN_TICK <= tick <= IntentCompiler.UNISWAP_MAX_TICK

    def test_negative_or_zero_price_raises(self) -> None:
        with pytest.raises(ValueError):
            IntentCompiler._price_to_tick(Decimal("0"), 18, 18)
        with pytest.raises(ValueError):
            IntentCompiler._price_to_tick(Decimal("-1"), 18, 18)

    def test_decimal_sweep_is_monotonic(self) -> None:
        prices = [Decimal("100"), Decimal("1000"), Decimal("2000"), Decimal("3000"), Decimal("5000")]
        ticks = [IntentCompiler._price_to_tick(p, token0_decimals=18, token1_decimals=6) for p in prices]
        assert ticks == sorted(ticks)

    def test_exact_tick_boundary_is_exact_tick(self) -> None:
        """A price that is mathematically exactly at tick N must produce tick N.

        The previous float-based implementation silently rounded down at every exact
        tick boundary because of the ``float(price) / 1e12`` precision loss followed
        by ``math.log(...) / math.log(1.0001)``. With 50-digit Decimal arithmetic the
        boundary price yields an integer log ratio and ``math.floor`` returns N.
        """
        from decimal import localcontext

        target_tick = -200312
        with localcontext() as ctx:
            ctx.prec = 60
            adjusted = Decimal("1.0001") ** target_tick
            nominal_price = adjusted * (Decimal(10) ** 12)

        tick = IntentCompiler._price_to_tick(nominal_price, token0_decimals=18, token1_decimals=6)
        assert tick == target_tick
