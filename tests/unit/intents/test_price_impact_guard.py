"""Unit tests for price impact guard in swap compilation (VIB-1738).

The price impact guard prevents compilation of swaps where the on-chain quoter
returns an amount that deviates catastrophically from the oracle estimate,
indicating insufficient pool liquidity.
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak import IntentCompiler, IntentCompilerConfig, SwapIntent
from almanak.framework.intents.compiler import CompilationStatus


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# Patch target for DefaultSwapAdapter — the compiler imports it from its own module.
ADAPTER_CLS = "almanak.framework.intents.compiler.DefaultSwapAdapter"


def _make_compiler(
    max_price_impact_pct: Decimal = Decimal("0.30"),
    allow_placeholder_prices: bool = False,
    price_oracle: dict[str, Decimal] | None = None,
) -> IntentCompiler:
    """Create a compiler with a price oracle for testing.

    By default uses allow_placeholder_prices=False and provides a real price
    oracle so that _using_placeholders=False and the guard is active.
    """
    if price_oracle is None:
        price_oracle = {
            "ETH": Decimal("2000"),
            "WETH": Decimal("2000"),
            "USDC": Decimal("1"),
            "USDT": Decimal("1"),
        }

    config = IntentCompilerConfig(
        allow_placeholder_prices=allow_placeholder_prices,
        max_price_impact_pct=max_price_impact_pct,
    )

    return IntentCompiler(
        chain="arbitrum",
        wallet_address="0x1234567890123456789012345678901234567890",
        config=config,
        price_oracle=price_oracle,
    )


def _make_mock_adapter(
    quoter_amount: int | None = None,
    router_address: str = "0xE592427A0AEce92De3Edee1F18E0157C05861564",
) -> MagicMock:
    """Create a mock DefaultSwapAdapter with controlled quoter output."""
    adapter = MagicMock()
    adapter.get_router_address.return_value = router_address
    adapter.select_fee_tier.return_value = 3000
    adapter.get_quoted_amount_out.return_value = quoter_amount
    adapter.last_fee_selection = {"selected_fee_tier": None}  # Skip pool validation
    adapter.get_swap_calldata.return_value = bytes.fromhex("abcdef")
    adapter.estimate_gas.return_value = 200_000
    return adapter


# ---------------------------------------------------------------------------
# IntentCompilerConfig validation
# ---------------------------------------------------------------------------


class TestIntentCompilerConfigPriceImpact:
    """Validate max_price_impact_pct config behavior."""

    def test_default_is_30_percent(self) -> None:
        config = IntentCompilerConfig(allow_placeholder_prices=True)
        assert config.max_price_impact_pct == Decimal("0.30")

    def test_custom_value_accepted(self) -> None:
        config = IntentCompilerConfig(
            allow_placeholder_prices=True,
            max_price_impact_pct=Decimal("0.80"),
        )
        assert config.max_price_impact_pct == Decimal("0.80")

    def test_rejects_zero(self) -> None:
        with pytest.raises(ValueError, match="max_price_impact_pct"):
            IntentCompilerConfig(allow_placeholder_prices=True, max_price_impact_pct=Decimal("0"))

    def test_rejects_negative(self) -> None:
        with pytest.raises(ValueError, match="max_price_impact_pct"):
            IntentCompilerConfig(allow_placeholder_prices=True, max_price_impact_pct=Decimal("-0.1"))

    def test_rejects_above_one(self) -> None:
        with pytest.raises(ValueError, match="max_price_impact_pct"):
            IntentCompilerConfig(allow_placeholder_prices=True, max_price_impact_pct=Decimal("1.5"))

    def test_accepts_one(self) -> None:
        config = IntentCompilerConfig(
            allow_placeholder_prices=True,
            max_price_impact_pct=Decimal("1"),
        )
        assert config.max_price_impact_pct == Decimal("1")


# ---------------------------------------------------------------------------
# SwapIntent.max_price_impact field validation
# ---------------------------------------------------------------------------


class TestSwapIntentMaxPriceImpact:
    """Validate the per-intent max_price_impact field."""

    def test_defaults_to_none(self) -> None:
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount_usd=Decimal("100"),
        )
        assert intent.max_price_impact is None

    def test_custom_value_accepted(self) -> None:
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount_usd=Decimal("100"),
            max_price_impact=Decimal("0.95"),
        )
        assert intent.max_price_impact == Decimal("0.95")

    def test_rejects_zero(self) -> None:
        with pytest.raises(ValueError, match="max_price_impact"):
            SwapIntent(
                from_token="USDC",
                to_token="WETH",
                amount_usd=Decimal("100"),
                max_price_impact=Decimal("0"),
            )

    def test_rejects_negative(self) -> None:
        with pytest.raises(ValueError, match="max_price_impact"):
            SwapIntent(
                from_token="USDC",
                to_token="WETH",
                amount_usd=Decimal("100"),
                max_price_impact=Decimal("-0.1"),
            )

    def test_rejects_above_one(self) -> None:
        with pytest.raises(ValueError, match="max_price_impact"):
            SwapIntent(
                from_token="USDC",
                to_token="WETH",
                amount_usd=Decimal("100"),
                max_price_impact=Decimal("1.5"),
            )

    def test_accepts_one(self) -> None:
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount_usd=Decimal("100"),
            max_price_impact=Decimal("1"),
        )
        assert intent.max_price_impact == Decimal("1")

    def test_intent_factory_passes_max_price_impact(self) -> None:
        from almanak.framework.intents.vocabulary import Intent

        intent = Intent.swap(
            from_token="USDC",
            to_token="WETH",
            amount_usd=Decimal("100"),
            max_price_impact=Decimal("0.80"),
        )
        assert intent.max_price_impact == Decimal("0.80")


# ---------------------------------------------------------------------------
# Price impact guard in compilation — exercises actual compiler.compile()
# ---------------------------------------------------------------------------


class TestPriceImpactGuardCompilation:
    """Test that price impact guard blocks catastrophically bad swaps.

    These tests call compiler.compile() with a mocked DefaultSwapAdapter
    that returns controlled quoter amounts, verifying the guard triggers
    CompilationStatus.FAILED when price impact exceeds the threshold.
    """

    @patch(ADAPTER_CLS)
    def test_guard_blocks_99pct_deviation(self, mock_adapter_cls) -> None:
        """Simulate iter-122 scenario: 100 USDC -> 0.0002 WETH (99.6% impact)."""
        compiler = _make_compiler()
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount_usd=Decimal("100"),
        )

        # Oracle estimate for 100 USDC -> WETH at $2000/ETH:
        # 100 / 2000 * 0.997 = ~0.04985 WETH = 49850000000000000 wei
        # Quoter returns catastrophically low: 200000000000000 wei (0.0002 WETH)
        mock_adapter = _make_mock_adapter(quoter_amount=200_000_000_000_000)
        mock_adapter_cls.return_value = mock_adapter

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "Price impact too high" in result.error
        assert "insufficient liquidity" in result.error

    @patch(ADAPTER_CLS)
    def test_guard_allows_20pct_deviation(self, mock_adapter_cls) -> None:
        """20% deviation should pass with default 30% threshold."""
        compiler = _make_compiler()
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount_usd=Decimal("100"),
        )

        # Oracle estimate: ~49850000000000000 wei (0.04985 WETH)
        # Quoter returns 80% of oracle (20% impact, under 30% threshold)
        oracle_approx = 49_850_000_000_000_000
        quoter_at_20pct = oracle_approx * 80 // 100
        mock_adapter = _make_mock_adapter(quoter_amount=quoter_at_20pct)
        mock_adapter_cls.return_value = mock_adapter

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS

    @patch(ADAPTER_CLS)
    def test_guard_boundary_at_threshold(self, mock_adapter_cls) -> None:
        """Exactly at 30% threshold should pass (guard uses strict >)."""
        compiler = _make_compiler()
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount_usd=Decimal("100"),
        )

        # Calculate oracle estimate: 100 USDC / 2000 * 0.997 = 0.04985 WETH
        # 0.04985 * 10^18 = 49850000000000000 wei
        # At exactly 30%: quoter = 49850000000000000 * 70 / 100 = 34895000000000000
        oracle_approx = 49_850_000_000_000_000
        quoter_at_30pct = oracle_approx * 70 // 100
        mock_adapter = _make_mock_adapter(quoter_amount=quoter_at_30pct)
        mock_adapter_cls.return_value = mock_adapter

        result = compiler.compile(intent)

        # At exactly the threshold, should NOT block (uses > not >=)
        assert result.status == CompilationStatus.SUCCESS

    @patch(ADAPTER_CLS)
    def test_guard_blocks_just_above_threshold(self, mock_adapter_cls) -> None:
        """Just above 30% threshold should fail."""
        compiler = _make_compiler()
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount_usd=Decimal("100"),
        )

        # Quoter returns slightly less than 70% of oracle (31% impact)
        oracle_approx = 49_850_000_000_000_000
        quoter_just_above = oracle_approx * 69 // 100  # 31% impact
        mock_adapter = _make_mock_adapter(quoter_amount=quoter_just_above)
        mock_adapter_cls.return_value = mock_adapter

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "Price impact too high" in result.error

    @patch(ADAPTER_CLS)
    def test_per_intent_override_allows_higher_impact(self, mock_adapter_cls) -> None:
        """Intent-level max_price_impact=0.95 should allow 90% deviation."""
        compiler = _make_compiler()
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount_usd=Decimal("100"),
            max_price_impact=Decimal("0.95"),
        )

        # 90% deviation: quoter returns 10% of oracle
        oracle_approx = 49_850_000_000_000_000
        quoter_at_90pct = oracle_approx // 10
        mock_adapter = _make_mock_adapter(quoter_amount=quoter_at_90pct)
        mock_adapter_cls.return_value = mock_adapter

        result = compiler.compile(intent)

        # 90% < 95% override, so should pass
        assert result.status == CompilationStatus.SUCCESS

    @patch(ADAPTER_CLS)
    def test_per_intent_override_still_blocks_above(self, mock_adapter_cls) -> None:
        """Intent-level max_price_impact=0.95 should block 97% deviation."""
        compiler = _make_compiler()
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount_usd=Decimal("100"),
            max_price_impact=Decimal("0.95"),
        )

        # 97% deviation: quoter returns 3% of oracle
        oracle_approx = 49_850_000_000_000_000
        quoter_at_97pct = oracle_approx * 3 // 100
        mock_adapter = _make_mock_adapter(quoter_amount=quoter_at_97pct)
        mock_adapter_cls.return_value = mock_adapter

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "Price impact too high" in result.error

    @patch(ADAPTER_CLS)
    def test_quoter_higher_than_oracle_passes(self, mock_adapter_cls) -> None:
        """When quoter returns more than oracle, no price impact issue."""
        compiler = _make_compiler()
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount_usd=Decimal("100"),
        )

        # Quoter returns 10% MORE than oracle (negative impact = better deal)
        oracle_approx = 49_850_000_000_000_000
        quoter_better = oracle_approx * 110 // 100
        mock_adapter = _make_mock_adapter(quoter_amount=quoter_better)
        mock_adapter_cls.return_value = mock_adapter

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS

    @patch(ADAPTER_CLS)
    def test_none_quoter_skips_guard(self, mock_adapter_cls) -> None:
        """When quoter returns None (RPC failed), guard is skipped."""
        compiler = _make_compiler()
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount_usd=Decimal("100"),
        )

        # Quoter returns None (simulating RPC failure)
        mock_adapter = _make_mock_adapter(quoter_amount=None)
        mock_adapter_cls.return_value = mock_adapter

        result = compiler.compile(intent)

        # Should succeed — guard skipped when quoter_amount is None
        assert result.status == CompilationStatus.SUCCESS

    @patch(ADAPTER_CLS)
    def test_placeholder_mode_skips_guard(self, mock_adapter_cls) -> None:
        """Guard is skipped when using placeholder prices (test mode)."""
        compiler = _make_compiler(allow_placeholder_prices=True)
        # Override to placeholder mode
        compiler._using_placeholders = True

        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount_usd=Decimal("100"),
        )

        # Catastrophic quoter amount that would normally trigger guard
        mock_adapter = _make_mock_adapter(quoter_amount=1)
        mock_adapter_cls.return_value = mock_adapter

        result = compiler.compile(intent)

        # Should succeed — guard skipped in placeholder mode
        assert result.status == CompilationStatus.SUCCESS

    @patch(ADAPTER_CLS)
    def test_custom_config_threshold(self, mock_adapter_cls) -> None:
        """Custom max_price_impact_pct=0.20 blocks 30% deviation."""
        compiler = _make_compiler(max_price_impact_pct=Decimal("0.20"))
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount_usd=Decimal("100"),
        )

        # 30% deviation — passes default 50% but fails custom 20%
        oracle_approx = 49_850_000_000_000_000
        quoter_at_30pct = oracle_approx * 70 // 100
        mock_adapter = _make_mock_adapter(quoter_amount=quoter_at_30pct)
        mock_adapter_cls.return_value = mock_adapter

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "Price impact too high" in result.error

    @patch(ADAPTER_CLS)
    def test_error_message_contains_amounts(self, mock_adapter_cls) -> None:
        """Error message includes oracle estimate and quoter amount for debugging."""
        compiler = _make_compiler()
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount_usd=Decimal("100"),
        )

        mock_adapter = _make_mock_adapter(quoter_amount=200_000_000_000_000)
        mock_adapter_cls.return_value = mock_adapter

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "oracle estimate:" in result.error
        assert "quoter:" in result.error
        assert "200000000000000" in result.error
        assert "USDC->WETH" in result.error
