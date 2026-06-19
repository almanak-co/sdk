"""Tests for paper trading bootstrap inference (VIB-2376).

Verifies that:
- Token requirements are inferred from SwapIntent, SupplyIntent, etc.
- HoldIntent and None results yield empty requirements
- Amounts are scaled by 1.5x safety buffer
- Divergence check warns when explicit and inferred amounts differ >20%
- Exceptions in decide() are caught gracefully
"""

import logging
from decimal import Decimal
from unittest.mock import MagicMock

from almanak.framework.backtesting.paper.bootstrap_inference import (
    SAFETY_BUFFER,
    _extract_tokens_from_intent,
    _extract_tokens_from_result,
    check_divergence,
    infer_token_requirements,
)


class TestExtractTokensFromIntent:
    """Test token extraction from individual intents."""

    def test_swap_intent_extracts_from_token(self):
        """SwapIntent extracts from_token with amount_usd-based amount."""
        from almanak.framework.intents.vocabulary import SwapIntent

        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount_usd=Decimal("1000"),
        )
        tokens = _extract_tokens_from_intent(intent)
        assert "USDC" in tokens
        assert tokens["USDC"] == Decimal("1000")  # $1000 / $1 per USDC

    def test_swap_intent_with_token_amount(self):
        """SwapIntent with explicit amount uses that amount."""
        from almanak.framework.intents.vocabulary import SwapIntent

        intent = SwapIntent(
            from_token="WETH",
            to_token="USDC",
            amount=Decimal("2.5"),
        )
        tokens = _extract_tokens_from_intent(intent)
        assert "WETH" in tokens
        assert tokens["WETH"] == Decimal("2.5")

    def test_hold_intent_returns_empty(self):
        """HoldIntent yields no token requirements."""
        from almanak.framework.intents.vocabulary import HoldIntent

        intent = HoldIntent(reason="waiting")
        tokens = _extract_tokens_from_intent(intent)
        assert tokens == {}

    def test_supply_intent_extracts_token(self):
        """SupplyIntent extracts the token to supply."""
        from almanak.framework.intents.lending_intents import SupplyIntent

        intent = SupplyIntent(
            protocol="aave_v3",
            token="USDC",
            amount=Decimal("5000"),
        )
        tokens = _extract_tokens_from_intent(intent)
        assert "USDC" in tokens
        assert tokens["USDC"] == Decimal("5000")

    def test_repay_intent_extracts_token(self):
        """RepayIntent extracts the token to repay."""
        from almanak.framework.intents.lending_intents import RepayIntent

        intent = RepayIntent(
            protocol="aave_v3",
            token="USDC",
            amount=Decimal("1000"),
        )
        tokens = _extract_tokens_from_intent(intent)
        assert "USDC" in tokens
        assert tokens["USDC"] == Decimal("1000")

    def test_perp_open_extracts_collateral(self):
        """PerpOpenIntent extracts collateral_token."""
        from almanak.framework.intents.perp_intents import PerpOpenIntent

        intent = PerpOpenIntent(
            protocol="gmx_v2",
            market="ETH/USD",
            is_long=True,
            collateral_token="USDC",
            collateral_amount=Decimal("2000"),
            size_usd=Decimal("10000"),
            leverage=Decimal("5"),
        )
        tokens = _extract_tokens_from_intent(intent)
        assert "USDC" in tokens

    def test_lp_open_extracts_pool_legs_and_skips_zero_leg(self):
        """LPOpenIntent extracts positive pool-token legs for bootstrap funding."""
        from almanak.framework.intents.vocabulary import LPOpenIntent

        intent = LPOpenIntent(
            pool="WETH/USDC/500",
            amount0=Decimal("0"),
            amount1=Decimal("1000"),
            range_lower=Decimal("1500"),
            range_upper=Decimal("2500"),
        )

        tokens = _extract_tokens_from_intent(intent)

        assert tokens == {"USDC": Decimal("1000")}

    def test_unknown_intent_returns_empty(self):
        """Unknown intent types return empty dict."""
        mock_intent = MagicMock()
        mock_intent.intent_type = None
        tokens = _extract_tokens_from_intent(mock_intent)
        assert tokens == {}


class TestExtractTokensFromResult:
    """Test extraction from full decide() results."""

    def test_none_result(self):
        """None result (no action) yields empty."""
        assert _extract_tokens_from_result(None) == {}

    def test_single_intent(self):
        """Single intent is extracted."""
        from almanak.framework.intents.vocabulary import SwapIntent

        intent = SwapIntent(from_token="USDC", to_token="WETH", amount_usd=Decimal("500"))
        tokens = _extract_tokens_from_result(intent)
        assert "USDC" in tokens

    def test_intent_sequence(self):
        """IntentSequence extracts tokens from all steps."""
        from almanak.framework.intents.vocabulary import IntentSequence, SwapIntent

        seq = IntentSequence(
            intents=[
                SwapIntent(from_token="USDC", to_token="WETH", amount_usd=Decimal("1000")),
                SwapIntent(from_token="WETH", to_token="DAI", amount=Decimal("0.5")),
            ]
        )
        tokens = _extract_tokens_from_result(seq)
        assert "USDC" in tokens
        assert "WETH" in tokens

    def test_nested_intent_sequence_flattens_recursively(self):
        """Nested IntentSequence values are flattened before token extraction."""
        from almanak.framework.intents.vocabulary import IntentSequence, SwapIntent

        inner = IntentSequence(
            intents=[
                SwapIntent(from_token="USDC", to_token="WETH", amount_usd=Decimal("1000")),
            ],
        )
        outer = IntentSequence(
            intents=[
                inner,
                SwapIntent(from_token="WETH", to_token="DAI", amount=Decimal("0.5")),
            ],
        )

        tokens = _extract_tokens_from_result(outer)

        assert tokens["USDC"] == Decimal("1000")
        assert tokens["WETH"] == Decimal("0.5")

    def test_list_of_intents(self):
        """List of intents extracts tokens from all items."""
        from almanak.framework.intents.vocabulary import SwapIntent

        result = [
            SwapIntent(from_token="USDC", to_token="WETH", amount_usd=Decimal("500")),
            SwapIntent(from_token="DAI", to_token="WETH", amount_usd=Decimal("500")),
        ]
        tokens = _extract_tokens_from_result(result)
        assert "USDC" in tokens
        assert "DAI" in tokens


class TestInferTokenRequirements:
    """Test the full inference pipeline."""

    def test_infer_from_swap_strategy(self):
        """Strategy returning SwapIntent gets inferred tokens."""
        from almanak.framework.intents.vocabulary import SwapIntent

        mock_strategy = MagicMock()
        mock_strategy.decide.return_value = SwapIntent(
            from_token="USDC", to_token="WETH", amount_usd=Decimal("1000")
        )

        result = infer_token_requirements(mock_strategy, "arbitrum")
        assert "USDC" in result
        # Amount should be scaled by 1.5x
        assert result["USDC"] == (Decimal("1000") * SAFETY_BUFFER).quantize(Decimal("0.000001"))

    def test_infer_from_hold_strategy(self):
        """Strategy returning HoldIntent gets empty requirements."""
        from almanak.framework.intents.vocabulary import HoldIntent

        mock_strategy = MagicMock()
        mock_strategy.decide.return_value = HoldIntent(reason="waiting for entry")

        result = infer_token_requirements(mock_strategy, "arbitrum")
        assert result == {}

    def test_infer_from_none_result(self):
        """Strategy returning None gets empty requirements."""
        mock_strategy = MagicMock()
        mock_strategy.decide.return_value = None

        result = infer_token_requirements(mock_strategy, "arbitrum")
        assert result == {}

    def test_infer_handles_exception(self):
        """Strategy.decide() raising an exception returns empty."""
        mock_strategy = MagicMock()
        mock_strategy.decide.side_effect = ValueError("No market data")

        result = infer_token_requirements(mock_strategy, "arbitrum")
        assert result == {}

    def test_safety_buffer_applied(self):
        """Inferred amounts include 1.5x safety buffer."""
        from almanak.framework.intents.vocabulary import SwapIntent

        mock_strategy = MagicMock()
        mock_strategy.decide.return_value = SwapIntent(
            from_token="WETH", to_token="USDC", amount=Decimal("2.0")
        )

        result = infer_token_requirements(mock_strategy, "ethereum")
        assert "WETH" in result
        expected = (Decimal("2.0") * SAFETY_BUFFER).quantize(Decimal("0.000001"))
        assert result["WETH"] == expected


class TestCheckDivergence:
    """Test divergence checking between explicit and inferred amounts."""

    def test_no_warning_within_threshold(self, caplog):
        """No warning when amounts are within 20% threshold."""
        explicit = {"USDC": Decimal("10000")}
        inferred = {"USDC": Decimal("11000")}  # 10% diff

        with caplog.at_level(logging.WARNING):
            check_divergence(explicit, inferred)

        assert "divergence" not in caplog.text.lower()

    def test_warning_above_threshold(self, caplog):
        """Warning when amounts diverge >20%."""
        explicit = {"USDC": Decimal("10000")}
        inferred = {"USDC": Decimal("15000")}  # 50% diff

        with caplog.at_level(logging.WARNING):
            check_divergence(explicit, inferred)

        assert "divergence" in caplog.text.lower()
        assert "USDC" in caplog.text

    def test_no_overlap_no_warning(self, caplog):
        """No warning when tokens don't overlap."""
        explicit = {"USDC": Decimal("10000")}
        inferred = {"WETH": Decimal("5")}

        with caplog.at_level(logging.WARNING):
            check_divergence(explicit, inferred)

        assert "divergence" not in caplog.text.lower()

    def test_zero_amounts_skipped(self, caplog):
        """Zero amounts don't trigger warnings."""
        explicit = {"USDC": Decimal("0")}
        inferred = {"USDC": Decimal("10000")}

        with caplog.at_level(logging.WARNING):
            check_divergence(explicit, inferred)

        assert "divergence" not in caplog.text.lower()
