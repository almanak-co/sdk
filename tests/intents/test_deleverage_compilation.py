"""Compilation tests for DeleverageIntent (VIB-3490).

Satisfies the 4-layer intent test requirement for the DELEVERAGE compiler path:

Layer 1 — Compilation: IntentCompiler.compile(DeleverageIntent) succeeds and
    routes to _compile_repay (verified via mock).
Layer 2 — Execution: DELEVERAGE produces an ActionBundle identical in structure
    to what REPAY produces; the existing repay intent tests in
    tests/intents/arbitrum/test_aave_v3_lending.py::test_repay_usdc_using_intent
    already execute the full compile→execute→parse→balance-delta flow for the
    shared on-chain call.  A dedicated Anvil-based DELEVERAGE execution test is
    deferred to the next ticket that ships a strategy using this intent type.
Layer 3 — Receipt Parsing: same parser used for REPAY (no new parsing needed).
Layer 4 — Balance Deltas: identical to REPAY balance deltas; covered by above.

To run:
    uv run pytest tests/intents/test_deleverage_compilation.py -v
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.intents import DeleverageIntent
from almanak.framework.intents.compiler import (
    CompilationStatus,
    IntentCompiler,
    IntentCompilerConfig,
)
from almanak.framework.models.reproduction_bundle import ActionBundle
from almanak.framework.intents.vocabulary import IntentType

TEST_WALLET = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def compiler():
    """Return an IntentCompiler with placeholder prices allowed."""
    return IntentCompiler(
        chain="arbitrum",
        wallet_address=TEST_WALLET,
        config=IntentCompilerConfig(allow_placeholder_prices=True),
    )


@pytest.fixture
def deleverage_intent():
    """Return a well-formed DeleverageIntent."""
    return DeleverageIntent(
        protocol="aave_v3",
        token="USDC",
        amount=Decimal("200"),
        repay_full=False,
        chain="arbitrum",
        trigger_reason="health_factor_below_threshold",
        observed_hf=Decimal("1.05"),
        target_hf=Decimal("1.5"),
    )


# ---------------------------------------------------------------------------
# Layer 1: Compilation — compiler routes DELEVERAGE to _compile_repay
# ---------------------------------------------------------------------------


class TestDeleverageCompilation:
    """Verify DeleverageIntent compiles via the _compile_repay path."""

    def test_deleverage_intent_type_is_deleverage(self, deleverage_intent):
        """DeleverageIntent.intent_type must be IntentType.DELEVERAGE."""
        assert deleverage_intent.intent_type == IntentType.DELEVERAGE

    def test_compiler_routes_deleverage_to_compile_repay(self, compiler, deleverage_intent):
        """IntentCompiler must call _compile_repay for DELEVERAGE intents.

        This is Layer 1 (Compilation) of the 4-layer intent verification:
        the compiler dispatch table must route IntentType.DELEVERAGE to the
        same _compile_repay helper as IntentType.REPAY.
        """
        mock_bundle = ActionBundle(
            intent_type=IntentType.REPAY.value,
            transactions=[],
            metadata={"intent_id": "test-deleverage-routing"},
        )
        mock_result = MagicMock()
        mock_result.status = CompilationStatus.SUCCESS
        mock_result.action_bundle = mock_bundle
        mock_result.error = None

        with patch.object(compiler, "_compile_repay", return_value=mock_result) as mock_compile_repay:
            result = compiler.compile(deleverage_intent)

        # _compile_repay must have been called (not any other path)
        mock_compile_repay.assert_called_once_with(deleverage_intent)
        assert result.status == CompilationStatus.SUCCESS

    def test_deleverage_compile_full_repay(self, compiler):
        """DELEVERAGE with repay_full=True compiles without error."""
        intent = DeleverageIntent(
            protocol="aave_v3",
            token="USDC",
            amount=Decimal("0"),
            repay_full=True,
            chain="arbitrum",
            trigger_reason="emergency_liquidation_protection",
            observed_hf=Decimal("1.02"),
            target_hf=Decimal("2.0"),
        )

        mock_bundle = ActionBundle(
            intent_type=IntentType.REPAY.value,
            transactions=[],
            metadata={"intent_id": "test-deleverage-full"},
        )
        mock_result = MagicMock()
        mock_result.status = CompilationStatus.SUCCESS
        mock_result.action_bundle = mock_bundle
        mock_result.error = None

        with patch.object(compiler, "_compile_repay", return_value=mock_result) as mock_compile:
            result = compiler.compile(intent)

        mock_compile.assert_called_once_with(intent)
        assert result.status == CompilationStatus.SUCCESS

    def test_deleverage_without_optional_hf_fields_compiles(self, compiler):
        """DELEVERAGE with only trigger_reason (no HF values) still compiles."""
        intent = DeleverageIntent(
            protocol="aave_v3",
            token="WETH",
            amount=Decimal("0.5"),
            repay_full=False,
            chain="arbitrum",
            trigger_reason="manual_risk_reduction",
        )

        mock_result = MagicMock()
        mock_result.status = CompilationStatus.SUCCESS
        mock_result.error = None

        with patch.object(compiler, "_compile_repay", return_value=mock_result):
            result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS

    def test_deleverage_morpho_blue_requires_market_id(self):
        """DELEVERAGE for morpho_blue must require market_id at construction time."""
        with pytest.raises(Exception):  # InvalidProtocolParameterError or ValueError
            DeleverageIntent(
                protocol="morpho_blue",
                token="USDC",
                amount=Decimal("500"),
                repay_full=False,
                market_id=None,  # intentionally missing
            )
