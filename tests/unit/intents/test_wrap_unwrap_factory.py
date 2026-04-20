"""Tests for Intent.wrap() and Intent.unwrap() factory methods (VIB-2119).

Validates that the factory methods on the Intent class correctly create
WrapNativeIntent and UnwrapNativeIntent instances with proper parameters.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import patch

import pytest

from almanak.framework.intents.compiler import IntentCompiler, IntentCompilerConfig
from almanak.framework.intents.vocabulary import (
    Intent,
    IntentType,
    UnwrapNativeIntent,
    WrapNativeIntent,
)


class TestIntentWrapFactory:
    """Test Intent.wrap() factory method."""

    def test_creates_wrap_intent(self) -> None:
        intent = Intent.wrap(token="WETH", amount=Decimal("0.01"), chain="arbitrum")
        assert isinstance(intent, WrapNativeIntent)
        assert intent.token == "WETH"
        assert intent.amount == Decimal("0.01")
        assert intent.chain == "arbitrum"
        assert intent.intent_type == IntentType.WRAP_NATIVE

    def test_wrap_amount_all(self) -> None:
        intent = Intent.wrap(token="WMATIC", amount="all", chain="polygon")
        assert isinstance(intent, WrapNativeIntent)
        assert intent.amount == "all"
        assert intent.is_chained_amount is True

    def test_wrap_chain_optional(self) -> None:
        intent = Intent.wrap(token="WETH", amount=Decimal("1"))
        assert intent.chain is None


class TestIntentUnwrapFactory:
    """Test Intent.unwrap() factory method."""

    def test_creates_unwrap_intent(self) -> None:
        intent = Intent.unwrap(token="WETH", amount=Decimal("0.01"), chain="arbitrum")
        assert isinstance(intent, UnwrapNativeIntent)
        assert intent.token == "WETH"
        assert intent.amount == Decimal("0.01")
        assert intent.chain == "arbitrum"
        assert intent.intent_type == IntentType.UNWRAP_NATIVE

    def test_unwrap_amount_all(self) -> None:
        intent = Intent.unwrap(token="WAVAX", amount="all", chain="avalanche")
        assert isinstance(intent, UnwrapNativeIntent)
        assert intent.amount == "all"
        assert intent.is_chained_amount is True

    def test_unwrap_chain_optional(self) -> None:
        intent = Intent.unwrap(token="WETH", amount=Decimal("1"))
        assert intent.chain is None

    def test_unwrap_compiles_with_balance(self) -> None:
        """Intent.unwrap() produces an intent that compiles successfully."""
        intent = Intent.unwrap(token="WETH", amount=Decimal("0.001"), chain="arbitrum")
        compiler = IntentCompiler(
            chain="arbitrum",
            wallet_address="0x" + "a" * 40,
            config=IntentCompilerConfig(allow_placeholder_prices=True),
        )
        with patch.object(compiler, "_query_erc20_balance", return_value=10**18):
            result = compiler.compile(intent)

        assert result.status.value == "SUCCESS"
        assert result.action_bundle is not None
        # Should produce a single withdraw() transaction
        txs = result.action_bundle.transactions
        assert len(txs) == 1
        tx_data = txs[0]["data"] if isinstance(txs[0], dict) else txs[0].data
        assert tx_data.startswith("0x2e1a7d4d")  # withdraw(uint256) selector

    def test_wrap_compiles_with_balance(self) -> None:
        """Intent.wrap() produces an intent that compiles successfully."""
        intent = Intent.wrap(token="WETH", amount=Decimal("0.001"), chain="arbitrum")
        compiler = IntentCompiler(
            chain="arbitrum",
            wallet_address="0x" + "a" * 40,
            config=IntentCompilerConfig(allow_placeholder_prices=True),
        )
        with patch.object(compiler, "_query_native_balance", return_value=10**18):
            result = compiler.compile(intent)

        assert result.status.value == "SUCCESS"
        assert result.action_bundle is not None
        txs = result.action_bundle.transactions
        assert len(txs) == 1
        tx_data = txs[0]["data"] if isinstance(txs[0], dict) else txs[0].data
        assert tx_data == "0xd0e30db0"  # deposit() selector
