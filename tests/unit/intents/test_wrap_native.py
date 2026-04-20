"""Unit tests for wrap_native intent compilation.

Tests the WRAP_NATIVE compiler path: balance checks, gas reserve,
amount validation, and wrong-token rejection.
"""

from decimal import Decimal
from unittest.mock import patch

import pytest
from pydantic import ValidationError

from almanak.framework.intents.compiler import IntentCompiler, IntentCompilerConfig
from almanak.framework.intents.vocabulary import IntentType, WrapNativeIntent


@pytest.fixture()
def compiler():
    """Compiler for Arbitrum with placeholder prices."""
    return IntentCompiler(
        chain="arbitrum",
        wallet_address="0x" + "a" * 40,
        config=IntentCompilerConfig(allow_placeholder_prices=True),
    )


class TestWrapNativeCompilation:
    """Verify WRAP_NATIVE intent compilation."""

    def test_successful_wrap(self, compiler):
        """Compiling a valid wrap intent should succeed."""
        intent = WrapNativeIntent(token="WETH", amount=Decimal("0.5"), chain="arbitrum")

        # Mock: wallet has 1 ETH (1e18 wei)
        with patch.object(compiler, "_query_native_balance", return_value=10**18):
            result = compiler.compile(intent)

        assert result.status.value == "SUCCESS"
        assert result.action_bundle is not None
        assert result.action_bundle.intent_type == IntentType.WRAP_NATIVE.value
        # deposit() calldata
        assert result.transactions[0].data == "0xd0e30db0"
        # msg.value should be the wrap amount
        assert result.transactions[0].value == int(Decimal("0.5") * Decimal(10**18))

    def test_insufficient_balance_with_gas_reserve(self, compiler):
        """When wallet balance is enough for amount but not amount + gas, should fail."""
        intent = WrapNativeIntent(token="WETH", amount=Decimal("1.0"), chain="arbitrum")

        # Wallet has exactly 1 ETH — not enough for 1 ETH + 0.001 gas reserve
        with patch.object(compiler, "_query_native_balance", return_value=10**18):
            result = compiler.compile(intent)

        assert result.status.value == "FAILED"
        assert "Insufficient" in result.error
        assert "gas reserve" in result.error

    def test_zero_balance_fails(self, compiler):
        """Zero native balance should fail clearly."""
        intent = WrapNativeIntent(token="WETH", amount=Decimal("0.1"), chain="arbitrum")

        with patch.object(compiler, "_query_native_balance", return_value=0):
            result = compiler.compile(intent)

        assert result.status.value == "FAILED"
        assert "Insufficient" in result.error

    def test_wrap_all_reserves_gas(self, compiler):
        """Wrapping 'all' should reserve gas and wrap the remainder."""
        intent = WrapNativeIntent(token="WETH", amount="all", chain="arbitrum")

        balance = 10**18  # 1 ETH
        gas_reserve = int(Decimal("0.001") * Decimal(10**18))

        with patch.object(compiler, "_query_native_balance", return_value=balance):
            result = compiler.compile(intent)

        assert result.status.value == "SUCCESS"
        expected_value = balance - gas_reserve
        assert result.transactions[0].value == expected_value

    def test_wrap_all_too_little_for_gas(self, compiler):
        """If balance is less than gas reserve, wrap 'all' should fail."""
        intent = WrapNativeIntent(token="WETH", amount="all", chain="arbitrum")

        tiny_balance = int(Decimal("0.0005") * Decimal(10**18))  # 0.0005 ETH
        with patch.object(compiler, "_query_native_balance", return_value=tiny_balance):
            result = compiler.compile(intent)

        assert result.status.value == "FAILED"
        assert "too low" in result.error

    def test_wrong_token_rejected(self, compiler):
        """Wrapping a non-native token (e.g., USDC) should fail."""
        intent = WrapNativeIntent(token="USDC", amount=Decimal("100"), chain="arbitrum")

        with patch.object(compiler, "_query_native_balance", return_value=10**18):
            result = compiler.compile(intent)

        assert result.status.value == "FAILED"
        assert "not the wrapped native token" in result.error

    def test_unsupported_chain(self):
        """Unknown chain should fail to find wrapped native token."""
        compiler = IntentCompiler(
            chain="unknown_chain",
            wallet_address="0x" + "a" * 40,
            config=IntentCompilerConfig(allow_placeholder_prices=True),
        )
        intent = WrapNativeIntent(token="WETH", amount=Decimal("0.1"), chain="unknown_chain")

        result = compiler.compile(intent)

        assert result.status.value == "FAILED"
        assert "No wrapped native token" in result.error

    def test_multichain_polygon(self):
        """WMATIC wrap should work on polygon."""
        compiler = IntentCompiler(
            chain="polygon",
            wallet_address="0x" + "a" * 40,
            config=IntentCompilerConfig(allow_placeholder_prices=True),
        )
        intent = WrapNativeIntent(token="WMATIC", amount=Decimal("10"), chain="polygon")

        with patch.object(compiler, "_query_native_balance", return_value=20 * 10**18):
            result = compiler.compile(intent)

        assert result.status.value == "SUCCESS"
        assert result.action_bundle.intent_type == IntentType.WRAP_NATIVE.value


class TestWrapNativeIntent:
    """Test WrapNativeIntent model validation."""

    def test_valid_decimal_amount(self):
        intent = WrapNativeIntent(token="WETH", amount=Decimal("1.5"), chain="arbitrum")
        assert intent.amount == Decimal("1.5")
        assert intent.intent_type == IntentType.WRAP_NATIVE

    def test_valid_all_amount(self):
        intent = WrapNativeIntent(token="WETH", amount="all", chain="arbitrum")
        assert intent.amount == "all"
        assert intent.is_chained_amount is True

    def test_zero_amount_rejected(self):
        with pytest.raises(ValueError, match="must be positive"):
            WrapNativeIntent(token="WETH", amount=Decimal("0"), chain="arbitrum")

    def test_negative_amount_rejected(self):
        with pytest.raises(ValueError, match="must be positive"):
            WrapNativeIntent(token="WETH", amount=Decimal("-1"), chain="arbitrum")

    def test_invalid_string_amount_rejected(self):
        with pytest.raises((ValueError, ValidationError)):
            WrapNativeIntent(token="WETH", amount="invalid", chain="arbitrum")

    def test_serialize_roundtrip(self):
        intent = WrapNativeIntent(token="WETH", amount=Decimal("0.5"), chain="arbitrum")
        data = intent.serialize()
        assert data["type"] == "WRAP_NATIVE"
        restored = WrapNativeIntent.deserialize(data)
        assert restored.token == "WETH"
        assert restored.amount == Decimal("0.5")
