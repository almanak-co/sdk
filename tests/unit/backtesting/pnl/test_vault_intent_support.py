"""Tests for VAULT_DEPOSIT and VAULT_REDEEM intent support in the PnL backtester.

Verifies that the PnL backtester correctly handles ERC-4626 vault intents:
- IntentType enum includes VAULT_DEPOSIT and VAULT_REDEEM
- Intent type detection works for VaultDepositIntent and VaultRedeemIntent
- Amount extraction handles vault-specific fields (amount, shares, deposit_token)
- Token flows are correct (deposit: token out, redeem: token in)
- Gas estimates are non-zero
- Zero slippage is applied (ERC-4626 deposits are 1:1)
- Position creation works for VAULT_DEPOSIT

VIB-1396: PnL backtest Morpho lending strategy on Ethereum
"""

from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest

from almanak.framework.backtesting.models import IntentType
from almanak.framework.backtesting.pnl.data_provider import MarketState
from almanak.framework.backtesting.pnl.engine import (
    PnLBacktester,
    _ZERO_SLIPPAGE_INTENTS,
)
from almanak.framework.intents.vocabulary import (
    VaultDepositIntent,
    VaultRedeemIntent,
)


class MockDataProvider:
    """Mock data provider for testing."""

    provider_name = "mock"

    async def iterate(self, config: Any):
        if False:
            yield


@dataclass
class MockVaultDepositIntent:
    """Mock vault deposit intent with deposit_token."""

    intent_type: str = "VAULT_DEPOSIT"
    protocol: str = "metamorpho"
    vault_address: str = "0xBEEF01735c132Ada46AA9aA4c54623cAA92A64CB"
    amount: Decimal = field(default_factory=lambda: Decimal("1000"))
    deposit_token: str = "USDC"
    chain: str = "ethereum"


@dataclass
class MockVaultRedeemIntent:
    """Mock vault redeem intent with deposit_token."""

    intent_type: str = "VAULT_REDEEM"
    protocol: str = "metamorpho"
    vault_address: str = "0xBEEF01735c132Ada46AA9aA4c54623cAA92A64CB"
    shares: Decimal = field(default_factory=lambda: Decimal("950"))
    deposit_token: str = "USDC"
    chain: str = "ethereum"


@dataclass
class MockVaultDepositNoToken:
    """Mock vault deposit without deposit_token (should fallback to USDC)."""

    intent_type: str = "VAULT_DEPOSIT"
    protocol: str = "metamorpho"
    vault_address: str = "0xBEEF01735c132Ada46AA9aA4c54623cAA92A64CB"
    amount: Decimal = field(default_factory=lambda: Decimal("500"))
    chain: str = "ethereum"


@pytest.fixture
def backtester():
    """Create a PnLBacktester instance for testing."""
    from almanak.framework.backtesting.pnl.engine import DefaultFeeModel, DefaultSlippageModel

    fee_models = {"default": DefaultFeeModel()}
    slippage_models = {"default": DefaultSlippageModel()}
    return PnLBacktester(MockDataProvider(), fee_models, slippage_models)


@pytest.fixture
def market_state():
    """Create a market state with USDC price."""
    prices = {"USDC": Decimal("1.0"), "WETH": Decimal("3000.0")}
    return MarketState(
        timestamp=datetime.now(UTC),
        prices=prices,
    )


# ---------------------------------------------------------------------------
# IntentType enum tests
# ---------------------------------------------------------------------------


class TestVaultIntentTypeEnum:
    """Test that VAULT_DEPOSIT and VAULT_REDEEM exist in backtester IntentType."""

    def test_vault_deposit_in_enum(self):
        assert IntentType.VAULT_DEPOSIT == "VAULT_DEPOSIT"

    def test_vault_redeem_in_enum(self):
        assert IntentType.VAULT_REDEEM == "VAULT_REDEEM"

    def test_vault_deposit_from_string(self):
        assert IntentType("VAULT_DEPOSIT") == IntentType.VAULT_DEPOSIT

    def test_vault_redeem_from_string(self):
        assert IntentType("VAULT_REDEEM") == IntentType.VAULT_REDEEM


# ---------------------------------------------------------------------------
# Zero slippage classification tests
# ---------------------------------------------------------------------------


class TestVaultZeroSlippage:
    """Test that vault intents are classified as zero-slippage."""

    def test_vault_deposit_zero_slippage(self):
        assert IntentType.VAULT_DEPOSIT in _ZERO_SLIPPAGE_INTENTS

    def test_vault_redeem_zero_slippage(self):
        assert IntentType.VAULT_REDEEM in _ZERO_SLIPPAGE_INTENTS


# ---------------------------------------------------------------------------
# Intent type detection tests
# ---------------------------------------------------------------------------


class TestVaultIntentTypeDetection:
    """Test that _get_intent_type correctly identifies vault intents."""

    def test_detect_vault_deposit_from_string(self, backtester):
        intent = MockVaultDepositIntent()
        result = backtester._get_intent_type(intent)
        assert result == IntentType.VAULT_DEPOSIT

    def test_detect_vault_redeem_from_string(self, backtester):
        intent = MockVaultRedeemIntent()
        result = backtester._get_intent_type(intent)
        assert result == IntentType.VAULT_REDEEM

    def test_detect_real_vault_deposit_intent(self, backtester):
        intent = VaultDepositIntent(
            protocol="metamorpho",
            vault_address="0xBEEF01735c132Ada46AA9aA4c54623cAA92A64CB",
            amount=Decimal("1000"),
            deposit_token="USDC",
            chain="ethereum",
        )
        result = backtester._get_intent_type(intent)
        assert result == IntentType.VAULT_DEPOSIT

    def test_detect_real_vault_redeem_intent(self, backtester):
        intent = VaultRedeemIntent(
            protocol="metamorpho",
            vault_address="0xBEEF01735c132Ada46AA9aA4c54623cAA92A64CB",
            shares=Decimal("1000"),
            deposit_token="USDC",
            chain="ethereum",
        )
        result = backtester._get_intent_type(intent)
        assert result == IntentType.VAULT_REDEEM


# ---------------------------------------------------------------------------
# Amount extraction tests
# ---------------------------------------------------------------------------


class TestVaultAmountExtraction:
    """Test that _get_intent_amount_usd works for vault intents."""

    def test_vault_deposit_amount_with_token(self, backtester, market_state):
        intent = MockVaultDepositIntent(amount=Decimal("1000"), deposit_token="USDC")
        amount = backtester._get_intent_amount_usd(intent, market_state)
        # 1000 USDC * $1.0 = $1000
        assert amount == Decimal("1000")

    def test_vault_deposit_amount_weth(self, backtester, market_state):
        intent = MockVaultDepositIntent(amount=Decimal("2"), deposit_token="WETH")
        amount = backtester._get_intent_amount_usd(intent, market_state)
        # 2 WETH * $3000 = $6000
        assert amount == Decimal("6000.0")

    def test_vault_redeem_shares_with_token(self, backtester, market_state):
        intent = MockVaultRedeemIntent(shares=Decimal("500"), deposit_token="USDC")
        amount = backtester._get_intent_amount_usd(intent, market_state)
        # 500 shares * $1.0 USDC = $500 (approximate, shares ~ underlying)
        assert amount == Decimal("500")

    def test_vault_deposit_no_token_fallback(self, backtester, market_state):
        intent = MockVaultDepositNoToken(amount=Decimal("500"))
        # No deposit_token means no token for conversion -> falls back to zero
        amount = backtester._get_intent_amount_usd(intent, market_state)
        assert amount == Decimal("0")

    def test_vault_redeem_shares_all(self, backtester, market_state):
        """Test that shares='all' does not crash Decimal conversion."""
        intent = MockVaultRedeemIntent(shares="all", deposit_token="USDC")
        amount = backtester._get_intent_amount_usd(intent, market_state)
        # 'all' is not numeric, so amount extraction returns 0 (no USD field)
        assert amount == Decimal("0")


# ---------------------------------------------------------------------------
# Token flow tests
# ---------------------------------------------------------------------------


class TestVaultTokenFlows:
    """Test that _calculate_token_flows handles vault intents correctly."""

    def test_vault_deposit_token_out(self, backtester, market_state):
        intent = MockVaultDepositIntent(deposit_token="USDC")
        tokens_in, tokens_out = backtester._calculate_token_flows(
            intent=intent,
            intent_type=IntentType.VAULT_DEPOSIT,
            amount_usd=Decimal("1000"),
            executed_price=Decimal("1.0"),
            fee_usd=Decimal("0"),
            slippage_usd=Decimal("0"),
            market_state=market_state,
        )
        assert "USDC" in tokens_out
        assert tokens_out["USDC"] == Decimal("1000")
        assert len(tokens_in) == 0

    def test_vault_redeem_token_in(self, backtester, market_state):
        intent = MockVaultRedeemIntent(deposit_token="USDC")
        tokens_in, tokens_out = backtester._calculate_token_flows(
            intent=intent,
            intent_type=IntentType.VAULT_REDEEM,
            amount_usd=Decimal("950"),
            executed_price=Decimal("1.0"),
            fee_usd=Decimal("0"),
            slippage_usd=Decimal("0"),
            market_state=market_state,
        )
        assert "USDC" in tokens_in
        assert tokens_in["USDC"] == Decimal("950")
        assert len(tokens_out) == 0

    def test_vault_deposit_no_token_defaults_usdc(self, backtester, market_state):
        intent = MockVaultDepositNoToken()
        tokens_in, tokens_out = backtester._calculate_token_flows(
            intent=intent,
            intent_type=IntentType.VAULT_DEPOSIT,
            amount_usd=Decimal("500"),
            executed_price=Decimal("1.0"),
            fee_usd=Decimal("0"),
            slippage_usd=Decimal("0"),
            market_state=market_state,
        )
        assert "USDC" in tokens_out
        assert tokens_out["USDC"] == Decimal("500")


# ---------------------------------------------------------------------------
# Gas estimate tests
# ---------------------------------------------------------------------------


class TestVaultGasEstimates:
    """Test that gas estimates are non-zero for vault intents."""

    def test_vault_deposit_gas(self, backtester):
        gas = backtester._estimate_gas_for_intent(IntentType.VAULT_DEPOSIT)
        assert gas > 0
        assert gas == 250000

    def test_vault_redeem_gas(self, backtester):
        gas = backtester._estimate_gas_for_intent(IntentType.VAULT_REDEEM)
        assert gas > 0
        assert gas == 200000


# ---------------------------------------------------------------------------
# Position creation tests
# ---------------------------------------------------------------------------


class TestVaultPositionCreation:
    """Test that _create_position_delta creates supply positions for vault deposits."""

    def test_vault_deposit_creates_supply_position(self, backtester, market_state):
        intent = MockVaultDepositIntent(amount=Decimal("1000"), deposit_token="USDC")
        position = backtester._create_position_delta(
            intent=intent,
            intent_type=IntentType.VAULT_DEPOSIT,
            protocol="metamorpho",
            tokens=["USDC"],
            executed_price=Decimal("1.0"),
            timestamp=datetime.now(UTC),
            market_state=market_state,
        )
        assert position is not None
        assert "USDC" in position.tokens
        assert position.position_type.value == "SUPPLY"

    def test_vault_redeem_no_position(self, backtester, market_state):
        intent = MockVaultRedeemIntent(shares=Decimal("1000"), deposit_token="USDC")
        position = backtester._create_position_delta(
            intent=intent,
            intent_type=IntentType.VAULT_REDEEM,
            protocol="metamorpho",
            tokens=["USDC"],
            executed_price=Decimal("1.0"),
            timestamp=datetime.now(UTC),
            market_state=market_state,
        )
        # VAULT_REDEEM doesn't create a new position (it closes one)
        assert position is None


# ---------------------------------------------------------------------------
# Token extraction tests
# ---------------------------------------------------------------------------


class TestVaultTokenExtraction:
    """Test that _get_intent_tokens finds deposit_token."""

    def test_get_tokens_from_vault_deposit(self, backtester):
        intent = MockVaultDepositIntent(deposit_token="USDC")
        tokens = backtester._get_intent_tokens(intent)
        assert "USDC" in tokens

    def test_get_tokens_from_vault_redeem(self, backtester):
        intent = MockVaultRedeemIntent(deposit_token="USDC")
        tokens = backtester._get_intent_tokens(intent)
        assert "USDC" in tokens


# ---------------------------------------------------------------------------
# VaultDepositIntent / VaultRedeemIntent field tests
# ---------------------------------------------------------------------------


class TestVaultIntentDepositTokenField:
    """Test that vault intent classes accept optional deposit_token."""

    def test_vault_deposit_with_deposit_token(self):
        intent = VaultDepositIntent(
            protocol="metamorpho",
            vault_address="0xBEEF01735c132Ada46AA9aA4c54623cAA92A64CB",
            amount=Decimal("1000"),
            deposit_token="USDC",
            chain="ethereum",
        )
        assert intent.deposit_token == "USDC"
        assert intent.amount == Decimal("1000")

    def test_vault_deposit_without_deposit_token(self):
        intent = VaultDepositIntent(
            protocol="metamorpho",
            vault_address="0xBEEF01735c132Ada46AA9aA4c54623cAA92A64CB",
            amount=Decimal("1000"),
        )
        assert intent.deposit_token is None

    def test_vault_redeem_with_deposit_token(self):
        intent = VaultRedeemIntent(
            protocol="metamorpho",
            vault_address="0xBEEF01735c132Ada46AA9aA4c54623cAA92A64CB",
            shares=Decimal("1000"),
            deposit_token="USDC",
        )
        assert intent.deposit_token == "USDC"

    def test_vault_redeem_without_deposit_token(self):
        intent = VaultRedeemIntent(
            protocol="metamorpho",
            vault_address="0xBEEF01735c132Ada46AA9aA4c54623cAA92A64CB",
            shares=Decimal("1000"),
        )
        assert intent.deposit_token is None

    def test_intent_factory_vault_deposit_with_token(self):
        from almanak.framework.intents.vocabulary import Intent

        intent = Intent.vault_deposit(
            protocol="metamorpho",
            vault_address="0xBEEF01735c132Ada46AA9aA4c54623cAA92A64CB",
            amount=Decimal("500"),
            deposit_token="USDC",
            chain="ethereum",
        )
        assert isinstance(intent, VaultDepositIntent)
        assert intent.deposit_token == "USDC"

    def test_intent_factory_vault_redeem_with_token(self):
        from almanak.framework.intents.vocabulary import Intent

        intent = Intent.vault_redeem(
            protocol="metamorpho",
            vault_address="0xBEEF01735c132Ada46AA9aA4c54623cAA92A64CB",
            shares=Decimal("500"),
            deposit_token="USDC",
            chain="ethereum",
        )
        assert isinstance(intent, VaultRedeemIntent)
        assert intent.deposit_token == "USDC"
