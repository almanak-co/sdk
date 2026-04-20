"""Unit tests for Aave V3 intent compilation on Sonic chain.

Tests verify that IntentCompiler correctly compiles SupplyIntent, BorrowIntent,
RepayIntent, and WithdrawIntent for the aave_v3 protocol on Sonic.

Sonic-specific: pool address 0x5362dBb1e601abF3a4c14c22ffEdA64042E5eAA3,
USDC (bridged, 6 decimals), WETH (bridged, 18 decimals).
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.intents import BorrowIntent, RepayIntent, SupplyIntent, WithdrawIntent
from almanak.framework.intents.compiler import (
    CompilationStatus,
    IntentCompiler,
    IntentCompilerConfig,
)

# Sonic Aave V3 pool address
SONIC_AAVE_V3_POOL = "0x5362dBb1e601abF3a4c14c22ffEdA64042E5eAA3"

# Test wallet
TEST_WALLET = "0x1234567890123456789012345678901234567890"


@pytest.fixture
def sonic_compiler():
    """Create an IntentCompiler for Sonic with placeholder prices."""
    config = IntentCompilerConfig(allow_placeholder_prices=True)
    return IntentCompiler(chain="sonic", config=config)


# =============================================================================
# SUPPLY
# =============================================================================


class TestAaveV3SonicSupply:
    """Test _compile_supply for aave_v3 on Sonic."""

    def test_supply_usdc_success(self, sonic_compiler):
        """Supply USDC to Aave V3 on Sonic should compile successfully."""
        intent = SupplyIntent(
            token="USDC",
            amount=Decimal("100"),
            protocol="aave_v3",
            use_as_collateral=True,
        )

        result = sonic_compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle is not None
        assert result.action_bundle.metadata["protocol"] == "aave_v3"
        assert result.action_bundle.metadata["chain"] == "sonic"
        # Should have approve + supply TXs
        assert len(result.transactions) >= 2

    def test_supply_weth_success(self, sonic_compiler):
        """Supply WETH to Aave V3 on Sonic should compile successfully."""
        intent = SupplyIntent(
            token="WETH",
            amount=Decimal("0.01"),
            protocol="aave_v3",
            use_as_collateral=True,
        )

        result = sonic_compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle is not None
        assert len(result.transactions) >= 2

    def test_supply_uses_correct_pool_address(self, sonic_compiler):
        """Supply should target the Sonic Aave V3 pool."""
        intent = SupplyIntent(
            token="USDC",
            amount=Decimal("100"),
            protocol="aave_v3",
        )

        result = sonic_compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS
        # The supply TX should target the Sonic pool
        supply_tx = result.transactions[-1]  # Last TX is the supply
        assert supply_tx.to.lower() == SONIC_AAVE_V3_POOL.lower()

    def test_supply_zero_amount_fails_validation(self):
        """Supply with zero amount should fail at Pydantic validation."""
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            SupplyIntent(
                token="USDC",
                amount=Decimal("0"),
                protocol="aave_v3",
            )


# =============================================================================
# BORROW
# =============================================================================


class TestAaveV3SonicBorrow:
    """Test _compile_borrow for aave_v3 on Sonic."""

    def test_borrow_weth_success(self, sonic_compiler):
        """Borrow WETH against USDC collateral on Sonic."""
        intent = BorrowIntent(
            collateral_token="USDC",
            collateral_amount=Decimal("0"),  # Already supplied
            borrow_token="WETH",
            borrow_amount=Decimal("0.01"),
            protocol="aave_v3",
            interest_rate_mode="variable",
        )

        result = sonic_compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle is not None
        assert result.action_bundle.metadata["protocol"] == "aave_v3"
        assert result.action_bundle.metadata["chain"] == "sonic"

    def test_borrow_usdc_success(self, sonic_compiler):
        """Borrow USDC against WETH collateral on Sonic."""
        intent = BorrowIntent(
            collateral_token="WETH",
            collateral_amount=Decimal("0"),
            borrow_token="USDC",
            borrow_amount=Decimal("50"),
            protocol="aave_v3",
            interest_rate_mode="variable",
        )

        result = sonic_compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle is not None

    def test_borrow_targets_sonic_pool(self, sonic_compiler):
        """Borrow TX should target the Sonic Aave V3 pool."""
        intent = BorrowIntent(
            collateral_token="USDC",
            collateral_amount=Decimal("0"),
            borrow_token="WETH",
            borrow_amount=Decimal("0.01"),
            protocol="aave_v3",
            interest_rate_mode="variable",
        )

        result = sonic_compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS
        # Borrow TX should target the pool
        borrow_tx = result.transactions[-1]
        assert borrow_tx.to.lower() == SONIC_AAVE_V3_POOL.lower()


# =============================================================================
# REPAY
# =============================================================================


class TestAaveV3SonicRepay:
    """Test _compile_repay for aave_v3 on Sonic."""

    def test_repay_weth_success(self, sonic_compiler):
        """Repay WETH debt on Sonic."""
        intent = RepayIntent(
            token="WETH",
            amount=Decimal("0.01"),
            protocol="aave_v3",
        )

        result = sonic_compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle is not None
        assert result.action_bundle.metadata["protocol"] == "aave_v3"
        # Should have approve + repay TXs
        assert len(result.transactions) >= 2

    def test_repay_usdc_success(self, sonic_compiler):
        """Repay USDC debt on Sonic."""
        intent = RepayIntent(
            token="USDC",
            amount=Decimal("50"),
            protocol="aave_v3",
        )

        result = sonic_compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle is not None

    def test_repay_full_success(self, sonic_compiler):
        """Repay full debt (repay_full=True) on Sonic."""
        intent = RepayIntent(
            token="WETH",
            amount=Decimal("0.01"),
            protocol="aave_v3",
            repay_full=True,
        )

        result = sonic_compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle is not None

    def test_repay_targets_sonic_pool(self, sonic_compiler):
        """Repay TX should target the Sonic Aave V3 pool."""
        intent = RepayIntent(
            token="WETH",
            amount=Decimal("0.01"),
            protocol="aave_v3",
        )

        result = sonic_compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS
        repay_tx = result.transactions[-1]
        assert repay_tx.to.lower() == SONIC_AAVE_V3_POOL.lower()


# =============================================================================
# WITHDRAW
# =============================================================================


class TestAaveV3SonicWithdraw:
    """Test _compile_withdraw for aave_v3 on Sonic."""

    def test_withdraw_usdc_success(self, sonic_compiler):
        """Withdraw USDC from Aave V3 on Sonic."""
        intent = WithdrawIntent(
            token="USDC",
            amount=Decimal("100"),
            protocol="aave_v3",
        )

        result = sonic_compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle is not None
        assert result.action_bundle.metadata["protocol"] == "aave_v3"

    def test_withdraw_weth_success(self, sonic_compiler):
        """Withdraw WETH from Aave V3 on Sonic."""
        intent = WithdrawIntent(
            token="WETH",
            amount=Decimal("0.01"),
            protocol="aave_v3",
        )

        result = sonic_compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle is not None

    def test_withdraw_all_success(self, sonic_compiler):
        """Withdraw all (withdraw_all=True) from Aave V3 on Sonic."""
        intent = WithdrawIntent(
            token="USDC",
            amount=Decimal("100"),
            protocol="aave_v3",
            withdraw_all=True,
        )

        result = sonic_compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle is not None

    def test_withdraw_targets_sonic_pool(self, sonic_compiler):
        """Withdraw TX should target the Sonic Aave V3 pool."""
        intent = WithdrawIntent(
            token="USDC",
            amount=Decimal("100"),
            protocol="aave_v3",
        )

        result = sonic_compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS
        withdraw_tx = result.transactions[-1]
        assert withdraw_tx.to.lower() == SONIC_AAVE_V3_POOL.lower()


# =============================================================================
# CROSS-CUTTING
# =============================================================================


class TestAaveV3SonicCrossCutting:
    """Cross-cutting tests for Aave V3 on Sonic."""

    def test_sonic_pool_address_is_configured(self):
        """Verify Sonic has an Aave V3 pool address configured in the compiler."""
        from almanak.framework.intents.compiler import LENDING_POOL_ADDRESSES

        assert "sonic" in LENDING_POOL_ADDRESSES
        assert "aave_v3" in LENDING_POOL_ADDRESSES["sonic"]
        assert LENDING_POOL_ADDRESSES["sonic"]["aave_v3"] == SONIC_AAVE_V3_POOL

    def test_sonic_usdc_resolves(self):
        """Verify USDC resolves on Sonic chain."""
        from almanak.framework.data.tokens import get_token_resolver

        resolver = get_token_resolver()
        token = resolver.resolve("USDC", "sonic")
        assert token is not None
        assert token.decimals == 6
        assert token.address.lower() == "0x29219dd400f2Bf60E5a23d13Be72B486D4038894".lower()

    def test_sonic_weth_resolves(self):
        """Verify WETH resolves on Sonic chain."""
        from almanak.framework.data.tokens import get_token_resolver

        resolver = get_token_resolver()
        token = resolver.resolve("WETH", "sonic")
        assert token is not None
        assert token.decimals == 18
        assert token.address.lower() == "0x50c42dEAcD8Fc9773493ED674b675bE577f2634b".lower()

    def test_sonic_chainlink_feeds_exist(self):
        """Verify Sonic has Chainlink price feeds configured."""
        from almanak.core.chainlink import CHAINLINK_PRICE_FEEDS

        assert "sonic" in CHAINLINK_PRICE_FEEDS
        sonic_feeds = CHAINLINK_PRICE_FEEDS["sonic"]
        assert "ETH/USD" in sonic_feeds
        assert "USDC/USD" in sonic_feeds
        assert "S/USD" in sonic_feeds

    def test_all_lending_intents_compile(self, sonic_compiler):
        """All 4 lending intent types should compile on Sonic."""
        supply = SupplyIntent(token="USDC", amount=Decimal("100"), protocol="aave_v3")
        borrow = BorrowIntent(
            collateral_token="USDC",
            collateral_amount=Decimal("0"),
            borrow_token="WETH",
            borrow_amount=Decimal("0.01"),
            protocol="aave_v3",
            interest_rate_mode="variable",
        )
        repay = RepayIntent(token="WETH", amount=Decimal("0.01"), protocol="aave_v3")
        withdraw = WithdrawIntent(token="USDC", amount=Decimal("100"), protocol="aave_v3")

        for intent in [supply, borrow, repay, withdraw]:
            result = sonic_compiler.compile(intent)
            assert result.status == CompilationStatus.SUCCESS, (
                f"{intent.intent_type.value} failed on Sonic: {result.error}"
            )
