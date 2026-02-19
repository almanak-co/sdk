"""Tests for VaultLifecycleManager.run_settlement_cycle() with valuation bounds."""

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.core.models.config import VaultVersion
from almanak.framework.vault.config import SettlementPhase, SettlementResult, VaultConfig, VaultState
from almanak.framework.vault.lifecycle import VAULT_STATE_KEY, VaultLifecycleManager


def _make_config(**overrides) -> VaultConfig:
    defaults = {
        "vault_address": "0x1111111111111111111111111111111111111111",
        "valuator_address": "0x3333333333333333333333333333333333333333",
        "underlying_token": "USDC",
        "settlement_interval_minutes": 60,
        "min_valuation_change_down_bps": 500,
        "max_valuation_change_up_bps": 1000,
    }
    defaults.update(overrides)
    return VaultConfig(**defaults)


def _make_strategy(chain: str = "ethereum", wallet_address: str = "0x3333333333333333333333333333333333333333"):
    """Create a mock strategy with valuate() and create_market_snapshot()."""
    strategy = MagicMock()
    strategy.chain = chain
    strategy.wallet_address = wallet_address
    return strategy


def _make_market(underlying_price: Decimal = Decimal("1.0"), total_portfolio_usd: Decimal = Decimal("10000")):
    """Create a mock MarketSnapshot."""
    market = MagicMock()
    market.price.return_value = underlying_price
    market.total_portfolio_usd.return_value = total_portfolio_usd
    return market


def _make_execution_result(success: bool = True, error: str | None = None):
    """Create a mock execution result."""
    result = MagicMock()
    result.success = success
    result.error = error
    result.receipts = [{}]
    return result


def _make_manager(
    vault_config: VaultConfig | None = None,
    strategy_id: str = "test-strategy-1",
    vault_state: VaultState | None = None,
) -> VaultLifecycleManager:
    """Create a VaultLifecycleManager with mocked dependencies."""
    config = vault_config or _make_config()
    sdk = MagicMock()
    adapter = MagicMock()
    orchestrator = MagicMock()
    orchestrator.execute = AsyncMock()

    # Default preflight mocks: pass all checks with valuator matching config
    sdk.verify_version.return_value = None
    sdk.get_valuation_manager.return_value = config.valuator_address
    sdk.get_curator.return_value = "0x3333333333333333333333333333333333333333"

    manager = VaultLifecycleManager(
        vault_config=config,
        vault_sdk=sdk,
        vault_adapter=adapter,
        execution_orchestrator=orchestrator,
        strategy_id=strategy_id,
    )

    if vault_state is not None:
        manager._vault_state = vault_state

    return manager


class TestSuccessfulSettlementCycle:
    """Tests for a successful full settlement cycle."""

    def test_full_cycle_returns_success(self):
        """A complete settlement cycle returns SettlementResult(success=True)."""
        manager = _make_manager(
            vault_state=VaultState(
                initialized=True,
                last_total_assets=10_000_000,  # 10 USDC (6 decimals)
                settlement_phase=SettlementPhase.IDLE,
                last_valuation_time=datetime.now(UTC) - timedelta(hours=2),
            ),
        )

        # Strategy valuates at $10 USD with USDC at $1
        strategy = _make_strategy()
        market = _make_market(underlying_price=Decimal("1.0"), total_portfolio_usd=Decimal("10"))
        strategy.create_market_snapshot.return_value = market
        strategy.valuate.return_value = Decimal("10")

        # Both executions succeed
        manager._execution_orchestrator.execute = AsyncMock(return_value=_make_execution_result(success=True))

        # Mock token resolver
        with patch("almanak.framework.vault.lifecycle.get_token_resolver") as mock_resolver:
            mock_resolver.return_value.get_decimals.return_value = 6

            result = asyncio.run(manager.run_settlement_cycle(strategy))

        assert result.success is True
        assert result.new_total_assets == 10_000_000  # 10 * 10^6
        assert result.epoch_id == 1

    def test_full_cycle_updates_vault_state(self):
        """Settlement cycle updates vault state to IDLE with new values."""
        manager = _make_manager(
            vault_state=VaultState(
                initialized=True,
                last_total_assets=10_000_000,
                settlement_phase=SettlementPhase.IDLE,
                last_valuation_time=datetime.now(UTC) - timedelta(hours=2),
                last_settlement_epoch=5,
            ),
        )

        strategy = _make_strategy()
        market = _make_market(underlying_price=Decimal("1.0"), total_portfolio_usd=Decimal("10"))
        strategy.create_market_snapshot.return_value = market
        strategy.valuate.return_value = Decimal("10")

        manager._execution_orchestrator.execute = AsyncMock(return_value=_make_execution_result(success=True))

        with patch("almanak.framework.vault.lifecycle.get_token_resolver") as mock_resolver:
            mock_resolver.return_value.get_decimals.return_value = 6
            asyncio.run(manager.run_settlement_cycle(strategy))

        state = manager.get_vault_state()
        assert state.settlement_phase == SettlementPhase.IDLE
        assert state.last_total_assets == 10_000_000
        assert state.last_settlement_epoch == 6
        assert state.last_valuation_time is not None
        assert state.initialized is True

    def test_propose_and_settle_bundles_built_correctly(self):
        """Adapter is called with correct params for propose and settle."""
        manager = _make_manager(
            vault_state=VaultState(
                initialized=True,
                last_total_assets=5_000_000,
                settlement_phase=SettlementPhase.IDLE,
                last_valuation_time=datetime.now(UTC) - timedelta(hours=2),
            ),
        )

        strategy = _make_strategy()
        market = _make_market(underlying_price=Decimal("1.0"), total_portfolio_usd=Decimal("5"))
        strategy.create_market_snapshot.return_value = market
        strategy.valuate.return_value = Decimal("5")

        manager._execution_orchestrator.execute = AsyncMock(return_value=_make_execution_result(success=True))

        with patch("almanak.framework.vault.lifecycle.get_token_resolver") as mock_resolver:
            mock_resolver.return_value.get_decimals.return_value = 6
            asyncio.run(manager.run_settlement_cycle(strategy))

        # Verify propose bundle was built
        propose_call = manager._vault_adapter.build_propose_valuation_bundle.call_args
        assert propose_call is not None
        propose_params = propose_call[0][0]
        assert propose_params.vault_address == "0x1111111111111111111111111111111111111111"
        assert propose_params.valuator_address == "0x3333333333333333333333333333333333333333"
        assert propose_params.new_total_assets == 5_000_000

        # Verify settle deposit bundle was built
        settle_call = manager._vault_adapter.build_settle_deposit_bundle.call_args
        assert settle_call is not None
        settle_params = settle_call[0][0]
        assert settle_params.vault_address == "0x1111111111111111111111111111111111111111"
        assert settle_params.safe_address == strategy.wallet_address
        assert settle_params.total_assets == 5_000_000

    def test_settle_redeems_called_when_auto_settle_enabled(self):
        """When auto_settle_redeems is True, settle_redeem bundle is built."""
        manager = _make_manager(
            vault_config=_make_config(auto_settle_redeems=True),
            vault_state=VaultState(
                initialized=True,
                last_total_assets=10_000_000,
                settlement_phase=SettlementPhase.IDLE,
                last_valuation_time=datetime.now(UTC) - timedelta(hours=2),
            ),
        )

        strategy = _make_strategy()
        market = _make_market()
        strategy.create_market_snapshot.return_value = market
        strategy.valuate.return_value = Decimal("10")

        manager._execution_orchestrator.execute = AsyncMock(return_value=_make_execution_result(success=True))

        with patch("almanak.framework.vault.lifecycle.get_token_resolver") as mock_resolver:
            mock_resolver.return_value.get_decimals.return_value = 6
            asyncio.run(manager.run_settlement_cycle(strategy))

        assert manager._vault_adapter.build_settle_redeem_bundle.called

    def test_settle_redeems_not_called_when_disabled(self):
        """When auto_settle_redeems is False, settle_redeem is skipped."""
        manager = _make_manager(
            vault_config=_make_config(auto_settle_redeems=False),
            vault_state=VaultState(
                initialized=True,
                last_total_assets=10_000_000,
                settlement_phase=SettlementPhase.IDLE,
                last_valuation_time=datetime.now(UTC) - timedelta(hours=2),
            ),
        )

        strategy = _make_strategy()
        market = _make_market()
        strategy.create_market_snapshot.return_value = market
        strategy.valuate.return_value = Decimal("10")

        manager._execution_orchestrator.execute = AsyncMock(return_value=_make_execution_result(success=True))

        with patch("almanak.framework.vault.lifecycle.get_token_resolver") as mock_resolver:
            mock_resolver.return_value.get_decimals.return_value = 6
            asyncio.run(manager.run_settlement_cycle(strategy))

        assert not manager._vault_adapter.build_settle_redeem_bundle.called


class TestValuationBoundsViolation:
    """Tests for valuation bounds checks."""

    def test_rejects_excessive_increase(self):
        """Valuation increase beyond max_valuation_change_up_bps is rejected."""
        manager = _make_manager(
            vault_config=_make_config(max_valuation_change_up_bps=1000),  # 10%
            vault_state=VaultState(
                initialized=True,
                last_total_assets=10_000_000,  # 10 USDC
                settlement_phase=SettlementPhase.IDLE,
                last_valuation_time=datetime.now(UTC) - timedelta(hours=2),
            ),
        )

        strategy = _make_strategy()
        # 15 USDC = 50% increase, exceeds 10% max
        market = _make_market(underlying_price=Decimal("1.0"), total_portfolio_usd=Decimal("15"))
        strategy.create_market_snapshot.return_value = market
        strategy.valuate.return_value = Decimal("15")

        with patch("almanak.framework.vault.lifecycle.get_token_resolver") as mock_resolver:
            mock_resolver.return_value.get_decimals.return_value = 6
            result = asyncio.run(manager.run_settlement_cycle(strategy))

        assert result.success is False
        # State should be reset to IDLE
        assert manager.get_vault_state().settlement_phase == SettlementPhase.IDLE

    def test_rejects_excessive_decrease(self):
        """Valuation decrease beyond min_valuation_change_down_bps is rejected."""
        manager = _make_manager(
            vault_config=_make_config(min_valuation_change_down_bps=500),  # 5%
            vault_state=VaultState(
                initialized=True,
                last_total_assets=10_000_000,  # 10 USDC
                settlement_phase=SettlementPhase.IDLE,
                last_valuation_time=datetime.now(UTC) - timedelta(hours=2),
            ),
        )

        strategy = _make_strategy()
        # 4 USDC = 60% decrease, exceeds 5% max
        market = _make_market(underlying_price=Decimal("1.0"), total_portfolio_usd=Decimal("4"))
        strategy.create_market_snapshot.return_value = market
        strategy.valuate.return_value = Decimal("4")

        with patch("almanak.framework.vault.lifecycle.get_token_resolver") as mock_resolver:
            mock_resolver.return_value.get_decimals.return_value = 6
            result = asyncio.run(manager.run_settlement_cycle(strategy))

        assert result.success is False
        assert manager.get_vault_state().settlement_phase == SettlementPhase.IDLE

    def test_allows_change_within_bounds(self):
        """Valuation change within bounds proceeds normally."""
        manager = _make_manager(
            vault_config=_make_config(max_valuation_change_up_bps=1000, min_valuation_change_down_bps=500),
            vault_state=VaultState(
                initialized=True,
                last_total_assets=10_000_000,  # 10 USDC
                settlement_phase=SettlementPhase.IDLE,
                last_valuation_time=datetime.now(UTC) - timedelta(hours=2),
            ),
        )

        strategy = _make_strategy()
        # 10.5 USDC = 5% increase, within 10% max
        market = _make_market(underlying_price=Decimal("1.0"), total_portfolio_usd=Decimal("10.5"))
        strategy.create_market_snapshot.return_value = market
        strategy.valuate.return_value = Decimal("10.5")

        manager._execution_orchestrator.execute = AsyncMock(return_value=_make_execution_result(success=True))

        with patch("almanak.framework.vault.lifecycle.get_token_resolver") as mock_resolver:
            mock_resolver.return_value.get_decimals.return_value = 6
            result = asyncio.run(manager.run_settlement_cycle(strategy))

        assert result.success is True
        assert result.new_total_assets == 10_500_000

    def test_no_bounds_check_when_last_total_assets_is_zero(self):
        """Bounds check is skipped when last_total_assets is 0 (prevents division by zero)."""
        manager = _make_manager(
            vault_state=VaultState(
                initialized=True,
                last_total_assets=0,
                settlement_phase=SettlementPhase.IDLE,
                last_valuation_time=datetime.now(UTC) - timedelta(hours=2),
            ),
        )

        strategy = _make_strategy()
        market = _make_market(underlying_price=Decimal("1.0"), total_portfolio_usd=Decimal("1000000"))
        strategy.create_market_snapshot.return_value = market
        strategy.valuate.return_value = Decimal("1000000")

        manager._execution_orchestrator.execute = AsyncMock(return_value=_make_execution_result(success=True))

        with patch("almanak.framework.vault.lifecycle.get_token_resolver") as mock_resolver:
            mock_resolver.return_value.get_decimals.return_value = 6
            result = asyncio.run(manager.run_settlement_cycle(strategy))

        assert result.success is True


class TestFirstSettlementGuard:
    """Tests for first settlement (initialized=False) behavior."""

    def test_v050_first_settlement_forces_zero(self):
        """V0.5.0: First settlement forces total_assets=0."""
        manager = _make_manager(
            vault_config=_make_config(version="0.5.0"),
            vault_state=VaultState(
                initialized=False,
                last_total_assets=0,
                settlement_phase=SettlementPhase.IDLE,
            ),
        )

        strategy = _make_strategy()
        market = _make_market(underlying_price=Decimal("1.0"), total_portfolio_usd=Decimal("50"))
        strategy.create_market_snapshot.return_value = market
        strategy.valuate.return_value = Decimal("50")

        manager._execution_orchestrator.execute = AsyncMock(return_value=_make_execution_result(success=True))

        with patch("almanak.framework.vault.lifecycle.get_token_resolver") as mock_resolver:
            mock_resolver.return_value.get_decimals.return_value = 6
            result = asyncio.run(manager.run_settlement_cycle(strategy))

        assert result.success is True
        assert result.new_total_assets == 0  # Forced to 0 on first settlement

        # Verify adapter was called with 0
        propose_call = manager._vault_adapter.build_propose_valuation_bundle.call_args
        propose_params = propose_call[0][0]
        assert propose_params.new_total_assets == 0

    def test_first_settlement_marks_initialized(self):
        """After first settlement, vault state is marked as initialized."""
        manager = _make_manager(
            vault_state=VaultState(
                initialized=False,
                settlement_phase=SettlementPhase.IDLE,
            ),
        )

        strategy = _make_strategy()
        market = _make_market()
        strategy.create_market_snapshot.return_value = market
        strategy.valuate.return_value = Decimal("10")

        manager._execution_orchestrator.execute = AsyncMock(return_value=_make_execution_result(success=True))

        with patch("almanak.framework.vault.lifecycle.get_token_resolver") as mock_resolver:
            mock_resolver.return_value.get_decimals.return_value = 6
            asyncio.run(manager.run_settlement_cycle(strategy))

        assert manager.get_vault_state().initialized is True

    def test_first_settlement_not_initialized_on_failure(self):
        """If first settlement fails, initialized stays False (P1 fix)."""
        manager = _make_manager(
            vault_state=VaultState(
                initialized=False,
                settlement_phase=SettlementPhase.IDLE,
            ),
        )

        strategy = _make_strategy()
        market = _make_market()
        strategy.create_market_snapshot.return_value = market
        strategy.valuate.return_value = Decimal("10")

        # Propose fails
        manager._execution_orchestrator.execute = AsyncMock(
            return_value=_make_execution_result(success=False, error="tx reverted")
        )

        with patch("almanak.framework.vault.lifecycle.get_token_resolver") as mock_resolver:
            mock_resolver.return_value.get_decimals.return_value = 6
            result = asyncio.run(manager.run_settlement_cycle(strategy))

        assert result.success is False
        assert manager.get_vault_state().initialized is False  # NOT flipped on failure

    def test_pre_v050_first_settlement_does_not_force_zero(self):
        """Pre-V0.5.0: First settlement uses actual computed value (not forced to 0)."""
        manager = _make_manager(
            vault_config=_make_config(version="0.3.0"),
            vault_state=VaultState(
                initialized=False,
                last_total_assets=0,
                settlement_phase=SettlementPhase.IDLE,
            ),
        )

        strategy = _make_strategy()
        market = _make_market(underlying_price=Decimal("1.0"), total_portfolio_usd=Decimal("50"))
        strategy.create_market_snapshot.return_value = market
        strategy.valuate.return_value = Decimal("50")

        # Mock pending deposits for pre-V0.5.0
        manager._vault_sdk.get_pending_deposits.return_value = 5_000_000  # 5 USDC pending

        manager._execution_orchestrator.execute = AsyncMock(return_value=_make_execution_result(success=True))

        with patch("almanak.framework.vault.lifecycle.get_token_resolver") as mock_resolver:
            mock_resolver.return_value.get_decimals.return_value = 6
            result = asyncio.run(manager.run_settlement_cycle(strategy))

        assert result.success is True
        # 50 USDC + 5 USDC pending deposits = 55 USDC
        assert result.new_total_assets == 55_000_000


class TestVersionAwareAccounting:
    """Tests for version-specific accounting behavior."""

    def test_v050_uses_value_directly(self):
        """V0.5.0: Total assets is the valuate() output directly (no pending deposits added)."""
        manager = _make_manager(
            vault_config=_make_config(version="0.5.0"),
            vault_state=VaultState(
                initialized=True,
                last_total_assets=10_000_000,
                settlement_phase=SettlementPhase.IDLE,
                last_valuation_time=datetime.now(UTC) - timedelta(hours=2),
            ),
        )

        strategy = _make_strategy()
        market = _make_market(underlying_price=Decimal("1.0"), total_portfolio_usd=Decimal("10"))
        strategy.create_market_snapshot.return_value = market
        strategy.valuate.return_value = Decimal("10")

        manager._execution_orchestrator.execute = AsyncMock(return_value=_make_execution_result(success=True))

        with patch("almanak.framework.vault.lifecycle.get_token_resolver") as mock_resolver:
            mock_resolver.return_value.get_decimals.return_value = 6
            result = asyncio.run(manager.run_settlement_cycle(strategy))

        assert result.new_total_assets == 10_000_000
        # Should NOT have called get_pending_deposits
        assert not manager._vault_sdk.get_pending_deposits.called

    def test_pre_v050_adds_pending_deposits(self):
        """Pre-V0.5.0: Pending deposits are added to total assets."""
        manager = _make_manager(
            vault_config=_make_config(version="0.3.0"),
            vault_state=VaultState(
                initialized=True,
                last_total_assets=20_000_000,
                settlement_phase=SettlementPhase.IDLE,
                last_valuation_time=datetime.now(UTC) - timedelta(hours=2),
            ),
        )

        strategy = _make_strategy()
        market = _make_market(underlying_price=Decimal("1.0"), total_portfolio_usd=Decimal("15"))
        strategy.create_market_snapshot.return_value = market
        strategy.valuate.return_value = Decimal("15")

        # 5 USDC in pending deposits
        manager._vault_sdk.get_pending_deposits.return_value = 5_000_000

        manager._execution_orchestrator.execute = AsyncMock(return_value=_make_execution_result(success=True))

        with patch("almanak.framework.vault.lifecycle.get_token_resolver") as mock_resolver:
            mock_resolver.return_value.get_decimals.return_value = 6
            result = asyncio.run(manager.run_settlement_cycle(strategy))

        # 15 USDC + 5 USDC pending = 20 USDC
        assert result.new_total_assets == 20_000_000

    def test_usd_to_token_conversion_with_different_decimals(self):
        """Correctly converts USD to underlying with 18-decimal token (e.g., DAI)."""
        manager = _make_manager(
            vault_config=_make_config(underlying_token="DAI"),
            vault_state=VaultState(
                initialized=True,
                last_total_assets=10 * 10**18,
                settlement_phase=SettlementPhase.IDLE,
                last_valuation_time=datetime.now(UTC) - timedelta(hours=2),
            ),
        )

        strategy = _make_strategy()
        market = _make_market(underlying_price=Decimal("1.0"), total_portfolio_usd=Decimal("10"))
        strategy.create_market_snapshot.return_value = market
        strategy.valuate.return_value = Decimal("10")

        manager._execution_orchestrator.execute = AsyncMock(return_value=_make_execution_result(success=True))

        with patch("almanak.framework.vault.lifecycle.get_token_resolver") as mock_resolver:
            mock_resolver.return_value.get_decimals.return_value = 18
            result = asyncio.run(manager.run_settlement_cycle(strategy))

        assert result.new_total_assets == 10 * 10**18

    def test_usd_to_token_conversion_with_non_dollar_underlying(self):
        """Correctly converts when underlying token is not $1 (e.g., WETH)."""
        manager = _make_manager(
            vault_config=_make_config(underlying_token="WETH"),
            vault_state=VaultState(
                initialized=True,
                last_total_assets=5 * 10**18,  # 5 WETH
                settlement_phase=SettlementPhase.IDLE,
                last_valuation_time=datetime.now(UTC) - timedelta(hours=2),
            ),
        )

        strategy = _make_strategy()
        # Portfolio worth $10,000 USD, WETH at $2,000 -> 5 WETH
        market = _make_market(underlying_price=Decimal("2000"), total_portfolio_usd=Decimal("10000"))
        strategy.create_market_snapshot.return_value = market
        strategy.valuate.return_value = Decimal("10000")

        manager._execution_orchestrator.execute = AsyncMock(return_value=_make_execution_result(success=True))

        with patch("almanak.framework.vault.lifecycle.get_token_resolver") as mock_resolver:
            mock_resolver.return_value.get_decimals.return_value = 18
            result = asyncio.run(manager.run_settlement_cycle(strategy))

        assert result.new_total_assets == 5 * 10**18


class TestProposeTxFailure:
    """Tests for propose transaction failure."""

    def test_propose_failure_resets_to_idle(self):
        """Failed propose tx resets settlement_phase to IDLE."""
        manager = _make_manager(
            vault_state=VaultState(
                initialized=True,
                last_total_assets=10_000_000,
                settlement_phase=SettlementPhase.IDLE,
                last_valuation_time=datetime.now(UTC) - timedelta(hours=2),
            ),
        )

        strategy = _make_strategy()
        market = _make_market()
        strategy.create_market_snapshot.return_value = market
        strategy.valuate.return_value = Decimal("10")

        # Propose fails
        manager._execution_orchestrator.execute = AsyncMock(
            return_value=_make_execution_result(success=False, error="tx reverted")
        )

        with patch("almanak.framework.vault.lifecycle.get_token_resolver") as mock_resolver:
            mock_resolver.return_value.get_decimals.return_value = 6
            result = asyncio.run(manager.run_settlement_cycle(strategy))

        assert result.success is False
        assert manager.get_vault_state().settlement_phase == SettlementPhase.IDLE

    def test_propose_failure_does_not_call_settle(self):
        """When propose fails, settle methods are never called."""
        manager = _make_manager(
            vault_state=VaultState(
                initialized=True,
                last_total_assets=10_000_000,
                settlement_phase=SettlementPhase.IDLE,
                last_valuation_time=datetime.now(UTC) - timedelta(hours=2),
            ),
        )

        strategy = _make_strategy()
        market = _make_market()
        strategy.create_market_snapshot.return_value = market
        strategy.valuate.return_value = Decimal("10")

        manager._execution_orchestrator.execute = AsyncMock(
            return_value=_make_execution_result(success=False, error="tx reverted")
        )

        with patch("almanak.framework.vault.lifecycle.get_token_resolver") as mock_resolver:
            mock_resolver.return_value.get_decimals.return_value = 6
            asyncio.run(manager.run_settlement_cycle(strategy))

        # Settle bundles should NOT have been built
        assert not manager._vault_adapter.build_settle_deposit_bundle.called
        assert not manager._vault_adapter.build_settle_redeem_bundle.called


class TestSettleDepositFailure:
    """Tests for settle deposit transaction failure."""

    def test_settle_failure_reverts_to_proposed(self):
        """Failed settle deposit tx reverts settlement_phase to PROPOSED."""
        manager = _make_manager(
            vault_state=VaultState(
                initialized=True,
                last_total_assets=10_000_000,
                settlement_phase=SettlementPhase.IDLE,
                last_valuation_time=datetime.now(UTC) - timedelta(hours=2),
            ),
        )

        strategy = _make_strategy()
        market = _make_market()
        strategy.create_market_snapshot.return_value = market
        strategy.valuate.return_value = Decimal("10")

        # Propose succeeds, settle fails
        propose_ok = _make_execution_result(success=True)
        settle_fail = _make_execution_result(success=False, error="settle reverted")
        manager._execution_orchestrator.execute = AsyncMock(side_effect=[propose_ok, settle_fail])

        with patch("almanak.framework.vault.lifecycle.get_token_resolver") as mock_resolver:
            mock_resolver.return_value.get_decimals.return_value = 6
            result = asyncio.run(manager.run_settlement_cycle(strategy))

        assert result.success is False
        assert manager.get_vault_state().settlement_phase == SettlementPhase.PROPOSED


class TestPhaseTransitions:
    """Tests for settlement phase state machine transitions."""

    def test_phases_follow_correct_order(self):
        """Phase transitions: IDLE -> PROPOSING -> PROPOSED -> SETTLING -> SETTLED -> IDLE."""
        phases_seen = []

        manager = _make_manager(
            vault_state=VaultState(
                initialized=True,
                last_total_assets=10_000_000,
                settlement_phase=SettlementPhase.IDLE,
                last_valuation_time=datetime.now(UTC) - timedelta(hours=2),
            ),
        )

        original_save = manager.save_vault_state

        def tracking_save():
            phases_seen.append(manager.get_vault_state().settlement_phase)
            original_save()

        manager.save_vault_state = tracking_save

        strategy = _make_strategy()
        market = _make_market()
        strategy.create_market_snapshot.return_value = market
        strategy.valuate.return_value = Decimal("10")

        manager._execution_orchestrator.execute = AsyncMock(return_value=_make_execution_result(success=True))

        with patch("almanak.framework.vault.lifecycle.get_token_resolver") as mock_resolver:
            mock_resolver.return_value.get_decimals.return_value = 6
            asyncio.run(manager.run_settlement_cycle(strategy))

        # Expected phase transitions saved:
        # PROPOSING (before propose), PROPOSED (after propose), SETTLING (before settle),
        # SETTLED (after settle), IDLE (final)
        assert SettlementPhase.PROPOSING in phases_seen
        assert SettlementPhase.PROPOSED in phases_seen
        assert SettlementPhase.SETTLING in phases_seen
        assert SettlementPhase.SETTLED in phases_seen
        assert phases_seen[-1] == SettlementPhase.IDLE


class TestSignerMismatchGuard:
    """Tests for the MVP single-signer guard."""

    def test_signer_mismatch_returns_failure(self):
        """Settlement fails when valuator_address != wallet_address."""
        manager = _make_manager(
            vault_config=_make_config(valuator_address="0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
            vault_state=VaultState(
                initialized=True,
                settlement_phase=SettlementPhase.IDLE,
            ),
        )

        strategy = _make_strategy(wallet_address="0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB")

        # Override curator to match wallet so preflight passes (signer guard is checked after)
        manager._vault_sdk.get_curator.return_value = "0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"

        result = asyncio.run(manager.run_settlement_cycle(strategy))

        assert result.success is False
        # No execution should have been attempted
        assert not manager._execution_orchestrator.execute.called

    def test_signer_match_proceeds(self):
        """Settlement proceeds when valuator_address == wallet_address."""
        same_addr = "0x3333333333333333333333333333333333333333"
        manager = _make_manager(
            vault_config=_make_config(valuator_address=same_addr),
            vault_state=VaultState(
                initialized=True,
                last_total_assets=10_000_000,
                settlement_phase=SettlementPhase.IDLE,
                last_valuation_time=datetime.now(UTC) - timedelta(hours=2),
            ),
        )

        strategy = _make_strategy(wallet_address=same_addr)
        market = _make_market()
        strategy.create_market_snapshot.return_value = market
        strategy.valuate.return_value = Decimal("10")

        manager._execution_orchestrator.execute = AsyncMock(return_value=_make_execution_result(success=True))

        with patch("almanak.framework.vault.lifecycle.get_token_resolver") as mock_resolver:
            mock_resolver.return_value.get_decimals.return_value = 6
            result = asyncio.run(manager.run_settlement_cycle(strategy))

        assert result.success is True

    def test_signer_match_case_insensitive(self):
        """Signer comparison is case-insensitive."""
        manager = _make_manager(
            vault_config=_make_config(valuator_address="0xABCDEF1234567890ABCDEF1234567890ABCDEF12"),
            vault_state=VaultState(initialized=True, settlement_phase=SettlementPhase.SETTLED),
        )

        # Override curator to match wallet so preflight passes
        manager._vault_sdk.get_curator.return_value = "0xabcdef1234567890abcdef1234567890abcdef12"

        strategy = _make_strategy(wallet_address="0xabcdef1234567890abcdef1234567890abcdef12")

        result = asyncio.run(manager.run_settlement_cycle(strategy))

        # Should proceed (case-insensitive match), not fail with signer mismatch
        assert result.success is True


class TestRedeemFailureFatal:
    """Tests for configurable redeem failure fatality (C2)."""

    def test_redeem_failure_fatal_returns_failure(self):
        """When redeem_failure_fatal=True (default), redeem failure aborts settlement."""
        manager = _make_manager(
            vault_config=_make_config(auto_settle_redeems=True, redeem_failure_fatal=True),
            vault_state=VaultState(
                initialized=True,
                last_total_assets=10_000_000,
                settlement_phase=SettlementPhase.IDLE,
                last_valuation_time=datetime.now(UTC) - timedelta(hours=2),
            ),
        )

        strategy = _make_strategy()
        market = _make_market()
        strategy.create_market_snapshot.return_value = market
        strategy.valuate.return_value = Decimal("10")

        # Propose succeeds, settle deposit succeeds, settle redeem fails
        propose_ok = _make_execution_result(success=True)
        settle_ok = _make_execution_result(success=True)
        redeem_fail = _make_execution_result(success=False, error="redeem reverted")
        manager._execution_orchestrator.execute = AsyncMock(side_effect=[propose_ok, settle_ok, redeem_fail])

        with patch("almanak.framework.vault.lifecycle.get_token_resolver") as mock_resolver:
            mock_resolver.return_value.get_decimals.return_value = 6
            result = asyncio.run(manager.run_settlement_cycle(strategy))

        assert result.success is False
        assert manager.get_vault_state().settlement_phase == SettlementPhase.PROPOSED

    def test_redeem_failure_non_fatal_continues(self):
        """When redeem_failure_fatal=False, redeem failure is non-fatal."""
        manager = _make_manager(
            vault_config=_make_config(auto_settle_redeems=True, redeem_failure_fatal=False),
            vault_state=VaultState(
                initialized=True,
                last_total_assets=10_000_000,
                settlement_phase=SettlementPhase.IDLE,
                last_valuation_time=datetime.now(UTC) - timedelta(hours=2),
            ),
        )

        strategy = _make_strategy()
        market = _make_market()
        strategy.create_market_snapshot.return_value = market
        strategy.valuate.return_value = Decimal("10")

        # Propose succeeds, settle deposit succeeds, settle redeem fails
        propose_ok = _make_execution_result(success=True)
        settle_ok = _make_execution_result(success=True)
        redeem_fail = _make_execution_result(success=False, error="redeem reverted")
        manager._execution_orchestrator.execute = AsyncMock(side_effect=[propose_ok, settle_ok, redeem_fail])

        with patch("almanak.framework.vault.lifecycle.get_token_resolver") as mock_resolver:
            mock_resolver.return_value.get_decimals.return_value = 6
            result = asyncio.run(manager.run_settlement_cycle(strategy))

        # Non-fatal: settlement still succeeds
        assert result.success is True
        assert manager.get_vault_state().settlement_phase == SettlementPhase.IDLE


class TestPreflightChecks:
    """Tests for on-chain preflight checks (C3)."""

    def test_preflight_version_mismatch_fails(self):
        """Settlement fails when vault version doesn't match."""
        manager = _make_manager(
            vault_state=VaultState(
                initialized=True,
                last_total_assets=10_000_000,
                settlement_phase=SettlementPhase.IDLE,
                last_valuation_time=datetime.now(UTC) - timedelta(hours=2),
            ),
        )

        strategy = _make_strategy()

        # Version check fails
        manager._vault_sdk.verify_version.side_effect = ValueError("version mismatch")

        result = asyncio.run(manager.run_settlement_cycle(strategy))

        assert result.success is False
        # No execution should have been attempted
        assert not manager._execution_orchestrator.execute.called

    def test_preflight_valuator_mismatch_fails(self):
        """Settlement fails when on-chain valuator doesn't match config."""
        manager = _make_manager(
            vault_state=VaultState(
                initialized=True,
                last_total_assets=10_000_000,
                settlement_phase=SettlementPhase.IDLE,
                last_valuation_time=datetime.now(UTC) - timedelta(hours=2),
            ),
        )

        strategy = _make_strategy()

        # Version check passes, but valuator mismatch
        manager._vault_sdk.verify_version.return_value = None
        manager._vault_sdk.get_valuation_manager.return_value = "0xDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEF"

        result = asyncio.run(manager.run_settlement_cycle(strategy))

        assert result.success is False
        assert not manager._execution_orchestrator.execute.called

    def test_preflight_curator_mismatch_fails(self):
        """Settlement fails when on-chain curator doesn't match wallet."""
        manager = _make_manager(
            vault_state=VaultState(
                initialized=True,
                last_total_assets=10_000_000,
                settlement_phase=SettlementPhase.IDLE,
                last_valuation_time=datetime.now(UTC) - timedelta(hours=2),
            ),
        )

        strategy = _make_strategy()

        # Version and valuator pass, curator mismatches
        manager._vault_sdk.verify_version.return_value = None
        manager._vault_sdk.get_valuation_manager.return_value = "0x3333333333333333333333333333333333333333"
        manager._vault_sdk.get_curator.return_value = "0xDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEF"

        result = asyncio.run(manager.run_settlement_cycle(strategy))

        assert result.success is False
        assert not manager._execution_orchestrator.execute.called

    def test_preflight_passes_proceeds_to_settlement(self):
        """When all preflight checks pass, settlement proceeds normally."""
        manager = _make_manager(
            vault_state=VaultState(
                initialized=True,
                last_total_assets=10_000_000,
                settlement_phase=SettlementPhase.IDLE,
                last_valuation_time=datetime.now(UTC) - timedelta(hours=2),
            ),
        )

        strategy = _make_strategy()
        market = _make_market()
        strategy.create_market_snapshot.return_value = market
        strategy.valuate.return_value = Decimal("10")

        # Preflight checks pass
        manager._vault_sdk.verify_version.return_value = None
        manager._vault_sdk.get_valuation_manager.return_value = "0x3333333333333333333333333333333333333333"
        manager._vault_sdk.get_curator.return_value = "0x3333333333333333333333333333333333333333"

        manager._execution_orchestrator.execute = AsyncMock(return_value=_make_execution_result(success=True))

        with patch("almanak.framework.vault.lifecycle.get_token_resolver") as mock_resolver:
            mock_resolver.return_value.get_decimals.return_value = 6
            result = asyncio.run(manager.run_settlement_cycle(strategy))

        assert result.success is True

    def test_preflight_only_runs_once_then_periodically(self):
        """Preflight runs once, then skips until interval is reached."""
        manager = _make_manager(
            vault_state=VaultState(
                initialized=True,
                last_total_assets=10_000_000,
                settlement_phase=SettlementPhase.IDLE,
                last_valuation_time=datetime.now(UTC) - timedelta(hours=2),
            ),
        )

        strategy = _make_strategy()
        market = _make_market()
        strategy.create_market_snapshot.return_value = market
        strategy.valuate.return_value = Decimal("10")

        # Preflight checks pass
        manager._vault_sdk.verify_version.return_value = None
        manager._vault_sdk.get_valuation_manager.return_value = "0x3333333333333333333333333333333333333333"
        manager._vault_sdk.get_curator.return_value = "0x3333333333333333333333333333333333333333"

        manager._execution_orchestrator.execute = AsyncMock(return_value=_make_execution_result(success=True))

        with patch("almanak.framework.vault.lifecycle.get_token_resolver") as mock_resolver:
            mock_resolver.return_value.get_decimals.return_value = 6

            # First settlement: preflight runs
            asyncio.run(manager.run_settlement_cycle(strategy))
            first_verify_count = manager._vault_sdk.verify_version.call_count

            # Reset state for second run
            manager._vault_state.settlement_phase = SettlementPhase.IDLE
            manager._vault_state.last_valuation_time = datetime.now(UTC) - timedelta(hours=2)

            # Second settlement: preflight should be skipped
            asyncio.run(manager.run_settlement_cycle(strategy))
            second_verify_count = manager._vault_sdk.verify_version.call_count

        # verify_version should only have been called once (during first settlement)
        assert first_verify_count == 1
        assert second_verify_count == 1


class TestMultiChainGuard:
    """Tests for multi-chain strategy guard (C4)."""

    def test_multi_chain_strategy_rejected(self):
        """Multi-chain strategies are rejected during valuation."""
        manager = _make_manager(
            vault_state=VaultState(
                initialized=True,
                last_total_assets=10_000_000,
                settlement_phase=SettlementPhase.IDLE,
                last_valuation_time=datetime.now(UTC) - timedelta(hours=2),
            ),
        )

        strategy = _make_strategy()
        strategy.chains = ["ethereum", "arbitrum"]  # Multi-chain

        # Preflight checks pass
        manager._vault_sdk.verify_version.return_value = None
        manager._vault_sdk.get_valuation_manager.return_value = "0x3333333333333333333333333333333333333333"
        manager._vault_sdk.get_curator.return_value = "0x3333333333333333333333333333333333333333"

        result = asyncio.run(manager.run_settlement_cycle(strategy))

        assert result.success is False
        assert not manager._execution_orchestrator.execute.called

    def test_single_chain_strategy_proceeds(self):
        """Single-chain strategies are not rejected."""
        manager = _make_manager(
            vault_state=VaultState(
                initialized=True,
                last_total_assets=10_000_000,
                settlement_phase=SettlementPhase.IDLE,
                last_valuation_time=datetime.now(UTC) - timedelta(hours=2),
            ),
        )

        strategy = _make_strategy()
        strategy.chains = ["ethereum"]  # Single-chain
        market = _make_market()
        strategy.create_market_snapshot.return_value = market
        strategy.valuate.return_value = Decimal("10")

        # Preflight checks pass
        manager._vault_sdk.verify_version.return_value = None
        manager._vault_sdk.get_valuation_manager.return_value = "0x3333333333333333333333333333333333333333"
        manager._vault_sdk.get_curator.return_value = "0x3333333333333333333333333333333333333333"

        manager._execution_orchestrator.execute = AsyncMock(return_value=_make_execution_result(success=True))

        with patch("almanak.framework.vault.lifecycle.get_token_resolver") as mock_resolver:
            mock_resolver.return_value.get_decimals.return_value = 6
            result = asyncio.run(manager.run_settlement_cycle(strategy))

        assert result.success is True
