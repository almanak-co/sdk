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
    deployment_id: str = "test-strategy-1",
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

    # Redeem-gate / spent-proposal defaults (Lagoon v0.5.0). Default: proposals are
    # live and NO redeem shares are waiting in the silo (deposits-only path), so the
    # redeem leg is skipped unless a test opts in with a non-zero silo balance.
    sdk.has_live_proposal.return_value = True
    sdk.get_silo_address.return_value = "0x2222222222222222222222222222222222222222"
    sdk.get_underlying_balance.return_value = 0

    # Share-backed AUM guard defaults (VIB-5672). Default the on-chain reads to a
    # share-backed base far above any test valuation, so the invariant is a no-op for
    # state-machine tests. Dedicated guard tests (TestShareBackedAumGuard) set small
    # bases to exercise the refuse / continue paths.
    sdk.get_total_assets.return_value = 10**30
    sdk.get_pending_deposits.return_value = 0

    manager = VaultLifecycleManager(
        vault_config=config,
        vault_sdk=sdk,
        vault_adapter=adapter,
        execution_orchestrator=orchestrator,
        deployment_id=deployment_id,
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
        """When auto_settle_redeems is True AND redeem shares remain, settle_redeem is built."""
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

        # Redeem shares are waiting in the silo -> redeem leg runs.
        manager._vault_sdk.get_underlying_balance.return_value = 5_000_000

        manager._execution_orchestrator.execute = AsyncMock(return_value=_make_execution_result(success=True))

        with patch("almanak.framework.vault.lifecycle.get_token_resolver") as mock_resolver:
            mock_resolver.return_value.get_decimals.return_value = 6
            asyncio.run(manager.run_settlement_cycle(strategy))

        assert manager._vault_adapter.build_settle_redeem_bundle.called
        # settleRedeem must be fed a FRESH proposal (Lagoon v0.5.0 single-use): the redeem
        # leg re-proposes, so build_propose_valuation_bundle is called twice, never reusing
        # the deposit proposal.
        assert manager._vault_adapter.build_propose_valuation_bundle.call_count == 2

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
        # V0.5.0 NAV excludes pending deposits: the proposed value equals valuate()
        # directly (10 USDC), NOT valuate() + pending. (The share-backed AUM guard
        # (VIB-5672) does read pending deposits for its invariant base, so the old
        # "get_pending_deposits never called" assertion no longer holds -- assert the
        # accounting *value* excludes pending instead of the read count.)

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
    """Tests for configurable redeem failure fatality (C2).

    v0.5.0: deposits commit before the redeem leg runs (settleDeposit spends the first
    proposal), so a redeem failure can no longer roll back to PROPOSED / undo deposits.
    """

    def test_redeem_failure_fatal_returns_failure(self):
        """When redeem_failure_fatal=True (default), a redeem failure surfaces as failure."""
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

        # Redeem shares remain -> redeem leg runs.
        manager._vault_sdk.get_underlying_balance.return_value = 5_000_000

        # propose #1 ok, settleDeposit ok, propose #2 ok, settleRedeem fails.
        propose_ok = _make_execution_result(success=True)
        settle_ok = _make_execution_result(success=True)
        propose2_ok = _make_execution_result(success=True)
        redeem_fail = _make_execution_result(success=False, error="redeem reverted")
        manager._execution_orchestrator.execute = AsyncMock(
            side_effect=[propose_ok, settle_ok, propose2_ok, redeem_fail]
        )

        with patch("almanak.framework.vault.lifecycle.get_token_resolver") as mock_resolver:
            mock_resolver.return_value.get_decimals.return_value = 6
            result = asyncio.run(manager.run_settlement_cycle(strategy))

        assert result.success is False
        # Parked at a redeem-leg-retry phase, NOT rolled back to PROPOSED (deposits stand).
        assert manager.get_vault_state().settlement_phase == SettlementPhase.PROPOSED_REDEEM

    def test_redeem_failure_does_not_roll_back_deposits(self):
        """A fatal redeem failure never reverts to PROPOSED (which would retry settleDeposit)."""
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

        manager._vault_sdk.get_underlying_balance.return_value = 5_000_000

        # propose #1 ok, settleDeposit ok, propose #2 FAILS.
        propose_ok = _make_execution_result(success=True)
        settle_ok = _make_execution_result(success=True)
        propose2_fail = _make_execution_result(success=False, error="propose2 reverted")
        manager._execution_orchestrator.execute = AsyncMock(side_effect=[propose_ok, settle_ok, propose2_fail])

        with patch("almanak.framework.vault.lifecycle.get_token_resolver") as mock_resolver:
            mock_resolver.return_value.get_decimals.return_value = 6
            result = asyncio.run(manager.run_settlement_cycle(strategy))

        assert result.success is False
        phase = manager.get_vault_state().settlement_phase
        assert phase != SettlementPhase.PROPOSED
        assert phase == SettlementPhase.PROPOSING_REDEEM

    def test_redeem_failure_non_fatal_continues(self):
        """When redeem_failure_fatal=False, redeem failure finalizes deposits and carries over."""
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

        manager._vault_sdk.get_underlying_balance.return_value = 5_000_000

        # propose #1 ok, settleDeposit ok, propose #2 ok, settleRedeem fails.
        propose_ok = _make_execution_result(success=True)
        settle_ok = _make_execution_result(success=True)
        propose2_ok = _make_execution_result(success=True)
        redeem_fail = _make_execution_result(success=False, error="redeem reverted")
        manager._execution_orchestrator.execute = AsyncMock(
            side_effect=[propose_ok, settle_ok, propose2_ok, redeem_fail]
        )

        with patch("almanak.framework.vault.lifecycle.get_token_resolver") as mock_resolver:
            mock_resolver.return_value.get_decimals.return_value = 6
            result = asyncio.run(manager.run_settlement_cycle(strategy))

        # Non-fatal: deposits stand, cycle finalizes; redeem shares carry to next cycle.
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


class TestRedeemLegGating:
    """Redeem-leg gating on the silo balance (VIB-5645 deadlock fix)."""

    def _run_first_settlement(self, silo_balance: int) -> VaultLifecycleManager:
        manager = _make_manager(
            vault_config=_make_config(version="0.5.0", auto_settle_redeems=True),
            vault_state=VaultState(initialized=False, settlement_phase=SettlementPhase.IDLE),
        )
        strategy = _make_strategy()
        market = _make_market()
        strategy.create_market_snapshot.return_value = market
        strategy.valuate.return_value = Decimal("10")
        manager._vault_sdk.get_underlying_balance.return_value = silo_balance
        manager._execution_orchestrator.execute = AsyncMock(return_value=_make_execution_result(success=True))
        with patch("almanak.framework.vault.lifecycle.get_token_resolver") as mock_resolver:
            mock_resolver.return_value.get_decimals.return_value = 6
            result = asyncio.run(manager.run_settlement_cycle(strategy))
        assert result.success is True
        return manager

    def test_first_settlement_issues_no_settle_redeem(self):
        """First settlement (silo empty) must NOT call settleRedeem -- the exact deadlock case."""
        manager = self._run_first_settlement(silo_balance=0)
        assert not manager._vault_adapter.build_settle_redeem_bundle.called
        # Only the deposit proposal is issued (no fresh redeem proposal).
        assert manager._vault_adapter.build_propose_valuation_bundle.call_count == 1
        assert manager.get_vault_state().settlement_phase == SettlementPhase.IDLE
        assert manager.get_vault_state().initialized is True

    def test_deposits_only_issues_no_settle_redeem(self):
        """A deposits-only cycle (no redeem shares) skips settleRedeem entirely."""
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
        manager._vault_sdk.get_underlying_balance.return_value = 0  # no redeem shares
        manager._execution_orchestrator.execute = AsyncMock(return_value=_make_execution_result(success=True))
        with patch("almanak.framework.vault.lifecycle.get_token_resolver") as mock_resolver:
            mock_resolver.return_value.get_decimals.return_value = 6
            result = asyncio.run(manager.run_settlement_cycle(strategy))
        assert result.success is True
        assert not manager._vault_adapter.build_settle_redeem_bundle.called

    def test_redeem_leg_issues_fresh_proposal_before_settle_redeem(self):
        """With redeem shares present, a FRESH proposal precedes settleRedeem (never reuse #1)."""
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
        manager._vault_sdk.get_underlying_balance.return_value = 5_000_000

        call_order: list[str] = []

        def _track(bundle, wallet_address=None):
            call_order.append(getattr(bundle, "_kind", "?"))
            return _make_execution_result(success=True)

        def _propose(params):
            b = MagicMock()
            b._kind = "propose"
            return b

        def _settle_deposit(params):
            b = MagicMock()
            b._kind = "settle_deposit"
            return b

        def _settle_redeem(params):
            b = MagicMock()
            b._kind = "settle_redeem"
            return b

        manager._vault_adapter.build_propose_valuation_bundle.side_effect = _propose
        manager._vault_adapter.build_settle_deposit_bundle.side_effect = _settle_deposit
        manager._vault_adapter.build_settle_redeem_bundle.side_effect = _settle_redeem
        manager._execution_orchestrator.execute = AsyncMock(side_effect=_track)

        with patch("almanak.framework.vault.lifecycle.get_token_resolver") as mock_resolver:
            mock_resolver.return_value.get_decimals.return_value = 6
            result = asyncio.run(manager.run_settlement_cycle(strategy))

        assert result.success is True
        # Exactly: propose #1 -> settleDeposit -> propose #2 -> settleRedeem.
        assert call_order == ["propose", "settle_deposit", "propose", "settle_redeem"]
        assert manager.get_vault_state().settlement_phase == SettlementPhase.IDLE


class TestShareBackedAumGuard:
    """Share-backed AUM invariant (VIB-5672, vault ship-gate #1).

    The vault Safe must hold ONLY share-backed AUM. The settlement-time guard refuses
    to propose a NAV that materially exceeds the share-backed base = on-chain
    ``totalAssets`` + pending deposits. Failure semantics are mode-aware: live REFUSES,
    paper / dry_run log ERROR and continue.
    """

    @staticmethod
    def _idle_state(last_total_assets: int) -> VaultState:
        """A vault that has settled at least once and is now IDLE, interval elapsed."""
        return VaultState(
            initialized=True,
            last_total_assets=last_total_assets,
            settlement_phase=SettlementPhase.IDLE,
            last_valuation_time=datetime.now(UTC) - timedelta(hours=2),
        )

    @staticmethod
    def _run(manager, strategy):
        with patch("almanak.framework.vault.lifecycle.get_token_resolver") as mock_resolver:
            mock_resolver.return_value.get_decimals.return_value = 6
            return asyncio.run(manager.run_settlement_cycle(strategy))

    def test_commingled_manager_seed_refused_in_live_mode(self):
        """The exact VIB-5667 E2E: 200,000 USDC manager seed + 2,000 USDC depositor.

        On-chain settled obligations are 2,000 USDC but valuate()-of-the-whole-Safe
        proposes 202,000. The guard must FIRE (refuse the propose) in live mode.
        """
        # Generous bounds isolate the share-backed guard from the change-bps bounds.
        manager = _make_manager(
            vault_config=_make_config(version="0.5.0", max_valuation_change_up_bps=100_000_000),
            vault_state=self._idle_state(last_total_assets=2_000_000_000),  # 2,000 USDC
        )
        # Share-backed base: only 2,000 USDC of depositor capital, no pending deposits.
        manager._vault_sdk.get_total_assets.return_value = 2_000_000_000
        manager._vault_sdk.get_pending_deposits.return_value = 0

        strategy = _make_strategy()
        strategy.create_market_snapshot.return_value = _make_market(underlying_price=Decimal("1.0"))
        strategy.valuate.return_value = Decimal("202000")  # 200k seed + 2k depositor

        result = self._run(manager, strategy)

        assert result.success is False
        # Refused BEFORE any on-chain propose was submitted.
        manager._execution_orchestrator.execute.assert_not_called()
        # State machine stays safe / resumable at IDLE.
        assert manager.get_vault_state().settlement_phase == SettlementPhase.IDLE

    def test_legitimate_pnl_within_tolerance_passes(self):
        """2% inter-settlement PnL is legitimate growth and must NOT fire the guard."""
        manager = _make_manager(
            vault_config=_make_config(version="0.5.0"),
            vault_state=self._idle_state(last_total_assets=2_000_000_000),
        )
        manager._vault_sdk.get_total_assets.return_value = 2_000_000_000  # 2,000 USDC base
        manager._vault_sdk.get_pending_deposits.return_value = 0
        manager._execution_orchestrator.execute = AsyncMock(
            return_value=_make_execution_result(success=True)
        )

        strategy = _make_strategy()
        strategy.create_market_snapshot.return_value = _make_market(underlying_price=Decimal("1.0"))
        strategy.valuate.return_value = Decimal("2040")  # +2% PnL, within 5% default tolerance

        result = self._run(manager, strategy)

        assert result.success is True
        manager._execution_orchestrator.execute.assert_called()  # propose proceeded

    def test_pending_deposits_counted_in_base_no_false_fire(self):
        """A deposit in-flight lifts the base; a NAV covered by totalAssets + pending passes.

        Without counting pending deposits the proposed NAV would exceed the tolerance and
        fire; counting them keeps a legitimate deposit-in-flight from false-firing.
        """
        manager = _make_manager(
            vault_config=_make_config(version="0.5.0", max_valuation_change_up_bps=100_000_000),
            vault_state=self._idle_state(last_total_assets=2_000_000_000),
        )
        manager._vault_sdk.get_total_assets.return_value = 2_000_000_000  # 2,000 settled
        manager._vault_sdk.get_pending_deposits.return_value = 1_000_000_000  # 1,000 in-flight
        manager._execution_orchestrator.execute = AsyncMock(
            return_value=_make_execution_result(success=True)
        )

        strategy = _make_strategy()
        strategy.create_market_snapshot.return_value = _make_market(underlying_price=Decimal("1.0"))
        # 2,900 USDC: exceeds 2,000 * 1.05 (would fire), but <= (2,000 + 1,000) * 1.05.
        strategy.valuate.return_value = Decimal("2900")

        result = self._run(manager, strategy)

        assert result.success is True
        manager._vault_sdk.get_pending_deposits.assert_called()
        manager._execution_orchestrator.execute.assert_called()

    def test_commingled_seed_paper_mode_logs_and_continues(self, caplog):
        """Paper mode surfaces the violation loudly but does NOT halt the settlement."""
        manager = _make_manager(
            vault_config=_make_config(version="0.5.0", max_valuation_change_up_bps=100_000_000),
            vault_state=self._idle_state(last_total_assets=2_000_000_000),
        )
        manager._execution_mode = "paper"
        manager._vault_sdk.get_total_assets.return_value = 2_000_000_000
        manager._vault_sdk.get_pending_deposits.return_value = 0
        manager._execution_orchestrator.execute = AsyncMock(
            return_value=_make_execution_result(success=True)
        )

        strategy = _make_strategy()
        strategy.create_market_snapshot.return_value = _make_market(underlying_price=Decimal("1.0"))
        strategy.valuate.return_value = Decimal("202000")

        with caplog.at_level("ERROR"):
            result = self._run(manager, strategy)

        # Continued despite the violation (no real funds move in paper mode).
        assert result.success is True
        manager._execution_orchestrator.execute.assert_called()
        assert any("Share-backed AUM invariant violated" in r.message for r in caplog.records)

    def test_tolerance_bps_config_plumbing(self):
        """A higher configured tolerance admits a proposal a tighter tolerance rejects."""
        base = 2_000_000_000
        # 15% over base: fires at the 5% default, passes at a 2000 bps (20%) tolerance.
        proposed_usd = Decimal("2300")

        tight = _make_manager(
            vault_config=_make_config(version="0.5.0", max_valuation_change_up_bps=100_000_000),
            vault_state=self._idle_state(last_total_assets=base),
        )
        tight._vault_sdk.get_total_assets.return_value = base
        tight._vault_sdk.get_pending_deposits.return_value = 0
        tight._execution_orchestrator.execute = AsyncMock(return_value=_make_execution_result(success=True))
        strategy_t = _make_strategy()
        strategy_t.create_market_snapshot.return_value = _make_market(underlying_price=Decimal("1.0"))
        strategy_t.valuate.return_value = proposed_usd
        assert self._run(tight, strategy_t).success is False  # default 500 bps -> refuse

        loose = _make_manager(
            vault_config=_make_config(
                version="0.5.0",
                max_valuation_change_up_bps=100_000_000,
                nav_share_backed_tolerance_bps=2000,
            ),
            vault_state=self._idle_state(last_total_assets=base),
        )
        loose._vault_sdk.get_total_assets.return_value = base
        loose._vault_sdk.get_pending_deposits.return_value = 0
        loose._execution_orchestrator.execute = AsyncMock(return_value=_make_execution_result(success=True))
        strategy_l = _make_strategy()
        strategy_l.create_market_snapshot.return_value = _make_market(underlying_price=Decimal("1.0"))
        strategy_l.valuate.return_value = proposed_usd
        assert self._run(loose, strategy_l).success is True  # 2000 bps -> allowed

    def test_abs_floor_config_plumbing_covers_dust_on_zero_base(self):
        """abs_floor cushions a tiny proposal when the share-backed base is zero."""
        # No settled AUM and no pending deposits -> base 0. A tiny proposal would fire on
        # relative tolerance alone (0 * anything = 0); abs_floor lets dust through.
        manager = _make_manager(
            vault_config=_make_config(
                version="0.5.0",
                max_valuation_change_up_bps=100_000_000,
                nav_share_backed_abs_floor=1_000_000,  # 1 USDC dust floor
            ),
            # last_total_assets=0 skips the change-bps bounds check, isolating the
            # share-backed guard's abs_floor behaviour on a zero base.
            vault_state=self._idle_state(last_total_assets=0),
        )
        manager._vault_sdk.get_total_assets.return_value = 0
        manager._vault_sdk.get_pending_deposits.return_value = 0
        manager._execution_orchestrator.execute = AsyncMock(return_value=_make_execution_result(success=True))

        strategy = _make_strategy()
        strategy.create_market_snapshot.return_value = _make_market(underlying_price=Decimal("1.0"))
        strategy.valuate.return_value = Decimal("0.5")  # 500,000 raw <= 1,000,000 floor

        assert self._run(manager, strategy).success is True

    def test_empty_not_zero_unreadable_total_assets_refuses_in_live(self):
        """Empty != Zero: an on-chain read that RAISES is an error, never a silent 0."""
        manager = _make_manager(
            vault_config=_make_config(version="0.5.0", max_valuation_change_up_bps=100_000_000),
            vault_state=self._idle_state(last_total_assets=2_000_000_000),
        )
        manager._vault_sdk.get_total_assets.side_effect = RuntimeError("RPC down")

        strategy = _make_strategy()
        strategy.create_market_snapshot.return_value = _make_market(underlying_price=Decimal("1.0"))
        strategy.valuate.return_value = Decimal("2040")  # would pass if base read as 0/ok

        result = self._run(manager, strategy)

        assert result.success is False
        manager._execution_orchestrator.execute.assert_not_called()
        assert manager.get_vault_state().settlement_phase == SettlementPhase.IDLE

    def test_empty_not_zero_none_pending_deposits_refuses_in_live(self):
        """Empty != Zero: a None from a degraded SDK is unmeasured, not a measured 0."""
        manager = _make_manager(
            vault_config=_make_config(version="0.5.0", max_valuation_change_up_bps=100_000_000),
            vault_state=self._idle_state(last_total_assets=2_000_000_000),
        )
        manager._vault_sdk.get_total_assets.return_value = 2_000_000_000
        manager._vault_sdk.get_pending_deposits.return_value = None  # unmeasured

        strategy = _make_strategy()
        strategy.create_market_snapshot.return_value = _make_market(underlying_price=Decimal("1.0"))
        strategy.valuate.return_value = Decimal("2040")

        result = self._run(manager, strategy)

        assert result.success is False
        manager._execution_orchestrator.execute.assert_not_called()
