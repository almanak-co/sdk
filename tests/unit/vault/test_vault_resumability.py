"""Tests for VaultLifecycleManager settlement resumability (crash recovery)."""

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

from almanak.framework.vault.config import SettlementPhase, VaultConfig, VaultState
from almanak.framework.vault.lifecycle import VaultLifecycleManager


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
    initial_vault_state: dict | None = None,
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
    # live and NO redeem shares are waiting in the silo (deposits-only path).
    sdk.has_live_proposal.return_value = True
    sdk.get_silo_address.return_value = "0x2222222222222222222222222222222222222222"
    sdk.get_underlying_balance.return_value = 0

    # Share-backed AUM guard defaults (VIB-5672). Base far above any test valuation so
    # the invariant is a no-op for the resumability state-machine tests. SETTLING-resume
    # tests override get_total_assets explicitly for their own on-chain checks.
    sdk.get_total_assets.return_value = 10**30
    sdk.get_pending_deposits.return_value = 0

    manager = VaultLifecycleManager(
        vault_config=config,
        vault_sdk=sdk,
        vault_adapter=adapter,
        execution_orchestrator=orchestrator,
        deployment_id=deployment_id,
        initial_vault_state=initial_vault_state,
    )

    if vault_state is not None:
        manager._vault_state = vault_state

    return manager


class TestResumeFromProposing:
    """Crash recovery: process crashed during PROPOSING phase."""

    def test_propose_already_confirmed_on_chain(self):
        """If on-chain proposed_total_assets matches, advance to PROPOSED and settle."""
        manager = _make_manager(
            vault_state=VaultState(
                initialized=True,
                last_total_assets=10_000_000,
                last_proposed_total_assets=10_500_000,
                settlement_phase=SettlementPhase.PROPOSING,
                last_valuation_time=datetime.now(UTC) - timedelta(hours=2),
                settlement_nonce=1,
            ),
        )

        strategy = _make_strategy()

        # On-chain proposed matches what we intended
        manager._vault_sdk.get_proposed_total_assets.return_value = 10_500_000

        # Settle succeeds
        manager._execution_orchestrator.execute = AsyncMock(return_value=_make_execution_result(success=True))

        result = asyncio.run(manager.run_settlement_cycle(strategy))

        assert result.success is True
        assert result.new_total_assets == 10_500_000
        assert manager.get_vault_state().settlement_phase == SettlementPhase.IDLE
        # Should NOT have called build_propose_valuation_bundle (skipped propose)
        assert not manager._vault_adapter.build_propose_valuation_bundle.called
        # Should have called settle
        assert manager._vault_adapter.build_settle_deposit_bundle.called

    def test_propose_not_confirmed_retries(self):
        """If on-chain proposed_total_assets doesn't match, retry propose."""
        manager = _make_manager(
            vault_state=VaultState(
                initialized=True,
                last_total_assets=10_000_000,
                last_proposed_total_assets=10_500_000,
                settlement_phase=SettlementPhase.PROPOSING,
                last_valuation_time=datetime.now(UTC) - timedelta(hours=2),
            ),
        )

        strategy = _make_strategy()

        # On-chain proposed does NOT match (propose didn't complete)
        manager._vault_sdk.get_proposed_total_assets.return_value = 0

        # Both propose and settle succeed
        manager._execution_orchestrator.execute = AsyncMock(return_value=_make_execution_result(success=True))

        result = asyncio.run(manager.run_settlement_cycle(strategy))

        assert result.success is True
        assert result.new_total_assets == 10_500_000
        # Should have called propose (retried)
        assert manager._vault_adapter.build_propose_valuation_bundle.called
        # Should have called settle
        assert manager._vault_adapter.build_settle_deposit_bundle.called

    def test_propose_retry_fails(self):
        """If retried propose fails, reset to IDLE."""
        manager = _make_manager(
            vault_state=VaultState(
                initialized=True,
                last_total_assets=10_000_000,
                last_proposed_total_assets=10_500_000,
                settlement_phase=SettlementPhase.PROPOSING,
                last_valuation_time=datetime.now(UTC) - timedelta(hours=2),
            ),
        )

        strategy = _make_strategy()

        # On-chain proposed doesn't match
        manager._vault_sdk.get_proposed_total_assets.return_value = 0

        # Propose fails
        manager._execution_orchestrator.execute = AsyncMock(
            return_value=_make_execution_result(success=False, error="tx reverted")
        )

        result = asyncio.run(manager.run_settlement_cycle(strategy))

        assert result.success is False
        assert manager.get_vault_state().settlement_phase == SettlementPhase.IDLE

    def test_proposing_zero_value_retries_instead_of_skipping(self):
        """When proposed_total_assets is 0 and on-chain is also 0, retry (zero-value guard)."""
        manager = _make_manager(
            vault_state=VaultState(
                initialized=True,
                last_total_assets=0,
                last_proposed_total_assets=0,
                settlement_phase=SettlementPhase.PROPOSING,
                last_valuation_time=datetime.now(UTC) - timedelta(hours=2),
            ),
        )

        strategy = _make_strategy()

        # Both are 0 -- ambiguous (0 == 0), so we should retry propose, NOT skip it
        manager._vault_sdk.get_proposed_total_assets.return_value = 0

        # Both propose and settle succeed
        manager._execution_orchestrator.execute = AsyncMock(return_value=_make_execution_result(success=True))

        result = asyncio.run(manager.run_settlement_cycle(strategy))

        assert result.success is True
        # Should have retried propose (not assumed confirmed)
        assert manager._vault_adapter.build_propose_valuation_bundle.called


class TestResumeFromProposed:
    """Crash recovery: process crashed after propose confirmed (PROPOSED phase)."""

    def test_skips_propose_goes_to_settle(self):
        """Resuming from PROPOSED skips propose and proceeds directly to settle."""
        manager = _make_manager(
            vault_state=VaultState(
                initialized=True,
                last_total_assets=10_000_000,
                last_proposed_total_assets=10_500_000,
                settlement_phase=SettlementPhase.PROPOSED,
                last_valuation_time=datetime.now(UTC) - timedelta(hours=2),
            ),
        )

        strategy = _make_strategy()

        # Settle succeeds
        manager._execution_orchestrator.execute = AsyncMock(return_value=_make_execution_result(success=True))

        result = asyncio.run(manager.run_settlement_cycle(strategy))

        assert result.success is True
        assert result.new_total_assets == 10_500_000
        assert manager.get_vault_state().settlement_phase == SettlementPhase.IDLE
        # Should NOT have called propose
        assert not manager._vault_adapter.build_propose_valuation_bundle.called
        # Should have called settle
        assert manager._vault_adapter.build_settle_deposit_bundle.called

    def test_settle_fails_stays_at_proposed(self):
        """If settle fails during resume from PROPOSED, stay at PROPOSED for next retry."""
        manager = _make_manager(
            vault_state=VaultState(
                initialized=True,
                last_total_assets=10_000_000,
                last_proposed_total_assets=10_500_000,
                settlement_phase=SettlementPhase.PROPOSED,
                last_valuation_time=datetime.now(UTC) - timedelta(hours=2),
            ),
        )

        strategy = _make_strategy()

        # Settle fails
        manager._execution_orchestrator.execute = AsyncMock(
            return_value=_make_execution_result(success=False, error="settle reverted")
        )

        result = asyncio.run(manager.run_settlement_cycle(strategy))

        assert result.success is False
        assert manager.get_vault_state().settlement_phase == SettlementPhase.PROPOSED


class TestResumeFromSettling:
    """Crash recovery: process crashed during SETTLING phase."""

    def test_settle_already_confirmed_on_chain(self):
        """If on-chain total_assets matches proposed, advance to SETTLED then IDLE."""
        manager = _make_manager(
            vault_state=VaultState(
                initialized=True,
                last_total_assets=10_000_000,
                last_proposed_total_assets=10_500_000,
                settlement_phase=SettlementPhase.SETTLING,
                last_valuation_time=datetime.now(UTC) - timedelta(hours=2),
                last_settlement_epoch=5,
                settlement_nonce=1,
            ),
        )

        strategy = _make_strategy()

        # On-chain total_assets matches proposed (settle succeeded)
        manager._vault_sdk.get_total_assets.return_value = 10_500_000

        result = asyncio.run(manager.run_settlement_cycle(strategy))

        assert result.success is True
        assert result.new_total_assets == 10_500_000
        assert result.epoch_id == 6
        assert manager.get_vault_state().settlement_phase == SettlementPhase.IDLE
        # Should NOT have called any execution (no tx needed)
        assert not manager._execution_orchestrator.execute.called

    def test_settle_not_confirmed_retries(self):
        """If on-chain total_assets doesn't match, retry settle."""
        manager = _make_manager(
            vault_state=VaultState(
                initialized=True,
                last_total_assets=10_000_000,
                last_proposed_total_assets=10_500_000,
                settlement_phase=SettlementPhase.SETTLING,
                last_valuation_time=datetime.now(UTC) - timedelta(hours=2),
            ),
        )

        strategy = _make_strategy()

        # On-chain total_assets doesn't match (settle didn't complete)
        manager._vault_sdk.get_total_assets.return_value = 10_000_000

        # Settle succeeds on retry
        manager._execution_orchestrator.execute = AsyncMock(return_value=_make_execution_result(success=True))

        result = asyncio.run(manager.run_settlement_cycle(strategy))

        assert result.success is True
        assert result.new_total_assets == 10_500_000
        # Should have called settle
        assert manager._vault_adapter.build_settle_deposit_bundle.called

    def test_settle_retry_fails_stays_proposed(self):
        """If retried settle fails, revert to PROPOSED for next attempt."""
        manager = _make_manager(
            vault_state=VaultState(
                initialized=True,
                last_total_assets=10_000_000,
                last_proposed_total_assets=10_500_000,
                settlement_phase=SettlementPhase.SETTLING,
                last_valuation_time=datetime.now(UTC) - timedelta(hours=2),
            ),
        )

        strategy = _make_strategy()

        # On-chain total_assets doesn't match
        manager._vault_sdk.get_total_assets.return_value = 10_000_000

        # Settle fails on retry
        manager._execution_orchestrator.execute = AsyncMock(
            return_value=_make_execution_result(success=False, error="settle reverted")
        )

        result = asyncio.run(manager.run_settlement_cycle(strategy))

        assert result.success is False
        assert manager.get_vault_state().settlement_phase == SettlementPhase.PROPOSED

    def test_settle_with_zero_proposed_retries(self):
        """When proposed_total_assets is 0 and on-chain is also 0, retry (can't confirm)."""
        manager = _make_manager(
            vault_state=VaultState(
                initialized=True,
                last_total_assets=0,
                last_proposed_total_assets=0,
                settlement_phase=SettlementPhase.SETTLING,
                last_valuation_time=datetime.now(UTC) - timedelta(hours=2),
            ),
        )

        strategy = _make_strategy()

        # Both are 0 -- ambiguous, so we retry
        manager._vault_sdk.get_total_assets.return_value = 0

        # Settle succeeds on retry
        manager._execution_orchestrator.execute = AsyncMock(return_value=_make_execution_result(success=True))

        result = asyncio.run(manager.run_settlement_cycle(strategy))

        assert result.success is True
        # Should have retried settle (not assumed confirmed)
        assert manager._vault_adapter.build_settle_deposit_bundle.called


class TestResumeFromSettled:
    """Crash recovery: process crashed after settle completed (SETTLED phase)."""

    def test_completes_finalization(self):
        """Resuming from SETTLED just completes finalization to IDLE."""
        manager = _make_manager(
            vault_state=VaultState(
                initialized=True,
                last_total_assets=10_000_000,
                last_proposed_total_assets=10_500_000,
                settlement_phase=SettlementPhase.SETTLED,
                last_valuation_time=datetime.now(UTC) - timedelta(hours=2),
                last_settlement_epoch=5,
            ),
        )

        strategy = _make_strategy()

        result = asyncio.run(manager.run_settlement_cycle(strategy))

        assert result.success is True
        assert result.new_total_assets == 10_500_000
        assert result.epoch_id == 6

        state = manager.get_vault_state()
        assert state.settlement_phase == SettlementPhase.IDLE
        assert state.last_total_assets == 10_500_000
        assert state.last_settlement_epoch == 6
        assert state.last_valuation_time is not None

        # No execution calls needed
        assert not manager._execution_orchestrator.execute.called

    def test_no_propose_or_settle_calls(self):
        """Resuming from SETTLED should not build any bundles."""
        manager = _make_manager(
            vault_state=VaultState(
                initialized=True,
                last_total_assets=10_000_000,
                last_proposed_total_assets=10_500_000,
                settlement_phase=SettlementPhase.SETTLED,
                last_valuation_time=datetime.now(UTC) - timedelta(hours=2),
            ),
        )

        strategy = _make_strategy()
        asyncio.run(manager.run_settlement_cycle(strategy))

        assert not manager._vault_adapter.build_propose_valuation_bundle.called
        assert not manager._vault_adapter.build_settle_deposit_bundle.called
        assert not manager._vault_adapter.build_settle_redeem_bundle.called


class TestProposeTxFailure:
    """Tests for propose transaction failure during fresh settlement."""

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

        assert not manager._vault_adapter.build_settle_deposit_bundle.called
        assert not manager._vault_adapter.build_settle_redeem_bundle.called


class TestSettleTxFailure:
    """Tests for settle transaction failure during fresh settlement."""

    def test_settle_failure_reverts_to_proposed(self):
        """Failed settle deposit tx reverts settlement_phase to PROPOSED for retry."""
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


class TestCrashRecoveryNonce:
    """Test that settlement nonce prevents false-positive recovery."""

    def test_same_value_different_epoch_retries_propose(self):
        """When nonce is 0 (fresh start), don't skip propose even if values match."""
        manager = _make_manager(
            vault_state=VaultState(
                initialized=True,
                last_total_assets=10_000_000,
                last_proposed_total_assets=10_500_000,
                settlement_phase=SettlementPhase.PROPOSING,
                last_valuation_time=datetime.now(UTC) - timedelta(hours=2),
                settlement_nonce=0,  # Fresh start -- no nonce from current epoch
            ),
        )

        strategy = _make_strategy()

        # On-chain proposed matches (from a prior epoch, but nonce=0 so we can't confirm)
        manager._vault_sdk.get_proposed_total_assets.return_value = 10_500_000

        # Both propose and settle succeed
        manager._execution_orchestrator.execute = AsyncMock(return_value=_make_execution_result(success=True))

        result = asyncio.run(manager.run_settlement_cycle(strategy))

        assert result.success is True
        # Should have retried propose (not assumed confirmed due to nonce=0)
        assert manager._vault_adapter.build_propose_valuation_bundle.called

    def test_same_value_different_epoch_retries_settle(self):
        """When nonce is 0, don't skip settle even if values match."""
        manager = _make_manager(
            vault_state=VaultState(
                initialized=True,
                last_total_assets=10_000_000,
                last_proposed_total_assets=10_500_000,
                settlement_phase=SettlementPhase.SETTLING,
                last_valuation_time=datetime.now(UTC) - timedelta(hours=2),
                settlement_nonce=0,  # Fresh start -- no nonce from current epoch
            ),
        )

        strategy = _make_strategy()

        # On-chain total_assets matches proposed (from a prior epoch)
        manager._vault_sdk.get_total_assets.return_value = 10_500_000

        # Settle succeeds on retry
        manager._execution_orchestrator.execute = AsyncMock(return_value=_make_execution_result(success=True))

        result = asyncio.run(manager.run_settlement_cycle(strategy))

        assert result.success is True
        # Should have retried settle (not assumed confirmed due to nonce=0)
        assert manager._vault_adapter.build_settle_deposit_bundle.called

    def test_nonce_incremented_during_propose(self):
        """Nonce is incremented when entering PROPOSING phase."""
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

        manager._execution_orchestrator.execute = AsyncMock(return_value=_make_execution_result(success=True))

        with patch("almanak.framework.vault.lifecycle.get_token_resolver") as mock_resolver:
            mock_resolver.return_value.get_decimals.return_value = 6
            result = asyncio.run(manager.run_settlement_cycle(strategy))

        assert result.success is True
        # After successful finalization, nonce is reset to 0
        assert manager.get_vault_state().settlement_nonce == 0

    def test_nonce_reset_after_finalization(self):
        """After successful settlement, nonce resets to 0."""
        manager = _make_manager(
            vault_state=VaultState(
                initialized=True,
                last_total_assets=10_000_000,
                last_proposed_total_assets=10_500_000,
                settlement_phase=SettlementPhase.SETTLED,
                last_valuation_time=datetime.now(UTC) - timedelta(hours=2),
                settlement_nonce=3,
            ),
        )

        strategy = _make_strategy()
        result = asyncio.run(manager.run_settlement_cycle(strategy))

        assert result.success is True
        assert manager.get_vault_state().settlement_nonce == 0


class TestResumeFromProposedSpentProposal:
    """PROPOSED resume must not retry settleDeposit against a spent proposal (VIB-5645)."""

    def test_spent_proposal_deposit_landed_finalizes_without_retry(self):
        """Spent proposal + settleDeposit already landed -> finalize, never retry settleDeposit."""
        manager = _make_manager(
            vault_state=VaultState(
                initialized=True,
                last_total_assets=10_000_000,
                last_proposed_total_assets=10_500_000,
                settlement_phase=SettlementPhase.PROPOSED,
                last_valuation_time=datetime.now(UTC) - timedelta(hours=2),
                settlement_nonce=1,
            ),
        )

        strategy = _make_strategy()

        # Proposal already consumed (sentinel) and deposit already landed on-chain.
        manager._vault_sdk.has_live_proposal.return_value = False
        manager._vault_sdk.get_total_assets.return_value = 10_500_000
        manager._vault_sdk.get_underlying_balance.return_value = 0  # no redeem shares

        manager._execution_orchestrator.execute = AsyncMock(return_value=_make_execution_result(success=True))

        result = asyncio.run(manager.run_settlement_cycle(strategy))

        assert result.success is True
        # CRITICAL: settleDeposit must NOT be retried against the spent proposal.
        assert not manager._vault_adapter.build_settle_deposit_bundle.called
        assert not manager._vault_adapter.build_propose_valuation_bundle.called
        assert manager.get_vault_state().settlement_phase == SettlementPhase.IDLE

    def test_spent_proposal_deposit_not_landed_re_proposes(self):
        """Spent proposal but settleDeposit never landed -> re-propose from scratch."""
        manager = _make_manager(
            vault_state=VaultState(
                initialized=True,
                last_total_assets=10_000_000,
                last_proposed_total_assets=10_500_000,
                settlement_phase=SettlementPhase.PROPOSED,
                last_valuation_time=datetime.now(UTC) - timedelta(hours=2),
                settlement_nonce=1,
            ),
        )

        strategy = _make_strategy()

        # Proposal consumed, but deposit did not land (on-chain total unchanged).
        manager._vault_sdk.has_live_proposal.return_value = False
        manager._vault_sdk.get_total_assets.return_value = 10_000_000
        manager._vault_sdk.get_underlying_balance.return_value = 0

        manager._execution_orchestrator.execute = AsyncMock(return_value=_make_execution_result(success=True))

        result = asyncio.run(manager.run_settlement_cycle(strategy))

        assert result.success is True
        # Re-proposed (fresh cycle) then settled.
        assert manager._vault_adapter.build_propose_valuation_bundle.called
        assert manager._vault_adapter.build_settle_deposit_bundle.called

    def test_live_proposal_settles_as_normal(self):
        """A still-live proposal at PROPOSED settles the deposit leg as normal."""
        manager = _make_manager(
            vault_state=VaultState(
                initialized=True,
                last_total_assets=10_000_000,
                last_proposed_total_assets=10_500_000,
                settlement_phase=SettlementPhase.PROPOSED,
                last_valuation_time=datetime.now(UTC) - timedelta(hours=2),
                settlement_nonce=1,
            ),
        )

        strategy = _make_strategy()
        manager._vault_sdk.has_live_proposal.return_value = True
        manager._vault_sdk.get_underlying_balance.return_value = 0

        manager._execution_orchestrator.execute = AsyncMock(return_value=_make_execution_result(success=True))

        result = asyncio.run(manager.run_settlement_cycle(strategy))

        assert result.success is True
        assert manager._vault_adapter.build_settle_deposit_bundle.called
        assert not manager._vault_adapter.build_propose_valuation_bundle.called
        assert manager.get_vault_state().settlement_phase == SettlementPhase.IDLE


class TestResumeRedeemLeg:
    """Crash recovery from the redeem-leg sub-phases (Lagoon v0.5.0 second proposal)."""

    def _base_state(self, phase: SettlementPhase, nonce: int = 2) -> VaultState:
        return VaultState(
            initialized=True,
            last_total_assets=10_000_000,
            last_proposed_total_assets=10_500_000,
            settlement_phase=phase,
            last_valuation_time=datetime.now(UTC) - timedelta(hours=2),
            settlement_nonce=nonce,
        )

    def test_proposing_redeem_live_proposal_settles_redeem(self):
        """PROPOSING_REDEEM with a live proposal #2 -> settleRedeem, no re-propose."""
        manager = _make_manager(vault_state=self._base_state(SettlementPhase.PROPOSING_REDEEM))
        strategy = _make_strategy()
        manager._vault_sdk.has_live_proposal.return_value = True
        manager._vault_sdk.get_underlying_balance.return_value = 0  # drained after redeem
        manager._execution_orchestrator.execute = AsyncMock(return_value=_make_execution_result(success=True))

        result = asyncio.run(manager.run_settlement_cycle(strategy))

        assert result.success is True
        assert manager._vault_adapter.build_settle_redeem_bundle.called
        assert not manager._vault_adapter.build_propose_valuation_bundle.called
        assert manager.get_vault_state().settlement_phase == SettlementPhase.IDLE

    def test_proposing_redeem_no_live_proposal_reproposes(self):
        """PROPOSING_REDEEM with no live proposal #2 -> re-issue proposal #2 then settleRedeem."""
        manager = _make_manager(vault_state=self._base_state(SettlementPhase.PROPOSING_REDEEM))
        strategy = _make_strategy()
        manager._vault_sdk.has_live_proposal.return_value = False
        manager._vault_sdk.get_underlying_balance.return_value = 0
        manager._execution_orchestrator.execute = AsyncMock(return_value=_make_execution_result(success=True))

        result = asyncio.run(manager.run_settlement_cycle(strategy))

        assert result.success is True
        assert manager._vault_adapter.build_propose_valuation_bundle.called  # proposal #2 re-issued
        assert manager._vault_adapter.build_settle_redeem_bundle.called

    def test_proposed_redeem_executes_settle_redeem(self):
        """PROPOSED_REDEEM -> settleRedeem directly (skip propose)."""
        manager = _make_manager(vault_state=self._base_state(SettlementPhase.PROPOSED_REDEEM))
        strategy = _make_strategy()
        manager._vault_sdk.get_underlying_balance.return_value = 0
        manager._execution_orchestrator.execute = AsyncMock(return_value=_make_execution_result(success=True))

        result = asyncio.run(manager.run_settlement_cycle(strategy))

        assert result.success is True
        assert manager._vault_adapter.build_settle_redeem_bundle.called
        assert not manager._vault_adapter.build_propose_valuation_bundle.called
        assert manager.get_vault_state().settlement_phase == SettlementPhase.IDLE

    def test_settling_redeem_silo_drained_finalizes(self):
        """SETTLING_REDEEM with a drained silo -> finalize, no tx."""
        manager = _make_manager(vault_state=self._base_state(SettlementPhase.SETTLING_REDEEM))
        strategy = _make_strategy()
        manager._vault_sdk.get_underlying_balance.return_value = 0  # silo drained

        result = asyncio.run(manager.run_settlement_cycle(strategy))

        assert result.success is True
        assert not manager._execution_orchestrator.execute.called
        assert manager.get_vault_state().settlement_phase == SettlementPhase.IDLE
        # The settleRedeem tx landed in the PRE-CRASH run; whether that run
        # emitted its SETTLE_REDEEM commit is unverifiable (receipt gone), so a
        # resume-only finalize must surface accounting_degraded (VIB-5666).
        assert result.accounting_degraded is True

    def test_settling_redeem_shares_remain_live_proposal_retries(self):
        """SETTLING_REDEEM, shares remain + proposal #2 live (tx reverted) -> retry settleRedeem."""
        manager = _make_manager(vault_state=self._base_state(SettlementPhase.SETTLING_REDEEM))
        strategy = _make_strategy()
        # Shares still parked; proposal #2 still live -> the settleRedeem tx reverted.
        manager._vault_sdk.get_underlying_balance.side_effect = [5_000_000, 0]
        manager._vault_sdk.has_live_proposal.return_value = True
        manager._execution_orchestrator.execute = AsyncMock(return_value=_make_execution_result(success=True))

        result = asyncio.run(manager.run_settlement_cycle(strategy))

        assert result.success is True
        assert manager._vault_adapter.build_settle_redeem_bundle.called

    def test_settling_redeem_shares_remain_spent_proposal_carries_over(self):
        """SETTLING_REDEEM, shares remain + proposal #2 spent (safe illiquidity) -> finalize + carry."""
        manager = _make_manager(vault_state=self._base_state(SettlementPhase.SETTLING_REDEEM))
        strategy = _make_strategy()
        # Shares remain but proposal #2 was consumed -> settleRedeem ran, safe illiquid.
        manager._vault_sdk.get_underlying_balance.return_value = 5_000_000
        manager._vault_sdk.has_live_proposal.return_value = False
        manager._execution_orchestrator.execute = AsyncMock(return_value=_make_execution_result(success=True))

        result = asyncio.run(manager.run_settlement_cycle(strategy))

        assert result.success is True
        # One attempt per cycle: do NOT retry settleRedeem; carry remaining shares over.
        assert not manager._vault_adapter.build_settle_redeem_bundle.called
        assert manager.get_vault_state().settlement_phase == SettlementPhase.IDLE
        # Pre-crash settleRedeem consumed the proposal; its commit is unverifiable.
        assert result.accounting_degraded is True
