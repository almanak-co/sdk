"""Tests for executor settle_vault crash-recovery state machine (A2)."""

import json
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.agent_tools.catalog import get_default_catalog
from almanak.framework.agent_tools.errors import ExecutionFailedError
from almanak.framework.agent_tools.executor import ToolExecutor
from almanak.framework.agent_tools.policy import AgentPolicy
from almanak.framework.agent_tools.schemas import ToolResponse


def _make_executor(
    *,
    settlement_phase: str = "idle",
    settlement_proposed_assets: int = 0,
    settlement_nonce: int = 0,
    vault_epoch_counter: int = 0,
) -> ToolExecutor:
    """Create a ToolExecutor with mocked gateway client and settlement state."""
    client = MagicMock()

    # Mock state service -- store settlement state for load/save
    state_data = {
        "_vault_settlement": {
            "phase": settlement_phase,
            "proposed_assets": settlement_proposed_assets,
            "nonce": settlement_nonce,
            "epoch_counter": vault_epoch_counter,
        }
    }

    def mock_load_state(req):
        resp = MagicMock()
        resp.data = json.dumps(state_data).encode()
        resp.version = 1
        return resp

    def mock_save_state(req):
        # Update state_data to reflect what was saved
        saved = json.loads(req.data)
        state_data.update(saved)
        resp = MagicMock()
        resp.success = True
        resp.new_version = 2
        return resp

    client.state.LoadState = mock_load_state
    client.state.SaveState = mock_save_state

    # Mock market service
    price_resp = MagicMock()
    price_resp.price = 1.0
    client.market.GetPrice = MagicMock(return_value=price_resp)

    policy = AgentPolicy(
        allowed_chains={"base"},
        allowed_tools={"settle_vault"},
        cooldown_seconds=0,
        require_simulation_before_execution=False,
    )
    executor = ToolExecutor(
        client,
        policy=policy,
        catalog=get_default_catalog(),
        wallet_address="0x1111111111111111111111111111111111111111",
        strategy_id="test-strategy",
        default_chain="base",
    )
    return executor


def _mock_lagoon_sdk(get_total_assets=10_000_000, get_proposed_total_assets=0, get_pending_deposits=0, get_pending_redemptions=0):
    """Create a mock LagoonVaultSDK."""
    sdk = MagicMock()
    sdk.get_total_assets.return_value = get_total_assets
    sdk.get_proposed_total_assets.return_value = get_proposed_total_assets
    sdk.get_pending_deposits.return_value = get_pending_deposits
    sdk.get_pending_redemptions.return_value = get_pending_redemptions
    sdk.get_underlying_token_address.return_value = "0xUSDC"
    sdk.get_silo_address.return_value = "0x" + "0" * 40
    return sdk


def _mock_adapter():
    """Create a mock LagoonVaultAdapter."""
    adapter = MagicMock()
    bundle = MagicMock()
    bundle.to_dict.return_value = {"intent_type": "test", "transactions": [], "metadata": {}}
    adapter.build_propose_valuation_bundle.return_value = bundle
    adapter.build_settle_deposit_bundle.return_value = bundle
    adapter.build_settle_redeem_bundle.return_value = bundle
    return adapter


def _mock_exec_response(success=True, tx_hashes=None, error=""):
    """Create a mock execution response."""
    resp = MagicMock()
    resp.success = success
    resp.tx_hashes = tx_hashes or ["0xabc123"]
    resp.error = error
    return resp


def _mock_rpc_response(result="0x0000000000000000000000000000000000000000000000000000000000989680"):
    """Create a mock RPC response (returns 10_000_000 in hex by default)."""
    resp = MagicMock()
    resp.success = True
    resp.result = json.dumps(result)
    return resp


SETTLE_ARGS = {
    "vault_address": "0x2222222222222222222222222222222222222222",
    "safe_address": "0x3333333333333333333333333333333333333333",
    "valuator_address": "0x1111111111111111111111111111111111111111",
    "chain": "base",
}


class TestSettleVaultCrashRecoveryPhases:
    """Test that settle_vault resumes correctly from each crash-recovery phase."""

    @pytest.mark.asyncio
    async def test_idle_runs_full_cycle(self):
        """From IDLE, runs full propose + settle + finalize."""
        executor = _make_executor(settlement_phase="idle")

        sdk = _mock_lagoon_sdk()
        adapter = _mock_adapter()
        exec_resp = _mock_exec_response(success=True)
        executor._client.execution.Execute = MagicMock(return_value=exec_resp)
        executor._client.rpc.Call = MagicMock(return_value=_mock_rpc_response())

        with (
            patch("almanak.framework.connectors.lagoon.sdk.LagoonVaultSDK", return_value=sdk),
            patch("almanak.framework.connectors.lagoon.adapter.LagoonVaultAdapter", return_value=adapter),
        ):
            result = await executor._execute_settle_vault(SETTLE_ARGS)

        assert result.status == "success"
        assert result.data["epoch_id"] == 1
        # Both propose and settle should have been called
        assert adapter.build_propose_valuation_bundle.called
        assert adapter.build_settle_deposit_bundle.called

    @pytest.mark.asyncio
    async def test_proposing_already_confirmed_skips_propose(self):
        """From PROPOSING with matching on-chain value and nonce>0, skip to settle."""
        executor = _make_executor(
            settlement_phase="proposing",
            settlement_proposed_assets=10_000_000,
            settlement_nonce=1,
        )

        sdk = _mock_lagoon_sdk(get_proposed_total_assets=10_000_000)
        adapter = _mock_adapter()
        exec_resp = _mock_exec_response(success=True)
        executor._client.execution.Execute = MagicMock(return_value=exec_resp)

        with (
            patch("almanak.framework.connectors.lagoon.sdk.LagoonVaultSDK", return_value=sdk),
            patch("almanak.framework.connectors.lagoon.adapter.LagoonVaultAdapter", return_value=adapter),
        ):
            result = await executor._execute_settle_vault(SETTLE_ARGS)

        assert result.status == "success"
        # Should NOT have called propose (skipped)
        assert not adapter.build_propose_valuation_bundle.called
        # Should have called settle
        assert adapter.build_settle_deposit_bundle.called

    @pytest.mark.asyncio
    async def test_proposing_nonce_zero_retries_propose(self):
        """From PROPOSING with nonce=0, retry propose even if values match (false-positive guard)."""
        executor = _make_executor(
            settlement_phase="proposing",
            settlement_proposed_assets=10_000_000,
            settlement_nonce=0,
        )

        sdk = _mock_lagoon_sdk(get_proposed_total_assets=10_000_000)
        adapter = _mock_adapter()
        exec_resp = _mock_exec_response(success=True)
        executor._client.execution.Execute = MagicMock(return_value=exec_resp)
        executor._client.rpc.Call = MagicMock(return_value=_mock_rpc_response())

        with (
            patch("almanak.framework.connectors.lagoon.sdk.LagoonVaultSDK", return_value=sdk),
            patch("almanak.framework.connectors.lagoon.adapter.LagoonVaultAdapter", return_value=adapter),
        ):
            result = await executor._execute_settle_vault(SETTLE_ARGS)

        assert result.status == "success"
        # Should have retried propose (nonce=0 means we can't confirm)
        assert adapter.build_propose_valuation_bundle.called

    @pytest.mark.asyncio
    async def test_proposed_skips_propose_does_settle(self):
        """From PROPOSED, skip propose and go directly to settle."""
        executor = _make_executor(
            settlement_phase="proposed",
            settlement_proposed_assets=10_000_000,
        )

        sdk = _mock_lagoon_sdk()
        adapter = _mock_adapter()
        exec_resp = _mock_exec_response(success=True)
        executor._client.execution.Execute = MagicMock(return_value=exec_resp)

        with (
            patch("almanak.framework.connectors.lagoon.sdk.LagoonVaultSDK", return_value=sdk),
            patch("almanak.framework.connectors.lagoon.adapter.LagoonVaultAdapter", return_value=adapter),
        ):
            result = await executor._execute_settle_vault(SETTLE_ARGS)

        assert result.status == "success"
        # Should NOT have called propose
        assert not adapter.build_propose_valuation_bundle.called
        # Should have called settle
        assert adapter.build_settle_deposit_bundle.called

    @pytest.mark.asyncio
    async def test_settling_already_confirmed_finalizes(self):
        """From SETTLING with matching on-chain value and nonce>0, finalize directly."""
        executor = _make_executor(
            settlement_phase="settling",
            settlement_proposed_assets=10_000_000,
            settlement_nonce=1,
            vault_epoch_counter=5,
        )

        sdk = _mock_lagoon_sdk(get_total_assets=10_000_000)
        adapter = _mock_adapter()

        with (
            patch("almanak.framework.connectors.lagoon.sdk.LagoonVaultSDK", return_value=sdk),
            patch("almanak.framework.connectors.lagoon.adapter.LagoonVaultAdapter", return_value=adapter),
        ):
            result = await executor._execute_settle_vault(SETTLE_ARGS)

        assert result.status == "success"
        assert result.data["epoch_id"] == 6
        # Should NOT have called any execution
        assert not adapter.build_propose_valuation_bundle.called
        assert not adapter.build_settle_deposit_bundle.called

    @pytest.mark.asyncio
    async def test_settling_not_confirmed_retries(self):
        """From SETTLING with non-matching on-chain value, retry settle."""
        executor = _make_executor(
            settlement_phase="settling",
            settlement_proposed_assets=10_500_000,
            settlement_nonce=1,
        )

        sdk = _mock_lagoon_sdk(get_total_assets=10_000_000)  # Doesn't match proposed
        adapter = _mock_adapter()
        exec_resp = _mock_exec_response(success=True)
        executor._client.execution.Execute = MagicMock(return_value=exec_resp)

        with (
            patch("almanak.framework.connectors.lagoon.sdk.LagoonVaultSDK", return_value=sdk),
            patch("almanak.framework.connectors.lagoon.adapter.LagoonVaultAdapter", return_value=adapter),
        ):
            result = await executor._execute_settle_vault(SETTLE_ARGS)

        assert result.status == "success"
        # Should have called settle (retry)
        assert adapter.build_settle_deposit_bundle.called

    @pytest.mark.asyncio
    async def test_settled_just_finalizes(self):
        """From SETTLED, just finalize (increment epoch, reset to idle)."""
        executor = _make_executor(
            settlement_phase="settled",
            settlement_proposed_assets=10_000_000,
            vault_epoch_counter=3,
        )

        sdk = _mock_lagoon_sdk()
        adapter = _mock_adapter()

        with (
            patch("almanak.framework.connectors.lagoon.sdk.LagoonVaultSDK", return_value=sdk),
            patch("almanak.framework.connectors.lagoon.adapter.LagoonVaultAdapter", return_value=adapter),
        ):
            result = await executor._execute_settle_vault(SETTLE_ARGS)

        assert result.status == "success"
        assert result.data["epoch_id"] == 4
        assert executor._settlement_phase == "idle"


class TestSettleVaultFailureRecovery:
    """Test that failures set the correct phase for retry."""

    @pytest.mark.asyncio
    async def test_propose_failure_resets_to_idle(self):
        """Failed propose resets to idle."""
        executor = _make_executor(settlement_phase="idle")

        sdk = _mock_lagoon_sdk()
        adapter = _mock_adapter()
        exec_resp = _mock_exec_response(success=False, error="tx reverted")
        executor._client.execution.Execute = MagicMock(return_value=exec_resp)
        executor._client.rpc.Call = MagicMock(return_value=_mock_rpc_response())

        with (
            patch("almanak.framework.connectors.lagoon.sdk.LagoonVaultSDK", return_value=sdk),
            patch("almanak.framework.connectors.lagoon.adapter.LagoonVaultAdapter", return_value=adapter),
        ):
            with pytest.raises(ExecutionFailedError):
                await executor._execute_settle_vault(SETTLE_ARGS)

        assert executor._settlement_phase == "idle"

    @pytest.mark.asyncio
    async def test_settle_failure_reverts_to_proposed(self):
        """Failed settle reverts to proposed (so next call retries settle, not propose)."""
        executor = _make_executor(
            settlement_phase="proposed",
            settlement_proposed_assets=10_000_000,
        )

        sdk = _mock_lagoon_sdk()
        adapter = _mock_adapter()
        exec_resp = _mock_exec_response(success=False, error="settle reverted")
        executor._client.execution.Execute = MagicMock(return_value=exec_resp)

        with (
            patch("almanak.framework.connectors.lagoon.sdk.LagoonVaultSDK", return_value=sdk),
            patch("almanak.framework.connectors.lagoon.adapter.LagoonVaultAdapter", return_value=adapter),
        ):
            with pytest.raises(ExecutionFailedError):
                await executor._execute_settle_vault(SETTLE_ARGS)

        assert executor._settlement_phase == "proposed"


class TestSettleVaultEpochTracking:
    """C6: Test that epoch_id is tracked and incremented correctly."""

    @pytest.mark.asyncio
    async def test_epoch_increments_on_success(self):
        """Each successful settlement increments the epoch counter."""
        executor = _make_executor(
            settlement_phase="settled",
            settlement_proposed_assets=10_000_000,
            vault_epoch_counter=0,
        )

        sdk = _mock_lagoon_sdk()
        adapter = _mock_adapter()

        with (
            patch("almanak.framework.connectors.lagoon.sdk.LagoonVaultSDK", return_value=sdk),
            patch("almanak.framework.connectors.lagoon.adapter.LagoonVaultAdapter", return_value=adapter),
        ):
            result = await executor._execute_settle_vault(SETTLE_ARGS)

        assert result.data["epoch_id"] == 1
        assert executor._vault_epoch_counter == 1

    @pytest.mark.asyncio
    async def test_epoch_continues_from_persisted_value(self):
        """Epoch counter continues from the persisted value, not zero."""
        executor = _make_executor(
            settlement_phase="settled",
            settlement_proposed_assets=10_000_000,
            vault_epoch_counter=42,
        )

        sdk = _mock_lagoon_sdk()
        adapter = _mock_adapter()

        with (
            patch("almanak.framework.connectors.lagoon.sdk.LagoonVaultSDK", return_value=sdk),
            patch("almanak.framework.connectors.lagoon.adapter.LagoonVaultAdapter", return_value=adapter),
        ):
            result = await executor._execute_settle_vault(SETTLE_ARGS)

        assert result.data["epoch_id"] == 43


class TestSettleVaultRedeems:
    """C6: Test that settle_redeem runs when pending redemptions exist."""

    @pytest.mark.asyncio
    async def test_settle_redeem_runs_when_pending(self):
        """After settle_deposit, if pending_redeems > 0, also runs settle_redeem."""
        executor = _make_executor(
            settlement_phase="proposed",
            settlement_proposed_assets=10_000_000,
        )

        sdk = _mock_lagoon_sdk(get_pending_redemptions=5_000_000)
        adapter = _mock_adapter()
        exec_resp = _mock_exec_response(success=True)
        executor._client.execution.Execute = MagicMock(return_value=exec_resp)

        with (
            patch("almanak.framework.connectors.lagoon.sdk.LagoonVaultSDK", return_value=sdk),
            patch("almanak.framework.connectors.lagoon.adapter.LagoonVaultAdapter", return_value=adapter),
        ):
            result = await executor._execute_settle_vault(SETTLE_ARGS)

        assert result.status == "success"
        # Should have called both settle_deposit and settle_redeem
        assert adapter.build_settle_deposit_bundle.called
        assert adapter.build_settle_redeem_bundle.called

    @pytest.mark.asyncio
    async def test_settle_redeem_skipped_when_none_pending(self):
        """When no pending redemptions, settle_redeem is not called."""
        executor = _make_executor(
            settlement_phase="proposed",
            settlement_proposed_assets=10_000_000,
        )

        sdk = _mock_lagoon_sdk(get_pending_redemptions=0)
        adapter = _mock_adapter()
        exec_resp = _mock_exec_response(success=True)
        executor._client.execution.Execute = MagicMock(return_value=exec_resp)

        with (
            patch("almanak.framework.connectors.lagoon.sdk.LagoonVaultSDK", return_value=sdk),
            patch("almanak.framework.connectors.lagoon.adapter.LagoonVaultAdapter", return_value=adapter),
        ):
            result = await executor._execute_settle_vault(SETTLE_ARGS)

        assert result.status == "success"
        assert adapter.build_settle_deposit_bundle.called
        assert not adapter.build_settle_redeem_bundle.called


class TestSettleVaultSimulationFlag:
    """C6: Test that propose step doesn't force is_safe_wallet=True."""

    @pytest.mark.asyncio
    async def test_propose_not_forced_safe_wallet(self):
        """Propose tx uses valuator EOA, should not be forced as safe_wallet."""
        executor = _make_executor(settlement_phase="idle")

        sdk = _mock_lagoon_sdk()
        adapter = _mock_adapter()
        exec_resp = _mock_exec_response(success=True)
        executor._client.execution.Execute = MagicMock(return_value=exec_resp)
        executor._client.rpc.Call = MagicMock(return_value=_mock_rpc_response())

        with (
            patch("almanak.framework.connectors.lagoon.sdk.LagoonVaultSDK", return_value=sdk),
            patch("almanak.framework.connectors.lagoon.adapter.LagoonVaultAdapter", return_value=adapter),
            patch.object(executor, "_resolve_simulation_flag", wraps=executor._resolve_simulation_flag) as mock_sim,
        ):
            await executor._execute_settle_vault(SETTLE_ARGS)

        # Check the calls to _resolve_simulation_flag
        # The propose call should NOT have is_safe_wallet=True
        propose_call = None
        for call in mock_sim.call_args_list:
            if "settle_vault.propose" in str(call):
                propose_call = call
                break

        if propose_call:
            # Verify is_safe_wallet is not set to True for propose
            kwargs = propose_call.kwargs if propose_call.kwargs else {}
            assert kwargs.get("is_safe_wallet") is not True
