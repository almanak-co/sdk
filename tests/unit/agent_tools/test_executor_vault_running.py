"""Tests for RUNNING mode vault operations (P0-P4 priority loop).

Covers the tool calls an LLM would invoke in 24/7 RUNNING mode:
- P0: Teardown (full happy path with LP + swaps + settlement)
- P1: Settle with pending deposits (auto-computed NAV)
- P1: Settle with pending redeems and liquidity check
- P2: LP health check (get_lp_position, compute_rebalance_candidate)
- P4: Save/load agent state round-trip
"""

import json
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.agent_tools.executor import ToolExecutor
from almanak.framework.agent_tools.policy import AgentPolicy
from almanak.framework.agent_tools.schemas import ToolResponse


@pytest.fixture
def mock_gateway():
    """Create a mock GatewayClient with service stubs."""
    client = MagicMock()
    client.is_connected = True
    return client


@pytest.fixture
def executor(mock_gateway):
    """Create a ToolExecutor with permissive policy for vault testing."""
    policy = AgentPolicy(
        allowed_chains={"base", "arbitrum"},
        max_tool_calls_per_minute=100,
        cooldown_seconds=0,
        max_single_trade_usd=Decimal("999999999"),
        max_daily_spend_usd=Decimal("999999999"),
        max_position_size_usd=Decimal("999999999"),
        require_human_approval_above_usd=Decimal("999999999"),
    )
    return ToolExecutor(
        mock_gateway,
        policy=policy,
        wallet_address="0x1234567890abcdef1234567890abcdef12345678",
        strategy_id="test-strategy",
        default_chain="base",
    )


def _mock_rpc_call(success=True, result="0x0"):
    """Create a mock RPC response."""
    resp = MagicMock()
    resp.success = success
    resp.result = json.dumps(result)
    resp.error = ""
    return resp


def _mock_exec_response(success=True, tx_hashes=None, error=""):
    """Create a mock execution response."""
    resp = MagicMock()
    resp.success = success
    resp.tx_hashes = tx_hashes or ["0xabc123"]
    resp.error = error
    resp.receipts = b"[]"
    return resp


# ── P1: Settle with pending deposits ────────────────────────────

class TestSettleWithPendingDeposits:
    """P1 scenario: vault has pending deposits, agent settles to process them."""

    @pytest.mark.asyncio
    async def test_settle_auto_computes_nav_and_processes_deposits(self, executor, mock_gateway):
        """settle_vault without new_total_assets computes NAV from Safe balance."""
        usdc_address = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
        asset_result = "0x" + "0" * 24 + usdc_address[2:]
        vault = "0x" + "a" * 40
        safe = "0x" + "b" * 40

        rpc_responses = [
            # NAV computation: asset(), balanceOf(safe), silo
            _mock_rpc_call(result=asset_result),
            _mock_rpc_call(result=hex(20_000_000)),  # 20 USDC in Safe
            _mock_rpc_call(result="0x" + "0" * 64),  # silo = zero addr
            # pending_deposits check
            _mock_rpc_call(result=hex(5_000_000)),  # 5 USDC pending
            # liquidity check: pending_redemptions
            _mock_rpc_call(result=hex(0)),  # no pending redeems
        ]
        mock_gateway.rpc.Call.side_effect = rpc_responses

        # No LP in agent state
        state_resp = MagicMock()
        state_resp.data = json.dumps({}).encode()
        mock_gateway.state.LoadState.return_value = state_resp

        # Execution succeeds (propose + settle)
        mock_gateway.execution.Execute.return_value = _mock_exec_response()

        result = await executor.execute("settle_vault", {
            "vault_address": vault,
            "safe_address": safe,
            "valuator_address": "0x1234567890abcdef1234567890abcdef12345678",
            "chain": "base",
        })

        assert result.status == "success"
        assert result.data["new_total_assets"] == "20000000"

    @pytest.mark.asyncio
    async def test_settle_with_pending_redeems_checks_liquidity(self, executor, mock_gateway):
        """settle_vault blocks when Safe doesn't have enough for pending redeems."""
        with patch('almanak.framework.connectors.lagoon.sdk.LagoonVaultSDK') as MockSDK:
            sdk = MockSDK.return_value
            sdk.get_underlying_token_address.return_value = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
            sdk.get_pending_redemptions.return_value = 50_000_000_000_000_000_000  # 50 shares
            sdk.get_share_price.return_value = Decimal("0.000000000001")  # 1 USDC per share

            # Safe only has 10 USDC but needs 50
            mock_gateway.rpc.Call.return_value = _mock_rpc_call(result=hex(10_000_000))

            sufficient, liquid, needed = await executor._check_settlement_liquidity(
                "0x" + "a" * 40, "0x" + "b" * 40, "base"
            )

            assert sufficient is False
            assert liquid == 10_000_000
            assert needed == 50_000_000


# ── P1: get_vault_state ────────────────────────────────────────

class TestGetVaultState:
    """P1 reads vault state to detect pending deposits/redeems."""

    @pytest.mark.asyncio
    async def test_get_vault_state_returns_all_fields(self, executor, mock_gateway):
        """get_vault_state returns total_assets, pending_deposits, pending_redeems, share_price."""
        with patch('almanak.framework.connectors.lagoon.sdk.LagoonVaultSDK') as MockSDK:
            sdk = MockSDK.return_value
            sdk.get_total_assets.return_value = 100_000_000  # 100 USDC
            sdk.get_pending_deposits.return_value = 5_000_000  # 5 USDC pending
            sdk.get_pending_redemptions.return_value = 2_000_000_000_000_000_000  # 2 shares pending
            sdk.get_share_price.return_value = Decimal("1.05")

            result = await executor.execute("get_vault_state", {
                "vault_address": "0x" + "a" * 40,
                "chain": "base",
            })

            assert result.status == "success"
            assert result.data["total_assets"] == "100000000"
            assert result.data["pending_deposits"] == "5000000"
            assert result.data["pending_redeems"] == "2000000000000000000"
            assert result.data["share_price"] == "1.05"

    @pytest.mark.asyncio
    async def test_get_vault_state_handles_rpc_failure(self, executor, mock_gateway):
        """get_vault_state returns error when RPC fails."""
        with patch('almanak.framework.connectors.lagoon.sdk.LagoonVaultSDK') as MockSDK:
            sdk = MockSDK.return_value
            sdk.get_total_assets.side_effect = Exception("RPC timeout")

            result = await executor.execute("get_vault_state", {
                "vault_address": "0x" + "a" * 40,
                "chain": "base",
            })

            assert result.status == "error"
            assert "vault_read_failed" in result.error["error_code"]


# ── P2: LP health check ─────────────────────────────────────────

class TestLPHealthCheck:
    """P2 scenario: agent checks LP position health and rebalance viability."""

    @pytest.mark.asyncio
    async def test_compute_rebalance_candidate_viable(self, executor, mock_gateway):
        """compute_rebalance_candidate returns viable=True when fees > gas."""
        # Gas price: 0.01 gwei (very cheap, Base L2)
        gas_resp = _mock_rpc_call(result=hex(10_000_000))  # 0.01 gwei
        mock_gateway.rpc.Call.return_value = gas_resp

        eth_price = MagicMock()
        eth_price.price = "2500.0"
        mock_gateway.market.GetPrice.return_value = eth_price

        result = await executor.execute("compute_rebalance_candidate", {
            "position_id": "42",
            "chain": "base",
            "fee_tier": 3000,
            "estimated_daily_volume": "10000",
            "our_liquidity_share": "0.1",
        })

        assert result.status == "success"
        assert result.data["viable"] is True
        assert float(result.data["breakdown"]["net_ev_usd"]) > 0

    @pytest.mark.asyncio
    async def test_compute_rebalance_candidate_not_viable_high_gas(self, executor, mock_gateway):
        """compute_rebalance_candidate returns viable=False when gas > fees."""
        # Gas price: 100 gwei (expensive)
        gas_resp = _mock_rpc_call(result=hex(100_000_000_000))  # 100 gwei
        mock_gateway.rpc.Call.return_value = gas_resp

        eth_price = MagicMock()
        eth_price.price = "2500.0"
        mock_gateway.market.GetPrice.return_value = eth_price

        result = await executor.execute("compute_rebalance_candidate", {
            "position_id": "42",
            "chain": "base",
            "fee_tier": 500,  # Low fee tier
            "estimated_daily_volume": "100",  # Low volume
            "our_liquidity_share": "0.01",  # Small share
        })

        assert result.status == "success"
        assert result.data["viable"] is False


# ── P0: Teardown full happy path ─────────────────────────────────

class TestTeardownHappyPath:
    """P0 scenario: full teardown with LP close + swap + settlement."""

    @pytest.mark.asyncio
    async def test_full_teardown_with_lp_and_swap(self, executor, mock_gateway):
        """Teardown closes LP, swaps non-underlying tokens, and settles."""
        vault = "0x" + "a" * 40
        safe = "0x" + "b" * 40
        valuator = "0x1234567890abcdef1234567890abcdef12345678"
        usdc = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
        almanak_token = "0x" + "e" * 40

        # Agent state has an LP position
        state_resp = MagicMock()
        state_resp.data = json.dumps({"lp_position_id": 42}).encode()
        mock_gateway.state.LoadState.return_value = state_resp

        # Mock state save
        save_resp = MagicMock()
        save_resp.success = True
        save_resp.new_version = 1
        save_resp.checksum = "abc"
        mock_gateway.state.SaveState.return_value = save_resp

        with patch('almanak.framework.connectors.lagoon.sdk.LagoonVaultSDK') as MockSDK:
            sdk_instance = MockSDK.return_value
            sdk_instance.get_underlying_token_address.return_value = usdc
            sdk_instance.get_total_assets.return_value = 15_000_000  # 15 USDC

            # Mock the executor's recursive calls (close_lp, swap, settle)
            call_count = {"n": 0}

            original_execute = executor.execute

            async def mock_execute(tool_name, args):
                call_count["n"] += 1

                if tool_name == "close_lp_position":
                    return ToolResponse(
                        status="success",
                        data={"tx_hash": "0xclose1", "collected_fees": {"token_a": "100", "token_b": "200"}},
                    )
                if tool_name == "swap_tokens":
                    return ToolResponse(
                        status="success",
                        data={"tx_hash": "0xswap1", "amount_out": "5000000"},
                    )
                if tool_name == "settle_vault":
                    return ToolResponse(
                        status="success",
                        data={"tx_hash": "0xsettle1", "new_total_assets": "15000000"},
                    )
                # Default passthrough for unexpected tools
                return ToolResponse(status="success", data={})

            with patch.object(executor, 'execute', side_effect=mock_execute):
                # Also mock LP position info (for swap token discovery)
                with patch.object(executor, '_execute_get_lp_position') as mock_lp:
                    mock_lp.return_value = ToolResponse(
                        status="success",
                        data={
                            "position_id": "42",
                            "token_a": almanak_token,
                            "token_b": usdc,
                            "liquidity": "1000000",
                        },
                    )

                    # Mock balance check for swap (has ALMANAK tokens to swap)
                    balance_hex = hex(500_000_000_000_000_000)  # 0.5 ALMANAK
                    mock_gateway.rpc.Call.return_value = _mock_rpc_call(result=balance_hex)

                    result = await executor._execute_teardown_vault({
                        "vault_address": vault,
                        "safe_address": safe,
                        "valuator_address": valuator,
                        "chain": "base",
                    })

            assert result.status == "success"
            assert result.data["positions_closed"] == 1
            assert result.data["final_nav"] == "15000000"

    @pytest.mark.asyncio
    async def test_teardown_resumes_from_lp_closed_phase(self, executor, mock_gateway):
        """Teardown that crashed after LP close resumes from swapping phase."""
        vault = "0x" + "a" * 40
        safe = "0x" + "b" * 40
        valuator = "0x1234567890abcdef1234567890abcdef12345678"
        usdc = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"

        # Agent state has teardown progress from a previous crash
        state_resp = MagicMock()
        state_resp.data = json.dumps({
            "lp_position_id": None,  # Already closed
            "_teardown": {
                "phase": "lp_closed",
                "positions_closed": 1,
            },
        }).encode()
        mock_gateway.state.LoadState.return_value = state_resp

        save_resp = MagicMock()
        save_resp.success = True
        save_resp.new_version = 2
        save_resp.checksum = "def"
        mock_gateway.state.SaveState.return_value = save_resp

        with patch('almanak.framework.connectors.lagoon.sdk.LagoonVaultSDK') as MockSDK:
            sdk_instance = MockSDK.return_value
            sdk_instance.get_underlying_token_address.return_value = usdc
            sdk_instance.get_total_assets.return_value = 20_000_000

            with patch.object(executor, 'execute') as mock_execute:
                async def side_effect(tool_name, args):
                    if tool_name == "settle_vault":
                        return ToolResponse(
                            status="success",
                            data={"tx_hash": "0xsettle2", "new_total_assets": "20000000"},
                        )
                    return ToolResponse(status="success", data={})

                mock_execute.side_effect = side_effect

                result = await executor._execute_teardown_vault({
                    "vault_address": vault,
                    "safe_address": safe,
                    "valuator_address": valuator,
                    "chain": "base",
                })

        assert result.status == "success"
        # Should have inherited positions_closed from saved progress
        assert result.data["positions_closed"] == 1


# ── P4: Save/load agent state round-trip ─────────────────────────

class TestAgentStateRoundTrip:
    """P4 scenario: agent saves state after each iteration and loads on resume."""

    @pytest.mark.asyncio
    async def test_save_and_load_state(self, executor, mock_gateway):
        """save_agent_state persists data and load_agent_state retrieves it."""
        state_payload = {
            "vault_address": "0x" + "a" * 40,
            "lp_position_id": 42,
            "pool": "ALMANAK/USDC/3000",
            "phase": "running",
            "last_rebalance_tick": -500,
        }

        # Mock save
        save_resp = MagicMock()
        save_resp.success = True
        save_resp.new_version = 1
        save_resp.checksum = "abc123"
        mock_gateway.state.SaveState.return_value = save_resp

        save_result = await executor.execute("save_agent_state", {
            "strategy_id": "defai-vault-lp",
            "state": state_payload,
        })

        assert save_result.status == "success"
        assert save_result.data["version"] == 1

        # Verify what was saved
        save_call = mock_gateway.state.SaveState.call_args[0][0]
        saved_data = json.loads(save_call.data)
        assert saved_data["vault_address"] == state_payload["vault_address"]
        assert saved_data["lp_position_id"] == 42

        # Mock load
        load_resp = MagicMock()
        load_resp.data = json.dumps(state_payload).encode()
        load_resp.version = 1
        mock_gateway.state.LoadState.return_value = load_resp

        load_result = await executor.execute("load_agent_state", {
            "strategy_id": "defai-vault-lp",
        })

        assert load_result.status == "success"
        assert load_result.data["state"]["vault_address"] == state_payload["vault_address"]
        assert load_result.data["state"]["lp_position_id"] == 42
        assert load_result.data["version"] == 1

    @pytest.mark.asyncio
    async def test_load_state_not_found_returns_empty(self, executor, mock_gateway):
        """load_agent_state returns empty dict when no previous state exists."""
        mock_gateway.state.LoadState.side_effect = Exception("NOT_FOUND: no state for strategy")

        result = await executor.execute("load_agent_state", {
            "strategy_id": "brand-new-strategy",
        })

        assert result.status == "success"
        assert result.data["state"] == {}
        assert result.data["version"] == 0

    @pytest.mark.asyncio
    async def test_load_state_real_error_propagates(self, executor, mock_gateway):
        """load_agent_state returns error for non-NOT_FOUND failures."""
        mock_gateway.state.LoadState.side_effect = Exception("UNAVAILABLE: gateway down")

        result = await executor.execute("load_agent_state", {
            "strategy_id": "defai-vault-lp",
        })

        assert result.status == "error"
        assert "state_load_failed" in result.error["error_code"]


# ── Alerting: verify _fire_alert is called on failures ────────────

class TestAlertingOnFailures:
    """Verify _fire_alert fires on critical vault failure paths."""

    @pytest.fixture
    def executor_with_alerts(self, mock_gateway):
        """Create executor with a mock alert manager."""
        policy = AgentPolicy(
            allowed_chains={"base"},
            max_tool_calls_per_minute=100,
            cooldown_seconds=0,
            max_single_trade_usd=Decimal("999999999"),
            max_daily_spend_usd=Decimal("999999999"),
        )
        alert_mgr = MagicMock()
        # Make send_alert return a coroutine
        import asyncio

        async def mock_send_alert(**kwargs):
            return MagicMock(success=True)

        alert_mgr.send_alert = mock_send_alert
        return ToolExecutor(
            mock_gateway,
            policy=policy,
            wallet_address="0x1234567890abcdef1234567890abcdef12345678",
            strategy_id="test-strategy",
            default_chain="base",
            alert_manager=alert_mgr,
        )

    @pytest.mark.asyncio
    async def test_propose_failure_fires_alert(self, executor_with_alerts, mock_gateway):
        """Propose NAV failure triggers critical alert."""
        usdc_address = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
        asset_result = "0x" + "0" * 24 + usdc_address[2:]

        rpc_responses = [
            # NAV computation
            _mock_rpc_call(result=asset_result),
            _mock_rpc_call(result=hex(10_000_000)),
            _mock_rpc_call(result="0x" + "0" * 64),
            # pending_deposits
            _mock_rpc_call(result=hex(0)),
            # liquidity check
            _mock_rpc_call(result=hex(0)),
        ]
        mock_gateway.rpc.Call.side_effect = rpc_responses

        state_resp = MagicMock()
        state_resp.data = json.dumps({}).encode()
        mock_gateway.state.LoadState.return_value = state_resp

        # Propose fails
        mock_gateway.execution.Execute.return_value = _mock_exec_response(
            success=False, error="execution reverted"
        )

        # Save settlement state
        save_resp = MagicMock()
        save_resp.success = True
        save_resp.new_version = 1
        save_resp.checksum = "x"
        mock_gateway.state.SaveState.return_value = save_resp

        result = await executor_with_alerts.execute("settle_vault", {
            "vault_address": "0x" + "a" * 40,
            "safe_address": "0x" + "b" * 40,
            "valuator_address": "0x1234567890abcdef1234567890abcdef12345678",
            "chain": "base",
        })

        # execute() catches ExecutionFailedError and wraps in ToolResponse
        assert result.status == "error"
        assert "propose failed" in result.explanation.lower()

    @pytest.mark.asyncio
    async def test_teardown_lp_failure_fires_alert(self, executor_with_alerts, mock_gateway):
        """Teardown LP close failure triggers critical alert."""
        state_resp = MagicMock()
        state_resp.data = json.dumps({"lp_position_id": 42}).encode()
        mock_gateway.state.LoadState.return_value = state_resp

        save_resp = MagicMock()
        save_resp.success = True
        save_resp.new_version = 1
        save_resp.checksum = "x"
        mock_gateway.state.SaveState.return_value = save_resp

        with patch.object(executor_with_alerts, 'execute') as mock_execute:
            async def side_effect(tool_name, args):
                if tool_name == "close_lp_position":
                    return ToolResponse(
                        status="error",
                        error={"message": "Position already burned"},
                    )
                return ToolResponse(status="success", data={})

            mock_execute.side_effect = side_effect

            result = await executor_with_alerts._execute_teardown_vault({
                "vault_address": "0x" + "a" * 40,
                "safe_address": "0x" + "b" * 40,
                "valuator_address": "0x1234567890abcdef1234567890abcdef12345678",
                "chain": "base",
            })

        assert result.status == "error"
        assert result.error["error_code"] == "teardown_lp_close_failed"
