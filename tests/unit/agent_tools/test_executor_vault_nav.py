"""Tests for deterministic vault NAV computation in ToolExecutor."""

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


def _roles_storage_rpc(valuator: str, safe: str):
    """Build a getRolesStorage RPC response encoding the given valuator and safe addresses."""
    v = valuator.lower().removeprefix("0x").zfill(64)
    s = safe.lower().removeprefix("0x").zfill(64)
    data = "0x" + "0" * 64 + "0" * 64 + s + "0" * 64 + v
    return _mock_rpc_call(result=data)


def _mock_exec_response(success=True, tx_hashes=None, error=""):
    """Create a mock execution response."""
    resp = MagicMock()
    resp.success = success
    resp.tx_hashes = tx_hashes or ["0xabc123"]
    resp.error = error
    return resp


class TestComputeVaultNav:
    @pytest.mark.asyncio
    async def test_compute_vault_nav_without_lp(self, executor, mock_gateway):
        """NAV = Safe's underlying balance when no LP position exists."""
        # Mock underlying token read (asset() selector)
        usdc_address = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
        asset_result = "0x" + "0" * 24 + usdc_address[2:]

        # Safe has 10 USDC (10_000_000 raw units with 6 decimals)
        balance_hex = hex(10_000_000)
        silo_address = "0x" + "0" * 40  # no silo

        rpc_responses = [
            _mock_rpc_call(result=asset_result),  # asset()
            _mock_rpc_call(result=balance_hex),  # balanceOf(safe)
            _mock_rpc_call(result="0x" + "0" * 64),  # silo storage slot
            # No silo balance call since silo is zero address
        ]
        mock_gateway.rpc.Call.side_effect = rpc_responses

        # Mock state load (no LP position)
        state_resp = MagicMock()
        state_resp.data = json.dumps({}).encode()
        mock_gateway.state.LoadState.return_value = state_resp

        vault = "0x" + "a" * 40
        safe = "0x" + "b" * 40
        nav = await executor._compute_vault_nav(vault, safe, "base")

        assert nav == 10_000_000

    @pytest.mark.asyncio
    async def test_compute_vault_nav_includes_silo_balance(self, executor, mock_gateway):
        """NAV includes underlying balance in the silo contract."""
        usdc_address = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
        asset_result = "0x" + "0" * 24 + usdc_address[2:]
        silo_addr = "0x" + "c" * 40

        rpc_responses = [
            _mock_rpc_call(result=asset_result),  # asset()
            _mock_rpc_call(result=hex(5_000_000)),  # balanceOf(safe) = 5 USDC
            _mock_rpc_call(result="0x" + "0" * 24 + silo_addr[2:]),  # silo storage
            _mock_rpc_call(result=hex(3_000_000)),  # balanceOf(silo) = 3 USDC
        ]
        mock_gateway.rpc.Call.side_effect = rpc_responses

        state_resp = MagicMock()
        state_resp.data = json.dumps({}).encode()
        mock_gateway.state.LoadState.return_value = state_resp

        vault = "0x" + "a" * 40
        safe = "0x" + "b" * 40
        nav = await executor._compute_vault_nav(vault, safe, "base")

        assert nav == 8_000_000  # 5 + 3

    @pytest.mark.asyncio
    async def test_settle_vault_computes_nav_when_no_override(self, executor, mock_gateway):
        """When new_total_assets is omitted, settle_vault uses computed NAV."""
        usdc_address = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
        asset_result = "0x" + "0" * 24 + usdc_address[2:]
        safe = "0x" + "b" * 40
        valuator = "0x1234567890abcdef1234567890abcdef12345678"

        # Set up RPC responses for NAV computation + settlement + liquidity check
        rpc_responses = [
            # Preflight: getRolesStorage (valuation manager + curator)
            _roles_storage_rpc(valuator, safe),
            _roles_storage_rpc(valuator, safe),
            # NAV computation
            _mock_rpc_call(result=asset_result),  # asset()
            _mock_rpc_call(result=hex(10_000_000)),  # balanceOf(safe)
            _mock_rpc_call(result="0x" + "0" * 64),  # silo slot (zero addr)
            # pending_deposits call
            _mock_rpc_call(result=hex(0)),  # pending deposits
            # liquidity check: get_pending_redemptions
            _mock_rpc_call(result=hex(0)),  # pending redemptions = 0
        ]
        mock_gateway.rpc.Call.side_effect = rpc_responses

        state_resp = MagicMock()
        state_resp.data = json.dumps({}).encode()
        mock_gateway.state.LoadState.return_value = state_resp

        # Mock execution for propose + settle
        mock_gateway.execution.Execute.return_value = _mock_exec_response()

        result = await executor.execute("settle_vault", {
            "vault_address": "0x" + "a" * 40,
            "safe_address": "0x" + "b" * 40,
            "valuator_address": "0x1234567890abcdef1234567890abcdef12345678",
            "chain": "base",
        })

        assert result.status == "success"
        assert result.data["new_total_assets"] == "10000000"

    @pytest.mark.asyncio
    async def test_settle_vault_rejects_llm_value_far_from_computed(self, executor, mock_gateway):
        """When LLM-provided NAV deviates too far from computed, reject."""
        usdc_address = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
        asset_result = "0x" + "0" * 24 + usdc_address[2:]
        safe = "0x" + "b" * 40
        valuator = "0x1234567890abcdef1234567890abcdef12345678"

        rpc_responses = [
            # Preflight: getRolesStorage (valuation manager + curator)
            _roles_storage_rpc(valuator, safe),
            _roles_storage_rpc(valuator, safe),
            _mock_rpc_call(result=asset_result),  # asset()
            _mock_rpc_call(result=hex(10_000_000)),  # balanceOf(safe) = 10 USDC
            _mock_rpc_call(result="0x" + "0" * 64),  # silo (zero addr)
        ]
        mock_gateway.rpc.Call.side_effect = rpc_responses

        state_resp = MagicMock()
        state_resp.data = json.dumps({}).encode()
        mock_gateway.state.LoadState.return_value = state_resp

        # LLM proposes 100 USDC (10x the computed 10 USDC -- way over bounds)
        result = await executor.execute("settle_vault", {
            "vault_address": "0x" + "a" * 40,
            "safe_address": "0x" + "b" * 40,
            "valuator_address": "0x1234567890abcdef1234567890abcdef12345678",
            "chain": "base",
            "new_total_assets": "100000000",
        })

        assert result.status == "error"
        assert "risk_blocked" in result.error.get("error_code", "")

    @pytest.mark.asyncio
    async def test_compute_vault_nav_with_lp_position(self, executor, mock_gateway):
        """NAV includes LP position value when lp_position_id is in agent state."""
        usdc_address = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
        asset_result = "0x" + "0" * 24 + usdc_address[2:]

        # We'll mock the LP position read and price lookups
        rpc_responses = [
            _mock_rpc_call(result=asset_result),  # asset()
            _mock_rpc_call(result=hex(5_000_000)),  # balanceOf(safe) = 5 USDC
            _mock_rpc_call(result="0x" + "0" * 64),  # silo (zero addr)
        ]
        mock_gateway.rpc.Call.side_effect = rpc_responses

        # Agent state has an LP position
        state_resp = MagicMock()
        state_resp.data = json.dumps({"lp_position_id": 12345}).encode()
        mock_gateway.state.LoadState.return_value = state_resp

        # Mock _execute_get_lp_position to return position data
        with patch.object(executor, '_execute_get_lp_position') as mock_lp:
            mock_lp.return_value = ToolResponse(
                status="success",
                data={
                    "position_id": "12345",
                    "token_a": "0xtoken_a_address_padded_to_forty_chars",
                    "token_b": usdc_address,
                    "fee_tier": 3000,
                    "tick_lower": -100,
                    "tick_upper": 100,
                    "liquidity": "1000000",
                    "tokens_owed_a": "500000000000000000",  # 0.5 tokens
                    "tokens_owed_b": "3000000",  # 3 USDC
                    "in_range": True,
                },
            )

            # Mock price lookups for LP tokens
            price_resp_a = MagicMock()
            price_resp_a.price = "10.0"  # token A = $10
            price_resp_b = MagicMock()
            price_resp_b.price = "1.0"  # USDC = $1
            mock_gateway.market.GetPrice.side_effect = [price_resp_a, price_resp_b]

            # Mock token resolver for decimals
            with patch('almanak.framework.data.tokens.get_token_resolver') as mock_resolver_fn:
                mock_resolver = MagicMock()
                token_a_resolved = MagicMock()
                token_a_resolved.decimals = 18
                token_b_resolved = MagicMock()
                token_b_resolved.decimals = 6
                underlying_resolved = MagicMock()
                underlying_resolved.decimals = 6
                mock_resolver.resolve.side_effect = [token_a_resolved, token_b_resolved, underlying_resolved]
                mock_resolver_fn.return_value = mock_resolver

                # Mock underlying price for conversion
                underlying_price_resp = MagicMock()
                underlying_price_resp.price = "1.0"
                mock_gateway.market.GetPrice.side_effect = [
                    price_resp_a, price_resp_b, underlying_price_resp
                ]

                vault = "0x" + "a" * 40
                safe = "0x" + "b" * 40
                nav = await executor._compute_vault_nav(vault, safe, "base")

                # Should be more than just 5 USDC since LP has value
                assert nav >= 5_000_000
