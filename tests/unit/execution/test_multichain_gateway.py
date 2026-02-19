"""Tests for gateway-backed MultiChainOrchestrator.

Tests the from_gateway() factory method and gateway-backed execution
path where all transactions are routed through the gateway sidecar.
"""

import json
from decimal import Decimal
from unittest.mock import MagicMock, AsyncMock, patch

import pytest

from almanak.framework.execution.multichain import MultiChainOrchestrator, IntentExecutionResult
from almanak.framework.execution.gateway_orchestrator import GatewayExecutionResult


@pytest.fixture
def mock_gateway_client():
    """Create a mock GatewayClient with execution and market stubs."""
    client = MagicMock()
    client.execution = MagicMock()
    client.market = MagicMock()
    client.health = MagicMock()
    return client


@pytest.fixture
def gateway_mco(mock_gateway_client):
    """Create a gateway-backed MultiChainOrchestrator."""
    return MultiChainOrchestrator.from_gateway(
        gateway_client=mock_gateway_client,
        chains=["arbitrum", "base"],
        wallet_address="0x1234567890abcdef1234567890abcdef12345678",
        max_gas_price_gwei=50,
    )


class TestFromGatewayFactory:
    """Test MultiChainOrchestrator.from_gateway() creation."""

    def test_creates_gateway_backed_instance(self, gateway_mco):
        assert gateway_mco._use_gateway is True
        assert gateway_mco._gateway_client is not None

    def test_chains_property(self, gateway_mco):
        assert gateway_mco.chains == ["arbitrum", "base"]

    def test_primary_chain_defaults_to_first(self, gateway_mco):
        assert gateway_mco.primary_chain == "arbitrum"

    def test_custom_primary_chain(self, mock_gateway_client):
        mco = MultiChainOrchestrator.from_gateway(
            gateway_client=mock_gateway_client,
            chains=["arbitrum", "base"],
            wallet_address="0x1234567890abcdef1234567890abcdef12345678",
            primary_chain="base",
        )
        assert mco.primary_chain == "base"

    def test_wallet_address_property(self, gateway_mco):
        assert gateway_mco.wallet_address == "0x1234567890abcdef1234567890abcdef12345678"

    def test_repr_shows_gateway_mode(self, gateway_mco):
        r = repr(gateway_mco)
        assert "gateway" in r.lower()
        assert "arbitrum" in r
        assert "base" in r


class TestGatewayOrchestrators:
    """Test per-chain GatewayExecutionOrchestrator management."""

    def test_lazy_creation(self, gateway_mco):
        """Orchestrators are created lazily on first use."""
        assert len(gateway_mco._gateway_orchestrators) == 0

        orch = gateway_mco._get_gateway_orchestrator("arbitrum")
        assert orch is not None
        assert len(gateway_mco._gateway_orchestrators) == 1

    def test_cached_across_calls(self, gateway_mco):
        """Same orchestrator is returned for same chain."""
        orch1 = gateway_mco._get_gateway_orchestrator("arbitrum")
        orch2 = gateway_mco._get_gateway_orchestrator("arbitrum")
        assert orch1 is orch2

    def test_per_chain_isolation(self, gateway_mco):
        """Different chains get different orchestrators."""
        orch_arb = gateway_mco._get_gateway_orchestrator("arbitrum")
        orch_base = gateway_mco._get_gateway_orchestrator("base")
        assert orch_arb is not orch_base
        assert orch_arb.chain == "arbitrum"
        assert orch_base.chain == "base"


class TestGatewayExecution:
    """Test execute() routing through gateway."""

    @pytest.mark.asyncio
    async def test_execute_routes_to_correct_chain(self, gateway_mco, mock_gateway_client):
        """Execute routes to the correct chain's orchestrator."""
        # Mock compile response
        compile_response = MagicMock()
        compile_response.success = True
        compile_response.action_bundle = json.dumps({"actions": []}).encode()
        mock_gateway_client.execution.CompileIntent = MagicMock(return_value=compile_response)

        # Mock execute response
        exec_response = MagicMock()
        exec_response.success = True
        exec_response.tx_hashes = ["0xabc"]
        exec_response.total_gas_used = 100000
        exec_response.receipts = json.dumps([{"status": 1}]).encode()
        exec_response.execution_id = "exec-1"
        exec_response.error = ""
        exec_response.error_code = ""
        mock_gateway_client.execution.Execute = MagicMock(return_value=exec_response)

        # Create a mock intent with chain set to "base"
        intent = MagicMock()
        intent.model_dump.return_value = {"token_in": "USDC", "token_out": "ETH"}
        intent.chain = "base"
        intent.intent_id = "test-intent-1"
        type(intent).__name__ = "SwapIntent"

        result = await gateway_mco.execute(intent)
        assert isinstance(result, IntentExecutionResult)
        assert result.success is True
        assert result.chain == "base"

    @pytest.mark.asyncio
    async def test_execute_uses_primary_chain_by_default(self, gateway_mco, mock_gateway_client):
        """When no chain on intent, uses primary chain."""
        compile_response = MagicMock()
        compile_response.success = True
        compile_response.action_bundle = json.dumps({"actions": []}).encode()
        mock_gateway_client.execution.CompileIntent = MagicMock(return_value=compile_response)

        exec_response = MagicMock()
        exec_response.success = True
        exec_response.tx_hashes = ["0xdef"]
        exec_response.total_gas_used = 50000
        exec_response.receipts = json.dumps([]).encode()
        exec_response.execution_id = "exec-2"
        exec_response.error = ""
        exec_response.error_code = ""
        mock_gateway_client.execution.Execute = MagicMock(return_value=exec_response)

        # Intent without explicit chain falls back to primary
        intent = MagicMock()
        intent.model_dump.return_value = {}
        intent.chain = None
        intent.intent_id = "test-intent-2"
        type(intent).__name__ = "HoldIntent"

        result = await gateway_mco.execute(intent)
        assert result.success is True
        assert result.chain == "arbitrum"  # primary chain


class TestGatewayInitialize:
    """Test initialize() in gateway mode."""

    @pytest.mark.asyncio
    async def test_initialize_creates_orchestrators(self, gateway_mco):
        """Initialize pre-creates orchestrators for all chains."""
        await gateway_mco.initialize()
        assert len(gateway_mco._gateway_orchestrators) == 2
        assert "arbitrum" in gateway_mco._gateway_orchestrators
        assert "base" in gateway_mco._gateway_orchestrators


class TestGatewayExecutionResultTxHash:
    """Test GatewayExecutionResult.tx_hash property."""

    def test_tx_hash_returns_first(self):
        result = GatewayExecutionResult(
            success=True,
            tx_hashes=["0xabc", "0xdef"],
            total_gas_used=100000,
            receipts=[],
            execution_id="test",
        )
        assert result.tx_hash == "0xabc"

    def test_tx_hash_returns_none_when_empty(self):
        result = GatewayExecutionResult(
            success=False,
            tx_hashes=[],
            total_gas_used=0,
            receipts=[],
            execution_id="test",
        )
        assert result.tx_hash is None


class TestGatewayGetBalances:
    """Test get_balances() in gateway mode."""

    @pytest.mark.asyncio
    async def test_get_balances_via_gateway(self, gateway_mco, mock_gateway_client):
        """get_balances() queries gateway market service per chain."""
        balance_response = MagicMock()
        balance_response.raw_balance = "1000000000000000000"
        mock_gateway_client.market.GetBalance = MagicMock(return_value=balance_response)

        balances = await gateway_mco.get_balances()
        assert isinstance(balances, dict)
        assert "arbitrum" in balances
        assert "base" in balances
        # Should query both chains
        assert mock_gateway_client.market.GetBalance.call_count == 2
