"""Reusable pytest fixtures for agent tool testing with MockGatewayClient.

Import these fixtures in your conftest.py or use them directly::

    from tests.fixtures.agent_test_fixtures import mock_gateway, mock_executor

Or add to your conftest.py::

    pytest_plugins = ["tests.fixtures.agent_test_fixtures"]
"""

from decimal import Decimal

import pytest

from almanak.framework.agent_tools.executor import ToolExecutor
from almanak.framework.agent_tools.policy import AgentPolicy
from almanak.framework.agent_tools.testing import MockGatewayClient


@pytest.fixture
def mock_gateway():
    """Provides a MockGatewayClient with sensible defaults.

    Pre-configured with:
    - USDC balance: 10,000 on arbitrum
    - ETH balance: 5 on arbitrum
    - ETH price: $2,000
    - USDC price: $1
    - WETH price: $2,000
    """
    gw = MockGatewayClient()
    gw.set_balance("USDC", "arbitrum", Decimal("10000"))
    gw.set_balance("ETH", "arbitrum", Decimal("5"))
    gw.set_price("ETH", Decimal("2000"))
    gw.set_price("USDC", Decimal("1"))
    gw.set_price("WETH", Decimal("2000"))
    return gw


@pytest.fixture
def mock_executor(mock_gateway):
    """Provides a ToolExecutor wired to the mock gateway with permissive policy.

    Policy is configured to be permissive for testing:
    - Allowed tokens: USDC, ETH, WETH
    - Allowed protocols: uniswap_v3
    - High daily spend limit ($100k)
    - No cooldown
    - No rebalance check required
    """
    policy = AgentPolicy(
        allowed_tokens={"USDC", "ETH", "WETH"},
        allowed_protocols={"uniswap_v3"},
        allowed_chains={"arbitrum"},
        max_single_trade_usd=Decimal("100000"),
        max_daily_spend_usd=Decimal("100000"),
        max_position_size_usd=Decimal("100000"),
        require_human_approval_above_usd=Decimal("100000"),
        max_tool_calls_per_minute=100,
        cooldown_seconds=0,
        require_rebalance_check=False,
    )
    return ToolExecutor(
        mock_gateway,
        policy=policy,
        wallet_address="0x1234567890abcdef1234567890abcdef12345678",
        strategy_id="test-strategy",
    )


@pytest.fixture
def strict_executor(mock_gateway):
    """Provides a ToolExecutor with strict/realistic policy limits.

    Policy mirrors production-like constraints:
    - $10k daily spend limit
    - $5k single trade limit
    - 10 trades per hour
    - 5-minute cooldown
    - Rebalance check required
    """
    policy = AgentPolicy(
        allowed_tokens={"USDC", "ETH", "WETH"},
        allowed_protocols={"uniswap_v3"},
        allowed_chains={"arbitrum"},
        max_single_trade_usd=Decimal("5000"),
        max_daily_spend_usd=Decimal("10000"),
        max_position_size_usd=Decimal("50000"),
        require_human_approval_above_usd=Decimal("5000"),
        max_tool_calls_per_minute=60,
        max_trades_per_hour=10,
        cooldown_seconds=300,
        require_rebalance_check=True,
    )
    return ToolExecutor(
        mock_gateway,
        policy=policy,
        wallet_address="0x1234567890abcdef1234567890abcdef12345678",
        strategy_id="test-strategy",
    )
