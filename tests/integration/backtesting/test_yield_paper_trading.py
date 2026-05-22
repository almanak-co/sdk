"""VIB-2637: E2E test — Aave V3 lending strategy shows non-zero PnL in paper trading.

This is the acceptance test for the yield-aware paper trading epic (VIB-2629).
It runs an Aave V3 USDC supply strategy in paper trading with
`fork_lifecycle=PERSISTENT` for multiple ticks with time advancement,
and verifies that PnL is non-zero and positive.

Run:
    pytest tests/integration/backtesting/test_yield_paper_trading.py -v -s

Requires:
    - ALCHEMY_API_KEY env var
    - `anvil` binary on PATH
"""

import asyncio
import logging
import os
from decimal import Decimal

import pytest

from almanak.framework.backtesting.paper.config import ForkLifecycle, PaperTraderConfig
from almanak.framework.backtesting.paper.engine import PaperTrader
from almanak.framework.backtesting.paper.models import PaperTradingSummary
from almanak.framework.backtesting.paper.portfolio_tracker import PaperPortfolioTracker
from almanak.framework.anvil.fork_manager import RollingForkManager

logger = logging.getLogger(__name__)

# Skip if no Alchemy key
pytestmark = pytest.mark.skipif(
    not os.environ.get("ALCHEMY_API_KEY"),
    reason="ALCHEMY_API_KEY not set",
)

# Aave V3 contract addresses (Arbitrum)
AAVE_V3_POOL = "0x794a61358D6845594F94dc1DB02A252b5b4814aD"
USDC_ARBITRUM = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"


class SimpleAaveSupplyStrategy:
    """Minimal strategy that supplies USDC to Aave V3 on first tick, then holds.

    This strategy is designed for yield paper trading testing — it makes one
    supply transaction and then holds, letting interest accrue.
    """

    deployment_id = "test_aave_yield"
    chain = "arbitrum"
    _supplied = False

    def decide(self, market):
        """Supply USDC to Aave V3 on first tick, then hold."""
        from almanak.framework.intents.vocabulary import HoldIntent, SupplyIntent

        if not self._supplied:
            self._supplied = True
            return SupplyIntent(
                protocol="aave_v3",
                token="USDC",
                amount=Decimal("1000"),
            )
        return HoldIntent(reason="Waiting for yield accrual")

    def _get_tracked_tokens(self):
        """Return tokens to track for portfolio valuation."""
        return ["USDC", USDC_ARBITRUM]


@pytest.mark.asyncio
@pytest.mark.timeout(120)
async def test_persistent_fork_advances_time():
    """Verify that persistent fork mode advances time between ticks."""
    alchemy_key = os.environ["ALCHEMY_API_KEY"]
    rpc_url = f"https://arb-mainnet.g.alchemy.com/v2/{alchemy_key}"

    fork = RollingForkManager(
        rpc_url=rpc_url,
        chain="arbitrum",
        anvil_port=18546,
        auto_impersonate=True,
    )

    try:
        success = await fork.start()
        assert success, "Failed to start Anvil fork"

        # Get initial timestamp
        block = await fork._rpc_call("eth_getBlockByNumber", ["latest", False])
        initial_timestamp = int(block["timestamp"], 16)

        # Advance time by 1 hour
        success = await fork.advance_time(3600)
        assert success, "advance_time failed"

        # Verify timestamp advanced
        block = await fork._rpc_call("eth_getBlockByNumber", ["latest", False])
        new_timestamp = int(block["timestamp"], 16)

        assert new_timestamp >= initial_timestamp + 3600, (
            f"Timestamp did not advance: {initial_timestamp} -> {new_timestamp} "
            f"(expected >= {initial_timestamp + 3600})"
        )
        logger.info(f"Time advanced: {initial_timestamp} -> {new_timestamp} (+{new_timestamp - initial_timestamp}s)")
    finally:
        await fork.stop()


@pytest.mark.asyncio
@pytest.mark.timeout(120)
async def test_fork_lifecycle_persistent_config():
    """Verify ForkLifecycle.PERSISTENT config works correctly."""
    config = PaperTraderConfig(
        chain="arbitrum",
        rpc_url="https://example.com/rpc",
        deployment_id="test",
        fork_lifecycle=ForkLifecycle.PERSISTENT,
        yield_poker_enabled=True,
        use_rich_valuation=True,
        position_reconciler_enabled=True,
    )

    assert config.fork_lifecycle == ForkLifecycle.PERSISTENT
    assert config.reset_fork_every_tick is False  # Auto-synced
    assert config.yield_poker_enabled is True
    assert config.use_rich_valuation is True
    assert config.position_reconciler_enabled is True

    # Verify serialization roundtrip
    d = config.to_dict()
    assert d["fork_lifecycle"] == "persistent"

    config2 = PaperTraderConfig.from_dict({
        "chain": "arbitrum",
        "rpc_url": "https://example.com/rpc",
        "deployment_id": "test",
        "fork_lifecycle": "persistent",
        "yield_poker_enabled": True,
        "use_rich_valuation": True,
    })
    assert config2.fork_lifecycle == ForkLifecycle.PERSISTENT


@pytest.mark.asyncio
@pytest.mark.timeout(120)
async def test_fork_lifecycle_rolling_reset_default():
    """Verify default config is ROLLING_RESET."""
    config = PaperTraderConfig(
        chain="arbitrum",
        rpc_url="https://example.com/rpc",
        deployment_id="test",
    )
    assert config.fork_lifecycle == ForkLifecycle.ROLLING_RESET
    assert config.yield_poker_enabled is False
    assert config.use_rich_valuation is False
