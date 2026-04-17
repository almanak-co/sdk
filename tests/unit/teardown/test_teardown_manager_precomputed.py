"""Tests for TeardownManager precomputed positions/intents path.

Regression for the PR audit of Bug 2B (0G DogFooding report, 2026-04-16):
the CLI's ``--discover`` flow builds LPCloseIntents from on-chain
discovered positions, but an earlier revision left
``TeardownManager.execute()`` re-reading ``strategy.get_open_positions()``
and ``strategy.generate_teardown_intents()`` internally — so the manager
would run with 0 intents and "successfully" close nothing while the
operator's orphaned NFTs remained open. These tests lock in the
precomputed-* parameters that thread discovered data straight through to
the executor.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.teardown.models import (
    PositionInfo,
    PositionType,
    TeardownPositionSummary,
)
from almanak.framework.teardown.teardown_manager import TeardownManager


def _make_strategy_with_no_local_state() -> MagicMock:
    """Strategy whose get_open_positions / generate_teardown_intents we
    deliberately never expect to be called (simulating gateway-restart
    recovery where the strategy forgot its positions)."""
    strategy = MagicMock()
    strategy.strategy_id = "exp12_jaine_wbtc_w0g"
    strategy.name = "JAINE LP"
    strategy.chain = "zerog"
    strategy.uses_safe_wallet = False
    strategy.pause = AsyncMock()
    strategy.get_open_positions.side_effect = AssertionError(
        "get_open_positions must NOT be called on the --discover path"
    )
    strategy.generate_teardown_intents.side_effect = AssertionError(
        "generate_teardown_intents must NOT be called on the --discover path"
    )
    return strategy


def _make_positions() -> TeardownPositionSummary:
    import datetime as dt

    info = PositionInfo(
        position_type=PositionType.LP,
        position_id="2359",
        chain="zerog",
        protocol="uniswap_v3",
        value_usd=Decimal("0"),
        details={"discovered_on_chain": True},
    )
    return TeardownPositionSummary(
        strategy_id="exp12_jaine_wbtc_w0g",
        timestamp=dt.datetime.now(dt.UTC),
        positions=[info],
    )


@pytest.mark.asyncio
async def test_precomputed_intents_bypass_strategy_queries():
    """When precomputed_positions / precomputed_intents are supplied, the
    manager skips strategy.get_open_positions and strategy.generate_teardown_intents
    and executes the supplied intents directly. This is the critical path
    the --discover CLI flag depends on."""
    strategy = _make_strategy_with_no_local_state()
    positions = _make_positions()

    # Build a minimal LP close intent stand-in
    intent = MagicMock()
    intent.intent_type = "LP_CLOSE"
    intent.chain = "zerog"
    intent.to_dict.return_value = {"type": "lp_close", "position_id": "2359"}
    del intent.max_slippage

    compiler = MagicMock()
    compiler.price_oracle = None
    compiler._using_placeholders = True

    def _update_prices(prices):
        compiler.price_oracle = prices
        compiler._using_placeholders = False

    def _restore_prices(oracle, placeholders):
        compiler.price_oracle = oracle
        compiler._using_placeholders = placeholders

    compiler.update_prices = _update_prices
    compiler.restore_prices = _restore_prices

    def _compile(_intent):
        result = MagicMock()
        result.status.value = "SUCCESS"
        result.action_bundle = MagicMock()
        return result

    compiler.compile = _compile

    orchestrator = MagicMock()
    orchestrator.execute = AsyncMock(
        return_value=MagicMock(success=True, transaction_results=[], total_gas_used=50_000)
    )

    manager = TeardownManager(orchestrator=orchestrator, compiler=compiler)
    manager.cancel_window.run_cancel_window = AsyncMock(return_value=MagicMock(was_cancelled=False))
    manager.safety_guard.validate_teardown_request = MagicMock(return_value=MagicMock(all_passed=True))
    manager._verify_closure = AsyncMock(return_value=True)

    result = await manager.execute(
        strategy=strategy,
        mode="graceful",
        precomputed_positions=positions,
        precomputed_intents=[intent],
    )

    # Strategy methods were NEVER invoked (side_effects would have raised)
    strategy.get_open_positions.assert_not_called()
    strategy.generate_teardown_intents.assert_not_called()

    # Orchestrator received the discovered intent
    assert orchestrator.execute.await_count >= 1

    # Result reflects the executed intent
    assert result.intents_total == 1


@pytest.mark.asyncio
async def test_without_precomputed_queries_strategy():
    """Default behaviour is preserved: when precomputed_* are omitted, the
    manager still calls get_open_positions and generate_teardown_intents."""
    strategy = MagicMock()
    strategy.strategy_id = "normal_strat"
    strategy.name = "Normal"
    strategy.chain = "arbitrum"
    strategy.uses_safe_wallet = False
    strategy.pause = AsyncMock()
    positions = TeardownPositionSummary(
        strategy_id="normal_strat",
        timestamp=__import__("datetime").datetime.now(__import__("datetime").UTC),
        positions=[],
    )
    strategy.get_open_positions.return_value = positions
    strategy.generate_teardown_intents.return_value = []

    manager = TeardownManager()

    await manager.execute(strategy=strategy, mode="graceful")

    strategy.get_open_positions.assert_called_once()
    strategy.generate_teardown_intents.assert_called_once()
