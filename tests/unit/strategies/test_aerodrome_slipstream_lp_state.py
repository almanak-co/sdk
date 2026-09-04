"""Restart-safe physical identity for the Slipstream demo strategy."""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from almanak.connectors.aerodrome.addresses import slipstream_lp_deployments
from almanak.framework.teardown import TeardownMode
from strategies.internal.demo_catalog.aerodrome_slipstream_lp import strategy as strategy_module
from strategies.internal.demo_catalog.aerodrome_slipstream_lp.strategy import AerodromeSlipstreamLPStrategy

EXACT_POOL = "0x3FE04A59Ebd38cF06080a6F60a98D124eb59392A"


def _strategy() -> AerodromeSlipstreamLPStrategy:
    strategy = AerodromeSlipstreamLPStrategy.__new__(AerodromeSlipstreamLPStrategy)
    strategy._chain = "base"
    strategy._deployment_id = "deployment:test"
    strategy._has_position = False
    strategy._position_token_id = ""
    strategy._position_manager = ""
    strategy._range_lower = None
    strategy._range_upper = None
    strategy._pending_range_lower = Decimal("0.9")
    strategy._pending_range_upper = Decimal("1.1")
    strategy.pool_address = EXACT_POOL
    strategy.range_percent = Decimal("0.2")
    strategy.range_lower_price = Decimal("0")
    strategy.range_upper_price = Decimal("0")
    strategy.pool = "WETH/USDC"
    strategy.tick_spacing = 50
    strategy.token0_symbol = "WETH"
    strategy.token1_symbol = "USDC"
    strategy.amount0 = Decimal("0.001")
    strategy.amount1 = Decimal("3")
    return strategy


def _mint_result(emitter: str) -> SimpleNamespace:
    """An execution result whose mint receipt was emitted by ``emitter``."""
    receipt = SimpleNamespace(logs=[{"address": emitter, "topics": []}])
    return SimpleNamespace(position_id=42, transaction_results=[SimpleNamespace(receipt=receipt)])


@pytest.mark.parametrize("generation", ["current", "legacy"])
def test_lp_open_persists_the_minting_manager_and_restores_teardown_identity(monkeypatch, generation) -> None:
    """The manager is whichever reviewed NPM emitted the mint, never a registry default."""
    monkeypatch.setattr(strategy_module, "add_event", MagicMock())
    strategy = _strategy()
    intent = SimpleNamespace(intent_type=SimpleNamespace(value="LP_OPEN"))
    deployment = next(d for d in slipstream_lp_deployments("base") if d.generation == generation)

    strategy.on_intent_executed(intent, success=True, result=_mint_result(deployment.position_manager.lower()))

    state = strategy.get_persistent_state()
    assert state["position_manager"] == deployment.position_manager

    restarted = _strategy()
    restarted.load_persistent_state(state)
    position = restarted.get_open_positions().positions[0]
    assert position.position_id == "42"
    assert position.details["nft_manager_addr"] == deployment.position_manager


def test_configured_exact_pool_is_reused_for_open_close_and_teardown() -> None:
    strategy = _strategy()
    strategy._has_position = True
    strategy._position_token_id = "42"

    open_intent = strategy._create_open_intent(Decimal("3000"))
    close_intent = strategy._create_close_intent()
    teardown_intents = strategy.generate_teardown_intents(TeardownMode.SOFT)

    assert open_intent.pool == EXACT_POOL
    assert close_intent.pool == EXACT_POOL
    assert len(teardown_intents) == 1
    assert teardown_intents[0].pool == EXACT_POOL


def test_lp_open_without_a_reviewed_emitter_persists_no_manager(monkeypatch) -> None:
    monkeypatch.setattr(strategy_module, "add_event", MagicMock())
    strategy = _strategy()
    intent = SimpleNamespace(intent_type=SimpleNamespace(value="LP_OPEN"))

    strategy.on_intent_executed(intent, success=True, result=_mint_result("0x" + "77" * 20))

    assert strategy._position_token_id == "42"
    assert strategy._position_manager == ""
    assert "position_manager" not in strategy.get_persistent_state()


def test_load_rejects_unreviewed_manager_from_persistent_state() -> None:
    strategy = _strategy()

    strategy.load_persistent_state(
        {
            "has_position": True,
            "position_token_id": "42",
            "position_manager": "0x" + "99" * 20,
        }
    )

    assert strategy._position_manager == ""
    assert "nft_manager_addr" not in strategy.get_open_positions().positions[0].details
