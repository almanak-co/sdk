"""Restart-safe physical identity for the Slipstream demo strategy."""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

from almanak.connectors.aerodrome.addresses import slipstream_lp_deployments
from strategies.internal.demo_catalog.aerodrome_slipstream_lp import strategy as strategy_module
from strategies.internal.demo_catalog.aerodrome_slipstream_lp.strategy import AerodromeSlipstreamLPStrategy


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
    strategy.pool = "WETH/USDC"
    strategy.tick_spacing = 50
    strategy.token0_symbol = "WETH"
    strategy.token1_symbol = "USDC"
    strategy.amount0 = Decimal("0.001")
    strategy.amount1 = Decimal("3")
    return strategy


def test_lp_open_persists_current_manager_and_restores_teardown_identity(monkeypatch) -> None:
    monkeypatch.setattr(strategy_module, "add_event", MagicMock())
    strategy = _strategy()
    intent = SimpleNamespace(intent_type=SimpleNamespace(value="LP_OPEN"))

    strategy.on_intent_executed(intent, success=True, result=SimpleNamespace(position_id=42))

    current_manager = slipstream_lp_deployments("base")[0].position_manager
    state = strategy.get_persistent_state()
    assert state["position_manager"] == current_manager

    restarted = _strategy()
    restarted.load_persistent_state(state)
    position = restarted.get_open_positions().positions[0]
    assert position.position_id == "42"
    assert position.details["nft_manager_addr"] == current_manager


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
