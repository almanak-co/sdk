"""Pendle LP / PT branch in ``_compute_outbox_position_key`` routes via the registry (VIB-4931).

The runner's two Pendle position-key branches were replaced by a call to
``AccountingTreatmentRegistry.position_key_for`` (the Pendle connector publishes the
derivation in its ``accounting_spec``). These tests pin that the runner still produces
byte-identical ``(position_key, market_id)`` for Pendle LP / PT intents, and that a
non-Pendle event falls through to the runner's generic derivation.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from almanak.framework.runner.strategy_runner import StrategyRunner


def _call(intent: Any, intent_type: str, chain: str = "arbitrum", wallet: str = "0xWallet") -> tuple[str, str]:
    # _compute_outbox_position_key only reads `self` in its except branch (logger),
    # so the happy path can be exercised with a stand-in self (cf. test_bridge_position_key).
    return StrategyRunner._compute_outbox_position_key(
        SimpleNamespace(),  # type: ignore[arg-type]
        intent,
        intent_type,
        chain,
        wallet,
    )


def test_pendle_lp_open_position_key():
    # pool "TOKEN/0xMarket" → market parsed + lowercased; key = pendle_lp:chain:wallet:market.
    intent = SimpleNamespace(protocol="pendle_v2", pool="WETH/0xMarketAddr")
    position_key, market_id = _call(intent, "LP_OPEN")
    assert position_key == "pendle_lp:arbitrum:0xwallet:0xmarketaddr"
    assert market_id == "0xmarketaddr"


def test_pendle_lp_close_position_key_bare_address():
    intent = SimpleNamespace(protocol="pendle_v2", pool="0xMarketAddr")
    position_key, market_id = _call(intent, "LP_CLOSE")
    assert position_key == "pendle_lp:arbitrum:0xwallet:0xmarketaddr"
    assert market_id == "0xmarketaddr"


def test_pendle_pt_swap_position_key():
    intent = SimpleNamespace(protocol="pendle_v2", pool="0xMarketAddr")
    position_key, market_id = _call(intent, "SWAP")
    assert position_key == "pendle_pt:arbitrum:0xwallet:0xmarketaddr"
    assert market_id == "0xmarketaddr"


def test_pendle_lp_no_market_returns_empty():
    intent = SimpleNamespace(protocol="pendle_v2", pool=None)
    position_key, market_id = _call(intent, "LP_OPEN")
    assert position_key == "" and market_id == ""


def test_pendle_lp_collect_fees_stays_connector_owned_without_generic_fallback():
    # The connector claims Pendle LP_COLLECT_FEES accounting but intentionally emits
    # no event/key today; the runner must not synthesize a generic lp: key.
    intent = SimpleNamespace(protocol="pendle_v2", pool="0xMarketAddr")
    position_key, market_id = _call(intent, "LP_COLLECT_FEES")
    assert position_key == "" and market_id == ""


def test_non_pendle_swap_falls_through_to_generic():
    # The registry declines a non-Pendle event, so the runner's generic SWAP branch runs.
    intent = SimpleNamespace(protocol="uniswap_v3", pool="0xPool")
    position_key, market_id = _call(intent, "SWAP")
    assert position_key == "swap:arbitrum:0xwallet"
    assert market_id == ""
