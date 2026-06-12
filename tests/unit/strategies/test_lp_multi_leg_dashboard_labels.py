"""VIB-5073 — dashboards must never synthesize a phantom ``leg_<N>`` handle.

Field evidence (mainnet lp_dual on Base, ``deployment:78fc633158d7``): after
the rebalance machine closed ``leg_narrow`` (tokenId 5316978) and reopened it
as tokenId 5317095, the Position Status header and the Liquidity Distribution
legend labeled the new position ``leg_3`` — a handle that exists nowhere in
the two-leg strategy config — while the Trade Tape and the position registry
correctly said ``leg_narrow``. Root cause: ``_dual_positions_from_events``
derived the label from the position ORDINAL (3rd distinct ``position_id`` →
``f"leg_{idx}"``) instead of the registry handle carried on the accounting
events / registry row.

These tests pin the fix for both lp_dual and lp_triple (same bug class):

* a rebalanced position (same registry handle, new tokenId) renders with the
  strategy's actual handle, not an ordinal;
* a position with no resolvable handle carries NO ``registry_handle`` key —
  the template then falls back to clearly-non-handle labels (the tokenId /
  ``Position N``) — and never a synthesized ``leg_<N>``.
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any

from strategies.accounting.lp_dual.dashboard import ui as dual_ui
from strategies.accounting.lp_triple.dashboard import ui as triple_ui

_CONFIG = dual_ui.LPDashboardConfig(
    protocol="uniswap_v3",
    token0="WETH",
    token1="USDC",
    fee_tier="0.05%",
    chain="base",
)


def _event(pid: str, event_type: str, ts: str) -> dict[str, Any]:
    return {
        "position_id": pid,
        "event_type": event_type,
        "timestamp": ts,
        "tick_lower": -887220,
        "tick_upper": 887220,
        "in_range": True,
        "amount0": "0",
        "amount1": "0",
        "value_usd": "100",
        "protocol": "uniswap_v3",
        "chain": "base",
    }


def _rebalance_events() -> list[dict[str, Any]]:
    """leg_narrow opened (5316978), closed, reopened as 5317095; leg_wide open."""
    return [
        _event("5316978", "OPEN", "2026-06-11T01:00:00"),
        _event("5316979", "OPEN", "2026-06-11T01:00:05"),
        _event("5316978", "CLOSE", "2026-06-11T02:00:00"),
        _event("5317095", "OPEN", "2026-06-11T02:00:30"),
    ]


def _tape_client(handle_by_pid: dict[str, str]) -> SimpleNamespace:
    rows = [
        SimpleNamespace(
            position_id=pid,
            accounting_payload_json=json.dumps({"position_reference": {"registry_handle": handle}}),
        )
        for pid, handle in handle_by_pid.items()
    ]
    return SimpleNamespace(get_trade_tape=lambda limit=50: SimpleNamespace(rows=rows, has_more=False))


class TestLPDualLegLabels:
    def test_rebalanced_leg_renders_registry_handle_not_ordinal(self) -> None:
        handles = {
            "5316978": "leg_narrow",
            "5316979": "leg_wide",
            "5317095": "leg_narrow",
        }
        positions = dual_ui._dual_positions_from_events(_rebalance_events(), _CONFIG, handles)
        by_pid = {p["position_id"]: p for p in positions}

        assert by_pid["5317095"]["registry_handle"] == "leg_narrow"
        assert by_pid["5317095"]["is_active"] is True
        assert by_pid["5316978"]["registry_handle"] == "leg_narrow"
        assert by_pid["5316978"]["is_active"] is False
        assert by_pid["5316979"]["registry_handle"] == "leg_wide"
        # The phantom: no position may ever carry an ordinal-synthesized name.
        labels = {p.get("registry_handle") for p in positions}
        assert "leg_3" not in labels
        assert "leg_4" not in labels

    def test_no_handle_omits_key_never_synthesizes(self) -> None:
        positions = dual_ui._dual_positions_from_events(_rebalance_events(), _CONFIG, {})
        assert positions  # events still render
        for pos in positions:
            assert "registry_handle" not in pos

    def test_leg_handles_state_slots_resolve_current_token_ids(self) -> None:
        """No trade tape (api_client unavailable) → live state slots still map
        the CURRENT tokenIds to the strategy's stamped handles."""
        state = {"position_id_1": "5317095", "position_id_2": "5316979"}
        handles = dual_ui._leg_handles(None, state)
        assert handles == {"5317095": "leg_narrow", "5316979": "leg_wide"}

    def test_leg_handles_trade_tape_wins_and_covers_closed_token_ids(self) -> None:
        state = {"position_id_1": "5317095", "position_id_2": "5316979"}
        api_client = _tape_client(
            {
                "5316978": "leg_narrow",  # closed tokenId — only the tape knows it
                "5317095": "leg_narrow",
                "5316979": "leg_wide",
            }
        )
        handles = dual_ui._leg_handles(api_client, state)
        assert handles == {
            "5316978": "leg_narrow",
            "5317095": "leg_narrow",
            "5316979": "leg_wide",
        }

    def test_leg_handles_empty_state_slots_are_skipped(self) -> None:
        assert dual_ui._leg_handles(None, {"position_id_1": "", "position_id_2": None}) == {}


class TestLPTripleLegLabels:
    def test_rebalanced_leg_renders_registry_handle_not_ordinal(self) -> None:
        events = _rebalance_events() + [_event("5316980", "OPEN", "2026-06-11T01:00:10")]
        handles = {
            "5316978": "leg_narrow",
            "5316979": "leg_mid",
            "5316980": "leg_wide",
            "5317095": "leg_narrow",
        }
        positions = triple_ui._triple_positions_from_events(events, _CONFIG, handles)
        by_pid = {p["position_id"]: p for p in positions}

        assert by_pid["5317095"]["registry_handle"] == "leg_narrow"
        labels = {p.get("registry_handle") for p in positions}
        assert not any(label in labels for label in ("leg_3", "leg_4", "leg_5"))

    def test_no_handle_omits_key_never_synthesizes(self) -> None:
        positions = triple_ui._triple_positions_from_events(_rebalance_events(), _CONFIG, {})
        assert positions
        for pos in positions:
            assert "registry_handle" not in pos

    def test_leg_handles_state_slots_resolve_current_token_ids(self) -> None:
        state = {"position_id_a": "1", "position_id_b": "2", "position_id_c": "3"}
        assert triple_ui._leg_handles(None, state) == {
            "1": "leg_narrow",
            "2": "leg_mid",
            "3": "leg_wide",
        }
