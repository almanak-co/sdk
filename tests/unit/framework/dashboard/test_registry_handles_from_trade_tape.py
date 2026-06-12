"""Tests for ``registry_handles_from_trade_tape`` (VIB-5073).

The LP template's multi-position leg labels must come from the SAME
provenance the Trade Tape page surfaces — the accounting payload's
``position_reference.registry_handle`` joined to ``position_id`` on the
trade-tape row. The helper must never synthesize a handle (the field bug:
the lp_dual dashboard labeled the 3rd distinct tokenId ``leg_3`` after a
rebalance re-opened ``leg_narrow`` under a new tokenId).
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any

from almanak.framework.dashboard.templates import registry_handles_from_trade_tape


def _payload(handle: str | None) -> str:
    return json.dumps({"position_reference": {"registry_handle": handle}})


def _row(position_id: str, payload_json: str) -> SimpleNamespace:
    return SimpleNamespace(position_id=position_id, accounting_payload_json=payload_json)


class _FakeClient:
    def __init__(self, rows: list[Any]) -> None:
        self._rows = rows
        self.requested_limit: int | None = None

    def get_trade_tape(self, limit: int = 50) -> SimpleNamespace:
        self.requested_limit = limit
        return SimpleNamespace(rows=self._rows, has_more=False)


class TestRegistryHandlesFromTradeTape:
    def test_maps_position_id_to_stamped_handle(self) -> None:
        client = _FakeClient(
            [
                _row("5316978", _payload("leg_narrow")),
                _row("5316979", _payload("leg_wide")),
            ]
        )
        assert registry_handles_from_trade_tape(client) == {
            "5316978": "leg_narrow",
            "5316979": "leg_wide",
        }

    def test_rebalanced_leg_new_token_id_resolves_same_handle(self) -> None:
        """The VIB-5073 field case: leg_narrow closed (tokenId 5316978) and
        reopened (tokenId 5317095) — BOTH tokenIds map to ``leg_narrow``;
        no third name is ever invented."""
        client = _FakeClient(
            [
                # Newest-first, as the gateway sorts timestamp DESC.
                _row("5317095", _payload("leg_narrow")),  # LP_OPEN (reopen)
                _row("5316978", _payload("leg_narrow")),  # LP_CLOSE
                _row("5316979", _payload("leg_wide")),
                _row("5316978", _payload("leg_narrow")),  # LP_OPEN (original)
            ]
        )
        handles = registry_handles_from_trade_tape(client)
        assert handles == {
            "5317095": "leg_narrow",
            "5316978": "leg_narrow",
            "5316979": "leg_wide",
        }
        assert not any(h.startswith("leg_3") for h in handles.values())

    def test_newest_row_wins_per_position_id(self) -> None:
        """Rows arrive newest-first; first-seen wins so a re-bound handle
        reflects the most recent accounting write."""
        client = _FakeClient(
            [
                _row("777", _payload("leg_renamed")),
                _row("777", _payload("leg_old")),
            ]
        )
        assert registry_handles_from_trade_tape(client) == {"777": "leg_renamed"}

    def test_rows_without_handle_are_absent_not_synthesized(self) -> None:
        client = _FakeClient(
            [
                _row("111", _payload(None)),
                _row("222", json.dumps({"event_type": "LP_OPEN"})),
                _row("333", ""),
            ]
        )
        assert registry_handles_from_trade_tape(client) == {}

    def test_rows_without_position_id_are_skipped(self) -> None:
        client = _FakeClient([_row("", _payload("leg_narrow"))])
        assert registry_handles_from_trade_tape(client) == {}

    def test_malformed_payload_is_skipped(self) -> None:
        client = _FakeClient(
            [
                _row("111", "{not json"),
                _row("222", _payload("leg_wide")),
            ]
        )
        assert registry_handles_from_trade_tape(client) == {"222": "leg_wide"}

    def test_none_client_returns_empty(self) -> None:
        assert registry_handles_from_trade_tape(None) == {}

    def test_client_without_trade_tape_returns_empty(self) -> None:
        assert registry_handles_from_trade_tape(object()) == {}

    def test_raising_client_returns_empty(self) -> None:
        class _Boom:
            def get_trade_tape(self, limit: int = 50) -> SimpleNamespace:
                raise RuntimeError("gateway down")

        assert registry_handles_from_trade_tape(_Boom()) == {}

    def test_limit_is_threaded_through(self) -> None:
        client = _FakeClient([])
        registry_handles_from_trade_tape(client, limit=321)
        assert client.requested_limit == 321
