"""Unit tests for ``_collect_open_positions`` and ``_open_event_payload``.

VIB-4086 / VIB-4085 — these helpers project rows from
``state_manager.get_position_events_sync`` into the runner's
``_recent_open_events`` cache. The grouping rule is:

* OPEN sets the cache row.
* CLOSE pops it.
* All other event types (INCREASE / DECREASE / SNAPSHOT / COLLECT_FEES)
  are ignored by hydration — only OPEN/CLOSE matter for cache state.

These tests pin the contract so future refactors don't regress.
"""

from __future__ import annotations

from almanak.framework.runner._run_loop_helpers import (
    _collect_open_positions,
    _open_event_payload,
)


def _ev(**overrides):
    base = {
        "position_id": "lending:arb:aave_v3:0xabc:usdc",
        "position_type": "LENDING_COLLATERAL",
        "event_type": "OPEN",
        "value_usd": "100",
        "ledger_entry_id": "led-1",
        "timestamp": "2026-05-06T00:00:00Z",
        "tick_lower": None,
        "tick_upper": None,
        "liquidity": None,
        "token0": "",
        "token1": "",
    }
    base.update(overrides)
    return base


class TestOpenEventPayload:
    def test_string_fields_stringify(self):
        ev = _ev(value_usd=100, ledger_entry_id=42, liquidity=1000)
        out = _open_event_payload(ev)
        assert out["value_usd"] == "100"
        assert out["ledger_entry_id"] == "42"
        assert out["liquidity"] == "1000"

    def test_none_string_fields_become_empty(self):
        ev = _ev(value_usd=None, ledger_entry_id=None, liquidity=None, token0=None)
        out = _open_event_payload(ev)
        assert out["value_usd"] == ""
        assert out["ledger_entry_id"] == ""
        assert out["liquidity"] == ""
        assert out["token0"] == ""

    def test_tick_fields_preserve_int_or_none(self):
        ev = _ev(tick_lower=-100, tick_upper=200)
        out = _open_event_payload(ev)
        assert out["tick_lower"] == -100
        assert out["tick_upper"] == 200

    def test_tick_fields_preserve_none(self):
        ev = _ev(tick_lower=None, tick_upper=None)
        out = _open_event_payload(ev)
        assert out["tick_lower"] is None
        assert out["tick_upper"] is None


class TestCollectOpenPositions:
    def test_empty_events(self):
        assert _collect_open_positions([]) == {}

    def test_single_open_lands_in_map(self):
        out = _collect_open_positions([_ev(value_usd="500")])
        key = ("lending:arb:aave_v3:0xabc:usdc", "LENDING_COLLATERAL")
        assert key in out
        assert out[key]["value_usd"] == "500"

    def test_open_then_close_pops(self):
        out = _collect_open_positions(
            [
                _ev(event_type="OPEN", value_usd="500"),
                _ev(event_type="CLOSE", value_usd="0"),
            ]
        )
        assert out == {}

    def test_close_then_open_keeps_open(self):
        # Operator restart sequence: prior CLOSE on disk, fresh OPEN.
        out = _collect_open_positions(
            [
                _ev(event_type="CLOSE"),
                _ev(event_type="OPEN", value_usd="700", ledger_entry_id="led-2"),
            ]
        )
        key = ("lending:arb:aave_v3:0xabc:usdc", "LENDING_COLLATERAL")
        assert out[key]["value_usd"] == "700"
        assert out[key]["ledger_entry_id"] == "led-2"

    def test_increase_event_ignored(self):
        # INCREASE is a position-state event but shouldn't disturb the OPEN row.
        out = _collect_open_positions(
            [
                _ev(event_type="OPEN", value_usd="500", ledger_entry_id="led-1"),
                _ev(event_type="INCREASE", value_usd="900", ledger_entry_id="led-99"),
            ]
        )
        key = ("lending:arb:aave_v3:0xabc:usdc", "LENDING_COLLATERAL")
        # OPEN row preserved; INCREASE ignored.
        assert out[key]["value_usd"] == "500"
        assert out[key]["ledger_entry_id"] == "led-1"

    def test_missing_position_id_skipped(self):
        out = _collect_open_positions([_ev(position_id="")])
        assert out == {}

    def test_missing_position_type_skipped(self):
        out = _collect_open_positions([_ev(position_type="")])
        assert out == {}

    def test_event_type_case_insensitive(self):
        out = _collect_open_positions([_ev(event_type="open")])
        assert ("lending:arb:aave_v3:0xabc:usdc", "LENDING_COLLATERAL") in out

    def test_distinct_position_ids_kept_separate(self):
        out = _collect_open_positions(
            [
                _ev(position_id="A", value_usd="100"),
                _ev(position_id="B", value_usd="200"),
            ]
        )
        assert out[("A", "LENDING_COLLATERAL")]["value_usd"] == "100"
        assert out[("B", "LENDING_COLLATERAL")]["value_usd"] == "200"

    def test_close_only_one_branch(self):
        # Two positions; close one. The other survives.
        out = _collect_open_positions(
            [
                _ev(position_id="A", event_type="OPEN", value_usd="10"),
                _ev(position_id="B", event_type="OPEN", value_usd="20"),
                _ev(position_id="A", event_type="CLOSE"),
            ]
        )
        assert ("A", "LENDING_COLLATERAL") not in out
        assert out[("B", "LENDING_COLLATERAL")]["value_usd"] == "20"

    def test_lp_open_preserves_tick_fields(self):
        out = _collect_open_positions(
            [
                _ev(
                    position_id="lp:eth:univ3:0x1:0x2",
                    position_type="LP",
                    event_type="OPEN",
                    tick_lower=-1000,
                    tick_upper=1000,
                    liquidity="123456",
                    token0="WETH",
                    token1="USDC",
                ),
            ]
        )
        key = ("lp:eth:univ3:0x1:0x2", "LP")
        payload = out[key]
        assert payload["tick_lower"] == -1000
        assert payload["tick_upper"] == 1000
        assert payload["liquidity"] == "123456"
        assert payload["token0"] == "WETH"
        assert payload["token1"] == "USDC"
