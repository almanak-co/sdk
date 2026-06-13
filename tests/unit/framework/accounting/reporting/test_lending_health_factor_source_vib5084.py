"""VIB-5084 — `strat pnl` must not render a stale per-event health factor.

The lending report set `summary.health_factor` from the last lending
accounting EVENT's `health_factor_after`, which freezes during a quiet hold
(events only exist when txs happen). The fix prefers the live per-iteration
Track-C `position_state_snapshots.health_factor` (VIB-5006) and, when only the
event value is available, renders it with an `(as of <ts>)` qualifier.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from almanak.framework.accounting.models import (
    AccountingConfidence,
    AccountingIdentity,
    LendingAccountingEvent,
    LendingEventType,
)
from almanak.framework.accounting.reporting.lending_report import (
    _latest_track_c_hf,
    build_lending_report,
)
from almanak.framework.accounting.reporting.loader import AccountingData
from almanak.framework.accounting.reporting.render_json import lending_section_to_dict
from almanak.framework.accounting.reporting.render_text import _hf_line, render_lending_section

DEPLOYMENT_ID = "test:hf-source"
_EVENT_TS = datetime(2026, 6, 13, 7, 0, 0, tzinfo=UTC)


def _identity() -> AccountingIdentity:
    return AccountingIdentity(
        id="evt-001",
        deployment_id=DEPLOYMENT_ID,
        cycle_id="cycle-001",
        execution_mode="live",
        timestamp=_EVENT_TS,
        chain="arbitrum",
        protocol="aave_v3",
        wallet_address="0xdeadbeef",
        tx_hash="0xabc123",
        ledger_entry_id="led-001",
    )


def _borrow_event(hf_after: Decimal | None) -> LendingAccountingEvent:
    return LendingAccountingEvent(
        identity=_identity(),
        event_type=LendingEventType.BORROW,
        position_key="aave_v3:BORROW:USDT:arbitrum",
        market_id="0xaave_market",
        asset="USDT",
        collateral_value_before_usd=Decimal("0"),
        collateral_value_after_usd=Decimal("0"),
        debt_value_before_usd=Decimal("0"),
        debt_value_after_usd=Decimal("1.2"),
        net_equity_before_usd=Decimal("0"),
        net_equity_after_usd=Decimal("-1.2"),
        health_factor_before=None,
        health_factor_after=hf_after,
        liquidation_threshold=None,
        lltv=None,
        supply_apr_bps=None,
        borrow_apr_bps=None,
        principal_delta_usd=Decimal("1.2"),
        interest_delta_usd=None,
        gas_usd=Decimal("0.02"),
        confidence=AccountingConfidence.HIGH,
    )


def _track_c_row(
    *,
    hf: str | None,
    position_id: str = "aave_v3:arbitrum:aave_v3 BORROW",
    position_type: str = "LENDING",
    captured_at: str = "2026-06-13T07:30:00+00:00",
    snapshot_id: int = 2,
) -> dict:
    return {
        "position_type": position_type,
        "position_id": position_id,
        "health_factor": hf,
        "captured_at": captured_at,
        "snapshot_id": snapshot_id,
    }


def _data(
    events: list[LendingAccountingEvent],
    track_c: list[dict] | None = None,
) -> AccountingData:
    return AccountingData(
        deployment_id=DEPLOYMENT_ID,
        metrics=None,
        ledger_entries=[],
        position_events=[],
        snapshot=None,
        lending_events=events,
        position_state_snapshots=track_c or [],
    )


# ---------------------------------------------------------------------------
# build_lending_report — source selection
# ---------------------------------------------------------------------------


def test_track_c_hf_preferred_over_frozen_event_hf():
    """The reported bug: event froze at 3.375 while chain HF was 2.6026."""
    data = _data(
        [_borrow_event(Decimal("3.375"))],
        track_c=[_track_c_row(hf="2.6026")],
    )
    section = build_lending_report(data)
    pos = section.positions[0]
    assert pos.health_factor == Decimal("2.6026")
    assert pos.health_factor_source == "track_c"
    assert pos.health_factor_as_of == datetime(2026, 6, 13, 7, 30, 0, tzinfo=UTC)


def test_event_hf_used_with_as_of_when_no_track_c():
    data = _data([_borrow_event(Decimal("3.375"))], track_c=[])
    pos = build_lending_report(data).positions[0]
    assert pos.health_factor == Decimal("3.375")
    assert pos.health_factor_source == "event"
    assert pos.health_factor_as_of == _EVENT_TS


def test_no_hf_anywhere_is_none():
    data = _data([_borrow_event(None)], track_c=[])
    pos = build_lending_report(data).positions[0]
    assert pos.health_factor is None
    assert pos.health_factor_source == ""
    assert pos.health_factor_as_of is None


def test_track_c_does_not_override_closed_position():
    """A closed position's HF is historical — the live read (no debt ⇒ ~inf)
    must not overwrite it."""
    ev = _borrow_event(Decimal("3.375"))
    ev.event_type = LendingEventType.CLOSE  # marks summary closed
    data = _data([ev], track_c=[_track_c_row(hf="2.6026")])
    pos = build_lending_report(data).positions[0]
    assert pos.is_closed
    # Track-C preference is gated to open positions; the event value stands.
    assert pos.health_factor_source != "track_c"


def test_track_c_wrong_protocol_chain_not_applied():
    """A Track-C row for a different (protocol, chain) must not match."""
    data = _data(
        [_borrow_event(Decimal("3.375"))],
        track_c=[_track_c_row(hf="2.60", position_id="morpho_blue:ethereum:x")],
    )
    pos = build_lending_report(data).positions[0]
    assert pos.health_factor == Decimal("3.375")  # event value, no match
    assert pos.health_factor_source == "event"


# ---------------------------------------------------------------------------
# _latest_track_c_hf — parsing / dedup / filtering
# ---------------------------------------------------------------------------


def test_latest_track_c_hf_newest_first_dedup():
    rows = [
        _track_c_row(hf="2.60", captured_at="2026-06-13T07:30:00+00:00"),  # latest (DESC)
        _track_c_row(hf="3.37", captured_at="2026-06-13T07:00:00+00:00"),  # older, same key
    ]
    out = _latest_track_c_hf(rows)
    hf, captured = out[("aave_v3", "arbitrum")]
    assert hf == Decimal("2.60")  # first (latest) wins
    assert captured == datetime(2026, 6, 13, 7, 30, 0, tzinfo=UTC)


def test_latest_track_c_hf_scopes_to_newest_snapshot_id():
    """HF is taken from the newest snapshot_id, not bled across snapshots — even
    if an older snapshot's row sorts first in the passed list (the bounded
    row-count window can't mix snapshots)."""
    rows = [
        # Older snapshot listed FIRST (e.g. a clipped/again-ordered window).
        _track_c_row(hf="3.37", snapshot_id=1, captured_at="2026-06-13T07:00:00+00:00"),
        # Newer snapshot.
        _track_c_row(hf="2.60", snapshot_id=2, captured_at="2026-06-13T07:30:00+00:00"),
    ]
    out = _latest_track_c_hf(rows)
    hf, _ = out[("aave_v3", "arbitrum")]
    assert hf == Decimal("2.60")  # newest snapshot_id wins, regardless of list order


def test_latest_track_c_hf_accepts_native_datetime_captured_at():
    """A backend that returns ``captured_at`` as a native datetime (not an ISO
    string) is accepted directly (Gemini)."""
    dt = datetime(2026, 6, 13, 7, 30, 0, tzinfo=UTC)
    row = _track_c_row(hf="2.60")
    row["captured_at"] = dt  # native datetime instead of ISO string
    out = _latest_track_c_hf([row])
    assert out[("aave_v3", "arbitrum")] == (Decimal("2.60"), dt)


def test_latest_track_c_hf_skips_non_lending_and_null_and_malformed():
    rows = [
        _track_c_row(hf="2.60", position_type="LP"),  # not lending
        _track_c_row(hf=None),  # null HF (Empty ≠ Zero)
        _track_c_row(hf="", position_id="aave_v3:arbitrum:x"),  # empty HF
        _track_c_row(hf="1.5", position_id="noseparator"),  # malformed id
        _track_c_row(hf="2.99"),  # valid → the only entry
    ]
    out = _latest_track_c_hf(rows)
    assert out == {("aave_v3", "arbitrum"): (Decimal("2.99"), datetime(2026, 6, 13, 7, 30, 0, tzinfo=UTC))}


# ---------------------------------------------------------------------------
# render — text + json
# ---------------------------------------------------------------------------


def test_render_text_event_hf_shows_as_of():
    data = _data([_borrow_event(Decimal("3.375"))], track_c=[])
    text = render_lending_section(build_lending_report(data))
    assert "Health:" in text
    assert "3.375 (as of 2026-06-13 07:00 UTC)" in text


def test_render_text_track_c_hf_plain_no_as_of():
    data = _data([_borrow_event(Decimal("3.375"))], track_c=[_track_c_row(hf="2.6026")])
    text = render_lending_section(build_lending_report(data))
    assert "Health:" in text
    assert "2.603" in text  # _hf rounds to 3dp
    assert "as of" not in text  # live value renders plainly


def test_hf_line_none_is_missing_marker():
    data = _data([_borrow_event(None)], track_c=[])
    pos = build_lending_report(data).positions[0]
    assert _hf_line(pos) == "—"


def test_render_json_carries_source_and_as_of():
    data = _data([_borrow_event(Decimal("3.375"))], track_c=[_track_c_row(hf="2.6026")])
    d = lending_section_to_dict(build_lending_report(data))
    pos = d["positions"][0]
    assert pos["health_factor"] == "2.6026"
    assert pos["health_factor_source"] == "track_c"
    assert pos["health_factor_as_of"] == "2026-06-13T07:30:00+00:00"


def test_render_json_event_source_and_none_asof():
    data = _data([_borrow_event(None)], track_c=[])
    pos = lending_section_to_dict(build_lending_report(data))["positions"][0]
    assert pos["health_factor"] is None
    assert pos["health_factor_source"] == ""
    assert pos["health_factor_as_of"] is None
