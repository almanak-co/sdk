"""VIB-5943 — ``almanak strat pnl`` classifies perp deployments as PERP, not "unknown".

``_detect_strategy_classes`` had no PERP branch: a GMX perp deployment produced
``strategy_classes=["unknown"]`` because nothing mapped the PERP position_type
(nor the PERP_OPEN / PERP_CLOSE event markers) to a class. With VIB-5941 giving
perps a real ``position_events`` row, the classifier now keys off it; the raw
event-marker branch also covers a malformed pre-VIB-5941 accounting row.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from almanak.framework.accounting.reporting.loader import (
    StrategyClass,
    _detect_strategy_classes,
    load_accounting_data,
)

# Sealed GMX perp mainnet capture (pre-VIB-5941 shape: malformed ESTIMATED perp
# payloads, is_long=null, 0 position_events) — the exact production case VIB-5943
# regressed on.
_MALFORMED_FIXTURE = (
    Path(__file__).resolve().parents[5]
    / "tests/fixtures/accounting/perp/vib5941_gmx_avax_mainnet.sqlite"
)
_FIXTURE_DEPLOYMENT_ID = "deployment:e32d997e1002"


def test_perp_position_event_classifies_as_perp() -> None:
    classes = _detect_strategy_classes(
        lending_events=[],
        position_events=[{"position_type": "PERP", "event_type": "OPEN"}],
        ledger_entries=[],
    )
    assert StrategyClass.PERP in classes
    assert StrategyClass.UNKNOWN not in classes


def test_perp_raw_marker_classifies_when_payload_fails_deserialization() -> None:
    """A pre-VIB-5941 malformed perp row (is_long=None / no size) has confidence
    ESTIMATED — NOT UNAVAILABLE — so it never reaches the unavailable bucket and
    its payload fails deserialization (parse_errors). Classification must key off
    the RAW event_type marker, which survives regardless. This is the exact case
    that regressed the old ``unavailable_records``-only check.
    """
    malformed_raw = {
        "id": "ae-1",
        "event_type": "PERP_OPEN",
        "confidence": "ESTIMATED",
        # The pre-fix broken payload: is_long null, no `size`, wrong `size_usd` key.
        "payload_json": '{"event_type":"PERP_OPEN","is_long":null,"size_usd":null}',
    }
    classes = _detect_strategy_classes(
        lending_events=[],
        position_events=[],
        ledger_entries=[],
        raw_accounting_events=[malformed_raw],
    )
    assert StrategyClass.PERP in classes
    assert StrategyClass.UNKNOWN not in classes


def test_perp_event_marker_in_unavailable_bucket_still_classifies() -> None:
    """An UNAVAILABLE-confidence perp marker also classifies PERP (defence in depth)."""
    classes = _detect_strategy_classes(
        lending_events=[],
        position_events=[],
        ledger_entries=[],
        unavailable_records=[{"event_type": "PERP_CLOSE"}],
    )
    assert StrategyClass.PERP in classes
    assert StrategyClass.UNKNOWN not in classes


def test_liquidation_only_history_classifies_as_perp() -> None:
    """A deployment whose ONLY rows are non-OPEN/CLOSE perp event types
    (PERP_LIQUIDATE, PERP_INCREASE, PERP_DECREASE) must still classify PERP —
    the classifier matches the full PerpEventType enum, not just OPEN/CLOSE.
    """
    for marker in ("PERP_LIQUIDATE", "PERP_INCREASE", "PERP_DECREASE"):
        classes = _detect_strategy_classes(
            lending_events=[],
            position_events=[],
            ledger_entries=[],
            raw_accounting_events=[{"event_type": marker, "confidence": "ESTIMATED"}],
        )
        assert StrategyClass.PERP in classes, f"{marker} should classify PERP"
        assert StrategyClass.UNKNOWN not in classes


def test_no_perp_signal_stays_unknown() -> None:
    classes = _detect_strategy_classes(lending_events=[], position_events=[], ledger_entries=[])
    assert classes == frozenset({StrategyClass.UNKNOWN})


def test_mixed_lp_and_perp_both_detected() -> None:
    classes = _detect_strategy_classes(
        lending_events=[],
        position_events=[
            {"position_type": "LP", "event_type": "OPEN"},
            {"position_type": "PERP", "event_type": "OPEN"},
        ],
        ledger_entries=[],
    )
    assert StrategyClass.LP in classes
    assert StrategyClass.PERP in classes


@pytest.mark.asyncio
async def test_end_to_end_malformed_perp_fixture_classifies_perp_not_unknown() -> None:
    """Full ``load_accounting_data`` against the sealed pre-fix mainnet DB.

    The fixture has ONLY malformed ESTIMATED perp accounting rows (is_long=null,
    no ``size``) and ZERO position_events — the exact production shape that made
    ``strat pnl`` report ``strategy_classes=['unknown']`` (VIB-5943). The perp
    rows never reach ``connector_events`` (no registered perp deserializer) and
    are ESTIMATED (not UNAVAILABLE), so ONLY the raw-marker path can classify
    them — which is exactly the seam the deserialize-gated check missed.
    """
    assert _MALFORMED_FIXTURE.exists(), f"missing sealed fixture {_MALFORMED_FIXTURE}"
    data = await load_accounting_data(str(_MALFORMED_FIXTURE), _FIXTURE_DEPLOYMENT_ID)
    assert StrategyClass.PERP in data.strategy_classes
    assert StrategyClass.UNKNOWN not in data.strategy_classes
    # Preconditions proving classification came from RAW markers, not a fallback:
    # zero position_events (the LP/PERP position_type path is dead here) and the
    # perp payloads are absent from connector_events (deserializer returns None).
    assert not data.position_events
    assert not any("perp" in str(k).lower() for k in data.connector_events)
