"""Unit tests for :mod:`almanak.framework.accounting.reporting.swap_class_fallback`.

The detection module is the source-of-truth for the F4 / VIB-4907 verdict
that ``strat pnl`` consumes; these tests pin the three conjunctive rules,
the canonicalisation discipline, and the negative-space cases (insufficient
data, wrong ordering, malformed rows) so a future edit to the detector
can't silently flip a verdict.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from almanak.framework.accounting.reporting.swap_class_fallback import (
    SwapClassFallbackDetection,
    detect_stale_post_teardown_snapshot,
)


# ---------------------------------------------------------------------------
# Test doubles — minimal shapes that look enough like a PortfolioSnapshot /
# LedgerEntry for the helper's ``getattr`` reads.  Real types pull in heavy
# imports we don't need here.
# ---------------------------------------------------------------------------


@dataclass
class _Snap:
    timestamp: datetime
    cycle_id: str = ""
    wallet_balances_json: str = "[]"
    positions_json: str = "[]"
    token_prices_json: str = "{}"


@dataclass
class _Ledger:
    timestamp: datetime
    intent_type: str = "SWAP"
    success: bool = True


def _ts(seconds: int) -> datetime:
    """Build deterministic timestamps offset from a fixed epoch."""
    return datetime(2026, 5, 30, 12, 0, 0, tzinfo=UTC) + timedelta(seconds=seconds)


# Canonical inputs the positive case reuses; identical between the two
# snapshots so Rule 1 (byte-identity) holds.
_BAL_JSON = '[{"symbol": "WETH", "amount": "0.001", "value_usd": "6.47"}]'
_POS_JSON = '[]'
_PRICES_JSON = '{"arbitrum:0xWETH": {"price_usd": "6470.0"}}'


def _positive_pair() -> list[_Snap]:
    """Two snapshots that satisfy Rule 1 (identity) and Rule 2 (cycle_id)."""
    return [
        _Snap(
            timestamp=_ts(0),
            cycle_id="iter-1",
            wallet_balances_json=_BAL_JSON,
            positions_json=_POS_JSON,
            token_prices_json=_PRICES_JSON,
        ),
        _Snap(
            timestamp=_ts(60),
            cycle_id="teardown-abc123",
            wallet_balances_json=_BAL_JSON,
            positions_json=_POS_JSON,
            token_prices_json=_PRICES_JSON,
        ),
    ]


# ---------------------------------------------------------------------------
# Positive: all three rules fire — the canonical RSI-mainnet pattern.
# ---------------------------------------------------------------------------


def test_positive_all_three_rules_fire() -> None:
    snaps = _positive_pair()
    swap_between = _Ledger(timestamp=_ts(30), intent_type="SWAP", success=True)

    verdict = detect_stale_post_teardown_snapshot(snaps, [swap_between])

    assert verdict.suppressed is True
    assert "byte-identical" in verdict.reason
    assert "VIB-4906" in verdict.reason
    assert "VIB-4907" in verdict.reason


def test_positive_swap_at_exact_boundary_timestamps() -> None:
    """Inclusive interval — a SWAP at either endpoint still counts."""
    snaps = _positive_pair()
    swap_at_start = _Ledger(timestamp=_ts(0), success=True)
    assert detect_stale_post_teardown_snapshot(snaps, [swap_at_start]).suppressed is True

    swap_at_end = _Ledger(timestamp=_ts(60), success=True)
    assert detect_stale_post_teardown_snapshot(snaps, [swap_at_end]).suppressed is True


# ---------------------------------------------------------------------------
# Rule 1: byte-identity must hold across all three JSON columns.
# ---------------------------------------------------------------------------


def test_rule1_wallet_balances_diverge_means_no_suppression() -> None:
    snaps = _positive_pair()
    snaps[1].wallet_balances_json = '[{"symbol": "USDC", "amount": "25.76"}]'
    swap = _Ledger(timestamp=_ts(30))

    verdict = detect_stale_post_teardown_snapshot(snaps, [swap])

    assert verdict.suppressed is False
    assert verdict.reason == ""


def test_rule1_positions_diverge_means_no_suppression() -> None:
    snaps = _positive_pair()
    snaps[1].positions_json = '[{"id": "1"}]'
    swap = _Ledger(timestamp=_ts(30))

    verdict = detect_stale_post_teardown_snapshot(snaps, [swap])

    assert verdict.suppressed is False


def test_rule1_token_prices_diverge_means_no_suppression() -> None:
    snaps = _positive_pair()
    snaps[1].token_prices_json = '{"arbitrum:0xWETH": {"price_usd": "6500.0"}}'
    swap = _Ledger(timestamp=_ts(30))

    verdict = detect_stale_post_teardown_snapshot(snaps, [swap])

    assert verdict.suppressed is False


def test_rule1_canonicalisation_ignores_key_order() -> None:
    """Same logical state but with different key insertion order still matches."""
    snaps = _positive_pair()
    # Reorder keys in the post-teardown snapshot's wallet_balances_json.  The
    # raw bytes differ; the canonical form is identical.
    snaps[1].wallet_balances_json = (
        '[{"value_usd": "6.47", "amount": "0.001", "symbol": "WETH"}]'
    )
    swap = _Ledger(timestamp=_ts(30))

    verdict = detect_stale_post_teardown_snapshot(snaps, [swap])

    assert verdict.suppressed is True


def test_rule1_canonicalisation_handles_unparseable_payload() -> None:
    """If both rows contain the same opaque non-JSON text, they still match.

    We compare the raw text as a fallback rather than failing loudly — a
    one-off parser hiccup shouldn't both prevent suppression AND raise.
    """
    snaps = _positive_pair()
    snaps[0].wallet_balances_json = "{ not json"
    snaps[1].wallet_balances_json = "{ not json"
    swap = _Ledger(timestamp=_ts(30))

    verdict = detect_stale_post_teardown_snapshot(snaps, [swap])

    assert verdict.suppressed is True


# ---------------------------------------------------------------------------
# Rule 2: latest snapshot's cycle_id must start with ``teardown-``.
# ---------------------------------------------------------------------------


def test_rule2_iteration_cycle_id_means_no_suppression() -> None:
    snaps = _positive_pair()
    snaps[1].cycle_id = "iter-2"
    swap = _Ledger(timestamp=_ts(30))

    verdict = detect_stale_post_teardown_snapshot(snaps, [swap])

    assert verdict.suppressed is False


def test_rule2_empty_cycle_id_means_no_suppression() -> None:
    snaps = _positive_pair()
    snaps[1].cycle_id = ""
    swap = _Ledger(timestamp=_ts(30))

    verdict = detect_stale_post_teardown_snapshot(snaps, [swap])

    assert verdict.suppressed is False


def test_rule2_substring_teardown_does_not_match() -> None:
    """``cycle_id`` must START with ``teardown-`` — a substring is rejected."""
    snaps = _positive_pair()
    snaps[1].cycle_id = "iter-2-teardown-abc"
    swap = _Ledger(timestamp=_ts(30))

    verdict = detect_stale_post_teardown_snapshot(snaps, [swap])

    assert verdict.suppressed is False


# ---------------------------------------------------------------------------
# Rule 3: a SWAP between the two snapshot timestamps must exist and succeed.
# ---------------------------------------------------------------------------


def test_rule3_no_intervening_swap_means_no_suppression() -> None:
    snaps = _positive_pair()
    # Empty ledger — no SWAP to fire the rule.
    verdict = detect_stale_post_teardown_snapshot(snaps, [])

    assert verdict.suppressed is False


def test_rule3_failed_swap_does_not_count() -> None:
    snaps = _positive_pair()
    failed = _Ledger(timestamp=_ts(30), intent_type="SWAP", success=False)

    verdict = detect_stale_post_teardown_snapshot(snaps, [failed])

    assert verdict.suppressed is False


def test_rule3_truthy_non_bool_success_does_not_count() -> None:
    """``success is True`` — a legacy string ``"1"`` or int ``1`` is rejected.

    Empty≠Zero discipline at the detection site: we only react when the
    writer was explicit about the boolean.
    """
    snaps = _positive_pair()

    # SimpleNamespace stand-in that returns a string for ``success``.  Cannot
    # use the @dataclass since the field type is ``bool``.
    class _LegacyLedger:
        def __init__(self) -> None:
            self.timestamp = _ts(30)
            self.intent_type = "SWAP"
            self.success: Any = "1"

    verdict = detect_stale_post_teardown_snapshot(snaps, [_LegacyLedger()])

    assert verdict.suppressed is False


def test_rule3_non_swap_intent_does_not_count() -> None:
    snaps = _positive_pair()
    lp_open = _Ledger(timestamp=_ts(30), intent_type="LP_OPEN", success=True)

    verdict = detect_stale_post_teardown_snapshot(snaps, [lp_open])

    assert verdict.suppressed is False


def test_rule3_swap_outside_window_does_not_count() -> None:
    snaps = _positive_pair()
    too_early = _Ledger(timestamp=_ts(-30), success=True)
    too_late = _Ledger(timestamp=_ts(90), success=True)

    verdict = detect_stale_post_teardown_snapshot(snaps, [too_early, too_late])

    assert verdict.suppressed is False


def test_rule3_case_insensitive_intent_type() -> None:
    """Writer historically lowercased; detection accepts both."""
    snaps = _positive_pair()
    lowercase = _Ledger(timestamp=_ts(30), intent_type="swap", success=True)

    verdict = detect_stale_post_teardown_snapshot(snaps, [lowercase])

    assert verdict.suppressed is True


def test_rule3_mixed_ledger_with_one_qualifying_swap_fires() -> None:
    snaps = _positive_pair()
    entries: list[Any] = [
        _Ledger(timestamp=_ts(-100), intent_type="SWAP", success=True),  # outside
        _Ledger(timestamp=_ts(30), intent_type="LP_OPEN", success=True),  # wrong type
        _Ledger(timestamp=_ts(30), intent_type="SWAP", success=False),  # failed
        _Ledger(timestamp=_ts(30), intent_type="SWAP", success=True),  # qualifies
    ]

    verdict = detect_stale_post_teardown_snapshot(snaps, entries)

    assert verdict.suppressed is True


# ---------------------------------------------------------------------------
# Negative-space: insufficient or malformed data.  The detector must return
# ``False`` cleanly rather than raising or producing a spurious positive.
# ---------------------------------------------------------------------------


def test_empty_snapshot_window_returns_false() -> None:
    verdict = detect_stale_post_teardown_snapshot([], [])
    assert verdict.suppressed is False


def test_single_snapshot_returns_false() -> None:
    """Cannot compute identity with only one row."""
    one = _Snap(timestamp=_ts(0), cycle_id="teardown-x")
    swap = _Ledger(timestamp=_ts(0))

    verdict = detect_stale_post_teardown_snapshot([one], [swap])

    assert verdict.suppressed is False


def test_wrong_ordering_refuses_to_fire() -> None:
    """Caller passed snapshots reversed — refuse rather than guess."""
    later = _Snap(timestamp=_ts(60), cycle_id="teardown-x", wallet_balances_json=_BAL_JSON)
    earlier = _Snap(timestamp=_ts(0), cycle_id="iter-1", wallet_balances_json=_BAL_JSON)
    swap = _Ledger(timestamp=_ts(30))

    # Reverse: latest-first
    verdict = detect_stale_post_teardown_snapshot([later, earlier], [swap])
    # By the helper's contract recent_snapshots[-1] is latest — here it's
    # ``earlier`` (cycle_id == "iter-1") so Rule 2 fails.  Returns False.
    assert verdict.suppressed is False


def test_mixed_tz_aware_and_naive_timestamps_dont_raise() -> None:
    """Gemini audit catch: aware-vs-naive datetime comparison.

    Without ``_naive_utc`` normalisation, ``prev_ts <= entry_ts <= latest_ts``
    raises ``TypeError`` when one side is tz-aware UTC and the other is
    naive (possible if a SQLite deserialiser drops tz info).  The detector
    must normalise both sides before comparing and continue producing a
    real verdict rather than crashing mid-detection.
    """
    from datetime import datetime as _datetime, timedelta as _timedelta

    # Build aware UTC snapshot bounds + a naive entry timestamp at the
    # SAME wall-clock moment.  Pre-fix, the inner comparison raises.
    aware_base = _datetime(2026, 5, 30, 12, 0, 0, tzinfo=UTC)
    aware_pair = [
        _Snap(
            timestamp=aware_base,
            cycle_id="iter-1",
            wallet_balances_json=_BAL_JSON,
            positions_json=_POS_JSON,
            token_prices_json=_PRICES_JSON,
        ),
        _Snap(
            timestamp=aware_base + _timedelta(seconds=60),
            cycle_id="teardown-mixed",
            wallet_balances_json=_BAL_JSON,
            positions_json=_POS_JSON,
            token_prices_json=_PRICES_JSON,
        ),
    ]
    naive_swap = _Ledger(
        timestamp=_datetime(2026, 5, 30, 12, 0, 30),  # NAIVE, same moment as _ts(30)
        intent_type="SWAP",
        success=True,
    )

    # No raise + correct verdict (rule 1 + 2 + 3 all hold).
    verdict = detect_stale_post_teardown_snapshot(aware_pair, [naive_swap])
    assert verdict.suppressed is True


def test_canonical_json_set_iteration_is_deterministic() -> None:
    """Gemini audit catch: set/frozenset iteration order varies via hash
    randomisation; ``_canonical_json`` must sort before recursing so the
    byte-identity check between two snapshots never falsely diverges
    because one snapshot's set happened to iterate differently.
    """
    from almanak.framework.accounting.reporting.swap_class_fallback import _canonical_json

    # Two sets with the same elements but added in different orders.
    a = {"z", "a", "m", "b"}
    b = {"b", "z", "a", "m"}
    # The two canonical dumps must match exactly.
    assert _canonical_json(a) == _canonical_json(b)
    # And the dump must be sorted (deterministic across processes).
    assert _canonical_json(a) == _canonical_json({"a", "b", "m", "z"})


def test_missing_timestamp_on_snapshot_returns_false() -> None:
    @dataclass
    class _Faulty:
        cycle_id: str = "teardown-x"
        wallet_balances_json: str = _BAL_JSON
        positions_json: str = _POS_JSON
        token_prices_json: str = _PRICES_JSON
        timestamp: Any = None

    snaps: list[Any] = [
        _Snap(timestamp=_ts(0), wallet_balances_json=_BAL_JSON),
        _Faulty(),
    ]
    swap = _Ledger(timestamp=_ts(30))

    verdict = detect_stale_post_teardown_snapshot(snaps, [swap])

    assert verdict.suppressed is False


# ---------------------------------------------------------------------------
# Dataclass contract — keep the shape pinned so consumers don't break.
# ---------------------------------------------------------------------------


def test_verdict_is_frozen_and_carries_both_fields() -> None:
    v = SwapClassFallbackDetection(suppressed=True, reason="hello")
    assert v.suppressed is True
    assert v.reason == "hello"
    with pytest.raises(Exception):
        v.suppressed = False  # type: ignore[misc]
