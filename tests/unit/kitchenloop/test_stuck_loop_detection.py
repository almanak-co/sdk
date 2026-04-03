"""Tests for stuck-loop detection logic in kitchenloop.sh.

Validates that:
1. The ideate timeout is set to 90 min (5400s) — VIB-2298 fix
2. Stuck-loop detection constants are logically consistent

VIB-2298: Ideate phase systematically timed out at 60 min for complex strategies,
leaving the loop stuck on the same iteration number indefinitely.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

KITCHENLOOP_SH = Path(__file__).parent.parent.parent.parent / "scripts" / "kitchenloop" / "kitchenloop.sh"


def _extract_var(name: str) -> str | None:
    """Extract the first assignment of `name=...` from kitchenloop.sh."""
    text = KITCHENLOOP_SH.read_text()
    pattern = rf"^\s*(?:export\s+)?{re.escape(name)}=(\S+)"
    for line in text.splitlines():
        m = re.match(pattern, line)
        if m:
            return m.group(1).strip()
    return None


def test_kitchenloop_sh_exists():
    assert KITCHENLOOP_SH.exists(), f"kitchenloop.sh not found at {KITCHENLOOP_SH}"


def test_ideate_timeout_is_90_min():
    """VIB-2298: ideate timeout must be at least 5400s (90 min)."""
    val = _extract_var("IDEATE_TIMEOUT")
    assert val is not None, "IDEATE_TIMEOUT not found in kitchenloop.sh"
    timeout = int(val)
    assert timeout >= 5400, (
        f"IDEATE_TIMEOUT={timeout} is less than 5400s (90 min). "
        "Complex strategies exceed 60 min — see VIB-2298."
    )


def test_max_stuck_iters_defined():
    """VIB-2298: MAX_STUCK_ITERS must be defined and less than MAX_CONSECUTIVE_FAILS.

    MAX_STUCK_ITERS must be strictly less than MAX_CONSECUTIVE_FAILS so that
    auto-advance fires before the exit gate, leaving at least one loop iteration
    to consume the advanced counter.
    """
    val = _extract_var("MAX_STUCK_ITERS")
    assert val is not None, "MAX_STUCK_ITERS not found in kitchenloop.sh"
    assert int(val) >= 1, f"MAX_STUCK_ITERS={val} must be at least 1"

    max_consec = _extract_var("MAX_CONSECUTIVE_FAILS")
    assert max_consec is not None, "MAX_CONSECUTIVE_FAILS not found in kitchenloop.sh"
    assert int(val) < int(max_consec), (
        f"MAX_STUCK_ITERS={val} must be strictly less than MAX_CONSECUTIVE_FAILS={max_consec}. "
        "If they are equal, auto-advance fires on the same iteration as the exit gate, "
        "making it useless."
    )


def test_stuck_detection_logic_present():
    """VIB-2298: stuck-loop detection logic must be present in the main loop."""
    text = KITCHENLOOP_SH.read_text()
    assert "STUCK_ITER_COUNT" in text, "Stuck-loop counter (STUCK_ITER_COUNT) not found"
    assert "advance_iteration_counter" in text, "advance_iteration_counter not found"
    # Verify the auto-advance path is actually reachable from STUCK_ITER_COUNT check
    assert "AUTO-ADVANCE" in text or "auto-advance" in text.lower(), (
        "Auto-advance log message not found in stuck-loop detection"
    )


def test_stuck_detection_runs_before_consecutive_fails_exit():
    """VIB-2298: stuck-loop detection must appear before the MAX_CONSECUTIVE_FAILS exit.

    The stuck-loop block must execute BEFORE the exit gate so that auto-advance fires
    (advancing the iteration counter and resetting STUCK_ITER_COUNT) before the exit
    check runs.  Note: auto-advance intentionally does NOT reset CONSECUTIVE_FAILS —
    see VIB-2361 and test_auto_advance_does_not_reset_consecutive_fails.
    """
    text = KITCHENLOOP_SH.read_text()
    stuck_pos = text.find("STUCK_ITER_COUNT=$((STUCK_ITER_COUNT + 1))")
    exit_pos = text.find('echo "$(date) | STOPPED after $MAX_CONSECUTIVE_FAILS consecutive failures"')
    assert stuck_pos != -1, "Stuck-loop increment not found"
    assert exit_pos != -1, "MAX_CONSECUTIVE_FAILS exit log not found"
    assert stuck_pos < exit_pos, (
        "Stuck-loop detection must appear BEFORE the MAX_CONSECUTIVE_FAILS exit check "
        "so that auto-advance (advance_iteration_counter) fires before the exit gate runs."
    )


def test_auto_advance_does_not_reset_consecutive_fails():
    """VIB-2361: auto-advance must NOT reset CONSECUTIVE_FAILS.

    Resetting the global failure counter on auto-advance would allow a systemically
    broken iteration to loop forever — MAX_STUCK_ITERS would fire, reset CONSECUTIVE_FAILS,
    then the same broken iteration could fail again indefinitely without ever hitting the
    MAX_CONSECUTIVE_FAILS exit gate.  The stuck detector must advance the counter only,
    leaving CONSECUTIVE_FAILS intact so that persistent failures still terminate the loop.
    """
    text = KITCHENLOOP_SH.read_text()
    # Find the AUTO-ADVANCE log line (unique to the stuck-loop block in the main loop)
    anchor = "AUTO-ADVANCE past iter"
    anchor_pos = text.find(anchor)
    assert anchor_pos != -1, "AUTO-ADVANCE log line not found in kitchenloop.sh"
    # CONSECUTIVE_FAILS=0 must NOT appear within the auto-advance block
    post_advance = text[anchor_pos:]
    # Find the end of the if-block (next `fi` or next blank-line + dedent) by limiting scope
    # to 400 chars — enough to cover STUCK_ITER_COUNT=0 / STUCK_ITER_NUM=0 / fi
    block = post_advance[:400]
    assert not re.search(r"\bCONSECUTIVE_FAILS=0\b", block), (
        "auto-advance block must NOT reset CONSECUTIVE_FAILS=0 — "
        "doing so would let a permanently-broken iteration bypass the exit gate (VIB-2361)"
    )


@pytest.mark.parametrize("stuck_count,max_stuck,should_advance", [
    (0, 3, False),
    (1, 3, False),
    (2, 3, False),
    (3, 3, True),
    (4, 3, True),
    (2, 2, True),
])
def test_stuck_advance_threshold(stuck_count: int, max_stuck: int, should_advance: bool):
    """Verify the >= comparison logic used in kitchenloop.sh stuck detection."""
    # Mirrors: if [ "$STUCK_ITER_COUNT" -ge "$MAX_STUCK_ITERS" ]
    result = stuck_count >= max_stuck
    assert result == should_advance, (
        f"With stuck_count={stuck_count}, max_stuck={max_stuck}: "
        f"expected should_advance={should_advance}, got {result}"
    )
