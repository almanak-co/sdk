"""Regression tests for the auto-refresh elapsed-time gate in ``app.main``.

Covers the behavioural change from issue #1715 where the ``time.sleep(0.1)``
guard was removed from the auto-refresh block. The remaining contract is:

    ``st.rerun()`` fires only when
    ``(now - last_refresh).total_seconds() >= refresh_interval``.

Below the threshold the function must leave ``last_refresh`` untouched and
never call ``st.rerun()``. Above (or at) the threshold it updates
``last_refresh`` to the current time and reruns once. Removing the sleep
means there is no longer a second defence against tight rerun loops, so
these tests nail the elapsed-time gate as the single source of truth.

The tests drive ``app.main`` via ``streamlit.testing.AppTest.from_function``
with session state seeded before the first run, and spy on
``streamlit.rerun`` so we can assert it was (or was not) called.

``AppTest.from_function`` pickles the driver and executes it as the whole
script body - so all imports must live *inside* the driver function, not at
module top level.
"""

from __future__ import annotations

from streamlit.testing.v1 import AppTest


def _drive_below_threshold() -> None:
    """Elapsed < refresh_interval -> no rerun, last_refresh unchanged."""
    from datetime import datetime, timedelta
    from unittest.mock import MagicMock, patch

    import streamlit as st

    # Session state seeded before ``main`` so the auto-refresh branch is
    # taken but the elapsed-time gate has not yet fired.
    last_refresh = datetime(2026, 4, 21, 12, 0, 0)
    st.session_state.auto_refresh = True
    st.session_state.last_refresh = last_refresh
    st.session_state.refresh_interval = 30

    # Freeze ``datetime.now`` and spy on ``st.rerun``. Both are patched at
    # the module level so imports inside ``main`` see the stubs.
    fake_now = last_refresh + timedelta(seconds=5)  # 5 < 30, well below threshold
    fake_rerun = MagicMock()

    class _FakeDatetime(datetime):
        @classmethod
        def now(cls, tz=None):  # type: ignore[override]
            return fake_now

    with (
        patch("almanak.framework.dashboard.app.datetime", _FakeDatetime),
        patch("streamlit.rerun", fake_rerun),
        patch(
            "almanak.framework.dashboard.app.get_all_strategies",
            return_value=[],
        ),
    ):
        from almanak.framework.dashboard.app import main

        main()

    # Below threshold: must not rerun, must not mutate last_refresh.
    assert fake_rerun.call_count == 0, "rerun() fired below threshold"
    assert st.session_state.last_refresh == last_refresh, (
        "last_refresh was mutated despite elapsed < refresh_interval"
    )
    # Interval must remain untouched.
    assert st.session_state.refresh_interval == 30


def _drive_above_threshold() -> None:
    """Elapsed >= refresh_interval -> rerun fires, last_refresh updates."""
    from datetime import datetime, timedelta
    from unittest.mock import MagicMock, patch

    import streamlit as st

    last_refresh = datetime(2026, 4, 21, 12, 0, 0)
    st.session_state.auto_refresh = True
    st.session_state.last_refresh = last_refresh
    st.session_state.refresh_interval = 30

    fake_now = last_refresh + timedelta(seconds=45)  # 45 >= 30
    fake_rerun = MagicMock()

    class _FakeDatetime(datetime):
        @classmethod
        def now(cls, tz=None):  # type: ignore[override]
            return fake_now

    with (
        patch("almanak.framework.dashboard.app.datetime", _FakeDatetime),
        patch("streamlit.rerun", fake_rerun),
        patch(
            "almanak.framework.dashboard.app.get_all_strategies",
            return_value=[],
        ),
    ):
        from almanak.framework.dashboard.app import main

        main()

    # Above threshold: rerun must have fired exactly once, and
    # ``last_refresh`` must now equal ``fake_now`` (the updated time).
    # The "== 1" assertion enforces the "exactly once per tick" contract -
    # anything higher would indicate a tight-loop regression, which is the
    # exact failure mode the removed ``time.sleep(0.1)`` was guarding
    # against.
    assert fake_rerun.call_count == 1, (
        "rerun() should fire exactly once when elapsed >= refresh_interval"
    )
    assert st.session_state.last_refresh == fake_now, (
        "last_refresh was not bumped to the current time after rerun"
    )
    # Interval must remain untouched.
    assert st.session_state.refresh_interval == 30


# ---------------------------------------------------------------------------
# AppTest-wrapped tests
# ---------------------------------------------------------------------------


def test_auto_refresh_below_threshold_does_not_rerun() -> None:
    """Regression for #1715: elapsed < interval -> no rerun, state untouched."""
    at = AppTest.from_function(_drive_below_threshold).run(timeout=30)

    # Any assertion failure inside the driver surfaces as ``at.exception``.
    assert not at.exception, f"driver failed: {at.exception}"


def test_auto_refresh_at_or_above_threshold_reruns_exactly_once_per_tick() -> None:
    """Regression for #1715: elapsed >= interval -> rerun, last_refresh bumped.

    Proves the elapsed-time gate is the single defence against tight loops
    now that the ``time.sleep(0.1)`` fallback has been removed.
    """
    at = AppTest.from_function(_drive_above_threshold).run(timeout=30)

    assert not at.exception, f"driver failed: {at.exception}"
