"""Regression tests for the auto-refresh loop in ``app.main``.

Covers the auto-refresh contract where Streamlit needs a scheduled rerun for
the countdown to advance:

    * below threshold: keep ``last_refresh`` unchanged, sleep briefly, rerun
    * at/above threshold: update ``last_refresh`` to now, rerun immediately

The tests drive ``app.main`` via ``streamlit.testing.AppTest.from_function``
with session state seeded before the first run, and spy on ``time.sleep`` plus
``streamlit.rerun`` so we can assert the selected branch.

``AppTest.from_function`` pickles the driver and executes it as the whole
script body - so all imports must live *inside* the driver function, not at
module top level.
"""

from __future__ import annotations

from streamlit.testing.v1 import AppTest


def _drive_below_threshold() -> None:
    """Elapsed < refresh_interval -> countdown rerun, last_refresh unchanged."""
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
    fake_sleep = MagicMock()

    class _FakeDatetime(datetime):
        @classmethod
        def now(cls, tz=None):  # type: ignore[override]
            return fake_now

    with (
        patch("almanak.framework.dashboard.app.datetime", _FakeDatetime),
        patch("almanak.framework.dashboard.app._sleep", fake_sleep),
        patch("streamlit.rerun", fake_rerun),
        patch(
            "almanak.framework.dashboard.app.get_all_strategies",
            return_value=[],
        ),
    ):
        from almanak.framework.dashboard.app import main

        main()

    # Below threshold: rerun once to advance the visible countdown, but do
    # not mutate last_refresh until the configured interval has elapsed.
    fake_sleep.assert_called_once_with(1)
    assert fake_rerun.call_count == 1, "rerun() should keep countdown moving below threshold"
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
    fake_sleep = MagicMock()

    class _FakeDatetime(datetime):
        @classmethod
        def now(cls, tz=None):  # type: ignore[override]
            return fake_now

    with (
        patch("almanak.framework.dashboard.app.datetime", _FakeDatetime),
        patch("almanak.framework.dashboard.app._sleep", fake_sleep),
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
    fake_sleep.assert_not_called()
    # Interval must remain untouched.
    assert st.session_state.refresh_interval == 30


# ---------------------------------------------------------------------------
# AppTest-wrapped tests
# ---------------------------------------------------------------------------


def test_auto_refresh_below_threshold_schedules_countdown_rerun() -> None:
    """Elapsed < interval -> sleep/rerun, last_refresh untouched."""
    at = AppTest.from_function(_drive_below_threshold).run(timeout=30)

    # Any assertion failure inside the driver surfaces as ``at.exception``.
    assert not at.exception, f"driver failed: {at.exception}"


def test_auto_refresh_at_or_above_threshold_reruns_exactly_once_per_tick() -> None:
    """Regression for #1715: elapsed >= interval -> rerun, last_refresh bumped.

    Proves the elapsed-time gate selects the immediate-refresh branch without
    the countdown sleep.
    """
    at = AppTest.from_function(_drive_above_threshold).run(timeout=30)

    assert not at.exception, f"driver failed: {at.exception}"
