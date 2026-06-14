"""Cross-section NAV-range coordination resolver (VIB-5114).

The NAV range selector (``sections.render_nav_history_section``) persists the
operator's chosen preset into ``session_state["nav_range_{deployment_id}"]`` via
the ``st.radio`` ``key=``. The TA/LP price-chart templates read that SAME key so
their candle fetch follows the range. These tests pin the shared key format and
the seconds-translation resolver, including the Empty != Zero distinction
(unset/garbage is ``None``, "All" is a measured ``0``).
"""

from __future__ import annotations

import pytest

from almanak.framework.dashboard.sections import (
    NAV_RANGE_SECONDS,
    nav_range_session_key,
    selected_nav_range_seconds,
)


def test_session_key_format_is_deployment_scoped() -> None:
    assert nav_range_session_key("deployment:abc123") == "nav_range_deployment:abc123"
    # Distinct deployments never share a key (two strategies in one session).
    assert nav_range_session_key("deployment:aaa") != nav_range_session_key("deployment:bbb")


def test_public_range_map_matches_presets() -> None:
    assert NAV_RANGE_SECONDS == {"24h": 86_400, "7d": 604_800, "30d": 2_592_000, "All": 0}


@pytest.mark.parametrize(
    ("label", "expected"),
    [("24h", 86_400), ("7d", 604_800), ("30d", 2_592_000)],
)
def test_bounded_preset_resolves_to_seconds(label: str, expected: int) -> None:
    state = {nav_range_session_key("d"): label}
    assert selected_nav_range_seconds("d", state) == expected


def test_all_resolves_to_zero_open_bound() -> None:
    # "All" is a *measured* open bound (full lifetime) — 0, NOT None.
    state = {nav_range_session_key("d"): "All"}
    assert selected_nav_range_seconds("d", state) == 0


def test_unset_is_none_not_all() -> None:
    # Empty != Zero: no selection is None (caller keeps its default), not 0/"All".
    assert selected_nav_range_seconds("d", {}) is None
    assert selected_nav_range_seconds("d", None) is None


def test_unknown_preset_is_none() -> None:
    assert selected_nav_range_seconds("d", {nav_range_session_key("d"): "bogus"}) is None


def test_non_string_stored_value_is_none() -> None:
    # A malformed value (e.g. an int leaked into session_state) must not crash and
    # must not be treated as a range — it reads as "unset".
    assert selected_nav_range_seconds("d", {nav_range_session_key("d"): 123}) is None


def test_other_deployment_key_is_ignored() -> None:
    # A range selected for a DIFFERENT deployment must not leak into this one.
    state = {nav_range_session_key("other"): "7d"}
    assert selected_nav_range_seconds("d", state) is None
