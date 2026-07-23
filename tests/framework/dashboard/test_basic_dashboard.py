"""Unit tests for the shared demo-agnostic dashboard renderer.

``almanak.framework.dashboard.custom.basic.render_basic_dashboard`` is the
single-source implementation that all seven basic demos re-export as
``render_custom_dashboard`` (the loader-discovered interface, VIB-3969). These
tests pin:

1. It composes all five deployment-scoped section renderers with the expected
   arguments (nav uses ``default_range="All"``; positions receives the
   ``api_client``).
2. The caption is built from ``deployment_id`` / ``chain`` / ``protocol`` and
   tolerates a missing / falsy ``strategy_config``.
3. ``_safe`` isolates each panel — a raising section degrades to ``st.info``
   instead of propagating and blanking the page.
4. Each demo ``dashboard/ui.py`` binds ``render_custom_dashboard`` to the shared
   implementation (no per-demo duplication).
"""

from __future__ import annotations

import importlib.util
from pathlib import Path
from unittest.mock import patch

import pytest

from almanak.framework.dashboard.custom import basic

DEMOS = [
    "benqi_lending_lifecycle",
    "benqi_looping",
    "gmx_perp_lifecycle",
    "gmx_v2_directional_perp",
    "metamorpho_base_yield",
    "pancakeswap_aave_carry_bsc",
    "uniswap_v4_hooks",
]

_REPO_ROOT = Path(__file__).resolve().parents[3]


def _patch_sections():
    """Patch ``st`` plus all five section renderers on the ``basic`` module."""
    return (
        patch.object(basic, "st"),
        patch.object(basic, "render_pnl_section"),
        patch.object(basic, "render_nav_history_section"),
        patch.object(basic, "render_cost_stack_section"),
        patch.object(basic, "render_position_lifecycle_section"),
        patch.object(basic, "render_trade_tape_section"),
    )


def test_render_basic_dashboard_composes_all_sections() -> None:
    p_st, p_pnl, p_nav, p_cost, p_pos, p_tape = _patch_sections()
    with p_st as mock_st, p_pnl as pnl, p_nav as nav, p_cost as cost, p_pos as pos, p_tape as tape:
        basic.render_basic_dashboard(
            "deployment:abc123",
            {"chain": "base", "protocol": "aave_v3"},
            api_client="API",
            session_state={},
        )

    pnl.assert_called_once_with("deployment:abc123")
    nav.assert_called_once_with("deployment:abc123", default_range="All")
    cost.assert_called_once_with("deployment:abc123")
    pos.assert_called_once_with("deployment:abc123", "API")
    tape.assert_called_once_with("deployment:abc123")

    mock_st.title.assert_called_once_with("Strategy Dashboard")
    caption = mock_st.caption.call_args.args[0]
    assert "deployment:abc123" in caption
    assert "base" in caption
    assert "aave_v3" in caption


def test_caption_omits_protocol_when_absent() -> None:
    p_st, p_pnl, p_nav, p_cost, p_pos, p_tape = _patch_sections()
    with p_st as mock_st, p_pnl, p_nav, p_cost, p_pos, p_tape:
        basic.render_basic_dashboard("dep", {"chain": "bnb"}, api_client=None, session_state={})

    caption = mock_st.caption.call_args.args[0]
    assert "bnb" in caption
    assert caption.endswith("**bnb**")  # no trailing " · <protocol>" segment


def test_protocol_falls_back_to_alternate_config_keys() -> None:
    p_st, p_pnl, p_nav, p_cost, p_pos, p_tape = _patch_sections()
    with p_st as mock_st, p_pnl, p_nav, p_cost, p_pos, p_tape:
        basic.render_basic_dashboard(
            "dep", {"chain": "arbitrum", "teardown_protocol": "gmx_v2"}, api_client=None, session_state={}
        )

    assert "gmx_v2" in mock_st.caption.call_args.args[0]


def test_none_config_defaults_chain_placeholder() -> None:
    p_st, p_pnl, p_nav, p_cost, p_pos, p_tape = _patch_sections()
    with p_st as mock_st, p_pnl as pnl, p_nav, p_cost, p_pos, p_tape:
        basic.render_basic_dashboard("dep", None, api_client=None, session_state={})  # type: ignore[arg-type]

    # Falsy config must not crash; chain renders the em-dash placeholder.
    assert "—" in mock_st.caption.call_args.args[0]
    pnl.assert_called_once_with("dep")


def test_safe_degrades_panel_on_exception() -> None:
    boom = RuntimeError("gateway hiccup")
    p_st, p_pnl, p_nav, p_cost, p_pos, p_tape = _patch_sections()
    with p_st as mock_st, p_pnl as pnl, p_nav, p_cost, p_pos, p_tape:
        pnl.side_effect = boom  # first panel raises
        # Must NOT propagate — the page keeps rendering the remaining panels.
        basic.render_basic_dashboard("dep", {"chain": "base"}, api_client=None, session_state={})

    # Degraded panel surfaced an info banner instead of crashing the page.
    assert mock_st.info.call_count == 1
    assert "temporarily unavailable" in mock_st.info.call_args.args[0]


def test_safe_passes_through_on_success() -> None:
    calls: list[str] = []
    with patch.object(basic, "st"):
        basic._safe("label", lambda *a, **k: calls.append("ran"), 1, kw=2)
    assert calls == ["ran"]


# Perp demos deliberately WRAP the shared renderer (include_perp_section=True —
# VIB-5942 twin-drift fix) instead of re-exporting it identically; their wiring
# contract is pinned in tests/unit/dashboard/test_perp_demo_dashboard_wiring_vib5942.py.
_PERP_WRAPPED_DEMOS = {"gmx_perp_lifecycle", "gmx_v2_directional_perp"}


@pytest.mark.parametrize("demo", DEMOS)
def test_demo_ui_reexports_shared_renderer(demo: str) -> None:
    ui_path = _REPO_ROOT / "almanak" / "demo_strategies" / demo / "dashboard" / "ui.py"
    spec = importlib.util.spec_from_file_location(f"ui_{demo}", ui_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    if demo in _PERP_WRAPPED_DEMOS:
        # Wrapper, not identity: same discovery interface, perp section opted in.
        assert module.render_custom_dashboard is not basic.render_basic_dashboard
        assert callable(module.render_custom_dashboard)
    else:
        assert module.render_custom_dashboard is basic.render_basic_dashboard
