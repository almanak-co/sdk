"""VIB-5942 twin-drift regression: perp DEMOS must render the perp story section.

PR #3358 wired ``render_perp_positions_section`` into the incubating
gmx_perp_lifecycle twin and the perp template, but the demo_strategies twins —
what ``almanak strat demo`` users actually run — stayed on the plain basic
layout, so live runs showed no direction/market/leverage (batch 20260722-0913,
both GMX legs). These tests pin the wiring at both layers so the drift cannot
silently return.
"""

from unittest.mock import MagicMock, patch

import almanak.framework.dashboard.custom.basic as basic


def _run_basic(**kwargs):
    with (
        patch.object(basic, "render_pnl_section") as pnl,
        patch.object(basic, "render_perp_positions_section") as perp,
        patch.object(basic, "render_nav_history_section"),
        patch.object(basic, "render_cost_stack_section"),
        patch.object(basic, "render_position_lifecycle_section"),
        patch.object(basic, "render_trade_tape_section"),
        patch.object(basic, "st", MagicMock()),
    ):
        basic.render_basic_dashboard("deployment:test", {"chain": "arbitrum"}, MagicMock(), {}, **kwargs)
        return pnl, perp


class TestBasicDashboardPerpFlag:
    def test_default_does_not_render_perp_section(self):
        # Non-perp demos share this layout — the perp header must not appear.
        _, perp = _run_basic()
        perp.assert_not_called()

    def test_optin_renders_perp_section_once(self):
        pnl, perp = _run_basic(include_perp_section=True)
        pnl.assert_called_once_with("deployment:test")
        perp.assert_called_once_with("deployment:test")


class TestPerpDemoWiring:
    def test_gmx_demos_opt_in(self):
        # Both GMX demo ui.py wrappers must pass include_perp_section=True.
        import importlib

        for mod in (
            "almanak.demo_strategies.gmx_perp_lifecycle.dashboard.ui",
            "almanak.demo_strategies.gmx_v2_directional_perp.dashboard.ui",
        ):
            ui = importlib.import_module(mod)
            with patch.object(basic, "render_basic_dashboard") as rb:
                # The wrapper may reference the symbol directly; patch at source and via module.
                with patch(f"{mod}.render_basic_dashboard", rb):
                    ui.render_custom_dashboard("deployment:test", {}, MagicMock(), {})
            assert rb.call_args.kwargs.get("include_perp_section") is True, mod
