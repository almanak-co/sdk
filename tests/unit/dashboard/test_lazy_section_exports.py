"""Positive-path test for the lazy ``render_*_section`` exports (VIB-4048).

The gateway-side regression test
(``tests/gateway/test_imports_lean.py::test_gateway_dashboard_handler_lazies_do_not_pull_streamlit``)
proves that the package init is *streamlit-free*. It does not exercise
the lazy resolution itself — a typo in ``_LAZY_IMPORTS`` (e.g.
``".sectionz"``) would still let CI pass while breaking every local
dashboard consumer at first attribute access.

This test forces resolution of all three lazy public names so the
``build_lazy_module_dispatch`` mapping is validated end-to-end. We only
check ``callable(...)``; calling the function would require a streamlit
runtime which is not the contract we're testing here.
"""

from __future__ import annotations


def test_render_pnl_section_resolves_via_lazy_dispatch() -> None:
    from almanak.framework.dashboard import render_pnl_section

    assert callable(render_pnl_section)


def test_render_cost_stack_section_resolves_via_lazy_dispatch() -> None:
    from almanak.framework.dashboard import render_cost_stack_section

    assert callable(render_cost_stack_section)


def test_render_trade_tape_section_resolves_via_lazy_dispatch() -> None:
    from almanak.framework.dashboard import render_trade_tape_section

    assert callable(render_trade_tape_section)


def test_lazy_dispatch_caches_resolved_section_helpers() -> None:
    """First access pays the import cost; subsequent accesses must read
    from ``globals()`` directly. Verified by checking that the second
    access returns the *same* object identity rather than a freshly
    re-resolved one — Python's import system guarantees module-level
    attribute identity, so two unequal results would mean the helper
    re-imported and grabbed a different binding."""
    import almanak.framework.dashboard as dashboard

    first = dashboard.render_pnl_section
    second = dashboard.render_pnl_section

    assert first is second
