"""Regression coverage for the Golden-demo dashboard chart + trade-tape sections.

The overnight-report dashboard captures are only useful if every showcased
Golden demo actually renders the two backbone sections an auditor reconstructs
from: the NAV/PnL history chart (``render_nav_history_section``) and the Trade
Tape (``render_trade_tape_section``). These were added to the LP/lender demos
whose custom ``ui.py`` previously drew neither; this test locks that in so a
later edit can't silently drop the chart or the tape from a demo we ship.

Each demo's ``dashboard/ui.py`` is loaded by file path (the dashboard dirs are
not importable packages), its Streamlit + framework section calls are replaced
with mocks, and ``render_custom_dashboard`` is invoked to assert both backbone
sections fire for the live deployment id.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType
from unittest.mock import MagicMock

import pytest

# The Golden demos whose custom dashboards must render the NAV chart + Trade
# Tape backbone (relative to the repo's demo_strategies package).
_DEMOS = ["traderjoe_lp", "curve_3pool_lp", "spark_lender"]

_REPO_ROOT = Path(__file__).resolve().parents[3]
_DEMO_ROOT = _REPO_ROOT / "almanak" / "demo_strategies"

# Framework dashboard section functions each demo imports into its own module
# namespace; we replace them with mocks to avoid any real gateway/render work.
_SECTION_FUNCS = (
    "render_pnl_section",
    "render_nav_history_section",
    "render_cost_stack_section",
    "render_trade_tape_section",
)


def _load_ui(demo: str) -> ModuleType:
    path = _DEMO_ROOT / demo / "dashboard" / "ui.py"
    spec = importlib.util.spec_from_file_location(f"_demo_ui_{demo}", path)
    assert spec and spec.loader, f"cannot load {path}"
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _fake_streamlit() -> MagicMock:
    """A Streamlit stand-in whose ``columns`` yields context-manager mocks."""
    st = MagicMock()

    def _columns(spec, **_kwargs):
        n = spec if isinstance(spec, int) else len(spec)
        return [MagicMock() for _ in range(n)]

    st.columns.side_effect = _columns
    return st


@pytest.mark.parametrize("demo", _DEMOS)
def test_golden_demo_renders_nav_chart_and_trade_tape(demo: str, monkeypatch) -> None:
    module = _load_ui(demo)

    # Every demo must import both backbone sections into its namespace.
    assert hasattr(module, "render_nav_history_section"), f"{demo} ui.py must import render_nav_history_section"
    assert hasattr(module, "render_trade_tape_section"), f"{demo} ui.py must import render_trade_tape_section"

    mocks = {name: MagicMock() for name in _SECTION_FUNCS}
    for name, mock in mocks.items():
        monkeypatch.setattr(module, name, mock)
    monkeypatch.setattr(module, "st", _fake_streamlit())

    deployment_id = "deployment:abc123def456"
    module.render_custom_dashboard(
        deployment_id=deployment_id,
        strategy_config={},
        api_client=MagicMock(),
        session_state={},
    )

    # The NAV/PnL history chart must render for the live deployment, spanning
    # the whole run ("All"), and the Trade Tape must render too.
    mocks["render_nav_history_section"].assert_called_once()
    nav_args, nav_kwargs = mocks["render_nav_history_section"].call_args
    assert deployment_id in nav_args
    assert nav_kwargs.get("default_range") == "All"

    mocks["render_trade_tape_section"].assert_called_once()
    tape_args, _ = mocks["render_trade_tape_section"].call_args
    assert deployment_id in tape_args
