"""Recorded state and missing measurements remain distinct in the V4 view."""

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from almanak.framework.dashboard.custom.loader import load_dashboard_module
from almanak.framework.state.strategy_state import STRATEGY_USER_STATE_KEY


@pytest.mark.parametrize("wrapped", [False, True])
def test_dashboard_reads_current_user_state_and_preserves_measured_zero(monkeypatch, wrapped):
    folder = Path(__file__).resolve().parents[3] / "strategies/experiments/quant_v4_robinhood_lifecycle"
    ui = load_dashboard_module(folder / "dashboard", "v4-state-test", use_cache=False)
    streamlit = MagicMock()
    monkeypatch.setattr(ui, "st", streamlit)
    for name in (
        "render_pnl_section",
        "render_position_lifecycle_section",
        "render_cost_stack_section",
        "render_trade_tape_section",
    ):
        monkeypatch.setattr(ui, name, MagicMock())
    client = MagicMock()
    state = {"phase": "done", "liquidity": "0", "position_id": None, "error": None}
    client.get_state.return_value = {STRATEGY_USER_STATE_KEY: state, "phase": "lp_open"} if wrapped else state
    client.get_balance.side_effect = [0.0, None]
    ui.render_custom_dashboard(
        "deployment:test", json.loads((folder / "config.json").read_text()), client, {"phase": "lp_open"}
    )
    rows = [row for call in streamlit.table.call_args_list for row in call.args[0]]
    assert {"Field": "Phase", "Value": "done"} in rows
    assert {"Field": "Liquidity (raw)", "Value": "0"} in rows
    assert {"Field": "Phase", "Value": "lp_open"} not in rows
    assert [row["Balance"] for row in rows if "Balance" in row] == ["0.0", "unmeasured"]
    client.get_state.return_value = {}
    client.get_balance.side_effect = [None, None]
    ui.render_custom_dashboard("deployment:test", json.loads((folder / "config.json").read_text()), client, state)
    streamlit.info.assert_called_with("Strategy state is unmeasured: no gateway state is available.")
