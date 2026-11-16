"""Shared reports remain available without providers, credentials or a custom hook."""

import json
import sqlite3
from types import SimpleNamespace

import httpx
import pytest

from almanak import reports
from almanak.strategy import StrategyDefinition
from tests.test_new_reference_replay import NAMES, proof
from tests.uniswap_support import live_state


def retain(root, state):
    with sqlite3.connect(root / "state.sqlite") as database:
        database.execute("CREATE TABLE checkpoint(id INTEGER PRIMARY KEY, body TEXT)")
        database.execute("INSERT INTO checkpoint VALUES(1, ?)", (state.model_dump_json(),))


@pytest.mark.parametrize("name", ["aave", "trading", "lp", *NAMES])
def test_all_seven_shared_reports_render_retained_financial_scope(
    tmp_path, recorded, name, monkeypatch
):
    def forbidden(*_, **__):
        raise AssertionError("offline report attempted a provider request")

    monkeypatch.setattr(httpx.Client, "send", forbidden)
    state = (
        recorded
        if name == "aave"
        else live_state(name)
        if name in ("trading", "lp")
        else proof(name)
    )
    retain(tmp_path, state)
    before = (tmp_path / "state.sqlite").read_bytes()
    projection = reports.project(tmp_path)
    html = reports.render_html(projection)
    assert "Recorded evidence only" in html and "gas" in html
    assert "eip155:" in html and "Strategy report" in html
    assert (tmp_path / "state.sqlite").read_bytes() == before


def test_failed_custom_hook_leaves_shared_report_with_diagnostic(tmp_path, recorded):
    state = recorded.model_copy(
        update={
            "strategy": StrategyDefinition(
                filename="strategy.py",
                source="def dashboard(ctx):\n    raise RuntimeError('secret')\n",
            )
        }
    )
    retain(tmp_path, state)
    custom, diagnostic = reports.custom_dashboard(tmp_path)
    assert custom.status == "unavailable" and "secret" not in diagnostic
    html = reports.render_html(reports.project(tmp_path), custom=custom, diagnostic=diagnostic)
    assert "dashboard.py raised RuntimeError" in html and "eip155:" in html


def test_uniswap_custom_hook_receives_scoped_exchange_inventory(tmp_path):
    source = """
from almanak.dashboard import Dashboard, Metric
def dashboard(ctx):
    return Dashboard(title='Exchange', blocks=(Metric(
        id='inventory', label=ctx.positions[0].allocation, value=ctx.positions[0].inventory[0]
    ),))
"""
    state = live_state().model_copy(
        update={"strategy": StrategyDefinition(filename="strategy.py", source=source)}
    )
    retain(tmp_path, state)
    custom, diagnostic = reports.custom_dashboard(tmp_path)
    assert custom.status == "ready", diagnostic
    metric = custom.document.blocks[0]
    assert metric.value.scope.target == state.owned[0].target


def test_missing_values_and_html_content_are_not_interpreted_as_markup():
    body = reports.render_html(
        {
            "evidence": "Retained",
            "source": "<script>unsafe</script>",
            "balances": [{"asset": "eip155:1/slip44:60", "amount": None}],
            "blocked_chains": [{"chain": "eip155:43114", "reason": "missing risk history"}],
        }
    )
    assert "Not measured" in body and "missing risk history" in body
    assert "<script>unsafe" not in body
    assert "&lt;script&gt;" in body


def test_backtest_report_retains_frozen_assumptions_and_pending_work(tmp_path):
    with sqlite3.connect(tmp_path / "state.sqlite") as database:
        database.execute("CREATE TABLE checkpoint(id INTEGER PRIMARY KEY, body TEXT)")
        database.execute(
            "INSERT INTO checkpoint VALUES(1, ?)",
            (
                json.dumps(
                    {
                        "mode": "running",
                        "run_id": "simulated",
                        "time": 10,
                        "pending": {"intent": "retained"},
                        "portfolio": {},
                    }
                ),
            ),
        )
    (tmp_path / "manifest.json").write_text(json.dumps({"settings": {"costs": "excluded"}}))
    projection = reports.project(tmp_path)
    assert projection["pending"] and projection["assumptions"] == {"costs": "excluded"}
    assert not projection["closed"]
    output = tmp_path / "report.html"
    reports.command(SimpleNamespace(product="report", run=tmp_path, output=output, format="html"))
    assert output.exists()


USDG = "eip155:4663/erc20:0x5fc5360d0400a0fd4f2af552add042d716f1d168"
VALUED = {
    "evidence": "Retained",
    "source": "run",
    "asset_metadata": {USDG: {"decimals": 6, "symbol": "USDG"}},
    "valuation_quote": "USDG",
    "metrics": {"total_return_pct": -0.466, "max_drawdown_pct": 3.2, "sharpe_ratio": None},
    "performance": {
        "initial_value": "500.000000",
        "final_value": "497.669556",
        "net_value": "-2.330444",
        "fees_value": "7.211394",
        "swap_cost_value": "0.610000",
        "price_effect_value": "-8.931838",
        "swaps": 4,
        "window_seconds": 14 * 86400,
        "time_in_market_seconds": 7 * 86400 + 300,
        "time_in_range_seconds": 5 * 3600 + 120,
        "positions": [
            {
                "allocation": "position",
                "opened_at": 1789171505,
                "closed_at": None,
                "price_range": ["143.04432", "157.77179"],
                "deposited": [{"asset": USDG, "amount": "251586272", "unit": "base_units"}],
                "withdrawn": [{"asset": USDG, "amount": "0", "unit": "base_units"}],
                "fees": [{"asset": USDG, "amount": "1425410", "unit": "base_units"}],
                "fees_value": "2.897000",
                "held_seconds": 45,
                "in_range_seconds": 0,
                "price_at_close": None,
            }
        ],
    },
}


def test_a_valued_report_leads_with_its_result_and_its_liquidity_positions():
    body = reports.render_html(VALUED)
    for text in (
        "<span>Net return</span><strong>-0.47%</strong>",
        "<span>Net result</span><strong>-2.330444 USDG</strong>",
        "<h2>Performance</h2>",
        "<dt>fees earned</dt><dd>7.211394 USDG</dd>",
        "<dt>largest drawdown</dt><dd>3.20%</dd>",
        "<dt>time in market</dt><dd>7d of 14d</dd>",
        "<dt>time in range</dt><dd>5h 2m</dd>",
        "<h2>Liquidity positions</h2>",
        "<td>2026-09-12 00:05 UTC</td><td>open</td>",
        "143.04432 to 157.77179 USDG",
        "251.586272 USDG",
        "<td>0m</td><td>0m</td><td>open</td>",
    ):
        assert text in body
    # A report without a valuation keeps its sections and cards as they were.
    plain = reports.render_html({"evidence": "Retained", "source": "run"})
    assert "Performance" not in plain and "Net return" not in plain


def test_a_finished_backtest_projects_its_report_once(tmp_path):
    with sqlite3.connect(tmp_path / "state.sqlite") as database:
        database.execute("CREATE TABLE checkpoint(id INTEGER PRIMARY KEY, body TEXT)")
        body = {"mode": "finished", "run_id": "simulated", "time": 10, "portfolio": {}}
        database.execute("INSERT INTO checkpoint VALUES(1, ?)", (json.dumps(body),))
    (tmp_path / "report.json").write_text(json.dumps({"closed": True, "metrics": {"x": 1}}))
    finished = reports.project(tmp_path)
    assert finished["closed"] and finished["metrics"] == {"x": 1}
    assert "last_completed_report" not in finished
    with sqlite3.connect(tmp_path / "state.sqlite") as database:
        database.execute("UPDATE checkpoint SET body=?", (json.dumps({**body, "mode": "running"}),))
    running = reports.project(tmp_path)
    assert running["last_completed_report"] == {"closed": True, "metrics": {"x": 1}}
    assert "metrics" not in running and not running["closed"]


def test_support_projection_matches_admission_declarations():
    from almanak.connectors.composition import connector_from_wire

    for name, declared in reports.support().items():
        bound = connector_from_wire({"connector": name})
        lifecycle = bound.execution.gas_lifecycle.model_dump(mode="json")["kinds"]
        assert declared == [
            item.model_dump(mode="json") | {"gas_lifecycle": lifecycle}
            for item in bound.chain_support
        ]


def test_terminal_report_keeps_missing_values_and_nested_scope_readable():
    from almanak.presentation import render_text

    rendered = render_text({"accounts": [{"fund": "main", "balance": None}], "pending": []})
    assert "fund: main" in rendered and "balance: not measured" in rendered
    assert '{"' not in rendered
