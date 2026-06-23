"""Unit tests for the Phase 5A.2 pretty-print helpers in
`almanak/framework/cli/status_helpers.py`.

Covers the pretty-print path of `strat status`:
    - _print_summary_header
    - _print_operator_card
    - _print_legacy_position
    - _format_strategy_position_size_line
    - _format_strategy_position_pnl_line (incl. silent-0.0 fallback, #1697)
    - _print_strategy_positions
    - _print_chain_health
    - _print_timeline
    - _render_details_pretty (end-to-end composition)

Output is captured via `click.testing.CliRunner` wrapping a one-line command
so ANSI stripping is deterministic. Some low-level helpers that do not use
Click runtime context are verified via `capsys` to keep tests focused.

The fakes mirror the shape of the gateway proto messages (SimpleNamespace
with duck-typed attributes) — consistent with `test_status_helpers_json.py`.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import click
import pytest
from click.testing import CliRunner

from almanak.framework.cli import status_helpers

# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


def _make_summary(**overrides: Any) -> SimpleNamespace:
    """Build a minimal `StrategySummary`-like object for pretty-print.

    Mirrors the proto fields actually read by the pretty-print path:
    deployment_id, name, status, chain(s), is_multi_chain, protocol,
    total_value_usd, pnl_24h_usd, pnl_since_deploy_usd, last_action_at,
    last_iteration_at, consecutive_errors, attention_required,
    attention_reason.
    """
    defaults = {
        "deployment_id": "demo",
        "name": "Demo Strategy",
        "status": "RUNNING",
        "chain": "arbitrum",
        "chains": ["arbitrum"],
        "is_multi_chain": False,
        "protocol": "uniswap_v3",
        "total_value_usd": "1000.00",
        "pnl_24h_usd": "12.34",
        "last_action_at": 1_700_000_000,
        "attention_required": False,
        "attention_reason": "",
        "consecutive_errors": 0,
        "last_iteration_at": 1_700_000_500,
        "pnl_since_deploy_usd": "",
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _make_operator_card(**overrides: Any) -> SimpleNamespace:
    defaults = {
        "deployment_id": "demo",
        "severity": "HIGH",
        "reason": "Stuck",
        "risk_description": "",
        "suggested_actions": [],
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _make_strategy_position(**overrides: Any) -> SimpleNamespace:
    defaults = {
        "position_type": "PERP",
        "position_id": "ETH-PERP",
        "chain": "arbitrum",
        "protocol": "gmx_v2",
        "value_usd": "500.00",
        "liquidation_risk": False,
        "direction": "",
        "entry_price": "",
        "current_price": "",
        "unrealized_pnl_usd": "",
        "unrealized_pnl_pct": "",
        "size_usd": "",
        "collateral_usd": "",
        "leverage": "",
        "health_factor": "",
        "details": {},
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _make_position(
    *,
    token_balances: list | None = None,
    lp_positions: list | None = None,
    health_factor: Any = None,
    strategy_positions: list | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        token_balances=list(token_balances or []),
        lp_positions=list(lp_positions or []),
        health_factor=health_factor,
        strategy_positions=list(strategy_positions or []),
    )


def _make_details(
    *,
    summary: SimpleNamespace | None = None,
    position: Any = None,
    timeline: list | None = None,
    chain_health: dict | None = None,
    operator_card: SimpleNamespace | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        summary=summary or _make_summary(),
        position=position,
        timeline=list(timeline or []),
        chain_health=dict(chain_health or {}),
        operator_card=operator_card,
    )


def _invoke(runner: CliRunner, func, *args, **kwargs) -> str:
    """Run a helper inside a tiny click command so color codes are stripped.

    `CliRunner().invoke(..., color=False)` ensures ANSI escape sequences are
    stripped from captured output — making substring assertions deterministic.
    """

    @click.command()
    def _cmd() -> None:
        func(*args, **kwargs)

    result = runner.invoke(_cmd, color=False)
    assert result.exit_code == 0, result.output
    return result.output


# ---------------------------------------------------------------------------
# _print_summary_header
# ---------------------------------------------------------------------------


def test_print_summary_header_minimal_fields() -> None:
    """Summary renders ID/Status/Chain/Protocol/Value/PnL lines verbatim."""
    runner = CliRunner()
    out = _invoke(runner, status_helpers._print_summary_header, _make_summary())
    assert "Strategy: Demo Strategy" in out
    assert "ID:          demo" in out
    assert "Status:      RUNNING" in out
    assert "Chain:       arbitrum" in out
    assert "Protocol:    uniswap_v3" in out
    assert "Value:       $1000.00" in out
    assert "PnL (24h):   $12.34" in out
    assert "PnL (total): -" in out  # empty string -> dash


def test_print_summary_header_fallback_to_deployment_id() -> None:
    """When `name` is empty, header falls back to `deployment_id`."""
    runner = CliRunner()
    out = _invoke(runner, status_helpers._print_summary_header, _make_summary(name=""))
    assert "Strategy: demo" in out


def test_print_summary_header_multi_chain() -> None:
    """Multi-chain summary joins `chains` with commas."""
    runner = CliRunner()
    s = _make_summary(is_multi_chain=True, chains=["arbitrum", "base"])
    out = _invoke(runner, status_helpers._print_summary_header, s)
    assert "Chain:       arbitrum,base" in out


def test_print_summary_header_none_value_renders_dash() -> None:
    """Value and PnL (24h) render '-' when proto field is None."""
    runner = CliRunner()
    s = _make_summary(total_value_usd=None, pnl_24h_usd=None)
    out = _invoke(runner, status_helpers._print_summary_header, s)
    assert "Value:       -" in out
    assert "PnL (24h):   -" in out


def test_print_summary_header_last_iter_only_when_set() -> None:
    """Last Iter line only emitted when `last_iteration_at` is truthy."""
    runner = CliRunner()
    out = _invoke(runner, status_helpers._print_summary_header, _make_summary(last_iteration_at=0))
    assert "Last Iter:" not in out


def test_print_summary_header_errors_only_when_nonzero() -> None:
    """Errors line only emitted when `consecutive_errors` is truthy."""
    runner = CliRunner()
    out_no_err = _invoke(runner, status_helpers._print_summary_header, _make_summary())
    assert "Errors:" not in out_no_err

    out_err = _invoke(runner, status_helpers._print_summary_header, _make_summary(consecutive_errors=3))
    assert "Errors:      3 consecutive" in out_err


def test_print_summary_header_attention_block() -> None:
    """Attention block only appears when `attention_required` is True."""
    runner = CliRunner()
    s = _make_summary(attention_required=True, attention_reason="Stale iteration")
    out = _invoke(runner, status_helpers._print_summary_header, s)
    assert "! Stale iteration" in out


# ---------------------------------------------------------------------------
# _print_operator_card
# ---------------------------------------------------------------------------


def test_print_operator_card_noop_when_none(capsys: pytest.CaptureFixture) -> None:
    """No output when `oc` is None."""
    status_helpers._print_operator_card(None)
    assert capsys.readouterr().out == ""


def test_print_operator_card_renders_even_when_deployment_id_empty() -> None:
    """Empty `deployment_id` no longer suppresses the card (#1704).

    Previously `_print_operator_card` bailed when `oc.deployment_id` was the
    empty string, using proto3's empty-string-as-falsy default as a presence
    sentinel. That conflates "unset" with "intentionally empty" and would
    silently drop a legitimately-empty card. The #1704 fix moves presence
    decisions to the orchestrator (`_has_operator_card` via `HasField`); the
    direct helper renders whatever non-None card it receives.
    """
    runner = CliRunner()
    oc = _make_operator_card(deployment_id="", severity="LOW", reason="note")
    out = _invoke(runner, status_helpers._print_operator_card, oc)
    assert "Operator Alert [LOW]: note" in out


def test_print_operator_card_minimal() -> None:
    """Minimum card emits the header and nothing for risk/actions."""
    runner = CliRunner()
    oc = _make_operator_card(severity="MEDIUM", reason="Watchlist")
    out = _invoke(runner, status_helpers._print_operator_card, oc)
    assert "Operator Alert [MEDIUM]: Watchlist" in out
    assert "Risk:" not in out
    assert "Suggested:" not in out


def test_print_operator_card_full() -> None:
    """Full card emits Risk and Suggested actions."""
    runner = CliRunner()
    oc = _make_operator_card(
        severity="CRITICAL",
        reason="Stuck >1h",
        risk_description="Strategy may be hung",
        suggested_actions=["pause", "investigate logs"],
    )
    out = _invoke(runner, status_helpers._print_operator_card, oc)
    assert "Operator Alert [CRITICAL]: Stuck >1h" in out
    assert "Risk: Strategy may be hung" in out
    assert "Suggested:" in out
    assert "      - pause" in out
    assert "      - investigate logs" in out


# ---------------------------------------------------------------------------
# _print_legacy_position
# ---------------------------------------------------------------------------


def test_print_legacy_position_noop_when_none(capsys: pytest.CaptureFixture) -> None:
    """No output when `pos` is None."""
    status_helpers._print_legacy_position(None)
    assert capsys.readouterr().out == ""


def test_print_legacy_position_noop_when_all_empty(capsys: pytest.CaptureFixture) -> None:
    """No output when every legacy field is empty/None."""
    status_helpers._print_legacy_position(_make_position())
    assert capsys.readouterr().out == ""


def test_print_legacy_position_token_balances() -> None:
    """Token balances rendered with optional USD value."""
    runner = CliRunner()
    pos = _make_position(
        token_balances=[
            SimpleNamespace(symbol="USDC", balance="100", value_usd="100"),
            SimpleNamespace(symbol="WETH", balance="0.5", value_usd=None),
        ]
    )
    out = _invoke(runner, status_helpers._print_legacy_position, pos)
    assert "Position:" in out
    assert "USDC: 100 ($100)" in out
    # value_usd=None -> no value suffix
    assert "WETH: 0.5\n" in out or "WETH: 0.5" in out
    assert "WETH: 0.5 ($" not in out


def test_print_legacy_position_lp_and_health_factor() -> None:
    """LP positions and health factor rendered in the block."""
    runner = CliRunner()
    pos = _make_position(
        lp_positions=[
            SimpleNamespace(pool="WETH/USDC", token0="WETH", token1="USDC", liquidity_usd="2500"),
        ],
        health_factor="1.75",
    )
    out = _invoke(runner, status_helpers._print_legacy_position, pos)
    assert "LP: WETH/USDC (WETH/USDC) $2500" in out
    assert "Health Factor: 1.75" in out


# ---------------------------------------------------------------------------
# _format_strategy_position_size_line
# ---------------------------------------------------------------------------


def test_format_size_line_empty_returns_empty_string() -> None:
    """Returns '' when no size/value/collateral/leverage/HF fields set."""
    # Override the default `value_usd` ("500.00") so NO fields are populated
    sp = _make_strategy_position(value_usd="")
    assert status_helpers._format_strategy_position_size_line(sp) == ""


def test_format_size_line_size_preferred_over_value() -> None:
    """Size takes precedence over Value when both are populated."""
    sp = _make_strategy_position(size_usd="1000", value_usd="900")
    out = status_helpers._format_strategy_position_size_line(sp)
    assert out == "Size: $1000"


def test_format_size_line_value_fallback() -> None:
    """Uses Value when Size is empty."""
    sp = _make_strategy_position(size_usd="", value_usd="750")
    assert status_helpers._format_strategy_position_size_line(sp) == "Value: $750"


def test_format_size_line_full() -> None:
    """All fields join with ' | ' in the expected order."""
    sp = _make_strategy_position(
        size_usd="1000", collateral_usd="200", leverage="5", health_factor="2.1"
    )
    out = status_helpers._format_strategy_position_size_line(sp)
    assert out == "Size: $1000 | Collateral: $200 | Leverage: 5x | HF: 2.1"


# ---------------------------------------------------------------------------
# _format_strategy_position_pnl_line
# ---------------------------------------------------------------------------


def test_format_pnl_line_empty_when_all_unset() -> None:
    """Returns '' when none of entry/current/pnl is set."""
    assert status_helpers._format_strategy_position_pnl_line(_make_strategy_position()) == ""


def test_format_pnl_line_entry_only() -> None:
    """Entry-only emits just the Entry part."""
    sp = _make_strategy_position(entry_price="1000")
    out = status_helpers._format_strategy_position_pnl_line(sp)
    # Strip click color codes for assertion robustness
    assert "Entry: $1000" in out
    assert "Current:" not in out
    assert "PnL:" not in out


def test_format_pnl_line_positive_pnl_green_with_plus() -> None:
    """Positive PnL => green color, '+' prefix."""
    sp = _make_strategy_position(
        entry_price="1000",
        current_price="1100",
        unrealized_pnl_usd="50",
        unrealized_pnl_pct="5.0",
    )
    out = status_helpers._format_strategy_position_pnl_line(sp)
    # Contains the green ANSI + '+' prefix
    assert "\x1b[32m" in out  # green
    assert "+$50 (5.0%)" in out


def test_format_pnl_line_negative_pnl_red_no_prefix() -> None:
    """Negative PnL => red color, NO prefix."""
    sp = _make_strategy_position(unrealized_pnl_usd="-25")
    out = status_helpers._format_strategy_position_pnl_line(sp)
    assert "\x1b[31m" in out  # red
    # No '+' prefix for negatives — value is '-25' (sign from the value itself)
    assert "$-25" in out
    assert "+$-25" not in out


def test_format_pnl_line_zero_pnl_white_no_prefix() -> None:
    """Zero PnL => white color (default), NO prefix."""
    sp = _make_strategy_position(unrealized_pnl_usd="0")
    out = status_helpers._format_strategy_position_pnl_line(sp)
    assert "\x1b[37m" in out  # white
    assert "$0" in out
    assert "+$0" not in out


def test_format_pnl_line_invalid_pnl_renders_yellow_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Invalid PnL string renders in YELLOW and emits a logger.warning (#1697).

    Previously the silent-0.0 fallback rendered white, making a bogus value
    indistinguishable from a measured zero. The fix: log a warning and render
    the raw value in yellow so the operator sees a distinct signal.
    """
    sp = _make_strategy_position(unrealized_pnl_usd="not_a_number")
    with caplog.at_level("WARNING", logger=status_helpers.logger.name):
        out = status_helpers._format_strategy_position_pnl_line(sp)
    # Yellow ANSI -> warning signal (was white before the fix)
    assert "\x1b[33m" in out
    # Raw value preserved in the output (no silent coercion)
    assert "$not_a_number" in out
    assert "+$not_a_number" not in out
    # logger.warning must fire, with the unparseable value in the message
    warning_records = [r for r in caplog.records if r.levelname == "WARNING"]
    assert warning_records, "expected logger.warning to fire for unparseable PnL"
    assert "not_a_number" in warning_records[0].getMessage()


def test_format_pnl_line_pct_suffix_optional() -> None:
    """Percent suffix only appended when `unrealized_pnl_pct` is non-empty."""
    sp = _make_strategy_position(unrealized_pnl_usd="10", unrealized_pnl_pct="")
    out = status_helpers._format_strategy_position_pnl_line(sp)
    assert "$10" in out
    assert "(" not in out  # no parenthesized pct


# ---------------------------------------------------------------------------
# _print_strategy_positions
# ---------------------------------------------------------------------------


def test_print_strategy_positions_noop_when_empty(capsys: pytest.CaptureFixture) -> None:
    """No output for an empty positions list."""
    status_helpers._print_strategy_positions([])
    assert capsys.readouterr().out == ""


def test_print_strategy_positions_header_and_direction() -> None:
    """Header line includes type, optional direction, id, protocol, chain."""
    runner = CliRunner()
    sp = _make_strategy_position(direction="LONG")
    out = _invoke(runner, status_helpers._print_strategy_positions, [sp])
    assert "Positions:" in out
    assert "PERP LONG ETH-PERP (gmx_v2) on arbitrum" in out


def test_print_strategy_positions_header_no_direction() -> None:
    """Header skips direction when empty string."""
    runner = CliRunner()
    sp = _make_strategy_position()  # direction=""
    out = _invoke(runner, status_helpers._print_strategy_positions, [sp])
    assert "PERP ETH-PERP (gmx_v2) on arbitrum" in out
    # Ensure no double-space before position_id (i.e. no missing-field artifact)
    assert "PERP  ETH-PERP" not in out


def test_print_strategy_positions_liquidation_risk_flag() -> None:
    """Liquidation risk warning emitted when flag is True."""
    runner = CliRunner()
    sp = _make_strategy_position(liquidation_risk=True)
    out = _invoke(runner, status_helpers._print_strategy_positions, [sp])
    assert "! Liquidation risk" in out


# ---------------------------------------------------------------------------
# _print_chain_health
# ---------------------------------------------------------------------------


def test_print_chain_health_noop_when_empty(capsys: pytest.CaptureFixture) -> None:
    """No output for an empty chain_health map."""
    status_helpers._print_chain_health({})
    assert capsys.readouterr().out == ""


def test_print_chain_health_populated() -> None:
    """Each chain emits a status/RPC/gas line."""
    runner = CliRunner()
    health = {
        "arbitrum": SimpleNamespace(status="HEALTHY", rpc_latency_ms=42, gas_price_gwei="0.1"),
        "base": SimpleNamespace(status="DEGRADED", rpc_latency_ms=500, gas_price_gwei="0.05"),
    }
    out = _invoke(runner, status_helpers._print_chain_health, health)
    assert "Chain Health:" in out
    assert "arbitrum: HEALTHY (RPC: 42ms, gas: 0.1 gwei)" in out
    assert "base: DEGRADED (RPC: 500ms, gas: 0.05 gwei)" in out


# ---------------------------------------------------------------------------
# _print_timeline
# ---------------------------------------------------------------------------


def test_print_timeline_noop_when_empty(capsys: pytest.CaptureFixture) -> None:
    """No output for an empty events list."""
    status_helpers._print_timeline([])
    assert capsys.readouterr().out == ""


def test_print_timeline_truncates_tx_hash() -> None:
    """tx_hash is truncated to first 10 chars followed by '...'."""
    runner = CliRunner()
    events = [
        SimpleNamespace(
            timestamp=1_700_000_000,
            event_type="TRADE",
            description="Bought ETH",
            tx_hash="0x1234567890abcdef",
            chain="arbitrum",
        )
    ]
    out = _invoke(runner, status_helpers._print_timeline, events)
    assert "Recent Events:" in out
    assert "TRADE" in out
    assert "Bought ETH" in out
    assert "tx:0x12345678..." in out


def test_print_timeline_emits_each_event() -> None:
    """All events from the list are emitted (N events -> N lines)."""
    runner = CliRunner()
    events = [
        SimpleNamespace(
            timestamp=1_700_000_000 + i,
            event_type="TRADE",
            description=f"event #{i}",
            tx_hash="",
            chain="arbitrum",
        )
        for i in range(3)
    ]
    out = _invoke(runner, status_helpers._print_timeline, events)
    for i in range(3):
        assert f"event #{i}" in out


# ---------------------------------------------------------------------------
# _render_details_pretty — end-to-end composition
# ---------------------------------------------------------------------------


def test_render_details_pretty_minimal_summary_only() -> None:
    """With no optional sections, only summary block + trailing blank line."""
    runner = CliRunner()
    details = _make_details(position=None, timeline=[], chain_health={}, operator_card=None)
    out = _invoke(runner, status_helpers._render_details_pretty, details, True)
    assert "Strategy: Demo Strategy" in out
    assert "Operator Alert" not in out
    assert "Position:" not in out
    assert "Positions:" not in out
    assert "Chain Health:" not in out
    assert "Recent Events:" not in out


def test_render_details_pretty_timeline_disabled_omits_events() -> None:
    """`timeline_enabled=False` suppresses timeline even when events exist."""
    runner = CliRunner()
    events = [
        SimpleNamespace(
            timestamp=1_700_000_000,
            event_type="TRADE",
            description="Buy 1 ETH",
            tx_hash="",
            chain="arbitrum",
        )
    ]
    details = _make_details(timeline=events)
    out = _invoke(runner, status_helpers._render_details_pretty, details, False)
    assert "Recent Events:" not in out
    assert "Buy 1 ETH" not in out


def test_render_details_pretty_kitchen_sink_section_order() -> None:
    """All sections present => expected section order from the original."""
    runner = CliRunner()
    pos = _make_position(
        token_balances=[SimpleNamespace(symbol="USDC", balance="100", value_usd="100")],
        strategy_positions=[_make_strategy_position(direction="LONG")],
    )
    events = [
        SimpleNamespace(
            timestamp=1_700_000_000,
            event_type="TRADE",
            description="Buy 1 ETH",
            tx_hash="",
            chain="arbitrum",
        )
    ]
    health = {"arbitrum": SimpleNamespace(status="HEALTHY", rpc_latency_ms=1, gas_price_gwei="0.1")}
    oc = _make_operator_card(severity="HIGH", reason="Stuck", deployment_id="demo")
    details = _make_details(
        position=pos, timeline=events, chain_health=health, operator_card=oc
    )
    out = _invoke(runner, status_helpers._render_details_pretty, details, True)

    # Check that sections appear in the expected order by index ordering.
    idx_strategy = out.index("Strategy: Demo Strategy")
    idx_operator = out.index("Operator Alert [HIGH]")
    idx_legacy = out.index("Position:")
    idx_positions = out.index("Positions:")
    idx_chain = out.index("Chain Health:")
    idx_events = out.index("Recent Events:")
    assert idx_strategy < idx_operator < idx_legacy < idx_positions < idx_chain < idx_events


def test_render_details_pretty_position_none_safe() -> None:
    """`details.position = None` does not raise in strategy_positions path."""
    runner = CliRunner()
    details = _make_details(position=None)
    # Should not raise
    out = _invoke(runner, status_helpers._render_details_pretty, details, True)
    assert "Strategy: Demo Strategy" in out


# ---------------------------------------------------------------------------
# Phase 5A.3 — extended coverage: _format_strategy_position_pnl_line branches
# ---------------------------------------------------------------------------


def test_format_pnl_line_current_only_no_color() -> None:
    """Current-only input skips the PnL color logic entirely."""
    sp = _make_strategy_position(current_price="1200")
    out = status_helpers._format_strategy_position_pnl_line(sp)
    assert out == "Current: $1200"
    # No PnL part -> no ANSI color codes
    assert "\x1b[" not in out


def test_format_pnl_line_entry_and_current_only() -> None:
    """Entry + current (no PnL) joins with ' | ' and no coloring."""
    sp = _make_strategy_position(entry_price="1000", current_price="1100")
    out = status_helpers._format_strategy_position_pnl_line(sp)
    assert out == "Entry: $1000 | Current: $1100"
    assert "PnL:" not in out


def test_format_pnl_line_negative_float_string() -> None:
    """Negative-float string triggers the `pnl_num < 0` branch (red, no prefix)."""
    sp = _make_strategy_position(unrealized_pnl_usd="-0.01")
    out = status_helpers._format_strategy_position_pnl_line(sp)
    assert "\x1b[31m" in out  # red
    assert "$-0.01" in out
    assert "+$" not in out


def test_format_pnl_line_positive_float_string_with_pct() -> None:
    """Positive float + pct renders the full `+$val (pct%)` block in green."""
    sp = _make_strategy_position(
        unrealized_pnl_usd="2.5",
        unrealized_pnl_pct="0.25",
    )
    out = status_helpers._format_strategy_position_pnl_line(sp)
    assert "\x1b[32m" in out  # green
    assert "+$2.5 (0.25%)" in out


def test_format_pnl_line_zero_float_string_with_pct_no_prefix() -> None:
    """Zero PnL with pct still emits the pct suffix but no '+' prefix."""
    sp = _make_strategy_position(
        unrealized_pnl_usd="0.0",
        unrealized_pnl_pct="0.0",
    )
    out = status_helpers._format_strategy_position_pnl_line(sp)
    assert "\x1b[37m" in out  # white
    assert "$0.0 (0.0%)" in out
    assert "+$" not in out


def test_format_pnl_line_none_value_invalid_fallback(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """`None` for `unrealized_pnl_usd` exercises the TypeError branch (#1697).

    The guard `sp.unrealized_pnl_usd != ""` is True for None, so the PnL
    branch runs. `float(None)` raises TypeError, which now routes to the
    yellow-warning path (was silent-0.0/white before the fix).
    """
    sp = _make_strategy_position(unrealized_pnl_usd=None)
    with caplog.at_level("WARNING", logger=status_helpers.logger.name):
        out = status_helpers._format_strategy_position_pnl_line(sp)
    # TypeError -> yellow warning color
    assert "\x1b[33m" in out
    # The raw `None` value is formatted into the output literal
    assert "$None" in out
    warning_records = [r for r in caplog.records if r.levelname == "WARNING"]
    assert warning_records, "expected logger.warning for TypeError fallback"


def test_format_pnl_line_value_error_branch(caplog: pytest.LogCaptureFixture) -> None:
    """Explicitly exercise the ValueError branch of the try/except (#1697).

    Previously fell back silently to 0.0/white. Now renders yellow with a
    warning log so operators can tell a parse failure from a real zero.
    """
    sp = _make_strategy_position(unrealized_pnl_usd="abc-not-a-num")
    with caplog.at_level("WARNING", logger=status_helpers.logger.name):
        out = status_helpers._format_strategy_position_pnl_line(sp)
    assert "\x1b[33m" in out  # yellow warning color
    assert "$abc-not-a-num" in out
    warning_records = [r for r in caplog.records if r.levelname == "WARNING"]
    assert warning_records, "expected logger.warning for ValueError fallback"
    assert "abc-not-a-num" in warning_records[0].getMessage()


# ---------------------------------------------------------------------------
# Phase 5A.3 — extended coverage: _format_strategy_position_size_line branches
# ---------------------------------------------------------------------------


def test_format_size_line_only_collateral() -> None:
    """Only collateral present -> single-segment line."""
    sp = _make_strategy_position(value_usd="", collateral_usd="50")
    out = status_helpers._format_strategy_position_size_line(sp)
    assert out == "Collateral: $50"


def test_format_size_line_only_leverage() -> None:
    """Only leverage present -> single-segment line."""
    sp = _make_strategy_position(value_usd="", leverage="10")
    out = status_helpers._format_strategy_position_size_line(sp)
    assert out == "Leverage: 10x"


def test_format_size_line_only_health_factor() -> None:
    """Only health_factor present -> single-segment line."""
    sp = _make_strategy_position(value_usd="", health_factor="2.0")
    out = status_helpers._format_strategy_position_size_line(sp)
    assert out == "HF: 2.0"


def test_format_size_line_size_plus_hf() -> None:
    """Size + HF (skipping collateral/leverage) joins with ' | '."""
    sp = _make_strategy_position(size_usd="1000", health_factor="1.5")
    out = status_helpers._format_strategy_position_size_line(sp)
    assert out == "Size: $1000 | HF: 1.5"


# ---------------------------------------------------------------------------
# Phase 5A.3 — extended coverage: _print_strategy_positions (line 450, 445->448)
# ---------------------------------------------------------------------------


def test_print_strategy_positions_pnl_line_emitted_when_populated() -> None:
    """`pnl_line` is non-empty => inner echo on line 450 fires."""
    runner = CliRunner()
    sp = _make_strategy_position(
        entry_price="1000",
        current_price="1100",
        unrealized_pnl_usd="100",
        unrealized_pnl_pct="10.0",
    )
    out = _invoke(runner, status_helpers._print_strategy_positions, [sp])
    # Pretty layout: indented 6 spaces then the joined PnL line
    assert "Entry: $1000" in out
    assert "Current: $1100" in out
    assert "+$100 (10.0%)" in out


def test_print_strategy_positions_size_line_emitted_when_populated() -> None:
    """`size_line` non-empty => indented size summary is emitted."""
    runner = CliRunner()
    sp = _make_strategy_position(size_usd="500", leverage="3", value_usd="")
    out = _invoke(runner, status_helpers._print_strategy_positions, [sp])
    assert "      Size: $500 | Leverage: 3x" in out


def test_print_strategy_positions_size_empty_pnl_populated() -> None:
    """Exercise branch 445->448 (size empty, pnl populated)."""
    runner = CliRunner()
    # Clear all the size-line fields so size_line is "" -> falsy
    # but provide pnl fields to drive line 450.
    sp = _make_strategy_position(
        value_usd="",
        size_usd="",
        collateral_usd="",
        leverage="",
        health_factor="",
        entry_price="2000",
        unrealized_pnl_usd="5",
    )
    out = _invoke(runner, status_helpers._print_strategy_positions, [sp])
    # No Size/Value line
    assert "Size:" not in out
    assert "Value:" not in out
    # PnL-line present
    assert "Entry: $2000" in out
    assert "+$5" in out


def test_print_strategy_positions_multiple_entries_each_rendered() -> None:
    """Each item in the iterable gets its own header + body block."""
    runner = CliRunner()
    sp1 = _make_strategy_position(position_id="ETH-PERP", direction="LONG")
    sp2 = _make_strategy_position(position_id="BTC-PERP", direction="SHORT")
    out = _invoke(runner, status_helpers._print_strategy_positions, [sp1, sp2])
    assert "ETH-PERP" in out
    assert "BTC-PERP" in out
    assert "LONG" in out
    assert "SHORT" in out


# ---------------------------------------------------------------------------
# Phase 5A.3 — extended coverage: _print_summary_header
# ---------------------------------------------------------------------------


def test_print_summary_header_protocol_empty_shows_dash() -> None:
    """Empty-string `protocol` renders as `-`."""
    runner = CliRunner()
    s = _make_summary(protocol="")
    out = _invoke(runner, status_helpers._print_summary_header, s)
    assert "Protocol:    -" in out


def test_print_summary_header_pnl_since_deploy_populated() -> None:
    """Non-empty `pnl_since_deploy_usd` drops the `-` placeholder."""
    runner = CliRunner()
    s = _make_summary(pnl_since_deploy_usd="100.00")
    out = _invoke(runner, status_helpers._print_summary_header, s)
    assert "PnL (total): $100.00" in out
    assert "PnL (total): -" not in out


# ---------------------------------------------------------------------------
# Phase 5A.3 — extended coverage: _print_operator_card severity color map
# ---------------------------------------------------------------------------


def _spy_click_style(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    """Install a `click.style` spy in `status_helpers` and return a call log.

    Each recorded entry captures `text`, `fg`, `bold`, and any additional
    kwargs passed through to the original style. Used to make color-mapping
    assertions deterministic even when the Runner strips ANSI with
    `color=False`.
    """
    calls: list[dict[str, Any]] = []
    original = status_helpers.click.style

    def _spy(text: str, fg: str | None = None, bold: bool = False, **kw: Any) -> str:
        calls.append({"text": text, "fg": fg, "bold": bold, **kw})
        return original(text, fg=fg, bold=bold, **kw)

    monkeypatch.setattr(status_helpers.click, "style", _spy)
    return calls


def test_print_operator_card_severity_low_uses_white_fg(monkeypatch: pytest.MonkeyPatch) -> None:
    """LOW severity maps to `fg="white"` (not bold) via severity_colors."""
    calls = _spy_click_style(monkeypatch)
    runner = CliRunner()
    oc = _make_operator_card(severity="LOW", reason="FYI")
    out = _invoke(runner, status_helpers._print_operator_card, oc)
    assert "Operator Alert [LOW]: FYI" in out
    # The alert header style call must record fg=white and bold=False for LOW
    header_calls = [c for c in calls if "Operator Alert [LOW]" in c["text"]]
    assert header_calls, "expected alert header to be styled via click.style"
    assert header_calls[0]["fg"] == "white"
    assert header_calls[0]["bold"] is False


def test_print_operator_card_severity_medium_uses_yellow_fg(monkeypatch: pytest.MonkeyPatch) -> None:
    """MEDIUM maps to yellow (not bold)."""
    calls = _spy_click_style(monkeypatch)
    runner = CliRunner()
    oc = _make_operator_card(severity="MEDIUM", reason="Watch")
    _invoke(runner, status_helpers._print_operator_card, oc)
    header_calls = [c for c in calls if "Operator Alert [MEDIUM]" in c["text"]]
    assert header_calls[0]["fg"] == "yellow"
    assert header_calls[0]["bold"] is False


def test_print_operator_card_severity_high_is_bold_red(monkeypatch: pytest.MonkeyPatch) -> None:
    """HIGH maps to bold red."""
    calls = _spy_click_style(monkeypatch)
    runner = CliRunner()
    oc = _make_operator_card(severity="HIGH", reason="Urgent")
    _invoke(runner, status_helpers._print_operator_card, oc)
    header_calls = [c for c in calls if "Operator Alert [HIGH]" in c["text"]]
    assert header_calls[0]["fg"] == "red"
    assert header_calls[0]["bold"] is True


def test_print_operator_card_severity_critical_is_bold_red(monkeypatch: pytest.MonkeyPatch) -> None:
    """CRITICAL maps to bold red."""
    calls = _spy_click_style(monkeypatch)
    runner = CliRunner()
    oc = _make_operator_card(severity="CRITICAL", reason="STOP")
    _invoke(runner, status_helpers._print_operator_card, oc)
    header_calls = [c for c in calls if "Operator Alert [CRITICAL]" in c["text"]]
    assert header_calls[0]["fg"] == "red"
    assert header_calls[0]["bold"] is True


def test_print_operator_card_severity_unknown_falls_back_to_white(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unknown severity falls back to `fg="white"` via `.get(sev, "white")`."""
    calls = _spy_click_style(monkeypatch)
    runner = CliRunner()
    oc = _make_operator_card(severity="BOGUS", reason="mystery")
    out = _invoke(runner, status_helpers._print_operator_card, oc)
    assert "Operator Alert [BOGUS]: mystery" in out
    header_calls = [c for c in calls if "Operator Alert [BOGUS]" in c["text"]]
    # Unknown severity -> falls back to the default "white" AND bold=False
    # (because "BOGUS" is not in the `{HIGH, CRITICAL}` bold set).
    assert header_calls[0]["fg"] == "white"
    assert header_calls[0]["bold"] is False


def test_print_operator_card_risk_only() -> None:
    """Populated `risk_description` but no actions => only Risk line."""
    runner = CliRunner()
    oc = _make_operator_card(risk_description="Immediate loss risk")
    out = _invoke(runner, status_helpers._print_operator_card, oc)
    assert "Risk: Immediate loss risk" in out
    assert "Suggested:" not in out


def test_print_operator_card_suggested_only() -> None:
    """Populated `suggested_actions` without `risk_description` => only Suggested block."""
    runner = CliRunner()
    oc = _make_operator_card(
        risk_description="",
        suggested_actions=["resume manually"],
    )
    out = _invoke(runner, status_helpers._print_operator_card, oc)
    assert "Risk:" not in out
    assert "Suggested:" in out
    assert "- resume manually" in out


# ---------------------------------------------------------------------------
# Phase 5A.3 — extended coverage: _print_legacy_position
# ---------------------------------------------------------------------------


def test_print_legacy_position_only_health_factor() -> None:
    """Only `health_factor` set -> block emitted with just HF line."""
    runner = CliRunner()
    pos = _make_position(health_factor="2.0")
    out = _invoke(runner, status_helpers._print_legacy_position, pos)
    assert "Position:" in out
    assert "Health Factor: 2.0" in out


def test_print_legacy_position_health_factor_zero_included() -> None:
    """`health_factor=0` triggers emission (only None suppresses)."""
    runner = CliRunner()
    pos = _make_position(health_factor=0)
    out = _invoke(runner, status_helpers._print_legacy_position, pos)
    assert "Health Factor: 0" in out


# ---------------------------------------------------------------------------
# Phase 5A.3 — extended coverage: _print_chain_health status color map
# ---------------------------------------------------------------------------


def test_print_chain_health_healthy_status_is_green(monkeypatch: pytest.MonkeyPatch) -> None:
    """HEALTHY status maps to `fg="green"`."""
    calls = _spy_click_style(monkeypatch)
    runner = CliRunner()
    health = {
        "arbitrum": SimpleNamespace(
            status="HEALTHY", rpc_latency_ms=10, gas_price_gwei="0.1"
        )
    }
    _invoke(runner, status_helpers._print_chain_health, health)
    status_calls = [c for c in calls if c["text"] == "HEALTHY"]
    assert status_calls, "expected status token to be styled"
    assert status_calls[0]["fg"] == "green"


def test_print_chain_health_degraded_status_is_yellow(monkeypatch: pytest.MonkeyPatch) -> None:
    """DEGRADED status maps to `fg="yellow"`."""
    calls = _spy_click_style(monkeypatch)
    runner = CliRunner()
    health = {
        "base": SimpleNamespace(status="DEGRADED", rpc_latency_ms=500, gas_price_gwei="1")
    }
    _invoke(runner, status_helpers._print_chain_health, health)
    status_calls = [c for c in calls if c["text"] == "DEGRADED"]
    assert status_calls[0]["fg"] == "yellow"


def test_print_chain_health_unavailable_status_is_red(monkeypatch: pytest.MonkeyPatch) -> None:
    """UNAVAILABLE status maps to `fg="red"`."""
    calls = _spy_click_style(monkeypatch)
    runner = CliRunner()
    health = {
        "arbitrum": SimpleNamespace(
            status="UNAVAILABLE", rpc_latency_ms=0, gas_price_gwei="0"
        )
    }
    out = _invoke(runner, status_helpers._print_chain_health, health)
    assert "UNAVAILABLE" in out
    status_calls = [c for c in calls if c["text"] == "UNAVAILABLE"]
    assert status_calls[0]["fg"] == "red"


def test_print_chain_health_unknown_status_falls_back_to_white(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unknown status falls back to white via `.get(status, "white")`."""
    calls = _spy_click_style(monkeypatch)
    runner = CliRunner()
    health = {
        "weird-chain": SimpleNamespace(
            status="MAINTENANCE", rpc_latency_ms=1, gas_price_gwei="1"
        )
    }
    out = _invoke(runner, status_helpers._print_chain_health, health)
    assert "MAINTENANCE" in out
    status_calls = [c for c in calls if c["text"] == "MAINTENANCE"]
    assert status_calls[0]["fg"] == "white"


# ---------------------------------------------------------------------------
# Phase 5A.3 — extended coverage: _print_timeline color map
# ---------------------------------------------------------------------------


def test_print_timeline_event_type_colors(monkeypatch: pytest.MonkeyPatch) -> None:
    """Each known event type maps to its declared color; unknown falls back to white."""
    calls = _spy_click_style(monkeypatch)
    runner = CliRunner()
    events = [
        SimpleNamespace(
            timestamp=1, event_type="TRADE", description="t", tx_hash="", chain="arb"
        ),
        SimpleNamespace(
            timestamp=2, event_type="REBALANCE", description="r", tx_hash="", chain="arb"
        ),
        SimpleNamespace(
            timestamp=3, event_type="ERROR", description="e", tx_hash="", chain="arb"
        ),
        SimpleNamespace(
            timestamp=4,
            event_type="STATE_CHANGE",
            description="s",
            tx_hash="",
            chain="arb",
        ),
        SimpleNamespace(
            timestamp=5, event_type="UNKNOWN", description="u", tx_hash="", chain="arb"
        ),
    ]
    _invoke(runner, status_helpers._print_timeline, events)

    expected = {
        "TRADE": "green",
        "REBALANCE": "cyan",
        "ERROR": "red",
        "STATE_CHANGE": "yellow",
        "UNKNOWN": "white",  # falls back via .get(etype, "white")
    }
    for etype, fg in expected.items():
        etype_calls = [c for c in calls if c["text"] == etype]
        assert etype_calls, f"expected event_type token '{etype}' to be styled"
        assert etype_calls[0]["fg"] == fg, (
            f"{etype} should map to fg={fg!r}, got {etype_calls[0]['fg']!r}"
        )


def test_print_timeline_no_tx_hash_omits_tx_suffix() -> None:
    """Empty `tx_hash` means no `tx:...` appendage on the event line."""
    runner = CliRunner()
    events = [
        SimpleNamespace(
            timestamp=1_700_000_000,
            event_type="TRADE",
            description="Buy",
            tx_hash="",
            chain="arbitrum",
        )
    ]
    out = _invoke(runner, status_helpers._print_timeline, events)
    assert "tx:" not in out


# ---------------------------------------------------------------------------
# Phase 5A.3 — extended coverage: _render_details_pretty empty-section guards
# ---------------------------------------------------------------------------


def test_render_details_pretty_operator_card_none_skipped() -> None:
    """`operator_card=None` -> no Operator Alert block (#1704).

    After the #1704 fix presence is determined by the parent message via
    `HasField("operator_card")` (authoritative) or, for test fakes lacking
    that method, by `operator_card is not None` (fallback). We no longer
    use the sub-field `deployment_id` emptiness as a presence sentinel — a
    separate test covers that case.
    """
    runner = CliRunner()
    details = _make_details(operator_card=None)
    out = _invoke(runner, status_helpers._render_details_pretty, details, True)
    assert "Operator Alert" not in out


def test_render_details_pretty_operator_card_empty_deployment_id_now_renders() -> None:
    """Empty `deployment_id` no longer suppresses the operator card (#1704).

    Regression guard for the fix: previously, a legitimately-empty
    `deployment_id` would silently hide the card because proto3 scalars are
    falsy. Now the pretty-print path relies on parent presence semantics,
    so a present card with empty `deployment_id` DOES render. For test
    fakes (no `HasField`), the fallback is `operator_card is not None`.
    """
    runner = CliRunner()
    oc = _make_operator_card(deployment_id="", severity="MEDIUM", reason="watchlist")
    details = _make_details(operator_card=oc)
    out = _invoke(runner, status_helpers._render_details_pretty, details, True)
    assert "Operator Alert [MEDIUM]: watchlist" in out


def test_render_details_pretty_operator_card_hasfield_respected() -> None:
    """Proto3-like `HasField` takes precedence over fallback logic (#1704)."""

    class _ProtoLikeDetails(SimpleNamespace):
        def HasField(self, name: str) -> bool:  # noqa: N802 (proto naming)
            return name in getattr(self, "_present", ())

    runner = CliRunner()
    oc = _make_operator_card(deployment_id="x", severity="HIGH", reason="stuck")
    details = _ProtoLikeDetails(
        summary=_make_summary(),
        position=None,
        timeline=[],
        chain_health={},
        operator_card=oc,
        _present=set(),  # message not present per HasField
    )
    out = _invoke(runner, status_helpers._render_details_pretty, details, True)
    assert "Operator Alert" not in out

    details._present = {"operator_card"}
    out = _invoke(runner, status_helpers._render_details_pretty, details, True)
    assert "Operator Alert [HIGH]: stuck" in out


def test_render_details_pretty_legacy_pos_populated_only() -> None:
    """Legacy position with token_balances populated -> `Position:` block."""
    runner = CliRunner()
    pos = _make_position(
        token_balances=[SimpleNamespace(symbol="USDC", balance="10", value_usd="10")]
    )
    details = _make_details(position=pos)
    out = _invoke(runner, status_helpers._render_details_pretty, details, True)
    assert "Position:" in out
    assert "USDC: 10 ($10)" in out
    # Empty strategy_positions -> NOT present
    assert "Positions:" not in out


def test_render_details_pretty_only_strategy_positions() -> None:
    """Only `strategy_positions` populated => `Positions:` present, no legacy block.

    Regression guard: if someone reintroduces the legacy `Position:` block on a
    position that only has `strategy_positions`, this test must fail. The
    legacy block starts with the literal line `  Position:` (2-space indent,
    newline-terminated), while the new-style block uses `  Positions:` (note
    the plural `s`). A plain substring check disambiguates -- NO `or` guard.
    """
    runner = CliRunner()
    pos = _make_position(strategy_positions=[_make_strategy_position()])
    details = _make_details(position=pos)
    out = _invoke(runner, status_helpers._render_details_pretty, details, True)
    # Legacy `Position:` line would be `  Position:\n` (singular + newline).
    # The plural `  Positions:\n` must NOT match this literal.
    assert "  Position:\n" not in out
    # But the strategy_positions block (plural) IS present
    assert "  Positions:\n" in out


def test_render_details_pretty_chain_health_only() -> None:
    """Chain_health populated alone -> only that block present (plus summary)."""
    runner = CliRunner()
    health = {
        "arbitrum": SimpleNamespace(
            status="HEALTHY", rpc_latency_ms=10, gas_price_gwei="0.1"
        )
    }
    details = _make_details(chain_health=health)
    out = _invoke(runner, status_helpers._render_details_pretty, details, True)
    assert "Chain Health:" in out
    assert "Position:" not in out
    assert "Positions:" not in out
    assert "Recent Events:" not in out


def test_render_details_pretty_timeline_enabled_but_empty() -> None:
    """`timeline_enabled=True` with empty events -> no `Recent Events:` block."""
    runner = CliRunner()
    details = _make_details(timeline=[])
    out = _invoke(runner, status_helpers._render_details_pretty, details, True)
    assert "Recent Events:" not in out


def test_render_details_pretty_trailing_blank_line() -> None:
    """Output ends with an actual blank line (the final `click.echo()` call).

    The orchestrator's last line is a bare `click.echo()` — that emits a
    single `"\n"` AFTER the previous line's own `"\n"`, producing `"\n\n"`
    at the tail. This asserts that invariant directly (not just "ends with
    a newline", which would pass even if the final blank line were removed).
    """
    runner = CliRunner()
    details = _make_details()
    out = _invoke(runner, status_helpers._render_details_pretty, details, True)
    assert out.endswith("\n\n")


# ---------------------------------------------------------------------------
# _format_pt_inventory_detail_line + _print_strategy_positions (PT, VIB-5317)
# ---------------------------------------------------------------------------


def _make_pt_position(**detail_overrides: Any) -> SimpleNamespace:
    """A FIFO-derived held-PT StrategyPosition-like row (proto map shape)."""
    details = {
        "source": "pt_inventory_lots",
        "pt_symbol": "PT-wstETH-26DEC2024",
        "quantity": "10.5",
        "days_to_maturity": "42",
        "sy_cost": "9.8",
        "price_confidence": "HIGH",
    }
    details.update(detail_overrides)
    return _make_strategy_position(
        position_type="TOKEN",
        position_id="PT-wstETH-26DEC2024",
        protocol="pt",
        chain="arbitrum",
        value_usd="1050.50",
        unrealized_pnl_usd="50.50",
        details=details,
    )


def test_format_pt_inventory_detail_line_measured() -> None:
    """Measured PT row renders qty / days / SY cost / confidence."""
    line = status_helpers._format_pt_inventory_detail_line(_make_pt_position())
    assert "Qty: 10.5" in line
    assert "Days to maturity: 42" in line
    assert "SY cost: 9.8" in line
    assert "Confidence: HIGH" in line


def test_format_pt_inventory_detail_line_unmeasured_badge() -> None:
    """Unmeasured PT shows Confidence: UNAVAILABLE and keeps qty + SY cost."""
    pt = _make_pt_position(price_confidence="UNAVAILABLE", mark_unmeasured="true")
    line = status_helpers._format_pt_inventory_detail_line(pt)
    assert "Confidence: UNAVAILABLE" in line
    assert "Qty: 10.5" in line


def test_format_pt_inventory_detail_line_skips_non_pt() -> None:
    """A non-PT position (no PT marker) yields an empty line."""
    perp = _make_strategy_position()  # PERP, empty details
    assert status_helpers._format_pt_inventory_detail_line(perp) == ""


def test_print_strategy_positions_pt_row_visible() -> None:
    """End-to-end: a PT row prints header + value + the PT detail line."""
    runner = CliRunner()
    out = _invoke(runner, status_helpers._print_strategy_positions, [_make_pt_position()])
    assert "TOKEN" in out
    assert "PT-wstETH-26DEC2024" in out
    assert "(pt) on arbitrum" in out
    assert "Value: $1050.50" in out
    assert "Qty: 10.5" in out
    assert "Confidence: HIGH" in out


def test_print_strategy_positions_unmeasured_pt_no_dollar_zero() -> None:
    """Unmeasured PT: blank value_usd → no 'Value:' line, never '$0'."""
    runner = CliRunner()
    pt = _make_pt_position(price_confidence="UNAVAILABLE", mark_unmeasured="true")
    # Unmeasured proto leaves value_usd / pnl blank.
    pt.value_usd = ""
    pt.unrealized_pnl_usd = ""
    out = _invoke(runner, status_helpers._print_strategy_positions, [pt])
    assert "PT-wstETH-26DEC2024" in out
    assert "$0" not in out
    assert "Value:" not in out
    assert "Confidence: UNAVAILABLE" in out
