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
    strategy_id, name, status, chain(s), is_multi_chain, protocol,
    total_value_usd, pnl_24h_usd, pnl_since_deploy_usd, last_action_at,
    last_iteration_at, consecutive_errors, attention_required,
    attention_reason.
    """
    defaults = {
        "strategy_id": "demo",
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
        "strategy_id": "demo",
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


def test_print_summary_header_fallback_to_strategy_id() -> None:
    """When `name` is empty, header falls back to `strategy_id`."""
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


def test_print_operator_card_noop_when_strategy_id_empty(capsys: pytest.CaptureFixture) -> None:
    """No output when `oc.strategy_id` is empty (matches original guard)."""
    oc = _make_operator_card(strategy_id="")
    status_helpers._print_operator_card(oc)
    assert capsys.readouterr().out == ""


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


def test_format_pnl_line_invalid_pnl_falls_back_to_zero() -> None:
    """Invalid PnL string falls back to 0.0 (white, no prefix) -- issue #1697.

    This silent-0.0 fallback is a latent bug tracked in #1697 and is preserved
    byte-for-byte by this refactor. Do NOT fix here.
    """
    sp = _make_strategy_position(unrealized_pnl_usd="not_a_number")
    out = status_helpers._format_strategy_position_pnl_line(sp)
    # Treated as 0 => white, no prefix
    assert "\x1b[37m" in out
    assert "$not_a_number" in out
    assert "+$not_a_number" not in out


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
    oc = _make_operator_card(severity="HIGH", reason="Stuck", strategy_id="demo")
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
