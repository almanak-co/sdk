"""Helper functions extracted from `framework/cli/status.py:strategy_status`.

Pure refactor: these helpers encapsulate the transport (gateway RPC), JSON
rendering, and pretty-print chunks of the `strat status` CLI. No behavior
change. Each helper preserves the exact click.echo output, exit codes, and
side effects (client.disconnect ordering, error strings) of the original
inlined code.

Phase 5A.1 scope (transport + JSON):
    - _validate_status_args       — timeline_limit validation
    - _fetch_strategy_details     — transport + disconnect + error string
    - _render_json_summary        — summary dict for JSON output
    - _render_json_position       — position dict (incl. strategy_positions)
    - _render_json_timeline       — timeline list for JSON output
    - _render_json_chain_health   — chain_health dict for JSON output
    - _render_json_operator_card  — operator_card dict for JSON output
    - _render_details_as_json     — top-level JSON orchestrator

Phase 5A.2 scope (pretty-print):
    - _print_summary_header              — strategy header + summary fields
    - _print_operator_card               — operator alert block
    - _print_legacy_position             — token_balances/lp_positions/HF block
    - _format_strategy_position_size_line — Size/Collateral/Leverage/HF line
    - _format_strategy_position_pnl_line  — Entry/Current/PnL line with colors
    - _print_strategy_positions          — strategy_positions block
    - _print_chain_health                — chain health block
    - _print_timeline                    — recent events block
    - _render_details_pretty             — top-level pretty-print orchestrator

See `blueprints/` and `.claude/plans/phase-5-cli-cc-reduction.md`.
"""

from __future__ import annotations

import json
import sys
from typing import TYPE_CHECKING, Any

import click

if TYPE_CHECKING:
    from ..gateway_client import GatewayClient


# =============================================================================
# Argument validation
# =============================================================================


def _validate_status_args(timeline_limit: int) -> None:
    """Validate `strat status` arguments.

    Preserves exact click.secho error message and sys.exit(1) semantics from
    the original inlined code at `status.py:284-286`.
    """
    if timeline_limit < 1:
        click.secho("--timeline-limit must be >= 1.", fg="red", err=True)
        sys.exit(1)


# =============================================================================
# Transport
# =============================================================================


def _fetch_strategy_details(
    client: GatewayClient,
    strategy_id: str,
    *,
    include_timeline: bool,
    timeline_limit: int,
) -> Any:
    """Fetch strategy details from the gateway DashboardService.

    Preserves exact transport semantics from `status.py:288-303`:
        - `try/except` wraps only the RPC call (not `client.disconnect()`).
        - Error string "Failed to get strategy details: {e}" is verbatim
          (grep-asserted in smoke tests).
        - `sys.exit(1)` on RPC failure.
        - `finally: client.disconnect()` always runs.
    """
    from almanak.gateway.proto import gateway_pb2

    try:
        request = gateway_pb2.GetStrategyDetailsRequest(
            strategy_id=strategy_id,
            include_timeline=include_timeline,
            include_pnl_history=False,
            timeline_limit=timeline_limit,
        )
        return client.dashboard.GetStrategyDetails(request)
    except Exception as e:
        click.secho(f"Failed to get strategy details: {e}", fg="red", err=True)
        sys.exit(1)
    finally:
        client.disconnect()


# =============================================================================
# JSON rendering
# =============================================================================


def _render_json_summary(summary: Any) -> dict[str, Any]:
    """Serialize `details.summary` to a JSON-friendly dict.

    Preserves the exact field ordering and transforms from `status.py:308-322`.
    In particular `pnl_since_deploy_usd` is normalized: empty string -> None.
    """
    return {
        "strategy_id": summary.strategy_id,
        "name": summary.name,
        "status": summary.status,
        "chain": summary.chain,
        "protocol": summary.protocol,
        "total_value_usd": summary.total_value_usd,
        "pnl_24h_usd": summary.pnl_24h_usd,
        "last_action_at": summary.last_action_at,
        "attention_required": summary.attention_required,
        "attention_reason": summary.attention_reason,
        "consecutive_errors": summary.consecutive_errors,
        "last_iteration_at": summary.last_iteration_at,
        "pnl_since_deploy_usd": summary.pnl_since_deploy_usd or None,
    }


def _render_json_position(position: Any) -> dict[str, Any]:
    """Serialize `details.position` to a JSON-friendly dict.

    Preserves the exact ordering and presence semantics from `status.py:323-373`:
        - `token_balances` only emitted when non-empty.
        - `lp_positions` only emitted when non-empty.
        - `health_factor` only emitted when not None.
        - For `strategy_positions`, optional monitoring fields are included
          only when proto string is non-empty (the proto3 empty-string sentinel
          — issue #1704 documents this; this refactor preserves behavior).
        - If no sub-fields populate, returns an empty dict (caller decides
          whether to include a `position` key at all).
    """
    pos_data: dict[str, Any] = {}
    if position.token_balances:
        pos_data["token_balances"] = [
            {"symbol": t.symbol, "balance": t.balance, "value_usd": t.value_usd} for t in position.token_balances
        ]
    if position.lp_positions:
        pos_data["lp_positions"] = [
            {
                "pool": lp.pool,
                "token0": lp.token0,
                "token1": lp.token1,
                "liquidity_usd": lp.liquidity_usd,
            }
            for lp in position.lp_positions
        ]
    if position.health_factor is not None:
        pos_data["health_factor"] = float(position.health_factor)
    if position.strategy_positions:
        sp_list = []
        for sp in position.strategy_positions:
            sp_dict: dict[str, Any] = {
                "position_type": sp.position_type,
                "position_id": sp.position_id,
                "chain": sp.chain,
                "protocol": sp.protocol,
                "value_usd": sp.value_usd,
                "liquidation_risk": sp.liquidation_risk,
            }
            # Include optional monitoring fields when present
            for field in (
                "direction",
                "entry_price",
                "current_price",
                "unrealized_pnl_usd",
                "unrealized_pnl_pct",
                "size_usd",
                "collateral_usd",
                "leverage",
                "health_factor",
            ):
                val = getattr(sp, field, "")
                if val != "":
                    sp_dict[field] = val
            if sp.details:
                sp_dict["details"] = dict(sp.details)
            sp_list.append(sp_dict)
        pos_data["strategy_positions"] = sp_list
    return pos_data


def _render_json_timeline(timeline: Any) -> list[dict[str, Any]]:
    """Serialize `details.timeline` to a JSON-friendly list.

    Preserves ordering and fields from `status.py:375-384`.
    """
    return [
        {
            "timestamp": e.timestamp,
            "event_type": e.event_type,
            "description": e.description,
            "tx_hash": e.tx_hash,
            "chain": e.chain,
        }
        for e in timeline
    ]


def _render_json_chain_health(chain_health: Any) -> dict[str, Any]:
    """Serialize `details.chain_health` map to a JSON-friendly dict.

    Preserves ordering and fields from `status.py:386-393`.
    """
    return {
        name: {
            "status": h.status,
            "rpc_latency_ms": h.rpc_latency_ms,
            "gas_price_gwei": h.gas_price_gwei,
        }
        for name, h in chain_health.items()
    }


def _render_json_operator_card(operator_card: Any) -> dict[str, Any]:
    """Serialize `details.operator_card` to a JSON-friendly dict.

    Preserves fields from `status.py:395-400`. Caller guards on
    `operator_card.strategy_id` truthiness before invoking.
    """
    return {
        "severity": operator_card.severity,
        "reason": operator_card.reason,
        "risk_description": operator_card.risk_description,
        "suggested_actions": list(operator_card.suggested_actions),
    }


def _render_details_as_json(details: Any) -> str:
    """Render `GetStrategyDetailsResponse` as the JSON string emitted by
    `strat status --json`.

    Returns the JSON text ready for `click.echo`. Does NOT call echo directly
    — keeps the helper pure and trivially unit-testable.

    Preserves the exact top-level structure from `status.py:307-401`:
        - summary always present.
        - `position` key only when _render_json_position produces non-empty dict
          AND `details.position` is truthy.
        - `timeline` key only when `details.timeline` truthy.
        - `chain_health` key only when `details.chain_health` truthy.
        - `operator_card` key only when both `details.operator_card` AND
          `operator_card.strategy_id` are truthy.
        - JSON is indented with 2 spaces.
    """
    result: dict[str, Any] = _render_json_summary(details.summary)
    if details.position:
        pos_data = _render_json_position(details.position)
        if pos_data:
            result["position"] = pos_data
    if details.timeline:
        result["timeline"] = _render_json_timeline(details.timeline)
    if details.chain_health:
        result["chain_health"] = _render_json_chain_health(details.chain_health)
    if details.operator_card and details.operator_card.strategy_id:
        result["operator_card"] = _render_json_operator_card(details.operator_card)
    return json.dumps(result, indent=2)


# =============================================================================
# Pretty-print rendering (Phase 5A.2)
# =============================================================================
#
# Each helper below is a no-op when its input section is empty/falsy, so
# composed output via `_render_details_pretty` is byte-for-byte identical to
# the original inlined code in `status.py:304-436`. `_format_timestamp`,
# `_format_relative_time`, and `_status_color` are imported lazily from
# `.status` to avoid a module-level import cycle (status.py imports from
# status_helpers.py).


def _print_summary_header(s: Any) -> None:
    """Emit the strategy header block (name + ID/Status/Chain/.../Errors).

    Preserves the exact format from `status.py:304-324`. Always emits output
    when called — caller decides whether to call (there is no empty-section
    case for the summary itself; every status response has a summary).
    """
    from .status import _format_timestamp, _status_color

    click.echo()
    click.echo(click.style(f"Strategy: {s.name or s.strategy_id}", bold=True, fg="cyan"))
    click.echo()

    click.echo(f"  ID:          {s.strategy_id}")
    click.echo(f"  Status:      {_status_color(s.status)}")
    click.echo(f"  Chain:       {','.join(s.chains) if s.is_multi_chain else s.chain}")
    click.echo(f"  Protocol:    {s.protocol or '-'}")
    click.echo(f"  Value:       ${s.total_value_usd}" if s.total_value_usd is not None else "  Value:       -")
    click.echo(f"  PnL (24h):   ${s.pnl_24h_usd}" if s.pnl_24h_usd is not None else "  PnL (24h):   -")
    click.echo(f"  PnL (total): ${s.pnl_since_deploy_usd}" if s.pnl_since_deploy_usd != "" else "  PnL (total): -")
    click.echo(f"  Last Active: {_format_timestamp(s.last_action_at)}")
    if s.last_iteration_at:
        click.echo(f"  Last Iter:   {_format_timestamp(s.last_iteration_at)}")
    if s.consecutive_errors:
        click.echo(click.style(f"  Errors:      {s.consecutive_errors} consecutive", fg="red"))

    if s.attention_required:
        click.echo()
        click.echo(click.style(f"  ! {s.attention_reason}", fg="yellow", bold=True))


def _print_operator_card(oc: Any) -> None:
    """Emit the operator alert block.

    Preserves the exact format from `status.py:327-343`. No-op when
    `oc` is falsy or `oc.strategy_id` is empty (matches the original guard).
    """
    if not oc or not oc.strategy_id:
        return

    click.echo()
    severity_colors = {"LOW": "white", "MEDIUM": "yellow", "HIGH": "red", "CRITICAL": "red"}
    click.echo(
        click.style(
            f"  Operator Alert [{oc.severity}]: {oc.reason}",
            fg=severity_colors.get(oc.severity, "white"),
            bold=oc.severity in ("HIGH", "CRITICAL"),
        )
    )
    if oc.risk_description:
        click.echo(f"    Risk: {oc.risk_description}")
    if oc.suggested_actions:
        click.echo("    Suggested:")
        for action in oc.suggested_actions:
            click.echo(f"      - {action}")


def _print_legacy_position(pos: Any) -> None:
    """Emit the legacy position block (token_balances/lp_positions/HF).

    Preserves the exact format from `status.py:346-358`. No-op when `pos` is
    falsy or none of `token_balances`, `lp_positions`, `health_factor` are
    populated (matches the original guard).
    """
    if not pos:
        return
    if not (pos.token_balances or pos.lp_positions or pos.health_factor is not None):
        return

    click.echo()
    click.echo(click.style("  Position:", bold=True))
    if pos.token_balances:
        for t in pos.token_balances:
            val = f" (${t.value_usd})" if t.value_usd is not None else ""
            click.echo(f"    {t.symbol}: {t.balance}{val}")
    if pos.lp_positions:
        for lp in pos.lp_positions:
            click.echo(f"    LP: {lp.pool} ({lp.token0}/{lp.token1}) ${lp.liquidity_usd}")
    if pos.health_factor is not None:
        click.echo(f"    Health Factor: {pos.health_factor}")


def _format_strategy_position_size_line(sp: Any) -> str:
    """Build the Size/Value/Collateral/Leverage/HF pipe-joined line.

    Preserves the field-selection order from `status.py:372-384`. Returns
    an empty string when NO parts apply; caller must not emit a blank line.
    Note: Size vs Value is an either/or (Size takes precedence when present).
    """
    parts: list[str] = []
    if sp.size_usd:
        parts.append(f"Size: ${sp.size_usd}")
    elif sp.value_usd:
        parts.append(f"Value: ${sp.value_usd}")
    if sp.collateral_usd:
        parts.append(f"Collateral: ${sp.collateral_usd}")
    if sp.leverage:
        parts.append(f"Leverage: {sp.leverage}x")
    if sp.health_factor:
        parts.append(f"HF: {sp.health_factor}")
    if not parts:
        return ""
    return " | ".join(parts)


def _format_strategy_position_pnl_line(sp: Any) -> str:
    """Build the Entry/Current/PnL pipe-joined line with colored PnL.

    Preserves the exact format and color rules from `status.py:386-407`:
        - Outer guard: only build when one of entry/current/pnl is non-empty.
        - PnL parsing uses `try/except (ValueError, TypeError) -> pnl_num = 0.0`
          — this silent-0.0 fallback is tracked as issue #1697 and is NOT
          being fixed in this refactor; it is preserved byte-for-byte.
        - Color: >0 green with '+' prefix, <0 red no prefix, ==0 white no
          prefix.

    Returns an empty string when none of the three sub-fields is set OR
    when the guard passes but no individual part materializes (defensive).
    """
    if sp.entry_price == "" and sp.current_price == "" and sp.unrealized_pnl_usd == "":
        return ""

    pnl_parts: list[str] = []
    if sp.entry_price != "":
        pnl_parts.append(f"Entry: ${sp.entry_price}")
    if sp.current_price != "":
        pnl_parts.append(f"Current: ${sp.current_price}")
    if sp.unrealized_pnl_usd != "":
        pnl_val = sp.unrealized_pnl_usd
        pnl_pct = f" ({sp.unrealized_pnl_pct}%)" if sp.unrealized_pnl_pct else ""
        try:
            pnl_num = float(pnl_val)
        except (ValueError, TypeError):
            pnl_num = 0.0
        if pnl_num > 0:
            pnl_prefix, pnl_color = "+", "green"
        elif pnl_num < 0:
            pnl_prefix, pnl_color = "", "red"
        else:
            pnl_prefix, pnl_color = "", "white"
        pnl_parts.append(f"PnL: {click.style(f'{pnl_prefix}${pnl_val}{pnl_pct}', fg=pnl_color)}")
    if not pnl_parts:
        return ""
    return " | ".join(pnl_parts)


def _print_strategy_positions(positions: Any) -> None:
    """Emit the strategy positions block.

    Preserves the exact format from `status.py:361-409`. No-op when the
    `positions` iterable is empty/falsy (matches the original guard).
    """
    if not positions:
        return

    click.echo()
    click.echo(click.style("  Positions:", bold=True))
    for sp in positions:
        # Header line: e.g. "PERP LONG ETH-PERP (gmx_v2) on arbitrum"
        direction_str = f" {sp.direction}" if sp.direction else ""
        click.echo(
            f"    {click.style(sp.position_type, bold=True)}"
            f"{direction_str} {sp.position_id} ({sp.protocol}) on {sp.chain}"
        )
        # Size / collateral / leverage line
        size_line = _format_strategy_position_size_line(sp)
        if size_line:
            click.echo(f"      {size_line}")
        # Entry / current / PnL line
        pnl_line = _format_strategy_position_pnl_line(sp)
        if pnl_line:
            click.echo(f"      {pnl_line}")
        if sp.liquidation_risk:
            click.echo(click.style("      ! Liquidation risk", fg="red", bold=True))


def _print_chain_health(chain_health: Any) -> None:
    """Emit the chain health block.

    Preserves the exact format from `status.py:412-421`. No-op when the
    map is empty/falsy (matches the original guard).
    """
    if not chain_health:
        return

    click.echo()
    click.echo(click.style("  Chain Health:", bold=True))
    for chain_name, health in chain_health.items():
        status_color = {"HEALTHY": "green", "DEGRADED": "yellow", "UNAVAILABLE": "red"}
        click.echo(
            f"    {chain_name}: "
            f"{click.style(health.status, fg=status_color.get(health.status, 'white'))} "
            f"(RPC: {health.rpc_latency_ms}ms, gas: {health.gas_price_gwei} gwei)"
        )


def _print_timeline(events: Any) -> None:
    """Emit the recent events block.

    Preserves the exact format from `status.py:424-434`. No-op when
    `events` is empty/falsy. Callers gate on the `--timeline` flag before
    calling (this helper trusts its input).
    """
    if not events:
        return

    from .status import _format_relative_time

    click.echo()
    click.echo(click.style("  Recent Events:", bold=True))
    for evt in events:
        ts = _format_relative_time(evt.timestamp)
        type_colors = {"TRADE": "green", "REBALANCE": "cyan", "ERROR": "red", "STATE_CHANGE": "yellow"}
        etype = click.style(evt.event_type, fg=type_colors.get(evt.event_type, "white"))
        line = f"    {ts:<12} {etype:<20} {evt.description}"
        if evt.tx_hash:
            line += f"  tx:{evt.tx_hash[:10]}..."
        click.echo(line)


def _render_details_pretty(details: Any, timeline_enabled: bool) -> None:
    """Compose the full pretty-print output for `strat status`.

    Preserves the exact section ordering and trailing blank line from
    `status.py:304-436`:
        1. summary header
        2. operator card (guarded)
        3. legacy position (guarded)
        4. strategy positions (guarded)
        5. chain health (guarded)
        6. timeline (guarded by `timeline_enabled` AND non-empty events)
        7. trailing blank line
    """
    _print_summary_header(details.summary)
    _print_operator_card(details.operator_card)
    _print_legacy_position(details.position)
    _print_strategy_positions(details.position.strategy_positions if details.position else None)
    _print_chain_health(details.chain_health)
    if timeline_enabled:
        _print_timeline(details.timeline)
    click.echo()
