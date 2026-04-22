"""Helper functions extracted from `framework/cli/status.py:strategy_status` (Phase 5A.1).

Pure refactor: these helpers encapsulate the transport (gateway RPC) and
JSON-rendering chunks of the `strat status` CLI. No behavior change. Each
helper preserves the exact click.echo output, exit codes, and side effects
(client.disconnect ordering, error strings) of the original inlined code.

Scope of 5A.1 (this module):
    - _validate_status_args       — timeline_limit validation
    - _fetch_strategy_details     — transport + disconnect + error string
    - _render_json_summary        — summary dict for JSON output
    - _render_json_position       — position dict (incl. strategy_positions)
    - _render_json_timeline       — timeline list for JSON output
    - _render_json_chain_health   — chain_health dict for JSON output
    - _render_json_operator_card  — operator_card dict for JSON output
    - _render_details_as_json     — top-level JSON orchestrator

Phase 5A.2 will extract the pretty-print path. Phase 5A.3 adds extended
coverage. See `blueprints/` and `.claude/plans/phase-5-cli-cc-reduction.md`.
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
