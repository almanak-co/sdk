"""JSON serialization for strategy-class-aware accounting sections."""

from __future__ import annotations

from decimal import Decimal
from importlib import import_module
from typing import Any

from .data_quality import DataQualitySection
from .lending_report import LendingSection
from .lp_report import LPSection


def _m(v: Decimal | None) -> str | None:
    return str(v) if v is not None else None


def lp_section_to_dict(section: LPSection) -> dict[str, Any]:
    return {
        "positions": [
            {
                "position_id": p.position_id,
                "protocol": p.protocol,
                "chain": p.chain,
                "token0": p.token0,
                "token1": p.token1,
                "is_closed": p.is_closed,
                "entry_value_usd": _m(p.entry_value_usd),
                "exit_value_usd": _m(p.exit_value_usd),
                "fees_token0": str(p.fees_token0),
                "fees_token1": str(p.fees_token1),
                "protocol_fees_usd": str(p.protocol_fees_usd),
                "total_gas_usd": str(p.total_gas_usd),
                "il_usd": _m(p.il_usd),
                "net_pnl_usd": _m(p.net_pnl_usd),
                "in_range": p.in_range,
            }
            for p in section.positions
        ],
        "total_net_pnl_usd": _m(section.total_net_pnl_usd),
        "total_gas_usd": str(section.total_gas_usd),
    }


def lending_section_to_dict(section: LendingSection) -> dict[str, Any]:
    return {
        "positions": [
            {
                "position_key": p.position_key,
                "protocol": p.protocol,
                "chain": p.chain,
                "asset": p.asset,
                "market_id": p.market_id,
                "is_closed": p.is_closed,
                "collateral_usd": _m(p.collateral_usd),
                "debt_usd": _m(p.debt_usd),
                "net_equity_usd": _m(p.net_equity_usd),
                "health_factor": _m(p.health_factor),
                "liquidation_threshold": _m(p.liquidation_threshold),
                "supply_apr_pct": _m(p.supply_apr_pct),
                "borrow_apr_pct": _m(p.borrow_apr_pct),
                "total_gas_usd": str(p.total_gas_usd),
                # VIB-4974: signed net realized interest (debt cost negative,
                # supply yield positive) plus the per-side gross magnitudes.
                "total_interest_delta_usd": str(p.total_interest_delta_usd),
                "total_interest_paid_usd": str(p.total_interest_paid_usd),
                "total_interest_earned_usd": str(p.total_interest_earned_usd),
                "deleverage_count": p.deleverage_count,
            }
            for p in section.positions
        ]
    }


def pendle_section_to_dict(section: Any) -> dict[str, Any]:
    return import_module("almanak.connectors.pendle.reporting").pendle_section_to_dict(section)


def data_quality_to_dict(section: DataQualitySection) -> dict[str, Any]:
    return {
        "unavailable_count": len(section.issues),
        "parse_errors": section.parse_errors,
        "issues": [
            {
                "event_type": i.event_type,
                "position_key": i.position_key,
                "timestamp": i.timestamp,
                "reason": i.reason,
                "protocol": i.protocol,
                "chain": i.chain,
            }
            for i in section.issues
        ],
    }
