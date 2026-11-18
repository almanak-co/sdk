"""Shared lending cash cannot become standalone V4 trade profit or a hold benchmark."""

CASH_PERFORMANCE = frozenset(
    {"fund_value_excluding_gas", "pnl_excluding_gas", "premint_hold_benchmark", "lp_minus_hold"}
)


def position_only(position):
    return {
        **{key: value for key, value in position.items() if key not in CASH_PERFORMANCE},
        "valuation_scope": (
            "Protocol principal and fees only; fund cash is shared across connectors."
        ),
    }
