"""Uniswap RSI demo dashboard.

Canonical exemplar of the TA template path: build a
``TADashboardConfig`` from ``config.json``, enrich session state via
``prepare_ta_session_state`` (gateway-backed OHLCV + RSI series + buy/sell
markers from the trade tape), then call ``render_ta_dashboard``. The
renderer owns the title, the strategy header, the price+RSI subplot, and
the three audit sections.

For multi-position or multi-signal layouts, hand-roll using the section
helpers and primitive plots directly (see
``blueprints/23-dashboard-plots-and-templates.md``).
"""

from __future__ import annotations

from typing import Any

from almanak.framework.dashboard.templates import (
    get_rsi_config,
    prepare_ta_session_state,
    render_ta_dashboard,
)


def render_custom_dashboard(
    strategy_id: str,
    strategy_config: dict[str, Any],
    api_client: Any,
    session_state: dict[str, Any],
) -> None:
    config = get_rsi_config(
        period=int(strategy_config.get("rsi_period", 14)),
        overbought=float(strategy_config.get("rsi_overbought", 70)),
        oversold=float(strategy_config.get("rsi_oversold", 30)),
    )
    config.base_token = str(strategy_config.get("base_token", config.base_token))
    config.quote_token = str(strategy_config.get("quote_token", config.quote_token))
    config.chain = str(strategy_config.get("chain", config.chain))
    config.protocol = str(strategy_config.get("protocol", config.protocol))

    session_state = prepare_ta_session_state(
        api_client,
        session_state=session_state,
        config=config,
    )

    render_ta_dashboard(strategy_id, strategy_config, session_state, config)
