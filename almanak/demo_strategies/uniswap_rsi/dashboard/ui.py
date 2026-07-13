"""Uniswap RSI demo dashboard.

Canonical exemplar of the TA template path: build a
``TADashboardConfig`` from ``config.json``, enrich session state via
``prepare_ta_session_state`` (gateway-backed OHLCV + RSI series + buy/sell
markers from the trade tape), then call ``render_ta_dashboard``. The
renderer owns the title, the strategy header, the price+RSI subplot, and
the three audit sections.

For multi-position or multi-signal layouts, hand-roll using the section
helpers and primitive plots directly (see
``docs/internal/blueprints/23-dashboard-plots-and-templates.md``).
"""

from __future__ import annotations

from typing import Any

from almanak.framework.dashboard.templates import (
    get_rsi_config,
    prepare_ta_session_state,
    render_ta_dashboard,
)


def render_custom_dashboard(
    deployment_id: str,
    strategy_config: dict[str, Any],
    api_client: Any,
    session_state: dict[str, Any],
) -> None:
    # Compute the dashboard RSI from the SAME candle granularity the strategy
    # decides on, so the RSI line and the buy/sell markers share one series
    # (VIB-4969). When ``data_granularity`` is unset the strategy's
    # ``market.rsi()`` falls back to ``market.snapshot.DEFAULT_TIMEFRAME`` ("4h")
    # — so the dashboard MUST use that same default, not a hardcoded "1h", or the
    # rendered RSI is a different series than the one that fired the trade (the
    # VIB-5737 field bug: RSI shown ~70 while the strategy lived at ~64). Passing
    # a truthy config value overrides; ``None`` / "" defers to get_rsi_config's
    # shared default. ``or None`` (not ``or "4h"``) keeps the single source of
    # truth in the template so a future DEFAULT_TIMEFRAME change can't drift.
    granularity = strategy_config.get("data_granularity") or None
    rsi_config_kwargs: dict[str, Any] = {
        "period": int(strategy_config.get("rsi_period", 14)),
        "overbought": float(strategy_config.get("rsi_overbought", 70)),
        "oversold": float(strategy_config.get("rsi_oversold", 30)),
    }
    if granularity is not None:
        rsi_config_kwargs["timeframe"] = str(granularity)
    config = get_rsi_config(**rsi_config_kwargs)
    config.base_token = str(strategy_config.get("base_token", config.base_token))
    config.quote_token = str(strategy_config.get("quote_token", config.quote_token))
    config.chain = str(strategy_config.get("chain", config.chain))
    config.protocol = str(strategy_config.get("protocol", config.protocol))

    session_state = prepare_ta_session_state(
        api_client,
        session_state=session_state,
        config=config,
        deployment_id=deployment_id,
    )

    render_ta_dashboard(deployment_id, strategy_config, session_state, config)
