"""WETH's hourly price with the strategy's buys and sells, and the RSI it trades on, recomputed
from the same closes the strategy read (`context.prices`) with the same TA-Lib call."""

import math
from collections.abc import Mapping
from decimal import Decimal

import numpy as np
import talib

from almanak import dashboard as ui
from almanak.connectors.uniswap_v3.models import Parameters
from almanak.dashboard import build as b
from almanak.dashboard.models import ExchangePosition

BUYS, SELLS = ("swap_exact_input",), ("exit_exchange",)
SOURCE = "The traded pool's hourly closes"


def param(config, name):
    """A strategy parameter by name, from the plain mapping or the typed parameters.py model."""
    values = config.parameters
    return values[name] if isinstance(values, Mapping) else getattr(values, name)


def price(id, amount, reason, *, pair, usdc, weth):
    return b.price(
        id,
        amount,
        reason,
        price_source=SOURCE,
        symbol=pair,
        quote=usdc.asset,
        asset=weth.asset,
        evidence="author_derived",
    )


def level(id, amount, reason):
    return b.index(id, amount, reason, evidence="author_derived")


def worth(id, amount, reason, usdc):
    return b.value(
        id,
        None if amount is None else Decimal(amount),
        "base_units",
        reason=reason,
        places=usdc.decimals or 0,
        asset=usdc.asset,
        scope=usdc.scope,
        symbol=usdc.symbol or "USDC",
        evidence="author_derived",
    )


def pair_tokens(context, config):
    """The pair's label and the position's USDC and WETH inventory."""
    position = next(
        p
        for p in context.positions
        if isinstance(p, ExchangePosition) and p.allocation == "position"
    )
    capital = config.allocation("position").capital.asset
    usdc = next(v for v in position.inventory if str(v.asset) == str(capital))
    weth = next(v for v in position.inventory if str(v.asset) != str(capital))
    pair = (context.prices[0].label if context.prices else None) or "USDC per WETH"
    return {"pair": pair, "usdc": usdc, "weth": weth}


def rsi_series(closes, period, window):
    """RSI at each close; empty until the strategy has read a full window, as it decides."""
    if len(closes) < window or len(closes) <= period:
        return []
    values = talib.RSI(np.array([float(v) for _, v in closes]), period)
    return [(t, v) for (t, _), v in zip(closes, values, strict=True) if math.isfinite(v)]


def status(now_rsi, holding, oversold, overbought):
    if now_rsi is None:
        return b.callout(
            "signal", "Waiting for prices", "RSI needs a full window of hourly closes."
        )
    if holding:
        return b.callout(
            "signal",
            "Holding WETH",
            f"Sells when RSI crosses above {overbought:g}.",
            "success",
        )
    return b.callout("signal", "Waiting to buy", f"Buys when RSI crosses below {oversold:g}.")


def now_group(context, closes, now_rsi, knobs, tokens):
    period, oversold, overbought = knobs
    latest = context.history[-1].fields if context.history else {}
    usdc = tokens["usdc"]
    return b.group(
        "now",
        "Now",
        b.metric(
            "price",
            "WETH price",
            price("price", closes[-1][1] if closes else None, "Latest close.", **tokens),
        ),
        b.metric("rsi", f"RSI ({period})", level("rsi", now_rsi, "From the latest closes.")),
        b.metric("buy_at", "Buys below", level("buy_at", oversold, "Configured oversold level.")),
        b.metric(
            "sell_at",
            "Sells above",
            level("sell_at", overbought, "Configured overbought level."),
        ),
        b.metric(
            "value", "Value", worth("value", latest.get("value"), "Wallet at the pool price.", usdc)
        ),
        b.metric(
            "hold",
            "If held",
            worth("hold", latest.get("hold"), "The starting tokens at this price.", usdc),
        ),
    )


def trade_markers(context, closes):
    return [
        b.marker(
            t,
            "Buy" if action in BUYS else "Sell",
            "success" if action in BUYS else "warning",
            "price",
        )
        for t, action, _ in b.trades(context, "position")
        if action in BUYS + SELLS and closes[0][0] <= t
    ]


def signal_chart(context, closes, rsi, knobs, tokens):
    _, oversold, overbought = knobs
    lines = [
        b.series(
            "price",
            "WETH",
            [(t, price(f"p{i}", v, "Close.", **tokens)) for i, (t, v) in enumerate(closes)],
        )
    ]
    if rsi:
        lines.append(
            b.series(
                "rsi",
                "RSI",
                [(t, level(f"r{i}", v, "RSI.")) for i, (t, v) in enumerate(rsi)],
                panel="lower",
            )
        )
    # The RSI levels belong on its panel, which exists only once RSI can be read.
    levels = [
        b.reference("Sell above", level("ob", overbought, "Overbought."), "warning"),
        b.reference("Buy below", level("os", oversold, "Oversold."), "success"),
    ]
    return b.line_chart(
        "signal_chart",
        "Price and RSI" if rsi else "Price",
        lines,
        markers=trade_markers(context, closes),
        references=levels if rsi else [],
        lower_marks=("Sell above", "Buy below") if rsi else (),
        caption="Hourly closes the strategy read and its trades; RSI below."
        if rsi
        else "Hourly closes the strategy read; RSI appears once a full window is read.",
    )


def dashboard(context: ui.Context) -> ui.Dashboard:
    config = context.config
    if not isinstance(config, Parameters):
        raise TypeError("this dashboard reads a Uniswap V3 configuration")
    # dashboard.py reads parameters by name: a plain mapping, or the typed model when the
    # workspace declares one in parameters.py.
    knobs = (
        int(param(config, "period")),
        float(param(config, "oversold")),
        float(param(config, "overbought")),
    )
    tokens = pair_tokens(context, config)
    closes = b.prices(context)
    rsi = rsi_series(closes, knobs[0], int(param(config, "window")))
    now_rsi = rsi[-1][1] if rsi else None
    holding = (tokens["weth"].amount or "0") != "0"

    blocks = [
        status(now_rsi, holding, knobs[1], knobs[2]),
        now_group(context, closes, now_rsi, knobs, tokens),
    ]
    if closes:
        blocks.append(signal_chart(context, closes, rsi, knobs, tokens))
    blocks += [
        *(
            ui.Table(id=id, title=title, data=context.datasets[key])
            for id, key, title in (
                ("inventory", "exchange_inventory", "Tokens held"),
                ("gas", "payer_costs", "Gas paid"),
                ("transactions", "transactions", "Transactions"),
            )
            if key in context.datasets
        ),
        ui.Text(
            id="scope",
            text="Values are in USDC at the pool price, not USD; gas is shown separately.",
        ),
    ]
    return b.dashboard("WETH RSI", *blocks, description="Buys oversold, sells overbought.")
