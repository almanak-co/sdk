"""The strategy's own price series and trades, beside the dashboard input for charts.

A decision reads its declared price series (closes over its window) and discards them; the export
keeps the latest decision's series here, so a chart shows the whole window from the first decision
and an indicator can be recomputed from the closes the strategy used. Trades come from the
checkpoint's committed outcomes. Display only: execution never reads either.
"""

import json
from decimal import Decimal
from pathlib import Path

from almanak.dashboard.history import decimal_text
from almanak.dashboard.models import PriceSample, StrategyPrices, Trade, TradeAmount

PRICES_FILE = "prices.json"
MAX_POINTS = 500
MAX_TRADES = 500


def prices_of(series_list, state) -> tuple[StrategyPrices, ...]:
    """The decision's price series as dashboard readings, newest MAX_POINTS points each."""
    rows = []
    for series in series_list or ():
        binding = series.binding
        asset = binding.asset
        quote = series.quote
        label = None
        base, counter = symbol(state, asset), None if quote is None else symbol(state, quote)
        if base and counter:
            label = f"{counter} per {base}"
        points = tuple(
            PriceSample(time=int(point.source_time), value=decimal_text(Decimal(point.value)))
            for point in series.points[-MAX_POINTS:]
        )
        rows.append(
            StrategyPrices(
                asset=asset,
                quote=quote,
                label=label,
                timeframe=getattr(binding.source, "timeframe", None),
                points=points,
            )
        )
    return tuple(rows[:8])


def keep_prices(directory: Path, rows: tuple[StrategyPrices, ...]) -> None:
    from almanak.dashboard.export import write_atomic

    write_atomic(directory / PRICES_FILE, [row.model_dump(mode="json") for row in rows])


def load_prices(directory: Path) -> tuple[StrategyPrices, ...]:
    try:
        rows = json.loads((directory / PRICES_FILE).read_text())
        return tuple(StrategyPrices.model_validate_json(json.dumps(row)) for row in rows)
    except (OSError, ValueError):
        return ()


def trades_of(state, effects) -> tuple[Trade, ...]:
    """Each successful outcome that moved the fund's tokens, with its intent and token changes.
    An outcome that moved nothing (an approval, a revoke) is not a trade; an external effect such
    as a liquidation carries no decision and is named by its kind."""
    rows = []
    for outcome in getattr(state, "outcomes", ()):
        if getattr(outcome, "success", True) is False:
            continue
        operation = getattr(outcome, "operation", None)
        target = getattr(operation, "target", None) or getattr(outcome, "target", None)
        allocation = getattr(target, "allocation", None)
        moment = getattr(outcome, "effective_at", None) or getattr(
            getattr(outcome, "after", None), "effective_at", None
        )
        if moment is None or not isinstance(allocation, str):
            continue
        moved = amounts(state, outcome, effects)
        if not moved:
            continue
        intent = getattr(getattr(outcome, "decision", None), "intent", None)
        action = (
            getattr(intent, "kind", None)
            or getattr(operation, "kind", None)
            or str(getattr(outcome, "kind", "external"))
            .removeprefix("gmx_")
            .removesuffix("_outcome")
        )
        rows.append(
            Trade(time=int(moment.timestamp()), allocation=allocation, action=action, amounts=moved)
        )
    return tuple(rows[-MAX_TRADES:])


def amounts(state, outcome, effects) -> tuple[TradeAmount, ...]:
    if effects is None:
        return ()
    try:
        deltas = effects(outcome)
    except Exception:  # noqa: BLE001 - an unreadable effect is not a guessed one
        return ()
    rows = []
    for delta in deltas:
        metadata = state.config.asset_metadata.get(delta.key.asset)
        if not delta.amount or metadata is None:
            continue
        amount = Decimal(delta.amount) / Decimal(10) ** metadata.decimals
        rows.append(
            TradeAmount(
                asset=delta.key.asset,
                symbol=symbol(state, delta.key.asset),
                amount=decimal_text(amount),
            )
        )
    return tuple(rows[:8])


def symbol(state, asset):
    from almanak.connectors.dex_dashboard import symbol_of

    return symbol_of(state, asset)
