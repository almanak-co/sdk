"""Report a HyperCore backtest in USDC, with what stood in for the missing history (D4).

Equity is the fund's USDC on HyperEVM plus its HyperCore account value at the latest
trade close. `net_result` is equity at the end less equity at the start; it equals the
realized PnL, funding, fees and activation fee the account accumulated plus the
unrealized PnL of a position left open, which the report shows separately.
"""

from decimal import Decimal

from almanak.backtest.dataset import InputUnavailable
from almanak.connectors.hyperliquid import perp_math as math
from almanak.connectors.hyperliquid.economic_data import MODEL
from almanak.connectors.hyperliquid.economic_model import wallet
from almanak.connectors.hyperliquid.economic_state import (
    HyperCoreEvents,
    HyperCoreResult,
    Liquidation,
)

LABELS = {
    "market_data": "HyperCore /info trade candles and hourly funding rates (D4)",
    "book": "latest trade close moved by the fill model's slippage",
    "mark": "trade candle low and high",
    "oracle": "latest trade close",
    "costs": "excluded: HyperEVM gas and the HYPE fee of a withdrawal",
}


def report(state, model) -> dict:
    """The run's equity, actions, events and labels, in USDC base units."""
    allocations = [allocation_report(state, model, held) for held in state.portfolio.positions]
    start = sum(a["start_equity"] for a in allocations)
    end = sum(a["equity"] for a in allocations if a["equity"] is not None)
    measured = all(a["equity"] is not None for a in allocations)
    return {
        "model": MODEL,
        "mode": state.mode,
        "closed": state.mode == "finished" and model.closed(state, ()),
        "labels": LABELS,
        "fill_model": model.settings.data.fills.model_dump(mode="json"),
        "candles": model.settings.data.candles,
        "allocations": allocations,
        "start_equity": start,
        "end_equity": end if measured else None,
        "net_result": end - start if measured else None,
        "return_pct": float(Decimal(end - start) / start * 100) if measured and start else None,
        "actions": [action(value) for value in results(state)],
        "events": [event_report(item) for value in events(state) for item in value.results],
        "cancelled": [pending.intent.kind for pending in state.cancelled],
    }


def allocation_report(state, model, held) -> dict:
    """One account's equity at the latest close, or why it is unmeasured."""
    account = held.payload
    target = account.target
    cash = state.portfolio.balance(wallet(target))
    try:
        record = model.book.candle(target, state.time)
        price = Decimal(record.payload.close)
        value, missing = math.micro(math.account_value(account, price)), None
    except InputUnavailable as error:
        price, value, missing = None, None, error.gap.model_dump(mode="json")
    return {
        "allocation": target.allocation,
        "coin": target.market.coin,
        "start_equity": initial_usdc(model.settings, target),
        "wallet_usdc": cash,
        "account_value": value,
        "equity": None if value is None else cash + value,
        "reference_price": None if price is None else math.text(price),
        "missing_input": missing,
        "account": account.model_dump(mode="json"),
        "unrealized_pnl": None if price is None else math.micro(math.unrealized(account, price)),
    }


def results(state):
    return [
        value
        for completed in state.completed
        if isinstance(value := completed.transition.result, HyperCoreResult)
    ]


def events(state):
    return [
        value
        for recorded in state.events
        if isinstance(value := recorded.transition.result, HyperCoreEvents)
    ]


def event_report(item) -> dict:
    body = item.model_dump(mode="json", exclude={"before", "after", "simulation"})
    if isinstance(item, Liquidation):
        body["account_value_after"] = item.after.perp_usdc
    return body


def initial_usdc(settings, target) -> int:
    """The fund's starting USDC, the run's starting equity."""
    fund = next(f for f in settings.funds if f.name == target.account)
    return next((b.amount for b in fund.balances if b.asset == target.margin_asset), 0)


def action(value: HyperCoreResult) -> dict:
    return {
        "kind": value.intent.kind,
        "allocation": value.intent.target.allocation,
        "effective_at": value.effective_at.isoformat(),
        "success": value.success,
        "error": value.error,
        "fill": None if value.fill is None else value.fill.model_dump(mode="json"),
    }
