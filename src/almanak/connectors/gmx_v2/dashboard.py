"""Retained GMX inventory and request facts carry no execution authority."""

from decimal import Decimal, localcontext

from almanak.caip import AssetType
from almanak.connectors.gmx_v2.dashboard_history import USD_E30
from almanak.connectors.gmx_v2.exchange_observations import ExchangeOwnership
from almanak.connectors.gmx_v2.models import Parameters
from almanak.connectors.gmx_v2.positions import Ownership
from almanak.dashboard.history import decimal_text
from almanak.dashboard.models import Column, Context, Dataset, ExchangePosition, Scope, Value
from almanak.dashboard.projection import snapshot
from almanak.dashboard.tables import (
    WRAPPED_NATIVE_DECIMALS,
    accounts_dataset,
    several_accounts,
    symbol_of,
    transactions_dataset,
    utc,
    wrapped_native_symbol,
)


def project(state, *, captured_at=None):
    if state.integrity_error:
        raise ValueError("invalidated GMX evidence cannot produce financial projections")
    values: dict[str, Value] = {}
    positions = tuple(position_values(state, owned, values) for owned in state.owned)
    context = Context(
        config=Parameters.model_validate(
            {name: getattr(state.config, name) for name in Parameters.model_fields}
        ),
        memory=state.memory,
        positions=positions,
        values=values,
        datasets={
            "positions": position_dataset(state),
            "gm_shares": shares_dataset(state),
            "requests": request_dataset(state),
            "submitters": payer_dataset(state),
            "transactions": transactions_dataset(state, values, gmx_label),
        },
    )
    return context, snapshot(state, values, captured_at)


def position_values(state, owned, values):
    target = owned.target
    history = [
        observation
        for observation in (*state.baseline, *(o.after for o in state.outcomes), *state.closure)
        if observation.target == target
    ]
    latest = history[-1] if history else None
    inventory = []
    amounts = (
        (*owned.inventory, owned.market_tokens)
        if isinstance(owned, ExchangeOwnership)
        else owned.inventory
    )
    for index, amount in enumerate(amounts):
        metadata = state.config.asset_metadata.get(amount.asset)
        value = Value(
            id=f"{target.allocation}-inventory{index}",
            amount=str(amount.amount),
            unit="base_units",
            asset=amount.asset,
            decimals=metadata.decimals if metadata else None,
            symbol=symbol_of(state, amount.asset),
            scope=Scope(account=target.account, fund=target.fund, target=target),
            evidence="recorded" if latest else "configured",
            block=latest.block if latest else None,
            reason="Attributed fungible GM shares; marked valuation is unmeasured."
            if amount.asset == target.market.share_asset
            else "Attributed liquid inventory excludes position collateral and request escrow.",
        )
        inventory.append(value)
        values[value.id] = value
    return ExchangePosition(allocation=target.allocation, target=target, inventory=tuple(inventory))


def position_dataset(state):
    return accounts_dataset(
        keep=several_accounts(state),
        key="fund",
        columns=tuple(
            Column(key=key, label=label)
            for key, label in (
                ("allocation", "Position"),
                ("fund", "Fund"),
                ("market", "Market asset"),
                ("side", "Side"),
                ("notional", "Size, USD"),
                ("collateral", "Collateral"),
                ("entry", "Average entry, USD"),
                ("funding", "Claimable funding"),
                ("status", "Position state"),
            )
        ),
        rows=tuple(
            {
                "allocation": o.target.allocation,
                "fund": o.target.fund.address,
                "market": market_text(state, o.target.market),
                "side": o.target.side,
                "notional": usd_text(Decimal(o.position.size_usd_e30) / USD_E30)
                if o.position
                else "No retained position",
                "collateral": token_text(state, o.position.collateral) if o.position else "None",
                "entry": entry_text(state, o.position) if o.position else "None",
                "funding": ", ".join(token_text(state, a) for a in o.claimable_funding)
                or "Unobserved",
                "status": "open" if o.position else "none",
            }
            for o in state.owned
            if isinstance(o, Ownership)
        ),
        coverage=(
            "Retained position identity, size in USD, collateral, average entry "
            "(size over index tokens) and claimable funding. "
            "Marked equity, PnL and liquidation valuation are unmeasured."
        ),
    )


def market_pair(state, market):
    """A GMX market by its index and short tokens' symbols ("WETH/USDC"), when both are known."""
    index, short = symbol_of(state, market.index_asset), symbol_of(state, market.short_asset)
    return f"{index}/{short}" if index and short else None


def market_text(state, market):
    return market_pair(state, market) or str(market.share_asset)


def gm_text(state, market):
    pair = market_pair(state, market)
    return f"GM {pair}" if pair else str(market.share_asset)


def native_text(state, paid):
    """Receipt gas in whole native units with the chain's symbol ("0.0000087 ETH")."""
    asset = AssetType(paid["asset"])
    metadata = state.config.asset_metadata.get(asset)
    decimals = metadata.decimals if metadata else 18
    symbol = symbol_of(state, asset) or str(asset)
    return f"{decimal_text(Decimal(paid['amount']) / Decimal(10) ** decimals)} {symbol}"


def token_text(state, value):
    metadata = state.config.asset_metadata.get(value.asset)
    if metadata is not None:
        symbol = metadata.symbol or wrapped_native_symbol(value.asset) or str(value.asset)
        decimals = metadata.decimals
    elif wrapped := wrapped_native_symbol(value.asset):
        decimals, symbol = WRAPPED_NATIVE_DECIMALS, wrapped
    else:
        return f"{value.amount} base units of {value.asset}"
    return f"{decimal_text(Decimal(value.amount) / Decimal(10) ** decimals)} {symbol}"


def entry_text(state, position):
    metadata = state.config.asset_metadata.get(position.index_amount.asset)
    if metadata is None or not position.index_amount.amount:
        return "Unavailable"
    with localcontext() as context:
        context.prec = 60
        tokens = Decimal(position.index_amount.amount) / Decimal(10) ** metadata.decimals
        return usd_text(Decimal(position.size_usd_e30) / USD_E30 / tokens)


def usd_text(amount: Decimal) -> str:
    return f"${amount.quantize(Decimal('0.01')):,}"


def shares_dataset(state):
    return accounts_dataset(
        keep=several_accounts(state),
        key="fund",
        columns=tuple(
            Column(key=key, label=label)
            for key, label in (
                ("allocation", "Position"),
                ("fund", "Fund"),
                ("asset", "GM market"),
                ("amount", "Owned GM tokens"),
            )
        ),
        rows=tuple(
            {
                "allocation": owned.target.allocation,
                "fund": owned.target.fund.address,
                "asset": gm_text(state, owned.target.market),
                "amount": token_text(state, owned.market_tokens),
            }
            for owned in state.owned
            if isinstance(owned, ExchangeOwnership)
        ),
        coverage="Retained fungible GM shares. "
        "Marked valuation and embedded fee growth are unmeasured.",
    )


def request_dataset(state):
    return accounts_dataset(
        keep=several_accounts(state),
        key="fund",
        columns=tuple(
            Column(key=key, label=label)
            for key, label in (
                ("allocation", "Position"),
                ("key", "Request key"),
                ("fund", "Fund"),
                ("status", "Protocol request state"),
                ("fee", "Initial execution fee"),
                ("deadline", "Admission deadline"),
            )
        ),
        rows=tuple(
            {
                "allocation": r.allocation,
                "key": r.key,
                "fund": r.fund.address,
                "status": r.status,
                "fee": token_text(state, r.execution_fee),
                "deadline": utc(r.admission_deadline)
                if r.admission_deadline is not None
                else "Not applicable",
            }
            for r in state.requests[-500:]
        ),
        truncated=len(state.requests) > 500,
        coverage=(
            "Latest retained requests. Initial fee escrow is separate from submitter gas; "
            "canonical fee/refund events remain in each request receipt."
        ),
    )


def payer_dataset(state):
    from almanak.connectors.gmx_v2.reporting import payer_reports

    return Dataset(
        columns=tuple(
            Column(key=key, label=label)
            for key, label in (
                ("submitter", "Transaction submitter"),
                ("amount", "Receipt gas"),
            )
        ),
        rows=tuple(
            {
                "submitter": row["submitter"]["address"],
                "amount": native_text(state, row["gas_paid"])
                if state.initialized
                else "Unobserved",
            }
            for row in payer_reports(state)
        ),
        coverage=(
            "Only strategy-submitted receipts. External keeper gas and fiat conversion "
            "are unmeasured; protocol keeper fees are separate escrow effects."
        ),
    )


def gmx_label(state, outcome):
    """What a GMX transaction asked for, in trading terms: "Open long ETH", "Swap USDC → WETH".
    A request's keeper execution is a separate step, shown in the requests table."""
    operation = outcome.operation
    kind = operation.kind

    def token(asset, fallback=""):
        return symbol_of(state, asset) or fallback

    if kind in ("gmx_approval", "gmx_exchange_approval"):
        # An approval of zero revokes: teardown's cleanup, or the reset before a new approval.
        verb = "Revoke approval" if operation.amount.amount == 0 else "Approve"
        return f"{verb} {token(operation.amount.asset)}".strip()
    if kind == "gmx_create_order":
        target = operation.target
        market = f"{target.side} {token(target.market.index_asset)}".strip()
        held = getattr(outcome.before, "position", None)
        if operation.order_type == 2:
            return f"{'Increase' if held else 'Open'} {market}"
        closes = held is not None and operation.size_delta_usd_e30 >= held.size_usd_e30
        return f"{'Close' if closes else 'Reduce'} {market}"
    if kind == "gmx_create_swap":
        spent, received = token(operation.amount.asset), token(operation.minimum_output.asset)
        return f"Swap {spent} → {received}" if spent and received else "Swap"
    if kind == "gmx_create_deposit":
        paid = " + ".join(
            token(a.asset) for a in operation.amounts.root if a.amount and token(a.asset)
        )
        return f"Deposit {paid} for GM" if paid else "Deposit for GM"
    if kind == "gmx_create_withdrawal":
        return "Withdraw GM"
    if kind == "gmx_cancel_liquidity":
        return f"Cancel {operation.request_kind}"
    return {
        "gmx_cancel_order": "Cancel order",
        "gmx_cancel_exchange_order": "Cancel swap",
        "gmx_claim_funding": "Claim funding",
    }.get(kind, kind.removeprefix("gmx_").replace("_", " ").capitalize())
