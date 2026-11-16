"""Per-decision lending readings for dashboard charts, from the decision's fresh observation."""

from dataclasses import dataclass
from decimal import Decimal, localcontext

from almanak.connectors.lending_rates import RATES
from almanak.dashboard.history import decimal_text, prefixes
from almanak.dashboard.models import HistoryRow

WAD = Decimal(10**18)
BPS = Decimal(10_000)
MAX_FIELDS = 32
FAMILIES = ("aave_v3", "sparklend", "compound_v3", "morpho_blue", "aave_v4")
USD = "usd"


@dataclass
class Leg:
    allocation: str
    fields: dict[str, Decimal]
    # Fund wallet balance of the allocation's asset, counted once per fund and asset in `value`.
    wallet: Decimal
    wallet_key: tuple
    # Supplied plus posted collateral minus debt, in the allocation's asset.
    units: Decimal
    # The asset's price in `quote`: "usd", or the asset a Morpho market lends.
    price: Decimal | None
    quote: str


def family(observation) -> str | None:
    reserve = getattr(getattr(observation, "target", None), "reserve", None)
    kind = getattr(reserve, "kind", None)
    return kind if kind in FAMILIES else None


def rates(kind, observation) -> dict[str, Decimal]:
    try:
        market = RATES[kind](observation)
    except (ValueError, ArithmeticError, AttributeError):
        return {}
    return {
        "supply_apy": Decimal(market["supply_apy"]),
        "borrow_apy": Decimal(market["borrow_apy"]),
    }


def pool_leg(kind, observation):
    position = observation.position
    reserve = position.reserve
    unit = Decimal(10) ** reserve.asset.decimals
    debt = position.debt_assets + getattr(position, "stable_debt_assets", 0)
    fields = {"supplied": position.supplied_assets / unit, "debt": debt / unit}
    oracle = reserve.oracle
    price = None if oracle is None else Decimal(oracle.price) / oracle.base_currency_unit
    fields |= rates(kind, reserve)
    risk = position.risk
    if risk is not None:
        if risk.total_collateral_base:
            fields["ltv"] = Decimal(risk.total_debt_base) / risk.total_collateral_base
            fields["max_ltv"] = risk.ltv_bps / BPS
        if risk.total_debt_base:
            fields["health"] = risk.health_factor_wad / WAD
    return fields, price, USD, reserve.asset.decimals


def comet_leg(observation):
    account = observation.account
    market = account.market
    asset = observation.target.asset
    if observation.target.reserve.is_base:
        places = market.base_asset.decimals
        unit = Decimal(10) ** places
        fields = {"supplied": account.supplied_base / unit, "debt": account.borrowed_base / unit}
        fields |= rates("compound_v3", market)
        quote = market.base_price
    else:
        collateral = next(c for c in market.collaterals if c.asset.identifier == asset)
        places, quote = collateral.asset.decimals, collateral.price
        balances = {b.asset: b.balance for b in account.collaterals}
        fields = {"collateral": balances.get(asset, 0) / Decimal(10) ** places}
    price = None if quote is None else Decimal(quote.price) / quote.price_scale
    return fields | comet_risk(account), price, comet_quote(market), places


def comet_quote(market) -> str:
    """The unit a Comet's prices are in: USD, except on a market whose base is the chain's
    wrapped native token (cWETH), which prices every asset in that token."""
    from almanak.chains import evm_network

    base = market.base_asset.identifier
    try:
        wrapped = evm_network(base.chain).wrapped_native
    except (KeyError, ValueError):
        return USD
    return str(base) if str(base) == str(wrapped) else USD


def comet_risk(account) -> dict[str, Decimal]:
    market = account.market
    if market.base_price is None:
        return {}
    base = Decimal(10) ** market.base_asset.decimals
    debt = account.borrowed_base * Decimal(market.base_price.price) / market.base_price.price_scale
    debt /= base
    balances = {b.asset: b.balance for b in account.collaterals}
    worth = liquidate = borrow = Decimal(0)
    for collateral in market.collaterals:
        balance = balances.get(collateral.asset.identifier, 0)
        if not balance:
            continue
        if collateral.price is None:
            return {}
        value = balance * Decimal(collateral.price.price) / collateral.price.price_scale
        value /= collateral.scale
        worth += value
        liquidate += value * collateral.liquidate_factor / WAD
        borrow += value * collateral.borrow_factor / WAD
    fields: dict[str, Decimal] = {}
    if worth:
        fields |= {"ltv": debt / worth, "max_ltv": borrow / worth}
    if debt:
        fields["health"] = liquidate / debt
    return fields


def morpho_leg(observation):
    from almanak.connectors.morpho_blue.math import ORACLE_SCALE
    from almanak.connectors.morpho_blue.risk import borrowed_assets, health_factor, supplied_assets

    account = observation.account
    market = account.market
    loan = Decimal(10) ** market.loan_asset.decimals
    collateral = account.position.collateral / Decimal(10) ** market.collateral_asset.decimals
    debt = borrowed_assets(account) / loan
    # Loan asset per collateral asset, at the market's oracle.
    price: Decimal | None = None
    if market.price is not None:
        scale = Decimal(10) ** market.collateral_asset.decimals / loan
        price = Decimal(market.price.price) * scale / ORACLE_SCALE
    if observation.target.reserve.is_loan:
        fields = {"supplied": supplied_assets(account) / loan, "debt": debt}
        fields |= rates("morpho_blue", market)
        leg_price: Decimal | None = Decimal(1)
        places = market.loan_asset.decimals
    else:
        fields = {"collateral": collateral}
        leg_price, places = price, market.collateral_asset.decimals
    if price is not None and collateral:
        fields |= {"ltv": debt / (collateral * price), "max_ltv": market.source.lltv / WAD}
        health = health_factor(account)
        if health is not None:
            fields["health"] = health / WAD
    return fields, leg_price, str(market.loan_asset.identifier), places


def hub_leg(observation):
    position = observation.position
    asset = position.reserve.asset
    unit = Decimal(10) ** asset.decimals
    fields = {"supplied": position.supplied_assets / unit, "debt": position.total_debt / unit}
    if position.total_debt and observation.health_factor is not None:
        fields["health"] = observation.health_factor / WAD
    return fields, Decimal(1), str(asset.identifier), asset.decimals


LEGS = {"compound_v3": comet_leg, "morpho_blue": morpho_leg, "aave_v4": hub_leg}


def leg(observation) -> Leg | None:
    kind = family(observation)
    if kind is None:
        return None
    try:
        if kind in ("aave_v3", "sparklend"):
            fields, price, quote, places = pool_leg(kind, observation)
        else:
            fields, price, quote, places = LEGS[kind](observation)
    except (ValueError, ArithmeticError, AttributeError, StopIteration):
        return None
    target = observation.target
    wallet = observation.wallet_balance / Decimal(10) ** places
    units = fields.get("supplied", 0) + fields.get("collateral", 0) - fields.get("debt", 0)
    fields = {"wallet": wallet, **fields}
    if price is not None and quote == USD:
        fields["price_usd"] = price
    elif price is not None and (fields.get("collateral") is not None or kind == "compound_v3"):
        fields["price"] = price
    return Leg(target.allocation, fields, wallet, (target.fund, target.asset), units, price, quote)


def worth(legs, prices) -> Decimal | None:
    """Wallet plus positions of every leg in their shared quote; None without one."""
    if len({leg.quote for leg in legs}) != 1:
        return None
    seen, total = set(), Decimal(0)
    for item in legs:
        price = prices.get(item.allocation)
        if price is None:
            return None
        units = item.units
        if item.wallet_key not in seen:
            seen.add(item.wallet_key)
            units += item.wallet
        total += units * price
    return total


def lending_fields(
    observed, baseline, *, prefixed=None, required=(), allocations=()
) -> dict[str, Decimal]:
    """Each leg's readings, and the strategy's `value` and `hold` when every lending allocation in
    `required` was read: a partial total is never a value. Prefixes come from the configured
    `allocations`, so a name keeps its prefix from one reading to the next."""
    with localcontext() as context:
        context.prec = 60
        legs = [item for item in map(leg, observed) if item is not None]
        if not legs:
            return {}
        prices = {item.allocation: item.price for item in legs}
        fields: dict[str, Decimal] = {}
        complete = {item.allocation for item in legs} >= set(required)
        value = worth(legs, prices) if complete else None
        if value is not None:
            fields["value"] = value
            started = [item for item in map(leg, baseline) if item is not None]
            hold = (
                worth(started, prices) if {i.allocation for i in started} == set(prices) else None
            )
            if hold is not None:
                fields["hold"] = hold
        prefixed = len(legs) > 1 if prefixed is None else prefixed
        names = prefixes([*allocations, *(item.allocation for item in legs)])
        for item in legs:
            prefix = f"{names[item.allocation]}:" if prefixed else ""
            fields |= {f"{prefix}{key}": amount for key, amount in item.fields.items()}
        return fields


def texts(fields) -> dict[str, str]:
    return {key: decimal_text(amount) for key, amount in list(fields.items())[:MAX_FIELDS]}


def history_row(state, observed) -> HistoryRow | None:
    """The decision's lending readings: one row for every lending allocation it observed."""
    lending = [o for o in observed or () if family(o)]
    fields = lending_fields(
        lending,
        [o for o in state.baseline if family(o)],
        required=[o.target.allocation for o in state.owned if family(o)],
        allocations=[o.target.allocation for o in state.owned],
    )
    if not fields:
        return None
    block = lending[0].block
    return HistoryRow(time=block.timestamp, block=block.number, fields=texts(fields))
