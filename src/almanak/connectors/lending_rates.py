"""Annual supply and borrow rates of an observed lending reserve, for `almanak ax`.

Each rate comes from a value the connector's reserve reader already observed; this
module reads nothing. The protocols state rates in different units:

- Aave V3 and SparkLend keep `liquidity_rate_ray` and `variable_borrow_rate_ray`, a
  per-year rate in ray (1e27), accrued linearly for supply and compounded per second
  for debt. APR = rate / 1e27; APY = (1 + APR / Y)^Y - 1.
- Compound V3 rates are per second in 1e18, from the Comet's own rate curves at its
  recorded utilization (the values `getSupplyRate` and `getBorrowRate` answer).
  APR = rate * Y / 1e18; APY = (1 + rate / 1e18)^Y - 1.
- Morpho Blue reports the IRM's per-second borrow rate in 1e18 (`borrowRateView`).
  Borrow APR = rate * Y / 1e18; Morpho accrues interest as e^(rate * t) - 1, so
  borrow APY = e^APR - 1. Suppliers earn the borrowers' interest less the fee, so
  the supply rate is the borrow rate * utilization * (1 - fee), utilization being
  the recorded borrow over supply assets, and supply APY = e^(supply APR) - 1:
  utilization and fee scale the rate before it compounds, as Morpho states it
  (https://docs.morpho.org/learn/concepts/irm/).

The rates are the market's for the asset it lends, named as `asset`: the reserve's
own asset for Aave V3 and SparkLend, the Comet's base asset, Morpho's loan asset.
`selected_asset` states what the asset the request selected earns: the market's
supply rates when it is the lent asset, and zero when it is collateral, which
neither a Comet nor a Morpho market lends out.

Y is 31,536,000 seconds (365 days), the year every one of these protocols uses.
APRs are exact fractions; APYs are evaluated with 50 significant digits. Both are
rounded half-even to 12 decimal places only for presentation, as ratios per year
(0.05 is 5%). Rates are the protocol's at the observed block, not a forecast.
"""

from collections.abc import Callable
from decimal import ROUND_HALF_EVEN, Decimal, localcontext
from fractions import Fraction

from almanak.connectors.compound_v3.accounting import rates as comet_rates
from almanak.models import Asset
from almanak.report_values import exact_decimal

SECONDS_PER_YEAR = 365 * 24 * 60 * 60
RAY = 10**27
WAD = 10**18
DIGITS = 50
PLACES = Decimal("1e-12")
UNIT = "ratio per year, 0.05 is 5%"


def ray_apr(rate_ray: int) -> Fraction:
    return Fraction(rate_ray, RAY)


def per_second_apr(rate: Fraction | int, scale: int = WAD) -> Fraction:
    return Fraction(rate) * SECONDS_PER_YEAR / scale


def as_decimal(value: Fraction) -> Decimal:
    return Decimal(value.numerator) / Decimal(value.denominator)


def per_second_apy(apr: Fraction) -> Decimal:
    """(1 + APR / Y)^Y - 1: the APR compounded every second."""
    with localcontext() as context:
        context.prec = DIGITS
        return (1 + as_decimal(apr / SECONDS_PER_YEAR)) ** SECONDS_PER_YEAR - 1


def continuous_apy(apr: Fraction) -> Decimal:
    """e^APR - 1: the APR compounded continuously."""
    with localcontext() as context:
        context.prec = DIGITS
        return as_decimal(apr).exp() - 1


def shown(value: Fraction | Decimal) -> str:
    exact = as_decimal(value) if isinstance(value, Fraction) else value
    with localcontext() as context:
        context.prec = DIGITS
        return format(exact.quantize(PLACES, rounding=ROUND_HALF_EVEN), "f")


def amount(raw: int, decimals: int) -> str:
    return exact_decimal(raw, decimals, trim=False)


def per_second(asset: Asset, supply: Fraction, borrow: Fraction) -> dict:
    return {
        "asset": str(asset.identifier),
        "supply_apr": shown(supply),
        "supply_apy": shown(per_second_apy(supply)),
        "borrow_apr": shown(borrow),
        "borrow_apy": shown(per_second_apy(borrow)),
        "unit": UNIT,
        "compounding": "per_second",
    }


def pool_rates(liquidity_field: str) -> Callable[..., dict]:
    """Aave V3 or SparkLend: the reserve's stored per-year ray rates.

    `liquidity_field` is the balance the reserve lends from: Aave V3's virtual
    balance, or SparkLend's physical balance, its revision having no virtual one.
    """

    def rates(observation) -> dict:
        liquidity = getattr(observation, liquidity_field)
        raw = {
            "liquidity_rate_ray": observation.liquidity_rate_ray,
            "variable_borrow_rate_ray": observation.variable_borrow_rate_ray,
            liquidity_field: liquidity,
        }
        return {
            **per_second(
                observation.asset,
                ray_apr(observation.liquidity_rate_ray),
                ray_apr(observation.variable_borrow_rate_ray),
            ),
            "available_liquidity": amount(liquidity, observation.asset.decimals),
            "raw": raw,
        }

    return rates


def comet_reserve_rates(observation) -> dict:
    """Compound V3: the Comet's per-second rates at its recorded utilization."""
    supply, borrow = comet_rates(observation)
    raw = {
        "supply_rate_per_second": supply,
        "borrow_rate_per_second": borrow,
        "base_available": observation.base_available,
    }
    return {
        **per_second(observation.base_asset, per_second_apr(supply), per_second_apr(borrow)),
        "available_liquidity": amount(observation.base_available, observation.base_asset.decimals),
        "raw": raw,
    }


def morpho_rates(observation) -> dict:
    """Morpho Blue: the IRM borrow rate; supply rate = borrow rate * utilization * (1 - fee)."""
    accounting = observation.accounting
    supplied, borrowed = accounting.supply_assets, accounting.borrow_assets
    share = Fraction(borrowed, supplied) if supplied else Fraction(0)
    share *= 1 - Fraction(accounting.fee, WAD)
    apr = per_second_apr(observation.borrow_rate)
    supply_apr = apr * share
    raw = {
        "borrow_rate_per_second": observation.borrow_rate,
        "supply_assets": supplied,
        "borrow_assets": borrowed,
        "fee": accounting.fee,
    }
    return {
        "asset": str(observation.loan_asset.identifier),
        "supply_apr": shown(supply_apr),
        "supply_apy": shown(continuous_apy(supply_apr)),
        "borrow_apr": shown(apr),
        "borrow_apy": shown(continuous_apy(apr)),
        "unit": UNIT,
        "compounding": "continuous",
        "available_liquidity": amount(max(0, supplied - borrowed), observation.loan_asset.decimals),
        "raw": raw,
    }


RATES: dict[str, Callable[..., dict]] = {
    "aave_v3": pool_rates("virtual_underlying_balance"),
    "sparklend": pool_rates("available_underlying"),
    "compound_v3": comet_reserve_rates,
    "morpho_blue": morpho_rates,
}
# Connectors whose reserve reader observes no rate; nothing is read to report one.
UNOBSERVED = {
    "aave_v4": "Aave V4's reserve observation reads no Hub drawn rate or liquidity",
}


def selected_yield(market: dict, selected) -> dict:
    """What the selected asset earns: the market's supply rates if it is lent, else zero."""
    lent = str(selected) == market["asset"]
    zero = shown(Fraction(0))
    return {
        "asset": str(selected),
        "role": "lent" if lent else "collateral",
        "supply_apr": market["supply_apr"] if lent else zero,
        "supply_apy": market["supply_apy"] if lent else zero,
    }


def rates_entry(connector: str, observation, selected) -> dict:
    """The `rates` of one observed reserve, or null with the reason none is observed.

    `selected` is the asset the request selected; the rates are the lent asset's.
    """
    if connector in UNOBSERVED:
        return {"rates": None, "rates_unavailable": UNOBSERVED[connector]}
    market = RATES[connector](observation)
    return {"rates": {**market, "selected_asset": selected_yield(market, selected)}}
