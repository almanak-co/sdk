"""Repayment funding shared by the lending connectors.

Interest is not prefunded, so a repayment can find the fund short of its debt.
That is an operator-resolvable condition, not an authority failure: the refusal
names the shortfall and the asset, and a transfer to the fund resolves it without
a restart or owner transaction. While debt is open, every decision also retains a
coverage measurement so the gap is visible long before teardown.
"""

import math
from typing import Literal

from almanak.caip import AssetType
from almanak.gateway.ethereum import DataError
from almanak.lending_intents import Name
from almanak.models import Block, Model, UInt

WAD = 10**18
RAY = 10**27
SECONDS_PER_YEAR = 365 * 24 * 60 * 60


def shortfall(debt: int, available: int) -> int:
    """Base units the fund lacks to repay `debt` from `available`; zero when covered."""
    return max(0, debt - available)


def require_repayment_funding(allocation: str, asset: AssetType, debt: int, available: int):
    missing = shortfall(debt, available)
    if missing:
        raise DataError(
            "repayment_funding",
            f"{allocation}: repaying {debt} base units of {asset} needs {missing} more than "
            f"the fund's {available}; send the shortfall to the fund to continue, "
            "no restart or owner transaction is needed",
        )


class Projection(Model):
    """A model output, never a measurement: the observed borrow rate held constant."""

    model: Literal["constant_observed_borrow_rate"] = "constant_observed_borrow_rate"
    borrow_rate_per_second_wad: UInt
    # None when the debt cannot grow at this rate, so no shortfall develops.
    seconds_until_shortfall: UInt | None


class Coverage(Model):
    """Debt against the loan asset the fund can repay from, at one observed block.

    `available` is what a full repayment may spend: the fund's wallet balance, composed
    strategies included (their partial repayments stay within the retained
    inventory). A missing
    observation stays None; it is never reported as zero.
    """

    allocation: Name
    asset: AssetType
    block: Block | None
    debt: UInt | None
    available: UInt | None
    shortfall: UInt | None
    projection: Projection | None = None


def seconds_until_shortfall(debt: int, available: int, rate_per_second_wad: int) -> int | None:
    """Seconds until debt compounding per second at a constant rate exceeds `available`."""
    if debt >= available:
        return 0
    if not debt or not rate_per_second_wad:
        return None
    rate = rate_per_second_wad / WAD
    return math.floor(math.log(available / debt) / math.log1p(rate))


def coverage(
    allocation: str,
    asset: AssetType,
    block: Block | None,
    debt: int | None,
    available: int | None,
    rate_per_second_wad: int | None = None,
) -> Coverage:
    if debt is None or available is None:
        return Coverage(
            allocation=allocation,
            asset=asset,
            block=block,
            debt=debt,
            available=available,
            shortfall=None,
        )
    projection = (
        None
        if rate_per_second_wad is None
        else Projection(
            borrow_rate_per_second_wad=rate_per_second_wad,
            seconds_until_shortfall=seconds_until_shortfall(debt, available, rate_per_second_wad),
        )
    )
    return Coverage(
        allocation=allocation,
        asset=asset,
        block=block,
        debt=debt,
        available=available,
        shortfall=shortfall(debt, available),
        projection=projection,
    )


def annual_ray_per_second_wad(rate_ray: int) -> int:
    """Convert an annual rate in ray (Aave's variable borrow rate) to a per-second wad."""
    return rate_ray // SECONDS_PER_YEAR // (RAY // WAD)


def latest_coverage(previous: tuple[Coverage, ...], current: tuple[Coverage, ...], observed):
    """This decision's rows; open debt that went unobserved stays visible as missing."""
    rows = {row.allocation: row for row in current}
    seen = {observation.allocation for observation in observed}
    for row in previous:
        if row.allocation not in rows and row.allocation not in seen:
            rows[row.allocation] = coverage(row.allocation, row.asset, None, None, None)
    return tuple(rows[name] for name in sorted(rows))


def transitions(previous: tuple[Coverage, ...], current: tuple[Coverage, ...]):
    """Structured events when an allocation's measured shortfall starts or ends."""
    before = {row.allocation: row for row in previous}
    rows = {row.allocation: row for row in current}
    for name, row in before.items():
        # Repaid debt drops its row; an open shortfall then ends with it.
        if name not in rows and row.shortfall:
            yield {
                "event": "repayment_shortfall_cleared",
                "allocation": name,
                "asset": str(row.asset),
            }
    for row in current:
        previous_row = before.get(row.allocation)
        was = previous_row.shortfall if previous_row else None
        now = row.shortfall
        if now is None:
            continue
        if now and not was:
            yield {
                "event": "repayment_shortfall",
                "allocation": row.allocation,
                "asset": str(row.asset),
                "debt": str(row.debt),
                "available": str(row.available),
                "shortfall": str(now),
            }
        elif was and not now:
            yield {
                "event": "repayment_shortfall_cleared",
                "allocation": row.allocation,
                "asset": str(row.asset),
            }
