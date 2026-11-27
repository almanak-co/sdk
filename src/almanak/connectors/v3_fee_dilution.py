"""An estimate of the LP fees a hypothetical position would keep after diluting the pool.

Credited fees multiply historical fee growth by the position's liquidity, as if that
liquidity had earned beside the pool's without taking a share of its fees. A real
position of liquidity L joins the active liquidity A, so the same swap fees are split
over A + L and the position earns the credited amount times A / (A + L).

The estimate applies that factor per sample interval, with A read from the prepared
sample. It assumes the historical swaps would have happened unchanged. It is reported
beside the credited fees and never replaces them or enters a balance.
"""

from bisect import bisect_right
from fractions import Fraction

from almanak.connectors.v3_fee_growth import accrued, sampled_inside
from almanak.connectors.v3_pool_planning import concrete_range

METHOD = "sample_interval_fees_times_active_over_active_plus_own_liquidity"


def diluted(liquidity, intervals):
    """Totals over `(fees, active)` intervals of one position liquidity.

    `fees` are the credited fees of the interval in both assets and `active` is the
    pool's active liquidity while the position was in range, or None when neither sample
    of the interval had the price inside the range. Such an interval keeps its fees
    undiluted and is counted, because no active liquidity was measured for it.
    """
    credited, estimate, share, earning, unmeasured = [0, 0], [0, 0], Fraction(0), 0, 0
    for fees, active in intervals:
        if not any(fees):
            continue
        earning += 1
        unmeasured += active is None
        for index, amount in enumerate(fees):
            credited[index] += amount
            estimate[index] += amount if active is None else amount * active // (active + liquidity)
        if active is not None:
            share = max(share, Fraction(liquidity, active + liquidity))
    return tuple(credited), tuple(estimate), share, earning, unmeasured


def active_liquidity(selected, before, after):
    """The pool's active liquidity at the first of the two samples priced inside the range."""
    for sample in (before, after):
        if selected.lower <= sample.pool.tick < selected.upper:
            return sample.pool.liquidity
    return None


def growth_inside(book, target, selected, index, row):
    if target.band is None:
        return sampled_inside(row.payload, target)
    return book.bands[target].at(index, selected.lower, selected.upper).lower


def intervals(book, target, position, start, end):
    """`(fees, active)` for each sample interval the position's fee growth was read over.

    Fees are credited between the samples in force at two liquidity actions, so the
    intervals run from the sample at `start` to the sample at `end`.
    """
    timeline = book.timelines[target]
    selected = concrete_range(target, position=position)
    first, last = (bisect_right(timeline.times, time) - 1 for time in (start, end))
    previous = growth_inside(book, target, selected, first, timeline.rows[first])
    for index in range(first, last):
        before, after = timeline.rows[index], timeline.rows[index + 1]
        current = growth_inside(book, target, selected, index + 1, after)
        fees = tuple(
            accrued(position.liquidity, old, new)
            for old, new in zip(previous, current, strict=True)
        )
        yield fees, active_liquidity(selected, before.payload, after.payload)
        previous = current


def holdings(state, target):
    """`(position, start, end)` for each span the target held one unchanged position."""
    position, since = None, None
    for completed in state.completed:
        result = completed.transition.result
        if not result.success or result.intent.target != target:
            continue
        at = int(result.effective_at.timestamp())
        if position is not None:
            yield position, since, at
        position, since = result.after.position, at
    if position is not None:
        yield position, since, int(state.time)


def estimate(state, book, target):
    """Sums over every position the target held, or why the estimate is unavailable."""
    totals = [0, 0, 0, 0]
    share, earning, unmeasured = Fraction(0), 0, 0
    try:
        for position, start, end in holdings(state, target):
            credited, kept, held_share, counted, missing = diluted(
                position.liquidity, intervals(book, target, position, start, end)
            )
            for index, amount in enumerate((*credited, *kept)):
                totals[index] += amount
            share = max(share, held_share)
            earning += counted
            unmeasured += missing
    except ValueError as error:
        return {"measurement_status": "unavailable", "reason": str(error)}
    return {
        "measurement_status": "estimated",
        "method": METHOD,
        "interval_fees": tuple(totals[:2]),
        "diluted_fees": tuple(totals[2:]),
        # Six significant digits, so a share far below one percent is not rounded away.
        "max_pool_liquidity_share": float(f"{float(share):.6g}"),
        "fee_intervals": earning,
        "fee_intervals_without_active_liquidity": unmeasured,
    }
