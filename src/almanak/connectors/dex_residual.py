"""Counter inventory whose exact-size exit quote returns no output.

A quote that consumes its whole input and returns zero output measures residual
dust; it is not missing data. An observation retains it as `exit_residual`, never
as a quote a swap can follow. Exit completion and closure accept counter inventory
only when it is exactly the residual measured in the same observation.
"""

from almanak.gateway.ethereum import DataError, Reverted


class QuoteUnfillable(ValueError):
    """Modeled pool state cannot fill an exact-input quote.

    It stays a ValueError, so every caller that refused the quote before still
    refuses it; only an entry observation records it as an unmeasured entry quote.
    """


def unfillable_reason(error):
    """Why a quote the market cannot fill is unmeasured, or None for any other failure.

    A reverting quote call, a pool without liquidity for the input and a modeled swap
    beyond prepared coverage are facts about the market at this block. Transport,
    provider and validation failures are not, and stay errors.
    """
    if isinstance(error, Reverted):
        # A fixed text keeps the reason independent of provider error codes, so a
        # verifying read of the same block reproduces the same observation.
        return "the quote call reverted"
    if isinstance(error, QuoteUnfillable) or (
        isinstance(error, DataError) and error.kind == "liquidity"
    ):
        return str(error)
    return None


def entry_or_reason(measure):
    """The entry quote fields from `measure()`, or why the entry quote is unmeasured.

    Entries repeat while funding inventory remains (blueprint 27 section 7), so the
    entry quote is measured beside exit and rebalance quotes. An entry the market cannot
    fill offers no entry and records `entry_quote_error` instead of failing the
    observation an exit or teardown needs. Missing data is never a zero quote.
    """
    try:
        return measure()
    except (DataError, QuoteUnfillable) as error:
        reason = unfillable_reason(error)
        if reason is None:
            raise
        return {"entry_quote_error": f"entry quote unavailable: {reason}"}


def refuse_unmeasured_entry(observed, refusal):
    """Refuse a swap along an entry quote the observation records as unmeasured.

    The observation already holds why the market could not fill the entry, for example
    the prepared tick coverage a modeled swap left; the refusal names it instead of
    reporting only that the quote is absent.
    """
    cause = getattr(observed, "entry_quote_error", None)
    if observed.entry_quote is None and cause is not None:
        raise ValueError(f"{refusal}: {cause}")


def validate_entry_error(entry_quote, entry_quote_error):
    if entry_quote is not None and entry_quote_error is not None:
        raise ValueError("an observation cannot offer an entry quote it records as unmeasured")


def counter_of(owned):
    return next(
        value
        for value in (owned.inventory0, owned.inventory1)
        if value.asset == owned.target.counter_currency
    )


def counter_settled(owned, residual):
    """Counter inventory needs no exit swap: it is zero or exactly the measured residual."""
    counter = counter_of(owned)
    return not counter.amount or (residual is not None and residual.amount_in == counter)


def validate_quotes(owned, offered, exit_quote, residual):
    if any(value is not None and not value.amount_out.amount for value in offered):
        raise ValueError("a decision quote requires a positive measured output")
    if residual is None:
        return
    if (
        exit_quote is not None
        or owned.position is not None
        or residual.amount_out.amount
        or residual.amount_in != counter_of(owned)
    ):
        raise ValueError("an exit residual measures all closed counter inventory with no output")


def classified(key, measured):
    """A positive quote is offered; a zero-output exit quote is retained as residual."""
    if measured.amount_out.amount:
        return {key: measured}
    # A zero-output entry or rebalance quote offers no swap and is absent.
    return {"exit_residual": measured} if key == "exit_quote" else {}


def residual_measurement(owned, quote):
    """The exit residual after an exit leg, or nothing when none is measured.

    An unavailable quote leaves the counter obligation visible instead of failing
    the observation that records an already mined leg.
    """
    counter = counter_of(owned)
    if owned.position is not None or not counter.amount:
        return {}
    try:
        measured = quote(counter)
    except DataError:
        return {}
    return {} if measured.amount_out.amount else {"exit_residual": measured}


def require_liquidity(quote, reserves):
    """A zero output measures dust only while both reserves of the pair are positive."""
    if not quote.amount_out.amount and not min(reserves):
        raise QuoteUnfillable("a zero-output quote requires positive reserves on both sides")


def residual_leg(unsettled, before, cleanup, message):
    """The exit leg once no swap remains: cleanup, or an explicit refusal."""
    if unsettled:
        raise ValueError(message)
    if not before.needs_cleanup:
        # The prior leg measured a swappable balance that is now residual dust.
        raise ValueError("exit has no remaining leg: counter inventory is measured residual")
    return cleanup(before)
