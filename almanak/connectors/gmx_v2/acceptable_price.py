"""Derivation of the GMX V2 ``acceptablePrice`` order bound (VIB-6219).

Before this module the connector's compiler shipped a *degenerate sentinel* on
every perp open and close::

    acceptable_price = Decimal(10**30) if intent.is_long else Decimal("0")   # open
    acceptable_price = Decimal("0") if intent.is_long else Decimal(10**30)   # close

Those are the "accept literally any execution price" values. The user's
``intent.max_slippage`` was converted to basis points, handed to the adapter as
``default_slippage_bps`` — and never read to derive a price. Every GMX position
this SDK opened or closed was unbounded.

What ``acceptablePrice`` actually protects
-----------------------------------------
This is **not** mempool sandwich protection. GMX V2 orders are executed by a
keeper against *signed oracle prices at execution time*, some blocks after the
order is submitted. ``acceptablePrice`` is the caller's bound on how far the
index price may move between submission and keeper execution before the order
should be rejected instead of filled.

GMX's own check (``PositionUtils.validateAcceptablePrice`` in gmx-synthetics)
reduces to a single question — is this leg a buy or a sell?

===============  =========  =====================  =================
order            isLong     economic direction     acceptablePrice
===============  =========  =====================  =================
increase (open)  True       buying the index       **maximum**
increase (open)  False      selling the index      **minimum**
decrease (close) True       selling the index      **minimum**
decrease (close) False      buying the index       **maximum**
===============  =========  =====================  =================

i.e. ``bound_is_maximum = is_long if is_increase else not is_long``.

The pre-VIB-6219 sentinels are an independent confirmation of that table: the
"accept anything" value was ``10**30`` (an absurdly large *maximum*) exactly in
the two rows above that say maximum, and ``0`` (an absurdly small *minimum*) in
the two that say minimum. The directions were always right; only the magnitudes
were missing.

Price units
-----------
GMX stores prices as **USD per smallest unit of the index token, scaled by
1e30** — i.e. a raw integer with ``30 - index_token_decimals`` decimals::

    raw = usd_per_token * 10 ** (30 - index_token_decimals)

``acceptablePrice`` is compared directly against ``executionPrice``, which this
repo already decodes with that exact convention (see
``receipt_parser._execution_price_usd`` and the ``3000 * 10**12`` / ``65000 *
10**22`` fixtures in ``tests/unit/connectors/gmx_v2/test_perp_fill_data_vib3873.py``
for ETH at 18 decimals and BTC at 8).

Failure model
-------------
Every failure path here raises :class:`UnprotectedTradeError` from the shared
``min_out_guard`` contract, because every one of them means "we cannot state a
real bound". Callers must surface that as ``CompilationStatus.FAILED`` with
``is_safety_refusal=True`` (VIB-5746) — zero calldata was built, so it is a
guard success, not a fault, and it must not trip the circuit breaker. A failure
to *read* the index price is a different class and is classified
``is_transient=True`` by the caller, so an RPC blip cannot turn into a permanent
teardown failure.
"""

from __future__ import annotations

from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal, localcontext

from almanak.framework.intents.min_out_guard import (
    UnprotectedTradeError,
    require_protective_min,
    slippage_bps_to_fraction,
)

__all__ = ["bound_is_maximum", "derive_acceptable_price_30dec", "price_30dec_to_usd"]

#: GMX's fixed USD scale. A price is ``usd_per_token * 10 ** (GMX_PRICE_SCALE_EXPONENT
#: - index_token_decimals)``.
GMX_PRICE_SCALE_EXPONENT = 30


def bound_is_maximum(*, is_long: bool, is_increase: bool) -> bool:
    """Return True when ``acceptablePrice`` is an upper bound for this leg.

    See the module docstring's table. Buying (opening a long, closing a short)
    bounds from above; selling (opening a short, closing a long) bounds from
    below.
    """
    return is_long if is_increase else not is_long


def price_30dec_to_usd(price_30dec: int, index_token_decimals: int) -> Decimal:
    """Invert :func:`derive_acceptable_price_30dec`'s scaling back to plain USD.

    Used for human-facing surfaces (order metadata, logs, the adapter's
    documented plain-USD ``acceptable_price`` parameter). The value encoded into
    calldata is always the 30-decimal integer, never this.
    """
    return Decimal(price_30dec) / (Decimal(10) ** (GMX_PRICE_SCALE_EXPONENT - index_token_decimals))


def derive_acceptable_price_30dec(
    *,
    index_price_usd: Decimal,
    index_token_decimals: int,
    slippage_bps: int,
    is_long: bool,
    is_increase: bool,
    context: str,
) -> int:
    """Derive the GMX-native ``acceptablePrice`` for one perp leg.

    Args:
        index_price_usd: Current index price in plain USD per whole token.
            Must be strictly positive — a zero or missing price cannot bound
            anything, and encoding a bound derived from it would be worse than
            refusing.
        index_token_decimals: Decimals of the market's index token, used for
            GMX's ``30 - decimals`` price scale. Callers must source this from
            ``addresses.index_token_decimals`` and refuse when it is ``None``;
            a wrong scale silently produces a bound off by orders of magnitude.
        slippage_bps: The user's tolerance in basis points, in ``[0, 10_000)``.
            ``10_000`` (100%) is rejected by ``slippage_bps_to_fraction``: it
            derives a zero minimum, which is the sentinel this module exists to
            delete.
        is_long: Position direction.
        is_increase: True for an open/increase, False for a close/decrease.
        context: Human-readable call-site description used in error messages.

    Returns:
        The bound as a GMX 30-decimal-convention integer, ready to ABI-encode
        into ``createOrder``'s ``numbers.acceptablePrice``.

    Raises:
        UnprotectedTradeError: If the price is non-positive, the decimals are
            out of range, the tolerance is out of range, or the derived bound
            is not strictly positive.
    """
    # Finiteness is checked BEFORE the sign comparison, not after: `Decimal("NaN")
    # <= 0` raises InvalidOperation rather than returning False, so an ordering
    # that compared first would escape this module as an undocumented exception
    # type instead of the refusal the docstring promises. (`Infinity` escaped a
    # step later, as OverflowError from `int()`.) Both are swallowed by the
    # compiler's outer `except Exception` into a generic FAILED — fail-closed, so
    # never a money leak — but a guard that raises something other than what it
    # documents cannot be relied on by callers that catch UnprotectedTradeError.
    if not index_price_usd.is_finite() or index_price_usd <= 0:
        raise UnprotectedTradeError(
            context,
            f"index price must be finite and > 0 to derive an acceptable price (got {index_price_usd})",
        )
    if index_token_decimals < 0 or index_token_decimals > GMX_PRICE_SCALE_EXPONENT:
        raise UnprotectedTradeError(
            context,
            f"index_token_decimals must be in [0, {GMX_PRICE_SCALE_EXPONENT}] "
            f"(got {index_token_decimals}) — GMX prices carry "
            f"'{GMX_PRICE_SCALE_EXPONENT} - decimals' decimals",
        )

    # Rejects [10_000, inf) and negatives. A 100% tolerance is the absence of
    # one: it derives a zero lower bound from any price.
    slippage_fraction = slippage_bps_to_fraction(slippage_bps, context=context)

    upper = bound_is_maximum(is_long=is_long, is_increase=is_increase)

    # The FLOOR/CEILING rounding below is only a one-way ratchet if the arithmetic
    # feeding it is EXACT. Under the ambient 28-digit context each multiplication
    # pre-rounds ROUND_HALF_EVEN — i.e. it can round OUTWARD — and the explicit
    # rounding then applies to an already-loosened value, so the "truncation can
    # only tighten" invariant this module advertises silently fails. Measured on
    # the ambient context: index_price_usd=Decimal("1.000000000000000000000000000001")
    # at 1 bp yielded a MINIMUM of 999900000000 where the exact value is
    # 999900000001 — one unit too low, i.e. looser than requested.
    #
    # >28-significant-digit prices are not exotic here: `Decimal(some_float)`
    # expands to the float's exact binary value (Decimal(3000.1) has 43
    # significant digits), and this module takes whatever the price oracle
    # produced.
    #
    # A local context sized from the operands makes the product exact, so the
    # single explicit rounding step is the ONLY rounding that happens. The
    # multiplier contributes at most 5 significant digits (bps/10_000 in
    # [0, 1)); the 10**k factor contributes none. +20 is slack, not a guess.
    digits = len(index_price_usd.as_tuple().digits)
    with localcontext() as ctx:
        ctx.prec = max(ctx.prec, digits + 20)
        raw_price = index_price_usd * (Decimal(10) ** (GMX_PRICE_SCALE_EXPONENT - index_token_decimals))
        if upper:
            # Round DOWN: a smaller maximum is the tighter, safer bound.
            bound = (raw_price * (Decimal(1) + slippage_fraction)).to_integral_value(rounding=ROUND_FLOOR)
        else:
            # Round UP: a larger minimum is the tighter, safer bound.
            bound = (raw_price * (Decimal(1) - slippage_fraction)).to_integral_value(rounding=ROUND_CEILING)

    value = int(bound)

    if upper:
        # A zero/negative maximum is unfulfillable rather than unsafe, but it is
        # never something we meant to encode — refuse instead of burning a
        # keeper execution fee on an order that cannot fill.
        if value <= 0:
            raise UnprotectedTradeError(
                context,
                f"derived maximum acceptable price is {value}, which no execution price "
                f"can satisfy (index_price_usd={index_price_usd}, slippage_bps={slippage_bps})",
            )
        return value

    # A zero LOWER bound is precisely the pre-VIB-6219 sentinel — "accept any
    # price, including near-total loss". ROUND_CEILING above already makes that
    # unreachable for any positive price (the classic min_out_guard failure is
    # FLOOR truncating a small value to zero), so this is defence-in-depth at
    # the shared chokepoint rather than a reachable branch. Keep it: a future
    # change to the rounding mode would make it reachable again.
    return require_protective_min(value, context=context)
