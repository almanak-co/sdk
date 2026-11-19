"""Command-line options of the direct liquidity commands of `almanak ax`.

The commands themselves run through `ax_actions`; what each one admits and how its
deployment is built is the connector's: the V3 pool family's
(`connectors/v3_pool_liquidity_direct.py`), Uniswap V4's
(`connectors/uniswap_v4/liquidity_direct.py`), Curve's
(`connectors/curve/liquidity_direct.py`) or Aerodrome's
(`connectors/aerodrome/liquidity_direct.py`).

argparse cannot tie a flag to the connector, so each command's parser holds the position
flags of every connector that serves it, none of them required, and `validate` holds a
command line to the ones its connector takes.
"""

from almanak.connectors.v3_pool_liquidity_direct import ADDITIONS, NFT_ACTIONS

# Commands whose step is bounded by a price tolerance. The V3 pool family's steps also
# carry a deadline on-chain; a Curve call carries none.
PRICED = ("open-liquidity", "increase-liquidity", "decrease-liquidity", "close-liquidity")
# Every position flag a liquidity command can take, as `ActionRequest` field names.
POSITION_FLAGS = (
    "tick_lower",
    "tick_upper",
    "token_id",
    "amount0",
    "amount1",
    "amounts",
    "liquidity",
    "receive",
)
# The position flags of a concentrated-liquidity position, per command: those it
# requires, then those it also takes. A connector with other positions declares its own
# (`DirectActions.liquidity_flags`).
RANGED = {
    "open-liquidity": (("tick_lower", "tick_upper"), ("amount0", "amount1")),
    "increase-liquidity": (("token_id",), ("amount0", "amount1")),
    "decrease-liquidity": (("token_id", "liquidity"), ()),
    "collect-liquidity-fees": (("token_id",), ()),
    "close-liquidity": (("token_id",), ()),
    "revoke-exchange": ((), ()),
}
# The call of each that carries the deadline, where the connector's call carries one;
# an approval before it carries none.
STEPS = {
    "open-liquidity": "mint",
    "increase-liquidity": "increase",
    "decrease-liquidity": "decrease",
    "close-liquidity": "close",
}


def arguments(name: str, action) -> None:
    """One pool by its address or its key, then the range or the NFT and the bounds the
    command takes."""
    from almanak.ax_actions import pool_arguments

    # No liquidity command takes the single --amount of a supply or a swap.
    action.set_defaults(amount=None)
    pool_arguments(action)
    if name == "open-liquidity":
        range_arguments(action)
    if name in NFT_ACTIONS:
        action.add_argument(
            "--token-id",
            type=token_id,
            help="the position's NFT at the pool's position manager; the wallet must own it"
            " and it must be a position of the named pool (not curve or aerodrome, whose position"
            " is the wallet's LP token balance)",
        )
    if name in ADDITIONS:
        amount_arguments(action)
    if name == "decrease-liquidity":
        action.add_argument(
            "--liquidity",
            type=liquidity_units,
            required=True,
            help="liquidity to remove, in the position's own liquidity units and below what"
            " it holds; `ax liquidity-position` reports what it holds. For curve and"
            " aerodrome, the LP shares to burn, in base units of the pool's LP token and"
            " below the wallet's balance",
        )
    if name in ("decrease-liquidity", "close-liquidity"):
        action.add_argument(
            "--receive",
            type=payout,
            help="curve only, and required there: how the burn pays out. `balanced` pays"
            " every coin in the pool's current proportions; a CAIP-19 coin of the pool"
            " pays that coin alone, which trades against the pool and pays its fee",
        )
    if name in PRICED:
        bound_arguments(name, action)


def validate(parser, args) -> None:
    """Hold a liquidity command line to the position flags its connector takes."""
    from almanak.connectors.direct import binding

    declared = binding(args.connector).liquidity_flags
    required, optional = RANGED[args.ax_command] if declared is None else declared(args.ax_command)
    given = [name for name in POSITION_FLAGS if stated(args, name)]
    foreign = [name for name in given if name not in (*required, *optional)]
    if foreign:
        parser.error(f"{args.connector} {args.ax_command} takes no {flags(foreign)}")
    missing = [name for name in required if name not in given]
    if missing:
        parser.error(f"{args.connector} {args.ax_command} requires {flags(missing)}")


def stated(args, name: str) -> bool:
    """Whether the command line states a position flag; an amount of zero states none."""
    value = getattr(args, name, None)
    return value is not None and not (name in ("amount0", "amount1") and value == 0)


def flags(names) -> str:
    return ", ".join("--amount" if n == "amounts" else "--" + n.replace("_", "-") for n in names)


def range_arguments(action) -> None:
    for side in ("lower", "upper"):
        action.add_argument(
            f"--tick-{side}",
            type=tick,
            help=f"{side} tick of the position's range, a multiple of the pool's tick spacing"
            " (not curve or aerodrome, whose pools have no range)",
        )


def amount_arguments(action) -> None:
    for index in (0, 1):
        action.add_argument(
            f"--amount{index}",
            type=whole_units,
            default=0,
            help=f"most base units of the pool's currency{index} the step may use"
            " (default: 0); at least one of the two amounts must be above zero, and for"
            " aerodrome both, since its deposit takes the two in the pool's proportion (not"
            " curve)",
        )
    action.add_argument(
        "--amount",
        dest="amounts",
        action="append",
        type=coin_amount,
        metavar="COIN=UNITS",
        help="curve only, and required there: a CAIP-19 coin of the pool and the base units"
        " of it the deposit uses, as <coin>=<units>; repeat it for each coin deposited."
        " A Curve deposit uses exactly the stated amounts",
    )


def bound_arguments(name: str, action) -> None:
    from almanak.ax_actions import DEFAULT_DEADLINE_SECONDS, seconds, slippage_bps

    if name in ADDITIONS:
        bound = (
            "a uniswap_v3, pancakeswap_v3 or aerodrome_slipstream mint or increase is neither"
            " planned nor signed while the pool price is further than this from the price at"
            " the observed block; it offers no more than the liquidity its amounts buy at that"
            " price takes at the edges of the tolerance and requires what that offer yields"
            " there, so it reverts when mined at a price beyond the tolerance of the observed"
            " price in either direction, unless the tolerance reaches an edge of the range:"
            " beyond that edge, and for a range beside the price, no price reverts it and it"
            " takes only the one currency the range holds there, at most the offered amount;"
            " a uniswap_v4 mint is bounded the same way around the price its mint step is"
            " planned at; a curve deposit reverts when it would mint more"
            " than this far below the LP shares its amounts were worth at the observed block,"
            " and an aerodrome deposit when it would take or mint that much less than its"
            " amounts were worth there"
        )
        unbounded = "; an approval before it carries no deadline"
    else:
        unbounded = ""
        bound = (
            "the step reverts when it would pay out any currency more than this far below"
            " the position's principal at the observed block"
        )
    action.add_argument(
        "--slippage-bps", type=slippage_bps, required=True, help=f"{bound}, 1 to 9999 basis points"
    )
    step = STEPS[name]
    action.add_argument(
        "--deadline-seconds",
        type=seconds,
        default=DEFAULT_DEADLINE_SECONDS,
        help="no step is planned or signed later than this after the observed block, and"
        f" the {step} reverts when mined later{unbounded}; a curve call carries no deadline,"
        " so a signed curve step stays valid until it is mined or its nonce is used"
        f" (default: {DEFAULT_DEADLINE_SECONDS})",
    )


def tick(value: str) -> int:
    import argparse

    from almanak.connectors.v3_pool_domain import MAX_TICK, MIN_TICK

    digits = value[1:] if value.startswith("-") else value
    if not (digits.isascii() and digits.isdigit()) or not MIN_TICK <= int(value) <= MAX_TICK:
        raise argparse.ArgumentTypeError(f"expected a whole tick from {MIN_TICK} to {MAX_TICK}")
    return int(value)


def whole_units(value: str) -> int:
    import argparse

    if not (value.isascii() and value.isdigit()):
        raise argparse.ArgumentTypeError("expected a whole number of base units, zero or more")
    return int(value)


def token_id(value: str) -> int:
    import argparse

    if not (value.isascii() and value.isdigit()) or not 0 < int(value) < 2**256:
        raise argparse.ArgumentTypeError("expected an NFT token ID, a positive whole number")
    return int(value)


def liquidity_units(value: str) -> int:
    from almanak.ax_actions import positive

    return positive(value, "liquidity units")


def coin_amount(value: str) -> tuple[str, int]:
    """One `<CAIP-19 coin>=<base units>` pair of a deposit."""
    import argparse

    from almanak.caip import AssetType

    coin, separator, units = value.rpartition("=")
    try:
        if not separator or not (units.isascii() and units.isdigit()):
            raise ValueError
        return str(AssetType(coin)), int(units)
    except ValueError:
        raise argparse.ArgumentTypeError(
            "expected <CAIP-19 coin>=<whole number of base units>"
        ) from None


def payout(value: str) -> str:
    """`balanced`, or the CAIP-19 coin a burn pays alone."""
    import argparse

    from almanak.caip import AssetType

    if value == "balanced":
        return value
    try:
        return str(AssetType(value))
    except ValueError:
        raise argparse.ArgumentTypeError("expected `balanced` or a CAIP-19 coin") from None
