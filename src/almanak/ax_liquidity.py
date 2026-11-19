"""Command-line options of the direct liquidity commands of `almanak ax`.

The commands themselves run through `ax_actions`; what each one admits and how its
deployment is built is the V3 pool family's (`connectors/v3_pool_liquidity_direct.py`).
"""

from almanak.connectors.v3_pool_liquidity_direct import ADDITIONS, NFT_ACTIONS

# Commands whose step is bounded by a price tolerance and carries a deadline on-chain.
PRICED = ("open-liquidity", "increase-liquidity", "decrease-liquidity", "close-liquidity")


def arguments(name: str, action) -> None:
    """One pool by its address, then the range or the NFT and the bounds the command takes."""
    # No liquidity command takes the single --amount of a supply or a swap.
    action.set_defaults(amount=None)
    action.add_argument("--chain", required=True, help="strict CAIP-2 chain of the pool")
    action.add_argument("--pool", required=True, help="pool address")
    if name == "open-liquidity":
        range_arguments(action)
    if name in NFT_ACTIONS:
        action.add_argument(
            "--token-id",
            type=token_id,
            required=True,
            help="the position's NFT at the pool's position manager; the wallet must own it"
            " and it must be a position of --pool",
        )
    if name in ADDITIONS:
        amount_arguments(action)
    if name == "decrease-liquidity":
        action.add_argument(
            "--liquidity",
            type=liquidity_units,
            required=True,
            help="liquidity to remove, in the position's own liquidity units and below what"
            " it holds; `ax liquidity-position` reports what it holds",
        )
    if name in PRICED:
        bound_arguments(name, action)


def range_arguments(action) -> None:
    for side in ("lower", "upper"):
        action.add_argument(
            f"--tick-{side}",
            type=tick,
            required=True,
            help=f"{side} tick of the position's range, a multiple of the pool's tick spacing",
        )


def amount_arguments(action) -> None:
    for index in (0, 1):
        action.add_argument(
            f"--amount{index}",
            type=whole_units,
            default=0,
            help=f"most base units of the pool's currency{index} the step may use"
            " (default: 0); at least one of the two amounts must be above zero",
        )


def bound_arguments(name: str, action) -> None:
    from almanak.ax_actions import DEFAULT_DEADLINE_SECONDS, seconds, slippage_bps

    if name in ADDITIONS:
        bound = (
            "the step is not signed when the pool price moved more than this since the"
            " observed block, and it reverts when it would take less of a currency than the"
            " range takes at the edges of that move"
        )
    else:
        bound = (
            "the step reverts when it would pay out either currency more than this far below"
            " the position's principal at the observed block"
        )
    action.add_argument(
        "--slippage-bps", type=slippage_bps, required=True, help=f"{bound}, 1 to 9999 basis points"
    )
    action.add_argument(
        "--deadline-seconds",
        type=seconds,
        default=DEFAULT_DEADLINE_SECONDS,
        help="the step reverts when mined later than this after the observed block"
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
