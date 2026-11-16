"""The share of capital an exchange allocation's entry quote offers.

Zero offers no entry quote at all, which fits a liquidity allocation that deposits
without an entry swap. A trading allocation enters only through that quote, so it
requires a positive share.
"""

from typing import Annotated

from pydantic import Field

from almanak.models import UInt

NO_ENTRY_SWAP = (
    "0 offers no entry quote: use it for a liquidity allocation that deposits without an "
    "entry swap. A trading allocation requires at least 1."
)

ENTRY_SWAP_BPS_DESCRIPTION = (
    "Share of capital each entry quote offers from the funding asset, capped by the funding "
    "inventory held; entries repeat while funding remains. " + NO_ENTRY_SWAP
)

CONCENTRATED_ENTRY_SWAP_BPS_DESCRIPTION = (
    "Share of capital each entry swaps from the funding asset into the counter asset, capped "
    "by the funding inventory held; entries repeat while funding remains. A liquidity mint "
    "then offers all inventory held, not only the swapped share, so 5000 suits a position "
    "centred on the price. A banded allocation that enters through rebalance_quote does not "
    "use it: that quote sizes itself from the centred position. " + NO_ENTRY_SWAP
)

EntrySwapBps = Annotated[UInt, Field(ge=0, le=10000)]


def require_trading_entry(liquidity_enabled: bool, entry_swap_bps: int) -> None:
    if not liquidity_enabled and not entry_swap_bps:
        raise ValueError("a trading allocation requires a positive entry_swap_bps")
