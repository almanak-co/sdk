"""The gateway's HyperCore reads for planning, admission and recovery (blueprint 30, 13.2).

Only the gateway reads the venue: `/info` answers at read time, so none of them can be a
decision's observation. These helpers reuse phase 3's `Trader` for its admission reads,
its recovery from an attempt's own evidence and its proof of the strategy's exposure. The
Trader runs over a `Scratch` store holding the checkpoint's documents in memory: whatever
it changes there is evidence for the caller to record through an outcome, never a write.
"""

from decimal import ROUND_DOWN, Decimal
from typing import TYPE_CHECKING

from almanak.connectors.hyperliquid.corewriter import HYPE_TOKEN, USDC_TOKEN
from almanak.connectors.hyperliquid.trading_store import HyperCoreState, TradingStore
from almanak.gateway.hypercore_info import (
    ActiveAssetData,
    ClearinghouseState,
    InfoRequest,
    InfoResult,
    L2Book,
    SpotClearinghouseState,
    typed,
)

if TYPE_CHECKING:
    from almanak.connectors.hyperliquid.trading_runtime import Trader, Venue

MICRO = Decimal("0.000001")


class Scratch(TradingStore):
    """An in-memory stand-in for `TradingStore`: the Trader's changes stay in the caller."""

    def __init__(self, state: HyperCoreState):
        self.state = state

    def load(self) -> HyperCoreState:
        return self.state

    def update(self, change) -> HyperCoreState:
        self.state = change(self.state)
        return self.state


def trader(venue: "Venue", state, trading=None, signer=None) -> "Trader":
    """A Trader over the checkpoint's documents, `trading` in place of its own if given."""
    # Imported here: the Trader brings the gateway's signers, and a strategy's decision
    # path loads this connector's planning without ever reading the venue.
    from almanak.connectors.hyperliquid.trading_runtime import Trader

    documents = HyperCoreState(
        custody=state.custody, trading=trading or state.trading, book=state.book
    )
    return Trader(Scratch(documents), venue, signer)


def fetch(venue: "Venue", query) -> InfoResult:
    return venue.fetch(InfoRequest(info=query))


def micro(value: Decimal) -> int:
    """A decimal USDC amount in base units, rounded down: never more than is held."""
    return int(value.quantize(MICRO, ROUND_DOWN).scaleb(6))


def spot_usdc(venue: "Venue", safe: str) -> int:
    """The Safe's spot USDC (token 0) in base units."""
    return micro(spot_total(venue, safe, USDC_TOKEN))


def spot_hype(venue: "Venue", safe: str) -> Decimal:
    """The Safe's HyperCore HYPE, in HYPE."""
    return spot_total(venue, safe, HYPE_TOKEN)


def spot_total(venue: "Venue", safe: str, token: int) -> Decimal:
    balances = typed(fetch(venue, SpotClearinghouseState(user=safe))).balances
    return sum((Decimal(b.total) for b in balances if b.token == token), Decimal(0))


def withdrawable(venue: "Venue", safe: str) -> int:
    """The perp USDC the Safe can move to spot, in base units: its margin stays."""
    return micro(Decimal(typed(fetch(venue, ClearinghouseState(user=safe))).withdrawable))


def leverage(venue: "Venue", safe: str, coin: str) -> tuple[str, int]:
    """The Safe's margin mode and leverage in `coin`, also while it holds no position."""
    applied = typed(fetch(venue, ActiveAssetData(user=safe, coin=coin))).leverage
    return applied.type, applied.value


def book(venue: "Venue", coin: str) -> InfoResult:
    return fetch(venue, L2Book(coin=coin))
