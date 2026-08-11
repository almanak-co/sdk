"""Typed strategy declaration for connector-native perp price history.

The funding lane needs the human-readable pair label while the price lane
prefers the venue market-token address.  Keeping both spellings in one record
prevents basket strategies from losing that distinction during preflight.
"""

from __future__ import annotations

from dataclasses import dataclass

from .data_provider import is_address_like


@dataclass(frozen=True, slots=True)
class PerpPriceHistoryTarget:
    """One strategy-declared perpetual market used by backtest data lanes.

    Strategies with dynamic or multi-market configuration may expose
    ``backtest_perp_price_history_targets()`` and return a sequence of these
    records. ``market`` is the pair label consumed by funding history;
    ``market_address`` is the optional address-first spelling consumed by the
    connector-native price provider.
    """

    protocol: str
    market: str
    market_address: str | None = None

    def __post_init__(self) -> None:
        protocol = self.protocol.strip().lower().replace("-", "_") if isinstance(self.protocol, str) else ""
        market = self.market.strip() if isinstance(self.market, str) else ""
        if not protocol:
            raise ValueError("Perp price-history target protocol must be a non-empty string")
        if not market:
            raise ValueError("Perp price-history target market must be a non-empty pair label")
        market_address = self.market_address
        if market_address is not None:
            market_address = market_address.strip() if isinstance(market_address, str) else ""
            if not is_address_like(market_address):
                raise ValueError(
                    f"Perp price-history target market_address {self.market_address!r} is not a token address"
                )
        object.__setattr__(self, "protocol", protocol)
        object.__setattr__(self, "market", market)
        object.__setattr__(self, "market_address", market_address)

    @property
    def price_market(self) -> str:
        """Address-first market spelling for connector price-history routing."""
        return self.market_address or self.market


__all__ = ["PerpPriceHistoryTarget"]
