"""Process-wide catalog of venue-verified GMX market metadata (address-first).

The ONLY way a row enters this catalog is a venue-verified resolution: a GMX
API catalogue record whose address tuple was confirmed byte-for-byte against
on-chain ``Reader.getMarket`` (``GmxV2MarketRegistry`` / ``GetPerpMarket``,
VIB-6561). There is deliberately no static seeding and no symbol axis:

* A hand-curated symbol→address table cannot be kept true (five wrong rows
  survived review until VIB-6155 audited them on-chain) and its absence rows
  read as "unsupported" — the 2026-08-07 XMR misread that motivated deleting
  ``GMX_V2_MARKETS`` outright.
* A market-token address names exactly one immutable on-chain tuple
  (``Reader.getMarket`` is a pure ``DataStore`` read), so a remembered
  verification never goes stale. Delisting removes the API row, not the
  address identity — which is why the close path may trust this catalog when
  the venue API cannot re-serve the row.

Consumers key strictly by ``(chain, market_token address)``. The catalog's
unique-label helper only lets the adapter reuse a record already verified in
this process; discovery of a new label belongs exclusively to the dynamic
registry.
"""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .market_metadata import ResolvedGmxMarket

_lock = threading.Lock()
_by_address: dict[tuple[str, str], ResolvedGmxMarket] = {}


def _key(chain: str, market_address: str) -> tuple[str, str]:
    return (chain.lower(), market_address.lower())


def remember(chain: str, market: ResolvedGmxMarket) -> None:
    """Record a venue-verified market for this process's lifetime."""
    with _lock:
        _by_address[_key(chain, market.market_token)] = market


def by_address(chain: str, market_address: str) -> ResolvedGmxMarket | None:
    """Return the verified record for a market-token address, or ``None``.

    ``None`` means "not verified in this process", never "does not exist" —
    callers must not turn a miss into an unsupported-market claim.
    """
    with _lock:
        return _by_address.get(_key(chain, market_address))


def index_symbol(chain: str, market_address: str) -> str | None:
    record = by_address(chain, market_address)
    return record.index_symbol if record is not None else None


def index_decimals(chain: str, market_address: str) -> int | None:
    record = by_address(chain, market_address)
    return record.index_token_decimals if record is not None else None


def address_for_label(chain: str, label: str) -> str | None:
    """Return the ONE verified market-token address carrying venue label ``label``.

    ``None`` on a miss — and also on an ambiguous label: GMX lists several
    collateral variants under one short label, and picking between remembered
    variants here would reintroduce exactly the order-dependence the dynamic
    registry's full-name pinning exists to prevent. Ambiguity fails closed.
    """
    chain_key = chain.lower()
    wanted = label.upper()
    with _lock:
        matches = {
            record.market_token.lower(): record.market_token
            for (row_chain, _), record in _by_address.items()
            if row_chain == chain_key and record.label.upper() == wanted
        }
    if len(matches) != 1:
        return None
    return next(iter(matches.values()))


def clear() -> None:
    """Forget every remembered market. Test isolation only."""
    with _lock:
        _by_address.clear()


__all__ = ["address_for_label", "by_address", "clear", "index_decimals", "index_symbol", "remember"]
