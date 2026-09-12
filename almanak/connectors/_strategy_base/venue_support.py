"""Whether a discovered pool is a venue a connector can execute on.

Token discovery (``dex-pools`` / ``list_token_pools``) answers with an
aggregator's venues, which include every DEX on the chain — most of which this
SDK cannot target. Without a support signal a builder reads a deep pool off the
list and aims an intent at a venue no connector owns.

The mapping from an aggregator's ``dex_id`` to a connector is declared by the
connectors themselves (``Connector.venue_dex_ids``), never by a table here: the
manifest is the single source of truth for what a connector is and where it
runs, and a framework-side table would drift from it silently. A declaration
names the PROTOCOL KEY, not just the connector, because one connector owns keys
with different execution models — answering ``aerodrome`` for a Slipstream pool
would route a concentrated-liquidity venue into the Classic LP compiler.

Three outcomes, because two would have to lie somewhere:

``supported``
    A connector declares this ``dex_id`` on this chain and can act on a pool
    there. Positive matches are sound on their own — a declaration is evidence
    about the connector that made it.
``unsupported``
    No connector matches AND every pool-capable connector on this chain has
    declared its venue ids, so "no match" is a complete answer rather than a
    gap. This is about targeting THIS POOL as an exact venue; a router
    connector may still be able to trade the pair.
``unknown``
    The provider's ``dex_id`` values are not product-distinct, or some
    pool-capable connector on this chain has not declared its venue ids. Empty
    is not zero: an undeclared connector must never read as an absent one.
"""

from __future__ import annotations

from dataclasses import dataclass

from almanak.connectors._connector_descriptor import CONNECTOR_REGISTRY, Connector
from almanak.core.intent_types import IntentType

__all__ = ["POOL_INTENTS", "VenueSupport", "VenueSupportIndex", "normalize_dex_id"]

# Intents that act on a specific pool. A connector declaring none of them on a
# chain cannot be the answer to "can I execute on this pool".
POOL_INTENTS: tuple[IntentType, ...] = (
    IntentType.SWAP,
    IntentType.LP_OPEN,
    IntentType.LP_CLOSE,
    IntentType.LP_COLLECT_FEES,
)
# A connector is pool-capable when it can OPEN a position on a pool; the close
# and collect legs alone describe an exit, not a venue a builder can target.
_ENTRY_INTENTS: tuple[IntentType, ...] = (IntentType.SWAP, IntentType.LP_OPEN)


def normalize_dex_id(dex_id: str) -> str:
    """Fold an aggregator dex id to its comparison form.

    The vendors spell one venue several ways (``uniswap_v3``,
    ``uniswap-v3-base``), but only the separator is free variation — the rest
    of the id, network segment included, is identity. Folding separators and
    case is therefore the whole normalization: matching is exact after it, so
    an unrecognised namespace can never be swept into a venue by resemblance.
    """
    return dex_id.strip().lower().replace("-", "_")


@dataclass(frozen=True)
class VenueSupport:
    """What the connector manifests say about one discovered venue.

    ``protocols`` holds protocol KEYS, which is what an intent must name to
    reach this venue's compiler — not connector names, which are coarser. It is
    a tuple because one venue can be reached by more than one connector (a DEX
    whose swap and LP legs are separate connectors); reporting only the first
    would hide a primitive the builder can actually use.
    """

    status: str
    protocols: tuple[str, ...] = ()
    intents: tuple[str, ...] = ()


_UNKNOWN = VenueSupport(status="unknown")


def _is_pool_capable(connector: Connector, chain: str, protocol: str | None = None) -> bool:
    return any(connector.supports(chain=chain, protocol=protocol, intent=intent) for intent in _ENTRY_INTENTS)


class VenueSupportIndex:
    """Per-chain view of the connector manifests' venue declarations.

    Built once per discovery response rather than memoized globally, so a
    registry change (a newly discovered connector, a test double) is picked up
    without a staleness window.
    """

    def __init__(self, chain: str) -> None:
        self.chain = chain
        self._by_dex_id: dict[str, list[tuple[Connector, str]]] = {}
        self._pool_protocols: list[str] = []
        undeclared: list[str] = []

        for connector in CONNECTOR_REGISTRY.all():
            if not _is_pool_capable(connector, chain):
                continue
            self._pool_protocols.append(connector.name)
            declared = connector.venue_dex_ids
            if declared is None or chain not in declared:
                undeclared.append(connector.name)
                continue
            for dex_id, protocol in declared[chain].items():
                self._by_dex_id.setdefault(normalize_dex_id(dex_id), []).append((connector, protocol))

        self._pool_protocols.sort()
        self.undeclared_protocols: tuple[str, ...] = tuple(sorted(undeclared))

    @property
    def complete(self) -> bool:
        """Whether a non-match on this chain is a complete answer."""
        return not self.undeclared_protocols

    @property
    def pool_protocols(self) -> tuple[str, ...]:
        """Connectors that can act on a pool on this chain, declared ids or not."""
        return tuple(self._pool_protocols)

    def classify(self, dex_id: str, *, product_distinct: bool) -> VenueSupport:
        """Classify one discovered venue by its aggregator dex id."""
        if not product_distinct or not dex_id.strip():
            return _UNKNOWN
        matches = self._by_dex_id.get(normalize_dex_id(dex_id))
        if not matches:
            return VenueSupport(status="unsupported") if self.complete else _UNKNOWN
        protocols = tuple(sorted({protocol for _, protocol in matches}))
        intents = tuple(
            intent.value
            for intent in POOL_INTENTS
            if any(c.supports(chain=self.chain, protocol=p, intent=intent) for c, p in matches)
        )
        if not intents:
            return VenueSupport(status="unsupported", protocols=protocols)
        return VenueSupport(status="supported", protocols=protocols, intents=intents)
