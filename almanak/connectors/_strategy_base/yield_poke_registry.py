"""Strategy-side yield-poke registry.

Lending connectors that support on-fork interest accrual declare a
``YieldPokeDecl`` on their ``CONNECTOR`` manifest. The population step in
``almanak.connectors._strategy_yield_poke_registry`` iterates the connector
registry and registers each connector's poke callable into this registry.

``YieldPoker`` derives its ``CHAIN_PROTOCOL_MAP`` from the registry rather
than maintaining a separate hand-written table.
"""

from __future__ import annotations

from almanak.connectors._strategy_base.yield_poke_base import PokeFunction

__all__ = [
    "YIELD_POKE_REGISTRY",
    "YieldPokeRegistry",
]


class YieldPokeRegistry:
    """Registry mapping (chain, protocol) -> PokeFunction.

    Populated lazily by ``_strategy_yield_poke_registry`` on first access
    via the ``CHAIN_PROTOCOL_MAP`` derivation path.
    """

    def __init__(self) -> None:
        # chain -> list of (protocol, poke_fn) in registration order
        self._map: dict[str, list[tuple[str, PokeFunction]]] = {}

    def register(self, chain: str, protocol: str, poke_fn: PokeFunction) -> None:
        """Register a poke function for (chain, protocol)."""
        if chain not in self._map:
            self._map[chain] = []
        # Replace if already registered (idempotent re-import guard)
        self._map[chain] = [(p, f) for p, f in self._map[chain] if p != protocol]
        self._map[chain].append((protocol, poke_fn))

    def chain_protocol_map(self) -> dict[str, list[tuple[str, PokeFunction]]]:
        """Return the full (chain -> [(protocol, poke_fn)]) mapping."""
        return {chain: list(entries) for chain, entries in self._map.items()}


YIELD_POKE_REGISTRY = YieldPokeRegistry()
