"""Yield Poker registry for paper trading.

Executes protocol-specific "poke" transactions before each trading tick
on persistent Anvil forks. These pokes trigger on-chain interest accrual
that wouldn't happen on a quiet fork where no external users are transacting.

Supported protocols are derived from the connector registry: any connector
that declares ``CONNECTOR.yield_poke`` contributes its poke function to the
``CHAIN_PROTOCOL_MAP`` for the chains it declares. The historical hardcoded
set (Aave V3 on Arbitrum, Compound V3 on Arbitrum, Morpho Blue on Ethereum)
is preserved via those three connectors' ``YieldPokeDecl`` declarations.
"""

import logging
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

__all__ = [
    "PokeResult",
    "YieldPoker",
]

# Re-export the shared types from yield_poke_base for backward compatibility.
# engine.py imports PokeResult and YieldPoker from this module.
from almanak.connectors._strategy_base.yield_poke_base import (  # noqa: E402
    PokeFunction,
    PokeResult,
)


def _derived_chain_protocol_map() -> dict[str, list[tuple[str, PokeFunction]]]:
    """Derive the chain -> [(protocol, poke_fn)] map from the connector registry.

    Lazy: the connector registry walk happens on first call only. This avoids
    slowing the paper/engine.py import path (which imports YieldPoker inside a
    try/except at line 834) and prevents import-time failures from masking as
    "YieldPoker not available".

    Importing the registration site here ensures connectors' poke declarations
    are populated into YIELD_POKE_REGISTRY before we read from it.
    """
    import almanak.connectors._strategy_yield_poke_registry  # noqa: F401 - side-effect import
    from almanak.connectors._strategy_base.yield_poke_registry import YIELD_POKE_REGISTRY

    return YIELD_POKE_REGISTRY.chain_protocol_map()


def __getattr__(name: str):  # noqa: ANN202 - PEP 562 module-level lazy attribute
    """Serve the derived CHAIN_PROTOCOL_MAP for external import compatibility.

    Note: PEP 562 __getattr__ handles module-attribute access and from-imports
    only; it does NOT handle global name lookups inside this module.
    YieldPoker.__post_init__ calls _derived_chain_protocol_map() directly
    instead of reading CHAIN_PROTOCOL_MAP as a module global.
    """
    if name == "CHAIN_PROTOCOL_MAP":
        return _derived_chain_protocol_map()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


# ---------------------------------------------------------------------------
# YieldPoker registry
# ---------------------------------------------------------------------------


@dataclass
class YieldPoker:
    """Chain-aware registry of per-protocol poke functions for interest accrual.

    Auto-registers default hooks for supported chain/protocol combinations
    derived from the connector registry (``CONNECTOR.yield_poke`` declarations).
    Additional protocols can be registered via register().

    The registry is chain-aware: poke_all() only executes hooks for the
    specified chain, avoiding failed transactions and log spam when running
    on chains where certain protocols don't exist.
    """

    _poke_hooks: dict[str, dict[str, PokeFunction]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Register default protocol poke hooks from the connector registry."""
        for chain, protocols in _derived_chain_protocol_map().items():
            for protocol, poke_fn in protocols:
                self.register(chain, protocol, poke_fn)

    def register(self, chain: str, protocol: str, poke_fn: PokeFunction) -> None:
        """Register a poke function for a protocol on a specific chain.

        Args:
            chain: Chain name (e.g., "arbitrum", "ethereum")
            protocol: Protocol name (e.g., "aave_v3")
            poke_fn: Async function(rpc_url, wallet_address) -> PokeResult
        """
        if chain not in self._poke_hooks:
            self._poke_hooks[chain] = {}
        self._poke_hooks[chain][protocol] = poke_fn
        logger.debug(f"Registered poke hook for {protocol} on {chain}")

    async def poke_all(self, chain: str, rpc_url: str, wallet_address: str) -> list[PokeResult]:
        """Execute all registered poke hooks for the specified chain.

        Each poke is executed sequentially. Failures are caught and returned
        as PokeResult(success=False) -- they never crash the paper trading session.

        Only poke hooks registered for the given chain are executed. If no hooks
        are registered for the chain, an empty list is returned with a debug log.

        Args:
            chain: Chain to poke protocols on (e.g., "arbitrum")
            rpc_url: Anvil fork RPC URL
            wallet_address: Wallet address for poke transactions

        Returns:
            List of PokeResult for each registered protocol on this chain
        """
        chain_hooks = self._poke_hooks.get(chain, {})
        if not chain_hooks:
            logger.debug(f"No poke hooks registered for chain '{chain}'")
            return []

        results: list[PokeResult] = []
        for protocol, poke_fn in chain_hooks.items():
            try:
                result = await poke_fn(rpc_url, wallet_address)
                results.append(result)
            except Exception as e:
                logger.warning(f"Poke hook for {protocol} on {chain} raised unexpected error: {e}")
                results.append(PokeResult(protocol=protocol, success=False, error=str(e)))
        return results
