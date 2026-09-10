"""Exact-input admission for reproduced Doppler/Rehype and EOA router runtimes."""

from __future__ import annotations

from typing import TYPE_CHECKING

from eth_utils import keccak

from almanak.framework.venues import VenueVerificationGateway

from .addresses import UNISWAP_V4
from .doppler_dependencies import _PinnedReader, observe_doppler_swap_dependencies
from .pool_key import PoolKey
from .router_deployments import router_deployment

if TYPE_CHECKING:
    from .behavior import HookEvidence

_QUOTER_RUNTIME = "0xd707b1da8cb165e5ea35a3b4450d971eb562ec171e23492aa117036b78a868f6"


class DopplerRehypeSwapProfile:
    """Both external callers receive the post-hook PoolManager output delta.

    The reproduced Rehype exempts only its own nested rebalancing swaps. The
    reproduced external quoter also rejects partial fills. Router input caps and
    output minima remain mandatory; quoting does not prove final settlement.
    """

    name = "doppler_rehype_exact_input"
    version = "1"

    def verify(
        self,
        *,
        chain: str,
        key: PoolKey,
        operation: str,
        route: str,
        hook_data: bytes,
        gateway: VenueVerificationGateway,
        block_number: int,
    ) -> HookEvidence | None:
        from .behavior import HookEvidence

        if (
            chain != "robinhood"
            or operation != "swap_exact_in"
            or route != "universal_router_eoa"
            or int(key.hooks, 16) & 0x3FFF != 0x2544
            or not key.is_dynamic
            or int(key.currency0, 16) == 0
        ):
            return None
        if hook_data != b"":
            raise ValueError("Doppler swap admission requires explicit empty hook_data")
        dependencies = observe_doppler_swap_dependencies(
            chain=chain, key=key, gateway=gateway, block_number=block_number
        )
        reader = _PinnedReader(gateway, chain, block_number)
        addresses = UNISWAP_V4[chain]
        router = router_deployment(chain, addresses["universal_router"])
        if router.runtime_hash is None:
            raise ValueError("Doppler swap admission requires an authenticated router")
        reader.runtime(router.address, router.runtime_hash)
        reader.runtime(addresses["quoter"], _QUOTER_RUNTIME)
        if (
            reader.read(addresses["quoter"], "poolManager()", [], [], ["address"])[0]
            != addresses["pool_manager"].lower()
        ):
            raise ValueError("Doppler external quoter targets a different PoolManager")
        digest = keccak(
            text=f"{self.name}:{self.version}:{dependencies.digest}:{router.address}:{router.runtime_hash}:"
            f"{addresses['quoter'].lower()}:{_QUOTER_RUNTIME}"
        )
        return HookEvidence(
            profile=self.name,
            version=self.version,
            operation=operation,
            route=route,
            chain=chain,
            hook=key.hooks,
            block_number=block_number,
            block_hash=dependencies.block_hash,
            dependency_digest="0x" + digest.hex(),
            amount_equivalent_to_quoter=True,
        )
