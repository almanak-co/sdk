"""Runtime-bound admission for Doppler's event-only liquidity callbacks."""

from __future__ import annotations

from typing import TYPE_CHECKING

from eth_abi import decode
from eth_utils import keccak

from almanak.framework.venues import VenueTargetRole, VenueVerificationGateway

from .addresses import UNISWAP_V4
from .pool_key import PoolKey
from .venue_verifier import address_ref

if TYPE_CHECKING:
    from .behavior import HookEvidence

# Runtime includes the deployed PoolManager and Airlock immutables.
_ROBINHOOD_INITIALIZER_RUNTIME = "0xc41a91106002f15bf70ae266824317f3f3ac638ac72ca5253bae395fa47ee631"


class DopplerLiquidityProfile:
    """The authenticated callbacks return ZERO_DELTA without nested hook dispatch."""

    name = "doppler_event_only_liquidity"
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
            or operation not in {"lp_open", "lp_close", "lp_collect_fees"}
            or route != "position_manager_eoa"
            or int(key.hooks, 16) & 0x3FFF != 0x2544
        ):
            return None
        target = address_ref(VenueTargetRole.PERMISSION_TARGET, key.hooks)
        runtime_hash = "0x" + keccak(gateway.code(chain=chain, target=target, block_number=block_number)).hex()
        if runtime_hash != _ROBINHOOD_INITIALIZER_RUNTIME:
            return None
        if hook_data != b"":
            raise ValueError("Doppler liquidity admission requires explicit empty hook_data")
        manager = decode(
            ["address"],
            gateway.read(
                chain=chain, target=target, payload=keccak(text="poolManager()")[:4], block_number=block_number
            ),
        )[0]
        if manager.lower() != UNISWAP_V4[chain]["pool_manager"].lower():
            raise ValueError("Doppler liquidity hook targets a different PoolManager")
        digest = keccak(text=f"{self.name}:{self.version}:{chain}:{key.hooks}:{runtime_hash}:{manager.lower()}")
        return HookEvidence(
            profile=self.name,
            version=self.version,
            operation=operation,
            route=route,
            chain=chain,
            hook=key.hooks,
            block_number=block_number,
            block_hash=gateway.block_hash(chain=chain, block_number=block_number),
            dependency_digest="0x" + digest.hex(),
            amount_equivalent_to_quoter=True,
        )
