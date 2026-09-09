"""Operation admission for reviewed V4 hook behavior, independent of ABI codecs."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from eth_utils import keccak

from almanak.connectors._strategy_base.v4_pool_abi import V4_ZERO_ADDRESS
from almanak.framework.venues import VenueVerificationGateway

from .pool_key import PoolKey


@dataclass(frozen=True, slots=True)
class HookEvidence:
    profile: str
    version: str
    operation: str
    route: str
    chain: str
    hook: str
    block_number: int
    block_hash: str
    dependency_digest: str
    amount_equivalent_to_quoter: bool


class HookBehaviorProfile(Protocol):
    """Reviewed family implementations authenticate each instance and its dependencies."""

    name: str
    version: str

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
        """Return covered evidence, or None for an instance outside this profile."""


class CallbackFreeOperationProfile:
    """PoolManager cannot invoke this hook during a swap, irrespective of its code.

    Hooks.sol dispatches by immutable address bits. Stored dynamic fee updates
    remain possible, so the quote is an observation and router limits are mandatory.
    This profile says nothing about liquidity, donation or initialization callbacks.
    """

    name = "callback_free_operation"
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
        masks = {"swap_exact_in": 0x00CC, "lp_open": 0x0C02, "lp_close": 0x0301, "lp_collect_fees": 0x0301}
        expected_route = "universal_router_eoa" if operation == "swap_exact_in" else "position_manager_eoa"
        if operation not in masks or route != expected_route:
            return None
        if int(key.hooks, 16) & masks[operation]:
            return None
        if hook_data != b"":
            raise ValueError("Callback-free swaps require explicit empty hook_data")
        return HookEvidence(
            profile=self.name,
            version=self.version,
            operation=operation,
            route=route,
            chain=chain,
            hook=key.hooks,
            block_number=block_number,
            block_hash=gateway.block_hash(chain=chain, block_number=block_number),
            dependency_digest="0x" + keccak(text=f"{self.name}:{self.version}:{chain}:{key.hooks}").hex(),
            amount_equivalent_to_quoter=True,
        )


# Strategy configuration cannot turn an arbitrary encoder into hook admission.
REVIEWED_PROFILES: tuple[HookBehaviorProfile, ...] = (CallbackFreeOperationProfile(),)


def admit_hook(
    *,
    chain: str,
    key: PoolKey,
    operation: str,
    route: str,
    hook_data: bytes | None,
    gateway: VenueVerificationGateway,
    block_number: int,
) -> HookEvidence | None:
    if key.hooks == V4_ZERO_ADDRESS:
        if hook_data != b"":
            raise ValueError("No-hook operations require explicit protocol empty data")
        return None
    if hook_data is None:
        raise ValueError("Hook data is absent; it cannot be treated as explicit empty bytes")
    for profile in REVIEWED_PROFILES:
        evidence = profile.verify(
            chain=chain,
            key=key,
            operation=operation,
            route=route,
            hook_data=hook_data,
            gateway=gateway,
            block_number=block_number,
        )
        if evidence is not None:
            if (evidence.chain, evidence.hook, evidence.operation, evidence.route, evidence.block_number) != (
                chain,
                key.hooks,
                operation,
                route,
                block_number,
            ):
                raise ValueError("Hook profile returned evidence for a different operation")
            if not evidence.amount_equivalent_to_quoter:
                raise ValueError(
                    "This hook route requires faithful final-route simulation; Quoter parity is unavailable"
                )
            return evidence
    raise ValueError(f"No reviewed hook behavior profile admits {operation} via {route} for {key.hooks} on {chain}")
