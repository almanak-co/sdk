"""Aerodrome pool-existence validation (connector-owned).

The Aerodrome connector owns both its Classic factory validator
(``getPool(address,address,bool)`` — selector ``0x79bc57d5``) and its
Slipstream / concentrated-liquidity factory validator
(``getPool(address,address,int24)`` — selector ``0x28af8d0b``). The Classic
factory is resolved through :class:`AddressRegistry`; Slipstream factories
come from the reviewed generation registry, and a symbolic
``(token0, token1, tickSpacing)`` key is asked of every reviewed generation so
the pool, not a constant, decides which generation executes.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from almanak.connectors._strategy_base.address_registry import AddressRegistry
from almanak.connectors._strategy_base.pool_identity_base import decode_word_int
from almanak.connectors._strategy_base.pool_validation_base import (
    ZERO_ADDRESS,
    PoolValidationReason,
    PoolValidationResult,
    decode_address,
    eth_call,
)
from almanak.connectors._strategy_base.solidly_pool_abi import SOLIDLY_FACTORY_SELECTOR
from almanak.connectors._strategy_base.v3_pool_abi import V3_TOKEN0_SELECTOR, V3_TOKEN1_SELECTOR
from almanak.connectors.aerodrome.addresses import SlipstreamDeployment, slipstream_lp_deployments

if TYPE_CHECKING:
    from almanak.framework.gateway_client import GatewayClient

__all__ = [
    "SLIPSTREAM_TICK_SPACING_SELECTOR",
    "SlipstreamKeyMatch",
    "SlipstreamKeyResolution",
    "SlipstreamPoolBinding",
    "encode_aerodrome_cl_get_pool",
    "read_slipstream_cl_pool_binding",
    "resolve_slipstream_pool_key",
    "validate_aerodrome_cl_pool",
    "validate_aerodrome_pool",
]

# Aerodrome Classic getPool(address,address,bool) selector
# See `almanak/connectors/aerodrome/abis/pool_factory.json`
_AERODROME_GET_POOL_SELECTOR = "0x79bc57d5"

# Aerodrome Slipstream CL getPool(address,address,int24) selector
_AERODROME_CL_GET_POOL_SELECTOR = "0x28af8d0b"

# Slipstream CL pool ``tickSpacing()`` selector (``int24``). Slipstream pools
# are keyed by tick spacing where Uniswap V3 pools are keyed by fee, so this
# is the pool-side counterpart of the factory's ``getPool(...,int24)`` key.
SLIPSTREAM_TICK_SPACING_SELECTOR = "0xd0c93a7c"

_MAX_INT24 = (1 << 23) - 1


def _encode_get_pool_aerodrome(token_a: str, token_b: str, stable: bool) -> str:
    """Encode getPool(address,address,bool) calldata for Aerodrome factory."""
    a = token_a.lower().replace("0x", "").zfill(64)
    b = token_b.lower().replace("0x", "").zfill(64)
    s = "1".zfill(64) if stable else "0".zfill(64)
    return _AERODROME_GET_POOL_SELECTOR + a + b + s


def encode_aerodrome_cl_get_pool(token_a: str, token_b: str, tick_spacing: int) -> str:
    """Encode getPool(address,address,int24) calldata for Aerodrome CL factory."""
    a = token_a.lower().replace("0x", "").zfill(64)
    b = token_b.lower().replace("0x", "").zfill(64)
    # tick_spacing is always positive, safe to encode as uint
    ts = hex(tick_spacing)[2:].zfill(64)
    return _AERODROME_CL_GET_POOL_SELECTOR + a + b + ts


def validate_aerodrome_pool(
    chain: str,
    token_a: str,
    token_b: str,
    stable: bool,
    rpc_url: str | None,
    gateway_client: GatewayClient | None = None,
) -> PoolValidationResult:
    """Validate that an Aerodrome Classic pool exists on-chain.

    Args:
        chain: Chain name (should be "base").
        token_a: Token A address.
        token_b: Token B address.
        stable: True for stable pool, False for volatile.
        rpc_url: RPC URL for on-chain query. If None, returns unknown unless gateway_client is available.
        gateway_client: Optional connected gateway client for gateway-routed eth_call.

    Returns:
        PoolValidationResult with exists=True/False/None.
    """
    if rpc_url is None and gateway_client is None:
        return PoolValidationResult(
            exists=None,
            reason=PoolValidationReason.RPC_UNAVAILABLE,
            warning=f"No RPC URL available — cannot verify Aerodrome pool existence on {chain}",
        )

    factory = AddressRegistry.resolve_contract_address("aerodrome", chain, "factory")
    if not factory:
        return PoolValidationResult(
            exists=None,
            reason=PoolValidationReason.FACTORY_MISSING,
            warning=f"No Aerodrome factory address for chain '{chain}' — cannot verify pool existence",
        )

    calldata = _encode_get_pool_aerodrome(token_a, token_b, stable)
    raw = eth_call(rpc_url or "", factory, calldata, chain=chain, gateway_client=gateway_client)

    if raw is None:
        return PoolValidationResult(
            exists=None,
            reason=PoolValidationReason.RPC_FAILED,
            warning=f"RPC call to Aerodrome factory failed on {chain} — cannot verify pool existence",
        )

    pool_address = decode_address(raw)
    pool_type = "stable" if stable else "volatile"

    if pool_address == ZERO_ADDRESS:
        return PoolValidationResult(
            exists=False,
            reason=PoolValidationReason.NOT_FOUND,
            error=(
                f"No Aerodrome {pool_type} pool found for "
                f"{token_a[:10]}.../{token_b[:10]}... on {chain}. "
                f"The pool may not exist or may use a different pool type "
                f"(try {'volatile' if stable else 'stable'})."
            ),
        )

    return PoolValidationResult(exists=True, reason=PoolValidationReason.CONFIRMED, pool_address=pool_address)


@dataclass(frozen=True)
class SlipstreamPoolBinding:
    """``token0``/``token1``/``tickSpacing``/``factory`` read from a Slipstream CL pool.

    Addresses are lowercase ``0x…`` strings as returned by ``decode_address``.
    ``factory`` is the pool's own claim of provenance; it selects the reviewed
    factory/position-manager generation but is never trusted on its own — the
    caller must round-trip the tuple through that reviewed factory and require
    the same pool address back.
    """

    token0: str
    token1: str
    tick_spacing: int
    factory: str


def read_slipstream_cl_pool_binding(
    pool_address: str,
    rpc_url: str | None,
    *,
    chain: str | None = None,
    gateway_client: GatewayClient | None = None,
) -> SlipstreamPoolBinding | None:
    """Read the pool-side identity tuple of an exact Slipstream CL pool address.

    Mirrors ``read_v3_pool_binding`` for the V3-family lane: the address is the
    authoritative input and this read reverses it into the ``(token0, token1,
    tick_spacing)`` key the factory and position manager need, plus the pool's
    declared ``factory()`` so the caller can pick the matching reviewed
    generation.

    Returns:
        The pool's binding, or ``None`` when any read fails or returns a value
        no real Slipstream pool would (zero token/factory address, non-positive
        tick spacing). Callers decide whether ``None`` is a hard error; this
        reader stays diagnostic.
    """
    if rpc_url is None and gateway_client is None:
        return None

    def _read(selector: str) -> bytes | None:
        return eth_call(rpc_url or "", pool_address, selector, chain=chain, gateway_client=gateway_client)

    token0_raw = _read(V3_TOKEN0_SELECTOR)
    token1_raw = _read(V3_TOKEN1_SELECTOR)
    tick_spacing_raw = _read(SLIPSTREAM_TICK_SPACING_SELECTOR)
    factory_raw = _read(SOLIDLY_FACTORY_SELECTOR)
    if token0_raw is None or token1_raw is None or tick_spacing_raw is None or factory_raw is None:
        return None

    token0 = decode_address(token0_raw)
    token1 = decode_address(token1_raw)
    factory = decode_address(factory_raw)
    if ZERO_ADDRESS in (token0, token1, factory):
        return None

    # Shared signed-word decoder (same seam as the CLAMM identity probe); a
    # Slipstream tick spacing is a positive int24, anything else is not a pool.
    tick_spacing = decode_word_int(tick_spacing_raw)
    if tick_spacing is None or tick_spacing <= 0 or tick_spacing > _MAX_INT24:
        return None

    return SlipstreamPoolBinding(token0=token0, token1=token1, tick_spacing=tick_spacing, factory=factory)


@dataclass(frozen=True)
class SlipstreamKeyMatch:
    """One reviewed generation whose factory returned a pool for a symbolic key."""

    deployment: SlipstreamDeployment
    pool_address: str


@dataclass(frozen=True)
class SlipstreamKeyResolution:
    """Outcome of asking every reviewed Slipstream generation for one key.

    ``matches`` are the generations whose factory returned a non-zero pool.
    ``unreachable`` are the generations whose factory read failed; a scan with
    any unreachable generation cannot prove uniqueness, so callers must treat
    it as unverified rather than trust a lone match.
    """

    chain: str
    token_a: str
    token_b: str
    tick_spacing: int
    matches: tuple[SlipstreamKeyMatch, ...]
    unreachable: tuple[SlipstreamDeployment, ...]
    reviewed: tuple[SlipstreamDeployment, ...]

    @property
    def unique(self) -> SlipstreamKeyMatch | None:
        """The single owning generation, or ``None`` when absent, ambiguous, or unverified."""
        if self.unreachable or len(self.matches) != 1:
            return None
        return self.matches[0]

    def describe_matches(self) -> str:
        return ", ".join(
            f"{match.deployment.generation} factory {match.deployment.factory} -> {match.pool_address}"
            for match in self.matches
        )

    def validation_result(self) -> PoolValidationResult:
        """Collapse the scan into the fail-closed shape the compiler's pool gate consumes."""
        key = f"{self.token_a[:10]}.../{self.token_b[:10]}... with tick spacing {self.tick_spacing} on {self.chain}"
        if not self.reviewed:
            return PoolValidationResult(
                exists=None,
                reason=PoolValidationReason.FACTORY_MISSING,
                warning=f"No reviewed Aerodrome Slipstream factory for chain '{self.chain}' — cannot verify pool existence",
            )
        if self.unreachable:
            names = ", ".join(f"{d.generation} ({d.factory})" for d in self.unreachable)
            return PoolValidationResult(
                exists=None,
                reason=PoolValidationReason.RPC_FAILED,
                warning=f"RPC call to Aerodrome Slipstream factory generation(s) {names} failed — cannot verify {key}",
            )
        if not self.matches:
            factories = ", ".join(f"{d.generation} ({d.factory})" for d in self.reviewed)
            return PoolValidationResult(
                exists=False,
                reason=PoolValidationReason.NOT_FOUND,
                error=(
                    f"No Aerodrome CL pool found for {key}. Probed every reviewed Slipstream factory "
                    f"generation: {factories}. The pool may not exist or may use a different tick spacing."
                ),
            )
        if len(self.matches) > 1:
            return PoolValidationResult(
                exists=False,
                reason=PoolValidationReason.AMBIGUOUS,
                error=(
                    f"Ambiguous Aerodrome Slipstream pool key {key}: more than one reviewed factory generation "
                    f"owns a pool for it ({self.describe_matches()}). Name the pool address instead of the "
                    f"symbolic key so the generation is decided by the pool."
                ),
            )
        match = self.matches[0]
        return PoolValidationResult(
            exists=True,
            reason=PoolValidationReason.CONFIRMED,
            pool_address=match.pool_address,
            factory=match.deployment.factory,
        )


def _factory_get_pool(
    chain: str,
    factory: str,
    token_a: str,
    token_b: str,
    tick_spacing: int,
    rpc_url: str | None,
    gateway_client: GatewayClient | None,
) -> str | None:
    """``getPool`` on one factory; ``None`` when the read failed, the zero address when absent."""
    calldata = encode_aerodrome_cl_get_pool(token_a, token_b, tick_spacing)
    raw = eth_call(rpc_url or "", factory, calldata, chain=chain, gateway_client=gateway_client)
    if raw is None:
        return None
    return decode_address(raw)


def resolve_slipstream_pool_key(
    chain: str,
    token_a: str,
    token_b: str,
    tick_spacing: int,
    rpc_url: str | None,
    gateway_client: GatewayClient | None = None,
) -> SlipstreamKeyResolution:
    """Ask every reviewed Slipstream generation for ``(token_a, token_b, tick_spacing)``.

    Every reviewed factory is read even after a hit: a key that resolves on
    two generations is ambiguous and must be refused, never resolved by the
    order generations happen to be listed in.
    """
    reviewed = slipstream_lp_deployments(chain)
    matches: list[SlipstreamKeyMatch] = []
    unreachable: list[SlipstreamDeployment] = []
    if rpc_url is not None or gateway_client is not None:
        for deployment in reviewed:
            pool = _factory_get_pool(chain, deployment.factory, token_a, token_b, tick_spacing, rpc_url, gateway_client)
            if pool is None:
                unreachable.append(deployment)
            elif pool != ZERO_ADDRESS:
                matches.append(SlipstreamKeyMatch(deployment=deployment, pool_address=pool))
    else:
        unreachable.extend(reviewed)
    return SlipstreamKeyResolution(
        chain=chain,
        token_a=token_a,
        token_b=token_b,
        tick_spacing=tick_spacing,
        matches=tuple(matches),
        unreachable=tuple(unreachable),
        reviewed=reviewed,
    )


def validate_aerodrome_cl_pool(
    chain: str,
    token_a: str,
    token_b: str,
    tick_spacing: int,
    rpc_url: str | None,
    gateway_client: GatewayClient | None = None,
    deployment: SlipstreamDeployment | None = None,
) -> PoolValidationResult:
    """Validate that an Aerodrome Slipstream (CL) pool exists on-chain.

    Args:
        chain: Chain name (should be "base").
        token_a: Token A address.
        token_b: Token B address.
        tick_spacing: CL pool tick spacing (e.g. 100).
        rpc_url: RPC URL for on-chain query. If None, returns unknown unless gateway_client is available.
        gateway_client: Optional connected gateway client for gateway-routed eth_call.
        deployment: Exact connector-reviewed generation to authenticate against.
            When omitted the key is asked of every reviewed generation and is
            confirmed only when exactly one owns a pool for it.

    Returns:
        PoolValidationResult with exists=True/False/None.
    """
    if rpc_url is None and gateway_client is None:
        return PoolValidationResult(
            exists=None,
            reason=PoolValidationReason.RPC_UNAVAILABLE,
            warning=f"No RPC URL available — cannot verify Aerodrome CL pool existence on {chain}",
        )

    if deployment is None:
        return resolve_slipstream_pool_key(
            chain, token_a, token_b, tick_spacing, rpc_url, gateway_client
        ).validation_result()

    if type(deployment) is not SlipstreamDeployment or deployment not in slipstream_lp_deployments(chain):
        raise ValueError(f"unreviewed Slipstream deployment for chain {chain}")

    pool_address = _factory_get_pool(chain, deployment.factory, token_a, token_b, tick_spacing, rpc_url, gateway_client)
    if pool_address is None:
        return PoolValidationResult(
            exists=None,
            reason=PoolValidationReason.RPC_FAILED,
            warning=f"RPC call to Aerodrome CL factory failed on {chain} — cannot verify pool existence",
        )

    if pool_address == ZERO_ADDRESS:
        return PoolValidationResult(
            exists=False,
            reason=PoolValidationReason.NOT_FOUND,
            error=(
                f"No Aerodrome CL pool found for "
                f"{token_a[:10]}.../{token_b[:10]}... with tick spacing {tick_spacing} on the "
                f"{deployment.generation} Slipstream factory {deployment.factory} on {chain}. "
                f"The pool may not exist or may use a different tick spacing."
            ),
        )

    return PoolValidationResult(
        exists=True, reason=PoolValidationReason.CONFIRMED, pool_address=pool_address, factory=deployment.factory
    )
