"""Strategy-side dispatch registry for connector-owned pool validation.

Sibling of the other ``_strategy_base`` registries (``CompilerRegistry``,
``AddressRegistry``, …). It owns the single protocol-identifier → owning-connector
``pool_validation`` mapping and lazily imports *only* the connector that owns a
requested protocol, so a broken sibling connector cannot poison an unrelated
lookup, and framework callers never hardcode a protocol name or a per-DEX
validator import.

The per-DEX validators have heterogeneous shape parameters (V3 fee tier,
Aerodrome ``stable`` flag, Slipstream tick spacing, TraderJoe bin step), so the
registry's :meth:`PoolValidationRegistry.validate` takes a ``params`` mapping
carrying the protocol-specific discriminator and adapts it to the owning
validator's signature. Behaviour is identical to calling the owning validator
directly — the registry only routes.
"""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING, Any, ClassVar

from almanak.connectors._strategy_base.address_registry import AbiFamily, AddressRegistry
from almanak.connectors._strategy_base.pool_validation_base import (
    PoolValidationReason,
    PoolValidationResult,
)

if TYPE_CHECKING:
    from almanak.framework.gateway_client import GatewayClient

__all__ = ["PoolValidationRegistry", "validate_pool"]


class PoolValidationRegistry:
    """Protocol-name to connector pool-validator dispatch registry."""

    # Protocol identifier -> (module path, function name) for the owning connector's
    # validator. V3 forks all route to the single uniswap_v3 validator; the forks are
    # not enumerated here — they're resolved dynamically via AbiFamily.V3_FACTORY so a
    # new fork connector needs no edit to this table.
    _EXPLICIT_LOADERS: ClassVar[dict[str, tuple[str, str]]] = {
        "aerodrome": (
            "almanak.connectors.aerodrome.pool_validation",
            "validate_aerodrome_pool",
        ),
        "aerodrome_slipstream": (
            "almanak.connectors.aerodrome.pool_validation",
            "validate_aerodrome_cl_pool",
        ),
        "traderjoe_v2": (
            "almanak.connectors.traderjoe_v2.pool_validation",
            "validate_traderjoe_pool",
        ),
    }

    _V3_LOADER: ClassVar[tuple[str, str]] = (
        "almanak.connectors.uniswap_v3.pool_validation",
        "validate_v3_pool",
    )

    # slot0() reader for the V3 pool ABI. Uniswap V3 owns the canonical V3 pool
    # interface; every V3 fork that groups under AbiFamily.V3_FACTORY exposes the
    # same slot0() shape, so the whole family routes to this one reader.
    _V3_SLOT0_LOADER: ClassVar[tuple[str, str]] = (
        "almanak.connectors.uniswap_v3.pool_validation",
        "fetch_v3_pool_sqrt_price_x96",
    )

    @classmethod
    def _normalize(cls, protocol: str) -> str:
        return protocol.lower().replace("-", "_")

    @classmethod
    def has(cls, protocol: str) -> bool:
        """Return True when ``protocol`` has a connector-owned pool validator."""
        key = cls._normalize(protocol)
        return key in cls._EXPLICIT_LOADERS or AddressRegistry.has_abi(key, AbiFamily.V3_FACTORY)

    @classmethod
    def _load(cls, module_path: str, func_name: str) -> Any:
        module = importlib.import_module(module_path)
        return getattr(module, func_name)

    @classmethod
    def validate(
        cls,
        protocol: str,
        chain: str,
        token_a: str,
        token_b: str,
        params: dict[str, Any],
        rpc_url: str | None,
        gateway_client: GatewayClient | None = None,
    ) -> PoolValidationResult:
        """Validate a pool's existence via the owning connector's validator.

        Args:
            protocol: Protocol identifier (e.g. "uniswap_v3", "aerodrome",
                "aerodrome_slipstream", "traderjoe_v2", or any V3 fork).
            chain: Chain name.
            token_a: First token address.
            token_b: Second token address.
            params: Protocol-specific shape discriminator. Recognised keys:
                ``fee_tier`` (V3), ``stable`` (Aerodrome Classic),
                ``tick_spacing`` (Aerodrome Slipstream), ``bin_step`` +
                optional ``allow_empty_reserves`` (TraderJoe V2).
            rpc_url: RPC URL for on-chain query (may be None when gateway_client given).
            gateway_client: Optional connected gateway client for gateway-routed eth_call.

        Returns:
            PoolValidationResult with exists=True/False/None.
        """
        key = cls._normalize(protocol)

        if key == "aerodrome":
            fn = cls._load(*cls._EXPLICIT_LOADERS[key])
            return fn(chain, token_a, token_b, bool(params["stable"]), rpc_url, gateway_client)

        if key == "aerodrome_slipstream":
            fn = cls._load(*cls._EXPLICIT_LOADERS[key])
            return fn(chain, token_a, token_b, int(params["tick_spacing"]), rpc_url, gateway_client)

        if key == "traderjoe_v2":
            fn = cls._load(*cls._EXPLICIT_LOADERS[key])
            return fn(
                chain,
                token_a,
                token_b,
                int(params["bin_step"]),
                rpc_url,
                gateway_client,
                allow_empty_reserves=bool(params.get("allow_empty_reserves", False)),
            )

        if AddressRegistry.has_abi(key, AbiFamily.V3_FACTORY):
            fn = cls._load(*cls._V3_LOADER)
            return fn(chain, key, token_a, token_b, int(params["fee_tier"]), rpc_url, gateway_client)

        return PoolValidationResult(
            exists=None,
            reason=PoolValidationReason.PROTOCOL_UNKNOWN,
            warning=f"Unknown protocol '{protocol}' — cannot verify pool existence",
        )

    @classmethod
    def fetch_sqrt_price(
        cls,
        protocol: str,
        pool_address: str,
        chain: str,
        rpc_url: str | None = None,
        gateway_client: GatewayClient | None = None,
    ) -> tuple[int, int] | None:
        """Read ``(sqrtPriceX96, current_tick)`` from a pool via the owning connector.

        Dispatches the ``slot0()`` read to the connector that owns ``protocol``'s
        pool ABI, so framework callers never import a per-DEX pool-reader directly.
        Every V3 fork grouped under :attr:`AbiFamily.V3_FACTORY` exposes the same
        ``slot0()`` shape, so the whole family routes to the single Uniswap V3
        reader; the per-fork pool address is supplied by the caller.

        Args:
            protocol: Protocol identifier (e.g. "uniswap_v3" or any V3 fork).
            pool_address: Pool contract address to read ``slot0()`` from.
            chain: Chain name for gateway-routed eth_call.
            rpc_url: RPC URL for on-chain query (may be None when gateway given).
            gateway_client: Optional connected gateway client for gateway-routed eth_call.

        Returns:
            ``(sqrtPriceX96, current_tick)`` on success, or ``None`` when the
            protocol has no connector-owned pool reader or the read fails.
        """
        key = cls._normalize(protocol)
        if not AddressRegistry.has_abi(key, AbiFamily.V3_FACTORY):
            return None
        fn = cls._load(*cls._V3_SLOT0_LOADER)
        return fn(pool_address, rpc_url, chain=chain, gateway_client=gateway_client)


def validate_pool(
    protocol: str,
    chain: str,
    token_a: str,
    token_b: str,
    params: dict[str, Any],
    rpc_url: str | None,
    gateway_client: GatewayClient | None = None,
) -> PoolValidationResult:
    """Module-level convenience wrapper for :meth:`PoolValidationRegistry.validate`."""
    return PoolValidationRegistry.validate(protocol, chain, token_a, token_b, params, rpc_url, gateway_client)
