"""Uniswap V4 protocol connector.

Provides swap compilation, receipt parsing, and pool utilities for
Uniswap V4's singleton PoolManager architecture.

Key differences from V3:
- Singleton PoolManager contract (all pools in one contract)
- Pool keys include hooks address (currency0, currency1, fee, tickSpacing, hooks)
- Native ETH support (no mandatory WETH wrapping)
- Flash accounting model
- New Swap event signature from PoolManager

Exact pool selection:
    Static fees are integers from 0 to 1_000_000 in hundredths of a basis
    point. Tick spacing is independent of fee; custom pools require explicit
    spacing. A dynamic pool uses raw fee 0x800000 in its immutable key, while
    stored fees and per-swap overrides are mutable observations.

    Pass all five canonical fields through ``swap_params['pool_key']``, or
    resolve ``swap_params['pool_id']`` through the gateway. Redundant pins
    must agree. Native currency is address zero; WETH is a distinct asset.

Example:
    from decimal import Decimal
    from almanak.connectors.uniswap_v4 import PoolKey
    from almanak.framework.intents import Intent

    key = PoolKey(
        "0x0000000000000000000000000000000000000000",
        "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
        fee=1000, tick_spacing=20,
    )
    intent = Intent.swap(
        from_token="USDC", to_token="ETH", amount=Decimal("3"),
        protocol="uniswap_v4", max_slippage=Decimal("0.005"),
        swap_params={"pool_key": key.to_wire()},
    )

LP and hook qualification:
    LP entry accepts a pool ID and ``protocol_params['pool_key']``. Withdrawal
    verifies the owned NFT's key and liquidity. Supply both explicit withdrawal
    minima or let measured principal establish the configured per-leg floors.

    Hooked operations require explicit ``hook_data`` and a reviewed operation
    profile. The built-in profile admits callbacks that are unreachable for
    the requested operation, including compatible stored-dynamic pools.
    Per-swap override hooks need family-specific quote/execution evidence;
    deterministic ABI tests do not admit arbitrary deployed hooks. Unqualified
    custom accounting, subscribers and Safe contexts fail closed.

    Execution rechecks bound identity, approvals, calldata, deployed route,
    expiry and block continuity before signing and submission. Mutable fees
    can invalidate a quote; on-chain input/output bounds remain authoritative.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .adapter import (
        UniswapV4Adapter,
        UniswapV4Config,
        UniswapV4UnsupportedPoolError,
    )
    from .compiler import UniswapV4Compiler
    from .hooks import (
        HookDataEncoder,
        HookFlags,
        PoolDiscoveryResult,
        PoolState,
        discover_pool,
    )
    from .pool_key import PoolKey
    from .receipt_parser import UniswapV4ReceiptParser
    from .sdk import UniswapV4SDK

__all__ = [
    "HookDataEncoder",
    "HookFlags",
    "PoolDiscoveryResult",
    "PoolKey",
    "PoolState",
    "UniswapV4Adapter",
    "UniswapV4Compiler",
    "UniswapV4Config",
    "UniswapV4ReceiptParser",
    "UniswapV4SDK",
    "UniswapV4UnsupportedPoolError",
    "discover_pool",
]

_LAZY: dict[str, tuple[str, str]] = {
    "HookDataEncoder": (".hooks", "HookDataEncoder"),
    "HookFlags": (".hooks", "HookFlags"),
    "PoolDiscoveryResult": (".hooks", "PoolDiscoveryResult"),
    "PoolKey": (".pool_key", "PoolKey"),
    "PoolState": (".hooks", "PoolState"),
    "UniswapV4Adapter": (".adapter", "UniswapV4Adapter"),
    "UniswapV4Compiler": (".compiler", "UniswapV4Compiler"),
    "UniswapV4Config": (".adapter", "UniswapV4Config"),
    "UniswapV4ReceiptParser": (".receipt_parser", "UniswapV4ReceiptParser"),
    "UniswapV4SDK": (".sdk", "UniswapV4SDK"),
    "UniswapV4UnsupportedPoolError": (".adapter", "UniswapV4UnsupportedPoolError"),
    "discover_pool": (".hooks", "discover_pool"),
}

_registered = False


def _register_once() -> None:
    """Compatibility no-op; strategy registration lives in connector.py."""
    global _registered
    if _registered:
        return
    _registered = True


def __getattr__(name: str) -> Any:
    """PEP 562 lazy attribute access."""
    if name not in _LAZY:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    submodule, attr = _LAZY[name]
    import importlib

    module = importlib.import_module(submodule, package=__name__)
    value = getattr(module, attr)
    globals()[name] = value
    _register_once()
    return value
