"""Uniswap V4 Hook Discovery & hookData Encoding.

V4 hook addresses encode their capabilities in the last 14 bits of the address
via CREATE2 address mining. This module provides:

1. HookFlags: Decode and inspect 14-bit capability bitmask from hook addresses
2. HookDataEncoder: Base class for typed hookData encoding
3. StateViewQuery: Pool discovery via StateView.getSlot0()

Hook capability bits (from PoolManager.sol):
    Bit 13: BEFORE_INITIALIZE
    Bit 12: AFTER_INITIALIZE
    Bit 11: BEFORE_ADD_LIQUIDITY
    Bit 10: AFTER_ADD_LIQUIDITY
    Bit  9: BEFORE_REMOVE_LIQUIDITY
    Bit  8: AFTER_REMOVE_LIQUIDITY
    Bit  7: BEFORE_SWAP
    Bit  6: AFTER_SWAP
    Bit  5: BEFORE_DONATE
    Bit  4: AFTER_DONATE
    Bit  3: BEFORE_SWAP_RETURNS_DELTA
    Bit  2: AFTER_SWAP_RETURNS_DELTA
    Bit  1: AFTER_ADD_LIQUIDITY_RETURNS_DELTA
    Bit  0: AFTER_REMOVE_LIQUIDITY_RETURNS_DELTA

Example:
    from almanak.framework.connectors.uniswap_v4.hooks import HookFlags

    flags = HookFlags.from_address("0x...hook_address...")
    if flags.before_swap:
        print("Hook has beforeSwap callback")
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass

from web3 import Web3

from almanak.framework.connectors.base import HexDecoder
from almanak.framework.connectors.uniswap_v4.sdk import (
    NATIVE_CURRENCY,
    TICK_SPACING,
    PoolKey,
    _pad_address,
    _pad_int24,
    _pad_uint24,
)

logger = logging.getLogger(__name__)


# =============================================================================
# Hook Capability Flags (14-bit bitmask from hook address)
# =============================================================================

# Flag bit positions (from V4 PoolManager.sol Hooks library)
BEFORE_INITIALIZE_FLAG = 1 << 13
AFTER_INITIALIZE_FLAG = 1 << 12
BEFORE_ADD_LIQUIDITY_FLAG = 1 << 11
AFTER_ADD_LIQUIDITY_FLAG = 1 << 10
BEFORE_REMOVE_LIQUIDITY_FLAG = 1 << 9
AFTER_REMOVE_LIQUIDITY_FLAG = 1 << 8
BEFORE_SWAP_FLAG = 1 << 7
AFTER_SWAP_FLAG = 1 << 6
BEFORE_DONATE_FLAG = 1 << 5
AFTER_DONATE_FLAG = 1 << 4
BEFORE_SWAP_RETURNS_DELTA_FLAG = 1 << 3
AFTER_SWAP_RETURNS_DELTA_FLAG = 1 << 2
AFTER_ADD_LIQUIDITY_RETURNS_DELTA_FLAG = 1 << 1
AFTER_REMOVE_LIQUIDITY_RETURNS_DELTA_FLAG = 1 << 0

# All 14 bits mask
ALL_HOOK_FLAGS_MASK = (1 << 14) - 1

# Zero address = no hooks
NO_HOOKS = NATIVE_CURRENCY


@dataclass(frozen=True)
class HookFlags:
    """Decoded 14-bit hook capability flags from a V4 hook address.

    In Uniswap V4, hook contract addresses encode their capabilities in the
    last 14 bits of the address. This is enforced by CREATE2 address mining --
    the PoolManager validates that a hook's address matches its declared capabilities.

    Usage:
        flags = HookFlags.from_address("0x...hook_address...")
        if flags.before_swap:
            print("Hook modifies swap behavior")
        if flags.has_any_swap_hooks:
            print("Hook participates in swaps")
    """

    before_initialize: bool = False
    after_initialize: bool = False
    before_add_liquidity: bool = False
    after_add_liquidity: bool = False
    before_remove_liquidity: bool = False
    after_remove_liquidity: bool = False
    before_swap: bool = False
    after_swap: bool = False
    before_donate: bool = False
    after_donate: bool = False
    before_swap_returns_delta: bool = False
    after_swap_returns_delta: bool = False
    after_add_liquidity_returns_delta: bool = False
    after_remove_liquidity_returns_delta: bool = False

    @classmethod
    def from_address(cls, hook_address: str) -> HookFlags:
        """Decode hook capabilities from a hook contract address.

        Args:
            hook_address: Hook contract address (hex string with 0x prefix).

        Returns:
            HookFlags with decoded capability bits.

        Raises:
            ValueError: If the address is not a valid hex string.
        """
        if not hook_address or hook_address.lower() == NO_HOOKS:
            return cls()  # No hooks -- all flags False

        clean = hook_address.lower().replace("0x", "")
        if len(clean) != 40:
            raise ValueError(f"Invalid hook address length: expected 40 hex chars, got {len(clean)}")

        # Extract last 14 bits from the address
        addr_int = int(clean, 16)
        flags = addr_int & ALL_HOOK_FLAGS_MASK

        return cls(
            before_initialize=bool(flags & BEFORE_INITIALIZE_FLAG),
            after_initialize=bool(flags & AFTER_INITIALIZE_FLAG),
            before_add_liquidity=bool(flags & BEFORE_ADD_LIQUIDITY_FLAG),
            after_add_liquidity=bool(flags & AFTER_ADD_LIQUIDITY_FLAG),
            before_remove_liquidity=bool(flags & BEFORE_REMOVE_LIQUIDITY_FLAG),
            after_remove_liquidity=bool(flags & AFTER_REMOVE_LIQUIDITY_FLAG),
            before_swap=bool(flags & BEFORE_SWAP_FLAG),
            after_swap=bool(flags & AFTER_SWAP_FLAG),
            before_donate=bool(flags & BEFORE_DONATE_FLAG),
            after_donate=bool(flags & AFTER_DONATE_FLAG),
            before_swap_returns_delta=bool(flags & BEFORE_SWAP_RETURNS_DELTA_FLAG),
            after_swap_returns_delta=bool(flags & AFTER_SWAP_RETURNS_DELTA_FLAG),
            after_add_liquidity_returns_delta=bool(flags & AFTER_ADD_LIQUIDITY_RETURNS_DELTA_FLAG),
            after_remove_liquidity_returns_delta=bool(flags & AFTER_REMOVE_LIQUIDITY_RETURNS_DELTA_FLAG),
        )

    @classmethod
    def from_bitmask(cls, bitmask: int) -> HookFlags:
        """Create HookFlags from a raw 14-bit bitmask.

        Args:
            bitmask: Integer with hook flags in the lower 14 bits.

        Returns:
            HookFlags with decoded capability bits.
        """
        flags = bitmask & ALL_HOOK_FLAGS_MASK
        return cls(
            before_initialize=bool(flags & BEFORE_INITIALIZE_FLAG),
            after_initialize=bool(flags & AFTER_INITIALIZE_FLAG),
            before_add_liquidity=bool(flags & BEFORE_ADD_LIQUIDITY_FLAG),
            after_add_liquidity=bool(flags & AFTER_ADD_LIQUIDITY_FLAG),
            before_remove_liquidity=bool(flags & BEFORE_REMOVE_LIQUIDITY_FLAG),
            after_remove_liquidity=bool(flags & AFTER_REMOVE_LIQUIDITY_FLAG),
            before_swap=bool(flags & BEFORE_SWAP_FLAG),
            after_swap=bool(flags & AFTER_SWAP_FLAG),
            before_donate=bool(flags & BEFORE_DONATE_FLAG),
            after_donate=bool(flags & AFTER_DONATE_FLAG),
            before_swap_returns_delta=bool(flags & BEFORE_SWAP_RETURNS_DELTA_FLAG),
            after_swap_returns_delta=bool(flags & AFTER_SWAP_RETURNS_DELTA_FLAG),
            after_add_liquidity_returns_delta=bool(flags & AFTER_ADD_LIQUIDITY_RETURNS_DELTA_FLAG),
            after_remove_liquidity_returns_delta=bool(flags & AFTER_REMOVE_LIQUIDITY_RETURNS_DELTA_FLAG),
        )

    def to_bitmask(self) -> int:
        """Convert flags back to a 14-bit integer bitmask."""
        mask = 0
        if self.before_initialize:
            mask |= BEFORE_INITIALIZE_FLAG
        if self.after_initialize:
            mask |= AFTER_INITIALIZE_FLAG
        if self.before_add_liquidity:
            mask |= BEFORE_ADD_LIQUIDITY_FLAG
        if self.after_add_liquidity:
            mask |= AFTER_ADD_LIQUIDITY_FLAG
        if self.before_remove_liquidity:
            mask |= BEFORE_REMOVE_LIQUIDITY_FLAG
        if self.after_remove_liquidity:
            mask |= AFTER_REMOVE_LIQUIDITY_FLAG
        if self.before_swap:
            mask |= BEFORE_SWAP_FLAG
        if self.after_swap:
            mask |= AFTER_SWAP_FLAG
        if self.before_donate:
            mask |= BEFORE_DONATE_FLAG
        if self.after_donate:
            mask |= AFTER_DONATE_FLAG
        if self.before_swap_returns_delta:
            mask |= BEFORE_SWAP_RETURNS_DELTA_FLAG
        if self.after_swap_returns_delta:
            mask |= AFTER_SWAP_RETURNS_DELTA_FLAG
        if self.after_add_liquidity_returns_delta:
            mask |= AFTER_ADD_LIQUIDITY_RETURNS_DELTA_FLAG
        if self.after_remove_liquidity_returns_delta:
            mask |= AFTER_REMOVE_LIQUIDITY_RETURNS_DELTA_FLAG
        return mask

    @property
    def has_any_swap_hooks(self) -> bool:
        """True if the hook participates in swap operations."""
        return self.before_swap or self.after_swap

    @property
    def has_any_liquidity_hooks(self) -> bool:
        """True if the hook participates in liquidity operations."""
        return (
            self.before_add_liquidity
            or self.after_add_liquidity
            or self.before_remove_liquidity
            or self.after_remove_liquidity
        )

    @property
    def has_any_delta_flags(self) -> bool:
        """True if the hook returns balance deltas (modifies amounts)."""
        return (
            self.before_swap_returns_delta
            or self.after_swap_returns_delta
            or self.after_add_liquidity_returns_delta
            or self.after_remove_liquidity_returns_delta
        )

    @property
    def is_empty(self) -> bool:
        """True if no hook capabilities are set (no-hook address)."""
        return self.to_bitmask() == 0

    @property
    def active_flags(self) -> list[str]:
        """Return list of active hook flag names."""
        names = []
        flag_names = [
            ("before_initialize", self.before_initialize),
            ("after_initialize", self.after_initialize),
            ("before_add_liquidity", self.before_add_liquidity),
            ("after_add_liquidity", self.after_add_liquidity),
            ("before_remove_liquidity", self.before_remove_liquidity),
            ("after_remove_liquidity", self.after_remove_liquidity),
            ("before_swap", self.before_swap),
            ("after_swap", self.after_swap),
            ("before_donate", self.before_donate),
            ("after_donate", self.after_donate),
            ("before_swap_returns_delta", self.before_swap_returns_delta),
            ("after_swap_returns_delta", self.after_swap_returns_delta),
            ("after_add_liquidity_returns_delta", self.after_add_liquidity_returns_delta),
            ("after_remove_liquidity_returns_delta", self.after_remove_liquidity_returns_delta),
        ]
        for name, active in flag_names:
            if active:
                names.append(name)
        return names

    def requires_hook_data(self) -> bool:
        """True if this hook likely requires non-empty hookData.

        Hooks with before_swap, after_swap, or delta-returning flags typically
        need hookData to function correctly. Empty hookData may cause reverts.
        """
        return self.has_any_swap_hooks or self.has_any_delta_flags


# =============================================================================
# HookDataEncoder — base class for typed hookData encoding
# =============================================================================


class HookDataEncoder(ABC):
    """Base class for encoding protocol-specific hookData.

    Strategy authors subclass this to provide typed encoding for known hook
    contracts. The encoder validates inputs and produces ABI-encoded bytes
    that the hook contract expects.

    Example:
        class DynamicFeeEncoder(HookDataEncoder):
            def encode(self, **kwargs) -> bytes:
                fee_override = kwargs.get("fee_override", 3000)
                return fee_override.to_bytes(32, "big")

            @property
            def hook_name(self) -> str:
                return "DynamicFeeHook"

        encoder = DynamicFeeEncoder()
        hook_data = encoder.encode(fee_override=500)
    """

    @abstractmethod
    def encode(self, **kwargs) -> bytes:
        """Encode hookData for this specific hook contract.

        Args:
            **kwargs: Hook-specific parameters.

        Returns:
            ABI-encoded bytes for the hookData field.
        """

    @property
    @abstractmethod
    def hook_name(self) -> str:
        """Human-readable name of the hook this encoder targets."""

    def validate_flags(self, flags: HookFlags) -> bool:
        """Validate that hook flags are compatible with this encoder.

        Override this method to enforce that the hook address has the
        expected capability bits set.

        Args:
            flags: Decoded HookFlags from the hook address.

        Returns:
            True if flags are compatible, False otherwise.
        """
        return True


class EmptyHookDataEncoder(HookDataEncoder):
    """Encoder for pools with no hooks or hooks that don't need hookData."""

    def encode(self, **kwargs) -> bytes:
        """Return empty bytes (no hookData needed)."""
        return b""

    @property
    def hook_name(self) -> str:
        return "NoHook"

    def validate_flags(self, flags: HookFlags) -> bool:
        """Valid for no-hook pools or hooks that don't require hookData."""
        return not flags.requires_hook_data()


class DynamicFeeHookEncoder(HookDataEncoder):
    """Example encoder for dynamic fee hooks.

    Dynamic fee hooks override the pool's static fee with a value computed
    in the beforeSwap callback. Some implementations accept a fee hint via
    hookData.
    """

    def encode(self, **kwargs) -> bytes:
        """Encode a fee hint for a dynamic fee hook.

        Args:
            fee_hint: Suggested fee in hundredths of a bip (e.g., 3000 = 0.3%).
                      If omitted, empty hookData lets the hook compute its own fee.
        """
        fee_hint = kwargs.get("fee_hint")
        if fee_hint is None:
            return b""
        # ABI-encode a single uint24 fee value
        return int(fee_hint).to_bytes(32, "big")

    @property
    def hook_name(self) -> str:
        return "DynamicFeeHook"

    def validate_flags(self, flags: HookFlags) -> bool:
        """Dynamic fee hooks must have before_swap capability."""
        return flags.before_swap


# =============================================================================
# Pool Discovery via StateView
# =============================================================================


@dataclass(frozen=True)
class PoolState:
    """State of a V4 pool from StateView.getSlot0().

    Fields:
        sqrt_price_x96: Current sqrt(price) as Q64.96 fixed-point.
        tick: Current tick index.
        protocol_fee: Protocol fee setting.
        lp_fee: LP fee in hundredths of a bip.
        exists: Whether the pool has been initialized.
    """

    sqrt_price_x96: int = 0
    tick: int = 0
    protocol_fee: int = 0
    lp_fee: int = 0
    exists: bool = False


@dataclass(frozen=True)
class PoolDiscoveryResult:
    """Result of pool discovery for a token pair.

    Fields:
        pool_key: The resolved PoolKey.
        pool_id: Keccak256 hash of the ABI-encoded PoolKey.
        hook_address: Hook contract address (zero address if no hooks).
        hook_flags: Decoded hook capabilities.
        state: Pool state from StateView (None if not queried on-chain).
    """

    pool_key: PoolKey
    pool_id: str
    hook_address: str
    hook_flags: HookFlags
    state: PoolState | None = None


def compute_pool_id(pool_key: PoolKey) -> str:
    """Compute the pool ID (keccak256 hash of ABI-encoded PoolKey).

    The pool ID is used to identify pools in the PoolManager. It's the
    keccak256 hash of abi.encode(currency0, currency1, fee, tickSpacing, hooks).

    Args:
        pool_key: The pool key to hash.

    Returns:
        Pool ID as hex string with 0x prefix.
    """
    # ABI-encode the PoolKey struct (5 words, each 32 bytes)
    encoded = (
        _pad_address(pool_key.currency0)
        + _pad_address(pool_key.currency1)
        + _pad_uint24(pool_key.fee)
        + _pad_int24(pool_key.tick_spacing)
        + _pad_address(pool_key.hooks)
    )

    # Ethereum keccak256 (NOT hashlib.sha3_256 which is NIST SHA3-256)
    encoded_bytes = bytes.fromhex(encoded)
    pool_id = Web3.keccak(encoded_bytes).hex()
    return "0x" + pool_id


def build_get_slot0_calldata(pool_key: PoolKey) -> str:
    """Build calldata for StateView.getSlot0(PoolKey).

    This can be used to query pool state via an RPC eth_call to the
    StateView contract. The return data contains (sqrtPriceX96, tick,
    protocolFee, lpFee).

    Args:
        pool_key: The pool key to query.

    Returns:
        Hex-encoded calldata with 0x prefix.
    """
    # Selector: keccak256("getSlot0((address,address,uint24,int24,address))")[:4]
    sig = "getSlot0((address,address,uint24,int24,address))"
    selector = Web3.keccak(text=sig).hex()[:8]  # first 4 bytes (HexBytes.hex() has no 0x prefix)

    # ABI-encode the PoolKey as a tuple
    encoded = (
        _pad_address(pool_key.currency0)
        + _pad_address(pool_key.currency1)
        + _pad_uint24(pool_key.fee)
        + _pad_int24(pool_key.tick_spacing)
        + _pad_address(pool_key.hooks)
    )

    return "0x" + selector + encoded


def decode_slot0_response(data: str) -> PoolState:
    """Decode the response from StateView.getSlot0().

    Args:
        data: Hex-encoded response data (with or without 0x prefix).

    Returns:
        PoolState with decoded values.
    """
    clean = data[2:] if data.startswith("0x") else data

    # getSlot0 returns 4 ABI-encoded values (4 * 32 bytes = 256 hex chars)
    if len(clean) < 256:
        return PoolState(exists=False)

    sqrt_price_x96 = HexDecoder.decode_uint256(clean[0:64])
    tick = HexDecoder.decode_int24(clean[64:128])
    protocol_fee = HexDecoder.decode_uint256(clean[128:192])
    lp_fee = HexDecoder.decode_uint256(clean[192:256])

    # Pool exists if sqrtPriceX96 > 0 (uninitialized pools return 0)
    exists = sqrt_price_x96 > 0

    return PoolState(
        sqrt_price_x96=sqrt_price_x96,
        tick=tick,
        protocol_fee=protocol_fee,
        lp_fee=lp_fee,
        exists=exists,
    )


def discover_pool(
    token0: str,
    token1: str,
    fee: int = 3000,
    tick_spacing: int | None = None,
    hooks: str = NO_HOOKS,
) -> PoolDiscoveryResult:
    """Discover a V4 pool and decode its hook capabilities.

    Two-step hook discovery:
    1. Resolve PoolKey for the token pair/fee/tickSpacing to get the hook address
    2. Decode the 14-bit capability bitmask from the hook address

    Args:
        token0: First token address.
        token1: Second token address.
        fee: Fee tier in hundredths of a bip.
        tick_spacing: Custom tick spacing (defaults to standard for fee tier).
        hooks: Hook contract address (zero address for no hooks).

    Returns:
        PoolDiscoveryResult with pool key, ID, and hook capabilities.
    """
    if tick_spacing is None:
        tick_spacing = TICK_SPACING.get(fee, 60)

    pool_key = PoolKey(
        currency0=token0,
        currency1=token1,
        fee=fee,
        tick_spacing=tick_spacing,
        hooks=hooks,
    )

    pool_id = compute_pool_id(pool_key)
    hook_flags = HookFlags.from_address(pool_key.hooks)

    return PoolDiscoveryResult(
        pool_key=pool_key,
        pool_id=pool_id,
        hook_address=pool_key.hooks,
        hook_flags=hook_flags,
    )


def warn_empty_hook_data(hook_flags: HookFlags, hook_data: bytes) -> str | None:
    """Check if empty hookData on a hooked pool might cause a revert.

    Args:
        hook_flags: Decoded hook capabilities.
        hook_data: The hookData bytes being sent.

    Returns:
        Warning message if empty hookData is likely problematic, None otherwise.
    """
    if hook_flags.is_empty:
        return None  # No hooks, empty data is fine

    if hook_data:
        return None  # Non-empty data provided

    if hook_flags.requires_hook_data():
        active = ", ".join(hook_flags.active_flags)
        return (
            f"Hook has active capabilities [{active}] but hookData is empty. "
            "This may cause an on-chain revert. Use a HookDataEncoder to provide "
            "properly encoded hookData for this hook contract."
        )

    return None


__all__ = [
    "AFTER_ADD_LIQUIDITY_FLAG",
    "AFTER_ADD_LIQUIDITY_RETURNS_DELTA_FLAG",
    "AFTER_DONATE_FLAG",
    "AFTER_INITIALIZE_FLAG",
    "AFTER_REMOVE_LIQUIDITY_FLAG",
    "AFTER_REMOVE_LIQUIDITY_RETURNS_DELTA_FLAG",
    "AFTER_SWAP_FLAG",
    "AFTER_SWAP_RETURNS_DELTA_FLAG",
    "ALL_HOOK_FLAGS_MASK",
    "BEFORE_ADD_LIQUIDITY_FLAG",
    "BEFORE_DONATE_FLAG",
    "BEFORE_INITIALIZE_FLAG",
    "BEFORE_REMOVE_LIQUIDITY_FLAG",
    "BEFORE_SWAP_FLAG",
    "BEFORE_SWAP_RETURNS_DELTA_FLAG",
    "DynamicFeeHookEncoder",
    "EmptyHookDataEncoder",
    "HookDataEncoder",
    "HookFlags",
    "NO_HOOKS",
    "PoolDiscoveryResult",
    "PoolState",
    "build_get_slot0_calldata",
    "compute_pool_id",
    "decode_slot0_response",
    "discover_pool",
    "warn_empty_hook_data",
]
