"""Tests for Uniswap V4 hook discovery and hookData encoding.

Tests cover:
- HookFlags: 14-bit capability bitmask decoding from addresses
- HookDataEncoder: Base class and concrete implementations
- Pool discovery: PoolKey computation, pool ID, hook decoding
- StateView calldata: getSlot0 encoding and response decoding
- Edge cases: zero address, all-bits-set, invalid inputs
"""

import pytest

from almanak.framework.connectors.uniswap_v4.hooks import (
    AFTER_ADD_LIQUIDITY_FLAG,
    AFTER_ADD_LIQUIDITY_RETURNS_DELTA_FLAG,
    AFTER_DONATE_FLAG,
    AFTER_INITIALIZE_FLAG,
    AFTER_REMOVE_LIQUIDITY_FLAG,
    AFTER_REMOVE_LIQUIDITY_RETURNS_DELTA_FLAG,
    AFTER_SWAP_FLAG,
    AFTER_SWAP_RETURNS_DELTA_FLAG,
    ALL_HOOK_FLAGS_MASK,
    BEFORE_ADD_LIQUIDITY_FLAG,
    BEFORE_DONATE_FLAG,
    BEFORE_INITIALIZE_FLAG,
    BEFORE_REMOVE_LIQUIDITY_FLAG,
    BEFORE_SWAP_FLAG,
    BEFORE_SWAP_RETURNS_DELTA_FLAG,
    NO_HOOKS,
    DynamicFeeHookEncoder,
    EmptyHookDataEncoder,
    HookFlags,
    PoolDiscoveryResult,
    PoolState,
    build_get_slot0_calldata,
    compute_pool_id,
    decode_slot0_response,
    discover_pool,
    warn_empty_hook_data,
)
from almanak.framework.connectors.uniswap_v4.sdk import NATIVE_CURRENCY, PoolKey


# =============================================================================
# HookFlags Tests
# =============================================================================


class TestHookFlags:
    """Test HookFlags 14-bit bitmask decoding."""

    def test_no_hooks_zero_address(self):
        """Zero address (no hooks) should have all flags False."""
        flags = HookFlags.from_address(NATIVE_CURRENCY)
        assert flags.is_empty
        assert flags.to_bitmask() == 0
        assert flags.active_flags == []

    def test_no_hooks_none_input(self):
        """None or empty string should return empty flags."""
        flags = HookFlags.from_address("")
        assert flags.is_empty

    def test_before_swap_flag(self):
        """Address with bit 7 set should have before_swap=True."""
        # BEFORE_SWAP_FLAG = 1 << 7 = 0x80
        # Address ending in 0x80 = ...0080
        addr = "0x" + "0" * 36 + "0080"
        flags = HookFlags.from_address(addr)
        assert flags.before_swap is True
        assert flags.after_swap is False
        assert flags.has_any_swap_hooks is True
        assert "before_swap" in flags.active_flags

    def test_after_swap_flag(self):
        """Address with bit 6 set should have after_swap=True."""
        # AFTER_SWAP_FLAG = 1 << 6 = 0x40
        addr = "0x" + "0" * 36 + "0040"
        flags = HookFlags.from_address(addr)
        assert flags.after_swap is True
        assert flags.before_swap is False
        assert flags.has_any_swap_hooks is True

    def test_before_and_after_swap(self):
        """Address with both swap bits set."""
        # 0x80 | 0x40 = 0xC0
        addr = "0x" + "0" * 36 + "00C0"
        flags = HookFlags.from_address(addr)
        assert flags.before_swap is True
        assert flags.after_swap is True
        assert flags.has_any_swap_hooks is True

    def test_liquidity_flags(self):
        """Test all liquidity-related flags."""
        # before_add=bit11, after_add=bit10, before_remove=bit9, after_remove=bit8
        # 0x0F00 = bits 11,10,9,8
        addr = "0x" + "0" * 36 + "0F00"
        flags = HookFlags.from_address(addr)
        assert flags.before_add_liquidity is True
        assert flags.after_add_liquidity is True
        assert flags.before_remove_liquidity is True
        assert flags.after_remove_liquidity is True
        assert flags.has_any_liquidity_hooks is True

    def test_initialize_flags(self):
        """Test before/after initialize flags (bits 13,12)."""
        # 0x3000 = bits 13,12
        addr = "0x" + "0" * 36 + "3000"
        flags = HookFlags.from_address(addr)
        assert flags.before_initialize is True
        assert flags.after_initialize is True

    def test_donate_flags(self):
        """Test before/after donate flags (bits 5,4)."""
        # 0x0030 = bits 5,4
        addr = "0x" + "0" * 36 + "0030"
        flags = HookFlags.from_address(addr)
        assert flags.before_donate is True
        assert flags.after_donate is True

    def test_delta_flags(self):
        """Test all delta-returning flags (bits 3,2,1,0)."""
        # 0x000F = bits 3,2,1,0
        addr = "0x" + "0" * 36 + "000F"
        flags = HookFlags.from_address(addr)
        assert flags.before_swap_returns_delta is True
        assert flags.after_swap_returns_delta is True
        assert flags.after_add_liquidity_returns_delta is True
        assert flags.after_remove_liquidity_returns_delta is True
        assert flags.has_any_delta_flags is True

    def test_all_flags_set(self):
        """Address with all 14 bits set should have all flags True."""
        # ALL_HOOK_FLAGS_MASK = 0x3FFF
        addr = "0x" + "0" * 36 + "3FFF"
        flags = HookFlags.from_address(addr)
        assert flags.before_initialize is True
        assert flags.after_initialize is True
        assert flags.before_add_liquidity is True
        assert flags.after_add_liquidity is True
        assert flags.before_remove_liquidity is True
        assert flags.after_remove_liquidity is True
        assert flags.before_swap is True
        assert flags.after_swap is True
        assert flags.before_donate is True
        assert flags.after_donate is True
        assert flags.before_swap_returns_delta is True
        assert flags.after_swap_returns_delta is True
        assert flags.after_add_liquidity_returns_delta is True
        assert flags.after_remove_liquidity_returns_delta is True
        assert flags.to_bitmask() == ALL_HOOK_FLAGS_MASK
        assert len(flags.active_flags) == 14

    def test_upper_address_bits_ignored(self):
        """Only the last 14 bits of the address should matter."""
        # Same last 14 bits, different upper bits
        addr1 = "0xdead" + "0" * 32 + "0080"
        addr2 = "0xbeef" + "0" * 32 + "0080"
        flags1 = HookFlags.from_address(addr1)
        flags2 = HookFlags.from_address(addr2)
        assert flags1.before_swap == flags2.before_swap
        assert flags1.to_bitmask() == flags2.to_bitmask()

    def test_from_bitmask(self):
        """Test creating HookFlags from a raw bitmask."""
        bitmask = BEFORE_SWAP_FLAG | AFTER_SWAP_FLAG
        flags = HookFlags.from_bitmask(bitmask)
        assert flags.before_swap is True
        assert flags.after_swap is True
        assert flags.before_initialize is False
        assert flags.to_bitmask() == bitmask

    def test_roundtrip_bitmask(self):
        """to_bitmask() should roundtrip through from_bitmask()."""
        original = BEFORE_SWAP_FLAG | AFTER_ADD_LIQUIDITY_FLAG | AFTER_SWAP_RETURNS_DELTA_FLAG
        flags = HookFlags.from_bitmask(original)
        assert flags.to_bitmask() == original
        flags2 = HookFlags.from_bitmask(flags.to_bitmask())
        assert flags == flags2

    def test_invalid_address_length(self):
        """Invalid address length should raise ValueError."""
        with pytest.raises(ValueError, match="Invalid hook address length"):
            HookFlags.from_address("0xdead")

    def test_case_insensitive(self):
        """Address should be case-insensitive."""
        addr_lower = "0x" + "0" * 36 + "00c0"
        addr_upper = "0x" + "0" * 36 + "00C0"
        flags_lower = HookFlags.from_address(addr_lower)
        flags_upper = HookFlags.from_address(addr_upper)
        assert flags_lower.to_bitmask() == flags_upper.to_bitmask()

    def test_requires_hook_data(self):
        """Hooks with swap or delta flags should require hookData."""
        # Swap hook
        swap_flags = HookFlags.from_bitmask(BEFORE_SWAP_FLAG)
        assert swap_flags.requires_hook_data() is True

        # Delta hook
        delta_flags = HookFlags.from_bitmask(BEFORE_SWAP_RETURNS_DELTA_FLAG)
        assert delta_flags.requires_hook_data() is True

        # Liquidity-only hook (no swap or delta)
        liq_flags = HookFlags.from_bitmask(BEFORE_ADD_LIQUIDITY_FLAG)
        assert liq_flags.requires_hook_data() is False

        # No hooks
        empty = HookFlags.from_bitmask(0)
        assert empty.requires_hook_data() is False

    def test_individual_flag_constants(self):
        """Each flag constant should correspond to exactly one bit."""
        all_flags = [
            BEFORE_INITIALIZE_FLAG,
            AFTER_INITIALIZE_FLAG,
            BEFORE_ADD_LIQUIDITY_FLAG,
            AFTER_ADD_LIQUIDITY_FLAG,
            BEFORE_REMOVE_LIQUIDITY_FLAG,
            AFTER_REMOVE_LIQUIDITY_FLAG,
            BEFORE_SWAP_FLAG,
            AFTER_SWAP_FLAG,
            BEFORE_DONATE_FLAG,
            AFTER_DONATE_FLAG,
            BEFORE_SWAP_RETURNS_DELTA_FLAG,
            AFTER_SWAP_RETURNS_DELTA_FLAG,
            AFTER_ADD_LIQUIDITY_RETURNS_DELTA_FLAG,
            AFTER_REMOVE_LIQUIDITY_RETURNS_DELTA_FLAG,
        ]
        # Each should be a power of 2
        for flag in all_flags:
            assert flag > 0
            assert flag & (flag - 1) == 0, f"Flag {flag} is not a power of 2"

        # All flags OR'd together should equal ALL_HOOK_FLAGS_MASK
        combined = 0
        for flag in all_flags:
            combined |= flag
        assert combined == ALL_HOOK_FLAGS_MASK

    def test_frozen_dataclass(self):
        """HookFlags should be immutable."""
        flags = HookFlags.from_bitmask(BEFORE_SWAP_FLAG)
        with pytest.raises(AttributeError):
            flags.before_swap = False  # type: ignore[misc]


# =============================================================================
# HookDataEncoder Tests
# =============================================================================


class TestEmptyHookDataEncoder:
    """Test EmptyHookDataEncoder."""

    def test_encode_returns_empty_bytes(self):
        encoder = EmptyHookDataEncoder()
        assert encoder.encode() == b""

    def test_hook_name(self):
        encoder = EmptyHookDataEncoder()
        assert encoder.hook_name == "NoHook"

    def test_validate_flags_empty(self):
        encoder = EmptyHookDataEncoder()
        empty_flags = HookFlags()
        assert encoder.validate_flags(empty_flags) is True

    def test_validate_flags_swap_hooks_rejected(self):
        """Swap hooks require hookData, so EmptyHookDataEncoder rejects them."""
        encoder = EmptyHookDataEncoder()
        swap_flags = HookFlags.from_bitmask(BEFORE_SWAP_FLAG)
        assert encoder.validate_flags(swap_flags) is False

    def test_validate_flags_liquidity_only_accepted(self):
        """Liquidity-only hooks don't need hookData, so empty encoder is valid."""
        encoder = EmptyHookDataEncoder()
        liq_flags = HookFlags.from_bitmask(BEFORE_ADD_LIQUIDITY_FLAG)
        assert encoder.validate_flags(liq_flags) is True


class TestDynamicFeeHookEncoder:
    """Test DynamicFeeHookEncoder."""

    def test_encode_with_fee_hint(self):
        encoder = DynamicFeeHookEncoder()
        result = encoder.encode(fee_hint=500)
        assert len(result) == 32
        # Decode the uint256 value
        value = int.from_bytes(result, "big")
        assert value == 500

    def test_encode_without_fee_hint(self):
        encoder = DynamicFeeHookEncoder()
        result = encoder.encode()
        assert result == b""

    def test_hook_name(self):
        encoder = DynamicFeeHookEncoder()
        assert encoder.hook_name == "DynamicFeeHook"

    def test_validate_flags_with_before_swap(self):
        encoder = DynamicFeeHookEncoder()
        flags = HookFlags.from_bitmask(BEFORE_SWAP_FLAG)
        assert encoder.validate_flags(flags) is True

    def test_validate_flags_without_before_swap(self):
        encoder = DynamicFeeHookEncoder()
        flags = HookFlags.from_bitmask(AFTER_SWAP_FLAG)
        assert encoder.validate_flags(flags) is False


# =============================================================================
# Pool Discovery Tests
# =============================================================================


class TestPoolDiscovery:
    """Test pool discovery and PoolKey computation."""

    WETH = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
    USDC = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"

    def test_discover_no_hooks(self):
        """Discover a pool with no hooks."""
        result = discover_pool(self.WETH, self.USDC, fee=3000)
        assert isinstance(result, PoolDiscoveryResult)
        assert result.hook_address == NO_HOOKS.lower()
        assert result.hook_flags.is_empty
        assert result.pool_id.startswith("0x")
        assert len(result.pool_id) == 66  # 0x + 64 hex chars

    def test_discover_with_hooks(self):
        """Discover a pool with a hooked address."""
        # Hook address with before_swap and after_swap bits set (0xC0)
        hook_addr = "0x" + "ab" * 19 + "C0"
        result = discover_pool(self.WETH, self.USDC, fee=3000, hooks=hook_addr)
        assert result.hook_flags.before_swap is True
        assert result.hook_flags.after_swap is True
        assert result.hook_address == hook_addr.lower()

    def test_pool_key_sorted(self):
        """PoolKey should sort currencies by address."""
        result = discover_pool(self.WETH, self.USDC, fee=3000)
        # USDC address is numerically less than WETH
        c0_int = int(result.pool_key.currency0, 16)
        c1_int = int(result.pool_key.currency1, 16)
        assert c0_int < c1_int

    def test_pool_id_deterministic(self):
        """Same inputs should produce the same pool ID."""
        result1 = discover_pool(self.WETH, self.USDC, fee=3000)
        result2 = discover_pool(self.WETH, self.USDC, fee=3000)
        assert result1.pool_id == result2.pool_id

    def test_different_fee_different_pool_id(self):
        """Different fee tiers should produce different pool IDs."""
        result_500 = discover_pool(self.WETH, self.USDC, fee=500)
        result_3000 = discover_pool(self.WETH, self.USDC, fee=3000)
        assert result_500.pool_id != result_3000.pool_id

    def test_different_hooks_different_pool_id(self):
        """Different hook addresses should produce different pool IDs."""
        hook1 = "0x" + "ab" * 19 + "80"
        hook2 = "0x" + "cd" * 19 + "80"
        result1 = discover_pool(self.WETH, self.USDC, fee=3000, hooks=hook1)
        result2 = discover_pool(self.WETH, self.USDC, fee=3000, hooks=hook2)
        assert result1.pool_id != result2.pool_id

    def test_state_is_none_without_rpc(self):
        """Pool state should be None when not queried on-chain."""
        result = discover_pool(self.WETH, self.USDC, fee=3000)
        assert result.state is None

    def test_default_tick_spacing(self):
        """Default tick spacing should match the fee tier."""
        result_500 = discover_pool(self.WETH, self.USDC, fee=500)
        assert result_500.pool_key.tick_spacing == 10

        result_3000 = discover_pool(self.WETH, self.USDC, fee=3000)
        assert result_3000.pool_key.tick_spacing == 60

    def test_custom_tick_spacing(self):
        """Custom tick spacing should override the default."""
        result = discover_pool(self.WETH, self.USDC, fee=3000, tick_spacing=100)
        assert result.pool_key.tick_spacing == 100


class TestComputePoolId:
    """Test pool ID computation."""

    def test_compute_pool_id_length(self):
        key = PoolKey(
            currency0="0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            currency1="0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
            fee=3000,
            tick_spacing=60,
        )
        pool_id = compute_pool_id(key)
        assert pool_id.startswith("0x")
        assert len(pool_id) == 66

    def test_deterministic(self):
        key = PoolKey(
            currency0="0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            currency1="0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
            fee=3000,
            tick_spacing=60,
        )
        assert compute_pool_id(key) == compute_pool_id(key)


# =============================================================================
# StateView Tests
# =============================================================================


class TestStateViewCalldata:
    """Test StateView getSlot0 calldata encoding."""

    def test_build_get_slot0_calldata(self):
        key = PoolKey(
            currency0="0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            currency1="0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
            fee=3000,
            tick_spacing=60,
        )
        calldata = build_get_slot0_calldata(key)
        assert calldata.startswith("0x")
        # 4-byte selector + 5 * 32-byte params = 164 bytes = 328 hex chars + 2 for 0x
        assert len(calldata) == 330

    def test_decode_slot0_response_valid(self):
        """Decode a valid getSlot0 response."""
        # sqrtPriceX96 = 1000000, tick = -100, protocolFee = 0, lpFee = 3000
        sqrt_price = hex(1000000)[2:].zfill(64)
        # tick = -100 in two's complement
        tick_val = (1 << 256) - 100
        tick_hex = hex(tick_val)[2:].zfill(64)
        protocol_fee = "0" * 64
        lp_fee = hex(3000)[2:].zfill(64)

        data = "0x" + sqrt_price + tick_hex + protocol_fee + lp_fee
        state = decode_slot0_response(data)

        assert state.exists is True
        assert state.sqrt_price_x96 == 1000000
        assert state.tick == -100
        assert state.protocol_fee == 0
        assert state.lp_fee == 3000

    def test_decode_slot0_response_uninitialized(self):
        """Uninitialized pool returns sqrtPriceX96=0."""
        data = "0x" + "0" * 256
        state = decode_slot0_response(data)
        assert state.exists is False
        assert state.sqrt_price_x96 == 0

    def test_decode_slot0_response_short_data(self):
        """Short response should return exists=False."""
        state = decode_slot0_response("0x")
        assert state.exists is False

    def test_pool_state_frozen(self):
        """PoolState should be immutable."""
        state = PoolState(sqrt_price_x96=100, tick=0, exists=True)
        with pytest.raises(AttributeError):
            state.tick = 5  # type: ignore[misc]


# =============================================================================
# Warning Tests
# =============================================================================


class TestWarnEmptyHookData:
    """Test empty hookData warning logic."""

    def test_no_warning_for_no_hooks(self):
        flags = HookFlags()
        assert warn_empty_hook_data(flags, b"") is None

    def test_no_warning_with_hook_data(self):
        flags = HookFlags.from_bitmask(BEFORE_SWAP_FLAG)
        assert warn_empty_hook_data(flags, b"\x01") is None

    def test_warning_for_swap_hook_empty_data(self):
        flags = HookFlags.from_bitmask(BEFORE_SWAP_FLAG)
        warning = warn_empty_hook_data(flags, b"")
        assert warning is not None
        assert "empty" in warning.lower()
        assert "before_swap" in warning

    def test_warning_for_delta_hook_empty_data(self):
        flags = HookFlags.from_bitmask(BEFORE_SWAP_RETURNS_DELTA_FLAG)
        warning = warn_empty_hook_data(flags, b"")
        assert warning is not None

    def test_no_warning_for_liquidity_only_hook(self):
        """Liquidity-only hooks don't typically need hookData."""
        flags = HookFlags.from_bitmask(BEFORE_ADD_LIQUIDITY_FLAG)
        assert warn_empty_hook_data(flags, b"") is None
