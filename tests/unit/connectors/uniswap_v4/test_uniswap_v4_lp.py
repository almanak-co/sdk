"""Tests for Uniswap V4 LP adapter, SDK LP methods, receipt parser LP extraction, and HookFlags.

Covers VIB-1966 Phase 2: PositionManager LP adapter & hook basics.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak.framework.connectors.uniswap_v4.hooks import HookFlags
from almanak.framework.connectors.uniswap_v4.sdk import (
    ACTION_CLEAR_OR_TAKE,
    ACTION_CLOSE_CURRENCY,
    ACTION_SETTLE,
    ACTION_SETTLE_ALL,
    ACTION_SETTLE_PAIR,
    ACTION_SWEEP,
    ACTION_SWAP_EXACT_IN,
    ACTION_SWAP_EXACT_IN_SINGLE,
    ACTION_SWAP_EXACT_OUT,
    ACTION_SWAP_EXACT_OUT_SINGLE,
    ACTION_TAKE,
    ACTION_TAKE_ALL,
    ACTION_TAKE_PAIR,
    ACTION_TAKE_PORTION,
    MODIFY_LIQUIDITIES_SELECTOR,
    NATIVE_CURRENCY,
    PM_BURN_POSITION,
    PM_CLEAR_OR_TAKE,
    PM_CLOSE_CURRENCY,
    PM_DECREASE_LIQUIDITY,
    PM_INCREASE_LIQUIDITY,
    PM_MINT_POSITION,
    PM_SETTLE,
    PM_SETTLE_ALL,
    PM_SETTLE_PAIR,
    PM_SWEEP,
    PM_TAKE,
    PM_TAKE_ALL,
    PM_TAKE_PAIR,
    PM_TAKE_PORTION,
    POSITION_MANAGER_ADDRESSES,
    UNISWAP_V4_GAS_ESTIMATES,
    LPDecreaseParams,
    LPMintParams,
    PoolKey,
    UniswapV4SDK,
    _tick_to_sqrt_ratio_x96,
)
from almanak.framework.connectors.uniswap_v4.receipt_parser import (
    EVENT_TOPICS,
    ModifyLiquidityEventData,
    UniswapV4ReceiptParser,
)


def _extract_actions_from_modify_liquidities(calldata_hex: str) -> bytes:
    """Extract the packed actions bytes from modifyLiquidities calldata.

    Layout: 4-byte selector + ABI-encoded (bytes unlockData, uint256 deadline).
    unlockData is itself ABI-encoded (bytes actions, bytes[] params).
    """
    raw = bytes.fromhex(calldata_hex[2:])  # strip 0x
    # Skip 4-byte selector, then read the ABI-encoded outer tuple
    outer = raw[4:]
    # First word: offset to unlockData bytes (always 0x40)
    unlock_offset = int.from_bytes(outer[0:32], "big")
    # At unlock_offset: length of unlockData
    unlock_len = int.from_bytes(outer[unlock_offset : unlock_offset + 32], "big")
    unlock_data = outer[unlock_offset + 32 : unlock_offset + 32 + unlock_len]
    # unlockData is abi.encode(bytes actions, bytes[] params)
    # First word: offset to actions bytes (always 0x40)
    actions_offset = int.from_bytes(unlock_data[0:32], "big")
    # At actions_offset: length of actions
    actions_len = int.from_bytes(unlock_data[actions_offset : actions_offset + 32], "big")
    return unlock_data[actions_offset + 32 : actions_offset + 32 + actions_len]


# =============================================================================
# HookFlags tests
# =============================================================================


class TestHookFlags:
    """Test HookFlags utility for decoding hook address capability bitmask."""

    def test_no_hooks_zero_address(self):
        flags = HookFlags.from_address(NATIVE_CURRENCY)
        assert flags.is_empty
        assert flags.active_flags == []

    def test_before_swap_flag(self):
        # Bit 7 = 0x80
        addr = "0x0000000000000000000000000000000000000080"
        flags = HookFlags.from_address(addr)
        assert flags.before_swap
        assert not flags.after_swap
        assert not flags.is_empty
        assert "before_swap" in flags.active_flags

    def test_after_swap_flag(self):
        # Bit 6 = 0x40
        addr = "0x0000000000000000000000000000000000000040"
        flags = HookFlags.from_address(addr)
        assert flags.after_swap
        assert not flags.before_swap

    def test_liquidity_hooks(self):
        # Bit 11 (before_add_liquidity) = 0x800
        addr = "0x0000000000000000000000000000000000000800"
        flags = HookFlags.from_address(addr)
        assert flags.before_add_liquidity
        assert flags.has_any_liquidity_hooks

    def test_multiple_flags(self):
        # Bits 7 + 6 = 0xC0
        addr = "0x00000000000000000000000000000000000000C0"
        flags = HookFlags.from_address(addr)
        assert flags.before_swap
        assert flags.after_swap
        assert "before_swap" in flags.active_flags
        assert "after_swap" in flags.active_flags

    def test_all_flags_set(self):
        # All 14 bits = 0x3FFF
        addr = "0x0000000000000000000000000000000000003FFF"
        flags = HookFlags.from_address(addr)
        assert flags.before_initialize
        assert flags.after_initialize
        assert flags.before_add_liquidity
        assert flags.after_add_liquidity
        assert flags.before_remove_liquidity
        assert flags.after_remove_liquidity
        assert flags.before_swap
        assert flags.after_swap
        assert flags.before_donate
        assert flags.after_donate
        assert not flags.is_empty
        assert flags.has_any_liquidity_hooks

    def test_realistic_hook_address(self):
        # A realistic hook address with before_swap + after_swap (bits 7+6 = 0xC0)
        addr = "0x1234567890abcdef1234567890abcdef000000C0"
        flags = HookFlags.from_address(addr)
        assert flags.before_swap
        assert flags.after_swap
        assert not flags.before_add_liquidity


# =============================================================================
# SDK LP constants tests
# =============================================================================


class TestLPConstants:
    def test_position_manager_addresses_exist(self):
        from almanak.core.contracts import UNISWAP_V4
        for chain, addr in POSITION_MANAGER_ADDRESSES.items():
            expected = UNISWAP_V4[chain]["position_manager"].lower()
            assert addr.lower() == expected, f"PositionManager on {chain} mismatch"

    def test_lp_gas_estimates(self):
        assert UNISWAP_V4_GAS_ESTIMATES["lp_mint"] == 450_000
        assert UNISWAP_V4_GAS_ESTIMATES["lp_decrease"] == 300_000
        assert UNISWAP_V4_GAS_ESTIMATES["lp_burn"] == 200_000
        assert UNISWAP_V4_GAS_ESTIMATES["lp_collect_fees"] == 250_000

    def test_action_bytes_liquidity(self):
        """Verify liquidity action bytes match Actions.sol."""
        assert PM_INCREASE_LIQUIDITY == 0x00
        assert PM_DECREASE_LIQUIDITY == 0x01
        assert PM_MINT_POSITION == 0x02
        assert PM_BURN_POSITION == 0x03

    def test_action_bytes_swap(self):
        """Verify swap action bytes match Actions.sol."""
        assert ACTION_SWAP_EXACT_IN_SINGLE == 0x06
        assert ACTION_SWAP_EXACT_IN == 0x07
        assert ACTION_SWAP_EXACT_OUT_SINGLE == 0x08
        assert ACTION_SWAP_EXACT_OUT == 0x09

    def test_action_bytes_settlement(self):
        """Verify settlement action bytes match Actions.sol (PR #1160 corrected values)."""
        assert ACTION_SETTLE == 0x0B
        assert ACTION_SETTLE_ALL == 0x0C
        assert ACTION_SETTLE_PAIR == 0x0D
        assert ACTION_TAKE == 0x0E
        assert ACTION_TAKE_ALL == 0x0F
        assert ACTION_TAKE_PORTION == 0x10
        assert ACTION_TAKE_PAIR == 0x11
        assert ACTION_CLOSE_CURRENCY == 0x12
        assert ACTION_CLEAR_OR_TAKE == 0x13
        assert ACTION_SWEEP == 0x14

    def test_pm_aliases_match_action_constants(self):
        """Verify all PM_* aliases point to the correct ACTION_* constants."""
        assert PM_SETTLE == ACTION_SETTLE
        assert PM_SETTLE_ALL == ACTION_SETTLE_ALL
        assert PM_SETTLE_PAIR == ACTION_SETTLE_PAIR
        assert PM_TAKE == ACTION_TAKE
        assert PM_TAKE_ALL == ACTION_TAKE_ALL
        assert PM_TAKE_PAIR == ACTION_TAKE_PAIR
        assert PM_TAKE_PORTION == ACTION_TAKE_PORTION
        assert PM_CLOSE_CURRENCY == ACTION_CLOSE_CURRENCY
        assert PM_CLEAR_OR_TAKE == ACTION_CLEAR_OR_TAKE
        assert PM_SWEEP == ACTION_SWEEP

    def test_action_bytes_no_gaps_in_sequence(self):
        """Verify no gaps exist in the settlement action byte sequence (0x0B-0x14).

        A gap would indicate a missing constant from Actions.sol.
        """
        settlement_bytes = sorted([
            ACTION_SETTLE, ACTION_SETTLE_ALL, ACTION_SETTLE_PAIR,
            ACTION_TAKE, ACTION_TAKE_ALL, ACTION_TAKE_PORTION,
            ACTION_TAKE_PAIR, ACTION_CLOSE_CURRENCY,
            ACTION_CLEAR_OR_TAKE, ACTION_SWEEP,
        ])
        assert settlement_bytes == list(range(0x0B, 0x15))

    def test_modify_liquidities_selector(self):
        from eth_hash.auto import keccak

        expected = "0x" + keccak(b"modifyLiquidities(bytes,uint256)")[:4].hex()
        assert MODIFY_LIQUIDITIES_SELECTOR == expected, (
            f"Wrong selector: got {MODIFY_LIQUIDITIES_SELECTOR}, expected {expected}"
        )


# =============================================================================
# SDK LP methods tests
# =============================================================================


class TestSDKLPMethods:
    @pytest.fixture()
    def sdk(self):
        return UniswapV4SDK(chain="arbitrum")

    def test_compute_liquidity_from_amounts_in_range(self, sdk):
        """Liquidity from amounts when price is in the tick range."""
        sqrt_price = _tick_to_sqrt_ratio_x96(0)  # tick 0 ~ price 1.0
        tick_lower = -1000
        tick_upper = 1000
        amount0 = 1_000_000_000_000_000_000  # 1e18
        amount1 = 1_000_000_000_000_000_000  # 1e18

        liquidity = sdk.compute_liquidity_from_amounts(
            sqrt_price, tick_lower, tick_upper, amount0, amount1
        )
        assert liquidity > 0

    def test_compute_liquidity_from_amounts_below_range(self, sdk):
        """When price is below range, only amount0 matters."""
        sqrt_price = _tick_to_sqrt_ratio_x96(-5000)  # below range
        tick_lower = -1000
        tick_upper = 1000
        amount0 = 1_000_000_000_000_000_000
        amount1 = 0

        liquidity = sdk.compute_liquidity_from_amounts(
            sqrt_price, tick_lower, tick_upper, amount0, amount1
        )
        assert liquidity > 0

    def test_compute_liquidity_from_amounts_above_range(self, sdk):
        """When price is above range, only amount1 matters."""
        sqrt_price = _tick_to_sqrt_ratio_x96(5000)  # above range
        tick_lower = -1000
        tick_upper = 1000
        amount0 = 0
        amount1 = 1_000_000_000_000_000_000

        liquidity = sdk.compute_liquidity_from_amounts(
            sqrt_price, tick_lower, tick_upper, amount0, amount1
        )
        assert liquidity > 0

    def test_compute_liquidity_zero_amounts(self, sdk):
        sqrt_price = _tick_to_sqrt_ratio_x96(0)
        liquidity = sdk.compute_liquidity_from_amounts(
            sqrt_price, -1000, 1000, 0, 0
        )
        assert liquidity == 0

    def test_estimate_sqrt_price_x96(self, sdk):
        # Price of 1.0 with same decimals should give ~2^96
        sqrt_price = sdk.estimate_sqrt_price_x96(Decimal("1.0"))
        q96 = 2**96
        assert abs(sqrt_price - q96) / q96 < 0.01  # within 1%

    def test_estimate_sqrt_price_x96_cross_decimal(self, sdk):
        # USDC/WETH-like: price ~2000, decimals 6 and 18
        sqrt_price = sdk.estimate_sqrt_price_x96(Decimal("2000"), decimals0=6, decimals1=18)
        assert sqrt_price > 0

    def test_estimate_sqrt_price_x96_zero_raises(self, sdk):
        with pytest.raises(ValueError, match="positive"):
            sdk.estimate_sqrt_price_x96(Decimal("0"))

    def test_build_mint_position_tx(self, sdk):
        pool_key = sdk.compute_pool_key(
            "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
            "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            fee=3000,
        )
        params = LPMintParams(
            pool_key=pool_key,
            tick_lower=-60000,
            tick_upper=60000,
            liquidity=1_000_000,
            amount0_max=1_000_000_000_000_000_000,
            amount1_max=2_000_000_000,
            owner="0x1234567890abcdef1234567890abcdef12345678",
        )

        tx = sdk.build_mint_position_tx(params)
        assert tx.to.lower() == POSITION_MANAGER_ADDRESSES["arbitrum"].lower()
        assert tx.data.startswith("0x" + MODIFY_LIQUIDITIES_SELECTOR[2:])
        assert tx.gas_estimate == UNISWAP_V4_GAS_ESTIMATES["lp_mint"]
        assert tx.value == 0

    def test_mint_tx_encodes_correct_action_bytes(self, sdk):
        """Verify mint calldata encodes PM_MINT_POSITION + PM_SETTLE_PAIR (VIB-2097)."""
        pool_key = sdk.compute_pool_key(
            "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
            "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            fee=3000,
        )
        params = LPMintParams(
            pool_key=pool_key,
            tick_lower=-60000,
            tick_upper=60000,
            liquidity=1_000_000,
            amount0_max=1_000_000_000_000_000_000,
            amount1_max=2_000_000_000,
            owner="0x1234567890abcdef1234567890abcdef12345678",
        )

        tx = sdk.build_mint_position_tx(params)
        actions = _extract_actions_from_modify_liquidities(tx.data)
        assert actions == bytes([PM_MINT_POSITION, PM_SETTLE_PAIR]), (
            f"Mint actions must be exactly [0x02, 0x0D] but got: {actions.hex()}"
        )

    def test_build_decrease_liquidity_tx(self, sdk):
        params = LPDecreaseParams(
            token_id=42,
            liquidity=500_000,
        )

        tx = sdk.build_decrease_liquidity_tx(
            params=params,
            currency0="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            currency1="0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
            recipient="0x1234567890abcdef1234567890abcdef12345678",
            burn=True,
        )
        assert tx.to.lower() == POSITION_MANAGER_ADDRESSES["arbitrum"].lower()
        assert tx.data.startswith("0x" + MODIFY_LIQUIDITIES_SELECTOR[2:])
        assert tx.gas_estimate == UNISWAP_V4_GAS_ESTIMATES["lp_decrease"]
        assert "close" in tx.description.lower()

    def test_decrease_tx_encodes_correct_action_bytes(self, sdk):
        """Verify decrease calldata encodes PM_DECREASE_LIQUIDITY + PM_TAKE_PAIR (VIB-2097)."""
        params = LPDecreaseParams(token_id=42, liquidity=500_000)
        tx = sdk.build_decrease_liquidity_tx(
            params=params,
            currency0="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            currency1="0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
            recipient="0x1234567890abcdef1234567890abcdef12345678",
            burn=False,
        )
        actions = _extract_actions_from_modify_liquidities(tx.data)
        assert actions == bytes([PM_DECREASE_LIQUIDITY, PM_TAKE_PAIR]), (
            f"Decrease (no burn) actions must be exactly [0x01, 0x11] but got: {actions.hex()}"
        )
        assert PM_BURN_POSITION not in actions, (
            "PM_BURN_POSITION must not appear when burn=False"
        )

    def test_build_decrease_liquidity_tx_no_burn(self, sdk):
        params = LPDecreaseParams(token_id=42, liquidity=500_000)
        tx = sdk.build_decrease_liquidity_tx(
            params=params,
            currency0="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            currency1="0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
            recipient="0x1234567890abcdef1234567890abcdef12345678",
            burn=False,
        )
        assert "decrease" in tx.description.lower()

    def test_build_collect_fees_tx(self, sdk):
        tx = sdk.build_collect_fees_tx(
            token_id=42,
            currency0="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            currency1="0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
            recipient="0x1234567890abcdef1234567890abcdef12345678",
        )
        assert tx.to.lower() == POSITION_MANAGER_ADDRESSES["arbitrum"].lower()
        assert tx.data.startswith("0x" + MODIFY_LIQUIDITIES_SELECTOR[2:])
        assert tx.gas_estimate == UNISWAP_V4_GAS_ESTIMATES["lp_collect_fees"]
        assert "collect" in tx.description.lower()

    def test_build_mint_with_hook_data(self, sdk):
        pool_key = sdk.compute_pool_key(
            "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
            "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            fee=3000,
            hooks="0x00000000000000000000000000000000000000C0",
        )
        params = LPMintParams(
            pool_key=pool_key,
            tick_lower=-60000,
            tick_upper=60000,
            liquidity=1_000_000,
            amount0_max=1_000_000_000_000_000_000,
            amount1_max=2_000_000_000,
            owner="0x1234567890abcdef1234567890abcdef12345678",
            hook_data=bytes.fromhex("deadbeef"),
        )
        tx = sdk.build_mint_position_tx(params)
        assert tx.data.startswith("0x" + MODIFY_LIQUIDITIES_SELECTOR[2:])
        # hookData should be non-empty in the calldata
        assert "deadbeef" in tx.data.lower()

    def test_build_mint_position_tx_native_value(self, sdk):
        """Native ETH pool: tx.value must equal the native currency's amount_max."""
        pool_key = sdk.compute_pool_key(
            NATIVE_CURRENCY,  # currency0 = native ETH
            "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            fee=3000,
        )
        amount0_max = 500_000_000_000_000_000  # 0.5 ETH in wei
        params = LPMintParams(
            pool_key=pool_key,
            tick_lower=-60000,
            tick_upper=60000,
            liquidity=1_000_000,
            amount0_max=amount0_max,
            amount1_max=2_000_000_000,
            owner="0x1234567890abcdef1234567890abcdef12345678",
        )

        tx = sdk.build_mint_position_tx(params)
        assert tx.value == amount0_max, f"Expected native value {amount0_max}, got {tx.value}"

    def test_build_mint_position_tx_no_native_value(self, sdk):
        """ERC-20 only pool: tx.value must be 0."""
        pool_key = sdk.compute_pool_key(
            "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
            "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            fee=3000,
        )
        params = LPMintParams(
            pool_key=pool_key,
            tick_lower=-60000,
            tick_upper=60000,
            liquidity=1_000_000,
            amount0_max=1_000_000_000_000_000_000,
            amount1_max=2_000_000_000,
            owner="0x1234567890abcdef1234567890abcdef12345678",
        )

        tx = sdk.build_mint_position_tx(params)
        assert tx.value == 0


# =============================================================================
# Adapter LP compilation tests
# =============================================================================


class TestAdapterLPCompilation:
    @pytest.fixture()
    def mock_resolver(self):
        resolver = MagicMock()

        def resolve_for_swap(symbol, chain):
            tokens = {
                "WETH": MagicMock(address="0x82af49447d8a07e3bd95bd0d56f35241523fbab1", decimals=18, is_native=False),
                "USDC": MagicMock(address="0xaf88d065e77c8cc2239327c5edb3a432268e5831", decimals=6, is_native=False),
            }
            return tokens[symbol.upper()]

        def resolve(symbol_or_addr, chain):
            return resolve_for_swap(symbol_or_addr, chain)

        resolver.resolve_for_swap = resolve_for_swap
        resolver.resolve = resolve
        return resolver

    @pytest.fixture()
    def adapter(self, mock_resolver):
        from almanak.framework.connectors.uniswap_v4.adapter import UniswapV4Adapter, UniswapV4Config

        config = UniswapV4Config(
            chain="arbitrum",
            wallet_address="0x1234567890abcdef1234567890abcdef12345678",
        )
        return UniswapV4Adapter(config=config, token_resolver=mock_resolver)

    def test_compile_lp_open_intent(self, adapter):
        from almanak.framework.intents.vocabulary import LPOpenIntent

        intent = LPOpenIntent(
            pool="WETH/USDC/3000",
            amount0=Decimal("0.1"),
            amount1=Decimal("200"),
            range_lower=Decimal("1500"),
            range_upper=Decimal("2500"),
            protocol="uniswap_v4",
        )

        price_oracle = {"WETH": Decimal("2000"), "USDC": Decimal("1")}
        bundle = adapter.compile_lp_open_intent(intent, price_oracle)

        assert bundle.intent_type == "LP_OPEN"
        assert len(bundle.transactions) > 0
        assert bundle.metadata.get("protocol_version") == "v4"
        assert bundle.metadata.get("chain") == "arbitrum"
        assert bundle.metadata.get("position_manager") is not None

    def test_compile_lp_open_intent_no_wallet_raises(self, mock_resolver):
        from almanak.framework.connectors.uniswap_v4.adapter import UniswapV4Adapter, UniswapV4Config

        config = UniswapV4Config(chain="arbitrum", wallet_address="")
        adapter = UniswapV4Adapter(config=config, token_resolver=mock_resolver)

        from almanak.framework.intents.vocabulary import LPOpenIntent

        intent = LPOpenIntent(
            pool="WETH/USDC/3000",
            amount0=Decimal("0.1"),
            amount1=Decimal("200"),
            range_lower=Decimal("1500"),
            range_upper=Decimal("2500"),
            protocol="uniswap_v4",
        )
        with pytest.raises(ValueError, match="wallet_address"):
            adapter.compile_lp_open_intent(intent)

    def test_compile_lp_open_invalid_pool_format(self, adapter):
        from almanak.framework.intents.vocabulary import LPOpenIntent

        intent = LPOpenIntent(
            pool="WETH-USDC",  # wrong format
            amount0=Decimal("0.1"),
            amount1=Decimal("200"),
            range_lower=Decimal("1500"),
            range_upper=Decimal("2500"),
            protocol="uniswap_v4",
        )
        bundle = adapter.compile_lp_open_intent(intent)
        assert len(bundle.transactions) == 0
        assert "error" in bundle.metadata

    def test_compile_lp_open_with_hooks_warning(self, adapter):
        from almanak.framework.intents.vocabulary import LPOpenIntent

        # Hook address with before_add_liquidity (bit 11 = 0x800)
        intent = LPOpenIntent(
            pool="WETH/USDC/3000",
            amount0=Decimal("0.1"),
            amount1=Decimal("200"),
            range_lower=Decimal("1500"),
            range_upper=Decimal("2500"),
            protocol="uniswap_v4",
            protocol_params={
                "hooks": "0x0000000000000000000000000000000000000800",
            },
        )
        price_oracle = {"WETH": Decimal("2000"), "USDC": Decimal("1")}
        bundle = adapter.compile_lp_open_intent(intent, price_oracle)

        # Should have a warning about empty hookData
        assert bundle.metadata.get("warnings")
        assert any("hook" in w.lower() for w in bundle.metadata["warnings"])

    def test_compile_lp_close_intent(self, adapter):
        from almanak.framework.intents.vocabulary import LPCloseIntent

        intent = LPCloseIntent(
            position_id="42",
            protocol="uniswap_v4",
        )

        bundle = adapter.compile_lp_close_intent(
            intent,
            liquidity=1_000_000,
            currency0="0xaf88d065e77c8cc2239327c5edb3a432268e5831",
            currency1="0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
        )

        assert bundle.intent_type == "LP_CLOSE"
        assert len(bundle.transactions) == 1
        assert bundle.metadata.get("protocol_version") == "v4"
        assert bundle.metadata.get("position_id") == "42"

    def test_compile_lp_close_invalid_position_id(self, adapter):
        from almanak.framework.intents.vocabulary import LPCloseIntent

        intent = LPCloseIntent(
            position_id="not_a_number",
            protocol="uniswap_v4",
        )
        bundle = adapter.compile_lp_close_intent(intent)
        assert len(bundle.transactions) == 0
        assert "error" in bundle.metadata

    def test_compile_collect_fees_intent(self, adapter):
        bundle = adapter.compile_collect_fees_intent(
            position_id=42,
            currency0="0xaf88d065e77c8cc2239327c5edb3a432268e5831",
            currency1="0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
        )

        assert bundle.intent_type == "LP_COLLECT_FEES"
        assert len(bundle.transactions) == 1
        assert bundle.metadata.get("protocol_version") == "v4"
        assert bundle.metadata.get("position_id") == "42"

    def test_parse_pool_valid(self, adapter):
        t0, t1, fee = adapter._parse_pool("WETH/USDC/3000")
        assert t0 == "WETH"
        assert t1 == "USDC"
        assert fee == 3000

    def test_parse_pool_invalid(self, adapter):
        with pytest.raises(ValueError, match="Invalid pool format"):
            adapter._parse_pool("WETH-USDC")


# =============================================================================
# Receipt parser LP extraction tests
# =============================================================================


class TestReceiptParserLP:
    POSITION_MANAGER = "0xBd216513D74C8cf14cF4747E6AaE6fDf64e83b24"
    POOL_MANAGER = "0x000000000004444c5dc75cB358380D2e3dE08A90"

    @pytest.fixture()
    def parser(self):
        return UniswapV4ReceiptParser(
            chain="arbitrum",
            pool_manager_address=self.POOL_MANAGER,
            position_manager_address=self.POSITION_MANAGER,
        )

    def _mint_transfer_log(self, token_id: int) -> dict:
        """Build a mock ERC-721 Transfer log (mint) from PositionManager."""
        return {
            "address": self.POSITION_MANAGER,
            "topics": [
                EVENT_TOPICS["Transfer"],
                "0x0000000000000000000000000000000000000000000000000000000000000000",  # from: zero (mint)
                "0x0000000000000000000000001234567890abcdef1234567890abcdef12345678",  # to
                hex(token_id),  # tokenId
            ],
            "data": "0x",
        }

    def _modify_liquidity_log(self, liquidity_delta: int, tick_lower: int = -60000, tick_upper: int = 60000) -> dict:
        """Build a mock ModifyLiquidity event log."""
        # Encode data: int24 tickLower, int24 tickUpper, int256 liquidityDelta, bytes32 salt
        from almanak.framework.connectors.uniswap_v4.sdk import _pad_int24, _pad_uint

        data_hex = (
            "0x"
            + _pad_int24(tick_lower)
            + _pad_int24(tick_upper)
            + (_pad_uint(liquidity_delta) if liquidity_delta >= 0 else _pad_uint((1 << 256) + liquidity_delta))
            + "0" * 64  # salt
        )

        return {
            "address": self.POOL_MANAGER,
            "topics": [
                EVENT_TOPICS["ModifyLiquidity"],
                "0x" + "ab" * 32,  # pool_id
                "0x0000000000000000000000001234567890abcdef1234567890abcdef12345678",  # sender
            ],
            "data": data_hex,
        }

    def _erc20_transfer_log(self, token: str, from_addr: str, to_addr: str, amount: int) -> dict:
        """Build a mock ERC-20 Transfer log."""
        from almanak.framework.connectors.uniswap_v4.sdk import _pad_address, _pad_uint

        return {
            "address": token,
            "topics": [
                EVENT_TOPICS["Transfer"],
                "0x" + _pad_address(from_addr),
                "0x" + _pad_address(to_addr),
            ],
            "data": "0x" + _pad_uint(amount),
        }

    def test_extract_position_id_from_mint(self, parser):
        receipt = {
            "logs": [self._mint_transfer_log(token_id=123)],
        }
        position_id = parser.extract_position_id(receipt)
        assert position_id == 123

    def test_extract_position_id_none_when_no_mint(self, parser):
        receipt = {"logs": []}
        assert parser.extract_position_id(receipt) is None

    def test_extract_position_id_ignores_non_pm_transfers(self, parser):
        """Transfer events from other contracts should not be matched."""
        log = self._mint_transfer_log(token_id=999)
        log["address"] = "0x0000000000000000000000000000000000001234"  # not PM
        receipt = {"logs": [log]}
        assert parser.extract_position_id(receipt) is None

    def test_extract_liquidity_from_mint(self, parser):
        receipt = {
            "logs": [self._modify_liquidity_log(liquidity_delta=1_000_000)],
        }
        liquidity = parser.extract_liquidity(receipt)
        assert liquidity == 1_000_000

    def test_extract_liquidity_none_when_no_events(self, parser):
        receipt = {"logs": []}
        assert parser.extract_liquidity(receipt) is None

    def test_extract_lp_close_data(self, parser):
        token_a = "0x000000000000000000000000000000000000000a"  # sorted first
        token_b = "0x000000000000000000000000000000000000000b"

        receipt = {
            "logs": [
                # Decrease liquidity event (negative delta)
                self._modify_liquidity_log(liquidity_delta=-500_000),
                # Token transfers out from pool manager
                self._erc20_transfer_log(token_a, self.POOL_MANAGER, "0x1234567890abcdef1234567890abcdef12345678", 1000),
                self._erc20_transfer_log(token_b, self.POOL_MANAGER, "0x1234567890abcdef1234567890abcdef12345678", 2000),
            ],
        }
        lp_close_data = parser.extract_lp_close_data(receipt)
        assert lp_close_data is not None
        assert lp_close_data.liquidity_removed == 500_000
        assert lp_close_data.amount0_collected == 1000
        assert lp_close_data.amount1_collected == 2000

    def test_extract_lp_close_data_none_when_no_events(self, parser):
        receipt = {"logs": []}
        assert parser.extract_lp_close_data(receipt) is None

    def test_parse_receipt_includes_modify_liquidity(self, parser):
        receipt = {
            "logs": [self._modify_liquidity_log(liquidity_delta=1_000_000)],
        }
        result = parser.parse_receipt(receipt)
        assert len(result.modify_liquidity_events) == 1
        assert result.modify_liquidity_events[0].liquidity_delta == 1_000_000
        assert result.modify_liquidity_events[0].tick_lower == -60000
        assert result.modify_liquidity_events[0].tick_upper == 60000


# =============================================================================
# Compiler routing tests
# =============================================================================


class TestCompilerV4LPRouting:
    """Test that the compiler correctly routes V4 LP intents."""

    def test_lp_open_routes_to_v4(self):
        """Verify _compile_lp_open dispatches to V4 when protocol='uniswap_v4'."""
        from unittest.mock import patch

        from almanak.framework.intents.compiler import IntentCompiler
        from almanak.framework.intents.vocabulary import LPOpenIntent

        intent = LPOpenIntent(
            pool="WETH/USDC/3000",
            amount0=Decimal("0.1"),
            amount1=Decimal("200"),
            range_lower=Decimal("1500"),
            range_upper=Decimal("2500"),
            protocol="uniswap_v4",
        )

        compiler = IntentCompiler.__new__(IntentCompiler)
        compiler.chain = "arbitrum"
        compiler.wallet_address = "0x1234567890abcdef1234567890abcdef12345678"
        compiler.price_oracle = {}
        compiler._token_resolver = None

        with patch.object(compiler, "_compile_lp_open_uniswap_v4") as mock_v4:
            mock_v4.return_value = MagicMock()
            compiler._compile_lp_open(intent)
            mock_v4.assert_called_once_with(intent)

    def test_lp_close_routes_to_v4(self):
        """Verify _compile_lp_close dispatches to V4 when protocol='uniswap_v4'."""
        from unittest.mock import patch

        from almanak.framework.intents.compiler import IntentCompiler
        from almanak.framework.intents.vocabulary import LPCloseIntent

        intent = LPCloseIntent(
            position_id="42",
            protocol="uniswap_v4",
        )

        compiler = IntentCompiler.__new__(IntentCompiler)
        compiler.chain = "arbitrum"
        compiler.wallet_address = "0x1234567890abcdef1234567890abcdef12345678"
        compiler.price_oracle = {}
        compiler._token_resolver = None

        with patch.object(compiler, "_compile_lp_close_uniswap_v4") as mock_v4:
            mock_v4.return_value = MagicMock()
            compiler._compile_lp_close(intent)
            mock_v4.assert_called_once_with(intent)

    def test_v4_in_lp_position_managers(self):
        """Verify V4 PositionManager is in LP_POSITION_MANAGERS for supported chains."""
        from almanak.framework.intents.compiler import LP_POSITION_MANAGERS

        for chain in ["ethereum", "arbitrum", "base", "optimism", "polygon", "avalanche", "bsc"]:
            assert "uniswap_v4" in LP_POSITION_MANAGERS[chain], f"uniswap_v4 missing from {chain}"
            assert LP_POSITION_MANAGERS[chain]["uniswap_v4"].lower() == "0xbd216513d74c8cf14cf4747e6aae6fdf64e83b24"
