"""Tests for Uniswap V4 SDK."""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import patch

import pytest

from almanak.connectors.uniswap_v4.sdk import (
    FEE_TIERS,
    NATIVE_CURRENCY,
    PERMIT2_ADDRESS,
    PERMIT2_APPROVE_SELECTOR,
    POOL_MANAGER_ADDRESSES,
    QUOTER_ADDRESSES,
    ROUTER_ADDRESSES,
    TICK_SPACING,
    UNISWAP_V4_GAS_ESTIMATES,
    UNIVERSAL_ROUTER_EXECUTE_SELECTOR,
    V4_SWAP_EXACT_IN_SINGLE,
    PoolKey,
    SwapQuote,
    UniswapV4SDK,
    _encode_execute,
)

# =============================================================================
# Constants tests
# =============================================================================


class TestConstants:
    def test_fee_tiers(self):
        assert FEE_TIERS == [100, 500, 3000, 10000]

    def test_tick_spacing(self):
        assert TICK_SPACING[100] == 1
        assert TICK_SPACING[3000] == 60

    def test_gas_estimates(self):
        assert UNISWAP_V4_GAS_ESTIMATES["approve"] == 65_000
        assert UNISWAP_V4_GAS_ESTIMATES["swap"] == 250_000

    def test_pool_manager_addresses(self):
        # Each chain should have a non-empty pool manager address
        from almanak.connectors.uniswap_v4.addresses import UNISWAP_V4

        for chain, addr in POOL_MANAGER_ADDRESSES.items():
            expected = UNISWAP_V4[chain]["pool_manager"].lower()
            assert addr.lower() == expected, f"PoolManager on {chain} mismatch"

    def test_router_addresses(self):
        expected_chains = {"ethereum", "arbitrum", "base", "optimism", "polygon", "avalanche", "bsc"}
        assert expected_chains.issubset(set(ROUTER_ADDRESSES.keys()))

    def test_quoter_addresses(self):
        expected_chains = {"ethereum", "arbitrum", "base", "optimism", "polygon", "avalanche", "bsc"}
        assert expected_chains.issubset(set(QUOTER_ADDRESSES.keys()))


# =============================================================================
# PoolKey tests
# =============================================================================


class TestPoolKey:
    def test_sorted_order(self):
        """Pool key should sort currency0 < currency1."""
        key = PoolKey(
            currency0="0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            currency1="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            fee=3000,
            tick_spacing=60,
        )
        # Should swap since 0xbb > 0xaa
        assert int(key.currency0, 16) < int(key.currency1, 16)

    def test_already_sorted(self):
        key = PoolKey(
            currency0="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            currency1="0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            fee=3000,
            tick_spacing=60,
        )
        assert key.currency0 == "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        assert key.currency1 == "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

    def test_native_currency(self):
        """Native ETH (zero address) should always be currency0."""
        key = PoolKey(
            currency0="0xaf88d065e77c8cc2239327c5edb3a432268e5831",  # USDC
            currency1=NATIVE_CURRENCY,
            fee=3000,
            tick_spacing=60,
        )
        assert key.currency0 == NATIVE_CURRENCY

    def test_hooks_default(self):
        key = PoolKey(
            currency0="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            currency1="0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            fee=3000,
            tick_spacing=60,
        )
        assert key.hooks == NATIVE_CURRENCY

    def test_custom_hooks(self):
        key = PoolKey(
            currency0="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            currency1="0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            fee=3000,
            tick_spacing=60,
            hooks="0x1234567890123456789012345678901234567890",
        )
        assert key.hooks == "0x1234567890123456789012345678901234567890"


# =============================================================================
# SDK initialization tests
# =============================================================================


class TestUniswapV4SDKInit:
    def test_init_supported_chain(self):
        from almanak.connectors.uniswap_v4.addresses import UNISWAP_V4

        sdk = UniswapV4SDK(chain="arbitrum")
        assert sdk.chain == "arbitrum"
        assert sdk.pool_manager.lower() == UNISWAP_V4["arbitrum"]["pool_manager"].lower()

    def test_init_unsupported_chain(self):
        with pytest.raises(ValueError, match="not supported"):
            UniswapV4SDK(chain="fantom")

    def test_init_case_insensitive(self):
        sdk = UniswapV4SDK(chain="Arbitrum")
        assert sdk.chain == "arbitrum"


# =============================================================================
# Pool key computation tests
# =============================================================================


class TestComputePoolKey:
    def test_default_tick_spacing(self):
        sdk = UniswapV4SDK(chain="arbitrum")
        key = sdk.compute_pool_key(
            token0="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            token1="0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            fee=3000,
        )
        assert key.tick_spacing == 60

    def test_custom_tick_spacing(self):
        sdk = UniswapV4SDK(chain="arbitrum")
        key = sdk.compute_pool_key(
            token0="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            token1="0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            fee=3000,
            tick_spacing=10,
        )
        assert key.tick_spacing == 10

    def test_fee_100_tick_spacing(self):
        sdk = UniswapV4SDK(chain="arbitrum")
        key = sdk.compute_pool_key(
            token0="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            token1="0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            fee=100,
        )
        assert key.tick_spacing == 1


# =============================================================================
# Local quote tests
# =============================================================================


class TestGetQuoteLocal:
    def test_basic_quote(self):
        sdk = UniswapV4SDK(chain="arbitrum")
        quote = sdk.get_quote_local(
            token_in="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            token_out="0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            amount_in=10**18,
            fee_tier=3000,
        )
        assert quote.amount_in == 10**18
        assert quote.amount_out > 0
        assert quote.amount_out < 10**18  # Less due to fees

    def test_quote_with_price_ratio(self):
        sdk = UniswapV4SDK(chain="arbitrum")
        quote = sdk.get_quote_local(
            token_in="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            token_out="0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            amount_in=1000 * 10**6,  # 1000 USDC
            fee_tier=500,
            token_in_decimals=6,
            token_out_decimals=18,
            price_ratio=Decimal("0.0005"),  # 1 USDC = 0.0005 ETH
        )
        assert quote.amount_out > 0
        assert quote.effective_price is not None

    def test_quote_fee_deduction(self):
        """Quote should deduct fees from output."""
        sdk = UniswapV4SDK(chain="arbitrum")
        amount_in = 10**18

        # 0.3% fee tier
        quote = sdk.get_quote_local(
            token_in="0xaaaa",
            token_out="0xbbbb",
            amount_in=amount_in,
            fee_tier=3000,
        )
        # Should be approximately 99.7% of input
        expected = int(Decimal(amount_in) * Decimal("0.997"))
        assert abs(quote.amount_out - expected) < 2  # Allow rounding

    def test_same_decimal_fallback_when_price_ratio_missing(self):
        """VIB-3875: same-decimal pairs without price_ratio still produce a quote."""
        sdk = UniswapV4SDK(chain="arbitrum")
        quote = sdk.get_quote_local(
            token_in="0xaaaa",
            token_out="0xbbbb",
            amount_in=10**18,
            fee_tier=3000,
            token_in_decimals=18,
            token_out_decimals=18,
            price_ratio=None,
        )
        assert quote.amount_out > 0

    def test_decimal_mismatch_without_price_ratio_raises_permanent(self):
        """VIB-3875: decimal mismatch + price_ratio=None must raise rather than
        silently emit a 10**12x-too-high amount_out_minimum that reverts on-chain
        with V4TooLittleReceived. Error must contain a ``permanent_keywords``
        token (see ``state_machine._categorize_error``) so the strategy classifies
        it COMPILATION_PERMANENT and does not retry the impossible swap.
        """
        import pytest

        sdk = UniswapV4SDK(chain="arbitrum")
        with pytest.raises(ValueError, match=r"not supported"):
            sdk.get_quote_local(
                token_in="0xaaaa",
                token_out="0xbbbb",
                amount_in=10**18,  # 1.0 in 18-dec
                fee_tier=3000,
                token_in_decimals=18,
                token_out_decimals=6,  # USDC
                price_ratio=None,
            )

    def test_decimal_mismatch_with_price_ratio_succeeds(self):
        """VIB-3875 control: decimal mismatch IS allowed when price_ratio bridges it."""
        sdk = UniswapV4SDK(chain="arbitrum")
        quote = sdk.get_quote_local(
            token_in="0xaaaa",
            token_out="0xbbbb",
            amount_in=10**18,  # 1.0 of 18-dec token
            fee_tier=500,
            token_in_decimals=18,
            token_out_decimals=6,  # USDC
            price_ratio=Decimal("3000"),  # 1 in = 3000 out
        )
        # Expect ~3000 USDC * (1 - 0.05%) = ~2998.5 USDC in 6-dec units
        assert 2_990_000_000 <= quote.amount_out <= 3_000_000_000


class TestGetQuote:
    RPC_URL = "https://arb-mainnet.g.alchemy.com/v2/test"
    TOKEN_IN = "0x1111111111111111111111111111111111111111"
    TOKEN_OUT = "0x2222222222222222222222222222222222222222"

    def _make_sdk(self) -> UniswapV4SDK:
        return UniswapV4SDK(chain="arbitrum", rpc_url=self.RPC_URL)

    def test_transport_failure_raises_value_error(self):
        sdk = self._make_sdk()

        with patch("almanak.connectors.uniswap_v4.sdk.eth_call", side_effect=ConnectionError("timeout")):
            with pytest.raises(ValueError, match="V4 Quoter quoteExactInputSingle failed"):
                sdk.get_quote(
                    token_in=self.TOKEN_IN,
                    token_out=self.TOKEN_OUT,
                    amount_in=100,
                    fee_tier=3000,
                )

    def test_no_result_raises_value_error(self):
        sdk = self._make_sdk()

        with patch("almanak.connectors.uniswap_v4.sdk.eth_call", return_value=None):
            with pytest.raises(ValueError, match="returned no result"):
                sdk.get_quote(
                    token_in=self.TOKEN_IN,
                    token_out=self.TOKEN_OUT,
                    amount_in=100,
                    fee_tier=3000,
                )

    def test_malformed_response_raises_value_error_with_payload(self):
        sdk = self._make_sdk()

        with patch("almanak.connectors.uniswap_v4.sdk.eth_call", return_value=b"\x12\x34"):
            with pytest.raises(ValueError, match="Malformed V4 Quoter response") as exc_info:
                sdk.get_quote(
                    token_in=self.TOKEN_IN,
                    token_out=self.TOKEN_OUT,
                    amount_in=100,
                    fee_tier=3000,
                )

        assert "0x1234" in str(exc_info.value)


# =============================================================================
# Transaction building tests
# =============================================================================


class TestBuildSwapTx:
    def test_build_swap_tx(self):
        sdk = UniswapV4SDK(chain="arbitrum")
        quote = SwapQuote(
            amount_in=10**18,
            amount_out=997 * 10**15,
            fee_tier=3000,
            token_in="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            token_out="0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        )
        tx = sdk.build_swap_tx(quote, recipient="0x1234567890123456789012345678901234567890")
        assert tx.to == sdk.router
        assert tx.data.startswith("0x3593564c")  # UniversalRouter execute selector
        assert tx.gas_estimate == 250_000
        assert tx.value == 0  # Not native ETH

    def test_build_native_swap_tx(self):
        sdk = UniswapV4SDK(chain="arbitrum")
        quote = SwapQuote(
            amount_in=10**18,
            amount_out=997 * 10**15,
            fee_tier=3000,
            token_in=NATIVE_CURRENCY,
            token_out="0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        )
        tx = sdk.build_swap_tx(quote, recipient="0x1234567890123456789012345678901234567890")
        assert tx.value == 10**18  # ETH value set


class TestWethRouting:
    """Test WETH -> native ETH routing in build_swap_tx."""

    WETH_ETHEREUM = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
    USDC = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
    RECIPIENT = "0x1234567890123456789012345678901234567890"

    @staticmethod
    def _extract_commands(calldata: str) -> list[int]:
        """Extract UniversalRouter command bytes from execute() calldata.

        execute(bytes commands, bytes[] inputs, uint256 deadline)
        ABI layout: selector(4) + offset_commands(32) + offset_inputs(32) + deadline(32)
                    + commands_len(32) + commands_data(padded)
        """
        raw = calldata[2:]  # strip 0x
        # offset to commands data: read first 32-byte word after selector
        cmd_offset = int(raw[8:72], 16) * 2  # byte offset -> hex char offset
        # commands data starts at: selector(8) + cmd_offset
        cmd_start = 8 + cmd_offset
        cmd_len = int(raw[cmd_start : cmd_start + 64], 16)
        cmd_hex = raw[cmd_start + 64 : cmd_start + 64 + cmd_len * 2]
        return [int(cmd_hex[i : i + 2], 16) for i in range(0, len(cmd_hex), 2)]

    def test_weth_in_produces_permit2_transfer_and_unwrap(self):
        """WETH -> ERC20: commands = [PERMIT2_TRANSFER_FROM, UNWRAP_WETH, V4_SWAP]."""
        sdk = UniswapV4SDK(chain="ethereum")
        quote = SwapQuote(
            amount_in=5 * 10**16,
            amount_out=100 * 10**6,
            fee_tier=500,
            token_in=self.WETH_ETHEREUM,
            token_out=self.USDC,
        )
        tx = sdk.build_swap_tx(quote, recipient=self.RECIPIENT)
        assert tx.value == 0, "WETH-in should not send native ETH"
        cmds = self._extract_commands(tx.data)
        assert cmds == [0x02, 0x0C, 0x10], (
            f"Expected [PERMIT2_TRANSFER_FROM, UNWRAP_WETH, V4_SWAP], got {[hex(c) for c in cmds]}"
        )

    def test_weth_out_produces_wrap_eth(self):
        """ERC20 -> WETH: commands = [V4_SWAP, WRAP_ETH]."""
        sdk = UniswapV4SDK(chain="ethereum")
        quote = SwapQuote(
            amount_in=100 * 10**6,
            amount_out=5 * 10**16,
            fee_tier=500,
            token_in=self.USDC,
            token_out=self.WETH_ETHEREUM,
        )
        tx = sdk.build_swap_tx(quote, recipient=self.RECIPIENT)
        assert tx.value == 0, "ERC20-in should not send native ETH"
        cmds = self._extract_commands(tx.data)
        assert cmds == [0x10, 0x0B], f"Expected [V4_SWAP, WRAP_ETH], got {[hex(c) for c in cmds]}"

    def test_weth_to_weth_raises(self):
        """WETH -> WETH: should raise ValueError."""
        sdk = UniswapV4SDK(chain="ethereum")
        quote = SwapQuote(
            amount_in=10**18,
            amount_out=10**18,
            fee_tier=500,
            token_in=self.WETH_ETHEREUM,
            token_out=self.WETH_ETHEREUM,
        )
        with pytest.raises(ValueError, match="Cannot swap wrapped native token to itself"):
            sdk.build_swap_tx(quote, recipient=self.RECIPIENT)

    def test_native_eth_passthrough_unchanged(self):
        """Native ETH swap should NOT trigger WETH routing."""
        sdk = UniswapV4SDK(chain="ethereum")
        quote = SwapQuote(
            amount_in=10**18,
            amount_out=100 * 10**6,
            fee_tier=500,
            token_in=NATIVE_CURRENCY,
            token_out=self.USDC,
        )
        tx = sdk.build_swap_tx(quote, recipient=self.RECIPIENT)
        assert tx.value == 10**18, "Native ETH-in should set msg.value"
        cmds = self._extract_commands(tx.data)
        assert cmds == [0x10], f"Native ETH should be single V4_SWAP, got {[hex(c) for c in cmds]}"

    def test_native_eth_out_has_sweep(self):
        """ERC20 -> native ETH: commands = [V4_SWAP, SWEEP]."""
        sdk = UniswapV4SDK(chain="ethereum")
        quote = SwapQuote(
            amount_in=100 * 10**6,
            amount_out=5 * 10**16,
            fee_tier=500,
            token_in=self.USDC,
            token_out=NATIVE_CURRENCY,
        )
        tx = sdk.build_swap_tx(quote, recipient=self.RECIPIENT)
        assert tx.value == 0
        cmds = self._extract_commands(tx.data)
        assert cmds == [0x10, 0x04], f"Expected [V4_SWAP, SWEEP], got {[hex(c) for c in cmds]}"


class TestBuildApproveTx:
    def test_build_approve(self):
        sdk = UniswapV4SDK(chain="arbitrum")
        tx = sdk.build_approve_tx(
            token_address="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            spender="0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            amount=10**18,
        )
        assert tx.data.startswith("0x095ea7b3")
        assert tx.gas_estimate == 65_000
        assert tx.value == 0


# =============================================================================
# Tick math tests
# =============================================================================


class TestTickMath:
    def test_tick_to_price_zero(self):
        price = UniswapV4SDK.tick_to_price(0)
        assert abs(price - Decimal("1")) < Decimal("0.001")

    def test_price_to_tick_roundtrip(self):
        price = Decimal("2000")
        tick = UniswapV4SDK.price_to_tick(price)
        recovered = UniswapV4SDK.tick_to_price(tick)
        # Should be within 0.1% due to tick discretization
        assert abs(float(recovered - price) / float(price)) < 0.001

    def test_price_to_tick_negative(self):
        with pytest.raises(ValueError, match="positive"):
            UniswapV4SDK.price_to_tick(Decimal("-1"))


# =============================================================================
# Permit2 approve tests
# =============================================================================


class TestBuildPermit2ApproveTx:
    def test_basic_permit2_approve(self):
        sdk = UniswapV4SDK(chain="arbitrum")
        tx = sdk.build_permit2_approve_tx(
            token_address="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            spender="0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            amount=10**18,
            expiration=1_700_000_000,
        )
        assert tx.to == PERMIT2_ADDRESS
        assert tx.value == 0
        assert tx.data.startswith(PERMIT2_APPROVE_SELECTOR)
        assert tx.gas_estimate == UNISWAP_V4_GAS_ESTIMATES["permit2_approve"]

    def test_permit2_approve_clamps_uint160(self):
        sdk = UniswapV4SDK(chain="arbitrum")
        huge_amount = 1 << 200  # exceeds uint160
        tx = sdk.build_permit2_approve_tx(
            token_address="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            spender="0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            amount=huge_amount,
            expiration=1_700_000_000,
        )
        assert tx.to == PERMIT2_ADDRESS
        # Decode amount word (3rd word after selector) and verify uint160 clamp
        payload = tx.data[10:]  # strip 0x + 4-byte selector
        amount_word = int(payload[128:192], 16)
        assert amount_word == (1 << 160) - 1

    def test_permit2_approve_default_expiration(self):
        import time

        sdk = UniswapV4SDK(chain="arbitrum")
        before = int(time.time())
        tx = sdk.build_permit2_approve_tx(
            token_address="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            spender="0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            amount=10**18,
        )
        after = int(time.time())
        assert tx.to == PERMIT2_ADDRESS
        assert tx.data.startswith(PERMIT2_APPROVE_SELECTOR)
        # Verify auto-generated expiration is ~30 days from now
        payload = tx.data[10:]
        expiration = int(payload[192:256], 16)
        thirty_days = 30 * 86400
        assert before + thirty_days <= expiration <= after + thirty_days


# =============================================================================
# UniversalRouter encode_execute tests
# =============================================================================


class TestEncodeExecute:
    def test_single_command_structure(self):
        """Verify _encode_execute produces valid ABI structure."""
        # Use a simple 11-word (352 byte) input for predictability
        dummy_input = "aa" * 352  # 352 bytes = 11 words
        result = _encode_execute(
            commands=bytes([V4_SWAP_EXACT_IN_SINGLE]),
            inputs=[dummy_input],
            deadline=1_700_000_000,
        )
        # Must start with UniversalRouter execute selector
        assert result.startswith("0x" + UNIVERSAL_ROUTER_EXECUTE_SELECTOR[2:])
        # Strip selector
        body = result[10:]  # remove 0x + 8 hex selector chars
        # All hex, no odd-length
        assert len(body) % 64 == 0, "ABI encoding must be 32-byte aligned"

    def test_deadline_encoded(self):
        """Verify the deadline appears in the third head slot."""
        result = _encode_execute(
            commands=bytes([0x06]),
            inputs=["00" * 32],
            deadline=12345,
        )
        body = result[10:]
        # Third 32-byte word in head is the deadline
        deadline_word = body[128:192]
        assert int(deadline_word, 16) == 12345

    def test_commands_length_encoded(self):
        """Verify command bytes length is correctly encoded."""
        result = _encode_execute(
            commands=bytes([0x06, 0x07]),
            inputs=["00" * 32, "00" * 32],
            deadline=1_000,
        )
        body = result[10:]
        # commands section starts at offset 0x60 = 96 bytes = 192 hex chars
        commands_length_word = body[192:256]
        assert int(commands_length_word, 16) == 2  # 2 command bytes


# =============================================================================
# ExactInputSingleParams encoding tests
# =============================================================================


class TestEncodeExactInputSingleParams:
    def test_params_contain_pool_key_and_amounts(self):
        """Verify the encoded params contain expected pool key fields."""
        sdk = UniswapV4SDK(chain="arbitrum")
        quote = SwapQuote(
            amount_in=10**18,
            amount_out=997 * 10**15,
            fee_tier=3000,
            token_in="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            token_out="0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        )
        params = sdk._encode_exact_input_single_params(quote, amount_out_minimum=990 * 10**15)
        # Should be hex string (no 0x prefix)
        assert not params.startswith("0x")
        # VIB-4413: 11 words = leading 0x20 struct-offset pointer (ExactInputSingleParams
        # is a dynamic tuple because of hookData) + 5 pool key + zeroForOne + amountIn +
        # amountOutMin + hookData offset + hookData length. (sqrtPriceLimitX96 removed in
        # the deployed V4 contracts.)
        words = [params[i : i + 64] for i in range(0, len(params), 64)]
        assert len(words) == 11, f"Expected 11 words, got {len(words)}"
        # Word 0 is the 0x20 offset pointer to the struct.
        assert int(words[0], 16) == 0x20, "First word must be the 0x20 dynamic-tuple offset"
        # Struct fields are offset by 1 word: fee tier (word 3), amountIn (word 7),
        # amountOutMinimum (word 8), hookData offset within struct (word 9) == 0x120.
        assert int(words[3], 16) == 3000  # fee_tier
        assert int(words[7], 16) == 10**18  # amount_in
        assert int(words[8], 16) == 990 * 10**15  # amount_out_minimum
        assert int(words[9], 16) == 0x120  # hookData offset (relative to struct start)

    def test_integer_slippage_precision(self):
        """Verify integer floor division gives correct amount_out_minimum."""
        sdk = UniswapV4SDK(chain="arbitrum")
        quote = SwapQuote(
            amount_in=10**30,
            amount_out=10**30,
            fee_tier=3000,
            token_in="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            token_out="0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        )
        # build_swap_tx uses integer floor division now
        tx = sdk.build_swap_tx(quote, recipient="0x1234567890123456789012345678901234567890", slippage_bps=50)
        # The amount_out_minimum should be exactly: 10^30 * 9950 // 10000
        expected = 10**30 * 9950 // 10000
        assert expected == 995000000000000000000000000000
        # Verify the encoded calldata contains the exact expected value
        assert f"{expected:064x}" in tx.data


# =============================================================================
# get_position_liquidity tests
# =============================================================================


class TestGetPositionLiquidity:
    """Unit tests for on-chain liquidity query via eth_call."""

    RPC_URL = "https://arb-mainnet.g.alchemy.com/v2/test"
    TOKEN_ID = 42

    def _make_sdk(self) -> UniswapV4SDK:
        sdk = UniswapV4SDK(chain="arbitrum")
        sdk.rpc_url = self.RPC_URL
        return sdk

    def test_success_returns_liquidity(self):
        sdk = self._make_sdk()
        liquidity_value = 123456789

        with patch("almanak.connectors.uniswap_v4.sdk.eth_call_hex", return_value=hex(liquidity_value)) as mock_call:
            result = sdk.get_position_liquidity(self.TOKEN_ID)

        assert result == liquidity_value
        call_kwargs = mock_call.call_args.kwargs
        assert call_kwargs["data"].startswith("0x1efeed33")
        assert call_kwargs["to"] == sdk.position_manager

    def test_rpc_error_response_raises(self):
        sdk = self._make_sdk()

        with patch("almanak.connectors.uniswap_v4.sdk.eth_call_hex", side_effect=ValueError("reverted")):
            with pytest.raises(ValueError, match="reverted"):
                sdk.get_position_liquidity(self.TOKEN_ID)

    def test_missing_rpc_url_raises(self):
        sdk = self._make_sdk()
        sdk.rpc_url = None

        with pytest.raises(ValueError, match="RPC URL required"):
            sdk.get_position_liquidity(self.TOKEN_ID, rpc_url=None)

    def test_malformed_result_missing_field_raises(self):
        sdk = self._make_sdk()

        with patch("almanak.connectors.uniswap_v4.sdk.eth_call_hex", return_value=None):
            with pytest.raises(ValueError, match="returned no result"):
                sdk.get_position_liquidity(self.TOKEN_ID)

    def test_malformed_result_non_hex_raises(self):
        sdk = self._make_sdk()

        with patch("almanak.connectors.uniswap_v4.sdk.eth_call_hex", return_value="not_hex"):
            with pytest.raises(ValueError, match="Malformed liquidity hex"):
                sdk.get_position_liquidity(self.TOKEN_ID)

    def test_transport_failure_raises(self):
        sdk = self._make_sdk()

        with patch("almanak.connectors.uniswap_v4.sdk.eth_call_hex", side_effect=ConnectionError("timeout")):
            with pytest.raises(ValueError, match="RPC call to getPositionLiquidity failed"):
                sdk.get_position_liquidity(self.TOKEN_ID)

    def test_explicit_rpc_url_overrides_default(self):
        sdk = self._make_sdk()
        override_url = "https://other-rpc.example.com"

        with patch("almanak.connectors.uniswap_v4.sdk.eth_call_hex", return_value="0x1") as mock_call:
            sdk.get_position_liquidity(self.TOKEN_ID, rpc_url=override_url)

        assert mock_call.call_args.kwargs["rpc_url"] == override_url

    def test_file_scheme_rejected(self):
        sdk = self._make_sdk()
        with pytest.raises(ValueError, match="Unsupported RPC URL scheme 'file'"):
            sdk.get_position_liquidity(self.TOKEN_ID, rpc_url="file:///etc/hosts")

    def test_calldata_encodes_token_id(self):
        sdk = self._make_sdk()
        token_id = 9999

        with patch("almanak.connectors.uniswap_v4.sdk.eth_call_hex", return_value="0x0") as mock_call:
            sdk.get_position_liquidity(token_id)

        calldata = mock_call.call_args.kwargs["data"]
        # Token ID should be zero-padded to 64 hex chars after selector
        expected_token_hex = format(token_id, "064x")
        assert calldata == "0x1efeed33" + expected_token_hex
