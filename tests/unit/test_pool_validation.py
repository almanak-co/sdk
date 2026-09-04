"""Unit tests for connector-owned pool validation.

The per-DEX pool-existence validators now live in the owning connectors
(``almanak.connectors.<dex>.pool_validation``) over a shared strategy-side base
(``almanak.connectors._strategy_base.pool_validation_base``), with protocol
dispatch in ``almanak.connectors._strategy_base.pool_validation_registry``.

Tests run without Anvil or live RPC — the on-chain ``eth_call`` is mocked at the
owning connector module's namespace. Verified behaviour:
- No RPC URL -> returns exists=None with warning
- Unknown chain/protocol -> returns exists=None with warning
- Correct calldata encoding
- Zero address detection
- Registry dispatch routes each protocol to its owning validator
"""

from unittest.mock import MagicMock, patch

import pytest

from almanak.connectors._strategy_base.pool_validation_base import (
    ZERO_ADDRESS,
    PoolValidationReason,
    PoolValidationResult,
    decode_address,
)
from almanak.connectors._strategy_base.pool_validation_registry import (
    PoolValidationRegistry,
    validate_pool,
)

# The V3-family validator + slot0 reader moved to the shared connector
# foundation (almanak.connectors._strategy_base.v3_pool_validation); the
# uniswap_v3 module re-exports the public surface. Test the internals (and
# patch eth_call) at their new home.
from almanak.connectors._strategy_base.v3_pool_validation import (
    _encode_get_pool_v3,
    fetch_v3_pool_sqrt_price_x96,
    validate_v3_pool,
)
from almanak.connectors.aerodrome.addresses import SlipstreamDeployment, slipstream_deployment_for_factory
from almanak.connectors.aerodrome.pool_validation import (
    _encode_get_pool_aerodrome,
    resolve_slipstream_pool_key,
    validate_aerodrome_cl_pool,
    validate_aerodrome_pool,
)
from almanak.connectors.traderjoe_v2.pool_validation import validate_traderjoe_pool

# eth_call patch targets — the connector validators call the base ``eth_call``
# through the name imported into their own module namespace.
V3_ETH_CALL = "almanak.connectors._strategy_base.v3_pool_validation.eth_call"
AERODROME_ETH_CALL = "almanak.connectors.aerodrome.pool_validation.eth_call"
TRADERJOE_ETH_CALL = "almanak.connectors.traderjoe_v2.pool_validation.eth_call"


class TestPoolValidationResult:
    """Test PoolValidationResult dataclass."""

    def test_exists_true(self):
        result = PoolValidationResult(exists=True, reason=PoolValidationReason.CONFIRMED, pool_address="0xabc")
        assert result.exists is True
        assert result.reason == PoolValidationReason.CONFIRMED
        assert result.pool_address == "0xabc"
        assert result.error is None
        assert result.warning is None

    def test_exists_false(self):
        result = PoolValidationResult(exists=False, reason=PoolValidationReason.NOT_FOUND, error="Pool not found")
        assert result.exists is False
        assert result.reason == PoolValidationReason.NOT_FOUND
        assert result.error == "Pool not found"

    def test_exists_none(self):
        result = PoolValidationResult(exists=None, reason=PoolValidationReason.RPC_UNAVAILABLE, warning="No RPC")
        assert result.exists is None
        assert result.reason == PoolValidationReason.RPC_UNAVAILABLE
        assert result.warning == "No RPC"


class TestV3PoolValidation:
    """Test validate_v3_pool function."""

    def test_no_rpc_url_returns_none(self):
        result = validate_v3_pool("arbitrum", "uniswap_v3", "0xabc", "0xdef", 3000, None)
        assert result.exists is None
        assert result.reason == PoolValidationReason.RPC_UNAVAILABLE
        assert result.warning is not None
        assert "No RPC URL" in result.warning

    def test_unknown_protocol_returns_none(self):
        result = validate_v3_pool("arbitrum", "unknown_protocol", "0xabc", "0xdef", 3000, "http://localhost:8545")
        assert result.exists is None
        assert result.reason == PoolValidationReason.PROTOCOL_UNKNOWN
        assert result.warning is not None
        assert "Unknown protocol" in result.warning

    def test_unknown_chain_returns_none(self):
        result = validate_v3_pool("unknown_chain", "uniswap_v3", "0xabc", "0xdef", 3000, "http://localhost:8545")
        assert result.exists is None
        assert result.reason == PoolValidationReason.FACTORY_MISSING
        assert result.warning is not None
        assert "No uniswap_v3 factory" in result.warning

    @patch(V3_ETH_CALL)
    def test_zero_address_returns_false(self, mock_eth_call):
        """When factory returns zero address, pool doesn't exist."""
        mock_eth_call.return_value = bytes(32)  # 32 zero bytes = zero address
        result = validate_v3_pool(
            "arbitrum",
            "uniswap_v3",
            "0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
            "0xaf88d065e77c8cc2239327c5edb3a432268e5831",
            3000,
            "http://localhost:8545",
        )
        assert result.exists is False
        assert result.reason == PoolValidationReason.NOT_FOUND
        assert result.error is not None
        assert "No uniswap_v3 pool found" in result.error

    @patch(V3_ETH_CALL)
    def test_valid_address_returns_true(self, mock_eth_call):
        """When factory returns a valid address, pool exists."""
        # Encode a valid address in 32 bytes (12 zero bytes + 20 address bytes)
        pool_addr = bytes(12) + bytes.fromhex("C31E54c7a869B9FcBEcc14363CF510d1c41fa443")
        mock_eth_call.return_value = pool_addr
        result = validate_v3_pool(
            "arbitrum",
            "uniswap_v3",
            "0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
            "0xaf88d065e77c8cc2239327c5edb3a432268e5831",
            500,
            "http://localhost:8545",
        )
        assert result.exists is True
        assert result.reason == PoolValidationReason.CONFIRMED
        assert result.pool_address is not None
        assert "c31e54c7" in result.pool_address.lower()

    @patch(V3_ETH_CALL)
    def test_rpc_failure_returns_none(self, mock_eth_call):
        """When RPC call fails, return RPC_FAILED (compiler must fail closed on this)."""
        mock_eth_call.return_value = None
        result = validate_v3_pool(
            "arbitrum",
            "uniswap_v3",
            "0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
            "0xaf88d065e77c8cc2239327c5edb3a432268e5831",
            3000,
            "http://localhost:8545",
        )
        assert result.exists is None
        assert result.reason == PoolValidationReason.RPC_FAILED
        assert result.warning is not None
        assert "RPC call" in result.warning

    def test_gateway_eth_call_is_used_when_available(self):
        gateway_client = MagicMock()
        gateway_client.is_connected = True
        gateway_client.eth_call.return_value = "0x" + ("00" * 12) + "c31e54c7a869b9fcbecc14363cf510d1c41fa443"

        result = validate_v3_pool(
            "arbitrum",
            "uniswap_v3",
            "0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
            "0xaf88d065e77c8cc2239327c5edb3a432268e5831",
            500,
            None,
            gateway_client=gateway_client,
        )

        assert result.exists is True
        gateway_client.eth_call.assert_called_once()


class TestAerodromePoolValidation:
    """Test validate_aerodrome_pool function."""

    def test_no_rpc_url_returns_none(self):
        result = validate_aerodrome_pool("base", "0xabc", "0xdef", False, None)
        assert result.exists is None
        assert "No RPC URL" in result.warning

    def test_unsupported_chain_returns_none(self):
        result = validate_aerodrome_pool("arbitrum", "0xabc", "0xdef", False, "http://localhost:8545")
        assert result.exists is None
        assert "No Aerodrome factory" in result.warning

    @patch(AERODROME_ETH_CALL)
    def test_zero_address_returns_false(self, mock_eth_call):
        mock_eth_call.return_value = bytes(32)
        result = validate_aerodrome_pool(
            "base",
            "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
            "0x4200000000000000000000000000000000000006",
            False,
            "http://localhost:8545",
        )
        assert result.exists is False
        assert "volatile" in result.error

    @patch(AERODROME_ETH_CALL)
    def test_valid_address_returns_true(self, mock_eth_call):
        pool_addr = bytes(12) + bytes.fromhex("abcdef1234567890abcdef1234567890abcdef12")
        mock_eth_call.return_value = pool_addr
        result = validate_aerodrome_pool(
            "base",
            "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
            "0x4200000000000000000000000000000000000006",
            False,
            "http://localhost:8545",
        )
        assert result.exists is True
        assert result.pool_address is not None

        # Ensure selector is for getPool(address,address,bool) (not the V3 uint24 overload)
        _, _, calldata = mock_eth_call.call_args.args
        assert calldata.startswith("0x79bc57d5")


class TestTraderJoePoolValidation:
    """Test validate_traderjoe_pool function."""

    def test_no_rpc_url_returns_none(self):
        result = validate_traderjoe_pool("avalanche", "0xabc", "0xdef", 20, None)
        assert result.exists is None
        assert "No RPC URL" in result.warning

    def test_unsupported_chain_returns_none(self):
        result = validate_traderjoe_pool("polygon", "0xabc", "0xdef", 20, "http://localhost:8545")
        assert result.exists is None
        assert "No TraderJoe V2 factory" in result.warning

    @patch(TRADERJOE_ETH_CALL)
    def test_zero_address_returns_false(self, mock_eth_call):
        # getLBPairInformation returns 4 words: binStep, LBPair, createdByOwner, ignoredForRouting
        # Return 128 bytes with zero address in second word
        mock_eth_call.return_value = bytes(128)
        result = validate_traderjoe_pool(
            "avalanche",
            "0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7",
            "0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E",
            20,
            "http://localhost:8545",
        )
        assert result.exists is False
        assert "No TraderJoe V2 pool found" in result.error

        # Ensure selector matches getLBPairInformation(address,address,uint256)
        _, _, calldata = mock_eth_call.call_args.args
        assert calldata.startswith("0x704037bd")

    @patch(TRADERJOE_ETH_CALL)
    def test_valid_address_returns_true(self, mock_eth_call):
        # First word: binStep (20), second word: valid LBPair address
        first_word = (20).to_bytes(32, byteorder="big")
        pool_addr_word = bytes(12) + bytes.fromhex("abcdef1234567890abcdef1234567890abcdef12")
        third_word = bytes(32)  # createdByOwner
        fourth_word = bytes(32)  # ignoredForRouting
        factory_response = first_word + pool_addr_word + third_word + fourth_word
        # Second call is getReserves() — return non-zero reserves
        reserves_response = (1000).to_bytes(32, byteorder="big") + (2000).to_bytes(32, byteorder="big")
        mock_eth_call.side_effect = [factory_response, reserves_response]
        result = validate_traderjoe_pool(
            "avalanche",
            "0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7",
            "0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E",
            20,
            "http://localhost:8545",
        )
        assert result.exists is True
        assert result.pool_address is not None

        # Ensure first call selector matches getLBPairInformation(address,address,uint256)
        _, _, calldata = mock_eth_call.call_args_list[0].args
        assert calldata.startswith("0x704037bd")
        # Ensure second call is getReserves() on the discovered pool address
        _, reserves_to, reserves_calldata = mock_eth_call.call_args_list[1].args
        assert reserves_calldata == "0x0902f1ac"
        assert reserves_to.lower() == "0xabcdef1234567890abcdef1234567890abcdef12"

    @patch(TRADERJOE_ETH_CALL)
    def test_zero_liquidity_pool_returns_false(self, mock_eth_call):
        """Pool exists in factory but has zero reserves — should fail validation."""
        first_word = (1).to_bytes(32, byteorder="big")  # binStep=1
        pool_addr_word = bytes(12) + bytes.fromhex("abcdef1234567890abcdef1234567890abcdef12")
        third_word = bytes(32)
        fourth_word = bytes(32)
        factory_response = first_word + pool_addr_word + third_word + fourth_word
        # getReserves() returns zero reserves
        reserves_response = (0).to_bytes(32, byteorder="big") + (0).to_bytes(32, byteorder="big")
        mock_eth_call.side_effect = [factory_response, reserves_response]
        result = validate_traderjoe_pool(
            "avalanche",
            "0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7",
            "0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E",
            1,
            "http://localhost:8545",
        )
        assert result.exists is False
        assert "zero liquidity" in result.error

    @patch(TRADERJOE_ETH_CALL)
    def test_reserves_rpc_failure_still_passes(self, mock_eth_call):
        """If getReserves() RPC fails, pool should still pass (factory confirmed it exists)."""
        first_word = (20).to_bytes(32, byteorder="big")
        pool_addr_word = bytes(12) + bytes.fromhex("abcdef1234567890abcdef1234567890abcdef12")
        third_word = bytes(32)
        fourth_word = bytes(32)
        factory_response = first_word + pool_addr_word + third_word + fourth_word
        # getReserves() call fails (returns None)
        mock_eth_call.side_effect = [factory_response, None]
        result = validate_traderjoe_pool(
            "avalanche",
            "0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7",
            "0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E",
            20,
            "http://localhost:8545",
        )
        assert result.exists is True
        assert result.pool_address is not None


class TestEncodingHelpers:
    """Test calldata encoding helpers."""

    def test_encode_get_pool_v3(self):
        calldata = _encode_get_pool_v3(
            "0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
            "0xaf88d065e77c8cc2239327c5edb3a432268e5831",
            3000,
        )
        # Should start with getPool selector
        assert calldata.startswith("0x1698ee82")
        # Total length: selector (10) + 3 * 64 = 202 chars
        assert len(calldata) == 202

    def test_encode_get_pool_aerodrome_volatile(self):
        calldata = _encode_get_pool_aerodrome("0xabc", "0xdef", False)
        assert calldata.startswith("0x79bc57d5")
        # stable=False -> last 64 chars should be all zeros
        assert calldata.endswith("0" * 64)

    def test_encode_get_pool_aerodrome_stable(self):
        calldata = _encode_get_pool_aerodrome("0xabc", "0xdef", True)
        assert calldata.startswith("0x79bc57d5")
        # stable=True -> last 64 chars should end with 1
        assert calldata[-1] == "1"
        assert calldata[-64:-1] == "0" * 63

    def test_decode_address_valid(self):
        # 12 zero bytes + 20 address bytes
        data = bytes(12) + bytes.fromhex("1234567890abcdef1234567890abcdef12345678")
        addr = decode_address(data)
        assert addr == "0x1234567890abcdef1234567890abcdef12345678"

    def test_decode_address_zero(self):
        data = bytes(32)
        addr = decode_address(data)
        assert addr == ZERO_ADDRESS

    def test_decode_address_too_short(self):
        data = bytes(16)
        addr = decode_address(data)
        assert addr == ZERO_ADDRESS


class TestFetchV3PoolSqrtPriceX96:
    """Unit tests for fetch_v3_pool_sqrt_price_x96."""

    POOL_ADDR = "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"
    RPC_URL = "http://localhost:8545"

    def test_rpc_failure_returns_none(self):
        with patch(V3_ETH_CALL, return_value=None):
            result = fetch_v3_pool_sqrt_price_x96(self.POOL_ADDR, self.RPC_URL)
        assert result is None

    def test_response_too_short_returns_none(self):
        with patch(V3_ETH_CALL, return_value=bytes(32)):
            result = fetch_v3_pool_sqrt_price_x96(self.POOL_ADDR, self.RPC_URL)
        assert result is None

    def test_valid_response_decodes_sqrt_price(self):
        # Use a realistic sqrtPriceX96 (tick=0 -> Q96) and tick=42
        sqrt_price = 79228162514264337593543950336  # Q96 (tick=0)
        tick = 42
        raw = sqrt_price.to_bytes(32, "big") + tick.to_bytes(32, "big")
        with patch(V3_ETH_CALL, return_value=raw):
            result = fetch_v3_pool_sqrt_price_x96(self.POOL_ADDR, self.RPC_URL)
        assert result is not None
        assert result[0] == sqrt_price
        assert result[1] == tick

    def test_negative_tick_sign_extended(self):
        # Negative ticks are sign-extended to int256 in ABI encoding
        sqrt_price = 79228162514264337593543950336  # Q96 (tick=0)
        tick = -60
        # ABI int256 two's complement for -60: 2**256 - 60
        tick_encoded = (2**256 + tick).to_bytes(32, "big")
        raw = sqrt_price.to_bytes(32, "big") + tick_encoded
        with patch(V3_ETH_CALL, return_value=raw):
            result = fetch_v3_pool_sqrt_price_x96(self.POOL_ADDR, self.RPC_URL)
        assert result is not None
        assert result[0] == sqrt_price
        assert result[1] == tick

    def test_out_of_range_sqrt_price_returns_none(self):
        # sqrtPriceX96 below MIN_SQRT_RATIO should be rejected
        sqrt_price = 100  # way below MIN_SQRT_RATIO
        tick = 0
        raw = sqrt_price.to_bytes(32, "big") + tick.to_bytes(32, "big")
        with patch(V3_ETH_CALL, return_value=raw):
            result = fetch_v3_pool_sqrt_price_x96(self.POOL_ADDR, self.RPC_URL)
        assert result is None

    def test_out_of_range_tick_returns_none(self):
        # Tick beyond MAX_TICK should be rejected
        sqrt_price = 79228162514264337593543950336  # Q96
        tick = 900000  # beyond MAX_TICK
        raw = sqrt_price.to_bytes(32, "big") + tick.to_bytes(32, "big")
        with patch(V3_ETH_CALL, return_value=raw):
            result = fetch_v3_pool_sqrt_price_x96(self.POOL_ADDR, self.RPC_URL)
        assert result is None


def _slipstream_generation(factory: str) -> SlipstreamDeployment:
    deployment = slipstream_deployment_for_factory("base", factory)
    assert deployment is not None, factory
    return deployment


SLIPSTREAM_CURRENT = _slipstream_generation("0xf8f2eB4940CFE7d13603DDDD87f123820Fc061Ef")
SLIPSTREAM_LEGACY = _slipstream_generation("0x5e7BB104d84c7CB9B682AaC2F3d509f5F406809A")
POOL_A = "0xc31e54c7a869b9fcbecc14363cf510d1c41fa443"
POOL_B = "0xabcdef1234567890abcdef1234567890abcdef12"


def _address_word(address: str) -> bytes:
    return bytes(12) + bytes.fromhex(address[2:])


def _factory_answers(answers: dict[str, bytes | None]):
    """``eth_call`` stand-in keyed on the factory being asked; ``None`` models a failed read."""

    def _call(rpc_url, to, calldata, *args, **kwargs):
        return answers[to]

    return _call


class TestValidateAerodromeClPool:
    """Aerodrome Slipstream (CL) pool validation.

    Without an explicit ``deployment`` the key is asked of EVERY reviewed
    factory generation and confirmed only when exactly one owns a pool for it;
    with one, only that factory is asked. Chains without a reviewed generation
    resolve FACTORY_MISSING.
    """

    # Base WETH / USDC — only need to be 42-char hex for calldata encoding; the
    # on-chain call itself is mocked.
    WETH = "0x4200000000000000000000000000000000000006"
    USDC = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
    TICK_SPACING = 100
    RPC_URL = "http://localhost:8545"

    def test_factory_missing_on_unsupported_chain(self):
        """A chain with no reviewed Slipstream generation resolves FACTORY_MISSING."""
        result = validate_aerodrome_cl_pool("ethereum", self.WETH, self.USDC, self.TICK_SPACING, self.RPC_URL)
        assert result.exists is None
        assert result.reason == PoolValidationReason.FACTORY_MISSING
        assert result.warning is not None

    @patch(AERODROME_ETH_CALL)
    def test_valid_address_returns_true(self, mock_eth_call):
        """Exactly one reviewed factory answering non-zero confirms the pool and names that factory."""
        mock_eth_call.side_effect = _factory_answers(
            {SLIPSTREAM_CURRENT.factory: _address_word(POOL_A), SLIPSTREAM_LEGACY.factory: bytes(32)}
        )
        result = validate_aerodrome_cl_pool("base", self.WETH, self.USDC, self.TICK_SPACING, self.RPC_URL)
        assert result.exists is True
        assert result.reason == PoolValidationReason.CONFIRMED
        assert result.pool_address == POOL_A
        assert result.factory == SLIPSTREAM_CURRENT.factory
        # Every reviewed factory is asked, even after a hit.
        assert {call.args[1] for call in mock_eth_call.call_args_list} == {
            SLIPSTREAM_CURRENT.factory,
            SLIPSTREAM_LEGACY.factory,
        }
        assert all(call.args[2].startswith("0x28af8d0b") for call in mock_eth_call.call_args_list)

    @patch(AERODROME_ETH_CALL)
    def test_zero_address_returns_false(self, mock_eth_call):
        """Zero from every reviewed factory means the pool does not exist."""
        mock_eth_call.return_value = bytes(32)
        result = validate_aerodrome_cl_pool("base", self.WETH, self.USDC, self.TICK_SPACING, self.RPC_URL)
        assert result.exists is False
        assert result.reason == PoolValidationReason.NOT_FOUND
        assert result.factory is None
        assert "current" in result.error and "legacy" in result.error

    @patch(AERODROME_ETH_CALL)
    def test_two_generations_owning_the_key_is_ambiguous(self, mock_eth_call):
        """A key two generations answer is refused; tuple order never picks the winner."""
        mock_eth_call.side_effect = _factory_answers(
            {SLIPSTREAM_CURRENT.factory: _address_word(POOL_A), SLIPSTREAM_LEGACY.factory: _address_word(POOL_B)}
        )
        result = validate_aerodrome_cl_pool("base", self.WETH, self.USDC, self.TICK_SPACING, self.RPC_URL)
        assert result.exists is False
        assert result.reason == PoolValidationReason.AMBIGUOUS
        assert result.pool_address is None
        assert result.factory is None
        assert POOL_A in result.error and POOL_B in result.error
        assert "Name the pool address" in result.error

    @patch(AERODROME_ETH_CALL)
    def test_unreachable_generation_is_rpc_failed_even_with_a_hit(self, mock_eth_call):
        """A lone hit cannot prove uniqueness while another reviewed factory is unreadable."""
        mock_eth_call.side_effect = _factory_answers(
            {SLIPSTREAM_CURRENT.factory: _address_word(POOL_A), SLIPSTREAM_LEGACY.factory: None}
        )
        result = validate_aerodrome_cl_pool("base", self.WETH, self.USDC, self.TICK_SPACING, self.RPC_URL)
        assert result.exists is None
        assert result.reason == PoolValidationReason.RPC_FAILED
        assert result.pool_address is None
        assert "legacy" in result.warning

    @patch(AERODROME_ETH_CALL)
    def test_explicit_deployment_queries_only_that_factory(self, mock_eth_call):
        mock_eth_call.return_value = _address_word(POOL_B)
        result = validate_aerodrome_cl_pool(
            "base", self.WETH, self.USDC, self.TICK_SPACING, self.RPC_URL, deployment=SLIPSTREAM_LEGACY
        )
        assert result.exists is True
        assert result.pool_address == POOL_B
        assert result.factory == SLIPSTREAM_LEGACY.factory
        mock_eth_call.assert_called_once()
        assert mock_eth_call.call_args.args[1] == SLIPSTREAM_LEGACY.factory

    @patch(AERODROME_ETH_CALL)
    def test_explicit_deployment_zero_names_the_generation(self, mock_eth_call):
        mock_eth_call.return_value = bytes(32)
        result = validate_aerodrome_cl_pool(
            "base", self.WETH, self.USDC, self.TICK_SPACING, self.RPC_URL, deployment=SLIPSTREAM_CURRENT
        )
        assert result.exists is False
        assert result.reason == PoolValidationReason.NOT_FOUND
        assert SLIPSTREAM_CURRENT.factory in result.error

    @patch(AERODROME_ETH_CALL)
    def test_explicit_deployment_read_failure_is_rpc_failed(self, mock_eth_call):
        mock_eth_call.return_value = None
        result = validate_aerodrome_cl_pool(
            "base", self.WETH, self.USDC, self.TICK_SPACING, self.RPC_URL, deployment=SLIPSTREAM_CURRENT
        )
        assert result.exists is None
        assert result.reason == PoolValidationReason.RPC_FAILED

    @patch(AERODROME_ETH_CALL)
    def test_unreviewed_deployment_is_rejected_before_any_read(self, mock_eth_call):
        injected = SlipstreamDeployment(factory="0x" + "aa" * 20, position_manager="0x" + "bb" * 20, generation="x")
        with pytest.raises(ValueError, match="unreviewed Slipstream deployment"):
            validate_aerodrome_cl_pool(
                "base", self.WETH, self.USDC, self.TICK_SPACING, self.RPC_URL, deployment=injected
            )
        mock_eth_call.assert_not_called()


class TestResolveSlipstreamPoolKey:
    """The cross-generation scan behind the deployment-less validator."""

    WETH = "0x4200000000000000000000000000000000000006"
    USDC = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
    RPC_URL = "http://localhost:8545"

    @patch(AERODROME_ETH_CALL)
    def test_scans_every_reviewed_factory_and_reports_the_unique_owner(self, mock_eth_call):
        mock_eth_call.side_effect = _factory_answers(
            {SLIPSTREAM_CURRENT.factory: bytes(32), SLIPSTREAM_LEGACY.factory: _address_word(POOL_B)}
        )
        resolution = resolve_slipstream_pool_key("base", self.WETH, self.USDC, 100, self.RPC_URL)
        assert [call.args[1] for call in mock_eth_call.call_args_list] == [d.factory for d in resolution.reviewed]
        assert resolution.unreachable == ()
        assert [(m.deployment, m.pool_address) for m in resolution.matches] == [(SLIPSTREAM_LEGACY, POOL_B)]
        assert resolution.unique is not None
        assert resolution.unique.deployment == SLIPSTREAM_LEGACY
        assert resolution.validation_result().factory == SLIPSTREAM_LEGACY.factory

    @patch(AERODROME_ETH_CALL)
    def test_ambiguous_and_unreachable_scans_have_no_unique_owner(self, mock_eth_call):
        mock_eth_call.side_effect = _factory_answers(
            {SLIPSTREAM_CURRENT.factory: _address_word(POOL_A), SLIPSTREAM_LEGACY.factory: _address_word(POOL_B)}
        )
        ambiguous = resolve_slipstream_pool_key("base", self.WETH, self.USDC, 100, self.RPC_URL)
        assert len(ambiguous.matches) == 2
        assert ambiguous.unique is None
        assert ambiguous.validation_result().reason == PoolValidationReason.AMBIGUOUS

        mock_eth_call.side_effect = _factory_answers(
            {SLIPSTREAM_CURRENT.factory: _address_word(POOL_A), SLIPSTREAM_LEGACY.factory: None}
        )
        unverified = resolve_slipstream_pool_key("base", self.WETH, self.USDC, 100, self.RPC_URL)
        assert unverified.unreachable == (SLIPSTREAM_LEGACY,)
        assert unverified.unique is None
        assert unverified.validation_result().reason == PoolValidationReason.RPC_FAILED

    @patch(AERODROME_ETH_CALL)
    def test_no_transport_marks_every_generation_unreachable_without_reading(self, mock_eth_call):
        resolution = resolve_slipstream_pool_key("base", self.WETH, self.USDC, 100, None)
        mock_eth_call.assert_not_called()
        assert resolution.matches == ()
        assert set(resolution.unreachable) == set(resolution.reviewed)
        assert resolution.unique is None

    def test_unsupported_chain_has_nothing_to_scan(self):
        resolution = resolve_slipstream_pool_key("ethereum", self.WETH, self.USDC, 100, self.RPC_URL)
        assert resolution.reviewed == ()
        assert resolution.validation_result().reason == PoolValidationReason.FACTORY_MISSING


class TestPoolValidationRegistry:
    """Dispatch registry routes each protocol to its owning connector validator."""

    WETH = "0x4200000000000000000000000000000000000006"
    USDC = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
    RPC_URL = "http://localhost:8545"

    def test_has_known_protocols(self):
        # V3 forks resolved via AbiFamily.V3_FACTORY (no hardcoded fork list).
        assert PoolValidationRegistry.has("uniswap_v3")
        assert PoolValidationRegistry.has("sushiswap_v3")
        assert PoolValidationRegistry.has("pancakeswap_v3")
        assert PoolValidationRegistry.has("aerodrome")
        assert PoolValidationRegistry.has("aerodrome_slipstream")
        assert PoolValidationRegistry.has("traderjoe_v2")

    def test_has_unknown_protocol(self):
        assert PoolValidationRegistry.has("definitely_not_a_dex") is False

    def test_unknown_protocol_returns_protocol_unknown(self):
        result = PoolValidationRegistry.validate("definitely_not_a_dex", "base", self.WETH, self.USDC, {}, self.RPC_URL)
        assert result.exists is None
        assert result.reason == PoolValidationReason.PROTOCOL_UNKNOWN

    @patch(V3_ETH_CALL)
    def test_dispatch_v3(self, mock_eth_call):
        """V3 dispatch routes to the uniswap_v3 validator (selector 0x1698ee82)."""
        mock_eth_call.return_value = bytes(12) + bytes.fromhex("C31E54c7a869B9FcBEcc14363CF510d1c41fa443")
        result = PoolValidationRegistry.validate(
            "uniswap_v3",
            "arbitrum",
            "0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
            "0xaf88d065e77c8cc2239327c5edb3a432268e5831",
            {"fee_tier": 500},
            self.RPC_URL,
        )
        assert result.exists is True
        _, _, calldata = mock_eth_call.call_args.args
        assert calldata.startswith("0x1698ee82")

    @patch(AERODROME_ETH_CALL)
    def test_dispatch_aerodrome_classic(self, mock_eth_call):
        """Aerodrome Classic dispatch uses getPool(address,address,bool)."""
        mock_eth_call.return_value = bytes(12) + bytes.fromhex("abcdef1234567890abcdef1234567890abcdef12")
        result = PoolValidationRegistry.validate(
            "aerodrome", "base", self.USDC, self.WETH, {"stable": False}, self.RPC_URL
        )
        assert result.exists is True
        _, _, calldata = mock_eth_call.call_args.args
        assert calldata.startswith("0x79bc57d5")

    @patch(AERODROME_ETH_CALL)
    def test_dispatch_aerodrome_slipstream(self, mock_eth_call):
        """Slipstream dispatch uses the CL getPool(address,address,int24) on every reviewed factory."""
        mock_eth_call.side_effect = _factory_answers(
            {SLIPSTREAM_CURRENT.factory: _address_word(POOL_B), SLIPSTREAM_LEGACY.factory: bytes(32)}
        )
        result = PoolValidationRegistry.validate(
            "aerodrome_slipstream", "base", self.USDC, self.WETH, {"tick_spacing": 100}, self.RPC_URL
        )
        assert result.exists is True
        assert result.factory == SLIPSTREAM_CURRENT.factory
        assert mock_eth_call.call_count == 2
        _, _, calldata = mock_eth_call.call_args.args
        assert calldata.startswith("0x28af8d0b")

    @patch(TRADERJOE_ETH_CALL)
    def test_dispatch_traderjoe(self, mock_eth_call):
        """TraderJoe dispatch uses getLBPairInformation and passes allow_empty_reserves."""
        first_word = (20).to_bytes(32, byteorder="big")
        pool_addr_word = bytes(12) + bytes.fromhex("abcdef1234567890abcdef1234567890abcdef12")
        factory_response = first_word + pool_addr_word + bytes(32) + bytes(32)
        mock_eth_call.return_value = factory_response
        result = PoolValidationRegistry.validate(
            "traderjoe_v2",
            "avalanche",
            "0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7",
            "0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E",
            {"bin_step": 20, "allow_empty_reserves": True},
            self.RPC_URL,
        )
        assert result.exists is True
        # allow_empty_reserves=True -> only the factory call happens, no getReserves().
        assert mock_eth_call.call_count == 1
        _, _, calldata = mock_eth_call.call_args.args
        assert calldata.startswith("0x704037bd")

    @patch(V3_ETH_CALL)
    def test_module_level_validate_pool_wrapper(self, mock_eth_call):
        """The module-level convenience wrapper delegates to the registry."""
        mock_eth_call.return_value = bytes(32)
        result = validate_pool(
            "uniswap_v3",
            "arbitrum",
            "0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
            "0xaf88d065e77c8cc2239327c5edb3a432268e5831",
            {"fee_tier": 3000},
            self.RPC_URL,
        )
        assert result.exists is False
        assert result.reason == PoolValidationReason.NOT_FOUND


class TestReaderSelectorReExport:
    """Pin the ``reader.GET_POOL_SELECTOR`` compatibility re-export.

    Ownership of the ``getPool(address,address,uint24)`` selector moved to the
    Uniswap V3 connector, but ``almanak.framework.data.pools.reader`` re-exports
    it so the historical import path keeps working for downstream callers. A
    later refactor that drops or renames the re-export would break those callers
    silently — this test makes that break loud.
    """

    def test_reader_get_pool_selector_matches_connector_source(self):
        from almanak.connectors.uniswap_v3.pool_validation import V3_GET_POOL_SELECTOR
        from almanak.framework.data.pools.reader import GET_POOL_SELECTOR

        assert GET_POOL_SELECTOR == V3_GET_POOL_SELECTOR


class TestDashboardV3PoolDelegation:
    """The dashboard API client delegates V3 pool resolution to the registry.

    ``DashboardAPIClient.get_v3_pool_address`` must (1) pass ``fee_tier`` to the
    registry inside a ``{"fee_tier": ...}`` dict and (2) return ``None`` when the
    registry reports the pool does not exist. Both are easy to break during a
    future registry refactor, so pin the contract here.
    """

    WETH = "0x4200000000000000000000000000000000000006"
    USDC = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
    VALIDATE_TARGET = "almanak.connectors._strategy_base.pool_validation_registry.PoolValidationRegistry.validate"

    def _make_client(self):
        from almanak.framework.dashboard.custom.api_client import DashboardAPIClient

        gateway_client = MagicMock()
        return DashboardAPIClient(gateway_client=gateway_client, deployment_id="Strat:test"), gateway_client

    def test_passes_fee_tier_dict_and_returns_address_when_pool_exists(self):
        client, gateway_client = self._make_client()
        with patch(self.VALIDATE_TARGET) as mock_validate:
            mock_validate.return_value = PoolValidationResult(
                exists=True, reason=PoolValidationReason.CONFIRMED, pool_address="0xPOOL"
            )
            result = client.get_v3_pool_address(
                chain="base",
                protocol="uniswap_v3",
                token0_address=self.WETH,
                token1_address=self.USDC,
                fee_tier=500,
            )

        assert result == "0xPOOL"
        # protocol, chain, token0, token1, params dict are positional args.
        call_args = mock_validate.call_args.args
        params = call_args[4]
        assert params == {"fee_tier": 500}
        # The gateway is threaded through so the registry can use the gRPC path.
        assert mock_validate.call_args.kwargs["gateway_client"] is gateway_client._client

    def test_returns_none_when_pool_does_not_exist(self):
        client, _ = self._make_client()
        with patch(self.VALIDATE_TARGET) as mock_validate:
            mock_validate.return_value = PoolValidationResult(
                exists=False, reason=PoolValidationReason.NOT_FOUND, pool_address="0xPOOL"
            )
            result = client.get_v3_pool_address(
                chain="base",
                protocol="uniswap_v3",
                token0_address=self.WETH,
                token1_address=self.USDC,
                fee_tier=500,
            )

        assert result is None
