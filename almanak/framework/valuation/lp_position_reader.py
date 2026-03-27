"""On-chain LP position reader via gateway RPC.

Queries Uniswap V3 NonfungiblePositionManager to get full position details
(tick range, liquidity, tokens, fees) and pool slot0 for current tick.

Uses the gateway's generic Call RPC — no proto changes needed.
Reuses data types from almanak.framework.backtesting.paper.position_queries.
"""

import json
import logging
from dataclasses import dataclass
from decimal import Decimal

logger = logging.getLogger(__name__)

# Uniswap V3 NonfungiblePositionManager addresses per chain
# Same address on most chains (deployed via CREATE2)
POSITION_MANAGER_ADDRESSES: dict[str, str] = {
    "ethereum": "0xC36442b4a4522E871399CD717aBDD847Ab11FE88",
    "arbitrum": "0xC36442b4a4522E871399CD717aBDD847Ab11FE88",
    "optimism": "0xC36442b4a4522E871399CD717aBDD847Ab11FE88",
    "polygon": "0xC36442b4a4522E871399CD717aBDD847Ab11FE88",
    "base": "0x03a520b32C04BF3bEEf7BEb72E919cf822Ed34f1",
    "avalanche": "0xC36442b4a4522E871399CD717aBDD847Ab11FE88",
    "bnb": "0xC36442b4a4522E871399CD717aBDD847Ab11FE88",
}

# Protocol-specific position managers (forks with different addresses)
PROTOCOL_POSITION_MANAGERS: dict[str, dict[str, str]] = {
    "sushiswap_v3": {
        "ethereum": "0x2214A42d8e2A1d20635c2cb0664422c528B6A432",
        "arbitrum": "0xf0cbCe1942A68BEb3d1B73f0Dd86C8Dcc643EF99",
        "optimism": "0x1af415a1EbA07a4986a52B6f2e7dE7003D82231e",
        "polygon": "0xb7402ee99F0A008e461098AC3A27F4957Df89a40",
        "base": "0x80C7DD17B01855a6D2347444a0FCC36136a314de",
        "avalanche": "0x18350b048AB366ed601fFDbC669110Ecb36016f3",
        "bnb": "0xF70c086618dcf2b1A461311275e00D6B722ef914",
    },
    "pancakeswap_v3": {
        "ethereum": "0x46A15B0b27311cedF172AB29E4f4766fbE7F4364",
        "arbitrum": "0x46A15B0b27311cedF172AB29E4f4766fbE7F4364",
        "base": "0x46A15B0b27311cedF172AB29E4f4766fbE7F4364",
        "bnb": "0x46A15B0b27311cedF172AB29E4f4766fbE7F4364",
    },
}

# Function selectors
POSITIONS_SELECTOR = "0x99fbab88"  # positions(uint256)
SLOT0_SELECTOR = "0x3850c7bd"  # slot0()


@dataclass
class LPPositionOnChain:
    """Full on-chain state of a V3 LP position.

    Decoded from NonfungiblePositionManager.positions(tokenId).
    """

    token_id: int
    token0: str  # Token0 contract address
    token1: str  # Token1 contract address
    fee: int  # Fee tier (100, 500, 3000, 10000)
    tick_lower: int
    tick_upper: int
    liquidity: int
    tokens_owed0: int  # Uncollected fees in token0 (wei)
    tokens_owed1: int  # Uncollected fees in token1 (wei)

    @property
    def is_active(self) -> bool:
        """Position has liquidity."""
        return self.liquidity > 0

    @property
    def fee_tier_percent(self) -> Decimal:
        """Fee tier as a percentage (e.g., 0.3 for 3000)."""
        return Decimal(self.fee) / Decimal("10000")


@dataclass
class PoolSlot0:
    """Current pool state from slot0().

    Only the fields needed for valuation.
    """

    sqrt_price_x96: int
    tick: int


class LPPositionReader:
    """Reads V3 LP position data via gateway RPC.

    Uses the gateway's generic Call RPC to make eth_call to on-chain
    contracts and decodes the response client-side.
    """

    def __init__(self, gateway_client: object | None = None) -> None:
        """Initialize with optional gateway client.

        Args:
            gateway_client: GatewayClient instance. If None, on-chain
                queries will return None (graceful degradation).
        """
        self._gateway = gateway_client

    def read_position(
        self,
        chain: str,
        token_id: int,
        protocol: str = "uniswap_v3",
        position_manager: str | None = None,
    ) -> LPPositionOnChain | None:
        """Query full position details from NonfungiblePositionManager.

        Args:
            chain: Chain identifier (e.g., "arbitrum", "base")
            token_id: Position NFT token ID
            protocol: Protocol name for address lookup
            position_manager: Override position manager address

        Returns:
            LPPositionOnChain with full position data, or None on failure
        """
        if self._gateway is None:
            return None

        pm_address = position_manager or self._resolve_position_manager(chain, protocol)
        if not pm_address:
            logger.debug("No position manager address for %s on %s", protocol, chain)
            return None

        # Build eth_call: positions(uint256 tokenId)
        token_id_hex = hex(token_id)[2:].zfill(64)
        calldata = POSITIONS_SELECTOR + token_id_hex

        result_hex = self._eth_call(chain, pm_address, calldata)
        if not result_hex:
            return None

        return _parse_position_hex(result_hex, token_id)

    def read_pool_slot0(
        self,
        chain: str,
        pool_address: str,
    ) -> PoolSlot0 | None:
        """Query pool slot0 for current sqrtPriceX96 and tick.

        Args:
            chain: Chain identifier
            pool_address: Uniswap V3 pool contract address

        Returns:
            PoolSlot0 with current price data, or None on failure
        """
        if self._gateway is None:
            return None

        result_hex = self._eth_call(chain, pool_address, SLOT0_SELECTOR)
        if not result_hex:
            return None

        return _parse_slot0_hex(result_hex)

    def _eth_call(self, chain: str, to: str, data: str) -> str | None:
        """Make an eth_call via gateway generic RPC."""
        try:
            from almanak.gateway.proto import gateway_pb2

            rpc_stub = getattr(self._gateway, "_rpc_stub", None)
            if rpc_stub is None:
                logger.debug("Gateway client not connected for LP position query")
                return None

            timeout = getattr(getattr(self._gateway, "config", None), "timeout", 10)

            params_json = json.dumps([{"to": to, "data": data}, "latest"])
            response = rpc_stub.Call(
                gateway_pb2.RpcRequest(
                    chain=chain,
                    method="eth_call",
                    params=params_json,
                ),
                timeout=timeout,
            )

            if not response.success:
                logger.debug("eth_call failed for LP position: %s", response.error)
                return None

            if response.result:
                return json.loads(response.result)
            return None
        except Exception:
            logger.debug("Failed to make eth_call for LP position", exc_info=True)
            return None

    def _resolve_position_manager(self, chain: str, protocol: str) -> str | None:
        """Resolve the position manager address for a protocol/chain."""
        # Check protocol-specific addresses first
        if protocol in PROTOCOL_POSITION_MANAGERS:
            addr = PROTOCOL_POSITION_MANAGERS[protocol].get(chain)
            if addr:
                return addr

        # Fall back to Uniswap V3 addresses (most forks use the same interface)
        return POSITION_MANAGER_ADDRESSES.get(chain)


# ---------------------------------------------------------------------------
# Hex response parsing
# ---------------------------------------------------------------------------


def _parse_position_hex(hex_result: str, token_id: int) -> LPPositionOnChain | None:
    """Parse hex response from positions(uint256) call.

    The return struct has 12 words (32 bytes each = 64 hex chars):
    [0] nonce, [1] operator, [2] token0, [3] token1, [4] fee,
    [5] tickLower, [6] tickUpper, [7] liquidity,
    [8] feeGrowthInside0LastX128, [9] feeGrowthInside1LastX128,
    [10] tokensOwed0, [11] tokensOwed1
    """
    hex_data = hex_result[2:] if hex_result.startswith("0x") else hex_result

    # Need at least 12 words * 64 hex chars = 768 hex chars
    if len(hex_data) < 768:
        logger.debug("Position response too short: %d hex chars", len(hex_data))
        return None

    try:
        return LPPositionOnChain(
            token_id=token_id,
            token0=_decode_address_hex(hex_data, 2),
            token1=_decode_address_hex(hex_data, 3),
            fee=_decode_uint_hex(hex_data, 4),
            tick_lower=_decode_int24_hex(hex_data, 5),
            tick_upper=_decode_int24_hex(hex_data, 6),
            liquidity=_decode_uint_hex(hex_data, 7),
            tokens_owed0=_decode_uint_hex(hex_data, 10),
            tokens_owed1=_decode_uint_hex(hex_data, 11),
        )
    except Exception:
        logger.debug("Failed to parse position #%d hex data", token_id, exc_info=True)
        return None


def _parse_slot0_hex(hex_result: str) -> PoolSlot0 | None:
    """Parse hex response from slot0() call.

    slot0() returns: sqrtPriceX96, tick, observationIndex,
    observationCardinality, observationCardinalityNext, feeProtocol, unlocked
    """
    hex_data = hex_result[2:] if hex_result.startswith("0x") else hex_result

    # Need at least 2 words (sqrtPriceX96 + tick)
    if len(hex_data) < 128:
        logger.debug("slot0 response too short: %d hex chars", len(hex_data))
        return None

    try:
        sqrt_price_x96 = _decode_uint_hex(hex_data, 0)
        tick = _decode_int24_hex(hex_data, 1)
        return PoolSlot0(sqrt_price_x96=sqrt_price_x96, tick=tick)
    except Exception:
        logger.debug("Failed to parse slot0 hex data", exc_info=True)
        return None


def _decode_uint_hex(hex_data: str, word_index: int) -> int:
    """Decode a uint256 from hex string at given word index."""
    start = word_index * 64
    return int(hex_data[start : start + 64], 16)


def _decode_address_hex(hex_data: str, word_index: int) -> str:
    """Decode an address from hex string at given word index."""
    start = word_index * 64
    # Address is the last 20 bytes (40 hex chars) of the 32-byte word
    return "0x" + hex_data[start + 24 : start + 64]


def _decode_int24_hex(hex_data: str, word_index: int) -> int:
    """Decode an int24 (tick) from hex string at given word index.

    Solidity ABI encodes int24 as sign-extended int256 (32 bytes).
    Negative values are stored as 256-bit two's complement.
    E.g., -100 is encoded as 0xFFFFFF...FF9C.
    """
    value = _decode_uint_hex(hex_data, word_index)
    # ABI sign extension: int24 is sign-extended to int256
    if value >= 2**255:
        value = value - 2**256
    return value
