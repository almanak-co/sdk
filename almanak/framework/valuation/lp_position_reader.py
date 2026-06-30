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

from almanak.connectors._strategy_base.address_registry import AddressRegistry
from almanak.connectors._strategy_base.contract_role_registry import (
    CONTRACT_ROLE_REGISTRY,
    ContractRole,
)
from almanak.core.chains import ChainRegistry

logger = logging.getLogger(__name__)

# Position-manager addresses are PROTOCOL facts owned by each connector's
# address tables and resolved through ``AddressRegistry`` (the Phase D5
# precedent — VIB-4851 CS-5). The two literal dicts that previously lived
# here had drifted from the connector tables on THREE money-path entries,
# all verified empty on-chain (eth_getCode == "0x") on 2026-06-11:
#   sushiswap_v3/arbitrum  0xf0cb...3EF99 (real NPM: 0xF0cB...eF49)
#   uniswap_v3/avalanche   0xC364...FE88  (real NPM: 0x655C...4f8B)
#   uniswap_v3/bsc         0xC364...FE88  (real NPM: 0x7b8A...0613)
# Reading an empty address returned no data, silently degrading LP
# valuation on those (protocol, chain) pairs. The registry values are the
# deployment-verified connector tables.

# Function selectors
POSITIONS_SELECTOR = "0x99fbab88"  # positions(uint256)
SLOT0_SELECTOR = "0x3850c7bd"  # slot0()
ERC20_BALANCE_OF_SELECTOR = "0x70a08231"  # balanceOf(address)

# A uint256 return value is the FIRST 32-byte (64-hex) ABI word; trailing bytes
# must be ignored. Some old Vyper contracts (e.g. the FRAX/3CRV metapool, which
# is its own integrated-ERC20 LP token) return MORE than 32 bytes from a uint256
# getter — the real value is in word 0 and the tail is leftover memory. Decoding
# ``int(whole_hex, 16)`` would read megabyte-wide garbage; every ABI decoder
# (cast, web3) takes word 0. VIB-5428.
_UINT256_WORD_HEX_LEN = 64


def _decode_uint256_word(result_hex: str | None) -> int | None:
    """Decode a uint256 from an eth_call result's FIRST 32-byte word.

    Returns ``None`` (Empty ≠ Zero) on an empty / unparseable result. A normal
    32-byte return is unchanged; an over-long return (extra trailing bytes) is
    correctly truncated to word 0 rather than int-parsed whole.
    """
    if not result_hex:
        return None
    body = result_hex[2:] if result_hex.startswith(("0x", "0X")) else result_hex
    if not body:
        return None
    word = body[:_UINT256_WORD_HEX_LEN] if len(body) >= _UINT256_WORD_HEX_LEN else body
    try:
        return int(word, 16)
    except (ValueError, TypeError):
        return None


@dataclass
class LPPositionOnChain:
    """Full on-chain state of a V3 (or Slipstream CL) LP position.

    Decoded from NonfungiblePositionManager.positions(tokenId).

    Uniswap-V3-family and Aerodrome/Velodrome **Slipstream** CL positions
    share the same selector (``positions(uint256)``) and 12-word return
    layout for every field this valuer uses (token0/1, tick range, liquidity,
    uncollected fees). They differ at exactly one word: V3 word [4] is the
    ``fee`` tier (bps), whereas Slipstream word [4] is ``tickSpacing`` (the CL
    NPM has no per-position ``fee`` field). ``fee`` is therefore ``None`` for
    Slipstream (unmeasured — Empty ≠ Zero; the CL NPM does not report it) and
    ``tick_spacing`` carries the CL value. The valuation math
    (``value_lp_position``) consumes neither field, so both paths value
    identically; the split exists only so each field carries truthful
    provenance.
    """

    token_id: int
    token0: str  # Token0 contract address
    token1: str  # Token1 contract address
    fee: int | None  # V3 fee tier (100/500/3000/10000); None for Slipstream CL
    tick_lower: int
    tick_upper: int
    liquidity: int
    tokens_owed0: int  # Uncollected fees in token0 (wei)
    tokens_owed1: int  # Uncollected fees in token1 (wei)
    tick_spacing: int | None = None  # Slipstream CL tick spacing; None for V3

    @property
    def is_active(self) -> bool:
        """Position has liquidity."""
        return self.liquidity > 0

    @property
    def fee_tier_percent(self) -> Decimal | None:
        """Fee tier as a percentage (e.g., 0.3 for 3000).

        ``None`` when the underlying NPM does not report a ``fee`` field
        (Slipstream CL keys liquidity on ``tick_spacing`` instead).
        """
        if self.fee is None:
            return None
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

        return _parse_position_hex(result_hex, token_id, slipstream=_is_slipstream_protocol(protocol))

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

    def read_erc20_balance(
        self,
        chain: str,
        token_address: str,
        wallet_address: str,
    ) -> int | None:
        """Read an ERC-20 ``balanceOf(wallet)`` live via the gateway eth_call.

        Generic, decode-trivial counterpart to :meth:`read_position`: encodes the
        standard ``balanceOf(address)`` selector client-side (exactly as
        ``positions(uint256)`` is encoded) and routes the read through the same
        gateway-boundary-correct ``_eth_call`` primitive. Used by the Curve LP
        valuation path (VIB-5420) to read the LP-token balance for the wallet.

        Returns the balance in wei, or ``None`` on any failure (Empty ≠ Zero — a
        miss is unmeasured, never a fabricated zero). A genuine zero balance
        returns the measured ``0``.
        """
        if self._gateway is None:
            return None
        if not token_address or not wallet_address:
            return None
        wallet_hex = wallet_address.lower().removeprefix("0x").zfill(64)
        calldata = ERC20_BALANCE_OF_SELECTOR + wallet_hex
        result_hex = self._eth_call(chain, token_address, calldata)
        return _decode_uint256_word(result_hex)

    def read_uint256_call(
        self,
        chain: str,
        contract_address: str,
        selector: str,
    ) -> int | None:
        """Read a zero-arg ``uint256`` getter live via the gateway eth_call.

        Generic counterpart used by the Curve LP valuation path (VIB-5420) to read
        a pool's live ``get_virtual_price()`` / ``virtual_price()`` (1e18-scaled).
        ``selector`` is the 4-byte function selector (``"0x...."``).

        Returns the decoded ``uint256``, or ``None`` on any failure (Empty ≠ Zero —
        an unreadable getter is unmeasured, never a fabricated zero).
        """
        if self._gateway is None:
            return None
        if not contract_address or not selector:
            return None
        result_hex = self._eth_call(chain, contract_address, selector)
        return _decode_uint256_word(result_hex)

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
        """Resolve the NonfungiblePositionManager address for a protocol/chain.

        Resolution order (connector-owned throughout — never a hardcoded
        address):

        1. **Contract-role ``CL_POSITION_MANAGER``** (VIB-5141). The
           ``CONTRACT_ROLE_REGISTRY`` knows each connector's semantic-role →
           private-kind map *and* its ``address_protocol`` alias, so it
           resolves slugs that ``AddressRegistry`` cannot see directly — the
           ``aerodrome_slipstream`` pseudo-slug's ``CL_POSITION_MANAGER`` role
           maps to the ``cl_nft`` kind on the ``aerodrome`` table. Only the CL
           role is consulted here: it is the genuine V3-NFT-shaped
           ``positions(uint256)`` manager. ``LP_POSITION_MANAGER`` is
           deliberately NOT followed — for Aerodrome / TraderJoe V2 it maps to
           the *fungible*-LP router (a swap router / LBRouter), which is not a
           ``positions(uint256)`` NFT manager and must not be fed to this
           V3-shaped reader.
        2. **Legacy AddressRegistry kinds** (``position_manager`` / ``nft`` —
           PancakeSwap records its NPM under ``nft``) for V3-family slugs that
           predate the role registry.
        3. **Uniswap V3 fallback** — unknown protocols share the V3 interface.

        The chain is alias-normalized first (legacy dicts were keyed ``"bnb"``
        while callers / connector tables use canonical ``"bsc"``). Misses stay
        ``None`` (fail-closed), so a failed resolution degrades valuation to
        UNAVAILABLE rather than fabricating a value.
        """
        descriptor = ChainRegistry.try_resolve(chain)
        canonical = descriptor.name if descriptor is not None else chain.lower()

        role_addr = self._resolve_npm_by_role(protocol, canonical)
        if role_addr:
            return role_addr

        kinds = ("position_manager", "nft")
        addr = AddressRegistry.resolve_contract_address(protocol, canonical, kinds)
        if addr:
            return addr
        return AddressRegistry.resolve_contract_address("uniswap_v3", canonical, kinds)

    @staticmethod
    def _resolve_npm_by_role(protocol: str, canonical_chain: str) -> str | None:
        """Resolve the CL NonfungiblePositionManager via the contract-role registry.

        Consults only the ``CL_POSITION_MANAGER`` role — the genuine
        V3-NFT-shaped ``positions(uint256)`` manager for a concentrated-liquidity
        slug (``aerodrome_slipstream`` → ``cl_nft`` on the ``aerodrome`` table).
        Resolves against the slug's ``address_protocol`` so a pseudo-slug riding
        another connector's table is found. ``None`` when the slug declares no CL
        role (V3-family slugs fall through to the legacy ``AddressRegistry``
        lookup, byte-for-byte unchanged) or none is present on the chain.
        """
        kinds = CONTRACT_ROLE_REGISTRY.kinds_for(protocol, ContractRole.CL_POSITION_MANAGER)
        if not kinds:
            return None
        address_protocol = CONTRACT_ROLE_REGISTRY.address_protocol(protocol)
        return AddressRegistry.resolve_contract_address(address_protocol, canonical_chain, kinds)


# ---------------------------------------------------------------------------
# Hex response parsing
# ---------------------------------------------------------------------------


def _is_slipstream_protocol(protocol: str) -> bool:
    """Whether ``protocol`` uses the Slipstream CL NPM positions() layout.

    Connector-owned, never a hardcoded slug: a protocol uses the Slipstream
    layout iff it declares the ``CL_POSITION_MANAGER`` contract role (only
    ``aerodrome_slipstream`` today). This is the same registry signal the
    resolver uses to find the CL NPM address, so the address lookup and the
    struct-layout choice cannot drift apart.
    """
    return bool(CONTRACT_ROLE_REGISTRY.kinds_for(protocol, ContractRole.CL_POSITION_MANAGER))


def _parse_position_hex(hex_result: str, token_id: int, *, slipstream: bool = False) -> LPPositionOnChain | None:
    """Parse hex response from positions(uint256) call.

    Both Uniswap-V3-family and Aerodrome/Velodrome **Slipstream** CL NPMs
    return a 12-word struct (32 bytes / 64 hex chars each) with identical
    layout for every field this valuer consumes:
    ``[0] nonce, [1] operator, [2] token0, [3] token1, [4] fee|tickSpacing,
    [5] tickLower, [6] tickUpper, [7] liquidity, [8] feeGrowthInside0LastX128,
    [9] feeGrowthInside1LastX128, [10] tokensOwed0, [11] tokensOwed1``.

    The single divergence is word [4]: Uniswap V3 encodes the ``fee`` tier
    (uint24 bps) there, whereas Slipstream CL encodes ``tickSpacing`` (int24)
    — its NPM has no per-position ``fee`` field. When ``slipstream`` is set we
    decode word [4] as ``tick_spacing`` (signed int24, matching the ABI) and
    leave ``fee`` ``None`` (unmeasured — Empty ≠ Zero; the CL NPM does not
    report a fee). Both layouts feed the same valuation math, which reads
    neither field. Slipstream layout confirmed against the Aerodrome connector
    ABI ``almanak/connectors/aerodrome/abis/cl_nft.json`` (VIB-5141).
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
            fee=None if slipstream else _decode_uint_hex(hex_data, 4),
            tick_lower=_decode_int24_hex(hex_data, 5),
            tick_upper=_decode_int24_hex(hex_data, 6),
            liquidity=_decode_uint_hex(hex_data, 7),
            tokens_owed0=_decode_uint_hex(hex_data, 10),
            tokens_owed1=_decode_uint_hex(hex_data, 11),
            tick_spacing=_decode_int24_hex(hex_data, 4) if slipstream else None,
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
