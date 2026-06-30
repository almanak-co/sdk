"""Curve Finance Receipt Parser (Refactored).

Refactored to use base infrastructure utilities while maintaining backward compatibility.
Uses int128 for token indices and handles 2-pool and 3-pool variants.
"""

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from typing import TYPE_CHECKING, Any

from almanak.connectors._strategy_base.base import EventRegistry, HexDecoder
from almanak.framework.execution.extract_result import (
    ExtractError,
    ExtractMissing,
    ExtractOk,
    ExtractResult,
)

if TYPE_CHECKING:
    from almanak.connectors._strategy_base.primitive_money_leg import PrimitiveMoneyLeg, PrimitiveMoneyLegs
    from almanak.framework.execution.extracted_data import LPCloseData, LPOpenData, ProtocolFees, SwapAmounts
from almanak.framework.utils.log_formatters import format_gas_cost, format_tx_hash

logger = logging.getLogger(__name__)


# =============================================================================
# Event Topic Signatures
# =============================================================================

EVENT_TOPICS: dict[str, str] = {
    # StableSwap: TokenExchange(address,int128,uint256,int128,uint256)
    "TokenExchange": "0x8b3e96f2b889fa771c53c981b40daf005f63f637f1869f707052d15a3dd97140",
    # CryptoSwap/Tricrypto: TokenExchange(address,uint256,uint256,uint256,uint256)
    "TokenExchangeCrypto": "0xb2e76ae99761dc136e598d4a629bb347eccb9532a5f8bbd72e18467c3c34cc98",
    "TokenExchangeUnderlying": "0xd013ca23e77a65003c2c659c5442c00c805371b7fc1ebd4c206c41d1536bd90b",
    # AddLiquidity for NG pools (StableswapNG, TwocryptoNG):
    # AddLiquidity(address,uint256[2],uint256[2],uint256,uint256) — includes fees array
    "AddLiquidity2": "0x26f55a85081d24974e85c6c00045d0f0453991e95873f52bff0d21af4079a768",
    "AddLiquidity3": "0x423f6495a08fc652425cf4ed0d1f9e37e571d9b9529b1c1c23cce780b2e7df0d",
    # AddLiquidity(address,uint256[4],uint256[4],uint256,uint256) — 4-coin NG pool
    "AddLiquidity4": "0x3f1915775e0c9a38a57a7bb7f1f9005f486fb904e1f84aa215364d567319a58d",
    # AddLiquidity for old-style Twocrypto (pre-NG, no fees array):
    # AddLiquidity(address,uint256[2],uint256,uint256) — provider, amounts, invariant, supply
    "AddLiquidityV2Crypto2": "0x540ab385f9b5d450a27404172caade516b3ba3f4be88239ac56a2ad1de2a1f5a",
    # AddLiquidity for old-style 3-coin CryptoSwap (Tricrypto2, Tricrypto): single
    # fee scalar, no fees array — AddLiquidity(address,uint256[3],uint256,uint256).
    # provider(indexed), amounts[3], fee, token_supply. Verified on-chain 2026-06-27
    # against tricrypto2 (0xD51a44…). VIB-5441 (CryptoSwap LP-open was a ghost).
    "AddLiquidityV2Crypto3": "0x96b486485420b963edd3fdec0b0195730035600feb7de6f544383d7950fa97ee",
    # AddLiquidity for StableSwap NG pools that emit a dynamic-array event
    # (e.g. Optimism crvUSD/USDC at 0x03771e24…). Signature:
    # AddLiquidity(address,uint256[],uint256[],uint256,uint256) — VIB-4836.
    "AddLiquidityDyn": "0x189c623b666b1b45b83d7178f39b8c087cb09774317ca2f53c2d3c3726f222a2",
    # RemoveLiquidity for NG pools (includes fees array):
    # RemoveLiquidity(address,uint256[2],uint256[2],uint256)
    "RemoveLiquidity2": "0x7c363854ccf79623411f8995b362bce5eddff18c927edc6f5dbbb5e05819a82c",
    "RemoveLiquidity3": "0xa49d4cf02656aebf8c771f5a8585638a2a15ee6c97cf7205d4208ed7c1df252d",
    # RemoveLiquidity(address,uint256[4],uint256[4],uint256) — 4-coin NG pool
    "RemoveLiquidity4": "0x9878ca375e106f2a43c3b599fc624568131c4c9a4ba66a14563715763be9d59d",
    # RemoveLiquidity for StableSwap NG pools that emit a dynamic-array event
    # (mirrors AddLiquidityDyn). VIB-4836.
    "RemoveLiquidityDyn": "0x347ad828e58cbe534d8f6b67985d791360756b18f0d95fd9f197a66cc46480ea",
    # RemoveLiquidity for old-style Twocrypto (no fees array):
    # RemoveLiquidity(address,uint256[2],uint256)
    "RemoveLiquidityV2Crypto2": "0xdd3c0336a16f1b64f172b7bb0dad5b2b3c7c76f91e8c4aafd6aae60dce800153",
    # RemoveLiquidity for old-style 3-coin CryptoSwap (Tricrypto2/Tricrypto):
    # RemoveLiquidity(address,uint256[3],uint256) — provider(indexed), amounts[3],
    # token_supply (NO fees array). Verified on-chain 2026-06-27 against tricrypto2
    # (0xD51a44…). VIB-5491 (proportional CryptoSwap LP_CLOSE was a teardown ghost).
    "RemoveLiquidityV2Crypto3": "0xd6cc314a0b1e3b2579f8e64248e82434072e8271290eef8ad0886709304195f5",
    # --- Single-coin withdrawal (VIB-5433) -----------------------------------
    # Curve emits THREE distinct RemoveLiquidityOne ABIs across pool generations;
    # the array-free signature still varies by arg count, so each is its own
    # topic0. Verified on-chain 2026-06-29 (Etherscan getabi + real logs):
    #   * Legacy StableSwap — 3pool (0xbEbc44…), stETH (0xDC2431…), frxETH:
    #     RemoveLiquidityOne(address provider, uint256 token_amount,
    #     uint256 coin_amount) — 2 data words, NO coin_index. tx
    #     0xcdb08cd6…0665f6 (3pool).
    "RemoveLiquidityOneLegacy": "0x9e96dd3b997a2a257eec4df9bb6eaf626e206df5f543bd963682d143300be310",
    #   * 3-word variant — topic0 SHARED by two INCOMPATIBLE layouts that cannot
    #     be told apart by topic alone (disambiguated by pool family in
    #     ``_decode_remove_liquidity_one_data``; the authoritative proceeds come
    #     from the coin Transfer in ``_resolve_one_coin_proceeds``):
    #       - CryptoSwap/Tricrypto (tricrypto2 0xD51a44…): (token_amount,
    #         coin_index, coin_amount). tx 0x04cfdfaf…d8c26d.
    #       - StableSwap-NG (crvUSD/USDC 0x4DEcE6…): (token_amount, coin_amount,
    #         token_supply). tx 0xf14fc49a…b888d2.
    "RemoveLiquidityOne": "0x5ad056f2e28a8cec232015406b843668c1e36cda598127ec3b8c59b8c72773a0",
    #   * Twocrypto-NG / Tricrypto-NG (tricryptoUSDC 0x7F86Bf…): (token_amount,
    #     coin_index, coin_amount, approx_fee, packed_price_scale) — 5 data words.
    "RemoveLiquidityOneNG": "0xe200e24d4a4c7cd367dd9befe394dc8a14e6d58c88ff5e2f512d65a9e0aa9c5c",
    # --- Imbalanced withdrawal (VIB-5433) ------------------------------------
    # StableSwap-family only (CryptoSwap pools have no remove_liquidity_imbalance).
    # RemoveLiquidityImbalance(address provider, uint256[N] token_amounts,
    # uint256[N] fees, uint256 invariant, uint256 token_supply) — one topic0 per
    # coin-count (the array size is in the signature). Verified on-chain
    # 2026-06-29 against 3pool (0xbEbc44…) tx 0x348c89c8…b21c833 ([3] variant).
    # ``token_amounts`` is positional by pool-coin index.
    "RemoveLiquidityImbalance": "0x2b5508378d7e19e0d5fa338419034731416c4f5b219a10379956f764317fd47e",  # [2]
    "RemoveLiquidityImbalance3": "0x173599dbf9c6ca6f7c3b590df07ae98a45d74ff54065505141e7de6c46a624c2",
    "RemoveLiquidityImbalance4": "0xb964b72f73f5ef5bf0fdc559b2fab9a7b12a39e47817a547f1f0aee47febd602",
    # StableSwap-NG dynamic-array variant (mirrors RemoveLiquidityDyn):
    # RemoveLiquidityImbalance(address, uint256[], uint256[], uint256, uint256).
    "RemoveLiquidityImbalanceDyn": "0x3631c28b1f9dd213e0319fb167b554d76b6c283a41143eb400a0d1adb1af1755",
    "Transfer": "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
}

TOPIC_TO_EVENT: dict[str, str] = {v: k for k, v in EVENT_TOPICS.items()}

# Legacy exports
TOKEN_EXCHANGE_TOPIC = EVENT_TOPICS["TokenExchange"]
TOKEN_EXCHANGE_UNDERLYING_TOPIC = EVENT_TOPICS["TokenExchangeUnderlying"]

# Curve LP (pool) tokens are minted by the Curve pool/factory contracts and are
# fixed at 18 decimals by protocol design — this is a known protocol invariant,
# not an arbitrary token whose decimals are unknown. When the resolver cannot
# confirm the LP token's decimals, this invariant is the correct value (vs the
# VIB-3164 anti-pattern of silently defaulting an *arbitrary* token to 18).
CURVE_LP_TOKEN_DECIMALS = 18  # decimal-policy-exempt: Curve LP tokens are 18 by protocol invariant (VIB-3164)

# Curve's native-ETH placeholder coin address (used in coin_addresses for pools
# that hold raw ETH, e.g. the stETH pool). A single-sided withdrawal of this coin
# emits NO ERC-20 Transfer, so it is resolved from the event scalar + the pool's
# single native slot rather than from a Transfer log (VIB-5433). Stored EIP-55
# checksummed (the test_eip55_checksum production gate forbids lowercase address
# literals); ``_NATIVE_ETH_PLACEHOLDER_LC`` is the lowercased form used for the
# case-insensitive comparison against lowercased pool-coin addresses.
CURVE_NATIVE_ETH_PLACEHOLDER = "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE"
_NATIVE_ETH_PLACEHOLDER_LC = CURVE_NATIVE_ETH_PLACEHOLDER.lower()

# Pool families that emit the CryptoSwap RemoveLiquidityOne layout
# ``(token_amount, coin_index, coin_amount)`` vs the StableSwap-NG layout
# ``(token_amount, coin_amount, token_supply)`` — both share topic0 0x5ad056…,
# so the 3-word decode is disambiguated by the pool's registry ``pool_type``
# (VIB-5433). CryptoSwap pools carry an explicit ``coin_index``; StableSwap
# pools do not.
_CRYPTO_POOL_TYPES: frozenset[str] = frozenset({"tricrypto", "cryptoswap", "twocrypto"})
_STABLE_POOL_TYPES: frozenset[str] = frozenset({"stableswap", "metapool"})

# Coin-count each fixed-array RemoveLiquidityImbalance topic0 bijectively encodes
# (the array size is in the signature). Used to cross-check the length-derived
# arity in the fixed-array decoder and fail closed on a corrupt/truncated payload
# whose length disagrees with its topic (VIB-5433 hardening).
_IMBALANCE_TOPIC_ARITY: dict[str, int] = {
    "RemoveLiquidityImbalance": 2,
    "RemoveLiquidityImbalance3": 3,
    "RemoveLiquidityImbalance4": 4,
}


# =============================================================================
# Enums
# =============================================================================


class CurveEventType(Enum):
    """Curve event types."""

    TOKEN_EXCHANGE = "TOKEN_EXCHANGE"
    TOKEN_EXCHANGE_UNDERLYING = "TOKEN_EXCHANGE_UNDERLYING"
    ADD_LIQUIDITY = "ADD_LIQUIDITY"
    REMOVE_LIQUIDITY = "REMOVE_LIQUIDITY"
    REMOVE_LIQUIDITY_ONE = "REMOVE_LIQUIDITY_ONE"
    REMOVE_LIQUIDITY_IMBALANCE = "REMOVE_LIQUIDITY_IMBALANCE"
    TRANSFER = "TRANSFER"
    UNKNOWN = "UNKNOWN"


EVENT_NAME_TO_TYPE: dict[str, CurveEventType] = {
    "TokenExchange": CurveEventType.TOKEN_EXCHANGE,
    "TokenExchangeCrypto": CurveEventType.TOKEN_EXCHANGE,
    "TokenExchangeUnderlying": CurveEventType.TOKEN_EXCHANGE_UNDERLYING,
    "AddLiquidity2": CurveEventType.ADD_LIQUIDITY,
    "AddLiquidity3": CurveEventType.ADD_LIQUIDITY,
    "AddLiquidity4": CurveEventType.ADD_LIQUIDITY,
    "AddLiquidityV2Crypto2": CurveEventType.ADD_LIQUIDITY,  # old-style Twocrypto (pre-NG)
    "AddLiquidityV2Crypto3": CurveEventType.ADD_LIQUIDITY,  # old-style 3-coin CryptoSwap (Tricrypto2)
    "AddLiquidityDyn": CurveEventType.ADD_LIQUIDITY,  # StableSwap NG dynamic-array (VIB-4836)
    "RemoveLiquidity2": CurveEventType.REMOVE_LIQUIDITY,
    "RemoveLiquidity3": CurveEventType.REMOVE_LIQUIDITY,
    "RemoveLiquidity4": CurveEventType.REMOVE_LIQUIDITY,
    "RemoveLiquidityV2Crypto2": CurveEventType.REMOVE_LIQUIDITY,  # old-style Twocrypto (pre-NG)
    "RemoveLiquidityV2Crypto3": CurveEventType.REMOVE_LIQUIDITY,  # old-style 3-coin CryptoSwap (Tricrypto2)
    "RemoveLiquidityDyn": CurveEventType.REMOVE_LIQUIDITY,  # StableSwap NG dynamic-array (VIB-4836)
    # Single-coin withdrawal variants (VIB-5433) — all map to REMOVE_LIQUIDITY_ONE
    "RemoveLiquidityOneLegacy": CurveEventType.REMOVE_LIQUIDITY_ONE,  # old StableSwap (no coin_index)
    "RemoveLiquidityOne": CurveEventType.REMOVE_LIQUIDITY_ONE,  # CryptoSwap / StableSwap-NG (3-word)
    "RemoveLiquidityOneNG": CurveEventType.REMOVE_LIQUIDITY_ONE,  # Twocrypto/Tricrypto-NG (5-word)
    # Imbalanced withdrawal variants (VIB-5433) — all map to REMOVE_LIQUIDITY_IMBALANCE
    "RemoveLiquidityImbalance": CurveEventType.REMOVE_LIQUIDITY_IMBALANCE,  # [2]
    "RemoveLiquidityImbalance3": CurveEventType.REMOVE_LIQUIDITY_IMBALANCE,
    "RemoveLiquidityImbalance4": CurveEventType.REMOVE_LIQUIDITY_IMBALANCE,
    "RemoveLiquidityImbalanceDyn": CurveEventType.REMOVE_LIQUIDITY_IMBALANCE,  # StableSwap-NG dynamic-array
    "Transfer": CurveEventType.TRANSFER,
}


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class CurveEvent:
    """Parsed Curve event."""

    event_type: CurveEventType
    event_name: str
    log_index: int
    transaction_hash: str
    block_number: int
    contract_address: str
    data: dict[str, Any]
    raw_topics: list[str] = field(default_factory=list)
    raw_data: str = ""
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "event_type": self.event_type.value,
            "event_name": self.event_name,
            "log_index": self.log_index,
            "transaction_hash": self.transaction_hash,
            "block_number": self.block_number,
            "contract_address": self.contract_address,
            "data": self.data,
            "raw_topics": self.raw_topics,
            "raw_data": self.raw_data,
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass
class SwapEventData:
    """Parsed data from TokenExchange event."""

    buyer: str
    sold_id: int  # int128 token index
    tokens_sold: int
    bought_id: int  # int128 token index
    tokens_bought: int
    pool_address: str

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "buyer": self.buyer,
            "sold_id": self.sold_id,
            "tokens_sold": str(self.tokens_sold),
            "bought_id": self.bought_id,
            "tokens_bought": str(self.tokens_bought),
            "pool_address": self.pool_address,
        }


@dataclass
class AddLiquidityEventData:
    """Parsed data from AddLiquidity event."""

    provider: str
    token_amounts: list[int]
    fees: list[int]
    invariant: int
    token_supply: int
    pool_address: str

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "provider": self.provider,
            "token_amounts": [str(a) for a in self.token_amounts],
            "fees": [str(f) for f in self.fees],
            "invariant": str(self.invariant),
            "token_supply": str(self.token_supply),
            "pool_address": self.pool_address,
        }


@dataclass
class RemoveLiquidityEventData:
    """Parsed data from RemoveLiquidity event."""

    provider: str
    token_amounts: list[int]
    fees: list[int]
    token_supply: int
    pool_address: str

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "provider": self.provider,
            "token_amounts": [str(a) for a in self.token_amounts],
            "fees": [str(f) for f in self.fees],
            "token_supply": str(self.token_supply),
            "pool_address": self.pool_address,
        }


@dataclass
class ParseResult:
    """Result of parsing a receipt."""

    success: bool
    events: list[CurveEvent] = field(default_factory=list)
    swap_events: list[SwapEventData] = field(default_factory=list)
    error: str | None = None
    transaction_hash: str = ""
    block_number: int = 0
    transaction_success: bool = True

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "events": [e.to_dict() for e in self.events],
            "swap_events": [s.to_dict() for s in self.swap_events],
            "error": self.error,
            "transaction_hash": self.transaction_hash,
            "block_number": self.block_number,
            "transaction_success": self.transaction_success,
        }


# =============================================================================
# Helpers
# =============================================================================


def _canonical_pool_address(event: CurveEvent) -> str:
    """Return the canonical Curve pool address for an Add/RemoveLiquidity event.

    The Curve pool contract is the contract that EMITS the
    AddLiquidity / RemoveLiquidity event — exactly the chain-data-first
    identity the LP accounting handler's ``_resolve_lp_pool_address`` accepts
    (a lowercased ``0x``-prefixed 20-byte address). The fixed-array decoders
    stamp ``pool_address`` into ``event.data``; the dynamic-array and the
    ``RemoveLiquidityOne`` / ``RemoveLiquidityImbalance`` paths do not, so fall
    back to ``event.contract_address`` (the same emitter). Returns ``""`` when
    no address is available rather than fabricating one (Empty ≠ Zero).
    """
    addr = event.data.get("pool_address") or event.contract_address or ""
    return str(addr).lower()


def _pool_coin_addresses(pool_address: str, chain: str) -> list[str]:
    """Return the pool-coin-ordered ERC-20 addresses for a Curve pool, or ``[]``.

    Looks the pool up by its on-chain address in the static ``CURVE_POOLS``
    registry (the same metadata the compiler funds from) and returns its
    ``coin_addresses`` in pool-coin index order — the SAME order the
    AddLiquidity event emits ``token_amounts``. This lets the money-leg
    declaration map ``token_amounts[i]`` to the coin that index actually funds,
    instead of the legacy positional ``amount0``/``amount1`` guess that blindly
    assumes coin 0 = ``token_in`` and coin 1 = ``token_out`` (VIB-3587: a
    single-sided deposit of coin index 1+ left coin 0 carrying a fabricated zero
    leg, and a deposit of coin index 2+ was dropped entirely).

    Returns ``[]`` (→ caller declares no legs, legacy fallback unchanged) when
    the pool is unknown or carries no ``coin_addresses`` — never fabricates an
    address (Empty ≠ Zero).
    """
    if not pool_address:
        return []
    try:
        from almanak.connectors.curve.adapter import CURVE_POOLS

        chain_pools = CURVE_POOLS.get(chain, {})
        target = pool_address.lower()
        for data in chain_pools.values():
            if str(data.get("address", "")).lower() == target:
                coin_addresses = data.get("coin_addresses") or []
                return [str(a) for a in coin_addresses]
    except Exception as exc:  # noqa: BLE001 — accounting path: degrade to legacy, never raise
        logger.debug("Curve money-legs: pool-coin lookup failed for %s on %s: %s", pool_address, chain, exc)
    return []


def _pool_coin_symbols(pool_address: str, chain: str) -> list[str]:
    """Return the pool-coin-ordered token SYMBOLS for a Curve pool, or ``[]``.

    Sibling of :func:`_pool_coin_addresses` reading the static ``CURVE_POOLS``
    ``coins`` list (e.g. ``["DAI", "USDC", "USDT"]`` for 3pool) in pool-coin
    index order — the SAME order the AddLiquidity / RemoveLiquidity events emit
    their ``token_amounts`` / ``fees`` arrays. VIB-5429: the LP accounting
    handler needs these symbols to map each decoded fee/amount leg to a coin it
    can resolve decimals + a USD price for; a Curve LP_CLOSE ledger row carries
    no ``token_in`` / ``token_out`` and the fungible position_key has no token
    descriptor, so without this lookup the close legs collapse to NULLs.

    Returns ``[]`` (→ caller degrades, never fabricates) when the pool is
    unknown or carries no ``coins`` (Empty ≠ Zero).
    """
    if not pool_address:
        return []
    try:
        from almanak.connectors.curve.adapter import CURVE_POOLS

        target = pool_address.lower()
        for data in CURVE_POOLS.get(chain, {}).values():
            if str(data.get("address", "")).lower() == target:
                coins = data.get("coins") or []
                return [str(c) for c in coins]
    except Exception as exc:  # noqa: BLE001 — accounting path: degrade to legacy, never raise
        logger.debug("Curve coin-symbol lookup failed for %s on %s: %s", pool_address, chain, exc)
    return []


def _pool_type(pool_address: str, chain: str) -> str:
    """Return the static ``CURVE_POOLS`` ``pool_type`` for a pool, or ``""``.

    Used to disambiguate the two incompatible 3-word ``RemoveLiquidityOne``
    layouts that share topic0 (CryptoSwap carries a ``coin_index``; StableSwap-NG
    does not — VIB-5433). Returns ``""`` (→ caller defers to the Transfer-based
    proceeds resolver) when the pool is unknown; never raises.
    """
    if not pool_address:
        return ""
    try:
        from almanak.connectors.curve.adapter import CURVE_POOLS

        target = pool_address.lower()
        for data in CURVE_POOLS.get(chain, {}).values():
            if str(data.get("address", "")).lower() == target:
                return str(data.get("pool_type", "")).lower()
    except Exception as exc:  # noqa: BLE001 — accounting path: degrade, never raise
        logger.debug("Curve pool-type lookup failed for %s on %s: %s", pool_address, chain, exc)
    return ""


# =============================================================================
# Receipt Parser
# =============================================================================


class CurveReceiptParser:
    """Parser for Curve Finance transaction receipts.

    Refactored to use base infrastructure utilities for hex decoding
    and event registry management. Maintains full backward compatibility.
    """

    # VIB-3587 — Connector-DECLARED per-intent extra extractions. The framework
    # enricher merges these onto the generic ``EXTRACTION_SPECS`` base via
    # ``ResultEnricher._with_parser_extra_extractions`` — keeping the Curve-specific
    # field choice in the connector, not as a protocol-named overlay in the framework
    # (``test_connector_descriptor`` / the chain-protocol coupling ratchet forbid that
    # for migrated connectors). An LP_OPEN routes through
    # ``extract_primitive_money_legs``, which declares one INPUT leg per FUNDED pool
    # coin (built from the AddLiquidity event's pool-coin-ordered ``token_amounts``
    # joined to the pool's ``coin_addresses``). This surfaces the typed
    # ``PrimitiveMoneyLegs`` under ``extracted_data["primitive_money_legs"]`` — the
    # seam the US-009 ledger dispatcher (``_extract_tokens_and_amounts``) prefers over
    # the legacy ``LPOpenData.amount0`` / ``amount1`` two-slot guess. Curve is a
    # multi-coin venue with single-sided and non-leading deposits, so that legacy
    # guess persisted a fabricated zero leg for an unfunded coin (and dropped a coin
    # index 2+ deposit entirely); the declared legs put each funded amount on the coin
    # it actually funds, with an unfunded coin simply ABSENT (Empty != Zero). A deposit
    # the extractor cannot measure yields ``None`` → legacy path, unchanged. Mirrors
    # the Pendle / Lido ``primitive_money_legs`` declaration.
    EXTRA_EXTRACTIONS_BY_INTENT: dict[str, tuple[str, ...]] = {
        "LP_OPEN": ("primitive_money_legs",),
    }

    # VIB-5432 — Capability surface for the ResultEnricher SUPPORTED_EXTRACTIONS
    # check. Each entry maps to a present ``extract_<field>`` method on this class.
    # Declaring the FULL set of currently-served fields is deliberately behaviour-
    # preserving: before this attribute existed the enricher attempted every
    # ``extract_<field>`` method that happened to be defined, so omitting one here
    # would newly skip a field that is served today. Curve is a pool-based
    # (non-NFT) multi-coin venue; ``primitive_money_legs`` is the LP_OPEN leg seam
    # (declared per-intent above).
    SUPPORTED_EXTRACTIONS: frozenset[str] = frozenset(
        {
            "swap_amounts",
            "position_id",  # LP-token semantics (pool-based, no NFT position id)
            "liquidity",
            "lp_tokens_received",
            "lp_open_data",
            "primitive_money_legs",
            "lp_close_data",
            "protocol_fees",  # UNAVAILABLE-with-reason per VIB-3495
        }
    )

    def __init__(self, chain: str = "ethereum", **kwargs: Any) -> None:
        """Initialize the parser.

        Args:
            chain: Blockchain network
            **kwargs: Additional arguments (ignored for compatibility)
        """
        self.chain = chain.lower()
        self.registry = EventRegistry(EVENT_TOPICS, EVENT_NAME_TO_TYPE)

    def parse_receipt(
        self,
        receipt: dict[str, Any],
    ) -> ParseResult:
        """Parse a transaction receipt.

        Args:
            receipt: Transaction receipt dict

        Returns:
            ParseResult with extracted events
        """
        try:
            tx_hash = receipt.get("transactionHash", "")
            if isinstance(tx_hash, bytes):
                tx_hash = "0x" + tx_hash.hex()

            block_number = receipt.get("blockNumber", 0)
            logs = receipt.get("logs", [])
            status = receipt.get("status", 1)
            tx_success = status == 1

            # Reverts must be reported before the empty-logs short-circuit,
            # otherwise an early-revert receipt (status=0, logs=[]) would be
            # silently surfaced as a successful empty receipt (issue #2064).
            if not tx_success:
                return ParseResult(
                    success=True,
                    transaction_hash=tx_hash,
                    block_number=block_number,
                    transaction_success=False,
                    error="Transaction reverted",
                )

            if not logs:
                return ParseResult(
                    success=True,
                    transaction_hash=tx_hash,
                    block_number=block_number,
                    transaction_success=tx_success,
                )

            events: list[CurveEvent] = []
            swap_events: list[SwapEventData] = []

            for log in logs:
                parsed_event = self._parse_log(log, tx_hash, block_number)
                if parsed_event:
                    events.append(parsed_event)

                    # Extract typed data for swaps
                    if parsed_event.event_type in (
                        CurveEventType.TOKEN_EXCHANGE,
                        CurveEventType.TOKEN_EXCHANGE_UNDERLYING,
                    ):
                        swap_data = self._parse_swap_event(parsed_event)
                        if swap_data:
                            swap_events.append(swap_data)

            # Log parsed receipt
            gas_used = receipt.get("gasUsed", 0)
            tx_fmt = format_tx_hash(tx_hash)
            gas_fmt = format_gas_cost(gas_used)

            if swap_events:
                swap = swap_events[0]
                logger.info(
                    f"🔍 Parsed Curve swap: token{swap.sold_id} → token{swap.bought_id}, tx={tx_fmt}, {gas_fmt}"
                )
            else:
                logger.info(f"🔍 Parsed Curve receipt: tx={tx_fmt}, events={len(events)}, {gas_fmt}")

            return ParseResult(
                success=True,
                events=events,
                swap_events=swap_events,
                transaction_hash=tx_hash,
                block_number=block_number,
                transaction_success=tx_success,
            )

        except Exception as e:
            logger.exception(f"Failed to parse receipt: {e}")
            return ParseResult(
                success=False,
                error=str(e),
            )

    def _parse_log(
        self,
        log: dict[str, Any],
        tx_hash: str,
        block_number: int,
    ) -> CurveEvent | None:
        """Parse a single log entry.

        Args:
            log: Log dict
            tx_hash: Transaction hash
            block_number: Block number

        Returns:
            Parsed event or None if not recognized
        """
        try:
            topics = log.get("topics", [])
            if not topics:
                return None

            # Normalize first topic
            first_topic = topics[0]
            if isinstance(first_topic, bytes):
                first_topic = "0x" + first_topic.hex()
            else:
                first_topic = str(first_topic)
            first_topic = first_topic.lower()

            # Check if known event
            event_name = self.registry.get_event_name(first_topic)
            if event_name is None:
                return None

            event_type = self.registry.get_event_type(event_name) or CurveEventType.UNKNOWN

            # Get raw data
            data = HexDecoder.normalize_hex(log.get("data", ""))

            # Normalize contract address
            contract_address = log.get("address", "")
            if isinstance(contract_address, bytes):
                contract_address = "0x" + contract_address.hex()

            # Convert topics to strings
            topics_str = []
            for topic in topics:
                if isinstance(topic, bytes):
                    topics_str.append("0x" + topic.hex())
                else:
                    topics_str.append(str(topic))

            # Parse log data
            parsed_data = self._decode_log_data(event_type, topics, data, contract_address, event_name=event_name)

            return CurveEvent(
                event_type=event_type,
                event_name=event_name,
                log_index=log.get("logIndex", 0),
                transaction_hash=tx_hash,
                block_number=block_number,
                contract_address=contract_address,
                data=parsed_data,
                raw_topics=topics_str,
                raw_data=data,
            )

        except Exception as e:
            logger.warning(f"Failed to parse log: {e}")
            return None

    def _decode_log_data(
        self,
        event_type: CurveEventType,
        topics: list[Any],
        data: str,
        address: str,
        event_name: str = "",
    ) -> dict[str, Any]:
        """Decode log data based on event type.

        Args:
            event_type: Type of event
            topics: List of topics
            data: Hex-encoded event data
            address: Contract address
            event_name: Original event name (e.g. "TokenExchange" vs "TokenExchangeCrypto")

        Returns:
            Decoded event data dict
        """
        if event_type in (CurveEventType.TOKEN_EXCHANGE, CurveEventType.TOKEN_EXCHANGE_UNDERLYING):
            return self._decode_swap_data(topics, data, address, event_name=event_name)
        elif event_type == CurveEventType.ADD_LIQUIDITY:
            return self._decode_add_liquidity_data(topics, data, address, event_name=event_name)
        elif event_type == CurveEventType.REMOVE_LIQUIDITY:
            return self._decode_remove_liquidity_data(topics, data, address, event_name=event_name)
        elif event_type == CurveEventType.REMOVE_LIQUIDITY_ONE:
            return self._decode_remove_liquidity_one_data(topics, data, address, event_name=event_name)
        elif event_type == CurveEventType.REMOVE_LIQUIDITY_IMBALANCE:
            return self._decode_remove_liquidity_imbalance_data(topics, data, address, event_name=event_name)
        else:
            return {"raw_data": data}

    def _decode_swap_data(
        self,
        topics: list[Any],
        data: str,
        address: str,
        event_name: str = "",
    ) -> dict[str, Any]:
        """Decode TokenExchange event data.

        StableSwap: TokenExchange(address indexed buyer, int128 sold_id, uint256 tokens_sold,
                                  int128 bought_id, uint256 tokens_bought)
        CryptoSwap: TokenExchange(address indexed buyer, uint256 sold_id, uint256 tokens_sold,
                                  uint256 bought_id, uint256 tokens_bought)
        """
        try:
            # Indexed: buyer
            buyer = HexDecoder.topic_to_address(topics[1]) if len(topics) > 1 else ""

            # CryptoSwap uses uint256 for token indices; StableSwap uses int128
            is_crypto = event_name == "TokenExchangeCrypto"
            decode_index = HexDecoder.decode_uint256 if is_crypto else HexDecoder.decode_int128

            sold_id = decode_index(data, 0)
            tokens_sold = HexDecoder.decode_uint256(data, 32)
            bought_id = decode_index(data, 64)
            tokens_bought = HexDecoder.decode_uint256(data, 96)

            pool_address = address.lower() if isinstance(address, str) else ""

            return {
                "buyer": buyer,
                "sold_id": sold_id,
                "tokens_sold": tokens_sold,
                "bought_id": bought_id,
                "tokens_bought": tokens_bought,
                "pool_address": pool_address,
            }

        except Exception as e:
            logger.warning(f"Failed to decode TokenExchange data: {e}")
            return {"raw_data": data}

    def _decode_add_liquidity_data(
        self,
        topics: list[Any],
        data: str,
        address: str,
        event_name: str = "",
    ) -> dict[str, Any]:
        """Decode AddLiquidity event data.

        Two formats are supported:
        - NG pools (AddLiquidity2/AddLiquidity3): amounts + fees + invariant + supply
          (2-coin: 6 fields × 64 = 384 hex chars; 3-coin: 8 fields × 512 hex chars)
        - Old-style Twocrypto (AddLiquidityV2Crypto2): amounts + invariant + supply
          (NO fees array: 2-coin: 4 fields × 64 = 256 hex chars)
        """
        try:
            # Indexed: provider
            provider = HexDecoder.topic_to_address(topics[1]) if len(topics) > 1 else ""
            pool_address = address.lower() if isinstance(address, str) else ""

            # Old-style Twocrypto (pre-NG): no fees array
            # Format: amounts[0], amounts[1], invariant, token_supply
            if event_name == "AddLiquidityV2Crypto2":
                token_amounts = [
                    HexDecoder.decode_uint256(data, 0),
                    HexDecoder.decode_uint256(data, 32),
                ]
                invariant = HexDecoder.decode_uint256(data, 64)
                token_supply = HexDecoder.decode_uint256(data, 96)
                return {
                    "provider": provider,
                    "token_amounts": token_amounts,
                    "fees": [],  # Old-style pools don't emit fees in this event
                    "invariant": invariant,
                    "token_supply": token_supply,
                    "pool_address": pool_address,
                }

            # Old-style 3-coin CryptoSwap (Tricrypto2/Tricrypto): provider(indexed),
            # amounts[3], single pool-level scalar, token_supply — 5 data words.
            # Mirror the 2-coin V2Crypto2 shape exactly (``fees=[]``, scalar under
            # ``invariant``) so no downstream consumer misreads a per-coin fees
            # array; accounting reads token_amounts + token_supply.
            if event_name == "AddLiquidityV2Crypto3":
                # Fail closed on a truncated payload: decode_uint256 returns 0 for a
                # missing word, so without this guard a short log would decode as a
                # "successful" LP_OPEN with fabricated zero amounts/supply (a ghost)
                # instead of tripping the raw_data path. 5 data words (amounts[3] +
                # invariant + token_supply) = 320 hex chars.
                if len(HexDecoder.normalize_hex(data)) < 5 * 64:
                    logger.warning(
                        "AddLiquidityV2Crypto3 payload too short (%d hex chars, need >=320); "
                        "failing closed to raw_data",
                        len(HexDecoder.normalize_hex(data)),
                    )
                    return {"raw_data": data}
                token_amounts = [
                    HexDecoder.decode_uint256(data, 0),
                    HexDecoder.decode_uint256(data, 32),
                    HexDecoder.decode_uint256(data, 64),
                ]
                invariant = HexDecoder.decode_uint256(data, 96)
                token_supply = HexDecoder.decode_uint256(data, 128)
                return {
                    "provider": provider,
                    "token_amounts": token_amounts,
                    "fees": [],  # single pool-level scalar, not a per-coin fees array
                    "invariant": invariant,
                    "token_supply": token_supply,
                    "pool_address": pool_address,
                }

            # StableSwap NG pools that emit a dynamic-array event:
            # AddLiquidity(address provider, uint256[] amounts, uint256[] fees,
            #              uint256 invariant, uint256 token_supply)
            # ABI head (4 × 32 bytes): offset_to_amounts, offset_to_fees,
            # invariant, supply. Tail at each offset: [length, *elements].
            if event_name == "AddLiquidityDyn":
                offset_amounts = HexDecoder.decode_uint256(data, 0)
                offset_fees = HexDecoder.decode_uint256(data, 32)
                invariant = HexDecoder.decode_uint256(data, 64)
                token_supply = HexDecoder.decode_uint256(data, 96)

                amounts_len = HexDecoder.decode_uint256(data, offset_amounts)
                token_amounts = [
                    HexDecoder.decode_uint256(data, offset_amounts + 32 + i * 32) for i in range(amounts_len)
                ]
                fees_len = HexDecoder.decode_uint256(data, offset_fees)
                fees = [HexDecoder.decode_uint256(data, offset_fees + 32 + i * 32) for i in range(fees_len)]
                return {
                    "provider": provider,
                    "token_amounts": token_amounts,
                    "fees": fees,
                    "invariant": invariant,
                    "token_supply": token_supply,
                    "pool_address": pool_address,
                }

            # NG pools: amounts + fees + invariant + supply
            # Determine n_coins from data length: n_coins*2 + 2 fields, each 64 hex chars
            # 2-coin: 6 * 64 = 384, 3-coin: 8 * 64 = 512, 4-coin: 10 * 64 = 640
            data_len = len(data)
            if data_len >= 640:  # 10 * 64 for 4-coin
                n_coins = 4
            elif data_len >= 512:  # 8 * 64 for 3-coin
                n_coins = 3
            else:
                n_coins = 2

            # Parse token amounts
            token_amounts = []
            for i in range(n_coins):
                token_amounts.append(HexDecoder.decode_uint256(data, i * 32))

            # Parse fees
            fees = []
            for i in range(n_coins):
                fees.append(HexDecoder.decode_uint256(data, (n_coins + i) * 32))

            # Parse invariant and supply
            invariant = HexDecoder.decode_uint256(data, n_coins * 2 * 32)
            token_supply = HexDecoder.decode_uint256(data, (n_coins * 2 + 1) * 32)

            return {
                "provider": provider,
                "token_amounts": token_amounts,
                "fees": fees,
                "invariant": invariant,
                "token_supply": token_supply,
                "pool_address": pool_address,
            }

        except Exception as e:
            logger.warning(f"Failed to decode AddLiquidity data: {e}")
            return {"raw_data": data}

    def _decode_remove_liquidity_data(
        self,
        topics: list[Any],
        data: str,
        address: str,
        event_name: str = "",
    ) -> dict[str, Any]:
        """Decode RemoveLiquidity event data.

        Two formats are supported:
        - NG pools (RemoveLiquidity2/RemoveLiquidity3): amounts + fees + supply
          (2-coin: 5 fields × 64 = 320 hex chars; 3-coin: 7 fields × 448 hex chars)
        - Old-style Twocrypto (RemoveLiquidityV2Crypto2): amounts + supply (NO fees)
          (2-coin: 3 fields × 64 = 192 hex chars)
        """
        try:
            # Indexed: provider
            provider = HexDecoder.topic_to_address(topics[1]) if len(topics) > 1 else ""
            pool_address = address.lower() if isinstance(address, str) else ""

            # Old-style Twocrypto (pre-NG): no fees array
            # Format: amounts[0], amounts[1], token_supply
            if event_name == "RemoveLiquidityV2Crypto2":
                token_amounts = [
                    HexDecoder.decode_uint256(data, 0),
                    HexDecoder.decode_uint256(data, 32),
                ]
                token_supply = HexDecoder.decode_uint256(data, 64)
                return {
                    "provider": provider,
                    "token_amounts": token_amounts,
                    "fees": [],  # Old-style pools don't emit fees in this event
                    "token_supply": token_supply,
                    "pool_address": pool_address,
                }

            # Old-style 3-coin CryptoSwap (Tricrypto2/Tricrypto): provider(indexed),
            # amounts[3], token_supply — 4 data words, no fees array.
            if event_name == "RemoveLiquidityV2Crypto3":
                # Fail closed on a truncated payload: decode_uint256 returns 0 for a
                # missing word, so without this guard a short log would decode as a
                # "successful" LP_CLOSE with fabricated zero amounts/supply (a ghost)
                # instead of tripping the raw_data path. 4 data words (amounts[3] +
                # token_supply) = 256 hex chars.
                if len(HexDecoder.normalize_hex(data)) < 4 * 64:
                    logger.warning(
                        "RemoveLiquidityV2Crypto3 payload too short (%d hex chars, need >=256); "
                        "failing closed to raw_data",
                        len(HexDecoder.normalize_hex(data)),
                    )
                    return {"raw_data": data}
                token_amounts = [
                    HexDecoder.decode_uint256(data, 0),
                    HexDecoder.decode_uint256(data, 32),
                    HexDecoder.decode_uint256(data, 64),
                ]
                token_supply = HexDecoder.decode_uint256(data, 96)
                return {
                    "provider": provider,
                    "token_amounts": token_amounts,
                    "fees": [],  # Old-style pools don't emit fees in this event
                    "token_supply": token_supply,
                    "pool_address": pool_address,
                }

            # StableSwap NG pools that emit a dynamic-array event:
            # RemoveLiquidity(address provider, uint256[] amounts, uint256[] fees,
            #                 uint256 token_supply)
            # ABI head (3 × 32 bytes): offset_to_amounts, offset_to_fees, supply.
            # Tail at each offset: [length, *elements].
            if event_name == "RemoveLiquidityDyn":
                offset_amounts = HexDecoder.decode_uint256(data, 0)
                offset_fees = HexDecoder.decode_uint256(data, 32)
                token_supply = HexDecoder.decode_uint256(data, 64)

                amounts_len = HexDecoder.decode_uint256(data, offset_amounts)
                token_amounts = [
                    HexDecoder.decode_uint256(data, offset_amounts + 32 + i * 32) for i in range(amounts_len)
                ]
                fees_len = HexDecoder.decode_uint256(data, offset_fees)
                fees = [HexDecoder.decode_uint256(data, offset_fees + 32 + i * 32) for i in range(fees_len)]
                return {
                    "provider": provider,
                    "token_amounts": token_amounts,
                    "fees": fees,
                    "token_supply": token_supply,
                    "pool_address": pool_address,
                }

            # NG pools: amounts + fees + supply (no invariant)
            # Determine n_coins from data length: n_coins*2 + 1 fields, each 64 hex chars
            # 2-coin: 5 * 64 = 320, 3-coin: 7 * 64 = 448, 4-coin: 9 * 64 = 576
            data_len = len(data)
            if data_len >= 576:  # 9 * 64 for 4-coin
                n_coins = 4
            elif data_len >= 448:  # 7 * 64 for 3-coin
                n_coins = 3
            else:
                n_coins = 2

            # Parse token amounts
            token_amounts = []
            for i in range(n_coins):
                token_amounts.append(HexDecoder.decode_uint256(data, i * 32))

            # Parse fees
            fees = []
            for i in range(n_coins):
                fees.append(HexDecoder.decode_uint256(data, (n_coins + i) * 32))

            # Parse supply
            token_supply = HexDecoder.decode_uint256(data, n_coins * 2 * 32)

            return {
                "provider": provider,
                "token_amounts": token_amounts,
                "fees": fees,
                "token_supply": token_supply,
                "pool_address": pool_address,
            }

        except Exception as e:
            logger.warning(f"Failed to decode RemoveLiquidity data: {e}")
            return {"raw_data": data}

    def _decode_remove_liquidity_one_data(
        self,
        topics: list[Any],
        data: str,
        address: str,
        event_name: str = "",
    ) -> dict[str, Any]:
        """Decode a RemoveLiquidityOne (single-coin withdrawal) event (VIB-5433).

        Curve emits three incompatible ABIs (see ``EVENT_TOPICS``). This decoder
        surfaces the LP ``token_amount`` burned and the BEST-EFFORT event
        ``coin_index`` / ``coin_amount`` — used only as the fallback in
        :meth:`_resolve_one_coin_proceeds` for a native-ETH leg that emits no
        ERC-20 Transfer. It deliberately emits NO ``token_amounts`` key (and keeps
        ``raw_data``) so the generic close path defers single-coin proceeds to the
        Transfer-based resolver, which is authoritative for the amount and the
        coin index. The 3-word topic-collision (CryptoSwap ``coin_index, coin_amount``
        vs StableSwap-NG ``coin_amount, token_supply``) is disambiguated by the
        pool's registry ``pool_type``; an unknown pool leaves both scalars ``None``
        (Empty ≠ Zero) so a wrong word is never read as proceeds.
        """
        try:
            provider = HexDecoder.topic_to_address(topics[1]) if len(topics) > 1 else ""
            pool_address = address.lower() if isinstance(address, str) else ""
            nwords = len(HexDecoder.normalize_hex(data)) // 64
            token_amount = HexDecoder.decode_uint256(data, 0) if nwords >= 1 else None

            # Each variant decodes its event scalars ONLY when the payload carries
            # the minimum word count; a truncated payload leaves them ``None`` so the
            # downstream Transfer resolver — not a fabricated ``0`` from
            # ``decode_uint256``'s missing-word default — supplies the proceeds
            # (CodeRabbit fail-closed: a short NG/legacy log must not become a
            # zero-proceeds close).
            coin_index: int | None = None
            coin_amount: int | None = None
            if event_name == "RemoveLiquidityOneNG":
                # Twocrypto/Tricrypto-NG: (token_amount, coin_index, coin_amount, ...).
                if nwords >= 5:
                    coin_index = HexDecoder.decode_uint256(data, 32)
                    coin_amount = HexDecoder.decode_uint256(data, 64)
            elif event_name == "RemoveLiquidityOneLegacy":
                # Legacy StableSwap: (token_amount, coin_amount) — no coin_index.
                if nwords >= 2:
                    coin_amount = HexDecoder.decode_uint256(data, 32)
            elif nwords >= 5:
                # Unnamed 5+-word payload → NG layout.
                coin_index = HexDecoder.decode_uint256(data, 32)
                coin_amount = HexDecoder.decode_uint256(data, 64)
            elif nwords == 3:
                # Topic-collision (0x5ad056…): disambiguate by pool family.
                ptype = _pool_type(pool_address, self.chain)
                if ptype in _CRYPTO_POOL_TYPES:
                    # CryptoSwap: (token_amount, coin_index, coin_amount).
                    coin_index = HexDecoder.decode_uint256(data, 32)
                    coin_amount = HexDecoder.decode_uint256(data, 64)
                elif ptype in _STABLE_POOL_TYPES:
                    # StableSwap-NG: (token_amount, coin_amount, token_supply).
                    coin_amount = HexDecoder.decode_uint256(data, 32)
                # Unknown pool → leave both None; the Transfer resolver decides.
            elif nwords == 2:
                # A 2-word payload under any name is the legacy shape.
                coin_amount = HexDecoder.decode_uint256(data, 32)
            # else (nwords < 2 / unexpected): leave both None → Transfer must resolve.

            return {
                "provider": provider,
                "pool_address": pool_address,
                "one_coin": True,
                "token_amount": token_amount,
                "one_coin_index": coin_index,
                "one_coin_amount": coin_amount,
                "fees": [],
                # No structured token_amounts: single-coin proceeds are resolved
                # from the coin Transfer in extract_lp_close_data. Keep raw_data
                # for diagnostics (this event is NOT in _STRUCTURALLY_DECODED_*,
                # so raw_data here is by-design, not a decode-failure sentinel).
                "raw_data": data,
            }
        except Exception as e:
            logger.warning(f"Failed to decode RemoveLiquidityOne data: {e}")
            return {"raw_data": data}

    def _decode_remove_liquidity_imbalance_data(
        self,
        topics: list[Any],
        data: str,
        address: str,
        event_name: str = "",
    ) -> dict[str, Any]:
        """Decode a RemoveLiquidityImbalance event (VIB-5433).

        StableSwap-family only. Two shapes are supported:

        - Fixed-array: ``amounts[N], fees[N], invariant, token_supply`` — ``2N+2``
          data words. ``N`` is derived from the payload length (each arity has its
          own topic0, but the length is the structural source of truth, mirroring
          ``_decode_remove_liquidity_data``).
        - StableSwap-NG dynamic-array (``RemoveLiquidityImbalanceDyn``):
          ``offset_amounts, offset_fees, invariant, token_supply`` head followed by
          ``[len, *elems]`` tails (mirrors ``RemoveLiquidityDyn``).

        ``token_amounts`` is positional by pool-coin index — the imbalanced
        withdrawal vector maps directly onto ``LPCloseData.amount{0,1}_collected``
        / ``additional_amounts``. Fails closed to ``{"raw_data": data}`` on a
        truncated / unexpected payload rather than fabricating zero proceeds.
        """
        try:
            provider = HexDecoder.topic_to_address(topics[1]) if len(topics) > 1 else ""
            pool_address = address.lower() if isinstance(address, str) else ""

            if event_name == "RemoveLiquidityImbalanceDyn":
                # Head: offset_amounts, offset_fees, invariant, token_supply.
                # Validate the full ABI envelope before decoding any array (a short
                # payload must fail closed to raw_data, and a malicious array length
                # must never drive an unbounded loop — CodeRabbit/gemini DoS guard).
                total_bytes = len(HexDecoder.normalize_hex(data)) // 2
                if total_bytes < 4 * 32:  # 4-word head required
                    logger.warning("RemoveLiquidityImbalanceDyn head truncated (%d bytes); raw_data", total_bytes)
                    return {"raw_data": data}
                offset_amounts = HexDecoder.decode_uint256(data, 0)
                offset_fees = HexDecoder.decode_uint256(data, 32)

                def _decode_dyn_array(offset: int) -> list[int] | None:
                    # length word must be inside the payload
                    if offset < 0 or offset + 32 > total_bytes:
                        return None
                    length = HexDecoder.decode_uint256(data, offset)
                    # Curve pools hold at most 8 coins; reject absurd/empty lengths
                    # and any length whose elements would run past the payload.
                    if not (1 <= length <= 8) or offset + 32 + length * 32 > total_bytes:
                        return None
                    return [HexDecoder.decode_uint256(data, offset + 32 + i * 32) for i in range(length)]

                token_amounts = _decode_dyn_array(offset_amounts)
                fees = _decode_dyn_array(offset_fees)
                if token_amounts is None or fees is None or len(token_amounts) != len(fees):
                    logger.warning("RemoveLiquidityImbalanceDyn payload failed bounds/symmetry validation; raw_data")
                    return {"raw_data": data}
                return {
                    "provider": provider,
                    "token_amounts": token_amounts,
                    "fees": fees,
                    "pool_address": pool_address,
                }

            # Fixed-array: 2N+2 words. Fail closed on a truncated / unexpected
            # payload so a short log never decodes as a zero-proceeds ghost.
            nwords = len(HexDecoder.normalize_hex(data)) // 64
            if nwords < 6 or (nwords - 2) % 2 != 0 or (nwords - 2) // 2 > 8:
                logger.warning(
                    "RemoveLiquidityImbalance payload unexpected (%d words; need 2N+2, 2<=N<=8); "
                    "failing closed to raw_data",
                    nwords,
                )
                return {"raw_data": data}
            n_coins = (nwords - 2) // 2
            # Cross-check the length-derived arity against the topic-implied arity:
            # each fixed RemoveLiquidityImbalance{,3,4} topic0 bijectively encodes N,
            # so a payload whose length disagrees (e.g. a truncated [3] log that
            # looks like a valid [2]) is corrupt — fail closed rather than mis-read a
            # real amount as a fee. (Unnamed-event defensive path keeps length-only.)
            topic_arity = _IMBALANCE_TOPIC_ARITY.get(event_name)
            if topic_arity is not None and n_coins != topic_arity:
                logger.warning(
                    "RemoveLiquidityImbalance %s implies %d coins but payload has %d; failing closed to raw_data",
                    event_name,
                    topic_arity,
                    n_coins,
                )
                return {"raw_data": data}
            token_amounts = [HexDecoder.decode_uint256(data, i * 32) for i in range(n_coins)]
            fees = [HexDecoder.decode_uint256(data, (n_coins + i) * 32) for i in range(n_coins)]
            # invariant = word 2N, token_supply = word 2N+1 (not needed for proceeds).
            return {
                "provider": provider,
                "token_amounts": token_amounts,
                "fees": fees,
                "pool_address": pool_address,
            }
        except Exception as e:
            logger.warning(f"Failed to decode RemoveLiquidityImbalance data: {e}")
            return {"raw_data": data}

    def _find_pool_coin_outflows(
        self,
        receipt: dict[str, Any],
        pool_address: str,
        coin_addresses_lower: list[str],
    ) -> list[tuple[int, int]]:
        """Return ALL ERC-20 Transfers that move a pool coin OUT of the pool.

        Each entry is ``(coin_index, raw_amount)`` for a ``Transfer`` whose token
        is one of the pool's coins and whose ``from`` is the pool. A single-coin
        removal emits exactly one such outflow (the withdrawn coin); the LP-token
        burn is a Transfer FROM the provider (not the pool), so it never matches.
        The caller treats >1 outflow as AMBIGUOUS and fails closed (a batched /
        zap transaction touching the same pool could otherwise mis-attribute the
        close to an unrelated transfer — CodeRabbit). Never raises: any malformed
        log is skipped (mirrors ``_has_mint_transfer``).
        """
        pool = (pool_address or "").lower()
        if not pool or not coin_addresses_lower:
            return []
        transfer_topic = EVENT_TOPICS["Transfer"].lower()
        logs = receipt.get("logs", [])
        if not isinstance(logs, list | tuple):
            return []
        outflows: list[tuple[int, int]] = []
        for log in logs:
            if not isinstance(log, dict):
                continue
            topics = log.get("topics", [])
            if not isinstance(topics, list | tuple) or len(topics) < 3:
                continue
            first_topic = topics[0]
            if isinstance(first_topic, bytes):
                first_topic = "0x" + first_topic.hex()
            if str(first_topic).lower() != transfer_topic:
                continue
            token_addr = log.get("address", "")
            if isinstance(token_addr, bytes):
                token_addr = "0x" + token_addr.hex()
            token_addr = str(token_addr).lower()
            if token_addr not in coin_addresses_lower:
                continue
            try:
                from_addr = HexDecoder.topic_to_address(topics[1]).lower()
                if from_addr != pool:
                    continue
                amount_data = HexDecoder.normalize_hex(log.get("data", ""))
                amount = HexDecoder.decode_uint256(amount_data, 0)
            except Exception:  # noqa: BLE001 — degenerate topic/data word; skip
                continue
            outflows.append((coin_addresses_lower.index(token_addr), amount))
        return outflows

    def _resolve_one_coin_proceeds(
        self,
        event: "CurveEvent",
        receipt: dict[str, Any],
    ) -> tuple[list[int] | None, list[int]]:
        """Resolve a RemoveLiquidityOne close into a pool-coin-ordered amounts list.

        Single-coin proceeds (VIB-5433). The single withdrawn coin's amount lands
        at its pool-coin index; the other coins are a measured ``0`` (a single-coin
        withdrawal genuinely returns nothing of them — a measured fact, not a
        fabricated Empty-as-Zero). Resolution order:

        1. **Authoritative** — the ERC-20 Transfer of a pool coin OUT of the pool
           gives the exact tokens received AND the coin index by address, immune to
           the 3-word topic collision. EXACTLY ONE such outflow is expected for a
           single-coin close; >1 is an ambiguous batch/zap and fails closed.
        2. **Native-ETH fallback** — a pool with a single native-ETH placeholder
           coin and no ERC-20 coin outflow withdrew the native coin; its amount is
           the event ``coin_amount`` scalar.
        3. **Event-scalar fallback** — a variant that carries an explicit
           ``coin_index`` + ``coin_amount`` (CryptoSwap / NG) when the pool's coin
           map is unavailable.

        Returns ``(token_amounts, [])`` on success, or ``(None, [])`` when the
        proceeds cannot be attributed to a single coin index — the caller then
        returns ``None`` so the ``*_result`` wrapper fails loud (ExtractError)
        instead of booking a zero-proceeds ghost.
        """
        pool_address = _canonical_pool_address(event)
        coin_addresses = _pool_coin_addresses(pool_address, self.chain)
        n_coins = len(coin_addresses)
        coin_addresses_lower = [a.lower() for a in coin_addresses]

        coin_index: int | None = None
        raw_amount: int | None = None

        # (1) Authoritative: pool-coin Transfer out of the pool. A single-coin
        # close has exactly one; multiple outflows touching this pool's coins in
        # the same receipt (a batched / zap tx) are ambiguous → fail closed rather
        # than guess which one is this close's proceeds (or silently fall through
        # to the weaker fallbacks).
        if coin_addresses_lower:
            outflows = self._find_pool_coin_outflows(receipt, pool_address, coin_addresses_lower)
            if len(outflows) > 1:
                logger.warning(
                    "Curve LP_CLOSE: %d pool-coin outflows for single-coin close on %s; ambiguous, failing closed",
                    len(outflows),
                    pool_address,
                )
                return None, []
            if len(outflows) == 1:
                coin_index, raw_amount = outflows[0]

        ev_idx = event.data.get("one_coin_index")
        ev_amt = event.data.get("one_coin_amount")

        # (2) Native-ETH fallback: exactly one native placeholder coin, no ERC-20
        # outflow matched → the withdrawn coin must be the native one. Only assume
        # native when the event carries NO explicit coin_index, or its coin_index
        # actually points at the native slot — otherwise a non-native withdrawal
        # whose Transfer was missed (e.g. stale registry coin address) must NOT be
        # mis-booked against the native coin; defer to the explicit index (path 3).
        if coin_index is None and ev_amt is not None and coin_addresses_lower:
            native_slots = [i for i, a in enumerate(coin_addresses_lower) if a == _NATIVE_ETH_PLACEHOLDER_LC]
            if len(native_slots) == 1 and (ev_idx is None or int(ev_idx) == native_slots[0]):
                coin_index, raw_amount = native_slots[0], int(ev_amt)

        # (3) Event-scalar fallback: explicit coin_index from a CryptoSwap / NG event
        # (used when the pool's coin map is unavailable).
        if coin_index is None and ev_idx is not None and ev_amt is not None:
            coin_index, raw_amount = int(ev_idx), int(ev_amt)

        if coin_index is None or raw_amount is None:
            return None, []

        # Defensive bound: a coin index beyond the pool's coin count (or beyond
        # Curve's 8-coin protocol max when the coin map is unavailable) is not
        # attributable — fail loud rather than allocate an unbounded list. A
        # mis-decoded 3-word event on a mislabeled pool could read a token-quantity
        # word (~1e18) as ``coin_index``; ``[0] * 1e18`` raises ``MemoryError``,
        # which is NOT an ``Exception`` subclass and would escape the enricher's
        # ``except Exception`` guard and crash the writer instead of degrading.
        max_coins = n_coins if n_coins else 8
        if coin_index < 0 or coin_index >= max_coins:
            logger.warning(
                "Curve LP_CLOSE: coin_index %s out of range (max %d) for pool %s; failing closed",
                coin_index,
                max_coins,
                pool_address,
            )
            return None, []

        size = max(n_coins, coin_index + 1)
        token_amounts = [0] * size
        token_amounts[coin_index] = raw_amount
        return token_amounts, []

    def _parse_swap_event(self, event: CurveEvent) -> SwapEventData | None:
        """Parse a swap event into typed data."""
        try:
            data = event.data
            return SwapEventData(
                buyer=data.get("buyer", ""),
                sold_id=data.get("sold_id", 0),
                tokens_sold=data.get("tokens_sold", 0),
                bought_id=data.get("bought_id", 0),
                tokens_bought=data.get("tokens_bought", 0),
                pool_address=data.get("pool_address", event.contract_address),
            )
        except Exception as e:
            logger.warning(f"Failed to parse SwapEventData: {e}")
            return None

    # =============================================================================
    # Extraction Methods (for Result Enrichment)
    # =============================================================================

    def extract_swap_amounts(
        self,
        receipt: dict[str, Any],
        *,
        expected_out: Decimal | None = None,
    ) -> "SwapAmounts | None":
        """Extract swap amounts from a transaction receipt.

        Uses ERC-20 Transfer events to identify token addresses, then resolves
        actual decimals via TokenResolver for accurate decimal conversion.
        Falls back to returning None if decimals cannot be resolved (rather than
        returning wildly wrong amounts).

        Args:
            receipt: Transaction receipt dict with 'logs' and 'from' fields
            expected_out: VIB-3203 Phase B — pre-slippage-discount quote in
                human (Decimal) units, sourced from
                ``ActionBundle.metadata["expected_output_human"]`` by the
                ResultEnricher. When provided and positive, realized
                ``slippage_bps`` is computed as
                ``(expected_out - amount_out_decimal) / expected_out * 10_000``.
                When absent, ``slippage_bps`` stays ``None`` (legacy behaviour).

        Returns:
            SwapAmounts dataclass if swap event found, None otherwise
        """
        from almanak.framework.execution.extracted_data import SwapAmounts

        try:
            result = self.parse_receipt(receipt)
            if not result.swap_events:
                return None

            swap = result.swap_events[0]
            amount_in = swap.tokens_sold
            amount_out = swap.tokens_bought

            # Find token addresses from ERC-20 Transfer events in the receipt
            token_in_addr, token_out_addr = self._find_swap_token_addresses(receipt)

            # Resolve actual decimals for accurate conversion
            decimals_in = self._resolve_decimals(token_in_addr)
            decimals_out = self._resolve_decimals(token_out_addr)

            # If we can't resolve decimals for either token, bail out rather
            # than returning wildly wrong amounts (e.g., 10^12x off for USDC)
            if decimals_in is None or decimals_out is None:
                logger.warning("Cannot compute Curve swap amounts: token decimals unknown")
                return None

            # Guard against malicious/bogus decimals values (ERC-20 max is uint8 = 255)
            if decimals_in > 77 or decimals_out > 77:
                logger.warning(f"Unreasonable decimals ({decimals_in}, {decimals_out}), refusing to compute")
                return None

            amount_in_decimal = Decimal(str(amount_in)) / Decimal(10**decimals_in)
            amount_out_decimal = Decimal(str(amount_out)) / Decimal(10**decimals_out)
            effective_price = amount_out_decimal / amount_in_decimal if amount_in_decimal > 0 else Decimal(0)

            # VIB-3203 Phase B: compute realized slippage when the enricher
            # supplied the pre-slippage quote. ``expected_out`` is vetted
            # upstream (``ResultEnricher._build_extract_kwargs`` already
            # rejects non-positive / non-finite values) so we only need to
            # guard against a zero ``amount_out_decimal`` here.
            slippage_bps: int | None = None
            if expected_out is not None and expected_out > 0 and amount_out_decimal > 0:
                realized = (expected_out - amount_out_decimal) / expected_out
                slippage_bps = int(realized * Decimal(10_000))

            return SwapAmounts(
                amount_in=amount_in,
                amount_out=amount_out,
                amount_in_decimal=amount_in_decimal,
                amount_out_decimal=amount_out_decimal,
                effective_price=effective_price,
                slippage_bps=slippage_bps,
                expected_out_decimal=expected_out,
                token_in=token_in_addr or f"token{swap.sold_id}",
                token_out=token_out_addr or f"token{swap.bought_id}",
            )

        except Exception as e:
            logger.warning(f"Failed to extract swap amounts: {e}")
            return None

    def _find_swap_token_addresses(self, receipt: dict[str, Any]) -> tuple[str, str]:
        """Find token_in and token_out addresses from ERC-20 Transfer events.

        Heuristic: token_in is the Transfer FROM the wallet (first),
        token_out is the Transfer TO the wallet (last).

        Args:
            receipt: Transaction receipt dict

        Returns:
            Tuple of (token_in_address, token_out_address), empty string if not found
        """
        wallet = receipt.get("from", "")
        if isinstance(wallet, bytes):
            wallet = "0x" + wallet.hex()
        wallet = str(wallet).lower()
        if not wallet:
            return ("", "")

        transfer_topic = EVENT_TOPICS["Transfer"].lower()
        token_in_addr = ""
        token_out_addr = ""

        for log in receipt.get("logs", []):
            topics = log.get("topics", [])
            if len(topics) < 3:
                continue

            first_topic = topics[0]
            if isinstance(first_topic, bytes):
                first_topic = "0x" + first_topic.hex()
            if str(first_topic).lower() != transfer_topic:
                continue

            log_from = HexDecoder.topic_to_address(topics[1])
            log_to = HexDecoder.topic_to_address(topics[2])
            token_address = log.get("address", "")
            if isinstance(token_address, bytes):
                token_address = "0x" + token_address.hex()
            token_address = str(token_address).lower()

            if log_from == wallet and not token_in_addr:
                token_in_addr = token_address
            if log_to == wallet:
                token_out_addr = token_address  # last Transfer TO wallet wins

        return (token_in_addr, token_out_addr)

    def _resolve_decimals(self, token_address: str) -> int | None:
        """Resolve token decimals via the token resolver.

        Returns None if the resolver is unavailable or the token is unknown.

        Args:
            token_address: Lowercase token address

        Returns:
            Token decimals (e.g. 6 for USDC, 18 for WETH), or None if unknown.
        """
        if not token_address:
            return None
        try:
            from almanak.framework.data.tokens import get_token_resolver

            resolver = get_token_resolver()
            token = resolver.resolve(token_address, self.chain)
            return token.decimals
        except Exception:
            logger.warning(f"Could not resolve decimals for {token_address}")
            return None

    def extract_position_id(self, receipt: dict[str, Any]) -> int | str | None:
        """Extract position identifier from LP transaction receipt.

        For Curve (pool-based LP, no NFT positions), returns the LP token
        contract address.  Unlike V3 DEXes where position_id is an NFT tokenId,
        Curve LP tokens are fungible ERC-20s — the LP token address is the
        stable identifier for the position.

        The minted LP token *amount* is available separately via
        ``extract_liquidity()``.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            LP token address as hex string, or None if not found
        """
        try:
            # Find the mint Transfer event (from zero address) and return the
            # emitting contract address — that is the LP token contract.
            zero_addr = "0x0000000000000000000000000000000000000000"
            transfer_topic = EVENT_TOPICS["Transfer"].lower()

            for log in receipt.get("logs", []):
                topics = log.get("topics", [])
                if len(topics) < 3:
                    continue

                first_topic = topics[0]
                if isinstance(first_topic, bytes):
                    first_topic = "0x" + first_topic.hex()
                first_topic = str(first_topic).lower()

                if first_topic != transfer_topic:
                    continue

                from_addr = HexDecoder.topic_to_address(topics[1])
                if from_addr.lower() == zero_addr:
                    lp_token_address = log.get("address", "")
                    if isinstance(lp_token_address, bytes):
                        lp_token_address = "0x" + lp_token_address.hex()
                    lp_token_address = str(lp_token_address).strip()
                    if lp_token_address.startswith("0x") and len(lp_token_address) == 42:
                        return lp_token_address
                    return None

            return None
        except Exception as e:
            logger.warning(f"Failed to extract position_id: {e}")
            return None

    def extract_liquidity(self, receipt: dict[str, Any]) -> Decimal | None:
        """Extract LP tokens minted from AddLiquidity transaction.

        Returns the LP token amount in **human-readable** form (e.g., ``Decimal("98.133")``)
        by dividing the raw wei value by 10^decimals. This matches the convention expected by
        the LP_CLOSE compiler, which treats the value as a human-readable amount and converts
        back to wei internally.

        Curve LP tokens always have 18 decimals. If the LP token address is found in the
        receipt, decimals are resolved via the token resolver; otherwise falls back to 18.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            LP token amount in human-readable Decimal, or None if not found
        """
        return self.extract_lp_tokens_received(receipt)

    def extract_lp_tokens_received(self, receipt: dict[str, Any]) -> Decimal | None:
        """Extract LP tokens received from AddLiquidity transaction.

        Looks for Transfer events from the zero address (mint).

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            LP token amount in human-readable Decimal, or None if not found
        """
        try:
            # Look for Transfer events from zero address (mint)
            zero_addr = "0x0000000000000000000000000000000000000000"
            transfer_topic = EVENT_TOPICS["Transfer"].lower()

            logs = receipt.get("logs", [])
            for log in logs:
                topics = log.get("topics", [])
                if len(topics) < 3:
                    continue

                first_topic = topics[0]
                if isinstance(first_topic, bytes):
                    first_topic = "0x" + first_topic.hex()
                first_topic = str(first_topic).lower()

                if first_topic != transfer_topic:
                    continue

                from_addr = HexDecoder.topic_to_address(topics[1])
                if from_addr.lower() == zero_addr:
                    data = HexDecoder.normalize_hex(log.get("data", ""))
                    lp_amount_raw = HexDecoder.decode_uint256(data, 0)

                    # Resolve LP token decimals (Curve LP tokens are always 18,
                    # but resolve to be safe)
                    lp_token_address = log.get("address", "")
                    if isinstance(lp_token_address, bytes):
                        lp_token_address = "0x" + lp_token_address.hex()
                    decimals = self._resolve_decimals(str(lp_token_address).lower())
                    if decimals is None:
                        # Protocol invariant: Curve LP tokens are always 18 decimals.
                        # This is NOT a silent arbitrary-token fallback (VIB-3164) —
                        # see CURVE_LP_TOKEN_DECIMALS for the rationale.
                        logger.warning(
                            f"Cannot resolve decimals for Curve LP token {lp_token_address}; "
                            f"using Curve protocol invariant ({CURVE_LP_TOKEN_DECIMALS})"
                        )
                        decimals = CURVE_LP_TOKEN_DECIMALS

                    return Decimal(lp_amount_raw) / Decimal(10**decimals)

            return None

        except Exception as e:
            logger.warning(f"Failed to extract LP tokens received: {e}")
            return None

    def extract_lp_open_data(self, receipt: dict[str, Any]) -> "LPOpenData | None":
        """Extract LP open data from an AddLiquidity transaction receipt (VIB-4968).

        Curve is a **fungible-LP (ERC20 LP-token) venue** — there is no NFT
        position, no tick bracket, and no per-position id. The single field
        the LP accounting handler genuinely needs from the open receipt is the
        canonical ``pool_address`` so ``handle_lp`` can book the LP_OPEN event
        (``_resolve_lp_pool_address`` step 1). Pre-VIB-4968 the parser had no
        ``extract_lp_open_data`` at all, so the receipt-extraction priority
        yielded nothing and — combined with the bare-label position-key tail —
        the handler dropped the event entirely (zero ``accounting_events`` rows
        for every Curve LP_OPEN).

        Directional null-contract (Empty ≠ Zero ≠ None, blueprint 27):

        - ``pool_address`` = the AddLiquidity event emitter (the Curve pool
          contract). Real ``0x`` address — chain data, most reliable.
        - ``position_id = 0`` — fungible LP has no NFT id. The handler's
          ``_resolve_lp_open_discriminator`` treats ``0`` as "no discriminator"
          and persists ``position_id = None`` (the faithful fungible-LP value).
        - ``tick_lower`` / ``tick_upper`` / ``liquidity`` / ``current_tick`` /
          ``position_hash`` stay ``None`` — Curve has no tick model and
          fabricating a bracket would be a correctness regression.
        - ``amount0`` / ``amount1`` carry the raw measured ``token_amounts``
          for the first two coins so the handler can scale them by token
          decimals. ``None`` (not ``0``) when the leg is genuinely absent.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            LPOpenData if an AddLiquidity event is found, None otherwise.
        """
        from almanak.framework.execution.extracted_data import LPOpenData

        try:
            result = self.parse_receipt(receipt)

            for event in result.events:
                if event.event_type != CurveEventType.ADD_LIQUIDITY:
                    continue

                # ``or []`` guards the decode ``else`` branch which carries no
                # ``token_amounts`` key (and a defensive None) — avoids len(None).
                token_amounts = event.data.get("token_amounts") or []
                # Empty ≠ Zero: a leg the AddLiquidity event simply did not
                # carry is ``None`` (unmeasured), never a fabricated ``0``.
                amount0 = token_amounts[0] if len(token_amounts) > 0 else None
                amount1 = token_amounts[1] if len(token_amounts) > 1 else None
                # VIB-5429 — capture coins beyond the first two (Curve 3/4-coin
                # pools). A single-sided deposit's unfunded coins are emitted as a
                # MEASURED ``0`` in the AddLiquidity ``token_amounts`` vector, so
                # carrying them lets the basis valuation see every coin instead of
                # silently dropping coin 2+ (symmetric with LPCloseData).
                additional_amounts = (
                    {i: token_amounts[i] for i in range(2, len(token_amounts))} if len(token_amounts) > 2 else None
                )

                # VIB-5429 — pool-coin-ordered symbols (same index order as the
                # ``token_amounts`` / ``amount0``/``amount1`` above), so the LP
                # accounting handler can value the per-coin deposit basis. A
                # Curve LP_OPEN carries no token0/token1 on the ledger row. ``[]``
                # for an unknown pool ⇒ ``None`` (handler keeps its legacy path).
                open_pool_address = _canonical_pool_address(event)
                coin_symbols = _pool_coin_symbols(open_pool_address, self.chain) or None

                return LPOpenData(
                    # Fungible LP: no NFT id. ``0`` is the canonical
                    # "no per-position discriminator" sentinel the handler
                    # collapses back to ``position_id=None``.
                    position_id=0,
                    amount0=amount0,
                    amount1=amount1,
                    additional_amounts=additional_amounts,
                    tick_lower=None,
                    tick_upper=None,
                    liquidity=None,
                    current_tick=None,
                    # VIB-4968 — canonical Curve pool address (the AddLiquidity
                    # event emitter IS the pool contract). Lets the LP
                    # accounting handler resolve a pool and book the event.
                    pool_address=open_pool_address,
                    position_hash=None,  # Curve has no V4 position-key hash.
                    coin_symbols=coin_symbols,
                )

            return None

        except Exception as e:
            logger.warning(f"Failed to extract lp_open_data: {e}")
            return None

    def _build_open_input_leg(self, coin_address: str, raw_amount: Any) -> "PrimitiveMoneyLeg | None":
        """Build one INPUT money leg for a funded Curve LP_OPEN coin (VIB-3587).

        Token identity is resolved FROM the pool's coin address (chain-truth,
        pool-coin order) rather than guessed from the intent's two-slot
        ``token0`` / ``token1`` — so a deposit of coin index 1+ lands on the
        coin it actually funds. When the static resolver misses the symbol, the
        leg keeps the lowercased coin ADDRESS (the known chain-truth identity the
        ledger treats opaquely) rather than dropping to ``""``; ``""`` is reserved
        for a genuinely unknown coin (no address), never a fabricated symbol.

        Amount is a human-unit ``MeasuredMoney`` (the VIB-5036 ledger contract)
        carrying Empty ≠ Zero (blueprint 27 §10.10) by construction. Mirrors the
        TraderJoe V2 ``_build_close_output_leg`` discipline:

        * a non-integer / missing raw → UNMEASURED;
        * a non-zero raw whose token decimals cannot be strictly resolved →
          UNMEASURED (never a wrongly-scaled value, NOT the 18-decimal
          best-effort fallback);
        * otherwise → measured human amount.

        Returns ``None`` for an UNFUNDED coin (raw ``0``): a single-sided
        deposit's zero-amount coins are simply ABSENT from the declared legs
        (Empty ≠ Zero — an unfunded coin is not a measured-zero leg), which is
        the whole point of VIB-3587. Callers must filter ``None``.
        """
        from almanak.connectors._strategy_base.primitive_money_leg import PrimitiveMoneyLeg
        from almanak.framework.accounting.measured import MeasuredMoney

        try:
            raw_int: int | None = int(raw_amount)
        except (TypeError, ValueError):
            raw_int = None

        # An unfunded coin (measured raw 0) is ABSENT from the declaration, not a
        # measured-zero leg. A None/garbage raw is also not declared (we cannot
        # assert it was funded). Either way: no leg, so the flat-tuple projection
        # never carries a fabricated zero for it.
        if not raw_int:
            return None

        # ``coin_address`` is the pool-ordered chain-truth token identity. Seed the
        # leg's token with the lowercased address so a static-resolver MISS still
        # carries the funded coin's identity — the ledger treats ``token`` opaquely
        # (symbol OR address; ``""`` only when unknown) per the ``PrimitiveMoneyLeg``
        # contract, so a known address is a measured fact, not a fabricated one. The
        # resolved symbol UPGRADES it when available; only a genuinely unknown coin
        # (``coin_address`` empty) leaves it ``""`` (Empty ≠ Zero).
        token_identity = str(coin_address).lower() if coin_address else ""
        decimals: int | None = None
        if coin_address and self.chain:
            try:
                from almanak.framework.data.tokens import get_token_resolver

                resolver = get_token_resolver()
                # skip_gateway/log_errors: accounting write hot path — resolve the
                # symbol AND decimals from the static registry without risking a
                # gateway round-trip stall (mirrors ledger ``_lp_amount_to_human``).
                info = resolver.resolve(coin_address, self.chain, log_errors=False, skip_gateway=True)
                token_identity = getattr(info, "symbol", "") or token_identity
                decimals = getattr(info, "decimals", None)
            except Exception as exc:  # noqa: BLE001 — fail to unmeasured, never raise on the accounting path
                logger.debug("Curve open leg: token resolve failed for %s on %s: %s", coin_address, self.chain, exc)

        if isinstance(decimals, int) and decimals >= 0:
            amount = MeasuredMoney.measured(Decimal(raw_int) / Decimal(10**decimals))
        else:
            amount = MeasuredMoney.unmeasured()
        return PrimitiveMoneyLeg.input(token_identity, amount)

    def extract_primitive_money_legs(self, receipt: dict[str, Any]) -> "PrimitiveMoneyLegs | None":
        """VIB-3587 — declare the LP_OPEN money legs as a typed ``PrimitiveMoneyLegs``
        the ledger dispatcher consumes directly (the Lido / TJ V2 US-009 pattern).

        Inverts the legacy control flow (blueprint 27 §6.6, 05 §7): instead of the
        ledger reverse-engineering an LP_OPEN's legs from ``LPOpenData.amount0`` /
        ``amount1`` (which it maps positionally onto ``token_in`` /``token_out`` of
        the pool's FIRST TWO coins), the connector DECLARES the coin(s) it actually
        funded on-chain. Curve is a multi-coin (2/3/4) venue with single-sided and
        non-leading-coin deposits, so the two-slot legacy guess is structurally
        wrong:

        * a single-sided deposit of coin 0 left coin 1 carrying a fabricated zero
          ``amount_out`` leg (and vice-versa) — a measured-zero where the coin was
          simply UNFUNDED (Empty ≠ Zero violation);
        * a deposit of coin index 2+ (e.g. USDT in 3pool, crvUSD in 4pool) was
          dropped entirely — ``amount0`` / ``amount1`` only ever carry coins 0/1.

        The declared legs are built FROM the AddLiquidity event's pool-coin-ordered
        ``token_amounts`` (chain truth) joined to the pool's ``coin_addresses``
        (same index order), emitting one INPUT leg per FUNDED coin and NOTHING for
        an unfunded coin. The dispatcher (``_extract_from_declared_legs``) projects
        leg0 → ``token_in`` / ``amount_in`` and leg1 → ``token_out`` / ``amount_out``
        — lane-symmetric with ``_extract_from_lp_open`` for a 2-coin deposit, and a
        single funded leg lands on ``token_in`` only (``token_out`` stays empty — no
        fabricated zero). A 3rd+ funded coin surfaces the dispatcher's documented
        "dropped leg" WARN rather than silently corrupting the trade tape.

        Returns ``None`` (→ legacy LP_OPEN fallback, byte-identical rows) when the
        receipt carries no AddLiquidity event, the pool's coin metadata is unknown,
        or no coin is funded — so non-Curve-resolvable receipts degrade unchanged.
        Never raises: any failure degrades to ``None`` rather than halting the live
        accounting writer.
        """
        from almanak.connectors._strategy_base.primitive_money_leg import PrimitiveMoneyLegs

        try:
            result = self.parse_receipt(receipt)

            for event in result.events:
                if event.event_type != CurveEventType.ADD_LIQUIDITY:
                    continue

                pool_address = _canonical_pool_address(event)
                coin_addresses = _pool_coin_addresses(pool_address, self.chain)
                if not coin_addresses:
                    # Without the pool-coin address map we cannot bind an amount to
                    # the coin it funds; fall back to the legacy two-slot path
                    # rather than guess.
                    return None

                token_amounts = event.data.get("token_amounts") or []

                # A stale / truncated ``CURVE_POOLS.coin_addresses`` (fewer coins
                # than the pool actually has) would let the loop below ignore a
                # FUNDED ``token_amounts`` slot beyond its length and still declare
                # a partial ``PrimitiveMoneyLegs``. Because declared legs BYPASS the
                # legacy path, that silently drops a funded coin from the trade tape.
                # If any positive amount cannot be bound to a known pool coin, fall
                # back to the legacy two-slot extraction rather than declare a lossy
                # subset.
                unbound_amounts = token_amounts[len(coin_addresses) :]
                if any(int(raw or 0) > 0 for raw in unbound_amounts):
                    logger.debug(
                        "Curve money-legs: %s funded token_amounts exceed %s coin "
                        "addresses for pool %s; falling back to legacy extraction",
                        len(token_amounts),
                        len(coin_addresses),
                        pool_address,
                    )
                    return None

                legs = []
                for idx, coin_address in enumerate(coin_addresses):
                    raw = token_amounts[idx] if idx < len(token_amounts) else None
                    leg = self._build_open_input_leg(coin_address, raw)
                    if leg is not None:
                        legs.append(leg)

                # No funded coin → no declaration (legacy fallback). A real
                # AddLiquidity always funds ≥1 coin, so this guards only a
                # degenerate / mis-decoded receipt.
                if not legs:
                    return None
                return PrimitiveMoneyLegs.of(*legs)

            return None
        except Exception as exc:  # noqa: BLE001 — never halt the accounting writer
            logger.warning(f"Failed to extract primitive_money_legs: {exc}")
            return None

    def extract_lp_close_data(self, receipt: dict[str, Any]) -> "LPCloseData | None":
        """Extract LP close data from transaction receipt.

        Looks for RemoveLiquidity, RemoveLiquidityOne, or RemoveLiquidityImbalance events.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            LPCloseData dataclass if liquidity removal found, None otherwise
        """
        from almanak.framework.execution.extracted_data import LPCloseData

        try:
            result = self.parse_receipt(receipt)

            # Look for removal events
            for event in result.events:
                if event.event_type not in (
                    CurveEventType.REMOVE_LIQUIDITY,
                    CurveEventType.REMOVE_LIQUIDITY_ONE,
                    CurveEventType.REMOVE_LIQUIDITY_IMBALANCE,
                ):
                    continue

                if event.event_type == CurveEventType.REMOVE_LIQUIDITY_ONE:
                    # VIB-5433 — single-coin withdrawal: proceeds are resolved from
                    # the coin Transfer (authoritative amount + index), since the
                    # event word layout is ambiguous (shared topic0) and old pools
                    # omit the coin_index entirely. A close we cannot attribute to a
                    # coin returns ``None`` here so the ``*_result`` wrapper fails
                    # loud (ExtractError) rather than booking a zero-proceeds ghost.
                    token_amounts, fees = self._resolve_one_coin_proceeds(event, receipt)
                    if token_amounts is None:
                        return None
                else:
                    # ``or []`` guards the decode ``else`` branch which carries
                    # no ``token_amounts`` key (and a defensive None).
                    token_amounts = event.data.get("token_amounts") or []
                    # Get fees if available. VIB-4470 — Empty ≠ Zero: emit
                    # ``None`` when the Curve event did not carry a fee for
                    # the leg (an unmeasured field is not a measured zero).
                    fees = event.data.get("fees") or []

                # Get amounts for token0 and token1. Empty ≠ Zero (matching the
                # fees below): a leg the event did not carry — e.g. a fail-closed
                # ``raw_data`` decode — is ``None`` (unmeasured), never a
                # fabricated measured ``0`` that would book a ghost LP_CLOSE. For a
                # single-coin close the non-withdrawn coins are a measured ``0``
                # (the withdrawal genuinely returned none of them — VIB-5433).
                amount0 = token_amounts[0] if len(token_amounts) > 0 else None
                amount1 = token_amounts[1] if len(token_amounts) > 1 else None
                fees0: int | None = fees[0] if len(fees) > 0 else None
                fees1: int | None = fees[1] if len(fees) > 1 else None

                # Capture additional amounts for 3/4-coin pools
                additional_amounts = None
                additional_fees = None
                if len(token_amounts) > 2:
                    additional_amounts = {i: token_amounts[i] for i in range(2, len(token_amounts))}
                if len(fees) > 2:
                    additional_fees = {i: fees[i] for i in range(2, len(fees))}

                # VIB-5429 — stamp the pool-coin-ordered symbols so the LP
                # accounting handler can price each fee/amount leg in USD. A
                # Curve close returns ALL N coins (no swap-style token_in/out)
                # and the fungible position_key carries no token descriptor, so
                # without this the handler cannot resolve decimals/prices and the
                # close payload (fees AND principal) collapses to NULLs. ``[]``
                # for an unknown pool ⇒ ``None`` (handler keeps its legacy path).
                close_pool_address = _canonical_pool_address(event)
                coin_symbols = _pool_coin_symbols(close_pool_address, self.chain) or None

                return LPCloseData(
                    amount0_collected=amount0,
                    amount1_collected=amount1,
                    fees0=fees0,
                    fees1=fees1,
                    liquidity_removed=None,  # LP tokens burned
                    additional_amounts=additional_amounts,
                    additional_fees=additional_fees,
                    coin_symbols=coin_symbols,
                    # VIB-4968 — stamp the canonical Curve pool address (the
                    # RemoveLiquidity* event emitter IS the pool contract).
                    # This is the chain-data-first source the LP accounting
                    # handler (`_resolve_lp_pool_address` step 1) needs to
                    # book the LP_CLOSE event; without it `handle_lp` could
                    # not resolve a pool and dropped the event entirely.
                    # ``_canonical_pool_address`` reads the decoder's stamped
                    # ``pool_address`` (RemoveLiquidity / Imbalance) and falls
                    # back to the event emitter for RemoveLiquidityOne.
                    pool_address=close_pool_address,
                )

            return None

        except Exception as e:
            logger.warning(f"Failed to extract lp_close_data: {e}")
            return None

    def extract_protocol_fees(self, receipt: dict[str, Any]) -> "ProtocolFees":
        """VIB-3495: Curve Finance LP protocol fee coverage audit.

        Curve NG pools encode ``fees`` arrays in AddLiquidity/RemoveLiquidity
        events, but these are LP-accrued fee amounts in token units — NOT a
        USD-denominated protocol fee. Additionally, Curve charges an admin fee
        (a cut of the LP fee) that is retained by the DAO, but this is not
        emitted in any receipt event. Converting token amounts to USD requires
        a price oracle unavailable at the receipt-parser layer.

        Returns a ProtocolFees with unavailable_reason so downstream
        attribution records "known-unknown" rather than "parser absent"
        (returning None was the pre-VIB-3495 behaviour).
        """
        from almanak.framework.execution.extracted_data import ProtocolFees

        # VIB-3495: Curve fee amounts in token units are available in NG
        # AddLiquidity/RemoveLiquidity events, but USD conversion requires
        # a price oracle. Old-style Twocrypto pools don't emit fees at all.
        return ProtocolFees(
            total_usd=None,
            unavailable_reason="protocol_fee_not_emitted_in_receipt",
        )

    # ---- VIB-5432: tagged-variant wrappers (ExtractOk / ExtractMissing /
    # ExtractError) ----------------------------------------------------------
    #
    # The raw ``extract_*`` methods above wrap their decode logic in
    # ``try/except: return None`` and ALSO return ``None`` for a genuinely
    # absent event. The ResultEnricher's legacy adapter cannot tell those two
    # ``None``s apart, so a decode CRASH is booked as a benign "no event" —
    # the ghost-position class (VIB-3159 / VIB-3368 / VIB-5368). These
    # ``extract_<field>_result`` wrappers, which the enricher prefers over the
    # raw method, restore the three-way signal: a crash becomes ``ExtractError``
    # (accounting-critical, never silently dropped), a genuinely absent event
    # stays ``ExtractMissing`` (benign), and a real value is ``ExtractOk``. The
    # raw methods keep their legacy return types so direct callers are
    # unchanged. See aerodrome / uniswap_v3 for the same pattern.
    #
    # CRITICAL (VIB-5432 field-level fix): Curve's raw extractors **swallow their
    # own exceptions** (``try/except Exception: return None``), so a field-level
    # decode CRASH on a PRESENT event returns ``None`` — indistinguishable from a
    # genuinely-absent event by the wrapper's ``try/except`` alone. (The aerodrome
    # reference shares this latent swallow but is out of scope here.) A bare
    # ``value is None -> ExtractMissing`` therefore does NOT close the ghost class
    # at the field level — only ``parse_receipt`` crashes are caught. To close it,
    # each wrapper disambiguates the two ``None``s with a PRESENCE signal derived
    # from the parsed ``ParseResult`` (or, for the LP-token mint extractors that
    # scan raw logs, from the same mint-Transfer scan they use):
    #   * relevant event/data PRESENT + extractor ``None``  -> ``ExtractError``
    #     (the ghost-position case — a present event we failed to decode);
    #   * relevant event/data ABSENT  + extractor ``None``  -> ``ExtractMissing``
    #     (benign, unchanged);
    #   * extractor returns a value                          -> ``ExtractOk``.

    def _strict_parse(self, receipt: dict[str, Any]) -> "ParseResult | ExtractError":
        """Run ``parse_receipt`` and return the parsed ``ParseResult`` on a clean
        parse, or an ``ExtractError`` if it crashes / returns ``None`` / reports
        failure. Returning the ``ParseResult`` lets each wrapper presence-check
        its own event(s) against chain truth (VIB-5432).

        Note: unlike the aerodrome equivalent, Curve's ``parse_receipt`` has
        ``return None`` paths, so a ``None`` result is guarded explicitly (an
        un-parseable receipt is an error, not a missing event)."""
        try:
            parsed = self.parse_receipt(receipt)
        except Exception as exc:  # noqa: BLE001 — malformed receipt shape
            return ExtractError(error=f"{type(exc).__name__}: {exc}", exception=exc)
        if parsed is None:
            return ExtractError(error="parse_receipt returned None")
        if not parsed.success:
            return ExtractError(error=parsed.error or "parse_receipt reported failure")
        return parsed

    @staticmethod
    def _event_present(parsed: "ParseResult", *event_types: CurveEventType) -> bool:
        """True when the parsed receipt carries at least one event of the given
        type(s) — the per-field PRESENCE discriminator (VIB-5432). A present
        event whose typed decode failed still counts as present (the event seam
        existed), so a swap/LP whose values we could not extract surfaces as an
        ``ExtractError`` rather than a benign ``ExtractMissing``."""
        return any(e.event_type in event_types for e in parsed.events)

    # VIB-5432 (round 2) — event types the parser STRUCTURALLY decodes into typed
    # ``token_amounts`` fields (``_decode_swap_data`` / ``_decode_add_liquidity_data``
    # / ``_decode_remove_liquidity_data`` / ``_decode_remove_liquidity_imbalance_data``).
    # On a clean decode each stamps its typed keys (``tokens_sold`` /
    # ``token_amounts`` / …) and NEVER a ``raw_data`` key; only the ``except`` (or
    # fail-closed truncation) fallback returns ``{"raw_data": data}``. So
    # ``raw_data`` on an event of one of THESE types is an exact decode-failure
    # sentinel.
    #
    # ``RemoveLiquidityImbalance`` JOINS this set (VIB-5433) — it now has a
    # structured decoder, so a present-but-undecodable imbalanced close must fail
    # loud rather than book a zero-proceeds ghost. ``RemoveLiquidityOne`` is
    # DELIBERATELY EXCLUDED: its decoder keeps ``raw_data`` BY DESIGN (single-coin
    # proceeds are resolved from the coin Transfer in ``_resolve_one_coin_proceeds``,
    # not from a structured ``token_amounts``), so flagging its ``raw_data`` would
    # convert every real single-coin withdrawal into a fatal accounting halt. Its
    # fail-loud path instead runs through ``extract_lp_close_data`` returning
    # ``None`` (present + None → ExtractError) when proceeds cannot be attributed.
    _STRUCTURALLY_DECODED_EVENT_TYPES: frozenset[CurveEventType] = frozenset(
        {
            CurveEventType.TOKEN_EXCHANGE,
            CurveEventType.TOKEN_EXCHANGE_UNDERLYING,
            CurveEventType.ADD_LIQUIDITY,
            CurveEventType.REMOVE_LIQUIDITY,
            CurveEventType.REMOVE_LIQUIDITY_IMBALANCE,
        }
    )

    @classmethod
    def _decode_fell_back(cls, parsed: "ParseResult", *event_types: CurveEventType) -> bool:
        """True when a PRESENT event of a structurally-decoded type fell back to the
        ``{"raw_data": ...}`` payload — i.e. ``_decode_*_data`` raised and the typed
        fields were never populated (VIB-5432 round 2 — CodeRabbit fail-closed fix).

        The presence + ``value is not None`` check in :meth:`_tag_presence` is NOT
        enough on its own: a present-but-undecodable event does not yield ``None``,
        it yields a FABRICATED DEFAULT downstream — a zero ``SwapEventData`` (every
        field ``.get(..., 0)``), or an ``LPOpenData`` / ``LPCloseData`` built from an
        empty ``token_amounts`` (``or []`` → ``amount0``/``amount1`` ``None``/``0``).
        The legacy wrapper would then emit ``ExtractOk(fabricated)`` for it — a
        silent ghost worse than ``ExtractError``. This sentinel reclassifies those.

        Scope is deliberately narrow (``_STRUCTURALLY_DECODED_EVENT_TYPES`` ∩
        ``event_types``): a present ``raw_data`` on those types CANNOT be a clean
        decode, so this never over-rejects a legitimately-zero-but-decoded value
        (decoded zero carries the typed key, not ``raw_data``). ``RemoveLiquidity``
        and ``RemoveLiquidityImbalance`` ARE in scope (structured ``token_amounts``
        decoders). It EXCLUDES ``RemoveLiquidityOne`` (VIB-5433), whose ``raw_data``
        is the by-design passthrough — its single-coin proceeds are resolved from
        the coin Transfer, so flagging its ``raw_data`` would convert every real
        single-coin withdrawal into a fatal accounting halt; its fail-loud path is
        ``extract_lp_close_data`` returning ``None`` (present + None → ExtractError)."""
        decodable = cls._STRUCTURALLY_DECODED_EVENT_TYPES.intersection(event_types)
        return any(e.event_type in decodable and "raw_data" in e.data for e in parsed.events)

    def _has_mint_transfer(self, receipt: dict[str, Any]) -> bool:
        """True when the receipt carries an ERC-20 *mint* Transfer (from the zero
        address) — the PRESENCE signal for the LP-token extractors
        (``extract_position_id`` / ``extract_liquidity`` /
        ``extract_lp_tokens_received``), which scan raw logs rather than the
        parsed Curve events (ERC-20 Transfer is not a registered Curve event).
        Mirrors their own scan so a present mint whose amount/address decode
        fails is disambiguated from a genuinely mint-less receipt (VIB-5432).

        Never raises: a malformed candidate log is skipped (the same log would
        make the extractor return ``None`` under its own swallow, and structural
        receipt corruption is already caught by ``_strict_parse``)."""
        zero_addr = "0x0000000000000000000000000000000000000000"
        transfer_topic = EVENT_TOPICS["Transfer"].lower()
        logs = receipt.get("logs", [])
        if not isinstance(logs, list | tuple):
            return False
        for log in logs:
            # A malformed entry (non-mapping log, or non-sequence ``topics``) is
            # skipped, never raised: ``parse_receipt`` can succeed while
            # ``_parse_log`` swallows individual malformed logs, so this presence
            # scan must tolerate the same shapes (e.g. ``{"logs": ["bad"]}``).
            if not isinstance(log, dict):
                continue
            topics = log.get("topics", [])
            if not isinstance(topics, list | tuple) or len(topics) < 3:
                continue
            first_topic = topics[0]
            if isinstance(first_topic, bytes):
                first_topic = "0x" + first_topic.hex()
            if str(first_topic).lower() != transfer_topic:
                continue
            try:
                from_addr = HexDecoder.topic_to_address(topics[1])
            except Exception:  # noqa: BLE001 — degenerate topic word; treat as non-mint
                continue
            if from_addr.lower() == zero_addr:
                return True
        return False

    @staticmethod
    def _tag_presence(
        value: Any,
        *,
        present: bool,
        field: str,
        missing_reason: str,
    ) -> ExtractResult[Any]:
        """Collapse a legacy ``value | None`` plus a PRESENCE flag into the
        three-variant result (VIB-5432): a value is ``ExtractOk``; ``None`` with
        the event present is the ghost-position ``ExtractError``; ``None`` with
        the event absent is the benign ``ExtractMissing``."""
        if value is not None:
            return ExtractOk(value=value)
        if present:
            return ExtractError(
                error=(
                    f"{field}: event present in receipt but extractor returned None "
                    "(field-level decode failure — would otherwise strand a ghost position)"
                )
            )
        return ExtractMissing(reason=missing_reason)

    def extract_swap_amounts_result(
        self,
        receipt: dict[str, Any],
        *,
        expected_out: Decimal | None = None,
    ) -> ExtractResult["SwapAmounts"]:
        """Fail-closed variant of :meth:`extract_swap_amounts` — see VIB-5432.

        Presence = a ``TokenExchange`` / ``TokenExchangeUnderlying`` event in the
        parsed receipt. A present swap whose amounts cannot be decoded (e.g.
        unresolvable token decimals) is an ``ExtractError``, not a benign miss.

        ``expected_out`` is forwarded for realized ``slippage_bps`` (VIB-3203).
        It MUST stay in this signature: the ResultEnricher calls this wrapper,
        and a missing kwarg would trip the enricher's TypeError fallback and
        silently drop ``expected_out``."""
        parsed = self._strict_parse(receipt)
        if isinstance(parsed, ExtractError):
            return parsed
        try:
            value = self.extract_swap_amounts(receipt, expected_out=expected_out)
        except Exception as exc:  # noqa: BLE001
            return ExtractError(error=f"{type(exc).__name__}: {exc}", exception=exc)
        # Fail-closed on a present-but-undecodable TokenExchange (VIB-5432 round 2):
        # ``_decode_swap_data`` swallows its decode crash to ``{"raw_data": ...}``,
        # so ``_parse_swap_event`` manufactures a zero-default ``SwapEventData`` and
        # ``extract_swap_amounts`` returns a FABRICATED zero ``SwapAmounts`` (non-None)
        # — ``_tag_presence`` would mis-tag that ``ExtractOk``. Reclassify to error.
        if self._decode_fell_back(parsed, CurveEventType.TOKEN_EXCHANGE, CurveEventType.TOKEN_EXCHANGE_UNDERLYING):
            return ExtractError(
                error=(
                    "swap_amounts: TokenExchange event present but decode fell back to raw_data "
                    "(fabricated zero-default SwapEventData would strand a ghost position)"
                )
            )
        present = self._event_present(parsed, CurveEventType.TOKEN_EXCHANGE, CurveEventType.TOKEN_EXCHANGE_UNDERLYING)
        return self._tag_presence(
            value, present=present, field="swap_amounts", missing_reason="no TokenExchange event in receipt"
        )

    def extract_position_id_result(self, receipt: dict[str, Any]) -> ExtractResult[Any]:
        """Fail-closed variant of :meth:`extract_position_id` — see VIB-5432.

        Presence = a mint Transfer (from the zero address). A present mint whose
        LP-token address is malformed yields ``None`` from the extractor, which
        is a decode failure -> ``ExtractError`` (not a missing event)."""
        parsed = self._strict_parse(receipt)
        if isinstance(parsed, ExtractError):
            return parsed
        try:
            value = self.extract_position_id(receipt)
        except Exception as exc:  # noqa: BLE001
            return ExtractError(error=f"{type(exc).__name__}: {exc}", exception=exc)
        return self._tag_presence(
            value,
            present=self._has_mint_transfer(receipt),
            field="position_id",
            missing_reason="no LP token mint Transfer in receipt",
        )

    def extract_liquidity_result(self, receipt: dict[str, Any]) -> ExtractResult["Decimal"]:
        """Fail-closed variant of :meth:`extract_liquidity` — see VIB-5432.

        Delegates to ``extract_lp_tokens_received``; presence = a mint Transfer
        (from the zero address). A present mint whose amount cannot be decoded
        is an ``ExtractError``."""
        parsed = self._strict_parse(receipt)
        if isinstance(parsed, ExtractError):
            return parsed
        try:
            value = self.extract_liquidity(receipt)
        except Exception as exc:  # noqa: BLE001
            return ExtractError(error=f"{type(exc).__name__}: {exc}", exception=exc)
        return self._tag_presence(
            value,
            present=self._has_mint_transfer(receipt),
            field="liquidity",
            missing_reason="no LP token mint Transfer in receipt",
        )

    def extract_lp_tokens_received_result(self, receipt: dict[str, Any]) -> ExtractResult["Decimal"]:
        """Fail-closed variant of :meth:`extract_lp_tokens_received` — see VIB-5432.

        Presence = a mint Transfer (from the zero address). A present mint whose
        amount cannot be decoded is an ``ExtractError``."""
        parsed = self._strict_parse(receipt)
        if isinstance(parsed, ExtractError):
            return parsed
        try:
            value = self.extract_lp_tokens_received(receipt)
        except Exception as exc:  # noqa: BLE001
            return ExtractError(error=f"{type(exc).__name__}: {exc}", exception=exc)
        return self._tag_presence(
            value,
            present=self._has_mint_transfer(receipt),
            field="lp_tokens_received",
            missing_reason="no LP token mint Transfer in receipt",
        )

    def extract_lp_open_data_result(self, receipt: dict[str, Any]) -> ExtractResult["LPOpenData"]:
        """Fail-closed variant of :meth:`extract_lp_open_data` — see VIB-5432.

        Presence = an ``AddLiquidity`` event in the parsed receipt. A present
        ``AddLiquidity`` we fail to assemble into ``LPOpenData`` is an
        ``ExtractError`` (the LP_OPEN ghost-position case)."""
        parsed = self._strict_parse(receipt)
        if isinstance(parsed, ExtractError):
            return parsed
        try:
            value = self.extract_lp_open_data(receipt)
        except Exception as exc:  # noqa: BLE001
            return ExtractError(error=f"{type(exc).__name__}: {exc}", exception=exc)
        # Fail-closed on a present-but-undecodable AddLiquidity (VIB-5432 round 2):
        # ``_decode_add_liquidity_data`` swallows its crash to ``{"raw_data": ...}``
        # (no ``token_amounts``), so ``extract_lp_open_data`` builds a non-None
        # ``LPOpenData`` with ``amount0``/``amount1`` ``None`` — a fabricated open
        # ``_tag_presence`` would mis-tag ``ExtractOk``. Reclassify to error.
        if self._decode_fell_back(parsed, CurveEventType.ADD_LIQUIDITY):
            return ExtractError(
                error=(
                    "lp_open_data: AddLiquidity event present but decode fell back to raw_data "
                    "(fabricated LPOpenData from missing token_amounts would strand a ghost position)"
                )
            )
        return self._tag_presence(
            value,
            present=self._event_present(parsed, CurveEventType.ADD_LIQUIDITY),
            field="lp_open_data",
            missing_reason="no AddLiquidity event in receipt",
        )

    def extract_primitive_money_legs_result(self, receipt: dict[str, Any]) -> ExtractResult["PrimitiveMoneyLegs"]:
        """Fail-closed variant of :meth:`extract_primitive_money_legs` — see VIB-5432.

        DOCUMENTED PER-FIELD EXCEPTION to the presence rule: unlike the other
        extractors, ``extract_primitive_money_legs`` returns ``None`` *by design*
        as a legacy fallback even when an ``AddLiquidity`` event IS present — when
        the pool's coin metadata is unknown, a funded amount cannot be bound to a
        coin, or no coin is funded (see its docstring). That ``None`` routes the
        LP_OPEN to the legacy two-slot path, which itself fail-closes via
        :meth:`extract_lp_open_data_result` (AddLiquidity present + decode crash ->
        ``ExtractError``). So the ghost-position guard for LP_OPEN lives on
        ``lp_open_data``; mapping this field's ``None`` to ``ExtractError`` would
        instead convert every unregistered-pool deposit — a common, benign
        fallback — into a fatal accounting halt. ``None`` is therefore the benign
        ``ExtractMissing`` here; only a genuine raise (caught below) is an error."""
        parsed = self._strict_parse(receipt)
        if isinstance(parsed, ExtractError):
            return parsed
        try:
            value = self.extract_primitive_money_legs(receipt)
        except Exception as exc:  # noqa: BLE001
            return ExtractError(error=f"{type(exc).__name__}: {exc}", exception=exc)
        if value is None:
            return ExtractMissing(reason="no declared money legs (legacy LP_OPEN fallback)")
        return ExtractOk(value=value)

    def extract_lp_close_data_result(self, receipt: dict[str, Any]) -> ExtractResult["LPCloseData"]:
        """Fail-closed variant of :meth:`extract_lp_close_data` — see VIB-5432.

        Presence = a ``RemoveLiquidity`` / ``RemoveLiquidityOne`` /
        ``RemoveLiquidityImbalance`` event in the parsed receipt. A present
        removal we fail to decode is an ``ExtractError`` (the LP_CLOSE
        ghost-position case)."""
        parsed = self._strict_parse(receipt)
        if isinstance(parsed, ExtractError):
            return parsed
        try:
            value = self.extract_lp_close_data(receipt)
        except Exception as exc:  # noqa: BLE001
            return ExtractError(error=f"{type(exc).__name__}: {exc}", exception=exc)
        # Fail-closed on a present-but-undecodable RemoveLiquidity /
        # RemoveLiquidityImbalance (VIB-5432 round 2, extended for VIB-5433):
        # those decoders swallow a crash to ``{"raw_data": ...}`` (no
        # ``token_amounts``), so ``extract_lp_close_data`` builds a non-None
        # ``LPCloseData`` with zero collected amounts — a fabricated close
        # ``_tag_presence`` would mis-tag ``ExtractOk``. Reclassify to error.
        # ``RemoveLiquidityOne`` is EXCLUDED here (its ``raw_data`` is by-design,
        # see :meth:`_decode_fell_back`); its fail-loud path is
        # ``extract_lp_close_data`` returning ``None`` (present + None →
        # ExtractError below) when single-coin proceeds cannot be attributed.
        if self._decode_fell_back(parsed, CurveEventType.REMOVE_LIQUIDITY, CurveEventType.REMOVE_LIQUIDITY_IMBALANCE):
            return ExtractError(
                error=(
                    "lp_close_data: RemoveLiquidity event present but decode fell back to raw_data "
                    "(fabricated LPCloseData from missing token_amounts would strand a ghost position)"
                )
            )
        present = self._event_present(
            parsed,
            CurveEventType.REMOVE_LIQUIDITY,
            CurveEventType.REMOVE_LIQUIDITY_ONE,
            CurveEventType.REMOVE_LIQUIDITY_IMBALANCE,
        )
        return self._tag_presence(
            value, present=present, field="lp_close_data", missing_reason="no RemoveLiquidity event in receipt"
        )

    def extract_protocol_fees_result(self, receipt: dict[str, Any]) -> ExtractResult["ProtocolFees"]:
        """Fail-closed variant of :meth:`extract_protocol_fees` — see VIB-5432.

        ``extract_protocol_fees`` always returns a ``ProtocolFees`` (with an
        ``unavailable_reason`` when fees aren't recoverable, never ``None``), so
        the only non-``ExtractOk`` outcome here is a genuine decode crash."""
        parsed = self._strict_parse(receipt)
        if isinstance(parsed, ExtractError):
            return parsed
        try:
            value = self.extract_protocol_fees(receipt)
        except Exception as exc:  # noqa: BLE001
            return ExtractError(error=f"{type(exc).__name__}: {exc}", exception=exc)
        return ExtractOk(value=value)

    # Backward compatibility methods
    def is_curve_event(self, topic: str | bytes) -> bool:
        """Check if a topic is a known Curve event.

        Args:
            topic: Event topic (supports bytes, hex string with/without 0x, any case)

        Returns:
            True if topic is a known Curve event
        """
        if isinstance(topic, bytes):
            topic = "0x" + topic.hex()
        else:
            topic = str(topic)
        if not topic.startswith("0x"):
            topic = "0x" + topic
        topic = topic.lower()
        return self.registry.is_known_event(topic)

    def get_event_type(self, topic: str | bytes) -> CurveEventType:
        """Get the event type for a topic.

        Args:
            topic: Event topic (supports bytes, hex string with/without 0x, any case)

        Returns:
            Event type or UNKNOWN
        """
        if isinstance(topic, bytes):
            topic = "0x" + topic.hex()
        else:
            topic = str(topic)
        if not topic.startswith("0x"):
            topic = "0x" + topic
        topic = topic.lower()
        return self.registry.get_event_type_from_topic(topic) or CurveEventType.UNKNOWN


__all__ = [
    "CurveReceiptParser",
    "CurveEvent",
    "CurveEventType",
    "SwapEventData",
    "AddLiquidityEventData",
    "RemoveLiquidityEventData",
    "ParseResult",
    "EVENT_TOPICS",
    "TOPIC_TO_EVENT",
    "EVENT_NAME_TO_TYPE",
    "TOKEN_EXCHANGE_TOPIC",
    "TOKEN_EXCHANGE_UNDERLYING_TOPIC",
]
