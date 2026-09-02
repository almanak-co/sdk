"""Parse Curve transaction receipts across StableSwap and CryptoSwap ABI variants."""

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from typing import TYPE_CHECKING, Any

from almanak.connectors._strategy_base.base import EventRegistry, HexDecoder, resolve_trading_wallet
from almanak.connectors._strategy_base.fail_closed_extract import FailClosedExtractMixin
from almanak.framework.data.tokens import TokenResolutionError, resolve_token_decimals
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


EVENT_TOPICS: dict[str, str] = {
    # StableSwap indices are signed int128; CryptoSwap indices are uint256.
    "TokenExchange": "0x8b3e96f2b889fa771c53c981b40daf005f63f637f1869f707052d15a3dd97140",
    "TokenExchangeCrypto": "0xb2e76ae99761dc136e598d4a629bb347eccb9532a5f8bbd72e18467c3c34cc98",
    "TokenExchangeUnderlying": "0xd013ca23e77a65003c2c659c5442c00c805371b7fc1ebd4c206c41d1536bd90b",
    # NG AddLiquidity events include per-coin fee arrays.
    "AddLiquidity2": "0x26f55a85081d24974e85c6c00045d0f0453991e95873f52bff0d21af4079a768",
    "AddLiquidity3": "0x423f6495a08fc652425cf4ed0d1f9e37e571d9b9529b1c1c23cce780b2e7df0d",
    "AddLiquidity4": "0x3f1915775e0c9a38a57a7bb7f1f9005f486fb904e1f84aa215364d567319a58d",
    # Pre-NG CryptoSwap events have a pool-level scalar, not a per-coin fee array.
    "AddLiquidityV2Crypto2": "0x540ab385f9b5d450a27404172caade516b3ba3f4be88239ac56a2ad1de2a1f5a",
    "AddLiquidityV2Crypto3": "0x96b486485420b963edd3fdec0b0195730035600feb7de6f544383d7950fa97ee",
    # Some StableSwap NG pools use dynamic amounts and fees arrays.
    "AddLiquidityDyn": "0x189c623b666b1b45b83d7178f39b8c087cb09774317ca2f53c2d3c3726f222a2",
    # NG RemoveLiquidity events include per-coin fee arrays.
    "RemoveLiquidity2": "0x7c363854ccf79623411f8995b362bce5eddff18c927edc6f5dbbb5e05819a82c",
    "RemoveLiquidity3": "0xa49d4cf02656aebf8c771f5a8585638a2a15ee6c97cf7205d4208ed7c1df252d",
    "RemoveLiquidity4": "0x9878ca375e106f2a43c3b599fc624568131c4c9a4ba66a14563715763be9d59d",
    "RemoveLiquidityDyn": "0x347ad828e58cbe534d8f6b67985d791360756b18f0d95fd9f197a66cc46480ea",
    # Pre-NG CryptoSwap RemoveLiquidity events do not include fees.
    "RemoveLiquidityV2Crypto2": "0xdd3c0336a16f1b64f172b7bb0dad5b2b3c7c76f91e8c4aafd6aae60dce800153",
    "RemoveLiquidityV2Crypto3": "0xd6cc314a0b1e3b2579f8e64248e82434072e8271290eef8ad0886709304195f5",
    # RemoveLiquidityOne has incompatible layouts across pool generations.
    # Legacy StableSwap: (token_amount, coin_amount), with no coin index.
    "RemoveLiquidityOneLegacy": "0x9e96dd3b997a2a257eec4df9bb6eaf626e206df5f543bd963682d143300be310",
    # This topic is shared by CryptoSwap (token_amount, coin_index, coin_amount)
    # and StableSwap-NG (token_amount, coin_amount, token_supply).
    "RemoveLiquidityOne": "0x5ad056f2e28a8cec232015406b843668c1e36cda598127ec3b8c59b8c72773a0",
    # CryptoSwap-NG: (token_amount, coin_index, coin_amount, approx_fee, packed_price_scale).
    "RemoveLiquidityOneNG": "0xe200e24d4a4c7cd367dd9befe394dc8a14e6d58c88ff5e2f512d65a9e0aa9c5c",
    # StableSwap imbalanced withdrawals encode fixed array size in topic0;
    # token_amounts remains positional by pool-coin index.
    "RemoveLiquidityImbalance": "0x2b5508378d7e19e0d5fa338419034731416c4f5b219a10379956f764317fd47e",  # [2]
    "RemoveLiquidityImbalance3": "0x173599dbf9c6ca6f7c3b590df07ae98a45d74ff54065505141e7de6c46a624c2",
    "RemoveLiquidityImbalance4": "0xb964b72f73f5ef5bf0fdc559b2fab9a7b12a39e47817a547f1f0aee47febd602",
    # StableSwap-NG also has a dynamic-array imbalance variant.
    "RemoveLiquidityImbalanceDyn": "0x3631c28b1f9dd213e0319fb167b554d76b6c283a41143eb400a0d1adb1af1755",
    "Transfer": "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
}

TOPIC_TO_EVENT: dict[str, str] = {v: k for k, v in EVENT_TOPICS.items()}

TOKEN_EXCHANGE_TOPIC = EVENT_TOPICS["TokenExchange"]
TOKEN_EXCHANGE_UNDERLYING_TOPIC = EVENT_TOPICS["TokenExchangeUnderlying"]

# Curve LP tokens are fixed at 18 decimals by protocol design.
CURVE_LP_TOKEN_DECIMALS = 18  # decimal-policy-exempt: Curve LP tokens are always 18 decimals

# Native-ETH withdrawals emit no ERC-20 Transfer and require event-scalar fallback.
CURVE_NATIVE_ETH_PLACEHOLDER = "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE"
_NATIVE_ETH_PLACEHOLDER_LC = CURVE_NATIVE_ETH_PLACEHOLDER.lower()

# Pool type disambiguates the incompatible three-word RemoveLiquidityOne layouts.
_CRYPTO_POOL_TYPES: frozenset[str] = frozenset({"tricrypto", "cryptoswap", "twocrypto"})
_STABLE_POOL_TYPES: frozenset[str] = frozenset({"stableswap", "metapool"})

# Fixed-array topic arity must agree with payload length before decoding.
_IMBALANCE_TOPIC_ARITY: dict[str, int] = {
    "RemoveLiquidityImbalance": 2,
    "RemoveLiquidityImbalance3": 3,
    "RemoveLiquidityImbalance4": 4,
}


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
    "AddLiquidityV2Crypto2": CurveEventType.ADD_LIQUIDITY,
    "AddLiquidityV2Crypto3": CurveEventType.ADD_LIQUIDITY,
    "AddLiquidityDyn": CurveEventType.ADD_LIQUIDITY,
    "RemoveLiquidity2": CurveEventType.REMOVE_LIQUIDITY,
    "RemoveLiquidity3": CurveEventType.REMOVE_LIQUIDITY,
    "RemoveLiquidity4": CurveEventType.REMOVE_LIQUIDITY,
    "RemoveLiquidityV2Crypto2": CurveEventType.REMOVE_LIQUIDITY,
    "RemoveLiquidityV2Crypto3": CurveEventType.REMOVE_LIQUIDITY,
    "RemoveLiquidityDyn": CurveEventType.REMOVE_LIQUIDITY,
    "RemoveLiquidityOneLegacy": CurveEventType.REMOVE_LIQUIDITY_ONE,
    "RemoveLiquidityOne": CurveEventType.REMOVE_LIQUIDITY_ONE,
    "RemoveLiquidityOneNG": CurveEventType.REMOVE_LIQUIDITY_ONE,
    "RemoveLiquidityImbalance": CurveEventType.REMOVE_LIQUIDITY_IMBALANCE,
    "RemoveLiquidityImbalance3": CurveEventType.REMOVE_LIQUIDITY_IMBALANCE,
    "RemoveLiquidityImbalance4": CurveEventType.REMOVE_LIQUIDITY_IMBALANCE,
    "RemoveLiquidityImbalanceDyn": CurveEventType.REMOVE_LIQUIDITY_IMBALANCE,
    "Transfer": CurveEventType.TRANSFER,
}


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
    sold_id: int
    tokens_sold: int
    bought_id: int
    tokens_bought: int
    pool_address: str

    def to_dict(self) -> dict[str, Any]:
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
        return {
            "success": self.success,
            "events": [e.to_dict() for e in self.events],
            "swap_events": [s.to_dict() for s in self.swap_events],
            "error": self.error,
            "transaction_hash": self.transaction_hash,
            "block_number": self.block_number,
            "transaction_success": self.transaction_success,
        }


def _canonical_pool_address(event: CurveEvent) -> str:
    """Return the lowercased event emitter, never a fabricated pool address."""
    addr = event.data.get("pool_address") or event.contract_address or ""
    return str(addr).lower()


# Optional metadata lookup keeps gateway access outside the parser.
PoolMetaLookup = Callable[[str, str], Any]


def _lookup_pool_meta(
    pool_meta_lookup: PoolMetaLookup | None,
    pool_address: str,
    chain: str,
) -> Any | None:
    """Resolve dynamic pool metadata without allowing lookup failures into accounting."""
    if pool_meta_lookup is None:
        return None
    try:
        return pool_meta_lookup(pool_address, chain)
    except Exception as exc:  # noqa: BLE001 — accounting path: degrade to legacy, never raise
        logger.debug("Curve dynamic pool-meta lookup failed for %s on %s: %s", pool_address, chain, exc)
        return None


def _pool_coin_addresses(
    pool_address: str,
    chain: str,
    pool_meta_lookup: PoolMetaLookup | None = None,
) -> list[str]:
    """Return pool-ordered coin addresses, or ``[]`` when metadata is unavailable."""
    if not pool_address:
        return []
    meta = _lookup_pool_meta(pool_meta_lookup, pool_address, chain)
    if meta is not None and meta.coin_addresses:
        return [str(a) for a in meta.coin_addresses]
    return []


def _pool_coin_symbols(
    pool_address: str,
    chain: str,
    pool_meta_lookup: PoolMetaLookup | None = None,
) -> list[str]:
    """Return pool-ordered coin symbols, or ``[]`` when metadata is unavailable."""
    if not pool_address:
        return []
    meta = _lookup_pool_meta(pool_meta_lookup, pool_address, chain)
    if meta is not None and meta.coin_symbols:
        return [str(c) for c in meta.coin_symbols]
    return []


def _pool_type(
    pool_address: str,
    chain: str,
    pool_meta_lookup: PoolMetaLookup | None = None,
) -> str:
    """Return pool type for ABI disambiguation, or ``""`` when unknown."""
    if not pool_address:
        return ""
    meta = _lookup_pool_meta(pool_meta_lookup, pool_address, chain)
    if meta is not None and meta.pool_type:
        return str(meta.pool_type).lower()
    return ""


class CurveReceiptParser(FailClosedExtractMixin):
    """Parser for Curve Finance transaction receipts."""

    # Multi-coin LP opens need pool-ordered legs rather than a two-token projection.
    EXTRA_EXTRACTIONS_BY_INTENT: dict[str, tuple[str, ...]] = {
        "LP_OPEN": ("primitive_money_legs",),
    }

    # Curve LPs are fungible and tickless; close fields are carried by lp_close_data.
    EXTRACTION_REMOVALS_BY_INTENT: dict[str, frozenset[str]] = {
        "LP_OPEN": frozenset({"tick_lower", "tick_upper"}),
        "LP_CLOSE": frozenset({"amount0_collected", "amount1_collected", "fees0", "fees1"}),
    }

    SUPPORTED_EXTRACTIONS: frozenset[str] = frozenset(
        {
            "swap_amounts",
            "position_id",
            "liquidity",
            "lp_tokens_received",
            "lp_open_data",
            "primitive_money_legs",
            "lp_close_data",
            "protocol_fees",
        }
    )

    def __init__(
        self,
        chain: str = "ethereum",
        *,
        pool_meta_lookup: PoolMetaLookup | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize with an optional live pool-metadata lookup."""
        self.chain = chain.lower()
        self._pool_meta_lookup = pool_meta_lookup
        self.registry = EventRegistry(EVENT_TOPICS, EVENT_NAME_TO_TYPE)

    def parse_receipt(
        self,
        receipt: dict[str, Any],
    ) -> ParseResult:
        """Parse a transaction receipt."""
        try:
            tx_hash = receipt.get("transactionHash", "")
            if isinstance(tx_hash, bytes):
                tx_hash = "0x" + tx_hash.hex()

            block_number = receipt.get("blockNumber", 0)
            logs = receipt.get("logs", [])
            status = receipt.get("status", 1)
            tx_success = status == 1

            # A reverted transaction may have no logs and must not look successful.
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

                    if parsed_event.event_type in (
                        CurveEventType.TOKEN_EXCHANGE,
                        CurveEventType.TOKEN_EXCHANGE_UNDERLYING,
                    ):
                        swap_data = self._parse_swap_event(parsed_event)
                        if swap_data:
                            swap_events.append(swap_data)

            tx_fmt = format_tx_hash(tx_hash)
            gas_fmt = format_gas_cost(receipt.get("gasUsed"))

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
        """Parse a recognized Curve log entry."""
        try:
            topics = log.get("topics", [])
            if not topics:
                return None

            first_topic = topics[0]
            if isinstance(first_topic, bytes):
                first_topic = "0x" + first_topic.hex()
            else:
                first_topic = str(first_topic)
            first_topic = first_topic.lower()

            event_name = self.registry.get_event_name(first_topic)
            if event_name is None:
                return None

            event_type = self.registry.get_event_type(event_name) or CurveEventType.UNKNOWN

            data = HexDecoder.normalize_hex(log.get("data", ""))

            contract_address = log.get("address", "")
            if isinstance(contract_address, bytes):
                contract_address = "0x" + contract_address.hex()

            topics_str = []
            for topic in topics:
                if isinstance(topic, bytes):
                    topics_str.append("0x" + topic.hex())
                else:
                    topics_str.append(str(topic))

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
        """Decode log data based on event type."""
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
            # Missing ABI words decode as zero, so truncated swaps must fail closed.
            normalized_data = HexDecoder.normalize_hex(data)
            if len(normalized_data) < 4 * 64:
                logger.warning(
                    "TokenExchange payload too short (%d hex chars, need >=256); failing closed to raw_data",
                    len(normalized_data),
                )
                return {"raw_data": data}

            buyer = HexDecoder.topic_to_address(topics[1]) if len(topics) > 1 else ""

            # StableSwap indices are signed int128; CryptoSwap indices are uint256.
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
        """Decode fixed, dynamic, and pre-NG AddLiquidity layouts."""
        try:
            provider = HexDecoder.topic_to_address(topics[1]) if len(topics) > 1 else ""
            pool_address = address.lower() if isinstance(address, str) else ""

            # Pre-NG Twocrypto: amounts[2], invariant, token_supply; no fees array.
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
                    "fees": [],
                    "invariant": invariant,
                    "token_supply": token_supply,
                    "pool_address": pool_address,
                }

            # Pre-NG CryptoSwap: amounts[3], pool-level scalar, token_supply.
            if event_name == "AddLiquidityV2Crypto3":
                # decode_uint256 defaults missing words to zero, so validate all five words.
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
                    "fees": [],
                    "invariant": invariant,
                    "token_supply": token_supply,
                    "pool_address": pool_address,
                }

            # Dynamic ABI head: amount offset, fee offset, invariant, supply.
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

            # Fixed NG layout has 2N+2 32-byte words: amounts, fees, invariant, supply.
            data_len = len(data)
            if data_len >= 640:
                n_coins = 4
            elif data_len >= 512:
                n_coins = 3
            else:
                n_coins = 2

            token_amounts = []
            for i in range(n_coins):
                token_amounts.append(HexDecoder.decode_uint256(data, i * 32))

            fees = []
            for i in range(n_coins):
                fees.append(HexDecoder.decode_uint256(data, (n_coins + i) * 32))

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
        """Decode fixed, dynamic, and pre-NG RemoveLiquidity layouts."""
        try:
            provider = HexDecoder.topic_to_address(topics[1]) if len(topics) > 1 else ""
            pool_address = address.lower() if isinstance(address, str) else ""

            # Pre-NG Twocrypto: amounts[2], token_supply; no fees array.
            if event_name == "RemoveLiquidityV2Crypto2":
                token_amounts = [
                    HexDecoder.decode_uint256(data, 0),
                    HexDecoder.decode_uint256(data, 32),
                ]
                token_supply = HexDecoder.decode_uint256(data, 64)
                return {
                    "provider": provider,
                    "token_amounts": token_amounts,
                    "fees": [],
                    "token_supply": token_supply,
                    "pool_address": pool_address,
                }

            # Pre-NG CryptoSwap: amounts[3], token_supply; no fees array.
            if event_name == "RemoveLiquidityV2Crypto3":
                # decode_uint256 defaults missing words to zero, so validate all four words.
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
                    "fees": [],
                    "token_supply": token_supply,
                    "pool_address": pool_address,
                }

            # Dynamic ABI head: amount offset, fee offset, supply.
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

            # Fixed NG layout has 2N+1 32-byte words: amounts, fees, supply.
            data_len = len(data)
            if data_len >= 576:
                n_coins = 4
            elif data_len >= 448:
                n_coins = 3
            else:
                n_coins = 2

            token_amounts = []
            for i in range(n_coins):
                token_amounts.append(HexDecoder.decode_uint256(data, i * 32))

            fees = []
            for i in range(n_coins):
                fees.append(HexDecoder.decode_uint256(data, (n_coins + i) * 32))

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
        """Decode best-effort scalars from incompatible RemoveLiquidityOne ABIs.

        Coin Transfers remain authoritative. The shared three-word topic is
        disambiguated by pool type; unknown pools leave scalars unmeasured rather
        than interpreting the wrong word as proceeds.
        """
        try:
            provider = HexDecoder.topic_to_address(topics[1]) if len(topics) > 1 else ""
            pool_address = address.lower() if isinstance(address, str) else ""
            nwords = len(HexDecoder.normalize_hex(data)) // 64
            token_amount = HexDecoder.decode_uint256(data, 0) if nwords >= 1 else None

            # Truncated variants remain unmeasured because missing words decode as zero.
            coin_index: int | None = None
            coin_amount: int | None = None
            if event_name == "RemoveLiquidityOneNG":
                if nwords >= 5:
                    coin_index = HexDecoder.decode_uint256(data, 32)
                    coin_amount = HexDecoder.decode_uint256(data, 64)
            elif event_name == "RemoveLiquidityOneLegacy":
                if nwords >= 2:
                    coin_amount = HexDecoder.decode_uint256(data, 32)
            elif nwords >= 5:
                coin_index = HexDecoder.decode_uint256(data, 32)
                coin_amount = HexDecoder.decode_uint256(data, 64)
            elif nwords == 3:
                # The shared topic's second word is index or amount depending on pool family.
                ptype = _pool_type(pool_address, self.chain, self._pool_meta_lookup)
                if ptype in _CRYPTO_POOL_TYPES:
                    coin_index = HexDecoder.decode_uint256(data, 32)
                    coin_amount = HexDecoder.decode_uint256(data, 64)
                elif ptype in _STABLE_POOL_TYPES:
                    coin_amount = HexDecoder.decode_uint256(data, 32)
            elif nwords == 2:
                coin_amount = HexDecoder.decode_uint256(data, 32)

            return {
                "provider": provider,
                "pool_address": pool_address,
                "one_coin": True,
                "token_amount": token_amount,
                "one_coin_index": coin_index,
                "one_coin_amount": coin_amount,
                "fees": [],
                # raw_data is intentional here; structured proceeds come from Transfers.
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
        """Decode fixed or dynamic StableSwap imbalanced withdrawals.

        Amounts are pool-coin ordered. Invalid envelopes fail closed to raw data
        rather than producing zero proceeds.
        """
        try:
            provider = HexDecoder.topic_to_address(topics[1]) if len(topics) > 1 else ""
            pool_address = address.lower() if isinstance(address, str) else ""

            if event_name == "RemoveLiquidityImbalanceDyn":
                # Validate the ABI envelope before trusting attacker-controlled offsets.
                total_bytes = len(HexDecoder.normalize_hex(data)) // 2
                if total_bytes < 4 * 32:
                    logger.warning("RemoveLiquidityImbalanceDyn head truncated (%d bytes); raw_data", total_bytes)
                    return {"raw_data": data}
                offset_amounts = HexDecoder.decode_uint256(data, 0)
                offset_fees = HexDecoder.decode_uint256(data, 32)

                def _decode_dyn_array(offset: int) -> list[int] | None:
                    if offset < 0 or offset + 32 > total_bytes:
                        return None
                    length = HexDecoder.decode_uint256(data, offset)
                    # Curve pools hold at most eight coins; bound both work and reads.
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

            # Fixed arrays contain amounts[N], fees[N], invariant, and supply.
            nwords = len(HexDecoder.normalize_hex(data)) // 64
            if nwords < 6 or (nwords - 2) % 2 != 0 or (nwords - 2) // 2 > 8:
                logger.warning(
                    "RemoveLiquidityImbalance payload unexpected (%d words; need 2N+2, 2<=N<=8); "
                    "failing closed to raw_data",
                    nwords,
                )
                return {"raw_data": data}
            n_coins = (nwords - 2) // 2
            # Topic and payload arity must agree or an amount could be read as a fee.
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
        """Return pool-coin Transfers out of the pool as ``(index, raw_amount)``.

        Multiple matches are left to the caller as an ambiguous batch. Malformed
        logs are skipped rather than failing receipt extraction.
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
        """Resolve single-coin proceeds into a pool-ordered amounts vector.

        A unique coin Transfer is authoritative, followed by native-ETH and
        explicit event-index fallbacks. Ambiguous or unattributable proceeds fail
        closed. Non-withdrawn coins are measured zero, not unmeasured.
        """
        pool_address = _canonical_pool_address(event)
        coin_addresses = _pool_coin_addresses(pool_address, self.chain, self._pool_meta_lookup)
        n_coins = len(coin_addresses)
        coin_addresses_lower = [a.lower() for a in coin_addresses]

        coin_index: int | None = None
        raw_amount: int | None = None

        # Multiple pool-coin outflows cannot be attributed safely in a batch or zap.
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

        # A native fallback is safe only when an explicit index is absent or agrees.
        if coin_index is None and ev_amt is not None and coin_addresses_lower:
            native_slots = [i for i, a in enumerate(coin_addresses_lower) if a == _NATIVE_ETH_PLACEHOLDER_LC]
            if len(native_slots) == 1 and (ev_idx is None or int(ev_idx) == native_slots[0]):
                coin_index, raw_amount = native_slots[0], int(ev_amt)

        if coin_index is None and ev_idx is not None and ev_amt is not None:
            coin_index, raw_amount = int(ev_idx), int(ev_amt)

        if coin_index is None or raw_amount is None:
            return None, []

        # Bound allocation because a misclassified amount word can look like a huge index.
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

    def extract_swap_amounts(
        self,
        receipt: dict[str, Any],
        *,
        expected_out: Decimal | None = None,
    ) -> "SwapAmounts | None":
        """Extract swap amounts using Transfer addresses and resolved decimals.

        ``expected_out`` is a human-unit pre-slippage quote used to calculate
        realized basis points. Unknown decimals fail closed instead of assuming 18.
        """
        from almanak.framework.execution.extracted_data import SwapAmounts

        try:
            result = self.parse_receipt(receipt)
            if not result.swap_events:
                return None

            swap = result.swap_events[0]
            amount_in = swap.tokens_sold
            amount_out = swap.tokens_bought

            token_in_addr, token_out_addr = self._find_swap_token_addresses(receipt)
            token_in_addr, token_out_addr = self._fill_native_swap_leg(
                result,
                swap,
                token_in_addr,
                token_out_addr,
            )

            decimals_in = self._resolve_decimals(token_in_addr)
            decimals_out = self._resolve_decimals(token_out_addr)

            # Assuming 18 decimals can misstate six-decimal tokens by 10^12.
            if decimals_in is None or decimals_out is None:
                logger.warning("Cannot compute Curve swap amounts: token decimals unknown")
                return None

            # Reject implausible token metadata before applying an extreme scale.
            if decimals_in > 77 or decimals_out > 77:
                logger.warning(f"Unreasonable decimals ({decimals_in}, {decimals_out}), refusing to compute")
                return None

            amount_in_decimal = Decimal(str(amount_in)) / Decimal(10**decimals_in)
            amount_out_decimal = Decimal(str(amount_out)) / Decimal(10**decimals_out)
            effective_price = amount_out_decimal / amount_in_decimal if amount_in_decimal > 0 else Decimal(0)

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
        """Infer tokens from the first Transfer from and last Transfer to the wallet.

        The effective wallet may be a Safe even when ``receipt["from"]`` is its signer.
        """
        wallet = resolve_trading_wallet(receipt)
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

    def _fill_native_swap_leg(
        self,
        parsed: ParseResult,
        swap: SwapEventData,
        token_in_addr: str,
        token_out_addr: str,
    ) -> tuple[str, str]:
        """Recover a raw-native swap leg that has no ERC-20 Transfer.

        Curve pools such as Ethereum's stETH pool hold raw ETH. The native leg
        moves as call value and therefore cannot be discovered by
        :meth:`_find_swap_token_addresses`. Recover only that missing identity
        from the emitting pool's ordered coin vector and the decoded
        ``sold_id``/``bought_id``. Existing Transfer-derived identities are never
        overwritten, and a missing non-native identity remains unresolved so the
        caller fails closed.

        ``TokenExchangeUnderlying`` indices address a metapool's combined coin
        space rather than its native coin vector, so this native-vector fallback
        does not apply to underlying swaps.
        """
        if token_in_addr and token_out_addr:
            return token_in_addr, token_out_addr

        first_swap_event = next(
            (
                event
                for event in parsed.events
                if event.event_type in (CurveEventType.TOKEN_EXCHANGE, CurveEventType.TOKEN_EXCHANGE_UNDERLYING)
            ),
            None,
        )
        if first_swap_event is None or first_swap_event.event_type is CurveEventType.TOKEN_EXCHANGE_UNDERLYING:
            return token_in_addr, token_out_addr

        pool_address = str(swap.pool_address or "").lower()
        if not pool_address:
            return token_in_addr, token_out_addr

        coin_addresses = _pool_coin_addresses(pool_address, self.chain, self._pool_meta_lookup)
        native_slots = [
            index for index, address in enumerate(coin_addresses) if address.lower() == _NATIVE_ETH_PLACEHOLDER_LC
        ]
        if len(native_slots) != 1:
            return token_in_addr, token_out_addr

        native_slot = native_slots[0]
        if not token_in_addr and swap.sold_id == native_slot:
            token_in_addr = _NATIVE_ETH_PLACEHOLDER_LC
        if not token_out_addr and swap.bought_id == native_slot:
            token_out_addr = _NATIVE_ETH_PLACEHOLDER_LC
        return token_in_addr, token_out_addr

    def _resolve_decimals(self, token_address: str) -> int | None:
        """Resolve token decimals, or return ``None`` when unknown."""
        if not token_address:
            return None
        try:
            return resolve_token_decimals(token_address, self.chain)
        except TokenResolutionError:
            logger.warning(f"Could not resolve decimals for {token_address}")
            return None

    def extract_position_id(self, receipt: dict[str, Any]) -> int | str | None:
        """Return the minted fungible LP token address as the position identifier."""
        try:
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
        """Return minted LP tokens in human units, not raw wei."""
        return self.extract_lp_tokens_received(receipt)

    def extract_lp_tokens_received(self, receipt: dict[str, Any]) -> Decimal | None:
        """Return a zero-address mint Transfer in human LP-token units."""
        try:
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

                    lp_token_address = log.get("address", "")
                    if isinstance(lp_token_address, bytes):
                        lp_token_address = "0x" + lp_token_address.hex()
                    decimals = self._resolve_decimals(str(lp_token_address).lower())
                    if decimals is None:
                        # This fallback is Curve's LP-token invariant, not a token default.
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
        """Extract an AddLiquidity event for a fungible, tickless Curve position.

        The emitter is the canonical pool address and ``position_id=0`` denotes no
        per-position discriminator. Absent amount slots remain unmeasured ``None``;
        emitted zero amounts remain measured zero.
        """
        from almanak.framework.execution.extracted_data import LPOpenData

        try:
            result = self.parse_receipt(receipt)

            for event in result.events:
                if event.event_type != CurveEventType.ADD_LIQUIDITY:
                    continue

                token_amounts = event.data.get("token_amounts") or []
                # Missing slots are unmeasured; zeros present in the event stay measured.
                amount0 = token_amounts[0] if len(token_amounts) > 0 else None
                amount1 = token_amounts[1] if len(token_amounts) > 1 else None
                additional_amounts = (
                    {i: token_amounts[i] for i in range(2, len(token_amounts))} if len(token_amounts) > 2 else None
                )

                # Symbol order must match token_amounts for per-coin valuation.
                open_pool_address = _canonical_pool_address(event)
                coin_symbols = _pool_coin_symbols(open_pool_address, self.chain, self._pool_meta_lookup) or None
                coin_addresses = _pool_coin_addresses(open_pool_address, self.chain, self._pool_meta_lookup) or None

                return LPOpenData(
                    position_id=0,
                    amount0=amount0,
                    amount1=amount1,
                    additional_amounts=additional_amounts,
                    tick_lower=None,
                    tick_upper=None,
                    liquidity=None,
                    current_tick=None,
                    pool_address=open_pool_address,
                    position_hash=None,
                    coin_symbols=coin_symbols,
                    coin_addresses=coin_addresses,
                )

            return None

        except Exception as e:
            logger.warning(f"Failed to extract lp_open_data: {e}")
            return None

    def _build_open_input_leg(self, coin_address: str, raw_amount: Any) -> "PrimitiveMoneyLeg | None":
        """Build an input leg from pool-ordered token identity and raw amount.

        Unfunded or invalid amounts produce no leg. Unknown decimals produce an
        unmeasured amount, never an assumed scale. A known address remains the
        token identity when symbol resolution misses.
        """
        from almanak.connectors._strategy_base.primitive_money_leg import PrimitiveMoneyLeg
        from almanak.framework.accounting.measured import MeasuredMoney

        try:
            raw_int: int | None = int(raw_amount)
        except (TypeError, ValueError):
            raw_int = None

        # An unfunded coin is absent from the declaration, not a measured-zero leg.
        if not raw_int:
            return None

        # Preserve the on-chain address if static symbol resolution misses.
        token_identity = str(coin_address).lower() if coin_address else ""
        decimals: int | None = None
        if coin_address and self.chain:
            try:
                from almanak.framework.data.tokens import get_token_resolver

                resolver = get_token_resolver()
                # Accounting writes must not block on a gateway round trip.
                info = resolver.resolve(coin_address, self.chain, log_errors=False, skip_gateway=True)
                token_identity = getattr(info, "symbol", "") or token_identity
                decimals = getattr(info, "decimals", None)
            except Exception as exc:  # noqa: BLE001 — unresolved metadata yields an unmeasured leg
                logger.debug("Curve open leg: token resolve failed for %s on %s: %s", coin_address, self.chain, exc)

        if isinstance(decimals, int) and decimals >= 0:
            amount = MeasuredMoney.measured(Decimal(raw_int) / Decimal(10**decimals))
        else:
            amount = MeasuredMoney.unmeasured()
        return PrimitiveMoneyLeg.input(token_identity, amount)

    def extract_primitive_money_legs(self, receipt: dict[str, Any]) -> "PrimitiveMoneyLegs | None":
        """Declare one input leg per funded pool-ordered AddLiquidity amount.

        Unknown or incomplete coin metadata, no funded coins, and extraction
        failures return ``None`` for the legacy path rather than guessing identity.
        """
        from almanak.connectors._strategy_base.primitive_money_leg import PrimitiveMoneyLegs

        try:
            result = self.parse_receipt(receipt)

            for event in result.events:
                if event.event_type != CurveEventType.ADD_LIQUIDITY:
                    continue

                pool_address = _canonical_pool_address(event)
                coin_addresses = _pool_coin_addresses(pool_address, self.chain, self._pool_meta_lookup)
                if not coin_addresses:
                    # Amounts cannot be bound safely without the pool's coin order.
                    return None

                token_amounts = event.data.get("token_amounts") or []

                # Never declare a partial set when metadata omits a funded coin.
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

                if not legs:
                    return None
                return PrimitiveMoneyLegs.of(*legs)

            return None
        except Exception as exc:  # noqa: BLE001 — degrade to the legacy accounting path
            logger.warning(f"Failed to extract primitive_money_legs: {exc}")
            return None

    def extract_lp_close_data(self, receipt: dict[str, Any]) -> "LPCloseData | None":
        """Extract pool-ordered proceeds from any supported liquidity removal."""
        from almanak.framework.execution.extracted_data import LPCloseData

        try:
            result = self.parse_receipt(receipt)

            for event in result.events:
                if event.event_type not in (
                    CurveEventType.REMOVE_LIQUIDITY,
                    CurveEventType.REMOVE_LIQUIDITY_ONE,
                    CurveEventType.REMOVE_LIQUIDITY_IMBALANCE,
                ):
                    continue

                if event.event_type == CurveEventType.REMOVE_LIQUIDITY_ONE:
                    # Transfers outrank ambiguous or index-free single-coin event layouts.
                    token_amounts, fees = self._resolve_one_coin_proceeds(event, receipt)
                    if token_amounts is None:
                        return None
                else:
                    token_amounts = event.data.get("token_amounts") or []
                    fees = event.data.get("fees") or []

                # Missing fields are unmeasured; non-withdrawn single-close coins are measured zero.
                amount0 = token_amounts[0] if len(token_amounts) > 0 else None
                amount1 = token_amounts[1] if len(token_amounts) > 1 else None
                fees0: int | None = fees[0] if len(fees) > 0 else None
                fees1: int | None = fees[1] if len(fees) > 1 else None

                additional_amounts = None
                additional_fees = None
                if len(token_amounts) > 2:
                    additional_amounts = {i: token_amounts[i] for i in range(2, len(token_amounts))}
                if len(fees) > 2:
                    additional_fees = {i: fees[i] for i in range(2, len(fees))}

                # Symbol order must match proceeds and fees for per-coin valuation.
                close_pool_address = _canonical_pool_address(event)
                coin_symbols = _pool_coin_symbols(close_pool_address, self.chain, self._pool_meta_lookup) or None
                coin_addresses = _pool_coin_addresses(close_pool_address, self.chain, self._pool_meta_lookup) or None

                return LPCloseData(
                    amount0_collected=amount0,
                    amount1_collected=amount1,
                    fees0=fees0,
                    fees1=fees1,
                    liquidity_removed=None,
                    additional_amounts=additional_amounts,
                    additional_fees=additional_fees,
                    coin_symbols=coin_symbols,
                    coin_addresses=coin_addresses,
                    pool_address=close_pool_address,
                )

            return None

        except Exception as e:
            logger.warning(f"Failed to extract lp_close_data: {e}")
            return None

    def extract_protocol_fees(self, receipt: dict[str, Any]) -> "ProtocolFees":
        """Report Curve receipt-level protocol fees as unavailable.

        Curve NG pools encode ``fees`` arrays in AddLiquidity/RemoveLiquidity
        events, but those are token-unit LP fees. The admin fee is not emitted,
        and this layer has no price oracle for USD conversion.
        """
        from almanak.framework.execution.extracted_data import ProtocolFees

        return ProtocolFees(
            total_usd=None,
            unavailable_reason="protocol_fee_not_emitted_in_receipt",
        )

    # Raw extractors conflate absent data and swallowed failures as None. Result
    # wrappers use event presence to preserve ExtractMissing versus ExtractError.

    def _parse_receipt_for_extract(self, receipt: dict[str, Any]) -> "ParseResult | ExtractError":
        """Parse strictly so an invalid receipt cannot appear merely event-free."""
        result = self._parse_receipt_result(receipt)
        if isinstance(result, ExtractError):
            return result
        if isinstance(result, ExtractOk) and isinstance(result.value, ParseResult):
            return result.value
        return ExtractError(error="unexpected parse result shape")

    @staticmethod
    def _event_present(parsed: "ParseResult", *event_types: CurveEventType) -> bool:
        """Return whether any requested event was present, including failed decodes."""
        return any(e.event_type in event_types for e in parsed.events)

    # raw_data signals failure for structured decoders. RemoveLiquidityOne is
    # excluded because it intentionally retains raw_data and resolves Transfers.
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
        """Detect structured decodes that produced raw data instead of typed fields.

        Without this check downstream defaults could turn failure into a fabricated
        zero-valued ``ExtractOk``. Legitimate decoded zeros retain typed keys.
        """
        decodable = cls._STRUCTURALLY_DECODED_EVENT_TYPES.intersection(event_types)
        return any(e.event_type in decodable and "raw_data" in e.data for e in parsed.events)

    def _has_mint_transfer(self, receipt: dict[str, Any]) -> bool:
        """Return whether raw logs contain a mint Transfer, skipping malformed logs."""
        zero_addr = "0x0000000000000000000000000000000000000000"
        transfer_topic = EVENT_TOPICS["Transfer"].lower()
        logs = receipt.get("logs", [])
        if not isinstance(logs, list | tuple):
            return False
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
            try:
                from_addr = HexDecoder.topic_to_address(topics[1])
            except Exception:  # noqa: BLE001 — malformed topics are not mint evidence
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
        """Tag a value, distinguishing absent events from failed extraction."""
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
        """Extract swap amounts, treating failure on a present swap as an error.

        ``expected_out`` is forwarded for realized slippage calculation.
        """
        parsed = self._parse_receipt_for_extract(receipt)
        if isinstance(parsed, ExtractError):
            return parsed
        try:
            value = self.extract_swap_amounts(receipt, expected_out=expected_out)
        except Exception as exc:  # noqa: BLE001
            return ExtractError(error=f"{type(exc).__name__}: {exc}", exception=exc)
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
        """Extract position ID, treating failure on a present mint as an error."""
        parsed = self._parse_receipt_for_extract(receipt)
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
        """Extract liquidity, treating failure on a present mint as an error."""
        parsed = self._parse_receipt_for_extract(receipt)
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
        """Extract received LP tokens, failing closed when a mint is present."""
        parsed = self._parse_receipt_for_extract(receipt)
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
        """Extract LP-open data, failing closed when AddLiquidity is present."""
        parsed = self._parse_receipt_for_extract(receipt)
        if isinstance(parsed, ExtractError):
            return parsed
        try:
            value = self.extract_lp_open_data(receipt)
        except Exception as exc:  # noqa: BLE001
            return ExtractError(error=f"{type(exc).__name__}: {exc}", exception=exc)
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
        """Extract declared legs while preserving the intentional legacy fallback.

        Unlike other fields, ``None`` with AddLiquidity present is benign when coin
        metadata cannot safely bind amounts; LP-open extraction guards decode failure.
        """
        parsed = self._parse_receipt_for_extract(receipt)
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
        """Extract LP-close data, failing closed when a removal is present."""
        parsed = self._parse_receipt_for_extract(receipt)
        if isinstance(parsed, ExtractError):
            return parsed
        try:
            value = self.extract_lp_close_data(receipt)
        except Exception as exc:  # noqa: BLE001
            return ExtractError(error=f"{type(exc).__name__}: {exc}", exception=exc)
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
        """Return unavailable fee metadata unless extraction itself fails."""
        parsed = self._parse_receipt_for_extract(receipt)
        if isinstance(parsed, ExtractError):
            return parsed
        try:
            value = self.extract_protocol_fees(receipt)
        except Exception as exc:  # noqa: BLE001
            return ExtractError(error=f"{type(exc).__name__}: {exc}", exception=exc)
        return ExtractOk(value=value)

    def is_curve_event(self, topic: str | bytes) -> bool:
        """Return whether a bytes or hex-string topic is a known Curve event."""
        if isinstance(topic, bytes):
            topic = "0x" + topic.hex()
        else:
            topic = str(topic)
        if not topic.startswith("0x"):
            topic = "0x" + topic
        topic = topic.lower()
        return self.registry.is_known_event(topic)

    def get_event_type(self, topic: str | bytes) -> CurveEventType:
        """Return the Curve event type for a bytes or hex-string topic."""
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
