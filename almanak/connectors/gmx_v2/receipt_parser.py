"""GMX v2 Receipt Parser.

This module provides parsing functionality for GMX v2 transaction receipts
and events, enabling extraction of position updates, order fills, and other
protocol events from on-chain data.

GMX v2 Events:
- OrderCreated: New order submitted
- OrderExecuted: Order filled successfully
- OrderCancelled: Order cancelled
- OrderFrozen: Order frozen due to error
- PositionIncrease: Position size increased
- PositionDecrease: Position size decreased
- DepositCreated: Liquidity deposit created
- DepositExecuted: Liquidity deposit executed
- WithdrawalCreated: Liquidity withdrawal created
- WithdrawalExecuted: Liquidity withdrawal executed

Example:
    from almanak.connectors.gmx_v2 import GMXv2ReceiptParser

    parser = GMXv2ReceiptParser()

    # Parse transaction receipt
    events = parser.parse_receipt(receipt)

    for event in events:
        if event.event_type == GMXv2EventType.POSITION_INCREASE:
            print(f"Position increased: {event.data}")
"""

import logging
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from typing import Any

from eth_abi import decode as abi_decode

from almanak.connectors._strategy_base.base import EventRegistry, HexDecoder
from almanak.connectors.gmx_v2 import market_catalog
from almanak.connectors.gmx_v2.addresses import GMX_V2, GMX_V2_TOKENS
from almanak.framework.execution.extract_result import (
    ExtractError,
    ExtractMissing,
    ExtractOk,
    ExtractResult,
)
from almanak.framework.execution.extracted_data import (
    AsyncOrderData,
    AsyncOrderKind,
    AsyncOrderStatus,
)
from almanak.framework.utils.log_formatters import format_gas_cost, format_tx_hash, format_usd

logger = logging.getLogger(__name__)


def _normalize_datetime_to_utc(dt: datetime) -> datetime:
    """Normalize a datetime to UTC timezone.

    If the datetime is naive (no tzinfo), assume it's UTC and add UTC timezone.
    If it already has a timezone, convert it to UTC.

    Args:
        dt: Datetime that may or may not have timezone info.

    Returns:
        Datetime with UTC timezone.
    """
    if dt.tzinfo is None:
        # Naive datetime - assume UTC
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


# =============================================================================
# Event Topic Signatures
# =============================================================================

# GMX v2 event name hashes (keccak256 of event name strings).
#
# GMX V2 uses a centralized EventEmitter contract that emits all protocol events
# via EventLog/EventLog1/EventLog2 event types. The event structure is:
#   topic[0] = keccak256 of EventLog/EventLog1/EventLog2 signature
#   topic[1] = keccak256 of event name string (indexed string eventNameHash)
#   topic[2+] = additional indexed parameters (for EventLog1/EventLog2)
#
# These hashes represent keccak256(eventName) and are matched against topic[1]
# to identify the specific GMX V2 event type.
EVENT_TOPICS: dict[str, str] = {
    # Order events
    "OrderCreated": "0xa7427759bfd3b941f14e687e129519da3c9b0046c5b9aaa290bb1dede63753b3",
    "OrderExecuted": "0x680f10f06595d3d707241f604672ec4b6ae50eb82728ec2f3c65f6789e897760",
    "OrderCancelled": "0xc7bb288dfd646d5b6c69d5099dd75b72f9c8c09ec9d40984c8ad8182357ae4b2",
    "OrderFrozen": "0x073fdba4e5f6b272d64068005c56e7adbaa6d8de035ca1f7b08422c6dc9fe606",
    "OrderUpdated": "0x84b670ed7b7ee8ccb350963a7dea39493daff6e7a43ab021a0e4ac2d652d359e",
    # Position events
    "PositionIncrease": "0xf94196ccb31f81a3e67df18f2a62cbfb50009c80a7d3c728a3f542e3abc5cb63",
    "PositionDecrease": "0x07d51b51b408d7c62dcc47cc558da5ce6a6e0fd129a427ebce150f52b0e5171a",
    "PositionFeesInfo": "0x8655f9667b66fc9e7efb1b6d44319284e1d7cffbff9751f70e263723a7080b83",
    "PositionFeesCollected": "0xe096982abd597114bdaa4a60612f87fabfcc7206aa12d61c50e7ba1e6c291100",
    # Deposit events
    "DepositCreated": "0xccee02d31cafad9001fbdc4dd5cf4957e152a372530316a7d856401e4c5d74bd",
    "DepositExecuted": "0x2856020a9644603d22d7b029b5649a55d708b88d9049150f146ac26c4107b880",
    "DepositCancelled": "0x70056e709adf36c0cb909b41ebecb620a44a31b6dc3867b92c2acf971785cdb5",
    # Withdrawal events
    "WithdrawalCreated": "0xfe021e2242f6c652ae824bc1428ee0fe7e8771a27295b9450792445dc456e37d",
    "WithdrawalExecuted": "0x7998e9258f1701223baddabfe884a5dc09ee23a6b31b57c9e8150d60c97707f8",
    "WithdrawalCancelled": "0xd523e46ff00e99354cd600fed716e4d5ed6346a3b5ff71e771307cac571b479e",
    # Market events
    "MarketCreated": "0xad5d762f1fc581b3e684cf095d93d3a2c10754f60124b09bec8bf3d76473baaf",
    "MarketPoolValueUpdated": "0x86834cf1ac14941d42cff9b14d28a052d57693ca16f3c3fc8a22efc050f96d97",
    # Oracle events
    "OraclePriceUpdated": "0xff32d64cc9a3a5a5937e62070e14f4cba3be96a7365d2d9e855df902f3c9c7f6",
    # Funding events
    "ClaimableFundingUpdated": "0x915eebf8297cc3f559ded968b9b253a3f043b1e6da5075ac2111083dc2c456fe",
    "FundingFeesClaimed": "0x7e7d869368a1c2fca23506342a50d40fcc45d39d44486d9319780252e3b66b2e",
    # Keeper execution-fee settlement. Emitted by the KEEPER transaction, not by
    # ours: GMX escrows ``executionFee`` (our ``msg.value``) in the OrderVault at
    # createOrder, then on execution ``GasUtils.payExecutionFee`` pays the keeper
    # and refunds the unspent remainder to the order's account. The escrow splits
    # EXACTLY — ``KeeperExecutionFee.executionFeeAmount`` is the portion we
    # actually consumed, and it is what the Cost Stack books as the venue
    # execution fee. Verified on Arbitrum mainnet keeper receipts:
    # 117808422544000 + 39858593456000 = 157667016000000 wei escrowed (open leg).
    "KeeperExecutionFee": "0x57f0018c9e19829fa2f55e53969d49e96f7bc3936cd7453806b7cd0eaf5593ca",
    "ExecutionFeeRefund": "0xe6db92bfa9c5428bfb1001511cc8b0376a77baf28e4b374949b9770bbbb799a0",
}

# Reverse lookup: topic -> event name
TOPIC_TO_EVENT: dict[str, str] = {v: k for k, v in EVENT_TOPICS.items()}

# EventEmitter's non-indexed payload is ``(msgSender, eventName, eventData)``.
# ``eventData`` is EventUtils.EventLogData, a tuple of seven keyed item groups.
_EVENT_LOG_DATA_ABI_TYPE = (
    "("
    "((string,address)[],(string,address[])[]),"
    "((string,uint256)[],(string,uint256[])[]),"
    "((string,int256)[],(string,int256[])[]),"
    "((string,bool)[],(string,bool[])[]),"
    "((string,bytes32)[],(string,bytes32[])[]),"
    "((string,bytes)[],(string,bytes[])[]),"
    "((string,string)[],(string,string[])[])"
    ")"
)

_TOKEN_DECIMALS_BY_SYMBOL = {
    "USDC": 6,
    "USDT": 6,
    "WBTC": 8,
    "BTC.b": 8,
    "WETH": 18,
    "WETH.e": 18,
    "WAVAX": 18,
}
_TOKEN_DECIMALS_BY_ADDRESS = {
    address.lower(): _TOKEN_DECIMALS_BY_SYMBOL[symbol]
    for tokens in GMX_V2_TOKENS.values()
    for symbol, address in tokens.items()
    if symbol in _TOKEN_DECIMALS_BY_SYMBOL
}


# =============================================================================
# Enums
# =============================================================================


class GMXv2EventType(Enum):
    """GMX v2 event types."""

    # Order events
    ORDER_CREATED = "ORDER_CREATED"
    ORDER_EXECUTED = "ORDER_EXECUTED"
    ORDER_CANCELLED = "ORDER_CANCELLED"
    ORDER_FROZEN = "ORDER_FROZEN"
    ORDER_UPDATED = "ORDER_UPDATED"

    # Position events
    POSITION_INCREASE = "POSITION_INCREASE"
    POSITION_DECREASE = "POSITION_DECREASE"
    POSITION_FEES_INFO = "POSITION_FEES_INFO"
    POSITION_FEES_COLLECTED = "POSITION_FEES_COLLECTED"

    # Deposit/Withdrawal events
    DEPOSIT_CREATED = "DEPOSIT_CREATED"
    DEPOSIT_EXECUTED = "DEPOSIT_EXECUTED"
    DEPOSIT_CANCELLED = "DEPOSIT_CANCELLED"
    WITHDRAWAL_CREATED = "WITHDRAWAL_CREATED"
    WITHDRAWAL_EXECUTED = "WITHDRAWAL_EXECUTED"
    WITHDRAWAL_CANCELLED = "WITHDRAWAL_CANCELLED"

    # Market events
    MARKET_CREATED = "MARKET_CREATED"
    MARKET_POOL_VALUE_UPDATED = "MARKET_POOL_VALUE_UPDATED"

    # Oracle events
    ORACLE_PRICE_UPDATED = "ORACLE_PRICE_UPDATED"

    # Funding events
    CLAIMABLE_FUNDING_UPDATED = "CLAIMABLE_FUNDING_UPDATED"
    FUNDING_FEES_CLAIMED = "FUNDING_FEES_CLAIMED"

    # Keeper execution-fee settlement events (keeper transaction)
    KEEPER_EXECUTION_FEE = "KEEPER_EXECUTION_FEE"
    EXECUTION_FEE_REFUND = "EXECUTION_FEE_REFUND"

    # Unknown
    UNKNOWN = "UNKNOWN"


# Mapping from event name to event type
EVENT_NAME_TO_TYPE: dict[str, GMXv2EventType] = {
    "OrderCreated": GMXv2EventType.ORDER_CREATED,
    "OrderExecuted": GMXv2EventType.ORDER_EXECUTED,
    "OrderCancelled": GMXv2EventType.ORDER_CANCELLED,
    "OrderFrozen": GMXv2EventType.ORDER_FROZEN,
    "OrderUpdated": GMXv2EventType.ORDER_UPDATED,
    "PositionIncrease": GMXv2EventType.POSITION_INCREASE,
    "PositionDecrease": GMXv2EventType.POSITION_DECREASE,
    "PositionFeesInfo": GMXv2EventType.POSITION_FEES_INFO,
    "PositionFeesCollected": GMXv2EventType.POSITION_FEES_COLLECTED,
    "DepositCreated": GMXv2EventType.DEPOSIT_CREATED,
    "DepositExecuted": GMXv2EventType.DEPOSIT_EXECUTED,
    "DepositCancelled": GMXv2EventType.DEPOSIT_CANCELLED,
    "WithdrawalCreated": GMXv2EventType.WITHDRAWAL_CREATED,
    "WithdrawalExecuted": GMXv2EventType.WITHDRAWAL_EXECUTED,
    "WithdrawalCancelled": GMXv2EventType.WITHDRAWAL_CANCELLED,
    "MarketCreated": GMXv2EventType.MARKET_CREATED,
    "MarketPoolValueUpdated": GMXv2EventType.MARKET_POOL_VALUE_UPDATED,
    "OraclePriceUpdated": GMXv2EventType.ORACLE_PRICE_UPDATED,
    "ClaimableFundingUpdated": GMXv2EventType.CLAIMABLE_FUNDING_UPDATED,
    "FundingFeesClaimed": GMXv2EventType.FUNDING_FEES_CLAIMED,
    "KeeperExecutionFee": GMXv2EventType.KEEPER_EXECUTION_FEE,
    "ExecutionFeeRefund": GMXv2EventType.EXECUTION_FEE_REFUND,
}

# VIB-6061 — the three keeper-receipt events the execution-fee settlement is read
# from, mapped to the EventUtils payload name each decodes under. A dict rather
# than a branch chain keeps the decode loop one shape per event kind, which is
# what took ``_select_execution_fee_events`` back under the complexity gate.
_EXECUTION_FEE_EVENT_NAMES: dict[GMXv2EventType, str] = {
    GMXv2EventType.ORDER_EXECUTED: "OrderExecuted",
    GMXv2EventType.KEEPER_EXECUTION_FEE: "KeeperExecutionFee",
    GMXv2EventType.EXECUTION_FEE_REFUND: "ExecutionFeeRefund",
}


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class GMXv2Event:
    """Parsed GMX v2 event.

    Attributes:
        event_type: Type of event
        event_name: Name of event (e.g., "PositionIncrease")
        log_index: Index of log in transaction
        transaction_hash: Transaction hash
        block_number: Block number
        contract_address: Contract that emitted event
        data: Parsed event data
        raw_topics: Raw event topics
        raw_data: Raw event data
        timestamp: Event timestamp
    """

    event_type: GMXv2EventType
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

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "GMXv2Event":
        """Create from dictionary."""
        # Parse and normalize timestamp to UTC
        if "timestamp" in data:
            timestamp = _normalize_datetime_to_utc(datetime.fromisoformat(data["timestamp"]))
        else:
            timestamp = datetime.now(UTC)

        return cls(
            event_type=GMXv2EventType(data["event_type"]),
            event_name=data["event_name"],
            log_index=data["log_index"],
            transaction_hash=data["transaction_hash"],
            block_number=data["block_number"],
            contract_address=data["contract_address"],
            data=data["data"],
            raw_topics=data.get("raw_topics", []),
            raw_data=data.get("raw_data", ""),
            timestamp=timestamp,
        )


_INTENT_ASYNC_ORDER_KINDS = {
    "PERP_OPEN": AsyncOrderKind.INCREASE,
    "PERP_CLOSE": AsyncOrderKind.DECREASE,
}
_RAW_ASYNC_ORDER_KINDS = {
    0: AsyncOrderKind.SWAP,
    1: AsyncOrderKind.SWAP,
    2: AsyncOrderKind.INCREASE,
    3: AsyncOrderKind.INCREASE,
    4: AsyncOrderKind.DECREASE,
    5: AsyncOrderKind.DECREASE,
    6: AsyncOrderKind.DECREASE,
    7: AsyncOrderKind.LIQUIDATION,
}


def _async_order_kind(intent_type: str | None, raw_order_type: Any) -> AsyncOrderKind:
    """Resolve an order kind, preferring the runner's authoritative intent."""
    intent_kind = _INTENT_ASYNC_ORDER_KINDS.get(intent_type) if intent_type is not None else None
    if intent_kind is not None:
        return intent_kind
    if not isinstance(raw_order_type, int) or isinstance(raw_order_type, bool):
        return AsyncOrderKind.UNKNOWN
    return _RAW_ASYNC_ORDER_KINDS.get(raw_order_type, AsyncOrderKind.UNKNOWN)


# GMX V2 ``Order.Type`` enum tops out at 7 (Liquidation). A decoded order type
# above this bound is not a real order kind — it is the fingerprint of the
# VIB-3873 misread class, where a fixed-word decode reads a dynamic-struct ABI
# offset (e.g. 32, 160) or an unrelated field as ``orderType``. The keyed
# EventUtils decode reads ``orderType`` BY NAME, so a real production payload
# can never exceed the bound; a value that does means the decode is wrong.
GMX_MAX_ORDER_TYPE = 7


class GMXOrderTypeError(ValueError):
    """Decoded GMX ``order_type`` exceeded the ``Order.Type`` enum bound (>7).

    This is the VIB-3873 tripwire: a flat-word decode of the dynamic EventUtils
    payload yields garbage (an ABI offset like 32 or 160, or an unrelated word)
    where a real order type belongs. Failing loud here stops fill economics
    from being booked against a misdecoded order instead of silently accepting
    the garbage as a valid order kind.
    """


def _checked_order_type(value: Any, event_name: str) -> int | None:
    """Return an in-range GMX order type, or raise the VIB-3873 tripwire.

    ``None`` (field absent) stays ``None`` — Empty != Zero. A non-int / bool
    value is treated as absent. An int outside ``[0, GMX_MAX_ORDER_TYPE]`` is a
    decode error and raises :class:`GMXOrderTypeError`.
    """
    if value is None:
        return None
    if not isinstance(value, int) or isinstance(value, bool):
        return None
    if value < 0 or value > GMX_MAX_ORDER_TYPE:
        raise GMXOrderTypeError(
            f"GMX {event_name} decoded order_type={value} exceeds the Order.Type "
            f"enum max ({GMX_MAX_ORDER_TYPE}) — dynamic EventUtils payload misread "
            "(VIB-3873)"
        )
    return value


def _keyed_order_created_fields(event: GMXv2Event) -> dict[str, Any]:
    """Decode OrderCreated settlement fields from the keyed EventUtils payload.

    ``event.data``'s flat fields come from the legacy fixed-word decode, which
    does NOT match production EventEmitter payloads — their leading words are
    dynamic-struct offsets, so "market"/"collateral"/"size"/"is_long" read as
    offset garbage (the VIB-3873 misread class; reproduced live 2026-07-25:
    requested market decoded as ``0x…a0``). Settlement identity must come from
    the keyed payload; a field that cannot be decoded stays absent so the
    settlement barrier measures loudly instead of comparing against garbage.
    """
    raw = str(event.raw_data or "").removeprefix("0x")
    items = GMXv2ReceiptParser._decode_event_utils_data(raw, "OrderCreated")
    if items is None:
        return {}
    return {
        "market": items["addresses"].get("market"),
        "initial_collateral_token": items["addresses"].get("initialCollateralToken"),
        "order_type": items["uints"].get("orderType"),
        "size_delta_usd": items["uints"].get("sizeDeltaUsd"),
        "is_long": items["bools"].get("isLong"),
    }


def _async_order_from_created_event(
    event: GMXv2Event,
    *,
    intent_type: str | None,
    transaction_hash: str,
) -> ExtractOk[AsyncOrderData] | ExtractError:
    """Convert one OrderCreated event into receipt-measured settlement data."""
    key = event.data.get("key")
    if not isinstance(key, str) or re.fullmatch(r"0x[0-9a-f]{64}", key) is None or int(key, 16) == 0:
        return ExtractError(
            error=(
                "OrderCreated event did not contain an exact non-zero bytes32 key "
                f"(tx={transaction_hash or 'unknown'}, log_index={event.log_index})"
            )
        )

    keyed = _keyed_order_created_fields(event)
    raw_size_delta = keyed.get("size_delta_usd")
    direction = keyed.get("is_long")
    market = str(keyed.get("market") or "") or None
    collateral_token = str(keyed.get("initial_collateral_token") or "") or None
    return ExtractOk(
        value=AsyncOrderData(
            protocol="gmx_v2",
            order_id=key,
            status=AsyncOrderStatus.PENDING,
            kind=_async_order_kind(intent_type, keyed.get("order_type")),
            market=market,
            collateral_token=collateral_token,
            is_long=direction if isinstance(direction, bool) else None,
            size_delta_usd=(
                Decimal(int(raw_size_delta)) / Decimal(10**30)
                if isinstance(raw_size_delta, int) and not isinstance(raw_size_delta, bool)
                else None
            ),
        )
    )


@dataclass
class PositionIncreaseData:
    """Parsed data from PositionIncrease event.

    Attributes:
        key: Position key
        account: Account address
        market: Market address
        collateral_token: Collateral token address
        is_long: Position direction
        size_in_usd: New position size in USD
        size_in_tokens: New position size in tokens
        collateral_amount: New collateral amount
        borrowing_factor: Current borrowing factor
        funding_fee_amount_per_size: Funding fee per size
        long_token_claimable_funding_amount_per_size: Long token funding
        short_token_claimable_funding_amount_per_size: Short token funding
        execution_price: Execution price
        index_token_price_max: Max index token price
        index_token_price_min: Min index token price
        collateral_token_price_max: Max collateral token price
        collateral_token_price_min: Min collateral token price
        size_delta_usd: Size change in USD
        size_delta_in_tokens: Size change in tokens
        collateral_delta_amount: Collateral change
        price_impact_usd: Price impact in USD
        price_impact_diff_usd: Price impact difference in USD
        order_key: Associated order key
        order_type: Order type
    """

    key: str
    account: str
    market: str
    collateral_token: str
    is_long: bool
    size_in_usd: Decimal
    size_in_tokens: Decimal
    collateral_amount: Decimal
    borrowing_factor: Decimal = Decimal("0")
    funding_fee_amount_per_size: Decimal = Decimal("0")
    long_token_claimable_funding_amount_per_size: Decimal = Decimal("0")
    short_token_claimable_funding_amount_per_size: Decimal = Decimal("0")
    execution_price: Decimal = Decimal("0")
    index_token_price_max: Decimal = Decimal("0")
    index_token_price_min: Decimal = Decimal("0")
    collateral_token_price_max: Decimal = Decimal("0")
    collateral_token_price_min: Decimal = Decimal("0")
    size_delta_usd: Decimal = Decimal("0")
    size_delta_in_tokens: Decimal = Decimal("0")
    collateral_delta_amount: Decimal = Decimal("0")
    price_impact_usd: Decimal = Decimal("0")
    price_impact_diff_usd: Decimal = Decimal("0")
    order_key: str = ""
    order_type: int = 0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "key": self.key,
            "account": self.account,
            "market": self.market,
            "collateral_token": self.collateral_token,
            "is_long": self.is_long,
            "size_in_usd": str(self.size_in_usd),
            "size_in_tokens": str(self.size_in_tokens),
            "collateral_amount": str(self.collateral_amount),
            "borrowing_factor": str(self.borrowing_factor),
            "funding_fee_amount_per_size": str(self.funding_fee_amount_per_size),
            "execution_price": str(self.execution_price),
            "index_token_price_max": str(self.index_token_price_max),
            "index_token_price_min": str(self.index_token_price_min),
            "collateral_token_price_max": str(self.collateral_token_price_max),
            "collateral_token_price_min": str(self.collateral_token_price_min),
            "size_delta_usd": str(self.size_delta_usd),
            "size_delta_in_tokens": str(self.size_delta_in_tokens),
            "collateral_delta_amount": str(self.collateral_delta_amount),
            "price_impact_usd": str(self.price_impact_usd),
            "price_impact_diff_usd": str(self.price_impact_diff_usd),
            "order_key": self.order_key,
            "order_type": self.order_type,
        }


@dataclass
class PositionDecreaseData:
    """Parsed data from PositionDecrease event.

    Attributes:
        key: Position key
        account: Account address
        market: Market address
        collateral_token: Collateral token address
        is_long: Position direction
        size_in_usd: New position size in USD
        size_in_tokens: New position size in tokens
        collateral_amount: New collateral amount
        execution_price: Execution price
        index_token_price_max: Max index token price
        index_token_price_min: Min index token price
        collateral_token_price_max: Max collateral token price
        collateral_token_price_min: Min collateral token price
        size_delta_usd: Size change in USD
        size_delta_in_tokens: Size change in tokens
        collateral_delta_amount: Collateral change
        price_impact_usd: Price impact in USD
        base_pnl_usd: Base PnL in USD
        uncapped_base_pnl_usd: Uncapped base PnL in USD
        realized_pnl: Realized PnL
        order_key: Associated order key
        order_type: Order type
    """

    key: str
    account: str
    market: str
    collateral_token: str
    is_long: bool
    size_in_usd: Decimal
    size_in_tokens: Decimal
    collateral_amount: Decimal
    execution_price: Decimal = Decimal("0")
    index_token_price_max: Decimal = Decimal("0")
    index_token_price_min: Decimal = Decimal("0")
    collateral_token_price_max: Decimal = Decimal("0")
    collateral_token_price_min: Decimal = Decimal("0")
    size_delta_usd: Decimal = Decimal("0")
    size_delta_in_tokens: Decimal = Decimal("0")
    collateral_delta_amount: Decimal = Decimal("0")
    price_impact_usd: Decimal = Decimal("0")
    base_pnl_usd: Decimal = Decimal("0")
    uncapped_base_pnl_usd: Decimal = Decimal("0")
    realized_pnl: Decimal = Decimal("0")
    order_key: str = ""
    order_type: int = 0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "key": self.key,
            "account": self.account,
            "market": self.market,
            "collateral_token": self.collateral_token,
            "is_long": self.is_long,
            "size_in_usd": str(self.size_in_usd),
            "size_in_tokens": str(self.size_in_tokens),
            "collateral_amount": str(self.collateral_amount),
            "execution_price": str(self.execution_price),
            "index_token_price_max": str(self.index_token_price_max),
            "index_token_price_min": str(self.index_token_price_min),
            "collateral_token_price_max": str(self.collateral_token_price_max),
            "collateral_token_price_min": str(self.collateral_token_price_min),
            "size_delta_usd": str(self.size_delta_usd),
            "size_delta_in_tokens": str(self.size_delta_in_tokens),
            "collateral_delta_amount": str(self.collateral_delta_amount),
            "price_impact_usd": str(self.price_impact_usd),
            "base_pnl_usd": str(self.base_pnl_usd),
            "uncapped_base_pnl_usd": str(self.uncapped_base_pnl_usd),
            "realized_pnl": str(self.realized_pnl),
            "order_key": self.order_key,
            "order_type": self.order_type,
        }


@dataclass
class OrderEventData:
    """Parsed data from Order events.

    Attributes:
        key: Order key
        account: Account address
        receiver: Receiver address
        market: Market address
        initial_collateral_token: Initial collateral token
        order_type: Order type
        decrease_position_swap_type: Swap type for decreases
        size_delta_usd: Size change in USD
        initial_collateral_delta_amount: Initial collateral change
        trigger_price: Trigger price
        acceptable_price: Acceptable price
        execution_fee: Execution fee paid
        min_output_amount: Minimum output amount
        updated_at_block: Block number when updated
        is_long: Position direction
        is_frozen: Whether order is frozen
        cancelled_reason: Reason for cancellation (if cancelled)
        frozen_reason: Reason for freezing (if frozen)
    """

    key: str
    account: str
    receiver: str
    market: str
    initial_collateral_token: str
    order_type: int
    decrease_position_swap_type: int = 0
    size_delta_usd: Decimal = Decimal("0")
    initial_collateral_delta_amount: Decimal = Decimal("0")
    trigger_price: Decimal = Decimal("0")
    acceptable_price: Decimal = Decimal("0")
    execution_fee: int = 0
    min_output_amount: Decimal = Decimal("0")
    updated_at_block: int = 0
    is_long: bool = True
    is_frozen: bool = False
    cancelled_reason: str = ""
    frozen_reason: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "key": self.key,
            "account": self.account,
            "receiver": self.receiver,
            "market": self.market,
            "initial_collateral_token": self.initial_collateral_token,
            "order_type": self.order_type,
            "decrease_position_swap_type": self.decrease_position_swap_type,
            "size_delta_usd": str(self.size_delta_usd),
            "initial_collateral_delta_amount": str(self.initial_collateral_delta_amount),
            "trigger_price": str(self.trigger_price),
            "acceptable_price": str(self.acceptable_price),
            "execution_fee": self.execution_fee,
            "min_output_amount": str(self.min_output_amount),
            "updated_at_block": self.updated_at_block,
            "is_long": self.is_long,
            "is_frozen": self.is_frozen,
            "cancelled_reason": self.cancelled_reason,
            "frozen_reason": self.frozen_reason,
        }


@dataclass(frozen=True)
class PerpFillData:
    """Receipt-measured GMX V2 fill economics (VIB-3873 / VIB-3872 WI-1).

    Built from a keeper transaction's ``PositionIncrease`` **or**
    ``PositionDecrease`` event, merged with its ``PositionFeesCollected`` event,
    each decoded BY KEY from the dynamic ``EventUtils.EventLogData`` payload (not
    fixed byte offsets — that is the VIB-3873 misread class). This is the typed
    verdict payload the WI-2 settlement capability carries into accounting; it is
    intentionally self-contained (no framework imports) so WI-2 can consume it
    without new plumbing.

    Empty != Zero — every field is Optional. ``None`` means the keeper receipt
    did not carry that field (unmeasured); ``Decimal("0")`` means a measured
    zero (e.g. zero funding over a short hold). Never substitute one for the
    other.

    Scaling:

    - ``size_delta_usd`` / ``price_impact_usd`` / ``realized_pnl_usd`` are plain
      USD Decimals (GMX's 30-decimal USD convention divided out).
    - ``position_fee_usd`` / ``funding_fee_usd`` / ``borrowing_fee_usd`` are plain
      USD, converted from the fee's collateral-token amount using the SAME
      ``PositionFeesCollected`` event's ``collateralTokenPrice`` (decimals-free:
      ``amount * price / 1e30``).
    - ``entry_price`` / ``exit_price`` are USD-per-token: GMX ``executionPrice``
      scaled by ``10**(index_token_decimals) / 1e30`` (VIB-6110). The index-token
      decimals are resolved from the parser's ``chain`` + the fill's ``market`` via
      the venue-verified catalog. Empty≠Zero: ``None`` when the price is absent
      OR the decimals cannot be resolved (parser constructed without a ``chain``, or
      an unlisted market) — NEVER the wrongly-scaled raw GMX-native ratio (the
      ``1.9e-15`` bug this field previously shipped).
    - ``collateral_delta_amount`` is the raw collateral-token smallest-unit
      integer (matches ``extract_collateral_returned`` semantics — no guessed
      decimal scale).
    - ``keeper_execution_fee_wei`` / ``execution_fee_refund_wei`` are NATIVE-token
      wei integers, not USD (VIB-6061). They are the two halves of the escrow we
      posted as ``msg.value`` at createOrder: the keeper's cut and our refund.
      Kept in wei rather than converted here because the parser is a pure function
      of the receipt and holds no price oracle — the USD conversion happens at the
      settlement-commit seam against the SUBMISSION row's own ``price_inputs_json``,
      so the booked USD is priced at the same moment as that row's ``gas_usd``.
    """

    is_open: bool | None = None
    is_long: bool | None = None
    market: str | None = None
    collateral_token: str | None = None
    position_key: str | None = None
    order_key: str | None = None
    entry_price: Decimal | None = None
    exit_price: Decimal | None = None
    size_delta_usd: Decimal | None = None
    size_delta_in_tokens: Decimal | None = None
    collateral_delta_amount: Decimal | None = None
    price_impact_usd: Decimal | None = None
    realized_pnl_usd: Decimal | None = None
    position_fee_usd: Decimal | None = None
    funding_fee_usd: Decimal | None = None
    borrowing_fee_usd: Decimal | None = None
    keeper_tx_hash: str | None = None
    block_number: int | None = None
    # VIB-6061 — native-wei execution-fee settlement (see class docstring).
    keeper_execution_fee_wei: int | None = None
    execution_fee_refund_wei: int | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to a stable machine-readable dictionary (Empty != Zero preserved)."""

        def _dec(value: Decimal | None) -> str | None:
            return str(value) if value is not None else None

        return {
            "is_open": self.is_open,
            "is_long": self.is_long,
            "market": self.market,
            "collateral_token": self.collateral_token,
            "position_key": self.position_key,
            "order_key": self.order_key,
            "entry_price": _dec(self.entry_price),
            "exit_price": _dec(self.exit_price),
            "size_delta_usd": _dec(self.size_delta_usd),
            "size_delta_in_tokens": _dec(self.size_delta_in_tokens),
            "collateral_delta_amount": _dec(self.collateral_delta_amount),
            "price_impact_usd": _dec(self.price_impact_usd),
            "realized_pnl_usd": _dec(self.realized_pnl_usd),
            "position_fee_usd": _dec(self.position_fee_usd),
            "funding_fee_usd": _dec(self.funding_fee_usd),
            "borrowing_fee_usd": _dec(self.borrowing_fee_usd),
            "keeper_tx_hash": self.keeper_tx_hash,
            "block_number": self.block_number,
            "keeper_execution_fee_wei": self.keeper_execution_fee_wei,
            "execution_fee_refund_wei": self.execution_fee_refund_wei,
        }


@dataclass
class ParseResult:
    """Result of parsing a receipt.

    Attributes:
        success: Whether parsing succeeded
        events: List of parsed events
        position_increases: Position increase events
        position_decreases: Position decrease events
        order_events: Order-related events
        error: Error message if parsing failed
        transaction_hash: Transaction hash
        block_number: Block number
    """

    success: bool
    events: list[GMXv2Event] = field(default_factory=list)
    position_increases: list[PositionIncreaseData] = field(default_factory=list)
    position_decreases: list[PositionDecreaseData] = field(default_factory=list)
    order_events: list[OrderEventData] = field(default_factory=list)
    error: str | None = None
    transaction_hash: str = ""
    block_number: int = 0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "events": [e.to_dict() for e in self.events],
            "position_increases": [p.to_dict() for p in self.position_increases],
            "position_decreases": [p.to_dict() for p in self.position_decreases],
            "order_events": [o.to_dict() for o in self.order_events],
            "error": self.error,
            "transaction_hash": self.transaction_hash,
            "block_number": self.block_number,
        }


# =============================================================================
# Receipt Parser
# =============================================================================


class GMXv2ReceiptParser:
    """Parser for GMX v2 transaction receipts.

    This parser extracts and decodes GMX v2 events from transaction receipts,
    providing structured data for position updates, order fills, and other
    protocol events.

    SUPPORTED_EXTRACTIONS declares the extraction fields this parser can provide.
    Used by ResultEnricher to warn when expected fields are unsupported.

    Example:
        parser = GMXv2ReceiptParser()

        # Parse a receipt dict (from web3.py)
        result = parser.parse_receipt(receipt)

        if result.success:
            for event in result.events:
                print(f"Event: {event.event_name}")

            for increase in result.position_increases:
                print(f"Position increased: size=${increase.size_in_usd}")
    """

    SUPPORTED_EXTRACTIONS: frozenset[str] = frozenset(
        {
            "async_orders",
            "swap_amounts",
            "position_id",
            "size_delta",
            "collateral",
            "entry_price",
            "leverage",
            "realized_pnl",
            "exit_price",
            "fees_paid",
            # PERP_CLOSE — collateral withdrawn from the position, decoded from
            # the PositionDecrease event's collateral_delta_amount. See
            # extract_collateral_returned for the exact semantics (this is the
            # event's collateral delta, NOT the net wallet payout).
            "collateral_returned",
            # VIB-3204 — placeholder extract_protocol_fees returning None;
            # real perp-fee extraction lives in follow-up VIB-3211.
            "protocol_fees",
            # VIB-3497 / VIB-3873 (WI-1) — funding fee USD at close, decoded from
            # the PositionFeesCollected keyed EventUtils payload and converted to
            # USD via the event's own collateralTokenPrice (decimals-free). Flows
            # to PerpData.funding_fee_usd -> attribution_json -> funding_pnl_usd.
            "funding_fee_usd",
            # VIB-3872 (WI-1) — typed PerpFillData built from PositionIncrease /
            # PositionDecrease + PositionFeesCollected keyed decodes (the WI-2
            # settlement verdict payload).
            "perp_fill",
        }
    )
    EXTRA_EXTRACTIONS_BY_INTENT: dict[str, tuple[str, ...]] = {
        "PERP_OPEN": ("async_orders",),
        "PERP_CLOSE": ("async_orders",),
    }
    REQUIRED_EXTRACTIONS_BY_INTENT: dict[str, frozenset[str]] = {
        "PERP_OPEN": frozenset({"async_orders"}),
        "PERP_CLOSE": frozenset({"async_orders"}),
    }

    def __init__(self, **kwargs: Any) -> None:
        """Initialize the parser.

        Args:
            chain: EVM chain slug (e.g. ``"arbitrum"``). Used ONLY to resolve a
                market's index-token decimals when scaling ``executionPrice`` to
                USD-per-token (VIB-6110). When absent, ``entry_price``/``exit_price``
                are left UNMEASURED (``None``) rather than shipped as the raw
                GMX-native ratio — the receipt decoder stays chain-agnostic for every
                other field.
            **kwargs: Additional arguments (ignored for compatibility).
        """
        chain = kwargs.get("chain")
        self.chain = str(chain).lower() if chain else None
        self.registry = EventRegistry(EVENT_TOPICS, EVENT_NAME_TO_TYPE)

    @staticmethod
    def build_extract_kwargs(*, field: str, bundle_metadata: dict[str, Any]) -> dict[str, Any]:
        """Thread compiler-verified index decimals into perp fill scaling."""
        if field != "perp_fill":
            return {}
        raw = bundle_metadata.get("index_token_decimals")
        if isinstance(raw, bool) or not isinstance(raw, int) or raw < 0 or raw > 30:
            return {}
        return {"index_token_decimals_override": raw}

    def parse_receipt(self, receipt: dict[str, Any]) -> ParseResult:  # noqa: C901
        """Parse a transaction receipt.

        Args:
            receipt: Transaction receipt dict containing 'logs', 'transactionHash',
                     'blockNumber', etc.

        Returns:
            ParseResult with extracted events and data
        """
        try:
            tx_hash = receipt.get("transactionHash", "")
            if isinstance(tx_hash, bytes):
                tx_hash = "0x" + tx_hash.hex()

            block_number = receipt.get("blockNumber", 0)
            logs = receipt.get("logs", [])

            if not logs:
                return ParseResult(
                    success=True,
                    transaction_hash=tx_hash,
                    block_number=block_number,
                )

            events: list[GMXv2Event] = []
            position_increases: list[PositionIncreaseData] = []
            position_decreases: list[PositionDecreaseData] = []
            order_events: list[OrderEventData] = []

            for log in logs:
                parsed_event = self._parse_log(log, tx_hash, block_number)
                if parsed_event:
                    events.append(parsed_event)

                    # Extract typed data based on event type
                    if parsed_event.event_type == GMXv2EventType.POSITION_INCREASE:
                        increase_data = self._parse_position_increase(parsed_event)
                        if increase_data:
                            position_increases.append(increase_data)

                    elif parsed_event.event_type == GMXv2EventType.POSITION_DECREASE:
                        decrease_data = self._parse_position_decrease(parsed_event)
                        if decrease_data:
                            position_decreases.append(decrease_data)

                    elif parsed_event.event_type in (
                        GMXv2EventType.ORDER_CREATED,
                        GMXv2EventType.ORDER_EXECUTED,
                        GMXv2EventType.ORDER_CANCELLED,
                        GMXv2EventType.ORDER_FROZEN,
                    ):
                        order_data = self._parse_order_event(parsed_event)
                        if order_data:
                            order_events.append(order_data)

            # Log parsed receipt with user-friendly formatting
            gas_used = receipt.get("gasUsed", 0)
            tx_fmt = format_tx_hash(tx_hash)
            gas_fmt = format_gas_cost(gas_used)

            if position_increases:
                for p in position_increases:
                    direction = "LONG" if p.is_long else "SHORT"
                    size_fmt = format_usd(p.size_delta_usd)
                    price_fmt = format_usd(p.execution_price)
                    logger.info(
                        f"🔍 Parsed GMX position OPEN: {direction} {size_fmt}, "
                        f"entry={price_fmt}, tx={tx_fmt}, {gas_fmt}"
                    )
            elif position_decreases:
                for pd in position_decreases:
                    direction = "LONG" if pd.is_long else "SHORT"
                    size_fmt = format_usd(pd.size_delta_usd)
                    pnl_fmt = format_usd(pd.realized_pnl)
                    logger.info(
                        f"🔍 Parsed GMX position CLOSE: {direction} {size_fmt}, PnL={pnl_fmt}, tx={tx_fmt}, {gas_fmt}"
                    )
            elif order_events:
                for o in order_events:
                    logger.info(f"🔍 Parsed GMX order: type={o.order_type}, tx={tx_fmt}, {gas_fmt}")
            else:
                logger.info(f"🔍 Parsed GMX V2 receipt: tx={tx_fmt}, events={len(events)}, {gas_fmt}")

            return ParseResult(
                success=True,
                events=events,
                position_increases=position_increases,
                position_decreases=position_decreases,
                order_events=order_events,
                transaction_hash=tx_hash,
                block_number=block_number,
            )

        except Exception as e:
            logger.exception(f"Failed to parse receipt: {e}")
            return ParseResult(
                success=False,
                error=str(e),
            )

    def parse_logs(self, logs: list[dict[str, Any]]) -> list[GMXv2Event]:
        """Parse a list of logs.

        Args:
            logs: List of log dicts

        Returns:
            List of parsed events
        """
        events = []
        for log in logs:
            event = self._parse_log(log, "", 0)
            if event:
                events.append(event)
        return events

    def _parse_log(
        self,
        log: dict[str, Any],
        tx_hash: str,
        block_number: int,
    ) -> GMXv2Event | None:
        """Parse a single log entry.

        Args:
            log: Log dict containing 'topics', 'data', 'address', etc.
            tx_hash: Transaction hash
            block_number: Block number

        Returns:
            Parsed event or None if not a known GMX v2 event
        """
        try:
            topics = log.get("topics", [])
            if not topics:
                return None

            # GMX V2 uses the EventEmitter contract which emits all protocol events
            # via EventLog/EventLog1/EventLog2. The event structure is:
            #   topic[0] = EventLog/EventLog1/EventLog2 signature (shared across all events)
            #   topic[1] = keccak256 of event name string (indexed eventNameHash)
            # We match on topic[1] to identify the specific GMX V2 event type.
            # For non-EventEmitter logs (e.g., ERC20 Transfer), topic[1] won't match
            # any known GMX event name hash, so they're correctly skipped.
            event_name = None

            # Try topic[1] first (GMX V2 EventEmitter pattern)
            if len(topics) >= 2:
                second_topic = topics[1]
                if isinstance(second_topic, bytes):
                    second_topic = "0x" + second_topic.hex()
                else:
                    second_topic = str(second_topic)
                second_topic = second_topic.lower()
                event_name = self.registry.get_event_name(second_topic)

            # Fall back to topic[0] for standard event matching
            if event_name is None:
                first_topic = topics[0]
                if isinstance(first_topic, bytes):
                    first_topic = "0x" + first_topic.hex()
                else:
                    first_topic = str(first_topic)
                first_topic = first_topic.lower()
                event_name = self.registry.get_event_name(first_topic)

            if event_name is None:
                # Unknown event, skip
                return None

            event_type = self.registry.get_event_type(event_name) or GMXv2EventType.UNKNOWN

            # Get raw data
            data = log.get("data", "")
            if isinstance(data, bytes):
                data = "0x" + data.hex()

            # Parse log data
            parsed_data = self._decode_log_data(event_name, topics, data)

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

            return GMXv2Event(
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

        except GMXOrderTypeError:
            # VIB-3873 tripwire: a decoded order_type above the enum bound is a
            # misread, not a parseable event. Propagate loudly so parse_receipt
            # fails closed instead of dropping the event as an unparseable log.
            raise
        except Exception as e:
            logger.warning(f"Failed to parse log: {e}")
            return None

    def _decode_log_data(
        self,
        event_name: str,
        topics: list[Any],
        data: str,
    ) -> dict[str, Any]:
        """Decode log data based on event type.

        Args:
            event_name: Name of the event
            topics: List of topics
            data: Hex-encoded event data

        Returns:
            Decoded event data dict
        """
        # Remove 0x prefix if present
        if data.startswith("0x"):
            data = data[2:]

        # Decode based on event type
        # Note: This is simplified - production would use proper ABI decoding

        if event_name == "PositionIncrease":
            return self._decode_position_increase_data(topics, data)
        elif event_name == "PositionDecrease":
            return self._decode_position_decrease_data(topics, data)
        elif event_name in ("OrderCreated", "OrderExecuted", "OrderCancelled", "OrderFrozen"):
            return self._decode_order_data(topics, data, event_name)
        else:
            # Return raw data for unknown events
            return {"raw_data": data}

    def _decode_position_increase_data(
        self,
        topics: list[Any],
        data: str,
    ) -> dict[str, Any]:
        """Decode PositionIncrease event data.

        Production payloads are the keyed EventUtils struct — decoded BY NAME
        (VIB-3873). The legacy fixed-offset decode below is retained only for
        synthetic flat-word fixtures; it is NEVER used for a real keyed payload.
        """
        event_utils = self._decode_event_utils_position_increase(data)
        if event_utils is not None:
            return event_utils

        # Simplified decoding - production would use proper ABI decoding
        try:
            # GMX uses 10**30 for USD values and 10**18 for token values
            usd_scale = Decimal(10**30)
            token_scale = Decimal(10**18)

            # Parse key fields from PositionIncrease event data.
            # Layout (32 bytes each): account, market, collateral_token, is_long,
            # size_in_usd, size_in_tokens, collateral_amount, execution_price,
            # size_delta_usd, collateral_delta_amount, index_token_price_max,
            # index_token_price_min, collateral_token_price_max, collateral_token_price_min,
            # price_impact_usd, order_type, order_key
            # EventEmitter pattern: topic[0]=EventLog sig, topic[1]=eventNameHash,
            # topic[2]=indexed key. Fall back to topic[1] for legacy format.
            key_topic = topics[2] if len(topics) > 2 else (topics[1] if len(topics) > 1 else None)
            key_str = HexDecoder.topic_to_bytes32(key_topic) if key_topic else "0x" + "00" * 32

            result: dict[str, Any] = {
                "key": key_str,
                "account": HexDecoder.decode_address_from_data(data, 0),
                "market": HexDecoder.decode_address_from_data(data, 32),
                "collateral_token": HexDecoder.decode_address_from_data(data, 64),
                "is_long": HexDecoder.decode_uint256(data, 96) == 1,
                "size_in_usd": str(Decimal(HexDecoder.decode_uint256(data, 128)) / usd_scale),
                "size_in_tokens": str(Decimal(HexDecoder.decode_uint256(data, 160)) / token_scale),
                "collateral_amount": str(Decimal(HexDecoder.decode_uint256(data, 192)) / token_scale),
                "execution_price": str(Decimal(HexDecoder.decode_uint256(data, 224)) / usd_scale),
                "size_delta_usd": str(Decimal(HexDecoder.decode_uint256(data, 256)) / usd_scale),
                "collateral_delta_amount": str(Decimal(HexDecoder.decode_uint256(data, 288)) / token_scale),
                "index_token_price_max": str(Decimal(HexDecoder.decode_uint256(data, 320)) / usd_scale),
                "index_token_price_min": str(Decimal(HexDecoder.decode_uint256(data, 352)) / usd_scale),
                "collateral_token_price_max": str(Decimal(HexDecoder.decode_uint256(data, 384)) / usd_scale),
                "collateral_token_price_min": str(Decimal(HexDecoder.decode_uint256(data, 416)) / usd_scale),
                "price_impact_usd": str(Decimal(HexDecoder.decode_int256(data, 448)) / usd_scale),
                "order_type": HexDecoder.decode_uint256(data, 480),
                "order_key": "0x" + data[512 * 2 : 512 * 2 + 64] if len(data) >= 512 * 2 + 64 else "",
            }

            return result

        except Exception as e:
            logger.warning(f"Failed to decode PositionIncrease data: {e}")
            return {"raw_data": data}

    @staticmethod
    def _decode_event_utils_data(data: str, expected_event_name: str) -> dict[str, dict[str, Any]] | None:
        """Decode GMX EventEmitter's keyed ``EventUtils.EventLogData`` payload."""
        try:
            _sender, event_name, event_data = abi_decode(
                ["address", "string", _EVENT_LOG_DATA_ABI_TYPE],
                bytes.fromhex(data),
            )
        except Exception:
            return None
        if event_name != expected_event_name:
            return None

        def _scalars(group: Any) -> dict[str, Any]:
            return {str(key): value for key, value in group[0]}

        return {
            "addresses": _scalars(event_data[0]),
            "uints": _scalars(event_data[1]),
            "ints": _scalars(event_data[2]),
            "bools": _scalars(event_data[3]),
            "bytes32": _scalars(event_data[4]),
        }

    @staticmethod
    def _bytes32_hex(value: Any) -> str:
        if isinstance(value, bytes):
            return "0x" + value.hex()
        text = str(value)
        return text if text.startswith("0x") else "0x" + text

    @staticmethod
    def _usd_from_1e30(raw: Any) -> Decimal | None:
        """Scale a GMX 30-decimal USD integer to a plain USD ``Decimal``.

        Empty != Zero: ``None`` (field absent) stays ``None``; a measured ``0``
        becomes ``Decimal("0")``.
        """
        if raw is None or isinstance(raw, bool) or not isinstance(raw, int):
            return None
        return Decimal(raw) / Decimal(10**30)

    def _execution_price_usd(
        self,
        raw: Any,
        market: str | None,
        index_token_decimals_override: int | None = None,
    ) -> Decimal | None:
        """Scale GMX ``executionPrice`` to USD-per-token (VIB-6110).

        GMX stores ``executionPrice`` as a 30-decimal ratio over the index token's
        smallest unit, so USD-per-token = ``executionPrice * 10**index_token_decimals
        / 1e30``. The index-token decimals come from the parser's ``chain`` + this
        fill's ``market``. Empty≠Zero: returns ``None`` when the raw price is absent,
        the parser has no ``chain``, or the market's decimals are unlisted — NEVER the
        wrongly-scaled raw ratio (dividing by ``1e30`` alone yields the ``1.9e-15``
        garbage that corrupted cost-basis downstream).
        """
        if raw is None or isinstance(raw, bool) or not isinstance(raw, int):
            return None
        decimals = index_token_decimals_override
        if decimals is None and self.chain and market:
            # Address-first: the bundle-carried override (stamped at compile
            # time from the verified record) is the primary source; the
            # process catalog covers fills whose bundle predates the stamp.
            # No static table — an unknown market (or an absent chain/market
            # reference) stays unmeasured (None), never wrongly scaled.
            decimals = market_catalog.index_decimals(self.chain, market)
        if decimals is None:
            return None
        return Decimal(raw).scaleb(decimals - 30)

    @staticmethod
    def _fee_amount_to_usd(amount_raw: Any, price_min_raw: Any, price_max_raw: Any) -> Decimal | None:
        """Convert a collateral-token fee amount to USD using the event's own price.

        GMX ``collateralTokenPrice`` is a raw price with ``30 - tokenDecimals``
        decimals, and the fee ``amount`` is in the token's smallest units
        (``tokenDecimals`` decimals). Their product carries ``30`` decimals of
        USD, so ``amount * price / 1e30`` yields plain USD **without** needing to
        know the token's decimals (decimals-free). The mid of the min/max price
        bounds is used so neither bound biases the fee valuation.

        Empty != Zero: any missing input yields ``None`` (unmeasured). A
        measured zero amount yields ``Decimal("0")``.
        """
        for value in (amount_raw, price_min_raw, price_max_raw):
            if value is None or isinstance(value, bool) or not isinstance(value, int):
                return None
        mid_price = (Decimal(price_min_raw) + Decimal(price_max_raw)) / Decimal(2)
        return Decimal(amount_raw) * mid_price / Decimal(10**30)

    def _decode_event_utils_position_increase(self, data: str) -> dict[str, Any] | None:
        """Decode PositionIncrease fill economics from the keyed EventUtils payload.

        Production GMX emits PositionIncrease through the EventEmitter's dynamic
        keyed payload; the legacy fixed-offset decode reads dynamic-struct ABI
        offsets as field values (the VIB-3873 misread class — the exact same
        failure #3429 fixed for OrderCreated). This reads every field BY NAME.

        Returns a dict carrying the ``_event_utils_payload`` sentinel (so the
        legacy typed builder does NOT fabricate positionally-decoded price/PnL
        fields from it) plus the identity fields and the raw integers WI-1's
        :class:`PerpFillData` needs. ``None`` when the payload is not a keyed
        PositionIncrease (the caller then falls back to the legacy decode for
        synthetic flat-word fixtures).

        Raises:
            GMXOrderTypeError: decoded ``orderType`` exceeds the enum bound (>7).
        """
        items = self._decode_event_utils_data(data, "PositionIncrease")
        if items is None:
            return None
        addresses = items["addresses"]
        uints = items["uints"]
        ints = items["ints"]
        bools = items["bools"]
        bytes32_items = items["bytes32"]

        order_type = _checked_order_type(uints.get("orderType"), "PositionIncrease")
        result: dict[str, Any] = {
            "_event_utils_payload": True,
            "key": self._bytes32_hex(bytes32_items["positionKey"]) if "positionKey" in bytes32_items else "",
            "account": str(addresses["account"]) if "account" in addresses else "",
            "market": str(addresses["market"]) if "market" in addresses else "",
            "collateral_token": str(addresses["collateralToken"]) if "collateralToken" in addresses else "",
            "is_long": bool(bools["isLong"]) if "isLong" in bools else None,
            "order_key": self._bytes32_hex(bytes32_items["orderKey"]) if "orderKey" in bytes32_items else "",
            "position_key": self._bytes32_hex(bytes32_items["positionKey"]) if "positionKey" in bytes32_items else "",
            "order_type": order_type,
            # Raw GMX integers preserved exactly for PerpFillData.
            "execution_price_raw": uints.get("executionPrice"),
            "size_delta_usd_raw": uints.get("sizeDeltaUsd"),
            "size_delta_in_tokens_raw": uints.get("sizeDeltaInTokens"),
            # PositionIncrease carries collateralDeltaAmount / price impact in the
            # SIGNED intItems group (int256), unlike PositionDecrease.
            "collateral_delta_amount_raw": ints.get("collateralDeltaAmount"),
            "price_impact_usd_raw": ints.get("pendingPriceImpactUsd", ints.get("priceImpactUsd")),
        }
        return result

    def _decode_event_utils_position_fees(self, data: str) -> dict[str, Any] | None:
        """Decode PositionFeesCollected fee economics from the keyed payload.

        Every fee amount is emitted in collateral-token units; this converts
        each to USD with the event's own ``collateralTokenPrice`` bounds
        (decimals-free — see :meth:`_fee_amount_to_usd`). ``None`` when the
        payload is not a keyed PositionFeesCollected event.
        """
        items = self._decode_event_utils_data(data, "PositionFeesCollected")
        if items is None:
            return None
        uints = items["uints"]
        bytes32_items = items["bytes32"]
        price_min = uints.get("collateralTokenPrice.min")
        price_max = uints.get("collateralTokenPrice.max")
        return {
            "order_key": self._bytes32_hex(bytes32_items["orderKey"]) if "orderKey" in bytes32_items else None,
            "position_key": self._bytes32_hex(bytes32_items["positionKey"]) if "positionKey" in bytes32_items else None,
            "funding_fee_usd": self._fee_amount_to_usd(uints.get("fundingFeeAmount"), price_min, price_max),
            "position_fee_usd": self._fee_amount_to_usd(uints.get("positionFeeAmount"), price_min, price_max),
            "borrowing_fee_usd": self._fee_amount_to_usd(uints.get("borrowingFeeAmount"), price_min, price_max),
        }

    def _decode_event_utils_order(self, data: str, event_name: str, key_str: str) -> dict[str, Any] | None:
        """Decode an Order event's fields from the keyed EventUtils payload.

        Only ``OrderCreated`` carries the full order struct; ``OrderExecuted`` /
        ``OrderCancelled`` / ``OrderFrozen`` payloads carry a sparse set, so any
        field that is absent stays absent (Empty != Zero). ``None`` when the
        payload is not this keyed event (caller falls back to the legacy decode).

        The async-order identity path (#3429) is unaffected: it reads the
        indexed ``key`` and re-decodes OrderCreated via ``_keyed_order_created_fields``.
        The ``order_type`` here is intentionally NOT bound-checked — the async
        path already tolerates an out-of-range value by falling back to the
        runner's authoritative intent (``_async_order_kind``). The VIB-3873
        tripwire lives on the fill-economics decoders (Position{Increase,Decrease}).
        """
        items = self._decode_event_utils_data(data, event_name)
        if items is None:
            return None
        addresses = items["addresses"]
        uints = items["uints"]
        bools = items["bools"]

        usd_scale = Decimal(10**30)
        token_scale = Decimal(10**18)

        def _usd(key: str) -> str:
            value = uints.get(key)
            return str(Decimal(value) / usd_scale) if isinstance(value, int) and not isinstance(value, bool) else "0"

        def _tokens(key: str) -> str:
            value = uints.get(key)
            return str(Decimal(value) / token_scale) if isinstance(value, int) and not isinstance(value, bool) else "0"

        order_type_raw = uints.get("orderType")
        result: dict[str, Any] = {
            "key": key_str,
            "account": str(addresses.get("account", "")),
            "receiver": str(addresses.get("receiver", addresses.get("account", ""))),
            "market": str(addresses.get("market", "")),
            "initial_collateral_token": str(addresses.get("initialCollateralToken", "")),
            "order_type": order_type_raw
            if isinstance(order_type_raw, int) and not isinstance(order_type_raw, bool)
            else 0,
            "decrease_position_swap_type": (
                uints.get("decreasePositionSwapType", 0)
                if isinstance(uints.get("decreasePositionSwapType"), int)
                else 0
            ),
            "is_long": bool(bools["isLong"]) if "isLong" in bools else True,
            "size_delta_usd": _usd("sizeDeltaUsd"),
            "initial_collateral_delta_amount": _tokens("initialCollateralDeltaAmount"),
            "trigger_price": _usd("triggerPrice"),
            "acceptable_price": _usd("acceptablePrice"),
            "execution_fee": uints.get("executionFee", 0) if isinstance(uints.get("executionFee"), int) else 0,
            "min_output_amount": _tokens("minOutputAmount"),
            # GMX renamed updatedAtBlock -> updatedAtTime; expose under the
            # existing OrderEventData field name for consumer compatibility.
            "updated_at_block": (
                uints.get("updatedAtTime", uints.get("updatedAtBlock", 0))
                if isinstance(uints.get("updatedAtTime", uints.get("updatedAtBlock")), int)
                else 0
            ),
            "event_name": event_name,
        }
        if event_name == "OrderCancelled":
            result["cancelled_reason"] = "User cancelled"
        elif event_name == "OrderFrozen":
            result["frozen_reason"] = "Execution failed"
            result["is_frozen"] = True
        return result

    def _decode_event_utils_position_decrease(self, data: str) -> dict[str, Any] | None:
        """Decode the reliable collateral fields from production PositionDecrease."""
        items = self._decode_event_utils_data(data, "PositionDecrease")
        if items is None:
            return None

        addresses = items["addresses"]
        uints = items["uints"]
        bools = items["bools"]
        bytes32_items = items["bytes32"]
        required = {
            "address": ("account", "market", "collateralToken"),
            "uint": ("collateralAmount", "collateralDeltaAmount"),
            "bool": ("isLong",),
            "bytes32": ("orderKey", "positionKey"),
        }
        missing = [
            f"{group}.{key}"
            for group, keys in required.items()
            for key in keys
            if key
            not in {
                "address": addresses,
                "uint": uints,
                "bool": bools,
                "bytes32": bytes32_items,
            }[group]
        ]
        if missing:
            logger.warning("GMX PositionDecrease EventUtils payload missing required items: %s", ", ".join(missing))
            return None

        collateral_token = str(addresses["collateralToken"])
        collateral_decimals = _TOKEN_DECIMALS_BY_ADDRESS.get(collateral_token.lower())
        if collateral_decimals is None:
            logger.warning(
                "GMX PositionDecrease collateral token %s has no declared decimals; "
                "collateral amount fields remain unmeasured",
                collateral_token,
            )
            collateral_amount = None
            collateral_delta_amount = None
        else:
            token_scale = Decimal(10**collateral_decimals)
            collateral_amount = Decimal(uints["collateralAmount"]) / token_scale
            collateral_delta_amount = Decimal(uints["collateralDeltaAmount"]) / token_scale

        ints = items["ints"]
        order_type = _checked_order_type(uints.get("orderType"), "PositionDecrease")
        result: dict[str, Any] = {
            "_event_utils_payload": True,
            "key": self._bytes32_hex(bytes32_items["positionKey"]),
            "account": str(addresses["account"]),
            "market": str(addresses["market"]),
            "collateral_token": collateral_token,
            "is_long": bool(bools["isLong"]),
            "collateral_delta_amount_raw": str(uints["collateralDeltaAmount"]),
            "order_key": self._bytes32_hex(bytes32_items["orderKey"]),
            "position_key": self._bytes32_hex(bytes32_items["positionKey"]),
            "order_type": order_type,
            # Raw GMX integers preserved exactly for PerpFillData. For
            # PositionDecrease, collateralDeltaAmount is UNSIGNED (uintItems) and
            # priceImpactUsd / basePnlUsd are SIGNED (intItems).
            "execution_price_raw": uints.get("executionPrice"),
            "size_delta_usd_raw": uints.get("sizeDeltaUsd"),
            "size_delta_in_tokens_raw": uints.get("sizeDeltaInTokens"),
            "collateral_delta_amount_raw_int": uints.get("collateralDeltaAmount"),
            "price_impact_usd_raw": ints.get("priceImpactUsd"),
            "realized_pnl_raw": ints.get("basePnlUsd"),
        }
        if collateral_amount is not None:
            result["collateral_amount"] = str(collateral_amount)
        if collateral_delta_amount is not None:
            result["collateral_delta_amount"] = str(collateral_delta_amount)
        return result

    def _decode_position_decrease_data(
        self,
        topics: list[Any],
        data: str,
    ) -> dict[str, Any]:
        """Decode PositionDecrease event data."""
        event_utils = self._decode_event_utils_position_decrease(data)
        if event_utils is not None:
            return event_utils

        try:
            # GMX uses 10**30 for USD values and 10**18 for token values
            usd_scale = Decimal(10**30)
            token_scale = Decimal(10**18)

            # Layout (32 bytes each): account, market, collateral_token, is_long,
            # size_in_usd, size_in_tokens, collateral_amount, execution_price,
            # size_delta_usd, collateral_delta_amount, index_token_price_max,
            # index_token_price_min, collateral_token_price_max, collateral_token_price_min,
            # price_impact_usd, realized_pnl
            # EventEmitter pattern: topic[2] has the indexed key
            key_topic = topics[2] if len(topics) > 2 else (topics[1] if len(topics) > 1 else None)
            key_str = HexDecoder.topic_to_bytes32(key_topic) if key_topic else "0x" + "00" * 32

            result: dict[str, Any] = {
                "key": key_str,
                "account": HexDecoder.decode_address_from_data(data, 0),
                "market": HexDecoder.decode_address_from_data(data, 32),
                "collateral_token": HexDecoder.decode_address_from_data(data, 64),
                "is_long": HexDecoder.decode_uint256(data, 96) == 1,
                "size_in_usd": str(Decimal(HexDecoder.decode_uint256(data, 128)) / usd_scale),
                "size_in_tokens": str(Decimal(HexDecoder.decode_uint256(data, 160)) / token_scale),
                "collateral_amount": str(Decimal(HexDecoder.decode_uint256(data, 192)) / token_scale),
                "execution_price": str(Decimal(HexDecoder.decode_uint256(data, 224)) / usd_scale),
                "size_delta_usd": str(Decimal(HexDecoder.decode_uint256(data, 256)) / usd_scale),
                "collateral_delta_amount": str(Decimal(HexDecoder.decode_uint256(data, 288)) / token_scale),
                "collateral_delta_amount_raw": str(HexDecoder.decode_uint256(data, 288)),
                "index_token_price_max": str(Decimal(HexDecoder.decode_uint256(data, 320)) / usd_scale),
                "index_token_price_min": str(Decimal(HexDecoder.decode_uint256(data, 352)) / usd_scale),
                "collateral_token_price_max": str(Decimal(HexDecoder.decode_uint256(data, 384)) / usd_scale),
                "collateral_token_price_min": str(Decimal(HexDecoder.decode_uint256(data, 416)) / usd_scale),
                "price_impact_usd": str(Decimal(HexDecoder.decode_int256(data, 448)) / usd_scale),
                "realized_pnl": str(Decimal(HexDecoder.decode_int256(data, 480)) / usd_scale),
            }

            return result

        except Exception as e:
            logger.warning(f"Failed to decode PositionDecrease data: {e}")
            return {"raw_data": data}

    def _decode_order_data(
        self,
        topics: list[Any],
        data: str,
        event_name: str,
    ) -> dict[str, Any]:
        """Decode Order event data.

        The indexed order key is authoritative and comes from the strict
        indexed-topic decode (unchanged, #3429). The order fields come from the
        keyed EventUtils payload BY NAME (VIB-3873); the legacy fixed-offset
        decode is retained only for synthetic flat-word fixtures.
        """
        key_str = self._strict_indexed_order_key(topics, event_name)
        event_utils = self._decode_event_utils_order(data, event_name, key_str)
        if event_utils is not None:
            return event_utils

        try:
            # GMX uses 10**30 for USD values and 10**18 for token values
            usd_scale = Decimal(10**30)
            token_scale = Decimal(10**18)

            # Layout (32 bytes each): account, receiver, market, initial_collateral_token,
            # order_type, decrease_position_swap_type, is_long, size_delta_usd,
            # initial_collateral_delta_amount, trigger_price, acceptable_price,
            # execution_fee, min_output_amount, updated_at_block
            # EventEmitter pattern: topic[2] has the indexed key. A missing
            # topic[2] must stay missing rather than falling back to the event
            # name hash in topic[1], which is itself bytes32-shaped but is not
            # an order identifier.
            key_str = self._strict_indexed_order_key(topics, event_name)

            result: dict[str, Any] = {
                "key": key_str,
                "account": HexDecoder.decode_address_from_data(data, 0),
                "receiver": HexDecoder.decode_address_from_data(data, 32),
                "market": HexDecoder.decode_address_from_data(data, 64),
                "initial_collateral_token": HexDecoder.decode_address_from_data(data, 96),
                "order_type": HexDecoder.decode_uint256(data, 128),
                "decrease_position_swap_type": HexDecoder.decode_uint256(data, 160),
                "is_long": HexDecoder.decode_uint256(data, 192) == 1,
                "size_delta_usd": str(Decimal(HexDecoder.decode_uint256(data, 224)) / usd_scale),
                "initial_collateral_delta_amount": str(Decimal(HexDecoder.decode_uint256(data, 256)) / token_scale),
                "trigger_price": str(Decimal(HexDecoder.decode_uint256(data, 288)) / usd_scale),
                "acceptable_price": str(Decimal(HexDecoder.decode_uint256(data, 320)) / usd_scale),
                "execution_fee": HexDecoder.decode_uint256(data, 352),
                "min_output_amount": str(Decimal(HexDecoder.decode_uint256(data, 384)) / token_scale),
                "updated_at_block": HexDecoder.decode_uint256(data, 416),
                "event_name": event_name,
            }

            # Add event-specific fields
            if event_name == "OrderCancelled":
                result["cancelled_reason"] = "User cancelled"  # Would decode from data
            elif event_name == "OrderFrozen":
                result["frozen_reason"] = "Execution failed"  # Would decode from data
                result["is_frozen"] = True

            return result

        except Exception as e:
            logger.warning(f"Failed to decode Order data: {e}")
            return {"raw_data": data, "event_name": event_name}

    @staticmethod
    def _strict_indexed_order_key(topics: list[Any], event_name: str) -> str:
        """Return an exact non-zero bytes32 indexed order key, or an empty string."""

        def normalize(topic: Any) -> str:
            if isinstance(topic, bytes):
                return "0x" + topic.hex()
            value = str(topic)
            return value if value.startswith("0x") else "0x" + value

        expected = EVENT_TOPICS.get(event_name, "").lower()
        key_topic: Any | None = None
        if len(topics) >= 2 and normalize(topics[1]).lower() == expected:
            if len(topics) >= 3:
                key_topic = topics[2]
        elif topics and normalize(topics[0]).lower() == expected and len(topics) >= 2:
            key_topic = topics[1]

        if key_topic is None:
            return ""
        if isinstance(key_topic, bytes):
            if len(key_topic) != 32:
                return ""
            key = "0x" + key_topic.hex()
        else:
            key = normalize(key_topic)
            if re.fullmatch(r"0x[0-9a-fA-F]{64}", key) is None:
                return ""
        return "" if int(key, 16) == 0 else key.lower()

    def _parse_position_increase(self, event: GMXv2Event) -> PositionIncreaseData | None:
        """Parse a PositionIncrease event into typed data."""
        try:
            data = event.data
            if data.get("_event_utils_payload"):
                # Production keyed payload: fill economics are exposed via
                # PerpFillData (extract_perp_fill), NOT re-derived positionally
                # here. Returning None keeps the legacy positional builder from
                # fabricating garbage price/PnL fields from a keyed payload — the
                # VIB-3873 misread class. (Mirrors _parse_position_decrease.)
                return None
            return PositionIncreaseData(
                key=data.get("key", ""),
                account=data.get("account", ""),
                market=data.get("market", ""),
                collateral_token=data.get("collateral_token", ""),
                is_long=data.get("is_long", True),
                size_in_usd=Decimal(data.get("size_in_usd", "0")),
                size_in_tokens=Decimal(data.get("size_in_tokens", "0")),
                collateral_amount=Decimal(data.get("collateral_amount", "0")),
                execution_price=Decimal(data.get("execution_price", "0")),
                index_token_price_max=Decimal(data.get("index_token_price_max", "0")),
                index_token_price_min=Decimal(data.get("index_token_price_min", "0")),
                collateral_token_price_max=Decimal(data.get("collateral_token_price_max", "0")),
                collateral_token_price_min=Decimal(data.get("collateral_token_price_min", "0")),
                size_delta_usd=Decimal(data.get("size_delta_usd", "0")),
                collateral_delta_amount=Decimal(data.get("collateral_delta_amount", "0")),
                price_impact_usd=Decimal(data.get("price_impact_usd", "0")),
                order_type=data.get("order_type", 0),
                order_key=data.get("order_key", ""),
            )
        except Exception as e:
            logger.warning(f"Failed to parse PositionIncreaseData: {e}")
            return None

    def _parse_position_decrease(self, event: GMXv2Event) -> PositionDecreaseData | None:
        """Parse a PositionDecrease event into typed data."""
        try:
            data = event.data
            if data.get("_event_utils_payload"):
                # This slice decodes exact collateral-returned semantics only.
                # Do not fabricate typed price/PnL fields from GMX's differently
                # scaled EventUtils values; their dedicated decoder is separate.
                return None
            return PositionDecreaseData(
                key=data.get("key", ""),
                account=data.get("account", ""),
                market=data.get("market", ""),
                collateral_token=data.get("collateral_token", ""),
                is_long=data.get("is_long", True),
                size_in_usd=Decimal(data.get("size_in_usd", "0")),
                size_in_tokens=Decimal(data.get("size_in_tokens", "0")),
                collateral_amount=Decimal(data.get("collateral_amount", "0")),
                execution_price=Decimal(data.get("execution_price", "0")),
                index_token_price_max=Decimal(data.get("index_token_price_max", "0")),
                index_token_price_min=Decimal(data.get("index_token_price_min", "0")),
                collateral_token_price_max=Decimal(data.get("collateral_token_price_max", "0")),
                collateral_token_price_min=Decimal(data.get("collateral_token_price_min", "0")),
                size_delta_usd=Decimal(data.get("size_delta_usd", "0")),
                collateral_delta_amount=Decimal(data.get("collateral_delta_amount", "0")),
                price_impact_usd=Decimal(data.get("price_impact_usd", "0")),
                realized_pnl=Decimal(data.get("realized_pnl", "0")),
            )
        except Exception as e:
            logger.warning(f"Failed to parse PositionDecreaseData: {e}")
            return None

    def _parse_order_event(self, event: GMXv2Event) -> OrderEventData | None:
        """Parse an Order event into typed data."""
        try:
            data = event.data
            return OrderEventData(
                key=data.get("key", ""),
                account=data.get("account", ""),
                receiver=data.get("receiver", data.get("account", "")),
                market=data.get("market", ""),
                initial_collateral_token=data.get("initial_collateral_token", ""),
                order_type=data.get("order_type", 0),
                decrease_position_swap_type=data.get("decrease_position_swap_type", 0),
                size_delta_usd=Decimal(data.get("size_delta_usd", "0")),
                initial_collateral_delta_amount=Decimal(data.get("initial_collateral_delta_amount", "0")),
                trigger_price=Decimal(data.get("trigger_price", "0")),
                acceptable_price=Decimal(data.get("acceptable_price", "0")),
                execution_fee=data.get("execution_fee", 0),
                min_output_amount=Decimal(data.get("min_output_amount", "0")),
                updated_at_block=data.get("updated_at_block", 0),
                is_long=data.get("is_long", True),
                is_frozen=data.get("is_frozen", False),
                cancelled_reason=data.get("cancelled_reason", ""),
                frozen_reason=data.get("frozen_reason", ""),
            )
        except Exception as e:
            logger.warning(f"Failed to parse OrderEventData: {e}")
            return None

    def is_gmx_event(self, topic: str | bytes) -> bool:
        """Check if a topic is a known GMX v2 event.

        Args:
            topic: Event topic (supports bytes, hex string with/without 0x, any case)

        Returns:
            True if topic is a known GMX v2 event
        """
        if isinstance(topic, bytes):
            topic = "0x" + topic.hex()
        else:
            topic = str(topic)
        if not topic.startswith("0x"):
            topic = "0x" + topic
        topic = topic.lower()
        return self.registry.is_known_event(topic)

    def get_event_type(self, topic: str | bytes) -> GMXv2EventType:
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
        return self.registry.get_event_type_from_topic(topic) or GMXv2EventType.UNKNOWN

    # =========================================================================
    # Extraction Methods for Result Enrichment
    # =========================================================================

    # ---- VIB-3159: tagged-variant wrappers ----------------------------------
    # See uniswap_v3/receipt_parser.py for rationale. The legacy raw methods
    # below keep their return types for direct callers.

    def _strict_parse(self, receipt: dict[str, Any]) -> ExtractResult[Any] | None:
        """Run ``parse_receipt`` and short-circuit with ``ExtractError`` if it
        reports a crash. See uniswap_v3 equivalent for rationale (VIB-3159)."""
        try:
            parsed = self.parse_receipt(receipt)
        except Exception as exc:  # noqa: BLE001 — malformed receipt shape
            return ExtractError(error=f"{type(exc).__name__}: {exc}", exception=exc)
        if not parsed.success:
            return ExtractError(error=parsed.error or "parse_receipt reported failure")
        return None

    def _wrap_extract(
        self,
        fn: Any,
        receipt: dict[str, Any],
        missing_reason: str,
    ) -> ExtractResult[Any]:
        """Calls ``parse_receipt`` first so actual parse crashes propagate as
        ``ExtractError`` rather than being silently swallowed by the legacy
        extractor's ``except Exception: return None`` (VIB-3159)."""
        err = self._strict_parse(receipt)
        if err is not None:
            return err
        try:
            value = fn(receipt)
        except Exception as exc:  # noqa: BLE001
            return ExtractError(error=f"{type(exc).__name__}: {exc}", exception=exc)
        if value is None:
            return ExtractMissing(reason=missing_reason)
        return ExtractOk(value=value)

    def extract_swap_amounts_result(self, receipt: dict[str, Any]) -> ExtractResult[Any]:
        """Fail-closed variant of :meth:`extract_swap_amounts` — see VIB-3159."""
        return self._wrap_extract(self.extract_swap_amounts, receipt, "no swap order event")

    def extract_async_orders_result(
        self,
        receipt: dict[str, Any],
        *,
        intent_type: str | None = None,
    ) -> ExtractResult[list[AsyncOrderData]]:
        """Extract authoritative GMX ``OrderCreated`` identifiers."""
        err = self._strict_parse(receipt)
        if err is not None:
            return err
        parsed = self.parse_receipt(receipt)
        created_events = [event for event in parsed.events if event.event_type == GMXv2EventType.ORDER_CREATED]
        if not created_events:
            return ExtractMissing(reason="no OrderCreated event")

        orders: list[AsyncOrderData] = []
        for event in created_events:
            result = _async_order_from_created_event(
                event,
                intent_type=intent_type,
                transaction_hash=parsed.transaction_hash,
            )
            if isinstance(result, ExtractError):
                return result
            orders.append(result.value)
        return ExtractOk(value=orders)

    def extract_async_orders(
        self,
        receipt: dict[str, Any],
        *,
        intent_type: str | None = None,
    ) -> list[AsyncOrderData] | None:
        """Legacy-compatible raw accessor for authoritative created orders."""
        result = self.extract_async_orders_result(receipt, intent_type=intent_type)
        if isinstance(result, ExtractOk):
            return result.value
        if isinstance(result, ExtractError):
            logger.warning("Failed to extract GMX asynchronous orders: %s", result.error)
        return None

    def extract_position_id_result(self, receipt: dict[str, Any]) -> ExtractResult[str]:
        """Fail-closed variant of :meth:`extract_position_id` — see VIB-3159."""
        return self._wrap_extract(self.extract_position_id, receipt, "no position increase/decrease key")

    def extract_size_delta_result(self, receipt: dict[str, Any]) -> ExtractResult[Decimal]:
        """Fail-closed variant of :meth:`extract_size_delta` — see VIB-3159."""
        return self._wrap_extract(self.extract_size_delta, receipt, "no size_delta_usd in order")

    def extract_collateral_result(self, receipt: dict[str, Any]) -> ExtractResult[Decimal]:
        """Fail-closed variant of :meth:`extract_collateral` — see VIB-3159."""
        return self._wrap_extract(self.extract_collateral, receipt, "no collateral_delta_amount in order")

    def extract_entry_price_result(self, receipt: dict[str, Any]) -> ExtractResult[Decimal]:
        """Fail-closed variant of :meth:`extract_entry_price` — see VIB-3159."""
        return self._wrap_extract(self.extract_entry_price, receipt, "no PositionIncrease event")

    def extract_leverage_result(self, receipt: dict[str, Any]) -> ExtractResult[Decimal]:
        """Fail-closed variant of :meth:`extract_leverage` — see VIB-3159."""
        return self._wrap_extract(self.extract_leverage, receipt, "insufficient data for leverage")

    def extract_realized_pnl_result(self, receipt: dict[str, Any]) -> ExtractResult[Decimal]:
        """Fail-closed variant of :meth:`extract_realized_pnl` — see VIB-3159."""
        return self._wrap_extract(self.extract_realized_pnl, receipt, "no PositionDecrease with PnL event")

    def extract_exit_price_result(self, receipt: dict[str, Any]) -> ExtractResult[Decimal]:
        """Fail-closed variant of :meth:`extract_exit_price` — see VIB-3159."""
        return self._wrap_extract(self.extract_exit_price, receipt, "no PositionDecrease event")

    def extract_fees_paid_result(self, receipt: dict[str, Any]) -> ExtractResult[int]:
        """Fail-closed variant of :meth:`extract_fees_paid` — see VIB-3159."""
        return self._wrap_extract(self.extract_fees_paid, receipt, "no fees_paid in order")

    def extract_collateral_returned_result(self, receipt: dict[str, Any]) -> ExtractResult[Decimal]:
        """Fail-closed variant of :meth:`extract_collateral_returned` — see VIB-3159."""
        return self._wrap_extract(
            self.extract_collateral_returned,
            receipt,
            "no PositionDecrease event with a decoded collateral_delta_amount",
        )

    def extract_swap_amounts(
        self,
        receipt: dict[str, Any],
        *,
        expected_out: Decimal | None = None,  # noqa: ARG002 — see docstring
    ) -> Any:
        """Extract swap amounts from transaction receipt.

        GMX V2 "swaps" are executed through perpetual orders, not spot swaps.
        For GMX orders:
        - amount_in = initial_collateral_delta_amount (collateral deposited)
        - amount_out = size_delta_usd (position size in USD, scaled by 1e30)
        - effective_price represents the leverage ratio

        The VIB-3203 ``expected_out`` kwarg is accepted for interface parity
        with spot-swap parsers, but NOT used to compute ``slippage_bps`` —
        comparing "realized collateral" to a "quoted collateral" is not the
        same semantic as realized vs quoted swap output, and would produce
        misleading slippage values. Slippage reporting for GMX V2 perps
        (acceptable price vs execution price) is a separate semantic and is
        out of scope for VIB-3203.

        Args:
            receipt: Transaction receipt dict with 'logs' field
            expected_out: Accepted but ignored — see docstring.

        Returns:
            SwapAmounts dataclass if swap order found, None otherwise
        """
        from almanak.framework.execution.extracted_data import SwapAmounts

        try:
            result = self.parse_receipt(receipt)
            # Look for swap-related order events
            for order in result.order_events:
                # Order type 0 is typically a swap in GMX
                if order.order_type == 0:
                    # Values are already decoded decimals from _decode_order_data
                    # (divided by 1e18 for tokens, 1e30 for USD)
                    amount_in_decimal = order.initial_collateral_delta_amount
                    amount_out_decimal = order.size_delta_usd

                    # Reconstruct raw integer amounts for SwapAmounts compatibility
                    amount_in = int(amount_in_decimal * Decimal(10**18))
                    amount_out = int(amount_out_decimal * Decimal(10**30))

                    # Effective price is the leverage ratio for GMX
                    effective_price = amount_out_decimal / amount_in_decimal if amount_in_decimal > 0 else Decimal(0)

                    return SwapAmounts(
                        amount_in=amount_in,
                        amount_out=amount_out,
                        amount_in_decimal=amount_in_decimal,
                        amount_out_decimal=amount_out_decimal,
                        effective_price=effective_price,
                        # VIB-3203: slippage_bps and expected_out_decimal are
                        # INTENTIONALLY unset for GMX V2 — perp orders compare
                        # collateral vs size_usd (not realized vs quoted swap
                        # output), so forwarding the VIB-3203 signal here would
                        # persist a misleading value. The kwarg is accepted at
                        # the signature level for interface parity and then
                        # ignored. Don't "fix" this asymmetry without also
                        # redesigning perp slippage semantics.
                        slippage_bps=None,
                        token_in=None,  # GMX swaps don't have simple token in/out
                        token_out=None,
                    )
            return None
        except Exception as e:
            logger.warning(f"Failed to extract swap amounts: {e}")
            return None

    # =========================================================================
    # Single-fill extractors — LOAD-BEARING INVARIANT (VIB-6110)
    #
    # Every extractor below takes the FIRST matching position / fee event in the
    # receipt (``position_increases[0]``, first PositionFeesCollected, or a sum
    # across all PositionDecrease events). That is only correct while the receipt
    # they are handed carries at most ONE of the strategy's orders.
    #
    # Today that holds by two INCIDENTAL properties, not by any enforcement:
    #   1. On GMX V2 the strategy's own tx is ``createOrder`` and emits only
    #      OrderCreated — the position / fee events land later, in the keeper tx.
    #      These extractors are reached by ``ResultEnricher`` dynamic dispatch
    #      (EXTRACTION_SPECS["PERP_CLOSE"]) on the OWN-tx receipt, where they
    #      therefore find nothing.
    #   2. The one lane that does feed keeper receipts to the enricher
    #      (``additional_receipts``, gated on ``_require_terminal_async_settlement``,
    #      set only by the CLI test lifecycle) uses the Anvil order executor, which
    #      submits ONE executeOrder tx per order key → one order per receipt.
    #
    # The production keeper-receipt path does NOT come through here: it goes
    # ``perp_settlement.py`` → :meth:`extract_perp_fill`, which IS correlated by
    # ``order_key`` precisely because a real keeper tx may batch several orders.
    #
    # If anyone batches the Anvil executor, enables terminal async settlement in
    # production, or routes a keeper receipt through ResultEnricher, every
    # extractor below becomes a live money-path defect at once. Correlate them by
    # ``order_key`` (same pattern as ``extract_perp_fill``) before doing any of
    # those. Follow-up: VIB-6152.
    # =========================================================================

    def extract_position_id(self, receipt: dict[str, Any]) -> str | None:
        """Extract position ID (key) from transaction receipt.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            Position key if found, None otherwise
        """
        try:
            result = self.parse_receipt(receipt)
            # Check position increases first (opening)
            if result.position_increases:
                return result.position_increases[0].key
            # Then check decreases (closing/reducing)
            if result.position_decreases:
                return result.position_decreases[0].key
            return None
        except Exception as e:
            logger.warning(f"Failed to extract position ID: {e}")
            return None

    def extract_size_delta(self, receipt: dict[str, Any]) -> Decimal | None:
        """Extract size delta (in USD) from transaction receipt.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            Size delta in USD if found, None otherwise
        """
        try:
            result = self.parse_receipt(receipt)
            if result.position_increases:
                return result.position_increases[0].size_delta_usd
            if result.position_decreases:
                return result.position_decreases[0].size_delta_usd
            return None
        except Exception as e:
            logger.warning(f"Failed to extract size delta: {e}")
            return None

    def extract_collateral(self, receipt: dict[str, Any]) -> Decimal | None:
        """Extract collateral amount from transaction receipt.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            Collateral amount if found, None otherwise
        """
        try:
            result = self.parse_receipt(receipt)
            if result.position_increases:
                return result.position_increases[0].collateral_amount
            if result.position_decreases:
                return result.position_decreases[0].collateral_amount
            return None
        except Exception as e:
            logger.warning(f"Failed to extract collateral: {e}")
            return None

    def extract_entry_price(self, receipt: dict[str, Any]) -> Decimal | None:
        """Extract entry price from transaction receipt.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            Entry price in USD if found, None otherwise
        """
        try:
            result = self.parse_receipt(receipt)
            if result.position_increases:
                return result.position_increases[0].execution_price
            return None
        except Exception as e:
            logger.warning(f"Failed to extract entry price: {e}")
            return None

    def extract_leverage(self, receipt: dict[str, Any]) -> Decimal | None:
        """Extract leverage from transaction receipt.

        Leverage is calculated as size_in_usd / (collateral_amount * collateral_token_price).

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            Leverage multiplier (e.g., Decimal("10") for 10x) if found, None otherwise.
        """
        try:
            result = self.parse_receipt(receipt)
            if result.position_increases:
                pos = result.position_increases[0]
                # Use collateral token price to convert collateral to USD
                if pos.collateral_amount > 0 and pos.collateral_token_price_max > 0:
                    collateral_value_usd = pos.collateral_amount * pos.collateral_token_price_max
                    return pos.size_in_usd / collateral_value_usd
            return None
        except Exception as e:
            logger.warning(f"Failed to extract leverage: {e}")
            return None

    def extract_realized_pnl(self, receipt: dict[str, Any]) -> Decimal | None:
        """Extract realized PnL from transaction receipt.

        Only available for position decreases (closing/reducing positions).

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            Realized PnL in USD if found, None otherwise
        """
        try:
            result = self.parse_receipt(receipt)
            if result.position_decreases:
                return result.position_decreases[0].realized_pnl
            return None
        except Exception as e:
            logger.warning(f"Failed to extract realized PnL: {e}")
            return None

    def extract_exit_price(self, receipt: dict[str, Any]) -> Decimal | None:
        """Extract exit price from transaction receipt.

        Only available for position decreases (closing/reducing positions).

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            Exit price in USD if found, None otherwise
        """
        try:
            result = self.parse_receipt(receipt)
            if result.position_decreases:
                return result.position_decreases[0].execution_price
            return None
        except Exception as e:
            logger.warning(f"Failed to extract exit price: {e}")
            return None

    def extract_collateral_returned(self, receipt: dict[str, Any]) -> Decimal | None:
        """Extract collateral returned at close from a PERP_CLOSE receipt.

        Sums raw ``collateralDeltaAmount`` values (the collateral withdrawn from the
        position) across every ``PositionDecrease`` event in the receipt. For a
        full close GMX sets the delta to the position's entire remaining
        collateral, so this is the raw collateral-token leg of the close payout.
        Raw smallest-unit semantics match the other perpetual receipt parsers
        and allow exact wallet-delta reconciliation without a guessed decimal
        scale.

        Semantics — what this is NOT: the net wallet credit (GMX's
        ``outputAmount`` = collateral delta ± realized PnL − fees, optionally
        swapped to another token) lives in the ``EventUtils.EventLogData``
        payload and is distinct from this collateral delta. PnL and fees are
        extracted separately (``realized_pnl``, ``fees_paid``).

        Empty != Zero: only values the decoder actually produced are summed.
        Events whose decode fell back to ``raw_data`` carry no
        ``collateral_delta_amount`` key and are skipped; if no event carries a
        decoded value this returns ``None`` (unmeasured), never a fabricated
        zero. A decoded ``0`` (size-only decrease) is a measured zero and is
        returned as ``Decimal("0")``.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            Total collateral withdrawn across PositionDecrease events, or None
            when no event carries a decoded collateral_delta_amount.
        """
        try:
            result = self.parse_receipt(receipt)
            total = Decimal("0")
            found = False
            for event in result.events:
                if event.event_type != GMXv2EventType.POSITION_DECREASE:
                    continue
                raw = event.data.get("collateral_delta_amount_raw")
                if raw is None:
                    # EventUtils or legacy fixture decoding did not produce the
                    # raw field. Skip rather than fabricate a zero.
                    continue
                total += Decimal(raw)
                found = True
            return total if found else None
        except Exception as e:
            logger.warning(f"Failed to extract collateral returned: {e}")
            return None

    def extract_fees_paid(self, receipt: dict[str, Any]) -> int | None:
        """Extract fees paid from transaction receipt.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            Execution fee in wei if found, None otherwise.
        """
        try:
            result = self.parse_receipt(receipt)
            if result.order_events:
                return result.order_events[0].execution_fee
            return None
        except Exception as e:
            logger.warning(f"Failed to extract fees paid: {e}")
            return None

    def extract_funding_fee_usd_result(self, receipt: dict[str, Any]) -> "ExtractResult[Decimal]":
        """Fail-closed variant of :meth:`extract_funding_fee_usd` — see VIB-3159."""
        return self._wrap_extract(
            self.extract_funding_fee_usd,
            receipt,
            "PositionFeesCollected EventUtils decoder not yet implemented",
        )

    def extract_funding_fee_usd(self, receipt: dict[str, Any]) -> Decimal | None:
        """Extract accumulated funding fee in USD from a CLOSE receipt (VIB-3497).

        GMX V2 emits a ``PositionFeesCollected`` event alongside every
        ``PositionDecrease``. The ``fundingFeeAmount`` field (collateral-token
        units) lives inside that event's keyed ``EventUtils.EventLogData``
        payload. This decodes it BY NAME (VIB-3873) and converts it to USD with
        the SAME event's ``collateralTokenPrice`` bounds — a decimals-free
        conversion (``amount * price / 1e30``) that needs no live oracle read, so
        the parser stays a pure function of the receipt.

        Empty != Zero: returns ``None`` when the receipt carries no
        ``PositionFeesCollected`` event (or the funding amount / price could not
        be decoded) — "funding cost unknown" — never a fabricated ``0``. A
        measured zero funding fee (short hold) returns ``Decimal("0")``.

        Returns:
            Funding fee in USD, or ``None`` when unmeasured.
        """
        try:
            result = self.parse_receipt(receipt)
        except Exception as e:  # noqa: BLE001 — malformed receipt shape
            logger.warning(f"Failed to extract funding fee: {e}")
            return None
        for event in result.events:
            if event.event_type != GMXv2EventType.POSITION_FEES_COLLECTED:
                continue
            fees = self._decode_event_utils_position_fees(str(event.raw_data or "").removeprefix("0x"))
            if fees is not None and fees.get("funding_fee_usd") is not None:
                return fees["funding_fee_usd"]
        return None

    # =========================================================================
    # Perp fill economics (VIB-3873 / VIB-3872 WI-1)
    # =========================================================================

    def _event_emitter_address(self) -> str | None:
        """Canonical GMX ``EventEmitter`` for this parser's chain, lowercased.

        ``None`` when the parser has no ``chain`` or the chain is not a GMX V2
        deployment — callers on the money path must treat that as unmeasured,
        never as "accept any emitter".
        """
        emitter = GMX_V2.get(str(self.chain or "").lower(), {}).get("event_emitter")
        return str(emitter).lower() if emitter else None

    @staticmethod
    def _normalized_order_key_filter(order_key: str | None) -> str | None:
        """Lowercase / ``0x``-prefix a watched order key, or ``None`` if absent."""
        text = str(order_key or "").strip().lower()
        if not text:
            return None
        return text if text.startswith("0x") else "0x" + text

    @staticmethod
    def _order_key_matches(decoded: dict[str, Any] | None, wanted: str | None) -> bool:
        """True when ``decoded`` may be attributed to the watched order (VIB-6110).

        ``wanted is None`` is the uncorrelated call and accepts any decoded
        event. When a watched order key IS supplied the event must carry that
        exact key: an event whose ``orderKey`` is absent or empty is NOT a
        match. Fail-closed is mandatory here — a keeper transaction routinely
        batches several orders, and attributing another order's market,
        direction and execution price to the watched settlement is silently
        wrong money, not a merely malformed row.
        """
        if decoded is None:
            return False
        if wanted is None:
            return True
        candidate = str(decoded.get("order_key") or "").strip().lower()
        return bool(candidate) and candidate == wanted

    def _select_fill_events(
        self,
        events: list[Any],
        wanted: str | None,
        emitter: str | None = None,
    ) -> tuple[dict[str, Any] | None, dict[str, Any] | None, dict[str, Any] | None]:
        """Pick the position + fee events for the watched order (VIB-6110).

        Returns ``(increase, decrease, fees)``, each ``None`` when the receipt
        holds no event of that type attributable to ``wanted``. With
        ``wanted is None`` this is the legacy first-wins selection. Decoding is
        attempted per candidate so a malformed event does not mask a later valid
        one of the same type.

        ``emitter`` authenticates the SOURCE. It is applied to each parsed
        event's ``contract_address`` rather than by pre-filtering the receipt's
        logs, and that distinction is load-bearing: a filtered *receipt* keeps
        the original ``transactionHash``, and ``ResultEnricher``'s
        ``_install_parse_cache`` keys its ``parse_receipt`` cache on exactly
        that hash — so a cache warmed with the unfiltered receipt would hand
        back the unfiltered parse and a forged fill would sail straight through
        the filter. ``_merge_receipt_logs`` stamps a synthetic hash to dodge the
        same trap. Filtering post-parse is immune to it, because
        ``contract_address`` is carried on every cached event.
        """
        decoders = {
            GMXv2EventType.POSITION_INCREASE: self._decode_event_utils_position_increase,
            GMXv2EventType.POSITION_DECREASE: self._decode_event_utils_position_decrease,
            GMXv2EventType.POSITION_FEES_COLLECTED: self._decode_event_utils_position_fees,
        }
        found: dict[Any, dict[str, Any] | None] = {}
        for event in events:
            decoder = decoders.get(event.event_type)
            if decoder is None or found.get(event.event_type) is not None:
                continue
            if emitter is not None and str(getattr(event, "contract_address", "") or "").lower() != emitter:
                continue  # not the canonical EventEmitter — unattributable is not authentic
            candidate = decoder(str(event.raw_data or "").removeprefix("0x"))
            if self._order_key_matches(candidate, wanted):
                found[event.event_type] = candidate
        return (
            found.get(GMXv2EventType.POSITION_INCREASE),
            found.get(GMXv2EventType.POSITION_DECREASE),
            found.get(GMXv2EventType.POSITION_FEES_COLLECTED),
        )

    def _collect_execution_fee_events(
        self,
        events: list[Any],
        emitter: str | None,
    ) -> tuple[list[int], list[tuple[int, str | None]], list[str]]:
        """Decode the three execution-fee-settlement event kinds from a keeper receipt.

        Returns ``(keeper_fee_amounts, (refund_amount, receiver) pairs,
        OrderExecuted accounts)`` — every authentic occurrence, undeduplicated and
        unjudged. Split out from :meth:`_select_execution_fee_events` so the
        DECISION (how many of each is attributable, and to whom) reads as the small
        set of rules it is, rather than being buried in decode plumbing.

        ``emitter`` authentication is applied here, per-event, on the parsed
        ``contract_address`` — never by pre-filtering the receipt's logs, for the
        parse-cache reason spelled out in :meth:`_select_fill_events`.
        """
        fees: list[int] = []
        refunds: list[tuple[int, str | None]] = []
        executed_accounts: list[str] = []
        for event in events:
            if emitter is not None and str(getattr(event, "contract_address", "") or "").lower() != emitter:
                continue  # not the canonical EventEmitter — unattributable is not authentic
            name = _EXECUTION_FEE_EVENT_NAMES.get(event.event_type)
            if name is None:
                continue
            items = self._decode_event_utils_data(str(event.raw_data or "").removeprefix("0x"), name)
            if items is None:
                continue
            uints = items.get("uints") or {}
            addresses = items.get("addresses") or {}
            if event.event_type is GMXv2EventType.ORDER_EXECUTED:
                account = addresses.get("account")
                if account:
                    executed_accounts.append(str(account).lower())
            elif event.event_type is GMXv2EventType.KEEPER_EXECUTION_FEE:
                amount = uints.get("executionFeeAmount")
                if isinstance(amount, int) and not isinstance(amount, bool):
                    fees.append(amount)
            else:
                amount = uints.get("refundFeeAmount")
                if isinstance(amount, int) and not isinstance(amount, bool):
                    receiver = addresses.get("receiver")
                    refunds.append((amount, str(receiver).lower() if receiver else None))
        return fees, refunds, executed_accounts

    def _select_execution_fee_events(
        self,
        events: list[Any],
        emitter: str | None,
        account: str | None,
    ) -> tuple[int | None, int | None]:
        """Pick ``(keeper_execution_fee_wei, execution_fee_refund_wei)`` (VIB-6061).

        Both are read from the KEEPER transaction. GMX escrows our ``msg.value``
        at ``createOrder`` and ``GasUtils.payExecutionFee`` splits it on execution:
        ``KeeperExecutionFee.executionFeeAmount`` to the keeper, the remainder back
        to the order's account as ``ExecutionFeeRefund.refundFeeAmount``. The keeper's
        cut IS what we consumed, so no subtraction is needed — the refund is carried
        alongside purely so the pair can be checked against the escrow.

        **Why this fails closed on a batched receipt.** Unlike the position and fee
        events, ``KeeperExecutionFee`` carries no ``orderKey`` — nothing in it names
        the order it settles. When a keeper transaction executes several orders it
        emits one such event per order, and there is no field to correlate them by.
        Attributing an arbitrary one to the watched settlement would book another
        trader's fee against this strategy, so the ONLY safe reading is the
        unambiguous one: exactly one event of each kind in the receipt. Anything
        else returns ``(None, None)`` — unmeasured, which the Cost Stack renders as
        "—" rather than as a fabricated zero (Empty ≠ Zero).

        ``account`` is the strategy wallet and is REQUIRED — a blank one yields
        ``(None, None)``. The refund's ``receiver`` must equal it, which is what
        proves the escrow being split was OURS. Without that check a single-order
        keeper transaction settling somebody else's order would book their fee
        against us, and every such receipt is single-order by definition — the count
        guard above cannot see it. Fail-closed rather than "accept any account when
        none is given", because ownership is not something a default can assert:
        there is no owner to compare against, so there is no measurement. When the
        refund event is absent or names a different receiver the keeper fee is
        dropped too, since the pair is what carries the proof of ownership.

        ``emitter`` authenticates the SOURCE, applied post-parse to each event's
        ``contract_address`` for exactly the reason spelled out in
        :meth:`_select_fill_events` — a pre-filtered receipt keeps its
        ``transactionHash`` and is defeatable through ``ResultEnricher``'s parse
        cache.
        """
        wanted_account = str(account or "").strip().lower() or None
        if wanted_account is None:
            return (None, None)
        fees, refunds, executed_accounts = self._collect_execution_fee_events(events, emitter)

        # Exactly one fee, always. See the batching argument above.
        if len(fees) != 1:
            return (None, None)

        # A FULLY CONSUMED ESCROW EMITS NO REFUND EVENT — and that is the LARGEST
        # fee there is. ``GasUtils.payExecutionFee`` emits ``KeeperExecutionFee``,
        # then computes ``refundFeeAmount = executionFee - executionFeeForKeeper``
        # and RETURNS EARLY when it is zero, before ``emitExecutionFeeRefund``.
        # Requiring the refund event would therefore drop precisely the worst-case
        # cost and render it "not measured" — this ticket's own defect, inverted.
        #
        # Ownership still has to be proven, and without a refund receiver the anchor
        # is ``OrderExecuted.account``, which names the order's owner directly (on
        # the standard path it IS the refund receiver; verified equal on both
        # mainnet fixture legs). Still exactly-one, for the same batching reason.
        #
        # The returned zero is a MEASURED zero — the chain proved the whole escrow
        # was consumed — not an unmeasured ``None``.
        if not refunds:
            if len(executed_accounts) != 1 or executed_accounts[0] != wanted_account:
                return (None, None)
            return (fees[0], 0)

        if len(refunds) != 1:
            return (None, None)
        refund_amount, refund_receiver = refunds[0]
        if refund_receiver != wanted_account:
            return (None, None)
        return (fees[0], refund_amount)

    def extract_keeper_execution_fee(
        self,
        receipt: dict[str, Any],
        *,
        account: str | None = None,
    ) -> tuple[int | None, int | None]:
        """Native-wei ``(keeper_fee, refund)`` from a keeper receipt (VIB-6061).

        Returns ``(None, None)`` when the receipt carries no unambiguous, authentic,
        account-owned pair — see :meth:`_select_execution_fee_events`. Never raises:
        a malformed receipt is unmeasured, not an error, because this rides alongside
        the fill economics and must never take a settlement write down with it.
        """
        emitter = self._event_emitter_address()
        if emitter is None:
            # An unknown chain cannot authenticate the emitter, and an unauthenticated
            # fee is a number an arbitrary contract can write. Refuse it.
            logger.warning(
                "gmx_v2_execution_fee_emitter_unresolved: no known EventEmitter for chain %r — "
                "keeper execution fee unmeasured",
                self.chain,
            )
            return (None, None)
        try:
            result = self.parse_receipt(receipt)
        except Exception as exc:  # noqa: BLE001 — malformed receipt ⇒ unmeasured
            logger.debug("GMX keeper execution-fee parse failed: %s", exc, exc_info=True)
            return (None, None)
        if not result.success:
            return (None, None)
        return self._select_execution_fee_events(result.events, emitter, account)

    def extract_perp_fill_result(
        self,
        receipt: dict[str, Any],
        order_key: str | None = None,
        account: str | None = None,
        index_token_decimals_override: int | None = None,
    ) -> ExtractResult[PerpFillData]:
        """Fail-closed variant of :meth:`extract_perp_fill` — see VIB-3159.

        ``account`` is threaded through (VIB-6061) so this wrapper measures the same
        fields as the method it wraps. Dropping it here would leave the keeper
        execution fee unmeasured BY CONSTRUCTION for every caller of the fail-closed
        variant — a silent divergence between two functions whose whole contract is
        that they differ only in error handling.
        """
        missing = "no PositionIncrease/PositionDecrease event to build fill economics"
        if self._normalized_order_key_filter(order_key) is not None:
            missing = f"no PositionIncrease/PositionDecrease event for order {order_key} to build fill economics"
        return self._wrap_extract(
            lambda parsed_receipt: self.extract_perp_fill(
                parsed_receipt,
                order_key=order_key,
                account=account,
                index_token_decimals_override=index_token_decimals_override,
            ),
            receipt,
            missing,
        )

    def extract_perp_fill(
        self,
        receipt: dict[str, Any],
        order_key: str | None = None,
        account: str | None = None,
        index_token_decimals_override: int | None = None,
    ) -> PerpFillData | None:
        """Build typed fill economics from a GMX keeper receipt (VIB-3872 WI-1).

        Merges the receipt's ``PositionIncrease`` **or** ``PositionDecrease``
        event (position identity, execution price, size / collateral deltas,
        price impact, realized PnL) with its ``PositionFeesCollected`` event
        (funding / position / borrowing fees in USD). Every field is decoded BY
        NAME from the keyed EventUtils payload and follows Empty != Zero.

        ``order_key`` is the settlement being measured (VIB-6110). A GMX keeper
        transaction may execute SEVERAL orders — on different markets, in
        different directions — so the position and fee events must be
        correlated to the watched order rather than taken first-wins.
        Uncorrelated, closing order B inside a batch that also opened order A
        returns A's fill: ``is_open=True`` with A's market and A's entry price,
        and B's real exit price is lost. Because the impostor price is itself
        plausible, nothing downstream can detect the substitution.

        Returns ``None`` when the receipt carries no PositionIncrease /
        PositionDecrease event attributable to ``order_key`` (nothing to
        settle) — callers map that to UNMEASURED rather than to a fill. A
        keeper receipt that only yields the position event (no matching fees
        event) still returns a PerpFillData with the fee fields left ``None``
        (unmeasured). Passing ``order_key=None`` keeps the legacy first-wins
        behaviour for callers that have no watched order.

        **Source authentication.** When ``order_key`` is supplied, only logs
        emitted by the chain's canonical GMX ``EventEmitter`` are considered.
        Correlating on an ``orderKey`` decoded from an *unauthenticated* log
        would not be correlation at all: ``_parse_log`` recognises GMX events by
        topic hash alone and never checks ``log["address"]``, and a GMX order
        carries an owner-chosen ``callbackContract`` that runs inside the very
        keeper transaction this method parses. A co-batched adversary can read
        the order nonce from ``DataStore``, compute our key, and emit a forged
        ``PositionDecrease`` carrying it with an arbitrary market and price —
        moving the attack from "be first in the log list" to "write the right
        32 bytes". Restricting to the EventEmitter is what makes the decoded
        ``orderKey`` trustworthy. If the emitter cannot be resolved (unknown or
        absent ``chain``) a correlated call fails closed to ``None``: an
        unmeasured settlement is recoverable, a forged one is not.

        The emitter check runs against each PARSED event's ``contract_address``
        (see :meth:`_select_fill_events`), never by pre-filtering the receipt's
        logs — a filtered receipt keeps its ``transactionHash``, which is the
        key ``ResultEnricher``'s parse cache uses, so pre-filtering is
        defeatable by a warmed cache.

        ``account`` (VIB-6061) is the strategy wallet. Supplying it additionally
        measures the keeper execution fee — the native-token cost of the fill that
        is NOT transaction gas and that the Cost Stack previously showed nowhere.
        Omitting it leaves ``keeper_execution_fee_wei`` unmeasured; it never
        affects the fill economics above.
        """
        wanted = self._normalized_order_key_filter(order_key)
        if wanted is None and order_key is not None:
            logger.warning(
                "gmx_v2_perp_fill_order_key_blank: order_key=%r normalized away — falling back to "
                "UNCORRELATED first-wins selection; a batched keeper tx may yield another order's fill",
                order_key,
            )
        emitter: str | None = None
        if wanted is not None:
            emitter = self._event_emitter_address()
            if emitter is None:
                logger.warning(
                    "gmx_v2_perp_fill_emitter_unresolved: refusing to correlate order %s without a "
                    "known EventEmitter for chain %r — fill economics unmeasured",
                    order_key,
                    self.chain,
                )
                return None

        result = self.parse_receipt(receipt)
        if not result.success:
            return None

        increase_raw, decrease_raw, fees = self._select_fill_events(result.events, wanted, emitter)

        position = increase_raw if increase_raw is not None else decrease_raw
        if position is None:
            return None
        is_open = increase_raw is not None

        # VIB-6061 — the keeper execution fee rides on the same receipt. Scoped to
        # the canonical emitter even on the uncorrelated (``order_key=None``) path:
        # unlike the fill events there is no order key to correlate on, so the
        # emitter is the ONLY authentication available and dropping it here would
        # let any contract in the transaction write this number.
        fee_emitter = emitter if emitter is not None else self._event_emitter_address()
        keeper_fee_wei, refund_wei = (
            self._select_execution_fee_events(result.events, fee_emitter, account)
            if fee_emitter is not None
            else (None, None)
        )

        # collateralDeltaAmount lives in intItems (increase) vs uintItems (decrease).
        collateral_delta = position.get("collateral_delta_amount_raw")
        if not is_open:
            collateral_delta = position.get("collateral_delta_amount_raw_int")

        return PerpFillData(
            is_open=is_open,
            is_long=position.get("is_long"),
            market=position.get("market") or None,
            collateral_token=position.get("collateral_token") or None,
            position_key=position.get("position_key") or None,
            order_key=position.get("order_key") or None,
            entry_price=(
                self._execution_price_usd(
                    position.get("execution_price_raw"),
                    position.get("market"),
                    index_token_decimals_override,
                )
                if is_open
                else None
            ),
            exit_price=(
                self._execution_price_usd(
                    position.get("execution_price_raw"),
                    position.get("market"),
                    index_token_decimals_override,
                )
                if not is_open
                else None
            ),
            size_delta_usd=self._usd_from_1e30(position.get("size_delta_usd_raw")),
            size_delta_in_tokens=(
                Decimal(position["size_delta_in_tokens_raw"])
                if isinstance(position.get("size_delta_in_tokens_raw"), int)
                else None
            ),
            collateral_delta_amount=(Decimal(collateral_delta) if isinstance(collateral_delta, int) else None),
            price_impact_usd=self._usd_from_1e30(position.get("price_impact_usd_raw")),
            realized_pnl_usd=self._usd_from_1e30(position.get("realized_pnl_raw")) if not is_open else None,
            position_fee_usd=fees.get("position_fee_usd") if fees else None,
            funding_fee_usd=fees.get("funding_fee_usd") if fees else None,
            borrowing_fee_usd=fees.get("borrowing_fee_usd") if fees else None,
            keeper_tx_hash=result.transaction_hash or None,
            block_number=result.block_number or None,
            keeper_execution_fee_wei=keeper_fee_wei,
            execution_fee_refund_wei=refund_wei,
        )

    # =============================================================================
    # Protocol Fee Extraction (VIB-3204)
    # =============================================================================

    def extract_protocol_fees(self, _receipt: dict[str, Any]) -> None:
        """Placeholder for GMX V2 perp-fee extraction (VIB-3204).

        GMX V2 encodes open / close fees in ``PositionFeesInfo`` events
        emitted by the position handler. Decoding those events (including
        borrowing, funding, and execution-fee components) is non-trivial
        and is deferred to a follow-up; ``extract_fees_paid`` already
        surfaces the execution fee in wei for operator-level accounting.

        Follow-up ticket: "Perps fee extraction for GMX V2 / Drift —
        follow-up to VIB-3204".
        """
        return None


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    "GMXv2ReceiptParser",
    "GMXv2Event",
    "GMXv2EventType",
    "GMXOrderTypeError",
    "GMX_MAX_ORDER_TYPE",
    "PerpFillData",
    "PositionIncreaseData",
    "PositionDecreaseData",
    "OrderEventData",
    "ParseResult",
    "EVENT_TOPICS",
    "TOPIC_TO_EVENT",
    "EVENT_NAME_TO_TYPE",
]
