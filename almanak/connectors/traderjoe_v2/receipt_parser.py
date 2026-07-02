"""TraderJoe V2 Receipt Parser (Refactored).

Refactored to use base infrastructure utilities while maintaining backward compatibility.
TraderJoe V2 uses ERC-1155 liquidity bins with dynamic arrays for bin IDs and amounts.
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import TYPE_CHECKING, Any

from almanak.connectors._strategy_base.base import EventRegistry, HexDecoder
from almanak.framework.data.tokens import get_token_resolver

if TYPE_CHECKING:
    from almanak.connectors._strategy_base.primitive_money_leg import PrimitiveMoneyLeg, PrimitiveMoneyLegs
    from almanak.framework.execution.extracted_data import LPCloseData, LPOpenData, ProtocolFees, SwapAmounts
from almanak.framework.utils.log_formatters import format_gas_cost, format_tx_hash

logger = logging.getLogger(__name__)


# =============================================================================
# Event Topic Signatures
# =============================================================================

EVENT_TOPICS: dict[str, str] = {
    # DepositedToBins(address,address,uint256[],bytes32[])
    "DepositedToBins": "0x87f1f9dcf5e8089a3e00811b6a008d8f30293a3da878cb1fe8c90ca376402f8a",
    # WithdrawnFromBins(address,address,uint256[],bytes32[])
    "WithdrawnFromBins": "0xa32e146844d6144a22e94c586715a1317d58a8aa3581ec33d040113ddcb24350",
    # TransferBatch(address,address,address,uint256[],uint256[]) - ERC1155
    "TransferBatch": "0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb",
    # Transfer(address,address,uint256) - ERC20
    "Transfer": "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
    # Approval(address,address,uint256) - ERC20
    "Approval": "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925",
    # ClaimedFees(address indexed sender, address indexed to, uint256[] ids, bytes32[] amounts)
    # This event is emitted by LBPair.collectFees()
    "ClaimedFees": "0xdf71cf7e8bfaf5953702c2be6d726b8f61d37bf9d90fda35b7bbb3981264e24d",
    # Deposit(address,uint256) - WAVAX wrap
    "Deposit": "0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c",
    # Withdrawal(address,uint256) - WAVAX unwrap
    "Withdrawal": "0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65",
}

TOPIC_TO_EVENT: dict[str, str] = {v: k for k, v in EVENT_TOPICS.items()}

# Individual topic constants for backward compatibility
DEPOSITED_TO_BINS_TOPIC = EVENT_TOPICS["DepositedToBins"]
WITHDRAWN_FROM_BINS_TOPIC = EVENT_TOPICS["WithdrawnFromBins"]


# =============================================================================
# Enums
# =============================================================================


class TraderJoeV2EventType(Enum):
    """TraderJoe V2 event types."""

    DEPOSITED_TO_BINS = "DEPOSITED_TO_BINS"
    WITHDRAWN_FROM_BINS = "WITHDRAWN_FROM_BINS"
    TRANSFER_BATCH = "TRANSFER_BATCH"
    TRANSFER = "TRANSFER"
    APPROVAL = "APPROVAL"
    CLAIMED_FEES = "CLAIMED_FEES"
    DEPOSIT = "DEPOSIT"
    WITHDRAWAL = "WITHDRAWAL"
    UNKNOWN = "UNKNOWN"


EVENT_NAME_TO_TYPE: dict[str, TraderJoeV2EventType] = {
    "DepositedToBins": TraderJoeV2EventType.DEPOSITED_TO_BINS,
    "WithdrawnFromBins": TraderJoeV2EventType.WITHDRAWN_FROM_BINS,
    "TransferBatch": TraderJoeV2EventType.TRANSFER_BATCH,
    "Transfer": TraderJoeV2EventType.TRANSFER,
    "Approval": TraderJoeV2EventType.APPROVAL,
    "ClaimedFees": TraderJoeV2EventType.CLAIMED_FEES,
    "Deposit": TraderJoeV2EventType.DEPOSIT,
    "Withdrawal": TraderJoeV2EventType.WITHDRAWAL,
}


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class TraderJoeV2Event:
    """Parsed TraderJoe V2 event."""

    event_type: TraderJoeV2EventType
    event_name: str
    log_index: int
    transaction_hash: str
    block_number: int
    contract_address: str
    data: dict[str, Any]
    raw_topics: list[str] = field(default_factory=list)
    raw_data: str = ""
    timestamp: datetime | None = None


@dataclass
class SwapEventData:
    """Parsed data from a swap (Transfer events)."""

    token_in: str
    token_out: str
    amount_in: int
    amount_out: int
    sender: str
    recipient: str


@dataclass
class LiquidityEventData:
    """Parsed data from add/remove liquidity events."""

    pool_address: str
    sender: str
    to: str
    bin_ids: list[int]
    amounts_x: list[int] = field(default_factory=list)
    amounts_y: list[int] = field(default_factory=list)
    total_amount_x: int = 0
    total_amount_y: int = 0


@dataclass
class TransferEventData:
    """Parsed data from Transfer event."""

    token: str
    from_address: str
    to_address: str
    amount: int


@dataclass
class ParsedSwapResult:
    """Result of parsing a swap transaction."""

    success: bool
    token_in: str | None = None
    token_out: str | None = None
    amount_in: int | None = None
    amount_out: int | None = None
    price: Decimal | None = None
    gas_used: int | None = None
    block_number: int | None = None
    timestamp: datetime | None = None


@dataclass
class ParsedLiquidityResult:
    """Result of parsing a liquidity transaction."""

    success: bool
    is_add: bool = True
    pool_address: str | None = None
    bin_ids: list[int] = field(default_factory=list)
    amount_x: int = 0
    amount_y: int = 0
    gas_used: int | None = None
    block_number: int | None = None


@dataclass
class ParsedFeeCollectionResult:
    """Result of parsing a fee collection transaction."""

    success: bool
    pool_address: str | None = None
    bin_ids: list[int] = field(default_factory=list)
    fees_x: int = 0
    fees_y: int = 0
    gas_used: int | None = None
    block_number: int | None = None


@dataclass
class ParseResult:
    """Result of parsing a transaction receipt."""

    success: bool
    transaction_hash: str
    block_number: int
    gas_used: int
    events: list[TraderJoeV2Event] = field(default_factory=list)
    swap_result: ParsedSwapResult | None = None
    liquidity_result: ParsedLiquidityResult | None = None
    error: str | None = None


# =============================================================================
# Parser Class
# =============================================================================


class TraderJoeV2ReceiptParser:
    """Parser for TraderJoe V2 transaction receipts.

    Refactored to use base infrastructure utilities for hex decoding
    and event registry management. Now properly parses dynamic arrays
    in DepositedToBins and WithdrawnFromBins events.
    """

    def __init__(self, chain: str | None = None, **kwargs: Any) -> None:
        """Initialize the parser.

        Args:
            chain: Chain name for token decimal resolution (e.g. "avalanche", "bsc").
                When provided, extract_swap_amounts() uses actual token decimals
                instead of assuming 18 for all tokens.
            **kwargs: Additional arguments (ignored for compatibility)
        """
        _ = kwargs  # Explicitly unused for forward compatibility
        self._chain = chain
        self.registry = EventRegistry(EVENT_TOPICS, EVENT_NAME_TO_TYPE)

    def parse_receipt(self, receipt: dict[str, Any]) -> ParseResult:
        """Parse a transaction receipt.

        Args:
            receipt: Web3 transaction receipt dict

        Returns:
            ParseResult with extracted data
        """
        try:
            # Normalize transaction hash
            tx_hash_raw = receipt.get("transactionHash", "")
            if isinstance(tx_hash_raw, bytes):
                tx_hash = "0x" + tx_hash_raw.hex()
            elif isinstance(tx_hash_raw, str):
                tx_hash = tx_hash_raw
            else:
                tx_hash = ""

            block_number = receipt.get("blockNumber", 0)
            gas_used = receipt.get("gasUsed", 0)
            logs = receipt.get("logs", [])

            # Parse all events
            events = self._parse_logs(logs, tx_hash, block_number)

            # Check transaction status
            status = receipt.get("status", 1)
            if status != 1:
                return ParseResult(
                    success=False,
                    transaction_hash=tx_hash,
                    block_number=block_number,
                    gas_used=gas_used,
                    events=events,
                    error="Transaction reverted",
                )

            # Try to extract swap result
            swap_result = self._extract_swap_result(events, gas_used, block_number)

            # Try to extract liquidity result
            liquidity_result = self._extract_liquidity_result(events, gas_used, block_number)

            # Log parsed receipt with user-friendly formatting
            tx_fmt = format_tx_hash(tx_hash)
            gas_fmt = format_gas_cost(gas_used)

            if liquidity_result and liquidity_result.success:
                action = "ADD" if liquidity_result.is_add else "REMOVE"
                logger.info(
                    f"🔍 Parsed TraderJoe V2 {action} liquidity: bins={len(liquidity_result.bin_ids)}, "
                    f"tx={tx_fmt}, {gas_fmt}"
                )
            elif swap_result and swap_result.success:
                logger.info(
                    f"🔍 Parsed TraderJoe V2 swap: {swap_result.amount_in:,} → {swap_result.amount_out:,}, "
                    f"tx={tx_fmt}, {gas_fmt}"
                )
            else:
                logger.info(f"🔍 Parsed TraderJoe V2 receipt: tx={tx_fmt}, events={len(events)}, {gas_fmt}")

            return ParseResult(
                success=True,
                transaction_hash=tx_hash,
                block_number=block_number,
                gas_used=gas_used,
                events=events,
                swap_result=swap_result,
                liquidity_result=liquidity_result,
            )

        except Exception as e:
            logger.error(f"Failed to parse receipt: {e}")
            return ParseResult(
                success=False,
                transaction_hash=str(receipt.get("transactionHash", "")),
                block_number=receipt.get("blockNumber", 0),
                gas_used=receipt.get("gasUsed", 0),
                error=str(e),
            )

    def _parse_logs(
        self,
        logs: list[dict[str, Any]],
        tx_hash: str,
        block_number: int,
    ) -> list[TraderJoeV2Event]:
        """Parse logs into TraderJoeV2Event objects."""
        events = []

        for i, log in enumerate(logs):
            topics = log.get("topics", [])
            if not topics:
                continue

            # Normalize first topic (event signature)
            first_topic = topics[0]
            if isinstance(first_topic, bytes):
                first_topic = "0x" + first_topic.hex()
            elif not first_topic.startswith("0x"):
                first_topic = "0x" + first_topic

            # Look up event name
            event_name = self.registry.get_event_name(first_topic.lower())
            if event_name is None:
                continue

            event_type = self.registry.get_event_type(event_name) or TraderJoeV2EventType.UNKNOWN

            # Get contract address
            address = log.get("address", "")
            if isinstance(address, bytes):
                address = "0x" + address.hex()

            # Parse event data based on type
            data = self._parse_event_data(event_type, log)

            # Get raw data
            raw_data = log.get("data", "")
            if isinstance(raw_data, bytes):
                raw_data = "0x" + raw_data.hex()

            # Get raw topics
            raw_topics = []
            for topic in topics:
                if isinstance(topic, bytes):
                    raw_topics.append("0x" + topic.hex())
                else:
                    raw_topics.append(topic)

            event = TraderJoeV2Event(
                event_type=event_type,
                event_name=event_name,
                log_index=i,
                transaction_hash=tx_hash,
                block_number=block_number,
                contract_address=address,
                data=data,
                raw_topics=raw_topics,
                raw_data=raw_data,
            )
            events.append(event)

        return events

    def _parse_event_data(  # noqa: C901
        self,
        event_type: TraderJoeV2EventType,
        log: dict[str, Any],
    ) -> dict[str, Any]:
        """Parse event data based on event type."""
        topics = log.get("topics", [])
        data = log.get("data", "")

        # Normalize data to hex string
        data_hex = HexDecoder.normalize_hex(data)

        result: dict[str, Any] = {}

        try:
            if event_type == TraderJoeV2EventType.TRANSFER:
                # Transfer(address indexed from, address indexed to, uint256 value)
                if len(topics) >= 3:
                    result["from"] = HexDecoder.topic_to_address(topics[1])
                    result["to"] = HexDecoder.topic_to_address(topics[2])
                result["value"] = HexDecoder.decode_uint256(data_hex, 0)

            elif event_type == TraderJoeV2EventType.APPROVAL:
                # Approval(address indexed owner, address indexed spender, uint256 value)
                if len(topics) >= 3:
                    result["owner"] = HexDecoder.topic_to_address(topics[1])
                    result["spender"] = HexDecoder.topic_to_address(topics[2])
                result["value"] = HexDecoder.decode_uint256(data_hex, 0)

            elif event_type == TraderJoeV2EventType.DEPOSITED_TO_BINS:
                # DepositedToBins(address indexed sender, address indexed to, uint256[] ids, bytes32[] amounts)
                if len(topics) >= 3:
                    result["sender"] = HexDecoder.topic_to_address(topics[1])
                    result["to"] = HexDecoder.topic_to_address(topics[2])
                # ids and amounts are dynamic arrays (complex to parse)
                # Store raw_data for now, matching original behavior
                result["raw_data"] = data_hex

            elif event_type == TraderJoeV2EventType.WITHDRAWN_FROM_BINS:
                # WithdrawnFromBins(address indexed sender, address indexed to, uint256[] ids, bytes32[] amounts)
                if len(topics) >= 3:
                    result["sender"] = HexDecoder.topic_to_address(topics[1])
                    result["to"] = HexDecoder.topic_to_address(topics[2])
                # ids and amounts are dynamic arrays (complex to parse)
                # Store raw_data for now, matching original behavior
                result["raw_data"] = data_hex

            elif event_type == TraderJoeV2EventType.CLAIMED_FEES:
                # ClaimedFees(address indexed sender, address indexed to, uint256[] ids, bytes32[] amounts)
                if len(topics) >= 3:
                    result["sender"] = HexDecoder.topic_to_address(topics[1])
                    result["to"] = HexDecoder.topic_to_address(topics[2])
                result["raw_data"] = data_hex

            elif event_type == TraderJoeV2EventType.DEPOSIT:
                # Deposit(address indexed dst, uint256 wad)
                if len(topics) >= 2:
                    result["dst"] = HexDecoder.topic_to_address(topics[1])
                result["wad"] = HexDecoder.decode_uint256(data_hex, 0)

            elif event_type == TraderJoeV2EventType.WITHDRAWAL:
                # Withdrawal(address indexed src, uint256 wad)
                if len(topics) >= 2:
                    result["src"] = HexDecoder.topic_to_address(topics[1])
                result["wad"] = HexDecoder.decode_uint256(data_hex, 0)

        except Exception as e:
            logger.warning(f"Failed to parse {event_type} event data: {e}")

        return result

    def _extract_swap_result(
        self,
        events: list[TraderJoeV2Event],
        gas_used: int,
        block_number: int,
    ) -> ParsedSwapResult | None:
        """Extract swap result from Transfer events."""
        # Get all Transfer events
        transfers = [e for e in events if e.event_type == TraderJoeV2EventType.TRANSFER]

        if len(transfers) < 2:
            return None

        # First transfer: user -> pool (token in)
        # Last transfer: pool -> user (token out)
        transfer_in = transfers[0]
        transfer_out = transfers[-1]

        try:
            amount_in = transfer_in.data.get("value", 0)
            amount_out = transfer_out.data.get("value", 0)

            if amount_in <= 0 or amount_out <= 0:
                return None

            # Calculate price
            price = Decimal(amount_out) / Decimal(amount_in) if amount_in > 0 else Decimal(0)

            return ParsedSwapResult(
                success=True,
                token_in=transfer_in.contract_address,
                token_out=transfer_out.contract_address,
                amount_in=amount_in,
                amount_out=amount_out,
                price=price,
                gas_used=gas_used,
                block_number=block_number,
            )

        except Exception as e:
            logger.warning(f"Failed to extract swap result: {e}")
            return None

    def _extract_liquidity_result(
        self,
        events: list[TraderJoeV2Event],
        gas_used: int,
        block_number: int,
    ) -> ParsedLiquidityResult | None:
        """Extract liquidity result from DepositedToBins or WithdrawnFromBins events."""
        # Look for deposit or withdrawal events
        deposit_events = [e for e in events if e.event_type == TraderJoeV2EventType.DEPOSITED_TO_BINS]
        withdraw_events = [e for e in events if e.event_type == TraderJoeV2EventType.WITHDRAWN_FROM_BINS]

        if deposit_events:
            event = deposit_events[0]
            raw_data = event.data.get("raw_data", "")
            bin_ids = self._parse_bin_ids_from_data(raw_data) or [] if raw_data else []
            return ParsedLiquidityResult(
                success=True,
                is_add=True,
                pool_address=event.contract_address,
                bin_ids=bin_ids,
                gas_used=gas_used,
                block_number=block_number,
            )

        if withdraw_events:
            event = withdraw_events[0]
            raw_data = event.data.get("raw_data", "")
            bin_ids = self._parse_bin_ids_from_data(raw_data) or [] if raw_data else []
            return ParsedLiquidityResult(
                success=True,
                is_add=False,
                pool_address=event.contract_address,
                bin_ids=bin_ids,
                gas_used=gas_used,
                block_number=block_number,
            )

        return None

    @staticmethod
    def _lbpair_transfers(
        events: list[TraderJoeV2Event],
        pool_address: str,
        *,
        to_pool: bool,
    ) -> list[TraderJoeV2Event]:
        """Return ERC-20 ``Transfer`` events to/from the LBPair, in emission order.

        VIB-4634 / gemini HIGH on PR #2607. The receipt of a TraderJoe V2 LP
        op can contain Transfer events unrelated to the deposit/withdrawal
        (native-token wrap/unwrap, router hops, refunds). Blindly taking the
        first two Transfers can mis-attribute amounts. Filtering to transfers
        whose counterparty is the LBPair isolates the principal legs:

          * ``to_pool=True`` (LP_OPEN): wallet → LBPair deposits — match ``to``.
          * ``to_pool=False`` (LP_CLOSE): LBPair → wallet withdrawals — match ``from``.

        Emission order is preserved (NOT sorted by token address) so leg 0
        stays tokenX / amount0 and leg 1 stays tokenY / amount1, matching the
        bin-pair convention the accounting handler resolves. When the pool
        address is unknown the events are returned unfiltered so callers keep
        the legacy first-two-Transfers behaviour rather than dropping amounts.
        """
        transfers = [e for e in events if e.event_type == TraderJoeV2EventType.TRANSFER]
        pool = (pool_address or "").lower()
        if not pool:
            return transfers
        key = "to" if to_pool else "from"
        filtered = [e for e in transfers if str(e.data.get(key, "")).lower() == pool]
        # Fall back to the unfiltered list if filtering finds nothing — a
        # connector variant that routes principal through an intermediary
        # would otherwise silently drop the amounts (Empty ≠ Zero).
        return filtered or transfers

    def parse_swap_events(self, receipt: dict[str, Any]) -> list[SwapEventData]:
        """Parse swap events from a receipt.

        Convenient method to extract all swap-related data.

        Args:
            receipt: Web3 transaction receipt dict

        Returns:
            List of SwapEventData objects
        """
        result = self.parse_receipt(receipt)
        swaps = []

        if result.swap_result and result.swap_result.success:
            swaps.append(
                SwapEventData(
                    token_in=result.swap_result.token_in or "",
                    token_out=result.swap_result.token_out or "",
                    amount_in=result.swap_result.amount_in or 0,
                    amount_out=result.swap_result.amount_out or 0,
                    sender="",  # Not available from basic parsing
                    recipient="",
                )
            )

        return swaps

    # =============================================================================
    # Extraction Methods (for Result Enrichment)
    # =============================================================================

    def _resolve_token_decimals(self, token_address: str | None) -> int:
        """Resolve token decimals from address using the token resolver.

        Requires self._chain to be set (passed to __init__ via chain= kwarg).
        Returns 18 as fallback when chain is unknown or resolution fails.

        Args:
            token_address: ERC-20 token contract address

        Returns:
            Token decimals (e.g. 6 for USDC, 8 for WBTC, 18 for WETH/WAVAX)
        """
        if not token_address or not self._chain:
            if token_address and not self._chain:
                logger.warning(
                    "No chain configured for decimal resolution, defaulting to 18 for %s",
                    token_address,
                )
            return 18
        try:
            resolver = get_token_resolver()
            return resolver.get_decimals(self._chain, token_address)
        except Exception as e:  # noqa: BLE001
            logger.warning(
                "Token decimal resolution failed for %s on %s, falling back to 18: %s",
                token_address,
                self._chain,
                e,
            )
            return 18

    def extract_swap_amounts(
        self,
        receipt: dict[str, Any],
        *,
        expected_out: Decimal | None = None,
    ) -> "SwapAmounts | None":
        """Extract swap amounts from a transaction receipt.

        Uses actual token decimals when chain is known (passed to __init__).
        Falls back to 18 decimals when chain is unavailable.

        Args:
            receipt: Transaction receipt dict with 'logs' field
            expected_out: VIB-3203 Phase B — pre-slippage-discount quote in
                human (Decimal) units, sourced from
                ``ActionBundle.metadata["expected_output_human"]`` by the
                ResultEnricher. When provided and positive, realized
                ``slippage_bps`` is computed. When absent, ``slippage_bps``
                stays ``None``.

        Returns:
            SwapAmounts dataclass if swap event found, None otherwise
        """
        from almanak.framework.execution.extracted_data import SwapAmounts

        try:
            result = self.parse_receipt(receipt)
            if not result.swap_result or not result.swap_result.success:
                return None

            sr = result.swap_result
            amount_in = sr.amount_in or 0
            amount_out = sr.amount_out or 0

            # Resolve actual token decimals (avoids VIB-593 wrong amount_in_decimal for USDC)
            token_in_decimals = self._resolve_token_decimals(sr.token_in)
            token_out_decimals = self._resolve_token_decimals(sr.token_out)

            amount_in_decimal = Decimal(str(amount_in)) / Decimal(10**token_in_decimals)
            amount_out_decimal = Decimal(str(amount_out)) / Decimal(10**token_out_decimals)
            effective_price = amount_out_decimal / amount_in_decimal if amount_in_decimal > 0 else Decimal(0)

            # VIB-3203 Phase B: realized slippage when enricher supplies a quote.
            # Decimal resolution above silently falls back to 18 when the token
            # is unknown — that's safe for amount logging but corrupts slippage
            # math (a USDC fallback would scale amount_out_decimal by 1e12).
            # Suppress slippage_bps unless we can confirm the token_out decimals
            # via a strict resolver lookup. ``self._chain`` must be set, AND the
            # resolver must succeed — otherwise leave slippage_bps as None.
            # Amounts themselves still surface for legacy paths.
            slippage_bps: int | None = None
            if (
                expected_out is not None
                and expected_out > 0
                and amount_out_decimal > 0
                and self._chain
                and sr.token_out
            ):
                try:
                    get_token_resolver().get_decimals(self._chain, sr.token_out)
                    realized = (expected_out - amount_out_decimal) / expected_out
                    slippage_bps = int(realized * Decimal(10_000))
                except Exception as decimals_exc:  # noqa: BLE001 — strict gate, suppress slippage
                    logger.debug(
                        "TJ V2 slippage suppressed for %s: token_out decimals unconfirmed (%s)",
                        sr.token_out,
                        decimals_exc,
                    )

            return SwapAmounts(
                amount_in=amount_in,
                amount_out=amount_out,
                amount_in_decimal=amount_in_decimal,
                amount_out_decimal=amount_out_decimal,
                effective_price=effective_price,
                slippage_bps=slippage_bps,
                expected_out_decimal=expected_out,
                token_in=sr.token_in,
                token_out=sr.token_out,
            )

        except Exception as e:  # noqa: BLE001  # Defensive: graceful degradation for extraction
            logger.warning(f"Failed to extract swap amounts: {e}")
            return None

    def extract_bin_ids(self, receipt: dict[str, Any]) -> list[int] | None:
        """Extract bin IDs from LP transaction receipt.

        TraderJoe V2 uses bins for liquidity. This extracts the bin IDs
        from DepositedToBins or WithdrawnFromBins events.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            List of bin IDs if found, None otherwise
        """
        try:
            result = self.parse_receipt(receipt)

            # Look for deposit or withdrawal events
            for event in result.events:
                if event.event_type in (
                    TraderJoeV2EventType.DEPOSITED_TO_BINS,
                    TraderJoeV2EventType.WITHDRAWN_FROM_BINS,
                ):
                    # Try to parse bin IDs from raw_data
                    raw_data = event.data.get("raw_data", "")
                    if raw_data:
                        bin_ids = self._parse_bin_ids_from_data(raw_data)
                        if bin_ids:
                            return bin_ids

            return None

        except Exception as e:  # noqa: BLE001  # Defensive: graceful degradation for extraction
            logger.warning(f"Failed to extract bin_ids: {e}")
            return None

    def _parse_bin_ids_from_data(self, data: str) -> list[int] | None:
        """Parse bin IDs from DepositedToBins/WithdrawnFromBins event data.

        The data layout for these events is:
        - offset to ids array (32 bytes)
        - offset to amounts array (32 bytes)
        - ids array: length (32 bytes) + elements
        - amounts array: length (32 bytes) + elements
        """
        try:
            data_hex = HexDecoder.normalize_hex(data)
            if len(data_hex) < 128:  # At least 64 bytes (two 32-byte offsets)
                return None

            # Get offset to ids array (first 32 bytes of data)
            ids_offset = HexDecoder.decode_uint256(data_hex, 0)

            # Read ids array length at offset
            ids_length = HexDecoder.decode_uint256(data_hex, ids_offset)

            if ids_length == 0 or ids_length > 1000:  # Sanity check
                return None

            # Read each bin ID
            bin_ids = []
            for i in range(ids_length):
                bin_id = HexDecoder.decode_uint256(data_hex, ids_offset + 32 + (i * 32))
                bin_ids.append(bin_id)

            return bin_ids

        except Exception as e:  # noqa: BLE001  # Defensive: graceful degradation for extraction
            logger.warning(f"Failed to parse bin IDs: {e}")
            return None

    def extract_liquidity(self, receipt: dict[str, Any]) -> int | None:
        """Extract total liquidity from LP transaction receipt.

        Note: TraderJoe V2 uses ERC-1155 tokens for LP positions (not ERC-20).
        Liquidity amounts are encoded in DepositedToBins/WithdrawnFromBins events
        as bytes32 arrays requiring complex decoding. This method returns None
        as the exact liquidity amount extraction is not yet implemented.

        For LP operation detection, use parse_receipt().liquidity_result instead.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            None (liquidity amount extraction not implemented for TraderJoe V2)
        """
        try:
            result = self.parse_receipt(receipt)

            # TraderJoe V2 uses DepositedToBins/WithdrawnFromBins events, not Transfer
            # The amounts are encoded as bytes32 arrays - returning None until decoded
            for event in result.events:
                if event.event_type in (
                    TraderJoeV2EventType.DEPOSITED_TO_BINS,
                    TraderJoeV2EventType.WITHDRAWN_FROM_BINS,
                ):
                    # Event detected but amount decoding not implemented
                    logger.debug("TraderJoe V2 liquidity event detected, amount decoding not implemented")
                    return None

            return None

        except Exception as e:  # noqa: BLE001  # Defensive: graceful degradation for extraction
            logger.warning(f"Failed to extract liquidity: {e}")
            return None

    def extract_collected_fees(self, receipt: dict[str, Any]) -> ParsedFeeCollectionResult | None:
        """Extract collected fees data from a fee collection transaction receipt.

        Looks for ClaimedFees events and Transfer events to determine fee amounts.
        Returns None if ClaimedFees is not found (older LBPair versions without this event).

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            ParsedFeeCollectionResult if fee collection found, None otherwise
        """
        try:
            result = self.parse_receipt(receipt)
            gas_used = receipt.get("gasUsed", 0)
            block_number = receipt.get("blockNumber", 0)

            # Look for ClaimedFees events first (V2.1)
            claimed_events = [e for e in result.events if e.event_type == TraderJoeV2EventType.CLAIMED_FEES]
            if claimed_events:
                event = claimed_events[0]
                bin_ids = []
                raw_data = event.data.get("raw_data", "")
                if raw_data:
                    bin_ids = self._parse_bin_ids_from_data(raw_data) or []

                # Get fee amounts from Transfer events
                fees_x = 0
                fees_y = 0
                transfers = [e for e in result.events if e.event_type == TraderJoeV2EventType.TRANSFER]
                if len(transfers) >= 2:
                    fees_x = transfers[0].data.get("value", 0)
                    fees_y = transfers[1].data.get("value", 0)
                elif len(transfers) == 1:
                    fees_x = transfers[0].data.get("value", 0)

                return ParsedFeeCollectionResult(
                    success=True,
                    pool_address=event.contract_address,
                    bin_ids=bin_ids,
                    fees_x=fees_x,
                    fees_y=fees_y,
                    gas_used=gas_used,
                    block_number=block_number,
                )

            return None

        except Exception as e:  # noqa: BLE001  # Defensive: graceful degradation for extraction
            logger.warning(f"Failed to extract collected fees: {e}")
            return None

    def extract_protocol_fees(self, receipt: dict[str, Any]) -> "ProtocolFees":
        """VIB-3495: TraderJoe V2 LP protocol fee coverage audit.

        TraderJoe V2 (Liquidity Book) charges a swap fee on trades that
        traverse active bins; this fee accrues to LPs and is claimable via
        ``collectFees()`` / ``ClaimedFees`` events. The ``ClaimedFees`` event
        carries bin-level amounts as packed ``bytes32[]`` and ERC-20 Transfer
        events carry the actual token amounts — but there is no on-chain
        protocol-level fee (the fee IS the LP reward). Since there is no
        on-chain USD price for the fee amounts available at the receipt layer,
        the USD amount is unavailable.

        Returns a ProtocolFees with unavailable_reason so downstream
        attribution records "known-unknown" rather than silently omitting
        the field (which would set protocol_fees_usd="" and produce
        fee_pnl=None in PnL attribution — the correct outcome, but now
        explicitly flagged by the parser rather than by parser absence).
        """
        from almanak.framework.execution.extracted_data import ProtocolFees

        # VIB-3495: TJ V2 fee amounts are available as raw token units in
        # ClaimedFees + Transfer events, but USD conversion requires a price
        # oracle that is not available at the receipt-parser layer. The fee
        # exists (TJ V2 always charges a non-zero LP fee on swaps) but we
        # cannot report total_usd without a price source.
        return ProtocolFees(
            total_usd=None,
            unavailable_reason="protocol_fee_not_emitted_in_receipt",
        )

    def extract_fees0(self, receipt: dict[str, Any]) -> int | None:
        """Extract fee amount for token X from fee collection receipt.

        Args:
            receipt: Transaction receipt dict

        Returns:
            Fee amount in wei for token X, or None
        """
        result = self.extract_collected_fees(receipt)
        if result and result.success:
            return result.fees_x
        return None

    def extract_fees1(self, receipt: dict[str, Any]) -> int | None:
        """Extract fee amount for token Y from fee collection receipt.

        Args:
            receipt: Transaction receipt dict

        Returns:
            Fee amount in wei for token Y, or None
        """
        result = self.extract_collected_fees(receipt)
        if result and result.success:
            return result.fees_y
        return None

    def extract_lp_open_data(self, receipt: dict[str, Any]) -> "LPOpenData | None":
        """Extract LP open data from a TraderJoe V2 (Liquidity Book) receipt.

        VIB-4634 — chain-data-first pool-address stamping. The Liquidity Book
        ``DepositedToBins(address sender, address to, uint256[] ids,
        bytes32[] amounts)`` event is emitted BY the LBPair contract itself,
        so ``event.contract_address`` IS the canonical LBPair (pool) address
        — no factory lookup or extra RPC needed. Stamping it on
        ``LPOpenData.pool_address`` lets the LP accounting handler's resolver
        accept-branch (``^0x[0-9a-f]{40}$``) book the LP_OPEN event, instead
        of dropping it because the position-key tail
        (``tokenX/tokenY/<binStep>``) is rejected by
        ``_clean_pool_address_candidate`` as a Uniswap-V3 fee-tier descriptor.
        Mirrors the Uniswap V3 Mint/IncreaseLiquidity ``pool_address`` path
        (VIB-3893).

        Bin-model directional null-contract (Empty ≠ Zero ≠ None, blueprint
        27 §10.10): the Liquidity Book has NO NFT token id and NO tick
        bracket, so ``position_id`` is ``0`` (the fungible-LP "no
        discriminator" sentinel the accounting handler maps to ``None``) and
        ``tick_lower`` / ``tick_upper`` / ``liquidity`` / ``current_tick``
        stay ``None`` — never fabricated. ``amount0`` / ``amount1`` are the
        raw token-X / token-Y deposit amounts decoded from the ERC-20
        ``Transfer`` legs (wallet → LBPair); the handler scales them by token
        decimals. Returns ``None`` when no ``DepositedToBins`` event is in the
        receipt (e.g. a failed mid-bundle open).

        Args:
            receipt: Transaction receipt dict with 'logs' field.

        Returns:
            ``LPOpenData`` if a ``DepositedToBins`` event is present,
            ``None`` otherwise.
        """
        from almanak.framework.execution.extracted_data import LPOpenData

        try:
            result = self.parse_receipt(receipt)

            if not result.liquidity_result or not result.liquidity_result.is_add:
                return None

            # The DepositedToBins emitter is the LBPair — chain-truth pool addr.
            # Lowercase it: a real RPC returns a checksummed (mixed-case)
            # log address, but the LP handler's _clean_pool_address_candidate
            # only accepts lowercase 0x-hex (matches the Uniswap V3 path).
            pool_address = (result.liquidity_result.pool_address or "").lower()

            # Token-X / token-Y deposit amounts come from the ERC-20 Transfer
            # legs into the LBPair (wallet → LBPair). Filter to transfers whose
            # destination IS the LBPair so unrelated legs (native-token wrap,
            # router hops, refunds) can't corrupt the amounts (gemini HIGH on
            # PR #2607). Emission order is preserved — the LBRouter transfers
            # tokenX then tokenY, matching the bin-pair (amount0=X, amount1=Y)
            # convention the accounting handler resolves; do NOT sort by
            # address (canonical lower-address-first ordering would swap the
            # legs whenever tokenX's address > tokenY's, mis-scaling amounts).
            deposit_transfers = self._lbpair_transfers(result.events, pool_address, to_pool=True)
            amount_x = 0
            amount_y = 0
            if len(deposit_transfers) >= 1:
                amount_x = deposit_transfers[0].data.get("value", 0)
            if len(deposit_transfers) >= 2:
                amount_y = deposit_transfers[1].data.get("value", 0)

            return LPOpenData(
                # Liquidity Book is fungible (ERC-1155), not an NFT — there is
                # no per-position token id. ``0`` is the documented
                # "no discriminator" sentinel (LPOpenData.position_id is typed
                # ``int``); the accounting handler's
                # ``_resolve_lp_open_discriminator`` maps ``0`` → ``None``.
                position_id=0,
                amount0=amount_x,
                amount1=amount_y,
                # Bin model has no tick bracket — leave None, do NOT fabricate.
                tick_lower=None,
                tick_upper=None,
                liquidity=None,
                current_tick=None,
                pool_address=pool_address,
            )

        except Exception as e:  # noqa: BLE001  # Defensive: graceful degradation for extraction
            logger.warning(f"Failed to extract lp_open_data: {e}")
            return None

    def extract_lp_close_data(self, receipt: dict[str, Any]) -> "LPCloseData | None":
        """Extract LP close data from transaction receipt.

        Looks for WithdrawnFromBins events and Transfer events.

        VIB-4634 — stamp the canonical LBPair (pool) address on the close
        leg. The ``WithdrawnFromBins`` event is emitted BY the LBPair, so
        ``event.contract_address`` IS the pool address (chain-truth, no
        factory lookup). Mirrors the Uniswap V3 Burn ``pool_address`` path
        (VIB-3940). Without it the LP accounting handler drops every
        TraderJoe V2 LP_CLOSE / LP_COLLECT_FEES because the position-key tail
        ``tokenX/tokenY/<binStep>`` is rejected as a V3 fee-tier descriptor.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            LPCloseData dataclass if withdrawal found, None otherwise
        """
        from almanak.framework.execution.extracted_data import LPCloseData

        try:
            result = self.parse_receipt(receipt)

            if not result.liquidity_result or result.liquidity_result.is_add:
                # VIB-4634 — LP_COLLECT_FEES path. A fee harvest emits
                # ``ClaimedFees`` (no WithdrawnFromBins), so the principal
                # withdrawal branch above does not fire. The Liquidity Book
                # keeps principal on-chain on a collect, so the collected
                # principal is a measured zero; the fee amounts live on the
                # separate ``fees0`` / ``fees1`` extraction. We still emit an
                # ``LPCloseData`` here solely to carry the canonical LBPair
                # ``pool_address`` (the ``ClaimedFees`` emitter) so the LP
                # accounting handler can book the LP_COLLECT_FEES event
                # instead of dropping it (the ``tokenX/tokenY/<binStep>``
                # position-key descriptor is rejected as a V3 fee tier).
                fees = self.extract_collected_fees(receipt)
                if fees is not None and fees.success and fees.pool_address:
                    return LPCloseData(
                        # Principal stays on-chain on a collect — measured zero,
                        # NOT unmeasured. Fees ship via fees0/fees1 separately.
                        amount0_collected=0,
                        amount1_collected=0,
                        # VIB-4470 — TraderJoe doesn't separate fees in the
                        # withdrawal events; fees are unmeasured here (Empty ≠
                        # Zero). The dedicated extract_fees0/1 path carries them.
                        fees0=None,
                        fees1=None,
                        liquidity_removed=None,
                        # Lowercase — a real RPC log address is checksummed and
                        # the LP handler only accepts lowercase 0x-hex.
                        pool_address=fees.pool_address.lower(),  # VIB-4634 — LBPair
                    )
                return None

            # The WithdrawnFromBins emitter is the LBPair — chain-truth pool addr.
            # Lowercase it (real RPC log addresses are checksummed; the LP
            # handler only accepts lowercase 0x-hex).
            pool_address = (result.liquidity_result.pool_address or "").lower()

            # Get amounts from the LBPair → wallet withdrawal Transfer legs.
            # Filter to transfers FROM the LBPair so unrelated legs (unwrap,
            # router hops) can't corrupt the amounts (gemini HIGH on PR #2607).
            # Emission order preserved (tokenX then tokenY); not sorted by
            # address — see _lbpair_transfers / extract_lp_open_data.
            amount_x = 0
            amount_y = 0
            withdraw_transfers = self._lbpair_transfers(result.events, pool_address, to_pool=False)
            if len(withdraw_transfers) >= 1:
                amount_x = withdraw_transfers[0].data.get("value", 0)
            if len(withdraw_transfers) >= 2:
                amount_y = withdraw_transfers[1].data.get("value", 0)

            if amount_x > 0 or amount_y > 0:
                return LPCloseData(
                    amount0_collected=amount_x,
                    amount1_collected=amount_y,
                    # VIB-4470 — TraderJoe doesn't separate fees in events;
                    # fees are unmeasured (Empty ≠ Zero) rather than zero.
                    fees0=None,
                    fees1=None,
                    liquidity_removed=None,
                    pool_address=pool_address,  # VIB-4634 — LBPair address
                )

            return None

        except Exception as e:  # noqa: BLE001  # Defensive: graceful degradation for extraction
            logger.warning(f"Failed to extract lp_close_data: {e}")
            return None

    def _build_close_output_leg(self, token_address: str | None, raw_amount: Any) -> "PrimitiveMoneyLeg":
        """Build one OUTPUT money leg for an LP_CLOSE proceeds token (VIB-5221).

        Token identity is the chain-truth ERC-20 symbol resolved FROM the on-chain
        transfer's emitting contract address — self-describing, so it never
        depends on the intent or the ``position_id`` (the fungible-LP synthetic-id
        problem #2894 had to patch around). An unresolved identity is ``""``,
        never a fabricated symbol.

        Amount is a human-unit ``MeasuredMoney`` (the VIB-5036 ledger contract)
        carrying Empty ≠ Zero (blueprint 27 §10.10) by construction:

        * a measured raw ``0`` → measured zero (principal kept on-chain);
        * a non-zero raw whose token decimals cannot be strictly resolved →
          UNMEASURED (never a wrongly-scaled value — mirrors the ledger's
          ``_lp_amount_to_human`` discipline, NOT the 18-decimal fallback
          ``_resolve_token_decimals`` uses for best-effort logging);
        * a non-integer / missing raw → UNMEASURED.
        """
        from almanak.connectors._strategy_base.primitive_money_leg import PrimitiveMoneyLeg
        from almanak.framework.accounting.measured import MeasuredMoney

        symbol = ""
        decimals: int | None = None
        if token_address and self._chain:
            try:
                resolver = get_token_resolver()
                # skip_gateway/log_errors: accounting write hot path — resolve the
                # symbol AND decimals from the static registry without risking a
                # gateway round-trip stall (mirrors ledger ``_lp_amount_to_human``).
                info = resolver.resolve(token_address, self._chain, log_errors=False, skip_gateway=True)
                symbol = getattr(info, "symbol", "") or ""
                decimals = getattr(info, "decimals", None)
            except Exception as exc:  # noqa: BLE001 — fail to unmeasured, never raise on the accounting path
                logger.debug(
                    "TJ V2 close leg: token resolve failed for %s on %s: %s",
                    token_address,
                    self._chain,
                    exc,
                )

        try:
            raw_int: int | None = int(raw_amount)
        except (TypeError, ValueError):
            raw_int = None

        if raw_int is None:
            amount = MeasuredMoney.unmeasured()
        elif raw_int == 0:
            amount = MeasuredMoney.measured(Decimal(0))
        elif isinstance(decimals, int) and decimals >= 0:
            amount = MeasuredMoney.measured(Decimal(raw_int) / Decimal(10**decimals))
        else:
            amount = MeasuredMoney.unmeasured()
        return PrimitiveMoneyLeg.output(symbol, amount)

    def _build_open_input_leg(self, token_address: str | None, raw_amount: Any) -> "PrimitiveMoneyLeg":
        """Build one INPUT money leg for an LP_OPEN deposited token (VIB-5414).

        The exact INPUT mirror of :meth:`_build_close_output_leg`: token identity
        is the chain-truth ERC-20 symbol resolved FROM the on-chain deposit
        transfer's emitting contract address (wallet → LBPair), so it is
        independent of the intent and of the fungible-LP synthetic ``position_id``.
        An unresolved identity is ``""``, never a fabricated symbol.

        Amount is a human-unit ``MeasuredMoney`` (the VIB-5036 ledger contract)
        carrying Empty ≠ Zero (blueprint 27 §10.10) by construction — identical
        discipline to the close leg:

        * a measured raw ``0`` → measured zero;
        * a non-zero raw whose token decimals cannot be strictly resolved →
          UNMEASURED (never a wrongly-scaled value — NOT the 18-decimal best-effort
          fallback ``_resolve_token_decimals`` uses for logging);
        * a non-integer / missing raw → UNMEASURED.

        Stamping the two INPUT legs lets the LP accounting handler compute a
        MEASURED ``cost_basis_usd`` from the deposited token0/token1 notional, so
        the position keeps a HIGH-confidence snapshot instead of degrading to
        ESTIMATED on a ``cost_basis_usd`` of ``0`` (VIB-5414 — the TraderJoe-LB
        case of the Uniswap-V3 ``deployed_capital=0`` family VIB-3883/3894).
        """
        from almanak.connectors._strategy_base.primitive_money_leg import PrimitiveMoneyLeg
        from almanak.framework.accounting.measured import MeasuredMoney

        symbol = ""
        decimals: int | None = None
        if token_address and self._chain:
            try:
                resolver = get_token_resolver()
                # skip_gateway/log_errors: accounting write hot path — resolve the
                # symbol AND decimals from the static registry without risking a
                # gateway round-trip stall (mirrors ledger ``_lp_amount_to_human``).
                info = resolver.resolve(token_address, self._chain, log_errors=False, skip_gateway=True)
                symbol = getattr(info, "symbol", "") or ""
                decimals = getattr(info, "decimals", None)
            except Exception as exc:  # noqa: BLE001 — fail to unmeasured, never raise on the accounting path
                logger.debug(
                    "TJ V2 open leg: token resolve failed for %s on %s: %s",
                    token_address,
                    self._chain,
                    exc,
                )

        try:
            raw_int: int | None = int(raw_amount)
        except (TypeError, ValueError):
            raw_int = None

        if raw_int is None:
            amount = MeasuredMoney.unmeasured()
        elif raw_int == 0:
            amount = MeasuredMoney.measured(Decimal(0))
        elif isinstance(decimals, int) and decimals >= 0:
            amount = MeasuredMoney.measured(Decimal(raw_int) / Decimal(10**decimals))
        else:
            amount = MeasuredMoney.unmeasured()
        return PrimitiveMoneyLeg.input(symbol, amount)

    def extract_primitive_money_legs(self, receipt: dict[str, Any]) -> "PrimitiveMoneyLegs | None":
        """VIB-5221 (US-011) / VIB-5414 — declare the LP_OPEN **and** LP_CLOSE money
        legs as a typed ``PrimitiveMoneyLegs`` the ledger dispatcher consumes directly.

        Inverts the legacy control flow (blueprint 27 §6.6, 05 §7): instead of the
        ledger reverse-engineering the legs from the intent's pool descriptor (the
        #2894 / VIB-5195 ``_pair_tokens_from_intent`` threading) or from
        ``LPOpenData.amount0`` / ``amount1`` positioned over the pool's first two
        coins, the connector DECLARES the two tokens it actually moved on-chain.
        Legs are built FROM the wallet ↔ LBPair ``DepositedToBins`` /
        ``WithdrawnFromBins`` Transfer legs (chain truth), so they are independent
        of the intent AND of the ``position_id`` — a fungible-LP event under a
        synthetic id (``traderjoe_*_lp_0``) declares the same legs as an NFT event,
        which is exactly the case #2894 had to special-case.

        * **LP_OPEN** (``is_add``) — two INPUT legs from the wallet → LBPair
          ``DepositedToBins`` deposit Transfers (the deposited token0/token1
          notional). Without these the LP handler had no measured amounts to price,
          so ``compute_lp_cost_basis`` returned ``0`` and the position degraded the
          snapshot HIGH→ESTIMATED for the whole hold (VIB-5414, same class as the
          Uniswap-V3 ``deployed_capital=0`` family VIB-3883/3894).
        * **LP_CLOSE** — two OUTPUT legs from the LBPair → wallet
          ``WithdrawnFromBins`` withdrawal Transfers (the proceeds).

        Legs are in tokenX-then-tokenY (``amount0`` / ``amount1``) emission order —
        the dispatcher (``_extract_from_declared_legs``) projects leg0 →
        ``token_in`` / ``amount_in`` and leg1 → ``token_out`` / ``amount_out``,
        lane-symmetric with the legacy ``_extract_from_lp_open`` /
        ``_extract_from_lp_close``. Amounts are human-unit ``MeasuredMoney`` carrying
        Empty ≠ Zero (§10.10); see ``_build_open_input_leg`` /
        ``_build_close_output_leg``.

        Returns ``None`` (→ benign ``ExtractMissing``) when the receipt is neither a
        real deposit nor a principal-withdrawal close (a fees-only ``ClaimedFees``
        collect, or a degenerate zero-amount event), so the dispatcher falls through
        to the legacy LP_OPEN / LP_CLOSE path unchanged. Never raises: any failure
        degrades to ``None`` rather than halting the live accounting writer.
        """
        from almanak.connectors._strategy_base.primitive_money_leg import PrimitiveMoneyLegs

        try:
            result = self.parse_receipt(receipt)
            # A missing ``liquidity_result`` (a fees-only ClaimedFees collect —
            # principal stays on-chain) carries no money legs.
            if not result.liquidity_result:
                return None
            # The DepositedToBins / WithdrawnFromBins emitter IS the LBPair —
            # chain-truth pool addr (lowercased to match the LP handler's resolver).
            pool_address = (result.liquidity_result.pool_address or "").lower()

            # Only declare legs for a REAL on-chain move (≥1 positive Transfer). A
            # zero-amount event returns None so the byte-for-byte legacy fallback
            # (→ intent fallback, amount="") is preserved for that degenerate case.
            def _positive(transfer: Any) -> bool:
                try:
                    return int(transfer.data.get("value", 0)) > 0
                except (TypeError, ValueError):
                    return False

            if result.liquidity_result.is_add:
                # LP_OPEN — INPUT legs from the wallet → LBPair deposit Transfers.
                deposit_transfers = self._lbpair_transfers(result.events, pool_address, to_pool=True)
                if not any(_positive(t) for t in deposit_transfers):
                    return None
                legs = [
                    self._build_open_input_leg(t.contract_address, t.data.get("value", 0))
                    for t in deposit_transfers[:2]
                ]
                return PrimitiveMoneyLegs.of(*legs)

            # LP_CLOSE — OUTPUT legs from the LBPair → wallet withdrawal Transfers.
            withdraw_transfers = self._lbpair_transfers(result.events, pool_address, to_pool=False)
            if not any(_positive(t) for t in withdraw_transfers):
                return None
            legs = [
                self._build_close_output_leg(t.contract_address, t.data.get("value", 0)) for t in withdraw_transfers[:2]
            ]
            return PrimitiveMoneyLegs.of(*legs)
        except Exception as exc:  # noqa: BLE001 — never halt the accounting writer
            logger.warning(f"Failed to extract primitive_money_legs: {exc}")
            return None


__all__ = [
    "TraderJoeV2ReceiptParser",
    "TraderJoeV2EventType",
    "TraderJoeV2Event",
    "SwapEventData",
    "LiquidityEventData",
    "TransferEventData",
    "ParsedSwapResult",
    "ParsedLiquidityResult",
    "ParsedFeeCollectionResult",
    "ParseResult",
    "EVENT_TOPICS",
    "TOPIC_TO_EVENT",
    "EVENT_NAME_TO_TYPE",
    "DEPOSITED_TO_BINS_TOPIC",
    "WITHDRAWN_FROM_BINS_TOPIC",
]
