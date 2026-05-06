"""PancakeSwap V3 Receipt Parser (Refactored).

Refactored to use base infrastructure while maintaining backward compatibility.
PancakeSwap V3 is a Uniswap V3 fork with identical event signatures.
"""

import logging
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import TYPE_CHECKING, Any

from almanak.framework.connectors.base import BaseReceiptParser, EventRegistry, HexDecoder

if TYPE_CHECKING:
    from almanak.framework.execution.extracted_data import LPCloseData, ProtocolFees, SwapAmounts

logger = logging.getLogger(__name__)


# =============================================================================
# Event Topic Signatures (same as Uniswap V3)
# =============================================================================

# PancakeSwap V3 uses a DIFFERENT Swap event signature than Uniswap V3!
# UniswapV3: Swap(address,address,int256,int256,uint160,uint128,int24) - 7 params
# PancakeSwapV3: Swap(address,address,int256,int256,uint160,uint128,int24,uint128,uint128) - 9 params
# The extra 2 uint128 params are protocolFeesToken0 and protocolFeesToken1
EVENT_TOPICS: dict[str, str] = {
    "Swap": "0x19b47279256b2a23a1665c810c8d55a1758940ee09377d4f8d26497a3577dc83",  # PancakeSwap V3 (9 params)
    "Mint": "0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde",
    "Burn": "0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c",
    "Collect": "0x70935338e69775456a85ddef226c395fb668b63fa0115f5f20610b388e6ca9c0",
    "Transfer": "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
}

TOPIC_TO_EVENT: dict[str, str] = {v: k for k, v in EVENT_TOPICS.items()}


# =============================================================================
# Enums
# =============================================================================


class PancakeSwapV3EventType(Enum):
    """PancakeSwap V3 event types."""

    SWAP = "SWAP"
    MINT = "MINT"
    BURN = "BURN"
    COLLECT = "COLLECT"
    TRANSFER = "TRANSFER"
    UNKNOWN = "UNKNOWN"


EVENT_NAME_TO_TYPE: dict[str, PancakeSwapV3EventType] = {
    "Swap": PancakeSwapV3EventType.SWAP,
    "Mint": PancakeSwapV3EventType.MINT,
    "Burn": PancakeSwapV3EventType.BURN,
    "Collect": PancakeSwapV3EventType.COLLECT,
    "Transfer": PancakeSwapV3EventType.TRANSFER,
}

# PancakeSwap V3 NonfungiblePositionManager addresses
POSITION_MANAGER_ADDRESSES: dict[str, str] = {
    "bsc": "0x46A15B0b27311cedF172AB29E4f4766fbE7F4364",
    "bnb": "0x46A15B0b27311cedF172AB29E4f4766fbE7F4364",
    "ethereum": "0x46A15B0b27311cedF172AB29E4f4766fbE7F4364",
    "arbitrum": "0x46A15B0b27311cedF172AB29E4f4766fbE7F4364",
    "base": "0x46A15B0b27311cedF172AB29E4f4766fbE7F4364",
}

# Zero address for detecting mints
ZERO_ADDRESS_PADDED = "0x" + "0" * 64


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class SwapEventData:
    """Parsed data from PancakeSwap V3 Swap event.

    Note: PancakeSwap V3 Swap event has 9 parameters (vs 7 for Uniswap V3):
    - sender, recipient (indexed)
    - amount0, amount1, sqrtPriceX96, liquidity, tick (same as UniV3)
    - protocolFeesToken0, protocolFeesToken1 (PancakeSwap V3 specific)
    """

    pool: str
    sender: str
    recipient: str
    amount0: Decimal
    amount1: Decimal
    sqrt_price_x96: int = 0
    liquidity: int = 0
    tick: int = 0
    protocol_fees_token0: int = 0  # PancakeSwap V3 specific
    protocol_fees_token1: int = 0  # PancakeSwap V3 specific

    @property
    def token0_in(self) -> bool:
        """Check if token0 is the input token."""
        return self.amount0 > 0

    @property
    def token1_in(self) -> bool:
        """Check if token1 is the input token."""
        return self.amount1 > 0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "pool": self.pool,
            "sender": self.sender,
            "recipient": self.recipient,
            "amount0": str(self.amount0),
            "amount1": str(self.amount1),
            "sqrt_price_x96": str(self.sqrt_price_x96),
            "liquidity": str(self.liquidity),
            "tick": self.tick,
            "token0_in": self.token0_in,
            "token1_in": self.token1_in,
            "protocol_fees_token0": str(self.protocol_fees_token0),
            "protocol_fees_token1": str(self.protocol_fees_token1),
        }


@dataclass
class ParseResult:
    """Result of parsing a receipt."""

    success: bool
    swaps: list[SwapEventData] = field(default_factory=list)
    error: str | None = None
    transaction_hash: str = ""
    block_number: int = 0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "swaps": [s.to_dict() for s in self.swaps],
            "error": self.error,
            "transaction_hash": self.transaction_hash,
            "block_number": self.block_number,
        }


# =============================================================================
# Internal swap-amount extraction state (Phase 8.4 refactor)
#
# These dataclasses are NOT part of the public API — they bundle state
# between the per-phase helpers of ``extract_swap_amounts`` so each helper
# stays small, pure, and individually testable. Mirrors the Phase 7.3
# Aerodrome ``_SwapSeed`` and the Phase 6 UniV4 pattern.
# =============================================================================


@dataclass
class _WalletTransfers:
    """ERC-20 Transfer events partitioned by wallet involvement.

    Each tuple is (token_address_lowercase, raw_amount_uint256).
    """

    from_wallet: list[tuple[str, int]]
    to_wallet: list[tuple[str, int]]


@dataclass
class _SwapSeed:
    """Raw (pre-decimal-scaling) swap amounts picked from wallet transfers."""

    token_in: str
    token_out: str
    amount_in: int
    amount_out: int


@dataclass
class _SwapDecimals:
    """Resolved (maybe-partial) token decimals for a swap.

    ``decimals_in`` is ``None`` when the input token could not be resolved —
    preserves the legacy fail-open behavior on the input side (output is
    fail-closed at the caller).
    """

    decimals_in: int | None
    decimals_out: int


# =============================================================================
# Receipt Parser
# =============================================================================


class PancakeSwapV3ReceiptParser(BaseReceiptParser[SwapEventData, ParseResult]):
    """Parser for PancakeSwap V3 transaction receipts.

    Uses base infrastructure for common parsing logic while handling
    PancakeSwap V3-specific event decoding.
    """

    SUPPORTED_EXTRACTIONS: frozenset[str] = frozenset(
        {
            "position_id",
            "swap_amounts",
            "tick_lower",
            "tick_upper",
            "liquidity",
            "lp_close_data",
            "protocol_fees",  # VIB-3204 — extract_protocol_fees implemented below
        }
    )

    def __init__(self, chain: str = "bsc", **kwargs: Any) -> None:
        """Initialize the parser.

        Args:
            chain: Blockchain network (for position manager address lookup)
        """
        registry = EventRegistry(EVENT_TOPICS, EVENT_NAME_TO_TYPE)
        super().__init__(registry=registry)
        self.chain = chain.lower()

    def _decode_log_data(
        self,
        event_name: str,
        topics: list[Any],
        data: str,
        contract_address: str,
    ) -> dict[str, Any]:
        """Decode Swap event data.

        PancakeSwap V3 Swap event structure (9 params, different from UniV3's 7):
        - topic0: event signature
        - topic1: sender (indexed)
        - topic2: recipient (indexed)
        - data: amount0 (int256), amount1 (int256), sqrtPriceX96 (uint160),
                liquidity (uint128), tick (int24),
                protocolFeesToken0 (uint128), protocolFeesToken1 (uint128)
        """
        if event_name != "Swap":
            return {}

        # Parse indexed topics
        sender = HexDecoder.topic_to_address(topics[1]) if len(topics) > 1 else ""
        recipient = HexDecoder.topic_to_address(topics[2]) if len(topics) > 2 else ""

        # Parse non-indexed data (each value is 32 bytes = 64 hex chars)
        # PancakeSwap V3 has 7 data fields vs UniV3's 5
        amount0 = HexDecoder.decode_int256(data, 0)
        amount1 = HexDecoder.decode_int256(data, 32)
        sqrt_price_x96 = HexDecoder.decode_uint160(data, 64)
        liquidity = HexDecoder.decode_uint128(data, 96)
        tick = HexDecoder.decode_int24(data, 128)
        # PancakeSwap V3 specific: protocol fees (offset 160 and 192)
        protocol_fees_token0 = HexDecoder.decode_uint128(data, 160)
        protocol_fees_token1 = HexDecoder.decode_uint128(data, 192)

        return {
            "pool_address": contract_address.lower(),
            "sender": sender,
            "recipient": recipient,
            "amount0": amount0,
            "amount1": amount1,
            "sqrt_price_x96": sqrt_price_x96,
            "liquidity": liquidity,
            "tick": tick,
            "protocol_fees_token0": protocol_fees_token0,
            "protocol_fees_token1": protocol_fees_token1,
        }

    def _create_event(
        self,
        event_name: str,
        log_index: int,
        tx_hash: str,
        block_number: int,
        contract_address: str,
        decoded_data: dict[str, Any],
        raw_topics: list[str],
        raw_data: str,
    ) -> SwapEventData | None:
        """Create SwapEventData from decoded data.

        Only creates events for Swap - returns None for other event types
        (Transfer, Mint, Burn, Collect) which are tracked but not returned.
        """
        _ = (log_index, tx_hash, block_number, raw_topics, raw_data)

        # Only create SwapEventData for actual Swap events
        if event_name != "Swap" or not decoded_data:
            return None

        return SwapEventData(
            pool=decoded_data.get("pool_address", contract_address),
            sender=decoded_data.get("sender", ""),
            recipient=decoded_data.get("recipient", ""),
            amount0=Decimal(decoded_data.get("amount0", 0)),
            amount1=Decimal(decoded_data.get("amount1", 0)),
            sqrt_price_x96=decoded_data.get("sqrt_price_x96", 0),
            liquidity=decoded_data.get("liquidity", 0),
            tick=decoded_data.get("tick", 0),
            protocol_fees_token0=decoded_data.get("protocol_fees_token0", 0),
            protocol_fees_token1=decoded_data.get("protocol_fees_token1", 0),
        )

    def _build_result(
        self,
        events: list[SwapEventData],
        receipt: dict[str, Any],
        tx_hash: str,
        block_number: int,
        tx_success: bool,
        **kwargs,
    ) -> ParseResult:
        """Build ParseResult from parsed events."""
        _ = receipt
        error = kwargs.get("error")

        if not tx_success or error:
            return ParseResult(
                success=False,
                error=error or "Transaction failed",
                transaction_hash=tx_hash,
                block_number=block_number,
            )

        logger.info(f"Parsed PancakeSwap V3 receipt: tx={tx_hash[:10]}..., swaps={len(events)}")

        return ParseResult(
            success=True,
            swaps=events,
            transaction_hash=tx_hash,
            block_number=block_number,
        )

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
        actual decimals via the token resolver for accurate decimal conversion.

        Args:
            receipt: Transaction receipt dict with 'logs' and 'from' fields
            expected_out: VIB-3203 Phase B — pre-slippage-discount quote in
                human (Decimal) units, sourced from
                ``ActionBundle.metadata["expected_output_human"]`` by the
                ResultEnricher. When provided and positive, realized
                ``slippage_bps`` is computed. When absent, ``slippage_bps``
                stays ``None``.

        Returns:
            SwapAmounts dataclass if swap event found, None otherwise

        Implementation note:
            Phase 8.4 — this is a thin orchestrator over per-phase helpers.
            Each helper is individually testable and keeps CC bounded.
            Preserves:
            (1) wallet-level "first transfer out / last transfer in" semantics
                for multi-hop disambiguation,
            (2) SwapAmounts field surface,
            (3) amount0/amount1 sign conventions (the parser is wallet-transfer
                based and does not inspect Swap-event signs here — that path
                runs on parse_receipt).
        """
        try:
            if not self._swap_status_ok(receipt):
                return None

            # CodeRabbit review (PR #1798): guard against transfer-only
            # receipts that happen to contain one wallet-out and one wallet-in
            # ERC-20 Transfer but NO PancakeSwap V3 Swap event. Without this
            # check, a plain ERC-20 transfer or an LP-add receipt (which can
            # legitimately produce wallet Transfer logs) would be misclassified
            # as a swap and pollute downstream PnL. Enforces the method-contract
            # invariant "returns None if no swap event found".
            if not self._has_pcs_swap_log(receipt):
                return None

            wallet = self._swap_wallet(receipt)
            if not wallet:
                return None

            transfers = self._collect_wallet_transfers(receipt, wallet)
            seed = self._pick_swap_raw_amounts(transfers)
            if seed is None:
                return None

            decimals = self._resolve_swap_decimals(seed)
            if decimals is None:
                return None

            return self._build_swap_amounts(seed, decimals, expected_out)

        except Exception as e:
            logger.warning(f"Failed to extract swap amounts: {e}")
            return None

    # ------------------------------------------------------------------
    # extract_swap_amounts — per-phase helpers (Phase 8.4 refactor).
    # Each is small, single-purpose, and independently testable.
    # ------------------------------------------------------------------

    @staticmethod
    def _swap_status_ok(receipt: dict[str, Any]) -> bool:
        """Return True iff ``receipt['status']`` is success (1).

        Accepts int, decimal string, and hex string ('0x0' / '0x1').
        """
        status = receipt.get("status", 0)
        if isinstance(status, str):
            status = int(status, 16) if status.startswith("0x") else int(status)
        return status == 1

    def _has_pcs_swap_log(self, receipt: dict[str, Any]) -> bool:
        """Return True iff the receipt contains at least one PCS V3 Swap log.

        Cheap topic-only scan — does NOT decode the log data. Gatekeeps the
        ``extract_swap_amounts`` pipeline against transfer-only receipts
        (e.g. a plain ERC-20 Transfer, or an LP add that still has wallet
        Transfer logs) so they cannot be misclassified as swaps.
        """
        swap_topic = self._normalize_topic(EVENT_TOPICS["Swap"])
        for log in receipt.get("logs", []) or []:
            topics = log.get("topics") or []
            if not topics:
                continue
            if self._normalize_topic(topics[0]) == swap_topic:
                return True
        return False

    def _swap_wallet(self, receipt: dict[str, Any]) -> str:
        """Return the normalized wallet address from ``receipt['from']`` or ''."""
        raw_wallet = receipt.get("from") or ""
        return self._normalize_topic(raw_wallet) if raw_wallet else ""

    def _collect_wallet_transfers(
        self,
        receipt: dict[str, Any],
        wallet: str,
    ) -> "_WalletTransfers":
        """Scan logs once; split ERC-20 Transfers into wallet-out and wallet-in.

        Per-log parsing is delegated to ``_parse_transfer_log`` so this loop
        stays a thin classifier. Malformed Transfer data (non-hex, truncated)
        is silently skipped by the parser — better to produce a partial-but-
        sane result than crash on a stray log from an unrelated contract.
        """
        transfer_topic = self._normalize_topic(EVENT_TOPICS["Transfer"])
        transfers_from_wallet: list[tuple[str, int]] = []
        transfers_to_wallet: list[tuple[str, int]] = []

        for log in receipt.get("logs", []):
            parsed = self._parse_transfer_log(log, transfer_topic)
            if parsed is None:
                continue
            log_from, log_to, token_address, amount = parsed
            if log_from == wallet:
                transfers_from_wallet.append((token_address, amount))
            if log_to == wallet:
                transfers_to_wallet.append((token_address, amount))

        return _WalletTransfers(
            from_wallet=transfers_from_wallet,
            to_wallet=transfers_to_wallet,
        )

    def _parse_transfer_log(
        self,
        log: dict[str, Any],
        transfer_topic: str,
    ) -> tuple[str, str, str, int] | None:
        """Decode one ERC-20 Transfer log into (from, to, token, amount).

        Returns None when the log is not an ERC-20 Transfer OR when the data
        cannot be decoded. Extracted from ``_collect_wallet_transfers`` so the
        hot-path loop stays CC-small and each branch is independently testable.
        """
        topics = log.get("topics", [])
        if not topics or len(topics) < 3:
            return None
        if self._normalize_topic(topics[0]) != transfer_topic:
            return None

        data = HexDecoder.normalize_hex(log.get("data", ""))
        if not data:
            return None
        try:
            amount = HexDecoder.decode_uint256(data, 0)
        except (ValueError, IndexError):
            return None

        log_from = HexDecoder.topic_to_address(topics[1])
        log_to = HexDecoder.topic_to_address(topics[2])
        raw_token = log.get("address") or ""
        token_address = self._normalize_topic(raw_token) if raw_token else ""
        return log_from, log_to, token_address, amount

    @staticmethod
    def _pick_swap_raw_amounts(
        transfers: "_WalletTransfers",
    ) -> "_SwapSeed | None":
        """Pick raw (amount, token) pairs for a wallet-level swap.

        Multi-hop semantics: wallet pays the FIRST token it transfers out,
        receives the LAST token it transfers in. That covers both direct swaps
        (1 in, 1 out) and router-orchestrated multi-hops where intermediate
        transfers traverse router / pool contracts (and thus are not tied to
        the wallet).

        Returns None when no wallet-in transfer was seen OR when the resulting
        ``amount_out`` is zero (current PCS V3 behavior — zero output is
        treated as "no swap signal").
        """
        if not transfers.from_wallet or not transfers.to_wallet:
            return None

        token_in_addr, amount_in = transfers.from_wallet[0]
        token_out_addr, amount_out = transfers.to_wallet[-1]

        if amount_out == 0:
            return None

        return _SwapSeed(
            token_in=token_in_addr,
            token_out=token_out_addr,
            amount_in=amount_in,
            amount_out=amount_out,
        )

    def _resolve_swap_decimals(
        self,
        seed: "_SwapSeed",
    ) -> "_SwapDecimals | None":
        """Resolve (in, out) decimals; fail closed only when OUT is unknown.

        Historical invariant (preserved for Phase 8.4):
        - Output decimals unknown -> return None (would corrupt PnL).
        - Input decimals unknown  -> proceed with ``decimals_in = None``;
          ``_build_swap_amounts`` emits ``None`` (NOT ``Decimal(0)``) for
          ``amount_in_decimal`` and ``effective_price`` so downstream
          accounting can distinguish "unmeasured" from "measured zero"
          (the "Empty != zero" invariant from blueprints/27-accounting.md).
          ``amount_in_decimal_resolved=False`` continues to mark the row.
        """
        decimals_in = self._resolve_decimals(seed.token_in)
        decimals_out = self._resolve_decimals(seed.token_out)
        if decimals_out is None:
            logger.warning("Cannot compute swap amounts: output token decimals unknown")
            return None
        return _SwapDecimals(decimals_in=decimals_in, decimals_out=decimals_out)

    @staticmethod
    def _build_swap_amounts(
        seed: "_SwapSeed",
        decimals: "_SwapDecimals",
        expected_out: Decimal | None,
    ) -> "SwapAmounts":
        """Assemble the final SwapAmounts, including realized slippage."""
        from almanak.framework.execution.extracted_data import SwapAmounts

        # "Empty != zero" invariant (blueprints/27-accounting.md):
        # When input decimals could not be resolved, we cannot compute
        # ``amount_in_decimal`` -- emit ``None`` (unmeasured), NOT
        # ``Decimal(0)`` (measured zero). The raw integer ``amount_in``
        # is still preserved so the row can be emitted gracefully.
        amount_in_decimal: Decimal | None
        if seed.amount_in and decimals.decimals_in is not None:
            amount_in_decimal = Decimal(seed.amount_in) / Decimal(10**decimals.decimals_in)
        else:
            amount_in_decimal = None

        amount_out_decimal = Decimal(seed.amount_out) / Decimal(10**decimals.decimals_out)

        # ``effective_price`` is unmeasurable when ``amount_in_decimal`` is
        # unmeasured -- emit ``None``, never substitute a sentinel zero.
        # The ledger writer / swap_handler already handle ``None`` via the
        # existing ``is not None`` guards.
        effective_price: Decimal | None
        if amount_in_decimal is not None and amount_in_decimal > 0:
            effective_price = amount_out_decimal / amount_in_decimal
        else:
            effective_price = None

        # VIB-3203 Phase B: realized slippage when enricher supplies a quote.
        slippage_bps: int | None = None
        if expected_out is not None and expected_out > 0 and amount_out_decimal > 0:
            realized = (expected_out - amount_out_decimal) / expected_out
            slippage_bps = int(realized * Decimal(10_000))

        # ``amount_in_decimal_resolved=False`` flags the asymmetric
        # fail-soft case where input decimals were unknown. Combined with
        # ``amount_in_decimal=None`` and ``effective_price=None`` this
        # preserves the "Empty != zero" invariant -- downstream consumers
        # see "unmeasured", not a literal zero.
        return SwapAmounts(
            amount_in=seed.amount_in,
            amount_out=seed.amount_out,
            amount_in_decimal=amount_in_decimal,
            amount_out_decimal=amount_out_decimal,
            effective_price=effective_price,
            slippage_bps=slippage_bps,
            expected_out_decimal=expected_out,
            token_in=seed.token_in or None,
            token_out=seed.token_out or None,
            amount_in_decimal_resolved=decimals.decimals_in is not None,
            amount_out_decimal_resolved=True,
        )

    def _resolve_decimals(self, token_address: str) -> int | None:
        """Resolve token decimals via the token resolver.

        Returns None if the resolver is unavailable or the token is unknown.
        """
        if not token_address:
            return None
        try:
            from almanak.framework.data.tokens import get_token_resolver

            resolver = get_token_resolver()
            token = resolver.resolve(token_address, self.chain)
            return token.decimals
        except Exception as e:
            logger.warning(f"Could not resolve decimals for {token_address}: {e}")
            return None

    def extract_position_id(self, receipt: dict[str, Any]) -> int | None:
        """Extract LP position ID (NFT tokenId) from a transaction receipt.

        Looks for ERC-721 Transfer events from the NonfungiblePositionManager
        where from=address(0), indicating a mint.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            Position ID (tokenId) if found, None otherwise
        """
        try:
            logs = receipt.get("logs", [])
            if not logs:
                return None

            position_manager = POSITION_MANAGER_ADDRESSES.get(self.chain, "").lower()
            if not position_manager:
                position_manager = "0x46A15B0b27311cedF172AB29E4f4766fbE7F4364".lower()

            transfer_topic = EVENT_TOPICS["Transfer"].lower()

            for log in logs:
                topics = log.get("topics", [])
                address = log.get("address", "")

                if isinstance(address, bytes):
                    address = "0x" + address.hex()
                address = str(address).lower()

                if address != position_manager:
                    continue

                if len(topics) < 4:
                    continue

                first_topic = topics[0]
                if isinstance(first_topic, bytes):
                    first_topic = "0x" + first_topic.hex()
                first_topic = str(first_topic).lower()

                if first_topic != transfer_topic:
                    continue

                from_topic = topics[1]
                if isinstance(from_topic, bytes):
                    from_topic = "0x" + from_topic.hex()
                from_topic = str(from_topic).lower()

                if from_topic != ZERO_ADDRESS_PADDED:
                    continue

                token_id_topic = topics[3]
                if isinstance(token_id_topic, bytes):
                    token_id_topic = "0x" + token_id_topic.hex()
                token_id_topic = str(token_id_topic)

                try:
                    token_id = int(token_id_topic, 16)
                    logger.info(f"Extracted PancakeSwap V3 LP position ID: {token_id}")
                    return token_id
                except (ValueError, TypeError):
                    continue

            return None

        except Exception as e:
            logger.warning(f"Failed to extract position ID: {e}")
            return None

    def extract_tick_lower(self, receipt: dict[str, Any]) -> int | None:
        """Extract tick lower from LP mint transaction receipt.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            Tick lower value if found, None otherwise
        """
        try:
            logs = receipt.get("logs", [])
            if not logs:
                return None

            mint_topic = EVENT_TOPICS["Mint"].lower()

            for log in logs:
                topics = log.get("topics", [])
                if len(topics) < 4:
                    continue

                first_topic = topics[0]
                if isinstance(first_topic, bytes):
                    first_topic = "0x" + first_topic.hex()
                first_topic = str(first_topic).lower()

                if first_topic != mint_topic:
                    continue

                tick_lower_topic = topics[2]
                if isinstance(tick_lower_topic, bytes):
                    tick_lower_topic = "0x" + tick_lower_topic.hex()
                tick_lower_topic = str(tick_lower_topic)

                tick_lower = HexDecoder.decode_int24(tick_lower_topic, 0)
                return tick_lower

            return None

        except Exception as e:
            logger.warning(f"Failed to extract tick_lower: {e}")
            return None

    def extract_tick_upper(self, receipt: dict[str, Any]) -> int | None:
        """Extract tick upper from LP mint transaction receipt.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            Tick upper value if found, None otherwise
        """
        try:
            logs = receipt.get("logs", [])
            if not logs:
                return None

            mint_topic = EVENT_TOPICS["Mint"].lower()

            for log in logs:
                topics = log.get("topics", [])
                if len(topics) < 4:
                    continue

                first_topic = topics[0]
                if isinstance(first_topic, bytes):
                    first_topic = "0x" + first_topic.hex()
                first_topic = str(first_topic).lower()

                if first_topic != mint_topic:
                    continue

                tick_upper_topic = topics[3]
                if isinstance(tick_upper_topic, bytes):
                    tick_upper_topic = "0x" + tick_upper_topic.hex()
                tick_upper_topic = str(tick_upper_topic)

                tick_upper = HexDecoder.decode_int24(tick_upper_topic, 0)
                return tick_upper

            return None

        except Exception as e:
            logger.warning(f"Failed to extract tick_upper: {e}")
            return None

    def extract_liquidity(self, receipt: dict[str, Any]) -> int | None:
        """Extract liquidity from LP mint transaction receipt.

        Mint event data layout (non-indexed fields, 32-byte padded):
        - sender (address): offset 0
        - amount (uint128): offset 32
        - amount0 (uint256): offset 64
        - amount1 (uint256): offset 96

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            Liquidity amount if found, None otherwise
        """
        try:
            logs = receipt.get("logs", [])
            if not logs:
                return None

            mint_topic = EVENT_TOPICS["Mint"].lower()

            for log in logs:
                topics = log.get("topics", [])
                if len(topics) < 4:
                    continue

                first_topic = topics[0]
                if isinstance(first_topic, bytes):
                    first_topic = "0x" + first_topic.hex()
                first_topic = str(first_topic).lower()

                if first_topic != mint_topic:
                    continue

                data = HexDecoder.normalize_hex(log.get("data", ""))
                if not data or data == "0x":
                    continue

                # Liquidity is at offset 32 (after sender address at offset 0)
                liquidity = HexDecoder.decode_uint128(data, 32)
                return liquidity

            return None

        except Exception as e:
            logger.warning(f"Failed to extract liquidity: {e}")
            return None

    def extract_lp_close_data(self, receipt: dict[str, Any]) -> "LPCloseData | None":
        """Extract LP close data from transaction receipt.

        Looks for Collect events which indicate fees and principal being collected.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            LPCloseData dataclass if Collect event found, None otherwise
        """
        from almanak.framework.execution.extracted_data import LPCloseData

        try:
            logs = receipt.get("logs", [])
            if not logs:
                return None

            collect_topic = EVENT_TOPICS["Collect"].lower()
            burn_topic = EVENT_TOPICS["Burn"].lower()

            total_amount0 = 0
            total_amount1 = 0
            liquidity_removed = None

            for log in logs:
                topics = log.get("topics", [])
                if not topics:
                    continue

                first_topic = topics[0]
                if isinstance(first_topic, bytes):
                    first_topic = "0x" + first_topic.hex()
                first_topic = str(first_topic).lower()

                data = HexDecoder.normalize_hex(log.get("data", ""))

                if first_topic == collect_topic and len(topics) >= 4:
                    # Collect event data layout (non-indexed fields):
                    # - recipient (address, padded to 32 bytes) - offset 0
                    # - amount0 (uint128) - offset 32
                    # - amount1 (uint128) - offset 64
                    amount0 = HexDecoder.decode_uint128(data, 32)
                    amount1 = HexDecoder.decode_uint128(data, 64)
                    total_amount0 += amount0
                    total_amount1 += amount1

                elif first_topic == burn_topic and len(topics) >= 4:
                    liquidity_removed = HexDecoder.decode_uint128(data, 0)

            if total_amount0 > 0 or total_amount1 > 0:
                return LPCloseData(
                    amount0_collected=total_amount0,
                    amount1_collected=total_amount1,
                    fees0=0,
                    fees1=0,
                    liquidity_removed=liquidity_removed,
                )

            return None

        except Exception as e:
            logger.warning(f"Failed to extract lp_close_data: {e}")
            return None

    # =============================================================================
    # Protocol Fee Extraction (VIB-3204)
    # =============================================================================

    def extract_protocol_fees(
        self,
        receipt: dict[str, Any],
        *,
        fee_tier_bps: int | None = None,
    ) -> "ProtocolFees | None":
        """Extract DEX protocol fees from a PancakeSwap V3 swap receipt.

        PancakeSwap V3 is a Uniswap V3 fork with the same pip-based fee
        tiers. The fee tier is resolved at compile time and forwarded by
        the ResultEnricher via ``bundle_metadata["selected_fee_tier"]``
        (signature-introspection opt-in — see VIB-3203 / VIB-3204).

        VIB-3204 audit fix (Codex P1, pr-auditor Blocker #2): do NOT
        return ``ProtocolFees(total_usd=Decimal(0))`` when the fee is
        known-non-zero but USD conversion isn't available. That would
        systematically under-attribute swap costs — a silent accounting
        bug. Until a price oracle is plumbed through to this layer, this
        parser returns ``None`` (ExtractMissing semantic). Callers that
        want the in-token fee can derive it from
        ``amount_in_decimal * fee_tier_bps / 1_000_000`` using values
        already on ``result.swap_amounts``.

        Args:
            receipt: Transaction receipt dict with 'logs' field.
            fee_tier_bps: Pool fee tier in pips (e.g. 500 = 5 bps),
                forwarded by the enricher.

        Returns:
            ``None`` — USD conversion not available at this layer. A
            future iteration with price-oracle access will return a
            populated ``ProtocolFees``. Also returns ``None`` when no
            Swap event is present OR when ``fee_tier_bps`` is missing.
        """
        # ProtocolFees import kept local to preserve import order semantics
        # inherited from the pre-fix version of this method.
        from almanak.framework.execution.extracted_data import ProtocolFees  # noqa: F401

        if fee_tier_bps is None or fee_tier_bps <= 0:
            return None

        try:
            status = receipt.get("status", 0)
            if isinstance(status, str):
                status = int(status, 16) if status.startswith("0x") else int(status)
            if status != 1:
                return None

            swap_topic = self._normalize_topic(EVENT_TOPICS["Swap"])
            logs = receipt.get("logs", []) or []
            for log in logs:
                topics = log.get("topics") or []
                if not topics:
                    continue
                if self._normalize_topic(topics[0]) == swap_topic:
                    # Swap detected + valid fee_tier_bps, but we cannot
                    # convert to USD in this layer. Return None (unknown)
                    # rather than a misleading Decimal(0).
                    return None
            return None

        except Exception as e:
            logger.warning(f"Failed to extract protocol_fees: {e}")
            return None

    # Backward compatibility methods
    def parse_swap(self, log: dict[str, Any]) -> SwapEventData | None:
        """Parse a Swap event from a single log entry.

        Backward compatibility method.
        """
        topics = log.get("topics", [])
        data = log.get("data", "")
        pool_address = log.get("address", "")

        if isinstance(data, bytes):
            data = "0x" + data.hex()
        if isinstance(pool_address, bytes):
            pool_address = "0x" + pool_address.hex()

        # Decode using internal method
        data_normalized = HexDecoder.normalize_hex(data)
        decoded_data = self._decode_log_data("Swap", topics, data_normalized, pool_address)

        if not decoded_data:
            return None

        return self._create_event(
            event_name="Swap",
            log_index=log.get("logIndex", 0),
            tx_hash="",
            block_number=0,
            contract_address=pool_address,
            decoded_data=decoded_data,
            raw_topics=[],
            raw_data="",
        )

    def is_pancakeswap_event(self, topic: str | bytes) -> bool:
        """Check if a topic is a known PancakeSwap V3 event.

        Args:
            topic: Event topic (supports bytes, hex string with/without 0x, any case)

        Returns:
            True if topic is a known PancakeSwap V3 event
        """
        if isinstance(topic, bytes):
            topic = "0x" + topic.hex()
        else:
            topic = str(topic)
        if not topic.startswith("0x"):
            topic = "0x" + topic
        topic = topic.lower()
        return self.registry.is_known_event(topic) if self.registry else False

    def get_event_type(self, topic: str | bytes) -> PancakeSwapV3EventType:
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
        if self.registry is None:
            return PancakeSwapV3EventType.UNKNOWN
        return self.registry.get_event_type_from_topic(topic) or PancakeSwapV3EventType.UNKNOWN


__all__ = [
    "PancakeSwapV3ReceiptParser",
    "PancakeSwapV3EventType",
    "SwapEventData",
    "ParseResult",
    "EVENT_TOPICS",
    "TOPIC_TO_EVENT",
    "EVENT_NAME_TO_TYPE",
    "POSITION_MANAGER_ADDRESSES",
]
