"""Uniswap V4 Receipt Parser.

Parses transaction receipts for V4 events emitted by PoolManager and
PositionManager:
- Swap events (PoolManager)
- ModifyLiquidity events (PoolManager)
- ERC-721 Transfer events (PositionManager, for position ID extraction)

V4 Swap event:
    event Swap(
        PoolId indexed id,
        address indexed sender,
        int128 amount0,
        int128 amount1,
        uint160 sqrtPriceX96,
        uint128 liquidity,
        int24 tick,
        uint24 fee
    )

V4 ModifyLiquidity event:
    event ModifyLiquidity(
        PoolId indexed id,
        address indexed sender,
        int24 tickLower,
        int24 tickUpper,
        int256 liquidityDelta,
        bytes32 salt
    )
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import TYPE_CHECKING, Any

from almanak.framework.connectors.base import HexDecoder

if TYPE_CHECKING:
    from almanak.framework.execution.extracted_data import LPCloseData, SwapAmounts

logger = logging.getLogger(__name__)


# =============================================================================
# Event Topic Signatures
# =============================================================================

# V4 PoolManager event topics
EVENT_TOPICS: dict[str, str] = {
    # Swap(bytes32 indexed id, address indexed sender, int128 amount0, int128 amount1,
    #       uint160 sqrtPriceX96, uint128 liquidity, int24 tick, uint24 fee)
    "Swap": "0x40e9cecb9f5f1f1c5b9c97dec2917b7ee92e57ba5563708daca94dd84ad7112f",
    # ModifyLiquidity(bytes32 indexed id, address indexed sender,
    #                  int24 tickLower, int24 tickUpper, int256 liquidityDelta, bytes32 salt)
    "ModifyLiquidity": "0xf208f4912782fd25c7f114ca3723a2d5dd6f3bcc3ac8db5af63baa85f711d5ec",
    # Transfer (ERC-20 standard)
    "Transfer": "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
    # Approval (ERC-20 standard)
    "Approval": "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925",
}

SWAP_EVENT_TOPIC = EVENT_TOPICS["Swap"]
MODIFY_LIQUIDITY_TOPIC = EVENT_TOPICS["ModifyLiquidity"]
TRANSFER_EVENT_TOPIC = EVENT_TOPICS["Transfer"]


# =============================================================================
# Enums
# =============================================================================


class UniswapV4EventType(Enum):
    """Uniswap V4 event types."""

    SWAP = "SWAP"
    MODIFY_LIQUIDITY = "MODIFY_LIQUIDITY"
    TRANSFER = "TRANSFER"
    APPROVAL = "APPROVAL"
    UNKNOWN = "UNKNOWN"


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class SwapEventData:
    """Decoded V4 Swap event data."""

    pool_id: str
    sender: str
    amount0: int
    amount1: int
    sqrt_price_x96: int
    liquidity: int
    tick: int
    fee: int


@dataclass
class ModifyLiquidityEventData:
    """Decoded V4 ModifyLiquidity event data."""

    pool_id: str
    sender: str
    tick_lower: int
    tick_upper: int
    liquidity_delta: int
    salt: str


@dataclass
class TransferEventData:
    """Decoded ERC-20/ERC-721 Transfer event."""

    token: str
    from_address: str
    to_address: str
    amount: int


@dataclass
class ParsedSwapResult:
    """High-level parsed swap result.

    ``amount_in_decimal`` / ``amount_out_decimal`` retain the historical
    ``Decimal(0)`` default so downstream consumers that never checked for
    None continue to see a safe sentinel (type is intentionally ``Decimal``,
    NOT ``Decimal | None`` — see issue #1778 guardrail).

    The companion ``amount_in_decimal_resolved`` / ``amount_out_decimal_resolved``
    flags let callers that DO care about the distinction (e.g. the
    observability ledger) tell a measured zero apart from an unresolvable-
    decimals sentinel without having to re-derive that state. ``True``
    means the human-readable amount was computed from a successfully
    resolved ``decimals`` value on the token resolver; ``False`` means the
    parser fell back to ``Decimal(0)`` because decimals were not
    resolvable for that side (#1778).
    """

    amount_in: int
    amount_out: int
    amount_in_decimal: Decimal = Decimal(0)
    amount_out_decimal: Decimal = Decimal(0)
    token_in: str | None = None
    token_out: str | None = None
    effective_price: Decimal | None = None
    price_impact_bps: int | None = None
    slippage_bps: int | None = None
    tick_after: int | None = None
    sqrt_price_x96_after: int | None = None
    amount_in_decimal_resolved: bool = True
    amount_out_decimal_resolved: bool = True


@dataclass
class ParseResult:
    """Full parse result from a V4 transaction receipt."""

    swap_events: list[SwapEventData] = field(default_factory=list)
    modify_liquidity_events: list[ModifyLiquidityEventData] = field(default_factory=list)
    transfer_events: list[TransferEventData] = field(default_factory=list)
    swap_result: ParsedSwapResult | None = None
    error: str | None = None


# =============================================================================
# UniswapV4ReceiptParser
# =============================================================================


class UniswapV4ReceiptParser:
    """Parse Uniswap V4 transaction receipts.

    Extracts swap amounts, effective prices, and balance deltas from
    V4 PoolManager events.

    Args:
        chain: Chain name for context.
        pool_manager_address: PoolManager address to filter events.
    """

    def __init__(
        self,
        chain: str = "ethereum",
        pool_manager_address: str | None = None,
        position_manager_address: str | None = None,
        token_resolver: Any | None = None,
    ) -> None:
        self.chain = chain.lower()
        self._token_resolver = token_resolver

        from almanak.core.contracts import UNISWAP_V4

        chain_addrs = UNISWAP_V4.get(self.chain, {})
        if pool_manager_address:
            self.pool_manager = pool_manager_address.lower()
        else:
            self.pool_manager = chain_addrs.get("pool_manager", "").lower()

        if position_manager_address:
            self.position_manager = position_manager_address.lower()
        else:
            self.position_manager = chain_addrs.get("position_manager", "").lower()

        # Infrastructure address set used by the direction-based token
        # identification fallback (see ``_identify_tokens_by_direction``).
        # A Transfer that enters or leaves one of these addresses is
        # infra-routing flow (user <-> swap rails), not a user-to-user
        # transfer. This MUST include more than the PoolManager — V4 swaps
        # often route ERC-20 legs through UniversalRouter + Permit2 and
        # WRAP_ETH / UNWRAP_WETH touches the chain's canonical wrapped-native
        # contract rather than the PoolManager. A narrow set (pool_manager
        # only) silently degrades the fallback to log-order elimination —
        # see issue #1767.
        #
        # Canonical Permit2 address is the same on every EVM chain
        # (https://github.com/Uniswap/permit2). Re-using the SDK's own
        # constant rather than re-declaring it keeps the two in sync.
        from almanak.framework.connectors.uniswap_v4.sdk import PERMIT2_ADDRESS
        from almanak.framework.data.tokens.defaults import WRAPPED_NATIVE

        infra_addresses: set[str] = set()
        if self.pool_manager:
            infra_addresses.add(self.pool_manager)
        if self.position_manager:
            infra_addresses.add(self.position_manager)
        universal_router = chain_addrs.get("universal_router", "")
        if universal_router:
            infra_addresses.add(universal_router.lower())
        infra_addresses.add(PERMIT2_ADDRESS.lower())
        wrapped_native = WRAPPED_NATIVE.get(self.chain, "")
        if wrapped_native:
            infra_addresses.add(wrapped_native.lower())
        self._infra_addresses: frozenset[str] = frozenset(infra_addresses)

    def parse_receipt(
        self,
        receipt: dict[str, Any],
        quoted_amount_out: int | None = None,
    ) -> ParseResult:
        """Parse a transaction receipt for V4 events.

        Args:
            receipt: Transaction receipt dict with 'logs' field.
            quoted_amount_out: Expected output for slippage calculation.

        Returns:
            ParseResult with decoded events and swap summary.
        """
        result = ParseResult()
        logs = receipt.get("logs", [])

        for log in logs:
            topics = log.get("topics", [])
            if not topics:
                continue

            topic0 = topics[0].lower() if isinstance(topics[0], str) else hex(topics[0])

            if topic0 == SWAP_EVENT_TOPIC.lower():
                swap_event = self._decode_swap_event(log)
                if swap_event:
                    result.swap_events.append(swap_event)

            elif topic0 == MODIFY_LIQUIDITY_TOPIC.lower():
                ml_event = self._decode_modify_liquidity_event(log)
                if ml_event:
                    result.modify_liquidity_events.append(ml_event)

            elif topic0 == TRANSFER_EVENT_TOPIC.lower():
                transfer = self._decode_transfer_event(log)
                if transfer:
                    result.transfer_events.append(transfer)

        # Build high-level swap result from events
        if result.swap_events:
            result.swap_result = self._build_swap_result(
                result.swap_events,
                result.transfer_events,
                quoted_amount_out,
            )

        return result

    def extract_swap_amounts(
        self,
        receipt: dict[str, Any],
        *,
        expected_out: Decimal | None = None,
    ) -> SwapAmounts | None:
        """Extract swap amounts for ResultEnricher integration.

        Args:
            receipt: Transaction receipt dict.
            expected_out: VIB-3203 — pre-slippage-discount quote in human
                (Decimal) units from the compiler's ActionBundle metadata.
                Overrides the parser's internal ``slippage_bps`` when provided,
                since the enrichment path does not supply constructor-level
                quote data.

        Returns:
            SwapAmounts or None if no swap event found.
        """
        from almanak.framework.execution.extracted_data import SwapAmounts

        parsed = self.parse_receipt(receipt)
        if not parsed.swap_result:
            return None

        sr = parsed.swap_result

        # VIB-3203: prefer the framework-supplied ``expected_out`` quote.
        slippage_bps = sr.slippage_bps
        if expected_out is not None and expected_out > 0 and sr.amount_out_decimal > 0:
            realized_slippage = (expected_out - sr.amount_out_decimal) / expected_out
            slippage_bps = int(realized_slippage * Decimal(10_000))

        return SwapAmounts(
            amount_in=sr.amount_in,
            amount_out=sr.amount_out,
            amount_in_decimal=sr.amount_in_decimal,
            amount_out_decimal=sr.amount_out_decimal,
            effective_price=sr.effective_price or Decimal(0),
            slippage_bps=slippage_bps,
            expected_out_decimal=expected_out,
            token_in=sr.token_in,
            token_out=sr.token_out,
            amount_in_decimal_resolved=sr.amount_in_decimal_resolved,
            amount_out_decimal_resolved=sr.amount_out_decimal_resolved,
        )

    def extract_position_id(self, receipt: dict[str, Any]) -> int | None:
        """Extract LP position NFT tokenId from ERC-721 Transfer event.

        Looks for a Transfer event emitted by the PositionManager contract
        where from_address is the zero address (indicating a mint).

        Falls back to ERC-721 mint Transfers from other known V4 PositionManager
        addresses if no exact chain match is found (handles address mismatches
        or proxy patterns). Rejects mints from unknown contracts to fail closed.

        Called by ResultEnricher for LP_OPEN intents.

        Args:
            receipt: Transaction receipt dict.

        Returns:
            Position ID (tokenId) or None if not found.
        """
        logs = receipt.get("logs", [])
        tx_hash = receipt.get("transactionHash", "unknown")

        # Build set of known V4 PositionManager addresses for fallback constraint
        from almanak.core.contracts import UNISWAP_V4

        known_pm_addresses = {
            addrs["position_manager"].lower() for addrs in UNISWAP_V4.values() if addrs.get("position_manager")
        }

        # Collect ERC-721 mint Transfer candidates as fallback
        fallback_candidates: list[tuple[int, str]] = []  # (token_id, emitting_address)

        for log in logs:
            topics = log.get("topics", [])
            if len(topics) < 4:
                continue

            topic0 = topics[0].lower() if isinstance(topics[0], str) else hex(topics[0])
            if topic0 != TRANSFER_EVENT_TOPIC.lower():
                continue

            # ERC-721 Transfer: topic[1]=from, topic[2]=to, topic[3]=tokenId
            from_addr = topics[1] if isinstance(topics[1], str) else hex(topics[1])

            # Only consider mint events (from = zero address)
            try:
                if int(from_addr, 16) != 0:
                    continue
            except (ValueError, TypeError):
                continue

            token_id_hex = topics[3] if isinstance(topics[3], str) else hex(topics[3])
            try:
                token_id = int(token_id_hex, 16)
            except (ValueError, TypeError):
                continue

            # Check if emitted by PositionManager (preferred match)
            log_address = log.get("address", "")
            log_address_lower = log_address.lower() if isinstance(log_address, str) else ""
            if self.position_manager and log_address_lower == self.position_manager:
                return token_id

            # Only consider known V4 PositionManager addresses as fallback candidates
            if log_address_lower in known_pm_addresses:
                fallback_candidates.append((token_id, log_address_lower))

        if len(fallback_candidates) == 1:
            token_id, emitter = fallback_candidates[0]
            logger.warning(
                "V4 extract_position_id: no exact PositionManager match (%s), using fallback tokenId=%d "
                "from known V4 PM %s. tx=%s, chain=%s",
                self.position_manager,
                token_id,
                emitter,
                tx_hash,
                self.chain,
            )
            return token_id

        if len(fallback_candidates) > 1:
            logger.error(
                "V4 extract_position_id: %d ambiguous ERC-721 mint candidates from known V4 PMs "
                "(expected 1). Failing closed to avoid storing wrong position_id. "
                "candidates=%s, position_manager=%s, chain=%s, tx=%s",
                len(fallback_candidates),
                [(tid, addr) for tid, addr in fallback_candidates],
                self.position_manager,
                self.chain,
                tx_hash,
            )
            return None

        # Log diagnostic info when extraction fails completely
        transfer_count = sum(
            1
            for log in logs
            if len(log.get("topics", [])) >= 4
            and (log["topics"][0].lower() if isinstance(log["topics"][0], str) else "") == TRANSFER_EVENT_TOPIC.lower()
        )
        logger.warning(
            "V4 extract_position_id: no position ID found. "
            "total_logs=%d, erc721_transfer_events=%d, position_manager=%s, chain=%s, tx=%s",
            len(logs),
            transfer_count,
            self.position_manager,
            self.chain,
            tx_hash,
        )
        return None

    def extract_liquidity(self, receipt: dict[str, Any]) -> int | None:
        """Extract liquidity delta from ModifyLiquidity event.

        Called by ResultEnricher for LP_OPEN intents.

        Args:
            receipt: Transaction receipt dict.

        Returns:
            Liquidity amount or None if not found.
        """
        parsed = self.parse_receipt(receipt)
        if not parsed.modify_liquidity_events:
            return None

        # Return the first positive (mint) liquidity delta
        for event in parsed.modify_liquidity_events:
            if event.liquidity_delta > 0:
                return event.liquidity_delta

        return None

    def extract_lp_close_data(self, receipt: dict[str, Any]) -> LPCloseData | None:
        """Extract LP close data from ModifyLiquidity and Transfer events.

        Called by ResultEnricher for LP_CLOSE intents.

        Args:
            receipt: Transaction receipt dict.

        Returns:
            LPCloseData with collected amounts, or None if not found.
        """
        from almanak.framework.execution.extracted_data import LPCloseData

        parsed = self.parse_receipt(receipt)

        # Find the decrease event (negative liquidity delta)
        liquidity_removed = None
        for event in parsed.modify_liquidity_events:
            if event.liquidity_delta < 0:
                liquidity_removed = abs(event.liquidity_delta)
                break

        if liquidity_removed is None and not parsed.modify_liquidity_events:
            return None

        # Sum Transfer events FROM the pool manager TO the wallet (tokens collected)
        amount0_collected = 0
        amount1_collected = 0

        # Group transfers from PoolManager by token address
        collected_by_token: dict[str, int] = {}
        for transfer in parsed.transfer_events:
            if transfer.from_address.lower() == self.pool_manager:
                token = transfer.token.lower()
                collected_by_token[token] = collected_by_token.get(token, 0) + transfer.amount

        # Assign to amount0/amount1 by sorted token address order
        sorted_tokens = sorted(collected_by_token.keys())
        if len(sorted_tokens) >= 1:
            amount0_collected = collected_by_token[sorted_tokens[0]]
        if len(sorted_tokens) >= 2:
            amount1_collected = collected_by_token[sorted_tokens[1]]

        return LPCloseData(
            amount0_collected=amount0_collected,
            amount1_collected=amount1_collected,
            liquidity_removed=liquidity_removed,
        )

    # -- Decoding helpers -----------------------------------------------------

    def _decode_modify_liquidity_event(self, log: dict[str, Any]) -> ModifyLiquidityEventData | None:
        """Decode a V4 ModifyLiquidity event from a log entry."""
        topics = log.get("topics", [])
        data = log.get("data", "0x")

        if len(topics) < 3:
            return None

        try:
            pool_id = topics[1] if isinstance(topics[1], str) else hex(topics[1])
            sender = (
                HexDecoder.decode_address_from_data(topics[2][2:]) if isinstance(topics[2], str) else hex(topics[2])
            )

            # Data layout: int24 tickLower, int24 tickUpper, int256 liquidityDelta, bytes32 salt
            clean_data = data[2:] if data.startswith("0x") else data

            tick_lower = HexDecoder.decode_int24(clean_data[0:64])
            tick_upper = HexDecoder.decode_int24(clean_data[64:128])
            liquidity_delta = HexDecoder.decode_int256(clean_data[128:192])
            salt = "0x" + clean_data[192:256] if len(clean_data) >= 256 else "0x0"

            return ModifyLiquidityEventData(
                pool_id=pool_id,
                sender=sender,
                tick_lower=tick_lower,
                tick_upper=tick_upper,
                liquidity_delta=liquidity_delta,
                salt=salt,
            )
        except Exception as e:
            logger.warning("Failed to decode V4 ModifyLiquidity event: %s", e)
            return None

    def _decode_swap_event(self, log: dict[str, Any]) -> SwapEventData | None:
        """Decode a V4 Swap event from a log entry."""
        topics = log.get("topics", [])
        data = log.get("data", "0x")

        if len(topics) < 3:
            return None

        try:
            pool_id = topics[1] if isinstance(topics[1], str) else hex(topics[1])
            sender = (
                HexDecoder.decode_address_from_data(topics[2][2:]) if isinstance(topics[2], str) else hex(topics[2])
            )

            # Data layout: int128 amount0, int128 amount1, uint160 sqrtPriceX96,
            #              uint128 liquidity, int24 tick, uint24 fee
            # Each field is 32 bytes in ABI encoding
            clean_data = data[2:] if data.startswith("0x") else data

            amount0 = HexDecoder.decode_int256(clean_data[0:64])
            amount1 = HexDecoder.decode_int256(clean_data[64:128])
            sqrt_price_x96 = HexDecoder.decode_uint256(clean_data[128:192])
            liquidity = HexDecoder.decode_uint256(clean_data[192:256])
            tick = HexDecoder.decode_int24(clean_data[256:320])
            fee = HexDecoder.decode_uint256(clean_data[320:384])

            return SwapEventData(
                pool_id=pool_id,
                sender=sender,
                amount0=amount0,
                amount1=amount1,
                sqrt_price_x96=sqrt_price_x96,
                liquidity=liquidity,
                tick=tick,
                fee=fee,
            )
        except Exception as e:
            logger.warning("Failed to decode V4 Swap event: %s", e)
            return None

    def _decode_transfer_event(self, log: dict[str, Any]) -> TransferEventData | None:
        """Decode an ERC-20 Transfer event."""
        topics = log.get("topics", [])
        data = log.get("data", "0x")

        if len(topics) < 3:
            return None

        try:
            token = log.get("address", "").lower()
            from_addr = HexDecoder.decode_address_from_data(topics[1][2:]) if isinstance(topics[1], str) else ""
            to_addr = HexDecoder.decode_address_from_data(topics[2][2:]) if isinstance(topics[2], str) else ""

            clean_data = data[2:] if data.startswith("0x") else data
            amount = HexDecoder.decode_uint256(clean_data[0:64]) if clean_data else 0

            return TransferEventData(
                token=token,
                from_address=from_addr,
                to_address=to_addr,
                amount=amount,
            )
        except Exception as e:
            logger.warning("Failed to decode Transfer event: %s", e)
            return None

    # -- _build_swap_result phase helpers -------------------------------------
    #
    # _build_swap_result orchestrates five independent phases. Each phase is
    # extracted into a small, independently testable helper so the public
    # contract (ParsedSwapResult field semantics, sign conventions, and
    # parse_receipt API) is preserved byte-for-byte while CC drops well below
    # the refactor target.

    @staticmethod
    def _compute_swap_amounts(swap: SwapEventData) -> tuple[int, int]:
        """Derive (amount_in, amount_out) from a V4 Swap event.

        V4 sign convention (swapper's perspective):
            positive = tokens RECEIVED by the swapper from the pool
            negative = tokens PAID by the swapper to the pool
        Verified against real mainnet transactions (2026-03-29).
        """
        if swap.amount0 > 0:
            # Swapper received token0, paid token1
            amount_in = abs(swap.amount1)
            amount_out = swap.amount0
        else:
            # Swapper paid token0, received token1
            amount_in = abs(swap.amount0)
            amount_out = swap.amount1

        if amount_out <= 0 or amount_in <= 0:
            logger.warning(
                "V4 Swap event has unexpected signs: amount0=%s, amount1=%s",
                swap.amount0,
                swap.amount1,
            )
        return amount_in, amount_out

    @staticmethod
    def _calculate_slippage_bps(amount_out: int, quoted_amount_out: int | None) -> int | None:
        """Return realized slippage in bps vs the pre-trade quote, or None."""
        if quoted_amount_out and quoted_amount_out > 0 and amount_out > 0:
            slippage = (quoted_amount_out - amount_out) / quoted_amount_out
            return int(slippage * 10000)
        return None

    def _identify_tokens_by_pool_manager(
        self, transfer_events: list[TransferEventData]
    ) -> tuple[str | None, str | None]:
        """Primary path: Transfers directly to/from PoolManager identify in/out."""
        token_in_addr: str | None = None
        token_out_addr: str | None = None
        pool_manager = self.pool_manager
        for transfer in transfer_events:
            if transfer.to_address.lower() == pool_manager:
                token_in_addr = transfer.token
            elif transfer.from_address.lower() == pool_manager:
                token_out_addr = transfer.token
        return token_in_addr, token_out_addr

    @staticmethod
    def _identify_tokens_by_amount(
        transfer_events: list[TransferEventData],
        amount_in: int,
        amount_out: int,
        token_in_addr: str | None,
        token_out_addr: str | None,
    ) -> tuple[str | None, str | None]:
        """Fallback 1: V4 flash accounting via UniversalRouter/Permit2 may
        route Transfers away from PoolManager. Match by amount instead.
        Skip transfers for tokens already assigned to the other side to
        handle stablecoin-to-stablecoin swaps where amount_in == amount_out.
        """
        for transfer in transfer_events:
            if token_in_addr is None and transfer.amount == amount_in and transfer.token != token_out_addr:
                token_in_addr = transfer.token
            elif token_out_addr is None and transfer.amount == amount_out and transfer.token != token_in_addr:
                token_out_addr = transfer.token
        return token_in_addr, token_out_addr

    def _identify_tokens_by_direction(
        self,
        transfer_events: list[TransferEventData],
        token_in_addr: str | None,
        token_out_addr: str | None,
    ) -> tuple[str | None, str | None]:
        """Fallback 2: For WETH-routed swaps, ERC-20 amounts may diverge from
        Swap event amounts due to WRAP_ETH/UNWRAP_WETH. Identify tokens by
        transfer direction relative to any known infra address (PoolManager,
        PositionManager, UniversalRouter, Permit2, wrapped-native contract).

        Historically this used only ``{self.pool_manager}``, which silently
        failed for router-routed receipts (Transfers never touched
        PoolManager) and fell through to log-order-based elimination —
        issue #1767. The broadened ``self._infra_addresses`` catches those
        paths.

        Last-resort elimination now uses a deterministic tiebreaker
        (lowest-lowercase-address -> output) instead of log order, and logs
        a WARNING so operators see that the assignment is a guess. A
        deterministic guess is still a guess — callers downstream should
        treat tokens produced by this last-resort branch as lower
        confidence than tokens produced by the direction pass.
        """
        seen_tokens: set[str] = set()
        if token_in_addr:
            seen_tokens.add(token_in_addr.lower())
        if token_out_addr:
            seen_tokens.add(token_out_addr.lower())

        for transfer in transfer_events:
            token_lower = transfer.token.lower()
            if token_lower in seen_tokens:
                continue
            from_lower = transfer.from_address.lower()
            to_lower = transfer.to_address.lower()
            from_is_infra = from_lower in self._infra_addresses
            to_is_infra = to_lower in self._infra_addresses
            # Only directional evidence fires when EXACTLY ONE side is
            # infra (user <-> rails). Infra-to-infra hops (e.g. Permit2 ->
            # PoolManager) are internal routing plumbing and carry no
            # directional information about the user's swap.
            if from_is_infra == to_is_infra:
                continue
            # Token sent FROM infrastructure TO non-infra = output (user receives)
            if token_out_addr is None and from_is_infra:
                token_out_addr = transfer.token
                seen_tokens.add(token_lower)
            # Token sent TO infrastructure FROM non-infra = input (user pays)
            elif token_in_addr is None and to_is_infra:
                token_in_addr = transfer.token
                seen_tokens.add(token_lower)

        # Last resort: deterministic tiebreaker over remaining unseen
        # tokens — sort by lowercase address so the assignment does NOT
        # depend on log ordering. Lowest address -> output (arbitrary but
        # stable). Emit a WARNING: any hit here means all 3 identification
        # passes failed to find a signal, which is a suspicious receipt.
        if token_in_addr is None or token_out_addr is None:
            remaining = sorted(
                {t.token for t in transfer_events if t.token.lower() not in seen_tokens},
                key=lambda addr: addr.lower(),
            )
            if remaining:
                logger.warning(
                    "V4 receipt parser: direction fallback hit last-resort "
                    "tiebreaker on chain=%s; assigning %s by address order. "
                    "This indicates neither PoolManager, amount-match, nor "
                    "infra-direction pass identified token sides — the "
                    "receipt may be malformed or routed through an "
                    "unrecognized infrastructure address. See issue #1767.",
                    self.chain,
                    remaining,
                )
            for token in remaining:
                if token_out_addr is None:
                    token_out_addr = token
                elif token_in_addr is None:
                    token_in_addr = token
        return token_in_addr, token_out_addr

    def _identify_swap_tokens(
        self,
        transfer_events: list[TransferEventData],
        amount_in: int,
        amount_out: int,
    ) -> tuple[str | None, str | None]:
        """Orchestrate the three token-identification passes.

        Returns (token_in_addr, token_out_addr). Either may be None if the
        receipt does not contain enough Transfer evidence.
        """
        token_in_addr, token_out_addr = self._identify_tokens_by_pool_manager(transfer_events)
        if not transfer_events:
            return token_in_addr, token_out_addr

        if token_in_addr is None or token_out_addr is None:
            token_in_addr, token_out_addr = self._identify_tokens_by_amount(
                transfer_events, amount_in, amount_out, token_in_addr, token_out_addr
            )
        if token_in_addr is None or token_out_addr is None:
            token_in_addr, token_out_addr = self._identify_tokens_by_direction(
                transfer_events, token_in_addr, token_out_addr
            )
        return token_in_addr, token_out_addr

    def _resolve_token_decimals(
        self,
        token_in_addr: str | None,
        token_out_addr: str | None,
    ) -> tuple[int | None, int | None]:
        """Resolve decimals for token_in and token_out via the token_resolver.

        Lazy-loads the global resolver if one wasn't injected at construction.
        Returns (None, None) on any failure; callers must handle missing
        decimals by falling back to Decimal(0) for human-readable fields.
        """
        resolver = self._token_resolver
        if resolver is None:
            try:
                from almanak.framework.data.tokens import get_token_resolver

                resolver = get_token_resolver()
            except Exception:
                logger.debug("Could not load token_resolver for decimal conversion")

        token_in_decimals: int | None = None
        token_out_decimals: int | None = None
        if resolver and token_in_addr:
            try:
                token_in_decimals = resolver.resolve(token_in_addr, self.chain).decimals
            except Exception:
                logger.warning(
                    "Could not resolve decimals for token_in %s — decimal amounts will be zero",
                    token_in_addr,
                )
        if resolver and token_out_addr:
            try:
                token_out_decimals = resolver.resolve(token_out_addr, self.chain).decimals
            except Exception:
                logger.warning(
                    "Could not resolve decimals for token_out %s — decimal amounts will be zero",
                    token_out_addr,
                )
        return token_in_decimals, token_out_decimals

    @staticmethod
    def _compute_decimal_amounts(
        amount_in: int,
        amount_out: int,
        token_in_decimals: int | None,
        token_out_decimals: int | None,
    ) -> tuple[Decimal | None, Decimal | None, Decimal | None]:
        """Compute (amount_in_decimal, amount_out_decimal, effective_price).

        Returns ``None`` for either ``amount_*_decimal`` when decimals could
        not be resolved for that side. This explicit ``None`` lets callers
        (``_build_swap_result`` and ``extract_swap_amounts``) tell an
        unresolvable-decimals case apart from a legitimately measured zero
        — historically this helper emitted ``Decimal(0)`` as a sentinel,
        conflating the two (issue #1778, Codex finding on PR #1774).

        ``effective_price`` is computed ONLY when BOTH decimals are
        resolved AND both amounts are positive, to avoid mixing raw
        integers with Decimals for cross-decimal pairs (e.g. USDC/WETH),
        which would be off by orders of magnitude.
        """
        amount_in_decimal: Decimal | None
        amount_out_decimal: Decimal | None
        if token_in_decimals is not None:
            amount_in_decimal = Decimal(str(amount_in)) / Decimal(10**token_in_decimals)
        else:
            amount_in_decimal = None
        if token_out_decimals is not None:
            amount_out_decimal = Decimal(str(amount_out)) / Decimal(10**token_out_decimals)
        else:
            amount_out_decimal = None

        effective_price: Decimal | None = None
        if (
            amount_in_decimal is not None
            and amount_out_decimal is not None
            and amount_in_decimal > 0
            and amount_out_decimal > 0
        ):
            effective_price = amount_out_decimal / amount_in_decimal
        return amount_in_decimal, amount_out_decimal, effective_price

    def _build_swap_result(
        self,
        swap_events: list[SwapEventData],
        transfer_events: list[TransferEventData],
        quoted_amount_out: int | None,
    ) -> ParsedSwapResult:
        """Build a high-level swap result from decoded events.

        Orchestrates five pure phase helpers:
          1. _compute_swap_amounts       — sign convention
          2. _calculate_slippage_bps     — realized slippage vs quote
          3. _identify_swap_tokens       — pool_mgr / amount / direction passes
          4. _resolve_token_decimals     — lazy resolver lookup
          5. _compute_decimal_amounts    — human-readable amounts + price

        ``_compute_decimal_amounts`` now returns ``Decimal | None`` per side
        to distinguish "decimals unresolvable" from "measured zero"
        (#1778). ``ParsedSwapResult`` still carries ``Decimal`` fields for
        backward compatibility — the unresolvable case is coerced back to
        ``Decimal(0)`` here and flagged via ``*_decimal_resolved=False`` so
        downstream consumers that care about the distinction (ledger) can
        see it without a type change to the dataclass.
        """
        # Use the first swap event (single-hop; multi-hop receipts may emit
        # several Swap events but the first carries the user's input side).
        swap = swap_events[0]
        amount_in, amount_out = self._compute_swap_amounts(swap)
        slippage_bps = self._calculate_slippage_bps(amount_out, quoted_amount_out)
        token_in_addr, token_out_addr = self._identify_swap_tokens(transfer_events, amount_in, amount_out)
        token_in_decimals, token_out_decimals = self._resolve_token_decimals(token_in_addr, token_out_addr)
        amount_in_decimal_opt, amount_out_decimal_opt, effective_price = self._compute_decimal_amounts(
            amount_in, amount_out, token_in_decimals, token_out_decimals
        )
        amount_in_resolved = amount_in_decimal_opt is not None
        amount_out_resolved = amount_out_decimal_opt is not None
        amount_in_decimal = amount_in_decimal_opt if amount_in_decimal_opt is not None else Decimal(0)
        amount_out_decimal = amount_out_decimal_opt if amount_out_decimal_opt is not None else Decimal(0)

        return ParsedSwapResult(
            amount_in=amount_in,
            amount_out=amount_out,
            amount_in_decimal=amount_in_decimal,
            amount_out_decimal=amount_out_decimal,
            token_in=token_in_addr,
            token_out=token_out_addr,
            effective_price=effective_price,
            slippage_bps=slippage_bps,
            tick_after=swap.tick,
            sqrt_price_x96_after=swap.sqrt_price_x96,
            amount_in_decimal_resolved=amount_in_resolved,
            amount_out_decimal_resolved=amount_out_resolved,
        )


__all__ = [
    "EVENT_TOPICS",
    "ModifyLiquidityEventData",
    "ParsedSwapResult",
    "ParseResult",
    "SwapEventData",
    "TransferEventData",
    "UniswapV4EventType",
    "UniswapV4ReceiptParser",
]
