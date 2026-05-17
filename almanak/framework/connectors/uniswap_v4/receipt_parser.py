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
from collections.abc import Callable
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import TYPE_CHECKING, Any

from almanak.framework.connectors.base import HexDecoder
from almanak.framework.observability.metrics import (
    V4LPDropOutcome,
    V4LPDropReason,
    record_v4_lp_parser_drop,
)

if TYPE_CHECKING:
    from almanak.framework.connectors.uniswap_v4.sdk import PoolKey
    from almanak.framework.execution.extracted_data import LPCloseData, LPOpenData, SwapAmounts

# Sync ``(pool_id_hex, chain) -> PoolKey | None`` callable injected by the
# framework so the V4 receipt parser can resolve a ``ModifyLiquidity.pool_id``
# back to its canonical PoolKey (currency0 < currency1) without performing
# any network I/O itself. Production callers wrap
# ``gateway_pool_key_client.lookup_v4_pool_key`` (async); tests inject a
# direct dict-backed lambda.
PoolKeyLookup = Callable[[str, str], "PoolKey | None"]

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
        pool_key_lookup: PoolKeyLookup | None = None,
    ) -> None:
        self.chain = chain.lower()
        self._token_resolver = token_resolver
        self._pool_key_lookup = pool_key_lookup

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

    def _emit_drop_telemetry(
        self,
        *,
        outcome: V4LPDropOutcome,
        reason: V4LPDropReason,
        pool_id: str,
        tx_hash: str,
        extras: str = "",
    ) -> None:
        """Emit a structured WARNING and increment the parser-drops counter.

        The single chokepoint for every V4 LP parser drop path. Every drop
        site MUST go through here so the WARNING fields and the counter
        label set stay locked together. ``outcome="drop"`` for return-None
        paths; ``outcome="raise"`` for the native-ETH typed-error path
        (counter is still incremented BEFORE the raise so dashboards see
        the event).

        Args:
            outcome: "drop" or "raise".
            reason: ``V4LPDropReason`` member; its string value is the
                stable error code in both the log and the counter label.
            pool_id: 32-byte canonical V4 pool_id (lowercase 66-char hex).
            tx_hash: Receipt transaction hash for traceability.
            extras: Free-form ``key=value`` tokens already formatted by the
                caller, appended verbatim to the WARNING. Stays optional so
                the helper does not lock down per-reason payload shape.
        """
        record_v4_lp_parser_drop(chain=self.chain, reason=reason, outcome=outcome)
        suffix = f" {extras}" if extras else ""
        logger.warning(
            "V4 LP parser %s: pool_id=%s tx=%s outcome=%s reason=%s chain=%s%s",
            "raised" if outcome == "raise" else "dropped",
            pool_id,
            tx_hash,
            outcome,
            reason.value,
            self.chain,
            suffix,
        )

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

    def extract_lp_open_data(self, receipt: dict[str, Any]) -> LPOpenData | None:
        """Extract LP open data from a V4 mint receipt.

        VIB-4474 / V4 LP accounting V0. Walks the receipt for the canonical
        PositionManager-mediated mint shape:

        1. ``ModifyLiquidity`` with ``liquidity_delta > 0`` (a mint, not a burn)
           and ``sender`` in ``POSITION_MANAGER_ADDRESS_SET`` (allowlist).
        2. ERC-721 ``Transfer(from=0x0, ...)`` emitted by the PositionManager
           NFT contract to recover the position ``tokenId``.
        3. Salt/tokenId consistency check: ``salt == bytes32(tokenId)`` per
           v4-periphery ``PositionManager._mint()``. Mismatched salt is the
           failure signal -- non-zero salt that matches the tokenId is the
           CANONICAL V4 path and must pass.
        4. ``position_hash = keccak(packed(positionManager, tickLower, tickUpper, salt))``
           per v4-core ``Position.calculatePositionKey``.

        Amount attribution: sum ERC-20 Transfers landing in the PoolManager
        grouped by token, then assign by sorted-address order
        (currency0 < currency1 invariant). When only one currency is observed
        (e.g. a concentrated-liquidity position opened out of range, or a
        single-sided deposit), the gateway PoolKey lookup is invoked to
        resolve both currency addresses and stamp a measured zero on the
        unobserved leg (VIB-4535 — symmetric with T07's close-side
        ``extract_lp_close_data``). On lookup failure the LPOpenData is
        dropped (telemetry counters: ``missing_pool_key_lookup`` /
        ``pool_key_not_found`` / ``pool_key_lookup_error``) rather than
        emitted with ambiguous attribution.

        Non-allowlisted ``sender`` or salt/tokenId mismatch → structured
        WARNING + returns None. The writer must not crash on a parser miss
        (Empty != Zero / blueprint 27).

        Args:
            receipt: Transaction receipt dict with 'logs' field.

        Returns:
            ``LPOpenData`` with ``pool_address`` set to the 32-byte V4 pool_id
            (66-char lowercase hex) and ``position_hash`` set to the v4-core
            position key. ``None`` when no eligible mint was found or any
            validation gate fired.
        """
        from almanak.framework.connectors.uniswap_v4.hooks import compute_position_hash
        from almanak.framework.connectors.uniswap_v4.sdk import POSITION_MANAGER_ADDRESS_SET
        from almanak.framework.execution.extracted_data import LPOpenData

        parsed = self.parse_receipt(receipt)
        tx_hash = receipt.get("transactionHash", "unknown")

        mint_event: ModifyLiquidityEventData | None = None
        for event in parsed.modify_liquidity_events:
            if event.liquidity_delta > 0:
                mint_event = event
                break
        if mint_event is None:
            return None

        sender_lower = mint_event.sender.lower()
        if sender_lower not in POSITION_MANAGER_ADDRESS_SET:
            self._emit_drop_telemetry(
                outcome="drop",
                reason=V4LPDropReason.NON_POSITION_MANAGER_SENDER,
                pool_id=mint_event.pool_id,
                tx_hash=tx_hash,
                extras=f"sender={sender_lower}",
            )
            return None

        token_id = self.extract_position_id(receipt)
        if token_id is None:
            self._emit_drop_telemetry(
                outcome="drop",
                reason=V4LPDropReason.MISSING_POSITION_ID,
                pool_id=mint_event.pool_id,
                tx_hash=tx_hash,
            )
            return None

        expected_salt = "0x" + format(token_id, "064x")
        if mint_event.salt.lower() != expected_salt:
            self._emit_drop_telemetry(
                outcome="drop",
                reason=V4LPDropReason.SALT_TOKENID_MISMATCH,
                pool_id=mint_event.pool_id,
                tx_hash=tx_hash,
                extras=f"salt={mint_event.salt} expected={expected_salt} token_id={token_id}",
            )
            return None

        position_hash = compute_position_hash(
            owner=sender_lower,
            tick_lower=mint_event.tick_lower,
            tick_upper=mint_event.tick_upper,
            salt=mint_event.salt,
        )

        amount0, amount1, currency0, currency1 = self._sum_deposit_transfers_by_currency_order(parsed.transfer_events)

        # VIB-4535: when only one currency landed in PoolManager we cannot
        # honestly attribute it to currency0 vs currency1 from the observed
        # transfers alone. Resolve via the gateway PoolKey lookup -- mirror of
        # close-side T07 (extract_lp_close_data). The helper either returns
        # a resolved (amount0, amount1, currency0, currency1) tuple, returns
        # None to signal a drop, OR raises UniswapV4UnsupportedPoolError on
        # native-ETH currency0 (defense-in-depth; T06 adapter guard already
        # rejects at compile time).
        if amount0 is not None and amount1 is None:
            resolved = self._resolve_single_sided_lp_open(
                pool_id_hex=mint_event.pool_id.lower(),
                tx_hash=tx_hash,
                observed_currency=currency0,  # type: ignore[arg-type]
                observed_amount=amount0,
            )
            if resolved is None:
                return None
            amount0, amount1, currency0, currency1 = resolved

        current_tick: int | None = None
        for swap in parsed.swap_events:
            if swap.pool_id.lower() == mint_event.pool_id.lower():
                current_tick = swap.tick
                break

        return LPOpenData(
            position_id=token_id,
            tick_lower=mint_event.tick_lower,
            tick_upper=mint_event.tick_upper,
            liquidity=mint_event.liquidity_delta,
            amount0=amount0,
            amount1=amount1,
            current_tick=current_tick,
            pool_address=mint_event.pool_id.lower(),
            position_hash=position_hash,
            # VIB-4426 P1 #4 — emit canonical sorted currency addresses so
            # build_lp_accounting_event resolves token symbols/decimals by
            # address (not user-intent index). VIB-4535 closed the V0 hole
            # where single-sided opens left currency1 unresolved; the
            # PoolKey-lookup branch above now resolves both currencies (or
            # drops fail-loud on lookup failure) for those receipts.
            currency0=currency0,
            currency1=currency1,
        )

    def _sum_deposit_transfers_by_currency_order(
        self, transfer_events: list[TransferEventData]
    ) -> tuple[int | None, int | None, str | None, str | None]:
        """Aggregate deposit ERC-20 transfers (TO PoolManager) by token, then
        return ``(amount0, amount1, currency0, currency1)`` ordered by
        ascending token address.

        Matches the V4 PoolKey invariant ``currency0 < currency1`` and the
        symmetric logic in ``extract_lp_close_data``. Returns
        ``(None, None, None, None)`` when no transfers landed in PoolManager
        -- ``None`` is the honest "unmeasured" signal per blueprint 27
        §Empty ≠ Zero (callers must not substitute zero). On a single-sided
        deposit, currency1 is ``None`` (we know one address transferred but
        cannot infer the unobserved currency from transfers alone); the
        caller (``extract_lp_open_data``) resolves the missing leg via the
        gateway PoolKey lookup -- see VIB-4535.
        """
        deposited_by_token: dict[str, int] = {}
        for transfer in transfer_events:
            if transfer.to_address.lower() == self.pool_manager:
                token = transfer.token.lower()
                deposited_by_token[token] = deposited_by_token.get(token, 0) + transfer.amount

        if not deposited_by_token:
            return None, None, None, None

        sorted_tokens = sorted(deposited_by_token.keys())
        amount0 = deposited_by_token[sorted_tokens[0]]
        currency0 = sorted_tokens[0]
        amount1 = deposited_by_token[sorted_tokens[1]] if len(sorted_tokens) >= 2 else None
        currency1 = sorted_tokens[1] if len(sorted_tokens) >= 2 else None
        return amount0, amount1, currency0, currency1

    def _resolve_single_sided_lp_open(
        self,
        *,
        pool_id_hex: str,
        tx_hash: str,
        observed_currency: str,
        observed_amount: int,
    ) -> tuple[int, int, str, str] | None:
        """Resolve a single-sided LP_OPEN via the gateway PoolKey lookup.

        VIB-4535: when only one currency landed in PoolManager,
        ``extract_lp_open_data`` cannot honestly attribute it to currency0 vs
        currency1 from the observed transfers alone. This helper mirrors
        T07's close-side ``extract_lp_close_data`` lookup discipline:

        - Calls ``self._pool_key_lookup(pool_id_hex, chain)`` to get the
          canonical PoolKey.
        - On lookup failure (no callable / returns None / raises) emits a
          structured WARNING + telemetry and returns ``None`` (caller drops).
        - On native-ETH ``currency0`` raises ``UniswapV4UnsupportedPoolError``
          (defense-in-depth; adapter T06 already rejects at compile time;
          gemini-code-assist PR-review medium-priority concern).
        - On observed-currency-outside-PoolKey returns ``None`` with
          ``transfer_set_mismatch`` telemetry (caller drops).
        - On success returns ``(amount0, amount1, currency0, currency1)``
          where the missing leg is stamped as measured zero (``0``) per
          blueprint 27 §Empty != Zero — the lookup succeeded AND we observed
          all transfers from the PoolManager so the unobserved leg truly
          received zero.

        Returns:
            ``None`` to signal the caller should drop ``LPOpenData``, OR
            a resolved ``(amount0, amount1, currency0, currency1)`` tuple.

        Raises:
            ``UniswapV4UnsupportedPoolError``: on native-ETH currency0.
        """
        if self._pool_key_lookup is None:
            self._emit_drop_telemetry(
                outcome="drop",
                reason=V4LPDropReason.MISSING_POOL_KEY_LOOKUP,
                pool_id=pool_id_hex,
                tx_hash=tx_hash,
            )
            return None

        try:
            pool_key = self._pool_key_lookup(pool_id_hex, self.chain)
        except Exception as exc:
            self._emit_drop_telemetry(
                outcome="drop",
                reason=V4LPDropReason.POOL_KEY_LOOKUP_ERROR,
                pool_id=pool_id_hex,
                tx_hash=tx_hash,
                extras=f"error={type(exc).__name__}",
            )
            return None

        if pool_key is None:
            self._emit_drop_telemetry(
                outcome="drop",
                reason=V4LPDropReason.POOL_KEY_NOT_FOUND,
                pool_id=pool_id_hex,
                tx_hash=tx_hash,
            )
            return None

        pk_currency0 = pool_key.currency0.lower()
        pk_currency1 = pool_key.currency1.lower()

        # Native-ETH currency0 is out of V0 scope (VIB-4483 / P-V1-B). The
        # adapter compile-time guard (T06 / VIB-4471) already rejects native
        # ETH at compile time, so in normal flow no native-ETH receipt should
        # reach this branch. Defense-in-depth: if one ever does (e.g. a
        # non-PositionManager hook bypass), raise rather than silently
        # attribute measured-zero to the native-ETH leg (the native leg
        # emits no ERC-20 Transfer so the single observed transfer is always
        # the ERC-20 side; stamping `amount=0` on the ETH leg would be a
        # misattribution). Mirror of ``extract_lp_close_data``.
        from almanak.framework.connectors.uniswap_v4.adapter import UniswapV4UnsupportedPoolError
        from almanak.framework.connectors.uniswap_v4.sdk import NATIVE_CURRENCY

        if pk_currency0 == NATIVE_CURRENCY:
            self._emit_drop_telemetry(
                outcome="raise",
                reason=V4LPDropReason.NATIVE_CURRENCY_UNSUPPORTED,
                pool_id=pool_id_hex,
                tx_hash=tx_hash,
                extras=f"currency0={pool_key.currency0}",
            )
            raise UniswapV4UnsupportedPoolError(
                f"Uniswap V4 LP open has currency0={pool_key.currency0} (native ETH) but "
                f"native-ETH legs are not in V0 scope. V0 (VIB-4426) supports only ERC20-ERC20 "
                f"pools. Native-ETH currency support is tracked by VIB-4483 (P-V1-B). "
                f"pool_id={pool_id_hex} chain={self.chain}"
            )

        # The single observed currency MUST be one of the two PoolKey
        # currencies; otherwise attribution is impossible (mirror of the
        # close-side ``transfer_set_mismatch`` drop). Catches parser
        # mis-extraction or a stale cache returning the wrong PoolKey.
        if observed_currency not in (pk_currency0, pk_currency1):
            self._emit_drop_telemetry(
                outcome="drop",
                reason=V4LPDropReason.TRANSFER_SET_MISMATCH,
                pool_id=pool_id_hex,
                tx_hash=tx_hash,
                extras=f"expected={sorted([pk_currency0, pk_currency1])} observed=[{observed_currency}]",
            )
            return None

        # Map observed amount onto its correct leg; missing leg is measured
        # zero (Decimal("0") semantics; int field = 0). Empty != Zero only
        # applies when we don't know -- here the lookup succeeded AND we
        # observed all transfers so the unobserved currency truly received
        # zero in this open.
        if observed_currency == pk_currency0:
            return observed_amount, 0, pk_currency0, pk_currency1
        return 0, observed_amount, pk_currency0, pk_currency1

    def extract_lp_close_data(self, receipt: dict[str, Any]) -> LPCloseData | None:
        """Extract LP close data from a V4 burn receipt.

        VIB-4476 / V4 LP accounting V0. Token attribution is driven by the
        canonical ``PoolKey`` resolved via the gateway
        ``LookupV4PoolKey`` RPC (T03), NOT by sorting observed Transfer
        logs. Sorted-Transfer attribution is broken for (a) native ETH
        (which emits no ERC-20 Transfer) and (b) any non-trivial pair
        ordering where the on-chain ``currency0 < currency1`` invariant
        does not match the order the transfers happen to appear in.

        Walks the receipt for:

        1. ``ModifyLiquidity`` with ``liquidity_delta < 0`` (a burn, not a
           mint). Pull ``pool_id`` from ``topics[1]``.
        2. Canonical ``PoolKey`` for that ``pool_id`` via the injected
           ``pool_key_lookup`` callable.
        3. Native-ETH currency leg (``currency0 == 0x0`` after PoolKey's
           sorted-order normalisation) → raise
           :class:`UniswapV4UnsupportedPoolError` citing VIB-4483 (P-V1-B),
           consistent with the T06 adapter guard.
        4. Transfer-set integrity check: the set of token addresses in
           observed ``Transfer`` logs leaving the PoolManager MUST match
           ``{currency0, currency1}`` from the PoolKey. On mismatch:
           structured WARNING + return ``None`` (fail-loud over silent
           misattribution).
        5. PoolKey-ordered amount assignment: ``amount0_collected`` =
           sum of transfers of ``currency0``; ``amount1_collected`` =
           sum of transfers of ``currency1``.

        Emits:

        - ``pool_address`` = 32-byte canonical pool_id (66-char lowercase hex)
        - ``source = "modify_liquidity"``
        - ``fees0 = None``, ``fees1 = None`` — V4 bundles fees into the
          withdrawal Transfer in V0; explicit ``None`` is the honest signal
          (Empty ≠ Zero, blueprint 27). Separate fee measurement is V1
          P-V1-A (VIB-4482).

        Args:
            receipt: Transaction receipt dict with 'logs' field.

        Returns:
            ``LPCloseData`` with PoolKey-driven amount attribution, or
            ``None`` when no eligible burn is found, the PoolKey lookup
            fails, or the observed Transfer set does not match the PoolKey.

        Raises:
            UniswapV4UnsupportedPoolError: PoolKey has native-ETH
                ``currency0``. Lifting tracked by VIB-4483 (P-V1-B).
        """
        from almanak.framework.connectors.uniswap_v4.adapter import UniswapV4UnsupportedPoolError
        from almanak.framework.connectors.uniswap_v4.sdk import NATIVE_CURRENCY
        from almanak.framework.execution.extracted_data import LPCloseData

        parsed = self.parse_receipt(receipt)
        tx_hash = receipt.get("transactionHash", "unknown")

        burn_event: ModifyLiquidityEventData | None = None
        for event in parsed.modify_liquidity_events:
            if event.liquidity_delta < 0:
                burn_event = event
                break
        if burn_event is None:
            return None

        liquidity_removed = abs(burn_event.liquidity_delta)
        pool_id_hex = burn_event.pool_id.lower()

        if self._pool_key_lookup is None:
            self._emit_drop_telemetry(
                outcome="drop",
                reason=V4LPDropReason.MISSING_POOL_KEY_LOOKUP,
                pool_id=pool_id_hex,
                tx_hash=tx_hash,
            )
            return None

        try:
            pool_key = self._pool_key_lookup(pool_id_hex, self.chain)
        except Exception as exc:
            self._emit_drop_telemetry(
                outcome="drop",
                reason=V4LPDropReason.POOL_KEY_LOOKUP_ERROR,
                pool_id=pool_id_hex,
                tx_hash=tx_hash,
                extras=f"error={type(exc).__name__}",
            )
            return None

        if pool_key is None:
            self._emit_drop_telemetry(
                outcome="drop",
                reason=V4LPDropReason.POOL_KEY_NOT_FOUND,
                pool_id=pool_id_hex,
                tx_hash=tx_hash,
            )
            return None

        currency0 = pool_key.currency0.lower()
        currency1 = pool_key.currency1.lower()

        if currency0 == NATIVE_CURRENCY:
            self._emit_drop_telemetry(
                outcome="raise",
                reason=V4LPDropReason.NATIVE_CURRENCY_UNSUPPORTED,
                pool_id=pool_id_hex,
                tx_hash=tx_hash,
                extras=f"currency0={pool_key.currency0}",
            )
            raise UniswapV4UnsupportedPoolError(
                f"Uniswap V4 LP close has currency0={pool_key.currency0} (native ETH) but "
                f"native-ETH legs are not in V0 scope. V0 (VIB-4426) supports only ERC20-ERC20 "
                f"pools. Native-ETH currency support is tracked by VIB-4483 (P-V1-B). "
                f"pool_id={pool_id_hex} chain={self.chain}"
            )

        collected_by_token: dict[str, int] = {}
        for transfer in parsed.transfer_events:
            if transfer.from_address.lower() == self.pool_manager:
                token = transfer.token.lower()
                collected_by_token[token] = collected_by_token.get(token, 0) + transfer.amount

        observed_tokens = set(collected_by_token.keys())
        expected_tokens = {currency0, currency1}
        # VIB-4426 P1 #3 — allow legitimate single-sided closes. A
        # concentrated-liquidity position that is out of range at burn time
        # legitimately returns only one of {currency0, currency1}; the
        # missing leg is a measured zero, not a "transfer-set mismatch".
        # Pre-fix the strict equality check dropped these as
        # ``transfer_set_mismatch`` and the LP_CLOSE accounting event was
        # silently lost.
        #
        # The drop predicate is now: observed tokens must be a non-empty
        # SUBSET of expected. An observation outside the PoolKey currency
        # set IS a real attribution error (could be a token the parser
        # mis-extracted) and stays as a drop.
        if not observed_tokens or not observed_tokens.issubset(expected_tokens):
            self._emit_drop_telemetry(
                outcome="drop",
                reason=V4LPDropReason.TRANSFER_SET_MISMATCH,
                pool_id=pool_id_hex,
                tx_hash=tx_hash,
                extras=f"expected={sorted(expected_tokens)} observed={sorted(observed_tokens)}",
            )
            return None

        # Missing leg = measured zero (Empty ≠ Zero only applies when we
        # don't know; here the PoolKey lookup succeeded AND we observed all
        # transfers from the PoolManager so a non-observed currency truly
        # received zero in this burn).
        amount0_collected = collected_by_token.get(currency0, 0)
        amount1_collected = collected_by_token.get(currency1, 0)

        return LPCloseData(
            amount0_collected=amount0_collected,
            amount1_collected=amount1_collected,
            # VIB-4470 / VIB-4476 — V4 currently bundles fees into the
            # withdrawal Transfer; fee separation is V1 P-V1-A (VIB-4482).
            # Explicit ``None`` is the honest signal (Empty ≠ Zero).
            fees0=None,
            fees1=None,
            liquidity_removed=liquidity_removed,
            pool_address=pool_id_hex,
            source="modify_liquidity",
            # VIB-4426 P1 #4 — emit canonical PoolKey-sorted currency
            # addresses so the LP handler can resolve symbols/decimals by
            # address (not by user-intent index). Without these the
            # handler would mis-pair amount0 (in PoolKey order) with the
            # intent's token0 (in user-supplied order).
            currency0=currency0,
            currency1=currency1,
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
