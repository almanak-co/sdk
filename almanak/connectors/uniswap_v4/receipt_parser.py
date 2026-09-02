"""Parse Uniswap V4 PoolManager and PositionManager receipt events.

``Swap`` indexes ``PoolId`` and ``sender``; its data words are signed
``amount0``/``amount1`` followed by ``sqrtPriceX96``, liquidity, tick, and fee.
``ModifyLiquidity`` has the same indexed fields and data words for tick bounds,
signed liquidity delta, and salt. The standard Transfer topic represents both
ERC-20 transfers (three topics) and PositionManager ERC-721 transfers (four).
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import TYPE_CHECKING, Any

from almanak.connectors._strategy_base.base import HexDecoder, resolve_swap_token_symbol
from almanak.framework.data.tokens import (
    TokenResolutionError,
    build_swap_token_meta_extract_kwargs,
    build_token_meta_hint_map,
    resolve_token_decimals,
)
from almanak.framework.observability.metrics import (
    V4LPDropOutcome,
    V4LPDropReason,
    record_v4_lp_parser_drop,
)

from .addresses import UNISWAP_V4

if TYPE_CHECKING:
    from almanak.connectors.uniswap_v4.sdk import PoolKey
    from almanak.framework.execution.extracted_data import LPCloseData, LPOpenData, SwapAmounts

# Synchronous injection keeps network I/O outside the parser while resolving a
# ModifyLiquidity pool ID to its canonical currency0 < currency1 PoolKey.
PoolKeyLookup = Callable[[str, str], "PoolKey | None"]

logger = logging.getLogger(__name__)


EVENT_TOPICS: dict[str, str] = {
    # Swap(bytes32,address,int128,int128,uint160,uint128,int24,uint24)
    "Swap": "0x40e9cecb9f5f1f1c5b9c97dec2917b7ee92e57ba5563708daca94dd84ad7112f",
    # ModifyLiquidity(bytes32,address,int24,int24,int256,bytes32)
    "ModifyLiquidity": "0xf208f4912782fd25c7f114ca3723a2d5dd6f3bcc3ac8db5af63baa85f711d5ec",
    "Transfer": "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
    "Approval": "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925",
}

SWAP_EVENT_TOPIC = EVENT_TOPICS["Swap"]
MODIFY_LIQUIDITY_TOPIC = EVENT_TOPICS["ModifyLiquidity"]
TRANSFER_EVENT_TOPIC = EVENT_TOPICS["Transfer"]


class UniswapV4EventType(Enum):
    SWAP = "SWAP"
    MODIFY_LIQUIDITY = "MODIFY_LIQUIDITY"
    TRANSFER = "TRANSFER"
    APPROVAL = "APPROVAL"
    UNKNOWN = "UNKNOWN"


@dataclass
class SwapEventData:
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
    pool_id: str
    sender: str
    tick_lower: int
    tick_upper: int
    liquidity_delta: int
    salt: str


@dataclass
class TransferEventData:
    token: str
    from_address: str
    to_address: str
    amount: int


@dataclass
class ParsedSwapResult:
    """High-level parsed swap result.

    Raw amounts are token base units. Decimal fields intentionally remain
    ``Decimal`` and use ``Decimal(0)`` when decimals are unresolved; the
    companion ``*_decimal_resolved`` flags distinguish that sentinel from a
    measured zero.
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
    swap_events: list[SwapEventData] = field(default_factory=list)
    modify_liquidity_events: list[ModifyLiquidityEventData] = field(default_factory=list)
    transfer_events: list[TransferEventData] = field(default_factory=list)
    swap_result: ParsedSwapResult | None = None
    error: str | None = None


class UniswapV4ReceiptParser:
    """Extract swaps, liquidity changes, transfers, and accounting identities."""

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

        chain_addrs = UNISWAP_V4.get(self.chain, {})
        if pool_manager_address:
            self.pool_manager = pool_manager_address.lower()
        else:
            self.pool_manager = chain_addrs.get("pool_manager", "").lower()

        if position_manager_address:
            self.position_manager = position_manager_address.lower()
        else:
            self.position_manager = chain_addrs.get("position_manager", "").lower()

        # Direction inference must recognize every swap rail, not just the
        # PoolManager: flash accounting and wrap/unwrap paths cross the router,
        # Permit2, PositionManager, or wrapped-native contract. The later pass
        # ignores infra-to-infra hops because they do not identify the user side.
        from almanak.connectors.uniswap_v4.sdk import PERMIT2_ADDRESS
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
        """Record every LP parse drop through one log/metric contract.

        ``drop`` denotes a ``None`` result and ``raise`` a typed failure. The
        metric is incremented before any caller raises.
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
        *,
        swap_token_meta: dict[str, dict[str, Any]] | None = None,
    ) -> ParseResult:
        """Decode supported events and build a swap summary when present.

        Compiler token metadata supplies decimal hints when resolution fails.
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

        if result.swap_events:
            result.swap_result = self._build_swap_result(
                result.swap_events,
                result.transfer_events,
                quoted_amount_out,
                swap_token_meta=swap_token_meta,
            )

        return result

    def extract_swap_amounts(
        self,
        receipt: dict[str, Any],
        *,
        expected_out: Decimal | None = None,
        swap_token_meta: dict[str, dict[str, Any]] | None = None,
    ) -> SwapAmounts | None:
        """Extract swap amounts for ResultEnricher integration.

        ``expected_out`` is a human-unit pre-slippage quote. Token metadata has
        ``token_in``/``token_out`` entries containing address, symbol, and
        decimals and is used when the resolver misses.
        """
        from almanak.framework.execution.extracted_data import SwapAmounts

        parsed = self.parse_receipt(receipt, swap_token_meta=swap_token_meta)
        if not parsed.swap_result:
            return None

        sr = parsed.swap_result

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
            # Ledger and FIFO identity use canonical symbols, not addresses.
            token_in=resolve_swap_token_symbol(sr.token_in, self.chain),
            token_out=resolve_swap_token_symbol(sr.token_out, self.chain),
            amount_in_decimal_resolved=sr.amount_in_decimal_resolved,
            amount_out_decimal_resolved=sr.amount_out_decimal_resolved,
        )

    def extract_position_id(self, receipt: dict[str, Any]) -> int | None:
        """Extract LP position NFT tokenId from ERC-721 Transfer event.

        Prefer a zero-address mint from the configured PositionManager. A sole
        mint from another known V4 PositionManager is an address-mismatch
        fallback; unknown or multiple fallback emitters fail closed.
        """
        logs = receipt.get("logs", [])
        tx_hash = receipt.get("transactionHash", "unknown")

        known_pm_addresses = {
            addrs["position_manager"].lower() for addrs in UNISWAP_V4.values() if addrs.get("position_manager")
        }

        fallback_candidates: list[tuple[int, str]] = []
        # Missing mints are normal on approval transactions; unmatched mints are not.
        erc721_mint_count = 0

        for log in logs:
            topics = log.get("topics", [])
            if len(topics) < 4:
                continue

            topic0 = topics[0].lower() if isinstance(topics[0], str) else hex(topics[0])
            if topic0 != TRANSFER_EVENT_TOPIC.lower():
                continue

            # ERC-721 Transfer uses topics for from, to, and tokenId; ERC-20 has only three topics.
            from_addr = topics[1] if isinstance(topics[1], str) else hex(topics[1])

            try:
                if int(from_addr, 16) != 0:
                    continue
            except (ValueError, TypeError):
                continue

            erc721_mint_count += 1

            token_id_hex = topics[3] if isinstance(topics[3], str) else hex(topics[3])
            try:
                token_id = int(token_id_hex, 16)
            except (ValueError, TypeError):
                continue

            log_address = log.get("address", "")
            log_address_lower = log_address.lower() if isinstance(log_address, str) else ""
            if self.position_manager and log_address_lower == self.position_manager:
                return token_id

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

        log_at = logger.warning if erc721_mint_count > 0 else logger.debug
        log_at(
            "V4 extract_position_id: no position ID found. "
            "total_logs=%d, erc721_mint_events=%d, position_manager=%s, chain=%s, tx=%s",
            len(logs),
            erc721_mint_count,
            self.position_manager,
            self.chain,
            tx_hash,
        )
        return None

    def extract_liquidity(self, receipt: dict[str, Any]) -> int | None:
        """Return the first positive ModifyLiquidity delta."""
        parsed = self.parse_receipt(receipt)
        if not parsed.modify_liquidity_events:
            return None

        for event in parsed.modify_liquidity_events:
            if event.liquidity_delta > 0:
                return event.liquidity_delta

        return None

    def extract_lp_open_data(self, receipt: dict[str, Any]) -> LPOpenData | None:
        """Extract LP open data from a V4 mint receipt.

        The first positive ModifyLiquidity must come from a known
        PositionManager; hook- or router-initiated mints are rejected. Its salt
        must equal ``bytes32(tokenId)`` from the PositionManager ERC-721 mint.
        The exact v4-core identity is
        ``keccak(packed(positionManager, tickLower, tickUpper, salt))``.

        ERC-20 deposits are raw base units summed by token and assigned in
        ``currency0 < currency1`` order. A single observed currency requires a
        canonical PoolKey lookup; lookup failure or a token outside that key
        drops the result rather than guessing. An absent ERC-20 leg is measured
        zero, but native currency is ``None`` because ``msg.value`` emits no
        Transfer. With no transfers and no lookup, the legacy all-``None``
        shape remains fail-open for callers that use intent token order. The
        first same-pool Swap supplies ``current_tick``.
        """
        from almanak.connectors.uniswap_v4.hooks import compute_position_hash
        from almanak.connectors.uniswap_v4.sdk import POSITION_MANAGER_ADDRESS_SET
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
        elif amount0 is None and amount1 is None and self._pool_key_lookup is not None:
            resolved = self._resolve_native_only_lp_open(
                pool_id_hex=mint_event.pool_id.lower(),
                tx_hash=tx_hash,
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
            currency0=currency0,
            currency1=currency1,
        )

    def _sum_deposit_transfers_by_currency_order(
        self, transfer_events: list[TransferEventData]
    ) -> tuple[int | None, int | None, str | None, str | None]:
        """Sum PoolManager deposits in address order.

        No transfers returns all ``None``. One token cannot reveal which
        PoolKey leg is absent, so the caller must resolve the key.
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
    ) -> tuple[int | None, int | None, str, str] | None:
        """Resolve a single-sided LP_OPEN via the gateway PoolKey lookup.

        Missing, failed, or inconsistent lookup is fail-closed. An unobserved
        ERC-20 leg is measured ``0``; an unobserved address-zero native leg is
        ``None`` because its ``msg.value`` deposit is invisible to Transfer
        logs. The runner first uses post-mint position state, then a block-pinned
        wallet/Safe native-balance bracket; total failure remains unmeasured.
        """
        from almanak.connectors.uniswap_v4.sdk import NATIVE_CURRENCY

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

        if observed_currency not in (pk_currency0, pk_currency1):
            self._emit_drop_telemetry(
                outcome="drop",
                reason=V4LPDropReason.TRANSFER_SET_MISMATCH,
                pool_id=pool_id_hex,
                tx_hash=tx_hash,
                extras=f"expected={sorted([pk_currency0, pk_currency1])} observed=[{observed_currency}]",
            )
            return None

        def _unobserved(currency: str) -> int | None:
            return None if currency == NATIVE_CURRENCY else 0

        if observed_currency == pk_currency0:
            return observed_amount, _unobserved(pk_currency1), pk_currency0, pk_currency1
        return _unobserved(pk_currency0), observed_amount, pk_currency0, pk_currency1

    def _resolve_native_only_lp_open(
        self,
        *,
        pool_id_hex: str,
        tx_hash: str,
    ) -> tuple[int | None, int | None, str, str] | None:
        """Resolve a fully-native single-sided LP_OPEN via the gateway PoolKey lookup.

        Zero observed transfers is attributable only when the PoolKey contains
        address-zero native currency. The native leg is unmeasured ``None`` and
        the ERC-20 leg measured ``0``; an all-ERC-20 key fails closed.
        """
        from almanak.connectors.uniswap_v4.sdk import NATIVE_CURRENCY

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

        if NATIVE_CURRENCY not in (pk_currency0, pk_currency1):
            self._emit_drop_telemetry(
                outcome="drop",
                reason=V4LPDropReason.TRANSFER_SET_MISMATCH,
                pool_id=pool_id_hex,
                tx_hash=tx_hash,
                extras=f"expected={sorted([pk_currency0, pk_currency1])} observed=[]",
            )
            return None

        def _unobserved(currency: str) -> int | None:
            return None if currency == NATIVE_CURRENCY else 0

        return _unobserved(pk_currency0), _unobserved(pk_currency1), pk_currency0, pk_currency1

    def extract_lp_close_data(self, receipt: dict[str, Any]) -> LPCloseData | None:
        """Extract LP close data from a V4 burn receipt.

        The first negative ModifyLiquidity supplies the pool ID and removed
        liquidity. Raw base-unit withdrawals are summed only from Transfers
        leaving PoolManager and assigned by the looked-up PoolKey, never by log
        order. Lookup failure or any observed token outside the key fails
        closed; a missing ERC-20 key leg is measured zero.

        Address-zero native currency returns raw ETH without an ERC-20 event,
        so its principal is unmeasured ``None`` and is filled from pre-burn
        position state. An empty observed set is valid only for a native pool;
        it remains an attribution failure for an all-ERC-20 pool. V4 does not
        separate fees from withdrawal Transfers here, so ``fees0``/``fees1``
        remain ``None`` and the runner measures fees separately before burning.
        """
        from almanak.framework.execution.extracted_data import LPCloseData

        parsed = self.parse_receipt(receipt)
        tx_hash = receipt.get("transactionHash", "unknown")

        burn_event: ModifyLiquidityEventData | None = None
        for event in parsed.modify_liquidity_events:
            if event.liquidity_delta < 0:
                burn_event = event
                break
        if burn_event is None:
            # Fee collection emits zero-delta ModifyLiquidity plus TAKE_PAIR;
            # its indexed pool ID is sufficient without a PoolKey lookup.
            return self._extract_fees_only_collect_data(parsed)

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

        collected_by_token: dict[str, int] = {}
        for transfer in parsed.transfer_events:
            if transfer.from_address.lower() == self.pool_manager:
                token = transfer.token.lower()
                collected_by_token[token] = collected_by_token.get(token, 0) + transfer.amount

        from almanak.connectors.uniswap_v4.sdk import NATIVE_CURRENCY

        observed_tokens = set(collected_by_token.keys())
        expected_tokens = {currency0, currency1}
        pool_has_native_leg = NATIVE_CURRENCY in expected_tokens
        empty_observed_drop = not observed_tokens and not pool_has_native_leg
        if empty_observed_drop or not observed_tokens.issubset(expected_tokens):
            self._emit_drop_telemetry(
                outcome="drop",
                reason=V4LPDropReason.TRANSFER_SET_MISMATCH,
                pool_id=pool_id_hex,
                tx_hash=tx_hash,
                extras=f"expected={sorted(expected_tokens)} observed={sorted(observed_tokens)}",
            )
            return None

        def _leg(currency: str) -> int | None:
            if currency in collected_by_token:
                return collected_by_token[currency]
            return None if currency == NATIVE_CURRENCY else 0

        amount0_collected = _leg(currency0)
        amount1_collected = _leg(currency1)

        return LPCloseData(
            amount0_collected=amount0_collected,
            amount1_collected=amount1_collected,
            fees0=None,
            fees1=None,
            liquidity_removed=liquidity_removed,
            pool_address=pool_id_hex,
            source="modify_liquidity",
            currency0=currency0,
            currency1=currency1,
        )

    def _extract_fees_only_collect_data(self, parsed: ParseResult) -> LPCloseData | None:
        """Build ``LPCloseData`` for a fees-only V4 LP_COLLECT_FEES receipt.

        ``DECREASE_LIQUIDITY(0) + TAKE_PAIR`` emits zero-delta
        ModifyLiquidity, whose indexed pool ID is chain truth and needs no
        lookup. Principal and removed liquidity are measured zero; bundled
        fees and unresolved currencies are ``None``. No zero-delta event means
        no recognizable collect rather than a parse error.
        """
        from almanak.framework.execution.extracted_data import LPCloseData

        collect_event: ModifyLiquidityEventData | None = None
        for event in parsed.modify_liquidity_events:
            if event.liquidity_delta == 0:
                collect_event = event
                break
        if collect_event is None:
            return None

        return LPCloseData(
            amount0_collected=0,
            amount1_collected=0,
            fees0=None,
            fees1=None,
            liquidity_removed=0,
            pool_address=collect_event.pool_id.lower(),
            source="modify_liquidity",
            currency0=None,
            currency1=None,
        )

    def extract_registry_payload_open(
        self,
        receipt: dict[str, Any],
        *,
        fee_tier: int | None = None,
    ) -> dict[str, Any] | None:
        """Build an LP_OPEN registry payload from canonical V4 identity.

        Physical identity requires tokenId and the chain's PositionManager;
        the 32-byte pool ID is the semantic grouping key. Missing fields fail
        closed rather than using zero or fabricated values. Fee tier is optional
        metadata, not identity.
        """
        lp_data = self.extract_lp_open_data(receipt)
        if lp_data is None:
            return None
        if lp_data.position_id is None or lp_data.position_id <= 0:
            return None
        if not lp_data.pool_address:
            return None

        position_manager = self.position_manager or None
        if not position_manager:
            return None

        payload: dict[str, Any] = {
            "token_id": str(lp_data.position_id),
            "pool_id": lp_data.pool_address.lower(),
            "position_manager": position_manager,
            "tick_lower": lp_data.tick_lower,
            "tick_upper": lp_data.tick_upper,
            "liquidity": str(lp_data.liquidity) if lp_data.liquidity is not None else None,
            "amount0": str(lp_data.amount0) if lp_data.amount0 is not None else None,
            "amount1": str(lp_data.amount1) if lp_data.amount1 is not None else None,
            "currency0": lp_data.currency0,
            "currency1": lp_data.currency1,
        }
        if fee_tier is not None and fee_tier > 0:
            payload["fee_tier"] = int(fee_tier)
        return payload

    def extract_registry_payload_close(
        self,
        receipt: dict[str, Any],
        *,
        open_payload: dict[str, Any] | None = None,
        fee_tier: int | None = None,
    ) -> dict[str, Any] | None:
        """Build an LP_CLOSE registry payload using its matched OPEN identity.

        Close receipts do not re-emit the NFT tokenId, so it must come from the
        OPEN payload. Missing tokenId fails closed, and a supplied OPEN pool ID
        must match the close receipt before identity fields are merged.
        """
        lp_close = self.extract_lp_close_data(receipt)
        if lp_close is None:
            return None
        pool_id = (lp_close.pool_address or "").lower()
        if not pool_id:
            return None

        token_id = self._open_payload_token_id_int(open_payload) if open_payload else None
        if token_id is None or token_id <= 0:
            return None

        open_pool = str((open_payload or {}).get("pool_id") or "").lower()
        if open_pool and open_pool != pool_id:
            return None

        position_manager = self.position_manager or str((open_payload or {}).get("position_manager") or "") or None
        if not position_manager:
            return None

        payload: dict[str, Any] = {
            "token_id": str(token_id),
            "pool_id": pool_id,
            "position_manager": position_manager,
            # Preserve unmeasured native principal as JSON null, never "None".
            "amount0_close": (str(lp_close.amount0_collected) if lp_close.amount0_collected is not None else None),
            "amount1_close": (str(lp_close.amount1_collected) if lp_close.amount1_collected is not None else None),
            "fee_owed_0": str(lp_close.fees0) if lp_close.fees0 is not None else None,
            "fee_owed_1": str(lp_close.fees1) if lp_close.fees1 is not None else None,
            "currency0": lp_close.currency0,
            "currency1": lp_close.currency1,
        }
        if lp_close.liquidity_removed is not None:
            payload["liquidity"] = str(lp_close.liquidity_removed)
        self._merge_open_payload_fields_v4(payload, open_payload, fee_tier=fee_tier)
        return payload

    @staticmethod
    def _open_payload_token_id_int(open_payload: dict[str, Any]) -> int | None:
        """Coerce a non-empty OPEN tokenId to int."""
        raw = open_payload.get("token_id")
        if raw is None or raw == "":
            return None
        try:
            return int(raw)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _merge_open_payload_fields_v4(
        payload: dict[str, Any],
        open_payload: dict[str, Any] | None,
        *,
        fee_tier: int | None,
    ) -> None:
        """Merge fields that a close receipt cannot re-derive.

        OPEN-time liquidity wins over burned liquidity for the registry row.
        """
        if open_payload is None:
            if fee_tier is not None and fee_tier > 0:
                payload.setdefault("fee_tier", int(fee_tier))
            return
        for key in ("tick_lower", "tick_upper"):
            if open_payload.get(key) is not None and key not in payload:
                payload[key] = open_payload[key]
        if open_payload.get("amount0") is not None:
            payload.setdefault("amount0_open", open_payload["amount0"])
        if open_payload.get("amount1") is not None:
            payload.setdefault("amount1_open", open_payload["amount1"])
        if open_payload.get("liquidity") is not None:
            payload["liquidity"] = open_payload["liquidity"]
        if open_payload.get("fee_tier") is not None:
            payload.setdefault("fee_tier", open_payload["fee_tier"])
        elif fee_tier is not None and fee_tier > 0:
            payload.setdefault("fee_tier", int(fee_tier))

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

            # Indexed pool ID and sender are followed by four 32-byte ABI words.
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

            # Signed int128 deltas are sign-extended to full 32-byte ABI words.
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

    @staticmethod
    def _compute_swap_amounts(swap: SwapEventData) -> tuple[int, int]:
        """Derive (amount_in, amount_out) from a V4 Swap event.

        V4 sign convention (swapper's perspective):
            positive = tokens RECEIVED by the swapper from the pool
            negative = tokens PAID by the swapper to the pool
        """
        if swap.amount0 > 0:
            amount_in = abs(swap.amount1)
            amount_out = swap.amount0
        else:
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
        """Match routed flash-accounting transfers by raw amount.

        Excluding the token already assigned to the opposite side preserves
        distinct tokens when equal-decimal pairs have equal raw amounts.
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
        """Infer token sides from transfers crossing an infrastructure boundary.

        Wrap/unwrap can make ERC-20 amounts differ from Swap deltas. A transfer
        from infrastructure is output and one to infrastructure is input;
        infra-to-infra hops provide no user direction. If no evidence remains,
        lowercase address order supplies a deterministic, warned guess rather
        than making attribution depend on log order.
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
            if from_is_infra == to_is_infra:
                continue
            if token_out_addr is None and from_is_infra:
                token_out_addr = transfer.token
                seen_tokens.add(token_lower)
            elif token_in_addr is None and to_is_infra:
                token_in_addr = transfer.token
                seen_tokens.add(token_lower)

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
        """Identify token sides by PoolManager, amount, then direction evidence."""
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

        The global resolver is loaded lazily when none was injected. Each side
        independently returns ``None`` on failure.
        """
        token_in_decimals: int | None = None
        token_out_decimals: int | None = None
        if token_in_addr:
            try:
                if self._token_resolver is None:
                    token_in_decimals = resolve_token_decimals(token_in_addr, self.chain)
                else:
                    token_in_decimals = resolve_token_decimals(token_in_addr, self.chain, resolver=self._token_resolver)
            except TokenResolutionError:
                logger.warning(
                    "Could not resolve decimals for token_in %s — decimal amounts will be zero",
                    token_in_addr,
                )
        if token_out_addr:
            try:
                if self._token_resolver is None:
                    token_out_decimals = resolve_token_decimals(token_out_addr, self.chain)
                else:
                    token_out_decimals = resolve_token_decimals(
                        token_out_addr, self.chain, resolver=self._token_resolver
                    )
            except TokenResolutionError:
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
        """Convert base units and compute output-per-input effective price.

        Unresolved decimals produce ``None`` per side. Price requires both
        resolved, positive human-unit amounts so cross-decimal pairs never mix
        base units with decimal units.
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

    def build_extract_kwargs(
        self,
        *,
        field: str,
        bundle_metadata: dict[str, Any],
    ) -> dict[str, Any]:
        """Return canonical typed swap metadata for receipt extraction."""
        return build_swap_token_meta_extract_kwargs(field=field, bundle_metadata=bundle_metadata, chain=self.chain)

    @staticmethod
    def _build_hint_map(
        swap_token_meta: dict[str, dict[str, Any]] | None,
    ) -> dict[str, tuple[str, int]]:
        """Map compiler token metadata to ``{address: (symbol, decimals)}``."""
        return {address.lower(): value for address, value in build_token_meta_hint_map(swap_token_meta).items()}

    @staticmethod
    def _apply_token_meta_addresses(
        token_in_addr: str | None,
        token_out_addr: str | None,
        swap_token_meta: dict[str, dict[str, Any]] | None,
        single_swap: bool,
    ) -> tuple[str | None, str | None]:
        """Fill None sides from compiler hint slots (single-swap only).

        In a multi-hop receipt the first event's output is intermediate, so
        intent-level endpoints cannot safely fill its sides. Existing addresses
        are never overwritten.
        """
        if not single_swap or not swap_token_meta:
            return token_in_addr, token_out_addr
        if not token_in_addr:
            in_slot = swap_token_meta.get("token_in")
            if isinstance(in_slot, dict) and in_slot.get("address"):
                token_in_addr = str(in_slot["address"]).lower()
        if not token_out_addr:
            out_slot = swap_token_meta.get("token_out")
            if isinstance(out_slot, dict) and out_slot.get("address"):
                token_out_addr = str(out_slot["address"]).lower()
        return token_in_addr, token_out_addr

    def _resolve_token_decimals_with_hints(
        self,
        token_in_addr: str | None,
        token_out_addr: str | None,
        hint_by_addr: dict[str, tuple[str, int]],
    ) -> tuple[int | None, int | None]:
        """Resolve decimals per side, with matching compiler hints authoritative."""
        token_in_decimals: int | None = None
        token_out_decimals: int | None = None
        if token_in_addr:
            try:
                if self._token_resolver is None:
                    token_in_decimals = resolve_token_decimals(token_in_addr, self.chain, hints=hint_by_addr)
                else:
                    token_in_decimals = resolve_token_decimals(
                        token_in_addr,
                        self.chain,
                        hints=hint_by_addr,
                        resolver=self._token_resolver,
                    )
            except TokenResolutionError:
                pass
        if token_out_addr:
            try:
                if self._token_resolver is None:
                    token_out_decimals = resolve_token_decimals(token_out_addr, self.chain, hints=hint_by_addr)
                else:
                    token_out_decimals = resolve_token_decimals(
                        token_out_addr,
                        self.chain,
                        hints=hint_by_addr,
                        resolver=self._token_resolver,
                    )
            except TokenResolutionError:
                pass
        return token_in_decimals, token_out_decimals

    def _build_swap_result(
        self,
        swap_events: list[SwapEventData],
        transfer_events: list[TransferEventData],
        quoted_amount_out: int | None,
        swap_token_meta: dict[str, dict[str, Any]] | None = None,
    ) -> ParsedSwapResult:
        """Build a high-level swap result from decoded events.

        Receipt order is significant: the first Swap supplies amounts and
        post-swap state. Decimal conversion uses ``None`` internally for
        unresolved sides, then preserves the public ``Decimal(0)`` sentinel and
        records the distinction in ``*_decimal_resolved``.
        """
        swap = swap_events[0]
        amount_in, amount_out = self._compute_swap_amounts(swap)
        slippage_bps = self._calculate_slippage_bps(amount_out, quoted_amount_out)
        token_in_addr, token_out_addr = self._identify_swap_tokens(transfer_events, amount_in, amount_out)
        token_in_addr, token_out_addr = self._apply_token_meta_addresses(
            token_in_addr, token_out_addr, swap_token_meta, single_swap=len(swap_events) == 1
        )
        hint_by_addr = self._build_hint_map(swap_token_meta)
        token_in_decimals, token_out_decimals = self._resolve_token_decimals_with_hints(
            token_in_addr, token_out_addr, hint_by_addr
        )
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
