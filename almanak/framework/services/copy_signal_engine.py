"""CopySignalEngine for receipt decoding and signal production.

Decodes LeaderEvents into CopySignals using the contract registry and
existing receipt parsers. Handles deduplication and age-based filtering.
Supports multi-action decoding: SWAP, LP, lending, and perps.
"""

from __future__ import annotations

import importlib
import logging
import time
import uuid
from collections.abc import Callable
from dataclasses import asdict, is_dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from almanak.framework.api.timeline import TimelineEvent, TimelineEventType, add_event
from almanak.framework.connectors.contract_registry import ContractInfo, ContractRegistry
from almanak.framework.services.copy_trading_models import (
    CopySignal,
    LeaderEvent,
    LendingPayload,
    LPPayload,
    PerpPayload,
    SwapPayload,
)

logger = logging.getLogger(__name__)

_SECONDS_PER_DAY = 86400


class CopySignalEngine:
    """Decodes LeaderEvents into CopySignals using registry and receipt parsers.

    Dispatches to action-specific extraction methods based on the
    supported_actions declared in the ContractRegistry entry.
    """

    def __init__(
        self,
        registry: ContractRegistry,
        max_age_seconds: int = 300,
        retention_days: int = 7,
        price_fn: Callable[[str, str], Decimal | None] | None = None,
        strategy_id: str = "",
        strict_token_resolution: bool = False,
    ) -> None:
        self._registry = registry
        self._max_age_seconds = max_age_seconds
        self._retention_days = retention_days
        self._price_fn = price_fn
        self._strategy_id = strategy_id
        self._strict_token_resolution = strict_token_resolution
        self._seen_event_ids: dict[str, int] = {}
        self._parser_cache: dict[str, Any] = {}

    def process_events(
        self,
        events: list[LeaderEvent],
        current_time: int | None = None,
        current_block: int | None = None,
    ) -> list[CopySignal]:
        """Process a batch of LeaderEvents and return decoded CopySignals.

        Args:
            events: Leader events to process.
            current_time: Override for current unix timestamp.
            current_block: Latest block number on the monitored chain. When provided,
                ``leader_lag_blocks`` is computed and added to each signal's metadata
                so that the policy engine's ``max_leader_lag_blocks`` check can fire.
        """
        if current_time is None:
            current_time = int(time.time())

        self.prune_seen(current_time)

        signals: list[CopySignal] = []
        for event in events:
            event_signals = self._process_single_event(event, current_time, current_block)
            signals.extend(event_signals)
        return signals

    def _process_single_event(
        self,
        event: LeaderEvent,
        current_time: int,
        current_block: int | None = None,
    ) -> list[CopySignal]:
        """Process a single LeaderEvent. Returns zero or more CopySignals."""
        event_id = event.event_id

        if event_id in self._seen_event_ids:
            logger.debug("Skipping duplicate event: %s", event_id)
            self._emit_skip_event(event, "duplicate")
            return []

        age_seconds = current_time - event.timestamp
        if age_seconds > self._max_age_seconds:
            logger.debug("Skipping stale event: %s (age=%ds)", event_id, age_seconds)
            self._emit_skip_event(event, "stale")
            return []

        info = self._registry.lookup(event.chain, event.to_address)
        if info is None:
            logger.debug("Skipping unknown protocol for address %s on %s", event.to_address, event.chain)
            self._emit_skip_event(event, "unknown_protocol")
            return []

        parser = self._get_parser(info.parser_module, info.parser_class_name, event.chain)
        if parser is None:
            self._emit_skip_event(event, "parser_load_failed")
            return []

        decoded = self._extract_signals(parser, info, event, current_time, current_block)
        if decoded:
            self._seen_event_ids[event_id] = current_time
            for signal in decoded:
                self._emit_signal_detected(signal)
            return decoded

        self._emit_skip_event(event, "decode_failed")
        return []

    def _extract_signals(
        self,
        parser: Any,
        info: ContractInfo,
        event: LeaderEvent,
        current_time: int,
        current_block: int | None = None,
    ) -> list[CopySignal]:
        """Extract one or more action signals from a parsed leader event."""
        actions = [a.upper() for a in info.supported_actions]
        signals: list[CopySignal] = []

        def safe_extract(fn: Callable[[], CopySignal | None]) -> CopySignal | None:
            try:
                return fn()
            except Exception:
                logger.debug("Signal extraction failed for %s", event.event_id, exc_info=True)
                return None

        def append_unique(sig: CopySignal | None) -> None:
            if sig is None:
                return
            if any(s.signal_id == sig.signal_id for s in signals):
                return
            signals.append(sig)

        # Local alias ensures stable capture in lambda closures below
        cb = current_block

        if "SWAP" in actions:
            append_unique(safe_extract(lambda: self._extract_swap(parser, info, event, current_time, cb)))

        lp_open_signal: CopySignal | None = None
        if "LP_OPEN" in actions:
            lp_open_signal = safe_extract(lambda: self._extract_lp_open(parser, info, event, current_time, cb))
            append_unique(lp_open_signal)

        if "LP_CLOSE" in actions and lp_open_signal is None:
            append_unique(safe_extract(lambda: self._extract_lp_close(parser, info, event, current_time, cb)))

        if "SUPPLY" in actions:
            append_unique(safe_extract(lambda: self._extract_lending(parser, info, event, current_time, "SUPPLY", cb)))

        if "WITHDRAW" in actions:
            append_unique(
                safe_extract(lambda: self._extract_lending(parser, info, event, current_time, "WITHDRAW", cb))
            )

        if "BORROW" in actions:
            append_unique(safe_extract(lambda: self._extract_lending(parser, info, event, current_time, "BORROW", cb)))

        if "REPAY" in actions:
            append_unique(safe_extract(lambda: self._extract_lending(parser, info, event, current_time, "REPAY", cb)))

        perp_open_signal: CopySignal | None = None
        if "PERP_OPEN" in actions:
            perp_open_signal = safe_extract(
                lambda: self._extract_perp(parser, info, event, current_time, "PERP_OPEN", cb)
            )
            append_unique(perp_open_signal)

        if "PERP_CLOSE" in actions and perp_open_signal is None:
            append_unique(safe_extract(lambda: self._extract_perp(parser, info, event, current_time, "PERP_CLOSE", cb)))

        # Backward-compat fallback for legacy registry entries
        if not actions:
            append_unique(safe_extract(lambda: self._extract_swap(parser, info, event, current_time, cb)))

        return signals

    def _extract_swap(
        self,
        parser: Any,
        info: ContractInfo,
        event: LeaderEvent,
        current_time: int,
        current_block: int | None = None,
    ) -> CopySignal | None:
        if not hasattr(parser, "extract_swap_amounts"):
            return None

        try:
            swap_amounts = parser.extract_swap_amounts(event.receipt)
        except Exception:
            logger.exception("Swap extraction failed for %s", event.event_id)
            return None

        if swap_amounts is None:
            return None

        token_in = self._get_field(swap_amounts, "token_in")
        token_out = self._get_field(swap_amounts, "token_out")
        amount_in = self._to_decimal(self._get_field(swap_amounts, "amount_in_decimal"))
        amount_out = self._to_decimal(self._get_field(swap_amounts, "amount_out_decimal"))

        if token_in is None or token_out is None or amount_in is None or amount_out is None:
            return None

        resolved_in, ok_in = self._resolve_symbol(str(token_in), event.chain)
        resolved_out, ok_out = self._resolve_symbol(str(token_out), event.chain)
        token_metadata_resolved = ok_in and ok_out

        amounts = {
            resolved_in: amount_in,
            resolved_out: amount_out,
        }
        amounts_usd = self._enrich_usd(amounts, event.chain)

        effective_price = self._to_decimal(self._get_field(swap_amounts, "effective_price"))
        slippage_bps_raw = self._get_field(swap_amounts, "slippage_bps")
        slippage_bps = int(slippage_bps_raw) if slippage_bps_raw is not None else None

        payload = SwapPayload(
            token_in=resolved_in,
            token_out=resolved_out,
            amount_in=amount_in,
            amount_out=amount_out,
            effective_price=effective_price,
            slippage_bps=slippage_bps,
        )

        metadata = {
            "effective_price": str(effective_price) if effective_price is not None else None,
            "slippage_bps": slippage_bps,
            "notional_usd": str(max((abs(v) for v in amounts_usd.values()), default=Decimal("0"))),
        }

        return self._build_signal(
            event=event,
            action_type="SWAP",
            protocol=info.protocol,
            tokens=[resolved_in, resolved_out],
            amounts=amounts,
            amounts_usd=amounts_usd,
            metadata=metadata,
            action_payload=payload,
            current_time=current_time,
            capability_flags=self._default_capability_flags(info, "SWAP", token_metadata_resolved),
            current_block=current_block,
        )

    def _extract_lp_open(
        self,
        parser: Any,
        info: ContractInfo,
        event: LeaderEvent,
        current_time: int,
        current_block: int | None = None,
    ) -> CopySignal | None:
        position_id = None
        if hasattr(parser, "extract_position_id"):
            try:
                position_id = parser.extract_position_id(event.receipt)
            except Exception:
                logger.debug("LP_OPEN position extraction failed", exc_info=True)
        if self._is_sentinel_value(position_id):
            position_id = None

        lp_minted = None
        if position_id is None and hasattr(parser, "extract_lp_minted"):
            try:
                lp_minted = parser.extract_lp_minted(event.receipt)
                if lp_minted is not None:
                    position_id = f"minted:{lp_minted}"
            except Exception:
                logger.debug("LP_OPEN minted extraction failed", exc_info=True)
        if self._is_sentinel_value(lp_minted):
            lp_minted = None
            position_id = None

        if position_id is None:
            return None

        liquidity = None
        if hasattr(parser, "extract_liquidity"):
            try:
                liquidity = parser.extract_liquidity(event.receipt)
            except Exception:
                logger.debug("LP_OPEN liquidity extraction failed", exc_info=True)

        tick_lower = None
        tick_upper = None
        if hasattr(parser, "extract_tick_lower"):
            try:
                tick_lower = parser.extract_tick_lower(event.receipt)
            except Exception:
                logger.debug("LP_OPEN tick lower extraction failed", exc_info=True)

        if hasattr(parser, "extract_tick_upper"):
            try:
                tick_upper = parser.extract_tick_upper(event.receipt)
            except Exception:
                logger.debug("LP_OPEN tick upper extraction failed", exc_info=True)

        payload = LPPayload(
            pool=event.to_address,
            position_id=str(position_id),
            range_lower=self._to_decimal(tick_lower),
            range_upper=self._to_decimal(tick_upper),
        )

        metadata: dict[str, Any] = {
            "position_id": position_id,
            "liquidity": str(liquidity) if liquidity is not None else None,
            "tick_lower": tick_lower,
            "tick_upper": tick_upper,
        }
        if lp_minted is not None:
            metadata["lp_minted"] = str(lp_minted)

        return self._build_signal(
            event=event,
            action_type="LP_OPEN",
            protocol=info.protocol,
            tokens=[],
            amounts={},
            amounts_usd={},
            metadata=metadata,
            action_payload=payload,
            current_time=current_time,
            capability_flags=self._default_capability_flags(info, "LP_OPEN", True),
            current_block=current_block,
        )

    def _extract_lp_close(
        self,
        parser: Any,
        info: ContractInfo,
        event: LeaderEvent,
        current_time: int,
        current_block: int | None = None,
    ) -> CopySignal | None:
        lp_close_data = None
        if hasattr(parser, "extract_lp_close_data"):
            try:
                lp_close_data = parser.extract_lp_close_data(event.receipt)
            except Exception:
                logger.debug("LP_CLOSE data extraction failed", exc_info=True)
        if self._is_sentinel_value(lp_close_data):
            lp_close_data = None

        lp_burned = None
        if lp_close_data is None and hasattr(parser, "extract_lp_burned"):
            try:
                lp_burned = parser.extract_lp_burned(event.receipt)
            except Exception:
                logger.debug("LP_CLOSE burned extraction failed", exc_info=True)
        if self._is_sentinel_value(lp_burned):
            lp_burned = None

        if lp_close_data is None and lp_burned is None:
            return None

        payload = LPPayload(
            pool=event.to_address,
            close_fraction=Decimal("1"),
            position_id=None,
        )

        metadata: dict[str, Any] = {}
        if lp_close_data is not None:
            metadata["lp_close_data"] = self._serialize_obj(lp_close_data)
        if lp_burned is not None:
            metadata["lp_burned"] = str(lp_burned)

        return self._build_signal(
            event=event,
            action_type="LP_CLOSE",
            protocol=info.protocol,
            tokens=[],
            amounts={},
            amounts_usd={},
            metadata=metadata,
            action_payload=payload,
            current_time=current_time,
            capability_flags=self._default_capability_flags(info, "LP_CLOSE", True),
            current_block=current_block,
        )

    def _extract_lending(
        self,
        parser: Any,
        info: ContractInfo,
        event: LeaderEvent,
        current_time: int,
        action_type: str,
        current_block: int | None = None,
    ) -> CopySignal | None:
        method_map = {
            "SUPPLY": "extract_supply_amount",
            "WITHDRAW": "extract_withdraw_amount",
            "BORROW": "extract_borrow_amount",
            "REPAY": "extract_repay_amount",
        }
        method_name = method_map[action_type]
        if not hasattr(parser, method_name):
            return None

        try:
            raw_amount = getattr(parser, method_name)(event.receipt)
        except Exception:
            logger.debug("Lending extraction failed: %s", method_name, exc_info=True)
            return None

        amount = self._to_decimal(raw_amount)
        if amount is None:
            return None

        payload = LendingPayload(
            token=None,
            amount=amount,
            collateral_token=None,
            borrow_token=None,
            market_id=event.to_address,
            use_as_collateral=True if action_type == "SUPPLY" else None,
        )

        metadata = {
            "raw_amount": str(raw_amount),
            "market_id": event.to_address,
            # Token-denominated approximation; roughly accurate for stablecoins.
            # True USD conversion requires a price lookup not available at signal extraction time.
            "notional_usd": str(abs(amount)),
        }

        return self._build_signal(
            event=event,
            action_type=action_type,
            protocol=info.protocol,
            tokens=[],
            amounts={},
            amounts_usd={},
            metadata=metadata,
            action_payload=payload,
            current_time=current_time,
            capability_flags=self._default_capability_flags(info, action_type, True),
            current_block=current_block,
        )

    def _extract_perp(
        self,
        parser: Any,
        info: ContractInfo,
        event: LeaderEvent,
        current_time: int,
        action_type: str,
        current_block: int | None = None,
    ) -> CopySignal | None:
        legacy_method = "extract_perp_open" if action_type == "PERP_OPEN" else "extract_perp_close"
        if hasattr(parser, legacy_method):
            try:
                legacy_data = getattr(parser, legacy_method)(event.receipt)
            except Exception:
                legacy_data = None
            if legacy_data is not None:
                payload = PerpPayload(
                    market=event.to_address,
                    collateral_token=None,
                    collateral_amount=None,
                    size_usd=self._to_decimal(self._get_field(legacy_data, "size")),
                    is_long=self._get_field(legacy_data, "is_long"),
                    leverage=self._to_decimal(self._get_field(legacy_data, "leverage")),
                    position_id=(
                        str(self._get_field(legacy_data, "position_id"))
                        if self._get_field(legacy_data, "position_id") is not None
                        else None
                    ),
                )

                return self._build_signal(
                    event=event,
                    action_type=action_type,
                    protocol=info.protocol,
                    tokens=[],
                    amounts={},
                    amounts_usd={},
                    metadata={"perp_data": self._serialize_obj(legacy_data)},
                    action_payload=payload,
                    current_time=current_time,
                    capability_flags=self._default_capability_flags(info, action_type, True),
                    current_block=current_block,
                )

        parse_result = None
        if hasattr(parser, "parse_receipt"):
            try:
                parse_result = parser.parse_receipt(event.receipt)
            except Exception:
                logger.debug("Perp parse_receipt failed", exc_info=True)

        position_data = None
        if parse_result is not None:
            if action_type == "PERP_OPEN" and getattr(parse_result, "position_increases", None):
                position_data = parse_result.position_increases[0]
            if action_type == "PERP_CLOSE" and getattr(parse_result, "position_decreases", None):
                position_data = parse_result.position_decreases[0]

        if position_data is None:
            # Fallback heuristic for older parser APIs
            if action_type == "PERP_OPEN" and hasattr(parser, "extract_entry_price"):
                entry_price = parser.extract_entry_price(event.receipt)
                if entry_price is None:
                    return None
            elif action_type == "PERP_CLOSE" and hasattr(parser, "extract_exit_price"):
                exit_price = parser.extract_exit_price(event.receipt)
                if exit_price is None:
                    return None
            else:
                return None

        market = self._get_field(position_data, "market") if position_data is not None else event.to_address
        collateral_token_raw = self._get_field(position_data, "collateral_token") if position_data is not None else None
        collateral_token = None
        token_metadata_resolved = True
        if collateral_token_raw is not None:
            collateral_token, token_metadata_resolved = self._resolve_symbol(str(collateral_token_raw), event.chain)

        size_usd = self._to_decimal(self._get_field(position_data, "size_delta_usd"))
        if size_usd is None:
            size_usd = self._to_decimal(self._get_field(position_data, "size_in_usd"))

        collateral_amount = self._to_decimal(self._get_field(position_data, "collateral_delta_amount"))
        if collateral_amount is None:
            collateral_amount = self._to_decimal(self._get_field(position_data, "collateral_amount"))

        is_long_raw = self._get_field(position_data, "is_long")
        is_long = bool(is_long_raw) if is_long_raw is not None else None

        payload = PerpPayload(
            market=str(market) if market is not None else event.to_address,
            collateral_token=collateral_token,
            collateral_amount=collateral_amount,
            size_usd=size_usd,
            is_long=is_long,
            leverage=self._to_decimal(self._get_field(position_data, "leverage")),
            position_id=(
                str(self._get_field(position_data, "key"))
                if self._get_field(position_data, "key") is not None
                else self._safe_extract_position_id(parser, event.receipt)
            ),
        )

        metadata = {
            "position": self._serialize_obj(position_data),
            "notional_usd": str(size_usd) if size_usd is not None else None,
        }

        amounts: dict[str, Decimal] = {}
        if collateral_token is not None and collateral_amount is not None:
            amounts[collateral_token] = collateral_amount

        amounts_usd = self._enrich_usd(amounts, event.chain)
        if size_usd is not None:
            amounts_usd["NOTIONAL_USD"] = size_usd

        return self._build_signal(
            event=event,
            action_type=action_type,
            protocol=info.protocol,
            tokens=[collateral_token] if collateral_token else [],
            amounts=amounts,
            amounts_usd=amounts_usd,
            metadata=metadata,
            action_payload=payload,
            current_time=current_time,
            capability_flags=self._default_capability_flags(info, action_type, token_metadata_resolved),
            current_block=current_block,
        )

    def _safe_extract_position_id(self, parser: Any, receipt: dict[str, Any]) -> str | None:
        if not hasattr(parser, "extract_position_id"):
            return None
        try:
            value = parser.extract_position_id(receipt)
            return str(value) if value is not None else None
        except Exception:
            return None

    def _build_signal(
        self,
        *,
        event: LeaderEvent,
        action_type: str,
        protocol: str,
        tokens: list[str],
        amounts: dict[str, Decimal],
        amounts_usd: dict[str, Decimal],
        metadata: dict[str, Any],
        action_payload: Any,
        current_time: int,
        capability_flags: dict[str, bool],
        current_block: int | None = None,
    ) -> CopySignal:
        signal_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{event.event_id}:{action_type}"))
        age_seconds = max(0, current_time - event.timestamp)

        # Keep both numeric and string notional forms for downstream tooling.
        if "notional_usd" not in metadata:
            metadata["notional_usd"] = str(max((abs(v) for v in amounts_usd.values()), default=Decimal("0")))

        # Compute leader lag in blocks so the policy engine's max_leader_lag_blocks check can fire
        if current_block is not None and event.block_number is not None:
            lag = current_block - event.block_number
            if lag >= 0:
                metadata["leader_lag_blocks"] = lag

        return CopySignal(
            event_id=event.event_id,
            signal_id=signal_id,
            action_type=action_type,
            protocol=protocol,
            chain=event.chain,
            tokens=tokens,
            amounts=amounts,
            amounts_usd=amounts_usd,
            metadata=metadata,
            leader_address=event.from_address,
            block_number=event.block_number,
            timestamp=event.timestamp,
            leader_tx_hash=event.tx_hash,
            leader_block=event.block_number,
            detected_at=current_time,
            age_seconds=age_seconds,
            action_payload=action_payload,
            capability_flags=capability_flags,
        )

    def _default_capability_flags(
        self,
        info: ContractInfo,
        action_type: str,
        token_metadata_resolved: bool,
    ) -> dict[str, bool]:
        return {
            "chain_supported": True,
            "protocol_supported": True,
            "action_supported": action_type.upper() in {a.upper() for a in info.supported_actions},
            "token_metadata_resolved": token_metadata_resolved,
        }

    def _enrich_usd(self, amounts: dict[str, Decimal], chain: str) -> dict[str, Decimal]:
        """Enrich token amounts with USD values using the price function."""
        amounts_usd: dict[str, Decimal] = {}
        if self._price_fn is not None:
            for symbol, amount in amounts.items():
                try:
                    price = self._price_fn(symbol, chain)
                except Exception:
                    price = None
                if price is not None:
                    amounts_usd[symbol] = amount * Decimal(str(price))
        return amounts_usd

    def prune_seen(self, current_time: int) -> None:
        """Remove entries from _seen_event_ids older than retention_days."""
        cutoff = current_time - (self._retention_days * _SECONDS_PER_DAY)
        stale_ids = [eid for eid, ts in self._seen_event_ids.items() if ts < cutoff]
        for eid in stale_ids:
            del self._seen_event_ids[eid]

    def get_skip_reason(self, event: LeaderEvent) -> str | None:
        """Return the reason an event would be skipped, or None if it would be processed."""
        if event.event_id in self._seen_event_ids:
            return "duplicate"

        current_time = int(time.time())
        if current_time - event.timestamp > self._max_age_seconds:
            return "stale"

        info = self._registry.lookup(event.chain, event.to_address)
        if info is None:
            return "unknown_protocol"

        return None

    def _get_parser(self, module_path: str, class_name: str, chain: str) -> Any | None:
        """Lazily import and cache a receipt parser instance."""
        cache_key = f"{module_path}.{class_name}:{chain}"
        if cache_key in self._parser_cache:
            return self._parser_cache[cache_key]

        try:
            mod = importlib.import_module(module_path)
            parser_cls = getattr(mod, class_name)
            parser = parser_cls(chain=chain)
            self._parser_cache[cache_key] = parser
            return parser
        except Exception:
            logger.exception("Failed to import parser %s.%s", module_path, class_name)
            return None

    def _emit_signal_detected(self, signal: CopySignal) -> None:
        """Emit a LEADER_SIGNAL_DETECTED timeline event."""
        try:
            add_event(
                TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=TimelineEventType.LEADER_SIGNAL_DETECTED,
                    description=(
                        f"Leader signal: {signal.action_type} on {signal.protocol} ({signal.leader_address[:10]}...)"
                    ),
                    strategy_id=self._strategy_id,
                    chain=signal.chain,
                    details={
                        "event_id": signal.event_id,
                        "signal_id": signal.signal_id,
                        "action_type": signal.action_type,
                        "protocol": signal.protocol,
                        "leader_address": signal.leader_address,
                        "tokens": signal.tokens,
                    },
                )
            )
        except Exception:
            logger.debug("Failed to emit LEADER_SIGNAL_DETECTED event", exc_info=True)

    def _emit_skip_event(self, event: LeaderEvent, reason: str) -> None:
        """Emit a LEADER_SIGNAL_SKIPPED timeline event."""
        try:
            add_event(
                TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=TimelineEventType.LEADER_SIGNAL_SKIPPED,
                    description=f"Leader signal skipped: {reason} ({event.event_id})",
                    strategy_id=self._strategy_id,
                    chain=event.chain,
                    details={
                        "event_id": event.event_id,
                        "reason": reason,
                        "leader_address": event.from_address,
                    },
                )
            )
        except Exception:
            logger.debug("Failed to emit LEADER_SIGNAL_SKIPPED event", exc_info=True)

    def _resolve_symbol(self, token: str, chain: str) -> tuple[str, bool]:
        """Resolve a token address to symbol.

        Returns:
            Tuple of (resolved_symbol_or_input, metadata_resolution_success)
        """
        if not token.startswith("0x"):
            return token, True
        try:
            from almanak.framework.data.tokens import get_token_resolver

            resolver = get_token_resolver()
            resolved = resolver.resolve(token, chain)
            # Access decimals to validate metadata completeness.
            _ = resolved.decimals
            return resolved.symbol, True
        except Exception:
            if self._strict_token_resolution:
                raise
            return token, False

    @staticmethod
    def _to_decimal(value: Any) -> Decimal | None:
        if value is None:
            return None
        if isinstance(value, Decimal):
            return value
        try:
            return Decimal(str(value))
        except Exception:
            return None

    @staticmethod
    def _get_field(obj: Any, field_name: str) -> Any:
        if obj is None:
            return None
        if isinstance(obj, dict):
            return obj.get(field_name)
        return getattr(obj, field_name, None)

    @staticmethod
    def _serialize_obj(obj: Any) -> Any:
        if obj is None:
            return None
        if is_dataclass(obj) and not isinstance(obj, type):
            return asdict(obj)
        if hasattr(obj, "to_dict") and callable(obj.to_dict):
            try:
                return obj.to_dict()
            except Exception:
                return str(obj)
        if isinstance(obj, dict):
            return obj
        return str(obj)

    @staticmethod
    def _is_sentinel_value(value: Any) -> bool:
        """Check whether a value is a non-concrete placeholder (e.g. from a proxy object).

        Real parser values are always primitives (int, str, Decimal) or structured
        types (dict, dataclass). This rejects proxy objects whose type does not
        originate from a concrete domain module.
        """
        if value is None:
            return False
        if isinstance(value, int | float | str | Decimal | dict | list | tuple | bytes):
            return False
        if is_dataclass(value):
            return False
        # Reject objects from proxy/mock frameworks
        module = getattr(type(value), "__module__", "") or ""
        if "mock" in module.lower() or "proxy" in module.lower():
            return True
        return False
