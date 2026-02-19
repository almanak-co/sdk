"""WalletActivityProvider for copy trading signal orchestration.

Orchestrates WalletMonitor(s) and CopySignalEngine, accumulates signals,
and exposes filtered access for strategy consumption via MarketSnapshot.
Supports multi-chain monitoring with per-chain cursors.
"""

from __future__ import annotations

import logging
from decimal import Decimal

from almanak.framework.services.copy_signal_engine import CopySignalEngine
from almanak.framework.services.copy_trading_models import CopySignal
from almanak.framework.services.wallet_monitor import WalletMonitor

logger = logging.getLogger(__name__)


class WalletActivityProvider:
    """Orchestrates wallet monitoring and signal production for strategies.

    Sits between the raw monitoring components (WalletMonitor, CopySignalEngine)
    and the MarketSnapshot. Accumulates signals between polls and provides
    filtered access for strategy consumption.

    Supports both single-monitor and multi-chain monitor configurations.
    """

    def __init__(
        self,
        wallet_monitor: WalletMonitor | None = None,
        signal_engine: CopySignalEngine | None = None,
        state_manager_state: dict | None = None,
        wallet_monitors: dict[str, WalletMonitor] | None = None,
    ) -> None:
        # Support both single-monitor (backward compat) and multi-monitor
        self._monitors: dict[str, WalletMonitor] = {}
        if wallet_monitors is not None:
            self._monitors = dict(wallet_monitors)
        elif wallet_monitor is not None:
            self._monitors[wallet_monitor.config.chain] = wallet_monitor

        self._signal_engine = signal_engine
        self._pending_signals: list[CopySignal] = []
        self._state: dict = {}
        if state_manager_state is not None:
            self.set_state(dict(state_manager_state))

    def poll_and_process(self) -> None:
        """Poll all monitors for new leader events and decode them into signals."""
        total_events = 0
        total_signals = 0

        for chain, monitor in self._monitors.items():
            cursor_key = f"cursor:{chain}"
            chain_state = self._state.get(cursor_key, {})

            events, updated_state = monitor.poll(chain_state)
            self._state[cursor_key] = updated_state

            # Also maintain legacy 'last_processed_block' for single-chain backward compat
            if len(self._monitors) == 1:
                self._state.update(updated_state)

            if events and self._signal_engine is not None:
                # Extract latest block from monitor state for leader lag computation
                latest_block = updated_state.get("last_processed_block")
                if isinstance(latest_block, str):
                    try:
                        latest_block = int(latest_block)
                    except (ValueError, TypeError):
                        latest_block = None
                signals = self._signal_engine.process_events(events, current_block=latest_block)
                self._pending_signals.extend(signals)
                total_signals += len(signals)

            total_events += len(events)

        if total_events > 0:
            logger.info(
                "WalletActivityProvider: %d events -> %d new signals (%d pending total)",
                total_events,
                total_signals,
                len(self._pending_signals),
            )

    def get_signals(
        self,
        action_types: list[str] | None = None,
        protocols: list[str] | None = None,
        min_usd_value: Decimal | None = None,
        leader_address: str | None = None,
    ) -> list[CopySignal]:
        """Return pending signals filtered by the provided criteria.

        Filters are AND-combined. Does NOT consume signals -- strategy can
        call multiple times.
        """
        result = list(self._pending_signals)

        if action_types is not None:
            result = [s for s in result if s.action_type in action_types]

        if protocols is not None:
            result = [s for s in result if s.protocol in protocols]

        if min_usd_value is not None:
            result = [s for s in result if sum(s.amounts_usd.values(), Decimal(0)) >= min_usd_value]

        if leader_address is not None:
            leader_lower = leader_address.lower()
            result = [s for s in result if s.leader_address.lower() == leader_lower]

        return result

    def consume_signals(self, event_ids: list[str]) -> None:
        """Remove signals with matching event_ids from pending list."""
        ids_to_remove = set(event_ids)
        self._pending_signals = [s for s in self._pending_signals if s.event_id not in ids_to_remove]

    def inject_signals(self, signals: list[CopySignal]) -> None:
        """Inject synthetic/replay signals into the pending queue.

        Signals are deduplicated by signal_id/event_id against the current queue.
        """
        existing_ids = {(s.signal_id or s.event_id) for s in self._pending_signals}
        for signal in signals:
            signal_id = signal.signal_id or signal.event_id
            if signal_id in existing_ids:
                continue
            self._pending_signals.append(signal)
            existing_ids.add(signal_id)

    def get_state(self) -> dict:
        """Return current cursor state for persistence."""
        return dict(self._state)

    def set_state(self, state: dict) -> None:
        """Restore cursor state (on startup).

        Migrates legacy flat state (``last_processed_block``) to per-chain
        cursor keys so that upgraded code does not re-scan from lookback.
        """
        self._state = dict(state)

        # Migrate legacy flat state -> per-chain cursor keys
        has_cursor_keys = any(k.startswith("cursor:") for k in self._state)
        if not has_cursor_keys and "last_processed_block" in self._state:
            legacy = {k: v for k, v in self._state.items() if k in ("last_processed_block", "last_block_hash")}
            for chain in self._monitors:
                self._state[f"cursor:{chain}"] = dict(legacy)
