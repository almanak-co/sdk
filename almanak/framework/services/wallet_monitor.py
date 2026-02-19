"""Wallet monitor for copy trading leader wallet tracking.

Polls for leader wallet transactions via gateway RPC (eth_getBlockByNumber,
eth_getTransactionReceipt) and produces LeaderEvent objects for downstream
signal decoding.

Uses transaction-centric monitoring: scans blocks for transactions originating
from leader addresses, then fetches receipts. This captures ALL leader activity
regardless of which contract emits events, and gets block timestamps for free.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Protocol

from almanak.framework.services.copy_trading_models import LeaderEvent
from almanak.gateway.proto import gateway_pb2

logger = logging.getLogger(__name__)


@dataclass
class WalletMonitorConfig:
    """Configuration for the WalletMonitor."""

    leader_addresses: list[str]
    chain: str
    poll_interval_seconds: int = 12
    lookback_blocks: int = 50
    confirmation_depth: int = 1


class _RpcStubProtocol(Protocol):
    def Call(self, request: gateway_pb2.RpcRequest, timeout: float | None = None) -> gateway_pb2.RpcResponse: ...


class _GatewayClientProtocol(Protocol):
    @property
    def rpc(self) -> _RpcStubProtocol: ...


@dataclass
class WalletMonitor:
    """Monitors leader wallets for on-chain activity via gateway RPC.

    Scans blocks for transactions from leader addresses, fetches full
    receipts, and produces LeaderEvent objects (one per log in the receipt).
    """

    config: WalletMonitorConfig
    gateway_client: _GatewayClientProtocol
    _leader_addresses_lower: set[str] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self._leader_addresses_lower = {addr.lower() for addr in self.config.leader_addresses}

    def poll(self, state: dict) -> tuple[list[LeaderEvent], dict]:
        """Poll for new leader events.

        Args:
            state: Cursor state dict with 'last_processed_block' key.

        Returns:
            Tuple of (new_events, updated_state).
        """
        latest_block = self._get_latest_block()
        if latest_block is None:
            return [], state

        safe_block = max(0, latest_block - self.config.confirmation_depth)

        # Validate cursor integrity before proceeding
        cursor_issue = self._validate_cursor(state, safe_block)
        if cursor_issue == "corrupted":
            logger.error(
                "Cursor corruption detected (block %s > safe %d + 100); "
                "returning empty events without advancing cursor",
                state.get("last_processed_block"),
                safe_block,
            )
            return [], state

        # Reorg detection: verify stored block hash matches chain
        if self._detect_reorg(state, safe_block):
            # Reorg detected: roll back cursor and return empty
            rollback = max(1, self.config.confirmation_depth * 2)
            old_cursor = state.get("last_processed_block", safe_block)
            new_cursor = max(0, old_cursor - rollback)
            logger.warning(
                "Chain reorg detected at block %d; rolling cursor back from %d to %d",
                old_cursor,
                old_cursor,
                new_cursor,
            )
            new_state = {**state, "last_processed_block": new_cursor}
            new_state.pop("last_block_hash", None)
            return [], new_state

        default_from = max(0, safe_block - self.config.lookback_blocks)
        last_processed = state.get("last_processed_block")
        if last_processed is not None:
            from_block = max(last_processed + 1, default_from)
        else:
            from_block = default_from

        if from_block > safe_block:
            return [], state

        # Scan each block for leader transactions
        events = []
        last_block_hash = state.get("last_block_hash")
        for block_num in range(from_block, safe_block + 1):
            block = self._get_block(block_num)
            if block is None:
                continue

            block_timestamp = _parse_hex_int(block.get("timestamp", "0x0"))
            transactions = block.get("transactions", [])
            last_block_hash = block.get("hash")

            # Filter transactions from leader addresses
            for tx in transactions:
                tx_from = tx.get("from", "")
                if tx_from.lower() not in self._leader_addresses_lower:
                    continue

                tx_hash = tx.get("hash")
                if not tx_hash:
                    continue

                # Fetch full receipt for this leader transaction
                receipt = self._get_transaction_receipt(tx_hash)
                if receipt is None:
                    continue

                to_address = tx.get("to", "")

                # One event per leader transaction (receipt has all logs)
                event = LeaderEvent(
                    chain=self.config.chain,
                    block_number=block_num,
                    tx_hash=tx_hash,
                    log_index=0,
                    timestamp=block_timestamp,
                    from_address=tx_from,
                    to_address=to_address,
                    receipt=receipt,
                    block_hash=block.get("hash"),
                    tx_index=_parse_hex_int(tx.get("transactionIndex") or "0x0"),
                    tx_type=tx.get("type"),
                    gas_price_wei=_parse_hex_int(tx.get("gasPrice") or "0x0"),
                )
                events.append(event)

        new_state = {**state, "last_processed_block": safe_block}
        if last_block_hash:
            new_state["last_block_hash"] = last_block_hash
        logger.info(
            "WalletMonitor polled blocks %d-%d: %d events",
            from_block,
            safe_block,
            len(events),
        )
        return events, new_state

    def _validate_cursor(self, state: dict, safe_block: int) -> str | None:
        """Validate cursor state for corruption.

        Returns:
            'corrupted' if cursor is impossibly ahead, else None.
        """
        last_processed = state.get("last_processed_block")
        if last_processed is None:
            return None

        # Cursor impossibly far ahead of chain tip
        if last_processed > safe_block + 100:
            return "corrupted"

        # Cursor impossibly far behind -- warn but don't fail
        if safe_block - last_processed > 100_000:
            logger.warning(
                "Cursor is %d blocks behind chain tip (block %d vs %d); this may cause slow catch-up",
                safe_block - last_processed,
                last_processed,
                safe_block,
            )

        return None

    def _detect_reorg(self, state: dict, safe_block: int) -> bool:
        """Detect chain reorganization by verifying stored block hash.

        Returns True if a reorg is detected.
        """
        stored_hash = state.get("last_block_hash")
        last_processed = state.get("last_processed_block")
        if stored_hash is None or last_processed is None:
            return False

        # Only check if we're continuing from where we left off
        if last_processed > safe_block:
            return False

        # Fetch the block at last_processed and compare hashes
        block = self._get_block(last_processed)
        if block is None:
            # Can't verify; assume no reorg
            return False

        chain_hash = block.get("hash")
        if chain_hash is None:
            return False

        return chain_hash.lower() != stored_hash.lower()

    def _get_latest_block(self) -> int | None:
        """Get the latest block number via eth_blockNumber."""
        request = gateway_pb2.RpcRequest(
            chain=self.config.chain,
            method="eth_blockNumber",
            params="[]",
            id="wallet_monitor_block_number",
        )
        try:
            response = self.gateway_client.rpc.Call(request, timeout=30.0)
        except Exception:
            logger.exception("Failed to call eth_blockNumber")
            return None

        if not response.success:
            logger.error("eth_blockNumber failed: %s", response.error)
            return None

        result = json.loads(response.result)
        return int(result, 16)

    def _get_block(self, block_number: int) -> dict | None:
        """Fetch a full block with transactions via eth_getBlockByNumber."""
        request = gateway_pb2.RpcRequest(
            chain=self.config.chain,
            method="eth_getBlockByNumber",
            params=json.dumps([hex(block_number), True]),
            id=f"wallet_monitor_block_{block_number}",
        )
        try:
            response = self.gateway_client.rpc.Call(request, timeout=30.0)
        except Exception:
            logger.exception("Failed to get block %d", block_number)
            return None

        if not response.success:
            logger.error("eth_getBlockByNumber failed for %d: %s", block_number, response.error)
            return None

        return json.loads(response.result)

    def _get_transaction_receipt(self, tx_hash: str) -> dict | None:
        """Fetch a single transaction receipt via eth_getTransactionReceipt."""
        request = gateway_pb2.RpcRequest(
            chain=self.config.chain,
            method="eth_getTransactionReceipt",
            params=json.dumps([tx_hash]),
            id=f"wallet_monitor_receipt_{tx_hash[:10]}",
        )
        try:
            response = self.gateway_client.rpc.Call(request, timeout=30.0)
        except Exception:
            logger.exception("Failed to get receipt for %s", tx_hash)
            return None

        if not response.success:
            logger.error("eth_getTransactionReceipt failed for %s: %s", tx_hash, response.error)
            return None

        return json.loads(response.result)


def _parse_hex_int(value: str | int) -> int:
    """Parse a hex string or int into an integer."""
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.startswith("0x"):
        return int(value, 16)
    return int(value)
