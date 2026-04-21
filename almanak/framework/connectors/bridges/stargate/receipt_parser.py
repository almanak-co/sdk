"""Stargate Bridge Receipt Parser (VIB-3226).

Parses the source-chain deposit transaction for a Stargate V2 bridge
transfer and exposes a typed ``BridgeData`` extraction method for the
framework's ``ResultEnricher`` pipeline.

Stargate V2 uses LayerZero messaging and emits ``OFTSent(bytes32 indexed
guid, uint32 dstEid, address indexed fromAddress, uint256 amountSentLD,
uint256 amountReceivedLD)`` from the source-chain pool contract on a
successful bridge. ``dstEid`` is the LayerZero endpoint id — we translate
it back to the framework's chain name via ``STARGATE_CHAIN_ID_TO_NAME``.

Destination-chain settlement is observed asynchronously via
``EnsoStateProvider``; this parser only reports what is visible on the
source-chain receipt.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any

from almanak.framework.connectors.base.hex_utils import HexDecoder
from almanak.framework.execution.extracted_data import BridgeData

from .adapter import STARGATE_CHAIN_ID_TO_NAME, STARGATE_ROUTER_ADDRESSES

logger = logging.getLogger(__name__)

# OFTSent(bytes32 indexed guid, uint32 dstEid, address indexed fromAddress,
#         uint256 amountSentLD, uint256 amountReceivedLD)
OFT_SENT_TOPIC = "0x85496b760a4b7f8d66384b9df21b381f5d1b1e79f229a47aaf4c232edc2fe59a"

# ERC-20 Transfer signature — used for the wallet->pool deposit fallback.
TRANSFER_EVENT_SIGNATURE = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

# Flatten router/pool addresses across chains for quick wallet-transfer matching.
_STARGATE_POOL_ADDRS: set[str] = {
    addr.lower() for chain_pools in STARGATE_ROUTER_ADDRESSES.values() for addr in chain_pools.values()
}


class StargateReceiptParser:
    """Receipt parser for Stargate V2 bridge deposits."""

    SUPPORTED_EXTRACTIONS: frozenset[str] = frozenset({"bridge_data"})

    def __init__(self, **kwargs: Any) -> None:
        """Initialize StargateReceiptParser.

        Args:
            **kwargs: Keyword arguments passed by the receipt_registry.
                chain: Source chain name for token-decimal resolution.
        """
        self._chain: str | None = kwargs.get("chain")

    def parse_receipt(self, receipt: dict[str, Any]) -> dict[str, Any]:
        """Return a minimal parsed view of the receipt for cache reuse."""
        return {
            "status": receipt.get("status", 0),
            "tx_hash": self._normalize_tx_hash(receipt.get("transactionHash") or receipt.get("tx_hash")),
        }

    def extract_bridge_data(
        self,
        receipt: dict[str, Any],
        *,
        from_chain: str | None = None,
        to_chain: str | None = None,
        token: str | None = None,
        amount: str | Decimal | None = None,
        bridge: str | None = None,
        expected_amount_out: str | Decimal | None = None,
    ) -> BridgeData | None:
        """Extract typed bridge data from a Stargate deposit receipt."""
        if receipt.get("status", 0) != 1:
            return None

        logs = receipt.get("logs", [])
        sent = self._find_oft_sent(logs)

        amount_sent_raw = 0
        dst_eid: int | None = None
        source_token_addr: str | None = None

        if sent is not None:
            amount_sent_raw = int(sent.get("amount_sent", 0) or 0)
            dst_eid = sent.get("dst_eid")

        # Fallback: the wallet's ERC-20 transfer into a Stargate pool gives
        # us the amount + source token even when OFTSent is absent.
        if amount_sent_raw <= 0 or source_token_addr is None:
            fallback_amount, fallback_token = self._find_wallet_deposit_transfer(logs, receipt)
            if amount_sent_raw <= 0:
                amount_sent_raw = fallback_amount
            if source_token_addr is None:
                source_token_addr = fallback_token

        if amount_sent_raw <= 0 and sent is None:
            # Not a Stargate receipt.
            return None

        dest_chain_name = (to_chain or "").lower() if to_chain else None
        if not dest_chain_name and dst_eid is not None:
            dest_chain_name = STARGATE_CHAIN_ID_TO_NAME.get(int(dst_eid))
            if dest_chain_name is None:
                dest_chain_name = str(dst_eid)
        if not dest_chain_name:
            logger.debug("Stargate receipt: cannot resolve destination chain; skipping bridge_data")
            return None

        source_chain_name = (from_chain or self._chain or "").lower()
        if not source_chain_name:
            logger.debug("Stargate receipt: no source chain hint; skipping bridge_data")
            return None

        decimals = self._resolve_decimals(token, source_token_addr, source_chain_name)
        if decimals is None:
            logger.warning(
                "Stargate receipt: cannot resolve token decimals "
                "(token=%s, token_address=%s, chain=%s); skipping bridge_data",
                token,
                source_token_addr,
                source_chain_name,
            )
            return None

        amount_sent_decimal = Decimal(amount_sent_raw) / Decimal(10**decimals) if amount_sent_raw else Decimal(0)
        expected_out_decimal = _coerce_decimal(expected_amount_out)

        tx_hash = self._normalize_tx_hash(receipt.get("transactionHash") or receipt.get("tx_hash"))

        return BridgeData(
            source_tx_hash=tx_hash,
            source_chain=source_chain_name,
            destination_chain=dest_chain_name,
            token_symbol=(token or "").upper(),
            amount_sent=amount_sent_decimal,
            amount_sent_raw=amount_sent_raw,
            bridge_name="stargate",
            source_token_address=source_token_addr.lower() if source_token_addr else None,
            destination_token_address=None,  # Stargate OFTSent does not encode dst token addr
            destination_tx_hash=None,
            expected_amount_out=expected_out_decimal,
        )

    # ------------------------------------------------------------------
    # Log decoding helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _find_oft_sent(logs: list[dict[str, Any]]) -> dict[str, Any] | None:
        """Find and decode the OFTSent event log if present."""
        for log in logs:
            topics = log.get("topics") or []
            if not topics:
                continue
            if _hex(topics[0]).lower() != OFT_SENT_TOPIC:
                continue
            try:
                # Data layout: dstEid (uint32, right-padded to 32), amountSentLD (32),
                # amountReceivedLD (32). Indexed: guid (topics[1]), fromAddress (topics[2]).
                # ``decode_uint256`` takes a **byte** offset — one 32-byte word = 32.
                data = HexDecoder.normalize_hex(log.get("data", ""))
                if len(data) < 64 * 3:
                    continue
                dst_eid = HexDecoder.decode_uint256(data, 0)
                amount_sent = HexDecoder.decode_uint256(data, 32)
                amount_received = HexDecoder.decode_uint256(data, 64)
                return {
                    "dst_eid": dst_eid,
                    "amount_sent": amount_sent,
                    "amount_received": amount_received,
                }
            except (ValueError, IndexError) as exc:
                logger.debug("Stargate OFTSent decode failed: %s", exc)
                continue
        return None

    @staticmethod
    def _find_wallet_deposit_transfer(
        logs: list[dict[str, Any]],
        receipt: dict[str, Any],
    ) -> tuple[int, str | None]:
        """Find the wallet's ERC-20 Transfer into the Stargate pool."""
        wallet = _hex(receipt.get("from") or receipt.get("from_address") or "").lower()
        if not wallet.startswith("0x"):
            return 0, None
        for log in logs:
            topics = log.get("topics") or []
            if len(topics) < 3:
                continue
            if _hex(topics[0]).lower() != TRANSFER_EVENT_SIGNATURE:
                continue
            log_from = HexDecoder.topic_to_address(topics[1])
            log_to = HexDecoder.topic_to_address(topics[2])
            if log_from.lower() != wallet:
                continue
            if log_to.lower() not in _STARGATE_POOL_ADDRS:
                continue
            data = HexDecoder.normalize_hex(log.get("data", ""))
            try:
                amount = HexDecoder.decode_uint256(data, 0) if data else 0
            except (ValueError, IndexError):
                continue
            token_address = _hex(log.get("address", ""))
            return amount, token_address or None
        return 0, None

    def _resolve_decimals(
        self,
        token_symbol: str | None,
        token_address: str | None,
        chain: str,
    ) -> int | None:
        """Resolve token decimals via the unified TokenResolver."""
        try:
            from almanak.framework.data.tokens import get_token_resolver
        except Exception:
            return None

        resolver = get_token_resolver()
        if token_address:
            try:
                return resolver.resolve(token_address, chain).decimals
            except Exception:
                pass
        if token_symbol:
            try:
                return resolver.resolve(token_symbol, chain).decimals
            except Exception:
                pass
        return None

    @staticmethod
    def _normalize_tx_hash(tx_hash: Any) -> str:
        if isinstance(tx_hash, bytes):
            result = tx_hash.hex()
            return result if result.startswith("0x") else "0x" + result
        return str(tx_hash) if tx_hash else ""


def _hex(value: Any) -> str:
    if isinstance(value, bytes):
        return "0x" + value.hex()
    return str(value) if value is not None else ""


def _coerce_decimal(raw: str | Decimal | None) -> Decimal | None:
    if raw is None or raw == "":
        return None
    try:
        val = Decimal(str(raw))
    except Exception:
        return None
    if not val.is_finite():
        return None
    return val


__all__ = ["StargateReceiptParser", "OFT_SENT_TOPIC"]
