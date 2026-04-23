"""Across Bridge Receipt Parser (VIB-3226).

Parses the source-chain deposit transaction for an Across bridge transfer
and exposes a typed ``BridgeData`` extraction method for the framework's
``ResultEnricher`` pipeline.

Across V3 SpokePool emits ``V3FundsDeposited(...)`` (topic0
``0xa123dc29...``) on a successful deposit. The event carries:

- ``inputToken`` / ``outputToken`` (source / destination token addresses)
- ``inputAmount`` / ``outputAmount`` (pre/post relayer-fee amounts, raw units)
- ``destinationChainId`` (indexed uint256 topic)

We also fall back to ERC-20 ``Transfer`` events when the deposit event is
absent (e.g., older SpokePool versions), matching the pattern used by the
LiFi parser for cross-chain transfers.

Destination-chain settlement is observed asynchronously by
:class:`almanak.framework.execution.enso_state_provider.EnsoStateProvider`;
this parser only reports what is visible on the source-chain receipt.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any

from almanak.framework.connectors.base.hex_utils import HexDecoder
from almanak.framework.execution.extracted_data import BridgeData

from .adapter import ACROSS_CHAIN_ID_TO_NAME, ACROSS_SPOKE_POOL_ADDRESSES

logger = logging.getLogger(__name__)


# V3FundsDeposited(address inputToken, address outputToken, uint256 inputAmount,
#                  uint256 outputAmount, uint256 indexed destinationChainId,
#                  uint32 indexed depositId, uint32 quoteTimestamp, uint32 fillDeadline,
#                  uint32 exclusivityDeadline, address indexed depositor, address recipient,
#                  address exclusiveRelayer, bytes message)
V3_FUNDS_DEPOSITED_TOPIC = "0xa123dc29aebf7d0c3322c8eeb5b999e859f39937950ed31056532713d0de396f"

# ERC-20 Transfer event signature
TRANSFER_EVENT_SIGNATURE = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

# Reverse lookup for spoke pool addresses — used to identify which log is the
# deposit event when multiple SpokePool-like addresses appear on the same chain.
_SPOKE_POOL_ADDRS: set[str] = {addr.lower() for addr in ACROSS_SPOKE_POOL_ADDRESSES.values()}


class AcrossReceiptParser:
    """Receipt parser for Across V3 bridge deposits.

    Implements the ``ResultEnricher`` extraction contract by exposing
    ``extract_bridge_data(receipt, **hints) -> BridgeData | None``. The
    parser declares ``SUPPORTED_EXTRACTIONS`` so the enricher skips fields
    that do not apply to bridges (swap_amounts, position_id, etc.) without
    emitting spurious warnings.
    """

    SUPPORTED_EXTRACTIONS: frozenset[str] = frozenset({"bridge_data"})

    def __init__(self, **kwargs: Any) -> None:
        """Initialize AcrossReceiptParser.

        Args:
            **kwargs: Keyword arguments passed by the receipt_registry.
                chain: Source chain name (for token-decimal resolution).
        """
        self._chain: str | None = kwargs.get("chain")

    def parse_receipt(self, receipt: dict[str, Any]) -> dict[str, Any]:
        """Return a minimal parsed view of the receipt.

        The framework caches this call during enrichment (see
        ``ResultEnricher._install_parse_cache``). We keep the shape simple
        since ``extract_bridge_data`` does the real work.
        """
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
        bridge: str | None = None,  # unused; kept for kwargs-permissive signature
        expected_amount_out: str | Decimal | None = None,
    ) -> BridgeData | None:
        """Extract typed bridge data from an Across deposit receipt.

        Args:
            receipt: Source-chain transaction receipt (dict form).
            from_chain: Source chain name, forwarded by the enricher from
                ``ActionBundle.metadata["from_chain"]``.
            to_chain: Destination chain name (from ``metadata["to_chain"]``).
            token: Token symbol (from ``metadata["token"]``). When the
                parser can compute ``amount_sent`` from the on-chain event,
                it still needs the symbol for ``BridgeData.token_symbol``.
            amount: Human-readable amount from the compiler quote. Used only
                as a last-resort fallback when neither the deposit event nor
                a wallet ERC-20 Transfer log are parseable.
            bridge: Adapter display name (unused — included so the parser
                tolerates the kwargs the enricher always threads in).
            expected_amount_out: Compiler quote output amount (post-fee,
                pre-slippage) if available.

        Returns:
            ``BridgeData`` when the source-chain deposit can be decoded,
            ``None`` when the receipt does not describe an Across deposit
            (benign — the enricher maps this to ``ExtractMissing``).
        """
        if receipt.get("status", 0) != 1:
            return None

        logs = receipt.get("logs", [])
        deposit = self._find_v3_funds_deposited(logs)

        source_token_addr: str | None = None
        dest_token_addr: str | None = None
        amount_sent_raw = 0
        dest_chain_id: int | None = None

        if deposit is not None:
            source_token_addr = deposit.get("input_token")
            dest_token_addr = deposit.get("output_token")
            amount_sent_raw = int(deposit.get("input_amount", 0) or 0)
            dest_chain_id = deposit.get("destination_chain_id")

        # Fallback: the wallet's ERC-20 transfer to the spoke pool gives us
        # the amount even when the deposit log schema changes.
        if amount_sent_raw <= 0:
            amount_sent_raw, fallback_token = self._find_wallet_deposit_transfer(logs, receipt)
            if source_token_addr is None:
                source_token_addr = fallback_token

        if amount_sent_raw <= 0 and not deposit:
            # No deposit event and no wallet Transfer to a SpokePool — this
            # is not an Across receipt. Treat as missing (benign).
            return None

        # Resolve destination chain name. Prefer the compiler hint; fall
        # back to the on-chain chain-id -> name mapping; last-resort keep
        # the raw id as a string so the field stays populated.
        dest_chain_name = (to_chain or "").lower() if to_chain else None
        if not dest_chain_name and dest_chain_id is not None:
            dest_chain_name = ACROSS_CHAIN_ID_TO_NAME.get(int(dest_chain_id))
            if dest_chain_name is None:
                dest_chain_name = str(dest_chain_id)
        if not dest_chain_name:
            logger.debug("Across receipt: cannot resolve destination chain; skipping bridge_data")
            return None

        source_chain_name = (from_chain or self._chain or "").lower()
        if not source_chain_name:
            logger.debug("Across receipt: no source chain hint; skipping bridge_data")
            return None

        decimals = self._resolve_decimals(token, source_token_addr, source_chain_name)
        if decimals is None:
            logger.warning(
                "Across receipt: cannot resolve token decimals "
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
            bridge_name="across",
            source_token_address=source_token_addr.lower() if source_token_addr else None,
            destination_token_address=dest_token_addr.lower() if dest_token_addr else None,
            destination_tx_hash=None,  # async settlement — EnsoStateProvider populates the handshake
            expected_amount_out=expected_out_decimal,
        )

    # ------------------------------------------------------------------
    # Log decoding helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _find_v3_funds_deposited(logs: list[dict[str, Any]]) -> dict[str, Any] | None:
        """Find and decode the V3FundsDeposited event log if present."""
        for log in logs:
            topics = log.get("topics") or []
            if not topics:
                continue
            topic0 = _hex(topics[0])
            if topic0.lower() != V3_FUNDS_DEPOSITED_TOPIC:
                continue
            if len(topics) < 4:
                continue
            try:
                # Indexed: destinationChainId (topic 1), depositId (topic 2), depositor (topic 3)
                dest_chain_id = HexDecoder.decode_uint256(_hex(topics[1]))
                # Data: inputToken (32), outputToken (32), inputAmount (32), outputAmount (32),
                # quoteTimestamp (32), fillDeadline (32), exclusivityDeadline (32),
                # recipient (32), exclusiveRelayer (32), message_offset (32), ...
                # ``normalize_hex`` strips the 0x prefix; indices below are
                # **hex-char** offsets into the unprefixed string. ``decode_uint256``
                # takes a **byte** offset and internally multiplies by 2.
                data = HexDecoder.normalize_hex(log.get("data", ""))
                if len(data) < 64 * 4:
                    continue
                input_token = HexDecoder.topic_to_address("0x" + data[0:64])
                output_token = HexDecoder.topic_to_address("0x" + data[64:128])
                input_amount = HexDecoder.decode_uint256(data, 32 * 2)  # word 2
                output_amount = HexDecoder.decode_uint256(data, 32 * 3)  # word 3
                return {
                    "input_token": input_token,
                    "output_token": output_token,
                    "input_amount": input_amount,
                    "output_amount": output_amount,
                    "destination_chain_id": dest_chain_id,
                }
            except (ValueError, IndexError) as exc:
                logger.debug("Across V3FundsDeposited decode failed: %s", exc)
                continue
        return None

    @staticmethod
    def _find_wallet_deposit_transfer(
        logs: list[dict[str, Any]],
        receipt: dict[str, Any],
    ) -> tuple[int, str | None]:
        """Find the wallet's ERC-20 Transfer into the spoke pool.

        Returns (amount_sent_raw, token_address) or (0, None) when no
        matching transfer is found.
        """
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
            if log_to.lower() not in _SPOKE_POOL_ADDRS:
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
        """Resolve token decimals via the unified TokenResolver.

        We never default to 18 (per CLAUDE.md "Token Resolution"). When
        decimals cannot be determined we return ``None`` and the parser
        surfaces ``ExtractMissing`` upstream.
        """
        try:
            from almanak.framework.data.tokens import get_token_resolver
        except Exception:
            return None

        resolver = get_token_resolver()
        # Prefer address when present — it disambiguates bridged variants
        # (USDC.e vs native USDC) without relying on symbol aliases.
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
    """Normalize a topic / address / data field to a ``0x...`` string."""
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


__all__ = ["AcrossReceiptParser", "V3_FUNDS_DEPOSITED_TOPIC"]
