"""Receipt parser for Hyperliquid CoreWriter perp submissions.

CoreWriter is fire-and-forget: the EVM tx emits a single ``RawAction`` log and
queues the order for HyperCore to settle **off the EVM**. So this receipt proves
*submission*, not *fill*: we can decode the order we sent (asset, side, size,
reduce-only) back out of the ``RawAction`` payload, but the **entry price,
realized PnL, fees, and confirmed fill size are NOT in the receipt** — they live
on HyperCore and are supplied by the perps-read snapshot (``perps_read.py``).

Empty ≠ Zero: every fill-economics extraction returns ``None`` (unmeasured),
never a fabricated ``0``. ``size_delta`` returns the *submitted* base size
(what we asked for on an IOC market order); the authoritative filled size comes
from the read path. This mirrors the async-settlement handling already used by
gmx_v2 (position observed on a later read, not from the submit receipt).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from .addresses import RAW_ACTION_EVENT_TOPIC
from .sdk import LimitOrderAction, decode_limit_order_action, decode_raw_action_log_data

logger = logging.getLogger(__name__)


@dataclass
class ParsedReceipt:
    """Decoded CoreWriter submissions found in a receipt."""

    limit_orders: list[LimitOrderAction] = field(default_factory=list)


class HyperliquidReceiptParser:
    """Parse CoreWriter ``RawAction`` submissions from a HyperEVM receipt."""

    # Fields ResultEnricher may request for PERP_OPEN / PERP_CLOSE. Everything
    # tied to the FILL is a declared-but-None extraction (off-EVM settlement),
    # so ResultEnricher does not warn about an unsupported field while we
    # honestly report the value as unmeasured.
    SUPPORTED_EXTRACTIONS: frozenset[str] = frozenset(
        {
            # PERP_OPEN
            "position_id",
            "size_delta",
            "collateral",
            "entry_price",
            "leverage",
            # PERP_CLOSE
            "exit_price",
            "realized_pnl",
            "fees_paid",
            "collateral_returned",
            "protocol_fees",
            "funding_fee_usd",
        }
    )

    def __init__(self, chain: str = "hyperevm", **_: Any) -> None:
        self.chain = chain

    # ------------------------------------------------------------------ parse
    def parse_receipt(self, receipt: dict[str, Any]) -> ParsedReceipt:
        """Decode every CoreWriter limit-order submission in a receipt.

        Safe on receipts with no RawAction logs (returns empty). Never raises on
        a malformed log — logs a warning and skips.
        """
        parsed = ParsedReceipt()
        for log in receipt.get("logs", []) or []:
            topics = log.get("topics", []) or []
            if not topics or self._to_hex_str(topics[0]) != RAW_ACTION_EVENT_TOPIC:
                continue
            try:
                blob = decode_raw_action_log_data(log.get("data", ""))
                if not blob:
                    continue
                parsed.limit_orders.append(decode_limit_order_action(blob))
            except Exception as exc:  # noqa: BLE001
                # Non-limit-order actions (cancel / usd-class transfer), a
                # truncated payload (eth_abi InsufficientDataBytes), or otherwise
                # malformed data — not a decodable limit order. parse_receipt must
                # never raise on a bad log; skip with a debug note.
                logger.debug("HyperliquidReceiptParser: skipping RawAction: %s", exc)
        return parsed

    # ------------------------------------------------ extractions (enricher)
    def extract_position_id(self, receipt: dict[str, Any]) -> str | None:
        """Client-order-id (cloid) of the submitted order, for traceability.

        Hyperliquid keys positions by (market, side), not a bytes32 id, so this
        is not load-bearing for close — it is a submission correlation handle.
        """
        parsed = self.parse_receipt(receipt)
        if not parsed.limit_orders:
            return None
        return hex(parsed.limit_orders[0].cloid)

    def extract_size_delta(self, _receipt: dict[str, Any]) -> None:
        """Unmeasured from the EVM receipt (off-EVM settlement).

        The submission carries a base-asset ``sz``, but the shared perp
        accounting path treats ``size_delta`` as USD notional (like GMX's
        ``size_delta_usd``, consumed by ``perp_handler``); returning the base
        quantity there would persist e.g. 0.01 BTC as ~$0.01. The EVM receipt has
        no fill notional — settlement happens off-EVM on HyperCore — so this is
        unmeasured (``None``), consistent with the other fill-economics
        extractors below. The true position size is sourced from the ``0x0800``
        position read; the submitted ``sz`` remains available via
        :meth:`extract_position_id`'s cloid for submission traceability.
        """
        return None

    # --- fill economics: unmeasured from the EVM receipt (off-EVM settlement) ---
    def extract_entry_price(self, _receipt: dict[str, Any]) -> None:
        """Fill price settles on HyperCore; not in the EVM receipt. Unmeasured."""
        return None

    def extract_exit_price(self, _receipt: dict[str, Any]) -> None:
        return None

    def extract_realized_pnl(self, _receipt: dict[str, Any]) -> None:
        return None

    def extract_collateral(self, _receipt: dict[str, Any]) -> None:
        """Margin is drawn from the HyperCore account; no EVM leg. Unmeasured."""
        return None

    def extract_collateral_returned(self, _receipt: dict[str, Any]) -> None:
        return None

    def extract_fees_paid(self, _receipt: dict[str, Any]) -> None:
        return None

    def extract_protocol_fees(self, _receipt: dict[str, Any]) -> None:
        return None

    def extract_funding_fee_usd(self, _receipt: dict[str, Any]) -> None:
        return None

    def extract_leverage(self, _receipt: dict[str, Any]) -> None:
        """Leverage is an account setting, not carried in the order. Unmeasured."""
        return None

    # ------------------------------------------------------------- internals
    @staticmethod
    def _to_hex_str(topic: Any) -> str:
        if isinstance(topic, bytes | bytearray):
            return "0x" + bytes(topic).hex()
        return str(topic).lower() if topic is not None else ""


__all__ = ["HyperliquidReceiptParser", "ParsedReceipt"]
