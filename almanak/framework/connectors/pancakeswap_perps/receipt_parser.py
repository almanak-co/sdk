"""PancakeSwap Perps receipt parser.

Decodes the events emitted by the ApolloX Diamond on BSC along the open/close
lifecycle:

  user-signed open:        TradingPortalFacet.MarketPendingTrade(user, tradeHash, trade)
  keeper settle open:      TradingOpenFacet.OpenMarketTrade(user, tradeHash, ot)
                           or PendingTradeRefund(user, tradeHash, refund)
  user-signed close:       (pending close request, no user-facing event — stored
                            in PriceFacadeFacet.pendingPrices; routes via price keeper)
  keeper settle close:     TradingCloseFacet.CloseTradeReceived(user, tradeHash, token, amount)
                           + CloseTradeSuccessful(user, tradeHash, closeInfo)

Extraction methods exposed to ResultEnricher:
    extract_position_id(receipt)      -> tradeHash (hex str, 0x-prefixed)   — PERP_OPEN
    extract_size_delta(receipt)       -> Decimal position qty (10-decimal)  — PERP_OPEN
    extract_collateral(receipt)       -> Decimal margin amount              — PERP_OPEN
    extract_entry_price(receipt)      -> Decimal (if OpenMarketTrade present) — PERP_OPEN
    extract_exit_price(receipt)       -> Decimal (if CloseTradeSuccessful present) — PERP_CLOSE
    extract_realized_pnl(receipt)     -> Decimal int96 PnL                  — PERP_CLOSE
    extract_fees_paid(receipt)        -> Decimal close fee                  — PERP_CLOSE
    extract_collateral_returned(receipt) -> Decimal payout token amount     — PERP_CLOSE

The parser is synchronous and operates purely on receipt dicts — no RPC calls.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from almanak.framework.connectors.base import HexDecoder

from .sdk import (
    EVENT_CLOSE_TRADE_RECEIVED,
    EVENT_CLOSE_TRADE_SUCCESSFUL,
    EVENT_MARKET_PENDING_TRADE,
    EVENT_OPEN_MARKET_TRADE,
    EVENT_PENDING_TRADE_REFUND,
    PRICE_DECIMALS,
    QTY_DECIMALS,
)

logger = logging.getLogger(__name__)


# =============================================================================
# Typed parsed events
# =============================================================================


@dataclass
class MarketPendingTradeEvent:
    """TradingPortalFacet.MarketPendingTrade — fires on user-signed openMarketTrade."""

    user: str
    trade_hash: str  # 0x-prefixed 32-byte hex
    pair_base: str
    is_long: bool
    token_in: str
    amount_in: int  # uint96, token smallest units
    qty: int  # uint80, 10-decimal fixed-point (see sdk.QTY_DECIMALS)
    price: int  # uint64, 8-decimal fixed-point
    stop_loss: int
    take_profit: int
    broker: int
    log_index: int = 0


@dataclass
class OpenMarketTradeEvent:
    """TradingOpenFacet.OpenMarketTrade — fires when keeper fills a pending open."""

    user: str
    trade_hash: str
    entry_price: int  # uint64, 8-decimal fixed-point
    pair_base: str
    token_in: str
    margin: int  # uint96
    qty: int  # uint80
    is_long: bool
    open_fee: int  # uint96
    execution_fee: int  # uint96
    timestamp: int  # uint40
    log_index: int = 0


@dataclass
class PendingTradeRefundEvent:
    """TradingOpenFacet.PendingTradeRefund — fires when keeper refunds a pending trade."""

    user: str
    trade_hash: str
    refund_code: int  # uint8 — reason code
    log_index: int = 0


@dataclass
class CloseTradeSuccessfulEvent:
    """TradingCloseFacet.CloseTradeSuccessful — fires when keeper fills a close."""

    user: str
    trade_hash: str
    close_price: int  # uint64
    funding_fee: int  # int96 (signed)
    close_fee: int  # uint96
    pnl: int  # int96 (signed)
    holding_fee: int  # uint96
    log_index: int = 0


@dataclass
class CloseTradeReceivedEvent:
    """TradingCloseFacet.CloseTradeReceived — payout leg of a close."""

    user: str
    trade_hash: str
    token: str
    amount: int
    log_index: int = 0


@dataclass
class ParsedReceipt:
    """Aggregate view of decoded PancakeSwap Perps events in a receipt."""

    market_pending_trades: list[MarketPendingTradeEvent] = field(default_factory=list)
    open_market_trades: list[OpenMarketTradeEvent] = field(default_factory=list)
    pending_trade_refunds: list[PendingTradeRefundEvent] = field(default_factory=list)
    close_trade_successful: list[CloseTradeSuccessfulEvent] = field(default_factory=list)
    close_trade_received: list[CloseTradeReceivedEvent] = field(default_factory=list)


# =============================================================================
# Parser
# =============================================================================


class PancakeSwapPerpsReceiptParser:
    """Receipt parser for PancakeSwap Perps (ApolloX Diamond)."""

    # Declared capabilities consumed by ResultEnricher (see _extract_field in result_enricher).
    SUPPORTED_EXTRACTIONS: frozenset[str] = frozenset(
        {
            # PERP_OPEN
            "position_id",
            "size_delta",
            "collateral",
            "entry_price",
            # PERP_CLOSE
            "exit_price",
            "realized_pnl",
            "fees_paid",
            "collateral_returned",
        }
    )

    def __init__(self, chain: str = "bsc", **_: Any) -> None:
        """Accept the chain kwarg that ReceiptParserRegistry passes in.

        PancakeSwap Perps is BSC-only in v1; the chain argument is accepted for
        registry-interface compatibility and stored for logging/diagnostic use.
        """
        self.chain = chain

    # -----------------------------------------------------------------
    # Top-level parse — decode every known event in a receipt
    # -----------------------------------------------------------------

    def parse_receipt(self, receipt: dict[str, Any]) -> ParsedReceipt:
        """Decode all PCS-Perps events present in a TX receipt.

        Safe to call on receipts with no PCS-Perps events (returns an empty
        ParsedReceipt). Never raises on malformed logs — logs a warning and
        skips.
        """
        parsed = ParsedReceipt()
        for log in receipt.get("logs", []) or []:
            topics = log.get("topics", []) or []
            if not topics:
                continue
            topic0 = self._to_hex_str(topics[0])
            if not topic0:
                continue
            try:
                if topic0 == EVENT_MARKET_PENDING_TRADE:
                    ev = self._decode_market_pending_trade(log)
                    if ev:
                        parsed.market_pending_trades.append(ev)
                elif topic0 == EVENT_OPEN_MARKET_TRADE:
                    ev2 = self._decode_open_market_trade(log)
                    if ev2:
                        parsed.open_market_trades.append(ev2)
                elif topic0 == EVENT_PENDING_TRADE_REFUND:
                    ev3 = self._decode_pending_trade_refund(log)
                    if ev3:
                        parsed.pending_trade_refunds.append(ev3)
                elif topic0 == EVENT_CLOSE_TRADE_SUCCESSFUL:
                    ev4 = self._decode_close_trade_successful(log)
                    if ev4:
                        parsed.close_trade_successful.append(ev4)
                elif topic0 == EVENT_CLOSE_TRADE_RECEIVED:
                    ev5 = self._decode_close_trade_received(log)
                    if ev5:
                        parsed.close_trade_received.append(ev5)
            except Exception as e:
                logger.warning(
                    "PancakeSwapPerpsReceiptParser: failed to decode log topic0=%s: %s",
                    topic0,
                    e,
                )
        return parsed

    # -----------------------------------------------------------------
    # Extraction methods called by ResultEnricher
    # -----------------------------------------------------------------

    def extract_position_id(self, receipt: dict[str, Any]) -> str | None:
        """Return the tradeHash from MarketPendingTrade (v1 OPEN path).

        If a filled open event (OpenMarketTrade) is present in the same receipt
        prefer that — but in practice keeper settlement happens in a separate TX,
        so MarketPendingTrade is the authoritative source for the OPEN-intent
        return value.
        """
        parsed = self.parse_receipt(receipt)
        if parsed.open_market_trades:
            return parsed.open_market_trades[0].trade_hash
        if parsed.market_pending_trades:
            return parsed.market_pending_trades[0].trade_hash
        return None

    def extract_size_delta(self, receipt: dict[str, Any]) -> Decimal | None:
        """Return the qty (position size in base units, 10-decimal — see sdk.QTY_DECIMALS) from the open event."""
        parsed = self.parse_receipt(receipt)
        if parsed.open_market_trades:
            qty_raw = parsed.open_market_trades[0].qty
        elif parsed.market_pending_trades:
            qty_raw = parsed.market_pending_trades[0].qty
        else:
            return None
        return Decimal(qty_raw) / (Decimal(10) ** QTY_DECIMALS)

    def extract_collateral(self, receipt: dict[str, Any]) -> Decimal | None:
        """Return the amountIn (raw margin, in the margin-token's smallest units).

        Note: the caller is responsible for applying token decimals — we expose
        the raw uint96 as a Decimal. (The token's decimal count isn't in the
        event; it lives in the token registry.)
        """
        parsed = self.parse_receipt(receipt)
        if parsed.open_market_trades:
            return Decimal(parsed.open_market_trades[0].margin)
        if parsed.market_pending_trades:
            return Decimal(parsed.market_pending_trades[0].amount_in)
        return None

    def extract_entry_price(self, receipt: dict[str, Any]) -> Decimal | None:
        """Return the keeper-filled entry price (only present if keeper settlement is in-receipt)."""
        parsed = self.parse_receipt(receipt)
        if not parsed.open_market_trades:
            return None
        raw = parsed.open_market_trades[0].entry_price
        return Decimal(raw) / (Decimal(10) ** PRICE_DECIMALS)

    def extract_exit_price(self, receipt: dict[str, Any]) -> Decimal | None:
        parsed = self.parse_receipt(receipt)
        if not parsed.close_trade_successful:
            return None
        raw = parsed.close_trade_successful[0].close_price
        return Decimal(raw) / (Decimal(10) ** PRICE_DECIMALS)

    def extract_realized_pnl(self, receipt: dict[str, Any]) -> Decimal | None:
        parsed = self.parse_receipt(receipt)
        if not parsed.close_trade_successful:
            return None
        # int96 signed; raw is already Python int with sign preserved by decoder.
        return Decimal(parsed.close_trade_successful[0].pnl)

    def extract_fees_paid(self, receipt: dict[str, Any]) -> Decimal | None:
        parsed = self.parse_receipt(receipt)
        if not parsed.close_trade_successful:
            return None
        e = parsed.close_trade_successful[0]
        # Close fee + holding fee (exclude funding fee which can be negative/positive).
        return Decimal(e.close_fee + e.holding_fee)

    def extract_collateral_returned(self, receipt: dict[str, Any]) -> Decimal | None:
        """Sum of all payout tokens emitted in CloseTradeReceived events for this receipt."""
        parsed = self.parse_receipt(receipt)
        if not parsed.close_trade_received:
            return None
        return Decimal(sum(e.amount for e in parsed.close_trade_received))

    # -----------------------------------------------------------------
    # Individual event decoders
    # -----------------------------------------------------------------

    @staticmethod
    def _to_hex_str(value: Any) -> str:
        """Normalize a topic / data value to a lowercase 0x-prefixed hex string.

        Handles:
          - str (already hex, maybe with 0x)
          - HexBytes (from web3.py receipts)
          - bytes / bytearray
        """
        if isinstance(value, str):
            s = value
        elif hasattr(value, "hex"):  # HexBytes / bytes / bytearray
            s = value.hex()
        else:
            s = str(value)
        if not s.startswith("0x") and not s.startswith("0X"):
            s = "0x" + s
        return s.lower()

    @staticmethod
    def _normalize_log(log: dict[str, Any]) -> tuple[list[str], str]:
        topics = log.get("topics") or []
        topics_norm = [PancakeSwapPerpsReceiptParser._to_hex_str(t) for t in topics]
        data = PancakeSwapPerpsReceiptParser._to_hex_str(log.get("data") or "0x")
        return topics_norm, data

    @staticmethod
    def _decode_market_pending_trade(log: dict[str, Any]) -> MarketPendingTradeEvent | None:
        """Decode MarketPendingTrade(user indexed, tradeHash indexed, tuple trade).

        tuple trade has 9 fields:
          (pairBase, isLong, tokenIn, amountIn, qty, price, stopLoss, takeProfit, broker).
        """
        topics, data = PancakeSwapPerpsReceiptParser._normalize_log(log)
        if len(topics) < 3:
            return None
        user = HexDecoder.topic_to_address(topics[1])
        trade_hash = HexDecoder.topic_to_bytes32(topics[2])
        # Check we have at least 9 words of data
        if len(data) < 2 + 9 * 64:
            logger.warning("MarketPendingTrade data too short: %d chars", len(data))
            return None
        return MarketPendingTradeEvent(
            user=user,
            trade_hash=trade_hash,
            pair_base=HexDecoder.decode_address_from_data(data, offset=0),
            is_long=HexDecoder.decode_uint256(data, offset=32) != 0,
            token_in=HexDecoder.decode_address_from_data(data, offset=64),
            amount_in=HexDecoder.decode_uint256(data, offset=96),
            qty=HexDecoder.decode_uint256(data, offset=128),
            price=HexDecoder.decode_uint256(data, offset=160),
            stop_loss=HexDecoder.decode_uint256(data, offset=192),
            take_profit=HexDecoder.decode_uint256(data, offset=224),
            broker=HexDecoder.decode_uint256(data, offset=256),
        )

    @staticmethod
    def _decode_open_market_trade(log: dict[str, Any]) -> OpenMarketTradeEvent | None:
        """Decode OpenMarketTrade(user indexed, tradeHash indexed, tuple ot).

        tuple ot has 17 fields (words):
          [0]user [1]userOpenTradeIndex [2]entryPrice [3]pairBase [4]tokenIn
          [5]margin [6]stopLoss [7]takeProfit [8]broker [9]isLong [10]openFee
          [11]longAccFundingFeePerShare [12]executionFee [13]timestamp
          [14]qty [15]holdingFeeRate [16]openBlock
        """
        topics, data = PancakeSwapPerpsReceiptParser._normalize_log(log)
        if len(topics) < 3:
            return None
        user = HexDecoder.topic_to_address(topics[1])
        trade_hash = HexDecoder.topic_to_bytes32(topics[2])
        if len(data) < 2 + 17 * 64:
            logger.warning("OpenMarketTrade data too short: %d chars", len(data))
            return None
        return OpenMarketTradeEvent(
            user=user,
            trade_hash=trade_hash,
            entry_price=HexDecoder.decode_uint256(data, offset=32 * 2),
            pair_base=HexDecoder.decode_address_from_data(data, offset=32 * 3),
            token_in=HexDecoder.decode_address_from_data(data, offset=32 * 4),
            margin=HexDecoder.decode_uint256(data, offset=32 * 5),
            is_long=HexDecoder.decode_uint256(data, offset=32 * 9) != 0,
            open_fee=HexDecoder.decode_uint256(data, offset=32 * 10),
            execution_fee=HexDecoder.decode_uint256(data, offset=32 * 12),
            timestamp=HexDecoder.decode_uint256(data, offset=32 * 13),
            qty=HexDecoder.decode_uint256(data, offset=32 * 14),
        )

    @staticmethod
    def _decode_pending_trade_refund(log: dict[str, Any]) -> PendingTradeRefundEvent | None:
        topics, data = PancakeSwapPerpsReceiptParser._normalize_log(log)
        if len(topics) < 3:
            return None
        return PendingTradeRefundEvent(
            user=HexDecoder.topic_to_address(topics[1]),
            trade_hash=HexDecoder.topic_to_bytes32(topics[2]),
            refund_code=HexDecoder.decode_uint256(data, offset=0),
        )

    @staticmethod
    def _decode_close_trade_successful(log: dict[str, Any]) -> CloseTradeSuccessfulEvent | None:
        """Decode CloseTradeSuccessful(user indexed, tradeHash indexed, tuple closeInfo).

        closeInfo: (uint64 closePrice, int96 fundingFee, uint96 closeFee, int96 pnl, uint96 holdingFee)
        Each field is padded to a 32-byte word, so 5 words total.
        """
        topics, data = PancakeSwapPerpsReceiptParser._normalize_log(log)
        if len(topics) < 3:
            return None
        if len(data) < 2 + 5 * 64:
            return None
        # int96 is stored in the low 12 bytes of a 32-byte word, sign-extended.
        # decode_int256 handles two's-complement correctly because the contract
        # ABI-encodes int96 with full-width sign extension.
        return CloseTradeSuccessfulEvent(
            user=HexDecoder.topic_to_address(topics[1]),
            trade_hash=HexDecoder.topic_to_bytes32(topics[2]),
            close_price=HexDecoder.decode_uint256(data, offset=0),
            funding_fee=HexDecoder.decode_int256(data, offset=32),
            close_fee=HexDecoder.decode_uint256(data, offset=64),
            pnl=HexDecoder.decode_int256(data, offset=96),
            holding_fee=HexDecoder.decode_uint256(data, offset=128),
        )

    @staticmethod
    def _decode_close_trade_received(log: dict[str, Any]) -> CloseTradeReceivedEvent | None:
        """Decode CloseTradeReceived(user indexed, tradeHash indexed, token indexed, uint256 amount)."""
        topics, data = PancakeSwapPerpsReceiptParser._normalize_log(log)
        if len(topics) < 4:
            return None
        return CloseTradeReceivedEvent(
            user=HexDecoder.topic_to_address(topics[1]),
            trade_hash=HexDecoder.topic_to_bytes32(topics[2]),
            token=HexDecoder.topic_to_address(topics[3]),
            amount=HexDecoder.decode_uint256(data, offset=0),
        )


__all__ = [
    "CloseTradeReceivedEvent",
    "CloseTradeSuccessfulEvent",
    "MarketPendingTradeEvent",
    "OpenMarketTradeEvent",
    "PancakeSwapPerpsReceiptParser",
    "ParsedReceipt",
    "PendingTradeRefundEvent",
]
