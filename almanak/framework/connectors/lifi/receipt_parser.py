"""LiFi Receipt Parser.

This module provides functionality to parse transaction receipts
from LiFi swap and bridge transactions and extract the actual amounts
transferred.

LiFi transactions are executed through the Diamond proxy which delegates
to bridge-specific facets. The receipt contains standard ERC-20 Transfer
events that we parse to extract actual amounts.

For cross-chain transfers, only the source chain transaction receipt is
parsed here. The destination chain delivery is tracked via the LiFi status API.
"""

import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from almanak.framework.connectors.base.hex_utils import HexDecoder
from almanak.framework.execution.extracted_data import BridgeData, SwapAmounts

logger = logging.getLogger(__name__)


# Event signatures (keccak256 of event signature)
TRANSFER_EVENT_SIGNATURE = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"


@dataclass
class LiFiSwapResult:
    """Result of a LiFi swap/bridge transaction.

    Attributes:
        success: Whether the transaction was successful
        token_in: Input token address
        token_out: Output token address (on source chain)
        amount_in: Actual input amount sent
        amount_out: Actual output amount received (source chain only)
        tx_hash: Transaction hash
        gas_used: Gas used by the transaction
        effective_gas_price: Effective gas price
        tool: Bridge/DEX tool used (e.g., "across", "1inch")
        is_cross_chain: Whether this was a cross-chain transfer
        error: Error message if failed
    """

    success: bool
    token_in: str | None = None
    token_out: str | None = None
    amount_in: int = 0
    amount_out: int = 0
    tx_hash: str | None = None
    gas_used: int = 0
    effective_gas_price: int = 0
    tool: str | None = None
    is_cross_chain: bool = False
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "token_in": self.token_in,
            "token_out": self.token_out,
            "amount_in": str(self.amount_in),
            "amount_out": str(self.amount_out),
            "tx_hash": self.tx_hash,
            "gas_used": self.gas_used,
            "effective_gas_price": self.effective_gas_price,
            "tool": self.tool,
            "is_cross_chain": self.is_cross_chain,
            "error": self.error,
        }


class LiFiReceiptParser:
    """Parser for LiFi transaction receipts.

    This parser extracts swap/bridge results from transaction receipts by:
    1. Checking transaction status
    2. Parsing ERC-20 Transfer event logs to find amounts
    3. Identifying the wallet's sent and received amounts

    For cross-chain transfers, amount_out reflects what was sent to the
    bridge on the source chain. The actual received amount on the destination
    chain must be checked via the LiFi status API.

    Example:
        parser = LiFiReceiptParser()
        receipt = web3.eth.get_transaction_receipt(tx_hash)

        result = parser.parse_swap_receipt(
            receipt=receipt,
            wallet_address="0x...",
            token_out="0x...",
        )
        print(f"Sent: {result.amount_in}, Received: {result.amount_out}")
    """

    def __init__(self, **kwargs: Any) -> None:
        """Initialize LiFiReceiptParser.

        Args:
            **kwargs: Keyword arguments passed by the receipt_registry.
                chain: Chain name for token decimal resolution.
        """
        self._chain: str | None = kwargs.get("chain")

    def parse_swap_receipt(
        self,
        receipt: dict[str, Any],
        wallet_address: str,
        token_out: str,
        token_in: str | None = None,
        expected_amount_out: int | None = None,
        tool: str | None = None,
        is_cross_chain: bool = False,
    ) -> LiFiSwapResult:
        """Parse a swap/bridge transaction receipt.

        Args:
            receipt: Transaction receipt from web3
            wallet_address: Address that executed the transaction
            token_out: Output token address (on source chain for bridges)
            token_in: Input token address (optional)
            expected_amount_out: Expected output amount for validation
            tool: Bridge/DEX tool used
            is_cross_chain: Whether this is a cross-chain transfer

        Returns:
            LiFiSwapResult with parsed data
        """
        tx_hash = self._normalize_tx_hash(receipt.get("transactionHash"))

        # Check transaction status
        status = receipt.get("status", 0)
        if status != 1:
            return LiFiSwapResult(
                success=False,
                tx_hash=tx_hash,
                tool=tool,
                is_cross_chain=is_cross_chain,
                error="Transaction reverted",
            )

        logs = receipt.get("logs", [])

        # Extract amount out (tokens received by wallet)
        amount_out = self._extract_transfer_amount(
            logs=logs,
            token_address=token_out,
            to_address=wallet_address,
        )

        # For cross-chain transfers, the wallet sends tokens TO the bridge contract,
        # so to_address=wallet won't match. Fall back to from_address=wallet which
        # captures the amount the wallet deposited into the bridge.
        if amount_out == 0 and is_cross_chain:
            amount_out = self._extract_transfer_amount(
                logs=logs,
                token_address=token_out,
                from_address=wallet_address,
            )

        # Extract amount in (tokens sent from wallet)
        amount_in = 0
        if token_in:
            amount_in = self._extract_transfer_amount(
                logs=logs,
                token_address=token_in,
                from_address=wallet_address,
            )

        # Use expected amount if we couldn't extract from logs
        if amount_out == 0 and expected_amount_out:
            logger.warning(f"Could not extract amount_out from logs, using expected: {expected_amount_out}")
            amount_out = expected_amount_out

        gas_used = receipt.get("gasUsed", 0)
        effective_gas_price = receipt.get("effectiveGasPrice", 0)

        logger.info(
            f"Parsed LiFi {'bridge' if is_cross_chain else 'swap'}: "
            f"tx={tx_hash[:10] if tx_hash else 'N/A'}..., "
            f"amount_in={amount_in:,}, amount_out={amount_out:,}, "
            f"tool={tool or 'unknown'}"
        )

        return LiFiSwapResult(
            success=True,
            token_in=token_in,
            token_out=token_out,
            amount_in=amount_in,
            amount_out=amount_out,
            tx_hash=tx_hash,
            gas_used=gas_used,
            effective_gas_price=effective_gas_price,
            tool=tool,
            is_cross_chain=is_cross_chain,
        )

    def extract_swap_amounts(self, receipt: dict[str, Any]) -> SwapAmounts | None:
        """Extract swap amounts for Result Enrichment system.

        Called by ResultEnricher after SWAP intent execution.

        Args:
            receipt: Transaction receipt

        Returns:
            SwapAmounts dataclass or None if not found
        """
        if receipt.get("status", 0) != 1:
            return None

        # For LiFi, we need wallet_address context to extract amounts.
        # This extraction method provides a best-effort parse by looking
        # at the first and last Transfer events in the receipt.
        logs = receipt.get("logs", [])
        transfers = self._get_all_transfers(logs)

        if not transfers:
            return None

        # First transfer is typically the input, last is the output
        first = transfers[0]
        last = transfers[-1]

        amount_in = first.get("amount", 0)
        amount_out = last.get("amount", 0)

        token_in_addr = first.get("token")
        token_out_addr = last.get("token")

        decimals_in = self._get_decimals(token_in_addr)
        decimals_out = self._get_decimals(token_out_addr)

        if decimals_in is None or decimals_out is None:
            logger.warning(
                f"Cannot compute LiFi swap amounts: token decimals unknown "
                f"(in={token_in_addr}:{decimals_in}, out={token_out_addr}:{decimals_out})"
            )
            return None

        amount_in_decimal = Decimal(amount_in) / Decimal(10**decimals_in) if amount_in else Decimal(0)
        amount_out_decimal = Decimal(amount_out) / Decimal(10**decimals_out)

        effective_price = amount_out_decimal / amount_in_decimal if amount_in_decimal else None

        return SwapAmounts(
            amount_in=amount_in,
            amount_out=amount_out,
            amount_in_decimal=amount_in_decimal,
            amount_out_decimal=amount_out_decimal,
            effective_price=effective_price,
            token_in=token_in_addr,
            token_out=token_out_addr,
        )

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
        """Extract typed BridgeData from a LiFi bridge receipt (VIB-3226).

        LiFi is primarily a swap aggregator but also handles cross-chain
        transfers (via embedded bridge "tools" like Across, Hop, etc.).
        When a BRIDGE intent is routed through LiFi — or a SwapIntent is
        cross-chain — the Diamond proxy emits ERC-20 Transfers that move
        the wallet's tokens to the bridge facet. We use the first
        wallet->contract Transfer to derive ``amount_sent`` and the token.

        Destination-chain settlement must be tracked via the LiFi status
        API — ``destination_tx_hash`` is intentionally None here.
        """
        if receipt.get("status", 0) != 1:
            return None

        logs = receipt.get("logs", [])
        transfers = self._get_all_transfers(logs)

        wallet = _norm_addr(receipt.get("from") or receipt.get("from_address") or "")
        if not wallet:
            return None

        # First outgoing transfer from the wallet is the deposit into LiFi.
        # ``_get_all_transfers`` stores ``from`` via ``HexDecoder.topic_to_address``
        # which returns checksummed (EIP-55) addresses; lowercase both sides
        # before comparing so mixed-case never silently drops a wallet transfer.
        wallet_outgoing = [t for t in transfers if str(t.get("from", "")).lower() == wallet]

        source_chain = (from_chain or self._chain or "").lower()
        dest_chain = (to_chain or "").lower() if to_chain else None
        if not source_chain or not dest_chain:
            logger.debug("LiFi bridge_data: missing chain hints (from=%s, to=%s)", source_chain, dest_chain)
            return None

        source_token_addr: str | None
        amount_sent_raw: int

        if wallet_outgoing:
            first_out = wallet_outgoing[0]
            amount_sent_raw = int(first_out.get("amount", 0) or 0)
            source_token_addr = first_out.get("token")
            if amount_sent_raw <= 0 or not source_token_addr:
                return None

            # Pass ``source_chain`` explicitly so decimals resolution uses the
            # caller-provided from_chain rather than ``self._chain`` (which
            # may not match when a single parser services multi-chain flows).
            decimals = self._get_decimals(source_token_addr, chain=source_chain)
            if decimals is None and token:
                try:
                    from almanak.framework.data.tokens import get_token_resolver

                    decimals = get_token_resolver().resolve(token, source_chain).decimals
                except Exception:
                    decimals = None
            if decimals is None:
                logger.warning(
                    "LiFi bridge_data: cannot resolve token decimals (token=%s, address=%s, chain=%s)",
                    token,
                    source_token_addr,
                    source_chain,
                )
                return None

            amount_sent_decimal = Decimal(amount_sent_raw) / Decimal(10**decimals)
        else:
            # Native-asset bridge (msg.value-funded, no ERC-20 Transfer).
            # Resolve amount from the compiler-provided quote and the native
            # token's decimals via the resolver — never default to 18.
            if amount in (None, "") or not token:
                return None
            try:
                amount_sent_decimal = Decimal(str(amount))
            except Exception:
                return None
            if not amount_sent_decimal.is_finite() or amount_sent_decimal <= 0:
                return None
            try:
                from almanak.framework.data.tokens import get_token_resolver

                decimals = get_token_resolver().resolve(token, source_chain).decimals
            except Exception:
                logger.warning(
                    "LiFi bridge_data: native-asset path cannot resolve decimals (token=%s, chain=%s)",
                    token,
                    source_chain,
                )
                return None
            amount_sent_raw = int(amount_sent_decimal * Decimal(10**decimals))
            source_token_addr = None

        expected_out_decimal: Decimal | None = None
        if expected_amount_out not in (None, ""):
            try:
                candidate = Decimal(str(expected_amount_out))
                if candidate.is_finite():
                    expected_out_decimal = candidate
            except Exception:
                expected_out_decimal = None

        tx_hash = self._normalize_tx_hash(receipt.get("transactionHash") or receipt.get("tx_hash"))

        return BridgeData(
            source_tx_hash=tx_hash,
            source_chain=source_chain,
            destination_chain=dest_chain,
            token_symbol=(token or "").upper(),
            amount_sent=amount_sent_decimal,
            amount_sent_raw=amount_sent_raw,
            bridge_name="lifi",
            source_token_address=source_token_addr.lower() if source_token_addr else None,
            destination_token_address=None,
            destination_tx_hash=None,
            expected_amount_out=expected_out_decimal,
        )

    def extract_position_id(self, receipt: dict[str, Any]) -> int | None:
        """LiFi swaps do not create LP positions."""
        return None

    def extract_liquidity(self, receipt: dict[str, Any]) -> dict[str, Any] | None:
        """LiFi swaps do not provide liquidity events."""
        return None

    def extract_lp_close_data(self, receipt: dict[str, Any]) -> dict[str, Any] | None:
        """LiFi swaps do not close LP positions."""
        return None

    # =============================================================================
    # Protocol Fee Extraction (VIB-3204)
    # =============================================================================

    def extract_protocol_fees(self, _receipt: dict[str, Any]) -> None:
        """Placeholder for LiFi aggregator protocol-fee extraction (VIB-3204).

        LiFi surfaces per-step fees in the quote response at compile
        time; receipts don't contain a uniform fee event. Threading the
        quote metadata into parser scope is deferred to a follow-up.

        Follow-up ticket: "Protocol fee extraction for aggregators
        (Enso, LiFi) — follow-up to VIB-3204".
        """
        return None

    def parse_approval_receipt(
        self,
        receipt: dict[str, Any],
    ) -> dict[str, Any]:
        """Parse an approval transaction receipt.

        Args:
            receipt: Transaction receipt from web3

        Returns:
            Dict with approval result
        """
        tx_hash = self._normalize_tx_hash(receipt.get("transactionHash"))
        status = receipt.get("status", 0)

        return {
            "success": status == 1,
            "tx_hash": tx_hash,
            "gas_used": receipt.get("gasUsed", 0),
            "effective_gas_price": receipt.get("effectiveGasPrice", 0),
            "error": "Transaction reverted" if status != 1 else None,
        }

    def _extract_transfer_amount(
        self,
        logs: list[dict[str, Any]],
        token_address: str,
        from_address: str | None = None,
        to_address: str | None = None,
    ) -> int:
        """Extract total transfer amount from logs.

        Sums all matching Transfer events to handle split routes,
        fee-on-transfer tokens, and multi-step settlements.

        Args:
            logs: Transaction logs
            token_address: Token contract address
            from_address: Filter by sender address
            to_address: Filter by recipient address

        Returns:
            Total transfer amount (0 if not found)
        """
        token_address_lower = token_address.lower()
        total = 0

        for log in logs:
            log_address = log.get("address", "")
            if isinstance(log_address, bytes):
                log_address = "0x" + log_address.hex()
            log_address = log_address.lower()

            if log_address != token_address_lower:
                continue

            topics = log.get("topics", [])
            if not topics:
                continue

            topic0 = topics[0]
            if isinstance(topic0, bytes):
                topic0 = "0x" + topic0.hex()
            topic0 = topic0.lower()

            if topic0 != TRANSFER_EVENT_SIGNATURE.lower():
                continue

            if len(topics) < 3:
                continue

            log_from = HexDecoder.topic_to_address(topics[1])
            log_to = HexDecoder.topic_to_address(topics[2])

            if from_address and log_from != from_address.lower():
                continue
            if to_address and log_to != to_address.lower():
                continue

            data = HexDecoder.normalize_hex(log.get("data", ""))
            if data:
                try:
                    amount = HexDecoder.decode_uint256(data, 0)
                    logger.debug(f"Found Transfer: from={log_from[:10]}..., to={log_to[:10]}..., amount={amount}")
                    total += amount
                except (ValueError, IndexError):
                    continue

        return total

    def _get_all_transfers(self, logs: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Extract all Transfer events from logs.

        Args:
            logs: Transaction logs

        Returns:
            List of dicts with token, from, to, amount
        """
        transfers = []

        for log in logs:
            topics = log.get("topics", [])
            if not topics:
                continue

            topic0 = topics[0]
            if isinstance(topic0, bytes):
                topic0 = "0x" + topic0.hex()

            if topic0.lower() != TRANSFER_EVENT_SIGNATURE.lower():
                continue

            if len(topics) < 3:
                continue

            log_address = log.get("address", "")
            if isinstance(log_address, bytes):
                log_address = "0x" + log_address.hex()

            data = HexDecoder.normalize_hex(log.get("data", ""))
            amount = 0
            if data:
                try:
                    amount = HexDecoder.decode_uint256(data, 0)
                except (ValueError, IndexError):
                    pass

            transfers.append(
                {
                    "token": log_address.lower(),
                    "from": HexDecoder.topic_to_address(topics[1]),
                    "to": HexDecoder.topic_to_address(topics[2]),
                    "amount": amount,
                }
            )

        return transfers

    def _get_decimals(self, token_address: str | None, chain: str | None = None) -> int | None:
        """Look up token decimals via the token resolver.

        Args:
            token_address: Token contract address.
            chain: Chain to resolve on. Takes precedence over ``self._chain``.
                VIB-3226 CodeRabbit audit: bridge flows need the *source*
                chain of the deposit — the parser-construction ``self._chain``
                can diverge from it when the same LiFiReceiptParser instance
                services a bridge whose from_chain is different from the
                parser's default.

        Returns:
            Decimals if resolvable, None otherwise.
        """
        if not token_address:
            return None
        chain_name = (chain or self._chain or "").lower()
        if not chain_name:
            logger.debug("LiFi _get_decimals: no chain available for resolution")
            return None
        try:
            from almanak.framework.data.tokens import get_token_resolver

            resolver = get_token_resolver()
            token = resolver.resolve(token_address, chain_name)
            return token.decimals
        except Exception:
            logger.debug(f"Could not resolve decimals for {token_address} on {chain_name}")
            return None

    @staticmethod
    def _normalize_tx_hash(tx_hash: Any) -> str:
        """Normalize transaction hash to hex string with 0x prefix."""
        if isinstance(tx_hash, bytes):
            result = tx_hash.hex()
            return result if result.startswith("0x") else "0x" + result
        return str(tx_hash) if tx_hash else ""


def _norm_addr(value: Any) -> str:
    """Normalize an address value to lowercase hex with 0x prefix."""
    if isinstance(value, bytes):
        value = "0x" + value.hex()
    return str(value).lower() if value else ""


__all__ = ["LiFiReceiptParser", "LiFiSwapResult"]
