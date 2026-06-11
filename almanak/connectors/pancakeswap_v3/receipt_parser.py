"""PancakeSwap V3 Receipt Parser (Refactored).

Refactored to use base infrastructure while maintaining backward compatibility.
PancakeSwap V3 is a Uniswap V3 fork with identical event signatures.
"""

import logging
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import TYPE_CHECKING, Any

from almanak.connectors._strategy_base.base import BaseReceiptParser, EventRegistry, HexDecoder
from almanak.framework.execution.extract_result import (
    ExtractError,
    ExtractMissing,
    ExtractOk,
    ExtractResult,
)

if TYPE_CHECKING:
    from almanak.framework.execution.extracted_data import (
        LPCloseData,
        LPOpenData,
        ProtocolFees,
        SwapAmounts,
    )

logger = logging.getLogger(__name__)


# =============================================================================
# Event Topic Signatures (same as Uniswap V3)
# =============================================================================

# PancakeSwap V3 uses a DIFFERENT Swap event signature than Uniswap V3!
# UniswapV3: Swap(address,address,int256,int256,uint160,uint128,int24) - 7 params
# PancakeSwapV3: Swap(address,address,int256,int256,uint160,uint128,int24,uint128,uint128) - 9 params
# The extra 2 uint128 params are protocolFeesToken0 and protocolFeesToken1
EVENT_TOPICS: dict[str, str] = {
    "Swap": "0x19b47279256b2a23a1665c810c8d55a1758940ee09377d4f8d26497a3577dc83",  # PancakeSwap V3 (9 params)
    "Mint": "0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde",
    "Burn": "0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c",
    "Collect": "0x70935338e69775456a85ddef226c395fb668b63fa0115f5f20610b388e6ca9c0",
    "Transfer": "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
    # NonfungiblePositionManager.IncreaseLiquidity(uint256 indexed tokenId, uint128 liquidity,
    #   uint256 amount0, uint256 amount1)
    # keccak("IncreaseLiquidity(uint256,uint128,uint256,uint256)") — identical to
    # Uniswap V3 (PancakeSwap V3 is a direct UV3 fork at the NPM contract level).
    "IncreaseLiquidity": "0x3067048beee31b25b2f1681f88dac838c8bba36af25bfb2b7cf7473a5847e35f",
    # NonfungiblePositionManager.DecreaseLiquidity(uint256 indexed tokenId, uint128 liquidity,
    #   uint256 amount0, uint256 amount1)
    # keccak("DecreaseLiquidity(uint256,uint128,uint256,uint256)") — identical to Uniswap V3
    # (PancakeSwap V3 is a direct UV3 fork at the NPM contract level). VIB-4305 (T12) so the
    # close-side parser can recover token_id from receipt facts alone (mirrors LP_OPEN's
    # IncreaseLiquidity → token_id pattern; required for physical_identity_hash on
    # registry-mode LP_CLOSE writes).
    "DecreaseLiquidity": "0x26f6a048ee9138f2c0ce266f322cb99228e8d619ae2bff30c67f8dcf9d2377b4",
}

TOPIC_TO_EVENT: dict[str, str] = {v: k for k, v in EVENT_TOPICS.items()}


# =============================================================================
# Enums
# =============================================================================


class PancakeSwapV3EventType(Enum):
    """PancakeSwap V3 event types."""

    SWAP = "SWAP"
    MINT = "MINT"
    BURN = "BURN"
    COLLECT = "COLLECT"
    TRANSFER = "TRANSFER"
    UNKNOWN = "UNKNOWN"


EVENT_NAME_TO_TYPE: dict[str, PancakeSwapV3EventType] = {
    "Swap": PancakeSwapV3EventType.SWAP,
    "Mint": PancakeSwapV3EventType.MINT,
    "Burn": PancakeSwapV3EventType.BURN,
    "Collect": PancakeSwapV3EventType.COLLECT,
    "Transfer": PancakeSwapV3EventType.TRANSFER,
}


# PancakeSwap V3 NonfungiblePositionManager addresses, sourced from the
# connector-local registry (single source of truth — ``PANCAKESWAP_V3``
# in ``almanak/connectors/pancakeswap_v3/addresses.py``). Mirrors the
# Aerodrome Slipstream pattern (``_build_slipstream_npm_addresses``) —
# adding a new chain is a one-line change in ``addresses.py`` and this
# dict rebuilds automatically.
#
# Historical note: this dict used to be hand-maintained as a flat
# ``{chain: addr}`` mapping. PancakeSwap V3 happens to use the same NPM
# deployer address across every supported chain today, but treating that
# coincidence as canonical hides drift the next time PCS deploys with a
# different address (the same trap that bit the early Uniswap V3 multi-
# chain story before VIB-3893). Reading from ``PANCAKESWAP_V3`` keeps the
# registry authoritative.
def _build_pancakeswap_v3_npm_addresses() -> dict[str, str]:
    """Derive ``{chain: nft_position_manager}`` from ``PANCAKESWAP_V3``.

    Returns a lowercase-keyed map so chain-name comparisons in the parser
    don't need case-handling at the call site. The ``bnb`` alias for
    ``bsc`` is preserved (some callers historically passed it).
    """
    from .addresses import PANCAKESWAP_V3

    out: dict[str, str] = {}
    for chain, entry in PANCAKESWAP_V3.items():
        nft = entry.get("nft")
        if nft:
            out[chain.lower()] = nft.lower()
    # Preserve the historical ``bnb`` alias of ``bsc`` — the connector
    # __init__ docstring documents both names.
    if "bsc" in out and "bnb" not in out:
        out["bnb"] = out["bsc"]
    return out


POSITION_MANAGER_ADDRESSES: dict[str, str] = _build_pancakeswap_v3_npm_addresses()

# Zero address for detecting mints
ZERO_ADDRESS_PADDED = "0x" + "0" * 64


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class SwapEventData:
    """Parsed data from PancakeSwap V3 Swap event.

    Note: PancakeSwap V3 Swap event has 9 parameters (vs 7 for Uniswap V3):
    - sender, recipient (indexed)
    - amount0, amount1, sqrtPriceX96, liquidity, tick (same as UniV3)
    - protocolFeesToken0, protocolFeesToken1 (PancakeSwap V3 specific)
    """

    pool: str
    sender: str
    recipient: str
    amount0: Decimal
    amount1: Decimal
    sqrt_price_x96: int = 0
    liquidity: int = 0
    tick: int = 0
    protocol_fees_token0: int = 0  # PancakeSwap V3 specific
    protocol_fees_token1: int = 0  # PancakeSwap V3 specific

    @property
    def token0_in(self) -> bool:
        """Check if token0 is the input token."""
        return self.amount0 > 0

    @property
    def token1_in(self) -> bool:
        """Check if token1 is the input token."""
        return self.amount1 > 0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "pool": self.pool,
            "sender": self.sender,
            "recipient": self.recipient,
            "amount0": str(self.amount0),
            "amount1": str(self.amount1),
            "sqrt_price_x96": str(self.sqrt_price_x96),
            "liquidity": str(self.liquidity),
            "tick": self.tick,
            "token0_in": self.token0_in,
            "token1_in": self.token1_in,
            "protocol_fees_token0": str(self.protocol_fees_token0),
            "protocol_fees_token1": str(self.protocol_fees_token1),
        }


@dataclass
class ParseResult:
    """Result of parsing a receipt."""

    success: bool
    swaps: list[SwapEventData] = field(default_factory=list)
    error: str | None = None
    transaction_hash: str = ""
    block_number: int = 0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "swaps": [s.to_dict() for s in self.swaps],
            "error": self.error,
            "transaction_hash": self.transaction_hash,
            "block_number": self.block_number,
        }


# =============================================================================
# Internal swap-amount extraction state (Phase 8.4 refactor)
#
# These dataclasses are NOT part of the public API — they bundle state
# between the per-phase helpers of ``extract_swap_amounts`` so each helper
# stays small, pure, and individually testable. Mirrors the Phase 7.3
# Aerodrome ``_SwapSeed`` and the Phase 6 UniV4 pattern.
# =============================================================================


@dataclass
class _WalletTransfers:
    """ERC-20 Transfer events partitioned by wallet involvement.

    Each tuple is (token_address_lowercase, raw_amount_uint256).
    """

    from_wallet: list[tuple[str, int]]
    to_wallet: list[tuple[str, int]]


@dataclass
class _SwapSeed:
    """Raw (pre-decimal-scaling) swap amounts picked from wallet transfers."""

    token_in: str
    token_out: str
    amount_in: int
    amount_out: int


@dataclass
class _SwapDecimals:
    """Resolved (maybe-partial) token decimals for a swap.

    ``decimals_in`` is ``None`` when the input token could not be resolved —
    preserves the legacy fail-open behavior on the input side (output is
    fail-closed at the caller).
    """

    decimals_in: int | None
    decimals_out: int


# =============================================================================
# Receipt Parser
# =============================================================================


class PancakeSwapV3ReceiptParser(BaseReceiptParser[SwapEventData, ParseResult]):
    """Parser for PancakeSwap V3 transaction receipts.

    Uses base infrastructure for common parsing logic while handling
    PancakeSwap V3-specific event decoding.
    """

    SUPPORTED_EXTRACTIONS: frozenset[str] = frozenset(
        {
            "position_id",
            "swap_amounts",
            "tick_lower",
            "tick_upper",
            "liquidity",
            "lp_open_data",  # extract_lp_open_data — see VIB-3887 / VIB-3893
            "lp_close_data",
            "protocol_fees",  # VIB-3204 — extract_protocol_fees implemented below
        }
    )

    def __init__(self, chain: str = "bsc", **kwargs: Any) -> None:
        """Initialize the parser.

        Args:
            chain: Blockchain network (for position manager address lookup)
        """
        registry = EventRegistry(EVENT_TOPICS, EVENT_NAME_TO_TYPE)
        super().__init__(registry=registry)
        self.chain = chain.lower()

    def _decode_log_data(
        self,
        event_name: str,
        topics: list[Any],
        data: str,
        contract_address: str,
    ) -> dict[str, Any]:
        """Decode Swap event data.

        PancakeSwap V3 Swap event structure (9 params, different from UniV3's 7):
        - topic0: event signature
        - topic1: sender (indexed)
        - topic2: recipient (indexed)
        - data: amount0 (int256), amount1 (int256), sqrtPriceX96 (uint160),
                liquidity (uint128), tick (int24),
                protocolFeesToken0 (uint128), protocolFeesToken1 (uint128)
        """
        if event_name != "Swap":
            return {}

        # Parse indexed topics
        sender = HexDecoder.topic_to_address(topics[1]) if len(topics) > 1 else ""
        recipient = HexDecoder.topic_to_address(topics[2]) if len(topics) > 2 else ""

        # Parse non-indexed data (each value is 32 bytes = 64 hex chars)
        # PancakeSwap V3 has 7 data fields vs UniV3's 5
        amount0 = HexDecoder.decode_int256(data, 0)
        amount1 = HexDecoder.decode_int256(data, 32)
        sqrt_price_x96 = HexDecoder.decode_uint160(data, 64)
        liquidity = HexDecoder.decode_uint128(data, 96)
        tick = HexDecoder.decode_int24(data, 128)
        # PancakeSwap V3 specific: protocol fees (offset 160 and 192)
        protocol_fees_token0 = HexDecoder.decode_uint128(data, 160)
        protocol_fees_token1 = HexDecoder.decode_uint128(data, 192)

        return {
            "pool_address": contract_address.lower(),
            "sender": sender,
            "recipient": recipient,
            "amount0": amount0,
            "amount1": amount1,
            "sqrt_price_x96": sqrt_price_x96,
            "liquidity": liquidity,
            "tick": tick,
            "protocol_fees_token0": protocol_fees_token0,
            "protocol_fees_token1": protocol_fees_token1,
        }

    def _create_event(
        self,
        event_name: str,
        log_index: int,
        tx_hash: str,
        block_number: int,
        contract_address: str,
        decoded_data: dict[str, Any],
        raw_topics: list[str],
        raw_data: str,
    ) -> SwapEventData | None:
        """Create SwapEventData from decoded data.

        Only creates events for Swap - returns None for other event types
        (Transfer, Mint, Burn, Collect) which are tracked but not returned.
        """
        _ = (log_index, tx_hash, block_number, raw_topics, raw_data)

        # Only create SwapEventData for actual Swap events
        if event_name != "Swap" or not decoded_data:
            return None

        return SwapEventData(
            pool=decoded_data.get("pool_address", contract_address),
            sender=decoded_data.get("sender", ""),
            recipient=decoded_data.get("recipient", ""),
            amount0=Decimal(decoded_data.get("amount0", 0)),
            amount1=Decimal(decoded_data.get("amount1", 0)),
            sqrt_price_x96=decoded_data.get("sqrt_price_x96", 0),
            liquidity=decoded_data.get("liquidity", 0),
            tick=decoded_data.get("tick", 0),
            protocol_fees_token0=decoded_data.get("protocol_fees_token0", 0),
            protocol_fees_token1=decoded_data.get("protocol_fees_token1", 0),
        )

    def _build_result(
        self,
        events: list[SwapEventData],
        receipt: dict[str, Any],
        tx_hash: str,
        block_number: int,
        tx_success: bool,
        **kwargs,
    ) -> ParseResult:
        """Build ParseResult from parsed events."""
        _ = receipt
        error = kwargs.get("error")

        if not tx_success or error:
            return ParseResult(
                success=False,
                error=error or "Transaction failed",
                transaction_hash=tx_hash,
                block_number=block_number,
            )

        logger.info(f"Parsed PancakeSwap V3 receipt: tx={tx_hash[:10]}..., swaps={len(events)}")

        return ParseResult(
            success=True,
            swaps=events,
            transaction_hash=tx_hash,
            block_number=block_number,
        )

    # =============================================================================
    # Extraction Methods (for Result Enrichment)
    # =============================================================================

    def build_extract_kwargs(
        self,
        *,
        field: str,
        bundle_metadata: dict[str, Any],
    ) -> dict[str, Any]:
        """Return PancakeSwapV3-owned kwargs for ResultEnricher extraction calls.

        VIB-3164: the compiler records full token identity
        (``from_token`` / ``to_token`` dicts with address, symbol, decimals —
        see ``UniswapV3Compiler.compile_swap``) in ``ActionBundle.metadata``.
        Threading it here lets ``_resolve_swap_decimals`` resolve decimals when
        the TokenResolver misses, instead of dropping the row or emitting an
        unresolved-input row.

        Native-token entries are skipped: the receipt's Transfer events carry
        the wrapped token's address, so a native entry can never match by
        address, and its decimals (18) equal the fallback anyway.
        """
        if field != "swap_amounts":
            return {}
        meta: dict[str, dict[str, Any]] = {}
        for metadata_key, slot in (("from_token", "token_in"), ("to_token", "token_out")):
            raw = bundle_metadata.get(metadata_key)
            if not isinstance(raw, dict) or raw.get("is_native"):
                continue
            address = raw.get("address")
            decimals = raw.get("decimals")
            if not address or decimals is None:
                continue
            try:
                decimals_int = int(decimals)
            except (TypeError, ValueError):
                logger.debug("Could not coerce %s.decimals=%r to int; skipping hint", metadata_key, decimals)
                continue
            meta[slot] = {
                "address": str(address).lower(),
                "symbol": str(raw.get("symbol") or ""),
                "decimals": decimals_int,
            }
        return {"swap_token_meta": meta} if meta else {}

    @staticmethod
    def _build_hint_map(
        swap_token_meta: dict[str, dict[str, Any]] | None,
    ) -> dict[str, tuple[str, int]]:
        """Map compiler token metadata to ``{address: (symbol, decimals)}``."""
        hints: dict[str, tuple[str, int]] = {}
        if not swap_token_meta:
            return hints
        for slot in ("token_in", "token_out"):
            entry = swap_token_meta.get(slot)
            if not isinstance(entry, dict):
                continue
            address = entry.get("address")
            decimals = entry.get("decimals")
            if not address or decimals is None:
                continue
            try:
                hints[str(address).lower()] = (str(entry.get("symbol") or ""), int(decimals))
            except (TypeError, ValueError):
                logger.debug("Ignoring malformed token hint: %r", entry)
        return hints

    def extract_swap_amounts(
        self,
        receipt: dict[str, Any],
        *,
        expected_out: Decimal | None = None,
        swap_token_meta: dict[str, dict[str, Any]] | None = None,
    ) -> "SwapAmounts | None":
        """Extract swap amounts from a transaction receipt.

        Uses ERC-20 Transfer events to identify token addresses, then resolves
        actual decimals via the token resolver for accurate decimal conversion.

        Args:
            receipt: Transaction receipt dict with 'logs' and 'from' fields
            expected_out: VIB-3203 Phase B — pre-slippage-discount quote in
                human (Decimal) units, sourced from
                ``ActionBundle.metadata["expected_output_human"]`` by the
                ResultEnricher. When provided and positive, realized
                ``slippage_bps`` is computed. When absent, ``slippage_bps``
                stays ``None``.
            swap_token_meta: VIB-3164 — compiler-supplied token metadata threaded
                from ``build_extract_kwargs`` via the ResultEnricher hook.
                Shape: ``{"token_in": {"address": ..., "symbol": ..., "decimals": ...},
                "token_out": {...}}``. Hints win over the TokenResolver per address
                (address-keyed only — no direction fallback, because when
                ``_pick_swap_raw_amounts`` returns None there are no raw amounts to
                scale, so hints have nothing to rescue in that path).

        Returns:
            SwapAmounts dataclass if swap event found, None otherwise

        Implementation note:
            Phase 8.4 — this is a thin orchestrator over per-phase helpers.
            Each helper is individually testable and keeps CC bounded.
            Preserves:
            (1) wallet-level "first transfer out / last transfer in" semantics
                for multi-hop disambiguation,
            (2) SwapAmounts field surface,
            (3) amount0/amount1 sign conventions (the parser is wallet-transfer
                based and does not inspect Swap-event signs here — that path
                runs on parse_receipt).
        """
        try:
            if not self._swap_status_ok(receipt):
                return None

            # CodeRabbit review (PR #1798): guard against transfer-only
            # receipts that happen to contain one wallet-out and one wallet-in
            # ERC-20 Transfer but NO PancakeSwap V3 Swap event. Without this
            # check, a plain ERC-20 transfer or an LP-add receipt (which can
            # legitimately produce wallet Transfer logs) would be misclassified
            # as a swap and pollute downstream PnL. Enforces the method-contract
            # invariant "returns None if no swap event found".
            if not self._has_pcs_swap_log(receipt):
                return None

            wallet = self._swap_wallet(receipt)
            if not wallet:
                return None

            transfers = self._collect_wallet_transfers(receipt, wallet)
            seed = self._pick_swap_raw_amounts(transfers)
            if seed is None:
                return None

            hint_by_addr = self._build_hint_map(swap_token_meta)
            decimals = self._resolve_swap_decimals(seed, hint_by_addr)
            if decimals is None:
                return None

            return self._build_swap_amounts(seed, decimals, expected_out)

        except Exception as e:
            logger.warning(f"Failed to extract swap amounts: {e}")
            return None

    # ------------------------------------------------------------------
    # extract_swap_amounts — per-phase helpers (Phase 8.4 refactor).
    # Each is small, single-purpose, and independently testable.
    # ------------------------------------------------------------------

    @staticmethod
    def _swap_status_ok(receipt: dict[str, Any]) -> bool:
        """Return True iff ``receipt['status']`` is success (1).

        Accepts int, decimal string, and hex string ('0x0' / '0x1').
        """
        status = receipt.get("status", 0)
        if isinstance(status, str):
            status = int(status, 16) if status.startswith("0x") else int(status)
        return status == 1

    def _has_pcs_swap_log(self, receipt: dict[str, Any]) -> bool:
        """Return True iff the receipt contains at least one PCS V3 Swap log.

        Cheap topic-only scan — does NOT decode the log data. Gatekeeps the
        ``extract_swap_amounts`` pipeline against transfer-only receipts
        (e.g. a plain ERC-20 Transfer, or an LP add that still has wallet
        Transfer logs) so they cannot be misclassified as swaps.
        """
        swap_topic = self._normalize_topic(EVENT_TOPICS["Swap"])
        for log in receipt.get("logs", []) or []:
            topics = log.get("topics") or []
            if not topics:
                continue
            if self._normalize_topic(topics[0]) == swap_topic:
                return True
        return False

    def _swap_wallet(self, receipt: dict[str, Any]) -> str:
        """Return the normalized wallet address from ``receipt['from']`` or ''."""
        raw_wallet = receipt.get("from") or ""
        return self._normalize_topic(raw_wallet) if raw_wallet else ""

    def _collect_wallet_transfers(
        self,
        receipt: dict[str, Any],
        wallet: str,
    ) -> "_WalletTransfers":
        """Scan logs once; split ERC-20 Transfers into wallet-out and wallet-in.

        Per-log parsing is delegated to ``_parse_transfer_log`` so this loop
        stays a thin classifier. Malformed Transfer data (non-hex, truncated)
        is silently skipped by the parser — better to produce a partial-but-
        sane result than crash on a stray log from an unrelated contract.
        """
        transfer_topic = self._normalize_topic(EVENT_TOPICS["Transfer"])
        transfers_from_wallet: list[tuple[str, int]] = []
        transfers_to_wallet: list[tuple[str, int]] = []

        for log in receipt.get("logs", []):
            parsed = self._parse_transfer_log(log, transfer_topic)
            if parsed is None:
                continue
            log_from, log_to, token_address, amount = parsed
            if log_from == wallet:
                transfers_from_wallet.append((token_address, amount))
            if log_to == wallet:
                transfers_to_wallet.append((token_address, amount))

        return _WalletTransfers(
            from_wallet=transfers_from_wallet,
            to_wallet=transfers_to_wallet,
        )

    def _parse_transfer_log(
        self,
        log: dict[str, Any],
        transfer_topic: str,
    ) -> tuple[str, str, str, int] | None:
        """Decode one ERC-20 Transfer log into (from, to, token, amount).

        Returns None when the log is not an ERC-20 Transfer OR when the data
        cannot be decoded. Extracted from ``_collect_wallet_transfers`` so the
        hot-path loop stays CC-small and each branch is independently testable.
        """
        topics = log.get("topics", [])
        if not topics or len(topics) < 3:
            return None
        if self._normalize_topic(topics[0]) != transfer_topic:
            return None

        data = HexDecoder.normalize_hex(log.get("data", ""))
        if not data:
            return None
        try:
            amount = HexDecoder.decode_uint256(data, 0)
        except (ValueError, IndexError):
            return None

        log_from = HexDecoder.topic_to_address(topics[1])
        log_to = HexDecoder.topic_to_address(topics[2])
        raw_token = log.get("address") or ""
        token_address = self._normalize_topic(raw_token) if raw_token else ""
        return log_from, log_to, token_address, amount

    @staticmethod
    def _pick_swap_raw_amounts(
        transfers: "_WalletTransfers",
    ) -> "_SwapSeed | None":
        """Pick raw (amount, token) pairs for a wallet-level swap.

        Multi-hop semantics: wallet pays the FIRST token it transfers out,
        receives the LAST token it transfers in. That covers both direct swaps
        (1 in, 1 out) and router-orchestrated multi-hops where intermediate
        transfers traverse router / pool contracts (and thus are not tied to
        the wallet).

        Returns None when no wallet-in transfer was seen OR when the resulting
        ``amount_out`` is zero (current PCS V3 behavior — zero output is
        treated as "no swap signal").
        """
        if not transfers.from_wallet or not transfers.to_wallet:
            return None

        token_in_addr, amount_in = transfers.from_wallet[0]
        token_out_addr, amount_out = transfers.to_wallet[-1]

        if amount_out == 0:
            return None

        return _SwapSeed(
            token_in=token_in_addr,
            token_out=token_out_addr,
            amount_in=amount_in,
            amount_out=amount_out,
        )

    def _resolve_swap_decimals(
        self,
        seed: "_SwapSeed",
        hint_by_addr: "dict[str, tuple[str, int]] | None" = None,
    ) -> "_SwapDecimals | None":
        """Resolve (in, out) decimals; fail closed only when OUT is unknown.

        VIB-3164: ``hint_by_addr`` (address -> (symbol, decimals)) is consulted
        per side before the TokenResolver. Compiler hints are intent-specific
        compile-time facts and win over the resolver for the same address.
        Address-keyed only — no direction fallback (if ``_pick_swap_raw_amounts``
        returned a seed we already have addresses from wallet Transfers, so
        directional hints are moot).

        Historical invariant (preserved):
        - Output decimals unknown -> return None (would corrupt PnL).
        - Input decimals unknown  -> proceed with ``decimals_in = None``;
          ``_build_swap_amounts`` emits ``None`` (NOT ``Decimal(0)``) for
          ``amount_in_decimal`` and ``effective_price`` so downstream
          accounting can distinguish "unmeasured" from "measured zero"
          (the "Empty != zero" invariant from docs/internal/blueprints/27-accounting.md).
          ``amount_in_decimal_resolved=False`` continues to mark the row.
        """
        addr_in = seed.token_in.lower() if seed.token_in else ""
        addr_out = seed.token_out.lower() if seed.token_out else ""

        if hint_by_addr and addr_in in hint_by_addr:
            decimals_in: int | None = hint_by_addr[addr_in][1]
        else:
            decimals_in = self._resolve_decimals(seed.token_in)

        if hint_by_addr and addr_out in hint_by_addr:
            decimals_out: int | None = hint_by_addr[addr_out][1]
        else:
            decimals_out = self._resolve_decimals(seed.token_out)

        if decimals_out is None:
            logger.warning("Cannot compute swap amounts: output token decimals unknown")
            return None
        return _SwapDecimals(decimals_in=decimals_in, decimals_out=decimals_out)

    @staticmethod
    def _build_swap_amounts(
        seed: "_SwapSeed",
        decimals: "_SwapDecimals",
        expected_out: Decimal | None,
    ) -> "SwapAmounts":
        """Assemble the final SwapAmounts, including realized slippage."""
        from almanak.framework.execution.extracted_data import SwapAmounts

        # "Empty != zero" invariant (docs/internal/blueprints/27-accounting.md):
        # When input decimals could not be resolved, we cannot compute
        # ``amount_in_decimal`` -- emit ``None`` (unmeasured), NOT
        # ``Decimal(0)`` (measured zero). The raw integer ``amount_in``
        # is still preserved so the row can be emitted gracefully.
        amount_in_decimal: Decimal | None
        if seed.amount_in and decimals.decimals_in is not None:
            amount_in_decimal = Decimal(seed.amount_in) / Decimal(10**decimals.decimals_in)
        else:
            amount_in_decimal = None

        amount_out_decimal = Decimal(seed.amount_out) / Decimal(10**decimals.decimals_out)

        # ``effective_price`` is unmeasurable when ``amount_in_decimal`` is
        # unmeasured -- emit ``None``, never substitute a sentinel zero.
        # The ledger writer / swap_handler already handle ``None`` via the
        # existing ``is not None`` guards.
        effective_price: Decimal | None
        if amount_in_decimal is not None and amount_in_decimal > 0:
            effective_price = amount_out_decimal / amount_in_decimal
        else:
            effective_price = None

        # VIB-3203 Phase B: realized slippage when enricher supplies a quote.
        slippage_bps: int | None = None
        if expected_out is not None and expected_out > 0 and amount_out_decimal > 0:
            realized = (expected_out - amount_out_decimal) / expected_out
            slippage_bps = int(realized * Decimal(10_000))

        # ``amount_in_decimal_resolved=False`` flags the asymmetric
        # fail-soft case where input decimals were unknown. Combined with
        # ``amount_in_decimal=None`` and ``effective_price=None`` this
        # preserves the "Empty != zero" invariant -- downstream consumers
        # see "unmeasured", not a literal zero.
        return SwapAmounts(
            amount_in=seed.amount_in,
            amount_out=seed.amount_out,
            amount_in_decimal=amount_in_decimal,
            amount_out_decimal=amount_out_decimal,
            effective_price=effective_price,
            slippage_bps=slippage_bps,
            expected_out_decimal=expected_out,
            token_in=seed.token_in or None,
            token_out=seed.token_out or None,
            amount_in_decimal_resolved=decimals.decimals_in is not None,
            amount_out_decimal_resolved=True,
        )

    def _resolve_decimals(self, token_address: str) -> int | None:
        """Resolve token decimals via the token resolver.

        Returns None if the resolver is unavailable or the token is unknown.
        """
        if not token_address:
            return None
        try:
            from almanak.framework.data.tokens import get_token_resolver

            resolver = get_token_resolver()
            token = resolver.resolve(token_address, self.chain)
            return token.decimals
        except Exception as e:
            logger.warning(f"Could not resolve decimals for {token_address}: {e}")
            return None

    def extract_position_id(self, receipt: dict[str, Any]) -> int | None:
        """Extract LP position ID (NFT tokenId) from a transaction receipt.

        Looks for ERC-721 Transfer events from the NonfungiblePositionManager
        where from=address(0), indicating a mint.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            Position ID (tokenId) if found, None otherwise
        """
        try:
            logs = receipt.get("logs", [])
            if not logs:
                return None

            position_manager = POSITION_MANAGER_ADDRESSES.get(self.chain, "").lower()
            if not position_manager:
                position_manager = "0x46A15B0b27311cedF172AB29E4f4766fbE7F4364".lower()

            transfer_topic = EVENT_TOPICS["Transfer"].lower()

            for log in logs:
                topics = log.get("topics", [])
                address = log.get("address", "")

                if isinstance(address, bytes):
                    address = "0x" + address.hex()
                address = str(address).lower()

                if address != position_manager:
                    continue

                if len(topics) < 4:
                    continue

                first_topic = topics[0]
                if isinstance(first_topic, bytes):
                    first_topic = "0x" + first_topic.hex()
                first_topic = str(first_topic).lower()

                if first_topic != transfer_topic:
                    continue

                from_topic = topics[1]
                if isinstance(from_topic, bytes):
                    from_topic = "0x" + from_topic.hex()
                from_topic = str(from_topic).lower()

                if from_topic != ZERO_ADDRESS_PADDED:
                    continue

                token_id_topic = topics[3]
                if isinstance(token_id_topic, bytes):
                    token_id_topic = "0x" + token_id_topic.hex()
                token_id_topic = str(token_id_topic)

                try:
                    token_id = int(token_id_topic, 16)
                    logger.info(f"Extracted PancakeSwap V3 LP position ID: {token_id}")
                    return token_id
                except (ValueError, TypeError):
                    continue

            return None

        except Exception as e:
            logger.warning(f"Failed to extract position ID: {e}")
            return None

    def extract_tick_lower(self, receipt: dict[str, Any]) -> int | None:
        """Extract tick lower from LP mint transaction receipt.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            Tick lower value if found, None otherwise
        """
        try:
            logs = receipt.get("logs", [])
            if not logs:
                return None

            mint_topic = EVENT_TOPICS["Mint"].lower()

            for log in logs:
                topics = log.get("topics", [])
                if len(topics) < 4:
                    continue

                first_topic = topics[0]
                if isinstance(first_topic, bytes):
                    first_topic = "0x" + first_topic.hex()
                first_topic = str(first_topic).lower()

                if first_topic != mint_topic:
                    continue

                tick_lower_topic = topics[2]
                if isinstance(tick_lower_topic, bytes):
                    tick_lower_topic = "0x" + tick_lower_topic.hex()
                tick_lower_topic = str(tick_lower_topic)

                tick_lower = HexDecoder.decode_int24(tick_lower_topic, 0)
                return tick_lower

            return None

        except Exception as e:
            logger.warning(f"Failed to extract tick_lower: {e}")
            return None

    def extract_tick_upper(self, receipt: dict[str, Any]) -> int | None:
        """Extract tick upper from LP mint transaction receipt.

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            Tick upper value if found, None otherwise
        """
        try:
            logs = receipt.get("logs", [])
            if not logs:
                return None

            mint_topic = EVENT_TOPICS["Mint"].lower()

            for log in logs:
                topics = log.get("topics", [])
                if len(topics) < 4:
                    continue

                first_topic = topics[0]
                if isinstance(first_topic, bytes):
                    first_topic = "0x" + first_topic.hex()
                first_topic = str(first_topic).lower()

                if first_topic != mint_topic:
                    continue

                tick_upper_topic = topics[3]
                if isinstance(tick_upper_topic, bytes):
                    tick_upper_topic = "0x" + tick_upper_topic.hex()
                tick_upper_topic = str(tick_upper_topic)

                tick_upper = HexDecoder.decode_int24(tick_upper_topic, 0)
                return tick_upper

            return None

        except Exception as e:
            logger.warning(f"Failed to extract tick_upper: {e}")
            return None

    def extract_liquidity(self, receipt: dict[str, Any]) -> int | None:
        """Extract liquidity from LP mint transaction receipt.

        Mint event data layout (non-indexed fields, 32-byte padded):
        - sender (address): offset 0
        - amount (uint128): offset 32
        - amount0 (uint256): offset 64
        - amount1 (uint256): offset 96

        Args:
            receipt: Transaction receipt dict with 'logs' field

        Returns:
            Liquidity amount if found, None otherwise
        """
        try:
            logs = receipt.get("logs", [])
            if not logs:
                return None

            mint_topic = EVENT_TOPICS["Mint"].lower()

            for log in logs:
                topics = log.get("topics", [])
                if len(topics) < 4:
                    continue

                first_topic = topics[0]
                if isinstance(first_topic, bytes):
                    first_topic = "0x" + first_topic.hex()
                first_topic = str(first_topic).lower()

                if first_topic != mint_topic:
                    continue

                data = HexDecoder.normalize_hex(log.get("data", ""))
                if not data or data == "0x":
                    continue

                # Liquidity is at offset 32 (after sender address at offset 0)
                liquidity = HexDecoder.decode_uint128(data, 32)
                return liquidity

            return None

        except Exception as e:
            logger.warning(f"Failed to extract liquidity: {e}")
            return None

    def extract_lp_close_data(self, receipt: dict[str, Any]) -> "LPCloseData | None":
        """Extract LP close data from transaction receipt.

        Decodes the PancakeSwap V3 LP close pattern: ``decreaseLiquidity`` emits
        ``Burn`` (which carries the principal amounts), and ``collect`` emits
        ``Collect`` (which carries principal PLUS earned fees). The accrued
        fees are the difference between the two. Mirrors the Uniswap V3
        implementation since PancakeSwap V3 is a direct UV3 fork at the pool
        contract level.

        Burn event: Burn(address indexed owner, int24 indexed tickLower,
                         int24 indexed tickUpper, uint128 amount,
                         uint256 amount0, uint256 amount1)
        - data layout: amount (uint128, left-padded to 32B)
                       ‖ amount0 (uint256) ‖ amount1 (uint256)

        Collect event: Collect(address indexed owner, address recipient,
                               int24 indexed tickLower, int24 indexed tickUpper,
                               uint128 amount0, uint128 amount1)
        - ``owner``, ``tickLower``, ``tickUpper`` are indexed (3 topics + topic0).
          ``recipient`` is non-indexed — it occupies the first 32-byte data
          slot, so amount0/amount1 start at offsets 32 and 64.

        For a fee-only ``collect()`` (no decreaseLiquidity in the same TX), there
        is no Burn event. We treat the full Collect amounts as fees, principal=0.

        VIB-4305: captures ``pool_address`` from the Burn event emitter so the
        registry-payload builder can stamp the LP_CLOSE position_registry row's
        ``pool_address`` field (semantic_grouping_key anchor) without an
        off-chain RPC. Mirrors the Uniswap V3 close-side capture from VIB-3940.

        Args:
            receipt: Transaction receipt dict with 'logs' field.

        Returns:
            LPCloseData if Burn or Collect event found, None otherwise.
        """
        from almanak.framework.execution.extracted_data import LPCloseData

        try:
            logs = receipt.get("logs", [])
            if not logs:
                return None

            collect_topic = EVENT_TOPICS["Collect"].lower()
            burn_topic = EVENT_TOPICS["Burn"].lower()

            collect_amount0 = 0
            collect_amount1 = 0
            burn_amount0 = 0
            burn_amount1 = 0
            burn_liquidity_total = 0
            saw_burn = False
            saw_collect = False
            # VIB-4305 / mirror VIB-3940: capture the pool address from the
            # Burn event emitter so the registry-payload builder has the
            # semantic_grouping_key anchor without an off-chain RPC.
            pool_address = ""

            for log in logs:
                topics = log.get("topics", [])
                if not topics:
                    continue

                first_topic = topics[0]
                if isinstance(first_topic, bytes):
                    first_topic = "0x" + first_topic.hex()
                first_topic = str(first_topic).lower()

                data = HexDecoder.normalize_hex(log.get("data", ""))

                if first_topic == collect_topic and len(topics) >= 4:
                    # data: recipient (32B padded address)
                    #       ‖ amount0 (uint128, 32B padded)
                    #       ‖ amount1 (uint128, 32B padded)
                    collect_amount0 += HexDecoder.decode_uint128(data, 32)
                    collect_amount1 += HexDecoder.decode_uint128(data, 64)
                    saw_collect = True

                elif first_topic == burn_topic and len(topics) >= 4:
                    # uint128 amount (padded) ‖ uint256 amount0 ‖ uint256 amount1
                    # Accumulate across multiple Burn logs (multicall LP_CLOSE).
                    burn_liquidity_total += HexDecoder.decode_uint128(data, 0)
                    burn_amount0 += HexDecoder.decode_uint256(data, 32)
                    burn_amount1 += HexDecoder.decode_uint256(data, 64)
                    saw_burn = True
                    if not pool_address:
                        # Burn is emitted by the pool itself — its emitter
                        # address IS the pool. Capture once on the first Burn.
                        addr = log.get("address", "")
                        if isinstance(addr, bytes):
                            addr = "0x" + addr.hex()
                        if addr:
                            pool_address = str(addr).lower()

            if not (saw_collect or saw_burn):
                return None

            liquidity_removed = burn_liquidity_total if saw_burn else None

            # Fees attribution from a single receipt — see
            # ``almanak/connectors/uniswap_v3/receipt_parser.py``
            # for the full rationale. Parser returns its best single-receipt
            # understanding; the aggregate layer
            # (``ResultEnricher._select_preferred_aggregate``) disambiguates
            # LP_COLLECT_FEES (collect-only, no decrease sibling) from
            # split-tx LP_CLOSE (collect-only with a decrease sibling) and
            # overrides fees in the second case.
            if saw_collect:
                fees0: int | None = max(collect_amount0 - burn_amount0, 0)
                fees1: int | None = max(collect_amount1 - burn_amount1, 0)
            else:
                # Burn-only receipt (no Collect): principal is observed, fees
                # are unmeasured. VIB-4470 / blueprint 27 §Empty ≠ Zero.
                fees0 = None
                fees1 = None

            # Try to recover ``current_tick`` from any Swap event in the same
            # receipt (multicall close that includes a router swap will emit
            # one on the pool). When absent, the runner's slot0() fallback
            # will fill the field after parsing.
            current_tick = self._current_tick_from_swap_event(logs, pool_address) if pool_address else None

            # ``source`` tags the receipt shape for the aggregator's
            # preferred-source picker (VIB-4310,
            # ``_AGGREGATE_FIELDS["lp_close_data"] = "collect"``). Convention
            # mirrors Aerodrome Slipstream — see uniswap_v3/receipt_parser.py
            # for full rationale.
            source = "collect" if saw_collect else "decrease_liquidity"

            return LPCloseData(
                amount0_collected=collect_amount0 if saw_collect else burn_amount0,
                amount1_collected=collect_amount1 if saw_collect else burn_amount1,
                fees0=fees0,
                fees1=fees1,
                liquidity_removed=liquidity_removed,
                current_tick=current_tick,
                pool_address=pool_address,
                source=source,
            )

        except Exception as e:
            logger.warning(f"Failed to extract lp_close_data: {e}")
            return None

    # =============================================================================
    # extract_lp_open_data — PancakeSwap V3 LP_OPEN payload extraction
    # (mirrors uniswap_v3 / aerodrome — see VIB-3887 / VIB-3893)
    # =============================================================================

    def extract_lp_open_data(self, receipt: dict[str, Any]) -> "LPOpenData | None":  # noqa: C901
        """Extract LP open data from a PancakeSwap V3 mint receipt.

        Looks for ``IncreaseLiquidity`` events emitted by the PancakeSwap V3
        NonfungiblePositionManager when an LP position is opened or topped
        up. The event signature is identical to Uniswap V3's NPM::

            IncreaseLiquidity(
                uint256 indexed tokenId,
                uint128 liquidity,
                uint256 amount0,
                uint256 amount1,
            )

        Note: PancakeSwap V3's ``Swap`` event has 9 parameters (vs 7 for
        Uniswap V3 — the extra two are ``protocolFeesToken{0,1}``), so
        ``current_tick`` is recovered from the Swap event at the same
        slot offset as UV3 (offset 128 = 5th 32-byte slot) — the layout
        ``amount0 | amount1 | sqrtPriceX96 | liquidity | tick`` is
        identical for the first 5 slots; the protocol-fee slots come
        after and don't shift the tick offset.

        Behaviour contract (matches the Uniswap V3 baseline):

        - Returns ``LPOpenData`` populated with the raw on-chain ints
          (``position_id``, ``liquidity``, ``amount0``, ``amount1``).
          The accounting handler is responsible for decimal-scaling.
        - Returns ``None`` when no ``IncreaseLiquidity`` log is present.
        - No outer ``try/except`` — the fail-closed variant
          ``extract_lp_open_data_result`` distinguishes parser crash vs.
          missing event per VIB-3159 / Blueprint 19.
        - Fail-loud on unsupported chains (warn + return None). NEVER
          silently default to a known-chain NPM — that would mis-attribute
          logs the moment PCS deploys with a different address.

        Args:
            receipt: Transaction receipt dict with 'logs' field.

        Returns:
            ``LPOpenData`` if an ``IncreaseLiquidity`` event is present,
            ``None`` otherwise.
        """
        from almanak.framework.execution.extracted_data import LPOpenData

        logs = receipt.get("logs") or []
        if not logs:
            return None

        chain_key = (self.chain or "").lower()
        position_manager = POSITION_MANAGER_ADDRESSES.get(chain_key)
        if not position_manager:
            # Fail loud on unsupported chains rather than defaulting to one
            # of the known NPMs. A silent fallback would mis-attribute logs
            # the moment PCS deploys with a different address (the same
            # trap that bit early Uniswap V3 multi-chain expansion).
            logger.warning(
                "PancakeSwap V3 NPM not registered for chain %r — extend "
                "almanak.connectors.pancakeswap_v3.addresses.PANCAKESWAP_V3[<chain>]['nft']",
                chain_key,
            )
            return None

        increase_topic = EVENT_TOPICS["IncreaseLiquidity"].lower()
        mint_topic = EVENT_TOPICS["Mint"].lower()

        # Track the most recent Pool Mint emitted with owner == NPM so the
        # next matching IncreaseLiquidity claims ITS ticks (and pool
        # address) — a multi-position bundle won't cross-contaminate.
        last_npm_mint: dict[str, Any] | None = None

        for log in logs:
            if hasattr(log, "get"):
                topics = log.get("topics", [])
                address = log.get("address", "")
                data = log.get("data", "")
            else:
                topics = getattr(log, "topics", [])
                address = getattr(log, "address", "")
                data = getattr(log, "data", "")

            if isinstance(address, bytes):
                address = "0x" + address.hex()
            address = str(address).lower()

            if not topics:
                continue

            first_topic = topics[0]
            if isinstance(first_topic, bytes):
                first_topic = "0x" + first_topic.hex()
            first_topic = str(first_topic).lower()
            if not first_topic.startswith("0x"):
                first_topic = "0x" + first_topic

            # Track the most recent Pool Mint emitted with owner == NPM.
            if first_topic == mint_topic and len(topics) >= 4:
                if self._mint_owner_matches_npm(topics, position_manager):
                    last_npm_mint = log
                continue

            if address != position_manager:
                continue

            if len(topics) < 2:
                continue

            if first_topic != increase_topic:
                continue

            token_id_topic = topics[1]
            if isinstance(token_id_topic, bytes):
                token_id_topic = "0x" + token_id_topic.hex()
            token_id_topic = str(token_id_topic)
            if not token_id_topic.startswith("0x"):
                token_id_topic = "0x" + token_id_topic

            try:
                token_id = int(token_id_topic, 16)
            except (ValueError, TypeError):
                continue

            normalized = HexDecoder.normalize_hex(data)
            if not normalized or normalized == "0x":
                continue

            # IncreaseLiquidity data layout: liquidity (uint128, left-padded
            # to 32 bytes), amount0 (uint256), amount1 (uint256). Decode
            # failures here represent a malformed receipt (NPM emitted a
            # structurally-invalid IncreaseLiquidity log), NOT a missing
            # event — propagate so ``extract_lp_open_data_result`` wraps as
            # ``ExtractError`` rather than ``ExtractMissing`` (VIB-3159 /
            # Blueprint 19 fail-closed disambiguation).
            #
            # Length guard (Codex P2 on PR #2248): ``HexDecoder.decode_uint256``
            # silently returns ``0`` when reading past the end of the
            # normalized string, so a truncated payload (fewer than 3 * 64
            # = 192 hex chars after the ``0x``) would record amount0=0 /
            # amount1=0 instead of raising. Reject up front so ``_result``
            # wraps the row as ``ExtractError`` rather than admitting a
            # measured-zero (CLAUDE.md §Accounting "Empty ≠ Zero").
            stripped = normalized[2:] if normalized.startswith("0x") else normalized
            if len(stripped) < 3 * 64:
                raise ValueError(f"Truncated IncreaseLiquidity payload: {len(stripped)} hex chars, expected >= 192")
            try:
                liquidity = HexDecoder.decode_uint128(normalized, 0)
                amount0 = HexDecoder.decode_uint256(normalized, 32)
                amount1 = HexDecoder.decode_uint256(normalized, 64)
            except Exception as exc:
                raise ValueError(f"Malformed IncreaseLiquidity payload at offset 0-96: {exc}") from exc

            tick_lower, tick_upper = self._ticks_from_pool_mint(last_npm_mint)

            pool_address = ""
            if last_npm_mint is not None:
                addr_attr = (
                    last_npm_mint.get("address")
                    if hasattr(last_npm_mint, "get")
                    else getattr(last_npm_mint, "address", "")
                )
                if isinstance(addr_attr, bytes):
                    addr_attr = "0x" + addr_attr.hex()
                pool_address = str(addr_attr).lower()

            current_tick = self._current_tick_from_swap_event(logs, pool_address)

            logger.info(
                f"Extracted PancakeSwap V3 LP open data: tokenId={token_id} "
                f"liquidity={liquidity} amount0={amount0} amount1={amount1} "
                f"ticks=[{tick_lower}, {tick_upper}] current_tick={current_tick}"
            )
            return LPOpenData(
                position_id=token_id,
                tick_lower=tick_lower,
                tick_upper=tick_upper,
                liquidity=liquidity,
                amount0=amount0,
                amount1=amount1,
                current_tick=current_tick,
                pool_address=pool_address,
            )

        return None

    @staticmethod
    def _mint_owner_matches_npm(topics: list[Any], npm_address: str) -> bool:
        """Return True iff the Pool Mint event's ``owner`` indexed topic == NPM."""
        if len(topics) < 2:
            return False
        owner_addr = HexDecoder.topic_to_address(topics[1])
        if not owner_addr:
            return False
        return owner_addr.lower() == npm_address.lower()

    @staticmethod
    def _ticks_from_pool_mint(mint_log: dict[str, Any] | None) -> tuple[int | None, int | None]:
        """Decode (tickLower, tickUpper) from a PancakeSwap V3 Pool Mint log.

        Mint(address sender, address indexed owner, int24 indexed tickLower,
             int24 indexed tickUpper, uint128 amount, uint256 amount0,
             uint256 amount1) — ticks live at topics[2] and topics[3] as
        indexed int24 values right-padded into 32-byte topics.
        """
        if mint_log is None:
            return (None, None)
        topics = mint_log.get("topics", []) if hasattr(mint_log, "get") else getattr(mint_log, "topics", [])
        if len(topics) < 4:
            return (None, None)

        def _decode_indexed_int24(topic: Any) -> int | None:
            if isinstance(topic, bytes):
                topic = "0x" + topic.hex()
            topic = str(topic).lower()
            if not topic.startswith("0x"):
                topic = "0x" + topic
            try:
                v = int(topic, 16)
            except (ValueError, TypeError):
                return None
            # Indexed int24 is stored as a 32-byte two's-complement value;
            # sign-extend by inspecting the top bit of the original 256-bit
            # word (negative numbers come back as huge unsigned values).
            if v >= 2**255:
                v -= 2**256
            return v

        return (_decode_indexed_int24(topics[2]), _decode_indexed_int24(topics[3]))

    @staticmethod
    def _current_tick_from_swap_event(logs: list[Any], pool_address: str) -> int | None:
        """Decode post-swap current tick from a PancakeSwap V3 Swap event.

        PancakeSwap V3 Swap data layout (9 params, same first-5-slot shape
        as Uniswap V3's 7-param Swap — the two extra ``protocolFees`` slots
        come AFTER ``tick`` so they don't shift the tick offset)::

            amount0 (int256, 32B) + amount1 (int256, 32B)
            + sqrtPriceX96 (uint160, padded 32B)
            + liquidity (uint128, padded 32B)
            + tick (int24, sign-extended into 32B)
            + protocolFeesToken0 (uint128, padded 32B)
            + protocolFeesToken1 (uint128, padded 32B)

        Tick lives at byte offset 128 — identical to UV3.

        For multi-hop / bundled receipts with more than one Swap log on
        the same pool, we keep the LATEST tick — receipts come back from
        the RPC in logIndex order, so the final matching Swap carries the
        live post-swap tick the LP_OPEN sees. A single malformed log does
        not abort the scan; we ``continue`` and let later valid logs win.

        Returns None when no matching Swap event is present (pure NPM.mint
        LP_OPEN with pre-balanced amounts).
        """
        if not pool_address:
            return None
        pool_addr_lower = pool_address.lower()
        swap_topic = EVENT_TOPICS["Swap"].lower()
        latest_tick: int | None = None
        for log in logs:
            if hasattr(log, "get"):
                topics = log.get("topics", [])
                address = log.get("address", "")
                data = log.get("data", "")
            else:
                topics = getattr(log, "topics", [])
                address = getattr(log, "address", "")
                data = getattr(log, "data", "")

            if isinstance(address, bytes):
                address = "0x" + address.hex()
            if str(address).lower() != pool_addr_lower:
                continue
            if not topics:
                continue
            topic0 = topics[0]
            if isinstance(topic0, bytes):
                topic0 = "0x" + topic0.hex()
            topic0 = str(topic0).lower()
            if not topic0.startswith("0x"):
                topic0 = "0x" + topic0
            if topic0 != swap_topic:
                continue

            normalized = HexDecoder.normalize_hex(data)
            # PancakeSwap V3 Swap data is 7 × 64 = 448 hex chars. Demand at
            # least the first 5 slots (320 hex chars) so we can read tick
            # at offset 128. Truncated logs are skipped — keep scanning.
            if not normalized or len(normalized) < 5 * 64:
                continue
            try:
                latest_tick = HexDecoder.decode_int24(normalized, 128)
            except Exception:
                continue
        return latest_tick

    # =============================================================================
    # Fail-closed wrappers (VIB-3159 / Blueprint 19) — disambiguate
    # "parser crashed" from "no event present"
    # =============================================================================

    def _strict_parse(self, receipt: dict[str, Any]) -> ExtractResult[Any] | None:
        """Run ``parse_receipt`` and short-circuit with ``ExtractError`` if it
        reports a crash.

        Returns ``None`` when parsing succeeded (caller should proceed), or an
        ``ExtractError`` variant when it did not. This is the strict
        counterpart to the legacy ``extract_*`` methods, which silently
        swallow exceptions and return ``None`` — making the "benign missing"
        and "crashed parsing" cases indistinguishable (VIB-3159).
        """
        try:
            parsed = self.parse_receipt(receipt)
        except Exception as exc:  # noqa: BLE001 — malformed receipt shape
            return ExtractError(error=f"{type(exc).__name__}: {exc}", exception=exc)
        if not parsed.success:
            return ExtractError(error=parsed.error or "parse_receipt reported failure")
        return None

    def extract_lp_open_data_result(self, receipt: dict[str, Any]) -> ExtractResult["LPOpenData"]:
        """Fail-closed variant of :meth:`extract_lp_open_data` — see VIB-3159.

        Distinguishes "no IncreaseLiquidity event in receipt" (benign — e.g.
        an LP_OPEN that failed mid-bundle, or a non-NPM contract path) from
        "parser crashed on a malformed receipt". Both are returned as
        ``None`` by the legacy method, which forces the enricher to treat
        genuine parse failures as missing data — the same ghost-position
        class of bug VIB-3159 addresses for Uniswap V3.
        """
        err = self._strict_parse(receipt)
        if err is not None:
            return err
        try:
            value = self.extract_lp_open_data(receipt)
        except Exception as exc:  # noqa: BLE001
            return ExtractError(error=f"{type(exc).__name__}: {exc}", exception=exc)
        if value is None:
            return ExtractMissing(reason="no IncreaseLiquidity event from position manager")
        return ExtractOk(value=value)

    # =============================================================================
    # Registry Payload Extraction (VIB-4305 / T12 mirror of Uniswap V3)
    #
    # Composes the ``position_registry.payload`` dict for LP_OPEN / LP_CLOSE
    # so the runner's registry-mode write path populates ``position_registry``
    # for PancakeSwap LP positions. Without these methods the runner emits
    # ``Registry-mode skip: parser returned no LP_OPEN registry payload`` and
    # falls back to ``save_ledger_entry``, which leaves the Positions
    # dashboard empty for Pancake LPs (the exact gap VIB-4305 fixes).
    #
    # The Uniswap V3 implementation is the canonical template (PancakeSwap V3
    # is a direct UV3 fork at the NPM contract level — same IncreaseLiquidity
    # / DecreaseLiquidity / Burn / Collect / Mint event signatures). Helpers
    # ``_open_payload_disagrees`` / ``_build_close_receipt_payload`` /
    # ``_merge_open_payload_fields`` are imported from
    # ``almanak.connectors.uniswap_v3.receipt_parser`` rather than
    # duplicated — they operate on the receipt-only payload dict, which is
    # the same shape across V3 forks.
    # =============================================================================

    def _nft_manager_address(self) -> str:
        """Return the canonical PancakeSwap V3 NPM address for ``self.chain``.

        The address is part of the ``physical_identity_hash`` input tuple
        (per T08 invariant #1). It is a parser-side configuration constant —
        NOT an off-chain RPC call — so the hash stays receipt-derivable
        from the receipt + parser config alone. Returns the empty string
        when the chain is unsupported; the caller treats that as "fall back
        to accounting_only" rather than substituting a known-good NPM (the
        Uniswap V3 implementation defaults to mainnet NPM as a safety net;
        for PancakeSwap we prefer fail-loud — there is no single canonical
        NPM the way Uniswap V3 has one).
        """
        return POSITION_MANAGER_ADDRESSES.get(self.chain, "").lower()

    def _decreaseliquidity_token_id(self, receipt: dict[str, Any]) -> int | None:
        """Recover ``tokenId`` from a ``DecreaseLiquidity`` log on the close-side
        receipt.

        The PancakeSwap V3 NPM emits ``DecreaseLiquidity(uint256 indexed
        tokenId, …)`` on every ``decreaseLiquidity()`` call (identical
        signature to Uniswap V3). ``topics[1]`` is the indexed tokenId.
        Returns ``None`` if no such log is in the receipt or the NPM emitter
        doesn't match the configured chain — the close-side identity is
        then derivable only from strategy-supplied state, which is the
        legacy path the registry cutover is replacing. The caller treats
        ``None`` as "fall back to ``accounting_only`` for this intent" with
        an INFO log; no ``Decimal("0")`` substitution (Empty != Zero).
        """
        logs = receipt.get("logs") or []
        if not logs:
            return None

        decrease_topic = EVENT_TOPICS["DecreaseLiquidity"].lower()
        position_manager = self._nft_manager_address()
        if not position_manager:
            # Unsupported chain — fail loud rather than guessing.
            return None

        for log in logs:
            if hasattr(log, "get"):
                topics = log.get("topics", [])
                address = log.get("address", "")
            else:
                topics = getattr(log, "topics", [])
                address = getattr(log, "address", "")

            if isinstance(address, bytes):
                address = "0x" + address.hex()
            address = str(address).lower()

            if not topics or len(topics) < 2:
                continue

            first_topic = topics[0]
            if isinstance(first_topic, bytes):
                first_topic = "0x" + first_topic.hex()
            first_topic = str(first_topic).lower()
            if not first_topic.startswith("0x"):
                first_topic = "0x" + first_topic

            if first_topic != decrease_topic:
                continue
            if address != position_manager:
                continue

            token_id_topic = topics[1]
            if isinstance(token_id_topic, bytes):
                token_id_topic = "0x" + token_id_topic.hex()
            token_id_topic = str(token_id_topic)
            if not token_id_topic.startswith("0x"):
                token_id_topic = "0x" + token_id_topic
            try:
                return int(token_id_topic, 16)
            except (ValueError, TypeError):
                return None
        return None

    def extract_registry_payload_open(
        self,
        receipt: dict[str, Any],
        *,
        fee_tier: int | None = None,
    ) -> dict[str, Any] | None:
        """Build the LP_OPEN ``position_registry.payload`` dict.

        Wraps :meth:`extract_lp_open_data` and composes the canonical 8-key
        shape (plus optional ``fee_tier`` and the per-chain
        ``nft_manager_addr``). Returns ``None`` when any of the load-bearing
        identity fields are missing — the caller treats that as "fall back
        to accounting_only", per CLAUDE.md "Empty != Zero" (a zero-substituted
        token_id would silently corrupt the ``physical_identity_hash``).

        Args:
            receipt: Transaction receipt dict with ``logs`` field.
            fee_tier: Optional pool fee tier (e.g. ``500`` for 0.05%);
                forwarded from the intent's compile-time metadata. ``None``
                when unknown — the payload key stays absent rather than
                substituting ``0`` (Empty != Zero).

        Returns:
            ``dict`` JSON-serializable with the 8 (or 9 with fee_tier) keys
            ratified by PRD §Registry Data Shape and the T08 golden, OR
            ``None`` when the LP_OPEN data isn't extractable from the
            receipt.
        """
        lp_data = self.extract_lp_open_data(receipt)
        if lp_data is None:
            return None
        if lp_data.position_id is None or lp_data.position_id <= 0:
            # token_id is the identity anchor; a zero/negative value would
            # corrupt physical_identity_hash. Refuse to build the payload.
            return None
        if not lp_data.pool_address:
            # pool_address is the semantic_grouping_key anchor; missing it
            # would let two un-grouped rows in the same pool collide on
            # ix_registry_auto_mode. Refuse rather than emit a partial row.
            return None
        if lp_data.tick_lower is None or lp_data.tick_upper is None:
            # Range is part of the position's economic identity; missing
            # ticks would let teardown / rebalancing read malformed bounds.
            return None
        if lp_data.liquidity is None:
            return None

        nft_manager_addr = self._nft_manager_address()
        if not nft_manager_addr:
            # Unsupported chain — refuse the payload rather than stamp a
            # collision-prone empty NPM into ``physical_identity_hash``.
            return None

        payload: dict[str, Any] = {
            "token_id": str(lp_data.position_id),
            "pool_address": lp_data.pool_address.lower(),
            "tick_lower": lp_data.tick_lower,
            "tick_upper": lp_data.tick_upper,
            "liquidity": str(lp_data.liquidity),
            "amount0": str(lp_data.amount0) if lp_data.amount0 is not None else None,
            "amount1": str(lp_data.amount1) if lp_data.amount1 is not None else None,
            "nft_manager_addr": nft_manager_addr,
        }
        if fee_tier is not None and fee_tier > 0:
            payload["fee_tier"] = int(fee_tier)
        # Optional token symbol labels — Pancake parser doesn't take symbols
        # in its constructor today, but we forward them when present so a
        # future enrichment iteration plugs in without a parser-shape change.
        token0_symbol = getattr(self, "token0_symbol", None)
        token1_symbol = getattr(self, "token1_symbol", None)
        if token0_symbol:
            payload["_token0_label"] = token0_symbol
        if token1_symbol:
            payload["_token1_label"] = token1_symbol
        return payload

    def extract_registry_payload_close(
        self,
        receipt: dict[str, Any],
        *,
        open_payload: dict[str, Any] | None = None,
        fee_tier: int | None = None,
    ) -> dict[str, Any] | None:
        """Build the LP_CLOSE ``position_registry.payload`` dict.

        Reads the existing :meth:`extract_lp_close_data` output (Burn /
        Collect amounts) and the close-side ``DecreaseLiquidity`` event
        for the NFT ``token_id``, then composes the 13-key shape that
        the T08 ``lp_close/expected_registry_row.json`` golden specifies
        for Uniswap V3 (PancakeSwap V3 is a direct UV3 fork at the NPM
        contract level, so the shape is identical).

        Audit M1 (CodeRabbit): a real LP_CLOSE proves itself with
        DecreaseLiquidity on the receipt AND a Burn log carrying the
        pool address. A Collect-only receipt is NOT a close — it's a fee
        harvest. If we silently synthesized ``token_id`` / ``pool_address``
        from ``open_payload`` here, a Collect-only receipt or a malformed
        close would produce a "successful" close payload with stale
        OPEN-side anchors, and the registry would mark a still-open NFT
        as closed (the cutover spec D3.F6 silent-error class).

        The flow is:

        1. Decode close-side events (``extract_lp_close_data``) and the
           DecreaseLiquidity log (``_decreaseliquidity_token_id``).
        2. Verify the receipt-derived identity anchors are present and
           non-zero.
        3. Cross-check against ``open_payload`` if supplied — refuse on
           any disagreement (``_open_payload_disagrees``).
        4. Compose the receipt-only payload
           (``_build_close_receipt_payload``).
        5. Merge OPEN-time fields the close receipt cannot re-derive
           (``_merge_open_payload_fields``) — ticks, OPEN-time amounts,
           original mint liquidity, fee tier, token labels.
        6. Apply the ``fee_tier`` argument if open_payload didn't carry one.

        Returns ``None`` when the close-side identity anchors (token_id +
        pool_address) cannot be derived OR cross-checks fail. The caller
        treats that as "fall back to accounting_only" with an INFO log
        (no zero substitution).
        """
        # Reuse the V3-fork-shared helpers directly from uniswap_v3 so we
        # don't duplicate ~50 lines of payload composition / cross-check
        # logic. PancakeSwap V3 produces a receipt-only close payload of
        # identical shape (same Burn/Collect events, same NPM ABI) so the
        # helpers' inputs are shape-compatible.
        from almanak.connectors.uniswap_v3.receipt_parser import (
            UniswapV3ReceiptParser,
        )

        lp_close = self.extract_lp_close_data(receipt)
        if lp_close is None:
            return None
        token_id = self._decreaseliquidity_token_id(receipt)
        if token_id is None or token_id <= 0:
            return None
        pool_address = (lp_close.pool_address or "").lower()
        if not pool_address:
            return None
        if UniswapV3ReceiptParser._open_payload_disagrees(
            open_payload=open_payload,
            token_id=token_id,
            pool_address=pool_address,
        ):
            return None

        nft_manager_addr = self._nft_manager_address()
        if not nft_manager_addr:
            return None

        payload = UniswapV3ReceiptParser._build_close_receipt_payload(
            token_id=token_id,
            pool_address=pool_address,
            lp_close=lp_close,
            nft_manager_addr=nft_manager_addr,
        )
        UniswapV3ReceiptParser._merge_open_payload_fields(payload, open_payload)
        if fee_tier is not None and fee_tier > 0:
            payload.setdefault("fee_tier", int(fee_tier))
        return payload

    # =============================================================================
    # Protocol Fee Extraction (VIB-3204)
    # =============================================================================

    def extract_protocol_fees(
        self,
        receipt: dict[str, Any],
        *,
        fee_tier_bps: int | None = None,
    ) -> "ProtocolFees | None":
        """Extract DEX protocol fees from a PancakeSwap V3 swap receipt.

        PancakeSwap V3 is a Uniswap V3 fork with the same pip-based fee
        tiers. The fee tier is resolved at compile time and forwarded by
        the ResultEnricher via ``bundle_metadata["selected_fee_tier"]``
        (signature-introspection opt-in — see VIB-3203 / VIB-3204).

        VIB-3204 audit fix (Codex P1, pr-auditor Blocker #2): do NOT
        return ``ProtocolFees(total_usd=Decimal(0))`` when the fee is
        known-non-zero but USD conversion isn't available. That would
        systematically under-attribute swap costs — a silent accounting
        bug. Until a price oracle is plumbed through to this layer, this
        parser returns ``None`` (ExtractMissing semantic). Callers that
        want the in-token fee can derive it from
        ``amount_in_decimal * fee_tier_bps / 1_000_000`` using values
        already on ``result.swap_amounts``.

        Args:
            receipt: Transaction receipt dict with 'logs' field.
            fee_tier_bps: Pool fee tier in pips (e.g. 500 = 5 bps),
                forwarded by the enricher.

        Returns:
            ``None`` — USD conversion not available at this layer. A
            future iteration with price-oracle access will return a
            populated ``ProtocolFees``. Also returns ``None`` when no
            Swap event is present OR when ``fee_tier_bps`` is missing.
        """
        # ProtocolFees import kept local to preserve import order semantics
        # inherited from the pre-fix version of this method.
        from almanak.framework.execution.extracted_data import ProtocolFees  # noqa: F401

        if fee_tier_bps is None or fee_tier_bps <= 0:
            return None

        try:
            status = receipt.get("status", 0)
            if isinstance(status, str):
                status = int(status, 16) if status.startswith("0x") else int(status)
            if status != 1:
                return None

            swap_topic = self._normalize_topic(EVENT_TOPICS["Swap"])
            logs = receipt.get("logs", []) or []
            for log in logs:
                topics = log.get("topics") or []
                if not topics:
                    continue
                if self._normalize_topic(topics[0]) == swap_topic:
                    # Swap detected + valid fee_tier_bps, but we cannot
                    # convert to USD in this layer. Return None (unknown)
                    # rather than a misleading Decimal(0).
                    return None
            return None

        except Exception as e:
            logger.warning(f"Failed to extract protocol_fees: {e}")
            return None

    # Backward compatibility methods
    def parse_swap(self, log: dict[str, Any]) -> SwapEventData | None:
        """Parse a Swap event from a single log entry.

        Backward compatibility method.
        """
        topics = log.get("topics", [])
        data = log.get("data", "")
        pool_address = log.get("address", "")

        if isinstance(data, bytes):
            data = "0x" + data.hex()
        if isinstance(pool_address, bytes):
            pool_address = "0x" + pool_address.hex()

        # Decode using internal method
        data_normalized = HexDecoder.normalize_hex(data)
        decoded_data = self._decode_log_data("Swap", topics, data_normalized, pool_address)

        if not decoded_data:
            return None

        return self._create_event(
            event_name="Swap",
            log_index=log.get("logIndex", 0),
            tx_hash="",
            block_number=0,
            contract_address=pool_address,
            decoded_data=decoded_data,
            raw_topics=[],
            raw_data="",
        )

    def is_pancakeswap_event(self, topic: str | bytes) -> bool:
        """Check if a topic is a known PancakeSwap V3 event.

        Args:
            topic: Event topic (supports bytes, hex string with/without 0x, any case)

        Returns:
            True if topic is a known PancakeSwap V3 event
        """
        if isinstance(topic, bytes):
            topic = "0x" + topic.hex()
        else:
            topic = str(topic)
        if not topic.startswith("0x"):
            topic = "0x" + topic
        topic = topic.lower()
        return self.registry.is_known_event(topic) if self.registry else False

    def get_event_type(self, topic: str | bytes) -> PancakeSwapV3EventType:
        """Get the event type for a topic.

        Args:
            topic: Event topic (supports bytes, hex string with/without 0x, any case)

        Returns:
            Event type or UNKNOWN
        """
        if isinstance(topic, bytes):
            topic = "0x" + topic.hex()
        else:
            topic = str(topic)
        if not topic.startswith("0x"):
            topic = "0x" + topic
        topic = topic.lower()
        if self.registry is None:
            return PancakeSwapV3EventType.UNKNOWN
        return self.registry.get_event_type_from_topic(topic) or PancakeSwapV3EventType.UNKNOWN


__all__ = [
    "PancakeSwapV3ReceiptParser",
    "PancakeSwapV3EventType",
    "SwapEventData",
    "ParseResult",
    "EVENT_TOPICS",
    "TOPIC_TO_EVENT",
    "EVENT_NAME_TO_TYPE",
    "POSITION_MANAGER_ADDRESSES",
]
