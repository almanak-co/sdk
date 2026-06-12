"""Fluid DEX Receipt Parser.

The verified surface is SWAP extraction (Phase 1, VIB-5029): the pool's
``Swap(bool,uint256,uint256,address)`` event carries direction and exact
amounts (event topology verified on-chain at Phase 0 — VIB-5028 §V1.5:
two ERC-20 Transfers + two Liquidity-layer LogOperate legs + the pool's
Swap event). Fluid pools pair the chain's native token directly, so a swap
may have only one ERC-20 Transfer leg — the missing leg is native and is
reported with Fluid's ``0xEeee…`` sentinel.

The LP/position extraction methods below are pre-Phase-0 scaffolding for a
position model (DEX-pool ``operate()`` NFTs) that Phase 0 disproved —
direct pool LP is whitelist-gated and vault NFTs live on the VaultFactory.
They are retained only so existing enricher plumbing keeps degrading
gracefully; Phase 4 (VIB-5032) replaces them with the SmartLending /
smart-vault design. Do not build on them.
"""

import logging
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from almanak.connectors._strategy_base.base import HexDecoder
from almanak.connectors.fluid.sdk import FLUID_ADDRESSES, FLUID_NATIVE_TOKEN
from almanak.framework.execution.extracted_data import LPCloseData, SwapAmounts

logger = logging.getLogger(__name__)


# =============================================================================
# Event Topics
# =============================================================================

# LogOperate(uint256 indexed nftId, int256 token0Amt, int256 token1Amt, uint256 timestamp)
# keccak256("LogOperate(uint256,int256,int256,uint256)")
LOG_OPERATE_TOPIC = "0x097c4f958acb54c3329d17179c2bd01bf6bdcb853ae77ffb63cdae3de1ddf156"

# ERC-721 Transfer(address indexed from, address indexed to, uint256 indexed tokenId)
ERC721_TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

# Swap(bool swap0to1, uint256 amountIn, uint256 amountOut, address to)
# keccak256("Swap(bool,uint256,uint256,address)")
SWAP_TOPIC = "0xdc004dbca4ef9c966218431ee5d9133d337ad018dd5b5c5493722803f75c64f7"

# =============================================================================
# Vault NFT-CDP events (Phase 3, VIB-5031) — byte-verified constants from
# docs/internal/qa/fluid-vault-verification-2026-06-12.md (D2)
# =============================================================================

# VaultT1 LogOperate(address user_, uint256 nftId_, int256 colAmt_,
#                    int256 debtAmt_, address to_)
# keccak256("LogOperate(address,uint256,int256,int256,address)") — ZERO
# indexed params: all five fields ride in ``data`` (exactly 5 words).
# Signed int256 two's-complement deltas (the live repay decoded -200000000).
VAULT_LOG_OPERATE_TOPIC = "0xfef64760e30a41b9d5ba7dd65ff7236a61d89ed8b44c67a29e84db1a67513a1c"

# Liquidity-layer LogOperate(address,address,int256,int256,address,address,
# uint256,uint256) — a DIFFERENT event that appears (twice) in the same
# vault receipt. Corroboration only; never confuse with the vault event.
LIQUIDITY_LOG_OPERATE_TOPIC = "0x4d93b232a24e82b284ced7461bf4deacffe66759d5c24513e6f29e571ad78d15"

# VaultFactory NewPositionMinted(address indexed vault, address indexed owner,
# uint256 indexed nftId) — the factory's second, vault-scoped mint signal,
# usable to corroborate the ERC-721 Transfer (both come from the factory).
FACTORY_NEW_POSITION_MINTED_TOPIC = "0xfcc2278353c4cc5d54b742d7eee2d4a7abc22e4dc6213340088293860d502b51"

# Vault LogOperate payload: exactly 5 non-indexed words (user, nftId, colAmt,
# debtAmt, to) = 160 bytes = 320 hex chars. Anything else is truncated /
# foreign and is REJECTED, never partially decoded (fail closed).
_VAULT_LOG_OPERATE_DATA_HEX_LEN = 5 * 64

# ERC-4626 fToken lending events (VIB-5030). Standard vault events — same
# topics the morpho_vault (MetaMorpho) parser pins:
# Deposit(address indexed sender, address indexed owner, uint256 assets, uint256 shares)
DEPOSIT_4626_TOPIC = "0xdcbc1c05240f31ff3ad067ef1ee35ce4997762752e3a095284754544f4c709d7"
# Withdraw(address indexed sender, address indexed receiver, address indexed owner,
#          uint256 assets, uint256 shares)
WITHDRAW_4626_TOPIC = "0xfbde797d201c681b91056529119e0b02407c7bb96a4a2c75c01fc9667232c8db"

# Zero address — indicates minting (new position)
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"

# Fluid's native-token sentinel (pools pair raw native, not WETH).
# Lowercased for receipt-side comparisons; 18 decimals on every
# supported chain (ETH on arbitrum/base/ethereum, POL on polygon).
_FLUID_NATIVE_SENTINEL = FLUID_NATIVE_TOKEN.lower()
_NATIVE_DECIMALS = 18  # decimal-policy-exempt: native gas tokens (ETH/POL) are 18-decimal protocol invariants


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class FluidOperateEvent:
    """Parsed LogOperate event from a Fluid DEX transaction."""

    nft_id: int
    token0_amt: int  # Signed: positive = deposited, negative = withdrawn
    token1_amt: int  # Signed: positive = deposited, negative = withdrawn
    log_index: int = 0


@dataclass
class FluidSwapEvent:
    """Parsed Swap event from a Fluid DEX transaction."""

    swap0to1: bool
    amount_in: int
    amount_out: int
    to: str
    log_index: int = 0


@dataclass
class FluidVaultOperateEvent:
    """Parsed VaultT1 ``LogOperate`` event (VIB-5031).

    The SIGNED deltas are the receipt-truth amounts for accounting — never
    inferred from ERC-20 Transfer topology, because native-collateral legs
    produce no Transfer (the V3.5 close paid raw ETH with no ERC-20 log),
    and the int-min sentinel's true amount is only knowable from this event.
    """

    nft_id: int
    col_delta: int  # signed, raw collateral token units
    debt_delta: int  # signed, raw debt token units
    vault: str  # log emitter (lowercased)
    log_index: int = 0


def _log_emitter(log: dict[str, Any]) -> str:
    """Normalized lowercase emitting address of a log."""
    address = log.get("address", "")
    if isinstance(address, bytes):
        address = "0x" + address.hex()
    return str(address).lower()


def _vault_factory_address(chain: str) -> str:
    """Lowercased VaultFactory address for ``chain`` (the ERC-721 home)."""
    return FLUID_ADDRESSES.get(chain, {}).get("vault_factory", "").lower()


@dataclass
class FluidVaultParseResult:
    """Result of parsing a Fluid vault ``operate()`` receipt."""

    success: bool
    transaction_hash: str = ""
    block_number: int = 0
    #: Factory-gated mint nftId (Transfer 0x0 -> wallet emitted by the
    #: VaultFactory ONLY); None when the receipt contains no genuine mint.
    minted_nft_id: int | None = None
    operate_events: list[FluidVaultOperateEvent] = field(default_factory=list)
    error: str | None = None


@dataclass
class FluidParseResult:
    """Result of parsing a Fluid DEX transaction receipt."""

    success: bool
    transaction_hash: str = ""
    block_number: int = 0
    events: list[FluidOperateEvent] = field(default_factory=list)
    swap_events: list[FluidSwapEvent] = field(default_factory=list)
    nft_id: int | None = None
    token0_amt: int = 0
    token1_amt: int = 0
    error: str | None = None


# =============================================================================
# FluidReceiptParser
# =============================================================================


class FluidReceiptParser:
    """Receipt parser for Fluid DEX transactions (swaps and LP operations).

    Extracts NFT position IDs and token amounts from LogOperate events.
    Supports both LP_OPEN (new position) and LP_CLOSE (withdrawal) receipts.

    SUPPORTED_EXTRACTIONS declares which ResultEnricher fields this parser supports.

    Args:
        chain: Chain name for token resolution (default: "arbitrum")
    """

    SUPPORTED_EXTRACTIONS: frozenset[str] = frozenset(
        {
            "position_id",
            "liquidity",
            "lp_close_data",
            "swap_amounts",
            "supply_amount",
            "withdraw_amount",
        }
    )

    def __init__(self, chain: str = "arbitrum") -> None:
        self.chain = chain.lower()
        self._decimals_cache: dict[str, int | None] = {}

    def parse_receipt(self, receipt: dict[str, Any]) -> FluidParseResult:
        """Parse a Fluid DEX transaction receipt.

        Args:
            receipt: Transaction receipt dict with 'logs', 'transactionHash', etc.

        Returns:
            FluidParseResult with extracted events and data
        """
        tx_hash = receipt.get("transactionHash", "")
        if isinstance(tx_hash, bytes):
            tx_hash = "0x" + tx_hash.hex()

        block_number = receipt.get("blockNumber", 0)
        status = receipt.get("status", 1)

        if status != 1:
            return FluidParseResult(
                success=False,
                transaction_hash=str(tx_hash),
                block_number=block_number,
                error="Transaction reverted",
            )

        logs = receipt.get("logs", [])
        events: list[FluidOperateEvent] = []
        swap_events: list[FluidSwapEvent] = []
        nft_id: int | None = None

        for i, log in enumerate(logs):
            topics = log.get("topics", [])
            if not topics:
                continue

            topic0 = self._normalize_topic(topics[0])

            # LogOperate event
            if topic0 == LOG_OPERATE_TOPIC and len(topics) >= 2:
                event = self._parse_log_operate(topics, log.get("data", "0x"), i)
                if event:
                    events.append(event)
                    if nft_id is None:
                        nft_id = event.nft_id

            # Swap event
            elif topic0 == SWAP_TOPIC:
                swap_event = self._parse_swap_event(log.get("data", "0x"), i)
                if swap_event:
                    swap_events.append(swap_event)

            # ERC-721 Transfer (mint) — fallback for NFT ID extraction.
            # FACTORY-GATED (VIB-5031, ADR §5): the position NFT lives on the
            # VaultFactory, so only a factory-emitted mint may be captured —
            # an unrelated mint inside a bundled/Zodiac transaction was
            # previously mis-captured by this branch (strictly narrowing).
            elif topic0 == ERC721_TRANSFER_TOPIC and len(topics) >= 4:
                from_addr = HexDecoder.topic_to_address(topics[1])
                if from_addr == ZERO_ADDRESS and _log_emitter(log) == _vault_factory_address(self.chain):
                    # Factory mint event — tokenId is topic[3]
                    token_id = int(HexDecoder.normalize_hex(topics[3]), 16)
                    if nft_id is None:
                        nft_id = token_id

        # Aggregate token amounts from all LogOperate events
        total_token0 = sum(e.token0_amt for e in events)
        total_token1 = sum(e.token1_amt for e in events)

        has_events = len(events) > 0 or len(swap_events) > 0

        return FluidParseResult(
            success=has_events,
            transaction_hash=str(tx_hash),
            block_number=block_number,
            events=events,
            swap_events=swap_events,
            nft_id=nft_id,
            token0_amt=total_token0,
            token1_amt=total_token1,
        )

    def _parse_log_operate(
        self,
        topics: list,
        data: str,
        log_index: int,
    ) -> FluidOperateEvent | None:
        """Parse a LogOperate event.

        Args:
            topics: Log topics (topic[0]=event sig, topic[1]=nftId)
            data: Hex-encoded event data (token0Amt, token1Amt, timestamp)
            log_index: Index of the log in the receipt

        Returns:
            FluidOperateEvent or None if parsing fails
        """
        try:
            # nftId is indexed (topic[1])
            nft_id = int(HexDecoder.normalize_hex(topics[1]), 16)

            # data contains: token0Amt (int256), token1Amt (int256), timestamp (uint256)
            hex_data = HexDecoder.normalize_hex(data)

            token0_amt = HexDecoder.decode_int256(hex_data, 0)
            token1_amt = HexDecoder.decode_int256(hex_data, 32)

            return FluidOperateEvent(
                nft_id=nft_id,
                token0_amt=token0_amt,
                token1_amt=token1_amt,
                log_index=log_index,
            )
        except (ValueError, IndexError, TypeError) as e:
            logger.warning(f"Failed to parse LogOperate event: {e}")
            return None

    def _parse_swap_event(self, data: str, log_index: int) -> FluidSwapEvent | None:
        """Parse a Swap(bool,uint256,uint256,address) event.

        Data layout: swap0to1 (bool), amountIn (uint256), amountOut (uint256), to (address)
        """
        try:
            hex_data = HexDecoder.normalize_hex(data)
            swap0to1 = int(hex_data[:64], 16) != 0
            amount_in = HexDecoder.decode_uint256(hex_data, 32)
            amount_out = HexDecoder.decode_uint256(hex_data, 64)
            to = "0x" + hex_data[64 * 3 + 24 : 64 * 4]

            return FluidSwapEvent(
                swap0to1=swap0to1,
                amount_in=amount_in,
                amount_out=amount_out,
                to=to,
                log_index=log_index,
            )
        except (ValueError, IndexError, TypeError) as e:
            logger.warning(f"Failed to parse Swap event: {e}")
            return None

    # =========================================================================
    # Result Enrichment Methods (called by ResultEnricher)
    # =========================================================================

    def extract_position_id(self, receipt: dict[str, Any]) -> int | None:
        """Extract LP position NFT tokenId from receipt.

        Called by ResultEnricher for LP_OPEN intents.

        Args:
            receipt: Transaction receipt dict

        Returns:
            NFT position ID or None if not found
        """
        result = self.parse_receipt(receipt)
        return result.nft_id

    def extract_swap_amounts(self, receipt: dict[str, Any]) -> SwapAmounts | None:
        """Extract swap amounts from receipt.

        Called by ResultEnricher for SWAP intents. Resolves token decimals
        to produce human-readable decimal amounts (consistent with other parsers).

        Args:
            receipt: Transaction receipt dict

        Returns:
            SwapAmounts with input/output amounts, or None
        """
        result = self.parse_receipt(receipt)
        if not result.swap_events:
            return None

        swap = result.swap_events[0]

        # Resolve token decimals for human-readable amounts
        # Identify tokens from the Swap event's pool (log address)
        token_in_addr, token_out_addr = self._extract_swap_token_addresses(receipt, swap.swap0to1)

        # Native-leg fallback: Fluid pools pair raw native (no WETH), so a
        # native leg produces no ERC-20 Transfer. The Swap event still proves
        # the swap, and Fluid pools are strictly per-pair, so when exactly one
        # leg resolved as an ERC-20 the missing leg is guaranteed to be the
        # chain's native token — symmetric on both sides, with no dependency
        # on the receipt sender (under Zodiac Safe execution ``receipt.from``
        # is the relayer EOA, not the swap recipient).
        if token_in_addr is None and token_out_addr is not None:
            token_in_addr = _FLUID_NATIVE_SENTINEL
        if token_out_addr is None and token_in_addr is not None:
            token_out_addr = _FLUID_NATIVE_SENTINEL

        decimals_in = self._resolve_decimals(token_in_addr) if token_in_addr else None
        decimals_out = self._resolve_decimals(token_out_addr) if token_out_addr else None

        if decimals_in is not None and decimals_out is not None:
            amount_in_decimal = Decimal(swap.amount_in) / Decimal(10**decimals_in)
            amount_out_decimal = Decimal(swap.amount_out) / Decimal(10**decimals_out)
        else:
            # Fail-closed: returning raw wei amounts would mislead strategy authors
            logger.warning("Cannot resolve token decimals for Fluid swap — returning None (fail-closed)")
            return None

        effective_price = amount_out_decimal / amount_in_decimal if amount_in_decimal > 0 else Decimal(0)

        return SwapAmounts(
            amount_in=swap.amount_in,
            amount_out=swap.amount_out,
            amount_in_decimal=amount_in_decimal,
            amount_out_decimal=amount_out_decimal,
            effective_price=effective_price,
            token_in=token_in_addr,
            token_out=token_out_addr,
        )

    def _extract_4626_assets(self, receipt: dict[str, Any], topic0: str) -> int | None:
        """Decode ``assets`` (data word 0) from an ERC-4626 Deposit/Withdraw log.

        Fail-closed: a receipt without the event (or with malformed data)
        returns ``None`` — never a zero-filled or fabricated amount. A
        receipt with MORE THAN ONE matching event is ambiguous attribution
        (a Fluid lending intent compiles exactly one deposit/withdraw, so
        multiple events mean a bundler/multicall receipt this parser cannot
        attribute) — also ``None``, never the first event's amount.
        """
        if receipt.get("status", 1) != 1:
            return None
        matches = [
            log
            for log in receipt.get("logs", [])
            if (topics := log.get("topics", [])) and self._normalize_topic(topics[0]) == topic0
        ]
        if not matches:
            return None
        if len(matches) > 1:
            logger.warning(
                "Ambiguous Fluid lending receipt: %d ERC-4626 events match topic %s — "
                "returning None (fail closed, ambiguous attribution)",
                len(matches),
                topic0,
            )
            return None
        data = HexDecoder.normalize_hex(matches[0].get("data", "0x"))
        # Deposit and Withdraw both ABI-encode TWO non-indexed uint256 words
        # (assets, shares) — a one-word/truncated payload is malformed, and
        # decoding word 0 from it would fabricate an amount. Require both.
        if len(data) < 128:
            logger.warning("Malformed ERC-4626 event data in Fluid lending receipt — returning None")
            return None
        try:
            return int(data[0:64], 16)
        except ValueError:
            logger.warning("Undecodable ERC-4626 event data in Fluid lending receipt — returning None")
            return None

    def extract_supply_amount(self, receipt: dict[str, Any]) -> int | None:
        """Exact ``assets`` supplied, from the fToken's ERC-4626 ``Deposit`` event.

        Called by ResultEnricher for SUPPLY intents (VIB-5030).
        """
        return self._extract_4626_assets(receipt, DEPOSIT_4626_TOPIC)

    def extract_withdraw_amount(self, receipt: dict[str, Any]) -> int | None:
        """Exact ``assets`` withdrawn, from the fToken's ERC-4626 ``Withdraw`` event.

        Both ``withdraw(assets, ...)`` and full-exit ``redeem(shares, ...)``
        emit the same event; ``assets`` is the underlying amount either way.
        """
        return self._extract_4626_assets(receipt, WITHDRAW_4626_TOPIC)

    def extract_lp_close_data(self, receipt: dict[str, Any]) -> LPCloseData | None:
        """Extract LP close data from receipt.

        Called by ResultEnricher for LP_CLOSE intents.
        Returns amounts collected on withdrawal (negative amounts = withdrawn).

        Args:
            receipt: Transaction receipt dict

        Returns:
            LPCloseData with collected amounts, or None if not found
        """
        result = self.parse_receipt(receipt)
        if not result.success:
            return None

        # On LP_CLOSE, amounts are negative (withdrawn from pool)
        amount0 = abs(result.token0_amt)
        amount1 = abs(result.token1_amt)

        return LPCloseData(
            amount0_collected=amount0,
            amount1_collected=amount1,
            # VIB-4470 — Fluid bundles fees into the withdrawal amount;
            # fees are not separately measured (Empty ≠ Zero).
            fees0=None,
            fees1=None,
            liquidity_removed=None,
        )

    def extract_liquidity(self, receipt: dict[str, Any]) -> int | None:
        """Extract liquidity amount from LP_OPEN receipt.

        For Fluid DEX, liquidity is represented by the collateral shares
        (token0_amt + token1_amt deposited).

        Args:
            receipt: Transaction receipt dict

        Returns:
            Combined deposit amount or None
        """
        result = self.parse_receipt(receipt)
        if not result.success:
            return None
        # Sum of positive amounts = total deposited
        return max(0, result.token0_amt) + max(0, result.token1_amt)

    # =========================================================================
    # Helpers
    # =========================================================================

    @staticmethod
    def _receipt_wallet(receipt: dict[str, Any]) -> str:
        """Normalized lowercase wallet (tx sender) from a receipt dict."""
        wallet = receipt.get("from", "")
        if isinstance(wallet, bytes):
            wallet = "0x" + wallet.hex()
        return wallet.lower() if wallet else ""

    def _extract_swap_token_addresses(self, receipt: dict[str, Any], swap0to1: bool) -> tuple[str | None, str | None]:
        """Extract token_in and token_out addresses from ERC-20 Transfer events.

        Fluid custodies all pool funds in the central Liquidity layer
        (deterministic address on every supported chain), so a swap's ERC-20
        legs are always ``payer -> Liquidity`` (input) and ``Liquidity ->
        recipient`` (output) — verified on-chain in the Phase-0 report §V1
        (VIB-5028). Matching on the Liquidity side identifies the tokens
        without depending on who sent the transaction: under Zodiac Safe
        execution ``receipt.from`` is the relayer EOA while the transfers
        involve the Safe, so a wallet-keyed heuristic (Enso-style) would
        miss both legs. The wallet-keyed match is kept as a secondary signal
        for receipts that don't involve the canonical Liquidity address.
        """
        liquidity = FLUID_ADDRESSES.get(self.chain, {}).get("liquidity", "").lower()
        wallet = self._receipt_wallet(receipt)

        token_in_addr = None
        token_out_addr = None

        for log in receipt.get("logs", []):
            topics = log.get("topics", [])
            if not topics or len(topics) < 3:
                continue
            topic0 = self._normalize_topic(topics[0])
            if topic0 != ERC721_TRANSFER_TOPIC:
                # ERC721_TRANSFER_TOPIC == ERC20 Transfer topic (same hash)
                continue
            log_from = HexDecoder.topic_to_address(topics[1])
            log_to = HexDecoder.topic_to_address(topics[2])
            token_address = log.get("address", "")
            if isinstance(token_address, bytes):
                token_address = "0x" + token_address.hex()

            # Primary: Liquidity-layer counterparty (sender-independent).
            # Both branches are first-write-guarded: a swap receipt has one
            # leg per side (one swap per intent), so the first match is the
            # swap leg and later Liquidity-touching transfers can't clobber.
            if liquidity:
                if log_to == liquidity and token_in_addr is None:
                    token_in_addr = token_address
                if log_from == liquidity and token_out_addr is None:
                    token_out_addr = token_address

            # Secondary: wallet-keyed heuristic (EOA execution only).
            if wallet:
                if log_from == wallet and token_in_addr is None:
                    token_in_addr = token_address
                if log_to == wallet and token_out_addr is None:
                    token_out_addr = token_address

        return token_in_addr, token_out_addr

    def _resolve_decimals(self, token_address: str) -> int | None:
        """Resolve token decimals via the token resolver.

        Returns None if the resolver is unavailable or the token is unknown.
        """
        if not token_address:
            return None

        if token_address.lower() == _FLUID_NATIVE_SENTINEL:
            return _NATIVE_DECIMALS

        cache_key = f"{self.chain}:{token_address.lower()}"
        if cache_key in self._decimals_cache:
            return self._decimals_cache[cache_key]

        try:
            from almanak.framework.data.tokens import get_token_resolver
            from almanak.framework.data.tokens.exceptions import TokenNotFoundError, TokenResolutionError

            resolver = get_token_resolver()
            resolved = resolver.resolve(token_address, self.chain)
            self._decimals_cache[cache_key] = resolved.decimals
            return resolved.decimals
        except TokenNotFoundError:
            # Token genuinely unknown — safe to cache as None
            self._decimals_cache[cache_key] = None
            return None
        except (TokenResolutionError, Exception):
            # Transient failure (RPC timeout, resolver error, etc.) — do NOT cache, allow retry
            return None

    @staticmethod
    def _normalize_topic(topic: Any) -> str:
        """Normalize a log topic to lowercase hex string with 0x prefix."""
        if isinstance(topic, bytes):
            return "0x" + topic.hex().lower()
        s = str(topic)
        if not s.startswith("0x"):
            s = "0x" + s
        return s.lower()


# =============================================================================
# FluidVaultReceiptParser — vault NFT-CDP lifecycle (Phase 3, VIB-5031)
# =============================================================================


class FluidVaultReceiptParser:
    """Receipt parser for Fluid vault ``operate()`` transactions.

    Routed by the ``fluid_vault`` protocol key (the fluid_vault manifest's
    receipt-parser connector) for SUPPLY / BORROW / REPAY / WITHDRAW /
    DELEVERAGE receipts. Three sources, in trust order:

    1. **Factory-gated ERC-721 mint** — ``Transfer(0x0 -> wallet, nftId)``
       emitted by the VaultFactory ONLY. Foreign mints inside a bundled /
       Zodiac receipt are ignored (ADR §5; the verification report proved
       the genuine mint's emitter == ``VaultResolver.FACTORY()``).
    2. **VaultT1 ``LogOperate``** — the receipt-truth SIGNED deltas (the
       int-min sentinel's true amount is only knowable from this event;
       native legs produce no ERC-20 Transfer to infer from).
    3. The two Liquidity-layer ``LogOperate`` legs are corroboration only
       and are never decoded as amounts (different signature — topic0
       ``0x4d93b232…``).

    Fail-closed everywhere (Empty ≠ Zero): reverted receipts, truncated
    payloads (data must be EXACTLY 5 words), foreign mints, and ambiguous
    multi-operate receipts all yield ``None`` — never a fabricated nftId or
    zero-filled delta.
    """

    SUPPORTED_EXTRACTIONS: frozenset[str] = frozenset(
        {
            "position_id",
            "supply_amount",
            "borrow_amount",
            "repay_amount",
            "withdraw_amount",
            "lending_data",
        }
    )

    def __init__(self, chain: str = "arbitrum") -> None:
        self.chain = chain.lower()

    # ------------------------------------------------------------------
    # Core parse
    # ------------------------------------------------------------------

    def parse_receipt(self, receipt: dict[str, Any]) -> FluidVaultParseResult:
        """Parse a Fluid vault operate() receipt (mint + signed deltas)."""
        tx_hash = receipt.get("transactionHash", "")
        if isinstance(tx_hash, bytes):
            tx_hash = "0x" + tx_hash.hex()
        block_number = receipt.get("blockNumber", 0)

        if receipt.get("status", 1) != 1:
            return FluidVaultParseResult(
                success=False,
                transaction_hash=str(tx_hash),
                block_number=block_number,
                error="Transaction reverted",
            )

        factory = _vault_factory_address(self.chain)
        minted_nft_ids: list[int] = []
        operate_events: list[FluidVaultOperateEvent] = []

        for i, log in enumerate(receipt.get("logs", [])):
            topics = log.get("topics", [])
            if not topics:
                continue
            topic0 = self._normalize_topic(topics[0])

            if topic0 == VAULT_LOG_OPERATE_TOPIC:
                event = self._parse_vault_operate(log, i)
                if event is not None:
                    operate_events.append(event)

            elif topic0 == ERC721_TRANSFER_TOPIC and len(topics) >= 4:
                # Factory-gated mint: emitter must BE the chain's VaultFactory;
                # a foreign ERC-721 mint in the same receipt is ignored.
                if factory and _log_emitter(log) == factory:
                    from_addr = HexDecoder.topic_to_address(topics[1])
                    if from_addr == ZERO_ADDRESS:
                        minted_nft_ids.append(int(HexDecoder.normalize_hex(topics[3]), 16))

        # Two DIFFERENT factory mints in one receipt: this connector never
        # batches opens, so attribution is ambiguous — fail closed (no
        # captured nftId) rather than guess first-wins.
        distinct_mints = set(minted_nft_ids)
        if len(distinct_mints) > 1:
            logger.warning(
                "Ambiguous Fluid vault receipt: %d distinct factory mints %s — not capturing an nftId",
                len(distinct_mints),
                sorted(distinct_mints),
            )
            minted_nft_id = None
        else:
            minted_nft_id = minted_nft_ids[0] if minted_nft_ids else None

        # Mint <-> operate correlation: when the vault's own LogOperate names
        # a nonzero nftId, a captured factory mint MUST agree with it. A
        # mismatch means the mint belongs to a different position in the same
        # receipt (bundler/multicall) — ambiguous attribution, fail closed.
        if minted_nft_id is not None:
            operate_nft_ids = {event.nft_id for event in operate_events if event.nft_id != 0}
            if operate_nft_ids and minted_nft_id not in operate_nft_ids:
                logger.warning(
                    "Ambiguous Fluid vault receipt: factory mint tokenId %d does not match "
                    "vault LogOperate nftId(s) %s — not capturing an nftId",
                    minted_nft_id,
                    sorted(operate_nft_ids),
                )
                minted_nft_id = None

        return FluidVaultParseResult(
            success=bool(operate_events) or minted_nft_id is not None,
            transaction_hash=str(tx_hash),
            block_number=block_number,
            minted_nft_id=minted_nft_id,
            operate_events=operate_events,
        )

    def _parse_vault_operate(self, log: dict[str, Any], log_index: int) -> FluidVaultOperateEvent | None:
        """Decode the 5-word VaultT1 LogOperate payload (nothing indexed).

        Layout (verification report D2): ``user_``, ``nftId_``, ``colAmt_``
        (int256 two's-complement), ``debtAmt_`` (int256), ``to_``. A payload
        that is not EXACTLY 5 words is truncated/foreign and is rejected —
        never partially decoded.
        """
        data = HexDecoder.normalize_hex(log.get("data", "0x"))
        if len(data) != _VAULT_LOG_OPERATE_DATA_HEX_LEN:
            logger.warning(
                "Malformed Fluid vault LogOperate payload (%d hex chars, expected %d) — rejecting (fail closed)",
                len(data),
                _VAULT_LOG_OPERATE_DATA_HEX_LEN,
            )
            return None
        try:
            nft_id = int(data[64:128], 16)
            col_delta = HexDecoder.decode_int256(data, 64)
            debt_delta = HexDecoder.decode_int256(data, 96)
        except (ValueError, IndexError, TypeError) as exc:
            logger.warning("Undecodable Fluid vault LogOperate payload: %s — rejecting (fail closed)", exc)
            return None
        return FluidVaultOperateEvent(
            nft_id=nft_id,
            col_delta=col_delta,
            debt_delta=debt_delta,
            vault=_log_emitter(log),
            log_index=log_index,
        )

    def _single_operate_event(self, receipt: dict[str, Any]) -> FluidVaultOperateEvent | None:
        """The receipt's ONE vault operate event, or None (fail closed).

        A fluid_vault intent compiles exactly one ``operate()``; multiple
        vault events mean a bundler/multicall receipt this parser cannot
        attribute — ambiguous, so no amount is ever guessed.
        """
        result = self.parse_receipt(receipt)
        if not result.operate_events:
            return None
        if len(result.operate_events) > 1:
            logger.warning(
                "Ambiguous Fluid vault receipt: %d LogOperate events — returning None (fail closed)",
                len(result.operate_events),
            )
            return None
        return result.operate_events[0]

    # ------------------------------------------------------------------
    # Result Enrichment Methods (called by ResultEnricher / runner hooks)
    # ------------------------------------------------------------------

    def extract_position_id(self, receipt: dict[str, Any]) -> int | None:
        """The position nftId: factory mint first (opens), vault event second.

        Returns ``None`` (never a fabricated id) when the receipt carries
        neither a factory-emitted mint nor a vault LogOperate. Multiple vault
        LogOperate events mean a bundler/multicall receipt this parser cannot
        attribute — fail closed exactly like ``_single_operate_event`` (a
        surviving factory mint does not rescue an ambiguous receipt). A
        LogOperate ``nftId`` of 0 is the operate() MINT SENTINEL, never a
        real position id — when no factory mint resolved it, fail closed
        (the same 0-is-not-an-id rule the mint<->operate correlation uses).
        """
        result = self.parse_receipt(receipt)
        if len(result.operate_events) > 1:
            logger.warning(
                "Ambiguous Fluid vault receipt: %d LogOperate events — not extracting a position id (fail closed)",
                len(result.operate_events),
            )
            return None
        if result.minted_nft_id is not None:
            return result.minted_nft_id
        if result.operate_events and result.operate_events[0].nft_id != 0:
            return result.operate_events[0].nft_id
        return None

    def extract_lending_data(self, receipt: dict[str, Any]) -> dict[str, str] | None:
        """``FluidVaultOperateData`` — the §6.3 ``extracted_data_json`` payload.

        String-encoded ints for JSON safety. This is the ONLY persisted home
        of the nftId (no Postgres DDL, no typed-event field at v1). A
        LogOperate ``nftId`` of 0 is the operate() MINT SENTINEL, never a
        real position id — the key is OMITTED (fail closed, never "0") while
        the receipt-truth deltas are still emitted.
        """
        event = self._single_operate_event(receipt)
        if event is None:
            return None
        data = {
            "vault": event.vault,
            "col_delta": str(event.col_delta),
            "debt_delta": str(event.debt_delta),
        }
        if event.nft_id != 0:
            data["nft_id"] = str(event.nft_id)
        return data

    def extract_supply_amount(self, receipt: dict[str, Any]) -> int | None:
        """Receipt-truth collateral added (positive col delta), else None."""
        event = self._single_operate_event(receipt)
        if event is None or event.col_delta <= 0:
            return None
        return event.col_delta

    def extract_borrow_amount(self, receipt: dict[str, Any]) -> int | None:
        """Receipt-truth debt drawn (positive debt delta), else None."""
        event = self._single_operate_event(receipt)
        if event is None or event.debt_delta <= 0:
            return None
        return event.debt_delta

    def extract_repay_amount(self, receipt: dict[str, Any]) -> int | None:
        """Receipt-truth debt repaid (negative debt delta, absolute).

        For ``repay_full`` the compiled calldata carried the int-min
        sentinel — the TRUE amount exists only here, in the resolved
        signed delta the vault logged.
        """
        event = self._single_operate_event(receipt)
        if event is None or event.debt_delta >= 0:
            return None
        return -event.debt_delta

    def extract_withdraw_amount(self, receipt: dict[str, Any]) -> int | None:
        """Receipt-truth collateral withdrawn (negative col delta, absolute)."""
        event = self._single_operate_event(receipt)
        if event is None or event.col_delta >= 0:
            return None
        return -event.col_delta

    @staticmethod
    def _normalize_topic(topic: Any) -> str:
        """Normalize a log topic to lowercase hex string with 0x prefix."""
        return FluidReceiptParser._normalize_topic(topic)
