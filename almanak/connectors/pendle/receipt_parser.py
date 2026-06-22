"""
Pendle Protocol Receipt Parser

This parser handles transaction receipts from Pendle Protocol operations,
extracting relevant event data for:
- Swaps (token -> PT, PT -> token, YT swaps)
- Liquidity operations (add/remove)
- PT/YT redemptions

Event Signatures (PendleMarketV3, verified on-chain 2026-04-26 via VIB-3419):
- Swap: Swap(address indexed caller, address indexed receiver, int256 ptToAccount, int256 syToAccount, uint256 syFee, uint256 syToReserve)
- Mint: Mint(address indexed receiver, uint256 netLpToAccount, uint256 netSyUsed, uint256 netPtUsed)
- Burn: Burn(address indexed receiverSy, address indexed receiverPt, uint256 netLpToBurn, uint256 netSyOut, uint256 netPtOut)
- RedeemPY: RedeemPY(address indexed caller, address indexed receiver, uint256 netPYRedeemed, uint256 netSYRedeemed)
- Transfer (ERC20): Transfer(address indexed from, address indexed to, uint256 value)
"""

import logging
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from typing import TYPE_CHECKING, Any

from almanak.connectors._strategy_base.base import EventRegistry, HexDecoder
from almanak.framework.execution.extract_result import (
    ExtractError,
    ExtractMissing,
    ExtractOk,
    ExtractResult,
)
from almanak.framework.execution.extracted_data import LPCloseData, LPOpenData, SwapAmounts

if TYPE_CHECKING:
    from almanak.connectors._strategy_base.primitive_money_leg import PrimitiveMoneyLegs

logger = logging.getLogger(__name__)


# =============================================================================
# Event Topic Signatures
# =============================================================================

# Pendle-specific event signatures (keccak256 hashes)
EVENT_TOPICS: dict[str, str] = {
    # Pendle Market events (verified against Pendle V2 contracts on Arbitrum — VIB-3419)
    # Swap(address,address,int256,int256,uint256,uint256) — caller,receiver,ptToAccount,syToAccount,syFee,syToReserve
    "Swap": "0x829000a5bc6a12d46e30cdcecd7c56b1efd88f6d7d059da6734a04f3764557c4",
    # Mint(address,uint256,uint256,uint256) — PendleMarketV3: receiver(indexed),netLpToAccount,netSyUsed,netPtUsed
    # keccak256("Mint(address,uint256,uint256,uint256)") — verified on-chain 2026-04-26
    "Mint": "0xb4c03061fb5b7fed76389d5af8f2e0ddb09f8c70d1333abbb62582835e10accb",
    # Burn(address,address,uint256,uint256,uint256) — PendleMarketV3: receiverSy(indexed),receiverPt(indexed),netLpToBurn,netSyOut,netPtOut
    # keccak256("Burn(address,address,uint256,uint256,uint256)") — verified on-chain 2026-04-26
    "Burn": "0x4cf25bc1d991c17529c25213d3cc0cda295eeaad5f13f361969b12ea48015f90",
    # PY events — corrected from placeholder values (VIB-3419)
    # RedeemPY(address,address,uint256,uint256) — caller,receiver,netPYRedeemed,netSYRedeemed
    "RedeemPY": "0x35ba0cff8a710db439ca6681204f5befe6e7868ab194cfebb108de45bcf0588b",
    # MintPY(address,address,uint256,uint256) — caller,receiver,netPYOut,netSyIn
    "MintPY": "0xee1779d47412b4ece8f96b61f5b2406c32c43ca08ed99cf2af83323cf3900008",
    # SY events from IStandardizedYield — corrected from placeholder values (VIB-3419)
    # Deposit(address,address,address,uint256,uint256) — caller,receiver,tokenIn,amountDeposited,amountSyOut
    "MintSY": "0x5fe47ed6d4225326d3303476197d782ded5a4e9c14f479dc9ec4992af4e85d59",
    # Redeem(address,address,address,uint256,uint256) — caller,receiver,tokenOut,amountSyIn,amountTokenOut
    "RedeemSY": "0xaee47cdf925cf525fdae94f9777ee5a06cac37e1c41220d0a8a89ed154f62d1c",
    # Standard ERC20 events
    "Transfer": "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
    "Approval": "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925",
}

# Reverse mapping for topic lookup
TOPIC_TO_EVENT: dict[str, str] = {v.lower(): k for k, v in EVENT_TOPICS.items()}


# =============================================================================
# Enums
# =============================================================================


class PendleEventType(Enum):
    """Pendle event types."""

    SWAP = "SWAP"
    MINT = "MINT"
    BURN = "BURN"
    REDEEM_PY = "REDEEM_PY"
    MINT_PY = "MINT_PY"
    MINT_SY = "MINT_SY"
    REDEEM_SY = "REDEEM_SY"
    TRANSFER = "TRANSFER"
    APPROVAL = "APPROVAL"
    UNKNOWN = "UNKNOWN"


EVENT_NAME_TO_TYPE: dict[str, PendleEventType] = {
    "Swap": PendleEventType.SWAP,
    "Mint": PendleEventType.MINT,
    "Burn": PendleEventType.BURN,
    "RedeemPY": PendleEventType.REDEEM_PY,
    "MintPY": PendleEventType.MINT_PY,
    "MintSY": PendleEventType.MINT_SY,
    "RedeemSY": PendleEventType.REDEEM_SY,
    "Transfer": PendleEventType.TRANSFER,
    "Approval": PendleEventType.APPROVAL,
}


# =============================================================================
# PT / base-token symbol resolution (G-PT0)
# =============================================================================
#
# The Pendle Market ``Swap`` event carries no token symbol — it reflects an
# internal SY <-> PT trade. The legacy parser therefore stamped the swap legs
# with the generic placeholders ``"SY"`` / ``"PT"``. Those placeholders flow
# through ResultEnricher into ``transaction_ledger.token_in`` / ``token_out``
# and win over the intent's ``to_token`` (ledger precedence prefers the parser
# label). The accounting categorizer claims a PT trade only when a leg
# ``.startswith("PT-")`` AND ``_parse_pt_maturity`` can read the expiry out of
# the symbol — neither of which a bare ``"PT"`` (or even the intent's
# maturity-less ``"PT-wstETH"`` alias) satisfies. The whole Pendle PT
# accounting vertical was inert as a result.
#
# The compiler already hands the parser the PT token *address* (``token_out``
# for a buy, ``token_in`` for a sell). That address reverse-maps — via the
# connector's own ``PT_TOKEN_INFO`` catalogue — to the canonical
# maturity-bearing symbol (e.g. ``PT-wstETH-25JUN2026``), which is the ONLY
# source that carries the expiry. We resolve it here so the ledger carries the
# real on-chain symbol for every downstream consumer, not just accounting.


def _resolve_pt_symbol(chain: str, pt_address: str | None) -> str | None:
    """Return the canonical maturity-bearing PT symbol for ``pt_address``.

    Reverse-looks the PT token address up in :data:`PT_TOKEN_INFO` and returns
    the alias that embeds the maturity date (``PT-<asset>-<DDMONYYYY>``) so that
    :func:`accounting_spec._parse_pt_maturity` can read the expiry. When several
    aliases share the address (the catalogue stores upper-case + mixed-case +
    maturity-less spellings of the same token), the longest alias that matches
    ``[-_]\\d{1,2}[A-Z]{3}\\d{4}`` wins — that is the maturity-bearing one.

    Returns ``None`` (never a fabricated symbol) when the chain/address is
    unknown or no catalogue alias carries a maturity — the caller then degrades
    to the generic label rather than guessing wrong (Empty != Zero).
    """
    if not pt_address:
        return None
    from almanak.connectors.pendle.sdk import PT_TOKEN_INFO

    target = pt_address.lower()
    chain_info = PT_TOKEN_INFO.get(chain.lower(), {})
    maturity_re = re.compile(r"[-_]\d{1,2}[A-Z]{3}\d{4}(?:$|[-_])")
    best: str | None = None
    for symbol, (addr, _decimals) in chain_info.items():
        if addr.lower() != target:
            continue
        if not maturity_re.search(symbol.upper()):
            continue
        # The catalogue stores several maturity-bearing aliases at one address
        # (a fully-upper-case spelling kept only "for compiler lookup" plus the
        # canonical mixed-case form). Prefer the longest, and among equal-length
        # ties prefer the mixed-case spelling — that is the human-readable
        # canonical symbol the strategy config carries
        # (``market_name="PT-wstETH-25JUN2026"``). The downstream maturity parse
        # is case-insensitive, so this only affects ledger readability.
        if best is None or _pt_symbol_rank(symbol) > _pt_symbol_rank(best):
            best = symbol
    return best


def _pt_symbol_rank(symbol: str) -> tuple[int, int]:
    """Rank a PT alias: longer wins; among ties, a non-all-caps spelling wins."""
    return (len(symbol), 0 if symbol.isupper() else 1)


def _resolve_base_symbol(chain: str, base_address: str | None) -> str | None:
    """Return the underlying/base token symbol for ``base_address``.

    Reverse-looks the address up in :data:`PENDLE_TOKENS` (the connector's
    canonical underlying-token catalogue). This is the non-PT leg of a PT swap
    (e.g. ``WSTETH``), which the accounting builders key ``price_inputs_json``
    by. Returns ``None`` when unknown — the caller degrades to ``"SY"`` rather
    than inventing a symbol.
    """
    if not base_address:
        return None
    from almanak.connectors.pendle.addresses import PENDLE_TOKENS

    target = base_address.lower()
    for symbol, addr in PENDLE_TOKENS.get(chain.lower(), {}).items():
        if addr.lower() == target:
            return symbol
    return None


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class PendleEvent:
    """Parsed Pendle event."""

    event_type: PendleEventType
    event_name: str
    log_index: int
    transaction_hash: str
    block_number: int
    contract_address: str
    data: dict[str, Any]
    raw_topics: list[str] = field(default_factory=list)
    raw_data: str = ""
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "event_type": self.event_type.value,
            "event_name": self.event_name,
            "log_index": self.log_index,
            "transaction_hash": self.transaction_hash,
            "block_number": self.block_number,
            "contract_address": self.contract_address,
            "data": self.data,
            "raw_topics": self.raw_topics,
            "raw_data": self.raw_data,
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass
class SwapEventData:
    """Parsed data from Pendle Swap event."""

    caller: str
    receiver: str
    pt_to_account: int  # Signed - positive means PT received
    sy_to_account: int  # Signed - positive means SY received
    market_address: str

    @property
    def is_buy_pt(self) -> bool:
        """Check if this is a buy PT operation (SY -> PT)."""
        return self.pt_to_account > 0

    @property
    def is_sell_pt(self) -> bool:
        """Check if this is a sell PT operation (PT -> SY)."""
        return self.pt_to_account < 0

    @property
    def pt_amount(self) -> int:
        """Get absolute PT amount."""
        return abs(self.pt_to_account)

    @property
    def sy_amount(self) -> int:
        """Get absolute SY amount."""
        return abs(self.sy_to_account)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "caller": self.caller,
            "receiver": self.receiver,
            "pt_to_account": str(self.pt_to_account),
            "sy_to_account": str(self.sy_to_account),
            "market_address": self.market_address,
            "is_buy_pt": self.is_buy_pt,
            "pt_amount": str(self.pt_amount),
            "sy_amount": str(self.sy_amount),
        }


@dataclass
class MintEventData:
    """Parsed data from Pendle Mint (LP) event."""

    receiver: str
    net_lp_minted: int
    net_sy_used: int
    net_pt_used: int
    market_address: str

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "receiver": self.receiver,
            "net_lp_minted": str(self.net_lp_minted),
            "net_sy_used": str(self.net_sy_used),
            "net_pt_used": str(self.net_pt_used),
            "market_address": self.market_address,
        }


@dataclass
class BurnEventData:
    """Parsed data from Pendle Burn (LP removal) event.

    V3 Burn emits two indexed receivers: receiverSy (gets SY) and receiverPt (gets PT).
    Both are exposed here; in normal LP close they are the same address.
    """

    receiver_sy: str
    receiver_pt: str
    net_lp_burned: int
    net_sy_out: int
    net_pt_out: int
    market_address: str

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "receiver_sy": self.receiver_sy,
            "receiver_pt": self.receiver_pt,
            "net_lp_burned": str(self.net_lp_burned),
            "net_sy_out": str(self.net_sy_out),
            "net_pt_out": str(self.net_pt_out),
            "market_address": self.market_address,
        }


@dataclass
class RedeemPYEventData:
    """Parsed data from Pendle RedeemPY event."""

    caller: str
    receiver: str
    net_py_redeemed: int
    net_sy_redeemed: int
    yt_address: str

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "caller": self.caller,
            "receiver": self.receiver,
            "net_py_redeemed": str(self.net_py_redeemed),
            "net_sy_redeemed": str(self.net_sy_redeemed),
            "yt_address": self.yt_address,
        }


@dataclass
class RedeemSYEventData:
    """Parsed data from Pendle SY Redeem event (post-maturity PT redemption path).

    Redeem(address indexed caller, address indexed receiver, address indexed tokenOut,
           uint256 amountSyToRedeem, uint256 amountTokenOut)
    Emitted by the SY contract when SY is redeemed for the underlying token.
    Used by redeemPyToToken after PT maturity (no RedeemPY is emitted in this path).
    """

    caller: str
    receiver: str
    token_out: str
    amount_sy_to_redeem: int
    amount_token_out: int
    sy_address: str

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "caller": self.caller,
            "receiver": self.receiver,
            "token_out": self.token_out,
            "amount_sy_to_redeem": str(self.amount_sy_to_redeem),
            "amount_token_out": str(self.amount_token_out),
            "sy_address": self.sy_address,
        }


@dataclass
class TransferEventData:
    """Parsed data from ERC20 Transfer event."""

    from_addr: str
    to_addr: str
    value: int
    token_address: str

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "from_addr": self.from_addr,
            "to_addr": self.to_addr,
            "value": str(self.value),
            "token_address": self.token_address,
        }


@dataclass
class ParsedSwapResult:
    """High-level swap result from Pendle."""

    token_in: str
    token_out: str
    amount_in: int
    amount_out: int
    amount_in_decimal: Decimal
    amount_out_decimal: Decimal
    effective_price: Decimal
    slippage_bps: int
    market_address: str
    swap_type: str  # "buy_pt", "sell_pt", "buy_yt", "sell_yt"

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "token_in": self.token_in,
            "token_out": self.token_out,
            "amount_in": str(self.amount_in),
            "amount_out": str(self.amount_out),
            "amount_in_decimal": str(self.amount_in_decimal),
            "amount_out_decimal": str(self.amount_out_decimal),
            "effective_price": str(self.effective_price),
            "slippage_bps": self.slippage_bps,
            "market_address": self.market_address,
            "swap_type": self.swap_type,
        }


@dataclass
class ParseResult:
    """Result of parsing a Pendle receipt."""

    success: bool
    events: list[PendleEvent] = field(default_factory=list)
    swap_events: list[SwapEventData] = field(default_factory=list)
    mint_events: list[MintEventData] = field(default_factory=list)
    burn_events: list[BurnEventData] = field(default_factory=list)
    redeem_events: list[RedeemPYEventData] = field(default_factory=list)
    redeem_sy_events: list[RedeemSYEventData] = field(default_factory=list)
    transfer_events: list[TransferEventData] = field(default_factory=list)
    swap_result: ParsedSwapResult | None = None
    error: str | None = None
    transaction_hash: str = ""
    block_number: int = 0
    transaction_success: bool = True

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "events": [e.to_dict() for e in self.events],
            "swap_events": [s.to_dict() for s in self.swap_events],
            "mint_events": [m.to_dict() for m in self.mint_events],
            "burn_events": [b.to_dict() for b in self.burn_events],
            "redeem_events": [r.to_dict() for r in self.redeem_events],
            "redeem_sy_events": [r.to_dict() for r in self.redeem_sy_events],
            "transfer_events": [t.to_dict() for t in self.transfer_events],
            "swap_result": self.swap_result.to_dict() if self.swap_result else None,
            "error": self.error,
            "transaction_hash": self.transaction_hash,
            "block_number": self.block_number,
            "transaction_success": self.transaction_success,
        }


# =============================================================================
# Receipt Parser
# =============================================================================


class PendleReceiptParser:
    """
    Parser for Pendle Protocol transaction receipts.

    Uses the base infrastructure (EventRegistry, HexDecoder) for standardized
    event parsing while handling Pendle-specific event structures.

    Example:
        parser = PendleReceiptParser(chain="arbitrum")
        result = parser.parse_receipt(receipt)

        if result.success and result.swap_events:
            swap = result.swap_events[0]
            print(f"Swapped {swap.sy_amount} SY for {swap.pt_amount} PT")
    """

    SUPPORTED_EXTRACTIONS: frozenset[str] = frozenset(
        {
            "swap_amounts",
            "lp_open_data",
            "lp_close_data",
            "position_id",
            "redemption_amounts",
            # G-PT (VIB-4988 part 2): a PT redeem (WITHDRAW) declares its money
            # legs (INPUT=PT, OUTPUT=underlying) as a typed PrimitiveMoneyLegs so
            # the ledger dispatcher carries the maturity-bearing PT symbol on
            # token_in instead of the lending-path guess (underlying-in/empty-out).
            "primitive_money_legs",
        }
    )

    # Connector-DECLARED per-intent extra extractions (VIB-4988): the framework
    # enricher merges these onto the generic EXTRACTION_SPECS base via
    # ``ResultEnricher._with_parser_extra_extractions`` — keeping the Pendle-specific
    # field choice in the connector, not as a protocol-named overlay in the
    # framework (test_connector_descriptor forbids that for migrated connectors).
    # A PT redeem is a WITHDRAW; ``extract_primitive_money_legs`` surfaces the
    # maturity-bearing PT symbol the lending-path guesser would otherwise drop. A
    # non-PT Pendle withdraw yields None from the extractor → legacy path, unchanged.
    #
    # VIB-5302 — an LP_CLOSE routes through the SAME ``primitive_money_legs``
    # extractor to declare the received-underlying OUTPUT leg (the realized
    # proceeds the legacy LP_CLOSE ledger path drops for Pendle). The extractor
    # discriminates redeem vs LP-close by the Market ``Burn`` event (present only
    # on an LP removal) and returns None for an unmeasured close → legacy path,
    # unchanged.
    EXTRA_EXTRACTIONS_BY_INTENT: dict[str, tuple[str, ...]] = {
        "WITHDRAW": ("primitive_money_legs",),
        "LP_CLOSE": ("primitive_money_legs",),
    }

    def __init__(
        self,
        chain: str = "arbitrum",
        token_in_decimals: int = 18,
        token_out_decimals: int = 18,
        quoted_price: Decimal | None = None,
        **kwargs: Any,
    ) -> None:
        """
        Initialize the Pendle receipt parser.

        Args:
            chain: Chain name for address resolution
            token_in_decimals: Decimals for input token
            token_out_decimals: Decimals for output token
            quoted_price: Expected price for slippage calculation
        """
        self.chain = chain.lower()
        self.token_in_decimals = token_in_decimals
        self.token_out_decimals = token_out_decimals
        self.quoted_price = quoted_price

        # Initialize event registry
        self.registry = EventRegistry(EVENT_TOPICS, EVENT_NAME_TO_TYPE)

    def build_extract_kwargs(
        self,
        *,
        field: str,
        bundle_metadata: dict[str, Any],
    ) -> dict[str, Any]:
        """Return Pendle-owned kwargs for ResultEnricher extraction calls.

        VIB-3751: Pendle YT swaps need compiler metadata to reconstruct
        user-facing amounts from Transfer events. The Market Swap event reports
        an internal PT flash-mint for YT trades, so the generic framework cannot
        derive these parser-specific hints.

        G-PT (VIB-4988 part 2): a PT redeem (``primitive_money_legs`` on a
        WITHDRAW) needs the compiler-resolved PT token address and the underlying
        out-token identity/decimals to declare its money legs — a redeem emits no
        Market Swap event, so the swap-path PT-symbol resolution never fires.
        """
        if field == "primitive_money_legs":
            return self._build_redeem_legs_kwargs(bundle_metadata)
        if field != "swap_amounts":
            return {}

        kwargs: dict[str, Any] = {}
        for metadata_key, kwarg_key in (
            ("swap_type", "intent_swap_type"),
            ("to_token_address", "token_out_address"),
            ("wallet_address", "wallet_address"),
        ):
            value = bundle_metadata.get(metadata_key)
            if value is not None and value != "":
                kwargs[kwarg_key] = value

        value = bundle_metadata.get("to_token_decimals")
        if value is not None:
            try:
                kwargs["token_out_decimals"] = int(value)
            except (TypeError, ValueError):
                logger.debug(
                    "Could not coerce to_token_decimals=%r to int; parser will fall back to constructor default",
                    value,
                )

        from_token_meta = bundle_metadata.get("from_token") or {}
        if isinstance(from_token_meta, dict):
            address = from_token_meta.get("address")
            if address:
                kwargs["token_in_address"] = address
            decimals = from_token_meta.get("decimals")
            if decimals is not None:
                try:
                    kwargs["token_in_decimals"] = int(decimals)
                except (TypeError, ValueError):
                    logger.debug(
                        "Could not coerce from_token.decimals=%r to int; parser will fall back to constructor default",
                        decimals,
                    )

        return kwargs

    @staticmethod
    def _build_redeem_legs_kwargs(bundle_metadata: dict[str, Any]) -> dict[str, Any]:
        """Thread the redeem compiler context into ``extract_primitive_money_legs``.

        The compiler's redeem ``ActionBundle.metadata`` carries the resolved PT
        token address (``pt_address``) and the underlying out-token descriptor
        (``out_token`` = ``{symbol, address, decimals}``). The receipt parser
        reverse-maps the PT address to the canonical maturity-bearing PT symbol
        and uses the out-token symbol/decimals for the OUTPUT leg. Each value is
        only threaded when present so a missing key degrades the parser rather
        than crashing the extraction (Empty != Zero).
        """
        kwargs: dict[str, Any] = {}
        pt_address = bundle_metadata.get("pt_address")
        if pt_address:
            kwargs["pt_address"] = pt_address
        out_token = bundle_metadata.get("out_token") or {}
        if isinstance(out_token, dict):
            out_symbol = out_token.get("symbol")
            if out_symbol:
                kwargs["out_token_symbol"] = out_symbol
            out_address = out_token.get("address")
            if out_address:
                kwargs["out_token_address"] = out_address
            out_decimals = out_token.get("decimals")
            if out_decimals is not None:
                try:
                    kwargs["out_token_decimals"] = int(out_decimals)
                except (TypeError, ValueError):
                    logger.debug(
                        "Could not coerce out_token.decimals=%r to int; redeem OUTPUT leg uses 18-decimal default",
                        out_decimals,
                    )
        return kwargs

    def parse_receipt(  # noqa: C901
        self,
        receipt: dict[str, Any],
        quoted_amount_out: int | None = None,
        intent_swap_type: str | None = None,
        token_in_address: str | None = None,
        token_out_address: str | None = None,
        token_in_decimals: int | None = None,
        token_out_decimals: int | None = None,
        wallet_address: str | None = None,
    ) -> ParseResult:
        """
        Parse a Pendle transaction receipt.

        Args:
            receipt: Transaction receipt dictionary
            quoted_amount_out: Expected output for slippage calculation
            intent_swap_type: Compiler-supplied swap_type ("token_to_yt",
                "yt_to_token", "token_to_pt", "pt_to_token"). Required for
                YT swaps to reconstruct user-facing amounts (VIB-3751).
            token_in_address: Compiler-supplied input token address (for YT).
            token_out_address: Compiler-supplied output token address (for YT).
            token_in_decimals: Compiler-supplied input token decimals (for YT).
                Falls back to ``self.token_in_decimals`` (constructor default).
            token_out_decimals: Compiler-supplied output token decimals (for YT).
                Falls back to ``self.token_out_decimals`` (constructor default).
            wallet_address: Compiler-supplied user wallet address (for YT).

        Returns:
            ParseResult with extracted events and swap data
        """
        try:
            # Extract transaction metadata
            tx_hash = receipt.get("transactionHash", "")
            if isinstance(tx_hash, bytes):
                tx_hash = "0x" + tx_hash.hex()

            block_number = receipt.get("blockNumber", 0)
            logs = receipt.get("logs", [])
            status = receipt.get("status", 1)
            tx_success = status == 1

            # Reverts must be reported before the empty-logs short-circuit,
            # otherwise an early-revert receipt (status=0, logs=[]) would be
            # silently surfaced as a successful empty receipt (issue #2064).
            if not tx_success:
                return ParseResult(
                    success=True,
                    transaction_hash=tx_hash,
                    block_number=block_number,
                    transaction_success=False,
                    error="Transaction reverted",
                )

            if not logs:
                return ParseResult(
                    success=True,
                    transaction_hash=tx_hash,
                    block_number=block_number,
                    transaction_success=tx_success,
                )

            # Parse all events
            events: list[PendleEvent] = []
            swap_events: list[SwapEventData] = []
            mint_events: list[MintEventData] = []
            burn_events: list[BurnEventData] = []
            redeem_events: list[RedeemPYEventData] = []
            redeem_sy_events: list[RedeemSYEventData] = []
            transfer_events: list[TransferEventData] = []

            for log in logs:
                parsed_event = self._parse_log(log, tx_hash, block_number)
                if parsed_event:
                    events.append(parsed_event)

                    # Extract typed data based on event type
                    if parsed_event.event_type == PendleEventType.SWAP:
                        swap_data = self._parse_swap_event(parsed_event)
                        if swap_data:
                            swap_events.append(swap_data)

                    elif parsed_event.event_type == PendleEventType.MINT:
                        mint_data = self._parse_mint_event(parsed_event)
                        if mint_data:
                            mint_events.append(mint_data)

                    elif parsed_event.event_type == PendleEventType.BURN:
                        burn_data = self._parse_burn_event(parsed_event)
                        if burn_data:
                            burn_events.append(burn_data)

                    elif parsed_event.event_type == PendleEventType.REDEEM_PY:
                        redeem_data = self._parse_redeem_event(parsed_event)
                        if redeem_data:
                            redeem_events.append(redeem_data)

                    elif parsed_event.event_type == PendleEventType.REDEEM_SY:
                        redeem_sy_data = self._parse_redeem_sy_event(parsed_event)
                        if redeem_sy_data:
                            redeem_sy_events.append(redeem_sy_data)

                    elif parsed_event.event_type == PendleEventType.TRANSFER:
                        transfer_data = self._parse_transfer_event(parsed_event)
                        if transfer_data:
                            transfer_events.append(transfer_data)

            # Build high-level swap result
            swap_result = None
            if swap_events:
                swap_result = self._build_swap_result(
                    swap_events[0],
                    transfer_events,
                    quoted_amount_out,
                    intent_swap_type=intent_swap_type,
                    token_in_address=token_in_address,
                    token_out_address=token_out_address,
                    token_in_decimals=token_in_decimals,
                    token_out_decimals=token_out_decimals,
                    wallet_address=wallet_address,
                )
            elif intent_swap_type in ("token_to_yt", "yt_to_token") and transfer_events:
                # VIB-5301: a YT swap's user-facing amounts live ONLY in Transfer
                # events (input token leaving the wallet, YT arriving — or the
                # reverse on a sell). The PendleMarket ``Swap`` event reflects an
                # internal flash-mint of PT and is NOT required to value the
                # user's YT trade; on real receipts it is frequently absent
                # entirely (limit-order fills, or markets whose AMM curve is
                # never touched — the report market emitted zero AMM Swap events
                # on mainnet yet still delivered YT to the wallet). Gating
                # reconstruction behind ``swap_events`` therefore silently
                # dropped the YT output amount, breaking ``amount="all"``
                # chaining (the runner reads ``swap_amounts.amount_out_decimal``)
                # and accounting. ``_build_yt_swap_result`` consumes only the
                # Transfer events and returns None when the amounts cannot be
                # reconstructed (Empty != Zero), so a missing leg stays
                # unmeasured rather than becoming a fabricated zero.
                swap_result = self._build_yt_swap_result(
                    intent_swap_type=intent_swap_type,
                    transfer_events=transfer_events,
                    token_in_address=token_in_address,
                    token_out_address=token_out_address,
                    token_in_decimals=token_in_decimals,
                    token_out_decimals=token_out_decimals,
                    wallet_address=wallet_address,
                    # No AMM Swap event ⇒ no market address available. The market
                    # address is only a label on ParsedSwapResult (not part of
                    # SwapAmounts), so an empty string is honest here.
                    market_address="",
                    quoted_amount_out=quoted_amount_out,
                )

            # Log parsed receipt
            logger.info(
                f"Parsed Pendle receipt: tx={tx_hash[:10]}..., "
                f"swaps={len(swap_events)}, mints={len(mint_events)}, "
                f"burns={len(burn_events)}, redeems={len(redeem_events)}, "
                f"redeem_sy={len(redeem_sy_events)}"
            )

            return ParseResult(
                success=True,
                events=events,
                swap_events=swap_events,
                mint_events=mint_events,
                burn_events=burn_events,
                redeem_events=redeem_events,
                redeem_sy_events=redeem_sy_events,
                transfer_events=transfer_events,
                swap_result=swap_result,
                transaction_hash=tx_hash,
                block_number=block_number,
                transaction_success=tx_success,
            )

        except Exception as e:
            logger.exception(f"Failed to parse Pendle receipt: {e}")
            return ParseResult(
                success=False,
                error=str(e),
            )

    def _parse_log(
        self,
        log: dict[str, Any],
        tx_hash: str,
        block_number: int,
    ) -> PendleEvent | None:
        """Parse a single log entry."""
        try:
            topics = log.get("topics", [])
            if not topics:
                return None

            # Normalize first topic
            first_topic = topics[0]
            if isinstance(first_topic, bytes):
                first_topic = "0x" + first_topic.hex()
            else:
                first_topic = str(first_topic)
            first_topic = first_topic.lower()

            # Check if known event
            event_name = self.registry.get_event_name(first_topic)
            if event_name is None:
                return None

            event_type = self.registry.get_event_type(event_name) or PendleEventType.UNKNOWN

            # Get raw data
            data = HexDecoder.normalize_hex(log.get("data", ""))

            # Normalize contract address
            contract_address = log.get("address", "")
            if isinstance(contract_address, bytes):
                contract_address = "0x" + contract_address.hex()

            # Convert topics to strings
            topics_str = []
            for topic in topics:
                if isinstance(topic, bytes):
                    topics_str.append("0x" + topic.hex())
                else:
                    topics_str.append(str(topic))

            # Decode log data
            parsed_data = self._decode_log_data(event_name, topics, data, contract_address)

            return PendleEvent(
                event_type=event_type,
                event_name=event_name,
                log_index=log.get("logIndex", 0),
                transaction_hash=tx_hash,
                block_number=block_number,
                contract_address=contract_address,
                data=parsed_data,
                raw_topics=topics_str,
                raw_data=data,
            )

        except Exception as e:
            logger.warning(f"Failed to parse log: {e}")
            return None

    def _decode_log_data(
        self,
        event_name: str,
        topics: list[Any],
        data: str,
        address: str,
    ) -> dict[str, Any]:
        """Decode log data based on event type."""
        if event_name == "Swap":
            return self._decode_swap_data(topics, data, address)
        elif event_name == "Mint":
            return self._decode_mint_data(topics, data, address)
        elif event_name == "Burn":
            return self._decode_burn_data(topics, data, address)
        elif event_name == "RedeemPY":
            return self._decode_redeem_data(topics, data, address)
        elif event_name == "RedeemSY":
            return self._decode_redeem_sy_data(topics, data, address)
        elif event_name == "Transfer":
            return self._decode_transfer_data(topics, data, address)
        else:
            return {"raw_data": data}

    def _decode_swap_data(
        self,
        topics: list[Any],
        data: str,
        address: str,
    ) -> dict[str, Any]:
        """
        Decode Pendle Swap event.

        Swap(address indexed caller, address indexed receiver, int256 ptToAccount, int256 syToAccount)
        """
        try:
            # Indexed: caller, receiver
            caller = HexDecoder.topic_to_address(topics[1]) if len(topics) > 1 else ""
            receiver = HexDecoder.topic_to_address(topics[2]) if len(topics) > 2 else ""

            # Non-indexed: ptToAccount (int256), syToAccount (int256)
            pt_to_account = HexDecoder.decode_int256(data, 0)
            sy_to_account = HexDecoder.decode_int256(data, 32)

            market_address = address.lower() if isinstance(address, str) else ""
            if isinstance(address, bytes):
                market_address = "0x" + address.hex()

            return {
                "caller": caller,
                "receiver": receiver,
                "pt_to_account": pt_to_account,
                "sy_to_account": sy_to_account,
                "market_address": market_address,
            }

        except Exception as e:
            logger.warning(f"Failed to decode Swap data: {e}")
            return {"raw_data": data}

    def _decode_mint_data(
        self,
        topics: list[Any],
        data: str,
        address: str,
    ) -> dict[str, Any]:
        """
        Decode Pendle Mint (LP) event.

        Mint(address indexed receiver, uint256 netLpMinted, uint256 netSyUsed, uint256 netPtUsed)
        """
        try:
            receiver = HexDecoder.topic_to_address(topics[1]) if len(topics) > 1 else ""

            net_lp_minted = HexDecoder.decode_uint256(data, 0)
            net_sy_used = HexDecoder.decode_uint256(data, 32)
            net_pt_used = HexDecoder.decode_uint256(data, 64)

            market_address = address.lower() if isinstance(address, str) else ""

            return {
                "receiver": receiver,
                "net_lp_minted": net_lp_minted,
                "net_sy_used": net_sy_used,
                "net_pt_used": net_pt_used,
                "market_address": market_address,
            }

        except Exception as e:
            logger.warning(f"Failed to decode Mint data: {e}")
            return {"raw_data": data}

    def _decode_burn_data(
        self,
        topics: list[Any],
        data: str,
        address: str,
    ) -> dict[str, Any]:
        """
        Decode Pendle Burn (LP removal) event.

        Burn(address indexed receiverSy, address indexed receiverPt, uint256 netLpToBurn, uint256 netSyOut, uint256 netPtOut)
        Topics: [hash, receiverSy, receiverPt]   Data: [netLpToBurn, netSyOut, netPtOut]
        """
        try:
            receiver_sy = HexDecoder.topic_to_address(topics[1]) if len(topics) > 1 else ""
            receiver_pt = HexDecoder.topic_to_address(topics[2]) if len(topics) > 2 else receiver_sy

            net_lp_burned = HexDecoder.decode_uint256(data, 0)
            net_sy_out = HexDecoder.decode_uint256(data, 32)
            net_pt_out = HexDecoder.decode_uint256(data, 64)

            market_address = address.lower() if isinstance(address, str) else ""

            return {
                "receiver_sy": receiver_sy,
                "receiver_pt": receiver_pt,
                "net_lp_burned": net_lp_burned,
                "net_sy_out": net_sy_out,
                "net_pt_out": net_pt_out,
                "market_address": market_address,
            }

        except Exception as e:
            logger.warning(f"Failed to decode Burn data: {e}")
            return {"raw_data": data}

    def _decode_redeem_data(
        self,
        topics: list[Any],
        data: str,
        address: str,
    ) -> dict[str, Any]:
        """
        Decode Pendle RedeemPY event.

        RedeemPY(address indexed caller, address indexed receiver, uint256 netPYRedeemed, uint256 netSYRedeemed)
        """
        try:
            caller = HexDecoder.topic_to_address(topics[1]) if len(topics) > 1 else ""
            receiver = HexDecoder.topic_to_address(topics[2]) if len(topics) > 2 else ""

            net_py_redeemed = HexDecoder.decode_uint256(data, 0)
            net_sy_redeemed = HexDecoder.decode_uint256(data, 32)

            yt_address = address.lower() if isinstance(address, str) else ""

            return {
                "caller": caller,
                "receiver": receiver,
                "net_py_redeemed": net_py_redeemed,
                "net_sy_redeemed": net_sy_redeemed,
                "yt_address": yt_address,
            }

        except Exception as e:
            logger.warning(f"Failed to decode RedeemPY data: {e}")
            return {"raw_data": data}

    def _decode_redeem_sy_data(
        self,
        topics: list[Any],
        data: str,
        address: str,
    ) -> dict[str, Any]:
        """
        Decode Pendle SY Redeem event (post-maturity PT redemption path).

        Redeem(address indexed caller, address indexed receiver, address indexed tokenOut,
               uint256 amountSyToRedeem, uint256 amountTokenOut)
        Topics: [hash, caller, receiver, tokenOut]   Data: [amountSyToRedeem, amountTokenOut]
        """
        try:
            caller = HexDecoder.topic_to_address(topics[1]) if len(topics) > 1 else ""
            receiver = HexDecoder.topic_to_address(topics[2]) if len(topics) > 2 else ""
            token_out = HexDecoder.topic_to_address(topics[3]) if len(topics) > 3 else ""

            amount_sy_to_redeem = HexDecoder.decode_uint256(data, 0)
            amount_token_out = HexDecoder.decode_uint256(data, 32)

            sy_address = address.lower() if isinstance(address, str) else ""

            return {
                "caller": caller,
                "receiver": receiver,
                "token_out": token_out,
                "amount_sy_to_redeem": amount_sy_to_redeem,
                "amount_token_out": amount_token_out,
                "sy_address": sy_address,
            }

        except Exception as e:
            logger.warning(f"Failed to decode RedeemSY data: {e}")
            return {"raw_data": data}

    def _decode_transfer_data(
        self,
        topics: list[Any],
        data: str,
        address: str,
    ) -> dict[str, Any]:
        """Decode ERC20 Transfer event."""
        try:
            from_addr = HexDecoder.topic_to_address(topics[1]) if len(topics) > 1 else ""
            to_addr = HexDecoder.topic_to_address(topics[2]) if len(topics) > 2 else ""
            value = HexDecoder.decode_uint256(data, 0)

            token_address = address.lower() if isinstance(address, str) else ""
            if isinstance(address, bytes):
                token_address = "0x" + address.hex()

            return {
                "from_addr": from_addr,
                "to_addr": to_addr,
                "value": value,
                "token_address": token_address,
            }

        except Exception as e:
            logger.warning(f"Failed to decode Transfer data: {e}")
            return {"raw_data": data}

    def _parse_swap_event(self, event: PendleEvent) -> SwapEventData | None:
        """Parse Swap event into typed data."""
        try:
            data = event.data
            return SwapEventData(
                caller=data.get("caller", ""),
                receiver=data.get("receiver", ""),
                pt_to_account=data.get("pt_to_account", 0),
                sy_to_account=data.get("sy_to_account", 0),
                market_address=data.get("market_address", event.contract_address),
            )
        except Exception as e:
            logger.warning(f"Failed to parse SwapEventData: {e}")
            return None

    def _parse_mint_event(self, event: PendleEvent) -> MintEventData | None:
        """Parse Mint event into typed data."""
        try:
            data = event.data
            return MintEventData(
                receiver=data.get("receiver", ""),
                net_lp_minted=data.get("net_lp_minted", 0),
                net_sy_used=data.get("net_sy_used", 0),
                net_pt_used=data.get("net_pt_used", 0),
                market_address=data.get("market_address", event.contract_address),
            )
        except Exception as e:
            logger.warning(f"Failed to parse MintEventData: {e}")
            return None

    def _parse_burn_event(self, event: PendleEvent) -> BurnEventData | None:
        """Parse Burn event into typed data."""
        try:
            data = event.data
            return BurnEventData(
                receiver_sy=data.get("receiver_sy", ""),
                receiver_pt=data.get("receiver_pt", ""),
                net_lp_burned=data.get("net_lp_burned", 0),
                net_sy_out=data.get("net_sy_out", 0),
                net_pt_out=data.get("net_pt_out", 0),
                market_address=data.get("market_address", event.contract_address),
            )
        except Exception as e:
            logger.warning(f"Failed to parse BurnEventData: {e}")
            return None

    def _parse_redeem_event(self, event: PendleEvent) -> RedeemPYEventData | None:
        """Parse RedeemPY event into typed data."""
        try:
            data = event.data
            return RedeemPYEventData(
                caller=data.get("caller", ""),
                receiver=data.get("receiver", ""),
                net_py_redeemed=data.get("net_py_redeemed", 0),
                net_sy_redeemed=data.get("net_sy_redeemed", 0),
                yt_address=data.get("yt_address", event.contract_address),
            )
        except Exception as e:
            logger.warning(f"Failed to parse RedeemPYEventData: {e}")
            return None

    def _parse_redeem_sy_event(self, event: PendleEvent) -> RedeemSYEventData | None:
        """Parse RedeemSY event into typed data."""
        try:
            data = event.data
            return RedeemSYEventData(
                caller=data.get("caller", ""),
                receiver=data.get("receiver", ""),
                token_out=data.get("token_out", ""),
                amount_sy_to_redeem=data.get("amount_sy_to_redeem", 0),
                amount_token_out=data.get("amount_token_out", 0),
                sy_address=data.get("sy_address", event.contract_address),
            )
        except Exception as e:
            logger.warning(f"Failed to parse RedeemSYEventData: {e}")
            return None

    def _parse_transfer_event(self, event: PendleEvent) -> TransferEventData | None:
        """Parse Transfer event into typed data."""
        try:
            data = event.data
            return TransferEventData(
                from_addr=data.get("from_addr", ""),
                to_addr=data.get("to_addr", ""),
                value=data.get("value", 0),
                token_address=data.get("token_address", event.contract_address),
            )
        except Exception as e:
            logger.warning(f"Failed to parse TransferEventData: {e}")
            return None

    def _build_swap_result(
        self,
        swap_event: SwapEventData,
        transfer_events: list[TransferEventData],
        quoted_amount_out: int | None,
        intent_swap_type: str | None = None,
        token_in_address: str | None = None,
        token_out_address: str | None = None,
        token_in_decimals: int | None = None,
        token_out_decimals: int | None = None,
        wallet_address: str | None = None,
    ) -> ParsedSwapResult:
        """Build high-level swap result from events.

        For PT swaps the Pendle Market Swap event directly reflects user-facing
        amounts (SY <-> PT). For YT swaps (token_to_yt / yt_to_token) the Swap
        event reflects an *internal* PT flash-mint+sell that the router uses to
        synthesize the YT exposure — its amounts are NOT what the user paid or
        received. To get user-facing amounts for YT swaps we MUST derive them
        from Transfer events (input token leaving wallet, YT arriving at wallet
        for buys; YT leaving wallet, output token arriving for sells).

        VIB-3751: prior to this fix, YT swaps were silently misclassified as
        PT sells, producing inflated ``amount_in`` (~the flash-minted PT
        amount) which corrupted reconciliation, valuation, and the QA harness
        ``deployed_usd`` column.

        Args:
            swap_event: Pendle Market Swap event (used for PT swaps).
            transfer_events: All Transfer events from the receipt — required
                for YT swap reconstruction.
            quoted_amount_out: Optional pre-trade quote (raw int) for slippage.
            intent_swap_type: Compiler-supplied swap_type ("token_to_yt",
                "yt_to_token", "token_to_pt", "pt_to_token"). When provided
                this overrides the legacy PT-direction inference.
            token_in_address: Compiler-supplied input token address (for YT).
            token_out_address: Compiler-supplied output token address (for YT).
            wallet_address: Compiler-supplied user wallet (recipient/payer for YT).
        """
        # ----------------------------------------------------------------
        # YT swap path — Market Swap event is misleading (reflects internal
        # flash-mint+sell of PT, not the user's YT trade). Reconstruct
        # user-facing amounts from Transfer events using the compiler's
        # context (intent_swap_type + addresses + wallet).
        # ----------------------------------------------------------------
        if intent_swap_type in ("token_to_yt", "yt_to_token"):
            yt_result = self._build_yt_swap_result(
                intent_swap_type=intent_swap_type,
                transfer_events=transfer_events,
                token_in_address=token_in_address,
                token_out_address=token_out_address,
                token_in_decimals=token_in_decimals,
                token_out_decimals=token_out_decimals,
                wallet_address=wallet_address,
                market_address=swap_event.market_address,
                quoted_amount_out=quoted_amount_out,
            )
            if yt_result is not None:
                logger.debug(
                    "Pendle YT swap reconstructed: amount_in=%s, amount_out=%s, swap_type=%s",
                    yt_result.amount_in_decimal,
                    yt_result.amount_out_decimal,
                    yt_result.swap_type,
                )
                return yt_result
            # Fall through to legacy path on reconstruction failure (best-effort
            # signal preserved with explicit warning rather than silently
            # producing nonsense PT-shaped amounts).
            logger.warning(
                "Pendle YT swap %s: could not reconstruct user-facing amounts "
                "from Transfer events; falling back to PT-shaped Swap event "
                "(amounts will be the internal flash-mint values, not user-facing). "
                "Check token_in_address=%s, token_out_address=%s, wallet_address=%s.",
                intent_swap_type,
                token_in_address,
                token_out_address,
                wallet_address,
            )

        # ----------------------------------------------------------------
        # PT swap path — Market Swap event reflects user-facing SY <-> PT.
        # If the compiler tagged the intent as PT we trust it; otherwise we
        # fall back to the legacy ``is_buy_pt`` sign-based inference.
        # ----------------------------------------------------------------
        is_buying_pt: bool
        if intent_swap_type == "token_to_pt":
            is_buying_pt = True
        elif intent_swap_type == "pt_to_token":
            is_buying_pt = False
        else:
            is_buying_pt = swap_event.is_buy_pt

        # G-PT0: stamp the FULL maturity-bearing PT symbol (resolved from the
        # compiler-supplied PT token address) instead of the generic ``"PT"``
        # placeholder, and the real underlying symbol instead of ``"SY"`` where
        # resolvable. This carries truth to ``transaction_ledger.token_in`` /
        # ``token_out`` so the accounting categorizer claims the trade and
        # ``_parse_pt_maturity`` can read the expiry. Degrade to the generic
        # label (never a fabricated symbol) when the address is unknown.
        if is_buying_pt:
            # base/SY -> PT swap: PT is the OUTPUT leg.
            amount_in = swap_event.sy_amount
            amount_out = swap_event.pt_amount
            swap_type = "buy_pt"
            token_in = _resolve_base_symbol(self.chain, token_in_address) or "SY"
            token_out = _resolve_pt_symbol(self.chain, token_out_address) or "PT"
            in_decimals = self.token_in_decimals
            out_decimals = self.token_out_decimals
        else:
            # PT -> base/SY swap: PT is the INPUT leg.
            amount_in = swap_event.pt_amount
            amount_out = swap_event.sy_amount
            swap_type = "sell_pt"
            token_in = _resolve_pt_symbol(self.chain, token_in_address) or "PT"
            token_out = _resolve_base_symbol(self.chain, token_out_address) or "SY"
            in_decimals = self.token_out_decimals
            out_decimals = self.token_in_decimals

        # Convert to decimal
        amount_in_decimal = Decimal(str(amount_in)) / Decimal(10**in_decimals)
        amount_out_decimal = Decimal(str(amount_out)) / Decimal(10**out_decimals)

        # Calculate effective price
        if amount_in_decimal > 0:
            effective_price = amount_out_decimal / amount_in_decimal
        else:
            effective_price = Decimal("0")

        # Calculate slippage
        slippage_bps = 0
        if quoted_amount_out and quoted_amount_out > 0:
            slippage_pct_f = (quoted_amount_out - amount_out) / quoted_amount_out
            slippage_bps = int(slippage_pct_f * 10000)
        elif self.quoted_price and self.quoted_price > 0:
            slippage_pct_d = (self.quoted_price - effective_price) / self.quoted_price
            slippage_bps = int(slippage_pct_d * 10000)

        return ParsedSwapResult(
            token_in=token_in,
            token_out=token_out,
            amount_in=amount_in,
            amount_out=amount_out,
            amount_in_decimal=amount_in_decimal,
            amount_out_decimal=amount_out_decimal,
            effective_price=effective_price,
            slippage_bps=slippage_bps,
            market_address=swap_event.market_address,
            swap_type=swap_type,
        )

    def _build_yt_swap_result(
        self,
        *,
        intent_swap_type: str,
        transfer_events: list[TransferEventData],
        token_in_address: str | None,
        token_out_address: str | None,
        wallet_address: str | None,
        market_address: str,
        quoted_amount_out: int | None,
        token_in_decimals: int | None = None,
        token_out_decimals: int | None = None,
    ) -> ParsedSwapResult | None:
        """Reconstruct user-facing amounts for a Pendle YT swap (VIB-3751).

        For ``token_to_yt`` (buy YT):
            - amount_in  = sum of token_in Transfers FROM wallet
            - amount_out = sum of token_out (YT) Transfers TO wallet
        For ``yt_to_token`` (sell YT):
            - amount_in  = sum of token_in (YT) Transfers FROM wallet
            - amount_out = sum of token_out Transfers TO wallet

        Returns None when required addresses are missing or when no matching
        Transfer events are present in the receipt — caller falls back to a
        legacy path with an explicit warning rather than producing wrong data.
        """
        if not token_in_address or not token_out_address or not wallet_address:
            return None

        wallet = wallet_address.lower()
        addr_in = token_in_address.lower()
        addr_out = token_out_address.lower()

        # Compute NET wallet-balance changes from Transfer events. We can't
        # rely on a single "user-facing" transfer because the Pendle router
        # may emit multiple Transfers (e.g., a partial refund of unused
        # input). The robust signal is the net delta:
        #   amount_in  = (token_in sent FROM wallet) - (token_in returned TO wallet)
        #   amount_out = (token_out received TO wallet) - (token_out leaving wallet)
        # A net of zero means there was no user-facing trade for that token —
        # caller falls back to legacy parsing rather than report nonsense.
        net_in = 0  # signed: positive when net flows OUT of wallet
        net_out = 0  # signed: positive when net flows IN to wallet
        for t in transfer_events:
            t_token = (t.token_address or "").lower()
            t_from = (t.from_addr or "").lower()
            t_to = (t.to_addr or "").lower()
            value = int(t.value or 0)
            if value <= 0:
                continue
            if t_token == addr_in:
                if t_from == wallet:
                    net_in += value
                elif t_to == wallet:
                    net_in -= value
            elif t_token == addr_out:
                if t_to == wallet:
                    net_out += value
                elif t_from == wallet:
                    net_out -= value

        amount_in = net_in
        amount_out = net_out

        if amount_in <= 0 or amount_out <= 0:
            return None

        # Prefer caller-supplied decimals (compiler bundle metadata) over the
        # parser's constructor-time defaults — the enricher instantiates
        # PendleReceiptParser with chain only, leaving the constructor
        # decimals at their 18-decimal fallbacks. Without this override,
        # non-18-decimal Pendle markets (e.g., Plasma fUSDT0 = 6) report
        # amounts off by 10^12. (Codex audit P1.)
        in_decimals = token_in_decimals if token_in_decimals is not None else self.token_in_decimals
        out_decimals = token_out_decimals if token_out_decimals is not None else self.token_out_decimals

        amount_in_decimal = Decimal(str(amount_in)) / Decimal(10**in_decimals)
        amount_out_decimal = Decimal(str(amount_out)) / Decimal(10**out_decimals)

        effective_price = amount_out_decimal / amount_in_decimal if amount_in_decimal > 0 else Decimal("0")

        slippage_bps = 0
        if quoted_amount_out and quoted_amount_out > 0:
            slippage_bps = int((Decimal(quoted_amount_out) - Decimal(amount_out)) / Decimal(quoted_amount_out) * 10000)
        elif self.quoted_price and self.quoted_price > 0:
            slippage_bps = int((self.quoted_price - effective_price) / self.quoted_price * 10000)

        # Surface user-facing labels: for buy_yt the input is the underlying
        # (e.g. "sUSDe") and the output is "YT"; vice versa for sell_yt.
        if intent_swap_type == "token_to_yt":
            swap_type = "buy_yt"
            token_in = "TOKEN"
            token_out = "YT"
        else:
            swap_type = "sell_yt"
            token_in = "YT"
            token_out = "TOKEN"

        return ParsedSwapResult(
            token_in=token_in,
            token_out=token_out,
            amount_in=amount_in,
            amount_out=amount_out,
            amount_in_decimal=amount_in_decimal,
            amount_out_decimal=amount_out_decimal,
            effective_price=effective_price,
            slippage_bps=slippage_bps,
            market_address=market_address,
            swap_type=swap_type,
        )

    # =========================================================================
    # Extraction Methods (for Result Enrichment)
    # =========================================================================

    def extract_swap_amounts(
        self,
        receipt: dict[str, Any],
        *,
        expected_out: Decimal | None = None,
        intent_swap_type: str | None = None,
        token_in_address: str | None = None,
        token_out_address: str | None = None,
        token_in_decimals: int | None = None,
        token_out_decimals: int | None = None,
        wallet_address: str | None = None,
    ) -> SwapAmounts | None:
        """
        Extract swap amounts from receipt for Result Enrichment.

        Called by the framework after SWAP execution to populate
        ExecutionResult.swap_amounts.

        Args:
            receipt: Transaction receipt dict.
            expected_out: VIB-3203 — pre-slippage-discount quote in human
                (Decimal) units from the compiler's ActionBundle metadata.
                Overrides the parser's internal ``slippage_bps``, which is
                ``None`` on the enrichment path because constructor
                ``quoted_price`` isn't available there.
            intent_swap_type: VIB-3751 — compiler-supplied swap_type
                ("token_to_yt", "yt_to_token", "token_to_pt", "pt_to_token").
                Required for YT swaps to reconstruct user-facing amounts;
                without it the parser falls back to the legacy PT-direction
                inference which is wrong for YT trades.
            token_in_address: Input token address from compiler (for YT).
            token_out_address: Output token address from compiler (for YT).
            token_in_decimals: Input token decimals from compiler. Required
                for non-18-decimal markets (e.g., Plasma fUSDT0 = 6) so that
                YT amount reconstruction does not mis-scale by 10^12.
            token_out_decimals: Output token decimals from compiler.
            wallet_address: User wallet from compiler (for YT).

        Returns:
            SwapAmounts dataclass or None if not found
        """
        try:
            result = self.parse_receipt(
                receipt,
                intent_swap_type=intent_swap_type,
                token_in_address=token_in_address,
                token_out_address=token_out_address,
                token_in_decimals=token_in_decimals,
                token_out_decimals=token_out_decimals,
                wallet_address=wallet_address,
            )
            if not result.swap_result:
                return None

            sr = result.swap_result

            # VIB-3203: compute realized slippage from framework-supplied quote.
            slippage_bps = sr.slippage_bps
            if expected_out is not None and expected_out > 0 and sr.amount_out_decimal > 0:
                realized_slippage = (expected_out - sr.amount_out_decimal) / expected_out
                slippage_bps = int(realized_slippage * Decimal(10_000))

            return SwapAmounts(
                amount_in=sr.amount_in,
                amount_out=sr.amount_out,
                amount_in_decimal=sr.amount_in_decimal,
                amount_out_decimal=sr.amount_out_decimal,
                effective_price=sr.effective_price,
                slippage_bps=slippage_bps,
                expected_out_decimal=expected_out,
                token_in=sr.token_in,
                token_out=sr.token_out,
            )

        except Exception as e:
            logger.warning(f"Failed to extract swap amounts: {e}")
            return None

    def extract_lp_minted(self, receipt: dict[str, Any]) -> int | None:
        """
        Extract LP tokens minted from receipt.

        Called by the framework after LP_OPEN execution.
        """
        try:
            result = self.parse_receipt(receipt)
            if result.mint_events:
                return result.mint_events[0].net_lp_minted
            return None
        except Exception as e:
            logger.warning(f"Failed to extract LP minted: {e}")
            return None

    def extract_lp_burned(self, receipt: dict[str, Any]) -> int | None:
        """
        Extract LP tokens burned from receipt.

        Called by the framework after LP_CLOSE execution.
        """
        try:
            result = self.parse_receipt(receipt)
            if result.burn_events:
                return result.burn_events[0].net_lp_burned
            return None
        except Exception as e:
            logger.warning(f"Failed to extract LP burned: {e}")
            return None

    def extract_position_id(self, receipt: dict[str, Any]) -> str | None:
        """Extract the Pendle LP market address as the position_id.

        Returns the market address as a lowercase hex string (e.g. "0xabcd...").
        This is the stable position identifier for Pendle's fungible LP: there is
        no NFT tokenId, so the market address itself is the unique position key.
        Checks mint_events first (LP_OPEN), falls back to burn_events (LP_CLOSE).
        """
        try:
            result = self.parse_receipt(receipt)
            if result.mint_events:
                return result.mint_events[0].market_address.lower()
            if result.burn_events:
                return result.burn_events[0].market_address.lower()
            return None
        except Exception as e:
            logger.warning(f"Failed to extract position_id from Pendle receipt: {e}")
            return None

    def extract_lp_open_data(self, receipt: dict[str, Any]) -> LPOpenData | None:
        """Extract LPOpenData from a Pendle LP Mint receipt (LP_OPEN enrichment).

        Maps MintEventData to the standard LPOpenData structure:
          amount0     = net_sy_used  (raw SY tokens deposited)
          amount1     = net_pt_used  (raw PT tokens deposited)
          liquidity   = net_lp_minted (raw LP tokens minted)
          position_id = 0 (Pendle has no NFT tokenId; canonical position_id is
                          the market address hex string from extract_position_id)
        """
        try:
            result = self.parse_receipt(receipt)
            if not result.mint_events:
                return None
            mint = result.mint_events[0]
            return LPOpenData(
                position_id=0,
                liquidity=mint.net_lp_minted,
                amount0=mint.net_sy_used,
                amount1=mint.net_pt_used,
            )
        except Exception as e:
            logger.warning(f"Failed to extract lp_open_data: {e}")
            return None

    def extract_lp_close_data(self, receipt: dict[str, Any]) -> LPCloseData | None:
        """Extract LPCloseData from a Pendle LP Burn receipt (LP_CLOSE enrichment).

        Maps BurnEventData to the standard LPCloseData structure:
          amount0_collected = net_sy_out   (raw SY tokens received)
          amount1_collected = net_pt_out   (raw PT tokens received)
          liquidity_removed = net_lp_burned (raw LP tokens burned)
        """
        try:
            result = self.parse_receipt(receipt)
            if not result.burn_events:
                return None
            burn = result.burn_events[0]
            return LPCloseData(
                amount0_collected=burn.net_sy_out,
                amount1_collected=burn.net_pt_out,
                # VIB-4470 — Pendle's Burn event does not surface fees
                # separately from the SY/PT amounts out; emit ``None``
                # (unmeasured) rather than relying on the prior measured-zero
                # default (Empty ≠ Zero).
                fees0=None,
                fees1=None,
                liquidity_removed=burn.net_lp_burned,
            )
        except Exception as e:
            logger.warning(f"Failed to extract lp_close_data: {e}")
            return None

    # =========================================================================
    # Tagged-variant (ExtractResult) extractors — VIB-5354
    #
    # These are the fail-closed counterparts of the legacy ``extract_*`` methods
    # above. They distinguish three outcomes the legacy ``X | None`` shape
    # collapses into a single ``None`` (VIB-3159 / VIB-5354):
    #   * ExtractOk(value)  — event present and parsed
    #   * ExtractMissing()  — event genuinely absent from the receipt (benign)
    #   * ExtractError()    — parse_receipt raised OR reported failure, OR the
    #                         extractor itself raised (accounting-broken — a
    #                         silent parse failure on a money path must never be
    #                         indistinguishable from "no event").
    #
    # The ``ResultEnricher`` prefers these ``*_result`` variants over the raw
    # methods (see ``ResultEnricher._invoke_extract`` /
    # ``_class_has_method``), so the enricher consumes the tagged signal with no
    # backward-compat ``DeprecationWarning``. The raw methods are retained
    # unchanged for direct callers (strategies, intent tests) that expect the
    # legacy return type — mirroring the canonical aave_v3 migration.
    # =========================================================================

    def _strict_parse(self, receipt: dict[str, Any]) -> ExtractError | None:
        """Run ``parse_receipt`` and return ``ExtractError`` if it crashed or
        reported failure; ``None`` when parsing succeeded.

        ``parse_receipt`` swallows decode exceptions internally and returns a
        ``ParseResult(success=False, error=...)`` (see its ``except`` branch),
        so both the raising case and the reported-failure case must be checked
        to surface a parse error rather than treating it as "no event"
        (VIB-3159 / VIB-5354). Canonical idiom shared with aave_v3 / uniswap_v3.
        """
        try:
            parsed = self.parse_receipt(receipt)
        except Exception as exc:  # noqa: BLE001 — malformed receipt shape
            return ExtractError(error=f"{type(exc).__name__}: {exc}", exception=exc)
        if not parsed.success:
            return ExtractError(error=parsed.error or "parse_receipt reported failure")
        return None

    def _wrap_extract(
        self,
        fn: Any,
        receipt: dict[str, Any],
        missing_reason: str,
        **kwargs: Any,
    ) -> ExtractResult[Any]:
        """Shared wrapper for the no-extra-kwargs tagged extractors.

        Runs ``_strict_parse`` first so an actual parse crash / reported failure
        propagates as ``ExtractError`` rather than being silently swallowed by
        the legacy extractor's ``except Exception: return None``. A ``None``
        from the legacy method then means a genuinely absent event
        (``ExtractMissing``); any value is wrapped in ``ExtractOk``.

        NOTE: ``_strict_parse`` probes ``parse_receipt(receipt)`` with NO extra
        kwargs. This is only sound for extractors whose ``**kwargs`` do not reach
        ``parse_receipt`` (true for all four migrated methods — their kwargs are
        post-parse, e.g. ``pt_address`` / ``out_token_*``). Before migrating an
        extractor whose kwargs DO change parsing (e.g. ``swap_amounts`` with
        ``intent_swap_type``), make ``_strict_parse`` forward those kwargs, or the
        probe and the real call could disagree.
        """
        err = self._strict_parse(receipt)
        if err is not None:
            return err
        try:
            value = fn(receipt, **kwargs)
        except Exception as exc:  # noqa: BLE001 — extractor crash is accounting-critical
            return ExtractError(error=f"{type(exc).__name__}: {exc}", exception=exc)
        if value is None:
            return ExtractMissing(reason=missing_reason)
        return ExtractOk(value=value)

    def extract_position_id_result(self, receipt: dict[str, Any]) -> ExtractResult[str]:
        """Fail-closed variant of :meth:`extract_position_id` — see VIB-5354."""
        return self._wrap_extract(self.extract_position_id, receipt, "no Pendle Mint/Burn event in receipt")

    def extract_lp_open_data_result(self, receipt: dict[str, Any]) -> "ExtractResult[LPOpenData]":
        """Fail-closed variant of :meth:`extract_lp_open_data` — see VIB-5354."""
        return self._wrap_extract(self.extract_lp_open_data, receipt, "no Pendle Mint event in receipt")

    def extract_lp_close_data_result(self, receipt: dict[str, Any]) -> "ExtractResult[LPCloseData]":
        """Fail-closed variant of :meth:`extract_lp_close_data` — see VIB-5354."""
        return self._wrap_extract(self.extract_lp_close_data, receipt, "no Pendle Burn event in receipt")

    @staticmethod
    def _pt_transfer_amount(
        transfer_events: "list[TransferEventData]",
        pt_address: str | None,
    ) -> int | None:
        """Return the PT TOKEN COUNT (raw 18-dec int) the redeem burned, from the
        PT-token ERC20 ``Transfer`` log — or ``None`` when not resolvable.

        AT/AFTER maturity ``redeemPyToToken`` emits no ``RedeemPY`` (only the SY
        ``Redeem``), so the PT count must be read off the PT token's own
        ``Transfer`` log: the wallet sends its PT to the router and the router
        burns it (Transfer ``to`` == 0x0). Matching on ``token_address ==
        pt_address`` isolates the PT leg from the SY / underlying transfers in the
        same receipt; this value IS the PT count in the SAME 18-dec basis the
        PT_BUY's PT ``amount_out`` uses, so PT-quantity conserves (PEN6).

        Picks the largest matching transfer value — a redeem of the full balance
        emits one PT transfer; the ``max`` guards against incidental dust/zero
        transfers without assuming log ordering. Returns ``None`` when no PT
        transfer is present (Empty != Zero — the caller degrades rather than
        fabricating the count from the SY-asset amount).
        """
        if not pt_address:
            return None
        target = pt_address.lower()
        values = [t.value for t in transfer_events if (t.token_address or "").lower() == target and t.value > 0]
        return max(values) if values else None

    def extract_primitive_money_legs(
        self,
        receipt: dict[str, Any],
        *,
        pt_address: str | None = None,
        out_token_symbol: str | None = None,
        out_token_address: str | None = None,
        out_token_decimals: int | None = None,
    ) -> "PrimitiveMoneyLegs | None":
        """Declare the money legs for a Pendle redeem or LP_CLOSE (G-PT / VIB-4988, VIB-5302).

        Two intents route through this single ``primitive_money_legs`` extractor;
        the Market ``Burn`` event (emitted only by an LP removal, never by a PT
        redeem) discriminates them. An LP_CLOSE delegates to
        :meth:`_lp_close_money_legs` (the received-underlying OUTPUT leg); the
        remainder of this method handles the PT-redeem WITHDRAW path described below.

        A Pendle PT redeem is a WITHDRAW intent whose compiler names
        ``intent.token`` = the *underlying* out token and ``intent.market_id`` =
        the YT address; the PT token is on *neither*. So the legacy ledger
        guesser (lending path) lands ``token_in`` = underlying / ``token_out`` =
        ``""`` and the canonical PT symbol — the only cross-boundary join key the
        accounting FIFO matcher and the position-events lifecycle use — never
        reaches the row. The PT redeem also emits ``RedeemPY`` / ``RedeemSY``,
        never a Market ``Swap``, so the swap-path PT-symbol resolution
        (:func:`_resolve_pt_symbol` inside :meth:`_build_swap_result`) never fires.

        This declares the redeem's two money legs as a typed
        :class:`PrimitiveMoneyLegs` — the contract the US-009 ledger dispatcher
        prefers over its legacy guess (blueprint 27 §6.6) — so the row carries:

        * INPUT  — the canonical maturity-bearing PT symbol (reverse-mapped from
          the compiler-supplied ``pt_address`` via :func:`_resolve_pt_symbol`),
          amount = the PT TOKEN COUNT (18-dec), basis-identical to the PT_BUY's PT
          ``amount_out`` so PT-quantity conserves (PEN6). Sourced from
          ``RedeemPY.net_py_redeemed`` (the netPYRedeemed PT count, PRE-maturity)
          or, at/after maturity where no RedeemPY fires, the PT-token ``Transfer``
          value (:meth:`_pt_transfer_amount`). The SY ``Redeem.amountSyToRedeem``
          is SY-ASSET denominated (≈ PT × SY-rate), NOT the PT count, and is never
          used for this leg — that mismatch was the PEN6 basis break this closes.
        * OUTPUT — the underlying token received (``out_token_symbol``), amount =
          ``amount_token_out`` from the SY ``Redeem`` event when present.

        Empty != Zero (blueprint 27 §10.10): when ``_resolve_pt_symbol`` returns
        ``None`` (unknown catalogue address), or no PT count is resolvable (no
        RedeemPY and no PT Transfer), this returns ``None`` so the dispatcher falls
        back to its legacy path and the accounting builder's R6 degrade fires
        (ESTIMATED + unavailable_reason) — a PT symbol / count is never fabricated,
        and the SY-asset amount is never recorded as the PT count. The OUTPUT
        amount is UNMEASURED (never a measured zero, never an SY proxy) when no
        ``Redeem`` event carries a measured ``amount_token_out``.

        Returns ``None`` for a non-PT Pendle withdraw (no resolvable PT symbol /
        count), so a YT redeem / non-PT path is unchanged.

        Args:
            receipt: Transaction receipt dict with a ``logs`` field.
            pt_address: Compiler-resolved PT token address (from redeem metadata).
            out_token_symbol: Underlying out-token symbol (from redeem metadata).
            out_token_address: Underlying out-token address (unused today; kept
                for symmetry with the swap-path resolution).
            out_token_decimals: Underlying out-token decimals (defaults to 18).

        Returns:
            A :class:`PrimitiveMoneyLegs` declaring the INPUT/OUTPUT legs, or
            ``None`` when the receipt is not a resolvable PT redeem.
        """
        from almanak.connectors._strategy_base.primitive_money_leg import PrimitiveMoneyLeg, PrimitiveMoneyLegs
        from almanak.framework.accounting.measured import MeasuredMoney

        try:
            result = self.parse_receipt(receipt)
        except Exception as e:
            logger.warning(f"Failed to extract primitive money legs: {e}")
            return None

        # VIB-5302 — LP_CLOSE branch. A Pendle LP removal emits a Market ``Burn``
        # event; a PT redeem never does, so the Burn is the reliable discriminator
        # between the two intents that both route through this
        # ``primitive_money_legs`` extractor. Declare the received-underlying
        # OUTPUT leg (the realized proceeds the wallet gets back), which the legacy
        # LP_CLOSE ledger path drops for Pendle. Must be checked BEFORE the
        # PT-symbol gate below (an LP close threads no ``pt_address``).
        if result.burn_events:
            return self._lp_close_money_legs(
                result,
                out_token_symbol=out_token_symbol,
                out_token_decimals=out_token_decimals,
            )

        pt_symbol = _resolve_pt_symbol(self.chain, pt_address)
        if pt_symbol is None:
            # Unknown catalogue address (or no pt_address threaded): never
            # fabricate a PT symbol. Fall back to the legacy dispatcher path so
            # the accounting R6 degrade fires rather than a wrong key.
            logger.debug(
                "Pendle redeem: could not resolve PT symbol from pt_address=%r on chain=%s; "
                "falling back to legacy ledger extraction (Empty != Zero).",
                pt_address,
                self.chain,
            )
            return None

        # PT redeemed (INPUT leg) — MUST be the PT TOKEN COUNT, basis-identical to
        # the PT_BUY's PT ``amount_out`` so PT-quantity conserves (PEN6). PT/YT are
        # always 18 decimals on Pendle. Source priority (PEN6 / VIB-4988):
        #   1. ``RedeemPY.net_py_redeemed`` — the netPYRedeemed field IS the PT
        #      token count, emitted by the YT on the PRE-maturity redeem path.
        #   2. The PT-token ERC20 ``Transfer`` value (token_address == pt_address)
        #      — the user→router/burn transfer is the PT token count, the robust
        #      source AT/AFTER maturity where ``redeemPyToToken`` emits only the SY
        #      ``Redeem`` (no RedeemPY). The SY ``Redeem.amountSyToRedeem`` is
        #      SY-ASSET denominated (≈ PT × SY-exchange-rate), NOT the PT count, so
        #      it must NEVER seed the PT leg (the basis bug this fix closes).
        #   3. Otherwise UNMEASURED-degrade → return None (Empty != Zero): never
        #      record the asset-denominated amount as the PT count.
        py_raw: int | None = None
        if result.redeem_events:
            py_raw = result.redeem_events[0].net_py_redeemed
        else:
            py_raw = self._pt_transfer_amount(result.transfer_events, pt_address)
        if py_raw is None:
            # Neither a RedeemPY token count nor a resolvable PT Transfer — do not
            # fabricate the PT count from the SY-asset amount. Fall back to the
            # legacy dispatcher path (R6 degrade) rather than book a wrong basis.
            logger.debug(
                "Pendle redeem: no PT token count available (no RedeemPY event and no PT Transfer "
                "for pt_address=%r); falling back to legacy ledger extraction (Empty != Zero).",
                pt_address,
            )
            return None
        pt_amount = MeasuredMoney.measured(Decimal(str(py_raw)) / Decimal(10**18))

        # Underlying received (OUTPUT leg). The SY ``Redeem`` event carries the
        # measured ``amount_token_out`` (the underlying the wallet actually got);
        # absent it the amount is UNMEASURED — never an SY proxy, never zero.
        out_decimals = out_token_decimals if out_token_decimals is not None else 18
        out_token = out_token_symbol or ""
        if result.redeem_sy_events and result.redeem_sy_events[0].amount_token_out:
            out_raw = result.redeem_sy_events[0].amount_token_out
            out_amount = MeasuredMoney.measured(Decimal(str(out_raw)) / Decimal(10**out_decimals))
        else:
            out_amount = MeasuredMoney.unmeasured()

        return PrimitiveMoneyLegs.of(
            PrimitiveMoneyLeg.input(pt_symbol, pt_amount),
            PrimitiveMoneyLeg.output(out_token, out_amount),
        )

    def extract_primitive_money_legs_result(
        self,
        receipt: dict[str, Any],
        *,
        pt_address: str | None = None,
        out_token_symbol: str | None = None,
        out_token_address: str | None = None,
        out_token_decimals: int | None = None,
    ) -> "ExtractResult[PrimitiveMoneyLegs]":
        """Fail-closed variant of :meth:`extract_primitive_money_legs` — VIB-5354.

        Forwards the compiler-threaded redeem / LP_CLOSE kwargs unchanged so the
        ``ResultEnricher`` (which builds them via :meth:`build_extract_kwargs`)
        gets the same INPUT/OUTPUT-leg semantics as the legacy method, now with
        a parse crash surfaced as ``ExtractError`` instead of a silent ``None``.

        ``ExtractMissing`` here preserves the legacy ``None`` contract: a non-PT
        / unmeasured Pendle withdraw or LP close where the extractor declines to
        declare typed legs (so the US-009 ledger dispatcher falls back to its
        legacy guess — blueprint 27 §6.6 / §10.10, Empty != Zero). It is NOT a
        parse error; only an actual crash / reported failure is ``ExtractError``.
        """
        return self._wrap_extract(
            self.extract_primitive_money_legs,
            receipt,
            "no resolvable Pendle PT-redeem / LP_CLOSE money legs in receipt",
            pt_address=pt_address,
            out_token_symbol=out_token_symbol,
            out_token_address=out_token_address,
            out_token_decimals=out_token_decimals,
        )

    def _lp_close_money_legs(
        self,
        result: ParseResult,
        *,
        out_token_symbol: str | None,
        out_token_decimals: int | None,
    ) -> "PrimitiveMoneyLegs | None":
        """Declare the received-underlying OUTPUT leg for a Pendle LP_CLOSE (VIB-5302).

        A single-sided Pendle LP removal burns the LP (Market ``Burn`` →
        intermediate ``net_sy_out`` / ``net_pt_out``) and then redeems the SY to
        the underlying out-token, which lands in the wallet.
        :meth:`extract_lp_close_data` already carries the intermediate SY/PT (the
        Pendle LP accounting leg, unchanged), but the actual underlying the wallet
        RECEIVES — the realized proceeds, and the amount an ``amount="all"`` LP
        re-entry would size off (deferred to VIB-5346) — was dropped from the
        ledger: the legacy LP_CLOSE path leaves it empty for Pendle (no PoolKey
        currencies, no intent ``token0`` / ``token1``).

        The underlying received is the SY ``Redeem`` event's ``amount_token_out``
        (the measured token the SY contract paid out to the wallet). It is declared
        as a typed OUTPUT leg so the US-009 ledger dispatcher records it on the row
        instead of guessing (blueprint 27 §6.6), mirroring the PT-redeem OUTPUT leg
        in :meth:`extract_primitive_money_legs`.

        Empty != Zero (blueprint 27 §10.10): when no SY ``Redeem`` carries a
        measured ``amount_token_out`` (e.g. a close that returns PT rather than the
        underlying — no SY redemption fires), this returns ``None`` so the
        dispatcher falls back to the legacy LP_CLOSE path. The proceeds are left
        UNMEASURED rather than fabricated as a zero or proxied from the
        SY-denominated ``Burn`` amounts.

        Args:
            result: The already-parsed receipt (carries ``burn_events`` /
                ``redeem_sy_events``).
            out_token_symbol: Underlying out-token symbol (from the LP_CLOSE
                ``ActionBundle.metadata["out_token"]``).
            out_token_decimals: Underlying out-token decimals. When ``None`` (the
                metadata seam did not carry it) the leg is left UNMEASURED rather
                than scaled by a guessed default (Empty != Zero — a wrong scale is
                worse than unmeasured).

        Returns:
            A :class:`PrimitiveMoneyLegs` with the single received-underlying
            OUTPUT leg, or ``None`` when the underlying proceeds were not measured
            OR the out-token decimals were not threaded.
        """
        from almanak.connectors._strategy_base.primitive_money_leg import PrimitiveMoneyLeg, PrimitiveMoneyLegs
        from almanak.framework.accounting.measured import MeasuredMoney

        out_raw: int | None = None
        if result.redeem_sy_events and result.redeem_sy_events[0].amount_token_out:
            out_raw = result.redeem_sy_events[0].amount_token_out
        if out_raw is None:
            logger.debug(
                "Pendle LP_CLOSE: no SY Redeem amount_token_out on the receipt "
                "(close-to-PT or non-underlying path); leaving proceeds UNMEASURED and "
                "falling back to legacy LP_CLOSE ledger extraction (Empty != Zero).",
            )
            return None

        if out_token_decimals is None:
            # Empty != Zero (blueprint 27 §10.10): a MEASURED raw amount scaled by
            # GUESSED decimals is measured-WRONG — a 6-dec underlying (USDC Pendle
            # markets exist) booked at the 18-dec default is off by 1e12, far worse
            # than leaving it unmeasured. self.token_out_decimals is NOT a safe
            # fallback here: the enricher builds PendleReceiptParser with chain only
            # (``_build_parser_kwargs``), so it stays at the 18-dec constructor
            # default for an LP_CLOSE — the same wrong guess. The compiler always
            # threads the out-token decimals via ``ActionBundle.metadata["out_token"]``
            # in prod, so a missing value means the metadata seam broke: degrade to
            # the legacy LP_CLOSE path rather than book a wrong scale.
            logger.debug(
                "Pendle LP_CLOSE: out_token_decimals not threaded; leaving proceeds "
                "UNMEASURED rather than guessing a scale (Empty != Zero)."
            )
            return None
        out_amount = MeasuredMoney.measured(Decimal(str(out_raw)) / Decimal(10**out_token_decimals))
        out_token = out_token_symbol or ""
        return PrimitiveMoneyLegs.of(PrimitiveMoneyLeg.output(out_token, out_amount))

    def extract_redemption_amounts(self, receipt: dict[str, Any]) -> dict[str, int] | None:
        """
        Extract redemption amounts from receipt.

        Called by the framework after WITHDRAW/REDEEM execution.
        """
        try:
            result = self.parse_receipt(receipt)
            if result.redeem_events:
                # Pre-maturity path: RedeemPY event from YT contract
                redeem = result.redeem_events[0]
                return {
                    "py_redeemed": redeem.net_py_redeemed,
                    "sy_received": redeem.net_sy_redeemed,
                }
            if result.redeem_sy_events:
                # Post-maturity path: SY Redeem event (PT → SY → token, no YT involvement)
                r = result.redeem_sy_events[0]
                return {
                    "py_redeemed": r.amount_sy_to_redeem,  # SY burned ≈ PT redeemed (1:1 at maturity)
                    "sy_received": r.amount_sy_to_redeem,  # SY component (before token conversion)
                }
            return None
        except Exception as e:
            logger.warning(f"Failed to extract redemption amounts: {e}")
            return None


__all__ = [
    "PendleReceiptParser",
    "PendleEvent",
    "PendleEventType",
    "SwapEventData",
    "MintEventData",
    "BurnEventData",
    "RedeemSYEventData",
    "RedeemPYEventData",
    "TransferEventData",
    "ParsedSwapResult",
    "ParseResult",
    "EVENT_TOPICS",
    "TOPIC_TO_EVENT",
    "EVENT_NAME_TO_TYPE",
]
