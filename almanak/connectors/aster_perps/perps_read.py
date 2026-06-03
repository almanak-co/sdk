"""Aster Perps perp-position read + valuation, published as a ``PerpsReadSpec``.

Second perp venue on the strategy-side perps-read seam
(:mod:`almanak.connectors._strategy_base.perps_read_base`), proving the
abstraction is one-folder-one-row: this module + a single
:data:`~almanak.connectors._strategy_base.perps_read_registry.PerpsReadRegistry._SPEC_LOADERS`
row, no framework edit. It folds Aster-specific read + valuation knowledge into
the connector as **pure** data + functions (no gateway, no egress):

* ``_build_aster_calls`` — one ``TradingReaderFacet.getPositionsV2(trader,
  pairBase)`` :class:`EthCall` per market against the Diamond ``router``
  (the framework reader executes them via the gateway).
* ``_reduce_aster_positions`` — decodes each market's ``ITradingReader.Position[]``
  ABI return into :class:`PerpsPositionOnChain`, applying Aster's decimal
  conventions, and returns a :class:`PerpsReadResult` whose ``ok`` flag carries
  Empty≠Zero.
* ``_aster_market_metadata`` — pairBase → symbol + the *qty* decimals the valuer
  must divide ``size_in_tokens`` by (Aster qty is 1e10, not a per-token native
  scale).
* ``value_aster_position`` — Aster mark-to-market in the shared
  :class:`PerpsPositionValue` shape, using Aster's 1e8 USD scale (NOT GMX's
  1e30).

CRITICAL — NET-NEW MONEY MATH (no byte-parity oracle). Unlike the GMX migration,
Aster never had framework perp valuation, so the decode + valuation here are
derived directly from the connector's own contract ABI / SDK:

* ``getPositionsV2`` return struct: ``abis/TradingReaderFacet.json`` →
  ``getPositionsV2.outputs`` (``ITradingReader.Position[]``, 15 fields). The
  field-index map and per-field decimals are documented on
  ``_reduce_aster_positions`` / :data:`_POSITION_TUPLE` below.
* qty scale 1e10 — ``sdk.py:119`` (``QTY_DECIMALS = 10``), ``sdk.py:161``
  (``qty: uint80 … 10-decimal fixed-point``), ``receipt_parser.py:257``
  (``Decimal(qty_raw) / 10**QTY_DECIMALS``).
* price scale 1e8 — ``sdk.py:118`` (``PRICE_DECIMALS = 8``), ``sdk.py:161-162``
  (``price/stopLoss/takeProfit`` 8-decimal), ``receipt_parser.py:279``
  (``Decimal(entry_price) / 10**PRICE_DECIMALS``).
* margin scale — ``sdk.py:160`` / ``sdk.py:148`` (``amountIn`` is "collateral
  token's smallest units"); the on-chain ``Position.margin`` is the same margin,
  so it is in the **marginToken's native decimals** (resolved by the framework's
  token resolver at value time, exactly as GMX's collateral is).

Gateway-boundary note: ``eth_abi`` / ``eth_utils`` are used here only for ABI
encode/decode + selector derivation (pure utilities) — never to open a provider.
The gateway-routed ``eth_call`` lives in the framework reader.

VIB-4930 (epic VIB-4851).
"""

from __future__ import annotations

import logging
from decimal import Decimal

from eth_abi import decode as abi_decode
from eth_abi import encode as abi_encode
from eth_utils import to_checksum_address

from almanak.connectors._strategy_base.perps_read_base import (
    EthCall,
    PerpsMarketMeta,
    PerpsPositionOnChain,
    PerpsPositionQuery,
    PerpsPositionValue,
    PerpsReadResult,
    PerpsReadSpec,
)

from .addresses import ASTER_PERPS_MARKETS
from .sdk import (
    PRICE_DECIMALS,
    QTY_DECIMALS,
    SELECTOR_GET_POSITIONS_V2,
)

logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------- #
# Decimal conventions (every scale below is sourced from the connector itself)
# --------------------------------------------------------------------------- #

# qty scale: Aster qty is 10-decimal fixed-point (sdk.py:119 QTY_DECIMALS=10;
# sdk.py:161; receipt_parser.py:257). NOT applied as a local divisor here — the
# framework valuer divides ``size_in_tokens`` by ``10**index_token_decimals``
# (portfolio_valuer.py:2217/2223 → value_position), so ``_aster_market_metadata``
# returns ``index_token_decimals = QTY_DECIMALS`` to recover human qty from the
# raw 1e10 ``qty``. (Imported ``QTY_DECIMALS`` is used directly in the reducer.)
#
# price scale: Aster price (entryPrice / mark) is 8-decimal fixed-point (sdk.py:118
# PRICE_DECIMALS=8; sdk.py:161-162; receipt_parser.py:279). Used via the imported
# ``PRICE_DECIMALS`` constant in the notional synthesis below.

# Aster carries NO on-chain USD notional (the Position struct stores qty +
# entryPrice, not size_in_usd — contrast GMX). We synthesise ``size_in_usd`` in
# the reducer as the *entry notional* in an 8-decimal USD fixed-point so that the
# venue-neutral ``PerpsPositionOnChain`` still carries a single integer notional,
# and ``value_aster_position`` divides by the same scale. 8 decimals (not GMX's
# 30) is Aster's native price scale, chosen so the synthesis is exact:
#   size_in_usd = qty(1e10) * entryPrice(1e8) / 1e10  ==  notional * 1e8
# i.e. ``Constants.QTY_DECIMALS`` of the qty cancel, leaving PRICE_DECIMALS.
_USD_DECIMALS = PRICE_DECIMALS  # 8 — Aster has no 1e30 USD scale
_USD_DIVISOR = Decimal(10**_USD_DECIMALS)  # 1e8

# getPositionsV2(address user, address pairBase) -> ITradingReader.Position[].
# Selector verified + self-checked in the SDK (sdk.py:83-84, sdk.py:447):
# SELECTOR_GET_POSITIONS_V2 = keccak("getPositionsV2(address,address)")[:4].
# Output ABI built from the known 15-field struct so a typo can't silently
# corrupt the decode. Field order/types are EXACTLY the ABI's
# ``getPositionsV2.outputs`` components (abis/TradingReaderFacet.json):
#   [0]  bytes32 positionHash
#   [1]  string  pair
#   [2]  address pairBase
#   [3]  address marginToken     (collateral token)
#   [4]  bool    isLong
#   [5]  uint96  margin          (marginToken native decimals — sdk.py:160)
#   [6]  uint80  qty             (1e10 — QTY_DECIMALS)
#   [7]  uint64  entryPrice      (1e8 — PRICE_DECIMALS)
#   [8]  uint64  stopLoss        (1e8; decoded-but-dropped)
#   [9]  uint64  takeProfit      (1e8; decoded-but-dropped)
#   [10] uint96  openFee         (marginToken native decimals; dropped)
#   [11] uint96  executionFee    (marginToken native decimals; dropped)
#   [12] int256  fundingFee      (signed, marginToken native decimals; dropped)
#   [13] uint40  timestamp       (unix seconds)
#   [14] uint96  holdingFee      (marginToken native decimals; dropped)
_POSITION_TUPLE = (
    "(bytes32,string,address,address,bool,uint96,uint80,uint64,uint64,uint64,uint96,uint96,int256,uint40,uint96)"
)
_GET_POSITIONS_V2_OUTPUT = f"{_POSITION_TUPLE}[]"

# Struct field indices (single source of truth for the decode + tests).
_IDX_PAIR_BASE = 2
_IDX_MARGIN_TOKEN = 3
_IDX_IS_LONG = 4
_IDX_MARGIN = 5
_IDX_QTY = 6
_IDX_ENTRY_PRICE = 7
_IDX_TIMESTAMP = 13

_POSITION_KEY_PREFIX = "aster"


def _markets_for_chain(chain: str) -> tuple[str, ...]:
    """Return the tuple of pairBase addresses Aster trades on ``chain``.

    Aster is a per-market venue: ``getPositionsV2`` is queried once per pairBase,
    so the registry fills ``query.markets`` from this. Reads the connector's own
    ``ASTER_PERPS_MARKETS`` table (chain key ``"bsc"``; see addresses.py:59-65).
    Synthetic / non-crypto markets (NVDA, TSLA, …) are out of the connector's v1
    scope and intentionally absent from ``ASTER_PERPS_MARKETS`` (sdk.py:301-303),
    so they are not read here — a strategy holding one would fall back to its
    strategy-reported value (Empty≠Zero) rather than be mis-valued. Order is the
    table's declaration order (BTC, ETH, BNB) for deterministic call planning.
    """
    return tuple(ASTER_PERPS_MARKETS.get(chain, {}).values())


def _build_aster_calls(query: PerpsPositionQuery) -> list[EthCall]:
    """Plan one ``getPositionsV2(trader, pairBase)`` read per market.

    Targets the resolved Diamond ``router`` (the ``TradingReaderFacet`` views are
    served through the EIP-2535 proxy, so the call target is the router address —
    addresses.py:50-54). One :class:`EthCall` per pairBase in ``query.markets``.
    Returns ``[]`` (fail-closed) when the router is unresolved or there are no
    markets — the reducer then yields an ``ok=False`` empty result.
    """
    targets = query.targets or {}
    router = targets.get("router")
    markets = query.markets
    if not router or not markets:
        return []
    calls: list[EthCall] = []
    for pair_base in markets:
        args = abi_encode(["address", "address"], [query.wallet_address, pair_base])
        data = "0x" + (SELECTOR_GET_POSITIONS_V2 + args).hex()
        calls.append(EthCall(to=router, data=data))
    return calls


def _reduce_aster_positions(query: PerpsPositionQuery, results: list[str | None]) -> PerpsReadResult:
    """Decode each market's ``getPositionsV2`` return into active positions.

    ``results[i]`` is the blob for ``query.markets[i]`` (1:1 with the calls
    ``_build_aster_calls`` planned). Each blob decodes to a
    ``ITradingReader.Position[]`` (one trader can hold several positions on a
    single pairBase — long + short, or multiple opens). Field mapping (see
    :data:`_POSITION_TUPLE` for the full indexed struct):

      * ``pairBase``      -> ``market``            (the valuation join key)
      * ``marginToken``   -> ``collateral_token``
      * ``isLong``        -> ``is_long``
      * ``margin``        -> ``collateral_amount`` (marginToken native decimals)
      * ``qty``           -> ``size_in_tokens``    (raw 1e10; metadata returns
                                                    decimals=10 so the valuer
                                                    recovers human qty)
      * ``timestamp``     -> ``increased_at_time`` (decreased_at_time unknown -> 0)
      * synthesised ``size_in_usd`` = entry notional in 1e8 USD (see below)

    Entry-notional synthesis (Aster has no on-chain notional):
      ``size_in_usd = qty * entryPrice // 1e10``  (units: 1e8 USD fixed-point)
    Derivation: ``(qty/1e10 tokens) * (entryPrice/1e8 USD) * 1e8 == qty*entryPrice/1e10``.
    ``value_aster_position`` divides this back by 1e8. Integer ``//`` truncates
    the sub-1e-8-USD remainder; entry price round-trips through
    ``entry = size_usd / tokens`` to within that truncation (pinned in tests).

    GMX-shaped fields with no Aster analogue are set to 0 (``borrowing_factor``,
    ``funding_fee_amount_per_size``, ``decreased_at_time``): they are NOT consumed
    by ``value_aster_position`` and exist only to satisfy the venue-neutral
    :class:`PerpsPositionOnChain` shape (its docstring: cosmetic / unmeasured
    fields stay 0, never a fabricated value).

    Empty≠Zero (per-market): a market whose blob is ``None`` (its ``eth_call``
    failed) is *skipped*, not fatal — one paused/thin market must not blind the
    read of the others. The whole read is ``ok=False`` (unmeasured) ONLY when
    EVERY market blob is ``None`` (the gateway round-trip failed outright);
    otherwise it is ``ok=True`` and the successfully-decoded markets are returned
    (a measured empty book is ``ok=True`` with no positions). A blob that decodes
    to an empty array is a measured "no positions on this pairBase".
    """
    if not results or all(r is None for r in results):
        # Every market failed (or nothing was planned) -> unmeasured.
        return PerpsReadResult(positions=(), ok=False)

    positions: list[PerpsPositionOnChain] = []
    for blob in results:
        if not blob:
            # Single failed market: skip (the other markets are still measured).
            continue
        try:
            raw = bytes.fromhex(blob[2:] if blob[:2].lower() == "0x" else blob)
            decoded = abi_decode([_GET_POSITIONS_V2_OUTPUT], raw)[0]
        except Exception:
            logger.debug("Failed to decode Aster getPositionsV2 return", exc_info=True)
            continue
        for entry in decoded:
            qty = entry[_IDX_QTY]
            entry_price = entry[_IDX_ENTRY_PRICE]
            # Entry notional in 1e8 USD (see method docstring for the derivation).
            size_in_usd = (qty * entry_price) // (10**QTY_DECIMALS)
            pos = PerpsPositionOnChain(
                # ``eth_abi`` lower-cases addresses; checksum them to match the
                # form a web3 contract call would return (mirrors gmx perps_read).
                account=to_checksum_address(query.wallet_address),
                market=to_checksum_address(entry[_IDX_PAIR_BASE]),
                collateral_token=to_checksum_address(entry[_IDX_MARGIN_TOKEN]),
                size_in_usd=size_in_usd,  # 8 decimals (Aster synthetic notional)
                size_in_tokens=qty,  # 10 decimals (QTY_DECIMALS)
                collateral_amount=entry[_IDX_MARGIN],  # marginToken native decimals
                is_long=entry[_IDX_IS_LONG],
                borrowing_factor=0,  # no Aster analogue
                funding_fee_amount_per_size=0,  # no Aster analogue
                increased_at_time=entry[_IDX_TIMESTAMP],
                decreased_at_time=0,  # not in the Position struct
                key_prefix=_POSITION_KEY_PREFIX,
            )
            if pos.is_active:  # size_in_usd > 0 — non-empty position
                positions.append(pos)
    return PerpsReadResult(positions=tuple(positions), ok=True)


def _aster_market_metadata(market_address: str, chain: str) -> PerpsMarketMeta | None:
    """Resolve an Aster market's symbol + qty decimals for valuation.

    ``index_token_symbol`` is the base symbol from ``ASTER_PERPS_MARKETS`` (key
    ``"BTC/USD"`` -> ``"BTC"``); the framework prices it via
    ``market.price(symbol)`` (portfolio_valuer.py:2183).

    ``index_token_decimals`` is **10 (QTY_DECIMALS), NOT a per-token native
    decimal count.** Aster stores position size as ``qty`` in 1e10 fixed-point
    for every market (sdk.py:119), and the framework valuer divides
    ``size_in_tokens`` by ``10**index_token_decimals`` to get human qty
    (value_position contract). Returning 10 here makes that division recover the
    true token quantity regardless of the underlying asset's own ERC-20 decimals.

    Lookup is case-insensitive on the pairBase address. Returns ``None`` for an
    unknown market / chain (callers fail closed). Every registered Aster market
    shares the same qty scale, so — unlike GMX — there is no per-market decimals
    table that could be missing.
    """
    addr_lower = market_address.lower()
    for name, addr in ASTER_PERPS_MARKETS.get(chain, {}).items():
        if addr.lower() == addr_lower:
            symbol = name.split("/")[0]  # "BTC/USD" -> "BTC"
            return PerpsMarketMeta(index_token_symbol=symbol, index_token_decimals=QTY_DECIMALS)
    return None


def value_aster_position(
    *,
    size_in_usd: int,
    size_in_tokens: int,
    collateral_amount: int,
    is_long: bool,
    mark_price_usd: Decimal,
    collateral_token_price_usd: Decimal,
    collateral_token_decimals: int,
    index_token_decimals: int,
    pending_funding_fees_usd: Decimal = Decimal("0"),
    pending_borrowing_fees_usd: Decimal = Decimal("0"),
    market: str = "",
) -> PerpsPositionValue:
    """Value a single Aster perpetual position at current market price.

    Keyword-only with the SAME parameter names the framework valuer passes for
    every venue (portfolio_valuer.py:2214-2225) — that is what lets Aster slot in
    behind ``PerpsReadRegistry.value_position`` with **zero framework edits**. The
    interpretation of the raw integers, however, is Aster's, NOT GMX's:

      * ``size_in_usd``  — Aster entry notional in 1e8 USD fixed-point (the
        reducer's synthesis), divided by ``_USD_DIVISOR`` (1e8), NOT GMX's 1e30.
      * ``size_in_tokens`` — raw 1e10 qty; ``index_token_decimals`` is 10
        (``_aster_market_metadata``), so ``size_in_tokens / 10**10`` is human qty.
      * ``collateral_amount`` — margin in the marginToken's native decimals;
        ``collateral_token_decimals`` is the resolver-provided native count.

    The mark-to-market formula is structurally identical to any linear perp
    (entry price from notional/size, PnL = qty·Δprice with sign by direction,
    net = collateral·price + PnL − fees, leverage = notional/collateral). It is
    pure (no I/O).

    Fees: ``pending_funding_fees_usd`` / ``pending_borrowing_fees_usd`` default to
    0 because the framework does not yet pass Aster's pending ``fundingFee`` /
    ``holdingFee`` (they are raw token amounts on-chain — receipt_parser.py:329-339
    notes the USD conversion is deferred). Net value is therefore an upper bound,
    matching the GMX path's documented behaviour (portfolio_valuer.py:2211-2213).
    """
    # Convert raw values to human-readable (Aster scales).
    size_usd = Decimal(size_in_usd) / _USD_DIVISOR  # 1e8 USD notional -> USD
    tokens = Decimal(size_in_tokens) / Decimal(10**index_token_decimals)  # 1e10 -> qty
    collateral = Decimal(collateral_amount) / Decimal(10**collateral_token_decimals)

    # Collateral value at current price (stablecoin margin: price ~= 1).
    collateral_value = collateral * collateral_token_price_usd

    # Entry price recovered from the synthesised notional: size_usd / qty. Exact
    # to the reducer's 1e8 truncation (Aster stores entryPrice at 1e8, so the
    # round-trip is faithful to the on-chain entry price).
    if tokens > 0:
        entry_price = size_usd / tokens
    else:
        entry_price = Decimal("0")

    # Unrealized PnL: qty * price move, signed by direction.
    if tokens > 0 and mark_price_usd > 0:
        if is_long:
            unrealized_pnl = tokens * (mark_price_usd - entry_price)
        else:
            unrealized_pnl = tokens * (entry_price - mark_price_usd)
    else:
        unrealized_pnl = Decimal("0")

    pending_fees = pending_funding_fees_usd + pending_borrowing_fees_usd

    # Net value: what the trader would receive if closing now.
    net_value = collateral_value + unrealized_pnl - pending_fees

    # Leverage: notional / collateral.
    if collateral_value > 0:
        leverage = size_usd / collateral_value
    else:
        leverage = Decimal("0")

    return PerpsPositionValue(
        market=market,
        is_long=is_long,
        size_usd=size_usd,
        collateral_value_usd=collateral_value,
        entry_price_usd=entry_price,
        mark_price_usd=mark_price_usd,
        unrealized_pnl_usd=unrealized_pnl,
        pending_fees_usd=pending_fees,
        net_value_usd=net_value,
        leverage=leverage,
    )


#: Aster Perps' published perp-read capability. The single read target is the
#: per-chain Diamond ``router`` (the ``TradingReaderFacet`` views are served
#: through the EIP-2535 proxy), owned by ``aster_perps/addresses.py``. Aster is a
#: per-market venue, so ``markets_for_chain`` supplies the pairBase list the
#: registry fills into ``query.markets`` and ``build_calls`` reads one per call.
PERPS_READ_SPEC = PerpsReadSpec(
    contract_kinds={"router": ("router",)},
    build_calls=_build_aster_calls,
    reduce_calls=_reduce_aster_positions,
    market_metadata=_aster_market_metadata,
    value_position=value_aster_position,
    position_key_prefix=_POSITION_KEY_PREFIX,
    markets_for_chain=_markets_for_chain,
)
