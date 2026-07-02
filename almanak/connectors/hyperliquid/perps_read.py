"""Hyperliquid perp-position read + valuation, published as a ``PerpsReadSpec``.

This is the **settlement observer** for the CoreWriter async model: an order
submitted via CoreWriter settles off the EVM on HyperCore, so the position only
becomes visible on a later read. The framework's portfolio valuer calls this
spec each snapshot; the position "appears" once HyperCore has filled it — the
same shape gmx_v2 uses for keeper-settled fills.

Reads use the HyperCore **position precompile** (``0x0800``) — one
``position(wallet, assetIndex)`` staticcall per seeded market (there is no
"all positions" precompile; we iterate the ``markets.py`` seed, so only seeded
markets are valued until the dynamic-universe capability lands). Precompile
input is raw ABI args, NO selector (see ``addresses.py``).

Position struct (verified layout, hyper-evm-lib ``PrecompileLib``):
``(int64 szi, uint64 entryNtl, int64 isolatedRawUsd, uint32 leverage, bool isIsolated)``.

MONEY-PATH SCALES — CONFIRMED against LIVE MAINNET (2026-07-01, chain 999):
read the position precompile for an active 174-position account
(``0x010461c1…703a``) and cross-checked every field against the same account's
``clearinghouseState`` from ``api.hyperliquid.xyz`` (human units), back-to-back
to defeat market-maker drift:
  * ``szi`` scale = ``10**szDecimals`` (asset units). ✅ CONFIRMED — raw szi
    (ETH -221852 → -22.1852, ATOM 1106793 → 11067.93, BTC -3613 → -0.03613)
    matched the API szi EXACTLY across coins in a tight back-to-back read.
  * ``entryNtl`` scale = ``1e6`` USD (HyperCore perp USD, matching the L1 SDK's
    ``float_to_usd_int`` = 1e6). ✅ CONFIRMED — entryNtl/1e6 matched |szi|·entryPx
    to sub-cent (ETH $35787.63 vs $35787.61; ATOM $16960.60 vs $16960.50; residual
    is API entryPx display-rounding, not a scale error).
  * Cross-margin positions carry NO per-position collateral in the position
    struct (margin is shared at the account level); only isolated positions
    report ``isolatedRawUsd``. ✅ CONFIRMED for cross — every read position was
    cross (``isIsolated=False``, ``isolatedRawUsd=0``, ``leverage=20``).
    VIB-5596 folds a cross position's own INITIAL MARGIN (``entryNtl/leverage``,
    both in the position struct) in as its ``collateral_amount`` so ``net_value``
    is margin-equity + PnL, not PnL-only. This is GATED on a successful
    account-margin read (0x080F ``accountMarginSummary``, appended as the last
    planned call): a measured account proves the wallet has a live cross account,
    so the derived collateral is asserted only when that read is measured
    (Empty≠Zero — a failed 0x080F read keeps the PnL-only fallback, never a
    fabricated collateral). The account's idle free-collateral buffer
    (``accountValue − marginUsed``) is NOT attributed here — it has no single
    position to live on and per-position attribution is unsafe under an
    incomplete seeded book; that is a separate portfolio-valuer follow-up
    (VIB-5596 flags it). The 0x080F layout+scale is CONFIRMED live — see
    ``sdk.AccountMarginSummary``.
  * ``isolatedRawUsd`` scale = ``1e6`` USD. [STILL ASSUMED — the cross-check
    account held no isolated position to confirm against. Highly likely correct
    (same 1e6 USD convention as the CONFIRMED entryNtl), but confirm against a
    live isolated position before trusting isolated-margin net_value.]

Gateway-boundary note: ``eth_abi`` is used here only for pure ABI decode; the
gateway-routed ``eth_call`` lives in the framework reader.
"""

from __future__ import annotations

import logging
from decimal import Decimal

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

from .addresses import PRECOMPILE_ACCOUNT_MARGIN_SUMMARY, PRECOMPILE_POSITION
from .markets import resolve_market, seeded_symbols
from .sdk import (
    Position,
    decode_account_margin_summary,
    decode_position,
    encode_account_margin_query,
    encode_position_query,
)

logger = logging.getLogger(__name__)

_POSITION_KEY_PREFIX = "hyperliquid"

# Scale constants (see module docstring). entryNtl 1e6 USD is CONFIRMED live; the
# same 1e6 divisor applies to the cross collateral (entryNtl/leverage is 1e6 USD)
# and to isolatedRawUsd (still ASSUMED 1e6 — no live isolated position to confirm).
_ENTRY_NTL_USD_DIVISOR = Decimal(10**6)  # entryNtl / entryNtl-derived margin / isolatedRawUsd → 1e6 USD
_USD_DECIMALS = 6  # size_in_usd carried at 1e6; value_position divides it back


def _markets_for_chain(chain: str) -> tuple[str, ...]:
    """Seeded market symbols to read on ``chain`` (per-market range-read).

    There is no "all positions" precompile, so we read ``position(wallet, index)``
    once per seeded major and keep the non-empty ones. A position in a market
    outside the seed is not valued here (falls back to the strategy-reported
    value, Empty≠Zero) until the dynamic universe capability lands.
    """
    if chain != "hyperevm":
        return ()
    return tuple(sorted(seeded_symbols()))


def _build_hyperliquid_calls(query: PerpsPositionQuery) -> list[EthCall]:
    """Plan one ``position(wallet, assetIndex)`` read per seeded market, then the
    account-margin read.

    Targets the fixed precompiles (0x0800 position, 0x080F accountMarginSummary)
    directly — Hyperliquid's read targets are protocol constants, not
    AddressRegistry addresses, so this ignores ``query.targets``. One
    :class:`EthCall` per symbol in ``query.markets``, followed by exactly ONE
    trailing account-margin call. The account-margin call is ALWAYS the last
    planned call, so ``_reduce_hyperliquid_positions`` can consume it positionally
    (VIB-5596: cross positions read their shared collateral gate from it).
    """
    calls: list[EthCall] = []
    for symbol in query.markets:
        try:
            market = resolve_market(symbol)
        except ValueError:
            continue
        data = "0x" + encode_position_query(query.wallet_address, market.asset_index).hex()
        calls.append(EthCall(to=PRECOMPILE_POSITION, data=data))
    # Trailing account-margin read (0x080F) — one per wallet, not per market.
    # Appended LAST so the reducer slices it off before walking per-market blobs.
    margin_data = "0x" + encode_account_margin_query(query.wallet_address).hex()
    calls.append(EthCall(to=PRECOMPILE_ACCOUNT_MARGIN_SUMMARY, data=margin_data))
    return calls


def _cross_collateral_1e6(pos: Position, *, cross_margin_measured: bool) -> int:
    """Per-position collateral (raw 1e6 USD) the reducer stamps (VIB-5596).

    * Isolated position → ``isolatedRawUsd`` (self-contained per-position margin).
    * Cross position → its own INITIAL MARGIN ``entryNtl / leverage`` (both in the
      position struct), GATED on a measured account-margin read — so ``net_value``
      is margin-equity + PnL, not PnL-only. When the account-margin read is
      unmeasured (0x080F ``None``/reverted) or ``leverage == 0`` (malformed), fall
      back to ``0`` = today's PnL-only behaviour (Empty≠Zero: never fabricate a
      collateral off a failed account read, never divide by zero).

    Cross collateral is a pure function of THIS position's own struct — no sibling
    coupling — so an incomplete seeded book (a position in a non-seeded market we
    never read) cannot corrupt a position we did read. Account-level attribution
    of the shared free-collateral buffer is deliberately NOT done here (unsafe
    under a partial book); see the module docstring.
    """
    if pos.is_isolated:
        return abs(pos.isolated_raw_usd)
    if cross_margin_measured and pos.leverage > 0:
        return int(pos.entry_ntl // pos.leverage)
    return 0


def _reduce_hyperliquid_positions(query: PerpsPositionQuery, results: list[str | None]) -> PerpsReadResult:
    """Decode each seeded market's position precompile return into active positions.

    ``build_calls`` plans one position call per RESOLVABLE seeded market, then ONE
    trailing account-margin (0x080F) call. So ``results`` is
    ``[<position blobs…>, <account-margin blob>]``: split the trailing
    account-margin result off FIRST, then consume the position blobs positionally
    against the resolvable subset (an unresolvable symbol emitted no call, so a
    plain ``zip(query.markets, results)`` would shift every later blob onto the
    wrong market). We mirror ``build_calls``: walk ``query.markets``, skip
    unresolvable symbols WITHOUT consuming a result, take one result per resolvable
    market.

    Empty≠Zero: ``ok=False`` (unmeasured) only when EVERY *position* blob is
    ``None`` — the account-margin blob is EXCLUDED from that test, so a 0x080F
    revert on an otherwise-good book does NOT falsely mark the whole read
    unmeasured. Otherwise ``ok=True`` and the decoded non-empty positions are
    returned (a measured empty book is ``ok=True`` with no positions). A blob
    decoding to ``szi == 0`` is a measured "no position on this market".

    The account-margin blob is a per-position *collateral gate* for cross
    positions (see :func:`_cross_collateral_1e6`), not a value source: a measured
    account proves the wallet has a live cross account, so a cross position's
    derived ``entryNtl/leverage`` collateral is asserted only when that read is
    measured.
    """
    if not results:
        return PerpsReadResult(positions=(), ok=False)

    # The trailing call is ALWAYS the account-margin read (build_calls appends it
    # unconditionally). Split it off so it never lands on a per-market blob and is
    # excluded from the position-level Empty≠Zero test.
    account_blob = results[-1]
    position_blobs = results[:-1]

    if not position_blobs or all(r is None for r in position_blobs):
        return PerpsReadResult(positions=(), ok=False)

    # Account-margin gate: measured iff the 0x080F read decoded to a summary.
    # A failed/empty read leaves cross positions on the PnL-only fallback.
    cross_margin_measured = False
    if account_blob:
        try:
            cross_margin_measured = decode_account_margin_summary(account_blob) is not None
        except Exception:  # noqa: BLE001 — a bad account blob must not blind positions
            logger.debug("Failed to decode Hyperliquid account-margin summary", exc_info=True)

    positions: list[PerpsPositionOnChain] = []
    result_iter = iter(position_blobs)
    for symbol in query.markets:
        try:
            market = resolve_market(symbol)
        except ValueError:
            # Unresolvable → build_calls emitted NO call, so there is NO result to
            # consume; skip WITHOUT advancing result_iter (this keeps every
            # remaining blob aligned to its market).
            continue
        blob = next(result_iter, None)
        if not blob:
            continue  # a resolvable market whose read failed: skip, others measured
        try:
            pos = decode_position(blob)
        except Exception:  # noqa: BLE001 — a bad blob must not blind the others
            logger.debug("Failed to decode Hyperliquid position for %s", symbol, exc_info=True)
            continue
        if pos.szi == 0:
            continue  # measured "no position"
        collateral_1e6 = _cross_collateral_1e6(pos, cross_margin_measured=cross_margin_measured)
        positions.append(
            PerpsPositionOnChain(
                account=to_checksum_address(query.wallet_address),
                market=market.symbol,  # symbol is the valuation join key (no market address)
                collateral_token="USDC",  # HyperCore perps are USDC-margined
                size_in_usd=int(pos.entry_ntl),  # entry notional, 1e6 USD (CONFIRMED live)
                size_in_tokens=abs(pos.szi),  # 10**szDecimals (CONFIRMED live)
                collateral_amount=collateral_1e6,  # 1e6 USD: isolatedRawUsd | entryNtl/leverage (cross) | 0
                is_long=pos.is_long,
                borrowing_factor=0,  # no HyperCore analogue
                funding_fee_amount_per_size=0,  # funding not in the position struct
                increased_at_time=0,  # not in the position struct
                decreased_at_time=0,
                key_prefix=_POSITION_KEY_PREFIX,
            )
        )
    return PerpsReadResult(positions=tuple(positions), ok=True)


def _hyperliquid_market_metadata(market_symbol: str, chain: str) -> PerpsMarketMeta | None:
    """Resolve a market symbol → (symbol, szDecimals) for valuation.

    ``index_token_decimals`` is the asset's ``szDecimals`` — the framework valuer
    divides ``size_in_tokens`` (raw szi) by ``10**szDecimals`` to get human size.
    ``None`` for an unknown market (callers fail closed).
    """
    try:
        market = resolve_market(market_symbol)
    except ValueError:
        return None
    return PerpsMarketMeta(index_token_symbol=market.symbol, index_token_decimals=market.sz_decimals)


def value_hyperliquid_position(
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
    """Mark-to-market a Hyperliquid position (pure; same signature as every venue).

    Interpretation of the raw integers is HyperCore's (see module docstring —
    entryNtl / entryNtl-derived margin 1e6 USD CONFIRMED live; isolatedRawUsd 1e6
    still assumed):
      * ``size_in_usd`` — entry notional at 1e6 USD → divide by 1e6.
      * ``size_in_tokens`` — raw szi; ``index_token_decimals`` = szDecimals, so
        ``size_in_tokens / 10**szDecimals`` is human size.
      * ``collateral_amount`` — margin at 1e6 USD: ``isolatedRawUsd`` (isolated),
        ``entryNtl/leverage`` initial margin (cross, gated on the 0x080F
        account-margin read — VIB-5596), or ``0`` when unmeasured (PnL-only
        fallback). The formula is the same either way: ``net_value = collateral +
        unrealized_pnl - fees`` (accounting blueprint §perps).

    ``collateral_token_price_usd`` / ``collateral_token_decimals`` are accepted for
    signature parity but not used (collateral is already a USD figure here, not a
    token amount) — documented so a reviewer doesn't read it as a bug.
    """
    _ = (collateral_token_price_usd, collateral_token_decimals)  # unused: collateral is USD, not a token amount
    size_usd = Decimal(size_in_usd) / _ENTRY_NTL_USD_DIVISOR
    tokens = Decimal(size_in_tokens) / Decimal(10**index_token_decimals) if index_token_decimals >= 0 else Decimal(0)
    collateral_value = Decimal(collateral_amount) / _ENTRY_NTL_USD_DIVISOR

    entry_price = size_usd / tokens if tokens > 0 else Decimal("0")

    if tokens > 0 and mark_price_usd > 0:
        unrealized_pnl = tokens * (mark_price_usd - entry_price) if is_long else tokens * (entry_price - mark_price_usd)
    else:
        unrealized_pnl = Decimal("0")

    pending_fees = pending_funding_fees_usd + pending_borrowing_fees_usd
    net_value = collateral_value + unrealized_pnl - pending_fees
    leverage = size_usd / collateral_value if collateral_value > 0 else Decimal("0")

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


#: Hyperliquid's published perp-read capability. No AddressRegistry contract
#: roles (reads hit fixed precompiles), so ``contract_kinds`` is empty; a
#: per-market range-read over the seeded majors via ``markets_for_chain``.
PERPS_READ_SPEC = PerpsReadSpec(
    contract_kinds={},
    build_calls=_build_hyperliquid_calls,
    reduce_calls=_reduce_hyperliquid_positions,
    market_metadata=_hyperliquid_market_metadata,
    value_position=value_hyperliquid_position,
    position_key_prefix=_POSITION_KEY_PREFIX,
    markets_for_chain=_markets_for_chain,
)

__all__ = ["PERPS_READ_SPEC", "value_hyperliquid_position"]
