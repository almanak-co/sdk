"""Silo V2 lending-read capability (bespoke per-silo aggregate account-state).

Publishes this connector's account-state read spec (VIB-4965) so the strategy-side
:class:`~almanak.connectors._strategy_base.lending_read_registry.LendingReadRegistry`
can let the framework lending reader query Silo V2 state without the framework
hardcoding Silo's per-silo selection, function selectors, or HF math.

Why a *bespoke* reader (the ticket's core point): unlike the Aave family, Silo V2
has **no** Aave-style ``getUserAccountData(user)`` whole-account aggregate. Each
Silo V2 market is an isolated pair of two ERC-4626 vaults (silo0 + silo1) sharing a
SiloConfig; depositing into one silo enables borrowing from the paired silo. So the
aggregate account state is assembled from per-silo reads, each a single eth_call the
protocol itself computes (no reimplemented ERC-4626 share math — Silo V2 uses a
non-standard virtual-offset rounding that a pure reducer could not reproduce
byte-exactly, verified on-chain: ``convertToAssets(1) == 1`` while
``totalAssets/totalSupply == 1e-3``):

* **Collateral** — the user's deposit-silo position, read as ``maxWithdraw(user)``
  (selector ``0xce96cb77``): the underlying assets the user can withdraw, which the
  silo derives from the user's share balance via its own conversion. This is the
  protocol's exact conversion in one call. Caveat (documented honestly, Empty ≠
  Zero): ``maxWithdraw`` is capped at the silo's currently-borrowable liquidity, so a
  *fully-utilised* silo can under-report a user's collateral. For the accounting
  pre/post-state read of a user's own freshly-established position the user's own
  liquidity is present, so it returns the exact collateral; the alternative
  (reproducing Silo's non-standard share→asset rounding in a pure reducer) is
  strictly less reliable.
* **Debt** — the user's debt on the PAIRED (loan) silo, read as ``maxRepay(user)``
  (selector ``0x5f301149``, matching the connector adapter's ``max_repay`` selector):
  Silo V2 caps repay to the full outstanding debt in underlying assets, so for a
  query ``maxRepay`` returns the borrower's current debt.

Like Compound V3 (and unlike the Aave family), Silo V2 is **not USD-native** and its
read target is per-market (a silo, not one per-chain contract). The spec therefore:

* declares empty ``contract_kinds`` (market-scoped target; the registry binds the
  per-silo target from the ``SILO_V2_ACCOUNT_STATE_MARKETS`` table's ``comet_address``
  slot, which Silo repurposes as the collateral-silo address);
* declares ``normalize_market_id=str.lower`` (Silo synthetic market ids are
  ``"<collateral_symbol>/<loan_symbol>"`` strings, not 32-byte hashes); and
* declares a ``query_inputs_fn`` that synthesises the market id from the intent's
  tokens (Silo V2 intents carry no ``market_id``: SUPPLY/WITHDRAW name a single
  ``token``, BORROW/REPAY name ``collateral_token`` / ``borrow_token``), plus the
  collateral token the framework reader prices.

The spec stays pure (no gateway, no oracle): the framework reader owns price
resolution + the gateway round-trip. The reducer values both legs from the injected
``query.prices`` / ``query.decimals`` (Silo is not USD-native) and derives a simple
collateral/debt HF proxy (no on-chain liquidation-threshold read — Silo's LTV is a
SiloConfig per-market constant not exposed cheaply on the silo; the proxy is
``collateral_usd / debt_usd``, capped, with the no-debt sentinel).

Gateway-boundary note: this module performs **no** network egress — pure dict
literals + pure functions describing/decoding the reads. The gateway-routed
``eth_call`` lives in the framework reader
(:func:`~almanak.framework.accounting.lending_reads.read_lending_account_state`).

Silo V2 publishes no single-reserve ``LENDING_READ_SPEC``: its account state is read
per-silo via the reads above, not an Aave-style ``getUserReserveData(asset, user)``.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any

from almanak.connectors._strategy_base.lending_read_base import (
    AccountStateQuery,
    AccountStateReadSpec,
    EthCall,
    LendingAccountState,
    LendingPositionRef,
    decode_uint_hex,
    pad_address,
)

logger = logging.getLogger(__name__)

__all__ = [
    "ACCOUNT_STATE_READ_SPEC",
    "SILO_V2_ACCOUNT_STATE_MARKETS",
    "build_silo_account_state_market_table",
]

# ── Silo V2 read selectors (verified on-chain, Avalanche) ────────────────────
# maxWithdraw(address owner) → uint256 (the user's withdrawable underlying = collateral)
_MAX_WITHDRAW_SELECTOR = "0xce96cb77"
# maxRepay(address borrower) → uint256 (full outstanding debt in underlying assets).
# Matches the connector adapter's ``max_repay`` selector.
_MAX_REPAY_SELECTOR = "0x5f301149"

# No-debt / undefined-HF sentinel, also the serialisation cap for huge HFs
# (mirrors the Morpho / Compound family sentinels in lending_read_base).
_SILO_HF_SENTINEL = Decimal("999999")


def build_silo_account_state_market_table() -> dict[str, dict[str, dict[str, Any]]]:
    """Build the per-chain ``{market_id: params}`` Silo account-state catalogue.

    Derived from the connector's :data:`SILO_V2_MARKETS` registry so the silo
    addresses / token symbols stay single-sourced in ``adapter.py``. The registry
    resolves this table on demand via ``_MARKET_TABLE_LOADERS`` (never eagerly into
    the framework).

    Silo V2 intents carry **no** ``market_id``, so the catalogue is keyed by a
    synthetic ``"<collateral_symbol>/<loan_symbol>"`` id (lowercased) that the
    spec's ``query_inputs_fn`` reconstructs from the intent's tokens. Each market
    pair yields **two** directed entries (each asset can be the collateral leg):
    e.g. WAVAX/USDC yields ``"wavax/usdc"`` (deposit WAVAX, borrow USDC) and
    ``"usdc/wavax"`` (deposit USDC, borrow WAVAX).

    Params per entry:
        * ``comet_address``: the COLLATERAL silo address — the registry's generic
          market-scoped target binding reads ``params["comet_address"]`` as the
          read target (Silo repurposes the Compound-shaped slot for the deposit silo).
        * ``debt_silo_address``: the PAIRED (loan) silo address where the user's debt
          is read via ``maxRepay``.
        * ``collateral_token`` / ``loan_token``: token symbols the framework reader
          prices + whose decimals it injects (Silo is not USD-native).

    When the same token symbol appears as the collateral leg in multiple markets, the
    FIRST market listed in :data:`SILO_V2_MARKETS` wins (most-liquid-first) —
    byte-identical to the adapter's ``find_silo_for_asset`` default-market choice, so
    the account-state read targets the same silo the intent compiled against.
    """
    from almanak.connectors.silo_v2.adapter import SILO_V2_MARKETS

    table: dict[str, dict[str, Any]] = {}
    for market in SILO_V2_MARKETS.values():
        # Direction A: asset0 collateral, asset1 loan.
        # Direction B: asset1 collateral, asset0 loan.
        for collateral_sym, collateral_silo, loan_sym, loan_silo in (
            (market.asset0_symbol, market.silo0_address, market.asset1_symbol, market.silo1_address),
            (market.asset1_symbol, market.silo1_address, market.asset0_symbol, market.silo0_address),
        ):
            market_id = f"{collateral_sym}/{loan_sym}".lower()
            if market_id in table:
                # Most-liquid-first wins (mirrors find_silo_for_asset default).
                continue
            table[market_id] = {
                "comet_address": collateral_silo,
                "debt_silo_address": loan_silo,
                "collateral_token": collateral_sym,
                "loan_token": loan_sym,
            }
    return {"avalanche": table}


#: Per-chain Silo V2 account-state market catalogue (resolved by the registry's
#: ``market_params`` via ``_MARKET_TABLE_LOADERS``). See
#: :func:`build_silo_account_state_market_table` for the synthetic-market-id scheme.
SILO_V2_ACCOUNT_STATE_MARKETS: dict[str, dict[str, dict[str, Any]]] = build_silo_account_state_market_table()


def _synthesize_market_id(collateral_token: str | None, debt_token: str | None) -> str | None:
    """Reconstruct the synthetic ``"<collateral>/<loan>"`` catalogue id from tokens.

    Resolution order (mirrors the catalogue build + adapter default-market choice):

    1. Both tokens known (BORROW): exact ``"<collateral>/<debt>"`` match.
    2. Only the collateral token known (SUPPLY/WITHDRAW): first catalogue entry
       whose collateral leg matches (most-liquid-first).
    3. Only the debt token known (REPAY): first entry whose loan leg matches.

    Returns ``None`` (read fails closed — never a guessed silo) when no entry
    matches or no token was named.
    """
    table = SILO_V2_ACCOUNT_STATE_MARKETS.get("avalanche", {})
    col = collateral_token.lower() if collateral_token else None
    debt = debt_token.lower() if debt_token else None

    if col and debt:
        candidate = f"{col}/{debt}"
        return candidate if candidate in table else None
    if col:
        for mid in table:
            if mid.split("/", 1)[0] == col:
                return mid
        return None
    if debt:
        for mid, params in table.items():
            if str(params.get("loan_token", "")).lower() == debt:
                return mid
        return None
    return None


def _silo_query_inputs_from_intent(intent: Any) -> dict[str, Any]:
    """Derive Silo V2's per-read inputs (synthetic market id + collateral token).

    Silo V2 intents carry no ``market_id``; the silo to read is determined by the
    intent's tokens:

    * SUPPLY / WITHDRAW: ``intent.token`` is the collateral; the loan leg is the
      paired silo's asset (resolved through the catalogue).
    * BORROW / REPAY / DELEVERAGE: the debt leg is ``intent.borrow_token`` (BORROW)
      or ``intent.token`` (REPAY); the collateral leg is ``intent.collateral_token``
      (BORROW) or, for REPAY where only the repaid token is named, recovered from the
      resolved catalogue entry so the framework prices the right collateral token.

    Returns ``{"market_id": <synthetic id or None>, "collateral_token": <symbol>}``.
    ``market_id`` is ``None`` (→ read fails closed) when the intent names no usable
    token — never a guessed silo (Empty ≠ Zero).
    """
    it = getattr(intent, "intent_type", None)
    if it is None:
        intent_type = ""
    else:
        intent_type = it.value if hasattr(it, "value") else str(it)
    intent_type = intent_type.upper()

    if intent_type in ("SUPPLY", "WITHDRAW"):
        collateral_token = getattr(intent, "token", None)
        debt_token = None
    else:  # BORROW / REPAY / DELEVERAGE
        collateral_token = getattr(intent, "collateral_token", None)
        debt_token = getattr(intent, "borrow_token", None) or getattr(intent, "token", None)

    market_id = _synthesize_market_id(collateral_token, debt_token)
    # For REPAY the collateral leg is not on the intent; recover it from the resolved
    # catalogue entry so the framework reader prices the right collateral token.
    if collateral_token is None and market_id is not None:
        entry = SILO_V2_ACCOUNT_STATE_MARKETS.get("avalanche", {}).get(market_id)
        if entry is not None:
            collateral_token = entry.get("collateral_token")
    return {"market_id": market_id, "collateral_token": collateral_token}


def _silo_market_id_from_ref(ref: LendingPositionRef) -> str | None:
    """Reconstruct Silo's synthetic ``market_id`` from a typed position ref (VIB-5775).

    Pure token-attribute logic: the ref names both legs explicitly, so this is a thin
    adapter over :func:`_synthesize_market_id` (the SAME derivation the intent path's
    ``query_inputs_fn`` uses) keyed off ``ref.collateral_token`` / ``ref.loan_token``.
    Silo's catalogue is Avalanche-only, so ``ref.chain`` is not consulted (mirrors
    ``_synthesize_market_id``, which is chain-agnostic). Sharing ``_synthesize_market_id``
    with the intent path keeps the two ids drift-proof for the same tokens.

    Unlike the intent path, this does NOT do catalogue collateral-recovery: the ref
    already carries the collateral token. Returns ``None`` (never a guessed silo —
    Empty ≠ Zero) when the tokens name no catalogued market.
    """
    return _synthesize_market_id(ref.collateral_token, ref.loan_token)


def _decode_uint(blob: str | None) -> int | None:
    """Decode word 0 of a single-uint return blob, or ``None`` on a short/None blob.

    Fail-closed (Empty ≠ Zero): a missing / malformed blob is ``None`` (unmeasured),
    never a fabricated ``0``.
    """
    if not blob:
        return None
    raw = blob[2:] if blob[:2].lower() == "0x" else blob
    if len(raw) < 64:
        return None
    try:
        return decode_uint_hex(raw, 0)
    except (ValueError, ArithmeticError):
        return None


def _build_silo_account_state_calls(query: AccountStateQuery) -> list[EthCall]:
    """Emit the Silo V2 reads: ``maxWithdraw`` (collateral silo) + ``maxRepay`` (debt silo).

    Both are single eth_calls the protocol itself computes (no chained reads — the
    pure plan→reduce seam executes calls in parallel). Order is the contract
    :func:`_reduce_silo_account_state` decodes against (collateral first, debt second):

    1. ``maxWithdraw(user)`` on the COLLATERAL silo (``query.position_manager_address``,
       bound by the registry from the catalogue ``comet_address``).
    2. ``maxRepay(user)`` on the PAIRED debt silo (catalogue ``debt_silo_address``),
       emitted only when the paired silo is known.

    Fails closed (returns ``[]``) when the collateral silo target was not bound — the
    reducer (which requires the collateral blob) then also fails closed.
    """
    collateral_silo = query.position_manager_address
    if not collateral_silo:
        return []
    params = query.market_params or {}
    debt_silo = params.get("debt_silo_address")
    user_hex = pad_address(query.wallet_address)
    calls = [EthCall(to=collateral_silo, data=_MAX_WITHDRAW_SELECTOR + user_hex)]
    if debt_silo:
        calls.append(EthCall(to=str(debt_silo), data=_MAX_REPAY_SELECTOR + user_hex))
    return calls


def _reduce_silo_account_state(
    query: AccountStateQuery,
    results: list[str | None],
) -> LendingAccountState | None:
    """Decode Silo V2 ``[maxWithdraw, maxRepay]`` blobs into aggregate account state.

    Pure: values both legs from the injected ``query.prices`` / ``query.decimals``
    (Silo is not USD-native). Fails closed (returns ``None``, never a fabricated
    zero — Empty ≠ Zero) when: the collateral blob is missing/short; a required
    injected input (collateral token / price / decimals) is absent; OR a debt read
    was *planned* (a paired debt silo exists ⇒ ``len(results) > 1``) but its blob
    failed / was short / its loan-token price/decimals are missing. A debt of
    ``Decimal("0")`` is only ever emitted when there is NO paired debt silo (a pure
    single-leg collateral plan) — a planned-but-failed debt read must NOT collapse a
    heavily-indebted position to zero debt + a perfect HF (Gemini review 2026-06).

    Health-factor proxy: Silo's per-market liquidation LTV lives in the SiloConfig and
    is not read here (no cheap on-chain whole-account threshold like Aave's). The proxy
    is ``collateral_usd / debt_usd`` (capped at the sentinel), with the no-debt
    sentinel when there is no debt. ``liquidation_threshold_bps`` / ``e_mode_category``
    / ``lltv`` stay ``None`` (Silo has no analogue exposed here — Empty ≠ Zero).
    """
    collateral_token = query.collateral_token
    loan_token = query.loan_token
    prices = query.prices
    decimals = query.decimals
    # Collateral leg required: fail closed when the inputs to value it are missing.
    if collateral_token is None or prices is None or decimals is None:
        return None
    if collateral_token not in prices or collateral_token not in decimals:
        return None
    collateral_price = prices.get(collateral_token)
    if collateral_price is None:
        return None

    collateral_hex = results[0] if results else None
    collateral_raw = _decode_uint(collateral_hex)
    if collateral_raw is None:
        return None
    collateral_amount = Decimal(collateral_raw) / Decimal(10 ** decimals[collateral_token])
    collateral_usd = collateral_amount * collateral_price

    # Debt leg: a measured Decimal("0") ONLY when no debt read was planned (no paired
    # debt silo ⇒ single-call plan). If a debt read WAS planned (``len(results) > 1``),
    # any failure / missing input MUST fail closed — a failed RPC must never collapse a
    # heavily-indebted position to zero debt + a perfect HF (Gemini review 2026-06).
    debt_usd = Decimal("0")
    if len(results) > 1:
        debt_hex = results[1]
        if debt_hex is None or loan_token is None:
            return None
        if loan_token not in prices or loan_token not in decimals:
            return None
        loan_price = prices.get(loan_token)
        if loan_price is None:
            return None
        debt_raw = _decode_uint(debt_hex)
        if debt_raw is None:
            return None
        debt_amount = Decimal(debt_raw) / Decimal(10 ** decimals[loan_token])
        debt_usd = debt_amount * loan_price

    if debt_usd == 0:
        health_factor = _SILO_HF_SENTINEL
    else:
        health_factor = min(collateral_usd / debt_usd, _SILO_HF_SENTINEL)

    return LendingAccountState(
        collateral_usd=collateral_usd,
        debt_usd=debt_usd,
        health_factor=health_factor,
        liquidation_threshold_bps=None,  # Silo LTV lives in SiloConfig, not read here.
        e_mode_category=None,  # No e-mode concept.
        lltv=None,  # Not a Morpho-style per-market lltv surfaced here.
        family=None,  # Not the Aave family — no Aave-only serialized keys.
    )


#: Aggregate account-state read for Silo V2 (VIB-4965). Market-scoped read target
#: (empty ``contract_kinds`` → the per-silo target is bound by the registry from the
#: ``SILO_V2_ACCOUNT_STATE_MARKETS`` table's ``comet_address``). Silo is not
#: USD-native: both legs are priced via ``valuation_role_keys`` (collateral + loan
#: token symbols read from the synthetic-market catalogue). ``normalize_market_id=
#: str.lower`` because Silo synthetic market ids are ``"<col>/<loan>"`` strings, not
#: hashes. ``query_inputs_fn`` synthesises the market id from the intent's tokens
#: (Silo intents carry no ``market_id``).
ACCOUNT_STATE_READ_SPEC: AccountStateReadSpec = AccountStateReadSpec(
    contract_kinds=(),
    build_calls=_build_silo_account_state_calls,
    reduce_calls=_reduce_silo_account_state,
    valuation_role_keys=(
        ("collateral_token", "collateral_token"),
        ("loan_token", "loan_token"),
    ),
    normalize_market_id=str.lower,
    query_inputs_fn=_silo_query_inputs_from_intent,
    # VIB-5775: teardown/valuation/health carry a typed LendingPositionRef (no intent,
    # no market_id). Derive the synthetic id from the ref's tokens — pure, drift-proof
    # against the intent path (both share ``_synthesize_market_id``).
    market_id_from_ref=_silo_market_id_from_ref,
)
