"""BENQI lending-read capability (bespoke Compound-V2 qiToken whole-account state).

Publishes this connector's account-state read spec (VIB-4967) so the strategy-side
:class:`~almanak.connectors._strategy_base.lending_read_registry.LendingReadRegistry`
can let the framework lending reader query BENQI state without the framework
hardcoding BENQI's per-qiToken selection, function selectors, or HF math.

Why a *bespoke* reader (the ticket's core point): BENQI is a **Compound-V2 fork**
(qiTokens), NOT an Aave V3 fork — it has **no** ``getUserAccountData(user)``
whole-account aggregate. But BENQI *is* a **pooled, cross-asset** market (every
supplied qiToken can back a borrow of any other), exactly like the Aave family —
so the right model is a **WHOLE-ACCOUNT** read (sum every entered market), not a
per-pair read. A per-pair read cannot resolve a bare REPAY (``token`` only, no
collateral leg) in a pooled market: many collaterals back the same debt token, so
"which collateral?" is ambiguous (Empty ≠ Zero — never guess). The whole-account
read sidesteps that: it values the user's entire position regardless of which
intent leg triggered the read, mirroring how Aave V3 / Compound V3 (VIB-4633
Finding B) read whole-account state for a bare REPAY.

The reads (a fixed, deterministic fan-out the pure plan→reduce seam executes in
parallel — no chained ``getAssetsIn`` enumeration):

* For EACH listed qiToken:
  * ``getAccountSnapshot(user)`` (selector ``0xc37f68e2``) → ``(error,
    qiTokenBalance, borrowBalance, exchangeRateMantissa)``. The user's **supply**
    in that market is the canonical Compound V2 conversion
    ``qiTokenBalance * exchangeRateMantissa / 1e18`` (the mantissa already folds in
    the qiToken-8dec / underlying-dec offset, so the product is the underlying's
    native units — verified on-chain, Avalanche). The user's **debt** in that
    market is the SAME call's ``borrowBalance`` word (already the full outstanding
    borrow in the underlying's units; no shares→assets step).
  * ``Comptroller.markets(qiToken)`` (selector ``0x8e8f294b``) → ``(isListed,
    collateralFactorMantissa, isComped)`` — the per-market liquidation collateral
    factor (1e18-scaled; qiUSDC = 0.8 on-chain).
  * ``Comptroller.checkMembership(user, qiToken)`` (selector ``0x929fe9a1``) → bool —
    whether the user has ENTERED this qiToken as collateral (``enterMarkets``). In
    Compound V2 a bare mint (supply without entering) earns interest but does NOT
    back borrows, so supplied-but-not-entered (or exited) markets are EXCLUDED from
    ``collateral_usd`` / the liquidation HF. Debt counts regardless of membership.

**Health factor — a TRUE liquidation-aware HF, not a bare collateral/debt proxy.**
BENQI (Compound V2) exposes the liquidation parameter directly: each market's
``collateralFactorMantissa``. The reducer computes the product-owner summed health
factor ``HF = Σ_over_markets(supply_usd_i × collateralFactor_i) / Σ debt_usd`` —
the liquidation threshold a Compound-V2 position is actually measured against (a
borrow becomes liquidatable when weighted collateral falls below debt),
self-consistent in one oracle. This is the SAME ``Σ(value_usd × liquidation_factor)
/ debt_usd`` shape Compound V3's whole-account reducer
(``_reduce_compound_whole_account``) uses, NOT the bare ``collateral_usd /
debt_usd`` proxy the Silo/Euler readers fall back to (which VIB-4992 flagged as
unsafe to write into the HF field that risk surfaces consume). The no-debt sentinel
(``999999``) marks "no liquidation risk" when there is no debt.

Like Compound V3 / Morpho / Silo / Euler (and unlike the Aave family), BENQI is
**not USD-native**: the reducer values every leg from the injected ``query.prices``
/ ``query.decimals``. Because the read is whole-account with no single named
collateral, the framework reader injects a price + decimals for EVERY listed
underlying (the ``_inject_whole_account_collateral_prices`` path, keyed off the
market table's ``collaterals`` map) — exactly the Compound V3 bare-REPAY path.
BENQI's native-AVAX market (qiAVAX) values via ``WAVAX`` (same price, same 18
decimals; the accounting oracle prices ``WAVAX``, not the native-``AVAX`` sentinel).

Gateway-boundary note: this module performs **no** network egress — pure dict
literals + pure functions describing/decoding the reads. The gateway-routed
``eth_call`` lives in the framework reader
(:func:`~almanak.framework.accounting.lending_reads.read_lending_account_state`).

BENQI publishes no single-reserve ``LENDING_READ_SPEC``: its account state is read
per-qiToken via the reads above, not an Aave-style ``getUserReserveData``.
"""

from __future__ import annotations

import logging
from decimal import ROUND_HALF_UP, Decimal
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
    "BENQI_ACCOUNT_STATE_MARKETS",
    "build_benqi_account_state_market_table",
]

# Synthetic whole-account market id — BENQI intents carry no ``market_id``; the
# read is whole-account (every entered market summed), so a single id per chain
# binds the catalogue entry the reducer needs.
_BENQI_MARKET_ID = "benqi"

# ── BENQI / Compound-V2 read selectors (verified on-chain, Avalanche) ─────────
# getAccountSnapshot(address) → (error, qiTokenBalance, borrowBalance, exchangeRateMantissa)
_GET_ACCOUNT_SNAPSHOT_SELECTOR = "0xc37f68e2"
# Comptroller.markets(address qiToken) → (isListed, collateralFactorMantissa, isComped)
_MARKETS_SELECTOR = "0x8e8f294b"
# Comptroller.checkMembership(address account, address qiToken) → bool
# Compound V2: a supplied qiToken only counts toward the account's collateral /
# liquidity once it has been ENTERED as collateral (enterMarkets). A bare mint
# (supply without entering) is NOT liquidation collateral. So collateral / the
# liquidation-weighted HF MUST be gated on membership — see _reduce.
_CHECK_MEMBERSHIP_SELECTOR = "0x929fe9a1"

# Compound V2 exchange-rate mantissa scale (1e18): underlying =
# qiTokenBalance * exchangeRateMantissa / 1e18.
_EXCHANGE_RATE_SCALE = 10**18
# collateralFactorMantissa is 1e18-scaled (qiUSDC = 8e17 = 0.8 on-chain).
_CF_SCALE = Decimal("1e18")
# No-debt / undefined-HF sentinel, also the serialisation cap for huge HFs
# (mirrors the Morpho / Compound / Silo / Euler family sentinels).
_BENQI_HF_SENTINEL = Decimal("999999")


def _value_symbol(asset: str) -> str:
    """Map BENQI's intent/asset symbol → the priceable+resolvable accounting symbol.

    qiAVAX uses native AVAX; the accounting oracle prices ``WAVAX`` (identical price
    + 18 decimals). Every other asset prices under its own key.
    """
    return "WAVAX" if asset.upper() == "AVAX" else asset


def build_benqi_account_state_market_table() -> dict[str, dict[str, dict[str, Any]]]:
    """Build the per-chain ``{market_id: params}`` BENQI account-state catalogue.

    Derived from the connector's :data:`BENQI_QI_TOKENS` registry so the qiToken
    addresses / token symbols stay single-sourced in ``adapter.py``. The registry
    resolves this table on demand via ``_MARKET_TABLE_LOADERS`` (never eagerly into
    the framework).

    BENQI is a pooled cross-asset market read WHOLE-ACCOUNT, so there is a SINGLE
    synthetic market id per chain (``"benqi"``). Its params carry everything the
    pure reducer needs plus the ``collaterals`` map the framework reader's
    whole-account price-injection path (``_inject_whole_account_collateral_prices``)
    keys off:

        * ``comet_address``: a sentinel non-empty target (the Comptroller) so the
          registry's market-scoped existence gate passes. The reducer never uses it
          as a read target — every read names its own qiToken / the Comptroller from
          ``markets`` below.
        * ``comptroller_address``: the BENQI Comptroller (where each market's
          ``collateralFactorMantissa`` is read for the true HF).
        * ``markets``: ordered list of ``{qi_token, symbol}`` (the deterministic order
          ``build_calls`` emits the per-qiToken reads in, and ``reduce_calls`` decodes
          them back). ``symbol`` is the PRICEABLE accounting symbol (WAVAX for the
          native-AVAX market).
        * ``collaterals``: ``{priceable_symbol: {address}}`` so the framework reader
          injects a USD price + decimals for every underlying (whole-account, no
          single named collateral — mirrors the Compound V3 bare-REPAY path). The
          address is the qiToken's underlying ERC-20 (native AVAX → WAVAX, since the
          accounting oracle/resolver key on the wrapped token).
    """
    from almanak.connectors.benqi.adapter import BENQI_COMPTROLLER_ADDRESS, BENQI_QI_TOKENS

    # WAVAX ERC-20 address (the priceable proxy for native AVAX). Sourced from the
    # USDC-sibling underlying convention is unsafe; resolve it from the well-known
    # wrapped-native address on Avalanche. The framework reader re-resolves decimals
    # via the token resolver, so this address only needs to be the canonical WAVAX.
    _WAVAX_ADDRESS = "0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7"

    markets: list[dict[str, Any]] = []
    collaterals: dict[str, dict[str, Any]] = {}
    for asset, info in BENQI_QI_TOKENS.items():
        sym = _value_symbol(asset)
        markets.append({"qi_token": info["qi_token"], "symbol": sym})
        # Underlying ERC-20 address for pricing; native AVAX → WAVAX.
        underlying_addr = info["underlying"] if info.get("underlying") else _WAVAX_ADDRESS
        collaterals[sym] = {"address": underlying_addr}

    return {
        "avalanche": {
            _BENQI_MARKET_ID: {
                "comet_address": BENQI_COMPTROLLER_ADDRESS,  # sentinel non-empty target (existence gate)
                "comptroller_address": BENQI_COMPTROLLER_ADDRESS,
                "markets": markets,
                "collaterals": collaterals,
            }
        }
    }


#: Per-chain BENQI account-state market catalogue (resolved by the registry's
#: ``market_params`` via ``_MARKET_TABLE_LOADERS``). Single whole-account market id
#: per chain — see :func:`build_benqi_account_state_market_table`.
BENQI_ACCOUNT_STATE_MARKETS: dict[str, dict[str, dict[str, Any]]] = build_benqi_account_state_market_table()


def _benqi_query_inputs_from_intent(intent: Any) -> dict[str, Any]:
    """Derive BENQI's per-read inputs.

    BENQI's read is whole-account (every entered market summed), so it needs no
    per-intent token selection: every lending intent type resolves to the SAME
    synthetic ``"benqi"`` market id, and ``collateral_token`` is ``None`` so the
    framework reader takes its whole-account price-injection path (pricing every
    listed underlying via the market table's ``collaterals`` map — the Compound V3
    bare-REPAY pattern). This is what lets a bare REPAY (``token`` only) reach a
    HIGH-confidence whole-account state in a pooled market, instead of degrading on
    an ambiguous per-pair collateral guess (Empty ≠ Zero — never guess).
    """
    return {"market_id": _BENQI_MARKET_ID, "collateral_token": None}


def _benqi_market_id_from_ref(ref: LendingPositionRef) -> str | None:
    """Return BENQI's fixed whole-account synthetic ``market_id`` (VIB-5775).

    BENQI is a pooled, cross-asset market read WHOLE-ACCOUNT: there is a single
    synthetic id per chain (``"benqi"``) and the read is intent/token-agnostic (it
    values the user's entire position regardless of which leg triggered it — the
    same reason ``_benqi_query_inputs_from_intent`` ignores the intent's tokens). So
    the ref's tokens are irrelevant and this always returns the fixed id, matching
    the account-state (intent) path exactly.
    """
    return _BENQI_MARKET_ID


def _decode_account_snapshot(blob: str | None) -> tuple[int, int, int] | None:
    """Decode a ``getAccountSnapshot`` blob into ``(qiTokenBalance, borrowBalance, exchangeRate)``.

    Layout: ``(error, qiTokenBalance, borrowBalance, exchangeRateMantissa)`` — 4
    uint256 words. Fail-closed (Empty ≠ Zero): a missing / short blob, or a
    non-zero ``error`` word (Compound returns ``error != 0`` on a failed snapshot),
    returns ``None`` (unmeasured), never a fabricated ``0``.
    """
    if not blob:
        return None
    raw = blob[2:] if blob[:2].lower() == "0x" else blob
    if len(raw) < 4 * 64:
        return None
    try:
        error = decode_uint_hex(raw, 0)
        if error != 0:
            return None
        qi_balance = decode_uint_hex(raw, 1)
        borrow_balance = decode_uint_hex(raw, 2)
        exchange_rate = decode_uint_hex(raw, 3)
    except (ValueError, ArithmeticError):
        return None
    return qi_balance, borrow_balance, exchange_rate


def _decode_collateral_factor(blob: str | None) -> Decimal | None:
    """Decode the ``collateralFactorMantissa`` (word 1) from a ``markets`` blob.

    Layout: ``(isListed bool, collateralFactorMantissa uint, isComped bool)`` — 3
    words. Returns the collateral factor as a fraction (mantissa / 1e18), or
    ``None`` (fail-closed) when the blob is missing/short, the market is not listed
    (word 0 == 0), or the value is malformed (Empty ≠ Zero).
    """
    if not blob:
        return None
    raw = blob[2:] if blob[:2].lower() == "0x" else blob
    if len(raw) < 3 * 64:
        return None
    try:
        is_listed = decode_uint_hex(raw, 0)
        if is_listed == 0:
            return None
        cf_mantissa = decode_uint_hex(raw, 1)
    except (ValueError, ArithmeticError):
        return None
    return Decimal(cf_mantissa) / _CF_SCALE


def _decode_membership(blob: str | None) -> bool:
    """Decode a ``checkMembership`` bool blob → True only on a well-formed truthy word.

    Fail-closed (Empty ≠ Zero): a missing / short / malformed blob decodes to
    ``False`` (treat-as-not-entered) — a market whose membership cannot be confirmed
    must NOT be counted as liquidation collateral (over-counting would inflate the
    HF and mask liquidation risk).
    """
    if not blob:
        return False
    raw = blob[2:] if blob[:2].lower() == "0x" else blob
    if len(raw) < 64:
        return False
    try:
        return decode_uint_hex(raw, 0) != 0
    except (ValueError, ArithmeticError):
        return False


def _ordered_markets(query: AccountStateQuery) -> list[dict[str, Any]]:
    """Return the deterministic per-qiToken market list from the injected params.

    Defensively filters to dict entries (the catalogue always emits dicts, but a
    malformed injected ``market_params`` must never raise — fail closed to an empty
    plan instead, Empty ≠ Zero).
    """
    params = query.market_params or {}
    markets = params.get("markets")
    if not isinstance(markets, list):
        return []
    return [m for m in markets if isinstance(m, dict)]


def _build_benqi_account_state_calls(query: AccountStateQuery) -> list[EthCall]:
    """Emit the whole-account BENQI reads: per qiToken ``getAccountSnapshot`` + ``markets`` + ``checkMembership``.

    Pure plan→reduce: a fixed, deterministic fan-out (THREE calls per listed qiToken)
    the framework reader executes in parallel — no chained ``getAssetsIn``
    enumeration. The emit order is the contract :func:`_reduce_benqi_account_state`
    decodes against: ``[snapshot_0, markets_0, membership_0, snapshot_1, …]``.

    The membership read (``Comptroller.checkMembership(user, qiToken)``) is what makes
    the collateral / liquidation-HF accounting *correct* for Compound V2: a supplied
    qiToken counts as liquidation collateral ONLY once it has been ENTERED
    (``enterMarkets``). A bare mint (supply without entering) earns interest but does
    NOT back borrows, so it must be excluded from ``collateral_usd`` /
    ``weighted_collateral_usd`` (CodeRabbit 2026-06). Debt (``borrowBalance``) always
    counts regardless of membership.

    Fails closed (returns ``[]``) when the Comptroller address or the per-qiToken
    market list is missing from the injected params (the reducer, which requires the
    blobs, then also fails closed).
    """
    params = query.market_params or {}
    comptroller = params.get("comptroller_address")
    markets = _ordered_markets(query)
    if not comptroller or not markets:
        return []
    comptroller_str = str(comptroller)
    user_hex = pad_address(query.wallet_address)
    calls: list[EthCall] = []
    for market in markets:
        qi_token = market.get("qi_token")
        if not qi_token:
            return []  # Malformed market table — fail closed, never a partial read.
        qi_hex = pad_address(str(qi_token))
        calls.append(EthCall(to=str(qi_token), data=_GET_ACCOUNT_SNAPSHOT_SELECTOR + user_hex))
        calls.append(EthCall(to=comptroller_str, data=_MARKETS_SELECTOR + qi_hex))
        # checkMembership(account, qiToken): account first, then qiToken.
        calls.append(EthCall(to=comptroller_str, data=_CHECK_MEMBERSHIP_SELECTOR + user_hex + qi_hex))
    return calls


def _reduce_benqi_account_state(
    query: AccountStateQuery,
    results: list[str | None],
) -> LendingAccountState | None:
    """Decode the whole-account BENQI ``[snapshot_i, markets_i, membership_i, …]`` blobs.

    Pure: values every leg from the injected ``query.prices`` / ``query.decimals``
    (BENQI is not USD-native, keyed by the market table's priceable symbols). For
    each listed qiToken (three blobs):

    * Decode ``getAccountSnapshot`` → supply (``qiBal × exchangeRate / 1e18``) +
      borrow. An ``error != 0`` / short blob fails the whole read closed.
    * Decode ``checkMembership`` → whether the qiToken is ENTERED as collateral. A
      supplied-but-not-entered (or exited) market does NOT back borrows in Compound
      V2, so it is EXCLUDED from collateral / the liquidation HF (a missing/malformed
      membership blob decodes to ``False`` — never over-count collateral). This is the
      correctness gate CodeRabbit (2026-06) called out.
    * Decode ``markets`` → the collateral factor; required only when a market both
      HOLDS supply AND is entered (a held+entered leg with no readable CF fails closed
      — never a fabricated HF).
    * A counted (entered+supplied, or borrowed) leg whose price/decimals were NOT
      injected fails the whole read closed (Empty ≠ Zero — under-counting would
      inflate the HF and mask liquidation risk). An untouched / supplied-but-not-
      entered-with-no-debt market contributes nothing and needs no price (skipped).

    Sums ``collateral_usd`` (Σ entered supply_usd), ``debt_usd`` (Σ borrow_usd), and
    the liquidation-weighted collateral (Σ entered supply_usd_i × collateralFactor_i).
    Computes the TRUE liquidation-aware ``HF = weighted_collateral / debt_usd`` (the
    on-chain Compound-V2 liquidation parameter, NOT a bare collateral/debt proxy),
    capped at the sentinel, with the no-debt sentinel when there is no debt. Fails
    closed (returns ``None``, never a fabricated zero) on any missing required input.
    """
    prices = query.prices
    decimals = query.decimals
    if prices is None or decimals is None:
        return None
    markets = _ordered_markets(query)
    if not markets:
        return None
    # Expect exactly three blobs per listed market (snapshot + markets + membership).
    if len(results) != 3 * len(markets):
        return None

    collateral_usd = Decimal("0")
    weighted_collateral_usd = Decimal("0")
    debt_usd = Decimal("0")

    for i, market in enumerate(markets):
        sym = market.get("symbol")
        snapshot = _decode_account_snapshot(results[3 * i])
        if snapshot is None or not isinstance(sym, str):
            return None  # A listed market's snapshot must decode — Empty ≠ Zero.
        qi_balance, borrow_balance, exchange_rate = snapshot
        # supply underlying = qiTokenBalance * exchangeRateMantissa / 1e18 (Compound V2),
        # on ints first so the 1e18-mantissa product keeps full precision.
        supply_raw = (qi_balance * exchange_rate) // _EXCHANGE_RATE_SCALE
        # Entered-as-collateral gate: supplied-but-not-entered supply is NOT
        # liquidation collateral (Compound V2). Membership fails closed to False.
        entered = _decode_membership(results[3 * i + 2])
        counts_as_collateral = supply_raw > 0 and entered

        if not counts_as_collateral and borrow_balance == 0:
            # Untouched, or supplied-but-not-entered with no debt — contributes
            # nothing to collateral / debt / HF; no price needed (Empty ≠ Zero).
            continue

        if sym not in prices or sym not in decimals or prices[sym] is None or decimals[sym] is None:
            # A counted leg we cannot value ⇒ fail closed (never under-count). A None
            # decimals would also raise on ``10 ** None`` — guard it (Empty ≠ Zero).
            return None
        scale = Decimal(10 ** decimals[sym])
        price = prices[sym]

        if counts_as_collateral:
            supply_usd = (Decimal(supply_raw) / scale) * price
            collateral_usd += supply_usd
            cf = _decode_collateral_factor(results[3 * i + 1])
            if cf is None:
                # Entered collateral with no readable liquidation factor ⇒ cannot
                # compute HF honestly. Fail closed (Empty ≠ Zero).
                return None
            weighted_collateral_usd += supply_usd * cf

        if borrow_balance > 0:
            debt_usd += (Decimal(borrow_balance) / scale) * price

    if debt_usd == 0:
        health_factor: Decimal | None = _BENQI_HF_SENTINEL
    else:
        # TRUE liquidation-aware HF: liquidation-weighted collateral / debt (the
        # on-chain Compound-V2 collateral factors), NOT a bare collateral/debt proxy.
        health_factor = min(weighted_collateral_usd / debt_usd, _BENQI_HF_SENTINEL)

    return LendingAccountState(
        collateral_usd=collateral_usd,
        debt_usd=debt_usd,
        health_factor=health_factor,
        # Per-asset collateral factors are folded into ``health_factor`` (the
        # summed liquidation-weighted collateral / debt), exactly as Compound V3's
        # whole-account reducer does — so no single ``liquidation_threshold_bps`` is
        # emitted (Empty ≠ Zero — there is no one weighted-average threshold here).
        liquidation_threshold_bps=None,
        e_mode_category=None,  # BENQI / Compound V2 has no e-mode concept.
        lltv=None,  # Not a Morpho-style per-market lltv (keeps common-3-keys serialization).
        family=None,  # Not the Aave family — no Aave-only serialized keys.
    )


# Re-export so the bps helper used in tests stays single-sourced even though the
# whole-account reducer folds CFs into the HF rather than emitting a single bps.
def collateral_factor_to_bps(cf: Decimal) -> int:
    """Convert a 1.0-scaled collateral factor to integer basis points (ROUND_HALF_UP)."""
    return int((cf * Decimal("10000")).to_integral_value(rounding=ROUND_HALF_UP))


#: Aggregate account-state read for BENQI (VIB-4967). Market-scoped read target
#: (empty ``contract_kinds`` → bound by the registry from the single whole-account
#: ``BENQI_ACCOUNT_STATE_MARKETS`` entry). BENQI is not USD-native: every listed
#: underlying is priced via the framework reader's whole-account injection path
#: (``collaterals`` map). ``normalize_market_id=str.lower`` because the BENQI market
#: id is the ``"benqi"`` string, not a hash. ``query_inputs_fn`` returns the fixed
#: whole-account market id with no named collateral (BENQI intents carry no
#: ``market_id``, and the whole-account read is intent-agnostic).
ACCOUNT_STATE_READ_SPEC: AccountStateReadSpec = AccountStateReadSpec(
    contract_kinds=(),
    build_calls=_build_benqi_account_state_calls,
    reduce_calls=_reduce_benqi_account_state,
    # No declared valuation roles: the whole-account read prices every listed
    # underlying via the framework's ``collaterals``-map injection (Compound V3
    # bare-REPAY path), not a fixed (collateral, loan) pair.
    valuation_role_keys=(),
    normalize_market_id=str.lower,
    query_inputs_fn=_benqi_query_inputs_from_intent,
    # VIB-5775: teardown/valuation/health carry a typed LendingPositionRef (no intent,
    # no market_id). BENQI's whole-account read is token-agnostic → the fixed id.
    market_id_from_ref=_benqi_market_id_from_ref,
)
