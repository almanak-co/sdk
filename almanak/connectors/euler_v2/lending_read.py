"""Euler V2 lending-read capability (vault/EVC aggregate account-state).

Publishes this connector's account-state read spec (VIB-4966) so the strategy-side
:class:`~almanak.connectors._strategy_base.lending_read_registry.LendingReadRegistry`
can let the framework lending reader query Euler V2 state without the framework
hardcoding Euler's per-vault selection, function selectors, or HF math.

Why a *bespoke* reader (the ticket's core point): unlike the Aave family, Euler V2
has **no** Aave-style ``getUserAccountData(user)`` whole-account aggregate. Euler V2
is a vault-based ERC-4626 model coordinated by the Ethereum Vault Connector (EVC):
each asset has one (or more) independent ERC-4626 vaults; depositing into one vault
(enabled as collateral on the EVC) backs a borrow from another vault (the
controller). So aggregate account state is assembled from per-vault reads, each a
single eth_call the protocol itself computes (no reimplemented ERC-4626 share math —
the pure plan→reduce seam executes independent calls in parallel and cannot chain
``convertToAssets(balanceOf(user))``):

* **Collateral** — the user's deposit-vault position, read as ``maxWithdraw(user)``
  (selector ``0xce96cb77``, matching the connector adapter's ``MAX_WITHDRAW_SELECTOR``):
  the underlying assets the vault lets the user withdraw, which it derives from the
  user's share balance via its own ERC-4626 conversion. This is the protocol's exact
  conversion in one call. Caveat (documented honestly, Empty ≠ Zero): an EVault's
  ``maxWithdraw`` is capped at the vault's currently-available cash, so a
  *fully-utilised* vault can under-report a user's collateral. For the accounting
  pre/post-state read of a user's own freshly-established position the user's own
  liquidity is present, so it returns the exact collateral; the alternative
  (chaining ``balanceOf`` → ``convertToAssets`` in a pure reducer) is impossible on
  the single-pass plan→reduce seam, and reproducing ERC-4626 rounding off cached
  totals is strictly less reliable.
* **Debt** — the user's debt on the BORROW (controller) vault, read as
  ``debtOf(user)`` (selector ``0xd283e75f``, matching the connector adapter's
  ``DEBT_OF_SELECTOR``): Euler V2 returns the borrower's full current debt in the
  vault's underlying assets directly (no shares→assets conversion needed). For a
  collateral-only position (SUPPLY/WITHDRAW) there is no controller vault, so no debt
  read is planned and the debt is a MEASURED ``Decimal("0")``.

Like Compound V3 / Morpho (and unlike the Aave family), Euler V2 is **not
USD-native** and its read target is per-vault (a vault, not one per-chain contract).
The spec therefore:

* declares empty ``contract_kinds`` (market-scoped target; the registry binds the
  per-vault target from the ``EULER_V2_ACCOUNT_STATE_MARKETS`` table's
  ``comet_address`` slot, which Euler repurposes as the collateral-vault address);
* declares ``normalize_market_id=str.lower`` (Euler synthetic market ids are
  ``"<collateral_symbol>"`` or ``"<collateral_symbol>/<loan_symbol>"`` strings, not
  32-byte hashes); and
* declares a ``query_inputs_fn`` that synthesises the market id from the intent's
  tokens (Euler V2 intents carry no ``market_id``: SUPPLY/WITHDRAW name a single
  ``token``; BORROW names ``collateral_token`` / ``borrow_token``; REPAY names the
  repaid ``token``), plus the collateral token the framework reader prices.

The spec stays pure (no gateway, no oracle): the framework reader owns price
resolution + the gateway round-trip. The reducer values both legs from the injected
``query.prices`` / ``query.decimals`` (Euler is not USD-native) and derives a simple
collateral/debt HF proxy (no on-chain liquidation-LTV read — Euler's per-vault LTV
lives on the controller's ``LTVList`` and is not exposed cheaply on the vault; the
proxy is ``collateral_usd / debt_usd``, capped, with the no-debt sentinel).

Gateway-boundary note: this module performs **no** network egress — pure dict
literals + pure functions describing/decoding the reads. The gateway-routed
``eth_call`` lives in the framework reader
(:func:`~almanak.framework.accounting.lending_reads.read_lending_account_state`).

Euler V2 publishes no single-reserve ``LENDING_READ_SPEC``: its account state is read
per-vault via the reads above, not an Aave-style ``getUserReserveData(asset, user)``.
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
    "EULER_V2_ACCOUNT_STATE_MARKETS",
    "build_euler_account_state_market_table",
]

# ── Euler V2 read selectors (verified against the connector adapter + cast sig) ──
# maxWithdraw(address owner) → uint256 (the user's withdrawable underlying = collateral).
# Matches euler_v2/adapter.py:MAX_WITHDRAW_SELECTOR.
_MAX_WITHDRAW_SELECTOR = "0xce96cb77"
# debtOf(address account) → uint256 (full outstanding debt in the vault's underlying).
# Matches euler_v2/adapter.py:DEBT_OF_SELECTOR.
_DEBT_OF_SELECTOR = "0xd283e75f"

# No-debt / undefined-HF sentinel, also the serialisation cap for huge HFs
# (mirrors the Morpho / Compound / Silo family sentinels in lending_read_base).
_EULER_HF_SENTINEL = Decimal("999999")


def _preferred_vault_by_symbol(chain_vaults: dict[str, dict]) -> dict[str, dict[str, Any]]:
    """Collapse a chain's vault registry to one entry per underlying symbol.

    Mirrors the adapter's ``find_vault_for_asset`` default-vault choice: the
    ``preferred`` vault wins, else the first-listed vault for that underlying
    symbol. Returns ``{underlying_symbol_lower: {vault_address, decimals, preferred}}``
    so the account-state read targets the SAME vault the intent compiled against.
    """
    by_symbol: dict[str, dict[str, Any]] = {}
    for info in chain_vaults.values():
        sym = info["underlying_symbol"].lower()
        existing = by_symbol.get(sym)
        if existing is None:
            by_symbol[sym] = {
                "vault_address": info["vault_address"],
                "decimals": info["decimals"],
                "preferred": bool(info.get("preferred", False)),
            }
        elif info.get("preferred", False) and not existing["preferred"]:
            # A preferred vault supersedes a non-preferred first-seen one.
            by_symbol[sym] = {
                "vault_address": info["vault_address"],
                "decimals": info["decimals"],
                "preferred": True,
            }
    return by_symbol


def build_euler_account_state_market_table() -> dict[str, dict[str, dict[str, Any]]]:
    """Build the per-chain ``{market_id: params}`` Euler V2 account-state catalogue.

    Derived from the connector's :data:`EULER_V2_VAULTS_BY_CHAIN` registry so the
    vault addresses / token symbols stay single-sourced in ``adapter.py``. The
    registry resolves this table on demand via ``_MARKET_TABLE_LOADERS`` (never
    eagerly into the framework).

    Euler V2 intents carry **no** ``market_id``, so the catalogue is keyed by a
    synthetic id the spec's ``query_inputs_fn`` reconstructs from the intent's
    tokens. Each chain yields two id shapes:

    * **Collateral-only** ``"<collateral_symbol>"`` (lowercased): a SUPPLY/WITHDRAW
      position with no controller vault (debt is a measured zero). ``params`` carries
      only the collateral leg.
    * **Directed pair** ``"<collateral_symbol>/<loan_symbol>"`` (lowercased): a
      BORROW/REPAY position. ``params`` additionally carries the paired debt vault
      and the loan token symbol.

    Params per entry:
        * ``comet_address``: the COLLATERAL vault address — the registry's generic
          market-scoped target binding reads ``params["comet_address"]`` as the read
          target (Euler repurposes the Compound-shaped slot for the deposit vault).
        * ``debt_vault_address``: the BORROW (controller) vault where the user's debt
          is read via ``debtOf`` (present only on directed-pair entries).
        * ``collateral_token`` / ``loan_token``: token symbols the framework reader
          prices + whose decimals it injects (Euler is not USD-native). ``loan_token``
          is ``None`` on collateral-only entries (no debt leg to value).

    A market id is keyed by the preferred vault's underlying symbol (mirrors
    ``find_vault_for_asset``), so the read targets the same vault the intent
    compiled against.
    """
    from almanak.connectors.euler_v2.adapter import EULER_V2_VAULTS_BY_CHAIN

    table: dict[str, dict[str, dict[str, Any]]] = {}
    for chain, chain_vaults in EULER_V2_VAULTS_BY_CHAIN.items():
        by_symbol = _preferred_vault_by_symbol(chain_vaults)
        chain_table: dict[str, dict[str, Any]] = {}
        # Collateral-only ids (SUPPLY / WITHDRAW): one per underlying symbol.
        for col_sym, col in by_symbol.items():
            chain_table[col_sym] = {
                "comet_address": col["vault_address"],
                "collateral_token": col_sym.upper(),
                "loan_token": None,
            }
        # Directed pair ids (BORROW / REPAY): every ordered (collateral, loan) pair
        # of distinct underlying symbols on the chain.
        for col_sym, col in by_symbol.items():
            for loan_sym, loan in by_symbol.items():
                if col_sym == loan_sym:
                    continue
                chain_table[f"{col_sym}/{loan_sym}"] = {
                    "comet_address": col["vault_address"],
                    "debt_vault_address": loan["vault_address"],
                    "collateral_token": col_sym.upper(),
                    "loan_token": loan_sym.upper(),
                }
        table[chain] = chain_table
    return table


#: Per-chain Euler V2 account-state market catalogue (resolved by the registry's
#: ``market_params`` via ``_MARKET_TABLE_LOADERS``). See
#: :func:`build_euler_account_state_market_table` for the synthetic-market-id scheme.
EULER_V2_ACCOUNT_STATE_MARKETS: dict[str, dict[str, dict[str, Any]]] = build_euler_account_state_market_table()


def _markets_for_chain(chain: str) -> dict[str, dict[str, Any]]:
    """Return the synthetic-market catalogue for ``chain`` (empty when unsupported)."""
    return EULER_V2_ACCOUNT_STATE_MARKETS.get(chain.lower(), {})


def _synthesize_market_id(
    chain: str,
    collateral_token: str | None,
    debt_token: str | None,
) -> str | None:
    """Reconstruct the synthetic catalogue id from intent tokens.

    Resolution order (mirrors the catalogue build + adapter default-vault choice):

    1. Both tokens known (BORROW): exact ``"<collateral>/<debt>"`` directed-pair
       match.
    2. Only the collateral token known (SUPPLY/WITHDRAW): the collateral-only
       ``"<collateral>"`` entry.
    3. Only the debt token known (REPAY): the directed-pair entry whose loan leg
       matches — but ONLY when exactly one such pair exists. When several collaterals
       back the same debt token the collateral leg is ambiguous, so this fails closed
       (``None``) rather than guess: a wrong collateral vault would make ``maxWithdraw``
       return 0 → a fabricated zero collateral + a wrong HF (Empty ≠ Zero). A
       standalone REPAY that names only the repaid token then honestly degrades to
       ESTIMATED.

    Returns ``None`` (read fails closed — never a guessed vault) when no entry
    matches, the debt-only match is ambiguous, or no token was named.
    """
    table = _markets_for_chain(chain)
    col = collateral_token.lower() if collateral_token else None
    debt = debt_token.lower() if debt_token else None

    if col and debt:
        candidate = f"{col}/{debt}"
        return candidate if candidate in table else None
    if col:
        return col if col in table else None
    if debt:
        # Fail closed unless the debt token maps to EXACTLY ONE directed-pair market.
        # When several collaterals back the same debt token, the collateral leg is
        # ambiguous: returning the first match would bind a collateral vault the user
        # may never have deposited into, and ``maxWithdraw`` on it returns 0 — a
        # FABRICATED zero collateral + a wrong HF (Empty ≠ Zero — never guess the
        # collateral). Standalone REPAY intents that name only the repaid token then
        # honestly degrade to ESTIMATED rather than report a misleading position.
        matches = [
            mid
            for mid, params in table.items()
            if isinstance(params.get("loan_token"), str) and str(params["loan_token"]).lower() == debt
        ]
        return matches[0] if len(matches) == 1 else None
    return None


def _euler_query_inputs_from_intent(intent: Any) -> dict[str, Any]:
    """Derive Euler V2's per-read inputs (synthetic market id + collateral token).

    Euler V2 intents carry no ``market_id``; the vault(s) to read are determined by
    the intent's tokens:

    * SUPPLY / WITHDRAW: ``intent.token`` is the collateral; there is no controller
      vault (collateral-only ``"<col>"`` market id → no debt read).
    * BORROW: collateral is ``intent.collateral_token``, debt is
      ``intent.borrow_token`` → directed-pair ``"<col>/<loan>"`` market id.
    * REPAY / DELEVERAGE: the debt leg is ``intent.borrow_token`` (DELEVERAGE) or
      ``intent.token`` (REPAY); the collateral leg is ``intent.collateral_token``
      when named, else recovered from the resolved catalogue entry so the framework
      prices the right collateral token.

    Returns ``{"market_id": <synthetic id or None>, "collateral_token": <symbol>}``.
    ``market_id`` is ``None`` (→ read fails closed) when the intent names no usable
    token — never a guessed vault (Empty ≠ Zero).
    """
    it = getattr(intent, "intent_type", None)
    if it is None:
        intent_type = ""
    else:
        intent_type = it.value if hasattr(it, "value") else str(it)
    intent_type = intent_type.upper()
    chain = getattr(intent, "chain", "") or ""

    if intent_type in ("SUPPLY", "WITHDRAW"):
        collateral_token = getattr(intent, "token", None)
        debt_token = None
    else:  # BORROW / REPAY / DELEVERAGE
        collateral_token = getattr(intent, "collateral_token", None)
        debt_token = getattr(intent, "borrow_token", None) or getattr(intent, "token", None)

    market_id = _synthesize_market_id(chain, collateral_token, debt_token)
    # For REPAY the collateral leg is not on the intent; recover it from the resolved
    # catalogue entry so the framework reader prices the right collateral token.
    if collateral_token is None and market_id is not None:
        entry = _markets_for_chain(chain).get(market_id)
        if entry is not None:
            collateral_token = entry.get("collateral_token")
    return {"market_id": market_id, "collateral_token": collateral_token}


def _euler_market_id_from_ref(ref: LendingPositionRef) -> str | None:
    """Reconstruct Euler's synthetic ``market_id`` from a typed position ref (VIB-5775).

    Pure token-attribute logic: the ref names BOTH legs explicitly, so this is a thin
    adapter over :func:`_synthesize_market_id` (the SAME derivation the intent path's
    ``query_inputs_fn`` uses) keyed off ``ref.collateral_token`` / ``ref.loan_token``
    on ``ref.chain``. Because both share ``_synthesize_market_id``, the ref-derived id
    can never drift from the account-state (intent) path's id for the same tokens.

    Unlike the intent path, this does NOT do catalogue collateral-recovery: the ref
    already carries the collateral token, so there is nothing to recover. Returns
    ``None`` (never a guessed vault — Empty ≠ Zero) when the tokens are ambiguous or
    name no catalogued market.
    """
    return _synthesize_market_id(ref.chain, ref.collateral_token, ref.loan_token)


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


def _build_euler_account_state_calls(query: AccountStateQuery) -> list[EthCall]:
    """Emit the Euler V2 reads: ``maxWithdraw`` (collateral vault) + ``debtOf`` (debt vault).

    Both are single eth_calls the protocol itself computes (no chained reads — the
    pure plan→reduce seam executes calls in parallel). Order is the contract
    :func:`_reduce_euler_account_state` decodes against (collateral first, debt
    second):

    1. ``maxWithdraw(user)`` on the COLLATERAL vault (``query.position_manager_address``,
       bound by the registry from the catalogue ``comet_address``).
    2. ``debtOf(user)`` on the BORROW (controller) vault (catalogue
       ``debt_vault_address``), emitted only when a borrow vault is known
       (directed-pair market id).

    Fails closed (returns ``[]``) when the collateral vault target was not bound —
    the reducer (which requires the collateral blob) then also fails closed.
    """
    collateral_vault = query.position_manager_address
    if not collateral_vault:
        return []
    params = query.market_params or {}
    debt_vault = params.get("debt_vault_address")
    user_hex = pad_address(query.wallet_address)
    calls = [EthCall(to=collateral_vault, data=_MAX_WITHDRAW_SELECTOR + user_hex)]
    if debt_vault:
        calls.append(EthCall(to=str(debt_vault), data=_DEBT_OF_SELECTOR + user_hex))
    return calls


def _reduce_euler_account_state(
    query: AccountStateQuery,
    results: list[str | None],
) -> LendingAccountState | None:
    """Decode Euler V2 ``[maxWithdraw, debtOf]`` blobs into aggregate account state.

    Pure: values both legs from the injected ``query.prices`` / ``query.decimals``
    (Euler is not USD-native). Fails closed (returns ``None``, never a fabricated
    zero — Empty ≠ Zero) when: the collateral blob is missing/short; a required
    injected input (collateral token / price / decimals) is absent; OR a debt read
    was *planned* (a paired borrow vault exists ⇒ ``len(results) > 1``) but its blob
    failed / was short / its loan-token price/decimals are missing. A debt of
    ``Decimal("0")`` is only ever emitted when there is NO paired borrow vault (a
    pure single-leg collateral plan) — a planned-but-failed debt read must NOT
    collapse a heavily-indebted position to zero debt + a perfect HF.

    Health-factor proxy: Euler's per-vault liquidation LTV lives on the controller's
    ``LTVList`` and is not read here (no cheap on-chain whole-account threshold like
    Aave's). The proxy is ``collateral_usd / debt_usd`` (capped at the sentinel),
    with the no-debt sentinel when there is no debt. ``liquidation_threshold_bps`` /
    ``e_mode_category`` / ``lltv`` stay ``None`` (Euler has no analogue exposed here
    — Empty ≠ Zero).
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
    # borrow vault ⇒ single-call plan). If a debt read WAS planned (``len(results) >
    # 1``), any failure / missing input MUST fail closed — a failed RPC must never
    # collapse a heavily-indebted position to zero debt + a perfect HF.
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
        health_factor = _EULER_HF_SENTINEL
    else:
        health_factor = min(collateral_usd / debt_usd, _EULER_HF_SENTINEL)

    return LendingAccountState(
        collateral_usd=collateral_usd,
        debt_usd=debt_usd,
        health_factor=health_factor,
        liquidation_threshold_bps=None,  # Euler LTV lives on the controller LTVList, not read here.
        e_mode_category=None,  # No e-mode concept.
        lltv=None,  # Not a Morpho-style per-market lltv surfaced here.
        family=None,  # Not the Aave family — no Aave-only serialized keys.
    )


#: Aggregate account-state read for Euler V2 (VIB-4966). Market-scoped read target
#: (empty ``contract_kinds`` → the per-vault target is bound by the registry from the
#: ``EULER_V2_ACCOUNT_STATE_MARKETS`` table's ``comet_address``). Euler is not
#: USD-native: both legs are priced via ``valuation_role_keys`` (collateral + loan
#: token symbols read from the synthetic-market catalogue). ``normalize_market_id=
#: str.lower`` because Euler synthetic market ids are ``"<col>"`` / ``"<col>/<loan>"``
#: strings, not hashes. ``query_inputs_fn`` synthesises the market id from the
#: intent's tokens (Euler intents carry no ``market_id``).
ACCOUNT_STATE_READ_SPEC: AccountStateReadSpec = AccountStateReadSpec(
    contract_kinds=(),
    build_calls=_build_euler_account_state_calls,
    reduce_calls=_reduce_euler_account_state,
    valuation_role_keys=(
        ("collateral_token", "collateral_token"),
        ("loan_token", "loan_token"),
    ),
    normalize_market_id=str.lower,
    query_inputs_fn=_euler_query_inputs_from_intent,
    # VIB-5775: teardown/valuation/health carry a typed LendingPositionRef (no intent,
    # no market_id). Derive the synthetic id from the ref's tokens — pure, drift-proof
    # against the intent path (both share ``_synthesize_market_id``).
    market_id_from_ref=_euler_market_id_from_ref,
)
