"""Lending accounting event builder (VIB-3418).

Wired into strategy_runner after every successful SUPPLY / BORROW / REPAY / WITHDRAW.

Before-state (VIB-3489): captured via capture_lending_pre_state() called by the runner
                          BEFORE the transaction is submitted.  The runner passes the
                          result as pre_execution_state to build_lending_accounting_event().
                          If the read fails, None is passed and before fields are None
                          with an unavailable_reason note — never fabricated or stale.

After-state (Aave V3): Pool.getUserAccountData — one call gives collateral_usd,
                        debt_usd, health_factor, liquidation_threshold.

After-state (Morpho Blue): position(id, user) + market(id) — two calls give collateral
                            (raw units), borrow shares, and market totals needed to
                            convert shares → assets. lltv comes from the market params
                            stored in the adapter registry.

FIFO interest attribution:
  BORROW → record_borrow() adds a principal lot to FIFOBasisStore.
  REPAY  → match_repay() consumes lots FIFO; interest = repay_amount − principal_consumed.
            If no lots exist for the position, unmatched_amount is non-zero and
            interest_delta_usd is None (UNAVAILABLE — never fabricated).
"""

from __future__ import annotations

import dataclasses
import logging
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from almanak.framework.accounting.basis import FIFOBasisStore

from almanak.connectors._strategy_base.lending_read_base import LendingAccountState
from almanak.framework.accounting.gas_pricing import native_token_for_chain
from almanak.framework.accounting.ids import make_accounting_event_id

# VIB-4851 PR-2: the light account-state *readers* live in ``lending_reads`` so the
# framework data surface (``MarketSnapshot`` → ``position_health``) can reach them
# without importing this module's heavy ``execution.*`` closure (pulled via
# ``gas_pricing`` / ``ids`` above, which only the accounting *event-builder* needs).
# Re-exported here UNCHANGED so every existing
# ``from almanak.framework.accounting.lending_accounting import read_lending_account_state``
# (and the private ``_gateway_eth_call`` / ``_resolve_oracle_price`` / ``_pad_address`` /
# ``read_aave_user_emode`` helpers) keeps working — pure relocation, no behaviour change.
from almanak.framework.accounting.lending_reads import (  # noqa: F401  (back-compat re-export — see comment above)
    _gateway_eth_call,
    _pad_address,
    _resolve_oracle_price,
    read_aave_user_emode,
    read_lending_account_state,
    read_lending_market_health,
)

logger = logging.getLogger(__name__)

# VIB-4929 PR-3a/3b: Aave + Morpho + Compound V3 aggregate account-state reads go
# through the single generic ``read_lending_account_state``, which drives the
# connector-owned specs (``AAVE_FORK_ACCOUNT_STATE_READ`` /
# ``MORPHO_BLUE_ACCOUNT_STATE_READ`` / ``COMPOUND_V3_ACCOUNT_STATE_READ`` in
# ``lending_read_base``) via ``LendingReadRegistry``. Adding a lending connector
# to the read path requires ZERO framework edits here — no per-protocol
# ``read_<protocol>_account_state`` function, no selector, scale, cap, lltv, HF
# sentinel, or decode is duplicated in this module. ``read_aave_user_emode`` is
# the one remaining single-call helper (used by the Tier-2 Aave registry), still
# decoding via the imported ``_AAVE_GET_USER_EMODE_SELECTOR`` + ``parse_user_emode_hex``.
# Compound V3's selectors + decode now live in ``lending_read_base`` alongside the
# other specs (folded in at PR-3b — it gained addresses.py + a derived market table).

# ─── Lending intent types ──────────────────────────────────────────────────────
_LENDING_INTENT_TYPES = frozenset({"SUPPLY", "BORROW", "REPAY", "WITHDRAW", "DELEVERAGE"})

# Chain native gas token resolution lives in ``gas_pricing.native_token_for_chain`` —
# a single framework source of truth shared with the EVM gas_usd writer
# (VIB-3805). The previous local map diverged on plasma (ETH vs XPL) and
# missed several chains in the gateway-side ``NATIVE_TOKEN_SYMBOLS``.


def _decode_word(hex_data: str, word_index: int) -> int:
    start = word_index * 64
    return int(hex_data[start : start + 64], 16)


# Protocols the generic ``read_lending_account_state`` path is *enabled* for on the
# live-money accounting read path. Registering an account-state spec
# (``LendingReadRegistry._ACCOUNT_STATE_LOADERS``) makes a connector spec-*capable*,
# but ENABLING it here is a deliberate, per-protocol opt-in: each entry was migrated
# AND fork/byte-equivalence-verified in its PR. This gate is what stops a connector
# that merely registered a spec — from silently producing HIGH-confidence reads.
# Add a protocol here only once its generic read is verified on a real fork.
# VIB-4929 PR-3b: ``compound_v3`` joined — its byte-equivalence to the retired
# ``read_compound_v3_account_state`` was proven (collateral+borrow HF via LCF,
# base-asset supply HF=999999, missing-price → None) before enabling it here.
# VIB-4929 PR-3c / VIB-4963: ``spark`` joined — it reuses the already-verified
# ``AAVE_FORK_ACCOUNT_STATE_READ`` spec (identical ``getUserAccountData`` ABI to
# Aave V3, USD-denominated on-chain), fork-verified on ethereum (HIGH-confidence
# before/after collateral / debt / HF on a real Spark position).
# VIB-4965: ``silo_v2`` joined — a BESPOKE per-silo reader (Silo V2 has no
# Aave-style ``getUserAccountData``; its isolated ERC-4626 silos are read via
# ``maxWithdraw`` on the deposit silo + ``maxRepay`` on the paired debt silo, both
# protocol-computed single eth_calls). Not USD-native (priced via the injected
# valuation seam, like Compound/Morpho). Fork-verified on avalanche by the Layer-5
# Silo intent tests (HIGH-confidence before/after collateral / debt / HF).
_GENERIC_PRE_STATE_PROTOCOLS: frozenset[str] = frozenset(
    {"aave_v3", "aave", "morpho_blue", "compound_v3", "spark", "silo_v2"}
)


def _overlay_aave_interest_rate_mode(state: LendingAccountState, intent: Any) -> LendingAccountState:
    """Overlay the Aave intent-layer ``interest_rate_mode`` onto a decoded state.

    Aave's ``interest_rate_mode`` is intent metadata, not an on-chain field — the
    generic reader never decodes it. For BORROW/REPAY intents we thread it onto
    the (frozen) :class:`LendingAccountState` via ``dataclasses.replace`` so it
    lands in ``pre_state_json`` / ``post_state_json``, mirroring the pre-VIB-4929
    Aave pre-state capture behaviour byte-for-byte:

    * ``intent.interest_rate_mode`` set → ``str(...)`` of it.
    * unset on a BORROW/REPAY → ``"variable"`` (the rate mode the on-chain tx
      actually carries; stable mode is deprecated on Aave V3 —
      ``connectors/base/lending/aave_helpers.py``).

    SUPPLY/WITHDRAW (and non-BORROW/REPAY) leave it ``None``.
    """
    intent_type_str = _intent_type_value(intent).upper()
    if intent_type_str not in {"BORROW", "REPAY"}:
        return state
    rate_mode = getattr(intent, "interest_rate_mode", None)
    # InterestRateMode is a ``Literal["variable"]`` at the intent layer; str()
    # handles both the Literal value and any future enum. Falls back to the
    # rate mode the BORROW/REPAY dispatch will actually carry.
    resolved = str(rate_mode) if rate_mode is not None else "variable"
    return dataclasses.replace(state, interest_rate_mode=resolved)


def capture_lending_pre_state(
    *,
    intent: Any,
    chain: str,
    wallet_address: str,
    gateway_client: Any | None,
    price_oracle: dict | None,
    block: int | str | None = None,
) -> LendingAccountState | None:
    """Read on-chain lending state BEFORE the transaction is submitted (VIB-3489).

    Called by the strategy runner before executing the intent bundle.  The
    returned state is later forwarded as ``pre_execution_state`` to
    ``build_lending_accounting_event()`` so that before/after deltas can be
    computed.

    Returns None (silently, with a debug log) when:
    - The gateway client is not available.
    - The intent is not a supported lending protocol (Aave V3 / Morpho Blue / Compound V3).
    - Any gateway eth_call fails.

    Never raises; never substitutes stale data on failure.

    VIB-4929 PR-3a/3b dispatch: Aave V3 + Morpho Blue + Compound V3 route through
    the single generic :func:`read_lending_account_state`, but ONLY for protocols
    explicitly enabled in ``_GENERIC_PRE_STATE_PROTOCOLS`` (migrated AND
    fork/byte-equivalence-verified in their PR). A connector that merely *registers*
    an account-state spec is spec-capable but is NOT auto-enabled on this
    live-money read path — e.g. Spark (an Aave-fork that opted into
    ``_ACCOUNT_STATE_LOADERS``) stays unread → ESTIMATED until it is verified and
    added (VIB-4963).

    VIB-4589 / F7: ``block`` pins every underlying eth_call to a single
    block reference. Pre-state captures pass ``None`` (→ ``"latest"`` — safe
    because the read precedes submission). Post-state captures pass
    ``receipt.block_number`` (via :func:`capture_lending_post_state`) so the
    snapshot reflects exactly the state produced by the confirmed receipt
    and cannot race the upstream RPC's receipt indexer.
    """
    from almanak.connectors._strategy_base.lending_read_registry import LendingReadRegistry

    if gateway_client is None:
        return None

    if _intent_type_value(intent) not in _LENDING_INTENT_TYPES:
        return None

    protocol = str(getattr(intent, "protocol", "") or "").lower()

    # Generic path — gated to the explicitly-enabled, fork-verified protocols
    # (``_GENERIC_PRE_STATE_PROTOCOLS``). A spec-capable-but-unverified connector
    # (e.g. Spark — VIB-4963) is NOT read here: it stays unread (→ ESTIMATED),
    # preserving pre-VIB-4929 behavior rather than silently upgrading to HIGH.
    if protocol not in _GENERIC_PRE_STATE_PROTOCOLS:
        return None
    inputs = LendingReadRegistry.query_inputs(protocol, intent)
    if inputs is None:
        return None

    state = read_lending_account_state(
        protocol=protocol,
        chain=chain,
        wallet_address=wallet_address,
        gateway_client=gateway_client,
        price_oracle=price_oracle,
        block=block,
        **inputs,
    )
    if state is None:
        return None
    # Aave-family intent-metadata overlay (interest_rate_mode). Gated on the
    # structural family discriminator the reducer stamps, not a protocol-name
    # string — keeps the framework consumer protocol-agnostic.
    if state.family == "aave":
        state = _overlay_aave_interest_rate_mode(state, intent)
    return state


def capture_lending_post_state(
    *,
    intent: Any,
    chain: str,
    wallet_address: str,
    gateway_client: Any | None,
    price_oracle: dict | None,
    block: int | str | None = None,
) -> LendingAccountState | None:
    """Read on-chain lending state AFTER the transaction confirms (VIB-3474).

    The post-state capture is the missing piece that ships
    ``transaction_ledger.post_state_json`` for lending intents. The legacy
    ``build_lending_accounting_event()`` performed the same read inline; we now
    expose it as a standalone capture so the runner can populate the column
    *before* it is serialised to the ledger row, which the new
    ``category_handlers/lending_handler.py`` then reads back.

    The implementation delegates to ``capture_lending_pre_state`` — the
    only difference is temporal (called by the runner after TX confirmation).
    VIB-4589 / F7: callers SHOULD pass ``block=receipt.block_number`` so the
    read pins to the exact block of the confirmed receipt. The pre-fix
    behaviour (``block=None`` → ``"latest"``) caused stale post-state on
    mainnet when the upstream RPC's receipt indexer trailed the call site
    — a confirmed WITHDRAW receipt was not yet visible to the next
    ``"latest"`` view, so the read returned a near-full collateral balance.

    Returns ``None`` (silently, with a debug log) when the intent isn't a
    supported lending protocol or any gateway call fails. Never raises; never
    fabricates stale data.
    """
    return capture_lending_pre_state(
        intent=intent,
        chain=chain,
        wallet_address=wallet_address,
        gateway_client=gateway_client,
        price_oracle=price_oracle,
        block=block,
    )


def lending_state_to_dict(
    state: LendingAccountState | None,
    *,
    protocol: str,
) -> dict[str, Any] | None:
    """Serialize a captured lending state to the ``pre_state_json`` /
    ``post_state_json`` shape that ``category_handlers/lending_handler.py`` reads.

    Returns ``None`` when ``state`` is ``None`` so callers can fall through
    to the wallet-balances-only path without fabricating zeros.

    Schema (Accounting-AttemptNo17 §3 D3, extended by VIB-4213 §Aave V3):
    ```json
    {
        "protocol": "aave_v3",
        "collateral_usd": "15420.50",
        "debt_usd": "8200.00",
        "health_factor": "1.882",
        "liquidation_threshold_bps": 8500,
        "e_mode_category": 0,
        "interest_rate_mode": "variable",
        "lltv": "0.86"
    }
    ```

    All numeric fields are stringified Decimals — the handler parses with
    ``Decimal(str(post_state["..."]))`` so JSON round-trip is loss-free.

    VIB-4929 PR-3a/3b: Aave + Morpho + Compound V3 now share the unified
    :class:`LendingAccountState`. The persisted dict stays **byte-identical** to
    the pre-PR per-protocol shapes:

    * **Aave family** (``state.family == "aave"``): emits ``liquidation_threshold_bps``
      (decoded int), ``e_mode_category``, AND ``interest_rate_mode``. CRITICAL: the
      last two keys are emitted **even when their value is ``None``** (JSON null) —
      the pre-PR ``isinstance(AaveAccountState)`` branch did this unconditionally,
      so they are gated on the **structural** ``family`` discriminator, NOT on
      value-presence. Dropping them when ``None`` would silently shrink the
      persisted dict. Empty ≠ Zero: a measured ``e_mode_category == 0`` (user not
      in any e-mode) stays distinguishable from ``null`` (read failed).
    * **Morpho family** (``LendingAccountState`` with ``lltv`` set, no Aave
      discriminator): emits ``lltv`` (str) + a derived ``liquidation_threshold_bps``
      (``round(lltv * 10000)``, ROUND_HALF_UP) and never the Aave-only keys.
    * **Compound V3** (``LendingAccountState`` with ``family=None`` and ``lltv=None``):
      only the common three keys — neither the Aave nor the Morpho branch fires.
    """
    if state is None:
        return None
    out: dict[str, Any] = {"protocol": protocol.lower()}
    # collateral_usd / debt_usd / health_factor are present on every state type.
    out["collateral_usd"] = str(state.collateral_usd) if state.collateral_usd is not None else None
    out["debt_usd"] = str(state.debt_usd) if state.debt_usd is not None else None
    out["health_factor"] = str(state.health_factor) if state.health_factor is not None else None

    if isinstance(state, LendingAccountState):
        if state.family == "aave":
            # Gated on the structural family discriminator, NOT value-presence —
            # the pre-PR AaveAccountState branch emitted all three keys
            # unconditionally. liquidation_threshold_bps is always populated for a
            # non-None Aave read (the spec requires the primary getUserAccountData
            # blob); int() matches the pre-PR cast.
            if state.liquidation_threshold_bps is not None:
                out["liquidation_threshold_bps"] = int(state.liquidation_threshold_bps)
            # e_mode_category (int | None) — emit None (JSON null) when the
            # secondary getUserEMode read failed; the raw int otherwise (incl. the
            # measured ``0`` = "not in any e-mode").
            out["e_mode_category"] = state.e_mode_category
            # interest_rate_mode (str | None) — set on BORROW/REPAY only;
            # SUPPLY/WITHDRAW and the post-state path leave it None ⇒ JSON null.
            out["interest_rate_mode"] = state.interest_rate_mode
        elif state.lltv is not None:
            # Morpho family: lltv IS the liquidation threshold; surface it in bps
            # too so the handler's lltv-aware path doesn't need to branch on protocol.
            out["lltv"] = str(state.lltv)
            try:
                out["liquidation_threshold_bps"] = int(
                    (state.lltv * Decimal("10000")).to_integral_value(rounding="ROUND_HALF_UP")
                )
            except (InvalidOperation, TypeError, ValueError):
                pass
    return out


def _derive_position_key(protocol: str, chain: str, wallet: str, market_id: str | None, asset: str) -> str:
    """Canonical position key for a lending position."""
    parts = ["lending", chain.lower(), protocol.lower(), wallet.lower()]
    if market_id:
        parts.append(market_id.lower())
    parts.append(asset.lower())
    return ":".join(parts)


def _intent_asset(intent: Any) -> str:
    """Extract the primary asset symbol from a lending intent."""
    # SUPPLY / WITHDRAW: intent.token
    # BORROW: intent.borrow_token (collateral_token is the collateral side)
    # REPAY: intent.token
    for attr in ("borrow_token", "token"):
        v = getattr(intent, attr, None)
        if v:
            return str(v)
    return "UNKNOWN"


def _intent_market_id(intent: Any) -> str | None:
    return getattr(intent, "market_id", None)


def _intent_type_value(intent: Any) -> str:
    it = getattr(intent, "intent_type", None)
    if it is None:
        return ""
    return it.value if hasattr(it, "value") else str(it)


def _to_lending_event_type(intent_type_str: str):
    """Map IntentType string to LendingEventType.  Returns None for non-lending intents."""
    from almanak.framework.accounting.models import LendingEventType

    _MAP = {
        "SUPPLY": LendingEventType.SUPPLY,
        "BORROW": LendingEventType.BORROW,
        "REPAY": LendingEventType.REPAY,
        "WITHDRAW": LendingEventType.WITHDRAW,
        "DELEVERAGE": LendingEventType.DELEVERAGE,
    }
    return _MAP.get(intent_type_str.upper())


def _select_lending_raw_amount(extracted: dict) -> int | None:
    """Return the canonical raw-int amount for a lending intent from enriched data.

    MorphoMay15 §6.2 (F2): Morpho Blue isolated-market SUPPLY intents emit
    ``SupplyCollateral`` on-chain — distinct from the loan-side ``Supply``.
    The enricher's per-protocol overlay (``EXTRACTION_SPECS_BY_PROTOCOL[
    "morpho_blue"]``) surfaces the collateral assets as
    ``supply_collateral_amount``. Without including it in this lookup,
    ``raw_amount`` stays ``None`` for Morpho collateral supplies and the
    SUPPLY accounting branch silently emits ``amount_token=None`` /
    ``principal_delta_usd=None``. ``supply_amount`` retains precedence so the
    loan-side path is unchanged. The symmetric ``withdraw_collateral_amount``
    slot is reserved for the WITHDRAW leg once the Morpho parser exposes
    that extractor.
    """
    return (
        extracted.get("supply_amount")
        or extracted.get("supply_collateral_amount")
        or extracted.get("borrow_amount")
        or extracted.get("repay_amount")
        or extracted.get("withdraw_amount")
    )


def _ray_to_bps(ray_value: int | float | Decimal | str | None) -> int | None:
    """Convert an APR value to integer basis-points (1 bps = 0.01 %).

    Accepts two input forms:
    - Already-fractional decimal (e.g. Decimal("0.05") → 500 bps): produced
      by Aave V3 / Spark receipt parsers which pre-normalize from ray.
    - Raw ray integer (≥ 1, scale 1e27): produced by synthetic test fixtures.
    """
    if ray_value is None:
        return None
    try:
        v = Decimal(str(ray_value))
        if v < Decimal("1"):
            # Already normalized fraction (e.g. 0.05 = 5% APY)
            bps = v * Decimal("10000")
        else:
            # Raw ray — divide by 1e27 first
            bps = v / Decimal("1e27") * Decimal("10000")
        return int(bps.to_integral_value(rounding="ROUND_HALF_UP"))
    except Exception:
        return None


def _amount_to_usd(amount_human: Decimal | None, price_oracle: dict | None, asset: str) -> Decimal | None:
    """Convert a human-readable token amount to USD using the price_oracle dict.

    Tolerant of both flat and nested oracle shapes via :func:`_resolve_oracle_price`.
    """
    if amount_human is None:
        return None
    price = _resolve_oracle_price(price_oracle, asset)
    if price is None:
        return None
    try:
        return price * amount_human
    except (InvalidOperation, ValueError, ArithmeticError):
        return None


# crap-allowlist: VIB-4437 / VIB-4440 — replay-path counterpart of handle_lending
# (also allowlisted at lending_handler.py:78 under VIB-4257). Dispatches over 5
# lending intent types × 3 protocols (Aave, Morpho, Compound); CRAP=165 (cc=99,
# cov=81%) reflects the integration matrix, not a tidiness gap. Refactor tracked
# under VIB-4440 and must follow .claude/rules/crap-refactor.md.
def build_lending_accounting_event(  # noqa: C901
    *,
    intent: Any,
    result: Any,
    deployment_id: str,
    cycle_id: str,
    execution_mode: str,
    chain: str,
    wallet_address: str,
    gateway_client: Any | None,
    basis_store: FIFOBasisStore,
    price_oracle: dict | None,
    ledger_entry_id: str | None = None,
    pre_execution_state: LendingAccountState | None = None,
) -> Any | None:
    """Build a LendingAccountingEvent for a completed lending intent.

    Returns None for non-lending intents or if the intent type cannot be mapped.

    pre_execution_state (VIB-3489): on-chain account state captured BEFORE the
    transaction was submitted, obtained by calling capture_lending_pre_state()
    in the runner.  When None, before fields are left as None rather than
    fabricated — honest absence is always preferred over stale data.

    FIFO lot tracking:
      - BORROW  → records a lot; interest_delta_usd = None at borrow time.
      - REPAY   → matches lots; interest_delta_usd = excess over principal.
      - SUPPLY / WITHDRAW → principal_delta_usd only.
    """
    from almanak.framework.accounting.models import (
        AccountingConfidence,
        AccountingIdentity,
        LendingAccountingEvent,
    )

    intent_type_str = _intent_type_value(intent)
    if intent_type_str not in _LENDING_INTENT_TYPES:
        return None

    lending_event_type = _to_lending_event_type(intent_type_str)
    if lending_event_type is None:
        return None

    now = datetime.now(UTC)
    protocol = getattr(intent, "protocol", "") or ""
    asset = _intent_asset(intent)
    market_id = _intent_market_id(intent)
    position_key = _derive_position_key(protocol, chain, wallet_address, market_id, asset)

    extracted = getattr(result, "extracted_data", None) or {}
    tx_hash = getattr(result, "tx_hash", None) or ""

    # ── Amounts & APRs from extracted_data ────────────────────────────────────
    raw_amount: int | None = _select_lending_raw_amount(extracted)
    amount_human: Decimal | None = None
    if raw_amount is not None:
        try:
            from almanak.framework.data.tokens.resolver import get_token_resolver

            resolver = get_token_resolver()
            token_info = resolver.resolve(asset, chain=chain)
            if token_info is None:
                logger.debug("token resolution returned None for %s on %s, skipping amount", asset, chain)
            else:
                amount_human = Decimal(str(raw_amount)) / Decimal(10**token_info.decimals)
        except Exception:
            logger.debug("token decimal resolution failed for %s, skipping amount conversion", asset)

    supply_apr_bps = _ray_to_bps(extracted.get("supply_rate"))
    borrow_apr_bps = _ray_to_bps(extracted.get("borrow_rate"))

    # ── Gas ───────────────────────────────────────────────────────────────────
    # ExecutionResult exposes total_gas_cost_wei (sum of all tx costs in the bundle).
    # Convert to native-token units (wei → 1e18), then look up the chain-specific
    # gas token (ETH on EVM L1/L2, AVAX on Avalanche, etc.).
    gas_cost_wei = getattr(result, "total_gas_cost_wei", None)
    gas_cost_native: Decimal | None = None
    if gas_cost_wei is not None and gas_cost_wei > 0:
        try:
            gas_cost_native = Decimal(str(gas_cost_wei)) / Decimal(10**18)
        except Exception:
            pass
    native_token = native_token_for_chain(chain)
    gas_usd = _amount_to_usd(gas_cost_native, price_oracle, native_token)

    # ── FIFO lot matching ─────────────────────────────────────────────────────
    principal_delta_usd: Decimal | None = None
    interest_delta_usd: Decimal | None = None

    # VIB-3964: a single chain+wallet wallet-basis pool is shared across the SWAP
    # handler and the lending writers — BORROW / WITHDRAW credit it, SUPPLY /
    # REPAY drain it. Mirroring on-chain wallet flow into the FIFO store is what
    # lets a SWAP that disposes a borrowed (or withdrawn) token report a non-null
    # ``realized_pnl_usd`` and unblocks the looping G6 reconciliation cell.
    _chain_norm = chain.lower().strip() if chain else ""
    _wallet_norm = wallet_address.lower().strip() if wallet_address else ""
    swap_wallet_key = f"swap:{_chain_norm}:{_wallet_norm}" if _chain_norm and _wallet_norm else ""

    if amount_human is not None:
        if intent_type_str == "BORROW":
            principal_delta_usd = _amount_to_usd(amount_human, price_oracle, asset)
            _borrow_id_seed = tx_hash or ledger_entry_id or position_key
            basis_store.record_borrow(
                deployment_id=deployment_id,
                position_key=position_key,
                token=asset,
                principal_amount=amount_human,
                principal_usd=principal_delta_usd,
                timestamp=now,
                lot_id=make_accounting_event_id(deployment_id, cycle_id, "BORROW_LOT", _borrow_id_seed, position_key),
                source_ledger_entry_id=ledger_entry_id,
            )
            # VIB-3964: borrowed tokens land in the wallet — credit the wallet
            # basis pool so a follow-up SWAP that disposes them gets a basis.
            if swap_wallet_key:
                basis_store.record_swap_acquisition(
                    deployment_id=deployment_id,
                    position_key=swap_wallet_key,
                    token=asset,
                    amount=amount_human,
                    cost_usd=principal_delta_usd,
                    timestamp=now,
                    lot_id=make_accounting_event_id(
                        deployment_id, cycle_id, "BORROW_WALLET_LOT", _borrow_id_seed, asset
                    ),
                    source="BORROW",
                )
            interest_delta_usd = None  # interest accrues, not known at borrow time

        elif intent_type_str in ("REPAY", "DELEVERAGE"):
            # DELEVERAGE is structurally a repay: it reduces an open borrow lot.
            match_result = basis_store.match_repay(
                deployment_id=deployment_id,
                position_key=position_key,
                token=asset,
                repay_amount=amount_human,
            )
            if match_result.unmatched_amount > 0:
                # No basis lots → interest is UNAVAILABLE, not zero
                logger.debug(
                    "%s unmatched for %s: unmatched=%.6f (no BORROW lots recorded)",
                    intent_type_str,
                    position_key,
                    match_result.unmatched_amount,
                )
                principal_delta_usd = _amount_to_usd(match_result.repaid_principal, price_oracle, asset)
                interest_delta_usd = None  # UNAVAILABLE — cannot fabricate
            else:
                principal_delta_usd = _amount_to_usd(match_result.repaid_principal, price_oracle, asset)
                interest_delta_usd = _amount_to_usd(match_result.interest_or_yield, price_oracle, asset)
            # VIB-3964: REPAY drains wallet inventory — dispose the swap-key
            # lots so the wallet pool stays consistent with on-chain balance.
            # Returned (cost_consumed, unmatched) is intentionally discarded
            # here; lending realized-PnL still routes through match_repay
            # above. The disposal exists purely to mirror wallet flow.
            if swap_wallet_key:
                basis_store.match_swap_disposal(
                    deployment_id=deployment_id,
                    position_key=swap_wallet_key,
                    token=asset,
                    amount=amount_human,
                )

        elif intent_type_str == "SUPPLY":
            principal_delta_usd = _amount_to_usd(amount_human, price_oracle, asset)
            # VIB-3964: SUPPLY drains wallet inventory — dispose to keep the
            # wallet basis pool truthful for a later WITHDRAW-then-SWAP.
            if swap_wallet_key:
                basis_store.match_swap_disposal(
                    deployment_id=deployment_id,
                    position_key=swap_wallet_key,
                    token=asset,
                    amount=amount_human,
                )
            # VIB-3964 (G6 closer): also record the supplied principal as a
            # BORROW-style lot keyed under ``supply:<lending_pk>`` so a later
            # WITHDRAW can FIFO-match and surface ``interest_accrued_usd``.
            # Symmetric with the live writer path in
            # ``category_handlers/lending_handler.py`` — keeping the two writers
            # in lock-step is the contract that prevents drift between live
            # and replay (CodeRabbit 2026-05-04).
            _supply_id_seed = tx_hash or ledger_entry_id or position_key
            basis_store.record_borrow(
                deployment_id=deployment_id,
                position_key=f"supply:{position_key}",
                token=asset,
                principal_amount=amount_human,
                principal_usd=principal_delta_usd,
                timestamp=now,
                lot_id=make_accounting_event_id(
                    deployment_id, cycle_id, "SUPPLY_LOT", _supply_id_seed, f"supply:{position_key}"
                ),
                source_ledger_entry_id=ledger_entry_id,
            )

        elif intent_type_str == "WITHDRAW":
            # Total withdraw value in USD — used as wallet-basis lot cost
            # but NOT as the event's ``principal_delta_usd``. The split
            # mirrors REPAY (pr-auditor 2026-05-04 item 2): principal is
            # the matched supply principal only; the residual is interest.
            _withdraw_total_usd = _amount_to_usd(amount_human, price_oracle, asset)
            # VIB-3964: WITHDRAW credits the wallet (principal + accrued
            # supply interest). Mint a swap-key lot for the FULL withdraw
            # amount so the next SWAP that disposes the withdrawn token
            # can compute realized PnL.
            if swap_wallet_key:
                _withdraw_id_seed = tx_hash or ledger_entry_id or position_key
                basis_store.record_swap_acquisition(
                    deployment_id=deployment_id,
                    position_key=swap_wallet_key,
                    token=asset,
                    amount=amount_human,
                    cost_usd=_withdraw_total_usd,
                    timestamp=now,
                    lot_id=make_accounting_event_id(
                        deployment_id, cycle_id, "WITHDRAW_WALLET_LOT", _withdraw_id_seed, asset
                    ),
                    source="WITHDRAW",
                )
            # VIB-3964 (G6 closer): FIFO-match the SUPPLY lots and split
            # principal vs interest the same way REPAY does. Trust the
            # matched ``interest_or_yield`` only when the FIFO match was
            # either fully principal-covered OR the implied interest is
            # bounded by consumed principal — see the lending_handler
            # counterpart for the full reasoning (Codex 2026-05-04 P2).
            _supply_match = basis_store.match_repay(
                deployment_id=deployment_id,
                position_key=f"supply:{position_key}",
                token=asset,
                repay_amount=amount_human,
            )
            if _supply_match.unmatched_amount > 0:
                principal_delta_usd = _withdraw_total_usd
                interest_delta_usd = None
            elif (
                _supply_match.repaid_principal >= amount_human
                or _supply_match.interest_or_yield <= _supply_match.repaid_principal
            ):
                principal_delta_usd = _amount_to_usd(_supply_match.repaid_principal, price_oracle, asset)
                interest_delta_usd = _amount_to_usd(_supply_match.interest_or_yield, price_oracle, asset)
            else:
                principal_delta_usd = _withdraw_total_usd
                interest_delta_usd = None

    # ── After-state: on-chain read ───────────────────────────────────────────
    # VIB-4929 PR-3a/3b: Aave V3 + Morpho Blue + Compound V3 share the unified
    # ``LendingAccountState``, read through the single generic
    # ``read_lending_account_state`` — but only for protocols explicitly enabled +
    # fork/byte-equivalence-verified in ``_GENERIC_PRE_STATE_PROTOCOLS``. A
    # spec-capable-but-unverified connector (e.g. Spark, which opted into
    # ``_ACCOUNT_STATE_LOADERS``) is NOT auto-read here (→ ESTIMATED; VIB-4963).
    from almanak.connectors._strategy_base.lending_read_registry import LendingReadRegistry

    generic_state: LendingAccountState | None = None
    morpho_unavailable_reason: str = ""

    is_morpho = protocol.lower() == "morpho_blue"

    # VIB-4964: pin the after-state read to the confirmed receipt's block,
    # mirroring the runner's ``capture_lending_post_state`` path (VIB-4589/F7).
    # The legacy ``block=None`` → ``"latest"`` read is wrong in two ways: in a
    # replay reprocessing it reads *present-day* state for a *historical* event,
    # and live it races the upstream RPC's receipt indexer. Reuses the canonical
    # receipt-block extractor (single source of truth; same lazy cross-module
    # reuse the runner's ``teardown_commit`` already relies on — VIB-4987 tracks
    # relocating both helpers to a shared module to retire the accounting→runner
    # edge). ``None`` (no receipt available) falls back to ``"latest"``,
    # preserving prior behaviour.
    # Indexer lag at the pinned block is absorbed by the gateway RPC retry
    # (VIB-4985 / ALM-2777), so the pin no longer trades stale-data for a
    # missing read.
    from almanak.framework.runner.strategy_runner import _last_receipt_block

    post_block = _last_receipt_block(result)
    if gateway_client is not None and protocol.lower() in _GENERIC_PRE_STATE_PROTOCOLS:
        query_inputs = LendingReadRegistry.query_inputs(protocol, intent)
        if query_inputs is not None and (
            not is_morpho or intent_type_str in ("BORROW", "REPAY", "DELEVERAGE", "SUPPLY", "WITHDRAW")
        ):
            # Morpho post-state HF persistence covers all lending intent types so
            # post-state is in parity with pre-state (VIB-4432). market_id is
            # required for Morpho — surface the same diagnostic as the pre-PR path.
            if is_morpho and not market_id:
                morpho_unavailable_reason = "market_id missing from intent — cannot read Morpho Blue position"
                logger.debug("read_lending_account_state skipped: %s", morpho_unavailable_reason)
            else:
                generic_state = read_lending_account_state(
                    protocol=protocol,
                    chain=chain,
                    wallet_address=wallet_address,
                    gateway_client=gateway_client,
                    price_oracle=price_oracle,
                    block=post_block,
                    **query_inputs,
                )
                # Aave-family intent-metadata overlay (interest_rate_mode), gated on
                # the structural family discriminator — parity with the pre-state arm.
                if generic_state is not None and generic_state.family == "aave":
                    generic_state = _overlay_aave_interest_rate_mode(generic_state, intent)
                if generic_state is None and is_morpho:
                    morpho_unavailable_reason = "Morpho Blue position/market gateway read failed"

    # ── Unify after-state fields from whichever protocol provided data ────────
    # Single generic arm now (VIB-4929 PR-3b retired the per-protocol Compound V3
    # branch): Aave / Morpho / Compound V3 all surface as ``LendingAccountState``.
    got_after_state = generic_state is not None

    if generic_state is not None:
        # Single field extraction off the unified ``LendingAccountState`` — no
        # per-protocol ``isinstance`` priority chain (VIB-4929 PR-3a/3b). The
        # protocol-shape differences are carried structurally on the state:
        #   * Aave family: ``liquidation_threshold_bps`` set, ``lltv`` None →
        #     ``liquidation_threshold = bps / 10000``.
        #   * Morpho: ``lltv`` set, ``liquidation_threshold_bps`` None → lltv IS
        #     the liquidation threshold (no-debt HF stays the 999999 sentinel;
        #     callers must not treat HF == 999999 as a trigger).
        #   * Compound V3: ``family=None`` and ``lltv=None`` → no single threshold
        #     (per-asset collateral factors folded into HF by the spec reducer).
        collateral_after: Decimal | None = generic_state.collateral_usd
        debt_after: Decimal | None = generic_state.debt_usd
        hf_after: Decimal | None = generic_state.health_factor
        lt_bps: int | None = generic_state.liquidation_threshold_bps
        lltv_after: Decimal | None = generic_state.lltv
        if lt_bps is not None:
            liquidation_threshold: Decimal | None = Decimal(lt_bps) / Decimal("10000")
        elif generic_state.lltv is not None:
            liquidation_threshold = generic_state.lltv  # LLTV serves as liquidation_threshold
        else:
            liquidation_threshold = None
    else:
        collateral_after = None
        debt_after = None
        hf_after = None
        lt_bps = None
        liquidation_threshold = None
        lltv_after = None

    net_equity_after = (
        (collateral_after - debt_after) if (collateral_after is not None and debt_after is not None) else None
    )

    # ── Before-state: from pre_execution_state (VIB-3489) ────────────────────
    # pre_execution_state is captured by the runner BEFORE the tx is submitted.
    # If None (read failed or not available), before fields stay None — honest
    # absence is preferred over stale data. Absence is signaled by before fields
    # being None; it does NOT affect unavailable_reason (which tracks after-state
    # quality) or confidence.
    collateral_before: Decimal | None = None
    debt_before: Decimal | None = None
    hf_before: Decimal | None = None
    net_equity_before: Decimal | None = None

    if pre_execution_state is not None:
        # Every protocol surfaces as the unified ``LendingAccountState`` (VIB-4929
        # PR-3a/3b), so the common fields read off directly — no protocol-specific
        # branching needed.
        collateral_before = pre_execution_state.collateral_usd
        debt_before = pre_execution_state.debt_usd
        hf_before = pre_execution_state.health_factor
        if collateral_before is not None and debt_before is not None:
            net_equity_before = collateral_before - debt_before

    # Confidence: HIGH if we got a live after-state read, ESTIMATED otherwise.
    # unavailable_reason tracks the primary (after-state) signal only — callers
    # interpret confidence + unavailable_reason as a pair. Pre-state absence is
    # already observable via the before fields being None; polluting
    # unavailable_reason with it would degrade HIGH-confidence events when
    # pre-state was simply not yet available on this cycle.
    confidence = AccountingConfidence.HIGH if got_after_state else AccountingConfidence.ESTIMATED
    if not got_after_state:
        if is_morpho and morpho_unavailable_reason:
            unavailable_reason = morpho_unavailable_reason
        else:
            unavailable_reason = "post-execution on-chain read unavailable"
    else:
        unavailable_reason = ""

    # ── DELEVERAGE enrichment (VIB-3490) ─────────────────────────────────────
    # For DELEVERAGE events, persist the observed HF as health_factor_before
    # (pre-trigger snapshot) so analytics can reconstruct the risk state at the
    # moment the deleverage was triggered without needing a separate pre-read.
    #
    # Trigger metadata (trigger_reason, observed_hf, target_hf) is appended to
    # unavailable_reason ONLY when the event is already estimated/degraded (i.e.
    # got_after_state is False). When confidence is HIGH the deleverage context
    # is emitted as a debug log only — it must not overwrite an empty
    # unavailable_reason, as that would incorrectly signal data degradation to
    # downstream consumers.
    hf_before_from_intent: Decimal | None = None  # populated below for DELEVERAGE only
    if intent_type_str == "DELEVERAGE":
        trigger_reason = getattr(intent, "trigger_reason", "") or ""
        observed_hf_intent = getattr(intent, "observed_hf", None)
        target_hf_intent = getattr(intent, "target_hf", None)

        # Persist the observed HF as health_factor_before (pre-trigger snapshot).
        if observed_hf_intent is not None:
            try:
                hf_before_from_intent = Decimal(str(observed_hf_intent))
            except (ValueError, TypeError, InvalidOperation):
                pass

        # Build trigger context string for logging / degraded-event annotation.
        parts: list[str] = []
        if trigger_reason:
            parts.append(f"DELEVERAGE: {trigger_reason}")
        else:
            parts.append("DELEVERAGE: emergency-triggered")
        if observed_hf_intent is not None:
            parts.append(f"observed_hf={observed_hf_intent}")
        if target_hf_intent is not None:
            parts.append(f"target_hf={target_hf_intent}")
        deleverage_context = "; ".join(parts)

        if unavailable_reason:
            # Event is already degraded/estimated — safe to append trigger context.
            unavailable_reason = f"{deleverage_context} | {unavailable_reason}"

        logger.debug(
            "DELEVERAGE accounting event enriched: %s (position=%s, confidence=%s)",
            deleverage_context,
            position_key,
            confidence.value,
        )

    _id_seed = tx_hash or ledger_entry_id or position_key
    identity = AccountingIdentity(
        id=make_accounting_event_id(deployment_id, cycle_id, intent_type_str, _id_seed, position_key),
        deployment_id=deployment_id,
        cycle_id=cycle_id,
        execution_mode=execution_mode,
        timestamp=now,
        chain=chain,
        protocol=protocol,
        wallet_address=wallet_address,
        tx_hash=tx_hash,
        ledger_entry_id=ledger_entry_id or "",
    )

    return LendingAccountingEvent(
        identity=identity,
        event_type=lending_event_type,
        position_key=position_key,
        market_id=market_id or "",
        asset=asset,
        collateral_value_before_usd=collateral_before,
        collateral_value_after_usd=collateral_after,
        debt_value_before_usd=debt_before,
        debt_value_after_usd=debt_after,
        net_equity_before_usd=net_equity_before,
        net_equity_after_usd=net_equity_after,
        # For DELEVERAGE intents: prefer the observed_hf from the intent (the exact HF
        # at the moment the strategy triggered the deleverage) over the pre-execution
        # gateway read. For all other intent types use the pre-execution state read.
        health_factor_before=hf_before_from_intent if hf_before_from_intent is not None else hf_before,
        health_factor_after=hf_after,
        liquidation_threshold=liquidation_threshold,
        lltv=lltv_after,
        supply_apr_bps=supply_apr_bps,
        borrow_apr_bps=borrow_apr_bps,
        principal_delta_usd=principal_delta_usd,
        interest_delta_usd=interest_delta_usd,
        gas_usd=gas_usd,
        amount_token=amount_human,
        confidence=confidence,
        unavailable_reason=unavailable_reason,
    )
