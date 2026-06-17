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
# VIB-4966: ``euler_v2`` joined — a BESPOKE vault/EVC reader (Euler V2 has no
# Aave-style ``getUserAccountData``; its independent ERC-4626 vaults are read via
# ``maxWithdraw`` on the deposit vault + ``debtOf`` on the borrow/controller vault,
# both protocol-computed single eth_calls). Not USD-native (priced via the injected
# valuation seam, like Compound/Morpho/Silo). Fork-verified on ethereum + avalanche
# by the Layer-5 Euler intent tests (HIGH-confidence before/after collateral / debt /
# HF on the SUPPLY/WITHDRAW path).
# VIB-4967: ``benqi`` joined — a BESPOKE Compound-V2 qiToken reader (BENQI is a
# Compound-V2 fork, NOT an Aave fork; it has no ``getUserAccountData``). The per-asset
# position is read via ``getAccountSnapshot`` on the collateral + debt qiTokens, and
# the HF is a TRUE liquidation-aware ``(collateral_usd × collateralFactor) / debt_usd``
# using the Comptroller's ``markets(qiToken).collateralFactorMantissa`` (the on-chain
# liquidation parameter — NOT a bare collateral/debt proxy). Not USD-native (priced via
# the injected valuation seam, like Compound/Morpho/Silo/Euler). Fork-verified on
# avalanche by the Layer-5 BENQI intent tests (HIGH-confidence before/after collateral /
# debt / HF). See benqi/lending_read.py.
# VIB-5030: ``fluid`` joined — a market-scoped ERC-4626 fToken reader (share
# balance × convertToAssets probe; single supply leg, debt is a measured zero,
# health_factor None — no liquidation surface on a pure supply). Not USD-native
# (priced via the injected valuation seam, like Compound/Morpho/Silo/Euler).
# Fork-verified on base + arbitrum by the Layer-5 ``test_fluid_lending`` intent
# tests (HIGH-confidence before/after collateral on the SUPPLY/WITHDRAW path).
# See fluid/lending_read.py.
# VIB-5031: ``fluid_vault`` joined — the Fluid NFT-CDP vault surface (its own
# protocol key / manifest; ``market_id`` = the vault address). A bespoke
# market-scoped ``positionsByUser`` reader: collateral AND debt legs, with the
# health factor computed PROTOCOL-TRUTH from the vault's own oracle data (the
# ratio liquidation actually keys on); USD legs priced via the injected
# valuation seam (like Compound/Morpho/Silo/Euler — not USD-native).
# Fork-verified on arbitrum + base by the Layer-5 ``test_fluid_vault_lending``
# intent tests (HIGH-confidence before/after collateral / debt / HF across the
# open/borrow/repay/withdraw/close lifecycle). See fluid/vault_lending_read.py.
_GENERIC_PRE_STATE_PROTOCOLS: frozenset[str] = frozenset(
    {"aave_v3", "aave", "morpho_blue", "compound_v3", "spark", "silo_v2", "euler_v2", "benqi", "fluid", "fluid_vault"}
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

    # Canonicalize lending-scoped protocol aliases at the accounting boundary
    # (VIB-5030: the platform spec emits ``protocol: "fluid_lending"``, which
    # must resolve to the connector's canonical ``fluid`` key BEFORE the
    # ``_GENERIC_PRE_STATE_PROTOCOLS`` gate — otherwise alias-spelled intents
    # silently degrade to ESTIMATED). ``normalize_protocol`` folds case /
    # hyphens and applies manifest-declared ``LendingReadDecl.aliases``;
    # unknown spellings pass through folded and fail the gate closed.
    protocol = LendingReadRegistry.normalize_protocol(getattr(intent, "protocol", None))

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
    """Canonical position key for a lending position.

    ``protocol`` is canonicalized through the manifest-declared lending
    aliases (``LendingReadRegistry.normalize_protocol``) so an alias-spelled
    intent (e.g. the platform spec's ``"fluid_lending"``) derives the SAME
    key as the canonical spelling (``lending:{chain}:fluid:...``). Every
    caller (this module's event builder, the runner's outbox derivation,
    and ``category_handlers/lending_handler``) flows through here, so the
    normalization lives inside the deriver — keys can never diverge by
    call site. Unknown spellings fold (lower / hyphen→underscore) and pass
    through, preserving the legacy ``protocol.lower()`` behaviour.
    """
    from almanak.connectors._strategy_base.lending_read_registry import LendingReadRegistry

    canonical_protocol = LendingReadRegistry.normalize_protocol(protocol) or str(protocol).lower()
    parts = ["lending", chain.lower(), canonical_protocol, wallet.lower()]
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


@dataclasses.dataclass(frozen=True)
class _LendingEventContext:
    intent_type_str: str
    lending_event_type: Any
    now: datetime
    protocol: str
    asset: str
    market_id: str | None
    position_key: str
    extracted: dict[str, Any]
    tx_hash: str
    id_seed: str
    is_morpho: bool
    swap_wallet_key: str


@dataclasses.dataclass(frozen=True)
class _LendingExecutionAmounts:
    amount_human: Decimal | None
    supply_apr_bps: int | None
    borrow_apr_bps: int | None
    gas_usd: Decimal | None


@dataclasses.dataclass(frozen=True)
class _LendingDeltas:
    principal_delta_usd: Decimal | None = None
    interest_delta_usd: Decimal | None = None


@dataclasses.dataclass(frozen=True)
class _PostStateRead:
    state: LendingAccountState | None
    unavailable_reason: str = ""


@dataclasses.dataclass(frozen=True)
class _LendingStateFields:
    collateral_usd: Decimal | None = None
    debt_usd: Decimal | None = None
    health_factor: Decimal | None = None
    net_equity_usd: Decimal | None = None
    liquidation_threshold: Decimal | None = None
    lltv: Decimal | None = None


@dataclasses.dataclass(frozen=True)
class _ConfidenceFields:
    confidence: Any
    unavailable_reason: str


@dataclasses.dataclass(frozen=True)
class _DeleverageFields:
    health_factor_before_override: Decimal | None
    unavailable_reason: str


def _resolve_lending_event_context(
    *,
    intent: Any,
    result: Any,
    chain: str,
    wallet_address: str,
    ledger_entry_id: str | None,
) -> _LendingEventContext | None:
    intent_type_str = _intent_type_value(intent)
    if intent_type_str not in _LENDING_INTENT_TYPES:
        return None

    lending_event_type = _to_lending_event_type(intent_type_str)
    if lending_event_type is None:
        return None

    from almanak.connectors._strategy_base.lending_read_registry import LendingReadRegistry

    raw_protocol = getattr(intent, "protocol", "") or ""
    protocol = LendingReadRegistry.normalize_protocol(raw_protocol) or str(raw_protocol)
    asset = _intent_asset(intent)
    market_id = _intent_market_id(intent)
    position_key = _derive_position_key(protocol, chain, wallet_address, market_id, asset)
    tx_hash = getattr(result, "tx_hash", None) or ""

    chain_norm = chain.lower().strip() if chain else ""
    wallet_norm = wallet_address.lower().strip() if wallet_address else ""
    return _LendingEventContext(
        intent_type_str=intent_type_str,
        lending_event_type=lending_event_type,
        now=datetime.now(UTC),
        protocol=protocol,
        asset=asset,
        market_id=market_id,
        position_key=position_key,
        extracted=getattr(result, "extracted_data", None) or {},
        tx_hash=tx_hash,
        id_seed=tx_hash or ledger_entry_id or position_key,
        is_morpho=protocol.lower() == "morpho_blue",
        swap_wallet_key=f"swap:{chain_norm}:{wallet_norm}" if chain_norm and wallet_norm else "",
    )


def _resolve_lending_amount_human(
    *,
    extracted: dict[str, Any],
    asset: str,
    chain: str,
) -> Decimal | None:
    raw_amount = _select_lending_raw_amount(extracted)
    if raw_amount is None:
        return None
    try:
        from almanak.framework.data.tokens.resolver import get_token_resolver

        resolver = get_token_resolver()
        token_info = resolver.resolve(asset, chain=chain)
        if token_info is None:
            logger.debug("token resolution returned None for %s on %s, skipping amount", asset, chain)
            return None
        return Decimal(str(raw_amount)) / Decimal(10**token_info.decimals)
    except Exception:
        logger.debug("token decimal resolution failed for %s, skipping amount conversion", asset)
        return None


def _resolve_lending_gas_usd(result: Any, chain: str, price_oracle: dict | None) -> Decimal | None:
    gas_cost_wei = getattr(result, "total_gas_cost_wei", None)
    gas_cost_native: Decimal | None = None
    if gas_cost_wei is not None and gas_cost_wei > 0:
        try:
            gas_cost_native = Decimal(str(gas_cost_wei)) / Decimal(10**18)
        except Exception:
            pass
    native_token = native_token_for_chain(chain)
    return _amount_to_usd(gas_cost_native, price_oracle, native_token)


def _resolve_lending_execution_amounts(
    *,
    context: _LendingEventContext,
    result: Any,
    chain: str,
    price_oracle: dict | None,
) -> _LendingExecutionAmounts:
    return _LendingExecutionAmounts(
        amount_human=_resolve_lending_amount_human(
            extracted=context.extracted,
            asset=context.asset,
            chain=chain,
        ),
        supply_apr_bps=_ray_to_bps(context.extracted.get("supply_rate")),
        borrow_apr_bps=_ray_to_bps(context.extracted.get("borrow_rate")),
        gas_usd=_resolve_lending_gas_usd(result, chain, price_oracle),
    )


def _apply_borrow_basis_effects(
    *,
    context: _LendingEventContext,
    amount_human: Decimal,
    deployment_id: str,
    cycle_id: str,
    ledger_entry_id: str | None,
    basis_store: FIFOBasisStore,
    price_oracle: dict | None,
) -> _LendingDeltas:
    principal_delta_usd = _amount_to_usd(amount_human, price_oracle, context.asset)
    basis_store.record_borrow(
        deployment_id=deployment_id,
        position_key=context.position_key,
        token=context.asset,
        principal_amount=amount_human,
        principal_usd=principal_delta_usd,
        timestamp=context.now,
        lot_id=make_accounting_event_id(deployment_id, cycle_id, "BORROW_LOT", context.id_seed, context.position_key),
        source_ledger_entry_id=ledger_entry_id,
    )
    if context.swap_wallet_key:
        basis_store.record_swap_acquisition(
            deployment_id=deployment_id,
            position_key=context.swap_wallet_key,
            token=context.asset,
            amount=amount_human,
            cost_usd=principal_delta_usd,
            timestamp=context.now,
            lot_id=make_accounting_event_id(
                deployment_id, cycle_id, "BORROW_WALLET_LOT", context.id_seed, context.asset
            ),
            source="BORROW",
        )
    return _LendingDeltas(principal_delta_usd=principal_delta_usd)


def _apply_repay_like_basis_effects(
    *,
    context: _LendingEventContext,
    amount_human: Decimal,
    deployment_id: str,
    basis_store: FIFOBasisStore,
    price_oracle: dict | None,
) -> _LendingDeltas:
    match_result = basis_store.match_repay(
        deployment_id=deployment_id,
        position_key=context.position_key,
        token=context.asset,
        repay_amount=amount_human,
    )
    principal_delta_usd = _amount_to_usd(match_result.repaid_principal, price_oracle, context.asset)
    if match_result.unmatched_amount > 0:
        logger.debug(
            "%s unmatched for %s: unmatched=%.6f (no BORROW lots recorded)",
            context.intent_type_str,
            context.position_key,
            match_result.unmatched_amount,
        )
        interest_delta_usd = None
    else:
        interest_delta_usd = _amount_to_usd(match_result.interest_or_yield, price_oracle, context.asset)

    if context.swap_wallet_key:
        basis_store.match_swap_disposal(
            deployment_id=deployment_id,
            position_key=context.swap_wallet_key,
            token=context.asset,
            amount=amount_human,
        )
    return _LendingDeltas(principal_delta_usd=principal_delta_usd, interest_delta_usd=interest_delta_usd)


def _apply_supply_basis_effects(
    *,
    context: _LendingEventContext,
    amount_human: Decimal,
    deployment_id: str,
    cycle_id: str,
    ledger_entry_id: str | None,
    basis_store: FIFOBasisStore,
    price_oracle: dict | None,
) -> _LendingDeltas:
    principal_delta_usd = _amount_to_usd(amount_human, price_oracle, context.asset)
    if context.swap_wallet_key:
        basis_store.match_swap_disposal(
            deployment_id=deployment_id,
            position_key=context.swap_wallet_key,
            token=context.asset,
            amount=amount_human,
        )

    supply_position_key = f"supply:{context.position_key}"
    basis_store.record_borrow(
        deployment_id=deployment_id,
        position_key=supply_position_key,
        token=context.asset,
        principal_amount=amount_human,
        principal_usd=principal_delta_usd,
        timestamp=context.now,
        lot_id=make_accounting_event_id(deployment_id, cycle_id, "SUPPLY_LOT", context.id_seed, supply_position_key),
        source_ledger_entry_id=ledger_entry_id,
    )
    return _LendingDeltas(principal_delta_usd=principal_delta_usd)


def _is_trustworthy_withdraw_supply_match(match_result: Any, amount_human: Decimal) -> bool:
    return match_result.unmatched_amount <= 0 and (
        match_result.repaid_principal >= amount_human or match_result.interest_or_yield <= match_result.repaid_principal
    )


def _apply_withdraw_basis_effects(
    *,
    context: _LendingEventContext,
    amount_human: Decimal,
    deployment_id: str,
    cycle_id: str,
    basis_store: FIFOBasisStore,
    price_oracle: dict | None,
) -> _LendingDeltas:
    withdraw_total_usd = _amount_to_usd(amount_human, price_oracle, context.asset)
    if context.swap_wallet_key:
        basis_store.record_swap_acquisition(
            deployment_id=deployment_id,
            position_key=context.swap_wallet_key,
            token=context.asset,
            amount=amount_human,
            cost_usd=withdraw_total_usd,
            timestamp=context.now,
            lot_id=make_accounting_event_id(
                deployment_id, cycle_id, "WITHDRAW_WALLET_LOT", context.id_seed, context.asset
            ),
            source="WITHDRAW",
        )

    supply_position_key = f"supply:{context.position_key}"
    supply_match = basis_store.match_repay(
        deployment_id=deployment_id,
        position_key=supply_position_key,
        token=context.asset,
        repay_amount=amount_human,
    )
    if _is_trustworthy_withdraw_supply_match(supply_match, amount_human):
        return _LendingDeltas(
            principal_delta_usd=_amount_to_usd(supply_match.repaid_principal, price_oracle, context.asset),
            interest_delta_usd=_amount_to_usd(supply_match.interest_or_yield, price_oracle, context.asset),
        )
    return _LendingDeltas(principal_delta_usd=withdraw_total_usd)


def _apply_lending_basis_effects(
    *,
    context: _LendingEventContext,
    amounts: _LendingExecutionAmounts,
    deployment_id: str,
    cycle_id: str,
    ledger_entry_id: str | None,
    basis_store: FIFOBasisStore,
    price_oracle: dict | None,
) -> _LendingDeltas:
    amount_human = amounts.amount_human
    if amount_human is None:
        return _LendingDeltas()

    if context.intent_type_str == "BORROW":
        return _apply_borrow_basis_effects(
            context=context,
            amount_human=amount_human,
            deployment_id=deployment_id,
            cycle_id=cycle_id,
            ledger_entry_id=ledger_entry_id,
            basis_store=basis_store,
            price_oracle=price_oracle,
        )
    if context.intent_type_str in ("REPAY", "DELEVERAGE"):
        return _apply_repay_like_basis_effects(
            context=context,
            amount_human=amount_human,
            deployment_id=deployment_id,
            basis_store=basis_store,
            price_oracle=price_oracle,
        )
    if context.intent_type_str == "SUPPLY":
        return _apply_supply_basis_effects(
            context=context,
            amount_human=amount_human,
            deployment_id=deployment_id,
            cycle_id=cycle_id,
            ledger_entry_id=ledger_entry_id,
            basis_store=basis_store,
            price_oracle=price_oracle,
        )
    if context.intent_type_str == "WITHDRAW":
        return _apply_withdraw_basis_effects(
            context=context,
            amount_human=amount_human,
            deployment_id=deployment_id,
            cycle_id=cycle_id,
            basis_store=basis_store,
            price_oracle=price_oracle,
        )
    return _LendingDeltas()


def _read_lending_post_state_for_event(
    *,
    context: _LendingEventContext,
    intent: Any,
    result: Any,
    chain: str,
    wallet_address: str,
    gateway_client: Any | None,
    price_oracle: dict | None,
) -> _PostStateRead:
    if gateway_client is None or context.protocol.lower() not in _GENERIC_PRE_STATE_PROTOCOLS:
        return _PostStateRead(state=None)

    from almanak.connectors._strategy_base.lending_read_registry import LendingReadRegistry
    from almanak.framework.runner.strategy_runner import _last_receipt_block

    query_inputs = LendingReadRegistry.query_inputs(context.protocol, intent)
    if query_inputs is None:
        return _PostStateRead(state=None)
    if context.is_morpho and not context.market_id:
        reason = "market_id missing from intent — cannot read Morpho Blue position"
        logger.debug("read_lending_account_state skipped: %s", reason)
        return _PostStateRead(state=None, unavailable_reason=reason)

    state = read_lending_account_state(
        protocol=context.protocol,
        chain=chain,
        wallet_address=wallet_address,
        gateway_client=gateway_client,
        price_oracle=price_oracle,
        block=_last_receipt_block(result),
        **query_inputs,
    )
    if state is not None and state.family == "aave":
        state = _overlay_aave_interest_rate_mode(state, intent)
    if state is None and context.is_morpho:
        return _PostStateRead(state=None, unavailable_reason="Morpho Blue position/market gateway read failed")
    return _PostStateRead(state=state)


def _state_fields_from_lending_state(
    state: LendingAccountState | None,
    *,
    include_threshold: bool,
) -> _LendingStateFields:
    if state is None:
        return _LendingStateFields()

    collateral_usd = state.collateral_usd
    debt_usd = state.debt_usd
    net_equity_usd = (collateral_usd - debt_usd) if (collateral_usd is not None and debt_usd is not None) else None
    liquidation_threshold: Decimal | None = None
    if include_threshold:
        if state.liquidation_threshold_bps is not None:
            liquidation_threshold = Decimal(state.liquidation_threshold_bps) / Decimal("10000")
        elif state.lltv is not None:
            liquidation_threshold = state.lltv

    return _LendingStateFields(
        collateral_usd=collateral_usd,
        debt_usd=debt_usd,
        health_factor=state.health_factor,
        net_equity_usd=net_equity_usd,
        liquidation_threshold=liquidation_threshold,
        lltv=state.lltv if include_threshold else None,
    )


def _confidence_from_post_state(context: _LendingEventContext, post_state: _PostStateRead) -> _ConfidenceFields:
    from almanak.framework.accounting.models import AccountingConfidence

    if post_state.state is not None:
        return _ConfidenceFields(confidence=AccountingConfidence.HIGH, unavailable_reason="")
    if context.is_morpho and post_state.unavailable_reason:
        return _ConfidenceFields(
            confidence=AccountingConfidence.ESTIMATED, unavailable_reason=post_state.unavailable_reason
        )
    return _ConfidenceFields(
        confidence=AccountingConfidence.ESTIMATED,
        unavailable_reason="post-execution on-chain read unavailable",
    )


def _apply_deleverage_enrichment(
    *,
    context: _LendingEventContext,
    intent: Any,
    confidence: Any,
    unavailable_reason: str,
) -> _DeleverageFields:
    if context.intent_type_str != "DELEVERAGE":
        return _DeleverageFields(health_factor_before_override=None, unavailable_reason=unavailable_reason)

    observed_hf_intent = getattr(intent, "observed_hf", None)
    hf_before_from_intent: Decimal | None = None
    if observed_hf_intent is not None:
        try:
            hf_before_from_intent = Decimal(str(observed_hf_intent))
        except (ValueError, TypeError, InvalidOperation):
            pass

    trigger_reason = getattr(intent, "trigger_reason", "") or ""
    target_hf_intent = getattr(intent, "target_hf", None)
    parts = [f"DELEVERAGE: {trigger_reason}" if trigger_reason else "DELEVERAGE: emergency-triggered"]
    if observed_hf_intent is not None:
        parts.append(f"observed_hf={observed_hf_intent}")
    if target_hf_intent is not None:
        parts.append(f"target_hf={target_hf_intent}")
    deleverage_context = "; ".join(parts)

    if unavailable_reason:
        unavailable_reason = f"{deleverage_context} | {unavailable_reason}"

    logger.debug(
        "DELEVERAGE accounting event enriched: %s (position=%s, confidence=%s)",
        deleverage_context,
        context.position_key,
        confidence.value,
    )
    return _DeleverageFields(
        health_factor_before_override=hf_before_from_intent,
        unavailable_reason=unavailable_reason,
    )


def _build_lending_identity(
    *,
    context: _LendingEventContext,
    deployment_id: str,
    cycle_id: str,
    execution_mode: str,
    chain: str,
    wallet_address: str,
    ledger_entry_id: str | None,
) -> Any:
    from almanak.framework.accounting.models import AccountingIdentity

    return AccountingIdentity(
        id=make_accounting_event_id(
            deployment_id,
            cycle_id,
            context.intent_type_str,
            context.id_seed,
            context.position_key,
        ),
        deployment_id=deployment_id,
        cycle_id=cycle_id,
        execution_mode=execution_mode,
        timestamp=context.now,
        chain=chain,
        protocol=context.protocol,
        wallet_address=wallet_address,
        tx_hash=context.tx_hash,
        ledger_entry_id=ledger_entry_id or "",
    )


def _build_lending_event(
    *,
    context: _LendingEventContext,
    identity: Any,
    amounts: _LendingExecutionAmounts,
    deltas: _LendingDeltas,
    before: _LendingStateFields,
    after: _LendingStateFields,
    confidence: _ConfidenceFields,
    deleverage: _DeleverageFields,
) -> Any:
    from almanak.framework.accounting.models import LendingAccountingEvent

    health_factor_before = (
        deleverage.health_factor_before_override
        if deleverage.health_factor_before_override is not None
        else before.health_factor
    )
    return LendingAccountingEvent(
        identity=identity,
        event_type=context.lending_event_type,
        position_key=context.position_key,
        market_id=context.market_id or "",
        asset=context.asset,
        collateral_value_before_usd=before.collateral_usd,
        collateral_value_after_usd=after.collateral_usd,
        debt_value_before_usd=before.debt_usd,
        debt_value_after_usd=after.debt_usd,
        net_equity_before_usd=before.net_equity_usd,
        net_equity_after_usd=after.net_equity_usd,
        health_factor_before=health_factor_before,
        health_factor_after=after.health_factor,
        liquidation_threshold=after.liquidation_threshold,
        lltv=after.lltv,
        supply_apr_bps=amounts.supply_apr_bps,
        borrow_apr_bps=amounts.borrow_apr_bps,
        principal_delta_usd=deltas.principal_delta_usd,
        interest_delta_usd=deltas.interest_delta_usd,
        gas_usd=amounts.gas_usd,
        amount_token=amounts.amount_human,
        confidence=confidence.confidence,
        unavailable_reason=deleverage.unavailable_reason,
    )


def build_lending_accounting_event(
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
    context = _resolve_lending_event_context(
        intent=intent,
        result=result,
        chain=chain,
        wallet_address=wallet_address,
        ledger_entry_id=ledger_entry_id,
    )
    if context is None:
        return None

    amounts = _resolve_lending_execution_amounts(
        context=context,
        result=result,
        chain=chain,
        price_oracle=price_oracle,
    )
    deltas = _apply_lending_basis_effects(
        context=context,
        amounts=amounts,
        deployment_id=deployment_id,
        cycle_id=cycle_id,
        ledger_entry_id=ledger_entry_id,
        basis_store=basis_store,
        price_oracle=price_oracle,
    )

    post_state = _read_lending_post_state_for_event(
        context=context,
        intent=intent,
        result=result,
        chain=chain,
        wallet_address=wallet_address,
        gateway_client=gateway_client,
        price_oracle=price_oracle,
    )
    before = _state_fields_from_lending_state(pre_execution_state, include_threshold=False)
    after = _state_fields_from_lending_state(post_state.state, include_threshold=True)

    confidence = _confidence_from_post_state(context, post_state)
    deleverage = _apply_deleverage_enrichment(
        context=context,
        intent=intent,
        confidence=confidence.confidence,
        unavailable_reason=confidence.unavailable_reason,
    )
    identity = _build_lending_identity(
        context=context,
        deployment_id=deployment_id,
        cycle_id=cycle_id,
        execution_mode=execution_mode,
        chain=chain,
        wallet_address=wallet_address,
        ledger_entry_id=ledger_entry_id,
    )
    return _build_lending_event(
        context=context,
        identity=identity,
        amounts=amounts,
        deltas=deltas,
        before=before,
        after=after,
        confidence=confidence,
        deleverage=deleverage,
    )
