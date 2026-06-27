"""VIB-5011 — framework-owned token-consolidation planner (teardown Phase 2).

Mainnet incident this closes: ``teardown request -a target -t USDC`` against an
open WETH/USDC LP closed the LP but planned **no** consolidation swap — the
wallet kept 0.011 WETH (~$18, 65% of capital). The three-phase pipeline
existed only as data models (``TeardownPhase.TOKEN_CONSOLIDATION``,
``TokenConsolidationConfig``, ``TeardownRequest.asset_policy``/``target_token``)
while the plan was exactly ``strategy.generate_teardown_intents(...)``.

This module is the **pure planner** for Phase 2. It decides which residual
wallet tokens to swap into the consolidation target AFTER position closure +
verification succeed. Execution stays in
``TeardownManager.run_token_consolidation`` which reuses ``_execute_intents``
(slippage-escalation ladder, per-intent ``runner_helpers.commit`` pairing,
zero-balance skips, resume-safe progress) — the planner never touches the
orchestrator.

Key design points (blueprint 14 §Token Consolidation):

* **Strategy-scoped token SELECTION, wallet-scoped AMOUNTS.** Which tokens
  are candidates is restricted to the strategy's universe — the union of
  (1) tokens referenced by this run's closing intents and
  ``PositionInfo.details`` (token0/token1/asset), (2) the strategy's
  accounting-event token footprint (shared scan with the sweep DX warning —
  :func:`almanak.framework.teardown.sweep_warning.extract_token_footprint`),
  and (3) ``get_teardown_profile().natural_exit_assets`` — so a token the
  strategy never touched is never swapped. The AMOUNT per token, however,
  is the full wallet balance (``amount="all"``): on a wallet shared across
  deployments that includes sibling strategies' balances of the SAME token
  (per-strategy attribution of fungible wallet balances is not reliably
  derivable — see the VIB-4976 adjudication). Mitigations: the phase only
  runs on an explicit operator ``TeardownRequest`` (its asset policy is the
  consent — the same model as the long-standing strategy-emitted
  ``amount="all"`` sweeps, VIB-4587); each run surfaces a wallet-scope
  warning on the result; ``keep_tokens`` / ``asset_policy=keep`` /
  ``token_consolidation.enabled=false`` opt out.
* **Double-swap safety is structural**: planning happens AFTER closure from
  live wallet balances (``market.balance(token)``); a strategy that already
  swept leaves ~0 residual and the planner emits nothing. "Live" is enforced,
  not assumed (VIB-5074): the snapshot handed to the planner was built BEFORE
  the closing intents executed, and ``MarketSnapshot.balance`` memoizes — so
  the planner evicts the per-token memo (``market.invalidate_balance``)
  before each read, exactly like the execution lane's zero-balance skip
  (``_zero_balance_swap_skip_reason``, PR #2726). A stale memo here reasons
  about tokens that no longer exist (mainnet: WETH "skip below_dust
  value_usd=3.58" logged AFTER the closing swap had already sold that WETH)
  — or, worse, sees a stale zero and strands a real residual (the VIB-5011
  $18-WETH mechanism).
* **HARD (emergency) mode appends no swaps** — speed-first exits skip the
  phase with a loud warning.
* **Never guess a trade**: ``entry_token`` policy degrades to "no
  consolidation + loud warning" when the entry asset is undiscoverable.
* The chain's native gas symbol is never swapped (wrapped native like WETH
  IS swappable — it's the incident token).
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field, replace
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any, Literal

from almanak.framework.teardown.models import TeardownAssetPolicy, TeardownMode
from almanak.framework.teardown.sweep_warning import extract_token_footprint

if TYPE_CHECKING:
    from almanak.framework.teardown.config import TokenConsolidationConfig
    from almanak.framework.teardown.models import TeardownResult

logger = logging.getLogger(__name__)


# Intent / position-detail keys scanned when building the closing-intent /
# position slice of the token universe. Mirrors the accounting-payload scan
# (sweep_warning._PAYLOAD_TOKEN_KEYS) for intents; positions add token0/token1.
_INTENT_TOKEN_KEYS: tuple[str, ...] = ("from_token", "to_token", "token", "asset")
_POSITION_DETAIL_TOKEN_KEYS: tuple[str, ...] = ("token0", "token1", "asset")

# VIB-5393 (Case A): a below-dust residual worth at least this fraction of the
# dust floor is "material" — not negligible dust the operator should ignore —
# so its skip surfaces a result-level warning (and, via the runner, a hosted
# log line). At the default $5 floor this is $1.00. Sized to flag real stranded
# value (e.g. $4 WETH) while staying quiet on true sub-dollar dust.
_MATERIAL_DUST_FRACTION: Decimal = Decimal("1") / Decimal("5")


@dataclass(frozen=True)
class ConsolidationDecision:
    """Per-token audit record of the consolidation planner."""

    token: str
    balance: Decimal
    value_usd: Decimal | None
    action: Literal["swap", "skip"]
    # Examples: "consolidate", "below_dust", "zero_balance", "native_gas",
    # "target", "keep_token", "not_in_universe", "not_a_token", "no_price",
    # "balance_unavailable".
    reason: str


@dataclass(frozen=True)
class ConsolidationPlan:
    """Output of :func:`plan_consolidation` — intents plus the audit trail."""

    intents: list[Any]
    decisions: list[ConsolidationDecision]
    warnings: list[str]


@dataclass(frozen=True)
class ConsolidationOutcome:
    """Execution summary of the token-consolidation phase.

    Returned by ``TeardownManager.run_token_consolidation``. A consolidation
    failure after successful closure keeps the teardown ``success=True`` —
    this outcome carries the partial state that lands in
    ``result_json["consolidation"]`` and the ``TeardownResult.consolidation_*``
    fields.
    """

    planned: int = 0
    succeeded: int = 0
    failed: int = 0
    warnings: list[str] = field(default_factory=list)
    decisions: list[ConsolidationDecision] = field(default_factory=list)
    accounting_degraded_count: int = 0


def _is_consolidatable_symbol(value: str) -> bool:
    """True when *value* is a single swappable token symbol, not a pool label.

    VIB-5393 (Case B): some LP connectors stamp a **pool-pair label** into the
    ``PositionInfo.details`` ``asset``/``token0``/``token1`` slots — TraderJoe
    V2's ``get_open_positions`` sets ``details["asset"] = "WAVAX/USDC"`` (and a
    Uniswap-style pool triple reads ``"WAVAX/USDC/20"``). The universe scan
    treats every ``asset``-keyed string as a swappable token symbol, so the
    pair label leaks in and the planner calls ``market.balance("WAVAX/USDC")``
    — which can never resolve (``Cannot determine balance for
    WAVAX/USDC@avalanche``), producing a ``balance_unavailable`` skip that
    looks like a real residual was stranded.

    A token symbol is a single asset ticker: it never contains a ``/`` (the
    pair/pool separator) or whitespace. Excluding these labels is correct —
    each underlying leg (``WAVAX``, ``USDC``) still enters the universe via the
    **SWAP accounting footprint** (``extract_token_footprint`` reads
    ``token_in`` / ``token_out`` off the entry/rebalance swap events), so the
    real residuals are still consolidated. Note the LP_CLOSE that unwinds the
    position carries **no** leg symbols (``LPCloseIntent`` has only
    ``position_id`` / ``pool``), and the TraderJoe demo stamps only
    ``details["asset"]`` (the pair label) + ``details["pool"]`` — so the legs
    do NOT re-enter via the closing intent or position details, only via the
    footprint. Dropping the label is best-effort, like every other universe
    source: a narrower universe means fewer swaps, never a raise.
    """
    return "/" not in value and not any(ch.isspace() for ch in value)


def _token_from(obj: Any, key: str) -> str | None:
    val = obj.get(key) if isinstance(obj, dict) else getattr(obj, key, None)
    if not (isinstance(val, str) and val):
        return None
    return val if _is_consolidatable_symbol(val) else None


def derive_strategy_token_universe(
    accounting_state_manager: Any,
    deployment_id: str,
    strategy: Any,
    closing_intents: Sequence[Any] | None,
    positions: Any,
) -> set[str]:
    """Build the strategy-scoped token universe (upper-cased symbols).

    Union of four sources — never the full wallet (shared across
    deployments; see module docstring):

    1. Tokens referenced by this run's closing intents and
       ``PositionInfo.details`` keys ``token0`` / ``token1`` / ``asset`` from
       ``get_open_positions()``.
    2. The deployment's accounting-event token footprint via
       ``accounting_state_manager.get_accounting_events_sync(deployment_id)``
       (the **accounting** StateManager — the teardown lifecycle SM does not
       expose this method).
    3. The deployment's NO_ACCOUNTING ledger acquisitions (STAKE→wstETH,
       WRAP→WETH, CDP MINT→stablecoin) via the MEASURED transaction-ledger read
       (``accounting_state_manager.read_ledger_entries_measured``, VIB-5445).
       These primitives write a ``transaction_ledger`` row but ZERO
       ``accounting_events``, so source 2 is blind to them — without this source
       a held NO_ACCOUNTING token is never a consolidation candidate and strands
       at teardown (VIB-5471, generalising the VIB-5416 swap-back-clamp fix to
       the consolidation lane). Measured-gated: an UNMEASURED / absent ledger
       read contributes nothing (the token strands — the safe under-sweep
       direction), never over-selecting on a shared wallet.
    4. ``strategy.get_teardown_profile().natural_exit_assets``.

    Every source is best-effort: failures shrink the universe (fewer
    consolidation swaps) rather than raising.
    """
    universe: set[str] = set()

    for intent in closing_intents or []:
        for key in _INTENT_TOKEN_KEYS:
            val = _token_from(intent, key)
            if val:
                universe.add(val)

    for position in getattr(positions, "positions", None) or []:
        details = getattr(position, "details", None)
        if not isinstance(details, dict):
            continue
        for key in _POSITION_DETAIL_TOKEN_KEYS:
            val = details.get(key)
            if isinstance(val, str) and val:
                universe.add(val)  # filtered below alongside every other source

    if (
        accounting_state_manager is not None
        and deployment_id
        and hasattr(accounting_state_manager, "get_accounting_events_sync")
    ):
        try:
            events = accounting_state_manager.get_accounting_events_sync(deployment_id)
        except Exception:  # noqa: BLE001 — universe derivation must never block the unwind
            logger.debug("token-universe accounting-event read failed for %s", deployment_id, exc_info=True)
            events = []
        universe |= extract_token_footprint(events)

    # Source 3 (VIB-5471): NO_ACCOUNTING ledger acquisitions. STAKE/WRAP/CDP-mint
    # write a transaction_ledger row but ZERO accounting_events, so the source-2
    # footprint cannot see the held token; without this it would never be a
    # consolidation candidate and would strand at teardown. Reuses the SAME
    # measured-gated ledger reader as the swap-back clamp (VIB-5416) so the two
    # fund-safety lanes share one trust gate — an UNMEASURED / absent / old-gateway
    # read drops this lane (adds nothing → the token strands, the safe under-sweep
    # direction; never over-selects a shared wallet).
    if accounting_state_manager is not None and deployment_id:
        from almanak.framework.accounting.basis import no_accounting_ledger_token_footprint

        from .swap_clamp import read_no_accounting_ledger_rows

        ledger_rows = read_no_accounting_ledger_rows(accounting_state_manager, deployment_id)
        universe |= no_accounting_ledger_token_footprint(ledger_rows)

    try:
        profile = strategy.get_teardown_profile()
        for sym in getattr(profile, "natural_exit_assets", None) or []:
            if isinstance(sym, str) and sym:
                universe.add(sym)
    except Exception:  # noqa: BLE001 — profile is UX metadata; never block
        logger.debug("get_teardown_profile failed while deriving token universe", exc_info=True)

    # VIB-5393 (Case B): drop pool-pair labels that any source may have
    # contributed (e.g. PositionInfo.details["asset"]="WAVAX/USDC", or the same
    # label persisted into an accounting-event payload). They are not swappable
    # token symbols; left in, the planner calls market.balance() on them and
    # logs a misleading balance_unavailable skip. Single filter seam so every
    # source — intents, positions, accounting footprint, NO_ACCOUNTING ledger
    # footprint, profile — is covered.
    dropped = {sym for sym in universe if not _is_consolidatable_symbol(sym)}
    if dropped:
        logger.debug("consolidation universe dropped non-token pool labels: %s", sorted(dropped))
    return universe - dropped


def _earliest_swap_entry_token(accounting_events: Sequence[dict] | None) -> str | None:
    """Return the from-token of the deployment's earliest SWAP accounting event.

    ``get_accounting_events_sync`` returns rows ordered ``timestamp ASC``, so
    the first SWAP-shaped row is the earliest. A row qualifies when its
    ``event_type`` mentions SWAP (or its position_key uses the ``swap:`` FIFO
    pool prefix) and its payload carries ``token_in`` / ``from_token``.
    """
    for ev in accounting_events or []:
        if not isinstance(ev, dict):
            continue
        event_type = str(ev.get("event_type") or "").upper()
        position_key = str(ev.get("position_key") or "").lower()
        if "SWAP" not in event_type and not position_key.startswith("swap:"):
            continue
        payload_raw = ev.get("payload_json")
        try:
            payload = json.loads(payload_raw) if isinstance(payload_raw, str) else payload_raw
        except (TypeError, ValueError):
            continue
        if not isinstance(payload, dict):
            continue
        for key in ("token_in", "from_token"):
            val = payload.get(key)
            if isinstance(val, str) and val:
                return val
    return None


def resolve_consolidation_targets(
    asset_policy: TeardownAssetPolicy | str,
    target_token: str | None,
    strategy: Any,
    *,
    accounting_events: Sequence[dict] | None = None,
) -> tuple[set[str] | None, list[str]]:
    """Resolve the consolidation target set for an asset policy.

    Returns ``(targets, warnings)``:

    * ``target_token`` policy → ``{target_token}`` (default USDC).
    * ``entry_token`` policy → ``get_teardown_profile().original_entry_assets``
      when non-empty; fallback = from-token of the deployment's earliest SWAP
      accounting event; undiscoverable → ``(None, [loud warning])`` —
      degrade to no consolidation, never guess a trade.
    * ``keep_outputs`` policy → ``(None, [])`` — phase skipped.
    """
    warnings: list[str] = []
    try:
        policy = TeardownAssetPolicy(asset_policy)
    except ValueError:
        warnings.append(f"unknown asset_policy {asset_policy!r} — skipping token consolidation")
        return None, warnings

    if policy == TeardownAssetPolicy.KEEP_OUTPUTS:
        return None, []

    if policy == TeardownAssetPolicy.TARGET_TOKEN:
        return {target_token or "USDC"}, []

    # ENTRY_TOKEN — profile first, earliest-SWAP fallback second.
    entry_assets: list[str] = []
    try:
        profile = strategy.get_teardown_profile()
        entry_assets = [a for a in (getattr(profile, "original_entry_assets", None) or []) if isinstance(a, str) and a]
    except Exception:  # noqa: BLE001 — profile is best-effort metadata
        logger.debug("get_teardown_profile failed while resolving entry assets", exc_info=True)
    if entry_assets:
        return set(entry_assets), []

    fallback = _earliest_swap_entry_token(accounting_events)
    if fallback:
        return {fallback}, []

    warnings.append(
        "asset_policy=entry_token but no entry asset is discoverable "
        "(empty get_teardown_profile().original_entry_assets and no SWAP "
        "accounting events) — skipping token consolidation; the wallet keeps "
        "its natural exit tokens. Re-run with --asset-policy target to "
        "consolidate explicitly."
    )
    return None, warnings


def _coerce_decimal(value: Any) -> Decimal | None:
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _resolve_plan_target_set(
    *,
    mode: TeardownMode,
    token_consolidation_cfg: TokenConsolidationConfig | None,
    asset_policy: TeardownAssetPolicy | str,
    target_token: str | None,
    targets: set[str] | None,
    warnings: list[str],
) -> dict[str, str] | None:
    """Pre-flight gates for :func:`plan_consolidation`.

    Returns the target mapping ``{UPPER_KEY: original_cased_symbol}``, or
    ``None`` when the phase must be skipped (HARD mode, disabled config,
    keep_outputs, unresolved entry targets). Appends skip explanations to
    *warnings*. Casing matters: comparisons fold to upper, but the original
    symbol feeds ``Intent.swap(to_token=...)`` — canonical registry symbols
    can be mixed-case (``USDC.e``).
    """
    if mode == TeardownMode.HARD:
        warnings.append("emergency_mode: HARD teardown skips token consolidation — speed-first exits append no swaps")
        return None
    if token_consolidation_cfg is not None and not token_consolidation_cfg.enabled:
        return None
    if targets is None:
        try:
            policy = TeardownAssetPolicy(asset_policy)
        except ValueError:
            warnings.append(f"unknown asset_policy {asset_policy!r} — skipping token consolidation")
            return None
        if policy == TeardownAssetPolicy.TARGET_TOKEN:
            targets = {target_token or "USDC"}
        elif policy == TeardownAssetPolicy.ENTRY_TOKEN:
            warnings.append(
                "entry_token policy requires pre-resolved targets "
                "(resolve_consolidation_targets) — skipping token consolidation"
            )
            return None
        else:  # KEEP_OUTPUTS
            return None
    targets_by_upper = {t.upper(): t for t in sorted(targets) if isinstance(t, str) and t}
    return targets_by_upper or None


def _skip(
    token: str, reason: str, *, balance: Decimal | None = None, value_usd: Decimal | None = None
) -> ConsolidationDecision:
    return ConsolidationDecision(
        token=token,
        balance=balance if balance is not None else Decimal("0"),
        value_usd=value_usd,
        action="skip",
        reason=reason,
    )


def _decide_token(
    token: str,
    *,
    market: Any,
    chain: str | None,
    targets_upper: set[str],
    native_symbol: str,
    keep_tokens: set[str],
    min_swap_value: Decimal,
    warnings: list[str],
) -> ConsolidationDecision:
    """Audit decision for one universe token (``action == "swap"`` ⇒ emit).

    *token* keeps its ORIGINAL casing — it feeds ``market.balance()`` /
    ``market.price()`` here and ``Intent.swap(from_token=...)`` in the
    caller; canonical registry symbols can be mixed-case (``USDC.e``).
    Membership comparisons fold to upper.
    """
    token_upper = token.upper()
    if not _is_consolidatable_symbol(token):
        # VIB-5393 (Case B) defense-in-depth: a pool-pair label (e.g.
        # "WAVAX/USDC") is not a swappable token. The universe derivation
        # already filters these, but the planner must never hand one to
        # market.balance() even if a caller passes a raw universe — that read
        # can only fail and emit a misleading balance_unavailable skip.
        return _skip(token, "not_a_token")
    if token_upper in targets_upper:
        return _skip(token, "target")
    if token_upper == native_symbol:
        # Wrapped native (WETH/WBNB/...) is a distinct symbol and IS
        # swappable — only the raw gas token is protected.
        return _skip(token, "native_gas")
    if token_upper in keep_tokens:
        return _skip(token, "keep_token")
    if market is None:
        warnings.append(f"no market snapshot — cannot read {token} balance; skipping")
        return _skip(token, "balance_unavailable")

    # VIB-5074: evict the snapshot-level balance memo BEFORE deciding. This
    # decision pass runs AFTER the closing intents executed against the same
    # snapshot — a memoized pre-closure balance makes the planner reason
    # about tokens the closure already sold (field: WETH "skip below_dust
    # value_usd=3.58" two seconds after the swap that emptied it), or see a
    # stale zero and strand a real residual (the VIB-5011 mechanism). Same
    # eviction the execution lane performs (_zero_balance_swap_skip_reason).
    # Best-effort eviction. If it FAILS we cannot trust the cached balance, so
    # we fail CLOSED: skip this token (balance_unavailable) rather than decide
    # on a possibly-stale cache (VIB-5196). Deciding on the stale value could
    # emit a SWAP for a token the closure already sold — a real money-path
    # action off untrusted data; skipping only strands recoverable dust. Loud
    # (warning + audit trail) but non-blocking, matching teardown's inverted
    # failure semantics. No-op on provider-less (paper) snapshots.
    # NOTE: eviction must stay symbol/protocol-symmetric with the read below —
    # both use the bare symbol (protocol=None). If a future change passes
    # protocol= into market.balance() here, mirror it in the eviction or the
    # protocol-variant memo key survives and re-introduces the stale read.
    invalidate = getattr(market, "invalidate_balance", None)
    if callable(invalidate):
        try:
            invalidate(token)
        except Exception as exc:  # noqa: BLE001 — fail closed: never decide on an un-evicted cache
            warnings.append(
                f"could not refresh {token} balance (invalidate_balance failed: {exc}) — "
                f"skipping consolidation for it (fail-closed; will not swap on a possibly-stale balance)"
            )
            logger.warning(
                "invalidate_balance(%s) failed in consolidation planner; skipping token "
                "(fail-closed, no swap on possibly-stale balance)",
                token,
                exc_info=True,
            )
            return _skip(token, "balance_unavailable")

    try:
        bal = market.balance(token, chain=chain) if chain else market.balance(token)
    except Exception as exc:  # noqa: BLE001 — planner is best-effort per token
        warnings.append(f"could not read {token} balance ({exc}) — skipping consolidation for it")
        return _skip(token, "balance_unavailable")
    balance = _coerce_decimal(bal.balance if hasattr(bal, "balance") else bal)
    if balance is None:
        # Empty ≠ Zero: an unparseable read is UNMEASURED, not a zero balance.
        warnings.append(f"unparseable {token} balance — skipping consolidation for it")
        return _skip(token, "balance_unavailable")
    if balance <= 0:
        # Measured zero — nothing to consolidate. Labelled distinctly from
        # below_dust: a zero balance is not "a residual under the dust
        # floor", it is no residual at all (VIB-5074 secondary defect).
        return _skip(token, "zero_balance", balance=balance, value_usd=Decimal("0"))

    price: Decimal | None
    try:
        raw_price = market.price(token, chain=chain) if chain else market.price(token)
        price = _coerce_decimal(raw_price)
    except Exception:  # noqa: BLE001 — Empty ≠ Zero: unmeasured price → skip, never assume
        price = None
    if price is None or price <= 0:
        warnings.append(f"no price available for {token} — skipping consolidation for it (residual stays in wallet)")
        return _skip(token, "no_price", balance=balance)

    value_usd = balance * price
    if value_usd < min_swap_value:
        # VIB-5393 (Case A): below the dust floor is WORKING AS CONFIGURED —
        # the floor (default $5, VIB-5011) deliberately leaves sub-floor
        # residuals unswapped because the swap gas would eat the proceeds. But
        # a residual that is a MEANINGFUL fraction of the floor (e.g. $4.12 WETH
        # at a $5 floor) is not negligible dust the operator should ignore: on a
        # hosted run with no operator sweep it strands in the strategy wallet.
        # The bare "below_dust" decision is only logged at INFO; surface a
        # result-level WARNING (visible on `teardown status` / `--wait`) so the
        # operator can sweep or lower the floor. The floor itself is unchanged —
        # raising it would just consolidate more gas-uneconomic dust.
        if value_usd >= min_swap_value * _MATERIAL_DUST_FRACTION:
            warnings.append(
                f"{token} residual is ${value_usd:.2f} — below the ${min_swap_value} "
                f"consolidation dust floor so it was NOT swapped to the target token "
                f"and stays in the wallet. On a hosted run with no operator sweep this "
                f"strands. Lower token_consolidation.min_swap_value_usd to consolidate it."
            )
        return _skip(token, "below_dust", balance=balance, value_usd=value_usd)

    return ConsolidationDecision(token=token, balance=balance, value_usd=value_usd, action="swap", reason="consolidate")


def plan_consolidation(
    *,
    market: Any,
    chain: str | None,
    asset_policy: TeardownAssetPolicy | str,
    target_token: str | None,
    token_consolidation_cfg: TokenConsolidationConfig | None,
    token_universe: Iterable[str],
    mode: TeardownMode,
    targets: set[str] | None = None,
    wallet_tokens: Iterable[str] | None = None,
) -> ConsolidationPlan:
    """Plan Phase-2 consolidation swaps from live post-closure balances.

    Pure: reads ``market.balance`` / ``market.price`` only; emits
    ``Intent.swap(from_token=t, to_token=target, amount="all", chain=chain)``
    for every universe token whose residual value clears the dust floor.

    Args:
        targets: Pre-resolved consolidation target set (from
            :func:`resolve_consolidation_targets`). When ``None``, targets are
            resolved here for the ``target_token`` policy only — ``entry_token``
            without pre-resolved targets degrades to an empty plan with a
            warning (the entry lookup needs accounting events the planner
            doesn't hold).
        wallet_tokens: Optional extra wallet symbols for audit records: tokens
            present here but OUTSIDE the strategy universe get a skip decision
            with reason ``not_in_universe`` (shared-wallet protection made
            visible). They are never swapped.
    """
    warnings: list[str] = []
    decisions: list[ConsolidationDecision] = []
    intents: list[Any] = []

    targets_by_upper = _resolve_plan_target_set(
        mode=mode,
        token_consolidation_cfg=token_consolidation_cfg,
        asset_policy=asset_policy,
        target_token=target_token,
        targets=targets,
        warnings=warnings,
    )
    if targets_by_upper is None:
        return ConsolidationPlan(intents=[], decisions=[], warnings=warnings)
    targets_upper = set(targets_by_upper)

    # Deterministic primary target: honour the configured target_token when it
    # is one of the resolved targets (always true for target_token policy);
    # otherwise the lexicographically-first entry asset. Original casing —
    # this symbol feeds Intent.swap(to_token=...).
    configured = (target_token or "").upper()
    primary_target = targets_by_upper.get(configured) or targets_by_upper[sorted(targets_by_upper)[0]]

    from almanak.framework.accounting.gas_pricing import native_token_for_chain

    native_symbol = native_token_for_chain(chain or "").upper()
    keep_tokens = {
        k.upper() for k in (getattr(token_consolidation_cfg, "keep_tokens", None) or []) if isinstance(k, str) and k
    }
    min_swap_value = _coerce_decimal(getattr(token_consolidation_cfg, "min_swap_value_usd", None))
    if min_swap_value is None:
        min_swap_value = Decimal("5")

    # Dedupe the universe case-insensitively but PRESERVE original casing
    # (first-seen wins): the symbol feeds market.balance()/price() and
    # Intent.swap(from_token=...) — upper-casing a canonical mixed-case
    # symbol (USDC.e) breaks registry lookups and skips the consolidation
    # it was supposed to perform (Codex audit).
    universe_by_upper: dict[str, str] = {}
    for t in token_universe:
        if isinstance(t, str) and t:
            universe_by_upper.setdefault(t.upper(), t)

    # Shared-wallet audit records: wallet tokens outside the strategy universe
    # are excluded structurally — surface them in the decision trail.
    wallet_by_upper: dict[str, str] = {}
    for t in wallet_tokens or []:
        if isinstance(t, str) and t:
            wallet_by_upper.setdefault(t.upper(), t)
    for key in sorted(set(wallet_by_upper) - set(universe_by_upper)):
        decisions.append(_skip(wallet_by_upper[key], "not_in_universe"))

    for key in sorted(universe_by_upper):
        token = universe_by_upper[key]
        decision = _decide_token(
            token,
            market=market,
            chain=chain,
            targets_upper=targets_upper,
            native_symbol=native_symbol,
            keep_tokens=keep_tokens,
            min_swap_value=min_swap_value,
            warnings=warnings,
        )
        decisions.append(decision)
        if decision.action != "swap":
            continue

        from almanak.framework.intents import Intent

        intents.append(
            Intent.swap(
                from_token=token,
                to_token=primary_target,
                amount="all",
                chain=chain,
                protocol=None,
            )
        )

    return ConsolidationPlan(intents=intents, decisions=decisions, warnings=warnings)


def fold_consolidation_outcome(result: TeardownResult, outcome: ConsolidationOutcome) -> TeardownResult:
    """Fold a :class:`ConsolidationOutcome` into a (successful) TeardownResult.

    Consolidation failure after successful closure keeps ``success=True`` —
    the closure already removed on-chain risk; the partial state is carried on
    the ``consolidation_*`` fields and surfaced via ``result_json``.
    """
    return replace(
        result,
        consolidation_planned=outcome.planned,
        consolidation_succeeded=outcome.succeeded,
        consolidation_failed=outcome.failed,
        consolidation_warnings=list(outcome.warnings),
        accounting_degraded=result.accounting_degraded or outcome.accounting_degraded_count > 0,
        accounting_degraded_count=result.accounting_degraded_count + outcome.accounting_degraded_count,
    )


__all__ = [
    "ConsolidationDecision",
    "ConsolidationOutcome",
    "ConsolidationPlan",
    "derive_strategy_token_universe",
    "fold_consolidation_outcome",
    "plan_consolidation",
    "resolve_consolidation_targets",
]
