"""Universal fresh-state guard for lending teardown intents (VIB-5139).

Strategies hand-roll their lending teardown intents (REPAY + WITHDRAW). When
those intents are built from STALE cached exposure, the unwind emits invalid
on-chain actions:

* a ``REPAY 0`` (zero measured debt) — reverts gas estimation,
* a ``WITHDRAW all`` / ``withdraw_all`` while debt is still open or collateral is
  already flat — reverts the protocol's LLTV check
  (e.g. Aave ``0xd27b44a9`` HealthFactorLowerThanLiquidationThreshold),
* a collateral withdraw ORDERED before the debt repay.

Each of those fails simulation / reverts on-chain and strands the position in
``STRATEGY_ERROR`` / "Paused awaiting approval".

``generate_lending_unwind`` already does the correct fresh-state unwind,
but it is opt-in. This guard makes the fresh-exposure discipline the DEFAULT for
*every* lending teardown by sanitising the strategy-emitted intent list before it
is dispatched — in the runner lane, the CLI lane, and the inline fallback lane
(all three derive their intents from ``generate_teardown_intents``).

It does two things: (1) **drop / reorder** — drop measured-zero REPAY/WITHDRAW
legs, enforce repay-before-withdraw; and (2) **synthesise** (VIB-4466 / VIB-589)
— when a simple single-round plan would STRAND because the wallet cannot fully
repay live debt (a plain borrow holds only the borrowed principal, but owes
principal + accrued interest, so the repay leaves dust debt and ``withdraw_all``
reverts ``HealthFactorLowerThanLiquidationThreshold``), the guard REPLACES that
position's naive ``REPAY → WITHDRAW(all)`` with the health-factor-aware unwind
staircase (``generate_lending_unwind``), which sources the interest
shortfall from collateral before the final withdraw-all. Synthesis fires ONLY
when the strand is provable (measured debt + readable prices/balance + wallet
< debt); otherwise the drop/reorder path is unchanged.

The guard remains a **pure transformation on the intent list**: it never
executes, signs, or commits — synthesis returns a *different* list of typed
intents. The result flows through the same ``_execute_intents`` funnel, so the
per-intent ``runner_helpers.commit`` pairing and the VIB-3773 anti-bypass guards
are untouched (no new execute site is introduced upstream of dispatch).

Correctness contract (CLAUDE.md §Accounting — Empty ≠ Zero):

* A FRESH on-chain exposure read (``market.position_health``) yields a MEASURED
  collateral / debt value, or it fails — in which case the value is ``None``
  (unmeasured), NEVER ``Decimal("0")``.
* We only DROP an intent on a MEASURED zero. A ``None`` read is the stale-state
  bug itself; under ``None`` we degrade conservatively — keep risk-reducing
  REPAY, never emit a withdraw-all that we cannot confirm is safe.

"Unmeasured ⇒ unsafe" is NOT the same as "no collateral" (VIB-5468 / VIB-5484).
Two failures stranded a *legitimate* zero-debt ``withdraw_all`` by conflating
those:

* ``position_health`` returns the ACCOUNT-LEVEL USD aggregate
  (``getUserAccountData.totalCollateralBase`` — the borrow-capacity figure),
  which is ``0`` for an LTV=0 / isolation / non-collateral supply EVEN WHEN the
  aToken balance is positive and fully withdrawable (DAI on Spark post-USDS
  migration — VIB-5484). The old ``collateral_is_zero`` drop refused it.
* A token-keyed protocol (Fluid) carries no ``market_id``, so the read RAISED
  ("market_id required") → unmeasured (VIB-5452); a cross-asset per-market read
  (Morpho Blue) with no price override or an empty snapshot came back unmeasured
  (VIB-5418). The old "unmeasured + no repay" branch refused it.

The fix: (1) ``_read_exposure`` falls back to the position's token for the
``market_id`` and passes oracle price overrides (matching ``generate_lending_unwind``)
so the read is MEASURED instead of spuriously unmeasured; (2) before refusing a
WITHDRAW, ``_keep_withdraw`` consults the UN-CONFLATED per-reserve on-chain read
(``MarketSnapshot.lending_position_balances`` → the literal aToken balance) to tell
"no aToken to withdraw" (skip, avoids the ``withdraw(MAX_UINT256)`` →
``INVALID_AMOUNT`` revert) from "aToken present but not counted as USD collateral"
(MUST withdraw).

Whole-position debt is the keep gate, NOT the withdraw token's reserve debt
(VIB-5468). A WITHDRAW reads the COLLATERAL reserve, so its reserve debt is ``0``
even while the ACCOUNT still owes another asset — withdrawing ALL collateral with
outstanding account debt reverts ``0x6679996d``. So the aToken-present keep is gated
on ACCOUNT-LEVEL ``debt_is_zero``: the ``position_health`` aggregate
(``getUserAccountData.totalDebtBase`` for Aave/Spark) when the account read is
measured, and — on the unmeasured fallback — only when the account-level debt leg
itself read as a measured zero. The per-reserve debt is never used as a
whole-position-debt proxy. VIB-5139's purpose is preserved: a withdraw with MEASURED
active debt and no repay-first is STILL refused, and an unconfirmable
(still-unmeasured) read STILL degrades conservatively.

Ordering contract — DO NOT mangle an already-correct staircase (VIB-5139 P0):

  ``generate_lending_unwind`` emits an ORDER-SENSITIVE *interleaved*
  staircase (blueprint 14 §"Lending unwind primitive"):
  ``WITHDRAW(slice) → SWAP(collat→borrow) → REPAY → … → WITHDRAW(all) → SWAP``.
  Globally rebuilding the list as ``[*repays, *withdraws, …]`` would push a REPAY
  of the borrow token to the FRONT — but the wallet holds no borrow token until a
  withdraw+swap runs first — re-introducing the exact "Insufficient token" revert
  this guard exists to prevent. So the guard NEVER globally reorders. It detects
  interleaving and chooses:

  * **Order-locked** (interleaved staircase): a passthrough intent sits BETWEEN
    two lending-unwind intents, OR there is more than one repay/withdraw round.
    → measured-zero drops applied IN PLACE; original relative order preserved.
  * **Simple single-round** (the hand-rolled ``REPAY 0`` / ``WITHDRAW all``
    strategies this ticket targets): all passthrough intents are entirely before
    OR entirely after one contiguous lending block, single round.
    → measured-zero drops AND the repay-before-withdraw reorder.

  Measured-zero drops are position-local and order-independent, so they apply in
  BOTH cases.

Exposure read is CHAIN-SCOPED (VIB-5139 P1): ``MarketSnapshot.position_health``
is pinned to the snapshot's primary chain. For a lending intent on a different
chain the read is forced unmeasured (``None``) — never trust an exposure read
from an unrelated chain to drop a live intent.

Limitation (honest): ``position_health`` is an ACCOUNT-LEVEL aggregate (summed
collateral / summed debt across reserves). A per-reserve stale ``withdraw_all``
(one reserve already emptied while another reserve is still live) is NOT caught —
only the all-reserves-flat case registers as measured zero. Per-reserve
validation is out of scope for this ticket.

Limitation (honest) — token-keyed multi-position conflation (VIB-5493): the
position key is ``(canonical_protocol, chain, market_id)`` with ``market_id``
empty for whole-pool protocols. For an ACCOUNT-keyed protocol (Aave / Spark /
Compound / Morpho) that empty ``market_id`` is correct — all reserves share one
account and one exposure read. But a TOKEN-keyed protocol (Fluid) also carries no
``market_id``, so two distinct Fluid reserves on the same chain (e.g. a USDC and a
WETH supply) collapse to the SAME key ``(fluid, chain, "")``. The single exposure
read is then taken for the FIRST token (``_position_tokens`` returns the first
WITHDRAW leg) and REUSED for the others, and ``has_repay`` is shared across them.
With no per-reserve balance reader for Fluid the un-conflation fallback can't
rescue the second position, so the outcome is ORDER-DEPENDENT and worse than a
loud degrade: if the first reserve reads measured-EMPTY, a LIVE second reserve
inherits that measured-zero exposure and is dropped as "measured zero collateral"
with ``degraded`` UNSET — i.e. the live position is dropped SILENTLY and even
reported as a flat ``no_op`` position (the teardown never tries to close it). If
the first reserve reads measured-LIVE, a genuinely-empty second withdraw is KEPT
and reverts ``withdraw(MAX_UINT256)`` → ``INVALID_AMOUNT`` on the empty reserve
(harmless under teardown's inverted semantics). Neither case loses funds (the
position stays intact on-chain), but the silent-drop case is a loud→silent
observability regression vs the pre-fix path (which read every Fluid reserve as
unmeasured and dropped it with ``degraded=True``). The correct fix is an
account-keyed-vs-token-keyed capability signal on the read registry so the key
splits per token ONLY for token-keyed protocols (a naive per-token key split would
regress Aave multi-collateral / looping, where all reserves share one account);
tracked in VIB-5493. Until then this gap is LATENT — the shipping Fluid strategies
are single-reserve, and the common single-position Fluid teardown (VIB-5452, this
ticket's target) is handled correctly. Do not rely on a multi-reserve token-keyed
teardown until VIB-5493 lands.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from almanak.framework.intents.vocabulary import Intent, IntentType

if TYPE_CHECKING:  # pragma: no cover
    from almanak.framework.teardown.models import TeardownMode

# Exposure (collateral or debt) below this USD value is treated as measured-flat.
# Deliberately matches ``leverage_loop._DUST_USD`` ($0.01) — both answer the same
# question ("is this leg effectively cleared on-chain?"), so the guard's drop
# threshold must agree with the staircase's "debt cleared" threshold. This is a
# DIFFERENT question from the $5 token-consolidation dust floor
# (``TokenConsolidationConfig.min_swap_value_usd``), which is "is a residual swap
# worth the gas?" — a much higher, economic threshold. Do not unify the two.
_DUST_USD = Decimal("0.01")

# Strand-predicate interest buffer: the naive plan is replaced when the wallet
# cannot cover the live debt by at least this margin. The debt read is a snapshot;
# variable-rate interest accrues between the read and on-chain execution, so a
# wallet that *just barely* covers snapshot debt can still strand at execution. We
# fire the (always-correct) staircase when the wallet doesn't exceed debt by >1% —
# the staircase itself handles the wallet-covers case (repay_full → withdraw_all),
# so erring toward synthesis here is safe and only costs a little gas in the thin
# near-parity band. Same 1% family as ``leverage_loop._REPAY_SAFETY_HAIRCUT``.
_STRAND_PREDICATE_BUFFER = Decimal("1.01")

# Degrade-path repay haircut: when synthesis fails and we keep only a risk-reducing
# partial repay, repay slightly under the wallet balance so a partial repay never
# tips into over-pull/revert on Morpho/Compound from rounding/interest at execution
# time. Mirrors ``leverage_loop._REPAY_SAFETY_HAIRCUT`` (under-repay is the safe
# direction for a degraded teardown).
_DEGRADE_REPAY_HAIRCUT = Decimal("0.01")

# Intent types this guard reasons about. SUPPLY/BORROW never appear in a teardown
# (they ADD exposure); the guard leaves any non-lending or non-unwind intent
# untouched.
_REPAY_TYPES = (IntentType.REPAY, IntentType.DELEVERAGE)
_WITHDRAW_TYPE = IntentType.WITHDRAW


@dataclass
class LendingGuardResult:
    """Outcome of sanitising one teardown intent list.

    Attributes:
        intents: The sanitised, repay-first-ordered intent list to dispatch.
        dropped: Human-readable reasons for every intent the guard removed.
        degraded: True when at least one position's fresh exposure read failed
            (unmeasured) and the guard had to make a conservative call.
        no_op_positions: Position keys found fully flat (no debt, no collateral)
            on the fresh read — their intents were all dropped.
        synthesized_positions: Position keys whose naive ``REPAY → WITHDRAW(all)``
            plan was REPLACED with a health-factor-aware unwind staircase because
            the wallet could not fully repay live debt (VIB-4466 / VIB-589) — the
            naive plan would have left dust debt and reverted the withdraw-all.
    """

    intents: list[Any]
    dropped: list[str] = field(default_factory=list)
    degraded: bool = False
    no_op_positions: list[str] = field(default_factory=list)
    synthesized_positions: list[str] = field(default_factory=list)


@dataclass
class _Exposure:
    """Fresh exposure for one lending position. ``None`` == unmeasured."""

    collateral_usd: Decimal | None
    debt_usd: Decimal | None

    @property
    def measured(self) -> bool:
        return self.collateral_usd is not None and self.debt_usd is not None

    @property
    def debt_is_zero(self) -> bool:
        """True only on a MEASURED zero debt (Empty ≠ Zero)."""
        return self.debt_usd is not None and self.debt_usd <= _DUST_USD

    @property
    def collateral_is_zero(self) -> bool:
        """True only on a MEASURED zero collateral (Empty ≠ Zero)."""
        return self.collateral_usd is not None and self.collateral_usd <= _DUST_USD


def _is_lending_protocol(protocol: str | None) -> bool:
    """Whether ``position_health`` can read this protocol's exposure.

    Mirrors the exact dispatch in ``PositionHealthProvider.get_health`` (a market-
    health reader OR an account-state spec), NOT ``LendingReadRegistry.has`` — which
    only covers ``spec_loaders`` (aave_v3 / spark) and would silently exclude
    Morpho Blue and Compound V3, the very protocols this ticket targets
    (aave-looping + a bsc deployment). A protocol the health reader cannot serve
    is left untouched (no false drops).
    """
    if not protocol:
        return False
    try:
        from almanak.connectors._strategy_base.lending_read_registry import LendingReadRegistry

        canonical = LendingReadRegistry.normalize_protocol(protocol)
        if not canonical:
            return False
        return LendingReadRegistry.market_health_reader(
            canonical
        ) is not None or LendingReadRegistry.supports_account_state(canonical)
    except Exception:
        return False


def _normalize_protocol(protocol: Any) -> str:
    """Canonicalise a loosely-spelled lending protocol for grouping + reads.

    Routes through ``LendingReadRegistry.normalize_protocol`` (folds case /
    whitespace / hyphens and applies manifest aliases — ``morpho`` → ``morpho_blue``,
    ``comet`` → ``compound_v3``) so ``Aave_V3`` / ``aave`` / ``aave_v3`` group as one
    position and a position read with the same alias hits the same key. Falls back
    to ``.strip().lower()`` only if the registry is unavailable.
    """
    if not isinstance(protocol, str):
        return ""
    try:
        from almanak.connectors._strategy_base.lending_read_registry import LendingReadRegistry

        return LendingReadRegistry.normalize_protocol(protocol)
    except Exception:
        return protocol.strip().lower()


def _intent_type(intent: Any) -> IntentType | None:
    return getattr(intent, "intent_type", None)


def _is_repay(intent: Any) -> bool:
    return _intent_type(intent) in _REPAY_TYPES


def _is_withdraw(intent: Any) -> bool:
    return _intent_type(intent) == _WITHDRAW_TYPE


def _is_lending_unwind(intent: Any) -> bool:
    """A lending REPAY / DELEVERAGE / WITHDRAW on a known lending protocol."""
    return (_is_repay(intent) or _is_withdraw(intent)) and _is_lending_protocol(getattr(intent, "protocol", None))


def _position_key(intent: Any) -> tuple[str, str, str]:
    """Group lending intents by ``(canonical_protocol, chain, market_id)``.

    ``market_id`` stays the raw string ("" for Aave where the protocol treats it
    as informational — one pool per chain; non-empty and required for Morpho /
    Compound isolated markets), so the key type stays ``tuple[str, str, str]``.
    """
    protocol = _normalize_protocol(getattr(intent, "protocol", ""))
    chain = (getattr(intent, "chain", "") or "").lower()
    market_id = getattr(intent, "market_id", "") or ""
    return (protocol, chain, market_id)


def _position_tokens(intents: list[Any], key: tuple[str, str, str]) -> tuple[str | None, str | None]:
    """``(collateral_token, borrow_token)`` for a position from its WITHDRAW / REPAY legs.

    The WITHDRAW intent's ``token`` is the collateral leg; the REPAY intent's
    ``token`` is the borrow leg. Unlike :func:`_extract_position_legs` (which
    requires BOTH legs and is used to size a synthesis staircase), this tolerates
    a **collateral-only** position (a pure lender with no debt and so no REPAY) —
    it returns ``(collateral, None)`` for it. Used to shape the exposure read
    (market-id fallback + price overrides), where the collateral leg alone is
    enough to make a Fluid / Morpho supply read MEASURED (VIB-5468).
    """
    collateral_token: str | None = None
    borrow_token: str | None = None
    for intent in intents:
        if not _is_lending_unwind(intent) or _position_key(intent) != key:
            continue
        token = getattr(intent, "token", None)
        if not token:
            continue
        if _is_withdraw(intent) and collateral_token is None:
            collateral_token = token
        elif _is_repay(intent) and borrow_token is None:
            borrow_token = token
    return (collateral_token, borrow_token)


def _safe_decimal(value: Any) -> Decimal | None:
    """Coerce a measured value to Decimal; ``None`` stays unmeasured."""
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _intent_chain_matches_snapshot(market: Any, intent: Any) -> bool:
    """Whether the intent's chain provably matches the snapshot's pinned chain.

    ``position_health`` is pinned to ``MarketSnapshot.chain`` (the primary chain);
    it takes no ``chain=`` argument. For a multi-chain teardown a lending intent on
    a non-primary chain would otherwise read the PRIMARY chain's exposure and could
    drop a live intent based on unrelated data (VIB-5139 P1). We only trust the read
    when the chains provably agree. An intent with no explicit chain inherits the
    snapshot's chain by convention, so it matches.
    """
    intent_chain = (getattr(intent, "chain", None) or "").lower()
    if not intent_chain:
        return True  # no explicit chain → executes on the snapshot's pinned chain
    snapshot_chain = (getattr(market, "chain", None) or "").lower()
    if not snapshot_chain:
        return False  # cannot confirm the snapshot's chain → do not trust the read
    return intent_chain == snapshot_chain


def _read_exposure(
    market: Any,
    intent: Any,
    *,
    collateral_token: str | None = None,
    borrow_token: str | None = None,
) -> _Exposure:
    """Fresh on-chain exposure for the position ``intent`` targets.

    Empty ≠ Zero: any failure leaves the value ``None`` (unmeasured), never a
    fabricated ``Decimal("0")``. The read is routed through the gateway-backed
    ``MarketSnapshot.position_health`` — no direct network access.

    Chain-scoped (P1): when the intent's chain differs from the snapshot's pinned
    chain (or cannot be confirmed equal), force unmeasured — the read would be for
    the wrong chain and must never drive a drop.

    Two read-shaping inputs avoid a spurious *unmeasured* (VIB-5468), the exact
    conflation this fix targets — an unmeasured read must mean "the chain could
    not answer", not "the guard asked the wrong question":

    * ``market_id`` falls back to the position's token when the intent carries
      none. Token-keyed protocols (Fluid) attach no ``market_id``; without this
      ``position_health`` raises "market_id is required" → unmeasured → a
      legitimate zero-debt ``withdraw_all`` gets refused (VIB-5452).
    * Oracle price overrides (collateral + borrow) are passed exactly as the
      ``generate_lending_unwind`` primitive does, so a cross-asset per-market read
      (Morpho Blue) computes USD instead of coming back unmeasured (VIB-5418).
      ``_market_price`` is failure-safe (0 → no override), so a market without a
      ``price`` method (or a missing oracle) simply omits the override.
    """
    if market is None or not hasattr(market, "position_health"):
        return _Exposure(collateral_usd=None, debt_usd=None)
    if not _intent_chain_matches_snapshot(market, intent):
        return _Exposure(collateral_usd=None, debt_usd=None)

    protocol = _normalize_protocol(getattr(intent, "protocol", ""))
    raw_market_id = getattr(intent, "market_id", "") or ""
    read_market_id = raw_market_id or collateral_token or borrow_token or ""
    collateral_price = _market_price(market, collateral_token) if collateral_token else Decimal("0")
    borrow_price = _market_price(market, borrow_token) if borrow_token else Decimal("0")
    try:
        health = market.position_health(
            protocol=protocol,
            market_id=read_market_id,
            collateral_price_usd=collateral_price if collateral_price > 0 else None,
            debt_price_usd=borrow_price if borrow_price > 0 else None,
        )
    except Exception:
        return _Exposure(collateral_usd=None, debt_usd=None)

    collateral = _safe_decimal(getattr(health, "collateral_value_usd", None))
    debt = _safe_decimal(getattr(health, "debt_value_usd", None))
    return _Exposure(collateral_usd=collateral, debt_usd=debt)


def _exposures_by_position(market: Any, intents: list[Any]) -> dict[tuple[str, str, str], _Exposure]:
    """One fresh exposure read per distinct lending position in ``intents``."""
    exposures: dict[tuple[str, str, str], _Exposure] = {}
    for intent in intents:
        if not _is_lending_unwind(intent):
            continue
        key = _position_key(intent)
        if key not in exposures:
            collateral_token, borrow_token = _position_tokens(intents, key)
            exposures[key] = _read_exposure(
                market, intent, collateral_token=collateral_token, borrow_token=borrow_token
            )
    return exposures


def _keep_repay(exposure: _Exposure, key: tuple[str, str, str], result: LendingGuardResult) -> bool:
    """Decide whether to KEEP a REPAY intent. Never keeps a measured-zero repay."""
    if exposure.debt_is_zero:
        result.dropped.append(f"REPAY {key}: measured zero debt (no stale REPAY 0)")
        return False
    # Measured nonzero debt OR unmeasured debt → keep the repay (risk-reducing).
    return True


def _keep_withdraw(
    market: Any,
    intent: Any,
    exposure: _Exposure,
    key: tuple[str, str, str],
    has_repay: bool,
    result: LendingGuardResult,
) -> bool:
    """Decide whether to KEEP a WITHDRAW intent (repay-first / safe ordering).

    ``has_repay`` is per-position over the WHOLE plan (a repay anywhere for this
    position ⇒ True), so an interleaved staircase — whose first WITHDRAW precedes
    its repays in execution order but whose plan DOES contain repays — is never
    dropped by the active-debt / unmeasured guards below.

    Two refuse branches first consult the un-conflated per-reserve on-chain read
    (:func:`_read_reserve_balances`) before stranding a legitimate withdraw
    (VIB-5468 / VIB-5484); see the inline notes.
    """
    token = getattr(intent, "token", None)
    if exposure.collateral_is_zero:
        # VIB-5484: the account-level USD collateral aggregate
        # (``getUserAccountData.totalCollateralBase``) is ``0`` for an LTV=0 /
        # isolation / non-collateral supply EVEN WHEN the aToken balance is positive
        # and fully withdrawable (e.g. DAI on Spark post-USDS migration). When debt
        # is a measured zero the withdraw carries no health-factor risk, so confirm
        # via the literal aToken balance instead of the conflated aggregate.
        if exposure.debt_is_zero:
            supply, _debt = _read_reserve_balances(market, key, token)
            if supply is not None and supply > 0:
                return True  # real withdrawable supply not counted as USD collateral
            if supply is not None and supply == 0:
                result.dropped.append(f"WITHDRAW {key}: zero on-chain aToken balance (nothing to withdraw)")
                return False
            # supply unmeasured (shares-based / no reader) → keep the conservative drop.
        result.dropped.append(f"WITHDRAW {key}: measured zero collateral (nothing to withdraw)")
        return False
    if exposure.measured and not exposure.debt_is_zero and not has_repay:
        # MEASURED active debt and no repay-first to clear it — withdrawing
        # collateral now trips the protocol's LLTV check (Aave
        # HealthFactorLowerThanLiquidationThreshold, the ALM-2811 failure).
        result.dropped.append(f"WITHDRAW {key}: active debt and no repay-first — refusing unsafe withdraw")
        return False
    if not exposure.measured and not has_repay:
        # VIB-5468: the account read came back unmeasured (gateway blip, empty
        # snapshot prices, an unsupported account-state read). Consult a fresh
        # on-chain per-reserve read to avoid stranding a genuinely-safe withdraw —
        # but the per-reserve read only sees the WITHDRAW token's OWN reserve, so
        # it can prove "nothing to withdraw" (a no-op drop) and confirm the aToken
        # is present, yet it CANNOT prove the ACCOUNT carries no debt.
        supply, _reserve_debt = _read_reserve_balances(market, key, token)
        if supply is not None and supply == 0:
            # Genuine no-op: no aToken to withdraw. Safe to drop regardless of debt
            # (avoids the ``withdraw(MAX_UINT256)`` → ``INVALID_AMOUNT`` revert).
            result.dropped.append(f"WITHDRAW {key}: zero on-chain aToken balance (nothing to withdraw)")
            return False
        # VIB-5468 (whole-position debt): KEEP a withdraw_all only when ACCOUNT-LEVEL
        # debt is CONFIRMED zero. ``_reserve_debt`` is the withdraw TOKEN's reserve
        # debt, NOT the account aggregate — on Aave/Spark a ``WETH`` withdraw_all
        # reads WETH-reserve debt == 0 while the account still owes ``USDC`` on
        # another reserve, and withdrawing ALL collateral with outstanding account
        # debt reverts ``0x6679996d``. So the keep is gated on ``exposure.debt_is_zero``
        # (the account aggregate, reachable here when collateral was the unmeasured
        # leg but debt read as a measured zero) — NEVER on ``_reserve_debt``. When the
        # account debt is itself unmeasured we cannot confirm whole-position safety,
        # so we fall through to the conservative refusal (VIB-5139's purpose).
        if supply is not None and supply > 0 and exposure.debt_is_zero:
            return True
        result.dropped.append(f"WITHDRAW {key}: unmeasured exposure and no repay-first — refusing unsafe withdraw_all")
        return False
    return True


def _decide_keeps(
    market: Any,
    intents: list[Any],
    exposures: dict[tuple[str, str, str], _Exposure],
    positions_with_repay: set[tuple[str, str, str]],
    result: LendingGuardResult,
) -> list[Any]:
    """Apply the position-local keep/drop gates IN ORDER.

    Returns the kept intents in their ORIGINAL relative order (no reorder here).
    Drops are position-local and order-independent, so this is correct for both
    interleaved staircases and simple single-round plans. ``result.dropped`` /
    ``result.degraded`` are populated as a side effect.
    """
    kept: list[Any] = []
    for intent in intents:
        if not _is_lending_unwind(intent):
            kept.append(intent)
            continue
        key = _position_key(intent)
        exposure = exposures[key]
        if not exposure.measured:
            result.degraded = True
        if _is_repay(intent):
            if _keep_repay(exposure, key, result):
                kept.append(intent)
        elif _keep_withdraw(market, intent, exposure, key, key in positions_with_repay, result):
            kept.append(intent)
    return kept


def _is_order_locked(intents: list[Any]) -> bool:
    """Whether the lending plan is an ORDER-SENSITIVE interleaved staircase.

    Order-locked ONLY when a passthrough (non-lending-unwind) intent sits BETWEEN
    two lending-unwind intents — the SWAPs interleaved in
    ``generate_lending_unwind``'s ``WITHDRAW → SWAP → REPAY`` staircase. A
    genuine HF-safe staircase ALWAYS interleaves a collateral→debt SWAP between its
    withdraw and repay, so interleaving is the reliable signal that the plan is a
    known-safe, self-clearing unwind that must not be reordered or re-synthesized.

    We deliberately do NOT order-lock on a raw repay/withdraw round count (the old
    clause (b)): a non-interleaved multi-round plan — e.g. two independent borrow
    positions (an Aave borrow + a Morpho borrow), or a hand-rolled
    ``partial REPAY → partial REPAY → WITHDRAW(all)`` — is NOT a known-safe
    staircase. Order-locking it would skip the strand/synthesis safety check and
    let an unsafe withdraw-all through if residual debt remains. Routing such plans
    through the normal decide→synthesise→reorder path is fail-closed: each position
    is checked, and a proven strand is replaced with the HF-safe staircase. The
    reorder of a non-interleaved plan is a no-op (repays already precede withdraws
    after ``_reorder_simple``), so nothing safe is broken.

    Operates on the ORIGINAL list so the structure is read before any drop applies.
    """
    lending_indices = [idx for idx, i in enumerate(intents) if _is_lending_unwind(i)]
    if len(lending_indices) <= 1:
        return False
    # A passthrough intent interleaved inside the lending span ⇒ genuine staircase.
    span = range(lending_indices[0], lending_indices[-1] + 1)
    return any(not _is_lending_unwind(intents[idx]) for idx in span)


def _reorder_simple(kept: list[Any]) -> list[Any]:
    """Repay-before-withdraw reorder for a SIMPLE single-round plan.

    Only called when the plan is NOT order-locked: all passthrough intents are
    entirely before OR entirely after one contiguous lending block, single round.
    Passthrough intents keep their position relative to the lending block; the
    lending intents are emitted repay(s) then withdraw(s).
    """
    first_lending_index = next((idx for idx, i in enumerate(kept) if _is_lending_unwind(i)), None)
    if first_lending_index is None:
        return list(kept)
    before: list[Any] = []
    repays: list[Any] = []
    withdraws: list[Any] = []
    after: list[Any] = []
    for idx, intent in enumerate(kept):
        if _is_lending_unwind(intent):
            (repays if _is_repay(intent) else withdraws).append(intent)
        elif idx < first_lending_index:
            before.append(intent)
        else:
            after.append(intent)
    return [*before, *repays, *withdraws, *after]


def _market_price(market: Any, token: str) -> Decimal:
    """Oracle price of ``token`` via the gateway-backed market; 0 if unavailable."""
    try:
        p = market.price(token)
        return Decimal(str(p)) if p else Decimal("0")
    except Exception:
        return Decimal("0")


def _market_wallet_balance(market: Any, token: str) -> Decimal | None:
    """Live wallet balance of ``token``; ``None`` (unmeasured) on read failure.

    Empty ≠ Zero: a failed balance read is unmeasured, NEVER ``Decimal("0")`` —
    a fabricated zero here would make the strand predicate fire spuriously.
    """
    try:
        bal = market.balance(token)
        amount = getattr(bal, "balance", bal)
        if amount is None:
            return None
        return Decimal(str(amount))
    except Exception:
        return None


def _read_reserve_balances(
    market: Any,
    key: tuple[str, str, str],
    token: str | None,
) -> tuple[int | None, int | None]:
    """Fresh per-reserve on-chain ``(supply_wei, debt_wei)`` for the withdraw token.

    The un-conflated counterpart to :func:`_read_exposure`: it reads the literal
    aToken balance (and reserve debt) via the gateway-backed
    ``MarketSnapshot.lending_position_balances`` instead of the account-level USD
    collateral aggregate. This is the on-chain ``position(market, wallet)`` fallback
    VIB-5468 calls for — it distinguishes "no aToken to withdraw" (a genuine no-op
    that must be skipped to avoid the ``withdraw(MAX_UINT256)`` → ``INVALID_AMOUNT``
    revert) from "aToken present but not counted as USD collateral" (an LTV=0 /
    isolation / non-collateral supply that MUST be withdrawn; VIB-5484).

    Empty ≠ Zero: each leg is ``None`` when unmeasured (no market method, no
    gateway, unresolvable token, a shares-based protocol such as Morpho Blue whose
    flat balance is not readable, or a read failure) — never a fabricated ``0``.
    """
    if market is None or not token or not hasattr(market, "lending_position_balances"):
        return (None, None)
    protocol, chain, market_id = key
    try:
        return market.lending_position_balances(protocol, token, market_id=market_id or None, chain=chain or None)
    except Exception:
        return (None, None)


def _extract_position_legs(intents: list[Any], key: tuple[str, str, str]) -> tuple[str, str] | None:
    """``(collateral_token, borrow_token)`` for a position, from its WITHDRAW / REPAY legs.

    The WITHDRAW intent's ``token`` is the collateral leg; the REPAY intent's
    ``token`` is the borrow leg. Returns ``None`` when either leg is missing — a
    position the guard cannot re-plan, so it falls through to the existing
    keep/drop gates untouched.
    """
    collateral_token: str | None = None
    borrow_token: str | None = None
    for intent in intents:
        if not _is_lending_unwind(intent) or _position_key(intent) != key:
            continue
        token = getattr(intent, "token", None)
        if not token:
            continue
        if _is_withdraw(intent) and collateral_token is None:
            collateral_token = token
        elif _is_repay(intent) and borrow_token is None:
            borrow_token = token
    if collateral_token is None or borrow_token is None:
        return None
    return (collateral_token, borrow_token)


def _plan_has_withdraw_all(intents: list[Any], key: tuple[str, str, str]) -> bool:
    """Whether the plan withdraws the FULL collateral (``withdraw_all``) for this position.

    The dust-debt strand is specific to a withdraw-all (MAX_UINT256) withdraw: Aave
    rejects withdrawing 100% of collateral while ANY debt remains. A plan that
    withdraws a specific (partial) amount keeps HF headroom and does not hit that
    revert, so it must NOT be replaced with a full unwind — only withdraw-all plans
    are eligible for synthesis.
    """
    return any(
        _is_lending_unwind(intent)
        and _is_withdraw(intent)
        and _position_key(intent) == key
        and bool(getattr(intent, "withdraw_all", False))
        for intent in intents
    )


def _planned_repay_covers_debt(
    intents: list[Any],
    key: tuple[str, str, str],
    *,
    debt_usd: Decimal,
    borrow_price: Decimal,
) -> bool:
    """Whether the plan's REPAY for this position would clear the measured debt.

    ``repay_full=True`` clears it (Aave caps at the debt). An explicit partial
    ``amount`` only clears it when ``amount * price >= debt``. A partial repay that
    does NOT cover the debt leaves residual debt before the withdraw-all — the same
    revert this guard fixes, even when the wallet itself could cover the debt.
    """
    for intent in intents:
        if not (_is_lending_unwind(intent) and _is_repay(intent) and _position_key(intent) == key):
            continue
        if bool(getattr(intent, "repay_full", False)):
            return True
        amount = getattr(intent, "amount", None)
        # Require the SAME interest buffer as the strand predicate: a partial repay
        # that only covers the snapshot debt can still leave dust once interest
        # accrues before execution, recreating the withdraw-all revert.
        if isinstance(amount, Decimal) and amount * borrow_price >= debt_usd * _STRAND_PREDICATE_BUFFER:
            return True
    return False


def _position_needs_staircase(
    market: Any,
    intents: list[Any],
    key: tuple[str, str, str],
    exposure: _Exposure,
) -> bool:
    """Whether a naive ``REPAY → WITHDRAW(all)`` for this position would STRAND.

    The bug (VIB-589 / VIB-4466): a plain borrow holds only the borrowed
    principal while it owes principal + accrued interest. Repaying the wallet
    balance leaves dust debt, and ``withdraw_all`` (MAX_UINT256) then reverts
    ``HealthFactorLowerThanLiquidationThreshold`` because no collateral can be
    fully withdrawn while ANY debt remains.

    We only intervene when we can PROVE the strand — i.e. all of:

    * MEASURED debt above the dust floor (Empty ≠ Zero — never act on ``None``),
    * both legs resolvable (a collateral WITHDRAW and a borrow REPAY),
    * live borrow / collateral prices and the wallet's borrow-token balance are
      all measured (the staircase planner needs them),
    * the plan actually issues a ``withdraw_all`` (only the withdraw-all-with-debt
      revert is in scope; a deliberate partial withdraw keeps HF headroom and must
      not be replaced), and
    * the position would still carry debt at the final withdraw-all — either the
      wallet cannot cover the live debt (with a small interest buffer for accrual
      between the snapshot read and execution), OR the plan's REPAY is an explicit
      partial that does not cover the measured debt.

    When the wallet covers the debt AND the planned repay clears it, the naive plan
    works (Aave caps the repay at the debt, so the withdraw-all is safe) — we leave
    it untouched. When any input is unmeasured, we do NOT synthesise (the planner
    would size against fabricated zeros); the existing keep/drop gates handle that.
    """
    if market is None or not exposure.measured or exposure.debt_is_zero:
        return False
    legs = _extract_position_legs(intents, key)
    if legs is None:
        return False
    # Only a withdraw-all plan can hit the dust-debt withdraw-all revert.
    if not _plan_has_withdraw_all(intents, key):
        return False
    collateral_token, borrow_token = legs
    borrow_price = _market_price(market, borrow_token)
    collateral_price = _market_price(market, collateral_token)
    if borrow_price <= 0 or collateral_price <= 0:
        return False
    wallet_borrow = _market_wallet_balance(market, borrow_token)
    if wallet_borrow is None:
        return False
    debt_usd = exposure.debt_usd
    assert debt_usd is not None  # guaranteed measured by the guard above
    # Proven strand (a): the wallet's borrow token cannot cover the live debt (with
    # a small interest buffer for accrual between the snapshot read and execution),
    # so a wallet-balance repay leaves residual debt and withdraw-all reverts.
    if wallet_borrow * borrow_price < debt_usd * _STRAND_PREDICATE_BUFFER:
        return True
    # Proven strand (b): the wallet could cover the debt, but the plan's REPAY is an
    # explicit partial that does NOT clear the measured debt, so residual debt
    # remains before the withdraw-all and it reverts the same way.
    return not _planned_repay_covers_debt(intents, key, debt_usd=debt_usd, borrow_price=borrow_price)


def _degrade_to_hf_safe_partial(
    market: Any,
    key: tuple[str, str, str],
    collateral_token: str,
    borrow_token: str,
    result: LendingGuardResult,
    reason: str,
) -> list[Any]:
    """Safe fallback when the staircase planner cannot size a full unwind.

    Reached only when synthesis raised (health factor too low to withdraw any
    collateral safely, or a price/LLTV read failed). Emits at most a
    risk-reducing REPAY (explicit partial of the wallet balance — never
    ``repay_full``, which over-pulls on Morpho/Compound) and NEVER a MAX_UINT256
    ``withdraw_all`` while debt may remain (that is the revert we are avoiding).
    Removing on-chain risk is teardown's first job; a residual collateral leg is
    surfaced loudly via ``degraded`` rather than stranded by a reverting tx.
    """
    protocol, chain, market_id = key
    result.degraded = True
    result.dropped.append(
        f"WITHDRAW {key}: staircase unavailable ({reason}) — degraded to repay-only, "
        "withholding unsafe withdraw_all (residual collateral surfaced via degraded)"
    )
    wallet_borrow = _market_wallet_balance(market, borrow_token)
    if wallet_borrow is not None and wallet_borrow > 0:
        # Repay slightly under the wallet balance (parity with leverage_loop's
        # partial repay) so rounding/interest at execution can't tip a partial
        # repay into an over-pull/revert on Morpho/Compound. Under-repay is safe.
        repay_amount = wallet_borrow * (Decimal("1") - _DEGRADE_REPAY_HAIRCUT)
        kwargs: dict[str, Any] = {
            "protocol": protocol,
            "token": borrow_token,
            "amount": repay_amount,
            "repay_full": False,
        }
        if market_id:
            kwargs["market_id"] = market_id
        if chain:
            kwargs["chain"] = chain
        return [Intent.repay(**kwargs)]
    return []


def _synthesize_or_degrade(
    market: Any,
    intents: list[Any],
    key: tuple[str, str, str],
    exposure: _Exposure,
    mode: TeardownMode | None,
    result: LendingGuardResult,
) -> list[Any]:
    """Replace a position's naive plan with the HF-aware unwind staircase.

    Calls the first-class ``generate_lending_unwind`` primitive (VIB-5467,
    blueprint 14 §"Lending unwind primitive") — the same staircase a strategy can
    call directly: it repays wallet-held debt first, then runs HF-safe
    WITHDRAW→SWAP→REPAY rounds that source the interest shortfall from collateral,
    then a final ``withdraw_all`` once debt is TRULY zero, then a residual sweep.

    ``consolidate_to=collateral_token`` for a cross-asset position (collateral ≠
    borrow, the plain-borrow case): the final sweep only converts the small
    over-funded BORROW-token buffer back to collateral, leaving the recovered
    collateral as-is for the framework's TOKEN_CONSOLIDATION phase — NOT swapping
    the whole collateral stack into the debt token (which would force a gratuitous
    collateral→debt→target round-trip). On planner failure, degrade safely.
    """
    legs = _extract_position_legs(intents, key)
    if legs is None:  # pragma: no cover - guarded by _position_needs_staircase
        return []
    collateral_token, borrow_token = legs
    protocol, chain, market_id = key
    from almanak.framework.teardown.lending_unwind import generate_lending_unwind

    try:
        synth = generate_lending_unwind(
            market=market,
            protocol=protocol,
            collateral_token=collateral_token,
            borrow_token=borrow_token,
            market_id=market_id or None,
            chain=chain or None,
            mode=mode,
            consolidate_to=collateral_token if collateral_token != borrow_token else None,
        )
    except Exception as exc:  # LendingUnwindError / ValueError (missing price/LLTV)
        return _degrade_to_hf_safe_partial(market, key, collateral_token, borrow_token, result, str(exc))

    result.synthesized_positions.append(f"{protocol}/{chain}/{market_id}")
    return synth


def _build_with_synthesis(
    intents: list[Any],
    exposures: dict[tuple[str, str, str], _Exposure],
    synth_keys: set[tuple[str, str, str]],
    kept: list[Any],
    market: Any,
    mode: TeardownMode | None,
    result: LendingGuardResult,
) -> list[Any]:
    """Splice synthesised staircases into the simple (non-order-locked) plan.

    Passthrough intents keep their before/after placement relative to the lending
    block (same contract as ``_reorder_simple``). Synthesised positions emit the
    staircase in planner order; non-synthesised positions keep their gated
    repay-first intents from ``kept``. Position order follows first appearance.

    Multi-position is genuinely reachable: ``_is_order_locked`` counts rounds PER
    position, so two independent single-round positions are NOT order-locked and
    each is evaluated for synthesis here. The per-position loop handles a mix of
    synth and non-synth positions in one simple plan.
    """
    first_lending_index = next((idx for idx, i in enumerate(intents) if _is_lending_unwind(i)), None)
    if first_lending_index is None:  # pragma: no cover - synth_keys implies a lending intent
        return _reorder_simple(kept)
    before = [i for idx, i in enumerate(intents) if idx < first_lending_index and not _is_lending_unwind(i)]
    after = [i for idx, i in enumerate(intents) if idx > first_lending_index and not _is_lending_unwind(i)]

    lending_block: list[Any] = []
    seen_keys: list[tuple[str, str, str]] = []
    for intent in intents:
        if not _is_lending_unwind(intent):
            continue
        key = _position_key(intent)
        if key in seen_keys:
            continue
        seen_keys.append(key)
        if key in synth_keys:
            lending_block.extend(_synthesize_or_degrade(market, intents, key, exposures[key], mode, result))
        else:
            # Non-synthesised position: keep its gated intents, repay-first.
            repays = [i for i in kept if _is_lending_unwind(i) and _is_repay(i) and _position_key(i) == key]
            withdraws = [i for i in kept if _is_lending_unwind(i) and _is_withdraw(i) and _position_key(i) == key]
            lending_block.extend([*repays, *withdraws])
    return [*before, *lending_block, *after]


def sanitize_lending_teardown_intents(
    intents: list[Any], market: Any, *, mode: TeardownMode | None = None
) -> LendingGuardResult:
    """Sanitise strategy-emitted lending teardown intents against fresh state.

    Args:
        intents: The ordered intent list from ``generate_teardown_intents``.
        market: A ``MarketSnapshot`` (gateway-backed) exposing ``position_health``,
            ``price`` and ``balance``. May be ``None`` (no market available) —
            every lending position then reads as unmeasured and the guard degrades
            conservatively (no synthesis from fabricated zeros).
        mode: TeardownMode (SOFT/HARD). Threaded into a synthesised unwind so an
            emergency (HARD) teardown uses the lower HF floor + wider slippage.

    Returns:
        A :class:`LendingGuardResult` whose ``intents`` is the fresh-state-validated
        list to dispatch. An interleaved leveraged-loop staircase keeps its EXACT
        original order (drops applied in place). A simple single-round hand-rolled
        plan gets the repay-before-withdraw reorder — UNLESS a position's wallet
        cannot fully repay its live debt, in which case the naive
        ``REPAY → WITHDRAW(all)`` (which would strand collateral on a withdraw-all
        revert) is REPLACED with a health-factor-aware unwind staircase (VIB-4466 /
        VIB-589). Non-lending intents pass through unchanged.
    """
    result = LendingGuardResult(intents=list(intents))
    if not intents:
        return result

    exposures = _exposures_by_position(market, intents)
    if not exposures:
        # No lending unwind intents — nothing to guard.
        return result

    positions_with_repay = {_position_key(i) for i in intents if _is_repay(i) and _is_lending_unwind(i)}

    # Read the plan's structure from the ORIGINAL list before any drop, then apply
    # position-local drops in place. An interleaved staircase keeps its exact order;
    # only a simple single-round plan is reordered repay-first.
    order_locked = _is_order_locked(intents)
    kept = _decide_keeps(market, intents, exposures, positions_with_repay, result)

    # No-op positions: fully flat on the fresh read (measured zero debt AND
    # collateral). Their intents were already dropped above — record for the caller.
    result.no_op_positions = [
        f"{p}/{c}/{m}" for (p, c, m), e in exposures.items() if e.debt_is_zero and e.collateral_is_zero
    ]

    if order_locked:
        # An already-correct interleaved staircase — never re-synthesise or reorder.
        result.intents = list(kept)
        return result

    # Simple single-round plan: replace any position whose naive plan would strand
    # (wallet cannot fully repay live debt) with the HF-aware unwind staircase. When
    # no position needs it, behaviour is identical to the prior repay-first reorder.
    synth_keys = {key for key in exposures if _position_needs_staircase(market, intents, key, exposures[key])}
    if not synth_keys:
        result.intents = _reorder_simple(kept)
    else:
        result.intents = _build_with_synthesis(intents, exposures, synth_keys, kept, market, mode, result)
    return result
