"""Boot-time resume-into-terminal-state guard (VIB-5887).

``deployment_id = sha256(wallet:chain)`` is deterministic (blueprint 29 §2), so
relaunching a strategy on the same wallet+chain RESUMES the prior run's persisted
``strategy_state``. When that prior state was **terminal** — a lifecycle strategy
that finished (``SUPPLY→BORROW→REPAY→WITHDRAW``) or a position that was fully
unwound — the restored ``decide()`` reads "nothing to do" and returns ``HOLD`` for
the whole run. If the wallet now holds materially fresh capital (a redeploy funded
new money onto the same identity, or an operator reused a pool wallet without
resetting its demo state), that capital sits **idle and unused** and the run is a
silent NULL test. On a hosted redeploy — where there is no operator state-reset
step — this presents as a healthy green run doing nothing with user funds.

This is the **detect-and-warn** half of the fix. At boot — after the strategy's
persisted state is restored and the gateway client is wired, before the first
iteration — it:

0. gates on ``is_resume`` — the run actually RESUMED persisted state (a prior run
   for this deployment exists). A fresh, first-ever boot short-circuits;
1. asks the strategy whether its RESTORED state is terminal, via the optional
   :meth:`~almanak.framework.strategies.intent_strategy.IntentStrategy.is_lifecycle_complete`
   hook (default ``False``; terminal-ness is strategy-owned — the framework
   persists an opaque ``get_persistent_state()`` dict and has no generic notion
   of a completed business lifecycle); and
2. reads the wallet's current idle deployable capital in USD (the sum of the
   strategy's portfolio-snapshot wallet balances, counting the native gas token only
   ABOVE the strategy's gas reserve — so a wallet holding only its gas reserve does
   not false-fire, while unwound-to-native collateral or a native top-up still
   counts).

When the run resumed, the state is terminal, **and** the idle capital clears a
non-trivial floor, the runner emits a distinct ``RESUMED-TERMINAL`` boot signal + a
loud WARNING naming the idle capital, so the no-op can never be silent.

**Design: warn, do NOT auto-reinitialize (VIB-5887).** Auto re-initializing the
lifecycle is deliberately NOT done. The state model (blueprint 06) has no generic
"reinitialize lifecycle" primitive; each strategy owns its state machine. Forcing
re-entry could double-open positions or re-run a completed lifecycle, and ``--fresh``
on a real network deliberately *preserves* the immutable on-chain accounting record
(``_fresh_clear_state``, VIB-5784). The clearly-safe fix is therefore a loud,
un-missable signal + structured log — not a mutation. The guard also does NOT halt:
a resumed-terminal deployment holds only idle cash (no open on-chain risk to
reduce), and halting a legitimately-idle redeploy would be worse than warning. Any
auto-reinitialize path is a separate, opt-in design that requires a safe per-strategy
re-entry contract the blueprint does not yet provide.

Gateway-boundary note: this is strategy-container / framework code with no network
egress of its own — the balance read goes through the strategy's already-wired
gateway-backed providers (``get_portfolio_snapshot`` → ``MarketSnapshot`` /
``GatewayBalanceProvider``), exactly as the portfolio valuer does.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from almanak.framework.runner.runner_models import StrategyProtocol
    from almanak.framework.runner.strategy_runner import StrategyRunner

logger = logging.getLogger(__name__)

# Floor (USD) above which idle NON-GAS wallet capital on a resumed-terminal
# deployment is treated as "materially fresh / unused" and worth a loud warning.
# The native gas reserve is excluded from the measured amount (see
# ``_read_idle_capital_usd``), so this floor only has to clear ERC-20 dust: the
# documented failure (~$2.5 of fresh capital funded onto a reused pool wallet,
# VIB-5887) always does, while a wallet holding only its gas reserve reads $0 here
# and stays quiet. A measured 0 balance read also stays quiet — Empty ≠ Zero is
# moot because the trigger is a POSITIVE threshold, so an unreadable balance simply
# fails toward "no warning", never a false alarm.
_MATERIAL_IDLE_USD = Decimal("1.0")


@dataclass(frozen=True)
class ResumeTerminalReport:
    """Outcome of the resume-into-terminal-state boot scan.

    Attributes:
        is_terminal: The strategy reported its restored lifecycle state as
            terminal (``is_lifecycle_complete()`` returned ``True``).
        idle_capital_usd: Best-effort idle wallet capital (USD) read at boot.
            ``Decimal("0")`` when the read was unavailable or genuinely zero.
        threshold_usd: The floor idle capital had to clear to fire the warning.
        fired: The warning condition held (``is_terminal`` and
            ``idle_capital_usd >= threshold_usd``).
    """

    is_terminal: bool
    idle_capital_usd: Decimal
    threshold_usd: Decimal
    fired: bool


def _strategy_reports_terminal(strategy: StrategyProtocol) -> bool:
    """Read the optional ``is_lifecycle_complete()`` hook (defensively).

    Missing hook or a raising hook is treated as "not terminal" — the guard must
    never fault boot, and a strategy that cannot answer is not flagged.
    """
    hook = getattr(strategy, "is_lifecycle_complete", None)
    if not callable(hook):
        return False
    try:
        return bool(hook())
    except Exception:  # noqa: BLE001 - a strategy-owned hook must never fault boot
        logger.debug("is_lifecycle_complete() raised; treating as not-terminal", exc_info=True)
        return False


def _coerce_decimal(value: Any) -> Decimal:
    """Coerce a possibly-None/str/Decimal/float amount to a FINITE ``Decimal``.

    VIB-4062 no-bifurcation contract: coerce UNCONDITIONALLY — do not branch on the
    value's runtime type. ``str()`` of a ``Decimal`` round-trips exactly, so a
    single ``Decimal(str(...))`` path handles Decimal / str / float / None without a
    type fork.

    Finite-safety: non-finite inputs (``NaN`` / ``Infinity``), unparseable strings,
    and any value whose ``str()`` raises all degrade to ``Decimal("0")``. A ``NaN``
    value_usd would otherwise poison the idle-capital sum/comparison (``NaN >=`` is
    always False → detection silently skipped) and an ``Infinity`` would false-fire.
    """
    if value is None:
        return Decimal("0")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return Decimal("0")
    except Exception:  # noqa: BLE001 - a value whose __str__ raises must not crash boot
        return Decimal("0")
    return result if result.is_finite() else Decimal("0")


def _native_gas_symbol(strategy: StrategyProtocol) -> str:
    """Uppercase native gas-token symbol for the strategy's chain (``""`` if unknown).

    Resolved from ``ChainRegistry`` via ``native_token_for_chain`` — the same
    source ``get_portfolio_snapshot`` uses when it appends the native row to
    ``wallet_balances`` — so the exclusion below matches that row exactly.
    """
    try:
        from almanak.framework.accounting.gas_pricing import native_token_for_chain

        chain = getattr(strategy, "chain", "") or ""
        return native_token_for_chain(chain).strip().upper()
    except Exception:  # noqa: BLE001 - a boot advisory must never fault on chain lookup
        logger.debug("native gas-token symbol lookup failed", exc_info=True)
        return ""


# Fallback native gas reserve (USD) subtracted from the native-token balance when a
# strategy does not declare its own ``gas_reserve``. Sized to cover a typical native
# gas float (≈0.0015 ETH, ≈0.05 AVAX) so a gas-only wallet reads ≈$0 idle here, while
# leaving real native capital above it to count. A strategy that declares ``gas_reserve``
# (native units) overrides this with its exact reserve.
_DEFAULT_GAS_RESERVE_USD = Decimal("5")


def _native_gas_reserve_usd(strategy: StrategyProtocol, bal: Any) -> Decimal:
    """USD value of the native gas reserve to exclude from idle capital.

    Prefers the strategy's declared ``gas_reserve`` (native units) priced at the
    native token's price (explicit ``price_usd`` on the balance row, else derived
    from ``value_usd / balance``); falls back to ``_DEFAULT_GAS_RESERVE_USD`` when the
    strategy declares none or the reserve cannot be priced. Only the reserve is
    excluded — native capital ABOVE it still counts, so a strategy that reclaims
    collateral as native (e.g. benqi_looping unwinds to AVAX) or receives a native
    top-up is not ignored.
    """
    declared = getattr(strategy, "gas_reserve", None)
    if declared is None:
        return _DEFAULT_GAS_RESERVE_USD
    reserve_units = _coerce_decimal(declared)
    if reserve_units <= 0:
        return _DEFAULT_GAS_RESERVE_USD

    price = _coerce_decimal(getattr(bal, "price_usd", None))
    if price > 0:
        return reserve_units * price
    balance = _coerce_decimal(getattr(bal, "balance", None))
    value_usd = _coerce_decimal(getattr(bal, "value_usd", None))
    if balance > 0:
        return reserve_units / balance * value_usd
    return _DEFAULT_GAS_RESERVE_USD


def _read_idle_capital_usd(strategy: StrategyProtocol) -> Decimal:
    """Best-effort read of idle deployable wallet capital (USD) at boot.

    Calls ``strategy.get_portfolio_snapshot()`` and sums the USD value of the wallet
    token balances. For the chain's native gas token, only the value ABOVE the gas
    reserve counts: a completed lifecycle keeps a native gas reserve (e.g.
    benqi_looping's ~0.05 AVAX) that is not deployable capital, so counting the whole
    native balance would false-fire on a gas-only wallet (CodeRabbit P2) — but native
    capital above the reserve (unwound collateral, a native top-up) IS real idle
    capital and must still count (adversarial-review fix). The positions of a
    *terminal* deployment are unwound, so wallet cash is exactly the capital that will
    sit idle. Degrades to ``Decimal("0")`` on any failure (Empty-read → no warning,
    never a false alarm).
    """
    getter = getattr(strategy, "get_portfolio_snapshot", None)
    if not callable(getter):
        return Decimal("0")
    try:
        snapshot = getter()
    except Exception:  # noqa: BLE001 - never fault boot on a best-effort valuation
        logger.debug("Idle-capital snapshot read failed; treating as 0", exc_info=True)
        return Decimal("0")

    wallet_balances = getattr(snapshot, "wallet_balances", None) or []
    native_symbol = _native_gas_symbol(strategy)
    idle = Decimal("0")
    for bal in wallet_balances:
        symbol = str(getattr(bal, "symbol", "") or "").strip().upper()
        value_usd = _coerce_decimal(getattr(bal, "value_usd", None))
        if native_symbol and symbol == native_symbol:
            # Exclude the gas reserve; count only native value above it.
            idle += max(Decimal("0"), value_usd - _native_gas_reserve_usd(strategy, bal))
        else:
            idle += value_usd
    return idle


def detect_resume_into_terminal(
    runner: StrategyRunner,
    strategy: StrategyProtocol,
    *,
    is_resume: bool,
) -> ResumeTerminalReport:
    """Scan for resume-into-terminal-state with idle capital; never raises.

    Pure detector: returns a :class:`ResumeTerminalReport` and does no logging or
    mutation. Enforcement (the warning) is the caller's job
    (:func:`warn_on_resume_into_terminal`), so the detector stays unit-testable.

    Gated on ``is_resume`` — the RESUMED-vs-FRESH signal (a prior run persisted state
    for this deployment). Only a run that actually resumed can be a resume-into-
    terminal; a fresh (first-ever) boot short-circuits here. A fresh boot's default
    state is never terminal so the gate is belt-and-suspenders, but making it explicit
    keeps the guard robust against a strategy that (wrongly) reports terminal without
    a resume. Balance is read only when both gates pass, so a fresh / mid-lifecycle
    boot does no RPC.
    """
    if not (is_resume and _strategy_reports_terminal(strategy)):
        return ResumeTerminalReport(
            is_terminal=False,
            idle_capital_usd=Decimal("0"),
            threshold_usd=_MATERIAL_IDLE_USD,
            fired=False,
        )

    idle = _read_idle_capital_usd(strategy)
    fired = idle >= _MATERIAL_IDLE_USD
    return ResumeTerminalReport(
        is_terminal=True,
        idle_capital_usd=idle,
        threshold_usd=_MATERIAL_IDLE_USD,
        fired=fired,
    )


def warn_on_resume_into_terminal(
    runner: StrategyRunner,
    strategy: StrategyProtocol,
    deployment_id: str,
    *,
    is_resume: bool,
) -> ResumeTerminalReport:
    """Run the resume-into-terminal scan and emit the loud boot signal.

    On a resumed-terminal deployment with non-trivial idle capital, logs a loud,
    actionable WARNING naming the idle capital (so the operator / hosted log viewer
    cannot mistake the no-op run for a healthy one) plus a distinct
    ``RESUMED-TERMINAL`` structured sentinel. ``is_resume`` gates the whole check (see
    :func:`detect_resume_into_terminal`). Deliberately does **not** halt or
    auto-reinitialize (see the module docstring). Returns the report for callers /
    tests to inspect.
    """
    report = detect_resume_into_terminal(runner, strategy, is_resume=is_resume)
    if not report.fired:
        return report

    logger.warning(
        "RESUMED-TERMINAL: deployment %s resumed a COMPLETED lifecycle (all positions "
        "unwound / lifecycle finished) but the wallet holds ~$%s of fresh, UNUSED capital. "
        "The strategy will HOLD for this entire run and no-op that capital. This is a "
        "deterministic-identity resume (deployment_id = sha256(wallet:chain)) onto a terminal "
        "state — NOT a healthy idle. To put the capital to work, reset this deployment's state "
        "(reset_demo_state.py for a reused pool wallet) or redeploy with a new identity "
        "(different wallet). Auto-reinitialize is intentionally NOT done (it could double-open "
        "or re-run a completed lifecycle). [VIB-5887]",
        deployment_id,
        report.idle_capital_usd,
    )
    # Structured single-line sentinel so hosted log tooling can alert on the class.
    logger.warning(
        "ALMANAK_RESUME_TERMINAL deployment_id=%s idle_capital_usd=%s threshold_usd=%s",
        deployment_id,
        report.idle_capital_usd,
        report.threshold_usd,
    )
    return report


__all__ = [
    "ResumeTerminalReport",
    "detect_resume_into_terminal",
    "warn_on_resume_into_terminal",
]
