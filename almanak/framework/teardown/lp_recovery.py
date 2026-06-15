"""Auto-fallback to on-chain LP discovery when strategy state is lost — VIB-5138.

Teardown emits ``LP_CLOSE`` only when the strategy's persisted/in-memory
``_position_id`` is present. On state desync — the NFT is still live on-chain
but ``_position_id`` was lost (often after an ``AccountingPersistenceError``
on LP open) — ``strategy.get_open_positions()`` returns no LP, teardown emits
only a token swap, reports complete, and leaves the Uniswap V3 LP NFT OPEN.
Stranded funds.

The ``--discover`` CLI flag already closes this gap manually by scanning the
NonfungiblePositionManager contracts via the gateway
(``teardown.discovery.discover_lp_positions``). This module reuses that SAME
primitive automatically inside the runner-driven teardown lane.

**Deployment-ownership scoping (fund-safety, VIB-4976).** The on-chain scan is
**wallet-scoped**, and a wallet may be shared across deployments (blueprint 14
§4.5 "Scope caveat"). A naive "close any discovered NFT not in the strategy's
reported set" would close ANOTHER strategy's live LP on the same wallet. So
recovery is scoped to ONLY the token ids attributable to *this* deployment via
its own durable accounting state:

* ``position_registry`` OPEN rows — the LP_OPEN's atomic ledger+registry write
  (T12 cutover) stamps ``payload.token_id`` keyed by ``deployment_id``, and is
  committed BEFORE the typed accounting-event write that raises on desync, so
  it survives the ``AccountingPersistenceError`` (the most robust signal).
* ``position_events`` LP OPEN rows — ``position_id`` IS the NFT token id, keyed
  by ``deployment_id``; written pre- and post-cutover (the legacy fallback).

A discovered NFT with NO attribution to this deployment is NEVER recovered.

This module is intentionally pure (no gateway/IO): the gateway-backed scan
lives in ``teardown.discovery`` and the ownership read lives on
``TeardownRunnerHelpers``; here we only decide *whether* to recover and *how*
to merge the scoped result.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from almanak.framework.teardown.models import PositionInfo, PositionType, TeardownPositionSummary

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DeploymentLpOwnership:
    """LP token ids attributable to a single deployment (VIB-5138).

    Built from the deployment's own durable accounting state (``position_registry``
    OPEN rows + ``position_events`` LP OPEN rows), NEVER from the shared wallet.
    Used to scope on-chain LP discovery so teardown can only ever close
    positions THIS deployment opened — a sibling strategy's live LP on the same
    wallet is not in this set and is never recovered.

    Attributes:
        token_ids: NFT token ids (as strings) this deployment opened.
        had_lp_open: True iff this deployment has ANY LP OPEN attribution on the
            scanned chain. Distinguishes "this deployment never held an LP here"
            (a non-LP strategy — an incomplete scan is benign) from "this
            deployment held an LP and the scan couldn't confirm it closed" (an
            incomplete scan is fatal — an orphan may remain).
        available: True iff at least one attribution source could be read. False
            means BOTH reads failed/unavailable — ownership is unknown, so
            recovery must NOT close anything (we cannot prove ownership).
    """

    token_ids: frozenset[str]
    had_lp_open: bool
    available: bool = True


@dataclass(frozen=True)
class LpDiscoveryResult:
    """Outcome of a bounded on-chain LP discovery scan (VIB-5138).

    Attributes:
        summary: Discovered positions as a ``TeardownPositionSummary``. Empty
            when the scan completed cleanly and the wallet holds no live LP
            NFTs.
        incomplete: True when the scan could NOT enumerate every position the
            NPM reports (strict ``discover_lp_positions`` raised
            ``DiscoveryIncomplete``) or when the scan itself errored.
        error: Human-readable reason when ``incomplete`` is True; ``None``
            otherwise.
    """

    summary: TeardownPositionSummary
    incomplete: bool = False
    error: str | None = None


@dataclass
class LpRecoveryOutcome:
    """Result of merging deployment-owned discovered LP into a teardown.

    Attributes:
        positions: The merged position summary (strategy-reported + net-new
            deployment-owned discovered LP). Identical object to the input when
            nothing was recovered.
        intents: The teardown intent list with recovered ``LP_CLOSE`` intents
            appended. Identical object to the input when nothing was recovered.
        recovered_count: Number of net-new deployment-owned LP positions closed.
        incomplete: True only when the scan was incomplete AND this deployment
            had LP attribution on the chain — i.e. an orphan THIS deployment
            owns may remain open. A blip on an unrelated NPM for a non-LP
            strategy does NOT set this (it degrades to a WARNING instead).
        warning: Loud operator-facing reason when ``incomplete`` is True.
        discovered_ids: Token ids recovered (for logging / tests).
    """

    positions: TeardownPositionSummary
    intents: list[Any]
    recovered_count: int = 0
    incomplete: bool = False
    warning: str | None = None
    discovered_ids: list[str] = field(default_factory=list)


def strategy_reports_lp(positions: TeardownPositionSummary | None) -> bool:
    """True iff the strategy-reported summary already contains an LP position.

    When the strategy still knows about its LP (``_position_id`` intact), the
    normal teardown path is authoritative and discovery is unnecessary — it
    would only re-confirm what the strategy already drives. We only fall back
    to discovery when NO LP is reported (the desync case).
    """
    if positions is None:
        return False
    return any(p.position_type == PositionType.LP for p in positions.positions)


def _lp_position_ids(positions: TeardownPositionSummary | None) -> set[str]:
    """Set of LP position ids (NFT token ids) the summary already covers."""
    if positions is None:
        return set()
    return {str(p.position_id) for p in positions.positions if p.position_type == PositionType.LP}


def merge_discovered_lp(
    *,
    positions: TeardownPositionSummary | None,
    intents: list[Any],
    discovery: LpDiscoveryResult,
    ownership: DeploymentLpOwnership,
    mode: Any,
) -> LpRecoveryOutcome:
    """Merge DEPLOYMENT-OWNED discovered LP into the teardown's positions/intents.

    A discovered NFT is recovered only when its token id is BOTH (a) attributable
    to this deployment (``ownership.token_ids``) AND (b) not already covered by
    the strategy-reported positions (dedupe). Net-new owned positions are appended
    to the summary and get a synthesised ``LPCloseIntent`` — the same intent the
    ``--discover`` CLI flow builds. Discovered NFTs with no attribution to this
    deployment (e.g. a sibling strategy's live LP on a shared wallet) are NEVER
    closed.

    Incomplete-scan honesty (resolves F2/F3): the ``incomplete`` signal is set
    ONLY when the scan was incomplete AND this deployment had LP attribution on
    the chain — a benign blip on an unrelated NPM for a non-LP strategy degrades
    to a WARNING, never a fatal flag.

    Args:
        positions: Strategy-reported (or precomputed) position summary.
        intents: Teardown intents generated so far.
        discovery: Result of the bounded on-chain scan.
        ownership: This deployment's LP token-id attribution (registry + events).
        mode: ``TeardownMode`` — graceful (SOFT) collects fees on close, emergency
            (HARD) skips fee collection (matches the CLI ``--discover`` behaviour).

    Returns:
        ``LpRecoveryOutcome``. When nothing net-new is owned the input objects are
        returned unchanged (still carrying any incomplete signal).
    """
    from almanak.framework.teardown.models import TeardownMode

    # None-safety (Gemini MEDIUM): some strategies/mocks return ``None`` from
    # ``get_open_positions()``. Normalize to an empty summary before any
    # dereference (``positions.deployment_id`` / ``.timestamp`` / ``.positions``
    # below) so a None can never raise AttributeError mid-teardown. The
    # discovered-summary's deployment_id is the best available identifier when
    # the strategy reported nothing.
    if positions is None:
        positions = TeardownPositionSummary.empty(
            getattr(discovery.summary, "deployment_id", "") or "unknown",
        )

    known_ids = _lp_position_ids(positions)
    discovered_positions = list(getattr(discovery.summary, "positions", []) or [])

    # Scope to deployment-owned, not-already-known token ids. A discovered NFT
    # the deployment never opened (sibling strategy on a shared wallet) is
    # excluded here — the core fund-safety gate (F1).
    net_new: list[PositionInfo] = [
        p
        for p in discovered_positions
        if p.position_type == PositionType.LP
        and str(p.position_id) in ownership.token_ids
        and str(p.position_id) not in known_ids
    ]

    # F2/F3: an incomplete scan only blocks/degrades the teardown when THIS
    # deployment is known to have held an LP on this chain. A non-LP strategy
    # (had_lp_open=False) hitting a transient blip on an unrelated NPM degrades
    # to a loud WARNING, never a fatal flag.
    incomplete = bool(discovery.incomplete and ownership.had_lp_open)
    warning: str | None = None
    if discovery.incomplete and incomplete:
        warning = (
            "On-chain LP discovery was INCOMPLETE during teardown recovery"
            + (f": {discovery.error}" if discovery.error else "")
            + ". This deployment is known to have opened an LP on this chain, so a "
            "deployment-owned position may remain open — manual on-chain verification "
            "required before treating this teardown as complete."
        )
        logger.error(warning)
    elif discovery.incomplete:
        logger.warning(
            "On-chain LP discovery was incomplete during teardown recovery (%s), but this "
            "deployment has no LP attribution on this chain — degrading to a warning, not a "
            "block. Unrelated NPM blip; no deployment-owned LP is at risk.",
            discovery.error or "unknown",
        )

    if not net_new:
        return LpRecoveryOutcome(
            positions=positions,
            intents=intents,
            recovered_count=0,
            incomplete=incomplete,
            warning=warning,
            discovered_ids=[],
        )

    from almanak.framework.intents.vocabulary import LPCloseIntent

    collect_fees_default = mode == TeardownMode.SOFT
    recovered_intents = [
        LPCloseIntent(
            position_id=p.position_id,
            protocol=p.protocol,
            chain=p.chain,
            collect_fees=collect_fees_default,
        )
        for p in net_new
    ]

    merged_positions = TeardownPositionSummary(
        deployment_id=positions.deployment_id,
        timestamp=positions.timestamp,
        positions=list(positions.positions) + net_new,
    )

    recovered_ids = [str(p.position_id) for p in net_new]
    logger.warning(
        "Teardown auto-recovery: strategy reported no LP but on-chain discovery found "
        "%d deployment-owned orphaned position(s) — appending LP_CLOSE for token id(s) %s. "
        "These will be closed and committed through the normal teardown accounting lane.",
        len(net_new),
        recovered_ids,
    )

    return LpRecoveryOutcome(
        positions=merged_positions,
        intents=list(intents) + recovered_intents,
        recovered_count=len(net_new),
        incomplete=incomplete,
        warning=warning,
        discovered_ids=recovered_ids,
    )


__all__ = [
    "DeploymentLpOwnership",
    "LpDiscoveryResult",
    "LpRecoveryOutcome",
    "merge_discovered_lp",
    "strategy_reports_lp",
]
