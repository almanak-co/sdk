"""Boot-time on-chain strand detection (VIB-5419, ToDo-DemoE2E §A2a).

A runner killed in the window between **"tx confirmed on-chain"** and
**"the durable ledger/accounting row is written"** leaves funds inside a
protocol with *zero* DB trace. ``--fresh`` / a plain restart only checks the
wallet balance, never on-chain *positions*, so the strand is invisible: the
strategy boots, its books show nothing, and real money sits unaccounted in a
lending market or a perp. This was confirmed on real mainnet (euler).

This module is the **detect-and-halt** half of the fix (A2a). At boot — after
the StateManager is initialised, the deployment id is resolved, and the gateway
client is wired — it:

1. reads on-chain positions for every connector the strategy uses *that has a
   registered on-chain reader*, via the framework-owned
   :class:`~almanak.framework.valuation.position_discovery.PositionDiscoveryService`
   (the same readers the portfolio valuer uses); and
2. diffs each discovered position against the DB's record of activity for that
   ``(protocol, chain)`` (``transaction_ledger`` + ``accounting_events`` +
   open ``position_registry`` rows).

When the chain shows a position the DB has **no trace of**, that is a strand:
the runner **halts loudly** in ``live`` mode (raising :class:`OnChainStrandError`)
and logs a loud ERROR + continues in ``paper`` / ``dry_run`` (mirroring the
mode-aware boot semantics of state-init / outbox-drain in
``initialize_run_loop``).

**Scope — this is detect-and-halt ONLY.** It never writes, reconstructs, or
"heals" anything; synthetic recovery writes are A2b (separate design:
provenance / idempotency / tx-attribution) and the dormant cross-process WAL is
A2c (hosted-gated). "Halt + operator remediation" is the safe interim the plan
mandates.

**Coverage is honest, NOT uniform.** Drift is only detectable where a
connector publishes an on-chain reader the discovery service drives:

* **Lending (Aave-fork single-reserve reads):** covered — Aave V3, Spark, and
  any fork that publishes a ``LENDING_READ_SPEC`` *and* the strategy declares a
  tracked-token list to scan reserves against.
* **Perps:** covered — every venue in ``PerpsReadRegistry`` (a single
  account-level read returns the whole book).
* **LP / vault / staking / CDP, and lending protocols without a single-reserve
  read (Compound V3, Morpho):** **NOT covered.** LP discovery needs an NFT
  token id that cannot be enumerated at boot (and a strand means the id was
  never recorded); vault/stake have no boot reader. These primitives are
  reported as *drift-undetectable* — surfaced explicitly, never silently passed
  as "clean". Uniform LP/vault coverage waits on the manifest-driven
  ``PositionReadRegistry`` (ToDo §D2). Do not read this module's "no drift" as
  "no strand" for an undetectable primitive.

Gateway-boundary note: this module is strategy-container / framework code. It
performs no network egress of its own — on-chain reads go through the gateway
client (``eth_call``) exactly as the valuer's readers do, and DB reads go
through the existing :class:`StateManager` read methods (so the same code path
works for local SQLite and the hosted gateway-proxied store with no new RPC).
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from almanak.framework.runner.runner_models import StrategyProtocol
    from almanak.framework.runner.strategy_runner import StrategyRunner

logger = logging.getLogger(__name__)

# Upper bound on DB rows pulled to build the "known (protocol, chain)" set. The
# detector only needs *existence* of any trace for a (protocol, chain); it does
# not aggregate amounts. This bound exists because the shared StateManager read
# methods are list-with-limit, not COUNT/EXISTS (a dedicated bounded existence
# query is a future hardening — see PR notes). It is generous enough that the
# documented P0 (a first-ever deposit ⇒ an EMPTY known-set) is always caught
# regardless of the bound; only a strategy with more than this many recorded
# events whose *sole* trace of an open protocol has aged past the bound could be
# missed, which fails toward a (rare) missed-detection, never a false halt.
_DB_TRACE_SCAN_LIMIT = 10_000


class OnChainStrandError(RuntimeError):
    """Boot halt: the chain shows a position the DB has no record of.

    A subclass of :class:`RuntimeError` so it propagates out of
    ``initialize_run_loop`` and aborts the runner exactly like the other
    live-mode boot failures (state-init, outbox-drain). Distinct type so a
    stuck-strategy log is greppable to *this* cause in one step.
    """


@dataclass(frozen=True)
class StrandDiscrepancy:
    """One on-chain position with no corresponding DB record.

    Attributes:
        protocol: Protocol the position lives in (e.g. ``"aave_v3"``).
        chain: Chain the position is on.
        position_type: ``"SUPPLY"`` / ``"BORROW"`` / ``"PERP"`` — the discovered
            primitive family.
        identity: A human-readable position identity (asset symbol for lending,
            position key for perps) for the operator to locate it.
        detail: One-line actionable description.
    """

    protocol: str
    chain: str
    position_type: str
    identity: str
    detail: str


@dataclass
class BootStrandReport:
    """Outcome of a boot-time drift scan.

    Attributes:
        scanned_protocols: Protocols a reader was driven for (drift IS detectable).
        undetectable: ``(protocol, reason)`` pairs whose primitive has no boot
            reader — drift NOT detectable; reported explicitly, never silently
            treated as clean.
        discrepancies: On-chain positions with no DB trace (the strands).
        scan_errors: Non-fatal errors hit while scanning (logged, not silently
            dropped — a failed read is *not* proof of "no position").
    """

    scanned_protocols: list[str] = field(default_factory=list)
    undetectable: list[tuple[str, str]] = field(default_factory=list)
    discrepancies: list[StrandDiscrepancy] = field(default_factory=list)
    scan_errors: list[str] = field(default_factory=list)

    @property
    def has_drift(self) -> bool:
        return bool(self.discrepancies)


def _classify_protocols(
    protocols: list[str], tracked_tokens: list[str]
) -> tuple[list[str], list[str], list[tuple[str, str]]]:
    """Split declared protocols into (lending-scannable, perps-scannable, undetectable).

    Coverage is exactly what the discovery readers can drive at boot:

    * lending-scannable — a connector-owned single-reserve read exists for the
      protocol AND the strategy declares tracked tokens to scan reserves with.
      A declared lending protocol with no tracked tokens is *undetectable* (we
      cannot enumerate which reserves to read).
    * perps-scannable — the protocol has a connector-owned perps read (a single
      account-level read returns the whole book — no token list needed).
    * undetectable — everything else (LP, vault, stake, CDP, Compound V3 /
      Morpho lending without a single-reserve read).
    """
    from almanak.connectors._strategy_base.lending_read_registry import LendingReadRegistry
    from almanak.connectors._strategy_base.perps_read_registry import PerpsReadRegistry
    from almanak.framework.valuation.position_discovery import _lending_protocols_to_scan

    declared = list(dict.fromkeys(protocols))  # de-dup, preserve order
    lending = _lending_protocols_to_scan(declared)
    # Perps scannability is driven by the ACTUAL reader set
    # (``PerpsReadRegistry.canonical`` is non-None only for venues with a
    # registered boot reader), NOT the broader conceptual perp membership
    # (``_has_perps_protocol`` / ``_PERP_PROTOCOLS``). A protocol that is
    # conceptually a perp venue but has no registered reader (e.g.
    # ``pancakeswap_perps``) is never scanned by
    # ``PositionDiscoveryService._discover_perps``, so classifying it scannable
    # would silently assume it clean. It must fall through to ``undetectable``.
    perps = [p for p in declared if PerpsReadRegistry.canonical(p) is not None]

    undetectable: list[tuple[str, str]] = []
    scannable_lending: list[str] = []
    if lending and not tracked_tokens:
        # Lending readers exist but we have nothing to scan reserves against.
        for p in declared:
            if LendingReadRegistry.canonical(p) is not None:
                undetectable.append(
                    (p, "lending reader present but strategy declares no tracked tokens to scan reserves")
                )
    else:
        scannable_lending = lending

    # Canonical keys of the lending protocols we can actually scan.
    lending_canon = {LendingReadRegistry.canonical(p) for p in scannable_lending}
    for p in declared:
        canon = LendingReadRegistry.canonical(p)
        is_lending_scannable = canon is not None and canon in lending_canon
        is_perp_scannable = p in perps
        if is_lending_scannable or is_perp_scannable:
            continue
        if any(p == u[0] for u in undetectable):
            continue
        undetectable.append(
            (p, "no boot-time on-chain reader for this primitive (LP/vault/stake/CDP or non-single-reserve lending)")
        )

    return scannable_lending, perps, undetectable


def _canonical_protocol(protocol: Any) -> str:
    """Canonicalise a protocol slug so the DB-side and discovery-side agree.

    Discovery emits connector-canonical slugs (``aave_v3``, ``gmx_v2``). DB rows
    may carry historical aliases — lending rows normalise on write via
    ``LendingReadRegistry`` but a perp row can persist ``intent.protocol.lower()``
    raw (``perp_accounting`` stores e.g. ``gmx`` while discovery reports the
    canonical ``gmx_v2``). Folding BOTH sides through the registries' alias-aware
    ``canonical`` before comparison stops an alias mismatch from missing the
    known-set lookup and FALSE-halting an already-accounted position at live boot.
    Unknown slugs (LP/vault) pass through lower-cased — they never reach the strand
    loop, so an identity fold is sufficient there.
    """
    from almanak.connectors._strategy_base.lending_read_registry import LendingReadRegistry
    from almanak.connectors._strategy_base.perps_read_registry import PerpsReadRegistry

    raw = str(protocol).strip().lower().replace("-", "_") if protocol else ""
    if not raw:
        return ""
    return LendingReadRegistry.canonical(raw) or PerpsReadRegistry.canonical(raw) or raw


async def _read_authoritative_accounting_trace(
    sm: Any,
    deployment_id: str,
    add: Callable[[Any, Any], None],
    errors: list[str],
) -> bool:
    """Read the AUTHORITATIVE ``accounting_events`` trace into the known-set.

    Returns whether the trace was read MEASURED-complete. Empty ≠ Zero: the
    hosted ``GatewayStateManager`` accounting read is fail-quiet (returns ``[]``
    on a transport error, a structurally-absent backend, or an old gateway), so
    an empty result must NOT be read as "no record" — that would shrink the
    known-set and FALSE-halt an accounted strategy. Prefer the MEASURED read
    (``read_accounting_events_measured``, present on the hosted
    ``GatewayStateManager``) which carries the measured/absent signal end-to-end
    (VIB-5185); fall back to the plain sync read on the local typed
    ``StateManager`` (SQLite is measured-by-construction once
    ``state_manager_ready``). Run the blocking call off the event loop —
    mirroring the ``discover()`` to_thread. No LIMIT (full history); existence is
    all the detector needs.
    """
    measured_reader = getattr(sm, "read_accounting_events_measured", None)
    if callable(measured_reader):
        try:
            rows, measured = await asyncio.to_thread(measured_reader, deployment_id)
        except Exception as exc:  # noqa: BLE001 - non-fatal; recorded, not swallowed
            errors.append(f"measured accounting-events read failed: {exc}")
            logger.debug("Boot strand detection: measured accounting read failed", exc_info=True)
            return False
        if not measured:
            # UNMEASURED: backend absent / errored / old gateway. The caller MUST
            # suppress the halt — an unread trace is not proof of absence.
            errors.append(
                "accounting-events read UNMEASURED (backend absent/errored/old gateway); "
                "lending/perp strand halt SUPPRESSED this boot (Empty != Zero)"
            )
            return False
        for event in rows:
            add(event.get("protocol"), event.get("chain"))
        return True

    events_reader = getattr(sm, "get_accounting_events_sync", None)
    if callable(events_reader):
        try:
            for event in await asyncio.to_thread(events_reader, deployment_id):
                add(event.get("protocol"), event.get("chain"))
        except Exception as exc:  # noqa: BLE001 - non-fatal; recorded, not swallowed
            errors.append(f"accounting-events read failed: {exc}")
            logger.debug("Boot strand detection: accounting-events read failed", exc_info=True)
            return False
        # Local SQLite read is authoritative once initialised (no fail-quiet
        # transport layer): a successful read — empty or not — is complete.
        return True

    # Neither read surface — the authoritative trace is unreadable; leave the
    # known-set incomplete so the caller suppresses the halt.
    errors.append(
        "accounting-events read surface unavailable "
        "(no read_accounting_events_measured / get_accounting_events_sync); "
        "lending/perp strand halt SUPPRESSED this boot"
    )
    return False


async def _build_known_protocol_chains(
    runner: StrategyRunner, deployment_id: str
) -> tuple[set[tuple[str, str]], list[str], bool]:
    """Build the set of ``(protocol, chain)`` the DB has any trace of.

    A position is "known" to the books if there is at least one
    ``transaction_ledger`` row, ``accounting_events`` row, or open
    ``position_registry`` row referencing its ``(protocol, chain)``. The strand
    case (crash between confirm and write) leaves NONE of these for the affected
    protocol — that absence is the signal.

    Returns ``(known_pairs, errors, authoritative_trace_complete)``:

    * ``errors`` — non-fatal read failures (a failed DB read must NOT be silently
      treated as "DB knows everything", which would suppress a real strand — but
      it also must not crash boot).
    * ``authoritative_trace_complete`` — ``True`` only when the AUTHORITATIVE
      lending/perp trace (``accounting_events``) was read MEASURED-complete.
      Empty ≠ Zero: on the hosted ``GatewayStateManager`` the accounting read is
      fail-quiet (returns ``[]`` on a transport error / structurally-absent
      backend / old gateway), so an empty result is NOT proof of "no record".
      When the read is UNMEASURED the caller must SUPPRESS the halt — a shrunken
      known-set would otherwise FALSE-halt a healthy, fully-accounted live
      strategy (the exact "never a false halt" invariant this feature promises).

    **Coarse-match limitation (known):** the known-set keys on ``(protocol,
    chain)`` existence, NOT per-position identity. A deployment that has ANY
    prior trace for ``(aave_v3, ethereum)`` (e.g. a previously-closed supply)
    will treat a NEWLY-stranded position in that same protocol/chain as
    accounted, missing it. This fails toward a (rare) missed-detection, never a
    false halt — consistent with the A2a stance; position-level reconciliation is
    A2b. The documented P0 (a FIRST-ever deposit ⇒ an empty known-set) is always
    caught.
    """
    known: set[tuple[str, str]] = set()
    errors: list[str] = []
    sm = runner.state_manager

    def _add(protocol: Any, chain: Any) -> None:
        canon = _canonical_protocol(protocol)
        if canon and chain:
            known.add((canon, str(chain).strip().lower()))

    # transaction_ledger trace. Async read present on the local ``StateManager``;
    # the hosted ``GatewayStateManager`` does NOT expose it (it carries the
    # authoritative lending/perp trace through ``accounting_events`` below).
    # Treat a STRUCTURALLY-ABSENT reader as a benign secondary (debug only) so it
    # does not raise a per-boot "incomplete coverage" alarm on hosted; treat a
    # reader that EXISTS but raises as a real scan error (never suppress a live
    # fault, which would shrink the known-set and risk a false strand).
    ledger_reader = getattr(sm, "get_ledger_entries", None)
    if callable(ledger_reader):
        try:
            for entry in await ledger_reader(deployment_id, limit=_DB_TRACE_SCAN_LIMIT):
                _add(getattr(entry, "protocol", ""), getattr(entry, "chain", ""))
        except Exception as exc:  # noqa: BLE001 - non-fatal; recorded, not swallowed
            errors.append(f"ledger read failed: {exc}")
            logger.debug("Boot strand detection: ledger read failed", exc_info=True)
    else:
        logger.debug(
            "Boot strand detection: backend exposes no get_ledger_entries; "
            "relying on the accounting_events trace for lending/perp coverage"
        )

    # accounting_events trace — the AUTHORITATIVE lending/perp trace. Extracted so
    # the measured-vs-fail-quiet dispatch (Empty != Zero) lives in one place and
    # keeps this builder under the complexity budget.
    authoritative_trace_complete = await _read_authoritative_accounting_trace(sm, deployment_id, _add, errors)

    # position_registry is a best-effort SECONDARY trace (LP-focused today, and
    # raises CutoverStorageNotSupported on hosted GatewayStateManager). It does
    # NOT gate lending/perp coverage -- ledger + accounting_events above are the
    # authoritative trace for those -- so a read failure here is logged at debug
    # only and does NOT become a scan error (would be per-boot noise on hosted).
    try:
        for row in await sm.get_position_registry_open_rows(deployment_id):
            if not isinstance(row, dict):
                continue
            payload = row.get("payload")
            protocol = payload.get("protocol") if isinstance(payload, dict) else None
            _add(protocol, row.get("chain"))
    except Exception:  # noqa: BLE001 - best-effort secondary source; never fatal
        logger.debug("Boot strand detection: position-registry read unavailable", exc_info=True)

    return known, errors, authoritative_trace_complete


def _strategy_scan_inputs(strategy: StrategyProtocol) -> tuple[str, str, list[str], list[str]]:
    """Pull (chain, wallet, protocols, tracked_tokens) from the strategy.

    Mirrors the same metadata sources the portfolio valuer's
    ``_build_discovery_config`` reads, so detection and valuation agree on what
    the strategy "uses".
    """
    chain = getattr(strategy, "chain", "") or ""
    wallet = getattr(strategy, "wallet_address", "") or ""

    protocols: list[str] = []
    metadata = getattr(strategy, "STRATEGY_METADATA", None)
    if metadata is not None and hasattr(metadata, "supported_protocols"):
        try:
            protocols = list(metadata.supported_protocols)
        except Exception:  # noqa: BLE001 - defensive; metadata is author-supplied
            protocols = []

    tracked_tokens: list[str] = []
    getter = getattr(strategy, "_get_tracked_tokens", None)
    if callable(getter):
        try:
            tracked_tokens = list(getter())
        except Exception:  # noqa: BLE001 - defensive; strategy-supplied
            tracked_tokens = []

    return chain, wallet, protocols, tracked_tokens


async def detect_boot_strands(
    runner: StrategyRunner,
    strategy: StrategyProtocol,
    deployment_id: str,
) -> BootStrandReport:
    """Scan on-chain positions and diff against the DB; never raises.

    Returns a :class:`BootStrandReport`. Enforcement (halt vs loud-continue) is
    the caller's job (:func:`enforce_no_boot_strands`) so the detector stays
    pure and unit-testable.
    """
    from almanak.framework.teardown.models import PositionType
    from almanak.framework.valuation.position_discovery import (
        DiscoveryConfig,
        PositionDiscoveryService,
    )

    report = BootStrandReport()

    chain, wallet, protocols, tracked_tokens = _strategy_scan_inputs(strategy)
    if not chain or not wallet or not protocols:
        # Nothing declared to scan — not an error, just no coverage to assert.
        return report

    scannable_lending, scannable_perps, undetectable = _classify_protocols(protocols, tracked_tokens)
    report.undetectable = undetectable
    report.scanned_protocols = list(dict.fromkeys([*scannable_lending, *scannable_perps]))

    if not report.scanned_protocols:
        # Everything the strategy uses is drift-undetectable (e.g. an LP-only
        # strategy). Report it; there is nothing on-chain we can read here.
        return report

    gateway_client = runner._get_gateway_client()
    if gateway_client is None:
        report.scan_errors.append("gateway client unavailable at boot; on-chain drift scan skipped")
        return report

    config = DiscoveryConfig(
        chain=chain,
        wallet_address=wallet,
        protocols=report.scanned_protocols,
        tracked_tokens=tracked_tokens,
        # No LP token ids at boot — LP drift is explicitly undetectable here.
        lp_token_ids=[],
    )

    service = PositionDiscoveryService(gateway_client)
    try:
        # discover() is a synchronous, gateway-blocking scan; run it off the
        # event loop so a slow RPC at boot doesn't stall the loop.
        discovered = await asyncio.to_thread(service.discover, config)
    except Exception as exc:  # noqa: BLE001 - discover() is documented never-raise, belt-and-braces
        report.scan_errors.append(f"position discovery failed: {exc}")
        logger.debug("Boot strand detection: discovery raised", exc_info=True)
        return report

    report.scan_errors.extend(discovered.errors)

    if not discovered.positions:
        return report

    known, db_errors, authoritative_trace_complete = await _build_known_protocol_chains(runner, deployment_id)
    report.scan_errors.extend(db_errors)

    if not authoritative_trace_complete:
        # The authoritative lending/perp DB trace could not be read
        # measured-complete (hosted backend absent/errored, a transient gRPC blip,
        # or an old gateway). Empty ≠ Zero: an unread trace is NOT proof of "no
        # record", so asserting a strand here would FALSE-halt a healthy,
        # fully-accounted strategy. The loud scan_errors are already recorded;
        # decline to flag drift this boot — failing toward a (rare,
        # operator-visible) missed-detection, never a brick.
        return report

    seen: set[tuple[str, str, str, str]] = set()
    for pos in discovered.positions:
        ptype = pos.position_type
        # Only lending (SUPPLY/BORROW) and perps are read here. LP is never
        # discovered at boot (no token ids), so it cannot reach this loop.
        if ptype not in (PositionType.SUPPLY, PositionType.BORROW, PositionType.PERP):
            continue
        # Canonicalise the discovered protocol through the SAME helper the known-set
        # uses so an alias (gmx vs gmx_v2) never misses the match and false-halts.
        protocol = _canonical_protocol(pos.protocol)
        pos_chain = (pos.chain or chain).lower()
        if (protocol, pos_chain) in known:
            continue  # DB has a trace for this protocol on this chain — not a strand.

        identity = _position_identity(pos)
        key = (protocol, pos_chain, ptype.value, identity)
        if key in seen:
            continue
        seen.add(key)
        report.discrepancies.append(
            StrandDiscrepancy(
                protocol=protocol,
                chain=pos_chain,
                position_type=ptype.value,
                identity=identity,
                detail=(
                    f"on-chain {ptype.value} position in {protocol} on {pos_chain} "
                    f"({identity}) has no transaction_ledger / accounting_events / "
                    f"position_registry record for this deployment"
                ),
            )
        )

    return report


def _position_identity(pos: Any) -> str:
    """Best-effort human identity for a discovered position."""
    details = getattr(pos, "details", {}) or {}
    asset = details.get("asset") or details.get("market") or ""
    pid = getattr(pos, "position_id", "") or ""
    if asset:
        return str(asset)
    return str(pid) or "<unknown>"


async def enforce_no_boot_strands(
    runner: StrategyRunner,
    strategy: StrategyProtocol,
    deployment_id: str,
) -> BootStrandReport:
    """Run boot drift detection and enforce the mode-aware halt.

    * Always logs the *drift-undetectable* primitives loudly (coverage honesty —
      a clean scan is NOT a clean bill of health for those).
    * On drift: logs a loud, actionable ERROR; **raises**
      :class:`OnChainStrandError` in ``live`` mode (aborting boot before the
      strategy can trade on top of an unaccounted position); logs ERROR and
      continues in ``paper`` / ``dry_run``.

    Returns the report (handy for tests / callers that want to inspect it).
    """
    report = await detect_boot_strands(runner, strategy, deployment_id)

    if report.undetectable:
        logger.warning(
            "Boot strand detection: %d primitive(s) are DRIFT-UNDETECTABLE at boot "
            "(no on-chain reader) — a clean scan does NOT clear them: %s",
            len(report.undetectable),
            "; ".join(f"{proto} ({reason})" for proto, reason in report.undetectable),
        )

    if report.scan_errors:
        logger.warning(
            "Boot strand detection: %d non-fatal scan error(s) — coverage may be incomplete this boot: %s",
            len(report.scan_errors),
            "; ".join(report.scan_errors),
        )

    if report.scanned_protocols:
        logger.info(
            "Boot strand detection scanned %s: %d on-chain position(s) without a DB record.",
            ", ".join(report.scanned_protocols),
            len(report.discrepancies),
        )

    if not report.has_drift:
        return report

    lines = "\n".join(f"  - {d.detail}" for d in report.discrepancies)
    message = (
        f"Boot-time on-chain STRAND detected for deployment {deployment_id}: "
        f"{len(report.discrepancies)} position(s) exist on-chain with no DB record "
        f"(crash between tx-confirm and ledger-write, or a pre-existing untracked "
        f"position). The strategy's books cannot account for these funds.\n{lines}\n"
        f"Resolve before running: confirm on-chain, then either close the position "
        f"manually or reconcile the ledger. (A2a is detect-and-halt only; automated "
        f"reconstruction is A2b, not yet shipped.)"
    )

    if runner._is_live_mode():
        logger.error(message)
        raise OnChainStrandError(message)

    logger.error("%s\nContinuing because mode is not 'live' (paper/dry_run).", message)
    return report


__all__ = [
    "BootStrandReport",
    "OnChainStrandError",
    "StrandDiscrepancy",
    "detect_boot_strands",
    "enforce_no_boot_strands",
]
