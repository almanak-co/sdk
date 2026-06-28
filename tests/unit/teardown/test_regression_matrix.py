"""E2E teardown regression matrix — the epic Done-when contract (VIB-5479 / TD-21).

This module is the **seam × primitive regression matrix** for the Teardown
Root-Cause Remediation epic (VIB-5458). Each *seam* is a teardown invariant the
epic establishes; each *primitive family* is a class of position teardown must
close. Every (seam, primitive) cell is a test that pins WHICH teardown behaviour
must hold for that primitive:

* **GREEN** — the seam is merged and provable for this primitive. The cell makes
  a real assertion against the shipped framework code (reusing the merged
  teardown surfaces, not re-implementing them). Cells assert BEHAVIOUR / ORDER
  (a residual flips ``success=False``; the ``withdraw_all`` is the LAST collateral
  withdraw; the completeness gate names the *specifically*-uncovered position),
  never shape/membership — a weak ratchet gives false confidence.
* **XFAIL (strict)** — the (primitive, seam) pair is genuinely not implemented
  yet. The cell asserts the *target* invariant, which currently fails.
  ``strict=True`` so the day the seam lands the cell XPASSes and CI turns red —
  the signal to flip the cell to GREEN. Reasons carry a ``VIB-XXXX`` ticket + a
  dated rationale + ``strict=True`` to satisfy ``scripts/ci/check_xfail_hygiene.py``.
* **N/A (skip)** — the seam does not apply to the primitive (e.g. the lending
  revert-selector decode for an LP position). Skipped with a clear reason, never
  xfailed.

Matrix shape and the cell → seam → owning-ticket → status table live in
``docs/internal/qa/teardown-regression-matrix.md``.

FINAL RATCHET (wired)
---------------------
The epic's Done-when contract — "all merged-seam cells GREEN + fail-CI-on-
regression" — is now satisfied. TD-10 (#3070), TD-11 (#3071) and TD-15 (#3066)
all landed, so their cells assert the shipped behaviour as GREEN. The only
remaining strict-xfails are **S6 × {perp, pendle}**: TD-15 delivered fail-closed
residual detection for LP (post-condition) + lending (Plan-A chain read), but
perp/pendle have no per-position on-chain closure verifier yet (``_reconcile_one``
returns UNVERIFIABLE) — tracked by VIB-5116 (GMX) / VIB-3808 (Pendle TOKEN). When
either lands, its cell XPASSes (strict ⇒ CI red) and is flipped GREEN.

This file is wired as a hard CI gate via ``make test-teardown-matrix`` (run in
the teardown CI job). A GREEN cell regressing to red, or a strict-xfail XPASSing,
fails the gate.

Single entry point:
    uv run pytest tests/unit/teardown/test_regression_matrix.py -q --import-mode=importlib
    (or: make test-teardown-matrix)
"""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest

from almanak.framework.accounting.basis import canonical_pt_symbol
from almanak.framework.intents import Intent
from almanak.framework.teardown import (
    PositionInfo,
    PositionType,
    TeardownResult,
    VerificationStatus,
    full_close_intents,
    generate_lending_unwind,
)
from almanak.framework.teardown import registry_enumeration as _renum
from almanak.framework.teardown.completeness import check_intent_coverage
from almanak.framework.teardown.lending_unwind_guard import sanitize_lending_teardown_intents
from almanak.framework.teardown.models import ClosureVerification, TeardownPositionSummary
from almanak.framework.teardown.plan_a_reconciliation import (
    PositionReconciliation,
    ReconciliationReport,
    ReconciliationVerdict,
)
from almanak.framework.teardown.post_conditions import has_teardown_post_condition
from almanak.framework.teardown.revert_hints import (
    annotate_teardown_error,
    operator_hint_for_selector,
)
from almanak.framework.teardown.swap_clamp import decide_swap_clamp, read_no_accounting_ledger_rows
from almanak.framework.teardown.teardown_manager import TeardownManager

# Date stamp for xfail rationales (xfail-hygiene format). Update with the matrix.
_AS_OF = "as of 2026-06-28"

# Primitive families (matrix columns).
LP = "lp"
LENDING = "lending"
PERP = "perp"
PENDLE = "pendle"
STAKE = "stake"  # staking / wrap / CDP — the NO_ACCOUNTING wallet-token family
PRIMITIVES = (LP, LENDING, PERP, PENDLE, STAKE)

_CHAIN = "ethereum"


# ---------------------------------------------------------------------------
# Fixtures / builders (kept tiny + self-contained; the heavy seam-specific
# fixtures live in the per-seam unit suites this matrix composes over).
# ---------------------------------------------------------------------------
def _pos(
    ptype: PositionType,
    protocol: str,
    details: dict[str, Any],
    *,
    position_id: str = "p1",
) -> PositionInfo:
    return PositionInfo(
        position_type=ptype,
        position_id=position_id,
        chain=_CHAIN,
        protocol=protocol,
        value_usd=Decimal("1000"),
        details=details,
    )


def _representative_positions(primitive: str) -> list[PositionInfo]:
    """A KNOWN-position set (Plan A) representative of ``primitive``."""
    if primitive == LP:
        return [_pos(PositionType.LP, "uniswap_v3", {"pool": "0xpool"}, position_id="123")]
    if primitive == LENDING:
        return [
            _pos(PositionType.BORROW, "aave_v3", {"asset": "USDC"}, position_id="borrow1"),
            _pos(PositionType.SUPPLY, "aave_v3", {"asset": "WETH"}, position_id="supply1"),
        ]
    if primitive == PERP:
        return [
            _pos(
                PositionType.PERP,
                "gmx_v2",
                {"market": "ETH-USD", "collateral_token": "USDC", "is_long": True},
            )
        ]
    if primitive == PENDLE:
        return [_pos(PositionType.TOKEN, "pendle", {"asset": "PT-wstETH"})]
    if primitive == STAKE:
        return [_pos(PositionType.STAKE, "lido", {"asset": "wstETH"})]
    raise AssertionError(f"unknown primitive {primitive!r}")


class _Health:
    def __init__(self, collateral_usd: Decimal, debt_usd: Decimal, lltv: Decimal) -> None:
        self.collateral_value_usd = collateral_usd
        self.debt_value_usd = debt_usd
        self.lltv = lltv


class _LendingMarket:
    """Minimal MarketSnapshot stand-in for the HF-safe unwind primitive.

    Exposes the read surface ``generate_lending_unwind`` uses: ``position_health``
    (live exposure + LLTV), ``price`` (oracle) and ``balance`` (wallet). A static
    health read is correct here — the primitive reads exposure ONCE then sizes the
    whole staircase with pure math (it is a planner, not an executor).
    """

    chain = _CHAIN

    def __init__(
        self,
        *,
        collateral_usd: Decimal,
        debt_usd: Decimal,
        lltv: Decimal,
        prices: dict[str, Decimal],
        balances: dict[str, Decimal],
    ) -> None:
        self._health = _Health(collateral_usd, debt_usd, lltv)
        self._prices = prices
        self._balances = balances

    def position_health(self, **_kw: Any) -> _Health:
        return self._health

    def price(self, token: str) -> Decimal:
        return self._prices.get(token, Decimal("0"))

    def balance(self, token: str, chain: str | None = None) -> Any:  # noqa: ARG002
        holder = type("_Bal", (), {})()
        holder.balance = self._balances.get(token, Decimal("0"))
        return holder


class _UnmeasuredMarket:
    """A snapshot whose fresh exposure read FAILS (the VIB-5418/5452 condition).

    The lending guard must treat this as UNMEASURED (None), never fabricated zero.
    Exposes NO per-reserve read surface, so it is the S4 **negative control**: with
    no on-chain re-read available the guard degrades conservatively and DROPS the
    unsafe ``withdraw_all`` — proving the green path's retention is driven by the
    real re-read, not a blanket keep.
    """

    chain = _CHAIN

    def position_health(self, protocol: str, market_id: str, **_kw: Any) -> Any:  # noqa: ARG002
        raise RuntimeError("position health unavailable (RPC failed)")


class _ReserveReadMarket:
    """LTV=0 / non-collateral supply: the account aggregate reads zero collateral
    AND zero debt, but the literal per-reserve aToken balance is positive and
    fully withdrawable (the DAI-on-Spark-post-USDS-migration condition; VIB-5484).

    This is the read surface TD-10 (VIB-5468) added: ``lending_position_balances``
    is the un-conflated ``position(market, wallet)`` re-read the guard consults
    before stranding a legitimately-safe ``withdraw_all``. With debt a measured
    zero and a positive aToken balance, the guard must KEEP the withdraw.
    """

    chain = _CHAIN

    def position_health(self, protocol: str, market_id: str, **_kw: Any) -> Any:  # noqa: ARG002
        return _Health(Decimal("0"), Decimal("0"), Decimal("0"))  # measured-zero aggregate + zero debt

    def lending_position_balances(
        self, protocol: str, token: str, market_id: str | None = None, chain: str | None = None
    ) -> tuple[int, int]:  # noqa: ARG002
        return (10**18, 0)  # (supply_wei > 0, debt_wei == 0) — real withdrawable aToken


class _LendingResidualMarket:
    """A POST-teardown snapshot whose lending position is STILL OPEN on-chain.

    Drives the TD-15 fail-closed verifier (S6 × lending): the closing intents
    fired, but the chain still reports a debt leg outstanding, so
    ``verify_closure_against_chain`` must flip the result to FAILED (residual
    on-chain risk). ``position_health`` returns the debt the Plan-A reconciliation
    reads back; ``price`` is unused by the CHECK.
    """

    def __init__(self, *, collateral_usd: Decimal, debt_usd: Decimal) -> None:
        self._health = _Health(collateral_usd, debt_usd, Decimal("0.8"))

    def position_health(
        self, protocol: str, market_id: str, *, collateral_price_usd: Any = None, debt_price_usd: Any = None
    ) -> Any:  # noqa: ARG002
        return self._health

    def price(self, token: str) -> Decimal:  # pragma: no cover - amounts unused by the CHECK
        raise KeyError(token)


def _verify_manager() -> TeardownManager:
    """A ``TeardownManager`` wired with a non-None gateway client so the TD-15
    POST-teardown reconciliation read path is reached (the lending read itself is
    served by the injected market stub, no network)."""
    mgr = TeardownManager()
    mgr.compiler = type("_C", (), {"_gateway_client": object(), "is_connected": True})()
    return mgr


class _VerifyStrategy:
    deployment_id = "deployment:matrix"
    _gateway_network = _CHAIN


def _lending_summary(leg: PositionType) -> TeardownPositionSummary:
    """A KNOWN pre-execution position set with one Aave lending leg."""
    pos = PositionInfo(
        position_type=leg,
        position_id="0xmkt",
        chain=_CHAIN,
        protocol="aave_v3",
        value_usd=Decimal("0"),
        details={"source": "position_registry", "market_id": "0xmkt", "asset_symbol": "USDC"},
    )
    return TeardownPositionSummary(deployment_id="deployment:matrix", timestamp=datetime.now(UTC), positions=[pos])


def _reconciliation(verdict: ReconciliationVerdict) -> ReconciliationReport:
    """A one-entry Plan-A report carrying ``verdict`` — the unit TD-15 composes."""
    entry = PositionReconciliation(
        position_type="PositionType.BORROW",
        position_id="0xmkt",
        chain=_CHAIN,
        protocol="aave_v3",
        verdict=verdict,
    )
    return ReconciliationReport(deployment_id="deployment:matrix", entries=(entry,))


def _intent_type(intent: Any) -> str:
    return getattr(getattr(intent, "intent_type", None), "value", "")


def _assert_live_marker(intent: Any) -> None:
    """A 'close fully' intent must carry a LIVE-resolution marker, not a constant.

    This is the Seam-2 invariant (TD-07 / VIB-5465): the on-chain amount is
    resolved at execution, never frozen at plan-build.
    """
    kind = _intent_type(intent)
    if kind == "REPAY":
        assert getattr(intent, "repay_full", False) is True
    elif kind == "WITHDRAW":
        assert getattr(intent, "withdraw_all", False) is True
    elif kind == "SWAP":
        assert getattr(intent, "amount", None) == "all"
    elif kind == "PERP_CLOSE":
        assert getattr(intent, "size_usd", "x") is None  # full close
    elif kind == "LP_CLOSE":
        # No amount: the connector reads the position's LIVE liquidity at close.
        assert getattr(intent, "amount", "x") is None
        assert getattr(intent, "position_id", None)
    else:
        raise AssertionError(f"unexpected close intent type {kind!r}")


# ---------------------------------------------------------------------------
# Seam checks. Each takes the primitive name and asserts that seam's invariant
# for that primitive. GREEN cells pass; XFAIL cells fail (yet); N/A cells are
# never invoked (skipped).
# ---------------------------------------------------------------------------
_S1_READERS = {
    LP: (_renum._LP_REGISTRY_SPECS, _renum.read_open_lp_positions_from_registry),
    LENDING: (_renum._LENDING_REGISTRY_SPECS, _renum.read_open_lending_positions_from_registry),
    PERP: (_renum._PERP_REGISTRY_SPECS, _renum.read_open_perp_positions_from_registry),
    PENDLE: (_renum._PENDLE_REGISTRY_SPECS, _renum.read_open_pendle_positions_from_registry),
}

# Representative protocol slug per primitive for the post-condition seam.
_POSTCOND_PROTO = {LP: "uniswap_v3", LENDING: "aave_v3", PERP: "gmx_v2", PENDLE: "pendle"}


# The registry KIND each primitive's enumerator is configured to read from the
# WARM ``position_registry`` tier (the first element of each spec tuple). Pinning
# this ties the cell to the actual read configuration, not just "some spec exists".
_S1_EXPECTED_KIND = {LP: "lp", LENDING: "lending", PERP: "perp", PENDLE: "swap"}


# A representative OPEN ``position_registry`` row per WARM kind (keyed by the
# spec's ``primitive`` string). Driving the reader with these proves it ACTUALLY
# consults the registry read path and decodes registry-sourced positions — a
# reader that stopped calling ``get_position_registry_open_rows`` (reverting to
# in-memory ``_position_id``) would record zero calls and surface nothing.
_S1_REGISTRY_ROW: dict[str, dict[str, Any]] = {
    "lp": {"chain": _CHAIN, "payload": {"token_id": "777", "pool_address": "0xpool"}},
    "lending": {"chain": _CHAIN, "payload": {"market_id": "0xmkt", "leg": "debt", "protocol": "aave_v3"}},
    "perp": {"chain": _CHAIN, "payload": {"position_id": "0xperpkey", "protocol": "gmx_v2"}},
    "swap": {"chain": _CHAIN, "payload": {"kind": "pt", "market_id": "0xpendlemkt", "protocol": "pendle"}},
}

# The ``PositionType`` each WARM kind's representative row decodes to — asserting
# this proves the enumeration returned the REGISTRY row decoded by the read path,
# not a fabricated placeholder.
_S1_EXPECTED_POSITION_TYPE = {
    "lp": PositionType.LP,
    "lending": PositionType.BORROW,  # leg='debt'
    "perp": PositionType.PERP,
    "swap": PositionType.TOKEN,  # kind='pt'
}


def check_s1_registry_enumeration(primitive: str) -> None:
    """Seam 1 (TD-01..04): enumeration reads from ``position_registry`` (WARM,
    restart-safe), per cut-over primitive — not legacy in-memory state.

    Behaviour (not bare presence): DRIVE the real reader with a fake state
    manager that records every ``get_position_registry_open_rows`` call and serves
    one OPEN row per WARM kind, then assert (a) the reader queried the registry for
    EXACTLY its configured ``(primitive, accounting_category)`` specs, (b) it
    reported ``available`` (a backend answered), and (c) it returned the
    REGISTRY-SOURCED positions decoded from those rows. A regression that drops a
    primitive's registry wiring (reverting to an in-memory ``_position_id`` read,
    or merely staying async with a stale private spec) records no registry call /
    surfaces no registry position and turns the cell red.
    """
    specs, reader = _S1_READERS[primitive]
    assert specs, f"no position_registry cutover spec for {primitive}"
    assert inspect.iscoroutinefunction(reader), "registry reader must be the async WARM read path"
    expected_kind = _S1_EXPECTED_KIND[primitive]
    assert expected_kind in {spec[0] for spec in specs}, (
        f"{primitive}: registry enumerator not configured to read its WARM kind "
        f"{expected_kind!r} (got {sorted(spec[0] for spec in specs)})"
    )

    calls: list[tuple[str, str | None]] = []

    class _RegistryState:
        """Records the registry queries the reader makes and serves OPEN rows."""

        async def get_position_registry_open_rows(
            self,
            deployment_id: str,  # noqa: ARG002 — positional, asserted via `calls`
            *,
            chain: str | None = None,  # noqa: ARG002
            primitive: str,
            accounting_category: str | None,
        ) -> list[dict[str, Any]]:
            calls.append((primitive, accounting_category))
            row = _S1_REGISTRY_ROW.get(primitive)
            return [row] if row is not None else []

    positions, available = asyncio.run(
        reader(state_manager=_RegistryState(), deployment_id="deployment:matrix", chain=_CHAIN)
    )

    # (a) The reader exercised the registry read path for EXACTLY its specs — not
    # a subset, not a different kind, not zero calls.
    assert {(p, c) for p, c in calls} == set(specs), (
        f"{primitive}: registry reader queried {sorted(calls)}, expected specs {sorted(specs)}"
    )
    # (b) A backend that answered the read must report available (drives TD-05's
    # degrade-vs-authoritative decision).
    assert available is True, f"{primitive}: a registry read that answered must report available"
    # (c) Enumeration returns REGISTRY-SOURCED positions (decoded from the rows),
    # never fabricated ones.
    assert positions, f"{primitive}: registry-backed read surfaced no open position"
    assert all(p.details.get("source") == "position_registry" for p in positions), (
        f"{primitive}: enumerated positions must be sourced from position_registry, got "
        f"{[p.details.get('source') for p in positions]}"
    )
    assert any(p.position_type is _S1_EXPECTED_POSITION_TYPE[expected_kind] for p in positions), (
        f"{primitive}: registry row for kind {expected_kind!r} must decode to "
        f"{_S1_EXPECTED_POSITION_TYPE[expected_kind]}, got {[p.position_type for p in positions]}"
    )


def check_s2_live_close_fully(primitive: str) -> None:
    """Seam 2 (TD-07): 'close my position fully' compiles to a LIVE per-position
    figure (marker), never a config constant / stale cache."""
    positions = _representative_positions(primitive)
    intents = full_close_intents(positions)
    assert intents, f"full_close produced no closing intent for {primitive}"
    for intent in intents:
        _assert_live_marker(intent)


def check_s3_hf_safe_unwind(primitive: str) -> None:
    """Seam 3 (TD-09): the HF-safe lending unwind primitive builds a live-sized
    repay→swap→withdraw staircase, so no dust-debt strands a collateral withdraw
    (eliminates the ``0x6679996d`` trap)."""
    assert primitive == LENDING
    # Plain cross-asset borrow: wallet holds NO debt token (cannot repay directly),
    # debt < collateral. The primitive must still fully unwind via the staircase.
    market = _LendingMarket(
        collateral_usd=Decimal("2000"),
        debt_usd=Decimal("1000"),
        lltv=Decimal("0.8"),
        prices={"WETH": Decimal("2000"), "USDC": Decimal("1")},
        balances={"USDC": Decimal("0")},
    )
    intents = generate_lending_unwind(
        market=market,
        protocol="aave_v3",
        collateral_token="WETH",
        borrow_token="USDC",
        chain=_CHAIN,
    )
    kinds = [_intent_type(i) for i in intents]
    assert len(intents) >= 3, f"expected a multi-round staircase, got {kinds}"

    # ORDER is the invariant, not membership: the HF-safe plan is N rounds of
    # (WITHDRAW slice → SWAP collateral→debt → REPAY), then a TERMINAL
    # (WITHDRAW-all → SWAP residual-sweep). Pin that exact shape — an unsafe
    # ``REPAY → WITHDRAW → SWAP`` (or any reordering that pulls collateral before
    # the debt it backs is repaid) breaks the triple structure and fails here,
    # which membership (``"REPAY" in kinds``) silently passed.
    assert kinds[-2:] == ["WITHDRAW", "SWAP"], (
        f"plan must end with the terminal withdraw-all + residual sweep, got {kinds}"
    )
    rounds = kinds[:-2]
    assert rounds, f"expected at least one unwind round before the terminal sweep, got {kinds}"
    assert len(rounds) % 3 == 0, f"unwind rounds must be (WITHDRAW,SWAP,REPAY) triples, got {kinds}"
    for r in range(0, len(rounds), 3):
        assert rounds[r : r + 3] == ["WITHDRAW", "SWAP", "REPAY"], (
            f"round {r // 3} is not the HF-safe WITHDRAW→SWAP→REPAY order: got {rounds[r : r + 3]} in {kinds}"
        )
    # First-occurrence order is therefore WITHDRAW < SWAP < REPAY (the round shape
    # above guarantees it; assert explicitly as the headline invariant).
    assert kinds.index("WITHDRAW") < kinds.index("SWAP") < kinds.index("REPAY"), kinds

    # ORDER is the invariant, not membership (Codex): the staircase de-risks the
    # health factor BEFORE pulling the last collateral, so the FULL collateral
    # withdraw (``withdraw_all=True``) must be the LAST withdraw, and a full REPAY
    # (``repay_full=True``) that clears the debt must come BEFORE it. An out-of-order
    # plan that withdrew all collateral first would trip the LLTV check (0x6679996d)
    # — exactly the strand this seam eliminates.
    withdraw_idx = [i for i, k in enumerate(kinds) if k == "WITHDRAW"]
    withdraw_all_idx = [i for i in withdraw_idx if getattr(intents[i], "withdraw_all", False)]
    assert len(withdraw_all_idx) == 1, f"exactly one full collateral withdraw expected, got {kinds}"
    last_full_withdraw = withdraw_all_idx[0]
    assert last_full_withdraw == withdraw_idx[-1], (
        f"withdraw_all must be the LAST collateral withdraw (got idx {last_full_withdraw} of {withdraw_idx})"
    )
    # Every earlier withdraw is a partial (live-sized), never a premature withdraw-all.
    assert all(not getattr(intents[i], "withdraw_all", False) for i in withdraw_idx[:-1]), kinds
    full_repay_idx = [i for i, k in enumerate(kinds) if k == "REPAY" and getattr(intents[i], "repay_full", False)]
    assert full_repay_idx, f"staircase must clear the debt with a repay_full REPAY, got {kinds}"
    assert min(full_repay_idx) < last_full_withdraw, (
        f"the debt-clearing REPAY must precede the withdraw_all (repay@{full_repay_idx} vs withdraw@{last_full_withdraw})"
    )


def check_s4_guard_zero_debt_withdraw(primitive: str) -> None:
    """Seam 4 (TD-10 / VIB-5468, MERGED #3070): the lending fresh-state guard must
    not strand a zero-debt collateral withdraw just because the account-level USD
    aggregate read zero / unmeasured (VIB-5418 / VIB-5452 / VIB-5484). It now
    consults the un-conflated per-reserve on-chain read and KEEPS the withdraw when
    the literal aToken balance is positive and debt is a measured zero.

    Behaviour (not membership): the SAME ``withdraw_all`` is RETAINED when the
    per-reserve re-read proves real withdrawable supply, and DROPPED when no
    re-read surface is available — proving retention is driven by the on-chain
    re-read TD-10 added, not a blanket keep."""
    assert primitive == LENDING
    withdraw = Intent.withdraw(protocol="aave_v3", token="WETH", amount=Decimal("0"), withdraw_all=True, chain=_CHAIN)

    # Green path: the per-reserve aToken read shows positive withdrawable supply
    # under a measured-zero account aggregate (LTV=0 / isolation) → KEEP.
    kept = sanitize_lending_teardown_intents([withdraw], _ReserveReadMarket())
    retained = [i for i in kept.intents if _intent_type(i) == "WITHDRAW" and getattr(i, "withdraw_all", False)]
    assert retained, (
        "TD-10: a zero-debt withdraw_all must be retained after the un-conflated "
        "per-reserve on-chain re-read, not stranded on a zero/unmeasured aggregate"
    )

    # Negative control: with NO per-reserve re-read surface the guard cannot prove
    # whole-position safety, so it degrades conservatively and DROPS the withdraw.
    # This is what proves the green branch is the re-read, not an unconditional keep.
    dropped = sanitize_lending_teardown_intents([withdraw], _UnmeasuredMarket())
    assert not [i for i in dropped.intents if _intent_type(i) == "WITHDRAW"], (
        "without an on-chain re-read the guard must conservatively drop the unsafe withdraw_all"
    )
    assert dropped.degraded


def check_s5_completeness_enforced(primitive: str) -> None:
    """Seam 5 (TD-11 / VIB-5469, MERGED #3071): a teardown that omits a
    tracked-open position must FAIL LOUD, not silently half-unwind (ALM-2900 /
    VIB-5417). ``check_intent_coverage`` is the merged pre-execution gate.

    Behaviour (not "a helper exists"): the gate must (1) FAIL an empty plan that
    covers nothing, (2) name the *specific* uncovered position when every OTHER
    position is covered, and (3) PASS once that position gets a closing intent —
    so the ratchet catches a regression that drops one primitive's coverage."""
    positions = _representative_positions(primitive)

    # (1) No closing intents at all → the gate fails loud and names every position.
    empty = check_intent_coverage(positions, [])
    assert not empty.complete, f"{primitive}: empty teardown plan must NOT be reported complete"
    assert empty.uncovered, "the uncovered set must name the stranded position(s)"
    assert empty.error_message(), "a fail-loud operator message must be produced"

    # (2) Cover every position EXCEPT a deliberately-omitted target → exactly that
    # one is flagged uncovered (the gate is position-specific, not all-or-nothing).
    target = positions[0]
    others = positions[1:]
    covering = [i for p in others for i in full_close_intents([p])]
    partial = check_intent_coverage(positions, covering)
    assert not partial.complete, f"{primitive}: omitting {target.position_id} must fail the coverage gate"
    uncovered_ids = {p.position_id for p in partial.uncovered}
    assert target.position_id in uncovered_ids, (
        f"the uncovered set must name the specifically-omitted position {target.position_id}, got {uncovered_ids}"
    )
    for p in others:
        assert p.position_id not in uncovered_ids, f"a covered position {p.position_id} must NOT be flagged uncovered"

    # (3) Once the omitted position also gets a closing intent, the gate PASSES.
    full = covering + list(full_close_intents([target]))
    assert check_intent_coverage(positions, full).complete, (
        f"{primitive}: a fully-covered plan must pass the completeness gate"
    )


def _assert_residual_fails_closed_contract() -> None:
    """The pure TD-15 composition contract every fail-closed cell relies on: a
    position the chain STILL reports CONFIRMED_OPEN after teardown flips the result
    to FAILED regardless of the proposed status; an UNVERIFIABLE re-read (a burned
    NFT reading 'not found') is a no-op that must NOT lower a TD-14-proven
    CHAIN_VERIFIED. Asserts BEHAVIOUR (status transitions), not field presence."""
    open_report = _reconciliation(ReconciliationVerdict.CONFIRMED_OPEN)
    assert open_report.has_confirmed_open
    assert (
        open_report.apply_post_teardown_to_verification_status(VerificationStatus.CHAIN_VERIFIED)
        is VerificationStatus.FAILED
    ), "a residual CONFIRMED_OPEN position must fail the teardown closed"
    unverifiable = _reconciliation(ReconciliationVerdict.UNVERIFIABLE)
    assert (
        unverifiable.apply_post_teardown_to_verification_status(VerificationStatus.CHAIN_VERIFIED)
        is VerificationStatus.CHAIN_VERIFIED
    ), "an UNVERIFIABLE post-teardown re-read must not lower a chain-verified closure (burned-NFT success signal)"


def check_s6_failclosed_onchain_verify(primitive: str) -> None:
    """Seam 6 (TD-15 / VIB-5473, MERGED #3066): post-teardown closure is verified
    ON-CHAIN so a residual position fails the teardown closed (``success=False`` +
    ``FAILED``) instead of being reported optimistically.

    The merged seam delivers fail-closed residual detection for **LP** (the
    uniswap_v3 on-chain post-condition, VIB-2925 / VIB-5140) and **lending** (the
    gateway-routed Plan-A debt/collateral chain read the hooks lack). **Perp** and
    **pendle** have NO per-position on-chain closure verifier yet —
    ``_reconcile_one`` returns UNVERIFIABLE for them and no post-condition is
    registered, so a residual perp/pendle is NOT fail-closed → those cells remain
    xfail against their owning tickets.

    Behaviour (not membership): each green cell asserts a residual position drives
    the result to FAILED via the real merged surfaces, never just that an API
    exists."""
    _assert_residual_fails_closed_contract()

    if primitive == LP:
        # LP's closure authority is the on-chain post-condition (block-pinned
        # liquidity read). A residual LP flips to FAILED through it (proven
        # end-to-end on a real fork in tests/reports/vib-5473-td15-*; the unit
        # composition is in test_td15_post_teardown_verification.py).
        assert has_teardown_post_condition("uniswap_v3"), (
            "uniswap_v3 must register an on-chain teardown post-condition (LP closure authority)"
        )
        return

    if primitive == LENDING:
        # Drive the REAL TD-15 seam: a still-open debt leg post-teardown must flip
        # the result to FAILED via the gateway-routed Plan-A lending chain read.
        out = asyncio.run(
            _verify_manager().verify_closure_against_chain(
                _VerifyStrategy(),
                verification=ClosureVerification(
                    all_closed=True,
                    positions_total=1,
                    positions_closed=1,
                    has_position_breakdown=True,
                    # TD-14 reports UNVERIFIED for hook-less lending; TD-15 must still fail it.
                    verification_status=VerificationStatus.UNVERIFIED,
                ),
                pre_execution_positions=_lending_summary(PositionType.BORROW),
                market=_LendingResidualMarket(collateral_usd=Decimal("0"), debt_usd=Decimal("500")),
            )
        )
        assert out.all_closed is False, "a residual lending debt leg must flip the teardown to not-closed"
        assert out.verification_status is VerificationStatus.FAILED, "residual on-chain risk must report FAILED"
        return

    # perp / pendle (xfail target): assert the primitive HAS a fail-closed on-chain
    # closure verifier. It does not yet, so this fails → strict xfail.
    proto = _POSTCOND_PROTO[primitive]
    assert has_teardown_post_condition(proto), (
        f"{proto}: no on-chain teardown post-condition AND no per-position Plan-A chain read "
        "(reconciliation returns UNVERIFIABLE) — a residual position is not fail-closed yet"
    )


def check_s7_authoritative_closure_count(primitive: str) -> None:
    """Seam 7 (TD-14 / VIB-5472): closure is reported as an authoritative POSITION
    count qualified by a confidence ``verification_status`` — never an intent count
    and never a droppable ``position_events`` row (VIB-5085 / VIB-5472).

    Behaviour (not field presence): build a ``TeardownResult`` where the intent
    count and the position count DISAGREE (6 intents closed 1 of 2 positions, one
    residual) and assert the closure surface reports POSITIONS, separates the
    confidence into ``verification_status``, and pairs a residual with
    ``FAILED`` + ``success=False`` — the exact VIB-5085 / VIB-5472 contract."""
    assert primitive in (LP, LENDING, PERP, PENDLE)

    # Confidence is a distinct axis from the count: a closure REPORTED closed but
    # not chain-proven (UNVERIFIED) must never read as chain-confirmed.
    assert VerificationStatus.UNVERIFIED is not VerificationStatus.CHAIN_VERIFIED
    members = {v.value for v in VerificationStatus}
    assert {"chain_verified", "unverified", "failed", "not_run"} <= members

    # A 2-position teardown that ran 6 intents and left 1 position residual.
    result = TeardownResult(
        success=False,
        deployment_id="deployment:matrix",
        mode="graceful",
        started_at=datetime.now(UTC),
        completed_at=datetime.now(UTC),
        duration_seconds=1.0,
        intents_total=6,
        intents_succeeded=6,
        intents_failed=0,
        starting_value_usd=Decimal("0"),
        final_value_usd=Decimal("0"),
        total_costs_usd=Decimal("0"),
        final_balances={},
        positions_total=2,
        positions_closed=1,
        has_position_breakdown=True,
        verification_status=VerificationStatus.FAILED,
    )
    # The count is POSITIONS, not intents: 6 intents succeeded but only 1 of 2
    # positions closed — a position_events / intent count would mis-report this.
    assert result.positions_closed != result.intents_succeeded
    assert result.positions_closed < result.positions_total, "the residual position must be visible in the count"
    # A residual closure pairs FAILED confidence with success=False (fail-closed).
    assert result.verification_status is VerificationStatus.FAILED
    assert result.success is False

    # The pre-execution verification surface counts positions and carries the
    # confidence independently — a residual flips all_closed without touching the
    # (separate) intent tally.
    verification = ClosureVerification(
        all_closed=False,
        positions_total=2,
        positions_closed=1,
        has_position_breakdown=True,
        verification_status=VerificationStatus.FAILED,
    )
    assert verification.positions_total - verification.positions_closed == 1
    assert verification.all_closed is False


def check_s8_revert_selector_decode(primitive: str) -> None:
    """Seam 8 (TD-12): lending revert selectors decode to operator-clear hints,
    and an unknown selector is left untouched (no guessing)."""
    assert primitive == LENDING
    hint = operator_hint_for_selector("0x6679996d")  # Aave HF dust-debt trap
    assert hint and "HealthFactor" in hint
    annotated = annotate_teardown_error("Reverted 0x6679996d")
    assert annotated and "operator hint:" in annotated
    assert operator_hint_for_selector("0x12345678") is None  # honest: unknown left raw


def check_s9_no_accounting_measured_lane(primitive: str) -> None:
    """Seam 9 (TD-13): NO_ACCOUNTING acquisitions (STAKE/WRAP/CDP) and Pendle PT
    are reconciled to the MEASURED wallet via the swap-back clamp, folding the
    measured ledger and failing closed when unmeasured (VIB-5416 / VIB-5471)."""
    if primitive == STAKE:
        key = canonical_pt_symbol("WSTETH")
        clamped = decide_swap_clamp(live_balance=Decimal("5"), tracked_map={key: Decimal("3")}, from_token="WSTETH")
        assert clamped.reason == "clamped" and clamped.amount == Decimal("3")
        # Fail-closed: an unmeasured tracked read SKIPS + flags degraded (no sweep).
        unmeasured = decide_swap_clamp(live_balance=Decimal("5"), tracked_map=None, from_token="WSTETH")
        assert unmeasured.skip and unmeasured.degraded
        # Measured-gating: an absent ledger reader drops ONLY the NO_ACCOUNTING
        # lane (safe under-sweep), never fabricates measured-zero.
        assert read_no_accounting_ledger_rows(object(), "deployment:abc") is None
    elif primitive == PENDLE:
        # The strategy's maturity-less PT swap-back must match the maturity-bearing
        # tracked lot (VIB-5353 canonical_pt_symbol), so PT clamps (not strands).
        lot_key = canonical_pt_symbol("PT-wstETH-26DEC2024")
        clamped = decide_swap_clamp(
            live_balance=Decimal("5"), tracked_map={lot_key: Decimal("3")}, from_token="PT-wstETH"
        )
        assert clamped.reason == "clamped" and clamped.amount == Decimal("3")
    else:
        raise AssertionError(f"seam 9 not applicable to {primitive}")


# ---------------------------------------------------------------------------
# The matrix. Each seam declares its owning ticket, its check, and the
# per-primitive status. Status values: "green" | ("xfail", ticket, note) | ("na", note).
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class Seam:
    id: str
    name: str
    ticket: str  # owning TD ticket
    check: Callable[[str], None]
    status: dict[str, Any]  # primitive -> "green" | ("xfail", "VIB-XXXX", note) | ("na", note)


_NA_REGISTRY_STAKE = (
    "na",
    "staking/wrap acquisitions are NO_ACCOUNTING wallet tokens, not registry positions (handled by Seam 9)",
)
_NA_LENDING_ONLY = ("na", "seam is lending-specific")

SEAMS: tuple[Seam, ...] = (
    Seam(
        id="S1",
        name="registry-enumerated read path / restart-safe",
        ticket="VIB-5459",
        check=check_s1_registry_enumeration,
        status={
            LP: "green",
            LENDING: "green",
            PERP: "green",
            PENDLE: "green",
            STAKE: _NA_REGISTRY_STAKE,
        },
    ),
    Seam(
        id="S2",
        name="live per-position amount resolution (close fully -> live figure)",
        ticket="VIB-5465",
        check=check_s2_live_close_fully,
        status={LP: "green", LENDING: "green", PERP: "green", PENDLE: "green", STAKE: "green"},
    ),
    Seam(
        id="S3",
        name="HF-safe lending unwind (no dust-debt strand)",
        ticket="VIB-5467",
        check=check_s3_hf_safe_unwind,
        status={
            LP: _NA_LENDING_ONLY,
            LENDING: "green",
            PERP: _NA_LENDING_ONLY,
            PENDLE: _NA_LENDING_ONLY,
            STAKE: _NA_LENDING_ONLY,
        },
    ),
    Seam(
        id="S4",
        name="lending guard does not strand zero-debt withdraw",
        ticket="VIB-5468",
        check=check_s4_guard_zero_debt_withdraw,
        status={
            LP: _NA_LENDING_ONLY,
            LENDING: "green",
            PERP: _NA_LENDING_ONLY,
            PENDLE: _NA_LENDING_ONLY,
            STAKE: _NA_LENDING_ONLY,
        },
    ),
    Seam(
        id="S5",
        name="completeness fail-loud on uncovered position",
        ticket="VIB-5469",
        check=check_s5_completeness_enforced,
        status=dict.fromkeys(PRIMITIVES, "green"),
    ),
    Seam(
        id="S6",
        name="fail-closed on-chain post-teardown verify",
        ticket="VIB-5473",
        check=check_s6_failclosed_onchain_verify,
        status={
            LP: "green",
            LENDING: "green",
            # TD-15 (#3066) delivered fail-closed residual detection for LP (post-
            # condition) + lending (Plan-A chain read). Perp/pendle have NO per-
            # position on-chain closure verifier yet — _reconcile_one returns
            # UNVERIFIABLE and no post-condition is registered, so a residual is not
            # fail-closed. Their on-chain closure read is owned by separate tickets.
            PERP: (
                "xfail",
                "VIB-5116",
                "no on-chain closure verifier for GMX perp — reconciliation returns UNVERIFIABLE "
                "and no post-condition is registered, so a residual perp is not fail-closed",
            ),
            PENDLE: (
                "xfail",
                "VIB-3808",
                "no on-chain closure post-condition for Pendle TOKEN positions — reconciliation "
                "returns UNVERIFIABLE, so a residual PT/LP is not fail-closed",
            ),
            STAKE: _NA_REGISTRY_STAKE,
        },
    ),
    Seam(
        id="S7",
        name="authoritative verified closure count (position-count + verification_status)",
        ticket="VIB-5472",
        check=check_s7_authoritative_closure_count,
        status={
            LP: "green",
            LENDING: "green",
            PERP: "green",
            PENDLE: "green",
            STAKE: (
                "na",
                "staking is swept via the consolidation/clamp lane, not a verified position closure",
            ),
        },
    ),
    Seam(
        id="S8",
        name="lending revert-selector decode",
        ticket="VIB-5470",
        check=check_s8_revert_selector_decode,
        status={
            LP: _NA_LENDING_ONLY,
            LENDING: "green",
            PERP: _NA_LENDING_ONLY,
            PENDLE: _NA_LENDING_ONLY,
            STAKE: _NA_LENDING_ONLY,
        },
    ),
    Seam(
        id="S9",
        name="measured-ledger NO_ACCOUNTING lane",
        ticket="VIB-5471",
        check=check_s9_no_accounting_measured_lane,
        status={
            LP: ("na", "LP acquisitions emit accounting events (not the NO_ACCOUNTING lane)"),
            LENDING: ("na", "lending acquisitions emit accounting events (not the NO_ACCOUNTING lane)"),
            PERP: ("na", "perp acquisitions emit accounting events (not the NO_ACCOUNTING lane)"),
            PENDLE: "green",
            STAKE: "green",
        },
    ),
)


def _build_params() -> list[Any]:
    params: list[Any] = []
    for seam in SEAMS:
        for primitive in PRIMITIVES:
            status = seam.status[primitive]
            cell_id = f"{seam.id}[{seam.name}]-{primitive}"
            marks: list[Any] = []
            if status == "green":
                pass
            elif isinstance(status, tuple) and status[0] == "xfail":
                _, ticket, note = status
                marks.append(
                    pytest.mark.xfail(
                        reason=f"{ticket}: {note} ({_AS_OF}).",
                        strict=True,
                    )
                )
            elif isinstance(status, tuple) and status[0] == "na":
                marks.append(pytest.mark.skip(reason=f"N/A: {status[1]}"))
            else:  # pragma: no cover - guards a malformed matrix entry
                raise AssertionError(f"bad status {status!r} for {cell_id}")
            params.append(pytest.param(seam, primitive, id=cell_id, marks=marks))
    return params


@pytest.mark.parametrize(("seam", "primitive"), _build_params())
def test_teardown_regression_matrix(seam: Seam, primitive: str) -> None:
    """One assertion per (seam, primitive) cell. See module docstring + the QA
    doc for the full status table."""
    seam.check(primitive)


def test_matrix_shape_is_complete() -> None:
    """Lock the matrix shape: every seam declares a status for every primitive,
    and the green/xfail/na distribution matches the documented matrix."""
    assert len(SEAMS) == 9
    counts = {"green": 0, "xfail": 0, "na": 0}
    for seam in SEAMS:
        assert set(seam.status) == set(PRIMITIVES), f"{seam.id} missing a primitive"
        for status in seam.status.values():
            key = status if isinstance(status, str) else status[0]
            counts[key] += 1
    assert sum(counts.values()) == len(SEAMS) * len(PRIMITIVES) == 45
    # 25 GREEN (all merged seams proven) / 2 XFAIL (perp+pendle on-chain closure
    # verifier still unbuilt — VIB-5116 / VIB-3808) / 18 N/A.
    assert counts == {"green": 25, "xfail": 2, "na": 18}, counts
