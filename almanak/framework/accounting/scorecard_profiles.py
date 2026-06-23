"""Scorecard profile registry shape for the Accountant Test (G-A foundation).

The Accountant Test scores a strategy DB under a *scorecard profile* — a named
bundle of (canonical primitive, required lifecycle, G6 ε tolerance, cell pack).
Historically three profiles (``"lp"``, ``"looping"``, ``"perp"``) were hard-coded
as a ``Literal`` plus three ``if/elif`` ladders (the lifecycle map, the G6 ε
selector, and the cell-pack dispatch). This module declares the *shape* of a
profile so those ladders collapse into one declarative table — assembled in
``accountant_test.py`` where the cell-pack callables live — and adding a
primitive becomes one registry entry instead of three new branches.

Blueprint 27 §2 (Primitives Foundation): "data-driven not hand-coded". Each
profile carries its canonical ``Primitive`` so the scorecard is no longer blind
to the taxonomy (§2.4). This is the neutral declaration tier (§2.1 layering): it
imports only the ``Primitive`` enum and stdlib — never ``accounting`` internals —
so it cannot create an import cycle with ``accountant_test``.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from almanak.framework.primitives.types import Primitive


@dataclass(frozen=True)
class G6Bases:
    """The three notional scaling bases G6 computes before resolving ε.

    Carries the locals the per-primitive ε selector reads so a profile's
    tolerance is a pure function of computed bases, not a closure over
    ``_cell_g6_reconciliation`` internals. All three are always computed
    (initialised to ``Decimal(0)`` and accumulated conditionally), so a profile
    that ignores two of them simply receives zeros — identical to the former
    ladder, which read only the one base relevant to its primitive.
    """

    notional_traded: Decimal
    max_debt: Decimal
    max_perp_notional: Decimal


@dataclass(frozen=True)
class ScorecardCtx:
    """The row-sets a cell pack needs, bundled so the registry can invoke every
    pack through one uniform interface. Adapter lambdas in the registry
    destructure this into each pack's existing (heterogeneous) signature, so the
    pack functions themselves are untouched.
    """

    pos_events: list[dict[str, Any]]
    acct_events: list[dict[str, Any]]
    snapshots: list[dict[str, Any]]
    acct_payloads: dict[Any, dict[str, Any]]
    payload_errors: dict[Any, str]
    position_state_rows: list[dict[str, Any]]


@dataclass(frozen=True)
class ScorecardProfile:
    """One Accountant Test scorecard profile.

    name
        Profile string key (``"lp"`` / ``"looping"`` / ``"perp"`` / …) — the
        stable contract used by the ratchet, matrix YAML, CLI, fixture
        directories, and the accounting unit tests.
    canonical_primitive
        The taxonomy ``Primitive`` this profile scores. Ties the scorecard to
        the canonical taxonomy (Blueprint 27 §2.4) without renaming the string
        key (``"looping"`` is a leverage-loop *lending* scorecard with no enum
        twin → ``Primitive.LENDING``).
    required_lifecycle
        Canonical intent_type lifecycle the fixture must exercise. Explicit
        tuple (not derived at runtime — a representative-intent lookup would risk
        grabbing the wrong tuple, e.g. the ``LP_COLLECT_FEES`` 3-step variant);
        a unit test asserts it equals the taxonomy lifecycle constant for
        ``canonical_primitive``.
    eps_pct
        G6 reconciliation tolerance percent.
    eps_scaling
        ``(G6Bases) -> (scaling_base, scaling_label)``: selects the primitive's
        notional base for ``ε = max(floor, eps_pct × base)``.
    cells
        ``(ScorecardCtx) -> list[CellResult]``: the primitive cell pack, adapted
        to the pack's existing signature.
    disposal_usd_unmeasured_is_xfail
        VIB-5319: when True, a generic money-trail / reconciliation cell (G1, G6)
        that is blocked *only* because a disposal leg's gross USD value is
        unmeasured — and that unmeasured value is a KNOWN, TICKETED gateway-price
        gap, not an accounting bug — surfaces ``XFAIL`` (measured-but-blocked)
        instead of ``FAIL`` (a wrong/contradictory number). Set True only on the
        ``pendle_pt`` profile: a PT_SELL / PT_REDEEM on a fork without the gateway
        PT/SY implied-price path (VIB-5276) carries ``sy_price = None``, so the
        disposal proceeds (``sy_amount × sy_price``) and the per-leg USD money
        trail are genuinely unmeasured (Empty ≠ Zero — never folded to a silent
        zero, never a fabricated PASS). Every other primitive keeps the strict
        FAIL-on-null behaviour (default False). The XFAIL is narrowly scoped to
        the PT-disposal-USD null bucket: any *other* unmeasured component bucket
        still FAILs.
    """

    name: str
    canonical_primitive: Primitive
    required_lifecycle: tuple[str, ...]
    eps_pct: Decimal
    eps_scaling: Callable[[G6Bases], tuple[Decimal, str]]
    cells: Callable[[ScorecardCtx], list[Any]]
    disposal_usd_unmeasured_is_xfail: bool = False
