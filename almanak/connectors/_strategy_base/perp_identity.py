"""Venue-owned position identity: the ALIAS-SET seam (VIB-6287).

Why an alias set and not a canonical key
----------------------------------------
A perp position is named DIFFERENTLY by every surface that produces it:

* the strategy's own ``get_open_positions()`` emits a synthetic label plus
  market / collateral as **symbols**;
* the ``position_registry`` runtime writer emits the venue's position key plus
  market / collateral as **symbols**;
* the settlement reconciler emits the venue key plus market / collateral as
  **addresses** — under the SAME ``details`` key names the strategy fills with
  symbols;
* the backfill folder emits the venue key and ``market=None`` (the column is not
  persisted — Empty ≠ Zero, so it stays absent rather than fabricated);
* the connector's on-chain discovery emits the venue key, the market NAME, a
  ``market_address``, and the collateral ADDRESS.

Every one of those describes ONE physical position. The teardown enumeration
unions HOT (strategy) and WARM (registry) surfaces, so any pair that does not
compare equal enumerates one position as TWO — and the completeness check then
fails the row that never received a closing intent, reporting a **FAILED**
teardown for a position that verifiably flattened on-chain (VIB-6287, measured
on Arbitrum mainnet across four runs).

A single canonical key cannot express this. Two venues of the five make that
concrete:

* ``aster_perps`` and ``pancakeswap_perps`` (the same Aster Diamond contract
  under two slugs) have **no derivable identity at all** — the ``tradeHash`` is
  contract-assigned per open call and there is no pure function from
  ``(account, market, collateral, side)`` to it. A derive-only seam is
  structurally incapable of serving them. **Do not assume a formula exists for
  the venue you are adding; ADOPTING a recorded id is the general mechanism and
  deriving one is a per-venue supplement.**
* ``drift`` keys on ``(owner_pubkey, sub_account_id, market_index)`` and
  ``hyperliquid`` on ``(wallet, assetIndex)`` with no collateral axis at all
  (margin is uniformly USDC). No single tuple shape generalises.

So a connector returns **a set of opaque tokens: every token it is CERTAIN
names this position**. Two rows are the same position **iff their token sets
intersect**. A row that carries both an adopted key and a semantic tuple bridges
a key-only row to a semantic-only row transitively — which is exactly how a
backfill row (key only) reaches a strategy row (semantic only).

The emission contract — this is the safety property
---------------------------------------------------
> **A connector may emit a token only when it is CERTAIN that token names this
> position. Never a heuristic. Never a partial tuple.**

If any component of a token is unresolved, emit **no token of that kind** rather
than a degraded one. An under-specified token is the only way this design can
over-collapse, and over-collapse is the strictly worse failure:

* **over-split** — a duplicate enumeration row ⇒ loud false ``FAILED`` after the
  money is already out (today's bug, and the duplicate row does NOT produce a
  second close intent);
* **over-collapse** — the warm row is suppressed ⇒ nothing closes it ⇒
  **silently stranded funds**, with no alarm at all.

Empty ≠ Zero: an empty token set means **UNMEASURED identity** — never "no
identity", never "closed". The framework falls back to its own per-type default
and, failing that, to the raw ``position_id``.

Tokens are opaque
-----------------
The framework compares tokens for equality and does **nothing else** — no
parsing, no splitting, and in particular **no case folding**. ``drift``'s
identity contains a base58 Solana pubkey, where ``'B' != 'b'`` is a different
byte rather than a formatting variant, while the EVM registry writers lowercase
hex reflexively. Normalisation belongs inside the connector, which is the only
layer that knows the encoding. No Solana perp path reaches this seam today, so
this is a latent trap for the next venue rather than a present bug — which is
precisely why it is written down here instead of discovered later.

Namespace your tokens with your own protocol slug so two venues can never emit
the same string for different positions.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:  # pragma: no cover - typing only, keeps the connector layer import-light
    from almanak.framework.teardown.models import PositionInfo

logger = logging.getLogger(__name__)


def is_residual_marked(details: Any) -> bool:
    """Does this row carry a residual / non-position ``kind`` marker?

    **SCOPE — read before adding a call site. ``details["kind"]`` is POLYSEMOUS
    across primitives, and this predicate is valid only where it is EXCLUSIVELY
    a residual marker.** That is true for ``PositionType.PERP``, which is why
    the only two call sites are the GMX hook and ``_perp_default_identity``.

    It is NOT true elsewhere. ``_position_info_from_pendle_registry_row``
    (``framework/teardown/registry_enumeration.py``) writes
    ``details["kind"] = "pt" | "lp"`` as a POSITIONAL DISCRIMINATOR — it says
    *which kind of position this is*, not *that this is not one*. A Pendle LP
    registry row is a real, closable position carrying ``kind="lp"``, and this
    predicate returns ``True`` for it. Verified, not assumed.

    So wiring this into ``_lp_default_identity`` — for symmetry, citing the
    "a guard present in one layer and absent in the other is worse than no
    guard" argument below — would leave **every Pendle LP registry row
    unnamed**. That degrades to raw-``position_id`` over-split, so it is the
    loud direction rather than a strand, but it would be a regression shipped in
    the name of a fix. ``test_residual_marker_is_not_wired_into_a_positional_
    kind_default`` fails if anyone does it.

    This is VIB-6287's own defect class turned on the fix: **one key name, two
    value spaces, and a consumer that assumes one of them.** The right
    generalisation is a per-primitive marker, not a broader predicate.

    Within its scope it is THE single predicate for "this row is not a
    position", shared by the connector hooks and the framework's per-type
    default so the two layers cannot drift apart about what a residual is. A
    guard that is present in one layer and absent in the other is worse than no
    guard: it hides in exactly the venues nobody is looking at — which is
    precisely how the first version of this guard shipped, present in the GMX
    hook and absent from the default that serves the four venues with no hook.

    Residuals (a pending unfilled order holding collateral in a vault, an
    unverified re-measurement sentinel) are surfaced as position-typed rows and
    can carry the SAME market, collateral and side as a real open position while
    being a different thing holding their own money. Naming one in position
    identity space lets the union merge the two and suppress one — and a
    suppressed residual is never recovered, because nothing builds a closing
    intent for a row that is not in the enumeration.

    **Polarity is deliberate: PRESENCE marks a residual, not truthiness.** A
    key that is present but blank (``""``, ``"   "``) is a MEASURED-but-malformed
    marker — the producer meant to say something — not an absence, so it counts
    (Empty ≠ Zero). The two failure directions are not symmetric:

    * treating a blank marker as "not a residual" NAMES the row ⇒ it can be
      merged with a real position ⇒ **silently stranded funds, no alarm**;
    * treating it as a residual leaves the row unnamed ⇒ at worst a duplicate
      enumeration row ⇒ loud.

    So the guard fails toward "residual". No producer emits a blank ``kind``
    today — every writer that reaches a position's ``details`` uses a hardcoded
    literal, and the one payload-derived path normalises and drops unrecognised
    values — but "cannot happen in practice" is precisely the reasoning that
    decays, and here it decays into the silent direction.

    ``None`` is treated as ABSENT rather than blank: it is the conventional
    "unset" value and no producer writes it deliberately as a marker.
    """
    if not isinstance(details, dict) or "kind" not in details:
        return False
    return details["kind"] is not None


class VenuePositionIdentity(Protocol):
    """Return every token the venue is CERTAIN names ``position``.

    Returns an empty ``frozenset`` when the venue cannot name the position —
    UNMEASURED identity, never "no identity". See the module docstring for the
    emission contract; violating it risks silently stranded funds.

    Must be pure and must never raise: it runs inside the teardown enumeration,
    and a hook fault must degrade to an empty set (over-split, loud) rather than
    fault the lane that is trying to close real positions.
    """

    def __call__(self, position: PositionInfo, *, wallet_address: str | None) -> frozenset[str]: ...


_REGISTRY: dict[str, VenuePositionIdentity] = {}


def _register_perp_identity(protocol: str, hook: VenuePositionIdentity) -> None:
    """Register an identity hook for ``protocol`` (framework-internal).

    Not a connector-facing API: connectors publish hooks through
    ``CONNECTOR.perp_identity`` (an ``ImportRef`` on the manifest) and the
    framework hydrates them at import time
    (``almanak.framework.teardown.perp_identity``).

    Re-registering the same hook is idempotent. Replacing an existing hook logs
    a warning so accidental shadowing is visible in logs.
    """
    key = protocol.lower()
    existing = _REGISTRY.get(key)
    if existing is not None and existing is not hook:
        logger.warning("Replacing existing venue position identity hook for protocol %r", protocol)
    _REGISTRY[key] = hook


def get_perp_identity_hook(protocol: str) -> VenuePositionIdentity | None:
    """Look up a registered hook. ``None`` when the venue publishes none.

    The lookup key is a protocol SLUG and is folded to lower case; that is a
    registry-key convention, not token normalisation (tokens are never folded).
    """
    return _REGISTRY.get(protocol.lower())


def has_perp_identity_hook(protocol: str) -> bool:
    """``True`` iff ``protocol`` publishes a venue position identity hook."""
    return protocol.lower() in _REGISTRY


def registered_perp_identity_protocols() -> frozenset[str]:
    """Every protocol slug with a registered hook — the census surface."""
    return frozenset(_REGISTRY)


__all__ = [
    "VenuePositionIdentity",
    "is_residual_marked",
    "get_perp_identity_hook",
    "has_perp_identity_hook",
    "registered_perp_identity_protocols",
]
