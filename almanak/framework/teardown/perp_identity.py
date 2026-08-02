"""Framework seam for venue-owned position identity (VIB-6287).

Hydrates connector-published ``CONNECTOR.perp_identity`` hooks into the
strategy-side registry and exposes the single lookup both teardown consumers
use — the enumeration union (``registry_enumeration``) and the completeness
coverage check (``completeness``) — so the two lanes resolve identity through
the SAME venue rule instead of each re-implementing a comparison and disagreeing
about which rows are the same position.

The framework's job here is deliberately tiny: dispatch on the row's
``position_type`` and ``position.protocol``, call the hook, validate the tokens
STRUCTURALLY, and hand back the survivors **verbatim**.

**"Verbatim" is exact and narrower than it used to be (VIB-6329).** This
docstring previously said the framework "does not parse, split, or case-fold a
token". Splitting is now required — the seam reads the leading
``<slug>:<family>`` labels of the ``<slug>:<family>:<payload>`` grammar so it can
reject a row that emits two tokens of ONE family, which is the row that bridges
two distinct positions and silently strands one. What remains absolutely true,
and is the part that rule was protecting, is that a **payload is never read,
folded, or rewritten**, and a surviving token is byte-identical to what the
connector emitted. See ``_strategy_base/perp_identity.py`` §"Token grammar" for
the full argument, and for why the payload must stay opaque (``drift`` carries a
case-SENSITIVE base58 pubkey while the EVM writers lowercase hex reflexively;
normalisation belongs in the connector, which knows the encoding).
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

from almanak.connectors._connector import CONNECTOR_REGISTRY, ConnectorDiscoveryError
from almanak.connectors._strategy_base.perp_identity import (
    VenuePositionIdentity,
    _register_perp_identity,
    get_perp_identity_hook,
    get_perp_identity_namespaces,
    has_perp_identity_hook,
    registered_perp_identity_protocols,
)

from .models import PositionInfo, PositionType
from .post_conditions import _connector_teardown_slugs

logger = logging.getLogger(__name__)


def _register_manifest_perp_identities() -> None:
    """Register connector-owned identity hooks from manifests.

    Registered under every slug the connector can emit
    (``_connector_teardown_slugs``), NOT just ``connector.name``: a position's
    ``protocol`` string is not always the connector folder name, and registering
    by bare name silently no-ops for such connectors (VIB-5573). A hook that
    never resolves is indistinguishable from no fix at all.

    ONE BAD CONNECTOR MUST NOT BREAK REGISTRATION FOR THE REST, and above all
    must not fault the teardown lane. ``_dedupe_keys`` imports this module lazily
    at call time, so an exception raised here surfaces inside
    ``resolve_open_positions_with_registry`` → ``STRATEGY_ERROR`` →
    ``_request_teardown_failure_shutdown``: **no closing intent is ever built,
    for any strategy**, because one connector shipped a typo in its manifest
    ``ImportRef``. ``Connector._validate`` only checks the ref is an ``ImportRef``,
    never that it loads.

    Skipping a broken connector degrades that venue to raw ``position_id``
    identity — over-split, loud — which is the correct direction. Raising
    degrades every venue to no teardown at all. Same rationale, and the same
    shape, as the secondary-load guard in ``post_conditions.py`` (#3534 audit
    panel).
    """
    for connector_manifest in CONNECTOR_REGISTRY.with_perp_identity():
        ref = connector_manifest.perp_identity
        if ref is None:
            continue
        try:
            hook = ref.load()
            if not callable(hook):
                raise ConnectorDiscoveryError(
                    f"{ref.module}.{ref.attribute} must be callable, got {type(hook).__qualname__}"
                )
            slugs = _connector_teardown_slugs(connector_manifest)
        except Exception:
            logger.error(
                "perp identity hook for connector %r failed to load (%s.%s) — skipping it; "
                "that venue falls back to raw position_id identity (over-split, loud)",
                getattr(connector_manifest, "name", "?"),
                getattr(ref, "module", "?"),
                getattr(ref, "attribute", "?"),
                exc_info=True,
            )
            continue
        for slug in slugs:
            # Every slug the connector can emit is a namespace it OWNS: a
            # connector registered under several slugs may legitimately namespace
            # all of its tokens with just one of them (VIB-6329).
            _register_perp_identity(slug, hook, namespaces=frozenset(slugs))


_register_manifest_perp_identities()


# A conforming token is ``<slug>:<family>:<payload>`` — see the grammar section
# in ``_strategy_base/perp_identity.py``. The payload may itself contain colons,
# so the split is bounded at 2.
_TOKEN_SEGMENTS = 3


def _structural_parts(token: str) -> tuple[str, str] | None:
    """``(namespace, family)`` folded FOR COMPARISON ONLY, or ``None`` if malformed.

    The payload is deliberately not returned: nothing in this module may read,
    fold, or rewrite it. Only the two structural labels are inspected, and the
    token that survives validation is emitted byte-identical.
    """
    parts = token.split(":", _TOKEN_SEGMENTS - 1)
    if len(parts) < _TOKEN_SEGMENTS:
        return None
    namespace, family, payload = parts
    # A blank segment is a MEASURED-but-malformed token, not an absent one. An
    # under-specified token is the over-collapse direction, so it is refused
    # rather than accepted with an empty part.
    if not namespace.strip() or not family.strip() or not payload.strip():
        return None
    return namespace.strip().lower(), family.strip().lower()


def _enforce_token_structure(protocol: str, tokens: frozenset[str]) -> frozenset[str]:
    """Drop tokens that break the structural rules the framework CAN check (VIB-6329).

    Two rules, both decidable without understanding a payload byte:

    1. **NAMESPACE** — a token must be namespaced by a slug its own connector
       owns. ``_dedupe_keys`` builds ``(chain, position_type, "venue", <token>)``
       and deliberately does NOT include the protocol, so cross-venue safety
       rests entirely on the namespace. (Adding ``protocol`` to the key is NOT
       the alternative fix: registry rows are labelled with the registry
       PRIMITIVE rather than the connector slug, so scoping the key by protocol
       would stop a registry row matching its own strategy row — trading a latent
       over-collapse for a live over-split.)

    2. **AT MOST ONE TOKEN PER FAMILY** — two different ``key`` tokens on one row
       means the row names two different positions in the same identity space.
       It is internally inconsistent by construction, and under the enumeration's
       transitive closure it BRIDGES two physically distinct positions, suppresses
       one, and strands it with no alarm.

    **Why the whole family is dropped rather than one member kept.** When two
    tokens of a family disagree the framework cannot tell WHICH is right — that
    needs the venue's own derivation. GMX resolves the same situation by keeping
    the ADOPTED key, because it knows a recorded key outranks a computed one; the
    framework has no such knowledge, so it keeps neither. Dropping is a strict
    SHRINK of the alias set, and a smaller set can only ever merge fewer rows —
    over-split, loud, recoverable. Keeping an arbitrary member could preserve the
    wrong one and merge on it.

    **All-token rejection must stay a shrink.** If every family is dropped, this
    function returns an empty set. ``_dedupe_keys`` distinguishes "a registered
    hook returned no usable identity" from "this venue has no hook": the former
    falls directly to raw ``position_id`` while only the latter may use the
    coarser per-type default. Without that distinction a future Aster hook could
    reject a malformed ``tradeHash`` token and then merge two distinct trades on
    ``(market, collateral, side)``; Drift could do the same across sub-accounts.
    Raw-id fallback keeps rejection strictly subtractive and therefore in the
    loud over-split direction.

    **What this does NOT check.** A row emitting ``{slug:key:A, slug:sem:B}``
    where ``A`` and ``B`` name different positions passes every rule above: the
    tokens are in different families, so nothing structural is violated. Deciding
    whether they agree needs the venue's derivation, and a framework-side check
    would have to call connector-supplied code to get it — checking the connector
    against itself. That case is closed by the emission contract and by review,
    never here.
    """
    namespaces = get_perp_identity_namespaces(protocol)
    by_family: dict[str, set[str]] = {}
    for token in tokens:
        parts = _structural_parts(token)
        if parts is None:
            logger.error(
                "perp identity hook for %r emitted a malformed token (expected "
                "'<slug>:<family>:<payload>') — dropping it; a fully rejected row "
                "falls back to raw position_id (over-split, loud)",
                protocol,
            )
            continue
        namespace, family = parts
        if namespace not in namespaces:
            logger.error(
                "perp identity hook for %r emitted a token namespaced %r, which it does not "
                "own (owns %s) — dropping it; an un-owned namespace can collide with another "
                "venue's tokens and merge two protocols' positions",
                protocol,
                namespace,
                sorted(namespaces),
            )
            continue
        by_family.setdefault(family, set()).add(token)

    kept: set[str] = set()
    for family, family_tokens in by_family.items():
        if len(family_tokens) > 1:
            logger.error(
                "perp identity hook for %r emitted %d different %r tokens for ONE row — the row "
                "names two positions in one identity space, which would bridge two distinct "
                "positions and silently strand one. Dropping the whole %r family; the row keeps "
                "its other families, or falls back to raw position_id (over-split, loud)",
                protocol,
                len(family_tokens),
                family,
                family,
            )
            continue
        kept |= family_tokens
    return frozenset(kept)


def venue_identity_tokens(position: PositionInfo, wallet_address: str | None = None) -> frozenset[str]:
    """Alias tokens the position's own venue is CERTAIN name it.

    Empty means UNMEASURED identity — never "no identity", never "closed". For
    a registered PERP hook, callers MUST fall directly to raw ``position_id``;
    the coarser per-type default is reserved for venues with no hook.
    """
    # PERP-SCOPED DISPATCH (VIB-6329). The hooks are perp identity hooks — the
    # registry is keyed by protocol alone, so without this a NON-perp row of a
    # hook-publishing protocol would be handed to a hook that never checks the
    # row's type, and would skip its own per-type default.
    #
    # Not reachable today, for two independently verified reasons: every key
    # ``_dedupe_keys`` builds is position-type-scoped, so rows of different types
    # can never share one; and ``gmx_v2`` declares ``strategy_intents=(PERP_OPEN,
    # PERP_CLOSE)`` only, so no non-perp gmx_v2 strategy surface exists. The
    # residual harm is layering rather than stranding — but the layering is what
    # makes the NEXT connector's mistake reachable, and the fix is three lines.
    #
    # Both live call sites already pass PERP: ``_dedupe_keys`` for real rows, and
    # ``completeness._intent_venue_aliases``, which builds a probe stamped
    # ``PositionType.PERP``.
    if getattr(position, "position_type", None) != PositionType.PERP:
        return frozenset()
    protocol = str(getattr(position, "protocol", "") or "").strip()
    if not protocol:
        return frozenset()
    hook: VenuePositionIdentity | None = get_perp_identity_hook(protocol)
    if hook is None:
        return frozenset()
    try:
        tokens = hook(position, wallet_address=wallet_address)
    except Exception:  # pragma: no cover - defensive; hooks promise not to raise
        logger.debug("venue position identity hook for %r raised", protocol, exc_info=True)
        return frozenset()
    if not tokens:
        return frozenset()
    # VALIDATE THE CONTAINER, NOT JUST THE ELEMENTS.
    #
    # A `str` is iterable, so `frozenset(t for t in "gmx_v2:key:...")` yields
    # SINGLE CHARACTERS, every one of which is a non-empty `str` and therefore
    # passes an element-wise check. That is the single most likely authoring
    # mistake against a new `-> frozenset[str]` contract: returning the token
    # instead of a set containing it.
    #
    # The consequence is the forbidden direction. Every position on that venue
    # would share tokens like "x", ":", "e", so `seen.isdisjoint(keys)` is never
    # true and EVERY registry-derived position after the first is suppressed from
    # the enumeration. Nothing builds a closing intent for a suppressed row and
    # nothing raises — teardown reports success over stranded funds.
    #
    # Found by the #3534 audit panel, demonstrated by execution against this very
    # seam. It also falsifies the claim this module made in three places, that
    # "an under-specified token is the ONLY way this design can over-collapse".
    # It was not: an under-specified CONTAINER was the other way.
    #
    # Latent when written — `gmx_v2`, the only shipped hook, is correct — but this
    # PR ships the extension seam, so the guard belongs with the seam and not with
    # the venue that eventually trips it.
    if isinstance(tokens, str) or not isinstance(tokens, set | frozenset | list | tuple):
        logger.error(
            "perp identity hook for %r returned %s, not a collection of tokens — "
            "ignoring it and falling back to the caller's own comparison",
            protocol,
            type(tokens).__qualname__,
        )
        return frozenset()
    # Payloads are returned VERBATIM — a surviving token is byte-identical to what
    # the connector emitted. The framework splits off and folds the leading
    # ``<slug>:<family>`` labels for COMPARISON ONLY (VIB-6329, and see the grammar
    # section in ``_strategy_base/perp_identity.py`` for why that narrowing was
    # made explicit rather than performed quietly). Only empty/non-string entries
    # are dropped here, which a conforming hook never produces.
    well_typed = frozenset(t for t in tokens if isinstance(t, str) and t)
    return _enforce_token_structure(protocol, well_typed)


def wallet_for(wallet_for_chain: Callable[[str], str | None] | None, chain: Any) -> str | None:
    """Resolve the wallet for ``chain`` through the caller's resolver.

    The resolver is a CALLABLE rather than a scalar because the teardown union
    spans chains and a multi-chain deployment holds a different wallet per chain
    (``_teardown_wallet_for_chain``). Never raises: an unresolvable wallet is
    ``None`` (UNMEASURED), which costs at most a derived token.
    """
    if wallet_for_chain is None:
        return None
    try:
        resolved = wallet_for_chain(str(chain or ""))
    except Exception:  # noqa: BLE001 - wallet resolution must never break teardown
        logger.debug("wallet resolution failed for chain %r", chain, exc_info=True)
        return None
    return str(resolved) if resolved else None


__all__ = [
    "has_perp_identity_hook",
    "registered_perp_identity_protocols",
    "venue_identity_tokens",
    "wallet_for",
]
