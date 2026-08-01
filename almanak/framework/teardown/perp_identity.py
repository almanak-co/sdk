"""Framework seam for venue-owned position identity (VIB-6287).

Hydrates connector-published ``CONNECTOR.perp_identity`` hooks into the
strategy-side registry and exposes the single lookup both teardown consumers
use — the enumeration union (``registry_enumeration``) and the completeness
coverage check (``completeness``) — so the two lanes resolve identity through
the SAME venue rule instead of each re-implementing a comparison and disagreeing
about which rows are the same position.

The framework's job here is deliberately tiny: dispatch on ``position.protocol``,
call the hook, and hand back the tokens **verbatim**. It does not parse, split,
or case-fold a token. See ``_strategy_base/perp_identity.py`` for why (``drift``
carries a case-SENSITIVE base58 pubkey while the EVM writers lowercase hex
reflexively; normalisation belongs in the connector, which knows the encoding).
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
    has_perp_identity_hook,
    registered_perp_identity_protocols,
)

from .models import PositionInfo
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
            _register_perp_identity(slug, hook)


_register_manifest_perp_identities()


def venue_identity_tokens(position: PositionInfo, wallet_address: str | None = None) -> frozenset[str]:
    """Alias tokens the position's own venue is CERTAIN name it.

    Empty means UNMEASURED identity — never "no identity", never "closed". Every
    caller MUST fall back to its pre-existing comparison on an empty set.
    """
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
    # Tokens are returned VERBATIM — no case folding, no parsing (see module
    # docstring). Only empty/non-string entries are dropped, which a conforming
    # hook never produces.
    return frozenset(t for t in tokens if isinstance(t, str) and t)


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
