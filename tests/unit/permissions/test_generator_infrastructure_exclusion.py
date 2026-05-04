"""Regression guard for ``INFRASTRUCTURE_NON_LOAD_BEARING_SELECTORS``.

The negative-authorisation harness
(``tests/intents/_permission_onchain_harness._auto_derive_load_bearing_selector``)
picks which target to revoke for negative-authz tests by EXCLUDING the
canonical set of "universal infrastructure" selectors — selectors the
manifest generator writes onto every manifest but which are not
load-bearing for any specific protocol bundle.

The exclusion set lives at
``almanak.framework.permissions.generator.INFRASTRUCTURE_NON_LOAD_BEARING_SELECTORS``
and is consumed by the harness. If the two drift apart — a new universal
infra selector lands in the generator without the matching exclusion-set
update, OR a sentinel selector remains in the exclusion set after the
generator stops emitting it — every per-chain ``test_zodiac_negative_anchor``
silently false-passes. Auto-derivation is the primary selector-resolution
path post-Phase-G.4 (every chain's negative anchor parametrizes cases
without an explicit selector), so a single missed exclusion lands as
silently broken negative coverage on all 7 chain anchors.

These tests pin the equality contract from both sides:

- ``test_universal_infrastructure_selectors_match_exclusion_set`` asserts
  that the universal-infra selectors emitted by the generator
  (``_build_infrastructure_permissions`` for no protocols + the
  per-token ``approve`` selector emitted by ``_extract_token_permissions``)
  equals the exclusion set exactly.
- ``test_protocol_conditional_infra_not_in_exclusion_set`` asserts that
  protocol-conditional infra (Enso Router) selectors — which ARE
  load-bearing for their protocol's bundles — are deliberately NOT in
  the exclusion set.
"""

from __future__ import annotations

from almanak.framework.intents.compiler import ERC20_APPROVE_SELECTOR
from almanak.framework.permissions.generator import (  # noqa: PLC2701 — private symbol imported intentionally for regression guard; see module docstring
    INFRASTRUCTURE_NON_LOAD_BEARING_SELECTORS,
    _build_infrastructure_permissions,
)


def test_universal_infrastructure_selectors_match_exclusion_set() -> None:
    """Generator's universal-infra selectors must equal the harness exclusion set.

    The exclusion set has two universal-infra entries today:

    - Safe ``MultiSend`` — emitted by ``_build_infrastructure_permissions``
      on every chain (DELEGATECALL batching primitive).
    - ERC-20 ``approve`` — emitted by ``_extract_token_permissions`` for
      every token referenced in a strategy's config. Universally non-load-
      bearing because revoking ``approve`` causes the bundle to revert on
      the first approval tx, which proves nothing about whether the
      protocol's core call is gated.

    Equality (not subset) is the right assertion: it catches drift in
    BOTH directions —

      - A new infra selector lands in ``_build_infrastructure_permissions``
        without being added to the exclusion set → auto-derivation might
        pick that selector as load-bearing, revoke it in a negative test,
        and surface "DID NOT RAISE" because Zodiac doesn't actually deny.
      - A selector lingers in the exclusion set after the generator stops
        emitting it → auto-derivation excludes a real protocol selector
        from candidate consideration and may skip the negative test or
        revoke the wrong target.

    Either drift silently false-passes negative-authz tests across all
    7 chain anchors (post-Phase-G.4 every chain relies on auto-derivation).
    """
    # Selectors emitted by the universal-infra-only path (no protocols
    # supplied → no Enso/etc. protocol-conditional permissions).
    universal_perms = _build_infrastructure_permissions(chain="ethereum", protocols=[])
    universal_infra_selectors: set[str] = {
        fn.selector for perm in universal_perms for fn in perm.function_selectors
    }

    # The per-token ``approve`` selector emitted by
    # ``_extract_token_permissions`` is also universal infrastructure —
    # every token target carries it. Combine both sources to form the
    # full universal-infra selector set.
    expected_universal_infra: set[str] = universal_infra_selectors | {ERC20_APPROVE_SELECTOR}

    assert expected_universal_infra == INFRASTRUCTURE_NON_LOAD_BEARING_SELECTORS, (
        "Universal-infra selector set drifted from the harness exclusion set. "
        "If you added a new universal infrastructure selector to "
        "`_build_infrastructure_permissions` or `_extract_token_permissions`, "
        "ALSO add it to `INFRASTRUCTURE_NON_LOAD_BEARING_SELECTORS` (or "
        "vice versa) — otherwise negative-authz tests will silently false-pass "
        "on every chain. "
        f"Universal-infra emitted: {sorted(expected_universal_infra)}; "
        f"exclusion set: {sorted(INFRASTRUCTURE_NON_LOAD_BEARING_SELECTORS)}."
    )


def test_protocol_conditional_infra_not_in_exclusion_set() -> None:
    """Protocol-conditional infra selectors are load-bearing — not excluded.

    Enso Router selectors (``routeSingle``, ``routeMulti``, etc.) are emitted
    by ``_build_infrastructure_permissions`` only when ``"enso"`` appears in
    ``protocols``. They ARE load-bearing for Enso's bundles: revoking
    Enso's router selectors in a negative authz test for an Enso swap is
    EXACTLY what proves the manifest gates the protocol call — the
    opposite of what the universal-infra exclusion is for.

    Regression guard: if someone "tidies up" the exclusion set by adding
    every selector emitted by ``_build_infrastructure_permissions`` (the
    naive read of the helper's name), Enso negative tests would silently
    skip the load-bearing target. This test asserts the per-protocol
    selectors are NOT in the universal-exclusion set.
    """
    universal_perms = _build_infrastructure_permissions(chain="ethereum", protocols=[])
    universal_selectors: set[str] = {
        fn.selector for perm in universal_perms for fn in perm.function_selectors
    }
    enso_perms = _build_infrastructure_permissions(chain="ethereum", protocols=["enso"])
    enso_selectors: set[str] = {
        fn.selector for perm in enso_perms for fn in perm.function_selectors
    }
    # The additional selectors Enso introduces — i.e. the protocol-conditional
    # infra — must be load-bearing, so none of them belongs in the
    # exclusion set.
    protocol_conditional = enso_selectors - universal_selectors
    assert protocol_conditional, (
        "Expected Enso to add at least one protocol-conditional selector — "
        "test setup is wrong if this fails."
    )
    assert protocol_conditional.isdisjoint(INFRASTRUCTURE_NON_LOAD_BEARING_SELECTORS), (
        "Protocol-conditional infra selectors leaked into the universal "
        "exclusion set. These selectors are load-bearing for their "
        "protocol's bundles and MUST be revocable in negative-authz tests. "
        f"Leaked: {sorted(protocol_conditional & INFRASTRUCTURE_NON_LOAD_BEARING_SELECTORS)}."
    )
