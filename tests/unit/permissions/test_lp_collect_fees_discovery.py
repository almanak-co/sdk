"""LP_COLLECT_FEES must actually be discoverable when a connector declares it (VIB-6149).

Two defects, one symptom — a manifest with zero protocol targets for the verb:

1. **Framework.** ``_build_lp_collect_fees_intents`` never passed ``position_id``.
   Every NFT-position collect compiler rejects the intent without it, so the
   default shape could never compile *for any protocol*. uniswap_v4 hit this and
   worked around it in its own ``build_discovery_vectors``; its docstring records
   the cause. The sibling ``_build_lp_close_intents`` reads correct because
   ``LPCloseIntent`` takes ``position_id`` as a top-level field, whereas
   ``CollectFeesIntent`` requires it inside ``protocol_params`` — same hint,
   different carrier, and that asymmetry is what hid it.

2. **Per connector.** uniswap_v3 / sushiswap_v3 / pancakeswap_v3 declared
   ``LP_COLLECT_FEES`` in ``strategy_intents`` while their hints said
   ``supports_standalone_fee_collection=False``. Nothing compared the two.

The first test below is the one that matters long-term: it asserts the two
declarations agree *for every connector*, catching the class rather than today's
three instances.

Masking note — this was invisible for two independent reasons. ``generate_manifest``
returned the degraded manifest with ``warnings: []``, and ``_TEARDOWN_COMPLEMENTS``
expands LP_OPEN -> LP_CLOSE whose flow authorises ``collect`` anyway, so only a
fee-harvest-ONLY strategy ever saw it. Hence the explicit collect-only assertions
here: any intent set containing LP_OPEN hides the bug.
"""

from __future__ import annotations

import pytest

from almanak.connectors._connector import CONNECTOR_REGISTRY
from almanak.framework.execution.signer.safe.constants import MULTISEND_ADDRESSES
from almanak.framework.intents.compiler import (
    ERC20_APPROVE_SELECTOR,
    LP_POSITION_MANAGERS,
    NFT_POSITION_COLLECT_SELECTOR,
)
from almanak.framework.permissions.discovery import discover_permissions
from almanak.framework.permissions.hints import get_permission_hints
from almanak.framework.permissions.synthetic_intents import build_synthetic_intents

_VERB = "LP_COLLECT_FEES"

# Declaring connectors whose standalone collect is NOT a single NFT-position-manager
# call, and which therefore cannot be held to the exact-grant assertion below.
#
# traderjoe_v2's LP is bin-based — there is no NFT position id — so its collect
# authorises the LBRouter plus the pair surface (measured: two targets) and its
# coverage comes from ``static_permissions`` rather than from a compile.
#
# This is an EXEMPTION list, not an inclusion list, on purpose: a newly added
# connector is covered by the strict test by default and has to be argued OUT,
# rather than silently escaping it by not being added to an inclusion list.
# ``test_exemptions_are_live`` fails if an entry here stops declaring the verb.
_NON_NFT_POSITION_COLLECTORS = frozenset({"traderjoe_v2"})


def _declaring_connectors() -> list[str]:
    return sorted(m.name for m in CONNECTOR_REGISTRY.all() if _VERB in (getattr(m, "strategy_intents", None) or ()))


def _safe_reachable_chains(connector: str) -> list[str]:
    """Every declared chain on which a Safe — and therefore a Zodiac manifest — exists.

    Chains without a MultiSend deployment have no Safe execution path, so a
    missing permission there cannot revert ``execTransactionWithRole``.
    """
    manifest = CONNECTOR_REGISTRY.get(connector)
    assert manifest is not None
    return sorted(c for c in (getattr(manifest, "strategy_chains", None) or ()) if c.lower() in MULTISEND_ADDRESSES)


def _first_chain(connector: str) -> str:
    chains = _safe_reachable_chains(connector)
    assert chains, f"{connector} declares no Safe-reachable chain"
    return chains[0]


def _nft_position_collect_pairs() -> list[tuple[str, str]]:
    """Every (connector, Safe-reachable chain) pair held to the exact-grant assertion.

    Per-chain rather than per-connector: the position-manager address is
    chain-specific, so a single-chain check would pass while a wrong address on
    every other chain went unnoticed.
    """
    return [
        (connector, chain)
        for connector in _declaring_connectors()
        if connector not in _NON_NFT_POSITION_COLLECTORS
        for chain in _safe_reachable_chains(connector)
    ]


class TestDeclarationsAgree:
    """The structural fix: the two declarations must never disagree again.

    Scope caveat — this covers every connector with a **1:1 slug**, not literally
    every protocol slug. ``get_permission_hints`` resolves aliases through
    ``_PROTOCOL_CONNECTOR_MAP``, so slugs like ``aerodrome_slipstream`` and
    ``metamorpho`` carry their own ``PermissionHints`` while having no row of
    their own in ``CONNECTOR_REGISTRY``. ``aerodrome_slipstream`` is a live
    example: its hints set ``supports_standalone_fee_collection=True`` while the
    single backing ``aerodrome`` connector row declares only SWAP/LP_OPEN/LP_CLOSE.

    That is not expressible as a disagreement here — one connector row backs two
    slugs, so ``strategy_intents`` cannot diverge per slug — and it is not a
    silent gap in practice: slipstream's collect surface is pinned by
    ``test_aerodrome_slipstream_manifest.py``. Deliberately NOT "fixed" by
    comparing against ``get_protocol_intent_matrix()``: that function *derives*
    LP_COLLECT_FEES membership from this very flag, so the comparison would be
    tautological — a test that cannot fail. Closing it properly needs a per-slug
    intent declaration; tracked as VIB-6164.
    """

    @pytest.mark.parametrize("connector", [m.name for m in CONNECTOR_REGISTRY.all()])
    def test_strategy_intents_and_hints_agree_on_fee_collection(self, connector: str) -> None:
        manifest = CONNECTOR_REGISTRY.get(connector)
        assert manifest is not None
        declares = _VERB in (getattr(manifest, "strategy_intents", None) or ())
        supports = get_permission_hints(connector).supports_standalone_fee_collection
        assert declares == supports, (
            f"{connector}: connector manifest declares {_VERB}={declares} but "
            f"PermissionHints.supports_standalone_fee_collection={supports}. These are the same "
            f"claim made twice, and nothing else compares them — a mismatch yields a manifest with "
            f"zero protocol targets for the verb (declares=True) or a connector advertising a verb "
            f"it never authorises (declares=False). Fix whichever is wrong; do not silence this."
        )


def _uses_framework_default_vectors(connector: str) -> bool:
    """True when the connector has no ``build_discovery_vectors`` override.

    A connector that owns its vectors owns their shape — traderjoe_v2's LP is
    bin-based with no NFT position id at all, and its coverage comes from
    ``static_permissions`` rather than from a compile. Asserting a position_id
    on those would be asserting a fact about NFT position managers against a
    protocol that has none.
    """
    from almanak.framework.permissions.hints import get_discovery_vectors_override

    return get_discovery_vectors_override(connector) is None


class TestSyntheticIntentShape:
    @pytest.mark.parametrize("connector", [c for c in _declaring_connectors() if _uses_framework_default_vectors(c)])
    def test_default_collect_intent_carries_position_id(self, connector: str) -> None:
        """The framework-default builder must supply ``position_id``.

        ``CollectFeesIntent`` rejects a top-level ``position_id`` ("Extra inputs
        are not permitted"), so it must travel in ``protocol_params`` — the exact
        asymmetry with ``LPCloseIntent`` that hid this. Scoped to connectors on
        the default path; an override owns its own shape.
        """
        chain = _first_chain(connector)
        intents = build_synthetic_intents(connector, _VERB, chain)
        assert intents, f"{connector} declares {_VERB} but builds no synthetic intent on {chain}"
        for intent in intents:
            params = getattr(intent, "protocol_params", None) or {}
            assert params.get("position_id"), (
                f"{connector}/{chain}: synthetic {_VERB} intent has no protocol_params['position_id'] — "
                f"every NFT-position collect compiler rejects it, so this can never compile"
            )


class TestCompiledManifest:
    """The layer that would have caught it: what discovery actually yields."""

    @pytest.mark.parametrize("connector", _declaring_connectors())
    def test_collect_only_manifest_has_a_protocol_target(self, connector: str) -> None:
        """Deliberately requests ``[LP_COLLECT_FEES]`` ALONE.

        Any intent set containing LP_OPEN masks the bug: ``_TEARDOWN_COMPLEMENTS``
        adds LP_CLOSE, whose flow authorises ``collect``. The broken case is a
        fee-harvest-only strategy, so that is what is asserted.
        """
        chain = _first_chain(connector)
        permissions, warnings = discover_permissions(chain, [connector], [_VERB])
        assert not warnings, f"{connector}/{chain} {_VERB} discovery emitted warnings: {warnings}"

        multisend = (MULTISEND_ADDRESSES.get(chain.lower()) or "").lower()
        core = [
            p
            for p in permissions
            if p.target.lower() != multisend and ({s.selector for s in p.function_selectors} - {ERC20_APPROVE_SELECTOR})
        ]
        assert core, (
            f"{connector}/{chain}: a {_VERB}-only manifest authorises no protocol contract. "
            f"A fee-harvest-only strategy would revert at execTransactionWithRole once Zodiac "
            f"batch enforcement is on (VIB-6057)."
        )

    @pytest.mark.parametrize("connector", ["uniswap_v3", "sushiswap_v3", "pancakeswap_v3"])
    def test_v3_family_authorises_the_npm_collect_selector(self, connector: str) -> None:
        """Pinned against the compiler's own constant, not a typed literal."""
        chain = _first_chain(connector)
        permissions, _ = discover_permissions(chain, [connector], [_VERB])
        selectors = {s.selector for p in permissions for s in p.function_selectors}
        assert NFT_POSITION_COLLECT_SELECTOR in selectors, (
            f"{connector}/{chain}: manifest is missing the NFT position-manager collect selector "
            f"{NFT_POSITION_COLLECT_SELECTOR}; got {sorted(selectors)}"
        )


class TestLeastPrivilege:
    """Availability is only half the contract — the grant must also be no wider.

    The assertions above prove a collect-only manifest authorises *something*.
    That is a strictly weaker claim than what this security perimeter needs: a
    manifest carrying the correct ``collect`` permission PLUS an arbitrary extra
    target, an extra selector, or ``send_allowed=True`` satisfies them all.
    Under-grant reverts at ``execTransactionWithRole``; over-grant silently
    authorises a call the strategy should never be able to make, and only the
    second one is a security defect. So this class pins the EXACT emitted grant,
    per connector and per declared Safe-reachable chain.
    """

    def test_exemptions_are_live(self) -> None:
        """A stale exemption would silently drop a connector from the strict test."""
        declaring = set(_declaring_connectors())
        stale = _NON_NFT_POSITION_COLLECTORS - declaring
        assert not stale, (
            f"{sorted(stale)} are exempted from the exact-grant assertion but no longer declare "
            f"{_VERB}. Remove them from _NON_NFT_POSITION_COLLECTORS — a stale exemption is how a "
            f"connector silently escapes the only test that checks its grant is not too wide."
        )

    def test_strict_universe_is_not_empty(self) -> None:
        """Guard against the parametrized class collecting zero cases and passing vacuously."""
        pairs = _nft_position_collect_pairs()
        assert pairs, (
            "no (connector, chain) pair is subject to the exact-grant assertion — the parametrized "
            "test below would collect nothing and report green while checking nothing"
        )
        assert {c for c, _ in pairs} >= {"uniswap_v3", "sushiswap_v3", "pancakeswap_v3", "uniswap_v4"}

    @pytest.mark.parametrize(("connector", "chain"), _nft_position_collect_pairs())
    def test_collect_only_manifest_grants_exactly_the_position_manager(self, connector: str, chain: str) -> None:
        permissions, warnings = discover_permissions(chain, [connector], [_VERB])
        assert not warnings, f"{connector}/{chain} {_VERB} discovery emitted warnings: {warnings}"

        expected_target = (LP_POSITION_MANAGERS.get(chain) or {}).get(connector)
        assert expected_target, f"no position manager registered for {connector} on {chain}"

        # Native value on a fee collection is never correct — ``collect`` is not payable.
        value_bearing = sorted(p.target.lower() for p in permissions if p.send_allowed)
        assert not value_bearing, (
            f"{connector}/{chain}: collect-only manifest sets send_allowed=True on {value_bearing}. "
            f"That authorises value-bearing calls the collect path never makes."
        )

        multisend = (MULTISEND_ADDRESSES.get(chain.lower()) or "").lower()
        core = [
            p
            for p in permissions
            if p.target.lower() != multisend and ({s.selector for s in p.function_selectors} - {ERC20_APPROVE_SELECTOR})
        ]
        emitted = sorted((p.target.lower(), tuple(sorted(s.selector for s in p.function_selectors))) for p in core)
        assert len(core) == 1, (
            f"{connector}/{chain}: expected exactly one protocol contract on a collect-only "
            f"manifest (the position manager {expected_target}); got {emitted}"
        )

        entry = core[0]
        assert entry.target.lower() == expected_target.lower(), (
            f"{connector}/{chain}: collect is authorised on {entry.target} but the registered "
            f"position manager is {expected_target}"
        )
        selectors = {s.selector for s in entry.function_selectors}
        assert len(selectors) == 1, (
            f"{connector}/{chain}: position manager authorises {sorted(selectors)} on a collect-only "
            f"manifest; exactly one selector is expected"
        )
        # operation 0 == CALL. A DELEGATECALL grant would hand the module the Safe's own storage.
        assert entry.operation == 0, (
            f"{connector}/{chain}: collect grant uses operation={entry.operation}, expected 0 (CALL)"
        )

    @pytest.mark.parametrize(
        ("connector", "chain"),
        [(c, ch) for c, ch in _nft_position_collect_pairs() if c != "uniswap_v4"],
    )
    def test_v3_family_collect_selector_is_bound_to_the_position_manager(self, connector: str, chain: str) -> None:
        """The selector must sit ON the NPM, on EVERY declared chain — not merely exist somewhere.

        ``uniswap_v4`` is excluded because its collect goes through PositionManager's
        multiplexed ``modifyLiquidities``, not the V3 ``collect`` selector; its target
        and single-selector shape are still pinned by the test above.
        """
        permissions, _ = discover_permissions(chain, [connector], [_VERB])
        expected_target = (LP_POSITION_MANAGERS.get(chain) or {}).get(connector)
        on_npm = {
            s.selector
            for p in permissions
            if p.target.lower() == (expected_target or "").lower()
            for s in p.function_selectors
        }
        assert on_npm == {NFT_POSITION_COLLECT_SELECTOR}, (
            f"{connector}/{chain}: position manager {expected_target} authorises {sorted(on_npm)}; "
            f"expected exactly {{{NFT_POSITION_COLLECT_SELECTOR}}}"
        )
