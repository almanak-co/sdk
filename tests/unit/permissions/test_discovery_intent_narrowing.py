"""Direct tests for ``_supported_intent_types_for`` — discovery's descriptor filter.

The narrowing this covers decides which ``(protocol, chain, intent)`` cells reach
the compiler, and therefore which ``(target, selector)`` pairs land in a Zodiac
Roles manifest. Getting it wrong is silent in both directions: too narrow and
every call reverts at ``execTransactionWithRole`` with an "unauthorized" that
names no cause; too wide and a manifest grants a verb the connector never
declared.

Before this file the branches were only exercised incidentally, through
whole-manifest assertions that stayed green whether or not the filter fired.
None of them asserted the ``warnings`` strings, which are the only signal a
thinner manifest was a *decision* rather than a discovery failure.
"""

from __future__ import annotations

from almanak.framework.permissions.discovery import _supported_intent_types_for

_LENDING = ["SUPPLY", "BORROW", "REPAY", "WITHDRAW"]


class TestPassThrough:
    """No descriptor, or no strategy support => nothing to narrow against."""

    def test_unknown_protocol_is_passed_through_unnarrowed(self) -> None:
        warnings: list[str] = []
        result = _supported_intent_types_for(
            chain="ethereum",
            protocol="definitely_not_a_protocol",
            intent_types=_LENDING,
            warnings=warnings,
        )
        # Returned as-is, NOT filtered to empty: emptying an unknown protocol's
        # manifest is the failure mode that reverts on-chain instead of here.
        assert result == _LENDING
        assert warnings == []


class TestChainNarrowing:
    """A chain the connector does not declare drops the protocol entirely."""

    def test_supported_chain_keeps_every_declared_intent(self) -> None:
        warnings: list[str] = []
        result = _supported_intent_types_for(
            chain="ethereum",
            protocol="aave_v3",
            intent_types=_LENDING,
            warnings=warnings,
        )
        assert result == _LENDING
        assert warnings == []

    def test_undeclared_chain_returns_none_and_explains_itself(self) -> None:
        # aave_v3 does not declare sonic (governance wound the market down).
        warnings: list[str] = []
        result = _supported_intent_types_for(
            chain="sonic",
            protocol="aave_v3",
            intent_types=_LENDING,
            warnings=warnings,
        )
        assert result is None
        assert warnings == ["Skipped unsupported permission discovery chain for aave_v3 on sonic"]


class TestIntentNarrowing:
    """A declared chain can still withhold individual verbs."""

    def test_per_intent_override_drops_only_the_withheld_verb(self) -> None:
        # aave_v3 declares mantle for SUPPLY/REPAY/WITHDRAW but withholds BORROW
        # (ALM-3075: base LTV 0 and no eMode enrolment in the compile path).
        warnings: list[str] = []
        result = _supported_intent_types_for(
            chain="mantle",
            protocol="aave_v3",
            intent_types=_LENDING,
            warnings=warnings,
        )
        assert result == ["SUPPLY", "REPAY", "WITHDRAW"]
        assert warnings == ["Skipped unsupported permission discovery cells for aave_v3 on mantle: ['BORROW']"]

    def test_requesting_only_a_withheld_verb_returns_none(self) -> None:
        warnings: list[str] = []
        result = _supported_intent_types_for(
            chain="mantle",
            protocol="aave_v3",
            intent_types=["BORROW"],
            warnings=warnings,
        )
        # None, not [] — the caller skips the protocol rather than proceeding
        # to build an empty permission set for it.
        assert result is None
        assert warnings == ["Skipped unsupported permission discovery cells for aave_v3 on mantle: ['BORROW']"]


class TestRecoveryVerbExtensions:
    """Two verbs are admitted despite being absent from ``strategy_intents``."""

    def test_perp_cancel_order_survives_the_descriptor_filter(self) -> None:
        # A framework-only teardown verb a strategy cannot author, so it is
        # deliberately not in strategy_intents — but its permission must still
        # be discoverable or stranded GMX orders cannot be cancelled.
        warnings: list[str] = []
        result = _supported_intent_types_for(
            chain="arbitrum",
            protocol="gmx_v2",
            intent_types=["PERP_CANCEL_ORDER"],
            warnings=warnings,
        )
        assert result == ["PERP_CANCEL_ORDER"]
        assert warnings == []

    def test_standalone_fee_collection_survives_when_the_connector_declares_it(self) -> None:
        # aerodrome_slipstream sets supports_standalone_fee_collection=True.
        warnings: list[str] = []
        result = _supported_intent_types_for(
            chain="base",
            protocol="aerodrome_slipstream",
            intent_types=["LP_COLLECT_FEES"],
            warnings=warnings,
        )
        assert result == ["LP_COLLECT_FEES"]
        assert warnings == []


class TestChainScopedBrandsResolveBeforeLookup:
    """A brand spelling must be narrowed, not silently waved through.

    Chain-scoped fork brands are NOT connector discovery keys: ``agni`` and
    ``velodrome`` reach a descriptor only through ``normalize_protocol``.
    Looking the raw name up in the registry returns ``None``, which takes the
    "no descriptor, nothing to narrow against" pass-through — so a manifest
    built for ``protocol="agni"`` was never gated on chain OR intent at all,
    and would grant every verb the caller happened to ask for.
    """

    def test_brand_is_narrowed_to_its_own_declaration(self) -> None:
        warnings: list[str] = []
        result = _supported_intent_types_for(
            chain="mantle",
            protocol="agni",
            intent_types=["SWAP", "LP_OPEN", *_LENDING],
            warnings=warnings,
        )
        # Agni is a Uniswap V3 fork: the DEX verbs survive, the lending verbs
        # it never declares are dropped — the pass-through would have kept them.
        assert result == ["SWAP", "LP_OPEN"]
        assert warnings == [
            "Skipped unsupported permission discovery cells for agni on mantle: "
            "['BORROW', 'REPAY', 'SUPPLY', 'WITHDRAW']"
        ]

    def test_brand_keeps_the_coverage_its_canonical_declares(self) -> None:
        # velodrome/optimism resolves to aerodrome, which IS declared there.
        # Narrowing must not empty a real manifest: an empty one reverts at
        # execTransactionWithRole with an unauthorized that names no cause.
        warnings: list[str] = []
        result = _supported_intent_types_for(
            chain="optimism",
            protocol="velodrome",
            intent_types=["SWAP", "LP_OPEN", "LP_CLOSE"],
            warnings=warnings,
        )
        assert result == ["SWAP", "LP_OPEN", "LP_CLOSE"]
        assert warnings == []

    def test_brand_outside_its_chain_scope_still_fails_open(self) -> None:
        # ``agni`` is scoped to mantle, so on arbitrum it normalizes to itself
        # and resolves to no descriptor. That is the unknown-protocol case, not
        # an unsupported-chain case: narrowing it to empty here would strand a
        # protocol the registry simply has no opinion about.
        warnings: list[str] = []
        result = _supported_intent_types_for(
            chain="arbitrum",
            protocol="agni",
            intent_types=["SWAP"],
            warnings=warnings,
        )
        assert result == ["SWAP"]
        assert warnings == []
