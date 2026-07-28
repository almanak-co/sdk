"""Validation behaviour for :class:`ConnectorManifest`.

Every row of the verification matrix that fails at decoration time
(scenarios 6-12 plus the keyword-only enforcement of scenario 17) is
asserted here. The error messages are part of the contract — a bad
``chains`` tuple must produce a message that names the offending value and
points the author at ``chains=None`` for off-chain venues. Without that,
the back-fill across ~42 connectors becomes a guessing game.
"""

from __future__ import annotations

import pytest

from almanak.connectors._strategy_base.registry import (
    KNOWN_VENUES,
    ConnectorManifest,
    IntentChainExclusion,
    register_connector,
)
from almanak.framework.intents.vocabulary import IntentType


def test_minimal_valid_manifest_constructs() -> None:
    m = ConnectorManifest(
        name="aave_v3",
        intents=(IntentType.SUPPLY,),
        chains=("ethereum",),
    )
    assert m.name == "aave_v3"
    assert m.intents == (IntentType.SUPPLY,)
    assert m.chains == ("ethereum",)


def test_chains_none_is_accepted_for_off_chain_venues() -> None:
    m = ConnectorManifest(name="kraken", intents=(IntentType.SWAP,), chains=None)
    assert m.chains is None


@pytest.mark.parametrize("bad_name", ["", "   ", "\t\n", None, 42])
def test_name_must_be_non_empty_string(bad_name: object) -> None:
    with pytest.raises(ValueError, match=r"name must be a non-empty string"):
        ConnectorManifest(
            name=bad_name,  # type: ignore[arg-type]
            intents=(IntentType.SWAP,),
            chains=("ethereum",),
        )


def test_intents_empty_tuple_is_rejected() -> None:
    with pytest.raises(ValueError, match=r"intents must be a non-empty tuple"):
        ConnectorManifest(name="x", intents=(), chains=("ethereum",))


def test_intents_non_tuple_is_rejected() -> None:
    with pytest.raises(ValueError, match=r"intents must be a non-empty tuple"):
        ConnectorManifest(
            name="x",
            intents=[IntentType.SWAP],  # type: ignore[arg-type]
            chains=("ethereum",),
        )


def test_intents_must_be_intenttype_members() -> None:
    with pytest.raises(ValueError, match=r"must contain only IntentType members"):
        ConnectorManifest(
            name="x",
            intents=("SWAP",),  # type: ignore[arg-type]
            chains=("ethereum",),
        )


def test_intents_duplicates_rejected() -> None:
    with pytest.raises(ValueError, match=r"intents contains duplicates"):
        ConnectorManifest(
            name="x",
            intents=(IntentType.SWAP, IntentType.SWAP),
            chains=("ethereum",),
        )


def test_chains_empty_tuple_is_rejected_with_hint() -> None:
    with pytest.raises(
        ValueError,
        match=r"chains must be None or a non-empty tuple",
    ) as exc:
        ConnectorManifest(name="x", intents=(IntentType.SWAP,), chains=())
    # The author needs a clear pointer to the off-chain path; otherwise
    # they'll resurrect the empty-tuple form thinking it means "no chains".
    assert "chains=None" in str(exc.value)


def test_chains_unknown_value_is_rejected() -> None:
    with pytest.raises(ValueError, match=r"not in KNOWN_VENUES"):
        ConnectorManifest(
            name="x",
            intents=(IntentType.SWAP,),
            chains=("ethereuem",),  # typo
        )


def test_chains_duplicates_rejected() -> None:
    with pytest.raises(ValueError, match=r"chains contains duplicates"):
        ConnectorManifest(
            name="x",
            intents=(IntentType.SWAP,),
            chains=("ethereum", "ethereum"),
        )


def test_chains_must_be_strings() -> None:
    with pytest.raises(ValueError, match=r"chains must contain only strings"):
        ConnectorManifest(
            name="x",
            intents=(IntentType.SWAP,),
            chains=("ethereum", 1),  # type: ignore[arg-type]
        )


def test_known_venues_includes_hyperliquid_and_solana() -> None:
    # These two were locked-in design calls (Q5c, Q5a) — guard against
    # accidental removal in future refactors of KNOWN_VENUES.
    assert "hyperliquid" in KNOWN_VENUES
    assert "solana" in KNOWN_VENUES


def test_known_venues_uses_canonical_bsc_not_bnb_alias() -> None:
    # VIB-5293: the strategy registry venue
    # vocabulary IS the ChainRegistry canonical vocabulary. "bnb" is an
    # alias, tolerated at the manifest boundary via canonicalization —
    # never a registry key.
    assert "bsc" in KNOWN_VENUES
    assert "bnb" not in KNOWN_VENUES


def test_chains_alias_canonicalizes_at_construction() -> None:
    # A registered ChainRegistry alias ("bnb") is accepted and rewritten to
    # its canonical name, so every downstream consumer of
    # ConnectorManifest.chains reads one vocabulary.
    m = ConnectorManifest(
        name="x",
        intents=(IntentType.SWAP,),
        chains=("bnb", "ethereum"),
    )
    assert m.chains == ("bsc", "ethereum")


def test_chains_non_registry_venue_passes_through() -> None:
    # Venues the chain registry does not model (Hyperliquid L1) are
    # first-class KNOWN_VENUES entries and must survive canonicalization
    # verbatim.
    m = ConnectorManifest(
        name="x",
        intents=(IntentType.PERP_OPEN,),
        chains=("hyperliquid",),
    )
    assert m.chains == ("hyperliquid",)


def test_chains_alias_and_canonical_duplicate_rejected() -> None:
    # Declaring both the alias and the canonical name is a duplicate after
    # canonicalization — fail loud rather than silently deduping.
    with pytest.raises(ValueError, match=r"chains contains duplicates"):
        ConnectorManifest(
            name="x",
            intents=(IntentType.SWAP,),
            chains=("bnb", "bsc"),
        )


def test_register_connector_keyword_only() -> None:
    # Positional args are rejected so call sites stay self-documenting
    # at the back-fill scale.
    with pytest.raises(TypeError):
        register_connector("aave_v3", (IntentType.SWAP,), ("ethereum",))  # type: ignore[misc]


def test_frozen_dataclass_cannot_be_mutated() -> None:
    from dataclasses import FrozenInstanceError

    m = ConnectorManifest(name="x", intents=(IntentType.SWAP,), chains=("ethereum",))
    with pytest.raises(FrozenInstanceError):
        m.name = "y"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# intent_chain_exclusions (VIB-6111) — narrowing-only per-(intent, chain)
# support exclusions, plus the two narrowed read helpers that are the ONLY
# supported way for a consumer to ask "is this cell actually supported?".
# ---------------------------------------------------------------------------


def _manifest(**overrides: object) -> ConnectorManifest:
    fields: dict[str, object] = {
        "name": "aave_v3",
        "intents": (IntentType.SUPPLY, IntentType.BORROW, IntentType.REPAY),
        "chains": ("ethereum", "mantle"),
    }
    fields.update(overrides)
    return ConnectorManifest(**fields)  # type: ignore[arg-type]


def _exclusion(**overrides: object) -> IntentChainExclusion:
    fields: dict[str, object] = {
        "intent": IntentType.BORROW,
        "chains": frozenset({"mantle"}),
        "reason": "governance zeroed ltv on every reserve",
        "ticket": "VIB-6111",
    }
    fields.update(overrides)
    return IntentChainExclusion(**fields)  # type: ignore[arg-type]


def test_exclusion_narrows_chains_for_intent_only_for_that_intent() -> None:
    m = _manifest(intent_chain_exclusions=(_exclusion(),))

    assert m.chains_for_intent(IntentType.BORROW) == ("ethereum",)
    # Every other verb keeps the full declared chain set.
    assert m.chains_for_intent(IntentType.SUPPLY) == ("ethereum", "mantle")
    assert m.chains_for_intent(IntentType.REPAY) == ("ethereum", "mantle")
    # The chain itself is NOT removed — narrowing, not deletion.
    assert m.chains == ("ethereum", "mantle")


def test_intents_for_chain_drops_only_excluded_verbs() -> None:
    m = _manifest(intent_chain_exclusions=(_exclusion(),))

    assert m.intents_for_chain("mantle") == (IntentType.SUPPLY, IntentType.REPAY)
    assert m.intents_for_chain("ethereum") == (
        IntentType.SUPPLY,
        IntentType.BORROW,
        IntentType.REPAY,
    )


def test_intents_for_chain_accepts_alias_names() -> None:
    m = ConnectorManifest(
        name="x",
        intents=(IntentType.SWAP, IntentType.BORROW),
        chains=("bsc", "ethereum"),
        intent_chain_exclusions=(_exclusion(chains=frozenset({"bsc"})),),
    )

    # "bnb" is the registered alias for "bsc" — both spellings must resolve.
    assert m.intents_for_chain("bnb") == (IntentType.SWAP,)
    assert m.chains_for_intent(IntentType.BORROW) == ("ethereum",)


def test_exclusion_chains_canonicalize_before_the_subset_check() -> None:
    # Declared with the alias; canonicalization must run BEFORE validation or
    # the subset check would spuriously reject "bnb" as not in chains.
    m = ConnectorManifest(
        name="x",
        intents=(IntentType.SWAP, IntentType.BORROW),
        chains=("bnb", "ethereum"),
        intent_chain_exclusions=(_exclusion(chains=frozenset({"bnb"})),),
    )

    assert m.chains == ("bsc", "ethereum")
    assert m.intent_chain_exclusions is not None
    assert m.intent_chain_exclusions[0].chains == frozenset({"bsc"})
    assert m.chains_for_intent(IntentType.BORROW) == ("ethereum",)


def test_intents_for_chain_returns_empty_for_undeclared_chain() -> None:
    # An exclusion narrows; it never widens. A chain the connector never
    # declared supports nothing.
    assert _manifest().intents_for_chain("solana") == ()


def test_helpers_return_empty_for_off_chain_venue() -> None:
    m = ConnectorManifest(name="kraken", intents=(IntentType.SWAP,), chains=None)
    assert m.chains_for_intent(IntentType.SWAP) == ()
    assert m.intents_for_chain("ethereum") == ()


def test_no_exclusions_is_the_full_cross_product() -> None:
    m = _manifest()
    assert m.chains_for_intent(IntentType.BORROW) == ("ethereum", "mantle")
    assert m.excluded_chains(IntentType.BORROW) == frozenset()
    assert m.exclusion_for(IntentType.BORROW, "mantle") is None


def test_exclusion_for_surfaces_reason_and_ticket() -> None:
    m = _manifest(intent_chain_exclusions=(_exclusion(),))
    found = m.exclusion_for(IntentType.BORROW, "mantle")
    assert found is not None
    assert found.ticket == "VIB-6111"
    assert "ltv" in found.reason


def test_exclusions_non_tuple_rejected() -> None:
    with pytest.raises(ValueError, match=r"intent_chain_exclusions must be a tuple"):
        _manifest(intent_chain_exclusions=[_exclusion()])


def test_exclusions_wrong_element_type_rejected() -> None:
    with pytest.raises(ValueError, match=r"must contain only IntentChainExclusion"):
        _manifest(intent_chain_exclusions=("BORROW",))


def test_exclusion_intent_must_be_intenttype() -> None:
    with pytest.raises(ValueError, match=r"intent must be an IntentType member"):
        _manifest(intent_chain_exclusions=(_exclusion(intent="BORROW"),))


def test_exclusion_intent_must_be_declared() -> None:
    with pytest.raises(ValueError, match=r"is not declared in ConnectorManifest.intents"):
        _manifest(intent_chain_exclusions=(_exclusion(intent=IntentType.WITHDRAW),))


@pytest.mark.parametrize("bad_chains", [frozenset(), {"mantle"}, ("mantle",)])
def test_exclusion_chains_container_validated(bad_chains: object) -> None:
    with pytest.raises(ValueError, match=r"chains must be a non-empty frozenset"):
        _manifest(intent_chain_exclusions=(_exclusion(chains=bad_chains),))


def test_exclusion_chains_blank_string_rejected() -> None:
    with pytest.raises(ValueError, match=r"chains must contain only non-empty strings"):
        _manifest(intent_chain_exclusions=(_exclusion(chains=frozenset({"   "})),))


def test_exclusion_chain_must_be_in_declared_chains() -> None:
    with pytest.raises(ValueError, match=r"contains chains not in ConnectorManifest.chains"):
        _manifest(intent_chain_exclusions=(_exclusion(chains=frozenset({"linea"})),))


def test_exclusions_rejected_when_chains_is_none() -> None:
    with pytest.raises(ValueError, match=r"may not be set when chains is None"):
        ConnectorManifest(
            name="kraken",
            intents=(IntentType.SWAP,),
            chains=None,
            intent_chain_exclusions=(
                _exclusion(intent=IntentType.SWAP, chains=frozenset({"ethereum"})),
            ),
        )


def test_exclusion_of_every_chain_rejected() -> None:
    with pytest.raises(ValueError, match=r"drop the intent from strategy_intents instead"):
        _manifest(intent_chain_exclusions=(_exclusion(chains=frozenset({"ethereum", "mantle"})),))


@pytest.mark.parametrize("blank", ["", "   ", None])
def test_exclusion_reason_must_be_non_empty(blank: object) -> None:
    with pytest.raises(ValueError, match=r"reason must be a non-empty string"):
        _manifest(intent_chain_exclusions=(_exclusion(reason=blank),))


@pytest.mark.parametrize("blank", ["", "   ", None])
def test_exclusion_ticket_must_be_non_empty(blank: object) -> None:
    with pytest.raises(ValueError, match=r"ticket must be a non-empty string"):
        _manifest(intent_chain_exclusions=(_exclusion(ticket=blank),))


def test_exclusion_duplicate_intent_keys_rejected() -> None:
    with pytest.raises(ValueError, match=r"has duplicate intent keys"):
        _manifest(
            intent_chain_exclusions=(
                _exclusion(chains=frozenset({"mantle"})),
                _exclusion(chains=frozenset({"ethereum"})),
            )
        )


def test_register_connector_forwards_exclusions() -> None:
    from almanak.connectors._strategy_base.registry import ConnectorRegistry

    register_connector(
        name="exclusion_forwarding_probe",
        intents=(IntentType.SUPPLY, IntentType.BORROW),
        chains=("ethereum", "mantle"),
        intent_chain_exclusions=(_exclusion(),),
    )
    registered = ConnectorRegistry.get("exclusion_forwarding_probe")
    assert registered is not None
    assert registered.chains_for_intent(IntentType.BORROW) == ("ethereum",)


def test_supports_is_the_membership_question() -> None:
    """VIB-6111: ``supports()`` shipped with no callers and no tests.

    Its docstring makes two non-obvious contract claims that exist to steer
    future consumers — ``False`` for off-chain venues, and "``exclusion_for(...)
    is None`` does NOT mean supported". Without a test the first real consumer
    would be validating that contract in production.
    """
    m = ConnectorManifest(
        name="vib6111_supports_probe",
        intents=(IntentType.SUPPLY, IntentType.BORROW),
        chains=("ethereum", "bsc"),
        intent_chain_exclusions=(
            IntentChainExclusion(
                intent=IntentType.BORROW,
                chains=frozenset({"bsc"}),
                reason="probe",
                ticket="VIB-6111",
            ),
        ),
    )
    # Declared, non-excluded cell.
    assert m.supports(IntentType.SUPPLY, "ethereum") is True
    assert m.supports(IntentType.BORROW, "ethereum") is True
    # Declared chain, EXCLUDED verb.
    assert m.supports(IntentType.BORROW, "bsc") is False
    assert m.supports(IntentType.SUPPLY, "bsc") is True
    # Alias input must fold to the canonical name before matching.
    assert m.supports(IntentType.BORROW, "bnb") is False
    assert m.supports(IntentType.SUPPLY, "bnb") is True
    # Undeclared intent and undeclared chain are both False — an exclusion
    # narrows, it never widens, and neither has an exclusion covering it.
    assert m.supports(IntentType.SWAP, "ethereum") is False
    assert m.supports(IntentType.SUPPLY, "solana") is False
    # exclusion_for is NOT the membership question: it returns None for an
    # undeclared cell too, which is precisely why supports() exists.
    assert m.exclusion_for(IntentType.SWAP, "ethereum") is None
    assert m.supports(IntentType.SWAP, "ethereum") is False


def test_supports_is_false_for_offchain_venues() -> None:
    """Documented contract: the matrix is on-chain only."""
    m = ConnectorManifest(
        name="vib6111_offchain_probe",
        intents=(IntentType.SWAP,),
        chains=None,
    )
    assert m.supports(IntentType.SWAP, "ethereum") is False
    assert m.chains_for_intent(IntentType.SWAP) == ()
    assert m.intents_for_chain("ethereum") == ()


def test_supports_is_false_for_offchain_venue_named_in_its_docstring() -> None:
    """The off-chain claim was documented but never asserted."""
    m = ConnectorManifest(name="vib6111_kraken_like", intents=(IntentType.SWAP,), chains=None)
    assert m.supports(IntentType.SWAP, "ethereum") is False


def test_manifest_rejects_chain_with_every_intent_excluded() -> None:
    """The DUAL invariant (VIB-6111) — untested until now on either twin.

    Per-entry rejects "one intent excluded on every chain". This is its dual:
    a chain on which EVERY intent is excluded leaves the connector claiming a
    chain it supports nothing on.
    """
    with pytest.raises(ValueError, match="supports nothing on"):
        ConnectorManifest(
            name="vib6111_dead_chain",
            intents=(IntentType.SWAP, IntentType.SUPPLY),
            chains=("ethereum", "mantle"),
            intent_chain_exclusions=(
                IntentChainExclusion(
                    intent=IntentType.SWAP,
                    chains=frozenset({"mantle"}),
                    reason="r",
                    ticket="VIB-6111",
                ),
                IntentChainExclusion(
                    intent=IntentType.SUPPLY,
                    chains=frozenset({"mantle"}),
                    reason="r",
                    ticket="VIB-6111",
                ),
            ),
        )


def test_manifest_rejects_matrix_entries_combined_with_exclusions() -> None:
    from almanak.connectors._strategy_base.registry import MatrixEntry as _MatrixEntry

    """matrix_entries is published verbatim and bypasses narrowing, so the two
    cannot coexist without the rendered matrix outrunning the declaration."""
    with pytest.raises(ValueError, match="matrix_entries"):
        ConnectorManifest(
            name="vib6111_both_declared",
            intents=(IntentType.SWAP, IntentType.SUPPLY),
            chains=("ethereum", "mantle"),
            matrix_entries=(
                _MatrixEntry(matrix_name="vib6111_both_declared", category="swap", chains=frozenset({"mantle"})),
            ),
            intent_chain_exclusions=(
                IntentChainExclusion(
                    intent=IntentType.SWAP,
                    chains=frozenset({"mantle"}),
                    reason="r",
                    ticket="VIB-6111",
                ),
            ),
        )


def test_descriptor_rejects_chain_with_every_intent_excluded() -> None:
    """Descriptor twin of the dual invariant, including alias folding."""
    from almanak.connectors._connector import Connector, StrategyIntentChainExclusion
    from almanak.connectors._connector_descriptor import ProtocolKind

    with pytest.raises(ValueError, match="supports nothing on"):
        Connector(
            name="vib6111_desc_dead_chain",
            kind=ProtocolKind.LENDING,
            strategy_intents=("SWAP", "SUPPLY"),
            strategy_chains=("ethereum", "bsc"),
            strategy_intent_chain_exclusions=(
                # Declared with the ALIAS on purpose: the descriptor must fold
                # it before deciding the chain is fully excluded.
                StrategyIntentChainExclusion(
                    intent="SWAP", chains=frozenset({"bnb"}), reason="r", ticket="VIB-6111"
                ),
                StrategyIntentChainExclusion(
                    intent="SUPPLY", chains=frozenset({"bsc"}), reason="r", ticket="VIB-6111"
                ),
            ),
        )


def test_descriptor_rejects_matrix_entries_combined_with_exclusions() -> None:
    from almanak.connectors._connector import Connector, StrategyIntentChainExclusion, StrategyMatrixEntry
    from almanak.connectors._connector_descriptor import ProtocolKind

    with pytest.raises(ValueError, match="strategy_matrix_entries"):
        Connector(
            name="vib6111_desc_both",
            kind=ProtocolKind.LENDING,
            strategy_intents=("SWAP", "SUPPLY"),
            strategy_chains=("ethereum", "mantle"),
            strategy_matrix_entries=(
                StrategyMatrixEntry(matrix_name="vib6111_desc_both", category="swap", chains=frozenset({"mantle"})),
            ),
            strategy_intent_chain_exclusions=(
                StrategyIntentChainExclusion(
                    intent="SWAP", chains=frozenset({"mantle"}), reason="r", ticket="VIB-6111"
                ),
            ),
        )
