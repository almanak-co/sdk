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

from almanak.framework.connectors.registry import (
    KNOWN_VENUES,
    ConnectorManifest,
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


def test_known_venues_uses_bnb_not_bsc() -> None:
    # ``resolve_chain_name`` normalises "bsc" -> "bnb"; the canonical form
    # in the registry must match the normalised output.
    assert "bnb" in KNOWN_VENUES
    assert "bsc" not in KNOWN_VENUES


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
