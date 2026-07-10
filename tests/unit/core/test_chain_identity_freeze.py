"""Frozen chain-identity contract (Chain-enum removal ladder, Rung 1).

``FROZEN_CHAIN_NAMES`` is the explicit, human-reviewed inventory of supported
chains. Editing this set is the deliberate review act when adding or removing
a chain — do NOT derive it from ``ChainRegistry`` (that would be tautological:
a descriptor module silently dropped from discovery would shrink both sides
and pass unnoticed).
"""

from almanak.core.chains import ChainRegistry

FROZEN_CHAIN_NAMES = frozenset(
    {
        "arbitrum",
        "avalanche",
        "base",
        "berachain",
        "blast",
        "bsc",
        "ethereum",
        "hyperevm",
        "linea",
        "mantle",
        "monad",
        "optimism",
        "plasma",
        "polygon",
        "robinhood",
        "solana",
        "sonic",
        "xlayer",
        "zerog",
    }
)


def test_registry_names_match_frozen_inventory() -> None:
    """Set equality (not subset): widening AND narrowing both need review."""
    assert set(ChainRegistry.names()) == FROZEN_CHAIN_NAMES


def test_legacy_uppercase_serialized_names_resolve_forever() -> None:
    """Persisted records written before the Chain-enum removal carry UPPERCASE
    names (the enum serialized ``.value`` — e.g. ``"ETHEREUM"``). The read-path
    contract is case-insensitive resolution, forever.
    """
    for name in sorted(FROZEN_CHAIN_NAMES):
        assert ChainRegistry.resolve(name.upper()).name == name


def test_registry_ordering_is_stable() -> None:
    """``all()`` and ``names()`` agree and are sorted by canonical name.

    Pins the Rung-2 sort-key change (enum name -> descriptor name) as a
    no-op: canonical names are the lowercase enum names, so lexicographic
    order is identical.
    """
    names = [d.name for d in ChainRegistry.all()]
    assert names == list(ChainRegistry.names())
    assert names == sorted(names)
