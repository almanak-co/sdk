"""Connector-manifest-derived venue support for discovered pools (ALM-10098)."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector_descriptor import (
    CONNECTOR_REGISTRY,
    Connector,
    ImportRef,
    SupportedChainsSpec,
)
from almanak.connectors._strategy_base.compiler_registry import CompilerRegistry
from almanak.connectors._strategy_base.venue_support import VenueSupportIndex, normalize_dex_id
from almanak.core.chains.base import DESCRIPTOR as BASE
from almanak.core.chains.ethereum import DESCRIPTOR as ETHEREUM
from almanak.core.intent_types import IntentType

# A venue declaration is validated against the connector's COMPILER keys, so a
# fixture without a compiler could declare nothing at all. The ref is lazy and
# never loaded here; only the key set it implies is under test.
_A_COMPILER = ImportRef(module="almanak.connectors.orca.compiler", attribute="OrcaCompiler")


def _connector(
    name: str,
    *,
    venue_dex_ids=None,
    intents=(IntentType.SWAP, IntentType.LP_OPEN),
    chains=(BASE,),
    aliases=(),
    protocol_overrides=None,
    compiler_protocols=None,
):
    return Connector(
        name=name,
        kind=ProtocolKind.LP,
        aliases=tuple(aliases),
        strategy_intents=tuple(intents),
        supported_chains=SupportedChainsSpec(
            chains=tuple(chains),
            protocol_overrides=protocol_overrides or {},
        ),
        compiler=_A_COMPILER,
        compiler_protocols=compiler_protocols,
        venue_dex_ids=venue_dex_ids,
    )


def _registry(*connectors):
    class _Fake:
        def all(self):
            return tuple(connectors)

    return patch("almanak.connectors._strategy_base.venue_support.CONNECTOR_REGISTRY", _Fake())


def test_separator_and_case_fold_but_nothing_else_does():
    assert normalize_dex_id("Uniswap-V3-Base") == "uniswap_v3_base"
    assert normalize_dex_id("  uniswap_v3  ") == "uniswap_v3"
    # The network segment is identity, not noise: folding it away would let one
    # chain's venue answer for another's.
    assert normalize_dex_id("uniswap-v3-base") != normalize_dex_id("uniswap-v3")


class TestClassification:
    def test_declared_venue_is_supported_with_its_intents(self):
        alpha = _connector("alpha", venue_dex_ids={"base": {"alpha-base": "alpha"}})
        with _registry(alpha):
            support = VenueSupportIndex("base").classify("alpha-base", product_distinct=True)
        assert support.status == "supported"
        assert support.protocols == ("alpha",)
        assert support.intents == (IntentType.SWAP.value, IntentType.LP_OPEN.value)

    def test_unmatched_venue_is_unsupported_only_when_the_chain_is_complete(self):
        alpha = _connector("alpha", venue_dex_ids={"base": {"alpha-base": "alpha"}})
        with _registry(alpha):
            assert VenueSupportIndex("base").classify("someswap", product_distinct=True).status == "unsupported"

    def test_an_undeclared_connector_makes_a_non_match_unknown_not_unsupported(self):
        alpha = _connector("alpha", venue_dex_ids={"base": {"alpha-base": "alpha"}})
        beta = _connector("beta")  # undeclared: its venues are unknown, not absent
        with _registry(alpha, beta):
            index = VenueSupportIndex("base")
            assert index.complete is False
            assert index.undeclared_protocols == ("beta",)
            assert index.classify("someswap", product_distinct=True).status == "unknown"
            # A positive match is evidence about the connector that declared it,
            # so it stands even while the chain is incomplete.
            assert index.classify("alpha-base", product_distinct=True).status == "supported"

    def test_a_declared_empty_chain_is_a_complete_answer(self):
        alpha = _connector("alpha", venue_dex_ids={"base": {"alpha-base": "alpha"}})
        router = _connector("router", venue_dex_ids={"base": {}}, intents=(IntentType.SWAP,))
        with _registry(alpha, router):
            index = VenueSupportIndex("base")
            assert index.complete is True
            assert index.classify("someswap", product_distinct=True).status == "unsupported"

    def test_non_product_distinct_ids_can_never_claim_support(self):
        alpha = _connector("alpha", venue_dex_ids={"base": {"alpha-base": "alpha"}})
        with _registry(alpha):
            support = VenueSupportIndex("base").classify("alpha-base", product_distinct=False)
        assert support.status == "unknown"
        assert support.protocols == ()

    def test_a_blank_dex_id_is_unknown_not_unsupported(self):
        alpha = _connector("alpha", venue_dex_ids={"base": {"alpha-base": "alpha"}})
        with _registry(alpha):
            assert VenueSupportIndex("base").classify("   ", product_distinct=True).status == "unknown"

    def test_a_shared_venue_reports_every_connector_that_can_act_on_it(self):
        swapper = _connector("swapper", venue_dex_ids={"base": {"shared": "swapper"}}, intents=(IntentType.SWAP,))
        lper = _connector(
            "lper",
            venue_dex_ids={"base": {"shared": "lper"}},
            intents=(IntentType.LP_OPEN, IntentType.LP_CLOSE),
        )
        with _registry(swapper, lper):
            support = VenueSupportIndex("base").classify("shared", product_distinct=True)
        assert support.protocols == ("lper", "swapper")
        assert set(support.intents) == {IntentType.SWAP.value, IntentType.LP_OPEN.value, IntentType.LP_CLOSE.value}

    def test_a_declaration_on_another_chain_never_answers_for_this_one(self):
        alpha = _connector("alpha", venue_dex_ids={"ethereum": {"alpha": "alpha"}}, chains=(BASE, ETHEREUM))
        with _registry(alpha):
            assert VenueSupportIndex("base").classify("alpha", product_distinct=True).status == "unknown"
            assert VenueSupportIndex("ethereum").classify("alpha", product_distinct=True).status == "supported"

    def test_a_venue_reports_the_protocol_key_its_own_product_compiles_under(self):
        """One connector, two LP models: the alias must not answer as the canonical key.

        A venue declared for an alias whose execution model differs is the case
        that routes a concentrated-liquidity pool into a fungible-LP compiler.
        """
        alpha = _connector(
            "alpha",
            venue_dex_ids={"base": {"alpha-classic": "alpha", "alpha-cl": "alpha_cl"}, "ethereum": {}},
            chains=(BASE, ETHEREUM),
            aliases=("alpha_cl",),
            protocol_overrides={"alpha_cl": (BASE,)},
        )
        with _registry(alpha):
            index = VenueSupportIndex("base")
            assert index.classify("alpha-classic", product_distinct=True).protocols == ("alpha",)
            assert index.classify("alpha-cl", product_distinct=True).protocols == ("alpha_cl",)


class TestManifestValidation:
    def test_a_declaration_cannot_name_an_unsupported_chain(self):
        with pytest.raises(ValueError, match="does not support"):
            _connector("alpha", venue_dex_ids={"ethereum": {"alpha": "alpha"}})

    def test_ids_must_be_lowercase_so_lookups_cannot_silently_miss(self):
        with pytest.raises(ValueError, match="lowercase"):
            _connector("alpha", venue_dex_ids={"base": {"Alpha-Base": "alpha"}})

    def test_ids_that_fold_to_the_same_key_are_rejected(self):
        with pytest.raises(ValueError, match="same"):
            _connector("alpha", venue_dex_ids={"base": {"alpha-base": "alpha", "alpha_base": "alpha"}})

    def test_a_declaration_cannot_name_a_protocol_no_compiler_resolves(self):
        with pytest.raises(ValueError, match="no 'alpha' compiler resolves"):
            _connector("alpha", venue_dex_ids={"base": {"alpha-base": "beta"}})

    def test_a_key_the_connector_owns_but_does_not_compile_is_rejected(self):
        """The `orca` shape: `orca` is an owned identity key, `orca_whirlpools` compiles.

        Validating against ownership would accept the name and mark a venue
        executable under a protocol that reaches no compiler.
        """
        with pytest.raises(ValueError, match="no 'alpha' compiler resolves"):
            _connector(
                "alpha",
                aliases=("alpha_cl",),
                compiler_protocols=("alpha_cl",),
                venue_dex_ids={"base": {"alpha-base": "alpha"}},
            )

    def test_a_padded_id_is_rejected_rather_than_silently_stripped_at_lookup(self):
        """Lookup strips, so a padded key would validate and then quietly work.

        A declaration that only works because the reader is lenient is a
        declaration the next reader can break by tightening the reader.
        """
        with pytest.raises(ValueError, match="lowercase"):
            _connector("alpha", venue_dex_ids={"base": {" alpha-base ": "alpha"}})

    def test_a_declaration_cannot_outlive_its_protocols_chain_coverage(self):
        """The Slipstream-on-Optimism shape: the alias is owned, but not there.

        Without this check a connector can claim a venue for a product it does
        not deploy on that chain, and the claim reads as executable support.
        """
        with pytest.raises(ValueError, match="not supported on 'ethereum'"):
            _connector(
                "alpha",
                venue_dex_ids={"ethereum": {"alpha-cl": "alpha_cl"}},
                chains=(BASE, ETHEREUM),
                aliases=("alpha_cl",),
                protocol_overrides={"alpha_cl": (BASE,)},
            )


class TestShippedManifests:
    """Venue declarations read off the real connector manifests.

    Every id asserted here was resolved against the CoinGecko Onchain dex
    catalogue and then confirmed on-chain by reading the pool's ``factory()``
    and comparing it with the connector's own address tables.
    """

    def test_pancakeswap_v3_on_bsc_is_an_executable_venue(self):
        support = VenueSupportIndex("bsc").classify("pancakeswap-v3-bsc", product_distinct=True)
        assert support.status == "supported"
        assert support.protocols == ("pancakeswap_v3",)
        assert IntentType.SWAP.value in support.intents

    def test_slipstream_resolves_under_its_own_protocol_not_the_classic_one(self):
        index = VenueSupportIndex("base")
        assert index.classify("aerodrome-base", product_distinct=True).protocols == ("aerodrome",)
        for dex_id in ("aerodrome-slipstream", "aerodrome-slipstream-3"):
            assert index.classify(dex_id, product_distinct=True).protocols == ("aerodrome_slipstream",)

    def test_the_unadmitted_slipstream_factory_is_not_claimed(self):
        """``aerodrome-slipstream-2`` is the "Gauge Caps" factory (0xaDe65c38…).

        ``addresses.py`` deliberately does not register it, so an LP_OPEN aimed
        at one of its pools cannot compile; claiming it would be a promise the
        connector refuses to keep.
        """
        assert VenueSupportIndex("base").classify("aerodrome-slipstream-2", product_distinct=True).status == (
            "unsupported"
        )

    def test_velodrome_v2_on_optimism_is_an_executable_venue(self):
        index = VenueSupportIndex("optimism")
        support = index.classify("velodrome-finance-v2", product_distinct=True)
        assert support.status == "supported"
        assert support.protocols == ("aerodrome",)
        assert IntentType.LP_OPEN.value in support.intents
        # V1 and Slipstream run on factories this connector does not register.
        for dex_id in ("velodrome", "velodrome-finance-slipstream", "velodrome-slipstream-v2-optimism"):
            assert index.classify(dex_id, product_distinct=True).status == "unsupported"

    def test_uniswap_v4_on_bsc_is_an_executable_venue(self):
        index = VenueSupportIndex("bsc")
        assert index.complete is True
        support = index.classify("uniswap-v4-bsc", product_distinct=True)
        assert support.status == "supported"
        assert support.protocols == ("uniswap_v4",)
        # With BSC complete, an unowned BSC venue is ruled out rather than unknown.
        assert index.classify("thena-fusion", product_distinct=True).status == "unsupported"

    def test_solana_venues_report_their_compiler_key_not_the_connector_name(self):
        """`orca` and `raydium` are connector names; neither reaches a compiler."""
        index = VenueSupportIndex("solana")
        assert index.classify("orca", product_distinct=True).protocols == ("orca_whirlpools",)
        assert index.classify("raydium-clmm", product_distinct=True).protocols == ("raydium_clmm",)

    def test_an_unowned_base_venue_is_reported_unsupported(self):
        index = VenueSupportIndex("base")
        assert index.complete is True
        assert index.classify("baseswap", product_distinct=True).status == "unsupported"


# Chains where a pool-capable connector has NOT declared its venue ids, and the
# connectors responsible. Every such chain reports `unknown` for unmatched
# venues, so this map is the honest inventory of what the feature cannot answer
# yet. Shipping a new pool-capable connector without `venue_dex_ids` silently
# turns a complete chain back into an incomplete one; that regression surfaces
# here as an unnamed entry rather than as a quietly weakened CLI verdict.
_UNDECLARED_POOL_CONNECTORS: dict[str, tuple[str, ...]] = {
    "mantle": ("uniswap_v3",),
    "monad": ("uniswap_v3",),
    "robinhood": ("uniswap_v3", "uniswap_v4"),
    "solana": ("meteora",),
    "xlayer": ("uniswap_v3",),
    "zerog": ("uniswap_v3",),
}


def test_every_declared_venue_resolves_to_a_registered_compiler():
    """A venue marked executable must be reachable by the key it reports.

    `protocols` is what a builder puts on the intent, and `IntentCompiler`
    dispatches on it. A connector can own a key its compiler does not resolve
    (`orca` owns `orca`, compiles `orca_whirlpools`), so ownership is not
    dispatchability and only this check tells them apart.
    """
    unreachable = [
        (connector.name, chain, dex_id, protocol)
        for connector in CONNECTOR_REGISTRY.all()
        if connector.venue_dex_ids is not None
        for chain, entries in connector.venue_dex_ids.items()
        for dex_id, protocol in entries.items()
        if not CompilerRegistry.has(protocol)
    ]
    assert unreachable == []


def test_venue_declaration_coverage_matches_the_named_gap_list():
    chains = sorted({chain for connector in CONNECTOR_REGISTRY.all() for chain in connector.all_supported_chains})
    actual = {}
    for chain in chains:
        undeclared = VenueSupportIndex(chain).undeclared_protocols
        if undeclared:
            actual[chain] = undeclared
    assert actual == _UNDECLARED_POOL_CONNECTORS
