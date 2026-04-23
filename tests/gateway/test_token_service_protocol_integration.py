"""Tests for the protocol-integration plumbing in ``token_service.py``.

``TokenService`` composes seven protocol-specific lookup services (Pendle,
Aave, Compound, Morpho, Beefy, Yearn, Fluid) into its EVM resolution
cascade. Two cross-cutting helpers hold the cascade together:

* ``_resolve_chain_enum(name)`` maps the lowercased chain strings that
  each protocol lookup indexes by onto the uppercase ``Chain`` enum so
  ``_build_resolved_from_*`` helpers emit the correct ``chain`` value on
  the returned ``ResolvedToken``. Naive ``Chain(name)`` with a lowercase
  string raises ``ValueError`` (enum values are uppercase), which is why
  this helper exists; the tests below cover every supported chain plus
  fallback behavior for unknown inputs.
* ``TokenServiceServicer._SOURCE_RANK`` drives the source-provenance
  policy in ``_cache_discovered_token``. Every ``source=<protocol>``
  string emitted by a ``_build_resolved_from_*`` helper must carry a
  rank above ``coingecko_dynamic`` / ``dexscreener_dynamic`` so a later
  generic dynamic hit can't overwrite a protocol-authoritative entry.

Compound predicate coverage lives here too because it sits alongside the
other protocol prefix checks at module scope.
"""

import pytest

from almanak.core.enums import Chain
from almanak.gateway.services.token_service import (
    TokenServiceServicer,
    _looks_like_compound_symbol,
    _resolve_chain_enum,
)


class TestResolveChainEnum:
    """Lowercase chain strings must resolve to the correct ``Chain`` enum.

    The protocol lookup services index tokens by a lowercased chain name
    (``"ethereum"``, ``"arbitrum"``, ...) and ``_build_resolved_from_*``
    passes that name straight into ``_resolve_chain_enum`` to populate
    ``ResolvedToken.chain``. A case-sensitive lookup here would stamp a
    wrong-chain enum on every non-ethereum resolution.
    """

    @pytest.mark.parametrize(
        "name, expected",
        [
            ("ethereum", Chain.ETHEREUM),
            ("ETHEREUM", Chain.ETHEREUM),
            ("Ethereum", Chain.ETHEREUM),
            ("arbitrum", Chain.ARBITRUM),
            ("ARBITRUM", Chain.ARBITRUM),
            ("base", Chain.BASE),
            ("optimism", Chain.OPTIMISM),
            ("polygon", Chain.POLYGON),
            ("bsc", Chain.BSC),
            ("avalanche", Chain.AVALANCHE),
            ("linea", Chain.LINEA),
            ("solana", Chain.SOLANA),
        ],
    )
    def test_recognises_known_chains_case_insensitive(self, name, expected):
        assert _resolve_chain_enum(name) is expected

    @pytest.mark.parametrize(
        "name",
        [
            "",
            "not_a_chain",
            "gnosis",  # real chain but not in Chain enum today
            "katana",
        ],
    )
    def test_falls_back_to_ethereum_for_unknown(self, name):
        assert _resolve_chain_enum(name) is Chain.ETHEREUM


class TestSourceRankCoversAllProtocols:
    """Every ``source`` string a ``_build_resolved_from_*`` helper emits
    must have a ``_SOURCE_RANK`` entry above the generic dynamic tiers.

    ``_cache_discovered_token`` consults ``_SOURCE_RANK`` when deciding
    whether an incoming write may overwrite an existing entry. A source
    with no rank defaults to 0, which would let a later
    ``dexscreener_dynamic`` (rank 40) or ``coingecko_dynamic`` (rank 60)
    write replace a protocol-authoritative entry — the very outcome the
    provenance guard is supposed to prevent.
    """

    PROTOCOL_SOURCES = {
        "pendle_pt",
        "pendle_yt",
        "pendle_sy",
        "pendle_lp",
        "aave_atoken",
        "aave_vtoken",
        "compound_ctoken",
        "morpho_vault",
        "beefy_vault",
        "yearn_vault",
        "fluid_ftoken",
    }

    def test_every_protocol_source_has_a_rank(self):
        missing = self.PROTOCOL_SOURCES - set(TokenServiceServicer._SOURCE_RANK)
        assert not missing, (
            f"_SOURCE_RANK is missing entries for: {sorted(missing)}. "
            f"Without explicit ranks these sources default to 0 and a "
            f"later dexscreener_dynamic (rank 40) write silently overwrites "
            f"the trusted protocol entry."
        )

    def test_protocol_ranks_beat_dexscreener(self):
        rank = TokenServiceServicer._SOURCE_RANK
        dexscreener_rank = rank["dexscreener_dynamic"]
        for src in self.PROTOCOL_SOURCES:
            assert rank[src] > dexscreener_rank, (
                f"{src} rank ({rank[src]}) should be above "
                f"dexscreener_dynamic ({dexscreener_rank}) so DexScreener "
                f"can't overwrite a prior protocol-authoritative resolution."
            )

    def test_protocol_ranks_beat_coingecko(self):
        rank = TokenServiceServicer._SOURCE_RANK
        coingecko_rank = rank["coingecko_dynamic"]
        for src in self.PROTOCOL_SOURCES:
            assert rank[src] > coingecko_rank, (
                f"{src} rank ({rank[src]}) should be above "
                f"coingecko_dynamic ({coingecko_rank}) — the protocol's own "
                f"API is more authoritative than CoinGecko for its tokens."
            )

    def test_static_still_wins(self):
        """Hand-curated static entries must stay at the top of the rank."""
        rank = TokenServiceServicer._SOURCE_RANK
        for src in self.PROTOCOL_SOURCES:
            assert rank["static"] > rank[src], (
                f"static (rank {rank['static']}) should outrank {src} "
                f"(rank {rank[src]}) — registry-curated entries must win "
                f"over protocol-fetched ones when both write the same symbol."
            )


class TestCompoundPredicateCaseInsensitive:
    """The Compound prefix predicate matches case-insensitively.

    ``CompoundMarketLookup.lookup_by_symbol`` indexes by uppercased
    symbol, so the predicate should accept any case variant — otherwise
    a user typing ``CUSDCV3`` skips the Compound tier and falls through
    to CoinGecko / DexScreener even though the index could answer.
    """

    @pytest.mark.parametrize(
        "symbol",
        [
            "cUSDCv3",
            "CUSDCV3",
            "CUSDCv3",
            "cusdcv3",
            "CuSdCv3",
        ],
    )
    def test_accepts_case_variants(self, symbol):
        assert _looks_like_compound_symbol(symbol) is True

    @pytest.mark.parametrize(
        "symbol",
        [
            "cbBTC",  # c-prefix, no v3 suffix
            "crvUSD",  # c-prefix, no v3 suffix
            "cUSDC",  # v2 cToken shape (no v3)
            "COMP",  # governance token
            "USDC",  # no c-prefix
            "v3",  # too short
            "",
        ],
    )
    def test_rejects_non_compound(self, symbol):
        assert _looks_like_compound_symbol(symbol) is False
