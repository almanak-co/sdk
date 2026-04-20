"""Tests for MATIC <-> POL alias behaviour on Polygon (VIB-3137).

POL is the Sep-2024 rename of MATIC on Polygon (1:1 migration).
Both symbols must resolve to the same underlying on-chain native asset so that
``ax balance MATIC`` and ``ax balance POL`` produce the same USD valuation.

These tests pin the contract:

* ``resolve("MATIC", "polygon")`` and ``resolve("POL", "polygon")`` both
  return a ResolvedToken that is native, has 18 decimals, and shares the
  same CoinGecko ID.
* The CoinGecko ID is ``polygon-ecosystem-token`` (the POL id, not the
  deprecated ``matic-network``) so the price aggregator sees the same
  oracle feed regardless of which symbol the user types.
* Other chains' natives (ETH on Ethereum, AVAX on Avalanche, BNB on BSC)
  are unaffected.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from almanak.framework.data.tokens.defaults import get_coingecko_id
from almanak.framework.data.tokens.resolver import TokenResolver


@pytest.fixture(autouse=True)
def reset_singleton():
    """Reset singleton before and after each test."""
    TokenResolver.reset_instance()
    yield
    TokenResolver.reset_instance()


@pytest.fixture
def temp_cache_file():
    """Create a temporary cache file."""
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
        temp_path = f.name
    yield temp_path
    Path(temp_path).unlink(missing_ok=True)


class TestMaticPolAlias:
    """MATIC and POL are the same native asset on Polygon (VIB-3137)."""

    def test_matic_resolves_as_native_on_polygon(self, temp_cache_file):
        resolver = TokenResolver(cache_file=temp_cache_file)
        token = resolver.resolve("MATIC", "polygon")

        assert token.symbol == "MATIC"
        assert token.is_native is True
        assert token.decimals == 18

    def test_pol_resolves_as_native_on_polygon(self, temp_cache_file):
        resolver = TokenResolver(cache_file=temp_cache_file)
        token = resolver.resolve("POL", "polygon")

        assert token.symbol == "POL"
        assert token.is_native is True
        assert token.decimals == 18

    def test_matic_and_pol_share_coingecko_id_on_polygon(self, temp_cache_file):
        """Both symbols must query the same price oracle on Polygon.

        Root cause of VIB-3137: MATIC's coingecko_id was ``matic-network``
        (deprecated) while POL's was ``polygon-ecosystem-token``. CoinGecko
        returns diverging prices for those two IDs, so ``ax balance MATIC``
        and ``ax balance POL`` on the same wallet produced different USD
        valuations even though they represent the same underlying asset.
        """
        resolver = TokenResolver(cache_file=temp_cache_file)
        matic = resolver.resolve("MATIC", "polygon")
        pol = resolver.resolve("POL", "polygon")

        assert matic.coingecko_id == pol.coingecko_id
        assert matic.coingecko_id == "polygon-ecosystem-token"

    def test_matic_and_pol_share_decimals(self, temp_cache_file):
        resolver = TokenResolver(cache_file=temp_cache_file)
        matic = resolver.resolve("MATIC", "polygon")
        pol = resolver.resolve("POL", "polygon")

        assert matic.decimals == pol.decimals == 18

    def test_matic_and_pol_share_native_address_on_polygon(self, temp_cache_file):
        """Both symbols point at the EVM native-sentinel address on Polygon.

        The underlying gas asset is the same — the sentinel ``0xEeee...EEeE``
        is the EVM convention for "native token" and both MATIC and POL map
        to it on Polygon.
        """
        resolver = TokenResolver(cache_file=temp_cache_file)
        matic = resolver.resolve("MATIC", "polygon")
        pol = resolver.resolve("POL", "polygon")

        assert matic.address.lower() == pol.address.lower()
        assert matic.address.lower() == "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"

    def test_get_coingecko_id_returns_pol_id_for_matic(self):
        """The symbol -> CoinGecko ID helper must return the POL id.

        ``get_coingecko_id`` is used by CoinGeckoPriceSource to map a symbol
        to a CoinGecko ID when no pre-resolved token is supplied.
        """
        assert get_coingecko_id("MATIC") == "polygon-ecosystem-token"
        assert get_coingecko_id("POL") == "polygon-ecosystem-token"


class TestOtherNativesUnaffected:
    """Regression guard: the alias work doesn't perturb other chains' natives."""

    @pytest.mark.parametrize(
        ("symbol", "chain", "expected_coingecko_id"),
        [
            ("ETH", "ethereum", "ethereum"),
            ("AVAX", "avalanche", "avalanche-2"),
            ("BNB", "bsc", "binancecoin"),
        ],
    )
    def test_native_token_still_resolves(self, temp_cache_file, symbol, chain, expected_coingecko_id):
        resolver = TokenResolver(cache_file=temp_cache_file)
        token = resolver.resolve(symbol, chain)
        assert token.symbol == symbol
        assert token.is_native is True
        assert token.coingecko_id == expected_coingecko_id
