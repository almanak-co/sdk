"""Regression test for VIB-3743 (BUG-50) — ETH symbol must resolve on Linea.

QA April29 Batch 17 surfaced ``Symbol 'ETH' not found in registry for linea``
when running ``aave_v3_lending_linea``. ETH is the gas token on Linea, but the
registry only listed ETH on ethereum/arbitrum/optimism/base.

The fix adds ``linea`` to the ETH symbol's address map (sentinel
``0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE``). Re-add another ETH-native
chain to ``tokens.json`` and extend this test to lock the contract.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from almanak.framework.data.tokens.resolver import TokenResolver


@pytest.fixture(autouse=True)
def _reset_singleton():
    TokenResolver.reset_instance()
    yield
    TokenResolver.reset_instance()


@pytest.fixture
def temp_cache_file():
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
        temp_path = f.name
    yield temp_path
    Path(temp_path).unlink(missing_ok=True)


class TestEthLineaAlias:
    def test_eth_resolves_on_linea(self, temp_cache_file):
        resolver = TokenResolver(cache_file=temp_cache_file)
        token = resolver.resolve("ETH", "linea", skip_gateway=True)

        assert token.symbol == "ETH"
        assert token.decimals == 18
        assert token.is_native is True

    def test_eth_address_is_native_sentinel_on_linea(self, temp_cache_file):
        resolver = TokenResolver(cache_file=temp_cache_file)
        token = resolver.resolve("ETH", "linea", skip_gateway=True)

        # 0xEeee...EEeE — EVM "native token" sentinel
        assert token.address.lower() == "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"

    def test_eth_resolves_on_canonical_eth_chains(self, temp_cache_file):
        """ETH must resolve on every chain where ETH is the native gas token."""
        resolver = TokenResolver(cache_file=temp_cache_file)
        for chain in ("ethereum", "arbitrum", "optimism", "base", "linea"):
            token = resolver.resolve("ETH", chain, skip_gateway=True)
            assert token.symbol == "ETH", f"failed on {chain}"
            assert token.is_native is True, f"failed on {chain}"

    def test_linea_resolved_token_has_correct_chain_id(self, temp_cache_file):
        """ResolvedToken.chain_id for Linea must be 59144, not 0.

        Adding a token to ``tokens.json`` for a chain that is missing from
        ``models.CHAIN_ID_MAP`` silently produces ``chain_id=0`` on the
        ResolvedToken — invalid and dangerous if any consumer pins on it.
        """
        resolver = TokenResolver(cache_file=temp_cache_file)
        token = resolver.resolve("ETH", "linea", skip_gateway=True)
        assert token.chain_id == 59144
