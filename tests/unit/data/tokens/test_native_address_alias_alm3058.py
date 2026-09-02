"""Chain-scoped native-coin address aliases fold onto NATIVE_SENTINEL (ALM-3058).

Polygon exposes its native coin behind the ``0x...1010`` MRC-20 precompile.
The platform's funding lane treats that address as native, so a strategy
funded through it must land under the same identity every symbol-form read
(``balance("POL")``, the intent compiler, snapshots) resolves to: the ERC-7528
native sentinel. The fold happens in the resolver before any cache,
negative-cache, or registry key is derived, and only on the chain that owns
the alias.
"""

import tempfile
from pathlib import Path

import pytest

from almanak.framework.data.tokens.defaults import NATIVE_SENTINEL
from almanak.framework.data.tokens.exceptions import TokenNotFoundError
from almanak.framework.data.tokens.resolver import (
    TokenResolver,
    fold_native_address_alias,
    is_native_address_alias,
)

POLYGON_NATIVE_PRECOMPILE = "0x0000000000000000000000000000000000001010"


@pytest.fixture(autouse=True)
def reset_singleton():
    TokenResolver.reset_instance()
    yield
    TokenResolver.reset_instance()


@pytest.fixture
def resolver():
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
        temp_path = f.name
    try:
        yield TokenResolver(cache_file=temp_path)
    finally:
        Path(temp_path).unlink(missing_ok=True)


class TestFoldNativeAddressAlias:
    def test_polygon_precompile_folds_to_sentinel(self) -> None:
        assert fold_native_address_alias(POLYGON_NATIVE_PRECOMPILE, "polygon") == NATIVE_SENTINEL
        assert is_native_address_alias(POLYGON_NATIVE_PRECOMPILE, "polygon")

    def test_fold_is_case_and_chain_alias_insensitive(self) -> None:
        # All-numeric addresses are checksum-neutral, so casing is only the 0X prefix.
        assert fold_native_address_alias(POLYGON_NATIVE_PRECOMPILE.upper(), "polygon") == NATIVE_SENTINEL
        assert fold_native_address_alias(POLYGON_NATIVE_PRECOMPILE, "eip155:137") == NATIVE_SENTINEL

    @pytest.mark.parametrize("chain", ["arbitrum", "ethereum", "base", "solana", "nosuchchain"])
    def test_same_address_is_not_folded_off_polygon(self, chain: str) -> None:
        assert fold_native_address_alias(POLYGON_NATIVE_PRECOMPILE, chain) == POLYGON_NATIVE_PRECOMPILE
        assert not is_native_address_alias(POLYGON_NATIVE_PRECOMPILE, chain)

    def test_sentinel_and_ordinary_addresses_pass_through(self) -> None:
        usdc = "0x3c499c542cef5e3811e1192ce70d8cc03d5c3359"
        assert fold_native_address_alias(NATIVE_SENTINEL, "polygon") == NATIVE_SENTINEL
        assert fold_native_address_alias(usdc, "polygon") == usdc
        assert not is_native_address_alias(usdc, "polygon")


class TestResolverFoldsPolygonNativeAlias:
    def test_alias_resolves_to_pol_native_sentinel(self, resolver: TokenResolver) -> None:
        token = resolver.resolve(POLYGON_NATIVE_PRECOMPILE, "polygon", skip_gateway=True)

        assert token.symbol == "POL"
        assert token.is_native is True
        assert token.address.lower() == NATIVE_SENTINEL.lower()
        assert token.decimals == 18

    def test_alias_and_symbol_reads_share_one_identity(self, resolver: TokenResolver) -> None:
        by_alias = resolver.resolve(POLYGON_NATIVE_PRECOMPILE, "polygon", skip_gateway=True)
        by_symbol = resolver.resolve("POL", "polygon", skip_gateway=True)

        assert by_alias.address.lower() == by_symbol.address.lower()
        assert by_alias.symbol == by_symbol.symbol

    def test_alias_resolves_through_caip19(self, resolver: TokenResolver) -> None:
        token = resolver.resolve_caip19(f"eip155:137/erc20:{POLYGON_NATIVE_PRECOMPILE}", skip_gateway=True)

        assert token.symbol == "POL"
        assert token.address.lower() == NATIVE_SENTINEL.lower()

    def test_alias_does_not_pollute_negative_cache(self, resolver: TokenResolver) -> None:
        resolver.resolve(POLYGON_NATIVE_PRECOMPILE, "polygon", skip_gateway=True)

        assert ("polygon", POLYGON_NATIVE_PRECOMPILE) not in resolver._negative_cache
        assert resolver._negative_cache == {}

    def test_alias_is_not_folded_on_other_chains(self, resolver: TokenResolver) -> None:
        with pytest.raises(TokenNotFoundError):
            resolver.resolve(POLYGON_NATIVE_PRECOMPILE, "arbitrum", skip_gateway=True, log_errors=False)

        # A static-only miss is never negative-cached, and the polygon fold
        # must not have leaked a positive answer for the arbitrum key.
        assert resolver._negative_cache == {}
        assert resolver._cache.get("arbitrum", address=POLYGON_NATIVE_PRECOMPILE) is None
