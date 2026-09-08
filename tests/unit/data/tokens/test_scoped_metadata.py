"""Request metadata cannot poison token caches or cross async tasks."""

import asyncio
from dataclasses import replace
from unittest.mock import patch

import pytest

from almanak.framework.data.tokens import ResolvedToken, create_token_resolver
from almanak.framework.data.tokens.exceptions import TokenNotFoundError

ADDRESS = "0x1234567890123456789012345678901234567890"


@pytest.fixture
def resolver(tmp_path):
    return create_token_resolver(cache_file=str(tmp_path / "tokens.json"))


@pytest.fixture
def discovered():
    return ResolvedToken(
        symbol="WETH",
        address=ADDRESS,
        decimals=6,
        chain="robinhood",
        chain_id=4663,
        source="on_chain",
        is_verified=False,
    )


def test_metadata_does_not_register_a_symbol_or_persist(resolver, discovered):
    weth = resolver.resolve("WETH", "robinhood", skip_gateway=True)
    with pytest.raises(TokenNotFoundError):
        resolver.resolve(ADDRESS, "robinhood", skip_gateway=True)
    with patch.object(resolver._cache, "put") as cache_write:
        with resolver.scoped_metadata([discovered]):
            assert resolver.resolve(ADDRESS, "robinhood") is discovered
            assert resolver.get_decimals("robinhood", ADDRESS) == 6
            assert resolver.resolve("WETH", "robinhood").address == weth.address
        cache_write.assert_not_called()
    with pytest.raises(TokenNotFoundError):
        resolver.resolve(ADDRESS, "robinhood", skip_gateway=True)


def test_scope_never_overrides_registry_metadata(resolver, discovered):
    weth = resolver.resolve("WETH", "robinhood", skip_gateway=True)
    with resolver.scoped_metadata([replace(discovered, address=weth.address, decimals=6)]):
        assert resolver.resolve(weth.address, "robinhood").decimals == 18


def test_scope_cleans_up_after_exception_and_disables_gateway(resolver, discovered):
    from almanak.framework.data.tokens.resolver import TokenResolver

    resolver._gateway_channel = object()
    with pytest.raises(RuntimeError, match="compile failed"):
        with resolver.scoped_metadata([discovered]):
            with patch.object(resolver, "_resolve_via_gateway", side_effect=AssertionError("self-RPC")):
                with pytest.raises(TokenNotFoundError):
                    resolver.resolve("0x2222222222222222222222222222222222222222", "robinhood")
            raise RuntimeError("compile failed")
    assert TokenResolver._request_metadata.get() is None


@pytest.mark.asyncio
async def test_concurrent_requests_have_separate_metadata(resolver, discovered):
    entered = asyncio.Event()
    release = asyncio.Event()

    async def first():
        with resolver.scoped_metadata([discovered]):
            entered.set()
            await release.wait()
            assert resolver.resolve(ADDRESS, "robinhood").decimals == 6

    async def second():
        await entered.wait()
        with resolver.scoped_metadata([replace(discovered, decimals=18)]):
            assert resolver.resolve(ADDRESS, "robinhood").decimals == 18
        release.set()

    await asyncio.gather(first(), second())
