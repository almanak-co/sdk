"""Contract metadata must authorize the requested identity before cache access."""

from dataclasses import replace
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.data.interfaces import DataSourceUnavailable
from almanak.framework.data.tokens import create_token_resolver
from almanak.framework.data.tokens.exceptions import TokenNotFoundError
from almanak.integrations.chainlink.gateway.live import ChainlinkPriceSource

FOREIGN = "0x1234567890123456789012345678901234567890"


@pytest.fixture
def source(tmp_path):
    source = ChainlinkPriceSource(chain="arbitrum")
    source._token_resolver = create_token_resolver(cache_file=tmp_path / "tokens.json")
    return source


@pytest.fixture
def verified(source):
    return source._token_resolver.resolve("WETH", "arbitrum", skip_gateway=True)


@pytest.mark.parametrize("supplied", [True, False])
@pytest.mark.parametrize("uppercase", [True, False])
def test_matching_verified_contract_is_accepted(source, verified, supplied, uppercase):
    token = verified.address.upper() if uppercase else verified.address
    assert source._verified_contract_for_price(token, verified if supplied else None) == verified


@pytest.mark.parametrize(
    "failure,supplied",
    [(failure, supplied) for failure in ("address", "chain", "unverified") for supplied in (True, False)]
    + [("missing", False)],
)
@pytest.mark.asyncio
async def test_invalid_metadata_is_rejected_before_cache_or_feed(source, verified, supplied, failure):
    metadata = {
        "address": replace(verified, address=FOREIGN),
        "chain": replace(verified, chain="ethereum", chain_id=1),
        "unverified": replace(verified, is_verified=False),
        "missing": None,
    }[failure]
    resolver = MagicMock()
    resolver.resolve.return_value = metadata
    if failure == "missing":
        resolver.resolve.side_effect = TokenNotFoundError(verified.address, "arbitrum", "not found")
    source._token_resolver = resolver
    source._cache = MagicMock()
    source._fetch_chainlink = AsyncMock()
    source._validate_chain_id = AsyncMock()
    with pytest.raises(DataSourceUnavailable):
        await source.get_price(verified.address, resolved_token=metadata if supplied else None)
    source._cache.get.assert_not_called()
    source._fetch_chainlink.assert_not_awaited()
    source._validate_chain_id.assert_not_awaited()
    if supplied:
        resolver.resolve.assert_not_called()
    else:
        resolver.resolve.assert_called_once_with(verified.address, "arbitrum", skip_gateway=True, log_errors=False)
