"""Venue-support marking on `list_token_pools` results (ALM-10098).

A tokenized-RWA listing can name a trading venue whose pool identifier is
unresolved while discovery returns dozens of pools on venues this SDK cannot
target. Without a per-venue support verdict a builder picks the deepest pool
and aims an intent at a DEX no connector owns.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.core.finality import DataFinality
from almanak.framework.agent_tools.executor import ToolExecutor
from almanak.framework.agent_tools.policy import AgentPolicy
from almanak.framework.agent_tools.schemas import ToolResponseStatus
from almanak.framework.data.models import DataClassification, DataEnvelope, DataMeta
from almanak.framework.data.pools.analytics import TokenPool, TokenPools

# The reported listing: NVDAB (tokenized NVDA) on BNB Smart Chain, whose quoted
# Uniswap V4 venue had no resolved pool while PancakeSwap V3 pools were deep.
NVDAB = "0x02fca66c1d1afb4e2a7884261eb00f63598a7436"
USDT = "0x55d398326f99059ff775485246999027b3197955"


@pytest.fixture
def executor() -> ToolExecutor:
    client = MagicMock()
    client.is_connected = True
    return ToolExecutor(
        client,
        policy=AgentPolicy(allowed_chains={"bsc", "base"}, cooldown_seconds=0),
        wallet_address="0x1234567890abcdef1234567890abcdef12345678",
        deployment_id="test-strategy",
    )


def _pool(address: str, dex_id: str, reserve: str) -> TokenPool:
    return TokenPool(
        pool_address=address,
        dex_id=dex_id,
        name="NVDAB / USDT",
        reserve_usd=Decimal(reserve),
        volume_24h_usd=None,
        base_token_address=NVDAB,
        quote_token_address=USDT,
    )


def _envelope(pools, *, chain: str, product_distinct: bool = True):
    return DataEnvelope(
        value=TokenPools(
            chain=chain,
            token_address=NVDAB,
            pools=tuple(pools),
            source="coingecko_onchain" if product_distinct else "dexscreener",
            complete=True,
            product_distinct_dex_id=product_distinct,
        ),
        meta=DataMeta(
            source="coingecko_onchain",
            observed_at=datetime.now(UTC),
            finality=DataFinality.OFF_CHAIN,
            staleness_ms=0,
            latency_ms=0,
            confidence=0.85,
            cache_hit=False,
        ),
        classification=DataClassification.INFORMATIONAL,
    )


async def _run(executor: ToolExecutor, args: dict, envelope):
    reader = MagicMock()
    reader.list_token_pools.return_value = envelope
    with patch("almanak.framework.data.pools.analytics.PoolAnalyticsReader", return_value=reader):
        return await executor._execute_list_token_pools(args)


@pytest.mark.asyncio
async def test_discovered_venues_are_marked_executable_or_not(executor: ToolExecutor):
    envelope = _envelope(
        [
            _pool("0x8fb4243b553ac29ba088acf00b9b7da24bd6690c", "pancakeswap-v3-bsc", "2130000"),
            _pool("0x" + "aa" * 20, "thena-fusion", "400000"),
        ],
        chain="bsc",
    )
    response = await _run(executor, {"token": NVDAB, "chain": "bsc"}, envelope)

    assert response.status == ToolResponseStatus.SUCCESS
    supported, other = response.data["pools"]
    assert supported["execution_support"] == "supported"
    assert supported["protocols"] == ["pancakeswap_v3"]
    # The exact list, in POOL_INTENTS order: a truthiness check would also pass
    # for a partial list, which is the shape that would quietly drop a primitive
    # the builder can actually use.
    assert supported["supported_intents"] == ["SWAP", "LP_OPEN", "LP_CLOSE", "LP_COLLECT_FEES"]
    # Every pool-capable BSC connector declares its venue ids, so a venue
    # nothing matched has actually been ruled out rather than merely unmapped.
    assert response.data["venue_support_complete"] is True
    assert other["execution_support"] == "unsupported"
    assert other["protocols"] == []


@pytest.mark.asyncio
async def test_an_unowned_venue_is_unsupported_once_the_chain_is_complete(executor: ToolExecutor):
    envelope = _envelope([_pool("0x" + "bb" * 20, "someswap-base", "900000")], chain="base")
    response = await _run(executor, {"token": NVDAB, "chain": "base"}, envelope)

    assert response.data["venue_support_complete"] is True
    assert response.data["pools"][0]["execution_support"] == "unsupported"


@pytest.mark.asyncio
async def test_a_non_product_distinct_provider_marks_every_venue_unknown(executor: ToolExecutor):
    envelope = _envelope(
        [_pool("0x" + "cc" * 20, "pancakeswap-v3-bsc", "2130000")], chain="bsc", product_distinct=False
    )
    response = await _run(executor, {"token": NVDAB, "chain": "bsc"}, envelope)

    assert response.data["product_distinct_dex_id"] is False
    assert response.data["pools"][0]["execution_support"] == "unknown"
    assert response.data["pools"][0]["protocols"] == []


@pytest.mark.asyncio
async def test_an_rwa_listing_whose_quoted_venue_has_no_pool_still_names_the_usable_ones(
    executor: ToolExecutor,
):
    """The reported shape: the listed Uniswap V4 (BSC) venue returns NO pool row.

    The response must not invent one, and must still tell the builder which of
    the venues that DID resolve can be executed on.
    """
    envelope = _envelope(
        [_pool("0x8fb4243b553ac29ba088acf00b9b7da24bd6690c", "pancakeswap-v3-bsc", "2130000")],
        chain="bsc",
    )
    response = await _run(executor, {"token": NVDAB, "chain": "bsc"}, envelope)

    pools = response.data["pools"]
    assert [p["dex_id"] for p in pools] == ["pancakeswap-v3-bsc"]
    # The token address is never echoed as a venue.
    assert NVDAB not in {p["pool_address"].lower() for p in pools}
    assert [p["execution_support"] for p in pools] == ["supported"]
    assert "uniswap_v4" in response.data["supported_pool_protocols"]
