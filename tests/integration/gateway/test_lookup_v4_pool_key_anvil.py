"""Integration tests for ``MarketService.LookupV4PoolKey`` on a real Anvil fork.

VIB-4472 / T03. Forks Base mainnet, scans the canonical PoolManager
(``0x498581fF718922c3f8e6A244956aF099B2652b2b``) for ``Initialize``
events, then asserts the gateway can resolve the resulting ``pool_id``
back to its canonical ``PoolKey`` with the ``currency0 < currency1``
invariant preserved.

The scan window is bounded to the most recent few thousand blocks of
the fork so the test stays cheap. Any Initialize event in that window
is enough to prove the round trip; the test does not pin a specific
pool because Base PoolManager initialisations are frequent and stable
hashes vary across blocks.

To run:
    uv run pytest tests/integration/gateway/test_lookup_v4_pool_key_anvil.py -v -s

Requirements:
    - ALCHEMY_API_KEY environment variable set
"""

from __future__ import annotations

from unittest.mock import MagicMock

import grpc
import pytest

from almanak.core.contracts import UNISWAP_V4
from almanak.framework.connectors.uniswap_v4.gateway_pool_key_client import (
    _coerce_pool_id_bytes,
)
from almanak.framework.connectors.uniswap_v4.sdk import PoolKey
from almanak.connectors.uniswap_v4.gateway.pool_key_cache import (
    INITIALIZE_EVENT_TOPIC,
    V4PoolKeyCache,
)
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.market_service import MarketServiceServicer
from tests.conftest_gateway import AnvilFixture

pytest_plugins = ["tests.conftest_gateway"]


BASE_POOL_MANAGER = UNISWAP_V4["base"]["pool_manager"]


def _make_servicer_with_anvil(anvil: AnvilFixture, *, cache: V4PoolKeyCache) -> MarketServiceServicer:
    """Build a MarketServiceServicer wired to the Anvil-backed cache."""
    settings = MagicMock()
    settings.network = "mainnet"
    settings.chains = ["base"]
    settings.coingecko_api_key = ""
    servicer = MarketServiceServicer(settings)
    servicer._pool_key_cache = cache
    return servicer


class _MockContext:
    def __init__(self) -> None:
        self.code: grpc.StatusCode | None = None
        self.details: str | None = None

    def set_code(self, code: grpc.StatusCode) -> None:
        self.code = code

    def set_details(self, details: str) -> None:
        self.details = details


@pytest.mark.asyncio
async def test_lookup_v4_pool_key_against_base_anvil_fork(anvil_base: AnvilFixture):
    """End-to-end: scan real Initialize events on Base, resolve via gateway."""
    from web3 import AsyncHTTPProvider, AsyncWeb3

    rpc_url = anvil_base.get_rpc_url()
    w3 = AsyncWeb3(AsyncHTTPProvider(rpc_url))

    head = await w3.eth.block_number
    # Scan a generous window to reliably catch at least one Initialize.
    # Anvil-fork relays historical eth_getLogs to the upstream node; Base
    # is busy on V4 (~95 inits per 500 blocks at head) so the smaller
    # window stays well under most providers' getLogs ceiling.
    from_block = max(0, head - 2_000)

    cache = V4PoolKeyCache(
        # Force the cache to use this fork's RPC even though gateway-wide
        # settings would otherwise look the URL up via Alchemy.
        rpc_url_resolver=lambda chain, network=None: rpc_url,
        network="anvil",
    )
    added = await cache.populate_from_logs(
        chain="base",
        w3=w3,
        pool_manager=BASE_POOL_MANAGER,
        from_block=from_block,
        to_block=head,
    )

    if added == 0:
        pytest.skip(
            "No PoolManager.Initialize events found in the scanned window "
            f"({from_block}..{head}). Widen the window or pin a known block range."
        )

    # Pick the first pool the cache learned about and verify the round trip.
    chain_index = cache._index["base"]
    pool_id_hex, expected_key = next(iter(chain_index.items()))
    pool_id_bytes = _coerce_pool_id_bytes(pool_id_hex)

    servicer = _make_servicer_with_anvil(anvil_base, cache=cache)
    ctx = _MockContext()
    request = gateway_pb2.LookupV4PoolKeyRequest(pool_id=pool_id_bytes, chain="base")
    response = await servicer.LookupV4PoolKey(request, ctx)

    assert ctx.code is None, f"expected success, got {ctx.code}: {ctx.details}"
    assert response.chain == "base"
    pk = response.pool_key
    assert pk.currency0 == expected_key.currency0
    assert pk.currency1 == expected_key.currency1
    assert pk.fee == expected_key.fee
    assert pk.tick_spacing == expected_key.tick_spacing
    assert pk.hooks == expected_key.hooks
    # currency0 < currency1 invariant -- enforced at decode time and again
    # here as defence-in-depth at the wire boundary.
    assert int(pk.currency0, 16) < int(pk.currency1, 16)

    # Constructing a PoolKey from the returned shape must succeed and
    # re-normalise to the same sorted pair (framework PoolKey sorts on
    # __post_init__).
    framework_pk = PoolKey(
        currency0=pk.currency0,
        currency1=pk.currency1,
        fee=int(pk.fee),
        tick_spacing=int(pk.tick_spacing),
        hooks=pk.hooks,
    )
    assert framework_pk.currency0 == pk.currency0
    assert framework_pk.currency1 == pk.currency1


@pytest.mark.asyncio
async def test_lookup_v4_pool_key_not_found_against_base_anvil_fork(anvil_base: AnvilFixture):
    """Unknown pool_id returns gRPC NOT_FOUND with an empty body.

    VIB-4426 P1 #2 — set ``max_historical_blocks=0`` to disable backward
    expansion. Without it, the cache attempts an additional deep-history
    ``eth_getLogs`` after the forward tail misses; Anvil forks do NOT
    reliably relay deep-history log queries and the call surfaces as
    ``UNAVAILABLE`` (correctly, per the new typed-error contract) rather
    than as ``NOT_FOUND``. The canonical "scanned forward, target not
    present" path is what this test exercises.
    """
    rpc_url = anvil_base.get_rpc_url()
    cache = V4PoolKeyCache(
        rpc_url_resolver=lambda chain, network=None: rpc_url,
        network="anvil",
        # Cap forward-tail to a small window the Anvil fork can serve.
        backfill_blocks=2_000,
        # Disable historical expansion — Anvil-fork eth_getLogs on deep
        # history is unreliable and would map to UNAVAILABLE.
        max_historical_blocks=0,
    )

    servicer = _make_servicer_with_anvil(anvil_base, cache=cache)
    ctx = _MockContext()
    # Definitionally never-deployed pool id -- random 32 bytes.
    request = gateway_pb2.LookupV4PoolKeyRequest(
        pool_id=b"\xde\xad" * 16,
        chain="base",
    )
    response = await servicer.LookupV4PoolKey(request, ctx)

    assert ctx.code == grpc.StatusCode.NOT_FOUND, (
        f"expected NOT_FOUND for never-deployed pool_id (status={ctx.code}, details={ctx.details})"
    )
    assert response.pool_key.currency0 == ""
    assert response.pool_key.currency1 == ""


@pytest.mark.asyncio
async def test_lookup_v4_pool_key_event_topic_matches_keccak() -> None:
    """Documented sanity check: the topic constant must match keccak of the signature.

    Cheap regression guard if anyone fiddles the constant by hand.
    """
    from web3 import Web3

    sig = "Initialize(bytes32,address,address,uint24,int24,address,uint160,int24)"
    computed = "0x" + Web3.keccak(text=sig).hex().removeprefix("0x")
    assert computed == INITIALIZE_EVENT_TOPIC
