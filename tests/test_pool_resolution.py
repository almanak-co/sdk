"""A declared price pool resolves through its connector, and a historical gateway retains it.

The connector's own resolution is replaced by a recorder, so these tests check what the
price source asks of it and what it keeps: the selection, the anchor and the tokens.
"""

from contextlib import nullcontext
from types import SimpleNamespace

import pytest

from almanak.backtest.gateway import AcquisitionGateway, HistoricalRequest
from almanak.backtest.history import Capture
from almanak.backtest.pool_reads import PoolGateway, PoolRequest, pool_scope, resolve_pool
from almanak.backtest.pool_venues import V3_SWAP
from almanak.backtest.pricing import PoolPrice, PriceBinding
from almanak.connectors.uniswap_v3 import resolution as v3_resolution
from almanak.connectors.uniswap_v4 import pool as v4_pool
from almanak.connectors.uniswap_v4 import resolution as v4_resolution
from almanak.gateway.ethereum import DataError
from almanak.gateway.evm import native_asset
from almanak.models import ZERO_ADDRESS, Asset
from tests.pool_price_chain import CHAIN, HOUR, POOL, USDC, WETH, FakeChain

POOL_ID = "0x" + "ab" * 32
USDC_ADDRESS = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"


def binding(connector="uniswap_v3", pool=POOL):
    return PriceBinding(
        purpose="strategy",
        chain=CHAIN,
        asset=WETH,
        window=4,
        source=PoolPrice(
            kind="pool_state",
            connector=connector,
            pool=pool,
            timeframe="1h",
            max_age_seconds=2 * HOUR,
            availability_lag_seconds=60,
        ),
    )


def resolved(asset0, asset1, pool=None):
    return SimpleNamespace(
        asset0=Asset(chain=CHAIN, identifier=asset0, decimals=18),
        asset1=Asset(chain=CHAIN, identifier=asset1, decimals=6),
        pool=pool,
    )


@pytest.fixture
def selections(monkeypatch):
    seen = []

    def resolve(reader, selection, block):
        seen.append((selection, block))
        reader.verify_chain()
        return resolved(WETH, USDC)

    monkeypatch.setattr(v3_resolution, "resolve_market", resolve)
    return seen


def test_a_v3_pool_resolves_through_its_connector_at_a_canonical_anchor(selections):
    chain = FakeChain()
    block = chain.header(10)
    pool = resolve_pool(chain, pool_scope(binding()), block)
    ((selection, anchor),) = selections
    assert selection.pool == POOL and anchor == block
    assert tuple(asset.identifier for asset in pool.assets) == (WETH, USDC)
    assert pool.pricing == "sqrt_price" and pool.swap_topics == (V3_SWAP,)
    assert pool.state == pool.emitter == POOL and pool.block == block
    stale = block.model_copy(update={"hash": "0x" + "ee" * 32})
    with pytest.raises(DataError, match="no longer canonical"):
        resolve_pool(chain, pool_scope(binding()), stale)


def test_a_v4_pool_id_resolves_through_the_reviewed_position_manager(monkeypatch):
    chain = FakeChain()
    block = chain.header(10)
    keys = [(ZERO_ADDRESS, USDC_ADDRESS, 500, 10, ZERO_ADDRESS)]
    monkeypatch.setattr(chain, "call", lambda *args, **kwargs: keys[0])
    selections = []

    def resolve(reader, selection, anchor):
        selections.append(selection)
        return resolved(selection.currency0, selection.currency1, pool="key")

    monkeypatch.setattr(v4_resolution, "resolve_market", resolve)
    ids = {"key": POOL_ID}
    monkeypatch.setattr(v4_pool, "pool_id", lambda key: ids[key])
    scope = pool_scope(binding("uniswap_v4", POOL_ID))
    pool = resolve_pool(chain, scope, block)
    # The native currency is the chain's native asset; the pool's state is the StateView's.
    assert selections[0].currency0 == native_asset(CHAIN.chain_id) and selections[0].fee == 500
    assert pool.assets[1].identifier == USDC and pool.state == scope.roles.state_view
    assert pool.emitter == scope.roles.pool_manager and pool.swap_topics[1] == POOL_ID
    ids["key"] = "0x" + "cd" * 32
    with pytest.raises(DataError, match="another id"):
        resolve_pool(chain, scope, block)
    keys[0] = (ZERO_ADDRESS, ZERO_ADDRESS, 0, 0, ZERO_ADDRESS)
    with pytest.raises(DataError, match="unknown to the reviewed PositionManager"):
        resolve_pool(chain, scope, block)


def gateway(tmp_path, readers):
    return SimpleNamespace(
        capture=Capture(tmp_path / "capture.sqlite"),
        readers=readers,
        fetch=lambda reader, request: reader._rpc(request.method, list(request.params)),
    )


def test_a_gateway_resolves_a_declared_pool_once_and_replays_its_reads(tmp_path, selections):
    chain = FakeChain()
    online = gateway(tmp_path, {CHAIN: chain})
    pools = PoolGateway(online, (pool_scope(binding()),))
    request = PoolRequest(scope=pool_scope(binding()), block=chain.header(10))
    first = pools.pool(request)
    assert pools.pool(request) == first and len(selections) == 1
    online.capture.close()
    # Offline, the resolution reads replay from the capture: no provider is reached.
    offline = gateway(tmp_path, {})
    replayed = PoolGateway(offline, (pool_scope(binding()),))
    with offline.capture.connect() as db:
        db.execute("DELETE FROM pool_reads")
    reads = sum(chain.reads.values())
    assert replayed.pool(request) == first and sum(chain.reads.values()) == reads
    offline.capture.close()
    (tmp_path / "empty").mkdir()
    empty = gateway(tmp_path / "empty", {})
    with pytest.raises(DataError, match="missing a pool resolution read"):
        PoolGateway(empty, (pool_scope(binding()),)).pool(request)
    empty.capture.close()


def test_the_acquisition_gateway_routes_only_matching_price_pool_operations():
    chain = FakeChain()
    request = PoolRequest(scope=pool_scope(binding()), block=chain.header(10))
    calls = []
    target = SimpleNamespace(
        request_locks=[nullcontext()],
        pools=SimpleNamespace(
            pool=lambda value: calls.append(("pool", value)) or "pool",
            swaps=lambda value: calls.append(("swaps", value)) or "swaps",
        ),
    )
    wrapped = HistoricalRequest.model_validate_json(request.model_dump_json())
    assert AcquisitionGateway.dispatch(target, "price_pool", wrapped) == "pool"
    assert calls == [("pool", request)]
    with pytest.raises(ValueError, match="unsupported price pool operation"):
        AcquisitionGateway.dispatch(target, "swap_read", request)
