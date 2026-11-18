"""`ax pool`: V3-style pool state and bounded liquidity depth, without provider access."""

import json
from types import SimpleNamespace

import pytest

from almanak import ax
from almanak.caip import AssetType, ChainId
from almanak.connectors import resolved_pool_prices, v3_pool_request
from almanak.connectors.concentrated_math import sqrt_at_tick, tick_at_price
from almanak.connectors.concentrated_swap import SwapCalculation, advance, v3_step
from almanak.connectors.curve import pool_report
from almanak.connectors.uniswap_v3.domain import PoolObservation, PoolPriceRequest, UniswapPool
from almanak.connectors.v3_pool_request import (
    Family,
    PoolRequest,
    band,
    depth,
    read_pool_state,
    support_level,
    walk_down,
)
from almanak.gateway.ethereum import DataError
from almanak.models import Asset, Block, Chain

CHAIN = Chain(chain_id=ChainId("eip155:4663"), environment="mainnet")
BLOCK = Block(chain=CHAIN, number=1_000, hash="0x" + "ab" * 32, timestamp=1_790_000_000)
POOL = "0xc61284332117c3fb23a2a56cceffd07f7af60029"
TOKEN0 = AssetType("eip155:4663/erc20:0x4a0e65a3eccec6dbe60ae065f2e7bb85fae35eea")
TOKEN1 = AssetType("eip155:4663/erc20:0x5fc5360d0400a0fd4f2af552add042d716f1d168")
SPACING = 10


def source(spacing=SPACING):
    return UniswapPool(
        kind="uniswap_v3",
        chain=CHAIN,
        factory="0x1f7d7550b1b028f7571e69a784071f0205fd2efa",
        pool=POOL,
        currency0=TOKEN0,
        currency1=TOKEN1,
        fee=500,
        tick_spacing=spacing,
    )


def observation(positions, tick, *, spacing=SPACING, at_boundary=False):
    """Active liquidity is the sum of positions whose range holds the current tick."""
    price = (
        sqrt_at_tick(tick) if at_boundary else (sqrt_at_tick(tick) + sqrt_at_tick(tick + 1)) // 2
    )
    return PoolObservation(
        source=source(spacing),
        block=BLOCK,
        currency0=Asset(chain=CHAIN, identifier=TOKEN0, decimals=18),
        currency1=Asset(chain=CHAIN, identifier=TOKEN1, decimals=6),
        sqrt_price_x96=price,
        tick=tick,
        liquidity=sum(amount for lower, upper, amount in positions if lower <= tick < upper),
        protocol_fee=0,
        unlocked=True,
    )


def nets(positions):
    result: dict[int, int] = {}
    for lower, upper, amount in positions:
        result[lower] = result.get(lower, 0) + amount
        result[upper] = result.get(upper, 0) - amount
    return result


class TickReader:
    """Serves the tick bitmap and tick records that the positions initialize."""

    def __init__(self, positions, spacing=SPACING):
        self.chain = CHAIN
        self.nets = nets(positions)
        self.spacing = spacing
        self.gross = {index: 0 for index in self.nets}
        for lower, upper, amount in positions:
            self.gross[lower] += amount
            self.gross[upper] += amount
        self.calls: list[str] = []

    def bitmap(self, word):
        compressed = (index // self.spacing for index in self.nets)
        return sum(1 << (value & 255) for value in compressed if value >> 8 == word)

    def call(self, target, signature, block, inputs=(), args=(), outputs=()):
        assert (target, block) == (POOL, BLOCK)
        self.calls.append(signature)
        if signature == "tickBitmap(int16)":
            return (self.bitmap(args[0]),)
        if signature == "ticks(int24)":
            index = args[0]
            return (self.gross[index], self.nets[index], 0, 0, 0, 0, 0, True)
        raise AssertionError(f"unexpected call {signature}")


def swapped_to(pool, positions, target, spacing=SPACING):
    """Independent oracle: zero-fee V3 swap steps over the bitmap until the price reaches target."""
    reader = TickReader(positions, spacing)
    down = target < pool.sqrt_price_x96
    words = {word: reader.bitmap(word) for word in range(-3_500, 3_500)}
    result = SwapCalculation(0, pool.sqrt_price_x96, pool.tick, pool.liquidity)
    while result.sqrt_price_x96 != target:
        advance(result, 2**126, target, 0, spacing, words, reader.nets, down, v3_step)
    return result.output


def family(**overrides):
    selected = v3_pool_request.family("uniswap_v3")
    return Family(**{**selected.__dict__, **overrides})


POSITIONS = [
    (-600, 600, 10**20),
    (-200, 50, 3 * 10**19),
    (-40, 30, 7 * 10**18),
    (100, 1_000, 5 * 10**19),
    (-1_000, -300, 2 * 10**19),
]


@pytest.mark.parametrize("bps", [1, 25, 200, 900])
@pytest.mark.parametrize("tick", [0, 7, -35, 29])
def test_depth_matches_zero_fee_swaps_to_each_band_edge(bps, tick):
    pool = observation(POSITIONS, tick)
    reader = TickReader(POSITIONS)
    result = depth(reader, family(), pool, BLOCK, bps)
    lower, upper = band(pool.sqrt_price_x96, bps)
    down = swapped_to(pool, POSITIONS, lower)
    up = swapped_to(pool, POSITIONS, upper)
    # Swap steps also stop at uninitialized word boundaries and each step rounds
    # down, so the swap can pay out a few units less than the walk.
    assert 0 <= int(result["to_lower"]["amount"]) - down <= 8
    assert 0 <= int(result["to_upper"]["amount"]) - up <= 8
    assert result["to_lower"]["asset"] == str(TOKEN1)
    assert result["to_upper"]["asset"] == str(TOKEN0)
    assert (result["to_lower"]["decimals"], result["to_upper"]["decimals"]) == (6, 18)
    assert result["initialized_ticks"] == reader.calls.count("ticks(int24)")
    assert result["bitmap_words"] == reader.calls.count("tickBitmap(int16)")


def test_depth_crosses_the_current_tick_first_at_an_exact_boundary():
    positions = [(-600, 600, 10**20), (0, 300, 4 * 10**19)]
    pool = observation(positions, 0, at_boundary=True)
    lower, _ = band(pool.sqrt_price_x96, 100)
    # At the boundary the upper position is active but provides nothing below it.
    assert walk_down(pool, lower, nets(positions)) == swapped_to(pool, positions, lower)


def test_depth_on_a_negative_spacing_word_reads_the_right_bitmap():
    positions = [(-6_000, -1_200, 10**20), (-3_000, 600, 10**19)]
    pool = observation(positions, -2_040, spacing=60)
    reader = TickReader(positions, spacing=60)
    result = depth(reader, family(), pool, BLOCK, 500)
    lower, upper = band(pool.sqrt_price_x96, 500)
    assert 0 <= int(result["to_lower"]["amount"]) - swapped_to(pool, positions, lower, 60) <= 8
    assert 0 <= int(result["to_upper"]["amount"]) - swapped_to(pool, positions, upper, 60) <= 8


def test_band_bounds_bracket_the_requested_price_move():
    price = sqrt_at_tick(12_345)
    lower, upper = band(price, 200)
    assert lower * lower <= price * price * 98 // 100 < (lower + 1) ** 2
    assert upper * upper <= price * price * 102 // 100 < (upper + 1) ** 2
    assert tick_at_price(lower) < 12_345 < tick_at_price(upper)


def test_depth_refuses_bands_outside_its_bounds():
    pool = observation(POSITIONS, 0)
    for bps in (0, 5_001):
        with pytest.raises(ValueError, match="between 1 and 5000"):
            depth(TickReader(POSITIONS), family(), pool, BLOCK, bps)
    crowded = [(index, index + 1, 10**15) for index in range(-1_100, 1_100)]
    with pytest.raises(ValueError, match="initialized-tick collection limit"):
        crowded_pool = observation(crowded, 0, spacing=1)
        depth(TickReader(crowded, spacing=1), family(), crowded_pool, BLOCK, 2_500)


def test_depth_refuses_inconsistent_tick_liquidity():
    pool = observation(POSITIONS, 0)
    broken = {**nets(POSITIONS), -40: 10**30}
    with pytest.raises(DataError, match="outside uint128"):
        walk_down(pool, band(pool.sqrt_price_x96, 200)[0], broken)


def test_request_requires_an_evm_chain_and_nonzero_pool():
    PoolRequest(chain=ChainId("eip155:4663"), pool=POOL)
    with pytest.raises(ValueError, match="nonzero pool"):
        PoolRequest(chain=ChainId("eip155:4663"), pool="0x" + "00" * 20)
    with pytest.raises(ValueError, match="EVM chain"):
        PoolRequest.model_validate_json(
            json.dumps({"chain": "solana:5eykt4UsFv8P8NJdTREpY1vzqKqZKvdp", "pool": POOL})
        )


UNDECLARED = ChainId("eip155:999999")


def test_support_level_distinguishes_verified_candidate_and_undeclared(monkeypatch):
    verified, candidate = ChainId("eip155:1"), ChainId("eip155:4663")
    tables = SimpleNamespace(
        verified_support=lambda: (SimpleNamespace(chain=verified),),
        candidate_support=lambda: (
            SimpleNamespace(chain=verified),
            SimpleNamespace(chain=candidate),
        ),
    )
    monkeypatch.setattr(v3_pool_request, "support_tables", lambda connector: tables)
    assert support_level("uniswap_v3", verified) == "verified"
    assert support_level("uniswap_v3", candidate) == "candidate"
    listed = "no support on eip155:999999; declared chains: eip155:1, eip155:4663"
    with pytest.raises(ValueError, match=listed):
        support_level("uniswap_v3", UNDECLARED)


@pytest.mark.parametrize(
    "connector", v3_pool_request.POOL_CONNECTORS + v3_pool_request.RESOLVED_POOL_CONNECTORS
)
def test_support_level_follows_each_connectors_declarations(connector):
    # Promotion moves chains between tiers, so expectations come from the tables.
    support = v3_pool_request.support_tables(connector)
    verified = {entry.chain for entry in support.verified_support()}
    for chain in {entry.chain for entry in support.candidate_support()}:
        assert support_level(connector, chain) == ("verified" if chain in verified else "candidate")
    with pytest.raises(ValueError, match=f"{connector} declares no support on eip155:999999"):
        support_level(connector, UNDECLARED)
    with pytest.raises(ValueError, match="unsupported pool connector"):
        v3_pool_request.family("uniswap_v4")


def resolved(pool):
    return SimpleNamespace(pool=pool.source, summary=lambda: {"pool": POOL, "block": BLOCK.number})


def test_read_pool_state_reports_resolution_state_price_and_depth(monkeypatch):
    pool = observation(POSITIONS, 7)
    seen = {}

    def resolve(reader, selection, block):
        seen["selection"] = selection
        return resolved(pool)

    resolution = SimpleNamespace(
        MarketSelection=v3_pool_request.family("uniswap_v3").resolution.MarketSelection,
        resolve_market=resolve,
    )
    selected = family(resolution=resolution, read_pool=lambda reader, source, block: pool)
    monkeypatch.setattr(v3_pool_request, "family", lambda connector: selected)
    request = PoolRequest(chain=CHAIN.chain_id, pool=POOL)
    result = read_pool_state(TickReader(POSITIONS), "uniswap_v3", request, BLOCK, 200)
    assert seen["selection"].pool == POOL
    assert seen["selection"].chain == CHAIN.chain_id
    assert result["product"] == "pool_state"
    assert result["request"] == {"chain": "eip155:4663", "pool": POOL, "connector": "uniswap_v3"}
    assert result["support"] == support_level("uniswap_v3", CHAIN.chain_id)
    assert result["resolution"] == {"pool": POOL, "block": BLOCK.number}
    assert result["observation"]["liquidity"] == pool.liquidity
    expected = pool.spot_price(PoolPriceRequest(source=pool.source, base=TOKEN0, quote=TOKEN1))
    assert result["price"] == {
        "base": str(TOKEN0),
        "quote": str(TOKEN1),
        "kind": "spot",
        "numerator": str(expected.numerator),
        "denominator": str(expected.denominator),
    }
    assert result["depth"]["bps"] == 200
    json.dumps(result)


def test_read_pool_state_omits_depth_and_price_when_not_measurable(monkeypatch):
    pool = observation([(100, 200, 10**18)], 0)
    resolution = SimpleNamespace(
        MarketSelection=v3_pool_request.family("uniswap_v3").resolution.MarketSelection,
        resolve_market=lambda reader, selection, block: resolved(pool),
    )
    selected = family(resolution=resolution, read_pool=lambda reader, source, block: pool)
    monkeypatch.setattr(v3_pool_request, "family", lambda connector: selected)
    reader = TickReader([])
    result = read_pool_state(
        reader, "uniswap_v3", PoolRequest(chain=CHAIN.chain_id, pool=POOL), BLOCK
    )
    assert pool.liquidity == 0
    assert result["price"] is None
    assert "depth" not in result
    assert reader.calls == []


def test_read_pool_state_refuses_a_reader_on_another_chain():
    reader = SimpleNamespace(chain=Chain(chain_id=ChainId("eip155:1"), environment="mainnet"))
    with pytest.raises(ValueError, match="differs from the reader chain"):
        read_pool_state(reader, "uniswap_v3", PoolRequest(chain=CHAIN.chain_id, pool=POOL), BLOCK)


def invocation(config, **overrides):
    return SimpleNamespace(
        **{
            "product": "ax-pool",
            "ax_command": "pool",
            "connector": "uniswap_v3",
            "config": config,
            "block": 1000,
            "env_file": None,
            "gateway_url": "http://127.0.0.1:50052",
            "gateway_auth_file": "token",
            "depth_bps": 200,
            **overrides,
        }
    )


class Opened:
    chain = CHAIN

    def pin_block(self, selector):
        assert selector == 1000
        return BLOCK

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return None


def test_pool_handler_passes_request_block_and_depth(monkeypatch, tmp_path):
    config = tmp_path / "pool.json"
    config.write_text(json.dumps({"chain": "eip155:4663", "pool": POOL}))
    seen = {}

    def read(reader, connector, request, block, depth_bps):
        seen.update(connector=connector, request=request, block=block, depth_bps=depth_bps)
        return {"product": "pool_state"}

    monkeypatch.setattr(ax, "read_pool_state", read)
    monkeypatch.setattr(ax, "gateway_reader", lambda chain, url, auth: Opened())
    assert ax.pool(invocation(config)) == {"product": "pool_state"}
    assert seen == {
        "connector": "uniswap_v3",
        "request": PoolRequest(chain=CHAIN.chain_id, pool=POOL),
        "block": BLOCK,
        "depth_bps": 200,
    }


def test_pool_handler_refuses_undeclared_chains_before_any_transport(monkeypatch, tmp_path):
    config = tmp_path / "pool.json"
    config.write_text(json.dumps({"chain": str(UNDECLARED), "pool": POOL}))

    def forbidden(*_):
        raise AssertionError("transport opened before the support check")

    monkeypatch.setattr(ax, "gateway_reader", forbidden)
    with pytest.raises(ValueError, match="pancakeswap_v3 declares no support on eip155:999999"):
        ax.pool(invocation(config, connector="pancakeswap_v3"))


def test_inline_pool_request_validates_as_the_file_form(monkeypatch):
    seen = {}

    def read(reader, connector, request, block, depth_bps):
        seen.update(request=request)
        return {"product": "pool_state"}

    monkeypatch.setattr(ax, "read_pool_state", read)
    monkeypatch.setattr(ax, "gateway_reader", lambda chain, url, auth: Opened())
    inline = invocation(None, chain="eip155:4663", pool=POOL)
    assert ax.pool(inline) == {"product": "pool_state"}
    assert seen["request"] == PoolRequest(chain=CHAIN.chain_id, pool=POOL)


@pytest.mark.parametrize(
    ("chain", "pool", "field"),
    [
        ("4663", POOL, "chain"),
        ("eip155:4663", "0x1234", "pool"),
        ("eip155:4663", "0x" + "00" * 20, ""),
    ],
)
def test_inline_pool_request_errors_keep_the_json_error_shape(
    monkeypatch, capsys, chain, pool, field
):
    from almanak.cli import main

    def forbidden(*_):
        raise AssertionError("transport opened for an invalid request")

    monkeypatch.setattr(ax, "gateway_reader", forbidden)
    monkeypatch.setattr(ax, "verified_reader", forbidden)
    argv = ["almanak", "ax", "pool", "--connector", "uniswap_v3", "--chain", chain]
    argv += ["--pool", pool, "--block", "latest", "--env-file", "operator.env"]
    monkeypatch.setattr("sys.argv", argv)
    assert main() == 1
    error = json.loads(capsys.readouterr().err)
    assert error["error"] == "invalid_config"
    assert [problem["field"] for problem in error["problems"]] == [field]


def test_resolved_pools_report_their_connectors_resolution_and_prices(monkeypatch):
    seen = {}
    source = SimpleNamespace(stable=False)
    observation = SimpleNamespace(
        currency0=SimpleNamespace(identifier="weth", decimals=18),
        currency1=SimpleNamespace(identifier="usdc", decimals=6),
        reserve0=2025 * 10**17,
        reserve1=473_000_120_000,
    )

    def resolve_market(reader, selection, block):
        seen["selection"] = selection
        return SimpleNamespace(pool=source, summary=lambda: {"pool": POOL, "stable": False})

    def read_pool(reader, pool, block):
        seen["read"] = (pool, block)
        return observation

    modules = {
        "resolution": SimpleNamespace(
            MarketSelection=lambda **values: SimpleNamespace(**values),
            resolve_market=resolve_market,
        ),
        "reader": SimpleNamespace(read_pool=read_pool),
    }
    monkeypatch.setattr(v3_pool_request, "import_module", lambda name: modules[name.split(".")[-1]])
    monkeypatch.setattr(v3_pool_request, "support_level", lambda connector, chain: "verified")
    # Each connector's discovery declaration names its price function; patch those.
    monkeypatch.setattr(
        resolved_pool_prices, "aerodrome_price", lambda observation: {"kind": "spot"}
    )
    monkeypatch.setattr(
        resolved_pool_prices, "curve_prices", lambda observation: [{"kind": "oracle"}]
    )
    monkeypatch.setattr(pool_report, "unfunded_report", lambda reader, resolved, block: None)
    monkeypatch.setattr(pool_report, "pool_state", lambda observation: {"fee": "state"})
    reader = SimpleNamespace(chain=CHAIN)
    request = PoolRequest(chain=CHAIN.chain_id, pool=POOL)
    assert v3_pool_request.read_resolved_pool(reader, "aerodrome", request, BLOCK) == {
        "product": "pool_resolution",
        "request": {"chain": str(CHAIN.chain_id), "pool": POOL, "connector": "aerodrome"},
        "support": "verified",
        "resolution": {"pool": POOL, "stable": False},
        "price": {"kind": "spot"},
        "reserves": [
            {"asset": "weth", "amount": str(2025 * 10**17), "decimals": 18, "quantity": "202.5"},
            {"asset": "usdc", "amount": "473000120000", "decimals": 6, "quantity": "473000.12"},
        ],
    }
    assert (seen["selection"].connector, seen["selection"].pool) == ("aerodrome", POOL)
    assert seen["read"] == (source, BLOCK)
    curve = v3_pool_request.read_resolved_pool(reader, "curve", request, BLOCK)
    assert (curve["prices"], "price" in curve) == ([{"kind": "oracle"}], False)
    assert curve["pool_state"] == {"fee": "state"} and "status" not in curve
    with pytest.raises(ValueError, match="unsupported pool connector"):
        v3_pool_request.read_resolved_pool(reader, "uniswap_v3", request, BLOCK)
    other = SimpleNamespace(chain=Chain(chain_id=ChainId("eip155:8453"), environment="mainnet"))
    with pytest.raises(ValueError, match="differs from the reader chain"):
        v3_pool_request.read_resolved_pool(other, "curve", request, BLOCK)


def test_aerodrome_and_curve_pools_route_to_their_resolution_without_depth(monkeypatch):
    seen = {}

    def read(reader, connector, request, block):
        seen.update(connector=connector, request=request, block=block)
        return {"product": "pool_resolution"}

    monkeypatch.setattr(ax, "read_resolved_pool", read)
    monkeypatch.setattr(ax, "gateway_reader", lambda chain, url, auth: Opened())
    base = "eip155:8453"
    plain = invocation(None, connector="aerodrome", chain=base, pool=POOL, depth_bps=None)
    assert ax.pool(plain) == {"product": "pool_resolution"}
    assert seen == {
        "connector": "aerodrome",
        "request": PoolRequest(chain=ChainId(base), pool=POOL),
        "block": BLOCK,
    }
    with pytest.raises(ValueError, match="--depth-bps is available for V3-style"):
        ax.pool(invocation(None, connector="curve", chain=base, pool=POOL, depth_bps=100))
