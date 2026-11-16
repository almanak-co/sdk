"""Aerodrome classic and Curve pools as strategy price sources.

The spot formulas are checked against the connectors' own swap maths; closes and
candles against values computed here from a synthetic chain's reserves, balances,
last prices and trade logs.
"""

from decimal import Decimal
from fractions import Fraction
from types import SimpleNamespace

import pytest

from almanak.backtest.pool_prices import (
    PricePool,
    compact_swaps,
    rounded,
    series_records,
    verify_rows,
)
from almanak.backtest.pool_reads import DirectReads, acquire, pool_scope, resolve_pool
from almanak.backtest.pool_venues import (
    A_PRECISE,
    AERODROME_SWAP,
    CRYPTO_EXCHANGE,
    GET_BALANCES,
    GET_RESERVES,
    LAST_PRICES,
    LAST_PRICES_AT,
    STABLE_EXCHANGE,
    SYNC,
    state_values,
    trade,
    word,
)
from almanak.backtest.pricing import PoolPrice, PoolSwapCandles, PriceBinding
from almanak.caip import AssetType
from almanak.connectors.aerodrome import resolution as aerodrome_resolution
from almanak.connectors.aerodrome.math import quote as aerodrome_quote
from almanak.connectors.curve import resolution as curve_resolution
from almanak.connectors.curve import stable_math
from almanak.connectors.pool_spot_prices import aerodrome_spot, crypto_spot, stable_spot
from almanak.gateway.ethereum import DataError
from almanak.models import Asset
from tests.pool_price_chain import CHAIN, GENESIS, HOUR, POOL, USDC, WETH, FakeChain

WBTC = AssetType("eip155:8453/erc20:0x0555e30da8f98308edb960aa94c0db47230d2b9c")
DAI = AssetType("eip155:8453/erc20:0x50c5725949a6f0c72e6c4a641f24049a917db0cb")
DECIMALS = {WETH: 18, USDC: 6, WBTC: 8, DAI: 18}
NOVEMBER = "stableswap-ng-2023-11-15"


def assets(*coins):
    return tuple(Asset(chain=CHAIN, identifier=c, decimals=DECIMALS[c]) for c in coins)


def pool(pricing, coins, topics, revision=None, connector="curve"):
    return PricePool(
        connector=connector,
        pool=POOL,
        block=FakeChain().header(1),
        assets=assets(*coins),
        pricing=pricing,
        revision=revision,
        state=POOL,
        emitter=POOL,
        swap_topics=topics,
    )


AERO = pool("aerodrome_volatile", (WETH, USDC), ((SYNC, AERODROME_SWAP),), connector="aerodrome")
AERO_STABLE = AERO.model_copy(update={"pricing": "aerodrome_stable"})
STABLE = pool("curve_stableswap", (DAI, USDC), (STABLE_EXCHANGE,), NOVEMBER)
TRI = pool("curve_tricrypto", (USDC, WBTC, WETH), (CRYPTO_EXCHANGE,))


def binding(connector, kind="pool_state", asset=WETH, quote=None, window=2):
    source = PoolPrice if kind == "pool_state" else PoolSwapCandles
    return PriceBinding(
        purpose="strategy",
        chain=CHAIN,
        asset=asset,
        window=window,
        source=source(
            kind=kind,
            connector=connector,
            pool=POOL,
            timeframe="1h",
            max_age_seconds=2 * HOUR,
            availability_lag_seconds=60,
            quote=quote,
        ),
    )


# Spot maths, adapted from the `ax pool` pull request's tests (#120).


def test_a_volatile_aerodrome_price_is_the_whole_unit_reserve_ratio():
    assert aerodrome_spot(False, 3_000 * 10**6, 10**18, 6, 18) == Fraction(1, 3_000)
    assert aerodrome_spot(False, 0, 10**18, 6, 18) is None


@pytest.mark.parametrize("reserves", [(10**24, 10**24), (2 * 10**24, 10**24), (10**24, 3 * 10**24)])
def test_a_stable_aerodrome_price_matches_a_tiny_fee_free_swap(reserves):
    price = aerodrome_spot(True, *reserves, 18, 18)
    amount = 10**12
    output = aerodrome_quote(True, 10**18, 10**18, *reserves, amount, 0, True)
    assert abs(Fraction(output, amount) - price) < Fraction(1, 10**6)
    assert (price == 1) == (reserves[0] == reserves[1])


def stable_state(balances, decimals, amplification=200_000, rates=None):
    return stable_math.StableState(
        balances=balances,
        rates=rates or tuple(10 ** (36 - d) for d in decimals),
        amplification=amplification,
        fee=0,
        offpeg_multiplier=10**10,
        total_supply=sum(balances),
    )


def test_a_balanced_stableswap_pool_prices_each_coin_at_par():
    state = stable_state((10**24, 10**12, 10**24), (18, 6, 18))
    prices = stable_spot(state.balances, state.rates, (18, 6, 18), 200_000, NOVEMBER)
    assert prices == (1, 1)


@pytest.mark.parametrize("amplification", [1_000, 200_000])
def test_an_imbalanced_stableswap_price_matches_a_tiny_fee_free_swap(amplification):
    state = stable_state((3 * 10**24, 10**12), (18, 6), amplification)
    (price,) = stable_spot(state.balances, state.rates, (18, 6), amplification, NOVEMBER)
    amount = 10**6
    output = stable_math.swap(state, 1, 0, amount).output
    assert price > 1
    assert abs(Fraction(output, 10**18) / Fraction(amount, 10**6) - price) < Fraction(1, 10**6)


def test_a_stableswap_price_converts_an_oracle_rate_into_whole_coins():
    state = stable_state((2 * 10**24, 10**24), (18, 18), rates=(10**18, 2 * 10**18))
    assert state.normalized[0] == state.normalized[1]
    (price,) = stable_spot(state.balances, state.rates, (18, 18), 200_000, NOVEMBER)
    assert price == 2
    output = stable_math.swap(state, 1, 0, 10**12).output
    assert abs(Fraction(output, 10**12) - 2) < Fraction(1, 10**6)
    assert stable_spot((10**24, 0), state.rates, (18, 18), 200_000, NOVEMBER) is None


def test_crypto_pools_price_each_later_coin_in_coin_zero():
    assert crypto_spot((2_500 * 10**18,)) == (2_500,)
    assert crypto_spot((2 * 10**18, 3 * 10**18)) == (2, 3)
    assert crypto_spot((0,)) is None


# Closes from each pool's state reads.


def returned(*values):
    return "0x" + "".join(word(v) for v in values)


def test_aerodrome_closes_price_the_reserves_spot():
    raw = (1_000 * 10**18, 2_500_000 * 10**6)
    assert state_values(AERO, (returned(*raw, 77),)) == raw
    assert AERO.price(AERO.pair(WETH), raw) == Decimal(2500)
    assert AERO.price(AERO.pair(USDC), raw) == Decimal("0.0004")
    expected = aerodrome_spot(True, *raw, 18, 6)
    assert AERO_STABLE.price((0, 1), raw) == rounded(expected)
    # Stable pricing of unequal whole reserves is not their ratio.
    assert AERO_STABLE.price((0, 1), raw) != Decimal(2500)
    assert [r.params[0]["data"] for r in AERO.state_requests(AERO.block)] == [GET_RESERVES]
    with pytest.raises(ValueError, match="no price"):
        AERO.price((0, 1), (0, 10))


def test_curve_closes_read_balances_and_amplification_or_last_prices():
    block = STABLE.block
    calls = [r.params[0]["data"] for r in STABLE.state_requests(block)]
    assert calls == [GET_BALANCES, A_PRECISE]

    balances = returned(32, 2, 3 * 10**24, 10**12)
    state = state_values(STABLE, (balances, returned(200_000)))
    assert state == (3 * 10**24, 10**12, 200_000)
    (spot,) = stable_spot(state[:2], (10**18, 10**30), (18, 6), 200_000, NOVEMBER)
    # USDC in DAI is the spot; DAI in USDC its inverse.
    assert STABLE.price(STABLE.pair(USDC), state) == rounded(spot)
    assert STABLE.price(STABLE.pair(DAI), state) == rounded(1 / spot)
    with pytest.raises(ValueError, match="differs from the pool's coins"):
        state_values(STABLE, (returned(32, 3, 1, 2, 3), returned(200_000)))
    tri = [r.params[0]["data"] for r in TRI.state_requests(block)]
    assert tri == [LAST_PRICES_AT + word(0), LAST_PRICES_AT + word(1)]
    prices = (80_000 * 10**18, 2_500 * 10**18)
    # WETH in WBTC through both coins' prices in USDC.
    assert TRI.price(TRI.pair(WETH, WBTC), prices) == Decimal("0.03125")
    assert TRI.price(TRI.pair(WETH, USDC), prices) == Decimal(2500)
    two = pool("curve_twocrypto", (USDC, WETH), (CRYPTO_EXCHANGE,))
    assert [r.params[0]["data"] for r in two.state_requests(block)] == [LAST_PRICES]


def test_a_multi_coin_pool_needs_a_declared_quote_and_a_pool_its_coin_count():
    with pytest.raises(ValueError, match="only in a declared quote asset"):
        TRI.pair(WETH)
    with pytest.raises(ValueError, match="not another of the pool's tokens"):
        TRI.pair(WETH, DAI)
    with pytest.raises(ValueError, match="not another of the pool's tokens"):
        TRI.pair(WETH, WETH)
    with pytest.raises(ValueError, match="cannot hold 2 coins"):
        pool("curve_tricrypto", (USDC, WETH), (CRYPTO_EXCHANGE,))
    with pytest.raises(ValueError, match="integer revision"):
        pool("curve_stableswap", (DAI, USDC), (STABLE_EXCHANGE,))
    with pytest.raises(ValueError, match="another asset on the binding's chain"):
        binding("curve", quote=WETH)
    foreign = AssetType("eip155:1/erc20:0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48")
    with pytest.raises(ValueError, match="another asset on the binding's chain"):
        binding("curve", quote=foreign)
    # A two-coin pool's quote is implied; declaring it changes the series identity.
    assert "quote" not in binding("aerodrome").source.model_dump()
    assert binding("aerodrome").key != binding("aerodrome", quote=USDC).key


# Candles from trade logs on a synthetic chain.


class VenueChain(FakeChain):
    """`state(n)` gives the pool's state reads after block n; `logs` are
    (block, topic, data words) of the pool's events."""

    def __init__(self, state, logs):
        super().__init__()
        self.state = state
        self.events = logs

    def _rpc(self, method, params, *, allow_none=False):
        if method == "eth_call":
            self.reads[method] += 1
            call, anchor = params
            assert call["to"] == POOL
            return self.state(int(anchor["blockHash"], 16))[call["data"]]
        if method == "eth_getLogs":
            self.reads[method] += 1
            (query,) = params
            first, last = int(query["fromBlock"], 16), int(query["toBlock"], 16)
            return [
                self.event(index, item)
                for index, item in enumerate(self.events)
                if first <= item[0] <= last
            ]
        return super()._rpc(method, params, allow_none=allow_none)

    def event(self, index, item):
        number, event, values = item
        return {
            "removed": False,
            "address": POOL,
            "blockNumber": hex(number),
            "blockHash": "0x" + f"{number:064x}",
            "logIndex": hex(index),
            "topics": [event, "0x" + "00" * 32],
            "data": returned(*values),
        }


FIRST = HOUR // 2  # the block at the first boundary, an hour after genesis
T1, T2 = GENESIS + 2 * HOUR, GENESIS + 3 * HOUR


def aerodrome_chain():
    """A trade and a liquidity change in the second hour; none in the third."""
    reserves = [(0, (1_000 * 10**18, 2_000_000 * 10**6))]
    logs = []
    for number, weth, usdc, swap in (
        (FIRST + 100, 990 * 10**18, 2_020_000 * 10**6, (0, 20_000 * 10**6, 10 * 10**18, 0)),
        (FIRST + 900, 1_020 * 10**18, 1_960_000 * 10**6, (30 * 10**18, 0, 0, 60_000 * 10**6)),
        (FIRST + 1000, 2_040 * 10**18, 3_920_000 * 10**6, None),
    ):
        reserves.append((number, (weth, usdc)))
        logs.append((number, SYNC, (weth, usdc)))
        if swap is not None:
            logs.append((number, AERODROME_SWAP, swap))

    def state(number):
        latest = [value for block, value in reserves if block <= number][-1]
        return {GET_RESERVES: returned(*latest, 0)}

    return VenueChain(state, logs)


def candles(chain, series, pool_, times=(T1, T2)):
    found = acquire(DirectReads(chain), series, times, pool=pool_)
    return series_records(series, found, times, 10.0**10), found


def test_aerodrome_candles_span_every_sync_and_sum_swap_quote_amounts():
    chain = aerodrome_chain()
    series = binding("aerodrome", "pool_swaps")
    (traded, quiet), found = candles(chain, series, AERO)
    assert traded.payload.open == Decimal(2000)
    assert traded.payload.close == rounded(Fraction(3_920_000, 2_040))
    assert traded.payload.high == rounded(Fraction(2_020_000, 990))
    assert traded.payload.low == traded.payload.close
    # Quote volume: 20,000 USDC in plus 60,000 USDC out; the liquidity Sync adds none.
    assert traded.payload.volume == Decimal(80_000) and traded.reference.swaps == 2
    assert traded.reference.state == (2_040 * 10**18, 3_920_000 * 10**6)
    assert quiet.payload.open == quiet.payload.high == quiet.payload.low == quiet.payload.close
    assert quiet.payload.volume == 0 and quiet.reference.swaps == 0
    # One filter per candle's window reads both events.
    assert chain.reads["eth_getLogs"] == 2
    bodies = found.bodies
    verify_rows(series, (traded, quiet), bodies.__getitem__)


def curve_chain():
    """WETH/USDC trades of a TriCrypto pool, one WBTC/USDC trade and a dust trade."""
    usdc, weth = 10**6, 10**18
    logs = [
        (FIRST + 100, CRYPTO_EXCHANGE, (0, 2_600 * usdc, 2, weth, 7, 0)),
        (FIRST + 200, CRYPTO_EXCHANGE, (2, 2 * weth, 0, 4_800 * usdc, 9, 0)),
        (FIRST + 300, CRYPTO_EXCHANGE, (0, 80_000 * usdc, 1, 10**8, 1, 0)),
        (FIRST + 400, CRYPTO_EXCHANGE, (2, 1, 0, 0, 0, 0)),
    ]

    def state(number):
        weth_price = 2_500 if number < FIRST + 200 else 2_450
        return {
            LAST_PRICES_AT + word(0): returned(80_000 * 10**18),
            LAST_PRICES_AT + word(1): returned(weth_price * 10**18),
        }

    return VenueChain(state, logs)


def test_curve_candles_use_the_pair_s_execution_prices_and_quote_amounts():
    chain = curve_chain()
    series = binding("curve", "pool_swaps", quote=USDC)
    (traded, quiet), found = candles(chain, series, TRI)
    assert (traded.payload.open, traded.payload.close) == (Decimal(2500), Decimal(2450))
    # Bought WETH at 2,600 USDC and sold two at 2,400 each, fees included.
    assert (traded.payload.high, traded.payload.low) == (Decimal(2600), Decimal(2400))
    # The WBTC trade is not the pair's; the dust trade has no price but counts.
    assert traded.payload.volume == Decimal(7_400) and traded.reference.swaps == 3
    assert traded.reference.state == (80_000 * 10**18, 2_450 * 10**18)
    assert quiet.payload.volume == 0 and quiet.payload.high == quiet.payload.close
    verify_rows(series, (traded, quiet), found.bodies.__getitem__)
    (wbtc, _), _ = candles(curve_chain(), binding("curve", "pool_swaps", WBTC, USDC), TRI)
    assert wbtc.payload.volume == Decimal(80_000) and wbtc.reference.swaps == 1
    with pytest.raises(DataError, match="only in a declared quote asset"):
        candles(curve_chain(), binding("curve", "pool_swaps"), TRI)


def test_curve_stableswap_closes_follow_balances_and_amplification():
    def state(number):
        dai = 1_000_000 if number <= FIRST else 1_500_000
        return {
            GET_BALANCES: returned(32, 2, dai * 10**18, 1_000_000 * 10**6),
            A_PRECISE: returned(200_000),
        }

    chain = VenueChain(state, [])
    series = binding("curve", asset=USDC)
    (before, after), _ = candles(chain, series, STABLE, (GENESIS + HOUR, T1))
    assert before.payload.close == 1 and before.payload.quote == DAI
    (spot,) = stable_spot(
        (1_500_000 * 10**18, 10**12), (10**18, 10**30), (18, 6), 200_000, NOVEMBER
    )
    assert after.payload.close == rounded(spot) and after.payload.close > 1
    assert after.reference.state == (1_500_000 * 10**18, 10**12, 200_000)
    assert chain.reads["eth_getLogs"] == 0


def test_evidence_must_be_the_pool_s_own_reads_and_events():
    chain = aerodrome_chain()
    series = binding("aerodrome", "pool_swaps")
    (traded, _), found = candles(chain, series, AERO)
    bodies = dict(found.bodies)
    # A boundary read of another contract is refused even with a matching answer.
    states = [d for d in traded.evidence if '"boundary_state"' in bodies[d].body]
    tampered = bodies | {
        d: SimpleNamespace(body=bodies[d].body.replace(POOL, "0x" + "12" * 20)) for d in states
    }
    with pytest.raises(ValueError, match="differs from its pool's state reads"):
        verify_rows(series, (traded,), tampered.__getitem__)
    # A V3 Swap record cannot stand in for an Aerodrome event.
    with pytest.raises(ValueError, match="differs from its pool's events"):
        trade(AERO, (1, 0, 1, 2, 3), (0, 1))
    window = AERO.window(FIRST + 1, chain.header(FIRST + 1800))
    raw = chain._rpc("eth_getLogs", window.params)
    assert len(compact_swaps(window, raw, 1.0).swaps) == 5
    wrong = [raw[0] | {"topics": [CRYPTO_EXCHANGE]}]
    with pytest.raises(ValueError, match="requested pool or event"):
        compact_swaps(window, wrong, 1.0)
    curve = TRI.window(FIRST + 1, chain.header(FIRST + 1800))
    bad = curve_chain().event(0, (FIRST + 2, CRYPTO_EXCHANGE, (9, 1, 0, 1, 0, 0)))
    with pytest.raises(ValueError, match="names an invalid coin"):
        compact_swaps(curve, [bad], 1.0)


# Resolution through each connector.


def resolved_aerodrome(stable):
    currency0, currency1 = assets(WETH, USDC)
    return SimpleNamespace(
        currencies=(currency0, currency1), pool=SimpleNamespace(stable=stable, pool=POOL)
    )


def test_an_aerodrome_pool_resolves_through_its_connector(monkeypatch):
    selections = []

    def resolve(reader, selection, block):
        selections.append(selection)
        return resolved_aerodrome(True)

    monkeypatch.setattr(aerodrome_resolution, "resolve_market", resolve)
    chain = FakeChain()
    scope = pool_scope(binding("aerodrome"))
    found = resolve_pool(chain, scope, chain.header(10))
    assert selections[0].connector == "aerodrome" and selections[0].pool == POOL
    assert found.pricing == "aerodrome_stable" and found.swap_topics == ((SYNC, AERODROME_SWAP),)
    assert found.swap_topics in scope.admitted and found.state == found.emitter == POOL
    scope.require_deployment()


@pytest.mark.parametrize(
    ("family", "pricing", "event"),
    [
        ("stableswap_ng", "curve_stableswap", STABLE_EXCHANGE),
        ("twocrypto_ng", "curve_twocrypto", CRYPTO_EXCHANGE),
        ("tricrypto_ng", "curve_tricrypto", CRYPTO_EXCHANGE),
    ],
)
def test_a_curve_pool_resolves_its_family_through_its_connector(
    monkeypatch, family, pricing, event
):
    coins = (USDC, WBTC, WETH) if family == "tricrypto_ng" else (DAI, USDC)
    rates = [10 ** (36 - DECIMALS[c]) for c in coins]
    resolved = SimpleNamespace(
        coins=assets(*coins), revision=NOVEMBER, pool=SimpleNamespace(family=family, pool=POOL)
    )
    monkeypatch.setattr(curve_resolution, "resolve_market", lambda *args: resolved)
    chain = FakeChain()
    monkeypatch.setattr(chain, "call", lambda *args, **kwargs: (rates,))
    scope = pool_scope(binding("curve"))
    found = resolve_pool(chain, scope, chain.header(10))
    assert (found.pricing, found.swap_topics) == (pricing, (event,))
    assert found.swap_topics in scope.admitted
    assert found.revision == (NOVEMBER if family == "stableswap_ng" else None)
    if family == "stableswap_ng":
        rates[0] *= 2
        with pytest.raises(DataError, match="rates differ from immutable token precision"):
            resolve_pool(chain, scope, chain.header(10))
    scope.require_deployment()


def test_the_live_gateway_serves_the_same_aerodrome_candles(tmp_path, monkeypatch):
    from almanak.backtest import pool_reads
    from almanak.gateway import strategy_prices as live

    monkeypatch.setattr(pool_reads, "resolve_pool", lambda reader, scope, block: AERO)
    series = binding("aerodrome", "pool_swaps")
    (backtest, _), _ = candles(aerodrome_chain(), series, AERO)
    source = live.StrategyPriceSource(SimpleNamespace(reader=aerodrome_chain()), tmp_path / "p")
    (served,) = source.series("d", (series,), T2 + 60)
    assert served.highs == (backtest.payload.high, rounded(Fraction(3_920_000, 2_040)))
    assert [p.value for p in served.points] == [backtest.payload.close, served.values[-1]]
    assert served.volumes == (Decimal(80_000), 0) and served.quote == USDC
    live.require_price_chains((series, binding("curve", quote=USDC)), {CHAIN})


def test_aerodrome_swap_volume_is_the_larger_quote_amount():
    # Records are (block, index, amount0In, amount1In, amount0Out, amount1Out).
    assert trade(AERO, (1, 0, 0, 500, 7, 0), (0, 1)) == (None, 500)
    assert trade(AERO, (1, 0, 9, 0, 0, 400), (0, 1)) == (None, 400)
    # A flash-style swap borrows and repays the quote: it traded the larger amount.
    assert trade(AERO, (1, 0, 0, 1003, 0, 1000), (0, 1)) == (None, 1003)
