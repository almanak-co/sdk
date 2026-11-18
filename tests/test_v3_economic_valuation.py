"""V3 runs are valued in their funding asset from the run's own pool samples."""

from bisect import bisect_right
from decimal import Decimal
from types import SimpleNamespace

import pytest

from almanak.backtest.dataset import InputUnavailable
from almanak.caip import AssetType, ChainId
from almanak.connectors import v3_economic_reporting as reporting
from almanak.connectors import v3_economic_valuation as layer
from almanak.connectors.aerodrome_slipstream import economic_reporting as slipstream
from almanak.connectors.band_fee_growth import Bounds
from almanak.connectors.concentrated_math import amounts, liquidity_for, sqrt_at_tick
from almanak.connectors.identifiers import ConnectorId
from almanak.connectors.uniswap_v3.economic_data import key
from almanak.connectors.uniswap_v3.economic_state import EconomicPosition
from almanak.connectors.uniswap_v3.models import Target
from almanak.connectors.uniswap_v3.positions import Ownership
from almanak.connectors.v3_fee_growth import accrued
from almanak.connectors.v3_pool_domain import TickRange
from almanak.connectors.v3_pool_positions import SimulatedPosition
from almanak.effects import (
    AssetDelta,
    Balance,
    Effects,
    Portfolio,
    Position,
    PositionChange,
    PositionKey,
    WalletAsset,
)
from almanak.models import AssetAmount, AssetMetadata, Chain

USDC = AssetType("eip155:1/erc20:0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48")
WETH = AssetType("eip155:1/erc20:0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2")
ETH = AssetType("eip155:1/slip44:60")
MAINNET = Chain(chain_id=ChainId("eip155:1"), environment="mainnet")
START, SECONDS = 1_000_000, 60
RANGE = TickRange(lower=197500, upper=198500)
# The pool tick at each sample: inside the range, then below it, then inside again.
TICKS = (198000, 198100, 197400, 197300, 197900)
GROWTH = tuple((index * 2**100, index * 2**130) for index in range(len(TICKS)))


def target(funding=USDC, allocation="position"):
    return Target.model_validate(
        {
            "allocation": allocation,
            "account": "fund",
            "fund": {"chain": MAINNET, "address": "0x" + "91" * 20},
            "pool": {
                "kind": "uniswap_v3",
                "chain": MAINNET,
                "factory": "0x" + "1f" * 20,
                "pool": "0x" + "88" * 20,
                "currency0": USDC,
                "currency1": WETH,
                "fee": 500,
                "tick_spacing": 10,
            },
            "contracts": {
                "router": "0x" + "68" * 20,
                "position_manager": "0x" + "c3" * 20,
                "quoter": "0x" + "61" * 20,
            },
            "range": None,
            "band": {"lower": 196000, "upper": 200000, "width": 1000},
            "funding_asset": funding,
        },
        strict=False,
    )


TARGET = target()


def pool(tick):
    return SimpleNamespace(
        sqrt_price_x96=sqrt_at_tick(tick),
        tick=tick,
        currency0=SimpleNamespace(identifier=USDC, decimals=6),
        currency1=SimpleNamespace(identifier=WETH, decimals=18),
    )


class Book:
    def __init__(self, selected=(TARGET,)):
        times = tuple(START + index * SECONDS for index in range(len(TICKS)))
        rows = tuple(
            SimpleNamespace(
                payload=SimpleNamespace(pool=pool(tick), baseline_digest="ab" * 32), growth=growth
            )
            for tick, growth in zip(TICKS, GROWTH, strict=True)
        )
        self.timelines = {item: SimpleNamespace(times=times, rows=rows) for item in selected}

    def at(self, selected, time):
        timeline = self.timelines[selected]
        if not timeline.times[0] <= time <= timeline.times[-1] + SECONDS:
            raise InputUnavailable(key(selected), time, "requested time is outside coverage")
        return timeline.rows[bisect_right(timeline.times, time) - 1]


def model(selected=(TARGET,), symbol="USDC"):
    names = {item.allocation: item for item in selected}
    return SimpleNamespace(
        book=Book(selected),
        config=SimpleNamespace(
            allocations=[SimpleNamespace(name=name) for name in names],
            target=names.__getitem__,
            asset_metadata={
                USDC: AssetMetadata(decimals=6, symbol=symbol),
                WETH: AssetMetadata(decimals=18),
                ETH: AssetMetadata(decimals=18),
            },
        ),
        settings=SimpleNamespace(data=SimpleNamespace(sample_seconds=SECONDS)),
        band_growth=lambda record, selected, chosen: Bounds(
            lower=record.growth, upper=record.growth
        ),
        data=SimpleNamespace(key=key),
    )


def wallet(asset):
    return WalletAsset(account="fund", chain=MAINNET, asset=asset)


def held(position=None):
    owned = Ownership(
        target=TARGET,
        inventory0=AssetAmount(asset=USDC, amount=0),
        inventory1=AssetAmount(asset=WETH, amount=0),
        position=position,
    )
    return Position(
        key=PositionKey(
            connector=ConnectorId("uniswap_v3"), chain=MAINNET, account="fund", reference="main"
        ),
        payload=EconomicPosition(target=TARGET, owned=owned),
    )


def simulated(liquidity, growth):
    return SimulatedPosition(
        identity="run:1",
        liquidity=liquidity,
        owed0=0,
        owed1=0,
        fee_growth=growth,
        range=RANGE,
        bound_growth=growth,
        bound_owed=(0, 0),
    )


def completed(at, kind, deltas, before, after, *, fees=(0, 0), position=None):
    """One successful step at `at`, executed against the sample in force then."""
    index = (at - START) // SECONDS
    result = SimpleNamespace(
        success=True,
        steps=(kind,),
        before=SimpleNamespace(pool=pool(TICKS[index])),
        intent=SimpleNamespace(target=TARGET),
        after=SimpleNamespace(position=position),
        delta0=deltas[0],
        delta1=deltas[1],
        fees0=fees[0],
        fees1=fees[1],
    )
    effects = Effects(
        assets=tuple(
            AssetDelta(key=wallet(asset), amount=delta)
            for asset, delta in zip((USDC, WETH), deltas, strict=True)
        ),
        positions=(PositionChange(before=before, after=after),),
    )
    return SimpleNamespace(
        pending=SimpleNamespace(eligible_at=at),
        transition=SimpleNamespace(result=result, effects=effects),
    )


def lifecycle(*, close=True):
    """Swap half the capital, open a position, then close it after the price left the range."""
    edges = sqrt_at_tick(RANGE.lower), sqrt_at_tick(RANGE.upper)
    entry = pool(TICKS[0])
    bought = 19_990_000_000_000_000
    liquidity = liquidity_for(50_000_000, bought, entry.sqrt_price_x96, *edges)
    deposits = amounts(liquidity, entry.sqrt_price_x96, *edges, up=True)
    opened = simulated(liquidity, GROWTH[0])
    steps = [
        completed(START + 5, "swap", (-50_000_000, bought), held(), held()),
        completed(
            START + 10,
            "mint",
            tuple(-value for value in deposits),
            held(),
            held(opened),
            position=opened,
        ),
    ]
    balances = [100_000_000 - 50_000_000 - deposits[0], bought - deposits[1]]
    final = held(opened)
    if close:
        exit_pool = pool(TICKS[3])
        principal = amounts(liquidity, exit_pool.sqrt_price_x96, *edges)
        fees = tuple(
            accrued(liquidity, before, after)
            for before, after in zip(GROWTH[0], GROWTH[3], strict=True)
        )
        returned = tuple(p + f for p, f in zip(principal, fees, strict=True))
        steps.append(completed(START + 185, "close", returned, held(opened), held(), fees=fees))
        balances = [value + delta for value, delta in zip(balances, returned, strict=True)]
        final = held()
    portfolio = Portfolio(
        balances=(
            Balance(key=wallet(USDC), amount=balances[0]),
            Balance(key=wallet(WETH), amount=balances[1]),
            Balance(key=wallet(ETH), amount=10**16),
        ),
        positions=(final,),
    )
    last = START + 4 * SECONDS
    return SimpleNamespace(
        schedule=SimpleNamespace(start=START, end=last),
        time=last,
        completed=tuple(steps),
        events=(),
        portfolio=portfolio,
    )


def test_a_closed_run_is_valued_in_its_funding_asset_and_explains_its_result():
    state, driver = lifecycle(), model()
    report = layer.valuation_layer(state, driver)
    assert (report["valuation"], report["valuation_quote"]) == ("pool_price", "USDC")
    assert report["valuation_asset"] == str(USDC) and report["valuation_excludes"] == [str(ETH)]
    curve = report["equity_curve"]
    # The funding capital alone opens the curve; gas is never priced into it.
    assert curve[0] == {"time": START, "value": "100.000000", "complete": True}
    assert [point["time"] for point in curve] == sorted({p["time"] for p in curve})
    assert {START + 5, START + 10, START + 185} <= {point["time"] for point in curve}
    assert all(point["complete"] for point in curve)
    performance = report["performance"]
    assert performance["initial_value"] == "100.000000"
    assert performance["final_value"] == curve[-1]["value"]
    assert report["net_quote_return"]["value"] == performance["net_value"]
    assert report["metrics"]["total_return_pct"] == pytest.approx(
        float(performance["net_value"]), abs=1e-6
    )
    # Fees less swap costs plus the price effect is the whole net result.
    parts = (
        Decimal(performance["fees_value"])
        - Decimal(performance["swap_cost_value"])
        + Decimal(performance["price_effect_value"])
    )
    assert abs(parts - Decimal(performance["net_value"])) <= Decimal("0.000002")
    assert Decimal(performance["fees_value"]) > 0
    assert (performance["swaps"], performance["positions_opened"]) == (1, 1)
    assert Decimal(performance["swap_volume_value"]) == Decimal("50.000000")


def test_a_position_row_states_its_range_fees_and_time_in_range():
    state, driver = lifecycle(), model()
    performance = layer.valuation_layer(state, driver)["performance"]
    (row,) = performance["positions"]
    assert (row["opened_at"], row["closed_at"]) == (START + 10, START + 185)
    assert row["range"] == {"lower": RANGE.lower, "upper": RANGE.upper}
    # USDC per WETH: a higher tick is a lower quote price, so the edges swap.
    low, high = (Decimal(price) for price in row["price_range"])
    assert Decimal(2300) < low < high < Decimal(2700)
    # The pool tick fell under the range, which is a quote price above it.
    assert row["price_at_close"] == "above_range"
    # In range from the mint until the third sample left the range at START + 120.
    assert (row["held_seconds"], row["in_range_seconds"]) == (175, 110)
    assert [fee["asset"] for fee in row["fees"]] == [str(USDC), str(WETH)]
    assert all(int(fee["amount"]) > 0 for fee in row["fees"])
    assert Decimal(row["fees_value"]) == Decimal(performance["fees_value"])
    assert performance["time_in_market_seconds"] == 175
    assert performance["time_in_range_seconds"] == 110
    assert performance["window_seconds"] == 4 * SECONDS
    (benchmark,) = performance["benchmarks"]
    assert benchmark["asset"] == str(WETH)
    assert benchmark["even_split_return_pct"] == pytest.approx(benchmark["hold_return_pct"] / 2)


def test_a_position_still_open_at_the_end_is_marked_at_the_final_sample():
    state, driver = lifecycle(close=False), model()
    report = layer.valuation_layer(state, driver)
    (row,) = report["performance"]["positions"]
    assert row["closed_at"] is None and row["price_at_close"] is None
    assert row["held_seconds"] == 4 * SECONDS - 10
    assert Decimal(row["withdrawn_value"]) > 0 and Decimal(row["fees_value"]) > 0
    assert all(int(amount["amount"]) == 0 for amount in row["withdrawn"])
    assert report["equity_curve"][-1]["complete"]


def test_an_unmeasurable_open_position_keeps_its_row_without_a_closing_value():
    state, driver = lifecycle(close=False), model()

    def unmeasured(record, selected, chosen):
        raise ValueError("banded fee growth requires a retained prepared sample")

    driver.band_growth = unmeasured
    report = layer.valuation_layer(state, driver)
    (row,) = report["performance"]["positions"]
    assert row["withdrawn_value"] is None
    unvalued = [point for point in report["equity_curve"] if not point["complete"]]
    assert unvalued and unvalued[-1]["unvalued_assets"] == [str(USDC)]


def test_allocations_with_different_funding_assets_stay_in_asset_units():
    selected = (TARGET, target(WETH, "second"))
    assert layer.funding_asset(model(selected).config) is None
    assert layer.valuation_layer(lifecycle(), model(selected)) == {}
    assert "without a common-currency valuation" in reporting.valuation_assumption({})
    assert "valued in the funding asset" in reporting.valuation_assumption(
        {"valuation": "pool_price"}
    )


def test_prices_come_only_from_a_prepared_sample_of_one_pool():
    prices = layer.PoolPrices(model(symbol=None), USDC)
    assert prices.quote == str(USDC)
    assert prices.price(USDC, START) == Decimal(1)
    assert Decimal(2300) < prices.price(WETH, START) < Decimal(2700)
    assert prices.price(WETH, START - 1) is None
    assert prices.price(ETH, START) is None and prices.decimals(ETH) is None
    assert prices.decimals(WETH) == 18
    # The same pair in two different pools has no single declared price.
    other = TARGET.model_copy(
        update={
            "allocation": "second",
            "pool": TARGET.pool.model_copy(update={"pool": "0x" + "77" * 20}),
        }
    )
    assert layer.PoolPrices(model((TARGET, other)), USDC).price(WETH, START) is None


def test_pool_values_use_the_pool_price_in_either_funding_asset():
    sample = pool(198000)
    in_usdc = layer.pair_value(sample, USDC, (1_000_000, 10**18))
    in_weth = layer.pair_value(sample, WETH, (1_000_000, 10**18))
    rate = layer.decimal(layer.pool_rate(sample, USDC))
    assert in_usdc == pytest.approx(1 + rate)
    assert in_weth == pytest.approx(1 + 1 / rate)
    assert layer.decimal(layer.pool_rate(sample, WETH)) * rate == pytest.approx(Decimal(1))


def test_a_range_a_sample_cannot_measure_is_an_unavailable_input():
    driver = model()

    def unmeasured(record, selected, chosen):
        raise ValueError("fixed historical LP growth requires both initialized boundary ticks")

    driver.band_growth = unmeasured
    payload = held(simulated(10**15, GROWTH[0])).payload
    accrual = layer.LiquidityAccrual(driver)
    assert accrual.asset(payload) == USDC
    with pytest.raises(InputUnavailable, match="initialized boundary ticks"):
        accrual.accrue(payload, START)
    with pytest.raises(InputUnavailable, match="outside coverage"):
        accrual.accrue(payload, START - 1)
    assert accrual.accrue(held().payload, START) == ()

    def unbound(selected, time):
        raise ValueError("requested V3 pool is outside prepared source bindings")

    # A pool the book cannot look up leaves the position unvalued the same way.
    driver.book.at = unbound
    with pytest.raises(InputUnavailable, match="outside prepared source bindings"):
        accrual.accrue(payload, START)


def test_marks_keep_every_action_and_a_bounded_even_grid(monkeypatch):
    monkeypatch.setattr(layer, "MARKS", 3)
    state = SimpleNamespace(
        schedule=SimpleNamespace(start=0),
        time=600,
        completed=(
            SimpleNamespace(pending=SimpleNamespace(eligible_at=7)),
            SimpleNamespace(pending=SimpleNamespace(eligible_at=900)),
        ),
    )
    assert layer.mark_times(state, 60) == (0, 7, 240, 480, 600)


def test_time_in_market_counts_overlapping_positions_once():
    assert layer.merged_seconds([(0, 10), (5, 20), (30, 40), (32, 35)]) == 30
    assert layer.merged_seconds([]) == 0


def test_the_report_names_its_continuity_proof_without_embedding_it():
    baseline = SimpleNamespace(
        first=SimpleNamespace(timestamp=START),
        events=(1, 2, 3),
        inputs=("a", "b"),
        model_dump=lambda mode, exclude: {"first": 1, "last": 9, "kept": sorted(exclude)},
    )
    book = Book()
    book.baselines = {TARGET: baseline}
    assert reporting.continuity_summary(book, TARGET) == {
        "first": 1,
        "last": 9,
        "kept": ["events", "inputs"],
        "liquidity_events": 3,
        "inputs": 2,
        "evidence_sha256": "ab" * 32,
    }


def shared_report(state, model, *, position_report, evidence, assumptions):
    return {"assumptions": assumptions, "boundary_evidence": evidence}


def test_a_standalone_report_is_valued_and_a_failed_valuation_never_withholds_it(monkeypatch):
    monkeypatch.setattr(reporting.shared, "report", shared_report)
    driver = model()
    driver.book.baselines = {}
    valued = reporting.valued_report(lifecycle(), driver)
    assert valued["valuation"] == "pool_price" and valued["boundary_evidence"] == []
    assert "valued in the funding asset" in valued["assumptions"][-1]
    assert any("banded LP fees" in line for line in valued["assumptions"])
    # A composed run reports each connector over a slice of shared cash, without equity.
    component = reporting.report(lifecycle(), driver)
    assert "valuation" not in component
    assert "without a common-currency valuation" in component["assumptions"][-1]

    def unreplayable(state, model):
        raise ValueError("retained effects do not replay")

    monkeypatch.setattr(reporting, "valuation_layer", unreplayable)
    assert reporting.valued_report(lifecycle(), driver) == component
    family = slipstream.valued_report(lifecycle(), driver)
    assert family["assumptions"][-len(slipstream.ASSUMPTIONS) :] == list(slipstream.ASSUMPTIONS)
    assert slipstream.report(lifecycle(), driver) == family
