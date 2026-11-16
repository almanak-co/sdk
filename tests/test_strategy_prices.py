"""Strategy price series in live/fork decisions and in backtest settings."""

import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak.backtest import pool_reads
from almanak.backtest import strategy_prices as backtest_prices
from almanak.backtest.pricing import (
    CoinGeckoPrice,
    CoinGeckoProvider,
    PoolPrice,
    PoolSwapCandles,
    PriceBinding,
    VendorBinding,
)
from almanak.caip import AssetType, ChainId
from almanak.gateway import strategy_prices as live
from almanak.gateway.ethereum import DataError
from almanak.models import Block, Chain, Model
from almanak.runtime import decision_cutoff
from tests.pool_price_chain import CBBTC, CHAIN, GENESIS, POOL, PRICE_POOL, USDC, WETH, FakeChain

HOUR = 3600
VENDOR = VendorBinding(
    name="market_data",
    provider=CoinGeckoProvider(api="pro", api_key_env="COINGECKO_API_KEY"),
)


def binding(asset=WETH, window=4, lag=60, kind="pool_state", connector="uniswap_v3"):
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
            availability_lag_seconds=lag,
        ),
    )


def single(asset=CBBTC):
    return PriceBinding(
        purpose="strategy",
        chain=CHAIN,
        asset=asset,
        source=CoinGeckoPrice(
            kind="coingecko",
            provider="market_data",
            coin_id="coinbase-wrapped-btc",
            quote="USD",
            interval="hourly",
            max_age_seconds=2 * HOUR,
            availability="retrospective_timestamp",
            availability_lag_seconds=60,
        ),
    )


@pytest.fixture
def chain(monkeypatch):
    resolved = []

    def resolve(reader, scope, block):
        resolved.append(block)
        return PRICE_POOL

    monkeypatch.setattr(pool_reads, "resolve_pool", resolve)
    made = FakeChain()
    made.resolved = resolved
    return made


@pytest.fixture
def source(tmp_path, chain):
    return live.StrategyPriceSource(SimpleNamespace(reader=chain), tmp_path / "prices.sqlite")


def test_a_live_series_reads_the_closes_available_at_the_cutoff_and_retains_them(source, chain):
    cutoff = GENESIS + 10 * HOUR + 30 * 60
    (series,) = source.series("deployment", (binding(),), cutoff)
    # 60 s lag: the 10:00 boundary close is the newest; the window holds four.
    assert [p.source_time for p in series.points] == [GENESIS + h * HOUR for h in (7, 8, 9, 10)]
    expected = PRICE_POOL.price((0, 1), (chain.price(chain.floor(GENESIS + 10 * HOUR)),))
    assert series.values[-1] == expected and series.quote == USDC and series.complete
    assert len(series.evidence) == 5  # the pool and one boundary state per point
    # A hold is retained like any other decision input.
    assert source.served("deployment") == (series,)
    assert len(chain.resolved) == 1
    (early,) = source.series("deployment", (binding(),), GENESIS + 10 * HOUR + 59)
    assert early.latest.source_time == GENESIS + 9 * HOUR


def test_later_decisions_read_only_boundaries_they_have_not_seen(source, chain):
    source.series("d", (binding(),), GENESIS + 10 * HOUR + 5 * 60)
    calls = chain.reads["eth_call"]
    assert calls == 4
    source.series("d", (binding(),), GENESIS + 10 * HOUR + 50 * 60)
    assert chain.reads["eth_call"] == calls  # same interval: nothing new
    headers = chain.reads["eth_getBlockByNumber"]
    (later,) = source.series("d", (binding(),), GENESIS + 11 * HOUR + 5 * 60)
    assert chain.reads["eth_call"] == calls + 1
    # The new boundary is searched from the retained one: the head and a few headers.
    assert chain.reads["eth_getBlockByNumber"] - headers <= 5
    assert later.latest.source_time == GENESIS + 11 * HOUR
    assert len(chain.resolved) == 1


def test_a_boundary_that_is_not_final_is_served_but_read_again(tmp_path, chain):
    # Head just before 20:00, finalized one hour behind: the 19:00 boundary's successor
    # can still be reorged out, so its point is not retained.
    chain.finality_lag = 1800
    made = live.StrategyPriceSource(SimpleNamespace(reader=chain), tmp_path / "f.sqlite")
    cutoff = GENESIS + 19 * HOUR + 59 * 60
    (series,) = made.series("d", (binding(),), cutoff)
    assert series.latest.source_time == GENESIS + 19 * HOUR and series.complete
    calls = chain.reads["eth_call"]
    made.series("d", (binding(),), cutoff)
    assert chain.reads["eth_call"] == calls + 1  # only the unsettled boundary again
    with made.connect() as db:
        retained = {at for (at,) in db.execute("SELECT time FROM points")}
    assert max(retained) == GENESIS + 18 * HOUR


def test_live_swap_candles_carry_the_candle_members(tmp_path, chain):
    swaps = [(chain.floor(GENESIS + 9 * HOUR) + 5, -(10**18), 2_000_000_000, chain.price(0))]
    swapped = FakeChain(swaps=swaps)
    made = live.StrategyPriceSource(SimpleNamespace(reader=swapped), tmp_path / "s.sqlite")
    (series,) = made.series("d", (binding(kind="pool_swaps"),), GENESIS + 10 * HOUR + 120)
    assert series.volumes == (0, 0, 0, Decimal(2000))
    assert series.highs[-1] >= series.values[-1] and series.lows[-1] <= series.opens[-1]
    assert series.points[-1].opened_at == GENESIS + 9 * HOUR


def test_a_pool_without_the_asset_or_a_chain_without_a_route_holds_the_decision(source):
    with pytest.raises(DataError, match="not one of the pool's tokens") as error:
        source.series("d", (binding(asset=CBBTC),), GENESIS + 10 * HOUR)
    assert error.value.kind == "pool_data"
    other = Chain(chain_id=ChainId("eip155:10"), environment="mainnet")
    elsewhere = binding().model_copy(
        update={"chain": other, "asset": AssetType("eip155:10/erc20:" + "0x" + "42" * 20)}
    )
    with pytest.raises(DataError, match="no provider for eip155:10") as error:
        source.series("d", (elsewhere,), GENESIS + 10 * HOUR)
    assert error.value.kind == "missing_provider"


def test_a_single_strategy_price_is_not_served_live(source):
    with pytest.raises(DataError, match="reads a pool series"):
        source.series("d", (single(),), GENESIS + 10 * HOUR)
    with pytest.raises(DataError, match="reads a pool series"):
        live.require_declared((single(),), None)
    prepared = SimpleNamespace(dependencies=SimpleNamespace(prices=(binding(), single())))
    assert live.strategy_bindings(prepared) == (binding(),)


def test_declared_pools_are_admitted_only_where_the_deployment_can_read_them(source):
    live.require_price_chains((binding(),), {CHAIN})
    live.require_price_chains((), set())
    with pytest.raises(ValueError, match="needs a provider for its chain"):
        live.require_price_chains((binding(),), set())
    optimism = Chain(chain_id=ChainId("eip155:10"), environment="mainnet")
    pancake = binding(connector="pancakeswap_v3").model_copy(
        update={"chain": optimism, "asset": AssetType("eip155:10/erc20:0x" + "42" * 20)}
    )
    with pytest.raises(ValueError, match="cannot be read"):
        live.require_price_chains((pancake,), {optimism})
    # At launch the gateway resolves each pool and requires the declared asset in it.
    live.resolve_price_pools(source, (binding(),))
    with pytest.raises(ValueError, match="cannot serve"):
        live.resolve_price_pools(source, (binding(asset=CBBTC),))


def test_the_gateway_serves_only_the_series_a_preparation_declared():
    declared = (binding(),)
    live.require_declared(declared, declared)
    live.require_declared((binding(),), None)  # an unprepared fork has no frozen declaration
    with pytest.raises(DataError, match="differ from the prepared strategy"):
        live.require_declared((single(),), declared)
    with pytest.raises(DataError, match="at most one price series per asset"):
        live.require_declared((binding(), binding(window=2)), None)


def test_the_decision_cutoff_is_the_trigger_or_the_oldest_observed_block():
    event = SimpleNamespace(started_at=datetime.fromtimestamp(10 * HOUR, UTC))
    block = Block(chain=CHAIN, number=7, hash="0x" + "ab" * 32, timestamp=9 * HOUR)

    class Pool(Model):
        block: Block

    class Snapshot(Model):
        pool: Pool

    assert decision_cutoff(event, ()) == 10 * HOUR
    assert decision_cutoff(event, (Snapshot(pool=Pool(block=block)),)) == 9 * HOUR


def test_the_decision_cutoff_reads_a_block_nested_deeper_through_effective_at():
    # An Aave V4 snapshot keeps its block at position.reserve.block.
    event = SimpleNamespace(started_at=datetime.fromtimestamp(10 * HOUR, UTC))

    class Lending(Model):
        stamp: int

        @property
        def effective_at(self):
            return datetime.fromtimestamp(self.stamp, UTC)

    assert decision_cutoff(event, (Lending(stamp=8 * HOUR),)) == 8 * HOUR


class Data(Model):
    prices: tuple[PriceBinding, ...] = ()


class Settings(Model):
    data: Data


class Unpriced(Model):
    data: Model


def test_backtest_settings_take_the_strategy_series_without_any_vendor(monkeypatch):
    declared = (binding(),)
    monkeypatch.setattr(
        backtest_prices, "declared_prices", lambda strategy, parameters, economic: declared
    )
    settings = backtest_prices.with_strategy_prices(Settings(data=Data()), "s", None, ())
    assert settings.data.prices == declared
    assert backtest_prices.strategy_bindings(settings) == declared
    with pytest.raises(ValueError, match="does not deliver strategy price series"):
        backtest_prices.with_strategy_prices(Unpriced(data=Model()), "s", None, ())
    authored = Settings(data=Data(prices=(binding(window=9),)))
    with pytest.raises(ValueError, match="differ from declared backtest bindings"):
        backtest_prices.with_strategy_prices(authored, "s", None, ())


def test_only_the_aave_v4_economic_backtest_takes_single_strategy_prices(monkeypatch):
    monkeypatch.setattr(
        backtest_prices, "declared_prices", lambda strategy, parameters, economic: (single(),)
    )
    with pytest.raises(ValueError, match="only Aave V4 economic backtests"):
        backtest_prices.with_strategy_prices(Settings(data=Data()), "s", None, (VENDOR,))


def test_a_strategy_without_series_leaves_backtest_settings_unchanged(monkeypatch):
    monkeypatch.setattr(
        backtest_prices, "declared_prices", lambda strategy, parameters, economic: ()
    )
    settings = Unpriced(data=Model())
    assert backtest_prices.with_strategy_prices(settings, "s", None, ()) is settings


FIXTURES = Path(__file__).parent / "fixtures" / "author" / "aave_v4_weth_supply"
REFERENCE = Path(__file__).parents[1] / "strategies" / "aave_v4_weth_supply"


def authored(tmp_path, change):
    from almanak.configuration.backtest_inputs import read_backtest_inputs
    from almanak.configuration.yaml import load

    raw = load(REFERENCE / "backtest-economic.yaml").value
    change(raw)
    # JSON is YAML, so the changed document reads through the authored path.
    path = tmp_path / "backtest.yaml"
    path.write_text(json.dumps(raw))
    return read_backtest_inputs(path)


def test_an_economic_backtest_configures_only_risk_and_valuation_prices(tmp_path):
    from almanak.configuration.yaml import ConfigurationError

    assert authored(tmp_path, lambda raw: None).inputs.mode == "economic"
    entry = json.loads(binding().model_dump_json())
    with pytest.raises(ConfigurationError, match="data_dependencies") as error:
        authored(tmp_path, lambda raw: raw["data"]["prices"].append(entry))
    assert error.value.field == "data.prices.3.purpose"

    def historical(raw):
        raw["mode"] = "historical_path"

    with pytest.raises(ConfigurationError, match="historical_path takes no data policy"):
        authored(tmp_path, historical)


def test_a_historical_gateway_admits_only_the_declared_pools():
    from almanak.backtest.gateway import ContractBinding, GatewayConfig

    config = GatewayConfig(
        providers=(), contracts=(ContractBinding(chain=CHAIN, contracts=("0x" + "11" * 20,)),)
    )
    plain = SimpleNamespace(data=SimpleNamespace(prices=()))
    assert backtest_prices.gateway_authority(plain, config) is config
    priced = SimpleNamespace(data=SimpleNamespace(prices=(binding(), binding(window=9))))
    granted = backtest_prices.gateway_authority(priced, config)
    (contracts,) = granted.contracts
    assert contracts.contracts == ("0x" + "11" * 20, POOL)
    assert [(p.connector, p.pool) for p in granted.price_pools] == [("uniswap_v3", POOL)]
    assert "price_pools" not in json.loads(config.model_dump_json())


def test_a_historical_run_without_series_reads_nothing():
    prices = backtest_prices.HistoricalSeries({}, historical_settings(()))
    assert prices.at(10 * HOUR) == ()
    with pytest.raises(ValueError, match="chain's historical gateway"):
        backtest_prices.HistoricalSeries({}, historical_settings((binding(),)))


def historical_settings(prices, start=10 * HOUR, end=14 * HOUR):
    return SimpleNamespace(
        schedule=SimpleNamespace(start=start, deadline=end),
        data=SimpleNamespace(prices=prices),
    )


def test_historical_path_settings_carry_series_only_when_the_strategy_declares_them(monkeypatch):
    from almanak.backtest.config import AaveBacktestConfig

    raw = (FIXTURES / "backtest.json").read_text()
    settings = AaveBacktestConfig.model_validate_json(raw)
    assert json.loads(settings.model_dump_json()) == json.loads(
        AaveBacktestConfig.model_validate_json(raw).model_dump_json()
    )
    assert "data" not in json.loads(settings.model_dump_json())
    assert backtest_prices.series_bindings(settings) == ()
    modes = []

    def declared(strategy, parameters, economic):
        modes.append(economic)
        return (binding(),)

    monkeypatch.setattr(backtest_prices, "declared_prices", declared)
    priced = backtest_prices.with_strategy_prices(settings, "s", None, ())
    assert modes == [False]
    assert priced.data.prices == (binding(),)
    assert backtest_prices.series_bindings(priced) == (binding(),)
    assert AaveBacktestConfig.model_validate_json(priced.model_dump_json()) == priced


def test_historical_path_dependencies_match_the_resolved_series(monkeypatch):
    from almanak import runtime
    from almanak.backtest import cli
    from almanak.backtest.dependencies import DataDependencies

    codec = SimpleNamespace(request_type=lambda kind: lambda **fields: fields)
    monkeypatch.setattr(cli, "connector_for", lambda config: SimpleNamespace(codec=codec))
    declared = DataDependencies(prices=(binding(),))
    monkeypatch.setattr(runtime, "strategy_worker", lambda request: declared.model_dump_json())
    settings = historical_settings((binding(),))
    cli.validate_historical_dependencies("s", None, settings)
    with pytest.raises(ValueError, match="differ from the resolved"):
        cli.validate_historical_dependencies("s", None, historical_settings(()))


class PriceGateway:
    def __init__(self, failure=None):
        self.failure = failure
        self.calls = []

    def prices(self, bindings, cutoff):
        self.calls.append((bindings, cutoff))
        if self.failure is not None:
            raise self.failure
        return ()


def price_runtime(monkeypatch, gateway, mode="running"):
    """A coordinator reduced to one decision; the strategy and its journal are stubbed."""
    import almanak.runtime as runtime_module

    runtime = object.__new__(runtime_module.Runtime)
    runtime.gateway = gateway
    runtime.clock = lambda: datetime.fromtimestamp(10 * HOUR, UTC)
    runtime.fault = lambda *args: None
    runtime.live_bindings = (binding(),)
    runtime.price_owner = runtime
    runtime.chain_availability = ("available",)
    # A lifecycle without repayment coverage, so the decision reaches the series.
    runtime.connector = SimpleNamespace(
        codec=SimpleNamespace(decision_input=lambda **fields: SimpleNamespace(**fields)),
        lifecycle=SimpleNamespace(),
    )
    decided, held = [], []
    monkeypatch.setattr(runtime_module, "validate_observed_ownership", lambda state, obs: None)
    monkeypatch.setattr(
        runtime_module, "strategy_decision", lambda strategy, context: decided.append(context)
    )
    monkeypatch.setattr(runtime, "record_decision", lambda *args: False, raising=False)
    monkeypatch.setattr(
        runtime, "hold_or_close", lambda state, obs, memory, event: held.append(memory) or False
    )
    schedule = SimpleNamespace(
        event=lambda now, teardown: SimpleNamespace(started_at=now, teardown=teardown)
    )
    state = SimpleNamespace(
        mode=mode, config="c", memory="kept", owned=(), schedule=schedule, strategy="s"
    )
    return runtime, state, decided, held


def test_an_unavailable_series_holds_the_decision_instead_of_failing_the_run(monkeypatch):
    gateway = PriceGateway(DataError("http", "eth_getLogs: HTTP 429"))
    runtime, state, decided, held = price_runtime(monkeypatch, gateway)
    assert runtime.apply_decision(state, ()) is False
    assert held == ["kept"] and decided == [] and len(gateway.calls) == 1


def test_a_gateway_failure_is_not_turned_into_a_hold(monkeypatch):
    gateway = PriceGateway(DataError("gateway_unavailable", "down"))
    runtime, state, decided, held = price_runtime(monkeypatch, gateway)
    with pytest.raises(DataError, match="down"):
        runtime.apply_decision(state, ())
    assert held == [] and decided == []


def test_teardown_never_reads_a_series(monkeypatch):
    gateway = PriceGateway(DataError("missing_provider", "unset"))
    runtime, state, decided, held = price_runtime(monkeypatch, gateway, mode="teardown")
    runtime.apply_decision(state, ())
    assert gateway.calls == [] and held == [] and len(decided) == 1
    assert not hasattr(decided[0], "prices")


def test_per_chain_copies_read_the_declared_series_once(monkeypatch):
    from copy import copy

    import almanak.backtest.strategy_prices as declared_module
    from almanak.runtime import Runtime

    calls = []
    monkeypatch.setattr(
        declared_module, "live_bindings", lambda strategy, config: calls.append(1) or (binding(),)
    )
    runtime = object.__new__(Runtime)
    runtime.live_bindings = None
    runtime.price_owner = runtime
    state = SimpleNamespace(strategy="s", config="c")
    for _ in range(3):
        assert copy(runtime).price_bindings(state) == (binding(),)
    assert calls == [1] and runtime.live_bindings == (binding(),)


def test_live_runs_read_only_pool_series(monkeypatch):
    from almanak.backtest.pricing import ProtocolPrice
    from almanak.strategy import StrategyDefinition

    oracle = PriceBinding(
        purpose="strategy",
        chain=CHAIN,
        asset=WETH,
        source=ProtocolPrice(kind="protocol_oracle", key=binding().key),
    )
    plain = StrategyDefinition(filename="s.py", source="def decide(ctx):\n    return None\n")
    # No hook: neither the worker nor the parameter codec is needed.
    assert backtest_prices.live_bindings(plain, object()) == ()
    hooked = plain.model_copy(update={"source": plain.source + "def data_dependencies(c): ...\n"})
    monkeypatch.setattr(
        "almanak.configuration.parameter_codec.hook_parameters", lambda config: config
    )
    monkeypatch.setattr(
        backtest_prices,
        "declared_prices",
        lambda strategy, parameters, economic: (single(), oracle, binding()),
    )
    assert backtest_prices.live_bindings(hooked, "config") == (binding(),)


def test_an_aave_v4_backtest_file_may_still_list_a_single_strategy_price(tmp_path):
    oracle = {
        "purpose": "strategy",
        "chain": {"chain_id": "eip155:1", "environment": "mainnet"},
        "asset": "eip155:1/erc20:0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
        "source": {"kind": "protocol_oracle", "key": {"connector": "aave_v4", "name": "x"}},
    }
    document = authored(tmp_path, lambda raw: raw["data"]["prices"].append(oracle))
    assert document.inputs.data["prices"][-1]["purpose"] == "strategy"


def test_aave_v4_economic_decisions_receive_single_strategy_prices_as_observations(
    tmp_path, monkeypatch
):
    from pydantic import TypeAdapter

    from almanak.backtest.dependencies import PriceObservation
    from almanak.backtest.economic_data import reserve_key
    from almanak.backtest.economic_state import EconomicDecisionInput
    from almanak.backtest.pricing import ProtocolPrice
    from almanak.price_series import PriceSeries, series_at
    from tests.aave_economic_support import prepared

    manifest, _, model = prepared(tmp_path, "ethereum-reference")
    try:
        settings = manifest.settings
        reserve = manifest.config.positions[0].reserve
        oracle = PriceBinding(
            purpose="strategy",
            chain=reserve.chain,
            asset=reserve.asset,
            source=ProtocolPrice(kind="protocol_oracle", key=reserve_key(reserve, "price")),
        )
        monkeypatch.setattr(
            backtest_prices, "declared_prices", lambda strategy, parameters, economic: (oracle,)
        )
        # Listed in the backtest file as on main, or declared by the hook alone.
        data = settings.data.model_copy(update={"prices": (*settings.data.prices, oracle)})
        listed = settings.model_copy(update={"data": data})
        for given in (listed, settings):
            resolved = backtest_prices.with_strategy_prices(given, "s", None, ())
            assert backtest_prices.strategy_bindings(resolved) == (oracle,)
        model.book.data = resolved.data
        (price,) = model.book.strategy_prices(int(settings.schedule.start))
        assert isinstance(price, PriceObservation) and price.binding == oracle and price.value > 0
        # Both shapes travel to the strategy worker and come back as themselves.
        series = series_at(binding(), (), 10 * HOUR)
        adapter = TypeAdapter(EconomicDecisionInput.model_fields["prices"].annotation)
        back = adapter.validate_json(adapter.dump_json((price, series)))
        assert type(back[0]) is PriceObservation and type(back[1]) is PriceSeries
        assert back == (price, series)
    finally:
        model.book.close()


def test_a_decision_waits_a_bounded_time_for_its_price_request():
    from almanak.gateway.client import GatewayClient

    budget = live.request_budget((binding(), binding()))
    assert budget == 2 * live.DECISION_READ_SECONDS + live.SERVING_SECONDS
    sent = {}
    client = object.__new__(GatewayClient)
    client.scoped = lambda method, **fields: sent.update(fields) or []
    assert client.prices((binding(),), 10 * HOUR) == ()
    assert sent["timeout"] == live.request_budget((binding(),))


def test_the_gateway_retains_each_window_before_serving_and_survives_a_failed_read(capsys):
    from types import SimpleNamespace

    read = []

    class Source:
        def reader(self, chain):
            return SimpleNamespace(pin_block=lambda tag: SimpleNamespace(timestamp=12 * HOUR))

        def read(self, binding, cutoff):
            read.append(cutoff)
            if len(read) == 2:
                raise DataError("transport", "provider unavailable")

    live.warm_price_series(Source(), (binding(), binding()))
    assert read == [12 * HOUR, 12 * HOUR]
    assert "price_window_not_retained" in capsys.readouterr().err


def test_a_point_is_retrieved_at_its_read_head_not_the_wall_clock(source, chain, monkeypatch):
    # `almanak test` advances fork time past the wall clock: the read head is later.
    monkeypatch.setattr(live.time, "time", lambda: GENESIS)
    cutoff = GENESIS + 10 * HOUR + 30 * 60
    (series,) = source.series("deployment", (binding(),), cutoff)
    assert series.complete
    head = chain.timestamps[-1]
    times = [p.source_time for p in series.points]
    rows = source.points(binding(), times)
    assert {row.retrieved_at for row in rows.values()} == {head}
    assert all(row.available_at <= row.retrieved_at for row in rows.values())


def test_a_decision_waits_for_the_warm_up_of_its_window_and_reuses_it(source, chain):
    import threading

    cutoff = GENESIS + 10 * HOUR + 30 * 60
    window = source.window(binding())
    window.acquire()
    served = []
    decision = threading.Thread(
        target=lambda: served.append(source.series("d", (binding(),), cutoff))
    )
    decision.start()
    decision.join(0.2)
    assert decision.is_alive()  # serialized behind the warm-up, never refused
    source.read_window(binding(), cutoff)
    calls = chain.reads["eth_call"]
    window.release()
    decision.join(5)
    assert served and served[0][0].complete
    assert chain.reads["eth_call"] == calls  # the warm-up's window, not a second read


def test_a_window_still_warming_past_the_decision_wait_holds_only_that_decision(
    source, monkeypatch
):
    monkeypatch.setattr(live, "DECISION_READ_SECONDS", 0.01)
    window = source.window(binding())
    window.acquire()
    try:
        with pytest.raises(DataError) as error:
            source.series("d", (binding(),), GENESIS + 10 * HOUR)
    finally:
        window.release()
    assert error.value.kind == "price_window_pending"
    from almanak.runtime import GATEWAY_FAILURES

    assert error.value.kind not in GATEWAY_FAILURES


@pytest.mark.parametrize(
    ("method", "kind"), [("prices", "timeout"), ("observe", "gateway_unavailable")]
)
def test_only_an_overrun_price_read_holds_the_decision(tmp_path, method, kind):
    import httpx

    from almanak.gateway.client import GatewayClient
    from almanak.runtime import GATEWAY_FAILURES

    token = tmp_path / "token"
    token.write_text("a" * 64)
    token.chmod(0o600)
    client = GatewayClient("http://127.0.0.1:9", token)

    def slow(request):
        raise httpx.ReadTimeout("slow", request=request)

    client.http = httpx.Client(transport=httpx.MockTransport(slow))
    with pytest.raises(DataError) as error:
        client.request(method)
    assert error.value.kind == kind
    assert (kind in GATEWAY_FAILURES) == (method != "prices")


def test_a_teardown_decision_receives_no_series_in_any_run_mode(source):
    from almanak.strategy_api import DecisionContext, Memory
    from almanak.triggers import IntervalEvent, TeardownEvent

    (series,) = source.series("d", (binding(),), GENESIS + 10 * HOUR + 30 * 60)
    at = datetime.fromtimestamp(GENESIS + 10 * HOUR + 30 * 60, UTC)

    class Config(Model):
        name: str = "c"

    class Input(DecisionContext[Config, Config, Config]):
        pass

    fields = {"config": Config(), "memory": Memory(), "owned": (), "observations": ()}
    teardown = Input(**fields, prices=(series,), trigger=TeardownEvent(started_at=at))
    assert teardown.prices == ()
    # The worker rebuilds the context from JSON; the rule holds there too.
    running = Input(**fields, prices=(series,), trigger=IntervalEvent(started_at=at, due_at=at))
    assert running.prices == (series,)
    rebuilt = json.loads(running.model_dump_json())
    rebuilt["trigger"] = json.loads(TeardownEvent(started_at=at).model_dump_json())
    assert Input.model_validate_json(json.dumps(rebuilt)).prices == ()


def test_a_series_on_an_unreadable_chain_publishes_no_preparation(tmp_path, monkeypatch):
    from almanak import supervisor_preparation

    def refuse(record):
        raise ValueError("strategy price series on a chain the deployment cannot read")

    monkeypatch.setattr(supervisor_preparation, "prepare", lambda *args: object())
    monkeypatch.setattr(supervisor_preparation, "require_price_access", refuse)
    path = tmp_path / "preparation.json"
    supervisor = SimpleNamespace(root=tmp_path)
    with pytest.raises(ValueError, match="cannot read"):
        supervisor_preparation.production_bundle(supervisor, None, None, None, path)
    # Nothing is retained, so corrected inputs prepare afresh.
    assert not path.exists()
