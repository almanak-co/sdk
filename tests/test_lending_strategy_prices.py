"""Strategy-declared price series in the Aave V3, SparkLend, Morpho and Comet backtests."""

import importlib
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace

import pytest

from almanak.backtest import strategy_prices
from almanak.backtest.pricing import PoolPrice, PriceBinding
from almanak.caip import AssetType, ChainId
from almanak.models import Chain
from almanak.price_series import PricePoint, PriceSeries
from almanak.strategy_api import Memory
from almanak.triggers import IntervalEvent, IntervalSelection
from scripts.legacy_configuration.author import read_author
from scripts.legacy_configuration.backtest import read_backtest
from tests.legacy_references import harness_directory

HOUR = 3600
# Policy digests of the frozen references before strategy series existed.
REFERENCES = {
    "aave_v3": ("aave_v3_base_lending", "aa29879c953f"),
    "sparklend": ("sparklend_ethereum_lending", "74595c6a1956"),
    "morpho_blue": ("morpho_blue_base_lending", "4bb53facdd8a"),
    "compound_v3": ("compound_v3_base_lending", "b7d48fb01c87"),
}


def module(connector, name):
    return importlib.import_module(f"almanak.connectors.{connector}.economic_{name}")


def reference(connector):
    name, digest = REFERENCES[connector]
    directory = harness_directory(name)
    author = read_author(directory / "config.yaml")
    settings = read_backtest(directory / "backtest-economic.yaml", author)
    interval = IntervalSelection(configured=author.decision_trigger)
    config = module(connector, "parameters").parameters(author, settings, interval)
    return config, settings, digest


def sources(connector, config):
    parameters = module(connector, "parameters")
    if connector in ("aave_v3", "sparklend"):
        return parameters.allocations(config)
    return parameters.markets(config)


def series(chain, window=24):
    weth = AssetType(f"{chain.chain_id}/erc20:0x4200000000000000000000000000000000000006")
    return PriceBinding(
        purpose="strategy",
        chain=chain,
        asset=weth,
        window=window,
        source=PoolPrice(
            kind="pool_state",
            connector="uniswap_v3",
            pool="0xd0b53d9277642d899df5c87a3966a349a798f224",
            timeframe="1h",
            max_age_seconds=2 * HOUR,
            availability_lag_seconds=300,
        ),
    )


def declared(monkeypatch, settings, binding):
    monkeypatch.setattr(
        strategy_prices, "declared_prices", lambda strategy, parameters, economic=True: (binding,)
    )
    return strategy_prices.with_strategy_prices(settings, "strategy", None, ())


@pytest.fixture(params=sorted(REFERENCES))
def connector(request):
    return request.param


def test_a_policy_without_series_keeps_its_dump_and_dataset_identity(connector):
    config, settings, digest = reference(connector)
    parameters = module(connector, "parameters")
    dump = settings.data.model_dump(mode="json")
    assert "prices" not in dump and "vendors" not in dump
    assert parameters.identity(config, settings).startswith(digest)
    assert strategy_prices.strategy_requirements(settings) == ()


def test_declared_series_join_requirements_and_identity(connector, monkeypatch):
    config, settings, digest = reference(connector)
    parameters = module(connector, "parameters")
    plain = parameters.requirements(config, settings)
    chain = settings.history[0].chain
    priced = declared(monkeypatch, settings, series(chain))
    assert "vendors" not in priced.data.model_dump(mode="json")
    (requirement,) = strategy_prices.strategy_requirements(priced)
    assert parameters.requirements(config, priced) == plain + (requirement,)
    # The window's 23 earlier hours are acquired before the schedule starts.
    assert requirement.start <= priced.schedule.start - 23 * HOUR
    assert not parameters.identity(config, priced).startswith(digest)
    module(connector, "prepare").validate_collection(sources(connector, config), priced)


def test_a_series_outside_the_historical_chains_is_refused(connector, monkeypatch):
    config, settings, _ = reference(connector)
    configured = settings.history[0].chain
    other = "eip155:1" if str(configured.chain_id) == "eip155:8453" else "eip155:8453"
    chain = Chain(chain_id=ChainId(other), environment="mainnet")
    priced = declared(monkeypatch, settings, series(chain))
    with pytest.raises(ValueError, match="configured historical chain"):
        module(connector, "prepare").validate_collection(sources(connector, config), priced)


def test_decide_hands_the_book_series_at_the_decision_time(connector, monkeypatch):
    config, settings, _ = reference(connector)
    binding = series(settings.history[0].chain)
    time = int(settings.schedule.start)
    delivered = PriceSeries(
        binding=binding,
        cutoff=time,
        points=(PricePoint(source_time=time - time % HOUR, value=Decimal("2400.5")),),
        evidence=("digest",),
    )
    asked = []
    book = SimpleNamespace(prices=SimpleNamespace(at=lambda at: asked.append(at) or (delivered,)))
    model = module(connector, "model")
    contexts = []

    def request(strategy, context):
        contexts.append(context)
        raise LookupError("decision captured")

    monkeypatch.setattr(model, "EconomicDecisionRequest", request)
    host = SimpleNamespace(
        config=config, strategy="strategy", book=book, availability=lambda state, observed: ()
    )
    state = SimpleNamespace(time=time, memory=Memory(), portfolio=SimpleNamespace(positions=()))
    instant = datetime.fromtimestamp(time, UTC)
    trigger = IntervalEvent(started_at=instant, due_at=instant)
    with pytest.raises(LookupError, match="decision captured"):
        model.EconomicModel.decide(host, state, (), trigger)
    assert asked == [time]
    (context,) = contexts
    assert context.price(binding.asset).latest.value == Decimal("2400.5")
