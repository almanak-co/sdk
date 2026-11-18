"""Declared strategy series in the Uniswap V4, Curve, Aerodrome, GMX and composed backtests."""

from types import SimpleNamespace
from unittest.mock import ANY

import pytest

from almanak.backtest import strategy_prices as backtest_prices
from almanak.backtest.dataset import file_digest
from almanak.backtest.evm import EvmHistory
from almanak.backtest.prepared import DatasetWriter
from almanak.backtest.pricing import PoolPrice, PriceBinding
from almanak.backtest.strategy_prices import StrategyPriceData, strategy_requirements
from almanak.connectors.aerodrome import economic_book as aerodrome_book
from almanak.connectors.aerodrome import economic_config as aerodrome_config
from almanak.connectors.composed import economic_config as composed_config
from almanak.connectors.composed import economic_model as composed_model
from almanak.connectors.composed import economic_prices
from almanak.connectors.curve import economic_book as curve_book
from almanak.connectors.curve import economic_config as curve_config
from almanak.connectors.curve import economic_model as curve_model
from almanak.connectors.dex_economic_prepare import collect_strategy_prices
from almanak.connectors.gmx_v2 import economic_book as gmx_book
from almanak.connectors.gmx_v2 import economic_config as gmx_config
from almanak.connectors.identifiers import ConnectorId
from almanak.connectors.uniswap_v4 import economic_book as v4_book
from almanak.connectors.uniswap_v4 import economic_model as v4_model
from almanak.models import Model
from tests.pool_price_chain import CHAIN, GENESIS, POOL, WETH, FakeChain

HOUR = 3600
START, END = GENESIS + 10 * HOUR + 1800, GENESIS + 12 * HOUR
DIGEST = "d" * 64


def binding(window=4):
    return PriceBinding(
        purpose="strategy",
        chain=CHAIN,
        asset=WETH,
        window=window,
        source=PoolPrice(
            kind="pool_state",
            connector="uniswap_v3",
            pool=POOL,
            timeframe="1h",
            max_age_seconds=2 * HOUR,
            availability_lag_seconds=300,
        ),
    )


def settings(prices=()):
    return SimpleNamespace(
        schedule=SimpleNamespace(start=START, deadline=END),
        data=SimpleNamespace(prices=prices),
        history=(SimpleNamespace(chain=CHAIN),),
    )


POLICIES = {
    "curve": (
        curve_config.DataPolicy,
        {
            "source": "curve_archive",
            "sample_seconds": 60,
            "max_age_seconds": 120,
            "max_samples": 10,
            "max_parameter_events": 10,
            "max_parameter_log_requests": 10,
        },
    ),
    "aerodrome": (
        aerodrome_config.DataPolicy,
        {
            "source": "aerodrome_archive",
            "sample_seconds": 60,
            "max_age_seconds": 120,
            "max_samples": 10,
        },
    ),
}


@pytest.mark.parametrize("name", sorted(POLICIES))
def test_a_data_policy_takes_declared_series_and_keeps_its_identity_without_them(name, monkeypatch):
    policy_type, fields = POLICIES[name]
    plain = policy_type.model_validate(fields)
    assert set(plain.model_dump(mode="json")) == set(fields)  # no new keys without series

    class Settings(Model):
        data: policy_type

    declared = (binding(window=24),)
    monkeypatch.setattr(
        backtest_prices, "declared_prices", lambda strategy, parameters, economic=True: declared
    )
    folded = backtest_prices.with_strategy_prices(Settings(data=plain), "s", None, ())
    assert folded.data.prices == declared
    assert {k: v for k, v in folded.data.model_dump().items() if k in fields} == plain.model_dump()


def test_the_gmx_policy_carries_series_like_every_exchange_policy():
    assert issubclass(gmx_config.DataPolicy, StrategyPriceData)


def test_the_composed_policy_is_absent_until_the_strategy_declares_a_series(monkeypatch):
    class Composed(Model):
        models: dict[str, int]
        data: StrategyPriceData = composed_config.EconomicConfig.model_fields["data"]

    plain = Composed(models={"a": 1})
    assert plain.model_dump(mode="json") == {"models": {"a": 1}}
    declared = (binding(window=24),)
    monkeypatch.setattr(
        backtest_prices, "declared_prices", lambda strategy, parameters, economic=True: declared
    )
    folded = backtest_prices.with_strategy_prices(plain, "s", None, ())
    assert folded.data.prices == declared
    assert folded.models == plain.models


def test_a_component_policy_cannot_carry_the_strategy_series():
    data = aerodrome_config.DataPolicy.model_validate(
        POLICIES["aerodrome"][1] | {"prices": [binding()]}
    )
    policy = composed_config.AerodromePolicy.model_construct(data=data)
    composed = composed_config.EconomicConfig.model_construct(
        models={ConnectorId("aerodrome"): policy}
    )
    with pytest.raises(ValueError, match="belong to the composition, not a component"):
        composed.policies()


class Reader:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


BOOKS = {
    "uniswap_v4": (v4_book, v4_book.PoolBook, "identity"),
    "curve": (curve_book, curve_book.PoolBook, None),
    "aerodrome": (aerodrome_book, aerodrome_book.PoolBook, None),
    "gmx_v2": (gmx_book, gmx_book.MarketBook, None),
}


def open_book(name, monkeypatch, prices, requirements):
    module, book_type, identity = BOOKS[name]
    preparation = SimpleNamespace(
        model=ANY, acquisition_revision=ANY, policy_digest=ANY, requirements=requirements
    )
    manifest = SimpleNamespace(preparation=preparation)
    monkeypatch.setattr(module, "load_dataset", lambda directory, digest: (manifest, Reader()))
    monkeypatch.setattr(module, "targets", lambda config: ())
    if identity is None:
        monkeypatch.setattr(book_type, "identity", staticmethod(lambda config, s: DIGEST))
    else:
        monkeypatch.setattr(module, identity, lambda config, s: DIGEST)
    if name == "curve":
        monkeypatch.setattr(module.economic_data, "model_identity", lambda config: (ANY, ANY))
    if name == "gmx_v2":
        monkeypatch.setattr(module.data, "revisions", lambda config: (ANY, ANY))
    monkeypatch.setattr(module, "StrategySeries", lambda reader, manifest, s: ("series", s))
    return book_type(None, None, settings(prices))


@pytest.mark.parametrize("name", sorted(BOOKS))
def test_a_book_requires_the_declared_series_and_slices_it(name, monkeypatch):
    declared = (binding(window=6),)
    required = strategy_requirements(settings(declared))
    assert required[0].start < START  # the warm-up precedes the first decision
    book = open_book(name, monkeypatch, declared, required)
    assert book.prices[0] == "series"
    with pytest.raises(ValueError, match="requirements differ"):
        open_book(name, monkeypatch, declared, ())
    assert open_book(name, monkeypatch, (), ()).prices[0] == "series"


class Prices:
    def __init__(self, value):
        self.value = value
        self.times = []

    def at(self, time):
        self.times.append(time)
        return self.value


def capture_decide(monkeypatch, module, context_name):
    seen = {}
    monkeypatch.setattr(module, context_name, lambda **fields: fields, raising=False)
    monkeypatch.setattr(module, "EconomicDecisionRequest", lambda **fields: fields)
    monkeypatch.setattr(module, "strategy_worker", lambda request: seen.setdefault("r", request))
    return seen


def test_uniswap_v4_decisions_receive_the_series_at_their_time(monkeypatch):
    seen = capture_decide(monkeypatch, v4_model, "DecisionInput")
    monkeypatch.setattr(v4_model, "Decision", SimpleNamespace(model_validate_json=lambda v: v))
    model = object.__new__(v4_model.EconomicModel)
    model.config, model.strategy = "config", "strategy"
    model.book = SimpleNamespace(prices=Prices(("weth",)))
    state = SimpleNamespace(time=START, memory="m", portfolio=SimpleNamespace(positions=()))
    model.decide(state, (), "trigger")
    assert seen["r"]["context"]["prices"] == ("weth",)
    assert model.book.prices.times == [START]


def test_exchange_family_decisions_receive_the_series_at_their_time(monkeypatch):
    from almanak.connectors import dex_economic_model as family

    seen = capture_decide(monkeypatch, family, "unused")
    model = object.__new__(curve_model.EconomicModel)
    model.config, model.strategy = "config", "strategy"
    model.state_types = SimpleNamespace(DecisionInput=lambda **fields: fields)
    model.decision_type = SimpleNamespace(model_validate_json=lambda v: v)
    model.book = SimpleNamespace(prices=Prices(("weth",)))
    state = SimpleNamespace(time=START, memory="m", portfolio=SimpleNamespace(positions=()))
    model.decide(state, (), "trigger")
    assert seen["r"]["context"]["prices"] == ("weth",)


def test_a_composed_decision_receives_the_series_once_for_the_composition(monkeypatch):
    seen = capture_decide(monkeypatch, composed_model, "DecisionInput")
    monkeypatch.setattr(composed_model, "ownership", lambda state: ())
    monkeypatch.setattr(composed_model, "availability", lambda state, observations: ())
    monkeypatch.setattr(
        composed_model, "Decision", SimpleNamespace(model_validate_json=lambda v: v)
    )
    driver = object.__new__(composed_model.EconomicDriver)
    driver.config, driver.strategy = "config", "strategy"
    driver.settings = settings((binding(),))
    driver.book = SimpleNamespace(prices=Prices(("weth",)))
    driver.components = {"a": None, "b": None}
    driver.decide(SimpleNamespace(time=START, memory="m"), (), "trigger")
    assert seen["r"]["context"]["prices"] == ("weth",)
    assert driver.book.prices.times == [START]


def prepared_prices(directory, declared):
    configured = settings(declared)
    writer = DatasetWriter(
        directory / economic_prices.DIRECTORY, economic_prices.preparation(DIGEST, configured)
    )
    try:
        collect_strategy_prices(writer, {CHAIN: EvmHistory(FakeChain())}, configured)
        writer.publish()
    finally:
        writer.close()
    return configured, file_digest(directory / economic_prices.DIRECTORY / "dataset.json")


def test_composed_series_are_one_prepared_dataset_sliced_per_decision(tmp_path):
    declared = (binding(window=4),)
    configured, digest = prepared_prices(tmp_path, declared)
    prices = economic_prices.Prices(tmp_path, DIGEST, configured, digest)
    try:
        (first,) = prices.at(START)
        assert first.complete and len(first.points) == 4
        assert first.latest.source_time == GENESIS + 10 * HOUR  # available at 10:05
        (later,) = prices.at(END)
        assert later.latest.source_time == GENESIS + 11 * HOUR  # 12:00 is not yet available
    finally:
        prices.close()
    with pytest.raises(ValueError, match="differs from the strategy's declared series"):
        economic_prices.Prices(tmp_path, DIGEST, configured, None)
    with pytest.raises(ValueError, match="differ from the declared series"):
        economic_prices.Prices(tmp_path, "e" * 64, configured, digest)


def test_a_composition_without_series_prepares_and_delivers_none(tmp_path):
    assert economic_prices.prepare_prices(tmp_path, DIGEST, settings(), None) is None
    assert not (tmp_path / economic_prices.DIRECTORY).exists()
    assert economic_prices.Prices(tmp_path, DIGEST, settings(), None).at(START) == ()
    with pytest.raises(ValueError, match="differs from the strategy's declared series"):
        economic_prices.Prices(tmp_path, DIGEST, settings(), DIGEST)


def test_a_composed_series_requires_a_provider_for_its_chain():
    economic_prices.validate_chains(settings((binding(),)))
    unbound = settings((binding(),))
    unbound.history = ()
    with pytest.raises(ValueError, match="historical provider for its chain"):
        economic_prices.validate_chains(unbound)


def test_the_composed_descriptor_names_series_only_when_declared():
    from almanak.connectors.composed.economic_book import Descriptor

    plain = Descriptor(policy_digest=DIGEST, components={})
    assert "prices" not in plain.model_dump(mode="json")
    assert Descriptor(policy_digest=DIGEST, components={}, prices=DIGEST).prices == DIGEST


def test_a_composed_book_binds_its_declared_series_beside_the_components(tmp_path, monkeypatch):
    from almanak.connectors.composed import economic_book

    digest = "a" * 64
    monkeypatch.setattr(economic_book, "identity", lambda config, settings: digest)
    config = SimpleNamespace(parameter_components={})
    plain = SimpleNamespace(data=None)
    (tmp_path / "dataset.json").write_text(
        economic_book.Descriptor(policy_digest=digest, components={}).model_dump_json()
    )
    book = economic_book.Book(tmp_path, config, plain)
    assert book.components == {} and book.prices.at(START) == ()
    book.close()
    with pytest.raises(ValueError, match="descriptor changed"):
        economic_book.Book(tmp_path, config, plain, digest="b" * 64)
    with pytest.raises(ValueError, match="declared series"):
        economic_book.Book(tmp_path, config, settings((binding(),)))


def test_composed_sources_check_the_declared_series_once_for_the_composition(monkeypatch):
    from almanak.connectors.composed import economic_prepare

    config = SimpleNamespace(parameter_components={}, positions=())
    configured = settings((binding(),))
    configured.models, configured.history = {}, ()
    calls = []
    monkeypatch.setattr(economic_prepare, "selected_parameters", lambda author, s: config)
    monkeypatch.setattr(
        economic_prepare, "validate_dependencies", lambda *a: calls.append("dependencies")
    )
    monkeypatch.setattr(economic_prepare, "validate_chains", lambda s: calls.append("chains"))
    assert economic_prepare.validate_sources("args", "author", configured) is config
    assert calls == ["dependencies", "chains"]
    configured.models = {"aave_v3": None}
    with pytest.raises(ValueError, match="match configured connectors"):
        economic_prepare.validate_sources("args", "author", configured)
