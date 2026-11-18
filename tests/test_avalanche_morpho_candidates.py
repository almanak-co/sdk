"""Avalanche Morpho references use the promoted public support declaration."""

import json
from importlib import import_module
from pathlib import Path
from uuid import uuid4

import pytest
import yaml

from almanak import reports
from almanak.caip import ChainId
from almanak.configuration.backtest_inputs import read_backtest_inputs
from almanak.configuration.parameter_codec import validation_parameters
from almanak.configuration.preparation import check_preparation
from almanak.configuration.unresolved import read_inputs
from almanak.connectors.morpho_blue import support
from almanak.runtime import strategy_worker, validate_strategy_config
from almanak.strategy import DataDependencyRequest, StrategyDefinition
from scripts.legacy_configuration.author import read_author
from scripts.legacy_configuration.backtest import read_backtest
from tests.lending_candidates import sandbox_available
from tests.test_new_reference_replay import module

ROOT = Path(__file__).parents[1]
AVALANCHE = ChainId("eip155:43114")
STANDALONE = "morpho_blue_avalanche_lending"
COMPOSED = "avalanche_morpho_borrow_and_trade_v4"
REFERENCES = (STANDALONE, COMPOSED)
RESOLVED = ROOT / "tests/fixtures/author/candidates"


@pytest.fixture
def avalanche_declaration():
    row = next(entry for entry in support.candidate_support() if entry.chain == AVALANCHE)
    assert row.restrictions[0] == (
        "verified Morpho Blue core, adaptive IRM and explicit ERC-4626 conversion oracle revision"
    )
    return row


def raw_config(name):
    return yaml.safe_load((ROOT / "strategies" / name / "config.yaml").read_text())


def resolved_config(name):
    """Frozen complete inputs used only to exercise reference hooks offline."""
    return RESOLVED / name / "config.yaml"


def public_document(name):
    return read_inputs(ROOT / "strategies" / name / "config.yaml")


def offline_stages(name):
    directory = ROOT / "strategies" / name
    report = check_preparation(
        public_document(name), StrategyDefinition.read(directory / "strategy.py")
    )
    return report, {item["stage"]: item["status"] for item in report["stages"]}


@pytest.mark.parametrize("name", REFERENCES)
def test_public_validation_accepts_avalanche_morpho_references(name, avalanche_declaration):
    document = public_document(name)
    assert {market.chain for market in document.inputs.markets.values()} == {AVALANCHE}
    assert any(
        row["chain"] == str(avalanche_declaration.chain) for row in reports.support()["morpho_blue"]
    )


@pytest.mark.parametrize("name", REFERENCES)
def test_public_declaration_qualifies_author_inputs_and_backtest(name, avalanche_declaration):
    directory = ROOT / "strategies" / name
    document = public_document(name)
    assert {str(m.chain) for m in document.inputs.markets.values()} == {"eip155:43114"}
    report, stages = offline_stages(name)
    assert report["valid"] and not report["ready"]
    assert stages["sandbox"] == "passed"
    assert stages["strategy"] == stages["capabilities"] == "not_checked"
    settings = read_backtest_inputs(directory / "backtest-economic.yaml").inputs
    assert settings.window.start < settings.window.end
    assert avalanche_declaration.chain == AVALANCHE


@pytest.mark.parametrize("name", REFERENCES)
def test_public_declaration_qualifies_actual_author_hooks_and_backtest(name):
    author = read_author(resolved_config(name))
    parameters = validation_parameters(author)
    source = module(name)
    source.validate_config(parameters)
    assert set(source.capabilities(parameters).allocations) == {a.name for a in author.allocations}
    dependencies = json.loads(
        strategy_worker(
            DataDependencyRequest(
                strategy=StrategyDefinition.read(ROOT / "strategies" / name / "strategy.py"),
                config=parameters,
            )
        )
    )
    assert dependencies == {"assets": [], "markets": [], "prices": []}
    settings = read_backtest(RESOLVED / name / "backtest-economic.yaml", author)
    assert settings.history and settings.funds
    assert settings.schedule.start < settings.schedule.end


@pytest.mark.skipif(not sandbox_available(), reason="strategy sandbox unavailable on this host")
@pytest.mark.parametrize("name", REFERENCES)
def test_public_declaration_reaches_the_strategy_sandbox_before_resolution(name):
    report, stages = offline_stages(name)
    assert report["valid"] and not report["ready"]
    assert stages["sandbox"] == "passed"
    assert stages["strategy"] == "not_checked"


@pytest.mark.skipif(not sandbox_available(), reason="strategy sandbox unavailable on this host")
@pytest.mark.parametrize("name", REFERENCES)
def test_public_resolved_reference_validates_inside_strategy_sandbox(name):
    validate_strategy_config(
        StrategyDefinition.read(ROOT / "strategies" / name / "strategy.py"),
        read_author(resolved_config(name)),
    )


def test_composed_reference_shares_one_fund_and_reuses_qualified_targets():
    inputs = public_document(COMPOSED).inputs
    collateral, loan, trade = (
        inputs.strategy.allocations[n] for n in ("collateral", "loan", "trade")
    )
    initial = next(
        item.balance.quantity
        for item in inputs.inventory
        if item.balance.asset == trade.capital.asset
    )
    assert initial == "0.011"
    assert trade.capital.quantity == "0.05" and loan.borrow
    assert inputs.strategy.parameters["borrow_quantity"] == "0.1"
    assert collateral.account == loan.account == trade.account
    assert {item.account for item in inputs.inventory} == {trade.account}
    pool = inputs.markets[trade.market]
    counter = next(a for a in (pool.currency0, pool.currency1) if a != trade.capital.asset)
    assert [
        item.balance.quantity for item in inputs.inventory if item.balance.asset == counter
    ] == ["0"]
    resolved = read_author(resolved_config(COMPOSED))
    resolved_trade = resolved.allocation("trade")
    assert resolved.fork_config().inventory == resolved.inventory
    assert resolved_trade.pool.currency0 == pool.currency0
    assert resolved_trade.pool.currency1 == pool.currency1
    assert resolved.allocation("collateral").reserve.asset != counter
    composed, standalone = raw_config(COMPOSED), raw_config(STANDALONE)
    for name in ("collateral", "loan"):
        assert composed["markets"][name]["market_id"] == standalone["markets"][name]["market_id"]
    aave = raw_config("avalanche_aave_v4_borrow_and_trade")
    assert composed["markets"]["pool"] == aave["markets"]["pool"]


def test_composed_economic_hook_cannot_pass_before_matching_resolution():
    report, stages = offline_stages(COMPOSED)
    assert report["valid"] and not report["ready"]
    assert stages["resolution"] == stages["strategy"] == stages["capabilities"] == "not_checked"


@pytest.mark.parametrize("name", REFERENCES)
def test_native_collection_policy_covers_window_and_preserves_author_inputs(name, tmp_path):
    from dataclasses import replace

    from almanak.configuration.values import seconds
    from almanak.connectors.identifiers import ConnectorId
    from tests.qa.catalog import Case
    from tests.qa.inputs import resolved_author, resolved_history
    from tests.test_native_risk_block_policy import required_blocks

    case = Case(
        id=f"native/{name}",
        lane="backtest",
        reference=name,
        connector=ConnectorId("morpho_blue") if name == STANDALONE else None,
        chains=(AVALANCHE,),
        model_mode="economic",
        input_policy="native",
    )
    path = ROOT / "strategies" / name / "backtest-economic.yaml"
    original = path.read_bytes()
    resolved_author(case, tmp_path, {})
    native = read_backtest_inputs(resolved_history(case, tmp_path, 0)).inputs
    schedule = native.schedule()
    duration = schedule["end"] + schedule["cleanup_seconds"] - schedule["start"]
    components = native.data.get("connectors", {"morpho_blue": native.data})
    lending = components["morpho_blue"]
    assert lending["max_accounting_samples"] == duration // seconds(lending["sample_seconds"]) + 1
    assert lending["max_risk_blocks"] == lending["max_risk_samples"] == required_blocks(AVALANCHE)
    if name == COMPOSED:
        dex = components["uniswap_v4"]
        assert dex["max_samples"] == duration // seconds(dex["sample_seconds"]) + 1
    pinned = read_backtest_inputs(
        resolved_history(replace(case, input_policy="pinned"), tmp_path, 0)
    ).inputs
    assert pinned == read_backtest_inputs(path).inputs
    assert path.read_bytes() == original


@pytest.mark.fork
@pytest.mark.strategy_fork
def test_standalone_safe_lifecycle(request, tmp_path):
    """Exercise the promoted Safe/Roles reference through the shared fork path."""
    from tests.strategy_fork.harness import discover, run_case

    assert AVALANCHE in {entry.chain for entry in support.candidate_support()}
    case = next(
        item
        for item in discover(ROOT / "strategies", ROOT / "tests/strategy_fork/scenarios")
        if item.name == STANDALONE
    )
    artifact = (
        ROOT / "artifacts/morpho-avalanche-qualification" / ("standalone-safe-" + uuid4().hex)
    )
    artifact.parent.mkdir(parents=True, exist_ok=True)
    run_case(
        case,
        request.config.getoption("--fork-env").resolve(),
        {},
        tmp_path / "workspace",
        artifact,
        "configured",
    )


@pytest.mark.parametrize("fault", ("counter_cash", "collateral_in_pool", "whole_loan"))
def test_composed_author_hook_refuses_ambiguous_cash_and_trades(tmp_path, fault):
    raw = yaml.safe_load(resolved_config(COMPOSED).read_text())
    trade = raw["strategy"]["allocations"]["trade"]
    pool = raw["markets"]["pool"]["pool"]
    counter = pool["currency1"]
    collateral = raw["markets"]["collateral"]["reserve"]["asset"]
    if fault == "counter_cash":
        balance = next(r["balance"] for r in raw["inventory"] if r["balance"]["asset"] == counter)
        balance["quantity"] = "1"
        raw["execution"]["fork"]["funds"][0]["assets"].append(
            {"balance": dict(balance), "method": "local_storage"}
        )
        message = "counter-asset must start with explicit zero fund cash"
    elif fault == "collateral_in_pool":
        pool["currency0"], pool["currency1"] = collateral, pool["currency0"]
        # Blueprint 27 retires the Uniswap V4 maxima; a fixture may still carry them.
        for key, currency in (("maximum0", "currency0"), ("maximum1", "currency1")):
            if key in trade:
                trade[key]["asset"] = pool[currency]
        raw["inventory"] = [r for r in raw["inventory"] if r["balance"]["asset"] != counter]
        message = "borrowed asset for a third asset"
    else:
        # The borrowed amount is the `borrow_quantity` parameter.
        trade["capital"]["quantity"] = str(raw["strategy"]["parameters"]["borrow_quantity"])
        message = "only part of the borrowed asset"
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(raw, sort_keys=False))
    with pytest.raises(ValueError, match=message):
        module(COMPOSED).validate_config(validation_parameters(read_author(path)))


def test_composed_teardown_deadline_covers_the_avalanche_production_path():
    """The production EOA rehearsal waits `teardown_timeout` for closure.

    Both retained batch-3 rehearsals of the composed reference were stopped by that
    wait, not by the runtime: one committed the final collateral withdrawal decision
    about 600 seconds after the teardown request, the other had committed but not
    delivered the preceding revoke outcome. The passing Avalanche Aave V4 control
    has the same exit, repay and withdraw teardown shape and also reached its
    withdrawal decision at about 595 seconds; it closes under its 900 second wait.
    """
    scenarios = "tests.strategy_fork.scenarios."
    morpho = import_module(scenarios + COMPOSED + ".assertions")
    control = import_module(scenarios + "avalanche_aave_v4_borrow_and_trade.assertions")
    assert morpho.teardown_timeout >= control.teardown_timeout
