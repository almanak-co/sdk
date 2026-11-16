"""Offline checks of the load-time mappings that read pre-scope-only captures."""

import json
import re
from decimal import Decimal
from pathlib import Path

import pytest

from almanak.backtest.dataset import file_digest
from almanak.connectors.composed.economic_book import identity
from almanak.models import Model
from almanak.strategy import StrategyDefinition
from tests.composed_captures import rebind, retained_identity, with_dataset
from tests.legacy_references import capture_era_imports
from tests.lending_captures import (
    captured_parameters,
    current_json,
    current_manifest,
    current_record,
    without_capability_budgets,
)
from tests.scope_only_sources import (
    before_scope_only_digests,
    edited_sources,
    fixture,
    reviewed_json,
    unconverted_source,
    unedited_source,
    with_scope_only_strategy,
)

ROOT = Path(__file__).parents[1]
NAME = "aave_v3_ethereum_lending"
USDC = "eip155:1/erc20:0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
WETH = "eip155:1/erc20:0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"


def current_reference(name=NAME):
    return StrategyDefinition.read(ROOT / "strategies" / name / "strategy.py")


def pre_edit_source():
    source = unedited_source(NAME, current_reference().source)
    assert source is not None
    return source


def recorded_config(borrowed=5_000_000, weth=WETH, usdc=USDC, **extra):
    return {
        "allocations": [
            {"name": "collateral", "reserve": {"asset": weth}, "supply_limit": 10**17}
            | {"borrow_limit": 0, "repayment_ceiling": 0},
            {"name": "loan", "reserve": {"asset": usdc}, "supply_limit": 0}
            | {"borrow_limit": borrowed, "repayment_ceiling": 6_000_000, **extra},
            {"name": "trade", "capital": 1},
        ],
        "asset_metadata": {weth: {"decimals": 18}, usdc: {"decimals": 6}},
    }


def wire(source, config):
    return {"strategy": {"filename": "strategy.py", "source": source}, "config": config}


def test_reviewed_edit_reverses_to_the_recorded_pre_edit_source_and_back():
    source = pre_edit_source()
    before = fixture()["before"]
    names = [name for name, digest in before.items() if digest == before[NAME]]
    assert edited_sources(source) == {current_reference(name).source for name in names}
    assert edited_sources(current_reference().source) == set()
    assert unedited_source(NAME, source) is None
    era = capture_era_imports(StrategyDefinition(filename="strategy.py", source=source))
    assert before_scope_only_digests(NAME, current_reference()) == {era.sha256}
    assert set(fixture()["references"]) == set(fixture()["before"])


def test_capture_era_source_reads_as_the_current_reference_with_equal_budgets():
    era = capture_era_imports(StrategyDefinition(filename="strategy.py", source=pre_edit_source()))
    mapped = json.loads(reviewed_json(json.dumps(wire(era.source, recorded_config()))))
    expected = capture_era_imports(current_reference())
    assert mapped["strategy"]["source"] == expected.source
    assert mapped["config"]["allocations"][1]["borrow"] is True
    assert mapped["config"]["parameters"] == {"borrow_quantity": "5"}
    assert "repayment_ceiling" not in mapped["config"]["allocations"][1]
    plain = json.loads(current_json(json.dumps(wire(era.source, recorded_config()))))
    assert plain["strategy"]["source"] == era.source


def test_the_capture_chain_singles_out_references_that_now_differ():
    # The Arbitrum reference alone accepts a loan-asset reserve; both read the same capture.
    weth = "eip155:42161/erc20:0x82af49447d8a07e3bd95bd0d56f35241523fbab1"
    usdc = "eip155:42161/erc20:0xaf88d065e77c8cc2239327c5edb3a432268e5831"
    config = recorded_config(weth=weth, usdc=usdc)
    mapped = with_scope_only_strategy(current_record(wire(pre_edit_source(), config)), config)
    assert mapped["strategy"]["source"] == current_reference("aave_v3_arbitrum_lending").source
    elsewhere = recorded_config(weth="eip155:999/erc20:0x1", usdc="eip155:999/erc20:0x2")
    with pytest.raises(ValueError, match="single out one reviewed scope-only edit"):
        with_scope_only_strategy(current_record(wire(pre_edit_source(), elsewhere)), elsewhere)


def constants(source):
    return dict(re.findall(r'^([A-Z_]+) = Decimal\("([^"]+)"\)$', source, re.M))


@pytest.mark.parametrize("name", sorted(fixture()["parameters"]["references"]))
def test_parameter_conversion_reverses_to_constants_the_parameters_equal(name):
    unconverted = unconverted_source(name, current_reference(name).source)
    assert unconverted is not None
    parameters = captured_parameters(name)
    names = {"BORROW": "borrow_quantity", "UNWIND_MARGIN": "unwind_margin"}
    assert {names[c]: Decimal(v) for c, v in constants(unconverted).items()} == {
        field: Decimal(value) for field, value in parameters.items()
    }


@pytest.mark.parametrize("config", [recorded_config(7_000_000), recorded_config(entry_amount=1)])
def test_budgets_the_edited_source_does_not_decide_refuse_the_mapping(config):
    with pytest.raises(ValueError, match="single out one reviewed scope-only edit"):
        with_scope_only_strategy(current_record(wire(pre_edit_source(), config)), config)


def test_other_sources_and_records_without_a_strategy_are_unchanged():
    config = recorded_config()
    current = current_record(wire(current_reference().source, config))
    assert with_scope_only_strategy(current, config) == current
    assert with_scope_only_strategy({"config": {}}, config) == {"config": {}}
    loan = current["config"]["allocations"][1]
    metadata = current["config"]["asset_metadata"]
    assert metadata[USDC] == {"decimals": 6} and loan["capital"] == 0


def test_only_capabilities_lose_retired_budgets():
    target = {"pool": "p"}
    value = [
        {"target": target, "maximums": [1]},
        {"kind": "open_liquidity", "target": target, "maximums": [1], "deadline": 2},
        {"target": target},
        "text",
    ]
    assert without_capability_budgets({"requirements": value}) == {
        "requirements": [{"target": target}, value[1], value[2], "text"]
    }


class Config(Model):
    allocations: list[dict]
    asset_metadata: dict


class Settings(Model):
    latency: int


class Run(Model):
    config: Config
    settings: Settings
    dataset: Path | None = None
    dataset_sha256: str = ""


def composed_dataset(directory, raw, model="composed-native-economic-v1"):
    directory.mkdir(exist_ok=True)
    descriptor = {"model": model, "policy_digest": retained_identity(raw), "components": {}}
    directory.joinpath("dataset.json").write_text(json.dumps(descriptor))
    return directory


def test_composed_descriptor_rebinds_only_the_retained_run_configuration(tmp_path):
    raw = {"config": recorded_config(), "settings": {"latency": 1}}
    config = Config.model_validate(current_record(raw)["config"])
    settings = Settings(latency=1)
    dataset = composed_dataset(tmp_path / "composed", raw)
    run = with_dataset(Run(config=config, settings=settings), raw, dataset)
    descriptor = json.loads((dataset / "dataset.json").read_text())
    assert descriptor["policy_digest"] == identity(config, settings)
    assert run.dataset == dataset and run.dataset_sha256 == file_digest(dataset / "dataset.json")
    with pytest.raises(AssertionError):
        rebind(dataset, raw, config, settings)
    composed_dataset(dataset, raw)
    with pytest.raises(AssertionError):
        rebind(dataset, raw, config, Settings(latency=2))
    native = composed_dataset(tmp_path / "native", raw, model="aave-v3-economic-v1")
    assert with_dataset(Run(config=config, settings=settings), raw, native).dataset_sha256 == ""


def test_an_already_mapped_config_cannot_vouch_for_a_pre_edit_source(tmp_path):
    mapped = current_record(wire(pre_edit_source(), recorded_config()))
    with pytest.raises(ValueError, match="single out one reviewed scope-only edit"):
        with_scope_only_strategy(mapped, mapped["config"])
    (tmp_path / "manifest.json").write_text(json.dumps(wire(pre_edit_source(), recorded_config())))
    current_manifest(tmp_path)
    assert json.loads((tmp_path / "manifest.json").read_text()) == mapped
