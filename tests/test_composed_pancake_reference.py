"""The actual candidate must bind Pancake and lending to one authoritative fund."""

import gzip
import json
from pathlib import Path

import pytest

from almanak.connectors.composed.authority import declared
from almanak.connectors.composed.models import Config
from almanak.connectors.identifiers import ConnectorId
from almanak.runtime import strategy_capabilities, validate_strategy_config
from almanak.strategy import StrategyDefinition
from tests.composed_public import economic_inputs, resolved_parameters
from tests.test_new_reference_replay import module

ROOT = Path(__file__).parents[1]


@pytest.mark.parametrize(
    ("reference", "connector"),
    (
        ("base_aave_pancake_borrow_and_trade", "pancakeswap_v3"),
        ("ethereum_aave_pancake_borrow_and_trade", "pancakeswap_v3"),
        ("arbitrum_aave_pancake_borrow_and_trade", "pancakeswap_v3"),
        ("bnb_aave_pancake_borrow_and_trade", "pancakeswap_v3"),
        ("base_aave_aerodrome_stable_borrow_and_trade", "aerodrome"),
        ("base_aave_aerodrome_volatile_borrow_and_trade", "aerodrome"),
        ("base_aave_slipstream_borrow_and_trade", "aerodrome_slipstream"),
    ),
)
def test_pancake_reference_validates_and_preserves_shared_inventory(reference, connector):
    directory = ROOT / "strategies" / reference
    _, author, _, _ = economic_inputs(reference)
    validate_strategy_config(StrategyDefinition.read(directory / "strategy.py"), author)
    parameters = author
    config = Config.model_construct(
        **{name: getattr(parameters, name) for name in type(parameters).model_fields}
    )
    assert set(config.execution_components) == {
        ConnectorId("aave_v3"),
        ConnectorId(connector),
    }
    assert config.component(ConnectorId(connector)).allocation("trade") is config.allocation(
        "trade"
    )
    loan = config.target("loan")
    trade = config.allocation("trade")
    initial = next(
        balance.amount for balance in config.inventory if balance.key.asset == loan.asset
    )
    source = module(reference)
    borrowed = source.borrow_amount(source.typed_config(config), "loan")
    assert config.allocation("loan").borrow and initial < trade.capital.amount < borrowed
    assert config.target("trade").fund == loan.fund


def test_public_composition_inventory_decodes_in_actual_sandbox_hooks():
    reference = "arbitrum_aave_pancake_borrow_and_trade"
    directory = ROOT / "strategies" / reference
    _, parameters = resolved_parameters(reference)
    assert parameters.inventory
    strategy = StrategyDefinition.read(directory / "strategy.py")
    validate_strategy_config(strategy, parameters)
    assert strategy_capabilities(strategy, parameters) == declared(parameters)


def test_bnb_composition_pool_replays_verified_usdt_wbnb_archive():
    from almanak.connectors.pancakeswap_v3.deployments import verify_periphery, verify_pool
    from almanak.connectors.pancakeswap_v3.domain import Contracts
    from tests.test_pancakeswap_v3_reader import Replay

    path = ROOT / "docs/internal/reports/evidence/2026-09-09-dex-finish/discovery"
    retained = json.loads(
        gzip.decompress((path / "pancakeswap-v3-bnb-composition-discovery.json.gz").read_bytes())
    )
    _, author = resolved_parameters("bnb_aave_pancake_borrow_and_trade")
    trade = author.allocation("trade")
    assert trade.pool.model_dump(mode="json") == retained["source"]
    assert trade.capital.asset == author.allocation("loan").reserve.asset
    with Replay({**retained, "requests": retained["reads"]}) as reader:
        block = reader.pin_block(retained["block"]["number"])
        contracts = Contracts.model_validate_json(json.dumps(retained["contracts"]))
        identities = (
            *verify_pool(reader, reader.source, block),
            *verify_periphery(reader, reader.source, contracts, block),
        )
        assert [i.model_dump(mode="json") for i in identities] == retained["identities"]
