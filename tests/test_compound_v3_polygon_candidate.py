"""The blocked Polygon Comet candidate refuses public support and qualifies only privately."""

from pathlib import Path

import pytest
import yaml

from almanak import reports
from almanak.caip import ChainId
from almanak.configuration.parameter_codec import validation_parameters
from almanak.configuration.unresolved import read_inputs
from almanak.configuration.yaml import ConfigurationError
from almanak.connectors.compound_v3 import connector as compound
from almanak.connectors.compound_v3 import support
from almanak.runtime import validate_strategy_config
from almanak.strategy import StrategyDefinition
from scripts.legacy_configuration.author import read_author
from scripts.legacy_configuration.backtest import read_backtest
from tests.legacy_references import harness_directory
from tests.lending_candidates import private_declaration, sandbox_available
from tests.reference_parameters import authored_parameters
from tests.test_new_reference_replay import module

ROOT = Path(__file__).parents[1]
POLYGON = ChainId("eip155:137")
REFERENCE = "compound_v3_polygon_lending"
# The harness copies are frozen; the shipped reference reads the public author root.
DIRECTORY = harness_directory(REFERENCE)
SHIPPED = ROOT / "strategies" / REFERENCE
# Identities recorded by the retained cap-blocker and qualification reports.
COMET = "0xf25212e676d1f7f89cd72ffee66158f541246445"
BASE_ASSET = "eip155:137/erc20:0x2791bca1f2de4661ed88a30c99a7a9449aa84174"
COLLATERAL = "eip155:137/erc20:0x7ceb23fd6bc0add59e62ac25578270cff1b9f619"


def frozen_author():
    """The frozen harness input, with the knobs the shipped reference now declares."""
    author = read_author(DIRECTORY / "config.yaml")
    return author.model_copy(update={"parameters": authored_parameters(REFERENCE)})


@pytest.fixture
def private_polygon_declaration(monkeypatch):
    return private_declaration(monkeypatch, compound, support, POLYGON)


def test_public_validation_refuses_the_polygon_candidate():
    with pytest.raises(ConfigurationError, match="connector does not support eip155:137"):
        read_inputs(SHIPPED / "config.yaml")
    assert "eip155:137" not in {row["chain"] for row in reports.support()["compound_v3"]}


def test_private_declaration_qualifies_author_hooks_and_backtest(private_polygon_declaration):
    author = frozen_author()
    parameters = validation_parameters(author)
    source = module(REFERENCE)
    source.validate_config(parameters)
    assert set(source.capabilities(parameters).allocations) == {"earn", "collateral", "loan"}
    settings = read_backtest(DIRECTORY / "backtest-economic.yaml", author)
    assert settings.history and settings.funds and settings.payers
    assert settings.schedule.start < settings.schedule.end
    # The private extension lives in this process; the declared source stays unchanged.
    assert POLYGON not in {entry.chain for entry in support.candidate_support()}


@pytest.mark.skipif(not sandbox_available(), reason="strategy sandbox unavailable on this host")
def test_private_declaration_validates_inside_the_strategy_sandbox(private_polygon_declaration):
    author = frozen_author()
    validate_strategy_config(StrategyDefinition.read(SHIPPED / "strategy.py"), author)


def test_candidate_binds_the_documented_blocked_market(private_polygon_declaration):
    author = frozen_author()
    markets = {a.reserve.market for a in author.allocations}
    assert len(markets) == 1
    market = markets.pop()
    assert (str(market.chain.chain_id), market.comet, str(market.base_asset)) == (
        "eip155:137",
        COMET,
        BASE_ASSET,
    )
    assert str(author.allocation("collateral").reserve.asset) == COLLATERAL
    assert author.allocation("earn").account != author.allocation("loan").account
    raw = yaml.safe_load((SHIPPED / "config.yaml").read_text())
    optimism = yaml.safe_load(
        (ROOT / "strategies/compound_v3_optimism_lending/config.yaml").read_text()
    )
    assert list(raw["strategy"]["allocations"]) == list(optimism["strategy"]["allocations"])
    readme = (SHIPPED / "README.md").read_text()
    assert "blocked candidate" in readme and "zero collateral supply caps" in readme
    # The mirrored hook must accept the Polygon parameters and declare the same authority
    # shape as the Optimism reference; source identity is not the contract.
    parameters = validation_parameters(author)
    source = module(REFERENCE)
    source.validate_config(parameters)
    assert set(source.capabilities(parameters).allocations) == {"earn", "collateral", "loan"}
