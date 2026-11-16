"""V4 candidate declaration construction and installed-binding capability lookup."""

import importlib
from datetime import UTC, datetime

import pytest

from almanak.caip import ChainId
from almanak.connectors.support import (
    UNISWAP_CANDIDATE_STARTS,
    ConnectorChainSupport,
    uniswap_candidate_entry,
    uniswap_support,
)

NO_CANDIDATES = pytest.mark.skipif(
    not UNISWAP_CANDIDATE_STARTS, reason="no uninstalled V4 candidate chain"
)


def test_candidate_entries_are_constructed_through_the_validated_model_path():
    template = next(s for s in uniswap_support() if s.chain == ChainId("eip155:1"))
    start = datetime(2026, 9, 9, tzinfo=UTC)
    entry = uniswap_candidate_entry(template, "eip155:56", start)
    assert ConnectorChainSupport.model_validate(entry.model_dump()) == entry
    assert {h.model for h in entry.historical} == {h.model for h in template.historical}
    assert all(h.backtest_start_at == start for h in entry.historical)
    # model_copy skips validation, so an inconsistent template must still be refused here.
    inconsistent = template.model_copy(update={"historical": ()})
    with pytest.raises(ValueError, match="backtest support requires its explicit historical"):
        uniswap_candidate_entry(inconsistent, "eip155:56", start)


def installed_candidate_connector():
    """The connector binding a private branch installs, without touching the live function."""
    from dataclasses import replace

    from almanak.connectors.support import candidate_uniswap_support
    from almanak.connectors.uniswap_v4.connector import connector

    return replace(connector(), chain_support=candidate_uniswap_support())


def mainnet(chain):
    from almanak.models import Chain

    return Chain(chain_id=ChainId(chain), environment="mainnet")


@pytest.mark.parametrize(
    ("module_name", "model"),
    (
        ("economic_prepare", "uniswap-v4-economic-integer-fixed-growth-v1"),
        ("backtest", "uniswap-v4-exact-quoter-fixed-path-lp-v1"),
    ),
)
@pytest.mark.parametrize("chain", tuple(chain for chain, _ in UNISWAP_CANDIDATE_STARTS))
def test_v4_capability_reads_the_installed_declaration(monkeypatch, module_name, model, chain):
    """Historical capability follows the connector binding, not the declaration function."""
    import almanak.connectors.support as support_module

    module = importlib.import_module(f"almanak.connectors.uniswap_v4.{module_name}")
    installed = installed_candidate_connector()
    # A candidate chain is absent from the live declaration by construction.
    assert ChainId(chain) not in {entry.chain for entry in uniswap_support()}
    # Reading the live declaration here would refuse the chain; the binding must win.
    monkeypatch.setattr(support_module, "uniswap_support", lambda: ())
    assert module.capability(installed, mainnet(chain)).model == model


@pytest.mark.parametrize(
    ("module_name", "message"),
    (
        ("economic_prepare", "Uniswap economic model does not support the requested chain"),
        ("backtest", "connector has no historical support for the requested chain"),
    ),
)
@NO_CANDIDATES
def test_v4_capability_refuses_a_chain_the_binding_does_not_declare(module_name, message):
    """An undeclared chain keeps its existing refusal even though the lookup moved."""
    from almanak.connectors.uniswap_v4.connector import connector

    module = importlib.import_module(f"almanak.connectors.uniswap_v4.{module_name}")
    chain = UNISWAP_CANDIDATE_STARTS[0][0]
    with pytest.raises(ValueError, match=message):
        module.capability(connector(), mainnet(chain))


@NO_CANDIDATES
def test_v4_historical_adapter_resolves_the_installed_binding(monkeypatch):
    """The shared historical adapter takes only a chain, so it must resolve the binding itself."""
    import almanak.connectors.support as support_module
    from almanak.connectors.uniswap_v4 import connector as connector_module
    from almanak.connectors.uniswap_v4.backtest_binding import UniswapHistorical

    installed = installed_candidate_connector()
    monkeypatch.setattr(connector_module, "connector", lambda: installed)
    monkeypatch.setattr(support_module, "uniswap_support", lambda: ())
    chain = UNISWAP_CANDIDATE_STARTS[0][0]
    resolved = UniswapHistorical.capability(mainnet(chain))
    assert resolved.model == "uniswap-v4-exact-quoter-fixed-path-lp-v1"
    assert resolved.chain == mainnet(chain)
