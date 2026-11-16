"""Prepared lending collection retains exact chain coverage and inclusive sample bounds."""

from importlib import import_module
from types import SimpleNamespace

import pytest

from almanak.caip import ChainId
from almanak.models import Chain


@pytest.fixture(params=["aave_v3", "compound_v3", "morpho_blue", "sparklend"])
def validate(request):
    return import_module(f"almanak.connectors.{request.param}.economic_prepare").validate_collection


def market(chain):
    return SimpleNamespace(chain=Chain(chain_id=ChainId(chain), environment="mainnet"))


def settings(history, maximum):
    return SimpleNamespace(
        history=history,
        schedule=SimpleNamespace(start=0, deadline=20),
        data=SimpleNamespace(sample_seconds=10, max_accounting_samples=maximum),
    )


@pytest.mark.parametrize("maximum", [5, 6])
def test_sample_limit_counts_both_endpoints_and_each_market(validate, maximum):
    markets = (market("eip155:1"), market("eip155:1"))
    selected = settings(markets[:1], maximum)
    if maximum == 5:
        with pytest.raises(ValueError, match="explicit sample limit"):
            validate(markets, selected)
    else:
        validate(markets, selected)


@pytest.mark.parametrize("history", [(), ("eip155:1", "eip155:42161"), ("eip155:42161",)])
def test_provider_coverage_must_match_the_selected_chains(validate, history):
    selected = settings(tuple(market(chain) for chain in history), 3)
    with pytest.raises(ValueError, match="cover exactly the configured chains"):
        validate((market("eip155:1"),), selected)
