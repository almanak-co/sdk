"""OP production authority rests on the quote-time exposure bound, not mined receipts."""

import pytest
from pydantic import ValidationError

from almanak.caip import ChainId
from almanak.connectors.composition import connector_from_wire
from almanak.connectors.curve.support import verified_support
from almanak.connectors.support import OP_FAMILY_CHAINS, OP_FEE_ACCOUNTING, RunMode
from almanak.gateway.production_config import ProductionNetwork
from almanak.gateway.providers import UrlProvider
from almanak.models import Chain
from tests.strategy_fork.rehearsal import production_references


@pytest.mark.parametrize("chain", ["eip155:8453", "eip155:10"])
def test_op_network_admits_production_with_the_pinned_exposure_bound(chain):
    # The quote-time bound covers L1 data and operator fees inside the same
    # payer ceilings; mined receipts remain accounting evidence only.
    network = ProductionNetwork(
        chain=Chain(chain_id=ChainId(chain), environment="mainnet"),
        provider=UrlProvider(kind="rpc_url", url_env="EXPLICIT_RPC_URL"),
        max_observation_age_seconds=180,
    )
    assert network.chain.chain_id == ChainId(chain)
    with pytest.raises(ValidationError, match="mainnet environment"):
        ProductionNetwork(
            chain=Chain(chain_id=ChainId(chain), environment="anvil:test"),
            provider=UrlProvider(kind="rpc_url", url_env="EXPLICIT_RPC_URL"),
            max_observation_age_seconds=180,
        )


def test_curve_op_declarations_admit_production_with_pinned_fee_accounting():
    selected = [
        support
        for support in verified_support()
        if support.chain in (ChainId("eip155:8453"), ChainId("eip155:10"))
    ]
    assert len(selected) == 2
    for support in selected:
        assert support.modes == (RunMode.FORK, RunMode.PRODUCTION, RunMode.BACKTEST)
        support.require(RunMode.PRODUCTION, account_profile="eoa")
        assert "pinned Jovian L1 and operator fee accounting" in support.restrictions
        assert not any("production unavailable" in item for item in support.restrictions)
        assert support.historical


def test_curve_op_production_rehearsal_follows_every_market_declaration():
    selected = {
        name
        for name in production_references()
        if name.startswith(("curve_base_", "curve_optimism_"))
    }
    assert selected == {
        f"curve_{chain}_{family}_lp"
        for chain in ("base", "optimism")
        for family in ("stable", "tricrypto", "twocrypto")
    }
    admitted = set(production_references())
    # Aave V3 on Base declares production, so its Curve composition joins; the fourth
    # batch admits Compound and Morpho on Optimism.
    assert {
        "base_aave_curve_borrow_and_trade",
        "optimism_compound_curve_borrow_and_trade",
        "optimism_morpho_curve_borrow_and_trade",
    } <= admitted


@pytest.mark.parametrize(
    "connector", ["uniswap_v3", "uniswap_v4", "pancakeswap_v3", "aerodrome", "aerodrome_slipstream"]
)
def test_op_dex_declarations_admit_production_with_pinned_fee_accounting(connector):
    declared = connector_from_wire({"connector": connector}).chain_support
    selected = [support for support in declared if support.chain in OP_FAMILY_CHAINS]
    assert selected
    for support in selected:
        assert support.modes == (RunMode.FORK, RunMode.PRODUCTION, RunMode.BACKTEST)
        support.require(RunMode.PRODUCTION, account_profile="eoa")
        assert support.restrictions[-1] == OP_FEE_ACCOUNTING
        assert not any("production unavailable" in item for item in support.restrictions)
    assert not any(
        OP_FEE_ACCOUNTING in support.restrictions
        for support in declared
        if support.chain not in OP_FAMILY_CHAINS
    )
