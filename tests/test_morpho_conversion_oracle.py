"""The compiled conversion oracle retains explicit collateral and loan units."""

import pytest

from almanak.caip import AssetType, ChainId
from almanak.connectors.morpho_blue.deployment import Deployment, oracle_dependencies
from almanak.connectors.morpho_blue.domain import Market, MarketId
from almanak.connectors.morpho_blue.reader import price_evidence
from almanak.gateway.ethereum import DataError
from almanak.models import Asset, Block, Chain, Hash32, address


def inputs():
    chain = Chain(chain_id=ChainId("eip155:43114"), environment="mainnet")
    market = Market(
        chain=chain,
        core=address("0x895383274303aa19fe978afb4ac55c7f094f982c"),
        market_id=MarketId("0x2358f128789e67e053f9b9082cfbe4c0ceca8c51fa67c72f3a395e140b96607b"),
        loan_asset=AssetType("eip155:43114/erc20:0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7"),
        collateral_asset=AssetType("eip155:43114/erc20:0xa25eaf2906fa1a3a13edac9b9657108af7b703e3"),
        oracle=address("0x7759c62e0e3c1c6ee5789b60ccd0826936cb618d"),
        irm=address("0xb6ac9477d574ee2a7bf32d2475b303fb70968aa4"),
        lltv=915000000000000000,
    )
    block = Block(chain=chain, number=1, hash=Hash32("0x" + "01" * 32), timestamp=100)
    deployment = Deployment(
        contracts=(),
        oracle_kind="erc4626_conversion",
        oracle_inputs={"STAVAX": int(market.collateral_asset.reference, 16)},
    )
    loan = Asset(chain=chain, identifier=market.loan_asset, decimals=18)
    collateral = Asset(chain=chain, identifier=market.collateral_asset, decimals=18)
    return market, block, deployment, loan, collateral


class Reader:
    def __init__(self, market):
        self.market = market
        self.underlying = market.loan_asset.reference
        self.assets = 1196439796834244754
        self.price = self.assets * 10**18
        self.unavailable = None

    def call(self, target, signature, block, *, inputs=(), args=(), outputs=()):
        if signature == self.unavailable:
            raise DataError("unavailable", "conversion evidence unavailable")
        if signature == "asset()":
            assert target == self.market.collateral_asset.reference
            return (self.underlying,)
        if signature == "convertToAssets(uint256)":
            assert target == self.market.collateral_asset.reference and args == (10**18,)
            return (self.assets,)
        assert target == self.market.oracle and signature == "price()"
        return (self.price,)


def test_exact_conversion_keeps_market_oracle_source_and_dependencies():
    market, block, deployment, loan, collateral = inputs()
    reader = Reader(market)
    observed = price_evidence(reader, market, block, deployment, loan, collateral)
    assert observed["price_error"] is None
    assert observed["price"].source == market
    assert observed["price"].price == reader.assets * 10**18
    assert oracle_dependencies(deployment) == (market.collateral_asset.reference,)


@pytest.mark.parametrize(
    "failure", ["loan_precision", "collateral_precision", "underlying", "conversion"]
)
def test_conversion_refuses_wrong_units_or_inconsistent_oracle(failure):
    market, block, deployment, loan, collateral = inputs()
    reader = Reader(market)
    if failure == "loan_precision":
        loan = loan.model_copy(update={"decimals": 6})
    elif failure == "collateral_precision":
        collateral = collateral.model_copy(update={"decimals": 6})
    elif failure == "underlying":
        reader.underlying = market.collateral_asset.reference
    else:
        reader.price += 1
    observed = price_evidence(reader, market, block, deployment, loan, collateral)
    assert observed["price"] is None and observed["price_error"]


@pytest.mark.parametrize("signature", ["asset()", "convertToAssets(uint256)", "price()"])
def test_missing_conversion_evidence_is_not_a_price(signature):
    market, block, deployment, loan, collateral = inputs()
    reader = Reader(market)
    reader.unavailable = signature
    observed = price_evidence(reader, market, block, deployment, loan, collateral)
    assert observed == {"price": None, "price_error": "conversion evidence unavailable"}
