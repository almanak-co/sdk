"""Read-only GMX perpetual position observation requests for the ax CLI."""

from typing import Literal

from pydantic import model_validator

from almanak.caip import AssetType, ChainId
from almanak.connectors.gmx_v2.domain import Market, Target
from almanak.connectors.gmx_v2.position_reader import read_position
from almanak.gateway.ethereum import EthereumReader
from almanak.models import Account, Block, Model


def supported_chains() -> frozenset[ChainId]:
    """Chains with declared GMX candidate support; imported lazily to keep reads light."""
    from almanak.connectors.gmx_v2.connector import candidate_support

    return frozenset(support.chain for support in candidate_support())


class GmxPositionRequest(Model):
    market: Market
    collateral_asset: AssetType
    side: Literal["long", "short"]

    @model_validator(mode="after")
    def scope(self):
        if self.market.chain.chain_id not in supported_chains():
            raise ValueError(f"GMX position reads are unsupported on {self.market.chain.chain_id}")
        if self.collateral_asset not in (self.market.long_asset, self.market.short_asset):
            raise ValueError("GMX position collateral must identify a configured market asset")
        return self


def read_gmx_position(
    reader: EthereumReader,
    request: GmxPositionRequest,
    account: Account,
    block: Block,
) -> dict:
    if request.market.chain != reader.chain:
        raise ValueError("request chain differs from the reader chain/environment")
    if account.chain != reader.chain:
        raise ValueError("account chain differs from the reader chain/environment")
    # Observation labels are presentation only; GMX ownership scope is
    # fund/market/collateral/side and never splits on allocation names.
    target = Target(
        allocation="ax",
        account="ax",
        fund=account,
        market=request.market,
        collateral_asset=request.collateral_asset,
        side=request.side,
    )
    observation = read_position(reader, target, block)
    return {
        "product": "gmx_perp_position",
        "request": request.model_dump(mode="json"),
        "observation": observation.model_dump(mode="json"),
    }
