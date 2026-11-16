"""The assets whose cumulative escrowed inputs a prepared GMX position reports."""

from almanak.connectors.gmx_v2.exchange_models import input_assets
from almanak.connectors.gmx_v2.liquidity_models import LiquidityAllocation
from almanak.connectors.gmx_v2.models import Allocation


def spending_assets(allocation):
    """Cumulative inputs are reported, never limited: Roles grants carry no allowance."""
    if isinstance(allocation, Allocation):
        # Position requests escrow collateral and execution fee in wrapped native only.
        return (allocation.execution_fee.asset,)
    assets = input_assets(allocation.market)
    if isinstance(allocation, LiquidityAllocation):
        return (*assets, allocation.market.share_asset)
    return assets
