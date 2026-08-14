"""Pool-data capability declaration for Trader Joe Liquidity Book."""

from almanak.connectors._strategy_base.pool_data import (
    PoolReferenceKind,
    unsupported_pool_data_spec,
)

POOL_DATA_SPEC = unsupported_pool_data_spec(
    protocol="traderjoe_v2",
    aliases=("joe_v2", "traderjoe"),
    reference_kind=PoolReferenceKind.EVM_CONTRACT,
    reason=(
        "Liquidity Book state is active-bin/bin-reserve based; its connector "
        "does not yet expose a generic pool metadata/state adapter."
    ),
)

__all__ = ["POOL_DATA_SPEC"]
