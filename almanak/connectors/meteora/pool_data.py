"""Pool-data capability declaration for Meteora DLMM."""

from almanak.connectors._strategy_base.pool_data import (
    PoolReferenceKind,
    unsupported_pool_data_spec,
)

POOL_DATA_SPEC = unsupported_pool_data_spec(
    protocol="meteora",
    aliases=("meteora_dlmm",),
    reference_kind=PoolReferenceKind.SOLANA_ACCOUNT,
    reason=(
        "Meteora DLMM state is bin-array/account based; its connector SDK has "
        "not yet been exposed through the generic pool-data gateway contract."
    ),
)

__all__ = ["POOL_DATA_SPEC"]
