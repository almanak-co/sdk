"""Pool-data capability declaration for Raydium CLMM."""

from almanak.connectors._strategy_base.pool_data import (
    PoolReferenceKind,
    unsupported_pool_data_spec,
)

POOL_DATA_SPEC = unsupported_pool_data_spec(
    protocol="raydium",
    aliases=("raydium_clmm",),
    reference_kind=PoolReferenceKind.SOLANA_ACCOUNT,
    reason=(
        "Raydium CLMM state is Solana-account based; its connector SDK has not "
        "yet been exported through the generic pool-data gateway contract."
    ),
)

__all__ = ["POOL_DATA_SPEC"]
