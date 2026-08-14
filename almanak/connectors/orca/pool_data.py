"""Pool-data capability declaration for Orca Whirlpools."""

from almanak.connectors._strategy_base.pool_data import (
    PoolReferenceKind,
    unsupported_pool_data_spec,
)

POOL_DATA_SPEC = unsupported_pool_data_spec(
    protocol="orca",
    aliases=("orca_whirlpools",),
    reference_kind=PoolReferenceKind.SOLANA_ACCOUNT,
    reason=(
        "Orca Whirlpool state is a Solana account graph; its connector reads "
        "are not yet exported through the generic pool-data gateway contract."
    ),
)

__all__ = ["POOL_DATA_SPEC"]
