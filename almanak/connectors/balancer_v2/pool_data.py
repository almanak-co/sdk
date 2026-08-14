"""Pool-data capability declaration for Balancer V2."""

from almanak.connectors._strategy_base.pool_data import (
    PoolReferenceKind,
    unsupported_pool_data_spec,
)

POOL_DATA_SPEC = unsupported_pool_data_spec(
    protocol="balancer_v2",
    aliases=("balancer", "bal"),
    reference_kind=PoolReferenceKind.EVM_POOL_ID,
    reason=(
        "Balancer pools are Vault-owned, N-asset pool IDs; the generic Vault "
        "metadata/balance adapter is not yet wired to MarketSnapshot."
    ),
)

__all__ = ["POOL_DATA_SPEC"]
