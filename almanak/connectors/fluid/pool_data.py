"""Pool-data capability declaration for Fluid DEX pools."""

from almanak.connectors._strategy_base.pool_data import (
    PoolReferenceKind,
    unsupported_pool_data_spec,
)

POOL_DATA_SPEC = unsupported_pool_data_spec(
    protocol="fluid",
    reference_kind=PoolReferenceKind.EVM_CONTRACT,
    reason=(
        "Fluid DEX pools use connector-specific Smart DEX state; swap quoting "
        "exists, but a generic authenticated metadata/state adapter does not."
    ),
)

__all__ = ["POOL_DATA_SPEC"]
