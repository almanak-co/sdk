"""Pool-data capability declaration for Fluid SmartLending DEX LP wrappers."""

from almanak.connectors._strategy_base.pool_data import (
    PoolReferenceKind,
    unsupported_pool_data_spec,
)

POOL_DATA_SPEC = unsupported_pool_data_spec(
    protocol="fluid_dex_lp",
    reference_kind=PoolReferenceKind.EVM_CONTRACT,
    reason=(
        "Fluid DEX LP positions are SmartLending wrapper shares; connector "
        "valuation exists, but a pool-address metadata/state adapter does not."
    ),
)

__all__ = ["POOL_DATA_SPEC"]
