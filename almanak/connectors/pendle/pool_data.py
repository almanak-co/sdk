"""Pool-data capability declaration for Pendle markets."""

from almanak.connectors._strategy_base.pool_data import (
    PoolReferenceKind,
    unsupported_pool_data_spec,
)

POOL_DATA_SPEC = unsupported_pool_data_spec(
    protocol="pendle",
    reference_kind=PoolReferenceKind.EVM_CONTRACT,
    reason=(
        "Pendle markets have SY/PT/YT-specific metadata and valuation; the "
        "principal-token reader is not a generic pool state/TWAP adapter."
    ),
)

__all__ = ["POOL_DATA_SPEC"]
