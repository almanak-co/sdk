"""Pool-data capability declaration for Camelot Algebra pools."""

from almanak.connectors._strategy_base.pool_data import (
    PoolReferenceKind,
    unsupported_pool_data_spec,
)

POOL_DATA_SPEC = unsupported_pool_data_spec(
    protocol="camelot",
    reference_kind=PoolReferenceKind.EVM_CONTRACT,
    reason=(
        "Camelot Algebra pools use dynamic fees and globalState; an authenticated "
        "Algebra metadata/state adapter is not yet wired to MarketSnapshot."
    ),
)

__all__ = ["POOL_DATA_SPEC"]
