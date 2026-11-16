"""Prepared dataset identity and decoding use concrete connector types."""

from almanak.connectors import v3_economic_book as family
from almanak.connectors.dex_economic_dataset import targets as targets
from almanak.connectors.pancakeswap_v3 import economic_data
from almanak.connectors.pancakeswap_v3.history import BoundaryEvidence


class PoolBook(family.PoolBook):
    data = economic_data
    boundary_type = BoundaryEvidence

    def validate_row(self, target, baseline, row):
        super().validate_row(target, baseline, row)
        callback = row.payload.pool.lm_pool
        # Pool verification retains factory, deployer, pool, optional LM, then
        # the three periphery contracts. A sample must preserve that LM binding.
        expected = 7 if int(callback, 16) else 6
        if len(baseline.contracts) != expected or (
            expected == 7 and baseline.contracts[3].address != callback
        ):
            raise ValueError("prepared Pancake callback lacks retained revision evidence")


identity = PoolBook.identity
