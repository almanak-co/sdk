"""Prepared fee-module observations retain the verified baseline dependencies."""

from almanak.connectors import v3_economic_book as family
from almanak.connectors.aerodrome_slipstream import economic_data
from almanak.connectors.aerodrome_slipstream.history import BoundaryEvidence
from almanak.models import ZERO_ADDRESS


class PoolBook(family.PoolBook):
    data = economic_data
    boundary_type = BoundaryEvidence

    def validate_row(self, target, baseline, row):
        super().validate_row(target, baseline, row)
        observed = row.payload.pool
        modules = tuple(
            address
            for address in (observed.swap_fee_module, observed.unstaked_fee_module)
            if address != ZERO_ADDRESS
        )
        identities = baseline.contracts
        expected = (target.pool.pool, *modules, *target.contracts.model_dump().values())
        if (
            len(identities) != len(expected) + 2
            or identities[0].address != target.pool.factory
            or tuple(i.address for i in identities[2:]) != expected
        ):
            raise ValueError("prepared Slipstream fee modules lack retained revision evidence")


identity = PoolBook.identity
