"""Shared checks that a minimum health factor is required only where debt is possible."""

import pytest

from almanak.connectors.lending_health import NO_DEBT_FLOOR, health_floor


def revalidated(parameters, allocations):
    return type(parameters).model_validate_json(
        parameters.model_copy(update={"allocations": allocations}).model_dump_json()
    )


def without_floor(allocation, **changes):
    return allocation.model_copy(update={"minimum_health_factor": None, **changes})


def check_health_floor_scope(parameters):
    stated = revalidated(parameters, parameters.allocations)
    assert stated == parameters
    assert all("minimum_health_factor" in a.model_dump(mode="json") for a in stated.allocations)

    lend_only = revalidated(
        parameters, tuple(without_floor(a, borrow=False) for a in parameters.allocations)
    )
    for allocation in lend_only.allocations:
        assert "minimum_health_factor" not in allocation.model_dump(mode="json")
        assert health_floor(allocation.minimum_health_factor) == NO_DEBT_FLOOR

    borrowers = {a.account for a in parameters.allocations if a.borrow}
    for index, allocation in enumerate(parameters.allocations):
        changed = list(parameters.allocations)
        changed[index] = without_floor(allocation)
        if allocation.account in borrowers:
            # The borrower itself, or an allocation sharing its fund and market.
            with pytest.raises(ValueError, match="minimum_health_factor|risk policy"):
                revalidated(parameters, tuple(changed))
        else:
            assert revalidated(parameters, tuple(changed)).allocations[index] == changed[index]
