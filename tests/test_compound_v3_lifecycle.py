"""Whole-market ownership refuses adoption and preserves cleanup evidence."""

from types import SimpleNamespace

import pytest

from almanak.connectors.compound_v3.lifecycle import (
    commit_owned,
    validate_closure,
    validate_initial,
    validate_owned,
)
from almanak.connectors.compound_v3.observations import owned
from almanak.gateway.ethereum import DataError
from tests.test_compound_v3_observations import snapshot


def positions():
    return tuple(
        observation.model_copy(
            update={"target": observation.target.model_copy(update={"allocation": name})}
        )
        for name, observation in (("collateral", snapshot()), ("loan", snapshot(True)))
    )


def closed(observation):
    account = observation.account.model_copy(
        update={
            "principal": 0,
            "supplied_base": 0,
            "borrowed_base": 0,
            "assets_in": 0,
            "extended_assets_in": 0,
            "collaterals": tuple(
                c.model_copy(update={"balance": 0}) for c in observation.account.collaterals
            ),
        }
    )
    return observation.model_copy(update={"account": account, "allowance": 0})


def test_initialization_cannot_adopt_positions_and_closure_requires_the_entire_market():
    observations = positions()
    state = SimpleNamespace(owned=tuple(owned(o) for o in observations))
    validate_owned(state, observations)
    with pytest.raises(DataError, match="adopt"):
        validate_initial(state, observations)
    with pytest.raises(DataError, match="retains"):
        validate_closure(state, observations)
    cleared = tuple(closed(o) for o in observations)
    state = SimpleNamespace(owned=tuple(owned(o) for o in cleared))
    validate_initial(state, cleared)
    validate_closure(state, cleared)
    with pytest.raises(DataError, match="retains"):
        validate_closure(state, (cleared[0].model_copy(update={"allowance": 1}), cleared[1]))


def test_missing_targets_unallocated_collateral_and_inconsistent_market_views_refuse():
    observations = positions()
    state = SimpleNamespace(owned=tuple(owned(o) for o in observations))
    with pytest.raises(DataError, match="every"):
        validate_owned(state, observations[:1])
    first = observations[0]
    index = next(i for i, c in enumerate(first.account.collaterals) if c.balance == 0)
    collaterals = list(first.account.collaterals)
    collaterals[index] = collaterals[index].model_copy(update={"balance": 1})
    changed = first.model_copy(
        update={"account": first.account.model_copy(update={"collaterals": tuple(collaterals)})}
    )
    with pytest.raises(DataError, match="not owned"):
        validate_owned(state, (changed, observations[1]))
    second = observations[1]
    changed = second.model_copy(
        update={
            "account": second.account.model_copy(
                update={
                    "market": second.account.market.model_copy(
                        update={"base_available": second.account.market.base_available + 1}
                    )
                }
            )
        }
    )
    with pytest.raises(DataError, match="inconsistent"):
        validate_owned(state, (first, changed))


def test_only_successful_outcomes_update_their_exact_owned_target():
    observations = positions()
    retained = tuple(owned(o) for o in observations)
    outcome = SimpleNamespace(success=False, after=closed(observations[1]))
    assert commit_owned(retained, outcome) == retained
    outcome.success = True
    changed = commit_owned(retained, outcome)
    assert changed[0] == retained[0] and changed[1].empty
