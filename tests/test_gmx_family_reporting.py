"""External keeper outcomes never acquire submitter transaction family facts."""

from types import SimpleNamespace

import pytest

from almanak.gateway.replacement import command
from almanak.transaction_family import family_report


def test_keeper_outcome_is_ignored_by_transaction_family_report_and_command():
    keeper = SimpleNamespace(kind="gmx_request_outcome")
    state = SimpleNamespace(work=(), pending=None, outcomes=(keeper,))
    assert family_report(state) == {"pending": None, "completed": []}
    with pytest.raises(ValueError, match="retained pending execution"):
        command(None, state, "reserve", "missing", 1)
    completed = SimpleNamespace(family=SimpleNamespace(identity="retained"))
    state.outcomes = (keeper, completed)
    assert command(None, state, "reserve", "retained", 1) is state
