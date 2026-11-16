"""Refactored fork assertions continue to accept retained native swap economics and cleanup."""

from tests import gmx_swap_path
from tests.test_gmx_swap_replay import completed as completed


def test_retained_swap_holding_and_closure_assertions(completed):
    outcome = completed.outcomes[2]
    holding = completed.model_copy(
        update={
            "mode": "running",
            "owned": (outcome.after.owned,),
            "requests": (outcome.request,),
            "outcomes": completed.outcomes[:3],
            "delivered": 3,
        }
    )
    gmx_swap_path.assert_holding(None, holding)
    gmx_swap_path.assert_closed(None, completed)
