"""Actual frozen-source GMX Safe lifecycle facts reused by offline component checks."""

import gzip
from functools import cache

from almanak.connectors.gmx_v2.state import Checkpoint
from tests.gmx_retired import current_json
from tests.retained_fixtures import retained


@cache
def checkpoint():
    path = (
        retained("2026-09-08-gmx-request-fixtures")
        / "final-arbitrum-safe-runtime/checkpoint.json.gz"
    )
    return Checkpoint.model_validate_json(current_json(gzip.decompress(path.read_bytes())))


def accepted():
    state = checkpoint()
    outcome = state.outcomes[1]
    return state.model_copy(
        update={
            "mode": "running",
            "outcomes": state.outcomes[:2],
            "delivered": 2,
            "decision": outcome.decision,
            "owned": (outcome.after.owned,),
            "requests": (outcome.request,),
            "closure": (),
        }
    )


def holding():
    state = checkpoint()
    outcome = state.outcomes[2]
    return state.model_copy(
        update={
            "mode": "running",
            "outcomes": state.outcomes[:3],
            "delivered": 3,
            "decision": None,
            "owned": (outcome.after.owned,),
            "requests": (outcome.request,),
            "closure": (),
        }
    )
