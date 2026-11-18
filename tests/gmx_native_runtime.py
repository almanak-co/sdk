"""Native GMX runtimes, read from pinned copies of the retained captures."""

import gzip

import pytest

from almanak.connectors.gmx_v2.state import Checkpoint
from tests.gmx_retired import current_json
from tests.retained_fixtures import retained

PROFILES = ["arbitrum-safe", "arbitrum-eoa", "avalanche-safe", "avalanche-eoa"]

# Lifecycles on the deployment GMX has authorized since the 1 October 2026 re-pin (PR 118),
# run on identified forks at the reference pins. Their creation events carry `uiFeeFactor`,
# which the retired deployment's events in the September captures lack, so replays that
# decode a creation receipt read these. Safe runs are the rotation report's own fork runs.
CURRENT = {
    "safe": "2026-09-30-gmx-deployment-rotation/fork/gmx_v2_{chain}_{reference}",
    "eoa": "2026-10-19-gmx-eoa-recapture/gmx_v2_{chain}_{reference}-pinned-eoa-configured/evidence",
}
# The Avalanche EOA liquidity lifecycle settles its close only with its teardown aligned to a
# retained keeper report, so its run is retained apart from the first EOA recapture.
CURRENT_RUNS = {
    ("avalanche-eoa", "liquidity"): (
        "2026-10-19-gmx-avalanche-gm-close/"
        "gmx_v2_avalanche_liquidity-pinned-eoa-configured/evidence"
    ),
}


def checkpoint(directory):
    path = directory / "checkpoint.json.gz"
    return Checkpoint.model_validate_json(current_json(gzip.decompress(path.read_bytes())))


def runtime(directory, name):
    return checkpoint(directory / name)


def current_runtime(profile, reference):
    """The closed `gmx_v2_<chain>_<reference>` lifecycle for `profile` on the current deployment."""
    chain, account = profile.split("-")
    path = CURRENT_RUNS.get((profile, reference), CURRENT[account])
    return checkpoint(retained(path.format(chain=chain, reference=reference)))


@pytest.fixture(name="completed", params=PROFILES)
def gm_completed(request):
    return runtime(retained("2026-09-09-gmx-gm"), request.param + "-authority-runtime")


@pytest.fixture(name="completed", params=PROFILES)
def current_gm_completed(request):
    return current_runtime(request.param, "liquidity")


@pytest.fixture(name="completed", params=PROFILES)
def current_exchange_completed(request):
    return current_runtime(request.param, "swap")


def current_position():
    """The Arbitrum Safe position lifecycle on the current deployment."""
    return current_runtime("arbitrum-safe", "position")


def cancellation_state():
    """A Safe position order just before its retained cancellation, and that cancellation."""
    closed = runtime(
        retained("2026-09-08-gmx-request-fixtures"), "final-arbitrum-safe-cancellation-runtime"
    )
    cancelled = closed.outcomes[-1]
    state = closed.model_copy(
        update={
            "mode": "teardown",
            "decision": cancelled.decision,
            "outcomes": closed.outcomes[:-1],
            "delivered": len(closed.outcomes) - 1,
            "requests": (cancelled.before.request,),
            "owned": (cancelled.before.owned,),
            "closure": (),
        }
    )
    return state, cancelled


def before_state(completed, index):
    outcome = completed.outcomes[index]
    return completed.model_copy(
        update={
            "mode": "running",
            "decision": outcome.decision,
            "outcomes": completed.outcomes[:index],
            "owned": (outcome.before.owned,),
            "requests": (),
        }
    )
