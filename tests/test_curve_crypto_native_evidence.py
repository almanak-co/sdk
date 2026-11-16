"""Actual TwoCrypto strategy checkpoints constrain shared execution and pool transitions."""

import gzip
import json
from pathlib import Path

import pytest

from almanak.connectors.curve import twocrypto_transitions as crypto
from almanak.connectors.curve.operations import Addition, Approval, Swap
from almanak.connectors.curve.state import Checkpoint
from tests.curve_retired import current_json

DIRECTORY = (
    Path(__file__).parents[1]
    / "docs/internal/reports/evidence/2026-09-07-rollout/curve-twocrypto-native"
)
BASE = DIRECTORY.parent / "curve-base-twocrypto-native"
CASES = tuple(
    directory / "native" / case["case"]
    for directory in (DIRECTORY, BASE)
    for case in json.loads((directory / "manifest.json").read_text())["cases"]
)


ARBITRUM = DIRECTORY.parent / "curve-arbitrum-twocrypto-native"
CASES += tuple(
    ARBITRUM / "native" / case["case"]
    for case in json.loads((ARBITRUM / "manifest.json").read_text())["cases"]
)


@pytest.fixture(params=CASES)
def retained(request):
    raw = gzip.decompress((request.param / "checkpoint.json.gz").read_bytes())
    return Checkpoint.model_validate_json(current_json(raw))


def transition(outcome):
    operation, pool = outcome.operation, outcome.before.pool
    target, time = operation.target, outcome.after.block.timestamp
    if isinstance(operation, Swap):
        return crypto.exchange(
            pool,
            target.assets.index(operation.amount.asset),
            target.assets.index(operation.minimum.asset),
            operation.amount.amount,
            timestamp=time,
        )
    if isinstance(operation, Addition):
        return crypto.add_liquidity(
            pool, tuple(v.amount for v in operation.amounts.root), timestamp=time
        )
    if operation.output_asset is None:
        return crypto.withdraw_proportional(pool, operation.shares.amount, timestamp=time)
    return crypto.withdraw_one(
        pool, target.assets.index(operation.output_asset), operation.shares.amount, timestamp=time
    )


def test_native_full_pool_state_matches_revision_bound_transitions(retained):
    for outcome in retained.outcomes:
        if isinstance(outcome.operation, Approval):
            continue
        result = transition(outcome)
        after = outcome.after.pool
        assert result.amounts == tuple(value.amount for value in outcome.asset_changes)
        assert (
            result.state.total_supply - outcome.before.pool.total_supply
            == outcome.share_change.amount
        )
        assert result.state == after.math_state()
        assert result.admin == after.accounting.math_state()
        assert result.prices == crypto.price_state(after)
