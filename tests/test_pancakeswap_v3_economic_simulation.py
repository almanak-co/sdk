"""Pancake economic actions share verified pool math and preserve hypothetical ownership."""

import hashlib
from types import SimpleNamespace

from almanak.connectors.pancakeswap_v3 import authoring
from almanak.connectors.pancakeswap_v3.economic_simulation import quote_at, simulate
from almanak.connectors.pancakeswap_v3.economic_state import EconomicPosition
from almanak.connectors.pancakeswap_v3.observations import initial_owned
from tests.pancakeswap_v3_support import parameters
from tests.test_pancakeswap_v3_pool_model import prepared
from tests.v3_economic_support import assert_all_actions


def test_all_economic_actions_preserve_pancake_sources_and_simulated_ownership():
    config = parameters(lp=True)
    sample = prepared()
    target = config.target("position")
    assert sample.pool.source == target.pool
    record = SimpleNamespace(
        payload=sample,
        reference=sample.pool.block,
        source_time=sample.pool.block.timestamp,
        available_at=sample.pool.block.timestamp,
        evidence=(hashlib.sha256(sample.model_dump_json().encode()).hexdigest(),),
    )
    payload = EconomicPosition(target=target, owned=initial_owned(config, "position"))
    assert_all_actions(config, record, payload, authoring, quote_at, simulate, EconomicPosition)
