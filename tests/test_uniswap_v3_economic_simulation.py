"""Economic action sequences use captured V3 pool state and explicit synthetic fee paths."""

import gzip
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak.connectors.uniswap_v3 import authoring
from almanak.connectors.uniswap_v3.economic_data import PoolSample
from almanak.connectors.uniswap_v3.economic_simulation import quote_at, simulate
from almanak.connectors.uniswap_v3.economic_state import EconomicPosition
from almanak.connectors.uniswap_v3.observations import initial_owned
from almanak.dex_intents import CollectLiquidityFees
from tests.uniswap_v3_support import parameters
from tests.v3_economic_support import assert_all_actions

RAW = (Path(__file__).with_name("fixtures") / "uniswap-v3-pool-samples.json.gz").read_bytes()
CAPTURE = json.loads(gzip.decompress(RAW))


def setup(index):
    config = parameters(index, lp=True)
    target = config.target("position")
    sample = PoolSample.model_validate_json(json.dumps(CAPTURE[index]["sample"]))
    assert sample.pool.source == target.pool
    record = SimpleNamespace(
        payload=sample,
        reference=sample.pool.block,
        source_time=sample.pool.block.timestamp,
        available_at=sample.pool.block.timestamp,
        evidence=(hashlib.sha256(RAW).hexdigest(),),
    )
    payload = EconomicPosition(target=target, owned=initial_owned(config, "position"))
    return config, record, payload


@pytest.mark.parametrize("index", [0, 1])
def test_all_economic_actions_preserve_inventory_and_simulated_position_identity(index):
    assert_all_actions(*setup(index), authoring, quote_at, simulate, EconomicPosition)


def test_future_quotes_and_unowned_liquidity_are_refused():
    config, record, payload = setup(0)
    with pytest.raises(ValueError, match="future"):
        quote_at(record, payload.target, config.allocations[0].capital, record.available_at - 1)
    intent = CollectLiquidityFees[type(payload.target)](
        target=payload.target, deadline=record.available_at + 300
    )
    with pytest.raises(ValueError, match="hypothetical liquidity"):
        simulate(config, intent, payload, record, record.available_at, "foreign")
