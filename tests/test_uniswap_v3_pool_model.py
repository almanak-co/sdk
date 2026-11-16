"""Complete offline V3 swaps compared with independently captured historical Quoter calls."""

import gzip
import json
from pathlib import Path

import pytest

from almanak.connectors.uniswap_v3.economic_acquisition import sample
from almanak.connectors.uniswap_v3.economic_data import PoolSample
from almanak.connectors.uniswap_v3.swap_math import exact_input
from tests.test_uniswap_v3_reader import Replay

CAPTURE = json.loads(
    gzip.decompress(
        (Path(__file__).with_name("fixtures") / "uniswap-v3-pool-samples.json.gz").read_bytes()
    )
)


@pytest.mark.parametrize("row", CAPTURE, ids=lambda r: r["source"]["chain"]["chain_id"])
def test_offline_swaps_match_archival_quoter_across_sizes_and_ticks(row):
    payload = PoolSample.model_validate_json(json.dumps(row["sample"]))
    crossed = []
    for case in row["comparisons"]:
        quote = case["quote"]
        result = exact_input(
            payload, quote["amount_in"], quote["asset_in"] == str(payload.pool.source.currency0)
        )
        assert result.output == quote["amount_out"]
        assert result.sqrt_price_x96 == quote["sqrt_price_after"]
        assert result.tick == case["calculated"]["tick"]
        assert result.liquidity == case["calculated"]["liquidity"]
        crossed.extend(result.crossed_ticks)
    assert crossed, "contract comparisons must exercise initialized tick crossings"


@pytest.mark.parametrize("row", CAPTURE, ids=lambda r: r["source"]["chain"]["chain_id"])
def test_bounded_acquisition_reproduces_frozen_pool_state(row):
    with Replay(row) as reader:
        block = reader.pin_block(row["sample"]["pool"]["block"]["number"])
        payload = sample(reader, reader.source, block, **row["limits"])
        assert payload == PoolSample.model_validate_json(json.dumps(row["sample"]))
        assert reader.seen == set(reader.records)


@pytest.mark.parametrize("limit,value", [("max_words", 2), ("max_ticks", 1)])
def test_collection_limits_cannot_expand_implicitly(limit, value):
    row = CAPTURE[0]
    with Replay(row) as reader:
        block = reader.pin_block(row["sample"]["pool"]["block"]["number"])
        bounds = {**row["limits"], limit: value}
        with pytest.raises(ValueError, match="collection limit"):
            sample(reader, reader.source, block, **bounds)


@pytest.mark.parametrize("field", ["words", "ticks"])
def test_missing_prepared_inputs_fail_without_quote_fallback(field):
    payload = PoolSample.model_validate_json(json.dumps(CAPTURE[0]["sample"]))
    broken = payload.model_copy(update={field: ()})
    with pytest.raises(ValueError, match="missing or unordered|does not match"):
        PoolSample.model_validate_json(broken.model_dump_json())
    with pytest.raises(ValueError, match="unprepared"):
        exact_input(broken, 100 * 10**18, False)


def test_unbounded_trade_cannot_escape_retained_tick_coverage():
    payload = PoolSample.model_validate_json(json.dumps(CAPTURE[0]["sample"]))
    with pytest.raises(ValueError, match="outside prepared tick coverage"):
        exact_input(payload, 2**126, False)
