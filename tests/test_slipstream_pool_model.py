"""Slipstream acquisition and economic fills replay archived pool and Quoter reads."""

import gzip
import json
from dataclasses import asdict

import pytest

from almanak.connectors.aerodrome_slipstream.economic_acquisition import sample
from almanak.connectors.aerodrome_slipstream.economic_data import PoolSample
from almanak.connectors.aerodrome_slipstream.swap_math import exact_input
from almanak.connectors.v3_fee_growth import accrued, inside
from tests.slipstream_support import EVIDENCE
from tests.test_slipstream_reader import CAPTURE, Replay


def retained(name):
    return json.loads(gzip.decompress((EVIDENCE / f"{name}.json.gz").read_bytes()))


def prepared():
    comparisons = retained("pool-comparison")
    row = {
        **CAPTURE,
        "requests": [
            *CAPTURE["requests"],
            *comparisons["requests"],
            *retained("global-growth")["requests"],
        ],
    }
    with Replay(row) as reader:
        block = reader.pin_block(CAPTURE["block"]["number"])
        return sample(
            reader,
            reader.source,
            block,
            lower=-887272,
            upper=887272,
            max_words=140,
            max_ticks=1024,
        )


def test_archival_quoter_matches_sampled_dynamic_fee_and_tick_crossings():
    payload = prepared()
    assert PoolSample.model_validate_json(payload.model_dump_json()) == payload
    crossings = []
    for row in retained("pool-comparison")["comparisons"]:
        result = exact_input(
            payload,
            row["amount"]["amount"],
            row["amount"]["asset"] == str(payload.pool.source.currency0),
        )
        assert (result.output, result.sqrt_price_x96) == tuple(row["quoter"][:2])
        assert json.loads(json.dumps(asdict(result))) == row["model"]
        crossings.append(len(result.crossed_ticks))
    assert len(crossings) == 16 and max(crossings) == 142


def test_tick_schema_does_not_treat_staked_net_as_fee_growth():
    payload = prepared()
    raw = retained("pool-comparison")
    assert len(payload.words) == 140 and len(payload.ticks) == 237
    assert payload.pool.swap_fee == 332 and payload.pool.source.tick_spacing == 50
    assert "fee" not in payload.pool.source.model_dump()
    assert [t.model_dump(mode="json") for t in payload.ticks] == [
        {k: t[k] for k in ("index", "gross", "net", "outside0", "outside1")} for t in raw["ticks"]
    ]
    assert any(t["staked_net"] != t["outside0"] for t in raw["ticks"])
    changed = payload.model_dump(mode="json")
    changed["ticks"] = []
    with pytest.raises(ValueError, match="does not match"):
        PoolSample.model_validate_json(json.dumps(changed))


@pytest.mark.parametrize("method", ("inside", "earned"))
def test_unstaked_fee_accounting_matches_compiled_tick_and_position(method):
    rows = retained("fee-vectors")["records"]
    for row in rows:
        if row["method"] != method:
            continue
        result = inside(*row["args"]) if method == "inside" else (accrued(*row["args"]),)
        assert result == tuple(row["result"])
